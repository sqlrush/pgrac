/*-------------------------------------------------------------------------
 *
 * test_cluster_gcs_block_dedup_r4_route.c
 *	  Behavioral tests for the R4 D3 typed master-route record.  This
 *	  binary links the real cluster_gcs_block_dedup.o against a bounded
 *	  fake shared-memory HTAB; only PostgreSQL runtime dependencies are
 *	  stubbed.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_gcs_block_dedup_r4_route.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "cluster/cluster_conf.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "storage/backendid.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static bool stop_new_work_allowed = true;
static int stop_gate_calls;

bool
cluster_normal_stop_service_new_work(bool modifies_data)
{
	(void)modifies_data;
	stop_gate_calls++;
	return stop_new_work_allowed;
}

/* Globals normally supplied by cluster_guc.c / globals.c. */
bool cluster_enabled = true;
int cluster_node_id = 0;
int cluster_lms_workers = 1;
int cluster_gcs_block_dedup_max_entries = 8;
int cluster_gcs_block_retransmit_initial_backoff_ms = 100;
int cluster_gcs_block_retransmit_max_retries = 4;
int cluster_gcs_reply_timeout_ms = 5000;
int MaxConnections = 1;
bool IsUnderPostmaster = false;
BackendId MyBackendId = InvalidBackendId;
BackendType MyBackendType = B_LMON;

/* 26.5 seconds, pinned with the established 2x discipline. */
#define TEST_LIFETIME_HINT_MS UINT32_C(26500)
#define TEST_PINNED_LIFETIME_US INT64CONST(53000000)
#define TEST_ROUTE_TRANSITION ((uint8)PCM_TRANS_N_TO_S)
#define TEST_WALL_BASE_US INT64CONST(1000000000)
#define TEST_MONOTONIC_BASE_US INT64CONST(2000000000)

static TimestampTz fake_now = TEST_WALL_BASE_US;
static struct timespec fake_monotonic_now;
static int fake_declared_nodes = 1;
static int fake_lock_depth = 0;

void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *context)
{
	static LWLock fixture_lock;
	if (fake_lock_depth != 0)
		callback(&fixture_lock, LW_EXCLUSIVE, context);
}

int cluster_test_clock_gettime(clockid_t clock_id, struct timespec *tp);

/* -------------------------------------------------------------------------
 * PostgreSQL runtime stubs required by cluster_gcs_block_dedup.o.
 * -------------------------------------------------------------------------
 */

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

bool
message_level_is_interesting(int elevel pg_attribute_unused())
{
	return false;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return fake_now;
}

int
cluster_test_clock_gettime(clockid_t clock_id pg_attribute_unused(), struct timespec *tp)
{
	Assert(tp != NULL);
	*tp = fake_monotonic_now;
	return 0;
}

int
cluster_conf_declared_node_count_early(void)
{
	return fake_declared_nodes;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

void
before_shmem_exit(pg_on_exit_callback function pg_attribute_unused(),
				  Datum arg pg_attribute_unused())
{}

Size
add_size(Size s1, Size s2)
{
	return s1 + s2;
}

Size
mul_size(Size s1, Size s2)
{
	return s1 * s2;
}

Size
hash_estimate_size(long num_entries, Size entrysize)
{
	return (Size)num_entries * entrysize;
}

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	Assert(fake_lock_depth == 0);
	fake_lock_depth++;
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{
	Assert(fake_lock_depth == 1);
	fake_lock_depth--;
}

/* -------------------------------------------------------------------------
 * Bounded fake shared-memory HTAB.  HASH_REMOVE and hole reuse are required
 * by route TTL/epoch/requester/closed cleanup.
 * -------------------------------------------------------------------------
 */

#define FAKE_ROUTE_MAX_SLOTS 32

static union {
	uint64 force_align;
	char data[4096];
} fake_dedup_header;

static union {
	uint64 force_align;
	char data[FAKE_ROUTE_MAX_SLOTS][sizeof(GcsBlockDedupEntry)];
} fake_dedup_slots;

static bool fake_slot_used[FAKE_ROUTE_MAX_SLOTS];
static char fake_dedup_htab_token;
static bool fake_dedup_header_found = false;
static long fake_dedup_entry_max = 0;
static Size fake_dedup_keysize = 0;

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	Assert(size <= sizeof(fake_dedup_header.data));
	*foundPtr = fake_dedup_header_found;
	fake_dedup_header_found = true;
	return fake_dedup_header.data;
}

HTAB *
ShmemInitHash(const char *name pg_attribute_unused(), long init_size pg_attribute_unused(),
			  long max_size, HASHCTL *infoP, int hash_flags pg_attribute_unused())
{
	Assert((hash_flags & HASH_ELEM) != 0);
	Assert(infoP->entrysize == sizeof(GcsBlockDedupEntry));
	Assert(max_size <= FAKE_ROUTE_MAX_SLOTS);
	fake_dedup_keysize = infoP->keysize;
	fake_dedup_entry_max = max_size;
	memset(fake_slot_used, 0, sizeof(fake_slot_used));
	return (HTAB *)&fake_dedup_htab_token;
}

static long
fake_live_count(void)
{
	long i;
	long n = 0;

	for (i = 0; i < FAKE_ROUTE_MAX_SLOTS; i++)
		if (fake_slot_used[i])
			n++;
	return n;
}

void *
hash_search(HTAB *hashp pg_attribute_unused(), const void *keyPtr, HASHACTION action,
			bool *foundPtr)
{
	long i;

	Assert(fake_lock_depth == 1);
	Assert(fake_dedup_keysize == sizeof(GcsBlockDedupKey));

	for (i = 0; i < FAKE_ROUTE_MAX_SLOTS; i++) {
		if (!fake_slot_used[i])
			continue;
		if (memcmp(fake_dedup_slots.data[i], keyPtr, fake_dedup_keysize) == 0) {
			if (foundPtr != NULL)
				*foundPtr = true;
			if (action == HASH_REMOVE)
				fake_slot_used[i] = false;
			return fake_dedup_slots.data[i];
		}
	}

	if (action != HASH_ENTER && action != HASH_ENTER_NULL) {
		if (foundPtr != NULL)
			*foundPtr = false;
		return NULL;
	}

	if (fake_live_count() >= fake_dedup_entry_max) {
		if (action == HASH_ENTER_NULL) {
			if (foundPtr != NULL)
				*foundPtr = false;
			return NULL;
		}
		Assert(false);
	}

	for (i = 0; i < FAKE_ROUTE_MAX_SLOTS; i++) {
		if (!fake_slot_used[i]) {
			memcpy(fake_dedup_slots.data[i], keyPtr, fake_dedup_keysize);
			fake_slot_used[i] = true;
			if (foundPtr != NULL)
				*foundPtr = false;
			return fake_dedup_slots.data[i];
		}
	}
	Assert(false);
	return NULL;
}

void
hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp pg_attribute_unused())
{
	Assert(fake_lock_depth == 1);
	status->curBucket = 0;
	status->hashp = NULL;
}

void *
hash_seq_search(HASH_SEQ_STATUS *status)
{
	Assert(fake_lock_depth == 1);
	while (status->curBucket < FAKE_ROUTE_MAX_SLOTS) {
		uint32 i = status->curBucket++;

		if (fake_slot_used[i])
			return fake_dedup_slots.data[i];
	}
	return NULL;
}

void
hash_seq_term(HASH_SEQ_STATUS *status pg_attribute_unused())
{}

/* -------------------------------------------------------------------------
 * Fixture and exact-value assertions.
 * -------------------------------------------------------------------------
 */

static int64
fixture_monotonic_us(void)
{
	return (int64)fake_monotonic_now.tv_sec * USECS_PER_SEC
		   + (int64)fake_monotonic_now.tv_nsec / 1000;
}

static void
fixture_set_monotonic_us(int64 now_us)
{
	Assert(now_us >= 0);
	fake_monotonic_now.tv_sec = (time_t)(now_us / USECS_PER_SEC);
	fake_monotonic_now.tv_nsec = (long)((now_us % USECS_PER_SEC) * 1000);
}

static void
fixture_advance_monotonic_us(int64 delta_us)
{
	fixture_set_monotonic_us(fixture_monotonic_us() + delta_us);
}

static void
fixture_reset(int cap)
{
	fake_dedup_header_found = false;
	fake_dedup_entry_max = 0;
	fake_dedup_keysize = 0;
	fake_lock_depth = 0;
	memset(fake_slot_used, 0, sizeof(fake_slot_used));
	memset(&fake_dedup_header, 0, sizeof(fake_dedup_header));
	memset(&fake_dedup_slots, 0, sizeof(fake_dedup_slots));
	fake_now = TEST_WALL_BASE_US;
	fixture_set_monotonic_us(TEST_MONOTONIC_BASE_US);

	cluster_enabled = true;
	cluster_node_id = 0;
	cluster_lms_workers = 1;
	cluster_gcs_block_dedup_max_entries = cap;
	MaxConnections = 1;
	fake_declared_nodes = 1;

	Assert(cluster_gcs_block_dedup_shmem_size() > 0);
	cluster_gcs_block_dedup_shmem_init();
	Assert(fake_lock_depth == 0);
}

static BufferTag
make_tag(uint32 blockno)
{
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	tag.spcOid = 1663;
	tag.dbOid = 1;
	tag.relNumber = 200;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = blockno;
	return tag;
}

static GcsBlockR4RouteIdentity
make_identity(uint64 seq, uint64 epoch, uint32 blockno, SCN read_scn,
			  uint64 activation_generation)
{
	GcsBlockR4RouteIdentity identity;

	memset(&identity, 0, sizeof(identity));
	identity.legacy_key.origin_node_id = 1;
	identity.legacy_key.requester_backend_id = 7;
	identity.legacy_key.request_id = gcs_reqid_requester(1, 7, seq);
	identity.legacy_key.cluster_epoch = epoch;
	identity.tag = make_tag(blockno);
	identity.read_scn = read_scn;
	identity.activation_generation = activation_generation;
	return identity;
}

static ClusterR4CrRouteProof
make_proof(const GcsBlockR4RouteIdentity *identity, int32 holder_node)
{
	ClusterR4CrRouteProof proof;

	memset(&proof, 0, sizeof(proof));
	proof.tag = identity->tag;
	proof.read_scn = identity->read_scn;
	proof.formation_epoch = identity->legacy_key.cluster_epoch;
	proof.activation_generation = identity->activation_generation;
	proof.master_authority_generation = (identity->legacy_key.cluster_epoch << 32) | UINT64_C(5);
	proof.master_resource_transition_count = UINT64_C(7);
	proof.expected_page_scn = (SCN)UINT64_C(11);
	proof.real_master_node = 0;
	proof.selected_holder_node = holder_node;
	return proof;
}

static void
assert_proof_exact(const ClusterR4CrRouteProof *actual, const ClusterR4CrRouteProof *expected)
{
	UT_ASSERT(memcmp(&actual->tag, &expected->tag, sizeof(BufferTag)) == 0);
	UT_ASSERT_EQ(actual->read_scn, expected->read_scn);
	UT_ASSERT_EQ(actual->formation_epoch, expected->formation_epoch);
	UT_ASSERT_EQ(actual->activation_generation, expected->activation_generation);
	UT_ASSERT_EQ(actual->master_authority_generation, expected->master_authority_generation);
	UT_ASSERT_EQ(actual->master_resource_transition_count,
				 expected->master_resource_transition_count);
	UT_ASSERT_EQ(actual->expected_page_scn, expected->expected_page_scn);
	UT_ASSERT_EQ(actual->real_master_node, expected->real_master_node);
	UT_ASSERT_EQ(actual->selected_holder_node, expected->selected_holder_node);
}

static GcsBlockR4RouteArmResult
arm_route(const GcsBlockR4RouteIdentity *identity, const ClusterR4CrRouteProof *proof,
		  GcsBlockR4RouteRecord *record_out)
{
	return cluster_gcs_block_dedup_r4_route_arm_or_match(
		0, identity, TEST_ROUTE_TRANSITION, proof, TEST_LIFETIME_HINT_MS, true, record_out);
}

/* -------------------------------------------------------------------------
 * ABI, arm/match, collision, drift and send publication.
 * -------------------------------------------------------------------------
 */

UT_TEST(test_route_abi_and_empty_count)
{
	fixture_reset(8);
	UT_ASSERT_EQ(64, sizeof(GcsBlockR4RouteIdentity));
	UT_ASSERT_EQ(128, sizeof(GcsBlockR4RouteRecord));
	UT_ASSERT_EQ(128, sizeof(GcsBlockDedupPayloadMeta));
	UT_ASSERT_EQ(8472, sizeof(GcsBlockDedupEntry));
	UT_ASSERT_EQ(0, cluster_gcs_block_dedup_r4_route_count());
}

UT_TEST(test_new_then_exact_duplicate_replays_stored_record)
{
	GcsBlockR4RouteIdentity identity = make_identity(1, 3, 41, (SCN)101, 13);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;

	fixture_reset(8);
	memset(&record, 0xA5, sizeof(record));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ROUTING, record.state);
	assert_proof_exact(&record.proof, &proof);
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(1, fake_live_count());

	memset(&record, 0x5A, sizeof(record));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_REPLAY, arm_route(&identity, &proof, &record));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ROUTING, record.state);
	assert_proof_exact(&record.proof, &proof);
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(1, fake_live_count());
}

UT_TEST(test_unarmed_expected_page_scn_new_then_exact_replay)
{
	GcsBlockR4RouteIdentity identity = make_identity(21, 3, 62, (SCN)121, 30);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;
	GcsBlockR4RouteArmResult result;

	proof.expected_page_scn = InvalidScn;
	fixture_reset(8);
	result = arm_route(&identity, &proof, &record);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, result);
	if (result != GCS_BLOCK_R4_ROUTE_ARM_NEW)
		return;
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ROUTING, record.state);
	UT_ASSERT_EQ(InvalidScn, record.proof.expected_page_scn);
	assert_proof_exact(&record.proof, &proof);
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_count());

	memset(&record, 0xA5, sizeof(record));
	result = arm_route(&identity, &proof, &record);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_REPLAY, result);
	if (result != GCS_BLOCK_R4_ROUTE_ARM_REPLAY)
		return;
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ROUTING, record.state);
	UT_ASSERT_EQ(InvalidScn, record.proof.expected_page_scn);
	assert_proof_exact(&record.proof, &proof);
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_count());
}

UT_TEST(test_read_scn_collision_preserves_first_record)
{
	GcsBlockR4RouteIdentity first = make_identity(2, 3, 42, (SCN)102, 14);
	GcsBlockR4RouteIdentity collision = first;
	ClusterR4CrRouteProof first_proof = make_proof(&first, 2);
	ClusterR4CrRouteProof collision_proof;
	GcsBlockR4RouteRecord record;

	fixture_reset(8);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&first, &first_proof, &record));
	collision.read_scn++;
	collision_proof = make_proof(&collision, 2);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_IDENTITY_COLLISION,
				 arm_route(&collision, &collision_proof, &record));
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_REPLAY, arm_route(&first, &first_proof, &record));
	assert_proof_exact(&record.proof, &first_proof);
}

UT_TEST(test_activation_collision_preserves_first_record)
{
	GcsBlockR4RouteIdentity first = make_identity(3, 3, 43, (SCN)103, 15);
	GcsBlockR4RouteIdentity collision = first;
	ClusterR4CrRouteProof first_proof = make_proof(&first, 2);
	ClusterR4CrRouteProof collision_proof;
	GcsBlockR4RouteRecord record;

	fixture_reset(8);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&first, &first_proof, &record));
	collision.activation_generation++;
	collision_proof = make_proof(&collision, 2);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_IDENTITY_COLLISION,
				 arm_route(&collision, &collision_proof, &record));
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_REPLAY, arm_route(&first, &first_proof, &record));
	assert_proof_exact(&record.proof, &first_proof);
}

UT_TEST(test_tag_collision_preserves_first_record)
{
	GcsBlockR4RouteIdentity first = make_identity(17, 3, 57, (SCN)117, 26);
	GcsBlockR4RouteIdentity collision = first;
	ClusterR4CrRouteProof first_proof = make_proof(&first, 2);
	ClusterR4CrRouteProof collision_proof;
	GcsBlockR4RouteRecord record;

	fixture_reset(8);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&first, &first_proof, &record));
	collision.tag = make_tag(58);
	collision_proof = make_proof(&collision, 2);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_IDENTITY_COLLISION,
				 arm_route(&collision, &collision_proof, &record));
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_REPLAY, arm_route(&first, &first_proof, &record));
	assert_proof_exact(&record.proof, &first_proof);
}

UT_TEST(test_transition_collision_preserves_first_record)
{
	GcsBlockR4RouteIdentity identity = make_identity(18, 3, 59, (SCN)118, 27);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;

	fixture_reset(8);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_IDENTITY_COLLISION,
				 cluster_gcs_block_dedup_r4_route_arm_or_match(
					 0, &identity, (uint8)(TEST_ROUTE_TRANSITION + 1), &proof,
					 TEST_LIFETIME_HINT_MS, true, &record));
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_REPLAY, arm_route(&identity, &proof, &record));
	assert_proof_exact(&record.proof, &proof);
}

UT_TEST(test_proof_drift_marks_retryable_without_overwriting_winner)
{
	GcsBlockR4RouteIdentity identity = make_identity(4, 3, 44, (SCN)104, 16);
	ClusterR4CrRouteProof first_proof = make_proof(&identity, 2);
	ClusterR4CrRouteProof drifted = first_proof;
	GcsBlockR4RouteRecord record;

	fixture_reset(8);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &first_proof, &record));
	drifted.selected_holder_node = 3;
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_HOLDER_MOVED, arm_route(&identity, &drifted, &record));
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_RETRYABLE,
				 arm_route(&identity, &first_proof, &record));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_SEND_COLLISION,
				 cluster_gcs_block_dedup_r4_route_finish_send(
					 0, &identity, TEST_ROUTE_TRANSITION, &drifted, true));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_SEND_FORWARDED,
				 cluster_gcs_block_dedup_r4_route_finish_send(
					 0, &identity, TEST_ROUTE_TRANSITION, &first_proof, true));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_REPLAY, arm_route(&identity, &first_proof, &record));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_FORWARDED, record.state);
	assert_proof_exact(&record.proof, &first_proof);
}

UT_TEST(test_send_failure_then_late_admission_becomes_forwarded)
{
	GcsBlockR4RouteIdentity identity = make_identity(5, 3, 45, (SCN)105, 17);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;

	fixture_reset(8);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_SEND_RETRYABLE,
				 cluster_gcs_block_dedup_r4_route_finish_send(
					 0, &identity, TEST_ROUTE_TRANSITION, &proof, false));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_RETRYABLE, arm_route(&identity, &proof, &record));

	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_SEND_FORWARDED,
				 cluster_gcs_block_dedup_r4_route_finish_send(
					 0, &identity, TEST_ROUTE_TRANSITION, &proof, true));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_REPLAY, arm_route(&identity, &proof, &record));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_FORWARDED, record.state);
}

UT_TEST(test_send_failure_cannot_downgrade_forwarded)
{
	GcsBlockR4RouteIdentity identity = make_identity(6, 3, 46, (SCN)106, 18);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;

	fixture_reset(8);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_SEND_FORWARDED,
				 cluster_gcs_block_dedup_r4_route_finish_send(
					 0, &identity, TEST_ROUTE_TRANSITION, &proof, true));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_SEND_RETRYABLE,
				 cluster_gcs_block_dedup_r4_route_finish_send(
					 0, &identity, TEST_ROUTE_TRANSITION, &proof, false));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_REPLAY, arm_route(&identity, &proof, &record));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_FORWARDED, record.state);
}

UT_TEST(test_duplicate_send_admission_does_not_extend_forwarded_ttl)
{
	GcsBlockR4RouteIdentity identity = make_identity(20, 3, 61, (SCN)120, 29);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;
	TimestampTz first_completion_wall;
	int64 first_completion_monotonic;

	fixture_reset(8);
	first_completion_wall = fake_now;
	first_completion_monotonic = fixture_monotonic_us();
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_SEND_FORWARDED,
				 cluster_gcs_block_dedup_r4_route_finish_send(
					 0, &identity, TEST_ROUTE_TRANSITION, &proof, true));

	/* A duplicate accepted publication one microsecond later must not refresh
	 * completed_at_ts.  At the original strict TTL + 1 boundary the route
	 * expires; a refreshed timestamp would leave age == deadline and survive. */
	fake_now = first_completion_wall + 1;
	fixture_set_monotonic_us(first_completion_monotonic + 1);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_SEND_FORWARDED,
				 cluster_gcs_block_dedup_r4_route_finish_send(
					 0, &identity, TEST_ROUTE_TRANSITION, &proof, true));
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_count());

	fake_now = first_completion_wall + TEST_PINNED_LIFETIME_US + 1;
	fixture_set_monotonic_us(first_completion_monotonic + TEST_PINNED_LIFETIME_US + 1);
	cluster_gcs_block_dedup_sweep_expired(fake_now);
	UT_ASSERT_EQ(0, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(0, fake_live_count());
}

UT_TEST(test_finish_send_after_epoch_cleanup_is_stale)
{
	GcsBlockR4RouteIdentity identity = make_identity(19, 3, 60, (SCN)119, 28);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;

	fixture_reset(8);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_sweep_epoch(4));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_SEND_STALE,
				 cluster_gcs_block_dedup_r4_route_finish_send(
					 0, &identity, TEST_ROUTE_TRANSITION, &proof, true));
	UT_ASSERT_EQ(0, cluster_gcs_block_dedup_r4_route_count());
}

UT_TEST(test_route_cap_full_preserves_live_record)
{
	GcsBlockR4RouteIdentity first = make_identity(7, 3, 47, (SCN)107, 19);
	GcsBlockR4RouteIdentity second = make_identity(8, 3, 48, (SCN)108, 19);
	ClusterR4CrRouteProof first_proof = make_proof(&first, 2);
	ClusterR4CrRouteProof second_proof = make_proof(&second, 2);
	GcsBlockR4RouteRecord record;

	fixture_reset(1);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&first, &first_proof, &record));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_FULL, arm_route(&second, &second_proof, &record));
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(1, fake_live_count());
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_REPLAY, arm_route(&first, &first_proof, &record));
}

/* -------------------------------------------------------------------------
 * Route-specific cleanup and legacy-kind isolation.
 * -------------------------------------------------------------------------
 */

UT_TEST(test_route_ttl_uses_strict_pinned_deadline)
{
	GcsBlockR4RouteIdentity identity = make_identity(9, 3, 49, (SCN)109, 20);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;

	fixture_reset(8);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
	fake_now += TEST_PINNED_LIFETIME_US;
	fixture_advance_monotonic_us(TEST_PINNED_LIFETIME_US);
	cluster_gcs_block_dedup_sweep_expired(fake_now);
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_count());

	fake_now++;
	fixture_advance_monotonic_us(1);
	cluster_gcs_block_dedup_sweep_expired(fake_now);
	UT_ASSERT_EQ(0, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(0, fake_live_count());
}

UT_TEST(test_wall_forward_monotonic_in_window_expires_generic_but_keeps_route)
{
	GcsBlockR4RouteIdentity route = make_identity(22, 3, 63, (SCN)122, 31);
	ClusterR4CrRouteProof proof = make_proof(&route, 2);
	GcsBlockR4RouteRecord record;
	GcsBlockDedupKey generic_key = route.legacy_key;
	BufferTag generic_tag = make_tag(64);

	fixture_reset(8);
	generic_key.request_id = gcs_reqid_requester(1, 7, 23);
	UT_ASSERT_EQ(GCS_BLOCK_DEDUP_MISS_REGISTERED,
				 cluster_gcs_block_dedup_lookup_or_register(
					 0, &generic_key, generic_tag, TEST_ROUTE_TRANSITION,
					 TEST_LIFETIME_HINT_MS, true, NULL));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&route, &proof, &record));

	/* Generic remains wall-clock based, while the route is still only one
	 * monotonic microsecond into its pinned window. */
	fake_now += TEST_PINNED_LIFETIME_US + 1;
	fixture_advance_monotonic_us(1);
	cluster_gcs_block_dedup_sweep_expired(fake_now);
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(1, fake_live_count());
}

UT_TEST(test_wall_backward_monotonic_expired_removes_route)
{
	GcsBlockR4RouteIdentity identity = make_identity(24, 3, 65, (SCN)124, 32);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;

	fixture_reset(8);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
	fake_now--;
	fixture_advance_monotonic_us(TEST_PINNED_LIFETIME_US + 1);
	cluster_gcs_block_dedup_sweep_expired(fake_now);
	UT_ASSERT_EQ(0, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(0, fake_live_count());
}

UT_TEST(test_cap_one_wall_forward_monotonic_in_window_preserves_route)
{
	GcsBlockR4RouteIdentity first = make_identity(25, 3, 66, (SCN)125, 33);
	GcsBlockR4RouteIdentity second = make_identity(26, 3, 67, (SCN)126, 33);
	ClusterR4CrRouteProof first_proof = make_proof(&first, 2);
	ClusterR4CrRouteProof second_proof = make_proof(&second, 2);
	GcsBlockR4RouteRecord record;
	GcsBlockR4RouteArmResult result;

	fixture_reset(1);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&first, &first_proof, &record));
	fake_now += TEST_PINNED_LIFETIME_US + 1;
	fixture_advance_monotonic_us(1);
	result = arm_route(&second, &second_proof, &record);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_FULL, result);
	if (result != GCS_BLOCK_R4_ROUTE_ARM_FULL)
		return;
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_REPLAY, arm_route(&first, &first_proof, &record));
}

UT_TEST(test_cap_one_wall_backward_monotonic_expired_reclaims_route)
{
	GcsBlockR4RouteIdentity first = make_identity(27, 3, 68, (SCN)127, 34);
	GcsBlockR4RouteIdentity second = make_identity(28, 3, 69, (SCN)128, 34);
	ClusterR4CrRouteProof first_proof = make_proof(&first, 2);
	ClusterR4CrRouteProof second_proof = make_proof(&second, 2);
	GcsBlockR4RouteRecord record;
	GcsBlockR4RouteArmResult result;

	fixture_reset(1);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&first, &first_proof, &record));
	fake_now--;
	fixture_advance_monotonic_us(TEST_PINNED_LIFETIME_US + 1);
	result = arm_route(&second, &second_proof, &record);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, result);
	if (result != GCS_BLOCK_R4_ROUTE_ARM_NEW)
		return;
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_REPLAY,
				 arm_route(&second, &second_proof, &record));
}

UT_TEST(test_epoch_zero_is_valid_and_only_stale_formation_is_swept)
{
	GcsBlockR4RouteIdentity identity = make_identity(10, 0, 50, (SCN)110, 21);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;

	fixture_reset(8);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
	UT_ASSERT_EQ(0, cluster_gcs_block_dedup_r4_route_sweep_epoch(0));
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_sweep_epoch(1));
	UT_ASSERT_EQ(0, cluster_gcs_block_dedup_r4_route_count());
}

UT_TEST(test_backend_exit_removes_exact_requester_route)
{
	GcsBlockR4RouteIdentity matching = make_identity(11, 3, 51, (SCN)111, 22);
	GcsBlockR4RouteIdentity survivor = make_identity(12, 3, 52, (SCN)112, 22);
	ClusterR4CrRouteProof matching_proof = make_proof(&matching, 2);
	ClusterR4CrRouteProof survivor_proof;
	GcsBlockR4RouteRecord record;

	survivor.legacy_key.requester_backend_id = 8;
	survivor.legacy_key.request_id = gcs_reqid_requester(1, 8, 12);
	fixture_reset(8);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&matching, &matching_proof, &record));
	survivor_proof = make_proof(&survivor, 2);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&survivor, &survivor_proof, &record));
	cluster_gcs_block_dedup_cleanup_on_backend_exit(1, 7);
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_REPLAY,
				 arm_route(&survivor, &survivor_proof, &record));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&matching, &matching_proof, &record));
}

UT_TEST(test_node_death_removes_requester_route)
{
	GcsBlockR4RouteIdentity identity = make_identity(13, 3, 53, (SCN)113, 23);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;

	fixture_reset(8);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
	cluster_gcs_block_dedup_cleanup_on_node_dead(1);
	UT_ASSERT_EQ(0, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(0, fake_live_count());
}

UT_TEST(test_closed_purge_removes_routes_but_preserves_generic_entry)
{
	GcsBlockR4RouteIdentity route = make_identity(14, 3, 54, (SCN)114, 24);
	ClusterR4CrRouteProof proof = make_proof(&route, 2);
	GcsBlockR4RouteRecord record;
	GcsBlockDedupKey generic_key;
	BufferTag generic_tag = make_tag(55);

	fixture_reset(8);
	generic_key = route.legacy_key;
	generic_key.request_id = gcs_reqid_requester(1, 7, 15);
	UT_ASSERT_EQ(GCS_BLOCK_DEDUP_MISS_REGISTERED,
				 cluster_gcs_block_dedup_lookup_or_register(
					 0, &generic_key, generic_tag, TEST_ROUTE_TRANSITION,
					 TEST_LIFETIME_HINT_MS, true, NULL));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&route, &proof, &record));

	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_purge_closed());
	UT_ASSERT_EQ(0, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(1, fake_live_count());
	UT_ASSERT_EQ(GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE,
				 cluster_gcs_block_dedup_lookup_or_register(
					 0, &generic_key, generic_tag, TEST_ROUTE_TRANSITION,
					 TEST_LIFETIME_HINT_MS, true, NULL));
}

UT_TEST(test_generic_done_and_remove_ignore_route)
{
	GcsBlockR4RouteIdentity identity = make_identity(16, 3, 56, (SCN)116, 25);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;
	uint64 done_mismatch_before;

	fixture_reset(8);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
	done_mismatch_before = cluster_gcs_block_dedup_get_done_mismatch_count();
	UT_ASSERT(!cluster_gcs_block_dedup_mark_done(
		0, &identity.legacy_key, &identity.tag, TEST_ROUTE_TRANSITION));
	cluster_gcs_block_dedup_remove(0, &identity.legacy_key);
	UT_ASSERT_EQ(GCS_BLOCK_DEDUP_VALIDATION_FAIL,
				 cluster_gcs_block_dedup_lookup_or_register(
					 0, &identity.legacy_key, identity.tag, TEST_ROUTE_TRANSITION,
					 TEST_LIFETIME_HINT_MS, true, NULL));
	UT_ASSERT_EQ(done_mismatch_before,
				 cluster_gcs_block_dedup_get_done_mismatch_count());
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_REPLAY, arm_route(&identity, &proof, &record));
}

UT_TEST(test_forwarded_route_consumes_exact_requester_done)
{
	GcsBlockR4RouteIdentity identity = make_identity(17, 3, 57, (SCN)117, 26);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;

	fixture_reset(8);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_SEND_FORWARDED,
				 cluster_gcs_block_dedup_r4_route_finish_send(0, &identity, TEST_ROUTE_TRANSITION,
															  &proof, true));
	UT_ASSERT(cluster_gcs_block_dedup_mark_done(0, &identity.legacy_key, &identity.tag,
												TEST_ROUTE_TRANSITION));
	UT_ASSERT_EQ(1, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(1, fake_live_count());
	UT_ASSERT_EQ(0, fake_lock_depth);
}

UT_TEST(test_completed_route_reclaims_after_existing_done_linger)
{
	GcsBlockR4RouteIdentity identity = make_identity(18, 3, 58, (SCN)118, 27);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;
	GcsBlockDedupKey next = identity.legacy_key;

	fixture_reset(1);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_SEND_FORWARDED,
				 cluster_gcs_block_dedup_r4_route_finish_send(0, &identity, TEST_ROUTE_TRANSITION,
															  &proof, true));
	(void)cluster_gcs_block_dedup_mark_done(0, &identity.legacy_key, &identity.tag,
											TEST_ROUTE_TRANSITION);
	/* Same established 2 x reply-timeout quarantine, not the 53-second
	 * no-completion lifetime. Wall time is deliberately unchanged. */
	fixture_advance_monotonic_us((int64)cluster_gcs_reply_timeout_ms * 2000 + 1);
	cluster_gcs_block_dedup_sweep_expired(fake_now);
	UT_ASSERT_EQ(0, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(0, fake_live_count());
	next.request_id = gcs_reqid_requester(1, 7, 19);
	UT_ASSERT_EQ(GCS_BLOCK_DEDUP_MISS_REGISTERED, cluster_gcs_block_dedup_lookup_or_register(
													  0, &next, make_tag(59), TEST_ROUTE_TRANSITION,
													  TEST_LIFETIME_HINT_MS, true, NULL));
	UT_ASSERT_EQ(0, cluster_gcs_block_dedup_get_full_count());
}

UT_TEST(test_retryable_done_preserves_proof_until_strict_quarantine)
{
	GcsBlockR4RouteIdentity identity = make_identity(30, 3, 70, (SCN)130, 35);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;

	fixture_reset(1);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_SEND_RETRYABLE,
				 cluster_gcs_block_dedup_r4_route_finish_send(0, &identity, TEST_ROUTE_TRANSITION,
															  &proof, false));
	UT_ASSERT(cluster_gcs_block_dedup_mark_done(0, &identity.legacy_key, &identity.tag,
												TEST_ROUTE_TRANSITION));
	fixture_advance_monotonic_us(10000000);
	cluster_gcs_block_dedup_sweep_expired(fake_now);
	UT_ASSERT_EQ(1, fake_live_count());
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_RETRYABLE, arm_route(&identity, &proof, &record));
	assert_proof_exact(&record.proof, &proof);
	fixture_advance_monotonic_us(1);
	cluster_gcs_block_dedup_sweep_expired(fake_now);
	UT_ASSERT_EQ(0, fake_live_count());
}

UT_TEST(test_duplicate_done_does_not_extend_quarantine)
{
	GcsBlockR4RouteIdentity identity = make_identity(31, 3, 71, (SCN)131, 35);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;

	fixture_reset(1);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
	(void)cluster_gcs_block_dedup_r4_route_finish_send(0, &identity, TEST_ROUTE_TRANSITION, &proof,
													   true);
	UT_ASSERT(cluster_gcs_block_dedup_mark_done(0, &identity.legacy_key, &identity.tag,
												TEST_ROUTE_TRANSITION));
	fixture_advance_monotonic_us(9000000);
	UT_ASSERT(cluster_gcs_block_dedup_mark_done(0, &identity.legacy_key, &identity.tag,
												TEST_ROUTE_TRANSITION));
	UT_ASSERT_EQ(2, cluster_gcs_block_dedup_get_done_marked_count());
	fixture_advance_monotonic_us(1000001);
	cluster_gcs_block_dedup_sweep_expired(fake_now);
	UT_ASSERT_EQ(0, fake_live_count());
}

UT_TEST(test_done_quarantine_ignores_wall_jumps_and_live_guc_changes)
{
	int direction;

	for (direction = -1; direction <= 1; direction += 2) {
		GcsBlockR4RouteIdentity identity = make_identity(32, 3, 72, (SCN)132, 35);
		ClusterR4CrRouteProof proof = make_proof(&identity, 2);
		GcsBlockR4RouteRecord record;

		fixture_reset(1);
		UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
		(void)cluster_gcs_block_dedup_r4_route_finish_send(0, &identity, TEST_ROUTE_TRANSITION,
														   &proof, true);
		UT_ASSERT(cluster_gcs_block_dedup_mark_done(0, &identity.legacy_key, &identity.tag,
													TEST_ROUTE_TRANSITION));
		fake_now += direction * INT64CONST(100000000);
		cluster_gcs_reply_timeout_ms = 1;
		fixture_advance_monotonic_us(10000000);
		cluster_gcs_block_dedup_sweep_expired(fake_now);
		UT_ASSERT_EQ(1, fake_live_count());
		fixture_advance_monotonic_us(1);
		cluster_gcs_block_dedup_sweep_expired(fake_now);
		UT_ASSERT_EQ(0, fake_live_count());
		cluster_gcs_reply_timeout_ms = 5000;
	}
}

UT_TEST(test_uncompleted_forwarded_route_keeps_full_lifetime)
{
	GcsBlockR4RouteIdentity identity = make_identity(33, 3, 73, (SCN)133, 35);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;

	fixture_reset(1);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
	(void)cluster_gcs_block_dedup_r4_route_finish_send(0, &identity, TEST_ROUTE_TRANSITION, &proof,
													   true);
	fixture_advance_monotonic_us(10000001);
	cluster_gcs_block_dedup_sweep_expired(fake_now);
	UT_ASSERT_EQ(1, fake_live_count());
	fixture_advance_monotonic_us(42999999);
	cluster_gcs_block_dedup_sweep_expired(fake_now);
	UT_ASSERT_EQ(1, fake_live_count());
	fixture_advance_monotonic_us(1);
	cluster_gcs_block_dedup_sweep_expired(fake_now);
	UT_ASSERT_EQ(0, fake_live_count());
}

UT_TEST(test_wrong_done_identity_cannot_shorten_route_lifetime)
{
	int variant;

	for (variant = 0; variant < 6; variant++) {
		GcsBlockR4RouteIdentity identity = make_identity(34, 3, 74, (SCN)134, 35);
		ClusterR4CrRouteProof proof = make_proof(&identity, 2);
		GcsBlockR4RouteRecord record;
		GcsBlockDedupKey wrong_key = identity.legacy_key;
		BufferTag wrong_tag = identity.tag;
		uint8 transition = TEST_ROUTE_TRANSITION;

		fixture_reset(1);
		UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
		(void)cluster_gcs_block_dedup_r4_route_finish_send(0, &identity, TEST_ROUTE_TRANSITION,
														   &proof, true);
		switch (variant) {
		case 0:
			wrong_key.request_id++;
			break;
		case 1:
			wrong_key.origin_node_id++;
			break;
		case 2:
			wrong_key.requester_backend_id++;
			break;
		case 3:
			wrong_key.cluster_epoch++;
			break;
		case 4:
			wrong_tag.blockNum++;
			break;
		case 5:
			transition = (uint8)PCM_TRANS_N_TO_X;
			break;
		}
		UT_ASSERT(!cluster_gcs_block_dedup_mark_done(0, &wrong_key, &wrong_tag, transition));
		fixture_advance_monotonic_us(10000001);
		cluster_gcs_block_dedup_sweep_expired(fake_now);
		UT_ASSERT_EQ(1, fake_live_count());
		UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_REPLAY, arm_route(&identity, &proof, &record));
		assert_proof_exact(&record.proof, &proof);
	}
}

UT_TEST(test_late_done_does_not_resurrect_or_change_successor)
{
	GcsBlockR4RouteIdentity first = make_identity(35, 3, 75, (SCN)135, 35);
	GcsBlockR4RouteIdentity next = make_identity(36, 3, 75, (SCN)136, 35);
	ClusterR4CrRouteProof first_proof = make_proof(&first, 2);
	ClusterR4CrRouteProof next_proof = make_proof(&next, 3);
	GcsBlockR4RouteRecord record;

	fixture_reset(1);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&first, &first_proof, &record));
	(void)cluster_gcs_block_dedup_r4_route_finish_send(0, &first, TEST_ROUTE_TRANSITION,
													   &first_proof, true);
	UT_ASSERT(
		cluster_gcs_block_dedup_mark_done(0, &first.legacy_key, &first.tag, TEST_ROUTE_TRANSITION));
	fixture_advance_monotonic_us(10000001);
	cluster_gcs_block_dedup_sweep_expired(fake_now);
	UT_ASSERT(!cluster_gcs_block_dedup_mark_done(0, &first.legacy_key, &first.tag,
												 TEST_ROUTE_TRANSITION));
	UT_ASSERT_EQ(0, fake_live_count());
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&next, &next_proof, &record));
	UT_ASSERT(!cluster_gcs_block_dedup_mark_done(0, &first.legacy_key, &first.tag,
												 TEST_ROUTE_TRANSITION));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_REPLAY, arm_route(&next, &next_proof, &record));
	assert_proof_exact(&record.proof, &next_proof);
	fixture_advance_monotonic_us(10000001);
	cluster_gcs_block_dedup_sweep_expired(fake_now);
	UT_ASSERT_EQ(1, fake_live_count());
}

UT_TEST(test_eager_reclaim_mixed_capacity_keeps_uncompleted_generic)
{
	GcsBlockR4RouteIdentity route = make_identity(37, 3, 77, (SCN)137, 35);
	ClusterR4CrRouteProof proof = make_proof(&route, 2);
	GcsBlockR4RouteRecord record;
	GcsBlockDedupKey pending = make_identity(38, 3, 78, (SCN)138, 35).legacy_key;
	GcsBlockDedupKey next = make_identity(39, 3, 79, (SCN)139, 35).legacy_key;

	fixture_reset(2);
	UT_ASSERT_EQ(GCS_BLOCK_DEDUP_MISS_REGISTERED,
				 cluster_gcs_block_dedup_lookup_or_register(0, &pending, make_tag(78),
															TEST_ROUTE_TRANSITION,
															TEST_LIFETIME_HINT_MS, true, NULL));
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&route, &proof, &record));
	(void)cluster_gcs_block_dedup_r4_route_finish_send(0, &route, TEST_ROUTE_TRANSITION, &proof,
													   true);
	UT_ASSERT(
		cluster_gcs_block_dedup_mark_done(0, &route.legacy_key, &route.tag, TEST_ROUTE_TRANSITION));
	fixture_advance_monotonic_us(10000000);
	UT_ASSERT_EQ(GCS_BLOCK_DEDUP_FULL, cluster_gcs_block_dedup_lookup_or_register(
										   0, &next, make_tag(79), TEST_ROUTE_TRANSITION,
										   TEST_LIFETIME_HINT_MS, true, NULL));
	fixture_advance_monotonic_us(1);
	/* No sweep: the real bounded capacity-pressure probe must reclaim it. */
	UT_ASSERT_EQ(GCS_BLOCK_DEDUP_MISS_REGISTERED, cluster_gcs_block_dedup_lookup_or_register(
													  0, &next, make_tag(79), TEST_ROUTE_TRANSITION,
													  TEST_LIFETIME_HINT_MS, true, NULL));
	UT_ASSERT_EQ(0, cluster_gcs_block_dedup_r4_route_count());
	UT_ASSERT_EQ(2, fake_live_count());
	UT_ASSERT_EQ(0, cluster_gcs_block_dedup_get_evict_count());
	UT_ASSERT_EQ(GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE,
				 cluster_gcs_block_dedup_lookup_or_register(0, &pending, make_tag(78),
															TEST_ROUTE_TRANSITION,
															TEST_LIFETIME_HINT_MS, true, NULL));
}

UT_TEST(test_done_rejects_uncompleted_or_malformed_route_without_mutation)
{
	int variant;

	for (variant = 0; variant < 7; variant++) {
		GcsBlockR4RouteIdentity identity = make_identity(40, 3, 80, (SCN)140, 35);
		ClusterR4CrRouteProof proof = make_proof(&identity, 2);
		GcsBlockR4RouteRecord record;
		GcsBlockDedupEntry *entry;
		GcsBlockDedupEntry before;

		fixture_reset(1);
		UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
		(void)cluster_gcs_block_dedup_r4_route_finish_send(0, &identity, TEST_ROUTE_TRANSITION,
														   &proof, true);
		UT_ASSERT(fake_slot_used[0]);
		entry = (GcsBlockDedupEntry *)fake_dedup_slots.data[0];
		switch (variant) {
		case 0:
			entry->payload_meta.r4_route.state = GCS_BLOCK_R4_ROUTE_ROUTING;
			break;
		case 1:
			entry->payload_meta.r4_route.state = UINT8_MAX;
			break;
		case 2:
			entry->completed_at_ts = 0;
			break;
		case 3:
			entry->payload_meta.r4_route.proof.formation_epoch++;
			break;
		case 4:
			entry->payload_meta.r4_route.proof.tag.blockNum++;
			break;
		case 5:
			entry->payload_meta.r4_route.proof.activation_generation = 0;
			break;
		case 6:
			entry->pinned_done_linger_us = 0;
			break;
		}
		memcpy(&before, entry, sizeof(before));
		UT_ASSERT(!cluster_gcs_block_dedup_mark_done(0, &identity.legacy_key, &identity.tag,
													 TEST_ROUTE_TRANSITION));
		UT_ASSERT_EQ(memcmp(entry, &before, sizeof(before)), 0);
	}
}

UT_TEST(test_done_anchor_in_monotonic_future_cannot_reclaim)
{
	GcsBlockR4RouteIdentity identity = make_identity(41, 3, 81, (SCN)141, 35);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;

	fixture_reset(1);
	UT_ASSERT_EQ(GCS_BLOCK_R4_ROUTE_ARM_NEW, arm_route(&identity, &proof, &record));
	(void)cluster_gcs_block_dedup_r4_route_finish_send(0, &identity, TEST_ROUTE_TRANSITION, &proof,
													   true);
	UT_ASSERT(cluster_gcs_block_dedup_mark_done(0, &identity.legacy_key, &identity.tag,
												TEST_ROUTE_TRANSITION));
	fixture_advance_monotonic_us(-1);
	fake_now += TEST_PINNED_LIFETIME_US + 1;
	cluster_gcs_block_dedup_sweep_expired(fake_now);
	UT_ASSERT_EQ(1, fake_live_count());
}

UT_TEST(test_stop_seal_new_route_not_original_handoff_or_replay)
{
	GcsBlockR4RouteIdentity identity = make_identity(42, 3, 82, (SCN)142, 35);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;

	fixture_reset(1);
	stop_new_work_allowed = false;
	stop_gate_calls = 0;
	UT_ASSERT_EQ(cluster_gcs_block_dedup_r4_route_arm_or_match(0, &identity, TEST_ROUTE_TRANSITION,
															   &proof, TEST_LIFETIME_HINT_MS, true,
															   &record),
				 GCS_BLOCK_R4_ROUTE_ARM_FULL);
	UT_ASSERT_EQ(stop_gate_calls, 1);
	UT_ASSERT_EQ(cluster_gcs_block_dedup_get_in_flight_count(), 0);
	stop_new_work_allowed = true;
	UT_ASSERT_EQ(cluster_gcs_block_dedup_r4_route_arm_or_match(0, &identity, TEST_ROUTE_TRANSITION,
															   &proof, TEST_LIFETIME_HINT_MS, true,
															   &record),
				 GCS_BLOCK_R4_ROUTE_ARM_NEW);
	stop_new_work_allowed = false;
	stop_gate_calls = 0;
	UT_ASSERT_EQ(cluster_gcs_block_dedup_r4_route_finish_send(0, &identity, TEST_ROUTE_TRANSITION,
															  &proof, true),
				 GCS_BLOCK_R4_ROUTE_SEND_FORWARDED);
	UT_ASSERT_EQ(cluster_gcs_block_dedup_r4_route_arm_or_match(0, &identity, TEST_ROUTE_TRANSITION,
															   &proof, TEST_LIFETIME_HINT_MS, true,
															   &record),
				 GCS_BLOCK_R4_ROUTE_ARM_REPLAY);
	UT_ASSERT_EQ(record.state, GCS_BLOCK_R4_ROUTE_FORWARDED);
	UT_ASSERT_EQ(stop_gate_calls, 0);
	UT_ASSERT_EQ(fake_lock_depth, 0);
	stop_new_work_allowed = true;
}

UT_TEST(test_stop_route_handoff_is_not_total_cluster_completion)
{
	GcsBlockR4RouteIdentity identity = make_identity(43, 3, 83, (SCN)143, 35);
	ClusterR4CrRouteProof proof = make_proof(&identity, 2);
	GcsBlockR4RouteRecord record;
	const char *domain, *reason;
	uint64 key;

	fixture_reset(1);
	IsUnderPostmaster = true;
	UT_ASSERT_EQ(cluster_gcs_block_dedup_r4_route_arm_or_match(0, &identity, TEST_ROUTE_TRANSITION,
															   &proof, TEST_LIFETIME_HINT_MS, true,
															   &record),
				 GCS_BLOCK_R4_ROUTE_ARM_NEW);
	UT_ASSERT_EQ(cluster_gcs_dedup_normal_stop_poll(&domain, &key, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(key, identity.legacy_key.request_id);
	UT_ASSERT_EQ(cluster_gcs_block_dedup_r4_route_finish_send(0, &identity, TEST_ROUTE_TRANSITION,
															  &proof, true),
				 GCS_BLOCK_R4_ROUTE_SEND_FORWARDED);
	UT_ASSERT_EQ(cluster_gcs_dedup_normal_stop_poll(&domain, &key, &reason),
				 CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(cluster_gcs_block_dedup_get_in_flight_count(), 1);
	UT_ASSERT_EQ(cluster_gcs_block_dedup_r4_route_arm_or_match(0, &identity, TEST_ROUTE_TRANSITION,
															   &proof, TEST_LIFETIME_HINT_MS, true,
															   &record),
				 GCS_BLOCK_R4_ROUTE_ARM_REPLAY);
	UT_ASSERT_EQ(record.state, GCS_BLOCK_R4_ROUTE_FORWARDED);
	IsUnderPostmaster = false;
}

int
main(void)
{
	UT_PLAN(36);
	UT_RUN(test_stop_route_handoff_is_not_total_cluster_completion);
	UT_RUN(test_stop_seal_new_route_not_original_handoff_or_replay);
	UT_RUN(test_route_abi_and_empty_count);
	UT_RUN(test_new_then_exact_duplicate_replays_stored_record);
	UT_RUN(test_unarmed_expected_page_scn_new_then_exact_replay);
	UT_RUN(test_read_scn_collision_preserves_first_record);
	UT_RUN(test_activation_collision_preserves_first_record);
	UT_RUN(test_tag_collision_preserves_first_record);
	UT_RUN(test_transition_collision_preserves_first_record);
	UT_RUN(test_proof_drift_marks_retryable_without_overwriting_winner);
	UT_RUN(test_send_failure_then_late_admission_becomes_forwarded);
	UT_RUN(test_send_failure_cannot_downgrade_forwarded);
	UT_RUN(test_duplicate_send_admission_does_not_extend_forwarded_ttl);
	UT_RUN(test_finish_send_after_epoch_cleanup_is_stale);
	UT_RUN(test_route_cap_full_preserves_live_record);
	UT_RUN(test_route_ttl_uses_strict_pinned_deadline);
	UT_RUN(test_wall_forward_monotonic_in_window_expires_generic_but_keeps_route);
	UT_RUN(test_wall_backward_monotonic_expired_removes_route);
	UT_RUN(test_cap_one_wall_forward_monotonic_in_window_preserves_route);
	UT_RUN(test_cap_one_wall_backward_monotonic_expired_reclaims_route);
	UT_RUN(test_epoch_zero_is_valid_and_only_stale_formation_is_swept);
	UT_RUN(test_backend_exit_removes_exact_requester_route);
	UT_RUN(test_node_death_removes_requester_route);
	UT_RUN(test_closed_purge_removes_routes_but_preserves_generic_entry);
	UT_RUN(test_generic_done_and_remove_ignore_route);
	UT_RUN(test_forwarded_route_consumes_exact_requester_done);
	UT_RUN(test_completed_route_reclaims_after_existing_done_linger);
	UT_RUN(test_retryable_done_preserves_proof_until_strict_quarantine);
	UT_RUN(test_duplicate_done_does_not_extend_quarantine);
	UT_RUN(test_done_quarantine_ignores_wall_jumps_and_live_guc_changes);
	UT_RUN(test_uncompleted_forwarded_route_keeps_full_lifetime);
	UT_RUN(test_wrong_done_identity_cannot_shorten_route_lifetime);
	UT_RUN(test_late_done_does_not_resurrect_or_change_successor);
	UT_RUN(test_eager_reclaim_mixed_capacity_keeps_uncompleted_generic);
	UT_RUN(test_done_rejects_uncompleted_or_malformed_route_without_mutation);
	UT_RUN(test_done_anchor_in_monotonic_future_cannot_reclaim);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
