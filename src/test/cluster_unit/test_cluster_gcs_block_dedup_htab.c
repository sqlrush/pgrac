/*-------------------------------------------------------------------------
 *
 * test_cluster_gcs_block_dedup_htab.c
 *	  Behavioral unit tests for the spec-7.2a GCS block dedup capacity +
 *	  eager-GC path (U1-U6):  links the REAL cluster_gcs_block_dedup.o and
 *	  drives cluster_gcs_block_dedup_lookup_or_register() to cap against a
 *	  bounded fake shmem HTAB fixture.
 *
 *	  Complements test_cluster_gcs_block_dedup_reclaim.c (header-only pure
 *	  decision primitives, R1-R12):  here the cap-full -> eager-reclaim ->
 *	  re-register chain itself is exercised.  The LMON TTL sweep
 *	  (cluster_gcs_block_dedup_sweep_expired) is NEVER called by this
 *	  binary and the clock is a controlled fake, so every evict_count
 *	  increment observed below is attributable to the MISS-path eager
 *	  reclaim alone (review F4: assertions must separate eager from TTL).
 *
 *	  Tests (spec §4.1 numbering):
 *	    U1  fill-to-cap with in-flight entries -> FULL fail-closed
 *	        (in-flight is never reclaim-safe; full_count++, evict_count 0)
 *	    U3  completed-but-in-window entries (GRANTED) -> still FULL
 *	        (whitelist empty: no in-window reclaim)
 *	    U4  completed-but-in-window storage-fallback -> still FULL
 *	        (:3305 N->S / :4089 S->X_UPGRADE sites; never in-window)
 *	    U2  past the 2x out-of-window threshold -> MISS triggers eager
 *	        reclaim, registration succeeds, full_count does NOT grow,
 *	        evict_count +1, entry_count conserved at cap
 *	    U5  the reclaimed key re-registers through MISS without double
 *	        counting, and dedup (install_reply -> CACHED_REPLY hit) still
 *	        works on the re-registered entry
 *	    U6  entry_count bookkeeping matches the actual HTAB element count
 *	        across register / reclaim / remove
 *	    D4a auto-size floor binds: configured < MaxConnections x declared
 *	        node count -> effective (stamped max_entries) = floor
 *	    D4b configured wins when >= floor
 *	    D4c floor clamps at the GUC ceiling (shmem_size equivalence)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_gcs_block_dedup_htab.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-7.2a-gcs-block-dedup-capacity-gc.md (APPROVED 2026-07-09)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "cluster/cluster_conf.h"
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

/* ============================================================
 * Globals consumed by cluster_gcs_block_dedup.o (normally owned by
 * cluster_guc.c / globals.c).
 * ============================================================ */

bool cluster_enabled = true;
int cluster_node_id = 0;
int cluster_gcs_block_dedup_max_entries = 4;
int cluster_gcs_block_retransmit_max_retries = 4;
int cluster_gcs_block_retransmit_initial_backoff_ms = 100;
int MaxConnections = 1;
bool IsUnderPostmaster = false;
BackendId MyBackendId = InvalidBackendId;

/* Controlled fake clock (no sleeps: window aging is simulated). */
static TimestampTz fake_now = 1000000000;

/* Controlled declared-node-count sniff (spec-7.2a D4 auto-size input). */
static int fake_declared_nodes = 1;

/*
 * With backoff 100ms x (2^4 - 1) = 1500ms total budget, the 2x
 * out-of-window threshold used by both eager reclaim and the TTL sweep is
 * 3,000,000 us.  Tests age entries past it by advancing fake_now.
 */
#define UT_OUT_OF_WINDOW_US INT64CONST(3000000)

/* ============================================================
 * Stubs to link cluster_gcs_block_dedup.o standalone.
 * ============================================================ */

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

bool
errstart(int e pg_attribute_unused(), const char *d pg_attribute_unused())
{
	return false;
}

bool
errstart_cold(int e pg_attribute_unused(), const char *d pg_attribute_unused())
{
	return false;
}

void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{}

int
errcode(int s pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *f pg_attribute_unused(), ...)
{
	return 0;
}

int
errmsg_internal(const char *f pg_attribute_unused(), ...)
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
cluster_conf_declared_node_count_early(void)
{
	return fake_declared_nodes;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *r pg_attribute_unused())
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

/* spec-7.3 D5: the sharded dedup sizes/initialises per LMS worker; a single
 * shard keeps these tests' capacity math identical to the pre-shard shape. */
int cluster_lms_workers = 1;

/* Linear estimate is enough for the D4c size-equivalence assertion. */
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
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

/* ============================================================
 * Bounded fake shmem HTAB (L105 union force-align).  Unlike the plain
 * append-only sibling fixture (test_cluster_bufmgr_pcm_hook.c) this one
 * supports HASH_REMOVE with hole reuse, because the unit under test is
 * exactly the remove-then-re-register eager-reclaim chain.
 * ============================================================ */

#define FAKE_DEDUP_MAX_SLOTS 32

static union {
	uint64 force_align;
	char data[4096]; /* generous; sizeof(ClusterGcsBlockDedupShared) << 4KB */
} fake_dedup_header;

static union {
	uint64 force_align;
	char data[FAKE_DEDUP_MAX_SLOTS][sizeof(GcsBlockDedupEntry)];
} fake_dedup_slots;

static bool fake_slot_used[FAKE_DEDUP_MAX_SLOTS];
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
	Assert(max_size <= FAKE_DEDUP_MAX_SLOTS);
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

	for (i = 0; i < FAKE_DEDUP_MAX_SLOTS; i++)
		if (fake_slot_used[i])
			n++;
	return n;
}

void *
hash_search(HTAB *hashp pg_attribute_unused(), const void *keyPtr, HASHACTION action,
			bool *foundPtr)
{
	long i;

	Assert(fake_dedup_keysize > 0);

	for (i = 0; i < FAKE_DEDUP_MAX_SLOTS; i++) {
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

	for (i = 0; i < FAKE_DEDUP_MAX_SLOTS; i++) {
		if (!fake_slot_used[i]) {
			/* dynahash contract: only the key is initialised on ENTER; the
			 * value area keeps whatever was there before (the module must
			 * reset it itself). */
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

/*
 * Slot-index iteration that skips holes.  Removing the element the scan
 * just returned (what the eager reclaim does) cannot disturb the remaining
 * iteration, matching dynahash's guarantee for HASH_REMOVE of the current
 * seq element.
 */
void
hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp pg_attribute_unused())
{
	status->curBucket = 0;
	status->hashp = NULL;
}

void *
hash_seq_search(HASH_SEQ_STATUS *status)
{
	while (status->curBucket < FAKE_DEDUP_MAX_SLOTS) {
		uint32 i = status->curBucket++;

		if (fake_slot_used[i])
			return fake_dedup_slots.data[i];
	}
	return NULL;
}

void
hash_seq_term(HASH_SEQ_STATUS *status pg_attribute_unused())
{}

/* ============================================================
 * Fixture helpers.
 * ============================================================ */

static void
fixture_reset(int configured, int max_conns, int declared_nodes)
{
	fake_dedup_header_found = false;
	fake_dedup_entry_max = 0;
	fake_dedup_keysize = 0;
	memset(fake_slot_used, 0, sizeof(fake_slot_used));
	memset(&fake_dedup_header, 0, sizeof(fake_dedup_header));
	fake_now = 1000000000;

	cluster_enabled = true;
	cluster_node_id = 0;
	cluster_gcs_block_dedup_max_entries = configured;
	MaxConnections = max_conns;
	fake_declared_nodes = declared_nodes;

	Assert(cluster_gcs_block_dedup_shmem_size() > 0);
	cluster_gcs_block_dedup_shmem_init();
}

static GcsBlockDedupKey
make_key(uint64 request_id)
{
	GcsBlockDedupKey key;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = 1;
	key.requester_backend_id = 7;
	key.request_id = request_id;
	key.cluster_epoch = 3;
	return key;
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

static GcsBlockDedupResult
register_key(uint64 request_id)
{
	GcsBlockDedupKey key = make_key(request_id);
	BufferTag tag = make_tag((uint32)request_id);

	return cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, 1, NULL);
}

static void
complete_key(uint64 request_id, GcsBlockReplyStatus status)
{
	GcsBlockDedupKey key = make_key(request_id);
	GcsBlockReplyHeader header;

	memset(&header, 0, sizeof(header));
	cluster_gcs_block_dedup_install_reply(0, &key, status, &header, NULL);
}

/* ============================================================
 * U1-U6: cap pressure -> eager reclaim -> re-register (one flow, four
 * cap-4 tests sharing fixture state built by the previous step, exactly
 * like the production MISS path would see it).
 * ============================================================ */

UT_TEST(test_u1_fill_to_cap_in_flight_full_fail_closed)
{
	uint64 k;

	fixture_reset(4, 1, 1); /* floor = 1x1 = 1 < 4 -> cap 4 */
	UT_ASSERT_EQ(4, (int)cluster_gcs_block_dedup_get_max_entries());

	for (k = 0; k < 4; k++)
		UT_ASSERT_EQ(GCS_BLOCK_DEDUP_MISS_REGISTERED, register_key(k));
	UT_ASSERT_EQ(4, (int)cluster_gcs_block_dedup_get_in_flight_count());

	/* 5th distinct key: every resident entry is in-flight, which is never
	 * reclaim-safe -> eager reclaim must NOT fire -> FULL fail-closed. */
	UT_ASSERT_EQ(GCS_BLOCK_DEDUP_FULL, register_key(4));
	UT_ASSERT_EQ(1, (int)cluster_gcs_block_dedup_get_full_count());
	UT_ASSERT_EQ(0, (int)cluster_gcs_block_dedup_get_evict_count());
	UT_ASSERT_EQ(4, (int)cluster_gcs_block_dedup_get_in_flight_count());
	UT_ASSERT_EQ(4, (int)fake_live_count());
}

UT_TEST(test_u3_u4_completed_in_window_still_full)
{
	/* Complete all four entries NOW (in-window).  k1 completes as
	 * storage-fallback (U4: :3305/:4089 transition sites -> never
	 * in-window reclaimable); the rest as payload GRANTED (U3). */
	complete_key(0, GCS_BLOCK_REPLY_GRANTED);
	complete_key(1, GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK);
	complete_key(2, GCS_BLOCK_REPLY_GRANTED);
	complete_key(3, GCS_BLOCK_REPLY_GRANTED);

	/* Still within the 2x window: whitelist is empty, so no in-window
	 * reclaim of ANY status -> FULL again. */
	UT_ASSERT_EQ(GCS_BLOCK_DEDUP_FULL, register_key(4));
	UT_ASSERT_EQ(2, (int)cluster_gcs_block_dedup_get_full_count());
	UT_ASSERT_EQ(0, (int)cluster_gcs_block_dedup_get_evict_count());
	UT_ASSERT_EQ(4, (int)cluster_gcs_block_dedup_get_in_flight_count());
}

UT_TEST(test_u2_out_of_window_miss_triggers_eager_reclaim)
{
	uint64 miss_before = cluster_gcs_block_dedup_get_miss_count();

	/* Age every completed entry past the 2x out-of-window threshold.  The
	 * TTL sweep is never invoked in this binary, so the eviction observed
	 * below is attributable to the MISS-path eager reclaim alone. */
	fake_now += UT_OUT_OF_WINDOW_US + 1000;

	UT_ASSERT_EQ(GCS_BLOCK_DEDUP_MISS_REGISTERED, register_key(4));

	/* full_count did NOT grow (still 2 from U1/U3); exactly one reclaim. */
	UT_ASSERT_EQ(2, (int)cluster_gcs_block_dedup_get_full_count());
	UT_ASSERT_EQ(1, (int)cluster_gcs_block_dedup_get_evict_count());
	UT_ASSERT_EQ(miss_before + 1, cluster_gcs_block_dedup_get_miss_count());

	/* Accounting conserved at cap: one out, one in. */
	UT_ASSERT_EQ(4, (int)cluster_gcs_block_dedup_get_in_flight_count());
	UT_ASSERT_EQ(4, (int)fake_live_count());
}

UT_TEST(test_u5_reclaimed_key_re_registers_without_double_count)
{
	GcsBlockDedupKey key0 = make_key(0);
	BufferTag tag0 = make_tag(0);
	GcsBlockDedupEntry cached;

	/* U2 evicted the scan-order-first safe entry: key 0 (slot 0).  A late
	 * duplicate of key 0 must re-register through MISS (not CACHED_REPLY /
	 * IN_FLIGHT_DUPLICATE) and must not double-count: the table is full
	 * again, so its registration eager-reclaims exactly one more aged
	 * completed entry. */
	UT_ASSERT_EQ(GCS_BLOCK_DEDUP_MISS_REGISTERED,
				 cluster_gcs_block_dedup_lookup_or_register(0, &key0, tag0, 1, NULL));
	UT_ASSERT_EQ(2, (int)cluster_gcs_block_dedup_get_evict_count());
	UT_ASSERT_EQ(2, (int)cluster_gcs_block_dedup_get_full_count());
	UT_ASSERT_EQ(4, (int)cluster_gcs_block_dedup_get_in_flight_count());
	UT_ASSERT_EQ(4, (int)fake_live_count());
	UT_ASSERT_EQ(0, (int)cluster_gcs_block_dedup_get_hit_count());

	/* Dedup still works on the re-registered entry: complete it, then a
	 * same-key retransmit hits the cached reply. */
	complete_key(0, GCS_BLOCK_REPLY_GRANTED);
	UT_ASSERT_EQ(GCS_BLOCK_DEDUP_CACHED_REPLY,
				 cluster_gcs_block_dedup_lookup_or_register(0, &key0, tag0, 1, &cached));
	UT_ASSERT_EQ(1, (int)cluster_gcs_block_dedup_get_hit_count());
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_GRANTED, (int)cached.status);
}

UT_TEST(test_u6_remove_keeps_accounting_conserved)
{
	GcsBlockDedupKey key0 = make_key(0);

	cluster_gcs_block_dedup_remove(0, &key0);
	UT_ASSERT_EQ(3, (int)cluster_gcs_block_dedup_get_in_flight_count());
	UT_ASSERT_EQ(3, (int)fake_live_count());

	/* Removing a key that is no longer resident must not underflow. */
	cluster_gcs_block_dedup_remove(0, &key0);
	UT_ASSERT_EQ(3, (int)cluster_gcs_block_dedup_get_in_flight_count());
	UT_ASSERT_EQ(3, (int)fake_live_count());
}

/* ============================================================
 * D4: auto-size floor (spec-7.2a Q4=A; review F3).
 * ============================================================ */

UT_TEST(test_d4a_autosize_floor_binds_small_config)
{
	/* configured 4 < floor 8x2 = 16 -> the HTAB is built (and stamped)
	 * with the floor. */
	fixture_reset(4, 8, 2);
	UT_ASSERT_EQ(16, (int)cluster_gcs_block_dedup_get_max_entries());
}

UT_TEST(test_d4b_configured_wins_over_floor)
{
	fixture_reset(24, 8, 2); /* floor 16 < configured 24 */
	UT_ASSERT_EQ(24, (int)cluster_gcs_block_dedup_get_max_entries());
}

UT_TEST(test_d4c_floor_clamps_at_guc_ceiling)
{
	Size at_ceiling;
	Size clamped;

	/* Size the region with configured == ceiling and a non-binding floor,
	 * then with a tiny config whose floor (40000 x 2 = 80000) overshoots
	 * the ceiling.  The clamp makes both resolve to the ceiling, so the
	 * reserved sizes must be identical (no init needed: the fake fixture
	 * cannot hold 65536 slots, and D4c is a sizing property). */
	cluster_gcs_block_dedup_max_entries = CLUSTER_GCS_BLOCK_DEDUP_MAX_ENTRIES_CEILING;
	MaxConnections = 1;
	fake_declared_nodes = 1;
	at_ceiling = cluster_gcs_block_dedup_shmem_size();

	cluster_gcs_block_dedup_max_entries = 256;
	MaxConnections = 40000;
	fake_declared_nodes = 2;
	clamped = cluster_gcs_block_dedup_shmem_size();

	UT_ASSERT(at_ceiling > 0);
	UT_ASSERT_EQ(at_ceiling, clamped);
}

UT_TEST(test_d4d_initdb_gate_still_returns_zero)
{
	/* The pre-existing initdb/bootstrap gate must survive the floor:
	 * cluster_node_id < 0 -> no allocation regardless of MaxConnections. */
	cluster_node_id = -1;
	MaxConnections = 40000;
	fake_declared_nodes = 2;
	UT_ASSERT_EQ(0, (int)cluster_gcs_block_dedup_shmem_size());
	cluster_node_id = 0;
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_u1_fill_to_cap_in_flight_full_fail_closed);
	UT_RUN(test_u3_u4_completed_in_window_still_full);
	UT_RUN(test_u2_out_of_window_miss_triggers_eager_reclaim);
	UT_RUN(test_u5_reclaimed_key_re_registers_without_double_count);
	UT_RUN(test_u6_remove_keeps_accounting_conserved);
	UT_RUN(test_d4a_autosize_floor_binds_small_config);
	UT_RUN(test_d4b_configured_wins_over_floor);
	UT_RUN(test_d4c_floor_clamps_at_guc_ceiling);
	UT_RUN(test_d4d_initdb_gate_still_returns_zero);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
