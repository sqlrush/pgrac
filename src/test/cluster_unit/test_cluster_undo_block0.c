/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_block0.c
 *	  Unit tests for the undo block-zero local identity core.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_undo_block0.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-8.4a-undo-block0-authority-prerequisite.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "miscadmin.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_undo_root_descriptor.h"
#include "cluster/storage/cluster_undo_block0.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "cluster/cluster_undo_smgr.h"
#include "port/atomics.h"
#include "port/pg_pthread.h"
#include "storage/bufpage.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

/* Spec 8.4A P1 closed diagnostic/private-recovery read seam. */
extern ClusterUndoBlock0Result cluster_undo_block0_copy_readonly(
	const ClusterUndoBlock0LogicalKey *logical, const ClusterUndoBlock0ResolvedRoot *read_root,
	const ClusterUndoBlock0Generation *expected, const ClusterUndoBlock0AuthorityProof *proof,
	char private_page[BLCKSZ]);
extern ClusterUndoBlock0Result cluster_undo_block0_provision_begin(
	const ClusterUndoBlock0LogicalKey *logical, const ClusterUndoBlock0ResolvedRoot *target_root,
	const ClusterUndoBlock0AuthorityProof *proof, ClusterUndoBlock0FrameToken *token,
	ClusterUndoBlock0Pin *pin, char **unpublished_page, bool *creator);
extern void cluster_undo_block0_provision_publish(ClusterUndoBlock0Pin *pin, XLogRecPtr init_lsn);
extern void cluster_undo_block0_provision_abort(ClusterUndoBlock0Pin *pin);
extern ClusterUndoBlock0Result
cluster_undo_block0_prove_strict_empty(const ClusterUndoBlock0LogicalKey *logical,
									   const ClusterUndoBlock0AuthorityProof *proof);

/* storage/shmem.h exports checked arithmetic from the backend executable. */
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

/* Standalone shared-memory/content-lock harness for the real D1 object. */
static void *block0_region = NULL;
static Size block0_region_size = 0;
static char smgr_image[BLCKSZ];
static int smgr_read_calls = 0;
static uint32 smgr_last_block = UINT32_MAX;
static bool smgr_read_ok = true;
static int smgr_write_calls = 0;
static uint32 smgr_last_write_block = UINT32_MAX;
static bool smgr_write_ok = true;
static bool smgr_last_do_fsync = false;
static char smgr_written_image[BLCKSZ];
static int xlog_flush_calls = 0;
static XLogRecPtr xlog_last_flush_lsn = InvalidXLogRecPtr;
static int wal_io_order[8];
static int wal_io_order_count = 0;
static sigjmp_buf wal_error_jump;
static bool wal_error_armed = false;
static bool copy_during_fill = false;
static bool empty_probe_during_fill = false;
static bool abort_during_fill = false;
static ClusterUndoBlock0Result copy_during_fill_result = CLUSTER_UNDO_BLOCK0_OK;
static ClusterUndoBlock0Result empty_probe_during_fill_result = CLUSTER_UNDO_BLOCK0_OK;
static ClusterUndoBlock0LogicalKey fill_logical;
static ClusterUndoBlock0ResolvedRoot fill_root;
static ClusterUndoBlock0AuthorityProof fill_proof;
static ResourceReleaseCallback resource_release_callback = NULL;
static void *resource_release_arg = NULL;
static int lwlock_acquire_calls = 0;
static int lwlock_throw_on_call = 0;
static int lwlock_release_without_holdoff_count = 0;
static ClusterUndoSmgrFinalState smgr_probe_state = CLUSTER_UNDO_SMGR_FINAL_EXACT;
static ClusterUndoSmgrPublishResult smgr_publish_result = CLUSTER_UNDO_SMGR_PUBLISH_PUBLISHED;
static char smgr_probe_image[BLCKSZ];
static int smgr_probe_calls = 0;
static int smgr_temp_create_calls = 0;
static int smgr_temp_publish_calls = 0;
static int smgr_temp_cleanup_calls = 0;
static bool smgr_temp_active = false;
static ClusterR4PrerequisiteSnapshot r4_owner_snapshot = {
	.status = CLUSTER_R4_PREREQUISITE_RF_DEFERRED,
	.ready = false,
	.reserved0 = { 0, 0, 0 },
	.target_node_id = -1,
};
static bool r4_owner_publish_enabled = false;
static bool r4_startup_fenced_owned = false;
static bool normal_start_closed = false;
int cluster_node_id = 0;

ResourceOwner CurrentResourceOwner = (ResourceOwner)(uintptr_t)1;
ResourceOwner CurTransactionResourceOwner = NULL;
ResourceOwner TopTransactionResourceOwner = NULL;
ResourceOwner AuxProcessResourceOwner = NULL;
MemoryContext TopMemoryContext = (MemoryContext)(uintptr_t)1;
volatile uint32 InterruptHoldoffCount = 0;
bool IsUnderPostmaster = false;
BackendType MyBackendType = B_INVALID;
AuxProcType MyAuxProcType = NotAnAuxProcess;

/* Standalone substitute for the reconfiguration owner's lock co-sample. */
ClusterR4PrerequisiteSnapshot
cluster_reconfig_r4_prerequisite_snapshot(void)
{
	return r4_owner_snapshot;
}

bool
cluster_reconfig_r4_publish_ready(const ClusterR4PrerequisiteSnapshot *expected)
{
	return r4_owner_publish_enabled && expected != NULL
		   && memcmp(expected, &r4_owner_snapshot, sizeof(*expected)) == 0;
}

bool
cluster_undo_block0_current_startup_fenced_owned(void)
{
	return r4_startup_fenced_owned;
}

/* The semantic closed-pass co-sample is tested against its real producer
 * separately. Resident decisions, pins, frames and cleanup below are real. */
bool
cluster_semantic_normal_start_closed(void)
{
	return normal_start_closed;
}

void *
MemoryContextAlloc(MemoryContext context pg_attribute_unused(), Size size)
{
	return malloc(size);
}

void
pfree(void *pointer)
{
	free(pointer);
}

void
RegisterResourceReleaseCallback(ResourceReleaseCallback callback, void *arg)
{
	resource_release_callback = callback;
	resource_release_arg = arg;
}

typedef struct TestClusterUndoBlock0Ctl {
	LWLock frame_lock;
	uint32 frame_count;
	uint32 free_count;
	uint8 reserved[104];
} TestClusterUndoBlock0Ctl;

StaticAssertDecl(sizeof(TestClusterUndoBlock0Ctl) == 128,
				 "test block-zero control header must match the product layout");

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	lwlock_acquire_calls++;
	if (lwlock_throw_on_call > 0 && lwlock_acquire_calls == lwlock_throw_on_call) {
		UT_ASSERT_NOT_NULL(PG_exception_stack);
		siglongjmp(*PG_exception_stack, 1);
	}
	HOLD_INTERRUPTS();
	return true;
}

bool
LWLockConditionalAcquire(LWLock *lock, LWLockMode mode)
{
	return LWLockAcquire(lock, mode);
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{
	if (InterruptHoldoffCount == 0) {
		lwlock_release_without_holdoff_count++;
		return;
	}
	RESUME_INTERRUPTS();
}

bool
cluster_undo_smgr_read_block(ClusterUndoPathIntent intent pg_attribute_unused(),
							 uint32 segment_id pg_attribute_unused(),
							 uint8 owner_instance pg_attribute_unused(), uint32 block_no, char *buf)
{
	smgr_read_calls++;
	smgr_last_block = block_no;
	if (copy_during_fill) {
		char private_page[BLCKSZ];

		copy_during_fill = false;
		copy_during_fill_result = cluster_undo_block0_copy_resident(
			&fill_logical, &fill_root, NULL, &fill_proof, private_page, NULL);
	}
	if (empty_probe_during_fill) {
		empty_probe_during_fill = false;
		empty_probe_during_fill_result
			= cluster_undo_block0_prove_strict_empty(&fill_logical, &fill_proof);
	}
	if (abort_during_fill) {
		abort_during_fill = false;
		UT_ASSERT_NOT_NULL(resource_release_callback);
		if (resource_release_callback != NULL)
			resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false,
									  resource_release_arg);
		siglongjmp(wal_error_jump, 1);
	}
	if (!smgr_read_ok)
		return false;
	memcpy(buf, smgr_image, BLCKSZ);
	return true;
}

bool
cluster_undo_smgr_write_block(ClusterUndoPathIntent intent pg_attribute_unused(),
							  uint32 segment_id pg_attribute_unused(),
							  uint8 owner_instance pg_attribute_unused(), uint32 block_no,
							  const char *buf, bool do_fsync)
{
	smgr_write_calls++;
	smgr_last_write_block = block_no;
	smgr_last_do_fsync = do_fsync;
	if (wal_io_order_count < (int)lengthof(wal_io_order))
		wal_io_order[wal_io_order_count++] = 2;
	if (buf != NULL)
		memcpy(smgr_written_image, buf, BLCKSZ);
	return smgr_write_ok;
}

ClusterUndoSmgrFinalState
cluster_undo_smgr_probe_segment(ClusterUndoPathIntent intent pg_attribute_unused(),
								uint32 segment_id pg_attribute_unused(),
								uint8 owner_instance pg_attribute_unused(), char block0[BLCKSZ])
{
	smgr_probe_calls++;
	if (smgr_probe_state == CLUSTER_UNDO_SMGR_FINAL_EXACT)
		memcpy(block0, smgr_probe_image, BLCKSZ);
	return smgr_probe_state;
}

bool
cluster_undo_smgr_provision_temp_create(ClusterUndoPathIntent intent pg_attribute_unused(),
										uint32 segment_id pg_attribute_unused(),
										uint8 owner_instance pg_attribute_unused(),
										char temp_path[MAXPGPATH])
{
	smgr_temp_create_calls++;
	strlcpy(temp_path, "/tmp/pgrac-b0-test-temp", MAXPGPATH);
	smgr_temp_active = true;
	return true;
}

ClusterUndoSmgrPublishResult
cluster_undo_smgr_provision_temp_publish(ClusterUndoPathIntent intent pg_attribute_unused(),
										 uint32 segment_id pg_attribute_unused(),
										 uint8 owner_instance pg_attribute_unused(),
										 const char *temp_path pg_attribute_unused(),
										 const char block0[BLCKSZ] pg_attribute_unused())
{
	smgr_temp_publish_calls++;
	if (smgr_publish_result != CLUSTER_UNDO_SMGR_PUBLISH_IO_ERROR)
		smgr_temp_active = false;
	if (smgr_publish_result == CLUSTER_UNDO_SMGR_PUBLISH_EXISTS)
		smgr_probe_state = CLUSTER_UNDO_SMGR_FINAL_EXACT;
	return smgr_publish_result;
}

bool
cluster_undo_smgr_provision_temp_cleanup(ClusterUndoPathIntent intent pg_attribute_unused(),
										 uint32 segment_id pg_attribute_unused(),
										 uint8 owner_instance pg_attribute_unused(),
										 const char *temp_path pg_attribute_unused())
{
	smgr_temp_cleanup_calls++;
	smgr_temp_active = false;
	return true;
}

void
XLogFlush(XLogRecPtr record)
{
	xlog_flush_calls++;
	xlog_last_flush_lsn = record;
	if (wal_io_order_count < (int)lengthof(wal_io_order))
		wal_io_order[wal_io_order_count++] = 1;
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	return elevel >= ERROR;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/* Linux arm64 libpgport's runtime CRC selection uses the internal variant. */
int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	if (wal_error_armed)
		siglongjmp(wal_error_jump, 1);
	abort();
}

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

static ClusterUndoBlock0LogicalKey
make_key(uint8 owner_instance, uint32 segment_id)
{
	ClusterUndoBlock0LogicalKey key;

	key.owner_instance = owner_instance;
	key.segment_id = segment_id;
	return key;
}

static ClusterUndoBlock0ResolvedRoot
make_root(ClusterUndoPathIntent intent, uint64 root_id, uint64 root_generation)
{
	ClusterUndoBlock0ResolvedRoot root;

	root.intent = intent;
	root.root_id = root_id;
	root.root_generation = root_generation;
	return root;
}

static ClusterUndoBlock0Generation
make_generation(bool known, uint32 value)
{
	ClusterUndoBlock0Generation generation;

	generation.known = known;
	generation.value = value;
	return generation;
}

static ClusterUndoBlock0AuthorityProof
make_live_proof(uint8 owner_instance, uint64 cluster_epoch)
{
	ClusterUndoBlock0AuthorityProof proof;

	memset(&proof, 0, sizeof(proof));
	proof.kind = CLUSTER_UNDO_BLOCK0_LIVE_OWNER;
	proof.owner_instance = owner_instance;
	proof.cluster_epoch_present = true;
	proof.cluster_epoch = cluster_epoch;
	return proof;
}

static ClusterUndoBlock0AuthorityProof
make_recovery_proof(uint8 owner_instance, uint64 cluster_epoch, uint64 recovery_generation)
{
	ClusterUndoBlock0AuthorityProof proof;

	memset(&proof, 0, sizeof(proof));
	proof.kind = CLUSTER_UNDO_BLOCK0_RECOVERY_OWNER;
	proof.owner_instance = owner_instance;
	proof.cluster_epoch_present = true;
	proof.cluster_epoch = cluster_epoch;
	proof.recovery_generation = recovery_generation;
	return proof;
}

static void
make_valid_block0(uint32 segment_id, uint8 owner_instance, uint32 generation, uint8 marker)
{
	PageHeader ph;
	UndoSegmentHeaderData *hdr;

	memset(smgr_image, marker, sizeof(smgr_image));
	ph = (PageHeader)smgr_image;
	hdr = (UndoSegmentHeaderData *)smgr_image;
	ph->pd_flags = PD_UNDO_SEG_HEADER;
	ph->pd_lower = SizeOfPageHeaderData;
	ph->pd_upper = BLCKSZ;
	ph->pd_special = BLCKSZ;
	PageSetPageSizeAndVersion((Page)smgr_image, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	hdr->segment_id = segment_id;
	hdr->segment_size_bytes = UNDO_SEGMENT_SIZE_BYTES;
	hdr->segment_state = SEGMENT_ACTIVE;
	hdr->owner_instance = owner_instance;
	hdr->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	hdr->wrap_count = generation;
}

static void
fresh_block0_region(uint32 frame_count)
{
	if (block0_region != NULL)
		free(block0_region);
	block0_region_size = cluster_undo_block0_shmem_size(frame_count);
	block0_region = malloc(block0_region_size);
	UT_ASSERT_NOT_NULL(block0_region);
	memset(block0_region, 0xa5, block0_region_size);
	cluster_undo_block0_shmem_init_region(block0_region, block0_region_size, frame_count, false);
	smgr_read_calls = 0;
	smgr_last_block = UINT32_MAX;
	smgr_read_ok = true;
	smgr_write_calls = 0;
	smgr_last_write_block = UINT32_MAX;
	smgr_write_ok = true;
	smgr_last_do_fsync = false;
	memset(smgr_written_image, 0, sizeof(smgr_written_image));
	xlog_flush_calls = 0;
	xlog_last_flush_lsn = InvalidXLogRecPtr;
	memset(wal_io_order, 0, sizeof(wal_io_order));
	wal_io_order_count = 0;
	wal_error_armed = false;
	copy_during_fill = false;
	empty_probe_during_fill = false;
	abort_during_fill = false;
	copy_during_fill_result = CLUSTER_UNDO_BLOCK0_OK;
	empty_probe_during_fill_result = CLUSTER_UNDO_BLOCK0_OK;
	lwlock_acquire_calls = 0;
	lwlock_throw_on_call = 0;
	smgr_probe_state = CLUSTER_UNDO_SMGR_FINAL_EXACT;
	smgr_publish_result = CLUSTER_UNDO_SMGR_PUBLISH_PUBLISHED;
	memcpy(smgr_probe_image, smgr_image, BLCKSZ);
	smgr_probe_calls = 0;
	smgr_temp_create_calls = 0;
	smgr_temp_publish_calls = 0;
	smgr_temp_cleanup_calls = 0;
	smgr_temp_active = false;
}

static bool
r4_prerequisite_snapshot_is_fixed_false(const ClusterR4PrerequisiteSnapshot *snapshot)
{
	return snapshot != NULL && snapshot->status == CLUSTER_R4_PREREQUISITE_RF_DEFERRED
		   && !snapshot->ready && snapshot->reserved0[0] == 0 && snapshot->reserved0[1] == 0
		   && snapshot->reserved0[2] == 0 && snapshot->target_node_id == -1
		   && snapshot->episode_state_generation == 0 && snapshot->jcmk_generation == 0
		   && snapshot->request_nonce == 0 && snapshot->old_admitted_incarnation == 0
		   && snapshot->fresh_incarnation == 0 && snapshot->committed_epoch == 0
		   && snapshot->grammar_fingerprint == 0;
}

#define R4_PREREQUISITE_THREAD_COUNT 8
#define R4_PREREQUISITE_CALLS_PER_THREAD 10000

typedef struct R4PrerequisiteThreadResult {
	pg_atomic_uint32 *ready;
	pg_atomic_uint32 *start;
	uint32 calls;
	uint32 mismatches;
} R4PrerequisiteThreadResult;

static void *
r4_prerequisite_snapshot_caller(void *arg)
{
	R4PrerequisiteThreadResult *result = (R4PrerequisiteThreadResult *)arg;
	int i;

	pg_atomic_fetch_add_u32(result->ready, 1);
	while (pg_atomic_read_u32(result->start) == 0)
		;

	for (i = 0; i < R4_PREREQUISITE_CALLS_PER_THREAD; i++) {
		ClusterR4PrerequisiteSnapshot snapshot;

		snapshot = cluster_undo_block0_r4_prerequisite_snapshot();
		result->calls++;
		if (!r4_prerequisite_snapshot_is_fixed_false(&snapshot))
			result->mismatches++;
	}

	return NULL;
}

UT_TEST(test_block0_key_endpoints_map_to_direct_slots)
{
	ClusterUndoBlock0LogicalKey key;
	uint32 slot = UINT32_MAX;

	key = make_key(1, 1);
	UT_ASSERT_EQ(cluster_undo_block0_logical_slot(&key, &slot), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(slot, 0);

	key = make_key(1, 256);
	UT_ASSERT_EQ(cluster_undo_block0_logical_slot(&key, &slot), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(slot, 255);

	key = make_key(128, 32513);
	UT_ASSERT_EQ(cluster_undo_block0_logical_slot(&key, &slot), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(slot, 32512);

	key = make_key(128, 32768);
	UT_ASSERT_EQ(cluster_undo_block0_logical_slot(&key, &slot), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(slot, 32767);
}

UT_TEST(test_block0_key_rejects_owner_segment_aliases)
{
	const ClusterUndoBlock0LogicalKey invalid[] = {
		{ .segment_id = 1, .owner_instance = 0 },	{ .segment_id = 1, .owner_instance = 129 },
		{ .segment_id = 0, .owner_instance = 1 },	{ .segment_id = 257, .owner_instance = 1 },
		{ .segment_id = 256, .owner_instance = 2 }, { .segment_id = 32769, .owner_instance = 128 },
	};
	uint32 slot;
	int i;

	for (i = 0; i < lengthof(invalid); i++) {
		slot = UINT32_MAX;
		UT_ASSERT_EQ(cluster_undo_block0_logical_slot(&invalid[i], &slot),
					 CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
		UT_ASSERT_EQ(slot, UINT32_MAX);
	}
	UT_ASSERT_EQ(cluster_undo_block0_logical_slot(NULL, &slot),
				 CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
	UT_ASSERT_EQ(cluster_undo_block0_logical_slot(&invalid[0], NULL),
				 CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
}

UT_TEST(test_block0_root_requires_pgrd_identity_and_declared_intent)
{
	ClusterUndoBlock0ResolvedRoot root;

	root = make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1);
	UT_ASSERT(cluster_undo_block0_root_valid(&root));
	root = make_root(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, 7, 1);
	UT_ASSERT(cluster_undo_block0_root_valid(&root));
	root = make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED_AUTHORITY_BLOCK0, 9, 3);
	UT_ASSERT(cluster_undo_block0_root_valid(&root));
	root.root_id = 0;
	UT_ASSERT(!cluster_undo_block0_root_valid(&root));
	root.root_id = 9;
	root.root_generation = 0;
	UT_ASSERT(!cluster_undo_block0_root_valid(&root));
	root.root_generation = 3;
	root.intent = (ClusterUndoPathIntent)3;
	UT_ASSERT(!cluster_undo_block0_root_valid(&root));
	UT_ASSERT(!cluster_undo_block0_root_valid(NULL));
}

UT_TEST(test_block0_root_match_is_field_exact)
{
	ClusterUndoBlock0ResolvedRoot observed = make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 7, 9);
	ClusterUndoBlock0ResolvedRoot expected = observed;

	UT_ASSERT(cluster_undo_block0_root_matches(&observed, &expected));
	expected.intent = CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL;
	UT_ASSERT(!cluster_undo_block0_root_matches(&observed, &expected));
	expected = observed;
	expected.root_id++;
	UT_ASSERT(!cluster_undo_block0_root_matches(&observed, &expected));
	expected = observed;
	expected.root_generation++;
	UT_ASSERT(!cluster_undo_block0_root_matches(&observed, &expected));
	UT_ASSERT(!cluster_undo_block0_root_matches(NULL, &expected));
}

UT_TEST(test_block0_generation_keeps_zero_distinct_from_absent)
{
	ClusterUndoBlock0Generation unknown = make_generation(false, 0);
	ClusterUndoBlock0Generation known_zero = make_generation(true, 0);
	ClusterUndoBlock0Generation known_one = make_generation(true, 1);

	UT_ASSERT(cluster_undo_block0_generation_matches(&unknown, &unknown));
	UT_ASSERT(cluster_undo_block0_generation_matches(&known_zero, &unknown));
	UT_ASSERT(!cluster_undo_block0_generation_matches(&unknown, &known_zero));
	UT_ASSERT(cluster_undo_block0_generation_matches(&known_zero, &known_zero));
	UT_ASSERT(!cluster_undo_block0_generation_matches(&known_one, &known_zero));
	UT_ASSERT(!cluster_undo_block0_generation_matches(NULL, &known_zero));
}

UT_TEST(test_block0_generation_exhaustion_never_wraps)
{
	ClusterUndoBlock0Generation current = make_generation(true, 0);
	ClusterUndoBlock0Generation next = make_generation(false, 77);

	UT_ASSERT(cluster_undo_block0_generation_advance(&current, &next));
	UT_ASSERT(next.known);
	UT_ASSERT_EQ(next.value, 1);

	current = make_generation(true, UINT32_MAX);
	next = make_generation(false, 77);
	UT_ASSERT(!cluster_undo_block0_generation_advance(&current, &next));
	UT_ASSERT(!next.known);
	UT_ASSERT_EQ(next.value, 77);

	current = make_generation(false, 0);
	UT_ASSERT(!cluster_undo_block0_generation_advance(&current, &next));
}

UT_TEST(test_block0_slot_state_allows_only_frozen_edges)
{
	UT_ASSERT(cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_EMPTY,
														   CLUSTER_UNDO_BLOCK0_SLOT_FILLING));
	UT_ASSERT(cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_FILLING,
														   CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN));
	UT_ASSERT(cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_FILLING,
														   CLUSTER_UNDO_BLOCK0_SLOT_EMPTY));
	UT_ASSERT(cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN,
														   CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY));
	UT_ASSERT(cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY,
														   CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN));
	UT_ASSERT(cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN,
														   CLUSTER_UNDO_BLOCK0_SLOT_RETIRING));
	UT_ASSERT(cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY,
														   CLUSTER_UNDO_BLOCK0_SLOT_RETIRING));
	UT_ASSERT(cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_RETIRING,
														   CLUSTER_UNDO_BLOCK0_SLOT_EMPTY));
}

UT_TEST(test_block0_slot_state_rejects_direct_publish_and_double_publish)
{
	UT_ASSERT(!cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_EMPTY,
															CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN));
	UT_ASSERT(!cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_EMPTY,
															CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY));
	UT_ASSERT(!cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_EMPTY,
															CLUSTER_UNDO_BLOCK0_SLOT_RETIRING));
	UT_ASSERT(!cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN,
															CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN));
	UT_ASSERT(!cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY,
															CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY));
	UT_ASSERT(!cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_RETIRING,
															CLUSTER_UNDO_BLOCK0_SLOT_FILLING));
	UT_ASSERT(!cluster_undo_block0_state_transition_allowed((ClusterUndoBlock0SlotState)-1,
															CLUSTER_UNDO_BLOCK0_SLOT_EMPTY));
}

UT_TEST(test_block0_checked_size_matches_frozen_default_increment)
{
	UT_ASSERT_EQ(cluster_undo_block0_shmem_size(2048), (Size)20979840);
	UT_ASSERT_EQ(cluster_undo_block0_shmem_size(0), (Size)0);
}

UT_TEST(test_block0_attach_mismatch_detaches_fail_closed)
{
	ClusterUndoBlock0FrameToken token;
	Size one_frame_size = cluster_undo_block0_shmem_size(1);

	fresh_block0_region(2);
	token.frame_index = 0;
	token.owned = true;
	UT_ASSERT(!cluster_undo_block0_shmem_init_region(block0_region, one_frame_size, 1, true));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token),
				 CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE);
	UT_ASSERT(!token.owned);
	UT_ASSERT_EQ(token.frame_index, UINT32_MAX);
}

UT_TEST(test_block0_attach_rejects_impossible_free_count)
{
	ClusterUndoBlock0FrameToken token;
	TestClusterUndoBlock0Ctl *ctl;

	fresh_block0_region(1);
	ctl = (TestClusterUndoBlock0Ctl *)block0_region;
	ctl->free_count = ctl->frame_count + 1;
	token.frame_index = 0;
	token.owned = true;
	UT_ASSERT(!cluster_undo_block0_shmem_init_region(block0_region, block0_region_size, 1, true));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token),
				 CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE);
	UT_ASSERT(!token.owned);
	UT_ASSERT_EQ(token.frame_index, UINT32_MAX);
}

UT_TEST(test_block0_frame_bank_is_sparse_and_all_or_none)
{
	ClusterUndoBlock0FrameToken tokens[2];
	ClusterUndoBlock0FrameToken retry;

	fresh_block0_region(2);
	memset(tokens, 0, sizeof(tokens));
	memset(&retry, 0, sizeof(retry));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(2, tokens), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(tokens[0].owned);
	UT_ASSERT(tokens[1].owned);
	UT_ASSERT(tokens[0].frame_index != tokens[1].frame_index);
	retry.frame_index = tokens[0].frame_index;
	retry.owned = true;
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &retry),
				 CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE);
	UT_ASSERT(!retry.owned);
	UT_ASSERT_EQ(retry.frame_index, UINT32_MAX);

	cluster_undo_block0_frame_release(&tokens[0]);
	UT_ASSERT(!tokens[0].owned);
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &retry), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(retry.owned);
	cluster_undo_block0_frame_release(&retry);
	cluster_undo_block0_frame_release(&tokens[1]);
}

UT_TEST(test_block0_runtime_admission_preserves_generation_zero_and_exact_identity)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x1111), 7);
	ClusterUndoBlock0ResolvedRoot read_only_root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED_AUTHORITY_BLOCK0, UINT64CONST(0x1111), 7);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 0);
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0Pin pin;
	ClusterUndoBlock0Generation expected_zero = make_generation(true, 0);
	ClusterUndoBlock0Generation observed = make_generation(false, 99);
	ClusterUndoBlock0Generation sampled = make_generation(false, 88);
	char copied[BLCKSZ];
	char *page = NULL;
	uint32 admitted_frame;

	fresh_block0_region(2);
	make_valid_block0(1, 1, 0, 0x4d);
	memset(&token, 0, sizeof(token));
	memset(&pin, 0, sizeof(pin));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	admitted_frame = token.frame_index;
	UT_ASSERT_EQ(
		cluster_undo_block0_admit_runtime(&logical, &read_only_root, &proof, &token, &pin, &page),
		CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT(token.owned);
	UT_ASSERT_EQ(smgr_read_calls, 0);
	UT_ASSERT_EQ(cluster_undo_block0_admit_runtime(&logical, &root, &proof, &token, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(!token.owned);
	UT_ASSERT_NOT_NULL(page);
	UT_ASSERT(pin.observed_generation.known);
	UT_ASSERT_EQ(pin.observed_generation.value, 0);
	UT_ASSERT_EQ(smgr_read_calls, 1);
	UT_ASSERT_EQ(smgr_last_block, 0);
	cluster_undo_block0_unpin(&pin);

	smgr_read_calls = 0;
	UT_ASSERT_EQ(cluster_undo_block0_sample_resident_generation(&logical, &root, &proof, &sampled),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(sampled.known);
	UT_ASSERT_EQ(sampled.value, 0);
	UT_ASSERT_EQ(smgr_read_calls, 0);
	memset(copied, 0xa5, sizeof(copied));
	UT_ASSERT_EQ(cluster_undo_block0_copy_resident(&logical, &root, &expected_zero, &proof, copied,
												   &observed),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(observed.known);
	UT_ASSERT_EQ(observed.value, 0);
	UT_ASSERT_EQ((int)(unsigned char)copied[BLCKSZ - 1], 0x4d);
	UT_ASSERT_EQ(smgr_read_calls, 0);

	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(token.frame_index != admitted_frame);
	cluster_undo_block0_frame_release(&token);
}

UT_TEST(test_block0_unpin_after_error_interrupt_reset_is_balanced)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x1112), 7);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 0);
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0Pin pin;
	char *page = NULL;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 0, 0x4e);
	memset(&token, 0, sizeof(token));
	memset(&pin, 0, sizeof(pin));
	lwlock_release_without_holdoff_count = 0;
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(cluster_undo_block0_admit_runtime(&logical, &root, &proof, &token, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(InterruptHoldoffCount, 1);

	/* ereport(ERROR) resets the process holdoff counter before PG_FINALLY. */
	InterruptHoldoffCount = 0;
	cluster_undo_block0_unpin(&pin);
	UT_ASSERT_EQ(lwlock_release_without_holdoff_count, 0);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
}

UT_TEST(test_block0_runtime_admission_rejects_exhausted_generation)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x1122), 8);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 1);
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0Pin pin;
	char *page = NULL;

	fresh_block0_region(1);
	make_valid_block0(1, 1, UINT32_MAX, 0x5e);
	memset(&token, 0, sizeof(token));
	memset(&pin, 0, sizeof(pin));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(cluster_undo_block0_admit_runtime(&logical, &root, &proof, &token, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH);
	UT_ASSERT(token.owned);
	UT_ASSERT_NULL(page);
	UT_ASSERT_EQ(pin.slot, -1);
	UT_ASSERT_EQ(smgr_read_calls, 1);
	cluster_undo_block0_frame_release(&token);
}

UT_TEST(test_block0_strict_empty_proof_excludes_filling_and_resident_states)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x1170), 8);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 7);
	ClusterUndoBlock0AuthorityProof wrong = make_live_proof(2, 7);
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0Pin pin;
	char *page = NULL;

	fresh_block0_region(1);
	UT_ASSERT_EQ(cluster_undo_block0_prove_strict_empty(&logical, &proof), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(cluster_undo_block0_prove_strict_empty(&logical, &wrong),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);

	make_valid_block0(1, 1, 0, 0x5f);
	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	fill_logical = logical;
	fill_root = root;
	fill_proof = proof;
	empty_probe_during_fill = true;
	UT_ASSERT_EQ(cluster_undo_block0_admit_runtime(&logical, &root, &proof, &token, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(empty_probe_during_fill_result, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	cluster_undo_block0_unpin(&pin);
	UT_ASSERT_EQ(cluster_undo_block0_prove_strict_empty(&logical, &proof),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
}

UT_TEST(test_block0_copy_readonly_is_recovery_only_and_generation_exact)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, UINT64CONST(0x1180), 3);
	ClusterUndoBlock0AuthorityProof recovery = make_recovery_proof(1, 7, 4);
	ClusterUndoBlock0AuthorityProof live = make_live_proof(1, 7);
	ClusterUndoBlock0Generation zero = make_generation(true, 0);
	ClusterUndoBlock0Generation wrong = make_generation(true, 1);
	char copied[BLCKSZ];

	fresh_block0_region(1);
	make_valid_block0(1, 1, 0, 0x60);
	memset(copied, 0xa5, sizeof(copied));
	UT_ASSERT_EQ(cluster_undo_block0_copy_readonly(&logical, &root, &zero, &recovery, copied),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(smgr_read_calls, 1);
	UT_ASSERT_EQ((int)(unsigned char)copied[BLCKSZ - 1], 0x60);

	smgr_read_calls = 0;
	memset(copied, 0xa5, sizeof(copied));
	UT_ASSERT_EQ(cluster_undo_block0_copy_readonly(&logical, &root, &zero, &live, copied),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(smgr_read_calls, 0);
	UT_ASSERT_EQ((int)(unsigned char)copied[0], 0xa5);

	memset(copied, 0xa5, sizeof(copied));
	UT_ASSERT_EQ(cluster_undo_block0_copy_readonly(&logical, &root, &wrong, &recovery, copied),
				 CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH);
	UT_ASSERT_EQ(smgr_read_calls, 1);
	UT_ASSERT_EQ((int)(unsigned char)copied[0], 0xa5);
}

UT_TEST(test_block0_resident_copy_rejects_empty_generation_and_root_drift_without_io)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0LogicalKey absent = make_key(1, 2);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x2222), 3);
	ClusterUndoBlock0ResolvedRoot drifted_root = root;
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 9);
	ClusterUndoBlock0AuthorityProof drifted_proof = proof;
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0Pin pin;
	ClusterUndoBlock0Generation wrong_generation = make_generation(true, 1);
	ClusterUndoBlock0Pin reserved_pin;
	char copied[BLCKSZ];
	char *page = NULL;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 0, 0x61);
	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(cluster_undo_block0_admit_runtime(&logical, &root, &proof, &token, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_OK);
	cluster_undo_block0_unpin(&pin);
	smgr_read_calls = 0;
	memset(&reserved_pin, 0, sizeof(reserved_pin));
	UT_ASSERT_EQ(cluster_undo_block0_reserve(&logical, &root, &proof, &reserved_pin),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(cluster_undo_block0_lock_content(&reserved_pin, &wrong_generation,
												  CLUSTER_UNDO_BLOCK0_SHARED, &page),
				 CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH);
	UT_ASSERT_EQ(reserved_pin.slot, -1);
	UT_ASSERT_NULL(page);

	memset(copied, 0xa5, sizeof(copied));
	UT_ASSERT_EQ(
		cluster_undo_block0_copy_resident(&logical, &root, &wrong_generation, &proof, copied, NULL),
		CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH);
	UT_ASSERT_EQ((int)(unsigned char)copied[0], 0xa5);

	drifted_root.root_generation++;
	UT_ASSERT_EQ(
		cluster_undo_block0_copy_resident(&logical, &drifted_root, NULL, &proof, copied, NULL),
		CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
	UT_ASSERT_EQ((int)(unsigned char)copied[0], 0xa5);

	drifted_proof.cluster_epoch++;
	UT_ASSERT_EQ(
		cluster_undo_block0_copy_resident(&logical, &root, NULL, &drifted_proof, copied, NULL),
		CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ((int)(unsigned char)copied[0], 0xa5);

	UT_ASSERT_EQ(cluster_undo_block0_copy_resident(&absent, &root, NULL, &proof, copied, NULL),
				 CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED);
	UT_ASSERT_EQ((int)(unsigned char)copied[0], 0xa5);
	UT_ASSERT_EQ(smgr_read_calls, 0);
}

UT_TEST(test_block0_pin_error_drops_reservation_before_rethrow)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x2290), 3);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 9);
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0Pin pin;
	ClusterUndoBlock0ResidentCensusItem item;
	ClusterUndoBlock0Generation expected = make_generation(true, 0);
	char *page = NULL;
	volatile bool caught = false;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 0, 0x62);
	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(cluster_undo_block0_admit_runtime(&logical, &root, &proof, &token, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_OK);
	memset(&item, 0, sizeof(item));
	item.logical = logical;
	item.resolved_root = root;
	item.generation = pin.observed_generation;
	item.proof = proof;
	cluster_undo_block0_unpin(&pin);

	lwlock_acquire_calls = 0;
	lwlock_throw_on_call = 2; /* reserve succeeds; content-X acquire throws */
	PG_TRY();
	{
		(void)cluster_undo_block0_pin(&logical, &root, &expected, CLUSTER_UNDO_BLOCK0_EXCLUSIVE,
									  &proof, &pin, &page);
	}
	PG_CATCH();
	{
		caught = true;
		lwlock_throw_on_call = 0;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(pin.slot, -1);
	UT_ASSERT(cluster_undo_block0_verify_clean_census(&item, 1));
}

UT_TEST(test_block0_filling_is_not_a_positive_resident_copy)
{
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0Pin pin;
	char *page = NULL;

	fresh_block0_region(1);
	fill_logical = make_key(1, 1);
	fill_root = make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x3333), 1);
	fill_proof = make_live_proof(1, 2);
	make_valid_block0(1, 1, 0, 0x72);
	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	copy_during_fill = true;
	UT_ASSERT_EQ(cluster_undo_block0_admit_runtime(&fill_logical, &fill_root, &fill_proof, &token,
												   &pin, &page),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(copy_during_fill_result, CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED);
	cluster_undo_block0_unpin(&pin);
}

UT_TEST(test_block0_runtime_fill_abort_releases_slot_and_frame_via_resource_owner)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x3380), 1);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 2);
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0FrameToken retry;
	ClusterUndoBlock0FrameToken extra;
	ClusterUndoBlock0Pin pin;
	char *page = NULL;
	bool caught = false;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 0, 0x73);
	memset(&token, 0, sizeof(token));
	memset(&retry, 0, sizeof(retry));
	memset(&extra, 0, sizeof(extra));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	abort_during_fill = true;
	if (sigsetjmp(wal_error_jump, 1) == 0)
		(void)cluster_undo_block0_admit_runtime(&logical, &root, &proof, &token, &pin, &page);
	else
		caught = true;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &retry), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(retry.owned);
	cluster_undo_block0_frame_release(&token);
	UT_ASSERT(!token.owned);
	UT_ASSERT_EQ(token.frame_index, UINT32_MAX);
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &extra),
				 CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE);
	cluster_undo_block0_frame_release(&retry);
}

UT_TEST(test_block0_resource_owner_cleanup_is_balanced_with_normal_release)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x3390), 1);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 2);
	ClusterUndoBlock0ResidentCensusItem item;
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0FrameToken retry;
	ClusterUndoBlock0FrameToken extra;
	ClusterUndoBlock0Pin pin;
	char *page = NULL;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 0, 0x74);
	memset(&token, 0, sizeof(token));
	memset(&retry, 0, sizeof(retry));
	memset(&extra, 0, sizeof(extra));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(cluster_undo_block0_admit_runtime(&logical, &root, &proof, &token, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_OK);
	memset(&item, 0, sizeof(item));
	item.logical = logical;
	item.resolved_root = root;
	item.generation = pin.observed_generation;
	item.proof = proof;
	resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, resource_release_arg);
	UT_ASSERT(cluster_undo_block0_verify_clean_census(&item, 1));

	/* The admitted frame stays bound.  No stale tracker may return it. */
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &retry),
				 CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE);

	/* Normal release removes its tracker, so a later owner callback cannot
	 * duplicate the free-stack entry. */
	fresh_block0_region(1);
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &retry), CLUSTER_UNDO_BLOCK0_OK);
	cluster_undo_block0_frame_release(&retry);
	resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, resource_release_arg);
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &retry), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &extra),
				 CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE);
	cluster_undo_block0_frame_release(&retry);
}

UT_TEST(test_block0_provision_existing_exact_loads_without_temp_or_write)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x3400), 2);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 3);
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0Pin pin;
	char *page = NULL;
	bool creator = true;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 4, 0x75);
	memcpy(smgr_probe_image, smgr_image, BLCKSZ);
	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(
		cluster_undo_block0_provision_begin(&logical, &root, &proof, &token, &pin, &page, &creator),
		CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(!creator);
	UT_ASSERT_EQ(smgr_probe_calls, 1);
	UT_ASSERT_EQ(smgr_temp_create_calls, 0);
	UT_ASSERT_EQ(smgr_write_calls, 0);
	UT_ASSERT_EQ((unsigned char)page[BLCKSZ - 1], (unsigned char)smgr_image[BLCKSZ - 1]);
	UT_ASSERT(pin.observed_generation.known);
	UT_ASSERT_EQ(pin.observed_generation.value, 4);
	cluster_undo_block0_unpin(&pin);
}

UT_TEST(test_block0_provision_rejects_non_live_authority_before_io)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x3408), 2);
	ClusterUndoBlock0AuthorityProof proof = make_recovery_proof(1, 3, 1);
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0Pin pin;
	ClusterUndoBlock0Result result;
	char *page = NULL;
	bool creator = false;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 4, 0x75);
	memcpy(smgr_probe_image, smgr_image, BLCKSZ);
	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	result = cluster_undo_block0_provision_begin(&logical, &root, &proof, &token, &pin, &page,
												 &creator);
	UT_ASSERT_EQ(result, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(smgr_probe_calls, 0);
	UT_ASSERT_EQ(smgr_temp_create_calls, 0);
	UT_ASSERT_NULL(page);
	UT_ASSERT(!creator);
	if (result == CLUSTER_UNDO_BLOCK0_OK)
		cluster_undo_block0_unpin(&pin);
	else
		cluster_undo_block0_frame_release(&token);
}

UT_TEST(test_block0_provision_rejects_unknown_probe_state_without_consuming_frame)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x340c), 2);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 3);
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0Pin pin;
	char *page = NULL;
	bool creator = false;

	fresh_block0_region(1);
	smgr_probe_state = (ClusterUndoSmgrFinalState)99;
	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(
		cluster_undo_block0_provision_begin(&logical, &root, &proof, &token, &pin, &page, &creator),
		CLUSTER_UNDO_BLOCK0_IO_ERROR);
	UT_ASSERT_EQ(smgr_probe_calls, 1);
	UT_ASSERT_EQ(smgr_temp_create_calls, 0);
	UT_ASSERT(token.owned);
	UT_ASSERT_NULL(page);
	UT_ASSERT(!creator);
	cluster_undo_block0_frame_release(&token);
}

UT_TEST(test_block0_provision_absent_creator_publishes_after_wal_flush)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x3410), 2);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 3);
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0Pin pin;
	char *page = NULL;
	bool creator = false;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 5, 0x76);
	smgr_probe_state = CLUSTER_UNDO_SMGR_FINAL_ABSENT;
	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(
		cluster_undo_block0_provision_begin(&logical, &root, &proof, &token, &pin, &page, &creator),
		CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(creator);
	UT_ASSERT(smgr_temp_active);
	UT_ASSERT_EQ(smgr_temp_create_calls, 1);
	memcpy(page, smgr_image, BLCKSZ);
	cluster_undo_block0_provision_publish(&pin, (XLogRecPtr)UINT64CONST(0x120));
	UT_ASSERT_EQ(xlog_flush_calls, 1);
	UT_ASSERT_EQ(xlog_last_flush_lsn, (XLogRecPtr)UINT64CONST(0x120));
	UT_ASSERT_EQ(smgr_temp_publish_calls, 1);
	UT_ASSERT(!smgr_temp_active);
	UT_ASSERT(pin.observed_generation.known);
	UT_ASSERT_EQ(pin.observed_generation.value, 5);
	cluster_undo_block0_unpin(&pin);
}

UT_TEST(test_block0_provision_eexist_loser_installs_exact_winner_image)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x3420), 2);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 3);
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0Pin pin;
	char winner[BLCKSZ];
	char *page = NULL;
	bool creator = false;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 6, 0x77);
	memcpy(winner, smgr_image, BLCKSZ);
	winner[BLCKSZ - 1] = (char)0xc7;
	memcpy(smgr_probe_image, winner, BLCKSZ);
	smgr_probe_state = CLUSTER_UNDO_SMGR_FINAL_ABSENT;
	smgr_publish_result = CLUSTER_UNDO_SMGR_PUBLISH_EXISTS;
	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(
		cluster_undo_block0_provision_begin(&logical, &root, &proof, &token, &pin, &page, &creator),
		CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(creator);
	memcpy(page, smgr_image, BLCKSZ);
	page[BLCKSZ - 1] = (char)0xd7;
	cluster_undo_block0_provision_publish(&pin, (XLogRecPtr)UINT64CONST(0x130));
	UT_ASSERT_EQ((unsigned char)page[BLCKSZ - 1], 0xc7);
	UT_ASSERT_EQ(smgr_probe_calls, 2);
	cluster_undo_block0_unpin(&pin);
}

UT_TEST(test_block0_provision_abort_cleans_temp_and_returns_frame)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x3430), 2);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 3);
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0FrameToken retry;
	ClusterUndoBlock0Pin pin;
	char *page = NULL;
	bool creator = false;

	fresh_block0_region(1);
	smgr_probe_state = CLUSTER_UNDO_SMGR_FINAL_ABSENT;
	memset(&token, 0, sizeof(token));
	memset(&retry, 0, sizeof(retry));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(
		cluster_undo_block0_provision_begin(&logical, &root, &proof, &token, &pin, &page, &creator),
		CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(creator);
	cluster_undo_block0_provision_abort(&pin);
	UT_ASSERT_EQ(pin.slot, -1);
	UT_ASSERT_EQ(smgr_temp_cleanup_calls, 1);
	UT_ASSERT(!smgr_temp_active);
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &retry), CLUSTER_UNDO_BLOCK0_OK);
	cluster_undo_block0_frame_release(&retry);
}

UT_TEST(test_block0_provision_publish_error_resource_owner_cleans_exact_temp)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x3440), 2);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 3);
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0FrameToken retry;
	ClusterUndoBlock0Pin pin;
	char *page = NULL;
	bool creator = false;
	bool caught = false;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 7, 0x78);
	smgr_probe_state = CLUSTER_UNDO_SMGR_FINAL_ABSENT;
	smgr_publish_result = CLUSTER_UNDO_SMGR_PUBLISH_IO_ERROR;
	memset(&token, 0, sizeof(token));
	memset(&retry, 0, sizeof(retry));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(
		cluster_undo_block0_provision_begin(&logical, &root, &proof, &token, &pin, &page, &creator),
		CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(creator);
	memcpy(page, smgr_image, BLCKSZ);

	wal_error_armed = true;
	if (sigsetjmp(wal_error_jump, 1) == 0)
		cluster_undo_block0_provision_publish(&pin, (XLogRecPtr)UINT64CONST(0x140));
	else
		caught = true;
	wal_error_armed = false;
	UT_ASSERT(caught);
	UT_ASSERT(smgr_temp_active);
	UT_ASSERT_NOT_NULL(resource_release_callback);
	resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, resource_release_arg);
	UT_ASSERT_EQ(smgr_temp_cleanup_calls, 1);
	UT_ASSERT(!smgr_temp_active);
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &retry), CLUSTER_UNDO_BLOCK0_OK);
	cluster_undo_block0_frame_release(&retry);
}

UT_TEST(test_block0_recovery_private_begin_reads_without_allocating_a_frame)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, UINT64CONST(0x4400), 6);
	ClusterUndoBlock0AuthorityProof proof = make_recovery_proof(1, 11, 4);
	ClusterUndoBlock0FrameToken only_frame;
	ClusterUndoBlock0RecoveryGuard guard;
	char private_page[BLCKSZ];
	bool exists = false;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 3, 0x84);
	memset(&only_frame, 0, sizeof(only_frame));
	memset(&guard, 0xa5, sizeof(guard));
	memset(private_page, 0, sizeof(private_page));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &only_frame), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(cluster_undo_block0_recovery_private_begin(&logical, &root, &proof, false, &guard,
															private_page, &exists),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(exists);
	UT_ASSERT(guard.content_x_held);
	UT_ASSERT_EQ(guard.slot, 0);
	UT_ASSERT_EQ((int)(unsigned char)private_page[BLCKSZ - 1], 0x84);
	UT_ASSERT_EQ(smgr_read_calls, 1);
	UT_ASSERT_EQ(smgr_last_block, 0);
	UT_ASSERT(only_frame.owned);

	cluster_undo_block0_recovery_private_abort(&guard);
	UT_ASSERT(!guard.content_x_held);
	UT_ASSERT_EQ(guard.slot, -1);
	cluster_undo_block0_frame_release(&only_frame);
}

UT_TEST(test_block0_recovery_private_begin_rejects_live_authority_before_io)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, UINT64CONST(0x4500), 1);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 13);
	ClusterUndoBlock0RecoveryGuard guard;
	char private_page[BLCKSZ];
	bool exists = true;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 0, 0x45);
	memset(&guard, 0xa5, sizeof(guard));
	memset(private_page, 0x5a, sizeof(private_page));
	UT_ASSERT_EQ(cluster_undo_block0_recovery_private_begin(&logical, &root, &proof, false, &guard,
															private_page, &exists),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT(!exists);
	UT_ASSERT(!guard.content_x_held);
	UT_ASSERT_EQ(guard.slot, -1);
	UT_ASSERT_EQ(smgr_read_calls, 0);
	UT_ASSERT_EQ((int)(unsigned char)private_page[0], 0x5a);
}

UT_TEST(test_block0_recovery_private_bad_identity_does_not_publish_disk_bytes)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, UINT64CONST(0x4580), 2);
	ClusterUndoBlock0AuthorityProof proof = make_recovery_proof(1, 13, 5);
	ClusterUndoBlock0RecoveryGuard guard;
	char private_page[BLCKSZ];
	bool exists = true;

	fresh_block0_region(1);
	make_valid_block0(2, 1, 0, 0x58);
	memset(&guard, 0xa5, sizeof(guard));
	memset(private_page, 0x6c, sizeof(private_page));
	UT_ASSERT_EQ(cluster_undo_block0_recovery_private_begin(&logical, &root, &proof, false, &guard,
															private_page, &exists),
				 CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
	UT_ASSERT(!exists);
	UT_ASSERT(!guard.content_x_held);
	UT_ASSERT_EQ(guard.slot, -1);
	UT_ASSERT_EQ(smgr_read_calls, 1);
	UT_ASSERT_EQ((int)(unsigned char)private_page[0], 0x6c);
	UT_ASSERT_EQ((int)(unsigned char)private_page[BLCKSZ - 1], 0x6c);
}

UT_TEST(test_block0_recovery_private_absence_stays_fail_closed_without_typed_smgr_result)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, UINT64CONST(0x4600), 2);
	ClusterUndoBlock0AuthorityProof proof = make_recovery_proof(1, 14, 5);
	ClusterUndoBlock0RecoveryGuard guard;
	char private_page[BLCKSZ];
	bool exists = true;

	fresh_block0_region(1);
	smgr_read_ok = false;
	memset(&guard, 0xa5, sizeof(guard));
	memset(private_page, 0x6b, sizeof(private_page));
	UT_ASSERT_EQ(cluster_undo_block0_recovery_private_begin(&logical, &root, &proof, true, &guard,
															private_page, &exists),
				 CLUSTER_UNDO_BLOCK0_IO_ERROR);
	UT_ASSERT(!exists);
	UT_ASSERT(!guard.content_x_held);
	UT_ASSERT_EQ(guard.slot, -1);
	UT_ASSERT_EQ(smgr_read_calls, 1);
	UT_ASSERT_EQ((int)(unsigned char)private_page[0], 0x6b);
}

UT_TEST(test_block0_recovery_private_finish_writes_fsyncs_and_releases_without_residency)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, UINT64CONST(0x4680), 3);
	ClusterUndoBlock0AuthorityProof proof = make_recovery_proof(1, 15, 6);
	ClusterUndoBlock0RecoveryGuard guard;
	ClusterUndoBlock0FrameToken frame;
	char private_page[BLCKSZ];
	bool exists = false;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 6, 0x68);
	UT_ASSERT_EQ(cluster_undo_block0_recovery_private_begin(&logical, &root, &proof, false, &guard,
															private_page, &exists),
				 CLUSTER_UNDO_BLOCK0_OK);
	private_page[BLCKSZ - 1] = (char)0xa8;
	cluster_undo_block0_recovery_private_finish(&guard, private_page, (XLogRecPtr)UINT64CONST(0xb0),
												true, false);

	UT_ASSERT_EQ(smgr_write_calls, 1);
	UT_ASSERT_EQ(smgr_last_write_block, 0);
	UT_ASSERT(smgr_last_do_fsync);
	UT_ASSERT_EQ((int)(unsigned char)smgr_written_image[BLCKSZ - 1], 0xa8);
	UT_ASSERT_EQ(xlog_flush_calls, 0);
	UT_ASSERT(!guard.content_x_held);
	UT_ASSERT_EQ(guard.slot, -1);
	memset(&frame, 0, sizeof(frame));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &frame), CLUSTER_UNDO_BLOCK0_OK);
	cluster_undo_block0_frame_release(&frame);
}

UT_TEST(test_block0_recovery_private_finish_validated_noop_has_zero_io)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, UINT64CONST(0x4690), 4);
	ClusterUndoBlock0AuthorityProof proof = make_recovery_proof(1, 16, 7);
	ClusterUndoBlock0RecoveryGuard guard;
	char private_page[BLCKSZ];
	bool exists = false;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 7, 0x69);
	UT_ASSERT_EQ(cluster_undo_block0_recovery_private_begin(&logical, &root, &proof, false, &guard,
															private_page, &exists),
				 CLUSTER_UNDO_BLOCK0_OK);
	cluster_undo_block0_recovery_private_finish(&guard, NULL, (XLogRecPtr)UINT64CONST(0xc0), false,
												true);
	UT_ASSERT_EQ(smgr_write_calls, 0);
	UT_ASSERT_EQ(xlog_flush_calls, 0);
	UT_ASSERT(!guard.content_x_held);
	UT_ASSERT_EQ(guard.slot, -1);
}

UT_TEST(test_block0_recovery_private_finish_write_failure_panics_with_guard_unpublished)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, UINT64CONST(0x46a0), 5);
	ClusterUndoBlock0AuthorityProof proof = make_recovery_proof(1, 17, 8);
	ClusterUndoBlock0RecoveryGuard guard;
	char private_page[BLCKSZ];
	bool exists = false;
	bool caught = false;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 8, 0x6a);
	UT_ASSERT_EQ(cluster_undo_block0_recovery_private_begin(&logical, &root, &proof, false, &guard,
															private_page, &exists),
				 CLUSTER_UNDO_BLOCK0_OK);
	private_page[BLCKSZ - 1] = (char)0xaa;
	smgr_write_ok = false;
	wal_error_armed = true;
	if (sigsetjmp(wal_error_jump, 1) == 0)
		cluster_undo_block0_recovery_private_finish(&guard, private_page,
													(XLogRecPtr)UINT64CONST(0xd0), true, false);
	else
		caught = true;
	wal_error_armed = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(smgr_write_calls, 1);
	UT_ASSERT(smgr_last_do_fsync);
	UT_ASSERT(guard.content_x_held);
	UT_ASSERT_EQ(guard.slot, 0);
	cluster_undo_block0_recovery_private_abort(&guard);
}

UT_TEST(test_block0_recovery_private_parent_fsync_gap_panics_before_write)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, UINT64CONST(0x46b0), 6);
	ClusterUndoBlock0AuthorityProof proof = make_recovery_proof(1, 18, 9);
	ClusterUndoBlock0RecoveryGuard guard;
	char private_page[BLCKSZ];
	bool exists = false;
	bool caught = false;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 9, 0x6b);
	UT_ASSERT_EQ(cluster_undo_block0_recovery_private_begin(&logical, &root, &proof, false, &guard,
															private_page, &exists),
				 CLUSTER_UNDO_BLOCK0_OK);
	wal_error_armed = true;
	if (sigsetjmp(wal_error_jump, 1) == 0)
		cluster_undo_block0_recovery_private_finish(&guard, private_page,
													(XLogRecPtr)UINT64CONST(0xe0), true, true);
	else
		caught = true;
	wal_error_armed = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(smgr_write_calls, 0);
	UT_ASSERT(guard.content_x_held);
	UT_ASSERT_EQ(guard.slot, 0);
	cluster_undo_block0_recovery_private_abort(&guard);
}

UT_TEST(test_block0_wal_dirty_uses_monotone_lsn_and_flushes_before_durable_data)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x4700), 3);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 15);
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0Pin pin;
	char successor[BLCKSZ];
	char *page = NULL;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 2, 0x47);
	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(cluster_undo_block0_admit_runtime(&logical, &root, &proof, &token, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_OK);
	memcpy(successor, page, sizeof(successor));
	successor[BLCKSZ - 1] = (char)0x94;

	cluster_undo_block0_mark_wal_dirty(&pin, (XLogRecPtr)UINT64CONST(0x80));
	cluster_undo_block0_mark_wal_dirty(&pin, (XLogRecPtr)UINT64CONST(0x70));
	cluster_undo_block0_flush_sync(&pin, successor, (XLogRecPtr)UINT64CONST(0x75), false);

	UT_ASSERT_EQ(xlog_flush_calls, 1);
	UT_ASSERT_EQ(xlog_last_flush_lsn, (XLogRecPtr)UINT64CONST(0x80));
	UT_ASSERT_EQ(smgr_write_calls, 1);
	UT_ASSERT_EQ(smgr_last_write_block, 0);
	UT_ASSERT(smgr_last_do_fsync);
	UT_ASSERT_EQ(wal_io_order_count, 2);
	UT_ASSERT_EQ(wal_io_order[0], 1);
	UT_ASSERT_EQ(wal_io_order[1], 2);
	UT_ASSERT_EQ((int)(unsigned char)smgr_written_image[BLCKSZ - 1], 0x94);
	UT_ASSERT_EQ((int)(unsigned char)page[BLCKSZ - 1], 0x94);
	cluster_undo_block0_unpin(&pin);
}

UT_TEST(test_block0_flush_publishes_successor_generation)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x4750), 3);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 15);
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0Pin pin;
	ClusterUndoBlock0Generation old_generation = make_generation(true, 2);
	ClusterUndoBlock0Generation new_generation = make_generation(true, 3);
	ClusterUndoBlock0Generation sampled = make_generation(false, 99);
	char successor[BLCKSZ];
	char copied[BLCKSZ];
	char *page = NULL;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 2, 0x47);
	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(cluster_undo_block0_admit_runtime(&logical, &root, &proof, &token, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_OK);
	memcpy(successor, page, sizeof(successor));
	((UndoSegmentHeaderData *)successor)->wrap_count = 3;
	cluster_undo_block0_flush_sync(&pin, successor, InvalidXLogRecPtr, false);
	UT_ASSERT(pin.observed_generation.known);
	UT_ASSERT_EQ(pin.observed_generation.value, 3);
	cluster_undo_block0_unpin(&pin);

	UT_ASSERT_EQ(cluster_undo_block0_sample_resident_generation(&logical, &root, &proof, &sampled),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(sampled.known);
	UT_ASSERT_EQ(sampled.value, 3);
	UT_ASSERT_EQ(
		cluster_undo_block0_copy_resident(&logical, &root, &old_generation, &proof, copied, NULL),
		CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH);
	UT_ASSERT_EQ(
		cluster_undo_block0_copy_resident(&logical, &root, &new_generation, &proof, copied, NULL),
		CLUSTER_UNDO_BLOCK0_OK);
}

UT_TEST(test_block0_flush_failure_preserves_resident_predecessor_and_dirty_lsn)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x4800), 4);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 16);
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0Pin pin;
	char successor[BLCKSZ];
	char *page = NULL;
	bool caught = false;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 4, 0x48);
	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(cluster_undo_block0_admit_runtime(&logical, &root, &proof, &token, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_OK);
	memcpy(successor, page, sizeof(successor));
	successor[BLCKSZ - 1] = (char)0x98;
	cluster_undo_block0_mark_wal_dirty(&pin, (XLogRecPtr)UINT64CONST(0x90));
	smgr_write_ok = false;

	wal_error_armed = true;
	if (sigsetjmp(wal_error_jump, 1) == 0)
		cluster_undo_block0_flush_sync(&pin, successor, (XLogRecPtr)UINT64CONST(0x85), false);
	else
		caught = true;
	wal_error_armed = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(xlog_flush_calls, 1);
	UT_ASSERT_EQ(xlog_last_flush_lsn, (XLogRecPtr)UINT64CONST(0x90));
	UT_ASSERT_EQ(smgr_write_calls, 1);
	UT_ASSERT_EQ((int)(unsigned char)page[BLCKSZ - 1], 0x48);
	UT_ASSERT_EQ(pin.slot, 0);

	smgr_write_ok = true;
	xlog_flush_calls = 0;
	xlog_last_flush_lsn = InvalidXLogRecPtr;
	smgr_write_calls = 0;
	cluster_undo_block0_flush_sync(&pin, successor, (XLogRecPtr)UINT64CONST(0x85), false);
	UT_ASSERT_EQ(xlog_flush_calls, 1);
	UT_ASSERT_EQ(xlog_last_flush_lsn, (XLogRecPtr)UINT64CONST(0x90));
	UT_ASSERT_EQ(smgr_write_calls, 1);
	UT_ASSERT_EQ((int)(unsigned char)page[BLCKSZ - 1], 0x98);
	cluster_undo_block0_unpin(&pin);
}

UT_TEST(test_block0_flush_parent_fsync_request_fails_before_wal_or_data_publication)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x4900), 5);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 17);
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0Pin pin;
	char successor[BLCKSZ];
	char *page = NULL;
	bool caught = false;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 5, 0x49);
	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(cluster_undo_block0_admit_runtime(&logical, &root, &proof, &token, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_OK);
	memcpy(successor, page, sizeof(successor));
	successor[BLCKSZ - 1] = (char)0x99;
	cluster_undo_block0_mark_wal_dirty(&pin, (XLogRecPtr)UINT64CONST(0xa0));

	wal_error_armed = true;
	if (sigsetjmp(wal_error_jump, 1) == 0)
		cluster_undo_block0_flush_sync(&pin, successor, (XLogRecPtr)UINT64CONST(0xa0), true);
	else
		caught = true;
	wal_error_armed = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(xlog_flush_calls, 0);
	UT_ASSERT_EQ(smgr_write_calls, 0);
	UT_ASSERT_EQ((int)(unsigned char)page[BLCKSZ - 1], 0x49);
	UT_ASSERT_EQ(pin.slot, 0);
	cluster_undo_block0_unpin(&pin);
}

UT_TEST(test_block0_flush_rejects_wrong_logical_successor_before_wal_or_data)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root
		= make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, UINT64CONST(0x4a00), 6);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 18);
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0Pin pin;
	char successor[BLCKSZ];
	char *page = NULL;
	bool caught = false;

	fresh_block0_region(1);
	make_valid_block0(1, 1, 6, 0x4a);
	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(cluster_undo_block0_admit_runtime(&logical, &root, &proof, &token, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_OK);
	memcpy(successor, page, sizeof(successor));
	((UndoSegmentHeaderData *)successor)->segment_id = 2;
	cluster_undo_block0_mark_wal_dirty(&pin, (XLogRecPtr)UINT64CONST(0xb0));

	wal_error_armed = true;
	if (sigsetjmp(wal_error_jump, 1) == 0)
		cluster_undo_block0_flush_sync(&pin, successor, (XLogRecPtr)UINT64CONST(0xb0), false);
	else
		caught = true;
	wal_error_armed = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(xlog_flush_calls, 0);
	UT_ASSERT_EQ(smgr_write_calls, 0);
	UT_ASSERT_EQ(((UndoSegmentHeaderData *)page)->segment_id, 1);
	UT_ASSERT_EQ(pin.slot, 0);
	cluster_undo_block0_unpin(&pin);
}

UT_TEST(test_block0_clean_census_requires_exact_complete_unpinned_residency)
{
	ClusterUndoBlock0LogicalKey logical[2] = {
		make_key(1, 1),
		make_key(1, 2),
	};
	ClusterUndoBlock0ResolvedRoot root[2] = {
		make_root(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, UINT64CONST(0x5101), 8),
		make_root(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, UINT64CONST(0x5102), 8),
	};
	ClusterUndoBlock0AuthorityProof proof = make_recovery_proof(1, 23, 9);
	ClusterUndoBlock0ResidentCensusItem items[3];
	ClusterUndoBlock0FrameToken tokens[2];
	ClusterUndoBlock0Pin pin;
	ClusterUndoBlock0Generation expected;
	char *page = NULL;
	int i;

	fresh_block0_region(2);
	memset(tokens, 0, sizeof(tokens));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(2, tokens), CLUSTER_UNDO_BLOCK0_OK);
	for (i = 0; i < 2; i++) {
		make_valid_block0(logical[i].segment_id, logical[i].owner_instance, 11 + i,
						  (uint8)(0x51 + i));
		UT_ASSERT_EQ(cluster_undo_block0_admit_runtime(&logical[i], &root[i], &proof, &tokens[i],
													   &pin, &page),
					 CLUSTER_UNDO_BLOCK0_OK);
		memset(&items[i], 0, sizeof(items[i]));
		items[i].logical = logical[i];
		items[i].resolved_root = root[i];
		items[i].generation = pin.observed_generation;
		items[i].proof = proof;
		cluster_undo_block0_unpin(&pin);
	}

	UT_ASSERT(cluster_undo_block0_verify_clean_census(items, 2));
	UT_ASSERT(!cluster_undo_block0_verify_clean_census(items, 1));
	UT_ASSERT(!cluster_undo_block0_verify_clean_census(NULL, 2));

	items[2] = items[1];
	items[2].logical = make_key(1, 3);
	items[2].generation.value = 13;
	UT_ASSERT(!cluster_undo_block0_verify_clean_census(items, 3));

	items[0].resolved_root.root_generation++;
	UT_ASSERT(!cluster_undo_block0_verify_clean_census(items, 2));
	items[0].resolved_root = root[0];
	items[0].proof.recovery_generation++;
	UT_ASSERT(!cluster_undo_block0_verify_clean_census(items, 2));
	items[0].proof = proof;

	UT_ASSERT_EQ(cluster_undo_block0_reserve(&logical[0], &root[0], &proof, &pin),
				 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(!cluster_undo_block0_verify_clean_census(items, 2));
	expected = items[0].generation;
	UT_ASSERT_EQ(
		cluster_undo_block0_lock_content(&pin, &expected, CLUSTER_UNDO_BLOCK0_EXCLUSIVE, &page),
		CLUSTER_UNDO_BLOCK0_OK);
	cluster_undo_block0_mark_wal_dirty(&pin, (XLogRecPtr)UINT64CONST(0xb0));
	cluster_undo_block0_unpin(&pin);
	UT_ASSERT(!cluster_undo_block0_verify_clean_census(items, 2));

	fresh_block0_region(1);
	UT_ASSERT(cluster_undo_block0_verify_clean_census(NULL, 0));
}

UT_TEST(test_normal_start_empty_requires_exclusive_owner_and_whole_pool)
{
	ClusterUndoBlock0FrameToken token;
	fresh_block0_region(2);
	MyAuxProcType = StartupProcess;
	r4_startup_fenced_owned = true;
	normal_start_closed = true;
	UT_ASSERT(cluster_undo_block0_normal_start_empty());
	MyAuxProcType = LmonProcess;
	UT_ASSERT(!cluster_undo_block0_normal_start_empty());
	MyAuxProcType = StartupProcess;
	r4_startup_fenced_owned = false;
	UT_ASSERT(!cluster_undo_block0_normal_start_empty());
	r4_startup_fenced_owned = true;
	normal_start_closed = false;
	UT_ASSERT(!cluster_undo_block0_normal_start_empty());
	normal_start_closed = true;
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(!cluster_undo_block0_normal_start_empty());
	cluster_undo_block0_frame_release(&token);
	UT_ASSERT(cluster_undo_block0_normal_start_empty());
	MyAuxProcType = NotAnAuxProcess;
	r4_startup_fenced_owned = normal_start_closed = false;
}

UT_TEST(test_normal_start_discard_is_exact_clean_unpinned_prefix_only)
{
	ClusterUndoBlock0ResidentCensusItem items[2];
	ClusterUndoBlock0FrameToken tokens[2];
	ClusterUndoBlock0Pin pin;
	uint32 frames[2];
	char *page;
	int i;
	fresh_block0_region(2);
	MyAuxProcType = StartupProcess;
	r4_startup_fenced_owned = normal_start_closed = true;
	memset(items, 0, sizeof(items));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(2, tokens), CLUSTER_UNDO_BLOCK0_OK);
	for (i = 0; i < 2; i++) {
		items[i].logical = make_key(1, i + 1);
		items[i].resolved_root = make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 100 + i, 1);
		items[i].proof = make_live_proof(1, 0);
		frames[i] = tokens[i].frame_index;
		make_valid_block0(i + 1, 1, i, 0);
		UT_ASSERT_EQ(cluster_undo_block0_admit_runtime(&items[i].logical, &items[i].resolved_root,
													   &items[i].proof, &tokens[i], &pin, &page),
					 CLUSTER_UNDO_BLOCK0_OK);
		items[i].generation = pin.observed_generation;
		UT_ASSERT(!cluster_undo_block0_normal_start_discard(&items[i], frames[i]));
		cluster_undo_block0_unpin(&pin);
	}
	UT_ASSERT(!cluster_undo_block0_normal_start_empty());
	for (i = 0; i < 7; i++) {
		ClusterUndoBlock0ResidentCensusItem wrong = items[0];
		switch (i) {
		case 0:
			wrong.logical = items[1].logical;
			break;
		case 1:
			wrong.resolved_root.root_id++;
			break;
		case 2:
			wrong.resolved_root.root_generation++;
			break;
		case 3:
			wrong.generation.value++;
			break;
		case 4:
			wrong.generation.known = false;
			break;
		case 5:
			wrong.proof.kind = CLUSTER_UNDO_BLOCK0_STARTUP_REDO;
			break;
		case 6:
			wrong.proof.cluster_epoch++;
			break;
		}
		UT_ASSERT(!cluster_undo_block0_normal_start_discard(&wrong, frames[0]));
		UT_ASSERT(cluster_undo_block0_verify_clean_census(items, 2));
	}
	UT_ASSERT(!cluster_undo_block0_normal_start_discard(&items[0], frames[1]));
	MyAuxProcType = LmonProcess;
	UT_ASSERT(!cluster_undo_block0_normal_start_discard(&items[0], frames[0]));
	MyAuxProcType = StartupProcess;
	normal_start_closed = false;
	UT_ASSERT(!cluster_undo_block0_normal_start_discard(&items[0], frames[0]));
	normal_start_closed = true;
	r4_startup_fenced_owned = false;
	UT_ASSERT(!cluster_undo_block0_normal_start_discard(&items[0], frames[0]));
	r4_startup_fenced_owned = true;
	UT_ASSERT(cluster_undo_block0_normal_start_discard(&items[0], frames[0]));
	UT_ASSERT(!cluster_undo_block0_normal_start_discard(&items[0], frames[0]));
	UT_ASSERT(cluster_undo_block0_verify_clean_census(&items[1], 1));
	UT_ASSERT(cluster_undo_block0_normal_start_discard(&items[1], frames[1]));
	UT_ASSERT(cluster_undo_block0_normal_start_empty());
	UT_ASSERT_EQ(smgr_write_calls, 0);
	UT_ASSERT_EQ(xlog_flush_calls, 0);
	UT_ASSERT(!cluster_undo_block0_backend_has_resources());
	MyAuxProcType = NotAnAuxProcess;
	r4_startup_fenced_owned = normal_start_closed = false;
}

UT_TEST(test_normal_start_discard_never_drops_dirty_or_foreign_binding)
{
	ClusterUndoBlock0ResidentCensusItem item;
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0Pin pin;
	uint32 frame;
	char *page;
	int i;
	for (i = 0; i < 2; i++) {
		fresh_block0_region(1);
		MyAuxProcType = StartupProcess;
		r4_startup_fenced_owned = normal_start_closed = true;
		memset(&item, 0, sizeof(item));
		item.logical = make_key(i + 1, i * 256 + 1);
		item.resolved_root = make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 100, 1);
		item.proof = make_live_proof(i + 1, 0);
		make_valid_block0(item.logical.segment_id, item.logical.owner_instance, 0, 0);
		UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
		frame = token.frame_index;
		UT_ASSERT_EQ(cluster_undo_block0_admit_runtime(&item.logical, &item.resolved_root,
													   &item.proof, &token, &pin, &page),
					 CLUSTER_UNDO_BLOCK0_OK);
		item.generation = pin.observed_generation;
		if (i == 0)
			cluster_undo_block0_mark_wal_dirty(&pin, 0x100);
		cluster_undo_block0_unpin(&pin);
		UT_ASSERT(!cluster_undo_block0_normal_start_discard(&item, frame));
		UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token),
					 CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE);
		UT_ASSERT_EQ(smgr_write_calls, 0);
	}
	MyAuxProcType = NotAnAuxProcess;
	r4_startup_fenced_owned = normal_start_closed = false;
}

UT_TEST(test_r4_prerequisite_snapshot_is_exact_and_repeatable)
{
	int i;

	UT_ASSERT_EQ((int)sizeof(ClusterR4PrerequisiteSnapshot), 64);
	UT_ASSERT_EQ((int)offsetof(ClusterR4PrerequisiteSnapshot, jcmk_generation), 16);
	UT_ASSERT_EQ((int)offsetof(ClusterR4PrerequisiteSnapshot, grammar_fingerprint), 56);
	UT_ASSERT_EQ((int)CLUSTER_R4_PREREQUISITE_RF_DEFERRED, 0);
	UT_ASSERT_EQ((int)CLUSTER_R4_PREREQUISITE_R4A_READY, 1);
	for (i = 0; i < 4096; i++) {
		ClusterR4PrerequisiteSnapshot snapshot;

		memset(&snapshot, 0xA5, sizeof(snapshot));
		snapshot = cluster_undo_block0_r4_prerequisite_snapshot();
		UT_ASSERT(r4_prerequisite_snapshot_is_fixed_false(&snapshot));
	}
}

UT_TEST(test_r4_prerequisite_snapshot_is_fixed_for_concurrent_callers)
{
	R4PrerequisiteThreadResult results[R4_PREREQUISITE_THREAD_COUNT];
	pthread_t threads[R4_PREREQUISITE_THREAD_COUNT];
	pg_atomic_uint32 ready;
	pg_atomic_uint32 start;
	uint32 calls = 0;
	uint32 mismatches = 0;
	int created = 0;
	int i;
	int rc;

	pg_atomic_init_u32(&ready, 0);
	pg_atomic_init_u32(&start, 0);
	memset(results, 0, sizeof(results));
	for (i = 0; i < R4_PREREQUISITE_THREAD_COUNT; i++) {
		results[i].ready = &ready;
		results[i].start = &start;
		rc = pthread_create(&threads[i], NULL, r4_prerequisite_snapshot_caller, &results[i]);
		UT_ASSERT_EQ(rc, 0);
		if (rc != 0)
			break;
		created++;
	}
	if (created != R4_PREREQUISITE_THREAD_COUNT) {
		pg_atomic_write_u32(&start, 1);
		for (i = 0; i < created; i++)
			(void)pthread_join(threads[i], NULL);
		return;
	}

	while (pg_atomic_read_u32(&ready) != R4_PREREQUISITE_THREAD_COUNT)
		;
	pg_atomic_write_u32(&start, 1);
	for (i = 0; i < R4_PREREQUISITE_THREAD_COUNT; i++) {
		rc = pthread_join(threads[i], NULL);
		UT_ASSERT_EQ(rc, 0);
		calls += results[i].calls;
		mismatches += results[i].mismatches;
	}
	UT_ASSERT_EQ(calls, R4_PREREQUISITE_THREAD_COUNT * R4_PREREQUISITE_CALLS_PER_THREAD);
	UT_ASSERT_EQ(mismatches, 0);
}

UT_TEST(test_r4_publish_ready_remains_fail_closed_for_every_unbound_input)
{
	ClusterR4PrerequisiteSnapshot forged;
	ClusterR4PrerequisiteSnapshot observed;

	memset(&forged, 0, sizeof(forged));
	UT_ASSERT(!cluster_undo_block0_r4_publish_ready(NULL));
	UT_ASSERT(!cluster_undo_block0_r4_publish_ready(&forged));

	forged.status = CLUSTER_R4_PREREQUISITE_R4A_READY;
	forged.ready = true;
	forged.target_node_id = 3;
	forged.episode_state_generation = 17;
	forged.jcmk_generation = UINT64CONST(29);
	forged.request_nonce = UINT64CONST(0x1112131415161718);
	forged.old_admitted_incarnation = UINT64CONST(41);
	forged.fresh_incarnation = UINT64CONST(42);
	forged.committed_epoch = UINT64CONST(9);
	forged.grammar_fingerprint = UINT64CONST(0x8e0dae5b428905e4);
	UT_ASSERT(!cluster_undo_block0_r4_publish_ready(&forged));
	observed = cluster_undo_block0_r4_prerequisite_snapshot();
	UT_ASSERT(r4_prerequisite_snapshot_is_fixed_false(&observed));
}

UT_TEST(test_r4_publish_ready_accepts_only_owner_cosampled_snapshot)
{
	ClusterR4PrerequisiteSnapshot expected;
	ClusterR4PrerequisiteSnapshot forged;
	ClusterR4PrerequisiteSnapshot observed;

	memset(&r4_owner_snapshot, 0, sizeof(r4_owner_snapshot));
	r4_owner_snapshot.status = CLUSTER_R4_PREREQUISITE_R4A_READY;
	r4_owner_snapshot.ready = true;
	r4_owner_snapshot.target_node_id = 3;
	r4_owner_snapshot.episode_state_generation = UINT32_C(18);
	r4_owner_snapshot.jcmk_generation = UINT64_C(29);
	r4_owner_snapshot.request_nonce = UINT64_C(0x1112131415161718);
	r4_owner_snapshot.old_admitted_incarnation = UINT64_C(41);
	r4_owner_snapshot.fresh_incarnation = UINT64_C(42);
	r4_owner_snapshot.committed_epoch = UINT64_C(9);
	r4_owner_snapshot.grammar_fingerprint = UINT64_C(0x8e0dae5b428905e4);
	r4_owner_publish_enabled = true;

	expected = r4_owner_snapshot;
	observed = cluster_undo_block0_r4_prerequisite_snapshot();
	UT_ASSERT_EQ(memcmp(&observed, &expected, sizeof(expected)), 0);
	UT_ASSERT(!cluster_undo_block0_r4_publish_ready(&expected));
	r4_startup_fenced_owned = true;
	UT_ASSERT(cluster_undo_block0_r4_publish_ready(&expected));
	forged = expected;
	forged.episode_state_generation++;
	UT_ASSERT(!cluster_undo_block0_r4_publish_ready(&forged));

	r4_owner_publish_enabled = false;
	r4_startup_fenced_owned = false;
	memset(&r4_owner_snapshot, 0, sizeof(r4_owner_snapshot));
}

UT_TEST(test_r4_startup_completion_surface_refuses_without_owner_proofs)
{
	ClusterR4StartupCompletionContextV1 *context
		= (ClusterR4StartupCompletionContextV1 *)(uintptr_t)1;

	UT_ASSERT_EQ(CLUSTER_R4_STARTUP_COMPLETION_OK, 0);
	UT_ASSERT_EQ(CLUSTER_R4_STARTUP_COMPLETION_RETRY, 1);
	UT_ASSERT_EQ(CLUSTER_R4_STARTUP_COMPLETION_BLOCKED_LINEAGE, 2);
	UT_ASSERT_EQ(CLUSTER_R4_STARTUP_COMPLETION_BLOCKED_ROOT, 3);
	UT_ASSERT_EQ(CLUSTER_R4_STARTUP_COMPLETION_BLOCKED_RECORD, 4);
	UT_ASSERT_EQ(CLUSTER_R4_STARTUP_COMPLETION_BLOCKED_PAGE, 5);
	UT_ASSERT_EQ(CLUSTER_R4_STARTUP_COMPLETION_BLOCKED_SIDE, 6);
	UT_ASSERT_EQ(CLUSTER_R4_STARTUP_COMPLETION_BLOCKED_CENSUS, 7);
	UT_ASSERT_EQ(CLUSTER_R4_STARTUP_COMPLETION_BLOCKED_IO, 8);
	UT_ASSERT_EQ(CLUSTER_R4_STARTUP_COMPLETION_BLOCKED_DIRTY, 9);
	UT_ASSERT_EQ(CLUSTER_R4_STARTUP_COMPLETION_BLOCKED_DEPENDENCY, 10);
	UT_ASSERT_EQ(CLUSTER_R4_STARTUP_COMPLETION_INVALID, 11);
	UT_ASSERT_EQ(cluster_undo_block0_r4_startup_begin(1000, &context),
				 CLUSTER_R4_STARTUP_COMPLETION_BLOCKED_DEPENDENCY);
	UT_ASSERT_NULL(context);
	cluster_undo_block0_r4_startup_abort(&context);
	UT_ASSERT_NULL(context);
}

/* A148: actual resident admission/reservation/flush/release, with disk bytes
 * and native lock scheduling as explicit fixture boundaries.  This observer
 * does not claim the separate Startup namespace census has run. */
static ClusterUndoBlock0ResolvedRoot stop_root;
static ClusterUndoRootDescriptorV1 stop_descriptor;
static uint8 stop_descriptor_bytes[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
static uint32 stop_segment;
static int stop_tt_slot;
static const char *stop_reason;

uint64
GetSystemIdentifier(void)
{
	return UINT64_C(42);
}

static void
stop_block0_encode_root(void)
{
	if (!cluster_undo_root_namespace_id(stop_descriptor.descriptor_incarnation,
										stop_descriptor.root_ordinal, &stop_descriptor.namespace_id)
		|| !cluster_undo_root_descriptor_encode(&stop_descriptor, stop_descriptor_bytes))
		abort();
}

static void
stop_block0_setup(uint32 frames)
{
	fresh_block0_region(frames);
	IsUnderPostmaster = true;
	MyBackendType = B_CHECKPOINTER;
	memset(&stop_descriptor, 0, sizeof(stop_descriptor));
	stop_descriptor.descriptor_incarnation = 3;
	stop_descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	stop_descriptor.owner_node = -1;
	stop_descriptor.system_identifier = 42;
	stop_descriptor.root_uuid[0] = 1;
	stop_block0_encode_root();
}

static ClusterNormalStopPollResult
stop_block0_poll(bool post_checkpoint)
{
	int reads = smgr_read_calls;
	int writes = smgr_write_calls;
	int flushes = xlog_flush_calls;
	uint32 holdoff = InterruptHoldoffCount;
	ClusterNormalStopPollResult result = cluster_undo_block0_normal_stop_poll(
		post_checkpoint, stop_descriptor_bytes, 7, &stop_segment, &stop_tt_slot, &stop_reason);

	UT_ASSERT_EQ(smgr_read_calls, reads);
	UT_ASSERT_EQ(smgr_write_calls, writes);
	UT_ASSERT_EQ(xlog_flush_calls, flushes);
	UT_ASSERT_EQ(InterruptHoldoffCount, holdoff);
	return result;
}

static void
stop_block0_load(uint32 segment, uint8 owner, ClusterUndoBlock0Pin *pin, char **page)
{
	ClusterUndoBlock0LogicalKey logical = make_key(owner, segment);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(owner, 7);
	ClusterUndoBlock0FrameToken token;

	UT_ASSERT(cluster_undo_root_descriptor_resolve(
		&stop_descriptor, CLUSTER_UNDO_PATH_RUNTIME_SHARED, owner, segment, &stop_root));
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(cluster_undo_block0_admit_runtime(&logical, &stop_root, &proof, &token, pin, page),
				 CLUSTER_UNDO_BLOCK0_OK);
}

UT_TEST(test_stop_block0_requires_owner_root_and_initialized_state)
{
	ClusterUndoBlock0FrameToken token;
	stop_block0_setup(2);
	UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(stop_segment, 0);
	UT_ASSERT_EQ(stop_tt_slot, -1);
	UT_ASSERT_STR_EQ(stop_reason, "NONE");
	IsUnderPostmaster = false;
	UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_INVALID);
	IsUnderPostmaster = true;
	MyBackendType = B_LMON;
	UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(stop_block0_poll(true), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(stop_block0_poll(true), CLUSTER_NORMAL_STOP_PENDING);
	cluster_undo_block0_frame_release(&token);
	UT_ASSERT_EQ(stop_block0_poll(true), CLUSTER_NORMAL_STOP_READY);
	MyBackendType = B_BACKEND;
	UT_ASSERT_EQ(stop_block0_poll(true), CLUSTER_NORMAL_STOP_INVALID);
	MyBackendType = B_CHECKPOINTER;
	memset(stop_descriptor_bytes, 0, sizeof(stop_descriptor_bytes));
	UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_INVALID);
	stop_block0_encode_root();
	stop_descriptor.system_identifier++;
	stop_block0_encode_root();
	UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_INVALID);
	stop_descriptor.system_identifier--;
	stop_descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_LOCAL;
	stop_descriptor.owner_node = 0;
	stop_descriptor.root_ordinal = 1;
	stop_block0_encode_root();
	UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_INVALID);
	stop_descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	stop_descriptor.owner_node = -1;
	stop_descriptor.root_ordinal = 0;
	stop_block0_encode_root();
	cluster_undo_block0_shmem_detach();
	UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(stop_reason, "UNDO_BLOCK0_STOP_STATE_INVALID");
}

UT_TEST(test_stop_block0_reserved_frames_need_original_release)
{
	ClusterUndoBlock0FrameToken tokens[2];

	stop_block0_setup(3);
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(2, tokens), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(tokens[0].owned && tokens[1].owned);
	cluster_undo_block0_frame_release(&tokens[0]);
	UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_PENDING);
	cluster_undo_block0_frame_release(&tokens[1]);
	UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(stop_block0_poll(true), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_stop_block0_pin_is_debt_but_unpinned_residency_is_not)
{
	ClusterUndoBlock0Pin pin;
	char *page;
	char original[BLCKSZ];

	stop_block0_setup(2);
	make_valid_block0(257, 2, 0, 0);
	stop_block0_load(257, 2, &pin, &page);
	memcpy(original, page, BLCKSZ);
	UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(pin.slot >= 0);
	cluster_undo_block0_unpin(&pin);
	UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(stop_block0_poll(true), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT(memcmp(original, page, BLCKSZ) == 0);
}

UT_TEST(test_stop_block0_checks_all_tt_slots_in_last_logical_segment)
{
	ClusterUndoBlock0LogicalKey logical = make_key(128, 32768);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(128, 7);
	ClusterUndoBlock0Pin pin;
	UndoSegmentHeaderData *header;
	char *page;
	int i;

	stop_block0_setup(2);
	make_valid_block0(32768, 128, 0, 0);
	stop_block0_load(32768, 128, &pin, &page);
	header = (UndoSegmentHeaderData *)page;
	cluster_undo_block0_unpin(&pin);
	for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		UT_ASSERT_EQ(cluster_undo_block0_pin(&logical, &stop_root, NULL,
											 CLUSTER_UNDO_BLOCK0_EXCLUSIVE, &proof, &pin, &page),
					 CLUSTER_UNDO_BLOCK0_OK);
		/* Canonical TT publication is the boundary input; resident flush and
		 * unpin are real.  No allocator mirror substitutes for these bytes. */
		header->tt_slots[i].xid = (TransactionId)(500 + i);
		header->tt_slots[i].status = TT_SLOT_ACTIVE;
		cluster_undo_block0_flush_sync(&pin, page, (XLogRecPtr)(200 + i), false);
		cluster_undo_block0_unpin(&pin);
		UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_EQ(stop_segment, 32768);
		UT_ASSERT_EQ(stop_tt_slot, i);
		UT_ASSERT_STR_EQ(stop_reason, "UNDO_BLOCK0_TT_ACTIVE");
		UT_ASSERT_EQ(header->tt_slots[i].status, TT_SLOT_ACTIVE);
		UT_ASSERT_EQ(cluster_undo_block0_pin(&logical, &stop_root, NULL,
											 CLUSTER_UNDO_BLOCK0_EXCLUSIVE, &proof, &pin, &page),
					 CLUSTER_UNDO_BLOCK0_OK);
		header->tt_slots[i].status = TT_SLOT_COMMITTED;
		header->tt_slots[i].commit_scn = scn_encode(127, 1000 + i);
		cluster_undo_block0_flush_sync(&pin, page, (XLogRecPtr)(300 + i), false);
		cluster_undo_block0_unpin(&pin);
		UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_READY);
	}
	UT_ASSERT_EQ(stop_block0_poll(true), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_stop_block0_durability_and_undo_anchor_are_not_cleared_by_poll)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 7);
	ClusterUndoBlock0Pin pin;
	char *page;
	UndoSegmentHeaderData *header;

	stop_block0_setup(2);
	make_valid_block0(1, 1, 0, 0);
	stop_block0_load(1, 1, &pin, &page);
	header = (UndoSegmentHeaderData *)page;
	cluster_undo_block0_mark_wal_dirty(&pin, 400);
	cluster_undo_block0_unpin(&pin);
	UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(stop_block0_poll(true), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_STR_EQ(stop_reason, "UNDO_BLOCK0_DURABILITY_PENDING");
	UT_ASSERT_EQ(cluster_undo_block0_pin(&logical, &stop_root, NULL, CLUSTER_UNDO_BLOCK0_EXCLUSIVE,
										 &proof, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_OK);
	cluster_undo_block0_flush_sync(&pin, page, 400, false);
	cluster_undo_block0_unpin(&pin);
	UT_ASSERT_EQ(stop_block0_poll(true), CLUSTER_NORMAL_STOP_READY);
	/* Retained ABORT undo is outstanding work, not an empty terminal slot. */
	UT_ASSERT_EQ(cluster_undo_block0_pin(&logical, &stop_root, NULL, CLUSTER_UNDO_BLOCK0_EXCLUSIVE,
										 &proof, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_OK);
	header->tt_slots[47].xid = 1000;
	header->tt_slots[47].status = TT_SLOT_ABORTED;
	header->tt_slots[47].first_undo_block.raw[0] = UINT64CONST(0x100000001);
	cluster_undo_block0_flush_sync(&pin, page, 500, false);
	cluster_undo_block0_unpin(&pin);
	UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(stop_tt_slot, 47);
	UT_ASSERT_STR_EQ(stop_reason, "UNDO_BLOCK0_TT_UNDO_PENDING");
	UT_ASSERT(!UBA_is_invalid(header->tt_slots[47].first_undo_block));
}

UT_TEST(test_stop_block0_invalid_late_slot_overrides_active_and_root_drift)
{
	ClusterUndoBlock0Pin pin;
	char *page;
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)smgr_image;

	stop_block0_setup(2);
	make_valid_block0(1, 1, 0, 0);
	header->tt_slots[0].xid = 600;
	header->tt_slots[0].status = TT_SLOT_ACTIVE;
	header->tt_slots[47].xid = 700;
	header->tt_slots[47].status = TT_SLOT_COMMITTED; /* invalid: no SCN */
	stop_block0_load(1, 1, &pin, &page);
	cluster_undo_block0_unpin(&pin);
	UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(stop_tt_slot, 47);
	UT_ASSERT_STR_EQ(stop_reason, "UNDO_BLOCK0_TT_SHAPE_INVALID");
	stop_descriptor.descriptor_incarnation++;
	stop_block0_encode_root();
	UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(stop_reason, "UNDO_BLOCK0_RESIDENT_IDENTITY_INVALID");
	stop_descriptor.descriptor_incarnation--;
	stop_block0_encode_root();
	UT_ASSERT_EQ(cluster_undo_block0_normal_stop_poll(false, stop_descriptor_bytes, 8,
													  &stop_segment, &stop_tt_slot, &stop_reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(stop_reason, "UNDO_BLOCK0_RESIDENT_IDENTITY_INVALID");
}

UT_TEST(test_stop_block0_two_actual_segment_roots_share_one_pgrd_not_one_file_id)
{
	ClusterUndoRootDescriptorV1 descriptor = { 0 };
	ClusterUndoBlock0ResolvedRoot first, second;
	ClusterUndoBlock0Pin pin;
	char *page;
	stop_block0_setup(3);
	descriptor.descriptor_incarnation = 3;
	descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	descriptor.owner_node = -1;
	descriptor.system_identifier = 42;
	descriptor.root_uuid[0] = 1;
	UT_ASSERT(cluster_undo_root_namespace_id(3, 0, &descriptor.namespace_id));
	UT_ASSERT(cluster_undo_root_descriptor_resolve(&descriptor, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1,
												   1, &first));
	UT_ASSERT(cluster_undo_root_descriptor_resolve(&descriptor, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 2,
												   257, &second));
	UT_ASSERT(first.root_id != second.root_id);
	UT_ASSERT_EQ(first.root_generation, second.root_generation);
	stop_root = first;
	make_valid_block0(1, 1, 0, 0);
	stop_block0_load(1, 1, &pin, &page);
	cluster_undo_block0_unpin(&pin);
	stop_root = second;
	make_valid_block0(257, 2, 0, 0);
	stop_block0_load(257, 2, &pin, &page);
	cluster_undo_block0_unpin(&pin);
	stop_root = first;
	UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(stop_block0_poll(true), CLUSTER_NORMAL_STOP_READY);
	{
		ClusterUndoBlock0LogicalKey logical = make_key(2, 258);
		ClusterUndoBlock0AuthorityProof proof = make_live_proof(2, 7);
		ClusterUndoBlock0FrameToken token;
		/* A real local admission with another file's well-formed root is
		 * not a namespace-wide match. The stop observer must reject it. */
		make_valid_block0(258, 2, 0, 0);
		UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
		UT_ASSERT_EQ(
			cluster_undo_block0_admit_runtime(&logical, &first, &proof, &token, &pin, &page),
			CLUSTER_UNDO_BLOCK0_OK);
		cluster_undo_block0_unpin(&pin);
		UT_ASSERT_EQ(stop_block0_poll(false), CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT_EQ(stop_segment, 258);
		UT_ASSERT_STR_EQ(stop_reason, "UNDO_BLOCK0_RESIDENT_IDENTITY_INVALID");
	}
}

int
main(void)
{
	UT_PLAN(58);
	UT_RUN(test_block0_key_endpoints_map_to_direct_slots);
	UT_RUN(test_block0_key_rejects_owner_segment_aliases);
	UT_RUN(test_block0_root_requires_pgrd_identity_and_declared_intent);
	UT_RUN(test_block0_root_match_is_field_exact);
	UT_RUN(test_block0_generation_keeps_zero_distinct_from_absent);
	UT_RUN(test_block0_generation_exhaustion_never_wraps);
	UT_RUN(test_block0_slot_state_allows_only_frozen_edges);
	UT_RUN(test_block0_slot_state_rejects_direct_publish_and_double_publish);
	UT_RUN(test_block0_checked_size_matches_frozen_default_increment);
	UT_RUN(test_block0_attach_mismatch_detaches_fail_closed);
	UT_RUN(test_block0_attach_rejects_impossible_free_count);
	UT_RUN(test_block0_frame_bank_is_sparse_and_all_or_none);
	UT_RUN(test_block0_runtime_admission_preserves_generation_zero_and_exact_identity);
	UT_RUN(test_block0_unpin_after_error_interrupt_reset_is_balanced);
	UT_RUN(test_block0_runtime_admission_rejects_exhausted_generation);
	UT_RUN(test_block0_strict_empty_proof_excludes_filling_and_resident_states);
	UT_RUN(test_block0_copy_readonly_is_recovery_only_and_generation_exact);
	UT_RUN(test_block0_resident_copy_rejects_empty_generation_and_root_drift_without_io);
	UT_RUN(test_block0_pin_error_drops_reservation_before_rethrow);
	UT_RUN(test_block0_filling_is_not_a_positive_resident_copy);
	UT_RUN(test_block0_runtime_fill_abort_releases_slot_and_frame_via_resource_owner);
	UT_RUN(test_block0_resource_owner_cleanup_is_balanced_with_normal_release);
	UT_RUN(test_block0_provision_existing_exact_loads_without_temp_or_write);
	UT_RUN(test_block0_provision_rejects_non_live_authority_before_io);
	UT_RUN(test_block0_provision_rejects_unknown_probe_state_without_consuming_frame);
	UT_RUN(test_block0_provision_absent_creator_publishes_after_wal_flush);
	UT_RUN(test_block0_provision_eexist_loser_installs_exact_winner_image);
	UT_RUN(test_block0_provision_abort_cleans_temp_and_returns_frame);
	UT_RUN(test_block0_provision_publish_error_resource_owner_cleans_exact_temp);
	UT_RUN(test_block0_recovery_private_begin_reads_without_allocating_a_frame);
	UT_RUN(test_block0_recovery_private_begin_rejects_live_authority_before_io);
	UT_RUN(test_block0_recovery_private_bad_identity_does_not_publish_disk_bytes);
	UT_RUN(test_block0_recovery_private_absence_stays_fail_closed_without_typed_smgr_result);
	UT_RUN(test_block0_recovery_private_finish_writes_fsyncs_and_releases_without_residency);
	UT_RUN(test_block0_recovery_private_finish_validated_noop_has_zero_io);
	UT_RUN(test_block0_recovery_private_finish_write_failure_panics_with_guard_unpublished);
	UT_RUN(test_block0_recovery_private_parent_fsync_gap_panics_before_write);
	UT_RUN(test_block0_wal_dirty_uses_monotone_lsn_and_flushes_before_durable_data);
	UT_RUN(test_block0_flush_publishes_successor_generation);
	UT_RUN(test_block0_flush_failure_preserves_resident_predecessor_and_dirty_lsn);
	UT_RUN(test_block0_flush_parent_fsync_request_fails_before_wal_or_data_publication);
	UT_RUN(test_block0_flush_rejects_wrong_logical_successor_before_wal_or_data);
	UT_RUN(test_block0_clean_census_requires_exact_complete_unpinned_residency);
	UT_RUN(test_normal_start_empty_requires_exclusive_owner_and_whole_pool);
	UT_RUN(test_normal_start_discard_is_exact_clean_unpinned_prefix_only);
	UT_RUN(test_normal_start_discard_never_drops_dirty_or_foreign_binding);
	UT_RUN(test_r4_prerequisite_snapshot_is_exact_and_repeatable);
	UT_RUN(test_r4_prerequisite_snapshot_is_fixed_for_concurrent_callers);
	UT_RUN(test_r4_publish_ready_remains_fail_closed_for_every_unbound_input);
	UT_RUN(test_r4_publish_ready_accepts_only_owner_cosampled_snapshot);
	UT_RUN(test_r4_startup_completion_surface_refuses_without_owner_proofs);
	UT_RUN(test_stop_block0_requires_owner_root_and_initialized_state);
	UT_RUN(test_stop_block0_reserved_frames_need_original_release);
	UT_RUN(test_stop_block0_pin_is_debt_but_unpinned_residency_is_not);
	UT_RUN(test_stop_block0_checks_all_tt_slots_in_last_logical_segment);
	UT_RUN(test_stop_block0_durability_and_undo_anchor_are_not_cleared_by_poll);
	UT_RUN(test_stop_block0_invalid_late_slot_overrides_active_and_root_drift);
	UT_RUN(test_stop_block0_two_actual_segment_roots_share_one_pgrd_not_one_file_id);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
