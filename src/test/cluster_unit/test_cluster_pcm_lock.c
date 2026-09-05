/*-------------------------------------------------------------------------
 *
 * test_cluster_pcm_lock.c
 *	  Compile-time + link-time + behavioral invariants for the PCM lock
 *	  9-state machine activated in spec-2.30.
 *
 *	  spec-2.30 replaces spec-1.7 4-stub bodies with the real 9-transition
 *	  state machine + GrdEntry HTAB + per-entry LWLockPadded.  This test
 *	  binary links cluster_pcm_lock.o + provides minimal stubs for all PG
 *	  runtime deps (ShmemInit*, LWLock*, hash_*, ereport, etc) so that
 *	  both pure-function paths and the real acquire/release/upgrade/
 *	  downgrade/query state machine can be exercised standalone.
 *
 *	  Test plan (26 tests; spec-2.30 §4.1 + codereview hardening):
 *	    T-pcm-1..8   :  transition validator returns true for legal (from, to, trans)
 *	    T-pcm-9      :  Trans-9 reserved as legal entry (validator accepts)
 *	    T-pcm-10     :  transition validator rejects illegal combinations
 *	    T-pcm-11     :  disable path (ClusterPcm == NULL):  counter accessors return 0
 *	    T-pcm-12     :  HTAB-FULL surface (link-only;  cap enforcement)
 *	    T-pcm-13     :  per-entry LWLock granularity invariant (symbol existence)
 *	    T-pcm-14     :  PI bitmap atomic primitive present (link-only)
 *	    T-pcm-15     :  9 counter accessor surface returns 0 under disabled path
 *	    T-pcm-16..20 :  real acquire/release/upgrade/downgrade/query paths,
 *	                   live summary rows, and wait-event callsites
 *
 *	  The fake shared HTAB below is intentionally tiny, but it models the
 *	  behaviours that matter for PCM correctness: key lookup, cap-full,
 *	  shared holder bitmap updates, per-entry LWLock ownership assertions,
 *	  and SQL-visible summary counters.
 *
 *	  Spec: spec-2.30-pcm-9-state-machine-activation.md (FROZEN v0.3)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_pcm_lock.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_buffer_desc.h" /* PcmState (1.6) */
#include "cluster/cluster_cssd.h"		 /* spec-4.7a D4 — ClusterCssdPeerState for stub */
#include "cluster/cluster_inject.h"
#include "cluster/cluster_gcs_block.h" /* spec-4.7 D1 — ClusterGcsBlockPhase + phase_for_tag proto */
#include "cluster/cluster_lms.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_resource_x_identity.h"
#include "cluster/cluster_resource_x_node_wire.h"
#include "cluster/cluster_shmem.h"
#include "storage/backendid.h"	   /* spec-6.14 D9 amend — MyBackendId stub */
#include "storage/buf_internals.h" /* BufferTag */
#include "storage/lwlock.h"
#include "utils/hsearch.h"

extern ResourceXApplyResult
cluster_pcm_lock_resource_x_holder_pair_drain_prepare_exact(
	const ResourceXAssertion *assertion, uint64 assertion_sequence,
	int32 authenticated_master_node, uint64 authenticated_master_session,
	uint64 *source_generation_out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_holder_pair_drain_commit_exact(
	const ResourceXAssertion *assertion, uint64 assertion_sequence,
	int32 authenticated_master_node, uint64 authenticated_master_session,
	uint64 source_generation);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_settled_retire_exact(
	const ResourceXAssertion *assertion, uint64 assertion_sequence,
	const ResourceXMasterSnapshot *settled);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_block_to_n_prepared_s_source_exact(
	const ResourceXDecodedFrame *block, int32 authenticated_master_node,
	const ResourceXDecodedFrame *blocked_status,
	const ResourceXDecodedFrame *image_envelope,
	const ClusterPcmOwnSnapshot *prepared_source, XLogRecPtr prepared_page_lsn,
	uint64 prepared_page_scn, uint32 prepared_page_checksum);
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include <setjmp.h>

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf and we don't link libpgport in this test binary. */
#undef printf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


/* ============================================================
 * PG-runtime stubs + fake shared HTAB.
 * ============================================================ */

int cluster_node_id = 0;
int NBuffers = 0;
int cluster_injection_armed_count = 0;
int cluster_gcs_reply_timeout_ms = 1;
static uint64 ut_lms_master_generation = (UINT64_C(1) << 32) | UINT64_C(1);
static uint32 ut_wait_event_info_storage = 0;
static int fake_gcs_master_node = -1;
static bool fake_gcs_transition_allowed = false;
static int fake_gcs_transition_count = 0;
static BufferTag fake_gcs_transition_tag;
static PcmLockTransition fake_gcs_transition_kind;
static int fake_gcs_transition_master = -1;
static bool fake_gcs_transition_rebind = false;
static uint64 fake_gcs_transition_rebound_generation = 0;
static bool fake_gcs_transition_apply_to_pcm = false;
static bool fake_gcs_transition_applied = false;
uint32 *my_wait_event_info = &ut_wait_event_info_storage;

uint64
cluster_lms_get_shard_master_generation(void)
{
	return ut_lms_master_generation;
}

void
cluster_lms_wakeup(int worker_id pg_attribute_unused())
{}

#define FAKE_PCM_MAX_ENTRIES 24
#define FAKE_PCM_ENTRY_BYTES 1056

static uint64 fake_pcm_clock_us;
static void (*fake_pcm_clock_hook)(void);
static unsigned int fake_pcm_clock_hook_countdown;
int cluster_test_pcm_clock_gettime(clockid_t clock_id, struct timespec *time_out);

int
cluster_test_pcm_clock_gettime(clockid_t clock_id, struct timespec *time_out)
{
	if (fake_pcm_clock_hook != NULL && --fake_pcm_clock_hook_countdown == 0) {
		void (*hook)(void) = fake_pcm_clock_hook;

		fake_pcm_clock_hook = NULL;
		hook();
	}
	if (fake_pcm_clock_us == 0)
		return clock_gettime(clock_id, time_out);
	time_out->tv_sec = fake_pcm_clock_us / UINT64_C(1000000);
	time_out->tv_nsec = (fake_pcm_clock_us % UINT64_C(1000000)) * UINT64_C(1000);
	return 0;
}

static union {
	uint64 force_align;
	/* Sized above sizeof(ClusterPcmShared) plus the 17-entry Resource-X
	 * reconfiguration fixture.  The retained holder image adds one full
	 * page per Resource-X master state; fake ShmemInitStruct asserts fit. */
	char data[2097152];
} fake_pcm_header;

static union {
	uint64 force_align;
	char data[FAKE_PCM_MAX_ENTRIES][FAKE_PCM_ENTRY_BYTES];
} fake_pcm_entries;

static char fake_pcm_htab_token;
static bool fake_pcm_header_found = false;
static Size fake_pcm_header_requested_size = 0;
static long fake_pcm_entry_count = 0;
static long fake_pcm_entry_max = 0;
static Size fake_pcm_keysize = 0;
static Size fake_pcm_entrysize = 0;
static LWLock *fake_lwlock_held = NULL;
static LWLockMode fake_lwlock_mode = LW_EXCLUSIVE;
static LWLock *fake_lwlock_stack[16];
static LWLockMode fake_lwlock_mode_stack[16];
static int fake_lwlock_depth = 0;
static uint32 fake_init_wait_event_seen = 0;
static uint32 fake_lwlock_wait_event_seen = 0;
static bool fake_lwlock_conditional_fail_once = false;

/* PGRAC: spec-2.31 D1 v0.4 — ConditionVariable stub counters (declared
 * here so reset_fake_pcm_runtime() can clear them;  definitions of the
 * stub functions themselves live below LWLockHeldByMeInMode). */
static int fake_cv_init_count = 0;
static int fake_cv_prepare_count = 0;
static int fake_cv_sleep_count = 0;
static int fake_cv_cancel_count = 0;
static int fake_cv_broadcast_count = 0;
static long fake_cv_timed_sleep_timeout = 0;
static bool fake_cv_timed_sleep_timed_out = false;
static bool fake_cv_prepared = false;
static bool fake_cv_signaled = false;
static uint32 fake_cv_sleep_wait_event = 0;
static void (*fake_cv_prepare_hook)(void) = NULL;
static struct {
	BufferTag tag;
	int holder_node;
	bool armed;
} fake_cv_wake_release = { { 0 }, 0, false };
static struct {
	BufferTag tag;
	int requester_node;
	uint64 ticket_id;
	bool armed;
} fake_cv_wake_pending_x_clear = { { 0 }, 0, 0, false };

static sigjmp_buf ut_ereport_jump;
static bool ut_ereport_jump_armed = false;
static int ut_ereport_fired_count = 0;
static bool fake_local_x_upgrade_result = false;
static bool fake_acquire_entry_handoff_armed = false;
static BufferTag fake_acquire_entry_handoff_tag;
static int fake_acquire_entry_handoff_source = -1;
static int fake_acquire_entry_handoff_target = -1;
static PcmLockTransition fake_acquire_entry_handoff_release = PCM_TRANS_X_TO_N_RELEASE;
static int fake_local_read_image_count = 0;
static int fake_local_read_image_holder = -1;
static PcmAuthoritySnapshot fake_local_read_image_expected;
static int fake_local_x_transfer_count = 0;
static int fake_local_x_transfer_holder = -1;
static PcmAuthoritySnapshot fake_local_x_transfer_expected;
static bool fake_pcm_x_local_s_barrier_active = false;
static int fake_pcm_x_local_s_barrier_checks = 0;
static ResourceXSidecarNeutralizeResult fake_neutralize_result
	= RESOURCE_X_SIDECAR_NEUTRALIZED;
static int fake_neutralize_count = 0;
static bool fake_neutralize_without_pcm_lock = false;
static BufferTag fake_neutralize_tag;
static uint64 fake_neutralize_formation = 0;
static uint64 fake_neutralize_generation = 0;
static uint64 fake_resource_x_transport_staged_count = 0;
static uint64 fake_resource_x_transport_mutation_sequence = 1;
static bool fake_resource_x_transport_snapshot_available = true;
static uint32 fake_resource_x_transport_snapshot_call_count = 0;
static uint32 fake_resource_x_transport_advance_on_call = 0;

bool
cluster_lms_outbound_resource_x_transport_snapshot(
	ClusterLmsResourceXTransportSnapshot *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (!fake_resource_x_transport_snapshot_available || out == NULL)
		return false;
	fake_resource_x_transport_snapshot_call_count++;
	if (fake_resource_x_transport_snapshot_call_count
		== fake_resource_x_transport_advance_on_call)
		fake_resource_x_transport_mutation_sequence++;
	out->mutation_sequence = fake_resource_x_transport_mutation_sequence;
	out->staged_count = fake_resource_x_transport_staged_count;
	return true;
}

uint64
cluster_lms_outbound_resource_x_staged_count(void)
{
	return fake_resource_x_transport_staged_count;
}

ResourceXSidecarNeutralizeResult
cluster_bufmgr_resource_x_neutralize_exact(const BufferTag *tag, uint64 old_formation,
										   uint64 acquisition_generation)
{
	fake_neutralize_count++;
	fake_neutralize_without_pcm_lock = fake_lwlock_depth == 0;
	if (tag != NULL)
		fake_neutralize_tag = *tag;
	fake_neutralize_formation = old_formation;
	fake_neutralize_generation = acquisition_generation;
	return fake_neutralize_result;
}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* spec-4.6a Amendment v1.2 (R5): the S->X upgrade is now wrapped in
 * PG_TRY/PG_CATCH, which references the exception stack + re-throw.  The
 * unit's errfinish mock longjmps to its own buffer (never through
 * PG_exception_stack), so these exist for the linker only. */
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void
pg_re_throw(void)
{
	abort();
}

static BufferTag
make_tag(uint32 blockno)
{
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	tag.spcOid = 1663;
	tag.dbOid = 1;
	tag.relNumber = 100;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = blockno;
	return tag;
}

static char *
read_text_file(const char *path)
{
	FILE *file;
	char *source;
	long length;

	file = fopen(path, "rb");
	UT_ASSERT_NOT_NULL(file);
	if (file == NULL)
		return NULL;
	UT_ASSERT_EQ(fseek(file, 0, SEEK_END), 0);
	length = ftell(file);
	UT_ASSERT(length > 0);
	UT_ASSERT_EQ(fseek(file, 0, SEEK_SET), 0);
	source = malloc((size_t)length + 1);
	UT_ASSERT_NOT_NULL(source);
	if (source == NULL) {
		fclose(file);
		return NULL;
	}
	UT_ASSERT_EQ(fread(source, 1, (size_t)length, file), (size_t)length);
	source[length] = '\0';
	fclose(file);
	return source;
}

static void
reset_fake_pcm_runtime(int max_entries)
{
	fake_pcm_clock_us = 0;
	fake_pcm_clock_hook = NULL;
	fake_pcm_clock_hook_countdown = 0;
	memset(&fake_pcm_header, 0, sizeof(fake_pcm_header));
	memset(&fake_pcm_entries, 0, sizeof(fake_pcm_entries));
	fake_pcm_header_found = false;
	fake_pcm_header_requested_size = 0;
	fake_pcm_entry_count = 0;
	fake_pcm_entry_max = max_entries;
	fake_pcm_keysize = 0;
	fake_pcm_entrysize = 0;
	fake_lwlock_held = NULL;
	fake_lwlock_mode = LW_EXCLUSIVE;
	fake_lwlock_depth = 0;
	memset(fake_lwlock_stack, 0, sizeof(fake_lwlock_stack));
	memset(fake_lwlock_mode_stack, 0, sizeof(fake_lwlock_mode_stack));
	fake_init_wait_event_seen = 0;
	fake_lwlock_wait_event_seen = 0;
	fake_lwlock_conditional_fail_once = false;
	ut_wait_event_info_storage = 0;
	ut_ereport_fired_count = 0;
	ut_ereport_jump_armed = false;
	fake_cv_init_count = 0;
	fake_cv_prepare_count = 0;
	fake_cv_sleep_count = 0;
	fake_cv_cancel_count = 0;
	fake_cv_broadcast_count = 0;
	fake_cv_timed_sleep_timeout = 0;
	fake_cv_timed_sleep_timed_out = false;
	fake_cv_prepared = false;
	fake_cv_signaled = false;
	fake_cv_sleep_wait_event = 0;
	fake_cv_prepare_hook = NULL;
	fake_cv_wake_release.armed = false;
	fake_cv_wake_pending_x_clear.armed = false;
	fake_acquire_entry_handoff_armed = false;
	fake_acquire_entry_handoff_release = PCM_TRANS_X_TO_N_RELEASE;
	fake_local_read_image_count = 0;
	fake_local_read_image_holder = -1;
	memset(&fake_local_read_image_expected, 0, sizeof(fake_local_read_image_expected));
	fake_local_x_upgrade_result = false;
	fake_pcm_x_local_s_barrier_active = false;
	fake_pcm_x_local_s_barrier_checks = 0;
	fake_neutralize_result = RESOURCE_X_SIDECAR_NEUTRALIZED;
	fake_neutralize_count = 0;
	fake_neutralize_without_pcm_lock = false;
	memset(&fake_neutralize_tag, 0, sizeof(fake_neutralize_tag));
	fake_neutralize_formation = 0;
	fake_neutralize_generation = 0;
	fake_resource_x_transport_staged_count = 0;
	fake_resource_x_transport_mutation_sequence = 1;
	fake_resource_x_transport_snapshot_available = true;
	fake_resource_x_transport_snapshot_call_count = 0;
	fake_resource_x_transport_advance_on_call = 0;
	fake_gcs_master_node = -1;
	fake_gcs_transition_allowed = false;
	fake_gcs_transition_count = 0;
	memset(&fake_gcs_transition_tag, 0, sizeof(fake_gcs_transition_tag));
	fake_gcs_transition_kind = PCM_TRANS_N_TO_S;
	fake_gcs_transition_master = -1;
	fake_gcs_transition_rebind = false;
	fake_gcs_transition_rebound_generation = 0;
	fake_gcs_transition_apply_to_pcm = false;
	fake_gcs_transition_applied = false;
	cluster_node_id = 0;
	NBuffers = max_entries;
	cluster_pcm_grd_max_entries = max_entries;
	cluster_pcm_grd_init();
}

/*
 * spec-4.7a B — cluster_pcm_lock.o's local acquire wait-path reads this GUC to
 * decide the bounded-fail-closed for a cross-node write transfer.  Stubbed OFF
 * here: the acquire state-machine tests below exercise transition logic, which
 * is GUC-independent.  The GUC-on bounded-fail-closed (B) needs a REAL remote
 * live holder (cssd liveness of a peer); the single-node unit harness stubs
 * cssd always-alive and fakes the GrdEntry, so it cannot model that path
 * honestly — it is e2e-tested by t/252 L3b (2-node, real cssd liveness).
 */
bool cluster_gcs_block_local_cache = false;

/* spec-6.12a stubs — the local-master S->X upgrade path is only reached with
 * the wave GUC on (default off here, so the branch is inert); provide inert
 * link-surface satisfaction.  Real coverage is t/352 (2-node). */
bool cluster_read_scache = false;

bool cluster_gcs_block_local_x_upgrade(BufferTag tag);
bool
cluster_gcs_block_local_x_upgrade(BufferTag tag)
{
	uint32 holders_bm;
	int n;

	if (!fake_local_x_upgrade_result)
		return false;
	holders_bm = cluster_pcm_lock_query_s_holders_bitmap(tag);
	if (cluster_node_id >= 0 && cluster_node_id < 32)
		holders_bm &= ~((uint32)1u << (uint32)cluster_node_id);
	for (n = 0; n < 32; n++)
		if ((holders_bm & ((uint32)1u << n)) != 0)
			(void)cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_S_TO_N_INVALIDATE, n);
	return cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_S_TO_X_UPGRADE, cluster_node_id);
}

/* spec-4.7a D4 — stub CSSD peer liveness for the other-live-holder gate.
 * Default: every peer ALIVE.  A test sets fake_cssd_dead_node to mark one
 * peer DEAD (to verify a dead holder is NOT counted by the D4 gate). */
static int32 fake_cssd_dead_node = -1;

ClusterCssdPeerState
cluster_cssd_get_peer_state(int32 peer_id)
{
	return (peer_id == fake_cssd_dead_node) ? CLUSTER_CSSD_PEER_DEAD : CLUSTER_CSSD_PEER_ALIVE;
}

/*
 * spec-4.7 D1 (L238) — cluster_pcm_lock.o's acquire_buffer now opens with a
 * RECOVERING gate that references cluster_gcs_block_phase_for_tag,
 * cluster_gcs_block_recovery_wait_ms and CHECK_FOR_INTERRUPTS.  This test
 * links cluster_pcm_lock.o but not cluster_gcs_block.o / cluster_guc.o /
 * postmaster core, so provide link-time stubs.  phase_for_tag → NORMAL keeps
 * the gate a no-op so these tests exercise the local acquire state machine,
 * not the recovery path (covered e2e by t/251).
 */
volatile sig_atomic_t InterruptPending = false;
void ProcessInterrupts(void);
void
ProcessInterrupts(void)
{}
int cluster_gcs_block_recovery_wait_ms = 200;

/* Controllable phase: default NORMAL (gate no-op so the state-machine tests
 * pass straight through);  a test sets it RECOVERING to drive the D1 gate. */
static ClusterGcsBlockPhase fake_block_phase = GCS_BLOCK_NORMAL;
ClusterGcsBlockPhase
cluster_gcs_block_phase_for_tag(BufferTag tag pg_attribute_unused())
{
	return fake_block_phase;
}

/* spec-4.7 D3 (L238) — the rebuild fn's not-double-X branch bumps this 4.6
 * counter;  stub it no-op for the unit harness. */
void cluster_grd_inc_block_path_failclosed(void);
void
cluster_grd_inc_block_path_failclosed(void)
{}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size,
				bool *foundPtr)
{
	Assert(size <= sizeof(fake_pcm_header.data));
	fake_pcm_header_requested_size = size;
	fake_init_wait_event_seen = ut_wait_event_info_storage;
	*foundPtr = fake_pcm_header_found;
	fake_pcm_header_found = true;
	return fake_pcm_header.data;
}

HTAB *
ShmemInitHash(const char *name pg_attribute_unused(), long init_size pg_attribute_unused(),
			  long max_size pg_attribute_unused(), HASHCTL *infoP pg_attribute_unused(),
			  int hash_flags pg_attribute_unused())
{
	Assert((hash_flags & HASH_ELEM) != 0);
	Assert(infoP->entrysize <= FAKE_PCM_ENTRY_BYTES);
	Assert(max_size <= FAKE_PCM_MAX_ENTRIES);
	fake_pcm_keysize = infoP->keysize;
	fake_pcm_entrysize = infoP->entrysize;
	fake_pcm_entry_max = max_size;
	fake_pcm_entry_count = 0;
	memset(&fake_pcm_entries, 0, sizeof(fake_pcm_entries));
	return (HTAB *)&fake_pcm_htab_token;
}

void *
hash_search(HTAB *hashp pg_attribute_unused(), const void *keyPtr pg_attribute_unused(),
			HASHACTION action pg_attribute_unused(), bool *foundPtr pg_attribute_unused())
{
	long i;

	Assert(hashp == (HTAB *)&fake_pcm_htab_token);
	Assert(fake_pcm_keysize > 0);
	Assert(fake_pcm_entrysize > 0);

	for (i = 0; i < fake_pcm_entry_count; i++) {
		char *entry = fake_pcm_entries.data[i];

		if (memcmp(entry, keyPtr, fake_pcm_keysize) == 0) {
			if (foundPtr != NULL)
				*foundPtr = true;
			if (action == HASH_REMOVE) {
				if (i + 1 < fake_pcm_entry_count)
					memmove(fake_pcm_entries.data[i], fake_pcm_entries.data[i + 1],
							(size_t)(fake_pcm_entry_count - i - 1) * FAKE_PCM_ENTRY_BYTES);
				fake_pcm_entry_count--;
				return entry;
			}
			return entry;
		}
	}

	if (foundPtr != NULL)
		*foundPtr = false;
	if (action == HASH_FIND || action == HASH_REMOVE)
		return NULL;
	if (action == HASH_ENTER_NULL && fake_pcm_entry_count >= fake_pcm_entry_max)
		return NULL;
	if (action == HASH_ENTER || action == HASH_ENTER_NULL) {
		char *entry = fake_pcm_entries.data[fake_pcm_entry_count++];

		memset(entry, 0, FAKE_PCM_ENTRY_BYTES);
		memcpy(entry, keyPtr, fake_pcm_keysize);
		return entry;
	}
	return NULL;
}

long
hash_get_num_entries(HTAB *hashp pg_attribute_unused())
{
	return fake_pcm_entry_count;
}

Size
hash_estimate_size(long num_entries pg_attribute_unused(), Size entrysize pg_attribute_unused())
{
	return (Size)num_entries * entrysize;
}

void
hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp)
{
	status->hashp = hashp;
	status->curBucket = 0;
	status->curEntry = NULL;
}

void *
hash_seq_search(HASH_SEQ_STATUS *status)
{
	if (status->curBucket >= (uint32)fake_pcm_entry_count)
		return NULL;
	return fake_pcm_entries.data[status->curBucket++];
}

void
hash_seq_term(HASH_SEQ_STATUS *status pg_attribute_unused())
{}

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	Assert(fake_lwlock_depth < (int)lengthof(fake_lwlock_stack));
	fake_lwlock_stack[fake_lwlock_depth] = lock;
	fake_lwlock_mode_stack[fake_lwlock_depth] = mode;
	fake_lwlock_depth++;
	fake_lwlock_held = lock;
	fake_lwlock_mode = mode;
	fake_lwlock_wait_event_seen = ut_wait_event_info_storage;
	return true;
}

bool
LWLockConditionalAcquire(LWLock *lock, LWLockMode mode)
{
	if (fake_lwlock_conditional_fail_once) {
		fake_lwlock_conditional_fail_once = false;
		return false;
	}
	return LWLockAcquire(lock, mode);
}

void
LWLockRelease(LWLock *lock)
{
	Assert(fake_lwlock_depth > 0);
	Assert(fake_lwlock_stack[fake_lwlock_depth - 1] == lock);
	fake_lwlock_depth--;
	fake_lwlock_stack[fake_lwlock_depth] = NULL;
	if (fake_lwlock_depth > 0) {
		fake_lwlock_held = fake_lwlock_stack[fake_lwlock_depth - 1];
		fake_lwlock_mode = fake_lwlock_mode_stack[fake_lwlock_depth - 1];
	} else {
		fake_lwlock_held = NULL;
		fake_lwlock_mode = LW_EXCLUSIVE;
	}
}

bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	int i;

	for (i = fake_lwlock_depth - 1; i >= 0; i--)
		if (fake_lwlock_stack[i] == lock && fake_lwlock_mode_stack[i] == mode)
			return true;
	return false;
}

/* ----------
 * PGRAC: spec-2.31 D1 v0.4 — ConditionVariable stubs for PCM-H1..H4.
 *
 *	cluster_pcm_lock.c now uses ConditionVariable for incompatible-state
 *	wait.  Unit tests are single-threaded, so the Sleep stub can't really
 *	block;  instead it records the call and (if armed) performs a release
 *	on a target tag so the acquire loop sees compatible state on retry.
 *	Counter variable declarations live above (before reset_fake_pcm_runtime).
 * ----------
 */
void
ConditionVariableInit(ConditionVariable *cv pg_attribute_unused())
{
	fake_cv_init_count++;
}

void
ConditionVariablePrepareToSleep(ConditionVariable *cv pg_attribute_unused())
{
	fake_cv_prepare_count++;
	fake_cv_prepared = true;
	if (fake_cv_prepare_hook != NULL) {
		void (*hook)(void) = fake_cv_prepare_hook;

		fake_cv_prepare_hook = NULL;
		hook();
	}
}

void
ConditionVariableSleep(ConditionVariable *cv pg_attribute_unused(), uint32 wait_event_info)
{
	fake_cv_sleep_count++;
	fake_cv_sleep_wait_event = wait_event_info;
	if (fake_cv_wake_release.armed) {
		int save_node = cluster_node_id;

		fake_cv_wake_release.armed = false;
		cluster_node_id = fake_cv_wake_release.holder_node;
		cluster_pcm_lock_release(fake_cv_wake_release.tag);
		cluster_node_id = save_node;
	}
	if (fake_cv_wake_pending_x_clear.armed) {
		fake_cv_wake_pending_x_clear.armed = false;
		UT_ASSERT(cluster_pcm_lock_clear_queue_pending_x_exact(
			fake_cv_wake_pending_x_clear.tag, fake_cv_wake_pending_x_clear.requester_node,
			fake_cv_wake_pending_x_clear.ticket_id));
	}
}

bool
ConditionVariableTimedSleep(ConditionVariable *cv pg_attribute_unused(), long timeout,
							uint32 wait_event_info)
{
	fake_cv_sleep_count++;
	fake_cv_timed_sleep_timeout = timeout;
	fake_cv_sleep_wait_event = wait_event_info;
	fake_cv_timed_sleep_timed_out = !fake_cv_signaled;
	fake_cv_signaled = false;
	return fake_cv_timed_sleep_timed_out;
}

bool
ConditionVariableCancelSleep(void)
{
	fake_cv_cancel_count++;
	fake_cv_prepared = false;
	fake_cv_signaled = false;
	return false;
}

void
ConditionVariableBroadcast(ConditionVariable *cv pg_attribute_unused())
{
	fake_cv_broadcast_count++;
	if (fake_cv_prepared)
		fake_cv_signaled = true;
}

void
ConditionVariableSignal(ConditionVariable *cv pg_attribute_unused())
{
	/* unused by cluster_pcm_lock.c but linker may require */
}

TimestampTz
GetCurrentTimestamp(void)
{
	return (TimestampTz)0;
}

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

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

void
cluster_injection_run(const char *name)
{
	if (fake_acquire_entry_handoff_armed && strcmp(name, "cluster-pcm-acquire-entry") == 0) {
		fake_acquire_entry_handoff_armed = false;
		cluster_injection_armed_count = 0;
		UT_ASSERT_EQ((int)cluster_pcm_lock_apply_gcs_transition(fake_acquire_entry_handoff_tag,
																fake_acquire_entry_handoff_release,
																fake_acquire_entry_handoff_source),
					 1);
		UT_ASSERT_EQ((int)cluster_pcm_lock_apply_gcs_transition(fake_acquire_entry_handoff_tag,
																PCM_TRANS_N_TO_X,
																fake_acquire_entry_handoff_target),
					 1);
	}
}

/* PGRAC spec-2.32 D5 stubs:  cluster_pcm_lock.c now calls cluster_gcs
 * helpers from each mutation entry point (master lookup branch).  Test
 * fixture forces local path by returning cluster_node_id from lookup. */
int
cluster_gcs_lookup_master(BufferTag tag pg_attribute_unused())
{
	return fake_gcs_master_node >= 0 ? fake_gcs_master_node : cluster_node_id;
}

void
cluster_gcs_send_transition_and_wait(BufferTag tag,
									 PcmLockTransition trans,
									 int master_node)
{
	if (!fake_gcs_transition_allowed)
		abort();
	fake_gcs_transition_count++;
	fake_gcs_transition_tag = tag;
	fake_gcs_transition_kind = trans;
	fake_gcs_transition_master = master_node;
	if (fake_gcs_transition_apply_to_pcm)
		fake_gcs_transition_applied
			= cluster_pcm_lock_apply_gcs_transition(tag, trans, cluster_node_id);
	if (fake_gcs_transition_rebind)
	{
		PcmEntryAcquireResult acquire_result;
		PcmEntryRef ref;
		uint64 old_generation;

		UT_ASSERT(pcm_entry_ref_acquire(
			&tag, false, &ref, &acquire_result));
		old_generation = ref.binding_generation;
		pcm_entry_ref_release(&ref);
		UT_ASSERT(pcm_entry_try_retire_exact(
			&tag, old_generation, PCM_RETIRE_REASON_HOLDER_RELEASE));
		UT_ASSERT(pcm_entry_ref_acquire(
			&tag, true, &ref, &acquire_result));
		fake_gcs_transition_rebound_generation = ref.binding_generation;
		pcm_entry_ref_release(&ref);
	}
}

bool
cluster_gcs_try_send_transition_and_wait(BufferTag tag,
										 PcmLockTransition trans,
										 int master_node)
{
	if (!fake_gcs_transition_allowed)
		return false;
	fake_gcs_transition_tag = tag;
	fake_gcs_transition_kind = trans;
	fake_gcs_transition_master = master_node;
	if (!fake_gcs_transition_apply_to_pcm)
		return false;
	fake_gcs_transition_applied
		= cluster_pcm_lock_apply_gcs_transition(tag, trans, cluster_node_id);
	return fake_gcs_transition_applied;
}

/* spec-2.33 D3 stub: cluster_pcm_lock_acquire_buffer (D7) takes the data-
 * plane branch when master is remote.  Fixture forces local path; reaching
 * here = test bug. */
bool
cluster_gcs_send_block_request_and_wait(struct BufferDesc *buf pg_attribute_unused(),
										PcmLockTransition trans pg_attribute_unused(),
										int master_node pg_attribute_unused(),
										bool clean_eligible pg_attribute_unused(),
										bool *out_retry_denied pg_attribute_unused())
{
	abort();
}

/* spec-5.2 D2 sub-case B stub: local-master read-image forward.  The pcm_lock
 * fixture records the selected holder so optimistic-precheck handoff tests can
 * prove the buffer-aware S path routes to the existing one-shot image helper
 * instead of the tag-only fail-closed terminal. */
bool
cluster_gcs_local_master_read_image_and_wait(struct BufferDesc *buf pg_attribute_unused(),
											 const PcmAuthoritySnapshot *expected,
											 bool force_one_shot pg_attribute_unused(),
											 bool *out_retry_denied)
{
	*out_retry_denied = false;
	fake_local_read_image_count++;
	fake_local_read_image_expected = *expected;
	fake_local_read_image_holder = expected->x_holder_node;
	return false;
}

/* spec-5.2 D11 stub: record the authoritative holder selected by the
 * buffer-aware local-master path.  The real data-plane behavior is covered by
 * the GCS block tests; this fixture only proves PCM routing and authority. */
bool
cluster_gcs_local_master_x_transfer_and_wait(struct BufferDesc *buf pg_attribute_unused(),
											 const PcmAuthoritySnapshot *expected,
											 bool clean_eligible pg_attribute_unused(),
											 bool *out_retry_denied)
{
	*out_retry_denied = false;
	fake_local_x_transfer_count++;
	fake_local_x_transfer_expected = *expected;
	fake_local_x_transfer_holder = expected->x_holder_node;
	return true;
}

bool
cluster_gcs_block_resource_x_local_s_barrier_active(BufferTag tag pg_attribute_unused())
{
	fake_pcm_x_local_s_barrier_checks++;
	return fake_pcm_x_local_s_barrier_active;
}

/* spec-2.35 D3 stub:  HC110 master_holder lifecycle counter bump invoked
 * from cluster_pcm_transition_apply helpers.  Standalone fixture has no
 * ClusterGcsBlockShared; vacuous no-op. */
void
cluster_gcs_block_bump_master_holder_lifecycle(void)
{}

/* GCS-race round-4c FUNC-1 stub: the tag-only local-master grant tail calls
 * the storage-fallback SCN verify.  The standalone fixture has no
 * ClusterGcsBlockShared / no watermark (query returns InvalidScn), so the
 * real helper would short-circuit to a no-op — mirror that here. */
void
cluster_gcs_block_fallback_verify_refresh(struct BufferDesc *buf pg_attribute_unused(),
										  BufferTag tag pg_attribute_unused(),
										  SCN expected_scn pg_attribute_unused())
{}

/* ereport stubs — minimal enough to satisfy linker.  ereport(ERROR, ...) in
 * cluster_pcm_lock.o calls errstart_cold + errfinish; test_pcm_b_local_master_
 * remote_x_holder_fail_closed exercises that path via UT_EXPECT_EREPORT. */
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

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	ut_ereport_fired_count++;
	if (ut_ereport_jump_armed)
		siglongjmp(ut_ereport_jump, 1);
}

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

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/* spec-6.14 D9 amend: acquire_buffer's no-backend-identity guard reads
 * MyBackendId; a valid id (1) keeps the historical acquire paths open. */
BackendId MyBackendId = 1;

#define UT_EXPECT_EREPORT(stmt)                                                                    \
	do {                                                                                           \
		if (sigsetjmp(ut_ereport_jump, 1) == 0) {                                                  \
			ut_ereport_jump_armed = true;                                                          \
			stmt;                                                                                  \
			ut_ereport_jump_armed = false;                                                         \
			UT_ASSERT(false);                                                                      \
		} else {                                                                                   \
			ut_ereport_jump_armed = false;                                                         \
			UT_ASSERT(ut_ereport_fired_count > 0);                                                 \
		}                                                                                          \
	} while (0)


/* ============================================================
 * Tests
 * ============================================================ */

static void make_resource_x_remote_join_pair(
	BufferTag tag, int32 requester_node, ResourceXDecodedFrame *grant,
	ResourceXDecodedFrame *image);
static void retarget_resource_x_remote_join_pair(
	ResourceXDecodedFrame *grant, ResourceXDecodedFrame *image,
	uint64 final_authority_generation, uint64 requester_target_generation);
static ResourceXDecodedFrame make_resource_x_remote_blocked_frame(
	BufferTag tag, int32 requester_node, int32 source_node);
static void canonicalize_resource_x_test_image(ResourceXDecodedFrame *image);

UT_TEST(test_pcm_lock_mode_constant_aliases_match_pcm_state)
{
	UT_ASSERT_EQ((int)PCM_LOCK_MODE_N, 0);
	UT_ASSERT_EQ((int)PCM_LOCK_MODE_S, 1);
	UT_ASSERT_EQ((int)PCM_LOCK_MODE_X, 2);
	UT_ASSERT_EQ((int)PCM_LOCK_MODE_N, (int)PCM_STATE_N);
	UT_ASSERT_EQ((int)PCM_LOCK_MODE_S, (int)PCM_STATE_S);
	UT_ASSERT_EQ((int)PCM_LOCK_MODE_X, (int)PCM_STATE_X);
}

UT_TEST(test_pcm_lock_transition_count_is_9)
{
	UT_ASSERT_EQ(PCM_TRANSITION_COUNT, 9);
}

UT_TEST(test_pcm_lock_transition_enum_values_are_1_to_9)
{
	UT_ASSERT_EQ((int)PCM_TRANS_N_TO_S, 1);
	UT_ASSERT_EQ((int)PCM_TRANS_N_TO_X, 2);
	UT_ASSERT_EQ((int)PCM_TRANS_S_TO_X_UPGRADE, 3);
	UT_ASSERT_EQ((int)PCM_TRANS_X_TO_S_DOWNGRADE, 4);
	UT_ASSERT_EQ((int)PCM_TRANS_X_TO_N_DOWNGRADE, 5);
	UT_ASSERT_EQ((int)PCM_TRANS_X_TO_N_RELEASE, 6);
	UT_ASSERT_EQ((int)PCM_TRANS_S_TO_N_INVALIDATE, 7);
	UT_ASSERT_EQ((int)PCM_TRANS_S_TO_N_RELEASE, 8);
	UT_ASSERT_EQ((int)PCM_TRANS_S_TO_X_CLEANOUT, 9);
}

UT_TEST(test_pcm_grd_max_entries_default_is_minus_one)
{
	/*
	 * spec-2.30 D5:  default changed 0 → -1 sentinel (auto-resolve to
	 * NBuffers at startup);  explicit 0 = disable path.
	 */
	extern int cluster_pcm_grd_max_entries;
	UT_ASSERT_EQ(cluster_pcm_grd_max_entries, -1);
}

UT_TEST(test_pcm_buffer_desc_invariants_hold_at_stage_2_30)
{
	UT_ASSERT_EQ((int)PCM_STATE_N, 0);
	UT_ASSERT_EQ((int)PCM_STATE_S, 1);
	UT_ASSERT_EQ((int)PCM_STATE_X, 2);
}

UT_TEST(test_pcm_lock_module_init_symbol_is_callable)
{
	void (*fn)(void) = cluster_pcm_lock_module_init;
	UT_ASSERT(fn != NULL);
}


/* ============================================================
 * spec-2.30 NEW tests T-pcm-1..15.
 * ============================================================ */

/* T-pcm-1..8: validator accepts each of 8 active transitions. */
UT_TEST(test_pcm_trans_1_n_to_s_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_N, PCM_STATE_S, PCM_TRANS_N_TO_S));
}

UT_TEST(test_pcm_trans_2_n_to_x_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_N, PCM_STATE_X, PCM_TRANS_N_TO_X));
}

UT_TEST(test_pcm_trans_3_s_to_x_upgrade_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_S, PCM_STATE_X, PCM_TRANS_S_TO_X_UPGRADE));
}

UT_TEST(test_pcm_trans_4_x_to_s_downgrade_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_X, PCM_STATE_S, PCM_TRANS_X_TO_S_DOWNGRADE));
}

UT_TEST(test_pcm_trans_5_x_to_n_downgrade_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_X, PCM_STATE_N, PCM_TRANS_X_TO_N_DOWNGRADE));
}

UT_TEST(test_pcm_trans_6_x_to_n_release_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_X, PCM_STATE_N, PCM_TRANS_X_TO_N_RELEASE));
}

UT_TEST(test_pcm_trans_7_s_to_n_invalidate_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_S, PCM_STATE_N, PCM_TRANS_S_TO_N_INVALIDATE));
}

UT_TEST(test_pcm_trans_8_s_to_n_release_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_S, PCM_STATE_N, PCM_TRANS_S_TO_N_RELEASE));
}


/* T-pcm-9: HC60 — Trans-9 reachable from validator. */
UT_TEST(test_pcm_trans_9_cleanout_validator_reachable_but_apply_fail_closed)
{
	/*
	 * HC60 — validator accepts as legal entry transition (reachable from
	 * validator);  apply body fail-closed FEATURE_NOT_SUPPORTED until
	 * Stage 3 AD-006 第五轮 wires ITL cleanout.  Counter永 0.
	 */
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_S, PCM_STATE_X, PCM_TRANS_S_TO_X_CLEANOUT));
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_x_cleanout_count(), 0);
}


/* T-pcm-10: HC56 — validator rejects illegal combinations. */
UT_TEST(test_pcm_illegal_transition_validator_rejects)
{
	/* (N → X) with trans=N_TO_S code → illegal */
	UT_ASSERT(!cluster_pcm_transition_legal(PCM_STATE_N, PCM_STATE_X, PCM_TRANS_N_TO_S));
	/* (S → S) any trans → illegal (no self-transition) */
	UT_ASSERT(!cluster_pcm_transition_legal(PCM_STATE_S, PCM_STATE_S, PCM_TRANS_N_TO_S));
	/* (X → X) any trans → illegal */
	UT_ASSERT(!cluster_pcm_transition_legal(PCM_STATE_X, PCM_STATE_X, PCM_TRANS_N_TO_X));
}


/* T-pcm-11: disable path — ClusterPcm == NULL → counter accessors return 0. */
UT_TEST(test_pcm_disable_path_counters_return_zero)
{
	/*
	 * In cluster_unit binary cluster_pcm_grd_init is never called so
	 * ClusterPcm stays NULL — all 9 counter accessors return 0.
	 */
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_n_to_s_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_n_to_x_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_x_upgrade_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_x_to_s_downgrade_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_x_to_n_downgrade_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_x_to_n_release_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_n_invalidate_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_n_release_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_x_cleanout_count(), 0);
}


/* T-pcm-12: HC59 fail-closed cap — link-only surface verification. */
UT_TEST(test_pcm_grd_entry_lifecycle_link_surface)
{
	/*
	 * HC59 lifecycle (alloc-on-first-acquire / never-freed-until-shutdown)
	 * is verified by cluster_tap 108 under a live postmaster.  Here we
	 * verify the cap GUC surface exists.
	 */
	extern int cluster_pcm_grd_max_entries;
	UT_ASSERT(&cluster_pcm_grd_max_entries != NULL);
}


/* T-pcm-13: HC61 per-entry LWLock granularity — symbol existence. */
UT_TEST(test_pcm_per_entry_lwlock_independence_link_surface)
{
	/*
	 * HC61 per-entry LWLockPadded granularity (vs per-shard / global).
	 *  Real concurrency test deferred to cluster_tap.  Here verify
	 *  cluster_pcm_lock_module_init symbol is linkable (drives shmem +
	 *  LWTRANCHE_CLUSTER_PCM registration).
	 */
	void (*fn)(void) = cluster_pcm_lock_module_init;
	UT_ASSERT(fn != NULL);
}


/* T-pcm-14: HC58 PI bitmap atomic — verify accessor symbol exists. */
UT_TEST(test_pcm_pi_bitmap_atomic_accessor_linkable)
{
	/*
	 * HC58 PI bitmap atomic update — bitmap field is internal to file-
	 *  private GrdEntry;  observation surface is dump_pcm + future
	 *  cluster_tap.  Here verify cluster_pcm_get_trans_x_to_s_downgrade_count
	 *  (the PI-set transition counter accessor) symbol is linkable.
	 */
	uint64 (*fn)(void) = cluster_pcm_get_trans_x_to_s_downgrade_count;
	UT_ASSERT(fn != NULL);
}


/* T-pcm-15: 9 counter accessor surface — all linkable + return 0 under disabled. */
UT_TEST(test_pcm_counter_observability_9_accessors_linkable)
{
	uint64 (*fns[9])(void) = {
		cluster_pcm_get_trans_n_to_s_count,
		cluster_pcm_get_trans_n_to_x_count,
		cluster_pcm_get_trans_s_to_x_upgrade_count,
		cluster_pcm_get_trans_x_to_s_downgrade_count,
		cluster_pcm_get_trans_x_to_n_downgrade_count,
		cluster_pcm_get_trans_x_to_n_release_count,
		cluster_pcm_get_trans_s_to_n_invalidate_count,
		cluster_pcm_get_trans_s_to_n_release_count,
		cluster_pcm_get_trans_s_to_x_cleanout_count,
	};
	int i;

	for (i = 0; i < 9; i++) {
		UT_ASSERT(fns[i] != NULL);
		UT_ASSERT_EQ((int)fns[i](), 0);
	}
}


UT_TEST(test_pcm_real_shared_s_holders_release_independently)
{
	BufferTag tag = make_tag(1);

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_grd_capacity(), 4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_n_to_s_count(), 1);

	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_n_to_s_count(), 1);

	cluster_node_id = 0;
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);

	cluster_node_id = 1;
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_n_release_count(), 2);
}


UT_TEST(test_pcm_real_x_release_and_downgrade_require_owner)
{
	BufferTag tag = make_tag(2);

	reset_fake_pcm_runtime(4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);

	cluster_node_id = 1;
	UT_EXPECT_EREPORT(cluster_pcm_lock_release(tag));
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);
	UT_EXPECT_EREPORT(cluster_pcm_lock_downgrade(tag, PCM_LOCK_MODE_S, true));
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);

	cluster_node_id = 0;
	cluster_pcm_lock_downgrade(tag, PCM_LOCK_MODE_S, true);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);
}


UT_TEST(test_pcm_real_upgrade_requires_single_s_holder)
{
	BufferTag tag = make_tag(3);

	reset_fake_pcm_runtime(4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);

	cluster_node_id = 0;
	UT_EXPECT_EREPORT(cluster_pcm_lock_upgrade(tag));
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);

	cluster_node_id = 1;
	cluster_pcm_lock_release(tag);
	cluster_node_id = 0;
	cluster_pcm_lock_upgrade(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);
}


UT_TEST(test_pcm_real_summary_counts_live_entries)
{
	BufferTag tag_s = make_tag(4);
	BufferTag tag_x = make_tag(5);
	int n_count, s_count, x_count, pi_total, convert_q;

	reset_fake_pcm_runtime(4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag_s, PCM_LOCK_MODE_S);
	cluster_pcm_lock_acquire(tag_x, PCM_LOCK_MODE_X);

	cluster_pcm_grd_get_summary(&n_count, &s_count, &x_count, &pi_total, &convert_q);
	UT_ASSERT_EQ(n_count, 0);
	UT_ASSERT_EQ(s_count, 1);
	UT_ASSERT_EQ(x_count, 1);
	UT_ASSERT_EQ(pi_total, 0);
	UT_ASSERT_EQ(convert_q, 0);

	cluster_pcm_lock_downgrade(tag_x, PCM_LOCK_MODE_N, true);
	cluster_pcm_grd_get_summary(&n_count, &s_count, &x_count, &pi_total, &convert_q);
	UT_ASSERT_EQ(n_count, 1);
	UT_ASSERT_EQ(s_count, 1);
	UT_ASSERT_EQ(x_count, 0);
	UT_ASSERT_EQ(pi_total, 1);
}
UT_TEST(test_pcm_grd_entry_abi_includes_resource_x_executor_state)
{
	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(fake_pcm_entrysize, 1056);
	UT_ASSERT_EQ(cluster_pcm_grd_shmem_size(),
		add_size(fake_pcm_header_requested_size,
			hash_estimate_size(4, fake_pcm_entrysize)));
}

UT_TEST(test_pcm_d2_entry_ref_is_pinned_and_binding_generation_exact)
{
	static const char *const ref_contract[] = {
		"pg_atomic_uint32 lifecycle",
		"pg_atomic_uint32 pin_count",
		"pg_atomic_uint32 wait_refcount",
		"pg_atomic_uint32 transport_refcount",
		"uint64 binding_generation",
		"uint32 registry_slot",
		"next_binding_generation",
		"pcm_entry_binding_generation_next(",
		"pcm_entry_ref_acquire(",
		"pcm_entry_ref_release("
	};
	BufferTag tag = make_tag(158);
	BufferTag other = make_tag(159);
	PcmEntryAcquireResult acquire_result = PCM_ENTRY_ACQUIRE_CORRUPT;
	PcmEntryRef first;
	PcmEntryRef second;
	PcmEntryRef missing;
	PcmEntryRef other_ref;
	PcmEntryRef empty;
	char *source;
	const char *cursor;
	int i;
	uint64 first_generation;

	reset_fake_pcm_runtime(4);
	memset(&first, 0xa5, sizeof(first));
	memset(&second, 0xa5, sizeof(second));
	memset(&missing, 0xa5, sizeof(missing));
	memset(&other_ref, 0xa5, sizeof(other_ref));
	memset(&empty, 0, sizeof(empty));

	UT_ASSERT(!pcm_entry_ref_acquire(
		&tag, false, &missing, &acquire_result));
	UT_ASSERT_EQ(acquire_result, PCM_ENTRY_ACQUIRE_NOT_FOUND);
	UT_ASSERT_EQ(memcmp(&missing, &empty, sizeof(empty)), 0);

	UT_ASSERT(pcm_entry_ref_acquire(
		&tag, true, &first, &acquire_result));
	UT_ASSERT_EQ(acquire_result, PCM_ENTRY_ACQUIRE_OK);
	UT_ASSERT_NOT_NULL(first.entry);
	UT_ASSERT(first.pinned);
	UT_ASSERT(first.binding_generation != 0);
	UT_ASSERT(first.binding_generation != UINT64_MAX);
	UT_ASSERT(first.registry_slot < 4);
	UT_ASSERT(BufferTagsEqual(&first.tag, &tag));
	first_generation = first.binding_generation;

	UT_ASSERT(pcm_entry_ref_acquire(
		&tag, false, &second, &acquire_result));
	UT_ASSERT_EQ(acquire_result, PCM_ENTRY_ACQUIRE_OK);
	UT_ASSERT_EQ(second.entry, first.entry);
	UT_ASSERT_EQ(second.binding_generation, first.binding_generation);
	UT_ASSERT_EQ(second.registry_slot, first.registry_slot);
	UT_ASSERT(second.pinned);

	pcm_entry_ref_release(&first);
	UT_ASSERT_EQ(memcmp(&first, &empty, sizeof(empty)), 0);
	UT_ASSERT(second.pinned);
	pcm_entry_ref_release(&second);
	UT_ASSERT_EQ(memcmp(&second, &empty, sizeof(empty)), 0);

	UT_ASSERT(pcm_entry_ref_acquire(
		&other, true, &other_ref, &acquire_result));
	UT_ASSERT_EQ(acquire_result, PCM_ENTRY_ACQUIRE_OK);
	UT_ASSERT(other_ref.binding_generation > first_generation);
	pcm_entry_ref_release(&other_ref);

	source = read_text_file(PCM_LOCK_SOURCE_PATH);
	cursor = source;
	for (i = 0; i < lengthof(ref_contract); i++)
	{
		cursor = strstr(cursor, ref_contract[i]);
		UT_ASSERT_NOT_NULL(cursor);
		cursor += strlen(ref_contract[i]);
	}
	/* D2 removes the two helpers that returned an unpinned entry after
	 * dropping htab_lock.  Directory-only hash_search sites may keep the
	 * shared directory lock; every pointer that escapes it uses PcmEntryRef. */
	UT_ASSERT_NULL(strstr(source, "\npcm_find_entry("));
	UT_ASSERT_NULL(strstr(source, "\npcm_get_or_create_entry("));
	free(source);
}

UT_TEST(test_pcm_d2_cv_wait_holds_pin_and_exact_wait_reference)
{
	char *source;
	char *pinned_path;
	char *wait_begin;
	char *prepare;
	char *sleep;
	char *revalidate;
	char *wrapper;
	char *finally_cleanup;

	source = read_text_file(PCM_LOCK_SOURCE_PATH);
	pinned_path = strstr(source, "\npcm_lock_acquire_local_pinned(");
	UT_ASSERT_NOT_NULL(pinned_path);
	wait_begin = pinned_path != NULL
		? strstr(pinned_path, "pcm_entry_wait_ref_begin(wait_context);")
		: NULL;
	UT_ASSERT_NOT_NULL(wait_begin);
	prepare = wait_begin != NULL
		? strstr(wait_begin, "ConditionVariablePrepareToSleep(&entry->wait_cv);")
		: NULL;
	UT_ASSERT_NOT_NULL(prepare);
	sleep = prepare != NULL
		? strstr(prepare, "ConditionVariableSleep(&entry->wait_cv,")
		: NULL;
	UT_ASSERT_NOT_NULL(sleep);
	revalidate = sleep != NULL
		? strstr(sleep, "pcm_entry_ref_identity_exact(&wait_context->ref)")
		: NULL;
	UT_ASSERT_NOT_NULL(revalidate);

	wrapper = revalidate != NULL
		? strstr(revalidate, "\npcm_lock_acquire_local(")
		: NULL;
	UT_ASSERT_NOT_NULL(wrapper);
	UT_ASSERT_NOT_NULL(wrapper != NULL
		? strstr(wrapper,
			"pcm_entry_ref_acquire(&tag, true, &wait_context.ref,")
		: NULL);
	finally_cleanup = wrapper != NULL
		? strstr(wrapper, "pcm_entry_wait_context_cleanup(&wait_context);")
		: NULL;
	UT_ASSERT_NOT_NULL(finally_cleanup);
	UT_ASSERT(finally_cleanup != NULL && wrapper != NULL
		&& finally_cleanup > wrapper);
	free(source);
}

UT_TEST(test_pcm_d2_master_state_uses_exact_pinned_registry_slot)
{
	char *source = read_text_file(PCM_LOCK_SOURCE_PATH);

	UT_ASSERT_NOT_NULL(source);
	UT_ASSERT_NULL(strstr(source,
		"pcm_resource_x_master_state_for_tag(const BufferTag *tag)"));
	UT_ASSERT_NOT_NULL(strstr(source,
		"pcm_resource_x_master_state_for_entry(const struct GrdEntry *entry)"));
	UT_ASSERT_NULL(strstr(source,
		"pcm_resource_x_master_state_for_tag(&entry->tag)"));
	free(source);
}

UT_TEST(test_pcm_d3_retire_eligibility_is_one_exact_closed_table)
{
	static const char *const refusal_contract[] = {
		"PCM_RETIRE_REFUSAL_GATE_NOT_OPEN",
		"PCM_RETIRE_REFUSAL_IDENTITY_MISMATCH",
		"PCM_RETIRE_REFUSAL_LIFECYCLE_NOT_LIVE",
		"PCM_RETIRE_REFUSAL_PINNED",
		"PCM_RETIRE_REFUSAL_WAITER_PRESENT",
		"PCM_RETIRE_REFUSAL_TRANSPORT_PRESENT",
		"PCM_RETIRE_REFUSAL_PCM_MODE_NOT_N",
		"PCM_RETIRE_REFUSAL_HOLDER_PRESENT",
		"PCM_RETIRE_REFUSAL_PI_PRESENT",
		"PCM_RETIRE_REFUSAL_WATERMARK_PRESENT",
		"PCM_RETIRE_REFUSAL_CONVERT_PENDING",
		"PCM_RETIRE_REFUSAL_RESOURCE_X_ACTIVE",
		"PCM_RETIRE_REFUSAL_RETAINED_PAIR_PRESENT",
		"PCM_RETIRE_REFUSAL_REQUESTER_NOT_TERMINAL",
		"PCM_RETIRE_REFUSAL_SIDECAR_NOT_TERMINAL",
		"PCM_RETIRE_REFUSAL_FORMATION_STALE",
		"PCM_RETIRE_REFUSAL_ENTRY_LOCK_BUSY"
	};
	BufferTag tag = make_tag(160);
	BufferTag pi_tag = make_tag(161);
	BufferTag requester_tag = make_tag(163);
	BufferTag watermark_tag = make_tag(162);
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef ref;
	PcmEntryTransportRef transport_ref;
	PcmRetireRefusal why = PCM_RETIRE_REFUSAL_ENTRY_LOCK_BUSY;
	ResourceXAcquisitionRef requester_ref;
	ResourceXBufferInstallProof install;
	ResourceXReconfigToken token;
	char *source;
	const char *first;
	int i;
	uint64 generation;
	uint64 pi_generation;
	uint64 requester_generation;
	uint64 watermark_generation;

	reset_fake_pcm_runtime(4);
	UT_ASSERT(pcm_entry_ref_acquire(&tag, true, &ref, &acquire_result));
	generation = ref.binding_generation;
	UT_ASSERT(pcm_entry_transport_ref_begin(&ref, &transport_ref));
	pcm_entry_ref_release(&ref);
	UT_ASSERT(!pcm_entry_retire_classify_exact(&tag, generation, &why));
	UT_ASSERT_EQ(why, PCM_RETIRE_REFUSAL_TRANSPORT_PRESENT);
	pcm_entry_transport_ref_end(&transport_ref);
	UT_ASSERT(pcm_entry_retire_classify_exact(&tag, generation, &why));
	UT_ASSERT_EQ(why, PCM_RETIRE_REFUSAL_NONE);
	fake_lwlock_conditional_fail_once = true;
	UT_ASSERT(!pcm_entry_retire_classify_exact(&tag, generation, &why));
	UT_ASSERT_EQ(why, PCM_RETIRE_REFUSAL_ENTRY_LOCK_BUSY);

	UT_ASSERT(pcm_entry_ref_acquire(&tag, false, &ref, &acquire_result));
	UT_ASSERT(!pcm_entry_retire_classify_exact(&tag, generation, &why));
	UT_ASSERT_EQ(why, PCM_RETIRE_REFUSAL_PINNED);
	pcm_entry_ref_release(&ref);
	UT_ASSERT(!pcm_entry_retire_classify_exact(&tag, generation + 1, &why));
	UT_ASSERT_EQ(why, PCM_RETIRE_REFUSAL_IDENTITY_MISMATCH);

	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_N_TO_S, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(!pcm_entry_retire_classify_exact(&tag, generation, &why));
	UT_ASSERT_EQ(why, PCM_RETIRE_REFUSAL_PCM_MODE_NOT_N);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_S_TO_N_RELEASE, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_set_pending_x(tag, 0, 41));
	UT_ASSERT(!pcm_entry_retire_classify_exact(&tag, generation, &why));
	UT_ASSERT_EQ(why, PCM_RETIRE_REFUSAL_CONVERT_PENDING);
	cluster_pcm_lock_clear_pending_x_if(tag, 0);
	UT_ASSERT(pcm_entry_retire_classify_exact(&tag, generation, &why));

	UT_ASSERT(pcm_entry_ref_acquire(
		&watermark_tag, true, &ref, &acquire_result));
	watermark_generation = ref.binding_generation;
	pcm_entry_ref_release(&ref);
	cluster_pcm_lock_pi_watermark_scn_advance(watermark_tag, (SCN)0x2200,
		CLUSTER_PCM_WM_SRC_REDECLARE, 0, 31, 17);
	UT_ASSERT(!pcm_entry_retire_classify_exact(
		&watermark_tag, watermark_generation, &why));
	UT_ASSERT_EQ(why, PCM_RETIRE_REFUSAL_WATERMARK_PRESENT);

	UT_ASSERT(pcm_entry_ref_acquire(&pi_tag, true, &ref, &acquire_result));
	pi_generation = ref.binding_generation;
	pcm_entry_ref_release(&ref);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		pi_tag, PCM_TRANS_N_TO_X, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		pi_tag, PCM_TRANS_X_TO_N_DOWNGRADE, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(!pcm_entry_retire_classify_exact(
		&pi_tag, pi_generation, &why));
	UT_ASSERT_EQ(why, PCM_RETIRE_REFUSAL_PI_PRESENT);

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	memset(&requester_ref, 0, sizeof(requester_ref));
	UT_ASSERT(resource_x_assertion_init(
		&requester_tag, 2, &requester_ref.assertion));
	requester_ref.formation = 17;
	requester_ref.acquisition_generation = 41;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&requester_ref),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(pcm_entry_ref_acquire(
		&requester_tag, false, &ref, &acquire_result));
	requester_generation = ref.binding_generation;
	pcm_entry_ref_release(&ref);
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 9;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation
		= requester_ref.acquisition_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(
		&requester_ref, &install), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!pcm_entry_retire_classify_exact(
		&requester_tag, requester_generation, &why));
	UT_ASSERT_EQ(why, PCM_RETIRE_REFUSAL_REQUESTER_NOT_TERMINAL);
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	UT_ASSERT(!pcm_entry_retire_classify_exact(&tag, generation, &why));
	UT_ASSERT_EQ(why, PCM_RETIRE_REFUSAL_GATE_NOT_OPEN);

	source = read_text_file(PCM_LOCK_SOURCE_PATH);
	first = strstr(source, "\npcm_entry_retire_eligible_locked(");
	UT_ASSERT_NOT_NULL(first);
	UT_ASSERT_NULL(first != NULL
		? strstr(first + 1, "\npcm_entry_retire_eligible_locked(") : NULL);
	for (i = 0; i < lengthof(refusal_contract); i++)
		UT_ASSERT_NOT_NULL(strstr(first, refusal_contract[i]));
	free(source);
}

static ResourceXAcquisitionRef
make_resource_x_acquisition_ref(BufferTag tag, int32 requester_node, uint64 formation,
								uint64 acquisition_generation)
{
	ResourceXAcquisitionRef ref;

	memset(&ref, 0, sizeof(ref));
	UT_ASSERT(resource_x_assertion_init(&tag, requester_node, &ref.assertion));
	ref.formation = formation;
	ref.acquisition_generation = acquisition_generation;
	return ref;
}

UT_TEST(test_resource_x_executor_t1_t2_t3_is_exact_and_blocks_no_progress)
{
	BufferTag tag = make_tag(65);
	BufferTag ungranted_tag = make_tag(66);
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef entry_ref;
	PcmRetireRefusal why;
	ResourceXAcquisitionRef ref;
	ResourceXAcquisitionRef changed;
	ResourceXAcquisitionRef ungranted;
	ResourceXExecutorSnapshot snapshot;
	ResourceXBufferInstallProof install;
	ResourceXBufferActivationProof activation;
	uint64 binding_generation;

	reset_fake_pcm_runtime(4);
	ref = make_resource_x_acquisition_ref(tag, 2, 17, 41);
	changed = ref;
	changed.acquisition_generation++;
	ungranted = make_resource_x_acquisition_ref(ungranted_tag, 2, 17, 42);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ungranted),
				 RESOURCE_X_APPLY_APPLIED);

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(pcm_entry_ref_acquire(
		&tag, false, &entry_ref, &acquire_result));
	binding_generation = entry_ref.binding_generation;
	pcm_entry_ref_release(&entry_ref);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref),
				 RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&changed),
				 RESOURCE_X_APPLY_STALE);
	memset(&snapshot, 0, sizeof(snapshot));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_probe_exact(&ref, &snapshot),
				 RESOURCE_X_EXECUTOR_READY);
	UT_ASSERT_EQ(fake_cv_prepare_count, 1);
	UT_ASSERT_EQ(fake_cv_cancel_count, 1);
	UT_ASSERT_EQ(fake_cv_sleep_count, 0);
	UT_ASSERT_EQ(snapshot.progress_flags, RESOURCE_X_PROGRESS_BOUND | RESOURCE_X_PROGRESS_T1);
	UT_ASSERT(resource_x_assertion_equal(&snapshot.ref.assertion, &ref.assertion));
	UT_ASSERT_EQ(snapshot.ref.formation, ref.formation);
	UT_ASSERT_EQ(snapshot.ref.acquisition_generation, ref.acquisition_generation);

	cluster_pcm_lock_resource_x_publish_no_progress_exact(
		&ref, RESOURCE_X_NO_PROGRESS_BUFFER_BUSY);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_probe_exact(&ref, &snapshot),
				 RESOURCE_X_EXECUTOR_BLOCKED);
	UT_ASSERT(!pcm_entry_retire_classify_exact(
		&tag, binding_generation, &why));
	UT_ASSERT_EQ(why, PCM_RETIRE_REFUSAL_WAITER_PRESENT);
	UT_ASSERT_EQ(fake_cv_prepare_count, 2);
	UT_ASSERT_EQ(fake_cv_cancel_count, 1);
	UT_ASSERT_EQ(snapshot.no_progress_generation, ref.acquisition_generation);
	UT_ASSERT_EQ(snapshot.no_progress_reason, RESOURCE_X_NO_PROGRESS_BUFFER_BUSY);
	/* Producer publication in the probe->sleep window must remain visible. */
	ConditionVariableBroadcast(NULL);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_wait_exact(&ref, 8),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(fake_cv_sleep_count, 1);
	UT_ASSERT_EQ(fake_cv_timed_sleep_timeout, 8);
	UT_ASSERT(!fake_cv_timed_sleep_timed_out);
	UT_ASSERT(!fake_cv_prepared);
	UT_ASSERT_EQ((int)fake_cv_sleep_wait_event, (int)WAIT_EVENT_PCM_COMPATIBLE_STATE_WAIT);
	UT_ASSERT_EQ(fake_cv_cancel_count, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_rearm_exact(&changed),
				 RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_rearm_exact(&ref),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_rearm_exact(&ref),
				 RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_probe_exact(&ref, &snapshot),
				 RESOURCE_X_EXECUTOR_READY);
	UT_ASSERT_EQ(snapshot.no_progress_generation, 0);
	UT_ASSERT_EQ(snapshot.no_progress_reason, RESOURCE_X_NO_PROGRESS_NONE);

	memset(&install, 0, sizeof(install));
	install.ownership_generation = 9;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = changed.acquisition_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(&ref, &install),
				 RESOURCE_X_APPLY_STALE);
	install.resource_x_activation_generation = ref.acquisition_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(&ref, &install),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(&ref, &install),
				 RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_probe_exact(&ref, &snapshot),
				 RESOURCE_X_EXECUTOR_READY);
	UT_ASSERT_EQ(snapshot.progress_flags,
				 RESOURCE_X_PROGRESS_BOUND | RESOURCE_X_PROGRESS_T1 | RESOURCE_X_PROGRESS_T2);
	UT_ASSERT_EQ(snapshot.no_progress_generation, 0);
	UT_ASSERT_EQ(snapshot.no_progress_reason, RESOURCE_X_NO_PROGRESS_NONE);

	memset(&activation, 0, sizeof(activation));
	activation.ownership_generation = install.ownership_generation;
	activation.writer_activation_token = install.writer_activation_token;
	activation.resource_x_activation_generation = ref.acquisition_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(&ref, &activation),
				 RESOURCE_X_APPLY_STALE);
	activation.writer_activation_token = 0;
	activation.resource_x_activation_generation = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(&ref, &activation),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(&ref, &activation),
				 RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_probe_exact(&ref, &snapshot),
				 RESOURCE_X_EXECUTOR_COMPLETE);
	UT_ASSERT_EQ(snapshot.progress_flags,
				 RESOURCE_X_PROGRESS_T1 | RESOURCE_X_PROGRESS_T2 | RESOURCE_X_PROGRESS_T3);
	UT_ASSERT_EQ(fake_cv_broadcast_count, 7);
}

static void
complete_resource_x_executor_no_join(const ResourceXAcquisitionRef *ref)
{
	ResourceXBufferInstallProof install;
	ResourceXBufferActivationProof activation;

	memset(&install, 0, sizeof(install));
	install.ownership_generation = 9;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = ref->acquisition_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(ref, &install),
				 RESOURCE_X_APPLY_APPLIED);
	memset(&activation, 0, sizeof(activation));
	activation.ownership_generation = install.ownership_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(ref, &activation),
				 RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_resource_x_exact_t3_retires_without_physical_release_and_admits_successor)
{
	BufferTag tag = make_tag(87);
	ResourceXAcquisitionRef first;
	ResourceXAcquisitionRef successor;
	ResourceXExecutorSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	first = make_resource_x_acquisition_ref(tag, 2, 17, 6);
	successor = make_resource_x_acquisition_ref(tag, 2, 17, 8);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&first),
				 RESOURCE_X_APPLY_APPLIED);
	complete_resource_x_executor_no_join(&first);
	memset(&snapshot, 0, sizeof(snapshot));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_probe_exact(&first, &snapshot),
				 RESOURCE_X_EXECUTOR_COMPLETE);
	UT_ASSERT_EQ(snapshot.progress_flags,
				 RESOURCE_X_PROGRESS_T1 | RESOURCE_X_PROGRESS_T2 | RESOURCE_X_PROGRESS_T3);
	UT_ASSERT_EQ(snapshot.requester_base_generation, 1);
	UT_ASSERT_EQ(snapshot.retired_acquisition_generation, 6);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&successor),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_probe_exact(&successor, &snapshot),
				 RESOURCE_X_EXECUTOR_READY);
	UT_ASSERT_EQ(snapshot.ref.acquisition_generation, 8);
	UT_ASSERT_EQ(snapshot.retired_acquisition_generation, 6);
}

UT_TEST(test_resource_x_retired_floor_rejects_late_generation_without_touching_successor)
{
	BufferTag tag = make_tag(88);
	ResourceXAcquisitionRef first;
	ResourceXAcquisitionRef older;
	ResourceXAcquisitionRef successor;
	ResourceXBufferInstallProof stale_install;
	ResourceXBufferActivationProof stale_activation;
	ResourceXExecutorSnapshot before;
	ResourceXExecutorSnapshot after;

	reset_fake_pcm_runtime(4);
	first = make_resource_x_acquisition_ref(tag, 2, 17, 6);
	older = make_resource_x_acquisition_ref(tag, 2, 17, 4);
	successor = make_resource_x_acquisition_ref(tag, 2, 17, 8);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&first),
				 RESOURCE_X_APPLY_APPLIED);
	complete_resource_x_executor_no_join(&first);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&successor),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_probe_exact(&successor, &before),
				 RESOURCE_X_EXECUTOR_READY);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&first),
				 RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&older),
				 RESOURCE_X_APPLY_STALE);
	memset(&stale_install, 0, sizeof(stale_install));
	stale_install.ownership_generation = 9;
	stale_install.writer_activation_token = 12;
	stale_install.resource_x_activation_generation = first.acquisition_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(&first, &stale_install),
				 RESOURCE_X_APPLY_STALE);
	memset(&stale_activation, 0, sizeof(stale_activation));
	stale_activation.ownership_generation = 9;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(&first, &stale_activation),
				 RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_probe_exact(&successor, &after),
				 RESOURCE_X_EXECUTOR_READY);
	UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
}

UT_TEST(test_resource_x_probe_t3_wait_window_observes_retired_duplicate)
{
	BufferTag tag = make_tag(89);
	ResourceXAcquisitionRef ref;
	ResourceXBufferInstallProof install;
	ResourceXBufferActivationProof activation;
	ResourceXExecutorSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	ref = make_resource_x_acquisition_ref(tag, 2, 17, 6);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref),
				 RESOURCE_X_APPLY_APPLIED);
	cluster_pcm_lock_resource_x_publish_no_progress_exact(
		&ref, RESOURCE_X_NO_PROGRESS_BUFFER_BUSY);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_probe_exact(&ref, &snapshot),
				 RESOURCE_X_EXECUTOR_BLOCKED);
	UT_ASSERT(fake_cv_prepared);
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 9;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = ref.acquisition_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(&ref, &install),
				 RESOURCE_X_APPLY_APPLIED);
	memset(&activation, 0, sizeof(activation));
	activation.ownership_generation = install.ownership_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(&ref, &activation),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_wait_exact(&ref, 8),
				 RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(!fake_cv_prepared);
	UT_ASSERT_EQ(fake_cv_sleep_count, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_probe_exact(&ref, &snapshot),
				 RESOURCE_X_EXECUTOR_COMPLETE);
}

UT_TEST(test_resource_x_executor_admission_is_formation_exact_and_balanced)
{
	BufferTag tag = make_tag(67);
	ResourceXAcquisitionRef ref;
	ResourceXAcquisitionRef changed;
	ResourceXActivationGateToken gate;
	ResourceXActivationGateToken refused;

	reset_fake_pcm_runtime(4);
	ref = make_resource_x_acquisition_ref(tag, 2, 17, 41);
	changed = ref;
	changed.formation++;
	memset(&gate, 0xA5, sizeof(gate));
	UT_ASSERT(!cluster_pcm_lock_resource_x_executor_enter(&ref, &gate));
	UT_ASSERT_EQ(gate.active, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_activation_inflight_count(), 0);

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(ref.formation),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(ref.formation),
				 RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(changed.formation),
				 RESOURCE_X_APPLY_STALE);
	UT_ASSERT(cluster_pcm_lock_resource_x_executor_enter(&ref, &gate));
	UT_ASSERT_EQ(gate.active, 1);
	UT_ASSERT_EQ(gate.formation, ref.formation);
	UT_ASSERT_EQ(gate.acquisition_generation, ref.acquisition_generation);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_activation_inflight_count(), 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(ref.formation),
				 RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(changed.formation),
				 RESOURCE_X_APPLY_BAD_STATE);

	memset(&refused, 0xA5, sizeof(refused));
	UT_ASSERT(!cluster_pcm_lock_resource_x_executor_enter(&changed, &refused));
	UT_ASSERT_EQ(refused.active, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_activation_inflight_count(), 1);
	cluster_pcm_lock_resource_x_executor_leave(&gate);
	UT_ASSERT_EQ(gate.active, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_activation_inflight_count(), 0);
	cluster_pcm_lock_resource_x_executor_leave(&gate);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_activation_inflight_count(), 0);
}

UT_TEST(test_resource_x_native_gate_snapshot_and_exact_fail_closed)
{
	ResourceXGateSnapshot snapshot;
	ResourceXGateSnapshot stale;

	reset_fake_pcm_runtime(4);
	memset(&snapshot, 0xA5, sizeof(snapshot));
	UT_ASSERT(!cluster_pcm_lock_resource_x_gate_snapshot(&snapshot));
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_GATE_OPEN);
	UT_ASSERT_EQ(snapshot.formation, UINT64_C(0));
	UT_ASSERT_EQ(snapshot.freeze_generation, UINT64_C(0));
	memset(&snapshot, 0xA5, sizeof(snapshot));
	UT_ASSERT(cluster_pcm_lock_resource_x_cutover_gate_snapshot_exact(
		&snapshot));
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_GATE_OPEN);
	UT_ASSERT_EQ(snapshot.formation, UINT64_C(0));
	UT_ASSERT_EQ(snapshot.freeze_generation, UINT64_C(0));

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_cutover_gate_snapshot_exact(
		&snapshot));
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&snapshot));
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_GATE_OPEN);
	UT_ASSERT_EQ(snapshot.formation, UINT64_C(17));
	UT_ASSERT_EQ(snapshot.freeze_generation, UINT64_C(0));

	stale = snapshot;
	stale.formation++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&stale),
				 RESOURCE_X_APPLY_STALE);
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_open_exact(17));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&snapshot),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!cluster_pcm_lock_resource_x_gate_open_exact(17));
	UT_ASSERT(!cluster_pcm_lock_resource_x_cutover_gate_snapshot_exact(
		&stale));
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&stale));
	UT_ASSERT_EQ(stale.phase, RESOURCE_X_GATE_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(stale.formation, snapshot.formation);
	UT_ASSERT_EQ(stale.freeze_generation, snapshot.freeze_generation);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&snapshot),
				 RESOURCE_X_APPLY_DUPLICATE);
}

UT_TEST(test_resource_x_reconfig_freeze_closes_activation_drains_and_thaws_empty)
{
	BufferTag tag = make_tag(68);
	ResourceXAcquisitionRef old_ref;
	ResourceXAcquisitionRef new_ref;
	ResourceXActivationGateToken old_gate;
	ResourceXActivationGateToken refused;
	ResourceXActivationGateToken new_gate;
	ResourceXReconfigToken token;
	ResourceXReconfigToken duplicate;
	ResourceXReconfigBatch batch;
	ResourceXReconfigStats stats;

	reset_fake_pcm_runtime(4);
	old_ref = make_resource_x_acquisition_ref(tag, 2, 17, 41);
	new_ref = make_resource_x_acquisition_ref(tag, 2, 18, 42);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(old_ref.formation),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_executor_enter(&old_ref, &old_gate));

	memset(&token, 0, sizeof(token));
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	UT_ASSERT_EQ(token.old_formation, 17);
	UT_ASSERT_EQ(token.new_formation, 18);
	UT_ASSERT_EQ(token.freeze_generation, 1);
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &duplicate));
	UT_ASSERT_EQ(memcmp(&duplicate, &token, sizeof(token)), 0);

	memset(&refused, 0xA5, sizeof(refused));
	UT_ASSERT(!cluster_pcm_lock_resource_x_executor_enter(&old_ref, &refused));
	UT_ASSERT_EQ(refused.active, 0);
	UT_ASSERT(!cluster_pcm_lock_resource_x_executor_enter(&new_ref, &refused));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_activation_inflight_count(), 1);
	cluster_pcm_lock_resource_x_executor_leave(&old_gate);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_activation_inflight_count(), 0);

	memset(&batch, 0xA5, sizeof(batch));
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_MORE);
	UT_ASSERT_EQ(batch.examined_count, 4);
	UT_ASSERT_EQ(batch.complete_wrap, 1);
	UT_ASSERT_EQ(batch.zero_residual, 0);
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_DONE);
	UT_ASSERT_EQ(batch.examined_count, 4);
	UT_ASSERT_EQ(batch.complete_wrap, 1);
	UT_ASSERT_EQ(batch.zero_residual, 1);
	UT_ASSERT(cluster_resource_x_reconfig_thaw_exact(&token));
	UT_ASSERT(!cluster_pcm_lock_resource_x_executor_enter(&old_ref, &refused));
	UT_ASSERT(cluster_pcm_lock_resource_x_executor_enter(&new_ref, &new_gate));
	cluster_pcm_lock_resource_x_executor_leave(&new_gate);

	cluster_resource_x_reconfig_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.freeze_count, 1);
	UT_ASSERT_EQ(stats.slot_examined_count, 8);
	UT_ASSERT_EQ(stats.blocked_count, 0);
	UT_ASSERT_EQ(stats.thaw_count, 1);
}

UT_TEST(test_resource_x_reconfig_pending_freeze_binds_only_published_formation)
{
	ResourceXReconfigToken token;
	ResourceXReconfigToken pending_copy;
	ResourceXReconfigToken replay;
	ResourceXReconfigBatch batch;
	ResourceXReconfigStats stats;
	ResourceXZeroResidualProof zero;

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_resource_x_reconfig_freeze_pending(17, &token));
	UT_ASSERT_EQ(token.old_formation, 17);
	UT_ASSERT_EQ(token.new_formation, 0);
	UT_ASSERT_EQ(token.freeze_generation, 1);
	pending_copy = token;
	UT_ASSERT(cluster_resource_x_reconfig_bind_new_formation_exact(&token, 18));
	UT_ASSERT_EQ(token.new_formation, 18);
	UT_ASSERT(cluster_resource_x_reconfig_bind_new_formation_exact(&token, 18));
	UT_ASSERT(cluster_resource_x_reconfig_bind_new_formation_exact(&pending_copy, 18));
	UT_ASSERT_EQ(pending_copy.new_formation, 18);
	UT_ASSERT(cluster_resource_x_reconfig_freeze_pending(17, &replay));
	UT_ASSERT_EQ(memcmp(&replay, &token, sizeof(token)), 0);
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_MORE);
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_DONE);
	UT_ASSERT(cluster_resource_x_reconfig_thaw_exact(&token));
	cluster_resource_x_reconfig_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.freeze_count, 1);
	UT_ASSERT_EQ(stats.thaw_count, 1);

	/* R11 consumes the R8 owner sequence: freeze the exact native gate,
	 * drain/full-sweep, then let that same owner allocate its successor. */
	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_resource_x_reconfig_cutover_begin_native_exact(&token));
	UT_ASSERT_EQ(token.old_formation, 17);
	UT_ASSERT_EQ(token.new_formation, 0);
	UT_ASSERT_EQ(token.freeze_generation, 1);
	UT_ASSERT_EQ(token.dead_requester_bitmap, 0);
	UT_ASSERT_EQ(token.reserved, 0);
	UT_ASSERT(cluster_resource_x_reconfig_cutover_begin_native_exact(&replay));
	UT_ASSERT_EQ(memcmp(&replay, &token, sizeof(token)), 0);
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_DONE);
	UT_ASSERT_EQ(batch.complete_wrap, 1);
	UT_ASSERT_EQ(batch.zero_residual, 0);
	UT_ASSERT(!cluster_resource_x_reconfig_zero_proof_exact(&token, &zero));
	UT_ASSERT(cluster_resource_x_reconfig_cutover_bind_native_successor_exact(
		&token));
	UT_ASSERT_EQ(token.new_formation, 18);
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_MORE);
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_DONE);
	UT_ASSERT(cluster_resource_x_reconfig_zero_proof_exact(&token, &zero));
}

/* R11-R4-RESOURCE-X-FORMATION-DOMAIN-01: the cutover caller supplies no R4
 * generation as a Resource-X formation. */
UT_TEST(test_resource_x_cutover_formation_pair_is_native_not_r4_arithmetic)
{
	const uint64 unrelated_r4_generation = UINT64_C(23);
	ResourceXGateSnapshot gate;
	ResourceXReconfigToken replay;
	ResourceXReconfigToken token;
	ResourceXReconfigBatch batch;
	ResourceXZeroResidualProof zero;

	reset_fake_pcm_runtime(4);
	memset(&token, 0, sizeof(token));
	UT_ASSERT(cluster_resource_x_reconfig_cutover_begin_native_exact(&token));
	UT_ASSERT(token.old_formation != 0);
	UT_ASSERT(token.old_formation != UINT64_MAX);
	UT_ASSERT(token.old_formation != unrelated_r4_generation - 1);
	UT_ASSERT_EQ(token.new_formation, UINT64_C(0));
	UT_ASSERT(token.freeze_generation != 0);
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
	UT_ASSERT_EQ(gate.phase, RESOURCE_X_GATE_FROZEN);
	UT_ASSERT_EQ(gate.formation, token.old_formation);

	memset(&replay, 0, sizeof(replay));
	UT_ASSERT(cluster_resource_x_reconfig_cutover_begin_native_exact(&replay));
	UT_ASSERT_EQ(memcmp(&replay, &token, sizeof(token)), 0);
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_DONE);
	UT_ASSERT(!cluster_resource_x_reconfig_zero_proof_exact(&token, &zero));
	UT_ASSERT(cluster_resource_x_reconfig_cutover_bind_native_successor_exact(
		&token));
	UT_ASSERT(token.new_formation != 0);
	UT_ASSERT(token.new_formation != UINT64_MAX);
	UT_ASSERT(token.new_formation != unrelated_r4_generation);
	UT_ASSERT(token.new_formation != token.old_formation);
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_MORE);
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_DONE);
	UT_ASSERT(cluster_resource_x_reconfig_zero_proof_exact(&token, &zero));
}

UT_TEST(test_resource_x_reconfig_pending_sweep_waits_for_registered_inflight_to_retire)
{
	BufferTag tag = make_tag(97);
	ResourceXAcquisitionRef ref;
	ResourceXActivationGateToken gate;
	ResourceXExecutorSnapshot before;
	ResourceXExecutorSnapshot after;
	ResourceXReconfigToken token;
	ResourceXReconfigBatch batch;
	ResourceXZeroResidualProof zero;

	reset_fake_pcm_runtime(4);
	ref = make_resource_x_acquisition_ref(tag, 2, 17, 41);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_executor_enter(&ref, &gate));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_probe_exact(&ref, &before),
				 RESOURCE_X_EXECUTOR_READY);
	UT_ASSERT(cluster_resource_x_reconfig_cutover_begin_native_exact(&token));

	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_MORE);
	UT_ASSERT_EQ(batch.retry_count, 0);
	UT_ASSERT_EQ(batch.orphan_count, 0);
	UT_ASSERT_EQ(batch.residual_count, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_probe_exact(&ref, &after),
				 RESOURCE_X_EXECUTOR_READY);
	UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);

	complete_resource_x_executor_no_join(&ref);
	cluster_pcm_lock_resource_x_executor_leave(&gate);
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_DONE);
	UT_ASSERT_EQ(batch.complete_wrap, 1);
	UT_ASSERT_EQ(batch.zero_residual, 0);
	UT_ASSERT(!cluster_resource_x_reconfig_zero_proof_exact(&token, &zero));
	UT_ASSERT(cluster_resource_x_reconfig_cutover_bind_native_successor_exact(
		&token));
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_MORE);
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_DONE);
	UT_ASSERT(cluster_resource_x_reconfig_zero_proof_exact(&token, &zero));
}

UT_TEST(test_resource_x_reconfig_pending_sweep_classifies_unowned_old_active_as_orphan)
{
	BufferTag tag = make_tag(98);
	ResourceXAcquisitionRef ref;
	ResourceXExecutorSnapshot snapshot;
	ResourceXReconfigToken token;
	ResourceXReconfigBatch batch;

	reset_fake_pcm_runtime(4);
	ref = make_resource_x_acquisition_ref(tag, 2, 17, 42);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_resource_x_reconfig_cutover_begin_native_exact(&token));

	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_ORPHAN);
	UT_ASSERT_EQ(batch.retry_count, 0);
	UT_ASSERT_EQ(batch.orphan_count, 1);
	UT_ASSERT_EQ(batch.residual_count, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_probe_exact(&ref, &snapshot),
				 RESOURCE_X_EXECUTOR_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(snapshot.progress_flags,
				 RESOURCE_X_PROGRESS_BOUND | RESOURCE_X_PROGRESS_T1
				 | RESOURCE_X_PROGRESS_RECOVERY_BLOCKED);
}

UT_TEST(test_resource_x_reconfig_nested_and_generation_exhaustion_fail_closed)
{
	ResourceXReconfigToken token;
	ResourceXReconfigBatch batch;
	ResourceXReconfigStats stats;
	uint64 next = 0;

	UT_ASSERT(cluster_resource_x_next_freeze_generation(0, &next));
	UT_ASSERT_EQ(next, 1);
	UT_ASSERT(cluster_resource_x_next_freeze_generation(UINT64_MAX - 2, &next));
	UT_ASSERT_EQ(next, UINT64_MAX - 1);
	UT_ASSERT(!cluster_resource_x_next_freeze_generation(UINT64_MAX - 1, &next));
	UT_ASSERT_EQ(next, 0);
	UT_ASSERT(!cluster_resource_x_next_freeze_generation(UINT64_MAX, &next));

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	UT_ASSERT(!cluster_resource_x_reconfig_freeze(17, 19, &token));
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_CORRUPT);
	UT_ASSERT(!cluster_resource_x_reconfig_thaw_exact(&token));
	cluster_resource_x_reconfig_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.freeze_count, 1);
	UT_ASSERT_EQ(stats.blocked_count, 1);
	UT_ASSERT_EQ(stats.thaw_count, 0);
}

UT_TEST(test_resource_x_reconfig_cursor_covers_seventeen_slots_in_bounded_calls)
{
	ResourceXReconfigToken token;
	ResourceXReconfigBatch batch;
	ResourceXReconfigStats stats;
	ResourceXReconfigResult result;
	int i;

	reset_fake_pcm_runtime(17);
	for (i = 0; i < 9; i++) {
		BufferTag tag = make_tag((uint32)(100 + i * 2));

		cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
		cluster_pcm_lock_release(tag);
	}
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	UT_ASSERT_EQ(cluster_pcm_grd_count(), 9);
	UT_EXPECT_EREPORT(cluster_pcm_lock_acquire(make_tag(999), PCM_LOCK_MODE_S));
	UT_ASSERT_EQ(cluster_pcm_grd_count(), 9);
	for (i = 0; i < 10; i++) {
		result = cluster_resource_x_reconfig_sweep(&token, 4, &batch);
		UT_ASSERT_EQ(batch.examined_count, (i % 5) < 4 ? 4 : 1);
		UT_ASSERT_EQ(batch.next_state_index,
			(i % 5) < 4 ? (uint64)((i % 5) + 1) * 4 : 0);
		if (i < 9) {
			UT_ASSERT_EQ(result, RESOURCE_X_RECONFIG_MORE);
			UT_ASSERT_EQ(batch.complete_wrap, i == 4 ? 1 : 0);
			UT_ASSERT_EQ(batch.zero_residual, 0);
		}
		else {
			UT_ASSERT_EQ(result, RESOURCE_X_RECONFIG_DONE);
			UT_ASSERT_EQ(batch.complete_wrap, 1);
			UT_ASSERT_EQ(batch.zero_residual, 1);
		}
	}
	UT_ASSERT(cluster_resource_x_reconfig_thaw_exact(&token));
	cluster_resource_x_reconfig_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.slot_examined_count, 34);
}

UT_TEST(test_resource_x_same_token_zero_and_clean_completion_proofs_are_exact)
{
	ResourceXCleanCompletionProof clean;
	ResourceXCleanCompletionProof clean_copy;
	ResourceXCleanCompletionProof cutover_clean;
	ResourceXReconfigBatch batch;
	ResourceXReconfigResult result;
	ResourceXReconfigToken cutover_token;
	ResourceXReconfigToken stale;
	ResourceXReconfigToken token;
	ResourceXZeroResidualProof zero;
	ResourceXZeroResidualProof zero_copy;
	ResourceXZeroResidualProof cutover_zero;
	ResourceXCleanCompletionProof thawed_clean;
	ResourceXReconfigToken thawed_token;
	ResourceXZeroResidualProof thawed_zero;
	uint64 frozen_digest = 0;
	uint64 thawed_digest = 0;
	int calls = 0;

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	do {
		result = cluster_resource_x_reconfig_sweep(&token, 4, &batch);
		calls++;
		UT_ASSERT(calls <= 4);
	} while (result == RESOURCE_X_RECONFIG_MORE
			 || result == RESOURCE_X_RECONFIG_RETRY);
	UT_ASSERT_EQ(result, RESOURCE_X_RECONFIG_DONE);
	UT_ASSERT(calls >= 2);
	UT_ASSERT(cluster_resource_x_reconfig_zero_proof_exact(&token, &zero));
	UT_ASSERT_EQ(zero.token.old_formation, token.old_formation);
	UT_ASSERT_EQ(zero.token.new_formation, token.new_formation);
	UT_ASSERT_EQ(zero.token.freeze_generation, token.freeze_generation);
	UT_ASSERT_EQ(zero.scan_begin_cursor, 0);
	UT_ASSERT_EQ(zero.scan_end_cursor, 4);
	UT_ASSERT_EQ(zero.scan_capacity, 4);
	UT_ASSERT_EQ(zero.scan_begin_slot_count, zero.scan_end_slot_count);
	UT_ASSERT(zero.final_mutation_sequence != 0);
	UT_ASSERT(zero.full_wrap_digest != 0);
	UT_ASSERT_EQ(zero.complete_wrap, 1);
	UT_ASSERT_EQ(zero.zero_residual, 1);

	stale = token;
	stale.new_formation++;
	UT_ASSERT(!cluster_resource_x_reconfig_zero_proof_exact(&stale, &zero_copy));
	fake_resource_x_transport_staged_count = 1;
	UT_ASSERT(!cluster_pcm_lock_resource_x_clean_completion_prove_exact(
		&token, &zero, &clean));
	fake_resource_x_transport_staged_count = 0;
	fake_resource_x_transport_snapshot_call_count = 0;
	fake_resource_x_transport_advance_on_call = 2;
	UT_ASSERT(!cluster_pcm_lock_resource_x_clean_completion_prove_exact(
		&token, &zero, &clean));
	fake_resource_x_transport_snapshot_call_count = 0;
	fake_resource_x_transport_advance_on_call = 0;
	UT_ASSERT(cluster_pcm_lock_resource_x_clean_completion_prove_exact(
		&token, &zero, &clean));
	UT_ASSERT_EQ(clean.final_mutation_sequence,
				 zero.final_mutation_sequence);
	UT_ASSERT_EQ(clean.logical_debt_zero, 1);
	UT_ASSERT_EQ(clean.transport_debt_zero, 1);
	UT_ASSERT_EQ(clean.transport_mutation_sequence,
				 fake_resource_x_transport_mutation_sequence);
	UT_ASSERT_EQ(clean.transport_staged_count, 0);
	UT_ASSERT(cluster_pcm_lock_resource_x_clean_completion_proof_exact(
		&token, &zero, &clean_copy));
	UT_ASSERT_EQ(memcmp(&clean_copy, &clean, sizeof(clean)), 0);
	UT_ASSERT(cluster_pcm_lock_resource_x_cutover_proofs_exact(
		&cutover_token, &cutover_zero, &cutover_clean));
	UT_ASSERT_EQ(memcmp(&cutover_token, &token, sizeof(token)), 0);
	UT_ASSERT_EQ(memcmp(&cutover_zero, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(memcmp(&cutover_clean, &clean, sizeof(clean)), 0);
	UT_ASSERT(cluster_pcm_lock_resource_x_cutover_current_proof_digest_exact(
		false, &cutover_token, &frozen_digest));
	UT_ASSERT_EQ(memcmp(&cutover_token, &token, sizeof(token)), 0);
	UT_ASSERT(frozen_digest != 0);

	/* A late physical owner may already have drained, leaving count zero.
	 * Its mutation generation still invalidates the older zero snapshot. */
	fake_resource_x_transport_mutation_sequence++;
	UT_ASSERT(!cluster_pcm_lock_resource_x_clean_completion_proof_exact(
		&token, &zero, &clean_copy));
	memset(&cutover_token, 0xff, sizeof(cutover_token));
	memset(&cutover_zero, 0xff, sizeof(cutover_zero));
	memset(&cutover_clean, 0xff, sizeof(cutover_clean));
	UT_ASSERT(!cluster_pcm_lock_resource_x_cutover_proofs_exact(
		&cutover_token, &cutover_zero, &cutover_clean));
	UT_ASSERT_EQ(cutover_token.old_formation, 0);
	UT_ASSERT_EQ(cutover_zero.proof_generation, 0);
	UT_ASSERT_EQ(cutover_clean.proof_generation, 0);
	UT_ASSERT(cluster_pcm_lock_resource_x_clean_completion_prove_exact(
		&token, &zero, &clean));
	fake_resource_x_transport_snapshot_available = false;
	UT_ASSERT(!cluster_pcm_lock_resource_x_clean_completion_proof_exact(
		&token, &zero, &clean_copy));
	fake_resource_x_transport_snapshot_available = true;
	UT_ASSERT(!cluster_pcm_lock_resource_x_clean_completion_proof_exact(
		&stale, &zero, &clean_copy));
	frozen_digest = 0;
	UT_ASSERT(cluster_pcm_lock_resource_x_cutover_current_proof_digest_exact(
		false, &cutover_token, &frozen_digest));
	UT_ASSERT(frozen_digest != 0);

	UT_ASSERT(cluster_resource_x_reconfig_thaw_exact(&token));
	UT_ASSERT(!cluster_resource_x_reconfig_zero_proof_exact(&token, &zero_copy));
	UT_ASSERT(!cluster_pcm_lock_resource_x_clean_completion_proof_exact(
		&token, &zero, &clean_copy));
	UT_ASSERT(!cluster_pcm_lock_resource_x_cutover_proofs_exact(
		&cutover_token, &cutover_zero, &cutover_clean));
	UT_ASSERT(cluster_pcm_lock_resource_x_cutover_thawed_proofs_exact(
		&thawed_token, &thawed_zero, &thawed_clean));
	UT_ASSERT_EQ(memcmp(&thawed_token, &token, sizeof(token)), 0);
	UT_ASSERT_EQ(memcmp(&thawed_zero, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(memcmp(&thawed_clean, &clean, sizeof(clean)), 0);
	UT_ASSERT(!cluster_pcm_lock_resource_x_cutover_current_proof_digest_exact(
		false, &cutover_token, &thawed_digest));
	UT_ASSERT(cluster_pcm_lock_resource_x_cutover_current_proof_digest_exact(
		true, &cutover_token, &thawed_digest));
	UT_ASSERT_EQ(memcmp(&cutover_token, &token, sizeof(token)), 0);
	UT_ASSERT_EQ(thawed_digest, frozen_digest);
	fake_resource_x_transport_mutation_sequence++;
	UT_ASSERT(!cluster_pcm_lock_resource_x_cutover_thawed_proofs_exact(
		&thawed_token, &thawed_zero, &thawed_clean));
	UT_ASSERT(!cluster_pcm_lock_resource_x_cutover_current_proof_digest_exact(
		true, &cutover_token, &thawed_digest));
	UT_ASSERT_EQ(thawed_digest, UINT64_C(0));
}

UT_TEST(test_resource_x_same_token_proofs_reject_postscan_entry_mutation)
{
	BufferTag tag = make_tag(154);
	ResourceXCleanCompletionProof clean;
	ResourceXCleanCompletionProof clean_copy;
	ResourceXReconfigBatch batch;
	ResourceXReconfigToken token;
	ResourceXZeroResidualProof zero;
	ResourceXZeroResidualProof zero_copy;

	reset_fake_pcm_runtime(4);
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_MORE);
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_DONE);
	UT_ASSERT(cluster_resource_x_reconfig_zero_proof_exact(&token, &zero));
	UT_ASSERT(cluster_pcm_lock_resource_x_clean_completion_prove_exact(
		&token, &zero, &clean));

	/* The cutover owner closes legacy admission before proof.  This deliberate
	 * legacy entry mutation models a violated barrier and must invalidate both
	 * predecessor records rather than be hidden by a final zero snapshot. */
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT(!cluster_resource_x_reconfig_zero_proof_exact(
		&token, &zero_copy));
	UT_ASSERT(!cluster_pcm_lock_resource_x_clean_completion_proof_exact(
		&token, &zero, &clean_copy));
	UT_ASSERT(!cluster_resource_x_reconfig_thaw_exact(&token));
}

UT_TEST(test_resource_x_reconfig_t1_orphan_blocks_once_and_retains_evidence)
{
	BufferTag tag = make_tag(140);
	ResourceXAcquisitionRef ref;
	ResourceXReconfigToken token;
	ResourceXReconfigBatch batch;
	ResourceXReconfigStats stats;

	reset_fake_pcm_runtime(4);
	ref = make_resource_x_acquisition_ref(tag, 2, 17, 41);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_ORPHAN);
	UT_ASSERT_EQ(batch.orphan_count, 1);
	UT_ASSERT_EQ(batch.residual_count, 1);
	UT_ASSERT(!cluster_resource_x_reconfig_thaw_exact(&token));
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_CORRUPT);
	cluster_resource_x_reconfig_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.orphan_count, 1);
	UT_ASSERT_EQ(stats.blocked_count, 1);
}

UT_TEST(test_resource_x_reconfig_half_join_without_active_is_retained_orphan)
{
	BufferTag tag = make_tag(143);
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame retained_grant;
	ResourceXDecodedFrame retained_image;
	ResourceXRequesterJoinSnapshot join;
	ResourceXAssertion assertion;
	ResourceXReconfigToken token;
	ResourceXReconfigBatch batch;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	make_resource_x_remote_join_pair(tag, 2, &grant, &image);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &join), RESOURCE_X_APPLY_APPLIED);
	assertion = join.assertion;
	UT_ASSERT_EQ(join.flags, RESOURCE_X_REQUESTER_JOIN_HAS_IMAGE);
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_ORPHAN);
	UT_ASSERT_EQ(batch.orphan_count, 1);
	UT_ASSERT_EQ(batch.residual_count, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_frames_exact(
		&assertion, &retained_grant, &retained_image, &join),
		RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(join.flags, RESOURCE_X_REQUESTER_JOIN_HAS_IMAGE);
}

UT_TEST(test_resource_x_reconfig_t2_neutralizes_outside_entry_lock_then_blocks_orphan)
{
	BufferTag tag = make_tag(141);
	ResourceXAcquisitionRef ref;
	ResourceXBufferInstallProof proof;
	ResourceXReconfigToken token;
	ResourceXReconfigBatch batch;
	ResourceXReconfigStats stats;

	reset_fake_pcm_runtime(4);
	ref = make_resource_x_acquisition_ref(tag, 2, 17, 41);
	proof.ownership_generation = 7;
	proof.writer_activation_token = 11;
	proof.resource_x_activation_generation = 41;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(&ref, &proof),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_ORPHAN);
	UT_ASSERT_EQ(fake_neutralize_count, 1);
	UT_ASSERT(fake_neutralize_without_pcm_lock);
	UT_ASSERT(BufferTagsEqual(&fake_neutralize_tag, &tag));
	UT_ASSERT_EQ(fake_neutralize_formation, 17);
	UT_ASSERT_EQ(fake_neutralize_generation, 41);
	UT_ASSERT_EQ(batch.sidecar_neutralized_count, 1);
	cluster_resource_x_reconfig_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.sidecar_neutralized_count, 1);
	UT_ASSERT_EQ(stats.orphan_count, 1);
	UT_ASSERT_EQ(stats.blocked_count, 1);
}

UT_TEST(test_resource_x_reconfig_t2_newer_sidecar_survives_and_blocks_orphan)
{
	BufferTag tag = make_tag(142);
	ResourceXAcquisitionRef ref;
	ResourceXBufferInstallProof proof;
	ResourceXReconfigToken token;
	ResourceXReconfigBatch batch;
	ResourceXReconfigStats stats;

	reset_fake_pcm_runtime(4);
	fake_neutralize_result = RESOURCE_X_SIDECAR_SUCCESSOR;
	ref = make_resource_x_acquisition_ref(tag, 2, 17, 42);
	proof.ownership_generation = 8;
	proof.writer_activation_token = 12;
	proof.resource_x_activation_generation = 42;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(&ref, &proof),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_ORPHAN);
	UT_ASSERT_EQ(fake_neutralize_count, 1);
	UT_ASSERT(fake_neutralize_without_pcm_lock);
	UT_ASSERT_EQ(batch.sidecar_neutralized_count, 0);
	UT_ASSERT_EQ(batch.orphan_count, 1);
	cluster_resource_x_reconfig_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.sidecar_neutralized_count, 0);
	UT_ASSERT_EQ(stats.sidecar_stale_count, 1);
	UT_ASSERT_EQ(stats.orphan_count, 1);
	UT_ASSERT_EQ(stats.blocked_count, 1);
}

static ResourceXDecodedFrame
make_resource_x_master_frame(ResourceXWireKind kind, BufferTag tag,
							 int32 requester_node, int32 action_node)
{
	ResourceXDecodedFrame frame;

	memset(&frame, 0, sizeof(frame));
	UT_ASSERT(resource_x_assertion_init(&tag, requester_node,
										 &frame.common.logical_assertion));
	frame.kind = kind;
	frame.common.base_authority_generation = 1;
	frame.common.resource_formation = 17;
	frame.common.master_session_incarnation = 31;
	frame.common.assertion_sequence = 41;
	frame.common.ordered_lane = 7;
	frame.common.action_node = action_node;
	frame.common.observed_mode = PCM_STATE_N;
	frame.common.target_mode = PCM_STATE_X;
	frame.common.sender_connection_generation = 51;
	frame.common.authority_generation = 1;
	return frame;
}

static void
put_resource_x_test_u64(uint8 *out, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++)
		out[i] = (uint8)(value >> ((7 - i) * 8));
}

static void
set_resource_x_test_source_fence(uint8 *fence, uint64 source_generation,
								 uint8 source_mode)
{
	put_resource_x_test_u64(fence + 20, source_generation);
	fence[28] = source_mode;
	fence[29] = 1;
}

static ResourceXDecodedFrame
make_resource_x_bootstrap_request_values(BufferTag tag, int32 requester_node,
									 uint64 resource_formation,
									 uint64 master_session_incarnation,
									 uint64 assertion_sequence,
									 uint32 sender_connection_generation)
{
	ResourceXDecodedFrame request = make_resource_x_master_frame(
		RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP, tag, requester_node,
		requester_node);
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 wire[RESOURCE_X_CONTROL_V1_BYTES];
	uint16 wire_len = 0;

	request.common.base_authority_generation = 0;
	request.common.ordered_lane = 0;
	request.common.resource_formation = resource_formation;
	request.common.master_session_incarnation
		= master_session_incarnation;
	request.common.assertion_sequence = assertion_sequence;
	request.common.sender_connection_generation
		= sender_connection_generation;
	request.common.authority_generation = 0;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X,
		&request, wire, sizeof(wire), &wire_len, &reject));
	UT_ASSERT_EQ(wire_len, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X,
		wire, wire_len, &decoded, &reject));
	return decoded;
}

static ResourceXDecodedFrame
make_resource_x_bootstrap_request(BufferTag tag, int32 requester_node)
{
	return make_resource_x_bootstrap_request_values(
		tag, requester_node, 17, 31, 41, 51);
}

static ResourceXDecodedFrame
make_resource_x_bootstrap_ack_values(
	const ResourceXDecodedFrame *request, uint64 base_authority_generation,
	uint32 sender_connection_generation)
{
	ResourceXDecodedFrame ack = *request;
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 wire[RESOURCE_X_CONTROL_V1_BYTES];
	uint16 wire_len = 0;

	ack.common.base_authority_generation = base_authority_generation;
	ack.common.sender_connection_generation = sender_connection_generation;
	ack.common.outcome = RESOURCE_X_OUTCOME_OK;
	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, &ack, wire, sizeof(wire),
		&wire_len, &reject));
	UT_ASSERT_EQ(wire_len, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, wire, wire_len, &decoded, &reject));
	return decoded;
}

UT_TEST(test_pcm_d4_tombstone_reuse_is_generation_exact)
{
	BufferTag retired_tag = make_tag(200);
	BufferTag survivor_tag = make_tag(202);
	ClusterPcmWmProv provenance;
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef ref;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame old_assertion;
	ResourceXDecodedFrame request;
	ResourceXMasterSnapshot snapshot;
	uint32 retired_slot;
	uint32 survivor_slot;
	uint64 new_generation;
	uint64 old_generation;
	uint64 survivor_generation;

	reset_fake_pcm_runtime(2);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(pcm_entry_ref_acquire(
		&retired_tag, true, &ref, &acquire_result));
	old_generation = ref.binding_generation;
	retired_slot = ref.registry_slot;
	pcm_entry_ref_release(&ref);
	cluster_pcm_lock_pi_watermark_scn_advance(retired_tag, (SCN)0x4400,
		CLUSTER_PCM_WM_SRC_REDECLARE, 0, 71, 17);
	UT_ASSERT(cluster_pcm_lock_pi_watermark_prov_query(
		retired_tag, &provenance));
	cluster_pcm_lock_pi_watermark_retire_for_tag(retired_tag);

	UT_ASSERT(pcm_entry_ref_acquire(
		&survivor_tag, true, &ref, &acquire_result));
	survivor_generation = ref.binding_generation;
	survivor_slot = ref.registry_slot;
	pcm_entry_ref_release(&ref);
	UT_ASSERT(retired_slot != survivor_slot);
	old_assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, retired_tag, 1, 1);

	UT_ASSERT(pcm_entry_try_retire_exact(
		&retired_tag, old_generation, PCM_RETIRE_REASON_PI_DISCARDED));
	UT_ASSERT(!cluster_pcm_lock_pi_watermark_prov_query(
		retired_tag, &provenance));
	UT_ASSERT(!pcm_entry_ref_acquire(
		&retired_tag, false, &ref, &acquire_result));
	UT_ASSERT_EQ(acquire_result, PCM_ENTRY_ACQUIRE_NOT_FOUND);
	UT_ASSERT(pcm_entry_ref_acquire(
		&survivor_tag, false, &ref, &acquire_result));
	UT_ASSERT_EQ(ref.binding_generation, survivor_generation);
	UT_ASSERT_EQ(ref.registry_slot, survivor_slot);
	pcm_entry_ref_release(&ref);

	UT_ASSERT(pcm_entry_ref_acquire(
		&retired_tag, true, &ref, &acquire_result));
	new_generation = ref.binding_generation;
	UT_ASSERT(new_generation > old_generation);
	UT_ASSERT_EQ(ref.registry_slot, retired_slot);
	pcm_entry_ref_release(&ref);
	UT_ASSERT(!cluster_pcm_lock_pi_watermark_prov_query(
		retired_tag, &provenance));

	request = make_resource_x_bootstrap_request(retired_tag, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&request, 1, 61, 77, 31, 71, &ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ack.common.base_authority_generation > UINT64_C(1));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&old_assertion, 1, &snapshot), RESOURCE_X_APPLY_STALE);
}

UT_TEST(test_pcm_d4_reused_binding_first_grant_keeps_generation_lineage)
{
	BufferTag tag = make_tag(201);
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef ref;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame request;
	ResourceXDurableProof durable;
	ResourceXMasterSnapshot snapshot;
	uint64 old_generation;

	reset_fake_pcm_runtime(1);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(pcm_entry_ref_acquire(&tag, true, &ref, &acquire_result));
	old_generation = ref.binding_generation;
	pcm_entry_ref_release(&ref);
	UT_ASSERT(pcm_entry_try_retire_exact(
		&tag, old_generation, PCM_RETIRE_REASON_PI_DISCARDED));
	UT_ASSERT(pcm_entry_ref_acquire(&tag, true, &ref, &acquire_result));
	UT_ASSERT(ref.binding_generation > old_generation);
	pcm_entry_ref_release(&ref);

	request = make_resource_x_bootstrap_request(tag, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&request, 1, 61, 77, 31, 71, &ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ack.common.base_authority_generation > UINT64_C(1));
	assertion = ack;
	assertion.kind = RESOURCE_X_WIRE_ASSERT_X;
	assertion.common.sender_connection_generation = 51;
	assertion.common.authority_generation
		= assertion.common.base_authority_generation;
	assertion.common.outcome = RESOURCE_X_OUTCOME_NONE;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&assertion, 1, 61, 77, 31, 71, &snapshot),
		RESOURCE_X_APPLY_APPLIED);

	memset(&durable, 0, sizeof(durable));
	durable.assertion = assertion.common.logical_assertion;
	durable.base_authority_generation
		= assertion.common.base_authority_generation;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = assertion.common.assertion_sequence;
	durable.requester_target_generation = assertion.common.assertion_sequence;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x12345678);
	durable.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(
		&durable, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
	UT_ASSERT(snapshot.final_authority_generation
		> assertion.common.base_authority_generation);
}

UT_TEST(test_pcm_d4_displaced_tombstone_preserves_authority_floor)
{
	BufferTag retired_tag = make_tag(200);
	BufferTag survivor_tag = make_tag(202);
	BufferTag displacement_tag = make_tag(204);
	PcmAuthoritySnapshot authority;
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef ref;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame old_assertion;
	ResourceXDecodedFrame request;
	ResourceXMasterSnapshot snapshot;
	uint32 displaced_slot;
	uint32 retired_slot;
	uint32 survivor_slot;
	uint64 retired_generation;
	uint64 survivor_generation;
	uint64 retired_authority;
	int cycle;

	reset_fake_pcm_runtime(2);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(pcm_entry_ref_acquire(
		&retired_tag, true, &ref, &acquire_result));
	retired_generation = ref.binding_generation;
	retired_slot = ref.registry_slot;
	pcm_entry_ref_release(&ref);
	UT_ASSERT(pcm_entry_ref_acquire(
		&survivor_tag, true, &ref, &acquire_result));
	survivor_generation = ref.binding_generation;
	survivor_slot = ref.registry_slot;
	pcm_entry_ref_release(&ref);
	UT_ASSERT(retired_slot != survivor_slot);

	for (cycle = 0; cycle < 5; cycle++) {
		UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
			retired_tag, PCM_TRANS_N_TO_X, 0), PCM_GCS_TRANSITION_APPLIED);
		UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
			retired_tag, PCM_TRANS_X_TO_N_RELEASE, 0),
			PCM_GCS_TRANSITION_APPLIED);
	}
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(retired_tag, &authority));
	retired_authority = authority.transition_count;
	UT_ASSERT(retired_authority > retired_generation);
	old_assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, retired_tag, 1, 1);
	old_assertion.common.base_authority_generation = retired_authority;
	old_assertion.common.authority_generation = retired_authority;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_base_bind_exact(
		&old_assertion.common.logical_assertion, 17,
		retired_authority, &authority), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(pcm_entry_try_retire_exact(&retired_tag, retired_generation,
		PCM_RETIRE_REASON_HOLDER_RELEASE));

	/* Same-home tag consumes the old tag's tombstone. */
	UT_ASSERT(pcm_entry_ref_acquire(
		&displacement_tag, true, &ref, &acquire_result));
	displaced_slot = ref.registry_slot;
	pcm_entry_ref_release(&ref);
	UT_ASSERT_EQ(displaced_slot, retired_slot);
	UT_ASSERT(pcm_entry_try_retire_exact(&survivor_tag, survivor_generation,
		PCM_RETIRE_REASON_HOLDER_RELEASE));

	/* Rebinding the original tag now uses the other tombstone. Its base must
	 * still dominate its own retired authority, not that lower slot floor. */
	UT_ASSERT(pcm_entry_ref_acquire(
		&retired_tag, true, &ref, &acquire_result));
	UT_ASSERT_EQ(ref.registry_slot, survivor_slot);
	pcm_entry_ref_release(&ref);
	request = make_resource_x_bootstrap_request(retired_tag, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&request, 1, 61, 77, 31, 71, &ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ack.common.base_authority_generation > retired_authority);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&old_assertion, 1, &snapshot), RESOURCE_X_APPLY_STALE);
}

UT_TEST(test_pcm_d5_capacity_retry_reclaims_one_terminal_binding)
{
	BufferTag retired_tag = make_tag(210);
	BufferTag survivor_tag = make_tag(211);
	BufferTag new_tag = make_tag(212);
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef new_ref;
	PcmEntryRef survivor_ref;
	PcmEntryRef retired_ref;
	uint64 retired_generation;
	uint64 survivor_generation;

	reset_fake_pcm_runtime(2);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(pcm_entry_ref_acquire(
		&retired_tag, true, &retired_ref, &acquire_result));
	retired_generation = retired_ref.binding_generation;
	pcm_entry_ref_release(&retired_ref);
	UT_ASSERT(pcm_entry_ref_acquire(
		&survivor_tag, true, &survivor_ref, &acquire_result));
	survivor_generation = survivor_ref.binding_generation;
	pcm_entry_ref_release(&survivor_ref);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		survivor_tag, PCM_TRANS_N_TO_S, 0), PCM_GCS_TRANSITION_APPLIED);

	/* The first insert sees a full directory. One bounded reclaim and one
	 * exact insert retry may reuse only the unpinned terminal binding. */
	UT_ASSERT(pcm_entry_ref_acquire(
		&new_tag, true, &new_ref, &acquire_result));
	UT_ASSERT(new_ref.binding_generation > retired_generation);
	pcm_entry_ref_release(&new_ref);
	UT_ASSERT(!pcm_entry_ref_acquire(
		&retired_tag, false, &retired_ref, &acquire_result));
	UT_ASSERT_EQ(acquire_result, PCM_ENTRY_ACQUIRE_NOT_FOUND);
	UT_ASSERT(pcm_entry_ref_acquire(
		&survivor_tag, false, &survivor_ref, &acquire_result));
	UT_ASSERT_EQ(survivor_ref.binding_generation, survivor_generation);
	pcm_entry_ref_release(&survivor_ref);
}

UT_TEST(test_pcm_d5_bounded_reclaim_never_exceeds_probe_budget)
{
	PcmReclaimBatch batch;
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef ref;
	int blockno;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	for (blockno = 220; blockno < 224; blockno++) {
		BufferTag tag = make_tag((uint32)blockno);

		UT_ASSERT(pcm_entry_ref_acquire(&tag, true, &ref, &acquire_result));
		pcm_entry_ref_release(&ref);
	}
	UT_ASSERT(cluster_pcm_lock_reclaim_bounded(2, &batch));
	UT_ASSERT_EQ(batch.examined_count, 2);
	UT_ASSERT_EQ(batch.attempted_count, 2);
	UT_ASSERT_EQ(batch.retired_count, 2);
	UT_ASSERT_EQ(fake_pcm_entry_count, 2);
}

UT_TEST(test_pcm_d5_last_holder_release_fast_retires_exact_binding)
{
	BufferTag tag = make_tag(224);
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef ref;
	uint64 generation;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT(pcm_entry_ref_acquire(&tag, false, &ref, &acquire_result));
	generation = ref.binding_generation;
	pcm_entry_ref_release(&ref);
	cluster_pcm_lock_release(tag);
	UT_ASSERT(!pcm_entry_ref_acquire(&tag, false, &ref, &acquire_result));
	UT_ASSERT_EQ(acquire_result, PCM_ENTRY_ACQUIRE_NOT_FOUND);

	UT_ASSERT(pcm_entry_ref_acquire(&tag, true, &ref, &acquire_result));
	UT_ASSERT(ref.binding_generation > generation);
	pcm_entry_ref_release(&ref);
}

UT_TEST(test_pcm_d5_remote_master_s_eviction_fast_retires_closed_requester_projection)
{
	BufferTag tag = make_tag(324);
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef ref;
	uint64 generation;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(pcm_entry_ref_acquire(&tag, true, &ref, &acquire_result));
	generation = ref.binding_generation;
	pcm_entry_ref_release(&ref);

	/* The remote master ACK closes the canonical cached-S residency.  This
	 * node's neutral, debt-free Resource-X projection is not a second S
	 * authority and must get the same one-shot D3 fast-retire opportunity as
	 * a local-master last-holder release. */
	fake_gcs_master_node = 1;
	fake_gcs_transition_allowed = true;
	cluster_pcm_lock_release_saved_tag_for_eviction(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(fake_gcs_transition_count, 1);
	UT_ASSERT(BufferTagsEqual(&fake_gcs_transition_tag, &tag));
	UT_ASSERT_EQ(fake_gcs_transition_kind, PCM_TRANS_S_TO_N_RELEASE);
	UT_ASSERT_EQ(fake_gcs_transition_master, 1);
	UT_ASSERT(!pcm_entry_ref_acquire(&tag, false, &ref, &acquire_result));
	UT_ASSERT_EQ(acquire_result, PCM_ENTRY_ACQUIRE_NOT_FOUND);
	UT_ASSERT(pcm_entry_ref_acquire(&tag, true, &ref, &acquire_result));
	UT_ASSERT(ref.binding_generation > generation);
	pcm_entry_ref_release(&ref);
}

UT_TEST(test_pcm_d5_remote_master_s_eviction_never_retires_rebound_projection)
{
	BufferTag tag = make_tag(325);
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef ref;
	uint64 old_generation;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(pcm_entry_ref_acquire(&tag, true, &ref, &acquire_result));
	old_generation = ref.binding_generation;
	pcm_entry_ref_release(&ref);

	fake_gcs_master_node = 1;
	fake_gcs_transition_allowed = true;
	fake_gcs_transition_rebind = true;
	cluster_pcm_lock_release_saved_tag_for_eviction(tag, PCM_LOCK_MODE_S);
	UT_ASSERT(fake_gcs_transition_rebound_generation > old_generation);
	UT_ASSERT(pcm_entry_ref_acquire(&tag, false, &ref, &acquire_result));
	UT_ASSERT_EQ(ref.binding_generation,
		fake_gcs_transition_rebound_generation);
	pcm_entry_ref_release(&ref);
}

UT_TEST(test_pcm_d7_remote_s_eviction_closes_pending_x_to_s_master_state)
{
	BufferTag tag = make_tag(326);
	PcmAuthoritySnapshot authority;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);

	/* Reproduce the exact terminal split observed by fresh t/430: the holder
	 * has already committed its local X->S, but the unacknowledged predecessor
	 * notification has not changed the remote master's X@self state yet.  A
	 * cache-replacement release must close that predecessor before returning;
	 * it may not leave the master X after the local mapping is gone. */
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_N_TO_X, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ((int) authority.state, (int) PCM_STATE_X);
	UT_ASSERT_EQ(authority.x_holder_node, 0);

	fake_gcs_master_node = 1;
	fake_gcs_transition_allowed = true;
	fake_gcs_transition_apply_to_pcm = true;
	cluster_pcm_lock_release_saved_tag_for_eviction(tag, PCM_LOCK_MODE_S);

	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ((int) authority.state, (int) PCM_STATE_N);
	UT_ASSERT_EQ(authority.x_holder_node, -1);
}

UT_TEST(test_pcm_d5_durable_pi_discard_fast_retires_exact_binding)
{
	BufferTag tag = make_tag(225);
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef ref;
	uint64 generation;
	uint32 holders = 0;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	cluster_pcm_lock_downgrade(tag, PCM_LOCK_MODE_N, true);
	cluster_pcm_lock_pi_watermark_scn_advance(tag, (SCN)0x5500,
		CLUSTER_PCM_WM_SRC_REDECLARE, 0, 31, 17);
	UT_ASSERT(pcm_entry_ref_acquire(&tag, false, &ref, &acquire_result));
	generation = ref.binding_generation;
	pcm_entry_ref_release(&ref);

	UT_ASSERT(!cluster_pcm_lock_pi_discard_collect(
		tag, (SCN)0x54ff, &holders));
	UT_ASSERT_EQ(holders, 0);
	UT_ASSERT(cluster_pcm_lock_pi_discard_collect(
		tag, (SCN)0x5500, &holders));
	UT_ASSERT_EQ(holders, UINT32_C(1));
	UT_ASSERT(!pcm_entry_ref_acquire(&tag, false, &ref, &acquire_result));
	UT_ASSERT_EQ(acquire_result, PCM_ENTRY_ACQUIRE_NOT_FOUND);

	UT_ASSERT(pcm_entry_ref_acquire(&tag, true, &ref, &acquire_result));
	UT_ASSERT(ref.binding_generation > generation);
	pcm_entry_ref_release(&ref);
}

UT_TEST(test_pcm_d5_resource_x_terminal_release_fast_retires_exact_binding)
{
	BufferTag tag = make_tag(226);
	PcmAuthoritySnapshot authority;
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef ref;
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame release;
	ResourceXDecodedFrame settlement;
	ResourceXDurableProof durable;
	ResourceXIntentSlot grant_intent;
	ResourceXMasterSnapshot settled;
	ResourceXMasterSnapshot snapshot;
	uint8 grant_bytes[RESOURCE_X_PROOF_V1_BYTES];

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_N_TO_X, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_X_TO_N_RELEASE, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));

	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 1, 1);
	assertion.common.base_authority_generation = authority.transition_count;
	assertion.common.authority_generation = authority.transition_count;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_base_bind_exact(
		&assertion.common.logical_assertion, 17,
		authority.transition_count, &authority), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);

	memset(&durable, 0, sizeof(durable));
	durable.assertion = assertion.common.logical_assertion;
	durable.base_authority_generation = authority.transition_count;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = assertion.common.assertion_sequence;
	durable.requester_target_generation = 41;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x12345678);
	durable.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(
		&durable, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
		&assertion.common.logical_assertion, &grant_intent, grant_bytes,
		sizeof(grant_bytes)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_stage_exact(
		&grant_intent, 101), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_grant_intent_complete_exact(
		&grant_intent));

	settlement = make_resource_x_master_frame(
		RESOURCE_X_WIRE_INSTALL_SETTLEMENT, tag, 1, 1);
	settlement.common.outcome = RESOURCE_X_OUTCOME_OK;
	settlement.common.base_authority_generation
		= durable.base_authority_generation;
	settlement.common.authority_generation
		= snapshot.final_authority_generation;
	settlement.body.install_settlement.conversion_base_generation
		= durable.base_authority_generation;
	settlement.body.install_settlement.final_authority_generation
		= snapshot.final_authority_generation;
	settlement.body.install_settlement.requester_connection_generation = 91;
	settlement.body.install_settlement.requester_target_generation = 41;
	settlement.body.install_settlement.page_scn_lsn = durable.page_scn_lsn;
	settlement.body.install_settlement.page_checksum = durable.page_checksum;
	settlement.body.install_settlement.source_proof_crc32c
		= durable.source_proof_crc32c;
	settlement.body.install_settlement.installed_mode = PCM_STATE_X;
	settlement.body.install_settlement.requester_role
		= RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
	settlement.body.install_settlement.terminal_outcome
		= RESOURCE_X_OUTCOME_OK;
	settlement.body.install_settlement.terminal_state
		= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
		&settlement, 1, &settled), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_settled_retire_exact(
		&assertion.common.logical_assertion,
		assertion.common.assertion_sequence, &settled),
		RESOURCE_X_APPLY_APPLIED);

	release = make_resource_x_master_frame(
		RESOURCE_X_WIRE_RELEASE_X, tag, 1, 1);
	release.common.observed_mode = PCM_STATE_X;
	release.common.target_mode = PCM_STATE_N;
	release.common.outcome = RESOURCE_X_OUTCOME_OK;
	release.common.base_authority_generation
		= durable.base_authority_generation;
	release.common.authority_generation
		= settled.final_authority_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_release_x_exact(
		&release, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_RELEASED);
	UT_ASSERT(!pcm_entry_ref_acquire(&tag, false, &ref, &acquire_result));
	UT_ASSERT_EQ(acquire_result, PCM_ENTRY_ACQUIRE_NOT_FOUND);
}

UT_TEST(test_pcm_d5_lifecycle_stats_are_exact_not_legacy_aliases)
{
	BufferTag first_tag = make_tag(227);
	BufferTag second_tag = make_tag(228);
	BufferTag third_tag = make_tag(229);
	BufferTag full_tag = make_tag(230);
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef ref;
	PcmGrdLifecycleStats stats;

	reset_fake_pcm_runtime(2);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	cluster_pcm_grd_lifecycle_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.live_entries, 0);
	UT_ASSERT_EQ(stats.peak_live_entries, 0);

	UT_ASSERT(pcm_entry_ref_acquire(
		&first_tag, true, &ref, &acquire_result));
	pcm_entry_ref_release(&ref);
	UT_ASSERT(pcm_entry_ref_acquire(
		&second_tag, true, &ref, &acquire_result));
	pcm_entry_ref_release(&ref);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		second_tag, PCM_TRANS_N_TO_S, 0), PCM_GCS_TRANSITION_APPLIED);

	/* Full allocation reclaims first_tag and reuses exactly one tombstone. */
	UT_ASSERT(pcm_entry_ref_acquire(
		&third_tag, true, &ref, &acquire_result));
	pcm_entry_ref_release(&ref);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		third_tag, PCM_TRANS_N_TO_S, 0), PCM_GCS_TRANSITION_APPLIED);

	/* Both live bindings are active. One bounded retry must terminate with
	 * NO_CAPACITY and leave the Resource-X gate open. */
	UT_ASSERT(!pcm_entry_ref_acquire(
		&full_tag, true, &ref, &acquire_result));
	UT_ASSERT_EQ(acquire_result, PCM_ENTRY_ACQUIRE_NO_CAPACITY);
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_open_exact(17));

	cluster_pcm_grd_lifecycle_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.live_entries, 2);
	UT_ASSERT_EQ(stats.tombstone_slots, 0);
	UT_ASSERT(stats.binding_generation >= UINT64_C(3));
	UT_ASSERT(stats.reclaim_attempt_count >= UINT64_C(3));
	UT_ASSERT_EQ(stats.reclaim_success_count, UINT64_C(1));
	UT_ASSERT_EQ(stats.reclaim_reuse_count, UINT64_C(1));
	UT_ASSERT_EQ(stats.capacity_retry_count, UINT64_C(2));
	UT_ASSERT_EQ(stats.capacity_fail_count, UINT64_C(1));
	UT_ASSERT_EQ(stats.peak_live_entries, UINT64_C(2));
	UT_ASSERT(stats.reclaim_refused[PCM_RETIRE_REFUSAL_PCM_MODE_NOT_N]
		>= UINT64_C(2));
}

UT_TEST(test_pcm_d1_bootstrap_no_capacity_is_pre_mutation_backpressure)
{
	BufferTag first_tag = make_tag(327);
	BufferTag second_tag = make_tag(328);
	BufferTag full_tag = make_tag(329);
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef ref;
	ResourceXAssertion assertion;
	ResourceXDecodedFrame dispatch;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBootstrapRoundAction action;

	reset_fake_pcm_runtime(2);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);

	UT_ASSERT(pcm_entry_ref_acquire(
		&first_tag, true, &ref, &acquire_result));
	pcm_entry_ref_release(&ref);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		first_tag, PCM_TRANS_N_TO_S, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(pcm_entry_ref_acquire(
		&second_tag, true, &ref, &acquire_result));
	pcm_entry_ref_release(&ref);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		second_tag, PCM_TRANS_N_TO_S, 0), PCM_GCS_TRANSITION_APPLIED);

	UT_ASSERT(resource_x_assertion_init(&full_tag, 0, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), false, 0, &dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_BACKPRESSURE);
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_open_exact(17));
	UT_ASSERT(!pcm_entry_ref_acquire(
		&full_tag, false, &ref, &acquire_result));
	UT_ASSERT_EQ(acquire_result, PCM_ENTRY_ACQUIRE_NOT_FOUND);
}

UT_TEST(test_pcm_d5_lmon_soft_threshold_reclaims_one_bounded_tick)
{
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef ref;
	PcmGrdLifecycleStats stats;
	int blockno;

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	for (blockno = 231; blockno < 234; blockno++) {
		BufferTag tag = make_tag((uint32)blockno);

		UT_ASSERT(pcm_entry_ref_acquire(
			&tag, true, &ref, &acquire_result));
		pcm_entry_ref_release(&ref);
	}
	cluster_pcm_grd_lifecycle_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.live_entries, UINT64_C(3));
	cluster_pcm_lock_lmon_reclaim_tick();
	cluster_pcm_grd_lifecycle_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.live_entries, 0);
	UT_ASSERT_EQ(stats.reclaim_attempt_count, UINT64_C(3));
	UT_ASSERT_EQ(stats.reclaim_success_count, UINT64_C(3));
	UT_ASSERT_EQ(stats.peak_live_entries, UINT64_C(3));
}

UT_TEST(test_pcm_d5_reclaim_stops_while_gate_is_not_open)
{
	BufferTag tag = make_tag(234);
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef ref;
	PcmGrdLifecycleStats stats;
	PcmReclaimBatch batch;
	ResourceXReconfigToken token;

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(pcm_entry_ref_acquire(&tag, true, &ref, &acquire_result));
	pcm_entry_ref_release(&ref);
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	UT_ASSERT(!cluster_pcm_lock_reclaim_bounded(4, &batch));
	UT_ASSERT(pcm_entry_ref_acquire(&tag, false, &ref, &acquire_result));
	pcm_entry_ref_release(&ref);
	cluster_pcm_grd_lifecycle_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.live_entries, UINT64_C(1));
	UT_ASSERT_EQ(stats.reclaim_attempt_count, 0);
	UT_ASSERT_EQ(stats.reclaim_success_count, 0);
}

UT_TEST(test_resource_x_native_head_is_exact_s_admission_barrier)
{
	BufferTag tag = make_tag(171);
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame settlement;
	ResourceXDurableProof durable;
	ResourceXMasterSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active_exact(&tag));

	request = make_resource_x_bootstrap_request(tag, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&request, 1, 61, 77, 31, 71, &ack), RESOURCE_X_APPLY_APPLIED);
	/* A kind-9 receipt is intentionally non-authority and cannot deny S. */
	UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active_exact(&tag));

	assertion = ack;
	assertion.kind = RESOURCE_X_WIRE_ASSERT_X;
	assertion.common.sender_connection_generation = 51;
	assertion.common.authority_generation
		= assertion.common.base_authority_generation;
	assertion.common.outcome = RESOURCE_X_OUTCOME_NONE;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&assertion, 1, 61, 77, 31, 71, &snapshot),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_PROOF);
	UT_ASSERT(cluster_pcm_lock_resource_x_s_barrier_active_exact(&tag));
	/* The native-head verdict and durable S publication must share the
	 * resource entry lock.  A pre-check followed by an independent apply
	 * would let ASSERT linearize in the gap. */
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_N_TO_S, 2), PCM_GCS_TRANSITION_PENDING_X);
	UT_ASSERT_EQ(cluster_pcm_lock_query(tag), PCM_LOCK_MODE_N);

	memset(&durable, 0, sizeof(durable));
	durable.assertion = assertion.common.logical_assertion;
	durable.base_authority_generation = 1;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = 41;
	durable.requester_target_generation = 41;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x12345678);
	durable.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(
		&durable, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
	UT_ASSERT(cluster_pcm_lock_resource_x_s_barrier_active_exact(&tag));

	settlement = make_resource_x_master_frame(
		RESOURCE_X_WIRE_INSTALL_SETTLEMENT, tag, 1, 1);
	settlement.common.ordered_lane = 0;
	settlement.common.outcome = RESOURCE_X_OUTCOME_OK;
	settlement.common.authority_generation = 2;
	settlement.body.install_settlement.conversion_base_generation = 1;
	settlement.body.install_settlement.final_authority_generation = 2;
	settlement.body.install_settlement.requester_connection_generation = 91;
	settlement.body.install_settlement.requester_target_generation = 41;
	settlement.body.install_settlement.page_scn_lsn = 82;
	settlement.body.install_settlement.page_checksum = UINT32_C(0x12345678);
	settlement.body.install_settlement.source_proof_crc32c
		= UINT32_C(0x87654321);
	settlement.body.install_settlement.installed_mode = PCM_STATE_X;
	settlement.body.install_settlement.requester_role
		= RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
	settlement.body.install_settlement.terminal_outcome
		= RESOURCE_X_OUTCOME_OK;
	settlement.body.install_settlement.terminal_state
		= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
		&settlement, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_SETTLED);
	UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active_exact(&tag));
}

UT_TEST(test_resource_x_bootstrap_receipt_replays_and_consumes_exactly)
{
	BufferTag tag = make_tag(155);
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame replay_ack;
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame mutated;
	ResourceXMasterSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	request = make_resource_x_bootstrap_request(tag, 1);
	UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active(&tag));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&request, 1, 61, 77, 31, 71, &ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_s_barrier_active(&tag));
	UT_ASSERT_EQ(ack.kind, RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP);
	UT_ASSERT(resource_x_assertion_equal(&ack.common.logical_assertion,
		&request.common.logical_assertion));
	UT_ASSERT_EQ(ack.common.base_authority_generation, UINT64_C(1));
	UT_ASSERT_EQ(ack.common.authority_generation, UINT64_C(0));
	UT_ASSERT_EQ(ack.common.resource_formation,
		request.common.resource_formation);
	UT_ASSERT_EQ(ack.common.master_session_incarnation,
		request.common.master_session_incarnation);
	UT_ASSERT_EQ(ack.common.assertion_sequence,
		request.common.assertion_sequence);
	UT_ASSERT_EQ(ack.common.sender_connection_generation, UINT32_C(71));
	UT_ASSERT_EQ(ack.common.outcome, RESOURCE_X_OUTCOME_OK);

	memset(&replay_ack, 0, sizeof(replay_ack));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&request, 1, 61, 77, 31, 71, &replay_ack),
		RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&ack, &replay_ack, sizeof(ack)) == 0);

	assertion = ack;
	assertion.kind = RESOURCE_X_WIRE_ASSERT_X;
	assertion.common.sender_connection_generation = 51;
	assertion.common.authority_generation
		= assertion.common.base_authority_generation;
	assertion.common.outcome = RESOURCE_X_OUTCOME_NONE;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&assertion, 1, 61, 77, 31, 71, &snapshot),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&assertion, 1, 61, 77, 31, 71, &snapshot),
		RESOURCE_X_APPLY_DUPLICATE);
	mutated = assertion;
	mutated.common.observed_mode = PCM_STATE_S;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&mutated, 1, 61, 77, 31, 71, &snapshot),
		RESOURCE_X_APPLY_INVALID);
	mutated = assertion;
	mutated.common.source_candidate = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&mutated, 1, 61, 77, 31, 71, &snapshot),
		RESOURCE_X_APPLY_INVALID);
	mutated = assertion;
	mutated.common.ordered_lane = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&mutated, 1, 61, 77, 31, 71, &snapshot),
		RESOURCE_X_APPLY_INVALID);
	mutated = assertion;
	mutated.payload_bytes--;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&mutated, 1, 61, 77, 31, 71, &snapshot),
		RESOURCE_X_APPLY_INVALID);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&request, 1, 61, 77, 31, 71, &replay_ack), RESOURCE_X_APPLY_STALE);
}

UT_TEST(test_resource_x_bootstrap_receipt_drift_invalidates_but_keeps_floor)
{
	BufferTag tag = make_tag(154);
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame different_sender;
	ResourceXDecodedFrame higher;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assertion;
	ResourceXMasterSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	request = make_resource_x_bootstrap_request(tag, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&request, 1, 61, 77, 31, 71, &ack), RESOURCE_X_APPLY_APPLIED);

	/* The requester sender coordinate changed independently of the unchanged
	 * master-receiver ingress coordinate.  The old binding is invalidated,
	 * but the exact attempt cannot be reused. */
	different_sender = make_resource_x_bootstrap_request_values(
		tag, 1, 17, 31, 41, 52);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&different_sender, 1, 61, 77, 31, 71, &ack),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&request, 1, 61, 77, 31, 71, &ack), RESOURCE_X_APPLY_STALE);

	higher = make_resource_x_bootstrap_request_values(
		tag, 1, 17, 31, 42, 52);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&higher, 1, 61, 77, 31, 72, &ack), RESOURCE_X_APPLY_APPLIED);
	assertion = ack;
	assertion.kind = RESOURCE_X_WIRE_ASSERT_X;
	assertion.common.sender_connection_generation = 52;
	assertion.common.authority_generation
		= assertion.common.base_authority_generation;
	assertion.common.outcome = RESOURCE_X_OUTCOME_NONE;

	/* An R4 record-generation change destroys the receipt.  Returning to the
	 * old value cannot resurrect it, and no canonical ASSERT was created. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&assertion, 1, 61, 78, 31, 72, &snapshot), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&assertion, 1, 61, 77, 31, 72, &snapshot), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&higher.common.logical_assertion, &snapshot),
		RESOURCE_X_APPLY_NOT_FOUND);

	higher = make_resource_x_bootstrap_request_values(
		tag, 1, 17, 31, 43, 52);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&higher, 1, 61, 78, 31, 72, &ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&request, 1, 61, 78, 31, 72, &ack), RESOURCE_X_APPLY_STALE);
}

UT_TEST(test_resource_x_bootstrap_terminal_retire_clears_binding_not_floor)
{
	BufferTag tag = make_tag(153);
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame next_request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame settlement;
	ResourceXDurableProof durable;
	ResourceXIntentSlot grant_intent;
	ResourceXMasterSnapshot snapshot;
	uint8 grant_bytes[RESOURCE_X_PROOF_V1_BYTES];

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	request = make_resource_x_bootstrap_request_values(
		tag, 3, 17, 31, 41, 51);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&request, 3, 61, 77, 31, 71, &ack), RESOURCE_X_APPLY_APPLIED);
	assertion = ack;
	assertion.kind = RESOURCE_X_WIRE_ASSERT_X;
	assertion.common.sender_connection_generation = 51;
	assertion.common.authority_generation
		= assertion.common.base_authority_generation;
	assertion.common.outcome = RESOURCE_X_OUTCOME_NONE;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&assertion, 3, 61, 77, 31, 71, &snapshot),
		RESOURCE_X_APPLY_APPLIED);

	memset(&durable, 0, sizeof(durable));
	durable.assertion = assertion.common.logical_assertion;
	durable.base_authority_generation = 1;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = 41;
	durable.requester_target_generation = 41;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x12345678);
	durable.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(
		&durable, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
		&assertion.common.logical_assertion, &grant_intent, grant_bytes,
		sizeof(grant_bytes)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_stage_exact(
		&grant_intent, 101), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_grant_intent_complete_exact(
		&grant_intent));

	settlement = make_resource_x_master_frame(
		RESOURCE_X_WIRE_INSTALL_SETTLEMENT, tag, 3, 3);
	settlement.common.ordered_lane = 0;
	settlement.common.outcome = RESOURCE_X_OUTCOME_OK;
	settlement.common.authority_generation = 2;
	settlement.body.install_settlement.conversion_base_generation = 1;
	settlement.body.install_settlement.final_authority_generation = 2;
	settlement.body.install_settlement.requester_connection_generation = 91;
	settlement.body.install_settlement.requester_target_generation = 41;
	settlement.body.install_settlement.page_scn_lsn = 82;
	settlement.body.install_settlement.page_checksum
		= UINT32_C(0x12345678);
	settlement.body.install_settlement.source_proof_crc32c
		= UINT32_C(0x87654321);
	settlement.body.install_settlement.installed_mode = PCM_STATE_X;
	settlement.body.install_settlement.requester_role
		= RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
	settlement.body.install_settlement.terminal_outcome
		= RESOURCE_X_OUTCOME_OK;
	settlement.body.install_settlement.terminal_state
		= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
		&settlement, 3, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_settled_retire_exact(
		&assertion.common.logical_assertion, 41, &snapshot),
		RESOURCE_X_APPLY_APPLIED);

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&request, 3, 61, 77, 31, 71, &ack), RESOURCE_X_APPLY_STALE);
	next_request = make_resource_x_bootstrap_request_values(
		tag, 3, 17, 31, 42, 51);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&next_request, 3, 61, 77, 31, 71, &ack),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ack.common.base_authority_generation, UINT64_C(2));
}

UT_TEST(test_resource_x_bootstrap_dispatches_one_current_base_at_a_time)
{
	BufferTag tag = make_tag(151);
	ResourceXDecodedFrame first_request;
	ResourceXDecodedFrame second_request;
	ResourceXDecodedFrame third_request;
	ResourceXDecodedFrame first_ack;
	ResourceXDecodedFrame replay_ack;
	ResourceXDecodedFrame second_ack;
	ResourceXDecodedFrame zero_ack;
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame settlement;
	ResourceXDurableProof durable;
	ResourceXIntentSlot grant_intent;
	ResourceXMasterSnapshot snapshot;
	uint8 grant_bytes[RESOURCE_X_PROOF_V1_BYTES];

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	first_request = make_resource_x_bootstrap_request_values(
		tag, 1, 17, 31, 41, 51);
	second_request = make_resource_x_bootstrap_request_values(
		tag, 2, 17, 31, 41, 52);
	third_request = make_resource_x_bootstrap_request_values(
		tag, 3, 17, 31, 41, 53);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&first_request, 1, 61, 77, 31, 71, &first_ack),
		RESOURCE_X_APPLY_APPLIED);

	/* A second requester must not freeze the same current base while the
	 * first receipt can still become canonical authority.  Its exact identity
	 * becomes the bounded next admission while ordinary R7 drives replay. */
	memset(&second_ack, 0xa5, sizeof(second_ack));
	memset(&zero_ack, 0, sizeof(zero_ack));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&second_request, 2, 62, 77, 31, 72, &second_ack),
		RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(&second_ack, &zero_ack, sizeof(second_ack)) == 0);

	/* The first otherwise-admissible rejection owns the single exact next
	 * admission.  A later requester cannot overwrite that identity. */
	memset(&second_ack, 0xa5, sizeof(second_ack));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&third_request, 3, 63, 77, 31, 73, &second_ack),
		RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(&second_ack, &zero_ack, sizeof(second_ack)) == 0);

	memset(&replay_ack, 0, sizeof(replay_ack));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&first_request, 1, 61, 77, 31, 71, &replay_ack),
		RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&first_ack, &replay_ack, sizeof(first_ack)) == 0);

	assertion = first_ack;
	assertion.kind = RESOURCE_X_WIRE_ASSERT_X;
	assertion.common.sender_connection_generation = 51;
	assertion.common.authority_generation
		= assertion.common.base_authority_generation;
	assertion.common.outcome = RESOURCE_X_OUTCOME_NONE;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&assertion, 1, 61, 77, 31, 71, &snapshot),
		RESOURCE_X_APPLY_APPLIED);

	memset(&second_ack, 0xa5, sizeof(second_ack));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&second_request, 2, 62, 77, 31, 72, &second_ack),
		RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(&second_ack, &zero_ack, sizeof(second_ack)) == 0);

	memset(&durable, 0, sizeof(durable));
	durable.assertion = assertion.common.logical_assertion;
	durable.base_authority_generation = 1;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = 41;
	durable.requester_target_generation = 41;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x12345678);
	durable.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(
		&durable, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
		&assertion.common.logical_assertion, &grant_intent, grant_bytes,
		sizeof(grant_bytes)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_stage_exact(
		&grant_intent, 101), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_grant_intent_complete_exact(
		&grant_intent));

	settlement = make_resource_x_master_frame(
		RESOURCE_X_WIRE_INSTALL_SETTLEMENT, tag, 1, 1);
	settlement.common.ordered_lane = 0;
	settlement.common.outcome = RESOURCE_X_OUTCOME_OK;
	settlement.common.authority_generation = 2;
	settlement.body.install_settlement.conversion_base_generation = 1;
	settlement.body.install_settlement.final_authority_generation = 2;
	settlement.body.install_settlement.requester_connection_generation = 91;
	settlement.body.install_settlement.requester_target_generation = 41;
	settlement.body.install_settlement.page_scn_lsn = 82;
	settlement.body.install_settlement.page_checksum
		= UINT32_C(0x12345678);
	settlement.body.install_settlement.source_proof_crc32c
		= UINT32_C(0x87654321);
	settlement.body.install_settlement.installed_mode = PCM_STATE_X;
	settlement.body.install_settlement.requester_role
		= RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
	settlement.body.install_settlement.terminal_outcome
		= RESOURCE_X_OUTCOME_OK;
	settlement.body.install_settlement.terminal_state
		= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
		&settlement, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_settled_retire_exact(
		&assertion.common.logical_assertion, 41, &snapshot),
		RESOURCE_X_APPLY_APPLIED);

	/* Retirement does not let a fresh contender bypass the retained exact
	 * successor.  The priority never froze base 1: only the exact successor
	 * replay may now sample current base 2 and create an ordinary receipt. */
	memset(&replay_ack, 0xa5, sizeof(replay_ack));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&third_request, 3, 63, 77, 31, 73, &replay_ack),
		RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(&replay_ack, &zero_ack, sizeof(replay_ack)) == 0);

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&second_request, 2, 62, 77, 31, 72, &second_ack),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(second_ack.common.base_authority_generation, UINT64_C(2));
}

UT_TEST(test_resource_x_bootstrap_r8_clears_old_binding_not_attempt_floor)
{
	BufferTag tag = make_tag(152);
	ResourceXDecodedFrame old_request;
	ResourceXDecodedFrame waiting_request;
	ResourceXDecodedFrame new_request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame round_dispatch;
	ResourceXAssertion round_assertion;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBootstrapRoundAction round_action;
	ResourceXReconfigToken token;
	ResourceXReconfigBatch batch;
	uint64 broadcast_before;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	old_request = make_resource_x_bootstrap_request_values(
		tag, 1, 17, 31, 41, 51);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&old_request, 1, 61, 77, 31, 71, &ack),
		RESOURCE_X_APPLY_APPLIED);
	waiting_request = make_resource_x_bootstrap_request_values(
		tag, 2, 17, 31, 41, 53);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&waiting_request, 2, 63, 77, 31, 71, &ack),
		RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(resource_x_assertion_init(&tag, 0, &round_assertion));
	round_action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&round_assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), false, 0, &round_dispatch, &terminal_ref);
	UT_ASSERT_EQ(round_action,
		RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(round_dispatch.common.assertion_sequence, UINT64_C(1));
	broadcast_before = fake_cv_broadcast_count;

	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
		RESOURCE_X_RECONFIG_MORE);
	UT_ASSERT_EQ(fake_cv_broadcast_count, broadcast_before + 1);
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
		RESOURCE_X_RECONFIG_DONE);
	UT_ASSERT(cluster_resource_x_reconfig_thaw_exact(&token));

	new_request = make_resource_x_bootstrap_request_values(
		tag, 1, 18, 32, 41, 52);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&new_request, 1, 62, 78, 32, 72, &ack),
		RESOURCE_X_APPLY_STALE);
	new_request = make_resource_x_bootstrap_request_values(
		tag, 1, 18, 32, 42, 52);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&new_request, 1, 62, 78, 32, 72, &ack),
		RESOURCE_X_APPLY_APPLIED);

	round_action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&round_assertion, 0, 18, 32, 78, 52, 62, UINT64_C(2000), (UINT64_C(2000)) - (UINT64_C(200)),
		UINT64_C(200), UINT64_C(50), false, 0, &round_dispatch, &terminal_ref);
	UT_ASSERT_EQ(round_action,
		RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(round_dispatch.common.assertion_sequence, UINT64_C(2));
}

UT_TEST(test_resource_x_bootstrap_round_fans_in_and_retries_same_attempt)
{
	BufferTag tag = make_tag(151);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame first_dispatch;
	ResourceXDecodedFrame retry_dispatch;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame first_assertion;
	ResourceXDecodedFrame retry_assertion;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXAcquisitionRef expected_ref;
	ResourceXBufferInstallProof install;
	ResourceXBufferActivationProof activation;
	ResourceXBootstrapRoundAction action;
	ResourceXBootstrapRoundFailureSnapshot failure_snapshot;
	int caller;

	reset_fake_pcm_runtime(4);
	fake_pcm_clock_us = 200;
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));

	for (caller = 0; caller < 4; caller++) {
		action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
			UINT64_C(100), UINT64_C(50), false, 0, caller == 0 ? &first_dispatch : &retry_dispatch,
			&terminal_ref);
		UT_ASSERT_EQ(action,
			caller == 0
				? RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST
				: RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	}
	UT_ASSERT_EQ(first_dispatch.kind,
		RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP);
	UT_ASSERT_EQ(first_dispatch.common.assertion_sequence, UINT64_C(1));
	UT_ASSERT_EQ(first_dispatch.common.resource_formation, UINT64_C(17));
	UT_ASSERT_EQ(first_dispatch.common.master_session_incarnation,
		UINT64_C(31));
	UT_ASSERT_EQ(first_dispatch.common.sender_connection_generation,
		UINT32_C(51));
	UT_ASSERT_EQ(first_dispatch.common.base_authority_generation,
		UINT64_C(0));
	UT_ASSERT_EQ(first_dispatch.common.authority_generation, UINT64_C(0));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_wait_exact(
					 &assertion, 0, 17, 31, 78, 51, 61, UINT64_C(50), UINT64_MAX - 1, 1),
				 RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_wait_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50), UINT64_MAX - 1, 0),
				 RESOURCE_X_APPLY_INVALID);

	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(2000), (UINT64_C(2000)) - (UINT64_C(149)),
		UINT64_C(149), UINT64_C(50), false, 0, &retry_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(2000), (UINT64_C(2000)) - (UINT64_C(150)),
		UINT64_C(150), UINT64_C(50), false, 0, &retry_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action,
		RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT(memcmp(&first_dispatch, &retry_dispatch,
		sizeof(first_dispatch)) == 0);

	ack = make_resource_x_bootstrap_ack_values(
		&first_dispatch, UINT64_C(9), UINT32_C(71));
	for (caller = 0; caller < 4; caller++) {
		action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
			&ack, 0, 61, 77, UINT64_C(160),
			caller == 0 ? &first_assertion : &retry_assertion);
		UT_ASSERT_EQ(action,
			caller == 0
				? RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT
				: RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	}
	/* The caller samples time before taking entry_lock.  An exact ACK may
	 * advance last_dispatch under that lock between the sample and the step;
	 * this is newer same-round progress, not a clock regression or drift. */
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(2000), (UINT64_C(2000)) - (UINT64_C(159)),
		UINT64_C(159), UINT64_C(50), false, 0, &retry_assertion, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	UT_ASSERT_EQ(first_assertion.kind, RESOURCE_X_WIRE_ASSERT_X);
	UT_ASSERT_EQ(first_assertion.common.base_authority_generation,
		UINT64_C(9));
	UT_ASSERT_EQ(first_assertion.common.authority_generation, UINT64_C(9));
	UT_ASSERT_EQ(first_assertion.common.assertion_sequence, UINT64_C(1));
	UT_ASSERT_EQ(first_assertion.common.sender_connection_generation,
		UINT32_C(51));

	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(2000), (UINT64_C(2000)) - (UINT64_C(209)),
		UINT64_C(209), UINT64_C(50), false, 0, &retry_assertion, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(2000), (UINT64_C(2000)) - (UINT64_C(210)),
		UINT64_C(210), UINT64_C(50), false, 0, &retry_assertion, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	UT_ASSERT(memcmp(&first_assertion, &retry_assertion,
		sizeof(first_assertion)) == 0);

	/* R9 T3 retires the active ledger but keeps node X cached.  The same
	 * requester round becomes the only complete ref cover for later local
	 * writers; the retired generation floor by itself is insufficient. */
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	expected_ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&expected_ref),
		RESOURCE_X_APPLY_APPLIED);
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 91;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(
		&expected_ref, &install), RESOURCE_X_APPLY_APPLIED);
	memset(&activation, 0, sizeof(activation));
	activation.ownership_generation = 91;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(
		&expected_ref, &activation), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_publish_terminal_exact(
			&expected_ref, 31, 77, 91, 10, UINT64_C(215)),
		RESOURCE_X_APPLY_APPLIED);
	memset(&failure_snapshot, 0, sizeof(failure_snapshot));
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&failure_snapshot),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(failure_snapshot.ref.acquisition_generation, UINT64_C(1));
	UT_ASSERT_EQ(failure_snapshot.base_authority_generation, UINT64_C(9));
	UT_ASSERT_EQ(failure_snapshot.authority_generation, UINT64_C(10));
	UT_ASSERT_EQ(failure_snapshot.buffer_ownership_generation, UINT64_C(91));
	UT_ASSERT_EQ(failure_snapshot.absolute_deadline_us, UINT64_C(1100));
	UT_ASSERT_EQ(failure_snapshot.terminal, 1);
	memset(&failure_snapshot, 0xa5, sizeof(failure_snapshot));
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
			&assertion, 0, 17, 32, 77, 51, 61, UINT64_C(50),
			&failure_snapshot),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(failure_snapshot.ref.acquisition_generation, UINT64_C(0));
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 91));
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 32, 77, 91));
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 78, 91));
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 92));

	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(2000), (UINT64_C(2000)) - (UINT64_C(220)),
		UINT64_C(220), UINT64_C(50), true, 91, &retry_assertion, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL);
	UT_ASSERT(memcmp(&terminal_ref, &expected_ref,
		sizeof(terminal_ref)) == 0);

	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(2000), (UINT64_C(2000)) - (UINT64_C(220)),
		UINT64_C(220), UINT64_C(50), true, 91, &retry_assertion, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL);
	UT_ASSERT(memcmp(&terminal_ref, &expected_ref,
		sizeof(terminal_ref)) == 0);

	memset(&terminal_ref, 0xff, sizeof(terminal_ref));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(2000), (UINT64_C(2000)) - (UINT64_C(221)),
		UINT64_C(221), UINT64_C(50), true, 92, &retry_assertion, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
	UT_ASSERT_EQ(terminal_ref.acquisition_generation, UINT64_C(0));

	/* Losing node X clears only the joinable binding.  The next acquisition
	 * advances past the retained/retired floor.  A mismatching follower cannot
	 * clear that request, even if it presents the mismatch repeatedly. */
	cluster_pcm_lock_release(tag);
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 91));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(2000), (UINT64_C(2000)) - (UINT64_C(230)),
		UINT64_C(230), UINT64_C(50), false, 0, &retry_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(retry_dispatch.common.assertion_sequence, UINT64_C(2));

	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 32, 77, 51, 61, UINT64_C(2000), (UINT64_C(2000)) - (UINT64_C(231)),
		UINT64_C(231), UINT64_C(50), false, 0, &retry_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 32, 77, 51, 61, UINT64_C(2000), (UINT64_C(2000)) - (UINT64_C(232)),
		UINT64_C(232), UINT64_C(50), false, 0, &retry_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(2000), (UINT64_C(2000)) - (UINT64_C(280)),
		UINT64_C(280), UINT64_C(50), false, 0, &retry_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(retry_dispatch.common.assertion_sequence, UINT64_C(2));

	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 32, 77, 51, 61, UINT64_C(3000), (UINT64_C(3000)) - (UINT64_C(2000)),
		UINT64_C(2000), UINT64_C(50), false, 0, &retry_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
}

UT_TEST(test_resource_x_ack_advances_lease_but_replays_do_not)
{
	BufferTag tag = make_tag(480);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request, ack, dispatch;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBootstrapRoundFailureSnapshot before, after;
	ResourceXGateSnapshot gate;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 1000, (1000) - (100), 100, 50, false, 0,
					 &request, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(&request, 9, 71);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(&ack, 0, 61, 77, 900,
																			  &dispatch),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, &before),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(before.absolute_deadline_us, UINT64_C(1800));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(&ack, 0, 61, 77, 950,
																			  &dispatch),
				 RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, &after),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 2000, (2000) - (1001), 1001, 50, false, 0,
					 &dispatch, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 2000, (2000) - (1800), 1800, 50, false, 0,
					 &dispatch, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
	UT_ASSERT_EQ(gate.phase, RESOURCE_X_GATE_OPEN);
}

UT_TEST(test_resource_x_exact_ack_sample_before_retry_is_not_failure)
{
	BufferTag tag = make_tag(481);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request, ack, dispatch;
	ResourceXAcquisitionRef terminal_ref;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 1000, (1000) - (100), 100, 50, false, 0,
					 &request, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(&request, 9, 71);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 1000, (1000) - (200), 200, 50, false, 0,
					 &dispatch, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(&ack, 0, 61, 77, 199,
																			  &dispatch),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
}

static ResourceXDecodedFrame wait_race_ack;

static void
accept_ack_during_wait_registration(void)
{
	ResourceXDecodedFrame dispatch;

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
					 &wait_race_ack, 0, 61, 77, fake_pcm_clock_us + 1, &dispatch),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
}

UT_TEST(test_resource_x_round_wait_rechecks_progress_and_clips_head_lease)
{
	BufferTag tag = make_tag(483);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXAcquisitionRef terminal;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_pcm_clock_us = 100000;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 500000, 5000, 100000, 2000, false, 0,
					 &request, &terminal),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	fake_pcm_clock_us = 104000;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_wait_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 2000, UINT64_MAX - 1, 50),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(fake_cv_timed_sleep_timeout, 1);
	UT_ASSERT_EQ(fake_cv_sleep_count, 1);
	wait_race_ack = make_resource_x_bootstrap_ack_values(&request, 9, 71);
	fake_cv_prepare_hook = accept_ack_during_wait_registration;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_wait_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 2000, UINT64_MAX - 1, 50),
				 RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(fake_cv_sleep_count, 1);
	UT_ASSERT_EQ(fake_cv_prepare_count, fake_cv_cancel_count);
}

UT_TEST(test_resource_x_reply_wait_metrics_are_shared_and_bucket_exact)
{
	ClusterUndoBlock0ReplyWaitStats stats;
	int site;
	int metric;

	reset_fake_pcm_runtime(4);
	UT_ASSERT(cluster_undo_block0_reply_wait_stats_snapshot(&stats));
	for (site = 0; site < CLUSTER_UNDO_BLOCK0_WAIT_SITE_COUNT; site++)
		for (metric = 0; metric < CLUSTER_UNDO_BLOCK0_WAIT_METRIC_COUNT; metric++)
			UT_ASSERT_EQ(stats.count[site][metric], UINT64_C(0));
	cluster_undo_block0_reply_wait_metric_add(CLUSTER_UNDO_BLOCK0_WAIT_TT_ACTIVE_ACQUIRE,
											  CLUSTER_UNDO_BLOCK0_WAIT_CV);
	cluster_undo_block0_reply_wait_metric_add(CLUSTER_UNDO_BLOCK0_WAIT_TT_ACTIVE_ACQUIRE,
											  CLUSTER_UNDO_BLOCK0_WAIT_CV);
	cluster_undo_block0_reply_wait_metric_add(CLUSTER_UNDO_BLOCK0_WAIT_SITE_COUNT,
											  CLUSTER_UNDO_BLOCK0_WAIT_CV);
	UT_ASSERT(cluster_undo_block0_reply_wait_stats_snapshot(&stats));
	for (site = 0; site < CLUSTER_UNDO_BLOCK0_WAIT_SITE_COUNT; site++)
		for (metric = 0; metric < CLUSTER_UNDO_BLOCK0_WAIT_METRIC_COUNT; metric++)
			UT_ASSERT_EQ(stats.count[site][metric],
						 (uint64)(site == CLUSTER_UNDO_BLOCK0_WAIT_TT_ACTIVE_ACQUIRE
										  && metric == CLUSTER_UNDO_BLOCK0_WAIT_CV
									  ? 2
									  : 0));
}

UT_TEST(test_resource_x_head_budget_and_owned_progress_are_independent)
{
	BufferTag tag = make_tag(482);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request, ack, dispatch;
	ResourceXAcquisitionRef ref, terminal;
	ResourceXBootstrapRoundFailureSnapshot before, after;
	ClusterPcmOwnSnapshot pending;
	ResourceXBufferInstallProof installed;
	ResourceXTargetInstallContinuation follower;
	ResourceXBufferActivationProof activated;
	ResourceXLocalOwnerHandle recycler;
	uint64 terminal_head_deadline;
	uint64 terminal_probe_r4;
	uint64 observed_lease;

	reset_fake_pcm_runtime(4);
	fake_pcm_clock_us = 100;
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	/* A short-lived first caller does not define the node's progress lease. */
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
			&assertion, 0, 17, 31, 77, 51, 61, 200, 1000, 100, 50, false, 0, &request, &terminal),
		RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, &before),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(before.head_change_generation, UINT64_C(1));
	UT_ASSERT_EQ(before.absolute_deadline_us, UINT64_C(1100));
	UT_ASSERT_EQ(before.head_no_progress_budget_us, UINT64_C(1000));
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
			&assertion, 0, 17, 31, 77, 51, 61, 2000, 1, 110, 75, false, 0, &dispatch, &terminal),
		RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
			&assertion, 0, 17, 31, 77, 51, 61, 200, 1, 200, 75, false, 0, &dispatch, &terminal),
		RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, &after),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
	ack = make_resource_x_bootstrap_ack_values(&request, 9, 71);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(&ack, 0, 61, 77, 250,
																			  &dispatch),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	fake_pcm_clock_us = 300;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, &before),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(before.head_change_generation, UINT64_C(3));
	UT_ASSERT_EQ(before.absolute_deadline_us, UINT64_C(1300));
	fake_pcm_clock_us = 350;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref), RESOURCE_X_APPLY_DUPLICATE);
	cluster_pcm_lock_resource_x_publish_no_progress_exact(&ref, RESOURCE_X_NO_PROGRESS_BUFFER_BUSY);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, &after),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(after.absolute_deadline_us, before.absolute_deadline_us);
	UT_ASSERT_EQ(after.head_change_generation, before.head_change_generation + 1);
	before = after;
	cluster_pcm_lock_resource_x_publish_no_progress_exact(&ref, RESOURCE_X_NO_PROGRESS_BUFFER_BUSY);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, &after),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_rearm_exact(&ref), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, &after),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(after.head_change_generation, before.head_change_generation + 1);
	UT_ASSERT_EQ(after.absolute_deadline_us, before.absolute_deadline_us);
	before = after;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_rearm_exact(&ref),
				 RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, &after),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
	memset(&pending, 0, sizeof(pending));
	pending.tag = tag;
	pending.pcm_state = (uint8)PCM_STATE_N;
	pending.flags = PCM_OWN_FLAG_GRANT_PENDING;
	pending.generation = 3;
	pending.reservation_token = 77;
	fake_pcm_clock_us = 400;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_bind_t1_exact(&ref, &pending),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, &before),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(before.head_change_generation, UINT64_C(6));
	UT_ASSERT_EQ(before.absolute_deadline_us, UINT64_C(1400));
	fake_pcm_clock_us = 450;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_bind_t1_exact(&ref, &pending),
				 RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, &after),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
	memset(&installed, 0, sizeof(installed));
	installed.ownership_generation = 4;
	installed.writer_activation_token = 78;
	installed.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, 4000, &pending, &follower),
				 RESOURCE_X_TARGET_INSTALL_INFLIGHT);
	fake_pcm_clock_us = 500;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(&ref, &installed),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, &before),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(before.head_change_generation, UINT64_C(7));
	UT_ASSERT_EQ(before.absolute_deadline_us, UINT64_C(1500));
	/* Progress happened after capture but before enrollment.  The CV wait
	 * must compare the registered predicate version before choosing to sleep. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
					 &follower, RESOURCE_X_TARGET_INSTALL_INFLIGHT, 1),
				 RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(fake_cv_sleep_count, 0);
	fake_pcm_clock_us = 550;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(&ref, &installed),
				 RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, &after),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
	memset(&activated, 0, sizeof(activated));
	activated.ownership_generation = 4;
	fake_pcm_clock_us = 600;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(&ref, &activated),
				 RESOURCE_X_APPLY_APPLIED);
	/* A concurrent retransmit cannot invalidate an earlier sampled terminal
	 * timestamp: pacing is not progress or terminal identity. */
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
			&assertion, 0, 17, 31, 77, 51, 61, 4000, 1000, 650, 50, false, 0, &dispatch, &terminal),
		RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_publish_terminal_exact(&ref, 31, 77, 4,
																					10, 640),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, &before),
				 RESOURCE_X_APPLY_APPLIED);
	terminal_head_deadline = before.absolute_deadline_us;
	/* A terminal cover has no lease: even the direct-init admission probe
	 * remains read-only and succeeds beyond the historical progress stamp. */
	fake_pcm_clock_us = terminal_head_deadline + 100;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_direct_init_join_budget_exact(
					 &assertion, 3, 77, fake_pcm_clock_us, &terminal_probe_r4, &observed_lease),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(terminal_probe_r4, UINT64_C(77));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_begin_exact(&ref, 31, 77, 4, 77, 7,
																	 fake_pcm_clock_us, &recycler),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, &after),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(after.head_change_generation, before.head_change_generation + 1);
	UT_ASSERT_EQ(after.absolute_deadline_us, terminal_head_deadline);
	before = after;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_cancel_exact(&recycler),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, &after),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(after.head_change_generation, before.head_change_generation + 1);
	UT_ASSERT_EQ(after.absolute_deadline_us, terminal_head_deadline);
	before = after;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_cancel_exact(&recycler),
				 RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, &after),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
}

UT_TEST(test_resource_x_unbound_claim_wait_cannot_bypass_head_expiry)
{
	BufferTag tag = make_tag(485);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXAcquisitionRef terminal;
	ResourceXBootstrapRoundFailureSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
			&assertion, 0, 17, 31, 77, 51, 61, 1000, 5000, 100, 50, false, 0, &request, &terminal),
		RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 9000, 5000, 5100, 50, 3, 77, false, 0,
					 &request, &terminal),
				 RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, &snapshot),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.head_failure_reason, RESOURCE_X_HEAD_NO_PROGRESS_EXPIRED);
	UT_ASSERT_EQ(snapshot.round_phase, UINT8_C(5)); /* FAILED_CLOSED, not global fuse. */
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_open_exact(17));
}

UT_TEST(test_resource_x_node_fanin_ignores_caller_deadline_and_retry_slice)
{
	BufferTag tag = make_tag(470);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame dispatch;
	ResourceXAcquisitionRef terminal_ref;
	char before[sizeof(fake_pcm_entries.data)];

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 1000, (1000) - (100), 100, 50, false, 0,
					 &request, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	memcpy(before, fake_pcm_entries.data, sizeof(before));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 2000, (2000) - (110), 110, 75, false, 0,
					 &dispatch, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	UT_ASSERT_EQ(memcmp(before, fake_pcm_entries.data, sizeof(before)), 0);
	UT_ASSERT_EQ(dispatch.common.assertion_sequence, 0);
	/* The first head's pacing controls an identical retransmit, not attempt 2. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 2000, (2000) - (150), 150, 75, false, 0,
					 &dispatch, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(memcmp(&request, &dispatch, sizeof(request)), 0);
}

UT_TEST(test_resource_x_expired_follower_cannot_cancel_live_head)
{
	BufferTag tag = make_tag(471);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame dispatch;
	ResourceXAcquisitionRef terminal_ref;
	char before[sizeof(fake_pcm_entries.data)];

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 1000, (1000) - (100), 100, 50, false, 0,
					 &request, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	memcpy(before, fake_pcm_entries.data, sizeof(before));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 120, (120) - (130), 130, 50, false, 0,
					 &dispatch, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
	UT_ASSERT_EQ(memcmp(before, fake_pcm_entries.data, sizeof(before)), 0);
	UT_ASSERT_EQ(terminal_ref.acquisition_generation, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 1000, (1000) - (140), 140, 50, false, 0,
					 &dispatch, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
}

UT_TEST(test_resource_x_post_ack_rejected_follower_preserves_shared_head)
{
	BufferTag tag = make_tag(472);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame dispatch;
	ResourceXAcquisitionRef terminal_ref;
	char before[sizeof(fake_pcm_entries.data)];

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 1000, (1000) - (100), 100, 50, 0, 5, false,
					 0, &request, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(&request, 9, 71);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(&ack, 0, 61, 77, 105,
																			  &dispatch),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	memcpy(before, fake_pcm_entries.data, sizeof(before));
	/* A token-less ordinary follower may join the node's direct-init head. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 2000, (2000) - (110), 110, 75, false, 0,
					 &dispatch, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	UT_ASSERT_EQ(memcmp(before, fake_pcm_entries.data, sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_join_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 2000, (2000) - (111), 111, 75, 0, 5, false,
					 0, &dispatch, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	UT_ASSERT_EQ(memcmp(before, fake_pcm_entries.data, sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 2000, (2000) - (112), 112, 75, 0, 6, false,
					 0, &dispatch, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
	UT_ASSERT_EQ(memcmp(before, fake_pcm_entries.data, sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
					 &assertion, 0, 17, 32, 77, 51, 61, 2000, (2000) - (113), 113, 75, false, 0,
					 &dispatch, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
	UT_ASSERT_EQ(memcmp(before, fake_pcm_entries.data, sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 1000, (1000) - (114), 114, 50, 0, 5, false,
					 0, &dispatch, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
}

UT_TEST(test_resource_x_t1_install_claim_binds_only_exact_physical_reservation)
{
	BufferTag tag = make_tag(279);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame dispatch;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXAcquisitionRef ref;
	ResourceXAcquisitionRef wrong_ref;
	ClusterPcmOwnSnapshot reserved;
	ClusterPcmOwnSnapshot conflict;
	ResourceXTargetInstallContinuation follow;
	ResourceXTargetInstallContinuation unbound_follow;
	ResourceXBufferInstallProof installed;
	ResourceXBufferActivationProof activation;
	uint8 claim_source;
	uint64 claim_generation;
	uint64 claim_token;
	unsigned char before[sizeof(fake_pcm_entries)];

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 1000, (1000) - (100), 100, 50, false, 0,
					 &request, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(&request, 9, 71);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(&ack, 0, 61, 77, 110,
																			  &dispatch),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_snapshot_exact(
					 &ref, &claim_source, &claim_generation, &claim_token),
				 RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(claim_source, RESOURCE_X_INSTALL_CLAIM_NONE);
	UT_ASSERT_EQ(claim_generation, UINT64_C(0));
	UT_ASSERT_EQ(claim_token, UINT64_C(0));
	memset(&reserved, 0, sizeof(reserved));
	reserved.tag = tag;
	reserved.pcm_state = (uint8)PCM_STATE_N;
	reserved.flags = PCM_OWN_FLAG_GRANT_PENDING;
	reserved.generation = 7;
	reserved.reservation_token = 5;
	memcpy(before, &fake_pcm_entries, sizeof(before));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_bind_t1_exact(&ref, &reserved),
				 RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(before, &fake_pcm_entries, sizeof(before)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 75, 2000, &reserved, &unbound_follow),
				 RESOURCE_X_TARGET_INSTALL_INFLIGHT);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_bind_t1_exact(&ref, &reserved),
				 RESOURCE_X_APPLY_APPLIED);
	memset(&terminal_ref, 0xa5, sizeof(terminal_ref));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_target_install_classify_exact(
					 &unbound_follow, &reserved, &terminal_ref),
				 RESOURCE_X_TARGET_INSTALL_RESAMPLE);
	UT_ASSERT_EQ(terminal_ref.acquisition_generation, UINT64_C(0));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_snapshot_exact(
					 &ref, &claim_source, &claim_generation, &claim_token),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(claim_source, RESOURCE_X_INSTALL_CLAIM_ORDINARY_T1);
	UT_ASSERT_EQ(claim_generation, UINT64_C(7));
	UT_ASSERT_EQ(claim_token, UINT64_C(5));
	memcpy(before, &fake_pcm_entries, sizeof(before));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_bind_t1_exact(&ref, &reserved),
				 RESOURCE_X_APPLY_DUPLICATE);
	conflict = reserved;
	conflict.reservation_token--;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_bind_t1_exact(&ref, &conflict),
				 RESOURCE_X_APPLY_STALE);
	conflict.reservation_token += 2;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_bind_t1_exact(&ref, &conflict),
				 RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	conflict = reserved;
	conflict.generation++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_bind_t1_exact(&ref, &conflict),
				 RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	wrong_ref = ref;
	wrong_ref.acquisition_generation++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_bind_t1_exact(&wrong_ref, &reserved),
				 RESOURCE_X_APPLY_STALE);
	conflict = reserved;
	conflict.writer_activation_token = 5;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_bind_t1_exact(&ref, &conflict),
				 RESOURCE_X_APPLY_INVALID);
	UT_ASSERT(memcmp(before, &fake_pcm_entries, sizeof(before)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_join_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 2000, (2000) - (111), 111, 75, 7, 5, false,
					 0, &dispatch, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	UT_ASSERT(memcmp(before, &fake_pcm_entries, sizeof(before)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 75, 2000, &reserved, &follow),
				 RESOURCE_X_TARGET_INSTALL_INFLIGHT);
	conflict = reserved;
	conflict.generation += 2;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 75, 2000, &conflict, &follow),
				 RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	UT_ASSERT(!follow.valid);
	UT_ASSERT(memcmp(before, &fake_pcm_entries, sizeof(before)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 75, 2000, &reserved, &follow),
				 RESOURCE_X_TARGET_INSTALL_INFLIGHT);
	UT_ASSERT_EQ(follow.observed_claim_pending_generation, UINT64_C(7));
	UT_ASSERT_EQ(follow.observed_claim_reservation_token, UINT64_C(5));
	UT_ASSERT_EQ(follow.capture_flags & RESOURCE_X_TARGET_INSTALL_CAPTURE_DIRECT_INIT, 0);
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_direct_init_matches_exact(&ref, 7, 5));
	memset(&installed, 0, sizeof(installed));
	installed.ownership_generation = 8;
	installed.writer_activation_token = 5;
	installed.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(&ref, &installed),
				 RESOURCE_X_APPLY_APPLIED);
	memset(&activation, 0, sizeof(activation));
	activation.ownership_generation = 8;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(&ref, &activation),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_publish_terminal_exact(&ref, 31, 77, 8,
																					10, 120),
				 RESOURCE_X_APPLY_APPLIED);
	reserved.pcm_state = (uint8)PCM_STATE_X;
	reserved.flags = 0;
	reserved.generation = 8;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_target_install_classify_exact(
					 &follow, &reserved, &terminal_ref),
				 RESOURCE_X_TARGET_INSTALL_TERMINAL);
	UT_ASSERT(memcmp(&terminal_ref, &ref, sizeof(ref)) == 0);
}

UT_TEST(test_resource_x_direct_claim_joins_unbound_head_only_after_complete_beb)
{
	BufferTag tag = make_tag(280);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame dispatch;
	ResourceXAcquisitionRef ref;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXInstallClaimJoinObservation observation;
	ResourceXInstallClaimJoinObservation stale;
	ClusterPcmOwnSnapshot before;
	ClusterPcmOwnSnapshot after;
	unsigned char unchanged[sizeof(fake_pcm_entries)];

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 1000, (1000) - (100), 100, 50, false, 0,
					 &request, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	memset(&before, 0, sizeof(before));
	before.tag = tag;
	before.generation = 7;
	before.reservation_token = 5;
	before.flags = PCM_OWN_FLAG_GRANT_PENDING;
	before.pcm_state = (uint8)PCM_STATE_N;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_join_observe_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, &observation),
				 RESOURCE_X_APPLY_APPLIED);
	after = before;
	after.writer_activation_token = 1;
	memcpy(unchanged, &fake_pcm_entries, sizeof(unchanged));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_bind_direct_init_exact(&observation,
																				  &before, &after),
				 RESOURCE_X_APPLY_STALE);
	after = before;
	after.semantic_buf_state ^= BM_DIRTY;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_bind_direct_init_exact(&observation,
																				  &before, &after),
				 RESOURCE_X_APPLY_STALE);
	after = before;
	after.buffer_type++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_bind_direct_init_exact(&observation,
																				  &before, &after),
				 RESOURCE_X_APPLY_STALE);
	after = before;
	stale = observation;
	stale.entry_binding_generation++;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_install_claim_bind_direct_init_exact(&stale, &before, &after),
		RESOURCE_X_APPLY_STALE);
	stale = observation;
	stale.request.master_session_incarnation++;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_install_claim_bind_direct_init_exact(&stale, &before, &after),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(unchanged, &fake_pcm_entries, sizeof(unchanged)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_bind_direct_init_exact(&observation,
																				  &before, &after),
				 RESOURCE_X_APPLY_APPLIED);
	memcpy(unchanged, &fake_pcm_entries, sizeof(unchanged));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_bind_direct_init_exact(&observation,
																				  &before, &after),
				 RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_join_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 2000, (2000) - (110), 110, 75, 7, 5, false,
					 0, &dispatch, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	UT_ASSERT(memcmp(unchanged, &fake_pcm_entries, sizeof(unchanged)) == 0);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_discard_pre_assert_authority_drift_exact(
			&request, 0, 77, 61, 50, 1000, 7, 5),
		RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(unchanged, &fake_pcm_entries, sizeof(unchanged)) == 0);
	/* A T1 that won before the bind owns the installation.  The follower
	 * must wait for its ordinary claim, not attach a convenient direct token. */
	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 1000, (1000) - (200), 200, 50, false, 0,
					 &request, &terminal_ref),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ref.acquisition_generation = request.common.assertion_sequence;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_join_observe_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, &observation),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref), RESOURCE_X_APPLY_APPLIED);
	memcpy(unchanged, &fake_pcm_entries, sizeof(unchanged));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_bind_direct_init_exact(&observation,
																				  &before, &after),
				 RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(unchanged, &fake_pcm_entries, sizeof(unchanged)) == 0);
}

UT_TEST(test_resource_x_requester_round_blocks_local_s_only_until_x_cached)
{
	BufferTag tag = make_tag(469);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame asserted;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXAcquisitionRef expected_ref;
	ResourceXBufferInstallProof install;
	ResourceXBufferActivationProof activation;
	ResourceXBootstrapRoundAction action;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	UT_ASSERT(!cluster_pcm_lock_resource_x_requester_s_barrier_active_exact(
		&tag));

	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), false, 0, &request, &terminal_ref);
	UT_ASSERT_EQ(action,
		RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT(cluster_pcm_lock_resource_x_requester_s_barrier_active_exact(
		&tag));

	ack = make_resource_x_bootstrap_ack_values(
		&request, UINT64_C(9), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, UINT64_C(150), &asserted);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	UT_ASSERT(cluster_pcm_lock_resource_x_requester_s_barrier_active_exact(
		&tag));

	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	expected_ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&expected_ref),
		RESOURCE_X_APPLY_APPLIED);
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 91;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(
		&expected_ref, &install), RESOURCE_X_APPLY_APPLIED);
	memset(&activation, 0, sizeof(activation));
	activation.ownership_generation = 91;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(
		&expected_ref, &activation), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_publish_terminal_exact(
			&expected_ref, 31, 77, 91, 10, UINT64_C(160)),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!cluster_pcm_lock_resource_x_requester_s_barrier_active_exact(
		&tag));
}

UT_TEST(test_resource_x_bootstrap_expired_round_allows_only_fresh_acquisition)
{
	BufferTag tag = make_tag(268);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame first_request;
	ResourceXDecodedFrame first_ack;
	ResourceXDecodedFrame first_assertion;
	ResourceXDecodedFrame retained_grant;
	ResourceXDecodedFrame retained_image;
	ResourceXDecodedFrame second_request;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXRequesterJoinSnapshot join;
	ResourceXBootstrapRoundAction action;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), false, 0, &first_request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(first_request.common.assertion_sequence, UINT64_C(1));
	first_ack = make_resource_x_bootstrap_ack_values(
		&first_request, UINT64_C(9), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&first_ack, 0, 61, 77, UINT64_C(150), &first_assertion);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);

	/* The first caller expires at 1000; ACK advances the head lease to 1050.
	 * Head expiry is terminal for
	 * that acquisition, and an equal or already-expired caller deadline can
	 * neither refresh it nor allocate a second attempt. */
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(1000)),
		UINT64_C(1000), UINT64_C(50), false, 0, &second_request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(1051)),
		UINT64_C(1051), UINT64_C(50), false, 0, &second_request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);

	/* Caller expiry above is zero-mutation.  A live driver observes the head's
	 * own expiry before any later independent acquisition can rearm it. */
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(2000), (UINT64_C(2000)) - (UINT64_C(1051)),
		UINT64_C(1051), UINT64_C(50), false, 0, &second_request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);

	/* A later independent acquisition may retire only the expired local
	 * binding and allocate a strictly higher attempt.  It starts with a clean
	 * request: no old base, ACK, assertion, proof, or deadline is reused. */
	memset(&second_request, 0xa5, sizeof(second_request));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(2000), (UINT64_C(2000)) - (UINT64_C(1051)),
		UINT64_C(1051), UINT64_C(50), false, 0, &second_request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(second_request.common.assertion_sequence, UINT64_C(2));
	UT_ASSERT_EQ(second_request.common.base_authority_generation, UINT64_C(0));
	UT_ASSERT_EQ(second_request.common.authority_generation, UINT64_C(0));
	UT_ASSERT_EQ(second_request.common.outcome, RESOURCE_X_OUTCOME_NONE);

	/* Creation-only direct-init identity is never reinterpreted as a fresh
	 * ordinary acquisition merely because a later deadline is presented. */
	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), 0, 5, false, 0, &first_request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(1000)),
		UINT64_C(1000), UINT64_C(50), 0, 5, false, 0, &second_request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(2000), (UINT64_C(2000)) - (UINT64_C(1051)),
		UINT64_C(1051), UINT64_C(50), 0, 5, false, 0, &second_request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);

	/* A failed ordinary round is not locally empty while any grant/image half
	 * remains retained for the same requester.  Expiry cannot discard that
	 * no-half-open debt or let a higher attempt reuse the entry. */
	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), false, 0, &first_request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	first_ack = make_resource_x_bootstrap_ack_values(
		&first_request, UINT64_C(9), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&first_ack, 0, 61, 77, UINT64_C(150), &first_assertion);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	make_resource_x_remote_join_pair(
		tag, 1, &retained_grant, &retained_image);
	retarget_resource_x_remote_join_pair(
		&retained_grant, &retained_image, UINT64_C(11),
		first_assertion.common.assertion_sequence);
	retained_grant.common.base_authority_generation
		= first_assertion.common.base_authority_generation;
	retained_grant.common.ordered_lane
		= first_assertion.common.ordered_lane;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&retained_grant, 0, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(join.flags, RESOURCE_X_REQUESTER_JOIN_HAS_GRANT);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(1000)),
		UINT64_C(1000), UINT64_C(50), false, 0, &second_request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(2000), (UINT64_C(2000)) - (UINT64_C(1051)),
		UINT64_C(1051), UINT64_C(50), false, 0, &second_request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
}

UT_TEST(test_resource_x_bootstrap_round_binds_exact_direct_init_reservation)
{
	BufferTag tag = make_tag(153);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXAcquisitionRef expected_ref;
	ResourceXBootstrapRoundAction action;
	uint64 direct_generation = UINT64_MAX;
	uint64 direct_token = UINT64_MAX;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(10000), (UINT64_C(10000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), 0, 5, false, 0, &request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	expected_ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_direct_init_matches_exact(
		&expected_ref, 0, 5));
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_direct_init_snapshot_exact(
		&expected_ref, &direct_generation, &direct_token));
	UT_ASSERT_EQ(direct_generation, UINT64_C(0));
	UT_ASSERT_EQ(direct_token, UINT64_C(0));

	ack = make_resource_x_bootstrap_ack_values(
		&request, UINT64_C(9), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, UINT64_C(110), &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_direct_init_matches_exact(
		&expected_ref, 0, 5));
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_direct_init_snapshot_exact(
		&expected_ref, &direct_generation, &direct_token));
	UT_ASSERT_EQ(direct_generation, UINT64_C(0));
	UT_ASSERT_EQ(direct_token, UINT64_C(5));
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_direct_init_matches_exact(
		&expected_ref, 1, 5));
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_direct_init_matches_exact(
		&expected_ref, 0, 6));
}

UT_TEST(test_resource_x_pre_assert_authority_drift_discards_exact_round_only)
{
	BufferTag tag = make_tag(216);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame first_request;
	ResourceXDecodedFrame second_request;
	ResourceXDecodedFrame first_ack;
	ResourceXDecodedFrame second_ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBootstrapRoundAction action;
	ResourceXApplyResult result;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(10000), (UINT64_C(10000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), false, 0, &first_request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(first_request.common.assertion_sequence, UINT64_C(1));

	/* A successful request enqueue may already have installed the master's
	 * non-authority RECEIVED receipt.  Before any ACK is accepted or ASSERT is
	 * dispatched, exact authority drift still discards the whole requester
	 * round; A1.1 requires a higher attempt to replace that old receipt. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&first_request, 1, 61, 77, 31, 71, &first_ack),
		RESOURCE_X_APPLY_APPLIED);
	result = cluster_pcm_lock_resource_x_bootstrap_round_discard_pre_assert_authority_drift_exact(
		&first_request, 0, 77, 61, UINT64_C(50), UINT64_C(10000), 0, 0);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	result = cluster_pcm_lock_resource_x_bootstrap_round_discard_pre_assert_authority_drift_exact(
		&first_request, 0, 77, 61, UINT64_C(50), UINT64_C(10000), 0, 0);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_NOT_FOUND);

	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 32, 77, 52, 62, UINT64_C(10000), (UINT64_C(10000)) - (UINT64_C(101)),
		UINT64_C(101), UINT64_C(50), false, 0, &second_request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(second_request.common.assertion_sequence, UINT64_C(2));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&second_request, 1, 62, 77, 32, 72, &second_ack),
		RESOURCE_X_APPLY_APPLIED);

	/* Old bytes and the old non-authority ACK cannot bind or clear the rebound
	 * round.  Only the replacement receipt's exact ACK may advance ASSERT. */
	result = cluster_pcm_lock_resource_x_bootstrap_round_discard_pre_assert_authority_drift_exact(
		&first_request, 0, 77, 61, UINT64_C(50), UINT64_C(10000), 0, 0);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_STALE);
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&first_ack, 0, 62, 77, UINT64_C(110), &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&second_ack, 0, 62, 77, UINT64_C(111), &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	result = cluster_pcm_lock_resource_x_bootstrap_round_discard_pre_assert_authority_drift_exact(
		&second_request, 0, 77, 62, UINT64_C(50), UINT64_C(10000), 0, 0);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_BAD_STATE);
}

UT_TEST(test_resource_x_direct_init_observer_is_join_only_and_keeps_round_deadline)
{
	BufferTag tag = make_tag(215);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame dispatch;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBootstrapRoundAction action;
	ResourceXApplyResult result;
	uint64 frozen_deadline = UINT64_MAX;
	uint64 frozen_r4_generation = UINT64_MAX;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));

	/* A pending BufferDesc sidecar ahead of requester-round creation cannot
	 * let an observer allocate attempt 1 or invent an R7 deadline. */
	result
		= cluster_pcm_lock_resource_x_bootstrap_round_direct_init_join_budget_exact(
			&assertion, 0, 5, UINT64_C(90),
			&frozen_r4_generation, &frozen_deadline);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(frozen_r4_generation, UINT64_C(0));
	UT_ASSERT_EQ(frozen_deadline, UINT64_C(0));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_join_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(10000), (UINT64_C(10000)) - (UINT64_C(90)),
		UINT64_C(90), UINT64_C(50), 0, 5, false, 0, &dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);

	/* The proof-owning path still creates the sole attempt. */
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(10000), (UINT64_C(10000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), 0, 5, false, 0, &dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(dispatch.common.assertion_sequence, UINT64_C(1));

	result
		= cluster_pcm_lock_resource_x_bootstrap_round_direct_init_join_budget_exact(
			&assertion, 0, 5, UINT64_C(101),
			&frozen_r4_generation, &frozen_deadline);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(frozen_r4_generation, UINT64_C(77));
	UT_ASSERT_EQ(frozen_deadline, UINT64_C(10000));

	/* A different caller deadline joins the same attempt and pacing.  A wrong
	 * physical token remains zero-mutation and cannot cancel that request. */
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_join_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(20000), (UINT64_C(20000)) - (UINT64_C(151)),
		UINT64_C(151), UINT64_C(50), 0, 5, false, 0, &dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(dispatch.common.assertion_sequence, UINT64_C(1));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_join_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(10000), (UINT64_C(10000)) - (UINT64_C(151)),
		UINT64_C(151), UINT64_C(50), 0, 6, false, 0, &dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_join_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(10000), (UINT64_C(10000)) - (UINT64_C(151)),
		UINT64_C(151), UINT64_C(50), 0, 5, false, 0, &dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	UT_ASSERT_EQ(dispatch.common.assertion_sequence, UINT64_C(0));
}

UT_TEST(test_resource_x_terminal_local_owner_serializes_recycle_and_revoke)
{
	BufferTag tag = make_tag(155);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXDecodedFrame conflict_block;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame old_block;
	ResourceXDecodedFrame old_image;
	ResourceXDecodedFrame old_status;
	ResourceXDecodedFrame status;
	ResourceXDecodedFrame unused_dispatch;
	ResourceXDecodedFrame successor_block;
	ResourceXAcquisitionRef expected_ref;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBufferInstallProof install;
	ResourceXBufferActivationProof activation;
	ResourceXBootstrapRoundAction action;
	ResourceXApplyResult owner_result;
	ResourceXTerminalXLineage lineage;
	ResourceXTerminalXLineage replay_lineage;
	ResourceXLocalOwnerHandle recycle;
	ResourceXLocalOwnerHandle stale;
	ResourceXLocalOwnerHandle revoke;
	ResourceXIntentSlot old_image_intent;
	ResourceXIntentSlot old_status_intent;
	ClusterPcmOwnSnapshot dropped;
	ClusterPcmOwnSnapshot revoking;
	uint8 old_image_payload[RESOURCE_X_IMAGE_V1_BYTES];
	uint8 old_status_payload[RESOURCE_X_PROOF_V1_BYTES];
	uint64 old_source_generation = 0;

	reset_fake_pcm_runtime(4);
	tag.forkNum = VISIBILITYMAP_FORKNUM;
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), 90, 19, false, 0, &request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(
		&request, UINT64_C(9), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, UINT64_C(110), &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);

	expected_ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&expected_ref),
		RESOURCE_X_APPLY_APPLIED);
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 91;
	install.writer_activation_token = 19;
	install.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(
		&expected_ref, &install), RESOURCE_X_APPLY_APPLIED);
	memset(&activation, 0, sizeof(activation));
	activation.ownership_generation = 91;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(
		&expected_ref, &activation), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_publish_terminal_exact(
			&expected_ref, 31, 77, 91, 10, UINT64_C(120)),
		RESOURCE_X_APPLY_APPLIED);

	successor_block = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCK_TO_N, tag, 2, 1);
	successor_block.payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	successor_block.common.base_authority_generation = UINT64_C(10);
	successor_block.common.authority_generation = UINT64_C(10);
	successor_block.common.assertion_sequence = UINT64_C(42);
	successor_block.common.ordered_lane = 0;
	successor_block.common.observed_mode = (uint8)PCM_STATE_X;
	successor_block.common.target_mode = (uint8)PCM_STATE_N;
	successor_block.common.source_candidate = 1;
	successor_block.common.retain_pi_if_dirty = 1;

	memset(&recycle, 0, sizeof(recycle));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_begin_exact(
		&expected_ref, 31, 77, 91, 19, 7, UINT64_C(100), &recycle),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(recycle.owner_generation, UINT64_C(1));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_begin_exact(
		&expected_ref, 31, 77, 91, 19, 8, UINT64_C(101), &revoke),
		RESOURCE_X_APPLY_BAD_STATE);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(10000), (UINT64_C(10000)) - (UINT64_C(125)),
		UINT64_C(125), UINT64_C(50), true, 91, &unused_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	memset(&lineage, 0, sizeof(lineage));
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_terminal_holder_exact(
		&successor_block, 0, 77, 91, &lineage));
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_direct_init_terminal_holder_exact(
		&successor_block, 0, 77, 91, &lineage));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&successor_block, 0, 77, 91, 19, 9, UINT64_C(110),
		&lineage, &revoke),
		RESOURCE_X_APPLY_BAD_STATE);
	/* The first exact type-17 owns the bounded local priority.  A different
	 * FIFO successor cannot overwrite it while the recycler is still active. */
	conflict_block = successor_block;
	UT_ASSERT(resource_x_assertion_init(&tag, 3,
		&conflict_block.common.logical_assertion));
	conflict_block.common.assertion_sequence = UINT64_C(43);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&conflict_block, 0, 77, 91, 19, 10, UINT64_C(111),
		&lineage, &stale),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&successor_block, 0, 77, 91, 19, 9, UINT64_C(112),
		&lineage, &revoke),
		RESOURCE_X_APPLY_BAD_STATE);

	stale = recycle;
	stale.owner_generation++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_finish_exact(
		&stale, UINT64_C(120)),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_finish_exact(
		&recycle, UINT64_C(121)),
		RESOURCE_X_APPLY_APPLIED);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(10000), (UINT64_C(10000)) - (UINT64_C(126)),
		UINT64_C(126), UINT64_C(50), true, 91, &unused_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_begin_exact(
		&expected_ref, 31, 77, 91, 19, 7, UINT64_C(126), &stale),
		RESOURCE_X_APPLY_BAD_STATE);

	memset(&lineage, 0, sizeof(lineage));
	memset(&revoke, 0, sizeof(revoke));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&successor_block, 0, 77, 91, 19, 9, UINT64_C(127),
		&lineage, &revoke),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(revoke.owner_generation, UINT64_C(2));
	UT_ASSERT_EQ(lineage.holder_attempt, UINT64_C(1));
	memset(&replay_lineage, 0, sizeof(replay_lineage));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_replay_exact(
		&successor_block, 0, 77, 91, UINT64_C(128), &replay_lineage),
		RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(memcmp(&lineage, &replay_lineage, sizeof(lineage)), 0);
	memset(&replay_lineage, 0, sizeof(replay_lineage));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_replay_exact(
		&conflict_block, 0, 77, 91, UINT64_C(128), &replay_lineage),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_replay_exact(
		&successor_block, 0, 78, 91, UINT64_C(128), &replay_lineage),
		RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT(cluster_pcm_lock_resource_x_terminal_x_revoke_revalidate_held_exact(
		&successor_block, 0, 77, 91, &revoke, &lineage));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_begin_exact(
		&expected_ref, 31, 77, 91, 19, 7, UINT64_C(128), &recycle),
		RESOURCE_X_APPLY_BAD_STATE);
	stale = revoke;
	stale.owner_procno++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_yield_exact(
		&stale, UINT64_C(128)), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_yield_exact(
		&revoke, UINT64_C(129)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!cluster_pcm_lock_resource_x_terminal_x_revoke_revalidate_held_exact(
		&successor_block, 0, 77, 91, &revoke, &lineage));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_begin_exact(
		&expected_ref, 31, 77, 91, 19, 7, UINT64_C(130), &recycle),
		RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&conflict_block, 0, 77, 91, 19, 10, UINT64_C(131),
		&lineage, &stale), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&successor_block, 0, 77, 91, 19, 9, UINT64_C(132),
		&lineage, &revoke), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(revoke.owner_generation, UINT64_C(3));
	UT_ASSERT(cluster_pcm_lock_resource_x_terminal_x_revoke_revalidate_held_exact(
		&successor_block, 0, 77, 91, &revoke, &lineage));
	stale = revoke;
	stale.owner_procno++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_release_exact(&stale),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_release_exact(&revoke),
		RESOURCE_X_APPLY_APPLIED);

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_begin_exact(
		&expected_ref, 31, 77, 91, 19, 7, UINT64_C(200), &recycle),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(recycle.owner_generation, UINT64_C(4));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&successor_block, 0, 77, 91, 19, 9, UINT64_C(210),
		&lineage, &revoke), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_finish_exact(
		&recycle, UINT64_C(220)), RESOURCE_X_APPLY_APPLIED);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(10000), (UINT64_C(10000)) - (UINT64_C(1209)),
		UINT64_C(1209), UINT64_C(50), true, 91, &unused_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(10000), (UINT64_C(10000)) - (UINT64_C(1210)),
		UINT64_C(1210), UINT64_C(50), true, 91, &unused_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_begin_exact(
		&expected_ref, 31, 77, 91, 19, 7, UINT64_C(1220), &recycle),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(recycle.owner_generation, UINT64_C(5));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_cancel_exact(&recycle),
		RESOURCE_X_APPLY_APPLIED);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, 10000, (10000) - (1221), 1221, 50, true, 91,
		&unused_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL);

	/* DRAIN-D1: the first authenticated exact type-17 must freeze the same
	 * bounded successor priority even when it claims an EMPTY local owner.
	 * A pre-retention content-BUSY yield therefore returns to HANDOFF rather
	 * than EMPTY: the cached-X target, recycler, and conflicting successor all
	 * remain excluded until the same frame reclaims or the original deadline
	 * expires.  Reclaim/yield at 2299 must not refresh the deadline frozen at
	 * 1300 (the test timeout is 1000ms). */
	memset(&lineage, 0, sizeof(lineage));
	memset(&revoke, 0, sizeof(revoke));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&successor_block, 0, 77, 91, 19, 9, UINT64_C(1300),
		&lineage, &revoke), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(revoke.owner_generation, UINT64_C(6));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&conflict_block, 0, 77, 91, 19, 10, UINT64_C(1300),
		&lineage, &stale), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&successor_block, 0, 77, 91, 19, 9, UINT64_C(1300),
		&lineage, &stale), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_yield_exact(
		&revoke, UINT64_C(1301)), RESOURCE_X_APPLY_APPLIED);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(10000), (UINT64_C(10000)) - (UINT64_C(1302)),
		UINT64_C(1302), UINT64_C(50), true, 91, &unused_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	owner_result = cluster_pcm_lock_resource_x_itl_recycle_begin_exact(
		&expected_ref, 31, 77, 91, 19, 7, UINT64_C(1303), &recycle);
	UT_ASSERT_EQ(owner_result, RESOURCE_X_APPLY_BAD_STATE);
	if (owner_result == RESOURCE_X_APPLY_APPLIED)
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_cancel_exact(
			&recycle), RESOURCE_X_APPLY_APPLIED);
	owner_result = cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&conflict_block, 0, 77, 91, 19, 10, UINT64_C(1304),
		&lineage, &stale);
	UT_ASSERT_EQ(owner_result, RESOURCE_X_APPLY_STALE);
	if (owner_result == RESOURCE_X_APPLY_APPLIED)
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_release_exact(
			&stale), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&successor_block, 0, 77, 91, 19, 9, UINT64_C(2299),
		&lineage, &revoke), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(revoke.owner_generation, UINT64_C(7));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_yield_exact(
		&revoke, UINT64_C(2299)), RESOURCE_X_APPLY_APPLIED);
	owner_result = cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&successor_block, 0, 77, 91, 19, 9, UINT64_C(2300),
		&lineage, &revoke);
	UT_ASSERT_EQ(owner_result, RESOURCE_X_APPLY_STALE);
	if (owner_result == RESOURCE_X_APPLY_APPLIED)
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_release_exact(
			&revoke), RESOURCE_X_APPLY_APPLIED);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(10000), (UINT64_C(10000)) - (UINT64_C(2301)),
		UINT64_C(2301), UINT64_C(50), true, 91, &unused_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL);

	/* REVOKING retains the same local deadline.  An exact duplicate at that
	 * boundary is stale and must not refresh priority or mutate the active
	 * owner; the original handle remains the only legal cleanup identity. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&successor_block, 0, 77, 91, 19, 9, UINT64_C(2400),
		&lineage, &revoke), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(revoke.owner_generation, UINT64_C(8));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_replay_exact(
		&successor_block, 0, 77, 91, UINT64_C(3399), &replay_lineage),
		RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_replay_exact(
		&successor_block, 0, 77, 91, UINT64_C(3400), &replay_lineage),
		RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&successor_block, 0, 77, 91, 19, 9, UINT64_C(3400),
		&lineage, &stale), RESOURCE_X_APPLY_STALE);

	/* A prior VM conversion may be fully DRAINed before replacement removes
	 * and later recreates the BufferDesc.  Its carrier generation therefore
	 * belongs to the old descriptor episode even when the new episode reaches
	 * the same numeric value. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_release_exact(
		&revoke), RESOURCE_X_APPLY_APPLIED);
	fake_gcs_master_node = 0;
	old_block = successor_block;
	UT_ASSERT(resource_x_assertion_init(&tag, 2,
		&old_block.common.logical_assertion));
	old_block.common.assertion_sequence = UINT64_C(41);
	make_resource_x_remote_join_pair(tag, 2, &grant, &old_image);
	retarget_resource_x_remote_join_pair(&grant, &old_image, UINT64_C(12),
		UINT64_C(41));
	old_image.common.ordered_lane = 0;
	set_resource_x_test_source_fence(
		old_image.body.image_envelope.source_fence, UINT64_C(91),
		PCM_STATE_X);
	old_image.body.image_envelope.source_carrier_generation = UINT64_C(92);
	canonicalize_resource_x_test_image(&old_image);
	old_status = make_resource_x_remote_blocked_frame(tag, 2, 1);
	old_status.common = old_block.common;
	old_status.common.source_candidate = 0;
	old_status.common.retain_pi_if_dirty = 0;
	old_status.common.outcome = RESOURCE_X_OUTCOME_OK;
	old_status.common.flags = RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED;
	memcpy(old_status.body.blocked_to_n.source_fence,
		old_image.body.image_envelope.source_fence,
		sizeof(old_status.body.blocked_to_n.source_fence));
	old_status.body.blocked_to_n.source_carrier_generation = UINT64_C(92);
	old_status.body.blocked_to_n.requester_target_generation = UINT64_C(41);
	old_status.body.blocked_to_n.page_scn_lsn
		= old_image.body.image_envelope.page_scn_lsn;
	old_status.body.blocked_to_n.dependency_count
		= old_image.body.image_envelope.dependency_count;
	memcpy(old_status.body.blocked_to_n.dependencies,
		old_image.body.image_envelope.dependencies,
		sizeof(old_status.body.blocked_to_n.dependencies));
	old_status.body.blocked_to_n.source_proof_crc32c
		= old_image.common.semantic_crc32c;
	old_status.body.blocked_to_n.page_checksum
		= old_image.body.image_envelope.page_checksum;
	old_status.body.blocked_to_n.source_disposition
		= RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
	old_status.body.blocked_to_n.proof_kind = RESOURCE_X_PROOF_REMOTE_CARRIER;
	old_status.body.blocked_to_n.holder_connection_generation = 51;
	old_status.body.blocked_to_n.acting_formation = 17;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_source_exact(
		&old_block, 0, &old_status, &old_image), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_publish_exact(
		&old_block.common.logical_assertion,
		old_block.common.assertion_sequence, 0,
		old_block.common.master_session_incarnation),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
		&old_block.common.logical_assertion, &old_status_intent,
		old_status_payload, sizeof(old_status_payload)),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_intent_snapshot_exact(
		&old_block.common.logical_assertion, &old_image_intent,
		old_image_payload, sizeof(old_image_payload)),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&old_status_intent, old_status_intent.first_armed_us + 1),
		RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&old_status_intent));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&old_image_intent, old_image_intent.first_armed_us + 1),
		RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&old_image_intent));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_drain_prepare_exact(
		&old_block.common.logical_assertion,
		old_block.common.assertion_sequence, 0,
		old_block.common.master_session_incarnation,
		&old_source_generation), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(old_source_generation, UINT64_C(91));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_drain_commit_exact(
		&old_block.common.logical_assertion,
		old_block.common.assertion_sequence, 0,
		old_block.common.master_session_incarnation,
		old_source_generation), RESOURCE_X_APPLY_APPLIED);

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&successor_block, 0, 77, 91, 19, 9, UINT64_C(3500),
		&lineage, &revoke), RESOURCE_X_APPLY_APPLIED);

	/* The production DROP order retains the exact pair before physically
	 * removing the descriptor.  That pending pair is protocol debt, but it must
	 * not invalidate the already-held terminal cover/REVOKING identity which
	 * the post-drop close consumes. */
	fake_gcs_master_node = 0;
	make_resource_x_remote_join_pair(tag, 2, &grant, &image);
	retarget_resource_x_remote_join_pair(&grant, &image, UINT64_C(12),
		UINT64_C(42));
	image.common.ordered_lane = 0;
	set_resource_x_test_source_fence(
		image.body.image_envelope.source_fence, UINT64_C(91), PCM_STATE_X);
	image.body.image_envelope.source_carrier_generation = UINT64_C(92);
	canonicalize_resource_x_test_image(&image);
	status = make_resource_x_remote_blocked_frame(tag, 2, 1);
	status.common = successor_block.common;
	status.common.source_candidate = 0;
	status.common.retain_pi_if_dirty = 0;
	status.common.outcome = RESOURCE_X_OUTCOME_OK;
	status.common.flags = RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED;
	memcpy(status.body.blocked_to_n.source_fence,
		image.body.image_envelope.source_fence,
		sizeof(status.body.blocked_to_n.source_fence));
	status.body.blocked_to_n.source_carrier_generation
		= image.body.image_envelope.source_carrier_generation;
	status.body.blocked_to_n.requester_target_generation = UINT64_C(42);
	status.body.blocked_to_n.page_scn_lsn
		= image.body.image_envelope.page_scn_lsn;
	status.body.blocked_to_n.dependency_count
		= image.body.image_envelope.dependency_count;
	memcpy(status.body.blocked_to_n.dependencies,
		image.body.image_envelope.dependencies,
		sizeof(status.body.blocked_to_n.dependencies));
	status.body.blocked_to_n.source_proof_crc32c
		= image.common.semantic_crc32c;
	status.body.blocked_to_n.page_checksum
		= image.body.image_envelope.page_checksum;
	status.body.blocked_to_n.source_disposition
		= RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
	status.body.blocked_to_n.proof_kind = RESOURCE_X_PROOF_REMOTE_CARRIER;
	status.body.blocked_to_n.holder_connection_generation = 51;
	status.body.blocked_to_n.acting_formation = 17;
	memset(&revoking, 0, sizeof(revoking));
	revoking.tag = tag;
	revoking.generation = 91;
	UT_ASSERT_EQ(revoke.reservation_token, UINT64_C(19));
	revoking.reservation_token = UINT64_C(20);
	revoking.flags = PCM_OWN_FLAG_REVOKING;
	revoking.pcm_state = (uint8)PCM_STATE_X;
	/* The generic path cannot infer a new descriptor episode from an equal
	 * generation and must remain stale.  The DROP-only path still rejects an
	 * equal or skipped BufferDesc token. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_source_exact(
		&successor_block, 0, &status, &image), RESOURCE_X_APPLY_STALE);
	stale = revoke;
	revoking.reservation_token = revoke.reservation_token;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_drop_x_source_exact(
		&successor_block, 0, &status, &image, &revoking, &stale),
		RESOURCE_X_APPLY_STALE);
	revoking.reservation_token = revoke.reservation_token + UINT64_C(1);
	stale = revoke;
	stale.owner_procno++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_drop_x_source_exact(
		&successor_block, 0, &status, &image, &revoking, &stale),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_drop_x_source_exact(
		&successor_block, 0, &status, &image, &revoking, &revoke),
		RESOURCE_X_APPLY_APPLIED);

	/* VM/FSM DROP has no retained N+PI descriptor on which SourceSettlement
	 * could later serialize terminal-cover cleanup.  The exact type-17 owner
	 * must therefore consume its own cover only after the physical X->N
	 * generation transition.  A mismatched post-drop snapshot changes neither
	 * the cover nor the owner; the exact transition closes both atomically. */
	/* The entry-local claim freezes the pre-begin token T.  BufferDesc
	 * reservation_begin then publishes the exact REVOKING token T+1. */
	dropped = revoking;
	dropped.generation = 93;
	dropped.flags = 0;
	dropped.pcm_state = (uint8)PCM_STATE_N;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_terminal_x_revoke_finish_drop_exact(
			&successor_block, 0, 77, &revoking, &dropped, &revoke),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 91));
	UT_ASSERT(cluster_pcm_lock_resource_x_terminal_x_revoke_revalidate_held_exact(
		&successor_block, 0, 77, 91, &revoke, &lineage));
	dropped.generation = 92;

	/* Equal, skipped, or overflowing reservation lineage cannot consume the
	 * exact owner or terminal cover. */
	revoking.reservation_token = revoke.reservation_token;
	dropped.reservation_token = revoking.reservation_token;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_terminal_x_revoke_finish_drop_exact(
			&successor_block, 0, 77, &revoking, &dropped, &revoke),
		RESOURCE_X_APPLY_STALE);
	revoking.reservation_token = revoke.reservation_token + UINT64_C(2);
	dropped.reservation_token = revoking.reservation_token;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_terminal_x_revoke_finish_drop_exact(
			&successor_block, 0, 77, &revoking, &dropped, &revoke),
		RESOURCE_X_APPLY_STALE);
	stale = revoke;
	stale.reservation_token = UINT64_MAX;
	revoking.reservation_token = UINT64_MAX;
	dropped.reservation_token = UINT64_MAX;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_terminal_x_revoke_finish_drop_exact(
			&successor_block, 0, 77, &revoking, &dropped, &stale),
		RESOURCE_X_APPLY_INVALID);

	revoking.reservation_token = revoke.reservation_token + UINT64_C(1);
	dropped.reservation_token = revoking.reservation_token;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_terminal_x_revoke_finish_drop_exact(
			&successor_block, 0, 77, &revoking, &dropped, &revoke),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 91));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_release_exact(
		&revoke), RESOURCE_X_APPLY_NOT_FOUND);
}

UT_TEST(test_resource_x_bootstrap_round_waits_only_for_exact_target_install)
{
	BufferTag tag = make_tag(157);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXAcquisitionRef ref;
	ResourceXBufferInstallProof install;
	ResourceXExecutorSnapshot executor;
	ResourceXBootstrapRoundAction action;
	ClusterPcmOwnSnapshot installing;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), false, 0, &request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(
		&request, UINT64_C(9), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, UINT64_C(110), &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);

	/* A BufferDesc marker alone is never sufficient.  It becomes waitable
	 * only after this exact attempt has entered T1.  The same predicate must
	 * then cover the executor's ordered N reservation, post-commit X fence,
	 * image-bound X fence, and T2-before-T3 X fence without admitting a
	 * mismatched token, attempt, or ledger phase. */
	memset(&installing, 0, sizeof(installing));
	installing.tag = tag;
	installing.pcm_state = (uint8)PCM_STATE_N;
	installing.flags = PCM_OWN_FLAG_GRANT_PENDING;
	installing.reservation_token = 1;
	UT_ASSERT(
		!cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&installing));

	ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(
		cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&installing));
	/* A conditional content-lock miss is the executor's retryable BLOCKED
	 * state, not ownership loss.  The same-node foreground follower must keep
	 * waiting both before and after the existing scheduled tick rearms the
	 * exact acquisition; dispatch_phase is a retry counter, not authority. */
	cluster_pcm_lock_resource_x_publish_no_progress_exact(
		&ref, RESOURCE_X_NO_PROGRESS_BUFFER_BUSY);
	UT_ASSERT(
		cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&installing));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_rearm_exact(&ref),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(
		cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&installing));
	UT_ASSERT(
		!cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 62, UINT64_C(50),
			&installing));
	installing.writer_activation_token = 1;
	UT_ASSERT(
		!cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&installing));
	installing.writer_activation_token = 0;
	installing.reservation_token = 0;
	UT_ASSERT(
		!cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&installing));
	installing.reservation_token = 1;
	installing.pcm_state = (uint8)PCM_STATE_X;
	installing.flags = 0;
	installing.generation = 1;
	installing.writer_activation_token = 1;
	UT_ASSERT(
		cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&installing));
	installing.writer_activation_token = 2;
	UT_ASSERT(
		!cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&installing));
	installing.writer_activation_token = 1;
	installing.resource_x_activation_generation = 1;
	UT_ASSERT(
		cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&installing));
	installing.resource_x_activation_generation = 2;
	UT_ASSERT(
		!cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&installing));
	installing.resource_x_activation_generation = 1;

	memset(&install, 0, sizeof(install));
	install.ownership_generation = 1;
	install.writer_activation_token = 1;
	install.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(
		&ref, &install), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_probe_exact(
		&ref, &executor), RESOURCE_X_EXECUTOR_READY);
	UT_ASSERT_EQ(executor.progress_flags,
		RESOURCE_X_PROGRESS_BOUND | RESOURCE_X_PROGRESS_T1
		| RESOURCE_X_PROGRESS_T2);
	UT_ASSERT(
		cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&installing));
	/* T3 clears the Resource-X generation before the writer token.  While
	 * the canonical round still carries T2, that exact ordered half-clear is
	 * part of the same install and must keep same-node followers waiting. */
	installing.resource_x_activation_generation = 0;
	UT_ASSERT(
		cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&installing));
	installing.writer_activation_token = 2;
	UT_ASSERT(
		!cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&installing));
	installing.resource_x_activation_generation = 1;
	installing.writer_activation_token = 0;
	UT_ASSERT(
		!cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&installing));
}

UT_TEST(test_resource_x_bootstrap_round_waits_across_exact_post_t3_cover_window)
{
	BufferTag tag = make_tag(214);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXAcquisitionRef ref;
	ResourceXBufferInstallProof install;
	ResourceXBootstrapRoundAction action;
	ClusterPcmOwnSnapshot post_t3;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), false, 0, &request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(
		&request, UINT64_C(9), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, UINT64_C(110), &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);

	ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref),
		RESOURCE_X_APPLY_APPLIED);
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 9;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(
		&ref, &install), RESOURCE_X_APPLY_APPLIED);

	/* T3 has physically cleared the BufferDesc writer fence, but the same
	 * executor has not yet entered the requester entry lock to retire T2 and
	 * publish TERMINAL_X_CACHED.  The exact T2 install generation must keep a
	 * follower waiting through this cross-lock interval. */
	memset(&post_t3, 0, sizeof(post_t3));
	post_t3.tag = tag;
	post_t3.pcm_state = (uint8)PCM_STATE_X;
	post_t3.generation = install.ownership_generation;
	post_t3.reservation_token = install.writer_activation_token;
	UT_ASSERT(
		cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&post_t3));
	post_t3.generation++;
	UT_ASSERT(
		!cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&post_t3));
}

static void
assert_resource_x_target_install_follow_state(
	const ResourceXTargetInstallContinuation *follow,
	const ClusterPcmOwnSnapshot *observed,
	ResourceXTargetInstallFollowState expected)
{
	ResourceXAcquisitionRef terminal_ref;
	ResourceXTargetInstallFollowState state;

	memset(&terminal_ref, 0x7f, sizeof(terminal_ref));
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_classify_exact(
			follow, observed, &terminal_ref);
	UT_ASSERT_EQ(state, expected);
	if (expected != RESOURCE_X_TARGET_INSTALL_TERMINAL)
		UT_ASSERT_EQ(terminal_ref.acquisition_generation, UINT64_C(0));
}

UT_TEST(test_resource_x_target_install_continuation_classifies_exact_attempt)
{
	BufferTag tag = make_tag(215);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXAcquisitionRef ref;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBufferInstallProof install;
	ResourceXBufferActivationProof activation;
	ResourceXBootstrapRoundAction action;
	ResourceXBootstrapRoundFailureSnapshot after_negative;
	ResourceXBootstrapRoundFailureSnapshot before_negative;
	ResourceXBootstrapRoundFailureSnapshot terminal_snapshot;
	ResourceXTargetInstallContinuation captured;
	ResourceXTargetInstallContinuation follow;
	ResourceXTargetInstallContinuation late_follow;
	ResourceXTargetInstallContinuation stale;
	ResourceXTargetInstallContinuation invalid;
	ResourceXTargetInstallFollowState state;
	ResourceXLocalOwnerHandle late_owner;
	ClusterPcmOwnSnapshot preterminal_observed;
	ClusterPcmOwnSnapshot late_observed;
	ClusterPcmOwnSnapshot changed;
	ClusterPcmOwnSnapshot observed;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), false, 0, &request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(
		&request, UINT64_C(9), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, UINT64_C(110), &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref),
		RESOURCE_X_APPLY_APPLIED);

	memset(&observed, 0, sizeof(observed));
	observed.tag = tag;
	observed.pcm_state = (uint8)PCM_STATE_N;
	observed.flags = PCM_OWN_FLAG_GRANT_PENDING;
	observed.generation = 0;
	observed.reservation_token = 12;
	/* A30 RED: ordinary T1 does not bind a BufferDesc generation/token.  This
	 * coherent immediate-successor reservation can therefore be sampled while
	 * the older round still says ASSERT_DISPATCHED and be miscaptured as I0.
	 * The later exact terminal publication below must disprove only this local
	 * receipt, never the terminal cover or the canonical successor path. */
	late_observed = observed;
	late_observed.generation = 2;
	memset(&late_follow, 0x7f, sizeof(late_follow));
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			UINT64_C(900), &late_observed, &late_follow);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_INFLIGHT);
	UT_ASSERT(late_follow.valid);
	UT_ASSERT_EQ(late_follow.pending_ownership_generation, UINT64_C(2));
	UT_ASSERT_EQ(late_follow.expected_x_ownership_generation, UINT64_C(3));
	UT_ASSERT_EQ(late_follow.reservation_token, UINT64_C(12));
	memset(&follow, 0x7f, sizeof(follow));
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			UINT64_C(900), &observed, &follow);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_INFLIGHT);
	UT_ASSERT(follow.valid);
	UT_ASSERT_EQ(follow.requester_node, 1);
	UT_ASSERT_EQ(follow.master_node, 0);
	UT_ASSERT(follow.entry_binding_generation != 0);
	UT_ASSERT_EQ(follow.resource_formation, UINT64_C(17));
	UT_ASSERT_EQ(follow.master_session_incarnation, UINT64_C(31));
	UT_ASSERT_EQ(follow.r4_record_generation, UINT64_C(77));
	UT_ASSERT_EQ(follow.acquisition_generation, UINT64_C(1));
	UT_ASSERT_EQ(follow.accepted_base_authority_generation, UINT64_C(9));
	UT_ASSERT_EQ(follow.observed_head_change_generation, UINT64_C(3));
	UT_ASSERT_EQ(follow.caller_absolute_deadline_us, UINT64_C(900));
	UT_ASSERT_EQ(follow.requested_sleep_slice_us, UINT64_C(50));
	UT_ASSERT_EQ(follow.pending_ownership_generation, UINT64_C(0));
	UT_ASSERT_EQ(follow.expected_x_ownership_generation, UINT64_C(1));
	UT_ASSERT_EQ(follow.reservation_token, UINT64_C(12));

	memset(&terminal_ref, 0x7f, sizeof(terminal_ref));
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_classify_exact(
			&follow, &observed, &terminal_ref);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_INFLIGHT);
	UT_ASSERT_EQ(terminal_ref.acquisition_generation, UINT64_C(0));

	/* I1: installed X with the writer fence and T1-only progress. */
	observed.pcm_state = (uint8)PCM_STATE_X;
	observed.flags = 0;
	observed.generation = 1;
	observed.writer_activation_token = 12;
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_classify_exact(
			&follow, &observed, &terminal_ref);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_INFLIGHT);
	memset(&captured, 0x7f, sizeof(captured));
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			UINT64_C(900), &observed, &captured);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_INFLIGHT);
	UT_ASSERT_EQ(captured.pending_ownership_generation, UINT64_C(0));
	UT_ASSERT_EQ(captured.expected_x_ownership_generation, UINT64_C(1));
	UT_ASSERT_EQ(captured.reservation_token, UINT64_C(12));
	UT_ASSERT_EQ(captured.acquisition_generation, follow.acquisition_generation);
	UT_ASSERT_EQ(captured.entry_binding_generation,
		follow.entry_binding_generation);

	/* I2: the exact requester apply publishes T2 and the attempt activation. */
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 1;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(&ref, &install),
		RESOURCE_X_APPLY_APPLIED);
	observed.resource_x_activation_generation = 1;
	preterminal_observed = observed;
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_classify_exact(
			&follow, &observed, &terminal_ref);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_INFLIGHT);
	memset(&captured, 0x7f, sizeof(captured));
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			UINT64_C(900), &observed, &captured);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_INFLIGHT);
	UT_ASSERT_EQ(captured.pending_ownership_generation, UINT64_C(0));
	UT_ASSERT_EQ(captured.acquisition_generation, follow.acquisition_generation);

	/* I3: the BufferDesc fence is clear while terminal publication is pending. */
	observed.writer_activation_token = 0;
	observed.resource_x_activation_generation = 0;
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_classify_exact(
			&follow, &observed, &terminal_ref);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_INFLIGHT);
	memset(&captured, 0x7f, sizeof(captured));
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			UINT64_C(900), &observed, &captured);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_INFLIGHT);
	UT_ASSERT_EQ(captured.pending_ownership_generation, UINT64_C(0));
	UT_ASSERT_EQ(captured.acquisition_generation, follow.acquisition_generation);
	memset(&before_negative, 0, sizeof(before_negative));
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&before_negative),
		RESOURCE_X_APPLY_APPLIED);

	/* Same-attempt malformed observations are fail-closed, never contention. */
	changed = observed;
	changed.tag = make_tag(216);
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_STALE);
	changed = observed;
	changed.reservation_token++;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_STALE);
	changed = observed;
	changed.generation++;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_STALE);
	changed = observed;
	changed.pcm_state = (uint8)PCM_STATE_S;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed.pcm_state = (uint8)PCM_STATE_READ_IMAGE;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed = observed;
	changed.pcm_state = (uint8)PCM_STATE_N;
	changed.generation = 0;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed = observed;
	changed.flags = PCM_OWN_FLAG_REVOKING;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed = observed;
	changed.writer_activation_token = changed.reservation_token;
	changed.resource_x_activation_generation = 2;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed = observed;
	changed.resource_x_activation_generation = 1;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed = observed;
	changed.generation = 0;
	memset(&captured, 0x7f, sizeof(captured));
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			UINT64_C(900), &changed, &captured);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	UT_ASSERT(!captured.valid);
	memset(&after_negative, 0, sizeof(after_negative));
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&after_negative),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(memcmp(&before_negative, &after_negative,
		sizeof(before_negative)), 0);

	/* Exact terminal cover is the only state that returns the acquisition ref. */
	memset(&activation, 0, sizeof(activation));
	activation.ownership_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(
		&ref, &activation), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_publish_terminal_exact(
			&ref, 31, 77, 1, 10, UINT64_C(120)),
		RESOURCE_X_APPLY_APPLIED);
	memset(&terminal_snapshot, 0, sizeof(terminal_snapshot));
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&terminal_snapshot),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(terminal_snapshot.round_phase, UINT8_C(4));
	UT_ASSERT_EQ(terminal_snapshot.terminal, UINT8_C(1));
	UT_ASSERT_EQ(terminal_snapshot.buffer_ownership_generation, UINT64_C(1));

	/* A30 capture-first ordering: terminal X(1) proves that the earlier
	 * N+GRANT_PENDING(2) sample predicted an unrelated X(3).  Pending, aborted
	 * clean N and a later N revoke are all receipt invalidation only. */
	memset(&late_owner, 0, sizeof(late_owner));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_begin_exact(
		&ref, 31, 77, 1, 12, 7, UINT64_C(130), &late_owner),
		RESOURCE_X_APPLY_APPLIED);
	assert_resource_x_target_install_follow_state(
		&late_follow, &late_observed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_cancel_exact(&late_owner),
		RESOURCE_X_APPLY_APPLIED);
	memset(&captured, 0x7f, sizeof(captured));
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			UINT64_C(900), &late_observed, &captured);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_STALE);
	UT_ASSERT(!captured.valid);

	late_observed.flags = 0;
	assert_resource_x_target_install_follow_state(
		&late_follow, &late_observed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	memset(&captured, 0x7f, sizeof(captured));
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			UINT64_C(900), &late_observed, &captured);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_STALE);
	UT_ASSERT(!captured.valid);

	late_observed.flags = PCM_OWN_FLAG_REVOKING;
	late_observed.reservation_token = 13;
	assert_resource_x_target_install_follow_state(
		&late_follow, &late_observed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	memset(&captured, 0x7f, sizeof(captured));
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			UINT64_C(900), &late_observed, &captured);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_STALE);
	UT_ASSERT(!captured.valid);

	/* The amendment is deliberately not a generic generation/token retry. */
	changed = late_observed;
	changed.generation++;
	assert_resource_x_target_install_follow_state(
		&late_follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed = late_observed;
	changed.reservation_token = 11;
	assert_resource_x_target_install_follow_state(
		&late_follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed = late_observed;
	changed.reservation_token = UINT64_MAX;
	assert_resource_x_target_install_follow_state(
		&late_follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed = late_observed;
	changed.writer_activation_token = changed.reservation_token;
	assert_resource_x_target_install_follow_state(
		&late_follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed = late_observed;
	changed.pcm_state = (uint8)PCM_STATE_X;
	assert_resource_x_target_install_follow_state(
		&late_follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	stale = late_follow;
	stale.observed_claim_pending_generation = 2;
	stale.observed_claim_reservation_token = 12;
	assert_resource_x_target_install_follow_state(
		&stale, &late_observed, RESOURCE_X_TARGET_INSTALL_STALE);

	memset(&terminal_ref, 0, sizeof(terminal_ref));
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_classify_exact(
			&follow, &observed, &terminal_ref);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_TERMINAL);
	UT_ASSERT_EQ(memcmp(&terminal_ref, &ref, sizeof(ref)), 0);

	/* The raw classifier now describes a simultaneous stable state.  Terminal
	 * publication paired with any uncleared I0/I1/I2 BufferDesc phase is an
	 * ordering contradiction.  Mixed-time observations are discarded by the
	 * driver's B-E-B adjudicator before this result is consumed. */
	changed = preterminal_observed;
	changed.pcm_state = (uint8)PCM_STATE_N;
	changed.flags = PCM_OWN_FLAG_GRANT_PENDING;
	changed.generation = follow.pending_ownership_generation;
	changed.writer_activation_token = 0;
	changed.resource_x_activation_generation = 0;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed = preterminal_observed;
	changed.resource_x_activation_generation = 0;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	memset(&terminal_ref, 0x7f, sizeof(terminal_ref));
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_classify_exact(
			&follow, &preterminal_observed, &terminal_ref);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(terminal_ref.acquisition_generation, UINT64_C(0));

	/* Each coherent immutable-axis drift is stale and returns no terminal ref. */
	stale = follow;
	stale.entry_binding_generation++;
	assert_resource_x_target_install_follow_state(
		&stale, &observed, RESOURCE_X_TARGET_INSTALL_STALE);
	stale = follow;
	stale.resource = make_tag(216);
	assert_resource_x_target_install_follow_state(
		&stale, &observed, RESOURCE_X_TARGET_INSTALL_STALE);
	stale = follow;
	stale.master_node = 2;
	assert_resource_x_target_install_follow_state(
		&stale, &observed, RESOURCE_X_TARGET_INSTALL_STALE);
	stale = follow;
	stale.resource_formation++;
	assert_resource_x_target_install_follow_state(
		&stale, &observed, RESOURCE_X_TARGET_INSTALL_STALE);
	stale = follow;
	stale.master_session_incarnation++;
	assert_resource_x_target_install_follow_state(
		&stale, &observed, RESOURCE_X_TARGET_INSTALL_STALE);
	stale = follow;
	stale.r4_record_generation++;
	assert_resource_x_target_install_follow_state(
		&stale, &observed, RESOURCE_X_TARGET_INSTALL_STALE);
	stale = follow;
	stale.acquisition_generation++;
	assert_resource_x_target_install_follow_state(
		&stale, &observed, RESOURCE_X_TARGET_INSTALL_STALE);
	stale = follow;
	stale.accepted_base_authority_generation++;
	assert_resource_x_target_install_follow_state(
		&stale, &observed, RESOURCE_X_TARGET_INSTALL_STALE);
	stale = follow;
	stale.observed_head_change_generation++;
	assert_resource_x_target_install_follow_state(&stale, &observed,
												  RESOURCE_X_TARGET_INSTALL_TERMINAL);
	stale = follow;
	stale.requested_sleep_slice_us++;
	assert_resource_x_target_install_follow_state(&stale, &observed,
												  RESOURCE_X_TARGET_INSTALL_TERMINAL);
	stale = follow;
	stale.pending_ownership_generation++;
	stale.expected_x_ownership_generation++;
	assert_resource_x_target_install_follow_state(
		&stale, &observed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	stale = follow;
	stale.reservation_token++;
	assert_resource_x_target_install_follow_state(
		&stale, &observed, RESOURCE_X_TARGET_INSTALL_STALE);
	stale = follow;
	stale.observed_claim_pending_generation = 3;
	stale.observed_claim_reservation_token = 4;
	assert_resource_x_target_install_follow_state(
		&stale, &observed, RESOURCE_X_TARGET_INSTALL_STALE);
	stale = follow;
	stale.requester_sender_connection_generation++;
	assert_resource_x_target_install_follow_state(
		&stale, &observed, RESOURCE_X_TARGET_INSTALL_STALE);
	stale = follow;
	stale.master_ingress_connection_generation++;
	assert_resource_x_target_install_follow_state(
		&stale, &observed, RESOURCE_X_TARGET_INSTALL_STALE);

	/* A malformed same-identity X shape is not ordinary contention. */
	observed.flags = PCM_OWN_FLAG_REVOKING;
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_classify_exact(
			&follow, &observed, &terminal_ref);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	memset(&invalid, 0, sizeof(invalid));
	assert_resource_x_target_install_follow_state(
		&invalid, &observed, RESOURCE_X_TARGET_INSTALL_INVALID);
}

UT_TEST(test_resource_x_target_install_stable_direct_init_i0_t2_is_closed)
{
	BufferTag tag = make_tag(229);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXAcquisitionRef ref;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBufferInstallProof install;
	ResourceXBufferActivationProof activation;
	ResourceXBootstrapRoundAction action;
	ResourceXTargetInstallContinuation follow;
	ResourceXTargetInstallContinuation changed_follow;
	ResourceXTargetInstallFollowState state;
	ClusterPcmOwnSnapshot observed_i0;
	ClusterPcmOwnSnapshot changed_i0;
	instr_time clock;
	uint64 now_us;
	uint64 round_deadline_us;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	INSTR_TIME_SET_CURRENT(clock);
	now_us = (uint64)INSTR_TIME_GET_MICROSEC(clock);
	round_deadline_us = now_us + UINT64_C(200000);
	fake_pcm_clock_us = now_us;
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&assertion, 0, 17, 31, 77, 51, 61, round_deadline_us, (round_deadline_us) - (now_us),
		now_us, UINT64_C(50000), 0, 12, false, 0, &request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(
		&request, UINT64_C(9), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, now_us + 1, &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref),
		RESOURCE_X_APPLY_APPLIED);

	memset(&observed_i0, 0, sizeof(observed_i0));
	observed_i0.tag = tag;
	observed_i0.pcm_state = (uint8)PCM_STATE_N;
	observed_i0.flags = PCM_OWN_FLAG_GRANT_PENDING;
	observed_i0.generation = 0;
	observed_i0.reservation_token = 12;
	memset(&follow, 0x7f, sizeof(follow));
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50000),
			round_deadline_us, &observed_i0, &follow);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_INFLIGHT);
	UT_ASSERT(follow.valid);
	UT_ASSERT_EQ(follow.observed_claim_pending_generation, UINT64_C(0));
	UT_ASSERT_EQ(follow.observed_claim_reservation_token, UINT64_C(12));
	UT_ASSERT_EQ(follow.pending_ownership_generation, UINT64_C(0));
	UT_ASSERT_EQ(follow.expected_x_ownership_generation, UINT64_C(1));

	/* Once T2 is visible, this raw I0/T2 pair is treated as simultaneously
	 * stable and must fail closed.  A legal old-I0/new-T2 interleaving is
	 * recognized only by the outer B-E-B snapshots changing. */
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 1;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(&ref, &install),
		RESOURCE_X_APPLY_APPLIED);
	memset(&terminal_ref, 0x7f, sizeof(terminal_ref));
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_classify_exact(
			&follow, &observed_i0, &terminal_ref);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(terminal_ref.acquisition_generation, UINT64_C(0));

	/* No-progress debt cannot borrow A33's sampled-order classification. */
	cluster_pcm_lock_resource_x_publish_no_progress_exact(
		&ref, RESOURCE_X_NO_PROGRESS_BUFFER_BUSY);
	assert_resource_x_target_install_follow_state(
		&follow, &observed_i0,
		RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_rearm_exact(&ref),
		RESOURCE_X_APPLY_APPLIED);
	assert_resource_x_target_install_follow_state(
		&follow, &observed_i0,
		RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);

	/* Direct-init identity and the exact I0 envelope are mandatory. */
	changed_follow = follow;
	changed_follow.observed_claim_reservation_token++;
	assert_resource_x_target_install_follow_state(
		&changed_follow, &observed_i0, RESOURCE_X_TARGET_INSTALL_STALE);
	changed_i0 = observed_i0;
	changed_i0.flags = 0;
	assert_resource_x_target_install_follow_state(
		&follow, &changed_i0,
		RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed_i0 = observed_i0;
	changed_i0.reservation_token++;
	assert_resource_x_target_install_follow_state(
		&follow, &changed_i0, RESOURCE_X_TARGET_INSTALL_STALE);

	/* T3 retires the active ledger.  The old I0 cannot bridge that state or
	 * produce terminal authority. */
	memset(&activation, 0, sizeof(activation));
	activation.ownership_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(
		&ref, &activation), RESOURCE_X_APPLY_APPLIED);
	assert_resource_x_target_install_follow_state(
		&follow, &observed_i0,
		RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_publish_terminal_exact(
			&ref, 31, 77, 1, 10, now_us + 2),
		RESOURCE_X_APPLY_APPLIED);
	/* Ordinary T1/T2 has no exact BufferDesc token binding.  Keep A30's
	 * misbound-I0 protection closed. */
	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, round_deadline_us, (round_deadline_us) - (now_us),
		now_us, UINT64_C(50000), false, 0, &request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(
		&request, UINT64_C(9), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, now_us + 1, &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref),
		RESOURCE_X_APPLY_APPLIED);
	memset(&follow, 0x7f, sizeof(follow));
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50000),
			round_deadline_us, &observed_i0, &follow);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_INFLIGHT);
	UT_ASSERT_EQ(follow.observed_claim_reservation_token, UINT64_C(0));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(&ref, &install),
		RESOURCE_X_APPLY_APPLIED);
	assert_resource_x_target_install_follow_state(
		&follow, &observed_i0,
		RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
}

static ResourceXAcquisitionRef target_wait_race_ref;
static ResourceXBufferInstallProof target_wait_race_install;

static void
renew_target_head_after_wait_unlock(void)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	fake_pcm_clock_us = UINT64_C(105001);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(&target_wait_race_ref,
																   &target_wait_race_install),
				 RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_resource_x_target_wait_old_lease_is_only_a_reprobe_hint)
{
	BufferTag tag = make_tag(484);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request, ack, dispatch;
	ResourceXAcquisitionRef terminal;
	ClusterPcmOwnSnapshot pending;
	ResourceXTargetInstallContinuation follower;
	ResourceXBootstrapRoundFailureSnapshot before, after;
	ResourceXGateSnapshot gate;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_pcm_clock_us = UINT64_C(100000);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 500000, 5000, 100000, 2000, false, 0,
					 &request, &terminal),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(&request, 9, 71);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(&ack, 0, 61, 77,
																			  100000, &dispatch),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	target_wait_race_ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&target_wait_race_ref),
				 RESOURCE_X_APPLY_APPLIED);
	memset(&pending, 0, sizeof(pending));
	pending.tag = tag;
	pending.pcm_state = (uint8)PCM_STATE_N;
	pending.flags = PCM_OWN_FLAG_GRANT_PENDING;
	pending.generation = 3;
	pending.reservation_token = 77;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_install_claim_bind_t1_exact(&target_wait_race_ref, &pending),
		RESOURCE_X_APPLY_APPLIED);
	memset(&target_wait_race_install, 0, sizeof(target_wait_race_install));
	target_wait_race_install.ownership_generation = 4;
	target_wait_race_install.writer_activation_token = 78;
	target_wait_race_install.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 2000, 500000, &pending, &follower),
				 RESOURCE_X_TARGET_INSTALL_INFLIGHT);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 2000, &before),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(before.absolute_deadline_us, UINT64_C(105000));
	/* The first clock sample is under entry lock.  On the second sample,
	 * the owner installs T2 and renews the lease after the waiter unlocked. */
	fake_pcm_clock_us = UINT64_C(104999);
	fake_pcm_clock_hook_countdown = 2;
	fake_pcm_clock_hook = renew_target_head_after_wait_unlock;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
					 &follower, RESOURCE_X_TARGET_INSTALL_INFLIGHT, 50),
				 RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(fake_pcm_clock_hook == NULL);
	UT_ASSERT_EQ(fake_cv_sleep_count, 0);
	UT_ASSERT_EQ(fake_cv_prepare_count, fake_cv_cancel_count);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 2000, &after),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(after.absolute_deadline_us, UINT64_C(110001));
	UT_ASSERT_EQ(after.head_change_generation, before.head_change_generation + 1);
	UT_ASSERT_EQ(after.round_phase, before.round_phase);
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
	UT_ASSERT_EQ(gate.phase, RESOURCE_X_GATE_OPEN);
}

UT_TEST(test_resource_x_target_install_wait_uses_receipt_and_fixed_deadline)
{
	BufferTag tag = make_tag(217);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXAcquisitionRef ref;
	ResourceXAcquisitionRef successor_ref;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBufferInstallProof install;
	ResourceXBufferActivationProof activation;
	ResourceXBootstrapRoundAction action;
	ResourceXTargetInstallContinuation follow;
	ResourceXTargetInstallContinuation follow_before;
	ResourceXTargetInstallContinuation round_limited;
	ResourceXTargetInstallContinuation stale;
	ResourceXTargetInstallContinuation expired;
	ResourceXTargetInstallContinuation invalid;
	ResourceXLocalOwnerHandle recycle;
	ResourceXTargetInstallFollowState state;
	ResourceXApplyResult wait_result;
	ClusterPcmOwnSnapshot observed;
	instr_time clock;
	uint64 now_us;
	uint64 round_deadline_us;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	INSTR_TIME_SET_CURRENT(clock);
	now_us = (uint64)INSTR_TIME_GET_MICROSEC(clock);
	round_deadline_us = now_us + UINT64_C(200000);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, round_deadline_us, (round_deadline_us) - (now_us),
		now_us, UINT64_C(50000), false, 0, &request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(
		&request, UINT64_C(9), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, now_us + 1, &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref),
		RESOURCE_X_APPLY_APPLIED);

	memset(&observed, 0, sizeof(observed));
	observed.tag = tag;
	observed.pcm_state = (uint8)PCM_STATE_N;
	observed.flags = PCM_OWN_FLAG_GRANT_PENDING;
	observed.generation = 0;
	observed.reservation_token = 12;
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50000),
			now_us + UINT64_C(1000000), &observed, &follow);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_INFLIGHT);
	follow_before = follow;

	/* A short caller slice wins while the caller and current head leases live. */
	wait_result
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
			&follow, RESOURCE_X_TARGET_INSTALL_INFLIGHT, 50);
	UT_ASSERT_EQ(wait_result, RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(fake_cv_prepare_count, 1);
	UT_ASSERT_EQ(fake_cv_sleep_count, 1);
	UT_ASSERT_EQ(fake_cv_cancel_count, 1);
	UT_ASSERT_EQ(fake_cv_timed_sleep_timeout, 50);
	UT_ASSERT_EQ(memcmp(&follow, &follow_before, sizeof(follow)), 0);

	/* The retained caller deadline clips a longer per-call timeout. */
	wait_result
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
			&follow, RESOURCE_X_TARGET_INSTALL_INFLIGHT, 5000);
	UT_ASSERT_EQ(wait_result, RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(fake_cv_prepare_count, 2);
	UT_ASSERT_EQ(fake_cv_sleep_count, 2);
	UT_ASSERT_EQ(fake_cv_cancel_count, 2);
	UT_ASSERT(fake_cv_timed_sleep_timeout > 0);
	UT_ASSERT(fake_cv_timed_sleep_timeout <= 1000);
	UT_ASSERT_EQ(memcmp(&follow, &follow_before, sizeof(follow)), 0);

	/* A later caller budget cannot extend the current head's progress lease. */
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50000),
			now_us + UINT64_C(5000000), &observed, &round_limited);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_INFLIGHT);
	wait_result
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
			&round_limited, RESOURCE_X_TARGET_INSTALL_INFLIGHT, 5000);
	UT_ASSERT_EQ(wait_result, RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(fake_cv_prepare_count, 3);
	UT_ASSERT_EQ(fake_cv_sleep_count, 3);
	UT_ASSERT_EQ(fake_cv_cancel_count, 3);
	UT_ASSERT(fake_cv_timed_sleep_timeout > 0);
	UT_ASSERT(fake_cv_timed_sleep_timeout <= 2000);

	/* Recycled binding is stale before CV registration. */
	stale = follow;
	stale.entry_binding_generation++;
	wait_result
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
			&stale, RESOURCE_X_TARGET_INSTALL_INFLIGHT, 50);
	UT_ASSERT_EQ(wait_result, RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(fake_cv_prepare_count, 3);
	UT_ASSERT_EQ(fake_cv_sleep_count, 3);
	UT_ASSERT_EQ(fake_cv_cancel_count, 3);

	/* A stable entry-side executor failure is fail-closed after the registered
	 * recheck; the wait cannot borrow an old BufferDesc to reinterpret it. */
	cluster_pcm_lock_resource_x_publish_no_progress_exact(
		&ref, RESOURCE_X_NO_PROGRESS_BUFFER_CORRUPT);
	wait_result
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
			&follow, RESOURCE_X_TARGET_INSTALL_INFLIGHT, 50);
	UT_ASSERT_EQ(wait_result, RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(fake_cv_prepare_count, 4);
	UT_ASSERT_EQ(fake_cv_sleep_count, 3);
	UT_ASSERT_EQ(fake_cv_cancel_count, 4);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_rearm_exact(&ref),
		RESOURCE_X_APPLY_APPLIED);

	/* An expired immutable budget never enters timed sleep. */
	expired = follow;
	expired.caller_absolute_deadline_us = 1;
	wait_result
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
			&expired, RESOURCE_X_TARGET_INSTALL_INFLIGHT, 50);
	UT_ASSERT_EQ(wait_result, RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(fake_cv_prepare_count, 5);
	UT_ASSERT_EQ(fake_cv_sleep_count, 3);
	UT_ASSERT_EQ(fake_cv_cancel_count, 5);

	/* Terminal is rechecked under the entry lock and returned without sleep. */
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 1;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(&ref, &install),
		RESOURCE_X_APPLY_APPLIED);
	memset(&activation, 0, sizeof(activation));
	activation.ownership_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(
		&ref, &activation), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_publish_terminal_exact(
			&ref, 31, 77, 1, 10, now_us + 2),
		RESOURCE_X_APPLY_APPLIED);
	/* Terminal publication before the registered entry recheck is progress,
	 * independent of any old BufferDesc sample.  The waiter must return an
	 * immediate reprobe hint without sleeping or returning authority. */
	wait_result
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
			&follow, RESOURCE_X_TARGET_INSTALL_INFLIGHT, 50);
	UT_ASSERT_EQ(wait_result, RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(fake_cv_prepare_count, 6);
	UT_ASSERT_EQ(fake_cv_sleep_count, 3);
	UT_ASSERT_EQ(fake_cv_cancel_count, 6);
	observed.pcm_state = (uint8)PCM_STATE_X;
	observed.flags = 0;
	observed.generation = 1;
	observed.writer_activation_token = 0;
	observed.resource_x_activation_generation = 0;
	wait_result
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
			&follow, RESOURCE_X_TARGET_INSTALL_INFLIGHT, 50);
	UT_ASSERT_EQ(wait_result, RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(fake_cv_prepare_count, 7);
	UT_ASSERT_EQ(fake_cv_sleep_count, 3);
	UT_ASSERT_EQ(fake_cv_cancel_count, 7);
	UT_ASSERT_EQ(memcmp(&follow, &follow_before, sizeof(follow)), 0);

	/* A terminal recycler owns the cross-lock interval.  The registered wait
	 * sleeps while that exact owner is live, then the owner-clear broadcast
	 * makes the same clean terminal sample immediately re-probeable. */
	memset(&recycle, 0, sizeof(recycle));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_begin_exact(
		&ref, 31, 77, 1, 12, 7, now_us + 3, &recycle),
		RESOURCE_X_APPLY_APPLIED);
	state = cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50000), now_us + UINT64_C(1000000), &observed,
		&follow);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	wait_result
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
			&follow, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY, 50);
	UT_ASSERT_EQ(wait_result, RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(fake_cv_prepare_count, 8);
	UT_ASSERT_EQ(fake_cv_sleep_count, 4);
	UT_ASSERT_EQ(fake_cv_cancel_count, 8);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_cancel_exact(&recycle),
		RESOURCE_X_APPLY_APPLIED);
	wait_result
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
			&follow, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY, 50);
	UT_ASSERT_EQ(wait_result, RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(fake_cv_prepare_count, 9);
	UT_ASSERT_EQ(fake_cv_sleep_count, 4);
	UT_ASSERT_EQ(fake_cv_cancel_count, 9);

	/* A terminal cover deliberately survives the requester round that created
	 * it.  Once a later caller has captured that cover, its PREUSE wait belongs
	 * to the later caller's still-live first deadline; the completed round's
	 * historical deadline remains identity only. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_begin_exact(
		&ref, 31, 77, 1, 12, 7, now_us + 4, &recycle),
		RESOURCE_X_APPLY_APPLIED);
	state = cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50000), now_us + UINT64_C(5000000), &observed,
		&round_limited);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	assert_resource_x_target_install_follow_state(
		&round_limited, &observed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	fake_pcm_clock_us = now_us + UINT64_C(250000);
	wait_result
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
			&round_limited, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY, 50);
	UT_ASSERT_EQ(wait_result, RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(fake_cv_prepare_count, 10);
	UT_ASSERT_EQ(fake_cv_sleep_count, 5);
	UT_ASSERT_EQ(fake_cv_cancel_count, 10);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_cancel_exact(&recycle),
		RESOURCE_X_APPLY_APPLIED);

	/* A valid successor T1 may be admitted after the coherent PREUSE
	 * classification but before the registered entry recheck.  The old
	 * terminal owner is still exact, but the active acquisition proves entry
	 * progress and invalidates the old wait predicate.  Re-probe immediately;
	 * never turn this legal ordering into RECOVERY_BLOCKED or sleep on it. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_begin_exact(
		&ref, 31, 77, 1, 12, 7, now_us + 4, &recycle),
		RESOURCE_X_APPLY_APPLIED);
	assert_resource_x_target_install_follow_state(
		&follow, &observed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	successor_ref = make_resource_x_acquisition_ref(tag, 1, 17, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&successor_ref),
		RESOURCE_X_APPLY_APPLIED);
	wait_result
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
			&follow, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY, 50);
	UT_ASSERT_EQ(wait_result, RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(fake_cv_prepare_count, 11);
	UT_ASSERT_EQ(fake_cv_sleep_count, 5);
	UT_ASSERT_EQ(fake_cv_cancel_count, 11);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_cancel_exact(&recycle),
		RESOURCE_X_APPLY_APPLIED);

	/* Invalid input never registers and cannot be repaired by a timeout. */
	memset(&invalid, 0, sizeof(invalid));
	wait_result
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
			&invalid, RESOURCE_X_TARGET_INSTALL_INFLIGHT, 50);
	UT_ASSERT_EQ(wait_result, RESOURCE_X_APPLY_INVALID);
	wait_result
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
			&follow, RESOURCE_X_TARGET_INSTALL_INFLIGHT, 0);
	UT_ASSERT_EQ(wait_result, RESOURCE_X_APPLY_INVALID);
	wait_result
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
			&follow, RESOURCE_X_TARGET_INSTALL_TERMINAL, 50);
	UT_ASSERT_EQ(wait_result, RESOURCE_X_APPLY_INVALID);
	UT_ASSERT_EQ(fake_cv_prepare_count, 11);
	UT_ASSERT_EQ(fake_cv_sleep_count, 5);
	UT_ASSERT_EQ(fake_cv_cancel_count, 11);
}

UT_TEST(test_resource_x_target_install_preuse_successor_never_grants_old_receipt)
{
	BufferTag tag = make_tag(218);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXDecodedFrame successor_block;
	ResourceXDecodedFrame release;
	ResourceXAcquisitionRef ref;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBufferInstallProof install;
	ResourceXBufferActivationProof activation;
	ResourceXBootstrapRoundAction action;
	ResourceXTargetInstallContinuation follow;
	ResourceXTargetInstallContinuation fresh;
	ResourceXTargetInstallFollowState state;
	ResourceXTerminalXLineage lineage;
	ResourceXLocalOwnerHandle recycle;
	ResourceXLocalOwnerHandle revoke;
	ResourceXLocalOwnerHandle evict;
	ClusterPcmOwnSnapshot observed;
	ClusterPcmOwnSnapshot changed;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(10000000),
		(UINT64_C(10000000)) - (UINT64_C(100)), UINT64_C(100), UINT64_C(50), false, 0, &request,
		&terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(
		&request, UINT64_C(9), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, UINT64_C(110), &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref),
		RESOURCE_X_APPLY_APPLIED);

	memset(&observed, 0, sizeof(observed));
	observed.tag = tag;
	observed.pcm_state = (uint8)PCM_STATE_N;
	observed.flags = PCM_OWN_FLAG_GRANT_PENDING;
	observed.generation = 0;
	observed.reservation_token = 12;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_bind_t1_exact(&ref, &observed),
				 RESOURCE_X_APPLY_APPLIED);
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			UINT64_C(9000000), &observed, &follow);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_INFLIGHT);
	UT_ASSERT(follow.valid);

	memset(&install, 0, sizeof(install));
	install.ownership_generation = 1;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(&ref, &install),
		RESOURCE_X_APPLY_APPLIED);
	memset(&activation, 0, sizeof(activation));
	activation.ownership_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(
		&ref, &activation), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_publish_terminal_exact(
			&ref, 31, 77, 1, 10, UINT64_C(120)),
		RESOURCE_X_APPLY_APPLIED);
	observed.pcm_state = (uint8)PCM_STATE_X;
	observed.flags = 0;
	observed.generation = 1;
	observed.writer_activation_token = 0;
	observed.resource_x_activation_generation = 0;

	/* A terminal recycler serializes use of the same cover.  The follower may
	 * use that cover only after a fresh owner-empty classification. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_begin_exact(
		&ref, 31, 77, 1, 12, 7, UINT64_C(130), &recycle),
		RESOURCE_X_APPLY_APPLIED);
	assert_resource_x_target_install_follow_state(
		&follow, &observed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_itl_recycle_cancel_exact(&recycle),
		RESOURCE_X_APPLY_APPLIED);
	assert_resource_x_target_install_follow_state(
		&follow, &observed, RESOURCE_X_TARGET_INSTALL_TERMINAL);

	successor_block = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCK_TO_N, tag, 2, 1);
	successor_block.payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	successor_block.common.base_authority_generation = UINT64_C(10);
	successor_block.common.authority_generation = UINT64_C(10);
	successor_block.common.assertion_sequence = UINT64_C(42);
	successor_block.common.ordered_lane = 0;
	successor_block.common.observed_mode = (uint8)PCM_STATE_X;
	successor_block.common.target_mode = (uint8)PCM_STATE_N;
	successor_block.common.source_candidate = 1;
	successor_block.common.retain_pi_if_dirty = 1;
	memset(&lineage, 0, sizeof(lineage));
	memset(&revoke, 0, sizeof(revoke));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&successor_block, 0, 77, 1, 12, 8, UINT64_C(200),
		&lineage, &revoke), RESOURCE_X_APPLY_APPLIED);

	/* The entry owner may win before the BufferDesc arm, or the BufferDesc
	 * sample may observe the exact one-token REVOKING successor.  Neither
	 * ordering may return the old terminal ref. */
	assert_resource_x_target_install_follow_state(
		&follow, &observed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	observed.flags = PCM_OWN_FLAG_REVOKING;
	observed.reservation_token = 13;
	assert_resource_x_target_install_follow_state(
		&follow, &observed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_yield_exact(
		&revoke, UINT64_C(210)), RESOURCE_X_APPLY_APPLIED);
	observed.flags = 0;
	assert_resource_x_target_install_follow_state(
		&follow, &observed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);

	/* A busy nonblocking drain may exact-abort and replay more than once.
	 * Every successful begin consumes a token while ownership generation stays
	 * fixed.  The original terminal receipt must treat every later cycle as
	 * pre-use invalidation, never as authority or corruption. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&successor_block, 0, 77, 1, 13, 8, UINT64_C(220),
		&lineage, &revoke), RESOURCE_X_APPLY_APPLIED);
	observed.flags = PCM_OWN_FLAG_REVOKING;
	observed.reservation_token = 14;
	assert_resource_x_target_install_follow_state(
		&follow, &observed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_yield_exact(
		&revoke, UINT64_C(230)), RESOURCE_X_APPLY_APPLIED);
	observed.flags = 0;
	assert_resource_x_target_install_follow_state(
		&follow, &observed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&successor_block, 0, 77, 1, 14, 8, UINT64_C(240),
		&lineage, &revoke), RESOURCE_X_APPLY_APPLIED);
	observed.flags = PCM_OWN_FLAG_REVOKING;
	observed.reservation_token = 15;
	assert_resource_x_target_install_follow_state(
		&follow, &observed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_yield_exact(
		&revoke, UINT64_C(250)), RESOURCE_X_APPLY_APPLIED);
	observed.flags = 0;
	assert_resource_x_target_install_follow_state(
		&follow, &observed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);

	/* Expiring the bounded HANDOFF clears only its local priority.  The old
	 * token still proves that this receipt crossed a revoke, while a freshly
	 * captured post-abort receipt can revalidate the surviving terminal X. */
	{
		unsigned char owner_before[sizeof(fake_pcm_entries)];
		ResourceXDecodedFrame ignored;
		ResourceXAcquisitionRef no_authority;

		/* A foreground follower cannot expire or clear another owner's handoff,
		 * even when it observes a timestamp beyond that handoff's deadline. */
		memcpy(owner_before, &fake_pcm_entries, sizeof(owner_before));
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
						 &assertion, 0, 17, 31, 77, 51, 61, 9000000, (9000000) - (1000200), 1000200,
						 50, true, 1, &ignored, &no_authority),
					 RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
		UT_ASSERT(memcmp(owner_before, &fake_pcm_entries, sizeof(owner_before)) == 0);
	}
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&successor_block, 0, 77, 1, 15, 8, UINT64_C(1000200),
		&lineage, &revoke), RESOURCE_X_APPLY_STALE);
	assert_resource_x_target_install_follow_state(
		&follow, &observed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	memset(&fresh, 0, sizeof(fresh));
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			UINT64_C(9000000), &observed, &fresh);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_TERMINAL);
	UT_ASSERT(fresh.valid);
	UT_ASSERT_EQ(fresh.reservation_token, UINT64_C(15));

	/* Exact target eviction is the same pre-use exclusion boundary.  Abort
	 * permits a fresh terminal classification; no terminal ref is returned
	 * while EVICTING owns the cover. */
	memset(&release, 0, sizeof(release));
	memset(&evict, 0, sizeof(evict));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_target_evict_prepare_exact(
		&tag, 0, 17, 31, 77, 1, 15, 61, 9, &release, &evict),
		RESOURCE_X_APPLY_APPLIED);
	assert_resource_x_target_install_follow_state(
		&fresh, &observed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_target_evict_abort_exact(&evict),
		RESOURCE_X_APPLY_APPLIED);
	assert_resource_x_target_install_follow_state(
		&fresh, &observed, RESOURCE_X_TARGET_INSTALL_TERMINAL);

	/* A committed X->N after any finite number of reversible cycles is retry
	 * evidence for the old receipt.  A second ownership generation, token
	 * regression/wrap or activation debt remains fail closed. */
	observed.pcm_state = (uint8)PCM_STATE_N;
	observed.generation = 2;
	assert_resource_x_target_install_follow_state(
		&follow, &observed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	observed.flags = PCM_OWN_FLAG_REVOKING;
	assert_resource_x_target_install_follow_state(
		&follow, &observed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	changed = observed;
	changed.generation++;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed = observed;
	changed.reservation_token++;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	changed = observed;
	changed.reservation_token = follow.reservation_token;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed = observed;
	changed.reservation_token = UINT64_MAX;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed = observed;
	changed.writer_activation_token = changed.reservation_token;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);

	/* After PREUSE_RETRY discards the old process-local receipt, the same
	 * post-terminal N+REVOKING sample is not a new install continuation: its
	 * generation already follows the cached terminal X.  Decline capture so
	 * the caller can enter the canonical retained-release path; fabricating an
	 * expected X at the next generation would fail closed before that path. */
	memset(&fresh, 0x5a, sizeof(fresh));
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			UINT64_C(9000000), &observed, &fresh);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_STALE);
	UT_ASSERT(!fresh.valid);
}

UT_TEST(test_resource_x_target_install_capture_during_revoke_retries_same_token_n)
{
	BufferTag tag = make_tag(219);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXDecodedFrame successor_block;
	ResourceXAcquisitionRef ref;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBufferInstallProof install;
	ResourceXBufferActivationProof activation;
	ResourceXBootstrapRoundAction action;
	ResourceXTargetInstallContinuation follow;
	ResourceXTargetInstallContinuation changed_follow;
	ResourceXTargetInstallFollowState state;
	ResourceXTerminalXLineage lineage;
	ResourceXLocalOwnerHandle revoke;
	ClusterPcmOwnSnapshot observed;
	ClusterPcmOwnSnapshot changed;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(10000000),
		(UINT64_C(10000000)) - (UINT64_C(100)), UINT64_C(100), UINT64_C(50), false, 0, &request,
		&terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(
		&request, UINT64_C(9), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, UINT64_C(110), &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref),
		RESOURCE_X_APPLY_APPLIED);
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 1;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(&ref, &install),
		RESOURCE_X_APPLY_APPLIED);
	memset(&activation, 0, sizeof(activation));
	activation.ownership_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(
		&ref, &activation), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_publish_terminal_exact(
			&ref, 31, 77, 1, 10, UINT64_C(120)),
		RESOURCE_X_APPLY_APPLIED);

	successor_block = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCK_TO_N, tag, 2, 1);
	successor_block.payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	successor_block.common.base_authority_generation = UINT64_C(10);
	successor_block.common.authority_generation = UINT64_C(10);
	successor_block.common.assertion_sequence = UINT64_C(42);
	successor_block.common.ordered_lane = 0;
	successor_block.common.observed_mode = (uint8)PCM_STATE_X;
	successor_block.common.target_mode = (uint8)PCM_STATE_N;
	successor_block.common.source_candidate = 1;
	successor_block.common.retain_pi_if_dirty = 1;
	memset(&lineage, 0, sizeof(lineage));
	memset(&revoke, 0, sizeof(revoke));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&successor_block, 0, 77, 1, 12, 8, UINT64_C(200),
		&lineage, &revoke), RESOURCE_X_APPLY_APPLIED);

	/* A31 RED: capture happens after reservation-begin has consumed token 13.
	 * The continuation therefore records the revoke token, not the earlier
	 * terminal-install token.  Finishing that same source conversion advances
	 * X(1,13) to N(2,13), so equality is retry evidence only. */
	memset(&observed, 0, sizeof(observed));
	observed.tag = tag;
	observed.pcm_state = (uint8)PCM_STATE_X;
	observed.flags = PCM_OWN_FLAG_REVOKING;
	observed.generation = 1;
	observed.reservation_token = 13;
	memset(&follow, 0x7f, sizeof(follow));
	state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			UINT64_C(9000000), &observed, &follow);
	UT_ASSERT_EQ(state, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	UT_ASSERT(follow.valid);
	UT_ASSERT_EQ(follow.pending_ownership_generation, UINT64_C(0));
	UT_ASSERT_EQ(follow.expected_x_ownership_generation, UINT64_C(1));
	UT_ASSERT_EQ(follow.reservation_token, UINT64_C(13));
	UT_ASSERT_EQ(follow.capture_flags,
		RESOURCE_X_TARGET_INSTALL_CAPTURE_REVOKE_TOKEN);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_release_exact(
		&revoke), RESOURCE_X_APPLY_APPLIED);

	observed.pcm_state = (uint8)PCM_STATE_N;
	observed.flags = PCM_OWN_FLAG_REVOKING;
	observed.generation = 2;
	assert_resource_x_target_install_follow_state(
		&follow, &observed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	observed.flags = 0;
	assert_resource_x_target_install_follow_state(
		&follow, &observed, RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY);
	changed_follow = follow;
	changed_follow.capture_flags = 0;
	assert_resource_x_target_install_follow_state(
		&changed_follow, &observed,
		RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed_follow = follow;
	changed_follow.capture_flags = UINT8_C(0x80);
	assert_resource_x_target_install_follow_state(
		&changed_follow, &observed, RESOURCE_X_TARGET_INSTALL_INVALID);

	/* Equality is not a generic N retry. */
	changed = observed;
	changed.flags = PCM_OWN_FLAG_GRANT_PENDING;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed = observed;
	changed.generation = 3;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed = observed;
	changed.reservation_token = 12;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed = observed;
	changed.reservation_token = UINT64_MAX;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed = observed;
	changed.writer_activation_token = changed.reservation_token;
	assert_resource_x_target_install_follow_state(
		&follow, &changed, RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED);
	changed_follow = follow;
	changed_follow.observed_claim_pending_generation = 1;
	changed_follow.observed_claim_reservation_token = 13;
	assert_resource_x_target_install_follow_state(
		&changed_follow, &observed, RESOURCE_X_TARGET_INSTALL_STALE);
}

UT_TEST(test_resource_x_bootstrap_terminal_cover_accepts_remote_master_requester_projection)
{
	BufferTag tag = make_tag(154);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame unused_dispatch;
	ResourceXAcquisitionRef expected_ref;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBufferInstallProof install;
	ResourceXBufferActivationProof activation;
	ResourceXBootstrapRoundAction action;
	ResourceXRequesterJoinSnapshot join;
	ClusterPcmOwnSnapshot lost;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), false, 0, &request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(
		&request, UINT64_C(9), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, UINT64_C(110), &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	make_resource_x_remote_join_pair(tag, 1, &grant, &image);
	retarget_resource_x_remote_join_pair(&grant, &image, 11, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 0, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(join.flags,
		RESOURCE_X_REQUESTER_JOIN_HAS_IMAGE
		| RESOURCE_X_REQUESTER_JOIN_HAS_GRANT
		| RESOURCE_X_REQUESTER_JOIN_READY);

	/* The canonical X grant lives at remote master node 0.  This requester
	 * keeps only its exact BufferDesc X generation, so its local GRD
	 * projection must remain neutral rather than becoming a second authority. */
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);
	expected_ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&expected_ref),
		RESOURCE_X_APPLY_APPLIED);
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 91;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(
		&expected_ref, &install), RESOURCE_X_APPLY_APPLIED);
	memset(&activation, 0, sizeof(activation));
	activation.ownership_generation = 92;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(
		&expected_ref, &activation), RESOURCE_X_APPLY_STALE);
	activation.ownership_generation = 91;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(
		&expected_ref, &activation), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);
	/* T3 retirement and the sole cached-X terminal cover share the same
	 * entry-lock linearization point.  Otherwise a same-node follower can
	 * observe post-T3 BufferDesc X while the round is still ASSERT_DISPATCHED
	 * and incorrectly fail the valid native acquisition. */
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 91));

	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_publish_terminal_exact(
			&expected_ref, 31, 77, 91, 11, UINT64_C(115)),
		RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 91));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(120)),
		UINT64_C(120), UINT64_C(50), true, 91, &unused_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL);
	UT_ASSERT(memcmp(&terminal_ref, &expected_ref,
		sizeof(terminal_ref)) == 0);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);

	/* A non-X ownership snapshot invalidates the joinable cover only after
	 * its generation has strictly crossed the retained cached-X generation.
	 * Equal/encumbered observations cannot erase a possibly-current cover. */
	memset(&lost, 0, sizeof(lost));
	lost.tag = tag;
	lost.pcm_state = (uint8)PCM_STATE_S;
	lost.generation = 91;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_invalidate_ownership_loss_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50), &lost),
		RESOURCE_X_APPLY_STALE);
	lost.generation = 92;
	lost.flags = PCM_OWN_FLAG_GRANT_PENDING;
	lost.reservation_token = 7;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_invalidate_ownership_loss_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50), &lost),
		RESOURCE_X_APPLY_STALE);
	lost.flags = 0;
	lost.reservation_token = 0;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_invalidate_ownership_loss_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50), &lost),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 91));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(130)),
		UINT64_C(130), UINT64_C(50), false, 0, &request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(request.common.assertion_sequence, UINT64_C(2));
}

UT_TEST(test_resource_x_cached_x_to_s_commit_clears_only_exact_terminal_cover)
{
	BufferTag tag = make_tag(214);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXDecodedFrame unused_dispatch;
	ResourceXAcquisitionRef expected_ref;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBufferInstallProof install;
	ResourceXBootstrapRoundAction action;
	ClusterPcmOwnSnapshot revoking;
	ClusterPcmOwnSnapshot shared;
	ClusterPcmOwnSnapshot inflight;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), 7, 5, false, 0, &request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(
		&request, UINT64_C(9), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, UINT64_C(110), &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	expected_ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&expected_ref),
		RESOURCE_X_APPLY_APPLIED);
	/* An ordinary same-node caller does not own the creation proof, but it
	 * must join (and only wait on) the exact direct-init T1 sidecar instead of
	 * invalidating the creator's requester round. */
	memset(&inflight, 0, sizeof(inflight));
	inflight.tag = tag;
	inflight.generation = 8;
	inflight.reservation_token = 5;
	inflight.pcm_state = (uint8)PCM_STATE_X;
	inflight.writer_activation_token = 5;
	UT_ASSERT(
		cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&inflight));
	inflight.reservation_token = 6;
	UT_ASSERT(
		!cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&inflight));
	inflight.reservation_token = 5;
	inflight.generation = 9;
	UT_ASSERT(
		!cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
			&inflight));
	inflight.generation = 8;
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 8;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(
		&expected_ref, &install), RESOURCE_X_APPLY_APPLIED);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(115)),
		UINT64_C(115), UINT64_C(50), 7, 5, true, 8, &unused_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 8));

	memset(&revoking, 0, sizeof(revoking));
	revoking.tag = tag;
	revoking.pcm_state = (uint8)PCM_STATE_X;
	revoking.flags = PCM_OWN_FLAG_REVOKING;
	revoking.generation = 8;
	revoking.reservation_token = 13;
	shared = revoking;
	shared.pcm_state = (uint8)PCM_STATE_S;
	shared.flags = 0;
	shared.generation = 10;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_note_x_to_s_exact(
			&revoking, &shared), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 8));
	shared.generation = 9;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_note_x_to_s_exact(
			&revoking, &shared), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 8));
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_note_x_to_s_exact(
			&revoking, &shared), RESOURCE_X_APPLY_NOT_FOUND);
}

extern ResourceXApplyResult
cluster_pcm_lock_resource_x_target_evict_prepare_exact(
	const BufferTag *tag, int32 current_master_node,
	uint64 resource_formation, uint64 master_session_incarnation,
	uint64 r4_record_generation, uint64 cached_ownership_generation,
	uint64 reservation_token, uint32 sender_connection_generation,
	int32 owner_procno, ResourceXDecodedFrame *release_out,
	ResourceXLocalOwnerHandle *handle_out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_target_evict_abort_exact(
	const ResourceXLocalOwnerHandle *handle);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_target_evict_commit_exact(
	const ResourceXDecodedFrame *release, int32 current_master_node,
	uint64 r4_record_generation, uint64 cached_ownership_generation,
	const ResourceXLocalOwnerHandle *handle);

UT_TEST(test_resource_x_bootstrap_direct_init_cached_x_consumes_same_round_t3_handoff)
{
	BufferTag tag = make_tag(155);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXDecodedFrame successor_block;
	ResourceXDecodedFrame unused_dispatch;
	ResourceXAcquisitionRef expected_ref;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXTerminalXLineage lineage;
	ResourceXExecutorSnapshot executor_snapshot;
	ResourceXBufferInstallProof install;
	ResourceXBufferActivationProof activation;
	ResourceXBootstrapRoundAction action;
	ClusterPcmOwnSnapshot inflight;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), 7, 5, false, 0, &request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(
		&request, UINT64_C(9), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, UINT64_C(110), &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);

	/* The DATA worker may publish X before the exact T2/T3 activation fields
	 * have been cleared.  This is waitable only while every direct-init round
	 * and BufferDesc field still names the same in-flight attempt. */
	memset(&inflight, 0, sizeof(inflight));
	inflight.tag = tag;
	inflight.generation = 8;
	inflight.reservation_token = 5;
	inflight.writer_activation_token = 5;
	inflight.pcm_state = (uint8)PCM_STATE_X;
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_direct_init_inflight_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
		7, 5, &inflight));
	inflight.resource_x_activation_generation = 1;
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_direct_init_inflight_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
		7, 5, &inflight));
	inflight.resource_x_activation_generation = 2;
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_direct_init_inflight_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
		7, 5, &inflight));
	inflight.resource_x_activation_generation = 1;
	inflight.writer_activation_token = 0;
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_direct_init_inflight_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
		7, 5, &inflight));

	expected_ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&expected_ref),
		RESOURCE_X_APPLY_APPLIED);
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 8;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(
		&expected_ref, &install), RESOURCE_X_APPLY_APPLIED);

	/* The direct-init sidecar clear has made exact generation 8 visible,
	 * while the DATA worker has not yet called requester_activate/publish.
	 * The same round must consume its retained T1/T2 handoff atomically. */
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(115)),
		UINT64_C(115), UINT64_C(50), 7, 5, true, 8, &unused_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL);
	UT_ASSERT(memcmp(&terminal_ref, &expected_ref,
		sizeof(terminal_ref)) == 0);
	memset(&executor_snapshot, 0, sizeof(executor_snapshot));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_probe_exact(
		&expected_ref, &executor_snapshot), RESOURCE_X_EXECUTOR_COMPLETE);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 8));

	/* BufferDesc and the requester round are separate lock domains.  A
	 * foreground direct initializer may have sampled this exact pre-T3 X
	 * sidecar immediately before the DATA worker published the terminal
	 * cover.  The cover must make that stale-in-time sample waitable so the
	 * caller reaches the existing BufferDesc re-probe; it must not grant X
	 * from the sample itself. */
	inflight.writer_activation_token = 5;
	inflight.resource_x_activation_generation = 1;
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_direct_init_inflight_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
		7, 5, &inflight));
	inflight.generation = 9;
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_direct_init_inflight_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50),
		7, 5, &inflight));
	inflight.generation = 8;

	memset(&activation, 0, sizeof(activation));
	activation.ownership_generation = 8;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(
		&expected_ref, &activation), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_publish_terminal_exact(
			&expected_ref, 31, 77, 8, 10, UINT64_C(120)),
		RESOURCE_X_APPLY_DUPLICATE);

	/* Once the exact direct-init round has become the retained node-X cover,
	 * an ordinary same-node writer joins that cover by its terminal identity.
	 * The creation-only direct-init generation/token must not make the cached
	 * X unusable immediately after known-new initialization. */
	memset(&terminal_ref, 0, sizeof(terminal_ref));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(125)),
		UINT64_C(125), UINT64_C(50), true, 8, &unused_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL);
	UT_ASSERT(memcmp(&terminal_ref, &expected_ref,
		sizeof(terminal_ref)) == 0);

	/* The old terminal holder and the incoming FIFO successor are distinct
	 * Resource-X episodes.  They join only at the exact canonical-authority
	 * edge: accepted base 9 granted old X at 10, and the successor blocks on
	 * base 10. */
	successor_block = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCK_TO_N, tag, 2, 1);
	successor_block.payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	successor_block.common.base_authority_generation = UINT64_C(10);
	successor_block.common.authority_generation = UINT64_C(10);
	successor_block.common.assertion_sequence = UINT64_C(42);
	successor_block.common.observed_mode = (uint8)PCM_STATE_X;
	successor_block.common.target_mode = (uint8)PCM_STATE_N;
	successor_block.common.source_candidate = 1;
	successor_block.common.retain_pi_if_dirty = 1;
	memset(&lineage, 0, sizeof(lineage));
	UT_ASSERT(
		cluster_pcm_lock_resource_x_bootstrap_round_direct_init_terminal_holder_exact(
			&successor_block, 0, 77, 8, &lineage));
	UT_ASSERT(resource_x_assertion_equal(
		&lineage.holder_assertion, &expected_ref.assertion));
	UT_ASSERT(!resource_x_assertion_equal(
		&lineage.holder_assertion,
		&successor_block.common.logical_assertion));
	UT_ASSERT_EQ(lineage.holder_attempt, UINT64_C(1));
	UT_ASSERT_EQ(lineage.accepted_base_authority_generation, UINT64_C(9));
	UT_ASSERT_EQ(lineage.final_authority_generation, UINT64_C(10));
	UT_ASSERT_EQ(lineage.successor_attempt, UINT64_C(42));
	UT_ASSERT_EQ(lineage.cached_ownership_generation, UINT64_C(8));
	UT_ASSERT_EQ(lineage.r4_record_generation, UINT64_C(77));

	successor_block.common.base_authority_generation = UINT64_C(9);
	successor_block.common.authority_generation = UINT64_C(9);
	UT_ASSERT(
		!cluster_pcm_lock_resource_x_bootstrap_round_direct_init_terminal_holder_exact(
			&successor_block, 0, 77, 8, &lineage));
	successor_block.common.base_authority_generation = UINT64_C(10);
	successor_block.common.authority_generation = UINT64_C(10);
	UT_ASSERT(
		!cluster_pcm_lock_resource_x_bootstrap_round_direct_init_terminal_holder_exact(
			&successor_block, 0, 77, 9, &lineage));
	UT_ASSERT(
		!cluster_pcm_lock_resource_x_bootstrap_round_direct_init_terminal_holder_exact(
			&successor_block, 1, 77, 8, &lineage));
	successor_block.common.resource_formation++;
	UT_ASSERT(
		!cluster_pcm_lock_resource_x_bootstrap_round_direct_init_terminal_holder_exact(
			&successor_block, 0, 77, 8, &lineage));
	successor_block.common.resource_formation--;
	successor_block.common.master_session_incarnation++;
	UT_ASSERT(
		!cluster_pcm_lock_resource_x_bootstrap_round_direct_init_terminal_holder_exact(
			&successor_block, 0, 77, 8, &lineage));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_wait_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50), UINT64_MAX - 1, 1),
				 RESOURCE_X_APPLY_DUPLICATE);

	/* Terminal node-level joining no longer uses creation-only tokens.  The
	 * exact cached physical X generation and terminal ref remain mandatory;
	 * the full B-E-B pre-use path separately verifies live physical tokens. */
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(126)),
		UINT64_C(126), UINT64_C(50), 7, 6, true, 8, &unused_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL);
	UT_ASSERT(memcmp(&terminal_ref, &expected_ref, sizeof(terminal_ref)) == 0);
}

UT_TEST(test_resource_x_cached_x_eviction_prepares_and_commits_release)
{
	BufferTag tag = make_tag(161);
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef entry_ref;
	PcmRetireRefusal why;
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXDecodedFrame release;
	ResourceXDecodedFrame replay_release;
	ResourceXDecodedFrame unused_dispatch;
	ResourceXAcquisitionRef expected_ref;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXLocalOwnerHandle eviction;
	ResourceXLocalOwnerHandle replay_eviction;
	ResourceXLocalOwnerHandle stale_eviction;
	ResourceXBufferInstallProof install;
	ResourceXBootstrapRoundAction action;
	ClusterPcmOwnSnapshot lost;
	uint64 binding_generation;
	bool residual_entry;

	reset_fake_pcm_runtime(4);
	fake_pcm_clock_us = 200;
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), 7, 5, false, 0, &request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(
		&request, UINT64_C(9), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, UINT64_C(110), &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	expected_ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&expected_ref),
		RESOURCE_X_APPLY_APPLIED);
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 8;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(
		&expected_ref, &install), RESOURCE_X_APPLY_APPLIED);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(115)),
		UINT64_C(115), UINT64_C(50), 7, 5, true, 8, &unused_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 8));
	UT_ASSERT(pcm_entry_ref_acquire(
		&tag, false, &entry_ref, &acquire_result));
	binding_generation = entry_ref.binding_generation;
	pcm_entry_ref_release(&entry_ref);

	/* Cache replacement may freeze kind-4 only while the exact BufferDesc
	 * REVOKING token is live.  The same entry-lock transaction claims one
	 * non-authority EVICTING owner; conflict, ownership-loss and D3 retire must
	 * not cross that owner. */
	memset(&release, 0x5a, sizeof(release));
	memset(&eviction, 0x5a, sizeof(eviction));
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_target_evict_prepare_exact(
			&tag, 0, 17, 31, 77, 9, 13, 81, 7,
			&release, &eviction),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(memcmp(&release,
		&(ResourceXDecodedFrame){0}, sizeof(release)), 0);
	UT_ASSERT_EQ(memcmp(&eviction,
		&(ResourceXLocalOwnerHandle){0}, sizeof(eviction)), 0);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_target_evict_prepare_exact(
			&tag, 0, 17, 31, 77, 8, 13, 81, 7,
			&release, &eviction),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(release.kind, RESOURCE_X_WIRE_RELEASE_X);
	UT_ASSERT_EQ(release.payload_bytes, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT(!release.blocked_has_remote_proof);
	UT_ASSERT(resource_x_assertion_equal(
		&release.common.logical_assertion, &expected_ref.assertion));
	UT_ASSERT_EQ(release.common.base_authority_generation, UINT64_C(9));
	UT_ASSERT_EQ(release.common.authority_generation, UINT64_C(10));
	UT_ASSERT_EQ(release.common.resource_formation, UINT64_C(17));
	UT_ASSERT_EQ(release.common.master_session_incarnation, UINT64_C(31));
	UT_ASSERT_EQ(release.common.assertion_sequence, UINT64_C(1));
	UT_ASSERT_EQ(release.common.ordered_lane, UINT32_C(0));
	UT_ASSERT_EQ(release.common.action_node, 1);
	UT_ASSERT_EQ(release.common.observed_mode, (uint8)PCM_STATE_X);
	UT_ASSERT_EQ(release.common.target_mode, (uint8)PCM_STATE_N);
	UT_ASSERT_EQ(release.common.source_candidate, 0);
	UT_ASSERT_EQ(release.common.retain_pi_if_dirty, 0);
	UT_ASSERT_EQ(release.common.sender_connection_generation, UINT32_C(81));
	UT_ASSERT_EQ(release.common.outcome, RESOURCE_X_OUTCOME_OK);
	UT_ASSERT_EQ(release.common.flags, 0);
	UT_ASSERT_EQ(memcmp(&eviction.ref, &expected_ref,
		sizeof(expected_ref)), 0);
	UT_ASSERT_EQ(eviction.master_session_incarnation, UINT64_C(31));
	UT_ASSERT_EQ(eviction.r4_record_generation, UINT64_C(77));
	UT_ASSERT_EQ(eviction.buffer_ownership_generation, UINT64_C(8));
	UT_ASSERT_EQ(eviction.reservation_token, UINT64_C(13));
	UT_ASSERT_EQ(eviction.absolute_deadline_us, UINT64_C(1100));
	UT_ASSERT_EQ(eviction.owner_procno, 7);

	memset(&replay_release, 0, sizeof(replay_release));
	memset(&replay_eviction, 0, sizeof(replay_eviction));
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_target_evict_prepare_exact(
			&tag, 0, 17, 31, 77, 8, 13, 81, 7,
			&replay_release, &replay_eviction),
		RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(memcmp(&replay_release, &release, sizeof(release)), 0);
	UT_ASSERT_EQ(memcmp(&replay_eviction, &eviction, sizeof(eviction)), 0);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_target_evict_prepare_exact(
			&tag, 0, 17, 31, 77, 8, 14, 81, 7,
			&replay_release, &replay_eviction),
		RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(!pcm_entry_retire_classify_exact(
		&tag, binding_generation, &why));
	UT_ASSERT_EQ(why, PCM_RETIRE_REFUSAL_RESOURCE_X_ACTIVE);

	memset(&lost, 0, sizeof(lost));
	lost.tag = tag;
	lost.pcm_state = (uint8)PCM_STATE_N;
	lost.generation = 9;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_invalidate_ownership_loss_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50), &lost),
		RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 8));

	stale_eviction = eviction;
	stale_eviction.owner_generation++;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_target_evict_abort_exact(&stale_eviction),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_target_evict_abort_exact(&eviction),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 8));

	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_target_evict_prepare_exact(
			&tag, 0, 17, 31, 77, 8, 14, 81, 7,
			&release, &eviction),
		RESOURCE_X_APPLY_APPLIED);
	stale_eviction = eviction;
	stale_eviction.reservation_token++;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_target_evict_commit_exact(
			&release, 0, 77, 8, &stale_eviction),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_target_evict_commit_exact(
			&release, 0, 77, 8, &eviction),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 8));
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);
	residual_entry = pcm_entry_ref_acquire(
		&tag, false, &entry_ref, &acquire_result);
	if (residual_entry)
		pcm_entry_ref_release(&entry_ref);
	UT_ASSERT(!residual_entry);
	UT_ASSERT_EQ(acquire_result, PCM_ENTRY_ACQUIRE_NOT_FOUND);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_target_evict_commit_exact(
			&release, 0, 77, 8, &eviction),
		RESOURCE_X_APPLY_NOT_FOUND);
}

UT_TEST(test_resource_x_local_master_release_keeps_cached_cover_until_commit)
{
	BufferTag tag = make_tag(203);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXDecodedFrame release;
	ResourceXDecodedFrame settlement;
	ResourceXDecodedFrame unused_dispatch;
	ResourceXAcquisitionRef expected_ref;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXLocalOwnerHandle eviction;
	ResourceXBufferInstallProof install;
	ResourceXBootstrapRoundAction action;
	ResourceXDurableProof durable;
	ResourceXMasterSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 0, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), 7, 5, false, 0, &request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&request, 0, 61, 77, 31, 71, &ack),
		RESOURCE_X_APPLY_APPLIED);
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, UINT64_C(110), &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&assert_frame, 0, 61, 77, 31, 71, &snapshot),
		RESOURCE_X_APPLY_APPLIED);

	memset(&durable, 0, sizeof(durable));
	durable.assertion = assertion;
	durable.base_authority_generation
		= assert_frame.common.base_authority_generation;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = assert_frame.common.assertion_sequence;
	durable.requester_target_generation
		= assert_frame.common.assertion_sequence;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x12345678);
	durable.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(
		&durable, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);

	expected_ref = make_resource_x_acquisition_ref(tag, 0, 17,
		assert_frame.common.assertion_sequence);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&expected_ref),
		RESOURCE_X_APPLY_APPLIED);
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 8;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation
		= assert_frame.common.assertion_sequence;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(
		&expected_ref, &install), RESOURCE_X_APPLY_APPLIED);

	settlement = assert_frame;
	settlement.kind = RESOURCE_X_WIRE_INSTALL_SETTLEMENT;
	memset(&settlement.body, 0, sizeof(settlement.body));
	settlement.common.ordered_lane = 0;
	settlement.common.outcome = RESOURCE_X_OUTCOME_OK;
	settlement.common.authority_generation
		= snapshot.final_authority_generation;
	settlement.body.install_settlement.conversion_base_generation
		= assert_frame.common.base_authority_generation;
	settlement.body.install_settlement.final_authority_generation
		= snapshot.final_authority_generation;
	settlement.body.install_settlement.requester_connection_generation = 91;
	settlement.body.install_settlement.requester_target_generation
		= assert_frame.common.assertion_sequence;
	settlement.body.install_settlement.page_scn_lsn = 82;
	settlement.body.install_settlement.page_checksum
		= UINT32_C(0x12345678);
	settlement.body.install_settlement.source_proof_crc32c
		= UINT32_C(0x87654321);
	settlement.body.install_settlement.installed_mode = PCM_STATE_X;
	settlement.body.install_settlement.requester_role
		= RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
	settlement.body.install_settlement.terminal_outcome
		= RESOURCE_X_OUTCOME_OK;
	settlement.body.install_settlement.terminal_state
		= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
		&settlement, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_SETTLED);

	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(115)),
		UINT64_C(115), UINT64_C(50), 7, 5, true, 8, &unused_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 8));
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_target_evict_prepare_exact(
			&tag, 0, 17, 31, 77, 8, 13, 81, 7,
			&release, &eviction),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_target_evict_abort_exact(&eviction),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_release_x_exact(
		&release, 0, &snapshot), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 8));
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_target_evict_prepare_exact(
			&tag, 0, 17, 31, 77, 8, 14, 81, 7,
			&release, &eviction),
		RESOURCE_X_APPLY_APPLIED);

	/* Local-master kind-4 apply mutates the canonical GRD X to N, but the
	 * requester cover is still the immutable admission evidence required by
	 * the matching release commit.  Only that commit may clear it. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_release_x_exact(
		&release, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 8));
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_target_evict_commit_exact(
			&release, 0, 77, 8, &eviction),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 8));
}

UT_TEST(test_resource_x_local_n_without_evicting_owner_is_post_mutation_ambiguity)
{
	BufferTag tag = make_tag(204);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXDecodedFrame successor_assert;
	ResourceXDecodedFrame release;
	ResourceXDecodedFrame settlement;
	ResourceXDecodedFrame unused_dispatch;
	ResourceXAcquisitionRef expected_ref;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXLocalOwnerHandle eviction;
	ResourceXBufferInstallProof install;
	ResourceXBootstrapRoundAction action;
	ResourceXDurableProof durable;
	ResourceXMasterSnapshot snapshot;
	ResourceXMasterSnapshot successor_before;
	ResourceXMasterSnapshot successor_after;
	ClusterPcmOwnSnapshot lost;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 0, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), 7, 5, false, 0, &request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&request, 0, 61, 77, 31, 71, &ack),
		RESOURCE_X_APPLY_APPLIED);
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, UINT64_C(110), &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&assert_frame, 0, 61, 77, 31, 71, &snapshot),
		RESOURCE_X_APPLY_APPLIED);
	successor_assert = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 1, 1);
	successor_assert.common.base_authority_generation
		= assert_frame.common.base_authority_generation;
	successor_assert.common.authority_generation
		= assert_frame.common.base_authority_generation;
	successor_assert.common.assertion_sequence
		= assert_frame.common.assertion_sequence + 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&successor_assert, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_QUEUED);

	memset(&durable, 0, sizeof(durable));
	durable.assertion = assertion;
	durable.base_authority_generation
		= assert_frame.common.base_authority_generation;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = assert_frame.common.assertion_sequence;
	durable.requester_target_generation
		= assert_frame.common.assertion_sequence;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x12345678);
	durable.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(
		&durable, &snapshot), RESOURCE_X_APPLY_APPLIED);

	expected_ref = make_resource_x_acquisition_ref(tag, 0, 17,
		assert_frame.common.assertion_sequence);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&expected_ref),
		RESOURCE_X_APPLY_APPLIED);
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 8;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation
		= assert_frame.common.assertion_sequence;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(
		&expected_ref, &install), RESOURCE_X_APPLY_APPLIED);

	settlement = assert_frame;
	settlement.kind = RESOURCE_X_WIRE_INSTALL_SETTLEMENT;
	memset(&settlement.body, 0, sizeof(settlement.body));
	settlement.common.ordered_lane = 0;
	settlement.common.outcome = RESOURCE_X_OUTCOME_OK;
	settlement.common.authority_generation
		= snapshot.final_authority_generation;
	settlement.body.install_settlement.conversion_base_generation
		= assert_frame.common.base_authority_generation;
	settlement.body.install_settlement.final_authority_generation
		= snapshot.final_authority_generation;
	settlement.body.install_settlement.requester_connection_generation = 91;
	settlement.body.install_settlement.requester_target_generation
		= assert_frame.common.assertion_sequence;
	settlement.body.install_settlement.page_scn_lsn = 82;
	settlement.body.install_settlement.page_checksum
		= UINT32_C(0x12345678);
	settlement.body.install_settlement.source_proof_crc32c
		= UINT32_C(0x87654321);
	settlement.body.install_settlement.installed_mode = PCM_STATE_X;
	settlement.body.install_settlement.requester_role
		= RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
	settlement.body.install_settlement.terminal_outcome
		= RESOURCE_X_OUTCOME_OK;
	settlement.body.install_settlement.terminal_state
		= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
		&settlement, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&successor_assert.common.logical_assertion, &successor_before),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(successor_before.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);

	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(115)),
		UINT64_C(115), UINT64_C(50), 7, 5, true, 8, &unused_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_target_evict_prepare_exact(
			&tag, 0, 17, 31, 77, 8, 13, 81, 7,
			&release, &eviction),
		RESOURCE_X_APPLY_BAD_STATE);
	/* A local master with an active FIFO successor rejects cache eviction
	 * before BufferDesc X->N and returns no reusable plan bytes or owner. */
	UT_ASSERT_EQ(memcmp(&release,
		&(ResourceXDecodedFrame){0}, sizeof(release)), 0);
	UT_ASSERT_EQ(memcmp(&eviction,
		&(ResourceXLocalOwnerHandle){0}, sizeof(eviction)), 0);
	/* Local N without the exact EVICTING owner means the irreversible
	 * BufferDesc step lost its completion evidence.  It is post-mutation
	 * ambiguity: retain the terminal cover and canonical master state rather
	 * than silently treating N as a completed release. */
	memset(&lost, 0, sizeof(lost));
	lost.tag = tag;
	lost.pcm_state = (uint8)PCM_STATE_N;
	lost.generation = 9;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_invalidate_ownership_loss_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50), &lost),
		RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 8));
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&successor_assert.common.logical_assertion, &successor_after),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(memcmp(&successor_before, &successor_after,
		sizeof(successor_before)), 0);
}

UT_TEST(test_resource_x_terminal_remote_holder_binds_exact_final_authority)
{
	BufferTag tag = make_tag(159);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXDecodedFrame successor_block;
	ResourceXAcquisitionRef expected_ref;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXTerminalXLineage lineage;
	ResourceXLocalOwnerHandle revoke;
	ResourceXBufferInstallProof install;
	ResourceXBufferActivationProof activation;
	ResourceXBootstrapRoundAction action;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), false, 0, &request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(
		&request, UINT64_C(9), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, UINT64_C(110), &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);

	expected_ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&expected_ref),
		RESOURCE_X_APPLY_APPLIED);
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 91;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(
		&expected_ref, &install), RESOURCE_X_APPLY_APPLIED);
	memset(&activation, 0, sizeof(activation));
	activation.ownership_generation = 91;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(
		&expected_ref, &activation), RESOURCE_X_APPLY_APPLIED);
	/* A remote X->N+PI then N->X handoff consumes two authority edges. */
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_publish_terminal_exact(
			&expected_ref, 31, 77, 91, 11, UINT64_C(120)),
		RESOURCE_X_APPLY_APPLIED);

	successor_block = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCK_TO_N, tag, 2, 1);
	successor_block.payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	successor_block.common.base_authority_generation = UINT64_C(11);
	successor_block.common.authority_generation = UINT64_C(11);
	successor_block.common.assertion_sequence = UINT64_C(42);
	successor_block.common.observed_mode = (uint8)PCM_STATE_X;
	successor_block.common.target_mode = (uint8)PCM_STATE_N;
	successor_block.common.source_candidate = 1;
	successor_block.common.retain_pi_if_dirty = 1;
	memset(&lineage, 0, sizeof(lineage));
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_terminal_holder_exact(
		&successor_block, 0, 77, 91, &lineage));
	UT_ASSERT_EQ(lineage.accepted_base_authority_generation, UINT64_C(9));
	UT_ASSERT_EQ(lineage.final_authority_generation, UINT64_C(11));
	UT_ASSERT_EQ(lineage.direct_init_ownership_generation, UINT64_C(0));
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_direct_init_terminal_holder_exact(
		&successor_block, 0, 77, 91, &lineage));

	memset(&revoke, 0, sizeof(revoke));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&successor_block, 0, 77, 91, 19, 9, UINT64_C(130),
		&lineage, &revoke),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_terminal_x_revoke_revalidate_held_exact(
		&successor_block, 0, 77, 91, &revoke, &lineage));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_release_exact(&revoke),
		RESOURCE_X_APPLY_APPLIED);

	successor_block.common.base_authority_generation = UINT64_C(10);
	successor_block.common.authority_generation = UINT64_C(10);
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_terminal_holder_exact(
		&successor_block, 0, 77, 91, &lineage));
}

static ResourceXDecodedFrame
make_resource_x_remote_blocked_frame(BufferTag tag, int32 requester_node,
								 int32 source_node)
{
	ResourceXDecodedFrame blocked = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCKED_TO_N, tag, requester_node, source_node);

	blocked.common.observed_mode = PCM_STATE_X;
	blocked.common.target_mode = PCM_STATE_N;
	blocked.common.outcome = RESOURCE_X_OUTCOME_OK;
	blocked.common.flags = RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED;
	blocked.blocked_has_remote_proof = true;
	memset(blocked.body.blocked_to_n.source_fence, 0x5a,
		   sizeof(blocked.body.blocked_to_n.source_fence));
	set_resource_x_test_source_fence(
		blocked.body.blocked_to_n.source_fence, UINT64_C(60), PCM_STATE_X);
	blocked.body.blocked_to_n.source_carrier_generation = 61;
	blocked.body.blocked_to_n.requester_target_generation = 41;
	blocked.body.blocked_to_n.page_scn_lsn = 63;
	blocked.body.blocked_to_n.dependency_count = 2;
	blocked.body.blocked_to_n.dependencies[0] = 65;
	blocked.body.blocked_to_n.dependencies[1] = 66;
	blocked.body.blocked_to_n.source_proof_crc32c = UINT32_C(0x11223344);
	blocked.body.blocked_to_n.page_checksum = UINT32_C(0x55667788);
	blocked.body.blocked_to_n.source_disposition
		= RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
	blocked.body.blocked_to_n.proof_kind = RESOURCE_X_PROOF_REMOTE_CARRIER;
	blocked.body.blocked_to_n.holder_connection_generation = 64;
	blocked.body.blocked_to_n.acting_formation
		= blocked.common.resource_formation;
	return blocked;
}

UT_TEST(test_resource_x_adapter_adopts_only_exact_pristine_legacy_base)
{
	BufferTag tag = make_tag(156);
	PcmAuthoritySnapshot authority;
	ResourceXDecodedFrame assertion;
	ResourceXMasterSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_N_TO_X, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_X_TO_N_RELEASE, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_N_TO_S, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(authority.transition_count, UINT64_C(3));
	UT_ASSERT_EQ(authority.state, PCM_STATE_S);

	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 0, 0);
	assertion.common.base_authority_generation = 3;
	assertion.common.authority_generation = 3;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 0, &snapshot), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_base_bind_exact(
		&assertion.common.logical_assertion, 17, 3, &authority),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_base_bind_exact(
		&assertion.common.logical_assertion, 17, 3, &authority),
		RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);

	/* Once any Resource-X request exists, even a later exact legacy
	 * transition count cannot reseed canonical authority. */
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_S_TO_N_RELEASE, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(authority.transition_count, UINT64_C(4));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_base_bind_exact(
		&assertion.common.logical_assertion, 17, 4, &authority),
		RESOURCE_X_APPLY_STALE);
}

UT_TEST(test_resource_x_adapter_head_rebinds_only_before_assert)
{
	BufferTag tag = make_tag(157);
	PcmAuthoritySnapshot authority;
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame predecessor;
	ResourceXDecodedFrame settlement;
	ResourceXDecodedFrame successor;
	ResourceXDurableProof durable;
	ResourceXIntentSlot grant_intent;
	ResourceXMasterSnapshot snapshot;
	uint8 grant_bytes[RESOURCE_X_PROOF_V1_BYTES];

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_N_TO_X, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_X_TO_N_RELEASE, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_N_TO_S, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(authority.transition_count, UINT64_C(3));
	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 0, 0);
	assertion.common.base_authority_generation = 3;
	assertion.common.authority_generation = 3;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_base_bind_exact(
		&assertion.common.logical_assertion, 17, 3, &authority),
		RESOURCE_X_APPLY_APPLIED);

	/* A real predecessor authority transition may make an admitted FIFO
	 * successor's base stale before that successor sends ASSERT_X.  Only the
	 * head-only adapter entry point may update an otherwise pristine semantic
	 * state, and exact replay is a duplicate. */
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_S_TO_N_RELEASE, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(authority.transition_count, UINT64_C(4));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_base_bind_exact(
		&assertion.common.logical_assertion, 17, 4, &authority),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_head_rebind_exact(
		&assertion.common.logical_assertion, 41, 73, 17, 4, &authority),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_head_rebind_exact(
		&assertion.common.logical_assertion, 41, 73, 17, 4, &authority),
		RESOURCE_X_APPLY_DUPLICATE);

	assertion.common.base_authority_generation = 4;
	assertion.common.authority_generation = 4;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);
	/* ASSERT is the atomic native S-admission barrier: an unrelated legacy
	 * N->S apply cannot manufacture a later base behind the canonical head. */
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_N_TO_S, 0), PCM_GCS_TRANSITION_PENDING_X);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(authority.transition_count, UINT64_C(4));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_head_rebind_exact(
		&assertion.common.logical_assertion, 41, 73, 17, 4, &authority),
		RESOURCE_X_APPLY_STALE);

	/* A prior request's SETTLED record is terminal history, not evidence that
	 * the promoted successor already sent ASSERT_X.  Preserve that history,
	 * accept the successor's exact admission once, and still reject any
	 * rebind after the successor assertion exists. */
	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	predecessor = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 3, 3);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&predecessor, 3, &snapshot), RESOURCE_X_APPLY_APPLIED);
	memset(&durable, 0, sizeof(durable));
	durable.assertion = predecessor.common.logical_assertion;
	durable.base_authority_generation = 1;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = 41;
	durable.requester_target_generation = 41;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x12345678);
	durable.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(
		&durable, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
		&predecessor.common.logical_assertion, &grant_intent, grant_bytes,
		sizeof(grant_bytes)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_stage_exact(
		&grant_intent, 101), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_grant_intent_complete_exact(
		&grant_intent));
	settlement = make_resource_x_master_frame(
		RESOURCE_X_WIRE_INSTALL_SETTLEMENT, tag, 3, 3);
	settlement.common.outcome = RESOURCE_X_OUTCOME_OK;
	settlement.common.authority_generation = 2;
	settlement.body.install_settlement.conversion_base_generation = 1;
	settlement.body.install_settlement.final_authority_generation = 2;
	settlement.body.install_settlement.requester_connection_generation = 91;
	settlement.body.install_settlement.requester_target_generation = 41;
	settlement.body.install_settlement.page_scn_lsn = 82;
	settlement.body.install_settlement.page_checksum = UINT32_C(0x12345678);
	settlement.body.install_settlement.source_proof_crc32c
		= UINT32_C(0x87654321);
	settlement.body.install_settlement.installed_mode = PCM_STATE_X;
	settlement.body.install_settlement.requester_role
		= RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
	settlement.body.install_settlement.terminal_outcome = RESOURCE_X_OUTCOME_OK;
	settlement.body.install_settlement.terminal_state
		= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
		&settlement, 3, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_SETTLED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_X_TO_N_RELEASE, 3), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_N_TO_X, 3), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 0, 78),
				 PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(authority.transition_count, UINT64_C(3));
	successor = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 0, 0);
	successor.common.base_authority_generation = 3;
	successor.common.authority_generation = 3;
	successor.common.assertion_sequence = 42;
	/* A claimed pending-X barrier may coexist with the one pre-ASSERT
	 * late-bind only when its queue cookie names this exact ticket. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_head_rebind_exact(
		&successor.common.logical_assertion, 42, 79, 17, 3, &authority),
		RESOURCE_X_APPLY_INVALID);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_head_rebind_exact(
		&successor.common.logical_assertion, 42, 78, 17, 3, &authority),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_head_rebind_exact(
		&successor.common.logical_assertion, 42, 78, 17, 3, &authority),
		RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&successor, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_head_rebind_exact(
		&successor.common.logical_assertion, 42, 78, 17, 3, &authority),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT(cluster_pcm_lock_clear_queue_pending_x_exact(tag, 0, 78));
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_X_TO_N_RELEASE, 3), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_head_rebind_exact(
		&successor.common.logical_assertion, 42, 78, 17, 4, &authority),
		RESOURCE_X_APPLY_STALE);
}

UT_TEST(test_resource_x_settled_retirement_tombstone_replays_and_frees_live_slot)
{
	BufferTag tag = make_tag(169);
	PcmAuthoritySnapshot authority;
	ResourceXDecodedFrame first_assert;
	ResourceXDecodedFrame next_assert;
	ResourceXDecodedFrame settlement;
	ResourceXDurableProof durable;
	ResourceXMasterSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_N_TO_X, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_X_TO_N_RELEASE, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(authority.transition_count, UINT64_C(2));
	first_assert = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 3, 3);
	first_assert.common.base_authority_generation = 2;
	first_assert.common.authority_generation = 2;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_base_bind_exact(
		&first_assert.common.logical_assertion, 17, 2, &authority),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&first_assert, 3, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_PROOF);

	memset(&durable, 0, sizeof(durable));
	durable.assertion = first_assert.common.logical_assertion;
	durable.base_authority_generation = 2;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = 41;
	durable.requester_target_generation = 41;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x12345678);
	durable.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(
		&durable, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
	UT_ASSERT_EQ(snapshot.final_authority_generation, UINT64_C(3));

	settlement = make_resource_x_master_frame(
		RESOURCE_X_WIRE_INSTALL_SETTLEMENT, tag, 3, 3);
	settlement.common.outcome = RESOURCE_X_OUTCOME_OK;
	settlement.common.base_authority_generation = 2;
	settlement.common.authority_generation = 3;
	settlement.body.install_settlement.conversion_base_generation = 2;
	settlement.body.install_settlement.final_authority_generation = 3;
	settlement.body.install_settlement.requester_connection_generation = 91;
	settlement.body.install_settlement.requester_target_generation = 41;
	settlement.body.install_settlement.page_scn_lsn = 82;
	settlement.body.install_settlement.page_checksum
		= UINT32_C(0x12345678);
	settlement.body.install_settlement.source_proof_crc32c
		= UINT32_C(0x87654321);
	settlement.body.install_settlement.installed_mode = PCM_STATE_X;
	settlement.body.install_settlement.requester_role
		= RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
	settlement.body.install_settlement.terminal_outcome
		= RESOURCE_X_OUTCOME_OK;
	settlement.body.install_settlement.terminal_state
		= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
		&settlement, 3, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_SETTLED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_settled_retire_exact(
		&first_assert.common.logical_assertion, 41, &snapshot),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_settled_retire_exact(
		&first_assert.common.logical_assertion, 41, &snapshot),
		RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&first_assert.common.logical_assertion, &snapshot),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_SETTLED);
	UT_ASSERT_EQ(snapshot.assertion_sequence, UINT64_C(41));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
		&settlement, 3, &snapshot), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_SETTLED);

	/* The live conversion slot is no longer the granted-holder authority. */
	next_assert = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 3, 3);
	next_assert.common.assertion_sequence = 42;

	/* The old physical X may independently become one of multiple current S
	 * holders.  A fresh assertion must recompute blockers from GRD rather than
	 * reusing the terminal conversion's sole-S shape. */
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_X_TO_S_DOWNGRADE, 3), PCM_GCS_TRANSITION_APPLIED);
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 0;
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(authority.state, PCM_STATE_S);
	UT_ASSERT_EQ(authority.x_holder_node, -1);
	UT_ASSERT_EQ(authority.s_holders_bitmap,
		(UINT32_C(1) << 1) | (UINT32_C(1) << 3));
	UT_ASSERT_EQ(authority.transition_count, UINT64_C(4));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_head_rebind_exact(
		&next_assert.common.logical_assertion, 42, 73, 17, 4, &authority),
		RESOURCE_X_APPLY_APPLIED);
	next_assert.common.base_authority_generation = 4;
	next_assert.common.authority_generation = 4;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&next_assert, 3, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(snapshot.incompatible_holders_bitmap, UINT32_C(1) << 1);
	UT_ASSERT_EQ(snapshot.base_authority_generation, UINT64_C(4));
	UT_ASSERT_EQ(snapshot.assertion_sequence, UINT64_C(42));

	/* Exact late terminal frames remain idempotent even after a fresh live
	 * request from the same requester occupies the slot. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&first_assert, 3, &snapshot), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_SETTLED);
	UT_ASSERT_EQ(snapshot.assertion_sequence, UINT64_C(41));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
		&settlement, 3, &snapshot), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_SETTLED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&next_assert.common.logical_assertion, &snapshot),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(snapshot.assertion_sequence, UINT64_C(42));
}

UT_TEST(test_resource_x_tombstone_lineage_drift_remains_terminal)
{
	BufferTag tag = make_tag(170);
	PcmAuthoritySnapshot authority;
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame release;
	ResourceXDecodedFrame settlement;
	ResourceXDurableProof durable;
	ResourceXMasterSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_N_TO_X, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_X_TO_N_RELEASE, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));

	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 3, 3);
	assertion.common.base_authority_generation = 2;
	assertion.common.authority_generation = 2;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_base_bind_exact(
		&assertion.common.logical_assertion, 17, 2, &authority),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 3, &snapshot), RESOURCE_X_APPLY_APPLIED);

	memset(&durable, 0, sizeof(durable));
	durable.assertion = assertion.common.logical_assertion;
	durable.base_authority_generation = 2;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = 41;
	durable.requester_target_generation = 41;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x12345678);
	durable.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(
		&durable, &snapshot), RESOURCE_X_APPLY_APPLIED);

	settlement = make_resource_x_master_frame(
		RESOURCE_X_WIRE_INSTALL_SETTLEMENT, tag, 3, 3);
	settlement.common.outcome = RESOURCE_X_OUTCOME_OK;
	settlement.common.base_authority_generation = 2;
	settlement.common.authority_generation = 3;
	settlement.body.install_settlement.conversion_base_generation = 2;
	settlement.body.install_settlement.final_authority_generation = 3;
	settlement.body.install_settlement.requester_connection_generation = 91;
	settlement.body.install_settlement.requester_target_generation = 41;
	settlement.body.install_settlement.page_scn_lsn = 82;
	settlement.body.install_settlement.page_checksum
		= UINT32_C(0x12345678);
	settlement.body.install_settlement.source_proof_crc32c
		= UINT32_C(0x87654321);
	settlement.body.install_settlement.installed_mode = PCM_STATE_X;
	settlement.body.install_settlement.requester_role
		= RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
	settlement.body.install_settlement.terminal_outcome
		= RESOURCE_X_OUTCOME_OK;
	settlement.body.install_settlement.terminal_state
		= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
		&settlement, 3, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_settled_retire_exact(
		&assertion.common.logical_assertion, 41, &snapshot),
		RESOURCE_X_APPLY_APPLIED);

	/* The same physical holder may have crossed a later legacy episode before
	 * its delayed kind-4 arrives.  The old tombstone is retained evidence, not
	 * mutable recovery state. */
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_X_TO_N_RELEASE, 3), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_N_TO_X, 3), PCM_GCS_TRANSITION_APPLIED);

	release = make_resource_x_master_frame(
		RESOURCE_X_WIRE_RELEASE_X, tag, 3, 3);
	release.common.observed_mode = PCM_STATE_X;
	release.common.target_mode = PCM_STATE_N;
	release.common.outcome = RESOURCE_X_OUTCOME_OK;
	release.common.base_authority_generation = 2;
	release.common.authority_generation = 3;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_release_x_exact(
		&release, 3, &snapshot), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_SETTLED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&assertion.common.logical_assertion, &snapshot),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_SETTLED);
}

UT_TEST(test_pcm_protocol_debt_projection_rejects_settled_without_cached_or_pi)
{
	BufferTag tag = make_tag(242);
	PcmAuthoritySnapshot authority;
	PcmGrdProtocolDebtStats debt;
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame settlement;
	ResourceXDurableProof durable;
	ResourceXIntentSlot grant_intent;
	ResourceXMasterSnapshot snapshot;
	uint8 grant_bytes[RESOURCE_X_PROOF_V1_BYTES];

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_N_TO_X, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_X_TO_N_RELEASE, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));

	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 3, 3);
	assertion.common.base_authority_generation = authority.transition_count;
	assertion.common.authority_generation = authority.transition_count;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_base_bind_exact(
		&assertion.common.logical_assertion, 17,
		authority.transition_count, &authority),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 3, &snapshot), RESOURCE_X_APPLY_APPLIED);

	memset(&durable, 0, sizeof(durable));
	durable.assertion = assertion.common.logical_assertion;
	durable.base_authority_generation = authority.transition_count;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = assertion.common.assertion_sequence;
	durable.requester_target_generation = 41;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x12345678);
	durable.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(
		&durable, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
		&assertion.common.logical_assertion, &grant_intent, grant_bytes,
		sizeof(grant_bytes)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_stage_exact(
		&grant_intent, 101), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_grant_intent_complete_exact(
		&grant_intent));

	settlement = make_resource_x_master_frame(
		RESOURCE_X_WIRE_INSTALL_SETTLEMENT, tag, 3, 3);
	settlement.common.outcome = RESOURCE_X_OUTCOME_OK;
	settlement.common.base_authority_generation
		= durable.base_authority_generation;
	settlement.common.authority_generation
		= snapshot.final_authority_generation;
	settlement.body.install_settlement.conversion_base_generation
		= durable.base_authority_generation;
	settlement.body.install_settlement.final_authority_generation
		= snapshot.final_authority_generation;
	settlement.body.install_settlement.requester_connection_generation = 91;
	settlement.body.install_settlement.requester_target_generation = 41;
	settlement.body.install_settlement.page_scn_lsn = durable.page_scn_lsn;
	settlement.body.install_settlement.page_checksum = durable.page_checksum;
	settlement.body.install_settlement.source_proof_crc32c
		= durable.source_proof_crc32c;
	settlement.body.install_settlement.installed_mode = PCM_STATE_X;
	settlement.body.install_settlement.requester_role
		= RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
	settlement.body.install_settlement.terminal_outcome
		= RESOURCE_X_OUTCOME_OK;
	settlement.body.install_settlement.terminal_state
		= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
		&settlement, 3, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_settled_retire_exact(
		&assertion.common.logical_assertion,
		assertion.common.assertion_sequence, &snapshot),
		RESOURCE_X_APPLY_APPLIED);

	cluster_pcm_grd_protocol_debt_snapshot(&debt);
	UT_ASSERT_EQ(debt.retained_entry_count, UINT64_C(0));
	UT_ASSERT_EQ(debt.active_resource_x_entry_count, UINT64_C(0));
	UT_ASSERT_EQ(debt.local_owner_entry_count, UINT64_C(0));
	UT_ASSERT_EQ(debt.invalid_entry_count, UINT64_C(0));

	/* A raw physical X->N without RELEASE_X settlement leaves no cached
	 * carrier and no durable PI for the SETTLED tombstone.  The projection
	 * must report this cross-axis terminal residue as invalid, not clean. */
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_X_TO_N_RELEASE, 3), PCM_GCS_TRANSITION_APPLIED);
	cluster_pcm_grd_protocol_debt_snapshot(&debt);
	UT_ASSERT_EQ(debt.retained_entry_count, UINT64_C(0));
	UT_ASSERT_EQ(debt.active_resource_x_entry_count, UINT64_C(0));
	UT_ASSERT_EQ(debt.invalid_entry_count, UINT64_C(1));
}

UT_TEST(test_resource_x_remote_lane0_settlement_retires_only_after_exact_source_ack)
{
	BufferTag tag = make_tag(193);
	ResourceXDecodedFrame bootstrap;
	ResourceXDecodedFrame bootstrap_ack;
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame blocked;
	ResourceXDecodedFrame settlement;
	ResourceXDecodedFrame source_request;
	ResourceXDecodedFrame source_ack;
	ResourceXDecodedFrame stale_ack;
	ResourceXDecodedFrame successor_request;
	ResourceXDecodedFrame successor_retry;
	ResourceXDecodedFrame successor_ack;
	ResourceXDecodedFrame release;
	ResourceXIntentSlot intent;
	ResourceXReconfigBatch batch;
	ResourceXReconfigResult reconfig_result;
	ResourceXReconfigToken token;
	ResourceXApplyResult retry_result;
	ResourceXZeroResidualProof zero;
	ResourceXMasterSnapshot after_ack;
	ResourceXMasterSnapshot settled_snapshot;
	ResourceXMasterSnapshot snapshot;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 payload[RESOURCE_X_PROOF_V1_BYTES];

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	bootstrap = make_resource_x_bootstrap_request(tag, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&bootstrap, 2, 61, 77, 31, 71, &bootstrap_ack),
		RESOURCE_X_APPLY_APPLIED);
	assertion = bootstrap_ack;
	assertion.kind = RESOURCE_X_WIRE_ASSERT_X;
	assertion.common.sender_connection_generation = 51;
	assertion.common.authority_generation
		= assertion.common.base_authority_generation;
	assertion.common.outcome = RESOURCE_X_OUTCOME_NONE;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&assertion, 2, 61, 77, 31, 71, &snapshot),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);

	/* A requester that is still an unresolved physical blocker of the active
	 * predecessor is not otherwise admissible for NEXT_ADMISSION.  Its local
	 * type-17 transition can invalidate this pre-ASSERT attempt before the
	 * predecessor retires, so retaining it would orphan the bounded priority
	 * and reject the required higher attempt after settlement. */
	successor_request = make_resource_x_bootstrap_request_values(
		tag, 0, 17, 31, 41, 53);
	memset(&successor_ack, 0, sizeof(successor_ack));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&successor_request, 0, 63, 77, 31, 71, &successor_ack),
		RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(successor_ack.common.base_authority_generation,
		UINT64_C(0));

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_intent_snapshot_exact(
		&assertion.common.logical_assertion, 0, &intent, payload,
		sizeof(payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&intent, 101), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&intent));

	blocked = make_resource_x_remote_blocked_frame(tag, 2, 0);
	blocked.common.base_authority_generation
		= assertion.common.base_authority_generation;
	blocked.common.authority_generation
		= assertion.common.base_authority_generation;
	blocked.common.assertion_sequence = assertion.common.assertion_sequence;
	blocked.common.ordered_lane = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(
		&blocked, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
	UT_ASSERT_EQ(snapshot.proof_kind, RESOURCE_X_PROOF_REMOTE_CARRIER);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
		&assertion.common.logical_assertion, &intent, payload,
		sizeof(payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_stage_exact(
		&intent, 102), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_grant_intent_complete_exact(
		&intent));

	settlement = make_resource_x_master_frame(
		RESOURCE_X_WIRE_INSTALL_SETTLEMENT, tag, 2, 2);
	settlement.common.base_authority_generation
		= assertion.common.base_authority_generation;
	settlement.common.resource_formation
		= assertion.common.resource_formation;
	settlement.common.master_session_incarnation
		= assertion.common.master_session_incarnation;
	settlement.common.assertion_sequence
		= assertion.common.assertion_sequence;
	settlement.common.ordered_lane = 0;
	settlement.common.authority_generation
		= snapshot.final_authority_generation;
	settlement.common.outcome = RESOURCE_X_OUTCOME_OK;
	settlement.body.install_settlement.conversion_base_generation
		= settlement.common.base_authority_generation;
	settlement.body.install_settlement.final_authority_generation
		= settlement.common.authority_generation;
	settlement.body.install_settlement.requester_connection_generation = 91;
	settlement.body.install_settlement.requester_target_generation
		= blocked.body.blocked_to_n.requester_target_generation;
	settlement.body.install_settlement.page_scn_lsn
		= blocked.body.blocked_to_n.page_scn_lsn;
	settlement.body.install_settlement.page_checksum
		= blocked.body.blocked_to_n.page_checksum;
	settlement.body.install_settlement.source_proof_crc32c
		= blocked.body.blocked_to_n.source_proof_crc32c;
	settlement.body.install_settlement.installed_mode = PCM_STATE_X;
	settlement.body.install_settlement.requester_role
		= RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
	settlement.body.install_settlement.terminal_outcome
		= RESOURCE_X_OUTCOME_OK;
	settlement.body.install_settlement.terminal_state
		= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
		&settlement, 2, &settled_snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(settled_snapshot.phase, RESOURCE_X_MASTER_SETTLED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_intent_snapshot_exact(
		&assertion.common.logical_assertion, &intent, payload,
		sizeof(payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(intent.body.owner_kind,
		RESOURCE_X_INTENT_OWNER_HOLDER_RELEASE);
	UT_ASSERT_EQ(intent.destination_node, UINT32_C(0));
	UT_ASSERT_EQ(intent.kind, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	UT_ASSERT_EQ(intent.payload_bytes, RESOURCE_X_PROOF_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_BLOCK_TO_N, payload, intent.payload_bytes,
		&source_request, &reject));
	UT_ASSERT_EQ(source_request.common.authority_generation,
		settled_snapshot.final_authority_generation);
	UT_ASSERT_EQ(source_request.body.blocked_to_n.source_carrier_generation,
		blocked.body.blocked_to_n.source_carrier_generation);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_settled_retire_exact(
		&assertion.common.logical_assertion,
		assertion.common.assertion_sequence, &settled_snapshot),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_intent_snapshot_exact(
		&assertion.common.logical_assertion, &intent, payload,
		sizeof(payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&assertion.common.logical_assertion, &snapshot),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_SETTLED);

	/* The live conversion is terminal, but node 0 remains the selected
	 * former-source while its exact physical release is protocol debt.  Its
	 * local successor is not otherwise admissible under the SourceSettlement
	 * interlock, so this rejected attempt must not claim master priority. */
	memset(&successor_ack, 0, sizeof(successor_ack));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&successor_request, 0, 63, 77, 31, 71, &successor_ack),
		RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(successor_ack.common.base_authority_generation,
		UINT64_C(0));

	source_ack = source_request;
	source_ack.kind = RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2;
	source_ack.common.sender_connection_generation = 64;
	source_ack.common.outcome = RESOURCE_X_OUTCOME_OK;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_ack_exact(
		&source_ack, 0, &after_ack), RESOURCE_X_APPLY_APPLIED);
	/* Pair retention canceled the unbound local attempt and preserved its
	 * floor, so the requester legitimately retries with a higher attempt.  It
	 * must sample the current base immediately after the old carrier settles;
	 * an orphan priority for attempt 41 would incorrectly reject it. */
	successor_retry = make_resource_x_bootstrap_request_values(
		tag, 0, 17, 31, 42, 53);
	retry_result = cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&successor_retry, 0, 63, 77, 31, 71, &successor_ack);
	UT_ASSERT_EQ(retry_result, RESOURCE_X_APPLY_APPLIED);
	if (retry_result != RESOURCE_X_APPLY_APPLIED)
		return;
	UT_ASSERT_EQ(successor_ack.common.base_authority_generation,
		settled_snapshot.final_authority_generation);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&assertion.common.logical_assertion, &after_ack),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&after_ack, &snapshot, sizeof(snapshot)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_ack_exact(
		&source_ack, 0, &after_ack), RESOURCE_X_APPLY_DUPLICATE);
	stale_ack = source_ack;
	stale_ack.body.blocked_to_n.source_carrier_generation++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_ack_exact(
		&stale_ack, 0, &snapshot), RESOURCE_X_APPLY_STALE);

	/* The exact ACK tombstone is terminal, but R8 owns formation retirement.
	 * It clears the old bounded source debt before producing the same-token
	 * zero proof; R10 then observes no logical or physical retry debt. */
	release = make_resource_x_master_frame(
		RESOURCE_X_WIRE_RELEASE_X, tag, 2, 2);
	release.common.base_authority_generation
		= source_request.common.base_authority_generation;
	release.common.authority_generation
		= source_request.common.authority_generation;
	release.common.resource_formation
		= source_request.common.resource_formation;
	release.common.master_session_incarnation
		= source_request.common.master_session_incarnation;
	release.common.assertion_sequence
		= source_request.common.assertion_sequence;
	release.common.ordered_lane = 0;
	release.common.observed_mode = PCM_STATE_X;
	release.common.target_mode = PCM_STATE_N;
	release.common.outcome = RESOURCE_X_OUTCOME_OK;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_release_x_exact(
		&release, 2, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_RELEASED);
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	do {
		reconfig_result = cluster_resource_x_reconfig_sweep(
			&token, 4, &batch);
	} while (reconfig_result == RESOURCE_X_RECONFIG_MORE
		|| reconfig_result == RESOURCE_X_RECONFIG_RETRY);
	UT_ASSERT_EQ(reconfig_result, RESOURCE_X_RECONFIG_DONE);
	UT_ASSERT(cluster_resource_x_reconfig_zero_proof_exact(&token, &zero));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_ack_exact(
		&source_ack, 0, &after_ack), RESOURCE_X_APPLY_NOT_FOUND);
}

UT_TEST(test_resource_x_adapter_successor_samples_only_exact_canonical_base)
{
	BufferTag tag = make_tag(158);
	PcmAuthoritySnapshot authority;
	ResourceXDecodedFrame predecessor;
	ResourceXDecodedFrame successor;
	ResourceXMasterSnapshot snapshot;
	uint64 canonical_base = 0;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_N_TO_S, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(authority.transition_count, UINT64_C(1));
	predecessor = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 3, 3);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_base_bind_exact(
		&predecessor.common.logical_assertion, 17, 1, &authority),
		RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&predecessor, 3, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 77),
				 PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	successor = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 0, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_successor_base_exact(
		&successor.common.logical_assertion, 17, &authority,
		&canonical_base), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(canonical_base, UINT64_C(1));

	/* The caller's complete legacy snapshot is an optimistic token.  Once
	 * the exact queue barrier changes, neither that token nor its sampled
	 * canonical base may be reused. */
	UT_ASSERT(cluster_pcm_lock_clear_queue_pending_x_exact(tag, 3, 77));
	canonical_base = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_successor_base_exact(
		&successor.common.logical_assertion, 17, &authority,
		&canonical_base), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(canonical_base, UINT64_C(0));
}

UT_TEST(test_resource_x_master_arms_exact_block_to_n_intent_per_holder)
{
	BufferTag tag = make_tag(149);
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame decoded;
	ResourceXIntentSlot intent;
	ResourceXMasterSnapshot snapshot;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 payload[RESOURCE_X_PROOF_V1_BYTES];
	uint32 examined = 0;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 2, 2);

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 2, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(snapshot.incompatible_holders_bitmap, UINT32_C(1));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_intent_snapshot_exact(
		&assertion.common.logical_assertion, 0, &intent, payload,
		sizeof(payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(intent.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(intent.logical_generation, 41);
	UT_ASSERT_EQ(intent.authority_generation, 1);
	UT_ASSERT_EQ(intent.destination_node, 0);
	UT_ASSERT_EQ(intent.payload_bytes, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT_EQ(intent.kind, RESOURCE_X_WIRE_BLOCK_TO_N);
	UT_ASSERT_EQ(intent.body.owner_kind,
		RESOURCE_X_INTENT_OWNER_MASTER_BLOCK);
	UT_ASSERT_EQ(intent.body.owner_node, 0);
	UT_ASSERT_EQ(intent.body.owner_index, 0);
	UT_ASSERT(resource_x_assertion_equal(&intent.body.assertion,
		&assertion.common.logical_assertion));
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_BLOCK_TO_N, payload,
		RESOURCE_X_CONTROL_V1_BYTES,
		&decoded, &reject));
	UT_ASSERT_EQ(decoded.kind, RESOURCE_X_WIRE_BLOCK_TO_N);
	UT_ASSERT(resource_x_assertion_equal(&decoded.common.logical_assertion,
		&assertion.common.logical_assertion));
	UT_ASSERT_EQ(decoded.common.action_node, 0);
	UT_ASSERT_EQ(decoded.common.observed_mode, PCM_STATE_X);
	UT_ASSERT_EQ(decoded.common.target_mode, PCM_STATE_N);
	UT_ASSERT_EQ(decoded.common.source_candidate, 1);
	UT_ASSERT_EQ(decoded.common.retain_pi_if_dirty, 1);
	UT_ASSERT_EQ(decoded.common.sender_connection_generation, 1);
	UT_ASSERT(decoded.common.sender_connection_generation
		!= assertion.common.sender_connection_generation);
	UT_ASSERT_EQ(decoded.common.authority_generation, 1);
	UT_ASSERT_EQ(decoded.common.outcome, RESOURCE_X_OUTCOME_NONE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_intent_snapshot_exact(
		&assertion.common.logical_assertion, 1, &intent, payload,
		sizeof(payload)), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_probe_exact(
		4, &intent, payload, sizeof(payload), &examined),
		RESOURCE_X_INTENT_PROBE_FOUND);
	UT_ASSERT(examined >= 1 && examined <= 4);
	UT_ASSERT_EQ(intent.body.owner_kind,
		RESOURCE_X_INTENT_OWNER_MASTER_BLOCK);
	UT_ASSERT_EQ(intent.body.owner_index, 0);
}

static ResourceXDecodedFrame
resource_x_block_intent_frame(const ResourceXAssertion *assertion,
							  int32 holder_node)
{
	ResourceXDecodedFrame decoded;
	ResourceXIntentSlot intent;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 payload[RESOURCE_X_PROOF_V1_BYTES];

	memset(&decoded, 0, sizeof(decoded));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_intent_snapshot_exact(
		assertion, holder_node, &intent, payload, sizeof(payload)),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(intent.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(intent.destination_node, (uint32)holder_node);
	UT_ASSERT_EQ(intent.payload_bytes, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_BLOCK_TO_N, payload, RESOURCE_X_CONTROL_V1_BYTES,
		&decoded, &reject));
	return decoded;
}

UT_TEST(test_resource_x_requester_absent_single_s_selects_only_holder)
{
	BufferTag tag = make_tag(176);
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame block;
	ResourceXMasterSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 3;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 1, 1);

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(snapshot.incompatible_holders_bitmap, UINT32_C(0x8));
	UT_ASSERT_EQ(snapshot.source_node, 3);
	UT_ASSERT_EQ(snapshot.proof_kind, 0);
	block = resource_x_block_intent_frame(
		&assertion.common.logical_assertion, 3);
	UT_ASSERT_EQ(block.common.observed_mode, PCM_STATE_S);
	UT_ASSERT_EQ(block.common.source_candidate, 1);
	UT_ASSERT_EQ(block.common.retain_pi_if_dirty, 1);
}

UT_TEST(test_resource_x_requester_absent_multi_s_prefers_master_local)
{
	BufferTag tag = make_tag(177);
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame local_block;
	ResourceXDecodedFrame peer_block;
	ResourceXMasterSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 3;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 1, 1);

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.incompatible_holders_bitmap, UINT32_C(0x9));
	UT_ASSERT_EQ(snapshot.source_node, 0);
	local_block = resource_x_block_intent_frame(
		&assertion.common.logical_assertion, 0);
	peer_block = resource_x_block_intent_frame(
		&assertion.common.logical_assertion, 3);
	UT_ASSERT_EQ(local_block.common.source_candidate, 1);
	UT_ASSERT_EQ(local_block.common.retain_pi_if_dirty, 1);
	UT_ASSERT_EQ(peer_block.common.source_candidate, 0);
	UT_ASSERT_EQ(peer_block.common.retain_pi_if_dirty, 0);
}

UT_TEST(test_resource_x_requester_absent_multi_s_uses_lowest_stable_peer)
{
	BufferTag tag = make_tag(178);
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame lower_block;
	ResourceXDecodedFrame upper_block;
	ResourceXMasterSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 3;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 1, 1);

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.incompatible_holders_bitmap, UINT32_C(0xc));
	UT_ASSERT_EQ(snapshot.source_node, 2);
	lower_block = resource_x_block_intent_frame(
		&assertion.common.logical_assertion, 2);
	upper_block = resource_x_block_intent_frame(
		&assertion.common.logical_assertion, 3);
	UT_ASSERT_EQ(lower_block.common.source_candidate, 1);
	UT_ASSERT_EQ(lower_block.common.retain_pi_if_dirty, 1);
	UT_ASSERT_EQ(upper_block.common.source_candidate, 0);
	UT_ASSERT_EQ(upper_block.common.retain_pi_if_dirty, 0);
}

UT_TEST(test_resource_x_requester_present_does_not_select_remote_s_carrier)
{
	BufferTag tag = make_tag(179);
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame peer_block;
	ResourceXMasterSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 3;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 1, 1);

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.incompatible_holders_bitmap, UINT32_C(0x8));
	UT_ASSERT_EQ(snapshot.source_node, -1);
	peer_block = resource_x_block_intent_frame(
		&assertion.common.logical_assertion, 3);
	UT_ASSERT_EQ(peer_block.common.source_candidate, 0);
	UT_ASSERT_EQ(peer_block.common.retain_pi_if_dirty, 0);
}

UT_TEST(test_resource_x_duplicate_assertion_preserves_selected_s_carrier)
{
	BufferTag tag = make_tag(180);
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame source_block;
	ResourceXIntentSlot intent;
	ResourceXMasterSnapshot snapshot;
	uint8 payload[RESOURCE_X_PROOF_V1_BYTES];

	reset_fake_pcm_runtime(4);
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 3;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 1, 1);

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.source_node, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_intent_snapshot_exact(
		&assertion.common.logical_assertion, 2, &intent, payload,
		sizeof(payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&intent, intent.first_armed_us + 1), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&intent));

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 1, &snapshot), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(snapshot.source_node, 2);
	source_block = resource_x_block_intent_frame(
		&assertion.common.logical_assertion, 2);
	UT_ASSERT_EQ(source_block.common.observed_mode, PCM_STATE_S);
	UT_ASSERT_EQ(source_block.common.source_candidate, 1);
	UT_ASSERT_EQ(source_block.common.retain_pi_if_dirty, 1);
}

UT_TEST(test_resource_x_existing_selected_s_intent_conflict_fails_closed)
{
	BufferTag tag = make_tag(181);
	ResourceXDecodedFrame assertion;
	ResourceXMasterSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 3;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 1, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.source_node, 3);

	/* The same retained owner now conflicts with the live source mode.  A
	 * duplicate assertion must not overwrite its S/1/1 bytes with X/1/1. */
	cluster_node_id = 3;
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_S_TO_X_UPGRADE, 3), PCM_GCS_TRANSITION_APPLIED);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 1, &snapshot), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(snapshot.source_node, 3);
}

UT_TEST(test_resource_x_source_pair_rejects_status_image_mode_mismatch)
{
	BufferTag tag = make_tag(182);
	ResourceXDecodedFrame block;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame status;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	block = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCK_TO_N, tag, 2, 0);
	block.common.observed_mode = PCM_STATE_X;
	block.common.target_mode = PCM_STATE_N;
	block.common.source_candidate = 1;
	block.common.retain_pi_if_dirty = 1;
	make_resource_x_remote_join_pair(tag, 2, &grant, &image);
	status = make_resource_x_remote_blocked_frame(tag, 2, 0);
	status.common.observed_mode = PCM_STATE_S;
	status.body.blocked_to_n.source_proof_crc32c
		= image.common.semantic_crc32c;

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_source_exact(
		&block, 0, &status, &image), RESOURCE_X_APPLY_INVALID);
}

UT_TEST(test_resource_x_s_partial_source_polarity_is_rejected_by_consumer)
{
	BufferTag tag = make_tag(183);
	ResourceXDecodedFrame block;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	block = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCK_TO_N, tag, 2, 0);
	block.common.observed_mode = PCM_STATE_S;
	block.common.target_mode = PCM_STATE_N;
	block.common.source_candidate = 1;
	block.common.retain_pi_if_dirty = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_exact(
		&block, 0), RESOURCE_X_APPLY_BAD_STATE);

	block.common.source_candidate = 0;
	block.common.retain_pi_if_dirty = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_exact(
		&block, 0), RESOURCE_X_APPLY_BAD_STATE);
}

UT_TEST(test_resource_x_prepared_s_source_retains_one_exact_matching_pair)
{
	BufferTag tag = make_tag(184);
	ClusterPcmOwnSnapshot prepared;
	ResourceXDecodedFrame block;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame retained_image;
	ResourceXDecodedFrame retained_status;
	ResourceXDecodedFrame status;
	ResourceXIntentSlot image_intent;
	ResourceXIntentSlot status_intent;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 image_bytes[RESOURCE_X_IMAGE_V1_BYTES];
	uint8 retained_image_bytes[RESOURCE_X_IMAGE_V1_BYTES];
	uint8 retained_status_bytes[RESOURCE_X_PROOF_V1_BYTES];
	uint64 source_generation = 0;
	uint16 image_bytes_len = 0;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	fake_gcs_master_node = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	block = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCK_TO_N, tag, 2, 0);
	block.common.observed_mode = PCM_STATE_S;
	block.common.target_mode = PCM_STATE_N;
	block.common.source_candidate = 1;
	block.common.retain_pi_if_dirty = 1;
	make_resource_x_remote_join_pair(tag, 2, &grant, &image);
	image.common.observed_mode = PCM_STATE_S;
	image.body.image_envelope.source_fence[28] = PCM_STATE_S;
	((PageHeader)image.body.image_envelope.page_bytes)->pd_block_scn
		= image.body.image_envelope.page_scn_lsn;
	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, &image, image_bytes,
		sizeof(image_bytes), &image_bytes_len, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, image_bytes, image_bytes_len,
		&image, &reject));
	status = make_resource_x_remote_blocked_frame(tag, 2, 0);
	status.common.observed_mode = PCM_STATE_S;
	memcpy(status.body.blocked_to_n.source_fence,
		image.body.image_envelope.source_fence,
		sizeof(status.body.blocked_to_n.source_fence));
	status.body.blocked_to_n.source_proof_crc32c
		= image.common.semantic_crc32c;

	memset(&prepared, 0, sizeof(prepared));
	prepared.tag = tag;
	prepared.generation
		= image.body.image_envelope.source_carrier_generation - 1;
	prepared.reservation_token = 77;
	prepared.flags = PCM_OWN_FLAG_REVOKING;
	prepared.pcm_state = PCM_STATE_S;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_block_to_n_prepared_s_source_exact(
			&block, 1, &status, &image, &prepared,
			PageGetLSN((Page)image.body.image_envelope.page_bytes),
			image.body.image_envelope.page_scn_lsn,
			image.body.image_envelope.page_checksum),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_exact(
		&block.common.logical_assertion, &retained_status),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_exact(
		&block.common.logical_assertion, &retained_image),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(retained_status.common.observed_mode, PCM_STATE_S);
	UT_ASSERT_EQ(retained_image.common.observed_mode, PCM_STATE_S);
	UT_ASSERT_EQ(retained_status.body.blocked_to_n.page_scn_lsn,
		retained_image.body.image_envelope.page_scn_lsn);
	UT_ASSERT_EQ(retained_status.body.blocked_to_n.page_checksum,
		retained_image.body.image_envelope.page_checksum);
	UT_ASSERT_EQ(cluster_pcm_lock_query(tag), PCM_LOCK_MODE_N);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_publish_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		1, block.common.master_session_incarnation),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
		&block.common.logical_assertion, &status_intent,
		retained_status_bytes, sizeof(retained_status_bytes)),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_intent_snapshot_exact(
		&block.common.logical_assertion, &image_intent,
		retained_image_bytes, sizeof(retained_image_bytes)),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&status_intent, status_intent.first_armed_us + 1),
		RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&status_intent));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&image_intent, image_intent.first_armed_us + 1),
		RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&image_intent));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_drain_prepare_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		1, block.common.master_session_incarnation, &source_generation),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(source_generation, prepared.generation);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_drain_commit_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		1, block.common.master_session_incarnation, source_generation),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_drain_commit_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		1, block.common.master_session_incarnation, source_generation),
		RESOURCE_X_APPLY_DUPLICATE);
}

UT_TEST(test_resource_x_selected_s_proof_and_noncarrier_status_commit_once)
{
	BufferTag tag = make_tag(185);
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame noncarrier_status;
	ResourceXDecodedFrame source_proof;
	ResourceXMasterSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 3;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 1, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.source_node, 2);
	UT_ASSERT_EQ(snapshot.incompatible_holders_bitmap, UINT32_C(0xc));

	source_proof = make_resource_x_remote_blocked_frame(tag, 1, 2);
	source_proof.common.observed_mode = PCM_STATE_S;
	source_proof.body.blocked_to_n.source_fence[28] = PCM_STATE_S;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(
		&source_proof, 2, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(snapshot.blocked_holders_bitmap, UINT32_C(0x4));
	UT_ASSERT_EQ(snapshot.proof_kind, RESOURCE_X_PROOF_REMOTE_CARRIER);
	UT_ASSERT_EQ(snapshot.source_node, 2);

	noncarrier_status = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCKED_TO_N, tag, 1, 3);
	noncarrier_status.common.observed_mode = PCM_STATE_S;
	noncarrier_status.common.target_mode = PCM_STATE_N;
	noncarrier_status.common.outcome = RESOURCE_X_OUTCOME_OK;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(
		&noncarrier_status, 3, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
	UT_ASSERT_EQ(snapshot.blocked_holders_bitmap, UINT32_C(0xc));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_authority_grant_exact(
		&assertion.common.logical_assertion, &grant),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(grant.body.authority_grant.proof_kind,
		RESOURCE_X_PROOF_REMOTE_CARRIER);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(
		&noncarrier_status, 3, &snapshot), RESOURCE_X_APPLY_DUPLICATE);
}

UT_TEST(test_resource_x_selected_s_status_only_is_rejected)
{
	BufferTag tag = make_tag(186);
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame status;
	ResourceXMasterSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 3;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 1, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.source_node, 3);
	status = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCKED_TO_N, tag, 1, 3);
	status.common.observed_mode = PCM_STATE_S;
	status.common.target_mode = PCM_STATE_N;
	status.common.outcome = RESOURCE_X_OUTCOME_OK;

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(
		&status, 3, &snapshot), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(snapshot.blocked_holders_bitmap, UINT32_C(0));
	UT_ASSERT_EQ(snapshot.proof_kind, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_query(tag), PCM_LOCK_MODE_S);
}

UT_TEST(test_resource_x_noncarrier_s_remote_proof_is_rejected)
{
	BufferTag tag = make_tag(187);
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame proof;
	ResourceXMasterSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 3;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 1, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.source_node, 2);
	proof = make_resource_x_remote_blocked_frame(tag, 1, 3);
	proof.common.observed_mode = PCM_STATE_S;
	proof.body.blocked_to_n.source_fence[28] = PCM_STATE_S;

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(
		&proof, 3, &snapshot), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(snapshot.blocked_holders_bitmap, UINT32_C(0));
	UT_ASSERT_EQ(snapshot.proof_kind, 0);
}

UT_TEST(test_resource_x_master_exact_replay_redrives_unanswered_blocker)
{
	BufferTag tag = make_tag(167);
	ResourceXDecodedFrame assertion;
	ResourceXIntentSlot intent;
	ResourceXMasterSnapshot snapshot;
	uint8 payload[RESOURCE_X_PROOF_V1_BYTES];

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 2, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 2, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_intent_snapshot_exact(
		&assertion.common.logical_assertion, 0, &intent, payload,
		sizeof(payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&intent, intent.first_armed_us + 1), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&intent));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_intent_snapshot_exact(
		&assertion.common.logical_assertion, 0, &intent, payload,
		sizeof(payload)), RESOURCE_X_APPLY_NOT_FOUND);

	/* Physical type-17 admission is not a holder acknowledgement.  Until
	 * exact type-18 clears this blocker, replay of the same assertion must
	 * rebuild the same owner-scoped type-17 obligation. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 2, &snapshot), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_intent_snapshot_exact(
		&assertion.common.logical_assertion, 0, &intent, payload,
		sizeof(payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(intent.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(intent.kind, RESOURCE_X_WIRE_BLOCK_TO_N);
	UT_ASSERT_EQ(intent.body.owner_kind,
		RESOURCE_X_INTENT_OWNER_MASTER_BLOCK);
	UT_ASSERT_EQ(intent.body.owner_index, 0);
}

UT_TEST(test_resource_x_holder_retains_status_before_s_to_n)
{
	BufferTag tag = make_tag(155);
	ResourceXDecodedFrame block;
	ResourceXDecodedFrame decoded;
	ResourceXIntentSlot intent;
	ResourceXReconfigBatch batch;
	ResourceXReconfigResult sweep_result;
	ResourceXReconfigToken token;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 payload[RESOURCE_X_PROOF_V1_BYTES];
	uint32 examined = 0;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	block = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCK_TO_N, tag, 2, 0);
	block.common.observed_mode = PCM_STATE_S;
	block.common.target_mode = PCM_STATE_N;
	block.common.source_candidate = 0;
	block.common.retain_pi_if_dirty = 0;

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_exact(&block, 0),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_query(tag), PCM_LOCK_MODE_N);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
		&block.common.logical_assertion, &intent, payload, sizeof(payload)),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(intent.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(intent.logical_generation, 41);
	UT_ASSERT_EQ(intent.authority_generation, 1);
	UT_ASSERT_EQ(intent.destination_node, 0);
	UT_ASSERT_EQ(intent.payload_bytes, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT_EQ(intent.kind, RESOURCE_X_WIRE_BLOCKED_TO_N);
	UT_ASSERT_EQ(intent.body.owner_kind,
		RESOURCE_X_INTENT_OWNER_HOLDER_STATUS);
	UT_ASSERT_EQ(intent.body.owner_node, 0);
	UT_ASSERT(resource_x_assertion_equal(&intent.body.assertion,
		&block.common.logical_assertion));
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_BLOCKED_TO_N, payload,
		RESOURCE_X_CONTROL_V1_BYTES, &decoded, &reject));
	UT_ASSERT_EQ(decoded.kind, RESOURCE_X_WIRE_BLOCKED_TO_N);
	UT_ASSERT(!decoded.blocked_has_remote_proof);
	UT_ASSERT(resource_x_assertion_equal(&decoded.common.logical_assertion,
		&block.common.logical_assertion));
	UT_ASSERT_EQ(decoded.common.action_node, 0);
	UT_ASSERT_EQ(decoded.common.observed_mode, PCM_STATE_S);
	UT_ASSERT_EQ(decoded.common.target_mode, PCM_STATE_N);
	UT_ASSERT_EQ(decoded.common.outcome, RESOURCE_X_OUTCOME_OK);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_probe_exact(
		4, &intent, payload, sizeof(payload), &examined),
		RESOURCE_X_INTENT_PROBE_FOUND);
	UT_ASSERT(examined >= 1 && examined <= 4);
	UT_ASSERT_EQ(intent.body.owner_kind,
		RESOURCE_X_INTENT_OWNER_HOLDER_STATUS);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&intent, 101), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&intent));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
		&block.common.logical_assertion, &intent, payload, sizeof(payload)),
		RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_exact(
		&block.common.logical_assertion, &decoded),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(decoded.kind, RESOURCE_X_WIRE_BLOCKED_TO_N);
	UT_ASSERT_EQ(decoded.common.outcome, RESOURCE_X_OUTCOME_OK);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_exact(&block, 0),
		RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
		&block.common.logical_assertion, &intent, payload, sizeof(payload)),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(intent.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(cluster_pcm_lock_query(tag), PCM_LOCK_MODE_N);
	UT_ASSERT(cluster_resource_x_reconfig_freeze_exact(17, 18, 0, &token));
	do {
		sweep_result = cluster_resource_x_reconfig_sweep(&token, 4, &batch);
	} while (sweep_result == RESOURCE_X_RECONFIG_MORE);
	UT_ASSERT_EQ(sweep_result, RESOURCE_X_RECONFIG_DONE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_exact(
		&block.common.logical_assertion, &decoded),
		RESOURCE_X_APPLY_NOT_FOUND);
}

UT_TEST(test_resource_x_master_remote_proof_is_retained_not_inferred)
{
	BufferTag tag = make_tag(143);
	BufferTag s_tag = make_tag(148);
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame blocked;
	ResourceXDecodedFrame changed;
	ResourceXDecodedFrame grant;
	ResourceXMasterSnapshot snapshot;
	PcmAuthoritySnapshot authority_before;
	PcmAuthoritySnapshot authority_after;
	char *source;
	char *function_start;
	char *post_grant_guard;
	char *holder_transition;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);

	assertion = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X,
										  tag, 2, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&assertion, 2,
														&snapshot),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(snapshot.incompatible_holders_bitmap, UINT32_C(1));
	UT_ASSERT_EQ(snapshot.blocked_holders_bitmap, 0);
	UT_ASSERT_EQ(snapshot.proof_kind, 0);
	UT_ASSERT_EQ(snapshot.final_authority_generation, 0);

	/* A topology-known X holder and a positive status are not a source
	 * proof.  The master must retain X and wait for the typed proof body. */
	blocked = make_resource_x_master_frame(RESOURCE_X_WIRE_BLOCKED_TO_N,
										tag, 2, 0);
	blocked.common.observed_mode = PCM_STATE_X;
	blocked.common.target_mode = PCM_STATE_N;
	blocked.common.outcome = RESOURCE_X_OUTCOME_OK;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(&blocked, 0,
															 &snapshot),
				 RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(snapshot.incompatible_holders_bitmap, UINT32_C(1));
	UT_ASSERT_EQ(snapshot.blocked_holders_bitmap, 0);
	UT_ASSERT_EQ(snapshot.proof_kind, 0);

	blocked.blocked_has_remote_proof = true;
	memset(blocked.body.blocked_to_n.source_fence, 0x5a,
		   sizeof(blocked.body.blocked_to_n.source_fence));
	blocked.body.blocked_to_n.source_carrier_generation = 61;
	blocked.body.blocked_to_n.requester_target_generation = 41;
	blocked.body.blocked_to_n.page_scn_lsn = 63;
	blocked.body.blocked_to_n.dependency_count = 2;
	blocked.body.blocked_to_n.dependencies[0] = 65;
	blocked.body.blocked_to_n.dependencies[1] = 66;
	blocked.body.blocked_to_n.source_proof_crc32c = UINT32_C(0x11223344);
	blocked.body.blocked_to_n.page_checksum = UINT32_C(0x55667788);
	blocked.body.blocked_to_n.source_disposition
		= RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
	blocked.body.blocked_to_n.proof_kind = RESOURCE_X_PROOF_REMOTE_CARRIER;
	blocked.body.blocked_to_n.holder_connection_generation = 64;
	blocked.body.blocked_to_n.acting_formation
		= blocked.common.resource_formation;
	/* A forged predecessor/base cannot consume the blocker or advance GRD.
	 * The exact type-17 ingress below is the sole X->N + grant edge. */
	changed = blocked;
	changed.common.base_authority_generation++;
	changed.common.authority_generation++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(&changed, 0,
												 &snapshot),
				 RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(snapshot.blocked_holders_bitmap, 0);
	UT_ASSERT_EQ(snapshot.final_authority_generation, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_query(tag), PCM_LOCK_MODE_X);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(&blocked, 0,
												 &snapshot),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
	UT_ASSERT_EQ(snapshot.incompatible_holders_bitmap, UINT32_C(1));
	UT_ASSERT_EQ(snapshot.blocked_holders_bitmap, UINT32_C(1));
	UT_ASSERT_EQ(snapshot.proof_kind, RESOURCE_X_PROOF_REMOTE_CARRIER);
	UT_ASSERT_EQ(snapshot.source_node, 0);
	UT_ASSERT_EQ(snapshot.source_carrier_generation, 61);
	UT_ASSERT_EQ(snapshot.requester_target_generation, 41);
	/* Exact X->N+PI followed by N->X advances the bridged authority twice. */
	UT_ASSERT_EQ(snapshot.final_authority_generation, 3);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_authority_grant_exact(
		&assertion.common.logical_assertion, &grant), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority_before));
	UT_ASSERT_EQ(grant.kind, RESOURCE_X_WIRE_AUTHORITY_GRANT);
	UT_ASSERT_EQ(grant.payload_bytes, RESOURCE_X_PROOF_V1_BYTES);
	UT_ASSERT(resource_x_assertion_equal(&grant.common.logical_assertion,
		&assertion.common.logical_assertion));
	UT_ASSERT_EQ(grant.common.base_authority_generation, 1);
	UT_ASSERT_EQ(grant.common.resource_formation, 17);
	UT_ASSERT_EQ(grant.common.master_session_incarnation, 31);
	UT_ASSERT_EQ(grant.common.assertion_sequence, 41);
	UT_ASSERT_EQ(grant.common.ordered_lane, 7);
	UT_ASSERT_EQ(grant.common.action_node, 2);
	UT_ASSERT_EQ(grant.common.target_mode, PCM_STATE_X);
	UT_ASSERT_EQ(grant.common.outcome, RESOURCE_X_OUTCOME_OK);
	UT_ASSERT_EQ(grant.common.flags,
		RESOURCE_X_COMMON_FLAG_REMOTE_IMAGE_REQUIRED);
	UT_ASSERT_EQ(grant.common.authority_generation, 3);
	UT_ASSERT_EQ(memcmp(grant.body.authority_grant.source_fence,
		blocked.body.blocked_to_n.source_fence,
		sizeof(grant.body.authority_grant.source_fence)), 0);
	UT_ASSERT_EQ(grant.body.authority_grant.final_authority_generation, 3);
	UT_ASSERT_EQ(grant.body.authority_grant.source_carrier_generation, 61);
	UT_ASSERT_EQ(grant.body.authority_grant.requester_target_generation, 41);
	UT_ASSERT_EQ(grant.body.authority_grant.page_scn_lsn, 63);
	UT_ASSERT_EQ(grant.body.authority_grant.dependency_count, 2);
	UT_ASSERT_EQ(grant.body.authority_grant.dependencies[0], 65);
	UT_ASSERT_EQ(grant.body.authority_grant.dependencies[1], 66);
	UT_ASSERT_EQ(grant.body.authority_grant.source_proof_crc32c,
		UINT32_C(0x11223344));
	UT_ASSERT_EQ(grant.body.authority_grant.page_checksum,
		UINT32_C(0x55667788));
	UT_ASSERT_EQ(grant.body.authority_grant.proof_kind,
		RESOURCE_X_PROOF_REMOTE_CARRIER);
	UT_ASSERT_EQ(grant.body.authority_grant.source_disposition,
		RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE);
	UT_ASSERT_EQ(grant.body.authority_grant.requester_connection_generation,
		51);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(&blocked, 0,
														 &snapshot),
				 RESOURCE_X_APPLY_DUPLICATE);
	changed = blocked;
	changed.body.blocked_to_n.source_fence[0] ^= UINT8_C(1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(&changed, 0,
														 &snapshot),
				 RESOURCE_X_APPLY_STALE);
	changed = blocked;
	changed.body.blocked_to_n.page_scn_lsn++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(&changed, 0,
														 &snapshot),
				 RESOURCE_X_APPLY_STALE);
	changed = blocked;
	changed.body.blocked_to_n.source_proof_crc32c++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(&changed, 0,
														 &snapshot),
				 RESOURCE_X_APPLY_STALE);
	changed = blocked;
	changed.body.blocked_to_n.page_checksum++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(&changed, 0,
														 &snapshot),
				 RESOURCE_X_APPLY_STALE);
	changed = blocked;
	changed.body.blocked_to_n.holder_connection_generation++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(&changed, 0,
														 &snapshot),
				 RESOURCE_X_APPLY_STALE);
	changed = blocked;
	changed.body.blocked_to_n.acting_formation++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(&changed, 0,
														 &snapshot),
				 RESOURCE_X_APPLY_STALE);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority_after));
	UT_ASSERT_EQ(memcmp(&authority_before, &authority_after,
						 sizeof(authority_before)), 0);

	/* Grant commit is an immutable authority boundary.  The production
	 * function must validate the complete retained blocker set before it can
	 * reach the holder-transition path. */
	source = read_text_file(PCM_LOCK_SOURCE_PATH);
	UT_ASSERT_NOT_NULL(source);
	function_start = strstr(source,
		"cluster_pcm_lock_resource_x_blocked_to_n_exact(");
	UT_ASSERT_NOT_NULL(function_start);
	post_grant_guard = strstr(function_start,
		"pcm_resource_x_post_grant_blocker_state_exact");
	UT_ASSERT_NOT_NULL(post_grant_guard);
	holder_transition = strstr(function_start, "direct_x_source\n\t\t=");
	UT_ASSERT_NOT_NULL(holder_transition);
	UT_ASSERT(post_grant_guard < holder_transition);
	free(source);

	/* A remote carrier is valid only from the exact frozen source.  In an S
	 * set, a non-carrier cannot manufacture the selected holder's proof. */
	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(s_tag, PCM_LOCK_MODE_S);
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(s_tag, PCM_LOCK_MODE_S);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	assertion = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X,
										  s_tag, 2, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&assertion, 2,
		&snapshot), RESOURCE_X_APPLY_APPLIED);
	blocked.common.logical_assertion = assertion.common.logical_assertion;
	blocked.common.action_node = 1;
	blocked.common.observed_mode = PCM_STATE_S;
	blocked.body.blocked_to_n.source_fence[28] = PCM_STATE_S;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(&blocked, 1,
		&snapshot), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_lock_query(s_tag), PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(snapshot.proof_kind, 0);
}

UT_TEST(test_resource_x_blocked_to_n_exact_clear_rejects_generation_drift)
{
	BufferTag tag = make_tag(159);
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame blocked;
	ResourceXDecodedFrame changed;
	ResourceXMasterSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 2, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 2, &snapshot), RESOURCE_X_APPLY_APPLIED);
	blocked = make_resource_x_remote_blocked_frame(tag, 2, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(
		&blocked, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
	UT_ASSERT_EQ(snapshot.blocked_holders_bitmap, UINT32_C(1));

	changed = blocked;
	changed.body.blocked_to_n.acting_formation++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(
		&changed, 0, &snapshot), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&assertion.common.logical_assertion, &snapshot),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
	UT_ASSERT_EQ(snapshot.blocked_holders_bitmap, UINT32_C(1));
}

UT_TEST(test_resource_x_type18_wire_decode_drives_master_exact_apply)
{
	BufferTag tag = make_tag(160);
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame blocked;
	ResourceXDecodedFrame decoded;
	ResourceXDecodedFrame grant;
	ResourceXMasterSnapshot snapshot;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 payload[RESOURCE_X_PROOF_V1_BYTES];
	uint16 payload_bytes = 0;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 2, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 2, &snapshot), RESOURCE_X_APPLY_APPLIED);
	blocked = make_resource_x_remote_blocked_frame(tag, 2, 0);
	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_BLOCKED_TO_N, &blocked, payload, sizeof(payload),
		&payload_bytes, &reject));
	UT_ASSERT_EQ(payload_bytes, RESOURCE_X_PROOF_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_BLOCKED_TO_N, payload, payload_bytes, &decoded,
		&reject));
	UT_ASSERT_EQ(decoded.kind, RESOURCE_X_WIRE_BLOCKED_TO_N);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(
		&decoded, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
	UT_ASSERT_EQ(snapshot.blocked_holders_bitmap, UINT32_C(1));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_authority_grant_exact(
		&assertion.common.logical_assertion, &grant),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(grant.kind, RESOURCE_X_WIRE_AUTHORITY_GRANT);
}

UT_TEST(test_resource_x_type18_releases_direct_old_x_request_and_tracks_exact_generation)
{
	BufferTag tag = make_tag(161);
	PcmAuthoritySnapshot authority;
	ResourceXDecodedFrame blocked;
	ResourceXDecodedFrame first_assert;
	ResourceXDecodedFrame local_proof;
	ResourceXDecodedFrame second_assert;
	ResourceXDecodedFrame settlement;
	ResourceXIntentSlot grant_intent;
	ResourceXMasterSnapshot old_snapshot;
	ResourceXMasterSnapshot snapshot;
	uint8 grant_bytes[RESOURCE_X_PROOF_V1_BYTES];

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	first_assert = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 0, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&first_assert, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_PROOF);

	local_proof = make_resource_x_master_frame(
		RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION, tag, 0, 0);
	local_proof.common.observed_mode = PCM_STATE_S;
	local_proof.common.outcome = RESOURCE_X_OUTCOME_OK;
	local_proof.body.local_proof.local_holder_authority_generation = 71;
	local_proof.body.local_proof.requester_target_generation = 41;
	local_proof.body.local_proof.page_scn_lsn = 82;
	local_proof.body.local_proof.page_checksum = UINT32_C(0x55667788);
	local_proof.body.local_proof.local_image_proof_crc32c
		= UINT32_C(0x99aabbcc);
	local_proof.body.local_proof.requester_connection_generation = 73;
	local_proof.body.local_proof.local_proof_generation = 74;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_local_proof_exact(
		&local_proof, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
	UT_ASSERT_EQ(snapshot.final_authority_generation, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
		&first_assert.common.logical_assertion, &grant_intent, grant_bytes,
		sizeof(grant_bytes)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_stage_exact(
		&grant_intent, 101), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_grant_intent_complete_exact(
		&grant_intent));

	settlement = make_resource_x_master_frame(
		RESOURCE_X_WIRE_INSTALL_SETTLEMENT, tag, 0, 0);
	settlement.common.outcome = RESOURCE_X_OUTCOME_OK;
	settlement.common.authority_generation = 2;
	settlement.body.install_settlement.conversion_base_generation = 1;
	settlement.body.install_settlement.final_authority_generation = 2;
	settlement.body.install_settlement.requester_connection_generation = 73;
	settlement.body.install_settlement.requester_target_generation = 41;
	settlement.body.install_settlement.page_scn_lsn = 82;
	settlement.body.install_settlement.page_checksum = UINT32_C(0x55667788);
	settlement.body.install_settlement.source_proof_crc32c
		= UINT32_C(0x99aabbcc);
	settlement.body.install_settlement.installed_mode = PCM_STATE_X;
	settlement.body.install_settlement.requester_role
		= RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
	settlement.body.install_settlement.terminal_outcome
		= RESOURCE_X_OUTCOME_OK;
	settlement.body.install_settlement.terminal_state
		= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
		&settlement, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_SETTLED);

	second_assert = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 2, 2);
	second_assert.common.base_authority_generation = 2;
	second_assert.common.authority_generation = 2;
	second_assert.common.assertion_sequence = 42;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&second_assert, 2, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(snapshot.incompatible_holders_bitmap, UINT32_C(1));

	blocked = make_resource_x_remote_blocked_frame(tag, 2, 0);
	blocked.common.base_authority_generation = 2;
	blocked.common.authority_generation = 2;
	blocked.common.assertion_sequence = 42;
	blocked.body.blocked_to_n.requester_target_generation = 42;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(
		&blocked, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(authority.state, PCM_STATE_X);
	UT_ASSERT_EQ(authority.x_holder_node, 2);
	UT_ASSERT_EQ(authority.transition_count, 4);
	UT_ASSERT_EQ(snapshot.final_authority_generation,
		authority.transition_count);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&first_assert.common.logical_assertion, &old_snapshot),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(old_snapshot.phase, RESOURCE_X_MASTER_RELEASED);
}

static void
make_resource_x_remote_join_pair(BufferTag tag, int32 requester_node,
								ResourceXDecodedFrame *grant,
								ResourceXDecodedFrame *image)
{
	ResourceXDecodedFrame decoded_image;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 image_payload[RESOURCE_X_IMAGE_V1_BYTES];
	uint16 image_payload_bytes = 0;
	int i;

	*grant = make_resource_x_master_frame(
		RESOURCE_X_WIRE_AUTHORITY_GRANT, tag, requester_node,
		requester_node);
	grant->common.outcome = RESOURCE_X_OUTCOME_OK;
	grant->common.flags = RESOURCE_X_COMMON_FLAG_REMOTE_IMAGE_REQUIRED;
	grant->common.authority_generation = 3;
	memset(grant->body.authority_grant.source_fence, 0x5a,
		   sizeof(grant->body.authority_grant.source_fence));
	set_resource_x_test_source_fence(
		grant->body.authority_grant.source_fence, UINT64_C(60), PCM_STATE_X);
	grant->body.authority_grant.final_authority_generation = 3;
	grant->body.authority_grant.source_carrier_generation = 61;
	grant->body.authority_grant.requester_target_generation = 41;
	grant->body.authority_grant.page_scn_lsn = 63;
	grant->body.authority_grant.dependency_count = 2;
	grant->body.authority_grant.dependencies[0] = 65;
	grant->body.authority_grant.dependencies[1] = 66;
	grant->body.authority_grant.source_proof_crc32c
		= UINT32_C(0x11223344);
	grant->body.authority_grant.page_checksum = UINT32_C(0x55667788);
	grant->body.authority_grant.proof_kind
		= RESOURCE_X_PROOF_REMOTE_CARRIER;
	grant->body.authority_grant.source_disposition
		= RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
	grant->body.authority_grant.requester_connection_generation = 71;

	*image = make_resource_x_master_frame(
		RESOURCE_X_WIRE_IMAGE_ENVELOPE, tag, requester_node,
		requester_node);
	image->common.observed_mode = PCM_STATE_X;
	image->common.outcome = RESOURCE_X_OUTCOME_OK;
	image->common.sender_connection_generation = UINT32_C(1);
	image->common.authority_generation = 2;
	for (i = 0; i < RESOURCE_X_REQUEST_TAIL_BYTES; i++)
		image->body.image_envelope.request_tail[i] = (uint8)(0x20 + i);
	image->body.image_envelope.conversion_base_generation = 1;
	memcpy(image->body.image_envelope.source_fence,
		grant->body.authority_grant.source_fence,
		sizeof(image->body.image_envelope.source_fence));
	image->body.image_envelope.source_carrier_generation = 61;
	image->body.image_envelope.requester_target_generation = 41;
	image->body.image_envelope.page_scn_lsn = 63;
	image->body.image_envelope.dependency_count = 2;
	image->body.image_envelope.dependencies[0] = 65;
	image->body.image_envelope.dependencies[1] = 66;
	image->body.image_envelope.dependency_vector_crc32c
		= UINT32_C(0x99aabbcc);
	image->body.image_envelope.page_checksum = UINT32_C(0x55667788);
	image->body.image_envelope.image_length = RESOURCE_X_PAGE_BYTES;
	image->body.image_envelope.source_disposition
		= RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
	image->body.image_envelope.proof_kind
		= RESOURCE_X_PROOF_REMOTE_CARRIER;
	for (i = 0; i < RESOURCE_X_PAGE_BYTES; i++)
		image->body.image_envelope.page_bytes[i] = (uint8)(i * 37);
	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, image, image_payload,
		sizeof(image_payload), &image_payload_bytes, &reject));
	UT_ASSERT_EQ(image_payload_bytes, RESOURCE_X_IMAGE_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, image_payload, image_payload_bytes,
		&decoded_image, &reject));
	*image = decoded_image;
	grant->body.authority_grant.source_proof_crc32c
		= image->common.semantic_crc32c;
}

static void
make_resource_x_s_remote_join_pair(BufferTag tag, int32 requester_node,
								   ResourceXDecodedFrame *grant,
								   ResourceXDecodedFrame *image)
{
	ResourceXDecodedFrame canonical_image;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 image_payload[RESOURCE_X_IMAGE_V1_BYTES];
	uint16 image_payload_bytes = 0;

	make_resource_x_remote_join_pair(tag, requester_node, grant, image);
	grant->body.authority_grant.source_fence[28] = PCM_STATE_S;
	image->common.observed_mode = PCM_STATE_S;
	image->body.image_envelope.source_fence[28] = PCM_STATE_S;
	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, image, image_payload,
		sizeof(image_payload), &image_payload_bytes, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, image_payload, image_payload_bytes,
		&canonical_image, &reject));
	*image = canonical_image;
	grant->body.authority_grant.source_proof_crc32c
		= image->common.semantic_crc32c;
}

static void
retarget_resource_x_remote_join_pair(
	ResourceXDecodedFrame *grant, ResourceXDecodedFrame *image,
	uint64 final_authority_generation, uint64 requester_target_generation)
{
	ResourceXDecodedFrame decoded_image;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 image_payload[RESOURCE_X_IMAGE_V1_BYTES];
	uint16 image_payload_bytes = 0;

	UT_ASSERT(final_authority_generation >= 3);
	grant->common.base_authority_generation = final_authority_generation - 2;
	grant->common.authority_generation = final_authority_generation;
	grant->common.assertion_sequence = requester_target_generation;
	grant->body.authority_grant.final_authority_generation
		= final_authority_generation;
	grant->body.authority_grant.requester_target_generation
		= requester_target_generation;
	image->common.base_authority_generation = final_authority_generation - 2;
	image->common.authority_generation = final_authority_generation - 1;
	image->common.assertion_sequence = requester_target_generation;
	image->body.image_envelope.conversion_base_generation
		= final_authority_generation - 2;
	image->body.image_envelope.requester_target_generation
		= requester_target_generation;
	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, image, image_payload,
		sizeof(image_payload), &image_payload_bytes, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, image_payload, image_payload_bytes,
		&decoded_image, &reject));
	*image = decoded_image;
	grant->body.authority_grant.source_proof_crc32c
		= image->common.semantic_crc32c;
}

static void
canonicalize_resource_x_test_image(ResourceXDecodedFrame *image)
{
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 payload[RESOURCE_X_IMAGE_V1_BYTES];
	uint16 payload_bytes = 0;

	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, image, payload, sizeof(payload),
		&payload_bytes, &reject));
	UT_ASSERT_EQ(payload_bytes, RESOURCE_X_IMAGE_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, payload, payload_bytes,
		&decoded, &reject));
	*image = decoded;
}

static ResourceXAcquisitionRef
setup_resource_x_test_terminal_cover(BufferTag tag,
									  uint64 cached_generation)
{
	ResourceXAssertion assertion;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXDecodedFrame bootstrap;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXAcquisitionRef ref;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBootstrapRoundAction action;
	ResourceXBufferActivationProof activation;
	ResourceXBufferInstallProof install;
	ResourceXRequesterJoinSnapshot join;

	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), false, 0, &bootstrap, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(
		&bootstrap, UINT64_C(1), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, UINT64_C(110), &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	make_resource_x_remote_join_pair(tag, 1, &grant, &image);
	retarget_resource_x_remote_join_pair(&grant, &image, UINT64_C(3),
		UINT64_C(1));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 0, &join), RESOURCE_X_APPLY_APPLIED);
	ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref),
		RESOURCE_X_APPLY_APPLIED);
	memset(&install, 0, sizeof(install));
	install.ownership_generation = cached_generation;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(
		&ref, &install), RESOURCE_X_APPLY_APPLIED);
	memset(&activation, 0, sizeof(activation));
	activation.ownership_generation = cached_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(
		&ref, &activation), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&ref, 31, 77, cached_generation));
	return ref;
}

static void
retain_resource_x_test_settlement_pair(
	BufferTag tag, uint8 source_mode, uint64 source_generation,
	uint64 carrier_generation, uint64 final_authority_generation,
	ResourceXDecodedFrame *settlement_out)
{
	ClusterPcmOwnSnapshot prepared;
	ResourceXDecodedFrame block;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame status;
	ResourceXIntentSlot image_intent;
	ResourceXIntentSlot status_intent;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 image_payload[RESOURCE_X_IMAGE_V1_BYTES];
	uint8 status_payload[RESOURCE_X_PROOF_V1_BYTES];

	block = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCK_TO_N, tag, 2, 1);
	block.common.base_authority_generation = final_authority_generation - 2;
	block.common.authority_generation = final_authority_generation - 2;
	block.common.ordered_lane = 0;
	block.common.observed_mode = source_mode;
	block.common.target_mode = PCM_STATE_N;
	block.common.source_candidate = 1;
	block.common.retain_pi_if_dirty = 1;
	make_resource_x_remote_join_pair(tag, 2, &grant, &image);
	retarget_resource_x_remote_join_pair(
		&grant, &image, final_authority_generation, 41);
	image.common.ordered_lane = 0;
	image.common.observed_mode = source_mode;
	set_resource_x_test_source_fence(
		image.body.image_envelope.source_fence,
		source_generation, source_mode);
	image.body.image_envelope.source_carrier_generation
		= carrier_generation;
	if (source_mode == (uint8)PCM_STATE_S)
		((PageHeader)image.body.image_envelope.page_bytes)->pd_block_scn
			= image.body.image_envelope.page_scn_lsn;
	canonicalize_resource_x_test_image(&image);
	status = make_resource_x_remote_blocked_frame(tag, 2, 1);
	status.common.base_authority_generation = final_authority_generation - 2;
	status.common.authority_generation = final_authority_generation - 2;
	status.common.ordered_lane = 0;
	status.common.observed_mode = source_mode;
	memcpy(status.body.blocked_to_n.source_fence,
		image.body.image_envelope.source_fence,
		sizeof(status.body.blocked_to_n.source_fence));
	status.body.blocked_to_n.source_carrier_generation
		= carrier_generation;
	status.body.blocked_to_n.source_proof_crc32c
		= image.common.semantic_crc32c;
	if (source_mode == (uint8)PCM_STATE_X) {
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_source_exact(
			&block, 0, &status, &image), RESOURCE_X_APPLY_APPLIED);
	} else {
		memset(&prepared, 0, sizeof(prepared));
		prepared.tag = tag;
		prepared.generation = source_generation;
		prepared.reservation_token = 77;
		prepared.flags = PCM_OWN_FLAG_REVOKING;
		prepared.pcm_state = PCM_STATE_S;
		UT_ASSERT_EQ(
			cluster_pcm_lock_resource_x_block_to_n_prepared_s_source_exact(
				&block, 0, &status, &image, &prepared,
				PageGetLSN((Page)image.body.image_envelope.page_bytes),
				image.body.image_envelope.page_scn_lsn,
				image.body.image_envelope.page_checksum),
			RESOURCE_X_APPLY_APPLIED);
	}
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_publish_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		0, block.common.master_session_incarnation),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
		&block.common.logical_assertion, &status_intent, status_payload,
		sizeof(status_payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_intent_snapshot_exact(
		&block.common.logical_assertion, &image_intent, image_payload,
		sizeof(image_payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&status_intent, 105), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&status_intent));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&image_intent, 106), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&image_intent));
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_BLOCKED_TO_N, status_payload,
		status_intent.payload_bytes, settlement_out, &reject));
	settlement_out->kind = RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2;
	settlement_out->common.observed_mode = PCM_STATE_N;
	settlement_out->common.target_mode = PCM_STATE_N;
	settlement_out->common.source_candidate = 1;
	settlement_out->common.retain_pi_if_dirty = 1;
	settlement_out->common.authority_generation = final_authority_generation;
	settlement_out->common.sender_connection_generation = 71;
	settlement_out->common.outcome = RESOURCE_X_OUTCOME_NONE;
	settlement_out->common.flags = 0;
}

UT_TEST(test_resource_x_source_settlement_prepare_closes_total_cover_table)
{
	BufferTag mismatch_x_tag = make_tag(215);
	BufferTag s_cover_tag = make_tag(216);
	BufferTag empty_x_tag = make_tag(217);
	BufferTag empty_s_tag = make_tag(218);
	BufferTag generation_tag = make_tag(219);
	BufferTag new_cover_tag = make_tag(220);
	BufferTag zero_generation_tag = make_tag(221);
	BufferTag max_generation_tag = make_tag(222);
	BufferTag overflow_generation_tag = make_tag(223);
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef entry_ref;
	ResourceXAcquisitionRef cover_ref;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXAssertion successor_assertion;
	ResourceXBootstrapRoundAction action;
	ResourceXDecodedFrame bootstrap;
	ResourceXDecodedFrame settlement;
	ResourceXSourceSettlementPlan plan;
	ResourceXSourceSettlementCommitObservation observation;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	cover_ref = setup_resource_x_test_terminal_cover(
		mismatch_x_tag, UINT64_C(60));
	retain_resource_x_test_settlement_pair(
		mismatch_x_tag, PCM_STATE_X, UINT64_C(61), UINT64_C(62), UINT64_C(5),
		&settlement);
	memset(&plan, 0x5a, sizeof(plan));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
		&settlement, 0, &plan), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(!plan.valid);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&cover_ref, 31, 77, 60));

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	cover_ref = setup_resource_x_test_terminal_cover(
		s_cover_tag, UINT64_C(60));
	retain_resource_x_test_settlement_pair(
		s_cover_tag, PCM_STATE_S, UINT64_C(60), UINT64_C(61), UINT64_C(5),
		&settlement);
	memset(&plan, 0x5a, sizeof(plan));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
		&settlement, 0, &plan), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT(!plan.valid);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&cover_ref, 31, 77, 60));

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	retain_resource_x_test_settlement_pair(
		empty_x_tag, PCM_STATE_X, UINT64_C(60), UINT64_C(61), UINT64_C(3),
		&settlement);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
		&settlement, 0, &plan), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(plan.valid);
	UT_ASSERT_EQ(plan.source_mode, (uint8)PCM_STATE_X);
	UT_ASSERT_EQ(plan.cover_action,
		(uint8)RESOURCE_X_SETTLEMENT_COVER_NO_COVER);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_commit_exact(
		&settlement, 0, &plan, &observation), RESOURCE_X_APPLY_APPLIED);
	memset(&plan, 0x5a, sizeof(plan));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
		&settlement, 0, &plan), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(!plan.valid);

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	retain_resource_x_test_settlement_pair(
		empty_s_tag, PCM_STATE_S, UINT64_C(60), UINT64_C(61), UINT64_C(5),
		&settlement);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
		&settlement, 0, &plan), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(plan.valid);
	UT_ASSERT_EQ(plan.source_mode, (uint8)PCM_STATE_S);
	UT_ASSERT_EQ(plan.cover_action,
		(uint8)RESOURCE_X_SETTLEMENT_COVER_NO_COVER);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_commit_exact(
		&settlement, 0, &plan, &observation), RESOURCE_X_APPLY_APPLIED);

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	retain_resource_x_test_settlement_pair(
		new_cover_tag, PCM_STATE_X, UINT64_C(60), UINT64_C(61), UINT64_C(3),
		&settlement);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
		&settlement, 0, &plan), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(plan.valid);
	UT_ASSERT_EQ(plan.cover_action,
		(uint8)RESOURCE_X_SETTLEMENT_COVER_NO_COVER);
	UT_ASSERT(resource_x_assertion_init(
		&new_cover_tag, 1, &successor_assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&successor_assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000),
		(UINT64_C(1000)) - (UINT64_C(100)), UINT64_C(100), UINT64_C(50), false, 0, &bootstrap,
		&terminal_ref);
	UT_ASSERT_EQ(action,
		RESOURCE_X_BOOTSTRAP_ROUND_PREDECESSOR_WAIT);
	UT_ASSERT_EQ(bootstrap.kind, UINT8_C(0));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_commit_exact(
		&settlement, 0, &plan, &observation), RESOURCE_X_APPLY_APPLIED);

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	retain_resource_x_test_settlement_pair(
		generation_tag, PCM_STATE_X, UINT64_C(59), UINT64_C(61), UINT64_C(3),
		&settlement);
	memset(&plan, 0x5a, sizeof(plan));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
		&settlement, 0, &plan), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT(!plan.valid);
	UT_ASSERT(pcm_entry_ref_acquire(
		&generation_tag, false, &entry_ref, &acquire_result));
	pcm_entry_ref_release(&entry_ref);

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	retain_resource_x_test_settlement_pair(
		zero_generation_tag, PCM_STATE_X, UINT64_C(0), UINT64_C(1),
		UINT64_C(3), &settlement);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
		&settlement, 0, &plan), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT(!plan.valid);

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	retain_resource_x_test_settlement_pair(
		max_generation_tag, PCM_STATE_X, UINT64_MAX, UINT64_C(1),
		UINT64_C(3), &settlement);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
		&settlement, 0, &plan), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT(!plan.valid);

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	retain_resource_x_test_settlement_pair(
		overflow_generation_tag, PCM_STATE_X, UINT64_MAX - 1, UINT64_C(1),
		UINT64_C(3), &settlement);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
		&settlement, 0, &plan), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT(!plan.valid);
}

static ResourceXDecodedFrame predecessor_wait_settlement;

static void
settle_predecessor_during_wait_registration(void)
{
	ResourceXSourceSettlementPlan plan;
	ResourceXSourceSettlementCommitObservation observation;

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
					 &predecessor_wait_settlement, 0, &plan),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_commit_exact(
					 &predecessor_wait_settlement, 0, &plan, &observation),
				 RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_resource_x_predecessor_cv_observes_settlement_without_a_head)
{
	BufferTag tag = make_tag(484);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame bootstrap;
	ResourceXAcquisitionRef terminal;
	ResourceXBootstrapRoundFailureSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	fake_pcm_clock_us = 100000;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	retain_resource_x_test_settlement_pair(tag, PCM_STATE_S, 60, 61, 5,
										   &predecessor_wait_settlement);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_predecessor_wait_exact(&tag, 0, 31, 17, 0, 102500, 10000),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(fake_cv_sleep_count, 1);
	UT_ASSERT_EQ(fake_cv_timed_sleep_timeout, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 50, &snapshot),
				 RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_predecessor_wait_exact(&tag, 0, 31, 17, 60, 102500, 10000),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_predecessor_wait_exact(&tag, 0, 31, 17, 61, 100000, 10000),
		RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(fake_cv_sleep_count, 1);
	fake_cv_prepare_hook = settle_predecessor_during_wait_registration;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_predecessor_wait_exact(&tag, 0, 31, 17, 0, 102500, 10000),
		RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(fake_cv_sleep_count, 1);
	UT_ASSERT_EQ(fake_cv_prepare_count, fake_cv_cancel_count);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 105000, 5000, 100000, 1000, false, 0,
					 &bootstrap, &terminal),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(bootstrap.common.assertion_sequence, UINT64_C(1));
}

UT_TEST(test_resource_x_s_predecessor_retained_pair_blocks_local_bootstrap)
{
	BufferTag tag = make_tag(225);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame bootstrap;
	ResourceXDecodedFrame settlement;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBootstrapRoundAction action;
	ResourceXSourceSettlementCommitObservation observation;
	ResourceXSourceSettlementPlan plan;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	retain_resource_x_test_settlement_pair(
		tag, PCM_STATE_S, UINT64_C(60), UINT64_C(61), UINT64_C(5),
		&settlement);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));

	/* The earlier holder settlement owns the ordering edge.  A local
	 * successor must not allocate or dispatch kind-9 while that exact pair is
	 * still undrained, otherwise SourceSettlement would encounter a non-EMPTY
	 * round and correctly refuse the physical release. */
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), false, 0, &bootstrap, &terminal_ref);
	UT_ASSERT_EQ(action,
		RESOURCE_X_BOOTSTRAP_ROUND_PREDECESSOR_WAIT);
	UT_ASSERT_EQ(bootstrap.kind, UINT8_C(0));

	memset(&plan, 0, sizeof(plan));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
		&settlement, 0, &plan), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(plan.valid);
	UT_ASSERT_EQ(plan.source_mode, (uint8)PCM_STATE_S);
	UT_ASSERT_EQ(plan.cover_action,
		(uint8)RESOURCE_X_SETTLEMENT_COVER_NO_COVER);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_commit_exact(
		&settlement, 0, &plan, &observation), RESOURCE_X_APPLY_APPLIED);

	/* Draining the predecessor exposes the ordinary local bootstrap path.  Its
	 * first attempt proves the wait did not create hidden requester state. */
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(150)),
		UINT64_C(150), UINT64_C(50), false, 0, &bootstrap, &terminal_ref);
	UT_ASSERT_EQ(action,
		RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(bootstrap.common.assertion_sequence, UINT64_C(1));
}

UT_TEST(test_resource_x_s_predecessor_cancels_only_unbound_preassert_round)
{
	BufferTag tag = make_tag(226);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame bootstrap;
	ResourceXDecodedFrame settlement;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBootstrapRoundAction action;
	ResourceXApplyResult prepare_result;
	ResourceXSourceSettlementCommitObservation observation;
	ResourceXSourceSettlementPlan plan;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));

	/* The successor sampled the resource before the predecessor pair became
	 * visible, but it has dispatched only a non-authoritative kind-9 request:
	 * no ACK/base or ASSERT exists yet.  Installing the exact older S pair must
	 * atomically return that local round to EMPTY so SourceSettlement retains
	 * its frozen EMPTY-only physical-release contract. */
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), false, 0, &bootstrap, &terminal_ref);
	UT_ASSERT_EQ(action,
		RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(bootstrap.common.assertion_sequence, UINT64_C(1));

	retain_resource_x_test_settlement_pair(
		tag, PCM_STATE_S, UINT64_C(60), UINT64_C(61), UINT64_C(5),
		&settlement);
	memset(&plan, 0, sizeof(plan));
	prepare_result
		= cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
			&settlement, 0, &plan);
	UT_ASSERT_EQ(prepare_result, RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(plan.valid);
	if (prepare_result != RESOURCE_X_APPLY_APPLIED || !plan.valid)
		return;
	UT_ASSERT_EQ(plan.source_mode, (uint8)PCM_STATE_S);
	UT_ASSERT_EQ(plan.cover_action,
		(uint8)RESOURCE_X_SETTLEMENT_COVER_NO_COVER);

	/* Before drain, the same foreground invocation keeps its original absolute
	 * deadline but owns no replacement round. */
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(125)),
		UINT64_C(125), UINT64_C(50), false, 0, &bootstrap, &terminal_ref);
	UT_ASSERT_EQ(action,
		RESOURCE_X_BOOTSTRAP_ROUND_PREDECESSOR_WAIT);
	UT_ASSERT_EQ(bootstrap.kind, UINT8_C(0));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_commit_exact(
		&settlement, 0, &plan, &observation), RESOURCE_X_APPLY_APPLIED);

	/* Cancellation preserves the attempt floor.  The next legal round uses a
	 * strictly higher attempt without refreshing the caller's deadline. */
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(150)),
		UINT64_C(150), UINT64_C(50), false, 0, &bootstrap, &terminal_ref);
	UT_ASSERT_EQ(action,
		RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(bootstrap.common.assertion_sequence, UINT64_C(2));
}

UT_TEST(test_resource_x_s_predecessor_cancellation_supersedes_old_wait)
{
	BufferTag tag = make_tag(227);
	ResourceXAssertion assertion;
	ResourceXDecodedFrame bootstrap;
	ResourceXDecodedFrame settlement;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBootstrapRoundAction action;
	ResourceXSourceSettlementCommitObservation observation;
	ResourceXSourceSettlementPlan plan;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));

	/* This caller has already received WAIT for attempt 1 when the exact older
	 * S predecessor becomes retained.  Retention legally cancels only that
	 * unbound request.  The subsequent wait recheck must report that its old
	 * predicate was superseded, rather than terminating the acquisition as a
	 * generic stale failure. */
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(100)),
		UINT64_C(100), UINT64_C(50), false, 0, &bootstrap, &terminal_ref);
	UT_ASSERT_EQ(action,
		RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(bootstrap.common.assertion_sequence, UINT64_C(1));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(125)),
		UINT64_C(125), UINT64_C(50), false, 0, &bootstrap, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_WAIT);

	retain_resource_x_test_settlement_pair(
		tag, PCM_STATE_S, UINT64_C(60), UINT64_C(61), UINT64_C(5),
		&settlement);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_wait_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50), UINT64_MAX - 1, 1),
				 RESOURCE_X_APPLY_DUPLICATE);

	/* The old wait neither creates a replacement round nor refreshes the
	 * caller's absolute deadline.  Until the exact pair drains, the next step
	 * observes the predecessor; after drain it allocates attempt 2. */
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(150)),
		UINT64_C(150), UINT64_C(50), false, 0, &bootstrap, &terminal_ref);
	UT_ASSERT_EQ(action,
		RESOURCE_X_BOOTSTRAP_ROUND_PREDECESSOR_WAIT);
	memset(&plan, 0, sizeof(plan));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
		&settlement, 0, &plan), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(plan.valid);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_commit_exact(
		&settlement, 0, &plan, &observation), RESOURCE_X_APPLY_APPLIED);
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000), (UINT64_C(1000)) - (UINT64_C(175)),
		UINT64_C(175), UINT64_C(50), false, 0, &bootstrap, &terminal_ref);
	UT_ASSERT_EQ(action,
		RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(bootstrap.common.assertion_sequence, UINT64_C(2));
}

UT_TEST(test_resource_x_source_settlement_accepts_multi_blocker_authority_span)
{
	BufferTag tag = make_tag(224);
	ResourceXSourceSettlementCommitObservation observation;
	ResourceXDecodedFrame drifted;
	ResourceXDecodedFrame settlement;
	ResourceXDecodedFrame stale;
	ResourceXSourceSettlementPlan plan;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	/* The retained carrier consumes the first authority edge (base 1 ->
	 * image 2), while two additional blockers and the final N -> X grant
	 * advance the authenticated master settlement to generation 5. */
	retain_resource_x_test_settlement_pair(
		tag, PCM_STATE_X, UINT64_C(60), UINT64_C(61), UINT64_C(3),
		&settlement);
	UT_ASSERT_EQ(settlement.common.base_authority_generation, UINT64_C(1));
	settlement.common.authority_generation = UINT64_C(5);
	memset(&plan, 0x5a, sizeof(plan));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
		&settlement, 0, &plan), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(plan.valid);
	UT_ASSERT_EQ(plan.pair_base_authority_generation, UINT64_C(1));
	UT_ASSERT_EQ(plan.pair_image_authority_generation, UINT64_C(2));
	UT_ASSERT_EQ(plan.settlement_authority_generation, UINT64_C(5));

	/* The final master authority is part of the stack-only freeze.  A changed
	 * settlement after prepare must not survive the physical-release gap. */
	drifted = settlement;
	drifted.common.authority_generation = UINT64_C(6);
	memset(&observation, 0x5a, sizeof(observation));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_commit_exact(
		&drifted, 0, &plan, &observation), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(observation.mismatch_mask
		& RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_PAIR_BYTES);

	/* A final authority which does not advance beyond the retained carrier
	 * is not a settlement for this request, even when all other bytes match. */
	stale = settlement;
	stale.common.authority_generation = UINT64_C(2);
	memset(&plan, 0x5a, sizeof(plan));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
		&stale, 0, &plan), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(!plan.valid);
}

UT_TEST(test_resource_x_direct_n_origin_offset_advances_blocker_and_grant)
{
	BufferTag tag = make_tag(191);
	PcmAuthoritySnapshot authority;
	ResourceXDecodedFrame blocked;
	ResourceXDecodedFrame first_assert;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame pair_grant;
	ResourceXDecodedFrame second_assert;
	ResourceXDecodedFrame settlement;
	ResourceXDurableProof durable;
	ResourceXIntentSlot grant_intent;
	ResourceXMasterSnapshot snapshot;
	ResourceXRequesterJoinSnapshot join;
	uint8 grant_bytes[RESOURCE_X_PROOF_V1_BYTES];

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);

	/* A pristine N resource starts Resource-X one generation ahead of the
	 * legacy transition counter.  That exact origin offset must survive the
	 * later X->N blocker edge and the successor N->X grant edge. */
	first_assert = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 0, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&first_assert, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_PROOF);
	memset(&durable, 0, sizeof(durable));
	durable.assertion = first_assert.common.logical_assertion;
	durable.base_authority_generation = 1;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = 41;
	durable.requester_target_generation = 41;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x55667788);
	durable.source_proof_crc32c = UINT32_C(0x99aabbcc);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(
		&durable, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.final_authority_generation, UINT64_C(2));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
		&first_assert.common.logical_assertion, &grant_intent, grant_bytes,
		sizeof(grant_bytes)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_stage_exact(
		&grant_intent, 101), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_grant_intent_complete_exact(
		&grant_intent));

	settlement = make_resource_x_master_frame(
		RESOURCE_X_WIRE_INSTALL_SETTLEMENT, tag, 0, 0);
	settlement.common.outcome = RESOURCE_X_OUTCOME_OK;
	settlement.common.authority_generation = 2;
	settlement.body.install_settlement.conversion_base_generation = 1;
	settlement.body.install_settlement.final_authority_generation = 2;
	settlement.body.install_settlement.requester_connection_generation = 73;
	settlement.body.install_settlement.requester_target_generation = 41;
	settlement.body.install_settlement.page_scn_lsn = 82;
	settlement.body.install_settlement.page_checksum = UINT32_C(0x55667788);
	settlement.body.install_settlement.source_proof_crc32c
		= UINT32_C(0x99aabbcc);
	settlement.body.install_settlement.installed_mode = PCM_STATE_X;
	settlement.body.install_settlement.requester_role
		= RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
	settlement.body.install_settlement.terminal_outcome
		= RESOURCE_X_OUTCOME_OK;
	settlement.body.install_settlement.terminal_state
		= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
		&settlement, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);

	second_assert = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 2, 2);
	second_assert.common.base_authority_generation = 2;
	second_assert.common.authority_generation = 2;
	second_assert.common.assertion_sequence = 42;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&second_assert, 2, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);

	make_resource_x_remote_join_pair(tag, 2, &pair_grant, &image);
	retarget_resource_x_remote_join_pair(&pair_grant, &image, 4, 42);
	blocked = make_resource_x_remote_blocked_frame(tag, 2, 0);
	blocked.common.base_authority_generation = 2;
	blocked.common.authority_generation = 2;
	blocked.common.assertion_sequence = 42;
	blocked.body.blocked_to_n.requester_target_generation = 42;
	blocked.body.blocked_to_n.source_proof_crc32c
		= image.common.semantic_crc32c;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(
		&blocked, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
	UT_ASSERT_EQ(snapshot.final_authority_generation, UINT64_C(4));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(authority.transition_count, UINT64_C(3));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_authority_grant_exact(
		&second_assert.common.logical_assertion, &grant),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(grant.common.base_authority_generation, UINT64_C(2));
	UT_ASSERT_EQ(grant.common.authority_generation, UINT64_C(4));

	cluster_node_id = 2;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 0, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(join.flags,
		RESOURCE_X_REQUESTER_JOIN_HAS_IMAGE
		| RESOURCE_X_REQUESTER_JOIN_HAS_GRANT
		| RESOURCE_X_REQUESTER_JOIN_READY);
}

UT_TEST(test_resource_x_head_accepts_exact_blockers_after_prior_pcm_s_churn)
{
	BufferTag tag = make_tag(207);
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame blocked;
	ResourceXMasterSnapshot snapshot;
	PcmAuthoritySnapshot authority;
	int cycle;
	int holder_node;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	for (cycle = 0; cycle < 2; cycle++) {
		for (holder_node = 1; holder_node < 4; holder_node++) {
			cluster_node_id = holder_node;
			cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
			cluster_pcm_lock_release(tag);
		}
	}

	cluster_node_id = 0;
	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 2, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 2, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(snapshot.incompatible_holders_bitmap, UINT32_C(1));

	blocked = make_resource_x_remote_blocked_frame(tag, 2, 0);
	blocked.common.observed_mode = PCM_STATE_S;
	blocked.body.blocked_to_n.source_fence[28] = PCM_STATE_S;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(
		&blocked, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
	UT_ASSERT_EQ(snapshot.final_authority_generation, UINT64_C(3));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(authority.transition_count, UINT64_C(9));
}

UT_TEST(test_resource_x_head_rejects_uncovered_pcm_transition_drift)
{
	BufferTag tag = make_tag(208);
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame local_proof;
	ResourceXMasterSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);

	cluster_node_id = 0;
	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 2, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 2, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_PROOF);

	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(
		tag, PCM_TRANS_X_TO_S_DOWNGRADE, 2));
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(
		tag, PCM_TRANS_S_TO_X_UPGRADE, 2));

	local_proof = make_resource_x_master_frame(
		RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION, tag, 2, 2);
	local_proof.common.observed_mode = PCM_STATE_X;
	local_proof.common.outcome = RESOURCE_X_OUTCOME_OK;
	local_proof.body.local_proof.local_holder_authority_generation = 71;
	local_proof.body.local_proof.requester_target_generation = 41;
	local_proof.body.local_proof.page_scn_lsn = 82;
	local_proof.body.local_proof.page_checksum = UINT32_C(0x55667788);
	local_proof.body.local_proof.local_image_proof_crc32c
		= UINT32_C(0x99aabbcc);
	local_proof.body.local_proof.requester_connection_generation = 73;
	local_proof.body.local_proof.local_proof_generation = 74;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_local_proof_exact(
		&local_proof, 2, &snapshot), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(snapshot.final_authority_generation, UINT64_C(0));
}

UT_TEST(test_resource_x_requester_join_accepts_either_order_and_never_overwrites)
{
	BufferTag image_first_tag = make_tag(155);
	BufferTag grant_first_tag = make_tag(156);
	BufferTag mismatch_tag = make_tag(157);
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame changed;
	ResourceXDecodedFrame retained_grant;
	ResourceXDecodedFrame retained_image;
	ResourceXDecodedFrame resealed_image;
	ResourceXRequesterJoinSnapshot snapshot;
	ResourceXRequesterJoinSnapshot first;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 image_payload[RESOURCE_X_IMAGE_V1_BYTES];
	uint16 image_payload_bytes = 0;

	reset_fake_pcm_runtime(8);
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(image_first_tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_release(image_first_tag);
	cluster_pcm_lock_acquire(grant_first_tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_release(grant_first_tag);
	cluster_pcm_lock_acquire(mismatch_tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_release(mismatch_tag);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);

	make_resource_x_remote_join_pair(image_first_tag, 2, &grant, &image);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.flags, RESOURCE_X_REQUESTER_JOIN_HAS_IMAGE);
	UT_ASSERT(snapshot.t_image_us != 0);
	UT_ASSERT_EQ(snapshot.t_grant_us, 0);
	first = snapshot;
	changed = image;
	changed.common.sender_connection_generation++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&changed, 0, &snapshot), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(snapshot.t_image_us, first.t_image_us);
	changed = image;
	changed.body.image_envelope.page_bytes[0] ^= UINT8_C(1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&changed, 0, &snapshot), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(snapshot.flags, RESOURCE_X_REQUESTER_JOIN_HAS_IMAGE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 2, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.flags,
		RESOURCE_X_REQUESTER_JOIN_HAS_IMAGE
		| RESOURCE_X_REQUESTER_JOIN_HAS_GRANT
		| RESOURCE_X_REQUESTER_JOIN_READY);
	UT_ASSERT(snapshot.t_grant_us != 0);
	first = snapshot;
	changed = grant;
	changed.common.sender_connection_generation++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&changed, 2, &snapshot), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(snapshot.t_image_us, first.t_image_us);
	UT_ASSERT_EQ(snapshot.t_grant_us, first.t_grant_us);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_frames_exact(
		&grant.common.logical_assertion, &retained_grant,
		&retained_image, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(retained_grant.kind, RESOURCE_X_WIRE_AUTHORITY_GRANT);
	UT_ASSERT_EQ(retained_image.kind, RESOURCE_X_WIRE_IMAGE_ENVELOPE);
	UT_ASSERT_EQ(retained_grant.body.authority_grant.final_authority_generation,
		3);
	UT_ASSERT_EQ(retained_image.body.image_envelope.page_bytes[4095],
		image.body.image_envelope.page_bytes[4095]);

	make_resource_x_remote_join_pair(grant_first_tag, 2, &grant, &image);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 2, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.flags, RESOURCE_X_REQUESTER_JOIN_HAS_GRANT);
	changed = image;
	changed.common.sender_connection_generation = 85;
	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, &changed, image_payload,
		sizeof(image_payload), &image_payload_bytes, &reject));
	UT_ASSERT_EQ(image_payload_bytes, RESOURCE_X_IMAGE_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, image_payload, image_payload_bytes,
		&resealed_image, &reject));
	UT_ASSERT(resealed_image.common.semantic_crc32c
		!= image.common.semantic_crc32c);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&resealed_image, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT((snapshot.flags & RESOURCE_X_REQUESTER_JOIN_READY) != 0);

	make_resource_x_remote_join_pair(mismatch_tag, 2, &grant, &image);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);
	grant.body.authority_grant.source_proof_crc32c++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 2, &snapshot), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(snapshot.flags, RESOURCE_X_REQUESTER_JOIN_HAS_IMAGE);
	UT_ASSERT_EQ(snapshot.t_grant_us, 0);
}

UT_TEST(test_resource_x_requester_join_accepts_exact_base_carrier_final_lineage)
{
	BufferTag tag = make_tag(181);
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXRequesterJoinSnapshot join;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	make_resource_x_remote_join_pair(tag, 2, &grant, &image);
	UT_ASSERT_EQ(grant.common.base_authority_generation, 1);
	UT_ASSERT_EQ(image.common.authority_generation, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(join.flags, RESOURCE_X_REQUESTER_JOIN_HAS_IMAGE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 2, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(join.flags,
		RESOURCE_X_REQUESTER_JOIN_HAS_IMAGE
		| RESOURCE_X_REQUESTER_JOIN_HAS_GRANT
		| RESOURCE_X_REQUESTER_JOIN_READY);
	UT_ASSERT_EQ(join.base_authority_generation, 1);
	UT_ASSERT_EQ(join.final_authority_generation, 3);
}

UT_TEST(test_resource_x_requester_join_uses_live_bootstrap_base_not_binding_floor)
{
	BufferTag retired_tag = make_tag(205);
	BufferTag tag = make_tag(206);
	PcmAuthoritySnapshot authority;
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef entry_ref;
	ResourceXAssertion retired_assertion;
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXAcquisitionRef ref;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBufferInstallProof install;
	ResourceXBufferActivationProof activation;
	ResourceXBootstrapRoundAction action;
	ResourceXBootstrapRoundFailureSnapshot failure;
	ResourceXRequesterJoinSnapshot join;
	uint64 retired_binding_generation;
	int cycle;

	reset_fake_pcm_runtime(4);
	fake_pcm_clock_us = UINT64_C(110);
	cluster_node_id = 2;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);

	/* Raise only the node-local physical binding/retirement floor.  It is
	 * lifecycle/ABA state for another resource, not current master authority
	 * for the request below. */
	UT_ASSERT(pcm_entry_ref_acquire(
		&retired_tag, true, &entry_ref, &acquire_result));
	retired_binding_generation = entry_ref.binding_generation;
	pcm_entry_ref_release(&entry_ref);
	for (cycle = 0; cycle < 3; cycle++) {
		UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
			retired_tag, PCM_TRANS_N_TO_X, 2), PCM_GCS_TRANSITION_APPLIED);
		UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
			retired_tag, PCM_TRANS_X_TO_N_RELEASE, 2),
			PCM_GCS_TRANSITION_APPLIED);
	}
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(retired_tag, &authority));
	UT_ASSERT(authority.transition_count > UINT64_C(3));
	UT_ASSERT(resource_x_assertion_init(
		&retired_tag, 2, &retired_assertion));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_base_bind_exact(
		&retired_assertion, 17, authority.transition_count, &authority),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(pcm_entry_try_retire_exact(&retired_tag,
		retired_binding_generation, PCM_RETIRE_REASON_HOLDER_RELEASE));

	/* The current master ACK freezes base=2 for this exact attempt.  A grant
	 * at final=3 must be judged against that live round, not the unrelated
	 * local binding floor inherited during physical slot reuse. */
	UT_ASSERT(resource_x_assertion_init(&tag, 2, &assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&assertion, 0, 17, 31, 77, 51, 61, UINT64_MAX - 1, UINT64_C(1000000), UINT64_C(100),
		UINT64_C(50), false, 0, &request, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(
		&request, UINT64_C(2), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, UINT64_C(110), &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
			&assertion, 0, 17, 31, 77, 51, 61, UINT64_C(50), &failure),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(failure.requester_base_generation > UINT64_C(3));
	UT_ASSERT_EQ(failure.base_authority_generation, UINT64_C(2));

	make_resource_x_remote_join_pair(tag, 2, &grant, &image);
	retarget_resource_x_remote_join_pair(&grant, &image, UINT64_C(3),
		assert_frame.common.assertion_sequence);
	grant.common.base_authority_generation
		= assert_frame.common.base_authority_generation;
	grant.common.ordered_lane = assert_frame.common.ordered_lane;
	grant.common.flags = 0;
	grant.body.authority_grant.source_carrier_generation = 0;
	grant.body.authority_grant.proof_kind
		= RESOURCE_X_PROOF_DURABLE_STORAGE;
	grant.body.authority_grant.source_disposition
		= RESOURCE_X_DISPOSITION_DURABLE_STORAGE;
	UT_ASSERT_EQ(grant.common.base_authority_generation, UINT64_C(2));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 0, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(join.flags,
		RESOURCE_X_REQUESTER_JOIN_HAS_GRANT
		| RESOURCE_X_REQUESTER_JOIN_READY);

	ref = make_resource_x_acquisition_ref(tag, 2, 17,
		assert_frame.common.assertion_sequence);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref),
		RESOURCE_X_APPLY_APPLIED);
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 9;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = ref.acquisition_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(
		&ref, &install), RESOURCE_X_APPLY_APPLIED);
	memset(&activation, 0, sizeof(activation));
	activation.ownership_generation = install.ownership_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(
		&ref, &activation), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&ref, 31, 77, install.ownership_generation));
}

UT_TEST(test_resource_x_requester_join_accepts_s_carrier_either_order_and_rejects_mode_drift)
{
	BufferTag image_first_tag = make_tag(188);
	BufferTag grant_first_tag = make_tag(189);
	BufferTag mismatch_tag = make_tag(190);
	ResourceXDecodedFrame canonical_image;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXRequesterJoinSnapshot join;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 image_payload[RESOURCE_X_IMAGE_V1_BYTES];
	uint16 image_payload_bytes = 0;

	reset_fake_pcm_runtime(8);
	cluster_node_id = 2;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);

	make_resource_x_s_remote_join_pair(
		image_first_tag, 2, &grant, &image);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(join.flags, RESOURCE_X_REQUESTER_JOIN_HAS_IMAGE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &join), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 2, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(join.flags,
		RESOURCE_X_REQUESTER_JOIN_HAS_IMAGE
		| RESOURCE_X_REQUESTER_JOIN_HAS_GRANT
		| RESOURCE_X_REQUESTER_JOIN_READY);

	make_resource_x_s_remote_join_pair(
		grant_first_tag, 2, &grant, &image);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 2, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(join.flags, RESOURCE_X_REQUESTER_JOIN_HAS_GRANT);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 2, &join), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT((join.flags & RESOURCE_X_REQUESTER_JOIN_READY) != 0);

	make_resource_x_s_remote_join_pair(mismatch_tag, 2, &grant, &image);
	image.common.observed_mode = PCM_STATE_X;
	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, &image, image_payload,
		sizeof(image_payload), &image_payload_bytes, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, image_payload, image_payload_bytes,
		&canonical_image, &reject));
	image = canonical_image;
	grant.body.authority_grant.source_proof_crc32c
		= image.common.semantic_crc32c;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 2, &join), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(join.flags, RESOURCE_X_REQUESTER_JOIN_HAS_IMAGE);
}

UT_TEST(test_resource_x_requester_join_accepts_multi_blocker_authority_span)
{
	BufferTag tag = make_tag(191);
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXRequesterJoinSnapshot join;

	reset_fake_pcm_runtime(8);
	cluster_node_id = 0;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	make_resource_x_s_remote_join_pair(tag, 0, &grant, &image);

	/* Three remote S blockers consume three holder authority edges before
	 * the final N->X grant.  The selected carrier remains the exact base+1
	 * image; the authenticated master grant is later, not necessarily the
	 * immediately adjacent generation. */
	grant.common.authority_generation = UINT64_C(5);
	grant.body.authority_grant.final_authority_generation = UINT64_C(5);
	UT_ASSERT_EQ(grant.common.base_authority_generation, UINT64_C(1));
	UT_ASSERT_EQ(image.common.authority_generation, UINT64_C(2));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 1, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(join.flags, RESOURCE_X_REQUESTER_JOIN_HAS_IMAGE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 0, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(join.flags,
		RESOURCE_X_REQUESTER_JOIN_HAS_IMAGE
		| RESOURCE_X_REQUESTER_JOIN_HAS_GRANT
		| RESOURCE_X_REQUESTER_JOIN_READY);
}

UT_TEST(test_resource_x_requester_join_creates_fresh_local_entry_before_t1)
{
	BufferTag tag = make_tag(172);
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXRequesterJoinSnapshot join;
	ResourceXAcquisitionRef ref;

	reset_fake_pcm_runtime(8);
	cluster_node_id = 2;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	make_resource_x_remote_join_pair(tag, 2, &grant, &image);

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 2, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(join.flags, RESOURCE_X_REQUESTER_JOIN_HAS_GRANT);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT((join.flags & RESOURCE_X_REQUESTER_JOIN_READY) != 0);
	memset(&ref, 0, sizeof(ref));
	ref.assertion = join.assertion;
	ref.formation = join.resource_formation;
	ref.acquisition_generation = join.requester_target_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref),
		RESOURCE_X_APPLY_APPLIED);
}

static void
wait_for_resource_x_timestamp_after(uint64 timestamp_us)
{
	instr_time now;

	do
	{
		INSTR_TIME_SET_CURRENT(now);
	} while ((uint64)INSTR_TIME_GET_MICROSEC(now) <= timestamp_us);
}

static void
complete_resource_x_remote_requester_terminal(
	const ResourceXRequesterJoinSnapshot *join, ResourceXAcquisitionRef *ref_out,
	ResourceXBufferActivationProof *activation_out)
{
	ResourceXBufferInstallProof install;

	memset(ref_out, 0, sizeof(*ref_out));
	ref_out->assertion = join->assertion;
	ref_out->formation = join->resource_formation;
	ref_out->acquisition_generation = join->requester_target_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(ref_out),
				 RESOURCE_X_APPLY_APPLIED);
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 9;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = ref_out->acquisition_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(ref_out, &install),
				 RESOURCE_X_APPLY_APPLIED);
	memset(activation_out, 0, sizeof(*activation_out));
	activation_out->ownership_generation = install.ownership_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(
		ref_out, activation_out), RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_resource_x_remote_terminal_settles_o1_once_and_keeps_first_times)
{
	BufferTag image_first_tag = make_tag(159);
	BufferTag grant_first_tag = make_tag(160);
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame retained_grant;
	ResourceXDecodedFrame retained_image;
	ResourceXDecodedFrame settlement;
	ResourceXRequesterJoinSnapshot join;
	ResourceXRequesterJoinSnapshot ready_join;
	ResourceXAssertion assertion;
	ResourceXAcquisitionRef ref;
	ResourceXBufferActivationProof activation;
	ResourceXIntentSlot settlement_intent;
	ResourceXO1Stats stats;
	ResourceXO1Stats duplicate_stats;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 settlement_payload[RESOURCE_X_PROOF_V1_BYTES];
	uint32 examined = 0;

	reset_fake_pcm_runtime(8);
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(image_first_tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_release(image_first_tag);
	cluster_pcm_lock_acquire(grant_first_tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_release(grant_first_tag);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	cluster_pcm_lock_resource_x_o1_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.remote_install_observed_count, 0);

	make_resource_x_remote_join_pair(image_first_tag, 2, &grant, &image);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &join), RESOURCE_X_APPLY_APPLIED);
	wait_for_resource_x_timestamp_after(join.t_image_us);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 2, &join), RESOURCE_X_APPLY_APPLIED);
	ready_join = join;
	complete_resource_x_remote_requester_terminal(&join, &ref, &activation);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_probe_exact(
		4, &settlement_intent, settlement_payload,
		sizeof(settlement_payload), &examined),
		RESOURCE_X_INTENT_PROBE_FOUND);
	UT_ASSERT_EQ(settlement_intent.body.owner_kind,
		RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT);
	UT_ASSERT_EQ(settlement_intent.destination_node,
		(uint32)ready_join.grant_source_node);
	UT_ASSERT_EQ(settlement_intent.kind,
		RESOURCE_X_WIRE_INSTALL_SETTLEMENT);
	UT_ASSERT_EQ(settlement_intent.payload_bytes,
		RESOURCE_X_SHORT_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, settlement_payload,
		settlement_intent.payload_bytes, &settlement, &reject));
	UT_ASSERT(resource_x_assertion_equal(
		&settlement.common.logical_assertion, &ready_join.assertion));
	UT_ASSERT_EQ(settlement.common.base_authority_generation,
		ready_join.base_authority_generation);
	UT_ASSERT_EQ(settlement.common.resource_formation,
		ready_join.resource_formation);
	UT_ASSERT_EQ(settlement.common.master_session_incarnation,
		ready_join.master_session_incarnation);
	UT_ASSERT_EQ(settlement.common.assertion_sequence,
		ready_join.assertion_sequence);
	UT_ASSERT_EQ(settlement.common.authority_generation,
		ready_join.final_authority_generation);
	UT_ASSERT_EQ(settlement.body.install_settlement.requester_target_generation,
		ready_join.requester_target_generation);
	UT_ASSERT_EQ(settlement.body.install_settlement.installed_mode,
		PCM_STATE_X);
	UT_ASSERT_EQ(settlement.body.install_settlement.requester_role,
		RESOURCE_X_REQUESTER_ROLE_ACQUIRER);
	UT_ASSERT_EQ(settlement.body.install_settlement.terminal_state,
		RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&settlement_intent, settlement_intent.first_armed_us + 1),
		RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&settlement_intent));
	assertion = join.assertion;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_frames_exact(
		&assertion, &retained_grant, &retained_image, &join),
		RESOURCE_X_APPLY_NOT_FOUND);
	cluster_pcm_lock_resource_x_o1_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.remote_install_observed_count, 1);
	UT_ASSERT_EQ(stats.remote_grant_after_image_count, 1);
	UT_ASSERT_EQ(stats.remote_image_at_or_after_grant_count, 0);
	UT_ASSERT_EQ(stats.remote_episode_excluded_no_install, 0);
	UT_ASSERT_EQ(stats.remote_episode_excluded_missing_grant, 0);
	UT_ASSERT_EQ(stats.remote_episode_excluded_missing_image, 0);
	UT_ASSERT_EQ(stats.last_remote_t_image_us, ready_join.t_image_us);
	UT_ASSERT_EQ(stats.last_remote_t_grant_us, ready_join.t_grant_us);
	UT_ASSERT(stats.last_remote_t_install_us != 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(
		&ref, &activation), RESOURCE_X_APPLY_DUPLICATE);
	cluster_pcm_lock_resource_x_o1_stats_snapshot(&duplicate_stats);
	UT_ASSERT_EQ(memcmp(&duplicate_stats, &stats, sizeof(stats)), 0);

	make_resource_x_remote_join_pair(grant_first_tag, 2, &grant, &image);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 2, &join), RESOURCE_X_APPLY_APPLIED);
	wait_for_resource_x_timestamp_after(join.t_grant_us);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &join), RESOURCE_X_APPLY_APPLIED);
	ready_join = join;
	complete_resource_x_remote_requester_terminal(&join, &ref, &activation);
	assertion = join.assertion;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_frames_exact(
		&assertion, &retained_grant, &retained_image, &join),
		RESOURCE_X_APPLY_NOT_FOUND);
	cluster_pcm_lock_resource_x_o1_stats_snapshot(&stats);
	UT_ASSERT_EQ(stats.remote_install_observed_count, 2);
	UT_ASSERT_EQ(stats.remote_grant_after_image_count, 1);
	UT_ASSERT_EQ(stats.remote_image_at_or_after_grant_count, 1);
	UT_ASSERT_EQ(stats.last_remote_t_image_us, ready_join.t_image_us);
	UT_ASSERT_EQ(stats.last_remote_t_grant_us, ready_join.t_grant_us);
	UT_ASSERT(stats.last_remote_t_install_us != 0);
}

UT_TEST(test_resource_x_requester_floors_keep_authority_and_target_axes_distinct)
{
	BufferTag tag = make_tag(161);
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame retained_grant;
	ResourceXDecodedFrame retained_image;
	ResourceXRequesterJoinSnapshot join;
	ResourceXAssertion assertion;
	ResourceXAcquisitionRef ref;
	ResourceXBufferActivationProof activation;
	ResourceXExecutorSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	make_resource_x_remote_join_pair(tag, 2, &grant, &image);
	UT_ASSERT_EQ(grant.body.authority_grant.final_authority_generation, 3);
	UT_ASSERT_EQ(grant.body.authority_grant.requester_target_generation, 41);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 2, &join), RESOURCE_X_APPLY_APPLIED);
	assertion = join.assertion;
	complete_resource_x_remote_requester_terminal(&join, &ref, &activation);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_probe_exact(&ref, &snapshot),
				 RESOURCE_X_EXECUTOR_COMPLETE);
	UT_ASSERT_EQ(snapshot.requester_base_generation, 3);
	UT_ASSERT_EQ(snapshot.retired_acquisition_generation, 41);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_frames_exact(
		&assertion, &retained_grant, &retained_image, &join),
		RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &join), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 2, &join), RESOURCE_X_APPLY_STALE);
}

UT_TEST(test_resource_x_requester_join_gates_both_floors_before_successor)
{
	BufferTag tag = make_tag(162);
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXRequesterJoinSnapshot join;
	ResourceXAcquisitionRef ref;
	ResourceXBufferActivationProof activation;
	ResourceXIntentSlot settlement_intent;
	ResourceXExecutorSnapshot snapshot;
	uint8 settlement_payload[RESOURCE_X_PROOF_V1_BYTES];
	uint32 examined = 0;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	make_resource_x_remote_join_pair(tag, 2, &grant, &image);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 2, &join), RESOURCE_X_APPLY_APPLIED);
	complete_resource_x_remote_requester_terminal(&join, &ref, &activation);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_probe_exact(
		4, &settlement_intent, settlement_payload,
		sizeof(settlement_payload), &examined),
		RESOURCE_X_INTENT_PROBE_FOUND);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&settlement_intent, settlement_intent.first_armed_us + 1),
		RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&settlement_intent));

	make_resource_x_remote_join_pair(tag, 2, &grant, &image);
	retarget_resource_x_remote_join_pair(&grant, &image, 5, 41);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &join), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 2, &join), RESOURCE_X_APPLY_STALE);

	make_resource_x_remote_join_pair(tag, 2, &grant, &image);
	retarget_resource_x_remote_join_pair(&grant, &image, 3, 42);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &join), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 2, &join), RESOURCE_X_APPLY_STALE);

	make_resource_x_remote_join_pair(tag, 2, &grant, &image);
	retarget_resource_x_remote_join_pair(&grant, &image, 5, 42);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 0, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 2, &join), RESOURCE_X_APPLY_APPLIED);
	complete_resource_x_remote_requester_terminal(&join, &ref, &activation);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_probe_exact(&ref, &snapshot),
				 RESOURCE_X_EXECUTOR_COMPLETE);
	UT_ASSERT_EQ(snapshot.requester_base_generation, 5);
	UT_ASSERT_EQ(snapshot.retired_acquisition_generation, 42);
}

UT_TEST(test_resource_x_x_source_defers_self_master_grd_transition_to_ingress)
{
	BufferTag tag = make_tag(158);
	ResourceXDecodedFrame block;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame newer_block;
	ResourceXDecodedFrame newer_image;
	ResourceXDecodedFrame newer_status;
	ResourceXDecodedFrame retained_image;
	ResourceXDecodedFrame status;
	ResourceXDecodedFrame decoded;
	ResourceXIntentSlot image_intent;
	ResourceXIntentSlot status_intent;
	ResourceXIntentSlot probe;
	ResourceXReconfigToken token;
	ResourceXReconfigBatch batch;
	ResourceXAssertion requester_assertion;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 image_payload[RESOURCE_X_IMAGE_V1_BYTES];
	uint8 newer_image_payload[RESOURCE_X_IMAGE_V1_BYTES];
	uint8 probe_payload[RESOURCE_X_IMAGE_V1_BYTES];
	uint8 status_payload[RESOURCE_X_PROOF_V1_BYTES];
	uint32 examined = 0;
	uint32 canonical_image_crc = 0;
	uint32 canonical_proof_crc = 0;
	uint64 source_generation = 0;
	uint16 newer_image_payload_bytes = 0;
	uint16 image_payload_bytes = 0;
	bool saw_image = false;
	bool saw_status = false;
	int calls;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	block = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCK_TO_N, tag, 2, 0);
	block.common.observed_mode = PCM_STATE_X;
	block.common.target_mode = PCM_STATE_N;
	block.common.source_candidate = 1;
	block.common.retain_pi_if_dirty = 1;

	make_resource_x_remote_join_pair(tag, 2, &grant, &image);
	image.common.sender_connection_generation = 84;
	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, &image, image_payload,
		sizeof(image_payload), &image_payload_bytes, &reject));
	UT_ASSERT_EQ(image_payload_bytes, RESOURCE_X_IMAGE_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, image_payload, image_payload_bytes,
		&decoded, &reject));
	image = decoded;
	status = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCKED_TO_N, tag, 2, 0);
	status.common.observed_mode = PCM_STATE_X;
	status.common.target_mode = PCM_STATE_N;
	status.common.outcome = RESOURCE_X_OUTCOME_OK;
	status.common.flags = RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED;
	status.blocked_has_remote_proof = true;
	memcpy(status.body.blocked_to_n.source_fence,
		image.body.image_envelope.source_fence,
		sizeof(status.body.blocked_to_n.source_fence));
	status.body.blocked_to_n.source_carrier_generation
		= image.body.image_envelope.source_carrier_generation;
	status.body.blocked_to_n.requester_target_generation
		= image.body.image_envelope.requester_target_generation;
	status.body.blocked_to_n.page_scn_lsn
		= image.body.image_envelope.page_scn_lsn;
	status.body.blocked_to_n.dependency_count
		= image.body.image_envelope.dependency_count;
	memcpy(status.body.blocked_to_n.dependencies,
		image.body.image_envelope.dependencies,
		sizeof(status.body.blocked_to_n.dependencies));
	status.body.blocked_to_n.source_proof_crc32c
		= image.common.semantic_crc32c;
	status.body.blocked_to_n.page_checksum
		= image.body.image_envelope.page_checksum;
	status.body.blocked_to_n.source_disposition
		= RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
	status.body.blocked_to_n.proof_kind
		= RESOURCE_X_PROOF_REMOTE_CARRIER;
	status.body.blocked_to_n.holder_connection_generation = 91;
	status.body.blocked_to_n.acting_formation = 17;

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_source_exact(
		&block, 0, &status, &image), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_supersedes_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		0, block.common.master_session_incarnation), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_query(tag), PCM_LOCK_MODE_X);
	/* Retaining canonical type-18/type-15 bytes is not transport readiness.
	 * The holder must finish the exact physical X revoke before publishing
	 * either half of the pair to the outbound scanner. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
		&block.common.logical_assertion, &status_intent, status_payload,
		sizeof(status_payload)), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_intent_snapshot_exact(
		&block.common.logical_assertion, &image_intent, image_payload,
		sizeof(image_payload)), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_publish_needed_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		0, block.common.master_session_incarnation), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_publish_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		0, block.common.master_session_incarnation), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_publish_needed_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		0, block.common.master_session_incarnation), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
		&block.common.logical_assertion, &status_intent, status_payload,
		sizeof(status_payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(status_intent.kind, RESOURCE_X_WIRE_BLOCKED_TO_N);
	UT_ASSERT_EQ(status_intent.payload_bytes, RESOURCE_X_PROOF_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_BLOCKED_TO_N, status_payload,
		status_intent.payload_bytes, &decoded, &reject));
	canonical_proof_crc
		= decoded.body.blocked_to_n.source_proof_crc32c;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_intent_snapshot_exact(
		&block.common.logical_assertion, &image_intent, image_payload,
		sizeof(image_payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(image_intent.kind, RESOURCE_X_WIRE_IMAGE_ENVELOPE);
	UT_ASSERT_EQ(image_intent.payload_bytes, RESOURCE_X_IMAGE_V1_BYTES);
	UT_ASSERT_EQ(image_intent.destination_node, 2);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, image_payload,
		image_intent.payload_bytes, &decoded, &reject));
	UT_ASSERT_EQ(decoded.common.sender_connection_generation, UINT32_C(1));
	canonical_image_crc = decoded.common.semantic_crc32c;
	UT_ASSERT_EQ(canonical_proof_crc, canonical_image_crc);
	UT_ASSERT(canonical_image_crc != image.common.semantic_crc32c);
	UT_ASSERT(memcmp(decoded.body.image_envelope.page_bytes,
		image.body.image_envelope.page_bytes, RESOURCE_X_PAGE_BYTES) == 0);

	for (calls = 0; calls < 12 && (!saw_status || !saw_image); calls++) {
		ResourceXIntentProbeResult probe_result
			= cluster_pcm_lock_resource_x_outbound_intent_probe_exact(
				1, &probe, probe_payload, sizeof(probe_payload), &examined);

		UT_ASSERT(probe_result == RESOURCE_X_INTENT_PROBE_FOUND
			|| probe_result == RESOURCE_X_INTENT_PROBE_MORE
			|| probe_result == RESOURCE_X_INTENT_PROBE_COMPLETE);
		if (probe_result != RESOURCE_X_INTENT_PROBE_FOUND)
			continue;
		if (probe.kind == RESOURCE_X_WIRE_BLOCKED_TO_N)
			saw_status = true;
		if (probe.kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE)
			saw_image = true;
	}
	UT_ASSERT(saw_status);
	UT_ASSERT(saw_image);

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&status_intent, 101), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&status_intent));
	/* A partial transport terminal is not permission to recreate only the
	 * missing half.  Keep the surviving image exact and fail closed until the
	 * already-admitted pair finishes its normal drain. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_publish_needed_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		0, block.common.master_session_incarnation), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_publish_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		0, block.common.master_session_incarnation),
		RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
		&block.common.logical_assertion, &probe, probe_payload,
		sizeof(probe_payload)), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_intent_snapshot_exact(
		&block.common.logical_assertion, &probe, probe_payload,
		sizeof(probe_payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&image_intent, 102), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&image_intent));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_exact(
		&block.common.logical_assertion, &retained_image),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(retained_image.kind, RESOURCE_X_WIRE_IMAGE_ENVELOPE);
	UT_ASSERT_EQ(retained_image.common.semantic_crc32c,
		canonical_image_crc);
	UT_ASSERT(memcmp(retained_image.body.image_envelope.page_bytes,
		image.body.image_envelope.page_bytes, RESOURCE_X_PAGE_BYTES) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_drain_prepare_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		0, block.common.master_session_incarnation + 1, &source_generation),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(source_generation, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_drain_prepare_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		0, block.common.master_session_incarnation, &source_generation),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(source_generation,
		image.body.image_envelope.source_carrier_generation - 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_drain_commit_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		0, block.common.master_session_incarnation, source_generation),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, cluster_node_id,
		&requester_assertion));
	source_generation = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_drain_prepare_exact(
		&requester_assertion, block.common.assertion_sequence,
		0, block.common.master_session_incarnation, &source_generation),
		RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(source_generation, 0);
	source_generation = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_drain_prepare_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		0, block.common.master_session_incarnation, &source_generation),
		RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(source_generation, 0);

	/* A replay of an already drained pair must not resurrect its outbound
	 * intents or reopen the retained physical carrier. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_source_exact(
		&block, 0, &status, &image), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
		&block.common.logical_assertion, &status_intent, status_payload,
		sizeof(status_payload)), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_intent_snapshot_exact(
		&block.common.logical_assertion, &image_intent, image_payload,
		sizeof(image_payload)), RESOURCE_X_APPLY_NOT_FOUND);

	/* A later direct-current-X lineage for the same requester may replace the
	 * drained tombstone only with a strictly newer admission.  A lower attempt
	 * in that same {resource, requester} namespace remains stale even when its
	 * physical carrier generation is newer. */
	newer_block = block;
	newer_status = status;
	newer_image = image;
	newer_block.common.assertion_sequence = 40;
	newer_status.common.assertion_sequence = 40;
	newer_status.body.blocked_to_n.source_carrier_generation = 62;
	newer_status.body.blocked_to_n.requester_target_generation = 40;
	newer_image.common.assertion_sequence = 40;
	newer_image.body.image_envelope.source_carrier_generation = 62;
	newer_image.body.image_envelope.requester_target_generation = 40;
	reject = RESOURCE_X_WIRE_REJECT_NONE;
	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, &newer_image,
		newer_image_payload, sizeof(newer_image_payload),
		&newer_image_payload_bytes, &reject));
	UT_ASSERT_EQ(newer_image_payload_bytes, RESOURCE_X_IMAGE_V1_BYTES);
	reject = RESOURCE_X_WIRE_REJECT_NONE;
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, newer_image_payload,
		newer_image_payload_bytes, &newer_image, &reject));
	newer_status.body.blocked_to_n.source_proof_crc32c
		= newer_image.common.semantic_crc32c;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_source_exact(
		&newer_block, 0, &newer_status, &newer_image),
		RESOURCE_X_APPLY_STALE);

	/* Attempts from different requesters are in different namespaces and are
	 * therefore not numerically comparable.  Once the old pair has exact DRAIN
	 * and no outbound owner, a new requester with a lower attempt may replace it
	 * only through a strictly newer physical carrier.  The old DRAIN remains
	 * replayable after that replacement. */
	newer_block = block;
	newer_status = status;
	newer_image = image;
	UT_ASSERT(resource_x_assertion_init(&tag, 3,
		&newer_block.common.logical_assertion));
	UT_ASSERT(resource_x_assertion_init(&tag, 3,
		&newer_status.common.logical_assertion));
	UT_ASSERT(resource_x_assertion_init(&tag, 3,
		&newer_image.common.logical_assertion));
	newer_block.common.assertion_sequence = 1;
	newer_status.common.assertion_sequence = 1;
	newer_status.body.blocked_to_n.source_carrier_generation = 62;
	newer_status.body.blocked_to_n.requester_target_generation = 1;
	newer_image.common.assertion_sequence = 1;
	newer_image.common.action_node = 3;
	newer_image.body.image_envelope.source_carrier_generation = 62;
	newer_image.body.image_envelope.requester_target_generation = 1;
	reject = RESOURCE_X_WIRE_REJECT_NONE;
	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, &newer_image,
		newer_image_payload, sizeof(newer_image_payload),
		&newer_image_payload_bytes, &reject));
	UT_ASSERT_EQ(newer_image_payload_bytes, RESOURCE_X_IMAGE_V1_BYTES);
	reject = RESOURCE_X_WIRE_REJECT_NONE;
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, newer_image_payload,
		newer_image_payload_bytes, &newer_image, &reject));
	newer_status.body.blocked_to_n.source_proof_crc32c
		= newer_image.common.semantic_crc32c;
	/* The retained predecessor has the same logical (tag, requester) identity,
	 * only within one requester namespace.  A different requester must not use
	 * that predecessor as evidence for its attempt. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_publish_needed_exact(
		&newer_block.common.logical_assertion,
		newer_block.common.assertion_sequence, 0,
		newer_block.common.master_session_incarnation),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_source_exact(
		&newer_block, 0, &newer_status, &newer_image),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_publish_exact(
		&newer_block.common.logical_assertion,
		newer_block.common.assertion_sequence, 0,
		newer_block.common.master_session_incarnation),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_supersedes_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		0, block.common.master_session_incarnation), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_supersedes_exact(
		&newer_block.common.logical_assertion,
		newer_block.common.assertion_sequence, 0,
		newer_block.common.master_session_incarnation), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_supersedes_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		0, block.common.master_session_incarnation + 1),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
		&newer_block.common.logical_assertion, &status_intent, status_payload,
		sizeof(status_payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_intent_snapshot_exact(
		&newer_block.common.logical_assertion, &image_intent, image_payload,
		sizeof(image_payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&status_intent, 103), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&status_intent));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&image_intent, 104), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&image_intent));
	source_generation = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_drain_prepare_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		0, block.common.master_session_incarnation, &source_generation),
		RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(source_generation, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_drain_prepare_exact(
		&newer_block.common.logical_assertion,
		newer_block.common.assertion_sequence, 0,
		newer_block.common.master_session_incarnation, &source_generation),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(source_generation, 61);
	UT_ASSERT_EQ(cluster_pcm_lock_query(tag), PCM_LOCK_MODE_X);

	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
		RESOURCE_X_RECONFIG_MORE);
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
		RESOURCE_X_RECONFIG_ORPHAN);
	UT_ASSERT_EQ(batch.residual_count, UINT64_C(1));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_exact(
		&newer_block.common.logical_assertion, &retained_image),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(retained_image.common.assertion_sequence, UINT64_C(1));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
		&block.common.logical_assertion, &status_intent, status_payload,
		sizeof(status_payload)), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_intent_snapshot_exact(
		&block.common.logical_assertion, &image_intent, image_payload,
		sizeof(image_payload)), RESOURCE_X_APPLY_NOT_FOUND);
}

UT_TEST(test_resource_x_source_settlement_drains_only_the_exact_retained_pair)
{
	BufferTag tag = make_tag(194);
	ResourceXAssertion holder_assertion;
	ResourceXDecodedFrame bootstrap;
	ResourceXDecodedFrame bootstrap_ack;
	ResourceXDecodedFrame bootstrap_assert;
	ResourceXDecodedFrame block;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame holder_grant;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame holder_image;
	ResourceXDecodedFrame status;
	ResourceXDecodedFrame settlement;
	ResourceXDecodedFrame stale;
	ResourceXDecodedFrame ack;
	ResourceXAcquisitionRef holder_ref;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBufferActivationProof activation;
	ResourceXBufferInstallProof install;
	ResourceXBootstrapRoundAction action;
	PcmGrdProtocolDebtStats debt;
	ResourceXSourceSettlementPlan plan;
	ResourceXSourceSettlementPlan stale_plan;
	ResourceXSourceSettlementCommitObservation commit_observation;
	ResourceXSourceSettlementCommitObservation prepare_observation;
	ResourceXIntentSlot image_intent;
	ResourceXIntentSlot requester_settlement_intent;
	ResourceXIntentSlot status_intent;
	ResourceXIntentProbeResult probe_result = RESOURCE_X_INTENT_PROBE_IDLE;
	ResourceXRequesterJoinSnapshot join;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 image_payload[RESOURCE_X_IMAGE_V1_BYTES];
	uint8 requester_settlement_payload[RESOURCE_X_PROOF_V1_BYTES];
	uint8 status_payload[RESOURCE_X_PROOF_V1_BYTES];
	uint32 examined = 0;
	int calls;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &holder_assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
		&holder_assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000),
		(UINT64_C(1000)) - (UINT64_C(100)), UINT64_C(100), UINT64_C(50), false, 0, &bootstrap,
		&terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	bootstrap_ack = make_resource_x_bootstrap_ack_values(
		&bootstrap, UINT64_C(1), UINT32_C(71));
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&bootstrap_ack, 0, 61, 77, UINT64_C(110), &bootstrap_assert);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	make_resource_x_remote_join_pair(
		tag, 1, &holder_grant, &holder_image);
	retarget_resource_x_remote_join_pair(
		&holder_grant, &holder_image, UINT64_C(3), UINT64_C(1));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&holder_image, 0, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&holder_grant, 0, &join), RESOURCE_X_APPLY_APPLIED);
	holder_ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&holder_ref),
		RESOURCE_X_APPLY_APPLIED);
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 60;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(
		&holder_ref, &install), RESOURCE_X_APPLY_APPLIED);
	memset(&activation, 0, sizeof(activation));
	activation.ownership_generation = install.ownership_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(
		&holder_ref, &activation), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_round_publish_terminal_exact(
			&holder_ref, 31, 77, 60, 3, UINT64_C(115)),
		RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&holder_ref, 31, 77, 60));

	block = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCK_TO_N, tag, 2, 1);
	block.common.base_authority_generation = 3;
	block.common.authority_generation = 3;
	block.common.ordered_lane = 0;
	block.common.observed_mode = PCM_STATE_X;
	block.common.target_mode = PCM_STATE_N;
	block.common.source_candidate = 1;
	block.common.retain_pi_if_dirty = 1;
	make_resource_x_remote_join_pair(tag, 2, &grant, &image);
	retarget_resource_x_remote_join_pair(&grant, &image, 5, 41);
	image.common.ordered_lane = 0;
	status = make_resource_x_remote_blocked_frame(tag, 2, 1);
	status.common.base_authority_generation = 3;
	status.common.authority_generation = 3;
	status.common.ordered_lane = 0;
	status.body.blocked_to_n.source_proof_crc32c
		= image.common.semantic_crc32c;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_source_exact(
		&block, 0, &status, &image), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_holder_pair_retained_fence_exact(
		&tag, 0, block.common.master_session_incarnation, 17,
		image.body.image_envelope.source_carrier_generation));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_publish_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		0, block.common.master_session_incarnation),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
		&block.common.logical_assertion, &status_intent, status_payload,
		sizeof(status_payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_intent_snapshot_exact(
		&block.common.logical_assertion, &image_intent, image_payload,
		sizeof(image_payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&status_intent, 105), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&status_intent));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&image_intent, 106), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&image_intent));
	for (calls = 0; calls < 12; calls++) {
		probe_result = cluster_pcm_lock_resource_x_outbound_intent_probe_exact(
			1, &requester_settlement_intent,
			requester_settlement_payload,
			sizeof(requester_settlement_payload), &examined);
		UT_ASSERT(probe_result == RESOURCE_X_INTENT_PROBE_FOUND
			|| probe_result == RESOURCE_X_INTENT_PROBE_MORE
			|| probe_result == RESOURCE_X_INTENT_PROBE_COMPLETE);
		if (probe_result == RESOURCE_X_INTENT_PROBE_FOUND
			&& requester_settlement_intent.body.owner_kind
				== RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT)
			break;
	}
	UT_ASSERT_EQ(probe_result, RESOURCE_X_INTENT_PROBE_FOUND);
	UT_ASSERT_EQ(requester_settlement_intent.body.owner_kind,
		RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&requester_settlement_intent, 107), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&requester_settlement_intent));
	UT_ASSERT(cluster_pcm_lock_resource_x_holder_pair_retained_fence_exact(
		&tag, 0, block.common.master_session_incarnation, 17,
		image.body.image_envelope.source_carrier_generation));
	UT_ASSERT(
		!cluster_pcm_lock_resource_x_holder_pair_retained_fence_exact(
			&tag, 0, block.common.master_session_incarnation + 1, 17,
			image.body.image_envelope.source_carrier_generation));
	UT_ASSERT(
		!cluster_pcm_lock_resource_x_holder_pair_retained_fence_exact(
			&tag, 0, block.common.master_session_incarnation, 17,
			image.body.image_envelope.source_carrier_generation + 1));
	cluster_pcm_grd_protocol_debt_snapshot(&debt);
	UT_ASSERT_EQ(debt.retained_entry_count, UINT64_C(1));

	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_BLOCKED_TO_N, status_payload,
		status_intent.payload_bytes, &settlement, &reject));
	settlement.kind = RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2;
	settlement.common.observed_mode = PCM_STATE_N;
	settlement.common.target_mode = PCM_STATE_N;
	settlement.common.source_candidate = 1;
	settlement.common.retain_pi_if_dirty = 1;
	settlement.common.authority_generation = 5;
	settlement.common.sender_connection_generation = 71;
	settlement.common.outcome = RESOURCE_X_OUTCOME_NONE;
	settlement.common.flags = 0;
	stale = settlement;
	stale.body.blocked_to_n.page_scn_lsn++;
	memset(&plan, 0x5a, sizeof(plan));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
		&stale, 0, &plan), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(!plan.valid);
	UT_ASSERT_EQ(plan.source_generation, UINT64_C(0));
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_source_settlement_prepare_observed_exact(
			&settlement, 0, &plan, &prepare_observation),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(plan.valid);
	UT_ASSERT_EQ(prepare_observation.commit_stage,
		RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_TERMINAL_COVER);
	UT_ASSERT_EQ(prepare_observation.mismatch_mask, UINT32_C(0));
	UT_ASSERT_EQ(prepare_observation.current_round_phase, UINT8_C(4));
	UT_ASSERT_EQ(prepare_observation.current_owner_state, UINT8_C(0));
	UT_ASSERT_EQ(prepare_observation.current_terminal_cover_present,
		UINT8_C(1));
	UT_ASSERT_EQ(prepare_observation.current_holder_status_valid, UINT8_C(2));
	UT_ASSERT_EQ(prepare_observation.current_holder_image_valid, UINT8_C(2));
	UT_ASSERT_EQ(prepare_observation.current_status_intent_state, UINT8_C(0));
	UT_ASSERT_EQ(prepare_observation.current_image_intent_state, UINT8_C(0));
	UT_ASSERT_EQ(prepare_observation.current_pair_source_generation,
		UINT64_C(60));
	UT_ASSERT_EQ(prepare_observation.current_pair_carrier_generation,
		UINT64_C(61));
	UT_ASSERT_EQ(prepare_observation.current_pair_observed_mode,
		(uint8)PCM_STATE_X);
	UT_ASSERT_EQ(plan.source_mode, (uint8)PCM_STATE_X);
	UT_ASSERT_EQ(plan.cover_action,
		(uint8)RESOURCE_X_SETTLEMENT_COVER_CLOSE_EXACT_X);
	UT_ASSERT_EQ(plan.source_generation, UINT64_C(60));
	UT_ASSERT_EQ(plan.carrier_generation, UINT64_C(61));
	UT_ASSERT_EQ(plan.terminal_cached_generation, UINT64_C(60));
	stale_plan = plan;
	stale_plan.source_generation++;
	stale_plan.carrier_generation++;
	stale_plan.terminal_cached_generation++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_commit_exact(
		&settlement, 0, &stale_plan, &commit_observation),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(commit_observation.commit_stage,
		RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_TERMINAL_COVER);
	UT_ASSERT_EQ(commit_observation.current_pair_observed_mode,
		(uint8)PCM_STATE_X);
	UT_ASSERT((commit_observation.mismatch_mask
		& RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_SOURCE_GENERATION) != 0);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&holder_ref, 31, 77, 60));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_commit_exact(
		&settlement, 0, &plan, &commit_observation),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(commit_observation.commit_stage,
		RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_TERMINAL_COVER);
	UT_ASSERT_EQ(commit_observation.mismatch_mask, UINT32_C(0));
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&holder_ref, 31, 77, 60));
	/* The exact drained tombstone makes the still-retained pair bounded
	 * duplicate/recovery history, not unfinished protocol debt.  Keep the
	 * bytes for an exact kind-10 replay while exposing zero terminal debt. */
	cluster_pcm_grd_protocol_debt_snapshot(&debt);
	UT_ASSERT_EQ(debt.retained_entry_count, UINT64_C(0));
	UT_ASSERT_EQ(debt.invalid_entry_count, UINT64_C(0));
	UT_ASSERT(
		!cluster_pcm_lock_resource_x_holder_pair_retained_fence_exact(
			&tag, 0, block.common.master_session_incarnation, 17,
			image.body.image_envelope.source_carrier_generation));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_ack_build_exact(
		&settlement, 64, &ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ack.kind, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	UT_ASSERT_EQ(ack.common.outcome, RESOURCE_X_OUTCOME_OK);
	UT_ASSERT_EQ(ack.common.sender_connection_generation, UINT32_C(64));
	memset(&plan, 0x5a, sizeof(plan));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
		&settlement, 0, &plan), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(!plan.valid);
	UT_ASSERT_EQ(plan.source_generation, UINT64_C(0));
}

UT_TEST(test_resource_x_source_settlement_debt_participates_in_same_token_r8_r10)
{
	char *source = read_text_file(PCM_LOCK_SOURCE_PATH);
	char *decoder;
	char *proof;
	char *proof_end;
	char *sweep;
	char *sweep_end;
	char *clean;
	char *clean_end;

	decoder = strstr(source,
		"\npcm_resource_x_source_settlement_debt_decode_locked(");
	proof = strstr(source,
		"\npcm_resource_x_reconfig_proof_slot_locked(");
	proof_end = proof != NULL ? strstr(proof,
		"\nstatic bool\npcm_resource_x_zero_proof_phase_exact(") : NULL;
	sweep = strstr(source, "\ncluster_resource_x_reconfig_sweep(");
	sweep_end = sweep != NULL ? strstr(sweep,
		"\nbool\ncluster_resource_x_reconfig_zero_proof_exact(") : NULL;
	clean = strstr(source, "\npcm_resource_x_terminal_state_locked(");
	clean_end = clean != NULL ? strstr(clean,
		"\nstatic bool\npcm_resource_x_clean_state_locked(") : NULL;

	UT_ASSERT_NOT_NULL(decoder);
	UT_ASSERT_NOT_NULL(proof);
	UT_ASSERT_NOT_NULL(proof_end);
	UT_ASSERT_NOT_NULL(sweep);
	UT_ASSERT_NOT_NULL(sweep_end);
	UT_ASSERT_NOT_NULL(clean);
	UT_ASSERT_NOT_NULL(clean_end);
	UT_ASSERT(strstr(proof,
		"pcm_resource_x_source_settlement_debt_decode_locked(") < proof_end);
	UT_ASSERT(strstr(proof,
		"RESOURCE_X_SOURCE_SETTLEMENT_PENDING") < proof_end);
	UT_ASSERT(strstr(proof,
		"RESOURCE_X_SOURCE_SETTLEMENT_ACKED") < proof_end);
	UT_ASSERT(strstr(sweep,
		"pcm_resource_x_source_settlement_debt_decode_locked(") < sweep_end);
	UT_ASSERT(strstr(sweep,
		"memset(&master_state->source_settlement") < sweep_end);
	UT_ASSERT(strstr(clean,
		"pcm_resource_x_source_settlement_debt_decode_locked(") < clean_end);
	UT_ASSERT(strstr(clean,
		"RESOURCE_X_SOURCE_SETTLEMENT_PENDING") < clean_end);
	UT_ASSERT(strstr(clean,
		"RESOURCE_X_SOURCE_SETTLEMENT_ACKED") < clean_end);
	free(source);
}

UT_TEST(test_resource_x_holder_pair_drain_allows_master_requester_colocation)
{
	BufferTag tag = make_tag(159);
	ResourceXDecodedFrame block;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame status;
	ResourceXIntentSlot image_intent;
	ResourceXIntentSlot status_intent;
	uint8 image_payload[RESOURCE_X_IMAGE_V1_BYTES];
	uint8 status_payload[RESOURCE_X_PROOF_V1_BYTES];
	uint64 source_generation = 0;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	fake_gcs_master_node = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	block = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCK_TO_N, tag, 1, 0);
	block.common.observed_mode = PCM_STATE_X;
	block.common.target_mode = PCM_STATE_N;
	block.common.source_candidate = 1;
	block.common.retain_pi_if_dirty = 1;
	make_resource_x_remote_join_pair(tag, 1, &grant, &image);
	status = make_resource_x_remote_blocked_frame(tag, 1, 0);
	status.body.blocked_to_n.source_proof_crc32c
		= image.common.semantic_crc32c;

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_source_exact(
		&block, 1, &status, &image), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_publish_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		1, block.common.master_session_incarnation), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
		&block.common.logical_assertion, &status_intent, status_payload,
		sizeof(status_payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_intent_snapshot_exact(
		&block.common.logical_assertion, &image_intent, image_payload,
		sizeof(image_payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(status_intent.destination_node, UINT32_C(1));
	UT_ASSERT_EQ(image_intent.destination_node, UINT32_C(1));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&status_intent, 105), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&status_intent));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&image_intent, 106), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
		&image_intent));

	/* Master and requester are independent authenticated roles, but a local
	 * master request legitimately assigns both to node 1.  Colocation must
	 * not invalidate an otherwise exact retained pair. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_drain_prepare_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		1, block.common.master_session_incarnation, &source_generation),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(source_generation, UINT64_C(60));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_drain_commit_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		1, block.common.master_session_incarnation, source_generation),
		RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_resource_x_remote_master_uses_exact_installed_holder_lineage)
{
	BufferTag tag = make_tag(174);
	ResourceXAcquisitionRef installed_ref;
	ResourceXBufferActivationProof activation;
	ResourceXDecodedFrame block;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame status;
	ResourceXRequesterJoinSnapshot join;
	ResourceXExecutorSnapshot executor;
	ResourceXIntentSlot image_intent;
	ResourceXIntentSlot status_intent;
	uint8 image_payload[RESOURCE_X_IMAGE_V1_BYTES];
	uint8 status_payload[RESOURCE_X_PROOF_V1_BYTES];

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	fake_gcs_master_node = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);

	/* First install the exact current X carrier from authenticated master 1.
	 * The requester terminal deliberately leaves this non-master node's GRD
	 * mirror at N; its existing requester base records final authority 3. */
	make_resource_x_remote_join_pair(tag, 0, &grant, &image);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&image, 2, &join), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(
		&grant, 1, &join), RESOURCE_X_APPLY_APPLIED);
	complete_resource_x_remote_requester_terminal(
		&join, &installed_ref, &activation);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_executor_probe_exact(
		&installed_ref, &executor), RESOURCE_X_EXECUTOR_COMPLETE);
	UT_ASSERT_EQ(executor.requester_base_generation, 3);
	UT_ASSERT_EQ(cluster_pcm_lock_query(tag), PCM_LOCK_MODE_N);

	/* A successor's authenticated type-17 may have a later canonical base:
	 * master-only serialization can advance while this node remains the same
	 * physical X carrier.  The installed final authority is therefore a
	 * monotonic lineage floor, not an equality with the current type-17 base. */
	block = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCK_TO_N, tag, 3, 0);
	block.common.base_authority_generation = 6;
	block.common.assertion_sequence = 43;
	block.common.observed_mode = PCM_STATE_X;
	block.common.target_mode = PCM_STATE_N;
	block.common.source_candidate = 1;
	block.common.retain_pi_if_dirty = 1;
	make_resource_x_remote_join_pair(tag, 3, &grant, &image);
	retarget_resource_x_remote_join_pair(&grant, &image, 8, 43);

	memset(&status, 0, sizeof(status));
	status.kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
	status.payload_bytes = RESOURCE_X_PROOF_V1_BYTES;
	status.blocked_has_remote_proof = true;
	status.common = block.common;
	status.common.source_candidate = 0;
	status.common.retain_pi_if_dirty = 0;
	status.common.outcome = RESOURCE_X_OUTCOME_OK;
	status.common.flags = RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED;
	status.common.authority_generation = 6;
	memcpy(status.body.blocked_to_n.source_fence,
		image.body.image_envelope.source_fence,
		sizeof(status.body.blocked_to_n.source_fence));
	status.body.blocked_to_n.source_carrier_generation
		= image.body.image_envelope.source_carrier_generation;
	status.body.blocked_to_n.requester_target_generation = 43;
	status.body.blocked_to_n.page_scn_lsn
		= image.body.image_envelope.page_scn_lsn;
	status.body.blocked_to_n.dependency_count
		= image.body.image_envelope.dependency_count;
	memcpy(status.body.blocked_to_n.dependencies,
		image.body.image_envelope.dependencies,
		sizeof(status.body.blocked_to_n.dependencies));
	status.body.blocked_to_n.source_proof_crc32c
		= image.common.semantic_crc32c;
	status.body.blocked_to_n.page_checksum
		= image.body.image_envelope.page_checksum;
	status.body.blocked_to_n.source_disposition
		= RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
	status.body.blocked_to_n.proof_kind
		= RESOURCE_X_PROOF_REMOTE_CARRIER;
	status.body.blocked_to_n.holder_connection_generation = 51;
	status.body.blocked_to_n.acting_formation = 17;

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_source_exact(
		&block, 1, &status, &image), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_query(tag), PCM_LOCK_MODE_N);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_publish_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		1, block.common.master_session_incarnation), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
		&block.common.logical_assertion, &status_intent, status_payload,
		sizeof(status_payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_intent_snapshot_exact(
		&block.common.logical_assertion, &image_intent, image_payload,
		sizeof(image_payload)), RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_resource_x_remote_master_retains_exact_current_x_without_local_authority)
{
	BufferTag tag = make_tag(175);
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef entry_ref;
	PcmRetireRefusal why;
	PcmAuthoritySnapshot authority;
	ResourceXDecodedFrame block;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame status;
	ResourceXIntentSlot image_intent;
	ResourceXIntentSlot status_intent;
	uint8 image_payload[RESOURCE_X_IMAGE_V1_BYTES];
	uint8 status_payload[RESOURCE_X_PROOF_V1_BYTES];
	uint64 binding_generation;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	fake_gcs_master_node = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);

	/* The GCS caller has already frozen and revalidated this node's exact
	 * BufferDesc X carrier.  A remote authoritative master may therefore ask
	 * this node to retain the indivisible type-18/type-15 pair even when this
	 * process has no local authority entry for the page. */
	block = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCK_TO_N, tag, 3, 0);
	block.common.observed_mode = PCM_STATE_X;
	block.common.target_mode = PCM_STATE_N;
	block.common.source_candidate = 1;
	block.common.retain_pi_if_dirty = 1;
	make_resource_x_remote_join_pair(tag, 3, &grant, &image);
	status = make_resource_x_remote_blocked_frame(tag, 3, 0);
	status.body.blocked_to_n.source_proof_crc32c
		= image.common.semantic_crc32c;

	UT_ASSERT_EQ(cluster_pcm_lock_query(tag), PCM_LOCK_MODE_N);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_source_exact(
		&block, 1, &status, &image), RESOURCE_X_APPLY_APPLIED);

	/* Creating retained-pair storage must not mint a local GRD owner. */
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(authority.state, PCM_STATE_N);
	UT_ASSERT_EQ(authority.x_holder_node, -1);
	UT_ASSERT_EQ(authority.s_holders_bitmap, UINT32_C(0));
	UT_ASSERT_EQ(authority.transition_count, UINT64_C(0));
	UT_ASSERT(pcm_entry_ref_acquire(
		&tag, false, &entry_ref, &acquire_result));
	binding_generation = entry_ref.binding_generation;
	pcm_entry_ref_release(&entry_ref);
	UT_ASSERT(!pcm_entry_retire_classify_exact(
		&tag, binding_generation, &why));
	UT_ASSERT_EQ(why, PCM_RETIRE_REFUSAL_RETAINED_PAIR_PRESENT);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_publish_exact(
		&block.common.logical_assertion, block.common.assertion_sequence,
		1, block.common.master_session_incarnation), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
		&block.common.logical_assertion, &status_intent, status_payload,
		sizeof(status_payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_intent_snapshot_exact(
		&block.common.logical_assertion, &image_intent, image_payload,
		sizeof(image_payload)), RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_resource_x_prepared_retain_uses_terminal_x_not_local_reuse_order)
{
	BufferTag tag = make_tag(176);
	PcmAuthoritySnapshot authority;
	ResourceXAcquisitionRef cover_ref;
	ResourceXAssertion adapter_assertion;
	ResourceXDecodedFrame block;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame status;
	ResourceXLocalOwnerHandle owner;
	ResourceXLocalOwnerHandle stale_owner;
	ResourceXTerminalXLineage lineage;
	ClusterPcmOwnSnapshot revoking;
	ClusterPcmOwnSnapshot stale_revoking;
	int cycle;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);

	/* Reproduce the bounded-slot history from t430: this local entry's
	 * administrative transition floor is already newer than the remote
	 * master's clean resource base before the exact current-X install. */
	for (cycle = 0; cycle < 4; cycle++) {
		UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
			tag, PCM_TRANS_N_TO_X, 1), PCM_GCS_TRANSITION_APPLIED);
		UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
			tag, PCM_TRANS_X_TO_N_RELEASE, 1),
			PCM_GCS_TRANSITION_APPLIED);
	}
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT(authority.transition_count > UINT64_C(3));
	UT_ASSERT_EQ(authority.state, PCM_STATE_N);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &adapter_assertion));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_base_bind_exact(
		&adapter_assertion, 17, authority.transition_count, &authority),
		RESOURCE_X_APPLY_APPLIED);

	cover_ref = setup_resource_x_test_terminal_cover(tag, UINT64_C(91));
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&cover_ref, 31, 77, 91));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT(authority.transition_count > UINT64_C(3));
	UT_ASSERT_EQ(authority.state, PCM_STATE_N);
	UT_ASSERT_EQ(authority.x_holder_node, -1);
	UT_ASSERT_EQ(authority.s_holders_bitmap, UINT32_C(0));

	block = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCK_TO_N, tag, 2, 1);
	block.payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	block.common.base_authority_generation = UINT64_C(3);
	block.common.authority_generation = UINT64_C(3);
	block.common.assertion_sequence = UINT64_C(42);
	block.common.ordered_lane = 0;
	block.common.observed_mode = (uint8)PCM_STATE_X;
	block.common.target_mode = (uint8)PCM_STATE_N;
	block.common.source_candidate = 1;
	block.common.retain_pi_if_dirty = 1;

	memset(&lineage, 0, sizeof(lineage));
	memset(&owner, 0, sizeof(owner));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&block, 0, 77, 91, 12, 9, UINT64_C(130),
		&lineage, &owner), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_terminal_x_revoke_revalidate_held_exact(
		&block, 0, 77, 91, &owner, &lineage));

	make_resource_x_remote_join_pair(tag, 2, &grant, &image);
	retarget_resource_x_remote_join_pair(
		&grant, &image, UINT64_C(5), UINT64_C(42));
	image.common.ordered_lane = 0;
	set_resource_x_test_source_fence(
		image.body.image_envelope.source_fence, UINT64_C(91), PCM_STATE_X);
	image.body.image_envelope.source_carrier_generation = UINT64_C(92);
	canonicalize_resource_x_test_image(&image);
	status = make_resource_x_remote_blocked_frame(tag, 2, 1);
	status.common = block.common;
	status.common.source_candidate = 0;
	status.common.retain_pi_if_dirty = 0;
	status.common.outcome = RESOURCE_X_OUTCOME_OK;
	status.common.flags = RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED;
	memcpy(status.body.blocked_to_n.source_fence,
		image.body.image_envelope.source_fence,
		sizeof(status.body.blocked_to_n.source_fence));
	status.body.blocked_to_n.source_carrier_generation
		= image.body.image_envelope.source_carrier_generation;
	status.body.blocked_to_n.requester_target_generation = UINT64_C(42);
	status.body.blocked_to_n.page_scn_lsn
		= image.body.image_envelope.page_scn_lsn;
	status.body.blocked_to_n.dependency_count
		= image.body.image_envelope.dependency_count;
	memcpy(status.body.blocked_to_n.dependencies,
		image.body.image_envelope.dependencies,
		sizeof(status.body.blocked_to_n.dependencies));
	status.body.blocked_to_n.source_proof_crc32c
		= image.common.semantic_crc32c;
	status.body.blocked_to_n.page_checksum
		= image.body.image_envelope.page_checksum;
	status.body.blocked_to_n.source_disposition
		= RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
	status.body.blocked_to_n.proof_kind = RESOURCE_X_PROOF_REMOTE_CARRIER;
	status.body.blocked_to_n.holder_connection_generation = 51;
	status.body.blocked_to_n.acting_formation = 17;

	memset(&revoking, 0, sizeof(revoking));
	revoking.tag = tag;
	revoking.generation = UINT64_C(91);
	revoking.reservation_token = owner.reservation_token + UINT64_C(1);
	revoking.flags = PCM_OWN_FLAG_REVOKING;
	revoking.pcm_state = (uint8)PCM_STATE_X;

	/* A bare local mirror remains insufficient.  The new path succeeds only
	 * with the exact terminal-X owner and the BufferDesc revoke snapshot. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_source_exact(
		&block, 0, &status, &image), RESOURCE_X_APPLY_BAD_STATE);
	stale_owner = owner;
	stale_owner.owner_procno++;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_block_to_n_prepared_x_source_exact(
			&block, 0, &status, &image, &revoking, &stale_owner),
		RESOURCE_X_APPLY_STALE);
	stale_revoking = revoking;
	stale_revoking.generation++;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_block_to_n_prepared_x_source_exact(
			&block, 0, &status, &image, &stale_revoking, &owner),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_drop_x_source_exact(
		&block, 0, &status, &image, &revoking, &owner),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_block_to_n_prepared_x_source_exact(
			&block, 0, &status, &image, &revoking, &owner),
		RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_resource_x_self_master_drop_keeps_local_grd_authority_domain)
{
	BufferTag tag = make_tag(177);
	PcmAuthoritySnapshot authority;
	ResourceXAcquisitionRef expected_ref;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXAssertion holder_assertion;
	ResourceXBootstrapRoundAction action;
	ResourceXBufferInstallProof install;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assert_frame;
	ResourceXDecodedFrame block;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame settlement;
	ResourceXDecodedFrame status;
	ResourceXDecodedFrame unused_dispatch;
	ResourceXDurableProof durable;
	ResourceXLocalOwnerHandle owner;
	ResourceXMasterSnapshot snapshot;
	ResourceXTerminalXLineage lineage;
	ClusterPcmOwnSnapshot revoking;
	uint64 base_generation;

	reset_fake_pcm_runtime(4);
	tag.forkNum = FSM_FORKNUM;
	cluster_node_id = 0;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 0, &holder_assertion));
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&holder_assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000),
		(UINT64_C(1000)) - (UINT64_C(100)), UINT64_C(100), UINT64_C(50), 7, 5, false, 0, &request,
		&terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&request, 0, 61, 77, 31, 71, &ack),
		RESOURCE_X_APPLY_APPLIED);
	action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, UINT64_C(110), &assert_frame);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&assert_frame, 0, 61, 77, 31, 71, &snapshot),
		RESOURCE_X_APPLY_APPLIED);

	memset(&durable, 0, sizeof(durable));
	durable.assertion = holder_assertion;
	durable.base_authority_generation
		= assert_frame.common.base_authority_generation;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = assert_frame.common.assertion_sequence;
	durable.requester_target_generation
		= assert_frame.common.assertion_sequence;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x12345678);
	durable.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(
		&durable, &snapshot), RESOURCE_X_APPLY_APPLIED);

	expected_ref = make_resource_x_acquisition_ref(
		tag, 0, 17, assert_frame.common.assertion_sequence);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&expected_ref),
		RESOURCE_X_APPLY_APPLIED);
	memset(&install, 0, sizeof(install));
	install.ownership_generation = 8;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation
		= assert_frame.common.assertion_sequence;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(
		&expected_ref, &install), RESOURCE_X_APPLY_APPLIED);

	settlement = assert_frame;
	settlement.kind = RESOURCE_X_WIRE_INSTALL_SETTLEMENT;
	memset(&settlement.body, 0, sizeof(settlement.body));
	settlement.common.ordered_lane = 0;
	settlement.common.outcome = RESOURCE_X_OUTCOME_OK;
	settlement.common.authority_generation
		= snapshot.final_authority_generation;
	settlement.body.install_settlement.conversion_base_generation
		= assert_frame.common.base_authority_generation;
	settlement.body.install_settlement.final_authority_generation
		= snapshot.final_authority_generation;
	settlement.body.install_settlement.requester_connection_generation = 91;
	settlement.body.install_settlement.requester_target_generation
		= assert_frame.common.assertion_sequence;
	settlement.body.install_settlement.page_scn_lsn = 82;
	settlement.body.install_settlement.page_checksum
		= UINT32_C(0x12345678);
	settlement.body.install_settlement.source_proof_crc32c
		= UINT32_C(0x87654321);
	settlement.body.install_settlement.installed_mode = PCM_STATE_X;
	settlement.body.install_settlement.requester_role
		= RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
	settlement.body.install_settlement.terminal_outcome
		= RESOURCE_X_OUTCOME_OK;
	settlement.body.install_settlement.terminal_state
		= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
		&settlement, 0, &snapshot), RESOURCE_X_APPLY_APPLIED);
	base_generation = snapshot.final_authority_generation;
	action = cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
		&holder_assertion, 0, 17, 31, 77, 51, 61, UINT64_C(1000),
		(UINT64_C(1000)) - (UINT64_C(115)), UINT64_C(115), UINT64_C(50), 7, 5, true, 8,
		&unused_dispatch, &terminal_ref);
	UT_ASSERT_EQ(action, RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&expected_ref, 31, 77, 8));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(authority.state, PCM_STATE_X);
	UT_ASSERT_EQ(authority.x_holder_node, 0);

	block = make_resource_x_master_frame(
		RESOURCE_X_WIRE_BLOCK_TO_N, tag, 3, 0);
	block.payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	block.common.base_authority_generation = base_generation;
	block.common.authority_generation = base_generation;
	block.common.assertion_sequence = UINT64_C(42);
	block.common.ordered_lane = 0;
	block.common.observed_mode = (uint8)PCM_STATE_X;
	block.common.target_mode = (uint8)PCM_STATE_N;
	block.common.source_candidate = 1;
	block.common.retain_pi_if_dirty = 1;
	memset(&lineage, 0, sizeof(lineage));
	memset(&owner, 0, sizeof(owner));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
		&block, 0, 77, 8, 13, 9, UINT64_C(130),
		&lineage, &owner), RESOURCE_X_APPLY_APPLIED);

	make_resource_x_remote_join_pair(tag, 3, &grant, &image);
	retarget_resource_x_remote_join_pair(
		&grant, &image, base_generation + UINT64_C(2),
		block.common.assertion_sequence);
	image.common.ordered_lane = 0;
	set_resource_x_test_source_fence(
		image.body.image_envelope.source_fence, UINT64_C(8), PCM_STATE_X);
	image.body.image_envelope.source_carrier_generation = UINT64_C(9);
	canonicalize_resource_x_test_image(&image);
	status = make_resource_x_remote_blocked_frame(tag, 3, 0);
	status.common = block.common;
	status.common.source_candidate = 0;
	status.common.retain_pi_if_dirty = 0;
	status.common.outcome = RESOURCE_X_OUTCOME_OK;
	status.common.flags = RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED;
	memcpy(status.body.blocked_to_n.source_fence,
		image.body.image_envelope.source_fence,
		sizeof(status.body.blocked_to_n.source_fence));
	status.body.blocked_to_n.source_carrier_generation
		= image.body.image_envelope.source_carrier_generation;
	status.body.blocked_to_n.requester_target_generation
		= block.common.assertion_sequence;
	status.body.blocked_to_n.page_scn_lsn
		= image.body.image_envelope.page_scn_lsn;
	status.body.blocked_to_n.dependency_count
		= image.body.image_envelope.dependency_count;
	memcpy(status.body.blocked_to_n.dependencies,
		image.body.image_envelope.dependencies,
		sizeof(status.body.blocked_to_n.dependencies));
	status.body.blocked_to_n.source_proof_crc32c
		= image.common.semantic_crc32c;
	status.body.blocked_to_n.page_checksum
		= image.body.image_envelope.page_checksum;
	status.body.blocked_to_n.source_disposition
		= RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
	status.body.blocked_to_n.proof_kind = RESOURCE_X_PROOF_REMOTE_CARRIER;
	status.body.blocked_to_n.holder_connection_generation = 51;
	status.body.blocked_to_n.acting_formation = 17;
	memset(&revoking, 0, sizeof(revoking));
	revoking.tag = tag;
	revoking.generation = 8;
	revoking.reservation_token = owner.reservation_token + UINT64_C(1);
	revoking.flags = PCM_OWN_FLAG_REVOKING;
	revoking.pcm_state = (uint8)PCM_STATE_X;

	/* The local GRD is the authority for a self-master DROP.  The remote-only
	 * prepared-X provenance class must not veto this exact existing path. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_drop_x_source_exact(
		&block, 0, &status, &image, &revoking, &owner),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_query(tag), PCM_LOCK_MODE_X);
}

UT_TEST(test_resource_x_master_local_and_durable_proofs_are_exact_and_closed)
{
	BufferTag local_tag = make_tag(144);
	BufferTag durable_tag = make_tag(145);
	BufferTag pi_tag = make_tag(146);
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame decoded_grant;
	ResourceXDecodedFrame local_proof;
	ResourceXDurableProof durable_proof;
	ResourceXIntentSlot grant_intent;
	ResourceXMasterSnapshot snapshot;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 grant_bytes[RESOURCE_X_PROOF_V1_BYTES];

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	assertion = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X,
										  local_tag, 1, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&assertion, 1,
														&snapshot),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_PROOF);

	local_proof = make_resource_x_master_frame(
		RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION, local_tag, 1, 1);
	local_proof.common.observed_mode = PCM_STATE_S;
	local_proof.common.outcome = RESOURCE_X_OUTCOME_OK;
	local_proof.body.local_proof.local_holder_authority_generation = 71;
	local_proof.body.local_proof.requester_target_generation = 41;
	local_proof.body.local_proof.dependency_count = 2;
	local_proof.body.local_proof.dependency_vector_crc32c
		= UINT32_C(0x11223344);
	local_proof.body.local_proof.page_checksum = UINT32_C(0x55667788);
	local_proof.body.local_proof.local_image_proof_crc32c
		= UINT32_C(0x99aabbcc);
	local_proof.body.local_proof.requester_connection_generation = 73;
	local_proof.body.local_proof.local_proof_generation = 74;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_local_proof_exact(&local_proof,
		1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
	UT_ASSERT_EQ(snapshot.proof_kind, RESOURCE_X_PROOF_LOCAL_IMAGE);
	UT_ASSERT_EQ(snapshot.source_disposition,
				 RESOURCE_X_DISPOSITION_LOCAL_IMAGE);
	UT_ASSERT_EQ(snapshot.requester_target_generation, 41);
	UT_ASSERT_EQ(snapshot.final_authority_generation, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_local_proof_exact(&local_proof,
		1, &snapshot), RESOURCE_X_APPLY_DUPLICATE);

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	assertion = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X,
										  durable_tag, 2, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&assertion, 2,
														&snapshot),
				 RESOURCE_X_APPLY_APPLIED);
	memset(&durable_proof, 0, sizeof(durable_proof));
	durable_proof.assertion = assertion.common.logical_assertion;
	durable_proof.base_authority_generation = 1;
	durable_proof.resource_formation = 17;
	durable_proof.master_session_incarnation = 31;
	durable_proof.assertion_sequence = 41;
	durable_proof.requester_target_generation = 41;
	durable_proof.page_scn_lsn = 82;
	durable_proof.page_checksum = UINT32_C(0x12345678);
	durable_proof.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(&durable_proof,
		&snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
	UT_ASSERT_EQ(snapshot.proof_kind, RESOURCE_X_PROOF_DURABLE_STORAGE);
	UT_ASSERT_EQ(snapshot.source_disposition,
				 RESOURCE_X_DISPOSITION_DURABLE_STORAGE);
	UT_ASSERT_EQ(snapshot.requester_target_generation, 41);
	UT_ASSERT_EQ(snapshot.final_authority_generation, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
		&assertion.common.logical_assertion, &grant_intent, grant_bytes,
		sizeof(grant_bytes)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(grant_intent.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(grant_intent.logical_generation, 41);
	UT_ASSERT_EQ(grant_intent.authority_generation, 2);
	UT_ASSERT_EQ(grant_intent.destination_node, 2);
	UT_ASSERT_EQ(grant_intent.payload_bytes, RESOURCE_X_PROOF_V1_BYTES);
	UT_ASSERT_EQ(grant_intent.kind, RESOURCE_X_WIRE_AUTHORITY_GRANT);
	UT_ASSERT_EQ(grant_intent.body.owner_kind,
		RESOURCE_X_INTENT_OWNER_MASTER_GRANT);
	UT_ASSERT(resource_x_assertion_equal(&grant_intent.body.assertion,
		&assertion.common.logical_assertion));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		grant_bytes, sizeof(grant_bytes), &decoded_grant, &reject));
	UT_ASSERT_EQ(decoded_grant.kind, RESOURCE_X_WIRE_AUTHORITY_GRANT);
	UT_ASSERT_EQ(decoded_grant.common.authority_generation, 2);
	UT_ASSERT_EQ(decoded_grant.body.authority_grant.requester_target_generation,
		41);

	/* A retained PI makes durable storage non-authoritative. */
	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(pi_tag, PCM_LOCK_MODE_X);
	cluster_pcm_lock_downgrade(pi_tag, PCM_LOCK_MODE_N, true);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	assertion = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X,
										  pi_tag, 2, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&assertion, 2,
														&snapshot),
				 RESOURCE_X_APPLY_APPLIED);
	durable_proof.assertion = assertion.common.logical_assertion;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(&durable_proof,
		&snapshot), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_PROOF);
	UT_ASSERT_EQ(snapshot.proof_kind, 0);
}

UT_TEST(test_resource_x_master_multi_s_blockers_advance_exact_frontier)
{
	BufferTag tag = make_tag(167);
	PcmAuthoritySnapshot authority;
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame blocked;
	ResourceXDecodedFrame local_proof;
	ResourceXMasterSnapshot snapshot;
	const int32 blocker_nodes[] = {0, 2, 3};
	uint64 base_authority_generation;
	int node;

	reset_fake_pcm_runtime(4);
	for (node = 0; node < 4; node++) {
		cluster_node_id = node;
		cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	}
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(authority.transition_count, UINT64_C(1));
	UT_ASSERT_EQ(authority.s_holders_bitmap, UINT32_C(0xf));
	base_authority_generation = authority.transition_count;

	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 1, 1);
	assertion.common.base_authority_generation = base_authority_generation;
	assertion.common.authority_generation = base_authority_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_base_bind_exact(
		&assertion.common.logical_assertion, 17,
		base_authority_generation, &authority), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(snapshot.incompatible_holders_bitmap, UINT32_C(0xd));

	for (node = 0; node < lengthof(blocker_nodes); node++) {
		blocked = make_resource_x_master_frame(
			RESOURCE_X_WIRE_BLOCKED_TO_N, tag, 1, blocker_nodes[node]);
		blocked.common.base_authority_generation = base_authority_generation;
		blocked.common.authority_generation = base_authority_generation;
		blocked.common.observed_mode = PCM_STATE_S;
		blocked.common.target_mode = PCM_STATE_N;
		blocked.common.outcome = RESOURCE_X_OUTCOME_OK;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(
			&blocked, blocker_nodes[node], &snapshot),
			RESOURCE_X_APPLY_APPLIED);
	}
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_PROOF);
	UT_ASSERT_EQ(snapshot.blocked_holders_bitmap, UINT32_C(0xd));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(authority.transition_count, UINT64_C(4));
	UT_ASSERT_EQ(authority.s_holders_bitmap, UINT32_C(1) << 1);

	local_proof = make_resource_x_master_frame(
		RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION, tag, 1, 1);
	local_proof.common.base_authority_generation = base_authority_generation;
	local_proof.common.authority_generation = base_authority_generation;
	local_proof.common.observed_mode = PCM_STATE_S;
	local_proof.common.outcome = RESOURCE_X_OUTCOME_OK;
	local_proof.body.local_proof.local_holder_authority_generation = 71;
	local_proof.body.local_proof.requester_target_generation = 41;
	local_proof.body.local_proof.page_scn_lsn = 82;
	local_proof.body.local_proof.page_checksum = UINT32_C(0x55667788);
	local_proof.body.local_proof.local_image_proof_crc32c
		= UINT32_C(0x99aabbcc);
	local_proof.body.local_proof.requester_connection_generation = 73;
	local_proof.body.local_proof.local_proof_generation = 74;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_local_proof_exact(
		&local_proof, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
	UT_ASSERT_EQ(snapshot.final_authority_generation, UINT64_C(5));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(authority.state, PCM_STATE_X);
	UT_ASSERT_EQ(authority.x_holder_node, 1);
	UT_ASSERT_EQ(authority.transition_count, UINT64_C(5));
}

UT_TEST(test_resource_x_local_settlement_reconciles_exact_requester_prestate)
{
	BufferTag s_tag = make_tag(160);
	BufferTag x_tag = make_tag(161);
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame local_proof;
	ResourceXDecodedFrame settlement;
	ResourceXMasterSnapshot snapshot;
	PcmAuthoritySnapshot authority;
	int scenario;

	for (scenario = 0; scenario < 2; scenario++) {
		BufferTag tag = scenario == 0 ? s_tag : x_tag;

		reset_fake_pcm_runtime(4);
		cluster_node_id = 1;
		cluster_pcm_lock_acquire(tag,
			scenario == 0 ? PCM_LOCK_MODE_S : PCM_LOCK_MODE_X);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
					 RESOURCE_X_APPLY_APPLIED);

		assertion = make_resource_x_master_frame(
			RESOURCE_X_WIRE_ASSERT_X, tag, 1, 1);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
			&assertion, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_PROOF);
		UT_ASSERT_EQ(snapshot.incompatible_holders_bitmap, 0);

		local_proof = make_resource_x_master_frame(
			RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION, tag, 1, 1);
		local_proof.common.observed_mode
			= scenario == 0 ? PCM_STATE_S : PCM_STATE_X;
		local_proof.common.outcome = RESOURCE_X_OUTCOME_OK;
		local_proof.body.local_proof.local_holder_authority_generation = 71;
		local_proof.body.local_proof.requester_target_generation = 41;
		local_proof.body.local_proof.page_scn_lsn = 82;
		local_proof.body.local_proof.page_checksum = UINT32_C(0x55667788);
		local_proof.body.local_proof.local_image_proof_crc32c
			= UINT32_C(0x99aabbcc);
		local_proof.body.local_proof.requester_connection_generation = 73;
		local_proof.body.local_proof.local_proof_generation = 74;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_local_proof_exact(
			&local_proof, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
		UT_ASSERT_EQ(snapshot.proof_kind, RESOURCE_X_PROOF_LOCAL_IMAGE);
		UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
		UT_ASSERT_EQ(authority.state, PCM_STATE_X);
		UT_ASSERT_EQ(authority.x_holder_node, 1);
		UT_ASSERT_EQ(authority.s_holders_bitmap, 0);
		UT_ASSERT_EQ(authority.pending_x_requester_node, -1);
		UT_ASSERT_EQ(authority.pending_x_since_lsn, 0);

		settlement = make_resource_x_master_frame(
			RESOURCE_X_WIRE_INSTALL_SETTLEMENT, tag, 1, 1);
		settlement.common.outcome = RESOURCE_X_OUTCOME_OK;
		settlement.common.authority_generation = 2;
		settlement.body.install_settlement.conversion_base_generation = 1;
		settlement.body.install_settlement.final_authority_generation = 2;
		settlement.body.install_settlement.requester_connection_generation = 73;
		settlement.body.install_settlement.requester_target_generation = 41;
		settlement.body.install_settlement.page_scn_lsn = 82;
		settlement.body.install_settlement.page_checksum
			= UINT32_C(0x55667788);
		settlement.body.install_settlement.source_proof_crc32c
			= UINT32_C(0x99aabbcc);
		settlement.body.install_settlement.installed_mode = PCM_STATE_X;
		settlement.body.install_settlement.requester_role
			= RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
		settlement.body.install_settlement.terminal_outcome
			= RESOURCE_X_OUTCOME_OK;
		settlement.body.install_settlement.terminal_state
			= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
		if (scenario == 0) {
			UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
				&settlement, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
			UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_SETTLED);
			UT_ASSERT_EQ(cluster_pcm_lock_query(tag), PCM_LOCK_MODE_X);
		} else {
			cluster_pcm_lock_downgrade(tag, PCM_LOCK_MODE_N, false);
			UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
				&settlement, 1, &snapshot), RESOURCE_X_APPLY_BAD_STATE);
			UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
			UT_ASSERT_EQ(cluster_pcm_lock_query(tag), PCM_LOCK_MODE_N);
		}
	}
}

UT_TEST(test_resource_x_master_settlement_release_starts_fifo_successor)
{
	BufferTag tag = make_tag(147);
	bool preserve_current_x = false;
	ResourceXDecodedFrame first_assert;
	ResourceXDecodedFrame next_assert;
	ResourceXDecodedFrame second_assert;
	ResourceXDecodedFrame settlement;
	ResourceXDecodedFrame release;
	ResourceXDurableProof durable;
	ResourceXIntentSlot block_intent;
	ResourceXIntentSlot grant_intent;
	ResourceXMasterSnapshot snapshot;
	uint8 block_bytes[RESOURCE_X_CONTROL_V1_BYTES];
	uint8 grant_bytes[RESOURCE_X_PROOF_V1_BYTES];

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	first_assert = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X,
											tag, 1, 1);
	second_assert = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X,
											 tag, 2, 2);
	second_assert.common.assertion_sequence = 42;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&first_assert, 1,
		&snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_PROOF);
	UT_ASSERT_EQ(snapshot.is_head, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&second_assert, 2,
		&snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_QUEUED);
	UT_ASSERT_EQ(snapshot.is_head, 0);

	memset(&durable, 0, sizeof(durable));
	durable.assertion = first_assert.common.logical_assertion;
	durable.base_authority_generation = 1;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = 41;
	durable.requester_target_generation = 41;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x12345678);
	durable.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(&durable,
		&snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.final_authority_generation, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
		&first_assert.common.logical_assertion, &grant_intent, grant_bytes,
		sizeof(grant_bytes)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_stage_exact(
		&grant_intent, 101), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_grant_intent_complete_exact(
		&grant_intent));

	settlement = make_resource_x_master_frame(
		RESOURCE_X_WIRE_INSTALL_SETTLEMENT, tag, 1, 1);
	settlement.common.outcome = RESOURCE_X_OUTCOME_OK;
	settlement.common.authority_generation = 2;
	settlement.body.install_settlement.conversion_base_generation = 1;
	settlement.body.install_settlement.final_authority_generation = 2;
	settlement.body.install_settlement.requester_connection_generation = 91;
	settlement.body.install_settlement.requester_target_generation = 41;
	settlement.body.install_settlement.page_scn_lsn = 82;
	settlement.body.install_settlement.page_checksum = UINT32_C(0x12345678);
	settlement.body.install_settlement.source_proof_crc32c
		= UINT32_C(0x87654321);
	settlement.body.install_settlement.installed_mode = PCM_STATE_X;
	settlement.body.install_settlement.requester_role
		= RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
	settlement.body.install_settlement.terminal_outcome = RESOURCE_X_OUTCOME_OK;
	settlement.body.install_settlement.terminal_state
		= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
		&settlement, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_SETTLED);
	UT_ASSERT_EQ(cluster_pcm_lock_query(tag), PCM_LOCK_MODE_X);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&second_assert.common.logical_assertion, &snapshot),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(snapshot.incompatible_holders_bitmap, UINT32_C(1) << 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_current_x_successor_exact(
		&tag, 1, &preserve_current_x), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(preserve_current_x);

	release = make_resource_x_master_frame(RESOURCE_X_WIRE_RELEASE_X,
										tag, 1, 1);
	release.common.observed_mode = PCM_STATE_X;
	release.common.target_mode = PCM_STATE_N;
	release.common.outcome = RESOURCE_X_OUTCOME_OK;
	release.common.authority_generation = 2;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_release_x_exact(&release, 1,
		&snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_RELEASED);
	/* A terminal requester remains the exact current source until its FIFO
	 * successor consumes BLOCK_TO_N and returns type-18.  Releasing to N first
	 * would strand the successor at N+historical-PI with no current carrier. */
	UT_ASSERT_EQ(cluster_pcm_lock_query(tag), PCM_LOCK_MODE_X);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_release_x_exact(&release, 1,
		&snapshot), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&second_assert.common.logical_assertion, &snapshot),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(snapshot.incompatible_holders_bitmap, UINT32_C(1) << 1);
	UT_ASSERT_EQ(snapshot.blocked_holders_bitmap, 0);
	UT_ASSERT_EQ(snapshot.is_head, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_intent_snapshot_exact(
		&second_assert.common.logical_assertion, 1, &block_intent,
		block_bytes, sizeof(block_bytes)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(block_intent.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(block_intent.destination_node, 1);

	/* The released requester slot accepts only a fresh attempt based on the
	 * current monotonic authority.  A late old release cannot alias it. */
	next_assert = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X,
										 tag, 1, 1);
	next_assert.common.base_authority_generation = 2;
	next_assert.common.authority_generation = 2;
	next_assert.common.assertion_sequence = 43;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&next_assert, 1,
		&snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_QUEUED);
	UT_ASSERT_EQ(snapshot.base_authority_generation, 2);
	UT_ASSERT_EQ(snapshot.assertion_sequence, 43);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_release_x_exact(&release, 1,
		&snapshot), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_QUEUED);

	durable.assertion = second_assert.common.logical_assertion;
	durable.assertion_sequence = 42;
	durable.requester_target_generation = 42;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(&durable,
		&snapshot), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(snapshot.proof_kind, 0);
}

UT_TEST(test_resource_x_release_without_successor_advances_canonical_base)
{
	BufferTag tag = make_tag(148);
	bool preserve_current_x = true;
	PcmAuthoritySnapshot authority;
	ResourceXAssertion next_assertion;
	ResourceXDecodedFrame first_assert;
	ResourceXDecodedFrame next_assert;
	ResourceXDecodedFrame settlement;
	ResourceXDecodedFrame release;
	ResourceXDurableProof durable;
	ResourceXMasterSnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_N_TO_X, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(
		tag, PCM_TRANS_X_TO_N_RELEASE, 0), PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(authority.transition_count, 2);
	first_assert = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 1, 1);
	first_assert.common.base_authority_generation = 2;
	first_assert.common.authority_generation = 2;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_base_bind_exact(
		&first_assert.common.logical_assertion, 17,
		authority.transition_count, &authority), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&first_assert, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_PROOF);

	memset(&durable, 0, sizeof(durable));
	durable.assertion = first_assert.common.logical_assertion;
	durable.base_authority_generation = 2;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = 41;
	durable.requester_target_generation = 41;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x12345678);
	durable.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(
		&durable, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.final_authority_generation, 3);

	settlement = make_resource_x_master_frame(
		RESOURCE_X_WIRE_INSTALL_SETTLEMENT, tag, 1, 1);
	settlement.common.outcome = RESOURCE_X_OUTCOME_OK;
	settlement.common.base_authority_generation = 2;
	settlement.common.authority_generation = 3;
	settlement.body.install_settlement.conversion_base_generation = 2;
	settlement.body.install_settlement.final_authority_generation = 3;
	settlement.body.install_settlement.requester_connection_generation = 91;
	settlement.body.install_settlement.requester_target_generation = 41;
	settlement.body.install_settlement.page_scn_lsn = 82;
	settlement.body.install_settlement.page_checksum = UINT32_C(0x12345678);
	settlement.body.install_settlement.source_proof_crc32c
		= UINT32_C(0x87654321);
	settlement.body.install_settlement.installed_mode = PCM_STATE_X;
	settlement.body.install_settlement.requester_role
		= RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
	settlement.body.install_settlement.terminal_outcome
		= RESOURCE_X_OUTCOME_OK;
	settlement.body.install_settlement.terminal_state
		= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(
		&settlement, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_current_x_successor_exact(
		&tag, 1, &preserve_current_x), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!preserve_current_x);

	release = make_resource_x_master_frame(
		RESOURCE_X_WIRE_RELEASE_X, tag, 1, 1);
	release.common.observed_mode = PCM_STATE_X;
	release.common.target_mode = PCM_STATE_N;
	release.common.outcome = RESOURCE_X_OUTCOME_OK;
	release.common.base_authority_generation = 2;
	release.common.authority_generation = 3;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_release_x_exact(
		&release, 1, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_RELEASED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	UT_ASSERT_EQ(authority.state, PCM_STATE_N);
	UT_ASSERT_EQ(authority.transition_count, 4);

	UT_ASSERT(resource_x_assertion_init(&tag, 2, &next_assertion));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_adapter_base_bind_exact(
		&next_assertion, 17, authority.transition_count, &authority),
		RESOURCE_X_APPLY_DUPLICATE);
	next_assert = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 2, 2);
	next_assert.common.base_authority_generation = authority.transition_count;
	next_assert.common.authority_generation = authority.transition_count;
	next_assert.common.assertion_sequence = 42;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&next_assert, 2, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_PROOF);
}

UT_TEST(test_resource_x_reclaim_nonhead_preserves_survivor_fifo)
{
	BufferTag tag = make_tag(149);
	ResourceXDecodedFrame first;
	ResourceXDecodedFrame dead;
	ResourceXDecodedFrame third;
	ResourceXMasterSnapshot snapshot;
	ResourceXReclaimWitness witness;
	ResourceXReconfigToken token;

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	first = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X, tag, 1, 1);
	dead = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X, tag, 2, 2);
	dead.common.assertion_sequence = 42;
	third = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X, tag, 3, 3);
	third.common.assertion_sequence = 43;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&first, 1, &snapshot),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&dead, 2, &snapshot),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&third, 3, &snapshot),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_resource_x_reconfig_freeze_exact(
		17, 18, UINT32_C(1) << 2, &token));

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_reclaim_requester_exact(
		&tag, 3, 17, &witness), RESOURCE_X_RECLAIM_NONE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_reclaim_requester_exact(
		&tag, 2, 17, &witness), RESOURCE_X_RECLAIM_NONHEAD);
	UT_ASSERT(resource_x_assertion_equal(&witness.assertion,
		&dead.common.logical_assertion));
	UT_ASSERT_EQ(witness.previous_phase, RESOURCE_X_MASTER_QUEUED);
	UT_ASSERT_EQ(witness.was_head, 0);
	UT_ASSERT_EQ(witness.successor_node, -1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&dead.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&first.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.is_head, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&third.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_QUEUED);
	UT_ASSERT_EQ(snapshot.is_head, 0);
}

UT_TEST(test_resource_x_reclaim_safe_head_starts_exact_successor)
{
	BufferTag tag = make_tag(150);
	ResourceXDecodedFrame dead;
	ResourceXDecodedFrame successor;
	ResourceXMasterSnapshot snapshot;
	ResourceXReclaimWitness witness;
	ResourceXReconfigToken token;

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	dead = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X, tag, 1, 1);
	successor = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X,
										 tag, 2, 2);
	successor.common.assertion_sequence = 42;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&dead, 1, &snapshot),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&successor, 2,
		&snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_resource_x_reconfig_freeze_exact(
		17, 18, UINT32_C(1) << 1, &token));

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_reclaim_requester_exact(
		&tag, 1, 17, &witness),
		RESOURCE_X_RECLAIM_HEAD_SUCCESSOR_STARTED);
	UT_ASSERT_EQ(witness.previous_phase, RESOURCE_X_MASTER_WAIT_PROOF);
	UT_ASSERT_EQ(witness.was_head, 1);
	UT_ASSERT_EQ(witness.successor_node, 2);
	UT_ASSERT_EQ(witness.successor_phase, RESOURCE_X_MASTER_WAIT_PROOF);
	UT_ASSERT_EQ(witness.successor_assertion_sequence, 42);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&dead.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&successor.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_PROOF);
	UT_ASSERT_EQ(snapshot.is_head, 1);
}

UT_TEST(test_resource_x_reclaim_post_grant_preserves_orphan_evidence)
{
	BufferTag tag = make_tag(151);
	ResourceXDecodedFrame dead;
	ResourceXDurableProof durable;
	ResourceXMasterSnapshot snapshot;
	ResourceXReclaimWitness witness;
	ResourceXReconfigToken token;

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	dead = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X, tag, 1, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&dead, 1, &snapshot),
				 RESOURCE_X_APPLY_APPLIED);
	memset(&durable, 0, sizeof(durable));
	durable.assertion = dead.common.logical_assertion;
	durable.base_authority_generation = 1;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = 41;
	durable.requester_target_generation = 41;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x12345678);
	durable.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(&durable,
		&snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
	UT_ASSERT(cluster_resource_x_reconfig_freeze_exact(
		17, 18, UINT32_C(1) << 1, &token));

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_reclaim_requester_exact(
		&tag, 1, 17, &witness), RESOURCE_X_RECLAIM_ORPHAN_BLOCKED);
	UT_ASSERT_EQ(witness.previous_phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
	UT_ASSERT_EQ(witness.source_evidence_preserved, 1);
	UT_ASSERT_EQ(witness.final_authority_generation, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&dead.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(snapshot.proof_kind, RESOURCE_X_PROOF_DURABLE_STORAGE);
	UT_ASSERT_EQ(snapshot.final_authority_generation, 2);
}

UT_TEST(test_resource_x_reconfig_sweep_drives_exact_dead_requester_reclaim)
{
	BufferTag tag = make_tag(152);
	ResourceXDecodedFrame dead;
	ResourceXDecodedFrame successor;
	ResourceXMasterSnapshot snapshot;
	ResourceXReconfigBatch batch;
	ResourceXReconfigToken token;

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	dead = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X, tag, 1, 1);
	successor = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X,
										 tag, 2, 2);
	successor.common.assertion_sequence = 42;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&dead, 1, &snapshot),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&successor, 2,
		&snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_resource_x_reconfig_freeze_exact(
		17, 18, UINT32_C(1) << 1, &token));
	UT_ASSERT_EQ(token.dead_requester_bitmap, UINT32_C(1) << 1);

	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_MORE);
	UT_ASSERT_EQ(batch.reclaim_count, 1);
	UT_ASSERT_EQ(batch.reclaim_head_count, 1);
	UT_ASSERT_EQ(batch.reclaim_nonhead_count, 0);
	UT_ASSERT_EQ(batch.reclaim_orphan_count, 0);
	UT_ASSERT_EQ(batch.reclaim_witnesses[0].result,
		RESOURCE_X_RECLAIM_HEAD_SUCCESSOR_STARTED);
	UT_ASSERT_EQ(batch.reclaim_witnesses[0].successor_node, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&dead.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&successor.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_PROOF);
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch),
				 RESOURCE_X_RECONFIG_ORPHAN);
	UT_ASSERT(!cluster_resource_x_reconfig_thaw_exact(&token));
}

UT_TEST(test_resource_x_intent_retains_logical_owner_across_physical_scarcity)
{
	BufferTag tag = make_tag(153);
	ResourceXIntentBodyHandle handle;
	ResourceXIntentSlot slot;
	ResourceXIntentSlot snapshot;
	ResourceXIntentSlot stale;

	memset(&handle, 0, sizeof(handle));
	memset(&slot, 0, sizeof(slot));
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &handle.assertion));
	handle.owner_generation = 41;
	handle.owner_node = 1;
	handle.owner_kind = RESOURCE_X_INTENT_OWNER_MASTER_GRANT;

	UT_ASSERT(!cluster_pcm_lock_resource_x_intent_arm_exact(
		&slot, &handle, 51, 61, 71, 2, RESOURCE_X_CONTROL_V1_BYTES,
		RESOURCE_X_WIRE_AUTHORITY_GRANT));
	UT_ASSERT_EQ(slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT(cluster_pcm_lock_resource_x_intent_arm_exact(
		&slot, &handle, 51, 61, 71, 2, RESOURCE_X_PROOF_V1_BYTES,
		RESOURCE_X_WIRE_AUTHORITY_GRANT));
	UT_ASSERT_EQ(slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(slot.first_armed_us, 71);
	UT_ASSERT_EQ(slot.last_attempt_us, 0);
	snapshot = slot;
	UT_ASSERT(!cluster_pcm_lock_resource_x_intent_arm_exact(
		&slot, &handle, 51, 61, 72, 3, RESOURCE_X_PROOF_V1_BYTES,
		RESOURCE_X_WIRE_AUTHORITY_GRANT));
	UT_ASSERT_EQ(memcmp(&slot, &snapshot, sizeof(slot)), 0);

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_intent_not_admitted_exact(
		&slot, &snapshot, 73), RESOURCE_X_INTENT_NOT_ADMITTED);
	UT_ASSERT_EQ(slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(slot.last_attempt_us, 73);
	stale = snapshot;
	stale.authority_generation++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_intent_stage_exact(&slot, &stale, 74),
		RESOURCE_X_INTENT_STALE);
	UT_ASSERT_EQ(slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_intent_stage_exact(&slot, &snapshot, 74),
		RESOURCE_X_INTENT_STAGED);
	UT_ASSERT_EQ(slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
	UT_ASSERT_EQ(slot.last_attempt_us, 74);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_intent_hard_rearm_exact(
		&slot, &snapshot, 75), RESOURCE_X_INTENT_HARD_REARMED);
	UT_ASSERT_EQ(slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(slot.first_armed_us, 71);
	UT_ASSERT_EQ(slot.last_attempt_us, 75);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_intent_stage_exact(&slot, &snapshot, 76),
		RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(!cluster_pcm_lock_resource_x_intent_complete_exact(&slot, &stale));
	UT_ASSERT_EQ(slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_intent_complete_exact(&slot, &snapshot));
	UT_ASSERT_EQ(slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(slot.logical_generation, 0);
}

UT_TEST(test_resource_x_intent_sparse_probe_rediscovers_exact_rearm)
{
	BufferTag tag = make_tag(154);
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame decoded;
	ResourceXDurableProof durable;
	ResourceXIntentProbeResult probe_result = RESOURCE_X_INTENT_PROBE_IDLE;
	ResourceXIntentSlot intent;
	ResourceXIntentSlot staged_intent;
	ResourceXMasterSnapshot snapshot;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 payload[RESOURCE_X_PROOF_V1_BYTES];
	uint32 examined = 0;
	int calls;

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	assertion = make_resource_x_master_frame(
		RESOURCE_X_WIRE_ASSERT_X, tag, 2, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(
		&assertion, 2, &snapshot), RESOURCE_X_APPLY_APPLIED);
	memset(&durable, 0, sizeof(durable));
	durable.assertion = assertion.common.logical_assertion;
	durable.base_authority_generation = 1;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = 41;
	durable.requester_target_generation = 41;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x12345678);
	durable.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(
		&durable, &snapshot), RESOURCE_X_APPLY_APPLIED);

	for (calls = 0; calls < 8; calls++) {
		probe_result = cluster_pcm_lock_resource_x_outbound_intent_probe_exact(
			1, &intent, payload, sizeof(payload), &examined);
		UT_ASSERT(examined <= 1);
		if (probe_result == RESOURCE_X_INTENT_PROBE_FOUND)
			break;
		UT_ASSERT_EQ(probe_result, RESOURCE_X_INTENT_PROBE_MORE);
	}
	UT_ASSERT_EQ(probe_result, RESOURCE_X_INTENT_PROBE_FOUND);
	UT_ASSERT_EQ(intent.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(intent.logical_generation, 41);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, payload, sizeof(payload),
		&decoded, &reject));
	UT_ASSERT_EQ(decoded.kind, RESOURCE_X_WIRE_AUTHORITY_GRANT);
	UT_ASSERT_EQ(decoded.common.authority_generation, 2);

	staged_intent = intent;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_stage_exact(
		&staged_intent, 201), RESOURCE_X_INTENT_STAGED);
	for (calls = 0; calls < 8; calls++) {
		probe_result = cluster_pcm_lock_resource_x_outbound_intent_probe_exact(
			1, &intent, payload, sizeof(payload), &examined);
		UT_ASSERT(examined <= 1);
		if (probe_result == RESOURCE_X_INTENT_PROBE_COMPLETE)
			break;
		UT_ASSERT_EQ(probe_result, RESOURCE_X_INTENT_PROBE_MORE);
	}
	UT_ASSERT_EQ(probe_result, RESOURCE_X_INTENT_PROBE_COMPLETE);

	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_hard_rearm_exact(
		&staged_intent, 202), RESOURCE_X_INTENT_HARD_REARMED);
	for (calls = 0; calls < 8; calls++) {
		probe_result = cluster_pcm_lock_resource_x_outbound_intent_probe_exact(
			1, &intent, payload, sizeof(payload), &examined);
		UT_ASSERT(examined <= 1);
		if (probe_result == RESOURCE_X_INTENT_PROBE_FOUND)
			break;
		UT_ASSERT_EQ(probe_result, RESOURCE_X_INTENT_PROBE_MORE);
	}
	UT_ASSERT_EQ(probe_result, RESOURCE_X_INTENT_PROBE_FOUND);
	UT_ASSERT_EQ(intent.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(intent.logical_generation, 41);
}

UT_TEST(test_resource_x_duplicate_assert_redrives_completed_grant_delivery)
{
	BufferTag tag = make_tag(172);
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame ack;
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame decoded;
	ResourceXDurableProof durable;
	ResourceXIntentSlot completed_intent;
	ResourceXIntentSlot replay_intent;
	ResourceXMasterSnapshot snapshot;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 completed_payload[RESOURCE_X_PROOF_V1_BYTES];
	uint8 replay_payload[RESOURCE_X_PROOF_V1_BYTES];

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
		RESOURCE_X_APPLY_APPLIED);
	request = make_resource_x_bootstrap_request_values(
		tag, 2, 17, 31, 41, 51);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&request, 2, 61, 77, 31, 71, &ack), RESOURCE_X_APPLY_APPLIED);
	assertion = ack;
	assertion.kind = RESOURCE_X_WIRE_ASSERT_X;
	assertion.common.sender_connection_generation = 51;
	assertion.common.authority_generation
		= assertion.common.base_authority_generation;
	assertion.common.outcome = RESOURCE_X_OUTCOME_NONE;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&assertion, 2, 61, 77, 31, 71, &snapshot),
		RESOURCE_X_APPLY_APPLIED);

	memset(&durable, 0, sizeof(durable));
	durable.assertion = assertion.common.logical_assertion;
	durable.base_authority_generation = 1;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = 41;
	durable.requester_target_generation = 41;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x12345678);
	durable.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(
		&durable, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
		&assertion.common.logical_assertion, &completed_intent,
		completed_payload, sizeof(completed_payload)),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_stage_exact(
		&completed_intent, 101), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_grant_intent_complete_exact(
		&completed_intent));

	/* Transport completion is not requester T3.  The existing R7 ASSERT
	 * replay must reconstruct the same frozen grant from the canonical
	 * GRANT_COMMITTED request, without changing authority or attempt. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&assertion, 2, 61, 77, 31, 71, &snapshot),
		RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_GRANT_COMMITTED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
		&assertion.common.logical_assertion, &replay_intent,
		replay_payload, sizeof(replay_payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(replay_intent.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(replay_intent.logical_generation,
		completed_intent.logical_generation);
	UT_ASSERT_EQ(replay_intent.authority_generation,
		completed_intent.authority_generation);
	UT_ASSERT_EQ(memcmp(replay_payload, completed_payload,
		sizeof(replay_payload)), 0);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, replay_payload,
		sizeof(replay_payload), &decoded, &reject));
	UT_ASSERT_EQ(decoded.kind, RESOURCE_X_WIRE_AUTHORITY_GRANT);
	UT_ASSERT_EQ(decoded.common.assertion_sequence, UINT64_C(41));
	UT_ASSERT_EQ(decoded.common.authority_generation, UINT64_C(2));
}

UT_TEST(test_pcm_grd_convert_queue_placeholder_remains_null)
{
	BufferTag tag = make_tag(64);
	int n_count, s_count, x_count, pi_total, convert_q;

	reset_fake_pcm_runtime(4);
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_pcm_grd_get_summary(&n_count, &s_count, &x_count, &pi_total, &convert_q);
	UT_ASSERT_EQ(convert_q, 0);
}


UT_TEST(test_pcm_real_wait_event_call_sites_are_exercised)
{
	BufferTag tag = make_tag(6);

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ((int)fake_init_wait_event_seen, (int)WAIT_EVENT_PCM_GRD_INIT);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)fake_lwlock_wait_event_seen, (int)WAIT_EVENT_PCM_TRANSITION_APPLY);
	UT_ASSERT_EQ((int)ut_wait_event_info_storage, 0);
}


/* ============================================================
 * PGRAC: spec-2.31 D1 v0.4 — PCM API hardening (PCM-H1..H4).
 * ============================================================ */
UT_TEST(test_pcm_H1_same_node_s_refcount_increments)
{
	BufferTag tag = make_tag(10);

	reset_fake_pcm_runtime(4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);
	/* N→S transition counter incremented once */
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_n_to_s_count(), 1);

	/* Second S acquire by same node — refcount bumps, no N→S transition. */
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_n_to_s_count(), 1);

	/* First release: state still S (refcount drops from 2 to 1). */
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_n_release_count(), 0);
}


UT_TEST(test_pcm_H2_last_s_release_transitions_to_n)
{
	BufferTag tag = make_tag(11);

	reset_fake_pcm_runtime(4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);

	/* First release: state remains S. */
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);

	/* Second release (refcount→0): state→N, broadcast fires. */
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_n_release_count(), 1);
	UT_ASSERT((fake_cv_broadcast_count) >= (1));
}


UT_TEST(test_pcm_H2b_same_node_s_residency_upgrades_to_x)
{
	BufferTag tag = make_tag(14);

	reset_fake_pcm_runtime(4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);

	/*
	 * spec-2.35 HC111/HC112 keeps S as cache residency after content-lock
	 * unlock.  A later local X acquire by the same node must upgrade the
	 * residency bit instead of waiting on its own preserved S holder.
	 */
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_x_upgrade_count(), 1);
	UT_ASSERT_EQ(fake_cv_sleep_count, 0);
}


UT_TEST(test_pcm_H3_incompatible_x_waits_and_wakes)
{
	BufferTag tag = make_tag(12);

	reset_fake_pcm_runtime(4);

	/* Node 0 holds X. */
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);

	/* Arm stub:  on first Sleep, simulate node-0 releasing X.  Then the
	 * acquire loop sees state=N and proceeds to acquire X for node 1. */
	fake_cv_wake_release.tag = tag;
	fake_cv_wake_release.holder_node = 0;
	fake_cv_wake_release.armed = true;

	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);

	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);
	UT_ASSERT((fake_cv_sleep_count) >= (1));
	UT_ASSERT_EQ((int)fake_cv_sleep_wait_event, (int)WAIT_EVENT_PCM_COMPATIBLE_STATE_WAIT);
	UT_ASSERT((fake_cv_prepare_count) >= (1));
	UT_ASSERT((fake_cv_cancel_count) >= (1));
}


UT_TEST(test_pcm_H4_release_broadcasts_only_on_state_change)
{
	BufferTag tag = make_tag(13);

	reset_fake_pcm_runtime(4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);

	/* X→N release: broadcast fires (state changed to N). */
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);
	UT_ASSERT_EQ(fake_cv_broadcast_count, 1);

	/* S acquire + release (single holder): broadcast fires again. */
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);
	UT_ASSERT_EQ(fake_cv_broadcast_count, 2);

	/* Two S acquires + one release: refcount 2→1, no state change, no broadcast. */
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(fake_cv_broadcast_count, 2);

	/* Second release: refcount 1→0, state→N, broadcast fires. */
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);
	UT_ASSERT_EQ(fake_cv_broadcast_count, 3);
}


/* ============================================================
 * spec-4.7a D2/D3/D4 — block coherence gate decision logic (direct unit
 * coverage of the master-side X-contention gate, since the non-injection
 * e2e is blocked by the deferred concurrent-relation data plane — see
 * t/252 + spec-4.7a §4.1).
 * ============================================================ */

UT_TEST(test_pcm_d2_mode_covers_truth_table)
{
	/* X covers {S,X}; S covers {S}; N covers nothing (hold-until-revoked gate). */
	UT_ASSERT(cluster_pcm_mode_covers(PCM_LOCK_MODE_X, PCM_LOCK_MODE_S));
	UT_ASSERT(cluster_pcm_mode_covers(PCM_LOCK_MODE_X, PCM_LOCK_MODE_X));
	UT_ASSERT(cluster_pcm_mode_covers(PCM_LOCK_MODE_S, PCM_LOCK_MODE_S));
	UT_ASSERT(!cluster_pcm_mode_covers(PCM_LOCK_MODE_S, PCM_LOCK_MODE_X));
	UT_ASSERT(!cluster_pcm_mode_covers(PCM_LOCK_MODE_N, PCM_LOCK_MODE_S));
	UT_ASSERT(!cluster_pcm_mode_covers(PCM_LOCK_MODE_N, PCM_LOCK_MODE_X));
}

UT_TEST(test_pcm_d3_requester_is_holder_strict)
{
	BufferTag tag = make_tag(40);

	reset_fake_pcm_runtime(4);
	fake_cssd_dead_node = -1;

	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X); /* node 2 holds X */

	/* x_holder==sender covers N→S and N→X (idempotent re-grant, D3). */
	UT_ASSERT(cluster_pcm_master_requester_is_holder(tag, 2, PCM_TRANS_N_TO_S));
	UT_ASSERT(cluster_pcm_master_requester_is_holder(tag, 2, PCM_TRANS_N_TO_X));
	/* S→X never self-regrants (real writer path → invalidate-then-grant). */
	UT_ASSERT(!cluster_pcm_master_requester_is_holder(tag, 2, PCM_TRANS_S_TO_X_UPGRADE));
	/* A non-holder is never a holder (fail-closed). */
	UT_ASSERT(!cluster_pcm_master_requester_is_holder(tag, 1, PCM_TRANS_N_TO_S));
	/* Missing entry → false (Rule 8.A fail-closed). */
	UT_ASSERT(!cluster_pcm_master_requester_is_holder(make_tag(41), 2, PCM_TRANS_N_TO_S));
}

UT_TEST(test_pcm_d4_other_live_holder_gate)
{
	BufferTag xtag = make_tag(42);
	BufferTag stag = make_tag(43);
	BufferTag selftag = make_tag(44);

	reset_fake_pcm_runtime(4);
	fake_cssd_dead_node = -1;

	/* node 2 holds X. */
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(xtag, PCM_LOCK_MODE_X);

	/* Another live node (2) holds X → a different sender is BLOCKED (D4). */
	UT_ASSERT(cluster_pcm_master_other_live_holder_exists(xtag, 1));
	/* The holder itself is not an "other" holder → not blocked (self path). */
	UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(xtag, 2));
	/* Missing entry → no holder → not blocked. */
	UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(make_tag(99), 1));

	/* A DEAD holder is NOT counted — that is the warm-recovery path, not D4. */
	fake_cssd_dead_node = 2;
	UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(xtag, 1));
	fake_cssd_dead_node = -1;

	/* node 1 and node 3 both hold S on stag. */
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(stag, PCM_LOCK_MODE_S);
	cluster_node_id = 3;
	cluster_pcm_lock_acquire(stag, PCM_LOCK_MODE_S);
	UT_ASSERT(cluster_pcm_master_other_live_holder_exists(stag, 1)); /* sees node 3 */
	UT_ASSERT(cluster_pcm_master_other_live_holder_exists(stag, 3)); /* sees node 1 */
	UT_ASSERT(cluster_pcm_master_other_live_holder_exists(stag, 0)); /* non-holder sees both */

	/* Sole S holder → no OTHER holder → not blocked (self can upgrade). */
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(selftag, PCM_LOCK_MODE_S);
	UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(selftag, 1));
}

/*
 * spec-4.7a B (HG7 local-path completion) — the acquire-side bounded fail-closed.
 * When the local master path meets an incompatible LIVE remote holder and
 * hold-until-revoked is on, it must ereport (FEATURE_NOT_SUPPORTED) rather than
 * hang on wait_cv (the writer transfer that would revoke the holder is deferred).
 * This is the acquire-path mirror of the D4 master-dispatch gate above; together
 * they cover both round-trip paths HG7 promises "no hang" for.
 */
UT_TEST(test_pcm_b_local_master_remote_x_holder_fail_closed)
{
	BufferTag tag = make_tag(45);
	bool save = cluster_gcs_block_local_cache;

	reset_fake_pcm_runtime(4);
	fake_cssd_dead_node = -1;

	/* node 2 holds X — the conflicting remote LIVE holder. */
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);

	/* node 1 (local master) wants X with hold-until-revoked on → fail-closed. */
	cluster_node_id = 1;
	cluster_gcs_block_local_cache = true;
	UT_EXPECT_EREPORT(cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X));
	cluster_gcs_block_local_cache = save;

	/* The holder is untouched — no illegal transition applied on the fail-closed
	 * path (node 2 still records X). */
	UT_ASSERT(cluster_pcm_master_other_live_holder_exists(tag, 1));

	/* A DEAD conflicting holder is NOT fail-closed — that block belongs to the
	 * warm-recovery path; the acquire falls through to the legitimate wait.  We
	 * assert only the holder-liveness scoping of the gate here (the wait itself
	 * is covered by H3); do not call acquire (it would block on wait_cv). */
	fake_cssd_dead_node = 2;
	UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(tag, 1));
	fake_cssd_dead_node = -1;
}

/*
 * spec-4.7 D1 — RECOVERING gate fail-closed.  When a block resource is
 * RECOVERING, cluster_pcm_lock_acquire_buffer must fail-closed 53R9L after the
 * bounded wait — never route to the dead master nor serve stale local state.
 * With cluster.gcs_block_recovery_wait_ms = 0 the gate fail-closes immediately
 * (deterministic;  the ereport precedes any sleep / CHECK_FOR_INTERRUPTS).
 * This proves the gate logic that lives in cluster_pcm_lock.o;  the phase
 * predicate itself (master DEAD → RECOVERING) is e2e-deferred (measure-first,
 * spec-4.7 D0 Impl note v0.1) and unit-proven with the master-rebuild logic in
 * spec-4.7 D3 (test_cluster_gcs_recovery).
 */
UT_TEST(test_pcm_d1_recovering_gate_fail_closed)
{
	BufferDesc buf;
	bool retry_denied = false;

	reset_fake_pcm_runtime(2);
	buf.tag = make_tag(77);

	fake_block_phase = GCS_BLOCK_RECOVERING;
	cluster_gcs_block_recovery_wait_ms = 0; /* immediate fail-closed, no sleep */
	UT_EXPECT_EREPORT(cluster_pcm_lock_acquire_buffer(&buf, PCM_LOCK_MODE_S, &retry_denied));
	fake_block_phase = GCS_BLOCK_NORMAL;
	cluster_gcs_block_recovery_wait_ms = 200;
}

/*
 * spec-4.7 D2 — master rebuild from one survivor re-declare.  Proves the
 * rebuild records the declared holder (X authoritative) and the monotone-max
 * PI watermark.  The block-protocol e2e is deferred (measure-first, D0 Impl
 * note v0.1);  this is the L239 unit-proof of the 8.A-relevant master-view
 * reconstruction (D3 adds the not-double-X conflict invariant).
 */
UT_TEST(test_pcm_d2_rebuild_from_redeclare)
{
	BufferTag tagx = make_tag(88);
	BufferTag tags = make_tag(89);

	reset_fake_pcm_runtime(4);
	fake_cssd_dead_node = -1;

	/* node 2 re-declares X on tagx with page_lsn 0x5000 + page_scn 0x5500
	 * (spec-2.41 D3 dual carrier). */
	cluster_gcs_block_master_rebuild_from_redeclare(tagx, (uint8)PCM_STATE_X, (XLogRecPtr)0x5000,
													(SCN)0x5500, 2, 7);
	/* The rebuilt master view records node 2 as the (live) X holder. */
	UT_ASSERT(cluster_pcm_master_other_live_holder_exists(tagx, 3));
	UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(tagx, 2));
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_lsn_query(tagx), (uint64)0x5000);
	/* spec-2.41 D3 — the SCN watermark is advanced from page_scn (orthogonal). */
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_scn_query(tagx), (uint64)0x5500);

	/* node 1 re-declares S on tags with higher lsn+scn — both watermarks = max. */
	cluster_gcs_block_master_rebuild_from_redeclare(tags, (uint8)PCM_STATE_S, (XLogRecPtr)0x9000,
													(SCN)0x9900, 1, 7);
	UT_ASSERT(cluster_pcm_master_other_live_holder_exists(tags, 0));
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_lsn_query(tags), (uint64)0x9000);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_scn_query(tags), (uint64)0x9900);

	/* A stale lower lsn+scn re-declare must NOT regress either watermark. */
	cluster_gcs_block_master_rebuild_from_redeclare(tags, (uint8)PCM_STATE_S, (XLogRecPtr)0x100,
													(SCN)0x150, 3, 7);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_lsn_query(tags), (uint64)0x9000);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_scn_query(tags), (uint64)0x9900);
}

/*
 * spec-4.7 D3 — not-double-X invariant (规则 8.A).  Two distinct nodes
 * declaring X on the SAME block (pre-crash single-X violated) must NEVER
 * reconstruct two X holders.  The first X declarer wins;  the conflicting
 * second is rejected (the rebuilt view keeps node 2, never node 3), so the
 * recovery path can never produce a cross-node double grant.
 */
UT_TEST(test_pcm_d3_not_double_x)
{
	BufferTag tag = make_tag(91);

	reset_fake_pcm_runtime(4);
	fake_cssd_dead_node = -1;

	cluster_gcs_block_master_rebuild_from_redeclare(tag, (uint8)PCM_STATE_X, (XLogRecPtr)0x4000,
													(SCN)0x4400, 2, 7);
	/* Conflicting X from a DIFFERENT node — must be rejected. */
	cluster_gcs_block_master_rebuild_from_redeclare(tag, (uint8)PCM_STATE_X, (XLogRecPtr)0x4000,
													(SCN)0x4400, 3, 7);

	/* x_holder stays node 2 (not 3):  node 2 self-excluded → false;  any other
	 * sender sees node 2 as the live X holder.  Had the conflicting node-3 X
	 * been applied, other_live_holder_exists(tag, 2) would be TRUE. */
	UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(tag, 2));
	UT_ASSERT(cluster_pcm_master_other_live_holder_exists(tag, 3));

	/* Same node re-declaring X is idempotent (not a conflict).  spec-2.41 D3:
	 * carries page_scn alongside page_lsn (value irrelevant to this invariant). */
	UT_ASSERT(cluster_gcs_block_master_rebuild_from_redeclare(
		tag, (uint8)PCM_STATE_X, (XLogRecPtr)0x4000, (SCN)0x4400, 2, 7));
	UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(tag, 2));
	UT_ASSERT(cluster_pcm_master_other_live_holder_exists(tag, 3));

	/*
	 * code-review P1 — X-vs-S contradiction (both directions) must fail-closed
	 * (return false), not silently keep/overwrite.
	 */
	{
		BufferTag tagxs = make_tag(92);
		BufferTag tagsx = make_tag(93);

		/* X-held then S from a DIFFERENT node → reject (was: silently dropped,
		 * returned true). */
		UT_ASSERT(cluster_gcs_block_master_rebuild_from_redeclare(
			tagxs, (uint8)PCM_STATE_X, (XLogRecPtr)0x10, (SCN)0x20, 2, 7));
		UT_ASSERT(!cluster_gcs_block_master_rebuild_from_redeclare(
			tagxs, (uint8)PCM_STATE_S, (XLogRecPtr)0x10, (SCN)0x20, 1, 7));
		/* still X by node 2 (S not merged). */
		UT_ASSERT(cluster_pcm_master_other_live_holder_exists(tagxs, 3));
		UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(tagxs, 2));

		/* S-held by node 1 then X from a DIFFERENT node → reject (X-over-live-S
		 * = never reconstruct a double grant; was: silently overwrote S). */
		UT_ASSERT(cluster_gcs_block_master_rebuild_from_redeclare(
			tagsx, (uint8)PCM_STATE_S, (XLogRecPtr)0x10, (SCN)0x20, 1, 7));
		UT_ASSERT(!cluster_gcs_block_master_rebuild_from_redeclare(
			tagsx, (uint8)PCM_STATE_X, (XLogRecPtr)0x10, (SCN)0x20, 2, 7));
	}
}

/*
 * S3 forensics step 1a — SCN-watermark advance provenance ring.
 *	Every feed records {source, sender, request_id, epoch, old->new,
 *	advanced}; the latest record per tag is queryable.  The key semantic:
 *	a LATE / stale feed is recorded with advanced=false and the watermark
 *	unchanged — the branch-3 (watermark false-positive) discriminator a
 *	53R93 emit site attaches to its errdetail / LOG line.
 */
UT_TEST(test_pcm_wm_prov_table_keeps_last_advance)
{
	BufferTag tag = make_tag(95);
	BufferTag other = make_tag(96);
	ClusterPcmWmProv prov;

	reset_fake_pcm_runtime(4);
	fake_cssd_dead_node = -1;

	/* No feed yet — a definite NONE (no insert has ever been dropped). */
	UT_ASSERT(!cluster_pcm_lock_pi_watermark_prov_query(tag, &prov));
	UT_ASSERT_EQ((long)prov.source, (long)CLUSTER_PCM_WM_SRC_NONE);
	UT_ASSERT(!prov.table_full);

	/* Redeclare feed (real inline writer path): advancing feed recorded. */
	cluster_gcs_block_master_rebuild_from_redeclare(tag, (uint8)PCM_STATE_S, (XLogRecPtr)0x1000,
													(SCN)0x2000, 2, 7);
	UT_ASSERT(cluster_pcm_lock_pi_watermark_prov_query(tag, &prov));
	UT_ASSERT_EQ((long)prov.source, (long)CLUSTER_PCM_WM_SRC_REDECLARE);
	UT_ASSERT_EQ((long)prov.sender_node, 2L);
	UT_ASSERT_EQ((long)prov.epoch, 7L);
	UT_ASSERT_EQ((long)prov.new_scn, 0x2000L);

	/* ACK feed advances further: the tag's single slot is updated in place
	 * with the new advance's full wire identity. */
	cluster_pcm_lock_pi_watermark_scn_advance(tag, (SCN)0x3000, CLUSTER_PCM_WM_SRC_ACK_SLOT, 3,
											  4242, 9);
	UT_ASSERT(cluster_pcm_lock_pi_watermark_prov_query(tag, &prov));
	UT_ASSERT_EQ((long)prov.source, (long)CLUSTER_PCM_WM_SRC_ACK_SLOT);
	UT_ASSERT_EQ((long)prov.sender_node, 3L);
	UT_ASSERT_EQ((long)prov.request_id, 4242L);
	UT_ASSERT_EQ((long)prov.epoch, 9L);
	UT_ASSERT_EQ((long)prov.old_scn, 0x2000L);
	UT_ASSERT_EQ((long)prov.new_scn, 0x3000L);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_scn_query(tag), (uint64)0x3000);

	/* A LATE / stale feed must NOT enter the table (step 1b): the query
	 * still returns the ACK_SLOT advance that produced the CURRENT
	 * watermark, and its new_scn equals that watermark. */
	cluster_pcm_lock_pi_watermark_scn_advance(tag, (SCN)0x100, CLUSTER_PCM_WM_SRC_ACK_SLOTLESS, 1,
											  4243, 5);
	UT_ASSERT(cluster_pcm_lock_pi_watermark_prov_query(tag, &prov));
	UT_ASSERT_EQ((long)prov.source, (long)CLUSTER_PCM_WM_SRC_ACK_SLOT);
	UT_ASSERT_EQ((long)prov.sender_node, 3L);
	UT_ASSERT_EQ((long)prov.request_id, 4242L);
	UT_ASSERT_EQ((long)prov.new_scn, 0x3000L);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_scn_query(tag), (uint64)0x3000);
	UT_ASSERT_EQ((uint64)prov.new_scn, (uint64)cluster_pcm_lock_pi_watermark_scn_query(tag));

	/* A second tag gets its own slot (open addressing, insert-once). */
	cluster_gcs_block_master_rebuild_from_redeclare(other, (uint8)PCM_STATE_S, (XLogRecPtr)0x1000,
													(SCN)0x5000, 1, 7);
	UT_ASSERT(cluster_pcm_lock_pi_watermark_prov_query(other, &prov));
	UT_ASSERT_EQ((long)prov.source, (long)CLUSTER_PCM_WM_SRC_REDECLARE);
	UT_ASSERT_EQ((long)prov.new_scn, 0x5000L);
	UT_ASSERT(cluster_pcm_lock_pi_watermark_prov_query(tag, &prov));
	UT_ASSERT_EQ((long)prov.new_scn, 0x3000L);
}

UT_TEST(test_pcm_acquire_buffer_local_s_nonholder_registers_s_then_upgrades)
{
	BufferTag tag = make_tag(96);
	BufferDesc buf;
	bool save = cluster_gcs_block_local_cache;
	bool retry_denied = false;

	reset_fake_pcm_runtime(4);
	fake_cssd_dead_node = -1;
	cluster_gcs_block_local_cache = true;
	fake_local_x_upgrade_result = true;

	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);

	memset(&buf, 0, sizeof(buf));
	buf.tag = tag;
	buf.pcm_state = (uint8)PCM_STATE_N;
	cluster_node_id = 0;
	UT_ASSERT(cluster_pcm_lock_acquire_buffer(&buf, PCM_LOCK_MODE_X, &retry_denied));
	UT_ASSERT(!retry_denied);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);
	UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(tag, 0));
	UT_ASSERT(cluster_pcm_master_requester_is_holder(tag, 0, PCM_TRANS_N_TO_X));

	cluster_gcs_block_local_cache = save;
}

UT_TEST(test_pcm_acquire_buffer_local_s_retries_while_pcm_x_head_is_active)
{
	BufferTag tag = make_tag(961);
	BufferDesc buf;
	bool retry_denied = false;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	fake_pcm_x_local_s_barrier_active = true;
	memset(&buf, 0, sizeof(buf));
	buf.tag = tag;
	buf.pcm_state = (uint8) PCM_STATE_N;

	UT_ASSERT(!cluster_pcm_lock_acquire_buffer(&buf, PCM_LOCK_MODE_S, &retry_denied));
	UT_ASSERT(retry_denied);
	UT_ASSERT_EQ(fake_pcm_x_local_s_barrier_checks, 1);
	UT_ASSERT_EQ((int) cluster_pcm_lock_query(tag), (int) PCM_LOCK_MODE_N);
	UT_ASSERT_EQ(fake_local_read_image_count, 0);
}

/*
 * P0-26 sibling race: the local-master buffer-aware X path observes shared S
 * with no local S bit, then bootstraps a local S declaration before upgrade.
 * A queue handoff may replace that S authority with remote X in between.  The
 * nested acquire must preserve the BufferDesc-aware exact transfer route,
 * never escape through the tag-only legacy terminal.
 */
UT_TEST(test_pcm_acquire_buffer_s_bootstrap_revalidates_remote_x)
{
	BufferTag tag = make_tag(970);
	BufferDesc buf;
	PcmAuthoritySnapshot after;
	bool retry_denied = false;
	bool acquired = false;
	bool escaped_error = false;

	reset_fake_pcm_runtime(4);
	cluster_gcs_block_local_cache = true;
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);

	memset(&buf, 0, sizeof(buf));
	buf.tag = tag;
	buf.pcm_state = (uint8)PCM_STATE_N;
	cluster_node_id = 0;
	fake_local_x_transfer_count = 0;
	fake_local_x_transfer_holder = -1;
	fake_acquire_entry_handoff_tag = tag;
	fake_acquire_entry_handoff_source = 1;
	fake_acquire_entry_handoff_target = 2;
	fake_acquire_entry_handoff_release = PCM_TRANS_S_TO_N_RELEASE;
	cluster_injection_armed_count = 1;
	fake_acquire_entry_handoff_armed = true;

	if (sigsetjmp(ut_ereport_jump, 1) == 0) {
		ut_ereport_jump_armed = true;
		acquired = cluster_pcm_lock_acquire_buffer(&buf, PCM_LOCK_MODE_X, &retry_denied);
		ut_ereport_jump_armed = false;
	} else {
		ut_ereport_jump_armed = false;
		escaped_error = true;
	}

	UT_ASSERT(!escaped_error);
	UT_ASSERT(acquired);
	UT_ASSERT(!retry_denied);
	UT_ASSERT_EQ(fake_local_x_transfer_count, 1);
	UT_ASSERT_EQ(fake_local_x_transfer_holder, 2);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ((int)after.state, (int)PCM_STATE_X);
	UT_ASSERT_EQ(after.x_holder_node, 2);
	UT_ASSERT_EQ(memcmp(&fake_local_x_transfer_expected, &after, sizeof(after)), 0);
}

/* P0-26 third entry: the ordinary buffer-aware S path also has an optimistic
 * remote-X precheck.  A local-X -> remote-X queue handoff between that check
 * and the entry-lock acquire must retain the BufferDesc-aware read-image
 * route; it must not fall through the tag-only legacy terminal. */
UT_TEST(test_pcm_acquire_buffer_s_revalidates_remote_x_after_precheck)
{
	BufferTag tag = make_tag(971);
	BufferDesc buf;
	PcmAuthoritySnapshot after;
	bool retry_denied = false;
	bool acquired = true;
	bool escaped_error = false;

	reset_fake_pcm_runtime(4);
	cluster_gcs_block_local_cache = true;
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);

	memset(&buf, 0, sizeof(buf));
	buf.tag = tag;
	fake_acquire_entry_handoff_tag = tag;
	fake_acquire_entry_handoff_source = 0;
	fake_acquire_entry_handoff_target = 1;
	cluster_injection_armed_count = 1;
	fake_acquire_entry_handoff_armed = true;

	if (sigsetjmp(ut_ereport_jump, 1) == 0) {
		ut_ereport_jump_armed = true;
		acquired = cluster_pcm_lock_acquire_buffer(&buf, PCM_LOCK_MODE_S, &retry_denied);
		ut_ereport_jump_armed = false;
	} else {
		ut_ereport_jump_armed = false;
		escaped_error = true;
	}

	UT_ASSERT(!escaped_error);
	UT_ASSERT(!acquired); /* one-shot READ_IMAGE is intentionally non-durable */
	UT_ASSERT(!retry_denied);
	UT_ASSERT_EQ(fake_local_read_image_count, 1);
	UT_ASSERT_EQ(fake_local_read_image_holder, 1);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ((int)after.state, (int)PCM_STATE_X);
	UT_ASSERT_EQ(after.x_holder_node, 1);
	UT_ASSERT_EQ(memcmp(&fake_local_read_image_expected, &after, sizeof(after)), 0);
}

/* P0-26: the buffer-aware local-master X path used to inspect state/holder,
 * then call the tag-only acquire without carrying an authoritative token.
 * Commit a queue-style local-X -> remote-X handoff at the existing acquire
 * entry injection point, exactly after the optimistic precheck and before the
 * tag-only entry lock.  The buffer-aware caller must redirect to the existing
 * safe X-transfer path; the old code escapes through the legacy
 * "cross-node block write transfer not supported" ERROR instead.
 */
UT_TEST(test_pcm_acquire_buffer_revalidates_remote_x_after_precheck)
{
	BufferTag tag = make_tag(97);
	BufferDesc buf;
	PcmAuthoritySnapshot after;
	bool retry_denied = false;
	bool acquired = false;
	bool escaped_error = false;

	reset_fake_pcm_runtime(4);
	cluster_gcs_block_local_cache = true;
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);

	memset(&buf, 0, sizeof(buf));
	buf.tag = tag;
	fake_local_x_transfer_count = 0;
	fake_local_x_transfer_holder = -1;
	fake_acquire_entry_handoff_tag = tag;
	fake_acquire_entry_handoff_source = 0;
	fake_acquire_entry_handoff_target = 1;
	cluster_injection_armed_count = 1;
	fake_acquire_entry_handoff_armed = true;

	if (sigsetjmp(ut_ereport_jump, 1) == 0) {
		ut_ereport_jump_armed = true;
		acquired = cluster_pcm_lock_acquire_buffer(&buf, PCM_LOCK_MODE_X, &retry_denied);
		ut_ereport_jump_armed = false;
	} else {
		ut_ereport_jump_armed = false;
		escaped_error = true;
	}

	UT_ASSERT(!escaped_error);
	UT_ASSERT(acquired);
	UT_ASSERT(!retry_denied);
	UT_ASSERT_EQ(fake_local_x_transfer_count, 1);
	UT_ASSERT_EQ(fake_local_x_transfer_holder, 1);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ((int)after.state, (int)PCM_STATE_X);
	UT_ASSERT_EQ(after.x_holder_node, 1);
	UT_ASSERT_EQ((long)after.s_holders_bitmap, 0L);
	UT_ASSERT_EQ(memcmp(&fake_local_x_transfer_expected, &after, sizeof(after)), 0);
}

UT_TEST(test_pcm_acquire_buffer_routes_unchanged_remote_x_with_exact_authority)
{
	BufferTag tag = make_tag(98);
	BufferDesc buf;
	PcmAuthoritySnapshot before;
	PcmAuthoritySnapshot after;
	bool retry_denied = false;

	reset_fake_pcm_runtime(4);
	cluster_gcs_block_local_cache = true;
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ((int)cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_X_TO_N_RELEASE, 0), 1);
	UT_ASSERT_EQ((int)cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_X, 1), 1);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &before));

	memset(&buf, 0, sizeof(buf));
	buf.tag = tag;
	fake_local_x_transfer_count = 0;
	fake_local_x_transfer_holder = -1;

	UT_ASSERT(cluster_pcm_lock_acquire_buffer(&buf, PCM_LOCK_MODE_X, &retry_denied));
	UT_ASSERT(!retry_denied);
	UT_ASSERT_EQ(fake_local_x_transfer_count, 1);
	UT_ASSERT_EQ(fake_local_x_transfer_holder, 1);
	UT_ASSERT_EQ(memcmp(&fake_local_x_transfer_expected, &before, sizeof(before)), 0);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ(memcmp(&after, &before, sizeof(before)), 0);
}

UT_TEST(test_pcm_x_transfer_commit_is_exact_and_late_reply_safe)
{
	BufferTag tag = make_tag(99);
	PcmAuthoritySnapshot expected;
	PcmAuthoritySnapshot stale;
	PcmAuthoritySnapshot after;
	PcmAuthoritySnapshot committed;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ((int)cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_X_TO_N_RELEASE, 0), 1);
	UT_ASSERT_EQ((int)cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_X, 1), 1);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &expected));
	UT_ASSERT(cluster_pcm_lock_authority_matches(tag, &expected));

	stale = expected;
	stale.transition_count++;
	UT_ASSERT_EQ(cluster_pcm_lock_master_take_x_after_transfer(tag, &stale, (XLogRecPtr)0x1234,
															   InvalidScn, 1, 44, 900, 12),
				 PCM_X_TRANSFER_COMMIT_STALE);
	stale = expected;
	stale.master_holder.request_id++;
	UT_ASSERT_EQ(cluster_pcm_lock_master_take_x_after_transfer(tag, &stale, (XLogRecPtr)0x1234,
															   InvalidScn, 1, 44, 900, 12),
				 PCM_X_TRANSFER_COMMIT_STALE);
	stale = expected;
	stale.master_holder.cluster_epoch++;
	UT_ASSERT_EQ(cluster_pcm_lock_master_take_x_after_transfer(tag, &stale, (XLogRecPtr)0x1234,
															   InvalidScn, 1, 44, 900, 12),
				 PCM_X_TRANSFER_COMMIT_STALE);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ(memcmp(&after, &expected, sizeof(expected)), 0);

	UT_ASSERT_EQ(cluster_pcm_lock_master_take_x_after_transfer(tag, &expected, (XLogRecPtr)0x1234,
															   InvalidScn, 1, 44, 900, 12),
				 PCM_X_TRANSFER_COMMIT_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &committed));
	UT_ASSERT_EQ((int)committed.state, (int)PCM_STATE_X);
	UT_ASSERT_EQ(committed.x_holder_node, 0);
	UT_ASSERT_EQ((long)committed.s_holders_bitmap, 0L);
	UT_ASSERT_EQ(committed.pending_x_requester_node, -1);
	UT_ASSERT_EQ((long)committed.master_holder.node_id, 0L);
	UT_ASSERT_EQ((long)committed.master_holder.procno, 44L);
	UT_ASSERT_EQ((uint64)committed.master_holder.cluster_epoch, (uint64)12);
	UT_ASSERT_EQ((uint64)committed.master_holder.request_id, (uint64)900);
	UT_ASSERT_EQ((uint64)committed.transition_count, (uint64)expected.transition_count + 1);

	/* A duplicate/late reply carries the displaced remote-X token. */
	UT_ASSERT_EQ(cluster_pcm_lock_master_take_x_after_transfer(tag, &expected, (XLogRecPtr)0x1234,
															   InvalidScn, 1, 44, 900, 12),
				 PCM_X_TRANSFER_COMMIT_STALE);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ(memcmp(&after, &committed, sizeof(committed)), 0);
}


UT_TEST(test_pcm_dead_node_cleanup_drops_holder_records)
{
	BufferTag stag = make_tag(94);
	BufferTag xtag = make_tag(95);
	uint32 s_bitmap;

	reset_fake_pcm_runtime(4);
	fake_cssd_dead_node = -1;

	/* Dead node 2 was the first S holder, so master_holder points at it. */
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(stag, PCM_LOCK_MODE_S);
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(stag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(cluster_pcm_master_holder_node_by_tag(stag), 2);
	UT_ASSERT(!cluster_pcm_lock_clean_leave_verify_no_leftover(2));

	cluster_node_id = 2;
	cluster_pcm_lock_acquire(xtag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(xtag), (int)PCM_LOCK_MODE_X);

	UT_ASSERT_EQ((uint64)cluster_pcm_lock_cleanup_on_node_dead(2), (uint64)2);

	s_bitmap = cluster_pcm_lock_query_s_holders_bitmap(stag);
	UT_ASSERT_EQ((int)(s_bitmap & (1u << 2)), 0);
	UT_ASSERT((s_bitmap & (1u << 1)) != 0);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(stag), (int)PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(cluster_pcm_master_holder_node_by_tag(stag), 1);

	UT_ASSERT_EQ((int)cluster_pcm_lock_query(xtag), (int)PCM_LOCK_MODE_N);
	UT_ASSERT(cluster_pcm_lock_clean_leave_verify_no_leftover(2));
	UT_ASSERT((fake_cv_broadcast_count) >= (1));

	/* Idempotent: a repeated dead-sweep pass has nothing left to clean. */
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_cleanup_on_node_dead(2), (uint64)0);

	/* spec-4.6a D12 (r2-P1-3) pending-X form: a dead requester's parked X
	 * intent is cleared by the companion HC124 sweep the same dead-sweep hook
	 * drives, and the sweep is idempotent too. */
	cluster_pcm_lock_set_pending_x(stag, 2, 1234);
	UT_ASSERT_EQ(cluster_pcm_lock_query_pending_x_requester(stag), 2);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_clear_pending_x_for_node(2), (uint64)1);
	UT_ASSERT_EQ(cluster_pcm_lock_query_pending_x_requester(stag), -1);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_clear_pending_x_for_node(2), (uint64)0);

	/* GCS-race round-2 additional hardening: identity-safe compare-and-
	 * clear.  A mismatched identity must NOT wipe another requester's
	 * pending-X (the starvation guard a newer writer relies on); the
	 * matching identity clears exactly once. */
	cluster_pcm_lock_set_pending_x(stag, 2, 1234);
	UT_ASSERT_EQ(cluster_pcm_lock_clear_pending_x_if(stag, 3), false);
	UT_ASSERT_EQ(cluster_pcm_lock_query_pending_x_requester(stag), 2);
	UT_ASSERT_EQ(cluster_pcm_lock_clear_pending_x_if(stag, 2), true);
	UT_ASSERT_EQ(cluster_pcm_lock_query_pending_x_requester(stag), -1);
	UT_ASSERT_EQ(cluster_pcm_lock_clear_pending_x_if(stag, 2), false);
}

UT_TEST(test_pcm_authority_snapshot_is_one_entry_lock_view)
{
	BufferTag tag = make_tag(97);
	PcmAuthoritySnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_set_pending_x(tag, 3, 0x1234);

	memset(&snapshot, 0x7f, sizeof(snapshot));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &snapshot));
	UT_ASSERT_EQ((int)snapshot.state, (int)PCM_STATE_S);
	UT_ASSERT_EQ(snapshot.x_holder_node, -1);
	UT_ASSERT_EQ(snapshot.s_holders_bitmap, (uint32)((1u << 1) | (1u << 2)));
	UT_ASSERT_EQ(snapshot.master_holder.node_id, (uint32)1);
	UT_ASSERT_EQ(snapshot.pending_x_requester_node, 3);
	UT_ASSERT_EQ(snapshot.pending_x_since_lsn, (uint64)0x1234);
	UT_ASSERT(snapshot.transition_count > 0);
}

UT_TEST(test_pcm_r4_route_snapshot_co_samples_master_generation_and_watermark)
{
	BufferTag tag = make_tag(98);
	PcmAuthoritySnapshot snapshot;
	uint64 master_generation = 0;
	SCN expected_page_scn = InvalidScn;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	ut_lms_master_generation = (UINT64_C(23) << 32) | UINT64_C(7);
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_pi_watermark_scn_advance(tag, (SCN)0x6600,
										 CLUSTER_PCM_WM_SRC_REDECLARE, 1, 88, 23);

	UT_ASSERT(cluster_pcm_lock_r4_route_snapshot(tag, &snapshot, &master_generation,
											 &expected_page_scn));
	UT_ASSERT_EQ((int)snapshot.state, (int)PCM_STATE_S);
	UT_ASSERT_EQ(snapshot.master_holder.node_id, (uint32)1);
	UT_ASSERT(snapshot.transition_count > 0);
	UT_ASSERT_EQ(master_generation, (UINT64_C(23) << 32) | UINT64_C(7));
	UT_ASSERT_EQ((uint64)expected_page_scn, UINT64_C(0x6600));
}

UT_TEST(test_pcm_queue_pending_x_reservation_never_overwrites_another_node)
{
	BufferTag tag = make_tag(101);
	PcmAuthoritySnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	/* Both producers lazily create the canonical N authority for a cold tag. */
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 2, 0x1111), PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &snapshot));
	UT_ASSERT_EQ((int)snapshot.state, (int)PCM_STATE_N);
	UT_ASSERT(cluster_pcm_lock_queue_pending_x_exact(tag, 2, 0x1111));
	/* A delayed same-node legacy clear cannot erase a queue-kind claim. */
	UT_ASSERT(!cluster_pcm_lock_clear_pending_x_if(tag, 2));
	UT_ASSERT(cluster_pcm_lock_queue_pending_x_exact(tag, 2, 0x1111));
	UT_ASSERT(cluster_pcm_lock_clear_queue_pending_x_exact(tag, 2, 0x1111));

	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);

	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 2, 0x1111), PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &snapshot));
	UT_ASSERT_EQ(snapshot.pending_x_requester_node, 2);
	UT_ASSERT(cluster_pcm_lock_queue_pending_x_exact(tag, 2, 0x1111));

	/* Legacy producers obey the same idle-only rule, including same-node. */
	UT_ASSERT_EQ(cluster_pcm_lock_set_pending_x(tag, 3, 0x1212), PCM_PENDING_X_RESERVE_OCCUPIED);
	UT_ASSERT_EQ(cluster_pcm_lock_set_pending_x(tag, 2, 0x1313), PCM_PENDING_X_RESERVE_OCCUPIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &snapshot));
	UT_ASSERT_EQ(snapshot.pending_x_requester_node, 2);
	UT_ASSERT(cluster_pcm_lock_queue_pending_x_exact(tag, 2, 0x1111));

	/* A different queue head cannot overwrite the live barrier. */
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 0x2222),
				 PCM_PENDING_X_RESERVE_OCCUPIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &snapshot));
	UT_ASSERT_EQ(snapshot.pending_x_requester_node, 2);
	UT_ASSERT(cluster_pcm_lock_queue_pending_x_exact(tag, 2, 0x1111));

	/* A same-node legacy round is not ticket-exact replay proof. */
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 2, 0x3333),
				 PCM_PENDING_X_RESERVE_OCCUPIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &snapshot));
	UT_ASSERT_EQ(snapshot.pending_x_requester_node, 2);
	UT_ASSERT(cluster_pcm_lock_queue_pending_x_exact(tag, 2, 0x1111));

	UT_ASSERT(cluster_pcm_lock_clear_queue_pending_x_exact(tag, 2, 0x1111));
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 0x4444), PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &snapshot));
	UT_ASSERT_EQ(snapshot.pending_x_requester_node, 3);
	UT_ASSERT(cluster_pcm_lock_queue_pending_x_exact(tag, 3, 0x4444));
	/* A replay of the old release cannot erase the successor cookie. */
	UT_ASSERT(!cluster_pcm_lock_clear_queue_pending_x_exact(tag, 2, 0x1111));
	UT_ASSERT(cluster_pcm_lock_queue_pending_x_exact(tag, 3, 0x4444));
	UT_ASSERT(cluster_pcm_lock_clear_queue_pending_x_exact(tag, 3, 0x4444));

	tag = make_tag(102);
	UT_ASSERT_EQ(cluster_pcm_lock_set_pending_x(tag, 2, 0x5555), PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &snapshot));
	UT_ASSERT_EQ((int)snapshot.state, (int)PCM_STATE_N);
	UT_ASSERT(cluster_pcm_lock_clear_pending_x_if(tag, 2));

	reset_fake_pcm_runtime(1);
	UT_ASSERT_EQ(cluster_pcm_lock_set_pending_x(make_tag(103), 2, 0x6666),
				 PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(make_tag(104), 3, 0x7777),
				 PCM_PENDING_X_RESERVE_NO_CAPACITY);
}

/* P0-25: pending-X is an admission barrier, not only an advisory wire gate.
 * The final decision and the S-holder bitmap mutation share entry_lock, so an
 * N->S request that raced past an earlier handler preflight cannot publish a
 * new holder after the queue writer claimed the tag.  An already-recorded S
 * holder may re-enter without changing the authority bytes. */
UT_TEST(test_pcm_pending_x_blocks_new_remote_s_holder_atomically)
{
	BufferTag tag = make_tag(110);
	PcmAuthoritySnapshot before;
	PcmAuthoritySnapshot after;

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 9010), PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &before));
	UT_ASSERT_EQ((int)before.state, (int)PCM_STATE_N);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_N_TO_S, 1),
				 PCM_GCS_TRANSITION_PENDING_X);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ((int)after.state, (int)PCM_STATE_N);
	UT_ASSERT_EQ(after.s_holders_bitmap, (uint32)0);
	UT_ASSERT_EQ(after.transition_count, before.transition_count);

	UT_ASSERT(cluster_pcm_lock_clear_queue_pending_x_exact(tag, 3, 9010));
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 1));
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(tag), (uint32)(1u << 1));

	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 9011), PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &before));
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 1));
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_N_TO_S, 2),
				 PCM_GCS_TRANSITION_PENDING_X);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ(after.s_holders_bitmap, before.s_holders_bitmap);
	UT_ASSERT_EQ(after.transition_count, before.transition_count);

	UT_ASSERT(cluster_pcm_lock_clear_queue_pending_x_exact(tag, 3, 9011));
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 2));
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(tag), (uint32)((1u << 1) | (1u << 2)));
}

/* P0-25 local-master mirror: an existing local S holder re-enters immediately,
 * while a different local node waits until the queue cookie is cleared.  The
 * CV callback deterministically performs that clear in this single-threaded
 * harness, proving both no pre-clear bitmap publication and post-clear liveness. */
UT_TEST(test_pcm_pending_x_blocks_new_local_s_holder_until_clear)
{
	BufferTag tag = make_tag(111);

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 9012), PCM_PENDING_X_RESERVE_OK);

	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(fake_cv_sleep_count, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(tag), (uint32)(1u << 0));

	fake_cv_wake_pending_x_clear.tag = tag;
	fake_cv_wake_pending_x_clear.requester_node = 3;
	fake_cv_wake_pending_x_clear.ticket_id = 9012;
	fake_cv_wake_pending_x_clear.armed = true;
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(fake_cv_sleep_count, 1);
	UT_ASSERT_EQ((int)fake_cv_sleep_wait_event, (int)WAIT_EVENT_PCM_COMPATIBLE_STATE_WAIT);
	UT_ASSERT_EQ(cluster_pcm_lock_query_pending_x_requester(tag), -1);
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(tag), (uint32)((1u << 0) | (1u << 1)));

	/* A reader woken by FINAL may observe the newly granted X and sleep again.
	 * The later holder X->S downgrade must wake it when S becomes compatible. */
	tag = make_tag(112);
	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_X_TO_S_DOWNGRADE, 0));
	UT_ASSERT_EQ(fake_cv_broadcast_count, 1);
}

UT_TEST(test_clean_page_xfer_arm_is_one_shot)
{
	/* Default disarmed. */
	cluster_pcm_clean_page_xfer_arm(false);
	UT_ASSERT_EQ(cluster_pcm_clean_page_xfer_is_armed() ? 1 : 0, 0);
	/* consume() on a disarmed flag returns false and stays disarmed. */
	UT_ASSERT_EQ(cluster_pcm_clean_page_xfer_consume() ? 1 : 0, 0);

	/* arm → peek true (non-destructive) → peek still true. */
	cluster_pcm_clean_page_xfer_arm(true);
	UT_ASSERT_EQ(cluster_pcm_clean_page_xfer_is_armed() ? 1 : 0, 1);
	UT_ASSERT_EQ(cluster_pcm_clean_page_xfer_is_armed() ? 1 : 0, 1);

	/* consume → returns true ONCE → flag now cleared (single-shot). */
	UT_ASSERT_EQ(cluster_pcm_clean_page_xfer_consume() ? 1 : 0, 1);
	UT_ASSERT_EQ(cluster_pcm_clean_page_xfer_is_armed() ? 1 : 0, 0);
	/* A second consume sees no leak. */
	UT_ASSERT_EQ(cluster_pcm_clean_page_xfer_consume() ? 1 : 0, 0);

	/* Idempotent re-arm/disarm. */
	cluster_pcm_clean_page_xfer_arm(true);
	cluster_pcm_clean_page_xfer_arm(false);
	UT_ASSERT_EQ(cluster_pcm_clean_page_xfer_is_armed() ? 1 : 0, 0);
}

int
main(void)
{
	UT_PLAN(196);
	UT_RUN(test_pcm_lock_mode_constant_aliases_match_pcm_state);
	UT_RUN(test_pcm_lock_transition_count_is_9);
	UT_RUN(test_pcm_lock_transition_enum_values_are_1_to_9);
	UT_RUN(test_pcm_grd_max_entries_default_is_minus_one);
	UT_RUN(test_pcm_buffer_desc_invariants_hold_at_stage_2_30);
	UT_RUN(test_pcm_lock_module_init_symbol_is_callable);
	UT_RUN(test_pcm_trans_1_n_to_s_validator_accepts);
	UT_RUN(test_pcm_trans_2_n_to_x_validator_accepts);
	UT_RUN(test_pcm_trans_3_s_to_x_upgrade_validator_accepts);
	UT_RUN(test_pcm_trans_4_x_to_s_downgrade_validator_accepts);
	UT_RUN(test_pcm_trans_5_x_to_n_downgrade_validator_accepts);
	UT_RUN(test_pcm_trans_6_x_to_n_release_validator_accepts);
	UT_RUN(test_pcm_trans_7_s_to_n_invalidate_validator_accepts);
	UT_RUN(test_pcm_trans_8_s_to_n_release_validator_accepts);
	UT_RUN(test_pcm_trans_9_cleanout_validator_reachable_but_apply_fail_closed);
	UT_RUN(test_pcm_illegal_transition_validator_rejects);
	UT_RUN(test_pcm_disable_path_counters_return_zero);
	UT_RUN(test_pcm_grd_entry_lifecycle_link_surface);
	UT_RUN(test_pcm_per_entry_lwlock_independence_link_surface);
	UT_RUN(test_pcm_pi_bitmap_atomic_accessor_linkable);
	UT_RUN(test_pcm_counter_observability_9_accessors_linkable);
	UT_RUN(test_pcm_real_shared_s_holders_release_independently);
	UT_RUN(test_pcm_real_x_release_and_downgrade_require_owner);
	UT_RUN(test_pcm_real_upgrade_requires_single_s_holder);
	UT_RUN(test_pcm_real_summary_counts_live_entries);
	UT_RUN(test_pcm_grd_entry_abi_includes_resource_x_executor_state);
	UT_RUN(test_pcm_d2_entry_ref_is_pinned_and_binding_generation_exact);
	UT_RUN(test_pcm_d2_cv_wait_holds_pin_and_exact_wait_reference);
	UT_RUN(test_pcm_d2_master_state_uses_exact_pinned_registry_slot);
	UT_RUN(test_pcm_d3_retire_eligibility_is_one_exact_closed_table);
	UT_RUN(test_pcm_d4_tombstone_reuse_is_generation_exact);
	UT_RUN(test_pcm_d4_reused_binding_first_grant_keeps_generation_lineage);
	UT_RUN(test_pcm_d4_displaced_tombstone_preserves_authority_floor);
	UT_RUN(test_pcm_d5_capacity_retry_reclaims_one_terminal_binding);
	UT_RUN(test_pcm_d5_bounded_reclaim_never_exceeds_probe_budget);
	UT_RUN(test_pcm_d5_last_holder_release_fast_retires_exact_binding);
	UT_RUN(test_pcm_d5_remote_master_s_eviction_fast_retires_closed_requester_projection);
	UT_RUN(test_pcm_d5_remote_master_s_eviction_never_retires_rebound_projection);
	UT_RUN(test_pcm_d7_remote_s_eviction_closes_pending_x_to_s_master_state);
	UT_RUN(test_pcm_d5_durable_pi_discard_fast_retires_exact_binding);
	UT_RUN(test_pcm_d5_resource_x_terminal_release_fast_retires_exact_binding);
	UT_RUN(test_pcm_d5_lifecycle_stats_are_exact_not_legacy_aliases);
	UT_RUN(test_pcm_d1_bootstrap_no_capacity_is_pre_mutation_backpressure);
	UT_RUN(test_pcm_d5_lmon_soft_threshold_reclaims_one_bounded_tick);
	UT_RUN(test_pcm_d5_reclaim_stops_while_gate_is_not_open);
	UT_RUN(test_resource_x_executor_t1_t2_t3_is_exact_and_blocks_no_progress);
	UT_RUN(test_resource_x_exact_t3_retires_without_physical_release_and_admits_successor);
	UT_RUN(test_resource_x_retired_floor_rejects_late_generation_without_touching_successor);
	UT_RUN(test_resource_x_probe_t3_wait_window_observes_retired_duplicate);
	UT_RUN(test_resource_x_executor_admission_is_formation_exact_and_balanced);
	UT_RUN(test_resource_x_native_gate_snapshot_and_exact_fail_closed);
	UT_RUN(test_resource_x_reconfig_freeze_closes_activation_drains_and_thaws_empty);
	UT_RUN(test_resource_x_reconfig_pending_freeze_binds_only_published_formation);
	UT_RUN(test_resource_x_cutover_formation_pair_is_native_not_r4_arithmetic);
	UT_RUN(test_resource_x_reconfig_pending_sweep_waits_for_registered_inflight_to_retire);
	UT_RUN(test_resource_x_reconfig_pending_sweep_classifies_unowned_old_active_as_orphan);
	UT_RUN(test_resource_x_reconfig_nested_and_generation_exhaustion_fail_closed);
	UT_RUN(test_resource_x_reconfig_cursor_covers_seventeen_slots_in_bounded_calls);
	UT_RUN(test_resource_x_same_token_zero_and_clean_completion_proofs_are_exact);
	UT_RUN(test_resource_x_same_token_proofs_reject_postscan_entry_mutation);
	UT_RUN(test_resource_x_reconfig_t1_orphan_blocks_once_and_retains_evidence);
	UT_RUN(test_resource_x_reconfig_half_join_without_active_is_retained_orphan);
	UT_RUN(test_resource_x_reconfig_t2_neutralizes_outside_entry_lock_then_blocks_orphan);
	UT_RUN(test_resource_x_reconfig_t2_newer_sidecar_survives_and_blocks_orphan);
	UT_RUN(test_resource_x_native_head_is_exact_s_admission_barrier);
	UT_RUN(test_resource_x_bootstrap_receipt_replays_and_consumes_exactly);
	UT_RUN(test_resource_x_bootstrap_receipt_drift_invalidates_but_keeps_floor);
	UT_RUN(test_resource_x_bootstrap_terminal_retire_clears_binding_not_floor);
	UT_RUN(test_resource_x_bootstrap_dispatches_one_current_base_at_a_time);
	UT_RUN(test_resource_x_bootstrap_r8_clears_old_binding_not_attempt_floor);
	UT_RUN(test_resource_x_bootstrap_round_fans_in_and_retries_same_attempt);
	UT_RUN(test_resource_x_node_fanin_ignores_caller_deadline_and_retry_slice);
	UT_RUN(test_resource_x_unbound_claim_wait_cannot_bypass_head_expiry);
	UT_RUN(test_resource_x_ack_advances_lease_but_replays_do_not);
	UT_RUN(test_resource_x_exact_ack_sample_before_retry_is_not_failure);
	UT_RUN(test_resource_x_head_budget_and_owned_progress_are_independent);
	UT_RUN(test_resource_x_round_wait_rechecks_progress_and_clips_head_lease);
	UT_RUN(test_resource_x_reply_wait_metrics_are_shared_and_bucket_exact);
	UT_RUN(test_resource_x_expired_follower_cannot_cancel_live_head);
	UT_RUN(test_resource_x_post_ack_rejected_follower_preserves_shared_head);
	UT_RUN(test_resource_x_t1_install_claim_binds_only_exact_physical_reservation);
	UT_RUN(test_resource_x_direct_claim_joins_unbound_head_only_after_complete_beb);
	UT_RUN(test_resource_x_requester_round_blocks_local_s_only_until_x_cached);
	UT_RUN(test_resource_x_bootstrap_expired_round_allows_only_fresh_acquisition);
	UT_RUN(test_resource_x_bootstrap_round_binds_exact_direct_init_reservation);
	UT_RUN(test_resource_x_pre_assert_authority_drift_discards_exact_round_only);
	UT_RUN(test_resource_x_direct_init_observer_is_join_only_and_keeps_round_deadline);
	UT_RUN(test_resource_x_terminal_local_owner_serializes_recycle_and_revoke);
		UT_RUN(test_resource_x_bootstrap_round_waits_only_for_exact_target_install);
		UT_RUN(test_resource_x_bootstrap_round_waits_across_exact_post_t3_cover_window);
		UT_RUN(test_resource_x_target_install_continuation_classifies_exact_attempt);
		UT_RUN(test_resource_x_target_install_stable_direct_init_i0_t2_is_closed);
		UT_RUN(test_resource_x_target_wait_old_lease_is_only_a_reprobe_hint);
		UT_RUN(test_resource_x_target_install_wait_uses_receipt_and_fixed_deadline);
		UT_RUN(test_resource_x_target_install_preuse_successor_never_grants_old_receipt);
		UT_RUN(test_resource_x_target_install_capture_during_revoke_retries_same_token_n);
		UT_RUN(test_resource_x_bootstrap_terminal_cover_accepts_remote_master_requester_projection);
	UT_RUN(test_resource_x_cached_x_to_s_commit_clears_only_exact_terminal_cover);
	UT_RUN(test_resource_x_bootstrap_direct_init_cached_x_consumes_same_round_t3_handoff);
	UT_RUN(test_resource_x_cached_x_eviction_prepares_and_commits_release);
	UT_RUN(test_resource_x_local_master_release_keeps_cached_cover_until_commit);
	UT_RUN(test_resource_x_local_n_without_evicting_owner_is_post_mutation_ambiguity);
	UT_RUN(test_resource_x_terminal_remote_holder_binds_exact_final_authority);
	UT_RUN(test_resource_x_adapter_adopts_only_exact_pristine_legacy_base);
	UT_RUN(test_resource_x_adapter_head_rebinds_only_before_assert);
	UT_RUN(test_resource_x_settled_retirement_tombstone_replays_and_frees_live_slot);
	UT_RUN(test_resource_x_tombstone_lineage_drift_remains_terminal);
	UT_RUN(test_pcm_protocol_debt_projection_rejects_settled_without_cached_or_pi);
	UT_RUN(test_resource_x_remote_lane0_settlement_retires_only_after_exact_source_ack);
	UT_RUN(test_resource_x_adapter_successor_samples_only_exact_canonical_base);
	UT_RUN(test_resource_x_master_arms_exact_block_to_n_intent_per_holder);
	UT_RUN(test_resource_x_requester_absent_single_s_selects_only_holder);
	UT_RUN(test_resource_x_requester_absent_multi_s_prefers_master_local);
	UT_RUN(test_resource_x_requester_absent_multi_s_uses_lowest_stable_peer);
	UT_RUN(test_resource_x_requester_present_does_not_select_remote_s_carrier);
	UT_RUN(test_resource_x_duplicate_assertion_preserves_selected_s_carrier);
	UT_RUN(test_resource_x_existing_selected_s_intent_conflict_fails_closed);
	UT_RUN(test_resource_x_source_pair_rejects_status_image_mode_mismatch);
	UT_RUN(test_resource_x_s_partial_source_polarity_is_rejected_by_consumer);
	UT_RUN(test_resource_x_prepared_s_source_retains_one_exact_matching_pair);
	UT_RUN(test_resource_x_selected_s_proof_and_noncarrier_status_commit_once);
	UT_RUN(test_resource_x_selected_s_status_only_is_rejected);
	UT_RUN(test_resource_x_noncarrier_s_remote_proof_is_rejected);
	UT_RUN(test_resource_x_master_exact_replay_redrives_unanswered_blocker);
	UT_RUN(test_resource_x_holder_retains_status_before_s_to_n);
	UT_RUN(test_resource_x_master_remote_proof_is_retained_not_inferred);
	UT_RUN(test_resource_x_blocked_to_n_exact_clear_rejects_generation_drift);
	UT_RUN(test_resource_x_type18_wire_decode_drives_master_exact_apply);
	UT_RUN(test_resource_x_type18_releases_direct_old_x_request_and_tracks_exact_generation);
	UT_RUN(test_resource_x_direct_n_origin_offset_advances_blocker_and_grant);
	UT_RUN(test_resource_x_requester_join_accepts_either_order_and_never_overwrites);
	UT_RUN(test_resource_x_requester_join_accepts_exact_base_carrier_final_lineage);
	UT_RUN(test_resource_x_requester_join_uses_live_bootstrap_base_not_binding_floor);
	UT_RUN(test_resource_x_requester_join_accepts_s_carrier_either_order_and_rejects_mode_drift);
	UT_RUN(test_resource_x_requester_join_accepts_multi_blocker_authority_span);
	UT_RUN(test_resource_x_requester_join_creates_fresh_local_entry_before_t1);
	UT_RUN(test_resource_x_remote_terminal_settles_o1_once_and_keeps_first_times);
	UT_RUN(test_resource_x_requester_floors_keep_authority_and_target_axes_distinct);
	UT_RUN(test_resource_x_requester_join_gates_both_floors_before_successor);
	UT_RUN(test_resource_x_x_source_defers_self_master_grd_transition_to_ingress);
	UT_RUN(test_resource_x_source_settlement_prepare_closes_total_cover_table);
	UT_RUN(test_resource_x_s_predecessor_retained_pair_blocks_local_bootstrap);
	UT_RUN(test_resource_x_predecessor_cv_observes_settlement_without_a_head);
	UT_RUN(test_resource_x_s_predecessor_cancels_only_unbound_preassert_round);
	UT_RUN(test_resource_x_s_predecessor_cancellation_supersedes_old_wait);
	UT_RUN(test_resource_x_source_settlement_accepts_multi_blocker_authority_span);
	UT_RUN(test_resource_x_source_settlement_drains_only_the_exact_retained_pair);
	UT_RUN(test_resource_x_source_settlement_debt_participates_in_same_token_r8_r10);
	UT_RUN(test_resource_x_holder_pair_drain_allows_master_requester_colocation);
	UT_RUN(test_resource_x_remote_master_uses_exact_installed_holder_lineage);
	UT_RUN(test_resource_x_remote_master_retains_exact_current_x_without_local_authority);
	UT_RUN(test_resource_x_prepared_retain_uses_terminal_x_not_local_reuse_order);
	UT_RUN(test_resource_x_self_master_drop_keeps_local_grd_authority_domain);
	UT_RUN(test_resource_x_master_local_and_durable_proofs_are_exact_and_closed);
	UT_RUN(test_resource_x_master_multi_s_blockers_advance_exact_frontier);
	UT_RUN(test_resource_x_head_accepts_exact_blockers_after_prior_pcm_s_churn);
	UT_RUN(test_resource_x_head_rejects_uncovered_pcm_transition_drift);
	UT_RUN(test_resource_x_local_settlement_reconciles_exact_requester_prestate);
	UT_RUN(test_resource_x_master_settlement_release_starts_fifo_successor);
	UT_RUN(test_resource_x_release_without_successor_advances_canonical_base);
	UT_RUN(test_resource_x_reclaim_nonhead_preserves_survivor_fifo);
	UT_RUN(test_resource_x_reclaim_safe_head_starts_exact_successor);
	UT_RUN(test_resource_x_reclaim_post_grant_preserves_orphan_evidence);
	UT_RUN(test_resource_x_reconfig_sweep_drives_exact_dead_requester_reclaim);
	UT_RUN(test_resource_x_intent_retains_logical_owner_across_physical_scarcity);
	UT_RUN(test_resource_x_intent_sparse_probe_rediscovers_exact_rearm);
	UT_RUN(test_resource_x_duplicate_assert_redrives_completed_grant_delivery);
	UT_RUN(test_pcm_grd_convert_queue_placeholder_remains_null);
	UT_RUN(test_pcm_real_wait_event_call_sites_are_exercised);
	UT_RUN(test_pcm_H1_same_node_s_refcount_increments);
	UT_RUN(test_pcm_H2_last_s_release_transitions_to_n);
	UT_RUN(test_pcm_H2b_same_node_s_residency_upgrades_to_x);
	UT_RUN(test_pcm_H3_incompatible_x_waits_and_wakes);
	UT_RUN(test_pcm_H4_release_broadcasts_only_on_state_change);
	UT_RUN(test_pcm_d2_mode_covers_truth_table);
	UT_RUN(test_pcm_d3_requester_is_holder_strict);
	UT_RUN(test_pcm_d4_other_live_holder_gate);
	UT_RUN(test_pcm_b_local_master_remote_x_holder_fail_closed);
	UT_RUN(test_pcm_d1_recovering_gate_fail_closed);
	UT_RUN(test_pcm_d2_rebuild_from_redeclare);
	UT_RUN(test_pcm_d3_not_double_x);
	UT_RUN(test_pcm_wm_prov_table_keeps_last_advance);
	UT_RUN(test_pcm_acquire_buffer_local_s_nonholder_registers_s_then_upgrades);
	UT_RUN(test_pcm_acquire_buffer_local_s_retries_while_pcm_x_head_is_active);
	UT_RUN(test_pcm_acquire_buffer_s_bootstrap_revalidates_remote_x);
	UT_RUN(test_pcm_acquire_buffer_s_revalidates_remote_x_after_precheck);
	UT_RUN(test_pcm_acquire_buffer_revalidates_remote_x_after_precheck);
	UT_RUN(test_pcm_acquire_buffer_routes_unchanged_remote_x_with_exact_authority);
	UT_RUN(test_pcm_x_transfer_commit_is_exact_and_late_reply_safe);
	UT_RUN(test_pcm_dead_node_cleanup_drops_holder_records);
	UT_RUN(test_pcm_authority_snapshot_is_one_entry_lock_view);
	UT_RUN(test_pcm_r4_route_snapshot_co_samples_master_generation_and_watermark);
	UT_RUN(test_pcm_queue_pending_x_reservation_never_overwrites_another_node);
	UT_RUN(test_pcm_pending_x_blocks_new_remote_s_holder_atomically);
	UT_RUN(test_pcm_pending_x_blocks_new_local_s_holder_until_clear);
	UT_RUN(test_clean_page_xfer_arm_is_one_shot);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
