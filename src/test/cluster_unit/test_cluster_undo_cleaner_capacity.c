/*-------------------------------------------------------------------------
 * test_cluster_undo_cleaner_capacity.c
 *    Actual capacity wait/context and worker lifecycle regression tests.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_undo_cleaner_capacity.c
 *
 * No replacement of the wait/context or lifecycle decisions. PostgreSQL
 * scheduling and the separately tested CTRC proof are boundary inputs.
 * Resource accessors are extracted verbatim, with fail-if-missing rules.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "lib/ilist.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "test_cluster_capacity_pool_compat.h"
#include "../../backend/cluster/cluster_undo_cleaner.c"

static struct {
	Buffer buffer;
	int32 refcount;
} PrivateRefCountArray[8];
static int32 PrivateRefCountOverflowed;
int NLocBuffer;
int32 *LocalRefCount;
static dlist_head current_active_guards = DLIST_STATIC_INIT(current_active_guards);
static dlist_head Block0OwnedResources = DLIST_STATIC_INIT(Block0OwnedResources);
static uint32 semantic_activation_local_inflight[2][64];
#include "test_cluster_undo_cleaner_holds.inc"

#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();
#include "access/twophase.h"
#include "test_cluster_pgproc_active_types.inc"

BackendType MyBackendType;
PGPROC *MyProc;
int MyProcPid;
bool IsUnderPostmaster;
volatile uint32 CritSectionCount;
volatile uint32 InterruptHoldoffCount;
volatile sig_atomic_t InterruptPending;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
bool cluster_undo_cleaner_enabled = true;
bool cluster_enabled = true;
bool cluster_shared_config = false;
bool cluster_undo_retention_horizon_enabled = true;
int cluster_lmon_main_loop_interval = 2000;
int cluster_node_id = 0;

static PGPROC test_proc;
static LOCALLOCK test_locks[4];
static unsigned test_lock_count, test_lock_depth, test_scan_ends;
static bool test_foreign_lwlock, test_floor, test_fence;
static unsigned prepared, cancelled, slept, signalled, broadcasts;
static uint32 test_wait_event, last_sleep_event;
uint32 *my_wait_event_info = &test_wait_event;
static bool subscribed, early_signal, test_cancel, test_pin_after_probe;
static int test_error_code = ERRCODE_QUERY_CANCELED;
static bool test_lock_wait;
static ClusterCtrcCapacityProbeResult test_proof;
static void (*test_capacity_probe_hook)(void);
static ClusterCapacityReasonV1 test_pool_reason;
static unsigned test_pool_calls;
static void (*test_pool_probe_hook)(void);
extern ClusterCapacityReasonV1
cluster_ctrc_capacity_probe_pool_v1(uint32 segment, SCN horizon, uint64 epoch,
									ClusterCapacityObservationV1 *observation);
#ifdef CLUSTER_CAPACITY_POOL_TEST_API_ABSENT
static bool
capacity_owner_api_absent(unsigned worker pg_attribute_unused(),
						  uint64 *coordinator pg_attribute_unused(),
						  uint64 *generation pg_attribute_unused())
{
	return false;
}
#define cluster_undo_cleaner_capacity_owner_snapshot capacity_owner_api_absent
#endif
static TransactionId test_xid = 100;
static bool test_maintenance_cut, test_ctrc_progress;
static unsigned test_ctrc_passes, test_gc_passes, test_modifier_entries, test_modifier_leaves;
static bool test_gc_complete;
static uint32 test_inventory_max;
static unsigned test_inventory_scans;
static uint64 test_gc_reclaimed;
static ClusterUndoSegTryRecycle test_recycle_result;
static unsigned test_census_begins, test_census_steps, test_reference_reads;
static FullTransactionId test_reference, test_pgproc_min, test_future;
static unsigned test_pgproc_samples;
static bool test_pgproc_invalid;
static bool test_lease_current;
static uint64 test_lease_identity;
static bool test_sample_admission_current;
static uint64 test_sample_admission_identity;
static unsigned test_sample_admission_captures;
static unsigned test_source_steps;
static ClusterSemanticActiveSampleFence test_sample_source, test_first_source, test_last_source;
static bool test_change_admission_after_n;
static bool test_census_busy, test_census_invalid;
static VariableCacheData native_variables;
VariableCache ShmemVariableCache = &native_variables;
static PROC_HDR native_procs;
PROC_HDR *ProcGlobal = &native_procs;
static char native_array_storage[offsetof(ProcArrayStruct, pgprocnos) + 4 * sizeof(int)]
	pg_attribute_aligned(MAXIMUM_ALIGNOF);
static ProcArrayStruct *procArray = (ProcArrayStruct *) native_array_storage;
static PGPROC native_entries[4];
static TransactionId native_xids[4];
static LWLockPadded native_locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = native_locks;
static unsigned native_lock_phase;
static bool native_custom;
bool cluster_undo_record_segment_commit_on_rollover;
int cluster_undo_cleaner_batch_segments = 8;
static ClusterNormalStartState cleaner_start_state = CLUSTER_NORMAL_START_TARGET_READY;
static bool cleaner_recovered_open = true;

ClusterNormalStartState
cluster_semantic_normal_start_state(void)
{
	return cleaner_start_state;
}

bool
cluster_semantic_activation_recovered_open_current(void)
{
	return cleaner_recovered_open;
}

/* The normal-stop state observer is a boundary input here. Its real shared
 * state/identity tests live in test_cluster_normal_stop. The complete actual
 * outer pass above must retain CTRC progress without starting new maintenance. */
bool
cluster_normal_stop_maintenance_cut(void)
{
	return test_maintenance_cut;
}

bool
cluster_ctrc_cleaner_run_pass(void)
{
	test_ctrc_passes++;
	return test_ctrc_progress;
}

/* Recovery and stripe promises remain boundary facts; the native array scan
 * below now runs from the exact original body in every outer-pass case. */
bool
RecoveryInProgress(void)
{
	return false;
}

bool
cluster_xid_stripe_sample_future(FullTransactionId reference, FullTransactionId *future,
							   ClusterXidStripeSampleFence *fence)
{
	if (test_lock_depth != 1 || native_lock_phase != 1
		|| !FullTransactionIdEquals(reference, test_reference))
		abort();
	MemSet(fence, 0, sizeof(*fence));
	fence->incarnation = test_lease_identity;
	*future = FullTransactionIdIsValid(test_future) ? test_future : reference;
	return true;
}

#include "test_cluster_worker_pgproc_sample.inc"

/* The original stripe predicate is executed in test_cluster_xid_stripe_evidence.
 * Here the boundary enforces the same start token and no overlapping locks. */
bool
cluster_xid_stripe_sample_fence_current(const ClusterXidStripeSampleFence *fence)
{
	if (test_lock_depth != 0 || fence == NULL)
		abort();
	return test_lease_current && fence->incarnation == test_lease_identity;
}

/* The actual read-only publisher observers run in active_sample_fence. This
 * boundary enforces capture before N and comparison with the original token. */
bool
cluster_semantic_activation_active_sample_capture(ClusterSemanticActiveSampleFence *out)
{
	if (test_lock_depth != 0 || out == NULL)
		abort();
	test_sample_admission_captures++;
	MemSet(out, 0, sizeof(*out));
	if (!test_sample_admission_current)
		return false;
	*out = test_sample_source;
	out->admission_publication_seq = test_sample_admission_identity;
	return true;
}

bool
cluster_semantic_activation_active_sample_current(const ClusterSemanticActiveSampleFence *expected)
{
	if (test_lock_depth != 0 || expected == NULL)
		abort();
	return test_sample_admission_current && expected->admission_publication_seq != 0
		   && expected->admission_publication_seq == test_sample_admission_identity;
}

static bool test_census_registration_current = true;

/* Actual shared ingress/generation behavior is covered by ctrc_cleaner.
 * This leaf only observes whether the real worker consults that last fence. */
bool
cluster_ctrc_active_scan_registration_current(const ClusterCtrcActiveScan *scan)
{
	if (test_lock_depth != 0 || scan == NULL)
		abort();
	return test_census_registration_current;
}

bool
cluster_ctrc_active_scan_begin(ClusterCtrcActiveScan *scan, FullTransactionId reference)
{
	if (test_lock_depth != 0)
		abort();
	test_census_begins++;
	MemSet(scan, 0, sizeof(*scan));
	scan->reference_full = scan->min_full = reference;
	scan->total_entries = 128;
	scan->status = CTRC_ACTIVE_SCAN_INCOMPLETE;
	return true;
}

void
cluster_ctrc_active_scan_step(ClusterCtrcActiveScan *scan)
{
	if (scan->status != CTRC_ACTIVE_SCAN_INCOMPLETE)
		return;
	if (test_lock_depth != 0 || test_ctrc_passes <= test_census_steps)
		abort();
	test_census_steps++;
	if (test_census_invalid) {
		scan->status = CTRC_ACTIVE_SCAN_INVALID;
		scan->min_full = InvalidFullTransactionId;
	} else if ((scan->lock_busy = test_census_busy) == false) {
		scan->next_index += 64;
		if (scan->next_index == scan->total_entries)
			scan->status = CTRC_ACTIVE_SCAN_COMPLETE;
	}
}

/* Real worker receives the original source snapshot; scanner details are
 * exercised separately by the complete shared-census fixture. */
void
cluster_ctrc_active_scan_step_source(ClusterCtrcActiveScan *scan,
									const ClusterSemanticActiveSampleFence *source)
{
	if (source == NULL || test_lock_depth != 0)
		abort();
	if (test_source_steps++ == 0)
		test_first_source = *source;
	test_last_source = *source;
	cluster_ctrc_active_scan_step(scan);
}

ClusterJoinGateVerdict
cluster_reconfig_self_join_gate_verdict(void)
{
	return CLUSTER_JOIN_GATE_ALLOW;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_modifier_enter(bool writable, ClusterSemanticAdmissionToken *token)
{
	if (!writable || test_lock_depth != 0)
		abort();
	MemSet(token, 0, sizeof(*token));
	test_modifier_entries++;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

bool
cluster_semantic_activation_modifier_recheck(const ClusterSemanticAdmissionToken *token,
											 bool writable)
{
	return token != NULL && writable;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	if (token == NULL || test_lock_depth != 0)
		abort();
	test_modifier_leaves++;
}

bool
cluster_tt_slot_gc_current_pass(SCN horizon, uint64 epoch, ClusterUndoCleanerPassStats *stats)
{
	if (horizon != 1000 || epoch != 13 || stats == NULL || test_lock_depth != 0)
		abort();
	test_gc_passes++;
	stats->shmem_tt_slots_gcd += test_gc_reclaimed;
	/* A finite existing GC refusal exercises the real pass's token release. */
	return test_gc_complete;
}

void
cluster_undo_horizon_note_stall(void)
{}
void
cluster_undo_horizon_note_peer_stale(void)
{}
void
cluster_undo_horizon_note_pass_abort(void)
{}
void
cluster_undo_horizon_note_floor(SCN scn pg_attribute_unused())
{}
const char *
cluster_undo_horizon_stall_reason_name(ClusterUndoHorizonStallReason reason pg_attribute_unused())
{
	return "fixture";
}
uint32
cluster_undo_segment_scan_max_existing(uint8 owner pg_attribute_unused())
{
	return test_inventory_max;
}
uint32
cluster_undo_record_active_segment_id(void)
{
	return 1;
}
uint32
cluster_tt_slot_current_segment(int node pg_attribute_unused())
{
	return 1;
}
bool
cluster_undo_segment_tt_header_scan_pass(uint32 segment, uint8 owner, SCN horizon,
										 ClusterUndoCleanerPassStats *stats)
{
	if (segment < 2 || segment > test_inventory_max || owner != 1 || horizon != 1000)
		abort();
	/* A durable COMMITTED slot remains in an ALREADY/RETAINED segment.
	 * This observation is deliberately unchanged by subsequent scans. */
	stats->header_tt_slots_below_horizon++;
	stats->segments_scanned++;
	test_inventory_scans++;
	return true;
}
void
cluster_undo_segment_advance_committed(uint32 segment pg_attribute_unused())
{
	abort();
}
ClusterUndoSegTryRecycle
cluster_undo_segment_advance_recyclable(uint32 segment, SCN horizon, uint64 epoch)
{
	if (segment < 2 || segment > test_inventory_max || horizon != 1000 || epoch != 13)
		abort();
	return test_recycle_result;
}
bool
errstart(int level pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}
bool
errstart_cold(int level pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}
int
errmsg(const char *format pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *format pg_attribute_unused(), ...)
{
	return 0;
}
void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *func pg_attribute_unused())
{
	abort();
}

void
LWLockInitialize(LWLock *lock, int tranche)
{
	MemSet(lock, 0, sizeof(*lock));
	lock->tranche = tranche;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (test_lock_depth++ != 0)
		abort();
	if (lock == XidGenLock)
	{
		if (mode != LW_SHARED || test_ctrc_passes <= test_pgproc_samples)
			abort();
		native_lock_phase = 1;
		test_pgproc_samples++;
		test_reference_reads++;
		native_variables.nextXid = test_reference;
		if (!native_custom)
		{
			procArray->numProcs = 0;
			if (FullTransactionIdIsValid(test_pgproc_min) || test_pgproc_invalid)
			{
				procArray->numProcs = 1;
				native_xids[0] = test_pgproc_invalid ? 80 : XidFromFullTransactionId(test_pgproc_min);
				pg_atomic_write_u64(&native_entries[0].cluster_top_full_xid_atomic,
					test_pgproc_invalid ? 0 : U64FromFullTransactionId(test_pgproc_min));
			}
		}
	}
	else if (lock == ProcArrayLock)
	{
		if (mode != LW_SHARED || native_lock_phase != 2)
			abort();
		native_lock_phase = 3;
	}
	/* A contended bookkeeping LWLock also clears its wait event. */
	if (test_lock_wait) {
		pgstat_report_wait_start(PG_WAIT_LWLOCK);
		pgstat_report_wait_end();
	}
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	if (test_lock_depth-- != 1)
		abort();
	if (lock == XidGenLock)
	{
		if (native_lock_phase != 1)
			abort();
		native_lock_phase = 2;
		if (test_change_admission_after_n)
			test_sample_admission_identity++;
	}
	else if (lock == ProcArrayLock)
	{
		if (native_lock_phase != 3)
			abort();
		native_lock_phase = 4;
	}
}

void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *context)
{
	if (test_foreign_lwlock || test_lock_depth != 0)
		callback(NULL, LW_EXCLUSIVE, context);
}

HTAB *
GetLockMethodLocalHash(void)
{
	return (HTAB *)test_locks;
}

void
hash_seq_init(HASH_SEQ_STATUS *scan, HTAB *table)
{
	scan->hashp = table;
	scan->curBucket = 0;
}

void *
hash_seq_search(HASH_SEQ_STATUS *scan)
{
	if (scan->curBucket >= test_lock_count)
		return NULL;
	return &test_locks[scan->curBucket++];
}

void
hash_seq_term(HASH_SEQ_STATUS *scan pg_attribute_unused())
{
	test_scan_ends++;
}

TransactionId
GetTopTransactionIdIfAny(void)
{
	return test_xid;
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *found)
{
	*found = false;
	return calloc(1, size);
}

TimestampTz
GetCurrentTimestamp(void)
{
	return 123456;
}

SCN
cluster_undo_retention_horizon(void)
{
	return 1000;
}

int
cluster_undo_horizon_sample_views(ClusterUndoHorizonReportView *views pg_attribute_unused(),
								  int maxviews pg_attribute_unused())
{
	return 0;
}

bool
cluster_undo_horizon_required_members(uint8 *required, uint64 *epoch)
{
	MemSet(required, 0, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	*epoch = 13;
	return test_floor;
}

ClusterUndoHorizonFoldStatus
cluster_undo_horizon_cluster_floor(
	SCN local, const ClusterUndoHorizonReportView *views pg_attribute_unused(),
	int nviews pg_attribute_unused(), const uint8 *required pg_attribute_unused(),
	int32 self pg_attribute_unused(), uint64 epoch, uint64 now pg_attribute_unused(),
	uint32 interval pg_attribute_unused(), ClusterUndoHorizonFloor *out,
	ClusterUndoHorizonStallReason *reason pg_attribute_unused(), int32 *blame pg_attribute_unused())
{
	out->scn = local;
	out->epoch = epoch;
	return CLUSTER_UNDO_HORIZON_FOLD_OK;
}

bool
cluster_undo_horizon_epoch_fence_tripped(uint64 epoch pg_attribute_unused())
{
	return test_fence;
}

ClusterCtrcCapacityProbeResult
cluster_ctrc_capacity_probe_current(uint32 segment, SCN horizon, uint64 epoch,
									ClusterCtrcTxnKeyV1 *continuation)
{
	if (test_lock_depth != 0 || !subscribed || horizon != 1000 || epoch != 13)
		abort();
	continuation->segment_id = segment;
	if (test_capacity_probe_hook != NULL)
		test_capacity_probe_hook();
	if (test_pin_after_probe)
		PrivateRefCountArray[0].refcount = 1;
	if (early_signal)
		ConditionVariableBroadcast(&undo_cleaner_state->capacity_cv);
	return test_proof;
}


ClusterCapacityReasonV1
cluster_ctrc_capacity_probe_pool_v1(uint32 segment, SCN horizon, uint64 epoch,
									ClusterCapacityObservationV1 *observation)
{
	if (test_lock_depth != 0 || !subscribed || (segment == 0 || segment > 256) || horizon != 1000
		|| epoch != 13)
		abort();
	test_pool_calls++;
	MemSet(observation, 0, sizeof(*observation));
	observation->reason = test_pool_reason;
	observation->key.segment_id = 256;
	observation->key.xid = 9123;
	observation->key.slot_wrap = 3;
	if (test_pool_probe_hook != NULL)
		test_pool_probe_hook();
	return test_pool_reason;
}

void
ConditionVariableInit(ConditionVariable *cv)
{
	MemSet(cv, 0, sizeof(*cv));
}

void
ConditionVariablePrepareToSleep(ConditionVariable *cv pg_attribute_unused())
{
	if (test_lock_depth != 0 || subscribed)
		abort();
	subscribed = true;
	prepared++;
}

bool
ConditionVariableTimedSleep(ConditionVariable *cv pg_attribute_unused(), long timeout, uint32 event)
{
	if (!subscribed || test_lock_depth != 0 || timeout != Max(cluster_lmon_main_loop_interval, 200)
		|| !undo_cleaner_capacity_context_safe())
		abort();
	slept++;
	last_sleep_event = event;
	/* The real CV's WaitLatch reports the supplied event and clears it. */
	pgstat_report_wait_start(event);
	if (test_cancel)
		pg_re_throw();
	pgstat_report_wait_end();
	return !early_signal; /* Both actual signal and timed repoll must retry allocation. */
}

bool
ConditionVariableCancelSleep(void)
{
	subscribed = false;
	cancelled++;
	return true;
}

void
ConditionVariableBroadcast(ConditionVariable *cv pg_attribute_unused())
{
	if (test_lock_depth != 0)
		abort();
	broadcasts++;
}

void
SetLatch(Latch *latch)
{
	if (test_lock_depth != 0 || latch == NULL)
		abort();
	signalled++;
}

void
pg_re_throw(void)
{
	if (PG_exception_stack == NULL)
		abort();
	siglongjmp(*PG_exception_stack, 1);
}

void
ProcessInterrupts(void)
{
	pg_re_throw();
}

/* ErrorData/SQLSTATE is a PostgreSQL boundary; the wait's error observer and
 * original PG_FINALLY/propagation run from the real product body. */
int
geterrcode(void)
{
	return test_error_code;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

static void
reset_wait_fixture(void)
{
	test_census_registration_current = true;
	free(undo_cleaner_state);
	undo_cleaner_state = NULL;
	cluster_undo_cleaner_shmem_init();
	MemSet(&test_proc, 0, sizeof(test_proc));
	MyProc = &test_proc;
	MyProcPid = 123;
	MyProc->backendId = 1;
	MyProc->lxid = 2;
	MyBackendType = B_BACKEND;
	CritSectionCount = InterruptHoldoffCount = InterruptPending = 0;
	test_xid = 100;
	MemSet(test_locks, 0, sizeof(test_locks));
	MemSet(PrivateRefCountArray, 0, sizeof(PrivateRefCountArray));
	MemSet(semantic_activation_local_inflight, 0, sizeof(semantic_activation_local_inflight));
	PrivateRefCountOverflowed = NLocBuffer = 0;
	LocalRefCount = NULL;
	dlist_init(&current_active_guards);
	dlist_init(&Block0OwnedResources);
	test_lock_count = test_lock_depth = test_scan_ends = 0;
	test_foreign_lwlock = test_fence = false;
	cluster_enabled = test_floor = true;
	cluster_shared_config = false;
	cluster_node_id = 0;
	test_error_code = ERRCODE_QUERY_CANCELED;
	cleaner_start_state = CLUSTER_NORMAL_START_TARGET_READY;
	cleaner_recovered_open = true;
	cluster_undo_cleaner_enabled = cluster_undo_retention_horizon_enabled = true;
	subscribed = early_signal = test_cancel = test_pin_after_probe = false;
	test_lock_wait = false;
	prepared = cancelled = slept = signalled = broadcasts = 0;
	test_wait_event = last_sleep_event = 0;
	test_proof = CLUSTER_CTRC_CAPACITY_WAIT;
	test_capacity_probe_hook = NULL;
	test_pool_reason = CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER;
	test_pool_calls = 0;
	test_pool_probe_hook = NULL;
	test_gc_complete = false;
	test_inventory_max = 1;
	test_inventory_scans = 0;
	test_gc_reclaimed = 0;
	test_recycle_result = CLUSTER_SEG_RECYCLE_ALREADY;
	test_ctrc_passes = test_gc_passes = test_modifier_entries = test_modifier_leaves = 0;
	test_census_begins = test_census_steps = test_reference_reads = test_pgproc_samples = 0;
	test_pgproc_min = test_future = InvalidFullTransactionId;
	test_pgproc_invalid = false;
	MemSet(&native_variables, 0, sizeof(native_variables));
	MemSet(&native_procs, 0, sizeof(native_procs));
	MemSet(native_array_storage, 0, sizeof(native_array_storage));
	MemSet(native_entries, 0, sizeof(native_entries));
	MemSet(native_xids, 0, sizeof(native_xids));
	procArray->maxProcs = lengthof(native_xids);
	for (int i = 0; i < lengthof(native_xids); i++)
	{
		procArray->pgprocnos[i] = i;
		pg_atomic_init_u64(&native_entries[i].cluster_top_full_xid_atomic, 0);
	}
	native_procs.xids = native_xids;
	native_procs.allProcs = native_entries;
	native_lock_phase = 0;
	native_custom = false;
	test_lease_current = test_sample_admission_current = true;
	test_lease_identity = 31;
	test_sample_admission_identity = 2;
	test_sample_admission_captures = test_source_steps = 0;
	MemSet(&test_sample_source, 0, sizeof(test_sample_source));
	MemSet(&test_first_source, 0, sizeof(test_first_source));
	MemSet(&test_last_source, 0, sizeof(test_last_source));
	test_sample_source.system_identifier = 91;
	test_sample_source.record_generation = 17;
	test_sample_source.formation_epoch = 9;
	test_sample_source.local_boot_incarnation = 47;
	test_sample_source.local_node_id = 0;
	test_sample_source.admitted_members_lo = 3;
	test_sample_source.admitted_incarnation[0] = 47;
	test_sample_source.admitted_incarnation[1] = 53;
	test_change_admission_after_n = false;
	test_census_busy = test_census_invalid = test_maintenance_cut = test_ctrc_progress = false;
	test_reference = FullTransactionIdFromEpochAndXid(7, 100);
	MemSet(&undo_cleaner_active_scan, 0, sizeof(undo_cleaner_active_scan));
	undo_cleaner_active_scan_pending = false;
	for (unsigned i = 0; i < CLUSTER_UNDO_CLEANER_WORKER_TYPES; i++) {
		undo_cleaner_worker = i;
		undo_cleaner_publish_status(UNDO_CLEANER_SPAWNING);
		undo_cleaner_publish_status(UNDO_CLEANER_READY);
	}
	undo_cleaner_worker = 0;
	broadcasts = 0;
}

static unsigned test_capacity_respawn_worker;

/* The physical/native CTRC proof is a boundary input, but worker lifecycle
 * publication and the wait's owner comparison execute their real bodies. */
static void
respawn_capacity_owner_during_probe(void)
{
	int previous_worker = undo_cleaner_worker;

	test_capacity_probe_hook = NULL;
	undo_cleaner_worker = test_capacity_respawn_worker;
	undo_cleaner_publish_status(UNDO_CLEANER_SHUTTING_DOWN);
	undo_cleaner_publish_status(UNDO_CLEANER_EXITED);
	undo_cleaner_publish_status(UNDO_CLEANER_SPAWNING);
	undo_cleaner_publish_status(UNDO_CLEANER_READY);
	undo_cleaner_worker = previous_worker;
}

UT_TEST(test_capacity_owner_respawn_requires_fresh_proof_before_sleep)
{
	static const unsigned owners[] = { 0, 3 };

	for (unsigned i = 0; i < lengthof(owners); i++) {
		ClusterCtrcTxnKeyV1 continuation = { 0 };
		ClusterCtrcTxnKeyV1 empty = { 0 };
		UndoCleanerWorkerState before, after;

		reset_wait_fixture();
		UT_ASSERT(cluster_undo_cleaner_worker_snapshot(owners[i], &before));
		test_capacity_respawn_worker = owners[i];
		test_capacity_probe_hook = respawn_capacity_owner_during_probe;
		UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(4, &continuation));
		UT_ASSERT(cluster_undo_cleaner_worker_snapshot(owners[i], &after));
		/* Same PID and fixed fixture clock cannot identify an incarnation. */
		UT_ASSERT_EQ(after.pid, before.pid);
		UT_ASSERT_EQ(after.spawned_at, before.spawned_at);
		UT_ASSERT_EQ(after.status, UNDO_CLEANER_READY);
		UT_ASSERT_EQ(slept, 0);
		UT_ASSERT_EQ(signalled, 0);
		UT_ASSERT_EQ(memcmp(&continuation, &empty, sizeof(empty)), 0);
		UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_entered, 0);
		UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_repolled, 0);
		UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_refused_proof, 0);
		UT_ASSERT_EQ(cancelled, 1);
		UT_ASSERT(!subscribed);
		UT_ASSERT_EQ(test_lock_depth, 0);
		/* The next attempt samples the original proof again, then may wait. */
		UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(4, &continuation));
		UT_ASSERT_EQ(slept, 1);
		UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_entered, 1);
		UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_repolled, 1);
	}
}

UT_TEST(test_unrelated_worker_respawn_keeps_exact_owner_wait)
{
	ClusterCtrcTxnKeyV1 continuation = { 0 };

	reset_wait_fixture();
	test_capacity_respawn_worker = 7;
	test_capacity_probe_hook = respawn_capacity_owner_during_probe;
	UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(4, &continuation));
	UT_ASSERT_EQ(slept, 1);
	UT_ASSERT_EQ(signalled, 8);
	UT_ASSERT_EQ(continuation.segment_id, 4);
	UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_entered, 1);
	UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_repolled, 1);
	UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_refused_proof, 0);
	UT_ASSERT(!subscribed);
}

UT_TEST(test_owner_respawn_cannot_override_proof_or_context_refusal)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterCtrcTxnKeyV1 continuation = { 0 };

		reset_wait_fixture();
		test_capacity_respawn_worker = 3;
		test_capacity_probe_hook = respawn_capacity_owner_during_probe;
		if (fault == 0)
			test_proof = CLUSTER_CTRC_CAPACITY_REFUSE;
		else
			test_pin_after_probe = true;
		UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(4, &continuation));
		UT_ASSERT_EQ(slept, 0);
		UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_entered, 0);
		UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_refused_proof, fault == 0);
		UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_refused_context, fault == 1);
		UT_ASSERT(!subscribed);
	}
}

UT_TEST(test_worker_incarnation_changes_only_on_its_original_spawn)
{
	for (unsigned worker = 0; worker < CLUSTER_UNDO_CLEANER_WORKER_TYPES; worker++) {
		UndoCleanerWorkerState before, after, peer;
		unsigned other = (worker + 1) % CLUSTER_UNDO_CLEANER_WORKER_TYPES;

		reset_wait_fixture();
		UT_ASSERT(cluster_undo_cleaner_worker_snapshot(worker, &before));
		UT_ASSERT_EQ(before.incarnation_generation, 1);
		UT_ASSERT(cluster_undo_cleaner_worker_snapshot(other, &peer));
		undo_cleaner_worker = worker;
		undo_cleaner_publish_status(UNDO_CLEANER_READY);
		UT_ASSERT(cluster_undo_cleaner_worker_snapshot(worker, &after));
		UT_ASSERT_EQ(after.incarnation_generation, before.incarnation_generation);
		undo_cleaner_publish_status(UNDO_CLEANER_SHUTTING_DOWN);
		undo_cleaner_publish_status(UNDO_CLEANER_EXITED);
		undo_cleaner_publish_status(UNDO_CLEANER_SPAWNING);
		undo_cleaner_publish_status(UNDO_CLEANER_READY);
		UT_ASSERT(cluster_undo_cleaner_worker_snapshot(worker, &after));
		UT_ASSERT_EQ(after.incarnation_generation, before.incarnation_generation + 1);
		UT_ASSERT_EQ(after.pid, before.pid);
		UT_ASSERT_EQ(after.spawned_at, before.spawned_at);
		UT_ASSERT(cluster_undo_cleaner_worker_snapshot(other, &after));
		UT_ASSERT_EQ(memcmp(&peer, &after, sizeof(peer)), 0);
	}
}

UT_TEST(test_worker_generation_exhaustion_never_reuses_an_incarnation)
{
	static const unsigned owners[] = { 0, 3 };

	for (unsigned i = 0; i < lengthof(owners); i++) {
		ClusterCtrcTxnKeyV1 continuation = { 0 };
		UndoCleanerWorkerState row;

		reset_wait_fixture();
		undo_cleaner_worker = owners[i];
		undo_cleaner_state->workers[owners[i]].incarnation_generation = UINT64_MAX - 2;
		undo_cleaner_publish_status(UNDO_CLEANER_SPAWNING);
		undo_cleaner_publish_status(UNDO_CLEANER_READY);
		UT_ASSERT(cluster_undo_cleaner_worker_snapshot(owners[i], &row));
		UT_ASSERT(row.incarnation_generation == UINT64_MAX - 1);
		UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(4, &continuation));
		UT_ASSERT_EQ(slept, 1);
		for (unsigned spawn = 0; spawn < 3; spawn++) {
			undo_cleaner_publish_status(UNDO_CLEANER_SPAWNING);
			undo_cleaner_publish_status(UNDO_CLEANER_READY);
			UT_ASSERT(cluster_undo_cleaner_worker_snapshot(owners[i], &row));
			UT_ASSERT(row.incarnation_generation == UINT64_MAX);
			UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(4, &continuation));
			UT_ASSERT_EQ(slept, 1);
			UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_refused_proof, spawn + 1);
		}
	}
}

UT_TEST(test_owner_generation_and_latch_cannot_be_replaced_by_ready_pid)
{
	static const unsigned owners[] = { 0, 3 };

	for (unsigned i = 0; i < lengthof(owners); i++) {
		for (unsigned fault = 0; fault < 3; fault++) {
			ClusterCtrcTxnKeyV1 continuation = { 0 };
			UndoCleanerWorkerState *row;

			reset_wait_fixture();
			row = &undo_cleaner_state->workers[owners[i]];
			if (fault == 0)
				row->incarnation_generation = 0;
			else if (fault == 1)
				row->incarnation_generation = UINT64_MAX;
			else
				row->latch = NULL;
			UT_ASSERT_EQ(row->status, UNDO_CLEANER_READY);
			UT_ASSERT_EQ(row->pid, MyProcPid);
			UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(4, &continuation));
			UT_ASSERT_EQ(slept, 0);
			UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_refused_proof, 1);
			UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_entered, 0);
			UT_ASSERT(!subscribed);
		}
	}
}

UT_TEST(test_newly_ready_owner_requires_reobservation_without_sleep)
{
	ClusterCtrcTxnKeyV1 continuation = { 0 };
	ClusterCtrcTxnKeyV1 empty = { 0 };

	reset_wait_fixture();
	undo_cleaner_state->workers[3].status = UNDO_CLEANER_EXITED;
	test_capacity_respawn_worker = 3;
	test_capacity_probe_hook = respawn_capacity_owner_during_probe;
	UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(4, &continuation));
	UT_ASSERT_EQ(slept, 0);
	UT_ASSERT_EQ(memcmp(&continuation, &empty, sizeof(empty)), 0);
	UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_refused_proof, 0);
	UT_ASSERT(!subscribed);
}

UT_TEST(test_recovered_open_gate_precedes_cleaner_ctrc_pass)
{
	bool remaining = false;

	reset_wait_fixture();
	undo_cleaner_worker = 0;
	cluster_shared_config = true;
	cleaner_start_state = CLUSTER_NORMAL_START_EXISTING_OTHER;
	cleaner_recovered_open = false;
	test_ctrc_progress = true;
	UT_ASSERT(!undo_cleaner_run_pass(&remaining, true));
	UT_ASSERT(!remaining);
	UT_ASSERT_EQ(test_ctrc_passes, 0);
	UT_ASSERT_EQ(test_modifier_entries, 0);
	cluster_shared_config = false;
	cleaner_start_state = CLUSTER_NORMAL_START_TARGET_READY;
	cleaner_recovered_open = true;
}

UT_TEST(test_capacity_wait_retries_signals_and_cadence_without_authorizing_free)
{
	for (int signal = 0; signal < 2; signal++) {
		ClusterCtrcTxnKeyV1 continuation = { 0 };

		reset_wait_fixture();
		early_signal = signal != 0;
		UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(1, &continuation));
		UT_ASSERT_EQ(prepared, 1);
		UT_ASSERT_EQ(slept, 1);
		UT_ASSERT_EQ(cancelled, 1);
		UT_ASSERT(!subscribed);
		UT_ASSERT_EQ(signalled, 8);
		UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_entered, 1);
		UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_repolled, 1);
		UT_ASSERT_EQ(undo_cleaner_state->shmem_tt_slots_gcd, 0);
		UT_ASSERT_EQ(undo_cleaner_state->segments_marked_recyclable, 0);
	}
}

UT_TEST(test_capacity_every_blocking_hold_refuses_without_sleep)
{
	for (int fault = 0; fault < 14; fault++) {
		ClusterCtrcTxnKeyV1 continuation = { 0 };
		dlist_node resource;
		int32 local_pin = 1;

		reset_wait_fixture();
		test_lock_wait = true;
		pgstat_report_wait_start(WAIT_EVENT_CLIENT_READ);
		switch (fault) {
		case 0:
			CritSectionCount = 1;
			break;
		case 1:
			InterruptHoldoffCount = 1;
			break;
		case 2:
			test_foreign_lwlock = true;
			break;
		case 3:
			PrivateRefCountArray[7].refcount = 1;
			break;
		case 4:
			PrivateRefCountOverflowed = 1;
			break;
		case 5:
			NLocBuffer = 1;
			LocalRefCount = &local_pin;
			break;
		case 6:
			dlist_push_head(&current_active_guards, &resource);
			break;
		case 7:
			dlist_push_head(&Block0OwnedResources, &resource);
			break;
		case 8:
			semantic_activation_local_inflight[0][0] = 1;
			break;
		case 9:
			semantic_activation_local_inflight[1][63] = 1;
			break;
		case 10:
			MyBackendType = B_UNDO_CLEANER;
			break;
		case 11:
			MyProc = NULL;
			break;
		case 12:
			NLocBuffer = 1;
			break;
		case 13:
			test_pin_after_probe = true;
			break;
		}
		UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(1, &continuation));
		UT_ASSERT_EQ(slept, 0);
		UT_ASSERT_EQ(test_wait_event, WAIT_EVENT_CLIENT_READ);
		UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_refused_context, 1);
		UT_ASSERT(!subscribed);
	}
}

UT_TEST(test_capacity_sleep_reports_frontend_undo_event)
{
	ClusterCtrcTxnKeyV1 continuation = { 0 };

	reset_wait_fixture();
	UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(1, &continuation));
	UT_ASSERT_EQ(last_sleep_event, PG_WAIT_CLUSTER_UNDO | 11U);
}

UT_TEST(test_capacity_sleep_restores_outer_event_on_normal_return)
{
	const uint32 outer_events[]
		= { 0, WAIT_EVENT_CLIENT_READ, WAIT_EVENT_CLUSTER_BGPROC_UNDO_CLEANER_MAIN_LOOP };

	for (unsigned i = 0; i < lengthof(outer_events); i++) {
		for (int signal = 0; signal < 2; signal++) {
			ClusterCtrcTxnKeyV1 continuation = { 0 };

			reset_wait_fixture();
			test_lock_wait = true;
			early_signal = signal != 0;
			pgstat_report_wait_start(outer_events[i]);
			UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(1, &continuation));
			UT_ASSERT_EQ(slept, 1);
			UT_ASSERT_EQ(test_wait_event, outer_events[i]);
		}
	}
}

UT_TEST(test_heavyweight_lock_identity_and_mode_are_exact)
{
	VirtualTransactionId vxid;

	reset_wait_fixture();
	test_lock_count = 1;
	test_locks[0].nLocks = 1;
	/* Deliberately unusable shared pointers: the production predicate must
	 * use only the local tag, for both session and transaction owners. */
	test_locks[0].lock = (LOCK *)(uintptr_t)1;
	test_locks[0].proclock = (PROCLOCK *)(uintptr_t)1;
	SET_LOCKTAG_RELATION(test_locks[0].tag.lock, 5, 123);
	for (LOCKMODE mode = AccessShareLock; mode <= AccessExclusiveLock; mode++) {
		test_locks[0].tag.mode = mode;
		UT_ASSERT_EQ(undo_cleaner_capacity_context_safe(), mode <= RowExclusiveLock);
	}
	SET_LOCKTAG_TRANSACTION(test_locks[0].tag.lock, test_xid);
	test_locks[0].tag.mode = ExclusiveLock;
	UT_ASSERT(undo_cleaner_capacity_context_safe());
	test_locks[0].tag.lock.locktag_field1++;
	UT_ASSERT(!undo_cleaner_capacity_context_safe());
	GET_VXID_FROM_PGPROC(vxid, *MyProc);
	SET_LOCKTAG_VIRTUALTRANSACTION(test_locks[0].tag.lock, vxid);
	UT_ASSERT(undo_cleaner_capacity_context_safe());
	test_locks[0].tag.lock.locktag_field2++;
	UT_ASSERT(!undo_cleaner_capacity_context_safe());
	test_locks[0].tag.lock.locktag_type = LOCKTAG_ADVISORY;
	UT_ASSERT(!undo_cleaner_capacity_context_safe());
	test_locks[0].nLocks = 0;
	UT_ASSERT(undo_cleaner_capacity_context_safe());
	test_locks[0].nLocks = -1;
	UT_ASSERT(!undo_cleaner_capacity_context_safe());
}

UT_TEST(test_capacity_cancellation_and_missing_supply_keep_original_outcome)
{
	ClusterCtrcTxnKeyV1 continuation = { 0 };
	volatile bool caught = false;

	reset_wait_fixture();
	test_cancel = true;
	PG_TRY();
	{
		(void)cluster_undo_cleaner_wait_for_capacity(1, &continuation);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(cancelled, 1);
	UT_ASSERT(!subscribed);
	for (int fault = 0; fault < 6; fault++) {
		reset_wait_fixture();
		test_lock_wait = true;
		pgstat_report_wait_start(WAIT_EVENT_CLIENT_READ);
		if (fault == 0)
			test_floor = false;
		if (fault == 1)
			test_fence = true;
		if (fault == 2)
			test_proof = CLUSTER_CTRC_CAPACITY_REFUSE;
		if (fault == 3)
			undo_cleaner_state->shutdown_requested = true;
		if (fault == 4)
			undo_cleaner_state->workers[0].status = UNDO_CLEANER_EXITED;
		if (fault == 5)
			undo_cleaner_state->workers[1].status = UNDO_CLEANER_EXITED;
		UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(2, &continuation));
		UT_ASSERT_EQ(slept, 0);
		UT_ASSERT_EQ(test_wait_event, WAIT_EVENT_CLIENT_READ);
		UT_ASSERT_EQ(cancelled, 1);
		UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_refused_proof, 1);
	}
}

UT_TEST(test_worker_lifecycle_rows_and_pid_inventory_are_independent)
{
	UndoCleanerWorkerState before, after;
	pid_t pids[8] = { 10, 11, 12, 13, 14, 15, 16, 17 };

	reset_wait_fixture();
	UT_ASSERT(cluster_undo_cleaner_worker_snapshot(0, &before));
	undo_cleaner_worker = 7;
	undo_cleaner_state->workers[7].local_completed_passes = 900;
	undo_cleaner_publish_status(UNDO_CLEANER_SPAWNING);
	UT_ASSERT(cluster_undo_cleaner_worker_snapshot(7, &after));
	UT_ASSERT_EQ(after.pid, MyProcPid);
	UT_ASSERT_EQ(after.local_completed_passes, 0);
	UT_ASSERT_EQ(after.ready_at, 0);
	undo_cleaner_publish_status(UNDO_CLEANER_READY);
	undo_cleaner_advance_liveness_tick();
	UT_ASSERT(cluster_undo_cleaner_worker_snapshot(7, &after));
	UT_ASSERT_EQ(after.main_loop_iters, 1);
	UT_ASSERT_EQ(after.ready_at, 123456);
	undo_cleaner_publish_status(UNDO_CLEANER_SHUTTING_DOWN);
	undo_cleaner_publish_status(UNDO_CLEANER_EXITED);
	UT_ASSERT(cluster_undo_cleaner_worker_snapshot(7, &after));
	UT_ASSERT(after.latch == NULL);
	UT_ASSERT_EQ(after.status, UNDO_CLEANER_EXITED);
	UT_ASSERT(cluster_undo_cleaner_worker_snapshot(0, &after));
	UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
	UT_ASSERT(!cluster_undo_cleaner_worker_snapshot(8, &after));
	UT_ASSERT_EQ(after.pid, 0);
	UT_ASSERT_EQ(broadcasts, 4);
	UT_ASSERT_EQ(cluster_undo_cleaner_find_pid(pids, 0), -1);
	UT_ASSERT_EQ(cluster_undo_cleaner_find_pid(pids, 99), -1);
	for (unsigned i = 0; i < 8; i++) {
		UT_ASSERT(!cluster_undo_cleaner_all_reaped(pids));
		UT_ASSERT_EQ(cluster_undo_cleaner_find_pid(pids, 10 + i), i);
		pids[i] = 0;
	}
	UT_ASSERT(cluster_undo_cleaner_all_reaped(pids));
}

UT_TEST(test_outer_pass_keeps_terminal_supply_but_cuts_new_optional_maintenance)
{
	for (int cut = 0; cut < 2; cut++) {
		for (int progress = 0; progress < 2; progress++) {
			bool remaining = false;
			reset_wait_fixture();
			undo_cleaner_worker = 0;
			test_maintenance_cut = cut != 0;
			test_ctrc_progress = progress != 0;
			test_ctrc_passes = test_gc_passes = test_modifier_entries = test_modifier_leaves = 0;
			(void)undo_cleaner_run_pass(&remaining, true);
			UT_ASSERT_EQ(test_ctrc_passes, 1);
			UT_ASSERT_EQ(remaining, progress != 0);
			UT_ASSERT_EQ(test_gc_passes, cut ? 0 : 1);
			UT_ASSERT_EQ(test_modifier_entries, cut ? 0 : 1);
			UT_ASSERT_EQ(test_modifier_leaves, test_modifier_entries);
			UT_ASSERT_EQ(undo_cleaner_state->segments_marked_recyclable, 0);
			UT_ASSERT_EQ(test_lock_depth, 0);
		}
	}
}

UT_TEST(test_full_history_batch_without_reclaim_yields)
{
	for (int retained = 0; retained < 2; retained++) {
		/* Reinitializing shmem does not erase the same durable inventory. */
		for (int boot = 0; boot < 2; boot++) {
			bool remaining = true;

			reset_wait_fixture();
			undo_cleaner_worker = 0;
			test_gc_complete = true;
			test_inventory_max = 9;
			test_recycle_result
				= retained ? CLUSTER_SEG_RECYCLE_RETAINED : CLUSTER_SEG_RECYCLE_ALREADY;
			test_ctrc_progress = test_maintenance_cut = false;
			for (int pass = 0; pass < 3; pass++) {
				UT_ASSERT(!undo_cleaner_run_pass(&remaining, true));
				UT_ASSERT(!remaining);
			}
			UT_ASSERT_EQ(test_inventory_scans, 24);
			UT_ASSERT_EQ(undo_cleaner_state->header_tt_slots_below_horizon, 24);
			UT_ASSERT_EQ(undo_cleaner_state->segments_marked_recyclable, 0);
			UT_ASSERT_EQ(undo_cleaner_state->shmem_tt_slots_gcd, 0);
			UT_ASSERT_EQ(test_lock_depth, 0);
		}
	}
}

UT_TEST(test_real_recycle_progress_continues_then_yields)
{
	for (int gc = 0; gc < 2; gc++) {
		bool remaining = false;

		reset_wait_fixture();
		undo_cleaner_worker = 0;
		test_gc_complete = true;
		test_inventory_max = 9;
		test_ctrc_progress = test_maintenance_cut = false;
		test_recycle_result = gc ? CLUSTER_SEG_RECYCLE_ALREADY : CLUSTER_SEG_RECYCLE_ADVANCED;
		test_gc_reclaimed = gc ? 1 : 0;
		UT_ASSERT(!undo_cleaner_run_pass(&remaining, true));
		UT_ASSERT(remaining);
		UT_ASSERT_EQ(undo_cleaner_state->segments_marked_recyclable, gc ? 0 : 8);
		UT_ASSERT_EQ(undo_cleaner_state->shmem_tt_slots_gcd, gc ? 1 : 0);
		test_recycle_result = CLUSTER_SEG_RECYCLE_ALREADY;
		test_gc_reclaimed = 0;
		UT_ASSERT(!undo_cleaner_run_pass(&remaining, true));
		UT_ASSERT(!remaining);
		UT_ASSERT_EQ(test_lock_depth, 0);
	}
}

UT_TEST(test_worker_zero_starts_readonly_census_after_mandatory_work)
{
	bool remaining = true;

	reset_wait_fixture();
	undo_cleaner_worker = 0;
	(void)undo_cleaner_run_pass(&remaining, true);
	UT_ASSERT_EQ(test_ctrc_passes, 1);
	UT_ASSERT_EQ(test_census_begins, 1);
	UT_ASSERT_EQ(test_reference_reads, 1);
	UT_ASSERT_EQ(test_census_steps, 1);
	UT_ASSERT(undo_cleaner_active_scan_pending);
	UT_ASSERT_EQ(undo_cleaner_active_scan.next_index, 64);
	UT_ASSERT(!remaining);
	UT_ASSERT_EQ(test_lock_depth, 0);
}

UT_TEST(test_census_continuations_keep_cursor_without_accelerating_gc)
{
	bool remaining;

	reset_wait_fixture();
	undo_cleaner_worker = 0;
	(void)undo_cleaner_run_pass(&remaining, true);
	UT_ASSERT_EQ(test_modifier_entries, 1);
	test_census_busy = true;
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(test_census_begins, 1);
	UT_ASSERT_EQ(test_reference_reads, 1);
	UT_ASSERT_EQ(undo_cleaner_active_scan.next_index, 64);
	UT_ASSERT(undo_cleaner_active_scan_pending);
	UT_ASSERT(!remaining);
	test_census_busy = false;
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(test_ctrc_passes, 3);
	UT_ASSERT_EQ(test_census_steps, 3);
	UT_ASSERT_EQ(test_census_begins, 1);
	UT_ASSERT_EQ(test_modifier_entries, 1);
	UT_ASSERT_EQ(test_modifier_leaves, 1);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT(!undo_cleaner_active_scan_pending);
	UT_ASSERT(!remaining);
}

UT_TEST(test_census_has_one_worker_and_no_shutdown_or_disabled_continuation)
{
	bool remaining;

	for (unsigned worker = 0; worker < CLUSTER_UNDO_CLEANER_WORKER_TYPES; worker++) {
		reset_wait_fixture();
		undo_cleaner_worker = worker;
		(void)undo_cleaner_run_pass(&remaining, true);
		UT_ASSERT_EQ(test_ctrc_passes, 1);
		UT_ASSERT_EQ(test_census_begins, worker == 0 ? 1 : 0);
		UT_ASSERT_EQ(test_census_steps, worker == 0 ? 1 : 0);
		UT_ASSERT_EQ(undo_cleaner_active_scan_pending, worker == 0);
	}
	for (int disabled = 0; disabled < 2; disabled++) {
		reset_wait_fixture();
		undo_cleaner_worker = 0;
		(void)undo_cleaner_run_pass(&remaining, true);
		test_maintenance_cut = !disabled;
		cluster_undo_cleaner_enabled = !disabled;
		(void)undo_cleaner_run_pass(&remaining, false);
		UT_ASSERT_EQ(test_census_steps, 1);
		UT_ASSERT_EQ(test_ctrc_passes, disabled ? 1 : 2);
		UT_ASSERT(!undo_cleaner_active_scan_pending);
		UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_NOT_STARTED);
	}
}

UT_TEST(test_census_invalid_sample_and_respawn_start_from_fresh_reference)
{
	bool remaining;
	UndoCleanerWorkerState row;

	reset_wait_fixture();
	undo_cleaner_worker = 0;
	test_census_invalid = true;
	(void)undo_cleaner_run_pass(&remaining, true);
	UT_ASSERT(!undo_cleaner_active_scan_pending);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INVALID);
	test_census_invalid = false;
	test_reference = FullTransactionIdFromEpochAndXid(8, 200);
	(void)undo_cleaner_run_pass(&remaining, true);
	UT_ASSERT_EQ(test_census_begins, 2);
	UT_ASSERT_EQ(undo_cleaner_active_scan.next_index, 64);
	UT_ASSERT_EQ(U64FromFullTransactionId(undo_cleaner_active_scan.reference_full),
				 U64FromFullTransactionId(test_reference));
	undo_cleaner_state->workers[0].active_census = undo_cleaner_active_scan;
	undo_cleaner_publish_status(UNDO_CLEANER_SPAWNING);
	UT_ASSERT(!undo_cleaner_active_scan_pending);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_NOT_STARTED);
	UT_ASSERT(cluster_undo_cleaner_worker_snapshot(0, &row));
	UT_ASSERT_EQ(row.active_census.status, CTRC_ACTIVE_SCAN_NOT_STARTED);
	test_reference = FullTransactionIdFromEpochAndXid(9, 300);
	(void)undo_cleaner_run_pass(&remaining, true);
	UT_ASSERT_EQ(test_census_begins, 3);
	UT_ASSERT_EQ(test_reference_reads, 3);
	UT_ASSERT_EQ(undo_cleaner_active_scan.next_index, 64);
	UT_ASSERT_EQ(U64FromFullTransactionId(undo_cleaner_active_scan.min_full),
				 U64FromFullTransactionId(test_reference));
}

UT_TEST(test_pgproc_and_future_are_captured_once_for_the_whole_census_round)
{
	bool remaining;

	reset_wait_fixture();
	undo_cleaner_worker = 0;
	test_pgproc_min = FullTransactionIdFromEpochAndXid(6, UINT32_MAX - 10);
	test_future = FullTransactionIdFromEpochAndXid(7, 500);
	(void)undo_cleaner_run_pass(&remaining, true);
	UT_ASSERT_EQ(test_pgproc_samples, 1);
	UT_ASSERT_EQ(U64FromFullTransactionId(undo_cleaner_active_scan.min_full),
				 (UINT64CONST(6) << 32) + UINT32_MAX - 10);
	UT_ASSERT_EQ(XidFromFullTransactionId(undo_cleaner_active_scan.future_full), 500);
	test_future = FullTransactionIdFromEpochAndXid(7, 700);
	test_pgproc_min = FullTransactionIdFromEpochAndXid(7, 99);
	test_reference = FullTransactionIdFromEpochAndXid(7, 200);
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(test_pgproc_samples, 1);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(XidFromFullTransactionId(undo_cleaner_active_scan.future_full), 500);
	UT_ASSERT_EQ(U64FromFullTransactionId(undo_cleaner_active_scan.reference_full),
				 (UINT64CONST(7) << 32) + 100);
	UT_ASSERT_EQ(U64FromFullTransactionId(undo_cleaner_active_scan.min_full),
				 (UINT64CONST(6) << 32) + UINT32_MAX - 10);
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(test_pgproc_samples, 2);
	UT_ASSERT_EQ(XidFromFullTransactionId(undo_cleaner_active_scan.future_full), 700);
	UT_ASSERT_EQ(U64FromFullTransactionId(undo_cleaner_active_scan.min_full),
				 U64FromFullTransactionId(test_pgproc_min));
}

UT_TEST(test_worker_retains_actual_native_prepared_minimum_after_source_exit)
{
	ClusterUndoCleanerActiveSample sample;
	bool remaining;

	reset_wait_fixture();
	undo_cleaner_worker = 0;
	native_custom = true;
	procArray->numProcs = 3;
	native_entries[0].pid = 1234;
	native_entries[1].pid = 0; /* Prepared transaction dummy. */
	native_entries[2].pid = 1235;
	native_xids[0] = native_xids[1] = 80;
	native_xids[2] = 90;
	pg_atomic_write_u64(&native_entries[0].cluster_top_full_xid_atomic,
					   U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(7, 80)));
	pg_atomic_write_u64(&native_entries[1].cluster_top_full_xid_atomic,
					   U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(7, 80)));
	pg_atomic_write_u64(&native_entries[2].cluster_top_full_xid_atomic,
					   U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(7, 90)));
	test_future = FullTransactionIdFromEpochAndXid(7, 500);
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INCOMPLETE);
	UT_ASSERT_EQ(XidFromFullTransactionId(undo_cleaner_active_scan.min_full), 80);
	UT_ASSERT(!cluster_undo_cleaner_active_sample(&sample));
	UT_ASSERT_EQ(native_lock_phase, 4);

	/* Controlled source exit: the original prepared dummy remains published. */
	native_xids[0] = InvalidTransactionId;
	pg_atomic_write_u64(&native_entries[0].cluster_top_full_xid_atomic, 0);
	test_reference = FullTransactionIdFromEpochAndXid(7, 200);
	test_future = FullTransactionIdFromEpochAndXid(7, 700);
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT(cluster_undo_cleaner_active_sample(&sample));
	UT_ASSERT(cluster_undo_cleaner_active_sample_current(&sample));
	UT_ASSERT_EQ(sample.sample_id, 1);
	UT_ASSERT_EQ(sample.scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(U64FromFullTransactionId(sample.scan.reference_full),
				 U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(7, 100)));
	UT_ASSERT_EQ(U64FromFullTransactionId(sample.scan.min_full),
				 U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(7, 80)));
	UT_ASSERT_EQ(XidFromFullTransactionId(sample.scan.future_full), 500);
	UT_ASSERT_EQ(test_pgproc_samples, 1);
	UT_ASSERT_EQ(test_census_begins, 1);
	UT_ASSERT_EQ(test_census_steps, 2);
	UT_ASSERT_EQ(test_ctrc_passes, 2);
	UT_ASSERT_EQ(test_modifier_entries, 0);
	UT_ASSERT_EQ(test_gc_passes, 0);
}

UT_TEST(test_worker_actual_unknown_prepared_identity_refuses_candidate)
{
	unsigned variant;

	for (variant = 0; variant < 2; variant++)
	{
		ClusterUndoCleanerActiveSample sample;
		bool remaining;

		reset_wait_fixture();
		undo_cleaner_worker = 0;
		native_custom = true;
		procArray->numProcs = 2;
		native_entries[0].pid = 1234;
		native_entries[1].pid = 0;
		native_xids[0] = 80;
		native_xids[1] = 81;
		pg_atomic_write_u64(&native_entries[0].cluster_top_full_xid_atomic,
						   U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(7, 80)));
		pg_atomic_write_u64(&native_entries[1].cluster_top_full_xid_atomic,
			variant == 0 ? 0 :
			U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(7, 82)));
		(void)undo_cleaner_run_pass(&remaining, false);
		UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INVALID);
		UT_ASSERT_EQ(test_pgproc_samples, 1);
		UT_ASSERT_EQ(test_census_begins, 0);
		UT_ASSERT_EQ(test_census_steps, 0);
		MemSet(&sample, 0xa5, sizeof(sample));
		UT_ASSERT(!cluster_undo_cleaner_active_sample(&sample));
		UT_ASSERT_EQ(sample.sample_id, 0);
		UT_ASSERT_EQ(native_lock_phase, 4);

		/* Remove the unknown input; only a fresh round may publish a sample. */
		native_xids[1] = InvalidTransactionId;
		pg_atomic_write_u64(&native_entries[1].cluster_top_full_xid_atomic, 0);
		(void)undo_cleaner_run_pass(&remaining, false);
		UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INCOMPLETE);
		UT_ASSERT(!cluster_undo_cleaner_active_sample(&sample));
		(void)undo_cleaner_run_pass(&remaining, false);
		UT_ASSERT(cluster_undo_cleaner_active_sample(&sample));
		UT_ASSERT(cluster_undo_cleaner_active_sample_current(&sample));
		UT_ASSERT_EQ(sample.sample_id, 1);
		UT_ASSERT_EQ(U64FromFullTransactionId(sample.scan.min_full),
			U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(7, 80)));
		UT_ASSERT_EQ(test_pgproc_samples, 2);
		UT_ASSERT_EQ(test_census_begins, 1);
		UT_ASSERT_EQ(test_census_steps, 2);
		UT_ASSERT_EQ(test_ctrc_passes, 3);
		UT_ASSERT_EQ(test_modifier_entries, 0);
		UT_ASSERT_EQ(test_gc_passes, 0);
		UT_ASSERT_EQ(native_lock_phase, 4);
	}
}

UT_TEST(test_changed_lease_cannot_complete_old_census_sample)
{
	bool remaining;

	reset_wait_fixture();
	undo_cleaner_worker = 0;
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INCOMPLETE);
	test_lease_current = false;
	test_ctrc_progress = true;
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(test_ctrc_passes, 2);
	UT_ASSERT(remaining);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INVALID);
	UT_ASSERT(!FullTransactionIdIsValid(undo_cleaner_active_scan.min_full));
	UT_ASSERT(!FullTransactionIdIsValid(undo_cleaner_active_scan.future_full));
	UT_ASSERT(!undo_cleaner_active_scan_pending);
	UT_ASSERT_EQ(test_pgproc_samples, 1);
	UT_ASSERT_EQ(test_gc_passes, 0);

	test_lease_current = true;
	test_reference = FullTransactionIdFromEpochAndXid(8, 200);
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(test_pgproc_samples, 2);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INCOMPLETE);
	UT_ASSERT_EQ(U64FromFullTransactionId(undo_cleaner_active_scan.reference_full),
				 U64FromFullTransactionId(test_reference));
}

UT_TEST(test_pgproc_failure_invalidates_old_sample_without_skipping_mandatory_work)
{
	bool remaining;

	reset_wait_fixture();
	undo_cleaner_worker = 0;
	(void)undo_cleaner_run_pass(&remaining, true);
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	test_pgproc_invalid = true;
	test_ctrc_progress = true;
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(test_ctrc_passes, 3);
	UT_ASSERT(remaining);
	UT_ASSERT_EQ(test_pgproc_samples, 2);
	UT_ASSERT_EQ(test_census_begins, 1);
	UT_ASSERT_EQ(test_census_steps, 2);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INVALID);
	UT_ASSERT(!FullTransactionIdIsValid(undo_cleaner_active_scan.min_full));
	UT_ASSERT(!FullTransactionIdIsValid(undo_cleaner_active_scan.reference_full));
	UT_ASSERT(!FullTransactionIdIsValid(undo_cleaner_active_scan.future_full));
	UT_ASSERT(!undo_cleaner_active_scan_pending);
	UT_ASSERT_EQ(test_modifier_entries, 1);
	test_pgproc_invalid = false;
	test_lease_current = true;
	test_pgproc_min = FullTransactionIdFromEpochAndXid(7, 80);
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(test_pgproc_samples, 3);
	UT_ASSERT_EQ(test_census_begins, 2);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INCOMPLETE);
	UT_ASSERT_EQ(XidFromFullTransactionId(undo_cleaner_active_scan.min_full), 80);
}

UT_TEST(test_closed_admission_cannot_start_native_census_sample)
{
	bool remaining;

	reset_wait_fixture();
	undo_cleaner_worker = 0;
	test_sample_admission_current = false;
	test_ctrc_progress = true;
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(test_ctrc_passes, 1);
	UT_ASSERT(remaining);
	UT_ASSERT_EQ(test_pgproc_samples, 0);
	UT_ASSERT_EQ(test_census_begins, 0);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INVALID);
	UT_ASSERT(!FullTransactionIdIsValid(undo_cleaner_active_scan.min_full));
	UT_ASSERT(!undo_cleaner_active_scan_pending);
}

UT_TEST(test_changed_admission_cannot_complete_old_census_sample)
{
	bool remaining;

	reset_wait_fixture();
	undo_cleaner_worker = 0;
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INCOMPLETE);
	test_sample_admission_current = false;
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INVALID);
	UT_ASSERT(!FullTransactionIdIsValid(undo_cleaner_active_scan.min_full));
	UT_ASSERT(!FullTransactionIdIsValid(undo_cleaner_active_scan.future_full));
	UT_ASSERT(!undo_cleaner_active_scan_pending);
	UT_ASSERT_EQ(test_modifier_entries, 0);

	test_sample_admission_current = true;
	test_reference = FullTransactionIdFromEpochAndXid(8, 200);
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(test_pgproc_samples, 2);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INCOMPLETE);
	UT_ASSERT_EQ(U64FromFullTransactionId(undo_cleaner_active_scan.reference_full),
				 U64FromFullTransactionId(test_reference));
}

UT_TEST(test_open_admission_republication_discards_original_scan)
{
	bool remaining;

	reset_wait_fixture();
	undo_cleaner_worker = 0;
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INCOMPLETE);
	test_sample_admission_identity += 2;
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INVALID);
	UT_ASSERT(!FullTransactionIdIsValid(undo_cleaner_active_scan.min_full));
	UT_ASSERT(!undo_cleaner_active_scan_pending);
	UT_ASSERT_EQ(test_sample_admission_captures, 1);
	UT_ASSERT_EQ(test_pgproc_samples, 1);
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INCOMPLETE);
	UT_ASSERT_EQ(test_sample_admission_captures, 2);
	UT_ASSERT_EQ(test_pgproc_samples, 2);
}

UT_TEST(test_admission_is_saved_before_n_and_rechecked_after_first_slice)
{
	bool remaining;

	reset_wait_fixture();
	undo_cleaner_worker = 0;
	test_change_admission_after_n = true;
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(test_sample_admission_captures, 1);
	UT_ASSERT_EQ(test_pgproc_samples, 1);
	UT_ASSERT_EQ(test_census_steps, 1);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INVALID);
	UT_ASSERT(!FullTransactionIdIsValid(undo_cleaner_active_scan.min_full));
	UT_ASSERT(!FullTransactionIdIsValid(undo_cleaner_active_scan.future_full));
	UT_ASSERT(!undo_cleaner_active_scan_pending);
	UT_ASSERT_EQ(test_modifier_entries, 0);
}

UT_TEST(test_complete_sample_is_shared_without_publishing_partial_round)
{
	ClusterUndoCleanerActiveSample first, again;
	bool remaining;

	reset_wait_fixture();
	undo_cleaner_worker = 0;
	UT_ASSERT(!cluster_undo_cleaner_active_sample(&first));
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT(!cluster_undo_cleaner_active_sample(&first));
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT(cluster_undo_cleaner_active_sample(&first));
	UT_ASSERT_EQ(first.sample_id, 1);
	UT_ASSERT_EQ(first.worker_pid, MyProcPid);
	UT_ASSERT_EQ(first.scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT_EQ(U64FromFullTransactionId(first.scan.min_full),
				 U64FromFullTransactionId(test_reference));
	UT_ASSERT_EQ(test_modifier_entries, 0);
	UT_ASSERT(cluster_undo_cleaner_active_sample(&again));
	UT_ASSERT_EQ(again.sample_id, first.sample_id);

	/* A partial newer scan cannot invent a new F generation. */
	test_reference = FullTransactionIdFromEpochAndXid(7, 200);
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT(cluster_undo_cleaner_active_sample(&again));
	UT_ASSERT_EQ(again.sample_id, first.sample_id);
	UT_ASSERT_EQ(U64FromFullTransactionId(again.scan.future_full),
				 U64FromFullTransactionId(first.scan.future_full));
	UT_ASSERT(cluster_undo_cleaner_active_sample_current(&first));
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT(cluster_undo_cleaner_active_sample(&again));
	UT_ASSERT_EQ(again.sample_id, 2);
	UT_ASSERT_EQ(U64FromFullTransactionId(again.scan.min_full),
				 U64FromFullTransactionId(test_reference));
	UT_ASSERT(!cluster_undo_cleaner_active_sample_current(&first));
}

UT_TEST(test_complete_sample_rechecks_original_fences_and_worker_lifecycle)
{
	for (int fault = 0; fault < 10; fault++) {
		ClusterUndoCleanerActiveSample sample, rejected;
		bool remaining;

		reset_wait_fixture();
		undo_cleaner_worker = 0;
		(void)undo_cleaner_run_pass(&remaining, false);
		(void)undo_cleaner_run_pass(&remaining, false);
		UT_ASSERT(cluster_undo_cleaner_active_sample(&sample));
		switch (fault) {
		case 0:
			test_lease_current = false;
			break;
		case 1:
			test_lease_identity++;
			break;
		case 2:
			test_sample_admission_identity += 2;
			break;
		case 3:
			test_sample_admission_current = false;
			break;
		case 4:
			undo_cleaner_state->shutdown_requested = true;
			break;
		case 5:
			undo_cleaner_publish_status(UNDO_CLEANER_SHUTTING_DOWN);
			break;
		case 6:
			cluster_undo_cleaner_enabled = false;
			break;
		case 7:
			cluster_enabled = false;
			break;
		case 8:
			test_maintenance_cut = true;
			break;
		case 9:
			test_census_registration_current = false;
			break;
		}
		UT_ASSERT(!cluster_undo_cleaner_active_sample_current(&sample));
		MemSet(&rejected, 0xa5, sizeof(rejected));
		UT_ASSERT(!cluster_undo_cleaner_active_sample(&rejected));
		UT_ASSERT_EQ(rejected.sample_id, 0);
		UT_ASSERT(!FullTransactionIdIsValid(rejected.scan.min_full));
	}
}

UT_TEST(test_sample_sequence_survives_worker0_respawn_and_never_wraps)
{
	ClusterUndoCleanerActiveSample sample;
	bool remaining;

	reset_wait_fixture();
	undo_cleaner_worker = 0;
	(void)undo_cleaner_run_pass(&remaining, false);
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT(cluster_undo_cleaner_active_sample(&sample));
	UT_ASSERT_EQ(sample.sample_id, 1);
	undo_cleaner_worker = 1;
	undo_cleaner_publish_status(UNDO_CLEANER_SPAWNING);
	UT_ASSERT(cluster_undo_cleaner_active_sample_current(&sample));
	undo_cleaner_worker = 0;
	MyProcPid++;
	undo_cleaner_publish_status(UNDO_CLEANER_SPAWNING);
	UT_ASSERT(!cluster_undo_cleaner_active_sample(&sample));
	undo_cleaner_publish_status(UNDO_CLEANER_READY);
	(void)undo_cleaner_run_pass(&remaining, false);
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT(cluster_undo_cleaner_active_sample(&sample));
	UT_ASSERT_EQ(sample.sample_id, 2);
	UT_ASSERT_EQ(sample.worker_pid, MyProcPid);

	undo_cleaner_state->active_sample_sequence = UINT64_MAX;
	(void)undo_cleaner_run_pass(&remaining, false);
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT(!cluster_undo_cleaner_active_sample(&sample));
	UT_ASSERT_EQ(undo_cleaner_state->active_sample_sequence, UINT64_MAX);
}

UT_TEST(test_worker_census_borrows_semantic_boot_not_lease_owner)
{
	ClusterUndoCleanerActiveSample sample;
	bool remaining;

	reset_wait_fixture();
	undo_cleaner_worker = 0;
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(test_source_steps, 1);
	UT_ASSERT_EQ(test_sample_admission_captures, 1);
	UT_ASSERT_EQ(test_first_source.system_identifier, test_sample_source.system_identifier);
	UT_ASSERT_EQ(test_first_source.record_generation, test_sample_source.record_generation);
	UT_ASSERT_EQ(test_first_source.formation_epoch, test_sample_source.formation_epoch);
	UT_ASSERT_EQ(test_first_source.local_boot_incarnation, 47);
	UT_ASSERT_EQ(undo_cleaner_active_lease_fence.incarnation, 31);
	UT_ASSERT_EQ(test_first_source.admitted_incarnation[1], 53);
	UT_ASSERT_EQ(test_first_source.admission_publication_seq, test_sample_admission_identity);
	UT_ASSERT(!cluster_undo_cleaner_active_sample(&sample));
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(test_source_steps, 2);
	UT_ASSERT_EQ(test_sample_admission_captures, 1);
	UT_ASSERT_EQ(memcmp(&test_first_source, &test_last_source, sizeof(test_first_source)), 0);
	UT_ASSERT(cluster_undo_cleaner_active_sample(&sample));
	UT_ASSERT(cluster_undo_cleaner_active_sample_current(&sample));
	UT_ASSERT_EQ(test_modifier_entries, 0);
}

UT_TEST(test_worker_census_source_republication_discards_before_replacement)
{
	bool remaining;

	reset_wait_fixture();
	undo_cleaner_worker = 0;
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(test_source_steps, 1);
	test_sample_admission_identity += 2;
	test_sample_source.local_boot_incarnation++;
	test_sample_source.admitted_incarnation[0]++;
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_INVALID);
	UT_ASSERT_EQ(test_source_steps, 2);
	UT_ASSERT_EQ(test_last_source.local_boot_incarnation, 47);
	UT_ASSERT_EQ(test_sample_admission_captures, 1);
	UT_ASSERT_EQ(undo_cleaner_active_admission_fence.local_boot_incarnation, 0);
	(void)undo_cleaner_run_pass(&remaining, false);
	UT_ASSERT_EQ(test_source_steps, 3);
	UT_ASSERT_EQ(test_last_source.local_boot_incarnation, 48);
	UT_ASSERT_EQ(test_last_source.admission_publication_seq, test_sample_admission_identity);
	UT_ASSERT_EQ(test_sample_admission_captures, 2);
	UT_ASSERT_EQ(test_pgproc_samples, 2);
	UT_ASSERT_EQ(test_modifier_entries, 0);
}

UT_TEST(test_capacity_stats_unavailable_does_not_fabricate_an_origin_or_zero)
{
	ClusterUndoCapacityStats out, before;
	UndoCleanerSharedState *saved;

	reset_wait_fixture();
	MemSet(&out, 0xa5, sizeof(out));
	before = out;
	saved = undo_cleaner_state;
	undo_cleaner_state = NULL;
	UT_ASSERT(!cluster_undo_cleaner_capacity_stats_snapshot(&out));
	UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
	undo_cleaner_state = saved;
	for (unsigned invalid = 0; invalid < 2; invalid++) {
		cluster_node_id = invalid ? CLUSTER_CTRC_MAX_PARTICIPANTS : -1;
		UT_ASSERT(!cluster_undo_cleaner_capacity_stats_snapshot(&out));
		UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
	}
	cluster_node_id = 0;
	UT_ASSERT(!cluster_undo_cleaner_capacity_stats_snapshot(NULL));
	UT_ASSERT_EQ(test_lock_depth, 0);
}

UT_TEST(test_capacity_stats_distinguishes_available_zero_and_preserves_unsigned_counts)
{
	ClusterUndoCapacityStats out;

	reset_wait_fixture();
	cluster_node_id = 3;
	for (unsigned i = 0; i < CLUSTER_UNDO_CLEANER_WORKER_TYPES; i++)
		undo_cleaner_state->workers[i].status = UNDO_CLEANER_NOT_STARTED;
	UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&out));
	UT_ASSERT_EQ(out.origin_node_id, 3);
	UT_ASSERT_EQ(out.pool_wait_count, 0);
	UT_ASSERT_EQ(out.wakeup_recheck_count, 0);
	UT_ASSERT_EQ(out.cancel_count, 0);
	UT_ASSERT_EQ(out.refused_context_count, 0);
	UT_ASSERT_EQ(out.refused_proof_count, 0);
	undo_cleaner_state->capacity_wait_entered = UINT64_MAX;
	undo_cleaner_state->capacity_wait_repolled = UINT64_C(9223372036854775809);
	undo_cleaner_state->capacity_wait_refused_context = 19;
	undo_cleaner_state->capacity_wait_refused_proof = 23;
	pg_atomic_write_u64(&undo_cleaner_state->capacity_wait_cancelled, 17);
	UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&out));
	UT_ASSERT_EQ(out.pool_wait_count, UINT64_MAX);
	UT_ASSERT_EQ(out.wakeup_recheck_count, UINT64_C(9223372036854775809));
	UT_ASSERT_EQ(out.cancel_count, 17);
	UT_ASSERT_EQ(out.refused_context_count, 19);
	UT_ASSERT_EQ(out.refused_proof_count, 23);
	UT_ASSERT_EQ(slept, 0);
	UT_ASSERT_EQ(test_lock_depth, 0);
	cluster_node_id = 0;
}

UT_TEST(test_capacity_stats_counts_existing_sleep_recheck_and_refusal_paths)
{
	for (unsigned mode = 0; mode < 5; mode++) {
		ClusterCtrcTxnKeyV1 continuation = { 0 };
		ClusterUndoCapacityStats out;

		reset_wait_fixture();
		early_signal = mode == 1;
		if (mode == 2)
			test_proof = CLUSTER_CTRC_CAPACITY_RETRY;
		if (mode == 3)
			test_foreign_lwlock = true;
		if (mode == 4)
			test_floor = false;
		UT_ASSERT_EQ(cluster_undo_cleaner_wait_for_capacity(1, &continuation), mode < 3);
		UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&out));
		UT_ASSERT_EQ(out.pool_wait_count, mode < 2 ? 1 : 0);
		UT_ASSERT_EQ(out.wakeup_recheck_count, mode < 2 ? 1 : 0);
		UT_ASSERT_EQ(out.cancel_count, 0);
		UT_ASSERT_EQ(out.refused_context_count, mode == 3 ? 1 : 0);
		UT_ASSERT_EQ(out.refused_proof_count, mode == 4 ? 1 : 0);
		UT_ASSERT_EQ(undo_cleaner_state->shmem_tt_slots_gcd, 0);
		UT_ASSERT_EQ(undo_cleaner_state->segments_marked_recyclable, 0);
	}
}

UT_TEST(test_capacity_stats_counts_only_query_cancellation_and_preserves_error_cleanup)
{
	static const int codes[] = { ERRCODE_QUERY_CANCELED, ERRCODE_QUERY_CANCELED,
								 ERRCODE_LOCK_NOT_AVAILABLE, ERRCODE_IO_ERROR };

	for (unsigned fault = 0; fault < lengthof(codes); fault++) {
		ClusterCtrcTxnKeyV1 continuation = { 0 };
		ClusterUndoCapacityStats out;
		volatile bool caught = false;
		volatile int caught_code = 0;

		reset_wait_fixture();
		test_error_code = codes[fault];
		test_cancel = true;
		InterruptPending = fault == 1;
		PG_TRY();
		{
			(void)cluster_undo_cleaner_wait_for_capacity(1, &continuation);
		}
		PG_CATCH();
		{
			caught = true;
			caught_code = geterrcode();
		}
		PG_END_TRY();
		InterruptPending = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(caught_code, codes[fault]);
		UT_ASSERT_EQ(cancelled, 1);
		UT_ASSERT(!subscribed);
		UT_ASSERT_EQ(test_lock_depth, 0);
		UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&out));
		UT_ASSERT_EQ(out.cancel_count, fault < 2 ? 1 : 0);
		UT_ASSERT_EQ(out.pool_wait_count, fault == 1 ? 0 : 1);
		UT_ASSERT_EQ(out.wakeup_recheck_count, 0);
		UT_ASSERT_EQ(out.refused_context_count, 0);
		UT_ASSERT_EQ(out.refused_proof_count, 0);
	}
}


UT_TEST(test_pool_owner_uses_original_cv_and_exact_old_segment_worker)
{
	ClusterCtrcTxnKeyV1 continuation = { 0 };
	reset_wait_fixture();
	test_proof = CLUSTER_CTRC_CAPACITY_REFUSE;
	test_pool_reason = CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE;
	UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(1, &continuation));
	UT_ASSERT_EQ(test_pool_calls, 1);
	UT_ASSERT_EQ(continuation.segment_id, 256);
	UT_ASSERT_EQ(continuation.xid, 9123);
	UT_ASSERT_EQ(slept, 1);
	UT_ASSERT_EQ(signalled, 8);
	UT_ASSERT_EQ(cancelled, 1);
	UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_entered, 1);
	UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_repolled, 1);
	UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_refused_proof, 0);
	UT_ASSERT(!subscribed);
}

UT_TEST(test_pool_recheck_returns_to_allocator_without_sleep_or_grant)
{
	ClusterCtrcTxnKeyV1 continuation = { 0 }, empty = { 0 };
	reset_wait_fixture();
	test_proof = CLUSTER_CTRC_CAPACITY_REFUSE;
	test_pool_reason = CLUSTER_CAPACITY_SUPPLY_RECHECK;
	UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(1, &continuation));
	UT_ASSERT_EQ(test_pool_calls, 1);
	UT_ASSERT_EQ(memcmp(&continuation, &empty, sizeof(empty)), 0);
	UT_ASSERT_EQ(slept, 0);
	UT_ASSERT_EQ(signalled, 0);
	UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_entered, 0);
	UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_refused_proof, 0);
	UT_ASSERT(!subscribed);
}

UT_TEST(test_pool_owner_change_cannot_bridge_original_wait_generation)
{
	ClusterCtrcTxnKeyV1 continuation = { 0 }, empty = { 0 };
	reset_wait_fixture();
	test_proof = CLUSTER_CTRC_CAPACITY_REFUSE;
	test_pool_reason = CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE;
	test_capacity_respawn_worker = 7;
	test_pool_probe_hook = respawn_capacity_owner_during_probe;
	UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(1, &continuation));
	UT_ASSERT_EQ(test_pool_calls, 1);
	UT_ASSERT_EQ(memcmp(&continuation, &empty, sizeof(empty)), 0);
	UT_ASSERT_EQ(slept, 0);
	UT_ASSERT_EQ(signalled, 0);
	UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_entered, 0);
	UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_refused_proof, 0);
	UT_ASSERT(!subscribed);
}

UT_TEST(test_current_proof_never_calls_pool_and_unproven_never_sleeps)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterCtrcTxnKeyV1 continuation = { 0 };
		reset_wait_fixture();
		test_pool_reason = CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE;
		if (fault == 1)
			test_proof = CLUSTER_CTRC_CAPACITY_RETRY;
		if (fault == 2) {
			test_proof = CLUSTER_CTRC_CAPACITY_REFUSE;
			test_pool_reason = CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER;
		}
		UT_ASSERT_EQ(cluster_undo_cleaner_wait_for_capacity(1, &continuation), fault != 2);
		UT_ASSERT_EQ(test_pool_calls, fault == 2);
		UT_ASSERT_EQ(slept, fault == 0);
		UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_refused_proof, fault == 2);
		UT_ASSERT(!subscribed);
	}
}

UT_TEST(test_owner_snapshot_has_original_ready_generations_and_no_false_output)
{
	uint64 coordinator = 91, generation = 93;
	reset_wait_fixture();
	UT_ASSERT(cluster_undo_cleaner_capacity_owner_snapshot(7, &coordinator, &generation));
	UT_ASSERT_EQ(coordinator, 1);
	UT_ASSERT_EQ(generation, 1);
	undo_cleaner_state->workers[7].incarnation_generation = 17;
	UT_ASSERT(cluster_undo_cleaner_capacity_owner_snapshot(7, &coordinator, &generation));
	UT_ASSERT_EQ(coordinator, 1);
	UT_ASSERT_EQ(generation, 17);
	for (unsigned fault = 0; fault < 6; fault++) {
		reset_wait_fixture();
		coordinator = 91;
		generation = 93;
		switch (fault) {
		case 0:
			undo_cleaner_state->shutdown_requested = true;
			break;
		case 1:
			undo_cleaner_state->workers[0].status = UNDO_CLEANER_SPAWNING;
			break;
		case 2:
			undo_cleaner_state->workers[7].latch = NULL;
			break;
		case 3:
			undo_cleaner_state->workers[7].incarnation_generation = UINT64_MAX;
			break;
		case 4:
			undo_cleaner_state->workers[7].incarnation_generation = 0;
			break;
		case 5:
			break;
		}
		UT_ASSERT(!cluster_undo_cleaner_capacity_owner_snapshot(fault == 5 ? 8 : 7, &coordinator,
																&generation));
		UT_ASSERT_EQ(coordinator, 91);
		UT_ASSERT_EQ(generation, 93);
		UT_ASSERT_EQ(test_lock_depth, 0);
	}
}


static void
pool_proof_returns_with_context_hold(void)
{
	PrivateRefCountArray[0].refcount = 1;
}

UT_TEST(test_pool_keeps_original_context_refusal_and_cancel_cleanup)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterCtrcTxnKeyV1 continuation = { 0 };
		volatile bool caught = false;
		reset_wait_fixture();
		test_proof = CLUSTER_CTRC_CAPACITY_REFUSE;
		test_pool_reason = CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE;
		if (fault == 0)
			test_pool_probe_hook = pool_proof_returns_with_context_hold;
		else
			test_cancel = true;
		PG_TRY();
		{
			UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(1, &continuation));
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 1);
		UT_ASSERT_EQ(test_pool_calls, 1);
		UT_ASSERT_EQ(slept, fault == 1);
		UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_refused_context, fault == 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&undo_cleaner_state->capacity_wait_cancelled), fault == 1);
		UT_ASSERT_EQ(cancelled, 1);
		UT_ASSERT_EQ(test_lock_depth, 0);
		UT_ASSERT(!subscribed);
	}
}

UT_TEST(test_native_slot_wait_scope_preserves_context_and_counts_only_its_cancellation)
{
	ClusterUndoCapacityStats out;

	reset_wait_fixture();
	UT_ASSERT(cluster_undo_cleaner_capacity_context_safe());
	UT_ASSERT(!cluster_undo_cleaner_tx_slot_waiting());
	for (unsigned i = 0; i < 3; i++) {
		cluster_undo_cleaner_tx_slot_wait_begin();
		UT_ASSERT(cluster_undo_cleaner_tx_slot_waiting());
		cluster_undo_cleaner_tx_slot_wait_end(i == 1);
		UT_ASSERT(!cluster_undo_cleaner_tx_slot_waiting());
		UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&out));
		UT_ASSERT_EQ(out.active_full_wait_count, i + 1);
		UT_ASSERT_EQ(out.cancel_count, i == 0 ? 0 : 1);
		UT_ASSERT_EQ(out.pool_wait_count, 0);
	}
	PrivateRefCountArray[7].refcount = 1;
	UT_ASSERT(!cluster_undo_cleaner_capacity_context_safe());
	PrivateRefCountArray[7].refcount = 0;
	semantic_activation_local_inflight[1][63] = 1;
	UT_ASSERT(!cluster_undo_cleaner_capacity_context_safe());
	semantic_activation_local_inflight[1][63] = 0;
	UT_ASSERT(cluster_undo_cleaner_capacity_context_safe());
	UT_ASSERT_EQ(slept, 0);
	UT_ASSERT_EQ(test_lock_depth, 0);
}

int
main(void)
{
	UT_PLAN(47);
	UT_RUN(test_native_slot_wait_scope_preserves_context_and_counts_only_its_cancellation);
	UT_RUN(test_pool_keeps_original_context_refusal_and_cancel_cleanup);
	UT_RUN(test_pool_owner_uses_original_cv_and_exact_old_segment_worker);
	UT_RUN(test_pool_recheck_returns_to_allocator_without_sleep_or_grant);
	UT_RUN(test_pool_owner_change_cannot_bridge_original_wait_generation);
	UT_RUN(test_current_proof_never_calls_pool_and_unproven_never_sleeps);
	UT_RUN(test_owner_snapshot_has_original_ready_generations_and_no_false_output);

	UT_RUN(test_capacity_owner_respawn_requires_fresh_proof_before_sleep);
	UT_RUN(test_unrelated_worker_respawn_keeps_exact_owner_wait);
	UT_RUN(test_owner_respawn_cannot_override_proof_or_context_refusal);
	UT_RUN(test_worker_incarnation_changes_only_on_its_original_spawn);
	UT_RUN(test_worker_generation_exhaustion_never_reuses_an_incarnation);
	UT_RUN(test_owner_generation_and_latch_cannot_be_replaced_by_ready_pid);
	UT_RUN(test_newly_ready_owner_requires_reobservation_without_sleep);
	UT_RUN(test_capacity_stats_unavailable_does_not_fabricate_an_origin_or_zero);
	UT_RUN(test_capacity_stats_distinguishes_available_zero_and_preserves_unsigned_counts);
	UT_RUN(test_capacity_stats_counts_existing_sleep_recheck_and_refusal_paths);
	UT_RUN(test_capacity_stats_counts_only_query_cancellation_and_preserves_error_cleanup);
	UT_RUN(test_recovered_open_gate_precedes_cleaner_ctrc_pass);
	UT_RUN(test_capacity_wait_retries_signals_and_cadence_without_authorizing_free);
	UT_RUN(test_capacity_sleep_reports_frontend_undo_event);
	UT_RUN(test_capacity_sleep_restores_outer_event_on_normal_return);
	UT_RUN(test_capacity_every_blocking_hold_refuses_without_sleep);
	UT_RUN(test_heavyweight_lock_identity_and_mode_are_exact);
	UT_RUN(test_capacity_cancellation_and_missing_supply_keep_original_outcome);
	UT_RUN(test_worker_lifecycle_rows_and_pid_inventory_are_independent);
	UT_RUN(test_outer_pass_keeps_terminal_supply_but_cuts_new_optional_maintenance);
	UT_RUN(test_full_history_batch_without_reclaim_yields);
	UT_RUN(test_real_recycle_progress_continues_then_yields);
	UT_RUN(test_worker_zero_starts_readonly_census_after_mandatory_work);
	UT_RUN(test_census_continuations_keep_cursor_without_accelerating_gc);
	UT_RUN(test_census_has_one_worker_and_no_shutdown_or_disabled_continuation);
	UT_RUN(test_census_invalid_sample_and_respawn_start_from_fresh_reference);
	UT_RUN(test_pgproc_and_future_are_captured_once_for_the_whole_census_round);
	UT_RUN(test_worker_retains_actual_native_prepared_minimum_after_source_exit);
	UT_RUN(test_worker_actual_unknown_prepared_identity_refuses_candidate);
	UT_RUN(test_changed_lease_cannot_complete_old_census_sample);
	UT_RUN(test_closed_admission_cannot_start_native_census_sample);
	UT_RUN(test_changed_admission_cannot_complete_old_census_sample);
	UT_RUN(test_open_admission_republication_discards_original_scan);
	UT_RUN(test_admission_is_saved_before_n_and_rechecked_after_first_slice);
	UT_RUN(test_pgproc_failure_invalidates_old_sample_without_skipping_mandatory_work);
	UT_RUN(test_complete_sample_is_shared_without_publishing_partial_round);
	UT_RUN(test_complete_sample_rechecks_original_fences_and_worker_lifecycle);
	UT_RUN(test_sample_sequence_survives_worker0_respawn_and_never_wraps);
	UT_RUN(test_worker_census_borrows_semantic_boot_not_lease_owner);
	UT_RUN(test_worker_census_source_republication_discards_before_replacement);
	free(undo_cleaner_state);
	UT_DONE();
	return ut_failed_count != 0;
}
