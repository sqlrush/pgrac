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
bool cluster_undo_retention_horizon_enabled = true;
int cluster_lmon_main_loop_interval = 2000;
int cluster_node_id = 0;

static PGPROC test_proc;
static LOCALLOCK test_locks[4];
static unsigned test_lock_count, test_lock_depth, test_scan_ends;
static bool test_foreign_lwlock, test_floor, test_fence;
static unsigned prepared, cancelled, slept, signalled, broadcasts;
static bool subscribed, early_signal, test_cancel, test_pin_after_probe;
static ClusterCtrcCapacityProbeResult test_proof;
static TransactionId test_xid = 100;
static bool test_maintenance_cut, test_ctrc_progress;
static unsigned test_ctrc_passes, test_gc_passes, test_modifier_entries, test_modifier_leaves;
bool cluster_undo_record_segment_commit_on_rollover;
int cluster_undo_cleaner_batch_segments = 8;

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
	/* A finite existing GC refusal exercises the real pass's token release. */
	return false;
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
	abort();
}
uint32
cluster_undo_record_active_segment_id(void)
{
	abort();
}
uint32
cluster_tt_slot_current_segment(int node pg_attribute_unused())
{
	abort();
}
bool
cluster_undo_segment_tt_header_scan_pass(uint32 segment pg_attribute_unused(),
										 uint8 owner pg_attribute_unused(),
										 SCN horizon pg_attribute_unused(),
										 ClusterUndoCleanerPassStats *stats pg_attribute_unused())
{
	abort();
}
void
cluster_undo_segment_advance_committed(uint32 segment pg_attribute_unused())
{
	abort();
}
ClusterUndoSegTryRecycle
cluster_undo_segment_advance_recyclable(uint32 segment pg_attribute_unused(),
										SCN horizon pg_attribute_unused(),
										uint64 epoch pg_attribute_unused())
{
	abort();
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
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	if (test_lock_depth++ != 0)
		abort();
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{
	if (test_lock_depth-- != 1)
		abort();
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
	if (test_pin_after_probe)
		PrivateRefCountArray[0].refcount = 1;
	if (early_signal)
		ConditionVariableBroadcast(&undo_cleaner_state->capacity_cv);
	return test_proof;
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
ConditionVariableTimedSleep(ConditionVariable *cv pg_attribute_unused(), long timeout,
							uint32 event pg_attribute_unused())
{
	if (!subscribed || test_lock_depth != 0 || timeout != Max(cluster_lmon_main_loop_interval, 200)
		|| !undo_cleaner_capacity_context_safe())
		abort();
	slept++;
	if (test_cancel)
		pg_re_throw();
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

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

static void
reset_wait_fixture(void)
{
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
	cluster_undo_cleaner_enabled = cluster_undo_retention_horizon_enabled = true;
	subscribed = early_signal = test_cancel = test_pin_after_probe = false;
	prepared = cancelled = slept = signalled = broadcasts = 0;
	test_proof = CLUSTER_CTRC_CAPACITY_WAIT;
	for (unsigned i = 0; i < CLUSTER_UNDO_CLEANER_WORKER_TYPES; i++) {
		undo_cleaner_state->workers[i].status = UNDO_CLEANER_READY;
		undo_cleaner_state->workers[i].latch = &test_proc.procLatch;
	}
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
		UT_ASSERT_EQ(undo_cleaner_state->capacity_wait_refused_context, 1);
		UT_ASSERT(!subscribed);
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
			(void)undo_cleaner_run_pass(&remaining);
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

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_capacity_wait_retries_signals_and_cadence_without_authorizing_free);
	UT_RUN(test_capacity_every_blocking_hold_refuses_without_sleep);
	UT_RUN(test_heavyweight_lock_identity_and_mode_are_exact);
	UT_RUN(test_capacity_cancellation_and_missing_supply_keep_original_outcome);
	UT_RUN(test_worker_lifecycle_rows_and_pid_inventory_are_independent);
	UT_RUN(test_outer_pass_keeps_terminal_supply_but_cuts_new_optional_maintenance);
	free(undo_cleaner_state);
	UT_DONE();
	return ut_failed_count != 0;
}
