/*-------------------------------------------------------------------------
 * test_cluster_retention_sample.c
 *    Execute the native retention scan and its scalar compatibility wrapper.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_retention_sample.c
 *
 * Actual ProcArray type and product bodies are extracted verbatim. Shared
 * memory, clock and lock scheduling are explicit boundary fixtures.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/twophase.h"
#include "access/transam.h"
#include "access/xact.h"
#include "cluster/cluster_undo_retention.h"
#include "cluster/cluster_mrp.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/slot.h"
#include "replication/walsender_private.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "test_cluster_pgproc_active_types.inc"

#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

int cluster_node_id = 0;
static ProcArrayStruct *procArray;
static PGPROC entries[8];
static PGPROC *allProcs = entries;
PGPROC *MyProc = &entries[0];
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static SCN sample_clock;
static unsigned acquires, releases, clock_reads, lock_errors, shared_acquires;
static bool held, change_after_unlock, corrupt_count_after_lock;
static bool sample_mrp, clock_sampled;
static SCN sample_mrp_bound;
static ReplicationSlotCtlData slot_storage;
ReplicationSlotCtlData *ReplicationSlotCtl = &slot_storage;
ReplicationSlot *MyReplicationSlot;
int max_replication_slots = 1;
static bool control_held, control_exclusive, proc_exclusive;
static bool sample_before_feedback_lock;
static WalSnd walsnd;
WalSnd *MyWalSnd = &walsnd;
static StringInfoData reply_message;
static uint32 feedback_fields[4];
static unsigned feedback_field_index;
static bool expecting_error;
static int feedback_error_code;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static PROC_HDR proc_global;
PROC_HDR *ProcGlobal = &proc_global;
static TransactionId safe_xids[8];
static VariableCacheData variable_cache;
VariableCache ShmemVariableCache = &variable_cache;
Oid MyDatabaseId = 1;
MemoryContext CurrentMemoryContext;
static bool xidgen_held, fail_safe_xmin;
static unsigned safe_xmin_calls, slot_saves, logical_starts;
static TransactionId logical_start_xmin;
static LogicalDecodingContext logical_context;
static ReorderBuffer logical_reorder;

int
s_lock(volatile slock_t *lock, const char *file, int line, const char *func)
{
	(void)lock;
	fprintf(stderr, "unexpected spin contention %s:%d %s\n", file, line, func);
	abort();
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

bool errstart(int level, const char *domain) { (void)domain; return level >= ERROR; }
bool errstart_cold(int level, const char *domain) { return errstart(level, domain); }
int errmsg(const char *format, ...) { (void)format; return 0; }
int errmsg_internal(const char *format, ...) { (void)format; return 0; }
int errcode(int code) { feedback_error_code = code; return 0; }
void
errfinish(const char *file, int line, const char *function)
{
	if (!expecting_error) {
		fprintf(stderr, "unexpected ERROR %s:%d %s\n", file, line, function);
		abort();
	}
	pg_re_throw();
}
bool message_level_is_interesting(int level) { (void)level; return false; }
int64 pq_getmsgint64(StringInfo msg) { UT_ASSERT(msg == &reply_message); return 1; }
unsigned int
pq_getmsgint(StringInfo msg, int bytes)
{
	UT_ASSERT(msg == &reply_message && bytes == 4 && feedback_field_index < 4);
	return feedback_fields[feedback_field_index++];
}
FullTransactionId ReadNextFullTransactionId(void) { return FullTransactionIdFromU64(1000); }

bool
cluster_mrp_should_start(void)
{
	UT_ASSERT(held);
	return sample_mrp;
}

SCN
cluster_mrp_standby_consistent_scn(void)
{
	UT_ASSERT(held && sample_mrp);
	return sample_mrp_bound;
}


bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (lock == XidGenLock) {
		if (!held || !proc_exclusive || xidgen_held || mode != LW_SHARED)
			lock_errors++;
		xidgen_held = true;
		return true;
	}
	if (lock == ReplicationSlotControlLock) {
		if (control_held || held || (mode != LW_EXCLUSIVE && mode != LW_SHARED))
			lock_errors++;
		control_held = true;
		control_exclusive = mode == LW_EXCLUSIVE;
		return true;
	}
	if (sample_before_feedback_lock && !held) {
		ClusterUndoRetentionSample sample;

		sample_before_feedback_lock = false;
		UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
		UT_ASSERT_EQ(sample.horizon_scn, 700);
	}
	if (held || lock != ProcArrayLock || (mode != LW_EXCLUSIVE && mode != LW_SHARED))
		lock_errors++;
	held = true;
	proc_exclusive = mode == LW_EXCLUSIVE;
	clock_sampled = false;
	acquires++;
	if (mode == LW_SHARED) shared_acquires++;
	if (corrupt_count_after_lock)
		procArray->numProcs = -1;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	if (lock == XidGenLock) {
		if (!xidgen_held || !held || !proc_exclusive) lock_errors++;
		xidgen_held = false;
		return;
	}
	if (lock == ReplicationSlotControlLock) {
		if (!control_held || held) lock_errors++;
		control_held = control_exclusive = false;
		return;
	}
	if (!held || lock != ProcArrayLock)
		lock_errors++;
	held = false;
	proc_exclusive = false;
	releases++;
	if (change_after_unlock) {
		sample_clock = 900;
		entries[0].pid = 0;
		pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 50);
	}
}

bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	if (lock == ProcArrayLock)
		return held && (mode == LW_SHARED || proc_exclusive);
	if (lock == ReplicationSlotControlLock)
		return control_held && (mode == LW_SHARED || control_exclusive);
	if (lock == XidGenLock)
		return xidgen_held && mode == LW_SHARED;
	return false;
}
bool LWLockHeldByMe(LWLock *lock) { return LWLockHeldByMeInMode(lock, LW_SHARED); }

SCN
cluster_scn_current(void)
{
	if (!held || clock_sampled)
		lock_errors++;
	clock_sampled = true;
	clock_reads++;
	return sample_clock;
}

int
scn_time_cmp(SCN a, SCN b)
{
	return scn_local(a) < scn_local(b) ? -1 : scn_local(a) > scn_local(b) ? 1 : 0;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

/* Context/plugin/WAL persistence are boundary providers. The complete native
 * create entry, safe-xmin resolver, pin writes and aggregation execute below. */
void CheckLogicalDecodingRequirements(void) {}
bool IsTransactionState(void) { return false; }
TransactionId GetTopTransactionIdIfAny(void) { return InvalidTransactionId; }
void namestrcpy(Name name, const char *str) { strlcpy(NameStr(*name), str, NAMEDATALEN); }
bool
RecoveryInProgress(void)
{
	UT_ASSERT(held && proc_exclusive && control_exclusive);
	safe_xmin_calls++;
	if (fail_safe_xmin)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("injected safe-xmin source failure")));
	return false;
}
void ReplicationSlotReserveWal(void) { abort(); } /* Tests supply an explicit LSN. */
void
ReplicationSlotSave(void)
{
	UT_ASSERT(!held && !control_held && !xidgen_held);
	UT_ASSERT(MyReplicationSlot->dirty);
	slot_saves++;
}
static LogicalDecodingContext *
StartupDecodingContext(List *options, XLogRecPtr start_lsn, TransactionId xmin,
					   bool need_full_snapshot, bool fast_forward, bool in_create,
					   XLogReaderRoutine *routine,
					   LogicalOutputPluginWriterPrepareWrite prepare,
					   LogicalOutputPluginWriterWrite write,
					   LogicalOutputPluginWriterUpdateProgress progress)
{
	UT_ASSERT(options == NIL && start_lsn == 4096 && in_create && !fast_forward);
	UT_ASSERT(routine == NULL && prepare == NULL && write == NULL && progress == NULL);
	UT_ASSERT(!held && !control_held && !xidgen_held && slot_saves == 1);
	UT_ASSERT_EQ(MyReplicationSlot->effective_catalog_xmin, xmin);
	UT_ASSERT_EQ(procArray->replication_slot_catalog_xmin, xmin);
	if (need_full_snapshot) {
		UT_ASSERT_EQ(MyReplicationSlot->effective_xmin, xmin);
		UT_ASSERT_EQ(procArray->replication_slot_xmin, xmin);
	}
	logical_start_xmin = xmin;
	logical_starts++;
	logical_context.reorder = &logical_reorder;
	return &logical_context;
}
static void
startup_cb_wrapper(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool init)
{
	(void)ctx; (void)opt; (void)init;
	abort(); /* No plugin callback is installed by this boundary fixture. */
}

#include "test_cluster_retention_sample.inc"
#include "test_cluster_retention_publish.inc"
#include "test_cluster_retention_promise.inc"
#include "test_cluster_retention_slot.inc"
#include "test_cluster_retention_feedback.inc"
#include "test_cluster_retention_logical.inc"

static void
reset_fixture(void)
{
	free(procArray);
	procArray = calloc(1, sizeof(ProcArrayStruct) + lengthof(entries) * sizeof(int));
	if (procArray == NULL)
		abort();
	procArray->maxProcs = lengthof(entries);
	MemSet(entries, 0, sizeof(entries));
	for (unsigned i = 0; i < lengthof(entries); i++) {
		procArray->pgprocnos[i] = i;
		entries[i].pid = 100 + i;
		pg_atomic_init_u64(&entries[i].cluster_read_scn_atomic, InvalidScn);
	}
	cluster_node_id = 0;
	sample_clock = 700;
	sample_mrp = false;
	sample_mrp_bound = InvalidScn;
	MyProc = &entries[0];
	acquires = releases = clock_reads = lock_errors = shared_acquires = 0;
	held = change_after_unlock = corrupt_count_after_lock = false;
	control_held = control_exclusive = proc_exclusive = sample_before_feedback_lock = false;
	memset(&slot_storage, 0, sizeof(slot_storage));
	SpinLockInit(&slot_storage.replication_slots[0].mutex);
	slot_storage.replication_slots[0].in_use = true;
	MyReplicationSlot = NULL;
	memset(&walsnd, 0, sizeof(walsnd));
	SpinLockInit(&walsnd.mutex);
	feedback_field_index = 0;
	memset(feedback_fields, 0, sizeof(feedback_fields));
	expecting_error = false;
	feedback_error_code = 0;
	xidgen_held = fail_safe_xmin = false;
	safe_xmin_calls = slot_saves = logical_starts = 0;
	logical_start_xmin = InvalidTransactionId;
	memset(&logical_context, 0, sizeof(logical_context));
	memset(&logical_reorder, 0, sizeof(logical_reorder));
	memset(&proc_global, 0, sizeof(proc_global));
	memset(safe_xids, 0, sizeof(safe_xids));
	proc_global.xids = safe_xids;
	memset(&variable_cache, 0, sizeof(variable_cache));
	variable_cache.nextXid = FullTransactionIdFromU64(1000);
}

static bool
empty_sample(const ClusterUndoRetentionSample *sample)
{
	return sample->horizon_scn == InvalidScn && sample->clock_scn == InvalidScn
		   && sample->contributor_kind == CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_UNKNOWN
		   && sample->contributor_token == 0;
}

UT_TEST(test_empty_array_has_one_prescan_clock)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 700);
	UT_ASSERT_EQ(sample.clock_scn, 700);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
	UT_ASSERT_EQ(sample.contributor_token, 0);
	UT_ASSERT_EQ(acquires, 1);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(clock_reads, 1);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_all_database_and_vacuum_snapshot_minima_are_included)
{
	for (int oldest = 0; oldest < 3; oldest++) {
		ClusterUndoRetentionSample sample;

		reset_fixture();
		procArray->numProcs = 3;
		entries[0].databaseId = 999;
		entries[1].statusFlags = PROC_IN_VACUUM;
		for (int i = 0; i < 3; i++)
			pg_atomic_write_u64(&entries[i].cluster_read_scn_atomic, i == oldest ? 400 : 500);
		UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
		UT_ASSERT_EQ(sample.horizon_scn, 400);
		UT_ASSERT_EQ(sample.clock_scn, 700);
		UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
		UT_ASSERT_EQ(sample.contributor_token, oldest + 1);
		UT_ASSERT_EQ(lock_errors, 0);
	}
}

UT_TEST(test_equal_time_prefers_snapshot_without_changing_scalar_bits)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = 1;
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic,
						(UINT64CONST(4) << SCN_NODE_ID_SHIFT) | sample_clock);
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, sample_clock);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT_EQ(sample.contributor_token, 1);
}

UT_TEST(test_prepared_without_read_scn_does_not_invent_a_pin)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = 1;
	entries[0].pid = 0;
	entries[0].xid = 81;
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 700);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
	UT_ASSERT_EQ(sample.contributor_token, 0);
}

UT_TEST(test_unexplained_read_scn_is_unknown_and_equal_snapshot_wins)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = 1;
	entries[0].pid = 0;
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 400);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_UNKNOWN);
	UT_ASSERT_EQ(sample.contributor_token, 0);
	clock_reads = 0;
	procArray->numProcs = 2;
	pg_atomic_write_u64(&entries[1].cluster_read_scn_atomic, 400);
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT_EQ(sample.contributor_token, 2);
}

UT_TEST(test_missing_read_scn_clears_partial_minimum)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = 2;
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
	entries[1].xmin = 80;
	MemSet(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	UT_ASSERT_EQ(acquires, 1);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_invalid_clock_never_borrows_a_snapshot_time)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	sample_clock = InvalidScn;
	procArray->numProcs = 1;
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
	MemSet(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	UT_ASSERT_EQ(releases, 1);
}

UT_TEST(test_nonfinite_clock_cannot_borrow_a_snapshot_or_empty_array)
{
	for (int count = 0; count <= 1; count++) {
		ClusterUndoRetentionSample sample;

		reset_fixture();
		sample_clock = (SCN) UINT64_MAX;
		procArray->numProcs = count;
		pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
		MemSet(&sample, 0xa5, sizeof(sample));
		UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
		UT_ASSERT(empty_sample(&sample));
		UT_ASSERT_EQ(acquires, 1);
		UT_ASSERT_EQ(releases, 1);
		UT_ASSERT_EQ(clock_reads, 1);
		UT_ASSERT_EQ(lock_errors, 0);
		UT_ASSERT(!held);
	}
}

UT_TEST(test_nonfinite_reader_cannot_be_ignored_in_a_finite_clock_cut)
{
	for (int has_xmin = 0; has_xmin <= 1; has_xmin++) {
		ClusterUndoRetentionSample sample;

		reset_fixture();
		procArray->numProcs = 2;
		pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
		entries[1].xmin = has_xmin ? 80 : InvalidTransactionId;
		pg_atomic_write_u64(&entries[1].cluster_read_scn_atomic, (SCN) UINT64_MAX);
		MemSet(&sample, 0xa5, sizeof(sample));
		UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
		UT_ASSERT(empty_sample(&sample));
		UT_ASSERT_EQ(releases, 1);
		UT_ASSERT_EQ(lock_errors, 0);
		UT_ASSERT(!held);
	}
}

UT_TEST(test_scalar_compatibility_never_publishes_a_nonfinite_clock)
{
	reset_fixture();
	sample_clock = (SCN) UINT64_MAX;
	UT_ASSERT_EQ(cluster_undo_retention_horizon(), InvalidScn);
	UT_ASSERT_EQ(acquires, 1);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(clock_reads, 1);
	UT_ASSERT_EQ(lock_errors, 0);
}

UT_TEST(test_original_sample_recovers_after_nonfinite_source_is_replaced)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = 1;
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, (SCN) UINT64_MAX);
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 400);
	UT_ASSERT_EQ(sample.clock_scn, 700);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT_EQ(sample.contributor_token, 1);
	UT_ASSERT_EQ(acquires, 2);
	UT_ASSERT_EQ(releases, 2);
	UT_ASSERT_EQ(clock_reads, 2);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_unlock_cannot_change_sample_or_scalar_wrapper)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = 1;
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
	change_after_unlock = true;
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 400);
	UT_ASSERT_EQ(sample.clock_scn, 700);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT_EQ(sample.contributor_token, 1);
	UT_ASSERT_EQ(clock_reads, 1);
	UT_ASSERT_EQ(lock_errors, 0);
	reset_fixture();
	procArray->numProcs = 1;
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
	change_after_unlock = true;
	UT_ASSERT_EQ(cluster_undo_retention_horizon(), 400);
	UT_ASSERT_EQ(clock_reads, 1);
	UT_ASSERT_EQ(acquires, 1);
	UT_ASSERT_EQ(lock_errors, 0);
}

UT_TEST(test_replication_slot_pin_without_backend_refuses_horizon)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->replication_slot_xmin = FirstNormalTransactionId + 17;
	MemSet(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	UT_ASSERT_EQ(cluster_undo_retention_horizon(), InvalidScn);
	UT_ASSERT_EQ(acquires, 2);
	UT_ASSERT_EQ(releases, 2);
	UT_ASSERT_EQ(clock_reads, 2);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_catalog_slot_pin_cannot_borrow_backend_read_scn)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = 1;
	entries[0].xmin = FirstNormalTransactionId + 17;
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 100);
	procArray->replication_slot_catalog_xmin = entries[0].xmin;
	MemSet(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	UT_ASSERT_EQ(acquires, 1);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_any_noninvalid_slot_pin_requires_its_own_scn_proof)
{
	const TransactionId pins[] = {BootstrapTransactionId, FrozenTransactionId,
								  FirstNormalTransactionId, MaxTransactionId};
	ClusterUndoRetentionSample sample;

	for (unsigned i = 0; i < lengthof(pins); i++) {
		for (unsigned catalog = 0; catalog < 2; catalog++) {
			reset_fixture();
			if (catalog)
				procArray->replication_slot_catalog_xmin = pins[i];
			else
				procArray->replication_slot_xmin = pins[i];
			MemSet(&sample, 0xa5, sizeof(sample));
			UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
			UT_ASSERT(empty_sample(&sample));
			UT_ASSERT_EQ(acquires, 1);
			UT_ASSERT_EQ(releases, 1);
			UT_ASSERT_EQ(lock_errors, 0);
			UT_ASSERT(!held);
		}
	}
}

UT_TEST(test_horizon_resamples_after_both_slot_pins_are_removed)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->replication_slot_xmin = FirstNormalTransactionId + 17;
	procArray->replication_slot_catalog_xmin = FirstNormalTransactionId + 19;
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	procArray->replication_slot_xmin = InvalidTransactionId;
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	procArray->replication_slot_catalog_xmin = InvalidTransactionId;
	sample_clock = 800;
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 800);
	UT_ASSERT_EQ(sample.clock_scn, 800);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
	UT_ASSERT_EQ(sample.contributor_token, 0);
	UT_ASSERT_EQ(acquires, 3);
	UT_ASSERT_EQ(releases, 3);
	UT_ASSERT_EQ(clock_reads, 3);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

/* The allocation contains eight entries even for an invalid logical count.
 * The parent can therefore run this RED without an out-of-bounds fixture. */
UT_TEST(test_invalid_procarray_counts_refuse_the_complete_h_observation)
{
	const int counts[][2] = {{-1, 8}, {2, 1}, {0, -1}};
	ClusterUndoRetentionSample sample;

	for (unsigned i = 0; i < lengthof(counts); i++) {
		reset_fixture();
		procArray->numProcs = counts[i][0];
		procArray->maxProcs = counts[i][1];
		pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 300);
		MemSet(&sample, 0xa5, sizeof(sample));
		UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
		UT_ASSERT(empty_sample(&sample));
		UT_ASSERT_EQ(cluster_undo_retention_horizon(), InvalidScn);
		UT_ASSERT_EQ(acquires, 2);
		UT_ASSERT_EQ(releases, 2);
		UT_ASSERT_EQ(clock_reads, 2);
		UT_ASSERT_EQ(lock_errors, 0);
		UT_ASSERT(!held);
	}
}

UT_TEST(test_procarray_count_is_validated_inside_the_original_lock_cut)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	corrupt_count_after_lock = true;
	MemSet(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	UT_ASSERT_EQ(acquires, 1);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(clock_reads, 1);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_full_capacity_census_preserves_its_actual_snapshot_minimum)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = procArray->maxProcs;
	pg_atomic_write_u64(&entries[lengthof(entries) - 1].cluster_read_scn_atomic, 300);
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 300);
	UT_ASSERT_EQ(sample.clock_scn, 700);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT_EQ(sample.contributor_token, lengthof(entries));
	UT_ASSERT_EQ(acquires, 1);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(clock_reads, 1);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_valid_census_can_be_sampled_after_a_rejected_count)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = -1;
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	procArray->numProcs = 1;
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 400);
	UT_ASSERT_EQ(sample.contributor_token, 1);
	UT_ASSERT_EQ(acquires, 2);
	UT_ASSERT_EQ(releases, 2);
	UT_ASSERT_EQ(clock_reads, 2);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_null_output_and_disabled_cluster_do_not_scan)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	UT_ASSERT(!cluster_undo_retention_horizon_sample(NULL));
	cluster_node_id = -1;
	MemSet(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	UT_ASSERT_EQ(cluster_undo_retention_horizon(), InvalidScn);
	UT_ASSERT_EQ(acquires, 0);
	UT_ASSERT_EQ(clock_reads, 0);
}

UT_TEST(test_sampled_promise_refuses_later_older_publication_without_mutation)
{
	ClusterUndoRetentionSample sample;
	unsigned before;

	reset_fixture();
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	before = clock_reads;
	UT_ASSERT(!ProcArrayPublishReadScn(400, InvalidScn));
	UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_read_scn_atomic), InvalidScn);
	UT_ASSERT(ProcArrayPublishReadScn(700, InvalidScn));
	UT_ASSERT(!ProcArrayPublishReadScn(699, InvalidScn));
	UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_read_scn_atomic), 700);
	UT_ASSERT(ProcArrayPublishReadScn((UINT64CONST(3) << SCN_NODE_ID_SHIFT) | 700, InvalidScn));
	UT_ASSERT_EQ(clock_reads, before);
	UT_ASSERT_EQ(acquires, releases);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_publication_before_cut_preserves_its_original_history)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = 1;
	UT_ASSERT(ProcArrayPublishReadScn(400, InvalidScn));
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 400);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT(ProcArrayPublishReadScn(400, InvalidScn));
	UT_ASSERT(!ProcArrayPublishReadScn(399, InvalidScn));
	UT_ASSERT(ProcArrayPublishReadScn(InvalidScn, InvalidScn));
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 700);
	/* A reader's retirement does not revoke the already exported promise. */
	UT_ASSERT(!ProcArrayPublishReadScn(400, InvalidScn));
	UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_read_scn_atomic), InvalidScn);
	UT_ASSERT_EQ(acquires, releases);
	UT_ASSERT_EQ(lock_errors, 0);
}

UT_TEST(test_unknown_publication_keeps_refusal_and_never_erases_promise)
{
	ClusterUndoRetentionSample sample, zero = {0};
	const SCN unknown[] = { InvalidScn, (SCN) UINT64_MAX };

	for (unsigned i = 0; i < lengthof(unknown); i++)
	{
		reset_fixture();
		UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
		procArray->numProcs = 1;
		entries[0].xmin = 10;
		UT_ASSERT(ProcArrayPublishReadScn(unknown[i], InvalidScn));
		memset(&sample, 0xa5, sizeof(sample));
		UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
		UT_ASSERT_EQ(memcmp(&sample, &zero, sizeof(sample)), 0);
		UT_ASSERT(!ProcArrayPublishReadScn(699, InvalidScn));
		UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_read_scn_atomic), unknown[i]);
		UT_ASSERT_EQ(acquires, releases);
		UT_ASSERT_EQ(lock_errors, 0);
	}
}

UT_TEST(test_adg_origin_bounds_the_promise_inside_the_original_cut)
{
	ClusterUndoRetentionSample sample, zero = {0};

	reset_fixture();
	sample_mrp = true;
	sample_mrp_bound = 400;
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 400);
	UT_ASSERT_EQ(sample.clock_scn, 700);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_REPLICATION);
	UT_ASSERT_EQ(sample.contributor_token, 0);
	UT_ASSERT(ProcArrayPublishReadScn(400, InvalidScn));
	UT_ASSERT(!ProcArrayPublishReadScn(399, InvalidScn));
	/* An invalid or regressing source cannot lower a previous promise. */
	for (unsigned i = 0; i < 3; i++)
	{
		sample_mrp_bound = i == 0 ? InvalidScn : i == 1 ? (SCN) UINT64_MAX : 399;
		memset(&sample, 0xa5, sizeof(sample));
		UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
		UT_ASSERT_EQ(memcmp(&sample, &zero, sizeof(sample)), 0);
		UT_ASSERT(!ProcArrayPublishReadScn(399, InvalidScn));
	}
	UT_ASSERT_EQ(acquires, releases);
	UT_ASSERT_EQ(lock_errors, 0);
}

UT_TEST(test_regressing_clock_or_snapshot_cut_cannot_lower_an_old_promise)
{
	ClusterUndoRetentionSample sample, zero = {0};

	reset_fixture();
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	sample_clock = 699;
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(memcmp(&sample, &zero, sizeof(sample)), 0);
	sample_clock = 700;
	procArray->numProcs = 1;
	/* Controlled corrupt legacy publication bypasses the original owner. */
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(memcmp(&sample, &zero, sizeof(sample)), 0);
	UT_ASSERT(!ProcArrayPublishReadScn(400, InvalidScn));
	UT_ASSERT(ProcArrayPublishReadScn(700, InvalidScn));
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 700);
	UT_ASSERT_EQ(acquires, releases);
	UT_ASSERT_EQ(lock_errors, 0);
}

UT_TEST(test_publication_without_an_original_owner_refuses_before_locking)
{
	reset_fixture();
	MyProc = NULL;
	UT_ASSERT(!ProcArrayPublishReadScn(700, InvalidScn));
	MyProc = &entries[0];
	cluster_node_id = -1;
	UT_ASSERT(!ProcArrayPublishReadScn(700, InvalidScn));
	cluster_node_id = 0;
	free(procArray);
	procArray = NULL;
	UT_ASSERT(!ProcArrayPublishReadScn(700, InvalidScn));
	UT_ASSERT_EQ(acquires, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_read_scn_atomic), InvalidScn);
}

UT_TEST(test_unknown_fold_cannot_hide_a_new_finite_reader)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(!ProcArrayPublishReadScn(InvalidScn, 699));
	UT_ASSERT(!ProcArrayPublishReadScn((SCN) UINT64_MAX, 699));
	UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_read_scn_atomic), InvalidScn);
	UT_ASSERT(ProcArrayPublishReadScn(InvalidScn, 700));
	UT_ASSERT(ProcArrayPublishReadScn((SCN) UINT64_MAX, 700));
	UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_read_scn_atomic), (SCN) UINT64_MAX);
	UT_ASSERT_EQ(acquires, releases);
	UT_ASSERT_EQ(lock_errors, 0);
}

UT_TEST(test_native_promise_covers_only_finite_already_retained_cuts)
{
	ClusterUndoRetentionSample sample;
	SCN saved;

	reset_fixture();
	UT_ASSERT(!cluster_undo_retention_horizon_covers(700));
	UT_ASSERT_EQ(clock_reads, 0);
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	saved = procArray->cluster_read_scn_floor;
	UT_ASSERT(cluster_undo_retention_horizon_covers(700));
	UT_ASSERT(cluster_undo_retention_horizon_covers(699));
	UT_ASSERT(!cluster_undo_retention_horizon_covers(701));
	UT_ASSERT(!cluster_undo_retention_horizon_covers(InvalidScn));
	UT_ASSERT(!cluster_undo_retention_horizon_covers((SCN)UINT64_MAX));
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, saved);
	UT_ASSERT_EQ(shared_acquires, 4);
	UT_ASSERT_EQ(clock_reads, 1);
	UT_ASSERT_EQ(acquires, releases);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_native_promise_survives_unproven_later_cut_and_blocks_resurrection)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	procArray->numProcs = 1;
	entries[0].xmin = 10;
	UT_ASSERT(ProcArrayPublishReadScn(InvalidScn, InvalidScn));
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(cluster_undo_retention_horizon_covers(700));
	UT_ASSERT(!ProcArrayPublishReadScn(699, 699));
	UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_read_scn_atomic), InvalidScn);
	UT_ASSERT_EQ(acquires, releases);
	UT_ASSERT_EQ(lock_errors, 0);
}

UT_TEST(test_native_promise_missing_owner_or_nonfinite_floor_never_covers)
{
	reset_fixture();
	procArray->cluster_read_scn_floor = (SCN)UINT64_MAX;
	UT_ASSERT(!cluster_undo_retention_horizon_covers(700));
	procArray->cluster_read_scn_floor = InvalidScn;
	UT_ASSERT(!cluster_undo_retention_horizon_covers(700));
	cluster_node_id = -1;
	UT_ASSERT(!cluster_undo_retention_horizon_covers(700));
	cluster_node_id = 0;
	free(procArray); procArray = NULL;
	UT_ASSERT(!cluster_undo_retention_horizon_covers(700));
	UT_ASSERT_EQ(acquires, 2);
	UT_ASSERT_EQ(acquires, releases);
	UT_ASSERT_EQ(lock_errors, 0);
}

static bool
receive_feedback(TransactionId xmin, TransactionId catalog_xmin)
{
	volatile bool rejected = false;

	feedback_fields[0] = xmin;
	feedback_fields[1] = 0;
	feedback_fields[2] = catalog_xmin;
	feedback_fields[3] = 0;
	feedback_field_index = 0;
	expecting_error = true;
	PG_TRY();
	{
		ProcessStandbyHSFeedbackMessage();
	}
	PG_CATCH();
	{
		rejected = true;
	}
	PG_END_TRY();
	expecting_error = false;
	return rejected;
}

UT_TEST(test_slot_feedback_after_retained_h_refuses_before_all_pin_writes)
{
	ClusterUndoRetentionSample sample;
	ReplicationSlot before;

	reset_fixture();
	MyReplicationSlot = &slot_storage.replication_slots[0];
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	before = *MyReplicationSlot;
	MyProc->xmin = 77;
	UT_ASSERT(receive_feedback(40, 45));
	UT_ASSERT_EQ(feedback_error_code, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
	UT_ASSERT(memcmp(&before, MyReplicationSlot, sizeof(before)) == 0);
	UT_ASSERT_EQ(MyProc->xmin, 77);
	UT_ASSERT_EQ(procArray->replication_slot_xmin, InvalidTransactionId);
	UT_ASSERT_EQ(procArray->replication_slot_catalog_xmin, InvalidTransactionId);
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, 700);
	UT_ASSERT(!held && !control_held && lock_errors == 0);
	UT_ASSERT(PG_exception_stack == NULL && error_context_stack == NULL);
}

UT_TEST(test_no_slot_feedback_cannot_borrow_old_scn_after_retained_h)
{
	ClusterUndoRetentionSample sample;
	PGPROC before;

	reset_fixture();
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	MyProc->xmin = 77;
	pg_atomic_write_u64(&MyProc->cluster_read_scn_atomic, 800);
	before = *MyProc;
	UT_ASSERT(receive_feedback(40, InvalidTransactionId));
	UT_ASSERT_EQ(feedback_error_code, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
	UT_ASSERT(memcmp(&before, MyProc, sizeof(before)) == 0);
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, 700);
	UT_ASSERT(!held && !control_held && lock_errors == 0);
	UT_ASSERT(PG_exception_stack == NULL && error_context_stack == NULL);
}

UT_TEST(test_h_cut_winning_original_feedback_lock_refuses_the_new_slot_pin)
{
	ReplicationSlot before;

	reset_fixture();
	MyReplicationSlot = &slot_storage.replication_slots[0];
	before = *MyReplicationSlot;
	sample_before_feedback_lock = true;
	UT_ASSERT(receive_feedback(40, 45));
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, 700);
	UT_ASSERT(memcmp(&before, MyReplicationSlot, sizeof(before)) == 0);
	UT_ASSERT_EQ(procArray->replication_slot_xmin, InvalidTransactionId);
	UT_ASSERT(!held && !control_held && lock_errors == 0);
}

UT_TEST(test_slot_feedback_winning_cut_publishes_both_original_pins_before_h)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	MyReplicationSlot = &slot_storage.replication_slots[0];
	MyProc->xmin = 77;
	UT_ASSERT(!receive_feedback(40, 45));
	UT_ASSERT_EQ(MyReplicationSlot->data.xmin, 40);
	UT_ASSERT_EQ(MyReplicationSlot->effective_xmin, 40);
	UT_ASSERT_EQ(MyReplicationSlot->data.catalog_xmin, 45);
	UT_ASSERT_EQ(MyReplicationSlot->effective_catalog_xmin, 45);
	UT_ASSERT(MyReplicationSlot->dirty && MyReplicationSlot->just_dirtied);
	UT_ASSERT_EQ(procArray->replication_slot_xmin, 40);
	UT_ASSERT_EQ(procArray->replication_slot_catalog_xmin, 45);
	UT_ASSERT_EQ(MyProc->xmin, InvalidTransactionId);
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, InvalidScn);
	UT_ASSERT(!held && !control_held && lock_errors == 0);
}

UT_TEST(test_no_slot_raw_feedback_clears_old_scn_before_next_h_cut)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = 1;
	MyProc->xmin = 77;
	pg_atomic_write_u64(&MyProc->cluster_read_scn_atomic, 800);
	UT_ASSERT(!receive_feedback(40, InvalidTransactionId));
	UT_ASSERT_EQ(MyProc->xmin, 40);
	UT_ASSERT_EQ(pg_atomic_read_u64(&MyProc->cluster_read_scn_atomic), InvalidScn);
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, InvalidScn);
	UT_ASSERT(!held && !control_held && lock_errors == 0);
}

UT_TEST(test_empty_feedback_can_retire_original_pins_without_erasing_h)
{
	for (unsigned use_slot = 0; use_slot < 2; use_slot++) {
		ClusterUndoRetentionSample sample;

		reset_fixture();
		UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
		MyProc->xmin = 77;
		pg_atomic_write_u64(&MyProc->cluster_read_scn_atomic, 800);
		if (use_slot) {
			MyReplicationSlot = &slot_storage.replication_slots[0];
			MyReplicationSlot->data.xmin = MyReplicationSlot->effective_xmin = 40;
			MyReplicationSlot->data.catalog_xmin = MyReplicationSlot->effective_catalog_xmin = 45;
			procArray->replication_slot_xmin = 40;
			procArray->replication_slot_catalog_xmin = 45;
		}
		UT_ASSERT(!receive_feedback(InvalidTransactionId, InvalidTransactionId));
		UT_ASSERT_EQ(MyProc->xmin, InvalidTransactionId);
		UT_ASSERT_EQ(procArray->replication_slot_xmin, InvalidTransactionId);
		UT_ASSERT_EQ(procArray->replication_slot_catalog_xmin, InvalidTransactionId);
		if (use_slot) {
			UT_ASSERT_EQ(MyReplicationSlot->data.xmin, InvalidTransactionId);
			UT_ASSERT_EQ(MyReplicationSlot->effective_xmin, InvalidTransactionId);
			UT_ASSERT_EQ(MyReplicationSlot->data.catalog_xmin, InvalidTransactionId);
			UT_ASSERT_EQ(MyReplicationSlot->effective_catalog_xmin, InvalidTransactionId);
		}
		else
			UT_ASSERT_EQ(pg_atomic_read_u64(&MyProc->cluster_read_scn_atomic), InvalidScn);
		UT_ASSERT_EQ(procArray->cluster_read_scn_floor, 700);
		UT_ASSERT(!held && !control_held && lock_errors == 0);
	}
}

UT_TEST(test_noncluster_feedback_keeps_native_pin_and_scn_semantics)
{
	for (unsigned use_slot = 0; use_slot < 2; use_slot++) {
		reset_fixture();
		cluster_node_id = -1;
		procArray->cluster_read_scn_floor = (SCN)UINT64_MAX;
		pg_atomic_write_u64(&MyProc->cluster_read_scn_atomic, 800);
		if (use_slot) MyReplicationSlot = &slot_storage.replication_slots[0];
		UT_ASSERT(!receive_feedback(45, 40));
		if (use_slot) {
			UT_ASSERT_EQ(procArray->replication_slot_xmin, 45);
			UT_ASSERT_EQ(procArray->replication_slot_catalog_xmin, 40);
			UT_ASSERT_EQ(MyProc->xmin, InvalidTransactionId);
		}
		else UT_ASSERT_EQ(MyProc->xmin, 40);
		UT_ASSERT_EQ(pg_atomic_read_u64(&MyProc->cluster_read_scn_atomic), 800);
		UT_ASSERT(!held && !control_held && lock_errors == 0);
	}
}

UT_TEST(test_nonfinite_promise_and_slot_special_raw_pins_stay_closed)
{
	const TransactionId pins[][2] = {{40, 45}, {InvalidTransactionId, 45},
		{FrozenTransactionId, InvalidTransactionId}};

	for (unsigned i = 0; i < lengthof(pins); i++) {
		ReplicationSlot before;

		reset_fixture();
		procArray->cluster_read_scn_floor = (SCN)UINT64_MAX;
		MyReplicationSlot = &slot_storage.replication_slots[0];
		before = *MyReplicationSlot;
		MyProc->xmin = 77;
		UT_ASSERT(receive_feedback(pins[i][0], pins[i][1]));
		UT_ASSERT(memcmp(&before, MyReplicationSlot, sizeof(before)) == 0);
		UT_ASSERT_EQ(MyProc->xmin, 77);
		UT_ASSERT_EQ(procArray->cluster_read_scn_floor, (SCN)UINT64_MAX);
		UT_ASSERT(!held && !control_held && lock_errors == 0);
	}
}

UT_TEST(test_catalog_only_no_slot_feedback_is_an_unknown_history_owner)
{
	const TransactionId data_pins[] = {InvalidTransactionId, FrozenTransactionId};

	for (unsigned i = 0; i < lengthof(data_pins); i++) {
		for (unsigned promised = 0; promised < 2; promised++) {
			ClusterUndoRetentionSample sample;
			PGPROC before;

			reset_fixture();
			procArray->numProcs = 1;
			if (promised) UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
			MyProc->xmin = 77;
			pg_atomic_write_u64(&MyProc->cluster_read_scn_atomic, 800);
			before = *MyProc;
			UT_ASSERT_EQ(receive_feedback(data_pins[i], 45), promised != 0);
			if (promised)
				UT_ASSERT(memcmp(&before, MyProc, sizeof(before)) == 0);
			else {
				UT_ASSERT_EQ(MyProc->xmin, 45);
				UT_ASSERT_EQ(pg_atomic_read_u64(&MyProc->cluster_read_scn_atomic), InvalidScn);
				UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
				UT_ASSERT(empty_sample(&sample));
			}
			UT_ASSERT(!held && !control_held && lock_errors == 0);
		}
	}
}

static void
use_logical_slot(void)
{
	MyReplicationSlot = &slot_storage.replication_slots[0];
	MyReplicationSlot->data.database = MyDatabaseId;
}

static bool
create_logical_context(bool need_full_snapshot)
{
	volatile bool accepted = false;

	expecting_error = true;
	PG_TRY();
	{
		UT_ASSERT(CreateInitDecodingContext("test_output", NIL, need_full_snapshot,
										   4096, NULL, NULL, NULL, NULL) == &logical_context);
		accepted = true;
	}
	PG_CATCH();
	{
		UT_ASSERT_EQ(feedback_error_code, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
	}
	PG_END_TRY();
	expecting_error = false;
	return accepted;
}

UT_TEST(test_logical_initial_slot_after_h_refuses_before_pin_or_decode)
{
	for (int full = 0; full < 2; full++) {
		ClusterUndoRetentionSample sample;

		reset_fixture();
		UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
		UT_ASSERT_EQ(sample.horizon_scn, 700);
		use_logical_slot();
		UT_ASSERT(!create_logical_context(full));
		UT_ASSERT_EQ(MyReplicationSlot->effective_xmin, InvalidTransactionId);
		UT_ASSERT_EQ(MyReplicationSlot->effective_catalog_xmin, InvalidTransactionId);
		UT_ASSERT_EQ(MyReplicationSlot->data.catalog_xmin, InvalidTransactionId);
		UT_ASSERT_EQ(procArray->replication_slot_xmin, InvalidTransactionId);
		UT_ASSERT_EQ(procArray->replication_slot_catalog_xmin, InvalidTransactionId);
		UT_ASSERT_EQ(procArray->cluster_read_scn_floor, 700);
		UT_ASSERT_EQ(slot_saves, 0);
		UT_ASSERT_EQ(logical_starts, 0);
		UT_ASSERT(!MyReplicationSlot->dirty);
		UT_ASSERT(!held && !control_held && !xidgen_held && lock_errors == 0);
	}
}

UT_TEST(test_h_winning_original_logical_slot_lock_blocks_history_resurrection)
{
	for (int full = 0; full < 2; full++) {
		reset_fixture();
		use_logical_slot();
		sample_before_feedback_lock = true;
		UT_ASSERT(!create_logical_context(full));
		UT_ASSERT_EQ(procArray->cluster_read_scn_floor, 700);
		UT_ASSERT_EQ(MyReplicationSlot->effective_catalog_xmin, InvalidTransactionId);
		UT_ASSERT_EQ(procArray->replication_slot_catalog_xmin, InvalidTransactionId);
		UT_ASSERT_EQ(slot_saves, 0);
		UT_ASSERT_EQ(logical_starts, 0);
		UT_ASSERT(!held && !control_held && !xidgen_held && lock_errors == 0);
	}
}

UT_TEST(test_logical_slot_first_installs_original_safe_pins_before_h)
{
	for (int full = 0; full < 2; full++) {
		ClusterUndoRetentionSample sample;

		reset_fixture();
		use_logical_slot();
		procArray->numProcs = 2;
		safe_xids[0] = 130;
		safe_xids[1] = 120;
		pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
		pg_atomic_write_u64(&entries[1].cluster_read_scn_atomic, 500);
		UT_ASSERT(create_logical_context(full));
		UT_ASSERT_EQ(safe_xmin_calls, 1);
		UT_ASSERT_EQ(logical_start_xmin, 120);
		UT_ASSERT_EQ(MyReplicationSlot->data.catalog_xmin, 120);
		UT_ASSERT_EQ(MyReplicationSlot->effective_xmin, full ? 120 : InvalidTransactionId);
		UT_ASSERT_EQ(procArray->replication_slot_xmin, full ? 120 : InvalidTransactionId);
		UT_ASSERT_EQ(procArray->replication_slot_catalog_xmin, 120);
		UT_ASSERT_EQ(slot_saves, 1);
		UT_ASSERT_EQ(logical_starts, 1);
		UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
		UT_ASSERT(empty_sample(&sample));
		UT_ASSERT_EQ(procArray->cluster_read_scn_floor, InvalidScn);
		UT_ASSERT(!held && !control_held && !xidgen_held && lock_errors == 0);
	}
}

UT_TEST(test_catalog_only_logical_slot_cannot_borrow_pgproc_scn)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	use_logical_slot();
	procArray->numProcs = 1;
	MyProc->xmin = 90;
	pg_atomic_write_u64(&MyProc->cluster_read_scn_atomic, 400);
	UT_ASSERT(create_logical_context(false));
	UT_ASSERT_EQ(MyProc->xmin, 90);
	UT_ASSERT_EQ(pg_atomic_read_u64(&MyProc->cluster_read_scn_atomic), 400);
	UT_ASSERT_EQ(MyReplicationSlot->effective_xmin, InvalidTransactionId);
	UT_ASSERT_EQ(MyReplicationSlot->effective_catalog_xmin, 1000);
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, InvalidScn);
}

UT_TEST(test_logical_safe_xmin_error_releases_owned_locks_without_pin)
{
	reset_fixture();
	use_logical_slot();
	fail_safe_xmin = true;
	UT_ASSERT(!create_logical_context(true));
	UT_ASSERT_EQ(safe_xmin_calls, 1);
	UT_ASSERT_EQ(MyReplicationSlot->effective_xmin, InvalidTransactionId);
	UT_ASSERT_EQ(MyReplicationSlot->effective_catalog_xmin, InvalidTransactionId);
	UT_ASSERT_EQ(procArray->replication_slot_catalog_xmin, InvalidTransactionId);
	UT_ASSERT_EQ(slot_saves, 0);
	UT_ASSERT_EQ(logical_starts, 0);
	UT_ASSERT(!held && !control_held && !xidgen_held && lock_errors == 0);
}

UT_TEST(test_noncluster_logical_initial_slot_keeps_original_publication)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	cluster_node_id = -1;
	use_logical_slot();
	UT_ASSERT(create_logical_context(true));
	UT_ASSERT_EQ(logical_start_xmin, 1000);
	UT_ASSERT_EQ(MyReplicationSlot->effective_xmin, 1000);
	UT_ASSERT_EQ(procArray->replication_slot_xmin, 1000);
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, 700);
	UT_ASSERT_EQ(slot_saves, 1);
	UT_ASSERT_EQ(logical_starts, 1);
	UT_ASSERT(!held && !control_held && !xidgen_held && lock_errors == 0);
}

int
main(void)
{
	UT_PLAN(46);
	UT_RUN(test_empty_array_has_one_prescan_clock);
	UT_RUN(test_all_database_and_vacuum_snapshot_minima_are_included);
	UT_RUN(test_equal_time_prefers_snapshot_without_changing_scalar_bits);
	UT_RUN(test_prepared_without_read_scn_does_not_invent_a_pin);
	UT_RUN(test_unexplained_read_scn_is_unknown_and_equal_snapshot_wins);
	UT_RUN(test_missing_read_scn_clears_partial_minimum);
	UT_RUN(test_invalid_clock_never_borrows_a_snapshot_time);
	UT_RUN(test_nonfinite_clock_cannot_borrow_a_snapshot_or_empty_array);
	UT_RUN(test_nonfinite_reader_cannot_be_ignored_in_a_finite_clock_cut);
	UT_RUN(test_scalar_compatibility_never_publishes_a_nonfinite_clock);
	UT_RUN(test_original_sample_recovers_after_nonfinite_source_is_replaced);
	UT_RUN(test_unlock_cannot_change_sample_or_scalar_wrapper);
	UT_RUN(test_replication_slot_pin_without_backend_refuses_horizon);
	UT_RUN(test_catalog_slot_pin_cannot_borrow_backend_read_scn);
	UT_RUN(test_any_noninvalid_slot_pin_requires_its_own_scn_proof);
	UT_RUN(test_horizon_resamples_after_both_slot_pins_are_removed);
	UT_RUN(test_invalid_procarray_counts_refuse_the_complete_h_observation);
	UT_RUN(test_procarray_count_is_validated_inside_the_original_lock_cut);
	UT_RUN(test_full_capacity_census_preserves_its_actual_snapshot_minimum);
	UT_RUN(test_valid_census_can_be_sampled_after_a_rejected_count);
	UT_RUN(test_null_output_and_disabled_cluster_do_not_scan);
	UT_RUN(test_sampled_promise_refuses_later_older_publication_without_mutation);
	UT_RUN(test_publication_before_cut_preserves_its_original_history);
	UT_RUN(test_unknown_publication_keeps_refusal_and_never_erases_promise);
	UT_RUN(test_adg_origin_bounds_the_promise_inside_the_original_cut);
	UT_RUN(test_regressing_clock_or_snapshot_cut_cannot_lower_an_old_promise);
	UT_RUN(test_publication_without_an_original_owner_refuses_before_locking);
	UT_RUN(test_unknown_fold_cannot_hide_a_new_finite_reader);
	UT_RUN(test_native_promise_covers_only_finite_already_retained_cuts);
	UT_RUN(test_native_promise_survives_unproven_later_cut_and_blocks_resurrection);
	UT_RUN(test_native_promise_missing_owner_or_nonfinite_floor_never_covers);
	UT_RUN(test_slot_feedback_after_retained_h_refuses_before_all_pin_writes);
	UT_RUN(test_no_slot_feedback_cannot_borrow_old_scn_after_retained_h);
	UT_RUN(test_h_cut_winning_original_feedback_lock_refuses_the_new_slot_pin);
	UT_RUN(test_slot_feedback_winning_cut_publishes_both_original_pins_before_h);
	UT_RUN(test_no_slot_raw_feedback_clears_old_scn_before_next_h_cut);
	UT_RUN(test_empty_feedback_can_retire_original_pins_without_erasing_h);
	UT_RUN(test_noncluster_feedback_keeps_native_pin_and_scn_semantics);
	UT_RUN(test_nonfinite_promise_and_slot_special_raw_pins_stay_closed);
	UT_RUN(test_catalog_only_no_slot_feedback_is_an_unknown_history_owner);
	UT_RUN(test_logical_initial_slot_after_h_refuses_before_pin_or_decode);
	UT_RUN(test_h_winning_original_logical_slot_lock_blocks_history_resurrection);
	UT_RUN(test_logical_slot_first_installs_original_safe_pins_before_h);
	UT_RUN(test_catalog_only_logical_slot_cannot_borrow_pgproc_scn);
	UT_RUN(test_logical_safe_xmin_error_releases_owned_locks_without_pin);
	UT_RUN(test_noncluster_logical_initial_slot_keeps_original_publication);
	free(procArray);
	UT_DONE();
	return ut_failed_count != 0;
}
