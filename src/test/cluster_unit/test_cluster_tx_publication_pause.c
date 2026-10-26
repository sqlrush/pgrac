/* Copyright (c) 2026, pgrac contributors.
 * Execute complete real commit/abort functions at scheduling-only boundaries.
 * CLOG and preabort I/O remain fixture boundaries; this is not a native VM run.
 */
#include "postgres.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_xnode_profile.h"
#include "miscadmin.h"
#include "replication/origin.h"
#include "storage/proc.h"
#include "utils/inval.h"

#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

volatile uint32 CritSectionCount;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static PGPROC publication_proc;
PGPROC *MyProc = &publication_proc;
static bool forceSyncCommit;
int synchronous_commit = SYNCHRONOUS_COMMIT_ON;
XLogRecPtr XactLastRecEnd, XactLastCommitEnd;
RepOriginId replorigin_session_origin = InvalidRepOriginId;
TimestampTz replorigin_session_origin_timestamp;
static unsigned publication_flushes, publication_commits, publication_clogs;
static bool publication_bound = true;

void
pg_re_throw(void)
{
	siglongjmp(*PG_exception_stack, 1);
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	(void)condition;
	(void)file;
	(void)line;
	abort();
}

static void
publication_commit_wal(void)
{
	UT_ASSERT_EQ(CritSectionCount, 1);
	UT_ASSERT_EQ(MyProc->delayChkptFlags, DELAY_CHKPT_START);
	XactLastRecEnd = 200;
}

static void
publication_flush(XLogRecPtr end)
{
	UT_ASSERT_EQ(end, 200);
	publication_flushes++;
}

static void
publication_commit_apply(XLogRecPtr end)
{
	UT_ASSERT_EQ(end, 200);
	UT_ASSERT_EQ(publication_flushes, 1);
	publication_commits++;
}

static void
publication_commit_tree(TransactionId xid, int count, TransactionId *children)
{
	UT_ASSERT_EQ(xid, 100);
	UT_ASSERT_EQ(count, 0);
	UT_ASSERT(children == NULL);
	UT_ASSERT_EQ(publication_flushes, 1);
	UT_ASSERT_EQ(publication_commits, publication_bound ? 1 : 0);
	publication_clogs++;
}

static void publication_pause(const char *name);
#undef CLUSTER_INJECTION_PAUSE
#define CLUSTER_INJECTION_PAUSE(name) publication_pause(name)

/* Execute the complete producer control flow. WAL, native CLOG, canonical
 * apply, SCN and unrelated subsystems are explicit boundaries, never authority. */
#define GetTopTransactionIdIfAny() ((TransactionId)100)
#undef XLogLogicalInfoActive
#undef XLogStandbyInfoActive
#define XLogLogicalInfoActive() false
#define XLogStandbyInfoActive() false
#define LogLogicalInvalidations() abort()
#define smgrGetPendingDeletes(commit, ptr) (*(ptr) = NULL, 0)
#define xactGetCommittedChildren(ptr) (*(ptr) = NULL, 0)
#define pgstat_get_transactional_drops(commit, ptr) (*(ptr) = NULL, 0)
#define xactGetCommittedInvalidationMessages(ptr, init) (*(ptr) = NULL, *(init) = false, 0)
#define LogStandbyInvalidations(...) abort()
#define cluster_storage_mode_enabled() true
#define cluster_xp_begin(scope, bucket) ((void)(scope))
#define cluster_xp_end(scope) ((void)(scope))
#define cluster_undo_xact_precommit_flush() ((void)0)
#define cluster_backup_pending_commit_enter() ((void)0)
#define cluster_scn_advance_for_commit() scn_encode(1, 42)
#define cluster_adg_emit_thread_barrier() InvalidXLogRecPtr
#define cluster_itl_touch_has_pending() false
#define cluster_itl_xact_precommit_finish(...) abort()
#define cluster_tt_local_precommit_durable_finish(...) publication_bound
#define cluster_sf_xact_commit_brake() abort()
#define cluster_smart_fusion false
#define cluster_shared_config true
#define cluster_backup_pending_commit_abort(...) ((void)0)
#define cluster_scn_durable_pending_abort_self() false
#define cluster_space_drop_prepare(...) NULL
#define cluster_space_drop_authorize(...) false
#define cluster_space_drop_is_deferred_v4(...) false
#define cluster_space_drop_wal(...) NULL
#define cluster_backup_pending_commit_exit() ((void)0)
#define cluster_scn_durable_pending_fill_lsn(...) true
#define replorigin_session_advance(...) abort()
#define GetCurrentTransactionStopTimestamp() ((TimestampTz)1)
#define TransactionTreeSetCommitTsData(...) ((void)0)
#define XactLogCommitRecord(...)                                                                   \
	((void)nmsgs, (void)invalMessages, (void)RelcacheInitFileInval, (void)has_tt_fold,             \
	 (void)tt_fold, (void)space_drop_data, (void)space_drop_len, publication_commit_wal())
#define XLogFlush publication_flush
#define cluster_sf_publish_origin_durable_lsn() abort()
#define cluster_tt_local_commit_durable_apply publication_commit_apply
#define TransactionIdCommitTree publication_commit_tree
#define XLogSetAsyncXactLSN(...) ((void)0)
#define TransactionIdAsyncCommitTree(...) abort()
#define cluster_space_drop_publish(...) abort()
#define cluster_space_drop_defer_v4(...) abort()
#define cluster_space_drop_finish(...) abort()
#define cluster_scn_pending_commit_clear(...) true
#define cluster_scn_durable_pending_discharge_scn(...) true
#define TransactionIdLatest(xid, count, children) (xid)
#define cluster_tt_local_has_binding(xid) publication_bound
#define cluster_tt_local_record_commit(...) ((void)0)
#define SyncRepWaitForLSN(...) ((void)0)
#include "test_cluster_tt_checkpoint_native.inc"

static const char *commit_points[]
	= { "cluster-tx-commit-tt-before", "cluster-tx-commit-tt-after",
		"cluster-tx-commit-clog-before", "cluster-tx-commit-clog-after" };
static const char *abort_points[]
	= { "cluster-tx-abort-tt-before", "cluster-tx-abort-tt-after", "cluster-tx-abort-clog-before",
		"cluster-tx-abort-clog-after" };
static bool publication_abort;
static bool publication_preabort_ok = true;
static unsigned publication_calls;
static unsigned publication_preaborts;
static unsigned publication_aborts;
static unsigned publication_abort_recorded;

static void
publication_pause(const char *name)
{
	unsigned i = publication_calls++;
	UT_ASSERT(i < 4);
	if (i >= 4)
		return;
	UT_ASSERT(strcmp(name, publication_abort ? abort_points[i] : commit_points[i]) == 0);
	if (publication_abort) {
		UT_ASSERT_EQ(CritSectionCount, i < 2 ? 0 : 1);
		UT_ASSERT_EQ(MyProc->delayChkptFlags, 0);
		UT_ASSERT_EQ(publication_preaborts, i == 0 ? 0 : 1);
		UT_ASSERT_EQ(publication_aborts, i == 3 ? 1 : 0);
	} else {
		UT_ASSERT_EQ(CritSectionCount, 1);
		UT_ASSERT_EQ(MyProc->delayChkptFlags, DELAY_CHKPT_START);
		UT_ASSERT_EQ(publication_flushes, 1);
		UT_ASSERT_EQ(publication_commits, i == 0 ? 0 : 1);
		UT_ASSERT_EQ(publication_clogs, i == 3 ? 1 : 0);
	}
}

static bool
publication_preabort(TransactionId xid)
{
	UT_ASSERT_EQ(xid, 100);
	UT_ASSERT_EQ(CritSectionCount, 0);
	publication_preaborts++;
	return publication_preabort_ok;
}

static void
publication_abort_tree(TransactionId xid, int nchildren, TransactionId *children)
{
	UT_ASSERT_EQ(xid, 100);
	UT_ASSERT_EQ(nchildren, 0);
	UT_ASSERT(children == NULL);
	UT_ASSERT_EQ(CritSectionCount, 1);
	publication_aborts++;
}

static struct {
	SubTransactionId subTransactionId;
} publication_xact pg_attribute_unused();
#define CurrentTransactionState (&publication_xact)
#define GetCurrentTransactionIdIfAny() ((TransactionId)100)
#define TransactionIdDidCommit(xid) false
#define cluster_scn_advance_for_abort() scn_encode(1, 43)
#define cluster_itl_xact_abort_finish(...) abort()
#define cluster_itl_xact_subabort_finish(...) abort()
#define cluster_tt_local_preabort_durable_finish publication_preabort
#undef cluster_tt_local_has_binding
#define cluster_tt_local_has_binding(xid) publication_bound
#define GetCurrentTimestamp() ((TimestampTz)1)
#define XactLogAbortRecord(...) ((void)nrels, (void)xact_time, (void)abort_scn)
#undef XLogSetAsyncXactLSN
#define XLogSetAsyncXactLSN(...) ((void)0)
#define TransactionIdAbortTree publication_abort_tree
#define cluster_xact_trace_cleanup(stage) ((void)0)
#define cluster_tt_local_record_abort(xid) (publication_abort_recorded++)
#define XidCacheRemoveRunningXids(...) ((void)0)
#include "test_cluster_tx_publication_abort.inc"

static void
publication_reset(bool aborting)
{
	MyProc->delayChkptFlags = 0;
	CritSectionCount = 0;
	publication_flushes = publication_clogs = publication_commits = 0;
	XactLastRecEnd = 1;
	publication_abort = aborting;
	publication_calls = publication_preaborts = publication_aborts = 0;
	publication_abort_recorded = 0;
	publication_bound = publication_preabort_ok = true;
}

UT_TEST(test_commit_pause_boundaries_keep_wal_tt_clog_and_checkpoint_order)
{
	publication_reset(false);
	UT_ASSERT_EQ(RecordTransactionCommit(NULL), 100);
	UT_ASSERT_EQ(publication_calls, 4);
	UT_ASSERT_EQ(publication_clogs, 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(MyProc->delayChkptFlags, 0);
	UT_ASSERT_EQ(publication_flushes, 1);
	UT_ASSERT_EQ(publication_commits, 1);
}

UT_TEST(test_unbound_commit_does_not_reach_canonical_pause)
{
	publication_reset(false);
	publication_bound = false;
	UT_ASSERT_EQ(RecordTransactionCommit(NULL), 100);
	UT_ASSERT_EQ(publication_calls, 0);
	UT_ASSERT_EQ(publication_commits, 0);
	UT_ASSERT_EQ(publication_clogs, 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(MyProc->delayChkptFlags, 0);
}

UT_TEST(test_abort_pause_boundaries_bracket_real_preabort_and_native_abort)
{
	publication_reset(true);
	UT_ASSERT_EQ(RecordTransactionAbort(false), 100);
	UT_ASSERT_EQ(publication_calls, 4);
	UT_ASSERT_EQ(publication_preaborts, 1);
	UT_ASSERT_EQ(publication_aborts, 1);
	UT_ASSERT_EQ(publication_abort_recorded, 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(XactLastRecEnd, 0);
}

UT_TEST(test_unbound_and_subabort_never_reach_canonical_pause)
{
	for (int subxact = 0; subxact < 2; subxact++) {
		publication_reset(true);
		publication_bound = false;
		UT_ASSERT_EQ(RecordTransactionAbort(subxact), 100);
		UT_ASSERT_EQ(publication_calls, 0);
		UT_ASSERT_EQ(publication_preaborts, 0);
		UT_ASSERT_EQ(publication_aborts, 1);
		UT_ASSERT_EQ(publication_abort_recorded, 0);
		UT_ASSERT_EQ(CritSectionCount, 0);
	}
}

UT_TEST(test_failed_preabort_does_not_claim_successor_pause)
{
	publication_reset(true);
	publication_preabort_ok = false;
	UT_ASSERT_EQ(RecordTransactionAbort(false), 100);
	UT_ASSERT_EQ(publication_calls, 1);
	UT_ASSERT_EQ(publication_preaborts, 1);
	UT_ASSERT_EQ(publication_aborts, 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_commit_pause_boundaries_keep_wal_tt_clog_and_checkpoint_order);
	UT_RUN(test_unbound_commit_does_not_reach_canonical_pause);
	UT_RUN(test_abort_pause_boundaries_bracket_real_preabort_and_native_abort);
	UT_RUN(test_unbound_and_subabort_never_reach_canonical_pause);
	UT_RUN(test_failed_preabort_does_not_claim_successor_pause);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
