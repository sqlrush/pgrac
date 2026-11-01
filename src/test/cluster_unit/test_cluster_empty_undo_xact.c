/* Original native transaction dispatch with real empty-undo TT consumers.
 * WAL storage, CLOG, SCN and unrelated subsystems remain explicit leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef EMPTY_UNDO_XACT_PREFLIGHT_SOURCE
#define EMPTY_UNDO_PREFLIGHT_FIXTURE_MAIN empty_xact_preflight_fixture_main
#define EMPTY_UNDO_XACT_PREFLIGHT_SOURCE "test_cluster_empty_undo_preflight.c"
#endif
#include EMPTY_UNDO_XACT_PREFLIGHT_SOURCE

#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_xnode_profile.h"
#include "replication/origin.h"
#include "utils/inval.h"

static bool forceSyncCommit;
int synchronous_commit;
int MyXactFlags;
XLogRecPtr XactLastRecEnd, XactLastCommitEnd;
RepOriginId replorigin_session_origin = InvalidRepOriginId;
XLogRecPtr replorigin_session_origin_lsn;
TimestampTz replorigin_session_origin_timestamp;
bool cluster_smart_fusion;
static unsigned native_commits, native_aborts, native_flushes, native_clogs;
static unsigned native_undo_flushes, native_pending_aborts, native_sync_waits;
static int native_flush_error;
static struct { SubTransactionId subTransactionId; } native_state pg_attribute_unused();
static ClusterCanonicalTxnBinding native_binding;
static const XLogRecPtr native_commit_lsn = UINT64_C(0x1000000);
static const XLogRecPtr native_abort_lsn = UINT64_C(0x2000000);

static void
native_bind_emitted(void)
{
	XactLastRecEnd = g_bind_emit_lsn;
}

static void
native_empty_undo_flush(void)
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(MyProc->delayChkptFlags, 0);
	native_undo_flushes++;
	if (native_flush_error != 0)
		ereport(ERROR, (errcode(native_flush_error), errmsg("controlled precommit flush error")));
}

static XLogRecPtr
native_commit_record(TimestampTz time, int nchildren, TransactionId *children,
					 int nrels, RelFileLocator *rels, int nstats, xl_xact_stats_item *stats,
					 int nmsgs, SharedInvalidationMessage *msgs, bool init, int flags,
					 TransactionId twophase, const char *gid, SCN scn,
					 const xl_xact_tt_commit *fold, const char *space, uint32 length)
{
	UT_ASSERT_EQ(time, 1);
	UT_ASSERT_EQ(nchildren + nrels + nstats + nmsgs + flags + length, 0);
	UT_ASSERT(children == NULL && rels == NULL && stats == NULL && msgs == NULL);
	UT_ASSERT(!init && !TransactionIdIsValid(twophase) && gid == NULL && space == NULL);
	UT_ASSERT_EQ(CritSectionCount, 1);
	UT_ASSERT_EQ(MyProc->delayChkptFlags, DELAY_CHKPT_START);
	UT_ASSERT_EQ(scn, 91);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 1);
	UT_ASSERT(fold != NULL);
	if (fold != NULL) {
		UT_ASSERT_EQ(fold->xid, 100);
		UT_ASSERT_EQ(fold->segment_id, native_binding.segment_id);
		UT_ASSERT_EQ(fold->segment_generation, native_binding.segment_generation);
		UT_ASSERT_EQ(fold->instance, native_binding.origin_instance);
		UT_ASSERT_EQ(fold->slot_offset, native_binding.slot_offset);
		UT_ASSERT_EQ(fold->wrap, native_binding.slot_wrap);
		UT_ASSERT_EQ(fold->commit_scn, 91);
		UT_ASSERT_EQ(fold->format_version, CLUSTER_XACT_TT_COMMIT_VERSION);
		UT_ASSERT_EQ(fold->flags, 0);
		UT_ASSERT_EQ(fold->reserved, 0);
	}
	UT_ASSERT_EQ(g_last_written_slot.status, TT_SLOT_ACTIVE);
	UT_ASSERT_EQ(native_flushes + native_clogs, 0);
	native_commits++;
	g_write_requires_flush = native_commit_lsn;
	XactLastRecEnd = native_commit_lsn;
	return XactLastRecEnd;
}

static XLogRecPtr
native_abort_record(TimestampTz time, int nchildren, TransactionId *children,
					int nrels, RelFileLocator *rels, int nstats, xl_xact_stats_item *stats,
					int flags, TransactionId twophase, const char *gid, SCN scn)
{
	UT_ASSERT_EQ(time, 1);
	UT_ASSERT_EQ(nchildren + nrels + nstats + flags, 0);
	UT_ASSERT(children == NULL && rels == NULL && stats == NULL);
	UT_ASSERT(!TransactionIdIsValid(twophase) && gid == NULL);
	UT_ASSERT_EQ(scn, 92);
	UT_ASSERT_EQ(CritSectionCount, 1);
	UT_ASSERT_EQ(g_abort_exact_emit_calls, 1);
	UT_ASSERT_EQ(g_flushed_lsn, g_abort_exact_lsn);
	UT_ASSERT_EQ(g_last_written_slot.status, TT_SLOT_ABORTED);
	native_aborts++;
	XactLastRecEnd = native_abort_lsn;
	return XactLastRecEnd;
}

static void
native_flush_record(XLogRecPtr lsn)
{
	UT_ASSERT_EQ(native_commits, 1);
	UT_ASSERT_EQ(native_clogs, 0);
	UT_ASSERT_EQ(lsn, native_commit_lsn);
	UT_ASSERT_EQ(CritSectionCount, 1);
	UT_ASSERT_EQ(MyProc->delayChkptFlags, DELAY_CHKPT_START);
	native_flushes++;
	XLogFlush(lsn);
	g_flush_ptr = lsn;
}

static void
native_clog(bool commit, TransactionId xid, int count, TransactionId *children)
{
	UT_ASSERT_EQ(xid, 100);
	UT_ASSERT_EQ(count, 0);
	UT_ASSERT(children == NULL);
	UT_ASSERT_EQ(CritSectionCount, 1);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 1);
	UT_ASSERT_EQ(empty_overlay_calls, 0);
	UT_ASSERT_EQ(g_last_written_slot.status, commit ? TT_SLOT_COMMITTED : TT_SLOT_ABORTED);
	if (commit) {
		UT_ASSERT_EQ(native_flushes, 1);
		UT_ASSERT_EQ(MyProc->delayChkptFlags, DELAY_CHKPT_START);
	}
	native_clogs++;
}

static void
native_async_lsn(XLogRecPtr lsn)
{
	/* A folded TT commit must force the synchronous branch even with GUC off. */
	UT_ASSERT_EQ(native_commits, 0);
	UT_ASSERT_EQ(lsn, native_abort_lsn);
}

static void
native_sync_wait(XLogRecPtr lsn, bool commit)
{
	UT_ASSERT(commit && lsn == native_commit_lsn);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(MyProc->delayChkptFlags, 0);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	native_sync_waits++;
}

/* Native durability/visibility sinks are leaves; the original branches and
 * TT stage/apply/terminal hooks below run without replacement. */
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
#define cluster_xp_begin(scope, bucket) ((void)(scope))
#define cluster_xp_end(scope) ((void)(scope))
#define cluster_undo_xact_precommit_flush native_empty_undo_flush
#define cluster_backup_pending_commit_enter() ((void)0)
#define cluster_scn_advance_for_commit() ((SCN)91)
#define cluster_scn_advance_for_abort() ((SCN)92)
#define cluster_adg_emit_thread_barrier() InvalidXLogRecPtr
#define cluster_itl_touch_has_pending() false
#define cluster_itl_xact_precommit_finish(...) abort()
#define cluster_itl_xact_abort_finish(...) abort()
#define cluster_itl_xact_subabort_finish(...) abort()
#define cluster_sf_xact_commit_brake() abort()
#define cluster_backup_pending_commit_abort(...) (native_pending_aborts++)
#define cluster_scn_durable_pending_abort_self() false
#define cluster_space_drop_prepare(...) NULL
#define cluster_space_drop_wal(...) NULL
#define cluster_space_drop_mark_dirty(...) abort()
#define cluster_backup_pending_commit_exit() ((void)0)
#define cluster_scn_durable_pending_fill_lsn(...) true
#define replorigin_session_advance(...) abort()
#define GetCurrentTransactionStopTimestamp() ((TimestampTz)1)
#define TransactionTreeSetCommitTsData(...) ((void)0)
#define XactLogCommitRecord native_commit_record
#define XactLogAbortRecord native_abort_record
#define XLogFlush native_flush_record
#define cluster_sf_publish_origin_durable_lsn() abort()
#define TransactionIdCommitTree(xid, count, children) native_clog(true, xid, count, children)
#define TransactionIdAbortTree(xid, count, children) native_clog(false, xid, count, children)
#define XLogSetAsyncXactLSN native_async_lsn
#define TransactionIdAsyncCommitTree(xid, count, children, lsn) native_clog(true, xid, count, children)
#define cluster_space_drop_publish(...) abort()
#define cluster_space_drop_finish(...) abort()
#define cluster_scn_pending_commit_clear(...) true
#define cluster_scn_durable_pending_discharge_scn(...) true
#define TransactionIdLatest(xid, count, children) (xid)
#define SyncRepWaitForLSN native_sync_wait
#define TransactionIdDidCommit(xid) false
#define CurrentTransactionState (&native_state)
#define XidCacheRemoveRunningXids(...) abort()
#define cluster_xact_trace_cleanup(stage) ((void)0)
#undef CLUSTER_INJECTION_POINT
#define CLUSTER_INJECTION_POINT(name) ((void)0)
#include "test_cluster_empty_undo_xact.inc"

static void
native_reset(bool rowmark)
{
	empty_reset(rowmark);
	native_commits = native_aborts = native_flushes = native_clogs = 0;
	native_undo_flushes = native_pending_aborts = native_sync_waits = 0;
	native_flush_error = 0;
	forceSyncCommit = false;
	MyXactFlags = 0;
	XactLastRecEnd = XactLastCommitEnd = InvalidXLogRecPtr;
	cluster_shared_config = true;
	g_after_bind_emit_hook = native_bind_emitted;
}

static bool
native_finish_transaction(bool commit)
{
	volatile bool caught = false;
	TransactionId latest = InvalidTransactionId;
	ClusterSpaceDropState *drop = NULL;

	PG_TRY();
	{
		latest = commit ? RecordTransactionCommit(&drop) : RecordTransactionAbort(false);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(!caught);
	if (caught)
		return false;
	UT_ASSERT_EQ(latest, empty_top);
	UT_ASSERT(drop == NULL);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(MyProc->delayChkptFlags, 0);
	UT_ASSERT_EQ(XactLastRecEnd, 0);
	return true;
}

UT_TEST(test_empty_native_commit_and_abort_finish_the_original_binding)
{
	for (unsigned mode = 0; mode < 6; mode++) {
		ClusterCanonicalTxnBinding binding;
		bool commit = mode < 4;
		void *bindings;

		native_reset((mode & 1) != 0);
		synchronous_commit = mode < 2 ? SYNCHRONOUS_COMMIT_OFF : SYNCHRONOUS_COMMIT_ON;
		if (!empty_preflight(&binding))
			return;
		empty_assert_active(&binding);
		native_binding = binding;
		bindings = cluster_tt_local_bindings;
		if (!native_finish_transaction(commit))
			return;
		empty_assert_finished(&binding, commit);
		UT_ASSERT_EQ(native_clogs, 1);
		UT_ASSERT_EQ(native_commits, commit);
		UT_ASSERT_EQ(native_aborts, !commit);
		UT_ASSERT_EQ(native_flushes, commit);
		UT_ASSERT_EQ(native_undo_flushes, commit);
		UT_ASSERT_EQ(native_sync_waits, commit);
		UT_ASSERT_EQ(XactLastCommitEnd, commit ? native_commit_lsn : 0);
		UT_ASSERT_EQ(g_bind_emit_calls, 1);
		UT_ASSERT_EQ(g_commit_0x30_emits, 0);
		UT_ASSERT(!g_write_before_flush);
		/* A failed terminal assertion may leave ownership with the fixture;
		 * its next reset frees that binding. */
		if (cluster_tt_local_bindings != bindings)
			free(bindings);
	}
}

UT_TEST(test_empty_native_precommit_errors_preserve_binding_until_original_abort)
{
	static const int errors[] = { ERRCODE_IO_ERROR, ERRCODE_QUERY_CANCELED };

	for (unsigned i = 0; i < lengthof(errors); i++) {
		ClusterCanonicalTxnBinding binding;
		ClusterSpaceDropState *drop = NULL;
		volatile bool caught = false;
		void *bindings;

		native_reset(false);
		if (!empty_preflight(&binding))
			return;
		native_binding = binding;
		bindings = cluster_tt_local_bindings;
		native_flush_error = errors[i];
		PG_TRY();
		{
			(void)RecordTransactionCommit(&drop);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(last_ereport_errcode, errors[i]);
		UT_ASSERT_EQ(native_pending_aborts, 1);
		UT_ASSERT_EQ(native_commits + native_clogs + native_flushes, 0);
		empty_assert_active(&binding);
		if (!native_finish_transaction(false))
			return;
		empty_assert_finished(&binding, false);
		UT_ASSERT_EQ(native_aborts, 1);
		if (cluster_tt_local_bindings != bindings)
			free(bindings);
	}
}

UT_TEST(test_empty_native_no_xid_controls_keep_lazy_binding)
{
	native_reset(false);
	list_free(empty_statement.resultRelations);
	empty_statement.resultRelations = NIL;
	cluster_executor_preflight_write(&empty_statement, &empty_estate, 0);
	UT_ASSERT(native_finish_transaction(true));
	UT_ASSERT(native_finish_transaction(false));
	UT_ASSERT_EQ(native_commits + native_aborts + native_clogs + native_flushes, 0);
	UT_ASSERT_EQ(native_undo_flushes + empty_xids + g_bind_emit_calls, 0);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
}

#ifdef EMPTY_UNDO_XACT_FIXTURE_MAIN
int EMPTY_UNDO_XACT_FIXTURE_MAIN(void);
#define main EMPTY_UNDO_XACT_FIXTURE_MAIN
#endif

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_empty_native_commit_and_abort_finish_the_original_binding);
	UT_RUN(test_empty_native_precommit_errors_preserve_binding_until_original_abort);
	UT_RUN(test_empty_native_no_xid_controls_keep_lazy_binding);
	UT_DONE();
	return ut_failed_count != 0;
}

#ifdef EMPTY_UNDO_XACT_FIXTURE_MAIN
#undef main
#endif
