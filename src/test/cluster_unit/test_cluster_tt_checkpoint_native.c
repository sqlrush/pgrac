/* Author: SqlRush <sqlrush@gmail.com>
 * Execute the complete original RecordTransactionCommit and native COMMIT
 * emitter against the existing TT/checkpoint durability fixture. A second
 * thread stops the producer at explicit boundaries; the original checkpoint
 * wait resumes it before running the real header write/fsync. Precommit
 * reservation, WAL storage, CLOG, process lookup and scheduling are boundaries.
 * This is not a postmaster crash test or qualified recovery-base authority.
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 */
int checkpoint_cuts_fixture_main(void);
#define TT_CHECKPOINT_TEST_MAIN checkpoint_cuts_fixture_main
#include "test_cluster_tt_checkpoint_cuts.c"

#include <pthread.h>
#include "access/xact.h"
#include "access/xloginsert.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_xnode_profile.h"
#include "replication/origin.h"
#include "utils/inval.h"

typedef enum NativeCommitCut {
	NATIVE_RESERVED,
	NATIVE_INSERTED,
	NATIVE_FLUSHED,
	NATIVE_STAMPED,
	NATIVE_RELEASED
} NativeCommitCut;

static pthread_mutex_t native_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t native_cond = PTHREAD_COND_INITIALIZER;
static pthread_t native_thread;
static NativeCommitCut native_cut;
static bool native_paused, native_resume, native_done, native_order_ok;
static unsigned native_inserts, native_flushes, native_clogs;
static TransactionId native_result;
static char native_wal[1024];
static uint32 native_wal_len;
static uint8 native_info;
static bool forceSyncCommit;
int synchronous_commit = SYNCHRONOUS_COMMIT_OFF;
int MyXactFlags;
XLogRecPtr XactLastRecEnd, XactLastCommitEnd;
Oid MyDatabaseId = 5, MyDatabaseTableSpace = DEFAULTTABLESPACE_OID;
RepOriginId replorigin_session_origin = InvalidRepOriginId;
XLogRecPtr replorigin_session_origin_lsn;
TimestampTz replorigin_session_origin_timestamp;
bool cluster_shared_config = true;
bool cluster_smart_fusion;

static void
native_pause(NativeCommitCut cut)
{
	if (cut != native_cut)
		return;
	pthread_mutex_lock(&native_mutex);
	native_paused = true;
	pthread_cond_signal(&native_cond);
	while (!native_resume)
		pthread_cond_wait(&native_cond, &native_mutex);
	pthread_mutex_unlock(&native_mutex);
}

static bool
native_precommit(TransactionId xid, SCN scn, xl_xact_tt_commit *fold)
{
	native_order_ok &= xid == 100 && scn == scn_encode(1, 42) && CritSectionCount == 0
					   && MyProc->delayChkptFlags == 0;
	memset(fold, 0, sizeof(*fold));
	fold->segment_id = tt_commit_stage.segment_id;
	fold->slot_offset = tt_commit_stage.slot_offset;
	fold->wrap = tt_commit_stage.successor.wrap;
	fold->commit_scn = scn;
	fold->xid = xid;
	fold->instance = tt_commit_stage.owner;
	fold->format_version = CLUSTER_XACT_TT_COMMIT_VERSION;
	fold->segment_generation = tt_commit_stage.generation.value;
	native_pause(NATIVE_RESERVED);
	return true;
}

void
XLogBeginInsert(void)
{
	native_wal_len = 0;
}

void
XLogRegisterData(char *data, uint32 len)
{
	if (len > sizeof(native_wal) - native_wal_len)
		abort();
	memcpy(native_wal + native_wal_len, data, len);
	native_wal_len += len;
}

void
XLogSetRecordFlags(uint8 flags)
{
	if (flags != XLOG_INCLUDE_ORIGIN)
		abort();
}

XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	native_order_ok &= rmid == RM_XACT_ID && CritSectionCount == 1
					   && MyProc->delayChkptFlags == DELAY_CHKPT_START && native_inserts == 0
					   && native_flushes == 0;
	native_inserts++;
	native_info = info;
	XactLastRecEnd = coverage_commit_end;
	native_pause(NATIVE_INSERTED);
	return XactLastRecEnd;
}

static void
native_flush(XLogRecPtr end)
{
	native_order_ok &= end == coverage_commit_end && native_inserts == 1 && CritSectionCount == 1
					   && MyProc->delayChkptFlags == DELAY_CHKPT_START && coverage_commits == 0
					   && native_clogs == 0;
	coverage_flush = end;
	native_flushes++;
	native_pause(NATIVE_FLUSHED);
}

static void
native_apply(XLogRecPtr end)
{
	native_order_ok &= native_flushes == 1 && native_clogs == 0;
	cluster_tt_slot_durable_commit_apply(end);
	native_pause(NATIVE_STAMPED);
}

static void
native_clog(TransactionId xid, int count, TransactionId *children)
{
	native_order_ok &= xid == 100 && count == 0 && children == NULL && coverage_commits == 1
					   && native_flushes == 1 && CritSectionCount == 1
					   && MyProc->delayChkptFlags == DELAY_CHKPT_START;
	native_clogs++;
}

static bool
native_clear(SCN scn)
{
	native_order_ok &= scn == scn_encode(1, 42) && native_clogs == 1 && CritSectionCount == 0
					   && MyProc->delayChkptFlags == 0;
	native_pause(NATIVE_RELEASED);
	return true;
}

/* Only the ordinary, headless TT-bound transaction is in this fixture.
 * Keep unrelated observers outside the extracted producer and retain the
 * producer's own branch, critical-section and delay-flag statements. */
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
#define cluster_tt_local_precommit_durable_finish native_precommit
#define cluster_sf_xact_commit_brake() abort()
#define cluster_backup_pending_commit_abort(...) ((void)0)
#define cluster_scn_durable_pending_abort_self() false
#define cluster_space_drop_prepare(...) NULL
#define cluster_space_drop_wal(...) NULL
#define cluster_space_drop_mark_dirty(...) abort()
#define cluster_backup_pending_commit_exit() ((void)0)
#define cluster_scn_durable_pending_fill_lsn(...) true
#define replorigin_session_advance(...) abort()
#define GetCurrentTransactionStopTimestamp() ((TimestampTz)1)
#define TransactionTreeSetCommitTsData(...) ((void)0)
#define XLogFlush native_flush
#define cluster_sf_publish_origin_durable_lsn() abort()
#define cluster_tt_local_commit_durable_apply native_apply
#define TransactionIdCommitTree native_clog
#define XLogSetAsyncXactLSN(...) abort()
#define TransactionIdAsyncCommitTree(...) abort()
#define cluster_space_drop_publish(...) abort()
#define cluster_space_drop_finish(...) abort()
#define cluster_scn_pending_commit_clear native_clear
#define cluster_scn_durable_pending_discharge_scn(...) true
#define TransactionIdLatest(xid, count, children) (xid)
#define cluster_tt_local_has_binding(xid) true
#define cluster_tt_local_record_commit(...) ((void)0)
#define SyncRepWaitForLSN(...) ((void)0)
#undef CLUSTER_INJECTION_POINT
#define CLUSTER_INJECTION_POINT(name) ((void)0)
#undef CLUSTER_INJECTION_PAUSE
#define CLUSTER_INJECTION_PAUSE(name) ((void)0)
#include "test_cluster_space_commit_emit.inc"
#include "test_cluster_tt_checkpoint_native.inc"

static void *
native_producer(void *unused)
{
	(void)unused;
	native_result = RecordTransactionCommit();
	pthread_mutex_lock(&native_mutex);
	native_done = true;
	pthread_cond_signal(&native_cond);
	pthread_mutex_unlock(&native_mutex);
	return NULL;
}

static void
native_finish(void)
{
	pthread_mutex_lock(&native_mutex);
	native_resume = true;
	pthread_cond_signal(&native_cond);
	pthread_mutex_unlock(&native_mutex);
	if (pthread_join(native_thread, NULL) != 0)
		abort();
}

static bool
native_start(NativeCommitCut cut)
{
	bool paused;

	coverage_setup();
	/* The original entry, rather than the fixture, must open this window. */
	MyProc->delayChkptFlags = 0;
	coverage_flush = InvalidXLogRecPtr;
	native_cut = cut;
	native_paused = native_resume = native_done = false;
	native_order_ok = true;
	native_inserts = native_flushes = native_clogs = native_wal_len = 0;
	native_result = InvalidTransactionId;
	XactLastRecEnd = 1;
	XactLastCommitEnd = 0;
	coverage_wait_resume = native_finish;
	if (pthread_create(&native_thread, NULL, native_producer, NULL) != 0)
		abort();
	pthread_mutex_lock(&native_mutex);
	while (!native_paused && !native_done)
		pthread_cond_wait(&native_cond, &native_mutex);
	paused = native_paused;
	pthread_mutex_unlock(&native_mutex);
	if (!paused && pthread_join(native_thread, NULL) != 0)
		abort();
	return paused;
}

static bool
native_record_matches(void)
{
	xl_xact_parsed_commit parsed;
	const xl_xact_tt_commit *fold;

	memset(&parsed, 0, sizeof(parsed));
	if (!ParseCommitRecord(native_info, (xl_xact_commit *)native_wal, native_wal_len, &parsed))
		return false;
	fold = &parsed.tt_commit;
	return native_wal_len
			   == sizeof(xl_xact_commit) + sizeof(xl_xact_xinfo) + sizeof(xl_xact_scn)
					  + sizeof(xl_xact_tt_commit)
		   && (native_info & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT
		   && parsed.xinfo == (XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_TT_COMMIT)
		   && parsed.scn == scn_encode(1, 42) && fold->segment_id == 1
		   && fold->segment_generation == 0 && fold->instance == 1 && fold->xid == 100
		   && fold->slot_offset == 7 && fold->wrap == 3
		   && fold->format_version == CLUSTER_XACT_TT_COMMIT_VERSION && fold->flags == 0
		   && fold->reserved == 0 && fold->commit_scn == parsed.scn;
}

static bool
native_checkpoint_case(NativeCommitCut cut)
{
	bool protected, ok;

	if (!native_start(cut))
		return false;
	protected = cut != NATIVE_RESERVED && cut != NATIVE_RELEASED;
	ok = MyProc->delayChkptFlags == (protected ? DELAY_CHKPT_START : 0);
	coverage_checkpoint(cut == NATIVE_RESERVED ? 50 : 200);
	if (coverage_waits == 0)
		native_finish();
	ok &= coverage_waits == (protected ? 1 : 0) && coverage_guts == 1
		  && coverage_absorbs == coverage_waits && !coverage_premature && native_order_ok
		  && native_result == 100 && native_inserts == 1 && native_flushes == 1 && native_clogs == 1
		  && coverage_commits == 1 && XactLastCommitEnd == coverage_commit_end
		  && XactLastRecEnd == 0 && MyProc->delayChkptFlags == 0 && CritSectionCount == 0
		  && native_record_matches();
	if (cut == NATIVE_RESERVED) {
		/* This checkpoint predates the COMMIT: only retained redo can
		 * recover the later volatile stamp after the crash. */
		ok &= model_files[1].fsyncs == 0;
		memcpy(coverage_ordinary_commit.data, model_files[1].cache, BLCKSZ);
		model_crash();
		process_crash_restart();
		ok &= !coverage_durable_committed();
		coverage_redo_commit();
		ok &= coverage_redo_applies == 1 && model_files[1].fsyncs == 1
			  && memcmp(model_files[1].durable, coverage_ordinary_commit.data, BLCKSZ) == 0;
	} else {
		ok &= model_files[1].fsyncs == 1;
		model_crash();
		process_crash_restart();
	}
	return ok && coverage_durable_committed();
}

UT_TEST(test_native_reservation_after_redo_requires_terminal_replay)
{
	UT_ASSERT(native_checkpoint_case(NATIVE_RESERVED));
}

UT_TEST(test_native_insert_before_redo_waits_for_stamp_and_sync)
{
	UT_ASSERT(native_checkpoint_case(NATIVE_INSERTED));
}

UT_TEST(test_native_flush_before_redo_waits_for_stamp_and_sync)
{
	UT_ASSERT(native_checkpoint_case(NATIVE_FLUSHED));
}

UT_TEST(test_native_stamp_before_redo_waits_for_window_exit)
{
	UT_ASSERT(native_checkpoint_case(NATIVE_STAMPED));
}

UT_TEST(test_native_released_window_is_already_covered_by_sync)
{
	UT_ASSERT(native_checkpoint_case(NATIVE_RELEASED));
}

#ifndef TT_CHECKPOINT_NATIVE_MAIN
#define TT_CHECKPOINT_NATIVE_MAIN main
#endif
int
TT_CHECKPOINT_NATIVE_MAIN(int argc, char **argv)
{
	/* A single early cut also makes missing-stamp mutation tests terminate. */
	if (argc == 2 && strcmp(argv[1], "--inserted-only") == 0) {
		UT_PLAN(1);
		UT_RUN(test_native_insert_before_redo_waits_for_stamp_and_sync);
	} else {
		UT_PLAN(5);
		UT_RUN(test_native_reservation_after_redo_requires_terminal_replay);
		UT_RUN(test_native_insert_before_redo_waits_for_stamp_and_sync);
		UT_RUN(test_native_flush_before_redo_waits_for_stamp_and_sync);
		UT_RUN(test_native_stamp_before_redo_waits_for_window_exit);
		UT_RUN(test_native_released_window_is_already_covered_by_sync);
	}
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
