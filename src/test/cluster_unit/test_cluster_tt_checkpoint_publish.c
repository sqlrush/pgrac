/* Author: SqlRush <sqlrush@gmail.com>
 * Execute native checkpoint redo selection, the complete CheckPointGuts and
 * CreateCheckPoint's wait-through-publication call sites. Reuse the real TT
 * terminal apply, undo header writer and fsync crash model. Other checkpoint
 * subsystems, checkpoint WAL storage and the ROOT publisher are boundaries;
 * this is neither a live crash test nor qualified recovery-base authority.
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 */
int checkpoint_publish_fixture_main(void);
#define TT_CHECKPOINT_TEST_MAIN checkpoint_publish_fixture_main
#define CheckPointGuts coverage_prior_guts
#include "test_cluster_tt_checkpoint_cuts.c"
#undef CheckPointGuts

#include <time.h>
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "catalog/pg_control.h"
#include "port/pg_crc32c.h"

typedef enum PublishFault { PUBLISH_NO_FAULT, PUBLISH_WAL_FLUSH, PUBLISH_ROOT } PublishFault;

bool cluster_shared_config = true;
bool cluster_controlfile_shared_authority = true;
int wal_segment_size = 1024 * 1024;
XLogRecPtr ProcLastRecPtr;
static XLogRecPtr PriorRedoPtr;
static int LocalXLogInsertAllowed;
static ControlFileData publish_control, publish_anchor;
static ControlFileData *ControlFile = &publish_control;
static struct {
	slock_t ulsn_lck, info_lck;
	XLogRecPtr unloggedLSN;
	FullTransactionId ckptFullXid;
} publish_xlog;
static typeof(publish_xlog) *XLogCtl = &publish_xlog;
static struct {
	TimestampTz ckpt_write_t, ckpt_sync_t, ckpt_sync_end_t;
} publish_stats;
static PublishFault publish_fault;
static unsigned publish_inserts, publish_flushes, publish_calls, publish_syncs;
static unsigned publish_pi_releases, publish_twophase;
static bool publish_lock_held;
static bool publish_order_ok;
static CheckPoint publish_registered;
static XLogRecPtr publish_wal_end;
static uint8 publish_info;
static const XLogRecPtr publish_start = 512;
static const XLogRecPtr publish_end = 768;

static bool
publish_durable_slot(uint32 index, const TTSlot *expected)
{
	TTSlot slot;

	memcpy(&slot,
		   model_files[1].durable + offsetof(UndoSegmentHeaderData, tt_slots)
			   + index * sizeof(TTSlot),
		   sizeof(slot));
	return memcmp(&slot, expected, sizeof(slot)) == 0;
}

static TTSlot publish_committed, publish_active;

static bool
publish_tt_durable(void)
{
	return publish_durable_slot(7, &publish_committed) && publish_durable_slot(8, &publish_active);
}

static void
publish_fail(void)
{
	ereport(ERROR, (errmsg("injected checkpoint publication failure")));
}

/* The remaining checkpoint participants are explicit fixture boundaries. */
#define CheckPointRelationMap() ((void)0)
#define CheckPointReplicationSlots() ((void)0)
#define CheckPointSnapBuild() ((void)0)
#define CheckPointLogicalRewriteHeap() ((void)0)
#define CheckPointReplicationOrigin() ((void)0)
#define TRACE_POSTGRESQL_BUFFER_CHECKPOINT_START(flags) ((void)(flags))
#define TRACE_POSTGRESQL_BUFFER_CHECKPOINT_SYNC_START() ((void)0)
#define TRACE_POSTGRESQL_BUFFER_CHECKPOINT_DONE() ((void)0)
#define GetCurrentTimestamp() ((TimestampTz)1)
#define CheckpointStats publish_stats
#define CheckPointCLOG() ((void)0)
#define CheckPointCommitTs() ((void)0)
#define CheckPointSUBTRANS() ((void)0)
#define CheckPointMultiXact() ((void)0)
#define CheckPointPredicate() ((void)0)
#define CheckPointBuffers(flags) ((void)(flags))
#define cluster_pi_writeback_checkpointer_release_v1() (++publish_pi_releases)
#define cluster_gcs_block_pi_note_presync_snapshot() UINT64CONST(1)
#define cluster_gcs_block_pi_note_confirm(seq) ((void)(seq))
#define ClusterStartupFileSync() false
#define ProcessSyncRequests() (++publish_syncs)
#define CheckPointTwoPhase(redo) ((void)(redo), ++publish_twophase)
#include "test_cluster_tt_checkpoint_guts.inc"

static void
publish_begin_insert(void)
{
	publish_order_ok &= publish_tt_durable() && publish_syncs == 1 && publish_twophase == 1
						&& CritSectionCount == 1;
}

static void
publish_register(char *data, uint32 len)
{
	if (len != sizeof(publish_registered))
		abort();
	memcpy(&publish_registered, data, len);
}

static XLogRecPtr
publish_insert(RmgrId rmid, uint8 info)
{
	publish_order_ok &= rmid == RM_XLOG_ID && publish_tt_durable();
	publish_inserts++;
	publish_info = info;
	ProcLastRecPtr = publish_start;
	return publish_end;
}

static void
publish_flush(XLogRecPtr end)
{
	publish_flushes++;
	if (publish_fault == PUBLISH_WAL_FLUSH)
		publish_fail();
	publish_order_ok &= end == publish_end && publish_inserts == 1 && publish_tt_durable();
	publish_wal_end = end;
}

static void
ClusterCheckpointV3Publish(const ControlFileData *candidate, XLogRecPtr end)
{
	publish_calls++;
	publish_order_ok
		&= !publish_lock_held && CritSectionCount == 0 && publish_tt_durable()
		   && publish_wal_end == end && end == publish_end && candidate->checkPoint == publish_start
		   && memcmp(&candidate->checkPointCopy, &publish_registered, sizeof(publish_registered))
				  == 0;
	if (publish_fault == PUBLISH_ROOT)
		publish_fail();
	publish_anchor = *candidate;
	/* ROOT storage and its selected-native installation are boundaries. */
	*ControlFile = *candidate;
}

#undef XLogStandbyInfoActive
#define XLogStandbyInfoActive() false
#define LogStandbySnapshot() ((void)0)
#define cluster_hw_metadata_configured() false
#define cluster_hw_authority_active() false
#define cluster_hw_snapshot_checkpoint_write(redo) ((void)(redo))
#define cluster_xid_stripe_checkpoint_reemit() ((void)0)
#define XLogBeginInsert publish_begin_insert
#define XLogRegisterData publish_register
#define XLogInsert publish_insert
#define XLogFlush publish_flush
#undef START_CRIT_SECTION
#define START_CRIT_SECTION() (++CritSectionCount)
#undef END_CRIT_SECTION
#define END_CRIT_SECTION() (--CritSectionCount)
#undef ControlFileLock
#define ControlFileLock ((LWLock *)NULL)
#define LWLockAcquire(lock, mode) ((void)(lock), (void)(mode), publish_lock_held = true)
#define LWLockRelease(lock) ((void)(lock), publish_lock_held = false)
#undef SpinLockAcquire
#define SpinLockAcquire(lock) ((void)(lock))
#undef SpinLockRelease
#define SpinLockRelease(lock) ((void)(lock))
#define UpdateControlFile() abort()
#define cluster_recovery_anchor_publish_checkpoint(...) abort()

static XLogRecPtr
publish_select_redo(XLogRecPtr curInsert)
{
	uint32 freespace;
	CheckPoint checkPoint = { 0 };

#include "test_cluster_tt_checkpoint_select.inc"
	return checkPoint.redo;
}

static void
publish_checkpoint(XLogRecPtr insert, bool shutdown)
{
	VirtualTransactionId *vxids;
	int nvxids;
	int flags = shutdown ? CHECKPOINT_IS_SHUTDOWN : 0;
	int oldXLogAllowed = 1;
	XLogRecPtr recptr;
	ControlFileData *checkpoint_control = ControlFile, v3_checkpoint = { 0 };
	CheckPoint checkPoint = { 0 };

	checkPoint.redo = publish_select_redo(insert);
	checkPoint.ThisTimeLineID = checkPoint.PrevTimeLineID = 1;
	checkPoint.nextXid = FullTransactionIdFromU64(150);
#define pg_usleep coverage_sleep
#include "test_cluster_tt_checkpoint_publish.inc"
#undef pg_usleep
}

static bool
publish_raises(XLogRecPtr insert, bool shutdown)
{
	sigjmp_buf local_sigjmp_buf;
	sigjmp_buf *saved_stack = PG_exception_stack;
	volatile bool raised = false;

	error_reports = 0;
	if (sigsetjmp(local_sigjmp_buf, 0) == 0) {
		PG_exception_stack = &local_sigjmp_buf;
		publish_checkpoint(insert, shutdown);
	} else
		raised = true;
	PG_exception_stack = saved_stack;
	/* Reset the fixture after intercepting an error. Native WAL flush failure
	 * inside the critical section terminates the server; no in-process retry
	 * after that failure is asserted here. */
	CritSectionCount = 0;
	return raised;
}

static void
publish_reset_calls(void)
{
	publish_inserts = publish_flushes = publish_calls = publish_syncs = 0;
	publish_pi_releases = publish_twophase = 0;
	publish_wal_end = InvalidXLogRecPtr;
	publish_order_ok = true;
	publish_lock_held = false;
	publish_fault = PUBLISH_NO_FAULT;
	model_fsync_fails = false;
	memset(&publish_registered, 0, sizeof(publish_registered));
}

static void
publish_setup(void)
{
	UndoSegmentHeaderData *resident;

	coverage_setup();
	MyBackendType = B_CHECKPOINTER;
	MyAuxProcType = CheckpointerProcess;
	resident = (UndoSegmentHeaderData *)coverage_resident.data;
	/* A long-lived top remains ACTIVE across the same checkpoint. */
	publish_active = resident->tt_slots[7];
	publish_active.xid = 101;
	publish_active.wrap = 4;
	resident->tt_slots[8] = publish_active;
	memcpy(model_files[1].cache, resident, BLCKSZ);
	memcpy(model_files[1].durable, resident, BLCKSZ);
	publish_committed = tt_commit_stage.successor;
	coverage_finish_commit();
	memset(&publish_control, 0, sizeof(publish_control));
	publish_control.checkPoint = 40;
	publish_control.checkPointCopy.redo = 20;
	publish_control.state = DB_IN_PRODUCTION;
	publish_anchor = publish_control;
	memset(&publish_xlog, 0, sizeof(publish_xlog));
	ProcLastRecPtr = 0;
	LocalXLogInsertAllowed = 1;
	CritSectionCount = 0;
	publish_reset_calls();
}

UT_TEST(native_checkpoint_syncs_tt_before_publishing_selected_redo)
{
	publish_setup();
	UT_ASSERT(!publish_raises(200, false));
	UT_ASSERT(publish_order_ok);
	UT_ASSERT_EQ(publish_inserts, 1);
	UT_ASSERT_EQ(publish_flushes, 1);
	UT_ASSERT_EQ(publish_calls, 1);
	UT_ASSERT_EQ(publish_pi_releases, 1);
	UT_ASSERT_EQ(publish_info, XLOG_CHECKPOINT_ONLINE);
	UT_ASSERT_EQ(publish_anchor.checkPoint, publish_start);
	UT_ASSERT_EQ(publish_anchor.checkPointCopy.redo, 200);
	UT_ASSERT_EQ(model_files[1].fsyncs, 1);
	UT_ASSERT_EQ(PriorRedoPtr, 20);
	UT_ASSERT_EQ(U64FromFullTransactionId(XLogCtl->ckptFullXid), 150);
	UT_ASSERT(publish_tt_durable());
	UT_ASSERT(coverage_durable_committed());
	UT_ASSERT(publish_durable_slot(8, &publish_active));
}

UT_TEST(native_checkpoint_redo_selection_accounts_for_wal_page_headers)
{
	UT_ASSERT_EQ(publish_select_redo(200), 200);
	UT_ASSERT_EQ(publish_select_redo(XLOG_BLCKSZ), XLOG_BLCKSZ + SizeOfXLogShortPHD);
	UT_ASSERT_EQ(publish_select_redo(wal_segment_size), wal_segment_size + SizeOfXLogLongPHD);
}

UT_TEST(native_checkpoint_tt_sync_error_keeps_old_anchor_and_pending_header)
{
	ControlFileData prior;

	publish_setup();
	prior = publish_anchor;
	model_fsync_fails = true;
	UT_ASSERT(publish_raises(200, false));
	UT_ASSERT_EQ(error_reports, 1);
	UT_ASSERT_EQ(publish_inserts, 0);
	UT_ASSERT_EQ(publish_calls, 0);
	UT_ASSERT_EQ(publish_syncs, 0);
	UT_ASSERT(memcmp(&publish_anchor, &prior, sizeof(prior)) == 0);
	UT_ASSERT(memcmp(ControlFile, &prior, sizeof(prior)) == 0);
	UT_ASSERT(!publish_tt_durable());
	/* Retry without crashing: the failed native sweep must retain its work. */
	publish_reset_calls();
	UT_ASSERT(!publish_raises(208, false));
	UT_ASSERT(publish_order_ok);
	UT_ASSERT(publish_tt_durable());
	UT_ASSERT_EQ(publish_anchor.checkPointCopy.redo, 208);
	UT_ASSERT(coverage_durable_committed());
}

UT_TEST(native_checkpoint_wal_flush_error_does_not_publish_candidate)
{
	ControlFileData prior;

	publish_setup();
	prior = publish_anchor;
	publish_fault = PUBLISH_WAL_FLUSH;
	UT_ASSERT(publish_raises(200, false));
	UT_ASSERT_EQ(error_reports, 1);
	UT_ASSERT_EQ(publish_inserts, 1);
	UT_ASSERT_EQ(publish_calls, 0);
	UT_ASSERT_EQ(publish_wal_end, InvalidXLogRecPtr);
	UT_ASSERT(memcmp(&publish_anchor, &prior, sizeof(prior)) == 0);
	UT_ASSERT(memcmp(ControlFile, &prior, sizeof(prior)) == 0);
	UT_ASSERT(publish_tt_durable());
	UT_ASSERT(coverage_durable_committed());
}

UT_TEST(native_checkpoint_root_error_leaves_the_native_candidate_private)
{
	ControlFileData prior;

	publish_setup();
	prior = publish_anchor;
	publish_fault = PUBLISH_ROOT;
	UT_ASSERT(publish_raises(200, false));
	UT_ASSERT_EQ(error_reports, 1);
	UT_ASSERT_EQ(publish_calls, 1);
	UT_ASSERT_EQ(publish_wal_end, publish_end);
	UT_ASSERT(publish_order_ok);
	UT_ASSERT(memcmp(&publish_anchor, &prior, sizeof(prior)) == 0);
	UT_ASSERT(memcmp(ControlFile, &prior, sizeof(prior)) == 0);
	UT_ASSERT(coverage_durable_committed());
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(native_checkpoint_syncs_tt_before_publishing_selected_redo);
	UT_RUN(native_checkpoint_redo_selection_accounts_for_wal_page_headers);
	UT_RUN(native_checkpoint_tt_sync_error_keeps_old_anchor_and_pending_header);
	UT_RUN(native_checkpoint_wal_flush_error_does_not_publish_candidate);
	UT_RUN(native_checkpoint_root_error_leaves_the_native_candidate_private);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
