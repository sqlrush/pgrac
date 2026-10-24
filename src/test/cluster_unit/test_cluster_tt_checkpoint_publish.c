/* Author: SqlRush <sqlrush@gmail.com>
 * Execute native checkpoint redo selection, the complete CheckPointGuts and
 * CreateCheckPoint's wait-through-publication call sites with the original
 * RecordTransactionCommit/emitter at controlled interleavings. Reuse the real TT
 * terminal apply, undo header writer and fsync crash model. Other checkpoint
 * subsystems, checkpoint WAL storage and the ROOT publisher are boundaries;
 * this is neither a live crash test nor qualified recovery-base authority.
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 */
int checkpoint_native_fixture_main(int argc, char **argv);
#define TT_CHECKPOINT_NATIVE_MAIN checkpoint_native_fixture_main
#define CheckPointGuts coverage_prior_guts
#include "test_cluster_tt_checkpoint_native.c"
#include "cluster/cluster_relmap_checkpoint.h"
#undef CheckPointGuts

#include <time.h>
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "catalog/pg_control.h"
#include "port/pg_crc32c.h"

typedef enum PublishFault { PUBLISH_NO_FAULT, PUBLISH_WAL_FLUSH, PUBLISH_ROOT } PublishFault;

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
static int publish_producer_crit;
static CheckPoint publish_registered;
static XLogRecPtr publish_wal_end;
static uint8 publish_info;
static XLogRecPtr publish_start, publish_end;

/* This existing TT closure fixture models an unavailable optional D3 sample;
 * execute the actual final call site outside critical/native target locks. */
bool
cluster_relmap_recovery_checkpoint_window_finish(const ClusterRelmapCheckpointSampleV1 *s,
												 const CheckPoint *cp, XLogRecPtr start,
												 XLogRecPtr end, uint8 out[160])
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(!publish_lock_held);
	memset(out, 0, 160);
	return false;
}

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
ClusterCheckpointV3Publish(const ControlFileData *candidate, XLogRecPtr end,
						   const uint8 window[160])
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
#undef XLogFlush
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
	ClusterRelmapCheckpointSampleV1 native_window = { 0 };
	uint8 native_window_bytes[160] = { 0 };
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
publish_setup_control(void)
{
	MyBackendType = B_CHECKPOINTER;
	MyAuxProcType = CheckpointerProcess;
	memset(&publish_control, 0, sizeof(publish_control));
	publish_control.checkPoint = 40;
	publish_control.checkPointCopy.redo = 20;
	publish_control.state = DB_IN_PRODUCTION;
	publish_anchor = publish_control;
	memset(&publish_xlog, 0, sizeof(publish_xlog));
	ProcLastRecPtr = 0;
	LocalXLogInsertAllowed = 1;
	CritSectionCount = 0;
	publish_start = 512;
	publish_end = 768;
	publish_reset_calls();
}

static void
publish_add_active(void)
{
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)coverage_resident.data;
	size_t offset = offsetof(UndoSegmentHeaderData, tt_slots) + 8 * sizeof(TTSlot);

	/* Slot 7's durable predecessor is still ACTIVE, including at STAMPED.
	 * Install only slot 8: copying the whole page would silently persist the
	 * producer's dirty terminal stamp before the checkpoint under test. */
	memcpy(&publish_active, model_files[1].durable + STAMP_OFFSET, sizeof(TTSlot));
	publish_active.xid = 101;
	publish_active.wrap = 4;
	resident->tt_slots[8] = publish_active;
	memcpy(model_files[1].cache + offset, &publish_active, sizeof(TTSlot));
	memcpy(model_files[1].durable + offset, &publish_active, sizeof(TTSlot));
	/* Seal each distinct image; the dirty terminal slot stays out of durable. */
	if (!cluster_undo_segment_header_seal(resident)
		|| !cluster_undo_segment_header_seal(model_files[1].cache)
		|| !cluster_undo_segment_header_seal(model_files[1].durable))
		abort();
	publish_committed = publish_active;
	publish_committed.xid = 100;
	publish_committed.wrap = 3;
	publish_committed.status = TT_SLOT_COMMITTED;
	publish_committed.commit_scn = scn_encode(1, 42);
	publish_committed.first_undo_block = coverage_invalid_head;
}

static void
publish_setup(void)
{
	coverage_setup();
	publish_add_active();
	coverage_finish_commit();
	publish_setup_control();
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

static void
publish_native_finish(void)
{
	/* The producer is paused until the checkpoint's original wait yields.
	 * Model the two backends' critical-section counters at this serialized
	 * scheduler boundary; never let the checkpointer inherit the producer's. */
	publish_order_ok &= CritSectionCount == 0;
	CritSectionCount = publish_producer_crit;
	native_finish();
	publish_order_ok &= CritSectionCount == 0;
}

static bool
publish_native_start(NativeCommitCut cut)
{
	if (!native_start(cut))
		return false;
	publish_producer_crit = CritSectionCount;
	publish_add_active();
	publish_setup_control();
	coverage_wait_resume = publish_native_finish;
	return true;
}

static bool
publish_native_case(NativeCommitCut cut)
{
	TTSlot committed, active;
	xl_xact_parsed_commit parsed;
	const xl_xact_tt_commit *fold;
	bool protected, raised, ok;

	if (!publish_native_start(cut))
		return false;
	committed = publish_committed;
	memcpy(&active, model_files[1].durable + STAMP_OFFSET, sizeof(active));
	protected = cut != NATIVE_RESERVED && cut != NATIVE_RELEASED;
	if (cut == NATIVE_RESERVED)
		publish_committed = active; /* This checkpoint must precede COMMIT. */
	raised = publish_raises(cut == NATIVE_RESERVED ? 50 : 200, false);
	if (coverage_waits == 0)
		publish_native_finish();
	publish_committed = committed;
	ok = !raised && publish_order_ok && native_order_ok && native_record_matches()
		 && coverage_waits == (protected ? 1 : 0) && coverage_absorbs == coverage_waits
		 && publish_inserts == 1 && publish_flushes == 1 && publish_calls == 1 && publish_syncs == 1
		 && publish_twophase == 1 && publish_pi_releases == 1
		 && publish_anchor.checkPoint == publish_start && publish_wal_end == publish_end
		 && publish_anchor.checkPointCopy.redo == (cut == NATIVE_RESERVED ? 50 : 200)
		 && native_result == 100 && native_inserts == 1 && native_flushes == 1 && native_clogs == 1
		 && coverage_commits == 1 && MyProc->delayChkptFlags == 0 && CritSectionCount == 0
		 && publish_durable_slot(8, &publish_active);
	model_crash();
	process_crash_restart();
	if (cut == NATIVE_RESERVED) {
		ok &= model_files[1].fsyncs == 0 && publish_durable_slot(7, &active);
		/* Consume the actual producer's native terminal payload, not a
		 * caller-supplied terminal shape or an invented checkpoint proof. */
		memset(&parsed, 0, sizeof(parsed));
		if (!ParseCommitRecord(native_info, (xl_xact_commit *)native_wal, native_wal_len, &parsed))
			return false;
		fold = &parsed.tt_commit;
		cluster_tt_durable_redo_stamp_slot_exact(fold->instance, fold->segment_id,
												 fold->segment_generation, fold->slot_offset,
												 fold->wrap, fold->xid, fold->commit_scn);
		ok &= coverage_redo_applies == 1;
	} else
		ok &= coverage_redo_applies == 0;
	return ok && model_files[1].fsyncs == 1 && publish_tt_durable() && coverage_durable_committed();
}

UT_TEST(native_reserved_commit_replays_after_earlier_published_checkpoint)
{
	UT_ASSERT(publish_native_case(NATIVE_RESERVED));
}

UT_TEST(native_inserted_commit_is_durable_before_checkpoint_publication)
{
	UT_ASSERT(publish_native_case(NATIVE_INSERTED));
}

UT_TEST(native_flushed_commit_is_durable_before_checkpoint_publication)
{
	UT_ASSERT(publish_native_case(NATIVE_FLUSHED));
}

UT_TEST(native_stamped_commit_leaves_window_before_checkpoint_publication)
{
	UT_ASSERT(publish_native_case(NATIVE_STAMPED));
}

UT_TEST(native_released_commit_is_durable_before_checkpoint_publication)
{
	UT_ASSERT(publish_native_case(NATIVE_RELEASED));
}

UT_TEST(native_commit_checkpoint_sync_refusal_keeps_old_selected_anchor)
{
	ControlFileData prior;

	UT_ASSERT(publish_native_start(NATIVE_INSERTED));
	prior = publish_anchor;
	model_fsync_fails = true;
	UT_ASSERT(publish_raises(200, false));
	UT_ASSERT(native_order_ok && native_record_matches());
	UT_ASSERT_EQ(coverage_waits, 1);
	UT_ASSERT_EQ(coverage_commits, 1);
	UT_ASSERT_EQ(publish_inserts, 0);
	UT_ASSERT_EQ(publish_calls, 0);
	UT_ASSERT(memcmp(&publish_anchor, &prior, sizeof(prior)) == 0);
	UT_ASSERT(memcmp(ControlFile, &prior, sizeof(prior)) == 0);
	UT_ASSERT(!publish_tt_durable());
	/* A live retry retains the original pending header sync request. */
	publish_reset_calls();
	UT_ASSERT(!publish_raises(208, false));
	UT_ASSERT(publish_order_ok && publish_tt_durable());
	UT_ASSERT_EQ(publish_anchor.checkPointCopy.redo, 208);
}

UT_TEST(old_active_survives_successive_published_checkpoints_without_bind_input)
{
	unsigned i;

	publish_setup();
	/* No BIND WAL is supplied to this header/checkpoint fixture. This checks
	 * physical preservation, not WAL pruning or qualified recovery input. */
	for (i = 0; i < 3; i++) {
		XLogRecPtr prior_redo = publish_anchor.checkPointCopy.redo;
		XLogRecPtr redo = 200 + i * 1024;

		publish_reset_calls();
		publish_start = 512 + i * 1024;
		publish_end = 768 + i * 1024;
		UT_ASSERT(!publish_raises(redo, false));
		UT_ASSERT(publish_order_ok && publish_tt_durable());
		UT_ASSERT_EQ(PriorRedoPtr, prior_redo);
		UT_ASSERT_EQ(publish_anchor.checkPointCopy.redo, redo);
		UT_ASSERT_EQ(publish_calls, 1);
		model_crash();
		process_crash_restart();
		UT_ASSERT(publish_durable_slot(8, &publish_active));
		UT_ASSERT_EQ(publish_active.status, TT_SLOT_ACTIVE);
	}
}

int
main(int argc, char **argv)
{
	/* Isolate the missing-wait mutation without stranding a later producer. */
	if (argc == 2 && strcmp(argv[1], "--inserted-only") == 0) {
		UT_PLAN(1);
		UT_RUN(native_inserted_commit_is_durable_before_checkpoint_publication);
		UT_DONE();
		return ut_failed_count == 0 ? 0 : 1;
	}
	UT_PLAN(12);
	UT_RUN(native_checkpoint_syncs_tt_before_publishing_selected_redo);
	UT_RUN(native_checkpoint_redo_selection_accounts_for_wal_page_headers);
	UT_RUN(native_checkpoint_tt_sync_error_keeps_old_anchor_and_pending_header);
	UT_RUN(native_checkpoint_wal_flush_error_does_not_publish_candidate);
	UT_RUN(native_checkpoint_root_error_leaves_the_native_candidate_private);
	UT_RUN(native_reserved_commit_replays_after_earlier_published_checkpoint);
	UT_RUN(native_inserted_commit_is_durable_before_checkpoint_publication);
	UT_RUN(native_flushed_commit_is_durable_before_checkpoint_publication);
	UT_RUN(native_stamped_commit_leaves_window_before_checkpoint_publication);
	UT_RUN(native_released_commit_is_durable_before_checkpoint_publication);
	UT_RUN(native_commit_checkpoint_sync_refusal_keeps_old_selected_anchor);
	UT_RUN(old_active_survives_successive_published_checkpoints_without_bind_input);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
