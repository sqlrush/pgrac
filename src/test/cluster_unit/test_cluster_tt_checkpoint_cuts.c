/* Author: SqlRush <sqlrush@gmail.com>
 * Execute the original ordinary TT commit apply, native checkpoint wait and
 * exact TT redo and real header write/fsync together. Reuse the explicit
 * volatile/durable file model. Process lookup, scheduler, WAL flush/decoding,
 * segment presence and the prepared resident pin are controlled boundaries;
 * this grants no recovery authority, qualifies no canonical top binding and
 * does not replace a postmaster crash test or complete WAL replay.
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 */
int header_fixture_main(void);
#define main header_fixture_main
#include "test_cluster_undo_header_durability.c"
#undef main

#include "catalog/pg_control.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_tt_durable.h"
#include "storage/proc.h"
#include "storage/procarray.h"

static PGPROC coverage_proc;
PGPROC *MyProc = &coverage_proc;
static PGAlignedBlock coverage_resident;
static PGAlignedBlock coverage_ordinary_commit;
static UBA coverage_head;
static const UBA coverage_invalid_head = InvalidUba_init;
static XLogRecPtr coverage_flush, coverage_commit_end;
static int coverage_waits, coverage_absorbs, coverage_guts, coverage_commits;
static int coverage_redo_applies;
static int coverage_finish_after;
static bool coverage_premature;

static XLogRecPtr
coverage_flush_ptr(TimeLineID *timeline)
{
	if (timeline != NULL)
		*timeline = 1;
	return coverage_flush;
}

static ClusterUndoBlock0Result
coverage_lock(ClusterUndoBlock0Pin *pin, const ClusterUndoBlock0Generation *generation,
			  ClusterUndoBlock0Mode mode, char **page)
{
	if (pin->slot != 0 || !generation->known || generation->value != 0
		|| mode != CLUSTER_UNDO_BLOCK0_EXCLUSIVE)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	*page = coverage_resident.data;
	return CLUSTER_UNDO_BLOCK0_OK;
}

static void
coverage_unpin(ClusterUndoBlock0Pin *pin)
{
	pin->slot = -1;
}

static void
coverage_count_commit(void)
{
	coverage_commits++;
}

#define GetFlushRecPtr coverage_flush_ptr
#define cluster_undo_block0_lock_content coverage_lock
#define cluster_undo_block0_unpin coverage_unpin
#define cluster_tt_durable_count_commit coverage_count_commit
#include "test_cluster_tt_checkpoint_apply.inc"
#undef cluster_tt_durable_count_commit
#undef cluster_undo_block0_unpin
#undef cluster_undo_block0_lock_content
#undef GetFlushRecPtr

/* The model has one already-existing segment; allocation is outside this cut. */
static int
cluster_undo_redo_open_segment(uint8 instance, uint32 segment_id, const char *path,
							   bool create_if_missing)
{
	if (instance != 1 || segment_id != 1 || create_if_missing)
		abort();
	return BasicOpenFile(path, O_RDWR | PG_BINARY);
}

void
cluster_tt_durable_count_redo_apply(void)
{
	coverage_redo_applies++;
}

void
cluster_vis_bump_recovery_undo_redo_applies(void)
{}

void
cluster_vis_bump_recovery_undo_redo_skips(void)
{}

#define pg_pread model_pread
#define pg_pwrite model_pwrite
#define close model_close
#include "test_cluster_tt_checkpoint_redo.inc"
#undef close
#undef pg_pwrite
#undef pg_pread

static void
coverage_finish_commit(void)
{
	CritSectionCount = 1;
	cluster_tt_slot_durable_commit_apply(coverage_commit_end);
	CritSectionCount = 0;
	MyProc->delayChkptFlags &= ~DELAY_CHKPT_START;
}

/* Capture the already-existing ordinary backend's native delay window. */
VirtualTransactionId *
GetVirtualXIDsDelayingChkpt(int *count, int flags)
{
	VirtualTransactionId *vxids = malloc(sizeof(*vxids));

	*count = LocalTransactionIdIsValid(MyProc->lxid) && (MyProc->delayChkptFlags & flags) != 0;
	return vxids;
}

bool
HaveVirtualXIDsDelayingChkpt(VirtualTransactionId *vxids, int count, int flags)
{
	(void)vxids;
	return count > 0 && (MyProc->delayChkptFlags & flags) != 0;
}

static void
coverage_sleep(long delay)
{
	(void)delay;
	coverage_waits++;
	if (coverage_waits == coverage_finish_after) {
		if (tt_commit_stage.armed)
			coverage_finish_commit();
		else
			MyProc->delayChkptFlags &= ~DELAY_CHKPT_START;
	}
}

static void
AbsorbSyncRequests(void)
{
	coverage_absorbs++;
}

static void
ClusterCheckpointWaitRecoveryStamps(int flags)
{
	(void)flags; /* No recovery executor exists in this ordinary backend test. */
}

static void
cluster_wal_retained_cut_before_sync_v1(XLogRecPtr redo)
{
	(void)redo;
}

static void
CheckPointGuts(XLogRecPtr redo, int flags)
{
	(void)flags;
	coverage_guts++;
	if (coverage_commit_end < redo && (tt_commit_stage.armed || MyProc->delayChkptFlags != 0))
		coverage_premature = true;
	cluster_undo_buf_flush_all(true);
}

static void
coverage_checkpoint(XLogRecPtr redo)
{
	VirtualTransactionId *vxids;
	int nvxids;
	int flags = 0;
	bool shutdown = false;
	CheckPoint checkPoint = { 0 };

	checkPoint.redo = redo;
#define pg_usleep coverage_sleep
#include "test_cluster_tt_checkpoint_wait.inc"
#undef pg_usleep
}

static void
coverage_setup(void)
{
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)coverage_resident.data;
	TTSlot *before = &resident->tt_slots[7];

	fresh_instance();
	memset(&coverage_proc, 0, sizeof(coverage_proc));
	memset(&coverage_resident, 0, sizeof(coverage_resident));
	memset(&tt_commit_stage, 0, sizeof(tt_commit_stage));
	coverage_waits = coverage_absorbs = coverage_guts = coverage_commits = 0;
	coverage_redo_applies = 0;
	coverage_finish_after = 1;
	coverage_premature = false;
	coverage_commit_end = coverage_flush = 100;
	MyProc->lxid = 1;
	MyProc->delayChkptFlags = DELAY_CHKPT_START;
	PageSetPageSizeAndVersion((Page)resident, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	((PageHeader)resident)->pd_flags = PD_UNDO_SEG_HEADER;
	resident->segment_id = 1;
	resident->owner_instance = 1;
	resident->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	resident->segment_size_bytes = UNDO_SEGMENT_SIZE_BYTES;
	resident->segment_state = SEGMENT_ACTIVE;
	before->xid = 100;
	before->wrap = 3;
	before->status = TT_SLOT_ACTIVE;
	/* Native ordinary terminal preparation requires a headless ACTIVE slot. */
	coverage_head = coverage_invalid_head;
	before->first_undo_block = coverage_head;
	memcpy(model_files[1].cache, resident, BLCKSZ);
	memcpy(model_files[1].durable, resident, BLCKSZ);
	/* The precommit's already-verified reservation is an explicit boundary. */
	tt_commit_stage.armed = true;
	tt_commit_stage.owner = 1;
	tt_commit_stage.slot_offset = 7;
	tt_commit_stage.segment_id = 1;
	tt_commit_stage.generation.known = true;
	tt_commit_stage.generation.value = 0;
	tt_commit_stage.intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED;
	tt_commit_stage.fd = cluster_undo_smgr_header_writer_open(tt_commit_stage.intent, 1, 1);
	tt_commit_stage.pin.slot = 0;
	tt_commit_stage.predecessor = *before;
	tt_commit_stage.successor = *before;
	tt_commit_stage.successor.status = TT_SLOT_COMMITTED;
	tt_commit_stage.successor.commit_scn = scn_encode(1, 42);
	/* Ordinary COMMIT clears its head; C0's retained-head rule is separate. */
	tt_commit_stage.successor.first_undo_block = coverage_invalid_head;
}

static bool
coverage_durable_committed(void)
{
	TTSlot slot;

	model_crash();
	memcpy(&slot, model_files[1].cache + STAMP_OFFSET, sizeof(slot));
	return slot.status == TT_SLOT_COMMITTED && slot.xid == 100 && slot.wrap == 3
		   && slot.commit_scn == scn_encode(1, 42)
		   && memcmp(&slot.first_undo_block, &coverage_invalid_head, sizeof(UBA)) == 0;
}

static bool
coverage_lose_post_redo_commit(void)
{
	TTSlot slot;

	coverage_setup();
	/* This commit has not entered its insertion window at the earlier cut. */
	MyProc->delayChkptFlags = 0;
	coverage_checkpoint(50);
	MyProc->delayChkptFlags = DELAY_CHKPT_START;
	coverage_finish_commit();
	memcpy(coverage_ordinary_commit.data, model_files[1].cache, BLCKSZ);
	model_crash();
	process_crash_restart(); /* Dirty registrations do not survive a restart. */
	memcpy(&slot, model_files[1].cache + STAMP_OFFSET, sizeof(slot));
	return coverage_waits == 0 && model_files[1].fsyncs == 0 && slot.status == TT_SLOT_ACTIVE
		   && slot.xid == 100 && slot.wrap == 3
		   && memcmp(&slot.first_undo_block, &coverage_head, sizeof(UBA)) == 0;
}

static void
coverage_redo_commit(void)
{
	/* Decoded original COMMIT fields; WAL parsing/selection is outside this cut. */
	cluster_tt_durable_redo_stamp_slot_exact(1, 1, 0, 7, 3, 100, scn_encode(1, 42));
}

UT_TEST(test_pre_redo_commit_waits_for_apply_and_header_sync)
{
	coverage_setup();
	coverage_checkpoint(200);
	UT_ASSERT(!coverage_premature);
	UT_ASSERT_EQ(coverage_waits, 1);
	UT_ASSERT_EQ(coverage_absorbs, 1);
	UT_ASSERT_EQ(coverage_commits, 1);
	UT_ASSERT_EQ(model_files[1].fsyncs, 1);
	UT_ASSERT(coverage_durable_committed());
}

UT_TEST(test_checkpoint_rechecks_until_original_commit_window_finishes)
{
	coverage_setup();
	coverage_finish_after = 3;
	coverage_checkpoint(200);
	UT_ASSERT(!coverage_premature);
	UT_ASSERT_EQ(coverage_waits, 3);
	UT_ASSERT_EQ(coverage_absorbs, 3);
	UT_ASSERT(coverage_durable_committed());
}

UT_TEST(test_applied_commit_is_still_waited_before_checkpoint_sync)
{
	coverage_setup();
	CritSectionCount = 1;
	cluster_tt_slot_durable_commit_apply(coverage_commit_end);
	CritSectionCount = 0;
	UT_ASSERT_EQ(model_files[1].fsyncs, 0);
	coverage_checkpoint(200);
	UT_ASSERT(!coverage_premature);
	UT_ASSERT_EQ(coverage_waits, 1);
	UT_ASSERT(coverage_durable_committed());
}

UT_TEST(test_failed_checkpoint_sync_keeps_commit_pending_for_retry)
{
	sigjmp_buf boundary;
	sigjmp_buf *save = PG_exception_stack;
	volatile bool raised = false;

	coverage_setup();
	model_fsync_fails = true;
	data_sync_retry = true;
	if (sigsetjmp(boundary, 0) == 0) {
		PG_exception_stack = &boundary;
		coverage_checkpoint(200);
	} else
		raised = true;
	PG_exception_stack = save;
	data_sync_retry = false;
	UT_ASSERT(raised);
	UT_ASSERT(!coverage_premature);
	UT_ASSERT_EQ(coverage_commits, 1);
	UT_ASSERT_EQ(model_files[1].fsyncs, 0);
	model_fsync_fails = false;
	coverage_checkpoint(300);
	UT_ASSERT_EQ(model_files[1].fsyncs, 1);
	UT_ASSERT(coverage_durable_committed());
}

UT_TEST(test_post_redo_commit_is_restored_by_exact_redo)
{
	UT_ASSERT(coverage_lose_post_redo_commit());
	coverage_redo_commit();
	UT_ASSERT_EQ(coverage_redo_applies, 1);
	UT_ASSERT_EQ(model_files[1].fsyncs, 1);
	UT_ASSERT(coverage_durable_committed());
	UT_ASSERT_EQ(memcmp(model_files[1].durable, coverage_ordinary_commit.data, BLCKSZ), 0);
}

UT_TEST(test_post_redo_commit_replay_is_idempotent_after_crash)
{
	UT_ASSERT(coverage_lose_post_redo_commit());
	coverage_redo_commit();
	UT_ASSERT(coverage_durable_committed());
	process_crash_restart();
	coverage_redo_commit();
	UT_ASSERT_EQ(coverage_redo_applies, 1);
	UT_ASSERT_EQ(model_files[1].fsyncs, 1);
	coverage_checkpoint(200);
	UT_ASSERT_EQ(model_files[1].fsyncs, 2);
	UT_ASSERT(coverage_durable_committed());
	UT_ASSERT_EQ(memcmp(model_files[1].durable, coverage_ordinary_commit.data, BLCKSZ), 0);
}

UT_TEST(test_post_redo_commit_failed_sync_retries_cached_terminal)
{
	sigjmp_buf boundary;
	sigjmp_buf *save = PG_exception_stack;
	volatile bool raised = false;
	TTSlot durable_slot;

	UT_ASSERT(coverage_lose_post_redo_commit());
	model_fsync_fails = true;
	if (sigsetjmp(boundary, 0) == 0) {
		PG_exception_stack = &boundary;
		coverage_redo_commit();
	} else
		raised = true;
	PG_exception_stack = save;
	model_fsync_fails = false;
	UT_ASSERT(raised);
	UT_ASSERT_EQ(coverage_redo_applies, 0);
	UT_ASSERT_EQ(model_files[1].fsyncs, 0);
	memcpy(&durable_slot, model_files[1].durable + STAMP_OFFSET, sizeof(durable_slot));
	UT_ASSERT_EQ(durable_slot.status, TT_SLOT_ACTIVE);
	UT_ASSERT_EQ(memcmp(model_files[1].cache, coverage_ordinary_commit.data, BLCKSZ), 0);
	/* Own shared ALREADY must register this cache image for checkpoint sync. */
	coverage_redo_commit();
	UT_ASSERT_EQ(coverage_redo_applies, 0);
	UT_ASSERT_EQ(model_files[1].fsyncs, 0);
	coverage_checkpoint(200);
	UT_ASSERT_EQ(model_files[1].fsyncs, 1);
	UT_ASSERT(coverage_durable_committed());
	UT_ASSERT_EQ(memcmp(model_files[1].durable, coverage_ordinary_commit.data, BLCKSZ), 0);
}

UT_TEST(test_active_base_survives_multiple_checkpoints_without_terminal_inference)
{
	TTSlot slot;

	coverage_setup();
	cluster_undo_smgr_header_writer_close(tt_commit_stage.fd);
	memset(&tt_commit_stage, 0, sizeof(tt_commit_stage));
	tt_commit_stage.fd = -1;
	MyProc->delayChkptFlags = 0;
	coverage_commit_end = InvalidXLogRecPtr;
	coverage_checkpoint(200);
	coverage_checkpoint(300);
	model_crash();
	memcpy(&slot, model_files[1].cache + STAMP_OFFSET, sizeof(slot));
	UT_ASSERT_EQ(slot.status, TT_SLOT_ACTIVE);
	UT_ASSERT_EQ(slot.xid, 100);
	UT_ASSERT_EQ(coverage_commits, 0);
	UT_ASSERT_EQ(coverage_waits, 0);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_pre_redo_commit_waits_for_apply_and_header_sync);
	UT_RUN(test_checkpoint_rechecks_until_original_commit_window_finishes);
	UT_RUN(test_applied_commit_is_still_waited_before_checkpoint_sync);
	UT_RUN(test_failed_checkpoint_sync_keeps_commit_pending_for_retry);
	UT_RUN(test_post_redo_commit_is_restored_by_exact_redo);
	UT_RUN(test_post_redo_commit_replay_is_idempotent_after_crash);
	UT_RUN(test_post_redo_commit_failed_sync_retries_cached_terminal);
	UT_RUN(test_active_base_survives_multiple_checkpoints_without_terminal_inference);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
