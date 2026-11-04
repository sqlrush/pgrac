/* Original checkpoint ordering with the real managed CLOG/SLRU/file writer.
 * WAL positions, other checkpoint families and control publication are finite
 * boundaries. The original shutdown cross-check is executed unchanged. */
#define CLOG_CREATION_MAIN boundary_creation_main
int boundary_creation_main(void);
#include "test_cluster_clog_creation.c"
#undef CLOG_CREATION_MAIN

static unsigned boundary_redo_samples, boundary_records, boundary_publications;
static unsigned boundary_prepares;
static bool boundary_late_dirty;

static void
boundary_prepare(void)
{
	boundary_prepares++;
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(!control_held && !io_held);
}

/* The real frame writer's insert counter advances this controlled WAL cursor.
 * The checkpoint record starts at that cursor after CheckPointGuts. */
static XLogRecPtr
boundary_position(void)
{
	return 512 + (XLogRecPtr) inserts * 16384;
}

static void
boundary_checkpoint(int flags)
{
	bool shutdown = (flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY)) != 0;
	struct { XLogRecPtr redo; } checkPoint;
	XLogRecPtr ProcLastRecPtr;

#define SyncPreCheckpoint boundary_prepare
#include "test_cluster_clog_checkpoint_boundary_prelude.inc"
#undef SyncPreCheckpoint
	checkPoint.redo = boundary_position();
	boundary_redo_samples++;
	if (boundary_late_dirty) {
		actual_end += 1000;
		dirty[0] = true;
	}
#include "test_cluster_clog_checkpoint_boundary_guts.inc"
	ProcLastRecPtr = boundary_position();
	boundary_records++;
#include "test_cluster_clog_checkpoint_boundary_check.inc"
	boundary_publications++;
}

static bool
boundary_raises(int flags)
{
	volatile bool caught = false;

	PG_TRY(); { boundary_checkpoint(flags); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	return caught;
}

static void
boundary_reset(ProcessingMode processing, uint16 origin)
{
	creation_reset_origin(processing, origin);
	boundary_redo_samples = boundary_records = boundary_publications = 0;
	boundary_prepares = 0;
	boundary_late_dirty = false;
	dirty[0] = true;
}

static void
boundary_cleanup_error(void)
{
	UT_ASSERT(!io_held && !native_borrows);
	if (control_held) {
		/* The WAL provider throws directly; elog failures reset holdoffs. */
		if (InterruptHoldoffCount == 0)
			HOLD_INTERRUPTS();
		LWLockRelease(XactSLRULock);
	}
	checkpoint_close();
}

UT_TEST(shutdown_writes_full_clog_before_selecting_redo_for_every_origin)
{
	const uint16 origins[] = {0, 1, 127};

	for (unsigned i = 0; i < lengthof(origins); i++) {
		ClusterClogWalObservationV2 record = {0};

		boundary_reset(NormalProcessing, origins[i]);
		UT_ASSERT(!boundary_raises(CHECKPOINT_IS_SHUTDOWN));
		UT_ASSERT_EQ(boundary_redo_samples, 1);
		UT_ASSERT_EQ(boundary_prepares, 1);
		UT_ASSERT_EQ(boundary_records, 1);
		UT_ASSERT_EQ(boundary_publications, 1);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT(cluster_clog_wal_decode_v2(0x50, native_record, sizeof(native_record), &record));
		UT_ASSERT_EQ(record.binding.identity.origin, origins[i]);
		UT_ASSERT(!dirty[0]);
		checkpoint_close();
	}
}

UT_TEST(online_checkpoint_retains_original_wal_and_writeback)
{
	boundary_reset(NormalProcessing, 1);
	UT_ASSERT(!boundary_raises(0));
	UT_ASSERT_EQ(boundary_publications, 1);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(writes, 1);
	checkpoint_close();
}

UT_TEST(bootstrap_checkpoint_does_not_invent_image_wal)
{
	boundary_reset(BootstrapProcessing, 1);
	creation_boot_page();
	UT_ASSERT(!boundary_raises(CHECKPOINT_IS_SHUTDOWN));
	UT_ASSERT_EQ(boundary_publications, 1);
	UT_ASSERT_EQ(inserts + flushes, 0);
	UT_ASSERT_EQ(writes, 1);
	checkpoint_close();
}

UT_TEST(shutdown_sync_error_cannot_select_or_publish_checkpoint)
{
	boundary_reset(NormalProcessing, 1);
	fail_sync = true;
	UT_ASSERT(boundary_raises(CHECKPOINT_IS_SHUTDOWN));
	UT_ASSERT_EQ(boundary_redo_samples + boundary_records + boundary_publications, 0);
	UT_ASSERT(dirty[0]);
	UT_ASSERT_EQ(inserts, 1);
	boundary_cleanup_error();
}

UT_TEST(shutdown_wal_error_keeps_original_dirty_page_and_borrow_cleanup)
{
	boundary_reset(NormalProcessing, 1);
	wal_throw = 1;
	UT_ASSERT(boundary_raises(CHECKPOINT_IS_SHUTDOWN));
	UT_ASSERT_EQ(boundary_redo_samples + boundary_records + boundary_publications, 0);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT(dirty[0]);
	boundary_cleanup_error();
}

UT_TEST(shutdown_rejects_changed_creation_identity_before_redo)
{
	boundary_reset(NormalProcessing, 1);
	creation_info.creation.creator_pid++;
	UT_ASSERT(boundary_raises(CHECKPOINT_IS_SHUTDOWN));
	UT_ASSERT_EQ(boundary_redo_samples + boundary_records + boundary_publications, 0);
	UT_ASSERT_EQ(inserts + writes, 0);
	UT_ASSERT(dirty[0]);
	boundary_cleanup_error();
}

UT_TEST(late_wal_still_triggers_original_shutdown_refusal)
{
	boundary_reset(NormalProcessing, 1);
	boundary_late_dirty = true;
	UT_ASSERT(boundary_raises(CHECKPOINT_IS_SHUTDOWN));
	UT_ASSERT_EQ(boundary_publications, 0);
	UT_ASSERT_EQ(boundary_records, 1);
	UT_ASSERT_EQ(checkpoint_error_level, PANIC);
	checkpoint_close();
}

int
main(void)
{
	strlcpy(directory, "/tmp/pgrac-clog-boundary-XXXXXX", sizeof(directory));
	if (mkdtemp(directory) == NULL) abort();
	snprintf(filename, sizeof(filename), "%s/0000", directory);
	UT_PLAN(7);
	UT_RUN(shutdown_writes_full_clog_before_selecting_redo_for_every_origin);
	UT_RUN(online_checkpoint_retains_original_wal_and_writeback);
	UT_RUN(bootstrap_checkpoint_does_not_invent_image_wal);
	UT_RUN(shutdown_sync_error_cannot_select_or_publish_checkpoint);
	UT_RUN(shutdown_wal_error_keeps_original_dirty_page_and_borrow_cleanup);
	UT_RUN(shutdown_rejects_changed_creation_identity_before_redo);
	UT_RUN(late_wal_still_triggers_original_shutdown_refusal);
	rmdir(directory);
	UT_DONE(); return ut_failed_count != 0;
}
