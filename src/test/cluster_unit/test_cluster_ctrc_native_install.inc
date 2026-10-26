/*-------------------------------------------------------------------------
 *
 * test_cluster_ctrc_native_install.inc
 *     Original native CTRC custody through real-file FPI installation.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_ctrc_native_install.inc
 *
 * NOTES
 *     Original native holders/PAGE, immutable image and stamp FSM execute.
 *     Native WAL flush, installed writer and SMGR transport are fixture leaves.
 *
 *-------------------------------------------------------------------------
 */
static bool native_install_active;
static RfPageCtrcFpiInsertV1 *native_install_input;
static unsigned native_install_fault, native_install_at, native_install_stage;
static unsigned native_install_writes, native_install_syncs;
static XLogRecPtr native_install_flush;
static TimeLineID native_install_timeline;

pg_cryptohash_ctx *recovery_fpi_hash_create(pg_cryptohash_type type);
void ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *context);
ClusterControlRootResult cluster_wal_writer_check(const ClusterWalWriterToken *writer);
bool ClusterStartupInstalledRecoveryWriterV1(ClusterWalWriterToken *writer,
	uint8 operation_uuid[16], uint64 *generation);
void smgrread(SMgrRelation relation, ForkNumber forknum, BlockNumber blockno, void *buffer);
XLogRecPtr GetFlushRecPtr(TimeLineID *timeline);

pg_cryptohash_ctx *
recovery_fpi_hash_create(pg_cryptohash_type type)
{
	if (native_step_active) native_step_hash_gap();
	return native_install_active ? pg_cryptohash_create(type)
		: native_install_original_hash_create(type);
}

void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *context)
{
	/* The unit bitmap also represents an independent process's collision. */
	if (native_step_active && native_step_foreign_blocker) return;
	native_install_original_held(callback, context);
	if (native_install_active && native_insert_caller_lock && native_insert_page_held())
		callback(NULL, LW_EXCLUSIVE, context);
}

ClusterControlRootResult
cluster_wal_writer_check(const ClusterWalWriterToken *writer)
{
	if (native_install_active && native_install_fault == 20)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	return native_install_original_writer_current(writer);
}

bool
ClusterStartupInstalledRecoveryWriterV1(ClusterWalWriterToken *writer,
	uint8 operation_uuid[16], uint64 *generation)
{
	bool ok = native_install_original_writer_installed(writer, operation_uuid, generation);
	if (native_install_active) {
		if (native_install_fault == 21) (*generation)++;
		if (native_install_fault == 22) operation_uuid[0]++;
		if (native_install_fault == 23) writer->epoch++;
	}
	return ok;
}

XLogRecPtr
GetFlushRecPtr(TimeLineID *timeline)
{
	if (!native_install_active) return native_install_original_flush_ptr(timeline);
	UT_ASSERT(native_install_active && native_insert_page_held());
	*timeline = native_install_timeline;
	return native_install_flush;
}

static void
native_install_drift(void)
{
	switch (native_install_fault) {
	case 1: ctrc_inputs_work.entries[1].slot.first_undo_block.raw[0]++; break;
	case 2: native_stamp_source.root.file_txn_seq++; break;
	case 3: native_install_input->image.image.data[100]++; break;
	case 4: native_install_input->record_end += 8; break;
	case 5: native_install_input->window.owner.writer.epoch++; break;
	case 6: native_fpi_region.checkpoint_owner.tx_result.processed_count++; break;
	case 7: native_install_flush--; break;
	case 8: cluster_wal_inputs_release_v1(&native_insert_live->page.replay.inputs); break;
	case 9: pg_re_throw(); break;
	}
}

void
smgrread(SMgrRelation relation, ForkNumber forknum, BlockNumber blockno, void *buffer)
{
	native_install_original_read(relation, forknum, blockno, buffer);
	if (native_install_active && native_install_stage == 0
		&& ctrc_read_native_reads == native_install_at)
		native_install_drift();
}

void
smgrwrite(SMgrRelation relation, ForkNumber forknum, BlockNumber blockno,
	const void *buffer, bool skipFsync)
{
	UT_ASSERT(native_install_active && native_insert_page_held());
	UT_ASSERT(relation == &ctrc_read_native_relation);
	UT_ASSERT_EQ(forknum, MAIN_FORKNUM); UT_ASSERT_EQ(blockno, 7);
	UT_ASSERT(!skipFsync); UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(native_fpi_region.thread_replay[native_stamp_source.selected.source.claim.identity.origin_thread_id].stamp.phase,
		CLUSTER_RECOVERY_STAMP_INSERTED);
	UT_ASSERT_EQ(pwrite(native_insert_live->main_fd, buffer, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
	native_install_writes++;
	if (native_install_stage == 1) native_install_drift();
}

void
smgrimmedsync(SMgrRelation relation, ForkNumber forknum)
{
	PGAlignedBlock changed;
	UT_ASSERT(native_install_active && native_insert_page_held());
	UT_ASSERT(relation == &ctrc_read_native_relation && forknum == MAIN_FORKNUM);
	UT_ASSERT_EQ(CritSectionCount, 0); UT_ASSERT_EQ(fsync(native_insert_live->main_fd), 0);
	native_install_syncs++;
	if (native_install_stage == 2) native_install_drift();
	if (native_install_fault == 10) {
		UT_ASSERT_EQ(pread(native_insert_live->main_fd, changed.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
		changed.data[BLCKSZ - 1]++;
		ctrc_read_native_write(native_insert_live, MAIN_FORKNUM, 7, &changed);
	}
}

static void
native_install_fixture(CtrcReadNativeFixture *f, RfPageCtrcFpiInsertV1 *inserted,
	uint32 source, bool checksums)
{
	PGAlignedBlock page;
	native_install_active = false;
	native_install_setup = true;
	native_insert_fixture(f, source);
	native_install_setup = false;
	*inserted = native_insert(f, 2, 1024 * 1024);
	UT_ASSERT(inserted->wal_flushed);
	if (!checksums) {
		ctrc_read_native_checksums = false;
		for (unsigned i = 0; i < 3; i++) {
			ForkNumber fork = i < 2 ? SPACE_FORKNUM : MAIN_FORKNUM;
			BlockNumber block = i < 2 ? i : 7;
			UT_ASSERT_EQ(pread(i < 2 ? f->space_fd : f->main_fd,
				page.data, BLCKSZ, (off_t)block * BLCKSZ), BLCKSZ);
			ctrc_read_native_write(f, fork, block, &page);
		}
	}
	native_install_input = inserted;
	native_install_fault = native_install_at = native_install_stage = 0;
	native_install_writes = native_install_syncs = ctrc_read_native_reads = ctrc_read_native_opens = 0;
	native_install_flush = 0x2000240;
	native_install_timeline = ctrc_inputs_owner.writer.ref.timeline;
	native_install_active = true;
}

static void
native_install_finish(CtrcReadNativeFixture *f)
{
	native_install_active = false; native_install_input = NULL;
	native_insert_finish(f);
}

static bool
native_install(CtrcReadNativeFixture *f, Size count, Size budget)
{
	return rf_page_recovery_ctrc_replays_fpi_install_smgr_v1(tx_inputs_start,
		&ctrc_inputs_work, &native_stamp_source, f->page.page.authority,
		native_install_input, count, budget);
}

static void
native_install_expected(const RfPageCtrcFpiInsertV1 *inserted, PGAlignedBlock *expected)
{
	*expected = inserted->image.image;
	PageSetLSNPreserveOrigin(expected->data, 0x2000240);
	UT_ASSERT_EQ(ctrc_inputs_owner.writer.ref.claim.identity.origin_node_id, 0);
	UT_ASSERT(PageSetLSNOrigin(expected->data, 0));
	((PageHeader)expected->data)->pd_checksum = 0;
	if (ctrc_read_native_checksums)
		((PageHeader)expected->data)->pd_checksum = pg_checksum_page(expected->data, 7);
}

static void
native_install_retained(void)
{
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && native_insert_page_held());
	UT_ASSERT_EQ(native_fpi_region.thread_replay[native_stamp_source.selected.source.claim.identity.origin_thread_id].stamp.phase,
		CLUSTER_RECOVERY_STAMP_INSERTED);
	UT_ASSERT(native_fpi_allocation == NULL);
}

UT_TEST(test_native_install_writes_syncs_and_postreads_the_actual_writer_page)
{
	for (unsigned variant = 0; variant < 4; variant++) {
		CtrcReadNativeFixture f; RfPageCtrcFpiInsertV1 inserted, fixed;
		PGAlignedBlock expected, actual; ClusterRecoveryStampCaptureV1 capture;
		unsigned releases, grants;
		native_install_fixture(&f, &inserted, variant % 2, variant < 2);
		fixed = inserted; native_install_expected(&inserted, &expected);
		releases = test_tx_ir_releases; grants = test_tx_ir_grants;
		UT_ASSERT(native_install(&f, 2, 1024 * 1024));
		UT_ASSERT_EQ(native_install_writes, 1); UT_ASSERT_EQ(native_install_syncs, 1);
		UT_ASSERT_EQ(ctrc_read_native_reads, 10);
		UT_ASSERT_EQ(pread(f.main_fd, actual.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(memcmp(actual.data, expected.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(PageGetLSN(actual.data), 0x2000240);
		UT_ASSERT_EQ(((PageHeader)actual.data)->pd_block_scn, 100001);
		UT_ASSERT(memcmp(&inserted, &fixed, sizeof(fixed)) == 0);
		UT_ASSERT_EQ(test_tx_ir_releases, releases); UT_ASSERT_EQ(test_tx_ir_grants, grants);
		UT_ASSERT(cluster_recovery_stamp_capture_v1(&capture));
		UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&capture), CLUSTER_RECOVERY_STAMP_PENDING);
		native_install_retained(); native_install_finish(&f);
	}
}

UT_TEST(test_native_install_already_result_still_syncs_and_postreads)
{
	CtrcReadNativeFixture f; RfPageCtrcFpiInsertV1 inserted; PGAlignedBlock expected;
	native_install_fixture(&f, &inserted, 1, true); native_install_expected(&inserted, &expected);
	ctrc_read_native_write(&f, MAIN_FORKNUM, 7, &expected);
	UT_ASSERT(native_install(&f, 2, 1024 * 1024));
	UT_ASSERT_EQ(native_install_writes, 0); UT_ASSERT_EQ(native_install_syncs, 1);
	UT_ASSERT_EQ(ctrc_read_native_reads, 10);
	native_install_retained(); native_install_finish(&f);
}

UT_TEST(test_native_install_bad_entry_refuses_before_storage)
{
	for (unsigned fault = 0; fault < 15; fault++) {
		CtrcReadNativeFixture f; RfPageCtrcFpiInsertV1 inserted; Size count = 2, budget = 1024 * 1024;
		native_install_fixture(&f, &inserted, 0, true);
		switch (fault) {
		case 0: MyBackendType = B_CHECKPOINTER; break;
		case 1: ShutdownRequestPending = true; break;
		case 2: inputs_native_recovery = true; break;
		case 3: count = 1; break;
		case 4: count = UINT32_MAX; break;
		case 5: budget = 1; break;
		case 6: inserted.wal_flushed = false; break;
		case 7: inserted.record_end++; break;
		case 8: native_install_flush--; break;
		case 9: native_install_timeline++; break;
		case 10: native_stamp_source.ir_request_id++; break;
		case 11: native_fpi_region.checkpoint_owner.tx_accepted_generation = 0; break;
		case 12: native_install_fault = 20; break;
		case 13: native_install_fault = 21; break;
		case 14: native_install_fault = 22; break;
		}
		UT_ASSERT(!native_install(&f, count, budget));
		UT_ASSERT_EQ(ctrc_read_native_opens, 0); UT_ASSERT_EQ(ctrc_read_native_reads, 0);
		UT_ASSERT_EQ(native_install_writes, 0); UT_ASSERT_EQ(native_install_syncs, 0);
		MyBackendType = B_STARTUP; ShutdownRequestPending = false; inputs_native_recovery = false;
		native_install_retained(); native_install_finish(&f);
	}
}

UT_TEST(test_native_install_refuses_missing_torn_third_and_changed_result)
{
	for (unsigned variant = 0; variant < 20; variant++) {
		CtrcReadNativeFixture f; RfPageCtrcFpiInsertV1 inserted; PGAlignedBlock bad;
		unsigned fault = variant % 10;
		native_install_fixture(&f, &inserted, 0, variant < 10);
		UT_ASSERT_EQ(pread(f.main_fd, bad.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
		switch (fault) {
		case 0: ctrc_read_native_missing_main = true; break;
		case 1: memset(&bad, 0, sizeof(bad)); break;
		case 2: ((PageHeader)bad.data)->pd_lower = 1; break;
		case 3: ((PageHeader)bad.data)->pd_block_scn = 999; break;
		case 4: native_install_expected(&inserted, &bad); bad.data[BLCKSZ - 1]++; break;
		case 5: ctrc_read_native_missing_space = true; break;
		case 6: UT_ASSERT_EQ(ftruncate(f.space_fd, BLCKSZ), 0); break;
		case 7: {
			ClusterSpaceIdentity wrong = f.space;
			wrong.incarnation[0]++;
			UT_ASSERT(cluster_space_identity_page_encode(&wrong, 51, bad.data, BLCKSZ));
			ctrc_read_native_write(&f, SPACE_FORKNUM, 0, &bad); break;
		}
		case 8: {
			ClusterSpaceReservation reservation = {0};
			reservation.identity = f.space; reservation.next_block = 7;
			UT_ASSERT(cluster_space_reservation_page_encode(&reservation, 52, bad.data, BLCKSZ));
			ctrc_read_native_write(&f, SPACE_FORKNUM, 1, &bad); break;
		}
		case 9: ((PageHeader)bad.data)->pd_checksum++; break;
		}
		if (fault >= 1 && fault <= 4) ctrc_read_native_write(&f, MAIN_FORKNUM, 7, &bad);
		if (fault == 9) UT_ASSERT_EQ(pwrite(f.main_fd, bad.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(!native_install(&f, 2, 1024 * 1024));
		UT_ASSERT_EQ(native_install_writes, 0); UT_ASSERT_EQ(native_install_syncs, 0);
		UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
		native_install_retained(); native_install_finish(&f);
	}
}

UT_TEST(test_native_install_rechecks_complete_inputs_after_every_read)
{
	for (unsigned fault = 1; fault <= 7; fault++) {
		for (unsigned read = 1; read <= 10; read++) {
			CtrcReadNativeFixture f; RfPageCtrcFpiInsertV1 inserted;
			native_install_fixture(&f, &inserted, 0, true);
			native_install_fault = fault; native_install_at = read;
			UT_ASSERT(!native_install(&f, 2, 1024 * 1024));
			UT_ASSERT_EQ(ctrc_read_native_reads, read);
			UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
			native_install_retained(); native_install_finish(&f);
		}
	}
}

UT_TEST(test_native_install_write_and_sync_drift_retains_window)
{
	for (unsigned stage = 1; stage <= 2; stage++) {
		for (unsigned fault = 1; fault <= 7; fault++) {
			CtrcReadNativeFixture f; RfPageCtrcFpiInsertV1 inserted;
			native_install_fixture(&f, &inserted, 0, true);
			native_install_fault = fault; native_install_stage = stage;
			UT_ASSERT(!native_install(&f, 2, 1024 * 1024));
			UT_ASSERT_EQ(native_install_writes, 1); UT_ASSERT_EQ(native_install_syncs, stage - 1);
			UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
			native_install_retained(); native_install_finish(&f);
		}
	}
	{
		CtrcReadNativeFixture f; RfPageCtrcFpiInsertV1 inserted;
		native_install_fixture(&f, &inserted, 0, true); native_install_fault = 10;
		UT_ASSERT(!native_install(&f, 2, 1024 * 1024));
		UT_ASSERT_EQ(native_install_writes, 1); UT_ASSERT_EQ(native_install_syncs, 1);
		UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
		native_install_retained(); native_install_finish(&f);
	}
}

UT_TEST(test_native_install_io_errors_free_only_private_memory)
{
	for (unsigned point = 0; point <= 12; point++) {
		CtrcReadNativeFixture f; RfPageCtrcFpiInsertV1 inserted; volatile bool caught = false;
		native_install_fixture(&f, &inserted, 0, true);
		if (!point) native_fpi_alloc_error = true;
		else { native_install_fault = 9; native_install_at = point; }
		if (point > 10) native_install_stage = point - 10;
		PG_TRY(); { (void)native_install(&f, 2, 1024 * 1024); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught); UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
		native_install_retained(); native_install_finish(&f);
	}
}

UT_TEST(test_native_install_requires_current_original_inserted_window)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		CtrcReadNativeFixture f; RfPageCtrcFpiInsertV1 inserted;
		native_install_fixture(&f, &inserted, 0, true);
		switch (fault) {
		case 0: (void)cluster_recovery_stamp_fail_v1(&inserted.window); break;
		case 1: native_fpi_region.thread_replay[128].stamp.record_end++; break;
		case 2: native_fpi_region.thread_replay[128].stamp.phase = CLUSTER_RECOVERY_STAMP_INSERTING; break;
		case 3: native_install_fault = 23; break;
		case 4: native_install_fault = 8; native_install_at = 3; break;
		}
		UT_ASSERT(!native_install(&f, 2, 1024 * 1024));
		UT_ASSERT_EQ(native_install_writes, 0); UT_ASSERT_EQ(native_install_syncs, 0);
		UT_ASSERT(native_insert_page_held());
		if (fault != 4) UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		native_install_finish(&f);
	}
}

UT_TEST(test_native_install_extra_lwlock_refuses_before_owner_snapshot)
{
	CtrcReadNativeFixture f; RfPageCtrcFpiInsertV1 inserted; volatile bool caught = false, synced = false;
	native_install_fixture(&f, &inserted, 0, true); native_insert_caller_lock = true;
	PG_TRY(); { synced = native_install(&f, 2, 1024 * 1024); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(!caught && !synced); UT_ASSERT_EQ(ctrc_read_native_reads, 0);
	native_install_retained(); native_install_finish(&f);
}
