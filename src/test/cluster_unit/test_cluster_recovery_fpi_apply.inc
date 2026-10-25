/* One original native Finish followed by the original real-file installer.
 * No recovery-driver, source/history qualification or discharge proof. */
static void
reset_apply(bool enable_checksums)
{
	PGAlignedBlock before;

	install_file_close();
	reading_input = installing = applying = false;
	reset_insert();
	memset(&inserted, 0, sizeof(inserted));
	checksums = enable_checksums;
	reads = writes = syncs = opens = install_hashes = 0;
	throw_read_at = 0;
	throw_write = throw_sync = postread_corrupt = false;
	drift_after_read = drift_after_sync = drift_after_postread = false;
	writer_drift_after_read = missing_target = false;
	/* Exact end/timeline returned by the original controlled native WAL. */
	native_flush_end = 0x2000240;
	native_flush_timeline = installed_writer.ref.timeline;
	strlcpy(install_path, "/tmp/pgrac-fpi-apply-XXXXXX", sizeof(install_path));
	install_fd = mkstemp(install_path);
	UT_ASSERT(install_fd >= 0);
	UT_ASSERT_EQ(ftruncate(install_fd, (off_t)8 * BLCKSZ), 0);
	before = predecessor;
	install_file_write(&before);
	UT_ASSERT_EQ(inserts + flushes + promotes, 0);
}

static bool
apply_image(void)
{
	bool result;

	applying = true;
	result = rf_page_recovery_fpi_apply_smgr_v1(&frame, &owner,
		(RfPageAuthorityPreflightV1 *)&preflight_object, &fpi_serial,
		(ClusterWalRetentionPin *)&fpi_pin_object, &inserted);
	applying = installing = false;
	return result;
}

static void
assert_apply_record(void)
{
	UT_ASSERT_EQ(capacity_calls, 1); UT_ASSERT_EQ(hash_calls, 1);
	UT_ASSERT_EQ(promotes, 1); UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(flushes, 1);
	UT_ASSERT_EQ(inserted.record_start, 0x2000040);
	UT_ASSERT_EQ(inserted.record_end, 0x2000240);
	UT_ASSERT(memcmp(&inserted.image, &frame, sizeof(frame)) == 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
}

UT_TEST(fpi_apply_flushes_before_exact_target_install)
{
	for (unsigned checksum = 0; checksum < 2; checksum++) {
		PGAlignedBlock expected, actual;
		reset_apply(checksum != 0);
		UT_ASSERT(apply_image());
		assert_apply_record();
		UT_ASSERT(inserted.wal_flushed && inserted.target_synced);
		UT_ASSERT_EQ(opens, 1); UT_ASSERT_EQ(reads, 2);
		UT_ASSERT_EQ(writes, 1); UT_ASSERT_EQ(syncs, 1);
		UT_ASSERT_EQ(install_hashes, 1);
		expected = frame.image;
		PageSetLSN(expected.data, inserted.record_end);
		((PageHeader)expected.data)->pd_checksum = 0;
		if (checksums)
			((PageHeader)expected.data)->pd_checksum = pg_checksum_page(expected.data, 7);
		UT_ASSERT_EQ(pread(install_fd, actual.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(memcmp(actual.data, expected.data, BLCKSZ) == 0);
		assert_install_pending(false);
	}
}

UT_TEST(fpi_apply_exact_result_still_syncs_and_postreads)
{
	PGAlignedBlock expected, actual;
	reset_apply(true);
	expected = frame.image;
	PageSetLSN(expected.data, native_flush_end);
	install_file_write(&expected);
	UT_ASSERT(apply_image());
	assert_apply_record();
	UT_ASSERT(inserted.wal_flushed && inserted.target_synced);
	UT_ASSERT_EQ(opens, 1); UT_ASSERT_EQ(reads, 2);
	UT_ASSERT_EQ(writes, 0); UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(pread(install_fd, actual.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
	UT_ASSERT(memcmp(actual.data, expected.data, BLCKSZ) == 0);
	assert_install_pending(false);
}

UT_TEST(fpi_apply_early_refusals_never_touch_target)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		PGAlignedBlock before, after;
		reset_apply(true);
		if (fault == 0) installed = false;
		if (fault == 1) MyBackendType = B_BACKEND;
		if (fault == 2) fpi_serial.mode = CLUSTER_RECOVERY_SERIAL_INPUT_SEAL;
		if (fault == 3) recovering = true;
		if (fault == 4) frame.owner = GENERIC_XLOG_ITL_FINISH;
		if (fault == 5) ShutdownRequestPending = 1;
		if (fault == 6) owner.executor_pid++;
		if (fault == 7) promote_ok = false;
		if (fault == 8) fpi_protection_current = false;
		if (fault == 9) stale_writer_at = 1;
		UT_ASSERT_EQ(pread(install_fd, before.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(!apply_image());
		UT_ASSERT(!inserted.wal_flushed && !inserted.target_synced);
		UT_ASSERT_EQ(inserts + flushes + opens + reads + writes + syncs + install_hashes, 0);
		UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);
		UT_ASSERT_EQ(pread(install_fd, after.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(memcmp(before.data, after.data, BLCKSZ) == 0);
		UT_ASSERT(fpi_serial.held);
	}
}

UT_TEST(fpi_apply_uncertain_wal_never_reaches_install)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		volatile bool caught = false;
		PGAlignedBlock before, after;
		reset_apply(true);
		if (fault == 0) stale_writer_at = 2;
		if (fault == 1) stale_protection_at = 2;
		flush_error = fault == 2;
		insert_error = fault == 3;
		corrupt_registered_token = fault == 4;
		UT_ASSERT_EQ(pread(install_fd, before.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
		PG_TRY();
		{
			UT_ASSERT(!apply_image());
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		applying = installing = false;
		UT_ASSERT_EQ(caught, fault >= 2);
		UT_ASSERT(!inserted.wal_flushed && !inserted.target_synced);
		UT_ASSERT(inserted.guard == (RfPageAuthorityGuardV1 *)&fpi_guard_object);
		UT_ASSERT(fpi_serial.held);
		UT_ASSERT_EQ(opens + reads + writes + syncs + install_hashes, 0);
		UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, fault < 3
			? CLUSTER_RECOVERY_STAMP_INSERTED : CLUSTER_RECOVERY_STAMP_INSERTING);
		UT_ASSERT(region.thread_replay[2].stamp.failed);
		UT_ASSERT_EQ(CritSectionCount, fault < 3 ? 0 : 1);
		CritSectionCount = 0; /* Controlled critical-error fixture unwinding only. */
		UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&inserted.window));
		UT_ASSERT(!cluster_recovery_stamp_clear_v1(&inserted.window));
		UT_ASSERT_EQ(pread(install_fd, after.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(memcmp(before.data, after.data, BLCKSZ) == 0);
	}
}

UT_TEST(fpi_apply_target_refusals_keep_flushed_pending_debt)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		PGAlignedBlock before, after;
		reset_apply(true);
		if (fault == 0) {
			before = predecessor;
			((PageHeader)before.data)->pd_block_scn = 777;
			install_file_write(&before);
		}
		postread_corrupt = fault == 1;
		drift_after_read = fault == 2;
		writer_drift_after_read = fault == 3;
		missing_target = fault == 4;
		if (fault == 5) native_flush_end--;
		UT_ASSERT_EQ(pread(install_fd, before.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(!apply_image());
		assert_apply_record();
		UT_ASSERT(inserted.wal_flushed && !inserted.target_synced);
		UT_ASSERT_EQ(writes, fault == 1 ? 1 : 0);
		UT_ASSERT_EQ(syncs, fault == 1 ? 1 : 0);
		if (fault != 1) {
			UT_ASSERT_EQ(pread(install_fd, after.data, BLCKSZ, (off_t)7 * BLCKSZ), BLCKSZ);
			UT_ASSERT(memcmp(before.data, after.data, BLCKSZ) == 0);
		}
		assert_install_pending(true);
	}
}

UT_TEST(fpi_apply_target_errors_rethrow_with_original_debt)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		volatile bool caught = false;
		reset_apply(true);
		throw_read_at = fault == 0 ? 1 : fault == 3 ? 2 : 0;
		throw_write = fault == 1;
		throw_sync = fault == 2;
		PG_TRY();
		{
			(void)apply_image();
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		applying = installing = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(raised_level, ERROR);
		assert_apply_record();
		UT_ASSERT(inserted.wal_flushed && !inserted.target_synced);
		assert_install_pending(true);
	}
}
