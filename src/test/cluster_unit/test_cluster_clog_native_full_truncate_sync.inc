/*-------------------------------------------------------------------------
 * test_cluster_clog_native_full_truncate_sync.inc
 *     Complete original founder FULL truncation before recording consumption.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_native_full_truncate_sync.inc
 * NOTES
 *     Original redo, SLRU deletion and file sync execute. Authority, locks and
 *     WAL transport remain fixtures; no continuous lifetime qualification.
 *-------------------------------------------------------------------------
 */
static SyncRequestHandler full_truncate_saved_handler;
static TransactionId full_truncate_saved_oldest;
static int full_truncate_saved_latest;
static bool full_truncate_saved_native;

static void
full_truncate_sync_begin(XLogReaderState *reader, DecodedXLogRecord *decoded,
	xl_clog_truncate_full *cut, uint32 epoch)
{
	xl_clog_zeropage_full unused;

	own_full_begin();
	/* Earlier fixture groups leave unrelated segments in this origin. Use
	 * an empty private fixture directory so the exact deletion count is stable. */
	{
		char directory[MAXPGPATH], path[MAXPGPATH];
		DIR *dir;
		struct dirent *entry;

		snprintf(directory, sizeof(directory), "%s/native_side/origin_1/pg_xact", fixture_root);
		dir = opendir(directory);
		UT_ASSERT(dir != NULL);
		if (dir != NULL) {
			while ((entry = readdir(dir)) != NULL) {
				if (entry->d_name[0] == '.') continue;
				snprintf(path, sizeof(path), "%s/%s", directory, entry->d_name);
				UT_ASSERT_EQ(unlink(path), 0);
			}
			UT_ASSERT_EQ(closedir(dir), 0);
		}
	}
	full_sync_reader(reader, decoded, &unused, 128);
	*cut = (xl_clog_truncate_full){128, 128 * CLOG_XACTS_PER_PAGE + 17, 79, 0,
		((uint64)epoch << 32) | (128 * CLOG_XACTS_PER_PAGE + 17)};
	decoded->main_data = (char *)cut;
	decoded->main_data_len = sizeof(*cut);
	decoded->header.xl_info = CLOG_TRUNCATE_FULL;
	full_truncate_saved_handler = XactCtl->sync_handler;
	full_truncate_saved_oldest = fixture_variable_cache.oldestClogXid;
	full_truncate_saved_latest = XactCtl->shared->latest_page_number;
	full_truncate_saved_native = fixture_native_truncate;
	fixture_native_truncate = true;
	XactCtl->sync_handler = SYNC_HANDLER_NONE;
	fixture_variable_cache.oldestClogXid = FirstNormalTransactionId;
	XactCtl->shared->latest_page_number = 128;
	fixture_zero(0);
	fixture_zero(128);
}

static void
full_truncate_sync_end(void)
{
	fixture_native_sync_hook = NULL;
	fixture_native_lock_hook = NULL;
	fail_unlink = fail_dirsync = false;
	directory_open_error = directory_sync_error = 0;
	fixture_native_truncate = full_truncate_saved_native;
	XactCtl->sync_handler = full_truncate_saved_handler;
	fixture_variable_cache.oldestClogXid = full_truncate_saved_oldest;
	XactCtl->shared->latest_page_number = full_truncate_saved_latest;
	own_full_end();
}

UT_TEST(native_full_truncate_sync_finishes_only_original_delete)
{
	const uint32 epochs[] = {0, 7, UINT32_MAX};

	for (unsigned which = 0; which < lengthof(epochs); which++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		xl_clog_truncate_full cut;
		unsigned before_deleted, before_writes, before_syncs, before_dirsyncs;
		int keep;

		full_truncate_sync_begin(&reader, &decoded, &cut, epochs[which]);
		LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
		keep = SimpleLruReadPage(XactCtl, 128, true, cut.oldestXact);
		UT_ASSERT(clog_recovery_status(XactCtl, keep, cut.oldestXact,
			TRANSACTION_STATUS_ABORTED, CLOG_RECOVERY_WRITE));
		XactCtl->shared->group_lsn[keep * CLOG_LSNS_PER_PAGE] = 0x1234;
		LWLockRelease(XactSLRULock);
		before_deleted = deleted; before_writes = writes;
		before_syncs = syncs; before_dirsyncs = dirsyncs;
		UT_ASSERT(!full_sync_call(&reader, false));
		UT_ASSERT_EQ(full_sync_applies, 1); UT_ASSERT_EQ(full_sync_returns, 1);
		UT_ASSERT(!fixture_exists(0) && fixture_exists(128));
		UT_ASSERT_EQ(deleted, before_deleted + 1);
		UT_ASSERT_EQ(writes, before_writes); UT_ASSERT_EQ(syncs, before_syncs);
		UT_ASSERT_EQ(dirsyncs, before_dirsyncs + 1);
		UT_ASSERT_EQ(fixture_variable_cache.oldestClogXid, cut.oldestXact);
		UT_ASSERT(XactCtl->shared->page_dirty[keep]);
		UT_ASSERT_EQ(XactCtl->shared->group_lsn[keep * CLOG_LSNS_PER_PAGE], 0x1234);
		UT_ASSERT_EQ(fixture_status(cut.oldestXact), TRANSACTION_STATUS_IN_PROGRESS);
		UT_ASSERT_EQ(pg_atomic_read_u32(&ClogRelmapMutations[1].writers), 0);
		full_truncate_sync_end();
	}
}

UT_TEST(native_full_truncate_sync_rejects_incomplete_delete)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		xl_clog_truncate_full cut;
		unsigned before_dirsyncs;

		full_truncate_sync_begin(&reader, &decoded, &cut, 7);
		if (fault == 0) fail_unlink = true;
		else XactCtl->shared->latest_page_number = 0; /* Native wrap guard. */
		if (fault == 2) {
			char path[MAXPGPATH];
			int slot;

			/* Empty disk alone must not hide original native wrap refusal. */
			fixture_path(0, path); UT_ASSERT_EQ(unlink(path), 0);
			LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
			slot = SimpleLruZeroPage(XactCtl, 0);
			UT_ASSERT(XactCtl->shared->page_dirty[slot]);
			LWLockRelease(XactSLRULock);
		}
		before_dirsyncs = dirsyncs;
		UT_ASSERT(full_sync_call(&reader, false));
		UT_ASSERT_EQ(full_sync_returns, 0); UT_ASSERT_EQ(full_sync_applies, 1);
		UT_ASSERT_EQ(fixture_exists(0), fault != 2);
		UT_ASSERT_EQ(dirsyncs, before_dirsyncs);
		UT_ASSERT_EQ(pg_atomic_read_u32(&ClogRelmapMutations[1].writers), 0);
		full_truncate_sync_end();
	}
}

UT_TEST(native_full_truncate_sync_errors_keep_original_native_effects)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		xl_clog_truncate_full cut;

		full_truncate_sync_begin(&reader, &decoded, &cut, 7);
		if (fault == 0) fail_dirsync = true;
		if (fault == 1) directory_sync_error = EIO;
		if (fault == 2) directory_open_error = EIO;
		if (fault >= 3 && fault <= 6) full_sync_drift_kind = fault - 2;
		if (fault == 7) full_sync_lock_error = XactTruncationLock;
		if (fault == 8) full_sync_lock_error = XactSLRULock;
		UT_ASSERT(full_sync_call(&reader, false));
		UT_ASSERT_EQ(full_sync_returns, 0); UT_ASSERT_EQ(full_sync_applies, 1);
		UT_ASSERT(!fixture_exists(0) && fixture_exists(128));
		UT_ASSERT_EQ(fixture_variable_cache.oldestClogXid, 128 * CLOG_XACTS_PER_PAGE + 17);
		UT_ASSERT_EQ(pg_atomic_read_u32(&ClogRelmapMutations[1].writers), 0);
		full_truncate_sync_end();
	}
}

UT_TEST(native_full_truncate_sync_rejects_invalid_context_before_io)
{
	for (unsigned fault = 0; fault < 19; fault++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		xl_clog_truncate_full cut;
		unsigned before_reads, before_writes, before_syncs, before_deleted;
		int origin = 1;

		full_truncate_sync_begin(&reader, &decoded, &cut, 7);
		if (fault == 0) cut.reserved_zero = 1;
		if (fault == 1) cut.oldest_full_xid++;
		if (fault == 2) cut.pageno++;
		if (fault == 3) cut.oldestXactDb = InvalidOid;
		if (fault == 4) decoded.main_data_len--;
		if (fault == 5) decoded.max_block_id = 0;
		if (fault == 6) reader.EndRecPtr = reader.ReadRecPtr;
		if (fault == 7) enableFsync = false;
		if (fault == 8) authority = false;
		if (fault == 9) MyAuxProcType = CheckpointerProcess;
		if (fault == 10) InRecovery = false;
		if (fault == 11) CritSectionCount = 1;
		if (fault == 12) LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
		if (fault == 13) LWLockAcquire(XactTruncationLock, LW_SHARED);
		if (fault == 14) LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		if (fault == 15) origin = -1;
		if (fault == 16) origin = CLOG_RECOVERY_ORIGINS;
		if (fault == 17) decoded.header.xl_rmid = RM_XACT_ID;
		if (fault == 18) pg_atomic_write_u32(&ClogRelmapMutations[1].writers, 1);
		before_reads = reads; before_writes = writes;
		before_syncs = syncs + dirsyncs; before_deleted = deleted;
		UT_ASSERT(!cluster_clog_native_recovery_full_sync_v1(origin, &reader));
		UT_ASSERT_EQ(reads, before_reads); UT_ASSERT_EQ(writes, before_writes);
		UT_ASSERT_EQ(syncs + dirsyncs, before_syncs); UT_ASSERT_EQ(deleted, before_deleted);
		if (fault == 12) LWLockRelease(XactSLRULock);
		if (fault == 13) LWLockRelease(XactTruncationLock);
		if (fault == 14) LWLockRelease(XidGenLock);
		if (fault == 18) pg_atomic_write_u32(&ClogRelmapMutations[1].writers, 0);
		enableFsync = true; CritSectionCount = 0;
		full_truncate_sync_end();
	}
}

UT_TEST(native_full_truncate_sync_preserves_legacy_and_foreign)
{
	for (unsigned which = 0; which < 2; which++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		xl_clog_truncate_full cut;
		xl_clog_truncate legacy;
		unsigned before_syncs;

		full_truncate_sync_begin(&reader, &decoded, &cut, 7);
		if (which == 0) {
			legacy = (xl_clog_truncate){cut.pageno, cut.oldestXact, cut.oldestXactDb};
			decoded.main_data = (char *)&legacy;
			decoded.main_data_len = sizeof(legacy);
			decoded.header.xl_info = CLOG_TRUNCATE;
		}
		before_syncs = syncs + dirsyncs;
		UT_ASSERT(!full_sync_call(&reader, which == 1));
		UT_ASSERT_EQ(full_sync_returns, 1); UT_ASSERT_EQ(full_sync_applies, 1);
		UT_ASSERT_EQ(syncs + dirsyncs, before_syncs);
		UT_ASSERT_EQ(fixture_exists(0), which == 1);
		full_truncate_sync_end();
	}
}
