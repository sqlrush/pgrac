/*-------------------------------------------------------------------------
 * test_cluster_clog_native_record_owner.inc
 *     Native FULL finish uses its current record, independently of cold scope.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_native_record_owner.inc
 * NOTES
 *     Original redo, SLRU and disk sync execute; the record-owner leaf here
 *     is controlled and is separately tested with the original Startup owner.
 *-------------------------------------------------------------------------
 */
static bool native_record_allowed;
static bool native_record_cold_denied(int origin) { return false; }
static bool
native_record_current(int origin, const XLogReaderState *record)
{
	UT_ASSERT_EQ(origin, 1);
	UT_ASSERT(record != NULL && record->record != NULL);
	return native_record_allowed;
}
static void
native_record_revoke(void)
{
	fixture_native_sync_hook = NULL;
	native_record_allowed = false;
}

UT_TEST(native_full_finish_uses_native_record_without_cold_scope)
{
	for (unsigned truncate = 0; truncate < 2; truncate++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		xl_clog_zeropage_full birth;
		xl_clog_truncate_full cut;

		if (truncate) full_truncate_sync_begin(&reader, &decoded, &cut, 7);
		else { full_sync_begin(); full_sync_reader(&reader, &decoded, &birth, 128); }
		fixture_cold_origin = native_record_cold_denied;
		fixture_native_record = native_record_current;
		native_record_allowed = true;
		UT_ASSERT(!cluster_undo_recovery_cold_origin_authorized_v1(1));
		UT_ASSERT(!full_sync_call(&reader, false));
		UT_ASSERT_EQ(full_sync_applies, 1);
		UT_ASSERT_EQ(full_sync_returns, 1);
		UT_ASSERT_EQ(transient_fds, 0);
		fixture_cold_origin = NULL;
		fixture_native_record = NULL;
		if (truncate) full_truncate_sync_end(); else full_sync_end();
	}
}

UT_TEST(native_full_finish_cannot_borrow_cold_scope)
{
	for (unsigned truncate = 0; truncate < 2; truncate++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		xl_clog_zeropage_full birth;
		xl_clog_truncate_full cut;
		unsigned before_reads, before_syncs, before_writes, before_deleted;

		if (truncate) full_truncate_sync_begin(&reader, &decoded, &cut, 7);
		else { full_sync_begin(); full_sync_reader(&reader, &decoded, &birth, 128); }
		clog_redo(&reader);
		fixture_native_record = native_record_current;
		native_record_allowed = false;
		UT_ASSERT(cluster_undo_recovery_cold_origin_authorized_v1(1));
		before_reads = reads; before_syncs = syncs + dirsyncs;
		before_writes = writes; before_deleted = deleted;
		UT_ASSERT(!cluster_clog_native_recovery_full_sync_v1(1, &reader));
		UT_ASSERT_EQ(reads, before_reads); UT_ASSERT_EQ(syncs + dirsyncs, before_syncs);
		UT_ASSERT_EQ(writes, before_writes); UT_ASSERT_EQ(deleted, before_deleted);
		fixture_native_record = NULL;
		if (truncate) full_truncate_sync_end(); else full_sync_end();
	}
}

UT_TEST(native_full_finish_rechecks_record_owner_after_sync)
{
	for (unsigned truncate = 0; truncate < 2; truncate++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		xl_clog_zeropage_full birth;
		xl_clog_truncate_full cut;
		unsigned before_syncs;

		if (truncate) full_truncate_sync_begin(&reader, &decoded, &cut, 7);
		else { full_sync_begin(); full_sync_reader(&reader, &decoded, &birth, 128); }
		clog_redo(&reader);
		fixture_native_record = native_record_current;
		native_record_allowed = true;
		fixture_native_sync_hook = native_record_revoke;
		before_syncs = syncs + dirsyncs;
		UT_ASSERT(!cluster_clog_native_recovery_full_sync_v1(1, &reader));
		UT_ASSERT(syncs + dirsyncs > before_syncs);
		UT_ASSERT(!native_record_allowed);
		UT_ASSERT_EQ(transient_fds, 0);
		fixture_native_record = NULL;
		if (truncate) full_truncate_sync_end(); else full_sync_end();
	}
}
