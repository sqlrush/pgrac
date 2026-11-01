/*-------------------------------------------------------------------------
 * test_cluster_relmap_redo_control.inc
 *    Original native redo cannot install legacy bytes in managed format.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_redo_control.inc
 * NOTES
 *    Original entry/helper bodies are extracted verbatim. CF/control/path/IO
 *    leaves are controlled; no native durability or replay authority grant.
 *-------------------------------------------------------------------------
 */
static XLogReaderState redo_reader;
static DecodedXLogRecord redo_record;
static char redo_payload[MinSizeOfRelmapUpdate + sizeof(RelMapFile) + 1];

static void
redo_setup(unsigned selected)
{
	xl_relmap_update header = {0};
	setup(selected);
	redo_test = true;
	memset(&redo_reader, 0, sizeof(redo_reader));
	memset(&redo_record, 0, sizeof(redo_record));
	memset(redo_payload, 0, sizeof(redo_payload));
	header.dbid = 200;
	header.tsid = 1663;
	header.nbytes = sizeof(RelMapFile);
	memcpy(redo_payload, &header, MinSizeOfRelmapUpdate);
	memcpy(redo_payload + MinSizeOfRelmapUpdate, &source, sizeof(source));
	redo_record.header.xl_rmid = RM_RELMAP_ID;
	redo_record.header.xl_info = XLOG_RELMAP_UPDATE;
	redo_record.main_data_len = MinSizeOfRelmapUpdate + sizeof(RelMapFile);
	redo_record.main_data = redo_payload;
	redo_record.max_block_id = -1;
	redo_reader.record = &redo_record;
}

static bool
redo_call(void)
{
	volatile bool failed = false;
	PG_TRY();
	{
		relmap_redo(&redo_reader);
	}
	PG_CATCH();
	{
		failed = true;
	}
	PG_END_TRY();
	return !failed;
}

static void
redo_no_effects(void)
{
	UT_ASSERT_EQ(redo_paths + reads + writes + mapping_acquires, 0);
	UT_ASSERT_EQ(x_acquires + x_releases, 0);
	UT_ASSERT_EQ(violations, 0);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
}

UT_TEST(redo_managed_and_unknown_refuse_before_effect)
{
	for (unsigned selected = 0; selected <= 5; selected++) {
		if (selected == 3) continue;
		redo_setup(selected);
		UT_ASSERT(!redo_call());
		redo_no_effects();
		UT_ASSERT_EQ(error_level, PANIC);
		UT_ASSERT_EQ(formats, 1);
		UT_ASSERT(!cf_held && !mapping_held);
		UT_ASSERT_EQ(cf_acquires, 1);
		UT_ASSERT_EQ(cf_releases, 1);
	}
}

UT_TEST(redo_exact_legacy_preserves_original_route)
{
	for (unsigned inherited = 0; inherited < 3; inherited++) {
		redo_setup(3);
		cf_held = inherited != 0;
		cf_mode = inherited == 2 ? ExclusiveLock : ShareLock;
		UT_ASSERT(redo_call());
		UT_ASSERT_EQ(reads, 0);
		UT_ASSERT_EQ(redo_paths, 1);
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(formats, 2);
		UT_ASSERT_EQ(write_db, 200);
		UT_ASSERT_EQ(write_ts, 1663);
		UT_ASSERT(memcmp(&source, &destination, sizeof(source)) == 0);
		UT_ASSERT_EQ(cf_acquires, inherited == 0);
		UT_ASSERT_EQ(cf_releases, inherited == 0);
		UT_ASSERT_EQ(cf_held, inherited != 0);
		UT_ASSERT_EQ(mapping_acquires, 1);
		UT_ASSERT_EQ(mapping_releases, 1);
		UT_ASSERT_EQ(x_acquires + x_releases + violations, 0);
		UT_ASSERT(!mapping_held);
	}
}

UT_TEST(redo_control_refusals_and_errors_keep_duties)
{
	for (unsigned i = 2; i <= 10; i++) {
		if (i > 4 && i < 10) continue;
		redo_setup(3);
		fault = i;
		x_held = true; /* unrelated original protection is never retired here */
		UT_ASSERT(!redo_call());
		redo_no_effects();
		UT_ASSERT(x_held && !mapping_held && !cf_held);
		UT_ASSERT_EQ(cf_releases, i == 4 || i == 10);
		UT_ASSERT_EQ(error_level, i == 3 || i == 4 ? ERROR : PANIC);
	}
}

UT_TEST(redo_late_drift_and_unconfirmed_release_are_fail_stop)
{
	for (unsigned i = 5; i <= 8; i += 3) {
		redo_setup(3);
		fault = i;
		UT_ASSERT(!redo_call());
		UT_ASSERT_EQ(error_level, PANIC);
		UT_ASSERT_EQ(writes, 1); /* retain effects; never rollback/reexecute */
		UT_ASSERT_EQ(mapping_acquires, 1);
		UT_ASSERT_EQ(cf_acquires, 1);
		UT_ASSERT_EQ(cf_releases, 1);
		UT_ASSERT(!cf_held && !mapping_held);
		UT_ASSERT_EQ(x_releases + violations + InterruptHoldoffCount, 0);
		UT_ASSERT(memcmp(&source, &destination, sizeof(source)) == 0);
	}
}

UT_TEST(redo_error_unwinds_only_owned_locks)
{
	for (unsigned i = 0; i < 6; i++)
		for (unsigned inherited = 0; inherited < 2; inherited++) {
			redo_setup(3);
			fault = i == 0 ? 7 : i == 1 ? 11 : i == 2 ? 9 : i == 3 ? 4 : i == 4 ? 12 : 13;
			cf_held = inherited != 0;
			if (fault == 9 && inherited) continue; /* borrowed CF is never released */
			x_held = true;
			UT_ASSERT(!redo_call());
			UT_ASSERT_EQ(error_level, ERROR);
			UT_ASSERT(x_held && !mapping_held && cf_held == (inherited != 0));
			UT_ASSERT_EQ(cf_releases, !inherited);
			UT_ASSERT_EQ(InterruptHoldoffCount, 0);
			UT_ASSERT_EQ(violations, 0);
			UT_ASSERT_EQ(x_releases, 0);
		}
}

UT_TEST(redo_full_record_shape_precedes_control_and_io)
{
	for (unsigned i = 0; i < 8; i++) {
		uint32 invalid = 1;
		redo_setup(3);
		if (i == 0) redo_record.main_data_len = 0;
		if (i == 1) redo_record.main_data_len = MinSizeOfRelmapUpdate - 1;
		if (i == 2) redo_record.main_data_len--;
		if (i == 3) redo_record.main_data_len++;
		if (i == 4) memcpy(redo_payload + offsetof(xl_relmap_update, nbytes), &invalid, sizeof(invalid));
		if (i == 5) redo_record.max_block_id = 0;
		if (i == 6) redo_record.header.xl_info = XLOG_RELMAP_UPDATE_V2;
		if (i == 7) redo_record.main_data = NULL;
		UT_ASSERT(!redo_call());
		redo_no_effects();
		UT_ASSERT_EQ(formats + cf_acquires + cf_releases, 0);
		UT_ASSERT_EQ(error_level, PANIC);
	}
}

UT_TEST(redo_inverted_locks_are_not_released)
{
	for (unsigned i = 0; i < 4; i++) {
		redo_setup(3);
		mapping_held = i == 0;
		xidgen_held = i == 1;
		control_held = i == 2;
		CritSectionCount = i == 3;
		UT_ASSERT(!redo_call());
		redo_no_effects();
		UT_ASSERT_EQ(mapping_held, i == 0);
		UT_ASSERT_EQ(xidgen_held, i == 1);
		UT_ASSERT_EQ(control_held, i == 2);
		UT_ASSERT_EQ(CritSectionCount, i == 3);
		UT_ASSERT_EQ(cf_acquires + cf_releases + mapping_releases, 0);
	}
}

UT_TEST(redo_catalog_toggle_cannot_bypass_managed_format)
{
	redo_setup(4);
	cluster_shared_catalog = false;
	UT_ASSERT(!redo_call());
	redo_no_effects();
	redo_setup(4);
	cluster_shared_config = false;
	UT_ASSERT(redo_call()); /* unchanged ordinary non-shared-config route */
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(formats + cf_acquires + cf_releases + violations, 0);
}
