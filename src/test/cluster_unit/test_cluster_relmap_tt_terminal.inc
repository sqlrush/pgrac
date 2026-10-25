/*-------------------------------------------------------------------------
 *
 * test_cluster_relmap_tt_terminal.inc
 *	  Original TT terminal owners beside relmap dual-copy cleanup.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_relmap_tt_terminal.inc
 *
 * NOTES
 *	  Test-only composition. TT physical I/O, native outcome/WAL/checkpoint
 *	  context and status/hint admission remain controlled fixture leaves.
 *
 *-------------------------------------------------------------------------
 */

static void
terminal_tt_retained(const ClusterRelmapRetentionDirectory *directory,
	const ClusterRelmapRetentionDirectory *before, const ClusterCanonicalTxnBinding *receipt)
{
	TestRelmapPhysicalStats io_before = test_relmap_physical_stats(), io_after;
	TestRelmapNativeTTStats tt_before = test_relmap_native_tt_stats(), tt_after;

	UT_ASSERT_EQ(cluster_relmap_retention_runtime_tt_release_check_v2(
		directory->system_identifier, directory->database_incarnation, receipt),
		CLUSTER_RELMAP_RETENTION_RETAINED);
	UT_ASSERT(memcmp(before, directory, sizeof(*before)) == 0);
	io_after = test_relmap_physical_stats();
	UT_ASSERT(memcmp(&io_before, &io_after, sizeof(io_before)) == 0);
	tt_after = test_relmap_native_tt_stats();
	UT_ASSERT(memcmp(&tt_before, &tt_after, sizeof(tt_before)) == 0);
	UT_ASSERT(tt_after.balanced && !tt_after.assertion_failed);
}

static ClusterRelmapRetentionDirectory *
terminal_tt_fixture(unsigned which, bool commit, ClusterCanonicalTxnBinding *receipt)
{
	ClusterRelmapRetentionDirectory *directory;
	bool refused;

	cluster_node_id = 0;
	directory = physical_fixture(which >= 3, which % 3 + 1, commit);
	test_relmap_native_tt_reset();
	v2_published_binding_observer = test_relmap_native_tt_lookup;
	(void)GetTopFullTransactionId();
	if (!original_tt_publish())
		return NULL;
	refused = precommit_error(false);
	UT_ASSERT(!refused);
	if (refused)
		return NULL;
	UT_ASSERT(test_relmap_native_tt_lookup(100, receipt));
	return directory;
}

static void
terminal_tt_success(const ClusterCanonicalTxnBinding *receipt,
	const TestRelmapNativeTTTerminal *result, bool commit)
{
	ClusterCanonicalTxnBinding absent;
	TestRelmapNativeTTStats stats = test_relmap_native_tt_stats();

	UT_ASSERT(!test_relmap_native_tt_lookup(100, &absent));
	UT_ASSERT_EQ(stats.bindings, 0);
	UT_ASSERT_EQ(stats.bind_records, 1);
	UT_ASSERT_EQ(stats.ctrc_opens, 1);
	UT_ASSERT(stats.balanced && !stats.assertion_failed);
	UT_ASSERT(result->allocator_present);
	UT_ASSERT_EQ(result->allocator.segment_id, receipt->segment_id);
	UT_ASSERT_EQ(result->allocator.slot_offset, receipt->slot_offset);
	UT_ASSERT_EQ(result->allocator.xid, receipt->xid);
	UT_ASSERT_EQ(result->allocator.wrap, receipt->slot_wrap);
	UT_ASSERT_EQ(result->allocator.status, commit ? CTS_COMMITTED : CTS_ABORTED);
	UT_ASSERT_EQ(result->resident.status, commit ? TT_SLOT_COMMITTED : TT_SLOT_ABORTED);
	UT_ASSERT_EQ(result->resident.xid, receipt->xid);
	UT_ASSERT_EQ(result->resident.wrap, receipt->slot_wrap);
	UT_ASSERT_EQ(result->resident.commit_scn, commit ? scn_encode(1, 42) : InvalidScn);
	UT_ASSERT_EQ(result->allocator.commit_scn, result->resident.commit_scn);
	UT_ASSERT(memcmp(&result->resident, &result->written, sizeof(TTSlot)) == 0);
	UT_ASSERT_EQ(result->writes, 1);
	UT_ASSERT_EQ(result->abort_records, commit ? 0 : 1);
	UT_ASSERT(result->flushed && !result->write_before_flush);
	UT_ASSERT_EQ(result->error_level, 0);
	if (commit)
	{
		UT_ASSERT_EQ(result->fold.segment_id, receipt->segment_id);
		UT_ASSERT_EQ(result->fold.segment_generation, receipt->segment_generation);
		UT_ASSERT_EQ(result->fold.slot_offset, receipt->slot_offset);
		UT_ASSERT_EQ(result->fold.wrap, receipt->slot_wrap);
		UT_ASSERT_EQ(result->fold.xid, receipt->xid);
		UT_ASSERT_EQ(result->fold.instance, receipt->origin_instance);
		UT_ASSERT_EQ(result->fold.commit_scn, scn_encode(1, 42));
		UT_ASSERT_EQ(result->fold.format_version, 1);
		UT_ASSERT_EQ(result->fold.flags + result->fold.reserved, 0);
	}
}

UT_TEST(v2_original_tt_commit_precedes_relmap_postcommit)
{
	for (unsigned which = 0; which < 6; which++)
	{
		ClusterCanonicalTxnBinding receipt;
		TestRelmapNativeTTTerminal result;
		ClusterRelmapRetentionDirectory *directory = terminal_tt_fixture(which, true, &receipt);
		ClusterRelmapRetentionDirectory before;

		if (directory == NULL) { original_tt_done(); continue; }
		before = *directory;
		/* Native RecordTransactionCommit retires the local TT binding first. */
		UT_ASSERT(test_relmap_native_tt_terminal(&receipt, true, false, &result));
		terminal_tt_success(&receipt, &result, true);
		terminal_tt_retained(directory, &before, &receipt);
		physical_finishing = v2_terminal_context = true;
		original_terminal_callback(true);
		physical_clean_pairs();
		UT_ASSERT_EQ(terminal_calls, which % 3 == 2 ? 2 : 1);
		UT_ASSERT(!cluster_relmap_authority_x_held());
		terminal_tt_retained(directory, &before, &receipt);
		original_tt_done();
	}
}

UT_TEST(v2_original_tt_abort_follows_relmap_abort_cleanup)
{
	for (unsigned which = 0; which < 6; which++)
	{
		ClusterCanonicalTxnBinding receipt;
		TestRelmapNativeTTTerminal result;
		ClusterRelmapRetentionDirectory *directory = terminal_tt_fixture(which, false, &receipt);
		ClusterRelmapRetentionDirectory before;

		if (directory == NULL) { original_tt_done(); continue; }
		before = *directory;
		physical_finishing = true;
		/* Native AbortTransaction cleans relmap before RecordTransactionAbort. */
		original_terminal_callback(false);
		physical_clean_pairs();
		UT_ASSERT(!cluster_relmap_authority_x_held());
		UT_ASSERT(test_relmap_native_tt_lookup(100, &receipt));
		terminal_tt_retained(directory, &before, &receipt);
		UT_ASSERT(test_relmap_native_tt_terminal(&receipt, false, false, &result));
		terminal_tt_success(&receipt, &result, false);
		terminal_tt_retained(directory, &before, &receipt);
		original_tt_done();
	}
}

UT_TEST(v2_original_tt_abort_failure_keeps_debt_after_clean_pairs)
{
	for (unsigned which = 0; which < 6; which++)
	{
		ClusterCanonicalTxnBinding receipt, after;
		TestRelmapNativeTTTerminal result;
		ClusterRelmapRetentionDirectory *directory = terminal_tt_fixture(which, false, &receipt);
		ClusterRelmapRetentionDirectory before;

		if (directory == NULL) { original_tt_done(); continue; }
		before = *directory;
		physical_finishing = true;
		original_terminal_callback(false);
		physical_clean_pairs();
		UT_ASSERT(!cluster_relmap_authority_x_held());
		UT_ASSERT(!test_relmap_native_tt_terminal(&receipt, false, true, &result));
		UT_ASSERT_EQ(result.error_level, ERROR);
		UT_ASSERT_EQ(result.abort_records, 1);
		UT_ASSERT_EQ(result.writes, 1);
		UT_ASSERT(result.flushed && !result.write_before_flush);
		UT_ASSERT_EQ(result.resident.status, TT_SLOT_ACTIVE);
		UT_ASSERT_EQ(result.resident.xid, receipt.xid);
		UT_ASSERT_EQ(result.resident.wrap, receipt.slot_wrap);
		UT_ASSERT(result.allocator_present);
		UT_ASSERT_EQ(result.allocator.status, CTS_ACTIVE);
		UT_ASSERT_EQ(test_relmap_native_tt_stats().bindings, 1);
		UT_ASSERT(test_relmap_native_tt_lookup(100, &after));
		UT_ASSERT(memcmp(&receipt, &after, sizeof(receipt)) == 0);
		terminal_tt_retained(directory, &before, &receipt);
		original_tt_done();
	}
}
