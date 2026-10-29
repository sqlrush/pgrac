/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_ctrc_tx_forest_terminals.inc
 *	  Complete original TX forest terminal coverage at final Startup.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cold_ctrc_tx_forest_terminals.inc
 *
 * NOTES
 *	  The actual Startup/Start/canonical-terminal factories execute. Original
 *	  lower census record transport, PAGE files and authority are controlled;
 *	  this does not establish native recovery or lifetime qualification.
 *-------------------------------------------------------------------------
 */
static unsigned forest_terminal_fault;

static void
forest_terminal_step(uint32 index, ClusterRecoveryCensusTxRecordV1 *out)
{
	ClusterRecoveryTxnKey *key = &out->value.transaction;

	if (index == 2) {
		switch (forest_terminal_fault) {
			case 1: key->system_identifier++; break;
			case 2: key->database_incarnation++; break;
			case 3: key->origin_node_id++; break;
			case 4: key->segment_id++; break;
			case 5: key->segment_generation++; break;
			case 6: key->slot_offset++; break;
			case 7: key->slot_wrap++; break;
			case 8: key->xid++; break;
			case 9: out->value.record = NULL; break;
			case 11: owner_boot++; break;
			case 12: observation_manifest.inputs_sha256[31]++; break;
			case 13: pg_re_throw(); break;
		}
	}
	if (index == out->count && forest_terminal_fault == 10) out->observed = false;
}

static void
forest_terminal_ready(uint32 pages, uint32 records)
{
	final_projection_ready(false); final_projection_check = false;
	prefix_count = pages;
	if (pages == 0) ctrc_wait_step = reference_terminals_empty_probe;
	observation_manifest.tx_record_count = records;
	observation_manifest.root_count = records == 0 ? 0 : 2;
	forest_terminal_fault = 0;
	start_tx_record_step = forest_terminal_step;
}

UT_TEST(test_startup_tx_forest_covers_complete_native_records_and_empty_end)
{
	for (uint32 records = 0; records <= 3; records += 3) {
		forest_terminal_ready(1, records);
		retirement_run_startup();
		UT_ASSERT(retirement_service_reached && !info.cluster_tx_start->revoked);
		UT_ASSERT_EQ(prefix_steps, 1);
		UT_ASSERT(pending_storage_live && observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		/* Native physical record generation is independent of its TT key. */
		UT_ASSERT_EQ(native_ctrc_work.entries[0].key.segment_generation, 7);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_tx_forest_nonroot_full_identity_requires_canonical_terminal)
{
	for (unsigned fault = 1; fault <= 8; fault++) {
		forest_terminal_ready(1, 3); forest_terminal_fault = fault;
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT(pending_storage_live && observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(pending_storage_created, 1); UT_ASSERT_EQ(pending_storage_closes, 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		/* Restoring the leaf cannot revive this original refused Start. */
		forest_terminal_fault = 0;
		UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
		retirement_local_cleanup(); UT_ASSERT_EQ(pending_storage_closes, 1);
	}
}

UT_TEST(test_startup_tx_forest_incomplete_record_or_end_refuses_service)
{
	for (unsigned fault = 9; fault <= 10; fault++) {
		forest_terminal_ready(1, 3); forest_terminal_fault = fault;
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		/* The original getter retires its local observation on bad shape;
		 * the original parent inputs/claim are not released or accepted. */
		UT_ASSERT(!observation_scope_live && !observation_cursor_live);
		UT_ASSERT(claim_held && info.cluster_cold_inputs == &typed);
		UT_ASSERT_EQ(claim_releases, 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		UT_ASSERT_EQ(pending_storage_closes, 1);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_tx_forest_late_current_and_error_keep_original_debt)
{
	for (unsigned fault = 11; fault <= 13; fault++) {
		forest_terminal_ready(1, 3); forest_terminal_fault = fault;
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT(!observation_scope_live && !observation_cursor_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		UT_ASSERT(claim_held && info.cluster_cold_inputs == &typed);
		UT_ASSERT_EQ(claim_releases, 0);
		UT_ASSERT_EQ(pending_storage_closes, 1);
		if (fault == 11 && start_tx_record_calls != 0) owner_boot--;
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_tx_forest_zero_page_still_checks_all_native_transactions)
{
	forest_terminal_ready(0, 3); forest_terminal_fault = 8;
	retirement_run_startup();
	UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
	UT_ASSERT_EQ(prefix_steps, 0); UT_ASSERT_EQ(pending_storage_created, 0);
	UT_ASSERT(observation_scope_live && observation_cursor_live && claim_held);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	retirement_local_cleanup();
}
