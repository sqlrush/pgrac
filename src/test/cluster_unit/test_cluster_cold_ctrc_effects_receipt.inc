/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_ctrc_effects_receipt.inc
 *     Original Startup final handoff keeps its pass-2 effects receipt.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_ctrc_effects_receipt.inc
 *
 * NOTES
 *     The real Startup/Start/PAGE drain executes; original PAGE, directory
 *     and effects-readiness transport leaves are controlled. This does not
 *     establish native effects, durability, lifetime or service qualification.
 *-------------------------------------------------------------------------
 */
static unsigned effects_receipt_fault, effects_receipt_probes;
static bool effects_receipt_wait;

static ClusterCtrcLocalEmptyV1
effects_receipt_directory(void)
{
	ClusterColdEffectsReceiptV1 *receipt = &typed.effects_receipt;
	unsigned last = CLUSTER_COLD_MAX_PARTICIPANTS - 1;

	effects_receipt_probes++;
	if (effects_receipt_probes == 1) {
		switch (effects_receipt_fault) {
			case 1: effects_ready = false; break;
			case 2: receipt->plan = (const ClusterColdPlanV1 *)&scope_storage; break;
			case 3: receipt->proof.records_seen[last]++; break;
			case 4: receipt->proof.records_routed++; break;
			case 5: receipt->proof.required_union ^= CLUSTER_COLD_EFFECT_PAGE; break;
			case 6: receipt->proof.routed_union ^= CLUSTER_COLD_EFFECT_PAGE; break;
			case 7: receipt->proof.required_union_by_participant[last] ^= CLUSTER_COLD_EFFECT_PAGE; break;
			case 8: receipt->proof.routed_union_by_participant[last] ^= CLUSTER_COLD_EFFECT_PAGE; break;
			case 9: receipt->proof.invalid = true; break;
			case 10: receipt->result.detail = 99; break;
			case 11: receipt->result.participant++; break;
			case 12: receipt->result.rec_ptr++; break;
			case 13: receipt->result.steps_done++; break;
			case 14: receipt->result.space_steps++; break;
			case 15: receipt->result.space_relations_finished++; break;
			case 16: receipt->result.pages_skipped++; break;
			case 17: receipt->result.pages_applied++; break;
			case 18: receipt->result.own_read++; break;
			case 19: receipt->result.own_end++; break;
			case 20: receipt->result.last_read[last]++; break;
			case 21: receipt->result.last_end[last]++; break;
			case 22: receipt->result.last_crc[last]++; break;
			case 23: pg_re_throw(); break;
		}
	}
	if (effects_receipt_wait && effects_receipt_probes < 3) {
		ctrc_driver_waiting = true;
		return CLUSTER_CTRC_LOCAL_WAIT;
	}
	return CLUSTER_CTRC_LOCAL_EMPTY;
}

static void
effects_receipt_ready(unsigned path)
{
	final_local_empty_ready(path);
	effects_receipt_fault = effects_receipt_probes = 0;
	effects_receipt_wait = false;
	startup_native_directory = effects_receipt_directory;
	/* This original lower readiness leaf is independently controlled; the
	 * complete by-value receipt below remains owned by the original typed. */
	typed.effects_receipt.plan = typed.plan;
}

UT_TEST(test_startup_effects_receipt_keeps_fpi_unchanged_and_zero_page_input)
{
	for (unsigned path = 0; path < 3; path++) {
		ClusterColdEffectsReceiptV1 saved;

		effects_receipt_ready(path); saved = typed.effects_receipt;
		retirement_run_startup();
		UT_ASSERT(retirement_service_reached && !info.cluster_tx_start->revoked);
		UT_ASSERT_EQ(effects_receipt_probes, 1);
		UT_ASSERT(memcmp(&saved, &typed.effects_receipt, sizeof(saved)) == 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_effects_receipt_late_unready_refuses_service_and_retry)
{
	for (unsigned path = 0; path < 3; path++) {
		effects_receipt_ready(path); effects_receipt_fault = 1;
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT(observation_scope_live && observation_cursor_live && claim_held);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		effects_ready = true;
		UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_effects_receipt_complete_value_cannot_change_after_page_checks)
{
	for (unsigned fault = 2; fault <= 22; fault++) {
		ClusterColdEffectsReceiptV1 saved;

		effects_receipt_ready(0); saved = typed.effects_receipt;
		effects_receipt_fault = fault; retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT(pending_storage_live && observation_scope_live && claim_held);
		UT_ASSERT_EQ(pending_storage_created, 2); UT_ASSERT_EQ(pending_storage_closes, 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		typed.effects_receipt = saved;
		UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
		retirement_local_cleanup(); UT_ASSERT_EQ(pending_storage_closes, 2);
	}
}

UT_TEST(test_startup_effects_receipt_local_wait_cannot_rebase_original_input)
{
	for (unsigned path = 0; path < 3; path++) {
		effects_receipt_ready(path); effects_receipt_fault = 22; effects_receipt_wait = true;
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT_EQ(effects_receipt_probes, 1); /* Refuse before waiting on drift. */
		UT_ASSERT(observation_scope_live && observation_cursor_live && claim_held);
		UT_ASSERT_EQ(pending_storage_created, path == 2 ? 0 : 2);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_effects_receipt_error_keeps_original_cleanup_debt)
{
	effects_receipt_ready(0); effects_receipt_fault = 23;
	retirement_run_startup();
	UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
	UT_ASSERT(pending_storage_live && observation_scope_live && observation_cursor_live && claim_held);
	UT_ASSERT_EQ(pending_storage_closes, 0); UT_ASSERT_EQ(claim_releases, 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	retirement_local_cleanup(); UT_ASSERT_EQ(pending_storage_closes, 2);
}
