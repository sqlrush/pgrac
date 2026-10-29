/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_effects_lifetime.inc
 *     One original Startup keeps its effects input across yielding calls.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Original Startup/Start/PAGE/checkpoint consumers execute. Native PAGE,
 * effects readiness and component publication remain controlled leaves.
 * This does not establish native durability or joint completion authority.
 *-------------------------------------------------------------------------
 */
static unsigned effects_lifetime_fault;

static void
effects_lifetime_change(void)
{
	unsigned last = CLUSTER_COLD_MAX_PARTICIPANTS - 1;

	switch (effects_lifetime_fault) {
		case 1: effects_ready = false; break;
		case 2: typed.effects_receipt.proof.records_seen[last]++; break;
		case 3: typed.effects_receipt.result.last_crc[last]++; break;
	}
}

static void
effects_lifetime_publish(void)
{
	post_service_publish();
	effects_lifetime_change();
}

UT_TEST(test_startup_effects_lifetime_stable_input_survives_complete_handoff)
{
	for (unsigned path = 0; path < 3; path++) {
		ClusterColdEffectsReceiptV1 saved;

		post_service_ready(path, 0);
		memcpy(&saved, &typed.effects_receipt, sizeof(saved));
		UT_ASSERT(ClusterColdRecoveryTxEffectsCurrent(info.cluster_tx_start));
		retirement_run_startup();
		UT_ASSERT(post_service_polled);
		UT_ASSERT(info.cluster_tx_start == NULL && info.cluster_cold_inputs == NULL);
		UT_ASSERT_EQ(claim_arbitrations, 1); UT_ASSERT_EQ(claim_releases, 1);
		UT_ASSERT_EQ(pending_storage_closes, path == 2 ? 0 : 2);
		UT_ASSERT(memcmp(&saved, &typed.effects_receipt, sizeof(saved)) == 0);
		post_service_done();
	}
}

UT_TEST(test_startup_effects_lifetime_first_validation_cannot_rebase_before_page)
{
	for (unsigned fault = 1; fault <= 3; fault++) {
		ClusterColdEffectsReceiptV1 saved;

		effects_receipt_ready(0);
		memcpy(&saved, &typed.effects_receipt, sizeof(saved));
		UT_ASSERT(ClusterColdRecoveryTxEffectsCurrent(info.cluster_tx_start));
		effects_lifetime_fault = fault; effects_lifetime_change();
		UT_ASSERT(!ClusterColdRecoveryTxEffectsCurrent(info.cluster_tx_start));
		UT_ASSERT(info.cluster_tx_start->revoked);
		effects_ready = true; memcpy(&typed.effects_receipt, &saved, sizeof(saved));
		UT_ASSERT(!ClusterColdRecoveryTxEffectsCurrent(info.cluster_tx_start));
		UT_ASSERT_EQ(prefix_steps, 0); UT_ASSERT_EQ(pending_storage_closes, 0);
		UT_ASSERT(info.cluster_cold_inputs != NULL && claim_held);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_effects_lifetime_service_wait_cannot_replace_input)
{
	for (unsigned path = 0; path < 3; path++)
		for (unsigned fault = 1; fault <= 3; fault++) {
			ClusterColdEffectsReceiptV1 saved;

			post_service_ready(path, 0);
			memcpy(&saved, &typed.effects_receipt, sizeof(saved));
			effects_lifetime_fault = fault; ctrc_wait_step = effects_lifetime_publish;
			retirement_run_startup();
			UT_ASSERT(post_service_polled && info.cluster_tx_start != NULL);
			if (info.cluster_tx_start != NULL) {
				UT_ASSERT(info.cluster_tx_start->revoked);
				effects_ready = true; memcpy(&typed.effects_receipt, &saved, sizeof(saved));
				UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
				post_service_retained();
			} else {
				UT_ASSERT_EQ(claim_arbitrations, 0); UT_ASSERT_EQ(claim_releases, 0);
				post_service_done();
			}
		}
}

UT_TEST(test_startup_effects_lifetime_repeated_page_call_keeps_original_input)
{
	for (unsigned path = 0; path < 3; path++) {
		effects_receipt_ready(path); retirement_run_startup();
		UT_ASSERT(retirement_service_reached && !info.cluster_tx_start->revoked);
		effects_lifetime_fault = 2; effects_lifetime_change();
		UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
		UT_ASSERT(info.cluster_tx_start->revoked);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		UT_ASSERT(observation_scope_live && observation_cursor_live && claim_held);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_effects_lifetime_final_owner_cannot_accept_changed_input)
{
	ClusterRecoveryCheckpointOwnerV1 owner, zero;
	ClusterColdEffectsReceiptV1 saved;

	effects_receipt_ready(0); retirement_run_startup();
	UT_ASSERT(retirement_service_reached && !info.cluster_tx_start->revoked);
	memcpy(&saved, &typed.effects_receipt, sizeof(saved));
	effects_lifetime_fault = 3; effects_lifetime_change();
	memset(&owner, 0x5a, sizeof(owner)); memset(&zero, 0, sizeof(zero));
	UT_ASSERT(!ClusterColdRecoveryTxCheckpointOwnerV1(info.cluster_tx_start, &owner));
	UT_ASSERT(memcmp(&owner, &zero, sizeof(owner)) == 0);
	memcpy(&typed.effects_receipt, &saved, sizeof(saved));
	UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT_EQ(pending_storage_closes, 0); UT_ASSERT_EQ(claim_releases, 0);
	UT_ASSERT(observation_scope_live && observation_cursor_live && claim_held);
	retirement_local_cleanup();
}
