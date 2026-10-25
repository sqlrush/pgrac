/* A released/replaced recovery claim cannot serialize the remaining service.
 * Exercise the original EOR/finish boundaries and original accepted results. */
static void claim_service_accept_on_third_poll(void)
{
	UT_ASSERT(claim_held);
	UT_ASSERT_EQ(claim_arbitrations, 0);
	UT_ASSERT_EQ(claim_releases, 0);
	ctrc_handoff_accept_on_third_poll();
}

UT_TEST(test_original_recovered_claim_spans_eor_pending_service_and_handoff)
{
	reset(); retain();
	actual_startup_eor_claim(&info);
	UT_ASSERT(claim_held);
	UT_ASSERT_EQ(claim_arbitrations, 0);
	UT_ASSERT_EQ(claim_releases, 0);
	if (!claim_held) return;
	UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	serving_actor(); UT_ASSERT(cluster_recovery_tx_result_accept_v1()); startup_actor();
	tx_wait_destroy_base = input_destroys;
	ctrc_wait_step = claim_service_accept_on_third_poll;
	actual_startup_finish_tx(&info);
	UT_ASSERT_EQ(tx_waits, 3);
	UT_ASSERT(!claim_held);
	UT_ASSERT_EQ(claim_arbitrations, 1);
	UT_ASSERT_EQ(claim_releases, 1);
	UT_ASSERT_EQ(claim_destroys_at_release, tx_wait_destroy_base + 1);
	UT_ASSERT(info.cluster_tx_start == NULL && info.cluster_cold_inputs == NULL);
}

UT_TEST(test_claim_eor_preserves_original_clean_no_attempt_behavior)
{
	reset();
	actual_startup_eor_claim(&info);
	UT_ASSERT(!claim_held);
	UT_ASSERT_EQ(claim_arbitrations, 1);
	UT_ASSERT_EQ(claim_releases, 1);
	UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT(info.cluster_tx_start == NULL);
}

UT_TEST(test_start_requires_the_original_live_claim_incarnation)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		reset(); retain();
		if (fault < 2) {
			if (fault == 0) claim_held = false;
			else claim_read_ok = false;
			UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
			UT_ASSERT(info.cluster_tx_start == NULL);
		} else {
			UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
			if (fault == 2) claim_held = false;
			else claim_serial++;
			UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
		}
		UT_ASSERT_EQ(claim_releases, 0);
	}
}

static void claim_service_revoke_after_many_polls(void)
{
	UT_ASSERT(claim_held);
	UT_ASSERT_EQ(claim_arbitrations, 0);
	UT_ASSERT_EQ(claim_releases, 0);
	if (tx_waits == 129) owner_boot++;
}

UT_TEST(test_failed_pending_ctrc_keeps_original_claim_and_inputs)
{
	ctrc_handoff_fixture(false, false);
	ctrc_wait_step = claim_service_revoke_after_many_polls;
	fatal_armed = true;
	if (setjmp(fatal_jump) == 0) { actual_startup_finish_tx(&info); UT_ASSERT(false); }
	fatal_armed = false;
	UT_ASSERT_EQ(tx_waits, 129);
	UT_ASSERT(claim_held);
	UT_ASSERT_EQ(claim_arbitrations, 0);
	UT_ASSERT_EQ(claim_releases, 0);
	UT_ASSERT_EQ(input_destroys, tx_wait_destroy_base);
	UT_ASSERT(info.cluster_tx_start != NULL && info.cluster_cold_inputs != NULL);
}

UT_TEST(test_final_claim_validation_or_arbitration_failure_retains_inputs)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ctrc_handoff_fixture(false, true);
		if (fault == 0) claim_read_ok = false;
		else claim_arbitrate_error = true;
		fatal_armed = true;
		if (setjmp(fatal_jump) == 0) { actual_startup_finish_tx(&info); UT_ASSERT(false); }
		fatal_armed = false;
		UT_ASSERT(claim_held);
		UT_ASSERT_EQ(claim_releases, 0);
		UT_ASSERT_EQ(input_destroys, tx_wait_destroy_base);
		UT_ASSERT(info.cluster_tx_start != NULL && info.cluster_cold_inputs != NULL);
	}
}

UT_TEST(test_claim_release_failure_never_returns_startup_success)
{
	ctrc_handoff_fixture(false, true);
	claim_release_error = true;
	fatal_armed = true;
	if (setjmp(fatal_jump) == 0) { actual_startup_finish_tx(&info); UT_ASSERT(false); }
	fatal_armed = false;
	UT_ASSERT(claim_held);
	UT_ASSERT_EQ(claim_releases, 0);
	UT_ASSERT_EQ(claim_arbitrations, 1);
	UT_ASSERT_EQ(input_destroys, tx_wait_destroy_base + 1);
	UT_ASSERT(info.cluster_tx_start == NULL && info.cluster_cold_inputs == NULL);
}
