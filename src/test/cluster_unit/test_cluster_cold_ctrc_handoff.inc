/* Original Startup boundary and original LMON acceptance are real. Native
 * writer, physical input loader and latch scheduling are fixture leaves. */
static void
ctrc_handoff_accept_on_third_poll(void)
{
	UT_ASSERT_EQ(input_destroys, tx_wait_destroy_base);
	UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT(window_region.checkpoint_owner.tx_accepted_generation != 0);
	if (tx_waits == 3) {
		serving_actor();
		UT_ASSERT(cluster_recovery_ctrc_result_accept_v1());
		startup_actor();
	}
}

UT_TEST(test_startup_retains_inputs_until_independent_ctrc_acceptance)
{
	reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	tx_wait_destroy_base = input_destroys;
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	/* Original pass-2 route coverage precedes this empty service handoff. */
	effects_ready = true;
	serving_actor(); UT_ASSERT(cluster_recovery_tx_result_accept_v1()); startup_actor();
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	ctrc_wait_step = ctrc_handoff_accept_on_third_poll;
	actual_startup_finish_tx(&info);
	UT_ASSERT_EQ(tx_waits, 3);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation,
		window_region.checkpoint_owner.ctrc_result.result_generation);
	UT_ASSERT_EQ(input_destroys, tx_wait_destroy_base + 1);
	UT_ASSERT(info.cluster_tx_start == NULL && info.cluster_cold_inputs == NULL);
}

static void
ctrc_handoff_fixture(bool pending, bool accepted)
{
	reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	tx_wait_destroy_base = input_destroys;
	if (pending) ctrc_census.subject_count = ctrc_census.unresolved_count = 1;
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	/* This handoff follows the original completed pass-2 route leaf. */
	effects_ready = true;
	serving_actor(); UT_ASSERT(cluster_recovery_tx_result_accept_v1());
	if (accepted) UT_ASSERT(cluster_recovery_ctrc_result_accept_v1());
	startup_actor();
}

static void
ctrc_handoff_revoke_after_many_polls(void)
{
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_result.state,
		CLUSTER_RECOVERY_CTRC_RESULT_PENDING);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	if (tx_waits == 129) owner_boot++;
}

UT_TEST(test_ctrc_handoff_pending_timeouts_keep_original_input_and_responsibility)
{
	ClusterRecoveryCtrcResultValueV1 pending;
	ctrc_handoff_fixture(true, false);
	pending = window_region.checkpoint_owner.ctrc_result;
	ctrc_wait_step = ctrc_handoff_revoke_after_many_polls;
	UT_ASSERT(!cluster_recovery_ctrc_result_wait_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(tx_waits, 129);
	UT_ASSERT(tx_interrupts >= tx_waits);
	UT_ASSERT_EQ(input_destroys, tx_wait_destroy_base);
	UT_ASSERT(info.cluster_tx_start != NULL && info.cluster_cold_inputs != NULL);
	UT_ASSERT(memcmp(&pending, &window_region.checkpoint_owner.ctrc_result, sizeof(pending)) == 0);
	UT_ASSERT_EQ(native_ctrc_work.counts.unresolved_count, 1);
	release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_ctrc_handoff_refuses_wrong_original_components_without_releasing_input)
{
	UT_ASSERT(cluster_recovery_ctrc_result_wait_v1(NULL));
	for (unsigned fault = 0; fault < 19; fault++) {
		ClusterRecoveryCtrcResultValueV1 before;
		ctrc_handoff_fixture(false, true);
		switch (fault) {
		case 0: window_region.checkpoint_owner.valid = false; break;
		case 1: window_region.checkpoint_owner.owner.start_serial++; break;
		case 2: window_region.checkpoint_owner.tx_accepted_generation = 0; break;
		case 3: window_region.checkpoint_owner.tx_result.input_root_sha256[0]++; break;
		case 4: window_region.checkpoint_owner.tx_result.completed_count++; break;
		case 5: window_region.checkpoint_owner.ctrc_result.input_root_sha256[0]++; break;
		case 6: window_region.checkpoint_owner.ctrc_result.subject_count++; break;
		case 7: window_region.checkpoint_owner.ctrc_result.released_count++; break;
		case 8: window_region.checkpoint_owner.ctrc_result.unresolved_count++; break;
		case 9: window_region.checkpoint_owner.ctrc_result.unqualified_count = 1; break;
		case 10: window_region.checkpoint_owner.ctrc_accepted_generation++; break;
		case 11: window_region.checkpoint_owner.ctrc_result.result_generation = 0; break;
		case 12: window_region.checkpoint_owner.ctrc_result.state = CLUSTER_RECOVERY_CTRC_RESULT_EMPTY; break;
		case 13: window_region.checkpoint_owner.tx_result.state = CLUSTER_RECOVERY_TX_RESULT_PENDING; break;
		case 14: checkpoint_epoch++; break;
		case 15: native_recovery = true; break;
		case 16: typed.ctrc_work = NULL; break;
		case 17: typed.census_root_captured = false; break;
		case 18: CurrentResourceOwner = NULL; break;
		}
		before = window_region.checkpoint_owner.ctrc_result;
		UT_ASSERT(!cluster_recovery_ctrc_result_wait_v1(info.cluster_tx_start));
		UT_ASSERT_EQ(tx_waits, 0);
		UT_ASSERT_EQ(input_destroys, tx_wait_destroy_base);
		UT_ASSERT(info.cluster_tx_start != NULL && info.cluster_cold_inputs != NULL);
		UT_ASSERT(memcmp(&before, &window_region.checkpoint_owner.ctrc_result, sizeof(before)) == 0);
		CurrentResourceOwner = (ResourceOwner)&info;
		release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		ClusterReleaseColdRecoveryInputs(&info);
	}
}

static unsigned ctrc_handoff_interrupt_fault;
static void
ctrc_handoff_change_at_interrupt(void)
{
	switch (ctrc_handoff_interrupt_fault) {
	case 0: owner_boot++; break;
	case 1: typed.ctrc_work_generation++; break;
	case 2: typed.census_root.file_txn_seq++; break;
	case 3: CurrentResourceOwner = NULL; break;
	case 4: window_region.checkpoint_owner.generation++; break;
	case 5: window_region.checkpoint_owner.tx_result.result_generation++; break;
	case 6: window_region.checkpoint_owner.ctrc_result.input_root_sha256[1]++; break;
	case 7: checkpoint_epoch++; break;
	}
}

UT_TEST(test_ctrc_handoff_revalidates_after_startup_interrupts)
{
	for (ctrc_handoff_interrupt_fault = 0; ctrc_handoff_interrupt_fault < 8;
		ctrc_handoff_interrupt_fault++) {
		ctrc_handoff_fixture(false, true);
		ctrc_interrupt_step = ctrc_handoff_change_at_interrupt;
		UT_ASSERT(!cluster_recovery_ctrc_result_wait_v1(info.cluster_tx_start));
		UT_ASSERT_EQ(tx_waits, 0);
		UT_ASSERT_EQ(tx_interrupts, 1);
		UT_ASSERT_EQ(input_destroys, tx_wait_destroy_base);
		UT_ASSERT(info.cluster_tx_start != NULL && info.cluster_cold_inputs != NULL);
		CurrentResourceOwner = (ResourceOwner)&info;
		release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		ClusterReleaseColdRecoveryInputs(&info);
	}
}

static void
ctrc_handoff_error_at_poll(void)
{
	if (tx_waits == 3) pg_re_throw();
}

UT_TEST(test_ctrc_handoff_propagates_error_without_private_input_release)
{
	volatile bool caught = false;
	ClusterRecoveryCtrcResultValueV1 pending;
	ctrc_handoff_fixture(true, false);
	pending = window_region.checkpoint_owner.ctrc_result;
	ctrc_wait_step = ctrc_handoff_error_at_poll;
	PG_TRY();
	{
		(void)cluster_recovery_ctrc_result_wait_v1(info.cluster_tx_start);
		UT_ASSERT(false);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(tx_waits, 3);
	UT_ASSERT_EQ(input_destroys, tx_wait_destroy_base);
	UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT(memcmp(&pending, &window_region.checkpoint_owner.ctrc_result, sizeof(pending)) == 0);
	release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(!window_region.checkpoint_owner.valid);
	ClusterReleaseColdRecoveryInputs(&info);
}
