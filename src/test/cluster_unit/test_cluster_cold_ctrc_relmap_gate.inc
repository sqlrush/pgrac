/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_relmap_gate.inc
 *     Original Start brackets its CTRC observation with relmap staging.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_ctrc_relmap_gate.inc
 * NOTES
 *     Physical recheck is controlled; no terminal or completion grant.
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
bool
cluster_cold_relmap_replay_recheck_v1(const struct ClusterRecoveryStartV1 *start)
{
	UT_ASSERT(start == info.cluster_tx_start);
	UT_ASSERT(ClusterColdRecoveryRelmapCurrent(start));
	relmap_observe_checks++;
	observation_event('M');
	if (relmap_observe_step != NULL) relmap_observe_step();
#ifdef TEST_CLUSTER_STARTUP_RELMAP_TERMINAL
	if (terminal_staging_active) return test_relmap_start_physical_recheck(start);
#endif
#ifdef TEST_CLUSTER_STARTUP_RELMAP_FILES
	if (relmap_files_active) return test_start_unused_relmap_recheck(start);
#endif
	return relmap_observe_allowed;
}

static void
relmap_observation_ready(void)
{
	observation_ready();
	/* Declaration is the real original shared duty. This unit controls only
	 * the complete physical participant's recheck result, never COMPLETE. */
	UT_ASSERT(ClusterColdRecoveryRelmapDeclare(info.cluster_tx_start));
	relmap_observe_allowed = true;
}

static void
relmap_observation_preserved(unsigned destroyed)
{
	UT_ASSERT_EQ(input_destroys, destroyed);
	UT_ASSERT(cluster_recovery_cold_inputs == &typed);
	UT_ASSERT(info.cluster_tx_start != NULL && info.cluster_cold_inputs == &typed);
	UT_ASSERT(claim_held);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
}

UT_TEST(test_ctrc_relmap_gate_preserves_undeclared_path)
{
	observation_ready();
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(NULL, 0));
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 0));
	UT_ASSERT_EQ(relmap_observe_checks, 0);
	UT_ASSERT_EQ(observation_scope_begins, 1);
}

UT_TEST(test_ctrc_relmap_gate_rechecks_every_observation_without_completion)
{
	unsigned destroyed, steps;
	relmap_observation_ready(); destroyed = input_destroys;
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT_EQ(relmap_observe_checks, 2);
	UT_ASSERT(observation_events[0] == 'M');
	UT_ASSERT(observation_events[observation_event_count - 1] == 'M');
	steps = observation_steps;
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 0));
	UT_ASSERT_EQ(relmap_observe_checks, 4);
	UT_ASSERT_EQ(observation_steps, steps);
	UT_ASSERT_EQ(observation_scope_begins, 1);
	UT_ASSERT(cluster_recovery_ctrc_result_collect_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_result.state, CLUSTER_RECOVERY_CTRC_RESULT_PENDING);
	UT_ASSERT(!cluster_recovery_checkpoint_handoff_v1(info.cluster_tx_start));
	relmap_observation_preserved(destroyed);
	/* Successful observation is not a cached staging certificate. */
	relmap_observe_allowed = false;
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 0));
	UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT_EQ(relmap_observe_checks, 5);
	UT_ASSERT_EQ(observation_census_destroys, 1);
	UT_ASSERT_EQ(observation_scope_releases, 1);
	relmap_observation_preserved(destroyed);
}

UT_TEST(test_ctrc_relmap_gate_refuses_before_census_without_retry)
{
	unsigned destroyed;
	relmap_observation_ready(); destroyed = input_destroys;
	relmap_observe_allowed = false;
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT_EQ(relmap_observe_checks, 1);
	UT_ASSERT_EQ(observation_scope_begins + observation_census_begins, 0);
	UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT(!window_region.checkpoint_owner.valid);
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT_EQ(relmap_observe_checks, 1);
	relmap_observation_preserved(destroyed);
}

static void
relmap_observation_refuse_at(void)
{
	if (relmap_observe_checks == relmap_observe_fault_at)
		relmap_observe_allowed = false;
}

UT_TEST(test_ctrc_relmap_gate_late_refusal_retires_only_observation)
{
	unsigned destroyed;
	relmap_observation_ready(); destroyed = input_destroys;
	relmap_observe_fault_at = 2; relmap_observe_step = relmap_observation_refuse_at;
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT_EQ(relmap_observe_checks, 2);
	UT_ASSERT(observation_steps >= 3);
	UT_ASSERT_EQ(observation_census_destroys, 1);
	UT_ASSERT_EQ(observation_scope_releases, 1);
	UT_ASSERT(!observation_cursor_live && !observation_scope_live);
	UT_ASSERT(!info.cluster_tx_start->ctrc_observation_complete);
	UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	relmap_observation_preserved(destroyed);
}

static void
relmap_observation_error_at(void)
{
	if (relmap_observe_checks == relmap_observe_fault_at) pg_re_throw();
}

UT_TEST(test_ctrc_relmap_gate_error_revokes_same_episode)
{
	for (unsigned stage = 1; stage <= 2; stage++) {
		unsigned destroyed;
		volatile bool caught = false;
		relmap_observation_ready(); destroyed = input_destroys;
		relmap_observe_fault_at = stage; relmap_observe_step = relmap_observation_error_at;
		PG_TRY(); { (void)ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
		UT_ASSERT(!window_region.checkpoint_owner.valid);
		UT_ASSERT_EQ(relmap_observe_checks, stage);
		UT_ASSERT_EQ(observation_scope_begins, stage - 1);
		UT_ASSERT_EQ(observation_scope_releases, stage - 1);
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 0));
		UT_ASSERT_EQ(relmap_observe_checks, stage);
		relmap_observation_preserved(destroyed);
	}
}

static void
relmap_observation_owner_drift_at(void)
{
	if (relmap_observe_checks == relmap_observe_fault_at) owner_boot++;
}

UT_TEST(test_ctrc_relmap_gate_provider_cannot_rebind_owner)
{
	for (unsigned stage = 1; stage <= 2; stage++) {
		unsigned destroyed;
		relmap_observation_ready(); destroyed = input_destroys;
		relmap_observe_fault_at = stage; relmap_observe_step = relmap_observation_owner_drift_at;
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		UT_ASSERT_EQ(relmap_observe_checks, stage);
		UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
		UT_ASSERT(!window_region.checkpoint_owner.valid);
		UT_ASSERT_EQ(observation_scope_releases, stage - 1);
		relmap_observation_preserved(destroyed);
	}
}

UT_TEST(test_ctrc_relmap_gate_cannot_replace_original_terminals)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		unsigned destroyed;
		relmap_observation_ready(); destroyed = input_destroys;
		if (fault == 0) window_region.checkpoint_owner.tx_accepted_generation = 0;
		else if (fault == 1) observation_physical[0].slot.status = TT_SLOT_ACTIVE;
		else observation_fault = 3;
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		UT_ASSERT_EQ(relmap_observe_checks, 1);
		UT_ASSERT(!info.cluster_tx_start->ctrc_observation_complete);
		UT_ASSERT(!cluster_recovery_checkpoint_handoff_v1(info.cluster_tx_start));
		relmap_observation_preserved(destroyed);
	}
}

static void
relmap_observation_reenter(void)
{
	unsigned checks = relmap_observe_checks;

	UT_ASSERT(!info.cluster_tx_start->ctrc_observation_complete);
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 0));
	UT_ASSERT_EQ(relmap_observe_checks, checks);
}

UT_TEST(test_ctrc_relmap_gate_withdraws_old_observation_during_recheck)
{
	relmap_observation_ready();
	relmap_observe_step = relmap_observation_reenter;
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT_EQ(relmap_observe_checks, 2);
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 0));
	UT_ASSERT_EQ(relmap_observe_checks, 4);
	UT_ASSERT_EQ(observation_scope_begins, 1);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
}
