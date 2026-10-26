/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_ctrc_replay_driver.inc
 *     Original Startup integration with native CTRC replay owners.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_ctrc_replay_driver.inc
 *
 * NOTES
 *     Original Start, observation, canonical terminal and actual Startup
 *     boundary; native all-replay acquisition, physical I/O and scheduling
 *     are controlled leaves. The separate native input suite exercises the
 *     real IR set.
 *
 *-------------------------------------------------------------------------
 */
void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *arg)
{
	static LWLock lock;
	for (unsigned i = 0; i < ctrc_driver_lwlocks; i++) callback(&lock, LW_EXCLUSIVE, arg);
}

ClusterControlRootResult
cluster_wal_inputs_ctrc_replays_acquire_v1(ClusterWalInputsV1 *inputs)
{
	UT_ASSERT(inputs == (ClusterWalInputsV1 *)&observation_scope_object);
	UT_ASSERT(observation_cursor_live && observation_scope_live);
	UT_ASSERT(cluster_recovery_cold_start->ctrc_observation_complete);
	UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
	ctrc_driver_acquires++;
	if (ctrc_driver_error) pg_re_throw();
	if (ctrc_driver_acquire_step != NULL) ctrc_driver_acquire_step();
	if (ctrc_driver_acquires <= ctrc_driver_busy_until) {
		ctrc_driver_waiting = true;
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	}
	return ctrc_driver_result;
}

bool
cluster_wal_inputs_ctrc_replays_authorized_v1(ClusterWalInputsV1 *inputs)
{
	/* A retired borrowed scope must never reach this callback. */
	UT_ASSERT(inputs == (ClusterWalInputsV1 *)&observation_scope_object);
	UT_ASSERT(observation_cursor_live && observation_scope_live);
	UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
	ctrc_driver_authorizations++;
	if (ctrc_driver_authorize_step != NULL) ctrc_driver_authorize_step();
	return !ctrc_driver_false_authorization;
}

static void
ctrc_driver_service_wait(void)
{
	UT_ASSERT_EQ(ctrc_driver_acquires, 1);
	UT_ASSERT(ctrc_driver_authorizations >= 2);
	UT_ASSERT_EQ(observation_scope_begins, 1);
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	ctrc_handoff_accept_on_third_poll();
}

UT_TEST(test_startup_acquires_native_replay_set_before_independent_service)
{
	ctrc_handoff_fixture(false, false);
	ctrc_wait_step = ctrc_driver_service_wait;
	actual_startup_finish_tx(&info);
	UT_ASSERT_EQ(ctrc_driver_acquires, 1);
	UT_ASSERT_EQ(ctrc_driver_waits, 0);
	UT_ASSERT_EQ(tx_waits, 3);
	UT_ASSERT_EQ(observation_scope_releases, 1);
	UT_ASSERT_EQ(observation_census_destroys, 1);
	UT_ASSERT(info.cluster_tx_start == NULL && info.cluster_cold_inputs == NULL);
}

UT_TEST(test_startup_replay_acquisition_requires_original_complete_observation)
{
	reset();
	UT_ASSERT(ClusterColdRecoveryCtrcReplaysAcquire(NULL));
	UT_ASSERT_EQ(ctrc_driver_acquires, 0);
	for (unsigned fault = 0; fault < 11; fault++) {
		const struct ClusterRecoveryStartV1 *start;
		ctrc_replay_request_fixture();
		start = info.cluster_tx_start;
		switch (fault) {
		case 0: start = (const struct ClusterRecoveryStartV1 *)&info; break;
		case 1: cluster_recovery_cold_start->ctrc_observation_complete = false; break;
		case 2: window_region.checkpoint_owner.tx_accepted_generation = 0; break;
		case 3: native_ctrc_work.entries[1].slot.xid++; break;
		case 4: typed.census_root.image_sha256[0]++; break;
		case 5: typed.ctrc_work_generation++; break;
		case 6: MyBackendType = B_BACKEND; break;
		case 7: native_recovery = true; break;
		case 8: ShutdownRequestPending = true; break;
		case 9: CritSectionCount++; break;
		case 10: cluster_recovery_cold_start->ctrc_observation_inputs = NULL; break;
		}
		UT_ASSERT(!ClusterColdRecoveryCtrcReplaysAcquire(start));
		UT_ASSERT_EQ(ctrc_driver_acquires, 0);
		UT_ASSERT_EQ(ctrc_driver_authorizations, 0);
		UT_ASSERT_EQ(observation_scope_releases, 0);
		/* Restore only the fixture's locator for original exit cleanup. */
		if (fault == 10) cluster_recovery_cold_start->ctrc_observation_inputs
			= (ClusterWalInputsV1 *)&observation_scope_object;
		CritSectionCount = 0;
	}
}

UT_TEST(test_startup_replay_wait_retains_partial_owners_without_deadline)
{
	ClusterRecoveryCtrcResultValueV1 before;
	unsigned steps, scopes, mutations;
	ctrc_replay_request_fixture();
	before = window_region.checkpoint_owner.ctrc_result;
	steps = observation_steps; scopes = observation_scope_begins;
	mutations = wal_inserts + stamp_writes;
	ctrc_driver_busy_until = 129;
	UT_ASSERT(ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
	UT_ASSERT_EQ(ctrc_driver_acquires, 130);
	UT_ASSERT_EQ(ctrc_driver_waits, 129);
	UT_ASSERT(tx_interrupts >= ctrc_driver_waits);
	UT_ASSERT_EQ(observation_steps, steps); UT_ASSERT_EQ(observation_scope_begins, scopes);
	UT_ASSERT_EQ(observation_scope_releases, 0);
	UT_ASSERT_EQ(observation_census_destroys, 0);
	UT_ASSERT(memcmp(&before, &window_region.checkpoint_owner.ctrc_result, sizeof(before)) == 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	UT_ASSERT_EQ(wal_inserts + stamp_writes, mutations);
}

static unsigned ctrc_driver_fault;
static void
ctrc_driver_drift(void)
{
	switch (ctrc_driver_fault) {
	case 0: owner_boot++; break;
	case 1: native_ctrc_work.entries[1].slot.xid++; break;
	case 2: typed.census_root.image_sha256[31]++; break;
	case 3: window_region.checkpoint_owner.tx_accepted_generation = 0; break;
	case 4: window_region.checkpoint_owner.tx_result.result_generation++; break;
	case 5: cluster_recovery_cold_observation_close(cluster_recovery_cold_start); break;
	case 6: ShutdownRequestPending = true; break;
	case 7: ctrc_driver_lwlocks = 200; break;
	case 8: MyBackendType = B_BACKEND; break;
	case 9: native_recovery = true; break;
	case 10: claim_serial++; break;
	case 11: typed.ctrc_work_generation++; break;
	}
}

UT_TEST(test_startup_replay_rechecks_originals_after_interrupts)
{
	for (unsigned phase = 0; phase < 2; phase++) {
		for (ctrc_driver_fault = 0; ctrc_driver_fault < 12; ctrc_driver_fault++) {
			ctrc_replay_request_fixture();
			if (phase == 0) ctrc_interrupt_step = ctrc_driver_drift;
			else {
				ctrc_driver_busy_until = 200;
				ctrc_driver_wait_step = ctrc_driver_drift;
			}
			UT_ASSERT(!ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
			UT_ASSERT_EQ(ctrc_driver_acquires, phase);
			UT_ASSERT_EQ(ctrc_driver_waits, phase);
			UT_ASSERT_EQ(ctrc_driver_authorizations, 0);
			UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
			ctrc_interrupt_step = NULL;
		}
	}
}

UT_TEST(test_startup_replay_refuses_late_acquisition_and_authorization_drift)
{
	for (unsigned phase = 0; phase < 2; phase++) {
		for (ctrc_driver_fault = 0; ctrc_driver_fault < 12; ctrc_driver_fault++) {
			ctrc_replay_request_fixture();
			if (phase == 0) ctrc_driver_acquire_step = ctrc_driver_drift;
			else ctrc_driver_authorize_step = ctrc_driver_drift;
			UT_ASSERT(!ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
			UT_ASSERT_EQ(ctrc_driver_acquires, 1);
			UT_ASSERT_EQ(ctrc_driver_authorizations, phase);
			UT_ASSERT_EQ(ctrc_driver_waits, 0);
			UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
			ctrc_driver_lwlocks = 0;
		}
	}
}

UT_TEST(test_startup_replay_refuses_native_failure_without_inline_release)
{
	const ClusterControlRootResult failures[] = {
		CLUSTER_CONTROL_ROOT_STALE_TOKEN, CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN,
		CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE
	};
	for (unsigned fault = 0; fault <= lengthof(failures); fault++) {
		ctrc_replay_request_fixture();
		if (fault < lengthof(failures)) ctrc_driver_result = failures[fault];
		else ctrc_driver_false_authorization = true;
		UT_ASSERT(!ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
		UT_ASSERT_EQ(ctrc_driver_acquires, 1);
		UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
		UT_ASSERT(observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		UT_ASSERT(strstr(ClusterColdRecoveryCtrcObservationRefusal(info.cluster_tx_start),
			fault < lengthof(failures) ? "stage=replay_acquire" : "stage=replay_holders") != NULL);
	}
}

UT_TEST(test_startup_replay_error_preserves_original_cleanup_responsibility)
{
	ctrc_replay_request_fixture();
	ctrc_driver_error = true;
	PG_TRY();
	{
		(void)ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start);
		UT_ASSERT(false);
	}
	PG_CATCH();
	{
		UT_ASSERT_EQ(ctrc_driver_acquires, 1);
		UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
		UT_ASSERT(observation_scope_live && observation_cursor_live);
	}
	PG_END_TRY();
	ctrc_driver_error = false;
	ClusterReleaseColdRecoveryInputs(&info);
	UT_ASSERT_EQ(observation_scope_releases, 1);
	UT_ASSERT_EQ(observation_census_destroys, 1);
}

static void
ctrc_driver_change_manifest(void)
{
	observation_fault = 6;
}

UT_TEST(test_startup_replay_revalidates_manifest_after_all_native_grants)
{
	ctrc_replay_request_fixture();
	ctrc_driver_acquire_step = ctrc_driver_change_manifest;
	UT_ASSERT(!ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
	UT_ASSERT_EQ(ctrc_driver_acquires, 1);
	UT_ASSERT_EQ(ctrc_driver_authorizations, 1);
	UT_ASSERT_EQ(observation_scope_releases, 1);
	UT_ASSERT_EQ(observation_census_destroys, 1);
	UT_ASSERT(!observation_cursor_live && !observation_scope_live);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
}

UT_TEST(test_startup_replay_refuses_lwlocks_before_original_owner_snapshots)
{
	ctrc_replay_request_fixture();
	for (unsigned held = 1; held <= 200; held++) {
		unsigned manifests = observation_manifests, interrupts = tx_interrupts;
		ctrc_driver_lwlocks = held;
		UT_ASSERT(!ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
		UT_ASSERT_EQ(observation_manifests, manifests);
		UT_ASSERT_EQ(tx_interrupts, interrupts);
		UT_ASSERT_EQ(ctrc_driver_acquires + ctrc_driver_authorizations, 0);
	}
	ctrc_driver_lwlocks = 0;
	UT_ASSERT(ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
}

UT_TEST(test_startup_replay_repeat_keeps_original_scope_and_empty_work)
{
	unsigned manifests, steps;
	ctrc_handoff_fixture(false, false);
	UT_ASSERT_EQ(native_ctrc_work.counts.subject_count, 0);
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	steps = observation_steps; manifests = observation_manifests;
	UT_ASSERT(ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
	UT_ASSERT(ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
	UT_ASSERT_EQ(ctrc_driver_acquires, 2);
	UT_ASSERT_EQ(observation_scope_begins, 1);
	UT_ASSERT_EQ(observation_census_begins, 1);
	UT_ASSERT_EQ(observation_steps, steps);
	UT_ASSERT(observation_manifests > manifests);
	UT_ASSERT_EQ(wal_inserts + stamp_writes, 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
}
