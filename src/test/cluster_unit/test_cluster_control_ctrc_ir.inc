/*-------------------------------------------------------------------------
 *
 * test_cluster_control_ctrc_ir.inc
 *     Exercise one original CTRC scope's native serial ownership.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_control_ctrc_ir.inc
 *
 * NOTES
 *     This pgrac-original fixture keeps private symbols in the ctrc_ir_
 *     and test_ctrc_ir_ namespaces. Included by test_cluster_control_root.c;
 *     external Start and GES owners
 *     use its controlled leaves. No PAGE permission is provided here.
 *
 *-------------------------------------------------------------------------
 */
/* Real original ROOT/claim/WAL scope; original Start/current and distributed
 * GES owners remain the existing fixture leaves. No PAGE rights are tested. */
static ClusterWalInputsV1 *ctrc_ir_original_scope;
static bool ctrc_ir_start_current;

static ClusterRecoverySerialRequest
ctrc_ir_provisional(void)
{
	ClusterRecoverySerialRequest request = {0};
	request.mode = CLUSTER_RECOVERY_SERIAL_TX_CLOSURE;
	request.duty = tx_inputs_typed.sources[0].claim.identity;
	request.formation = (const ClusterFormationWitnessV1 *)&tx_inputs_typed;
	request.fence_need_set = (const PgracExternalFenceNeedSetV1 *)&tx_inputs_typed;
	request.fence_admission_set = (const PgracExternalFenceAdmissionSetV1 *)&tx_inputs_typed;
	request.acquire_timeout_ms = request.release_timeout_ms = 1000;
	return request;
}

bool
ClusterColdRecoveryCtrcSerialRequestCurrent(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 index, ClusterWalInputsV1 *scope,
	const ClusterRecoverySerialRequest *request)
{
	ClusterRecoverySerialRequest expected = ctrc_ir_provisional();
	expected.tx_closure_scope = scope;
	return ctrc_ir_start_current && ClusterColdRecoveryCtrcWorkCurrent(start, work)
		&& index < work->counts.subject_count && scope == ctrc_ir_original_scope
		&& request != NULL && memcmp(request, &expected, sizeof(expected)) == 0;
}

static void
ctrc_ir_fixture(bool complete, ClusterWalInputsV1 **inputs)
{
	tx_ir_fixture(complete);
	ctrc_inputs_activate_work();
	UT_ASSERT_EQ(ctrc_inputs_begin(inputs), 0);
	ctrc_ir_original_scope = *inputs;
	ctrc_ir_start_current = true;
}

UT_TEST(test_ctrc_ir_requires_native_grant_and_keeps_original_walr)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoverySerialRequest request;
	uint64 pin_generation;
	unsigned cf;
	ctrc_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
	pin_generation = inputs_pin_generation;
	UT_ASSERT(!cluster_wal_inputs_ctrc_authorized_v1(inputs, 0));
	UT_ASSERT_EQ(cluster_wal_inputs_tx_abort_acquire_v1(inputs, &tx_inputs_candidate.record),
		CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request), 0);
	UT_ASSERT_EQ(test_tx_ir_grants, 1);
	cf = test_cf_lock_calls;
	UT_ASSERT(cluster_wal_inputs_ctrc_authorized_v1(inputs, 0));
	UT_ASSERT_EQ(test_cf_lock_calls, cf);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request), 0);
	UT_ASSERT_EQ(test_tx_ir_grants, 1);
	UT_ASSERT_EQ(inputs_pin_generation, pin_generation);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_release_v1(inputs, 0), 0);
	UT_ASSERT(inputs_pin_held && !test_tx_ir_held);
	UT_ASSERT_EQ(test_tx_ir_releases, 1);
	cluster_wal_inputs_release_v1(&inputs);
	UT_ASSERT(!inputs_pin_held && !test_tx_ir_held); test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_ir_subject_cannot_change_before_confirmed_release)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoverySerialRequest request;
	uint64 pin_generation;
	ctrc_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
	pin_generation = inputs_pin_generation;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 1, &request),
		CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_release_v1(inputs, 1),
		CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT(cluster_wal_inputs_ctrc_authorized_v1(inputs, 0));
	UT_ASSERT(!cluster_wal_inputs_ctrc_authorized_v1(inputs, 1));
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_release_v1(inputs, 0), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 1, &request), 0);
	UT_ASSERT(cluster_wal_inputs_ctrc_authorized_v1(inputs, 1));
	UT_ASSERT_EQ(test_tx_ir_grants, 2);
	UT_ASSERT_EQ(inputs_pin_generation, pin_generation);
	cluster_wal_inputs_release_v1(&inputs);
	UT_ASSERT_EQ(test_tx_ir_releases, 2); test_inputs_startup_cf = false;
}

static void
ctrc_ir_cf_contention(void)
{
	test_cf_grant = false;
}

UT_TEST(test_ctrc_ir_busy_retry_and_cf_contention_retain_one_subject)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoverySerialRequest request;
	ctrc_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
	for (unsigned retry = 0; retry < 2; retry++) {
		test_input_acquire = retry ? CLUSTER_RECOVERY_SERIAL_RETRY : CLUSTER_RECOVERY_SERIAL_BUSY;
		UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request),
			CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT(!test_tx_ir_held && inputs_pin_held);
		UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 1, &request),
			CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	}
	test_input_acquire = CLUSTER_RECOVERY_SERIAL_GRANTED;
	test_tx_ir_after_grant = ctrc_ir_cf_contention;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request),
		CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(test_tx_ir_held && inputs_pin_held);
	UT_ASSERT(!cluster_wal_inputs_ctrc_authorized_v1(inputs, 0));
	test_cf_grant = true; test_tx_ir_after_grant = NULL;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request), 0);
	UT_ASSERT_EQ(test_tx_ir_grants, 1);
	cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_ir_requires_original_complete_subject_request_and_scope)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoverySerialRequest request;
		ctrc_ir_fixture(fault != 0, &inputs); request = ctrc_ir_provisional();
		switch (fault) {
		case 0: break;
		case 1: request.duty.origin_owner_incarnation++; break;
		case 2: request.expected_root_token.root_publish_seq++; break;
		case 3: request.tx_closure_scope = inputs; break;
		case 4: ctrc_ir_original_scope = NULL; break;
		case 5: ctrc_ir_start_current = false; break;
		case 6: ctrc_inputs_work.generation++; break;
		case 7: inputs_pin_current = false; break;
		}
		UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request),
			CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT_EQ(test_tx_ir_grants, 0);
		UT_ASSERT(!cluster_wal_inputs_ctrc_authorized_v1(inputs, 0));
		cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
	}
}

static unsigned ctrc_ir_late_fault;
static void
ctrc_ir_late_change(void)
{
	if (ctrc_ir_late_fault == 0) v2_checkpoint_root_race();
	if (ctrc_ir_late_fault == 1) ctrc_ir_start_current = false;
	if (ctrc_ir_late_fault == 2) ctrc_inputs_work.generation++;
	if (ctrc_ir_late_fault == 3) inputs_pin_current = false;
	if (ctrc_ir_late_fault == 4) test_input_current = false;
}

UT_TEST(test_ctrc_ir_late_drift_keeps_native_holder_until_exact_cleanup)
{
	for (ctrc_ir_late_fault = 0; ctrc_ir_late_fault < 5; ctrc_ir_late_fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoverySerialRequest request;
		ctrc_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
		test_tx_ir_after_grant = ctrc_ir_late_change;
		UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request),
			CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT(test_tx_ir_held && inputs_pin_held);
		UT_ASSERT(!cluster_wal_inputs_ctrc_authorized_v1(inputs, 0));
		UT_ASSERT_EQ(cluster_wal_inputs_ctrc_release_v1(inputs, 0), 0);
		UT_ASSERT(inputs_pin_held && !test_tx_ir_held);
		cluster_wal_inputs_release_v1(&inputs);
		UT_ASSERT_EQ(test_tx_ir_releases, 1); test_inputs_startup_cf = false;
	}
}

UT_TEST(test_ctrc_ir_uncertain_release_retains_request_ir_and_walr_debt)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoverySerialRequest request;
	volatile bool caught = false;
	ctrc_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request), 0);
	test_input_release = false; test_capture_error_level = true;
	PG_TRY(); { (void)cluster_wal_inputs_ctrc_release_v1(inputs, 0); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	test_capture_error_level = false;
	UT_ASSERT(caught && test_last_error_level == FATAL);
	UT_ASSERT(inputs != NULL && inputs_pin_held && test_tx_ir_held);
	UT_ASSERT(!cluster_wal_inputs_ctrc_authorized_v1(inputs, 0));
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 1, &request),
		CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	test_input_release = true;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_release_v1(inputs, 0), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request),
		CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	cluster_wal_inputs_release_v1(&inputs);
	UT_ASSERT(!inputs_pin_held && !test_tx_ir_held); test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_ir_current_is_local_and_suspend_never_renews_held_rights)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoverySerialRequest request, bound;
	unsigned cf;
	ctrc_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request), 0);
	bound = request; bound.tx_closure_scope = inputs;
	cf = test_cf_lock_calls; test_actual_cf = ShareLock;
	UT_ASSERT(cluster_wal_inputs_tx_serial_current_v1(inputs, &bound));
	UT_ASSERT(cluster_wal_inputs_ctrc_authorized_v1(inputs, 0));
	UT_ASSERT_EQ(test_cf_lock_calls, cf); test_actual_cf = NoLock;
	UT_ASSERT_EQ(cluster_wal_inputs_suspend_v1(inputs), 0);
	UT_ASSERT(!test_tx_ir_held && !inputs_pin_held);
	UT_ASSERT_EQ(cluster_wal_inputs_resume_v1(inputs), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request),
		CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_ir_ordinary_tx_scope_cannot_claim_ctrc_subject_rights)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoverySerialRequest request;
	tx_ir_fixture(true); UT_ASSERT_EQ(tx_inputs_begin(&inputs), 0);
	request = ctrc_ir_provisional();
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request),
		CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_release_v1(inputs, 0),
		CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT(!cluster_wal_inputs_ctrc_authorized_v1(inputs, 0));
	UT_ASSERT_EQ(cluster_wal_inputs_tx_abort_acquire_v1(inputs, &tx_inputs_candidate.record), 0);
	cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
}

static void
ctrc_ir_native_error(void)
{
	pg_re_throw();
}

UT_TEST(test_ctrc_ir_error_keeps_original_holder_and_prepared_subject_revoked)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoverySerialRequest request;
	volatile bool caught = false;
	ctrc_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
	test_tx_ir_after_grant = ctrc_ir_native_error;
	PG_TRY(); { (void)cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught && test_tx_ir_held && inputs_pin_held);
	UT_ASSERT(!cluster_wal_inputs_ctrc_authorized_v1(inputs, 0));
	test_tx_ir_after_grant = NULL;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request),
		CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_release_v1(inputs, 0), 0);
	cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
}
