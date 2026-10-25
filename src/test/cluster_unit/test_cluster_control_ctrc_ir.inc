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

UT_TEST(test_ctrc_subject_borrows_only_original_granted_native_owners)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoverySerialRequest request;
	ClusterRecoveryCtrcSubjectAuthorityV1 authority, zero = {0};
	ClusterControlRootFileToken root;
	unsigned cf, grants;
	ctrc_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
	authority = cluster_wal_inputs_ctrc_subject_v1(inputs, 0);
	UT_ASSERT_EQ(memcmp(&authority, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request), 0);
	cf = test_cf_lock_calls; grants = test_tx_ir_grants;
	authority = cluster_wal_inputs_ctrc_subject_v1(inputs, 0);
	UT_ASSERT(cluster_wal_inputs_ctrc_subject_current_v1(&authority));
	UT_ASSERT_EQ(authority.source.retention.scope, inputs);
	UT_ASSERT_EQ(authority.source.retention.input_owner.generation, inputs_pin_generation);
	UT_ASSERT_EQ(authority.ir_request_id, 42);
	UT_ASSERT_EQ(authority.subject_index, 0);
	UT_ASSERT_EQ(authority.work_generation, ctrc_inputs_work.generation);
	UT_ASSERT_EQ(authority.source.selected.kind, CLUSTER_WAL_INPUT_CHECKPOINT);
	UT_ASSERT_EQ(authority.source.selected.checkpoint.lifecycle,
		CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE);
	UT_ASSERT_EQ(memcmp(&authority.source.selected.source,
		&tx_inputs_typed.sources[0], sizeof(authority.source.selected.source)), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_root_token_v1(inputs, &root), 0);
	UT_ASSERT_EQ(memcmp(&authority.source.root, &root, sizeof(root)), 0);
	UT_ASSERT_EQ(memcmp(&authority.source.replay_token, &request.expected_root_token,
		sizeof(request.expected_root_token)), 0);
	UT_ASSERT_EQ(test_cf_lock_calls, cf);
	UT_ASSERT_EQ(test_tx_ir_grants, grants);
	UT_ASSERT(!cluster_wal_inputs_tx_retention_current_v1(&authority.source.retention));
	UT_ASSERT(!cluster_wal_inputs_tx_subject_current_v1(&authority.source));
	cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_subject_rejects_changed_complete_value_without_borrowed_dereference)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoverySerialRequest request;
	ClusterRecoveryCtrcSubjectAuthorityV1 original;
	ctrc_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request), 0);
	original = cluster_wal_inputs_ctrc_subject_v1(inputs, 0);
	UT_ASSERT(cluster_wal_inputs_ctrc_subject_current_v1(&original));
	for (unsigned fault = 0; fault < 13; fault++) {
		ClusterRecoveryCtrcSubjectAuthorityV1 changed = original;
		switch (fault) {
		case 0: changed.source.retention.scope = (ClusterWalInputsV1 *)(uintptr_t)1; break;
		case 1: changed.source.retention.serial = (ClusterRecoverySerialGuard *)(uintptr_t)1; break;
		case 2: changed.source.retention.input_owner.pin = (ClusterWalReadPinV1 *)(uintptr_t)1; break;
		case 3: changed.source.retention.input_owner.generation++; break;
		case 4: ((uint8 *)&changed.source.root)[sizeof(changed.source.root) - 1] ^= 1; break;
		case 5: ((uint8 *)&changed.source.selected)[sizeof(changed.source.selected) - 1] ^= 1; break;
		case 6: changed.source.selected.current = !changed.source.selected.current; break;
		case 7: changed.source.selected.checkpoint.validated_tail_lsn_exclusive++; break;
		case 8: changed.source.replay_token.root_publish_seq++; break;
		case 9: changed.ir_request_id++; break;
		case 10: changed.work_generation++; break;
		case 11: changed.subject_index++; break;
		case 12: changed.reserved_zero = 1; break;
		}
		UT_ASSERT(!cluster_wal_inputs_ctrc_subject_current_v1(&changed));
		UT_ASSERT(cluster_wal_inputs_ctrc_subject_current_v1(&original));
	}
	UT_ASSERT(!cluster_wal_inputs_ctrc_subject_current_v1(NULL));
	cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_subject_live_proof_loss_keeps_original_native_cleanup_debt)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoverySerialRequest request;
		ClusterRecoveryCtrcSubjectAuthorityV1 authority, empty, zero = {0};
		ctrc_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
		UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request), 0);
		authority = cluster_wal_inputs_ctrc_subject_v1(inputs, 0);
		switch (fault) {
		case 0: ctrc_ir_start_current = false; break;
		case 1: ctrc_inputs_work.generation++; break;
		case 2: inputs_pin_current = false; break;
		case 3: test_input_current = false; break;
		case 4: tx_inputs_owner_current = false; break;
		}
		UT_ASSERT(!cluster_wal_inputs_ctrc_subject_current_v1(&authority));
		empty = cluster_wal_inputs_ctrc_subject_v1(inputs, 0);
		UT_ASSERT_EQ(memcmp(&empty, &zero, sizeof(zero)), 0);
		UT_ASSERT(inputs_pin_held && test_tx_ir_held);
		cluster_wal_inputs_release_v1(&inputs);
		UT_ASSERT(!inputs_pin_held && !test_tx_ir_held); test_inputs_startup_cf = false;
	}
}

static ClusterRecoverySerialGuard *ctrc_subject_reacquire_guard;
static uint64 ctrc_subject_reacquire_id;
static void
ctrc_subject_fresh_native_request(void)
{
	/* Native GES supplies a fresh request_id on every actual acquire. This
	 * controlled leaf keeps the old fixture's fixed42 for its original tests. */
	ctrc_subject_reacquire_guard->lock_request.request_id = ++ctrc_subject_reacquire_id;
}

UT_TEST(test_ctrc_subject_release_and_reacquire_never_revives_old_values)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoverySerialRequest request;
	ClusterRecoveryCtrcSubjectAuthorityV1 original, current;
	ctrc_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request), 0);
	original = cluster_wal_inputs_ctrc_subject_v1(inputs, 0);
	UT_ASSERT(cluster_wal_inputs_ctrc_subject_current_v1(&original));
	ctrc_subject_reacquire_guard = original.source.retention.serial;
	ctrc_subject_reacquire_id = original.ir_request_id;
	for (unsigned next = 0; next < 2; next++) {
		UT_ASSERT_EQ(cluster_wal_inputs_ctrc_release_v1(inputs, next == 0 ? 0 : 1), 0);
		UT_ASSERT(!cluster_wal_inputs_ctrc_subject_current_v1(&original));
		test_tx_ir_after_grant = ctrc_subject_fresh_native_request;
		UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, next == 0 ? 1 : 0, &request), 0);
		test_tx_ir_after_grant = NULL;
		current = cluster_wal_inputs_ctrc_subject_v1(inputs, next == 0 ? 1 : 0);
		UT_ASSERT(cluster_wal_inputs_ctrc_subject_current_v1(&current));
		UT_ASSERT(current.ir_request_id > original.ir_request_id);
		UT_ASSERT(!cluster_wal_inputs_ctrc_subject_current_v1(&original));
	}
	cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_subject_recreated_scope_or_pin_cannot_renew_old_value)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoverySerialRequest request;
	ClusterRecoveryCtrcSubjectAuthorityV1 original, current, changed;
	ctrc_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request), 0);
	original = cluster_wal_inputs_ctrc_subject_v1(inputs, 0);
	cluster_wal_inputs_release_v1(&inputs);
	UT_ASSERT(!cluster_wal_inputs_ctrc_subject_current_v1(&original));
	ctrc_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request), 0);
	current = cluster_wal_inputs_ctrc_subject_v1(inputs, 0);
	UT_ASSERT(cluster_wal_inputs_ctrc_subject_current_v1(&current));
	UT_ASSERT(!cluster_wal_inputs_ctrc_subject_current_v1(&original));
	changed = original;
	changed.source.retention.scope = current.source.retention.scope;
	changed.source.retention.serial = current.source.retention.serial;
	UT_ASSERT(!cluster_wal_inputs_ctrc_subject_current_v1(&changed));
	UT_ASSERT(current.source.retention.input_owner.generation
		> original.source.retention.input_owner.generation);
	cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_subject_window_is_exact_and_current_does_not_read_root)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoverySerialRequest request;
	ClusterRecoveryCtrcSubjectAuthorityV1 authority;
	XLogRecPtr lower, upper;
	uint16 thread;
	unsigned cf;
	ctrc_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request), 0);
	authority = cluster_wal_inputs_ctrc_subject_v1(inputs, 0);
	lower = authority.source.selected.checkpoint.checkpoint_lower_lsn;
	upper = authority.source.selected.checkpoint.validated_tail_lsn_exclusive;
	thread = authority.source.selected.source.claim.identity.origin_thread_id;
	UT_ASSERT(cluster_wal_inputs_ctrc_subject_covers_window_v1(&authority, thread, lower, upper));
	UT_ASSERT(!cluster_wal_inputs_ctrc_subject_covers_window_v1(&authority, thread + 1, lower, upper));
	UT_ASSERT(!cluster_wal_inputs_ctrc_subject_covers_window_v1(&authority, thread, lower - 1, upper));
	UT_ASSERT(!cluster_wal_inputs_ctrc_subject_covers_window_v1(&authority, thread, lower, upper + 1));
	UT_ASSERT(!cluster_wal_inputs_ctrc_subject_covers_window_v1(&authority, thread, lower, lower));
	cf = test_cf_lock_calls;
	v2_checkpoint_root_race();
	/* The no-I/O check is not a disk proof. The original full ROOT compare
	 * outside native locks must detect that change before PAGE preflight. */
	UT_ASSERT(cluster_wal_inputs_ctrc_subject_current_v1(&authority));
	UT_ASSERT_EQ(test_cf_lock_calls, cf);
	UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT(!cluster_wal_inputs_ctrc_subject_current_v1(&authority));
	UT_ASSERT(inputs_pin_held && test_tx_ir_held);
	cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_subject_uncertain_release_withholds_view_and_retains_owners)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoverySerialRequest request;
	ClusterRecoveryCtrcSubjectAuthorityV1 authority, empty, zero = {0};
	volatile bool caught = false;
	ctrc_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request), 0);
	authority = cluster_wal_inputs_ctrc_subject_v1(inputs, 0);
	test_input_release = false; test_capture_error_level = true;
	PG_TRY(); { (void)cluster_wal_inputs_ctrc_release_v1(inputs, 0); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	test_capture_error_level = false;
	UT_ASSERT(caught && test_last_error_level == FATAL);
	UT_ASSERT(!cluster_wal_inputs_ctrc_subject_current_v1(&authority));
	empty = cluster_wal_inputs_ctrc_subject_v1(inputs, 0);
	UT_ASSERT_EQ(memcmp(&empty, &zero, sizeof(zero)), 0);
	UT_ASSERT(inputs_pin_held && test_tx_ir_held);
	test_input_release = true;
	cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_subject_binds_exact_selected_history)
{
	uint8 bytes[66048], uuid[16] = {0x86};
	ClusterPhase1FullStopPlan plan;
	ClusterWalHistoryImage history;
	ClusterWalHistoryStage stage;
	ControlRootImage root;
	ClusterWalInputsV1 *inputs = NULL;
	const ClusterWalInputV1 *item = NULL;
	ClusterWalInputV1 expected = {0};
	ClusterRecoverySerialRequest request;
	ClusterRecoveryCtrcSubjectAuthorityV1 authority;
	v2_close_history_fixture_sequenced(bytes, &plan, &history, &stage, 2, true);
	root_fixture_version3(bytes);
	UT_ASSERT_EQ(cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	tx_ir_complete(&history.records[0].snapshot);
	test_actual_cf = ExclusiveLock;
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 890002, uuid, &stage), 0);
	UT_ASSERT_EQ(cluster_wal_history_install(&stage), 0);
	root.refs[0].history_generation = stage.generation;
	memcpy(root.refs[0].history_sha256, stage.sha256, 32);
	UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
	test_actual_cf = test_cf_mode = NoLock;
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	v2_write_roots(root.bytes);
	cluster_shared_config = true; MyBackendType = B_BG_WORKER;
	inputs_pin_held = inputs_pin_refuse = inputs_pin_race = inputs_pin_throw = false;
	UT_ASSERT_EQ(cluster_wal_inputs_begin_v1(v2_storage, TEST_SYSID, &inputs), 0);
	for (uint32 i = 0; i < cluster_wal_inputs_count_v1(inputs); i++) {
		const ClusterWalInputV1 *at = cluster_wal_inputs_at_v1(inputs, i);
		if (!at->current && at->kind == CLUSTER_WAL_INPUT_CHECKPOINT
			&& at->checkpoint.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE) {
			UT_ASSERT(item == NULL); item = at;
		}
	}
	UT_ASSERT(item != NULL);
	if (item != NULL) {
		ClusterColdParticipantV1 *cut;
		expected = *item;
		memset(&tx_inputs_typed, 0, sizeof(tx_inputs_typed));
		tx_inputs_typed.system_identifier = TEST_SYSID;
		tx_inputs_typed.census_root_captured = true;
		UT_ASSERT_EQ(cluster_wal_inputs_root_token_v1(inputs, &tx_inputs_typed.census_root), 0);
		tx_inputs_typed.participant_count = tx_inputs_typed.replay_count = 1;
		tx_inputs_typed.sources[0] = item->source;
		cut = &tx_inputs_typed.participants[0];
		cut->thread_id = item->source.claim.identity.origin_thread_id;
		cut->owner_incarnation = item->source.claim.identity.origin_owner_incarnation;
		cut->timeline = item->source.timeline;
		cut->physical_lower = item->checkpoint.checkpoint_lower_lsn;
		cut->native_redo = item->native_redo;
		cut->tail_end = item->checkpoint.validated_tail_lsn_exclusive;
	}
	cluster_wal_inputs_release_v1(&inputs);
	tx_inputs_activate();
	ctrc_inputs_activate_work();
	UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
	ctrc_ir_original_scope = inputs; ctrc_ir_start_current = true;
	request = ctrc_ir_provisional();
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request), 0);
	authority = cluster_wal_inputs_ctrc_subject_v1(inputs, 0);
	UT_ASSERT(authority.source.retention.scope == inputs);
	UT_ASSERT(!authority.source.selected.current);
	UT_ASSERT(memcmp(&authority.source.selected, &expected, sizeof(expected)) == 0);
	UT_ASSERT(cluster_wal_inputs_ctrc_subject_current_v1(&authority));
	UT_ASSERT(cluster_wal_inputs_ctrc_subject_covers_window_v1(&authority,
		expected.source.claim.identity.origin_thread_id, expected.checkpoint.checkpoint_lower_lsn,
		expected.checkpoint.validated_tail_lsn_exclusive));
	cluster_wal_inputs_release_v1(&inputs);
	UT_ASSERT(!test_tx_ir_held && !inputs_pin_held);
	test_inputs_startup_cf = false;
}
