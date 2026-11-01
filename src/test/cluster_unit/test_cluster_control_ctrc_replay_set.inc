/*-------------------------------------------------------------------------
 *
 * test_cluster_control_ctrc_replay_set.inc
 *     Retain every original replay-origin IR on one native WALR scope.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_control_ctrc_replay_set.inc
 *
 * NOTES
 *     Original ROOT/claim/WAL readers execute. Start and distributed GES
 *     remain controlled leaves; no PAGE or service permission is exercised.
 *
 *-------------------------------------------------------------------------
 */
static unsigned ctrc_set_held, ctrc_set_calls, ctrc_set_release_calls;
static uint16 ctrc_set_call_threads[512], ctrc_set_release_threads[512];
static uint16 ctrc_set_block_thread, ctrc_set_fail_release;
static ClusterRecoverySerialAcquireResult ctrc_set_block_result;
static uint64 ctrc_set_next_id;
static void (*ctrc_set_after_grant)(void);
static unsigned ctrc_set_preflight_retire;

ClusterRecoverySerialRequest
ClusterColdRecoveryCtrcReplayRequest(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 index)
{
	ClusterRecoverySerialRequest request = {0};
	/* Full Start observation can close its original scope on refusal/ERROR. */
	if (index == 1 && ctrc_set_preflight_retire != 0) {
		cluster_wal_inputs_release_v1(&ctrc_ir_original_scope);
		if (ctrc_set_preflight_retire == 2) pg_re_throw();
		return request;
	}
	if (!ctrc_ir_start_current || !ClusterColdRecoveryCtrcWorkCurrent(start, work)
		|| index >= tx_inputs_typed.replay_count || index >= CLUSTER_COLD_MAX_PARTICIPANTS)
		return request;
	request = ctrc_ir_provisional(); request.duty = tx_inputs_typed.sources[index].claim.identity;
	return request;
}

static bool
ctrc_set_original_current(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 index, ClusterWalInputsV1 *scope,
	const ClusterRecoverySerialRequest *request)
{
	ClusterRecoverySerialRequest expected = ClusterColdRecoveryCtrcReplayRequest(start, work, index);
	expected.tx_closure_scope = scope;
	return expected.mode == CLUSTER_RECOVERY_SERIAL_TX_CLOSURE
		&& scope == ctrc_ir_original_scope && request != NULL
		&& memcmp(request, &expected, sizeof(expected)) == 0;
}

static ClusterRecoverySerialAcquireResult
ctrc_set_native_acquire(const ClusterRecoverySerialRequest *request, ClusterRecoverySerialGuard *guard)
{
	uint16 thread = request->duty.origin_thread_id;
	UT_ASSERT_EQ(request->mode, CLUSTER_RECOVERY_SERIAL_TX_CLOSURE);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT(!guard->held && !guard->release_uncertain);
	UT_ASSERT_EQ(cluster_wal_inputs_tx_serial_validate_v1(request->tx_closure_scope, request), 0);
	UT_ASSERT(ctrc_set_calls < lengthof(ctrc_set_call_threads));
	ctrc_set_call_threads[ctrc_set_calls++] = thread;
	if (thread == ctrc_set_block_thread) return ctrc_set_block_result;
	memset(guard, 0, sizeof(*guard));
	guard->held = true; guard->mode = request->mode; guard->duty = request->duty;
	guard->root_read_token = request->expected_root_token; guard->pending = request->pending;
	guard->formation = request->formation; guard->fence_need_set = request->fence_need_set;
	guard->fence_admission_set = request->fence_admission_set;
	guard->tx_closure_scope = request->tx_closure_scope;
	guard->release_timeout_ms = request->release_timeout_ms;
	guard->lock_request.request_id = ++ctrc_set_next_id;
	ctrc_set_held++; test_tx_ir_held = true; test_tx_ir_grants++;
	if (ctrc_set_after_grant != NULL) ctrc_set_after_grant();
	return CLUSTER_RECOVERY_SERIAL_GRANTED;
}

static ClusterRecoverySerialReleaseResult
ctrc_set_native_release(ClusterRecoverySerialGuard *guard)
{
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT(guard->held || guard->release_uncertain);
	UT_ASSERT(ctrc_set_release_calls < lengthof(ctrc_set_release_threads));
	ctrc_set_release_threads[ctrc_set_release_calls++] = guard->duty.origin_thread_id;
	if (guard->duty.origin_thread_id == ctrc_set_fail_release) {
		guard->release_uncertain = true;
		return CLUSTER_RECOVERY_SERIAL_RELEASE_UNCONFIRMED;
	}
	guard->held = guard->release_uncertain = false;
	UT_ASSERT(ctrc_set_held > 0); ctrc_set_held--;
	test_tx_ir_held = ctrc_set_held != 0; test_tx_ir_releases++;
	return CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED;
}

static void
ctrc_set_fixture(unsigned complete_mask, bool reverse, ClusterWalInputsV1 **inputs)
{
	uint8 bytes[66048]; ClusterRecoveryAnchorV2 anchors[2]; ControlRootImage root;
	ClusterWalInputsV1 *original = NULL;
	inputs_closed_wal_fixture(bytes, anchors);
	UT_ASSERT_EQ(cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	if (complete_mask & 1) tx_ir_complete(&root.records[0]);
	if (complete_mask & 2) tx_ir_complete(&root.records[127]);
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0); v2_write_roots(root.bytes);
	UT_ASSERT_EQ(cluster_wal_inputs_begin_v1(v2_storage, TEST_SYSID, &original), 0);
	tx_inputs_capture(original, NULL); cluster_wal_inputs_release_v1(&original);
	tx_inputs_activate(); ctrc_inputs_activate_work();
	ctrc_inputs_work.counts.subject_count = ctrc_inputs_work.counts.unresolved_count = 0;
	if (reverse) {
		ClusterWalSourceRef source = tx_inputs_typed.sources[0];
		ClusterColdParticipantV1 participant = tx_inputs_typed.participants[0];
		tx_inputs_typed.sources[0] = tx_inputs_typed.sources[1]; tx_inputs_typed.sources[1] = source;
		tx_inputs_typed.participants[0] = tx_inputs_typed.participants[1];
		tx_inputs_typed.participants[1] = participant;
	}
	UT_ASSERT_EQ(ctrc_inputs_begin(inputs), 0);
	ctrc_ir_original_scope = *inputs; ctrc_ir_start_current = true;
	ctrc_set_held = ctrc_set_calls = ctrc_set_release_calls = 0;
	ctrc_set_block_thread = ctrc_set_fail_release = 0; ctrc_set_after_grant = NULL;
	ctrc_set_preflight_retire = 0;
	ctrc_replay_current_override = ctrc_set_original_current;
	test_ctrc_set_acquire_hook = ctrc_set_native_acquire;
	test_ctrc_set_release_hook = ctrc_set_native_release;
}

static void
ctrc_set_finish(ClusterWalInputsV1 **inputs)
{
	cluster_wal_inputs_release_v1(inputs);
	UT_ASSERT_EQ(ctrc_set_held, 0); UT_ASSERT(!inputs_pin_held && !test_tx_ir_held);
	test_ctrc_set_acquire_hook = NULL; test_ctrc_set_release_hook = NULL;
	ctrc_replay_current_override = NULL; test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_set_holds_unordered_original_origins_without_tt_subjects)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryCtrcReplaySetSourceV1 first, second;
	ClusterRecoverySerialRequest request;
	uint64 generation; unsigned cf;
	ctrc_set_fixture(3, true, &inputs); generation = inputs_pin_generation;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT_EQ(test_tx_ir_grants, 2);
	UT_ASSERT_EQ(ctrc_set_call_threads[0], 1); UT_ASSERT_EQ(ctrc_set_call_threads[1], 128);
	cf = test_cf_lock_calls;
	first = cluster_wal_inputs_ctrc_replays_source_v1(inputs, 0);
	second = cluster_wal_inputs_ctrc_replays_source_v1(inputs, 1);
	UT_ASSERT_EQ(first.selected.source.claim.identity.origin_thread_id, 128);
	UT_ASSERT_EQ(second.selected.source.claim.identity.origin_thread_id, 1);
	UT_ASSERT(first.ir_request_id != 0 && second.ir_request_id != 0);
	UT_ASSERT(cluster_wal_inputs_ctrc_replays_source_current_v1(&first));
	UT_ASSERT(cluster_wal_inputs_ctrc_replays_source_current_v1(&second));
	UT_ASSERT_EQ(test_cf_lock_calls, cf); UT_ASSERT_EQ(inputs_pin_generation, generation);
	request = ClusterColdRecoveryCtrcReplayRequest(tx_inputs_start, &ctrc_inputs_work, 0);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replay_acquire_v1(inputs, 0, &request), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT(!cluster_wal_inputs_ctrc_replay_authorized_v1(inputs, 0));
	UT_ASSERT(!cluster_wal_inputs_ctrc_authorized_v1(inputs, 0));
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_release_v1(inputs), 0);
	UT_ASSERT(!cluster_wal_inputs_ctrc_replays_source_current_v1(&first));
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT(inputs_recovery_held); ctrc_set_finish(&inputs);
}

UT_TEST(test_ctrc_set_partial_native_wait_withholds_every_source_view)
{
	for (unsigned retry = 0; retry < 2; retry++) {
		ClusterWalInputsV1 *inputs = NULL; ClusterRecoveryCtrcReplaySetSourceV1 source, zero = {0};
		ctrc_set_fixture(3, false, &inputs); ctrc_set_block_thread = 128;
		ctrc_set_block_result = retry ? CLUSTER_RECOVERY_SERIAL_RETRY : CLUSTER_RECOVERY_SERIAL_BUSY;
		UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT_EQ(ctrc_set_held, 1); UT_ASSERT(inputs_recovery_held);
		UT_ASSERT(!cluster_wal_inputs_ctrc_replays_authorized_v1(inputs));
		source = cluster_wal_inputs_ctrc_replays_source_v1(inputs, 0);
		UT_ASSERT(memcmp(&source, &zero, sizeof(zero)) == 0);
		ctrc_set_block_thread = 0;
		UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
		UT_ASSERT_EQ(test_tx_ir_grants, 2); UT_ASSERT_EQ(ctrc_set_calls, 3);
		UT_ASSERT_EQ(ctrc_set_call_threads[2], 128); ctrc_set_finish(&inputs);
	}
}

static void ctrc_set_cf_after_all_grants(void) { if (ctrc_set_held == 2) test_cf_grant = false; }
UT_TEST(test_ctrc_set_postgrant_cf_wait_keeps_all_original_holders)
{
	ClusterWalInputsV1 *inputs = NULL;
	ctrc_set_fixture(3, false, &inputs); ctrc_set_after_grant = ctrc_set_cf_after_all_grants;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT(!cluster_wal_inputs_ctrc_replays_authorized_v1(inputs));
	test_cf_grant = true; ctrc_set_after_grant = NULL;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	UT_ASSERT_EQ(test_tx_ir_grants, 2); ctrc_set_finish(&inputs);
}

UT_TEST(test_ctrc_set_validates_every_source_before_first_native_grant)
{
	for (unsigned mask = 0; mask < 3; mask++) {
		ClusterWalInputsV1 *inputs = NULL;
		ctrc_set_fixture(mask, false, &inputs);
		UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT_EQ(test_tx_ir_grants, 0); UT_ASSERT(inputs_recovery_held);
		ctrc_set_finish(&inputs);
	}
}

static unsigned ctrc_set_late_fault;
static void
ctrc_set_late_change(void)
{
	if (ctrc_set_held != 2) return;
	if (ctrc_set_late_fault == 0) v2_checkpoint_root_race();
	if (ctrc_set_late_fault == 1) tx_inputs_typed.participants[1].reserved_zero = 1;
	if (ctrc_set_late_fault == 2) ctrc_inputs_work.generation++;
	if (ctrc_set_late_fault == 3) inputs_pin_current = false;
	if (ctrc_set_late_fault == 4) test_input_current = false;
}
UT_TEST(test_ctrc_set_late_original_loss_never_discards_native_cleanup_debt)
{
	for (ctrc_set_late_fault = 0; ctrc_set_late_fault < 5; ctrc_set_late_fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		ctrc_set_fixture(3, false, &inputs); ctrc_set_after_grant = ctrc_set_late_change;
		UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT(inputs_recovery_held);
		UT_ASSERT(!cluster_wal_inputs_ctrc_replays_authorized_v1(inputs));
		ctrc_set_after_grant = NULL; ctrc_set_finish(&inputs);
	}
}

UT_TEST(test_ctrc_set_source_rejects_every_changed_byte_and_other_window)
{
	ClusterWalInputsV1 *inputs = NULL; ClusterRecoveryCtrcReplaySetSourceV1 original;
	XLogRecPtr lower, upper; uint16 thread; unsigned cf;
	ctrc_set_fixture(3, false, &inputs);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	original = cluster_wal_inputs_ctrc_replays_source_v1(inputs, 1); cf = test_cf_lock_calls;
	for (Size i = 0; i < sizeof(original); i++) {
		ClusterRecoveryCtrcReplaySetSourceV1 changed = original; ((uint8 *)&changed)[i] ^= 1;
		UT_ASSERT(!cluster_wal_inputs_ctrc_replays_source_current_v1(&changed));
	}
	lower = original.selected.checkpoint.checkpoint_lower_lsn;
	upper = original.selected.checkpoint.validated_tail_lsn_exclusive;
	thread = original.selected.source.claim.identity.origin_thread_id;
	UT_ASSERT(cluster_wal_inputs_ctrc_replays_source_covers_window_v1(&original, thread, lower, upper));
	UT_ASSERT(!cluster_wal_inputs_ctrc_replays_source_covers_window_v1(&original, thread + 1, lower, upper));
	UT_ASSERT(!cluster_wal_inputs_ctrc_replays_source_covers_window_v1(&original, thread, lower - 1, upper));
	UT_ASSERT(!cluster_wal_inputs_ctrc_replays_source_covers_window_v1(&original, thread, lower, upper + 1));
	UT_ASSERT_EQ(test_cf_lock_calls, cf); ctrc_set_finish(&inputs);
	UT_ASSERT(!cluster_wal_inputs_ctrc_replays_source_current_v1(&original));
}

UT_TEST(test_ctrc_set_reverse_uncertain_release_keeps_remaining_ir_and_walr)
{
	ClusterWalInputsV1 *inputs = NULL; volatile bool caught = false;
	ctrc_set_fixture(3, false, &inputs);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	ctrc_set_fail_release = 1; test_capture_error_level = true;
	PG_TRY(); { (void)cluster_wal_inputs_ctrc_replays_release_v1(inputs); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY(); test_capture_error_level = false;
	UT_ASSERT(caught && test_last_error_level == FATAL);
	UT_ASSERT_EQ(ctrc_set_release_threads[0], 128); UT_ASSERT_EQ(ctrc_set_release_threads[1], 1);
	UT_ASSERT_EQ(ctrc_set_held, 1); UT_ASSERT(inputs_recovery_held);
	UT_ASSERT(!cluster_wal_inputs_ctrc_replays_authorized_v1(inputs));
	ctrc_set_fail_release = 0; ctrc_set_finish(&inputs);
	UT_ASSERT_EQ(ctrc_set_release_threads[2], 1);
}

static void ctrc_set_native_error(void) { if (ctrc_set_held == 2) pg_re_throw(); }
UT_TEST(test_ctrc_set_native_error_keeps_exact_holders_and_original_purpose)
{
	ClusterWalInputsV1 *inputs = NULL; volatile bool caught = false;
	ctrc_set_fixture(3, false, &inputs); ctrc_set_after_grant = ctrc_set_native_error;
	PG_TRY(); { (void)cluster_wal_inputs_ctrc_replays_acquire_v1(inputs); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY(); ctrc_set_after_grant = NULL;
	UT_ASSERT(caught && ctrc_set_held == 2 && inputs_recovery_held);
	UT_ASSERT(!cluster_wal_inputs_ctrc_replays_authorized_v1(inputs));
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	ctrc_set_finish(&inputs);
}

UT_TEST(test_ctrc_set_full_preflight_refusal_can_retire_original_scope)
{
	ClusterWalInputsV1 *inputs = NULL;
	ctrc_set_fixture(3, false, &inputs); ctrc_set_preflight_retire = 1;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT(ctrc_ir_original_scope == NULL && !inputs_pin_held);
	UT_ASSERT_EQ(test_tx_ir_grants, 0); UT_ASSERT_EQ(ctrc_set_held, 0);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT(!cluster_wal_inputs_ctrc_replays_authorized_v1(inputs));
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_release_v1(inputs), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	inputs = NULL; ctrc_set_finish(&inputs);
}

UT_TEST(test_ctrc_set_full_preflight_error_can_retire_original_scope)
{
	ClusterWalInputsV1 *inputs = NULL; volatile bool caught = false;
	ctrc_set_fixture(3, false, &inputs); ctrc_set_preflight_retire = 2;
	PG_TRY(); { (void)cluster_wal_inputs_ctrc_replays_acquire_v1(inputs); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught && ctrc_ir_original_scope == NULL && !inputs_pin_held);
	UT_ASSERT_EQ(test_tx_ir_grants, 0); UT_ASSERT_EQ(ctrc_set_held, 0);
	UT_ASSERT(!cluster_wal_inputs_ctrc_replays_authorized_v1(inputs));
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_release_v1(inputs), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	inputs = NULL; ctrc_set_finish(&inputs);
}
