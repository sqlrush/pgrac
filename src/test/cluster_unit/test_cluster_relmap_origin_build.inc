/* Original checkpoint inputs for new BUILDING storage. The ROOT, immutable
 * anchor, claim, retained WAL and readers are real; cold authority is the
 * surrounding fixture's controlled leaf. No native base/terminal provider
 * is needed by this input projection. */
static ClusterWalInputsV1 *
origin_build_setup(unsigned fault, bool clean)
{
	ClusterWalInputsV1 *inputs = NULL;
	custody_saved_node = cluster_node_id;
	cluster_node_id = 1;
	custody_scope = 7;
	custody_scope_reads = custody_scope_error_after = origin_build_scope_change_after = 0;
	origin_build_recheck_hook = NULL;
	prefix_base_fault = prefix_base_scope_checks = prefix_base_reads = 0;
	custody_terminal_calls = custody_inventory_calls = 0;
	if (clean) {
		uint8 bytes[66048];
		ClusterRecoveryAnchorV2 anchors[2];
		ControlRootImage root;
		ClusterRelmapCheckpointWindow w = { 0 };
		char path[MAXPGPATH];
		inputs_closed_wal_fixture(bytes, anchors);
		UT_ASSERT_EQ(
			cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
		w.system_identifier = TEST_SYSID;
		w.database_incarnation = anchors[0].database_incarnation;
		w.origin = 0;
		w.epoch = 97;
		w.boot_incarnation = 411;
		w.admitted_incarnation = anchors[0].identity.origin_owner_incarnation;
		w.native_floor = 80;
		w.last_issued = 99;
		w.allocation_next = 100;
		UT_ASSERT(cluster_relmap_checkpoint_window_encode_v2(
			&w, &anchors[0].checkpoint_copy, anchors[0].checkpoint,
			root.records[0].validated_tail_lsn_exclusive, 105, anchors[0].native_window));
		v2_anchor_object(bytes, &anchors[0], &anchors[0].identity, path);
		v2_write_roots(bytes);
		test_inputs_startup_cf = true;
		UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	} else
		inputs = terminal_prefix_fixture(false, fault);
	if (inputs != NULL)
		prefix_base_expected = *cluster_wal_inputs_at_v1(inputs, 0);
	return inputs;
}

static void
origin_build_done(ClusterWalInputsV1 **inputs)
{
	custody_scope_error_after = origin_build_scope_change_after = 0;
	origin_build_recheck_hook = NULL;
	test_stop_share_hook = NULL;
	test_throw_root_read = false;
	CritSectionCount = 0;
	custody_done(inputs);
}

UT_TEST(test_origin_build_uses_original_checkpoint_and_round)
{
	for (unsigned era = 0; era < 2; era++) {
		ClusterRelmapRetentionDirectory out = { 0 };
		ClusterWalInputsV1 *inputs;
		custody_era_offset = (uint64)era << 32;
		inputs = origin_build_setup(29, false);
		UT_ASSERT(cluster_relmap_recovery_retention_build_start_v1(inputs, 0, &out));
		UT_ASSERT_EQ(out.state, CLUSTER_RELMAP_RETENTION_BUILDING);
		UT_ASSERT_EQ(out.system_identifier, TEST_SYSID);
		UT_ASSERT_EQ(out.database_incarnation,
					 prefix_base_expected.source.claim.database_incarnation);
		UT_ASSERT_EQ(out.origin, 0);
		UT_ASSERT_EQ(out.generation, custody_scope);
		UT_ASSERT_EQ(out.native_floor, custody_era_offset + 80);
		UT_ASSERT_EQ(out.last_issued, custody_era_offset + 99);
		UT_ASSERT_EQ(out.truncate_serial, 0);
		for (unsigned i = 0; i < lengthof(out.pins); i++)
			UT_ASSERT(!out.pins[i].occupied);
		UT_ASSERT_EQ(cluster_relmap_retention_allocation_check(&out, out.last_issued + 1),
					 CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT(custody_scope_reads >= 2);
		UT_ASSERT_EQ(prefix_base_reads + custody_terminal_calls + custody_inventory_calls, 0);
		UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
		UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
		origin_build_done(&inputs);
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_origin_build_clean_checkpoint_needs_no_terminal_or_pending)
{
	ClusterWalInputsV1 *inputs = origin_build_setup(0, true);
	ClusterRelmapRetentionDirectory out = { 0 };
	UT_ASSERT(cluster_relmap_recovery_retention_build_start_v1(inputs, 0, &out));
	UT_ASSERT_EQ(out.state, CLUSTER_RELMAP_RETENTION_BUILDING);
	UT_ASSERT_EQ(out.native_floor, 80);
	UT_ASSERT_EQ(out.last_issued, 99);
	UT_ASSERT_EQ(out.generation, 7);
	UT_ASSERT_EQ(prefix_base_reads + custody_terminal_calls + custody_inventory_calls, 0);
	UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
	origin_build_done(&inputs);
}

UT_TEST(test_origin_build_refuses_wrong_owner_and_scope)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterWalInputsV1 *inputs = origin_build_setup(29, false);
		ClusterRelmapRetentionDirectory out = { 0 }, zero = { 0 };
		if (fault == 0)
			MyBackendType = B_BG_WORKER;
		if (fault == 1)
			inputs_native_recovery = false;
		if (fault == 2)
			CritSectionCount = 1;
		if (fault == 3)
			custody_scope = 0;
		if (fault == 4)
			prefix_base_fault = 1;
		UT_ASSERT(
			!cluster_relmap_recovery_retention_build_start_v1(fault == 5 ? NULL : inputs, 0, &out));
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		UT_ASSERT_EQ(prefix_base_reads + custody_terminal_calls, 0);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		origin_build_done(&inputs);
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_origin_build_refuses_unsupported_or_changed_inputs)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		ClusterWalInputsV1 *inputs = origin_build_setup(fault == 0	 ? 1
														: fault == 1 ? 10
														: fault == 2 ? 9
																	 : 29,
														false);
		ClusterRelmapRetentionDirectory out = { 0 }, zero = { 0 };
		ClusterWalInputV1 *input = (void *)cluster_wal_inputs_at_v1(inputs, 0);
		if (fault == 3)
			input->kind = CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL;
		if (fault == 4)
			input->checkpoint.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
		if (fault == 5) {
			ClusterWalInputV1 *other = (void *)cluster_wal_inputs_at_v1(inputs, 1);
			other->source.claim.identity.origin_node_id = 0;
		}
		if (fault == 6)
			input->native_checkpoint.nextXid = FullTransactionIdFromU64(101);
		UT_ASSERT(!cluster_relmap_recovery_retention_build_start_v1(inputs,
																	fault == 7	 ? 1
																	: fault == 8 ? 128
																				 : 0,
																	&out));
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		UT_ASSERT_EQ(prefix_base_reads, 0);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		origin_build_done(&inputs);
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_origin_build_cannot_reset_or_overlap_storage)
{
	ClusterWalInputsV1 *inputs = origin_build_setup(29, false);
	ClusterWalInputV1 before = *cluster_wal_inputs_at_v1(inputs, 0);
	ClusterRelmapRetentionDirectory out = { 0 }, saved;
	for (unsigned fault = 0; fault < 4; fault++) {
		memset(&out, 0, sizeof(out));
		if (fault == 0)
			out.state = CLUSTER_RELMAP_RETENTION_BUILDING;
		if (fault == 1)
			out.generation = 2;
		if (fault == 2)
			out.pins[63].wal_end = 123; /* Even unoccupied stale bytes. */
		if (fault == 3)
			out.state = CLUSTER_RELMAP_RETENTION_COMPLETE;
		saved = out;
		UT_ASSERT(!cluster_relmap_recovery_retention_build_start_v1(inputs, 0, &out));
		UT_ASSERT_EQ(memcmp(&out, &saved, sizeof(out)), 0);
	}
	UT_ASSERT(!cluster_relmap_recovery_retention_build_start_v1(inputs, 0, NULL));
	UT_ASSERT(!cluster_relmap_recovery_retention_build_start_v1(
		inputs, 0, (void *)cluster_wal_inputs_at_v1(inputs, 0)));
	UT_ASSERT_EQ(memcmp(&before, cluster_wal_inputs_at_v1(inputs, 0), sizeof(before)), 0);
	UT_ASSERT_EQ(custody_scope_reads, 0);
	UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
	origin_build_done(&inputs);
}

static void
origin_build_recheck_fault(void)
{
	origin_build_recheck_hook = NULL;
	if (relmap_test_fault == 1)
		v2_checkpoint_root_race();
	else
		inputs_pin_current = false;
}

UT_TEST(test_origin_build_late_round_root_and_pin_refusal_is_atomic)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterWalInputsV1 *inputs = origin_build_setup(29, false);
		ClusterRelmapRetentionDirectory out = { 0 }, zero = { 0 };
		if (fault == 0)
			origin_build_scope_change_after = 2;
		if (fault == 1)
			prefix_base_fault = 9;
		if (fault >= 2) {
			relmap_test_fault = fault - 1;
			origin_build_recheck_hook = origin_build_recheck_fault;
		}
		UT_ASSERT(!cluster_relmap_recovery_retention_build_start_v1(inputs, 0, &out));
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		if (fault <= 1)
			UT_ASSERT(custody_scope_reads >= 2);
		if (fault >= 2)
			UT_ASSERT(origin_build_recheck_hook == NULL);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		UT_ASSERT_EQ(prefix_base_reads, 0);
		origin_build_done(&inputs);
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_origin_build_error_keeps_output_and_original_pins)
{
	for (unsigned at = 1; at <= 2; at++) {
		ClusterWalInputsV1 *inputs = origin_build_setup(29, false);
		ClusterRelmapRetentionDirectory out = { 0 }, zero = { 0 };
		volatile bool threw = false;
		custody_scope_error_after = at;
		test_capture_error_level = true;
		PG_TRY();
		{
			(void)cluster_relmap_recovery_retention_build_start_v1(inputs, 0, &out);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT(threw);
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		UT_ASSERT_EQ(prefix_base_reads, 0);
		origin_build_done(&inputs);
		if (ut_current_failed)
			return;
	}
}

/* Execution-owner authority is a controlled leaf here. Its selected ROOT,
 * anchor, checkpoint and entire retained WAL use the production readers.
 * The actual original-scope provider is tested by test_cluster_undo_recovery. */
static ClusterControlRootSnapshot execution_root;
static ClusterControlRootReadToken execution_token;
static unsigned execution_fault, execution_visits, execution_records;
typedef struct ExecutionCheckpointDelivery {
	ClusterWalRecoveryCheckpointV1 checkpoint;
	ClusterUndoRecoveryCheckpointVisitorV1 visitor;
	void *arg;
} ExecutionCheckpointDelivery;

static bool
execution_checkpoint_record(XLogReaderState *reader, void *arg)
{
	ExecutionCheckpointDelivery *work = arg;
	bool ok;
	execution_records++;
	UT_ASSERT(!inputs_pin_held && inputs_pin_refuse && test_actual_cf == NoLock);
	if (execution_fault == 9)
		ereport(ERROR, (errmsg("injected execution-owner WAL callback ERROR")));
	ok = work->visitor(&work->checkpoint, &execution_root, custody_scope, reader, work->arg);
	if (ok && execution_fault == 8 && execution_records == 1)
		v2_checkpoint_root_race();
	return ok;
}

bool
cluster_undo_recovery_cold_checkpoint_visit_v1(int origin,
											   ClusterUndoRecoveryCheckpointVisitorV1 visitor,
											   void *arg)
{
	ExecutionCheckpointDelivery work = { 0 };
	ClusterWalTailObservation observed = { 0 };
	execution_visits++;
	UT_ASSERT(!inputs_pin_held && inputs_pin_refuse);
	if (execution_fault == 1 || custody_scope == 0
		|| origin != execution_root.identity.origin_node_id || visitor == NULL)
		return false;
	if (cluster_control_root_recovery_checkpoint_v1(&execution_root, &execution_token,
													&work.checkpoint)
		!= CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return false;
	if (execution_fault == 4)
		work.checkpoint.checkpoint.nextXid = FullTransactionIdFromU64(101);
	if (execution_fault == 5)
		work.checkpoint.native_window[159] ^= 1;
	if (execution_fault == 6)
		work.checkpoint.source.timeline++;
	work.visitor = visitor;
	work.arg = arg;
	if (cluster_control_root_recovery_visit(&execution_root, &execution_token,
											execution_checkpoint_record, &work, &observed)
		!= CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return false;
	if (execution_fault == 2)
		return false; /* Late original owner refusal after all physical input. */
	if (execution_fault == 3)
		ereport(ERROR, (errmsg("injected execution-owner post-visit ERROR")));
	if (execution_fault == 7)
		MyBackendType = B_BG_WORKER;
	return true;
}

static void
execution_build_setup(unsigned input_fault, bool clean)
{
	ClusterWalInputsV1 *inputs = origin_build_setup(input_fault, clean);
	uint8 bytes[66048];
	ControlRootImage root;
	char path[MAXPGPATH];
	UT_ASSERT(inputs != NULL);
	cluster_wal_inputs_release_v1(&inputs);
	UT_ASSERT(!inputs_pin_held);
	inputs_pin_refuse = true; /* Execution may not reacquire pass-1 census pins. */
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, bytes, sizeof(bytes));
	UT_ASSERT_EQ(
		cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	root.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	memcpy(bytes, root.bytes, sizeof(bytes));
	v2_write_roots(bytes);
	UT_ASSERT_EQ(cluster_control_root_v3_read_canonical(1, &root.records[0].identity,
														&execution_root, &execution_token),
				 0);
	execution_fault = execution_visits = execution_records = 0;
}

static void
execution_build_done(void)
{
	ClusterWalInputsV1 *inputs = NULL;
	inputs_pin_refuse = false;
	origin_build_done(&inputs);
}

UT_TEST(test_execution_build_uses_original_inputs_without_census_read_scope)
{
	for (unsigned sample = 0; sample < 3; sample++) {
		ClusterRelmapRetentionDirectory out = { 0 };
		custody_era_offset = sample == 1 ? UINT64CONST(1) << 32 : 0;
		execution_build_setup(29, sample == 2);
		UT_ASSERT(cluster_relmap_recovery_retention_build_start_cold_v1(0, &out));
		UT_ASSERT_EQ(out.state, CLUSTER_RELMAP_RETENTION_BUILDING);
		UT_ASSERT_EQ(out.system_identifier, TEST_SYSID);
		UT_ASSERT_EQ(out.database_incarnation,
					 prefix_base_expected.source.claim.database_incarnation);
		UT_ASSERT_EQ(out.origin, 0);
		UT_ASSERT_EQ(out.generation, custody_scope);
		UT_ASSERT_EQ(out.native_floor, custody_era_offset + 80);
		UT_ASSERT_EQ(out.last_issued, custody_era_offset + 99);
		UT_ASSERT_EQ(cluster_relmap_retention_allocation_check(&out, out.last_issued + 1),
					 CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(execution_visits, 1);
		UT_ASSERT(execution_records > 0 && !inputs_pin_held && test_actual_cf == NoLock);
		UT_ASSERT_EQ(prefix_base_reads + custody_terminal_calls + custody_inventory_calls, 0);
		UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
		execution_build_done();
	}
}

UT_TEST(test_execution_build_refuses_wrong_role_and_original_owner)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterRelmapRetentionDirectory out = { 0 }, zero = { 0 };
		execution_build_setup(29, false);
		if (fault == 0)
			MyBackendType = B_BG_WORKER;
		if (fault == 1)
			inputs_native_recovery = false;
		if (fault == 2)
			CritSectionCount = 1;
		if (fault == 3)
			custody_scope = 0;
		if (fault == 4)
			execution_fault = 1;
		UT_ASSERT(
			!cluster_relmap_recovery_retention_build_start_cold_v1(fault == 5 ? 128 : 0, &out));
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		UT_ASSERT_EQ(execution_records + prefix_base_reads, 0);
		UT_ASSERT(!inputs_pin_held && test_actual_cf == NoLock);
		execution_build_done();
	}
}

UT_TEST(test_execution_build_validates_checkpoint_window_and_complete_suffix)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterRelmapRetentionDirectory out = { 0 }, zero = { 0 };
		execution_build_setup(fault == 0 ? 1 : fault == 1 ? 9 : fault == 2 ? 10 : 29, false);
		if (fault >= 3)
			execution_fault = fault + 1;
		UT_ASSERT(!cluster_relmap_recovery_retention_build_start_cold_v1(0, &out));
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		UT_ASSERT_EQ(prefix_base_reads + custody_terminal_calls, 0);
		UT_ASSERT(!inputs_pin_held && test_actual_cf == NoLock);
		execution_build_done();
	}
}

UT_TEST(test_execution_build_never_resets_old_storage)
{
	ClusterRelmapRetentionDirectory out = { 0 }, saved;
	execution_build_setup(29, false);
	for (unsigned fault = 0; fault < 4; fault++) {
		memset(&out, 0, sizeof(out));
		if (fault == 0)
			out.state = CLUSTER_RELMAP_RETENTION_BUILDING;
		if (fault == 1)
			out.generation = 17;
		if (fault == 2)
			out.pins[63].wal_end = 99;
		if (fault == 3)
			out.state = CLUSTER_RELMAP_RETENTION_COMPLETE;
		saved = out;
		UT_ASSERT(!cluster_relmap_recovery_retention_build_start_cold_v1(0, &out));
		UT_ASSERT_EQ(memcmp(&out, &saved, sizeof(out)), 0);
	}
	UT_ASSERT(!cluster_relmap_recovery_retention_build_start_cold_v1(0, NULL));
	{
		ClusterControlRootSnapshot before = execution_root;
		UT_ASSERT(
			!cluster_relmap_recovery_retention_build_start_cold_v1(0, (void *)&execution_root));
		UT_ASSERT_EQ(memcmp(&before, &execution_root, sizeof(before)), 0);
	}
	UT_ASSERT_EQ(execution_visits + execution_records, 0);
	UT_ASSERT(!inputs_pin_held && test_actual_cf == NoLock);
	execution_build_done();
}

UT_TEST(test_execution_build_late_refusal_and_error_publish_nothing)
{
	const unsigned faults[] = { 2, 3, 9 };
	for (unsigned i = 0; i < lengthof(faults); i++) {
		ClusterRelmapRetentionDirectory out = { 0 }, zero = { 0 };
		volatile bool threw = false;
		execution_build_setup(29, false);
		execution_fault = faults[i];
		test_capture_error_level = true;
		PG_TRY();
		{
			UT_ASSERT(!cluster_relmap_recovery_retention_build_start_cold_v1(0, &out));
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT_EQ(threw, faults[i] != 2);
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		UT_ASSERT((faults[i] == 9 ? execution_records == 1 : execution_records >= 3)
				  && custody_scope == 7);
		UT_ASSERT(!inputs_pin_held && test_actual_cf == NoLock);
		UT_ASSERT_EQ(prefix_base_reads + custody_terminal_calls, 0);
		execution_build_done();
	}
}
