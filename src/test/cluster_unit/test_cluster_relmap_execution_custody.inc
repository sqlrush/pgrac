/* Actual ROOT/anchor/claim/native WAL and scanner. Original cold owner,
 * single-source plan and native CLOG I/O remain controlled leaf boundaries.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned execution_pending_guard_fault;

bool
cluster_undo_recovery_cold_single_source_v1(const ClusterWalSourceRef *source,
											const ClusterControlRootSnapshot *checkpoint,
											XLogRecPtr native_redo, uint64 serial)
{
	ClusterWalSourceRef observed;
	XLogRecPtr redo;
	UT_ASSERT(!inputs_pin_held && inputs_pin_refuse);
	if (execution_pending_guard_fault == 3 && prefix_base_reads > 0)
		ereport(ERROR, (errmsg("injected execution single-source ERROR")));
	return execution_pending_guard_fault != 1
		   && !(execution_pending_guard_fault == 2 && prefix_base_reads > 0) && serial != 0
		   && cluster_undo_recovery_cold_scope_id_v1(source, checkpoint, native_redo) == serial
		   && cluster_control_root_recovery_source_v1(&execution_root, &execution_token, &observed,
													  &redo)
				  == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		   && memcmp(&observed, source, sizeof(observed)) == 0 && redo == native_redo;
}

static void
execution_pending_setup(bool shared, bool aborted, unsigned source_fault)
{
	ClusterWalInputsV1 *inputs = custody_setup(shared, aborted);
	if (source_fault != 0) {
		cluster_wal_inputs_release_v1(&inputs);
		inputs = terminal_prefix_fixture(aborted, source_fault);
		prefix_base_expected = *cluster_wal_inputs_at_v1(inputs, 0);
	}
	execution_inputs_from_read_scope(&inputs);
	execution_pending_guard_fault = 0;
}

UT_TEST(test_execution_pending_original_source_and_native_terminal_read)
{
	for (unsigned sample = 0; sample < 8; sample++) {
		ClusterRelmapRecoveryHandle *handle = NULL;
		ClusterRelmapRecoveryTerminalV1 terminal;
		ClusterRelmapV2View view;
		bool aborted = (sample & 2) != 0;
		custody_era_offset = sample & 4 ? UINT64CONST(1) << 32 : 0;
		custody_extra_terminal = sample == 7;
		execution_pending_setup((sample & 1) != 0, aborted, 0);
		view = custody_view(sample % 3 == 1);
		UT_ASSERT(cluster_relmap_recovery_prepare_cold_v1(&custody_image.identity, &view, &handle));
		UT_ASSERT(handle != NULL && !inputs_pin_held && execution_records >= 4);
		UT_ASSERT_EQ(prefix_base_reads, 1);
		UT_ASSERT(cluster_relmap_recovery_terminal_read(handle, &custody_image.identity, &view,
														&terminal));
		UT_ASSERT_EQ(prefix_base_reads, 2);
		UT_ASSERT_EQ(terminal.status,
					 aborted ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
		UT_ASSERT_EQ(terminal.top_full_xid, custody_era_offset + 99);
		UT_ASSERT_EQ(terminal.bounds.native_floor, custody_era_offset + 80);
		UT_ASSERT_EQ(terminal.bounds.last_issued, custody_era_offset + 99);
		UT_ASSERT_EQ(terminal.bounds.origin, 0);
		UT_ASSERT_EQ(terminal.source_cut, execution_root.validated_tail_lsn_exclusive);
		UT_ASSERT_EQ(custody_terminal_calls, custody_inventory_expected);
		UT_ASSERT(!inputs_pin_held && custody_scope == 7 && test_actual_cf == NoLock);
		UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
		cluster_relmap_recovery_handle_release_v1(&handle);
		UT_ASSERT(handle == NULL && !inputs_pin_held);
		execution_build_done();
	}
}

UT_TEST(test_execution_pending_requires_original_owner_and_single_source)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterRelmapRecoveryHandle *handle = (void *)1;
		ClusterRelmapV2View view;
		execution_pending_setup(false, false, 0);
		view = custody_view(false);
		if (fault == 0)
			MyBackendType = B_BG_WORKER;
		if (fault == 1)
			inputs_native_recovery = false;
		if (fault == 2)
			CritSectionCount = 1;
		if (fault == 3)
			custody_scope = 0;
		if (fault == 4)
			execution_pending_guard_fault = 1;
		if (fault == 5)
			execution_fault = 2;
		UT_ASSERT(
			!cluster_relmap_recovery_prepare_cold_v1(&custody_image.identity, &view, &handle));
		UT_ASSERT(handle == NULL && !inputs_pin_held && test_actual_cf == NoLock);
		UT_ASSERT_EQ(prefix_base_reads, 0);
		execution_build_done();
	}
}

UT_TEST(test_execution_pending_refuses_wrong_complete_view_and_era)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		ClusterRelmapRecoveryHandle *handle = NULL;
		ClusterRelmapV2View view;
		ClusterRelmapV2Namespace identity;
		execution_pending_setup(false, false, 0);
		view = custody_view(false);
		identity = custody_image.identity;
		if (fault == 0)
			view.pending_complete = false;
		if (fault == 1)
			view.used_backup = true;
		if (fault == 2)
			view.pending_owner.membership_epoch++;
		if (fault == 3)
			view.pending_owner.boot_incarnation++;
		if (fault == 4)
			view.pending_owner.admitted_incarnation++;
		if (fault == 5)
			view.pending_owner.top_full_xid += UINT64CONST(1) << 32;
		if (fault == 6)
			view.pending_owner.wal_end++;
		if (fault == 7)
			identity.system_identifier++;
		if (fault == 8)
			identity.database_incarnation++;
		UT_ASSERT(!cluster_relmap_recovery_prepare_cold_v1(&identity, &view, &handle));
		UT_ASSERT(handle == NULL && !inputs_pin_held && test_actual_cf == NoLock);
		UT_ASSERT_EQ(prefix_base_reads, 0);
		execution_build_done();
	}
}

UT_TEST(test_execution_pending_keeps_native_inventory_and_lifetime_gates)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		ClusterRelmapRecoveryHandle *handle = NULL;
		ClusterRelmapV2View view;
		execution_pending_setup(false, false, fault == 8 ? 31 : fault == 9 ? 32 : 0);
		view = custody_view(false);
		if (fault <= 4)
			custody_inventory_fault = fault + 1;
		if (fault == 5)
			custody_terminal_fault = 1;
		if (fault == 6)
			prefix_base_fault = 2;
		if (fault == 7)
			prefix_base_fault = 5;
		UT_ASSERT(
			!cluster_relmap_recovery_prepare_cold_v1(&custody_image.identity, &view, &handle));
		UT_ASSERT(handle == NULL && !inputs_pin_held && test_actual_cf == NoLock);
		UT_ASSERT_EQ(prefix_base_reads, fault == 6 || fault == 7 ? 1 : 0);
		execution_build_done();
	}
}

UT_TEST(test_execution_pending_drift_is_sticky_and_keeps_original_owner)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterRelmapRecoveryHandle *handle = NULL;
		ClusterRelmapV2View view;
		ClusterRelmapRecoveryTerminalV1 out, zero = { 0 };
		execution_pending_setup(false, false, 0);
		view = custody_view(false);
		UT_ASSERT(cluster_relmap_recovery_prepare_cold_v1(&custody_image.identity, &view, &handle));
		if (fault == 0)
			custody_scope++;
		if (fault == 1)
			v2_checkpoint_root_race();
		if (fault == 2)
			prefix_base_last_byte++;
		if (fault == 3)
			execution_pending_guard_fault = 1;
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(
			!cluster_relmap_recovery_terminal_read(handle, &custody_image.identity, &view, &out));
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		if (fault == 0)
			custody_scope--;
		if (fault == 2)
			prefix_base_last_byte--;
		execution_pending_guard_fault = 0;
		UT_ASSERT(
			!cluster_relmap_recovery_handle_matches_v1(handle, &custody_image.identity, &view));
		UT_ASSERT(!inputs_pin_held && test_actual_cf == NoLock);
		cluster_relmap_recovery_handle_release_v1(&handle);
		execution_build_done();
	}
}

UT_TEST(test_execution_pending_error_releases_only_local_custody)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterRelmapRecoveryHandle *handle = NULL;
		ClusterRelmapV2View view;
		volatile bool threw = false;
		execution_pending_setup(false, false, 0);
		view = custody_view(false);
		if (fault == 0)
			execution_fault = 9;
		if (fault == 1)
			execution_fault = 3;
		if (fault == 2)
			custody_inventory_fault = 8;
		if (fault == 3)
			custody_terminal_fault = 2;
		if (fault == 4)
			prefix_base_fault = 8;
		if (fault == 5)
			execution_pending_guard_fault = 3;
		test_capture_error_level = true;
		PG_TRY();
		{
			(void)cluster_relmap_recovery_prepare_cold_v1(&custody_image.identity, &view, &handle);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT(threw && handle == NULL && custody_scope == 7);
		UT_ASSERT(!inputs_pin_held && test_actual_cf == NoLock);
		execution_build_done();
	}
}

UT_TEST(test_execution_pending_alias_refusal_preserves_inputs)
{
	ClusterRelmapV2View view, before;
	ClusterRelmapV2Namespace identity, saved;
	execution_pending_setup(false, false, 0);
	view = before = custody_view(false);
	identity = saved = custody_image.identity;
	UT_ASSERT(!cluster_relmap_recovery_prepare_cold_v1(&identity, &view, (void *)&identity));
	UT_ASSERT(!cluster_relmap_recovery_prepare_cold_v1(&identity, &view, (void *)&view));
	UT_ASSERT(!cluster_relmap_recovery_prepare_cold_v1(&identity, &view, NULL));
	UT_ASSERT_EQ(memcmp(&identity, &saved, sizeof(saved)), 0);
	UT_ASSERT_EQ(memcmp(&view, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(execution_visits + prefix_base_reads, 0);
	UT_ASSERT(!inputs_pin_held && test_actual_cf == NoLock);
	execution_build_done();
}

UT_TEST(test_custody_source_observes_original_tuple_without_native_page_io)
{
	for (unsigned execution = 0; execution < 2; execution++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRelmapRecoveryHandle *handle = NULL;
		ClusterRelmapRecoverySourceObservationV1 out;
		ClusterRelmapV2View view;
		if (execution)
			execution_pending_setup(true, false, 0);
		else
			inputs = custody_setup(false, true);
		view = custody_view(true);
		UT_ASSERT(execution ? cluster_relmap_recovery_prepare_cold_v1(&custody_image.identity,
																	  &view, &handle)
							: cluster_relmap_recovery_prepare_v1(inputs, &custody_image.identity,
																 &view, &handle));
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(cluster_relmap_recovery_handle_source_v1(handle, &out));
		UT_ASSERT_EQ(memcmp(&out.source, &prefix_base_expected.source, sizeof(out.source)), 0);
		UT_ASSERT_EQ(
			memcmp(&out.checkpoint, &prefix_base_expected.checkpoint, sizeof(out.checkpoint)), 0);
		UT_ASSERT_EQ(out.native_redo, prefix_base_expected.native_redo);
		UT_ASSERT_EQ(out.scope_serial, 7);
		UT_ASSERT_EQ(prefix_base_reads, 1);
		UT_ASSERT_EQ(inputs_pin_held, !execution);
		cluster_relmap_recovery_handle_release_v1(&handle);
		if (execution)
			execution_build_done();
		else
			custody_done(&inputs);
	}
}

UT_TEST(test_custody_source_refusal_and_error_are_sticky)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterRelmapRecoveryHandle *handle = NULL;
		ClusterRelmapRecoverySourceObservationV1 out, zero = { 0 };
		ClusterRelmapV2View view;
		volatile bool threw = false;
		execution_pending_setup(false, false, 0);
		view = custody_view(false);
		UT_ASSERT(cluster_relmap_recovery_prepare_cold_v1(&custody_image.identity, &view, &handle));
		if (fault == 0)
			custody_scope++;
		if (fault == 1)
			v2_checkpoint_root_race();
		if (fault == 2)
			execution_pending_guard_fault = 1;
		if (fault == 3)
			CritSectionCount = 1;
		if (fault == 4)
			execution_pending_guard_fault = 3;
		memset(&out, 0xa5, sizeof(out));
		test_capture_error_level = true;
		PG_TRY();
		{
			UT_ASSERT(!cluster_relmap_recovery_handle_source_v1(handle, &out));
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT_EQ(threw, fault == 4);
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		custody_scope = 7;
		CritSectionCount = execution_pending_guard_fault = 0;
		UT_ASSERT(!cluster_relmap_recovery_handle_source_v1(handle, &out));
		UT_ASSERT(
			!cluster_relmap_recovery_handle_matches_v1(handle, &custody_image.identity, &view));
		UT_ASSERT_EQ(prefix_base_reads, 1);
		UT_ASSERT(!inputs_pin_held && test_actual_cf == NoLock);
		cluster_relmap_recovery_handle_release_v1(&handle);
		execution_build_done();
	}
}

UT_TEST(test_custody_source_alias_refusal_keeps_handle_usable)
{
	ClusterRelmapRecoveryHandle *handle = NULL;
	ClusterRelmapRecoverySourceObservationV1 out, before, zero = { 0 };
	ClusterRelmapV2View view;
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_relmap_recovery_handle_source_v1(NULL, &out));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	execution_pending_setup(false, false, 0);
	view = custody_view(false);
	UT_ASSERT(cluster_relmap_recovery_prepare_cold_v1(&custody_image.identity, &view, &handle));
	memcpy(&before, handle, sizeof(before));
	UT_ASSERT(!cluster_relmap_recovery_handle_source_v1(handle, (void *)handle));
	UT_ASSERT(!cluster_relmap_recovery_handle_source_v1(handle, NULL));
	UT_ASSERT_EQ(memcmp(&before, handle, sizeof(before)), 0);
	UT_ASSERT(cluster_relmap_recovery_handle_source_v1(handle, &out));
	UT_ASSERT_EQ(prefix_base_reads, 1);
	cluster_relmap_recovery_handle_release_v1(&handle);
	execution_build_done();
}
