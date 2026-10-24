/* Actual original-scope owner; ROOT checkpoint/WAL transport is controlled.
 * The composed retained-WAL fixture separately uses the real ROOT/readers.
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterWalRecoveryCheckpointV1 visit_checkpoint;
static unsigned checkpoint_fault, checkpoint_gets, checkpoint_visits, checkpoint_callbacks;
static uint64 checkpoint_first_serial;
static ClusterUndoRecoveryScopeV1 *checkpoint_scope;

ClusterControlRootResult
cluster_control_root_recovery_checkpoint_v1(const ClusterControlRootSnapshot *expected,
											const ClusterControlRootReadToken *selected,
											ClusterWalRecoveryCheckpointV1 *out)
{
	UT_ASSERT_EQ(memcmp(expected, &root, sizeof(root)), 0);
	UT_ASSERT_EQ(memcmp(selected, &token, sizeof(token)), 0);
	checkpoint_gets++;
	*out = visit_checkpoint;
	if (checkpoint_fault == 1 || (checkpoint_fault == 2 && checkpoint_gets == 2))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (checkpoint_fault == 3)
		authority_result = CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
	if (checkpoint_fault == 4)
		out->source.claim.claim_sha256[31] ^= 1;
	if (checkpoint_gets == 2) {
		if (checkpoint_fault == 5)
			out->native_window[159] ^= 1;
		if (checkpoint_fault == 6)
			out->checkpoint.nextXid = FullTransactionIdFromU64(101);
		if (checkpoint_fault == 7)
			root.tail_last_record_crc32c++;
		if (checkpoint_fault == 8)
			out->checkpoint_start++;
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_control_root_recovery_visit(const ClusterControlRootSnapshot *expected,
									const ClusterControlRootReadToken *selected,
									ClusterWalRecordVisitor visitor, void *arg,
									ClusterWalTailObservation *out)
{
	UT_ASSERT_EQ(memcmp(expected, &root, sizeof(root)), 0);
	UT_ASSERT_EQ(memcmp(selected, &token, sizeof(token)), 0);
	checkpoint_visits++;
	memset(out, 0, sizeof(*out));
	if (checkpoint_fault != 29) {
		for (unsigned i = 0; i < 2; i++) {
			if (!visitor(&terminal_reader, arg))
				return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			out->records++;
		}
	}
	out->complete_end = root.validated_tail_lsn_exclusive;
	out->last_record_start = root.tail_last_record_lsn;
	out->last_record_crc = root.tail_last_record_crc32c;
	out->database_incarnation = cold_source.claim.database_incarnation;
	if (checkpoint_fault == 20)
		out->complete_end++;
	if (checkpoint_fault == 21)
		out->last_record_start++;
	if (checkpoint_fault == 22)
		out->last_record_crc++;
	if (checkpoint_fault == 23)
		out->records++;
	if (checkpoint_fault == 24)
		out->database_incarnation++;
	if (checkpoint_fault == 25)
		root.tail_last_record_crc32c++;
	if (checkpoint_fault == 26)
		authority_result = CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
	return checkpoint_fault == 27 ? CLUSTER_CONTROL_ROOT_IO_ERROR : CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static void
checkpoint_setup(ClusterUndoRecoveryScopeV1 *scope)
{
	reset_authority();
	duty.origin_node_id = 2;
	root.identity = duty;
	root.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	root.tail_last_record_lsn = 400;
	root.tail_last_record_crc32c = 57;
	serial.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
	memset(&cold_source, 0, sizeof(cold_source));
	cold_source.claim.identity = duty;
	cold_source.claim.database_incarnation = 4;
	cold_source.claim.max_config_generation = 8;
	memset(cold_source.claim.claim_sha256, 0x55, 32);
	cold_source.timeline = 7;
	cold_source_fault = cold_source_reads = 0;
	memset(&visit_checkpoint, 0, sizeof(visit_checkpoint));
	visit_checkpoint.source = cold_source;
	visit_checkpoint.checkpoint_start = 300;
	visit_checkpoint.checkpoint.redo = 120;
	visit_checkpoint.checkpoint.ThisTimeLineID = 7;
	visit_checkpoint.checkpoint.nextXid = FullTransactionIdFromU64(100);
	memset(visit_checkpoint.native_window, 0x6a, sizeof(visit_checkpoint.native_window));
	checkpoint_fault = checkpoint_gets = checkpoint_visits = checkpoint_callbacks = 0;
	checkpoint_first_serial = 0;
	checkpoint_scope = scope;
	UT_ASSERT(cluster_undo_recovery_scope_enter_v1(scope, &authority, plan));
}

static bool
checkpoint_record(const ClusterWalRecoveryCheckpointV1 *checkpoint,
				  const ClusterControlRootSnapshot *selected, uint64 scope_serial,
				  XLogReaderState *reader, void *arg)
{
	UT_ASSERT(arg == checkpoint_scope && reader == &terminal_reader);
	UT_ASSERT_EQ(memcmp(checkpoint, &visit_checkpoint, sizeof(*checkpoint)), 0);
	UT_ASSERT_EQ(memcmp(selected, &root, sizeof(root)), 0);
	UT_ASSERT(scope_serial != 0);
	if (checkpoint_callbacks++ == 0)
		checkpoint_first_serial = scope_serial;
	else
		UT_ASSERT_EQ(scope_serial, checkpoint_first_serial);
	if (checkpoint_fault == 30)
		return false;
	if (checkpoint_fault == 31) {
		cluster_undo_recovery_scope_leave_v1(checkpoint_scope);
		UT_ASSERT(cluster_undo_recovery_scope_enter_v1(checkpoint_scope, &authority, plan));
	}
	if (checkpoint_fault == 32)
		cluster_undo_recovery_scope_leave_v1(checkpoint_scope);
	if (checkpoint_fault == 33)
		serial.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
	if (checkpoint_fault == 34)
		longjmp(failure_jump, 1);
	if (checkpoint_fault == 35)
		root.tail_last_record_crc32c++;
	return true;
}

UT_TEST(test_checkpoint_visit_uses_original_execution_owner)
{
	ClusterUndoRecoveryScopeV1 scope = { 0 };
	checkpoint_setup(&scope);
	UT_ASSERT(cluster_undo_recovery_cold_checkpoint_visit_v1(2, checkpoint_record, &scope));
	UT_ASSERT_EQ(checkpoint_gets, 2);
	UT_ASSERT_EQ(checkpoint_visits, 1);
	UT_ASSERT_EQ(checkpoint_callbacks, 2);
	UT_ASSERT(checkpoint_first_serial != 0);
	UT_ASSERT(cluster_undo_recovery_cold_origin_authorized_v1(2));
	UT_ASSERT(!scope.wal_synced && wal_sync_calls == 0 && path_calls == 0);
	UT_ASSERT_EQ(cluster_undo_recovery_cold_scope_id_v1(&cold_source, &root, 120),
				 checkpoint_first_serial);
	cluster_undo_recovery_scope_leave_v1(&scope);
}

UT_TEST(test_checkpoint_visit_refuses_missing_original_owner)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterUndoRecoveryScopeV1 scope = { 0 };
		checkpoint_setup(&scope);
		if (fault == 0)
			cluster_undo_recovery_scope_leave_v1(&scope);
		if (fault == 1)
			serial.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		if (fault == 2)
			recovery_purpose = false;
		if (fault == 3)
			authority_result = CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
		UT_ASSERT(!cluster_undo_recovery_cold_checkpoint_visit_v1(
			fault == 4	 ? 1
			: fault == 5 ? -1
			: fault == 6 ? 128
						 : 2,
			fault == 7 ? NULL : checkpoint_record, &scope));
		UT_ASSERT_EQ(checkpoint_gets + checkpoint_visits + checkpoint_callbacks, 0);
		cluster_undo_recovery_scope_leave_v1(&scope);
	}
}

UT_TEST(test_checkpoint_visit_refuses_changed_metadata_and_sealed_cut)
{
	const unsigned faults[] = { 1, 2, 3, 4, 5, 6, 7, 8, 20, 21, 22, 23, 24, 25, 26, 27, 29 };
	for (unsigned i = 0; i < lengthof(faults); i++) {
		ClusterUndoRecoveryScopeV1 scope = { 0 };
		checkpoint_setup(&scope);
		checkpoint_fault = faults[i];
		UT_ASSERT(!cluster_undo_recovery_cold_checkpoint_visit_v1(2, checkpoint_record, &scope));
		if (faults[i] <= 4 && faults[i] != 2)
			UT_ASSERT_EQ(checkpoint_visits + checkpoint_callbacks, 0);
		else
			UT_ASSERT_EQ(checkpoint_visits, 1);
		UT_ASSERT_EQ(wal_sync_calls + path_calls, 0);
		cluster_undo_recovery_scope_leave_v1(&scope);
	}
}

UT_TEST(test_checkpoint_visit_refuses_callback_and_same_address_reentry)
{
	const unsigned faults[] = { 30, 31, 32, 33, 35 };
	for (unsigned i = 0; i < lengthof(faults); i++) {
		ClusterUndoRecoveryScopeV1 scope = { 0 };
		checkpoint_setup(&scope);
		checkpoint_fault = faults[i];
		UT_ASSERT(!cluster_undo_recovery_cold_checkpoint_visit_v1(2, checkpoint_record, &scope));
		UT_ASSERT(checkpoint_callbacks >= 1);
		UT_ASSERT_EQ(wal_sync_calls + path_calls, 0);
		cluster_undo_recovery_scope_leave_v1(&scope);
	}
}

UT_TEST(test_checkpoint_visit_error_keeps_original_owner)
{
	ClusterUndoRecoveryScopeV1 scope = { 0 };
	volatile bool threw = false;
	checkpoint_setup(&scope);
	checkpoint_fault = 34;
	if (setjmp(failure_jump) == 0)
		(void)cluster_undo_recovery_cold_checkpoint_visit_v1(2, checkpoint_record, &scope);
	else
		threw = true;
	UT_ASSERT(threw && checkpoint_callbacks == 1);
	UT_ASSERT(cluster_undo_recovery_cold_origin_authorized_v1(2));
	UT_ASSERT(!scope.wal_synced && wal_sync_calls == 0 && path_calls == 0);
	cluster_undo_recovery_scope_leave_v1(&scope);
}
