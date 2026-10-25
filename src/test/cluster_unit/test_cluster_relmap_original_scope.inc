/* Original scope and immutable SIDE plan, with controlled authority leaves.
 * Selected native capture is not full recovery/all-origin qualification.
 * Author: SqlRush <sqlrush@gmail.com> */
bool cluster_undo_gcs_coherence = true;
static ClusterUndoRecoveryScopeV1 cold_original_scope;
static ClusterThreadRecoveryAuthorityV1 cold_original_authority;
static ClusterRecoverySerialGuard cold_original_serial;
static ClusterRecoveryDutyKey cold_original_duty;
static RfSideOnlinePlanV1 *cold_original_plan;
static unsigned cold_original_fault, cold_original_checks, cold_original_resets;
static uint64 cold_original_previous_serial;

/* Existing minimal relmap terminals intentionally omit typed SIDE sections.
 * New scope cases write a bounded native SCN/TT payload into the original
 * WAL before any ROOT/source capture. Nothing is patched during plan feed. */
static size_t
cold_original_terminal_payload(bool aborted, uint16 thread, uint8 *out)
{
	TimestampTz timestamp = 123;
	xl_xact_xinfo info = { .xinfo = XACT_XINFO_HAS_SCN };
	xl_xact_scn scn = { .scn = 12345 };
	xl_xact_tt_commit tt = { .segment_id = 1,
							 .segment_generation = 11,
							 .xid = 99,
							 .slot_offset = 4,
							 .wrap = 7,
							 .instance = thread,
							 .format_version = CLUSTER_XACT_TT_COMMIT_VERSION,
							 .commit_scn = 12345 };
	size_t length = 0;
	if (!aborted)
		info.xinfo |= XACT_XINFO_HAS_TT_COMMIT;
	memcpy(out + length, &timestamp, sizeof(timestamp));
	length += sizeof(timestamp);
	memcpy(out + length, &info, sizeof(info));
	length += sizeof(info);
	memcpy(out + length, &scn, sizeof(scn));
	length += sizeof(scn);
	if (!aborted) {
		memcpy(out + length, &tt, sizeof(tt));
		length += sizeof(tt);
	}
	return length;
}

ClusterThreadRecoveryAuthorityResultV1
test_relmap_scope_authority(const ClusterThreadRecoveryAuthorityV1 *a)
{
	cold_original_checks++;
	return a == &cold_original_authority && a->serial_guard == &cold_original_serial
				   && a->serial_guard->held && !a->serial_guard->release_uncertain
				   && a->duty == &cold_original_duty && a->root_snapshot == &execution_root
				   && a->root_token == &execution_token && cold_original_fault == 0
			   ? CLUSTER_THREAD_AUTHORITY_OK
			   : CLUSTER_THREAD_AUTHORITY_PIN_STALE;
}

bool
test_relmap_scope_storage(ClusterProtectedSetIdentityV1 *out)
{
	memset(out, 0, sizeof(*out));
	out->backend_id = CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS;
	memcpy(out->storage_uuid, execution_root.identity.storage_uuid, 16);
	return true;
}

void
test_relmap_scope_cache_reset(void)
{
	cold_original_resets++;
}

static uint64
cold_original_id(void)
{
	return test_relmap_scope_id(&prefix_base_expected.source, &execution_root,
								prefix_base_expected.native_redo);
}

static void
cold_original_scope_setup(void)
{
	ClusterRelmapPendingSideSource input = { 0 };
	RfSideOnlinePlanRequestV1 request = { 0 };
	ClusterWalTailObservation observed = { 0 };
	RelmapRetainedSideFeed feed = { &input, 0, 0, &custody_image };
	XLogRecPtr redo = prefix_base_expected.native_redo;
	uint64 serial;
	UT_ASSERT(cold_original_plan == NULL);
	input.source = prefix_base_expected.source;
	input.cut.failed_thread = input.source.claim.identity.origin_thread_id;
	input.cut.origin_owner_incarnation = input.source.claim.identity.origin_owner_incarnation;
	input.cut.timeline_id = execution_root.checkpoint_tli;
	input.cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	input.cut.scan_begin_inclusive = execution_root.checkpoint_lower_lsn;
	input.cut.scan_end_exclusive = execution_root.validated_tail_lsn_exclusive;
	request.system_identifier = execution_root.identity.system_identifier;
	memcpy(request.storage_uuid, execution_root.identity.storage_uuid, 16);
	request.physical_cuts = &input.cut;
	request.redo_starts = &redo;
	request.participant_count = 1;
	request.purpose = RF_SIDE_PLAN_RECOVERY;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &cold_original_plan),
				 RF_PAGE_PROOF_DETAIL_OK);
	if (cold_original_plan == NULL)
		return;
	input.plan = cold_original_plan;
	UT_ASSERT(rf_side_online_plan_bind_sources_v1(cold_original_plan, &input.source, 1));
	/* Use the held execution ROOT reader; pass-1 pins may not be reacquired. */
	UT_ASSERT_EQ(cluster_control_root_recovery_visit(&execution_root, &execution_token,
													 relmap_retained_side_feed, &feed, &observed),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(observed.complete_end, input.cut.scan_end_exclusive);
	UT_ASSERT(rf_side_online_plan_bind_database_v1(cold_original_plan,
												   custody_image.identity.database_incarnation));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(cold_original_plan), RF_PAGE_PROOF_DETAIL_OK);
	cold_original_duty = execution_root.identity;
	memset(&cold_original_serial, 0, sizeof(cold_original_serial));
	cold_original_serial.held = true;
	cold_original_serial.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
	cold_original_serial.duty = cold_original_duty;
	cold_original_serial.root_read_token = execution_token;
	memset(&cold_original_authority, 0, sizeof(cold_original_authority));
	cold_original_authority.duty = &cold_original_duty;
	cold_original_authority.root_snapshot = &execution_root;
	cold_original_authority.root_token = &execution_token;
	cold_original_authority.serial_guard = &cold_original_serial;
	cold_original_fault = cold_original_checks = cold_original_resets = 0;
	UT_ASSERT(cluster_undo_recovery_scope_enter_v1(&cold_original_scope, &cold_original_authority,
												   cold_original_plan));
	serial = cold_original_id();
	UT_ASSERT(serial != 0 && serial > cold_original_previous_serial);
	cold_original_previous_serial = serial;
	UT_ASSERT_EQ(cold_original_resets, 1);
	UT_ASSERT(cluster_undo_recovery_cold_origin_authorized_v1(0));
	UT_ASSERT(!cluster_undo_recovery_cold_origin_authorized_v1(1));
	if (cold_native_real_clog)
		test_relmap_native_clog_authorize(cluster_undo_recovery_cold_origin_authorized_v1);
}

static void
cold_original_scope_done(void)
{
	/* Local descendant census/native descriptors are already gone. The
	 * original scope and plan outlive them, and retain their external debt. */
	cluster_undo_recovery_scope_leave_v1(&cold_original_scope);
	UT_ASSERT(cold_original_scope.authority == NULL && cold_original_scope.plan == NULL);
	UT_ASSERT(!cluster_undo_recovery_cold_origin_authorized_v1(0));
	UT_ASSERT(cold_original_resets >= 2 && cold_original_checks > 0);
	rf_side_online_plan_destroy_v1(&cold_original_plan);
	cold_original_fault = 0;
}

UT_TEST(test_cold_original_scope_native_commit_abort)
{
	cold_native_real_scope = cold_native_real_clog = true;
	test_cold_native_commit_abort_actual_pair_and_fresh_committed();
	cold_native_real_scope = cold_native_real_clog = false;
}

UT_TEST(test_cold_original_scope_reentry_rejects_old_cursor)
{
	ClusterRelmapRetentionCensus *census = NULL;
	ClusterRelmapCensusCommitted out;
	uint64 before, after;
	cold_native_real_scope = cold_native_real_clog = true;
	cold_native_setup(false, false, true);
	before = cold_original_id();
	UT_ASSERT(before != 0 && cold_native_execute(&census));
	if (census != NULL) {
		UT_ASSERT(cluster_relmap_retention_census_committed_cold(census, 42, &out));
		UT_ASSERT(out.present && out.generation == 7);
		cluster_undo_recovery_scope_leave_v1(&cold_original_scope);
		UT_ASSERT(cluster_undo_recovery_scope_enter_v1(
			&cold_original_scope, &cold_original_authority, cold_original_plan));
		after = cold_original_id();
		UT_ASSERT(after > before);
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(!cluster_relmap_retention_census_committed_cold(census, 42, &out));
		UT_ASSERT(!out.present && out.generation == 0);
		UT_ASSERT_EQ(cluster_relmap_retention_census_next(census),
					 CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		UT_ASSERT(fcntl(execution_root_fd, F_GETFD) >= 0);
		UT_ASSERT_EQ(cold_original_id(), after);
		UT_ASSERT(cold_original_serial.held && cold_original_plan != NULL);
	}
	UT_ASSERT_EQ(prefix_base_reads + cold_native_syncs + cold_native_invals + cold_native_acks, 0);
	cold_native_done(census);
	cold_native_real_scope = cold_native_real_clog = false;
}

UT_TEST(test_cold_original_scope_refuses_non_cold_or_stale_owner)
{
	cold_native_real_scope = cold_native_real_clog = true;
	for (unsigned sample = 0; sample < 5; sample++) {
		ClusterRelmapRetentionCensus *census = NULL;
		cold_native_setup(false, false, false);
		UT_ASSERT(cold_original_id() != 0);
		if (sample == 0)
			cold_original_serial.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		if (sample == 1)
			cold_original_scope.plan = NULL;
		if (sample == 2)
			cold_original_fault = 1;
		if (sample == 3)
			cold_original_serial.held = false;
		if (sample == 4)
			cold_original_serial.release_uncertain = true;
		UT_ASSERT(!cluster_undo_recovery_cold_origin_authorized_v1(0));
		UT_ASSERT_EQ(cold_original_id(), 0);
		UT_ASSERT(!cold_native_execute(&census));
		UT_ASSERT(census == NULL && cold_original_plan != NULL);
		UT_ASSERT_EQ(execution_root_opens + prefix_base_reads + cold_native_syncs
						 + cold_native_invals + cold_native_acks,
					 0);
		cold_native_done(census);
		if (ut_current_failed)
			break;
	}
	cold_native_real_scope = cold_native_real_clog = false;
}
