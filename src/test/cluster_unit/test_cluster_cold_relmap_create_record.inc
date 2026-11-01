/* Original Startup/plan/whole-source owner; decoded frame and fence/profile
 * providers remain controlled. No RMC1 decoder/live rmgr admission is claimed. */
static ClusterColdTypedV1 *
create_record_fixture(void)
{
	ClusterColdTypedV1 *typed;
	scan_create_frame = true;
	typed = endpoint_fixture(true);
	scan_create_frame = false;
	restart_ref = typed->sources[0];
	control_borrow_ok = control_authority_ok = control_cover_ok = true;
	control_duty = roots[2].identity;
	control_root = roots[2];
	control_token = tokens[2];
	memset(&control_serial, 0, sizeof(control_serial));
	control_serial.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
	c0_mode = c0_target_borrow = c0_target_current = c0_target_cover = true;
	c0_duty = roots[3].identity;
	c0_root = roots[3];
	c0_token = tokens[3];
	c0_serial = control_serial;
	MyBackendType = B_STARTUP;
	InRecovery = true;
	return typed;
}
static void
create_record_destroy(ClusterColdTypedV1 **typed)
{
	endpoint_destroy(typed);
	MyBackendType = B_INVALID;
	InRecovery = false;
	scan_create_frame = false;
	create_record_fence_error_after = 0;
}
static bool
create_record_current(ClusterColdTypedV1 *typed, uint32 p, bool held)
{
	const ClusterColdParticipantV1 *c = &typed->participants[p];
	ClusterRecoveryFencePlan *f = (ClusterRecoveryFencePlan *)&fixture_context;
	return held ? cluster_cold_typed_relmap_create_record_held_v2(typed, f, p, &typed->sources[p],
																  c->native_redo, c->tail_end)
				: cluster_cold_typed_relmap_create_record_current_v2(
					  typed, f, p, &typed->sources[p], c->native_redo, c->tail_end);
}
UT_TEST(test_recovery_create_record_borrows_original_whole_episode)
{
	ClusterColdTypedV1 *typed = create_record_fixture();
	unsigned reads = pass2_root_reads, profiles = commit_ts_profile_calls[0];
	for (uint32 p = 0; p < typed->replay_count; p++)
		UT_ASSERT(create_record_current(typed, p, false));
	profiles = commit_ts_profile_calls[0];
	for (uint32 p = 0; p < typed->replay_count; p++)
		UT_ASSERT(create_record_current(typed, p, true));
	UT_ASSERT_EQ(commit_ts_profile_calls[0], profiles);
	UT_ASSERT_EQ(pass2_root_reads, reads);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	create_record_destroy(&typed);
}
UT_TEST(test_recovery_create_record_refuses_positions_source_and_lost_protection)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterColdTypedV1 *typed = create_record_fixture();
		ClusterRecoveryFencePlan *f = (ClusterRecoveryFencePlan *)&fixture_context;
		ClusterWalSourceRef source = typed->sources[1];
		XLogRecPtr read = typed->participants[1].native_redo;
		XLogRecPtr end = typed->participants[1].tail_end;
		if (fault == 0)
			read++;
		if (fault == 1)
			end--;
		if (fault == 2)
			source.claim.claim_sha256[0] ^= 1;
		if (fault == 3)
			fence_current = false;
		if (fault == 4)
			typed->census_root_captured = false;
		if (fault == 5)
			restart_ref_present = false;
		if (fault == 6)
			cluster_cold_replay_window_leave_v1();
		UT_ASSERT(!cluster_cold_typed_relmap_create_record_current_v2(
			typed, f, fault == 5 ? 0 : 1, fault == 5 ? &typed->sources[0] : &source,
			fault == 5 ? typed->participants[0].native_redo : read,
			fault == 5 ? typed->participants[0].tail_end : end));
		UT_ASSERT_NE(typed->refusal, CLUSTER_COLD_OK);
		fence_current = restart_ref_present = true;
		UT_ASSERT(!create_record_current(typed, 1, true));
		create_record_destroy(&typed);
	}
}
UT_TEST(test_recovery_create_record_refuses_wrong_kind_or_history)
{
	ClusterColdTypedV1 *typed = endpoint_fixture(true); /* original page frame */
	MyBackendType = B_STARTUP;
	InRecovery = true;
	UT_ASSERT(!create_record_current(typed, 1, false));
	create_record_destroy(&typed);
	typed = create_record_fixture();
	UT_ASSERT(!create_record_current(typed, typed->replay_count, false));
	create_record_destroy(&typed);
}
UT_TEST(test_recovery_create_record_profile_error_poison_is_sticky)
{
	ClusterColdTypedV1 *typed = create_record_fixture();
	unsigned reads = pass2_root_reads;
	commit_ts_profile_fail_after[1] = 0;
	commit_ts_profile_result[1] = CLUSTER_CONTROL_ROOT_IO_ERROR;
	UT_ASSERT(!create_record_current(typed, 1, false));
	UT_ASSERT_NE(typed->refusal, CLUSTER_COLD_OK);
	commit_ts_profile_result[1] = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	UT_ASSERT(!create_record_current(typed, 1, false));
	UT_ASSERT_EQ(pass2_root_reads, reads);
	create_record_destroy(&typed);
}

/* Late ERROR must poison even after the frame itself matched. */
UT_TEST(test_recovery_create_record_final_fence_error_is_sticky)
{
	ClusterColdTypedV1 *typed = create_record_fixture();
	volatile bool caught = false;
	create_record_fence_error_after = 2;
	PG_TRY();
	{
		(void)create_record_current(typed, 0, true);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_NE(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT(!create_record_current(typed, 0, true));
	create_record_destroy(&typed);
}
