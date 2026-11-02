/* Original typed frame/source checks; H pin/CF boundary is controlled. */
/* The barrier consumes the independently captured control selection too.
 * Keep these source/frame tests on an exact legacy control leaf; the real
 * managed prepare/delta fixture covers the complete registered table. */
static ClusterColdTypedV1 *
create_barrier_fixture(void)
{
	ClusterColdTypedV1 *typed = create_record_fixture();
	ClusterColdNativeRegistryV2 *saved = palloc0(sizeof(*saved));
	registry_managed = false; registry_cf_held = true;
	registry_ok = true; registry_error = registry_drift = false;
	UT_ASSERT_EQ(cluster_control_root_relmap_format_locked_v2(
		sizeof(*saved), &saved->format), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	saved->bytes = sizeof(*saved); typed->native_registry = saved;
	return typed;
}

static bool
create_barrier_record(ClusterColdTypedV1 *typed, uint64 id, uint32 p, bool held)
{
	ClusterRecoveryFencePlan *f = (ClusterRecoveryFencePlan *)&fixture_context;
	const ClusterColdParticipantV1 *c = &typed->participants[p];
	create_barrier_probe_active = true;
	return held ? cluster_cold_typed_relmap_create_barrier_record_held_v2(
		typed, f, id, p, &typed->sources[p], c->native_redo, c->tail_end)
		: cluster_cold_typed_relmap_create_barrier_record_current_v2(
		typed, f, id, p, &typed->sources[p], c->native_redo, c->tail_end);
}
static void
create_barrier_finish(ClusterColdTypedV1 **typed)
{
	create_barrier_probe_active = false;
	create_barrier_probe_lock = NULL;
	create_barrier_suspended = false;
	create_barrier_error_after = 0;
	create_record_destroy(typed);
	registry_cf_held = false;
}
UT_TEST(test_create_barrier_matches_original_frames_without_old_serial_grant)
{
	ClusterColdTypedV1 *typed = create_barrier_fixture();
	create_barrier_suspended = true;
	control_borrow_ok = c0_target_borrow = false;
	UT_ASSERT(!cluster_recovery_merge_fence_plan_revalidate_nowait(
		(ClusterRecoveryFencePlan *)&fixture_context));
	for (uint32 p = 0; p < typed->replay_count; ++p)
		UT_ASSERT(create_barrier_record(typed, 73, p, false));
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	create_barrier_finish(&typed);
}
UT_TEST(test_create_barrier_held_checks_do_not_perform_profile_or_root_reads)
{
	ClusterColdTypedV1 *typed = create_barrier_fixture();
	unsigned roots_before = pass2_root_reads, profiles = commit_ts_profile_calls[0];
	unsigned full = create_barrier_full_checks;
	create_barrier_suspended = true;
	for (uint32 p = 0; p < typed->replay_count; ++p)
		UT_ASSERT(create_barrier_record(typed, 73, p, true));
	UT_ASSERT_EQ(pass2_root_reads, roots_before);
	UT_ASSERT_EQ(commit_ts_profile_calls[0], profiles);
	UT_ASSERT_EQ(create_barrier_full_checks, full);
	create_barrier_finish(&typed);
}
UT_TEST(test_create_barrier_wrong_episode_and_whole_source_drift_stick)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		ClusterColdTypedV1 *typed = create_barrier_fixture();
		create_barrier_suspended = true;
		if (fault == 1) typed->sources[0].claim.claim_sha256[0] ^= 1;
		if (fault == 2) typed->participants[1].tail_end++;
		if (fault == 3) fence_current = false;
		UT_ASSERT(!create_barrier_record(typed, fault == 0 ? 74 : 73, 1, false));
		UT_ASSERT_NE(typed->refusal, CLUSTER_COLD_OK);
		fence_current = true;
		UT_ASSERT(!create_barrier_record(typed, 73, 1, false));
		create_barrier_finish(&typed);
	}
}
UT_TEST(test_create_barrier_current_error_revokes_original_episode)
{
	ClusterColdTypedV1 *typed = create_barrier_fixture();
	volatile bool caught = false;
	create_barrier_suspended = true;
	create_barrier_error_after = 2;
	PG_TRY(); { (void)create_barrier_record(typed, 73, 0, false); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_NE(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT(!create_barrier_record(typed, 73, 0, false));
	create_barrier_finish(&typed);
}
UT_TEST(test_create_barrier_full_current_refuses_family_lock_before_root_io)
{
	ClusterColdTypedV1 *typed = create_barrier_fixture();
	unsigned checks = create_barrier_full_checks;
	create_barrier_suspended = true;
	create_barrier_probe_lock = RelationMappingLock;
	UT_ASSERT(!create_barrier_record(typed, 73, 0, false));
	UT_ASSERT_EQ(create_barrier_full_checks, checks);
	UT_ASSERT_NE(typed->refusal, CLUSTER_COLD_OK);
	create_barrier_finish(&typed);
}
