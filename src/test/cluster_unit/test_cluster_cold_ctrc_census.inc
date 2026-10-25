/* The original Startup owner, TX/CTRC components and finish boundary are
 * real. Full physical census and retained scope services are fixture leaves;
 * their original parser/IO has the separate native-input suite. */
static bool
observation_work_allocation(Size size, int flags)
{
	/* The original Start still requires ZERO|NO_OOM. Its new complete work
	 * copy deliberately overwrites all bytes and needs only NO_OOM. */
	return flags == MCXT_ALLOC_NO_OOM && info.cluster_tx_start != NULL
		&& ClusterColdRecoveryCtrcWorkCurrent(info.cluster_tx_start, &native_ctrc_work)
		&& size == offsetof(ClusterColdCtrcWorkV1, entries)
			+ (Size)native_ctrc_work.counts.subject_count * sizeof(native_ctrc_work.entries[0]);
}

static void
observation_event(char event)
{
	if (observation_event_count + 1 < sizeof(observation_events)) {
		observation_events[observation_event_count++] = event;
		observation_events[observation_event_count] = '\0';
	}
}

static void
observation_fixture_reset(void)
{
	if (observation_scope_live || observation_cursor_live) abort();
	observation_scope_begins = observation_census_begins = observation_steps = 0;
	observation_manifests = observation_census_destroys = observation_scope_releases = 0;
	observation_fault = observation_fault_at = observation_event_count = 0;
	memset(observation_events, 0, sizeof(observation_events));
	memset(&observation_manifest, 0, sizeof(observation_manifest));
	observation_manifest.encoding_version = 1;
	observation_manifest.source_count = 1;
	observation_manifest.root_count = 17;
	observation_manifest.tx_record_count = 23;
	observation_manifest.wal_record_count = 37;
	observation_manifest.native_effect_count = 5;
	/* Explicit unresolved observations must never become service COMPLETE. */
	observation_manifest.other_wal_record_count = 7;
	observation_manifest.unowned_target_record_count = 3;
	memset(observation_manifest.inputs_sha256, 1, 32);
	memset(observation_manifest.roots_sha256, 2, 32);
	memset(observation_manifest.observation_sha256, 3, 32);
}

ClusterControlRootResult
cluster_wal_inputs_begin_ctrc_census_v1(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, ClusterWalInputsV1 **out)
{
	if (!ClusterColdRecoveryCtrcWorkCurrent(start, work) || observation_scope_live
		|| window_region.checkpoint_owner.tx_accepted_generation == 0) abort();
	observation_scope_begins++;
	observation_event('S');
	if (observation_fault == 1) return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	observation_scope_live = true;
	*out = (ClusterWalInputsV1 *)&observation_scope_object;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterRecoveryCensusResult
cluster_recovery_census_begin_ctrc(ClusterWalInputsV1 *inputs, size_t memory_budget,
	ClusterRecoveryCensus **out)
{
	if (inputs != (ClusterWalInputsV1 *)&observation_scope_object || !observation_scope_live
		|| observation_cursor_live || memory_budget == 0) abort();
	observation_census_begins++;
	observation_event('B');
	if (observation_fault == 2) return CLUSTER_RECOVERY_CENSUS_CAPACITY;
	observation_cursor_live = true;
	*out = (ClusterRecoveryCensus *)&observation_cursor_object;
	return CLUSTER_RECOVERY_CENSUS_MORE;
}

ClusterRecoveryCensusResult
cluster_recovery_census_next(ClusterRecoveryCensus *cursor, ClusterRecoveryCensusEntry *out)
{
	if (cursor != (ClusterRecoveryCensus *)&observation_cursor_object
		|| !observation_cursor_live || !observation_scope_live || CritSectionCount != 0) abort();
	observation_steps++;
	observation_event('N');
	memset(out, 0, sizeof(*out));
	if (observation_fault == 3) return CLUSTER_RECOVERY_CENSUS_REFUSED;
	if (observation_fault == 4) pg_re_throw();
	if (observation_fault == 5 && observation_steps == 2) owner_boot++;
	if (observation_fault == 7 && observation_steps == 2) {
		switch (observation_fault_at) {
		case 0: native_ctrc_work.entries[0].slot.xid++; break;
		case 1: typed.census_root.file_txn_seq++; break;
		case 2: typed.census_root.image_sha256[0]++; break;
		case 3: typed.ctrc_work_generation++; break;
		case 4: native_ctrc_work.counts.subject_count++; break;
		case 5: window_region.checkpoint_owner.generation++; break;
		case 6: window_region.checkpoint_owner.tx_result.result_generation++; break;
		case 7: window_region.checkpoint_owner.tx_accepted_generation = 0; break;
		case 8: window_region.checkpoint_owner.owner.start_serial++; break;
		case 9: window_region.checkpoint_owner.tx_result.input_root_sha256[0]++; break;
		case 10: clusterStartupWriter.generation++; break;
		case 11: observed_writer.epoch++; break;
		case 12: native_ctrc_work.plan = NULL; break;
		case 13: typed.census_root_captured = false; break;
		case 15: native_ctrc_work.generation = ++typed.ctrc_work_generation; break;
		case 16: native_ctrc_work.counts.subject_count = 0; break;
		}
	}
	if (observation_steps == 1) return CLUSTER_RECOVERY_CENSUS_MORE;
	if (observation_steps == 2) {
		out->kind = CLUSTER_RECOVERY_CENSUS_TT_SLOT;
		return CLUSTER_RECOVERY_CENSUS_ENTRY;
	}
	return CLUSTER_RECOVERY_CENSUS_DRAINED;
}

bool
cluster_recovery_census_manifest(ClusterRecoveryCensus *cursor, ClusterRecoveryCensusManifest *out)
{
	if (cursor != (ClusterRecoveryCensus *)&observation_cursor_object
		|| !observation_cursor_live || !observation_scope_live || observation_steps < 3) abort();
	observation_manifests++;
	observation_event('M');
	if (observation_fault == 6) return false;
	*out = observation_manifest;
	if (observation_fault == 8 && observation_manifests == 2)
		out->roots_sha256[0]++;
	if (observation_fault == 9 && observation_manifests == 2)
		owner_boot++;
	return true;
}

bool
cluster_recovery_census_diag(const ClusterRecoveryCensus *cursor, ClusterRecoveryCensusDiag *out)
{
	if (cursor != (const ClusterRecoveryCensus *)&observation_cursor_object) abort();
	memset(out, 0, sizeof(*out));
	out->phase = CLUSTER_RECOVERY_CENSUS_TX;
	out->graph_result = CLUSTER_RECOVERY_GRAPH_PENDING;
	return true;
}

void
cluster_recovery_census_destroy(ClusterRecoveryCensus *cursor)
{
	if (cursor == NULL) return;
	if (cursor != (ClusterRecoveryCensus *)&observation_cursor_object
		|| !observation_cursor_live || !observation_scope_live) abort();
	observation_census_destroys++;
	observation_event('D');
	observation_cursor_live = false;
}

static void
observation_scope_release(void)
{
	if (!observation_scope_live || observation_cursor_live) abort();
	observation_scope_releases++;
	observation_event('R');
	observation_scope_live = false;
}

UT_TEST(test_actual_startup_observes_full_ctrc_input_before_independent_handoff)
{
	c0_subject();
	tx_wait_destroy_base = input_destroys;
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	serving_actor(); UT_ASSERT(cluster_recovery_tx_result_accept_v1()); startup_actor();
	actual_startup_finish_tx(&info);
	UT_ASSERT_EQ(observation_scope_begins, 1);
	UT_ASSERT_EQ(observation_census_begins, 1);
	UT_ASSERT_EQ(observation_steps, 3);
	UT_ASSERT(observation_manifests >= 2);
	UT_ASSERT_EQ(observation_census_destroys, 1);
	UT_ASSERT_EQ(observation_scope_releases, 1);
	UT_ASSERT(strstr(observation_events, "DR") != NULL);
	UT_ASSERT_EQ(input_destroys, tx_wait_destroy_base + 1);
	UT_ASSERT(info.cluster_tx_start == NULL && info.cluster_cold_inputs == NULL);
}

static void
observation_ready(void)
{
	c0_subject();
	ctrc_census.subject_count = ctrc_census.unresolved_count = 1;
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	native_ctrc_work.entries[0].slot.xid = 345;
	serving_actor(); UT_ASSERT(cluster_recovery_tx_result_accept_v1()); startup_actor();
}

UT_TEST(test_ctrc_observation_retains_original_cursor_and_never_completes_service)
{
	ClusterRecoveryCtrcResultValueV1 before;
	observation_ready();
	before = window_region.checkpoint_owner.ctrc_result;
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT(observation_cursor_live && observation_scope_live);
	UT_ASSERT_EQ(observation_steps, 3);
	UT_ASSERT_EQ(observation_census_destroys, 0);
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT_EQ(observation_scope_begins, 1);
	UT_ASSERT_EQ(observation_steps, 3);
	UT_ASSERT(memcmp(&before, &window_region.checkpoint_owner.ctrc_result, sizeof(before)) == 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	UT_ASSERT_EQ(observation_manifest.other_wal_record_count, 7);
	UT_ASSERT_EQ(observation_manifest.unowned_target_record_count, 3);
	release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(!observation_cursor_live && !observation_scope_live);
	UT_ASSERT(strstr(observation_events, "DR") != NULL);
	UT_ASSERT(info.cluster_tx_start != NULL && info.cluster_cold_inputs != NULL);
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_ctrc_observation_refusal_is_sticky_and_preserves_first_stage_and_results)
{
	const unsigned faults[] = {1, 2, 3, 6, 8, 9};
	const char *stages[] = {"scope_begin", "census_begin", "census_next", "manifest", "manifest_changed", "manifest_changed"};
	for (unsigned i = 0; i < lengthof(faults); i++) {
		ClusterRecoveryCtrcResultValueV1 before;
		unsigned begins;
		char first[512];
		observation_ready(); before = window_region.checkpoint_owner.ctrc_result;
		observation_fault = faults[i];
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		strlcpy(first, ClusterColdRecoveryCtrcObservationRefusal(info.cluster_tx_start), sizeof(first));
		UT_ASSERT(strstr(first, stages[i]) != NULL);
		begins = observation_scope_begins;
		observation_fault = 0;
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		UT_ASSERT_EQ(observation_scope_begins, begins);
		UT_ASSERT(strcmp(first, ClusterColdRecoveryCtrcObservationRefusal(info.cluster_tx_start)) == 0);
		UT_ASSERT(memcmp(&before, &window_region.checkpoint_owner.ctrc_result, sizeof(before)) == 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		ClusterReleaseColdRecoveryInputs(&info);
	}
}

UT_TEST(test_ctrc_observation_rejects_original_work_root_owner_and_tx_drift_mid_census)
{
	for (unsigned fault = 0; fault < 17; fault++) {
		observation_ready();
		observation_fault = fault == 14 ? 5 : 7;
		observation_fault_at = fault;
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		UT_ASSERT_EQ(observation_steps, 2);
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT_EQ(observation_manifests, 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_result.state, CLUSTER_RECOVERY_CTRC_RESULT_PENDING);
		release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		ClusterReleaseColdRecoveryInputs(&info);
	}
}

UT_TEST(test_ctrc_observation_propagates_native_error_after_child_before_scope_cleanup)
{
	ClusterRecoveryCtrcResultValueV1 before;
	volatile bool caught = false;
	observation_ready(); before = window_region.checkpoint_owner.ctrc_result;
	observation_fault = 4;
	PG_TRY();
	{
		(void)ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(!observation_cursor_live && !observation_scope_live);
	UT_ASSERT(strstr(observation_events, "NDR") != NULL);
	UT_ASSERT(memcmp(&before, &window_region.checkpoint_owner.ctrc_result, sizeof(before)) == 0);
	UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_ctrc_observation_requires_accepted_tx_and_reserves_budget_before_scope)
{
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(NULL, 0));
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve((const struct ClusterRecoveryStartV1 *)&typed, 0));
	for (unsigned fault = 0; fault < 5; fault++) {
		Size budget = 2 * 1024 * 1024;
		observation_ready();
		if (fault == 0) budget = 1;
		if (fault == 1) budget = (Size)MaxAllocSize + 1;
		if (fault == 2) allocation_ok = false;
		if (fault == 3) window_region.checkpoint_owner.tx_accepted_generation = 0;
		if (fault == 4) native_ctrc_work.counts.subject_count = UINT32_MAX;
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, budget));
		UT_ASSERT_EQ(observation_scope_begins, 0);
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		allocation_ok = true;
		release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		ClusterReleaseColdRecoveryInputs(&info);
	}
}

UT_TEST(test_ctrc_observation_process_cleanup_retires_children_once_without_handoff)
{
	unsigned destroyed;
	observation_ready(); destroyed = input_destroys;
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	exit_callback(1, (Datum)0);
	UT_ASSERT_EQ(observation_census_destroys, 1);
	UT_ASSERT_EQ(observation_scope_releases, 1);
	UT_ASSERT_EQ(input_destroys, destroyed + 1);
	UT_ASSERT(!window_region.checkpoint_owner.valid);
	UT_ASSERT(strstr(observation_events, "DR") != NULL);
	info.cluster_tx_start = NULL;
	info.cluster_cold_inputs = NULL;
}
