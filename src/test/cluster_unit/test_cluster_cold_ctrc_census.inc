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


static union {
	uint64 align;
	uint8 bytes[offsetof(ClusterRecoveryCensusPagesV1, entries)
		+ 2 * sizeof(ClusterRecoveryCensusPageV1)];
} observation_pages_storage[2];
#define observation_pages (*(ClusterRecoveryCensusPagesV1 *)observation_pages_storage[0].bytes)
static unsigned observation_page_fault;
static unsigned observation_page_wal_fault;
static uint32 observation_page_wal_count;
static XLogReaderState observation_page_wal_readers[2];

bool
cluster_recovery_census_page_wal_count_v1(ClusterRecoveryCensus *cursor, uint32 *out)
{
	if (cursor != (ClusterRecoveryCensus *)&observation_cursor_object
		|| !observation_cursor_live || !observation_scope_live || observation_steps < 3) abort();
	*out = 0;
	if (observation_page_wal_fault == 1) return false;
	if (observation_page_wal_fault == 3) pg_re_throw();
	*out = observation_page_wal_count + (observation_page_wal_fault == 2 ? 1 : 0);
	return true;
}

bool
cluster_recovery_census_page_wal_at_v1(ClusterRecoveryCensus *cursor, uint32 index,
	ClusterRecoveryPageWalV1 *out)
{
	uint32 count;
	memset(out, 0, sizeof(*out));
	if (!cluster_recovery_census_page_wal_count_v1(cursor, &count) || index >= count || index >= 2)
		return false;
	if (observation_page_wal_fault == 4) return false;
	if (observation_page_wal_fault == 5) pg_re_throw();
	out->source_index = index; out->ordinal = index + 4;
	out->reader = &observation_page_wal_readers[index];
	return true;
}

const ClusterRecoveryCensusPagesV1 *
cluster_recovery_census_pages_v1(ClusterRecoveryCensus *cursor)
{
	if (cursor != (ClusterRecoveryCensus *)&observation_cursor_object
		|| !observation_cursor_live || !observation_scope_live || observation_steps < 3) abort();
	if (observation_page_fault == 1) return NULL;
	if (observation_page_fault == 3) pg_re_throw();
	return (const ClusterRecoveryCensusPagesV1 *)observation_pages_storage[
		observation_page_fault == 2 ? 1 : 0].bytes;
}

static void
observation_fixture_reset(void)
{
	if (observation_scope_live || observation_cursor_live) abort();
	observation_scope_begins = observation_census_begins = observation_steps = 0;
	observation_manifests = observation_census_destroys = observation_scope_releases = 0;
	observation_fault = observation_fault_at = observation_event_count = 0;
	observation_physical_count = observation_fail_allocation = 0;
	observation_release_error = observation_early_read = false;
	observation_page_fault = 0;
	observation_page_wal_fault = 0;
	observation_page_wal_count = 2;
	memset(observation_page_wal_readers, 0, sizeof(observation_page_wal_readers));
	memset(observation_pages_storage, 0, sizeof(observation_pages_storage));
	observation_pages.count = 2;
	observation_pages.entries[0].forknum = MAIN_FORKNUM;
	observation_pages.entries[0].blockno = 23;
	observation_pages.entries[1].forknum = SPACE_FORKNUM;
	observation_pages.entries[1].blockno = 1;
	memset(observation_physical, 0, sizeof(observation_physical));
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
	if (observation_early_read && ClusterColdRecoveryCtrcPhysical(info.cluster_tx_start,
		&native_ctrc_work) != NULL) abort();
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
	if (observation_steps == 2 || observation_steps - 2 < observation_physical_count) {
		if (observation_fault == 10) {
			out->kind = CLUSTER_RECOVERY_CENSUS_NATIVE_EFFECT;
			return CLUSTER_RECOVERY_CENSUS_ENTRY;
		}
		out->kind = CLUSTER_RECOVERY_CENSUS_TT_SLOT;
		if (observation_steps - 2 < observation_physical_count)
			out->value.tt = observation_physical[observation_steps - 2];
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
	if (observation_release_error) pg_re_throw();
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
	ctrc_terminal_subject_fixture(&observation_physical[0], true);
	observation_physical_count = 1;
}

static void
observation_two_ready(void)
{
	c0_subject();
	ctrc_census.subject_count = ctrc_census.unresolved_count = 2;
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	native_ctrc_work.entries[0] = census.entries[0].input;
	native_ctrc_work.entries[1] = native_ctrc_work.entries[0];
	native_ctrc_work.entries[1].key.slot_offset = 7;
	native_ctrc_work.entries[1].key.xid = native_ctrc_work.entries[1].slot.xid = 812;
	native_ctrc_work.entries[1].key.segment_generation = 0;
	native_ctrc_work.entries[1].slot.status = TT_SLOT_COMMITTED;
	native_ctrc_work.entries[1].slot.commit_scn = 97;
	for (unsigned i = 0; i < 2; i++) {
		const ClusterColdTtSlotInputV1 *subject = &native_ctrc_work.entries[1 - i];
		observation_physical[i].owner_instance = subject->key.owner_instance;
		observation_physical[i].segment_id = subject->key.segment_id;
		observation_physical[i].segment_generation = subject->key.segment_generation;
		observation_physical[i].slot_offset = subject->key.slot_offset;
		observation_physical[i].slot = subject->slot;
	}
	observation_physical[1].slot.status = TT_SLOT_ABORTED;
	observation_physical[2] = observation_physical[0];
	observation_physical[2].segment_id += 100; /* Still enumerate unrelated TT. */
	observation_physical_count = 3;
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

UT_TEST(test_ctrc_observation_refuses_missing_required_current_physical_slot)
{
	ClusterRecoveryUndoEntry physical;
	ctrc_terminal_subject_fixture(&physical, true);
	observation_fault = 10; /* No TT_SLOT for the original required subject. */
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT(!observation_cursor_live && !observation_scope_live);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
}

UT_TEST(test_ctrc_physical_input_keeps_work_order_and_raw_c0_successor)
{
	const ClusterColdCtrcPhysicalV1 *physical;
	ClusterColdTtSlotInputV1 terminal, original;
	ClusterRecoveryUndoEntry expected;
	unsigned begins, allocations_before;
	observation_ready();
	original = native_ctrc_work.entries[0];
	UT_ASSERT_EQ(original.slot.status, TT_SLOT_ACTIVE);
	expected = observation_physical[0];
	UT_ASSERT_EQ(expected.slot.status, TT_SLOT_ABORTED);
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	physical = ClusterColdRecoveryCtrcPhysical(info.cluster_tx_start, &native_ctrc_work);
	UT_ASSERT(physical != NULL && physical->count == 1);
	UT_ASSERT(memcmp(&physical->entries[0], &expected, sizeof(expected)) == 0);
	UT_ASSERT(cluster_recovery_ctrc_terminal_subject_v1(info.cluster_tx_start,
		&native_ctrc_work, 0, &physical->entries[0], &terminal));
	UT_ASSERT_EQ(terminal.slot.status, TT_SLOT_ABORTED);
	UT_ASSERT(memcmp(&original, &native_ctrc_work.entries[0], sizeof(original)) == 0);
	begins = observation_scope_begins; allocations_before = allocations;
	UT_ASSERT(ClusterColdRecoveryCtrcPhysical(info.cluster_tx_start, &native_ctrc_work) == physical);
	UT_ASSERT_EQ(observation_scope_begins, begins);
	UT_ASSERT_EQ(allocations, allocations_before);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_result.state, CLUSTER_RECOVERY_CTRC_RESULT_PENDING);
	/* Two independently retained terminal subjects, reversed census order.
	 * Generation0 is a valid physical value and remains unguessed. */
	observation_two_ready();
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	physical = ClusterColdRecoveryCtrcPhysical(info.cluster_tx_start, &native_ctrc_work);
	UT_ASSERT(physical != NULL && physical->count == 2);
	UT_ASSERT_EQ(physical->entries[0].slot.status, TT_SLOT_ABORTED);
	UT_ASSERT_EQ(physical->entries[1].slot.xid, 812);
	UT_ASSERT_EQ(physical->entries[1].segment_generation, 0);
	UT_ASSERT_EQ(physical->entries[1].slot.commit_scn, 97);
	UT_ASSERT_EQ(observation_steps, 5);
}

UT_TEST(test_ctrc_physical_input_refuses_reuse_duplicates_and_ambiguous_originals)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		observation_ready();
		switch (fault) {
		case 0: observation_physical[0].owner_instance++; break;
		case 1: observation_physical[0].segment_id++; break;
		case 2: observation_physical[0].segment_generation++; break;
		case 3: observation_physical[0].slot_offset++; break;
		case 4: observation_physical[0].slot.xid++; break;
		case 5: observation_physical[0].slot.wrap++; break;
		case 6:
			observation_physical[1] = observation_physical[0]; observation_physical_count = 2; break;
		case 7:
			native_ctrc_work.entries[1] = native_ctrc_work.entries[0];
			native_ctrc_work.counts.subject_count = 2; break;
		case 8: native_ctrc_work.entries[0].key.xid++; break;
		case 9: observation_two_ready(); observation_physical_count = 1; break;
		}
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		UT_ASSERT(ClusterColdRecoveryCtrcPhysical(info.cluster_tx_start, &native_ctrc_work) == NULL);
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	}
}

UT_TEST(test_ctrc_physical_input_is_private_until_full_census_and_original_rechecks)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		observation_ready(); observation_early_read = true;
		UT_ASSERT(ClusterColdRecoveryCtrcPhysical(info.cluster_tx_start, &native_ctrc_work) == NULL);
		UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		observation_early_read = false;
		switch (fault) {
		case 0: typed.census_root.file_txn_seq++; break;
		case 1: native_ctrc_work.entries[0].slot.first_undo_block.raw[0]++; break;
		case 2: window_region.checkpoint_owner.tx_accepted_generation = 0; break;
		case 3: observation_manifest.roots_sha256[0]++; break;
		case 4: owner_boot++; break;
		}
		UT_ASSERT(ClusterColdRecoveryCtrcPhysical(info.cluster_tx_start, &native_ctrc_work) == NULL);
		if (fault == 4) {
			/* Original owner loss retains children for matching cleanup. */
			UT_ASSERT(observation_cursor_live && observation_scope_live);
			owner_boot--;
			release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		}
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	}
}

UT_TEST(test_ctrc_physical_input_requires_exact_owner_work_and_valid_empty_observation)
{
	const ClusterColdCtrcPhysicalV1 *physical;
	struct ClusterRecoveryStartV1 *old;
	UT_ASSERT(ClusterColdRecoveryCtrcPhysical(NULL, NULL) == NULL);
	observation_ready();
	UT_ASSERT(ClusterColdRecoveryCtrcPhysical((const struct ClusterRecoveryStartV1 *)&typed,
		&native_ctrc_work) == NULL);
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT(ClusterColdRecoveryCtrcPhysical(info.cluster_tx_start,
		(const ClusterColdCtrcWorkV1 *)&typed) == NULL);
	CritSectionCount = 1;
	UT_ASSERT(ClusterColdRecoveryCtrcPhysical(info.cluster_tx_start, &native_ctrc_work) == NULL);
	CritSectionCount = 0;
	UT_ASSERT(ClusterColdRecoveryCtrcPhysical(info.cluster_tx_start, &native_ctrc_work) != NULL);
	old = info.cluster_tx_start;
	ClusterReleaseColdRecoveryInputs(&info);
	UT_ASSERT(ClusterColdRecoveryCtrcPhysical(old, &native_ctrc_work) == NULL);
	c0_subject();
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	serving_actor(); UT_ASSERT(cluster_recovery_tx_result_accept_v1()); startup_actor();
	UT_ASSERT_EQ(native_ctrc_work.counts.subject_count, 0);
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	physical = ClusterColdRecoveryCtrcPhysical(info.cluster_tx_start, &native_ctrc_work);
	UT_ASSERT(physical != NULL && physical->count == 0);
}

UT_TEST(test_ctrc_physical_input_reserves_storage_before_scope_and_preserves_results)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterRecoveryCtrcResultValueV1 result;
		Size budget;
		unsigned freed;
		observation_ready(); result = window_region.checkpoint_owner.ctrc_result; freed = frees;
		budget = offsetof(ClusterColdCtrcWorkV1, entries) + sizeof(native_ctrc_work.entries[0])
			+ offsetof(ClusterColdCtrcPhysicalV1, entries) + sizeof(ClusterRecoveryUndoEntry);
		if (fault == 1) { budget = 2 * 1024 * 1024; observation_fail_allocation = allocations + 2; }
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, budget));
		UT_ASSERT_EQ(observation_scope_begins, 0);
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT(fault == 0 ? frees == freed : frees > freed);
		UT_ASSERT(memcmp(&result, &window_region.checkpoint_owner.ctrc_result, sizeof(result)) == 0);
		observation_fail_allocation = 0;
	}
}

UT_TEST(test_ctrc_physical_input_retains_storage_on_original_scope_release_error)
{
	volatile bool caught = false;
	unsigned freed;
	observation_ready();
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	freed = frees; observation_release_error = true;
	PG_TRY();
	{
		release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught && !observation_cursor_live && observation_scope_live);
	UT_ASSERT_EQ(frees, freed);
	UT_ASSERT(ClusterColdRecoveryCtrcPhysical(info.cluster_tx_start, &native_ctrc_work) == NULL);
	observation_release_error = false;
	ClusterReleaseColdRecoveryInputs(&info);
	UT_ASSERT_EQ(frees, freed + 3); /* physical view, original copy, opaque Start */
	UT_ASSERT(!observation_scope_live);
}


#ifdef CTRC_PAGES_REFUSE_ONLY
const struct ClusterRecoveryCensusPagesV1 *
ClusterColdRecoveryCtrcPages(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work)
{
	(void)start; (void)work;
	return NULL;
}
#endif

UT_TEST(test_ctrc_pages_belong_to_the_complete_original_start_observation)
{
	const ClusterRecoveryCensusPagesV1 *pages;
	ClusterRecoveryCtrcResultValueV1 result;
	observation_two_ready(); result = window_region.checkpoint_owner.ctrc_result;
	UT_ASSERT(ClusterColdRecoveryCtrcPages(info.cluster_tx_start, &native_ctrc_work) == NULL);
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	pages = ClusterColdRecoveryCtrcPages(info.cluster_tx_start, &native_ctrc_work);
	UT_ASSERT(pages == &observation_pages);
	if (pages == NULL) {
		ClusterReleaseColdRecoveryInputs(&info);
		return;
	}
	UT_ASSERT_EQ(pages->count, 2); UT_ASSERT_EQ(pages->entries[0].blockno, 23);
	UT_ASSERT_EQ(pages->entries[1].forknum, SPACE_FORKNUM);
	UT_ASSERT(memcmp(&result, &window_region.checkpoint_owner.ctrc_result, sizeof(result)) == 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	UT_ASSERT(observation_cursor_live && observation_scope_live);
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_ctrc_pages_missing_or_rebound_view_revokes_the_original_observation)
{
	for (unsigned fault = 1; fault <= 2; fault++) {
		observation_two_ready();
		UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		observation_page_fault = fault;
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 0));
		UT_ASSERT(ClusterColdRecoveryCtrcPages(info.cluster_tx_start, &native_ctrc_work) == NULL);
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		observation_page_fault = 0;
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		ClusterReleaseColdRecoveryInputs(&info);
	}
}

UT_TEST(test_ctrc_pages_require_original_work_owner_and_accepted_tx)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		const struct ClusterRecoveryStartV1 *start;
		const ClusterColdCtrcWorkV1 *work;
		observation_two_ready();
		UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		start = info.cluster_tx_start; work = &native_ctrc_work;
		if (fault == 0) start = (const struct ClusterRecoveryStartV1 *)&typed;
		if (fault == 1) work = (const ClusterColdCtrcWorkV1 *)&typed;
		if (fault == 2) owner_boot++;
		if (fault == 3) native_ctrc_work.entries[1].slot.xid++;
		if (fault == 4) window_region.checkpoint_owner.tx_accepted_generation = 0;
		if (fault == 5) typed.census_root.image_sha256[31]++;
		UT_ASSERT(ClusterColdRecoveryCtrcPages(start, work) == NULL);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		if (fault == 2) owner_boot--;
		/* Refused owner/work is error cleanup, never a successful handoff. */
		release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		ClusterReleaseColdRecoveryInputs(&info);
	}
}

UT_TEST(test_ctrc_pages_complete_empty_view_is_distinct_from_missing)
{
	observation_two_ready(); observation_pages.count = 0;
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT(ClusterColdRecoveryCtrcPages(info.cluster_tx_start, &native_ctrc_work)
		== &observation_pages);
	UT_ASSERT_EQ(observation_pages.count, 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_ctrc_pages_native_error_retires_cursor_before_original_scope)
{
	ClusterRecoveryCtrcResultValueV1 result;
	volatile bool caught = false;
	observation_two_ready(); result = window_region.checkpoint_owner.ctrc_result;
	observation_page_fault = 3;
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
	UT_ASSERT(strstr(observation_events, "DR") != NULL);
	UT_ASSERT(memcmp(&result, &window_region.checkpoint_owner.ctrc_result, sizeof(result)) == 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	ClusterReleaseColdRecoveryInputs(&info);
}

#ifdef CTRC_PAGE_WAL_START_REFUSE_ONLY
bool
ClusterColdRecoveryCtrcPageWalCount(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 *out)
{
	(void)start; (void)work; if (out != NULL) *out = 0; return false;
}
bool
ClusterColdRecoveryCtrcPageWalAt(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 index, struct ClusterRecoveryPageWalV1 *out)
{
	(void)start; (void)work; (void)index;
	if (out != NULL) memset(out, 0, sizeof(*out)); return false;
}
#endif

UT_TEST(test_ctrc_page_wal_requires_complete_original_start_and_keeps_service_pending)
{
	ClusterRecoveryPageWalV1 value, zero = {0};
	ClusterRecoveryCtrcResultValueV1 result;
	uint32 count = 99;
	observation_two_ready(); result = window_region.checkpoint_owner.ctrc_result;
	UT_ASSERT(!ClusterColdRecoveryCtrcPageWalCount(info.cluster_tx_start, &native_ctrc_work, &count));
	UT_ASSERT_EQ(count, 0);
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT(ClusterColdRecoveryCtrcPageWalCount(info.cluster_tx_start, &native_ctrc_work, &count));
	UT_ASSERT_EQ(count, 2);
	UT_ASSERT(ClusterColdRecoveryCtrcPageWalAt(info.cluster_tx_start, &native_ctrc_work, 1, &value));
	UT_ASSERT(value.reader == &observation_page_wal_readers[1]);
	UT_ASSERT_EQ(value.source_index, 1); UT_ASSERT_EQ(value.ordinal, 5);
	memset(&value, 0xa5, sizeof(value));
	UT_ASSERT(!ClusterColdRecoveryCtrcPageWalAt(info.cluster_tx_start, &native_ctrc_work, count, &value));
	UT_ASSERT(memcmp(&value, &zero, sizeof(value)) == 0);
	UT_ASSERT_EQ(observation_scope_begins, 1); UT_ASSERT_EQ(observation_census_begins, 1);
	UT_ASSERT(memcmp(&result, &window_region.checkpoint_owner.ctrc_result, sizeof(result)) == 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_ctrc_page_wal_changed_missing_or_error_revokes_original_observation)
{
	for (unsigned fault = 1; fault <= 5; fault++) {
		volatile bool caught = false;
		volatile uint32 count = 99;
		volatile ClusterRecoveryPageWalV1 value;
		ClusterRecoveryPageWalV1 zero = {0};
		observation_two_ready();
		UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		observation_page_wal_fault = fault;
		PG_TRY();
		{
			if (fault < 4)
				UT_ASSERT(!ClusterColdRecoveryCtrcPageWalCount(info.cluster_tx_start, &native_ctrc_work,
					(uint32 *)&count));
			else {
				count = 0;
				memset((void *)&value, 0xa5, sizeof(value));
				UT_ASSERT(!ClusterColdRecoveryCtrcPageWalAt(info.cluster_tx_start, &native_ctrc_work, 0,
					(ClusterRecoveryPageWalV1 *)&value));
			}
		}
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 3 || fault == 5); UT_ASSERT_EQ(count, 0);
		if (fault >= 4) UT_ASSERT(memcmp((const void *)&value, &zero, sizeof(value)) == 0);
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT(strstr(observation_events, "DR") != NULL);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		observation_page_wal_fault = 0;
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		ClusterReleaseColdRecoveryInputs(&info);
	}
}

UT_TEST(test_ctrc_page_wal_requires_exact_work_owner_root_and_accepted_tx)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		const struct ClusterRecoveryStartV1 *start;
		const ClusterColdCtrcWorkV1 *work;
		ClusterRecoveryPageWalV1 value, zero = {0};
		uint32 count = 99;
		observation_two_ready();
		UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		start = info.cluster_tx_start; work = &native_ctrc_work;
		if (fault == 0) start = (const struct ClusterRecoveryStartV1 *)(uintptr_t)1;
		if (fault == 1) work = (const ClusterColdCtrcWorkV1 *)&typed;
		if (fault == 2) owner_boot++;
		if (fault == 3) native_ctrc_work.entries[1].slot.xid++;
		if (fault == 4) window_region.checkpoint_owner.tx_accepted_generation = 0;
		if (fault == 5) typed.census_root.image_sha256[31]++;
		UT_ASSERT(!ClusterColdRecoveryCtrcPageWalCount(start, work, &count));
		UT_ASSERT_EQ(count, 0); memset(&value, 0xa5, sizeof(value));
		UT_ASSERT(!ClusterColdRecoveryCtrcPageWalAt(start, work, 0, &value));
		UT_ASSERT(memcmp(&value, &zero, sizeof(value)) == 0);
		if (fault == 2) owner_boot--;
		release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		ClusterReleaseColdRecoveryInputs(&info);
	}
}

UT_TEST(test_ctrc_page_wal_complete_empty_is_not_a_missing_input)
{
	uint32 count = 99;
	observation_two_ready(); observation_page_wal_count = 0;
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT(ClusterColdRecoveryCtrcPageWalCount(info.cluster_tx_start, &native_ctrc_work, &count));
	UT_ASSERT_EQ(count, 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	ClusterReleaseColdRecoveryInputs(&info);
}
