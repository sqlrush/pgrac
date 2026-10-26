/* Copyright (c) 2026, pgrac contributors.
 * Complete runtime provider, controlled copy/native publication boundaries.
 * These interleavings are unit inputs, not a native scheduling replay.
 */

enum CanonicalSamplePath {
	CANONICAL_PHYSICAL,
	CANONICAL_OWNER,
	CANONICAL_SAME_SEGMENT,
	CANONICAL_SPLIT_SEGMENT,
	CANONICAL_PATH_COUNT
};

static uint64 canonical_copy_root;
static int canonical_copy_count;
static int canonical_copy_drift;
static TTSlot canonical_successor;
static XidStatus canonical_next_native;
static bool canonical_physical_active;
static ClusterTxResolution canonical_resolution;
static ClusterTxResolveReason canonical_reason;

/* Publish only after the real provider has taken its first private copy. */
static void
canonical_after_copy(uint64 root_id)
{
	if (root_id != canonical_copy_root || ++canonical_copy_count != 1)
		return;
	test_tt_slot = canonical_successor;
	test_native_status = canonical_next_native;
	switch (canonical_copy_drift) {
	case 1:
		test_candidate_tt_generation++;
		test_candidate_data_generation++;
		break;
	case 2:
		test_candidate_root_drift = true;
		test_regular_root_resolve_calls = 1;
		break;
	case 3:
		test_admission_revoked = true;
		break;
	case 4:
		test_candidate_copy_result = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
		break;
	case 5:
		test_current_owner.wrap++;
		break;
	case 6:
		test_current_segment++;
		break;
	default:
		break;
	}
}

static void
canonical_reset(enum CanonicalSamplePath path)
{
	reset_exact_origin_fixture();
	canonical_copy_root = path == CANONICAL_SAME_SEGMENT ? 91 : 92;
	canonical_copy_count = 0;
	canonical_copy_drift = 0;
	canonical_next_native = TRANSACTION_STATUS_COMMITTED;
	canonical_physical_active = true;
	canonical_reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	memset(&canonical_resolution, 0, sizeof(canonical_resolution));
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	if (path == CANONICAL_SAME_SEGMENT)
		test_origin_record.tt_slot_segment_id = TEST_RECORD_SEGMENT;
	if (path == CANONICAL_OWNER)
		test_current_owner_available = true;
}

static ClusterTxOutcome
canonical_sample(enum CanonicalSamplePath path, ClusterTxResolveMode mode)
{
	ClusterSemanticAdmissionToken admission = { 0 };
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot data_root = { 0 }, tt_root = { 0 };
	ClusterRuntimeVisibilityOriginPlan plan;
	ClusterRuntimeVisibilityOriginStep step;
	ClusterTTSlotCurrentOwner owner = test_current_owner;
	ClusterTTSlotPhysicalLocator physical = { 0 };
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	bool sampled;
	bool precommit_retry = true;

	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 5;
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	data_root.intent = tt_root.intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED;
	data_root.root_id = 91;
	tt_root.root_id = 92;
	data_root.root_generation = tt_root.root_generation = 7;
	if (path == CANONICAL_PHYSICAL || path == CANONICAL_OWNER) {
		physical.segment_id = TEST_TT_SEGMENT;
		physical.xid = TEST_ORIGIN_XID;
		physical.slot_offset = TEST_TT_OFFSET;
		physical.wrap = TEST_ORIGIN_WRAP;
		if (path == CANONICAL_OWNER)
			sampled = cluster_runtime_visibility_current_owner_sample_held(
				TEST_ORIGIN_XID, &owner, &admission, &guard, &tt_root, &key, &result,
				&canonical_physical_active);
		else
			sampled = cluster_runtime_visibility_physical_locator_sample_held(
				&physical, &admission, &guard, &tt_root, &key, &result, &canonical_physical_active,
				&precommit_retry);
		if (!sampled) {
			UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_UNKNOWN);
			UT_ASSERT(!result.authoritative);
			return CLUSTER_TX_UNKNOWN;
		}
		UT_ASSERT_EQ(key.local_xid, TEST_ORIGIN_XID);
		UT_ASSERT_EQ(key.undo_segment_id, TEST_TT_SEGMENT);
		UT_ASSERT_EQ(key.tt_slot_id, cluster_tt_slot_offset_to_id(TEST_TT_OFFSET));
		UT_ASSERT_EQ(result.status_epoch, test_formation_epoch);
		UT_ASSERT(result.authoritative);
		canonical_resolution.commit_scn = result.commit_scn;
		if (result.status == CLUSTER_TT_STATUS_COMMITTED)
			return CLUSTER_TX_COMMITTED;
		if (result.status == CLUSTER_TT_STATUS_ABORTED)
			return CLUSTER_TX_ABORTED;
		UT_ASSERT_EQ(result.status, CLUSTER_TT_STATUS_IN_PROGRESS);
		UT_ASSERT(!precommit_retry || path == CANONICAL_OWNER);
		return CLUSTER_TX_IN_PROGRESS;
	}
	step = cluster_runtime_visibility_origin_plan_freeze_data_held(
		&test_origin_locator, mode, &admission, NULL, &guard, &data_root, &plan,
		&canonical_resolution, &canonical_reason);
	if (step == CLUSTER_RUNTIME_VISIBILITY_ORIGIN_FAILED)
		return CLUSTER_TX_UNKNOWN;
	if (step == CLUSTER_RUNTIME_VISIBILITY_ORIGIN_COMPLETE)
		return canonical_resolution.outcome;
	UT_ASSERT_EQ(path, CANONICAL_SPLIT_SEGMENT);
	if (!cluster_runtime_visibility_origin_plan_sample_canonical_held(
			&plan, mode, &admission, &guard, &tt_root, &canonical_reason))
		return CLUSTER_TX_UNKNOWN;
	return cluster_runtime_visibility_origin_plan_recheck_data_held(
		&plan, mode, &admission, &guard, &data_root, &canonical_resolution, &canonical_reason);
}

static void
canonical_commit_input(enum CanonicalSamplePath path)
{
	canonical_reset(path);
	canonical_successor = test_tt_slot;
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_candidate_after_block0_copy = canonical_after_copy;
}

static void
canonical_commit_succeeds(enum CanonicalSamplePath path)
{
	canonical_commit_input(path);
	UT_ASSERT_EQ(canonical_sample(path, CLUSTER_TX_RESOLVE_VISIBILITY), CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(canonical_resolution.commit_scn, test_commit_scn);
	UT_ASSERT_EQ(canonical_copy_count, 2);
	UT_ASSERT(!canonical_physical_active || path >= CANONICAL_SAME_SEGMENT);
}

UT_TEST(test_canonical_commit_resample_physical)
{
	canonical_commit_succeeds(CANONICAL_PHYSICAL);
}

UT_TEST(test_canonical_commit_resample_owner_preserves_allocator_fence)
{
	canonical_commit_input(CANONICAL_OWNER);
	/* An ACTIVE allocator snapshot cannot authorize a COMMITTED successor.
	 * Resample, refuse this call, then a new owner capture can prove it. */
	UT_ASSERT_EQ(canonical_sample(CANONICAL_OWNER, CLUSTER_TX_RESOLVE_VISIBILITY),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(canonical_copy_count, 2);
	test_current_owner.status = CTS_COMMITTED;
	test_current_owner.commit_scn = test_commit_scn;
	UT_ASSERT_EQ(canonical_sample(CANONICAL_OWNER, CLUSTER_TX_RESOLVE_VISIBILITY),
				 CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(canonical_resolution.commit_scn, test_commit_scn);
}

UT_TEST(test_canonical_commit_resample_same_segment)
{
	canonical_commit_succeeds(CANONICAL_SAME_SEGMENT);
}

UT_TEST(test_canonical_commit_resample_split_segment)
{
	canonical_commit_succeeds(CANONICAL_SPLIT_SEGMENT);
}

UT_TEST(test_canonical_commit_resample_keeps_terminal_census_mode)
{
	for (int path = CANONICAL_SAME_SEGMENT; path < CANONICAL_PATH_COUNT; path++) {
		canonical_commit_input(path);
		UT_ASSERT_EQ(canonical_sample(path, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS),
					 CLUSTER_TX_COMMITTED);
		UT_ASSERT_EQ(canonical_resolution.commit_scn, test_commit_scn);
		UT_ASSERT_EQ(canonical_copy_count, 2);
	}
}

UT_TEST(test_canonical_native_abort_does_not_resample)
{
	for (int path = 0; path < CANONICAL_PATH_COUNT; path++) {
		canonical_commit_input(path);
		canonical_successor.status = TT_SLOT_ABORTED;
		canonical_successor.commit_scn = InvalidScn;
		canonical_next_native = TRANSACTION_STATUS_ABORTED;
		UT_ASSERT_EQ(canonical_sample(path, CLUSTER_TX_RESOLVE_VISIBILITY),
					 path >= CANONICAL_SAME_SEGMENT ? CLUSTER_TX_ABORTED : CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(canonical_copy_count, 1);
		UT_ASSERT_EQ(canonical_resolution.commit_scn, InvalidScn);
	}
}

static void
canonical_preabort_input(enum CanonicalSamplePath path)
{
	canonical_reset(path);
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
}

static void
canonical_preabort_stays_live(enum CanonicalSamplePath path)
{
	canonical_preabort_input(path);
	UT_ASSERT_EQ(canonical_sample(path, CLUSTER_TX_RESOLVE_VISIBILITY), CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(canonical_resolution.commit_scn, InvalidScn);
	UT_ASSERT(!canonical_physical_active || path >= CANONICAL_SAME_SEGMENT);
	test_native_status = TRANSACTION_STATUS_ABORTED;
	if (path == CANONICAL_OWNER)
		test_current_owner.status = CTS_ABORTED;
	UT_ASSERT_EQ(canonical_sample(path, CLUSTER_TX_RESOLVE_VISIBILITY), CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(canonical_resolution.commit_scn, InvalidScn);
}

UT_TEST(test_canonical_preabort_physical)
{
	canonical_preabort_stays_live(CANONICAL_PHYSICAL);
}

UT_TEST(test_canonical_preabort_owner)
{
	canonical_preabort_stays_live(CANONICAL_OWNER);
}

UT_TEST(test_canonical_preabort_same_segment)
{
	canonical_preabort_stays_live(CANONICAL_SAME_SEGMENT);
}

UT_TEST(test_canonical_preabort_split_segment)
{
	canonical_preabort_stays_live(CANONICAL_SPLIT_SEGMENT);
}

UT_TEST(test_canonical_commit_resample_incomplete_is_bounded)
{
	for (int path = 0; path < CANONICAL_PATH_COUNT; path++) {
		for (int bad = 0; bad < 6; bad++) {
			canonical_commit_input(path);
			switch (bad) {
			case 0:
				canonical_successor.status = TT_SLOT_ACTIVE;
				canonical_successor.commit_scn = InvalidScn;
				break;
			case 1:
				canonical_successor.commit_scn = InvalidScn;
				break;
			case 2:
				canonical_successor.xid++;
				break;
			case 3:
				canonical_successor.wrap++;
				break;
			case 4:
				canonical_successor.status = TT_SLOT_ABORTED;
				canonical_successor.commit_scn = InvalidScn;
				break;
			case 5:
				canonical_copy_drift = 4;
				break;
			}
			UT_ASSERT_EQ(canonical_sample(path, CLUSTER_TX_RESOLVE_VISIBILITY), CLUSTER_TX_UNKNOWN);
			UT_ASSERT(canonical_copy_count <= 2);
			UT_ASSERT_EQ(canonical_resolution.commit_scn, InvalidScn);
		}
	}
}

UT_TEST(test_canonical_commit_resample_preserves_identity_fences)
{
	for (int path = 0; path < CANONICAL_PATH_COUNT; path++) {
		for (int drift = 1; drift <= 3; drift++) {
			canonical_commit_input(path);
			canonical_copy_drift = drift;
			UT_ASSERT_EQ(canonical_sample(path, CLUSTER_TX_RESOLVE_VISIBILITY), CLUSTER_TX_UNKNOWN);
			UT_ASSERT(canonical_copy_count <= 2);
		}
	}
	canonical_commit_input(CANONICAL_PHYSICAL);
	canonical_copy_drift = 6;
	UT_ASSERT_EQ(canonical_sample(CANONICAL_PHYSICAL, CLUSTER_TX_RESOLVE_VISIBILITY),
				 CLUSTER_TX_UNKNOWN);
}

UT_TEST(test_canonical_commit_resample_excludes_subtrans)
{
	for (int path = CANONICAL_SAME_SEGMENT; path < CANONICAL_PATH_COUNT; path++) {
		canonical_commit_input(path);
		set_native_status_sample(0, TEST_ORIGIN_XID, TRANSACTION_STATUS_SUB_COMMITTED);
		test_subtrans_chain[0] = TEST_ORIGIN_XID;
		test_subtrans_chain[1] = TEST_ORIGIN_XID - 1;
		test_subtrans_chain_count = 2;
		test_twophase_xid = TEST_ORIGIN_XID - 1;
		set_native_status_sample(1, TEST_ORIGIN_XID - 1, TRANSACTION_STATUS_COMMITTED);
		UT_ASSERT_EQ(canonical_sample(path, CLUSTER_TX_RESOLVE_VISIBILITY), CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(canonical_copy_count, 1);
	}
}

UT_TEST(test_canonical_preabort_malformed_stays_unknown)
{
	for (int path = 0; path < CANONICAL_PATH_COUNT; path++) {
		for (int bad = 0; bad < 5; bad++) {
			canonical_preabort_input(path);
			switch (bad) {
			case 0:
				test_tt_slot.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
				break;
			case 1:
				test_tt_slot.commit_scn = test_commit_scn;
				break;
			case 2:
				test_tt_slot.first_undo_block = test_origin_locator.uba;
				break;
			case 3:
				test_tt_slot.xid++;
				break;
			case 4:
				test_tt_slot.wrap++;
				break;
			}
			UT_ASSERT_EQ(canonical_sample(path, CLUSTER_TX_RESOLVE_VISIBILITY), CLUSTER_TX_UNKNOWN);
		}
	}
}

UT_TEST(test_canonical_preabort_does_not_grant_terminal_or_prepared_active)
{
	for (int path = 0; path < CANONICAL_PATH_COUNT; path++) {
		canonical_preabort_input(path);
		test_twophase_prepared = true;
		UT_ASSERT_EQ(canonical_sample(path, CLUSTER_TX_RESOLVE_VISIBILITY),
					 path >= CANONICAL_SAME_SEGMENT ? CLUSTER_TX_PREPARED : CLUSTER_TX_UNKNOWN);
	}
	for (int path = CANONICAL_SAME_SEGMENT; path < CANONICAL_PATH_COUNT; path++) {
		for (int mode = CLUSTER_TX_RESOLVE_ROW_WAIT; mode <= CLUSTER_TX_RESOLVE_TERMINAL_CENSUS;
			 mode++) {
			canonical_preabort_input(path);
			UT_ASSERT_EQ(canonical_sample(path, mode), CLUSTER_TX_UNKNOWN);
		}
	}
}

UT_TEST(test_canonical_preabort_preserves_current_allocator_corroboration)
{
	for (int fault = 0; fault < 3; fault++) {
		canonical_preabort_input(CANONICAL_PHYSICAL);
		test_current_segment = TEST_TT_SEGMENT;
		test_current_owner_available = true;
		if (fault == 1)
			test_current_owner.status = CTS_FREE;
		if (fault == 2) {
			test_current_owner_calls = 1;
			test_current_owner_drift_on_recheck = true;
		}
		UT_ASSERT_EQ(canonical_sample(CANONICAL_PHYSICAL, CLUSTER_TX_RESOLVE_VISIBILITY),
					 fault == 0 ? CLUSTER_TX_IN_PROGRESS : CLUSTER_TX_UNKNOWN);
	}
}

UT_TEST(test_canonical_preabort_rechecks_data_and_guard_exit)
{
	ClusterSemanticAdmissionToken admission = { 0 };
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason;

	for (int fault = 0; fault < 3; fault++) {
		canonical_preabort_input(CANONICAL_SPLIT_SEGMENT);
		test_candidate_mutate_record_on_recheck = fault == 0;
		test_candidate_release_admission_drift = fault == 1;
		test_candidate_release_epoch_drift = fault == 2;
		/* Exercise the outer exit recheck after the final DATA phase. */
		test_candidate_release_fault_call = 3;
		admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		admission.record_generation = 5;
		admission.formation_epoch = test_formation_epoch;
		admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
		admission.entered = true;
		UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
						 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
						 &resolution, &reason),
					 CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
		UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_UNKNOWN);
	}
}

UT_TEST(test_canonical_commit_resample_rechecks_data_and_guard_exit)
{
	ClusterSemanticAdmissionToken admission = { 0 };
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason;

	for (int fault = 0; fault < 3; fault++) {
		canonical_commit_input(CANONICAL_SPLIT_SEGMENT);
		test_candidate_mutate_record_on_recheck = fault == 0;
		test_candidate_release_admission_drift = fault == 1;
		test_candidate_release_epoch_drift = fault == 2;
		test_candidate_release_fault_call = 3;
		admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		admission.record_generation = 5;
		admission.formation_epoch = test_formation_epoch;
		admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
		admission.entered = true;
		UT_ASSERT_EQ(cluster_runtime_visibility_resolve_exact_origin_admitted(
						 &test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission,
						 &resolution, &reason),
					 CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(canonical_copy_count, 2);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
		UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_UNKNOWN);
	}
}

static unsigned canonical_pause_calls;

static void
canonical_pause_count_copy(uint64 root_id)
{
	if (root_id == canonical_copy_root)
		canonical_copy_count++;
}

static void
canonical_pause_publish(const char *name)
{
	UT_ASSERT(strcmp(name, "cluster-tx-canonical-copy") == 0);
	UT_ASSERT_EQ(canonical_copy_count, 1);
	UT_ASSERT_EQ(test_native_status, TRANSACTION_STATUS_IN_PROGRESS);
	UT_ASSERT_EQ(test_tt_slot.status, TT_SLOT_ACTIVE);
	canonical_pause_calls++;
	test_tt_slot = canonical_successor;
	test_native_status = canonical_next_native;
}

static void
canonical_pause_case(enum CanonicalSamplePath path)
{
	ClusterTxOutcome outcome;
	canonical_commit_input(path);
	test_candidate_after_block0_copy = canonical_pause_count_copy;
	test_canonical_pause_hook = canonical_pause_publish;
	canonical_pause_calls = 0;
	cluster_injection_armed_count = 1;
	outcome = canonical_sample(path, CLUSTER_TX_RESOLVE_VISIBILITY);
	cluster_injection_armed_count = 0;
	test_canonical_pause_hook = NULL;
	UT_ASSERT_EQ(canonical_pause_calls, 1);
	UT_ASSERT_EQ(canonical_copy_count, 2);
	if (path == CANONICAL_OWNER) {
		UT_ASSERT_EQ(outcome, CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(canonical_resolution.commit_scn, InvalidScn);
		test_current_owner.status = CTS_COMMITTED;
		test_current_owner.commit_scn = test_commit_scn;
		outcome = canonical_sample(path, CLUSTER_TX_RESOLVE_VISIBILITY);
		UT_ASSERT_EQ(canonical_copy_count, 3);
	}
	UT_ASSERT_EQ(outcome, CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(canonical_resolution.commit_scn, test_commit_scn);
}

UT_TEST(test_canonical_pause_physical)
{
	canonical_pause_case(CANONICAL_PHYSICAL);
}
UT_TEST(test_canonical_pause_owner)
{
	canonical_pause_case(CANONICAL_OWNER);
}
UT_TEST(test_canonical_pause_same_segment)
{
	canonical_pause_case(CANONICAL_SAME_SEGMENT);
}
UT_TEST(test_canonical_pause_split_segment)
{
	canonical_pause_case(CANONICAL_SPLIT_SEGMENT);
}

static void
canonical_pause_cancel(const char *name)
{
	UT_ASSERT(strcmp(name, "cluster-tx-canonical-copy") == 0);
	pg_re_throw();
}

UT_TEST(test_canonical_pause_cancel_releases_original_guard)
{
	for (int same_segment = 0; same_segment < 2; same_segment++) {
		ClusterSemanticAdmissionToken admission = { 0 };
		ClusterTxResolution resolution, zero = { 0 };
		ClusterTxResolveReason reason;
		volatile bool caught = false;

		canonical_reset(same_segment ? CANONICAL_SAME_SEGMENT : CANONICAL_SPLIT_SEGMENT);
		test_tt_slot.status = TT_SLOT_ACTIVE;
		test_tt_slot.commit_scn = InvalidScn;
		test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
		admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		admission.record_generation = 5;
		admission.formation_epoch = test_formation_epoch;
		admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
		admission.entered = true;
		test_canonical_pause_hook = canonical_pause_cancel;
		cluster_injection_armed_count = 1;
		PG_TRY();
		{
			(void)cluster_runtime_visibility_resolve_exact_origin_admitted(
				&test_origin_locator, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &resolution,
				&reason);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		cluster_injection_armed_count = 0;
		test_canonical_pause_hook = NULL;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(memcmp(&resolution, &zero, sizeof(zero)), 0);
	}
}
