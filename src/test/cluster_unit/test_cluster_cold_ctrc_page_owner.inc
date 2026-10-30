/* Original Start's PAGE owner predicates; native PAGE/FD transport is a
 * controlled leaf here, exercised separately by the native readback target.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */

static bool
startup_owner_empty(RfPageCtrcOwnerProofV1 value)
{
	static const RfPageCtrcOwnerProofV1 zero = {0};
	value.detail = 0;
	return memcmp(&value, &zero, sizeof(value)) == 0;
}

UT_TEST(test_startup_page_owner_uses_original_completed_operation_and_full_image)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		RfPageCtrcOwnerProofV1 proof;
		const struct ClusterRecoveryStartV1 *start;
		ClusterColdCtrcPageProgressV1 *progress;

		startup_image_ready(unchanged != 0); start = info.cluster_tx_start;
		progress = start->ctrc_page_progress;
		for (uint32 i = 0; i < 2; i++) {
			proof = ClusterColdRecoveryCtrcPageOwnerProof(start, i);
			UT_ASSERT_EQ(proof.detail, RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(proof.owner_family, 10); UT_ASSERT_EQ(proof.outcome, 1);
			UT_ASSERT_EQ(proof.rule, unchanged ? 6 : 1);
			UT_ASSERT_EQ(proof.target_index, i); UT_ASSERT_EQ(proof.target_count, 2);
			UT_ASSERT_EQ(proof.replay_source_count, start->ctrc_wal_source_count);
			UT_ASSERT_EQ(proof.replay_source_index,
				start->ctrc_page_sources[prefix_source_ordinal].generation_index);
			UT_ASSERT_EQ(proof.blockno, prefix_targets[i].page_identity.blockno);
			UT_ASSERT(memcmp(&proof.before, &prefix_targets[i].expected_result, sizeof(proof.before)) == 0);
			UT_ASSERT(memcmp(&proof.image, &startup_image_expected, sizeof(proof.image)) == 0);
			if (unchanged) {
				static const ClusterWalWriterToken no_writer = {0};
				UT_ASSERT(memcmp(&proof.space, &progress->completed[i].unchanged.space, sizeof(proof.space)) == 0);
				UT_ASSERT(memcmp(&proof.writer, &no_writer, sizeof(no_writer)) == 0);
				UT_ASSERT_EQ(proof.record_start, 0); UT_ASSERT_EQ(proof.record_end, 0);
			} else {
				const RfPageCtrcFpiInsertV1 *actual = &progress->completed[i].result.inserted;
				UT_ASSERT(memcmp(&proof.space, &actual->image.identity, sizeof(proof.space)) == 0);
				UT_ASSERT(memcmp(&proof.writer, &actual->window.owner.writer, sizeof(proof.writer)) == 0);
				UT_ASSERT_EQ(proof.record_start, actual->record_start);
				UT_ASSERT_EQ(proof.record_end, actual->record_end);
			}
		}
		UT_ASSERT_EQ(startup_image_calls, 2); UT_ASSERT_EQ(prefix_steps, 2);
		UT_ASSERT_EQ(pending_storage_mask, 3); UT_ASSERT(observation_scope_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_page_owner_missing_pending_or_unsupported_target_has_no_proof)
{
	RfPageCtrcOwnerProofV1 proof;
	proof = ClusterColdRecoveryCtrcPageOwnerProof(NULL, 0);
	UT_ASSERT(proof.detail != RF_PAGE_PROOF_DETAIL_OK && startup_owner_empty(proof));
	startup_image_ready(false);
	proof = ClusterColdRecoveryCtrcPageOwnerProof(info.cluster_tx_start, 2);
	UT_ASSERT(proof.detail != RF_PAGE_PROOF_DETAIL_OK && startup_owner_empty(proof));
	UT_ASSERT_EQ(startup_image_calls, 0); UT_ASSERT(!cluster_recovery_cold_start->revoked);
	retirement_local_cleanup();
	prefix_ready(); startup_image_calls = 0;
	UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
	proof = ClusterColdRecoveryCtrcPageOwnerProof(info.cluster_tx_start, 0);
	UT_ASSERT(proof.detail != RF_PAGE_PROOF_DETAIL_OK && startup_owner_empty(proof));
	UT_ASSERT_EQ(startup_image_calls, 0); UT_ASSERT(pending_storage_live);
	retirement_local_cleanup();
	startup_image_ready(false); prefix_targets[0].history_only = true;
	proof = ClusterColdRecoveryCtrcPageOwnerProof(info.cluster_tx_start, 0);
	UT_ASSERT(proof.detail != RF_PAGE_PROOF_DETAIL_OK && startup_owner_empty(proof));
	UT_ASSERT_EQ(startup_image_calls, 0); UT_ASSERT(pending_storage_mask == 3);
	retirement_local_cleanup();
}

UT_TEST(test_startup_page_owner_late_cleanup_drift_publishes_no_metadata_or_bytes)
{
	RfPageCtrcOwnerProofV1 proof;
	startup_image_ready(false); prefix_destroy_callback = startup_image_late_drift;
	proof = ClusterColdRecoveryCtrcPageOwnerProof(info.cluster_tx_start, 0);
	prefix_destroy_callback = NULL;
	UT_ASSERT(proof.detail != RF_PAGE_PROOF_DETAIL_OK && startup_owner_empty(proof));
	UT_ASSERT(cluster_recovery_cold_start->revoked && pending_storage_mask == 3);
	UT_ASSERT_EQ(startup_image_calls, 1); UT_ASSERT(observation_scope_live);
	retirement_local_cleanup();
}

UT_TEST(test_startup_page_owner_wait_rechecks_without_cached_rule_or_new_wal)
{
	RfPageCtrcOwnerProofV1 proof;
	startup_image_ready(false); completed_readback_mode = 4; completed_readback_calls = 0;
	proof = ClusterColdRecoveryCtrcPageOwnerProof(info.cluster_tx_start, 0);
	UT_ASSERT_EQ(proof.detail, RF_PAGE_PROOF_DETAIL_WOULD_BLOCK);
	UT_ASSERT(startup_owner_empty(proof) && !cluster_recovery_cold_start->revoked);
	completed_readback_mode = 0;
	proof = ClusterColdRecoveryCtrcPageOwnerProof(info.cluster_tx_start, 0);
	UT_ASSERT_EQ(proof.detail, RF_PAGE_PROOF_DETAIL_OK); UT_ASSERT_EQ(proof.rule, 1);
	UT_ASSERT_EQ(startup_image_calls, 2); UT_ASSERT_EQ(prefix_steps, 2);
	UT_ASSERT_EQ(pending_storage_created, 2); UT_ASSERT_EQ(pending_storage_closes, 0);
	retirement_local_cleanup();
}

UT_TEST(test_startup_page_owner_false_or_error_preserves_all_original_custody)
{
	for (unsigned mode = 1; mode <= 2; mode++) {
		RfPageCtrcOwnerProofV1 proof = {0}; volatile bool caught = false;
		startup_image_ready(false); completed_readback_mode = mode == 1 ? 1 : 3;
		PG_TRY(); { proof = ClusterColdRecoveryCtrcPageOwnerProof(info.cluster_tx_start, 0); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(mode == 1 ? !caught && proof.detail != RF_PAGE_PROOF_DETAIL_OK : caught);
		UT_ASSERT(startup_owner_empty(proof));
		UT_ASSERT(cluster_recovery_cold_start->revoked && pending_storage_mask == 3 && observation_scope_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_page_owner_scratch_is_reserved_before_first_page)
{
	Size before, scratch;
	prefix_ready();
	UT_ASSERT(ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 1024 * 1024));
	before = sizeof(ClusterColdCtrcPageProgressV1) + 2 * sizeof(ClusterColdCtrcPageCompletedV1)
		+ 2 * cluster_shared_fs_page_bytes_v1();
	scratch = native_ctrc_work.counts.subject_count * sizeof(ClusterColdTtSlotInputV1)
		+ 2 * sizeof(PGAlignedBlock) + sizeof(RfPageCtrcOwnerProofV1);
	UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, before + scratch));
	UT_ASSERT_EQ(prefix_pages, 0); UT_ASSERT_EQ(prefix_steps, 0);
	UT_ASSERT_EQ(pending_storage_created, 0); UT_ASSERT(observation_scope_live);
	retirement_local_cleanup();
}
