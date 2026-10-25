/* Startup-shaped retained bindings, not foreign ROOT/fence/lifetime proof.
 * Only the selected physical origin's original scope executes. Additional
 * complete claim tuples are fixture data, never native authorization.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned cold_cohort_shape;
static ClusterWalSourceRef cold_cohort_bound[3];
static uint32 cold_cohort_bound_count;

static bool
cold_original_cohort_bind(RfSideOnlinePlanV1 *plan, const ClusterWalSourceRef *selected)
{
	ClusterWalSourceRef other = *selected, history = *selected;
	other.claim.identity.origin_node_id = 1;
	other.claim.identity.origin_thread_id = 2;
	other.claim.identity.root_lineage_seq += 10;
	other.claim.identity.authority_uuid[0] ^= 0x5a;
	other.claim.claim_sha256[0] ^= 0x11;
	/* Distinct retained generation; no chronological or native-lifetime claim. */
	history.claim.identity.origin_owner_incarnation++;
	history.claim.identity.root_lineage_seq++;
	history.claim.claim_sha256[0] ^= 0x22;
	memset(cold_cohort_bound, 0, sizeof(cold_cohort_bound));
	cold_cohort_bound[0] = *selected;
	cold_cohort_bound_count = 1;
	if (cold_cohort_shape != 0) {
		cold_cohort_bound[1] = *selected;
		cold_cohort_bound_count = 2;
		cold_cohort_bound[0] = cold_cohort_shape <= 2 ? other : history;
		if (cold_cohort_shape == 2) {
			cold_cohort_bound[2] = other;
			cold_cohort_bound[2].claim.identity.origin_owner_incarnation++;
			cold_cohort_bound[2].claim.identity.root_lineage_seq++;
			cold_cohort_bound[2].claim.claim_sha256[0] ^= 0x44;
			cold_cohort_bound_count = 3;
		} else if (cold_cohort_shape == 4) {
			cold_cohort_bound[2] = other;
			cold_cohort_bound_count = 3;
		}
	}
	return rf_side_online_plan_bind_sources_v1(plan, cold_cohort_bound, cold_cohort_bound_count);
}

UT_TEST(test_cold_cohort_other_origins_preserve_selected_consumer)
{
	cold_native_real_scope = cold_native_real_clog = true;
	for (unsigned shape = 1; shape <= 2; shape++) {
		cold_cohort_shape = shape;
		for (unsigned sample = 0; sample < 8; sample++) {
			ClusterRelmapRetentionCensus *census = NULL;
			ClusterRelmapCensusCommitted out = { 0 };
			uint64 serial;
			bool shared = (sample & 1) != 0, aborted = (sample & 2) != 0;
			custody_era_offset = sample & 4 ? UINT64CONST(1) << 32 : 0;
			cold_native_setup(shared, aborted, false);
			serial = cold_original_id();
			UT_ASSERT(serial != 0 && !cluster_undo_recovery_cold_origin_authorized_v1(1));
			for (uint32 i = 0; i < cold_cohort_bound_count; i++)
				UT_ASSERT(rf_side_online_plan_recovery_bound_claim_matches_v1(
					cold_original_plan, &cold_cohort_bound[i]));
			UT_ASSERT(cold_native_execute(&census));
			UT_ASSERT(census != NULL && cold_original_plan != NULL && cold_original_serial.held);
			UT_ASSERT_EQ(cold_original_id(), serial);
			UT_ASSERT_EQ(cold_native_syncs, 4);
			UT_ASSERT_EQ(cold_native_invals, !aborted);
			UT_ASSERT_EQ(cold_native_acks, !aborted && cluster_conf_has_peers());
			UT_ASSERT(cluster_relmap_retention_census_committed_cold(
				census, shared ? InvalidOid : 42, &out));
			UT_ASSERT(out.present && out.generation == (aborted ? 7 : 8));
			UT_ASSERT_EQ(out.map.mappings[0].mapfilenumber, aborted ? 40000 : 40001);
			UT_ASSERT_EQ(cluster_relmap_retention_census_next(census),
						 CLUSTER_RELMAP_CENSUS_DRAINED);
			cold_native_done(census);
			UT_ASSERT(cold_native_clog_last.calls >= 3 && cold_native_clog_last.file_syncs >= 3);
			if (ut_current_failed)
				break;
		}
		if (ut_current_failed)
			break;
	}
	cold_cohort_shape = 0;
	custody_era_offset = 0;
	cold_native_real_scope = cold_native_real_clog = false;
}

UT_TEST(test_cold_cohort_same_origin_generations_refuse_before_native)
{
	cold_native_real_scope = cold_native_real_clog = true;
	for (unsigned shape = 3; shape <= 4; shape++) {
		ClusterRelmapRetentionCensus *census = NULL;
		ClusterRelmapV2View view = { 0 };
		cold_cohort_shape = shape;
		cold_native_setup(false, false, false);
		UT_ASSERT(cold_original_id() != 0);
		UT_ASSERT(!test_relmap_scope_single_source(&prefix_base_expected.source, &execution_root,
												   prefix_base_expected.native_redo,
												   cold_original_id()));
		UT_ASSERT(!cold_native_execute(&census));
		UT_ASSERT(census == NULL && cold_original_plan != NULL && cold_original_serial.held);
		UT_ASSERT_EQ(execution_root_opens + prefix_base_reads + cold_native_syncs
						 + cold_native_invals + cold_native_acks,
					 0);
		UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&custody_image.identity, &view),
					 CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(view.pending_complete && view.has_pending && view.committed_generation == 7);
		cold_native_done(census);
		UT_ASSERT_EQ(cold_native_clog_last.calls, 0);
		if (ut_current_failed)
			break;
	}
	cold_cohort_shape = 0;
	cold_native_real_scope = cold_native_real_clog = false;
}

UT_TEST(test_cold_cohort_foreign_pending_requires_original_directory)
{
	ClusterRelmapRetentionCensus *census = NULL;
	ClusterRelmapV2Image foreign;
	ClusterRelmapV2View view = { 0 };
	uint64 serial;
	int fd;
	cold_native_real_scope = cold_native_real_clog = true;
	cold_cohort_shape = 1;
	cold_native_setup(false, false, false);
	serial = cold_original_id();
	UT_ASSERT(serial != 0 && !cluster_undo_recovery_cold_origin_authorized_v1(1));
	UT_ASSERT(rf_side_online_plan_recovery_bound_claim_matches_v1(cold_original_plan,
																  &cold_cohort_bound[0]));
	UT_ASSERT(
		!rf_side_online_plan_recovery_claim_matches_v1(cold_original_plan, &cold_cohort_bound[0]));
	foreign = custody_image;
	foreign.identity.shared = true;
	foreign.identity.dbid = InvalidOid;
	foreign.pending_owner.node = 1;
	fd = open(test_root, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(fd >= 0);
	relmap_census_file(fd, cold_native_paths[0], &foreign);
	relmap_census_file(fd, cold_native_paths[1], &foreign);
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT(!cold_native_execute(&census));
	UT_ASSERT(census == NULL && cold_original_plan != NULL && cold_original_serial.held);
	UT_ASSERT_EQ(cold_original_id(), serial);
	UT_ASSERT_EQ(prefix_base_reads + cold_native_syncs + cold_native_invals + cold_native_acks, 0);
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&foreign.identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT(view.pending_complete && view.has_pending && view.pending_owner.node == 1
			  && view.committed_generation == 7);
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&custody_image.identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT(view.pending_complete && view.has_pending && view.pending_owner.node == 0
			  && view.committed_generation == 7);
	cold_native_done(census);
	UT_ASSERT_EQ(cold_native_clog_last.calls, 0);
	cold_cohort_shape = 0;
	cold_native_real_scope = cold_native_real_clog = false;
}
