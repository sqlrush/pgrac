/* Full physical pending selection before any native target mutation.
 * Native custody is controlled; ROOT/copy readers use actual files.
 * Author: SqlRush <sqlrush@gmail.com> */
static bool
census_batch_begin(CensusFixture *f, unsigned global_cut, unsigned database_cut, bool clean)
{
	census_fixture(f, true);
	census_native_reset(f);
	if (!clean)
		census_pair(f->fd, 5, 3, database_cut == 0, database_cut != 0, database_cut == 1);
	census_pair(f->fd, InvalidOid, clean ? 0 : 7, !clean && global_cut == 0,
				!clean && global_cut != 0, !clean && global_cut == 1);
	if (clean)
		census_pair(f->fd, 5, 0, false, false, false);
	return census_observe_begin(f) && census_drain(f) == CLUSTER_RELMAP_CENSUS_DRAINED;
}

static bool
census_prepare_all(CensusFixture *f, size_t budget, ClusterRelmapCensusRecoveryBatch **out)
{
	return cluster_relmap_retention_census_recovery_prepare_all(
		f->census, (ClusterWalInputsV1 *)&census_inputs_cookie, budget, out);
}

static void
census_batch_pins_same(CensusFixture *f, const ClusterRelmapRetentionDirectory before[2])
{
	for (unsigned i = 0; i < 2; i++)
		UT_ASSERT_EQ(memcmp(f->origin[i].pins, before[i].pins, sizeof(before[i].pins)), 0);
}

UT_TEST(test_census_recovery_batch_null_refusal)
{
	ClusterRelmapCensusRecoveryBatch *batch = (void *)1;
	CensusFixture f;

	UT_ASSERT(!cluster_relmap_retention_census_recovery_prepare_all(NULL, NULL, 0, &batch));
	UT_ASSERT(batch == NULL);
	cluster_relmap_retention_census_recovery_batch_release(&batch);
	cluster_relmap_retention_census_recovery_batch_release(NULL);
	if (census_native_begin(&f, 5, 2, false)) {
		UT_ASSERT(!census_prepare_all(&f, CENSUS_BUDGET, &batch));
		UT_ASSERT(batch == NULL);
		UT_ASSERT_EQ(census_native_prepares, 0);
		UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
	}
	census_cleanup(&f);
}

UT_TEST(test_census_recovery_batch_captures_every_namespace_before_publication)
{
	for (unsigned g = 0; g < 3; g++) {
		for (unsigned d = 0; d < 3; d++) {
			CensusFixture f;
			ClusterRelmapCensusRecoveryBatch *batch = NULL;
			ClusterRelmapRetentionDirectory before[2];
			uint8 digest[32];

			if (census_batch_begin(&f, g, d, false)) {
				memcpy(before, f.origin, sizeof(before));
				UT_ASSERT(census_prepare_all(&f, CENSUS_BUDGET, &batch));
				UT_ASSERT(batch != NULL);
				UT_ASSERT_EQ(census_native_prepares, 2);
				UT_ASSERT_EQ(census_native_releases, 0);
				UT_ASSERT_EQ(census_arbitrations, 0);
				UT_ASSERT(census_native_handles[0].live && census_native_handles[1].live);
				UT_ASSERT_NE(census_native_handles[0].identity.dbid,
							 census_native_handles[1].identity.dbid);
				UT_ASSERT(cluster_recovery_root_set_digest(f.roots, digest));
				census_batch_pins_same(&f, before);
				cluster_relmap_retention_census_recovery_batch_release(&batch);
				UT_ASSERT(batch == NULL);
				UT_ASSERT_EQ(census_native_releases, 2);
				UT_ASSERT(!census_native_handles[0].live && !census_native_handles[1].live);
				UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
				census_batch_pins_same(&f, before);
			}
			census_cleanup(&f);
		}
	}
}

UT_TEST(test_census_recovery_batch_late_native_failure_cleans_every_local_handle)
{
	const unsigned faults[] = { 1, 2, 3, 6 };
	for (unsigned i = 0; i < lengthof(faults); i++) {
		CensusFixture f;
		ClusterRelmapCensusRecoveryBatch *volatile batch = (void *)1;
		ClusterRelmapRetentionDirectory before[2];
		volatile bool caught = false;

		if (census_batch_begin(&f, 1, 2, false)) {
			memcpy(before, f.origin, sizeof(before));
			census_native_fault_after = 2;
			census_native_fault = faults[i];
			PG_TRY();
			{
				UT_ASSERT(!census_prepare_all(&f, CENSUS_BUDGET,
											  (ClusterRelmapCensusRecoveryBatch **)&batch));
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			UT_ASSERT_EQ(caught, faults[i] == 2);
			UT_ASSERT(batch == NULL);
			UT_ASSERT_EQ(census_native_prepares, 2);
			UT_ASSERT_EQ(census_native_releases, faults[i] <= 2 ? 1 : 2);
			UT_ASSERT_EQ(census_arbitrations, 0);
			UT_ASSERT(!census_native_handles[0].live && !census_native_handles[1].live);
			census_batch_pins_same(&f, before);
			census_refused(&f);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_census_recovery_batch_original_gates_and_memory_budget)
{
	for (unsigned gate = 0; gate < 4; gate++) {
		CensusFixture f;
		ClusterRelmapCensusRecoveryBatch *batch = (void *)1;
		ClusterRelmapRetentionDirectory before[2];
		uint8 digest[32];

		if (census_batch_begin(&f, 1, 2, false)) {
			memcpy(before, f.origin, sizeof(before));
			census_merger_held = gate != 0;
			census_namespace_x = gate != 1;
			cluster_shared_catalog = gate != 2;
			UT_ASSERT(!census_prepare_all(&f, gate == 3 ? 0 : CENSUS_BUDGET, &batch));
			UT_ASSERT(batch == NULL);
			UT_ASSERT_EQ(census_native_prepares, 0);
			census_batch_pins_same(&f, before);
			if (gate == 3)
				census_refused(&f);
			else
				UT_ASSERT(cluster_recovery_root_set_digest(f.roots, digest));
		}
		census_cleanup(&f);
	}
	census_merger_held = census_namespace_x = cluster_shared_catalog = true;
}

UT_TEST(test_census_recovery_batch_final_whole_root_and_earlier_pin_recheck)
{
	for (unsigned fault = 7; fault <= 8; fault++) {
		CensusFixture f;
		ClusterRelmapCensusRecoveryBatch *batch = (void *)1;
		ClusterRelmapRetentionDirectory before[2];

		if (census_batch_begin(&f, 1, 2, false)) {
			memcpy(before, f.origin, sizeof(before));
			census_native_fault_after = 2;
			census_native_fault = fault;
			UT_ASSERT(!census_prepare_all(&f, CENSUS_BUDGET, &batch));
			UT_ASSERT(batch == NULL);
			UT_ASSERT_EQ(census_native_prepares, 2);
			UT_ASSERT_EQ(census_native_releases, 2);
			UT_ASSERT_EQ(census_arbitrations, 0);
			if (fault == 7) {
				UT_ASSERT(census_native_mutated_pin != NULL);
				if (census_native_mutated_pin != NULL)
					census_native_mutated_pin->wal_end--;
			} else
				UT_ASSERT_EQ(unlinkat(f.fd, "base/5/99999", 0), 0);
			census_batch_pins_same(&f, before);
			census_refused(&f);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_census_recovery_batch_clean_namespaces_need_no_native_handle)
{
	for (unsigned clean = 0; clean < 2; clean++) {
		CensusFixture f;
		ClusterRelmapCensusRecoveryBatch *batch = NULL;
		uint8 digest[32];

		if (clean ? census_batch_begin(&f, 0, 0, true) : census_native_begin(&f, 5, 2, true)) {
			UT_ASSERT(census_prepare_all(&f, CENSUS_BUDGET, &batch));
			UT_ASSERT(batch != NULL);
			UT_ASSERT_EQ(census_native_prepares, clean ? 0 : 1);
			UT_ASSERT_EQ(census_arbitrations, 0);
			cluster_relmap_retention_census_recovery_batch_release(&batch);
			UT_ASSERT_EQ(census_native_releases, clean ? 0 : 1);
			UT_ASSERT(cluster_recovery_root_set_digest(f.roots, digest));
			for (unsigned i = 0; i < 2; i++)
				UT_ASSERT_EQ(f.origin[i].state, CLUSTER_RELMAP_RETENTION_BUILDING);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_census_batch_consume_all_original_namespaces)
{
	for (unsigned g = 0; g < 3; g++) {
		for (unsigned d = 0; d < 3; d++) {
			CensusFixture f;
			ClusterRelmapCensusRecoveryBatch *batch = NULL;
			ClusterRelmapRetentionDirectory before[2];

			if (census_batch_begin(&f, g, d, false)) {
				memcpy(before, f.origin, sizeof(before));
				UT_ASSERT(census_prepare_all(&f, CENSUS_BUDGET, &batch));
				census_batch_consuming = true;
				UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_all(&batch),
							 CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
				UT_ASSERT(batch == NULL);
				UT_ASSERT_EQ(census_native_prepares, 2);
				UT_ASSERT_EQ(census_arbitrations, 2);
				UT_ASSERT_EQ(census_native_releases, 2);
				census_batch_pins_same(&f, before);
				census_refused(&f);
				UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_all(&batch),
							 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
				UT_ASSERT_EQ(census_arbitrations, 2);
				cluster_relmap_retention_census_recovery_batch_release(&batch);
			}
			census_cleanup(&f);
		}
	}
}

UT_TEST(test_census_batch_consume_stops_at_first_unknown_or_error)
{
	for (unsigned after = 1; after <= 2; after++) {
		for (unsigned mode = 1; mode <= 2; mode++) {
			CensusFixture f;
			ClusterRelmapCensusRecoveryBatch *volatile batch = NULL;
			ClusterRelmapRetentionDirectory before[2];
			volatile bool caught = false;

			if (census_batch_begin(&f, 1, 2, false)) {
				memcpy(before, f.origin, sizeof(before));
				UT_ASSERT(census_prepare_all(&f, CENSUS_BUDGET,
											(ClusterRelmapCensusRecoveryBatch **)&batch));
				census_batch_consuming = true;
				census_arbitration_after = after;
				census_arbitration_mode = mode;
				PG_TRY();
				{
					UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_all(
									 (ClusterRelmapCensusRecoveryBatch **)&batch),
								 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
				}
				PG_CATCH();
				{
					caught = true;
				}
				PG_END_TRY();
				UT_ASSERT_EQ(caught, mode == 2);
				UT_ASSERT(batch == NULL);
				UT_ASSERT_EQ(census_arbitrations, after);
				UT_ASSERT_EQ(census_native_releases, 2);
				census_batch_pins_same(&f, before);
				census_refused(&f);
				cluster_relmap_retention_census_recovery_batch_release(
					(ClusterRelmapCensusRecoveryBatch **)&batch);
			}
			census_cleanup(&f);
		}
	}
}

UT_TEST(test_census_batch_consume_missing_ownership_cancels_local_custody_only)
{
	UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_all(NULL),
				 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	for (unsigned gate = 0; gate < 3; gate++) {
		CensusFixture f;
		ClusterRelmapCensusRecoveryBatch *batch = NULL;
		ClusterRelmapRetentionDirectory before[2];
		uint8 digest[32];

		if (census_batch_begin(&f, 1, 2, false)) {
			memcpy(before, f.origin, sizeof(before));
			UT_ASSERT(census_prepare_all(&f, CENSUS_BUDGET, &batch));
			census_merger_held = gate != 0;
			census_namespace_x = gate != 1;
			cluster_shared_catalog = gate != 2;
			UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_all(&batch),
						 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
			UT_ASSERT(batch == NULL);
			UT_ASSERT_EQ(census_arbitrations, 0);
			UT_ASSERT_EQ(census_native_releases, 2);
			UT_ASSERT(cluster_recovery_root_set_digest(f.roots, digest));
			UT_ASSERT_EQ(memcmp(f.origin, before, sizeof(before)), 0);
			UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
			cluster_relmap_retention_census_recovery_batch_release(&batch);
		}
		census_cleanup(&f);
	}
	census_merger_held = census_namespace_x = cluster_shared_catalog = true;
}

UT_TEST(test_census_batch_consume_rechecks_original_whole_root_sources_and_pins)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		CensusFixture f;
		ClusterRelmapCensusRecoveryBatch *batch = NULL;
		ClusterRelmapRetentionDirectory before[2];
		ClusterRelmapRetentionPin *pin = NULL;

		if (census_batch_begin(&f, 1, 2, false)) {
			memcpy(before, f.origin, sizeof(before));
			UT_ASSERT(census_prepare_all(&f, CENSUS_BUDGET, &batch));
			if (fault == 0)
				write_file(f.fd, "base/5/99999");
			if (fault == 1)
				census_pair(f.fd, 5, 3, true, true, false);
			if (fault == 2) {
				pin = (ClusterRelmapRetentionPin *)census_pin(&f.origin[0], 5);
				UT_ASSERT(pin != NULL);
				if (pin != NULL)
					pin->wal_end++;
			}
			if (fault == 3)
				f.origin[1].last_issued++;
			if (fault == 4)
				census_native_sources[1].source.claim.identity.storage_uuid[0] ^= 1;
			if (fault == 5)
				census_native_scope_current = false;
			if (fault == 6)
				cluster_recovery_root_set_invalidate(f.roots);
			census_batch_consuming = true;
			UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_all(&batch),
						 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
			UT_ASSERT(batch == NULL);
			UT_ASSERT_EQ(census_arbitrations, 0);
			UT_ASSERT_EQ(census_native_releases, 2);
			if (pin != NULL)
				pin->wal_end--;
			if (fault == 0)
				UT_ASSERT_EQ(unlinkat(f.fd, "base/5/99999", 0), 0);
			census_batch_pins_same(&f, before);
			census_refused(&f);
			cluster_relmap_retention_census_recovery_batch_release(&batch);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_census_batch_consume_post_native_drift_stops_and_never_completes_directory)
{
	for (unsigned after = 1; after <= 2; after++) {
		for (unsigned drift = 1; drift <= 7; drift++) {
			CensusFixture f;
			ClusterRelmapCensusRecoveryBatch *batch = NULL;
			ClusterRelmapRetentionDirectory before[2];

			if (census_batch_begin(&f, 1, 2, false)) {
				memcpy(before, f.origin, sizeof(before));
				UT_ASSERT(census_prepare_all(&f, CENSUS_BUDGET, &batch));
				census_batch_consuming = true;
				census_arbitration_drift = drift;
				census_arbitration_drift_after = after;
				UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_all(&batch),
							 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
				UT_ASSERT(batch == NULL);
				UT_ASSERT_EQ(census_arbitrations, after);
				UT_ASSERT_EQ(census_native_releases, 2);
				if (census_native_mutated_pin != NULL)
					census_native_mutated_pin->wal_end--;
				census_batch_pins_same(&f, before);
				census_refused(&f);
				cluster_relmap_retention_census_recovery_batch_release(&batch);
			}
			census_cleanup(&f);
		}
	}
}

UT_TEST(test_census_batch_consume_empty_set_does_not_publish_completion)
{
	CensusFixture f;
	ClusterRelmapCensusRecoveryBatch *batch = NULL;
	uint8 digest[32];

	if (census_batch_begin(&f, 0, 0, true)) {
		UT_ASSERT(census_prepare_all(&f, CENSUS_BUDGET, &batch));
		UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_all(&batch),
					 CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
		UT_ASSERT(batch == NULL);
		UT_ASSERT_EQ(census_arbitrations, 0);
		UT_ASSERT_EQ(census_native_prepares, 0);
		UT_ASSERT_EQ(census_native_releases, 0);
		UT_ASSERT(cluster_recovery_root_set_digest(f.roots, digest));
		for (unsigned i = 0; i < 2; i++)
			UT_ASSERT_EQ(f.origin[i].state, CLUSTER_RELMAP_RETENTION_BUILDING);
		cluster_relmap_retention_census_recovery_batch_release(&batch);
	}
	census_cleanup(&f);
}

UT_TEST(test_census_batch_consume_preflight_error_releases_every_handle)
{
	CensusFixture f;
	ClusterRelmapCensusRecoveryBatch *volatile batch = NULL;
	ClusterRelmapRetentionDirectory before[2];
	volatile bool caught = false;

	if (census_batch_begin(&f, 1, 2, false)) {
		memcpy(before, f.origin, sizeof(before));
		UT_ASSERT(census_prepare_all(&f, CENSUS_BUDGET,
									(ClusterRelmapCensusRecoveryBatch **)&batch));
		census_native_scope_error = true;
		PG_TRY();
		{
			(void)cluster_relmap_retention_census_recovery_arbitrate_all(
				(ClusterRelmapCensusRecoveryBatch **)&batch);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		census_native_scope_error = false;
		UT_ASSERT(caught);
		UT_ASSERT(batch == NULL);
		UT_ASSERT_EQ(census_arbitrations, 0);
		UT_ASSERT_EQ(census_native_releases, 2);
		census_batch_pins_same(&f, before);
		census_refused(&f);
		cluster_relmap_retention_census_recovery_batch_release(
			(ClusterRelmapCensusRecoveryBatch **)&batch);
	}
	census_cleanup(&f);
}
