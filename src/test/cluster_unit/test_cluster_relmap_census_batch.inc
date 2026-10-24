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
