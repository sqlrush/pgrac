/* Actual ROOT/pair/batch/fresh-census composition. Original native checkpoint,
 * scope and arbiter leaves are controlled; old protection is never released.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned census_execution_post_fault;

static bool
census_execution_batch_begin(CensusFixture *f, bool clean, bool drain)
{
	census_cold_reset();
	census_fixture(f, !clean);
	census_pair(f->fd, InvalidOid, clean ? 0 : 3, false, !clean, false);
	census_native_reset(f);
	cluster_relmap_retention_invalidate(&f->origin[1]);
	f->directories[0] = &f->origin[0];
	f->roots = begin_roots(f->fd, 4 * 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(f->roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT_EQ(cluster_relmap_retention_census_begin(f->roots, 817, 91, root_uuid, f->directories,
													   1, CENSUS_BUDGET, &f->census),
				 CLUSTER_RELMAP_CENSUS_MORE);
	return f->census != NULL && (!drain || census_drain(f) == CLUSTER_RELMAP_CENSUS_DRAINED);
}

static void
census_execution_after_arbitration(void)
{
	UT_ASSERT(census_arbitrations >= 1 && census_arbitrations <= 2);
	census_native_identity = census_native_handles[census_arbitrations - 1].identity;
	census_cold_clear_pair_bytes();
	if (census_arbitrations != 2)
		return;
	if (census_execution_post_fault == 1)
		census_cold_selected.scope_serial++;
	if (census_execution_post_fault == 2)
		census_native_fixture->origin[0].pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1].wal_end++;
	if (census_execution_post_fault == 3)
		census_merger_held = false;
	if (census_execution_post_fault == 4)
		census_cold_fault = 8;
}

static void
census_execution_no_local_handles(void)
{
	for (size_t i = 0; i < lengthof(census_native_handles); i++)
		UT_ASSERT(!census_native_handles[i].live);
}

UT_TEST(test_execution_batch_all_pending_then_one_fresh_census_keeps_old_debt)
{
	CensusFixture f;
	ClusterRelmapCensusRecoveryBatch *batch = NULL;
	ClusterRelmapRetentionCensus *fresh = NULL;
	ClusterRelmapRetentionDirectory saved[2];
	ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;
	unsigned images = 0;
	if (census_execution_batch_begin(&f, false, true)) {
		memcpy(saved, f.origin, sizeof(saved));
		/* Execution cannot re-enter the old pass-1 read scope. */
		census_native_scope_error = true;
		UT_ASSERT(cluster_relmap_retention_census_recovery_prepare_all_cold(f.census, CENSUS_BUDGET,
																			&batch));
		UT_ASSERT(batch != NULL && census_native_prepares == 2 && census_native_releases == 0);
		UT_ASSERT(census_native_handles[0].identity.dbid != census_native_handles[1].identity.dbid);
		census_batch_consuming = true;
		census_execution_post_fault = 0;
		census_native_after_arbitration = census_execution_after_arbitration;
		UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_all_cold(&batch),
					 CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
		UT_ASSERT(batch == NULL && census_arbitrations == 2 && census_native_releases == 2);
		census_execution_no_local_handles();
		UT_ASSERT(cluster_relmap_retention_census_restart_cold(f.census, 1024 * 1024, CENSUS_BUDGET,
															   &fresh));
		for (unsigned i = 0; fresh != NULL && i < 20000 && result == CLUSTER_RELMAP_CENSUS_MORE;
			 i++) {
			ClusterRelmapCensusCommitted image;
			result = cluster_relmap_retention_census_next_committed(fresh, &image);
			if (image.present) {
				images++;
				UT_ASSERT_EQ(image.map.mappings[0].mapfilenumber, 40000);
			}
		}
		UT_ASSERT_EQ(result, CLUSTER_RELMAP_CENSUS_DRAINED);
		UT_ASSERT_EQ(images, 2);
		census_batch_pins_same(&f, saved);
		UT_ASSERT(census_pin(&f.origin[0], 5) != NULL);
		UT_ASSERT(census_pin(&f.origin[0], InvalidOid) != NULL);
		census_refused(&f);
	}
	cluster_relmap_retention_census_recovery_batch_release(&batch);
	cluster_relmap_retention_census_destroy(fresh);
	census_cold_consume_cleanup(&f);
}

UT_TEST(test_execution_batch_initial_gates_unsupported_and_empty_do_not_complete)
{
	for (unsigned mode = 0; mode < 7; mode++) {
		CensusFixture f;
		ClusterRelmapCensusRecoveryBatch *batch = NULL;
		uint8 digest[32];
		bool ready = mode == 5 ? census_batch_begin(&f, 2, 2, false)
							   : census_execution_batch_begin(&f, mode == 6, mode != 0);
		if (ready) {
			cluster_shared_catalog = mode != 1;
			census_merger_held = mode != 2;
			census_namespace_x = mode != 3;
			UT_ASSERT(!cluster_relmap_retention_census_recovery_prepare_all_cold(
				f.census, mode == 4 ? 1 : CENSUS_BUDGET, &batch));
			UT_ASSERT(batch == NULL && census_native_prepares == 0 && census_arbitrations == 0);
			UT_ASSERT_EQ(cluster_recovery_root_set_digest(f.roots, digest), mode < 4 || mode == 5);
			cluster_shared_catalog = census_merger_held = census_namespace_x = true;
			if (mode < 4) {
				UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
				UT_ASSERT(cluster_relmap_retention_census_recovery_prepare_all_cold(
					f.census, CENSUS_BUDGET, &batch));
				UT_ASSERT(batch != NULL);
				cluster_relmap_retention_census_recovery_batch_release(&batch);
			}
		}
		census_cold_consume_cleanup(&f);
	}
}

UT_TEST(test_execution_batch_late_native_source_or_root_failure_releases_every_handle)
{
	const unsigned faults[] = { 1, 2, 30, 31, 32, 33, 34, 35, 36 };
	for (unsigned k = 0; k < lengthof(faults); k++) {
		CensusFixture f;
		ClusterRelmapCensusRecoveryBatch *volatile batch = NULL;
		ClusterRelmapRetentionDirectory saved[2];
		volatile bool caught = false;
		unsigned fault = faults[k];
		if (census_execution_batch_begin(&f, false, true)) {
			memcpy(saved, f.origin, sizeof(saved));
			if (fault < 3) {
				census_native_fault_after = 2;
				census_native_fault = fault;
			} else
				census_cold_fault = fault;
			PG_TRY();
			{
				UT_ASSERT(!cluster_relmap_retention_census_recovery_prepare_all_cold(
					f.census, CENSUS_BUDGET, (ClusterRelmapCensusRecoveryBatch **)&batch));
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			UT_ASSERT_EQ(caught, fault == 2 || fault == 36);
			UT_ASSERT(batch == NULL && census_native_prepares == 2 && census_arbitrations == 0);
			UT_ASSERT_EQ(census_native_releases, fault < 3 ? 1 : 2);
			if (fault == 34)
				saved[0].pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1].wal_end++;
			census_batch_pins_same(&f, saved);
			census_execution_no_local_handles();
			census_refused(&f);
		}
		census_cold_consume_cleanup(&f);
	}
}

UT_TEST(test_execution_batch_unknown_or_error_stops_and_cannot_seed_observation)
{
	for (unsigned after = 1; after <= 2; after++)
		for (unsigned mode = 1; mode <= 2; mode++) {
			CensusFixture f;
			ClusterRelmapCensusRecoveryBatch *volatile batch = NULL;
			ClusterRecoveryRootSet *fresh = NULL;
			ClusterRelmapRetentionDirectory saved[2];
			volatile bool caught = false;
			if (census_execution_batch_begin(&f, false, true)) {
				memcpy(saved, f.origin, sizeof(saved));
				UT_ASSERT(cluster_relmap_retention_census_recovery_prepare_all_cold(
					f.census, CENSUS_BUDGET, (ClusterRelmapCensusRecoveryBatch **)&batch));
				census_batch_consuming = true;
				census_arbitration_after = after;
				census_arbitration_mode = mode;
				PG_TRY();
				{
					UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_all_cold(
									 (ClusterRelmapCensusRecoveryBatch **)&batch),
								 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
				}
				PG_CATCH();
				{
					caught = true;
				}
				PG_END_TRY();
				UT_ASSERT_EQ(caught, mode == 2);
				UT_ASSERT(batch == NULL && census_arbitrations == after
						  && census_native_releases == 2);
				UT_ASSERT(
					!cluster_relmap_retention_census_reobserve_cold(f.census, 1024 * 1024, &fresh));
				UT_ASSERT(fresh == NULL);
				census_batch_pins_same(&f, saved);
				census_execution_no_local_handles();
				census_refused(&f);
			}
			cluster_relmap_retention_census_recovery_batch_release(
				(ClusterRelmapCensusRecoveryBatch **)&batch);
			census_cold_consume_cleanup(&f);
		}
}

UT_TEST(test_execution_batch_checks_after_last_clear_and_unknown_including_error)
{
	for (unsigned unknown = 0; unknown < 2; unknown++)
		for (unsigned fault = 1; fault <= 4; fault++) {
			CensusFixture f;
			ClusterRelmapCensusRecoveryBatch *volatile batch = NULL;
			ClusterRelmapRetentionDirectory saved[2];
			ClusterRecoveryRootSet *fresh = NULL;
			volatile bool caught = false;
			if (census_execution_batch_begin(&f, false, true)) {
				memcpy(saved, f.origin, sizeof(saved));
				UT_ASSERT(cluster_relmap_retention_census_recovery_prepare_all_cold(
					f.census, CENSUS_BUDGET, (ClusterRelmapCensusRecoveryBatch **)&batch));
				census_batch_consuming = true;
				census_execution_post_fault = fault;
				census_native_after_arbitration = census_execution_after_arbitration;
				census_arbitration_mode = unknown;
				census_arbitration_after = 2;
				PG_TRY();
				{
					UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_all_cold(
									 (ClusterRelmapCensusRecoveryBatch **)&batch),
								 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
				}
				PG_CATCH();
				{
					caught = true;
				}
				PG_END_TRY();
				UT_ASSERT_EQ(caught, fault == 4);
				UT_ASSERT(batch == NULL && census_arbitrations == 2 && census_native_releases == 2);
				if (fault == 2)
					saved[0].pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1].wal_end++;
				UT_ASSERT(
					!cluster_relmap_retention_census_reobserve_cold(f.census, 1024 * 1024, &fresh));
				UT_ASSERT(fresh == NULL);
				census_batch_pins_same(&f, saved);
				census_execution_no_local_handles();
				census_refused(&f);
			}
			cluster_relmap_retention_census_recovery_batch_release(
				(ClusterRelmapCensusRecoveryBatch **)&batch);
			census_cold_consume_cleanup(&f);
		}
}

UT_TEST(test_execution_batch_preflight_whole_root_source_and_all_pins_are_current)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		CensusFixture f;
		ClusterRelmapCensusRecoveryBatch *volatile batch = NULL;
		ClusterRelmapRetentionDirectory saved[2];
		volatile bool caught = false;
		if (census_execution_batch_begin(&f, false, true)) {
			memcpy(saved, f.origin, sizeof(saved));
			UT_ASSERT(cluster_relmap_retention_census_recovery_prepare_all_cold(
				f.census, CENSUS_BUDGET, (ClusterRelmapCensusRecoveryBatch **)&batch));
			if (fault == 0)
				census_cold_selected.scope_serial++;
			if (fault == 1)
				f.origin[0].pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1].wal_end++;
			if (fault == 2)
				root_file(f.fd, "unrelated", "warm", 4);
			if (fault == 3)
				census_cold_fault = 8;
			cluster_shared_catalog = fault != 4;
			census_merger_held = fault != 5;
			census_namespace_x = fault != 6;
			PG_TRY();
			{
				UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_all_cold(
								 (ClusterRelmapCensusRecoveryBatch **)&batch),
							 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			UT_ASSERT_EQ(caught, fault == 3);
			UT_ASSERT(batch == NULL && census_arbitrations == 0 && census_native_releases == 2);
			if (fault == 1)
				saved[0].pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1].wal_end++;
			census_batch_pins_same(&f, saved);
			census_execution_no_local_handles();
			if (fault < 4)
				census_refused(&f);
			else {
				uint8 digest[32];
				UT_ASSERT(cluster_recovery_root_set_digest(f.roots, digest));
				UT_ASSERT_EQ(f.origin[0].state, CLUSTER_RELMAP_RETENTION_BUILDING);
			}
		}
		cluster_relmap_retention_census_recovery_batch_release(
			(ClusterRelmapCensusRecoveryBatch **)&batch);
		census_cold_consume_cleanup(&f);
	}
}

UT_TEST(test_execution_batch_wrong_consumer_cancels_only_local_custody)
{
	for (unsigned cold = 0; cold < 2; cold++) {
		CensusFixture f;
		ClusterRelmapCensusRecoveryBatch *batch = NULL;
		if (census_execution_batch_begin(&f, false, true)) {
			uint8 digest[32];
			if (cold)
				UT_ASSERT(cluster_relmap_retention_census_recovery_prepare_all_cold(
					f.census, CENSUS_BUDGET, &batch));
			else
				UT_ASSERT(census_prepare_all(&f, CENSUS_BUDGET, &batch));
			UT_ASSERT_EQ(cold ? cluster_relmap_retention_census_recovery_arbitrate_all(&batch)
							  : cluster_relmap_retention_census_recovery_arbitrate_all_cold(&batch),
						 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
			UT_ASSERT(batch == NULL && census_arbitrations == 0 && census_native_releases == 2);
			UT_ASSERT(cluster_recovery_root_set_digest(f.roots, digest));
			UT_ASSERT_EQ(f.origin[0].state, CLUSTER_RELMAP_RETENTION_BUILDING);
			census_execution_no_local_handles();
		}
		cluster_relmap_retention_census_recovery_batch_release(&batch);
		census_cold_consume_cleanup(&f);
	}
}

UT_TEST(test_execution_batch_known_aliases_and_null_custody_preserve_borrowed_storage)
{
	CensusFixture f;
	ClusterRelmapCensusRecoveryBatch *batch = NULL;
	batch = (void *)1;
	UT_ASSERT(!cluster_relmap_retention_census_recovery_prepare_all_cold(NULL, 0, &batch));
	UT_ASSERT(batch == NULL);
	batch = NULL;
	UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_all_cold(NULL),
				 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_all_cold(&batch),
				 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	if (census_execution_batch_begin(&f, false, true)) {
		ClusterRelmapRetentionDirectory saved[2];
		memcpy(saved, f.origin, sizeof(saved));
		UT_ASSERT(!cluster_relmap_retention_census_recovery_prepare_all_cold(
			f.census, CENSUS_BUDGET, (ClusterRelmapCensusRecoveryBatch **)&f.origin[0]));
		UT_ASSERT(!cluster_relmap_retention_census_recovery_prepare_all_cold(
			f.census, CENSUS_BUDGET, (ClusterRelmapCensusRecoveryBatch **)f.census));
		UT_ASSERT_EQ(memcmp(saved, f.origin, sizeof(saved)), 0);
		UT_ASSERT(cluster_relmap_retention_census_recovery_prepare_all_cold(f.census, CENSUS_BUDGET,
																			&batch));
		if (batch != NULL) {
			ClusterRelmapCensusRecoveryBatch **alias
				= (void *)&f.origin[0].pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1].wal_end;
			*alias = batch;
			UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_all_cold(alias),
						 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
			UT_ASSERT(*alias == batch && census_arbitrations == 0 && census_native_releases == 0);
			f.origin[0].pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1].wal_end = 0;
			UT_ASSERT_EQ(memcmp(saved, f.origin, sizeof(saved)), 0);
			cluster_relmap_retention_census_recovery_batch_release(&batch);
			census_execution_no_local_handles();
		}
	}
	census_cold_consume_cleanup(&f);
}
