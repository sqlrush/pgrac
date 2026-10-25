/* Retained batches borrow every original protection, not only selected pins.
 * Physical ROOT/pairs are real; source/native providers are controlled.
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterRelmapRetentionDirectory census_all_expected[2];
static ClusterRelmapRetentionPin *census_all_extra;
static unsigned census_all_fault, census_all_after;

static void
census_all_add_original(CensusFixture *f, unsigned origin)
{
	ClusterRelmapRetentionKey key = { 0 };
	key.identity = (ClusterRelmapV2Namespace){ 817, 91, 19, false };
	key.generation = 3;
	key.origin = f->origin[origin].origin;
	key.top_full_xid = CENSUS_FULL;
	key.admitted_incarnation = 909;
	key.boot_incarnation = 202;
	UT_ASSERT_EQ(cluster_relmap_retention_build_add(&f->origin[origin], &key, 101),
				 CLUSTER_RELMAP_RETENTION_OK);
	census_all_extra = (ClusterRelmapRetentionPin *)census_pin(&f->origin[origin], 19);
	UT_ASSERT(census_all_extra != NULL);
	memcpy(census_all_expected, f->origin, sizeof(census_all_expected));
}

static void
census_all_change_original(void)
{
	CensusFixture *f = census_native_fixture;

	if (census_all_fault == 1 && census_all_extra != NULL)
		census_all_extra->wal_end++;
	if (census_all_fault == 2 && census_all_extra != NULL)
		census_all_extra->occupied = false;
	if (census_all_fault == 3) {
		ClusterRelmapRetentionPin *last = &f->origin[1].pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1];
		UT_ASSERT(!last->occupied);
		last->wal_end = 31337;
	}
	memcpy(census_all_expected, f->origin, sizeof(census_all_expected));
}

static void
census_all_after_prepare(void)
{
	if (census_native_prepares == census_all_after)
		census_all_change_original();
}

static void
census_all_after_arbitration(void)
{
	if (census_arbitrations == census_all_after)
		census_all_change_original();
}

UT_TEST(test_retained_batch_prepare_keeps_unselected_and_unused_original_pins)
{
	for (unsigned origin = 0; origin < 2; origin++)
		for (unsigned after = 1; after <= 2; after++)
			for (unsigned fault = 1; fault <= 3; fault++) {
				CensusFixture f;
				ClusterRelmapCensusRecoveryBatch *batch = NULL;

				if (census_batch_begin(&f, 1, 2, false)) {
					census_all_add_original(&f, origin);
					census_all_fault = fault;
					census_all_after = after;
					census_native_after_prepare = census_all_after_prepare;
					UT_ASSERT(!census_prepare_all(&f, CENSUS_BUDGET, &batch));
					UT_ASSERT(batch == NULL);
					UT_ASSERT_EQ(census_native_prepares, after);
					UT_ASSERT_EQ(census_native_releases, after);
					UT_ASSERT_EQ(census_arbitrations, 0);
					census_batch_pins_same(&f, census_all_expected);
					census_refused(&f);
				}
				cluster_relmap_retention_census_recovery_batch_release(&batch);
				census_cleanup(&f);
				if (ut_current_failed)
					return;
			}
}

UT_TEST(test_retained_batch_all_original_pins_bind_interval_and_empty_batch)
{
	for (unsigned clean = 0; clean < 2; clean++)
		for (unsigned origin = 0; origin < 2; origin++)
			for (unsigned fault = 0; fault <= 3; fault++) {
				CensusFixture f;
				ClusterRelmapCensusRecoveryBatch *batch = NULL;

				if (census_batch_begin(&f, 1, 2, clean != 0)) {
					census_all_add_original(&f, origin);
					UT_ASSERT(census_prepare_all(&f, CENSUS_BUDGET, &batch));
					UT_ASSERT(batch != NULL);
					census_all_fault = fault;
					census_all_change_original();
					census_batch_consuming = true;
					UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_all(&batch),
								 fault == 0 ? CLUSTER_RELMAP_V2_CLEARED_POSTREAD
											: CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
					UT_ASSERT(batch == NULL);
					UT_ASSERT_EQ(census_arbitrations, fault == 0 && !clean ? 2 : 0);
					UT_ASSERT_EQ(census_native_prepares, clean ? 0 : 2);
					UT_ASSERT_EQ(census_native_releases, clean ? 0 : 2);
					census_batch_pins_same(&f, census_all_expected);
					if (fault != 0 || !clean)
						census_refused(&f);
					else {
						uint8 digest[32];
						UT_ASSERT(cluster_recovery_root_set_digest(f.roots, digest));
						UT_ASSERT_EQ(f.origin[0].state, CLUSTER_RELMAP_RETENTION_BUILDING);
						UT_ASSERT_EQ(f.origin[1].state, CLUSTER_RELMAP_RETENTION_BUILDING);
					}
				}
				cluster_relmap_retention_census_recovery_batch_release(&batch);
				census_cleanup(&f);
				if (ut_current_failed)
					return;
			}
}

UT_TEST(test_retained_batch_native_clear_rechecks_every_original_pin)
{
	for (unsigned origin = 0; origin < 2; origin++)
		for (unsigned after = 1; after <= 2; after++)
			for (unsigned fault = 1; fault <= 3; fault++) {
				CensusFixture f;
				ClusterRelmapCensusRecoveryBatch *batch = NULL;

				if (census_batch_begin(&f, 1, 2, false)) {
					census_all_add_original(&f, origin);
					UT_ASSERT(census_prepare_all(&f, CENSUS_BUDGET, &batch));
					census_all_fault = fault;
					census_all_after = after;
					census_batch_consuming = true;
					census_native_after_arbitration = census_all_after_arbitration;
					UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_all(&batch),
								 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
					UT_ASSERT(batch == NULL);
					UT_ASSERT_EQ(census_arbitrations, after);
					UT_ASSERT_EQ(census_native_releases, 2);
					census_batch_pins_same(&f, census_all_expected);
					census_refused(&f);
				}
				cluster_relmap_retention_census_recovery_batch_release(&batch);
				census_cleanup(&f);
				if (ut_current_failed)
					return;
			}
}

static void
census_all_unknown_error(void)
{
	if (census_arbitrations == census_arbitration_after) {
		census_all_change_original();
		census_native_scope_error = true;
	}
}

UT_TEST(test_retained_batch_native_unknown_still_rechecks_original_sources)
{
	for (unsigned after = 1; after <= 2; after++) {
		CensusFixture f;
		ClusterRelmapCensusRecoveryBatch *volatile batch = NULL;
		volatile bool caught = false;

		if (census_batch_begin(&f, 1, 2, false)) {
			census_all_add_original(&f, 1);
			UT_ASSERT(census_prepare_all(&f, CENSUS_BUDGET,
										(ClusterRelmapCensusRecoveryBatch **)&batch));
			census_all_fault = 1;
			census_batch_consuming = true;
			census_arbitration_after = after;
			census_arbitration_mode = 1;
			census_native_after_arbitration = census_all_unknown_error;
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
			UT_ASSERT(caught && batch == NULL);
			UT_ASSERT_EQ(census_arbitrations, after);
			UT_ASSERT_EQ(census_native_releases, 2);
			census_batch_pins_same(&f, census_all_expected);
			census_refused(&f);
		}
		cluster_relmap_retention_census_recovery_batch_release(
			(ClusterRelmapCensusRecoveryBatch **)&batch);
		census_cleanup(&f);
		if (ut_current_failed)
			return;
	}
}
