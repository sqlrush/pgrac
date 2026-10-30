/* Original parsed records projected into unresolved fixed-family duties.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_native_owner_routes_complete_commit_and_zero_message_init_duties)
{
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effects[32]; size_t count = 0;
	const uint16 expected[] = {387, 4, 4, 1024, 1024, 4096, 4096, 384, 384, 8192, 1024};
	full_commit();
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan == NULL) return;
	UT_ASSERT_EQ(cluster_recovery_native_scan_effects_v1(scan, effects, lengthof(effects), &count),
		CLUSTER_RECOVERY_NATIVE_EFFECT);
	UT_ASSERT_EQ(count, lengthof(expected));
	for (size_t i = 0; i < count && i < lengthof(expected); i++)
		UT_ASSERT_EQ(cluster_recovery_native_effect_owner_families_v1(&effects[i]), expected[i]);
	cluster_recovery_native_scan_destroy(scan); scan = NULL;
	{
		xl_invalidations invalidations = {0};
		fixture(RM_XACT_ID, XLOG_XACT_INVALIDATIONS);
		invalidations.relcacheInitFileInval = true;
		append(&invalidations, MinSizeOfInvalidations);
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL) return;
		UT_ASSERT_EQ(cluster_recovery_native_scan_primary(scan, effects), CLUSTER_RECOVERY_NATIVE_EFFECT);
		UT_ASSERT_EQ(cluster_recovery_native_effect_owner_families_v1(effects), 384);
		UT_ASSERT_EQ(scan->record.relcache_init_file, true);
	}
	cluster_recovery_native_scan_destroy(scan);
}

UT_TEST(test_native_owner_routes_preserve_prepared_undo_and_complete_mx_array_duties)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		ClusterRecoveryNativeScan *scan = NULL;
		ClusterRecoveryNativeEffect effect;
		size_t count = 0;
		if (mode == 0) (void)native_prepare_fixture(true, 13);
		if (mode == 1) (void)undo_effect_fixture(9, CLUSTER_UNDO_SEGS_PER_INSTANCE + 1);
		if (mode == 2) native_mx_create(300);
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL) continue;
		while (cluster_recovery_native_scan_next(scan, &effect) == CLUSTER_RECOVERY_NATIVE_EFFECT) {
			UT_ASSERT_EQ(cluster_recovery_native_effect_owner_families_v1(&effect),
				mode == 0 ? 32 : mode == 1 ? 8192 : count == 0 ? 24 : 16);
			count++;
		}
		UT_ASSERT(mode == 0 ? count > 13 : mode == 1 ? count == 1 : count == 301);
		cluster_recovery_native_scan_destroy(scan);
	}
}

UT_TEST(test_native_owner_routes_keep_native_slru_and_unknown_duties_distinct)
{
	for (unsigned shape = 0; shape < 4; shape++) {
		ClusterRecoveryNativeScan *scan = NULL; ClusterRecoveryNativeEffect effect;
		native_slru_fixture(shape, 1);
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL) continue;
		UT_ASSERT_EQ(cluster_recovery_native_scan_primary(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
		UT_ASSERT_EQ(cluster_recovery_native_effect_owner_families_v1(&effect), shape < 2 ? 1 : 2);
		effect.kind = 0;
		UT_ASSERT_EQ(cluster_recovery_native_effect_owner_families_v1(&effect), 0);
		effect.kind = CLUSTER_RECOVERY_NATIVE_PREPARE_END + 1;
		UT_ASSERT_EQ(cluster_recovery_native_effect_owner_families_v1(&effect), 0);
		cluster_recovery_native_scan_destroy(scan);
	}
}
