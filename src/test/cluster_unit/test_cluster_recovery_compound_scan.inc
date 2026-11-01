/* Reuse the complete original parser/walker without touching its cursor.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_compound_snapshot_retains_commit_prepare_and_undo_after_drain)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		ClusterRecoveryNativeScan *scan = NULL, saved;
		ClusterRecoveryNativeEffect expected[64], actual[64], next;
		size_t count = 0, required = 0;
		if (mode == 0) full_commit();
		if (mode == 1) (void)native_prepare_fixture(true, 13);
		if (mode == 2) (void)undo_effect_fixture(9, CLUSTER_UNDO_SEGS_PER_INSTANCE + 1);
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL) continue;
		while (cluster_recovery_native_scan_next(scan, &next) == CLUSTER_RECOVERY_NATIVE_EFFECT) {
			UT_ASSERT(count < lengthof(expected));
			if (count < lengthof(expected)) expected[count++] = next;
		}
		saved = *scan;
		UT_ASSERT_EQ(cluster_recovery_native_scan_effects_v1(scan, NULL, 0, &required),
			CLUSTER_RECOVERY_NATIVE_MORE);
		UT_ASSERT_EQ(required, count);
		UT_ASSERT_EQ(cluster_recovery_native_scan_effects_v1(scan, actual, lengthof(actual), &required),
			CLUSTER_RECOVERY_NATIVE_EFFECT);
		UT_ASSERT_EQ(required, count); UT_ASSERT(memcmp(scan, &saved, sizeof(saved)) == 0);
		for (size_t i = 0; i < count; i++) {
			UT_ASSERT_EQ(actual[i].kind, expected[i].kind);
			UT_ASSERT_EQ(actual[i].ordinal, expected[i].ordinal);
			UT_ASSERT(actual[i].record == expected[i].record);
			UT_ASSERT(actual[i].bytes == expected[i].bytes);
			UT_ASSERT_EQ(actual[i].length, expected[i].length);
		}
		if (mode == 0) {
			UT_ASSERT(count > 9);
			UT_ASSERT(actual[0].record->xinfo & XACT_COMPLETION_FORCE_SYNC_COMMIT);
			UT_ASSERT(actual[0].record->relcache_init_file);
			UT_ASSERT_EQ(actual[count - 1].kind, CLUSTER_RECOVERY_NATIVE_SPACE_DROP);
		}
		if (mode == 1) UT_ASSERT_EQ(actual[count - 1].kind, CLUSTER_RECOVERY_NATIVE_PREPARE_END);
		if (mode == 2) {
			const ClusterUndoDecoded *undo = actual[0].bytes;
			UT_ASSERT(undo == &scan->undo); UT_ASSERT_EQ(undo->kind, CLUSTER_UNDO_KIND_TT_SET_HEAD);
		}
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &next), CLUSTER_RECOVERY_NATIVE_END);
		cluster_recovery_native_scan_destroy(scan);
	}
}

UT_TEST(test_compound_snapshot_capacity_and_changed_source_never_export_prefix)
{
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect actual[301], sentinel[301];
	size_t count = 0;
	native_mx_create(300);
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan == NULL) return;
	memset(actual, 0xa5, sizeof(actual)); memcpy(sentinel, actual, sizeof(actual));
	UT_ASSERT_EQ(cluster_recovery_native_scan_effects_v1(scan, actual, 300, &count),
		CLUSTER_RECOVERY_NATIVE_CAPACITY);
	UT_ASSERT_EQ(count, 301); UT_ASSERT(memcmp(actual, sentinel, sizeof(actual)) == 0);
	UT_ASSERT_EQ(cluster_recovery_native_scan_effects_v1(scan, actual, 301, &count),
		CLUSTER_RECOVERY_NATIVE_EFFECT);
	UT_ASSERT_EQ(actual[300].kind, CLUSTER_RECOVERY_NATIVE_MULTIXACT_MEMBER);
	UT_ASSERT_EQ(actual[300].ordinal, 299);
	source.claim.claim_sha256[7]++;
	UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
		CLUSTER_RECOVERY_NATIVE_CHANGED);
	memcpy(actual, sentinel, sizeof(actual));
	UT_ASSERT_EQ(cluster_recovery_native_scan_effects_v1(scan, actual, 301, &count),
		CLUSTER_RECOVERY_NATIVE_CHANGED);
	UT_ASSERT_EQ(count, 0); UT_ASSERT(memcmp(actual, sentinel, sizeof(actual)) == 0);
	cluster_recovery_native_scan_destroy(scan);
}

UT_TEST(test_compound_snapshot_zero_message_flags_remain_a_complete_obligation)
{
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effect;
	xl_invalidations record = {0};
	size_t count = 0;
	fixture(RM_XACT_ID, XLOG_XACT_INVALIDATIONS);
	record.dbId = 5; record.tsId = 1663; record.relcacheInitFileInval = true;
	append(&record, MinSizeOfInvalidations);
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan == NULL) return;
	UT_ASSERT_EQ(cluster_recovery_native_scan_effects_v1(scan, &effect, 1, &count),
		CLUSTER_RECOVERY_NATIVE_EFFECT);
	UT_ASSERT_EQ(count, 1); UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_INVALIDATION_BATCH);
	UT_ASSERT(effect.record->relcache_init_file);
	UT_ASSERT_EQ(effect.length, MinSizeOfInvalidations);
	cluster_recovery_native_scan_destroy(scan);
}
