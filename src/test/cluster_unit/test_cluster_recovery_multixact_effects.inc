/* Owned native MultiXact WAL observations; no online member lookup.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */

static void
native_mx_create(int count)
{
	xl_multixact_create create = { 0 };

	fixture(RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID);
	create.mid = 42;
	create.moff = UINT32_MAX - 1;
	create.nmembers = count;
	append(&create, SizeOfMultiXactCreate);
	for (int i = 0; i < count; i++) {
		MultiXactMember member
			= { 4195136 + i, (i == count - 1) ? MultiXactStatusUpdate : (MultiXactStatus)(i % 4) };

		append(&member, sizeof(member));
	}
}

UT_TEST(test_multixact_owns_every_member_and_full_source)
{
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effect;
	ClusterWalSourceRef expected_source;
	char expected[4096];
	uint32 length;
	unsigned members = 0;

	native_mx_create(300);
	expected_source = source;
	length = decoded.main_data_len;
	memcpy(expected, payload.bytes, length);
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan == NULL)
		return;
	memset(&reader, 0, sizeof(reader));
	memset(&source, 0, sizeof(source));
	memset(&payload, 0, sizeof(payload));
	UT_ASSERT_EQ(cluster_recovery_native_scan_primary(scan, &effect),
				 CLUSTER_RECOVERY_NATIVE_EFFECT);
	UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_MULTIXACT_CREATE);
	UT_ASSERT_EQ(effect.length, length);
	UT_ASSERT(memcmp(effect.bytes, expected, length) == 0);
	UT_ASSERT(memcmp(&effect.record->source, &expected_source, sizeof(expected_source)) == 0);
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
	UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_MULTIXACT_CREATE);
	while (cluster_recovery_native_scan_next(scan, &effect) == CLUSTER_RECOVERY_NATIVE_EFFECT) {
		MultiXactMember member;

		UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_MULTIXACT_MEMBER);
		UT_ASSERT_EQ(effect.ordinal, members);
		UT_ASSERT_EQ(effect.length, sizeof(member));
		memcpy(&member, effect.bytes, sizeof(member));
		UT_ASSERT_EQ(member.xid, 4195136 + members);
		UT_ASSERT_EQ(member.status, members == 299 ? MultiXactStatusUpdate : members % 4);
		UT_ASSERT(memcmp(effect.record->payload, expected, length) == 0);
		members++;
	}
	UT_ASSERT_EQ(members, 300);
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_END);
	UT_ASSERT(effect.record == NULL && effect.bytes == NULL);
	cluster_recovery_native_scan_destroy(scan);
}

UT_TEST(test_multixact_zero_and_truncate_preserve_distinct_raw_obligations)
{
	uint8 opcodes[] = { XLOG_MULTIXACT_ZERO_OFF_PAGE, XLOG_MULTIXACT_ZERO_MEM_PAGE,
						XLOG_MULTIXACT_TRUNCATE_ID };
	ClusterRecoveryNativeKind kinds[] = { CLUSTER_RECOVERY_NATIVE_MULTIXACT_ZERO_OFFSETS,
										  CLUSTER_RECOVERY_NATIVE_MULTIXACT_ZERO_MEMBERS,
										  CLUSTER_RECOVERY_NATIVE_MULTIXACT_TRUNCATE };

	for (unsigned i = 0; i < lengthof(opcodes); i++) {
		ClusterRecoveryNativeScan *scan = NULL;
		ClusterRecoveryNativeEffect effect;
		int page = 0;
		xl_multixact_truncate trunc = { 5, UINT32_MAX - 7, 6, UINT32_MAX - 3, 0 };

		fixture(RM_MULTIXACT_ID, opcodes[i]);
		if (i < 2)
			append(&page, sizeof(page));
		else
			append(&trunc, sizeof(trunc));
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL)
			continue;
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		UT_ASSERT_EQ(effect.kind, kinds[i]);
		UT_ASSERT_EQ(effect.length, decoded.main_data_len);
		UT_ASSERT(memcmp(effect.bytes, payload.bytes, effect.length) == 0);
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_END);
		cluster_recovery_native_scan_destroy(scan);
	}
}

UT_TEST(test_multixact_bad_counts_truncation_and_member_bytes_refuse)
{
	uint32 length;

	native_mx_create(3);
	length = decoded.main_data_len;
	for (uint32 n = 0; n <= length; n++) {
		ClusterRecoveryNativeScan *scan = NULL;

		native_mx_create(3);
		decoded.main_data_len = n == length ? length + 1 : n;
		UT_ASSERT(begin(&scan) != CLUSTER_RECOVERY_NATIVE_MORE);
		UT_ASSERT(scan == NULL);
	}
	for (unsigned mode = 0; mode < 10; mode++) {
		ClusterRecoveryNativeScan *scan = NULL;
		xl_multixact_create *create;

		native_mx_create(3);
		create = (xl_multixact_create *)payload.bytes;
		switch (mode) {
		case 0:
			create->nmembers = 0;
			break;
		case 1:
			create->nmembers = -1;
			break;
		case 2:
			create->nmembers = INT_MAX;
			break;
		case 3:
			create->mid = 0;
			break;
		case 4:
			create->moff = 0;
			break;
		case 5:
			create->members[2].xid = InvalidTransactionId;
			break;
		case 6:
			create->members[2].xid = FrozenTransactionId;
			break;
		case 7:
			create->members[2].status = (MultiXactStatus)-1;
			break;
		case 8:
			create->members[2].status = (MultiXactStatus)(MaxMultiXactStatus + 1);
			break;
		case 9:
			decoded.header.xl_info = 0x40;
			break;
		}
		UT_ASSERT(begin(&scan) != CLUSTER_RECOVERY_NATIVE_MORE);
		UT_ASSERT(scan == NULL);
	}
}

UT_TEST(test_multixact_zero_page_bounds_and_truncate_shape_refuse)
{
	for (unsigned family = 0; family < 3; family++) {
		for (unsigned mode = 0; mode < 5; mode++) {
			ClusterRecoveryNativeScan *scan = NULL;
			uint8 opcodes[] = { XLOG_MULTIXACT_ZERO_OFF_PAGE, XLOG_MULTIXACT_ZERO_MEM_PAGE,
								XLOG_MULTIXACT_TRUNCATE_ID };
			int page = mode == 0 ? -1 : INT_MAX;
			xl_multixact_truncate trunc = { 5, 1, 6, 7, 0 };

			fixture(RM_MULTIXACT_ID, opcodes[family]);
			if (family < 2)
				append(&page, sizeof(page));
			else {
				if (mode == 0)
					trunc.oldestMultiDB = InvalidOid;
				if (mode == 1)
					trunc.startTruncOff = InvalidMultiXactId;
				if (mode == 2)
					trunc.endTruncOff = InvalidMultiXactId;
				append(&trunc, sizeof(trunc));
			}
			if (mode == 3)
				decoded.main_data_len--;
			if (mode == 4)
				decoded.main_data_len++;
			UT_ASSERT(begin(&scan) != CLUSTER_RECOVERY_NATIVE_MORE);
			UT_ASSERT(scan == NULL);
		}
	}
}

UT_TEST(test_multixact_cut_budget_and_final_recheck_keep_original_gates)
{
	for (unsigned mode = 0; mode < 6; mode++) {
		ClusterRecoveryNativeScan *scan = NULL;
		ClusterRecoveryNativeEffect effect;
		size_t budget;

		native_mx_create(3);
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL)
			continue;
		budget = cluster_recovery_native_scan_bytes(scan);
		cluster_recovery_native_scan_destroy(scan);
		scan = NULL;
		if (mode == 0) {
			UT_ASSERT_EQ(
				cluster_recovery_native_scan_begin(&reader, &source, &cut, budget - 1, &scan),
				CLUSTER_RECOVERY_NATIVE_CAPACITY);
			UT_ASSERT(scan == NULL);
			continue;
		}
		if (mode == 1)
			cut.failed_thread++;
		if (mode == 2)
			decoded.max_block_id = 0;
		if (mode == 3)
			decoded.has_page_version_edge = true;
		if (mode < 4) {
			UT_ASSERT(begin(&scan) != CLUSTER_RECOVERY_NATIVE_MORE);
			UT_ASSERT(scan == NULL);
			continue;
		}
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL)
			continue;
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		if (mode == 4)
			((xl_multixact_create *)payload.bytes)->members[2].xid++;
		else
			source.claim.identity.origin_owner_incarnation++;
		UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
					 CLUSTER_RECOVERY_NATIVE_CHANGED);
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_CHANGED);
		UT_ASSERT(effect.record == NULL && effect.bytes == NULL);
		native_mx_create(3);
		UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
					 CLUSTER_RECOVERY_NATIVE_CHANGED);
		cluster_recovery_native_scan_destroy(scan);
	}
}
