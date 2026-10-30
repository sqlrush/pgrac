/* Owned native CLOG/commit_ts WAL observations; no status or truncate authority.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#include "access/clog.h"
#include "access/commit_ts.h"

static void
native_slru_fixture(unsigned shape, int page)
{
	uint32 per = shape < 2 ? CLUSTER_NATIVE_CLOG_PER_PAGE : CLUSTER_NATIVE_COMMIT_TS_PER_PAGE;
	TransactionId oldest = (uint64)page * per + (page == 0 ? FirstNormalTransactionId : 0);
	xl_clog_truncate clog = { page, oldest, InvalidOid };
	xl_commit_ts_truncate cts = { page, oldest };

	fixture(shape < 2 ? RM_CLOG_ID : RM_COMMIT_TS_ID, shape % 2 ? 0x10 : 0);
	if (shape % 2 == 0)
		append(&page, sizeof(page));
	else if (shape == 1)
		append(&clog, sizeof(clog));
	else
		append(&cts, SizeOfCommitTsTruncate);
}

UT_TEST(test_native_slru_owns_all_four_shapes_and_raw_cutoff)
{
	const ClusterRecoveryNativeKind kinds[]
		= { CLUSTER_RECOVERY_NATIVE_CLOG_ZERO, CLUSTER_RECOVERY_NATIVE_CLOG_TRUNCATE,
			CLUSTER_RECOVERY_NATIVE_COMMIT_TS_ZERO, CLUSTER_RECOVERY_NATIVE_COMMIT_TS_TRUNCATE };
	for (unsigned shape = 0; shape < 4; shape++)
		for (unsigned edge = 0; edge < 2; edge++) {
			ClusterRecoveryNativeScan *scan = NULL;
			ClusterRecoveryNativeEffect effect;
			ClusterWalSourceRef expected;
			char saved[sizeof(xl_clog_truncate)];
			uint32 length,
				per = shape < 2 ? CLUSTER_NATIVE_CLOG_PER_PAGE : CLUSTER_NATIVE_COMMIT_TS_PER_PAGE;
			int page = edge ? UINT32_MAX / per : 0;
			native_slru_fixture(shape, page);
			expected = source;
			length = decoded.main_data_len;
			memcpy(saved, payload.bytes, length);
			UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
			if (scan == NULL)
				continue;
			memset(&source, 0, sizeof(source));
			memset(&reader, 0, sizeof(reader));
			memset(&payload, 0, sizeof(payload));
			UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
						 CLUSTER_RECOVERY_NATIVE_EFFECT);
			UT_ASSERT_EQ(effect.kind, kinds[shape]);
			UT_ASSERT_EQ(effect.length, length);
			UT_ASSERT(memcmp(effect.bytes, saved, length) == 0);
			UT_ASSERT(memcmp(&effect.record->source, &expected, sizeof(expected)) == 0);
			UT_ASSERT_EQ(effect.record->terminal_scn, 0);
			UT_ASSERT_EQ(effect.record->top_xid, 0);
			UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
						 CLUSTER_RECOVERY_NATIVE_END);
			UT_ASSERT(effect.record == NULL && effect.bytes == NULL);
			cluster_recovery_native_scan_destroy(scan);
		}
}

UT_TEST(test_native_slru_bad_page_cutoff_shape_and_unknown_refuse)
{
	for (unsigned shape = 0; shape < 4; shape++)
		for (unsigned fault = 0; fault < 8; fault++) {
			ClusterRecoveryNativeScan *scan = NULL;
			int page;
			native_slru_fixture(shape, 1);
			switch (fault) {
			case 0:
				page = -1;
				memcpy(payload.bytes, &page, sizeof(page));
				break;
			case 1:
				page = INT_MAX;
				memcpy(payload.bytes, &page, sizeof(page));
				break;
			case 2:
				decoded.main_data_len--;
				break;
			case 3:
				decoded.main_data_len++;
				break;
			case 4:
				decoded.header.xl_info = 0x20;
				break;
			case 5:
				decoded.header.xl_info = 0xF0;
				break;
			case 6:
			case 7:
				if (shape % 2) {
					TransactionId oldest
						= fault == 6 ? FrozenTransactionId : FirstNormalTransactionId;
					memcpy(payload.bytes + sizeof(page), &oldest, sizeof(oldest));
				} else
					decoded.main_data_len = fault == 6 ? 0 : sizeof(xl_clog_truncate);
				break;
			}
			UT_ASSERT(begin(&scan) != CLUSTER_RECOVERY_NATIVE_MORE);
			UT_ASSERT(scan == NULL);
		}
}

UT_TEST(test_native_slru_original_cut_shape_and_allocation_gates)
{
	for (unsigned shape = 0; shape < 4; shape++)
		for (unsigned fault = 0; fault < 6; fault++) {
			ClusterRecoveryNativeScan *scan = NULL;
			size_t bytes;
			native_slru_fixture(shape, 1);
			UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
			if (scan == NULL)
				continue;
			bytes = cluster_recovery_native_scan_bytes(scan);
			cluster_recovery_native_scan_destroy(scan);
			scan = NULL;
			if (fault == 0) {
				UT_ASSERT_EQ(
					cluster_recovery_native_scan_begin(&reader, &source, &cut, bytes - 1, &scan),
					CLUSTER_RECOVERY_NATIVE_CAPACITY);
			} else {
				if (fault == 1)
					cut.failed_thread++;
				if (fault == 2)
					cut.scan_end_exclusive = reader.EndRecPtr - 1;
				if (fault == 3)
					decoded.max_block_id = 0;
				if (fault == 4)
					decoded.has_page_version_edge = true;
				if (fault == 5)
					source.claim.identity.system_identifier = 0;
				UT_ASSERT(begin(&scan) != CLUSTER_RECOVERY_NATIVE_MORE);
			}
			UT_ASSERT(scan == NULL);
		}
}

UT_TEST(test_native_slru_full_recheck_after_drain_is_sticky)
{
	for (unsigned shape = 0; shape < 4; shape++) {
		ClusterRecoveryNativeScan *scan = NULL;
		ClusterRecoveryNativeEffect effect;
		uint8 saved;
		native_slru_fixture(shape, 1);
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL)
			continue;
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_END);
		UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
					 CLUSTER_RECOVERY_NATIVE_UNCHANGED);
		/* Include the raw database/cutoff suffix, not just the page prefix. */
		saved = payload.bytes[decoded.main_data_len - 1];
		payload.bytes[decoded.main_data_len - 1] ^= 1;
		UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
					 CLUSTER_RECOVERY_NATIVE_CHANGED);
		payload.bytes[decoded.main_data_len - 1] = saved;
		UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
					 CLUSTER_RECOVERY_NATIVE_CHANGED);
		memset(&effect, 0xA5, sizeof(effect));
		UT_ASSERT_EQ(cluster_recovery_native_scan_primary(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_CHANGED);
		UT_ASSERT(effect.record == NULL && effect.bytes == NULL);
		cluster_recovery_native_scan_destroy(scan);
	}
}

/* The full first-normal value identifies a physical page generation only.
 * Keep the complete source and bytes; do not relabel legacy raw ZERO. */
UT_TEST(test_native_clog_full_birth_retains_exact_generation)
{
	const uint64 first[] = {3, (UINT64_C(1) << 32) | 3,
		(UINT64_C(7) << 32) | (127 * CLUSTER_NATIVE_CLOG_PER_PAGE),
		(UINT64_C(0xFFFFFFFF) << 32) | (UINT32_MAX & ~(CLUSTER_NATIVE_CLOG_PER_PAGE - 1))};
	for (unsigned i = 0; i < lengthof(first); ++i) {
		ClusterRecoveryNativeScan *scan = NULL;
		ClusterRecoveryNativeEffect effect;
		xl_clog_zeropage_full image = {(TransactionId)first[i] / CLUSTER_NATIVE_CLOG_PER_PAGE,
			0, first[i]}, copy;
		fixture(RM_CLOG_ID, CLOG_ZEROPAGE_FULL);
		append(&image, sizeof(image));
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL) continue;
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
		UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_CLOG_ZERO);
		UT_ASSERT_EQ(effect.length, sizeof(image));
		memcpy(&copy, effect.bytes, sizeof(copy));
		UT_ASSERT_EQ(memcmp(&image, &copy, sizeof(image)), 0);
		UT_ASSERT_EQ(memcmp(&effect.record->source, &source, sizeof(source)), 0);
		UT_ASSERT_EQ(effect.record->header.xl_info, CLOG_ZEROPAGE_FULL);
		UT_ASSERT_EQ(effect.record->top_xid, 0);
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_END);
		cluster_recovery_native_scan_destroy(scan);
	}
}

UT_TEST(test_native_clog_full_birth_rejects_bad_generation_shapes)
{
	for (unsigned fault = 0; fault < 10; ++fault) {
		ClusterRecoveryNativeScan *scan = NULL;
		xl_clog_zeropage_full image = {1, 0, (UINT64_C(7) << 32) | CLUSTER_NATIVE_CLOG_PER_PAGE};
		fixture(RM_CLOG_ID, CLOG_ZEROPAGE_FULL);
		if (fault == 0) image.reserved_zero = 1;
		if (fault == 1) image.pageno = -1;
		if (fault == 2) image.pageno = 2;
		if (fault == 3) image.first_full_xid++;
		if (fault == 4) image.first_full_xid = (UINT64_C(7) << 32) | FrozenTransactionId;
		if (fault == 5) {image.pageno = 0; image.first_full_xid = UINT64_C(7) << 32;}
		append(&image, sizeof(image));
		if (fault == 6) decoded.main_data_len--;
		if (fault == 7) decoded.main_data_len++;
		if (fault == 8) decoded.header.xl_info = CLOG_ZEROPAGE;
		if (fault == 9) decoded.header.xl_rmid = RM_COMMIT_TS_ID;
		UT_ASSERT(begin(&scan) != CLUSTER_RECOVERY_NATIVE_MORE);
		UT_ASSERT(scan == NULL);
	}
}

UT_TEST(test_native_clog_full_birth_recheck_cannot_rebase)
{
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effect;
	xl_clog_zeropage_full image = {1, 0, (UINT64_C(7) << 32) | CLUSTER_NATIVE_CLOG_PER_PAGE};
	fixture(RM_CLOG_ID, CLOG_ZEROPAGE_FULL);
	append(&image, sizeof(image));
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan == NULL) return;
	image.first_full_xid += UINT64_C(1) << 32;
	memcpy(payload.bytes, &image, sizeof(image));
	UT_ASSERT_NE(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
		CLUSTER_RECOVERY_NATIVE_UNCHANGED);
	UT_ASSERT_NE(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
	cluster_recovery_native_scan_destroy(scan);
}


UT_TEST(test_native_clog_full_truncate_keeps_exact_cutoff_image)
{
	const uint32 epochs[] = {0, 7, UINT32_MAX};
	for (unsigned i = 0; i < lengthof(epochs); i++) {
		xl_clog_truncate_full cut = {33, 33 * CLUSTER_NATIVE_CLOG_PER_PAGE + 17, 606, 0, 0};
		ClusterRecoveryNativeEffect effect;
		ClusterRecoveryNativeScan *scan = NULL;
		cut.oldest_full_xid = ((uint64)epochs[i] << 32) | cut.oldestXact;
		fixture(RM_CLOG_ID, CLOG_TRUNCATE_FULL);
		append(&cut, sizeof(cut));
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL) continue;
		UT_ASSERT_EQ(cluster_recovery_native_scan_primary(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
		UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_CLOG_TRUNCATE);
		UT_ASSERT_EQ(effect.length, sizeof(cut));
		UT_ASSERT(memcmp(effect.bytes, &cut, sizeof(cut)) == 0);
		cluster_recovery_native_scan_destroy(scan);
	}
}

UT_TEST(test_native_clog_full_truncate_refuses_raw_alias_and_incomplete_image)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		xl_clog_truncate_full cut = {33, 33 * CLUSTER_NATIVE_CLOG_PER_PAGE + 17, 606, 0, 0};
		ClusterRecoveryNativeScan *scan = NULL;
		cut.oldest_full_xid = (UINT64_C(7) << 32) | cut.oldestXact;
		if (fault == 0) cut.reserved_zero = 1;
		if (fault == 1) cut.pageno = -1;
		if (fault == 2) cut.pageno++;
		if (fault == 3) cut.oldest_full_xid++;
		if (fault == 4) cut.oldestXact = 2;
		fixture(RM_CLOG_ID, CLOG_TRUNCATE_FULL);
		append(&cut, sizeof(cut));
		if (fault == 5) decoded.main_data_len--;
		if (fault == 6) decoded.main_data_len++;
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_SHAPE);
		UT_ASSERT(scan == NULL);
	}
}
