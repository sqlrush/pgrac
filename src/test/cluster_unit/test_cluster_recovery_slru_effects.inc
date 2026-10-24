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
