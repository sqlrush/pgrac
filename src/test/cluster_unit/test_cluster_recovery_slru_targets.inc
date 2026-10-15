/* Native SLRU WAL -> original-origin candidate roots, never truncate completion.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#include "access/clog.h"
#include "access/commit_ts.h"

static void
slru_target_zero(bool cts, int page, unsigned node)
{
	wal_fixture(cts ? RM_COMMIT_TS_ID : RM_CLOG_ID, 0, sizeof(page), 0);
	memcpy(wal_payload.bytes, &page, sizeof(page));
	native_source.claim.identity.origin_node_id = node;
	native_source.claim.identity.origin_thread_id = node + 1;
	native_cut.failed_thread = node + 1;
}

UT_TEST(test_slru_zero_roots_keep_local_origin_missing_short_and_maximum)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryWalTargetsDiag diag;
	const ClusterRecoveryRootEntry *entry;
	const char *dirs[] = { "pg_xact",
						   "native_side",
						   "native_side/origin_0",
						   "native_side/origin_0/pg_xact",
						   "native_side/origin_127",
						   "native_side/origin_127/pg_commit_ts" };
	for (unsigned i = 0; i < lengthof(dirs); ++i)
		UT_ASSERT_EQ(mkdirat(fd, dirs[i], 0700), 0);
	root_file(fd, "pg_xact/0000", wal_image, BLCKSZ);
	root_file(fd, "native_side/origin_0/pg_xact/0000", wal_image, BLCKSZ);
	root_file(fd, "native_side/origin_127/pg_commit_ts/000A", wal_image, BLCKSZ);
	roots = begin_roots(fd, 1024 * 1024);
	for (unsigned family = 0; family < 2; ++family) {
		uint32 per = family ? CLUSTER_NATIVE_COMMIT_TS_PER_PAGE : CLUSTER_NATIVE_CLOG_PER_PAGE;
		unsigned node = family ? 127 : 0;
		slru_target_zero(family, 31, node);
		UT_ASSERT(collect(roots, &diag));
		UT_ASSERT_EQ(diag.unowned_records, 1);
		slru_target_zero(family, 0, node);
		UT_ASSERT(collect(roots, &diag)); /* lower requirement cannot shrink 32 */
		slru_target_zero(family, 32, node);
		UT_ASSERT(collect(roots, &diag));
		slru_target_zero(family, UINT32_MAX / per, node);
		UT_ASSERT(collect(roots, &diag));
	}
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	for (unsigned family = 0; family < 2; ++family) {
		uint32 per = family ? CLUSTER_NATIVE_COMMIT_TS_PER_PAGE : CLUSTER_NATIVE_CLOG_PER_PAGE;
		uint32 page = UINT32_MAX / per;
		unsigned node = family ? 127 : 0;
		const char *name = family ? "pg_commit_ts" : "pg_xact";
		char path[MAXPGPATH];
		snprintf(path, sizeof(path), "native_side/origin_%u/%s/0000", node, name);
		mx_target_expect(roots, path, 32, node, !family);
		entry = mx_target_entry(roots, path);
		UT_ASSERT(entry != NULL);
		if (entry != NULL)
			UT_ASSERT_EQ(entry->file.native_family, family
														? CLUSTER_RECOVERY_NATIVE_FAMILY_COMMIT_TS
														: CLUSTER_RECOVERY_NATIVE_FAMILY_XACT);
		snprintf(path, sizeof(path), "native_side/origin_%u/%s/0001", node, name);
		mx_target_expect(roots, path, 1, node, false);
		snprintf(path, sizeof(path), "native_side/origin_%u/%s/%04X", node, name,
				 page / SLRU_PAGES_PER_SEGMENT);
		mx_target_expect(roots, path, page % SLRU_PAGES_PER_SEGMENT + 1, node, false);
	}
	entry = mx_target_entry(roots, "pg_xact/0000");
	UT_ASSERT(entry && entry->origins == CLUSTER_RECOVERY_ROOT_PHYSICAL);
	entry = mx_target_entry(roots, "native_side/origin_127/pg_commit_ts/000A");
	UT_ASSERT(entry && entry->origins == CLUSTER_RECOVERY_ROOT_PHYSICAL);
	root_file(fd, "native_side/origin_0/pg_xact/0000", "changed", 7);
	UT_ASSERT_EQ(cluster_recovery_root_set_recheck_begin(roots), CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT(finish_roots(roots, NULL) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_slru_truncate_keeps_obligation_without_deleting_or_faking_roots)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
	ClusterRecoveryWalTargetsDiag diag;
	xl_clog_truncate clog = { 128, 128 * CLUSTER_NATIVE_CLOG_PER_PAGE, 5 };
	xl_commit_ts_truncate cts = { 128, 128 * CLUSTER_NATIVE_COMMIT_TS_PER_PAGE };
	slru_target_zero(false, 0, 1);
	UT_ASSERT(collect(roots, &diag));
	slru_target_zero(true, 0, 1);
	UT_ASSERT(collect(roots, &diag));
	wal_fixture(RM_CLOG_ID, CLOG_TRUNCATE, sizeof(clog), 0);
	memcpy(wal_payload.bytes, &clog, sizeof(clog));
	UT_ASSERT(collect(roots, &diag));
	UT_ASSERT_EQ(diag.unowned_records, 1);
	UT_ASSERT_EQ(diag.other_native_effects, 1);
	wal_fixture(RM_COMMIT_TS_ID, COMMIT_TS_TRUNCATE, SizeOfCommitTsTruncate, 0);
	memcpy(wal_payload.bytes, &cts, SizeOfCommitTsTruncate);
	UT_ASSERT(collect(roots, &diag));
	UT_ASSERT_EQ(diag.unowned_records, 1);
	UT_ASSERT_EQ(diag.other_native_effects, 1);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT_EQ(cluster_recovery_root_set_count(roots), 2);
	mx_target_expect(roots, "native_side/origin_1/pg_xact/0000", 1, 1, false);
	mx_target_expect(roots, "native_side/origin_1/pg_commit_ts/0000", 1, 1, false);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_slru_zero_target_failures_poison_all_candidates)
{
	int fd = new_root();
	for (unsigned family = 0; family < 2; ++family)
		for (unsigned fault = 0; fault < 7; ++fault) {
			ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
			ClusterRecoveryWalTargetsDiag diag;
			slru_target_zero(family, 0, 1);
			UT_ASSERT(collect(roots, &diag));
			if (fault == 0)
				native_source.claim.identity.origin_node_id = 128;
			if (fault == 1)
				native_source.claim.database_incarnation++;
			if (fault == 2)
				native_cut.scan_end_exclusive--;
			if (fault == 3)
				wal_record_storage.decoded.main_data_len--;
			if (fault == 4)
				wal_record_storage.decoded.header.xl_info = 0xF0;
			if (fault == 5)
				UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
			if (fault == 6) {
				int page = -1;
				memcpy(wal_payload.bytes, &page, sizeof(page));
			}
			UT_ASSERT(!collect(roots, &diag));
			UT_ASSERT(finish_roots(roots, NULL) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
			UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
			cluster_recovery_root_set_destroy(roots);
		}
	remove_root(fd);
}

UT_TEST(test_slru_zero_target_capacity_does_not_export_a_prefix)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots = begin_roots(fd, 320 * 1024);
	ClusterRecoveryWalTargetsDiag diag;
	bool collected = true;
	unsigned i = 0;
	for (; i < 1000 && collected; ++i) {
		slru_target_zero(i % 2, (i / 2) * SLRU_PAGES_PER_SEGMENT, 1);
		collected = collect(roots, &diag);
	}
	UT_ASSERT(i > 1 && i < 1000);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_CAPACITY);
	UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}
