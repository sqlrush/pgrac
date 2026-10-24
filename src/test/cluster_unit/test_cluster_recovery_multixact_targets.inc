/* Original MultiXact WAL -> original-origin physical roots; no apply.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#include "access/multixact.h"

static void
mx_target_create(uint32 mid, uint32 offset, unsigned node)
{
	xl_multixact_create create = { 0 };
	MultiXactMember members[3] = { { 4195136, MultiXactStatusForShare },
								   { 4195138, MultiXactStatusForUpdate },
								   { 4195140, MultiXactStatusUpdate } };
	create.mid = mid;
	create.moff = offset;
	create.nmembers = 3;
	wal_fixture(RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID, SizeOfMultiXactCreate + sizeof(members),
				0);
	memcpy(wal_payload.bytes, &create, SizeOfMultiXactCreate);
	memcpy(wal_payload.bytes + SizeOfMultiXactCreate, members, sizeof(members));
	native_source.claim.identity.origin_node_id = node;
	native_source.claim.identity.origin_thread_id = node + 1;
	native_cut.failed_thread = node + 1;
}

static const ClusterRecoveryRootEntry *
mx_target_entry(ClusterRecoveryRootSet *roots, const char *path)
{
	for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); ++i) {
		const ClusterRecoveryRootEntry *e = cluster_recovery_root_set_entry(roots, i);
		if (e != NULL && strcmp(e->file.path, path) == 0)
			return e;
	}
	return NULL;
}

static void
mx_target_expect(ClusterRecoveryRootSet *roots, const char *path, uint64 blocks, unsigned node,
				 bool physical)
{
	const ClusterRecoveryRootEntry *e = mx_target_entry(roots, path);
	UT_ASSERT(e != NULL);
	if (e == NULL)
		return;
	UT_ASSERT_EQ(e->file.kind, CLUSTER_RECOVERY_FILE_ORIGIN_SLRU);
	UT_ASSERT_EQ(e->file.native_origin_node, node);
	UT_ASSERT_EQ(e->required_blocks, blocks);
	UT_ASSERT(e->origins & CLUSTER_RECOVERY_ROOT_FROM_WAL);
	UT_ASSERT_EQ((e->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) != 0, physical);
}

UT_TEST(test_origin_slru_names_do_not_adopt_local_state)
{
	static const char *const suffixes[]
		= { "pg_xact/0000", "pg_subtrans/0001", "pg_commit_ts/FFFF", "pg_multixact/offsets/FFFF",
			"pg_multixact/members/14078" };
	static const unsigned families[] = { 1, 2, 4, 5, 6 };
	static const char *const bad[] = { "native_side",
									   "native_side/origin_",
									   "native_side/origin_128/pg_xact/0000",
									   "native_side/origin_00/pg_xact/0000",
									   "native_side/origin_-1/pg_xact/0000",
									   "native_side/origin_0/../pg_xact/0000",
									   "native_side/origin_0/pg_multixact/members/1407a",
									   "native_side/origin_0/pg_multixact/members/1407A",
									   "native_side/origin_0/pg_multixact/offsets/10000" };
	for (unsigned n = 0; n < 2; n++)
		for (unsigned i = 0; i < lengthof(suffixes); ++i) {
			char path[MAXPGPATH];
			RelFileLocator locator;
			ForkNumber forknum;
			uint8 family = 0;
			uint32 segment = 0;
			snprintf(path, sizeof(path), "native_side/origin_%u/%s", n ? 127 : 0, suffixes[i]);
			UT_ASSERT_EQ(
				cluster_recovery_namespace_classify_ex(path, &locator, &forknum, &family, &segment),
				CLUSTER_RECOVERY_FILE_ORIGIN_SLRU);
			UT_ASSERT_EQ(family, families[i]);
			UT_ASSERT_EQ(locator.relNumber, 0);
			UT_ASSERT_EQ(forknum, InvalidForkNumber);
		}
	for (unsigned i = 0; i < lengthof(bad); ++i) {
		RelFileLocator locator;
		ForkNumber forknum;
		UT_ASSERT_EQ(cluster_recovery_namespace_classify(bad[i], &locator, &forknum),
					 CLUSTER_RECOVERY_FILE_UNCLASSIFIED);
	}
}

UT_TEST(test_mx_create_keeps_next_sentinel_and_every_member_segment)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
	ClusterRecoveryWalTargetsDiag diag;
	/* offsets[65535] and sentinel[65536] cross a segment; members 52351..3. */
	mx_target_create(65535, 52351, 127);
	UT_ASSERT(collect(roots, &diag));
	UT_ASSERT_EQ(diag.unowned_records, 1); /* files do not prove member/terminal truth */
	mx_target_create(7, 1, 127);
	UT_ASSERT(collect(roots, &diag)); /* smaller duplicates cannot lower either extent */
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	mx_target_expect(roots, "native_side/origin_127/pg_multixact/offsets/0000", 32, 127, false);
	mx_target_expect(roots, "native_side/origin_127/pg_multixact/offsets/0001", 1, 127, false);
	mx_target_expect(roots, "native_side/origin_127/pg_multixact/members/0000", 32, 127, false);
	mx_target_expect(roots, "native_side/origin_127/pg_multixact/members/0001", 1, 127, false);
	UT_ASSERT_EQ(cluster_recovery_root_set_count(roots), 4);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_mx_wrap_preserves_both_ends_without_native_local_substitution)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
	ClusterRecoveryWalTargetsDiag diag;
	mx_target_create(UINT32_MAX, UINT32_MAX, 0);
	UT_ASSERT(collect(roots, &diag));
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	mx_target_expect(roots, "native_side/origin_0/pg_multixact/offsets/FFFF", 32, 0, false);
	mx_target_expect(roots, "native_side/origin_0/pg_multixact/offsets/0000", 1, 0, false);
	mx_target_expect(roots, "native_side/origin_0/pg_multixact/members/14078", 6, 0, false);
	mx_target_expect(roots, "native_side/origin_0/pg_multixact/members/0000", 1, 0, false);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

static void
mx_target_files(int fd)
{
	const char *dirs[] = { "native_side",
						   "native_side/origin_1",
						   "native_side/origin_1/pg_multixact",
						   "native_side/origin_1/pg_multixact/offsets",
						   "native_side/origin_1/pg_multixact/members",
						   "pg_multixact",
						   "pg_multixact/offsets" };
	for (unsigned i = 0; i < lengthof(dirs); ++i)
		UT_ASSERT_EQ(mkdirat(fd, dirs[i], 0700), 0);
	root_file(fd, "native_side/origin_1/pg_multixact/offsets/0000", wal_image, BLCKSZ);
	root_file(fd, "pg_multixact/offsets/0000", wal_image, BLCKSZ);
	root_file(fd, "native_side/origin_1/pg_multixact/members/0010", wal_image, BLCKSZ);
}

UT_TEST(test_mx_root_union_keeps_short_missing_untouched_and_local_files)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryWalTargetsDiag diag;
	uint32 counts[6], total;
	const ClusterRecoveryRootEntry *local, *untouched;
	mx_target_files(fd);
	roots = begin_roots(fd, 1024 * 1024);
	mx_target_create(65535, 52351, 1);
	UT_ASSERT(collect(roots, &diag));
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	mx_target_expect(roots, "native_side/origin_1/pg_multixact/offsets/0000", 32, 1, true);
	mx_target_expect(roots, "native_side/origin_1/pg_multixact/offsets/0001", 1, 1, false);
	local = mx_target_entry(roots, "pg_multixact/offsets/0000");
	untouched = mx_target_entry(roots, "native_side/origin_1/pg_multixact/members/0010");
	UT_ASSERT(local && untouched);
	if (local && untouched) {
		UT_ASSERT_EQ(local->origins, CLUSTER_RECOVERY_ROOT_PHYSICAL);
		UT_ASSERT_EQ(untouched->origins, CLUSTER_RECOVERY_ROOT_PHYSICAL);
		UT_ASSERT_EQ(untouched->file.kind, CLUSTER_RECOVERY_FILE_ORIGIN_SLRU);
		UT_ASSERT_EQ(untouched->file.native_segment, 16);
		UT_ASSERT_EQ(untouched->file.native_origin_node, 1);
	}
	UT_ASSERT(cluster_recovery_root_set_native_counts(roots, counts, &total));
	UT_ASSERT_EQ(total, 1); /* local census must not count origin-side files */
	root_file(fd, "native_side/origin_1/pg_multixact/offsets/0000", "changed", 7);
	UT_ASSERT_EQ(cluster_recovery_root_set_recheck_begin(roots), CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT(finish_roots(roots, NULL) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_mx_zero_page_and_truncate_preserve_distinct_obligations)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
	ClusterRecoveryWalTargetsDiag diag;
	int page = 33;
	xl_multixact_truncate trunc = { 5, 100, 4, UINT32_MAX, 0 };
	wal_fixture(RM_MULTIXACT_ID, XLOG_MULTIXACT_ZERO_OFF_PAGE, sizeof(page), 0);
	memcpy(wal_payload.bytes, &page, sizeof(page));
	UT_ASSERT(collect(roots, &diag));
	wal_fixture(RM_MULTIXACT_ID, XLOG_MULTIXACT_ZERO_MEM_PAGE, sizeof(page), 0);
	memcpy(wal_payload.bytes, &page, sizeof(page));
	UT_ASSERT(collect(roots, &diag));
	wal_fixture(RM_MULTIXACT_ID, XLOG_MULTIXACT_TRUNCATE_ID, sizeof(trunc), 0);
	memcpy(wal_payload.bytes, &trunc, sizeof(trunc));
	UT_ASSERT(collect(roots, &diag));
	UT_ASSERT_EQ(diag.unowned_records, 1);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	mx_target_expect(roots, "native_side/origin_1/pg_multixact/offsets/0001", 2, 1, false);
	mx_target_expect(roots, "native_side/origin_1/pg_multixact/members/0001", 2, 1, false);
	UT_ASSERT_EQ(cluster_recovery_root_set_count(roots), 2); /* no fake truncate completion */
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_mx_target_failures_revoke_the_whole_union)
{
	int fd = new_root();
	for (unsigned mode = 0; mode < 6; ++mode) {
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryWalTargetsDiag diag;
		mx_target_create(7, 1, 1);
		UT_ASSERT(collect(roots, &diag));
		if (mode == 0)
			native_source.claim.identity.origin_node_id = 128;
		if (mode == 1)
			native_source.claim.database_incarnation++;
		if (mode == 2)
			native_cut.scan_end_exclusive--;
		if (mode == 3)
			wal_record_storage.decoded.main_data_len--;
		if (mode == 4)
			wal_record_storage.decoded.header.xl_info = 0xf0;
		if (mode == 5)
			UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT(!collect(roots, &diag));
		UT_ASSERT(finish_roots(roots, NULL) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
		cluster_recovery_root_set_destroy(roots);
	}
	remove_root(fd);
}

UT_TEST(test_mx_target_capacity_never_exports_a_partial_union)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots = begin_roots(fd, 320 * 1024);
	ClusterRecoveryWalTargetsDiag diag;
	uint8 digest[32];
	bool collected = true;
	unsigned added = 0;

	for (; added < 1000 && collected; ++added) {
		int page = added * SLRU_PAGES_PER_SEGMENT;
		wal_fixture(RM_MULTIXACT_ID, XLOG_MULTIXACT_ZERO_OFF_PAGE, sizeof(page), 0);
		memcpy(wal_payload.bytes, &page, sizeof(page));
		collected = collect(roots, &diag);
	}
	UT_ASSERT(added > 1 && added < 1000);
	UT_ASSERT(!collected);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_CAPACITY);
	UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
	UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}
