/*-------------------------------------------------------------------------
 * test_cluster_recovery_multixact_physical.inc
 *   Actual physical MX files through the original cold root reader.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_recovery_multixact_physical.inc
 * NOTES
 *   These are physical observations, not member/terminal proofs.
 *-------------------------------------------------------------------------
 */
#include "../../backend/cluster/cluster_recovery_multixact_scan.c"

static void
mx_physical_path(int fd, unsigned node, bool members, uint32 segment, char *path)
{
	char prefix[MAXPGPATH];
	if (node == UINT32_MAX)
		strlcpy(prefix, "pg_multixact", sizeof(prefix));
	else {
		if (mkdirat(fd, "native_side", 0700) != 0)
			UT_ASSERT_EQ(errno, EEXIST);
		snprintf(prefix, sizeof(prefix), "native_side/origin_%u", node);
		if (mkdirat(fd, prefix, 0700) != 0)
			UT_ASSERT_EQ(errno, EEXIST);
		strlcat(prefix, "/pg_multixact", sizeof(prefix));
	}
	if (mkdirat(fd, prefix, 0700) != 0)
		UT_ASSERT_EQ(errno, EEXIST);
	strlcat(prefix, members ? "/members" : "/offsets", sizeof(prefix));
	if (mkdirat(fd, prefix, 0700) != 0)
		UT_ASSERT_EQ(errno, EEXIST);
	snprintf(path, MAXPGPATH, "%s/%04X", prefix, segment);
}

static void
mx_physical_member(char *page, unsigned slot, uint32 xid, unsigned status)
{
	unsigned group = slot / 4, shift = (slot % 4) * 8;
	uint32 flags;
	memcpy(&flags, page + group * 20, sizeof(flags));
	flags = (flags & ~((uint32)255 << shift)) | ((uint32)status << shift);
	memcpy(page + group * 20, &flags, sizeof(flags));
	memcpy(page + group * 20 + 4 + (slot % 4) * 4, &xid, sizeof(xid));
}

static ClusterRecoveryMultixactScan *
mx_physical_begin(ClusterRecoveryRootSet *roots)
{
	ClusterRecoveryMultixactScan *scan = NULL;
	UT_ASSERT_EQ(cluster_recovery_multixact_scan_begin(roots, 64 * 1024, &scan),
				 CLUSTER_RECOVERY_MX_MORE);
	return scan;
}

static ClusterRecoveryMultixactResult
mx_physical_drain(ClusterRecoveryMultixactScan *scan)
{
	ClusterRecoveryMultixactEntry out;
	for (unsigned i = 0; i < 100000; ++i) {
		ClusterRecoveryMultixactResult result = cluster_recovery_multixact_scan_next(scan, &out);
		if (result != CLUSTER_RECOVERY_MX_MORE && result != CLUSTER_RECOVERY_MX_ENTRY)
			return result;
	}
	UT_ASSERT(false);
	return CLUSTER_RECOVERY_MX_INVALID;
}

UT_TEST(test_mx_physical_census_retains_all_slots_and_origin_boundaries)
{
	int fd = new_root();
	char path[MAXPGPATH];
	PGAlignedBlock page = { 0 };
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryMultixactScan *scan;
	ClusterRecoveryMultixactEntry out;
	ClusterRecoveryMultixactDiag diag;
	ClusterRecoveryMultixactResult result;
	unsigned local = 0, origin = 0, members = 0, files = 0, padding = 0;
	for (unsigned n = 0; n < 2; ++n) {
		for (unsigned i = 0; i < CLUSTER_NATIVE_MX_OFFSETS_PER_PAGE; ++i) {
			uint32 value = n ? i : UINT32_MAX - i;
			memcpy(page.data + i * sizeof(value), &value, sizeof(value));
		}
		mx_physical_path(fd, n ? 0 : UINT32_MAX, false, 0, path);
		root_file(fd, path, page.data, BLCKSZ);
	}
	memset(&page, 0, sizeof(page));
	for (unsigned i = 0; i < CLUSTER_NATIVE_MX_MEMBERS_PER_PAGE; ++i)
		mx_physical_member(page.data, i, i % 7 ? 4195136 + i : 0, i % 256);
	page.data[BLCKSZ - 1] = (char)0xa5;
	mx_physical_path(fd, 127, true, 0, path);
	root_file(fd, path, page.data, BLCKSZ);
	roots = begin_roots(fd, 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	scan = mx_physical_begin(roots);
	if (scan)
		for (unsigned calls = 0; calls < 10000; ++calls) {
			result = cluster_recovery_multixact_scan_next(scan, &out);
			if (result == CLUSTER_RECOVERY_MX_DRAINED)
				break;
			UT_ASSERT(result <= CLUSTER_RECOVERY_MX_ENTRY);
			if (result != CLUSTER_RECOVERY_MX_ENTRY)
				continue;
			if (out.kind == CLUSTER_RECOVERY_MX_FILE) {
				files++;
				continue;
			}
			UT_ASSERT_EQ(out.page_number, 0);
			UT_ASSERT_EQ(out.segment, 0);
			if (out.kind == CLUSTER_RECOVERY_MX_PADDING) {
				padding += out.length;
				UT_ASSERT_EQ(out.origin_node, 127);
				UT_ASSERT_EQ((unsigned char)out.bytes[out.length - 1], 0xa5);
				continue;
			}
			UT_ASSERT(out.addressable);
			UT_ASSERT_EQ(out.raw_index, out.slot);
			if (out.kind == CLUSTER_RECOVERY_MX_OFFSET) {
				if (out.origin_node == UINT32_MAX) {
					local++;
					UT_ASSERT_EQ(out.file_kind, CLUSTER_RECOVERY_FILE_NATIVE_SLRU);
					UT_ASSERT_EQ(out.value, UINT32_MAX - out.slot);
				} else {
					origin++;
					UT_ASSERT_EQ(out.origin_node, 0);
					UT_ASSERT_EQ(out.value, out.slot);
				}
			} else {
				members++;
				UT_ASSERT_EQ(out.kind, CLUSTER_RECOVERY_MX_MEMBER);
				UT_ASSERT_EQ(out.origin_node, 127);
				UT_ASSERT_EQ(out.value, out.slot % 7 ? 4195136 + out.slot : 0);
				UT_ASSERT_EQ(out.raw_status, out.slot % 256);
			}
		}
	UT_ASSERT_EQ(local, CLUSTER_NATIVE_MX_OFFSETS_PER_PAGE);
	UT_ASSERT_EQ(origin, local);
	UT_ASSERT_EQ(members, CLUSTER_NATIVE_MX_MEMBERS_PER_PAGE);
	UT_ASSERT_EQ(files, 3);
	UT_ASSERT_EQ(padding, BLCKSZ % 20);
	UT_ASSERT(cluster_recovery_multixact_scan_diag(scan, &diag));
	UT_ASSERT_EQ(diag.offset_slots, local + origin);
	UT_ASSERT_EQ(diag.member_slots, members);
	UT_ASSERT_EQ(diag.pages, 3);
	UT_ASSERT_EQ(diag.reread_pages, 3);
	cluster_recovery_multixact_scan_destroy(scan);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_mx_physical_uint32_tail_does_not_become_member_zero)
{
	int fd = new_root();
	char path[MAXPGPATH];
	uint32 per = CLUSTER_NATIVE_MX_MEMBERS_PER_PAGE;
	uint32 segment = (UINT32_MAX / per) / SLRU_PAGES_PER_SEGMENT;
	uint32 pages = (UINT32_MAX / per) % SLRU_PAGES_PER_SEGMENT + 1;
	char *data = calloc(pages, BLCKSZ);
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryMultixactScan *scan;
	ClusterRecoveryMultixactEntry out;
	unsigned unaddressable = 0, seen_max = 0;
	uint64 members = 0;
	mx_physical_path(fd, 1, true, segment, path);
	root_file(fd, path, data, pages * BLCKSZ);
	free(data);
	roots = begin_roots(fd, 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	scan = mx_physical_begin(roots);
	if (scan)
		for (unsigned calls = 0; calls < 100000; ++calls) {
			ClusterRecoveryMultixactResult result
				= cluster_recovery_multixact_scan_next(scan, &out);
			if (result == CLUSTER_RECOVERY_MX_DRAINED)
				break;
			UT_ASSERT(result <= CLUSTER_RECOVERY_MX_ENTRY);
			if (result != CLUSTER_RECOVERY_MX_ENTRY || out.kind != CLUSTER_RECOVERY_MX_MEMBER)
				continue;
			UT_ASSERT_EQ(out.raw_index, (uint64)segment * SLRU_PAGES_PER_SEGMENT * per + members);
			UT_ASSERT_EQ(out.addressable, out.raw_index <= UINT32_MAX);
			unaddressable += !out.addressable;
			seen_max += out.raw_index == UINT32_MAX;
			members++;
		}
	UT_ASSERT_EQ(members, (uint64)pages * per);
	UT_ASSERT_EQ(seen_max, 1);
	UT_ASSERT_EQ(unaddressable, per - (UINT32_MAX % per + 1));
	cluster_recovery_multixact_scan_destroy(scan);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_mx_physical_empty_files_remain_explicit_candidates)
{
	int fd = new_root();
	char path[MAXPGPATH];
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryMultixactScan *scan;
	ClusterRecoveryMultixactEntry out;
	ClusterRecoveryMultixactDiag diag;
	mx_physical_path(fd, 0, false, 0, path);
	root_file(fd, path, "", 0);
	roots = begin_roots(fd, 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	scan = mx_physical_begin(roots);
	if (scan) {
		UT_ASSERT_EQ(cluster_recovery_multixact_scan_next(scan, &out), CLUSTER_RECOVERY_MX_ENTRY);
		UT_ASSERT_EQ(out.kind, CLUSTER_RECOVERY_MX_FILE);
		UT_ASSERT_EQ(out.file_size, 0);
		UT_ASSERT_EQ(mx_physical_drain(scan), CLUSTER_RECOVERY_MX_DRAINED);
		UT_ASSERT(cluster_recovery_multixact_scan_diag(scan, &diag));
		UT_ASSERT_EQ(diag.files, 1);
		UT_ASSERT_EQ(diag.pages, 0);
	}
	cluster_recovery_multixact_scan_destroy(scan);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_mx_physical_missing_short_and_bad_geometry_do_not_drain)
{
	for (unsigned mode = 0; mode < 5; ++mode) {
		int fd = new_root();
		char path[MAXPGPATH];
		char *data = calloc(33, BLCKSZ);
		ClusterRecoveryRootSet *roots;
		ClusterRecoveryMultixactScan *scan;
		ClusterRecoveryWalTargetsDiag diag;
		uint8 digest[32];
		mx_physical_path(fd, 1, false, 0, path);
		if (mode)
			root_file(fd, path, data,
					  mode == 1	  ? BLCKSZ - 1
					  : mode == 3 ? 33 * BLCKSZ
					  : mode == 4 ? 0
								  : BLCKSZ);
		free(data);
		roots = begin_roots(fd, 1024 * 1024);
		if (mode == 0 || mode == 2 || mode == 4) {
			int page = 1;
			wal_fixture(RM_MULTIXACT_ID, XLOG_MULTIXACT_ZERO_OFF_PAGE, sizeof(page), 0);
			memcpy(wal_payload.bytes, &page, sizeof(page));
			UT_ASSERT(collect(roots, &diag));
		}
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		scan = mx_physical_begin(roots);
		if (scan) {
			UT_ASSERT_EQ(mx_physical_drain(scan), mode == 1 || mode == 3
													  ? CLUSTER_RECOVERY_MX_CORRUPT
													  : CLUSTER_RECOVERY_MX_BASE_REQUIRED);
			UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		}
		cluster_recovery_multixact_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_mx_physical_reads_and_rereads_refuse_all_io_failures)
{
	for (unsigned mode = 1; mode <= 3; ++mode) {
		int fd = new_root();
		char path[MAXPGPATH];
		PGAlignedBlock page = { 0 };
		ClusterRecoveryRootSet *roots;
		ClusterRecoveryMultixactScan *scan;
		ClusterRecoveryMultixactEntry out;
		ClusterRecoveryMultixactDiag diag;
		mx_physical_path(fd, 1, true, 0, path);
		root_file(fd, path, page.data, BLCKSZ);
		roots = begin_roots(fd, 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		scan = mx_physical_begin(roots);
		if (scan) {
			UT_ASSERT_EQ(cluster_recovery_multixact_scan_next(scan, &out),
						 CLUSTER_RECOVERY_MX_ENTRY);
			if (mode == 3)
				UT_ASSERT_EQ(cluster_recovery_multixact_scan_next(scan, &out),
							 CLUSTER_RECOVERY_MX_MORE);
			inject_pread_fault = mode == 3 ? 0 : mode;
			inject_read_corruption = mode == 3;
			UT_ASSERT_EQ(mx_physical_drain(scan),
						 mode == 3 ? CLUSTER_RECOVERY_MX_CHANGED : CLUSTER_RECOVERY_MX_IO);
			inject_pread_fault = 0;
			inject_read_corruption = false;
			memset(&out, 0xff, sizeof(out));
			memset(&diag, 0xff, sizeof(diag));
			UT_ASSERT(cluster_recovery_multixact_scan_next(scan, &out)
					  > CLUSTER_RECOVERY_MX_DRAINED);
			UT_ASSERT(out.bytes == NULL && out.value == 0);
			UT_ASSERT(!cluster_recovery_multixact_scan_diag(scan, &diag));
			UT_ASSERT_EQ(diag.files, 0);
		}
		cluster_recovery_multixact_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_mx_physical_mutation_and_final_recheck_revoke_observations)
{
	for (unsigned mode = 0; mode < 3; ++mode) {
		int fd = new_root();
		char path[MAXPGPATH];
		PGAlignedBlock page = { 0 };
		ClusterRecoveryRootSet *roots;
		ClusterRecoveryMultixactScan *scan;
		ClusterRecoveryMultixactEntry out;
		uint32 value = 55;
		mx_physical_path(fd, 1, false, 0, path);
		root_file(fd, path, page.data, BLCKSZ);
		roots = begin_roots(fd, 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		scan = mx_physical_begin(roots);
		if (scan) {
			UT_ASSERT_EQ(cluster_recovery_multixact_scan_next(scan, &out),
						 CLUSTER_RECOVERY_MX_ENTRY);
			UT_ASSERT_EQ(cluster_recovery_multixact_scan_next(scan, &out),
						 CLUSTER_RECOVERY_MX_MORE);
			if (mode == 2)
				UT_ASSERT_EQ(mx_physical_drain(scan), CLUSTER_RECOVERY_MX_DRAINED);
			if (mode == 0) {
				int file = openat(fd, path, O_WRONLY | O_APPEND);
				UT_ASSERT(file >= 0);
				UT_ASSERT_EQ(write(file, "x", 1), 1);
				UT_ASSERT_EQ(close(file), 0);
			} else if (mode == 1) {
				root_file(fd, "replacement", page.data, BLCKSZ);
				UT_ASSERT_EQ(renameat(fd, "replacement", fd, path), 0);
			} else {
				memcpy(page.data, &value, sizeof(value));
				root_file(fd, path, page.data, BLCKSZ);
				UT_ASSERT_EQ(cluster_recovery_root_set_recheck_begin(roots),
							 CLUSTER_RECOVERY_ROOTS_MORE);
				UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_CHANGED);
			}
			UT_ASSERT_EQ(mx_physical_drain(scan), CLUSTER_RECOVERY_MX_CHANGED);
		}
		cluster_recovery_multixact_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_mx_physical_budget_and_invalid_arguments_fail_closed)
{
	for (unsigned mode = 0; mode < 3; ++mode) {
		int fd = new_root();
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryMultixactScan *scan = NULL;
		ClusterRecoveryMultixactEntry out;
		uint8 digest[32];
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		if (mode == 0)
			UT_ASSERT_EQ(cluster_recovery_multixact_scan_begin(roots, 1, &scan),
						 CLUSTER_RECOVERY_MX_CAPACITY);
		else if (mode == 1)
			UT_ASSERT_EQ(cluster_recovery_multixact_scan_begin(roots, 65536, NULL),
						 CLUSTER_RECOVERY_MX_INVALID);
		else {
			scan = mx_physical_begin(roots);
			if (scan) {
				UT_ASSERT_EQ(cluster_recovery_multixact_scan_next(scan, NULL),
							 CLUSTER_RECOVERY_MX_INVALID);
				UT_ASSERT_EQ(cluster_recovery_multixact_scan_next(scan, &out),
							 CLUSTER_RECOVERY_MX_INVALID);
			}
		}
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		cluster_recovery_multixact_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_mx_physical_metadata_budget_preserves_untouched_files)
{
	int fd = new_root();
	char path[MAXPGPATH];
	PGAlignedBlock page = { 0 };
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryMultixactScan *scan;
	ClusterRecoveryMultixactEntry out;
	ClusterRecoveryMultixactDiag diag;
	for (unsigned i = 0; i < 200; ++i) {
		snprintf(path, sizeof(path), "a%03u", i);
		root_file(fd, path, "", 0);
	}
	mx_physical_path(fd, 1, true, 0, path);
	root_file(fd, path, page.data, BLCKSZ);
	roots = begin_roots(fd, 4 * 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	scan = mx_physical_begin(roots);
	if (scan) {
		UT_ASSERT_EQ(cluster_recovery_multixact_scan_next(scan, &out), CLUSTER_RECOVERY_MX_MORE);
		UT_ASSERT(cluster_recovery_multixact_scan_diag(scan, &diag));
		UT_ASSERT_EQ(diag.skipped_roots, 128);
		UT_ASSERT_EQ(mx_physical_drain(scan), CLUSTER_RECOVERY_MX_DRAINED);
		UT_ASSERT(cluster_recovery_multixact_scan_diag(scan, &diag));
		UT_ASSERT_EQ(diag.files, 1);
		UT_ASSERT_EQ(diag.member_slots, CLUSTER_NATIVE_MX_MEMBERS_PER_PAGE);
	}
	cluster_recovery_multixact_scan_destroy(scan);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}
