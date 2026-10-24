/* Raw transaction SLRU obligations through the original real-file reader.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
#define RECOVERY_ROOT_SET_FIXTURE_ONLY
#include "test_cluster_recovery_root_set.c"
#include "../../backend/cluster/cluster_recovery_slru_scan.c"

static void
slru_path(int fd, uint32 origin, const char *family, uint32 segment, char *path)
{
	char prefix[MAXPGPATH];
	if (origin == UINT32_MAX)
		strlcpy(prefix, family, sizeof(prefix));
	else {
		if (mkdirat(fd, "native_side", 0700) != 0)
			UT_ASSERT_EQ(errno, EEXIST);
		snprintf(prefix, sizeof(prefix), "native_side/origin_%u", origin);
		if (mkdirat(fd, prefix, 0700) != 0)
			UT_ASSERT_EQ(errno, EEXIST);
		strlcat(prefix, "/", sizeof(prefix));
		strlcat(prefix, family, sizeof(prefix));
	}
	if (mkdirat(fd, prefix, 0700) != 0)
		UT_ASSERT_EQ(errno, EEXIST);
	snprintf(path, MAXPGPATH, "%s/%04X", prefix, segment);
}

static ClusterRecoverySlruScan *
slru_begin(ClusterRecoveryRootSet *roots)
{
	ClusterRecoverySlruScan *scan = NULL;
	UT_ASSERT_EQ(cluster_recovery_slru_scan_begin(roots, 64 * 1024, &scan),
				 CLUSTER_RECOVERY_SLRU_MORE);
	UT_ASSERT(scan != NULL);
	return scan;
}

static ClusterRecoverySlruResult
slru_drain(ClusterRecoverySlruScan *scan)
{
	ClusterRecoverySlruEntry out;
	for (unsigned i = 0; i < 200000; ++i) {
		ClusterRecoverySlruResult result = cluster_recovery_slru_scan_next(scan, &out);
		if (result > CLUSTER_RECOVERY_SLRU_ENTRY)
			return result;
	}
	UT_ASSERT(false);
	return CLUSTER_RECOVERY_SLRU_INVALID;
}

static void
slru_sticky(ClusterRecoverySlruScan *scan, ClusterRecoveryRootSet *roots,
			ClusterRecoverySlruResult expected)
{
	ClusterRecoverySlruEntry out, zero = { 0 };
	ClusterRecoverySlruDiag diag, empty = { 0 };
	uint8 digest[32];
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(cluster_recovery_slru_scan_next(scan, &out), expected);
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	memset(&diag, 0xa5, sizeof(diag));
	UT_ASSERT(!cluster_recovery_slru_scan_diag(scan, &diag));
	UT_ASSERT_EQ(memcmp(&diag, &empty, sizeof(diag)), 0);
	UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
}

UT_TEST(test_slru_all_raw_values_and_local_origin_namespaces)
{
	int fd = new_root();
	char path[MAXPGPATH];
	PGAlignedBlock page = { 0 };
	ClusterRecoveryRootSet *roots;
	ClusterRecoverySlruScan *scan;
	ClusterRecoverySlruEntry out;
	ClusterRecoverySlruDiag diag = { 0 };
	ClusterRecoverySlruResult result = CLUSTER_RECOVERY_SLRU_MORE;
	unsigned files = 0, clogs = 0, parents = 0, times = 0, padding = 0;
	for (unsigned n = 0; n < 2; ++n) {
		memset(&page, 0xe4, sizeof(page)); /* statuses 0,1,2,3; no terminal inference */
		slru_path(fd, n ? 0 : UINT32_MAX, "pg_xact", 0, path);
		root_file(fd, path, page.data, BLCKSZ);
		for (unsigned i = 0; i < BLCKSZ / 4; ++i) {
			uint32 parent = i % 2 ? UINT32_MAX : 0;
			memcpy(page.data + i * 4, &parent, 4);
		}
		slru_path(fd, n ? 127 : UINT32_MAX, "pg_subtrans", 0, path);
		root_file(fd, path, page.data, BLCKSZ);
		memset(&page, 0xa7, sizeof(page));
		for (unsigned i = 0; i < BLCKSZ / 10; ++i) {
			int64 timestamp = i % 2 ? PG_INT64_MIN : PG_INT64_MAX;
			uint16 origin = i;
			memcpy(page.data + i * 10, &timestamp, 8);
			memcpy(page.data + i * 10 + 8, &origin, 2);
		}
		slru_path(fd, n ? 127 : UINT32_MAX, "pg_commit_ts", 0, path);
		root_file(fd, path, page.data, BLCKSZ);
	}
	roots = begin_roots(fd, 2 * 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	scan = slru_begin(roots);
	if (scan)
		for (unsigned calls = 0; calls < 200000; ++calls) {
			result = cluster_recovery_slru_scan_next(scan, &out);
			if (result > CLUSTER_RECOVERY_SLRU_ENTRY)
				break;
			if (result != CLUSTER_RECOVERY_SLRU_ENTRY)
				continue;
			UT_ASSERT_EQ(out.file_kind == CLUSTER_RECOVERY_FILE_NATIVE_SLRU,
						 out.origin_node == UINT32_MAX);
			UT_ASSERT_EQ(out.segment, 0);
			if (out.kind == CLUSTER_RECOVERY_SLRU_FILE) {
				files++;
				UT_ASSERT_EQ(out.file_size, BLCKSZ);
				UT_ASSERT(out.bytes == NULL);
				continue;
			}
			UT_ASSERT_EQ(out.page_number, 0);
			if (out.kind == CLUSTER_RECOVERY_SLRU_PADDING) {
				padding += out.length;
				UT_ASSERT_EQ(out.family, CLUSTER_RECOVERY_NATIVE_FAMILY_COMMIT_TS);
				for (size_t i = 0; i < out.length; ++i)
					UT_ASSERT_EQ((unsigned char)out.bytes[i], 0xa7);
				continue;
			}
			UT_ASSERT(out.addressable);
			UT_ASSERT_EQ(out.raw_xid, out.slot);
			UT_ASSERT_EQ(out.length, BLCKSZ);
			if (out.kind == CLUSTER_RECOVERY_SLRU_CLOG) {
				clogs++;
				UT_ASSERT_EQ(out.family, CLUSTER_RECOVERY_NATIVE_FAMILY_XACT);
				UT_ASSERT(out.origin_node == UINT32_MAX || out.origin_node == 0);
				UT_ASSERT_EQ(out.raw_status, out.slot % 4);
			} else if (out.kind == CLUSTER_RECOVERY_SLRU_SUBTRANS) {
				parents++;
				UT_ASSERT_EQ(out.raw_parent, out.slot % 2 ? UINT32_MAX : 0);
			} else {
				times++;
				UT_ASSERT_EQ(out.kind, CLUSTER_RECOVERY_SLRU_COMMIT_TS);
				UT_ASSERT_EQ(out.raw_timestamp, out.slot % 2 ? PG_INT64_MIN : PG_INT64_MAX);
				UT_ASSERT_EQ(out.raw_replication_origin, out.slot);
			}
		}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_SLRU_DRAINED);
	UT_ASSERT_EQ(files, 6);
	UT_ASSERT_EQ(clogs, 2 * BLCKSZ * 4);
	UT_ASSERT_EQ(parents, 2 * BLCKSZ / 4);
	UT_ASSERT_EQ(times, 2 * (BLCKSZ / 10));
	UT_ASSERT_EQ(padding, 2 * (BLCKSZ % 10));
	UT_ASSERT(cluster_recovery_slru_scan_diag(scan, &diag));
	UT_ASSERT_EQ(diag.clog_slots, clogs);
	UT_ASSERT_EQ(diag.subtrans_slots, parents);
	UT_ASSERT_EQ(diag.commit_ts_slots, times);
	UT_ASSERT_EQ(diag.files, 6);
	UT_ASSERT_EQ(diag.pages, 6);
	UT_ASSERT_EQ(diag.reread_pages, 6);
	cluster_recovery_slru_scan_destroy(scan);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_slru_commit_ts_uint32_tail_does_not_wrap)
{
	int fd = new_root();
	char path[MAXPGPATH];
	uint32 per = BLCKSZ / 10;
	uint32 segment = UINT32_MAX / per / SLRU_PAGES_PER_SEGMENT;
	uint32 pages = UINT32_MAX / per % SLRU_PAGES_PER_SEGMENT + 1;
	char *bytes = calloc(pages, BLCKSZ);
	ClusterRecoveryRootSet *roots;
	ClusterRecoverySlruScan *scan;
	ClusterRecoverySlruEntry out;
	uint64 slots = 0;
	unsigned max_seen = 0, tail = 0;
	slru_path(fd, 3, "pg_commit_ts", segment, path);
	root_file(fd, path, bytes, pages * BLCKSZ);
	free(bytes);
	roots = begin_roots(fd, 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	scan = slru_begin(roots);
	if (scan)
		for (unsigned calls = 0; calls < 100000; ++calls) {
			ClusterRecoverySlruResult r = cluster_recovery_slru_scan_next(scan, &out);
			if (r == CLUSTER_RECOVERY_SLRU_DRAINED)
				break;
			UT_ASSERT(r <= CLUSTER_RECOVERY_SLRU_ENTRY);
			if (r != CLUSTER_RECOVERY_SLRU_ENTRY || out.kind != CLUSTER_RECOVERY_SLRU_COMMIT_TS)
				continue;
			UT_ASSERT_EQ(out.raw_xid, (uint64)segment * SLRU_PAGES_PER_SEGMENT * per + slots);
			UT_ASSERT_EQ(out.addressable, out.raw_xid <= UINT32_MAX);
			max_seen += out.raw_xid == UINT32_MAX;
			tail += !out.addressable;
			slots++;
		}
	UT_ASSERT_EQ(slots, (uint64)pages * per);
	UT_ASSERT_EQ(max_seen, 1);
	UT_ASSERT(tail > 0);
	cluster_recovery_slru_scan_destroy(scan);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_slru_empty_file_and_bounded_unknown_metadata)
{
	int fd = new_root();
	char path[MAXPGPATH];
	ClusterRecoveryRootSet *roots;
	ClusterRecoverySlruScan *scan;
	ClusterRecoverySlruEntry out;
	ClusterRecoverySlruDiag diag;
	unsigned files = 0, more = 0;
	for (unsigned i = 0; i < 300; ++i) {
		snprintf(path, sizeof(path), "unknown-%03u", i);
		root_file(fd, path, "", 0);
	}
	slru_path(fd, 127, "pg_subtrans", 0, path);
	root_file(fd, path, "", 0);
	roots = begin_roots(fd, 8 * 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	scan = slru_begin(roots);
	if (scan)
		for (unsigned calls = 0; calls < 20; ++calls) {
			ClusterRecoverySlruDiag before = { 0 };
			ClusterRecoverySlruResult r;
			UT_ASSERT(cluster_recovery_slru_scan_diag(scan, &before));
			r = cluster_recovery_slru_scan_next(scan, &out);
			UT_ASSERT(cluster_recovery_slru_scan_diag(scan, &diag));
			UT_ASSERT(diag.skipped_roots - before.skipped_roots <= 128);
			if (r == CLUSTER_RECOVERY_SLRU_DRAINED)
				break;
			UT_ASSERT(r <= CLUSTER_RECOVERY_SLRU_ENTRY);
			more += r == CLUSTER_RECOVERY_SLRU_MORE;
			if (r == CLUSTER_RECOVERY_SLRU_ENTRY) {
				files++;
				UT_ASSERT_EQ(out.kind, CLUSTER_RECOVERY_SLRU_FILE);
				UT_ASSERT_EQ(out.file_size, 0);
			}
		}
	UT_ASSERT_EQ(files, 1);
	UT_ASSERT(more >= 2);
	UT_ASSERT(cluster_recovery_slru_scan_diag(scan, &diag));
	UT_ASSERT_EQ(diag.pages | diag.subtrans_slots, 0);
	cluster_recovery_slru_scan_destroy(scan);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_slru_short_and_oversize_segments_refuse)
{
	for (unsigned variant = 0; variant < 2; ++variant) {
		int fd = new_root();
		char path[MAXPGPATH];
		size_t length = variant ? (SLRU_PAGES_PER_SEGMENT + 1) * BLCKSZ : BLCKSZ - 1;
		char *bytes = calloc(1, length);
		ClusterRecoveryRootSet *roots;
		ClusterRecoverySlruScan *scan;
		slru_path(fd, UINT32_MAX, "pg_xact", 0, path);
		root_file(fd, path, bytes, length);
		free(bytes);
		roots = begin_roots(fd, 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		scan = slru_begin(roots);
		if (scan) {
			UT_ASSERT_EQ(slru_drain(scan), CLUSTER_RECOVERY_SLRU_CORRUPT);
			slru_sticky(scan, roots, CLUSTER_RECOVERY_SLRU_CORRUPT);
		}
		cluster_recovery_slru_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_slru_original_read_io_short_read_and_reread_drift)
{
	for (unsigned variant = 0; variant < 3; ++variant) {
		int fd = new_root();
		char path[MAXPGPATH];
		PGAlignedBlock page = { 0 };
		ClusterRecoveryRootSet *roots;
		ClusterRecoverySlruScan *scan;
		ClusterRecoverySlruEntry out;
		ClusterRecoverySlruResult result;
		slru_path(fd, 0, "pg_subtrans", 0, path);
		root_file(fd, path, page.data, BLCKSZ);
		roots = begin_roots(fd, 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		scan = slru_begin(roots);
		if (scan) {
			if (variant == 2) {
				for (unsigned i = 0; i < 100; ++i) {
					result = cluster_recovery_slru_scan_next(scan, &out);
					UT_ASSERT(result <= CLUSTER_RECOVERY_SLRU_ENTRY);
					if (result == CLUSTER_RECOVERY_SLRU_ENTRY
						&& out.kind == CLUSTER_RECOVERY_SLRU_SUBTRANS)
						break;
				}
				inject_read_corruption = true; /* unchanged stat, different reread bytes */
			} else
				inject_pread_fault = variant + 1;
			result = slru_drain(scan);
			UT_ASSERT_EQ(result,
						 variant == 2 ? CLUSTER_RECOVERY_SLRU_CHANGED : CLUSTER_RECOVERY_SLRU_IO);
			inject_pread_fault = 0;
			inject_read_corruption = false;
			slru_sticky(scan, roots, result);
		}
		cluster_recovery_slru_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_slru_last_page_append_replace_and_root_revocation)
{
	for (unsigned variant = 0; variant < 4; ++variant) {
		int fd = new_root();
		char path[MAXPGPATH];
		PGAlignedBlock page = { 0 };
		ClusterRecoveryRootSet *roots;
		ClusterRecoverySlruScan *scan;
		ClusterRecoverySlruEntry out;
		slru_path(fd, 127, "pg_commit_ts", 0, path);
		root_file(fd, path, page.data, BLCKSZ);
		roots = begin_roots(fd, 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		scan = slru_begin(roots);
		if (scan) {
			if (variant < 2) {
				bool padding = false;
				for (unsigned i = 0; i < 2000; ++i) {
					ClusterRecoverySlruResult r = cluster_recovery_slru_scan_next(scan, &out);
					UT_ASSERT(r <= CLUSTER_RECOVERY_SLRU_ENTRY);
					if (r == CLUSTER_RECOVERY_SLRU_ENTRY
						&& out.kind == CLUSTER_RECOVERY_SLRU_PADDING) {
						padding = true;
						break;
					}
				}
				UT_ASSERT(padding);
				if (variant == 0) {
					int file = openat(fd, path, O_WRONLY | O_APPEND);
					UT_ASSERT(file >= 0);
					UT_ASSERT_EQ(write(file, page.data, BLCKSZ), BLCKSZ);
					UT_ASSERT_EQ(close(file), 0);
				} else {
					UT_ASSERT_EQ(renameat(fd, path, fd, "old-segment"), 0);
					root_file(fd, path, page.data, BLCKSZ);
				}
			} else {
				if (variant == 3)
					UT_ASSERT_EQ(slru_drain(scan), CLUSTER_RECOVERY_SLRU_DRAINED);
				cluster_recovery_root_set_invalidate(roots);
			}
			UT_ASSERT_EQ(slru_drain(scan), CLUSTER_RECOVERY_SLRU_CHANGED);
			slru_sticky(scan, roots, CLUSTER_RECOVERY_SLRU_CHANGED);
		}
		cluster_recovery_slru_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_slru_original_roots_and_capacity_are_required)
{
	for (unsigned variant = 0; variant < 3; ++variant) {
		int fd = new_root();
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoverySlruScan *scan = NULL;
		uint8 digest[32];
		if (variant != 0)
			UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT_EQ(cluster_recovery_slru_scan_begin(roots, variant == 1 ? 1 : 64 * 1024,
													  variant == 2 ? NULL : &scan),
					 variant == 1 ? CLUSTER_RECOVERY_SLRU_CAPACITY : CLUSTER_RECOVERY_SLRU_INVALID);
		UT_ASSERT(scan == NULL);
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
	cluster_recovery_slru_scan_destroy(NULL);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_slru_all_raw_values_and_local_origin_namespaces);
	UT_RUN(test_slru_commit_ts_uint32_tail_does_not_wrap);
	UT_RUN(test_slru_empty_file_and_bounded_unknown_metadata);
	UT_RUN(test_slru_short_and_oversize_segments_refuse);
	UT_RUN(test_slru_original_read_io_short_read_and_reread_drift);
	UT_RUN(test_slru_last_page_append_replace_and_root_revocation);
	UT_RUN(test_slru_original_roots_and_capacity_are_required);
	UT_DONE();
	return ut_failed_count != 0;
}
