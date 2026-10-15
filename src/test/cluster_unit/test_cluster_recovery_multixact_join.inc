/* Actual origin files joined to original owned native WAL, no visibility API.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#include "../../backend/cluster/cluster_recovery_multixact_join.c"

static void
mx_join_write(int fd, unsigned node, bool members, uint32 address, uint32 value, uint8 status)
{
	uint32 per = members ? CLUSTER_NATIVE_MX_MEMBERS_PER_PAGE : CLUSTER_NATIVE_MX_OFFSETS_PER_PAGE;
	uint32 page_number = address / per, slot = address % per;
	char path[MAXPGPATH];
	PGAlignedBlock page = { 0 };
	struct stat st;
	int file;
	off_t offset = (page_number % 32) * BLCKSZ;
	mx_physical_path(fd, node, members, page_number / 32, path);
	file = openat(fd, path, O_RDWR | O_CREAT, 0600);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(fstat(file, &st), 0);
	if (st.st_size < offset + BLCKSZ)
		UT_ASSERT_EQ(ftruncate(file, offset + BLCKSZ), 0);
	UT_ASSERT_EQ(pread(file, page.data, BLCKSZ, offset), BLCKSZ);
	if (members)
		mx_physical_member(page.data, slot, value, status);
	else
		memcpy(page.data + slot * sizeof(uint32), &value, sizeof(value));
	UT_ASSERT_EQ(pwrite(file, page.data, BLCKSZ, offset), BLCKSZ);
	UT_ASSERT_EQ(close(file), 0);
}

static ClusterRecoveryNativeScan *
mx_join_create(int fd, unsigned node, uint32 mid, uint32 offset, unsigned count)
{
	xl_multixact_create create = { 0 };
	ClusterRecoveryNativeScan *scan = NULL;
	create.mid = mid;
	create.moff = offset;
	create.nmembers = count;
	wal_fixture(RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID,
				SizeOfMultiXactCreate + count * sizeof(MultiXactMember), 0);
	native_source.claim.identity.origin_node_id = node;
	native_source.claim.identity.origin_thread_id = node + 1;
	native_cut.failed_thread = node + 1;
	memcpy(wal_payload.bytes, &create, SizeOfMultiXactCreate);
	mx_join_write(fd, node, false, mid, offset, 0);
	for (unsigned i = 0; i < count; ++i) {
		MultiXactMember member = { 4195136 + i, i % 6 };
		memcpy(wal_payload.bytes + SizeOfMultiXactCreate + i * sizeof(member), &member,
			   sizeof(member));
		mx_join_write(fd, node, true, (uint32)(offset + i), member.xid, member.status);
	}
	UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&native_reader, &native_source, &native_cut,
													65536, &scan),
				 CLUSTER_RECOVERY_NATIVE_MORE);
	return scan;
}

static ClusterRecoveryMultixactJoin *
mx_join_start(ClusterRecoveryRootSet *roots, ClusterRecoveryNativeScan *scan)
{
	ClusterRecoveryMultixactJoin *join = NULL;
	UT_ASSERT_EQ(cluster_recovery_multixact_join_begin(roots, scan, 65536, &join),
				 CLUSTER_RECOVERY_MX_JOIN_MORE);
	return join;
}

static ClusterRecoveryMultixactJoinResult
mx_join_drain(ClusterRecoveryMultixactJoin *join, unsigned *entries, unsigned *mismatches)
{
	ClusterRecoveryMultixactJoinEntry out;
	for (unsigned i = 0; i < 100000; ++i) {
		ClusterRecoveryMultixactJoinResult result
			= cluster_recovery_multixact_join_next(join, &out);
		if (result == CLUSTER_RECOVERY_MX_JOIN_ENTRY) {
			if (entries)
				(*entries)++;
			if (mismatches)
				*mismatches += !out.equal;
		} else if (result != CLUSTER_RECOVERY_MX_JOIN_MORE)
			return result;
	}
	UT_ASSERT(false);
	return CLUSTER_RECOVERY_MX_JOIN_INVALID;
}

UT_TEST(test_mx_join_keeps_origin_record_and_independent_cursor)
{
	for (unsigned variant = 0; variant < 2; ++variant) {
		int fd = new_root();
		unsigned node = variant ? 127 : 0, entries = 0;
		ClusterRecoveryNativeScan *scan = mx_join_create(fd, node, 42, 108, 3);
		ClusterRecoveryNativeEffect primary, next;
		ClusterRecoveryRootSet *roots;
		ClusterRecoveryMultixactJoin *join;
		ClusterRecoveryMultixactJoinEntry out;
		mx_join_write(fd, UINT32_MAX, false, 42, 999, 0);
		mx_join_write(fd, node == 0 ? 1 : 0, true, 108, 999, 255);
		roots = begin_roots(fd, 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT_EQ(cluster_recovery_native_scan_primary(scan, &primary),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		join = mx_join_start(roots, scan);
		if (join) {
			ClusterRecoveryMultixactJoinResult result;
			while ((result = cluster_recovery_multixact_join_next(join, &out))
				   <= CLUSTER_RECOVERY_MX_JOIN_ENTRY) {
				if (result == CLUSTER_RECOVERY_MX_JOIN_MORE)
					continue;
				UT_ASSERT(out.equal);
				UT_ASSERT(out.record == primary.record);
				UT_ASSERT_EQ(out.record->source.claim.identity.origin_node_id, node);
				UT_ASSERT_EQ(out.record->cut.failed_thread, node + 1);
				UT_ASSERT_EQ(out.expected_value, entries ? 4195135 + entries : 108);
				UT_ASSERT_EQ(out.address, entries ? 107 + entries : 42);
				UT_ASSERT_EQ(out.is_offset, entries == 0);
				entries++;
			}
			UT_ASSERT_EQ(result, CLUSTER_RECOVERY_MX_JOIN_DRAINED);
			UT_ASSERT_EQ(entries, 4);
			UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &next),
						 CLUSTER_RECOVERY_NATIVE_EFFECT);
			UT_ASSERT_EQ(next.kind, CLUSTER_RECOVERY_NATIVE_MULTIXACT_CREATE);
		}
		cluster_recovery_multixact_join_destroy(join);
		cluster_recovery_native_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_mx_join_crosses_pages_segments_wrap_and_online_size_limit)
{
	const uint32 offsets[] = { CLUSTER_NATIVE_MX_MEMBERS_PER_PAGE - 1,
							   32 * CLUSTER_NATIVE_MX_MEMBERS_PER_PAGE - 1, UINT32_MAX };
	for (unsigned i = 0; i < lengthof(offsets); ++i) {
		int fd = new_root();
		ClusterRecoveryNativeScan *scan = mx_join_create(fd, 1, UINT32_MAX, offsets[i], 300);
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryMultixactJoin *join;
		unsigned entries = 0, mismatches = 0;
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		join = mx_join_start(roots, scan);
		if (join) {
			UT_ASSERT_EQ(mx_join_drain(join, &entries, &mismatches),
						 CLUSTER_RECOVERY_MX_JOIN_DRAINED);
			UT_ASSERT_EQ(entries, 301);
			UT_ASSERT_EQ(mismatches, 0);
		}
		cluster_recovery_multixact_join_destroy(join);
		cluster_recovery_native_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_mx_join_retains_physical_zero_unknown_and_mismatch)
{
	int fd = new_root();
	ClusterRecoveryNativeScan *scan = mx_join_create(fd, 1, 42, 108, 3);
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryMultixactJoin *join;
	unsigned entries = 0, mismatches = 0;
	mx_join_write(fd, 1, false, 42, 0, 0);
	mx_join_write(fd, 1, true, 108, 0, 255);
	mx_join_write(fd, 1, true, 109, 4195137, 5);
	roots = begin_roots(fd, 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	join = mx_join_start(roots, scan);
	if (join) {
		UT_ASSERT_EQ(mx_join_drain(join, &entries, &mismatches), CLUSTER_RECOVERY_MX_JOIN_DRAINED);
		UT_ASSERT_EQ(entries, 4);
		UT_ASSERT_EQ(mismatches, 3);
	}
	cluster_recovery_multixact_join_destroy(join);
	cluster_recovery_native_scan_destroy(scan);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_mx_join_missing_short_io_and_page_reread_fail_sticky)
{
	for (unsigned mode = 0; mode < 5; ++mode) {
		int fd = new_root();
		ClusterRecoveryNativeScan *scan = mx_join_create(fd, 1, 42, 108, 3);
		ClusterRecoveryRootSet *roots;
		ClusterRecoveryMultixactJoin *join;
		ClusterRecoveryMultixactJoinEntry out;
		char path[MAXPGPATH];
		uint8 digest[32];
		mx_physical_path(fd, 1, true, 0, path);
		if (mode == 0)
			UT_ASSERT_EQ(unlinkat(fd, path, 0), 0);
		if (mode == 1)
			root_file(fd, path, "x", 1);
		roots = begin_roots(fd, 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		join = mx_join_start(roots, scan);
		if (join) {
			if (mode > 1) {
				ClusterRecoveryMultixactJoinResult result;
				do {
					result = cluster_recovery_multixact_join_next(join, &out);
				} while (result == CLUSTER_RECOVERY_MX_JOIN_MORE);
				UT_ASSERT_EQ(result, CLUSTER_RECOVERY_MX_JOIN_ENTRY);
				inject_pread_fault = mode == 4 ? 0 : mode - 1;
				inject_read_corruption = mode == 4;
			}
			UT_ASSERT(mx_join_drain(join, NULL, NULL) > CLUSTER_RECOVERY_MX_JOIN_DRAINED);
			inject_pread_fault = 0;
			inject_read_corruption = false;
			memset(&out, 0xff, sizeof(out));
			UT_ASSERT(cluster_recovery_multixact_join_next(join, &out)
					  > CLUSTER_RECOVERY_MX_JOIN_DRAINED);
			UT_ASSERT(out.record == NULL && out.observed_value == 0);
			UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		}
		cluster_recovery_multixact_join_destroy(join);
		cluster_recovery_native_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_mx_join_rejects_scope_capacity_and_non_create)
{
	for (unsigned mode = 0; mode < 6; ++mode) {
		int fd = new_root();
		ClusterRecoveryNativeScan *scan = mx_join_create(fd, 1, 42, 108, 3);
		ClusterRecoveryRootSet *roots = NULL;
		ClusterRecoveryMultixactJoin *join = NULL;
		uint8 uuid[16], digest[32];
		memcpy(uuid, root_uuid, 16);
		if (mode == 0)
			uuid[0]++;
		UT_ASSERT_EQ(cluster_recovery_root_set_begin(fd, mode == 1 ? 818 : 817, mode == 2 ? 92 : 91,
													 uuid, 1024 * 1024, &roots),
					 CLUSTER_RECOVERY_ROOTS_MORE);
		if (mode != 5)
			UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		if (mode == 4) {
			int page = 0;

			cluster_recovery_native_scan_destroy(scan);
			wal_fixture(RM_MULTIXACT_ID, XLOG_MULTIXACT_ZERO_OFF_PAGE, sizeof(page), 0);
			memcpy(wal_payload.bytes, &page, sizeof(page));
			scan = NULL;
			UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&native_reader, &native_source,
															&native_cut, 65536, &scan),
						 CLUSTER_RECOVERY_NATIVE_MORE);
		}
		UT_ASSERT(cluster_recovery_multixact_join_begin(roots, scan, mode == 3 ? 1 : 65536, &join)
				  > CLUSTER_RECOVERY_MX_JOIN_DRAINED);
		UT_ASSERT(join == NULL);
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		cluster_recovery_native_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_mx_join_final_wal_or_roots_drift_revokes_drained)
{
	for (unsigned mode = 0; mode < 3; ++mode) {
		int fd = new_root();
		ClusterRecoveryNativeScan *scan = mx_join_create(fd, 1, 42, 108, 3);
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryMultixactJoin *join;
		ClusterRecoveryMultixactJoinEntry out;
		uint8 digest[32];
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		join = mx_join_start(roots, scan);
		if (join) {
			UT_ASSERT_EQ(mx_join_drain(join, NULL, NULL), CLUSTER_RECOVERY_MX_JOIN_DRAINED);
			if (mode == 0) {
				wal_payload.bytes[SizeOfMultiXactCreate] ^= 1;
				UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &native_reader,
																  &native_source, &native_cut),
							 CLUSTER_RECOVERY_NATIVE_CHANGED);
			} else if (mode == 1)
				cluster_recovery_root_set_invalidate(roots);
			else {
				mx_join_write(fd, 1, true, 108, 11, 0);
				UT_ASSERT_EQ(cluster_recovery_root_set_recheck_begin(roots),
							 CLUSTER_RECOVERY_ROOTS_MORE);
				UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_CHANGED);
			}
			UT_ASSERT_EQ(cluster_recovery_multixact_join_next(join, &out),
						 CLUSTER_RECOVERY_MX_JOIN_CHANGED);
			UT_ASSERT(out.record == NULL);
			UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		}
		cluster_recovery_multixact_join_destroy(join);
		cluster_recovery_native_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_mx_join_metadata_search_has_a_bounded_step)
{
	int fd = new_root();
	char name[64];
	ClusterRecoveryNativeScan *scan = mx_join_create(fd, 1, 42, 108, 3);
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryMultixactJoin *join;
	ClusterRecoveryMultixactJoinEntry out;
	for (unsigned i = 0; i < 200; ++i) {
		snprintf(name, sizeof(name), "unknown%u", i);
		root_file(fd, name, "", 0);
	}
	roots = begin_roots(fd, 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	join = mx_join_start(roots, scan);
	if (join) {
		UT_ASSERT_EQ(cluster_recovery_multixact_join_next(join, &out),
					 CLUSTER_RECOVERY_MX_JOIN_MORE);
		UT_ASSERT(out.record == NULL);
		UT_ASSERT_EQ(mx_join_drain(join, NULL, NULL), CLUSTER_RECOVERY_MX_JOIN_DRAINED);
	}
	cluster_recovery_multixact_join_destroy(join);
	cluster_recovery_native_scan_destroy(scan);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}
