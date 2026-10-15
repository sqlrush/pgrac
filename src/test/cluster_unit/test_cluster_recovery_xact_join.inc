/* Owned native WAL joined to actual original-origin files.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#include "../../backend/cluster/cluster_recovery_xact_join.c"

static void
xj_path(int fd, unsigned node, bool parent, TransactionId xid, char *path)
{
	uint32 per = parent ? CLUSTER_NATIVE_SUBTRANS_PER_PAGE : CLUSTER_NATIVE_CLOG_PER_PAGE;
	const char *family = parent ? "pg_subtrans" : "pg_xact";
	char dir[MAXPGPATH];
	if (node != UINT32_MAX) {
		UT_ASSERT(mkdirat(fd, "native_side", 0700) == 0 || errno == EEXIST);
		snprintf(dir, sizeof(dir), "native_side/origin_%u", node);
		UT_ASSERT(mkdirat(fd, dir, 0700) == 0 || errno == EEXIST);
		snprintf(dir, sizeof(dir), "native_side/origin_%u/%s", node, family);
	} else
		snprintf(dir, sizeof(dir), "%s", family);
	UT_ASSERT(mkdirat(fd, dir, 0700) == 0 || errno == EEXIST);
	snprintf(path, MAXPGPATH, "%s/%04X", dir, xid / per / SLRU_PAGES_PER_SEGMENT);
}

static void
xj_write(int fd, unsigned node, bool parent, TransactionId xid, uint32 value)
{
	uint32 per = parent ? CLUSTER_NATIVE_SUBTRANS_PER_PAGE : CLUSTER_NATIVE_CLOG_PER_PAGE;
	uint32 slot = xid % per;
	off_t off = (xid / per % SLRU_PAGES_PER_SEGMENT) * BLCKSZ;
	char path[MAXPGPATH];
	PGAlignedBlock page;
	struct stat st;
	int file;
	xj_path(fd, node, parent, xid, path);
	file = openat(fd, path, O_CREAT | O_RDWR, 0600);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(fstat(file, &st), 0);
	if (st.st_size < off + BLCKSZ)
		UT_ASSERT_EQ(ftruncate(file, off + BLCKSZ), 0);
	UT_ASSERT_EQ(pread(file, page.data, BLCKSZ, off), BLCKSZ);
	if (parent)
		memcpy(page.data + slot * sizeof(TransactionId), &value, sizeof(TransactionId));
	else {
		uint8 byte = (uint8)page.data[slot / 4];
		page.data[slot / 4] = (byte & ~(3U << ((slot % 4) * 2))) | (value << ((slot % 4) * 2));
	}
	UT_ASSERT_EQ(pwrite(file, page.data, BLCKSZ, off), BLCKSZ);
	UT_ASSERT_EQ(close(file), 0);
}

static ClusterRecoveryNativeScan *
xj_record(int fd, unsigned node, uint8 op, TransactionId top, const TransactionId *children,
		  unsigned count)
{
	ClusterRecoveryNativeScan *scan = NULL;
	bool parent = op == XLOG_XACT_ASSIGNMENT;
	uint32 value = parent					? top
				   : op == XLOG_XACT_COMMIT ? TRANSACTION_STATUS_COMMITTED
											: TRANSACTION_STATUS_ABORTED;
	xact_slru_record(op, top, children, count, node);
	UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&native_reader, &native_source, &native_cut,
													65536, &scan),
				 CLUSTER_RECOVERY_NATIVE_MORE);
	if (!parent)
		xj_write(fd, node, false, top, value);
	for (unsigned i = 0; i < count; ++i)
		xj_write(fd, node, parent, children[i], value);
	return scan;
}

static ClusterRecoveryXactJoin *
xj_start(ClusterRecoveryRootSet *roots, ClusterRecoveryNativeScan *scan)
{
	ClusterRecoveryXactJoin *join = NULL;
	UT_ASSERT_EQ(cluster_recovery_xact_join_begin(roots, scan, 65536, &join),
				 CLUSTER_RECOVERY_XACT_JOIN_MORE);
	return join;
}

static ClusterRecoveryXactJoinResult
xj_drain(ClusterRecoveryXactJoin *join, unsigned *count, unsigned *mismatch)
{
	ClusterRecoveryXactJoinEntry out;
	for (unsigned i = 0; i < 100000; ++i) {
		ClusterRecoveryXactJoinResult r = cluster_recovery_xact_join_next(join, &out);
		if (r == CLUSTER_RECOVERY_XACT_JOIN_ENTRY) {
			if (count)
				(*count)++;
			if (mismatch)
				*mismatch += !out.equal;
		} else if (r != CLUSTER_RECOVERY_XACT_JOIN_MORE)
			return r;
	}
	UT_ASSERT(false);
	return CLUSTER_RECOVERY_XACT_JOIN_INVALID;
}

UT_TEST(test_xact_join_original_origin_and_complete_membership)
{
	const uint8 ops[] = { XLOG_XACT_COMMIT, XLOG_XACT_ABORT, XLOG_XACT_ASSIGNMENT };
	const TransactionId children[] = { 32767, 32768, 1048576, UINT32_MAX };
	for (unsigned variant = 0; variant < 6; ++variant) {
		unsigned node = variant / 3 ? 127 : 0, count = 0;
		uint8 op = ops[variant % 3];
		bool parent = op == XLOG_XACT_ASSIGNMENT;
		int fd = new_root();
		ClusterRecoveryNativeScan *scan = xj_record(fd, node, op, 4195136, children, 4);
		ClusterRecoveryNativeEffect primary, effect;
		ClusterRecoveryRootSet *roots;
		ClusterRecoveryXactJoin *join;
		ClusterRecoveryXactJoinEntry out;
		ClusterRecoveryXactJoinResult r;
		xj_write(fd, UINT32_MAX, parent, children[0], 0);
		xj_write(fd, node ? 0 : 127, parent, children[0], 0);
		roots = begin_roots(fd, 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT_EQ(cluster_recovery_native_scan_primary(scan, &primary),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		join = xj_start(roots, scan);
		if (join) {
			while ((r = cluster_recovery_xact_join_next(join, &out))
				   <= CLUSTER_RECOVERY_XACT_JOIN_ENTRY) {
				bool top;
				unsigned ordinal;
				if (r == CLUSTER_RECOVERY_XACT_JOIN_MORE)
					continue;
				top = !parent && count == 0;
				ordinal = top ? 0 : count - !parent;
				UT_ASSERT(out.equal && out.record == primary.record);
				UT_ASSERT_EQ(out.is_top, top);
				UT_ASSERT_EQ(out.is_parent, parent);
				UT_ASSERT_EQ(out.ordinal, ordinal);
				UT_ASSERT_EQ(out.xid, top ? 4195136 : children[ordinal]);
				UT_ASSERT_EQ(out.record->source.claim.identity.origin_node_id, node);
				UT_ASSERT_EQ(out.record->cut.failed_thread, node + 1);
				UT_ASSERT_EQ(out.expected_value, parent ? 4195136 : op == XLOG_XACT_COMMIT ? 1 : 2);
				count++;
			}
			UT_ASSERT_EQ(r, CLUSTER_RECOVERY_XACT_JOIN_DRAINED);
			UT_ASSERT_EQ(count, parent ? 4 : 5);
			UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
						 CLUSTER_RECOVERY_NATIVE_EFFECT);
			UT_ASSERT_EQ(effect.kind, primary.kind);
		}
		cluster_recovery_xact_join_destroy(join);
		cluster_recovery_native_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_xact_join_empty_and_large_child_arrays)
{
	TransactionId children[300];
	const uint8 ops[] = { XLOG_XACT_COMMIT, XLOG_XACT_ABORT, XLOG_XACT_ASSIGNMENT };
	for (unsigned i = 0; i < 300; ++i)
		children[i] = 65535 + i;
	for (unsigned variant = 0; variant < 6; ++variant) {
		int fd = new_root();
		unsigned n = variant / 3 ? 300 : 0, count = 0, mismatch = 0;
		ClusterRecoveryNativeScan *scan = xj_record(fd, 1, ops[variant % 3], 3, children, n);
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryXactJoin *join;
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		join = xj_start(roots, scan);
		if (join) {
			UT_ASSERT_EQ(xj_drain(join, &count, &mismatch), CLUSTER_RECOVERY_XACT_JOIN_DRAINED);
			UT_ASSERT_EQ(count, n + (variant % 3 != 2));
			UT_ASSERT_EQ(mismatch, 0);
		}
		cluster_recovery_xact_join_destroy(join);
		cluster_recovery_native_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_xact_join_raw_mismatches_remain_obligations)
{
	const TransactionId children[] = { 4195137, 4195138, 4195139 };
	for (unsigned parent = 0; parent < 2; ++parent) {
		int fd = new_root();
		unsigned count = 0, mismatch = 0;
		ClusterRecoveryNativeScan *scan = xj_record(
			fd, 1, parent ? XLOG_XACT_ASSIGNMENT : XLOG_XACT_COMMIT, 4195136, children, 3);
		ClusterRecoveryRootSet *roots;
		ClusterRecoveryXactJoin *join;
		const uint32 raw[] = { 0, parent ? 1 : 2, parent ? UINT32_MAX : 3 };
		for (unsigned i = 0; i < 3; ++i)
			xj_write(fd, 1, parent, children[i], raw[i]);
		roots = begin_roots(fd, 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		join = xj_start(roots, scan);
		if (join) {
			UT_ASSERT_EQ(xj_drain(join, &count, &mismatch), CLUSTER_RECOVERY_XACT_JOIN_DRAINED);
			UT_ASSERT_EQ(count, parent ? 3 : 4);
			UT_ASSERT_EQ(mismatch, 3);
		}
		cluster_recovery_xact_join_destroy(join);
		cluster_recovery_native_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_xact_join_missing_short_io_and_reread_refuse)
{
	const TransactionId child = 4195137;
	for (unsigned mode = 0; mode < 5; ++mode) {
		int fd = new_root();
		char path[MAXPGPATH];
		uint8 digest[32];
		ClusterRecoveryNativeScan *scan = xj_record(fd, 1, XLOG_XACT_COMMIT, 4195136, &child, 1);
		ClusterRecoveryRootSet *roots;
		ClusterRecoveryXactJoin *join;
		ClusterRecoveryXactJoinEntry out;
		xj_path(fd, 1, false, child, path);
		if (mode == 0)
			UT_ASSERT_EQ(unlinkat(fd, path, 0), 0);
		if (mode == 1)
			root_file(fd, path, "x", 1);
		roots = begin_roots(fd, 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		join = xj_start(roots, scan);
		if (join) {
			if (mode > 1) {
				ClusterRecoveryXactJoinResult r;
				do {
					r = cluster_recovery_xact_join_next(join, &out);
				} while (r == CLUSTER_RECOVERY_XACT_JOIN_MORE);
				UT_ASSERT_EQ(r, CLUSTER_RECOVERY_XACT_JOIN_ENTRY);
				inject_pread_fault = mode == 4 ? 0 : mode - 1;
				inject_read_corruption = mode == 4;
			}
			UT_ASSERT(xj_drain(join, NULL, NULL) > CLUSTER_RECOVERY_XACT_JOIN_DRAINED);
			inject_pread_fault = 0;
			inject_read_corruption = false;
			memset(&out, 0xff, sizeof(out));
			UT_ASSERT(cluster_recovery_xact_join_next(join, &out)
					  > CLUSTER_RECOVERY_XACT_JOIN_DRAINED);
			UT_ASSERT(out.record == NULL && out.xid == 0);
			UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		}
		cluster_recovery_xact_join_destroy(join);
		cluster_recovery_native_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_xact_join_scope_capacity_and_special_requested_xid_refuse)
{
	for (unsigned mode = 0; mode < 9; ++mode) {
		int fd = new_root();
		TransactionId child = mode >= 6 ? mode - 6 : 4195137;
		ClusterRecoveryNativeScan *scan = xj_record(fd, 1, XLOG_XACT_COMMIT, 4195136, &child, 1);
		ClusterRecoveryRootSet *roots = NULL;
		ClusterRecoveryXactJoin *join = NULL;
		ClusterRecoveryXactJoinResult r;
		uint8 uuid[16], digest[32];
		memcpy(uuid, root_uuid, 16);
		if (mode == 0)
			uuid[0]++;
		UT_ASSERT_EQ(cluster_recovery_root_set_begin(fd, mode == 1 ? 818 : 817, mode == 2 ? 92 : 91,
													 uuid, 1024 * 1024, &roots),
					 CLUSTER_RECOVERY_ROOTS_MORE);
		if (mode != 4)
			UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		if (mode == 5) {
			cluster_recovery_native_scan_destroy(scan);
			wal_fixture(RM_CLOG_ID, CLOG_ZEROPAGE, sizeof(int), 0);
			scan = NULL;
			UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&native_reader, &native_source,
															&native_cut, 65536, &scan),
						 CLUSTER_RECOVERY_NATIVE_MORE);
		}
		r = cluster_recovery_xact_join_begin(roots, scan, mode == 3 ? 1 : 65536, &join);
		if (r == CLUSTER_RECOVERY_XACT_JOIN_MORE)
			r = xj_drain(join, NULL, NULL);
		UT_ASSERT(r > CLUSTER_RECOVERY_XACT_JOIN_DRAINED);
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		cluster_recovery_xact_join_destroy(join);
		cluster_recovery_native_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_xact_join_drained_revoked_by_original_owner)
{
	for (unsigned mode = 0; mode < 3; ++mode) {
		int fd = new_root();
		TransactionId child = 4195137;
		ClusterRecoveryNativeScan *scan
			= xj_record(fd, 1, XLOG_XACT_ASSIGNMENT, 4195136, &child, 1);
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryXactJoin *join;
		ClusterRecoveryXactJoinEntry out;
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		join = xj_start(roots, scan);
		if (join) {
			UT_ASSERT_EQ(xj_drain(join, NULL, NULL), CLUSTER_RECOVERY_XACT_JOIN_DRAINED);
			if (mode == 0) {
				wal_payload.bytes[MinSizeOfXactAssignment] ^= 1;
				UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &native_reader,
																  &native_source, &native_cut),
							 CLUSTER_RECOVERY_NATIVE_CHANGED);
			} else if (mode == 1)
				cluster_recovery_root_set_invalidate(roots);
			else {
				xj_write(fd, 1, true, child, 1);
				UT_ASSERT_EQ(cluster_recovery_root_set_recheck_begin(roots),
							 CLUSTER_RECOVERY_ROOTS_MORE);
				UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_CHANGED);
			}
			UT_ASSERT_EQ(cluster_recovery_xact_join_next(join, &out),
						 CLUSTER_RECOVERY_XACT_JOIN_CHANGED);
			UT_ASSERT(out.record == NULL);
		}
		cluster_recovery_xact_join_destroy(join);
		cluster_recovery_native_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_xact_join_metadata_has_bounded_progress)
{
	int fd = new_root();
	char name[40];
	unsigned count = 0;
	ClusterRecoveryNativeScan *scan = xj_record(fd, 1, XLOG_XACT_COMMIT, 4195136, NULL, 0);
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryXactJoin *join;
	ClusterRecoveryXactJoinEntry out;
	for (unsigned i = 0; i < 300; ++i) {
		snprintf(name, sizeof(name), "unknown%u", i);
		root_file(fd, name, "", 0);
	}
	roots = begin_roots(fd, 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	join = xj_start(roots, scan);
	if (join) {
		for (unsigned i = 0; i < 3; ++i) {
			UT_ASSERT_EQ(cluster_recovery_xact_join_next(join, &out),
						 CLUSTER_RECOVERY_XACT_JOIN_MORE);
			UT_ASSERT(out.record == NULL);
		}
		UT_ASSERT_EQ(xj_drain(join, &count, NULL), CLUSTER_RECOVERY_XACT_JOIN_DRAINED);
		UT_ASSERT_EQ(count, 1);
	}
	cluster_recovery_xact_join_destroy(join);
	cluster_recovery_native_scan_destroy(scan);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}
