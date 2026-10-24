/* Terminal/assignment targets retain native file obligations without
 * granting a terminal, parent chain or permission to replay.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */

static void
xact_slru_record(uint8 opcode, TransactionId top, const TransactionId *children, unsigned count,
				 unsigned node)
{
	uint32 xinfo = XACT_XINFO_HAS_SUBXACTS;
	int nsubxacts = count;
	size_t base = opcode == XLOG_XACT_ASSIGNMENT
					  ? MinSizeOfXactAssignment
					  : sizeof(xl_xact_commit) + sizeof(xinfo) + sizeof(int);

	wal_fixture(RM_XACT_ID, opcode == XLOG_XACT_ASSIGNMENT ? opcode : opcode | XLOG_XACT_HAS_INFO,
				base + count * sizeof(TransactionId), 0);
	wal_record_storage.decoded.header.xl_xid = top;
	if (opcode == XLOG_XACT_ASSIGNMENT) {
		memcpy(wal_payload.bytes, &top, sizeof(top));
		memcpy(wal_payload.bytes + sizeof(top), &nsubxacts, sizeof(nsubxacts));
	} else {
		memcpy(wal_payload.bytes + sizeof(xl_xact_commit), &xinfo, sizeof(xinfo));
		memcpy(wal_payload.bytes + sizeof(xl_xact_commit) + sizeof(xinfo), &nsubxacts,
			   sizeof(nsubxacts));
	}
	if (count != 0)
		memcpy(wal_payload.bytes + base, children, count * sizeof(TransactionId));
	native_source.claim.identity.origin_node_id = node;
	native_source.claim.identity.origin_thread_id = node + 1;
	native_cut.failed_thread = node + 1;
	native_reader.EndRecPtr
		= native_reader.ReadRecPtr
		  + MAXALIGN(SizeOfXLogRecord + 2 + base + count * sizeof(TransactionId));
	wal_record_storage.decoded.next_lsn = native_reader.EndRecPtr;
	native_cut.scan_end_exclusive = native_reader.EndRecPtr;
}

UT_TEST(test_terminal_clog_targets_without_zero_keep_every_xid)
{
	const TransactionId children[] = { 32767, 32768, 1048576, UINT32_MAX };
	int fd = new_root();
	const char *dirs[]
		= { "pg_xact", "native_side", "native_side/origin_0", "native_side/origin_0/pg_xact" };
	for (unsigned i = 0; i < lengthof(dirs); ++i)
		UT_ASSERT_EQ(mkdirat(fd, dirs[i], 0700), 0);
	root_file(fd, "pg_xact/0000", wal_image, BLCKSZ);
	root_file(fd, "native_side/origin_0/pg_xact/0000", wal_image, BLCKSZ);
	for (unsigned aborting = 0; aborting < 2; ++aborting) {
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryWalTargetsDiag diag;
		const ClusterRecoveryRootEntry *entry;
		unsigned node = aborting ? 127 : 0;
		char path[MAXPGPATH];
		xact_slru_record(aborting ? XLOG_XACT_ABORT : XLOG_XACT_COMMIT, 4195136, children,
						 lengthof(children), node);
		UT_ASSERT(collect(roots, &diag));
		UT_ASSERT_EQ(diag.other_native_effects, 5);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		const char *segments[] = { "0000", "0001", "0004", "0FFF" };
		const unsigned blocks[] = { 2, 1, 1, 32 };
		for (unsigned i = 0; i < lengthof(segments); ++i) {
			snprintf(path, sizeof(path), "native_side/origin_%u/pg_xact/%s", node, segments[i]);
			mx_target_expect(roots, path, blocks[i], node, !aborting && i == 0);
			entry = mx_target_entry(roots, path);
			UT_ASSERT(entry && entry->file.native_family == CLUSTER_RECOVERY_NATIVE_FAMILY_XACT);
		}
		entry = mx_target_entry(roots, "pg_xact/0000");
		UT_ASSERT(entry && entry->origins == CLUSTER_RECOVERY_ROOT_PHYSICAL);
		UT_ASSERT(mx_target_entry(roots, "native_side/origin_0/pg_commit_ts/0000") == NULL);
		if (!aborting) {
			root_file(fd, "native_side/origin_0/pg_xact/0000", "changed", 7);
			UT_ASSERT_EQ(cluster_recovery_root_set_recheck_begin(roots),
						 CLUSTER_RECOVERY_ROOTS_MORE);
			UT_ASSERT(finish_roots(roots, NULL) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
			UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
		}
		cluster_recovery_root_set_destroy(roots);
	}
	remove_root(fd);
}

UT_TEST(test_assignment_targets_only_child_subtrans_pages)
{
	TransactionId children[300];
	int fd = new_root();
	ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
	ClusterRecoveryWalTargetsDiag diag;
	for (unsigned i = 0; i < lengthof(children); ++i)
		children[i] = 65535;
	children[298] = 65536;
	children[299] = UINT32_MAX;
	xact_slru_record(XLOG_XACT_ASSIGNMENT, 4195136, children, lengthof(children), 127);
	UT_ASSERT(collect(roots, &diag));
	UT_ASSERT_EQ(diag.other_native_effects, 301);
	/* A lower repeat must not shrink the existing 32-page requirement. */
	children[0] = 3;
	xact_slru_record(XLOG_XACT_ASSIGNMENT, 4195136, children, 1, 127);
	UT_ASSERT(collect(roots, &diag));
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT_EQ(cluster_recovery_root_set_count(roots), 3);
	mx_target_expect(roots, "native_side/origin_127/pg_subtrans/0000", 32, 127, false);
	mx_target_expect(roots, "native_side/origin_127/pg_subtrans/0001", 1, 127, false);
	mx_target_expect(roots, "native_side/origin_127/pg_subtrans/FFFF", 32, 127, false);
	for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); ++i) {
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);
		UT_ASSERT(entry && entry->file.native_family == CLUSTER_RECOVERY_NATIVE_FAMILY_SUBTRANS);
	}
	UT_ASSERT(mx_target_entry(roots, "native_side/origin_127/pg_subtrans/0040") == NULL);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_empty_terminal_has_clog_but_empty_assignment_has_no_page)
{
	int fd = new_root();
	const uint8 opcodes[] = { XLOG_XACT_COMMIT, XLOG_XACT_ABORT, XLOG_XACT_ASSIGNMENT };
	for (unsigned i = 0; i < lengthof(opcodes); ++i) {
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryWalTargetsDiag diag;
		xact_slru_record(opcodes[i], 4195136, NULL, 0, 0);
		UT_ASSERT(collect(roots, &diag));
		UT_ASSERT_EQ(diag.other_native_effects, 1);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT_EQ(cluster_recovery_root_set_count(roots), i == 2 ? 0 : 1);
		if (i != 2)
			mx_target_expect(roots, "native_side/origin_0/pg_xact/0004", 1, 0, false);
		cluster_recovery_root_set_destroy(roots);
	}
	remove_root(fd);
}

UT_TEST(test_transaction_target_refusal_never_exports_a_prefix)
{
	int fd = new_root();
	const uint8 opcodes[] = { XLOG_XACT_COMMIT, XLOG_XACT_ABORT, XLOG_XACT_ASSIGNMENT };
	for (unsigned op = 0; op < lengthof(opcodes); ++op)
		for (unsigned fault = 0; fault < 10; ++fault) {
			TransactionId children[] = { 32768, 1048576 };
			ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
			ClusterRecoveryWalTargetsDiag diag;
			if (fault < 3)
				children[1] = fault;
			xact_slru_record(opcodes[op], fault >= 3 && fault < 6 ? fault - 3 : 4195136, children,
							 lengthof(children), 0);
			if (fault == 6)
				native_source.claim.database_incarnation++;
			if (fault == 7)
				native_cut.scan_end_exclusive--;
			if (fault == 8)
				wal_record_storage.decoded.main_data_len--;
			if (fault == 9)
				UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
			UT_ASSERT(!collect(roots, &diag));
			UT_ASSERT(finish_roots(roots, NULL) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
			UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
			cluster_recovery_root_set_destroy(roots);
		}
	remove_root(fd);
}

UT_TEST(test_child_target_is_bound_to_original_owned_array_and_ordinal)
{
	int fd = new_root();
	TransactionId children[] = { 32768, 1048576 };
	for (unsigned fault = 0; fault < 4; ++fault) {
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryNativeScan *scan = NULL;
		ClusterRecoveryNativeEffect effect;
		xact_slru_record(XLOG_XACT_COMMIT, 4195136, children, 2, 0);
		UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&native_reader, &native_source, &native_cut,
														65536, &scan),
					 CLUSTER_RECOVERY_NATIVE_MORE);
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		UT_ASSERT_EQ(cluster_recovery_root_set_add_native(roots, &effect),
					 CLUSTER_RECOVERY_ROOTS_MORE);
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		if (fault == 0)
			effect.bytes = children;
		if (fault == 1)
			effect.ordinal++;
		if (fault == 2)
			effect.length--;
		if (fault == 3)
			effect.kind = CLUSTER_RECOVERY_NATIVE_TERMINAL;
		UT_ASSERT_EQ(cluster_recovery_root_set_add_native(roots, &effect),
					 CLUSTER_RECOVERY_ROOTS_INVALID);
		UT_ASSERT(finish_roots(roots, NULL) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
		cluster_recovery_native_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
	}
	remove_root(fd);
}
