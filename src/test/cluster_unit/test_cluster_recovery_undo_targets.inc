/* Retained undo WAL targets use the original codecs and real namespace.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */

static uint64
undo_wal_fixture(unsigned shape, uint32 segment)
{
	uint64 required = 1;
	uint8 instance = (segment - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
#define UNDO_FIX(op_, rec_, extra_)                                                                \
	do {                                                                                           \
		wal_fixture(RM_CLUSTER_UNDO_ID, (op_), sizeof(rec_) + (extra_), 0);                        \
		memcpy(wal_payload.bytes, &(rec_), sizeof(rec_));                                          \
	} while (0)
	switch (shape) {
	case 0: {
		xl_cluster_undo_segment_init r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		UNDO_FIX(XLOG_UNDO_SEGMENT_INIT, r, BLCKSZ);
		break;
	}
	case 1: {
		xl_undo_tt_slot_bind r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		r.xid = 4195136;
		r.wrap = 1;
		r.format_version = CLUSTER_UNDO_TT_BIND_VERSION;
		UNDO_FIX(XLOG_UNDO_TT_SLOT_BIND, r, 0);
		break;
	}
	case 2: {
		xl_undo_tt_slot_commit r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		r.xid = 4195136;
		r.wrap = 1;
		r.commit_scn = 42;
		UNDO_FIX(XLOG_UNDO_TT_SLOT_COMMIT, r, 0);
		break;
	}
	case 3: {
		xl_undo_segment_recycle r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		r.old_state = SEGMENT_COMMITTED;
		r.new_state = SEGMENT_RECYCLABLE;
		UNDO_FIX(XLOG_UNDO_SEGMENT_RECYCLE, r, 0);
		break;
	}
	case 4: {
		xl_undo_segment_reuse r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		r.new_generation = 1;
		UNDO_FIX(XLOG_UNDO_SEGMENT_REUSE, r, BLCKSZ);
		break;
	}
	case 5: {
		xl_undo_tt_slot_abort r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		r.xid = 4195136;
		r.wrap = 1;
		UNDO_FIX(XLOG_UNDO_TT_SLOT_ABORT, r, 0);
		break;
	}
	case 6: {
		xl_undo_tt_slot_abort_exact r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		r.xid = 4195136;
		r.wrap = 1;
		r.format_version = CLUSTER_UNDO_TT_ABORT_EXACT_VERSION;
		UNDO_FIX(XLOG_UNDO_TT_SLOT_ABORT, r, 0);
		break;
	}
	case 7: {
		xl_undo_block_write r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		r.block_no = 9;
		r.has_fpi = 1;
		UNDO_FIX(XLOG_UNDO_BLOCK_WRITE, r, BLCKSZ);
		required = 10;
		break;
	}
	case 8: {
		xl_undo_block_write_multi r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		r.block_no = 12;
		r.has_fpi = 1;
		UNDO_FIX(XLOG_UNDO_BLOCK_WRITE_MULTI, r, BLCKSZ);
		required = 13;
		break;
	}
	case 9: {
		xl_undo_tt_slot_set_head r = { 0 };
		r.segment_id = segment;
		r.instance = instance;
		r.xid = 4195136;
		r.wrap = 1;
		r.first_undo_block = uba_encode(segment, 7, 0, 0);
		UNDO_FIX(XLOG_UNDO_TT_SLOT_SET_HEAD, r, 0);
		required = 8;
		break;
	}
	default: {
		xl_undo_tt_slot_ctrc_release_v1 r = { 0 };
		r.segment_id = segment;
		r.owner_instance = instance;
		r.xid = 4195136;
		r.slot_wrap = 1;
		r.cluster_epoch = 1;
		r.root_id = 1;
		r.root_generation = 1;
		r.formation_epoch = 1;
		r.admission_record_generation = 1;
		r.seal_generation = 1;
		r.terminal_status = TT_SLOT_COMMITTED;
		r.ack_set_digest[0] = 1;
		r.format_version = CLUSTER_UNDO_TT_CTRC_RELEASE_VERSION;
		r.flags = CLUSTER_UNDO_TT_CTRC_RELEASE_ALL_TOUCHED_ACKED;
		wal_fixture(RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_CTRC_RELEASE,
					CLUSTER_UNDO_TT_CTRC_RELEASE_BYTES, 0);
		UT_ASSERT(cluster_undo_tt_ctrc_release_encode(&r, (uint8 *)wal_payload.bytes));
		break;
	}
	}
#undef UNDO_FIX
	return required;
}

static const ClusterRecoveryRootEntry *
find_undo_target(ClusterRecoveryRootSet *roots, uint32 segment)
{
	for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); i++) {
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);
		if (entry != NULL && entry->file.kind == CLUSTER_RECOVERY_FILE_UNDO_SEGMENT
			&& entry->file.undo_segment == segment)
			return entry;
	}
	return NULL;
}

UT_TEST(test_undo_wal_all_shapes_preserve_segment_targets)
{
	int fd = new_root();
	for (unsigned shape = 0; shape < 11; shape++) {
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryWalTargetsDiag diag;
		const ClusterRecoveryRootEntry *entry;
		uint64 required = undo_wal_fixture(shape, 257);
		UT_ASSERT(collect(roots, &diag));
		/* Target extraction never discharges TT/undo effects. */
		UT_ASSERT_EQ(diag.unowned_records, 1);
		UT_ASSERT_EQ(diag.undo_targets, 1);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		entry = find_undo_target(roots, 257);
		UT_ASSERT(entry != NULL);
		if (entry != NULL) {
			UT_ASSERT_EQ(entry->file.undo_owner_instance, 2);
			UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_FROM_WAL);
			UT_ASSERT_EQ(entry->required_blocks, required);
			UT_ASSERT(strcmp(entry->file.path, "pg_undo/instance_1/seg_257.dat") == 0);
		}
		cluster_recovery_root_set_destroy(roots);
	}
	remove_root(fd);
}

UT_TEST(test_undo_wal_union_keeps_missing_and_largest_requirement)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryWalTargetsDiag diag;
	const ClusterRecoveryRootEntry *entry;
	UT_ASSERT_EQ(mkdirat(fd, "pg_undo", 0700), 0);
	UT_ASSERT_EQ(mkdirat(fd, "pg_undo/instance_1", 0700), 0);
	root_file(fd, "pg_undo/instance_1/seg_257.dat", "", 0);
	root_file(fd, "pg_undo/instance_1/seg_259.dat", wal_image, BLCKSZ);
	roots = begin_roots(fd, 1024 * 1024);
	for (unsigned i = 0; i < 3; i++) {
		undo_wal_fixture(i == 0 ? 8 : 1, 257);
		UT_ASSERT(collect(roots, &diag));
	}
	undo_wal_fixture(0, 258);
	UT_ASSERT(collect(roots, &diag));
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	entry = find_undo_target(roots, 257);
	UT_ASSERT(entry != NULL);
	if (entry != NULL) {
		UT_ASSERT_EQ(entry->origins,
					 CLUSTER_RECOVERY_ROOT_FROM_WAL | CLUSTER_RECOVERY_ROOT_PHYSICAL);
		UT_ASSERT_EQ(entry->required_blocks, 13);
		UT_ASSERT_EQ(entry->file.size, 0);
	}
	entry = find_undo_target(roots, 258);
	UT_ASSERT(entry != NULL);
	if (entry != NULL)
		UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_FROM_WAL);
	entry = find_undo_target(roots, 259);
	UT_ASSERT(entry != NULL);
	if (entry != NULL)
		UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_PHYSICAL);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

static void
folded_tt_fixture(xl_xact_tt_commit *tt)
{
	TimestampTz time = 42;
	uint32 xinfo = XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_TT_COMMIT;
	SCN scn = 42;
	size_t at = 0;
	wal_fixture(RM_XACT_ID, XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO,
				MinSizeOfXactCommit + sizeof(xinfo) + sizeof(scn) + sizeof(*tt), 0);
	memcpy(wal_payload.bytes + at, &time, sizeof(time));
	at += sizeof(time);
	memcpy(wal_payload.bytes + at, &xinfo, sizeof(xinfo));
	at += sizeof(xinfo);
	memcpy(wal_payload.bytes + at, &scn, sizeof(scn));
	at += sizeof(scn);
	memcpy(wal_payload.bytes + at, tt, sizeof(*tt));
}

UT_TEST(test_folded_tt_commit_keeps_undo_target_and_terminal_obligation)
{
	int fd = new_root();
	xl_xact_tt_commit tt = { 0 };
	ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
	ClusterRecoveryWalTargetsDiag diag;
	const ClusterRecoveryRootEntry *entry;
	tt.segment_id = 257;
	tt.instance = 2;
	tt.wrap = 1;
	tt.xid = 4195136;
	tt.format_version = CLUSTER_XACT_TT_COMMIT_VERSION;
	tt.commit_scn = 42;
	folded_tt_fixture(&tt);
	UT_ASSERT(collect(roots, &diag));
	UT_ASSERT_EQ(diag.native_effects, 2);
	UT_ASSERT_EQ(diag.undo_targets, 1);
	UT_ASSERT_EQ(diag.other_native_effects, 2);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	entry = find_undo_target(roots, 257);
	UT_ASSERT(entry != NULL);
	if (entry != NULL)
		UT_ASSERT_EQ(entry->required_blocks, 1);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_undo_wal_malformed_targets_poison_whole_roots)
{
	int fd = new_root();
	for (unsigned variant = 0; variant < 12; variant++) {
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryWalTargetsDiag diag;
		xl_undo_tt_slot_bind r = { 0 };
		uint8 digest[32];
		undo_wal_fixture(1, 257);
		UT_ASSERT(collect(roots, &diag));
		memcpy(&r, wal_payload.bytes, sizeof(r));
		switch (variant) {
		case 0:
			r.segment_id = 0;
			break;
		case 1:
			r.instance = 1;
			break;
		case 2:
			r.instance = 129;
			r.segment_id = 32769;
			break;
		case 3:
			r.format_version++;
			break;
		case 4:
			r.slot_offset = TT_SLOTS_PER_SEGMENT;
			break;
		case 5:
			r.reserved32 = 1;
			break;
		case 6:
			native_cut.scan_end_exclusive--;
			break;
		case 7:
			native_source.claim.database_incarnation++;
			break;
		case 8:
			native_reader.record->main_data_len--;
			break;
		case 9:
			native_reader.record->header.xl_info = 0xf0;
			break;
		case 10:
			UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
			break;
		case 11:
			r.wrap = TT_WRAP_INVALID;
			break;
		}
		memcpy(wal_payload.bytes, &r, sizeof(r));
		UT_ASSERT(!collect(roots, &diag));
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		UT_ASSERT(finish_roots(roots, NULL) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		cluster_recovery_root_set_destroy(roots);
	}
	remove_root(fd);
}

UT_TEST(test_undo_targets_reject_block_and_namespace_boundaries)
{
	int fd = new_root();
	for (unsigned variant = 0; variant < 9; variant++) {
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryWalTargetsDiag diag;
		xl_undo_block_write r = { 0 };
		uint8 digest[32];
		undo_wal_fixture(7, 257);
		memcpy(&r, wal_payload.bytes, sizeof(r));
		switch (variant) {
		case 0:
			r.block_no = 0;
			break;
		case 1:
			r.block_no = UNDO_BLOCKS_PER_SEGMENT;
			break;
		case 2:
			r.block_no = UINT32_MAX;
			break;
		case 3:
			r.has_fpi = 2;
			break;
		case 4:
			native_reader.record->main_data_len--;
			break;
		case 5:
			native_source.claim.identity.storage_uuid[0]++;
			break;
		case 6:
			native_reader.record->max_block_id = 0;
			native_reader.record->blocks[0].in_use = true;
			break;
		case 7:
			r.rec_off = 1;
			break;
		case 8:
			r.segment_id = 0;
			break;
		}
		memcpy(wal_payload.bytes, &r, sizeof(r));
		UT_ASSERT(!collect(roots, &diag));
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		UT_ASSERT(finish_roots(roots, NULL) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		cluster_recovery_root_set_destroy(roots);
	}
	/* The path index has its own exact namespace/range and late-add gates. */
	for (unsigned variant = 0; variant < 10; variant++) {
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		uint8 uuid[16];
		uint64 sysid = 817, incarnation = 91, required = 1;
		uint32 segment = 257;
		uint8 owner = 2;
		memcpy(uuid, root_uuid, 16);
		switch (variant) {
		case 0:
			sysid++;
			break;
		case 1:
			incarnation++;
			break;
		case 2:
			uuid[0]++;
			break;
		case 3:
			owner = 0;
			break;
		case 4:
			owner = 1;
			break;
		case 5:
			segment = 32769;
			owner = 129;
			break;
		case 6:
			required = 0;
			break;
		case 7:
			required = UNDO_BLOCKS_PER_SEGMENT + 1;
			break;
		case 8:
			required = UINT64_MAX;
			break;
		case 9:
			UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
			break;
		}
		UT_ASSERT_EQ(cluster_recovery_root_set_add_undo(roots, sysid, incarnation, uuid, owner,
														segment, required),
					 CLUSTER_RECOVERY_ROOTS_INVALID);
		UT_ASSERT(finish_roots(roots, NULL) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		cluster_recovery_root_set_destroy(roots);
	}
	remove_root(fd);
}

UT_TEST(test_folded_tt_commit_rejects_wrong_identity_and_shape)
{
	int fd = new_root();
	for (unsigned variant = 0; variant < 13; variant++) {
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryWalTargetsDiag diag;
		xl_xact_tt_commit tt = { 0 };
		tt.segment_id = 257;
		tt.instance = 2;
		tt.wrap = 1;
		tt.xid = 4195136;
		tt.format_version = CLUSTER_XACT_TT_COMMIT_VERSION;
		tt.commit_scn = 42;
		switch (variant) {
		case 0:
			tt.segment_id = 0;
			break;
		case 1:
			tt.instance = 1;
			break;
		case 2:
			tt.segment_generation = UINT32_MAX;
			break;
		case 3:
			tt.slot_offset = TT_SLOTS_PER_SEGMENT;
			break;
		case 4:
			tt.wrap = TT_WRAP_INVALID;
			break;
		case 5:
			tt.xid = InvalidTransactionId;
			break;
		case 6:
			tt.xid++;
			break;
		case 7:
			tt.format_version++;
			break;
		case 8:
			tt.flags = 1;
			break;
		case 9:
			tt.reserved = 1;
			break;
		case 10:
			tt.commit_scn++;
			break;
		case 11:
			tt.commit_scn = InvalidScn;
			break;
		case 12:
			tt.segment_id = 1;
			break;
		}
		folded_tt_fixture(&tt);
		UT_ASSERT(!collect(roots, &diag));
		UT_ASSERT(finish_roots(roots, NULL) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		cluster_recovery_root_set_destroy(roots);
	}
	remove_root(fd);
}
