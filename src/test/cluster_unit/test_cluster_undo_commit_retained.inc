/*-------------------------------------------------------------------------
 * test_cluster_undo_commit_retained.inc
 *      Generation-qualified commit projection and native redo retain undo.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *      src/test/cluster_unit/test_cluster_undo_commit_retained.inc
 * NOTES
 *      Native entry points execute against the enclosing physical I/O model.
 *-------------------------------------------------------------------------
 */
static TTSlot
native_retained_commit_seed(void)
{
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)disk.data;
	TTSlot *slot = &header->tt_slots[4];

	cluster_undo_segment_make_header_bytes(513, 3, disk.data);
	header->wrap_count = 8;
	slot->xid = 802;
	slot->wrap = 7;
	slot->status = TT_SLOT_ACTIVE;
	slot->first_undo_block = uba_encode(514, 9, 4, 2);
	UT_ASSERT(cluster_undo_segment_header_seal(disk.data));
	reset_io();
	return *slot;
}

static bool
native_retained_commit_redo(void)
{
	bool success = false;

	expect_panic = true;
	if (setjmp(panic_jump) == 0) {
		cluster_tt_durable_redo_stamp_slot_exact(3, 513, 8, 4, 7, 802, 999);
		success = true;
	}
	expect_panic = false;
	return success;
}

UT_TEST(test_private_exact_commit_preserves_head_and_unrelated_bytes)
{
	for (unsigned alias = 0; alias < 2; alias++) {
		TTSlot expected = native_retained_commit_seed();
		PGAlignedBlock before = disk;
		PGAlignedBlock out;
		char *result = alias ? disk.data : out.data;
		ClusterUndoHeaderPrepareResultV1 decision;

		expected.status = TT_SLOT_COMMITTED;
		expected.commit_scn = 999;
		decision = cluster_undo_prepare_commit_v1(3, 513, 8, 4, 7, 802, 999, disk.data, result);

		UT_ASSERT_EQ(decision, CLUSTER_UNDO_HEADER_APPLY);
		if (decision != CLUSTER_UNDO_HEADER_APPLY)
			return;
		UT_ASSERT_EQ(
			memcmp(&((UndoSegmentHeaderData *)result)->tt_slots[4], &expected, sizeof(expected)),
			0);
		((UndoSegmentHeaderData *)result)->tt_slots[4]
			= ((UndoSegmentHeaderData *)before.data)->tt_slots[4];
		UT_ASSERT_EQ(memcmp(result, before.data, BLCKSZ), 0);
		UT_ASSERT_EQ(opens + writes + syncs + header_locks, 0);
	}
}

UT_TEST(test_native_exact_commit_retains_head_across_repeat_and_release_marker)
{
	TTSlot expected = native_retained_commit_seed();
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)disk.data;

	expected.status = TT_SLOT_COMMITTED;
	expected.commit_scn = 999;
	if (!native_retained_commit_redo()) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT_EQ(memcmp(&header->tt_slots[4], &expected, sizeof(expected)), 0);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(applies, 1);
	UT_ASSERT_EQ(cluster_undo_segment_header_integrity(disk.data), CLUSTER_UNDO_HEADER_VALID);
	UT_ASSERT_EQ(memcmp(disk.data, durable.data, BLCKSZ), 0);
	reset_io();
	if (!native_retained_commit_redo()) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT_EQ(writes + applies + skips, 0);
	UT_ASSERT_EQ(notes, 1);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(memcmp(&header->tt_slots[4], &expected, sizeof(expected)), 0);
	header->tt_slots[4].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	UT_ASSERT(cluster_undo_segment_header_seal(disk.data));
	reset_io();
	if (!native_retained_commit_redo()) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT_EQ(memcmp(&header->tt_slots[4], &expected, sizeof(expected)), 0);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(header_locks, header_unlocks);
	fixture_power_loss();
	UT_ASSERT_EQ(memcmp(&header->tt_slots[4], &expected, sizeof(expected)), 0);
}

static void
native_retained_commit_fault(unsigned fault)
{
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)disk.data;
	TTSlot *slot = &header->tt_slots[4];

	switch (fault) {
	case 0:
		slot->xid++;
		break;
	case 1:
		slot->wrap--;
		break;
	case 2:
		header->wrap_count--;
		break;
	case 3:
		slot->flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		break;
	case 4:
		slot->flags = 0x80;
		break;
	case 5:
		slot->commit_scn = 999;
		break;
	case 6:
		slot->status = TT_SLOT_ABORTED;
		break;
	case 7:
		slot->status = TT_SLOT_COMMITTED;
		slot->commit_scn = 1000;
		break;
	case 8:
		slot->first_undo_block = uba_encode(1, 9, 4, 2);
		break;
	case 9:
		slot->first_undo_block = uba_encode(514, 0, 4, 2);
		break;
	case 10:
		slot->first_undo_block = uba_encode(514, 9, 5, 2);
		break;
	case 11:
		slot->first_undo_block.raw[1] |= UINT64_C(1) << 32;
		break;
	}
	UT_ASSERT(cluster_undo_segment_header_seal(disk.data));
}

UT_TEST(test_exact_commit_projection_and_redo_reject_conflicts_without_mutation)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		PGAlignedBlock before, out, untouched;

		(void)native_retained_commit_seed();
		native_retained_commit_fault(fault);
		before = disk;
		memset(untouched.data, 0x62, BLCKSZ);
		out = untouched;
		UT_ASSERT_EQ(cluster_undo_prepare_commit_v1(3, 513, 8, 4, 7, 802, 999, disk.data, out.data),
					 CLUSTER_UNDO_HEADER_BLOCKED);
		UT_ASSERT_EQ(memcmp(out.data, untouched.data, BLCKSZ), 0);
		expect_panic = true;
		if (setjmp(panic_jump) == 0) {
			cluster_tt_durable_redo_stamp_slot_exact(3, 513, 8, 4, 7, 802, 999);
			UT_ASSERT(false);
		}
		expect_panic = false;
		UT_ASSERT_EQ(writes + applies + syncs, 0);
		UT_ASSERT_EQ(memcmp(disk.data, before.data, BLCKSZ), 0);
		UT_ASSERT_EQ(header_locks, header_unlocks);
		UT_ASSERT(!header_locked);
	}
}

UT_TEST(test_exact_commit_retained_stale_and_present_durability_failure)
{
	TTSlot before = native_retained_commit_seed();
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)disk.data;

	header->wrap_count++;
	UT_ASSERT(cluster_undo_segment_header_seal(disk.data));
	cluster_tt_durable_redo_stamp_slot_exact(3, 513, 8, 4, 7, 802, 999);
	UT_ASSERT_EQ(skips, 1);
	UT_ASSERT_EQ(notes, 1);
	UT_ASSERT_EQ(writes + applies, 0);
	UT_ASSERT_EQ(memcmp(&header->tt_slots[4], &before, sizeof(before)), 0);
	(void)native_retained_commit_seed();
	header->tt_slots[4].status = TT_SLOT_COMMITTED;
	header->tt_slots[4].commit_scn = 999;
	UT_ASSERT(cluster_undo_segment_header_seal(disk.data));
	expect_panic = fail_sync = true;
	if (setjmp(panic_jump) == 0) {
		cluster_tt_durable_redo_stamp_slot_exact(3, 513, 8, 4, 7, 802, 999);
		UT_ASSERT(false);
	}
	expect_panic = fail_sync = false;
	UT_ASSERT_EQ(writes + applies, 0);
	UT_ASSERT_EQ(notes, 1);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(header_locks, header_unlocks);
}
