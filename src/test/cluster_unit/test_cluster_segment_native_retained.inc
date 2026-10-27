/*-------------------------------------------------------------------------
 * test_cluster_segment_native_retained.inc
 *      Generation-qualified segment replay cannot destroy retained heads.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *      src/test/cluster_unit/test_cluster_segment_native_retained.inc
 * NOTES
 *      The enclosing fixture models physical file I/O, not a running server.
 *-------------------------------------------------------------------------
 */
static ClusterUndoDecoded
retained_segment_recycle_decoded(void)
{
	ClusterUndoDecoded decoded = { 0 };

	decoded.kind = CLUSTER_UNDO_KIND_SEGMENT_RECYCLE;
	decoded.opcode = XLOG_UNDO_SEGMENT_RECYCLE;
	decoded.instance = 3;
	decoded.segment_id = 513;
	decoded.expected_generation = 8;
	decoded.old_state = SEGMENT_COMMITTED;
	decoded.new_state = SEGMENT_RECYCLABLE;
	return decoded;
}

static void
retained_segment_native_seed(uint32 generation, uint8 state)
{
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)disk.data;
	TTSlot *slot = &header->tt_slots[TT_SLOTS_PER_SEGMENT - 1];

	cluster_undo_segment_make_header_bytes(513, 3, disk.data);
	header->wrap_count = generation;
	header->segment_state = state;
	slot->xid = 802;
	slot->wrap = 7;
	slot->status = TT_SLOT_ABORTED;
	slot->flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	slot->first_undo_block = uba_encode(514, 9, TT_SLOTS_PER_SEGMENT - 1, 2);
	UT_ASSERT(cluster_undo_segment_header_seal(disk.data));
	reset_io();
}

UT_TEST(test_private_recycle_retains_head_in_every_same_generation_state)
{
	ClusterUndoDecoded decoded = retained_segment_recycle_decoded();
	PGAlignedBlock unchanged, out, before;
	unsigned state;

	memset(unchanged.data, 0x7d, BLCKSZ);
	for (state = SEGMENT_ALLOCATED; state <= SEGMENT_RECYCLABLE; state++) {
		retained_segment_native_seed(8, state);
		before = disk;
		out = unchanged;
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, NULL, 0, disk.data, out.data),
					 CLUSTER_UNDO_HEADER_BLOCKED);
		UT_ASSERT_EQ(memcmp(out.data, unchanged.data, BLCKSZ), 0);
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, NULL, 0, disk.data, disk.data),
					 CLUSTER_UNDO_HEADER_BLOCKED);
		UT_ASSERT_EQ(memcmp(disk.data, before.data, BLCKSZ), 0);
		UT_ASSERT_EQ(opens + writes + syncs + applies, 0);
	}
}

static ClusterUndoDecoded
retained_segment_reuse_decoded(PGAlignedBlock *image)
{
	ClusterUndoDecoded decoded = retained_segment_recycle_decoded();

	decoded.kind = CLUSTER_UNDO_KIND_SEGMENT_REUSE;
	decoded.opcode = XLOG_UNDO_SEGMENT_REUSE;
	decoded.new_generation = 9;
	decoded.has_payload = decoded.has_fpi = true;
	decoded.payload_length = BLCKSZ;
	decoded.old_state = decoded.new_state = 0;
	cluster_undo_segment_make_header_bytes(513, 3, image->data);
	((UndoSegmentHeaderData *)image->data)->wrap_count = 9;
	return decoded;
}

UT_TEST(test_private_reuse_retains_old_generation_undo_head)
{
	PGAlignedBlock image, before, out, unchanged;
	ClusterUndoDecoded decoded = retained_segment_reuse_decoded(&image);

	retained_segment_native_seed(8, SEGMENT_RECYCLABLE);
	before = disk;
	memset(unchanged.data, 0x7d, BLCKSZ);
	out = unchanged;
	UT_ASSERT_EQ(
		cluster_undo_prepare_header_v1(&decoded, (uint8 *)image.data, BLCKSZ, disk.data, out.data),
		CLUSTER_UNDO_HEADER_BLOCKED);
	UT_ASSERT_EQ(memcmp(out.data, unchanged.data, BLCKSZ), 0);
	UT_ASSERT_EQ(
		cluster_undo_prepare_header_v1(&decoded, (uint8 *)image.data, BLCKSZ, disk.data, disk.data),
		CLUSTER_UNDO_HEADER_BLOCKED);
	UT_ASSERT_EQ(memcmp(disk.data, before.data, BLCKSZ), 0);
}

UT_TEST(test_private_fresh_segment_image_cannot_publish_an_undo_head)
{
	PGAlignedBlock image, out, unchanged;
	ClusterUndoDecoded decoded = retained_segment_reuse_decoded(&image);
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)image.data;
	unsigned init;

	memset(unchanged.data, 0x7d, BLCKSZ);
	for (init = 0; init < 2; init++) {
		retained_segment_native_seed(8, SEGMENT_RECYCLABLE);
		((UndoSegmentHeaderData *)disk.data)->tt_slots[TT_SLOTS_PER_SEGMENT - 1].first_undo_block
			= (UBA)InvalidUba_init;
		if (init) {
			decoded.kind = CLUSTER_UNDO_KIND_SEGMENT_INIT;
			decoded.opcode = XLOG_UNDO_SEGMENT_INIT;
			header->wrap_count = 0;
		}
		header->tt_slots[TT_SLOTS_PER_SEGMENT - 1].first_undo_block.raw[1] = 1;
		out = unchanged;
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, (uint8 *)image.data, BLCKSZ,
													init ? NULL : disk.data, out.data),
					 CLUSTER_UNDO_HEADER_BLOCKED);
		UT_ASSERT_EQ(memcmp(out.data, unchanged.data, BLCKSZ), 0);
	}
}

static bool
retained_segment_native_apply(uint8 opcode, void *record, Size length)
{
	bool success = false;

	expect_panic = true;
	if (setjmp(panic_jump) == 0) {
		apply_record(opcode, record, length);
		success = true;
	}
	expect_panic = false;
	return success;
}

UT_TEST(test_native_segment_replay_rejects_anchored_generation_before_write)
{
	xl_undo_segment_recycle recycle = { 0 };
	struct {
		xl_undo_segment_reuse record;
		char image[BLCKSZ];
	} reuse = { 0 };
	PGAlignedBlock before, image;
	unsigned kind;

	recycle.instance = reuse.record.instance = 3;
	recycle.segment_id = reuse.record.segment_id = 513;
	recycle.expected_generation = reuse.record.old_generation = 8;
	recycle.old_state = SEGMENT_COMMITTED;
	recycle.new_state = SEGMENT_RECYCLABLE;
	reuse.record.new_generation = 9;
	(void)retained_segment_reuse_decoded(&image);
	memcpy(reuse.image, image.data, BLCKSZ);
	for (kind = 0; kind < 2; kind++) {
		retained_segment_native_seed(8, kind ? SEGMENT_RECYCLABLE : SEGMENT_COMMITTED);
		before = disk;
		UT_ASSERT(!retained_segment_native_apply(
			kind ? XLOG_UNDO_SEGMENT_REUSE : XLOG_UNDO_SEGMENT_RECYCLE,
			kind ? (void *)&reuse : (void *)&recycle, kind ? sizeof(reuse) : sizeof(recycle)));
		UT_ASSERT_EQ(writes + syncs + applies + skips + notes, 0);
		UT_ASSERT_EQ(memcmp(disk.data, before.data, BLCKSZ), 0);
		UT_ASSERT_EQ(header_locks, header_unlocks);
		UT_ASSERT_EQ(opens, closes);
	}
}

UT_TEST(test_segment_generation_order_precedes_retained_head_shape)
{
	PGAlignedBlock image, before, out;
	ClusterUndoDecoded decoded = retained_segment_reuse_decoded(&image);

	retained_segment_native_seed(10, SEGMENT_ACTIVE);
	before = disk;
	out = before;
	UT_ASSERT_EQ(
		cluster_undo_prepare_header_v1(&decoded, (uint8 *)image.data, BLCKSZ, disk.data, out.data),
		CLUSTER_UNDO_HEADER_SKIP_STALE);
	UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
	decoded = retained_segment_recycle_decoded();
	UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, NULL, 0, disk.data, out.data),
				 CLUSTER_UNDO_HEADER_SKIP_STALE);
	UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
	/* Same-new-generation replay retains its original image replay semantics;
	 * later WAL will restore later operations in that generation. */
	decoded = retained_segment_reuse_decoded(&image);
	retained_segment_native_seed(9, SEGMENT_ACTIVE);
	UT_ASSERT_EQ(
		cluster_undo_prepare_header_v1(&decoded, (uint8 *)image.data, BLCKSZ, disk.data, out.data),
		CLUSTER_UNDO_HEADER_APPLY);
	UT_ASSERT_EQ(memcmp(out.data, image.data, BLCKSZ), 0);
}
