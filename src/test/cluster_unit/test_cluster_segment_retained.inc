/*-------------------------------------------------------------------------
 * test_cluster_segment_retained.inc
 *      Retained undo anchors constrain whole-segment capacity in both modes.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *      src/test/cluster_unit/test_cluster_segment_retained.inc
 *-------------------------------------------------------------------------
 */
static void
retained_segment_seed(UndoSegmentHeaderData *header, uint8 status, unsigned index)
{
	TTSlot *slot;

	init_header(header, SEGMENT_COMMITTED);
	slot = &header->tt_slots[index];
	slot->status = status;
	if (status == TT_SLOT_COMMITTED || status == TT_SLOT_ABORTED) {
		slot->xid = 100;
		slot->wrap = TT_WRAP_INITIAL;
		slot->flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		slot->commit_scn = status == TT_SLOT_COMMITTED ? mk_scn(7) : InvalidScn;
	}
}

UT_TEST(test_segment_every_anchored_slot_retains_in_both_modes)
{
	const uint8 statuses[]
		= { TT_SLOT_UNUSED, TT_SLOT_RECYCLABLE, TT_SLOT_COMMITTED, TT_SLOT_ABORTED };
	UndoSegmentHeaderData header;
	unsigned status, index;
	int peer;

	for (status = 0; status < lengthof(statuses); status++) {
		for (index = 0; index < TT_SLOTS_PER_SEGMENT; index++) {
			retained_segment_seed(&header, statuses[status], index);
			for (peer = 0; peer < 2; peer++)
				UT_ASSERT(cluster_undo_segment_recyclable_for_mode(&header, mk_scn(10), peer != 0));
			header.tt_slots[index].first_undo_block = uba_encode(2, 9, index, 3);
			for (peer = 0; peer < 2; peer++)
				UT_ASSERT(
					!cluster_undo_segment_recyclable_for_mode(&header, mk_scn(10), peer != 0));
			UT_ASSERT(!cluster_undo_segment_recyclable(&header, mk_scn(10)));
		}
	}
}

UT_TEST(test_segment_partial_anchor_and_unknown_slot_count_retain)
{
	const uint16 counts[] = { 0, TT_SLOTS_PER_SEGMENT - 1, TT_SLOTS_PER_SEGMENT + 1, UINT16_MAX };
	UndoSegmentHeaderData header;
	unsigned byte, count;
	int peer;

	for (byte = 0; byte < sizeof(UBA); byte++) {
		retained_segment_seed(&header, TT_SLOT_ABORTED, TT_SLOTS_PER_SEGMENT - 1);
		((uint8 *)&header.tt_slots[TT_SLOTS_PER_SEGMENT - 1].first_undo_block)[byte] = 1;
		for (peer = 0; peer < 2; peer++)
			UT_ASSERT(!cluster_undo_segment_recyclable_for_mode(&header, mk_scn(10), peer != 0));
	}
	for (count = 0; count < lengthof(counts); count++) {
		retained_segment_seed(&header, TT_SLOT_ABORTED, 0);
		header.tt_slots_count = counts[count];
		for (peer = 0; peer < 2; peer++)
			UT_ASSERT(!cluster_undo_segment_recyclable_for_mode(&header, mk_scn(10), peer != 0));
	}
}

UT_TEST(test_segment_anchor_is_independent_of_release_and_record_drain)
{
	UndoSegmentHeaderData header, unchanged;
	int peer;

	retained_segment_seed(&header, TT_SLOT_COMMITTED, TT_SLOTS_PER_SEGMENT - 1);
	header.tail_block = 1;
	UndoSegmentHeader_set_record_seal_upper_scn(&header, mk_scn(5));
	UndoSegmentHeader_set_record_drain_upper_scn(&header, mk_scn(6));
	header.tt_slots[TT_SLOTS_PER_SEGMENT - 1].first_undo_block = uba_encode(2, 9, 47, 3);
	unchanged = header;
	for (peer = 0; peer < 2; peer++) {
		UT_ASSERT(!cluster_undo_segment_recyclable_for_mode(&header, mk_scn(10), peer != 0));
		UT_ASSERT_EQ(memcmp(&header, &unchanged, sizeof(header)), 0);
	}
	/* Model the original retirement owner's completed head removal. Empty
	 * shape still cannot substitute for its independent horizon/ref proofs. */
	header.tt_slots[TT_SLOTS_PER_SEGMENT - 1].first_undo_block = (UBA)InvalidUba_init;
	for (peer = 0; peer < 2; peer++)
		UT_ASSERT(cluster_undo_segment_recyclable_for_mode(&header, mk_scn(10), peer != 0));
	UT_ASSERT(!cluster_undo_segment_recyclable_for_mode(&header, mk_scn(7), false));
	UT_ASSERT(cluster_undo_segment_recyclable_for_mode(&header, mk_scn(7), true));
	header.tt_slots[TT_SLOTS_PER_SEGMENT - 1].flags = TT_FLAGS_RESERVED;
	UT_ASSERT(!cluster_undo_segment_recyclable_for_mode(&header, mk_scn(10), true));
	UndoSegmentHeader_set_record_drain_upper_scn(&header, mk_scn(10));
	UT_ASSERT(!cluster_undo_segment_recyclable_for_mode(&header, mk_scn(10), false));
}
