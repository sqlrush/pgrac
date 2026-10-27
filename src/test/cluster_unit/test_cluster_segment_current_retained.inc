/*-------------------------------------------------------------------------
 * test_cluster_segment_current_retained.inc
 *      Actual current-owner recycle/reuse must preserve anchored segments.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *      src/test/cluster_unit/test_cluster_segment_current_retained.inc
 *-------------------------------------------------------------------------
 */
static void
retained_current_seed(unsigned peers, uint8 state, uint8 status)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)fake_disk_page;
	TTSlot *slot = &disk->tt_slots[TT_SLOTS_PER_SEGMENT - 1];

	reset_fixture();
	test_cluster_conf.node_count = peers;
	UT_ASSERT_EQ(cluster_peer_mode_enabled(), peers > 1);
	fake_modifier_side = CLUSTER_SEMANTIC_TARGET_SIDE;
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_sample_generation = (ClusterUndoBlock0Generation){ true, 7 };
	init_recyclable_block0(fake_disk_page, key.segment_id, key.owner_instance, 7);
	disk->segment_state = state;
	slot->status = status;
	slot->xid = 700;
	slot->wrap = 3;
	slot->commit_scn = status == TT_SLOT_COMMITTED ? 70 : InvalidScn;
	slot->flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	slot->first_undo_block = uba_encode(2, 9, TT_SLOTS_PER_SEGMENT - 1, 3);
	memcpy(fake_pin_page, fake_disk_page, BLCKSZ);
}

static void
retained_current_no_mutation(const PGAlignedBlock *before)
{
	UT_ASSERT_EQ(recycle_wal_calls + reuse_wal_calls + flush_sync_calls, 0);
	UT_ASSERT_EQ(memcmp(fake_disk_page, before->data, BLCKSZ), 0);
	UT_ASSERT_EQ(memcmp(fake_pin_page, before->data, BLCKSZ), 0);
	UT_ASSERT_EQ(pin_calls, unpin_calls);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
	UT_ASSERT_EQ(local_release_calls, 1);
}

UT_TEST(test_live_recycle_and_already_retain_undo_head_in_both_modes)
{
	const uint8 states[] = { SEGMENT_COMMITTED, SEGMENT_RECYCLABLE };
	const uint8 statuses[] = { TT_SLOT_COMMITTED, TT_SLOT_ABORTED };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	PGAlignedBlock before;
	unsigned peers, state, status;

	for (peers = 1; peers <= 2; peers++) {
		for (state = 0; state < lengthof(states); state++) {
			for (status = 0; status < lengthof(statuses); status++) {
				retained_current_seed(peers, states[state], statuses[status]);
				memcpy(before.data, fake_disk_page, BLCKSZ);
				UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_recycle_exact(&key, 100,
																				  fake_epoch, 1000),
							 CLUSTER_UNDO_BLOCK0_RECYCLE_RETAINED);
				retained_current_no_mutation(&before);
			}
		}
	}
}

UT_TEST(test_live_direct_reuse_rejects_retained_head_before_generation_wal)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Generation expected = { true, 7 };
	PGAlignedBlock before, successor;
	unsigned peers;

	for (peers = 1; peers <= 2; peers++) {
		retained_current_seed(peers, SEGMENT_RECYCLABLE, TT_SLOT_ABORTED);
		memcpy(before.data, fake_disk_page, BLCKSZ);
		init_fresh_successor_block0(successor.data, key.segment_id, key.owner_instance, 8);
		UT_ASSERT_NE(cluster_undo_block0_current_live_owner_reuse_exact(&key, &expected,
																		successor.data, 1000),
					 CLUSTER_UNDO_BLOCK0_OK);
		retained_current_no_mutation(&before);
	}
}

UT_TEST(test_live_segment_capacity_reopens_only_after_original_head_retirement)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Generation expected = { true, 7 };
	PGAlignedBlock successor;
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)fake_disk_page;
	unsigned peers;

	for (peers = 1; peers <= 2; peers++) {
		retained_current_seed(peers, SEGMENT_COMMITTED, TT_SLOT_COMMITTED);
		/* Input models completed original-owner retirement; it does not
		 * create a production fold/ref proof or a new head-clearing API. */
		disk->tt_slots[TT_SLOTS_PER_SEGMENT - 1].first_undo_block = (UBA)InvalidUba_init;
		memcpy(fake_pin_page, fake_disk_page, BLCKSZ);
		UT_ASSERT_EQ(
			cluster_undo_block0_current_live_owner_recycle_exact(&key, 100, fake_epoch, 1000),
			CLUSTER_UNDO_BLOCK0_RECYCLE_ADVANCED);
		if (recycle_wal_calls != 1) {
			UT_ASSERT(false);
			return;
		}
		init_fresh_successor_block0(successor.data, key.segment_id, key.owner_instance, 8);
		UT_ASSERT_EQ(cluster_undo_block0_current_live_owner_reuse_exact(&key, &expected,
																		successor.data, 1000),
					 CLUSTER_UNDO_BLOCK0_OK);
		UT_ASSERT_EQ(reuse_wal_calls, 1);
		UT_ASSERT_EQ(flush_sync_calls, 2);
		UT_ASSERT_EQ(memcmp(fake_pin_page, successor.data, BLCKSZ), 0);
	}
}
