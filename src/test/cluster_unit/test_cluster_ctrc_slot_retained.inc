/*-------------------------------------------------------------------------
 * test_cluster_ctrc_slot_retained.inc
 *      Original canonical sampler keeps retained heads after release proof.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *      src/test/cluster_unit/test_cluster_ctrc_slot_retained.inc
 *-------------------------------------------------------------------------
 */
UT_TEST(test_solo_actual_release_sampler_retains_every_undo_head_byte)
{
	unsigned aborted, byte;

	test_cluster_epoch = 23;
	for (aborted = 0; aborted < 2; aborted++) {
		ClusterCtrcTxnKeyV1 key = reset_capacity_sample();
		ClusterCtrcOriginCertificateSnapshot snapshot;
		TTSlot *slot = &sample_header.tt_slots[0];

		cluster_shared_config = true;
		sample_node_count = 1;
		sample_commit = aborted == 0;
		sample_abort = aborted != 0;
		slot->status = aborted ? TT_SLOT_ABORTED : TT_SLOT_COMMITTED;
		slot->commit_scn = aborted ? InvalidScn : 1000;
		slot->flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		UT_ASSERT(ctrc_origin_certificate_snapshot_index_locked(0, &snapshot));
		UT_ASSERT(ctrc_origin_certificate_commit_shared(&snapshot));
		for (byte = 0; byte < sizeof(UBA); byte++) {
			UndoSegmentHeaderData before;

			((uint8 *)&slot->first_undo_block)[byte] = 1;
			before = sample_header;
			UT_ASSERT(!cluster_ctrc_terminal_release_sample_exact(
				1, 0, key.xid, key.slot_wrap, slot->status, slot->commit_scn, 23));
			UT_ASSERT_EQ(memcmp(&before, &sample_header, sizeof(before)), 0);
			((uint8 *)&slot->first_undo_block)[byte] = 0;
		}
		/* Nil-head input models the original retirement owner's result.
		 * This fixture supplies no production head-clearing implementation. */
		UT_ASSERT(cluster_ctrc_terminal_release_sample_exact(1, 0, key.xid, key.slot_wrap,
															 slot->status, slot->commit_scn, 23));
		sample_active = true;
		UT_ASSERT(!cluster_ctrc_terminal_release_sample_exact(1, 0, key.xid, key.slot_wrap,
															  slot->status, slot->commit_scn, 23));
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
	}
	test_cluster_epoch = 13;
}
