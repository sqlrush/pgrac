/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_pending_product.c
 *    Backend-local reservation fixture for the real undo pending consumer.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_undo_pending_product.c
 *
 * NOTES
 *    This is a pgrac-original test file. Only the already-prepared backend
 *    reservation is seeded here; extent matching, receipt synchronization
 *    and pending preparation execute the unchanged product implementation.
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#ifndef UNDO_PENDING_SOURCE_PATH
#define UNDO_PENDING_SOURCE_PATH "../../backend/cluster/cluster_undo_record.c"
#endif

/* The runtime fixture supplies its own undo-read boundary for other tests. */
#define cluster_undo_get_record undo_pending_unused_get_record
#define cluster_undo_tt_retention_rollover_count undo_pending_unused_rollover_count
#include UNDO_PENDING_SOURCE_PATH
#undef cluster_undo_get_record
#undef cluster_undo_tt_retention_rollover_count

void undo_pending_test_reserve(ClusterUndoRecordPrepareReceipt *receipt, uint16 segment,
							   uint16 offset, uint8 pending_mask);
bool undo_pending_test_receipt_matches(const ClusterUndoRecordPrepareReceipt *receipt);

void
undo_pending_test_reserve(ClusterUndoRecordPrepareReceipt *receipt, uint16 segment, uint16 offset,
						  uint8 pending_mask)
{
	memset(&cluster_undo_record_reservation, 0, sizeof(cluster_undo_record_reservation));
	memset(&cluster_undo_current_extent, 0, sizeof(cluster_undo_current_extent));
	cluster_undo_current_extent.segment_id = 1;
	cluster_undo_current_extent.first_block = 1;
	cluster_undo_current_extent.nblocks = 1;
	cluster_undo_current_extent.cur_block = 1;
	cluster_undo_current_extent.cur_free_offset = sizeof(UndoBlockHeader);
	memset(receipt, 0, sizeof(*receipt));
	receipt->magic = CLUSTER_UNDO_RECORD_RECEIPT_MAGIC;
	receipt->record_type = UNDO_RECORD_UPDATE;
	receipt->owner_instance = 1;
	receipt->payload_capacity = 128;
	receipt->tt_slot_segment_id = segment;
	receipt->tt_slot_offset = offset;
	/* Explicit synthetic top capture for the caller's backend-local boundary.
	 * The real pending consumer now checks this immutable receipt identity. */
	receipt->operation.top
		= (ClusterCanonicalTxnBinding){ .segment_id = segment,
										.segment_generation = 23,
										.xid = 797,
										.slot_offset = offset,
										.slot_wrap = 7,
										.origin_instance = 1,
										.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED,
										.active_lsn = 1 };
	receipt->operation.top_fxid = FullTransactionIdFromU64(797);
	receipt->operation.operation_fxid = receipt->operation.top_fxid;
	receipt->operation.operation_sequence = 1;
	receipt->operation.subid = TopSubTransactionId;
	receipt->actual_segment_id = 1;
	receipt->reservation_sequence = 17;
	receipt->absolute_deadline_us = 1000;
	receipt->ctrc_attempt_generation = 3;
	receipt->extent = cluster_undo_current_extent;
	receipt->ctrc_pending_mask = pending_mask;
	receipt->ctrc_pending_targets[0].block_number = 37;
	receipt->ctrc_pending_targets[1].block_number = 38;
	cluster_undo_record_reservation.active = true;
	cluster_undo_record_reservation.sequence = receipt->reservation_sequence;
	cluster_undo_record_reservation.receipt = *receipt;
}

bool
undo_pending_test_receipt_matches(const ClusterUndoRecordPrepareReceipt *receipt)
{
	return cluster_undo_record_receipt_extent_matches(receipt);
}
