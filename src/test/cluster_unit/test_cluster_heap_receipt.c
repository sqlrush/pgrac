/*-------------------------------------------------------------------------
 *
 * test_cluster_heap_receipt.c
 *    Real heap target and undo lifetime tests across terminal cleanout.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_heap_receipt.c
 *
 * NOTES
 *    This is a pgrac-original file. Reuse the undo test's runtime fixtures;
 *    real heap code is linked without USE_CLUSTER_UNIT. Shared PREPARE is
 *    the existing undo fixture, not a live shared-table or WAL replay test.
 *    The actual CTRC finalization engine is tested in its own unit target.
 *-------------------------------------------------------------------------
 */
#define cluster_ctrc_receipt_prepare_shared heap_receipt_fixture_prepare_shared
#define cluster_ctrc_receipt_apply_shared heap_receipt_fixture_apply_shared
#define cluster_ctrc_receipt_retarget_itl_shared heap_receipt_fixture_retarget_shared
#define cluster_ctrc_receipt_cancel_shared heap_receipt_fixture_cancel_shared
int original_undo_fixture_main(int argc, char **argv);
#define main original_undo_fixture_main
#include "test_cluster_undo_record.c"
#undef main
#undef cluster_ctrc_receipt_prepare_shared
#undef cluster_ctrc_receipt_apply_shared
#undef cluster_ctrc_receipt_retarget_itl_shared
#undef cluster_ctrc_receipt_cancel_shared

#include "access/htup_details.h"

extern bool heap_receipt_test_capture(Page page, bool first, uint8 operation,
									  ClusterCtrcTargetV1 *target);
extern uint32 heap_receipt_test_authority_mismatch(void);
extern bool heap_receipt_test_final(ClusterUndoRecordPrepareReceipt *receipt,
									ClusterCtrcTargetV1 *target);
extern bool heap_receipt_test_plan_capture(ClusterUndoRecordPrepareReceipt *receipt);
extern bool heap_receipt_test_plan_recheck(ClusterUndoRecordPrepareReceipt *receipt);
extern int heap_receipt_test_error_detail(const ClusterUndoRecordPrepareReceipt *receipt,
										  const ClusterCtrcTargetV1 *observed);
extern void heap_receipt_test_current_handoff(void);
extern void heap_receipt_test_current_mode(uint8 state);
extern OffsetNumber heap_receipt_test_insert_offset(Page page, HeapTuple tuple);
extern bool heap_receipt_test_tuple_address(Page page, int leg);

static char heap_receipt_error_detail[4096];

void *
palloc(Size size)
{
	void *result = malloc(size);

	if (result == NULL)
		abort();
	return result;
}

void *
repalloc(void *pointer, Size size)
{
	void *result = realloc(pointer, size);

	if (result == NULL)
		abort();
	return result;
}

void
pfree(void *pointer)
{
	free(pointer);
}

int
errdetail_internal(const char *format, ...)
{
	va_list arguments;

	va_start(arguments, format);
	vsnprintf(heap_receipt_error_detail, sizeof(heap_receipt_error_detail), format, arguments);
	va_end(arguments);
	return 0;
}

int
errdetail(const char *format pg_attribute_unused(), ...)
{
	/* StringInfo's allocation-limit error is outside this bounded fixture. */
	abort();
}

static void
heap_receipt_fixture(Page page, ClusterUndoRecordPrepareReceipt *receipt,
					 ClusterCtrcTargetV1 *pending)
{
	HeapTupleHeaderData tuple = { 0 };

	receipt_fixture_ready(UNDO_RECORD_UPDATE, receipt);
	tuple.t_hoff = SizeofHeapTupleHeader;
	tuple.t_itl_slot_idx = CLUSTER_ITL_SLOT_UNALLOCATED;
	PageInitHeapPage(page, BLCKSZ, 0);
	UT_ASSERT_EQ(PageAddItem(page, (Item)&tuple, sizeof(tuple), FirstOffsetNumber, false, true),
				 FirstOffsetNumber);
	PageSetLSN(page, 100);
	((PageHeader)page)->pd_block_scn = 100;
	UT_ASSERT(heap_receipt_test_capture(page, true, receipt->record_type, pending));
	UT_ASSERT(cluster_undo_record_ctrc_stage_pending(receipt, 0, pending));
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(receipt, 0));
}

static void
heap_receipt_cleanout(Page page, const ClusterCtrcTargetV1 *pending)
{
	ClusterCtrcTxnKeyV1 key = { 0 };
	ClusterCtrcTargetV1 terminal_target = *pending;
	ClusterItlSlotData *slot = &ClusterPageGetItlSlots(page)[1];
	ClusterItlSlotData expected;

	key.xid = 701;
	terminal_target.kind = CTRC_TARGET_EXACT_ITL_SLOT;
	terminal_target.itl_slot_index = 1;
	terminal_target.itl_slot_wrap = 3;
	terminal_target.itl_xid = key.xid;
	terminal_target.itl_class = 1;
	memset(terminal_target.uba, 0x5a, sizeof(terminal_target.uba));
	memset(terminal_target.planned_predecessor_sha256, 0x11, 32);
	memset(terminal_target.planned_successor_sha256, 0x22, 32);
	slot->xid = key.xid;
	slot->wrap = 3;
	slot->flags = ITL_FLAG_NEEDS_CLEANOUT;
	slot->commit_scn = 90;
	memcpy(&slot->undo_segment_head, terminal_target.uba, sizeof(slot->undo_segment_head));
	expected = *slot;
	expected.flags = ITL_FLAG_COMMITTED;
	UT_ASSERT_EQ(
		cluster_ctrc_itl_cleanout_slot(&key, &terminal_target, CTRC_TERMINAL_COMMITTED, 90, slot),
		CLUSTER_CTRC_ITL_CLEANOUT_REWRITTEN);
	UT_ASSERT_EQ(memcmp(slot, &expected, sizeof(expected)), 0);
	/* Model publication coordinates; this does not execute Generic WAL redo. */
	PageSetLSN(page, 200);
	((PageHeader)page)->pd_block_scn = 200;
}

UT_TEST(cleanout_preserves_exact_unpublished_resource_after_prepare_deadline)
{
	PGAlignedBlock image;
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterUndoRecordPrepareReceipt saved;
	ClusterCtrcTargetV1 before, after;

	heap_receipt_fixture(image.data, &receipt, &before);
	saved = receipt;
	heap_receipt_cleanout(image.data, &before);
	UT_ASSERT_EQ(heap_receipt_test_authority_mismatch(), 0);
	UT_ASSERT(heap_receipt_test_capture(image.data, false, receipt.record_type, &after));
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) != 0);
	receipt_clock_us = 101;
	UT_ASSERT(cluster_undo_record_prepared_recheck(&receipt, 64));
	UT_ASSERT(cluster_undo_record_ctrc_pending_matches(&receipt, 0, &after));
	UT_ASSERT_EQ(cluster_undo_record_requalify_for_retry(&receipt, 64, false),
				 CLUSTER_UNDO_RECORD_PREPARE_READY);
	UT_ASSERT(cluster_undo_record_reservation.active);
	UT_ASSERT_EQ(memcmp(&receipt, &saved, sizeof(saved)), 0);
	UT_ASSERT_EQ(receipt_prepare_calls, 1);
	UT_ASSERT_EQ(receipt_cancel_calls, 0);
	cluster_undo_record_cancel_prepared(&receipt);
}

UT_TEST(logical_tid_cannot_authorize_another_tuple_address)
{
	PGAlignedBlock image;
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterCtrcTargetV1 pending;
	int leg;

	heap_receipt_fixture(image.data, &receipt, &pending);
	for (leg = 0; leg < 4; leg++)
		UT_ASSERT_EQ(heap_receipt_test_tuple_address(image.data, leg), leg == 0);
	cluster_undo_record_cancel_prepared(&receipt);
}

UT_TEST(final_itl_predecessor_is_captured_from_current_page)
{
	PGAlignedBlock image;
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterCtrcTargetV1 before, final_target;

	heap_receipt_fixture(image.data, &receipt, &before);
	heap_receipt_cleanout(image.data, &before);
	UT_ASSERT(heap_receipt_test_final(&receipt, &final_target));
	UT_ASSERT_EQ(final_target.predecessor_page_lsn, PageGetLSN(image.data));
	UT_ASSERT_EQ(final_target.predecessor_page_scn, ((PageHeader)image.data)->pd_block_scn);
	UT_ASSERT_EQ(memcmp(&receipt.ctrc_pending_targets[0], &before, sizeof(before)), 0);
	cluster_undo_record_cancel_prepared(&receipt);
}

UT_TEST(final_plan_drift_is_rejected_even_when_intent_is_compatible)
{
	PGAlignedBlock image;
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterCtrcTargetV1 before, after;
	HeapTupleHeader tuple;

	heap_receipt_fixture(image.data, &receipt, &before);
	UT_ASSERT(heap_receipt_test_plan_capture(&receipt));
	UT_ASSERT(heap_receipt_test_plan_recheck(&receipt));
	heap_receipt_cleanout(image.data, &before);
	UT_ASSERT_EQ(heap_receipt_test_authority_mismatch(), 0);
	UT_ASSERT(heap_receipt_test_capture(image.data, false, receipt.record_type, &after));
	UT_ASSERT(cluster_undo_record_ctrc_pending_matches(&receipt, 0, &after));
	UT_ASSERT(!heap_receipt_test_plan_recheck(&receipt));
	UT_ASSERT_EQ(receipt_apply_calls, 0);
	tuple = (HeapTupleHeader)PageGetItem(image.data, PageGetItemId(image.data, FirstOffsetNumber));
	tuple->t_infomask ^= HEAP_XMAX_INVALID;
	UT_ASSERT(heap_receipt_test_authority_mismatch() != 0);
	cluster_undo_record_cancel_prepared(&receipt);
}

UT_TEST(own_lock_handoff_gets_fresh_data_receipt_and_full_predecessor)
{
	PGAlignedBlock image, before;
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterCtrcTargetV1 pending, final_target;
	ClusterItlSlotData prior;
	ClusterItlSlotData *slot;
	uint8 digest[32];

	heap_receipt_fixture(image.data, &receipt, &pending);
	slot = &ClusterPageGetItlSlots(image.data)[0];
	slot->xid = 700;
	slot->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	slot->wrap = 12;
	slot->write_scn = 90;
	slot->undo_segment_head = uba_encode(1, 7, 0, 0);
	prior = *slot;
	memcpy(before.data, image.data, BLCKSZ);
	UT_ASSERT(heap_receipt_test_plan_capture(&receipt));
	UT_ASSERT(heap_receipt_test_plan_recheck(&receipt));
	UT_ASSERT(heap_receipt_test_final(&receipt, &final_target));
	UT_ASSERT_EQ(final_target.itl_class, 1);
	UT_ASSERT_EQ(final_target.itl_slot_wrap, 13);
	UT_ASSERT_EQ(receipt.ctrc_reuse_mask, 0);
	UT_ASSERT_EQ(receipt_prepare_calls, 1);
	UT_ASSERT_EQ(receipt_apply_calls, 0);
	UT_ASSERT_EQ(memcmp(&receipt.itl_history[0].prior, &prior, sizeof(prior)), 0);
	UT_ASSERT_EQ(receipt.itl_history[0].after_kind, ITL_FLAG_ACTIVE);
	UT_ASSERT(cluster_ctrc_sha256_exact(&prior, sizeof(prior), digest));
	UT_ASSERT_EQ(memcmp(digest, final_target.planned_predecessor_sha256, sizeof(digest)), 0);
	UT_ASSERT(memcmp(final_target.planned_predecessor_sha256, final_target.planned_successor_sha256,
					 sizeof(digest))
			  != 0);
	UT_ASSERT_EQ(memcmp(image.data, before.data, BLCKSZ), 0);
	slot->wrap++;
	UT_ASSERT(!heap_receipt_test_plan_recheck(&receipt));
	cluster_undo_record_cancel_prepared(&receipt);
}

UT_TEST(reuse_applied_and_identity_drift_are_not_page_version_refreshes)
{
	PGAlignedBlock image;
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterCtrcTargetV1 before, after;

	heap_receipt_fixture(image.data, &receipt, &before);
	heap_receipt_cleanout(image.data, &before);
	UT_ASSERT(heap_receipt_test_capture(image.data, false, receipt.record_type, &after));
	after.publication_own_generation--;
	UT_ASSERT(!cluster_undo_record_ctrc_pending_matches(&receipt, 0, &after));
	after.publication_own_generation++;
	receipt.ctrc_reuse_mask = 1;
	UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
	UT_ASSERT(!cluster_undo_record_ctrc_pending_matches(&receipt, 0, &after));
	receipt.ctrc_reuse_mask = 0;
	receipt.ctrc_applied_mask = 1;
	UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
	UT_ASSERT(!cluster_undo_record_ctrc_pending_matches(&receipt, 0, &after));
	UT_ASSERT_EQ(cluster_undo_record_requalify_for_retry(&receipt, 64, true),
				 CLUSTER_UNDO_RECORD_PREPARE_REFUSED);
	UT_ASSERT_EQ(receipt_cancel_calls, 0);
	cluster_undo_record_cancel_prepared(&receipt);
}

UT_TEST(cancel_evidence_belongs_only_to_the_exact_retry)
{
	ClusterUndoRecordPrepareReceipt receipt;
	bool exact_ready = true;
	bool targets_invalidated = true;
	uint64 sequence;

	receipt_fixture_ready(UNDO_RECORD_UPDATE, &receipt);
	sequence = receipt.reservation_sequence;
	receipt_clock_us = 101;
	UT_ASSERT_EQ(cluster_undo_record_requalify_for_retry(&receipt, 64, true),
				 CLUSTER_UNDO_RECORD_PREPARE_REFUSED);
	UT_ASSERT(cluster_undo_record_retry_evidence(sequence, &exact_ready, &targets_invalidated));
	UT_ASSERT(exact_ready && targets_invalidated);
	UT_ASSERT(
		!cluster_undo_record_retry_evidence(sequence + 1, &exact_ready, &targets_invalidated));
	UT_ASSERT(!exact_ready && !targets_invalidated);
	UT_ASSERT(!cluster_undo_record_retry_evidence(sequence, NULL, &targets_invalidated));
	/* Even a new refusal before cancellation must hide the previous evidence. */
	UT_ASSERT_EQ(cluster_undo_record_requalify_for_retry(NULL, 64, true),
				 CLUSTER_UNDO_RECORD_PREPARE_REFUSED);
	UT_ASSERT(!cluster_undo_record_retry_evidence(sequence, &exact_ready, &targets_invalidated));
}

UT_TEST(final_error_keeps_the_exact_cancel_snapshot_and_both_page_observations)
{
	PGAlignedBlock image;
	ClusterUndoRecordPrepareReceipt receipt, saved;
	ClusterCtrcTargetV1 before, after;

	heap_receipt_fixture(image.data, &receipt, &before);
	saved = receipt;
	after = before;
	after.publication_own_generation++;
	receipt_clock_us = 101;
	UT_ASSERT_EQ(cluster_undo_record_requalify_for_retry(&receipt, 64, true),
				 CLUSTER_UNDO_RECORD_PREPARE_REFUSED);
	UT_ASSERT_EQ(heap_receipt_test_error_detail(&saved, &after), 0);
	UT_ASSERT(strstr(heap_receipt_error_detail,
					 "PGRAC_REASON=PREPARE_BUDGET_EXHAUSTED_AFTER_EXACT_INVALIDATION")
			  != NULL);
	UT_ASSERT(strstr(heap_receipt_error_detail,
					 "reservation=17 retry_evidence=1 exact_ready=1 targets_invalidated=1")
			  != NULL);
	UT_ASSERT(strstr(heap_receipt_error_detail,
					 "target0_old=1663/5/9001/0/44,origin0,lsn100,scn100,generation17,epoch19")
			  != NULL);
	UT_ASSERT(strstr(heap_receipt_error_detail,
					 "target0_new=1663/5/9001/0/44,origin0,lsn100,scn100,generation18,epoch19")
			  != NULL);
	UT_ASSERT(strstr(heap_receipt_error_detail, "changed=1") != NULL);
	UT_ASSERT_EQ(receipt_cancel_calls, 1);
	UT_ASSERT_EQ(receipt_apply_calls, 0);
}

UT_TEST(reacquired_current_keeps_unpublished_receipt_not_old_dml_plan)
{
	PGAlignedBlock image;
	ClusterUndoRecordPrepareReceipt receipt, saved;
	ClusterCtrcTargetV1 before, after, final_target;

	heap_receipt_fixture(image.data, &receipt, &before);
	UT_ASSERT(heap_receipt_test_plan_capture(&receipt));
	saved = receipt; /* the staged history is part of this exact reservation */
	heap_receipt_cleanout(image.data, &before);
	heap_receipt_test_current_handoff();
	UT_ASSERT(heap_receipt_test_authority_mismatch() != 0);
	UT_ASSERT(!heap_receipt_test_plan_recheck(&receipt));
	UT_ASSERT(heap_receipt_test_capture(image.data, false, receipt.record_type, &after));
	UT_ASSERT_EQ(after.publication_own_generation, before.publication_own_generation + 1);
	UT_ASSERT_EQ(after.publication_acquisition_epoch, before.publication_acquisition_epoch);
	receipt_clock_us = 101;
	UT_ASSERT(cluster_undo_record_prepared_recheck(&receipt, 64));
	UT_ASSERT(cluster_undo_record_ctrc_pending_matches(&receipt, 0, &after));
	UT_ASSERT_EQ(cluster_undo_record_requalify_for_retry(&receipt, 64, false),
				 CLUSTER_UNDO_RECORD_PREPARE_READY);
	UT_ASSERT_EQ(memcmp(&receipt, &saved, sizeof(saved)), 0);
	UT_ASSERT(heap_receipt_test_final(&receipt, &final_target));
	UT_ASSERT_EQ(final_target.publication_own_generation, after.publication_own_generation);
	heap_receipt_test_current_mode(PCM_STATE_S);
	UT_ASSERT(!heap_receipt_test_final(&receipt, &final_target));
	heap_receipt_test_current_mode(PCM_STATE_X);
	UT_ASSERT(heap_receipt_test_plan_capture(&receipt));
	UT_ASSERT(heap_receipt_test_plan_recheck(&receipt));
	UT_ASSERT_EQ(receipt_cancel_calls, 0);
	UT_ASSERT_EQ(receipt_apply_calls, 0);
	cluster_undo_record_cancel_prepared(&receipt);
}

UT_TEST(reacquisition_cannot_cross_membership_or_retained_identity)
{
	PGAlignedBlock image;
	ClusterUndoRecordPrepareReceipt receipt;
	ClusterCtrcTargetV1 before, after;

	heap_receipt_fixture(image.data, &receipt, &before);
	heap_receipt_test_current_handoff();
	UT_ASSERT(heap_receipt_test_capture(image.data, false, receipt.record_type, &after));
	UT_ASSERT(cluster_undo_record_ctrc_pending_matches(&receipt, 0, &after));
	after.publication_acquisition_epoch++;
	UT_ASSERT(!cluster_undo_record_ctrc_pending_matches(&receipt, 0, &after));
	after.publication_acquisition_epoch--;
	after.block_number++;
	UT_ASSERT(!cluster_undo_record_ctrc_pending_matches(&receipt, 0, &after));
	after.block_number--;
	after.publication_own_generation = 0;
	UT_ASSERT(!cluster_undo_record_ctrc_pending_matches(&receipt, 0, &after));
	after.publication_own_generation = before.publication_own_generation + 1;
	receipt.ctrc_reuse_mask = 1;
	UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
	UT_ASSERT(!cluster_undo_record_ctrc_pending_matches(&receipt, 0, &after));
	receipt.ctrc_reuse_mask = 0;
	receipt.ctrc_applied_mask = 1;
	UT_ASSERT(cluster_undo_record_receipt_sync(&receipt));
	UT_ASSERT(!cluster_undo_record_ctrc_pending_matches(&receipt, 0, &after));
	cluster_undo_record_cancel_prepared(&receipt);
}

UT_TEST(insert_undo_target_matches_real_empty_or_reused_line_pointer)
{
	int variant;

	for (variant = 0; variant < 4; variant++) {
		PGAlignedBlock image, before;
		HeapTupleHeaderData header = { 0 };
		HeapTupleData tuple = { 0 };
		OffsetNumber predicted, actual;

		header.t_hoff = SizeofHeapTupleHeader;
		HeapTupleHeaderSetXmin(&header, 700);
		tuple.t_len = sizeof(header);
		tuple.t_data = &header;
		PageInitHeapPage(image.data, BLCKSZ, 0);
		if (variant != 0) {
			UT_ASSERT_EQ(PageAddItem(image.data, (Item)&header, sizeof(header), InvalidOffsetNumber,
									 false, true),
						 1);
			UT_ASSERT_EQ(PageAddItem(image.data, (Item)&header, sizeof(header), InvalidOffsetNumber,
									 false, true),
						 2);
			if (variant >= 2)
				ItemIdSetUnused(PageGetItemId(image.data, 1));
			if (variant == 2)
				PageSetHasFreeLinePointers(image.data);
		}
		memcpy(before.data, image.data, BLCKSZ);
		predicted = heap_receipt_test_insert_offset(image.data, &tuple);
		UT_ASSERT_EQ(memcmp(before.data, image.data, BLCKSZ), 0);
		actual = PageAddItem(image.data, (Item)&header, sizeof(header), InvalidOffsetNumber, false,
							 true);
		UT_ASSERT_EQ(predicted, actual);
		UT_ASSERT_EQ(actual, variant == 0 || variant == 2 ? 1 : 3);
	}
}

int
main(void)
{
	UT_PLAN(14);
	UT_RUN(own_lock_handoff_gets_fresh_data_receipt_and_full_predecessor);
	UT_RUN(logical_tid_cannot_authorize_another_tuple_address);
	UT_RUN(insert_undo_target_matches_real_empty_or_reused_line_pointer);
	UT_RUN(cleanout_preserves_exact_unpublished_resource_after_prepare_deadline);
	UT_RUN(final_itl_predecessor_is_captured_from_current_page);
	UT_RUN(final_plan_drift_is_rejected_even_when_intent_is_compatible);
	UT_RUN(reuse_applied_and_identity_drift_are_not_page_version_refreshes);
	UT_RUN(test_ready_identity_mismatch_is_not_hidden_by_lifetime_fix);
	UT_RUN(test_retry_after_apply_refuses_without_canceling_shared_owner);
	UT_RUN(test_retry_requalification_keeps_ready_and_proves_actual_invalidation);
	UT_RUN(cancel_evidence_belongs_only_to_the_exact_retry);
	UT_RUN(final_error_keeps_the_exact_cancel_snapshot_and_both_page_observations);
	UT_RUN(reacquired_current_keeps_unpublished_receipt_not_old_dml_plan);
	UT_RUN(reacquisition_cannot_cross_membership_or_retained_identity);
	UT_DONE();
	return ut_failed_count != 0;
}
