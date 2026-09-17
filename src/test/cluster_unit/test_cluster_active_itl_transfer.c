/*-------------------------------------------------------------------------
 *
 * test_cluster_active_itl_transfer.c
 *    Executable policy tests for active-ITL current-block transfer.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/xlog.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_itl_touch.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_xnode_lever.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/*
 * Real cluster_itl_touch.o harness.  Only backend services below the
 * registration list and terminal-finish boundary are replaced: one aligned
 * shared-buffer page, holder-authority projection, memory allocation and the
 * no-fetch/GenericXLog terminal-stamp boundary.  The registration, dedupe,
 * proof invalidation and finish traversal remain the production object.
 */
static PGAlignedBlock test_buffer_block;
static bool test_capture_authority;
static uint64 test_own_generation;
static uint64 test_acquisition_epoch;
static uint8 test_pcm_state;
static uint32 test_own_flags;
static uint64 test_writer_activation_token;
static int test_node_count;
static bool test_recovery_merge_active;
static BufferTag test_live_tag;
static int test_stamp_lock_calls;
static bool test_stamp_lock_saw_valid_proof;
static int test_generic_register_calls;
static int test_generic_abort_calls;
static int test_generic_finish_calls;
static int test_stamp_unlock_calls;
static int test_stamp_skip_calls;
static int test_generic_state_storage;
static XLogRecPtr test_generic_finish_lsn;
static XLogRecPtr test_local_flush_lsn;
static ClusterSfDepVec test_dependency_vec;
static XLogRecPtr test_origin_durable[CLUSTER_SF_DEP_MAX_ORIGINS];
static int test_event_sequence;
static int test_generic_finish_sequence;
static int test_stamp_unlock_sequence;
static int test_xlog_flush_sequence;
static int test_discharge_sequence;
static int test_discharge_calls;
static ClusterCtrcItlProjection test_discharge_projection;
static ClusterCtrcItlTargetIdentity test_discharge_target;
static ClusterCtrcDurability test_discharge_durability;
static ClusterCtrcReceipt *test_discharged_receipt;
static ClusterCtrcParticipantEntry test_ctrc_participant;
static ClusterCtrcReceipt test_ctrc_receipts[2];

int NBuffers = 1;
int NLocBuffer = 0;
char *BufferBlocks = test_buffer_block.data;
Block *LocalBufferBlockPointers = NULL;
MemoryContext CurrentMemoryContext = (MemoryContext)1;
MemoryContext TopTransactionContext = (MemoryContext)1;
bool cluster_enabled = true;
int cluster_node_id = 0;

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

void *
palloc(Size size)
{
	void *memory = malloc(size);

	if (memory == NULL)
		abort();
	return memory;
}

void *
repalloc(void *pointer, Size size)
{
	void *memory = realloc(pointer, size);

	if (memory == NULL)
		abort();
	return memory;
}

bool
cluster_bufmgr_terminal_stamp_authority(Buffer buffer, const BufferTag *expected_tag,
										uint64 *own_generation, uint64 *acquisition_epoch,
										uint8 *pcm_state)
{
	UT_ASSERT_EQ(buffer, 1);
	if (!test_capture_authority || expected_tag == NULL
		|| !BufferTagsEqual(expected_tag, &test_live_tag)
		|| !cluster_itl_terminal_stamp_authority_admissible(
			cluster_enabled, cluster_node_id, test_node_count, test_recovery_merge_active,
			test_pcm_state, test_own_flags, test_writer_activation_token))
		return false;
	*own_generation = test_own_generation;
	*acquisition_epoch = test_acquisition_epoch;
	*pcm_state = test_pcm_state;
	return true;
}

Buffer
cluster_bufmgr_lock_resident_for_exact_itl_stamp(const ClusterItlTouchRecord *record,
												 ClusterItlStampSkipReason *out_reason)
{
	test_stamp_lock_calls++;
	test_stamp_lock_saw_valid_proof = record->proof.valid;
	if (!record->proof.valid
		|| !BufferTagsEqual(&test_live_tag, &(BufferTag){ .spcOid = record->key.rloc.spcOid,
														  .dbOid = record->key.rloc.dbOid,
														  .relNumber = record->key.rloc.relNumber,
														  .forkNum = record->key.forknum,
														  .blockNum = record->key.block })
		|| !cluster_itl_terminal_stamp_authority_admissible(
			cluster_enabled, cluster_node_id, test_node_count, test_recovery_merge_active,
			test_pcm_state, test_own_flags, test_writer_activation_token)
		|| !cluster_itl_terminal_proof_owner_exact(&record->proof, test_own_generation,
												   test_acquisition_epoch, test_pcm_state, true,
												   test_own_flags, test_writer_activation_token)) {
		*out_reason = CLUSTER_ITL_STAMP_SKIP_INVALID_PROOF;
		return InvalidBuffer;
	}
	*out_reason = CLUSTER_ITL_STAMP_SKIP_NONE;
	return 1;
}

void
cluster_bufmgr_unlock_resident_stamp(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	test_stamp_unlock_calls++;
	test_stamp_unlock_sequence = ++test_event_sequence;
}

void
cluster_lever_g_note_stamp_skipped(void)
{
	test_stamp_skip_calls++;
}

GenericXLogState *
GenericXLogStartLogged(bool is_logged pg_attribute_unused())
{
	return (GenericXLogState *)&test_generic_state_storage;
}

Page
GenericXLogRegisterBuffer(GenericXLogState *state, Buffer buffer, int flags pg_attribute_unused())
{
	UT_ASSERT(state == (GenericXLogState *)&test_generic_state_storage);
	UT_ASSERT_EQ(buffer, 1);
	test_generic_register_calls++;
	return BufferGetPage(buffer);
}

XLogRecPtr
GenericXLogFinish(GenericXLogState *state)
{
	UT_ASSERT(state == (GenericXLogState *)&test_generic_state_storage);
	test_generic_finish_calls++;
	test_generic_finish_sequence = ++test_event_sequence;
	return test_generic_finish_lsn;
}

void
GenericXLogAbort(GenericXLogState *state)
{
	UT_ASSERT(state == (GenericXLogState *)&test_generic_state_storage);
	test_generic_abort_calls++;
}

void
XLogFlush(XLogRecPtr record)
{
	test_xlog_flush_sequence = ++test_event_sequence;
	if (record > test_local_flush_lsn)
		test_local_flush_lsn = record;
}

XLogRecPtr
GetFlushRecPtr(TimeLineID *insert_tli pg_attribute_unused())
{
	return test_local_flush_lsn;
}

bool
cluster_sf_dep_vec_for_ship(Buffer buffer, ClusterSfDepVec *out_vec)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(out_vec != NULL);
	*out_vec = test_dependency_vec;
	return !cluster_sf_dep_vec_is_empty(out_vec);
}

XLogRecPtr
cluster_sf_observed_origin_durable_lsn(int32 origin)
{
	UT_ASSERT(origin >= 0 && origin < CLUSTER_SF_DEP_MAX_ORIGINS);
	return test_origin_durable[origin];
}

ClusterCtrcDischargeResult
cluster_ctrc_receipt_discharge_itl_shared(const ClusterCtrcReceiptHandle *handle,
										  const ClusterCtrcItlTargetIdentity *expected_target,
										  ClusterCtrcItlProjection projection,
										  const ClusterCtrcDurability *durability)
{
	UT_ASSERT(handle != NULL && handle->valid);
	UT_ASSERT(expected_target != NULL);
	UT_ASSERT(durability != NULL);
	test_discharge_calls++;
	test_discharge_sequence = ++test_event_sequence;
	test_discharge_projection = projection;
	test_discharge_target = *expected_target;
	test_discharge_durability = *durability;
	test_discharged_receipt = handle->receipt;
	return CLUSTER_CTRC_DISCHARGE_CLEANED;
}

void
cluster_ctrc_note_publication_after_apply(const ClusterCtrcReceiptHandle *handle
											  pg_attribute_unused(),
										  bool current_mx pg_attribute_unused())
{}

static ClusterItlSlotData *
reset_registration_fixture(TransactionId xid)
{
	Page page = (Page)test_buffer_block.data;
	PageHeader page_header = (PageHeader)page;
	ClusterItlSlotData *slot;

	cluster_itl_touch_reset_at_end_xact();
	MemSet(&test_buffer_block, 0, sizeof(test_buffer_block));
	page_header->pd_flags = PD_HAS_ITL;
	page_header->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	page_header->pd_lower = SizeOfPageHeaderData;
	page_header->pd_upper = page_header->pd_special;
	PageSetPageSizeAndVersion(page, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	slot = &ClusterPageGetItlSlots(page)[0];
	slot->xid = xid;
	slot->wrap = 9;
	slot->flags = ITL_FLAG_ACTIVE;

	test_capture_authority = false;
	test_own_generation = 41;
	test_acquisition_epoch = 17;
	test_pcm_state = PCM_STATE_X;
	test_own_flags = 0;
	test_writer_activation_token = 0;
	test_node_count = 2;
	test_recovery_merge_active = false;
	test_live_tag.spcOid = 11;
	test_live_tag.dbOid = 22;
	test_live_tag.relNumber = 33;
	test_live_tag.forkNum = MAIN_FORKNUM;
	test_live_tag.blockNum = 7;
	test_stamp_lock_calls = 0;
	test_stamp_lock_saw_valid_proof = false;
	test_generic_register_calls = 0;
	test_generic_abort_calls = 0;
	test_generic_finish_calls = 0;
	test_stamp_unlock_calls = 0;
	test_stamp_skip_calls = 0;
	test_generic_finish_lsn = InvalidXLogRecPtr;
	test_local_flush_lsn = InvalidXLogRecPtr;
	cluster_sf_dep_vec_reset(&test_dependency_vec);
	MemSet(test_origin_durable, 0, sizeof(test_origin_durable));
	test_event_sequence = 0;
	test_generic_finish_sequence = 0;
	test_stamp_unlock_sequence = 0;
	test_xlog_flush_sequence = 0;
	test_discharge_sequence = 0;
	test_discharge_calls = 0;
	test_discharge_projection = CTRC_ITL_HINT_SKIPPED;
	MemSet(&test_discharge_target, 0, sizeof(test_discharge_target));
	MemSet(&test_discharge_durability, 0, sizeof(test_discharge_durability));
	test_discharged_receipt = NULL;
	MemSet(&test_ctrc_participant, 0, sizeof(test_ctrc_participant));
	MemSet(test_ctrc_receipts, 0, sizeof(test_ctrc_receipts));
	return slot;
}

static ClusterItlTouchHandle
registration_handle(void)
{
	ClusterItlTouchHandle handle;

	MemSet(&handle, 0, sizeof(handle));
	handle.rloc.spcOid = 11;
	handle.rloc.dbOid = 22;
	handle.rloc.relNumber = 33;
	handle.block = 7;
	handle.forknum = MAIN_FORKNUM;
	handle.slot_idx = 0;
	return handle;
}

static ClusterCtrcReceiptHandle
registration_ctrc_handle(int receipt_index, const ClusterItlTouchHandle *touch,
						 const ClusterItlSlotData *slot)
{
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcReceipt *receipt;

	UT_ASSERT(receipt_index >= 0 && receipt_index < (int)lengthof(test_ctrc_receipts));
	UT_ASSERT(touch != NULL);
	UT_ASSERT(slot != NULL);
	receipt = &test_ctrc_receipts[receipt_index];
	MemSet(&handle, 0, sizeof(handle));
	handle.participant = &test_ctrc_participant;
	handle.receipt = receipt;
	handle.receipt_index = (uint64)receipt_index;
	handle.journal_slot_generation = (uint64)receipt_index + 1;
	handle.valid = true;
	receipt->publication.reference_kind = CTRC_REF_HEAP_ITL_UBA;
	receipt->publication.target_kind = CTRC_TARGET_PAGE_PENDING_ITL_SLOT;
	receipt->publication.journal_slot_generation = handle.journal_slot_generation;
	receipt->target.kind = CTRC_TARGET_EXACT_ITL_SLOT;
	receipt->target.spc_oid = touch->rloc.spcOid;
	receipt->target.db_oid = touch->rloc.dbOid;
	receipt->target.rel_number = touch->rloc.relNumber;
	receipt->target.fork_number = touch->forknum;
	receipt->target.block_number = touch->block;
	receipt->target.itl_slot_index = touch->slot_idx;
	receipt->target.itl_slot_wrap = slot->wrap;
	receipt->target.itl_xid = slot->xid;
	receipt->target.itl_class = slot->flags == ITL_FLAG_LOCK_ONLY_ACTIVE ? 2 : 1;
	receipt->target.needs_wal = (touch->flags & CLUSTER_ITL_TOUCH_FLAG_NEEDS_WAL) != 0;
	memcpy(receipt->target.uba, &slot->undo_segment_head, sizeof(slot->undo_segment_head));
	return handle;
}

static ClusterItlTerminalProof
valid_proof(void)
{
	ClusterItlTerminalProof proof;

	MemSet(&proof, 0, sizeof(proof));
	proof.xid = 700;
	proof.buffer_id = 8;
	proof.own_generation = 41;
	proof.acquisition_epoch = 17;
	proof.slot_wrap = 3;
	proof.slot_class = ITL_FLAG_ACTIVE;
	proof.undo_segment_head.raw[0] = UINT64_C(0x1020304050607080);
	proof.undo_segment_head.raw[1] = UINT64_C(0x90a0b0c0d0e0f000);
	proof.pcm_state = PCM_STATE_X;
	proof.valid = true;
	return proof;
}

UT_TEST(u13_exact_owner_proof_matches)
{
	ClusterItlTerminalProof proof = valid_proof();

	UT_ASSERT(cluster_itl_terminal_proof_owner_exact(&proof, 41, 17, PCM_STATE_X, true, 0, 0));
}

UT_TEST(u14_missing_owner_proof_is_rejected)
{
	ClusterItlTerminalProof proof = valid_proof();

	proof.valid = false;
	UT_ASSERT(!cluster_itl_terminal_proof_owner_exact(&proof, 41, 17, PCM_STATE_X, true, 0, 0));
}

UT_TEST(u15_later_x_generation_is_rejected)
{
	ClusterItlTerminalProof proof = valid_proof();

	UT_ASSERT(!cluster_itl_terminal_proof_owner_exact(&proof, 42, 17, PCM_STATE_X, true, 0, 0));
}

UT_TEST(u16_scope_change_is_rejected)
{
	ClusterItlTerminalProof proof = valid_proof();

	UT_ASSERT(!cluster_itl_terminal_proof_owner_exact(&proof, 41, 18, PCM_STATE_X, true, 0, 0));
}

UT_TEST(u17_non_x_owner_is_rejected)
{
	ClusterItlTerminalProof proof = valid_proof();

	UT_ASSERT(!cluster_itl_terminal_proof_owner_exact(&proof, 41, 17, PCM_STATE_N, true, 0, 0));
}

UT_TEST(u18_busy_owner_is_rejected)
{
	ClusterItlTerminalProof proof = valid_proof();

	UT_ASSERT(!cluster_itl_terminal_proof_owner_exact(&proof, 41, 17, PCM_STATE_X, true, 1, 0));
	UT_ASSERT(!cluster_itl_terminal_proof_owner_exact(&proof, 41, 17, PCM_STATE_X, true, 0, 99));
}

UT_TEST(u19_exact_slot_proof_matches)
{
	ClusterItlTerminalProof proof = valid_proof();
	UBA uba = proof.undo_segment_head;

	UT_ASSERT(cluster_itl_terminal_proof_slot_exact(&proof, 700, 3, ITL_FLAG_ACTIVE, &uba));
}

UT_TEST(u20_slot_aba_or_class_change_is_rejected)
{
	ClusterItlTerminalProof proof = valid_proof();
	UBA uba = proof.undo_segment_head;

	UT_ASSERT(!cluster_itl_terminal_proof_slot_exact(&proof, 701, 3, ITL_FLAG_ACTIVE, &uba));
	UT_ASSERT(!cluster_itl_terminal_proof_slot_exact(&proof, 700, 4, ITL_FLAG_ACTIVE, &uba));
	UT_ASSERT(
		!cluster_itl_terminal_proof_slot_exact(&proof, 700, 3, ITL_FLAG_LOCK_ONLY_ACTIVE, &uba));
	uba.raw[1]++;
	UT_ASSERT(!cluster_itl_terminal_proof_slot_exact(&proof, 700, 3, ITL_FLAG_ACTIVE, &uba));
}

UT_TEST(u35_uba_drift_preserves_active_slot)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);
	ClusterCtrcReceiptHandle ctrc;

	slot->undo_segment_head.raw[0] = UINT64_C(0x1111222233334444);
	slot->undo_segment_head.raw[1] = UINT64_C(0x5555666677778888);
	ctrc = registration_ctrc_handle(0, &handle, slot);
	test_capture_authority = true;
	cluster_itl_touch_register_exact_ctrc(&handle, 1, xid, &ctrc);
	slot->undo_segment_head.raw[1]++;
	cluster_itl_xact_abort_finish(xid);

	UT_ASSERT_EQ(slot->flags, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(test_generic_register_calls, 0);
	UT_ASSERT_EQ(test_stamp_skip_calls, 1);
	UT_ASSERT_EQ(test_discharge_calls, 0);
}

UT_TEST(u36_abort_discharge_waits_for_terminal_wal_and_dependency_frontier)
{
	ClusterItlTouchHandle touch = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);
	ClusterCtrcReceiptHandle ctrc;

	touch.flags = CLUSTER_ITL_TOUCH_FLAG_NEEDS_WAL;
	slot->undo_segment_head.raw[0] = UINT64_C(0x1111222233334444);
	slot->undo_segment_head.raw[1] = UINT64_C(0x5555666677778888);
	ctrc = registration_ctrc_handle(0, &touch, slot);
	test_capture_authority = true;
	test_generic_finish_lsn = 300;
	test_local_flush_lsn = 250;
	test_dependency_vec.required[4] = 400;
	test_origin_durable[4] = 400;
	cluster_itl_touch_register_exact_ctrc(&touch, 1, xid, &ctrc);
	cluster_itl_xact_abort_finish(xid);

	UT_ASSERT_EQ(test_discharge_calls, 1);
	UT_ASSERT_EQ(test_discharge_projection, CTRC_ITL_TERMINAL_INDEPENDENT);
	UT_ASSERT(test_discharged_receipt == &test_ctrc_receipts[0]);
	UT_ASSERT_EQ(test_discharge_target.itl_xid, xid);
	UT_ASSERT_EQ(test_discharge_target.itl_slot_wrap, slot->wrap);
	UT_ASSERT_EQ(test_discharge_target.itl_class, 1);
	UT_ASSERT_EQ(test_discharge_target.uba[15], ((const uint8 *)&slot->undo_segment_head)[15]);
	UT_ASSERT_EQ(test_discharge_durability.highest_local_lsn, 300);
	UT_ASSERT_EQ(test_discharge_durability.local_flush_lsn, 300);
	UT_ASSERT_EQ(test_discharge_durability.required_lsn[4], 400);
	UT_ASSERT_EQ(test_discharge_durability.durable_lsn[4], 400);
	UT_ASSERT(test_generic_finish_sequence > 0);
	UT_ASSERT(test_stamp_unlock_sequence > test_generic_finish_sequence);
	UT_ASSERT(test_xlog_flush_sequence > test_stamp_unlock_sequence);
	UT_ASSERT(test_discharge_sequence > test_xlog_flush_sequence);
}

UT_TEST(u37_data_commit_retains_applied_receipt_for_lazy_cleanout)
{
	ClusterItlTouchHandle touch = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);
	ClusterCtrcReceiptHandle ctrc;

	touch.flags = CLUSTER_ITL_TOUCH_FLAG_NEEDS_WAL;
	slot->undo_segment_head.raw[0] = 17;
	ctrc = registration_ctrc_handle(0, &touch, slot);
	test_capture_authority = true;
	test_generic_finish_lsn = 300;
	cluster_itl_touch_register_exact_ctrc(&touch, 1, xid, &ctrc);
	cluster_itl_xact_precommit_finish(xid, 99);

	UT_ASSERT_EQ(slot->flags, ITL_FLAG_NEEDS_CLEANOUT);
	UT_ASSERT_EQ(test_discharge_calls, 0);
	UT_ASSERT_EQ(test_xlog_flush_sequence, 0);
}

UT_TEST(u38_lock_precommit_retains_authority_until_real_terminal_proof)
{
	ClusterItlTouchHandle touch = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);
	ClusterCtrcReceiptHandle ctrc;

	touch.flags = CLUSTER_ITL_TOUCH_FLAG_NEEDS_WAL;
	slot->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	slot->undo_segment_head.raw[0] = 23;
	ctrc = registration_ctrc_handle(0, &touch, slot);
	test_capture_authority = true;
	test_generic_finish_lsn = 500;
	cluster_itl_touch_register_exact_ctrc(&touch, 1, xid, &ctrc);
	cluster_itl_xact_precommit_finish(xid, 99);

	/* This hook precedes both the commit record and terminal TT publication. */
	UT_ASSERT_EQ(slot->flags, ITL_FLAG_LOCK_ONLY_ACTIVE);
	UT_ASSERT_EQ(slot->commit_scn, InvalidScn);
	UT_ASSERT_EQ(test_discharge_calls, 0);
}

UT_TEST(lock_terminal_finish_preserves_precommit_and_clears_only_exact_abort)
{
	int leg;

	for (leg = 0; leg < 3; leg++) {
		ClusterItlTouchHandle touch = registration_handle();
		TransactionId xid = 700;
		ClusterItlSlotData *slot = reset_registration_fixture(xid);
		Page page = (Page)test_buffer_block.data;
		PageHeader header = (PageHeader)page;
		HeapTupleHeader tuple;
		ClusterCtrcReceiptHandle ctrc;

		header->pd_lower += sizeof(ItemIdData);
		header->pd_upper -= MAXALIGN(SizeofHeapTupleHeader);
		ItemIdSetNormal(PageGetItemId(page, FirstOffsetNumber), header->pd_upper,
						SizeofHeapTupleHeader);
		tuple = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
		tuple->t_hoff = SizeofHeapTupleHeader;
		tuple->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
		HeapTupleHeaderSetXmin(tuple, 600);
		HeapTupleHeaderSetXmax(tuple, xid);
		slot->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
		slot->undo_segment_head.raw[0] = 23;
		touch.flags = CLUSTER_ITL_TOUCH_FLAG_NEEDS_WAL;
		ctrc = registration_ctrc_handle(0, &touch, slot);
		test_capture_authority = true;
		test_generic_finish_lsn = 500;
		cluster_itl_touch_register_exact_ctrc(&touch, 1, xid, &ctrc);
		if (leg == 2)
			slot->undo_segment_head.raw[0]++;
		if (leg == 0)
			cluster_itl_xact_precommit_finish(xid, 99);
		else
			cluster_itl_xact_abort_finish(xid);
		UT_ASSERT_EQ((tuple->t_infomask & HEAP_XMAX_INVALID) != 0, leg == 1);
		UT_ASSERT_EQ(slot->flags,
					 leg == 1 ? ITL_FLAG_LOCK_ONLY_ABORTED : ITL_FLAG_LOCK_ONLY_ACTIVE);
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple), 600);
		UT_ASSERT_EQ(test_discharge_calls, leg == 1 ? 1 : 0);
		if (leg == 1) {
			UT_ASSERT(test_generic_finish_sequence > 0);
			UT_ASSERT(test_discharge_sequence > test_xlog_flush_sequence);
		}
	}
}

UT_TEST(u39_same_slot_recapture_replaces_only_the_eager_receipt_handle)
{
	ClusterItlTouchHandle touch = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);
	ClusterCtrcReceiptHandle first;
	ClusterCtrcReceiptHandle second;

	slot->undo_segment_head.raw[0] = 31;
	first = registration_ctrc_handle(0, &touch, slot);
	test_capture_authority = true;
	cluster_itl_touch_register_exact_ctrc(&touch, 1, xid, &first);
	slot->undo_segment_head.raw[0] = 32;
	second = registration_ctrc_handle(1, &touch, slot);
	cluster_itl_touch_register_exact_ctrc(&touch, 1, xid, &second);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 1);
	cluster_itl_xact_abort_finish(xid);

	UT_ASSERT_EQ(test_discharge_calls, 1);
	UT_ASSERT(test_discharged_receipt == &test_ctrc_receipts[1]);
	UT_ASSERT_EQ(test_discharge_target.uba[0], ((const uint8 *)&slot->undo_segment_head)[0]);
}

UT_TEST(u40_reuse_lookup_returns_only_the_exact_live_itl_receipt)
{
	ClusterItlTouchHandle touch = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);
	ClusterCtrcReceiptHandle registered;
	ClusterCtrcReceiptHandle found;

	slot->undo_segment_head.raw[0] = UINT64_C(0x1111222233334444);
	slot->undo_segment_head.raw[1] = UINT64_C(0x5555666677778888);
	registered = registration_ctrc_handle(0, &touch, slot);
	test_capture_authority = true;
	cluster_itl_touch_register_exact_ctrc(&touch, 1, xid, &registered);

	MemSet(&found, 0, sizeof(found));
	UT_ASSERT(cluster_itl_touch_lookup_reusable_ctrc(&touch, 1, xid, &found));
	UT_ASSERT(found.valid);
	UT_ASSERT(found.receipt == registered.receipt);
	UT_ASSERT_EQ(found.journal_slot_generation, registered.journal_slot_generation);

	slot->undo_segment_head.raw[1]++;
	UT_ASSERT(!cluster_itl_touch_lookup_reusable_ctrc(&touch, 1, xid, &found));
	UT_ASSERT(!found.valid);
	slot->undo_segment_head.raw[1]--;
	test_own_generation++;
	UT_ASSERT(!cluster_itl_touch_lookup_reusable_ctrc(&touch, 1, xid, &found));
	UT_ASSERT(!found.valid);
}

UT_TEST(u21_first_failed_capture_does_not_append)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot;
	ClusterCtrcReceiptHandle ctrc;

	slot = reset_registration_fixture(xid);
	ctrc = registration_ctrc_handle(0, &handle, slot);
	cluster_itl_touch_register_exact_ctrc(&handle, 1, xid, &ctrc);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 0);
	cluster_itl_xact_abort_finish(xid);
	UT_ASSERT_EQ(test_discharge_calls, 0);
}

UT_TEST(u22_failed_recapture_invalidates_one_existing_record)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot;
	ClusterCtrcReceiptHandle ctrc;

	slot = reset_registration_fixture(xid);
	ctrc = registration_ctrc_handle(0, &handle, slot);
	test_capture_authority = true;
	cluster_itl_touch_register_exact_ctrc(&handle, 1, xid, &ctrc);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 1);

	test_capture_authority = false;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 1);

	cluster_itl_xact_abort_finish(xid);
	UT_ASSERT_EQ(test_stamp_lock_calls, 1);
	UT_ASSERT(!test_stamp_lock_saw_valid_proof);
	UT_ASSERT_EQ(test_discharge_calls, 0);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 0);
}

UT_TEST(u23_invalidated_record_performs_no_terminal_page_stamp)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);

	test_capture_authority = true;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	test_capture_authority = false;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	cluster_itl_xact_precommit_finish(xid, 99);

	UT_ASSERT_EQ(test_generic_register_calls, 0);
	UT_ASSERT_EQ(test_generic_finish_calls, 0);
	UT_ASSERT_EQ(test_generic_abort_calls, 1);
	UT_ASSERT_EQ(test_stamp_unlock_calls, 0);
	UT_ASSERT_EQ(test_stamp_skip_calls, 1);
	UT_ASSERT_EQ(slot->flags, ITL_FLAG_ACTIVE);
}

UT_TEST(u24_known_single_node_pcm_n_stages_delayed_cleanout)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);

	test_capture_authority = true;
	test_node_count = 1;
	test_pcm_state = PCM_STATE_N;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 1);

	cluster_itl_xact_precommit_finish(xid, 99);
	/*
	 * The hook runs before TransactionIdCommitTree().  Its page byte is
	 * therefore retained commit evidence, not reusable terminal authority.
	 */
	UT_ASSERT_EQ(slot->flags, ITL_FLAG_NEEDS_CLEANOUT);
	UT_ASSERT_EQ(slot->commit_scn, 99);
	UT_ASSERT_EQ(test_generic_register_calls, 1);
	UT_ASSERT_EQ(test_generic_finish_calls, 1);
	UT_ASSERT_EQ(test_stamp_unlock_calls, 1);
	UT_ASSERT_EQ(test_stamp_skip_calls, 0);
}

UT_TEST(u25_peer_pcm_n_is_refused_before_registration)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;

	(void)reset_registration_fixture(xid);
	test_capture_authority = true;
	test_node_count = 2;
	test_pcm_state = PCM_STATE_N;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 0);
}

UT_TEST(u26_unknown_topology_pcm_n_is_refused)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;

	(void)reset_registration_fixture(xid);
	test_capture_authority = true;
	test_node_count = 0;
	test_pcm_state = PCM_STATE_N;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 0);
}

UT_TEST(u27_recovery_merge_pcm_n_is_refused)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;

	(void)reset_registration_fixture(xid);
	test_capture_authority = true;
	test_node_count = 1;
	test_pcm_state = PCM_STATE_N;
	test_recovery_merge_active = true;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 0);
}

UT_TEST(u28_tag_drift_preserves_active_slot)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);

	test_capture_authority = true;
	test_node_count = 1;
	test_pcm_state = PCM_STATE_N;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	test_live_tag.blockNum++;
	cluster_itl_xact_precommit_finish(xid, 99);

	UT_ASSERT_EQ(slot->flags, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(test_generic_register_calls, 0);
	UT_ASSERT_EQ(test_stamp_skip_calls, 1);
}

UT_TEST(u29_generation_drift_preserves_active_slot)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);

	test_capture_authority = true;
	test_node_count = 1;
	test_pcm_state = PCM_STATE_N;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	test_own_generation++;
	cluster_itl_xact_precommit_finish(xid, 99);

	UT_ASSERT_EQ(slot->flags, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(test_generic_register_calls, 0);
	UT_ASSERT_EQ(test_stamp_skip_calls, 1);
}

UT_TEST(u30_slot_drift_preserves_active_slot)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);

	test_capture_authority = true;
	test_node_count = 1;
	test_pcm_state = PCM_STATE_N;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	slot->wrap++;
	cluster_itl_xact_precommit_finish(xid, 99);

	UT_ASSERT_EQ(slot->flags, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(test_generic_register_calls, 0);
	UT_ASSERT_EQ(test_stamp_unlock_calls, 1);
	UT_ASSERT_EQ(test_stamp_skip_calls, 1);
}

UT_TEST(u31_peer_pcm_x_contract_stages_delayed_cleanout)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);

	test_capture_authority = true;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	cluster_itl_xact_precommit_finish(xid, 99);

	UT_ASSERT_EQ(slot->flags, ITL_FLAG_NEEDS_CLEANOUT);
	UT_ASSERT_EQ(test_generic_register_calls, 1);
	UT_ASSERT_EQ(test_stamp_skip_calls, 0);
}

UT_TEST(u32_known_single_node_pcm_x_is_refused)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;

	(void)reset_registration_fixture(xid);
	test_capture_authority = true;
	test_node_count = 1;
	test_pcm_state = PCM_STATE_X;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 0);
}

UT_TEST(u33_busy_pcm_n_tuple_is_refused)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;

	(void)reset_registration_fixture(xid);
	test_capture_authority = true;
	test_node_count = 1;
	test_pcm_state = PCM_STATE_N;
	test_own_flags = 1;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 0);

	test_own_flags = 0;
	test_writer_activation_token = 9;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 0);
}

UT_TEST(u34_epoch_drift_preserves_active_slot)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);

	test_capture_authority = true;
	test_node_count = 1;
	test_pcm_state = PCM_STATE_N;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	test_acquisition_epoch++;
	cluster_itl_xact_precommit_finish(xid, 99);

	UT_ASSERT_EQ(slot->flags, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(test_generic_register_calls, 0);
	UT_ASSERT_EQ(test_stamp_skip_calls, 1);
}

int
main(void)
{
	UT_PLAN(29);
	UT_RUN(lock_terminal_finish_preserves_precommit_and_clears_only_exact_abort);
	UT_RUN(u13_exact_owner_proof_matches);
	UT_RUN(u14_missing_owner_proof_is_rejected);
	UT_RUN(u15_later_x_generation_is_rejected);
	UT_RUN(u16_scope_change_is_rejected);
	UT_RUN(u17_non_x_owner_is_rejected);
	UT_RUN(u18_busy_owner_is_rejected);
	UT_RUN(u19_exact_slot_proof_matches);
	UT_RUN(u20_slot_aba_or_class_change_is_rejected);
	UT_RUN(u21_first_failed_capture_does_not_append);
	UT_RUN(u22_failed_recapture_invalidates_one_existing_record);
	UT_RUN(u23_invalidated_record_performs_no_terminal_page_stamp);
	UT_RUN(u24_known_single_node_pcm_n_stages_delayed_cleanout);
	UT_RUN(u25_peer_pcm_n_is_refused_before_registration);
	UT_RUN(u26_unknown_topology_pcm_n_is_refused);
	UT_RUN(u27_recovery_merge_pcm_n_is_refused);
	UT_RUN(u28_tag_drift_preserves_active_slot);
	UT_RUN(u29_generation_drift_preserves_active_slot);
	UT_RUN(u30_slot_drift_preserves_active_slot);
	UT_RUN(u31_peer_pcm_x_contract_stages_delayed_cleanout);
	UT_RUN(u32_known_single_node_pcm_x_is_refused);
	UT_RUN(u33_busy_pcm_n_tuple_is_refused);
	UT_RUN(u34_epoch_drift_preserves_active_slot);
	UT_RUN(u35_uba_drift_preserves_active_slot);
	UT_RUN(u36_abort_discharge_waits_for_terminal_wal_and_dependency_frontier);
	UT_RUN(u37_data_commit_retains_applied_receipt_for_lazy_cleanout);
	UT_RUN(u38_lock_precommit_retains_authority_until_real_terminal_proof);
	UT_RUN(u39_same_slot_recapture_replaces_only_the_eager_receipt_handle);
	UT_RUN(u40_reuse_lookup_returns_only_the_exact_live_itl_receipt);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
