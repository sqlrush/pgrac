/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_cr_walk.c
 *	R4 synchronous-CR undo-walk contract tests.
 *
 * B1 owns only the transaction-head and previous-edge identity subset in
 * this file.  Later R4 batches extend the same target with physical target,
 * payload, ordering, depth, horizon, foreign-fetch and FULL-only cases.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <string.h>

#include "access/htup_details.h"
#include "storage/bufpage.h"

#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_tx_resolve.h"
#include "cluster/cluster_undo_record.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_uba.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

int cluster_node_id = 0;
int cluster_cr_chain_walk_max_steps = 4096;

MemoryContext TopMemoryContext = (MemoryContext)0x1;

void *
MemoryContextAllocZero(MemoryContext context pg_attribute_unused(), Size size)
{
	void *pointer = malloc(size);

	if (pointer != NULL)
		memset(pointer, 0, size);
	return pointer;
}

void
pfree(void *pointer)
{
	free(pointer);
}

#define TEST_TUPLE_LENGTH 32
#define TEST_UNDO_RECORD_CAPACITY \
	(sizeof(UndoRecordHeader) + sizeof(UndoUpdatePayload) + TEST_TUPLE_LENGTH)

static char ut_undo_record[TEST_UNDO_RECORD_CAPACITY]
	pg_attribute_aligned(MAXIMUM_ALIGNOF);
static size_t ut_undo_record_length;
static UBA ut_expected_record_uba;
static char ut_second_undo_record[TEST_UNDO_RECORD_CAPACITY]
	pg_attribute_aligned(MAXIMUM_ALIGNOF);
static size_t ut_second_undo_record_length;
static UBA ut_expected_second_record_uba;
static bool ut_two_record_sequence;
static char ut_third_undo_record[TEST_UNDO_RECORD_CAPACITY]
	pg_attribute_aligned(MAXIMUM_ALIGNOF);
static size_t ut_third_undo_record_length;
static UBA ut_expected_third_record_uba;
static bool ut_three_record_sequence;
static bool ut_cycle_record_sequence;
static int ut_undo_get_record_calls;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

int
scn_time_cmp(SCN a, SCN b)
{
	uint64 la = scn_local(a);
	uint64 lb = scn_local(b);

	if (la < lb)
		return -1;
	if (la > lb)
		return 1;
	return 0;
}

/*
 * Faithful standalone subset used by cluster_cr_apply.o when an inverse must
 * re-add a tuple at an UNUSED/truncated offset.  This focused R4 case restores
 * in place, but the real helper retains that branch in the linked function.
 */
OffsetNumber
PageAddItemExtended(Page page, Item item, Size size, OffsetNumber offsetNumber, int flags)
{
	PageHeader phdr = (PageHeader)page;
	OffsetNumber limit = OffsetNumberNext(PageGetMaxOffsetNumber(page));
	ItemId item_id;
	Size aligned_size;
	int lower;
	int upper;

	(void)flags;
	if (offsetNumber == InvalidOffsetNumber)
		offsetNumber = limit;
	if (offsetNumber > limit)
		return InvalidOffsetNumber;
	if (offsetNumber < limit) {
		item_id = PageGetItemId(page, offsetNumber);
		if (ItemIdIsUsed(item_id) || ItemIdHasStorage(item_id))
			return InvalidOffsetNumber;
		lower = phdr->pd_lower;
	} else {
		lower = (int)phdr->pd_lower + (int)sizeof(ItemIdData);
	}
	aligned_size = MAXALIGN(size);
	upper = (int)phdr->pd_upper - (int)aligned_size;
	if (lower > upper)
		return InvalidOffsetNumber;
	item_id = PageGetItemId(page, offsetNumber);
	ItemIdSetNormal(item_id, (unsigned)upper, size);
	memcpy((char *)page + upper, item, size);
	phdr->pd_lower = (LocationIndex)lower;
	phdr->pd_upper = (LocationIndex)upper;
	return offsetNumber;
}

size_t
cluster_undo_get_record(UBA uba, void *out_buffer, size_t buffer_size)
{
	const char *record;
	size_t record_length;
	UBA expected_uba;
	int record_index = ut_undo_get_record_calls++;

	if (ut_cycle_record_sequence && (record_index % 2) == 0) {
		record = ut_undo_record;
		record_length = ut_undo_record_length;
		expected_uba = ut_expected_record_uba;
	} else if (ut_cycle_record_sequence) {
		record = ut_second_undo_record;
		record_length = ut_second_undo_record_length;
		expected_uba = ut_expected_second_record_uba;
	} else if (record_index == 0) {
		record = ut_undo_record;
		record_length = ut_undo_record_length;
		expected_uba = ut_expected_record_uba;
	} else if (ut_two_record_sequence && record_index == 1) {
		record = ut_second_undo_record;
		record_length = ut_second_undo_record_length;
		expected_uba = ut_expected_second_record_uba;
	} else if (ut_three_record_sequence && record_index == 2) {
		record = ut_third_undo_record;
		record_length = ut_third_undo_record_length;
		expected_uba = ut_expected_third_record_uba;
	} else {
		UT_ASSERT(false);
		return 0;
	}

	UT_ASSERT(uba.raw[0] == expected_uba.raw[0]
			  && uba.raw[1] == expected_uba.raw[1]);
	UT_ASSERT(buffer_size >= record_length);
	if (out_buffer == NULL || buffer_size < record_length)
		return 0;
	memcpy(out_buffer, record, record_length);
	return record_length;
}

#define TEST_XID ((TransactionId)797)
#define TEST_WRAP ((uint16)7)
#define TEST_SEGMENT ((uint32)1)
#define TEST_TT_OFFSET ((uint16)3)

static UBA
make_record_uba_in_segment(uint32 segment, uint32 block, uint16 row)
{
	return uba_encode(segment, block, TEST_TT_OFFSET, row);
}

static UBA
make_record_uba(uint32 block, uint16 row)
{
	return make_record_uba_in_segment(TEST_SEGMENT, block, row);
}

static ClusterTxLocator
make_locator(void)
{
	ClusterTxLocator locator;

	memset(&locator, 0, sizeof(locator));
	locator.uba = make_record_uba(7, 400);
	locator.xid = TEST_XID;
	locator.tt_wrap = TEST_WRAP;
	locator.itl_kind = ITL_FLAG_ACTIVE;
	locator.itl_slot_index = 2;
	return locator;
}

static UndoRecordHeader
make_record(UBA record_uba, OffsetNumber target_offset)
{
	UndoRecordHeader record;
	uint32 segment;
	uint32 block;
	uint16 tt_offset;
	uint16 row;

	memset(&record, 0, sizeof(record));
	UT_ASSERT(uba_decode(record_uba, &segment, &block, &tt_offset, &row));
	record.record_type = UNDO_RECORD_UPDATE;
	record.payload_length = sizeof(UndoUpdatePayload);
	record.xid = TEST_XID;
	record.origin_node_id = 0;
	record.tt_slot_segment_id = (uint16)segment;
	record.tt_slot_id = cluster_tt_slot_offset_to_id(tt_offset);
	record.write_scn = (SCN)900;
	record.target_locator.relNumber = 123;
	record.target_fork = MAIN_FORKNUM;
	record.target_block = 408;
	record.target_offset = target_offset;
	record.tt_wrap_plus1 = (uint16)(TEST_WRAP + 1);
	return record;
}

static void
assert_head_rejected(const ClusterTxLocator *locator, UBA record_uba,
					 const UndoRecordHeader *record, ClusterTxResolveReason expected_reason)
{
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	UT_ASSERT(!cluster_undo_record_validate_identity(locator, record_uba, record, &reason));
	UT_ASSERT_EQ(reason, expected_reason);
}

static void
assert_edge_rejected(const ClusterTxLocator *locator, UBA current_uba,
					 const UndoRecordHeader *current, UBA previous_uba,
					 const UndoRecordHeader *previous, ClusterTxResolveReason expected_reason)
{
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	UT_ASSERT(!cluster_undo_record_validate_prev_edge(locator, current_uba, current, previous_uba,
													  previous, &reason));
	UT_ASSERT_EQ(reason, expected_reason);
}

UT_TEST(test_head_identity_accepts_exact_uba_xid_wrap_and_tt_slot)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 2);
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	UT_ASSERT(cluster_undo_record_validate_identity(&locator, locator.uba, &head, &reason));
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
}

UT_TEST(test_head_target_offset_is_not_transaction_identity)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 99);
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	/* The queried row may be lp1 while the transaction head targets lp99. */
	UT_ASSERT(cluster_undo_record_validate_identity(&locator, locator.uba, &head, &reason));
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
}

UT_TEST(test_head_exact_uba_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UBA other = make_record_uba(7, 401);
	UndoRecordHeader head = make_record(other, 2);

	assert_head_rejected(&locator, other, &head, CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_head_malformed_uba_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 2);

	locator.uba.raw[1] |= UINT64CONST(1) << 63;
	assert_head_rejected(&locator, locator.uba, &head, CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_head_xid_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 2);

	head.xid = (TransactionId)(TEST_XID + 1);
	assert_head_rejected(&locator, locator.uba, &head, CLUSTER_TX_RESOLVE_XID_MISMATCH);
}

UT_TEST(test_head_missing_wrap_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 2);

	head.tt_wrap_plus1 = 0;
	assert_head_rejected(&locator, locator.uba, &head, CLUSTER_TX_RESOLVE_WRAP_MISMATCH);
}

UT_TEST(test_head_wrap_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 2);

	head.tt_wrap_plus1++;
	assert_head_rejected(&locator, locator.uba, &head, CLUSTER_TX_RESOLVE_WRAP_MISMATCH);
}

UT_TEST(test_head_tt_segment_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 2);

	/* Segment 257 belongs to a different owner partition than segment 1. */
	head.tt_slot_segment_id = 257;
	assert_head_rejected(&locator, locator.uba, &head, CLUSTER_TX_RESOLVE_SLOT_MISMATCH);
}

UT_TEST(test_head_tt_slot_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 2);

	head.tt_slot_id++;
	assert_head_rejected(&locator, locator.uba, &head, CLUSTER_TX_RESOLVE_SLOT_MISMATCH);
}

UT_TEST(test_head_origin_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 2);

	head.origin_node_id = 1;
	assert_head_rejected(&locator, locator.uba, &head, CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_previous_edge_accepts_same_tx_different_target_offset)
{
	ClusterTxLocator locator = make_locator();
	UBA previous_uba = make_record_uba(7, 300);
	UndoRecordHeader head = make_record(locator.uba, 2);
	UndoRecordHeader previous = make_record(previous_uba, 1);
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	head.prev_uba = previous_uba;
	UT_ASSERT(cluster_undo_record_validate_prev_edge(&locator, locator.uba, &head, previous_uba,
													 &previous, &reason));
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
}

UT_TEST(test_previous_edge_accepts_same_owner_physical_segment_rollover)
{
	ClusterTxLocator locator = make_locator();
	UBA previous_uba = make_record_uba_in_segment(2, 9, 500);
	UndoRecordHeader head = make_record(locator.uba, 2);
	UndoRecordHeader previous = make_record(previous_uba, 1);
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	/* Physical segment 2 is a same-owner alias; canonical TT binding stays 1. */
	previous.tt_slot_segment_id = head.tt_slot_segment_id;
	head.prev_uba = previous_uba;
	UT_ASSERT(cluster_undo_record_validate_prev_edge(&locator, locator.uba, &head, previous_uba,
													 &previous, &reason));
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
}

UT_TEST(test_previous_edge_pointer_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UBA previous_uba = make_record_uba(7, 300);
	UndoRecordHeader head = make_record(locator.uba, 2);
	UndoRecordHeader previous = make_record(previous_uba, 1);

	head.prev_uba = make_record_uba(7, 200);
	assert_edge_rejected(&locator, locator.uba, &head, previous_uba, &previous,
						 CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_previous_edge_malformed_uba_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UBA previous_uba = make_record_uba(7, 300);
	UndoRecordHeader head = make_record(locator.uba, 2);
	UndoRecordHeader previous = make_record(previous_uba, 1);

	previous_uba.raw[1] |= UINT64CONST(1) << 63;
	head.prev_uba = previous_uba;
	assert_edge_rejected(&locator, locator.uba, &head, previous_uba, &previous,
						 CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_previous_edge_self_cycle_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UndoRecordHeader head = make_record(locator.uba, 2);

	head.prev_uba = locator.uba;
	assert_edge_rejected(&locator, locator.uba, &head, locator.uba, &head,
						 CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_previous_edge_forward_motion_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UBA previous_uba = make_record_uba(7, 500);
	UndoRecordHeader head = make_record(locator.uba, 2);
	UndoRecordHeader previous = make_record(previous_uba, 1);

	head.prev_uba = previous_uba;
	assert_edge_rejected(&locator, locator.uba, &head, previous_uba, &previous,
						 CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_previous_edge_xid_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UBA previous_uba = make_record_uba(7, 300);
	UndoRecordHeader head = make_record(locator.uba, 2);
	UndoRecordHeader previous = make_record(previous_uba, 1);

	head.prev_uba = previous_uba;
	previous.xid = (TransactionId)(TEST_XID + 1);
	assert_edge_rejected(&locator, locator.uba, &head, previous_uba, &previous,
						 CLUSTER_TX_RESOLVE_XID_MISMATCH);
}

UT_TEST(test_previous_edge_wrap_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UBA previous_uba = make_record_uba(7, 300);
	UndoRecordHeader head = make_record(locator.uba, 2);
	UndoRecordHeader previous = make_record(previous_uba, 1);

	head.prev_uba = previous_uba;
	previous.tt_wrap_plus1++;
	assert_edge_rejected(&locator, locator.uba, &head, previous_uba, &previous,
						 CLUSTER_TX_RESOLVE_WRAP_MISMATCH);
}

UT_TEST(test_previous_edge_tt_segment_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UBA previous_uba = make_record_uba(7, 300);
	UndoRecordHeader head = make_record(locator.uba, 2);
	UndoRecordHeader previous = make_record(previous_uba, 1);

	head.prev_uba = previous_uba;
	previous.tt_slot_segment_id = 257;
	assert_edge_rejected(&locator, locator.uba, &head, previous_uba, &previous,
						 CLUSTER_TX_RESOLVE_SLOT_MISMATCH);
}

UT_TEST(test_previous_edge_tt_slot_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UBA previous_uba = make_record_uba(7, 300);
	UndoRecordHeader head = make_record(locator.uba, 2);
	UndoRecordHeader previous = make_record(previous_uba, 1);

	head.prev_uba = previous_uba;
	previous.tt_slot_id++;
	assert_edge_rejected(&locator, locator.uba, &head, previous_uba, &previous,
						 CLUSTER_TX_RESOLVE_SLOT_MISMATCH);
}

UT_TEST(test_previous_edge_origin_mismatch_is_rejected)
{
	ClusterTxLocator locator = make_locator();
	UBA previous_uba = make_record_uba(7, 300);
	UndoRecordHeader head = make_record(locator.uba, 2);
	UndoRecordHeader previous = make_record(previous_uba, 1);

	head.prev_uba = previous_uba;
	previous.origin_node_id = 1;
	assert_edge_rejected(&locator, locator.uba, &head, previous_uba, &previous,
						 CLUSTER_TX_RESOLVE_BAD_UBA);
}

static void
make_zero_candidate_page(char page[BLCKSZ])
{
	PageHeader header = (PageHeader)page;

	memset(page, 0, BLCKSZ);
	PageSetPageSizeAndVersion((Page)page, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	header->pd_lower = SizeOfPageHeaderData;
	header->pd_upper = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	header->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	header->pd_flags = PD_HAS_ITL;
}

static ClusterR4CrSlotExtension
make_builder_extension(uint64 generation, uint64 builder_incarnation)
{
	ClusterR4CrSlotExtension extension;

	memset(&extension, 0, sizeof(extension));
	extension.slot_generation = generation;
	extension.owner.builder_incarnation = builder_incarnation;
	extension.route_proof.read_scn = (SCN)100;
	return extension;
}

static void
make_one_insert_candidate_page(char page[BLCKSZ],
						   ClusterR4CrSlotExtension *extension)
{
	PageHeader header = (PageHeader)page;
	ItemId item_id;
	ClusterItlSlotData *slot;
	const uint16 tuple_length = TEST_TUPLE_LENGTH;
	const uint16 tuple_offset
		= (uint16)(BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE - tuple_length);
	UndoRecordHeader *record = (UndoRecordHeader *)ut_undo_record;
	UndoInsertPayload *payload
		= (UndoInsertPayload *)(ut_undo_record + sizeof(UndoRecordHeader));

	make_zero_candidate_page(page);
	header->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	header->pd_upper = tuple_offset;
	item_id = PageGetItemId((Page)page, FirstOffsetNumber);
	ItemIdSetNormal(item_id, tuple_offset, tuple_length);
	memset(page + tuple_offset, 0x3c, tuple_length);

	ut_expected_record_uba = make_record_uba(7, 1);
	slot = &ClusterPageGetItlSlots((Page)page)[0];
	slot->xid = TEST_XID;
	slot->wrap = TEST_WRAP;
	slot->flags = ITL_FLAG_ACTIVE;
	slot->undo_segment_head = ut_expected_record_uba;
	slot->write_scn = (SCN)200;

	extension->route_proof.tag.spcOid = 1663;
	extension->route_proof.tag.dbOid = 5;
	extension->route_proof.tag.relNumber = 20000;
	extension->route_proof.tag.forkNum = MAIN_FORKNUM;
	extension->route_proof.tag.blockNum = 37;

	memset(ut_undo_record, 0, sizeof(ut_undo_record));
	record->record_type = UNDO_RECORD_INSERT;
	record->flags = UNDO_REC_FLAG_FIRST_IN_TX;
	record->payload_length = sizeof(*payload);
	record->xid = TEST_XID;
	record->origin_node_id = 0;
	record->tt_slot_segment_id = TEST_SEGMENT;
	record->tt_slot_id = TEST_TT_OFFSET + 1;
	record->write_scn = (SCN)200;
	record->target_locator.spcOid = extension->route_proof.tag.spcOid;
	record->target_locator.dbOid = extension->route_proof.tag.dbOid;
	record->target_locator.relNumber = extension->route_proof.tag.relNumber;
	record->target_fork = extension->route_proof.tag.forkNum;
	record->target_block = extension->route_proof.tag.blockNum;
	record->target_offset = FirstOffsetNumber;
	record->tt_wrap_plus1 = TEST_WRAP + 1;
	payload->inserted_tuple_len = 0;
	ut_undo_record_length = sizeof(UndoRecordHeader) + sizeof(*payload);
	ut_two_record_sequence = false;
	ut_undo_get_record_calls = 0;
}

static void
make_one_update_candidate_page(char page[BLCKSZ],
						   ClusterR4CrSlotExtension *extension,
						   char expected_old[TEST_TUPLE_LENGTH])
{
	PageHeader header = (PageHeader)page;
	ItemId old_item;
	ItemId new_item;
	HeapTupleHeader current_old;
	HeapTupleHeader replacement;
	HeapTupleHeader old_image = (HeapTupleHeader)expected_old;
	ClusterItlSlotData *slot;
	const uint16 replacement_offset
		= (uint16)(BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE - TEST_TUPLE_LENGTH);
	const uint16 old_offset = (uint16)(replacement_offset - TEST_TUPLE_LENGTH);
	UndoRecordHeader *record = (UndoRecordHeader *)ut_undo_record;
	UndoUpdatePayload *payload
		= (UndoUpdatePayload *)(ut_undo_record + sizeof(UndoRecordHeader));
	char *record_old = (char *)payload + sizeof(UndoUpdatePayload);
	const TransactionId base_xid = (TransactionId)(TEST_XID - 1);

	make_zero_candidate_page(page);
	header->pd_lower = SizeOfPageHeaderData + 2 * sizeof(ItemIdData);
	header->pd_upper = old_offset;
	old_item = PageGetItemId((Page)page, FirstOffsetNumber);
	new_item = PageGetItemId((Page)page, OffsetNumberNext(FirstOffsetNumber));
	ItemIdSetNormal(old_item, old_offset, TEST_TUPLE_LENGTH);
	ItemIdSetNormal(new_item, replacement_offset, TEST_TUPLE_LENGTH);

	memset(expected_old, 0, TEST_TUPLE_LENGTH);
	HeapTupleHeaderSetXmin(old_image, base_xid);
	HeapTupleHeaderSetXmax(old_image, InvalidTransactionId);
	old_image->t_infomask = HEAP_XMAX_INVALID;
	old_image->t_infomask2 = 0;
	old_image->t_hoff = SizeofHeapTupleHeader;
	old_image->t_itl_slot_idx = 0;
	ItemPointerSet(&old_image->t_ctid, extension->route_proof.tag.blockNum,
				   FirstOffsetNumber);

	current_old = (HeapTupleHeader)(page + old_offset);
	memcpy(current_old, expected_old, TEST_TUPLE_LENGTH);
	HeapTupleHeaderSetXmax(current_old, TEST_XID);
	current_old->t_infomask &= ~HEAP_XMAX_INVALID;
	ItemPointerSet(&current_old->t_ctid, extension->route_proof.tag.blockNum,
				   OffsetNumberNext(FirstOffsetNumber));

	replacement = (HeapTupleHeader)(page + replacement_offset);
	memset(replacement, 0, TEST_TUPLE_LENGTH);
	HeapTupleHeaderSetXmin(replacement, TEST_XID);
	HeapTupleHeaderSetXmax(replacement, InvalidTransactionId);
	replacement->t_infomask = HEAP_XMAX_INVALID;
	replacement->t_hoff = SizeofHeapTupleHeader;
	replacement->t_itl_slot_idx = 0;
	ItemPointerSet(&replacement->t_ctid, extension->route_proof.tag.blockNum,
				   OffsetNumberNext(FirstOffsetNumber));

	ut_expected_record_uba = make_record_uba(7, 2);
	slot = &ClusterPageGetItlSlots((Page)page)[0];
	slot->xid = TEST_XID;
	slot->wrap = TEST_WRAP;
	slot->flags = ITL_FLAG_ACTIVE;
	slot->undo_segment_head = ut_expected_record_uba;
	slot->write_scn = (SCN)200;

	memset(ut_undo_record, 0, sizeof(ut_undo_record));
	record->record_type = UNDO_RECORD_UPDATE;
	record->flags = UNDO_REC_FLAG_FIRST_IN_TX;
	record->payload_length = sizeof(*payload) + TEST_TUPLE_LENGTH;
	record->xid = TEST_XID;
	record->origin_node_id = 0;
	record->tt_slot_segment_id = TEST_SEGMENT;
	record->tt_slot_id = TEST_TT_OFFSET + 1;
	record->write_scn = (SCN)200;
	record->target_locator.spcOid = extension->route_proof.tag.spcOid;
	record->target_locator.dbOid = extension->route_proof.tag.dbOid;
	record->target_locator.relNumber = extension->route_proof.tag.relNumber;
	record->target_fork = extension->route_proof.tag.forkNum;
	record->target_block = extension->route_proof.tag.blockNum;
	record->target_offset = FirstOffsetNumber;
	record->tt_wrap_plus1 = TEST_WRAP + 1;
	payload->new_block = extension->route_proof.tag.blockNum;
	payload->new_offset = 2;
	payload->old_tuple_length = TEST_TUPLE_LENGTH;
	payload->old_tuple_offset = sizeof(*payload);
	memcpy(record_old, expected_old, TEST_TUPLE_LENGTH);
	ut_undo_record_length
		= sizeof(UndoRecordHeader) + sizeof(*payload) + TEST_TUPLE_LENGTH;
	ut_two_record_sequence = false;
	ut_undo_get_record_calls = 0;
}

static UBA
make_one_foreign_update_candidate_page(char page[BLCKSZ],
								   ClusterR4CrSlotExtension *extension,
								   char expected_old[TEST_TUPLE_LENGTH])
{
	ClusterItlSlotData *slot;
	UBA foreign_uba = uba_encode(257, 9, TEST_TT_OFFSET, 6);

	make_one_update_candidate_page(page, extension, expected_old);
	slot = &ClusterPageGetItlSlots((Page)page)[0];
	slot->undo_segment_head = foreign_uba;

	/* A foreign head must pause before consulting the holder-local reader. */
	ut_expected_record_uba = foreign_uba;
	ut_undo_record_length = 0;
	ut_two_record_sequence = false;
	ut_undo_get_record_calls = 0;
	return foreign_uba;
}

static void
make_one_delete_candidate_page(char page[BLCKSZ],
						   ClusterR4CrSlotExtension *extension,
						   char expected_old[TEST_TUPLE_LENGTH])
{
	PageHeader header = (PageHeader)page;
	ItemId item;
	HeapTupleHeader current;
	HeapTupleHeader old_image = (HeapTupleHeader)expected_old;
	ClusterItlSlotData *slot;
	const uint16 tuple_offset
		= (uint16)(BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE - TEST_TUPLE_LENGTH);
	UndoRecordHeader *record = (UndoRecordHeader *)ut_undo_record;
	UndoDeletePayload *payload
		= (UndoDeletePayload *)(ut_undo_record + sizeof(UndoRecordHeader));
	char *record_old = (char *)payload + sizeof(UndoDeletePayload);
	const TransactionId base_xid = (TransactionId)(TEST_XID - 1);

	make_zero_candidate_page(page);
	header->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	header->pd_upper = tuple_offset;
	item = PageGetItemId((Page)page, FirstOffsetNumber);
	ItemIdSetNormal(item, tuple_offset, TEST_TUPLE_LENGTH);

	memset(expected_old, 0, TEST_TUPLE_LENGTH);
	HeapTupleHeaderSetXmin(old_image, base_xid);
	HeapTupleHeaderSetXmax(old_image, InvalidTransactionId);
	old_image->t_infomask = HEAP_XMAX_INVALID;
	old_image->t_infomask2 = 0;
	old_image->t_hoff = SizeofHeapTupleHeader;
	old_image->t_itl_slot_idx = 0;
	ItemPointerSet(&old_image->t_ctid, extension->route_proof.tag.blockNum,
				   FirstOffsetNumber);

	current = (HeapTupleHeader)(page + tuple_offset);
	memcpy(current, expected_old, TEST_TUPLE_LENGTH);
	HeapTupleHeaderSetXmax(current, TEST_XID);
	current->t_infomask &= ~HEAP_XMAX_INVALID;

	ut_expected_record_uba = make_record_uba(7, 3);
	slot = &ClusterPageGetItlSlots((Page)page)[0];
	slot->xid = TEST_XID;
	slot->wrap = TEST_WRAP;
	slot->flags = ITL_FLAG_ACTIVE;
	slot->undo_segment_head = ut_expected_record_uba;
	slot->write_scn = (SCN)200;

	memset(ut_undo_record, 0, sizeof(ut_undo_record));
	record->record_type = UNDO_RECORD_DELETE;
	record->flags = UNDO_REC_FLAG_FIRST_IN_TX;
	record->payload_length = sizeof(*payload) + TEST_TUPLE_LENGTH;
	record->xid = TEST_XID;
	record->origin_node_id = 0;
	record->tt_slot_segment_id = TEST_SEGMENT;
	record->tt_slot_id = TEST_TT_OFFSET + 1;
	record->write_scn = (SCN)200;
	record->target_locator.spcOid = extension->route_proof.tag.spcOid;
	record->target_locator.dbOid = extension->route_proof.tag.dbOid;
	record->target_locator.relNumber = extension->route_proof.tag.relNumber;
	record->target_fork = extension->route_proof.tag.forkNum;
	record->target_block = extension->route_proof.tag.blockNum;
	record->target_offset = FirstOffsetNumber;
	record->tt_wrap_plus1 = TEST_WRAP + 1;
	payload->full_tuple_length = TEST_TUPLE_LENGTH;
	payload->full_tuple_offset = sizeof(*payload);
	memcpy(record_old, expected_old, TEST_TUPLE_LENGTH);
	ut_undo_record_length
		= sizeof(UndoRecordHeader) + sizeof(*payload) + TEST_TUPLE_LENGTH;
	ut_two_record_sequence = false;
	ut_undo_get_record_calls = 0;
}

static void
make_one_itl_candidate_page(char page[BLCKSZ],
						ClusterR4CrSlotExtension *extension)
{
	PageHeader header = (PageHeader)page;
	ItemId item;
	HeapTupleHeader tuple;
	ClusterItlSlotData *slot;
	const uint16 tuple_offset
		= (uint16)(BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE - TEST_TUPLE_LENGTH);
	UndoRecordHeader *record = (UndoRecordHeader *)ut_undo_record;
	UndoItlPayload *payload
		= (UndoItlPayload *)(ut_undo_record + sizeof(UndoRecordHeader));
	const TransactionId base_xid = (TransactionId)(TEST_XID - 1);

	make_zero_candidate_page(page);
	header->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	header->pd_upper = tuple_offset;
	item = PageGetItemId((Page)page, FirstOffsetNumber);
	ItemIdSetNormal(item, tuple_offset, TEST_TUPLE_LENGTH);
	tuple = (HeapTupleHeader)(page + tuple_offset);
	memset(tuple, 0, TEST_TUPLE_LENGTH);
	HeapTupleHeaderSetXmin(tuple, base_xid);
	HeapTupleHeaderSetXmax(tuple, TEST_XID);
	tuple->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
	tuple->t_infomask2 = HEAP_KEYS_UPDATED;
	tuple->t_hoff = SizeofHeapTupleHeader;
	tuple->t_itl_slot_idx = 0;
	ItemPointerSet(&tuple->t_ctid, extension->route_proof.tag.blockNum,
				   FirstOffsetNumber);

	ut_expected_record_uba = make_record_uba(7, 4);
	slot = &ClusterPageGetItlSlots((Page)page)[0];
	slot->xid = TEST_XID;
	slot->wrap = TEST_WRAP;
	slot->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	slot->undo_segment_head = ut_expected_record_uba;
	slot->write_scn = (SCN)200;

	memset(ut_undo_record, 0, sizeof(ut_undo_record));
	record->record_type = UNDO_RECORD_ITL;
	record->flags = UNDO_REC_FLAG_FIRST_IN_TX;
	record->payload_length = sizeof(*payload);
	record->xid = TEST_XID;
	record->origin_node_id = 0;
	record->tt_slot_segment_id = TEST_SEGMENT;
	record->tt_slot_id = TEST_TT_OFFSET + 1;
	record->write_scn = (SCN)200;
	record->target_locator.spcOid = extension->route_proof.tag.spcOid;
	record->target_locator.dbOid = extension->route_proof.tag.dbOid;
	record->target_locator.relNumber = extension->route_proof.tag.relNumber;
	record->target_fork = extension->route_proof.tag.forkNum;
	record->target_block = extension->route_proof.tag.blockNum;
	record->target_offset = FirstOffsetNumber;
	record->tt_wrap_plus1 = TEST_WRAP + 1;
	payload->itl_slot_idx = 0;
	payload->prev_flags = ITL_FLAG_FREE;
	payload->new_flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	payload->lock_xid = TEST_XID;
	payload->prev_xmax = InvalidTransactionId;
	payload->prev_infomask = HEAP_XMAX_INVALID;
	payload->prev_infomask2 = 0;
	payload->prev_commit_scn = InvalidScn;
	ut_undo_record_length = sizeof(UndoRecordHeader) + sizeof(*payload);
	ut_two_record_sequence = false;
	ut_undo_get_record_calls = 0;
}

static void
make_two_update_candidate_page(char page[BLCKSZ],
						   ClusterR4CrSlotExtension *extension,
						   char expected_oldest[TEST_TUPLE_LENGTH])
{
	PageHeader header = (PageHeader)page;
	ItemId oldest_item;
	ItemId middle_item;
	ItemId newest_item;
	HeapTupleHeader oldest_current;
	HeapTupleHeader middle_current;
	HeapTupleHeader newest_current;
	HeapTupleHeader oldest_image = (HeapTupleHeader)expected_oldest;
	char middle_image_bytes[TEST_TUPLE_LENGTH];
	HeapTupleHeader middle_image = (HeapTupleHeader)middle_image_bytes;
	ClusterItlSlotData *slot;
	const uint16 newest_offset
		= (uint16)(BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE - TEST_TUPLE_LENGTH);
	const uint16 middle_offset = (uint16)(newest_offset - TEST_TUPLE_LENGTH);
	const uint16 oldest_offset = (uint16)(middle_offset - TEST_TUPLE_LENGTH);
	const OffsetNumber middle_offnum = OffsetNumberNext(FirstOffsetNumber);
	const OffsetNumber newest_offnum = OffsetNumberNext(middle_offnum);
	UBA head_uba = make_record_uba(8, 5);
	UBA tail_uba = make_record_uba(8, 4);
	UndoRecordHeader *head = (UndoRecordHeader *)ut_undo_record;
	UndoUpdatePayload *head_payload
		= (UndoUpdatePayload *)(ut_undo_record + sizeof(UndoRecordHeader));
	char *head_old = (char *)head_payload + sizeof(UndoUpdatePayload);
	UndoRecordHeader *tail = (UndoRecordHeader *)ut_second_undo_record;
	UndoUpdatePayload *tail_payload
		= (UndoUpdatePayload *)(ut_second_undo_record + sizeof(UndoRecordHeader));
	char *tail_old = (char *)tail_payload + sizeof(UndoUpdatePayload);
	const TransactionId base_xid = (TransactionId)(TEST_XID - 1);

	make_zero_candidate_page(page);
	header->pd_lower = SizeOfPageHeaderData + 3 * sizeof(ItemIdData);
	header->pd_upper = oldest_offset;
	oldest_item = PageGetItemId((Page)page, FirstOffsetNumber);
	middle_item = PageGetItemId((Page)page, middle_offnum);
	newest_item = PageGetItemId((Page)page, newest_offnum);
	ItemIdSetNormal(oldest_item, oldest_offset, TEST_TUPLE_LENGTH);
	ItemIdSetNormal(middle_item, middle_offset, TEST_TUPLE_LENGTH);
	ItemIdSetNormal(newest_item, newest_offset, TEST_TUPLE_LENGTH);

	memset(expected_oldest, 0, TEST_TUPLE_LENGTH);
	HeapTupleHeaderSetXmin(oldest_image, base_xid);
	HeapTupleHeaderSetXmax(oldest_image, InvalidTransactionId);
	oldest_image->t_infomask = HEAP_XMAX_INVALID;
	oldest_image->t_infomask2 = 0;
	oldest_image->t_hoff = SizeofHeapTupleHeader;
	oldest_image->t_itl_slot_idx = 0;
	ItemPointerSet(&oldest_image->t_ctid, extension->route_proof.tag.blockNum,
				   FirstOffsetNumber);

	oldest_current = (HeapTupleHeader)(page + oldest_offset);
	memcpy(oldest_current, expected_oldest, TEST_TUPLE_LENGTH);
	HeapTupleHeaderSetXmax(oldest_current, TEST_XID);
	oldest_current->t_infomask &= ~HEAP_XMAX_INVALID;
	ItemPointerSet(&oldest_current->t_ctid, extension->route_proof.tag.blockNum,
				   middle_offnum);

	memset(middle_image_bytes, 0, sizeof(middle_image_bytes));
	HeapTupleHeaderSetXmin(middle_image, TEST_XID);
	HeapTupleHeaderSetXmax(middle_image, InvalidTransactionId);
	middle_image->t_infomask = HEAP_XMAX_INVALID;
	middle_image->t_infomask2 = 0;
	middle_image->t_hoff = SizeofHeapTupleHeader;
	middle_image->t_itl_slot_idx = 0;
	ItemPointerSet(&middle_image->t_ctid, extension->route_proof.tag.blockNum,
				   middle_offnum);

	middle_current = (HeapTupleHeader)(page + middle_offset);
	memcpy(middle_current, middle_image_bytes, TEST_TUPLE_LENGTH);
	HeapTupleHeaderSetXmax(middle_current, TEST_XID);
	middle_current->t_infomask &= ~HEAP_XMAX_INVALID;
	ItemPointerSet(&middle_current->t_ctid, extension->route_proof.tag.blockNum,
				   newest_offnum);

	newest_current = (HeapTupleHeader)(page + newest_offset);
	memset(newest_current, 0, TEST_TUPLE_LENGTH);
	HeapTupleHeaderSetXmin(newest_current, TEST_XID);
	HeapTupleHeaderSetXmax(newest_current, InvalidTransactionId);
	newest_current->t_infomask = HEAP_XMAX_INVALID;
	newest_current->t_hoff = SizeofHeapTupleHeader;
	newest_current->t_itl_slot_idx = 0;
	ItemPointerSet(&newest_current->t_ctid, extension->route_proof.tag.blockNum,
				   newest_offnum);

	slot = &ClusterPageGetItlSlots((Page)page)[0];
	slot->xid = TEST_XID;
	slot->wrap = TEST_WRAP;
	slot->flags = ITL_FLAG_ACTIVE;
	slot->undo_segment_head = head_uba;
	slot->write_scn = (SCN)300;

	memset(ut_undo_record, 0, sizeof(ut_undo_record));
	head->record_type = UNDO_RECORD_UPDATE;
	head->payload_length = sizeof(*head_payload) + TEST_TUPLE_LENGTH;
	head->xid = TEST_XID;
	head->origin_node_id = 0;
	head->tt_slot_segment_id = TEST_SEGMENT;
	head->tt_slot_id = TEST_TT_OFFSET + 1;
	head->write_scn = (SCN)300;
	head->prev_uba = tail_uba;
	head->target_locator.spcOid = extension->route_proof.tag.spcOid;
	head->target_locator.dbOid = extension->route_proof.tag.dbOid;
	head->target_locator.relNumber = extension->route_proof.tag.relNumber;
	head->target_fork = extension->route_proof.tag.forkNum;
	head->target_block = extension->route_proof.tag.blockNum;
	head->target_offset = middle_offnum;
	head->tt_wrap_plus1 = TEST_WRAP + 1;
	head_payload->new_block = extension->route_proof.tag.blockNum;
	head_payload->new_offset = newest_offnum;
	head_payload->old_tuple_length = TEST_TUPLE_LENGTH;
	head_payload->old_tuple_offset = sizeof(*head_payload);
	memcpy(head_old, middle_image_bytes, TEST_TUPLE_LENGTH);

	memset(ut_second_undo_record, 0, sizeof(ut_second_undo_record));
	tail->record_type = UNDO_RECORD_UPDATE;
	tail->flags = UNDO_REC_FLAG_FIRST_IN_TX;
	tail->payload_length = sizeof(*tail_payload) + TEST_TUPLE_LENGTH;
	tail->xid = TEST_XID;
	tail->origin_node_id = 0;
	tail->tt_slot_segment_id = TEST_SEGMENT;
	tail->tt_slot_id = TEST_TT_OFFSET + 1;
	tail->write_scn = (SCN)200;
	tail->target_locator.spcOid = extension->route_proof.tag.spcOid;
	tail->target_locator.dbOid = extension->route_proof.tag.dbOid;
	tail->target_locator.relNumber = extension->route_proof.tag.relNumber;
	tail->target_fork = extension->route_proof.tag.forkNum;
	tail->target_block = extension->route_proof.tag.blockNum;
	tail->target_offset = FirstOffsetNumber;
	tail->tt_wrap_plus1 = TEST_WRAP + 1;
	tail_payload->new_block = extension->route_proof.tag.blockNum;
	tail_payload->new_offset = middle_offnum;
	tail_payload->old_tuple_length = TEST_TUPLE_LENGTH;
	tail_payload->old_tuple_offset = sizeof(*tail_payload);
	memcpy(tail_old, expected_oldest, TEST_TUPLE_LENGTH);

	ut_undo_record_length
		= sizeof(UndoRecordHeader) + sizeof(*head_payload) + TEST_TUPLE_LENGTH;
	ut_expected_record_uba = head_uba;
	ut_second_undo_record_length
		= sizeof(UndoRecordHeader) + sizeof(*tail_payload) + TEST_TUPLE_LENGTH;
	ut_expected_second_record_uba = tail_uba;
	ut_two_record_sequence = true;
	ut_undo_get_record_calls = 0;
}

static void
make_single_record_undo_block(char block[BLCKSZ], const char *record_bytes,
							  size_t record_length)
{
	UndoBlockHeader *header = (UndoBlockHeader *)block;
	const UndoRecordHeader *record = (const UndoRecordHeader *)record_bytes;
	UndoSlotDirEntry *slot;

	UT_ASSERT(record_bytes != NULL);
	UT_ASSERT(record_length >= sizeof(UndoRecordHeader));
	UT_ASSERT(record_length <= UINT16_MAX);
	UT_ASSERT(sizeof(UndoBlockHeader) + record_length
			  < UNDO_SLOT_DIR_OFFSET(0));
	memset(block, 0, BLCKSZ);
	header->magic = PGRAC_UNDO_BLOCK_MAGIC;
	header->block_version = UNDO_BLOCK_VERSION_1;
	header->slot_count = 1;
	header->free_offset = (uint32)sizeof(UndoBlockHeader) + (uint32)record_length;
	header->first_change_scn = record->write_scn;
	memcpy(block + sizeof(UndoBlockHeader), record_bytes, record_length);
	slot = UNDO_SLOT_DIR_PTR(block, 0);
	slot->record_offset = sizeof(UndoBlockHeader);
	slot->record_length = (uint16)record_length;
	slot->record_type = record->record_type;
	slot->flags = record->flags;
}

static size_t
make_resident_record_fixture(char block[BLCKSZ], PGAlignedBlock *record_bytes,
							 ClusterTxLocator *locator)
{
	UndoRecordHeader *record = (UndoRecordHeader *)record_bytes->data;
	size_t record_length = sizeof(*record) + sizeof(UndoUpdatePayload);

	*locator = make_locator();
	locator->uba = make_record_uba(7, 0);
	memset(record_bytes, 0, sizeof(*record_bytes));
	*record = make_record(locator->uba, FirstOffsetNumber);
	record->flags = UNDO_REC_FLAG_FIRST_IN_TX;
	memset(&record->prev_uba, 0, sizeof(record->prev_uba));
	make_single_record_undo_block(block, record_bytes->data, record_length);
	return record_length;
}

static void
assert_resident_record_rejected_unchanged(
	const char block[BLCKSZ], const ClusterTxLocator *locator)
{
	PGAlignedBlock record_out;
	PGAlignedBlock record_before;
	char block_before[BLCKSZ];
	ClusterTxLocator locator_before = *locator;
	ClusterTxLocator canonical_out;
	ClusterTxLocator canonical_before;
	size_t record_length = 777;

	memset(&record_out, 0xa5, sizeof(record_out));
	record_before = record_out;
	memset(&canonical_out, 0xc3, sizeof(canonical_out));
	canonical_before = canonical_out;
	memcpy(block_before, block, sizeof(block_before));

	UT_ASSERT(!cluster_cr_r4_extract_resident_record(
		block, locator, record_out.data, &record_length, &canonical_out));
	UT_ASSERT_EQ(record_length, 777);
	UT_ASSERT_EQ(memcmp(&record_out, &record_before, sizeof(record_out)), 0);
	UT_ASSERT_EQ(memcmp(&canonical_out, &canonical_before,
					 sizeof(canonical_out)), 0);
	UT_ASSERT_EQ(memcmp(block, block_before, sizeof(block_before)), 0);
	UT_ASSERT_EQ(memcmp(locator, &locator_before, sizeof(locator_before)), 0);
}

UT_TEST(test_r4_resident_record_extracts_exact_canonical_hit)
{
	char block[BLCKSZ];
	char block_before[BLCKSZ];
	PGAlignedBlock record_bytes;
	PGAlignedBlock record_out;
	ClusterTxLocator locator;
	ClusterTxLocator locator_before;
	ClusterTxLocator canonical_out;
	size_t expected_length;
	size_t record_length = 0;

	expected_length
		= make_resident_record_fixture(block, &record_bytes, &locator);
	locator_before = locator;
	memcpy(block_before, block, sizeof(block_before));
	memset(&record_out, 0xa5, sizeof(record_out));
	memset(&canonical_out, 0, sizeof(canonical_out));

	UT_ASSERT(cluster_cr_r4_extract_resident_record(
		block, &locator, record_out.data, &record_length, &canonical_out));
	UT_ASSERT_EQ(record_length, expected_length);
	UT_ASSERT_EQ(memcmp(record_out.data, record_bytes.data, expected_length), 0);
	UT_ASSERT_EQ((unsigned char)record_out.data[expected_length], 0xa5);
	UT_ASSERT_EQ(memcmp(&canonical_out, &locator, sizeof(locator)), 0);
	UT_ASSERT_EQ(memcmp(block, block_before, sizeof(block_before)), 0);
	UT_ASSERT_EQ(memcmp(&locator, &locator_before, sizeof(locator)), 0);
}

UT_TEST(test_r4_resident_record_upgrades_unknown_wrap_once)
{
	char block[BLCKSZ];
	PGAlignedBlock record_bytes;
	PGAlignedBlock record_out;
	ClusterTxLocator locator;
	ClusterTxLocator canonical_out;
	size_t expected_length;
	size_t record_length = 0;

	expected_length
		= make_resident_record_fixture(block, &record_bytes, &locator);
	locator.tt_wrap = TT_WRAP_INVALID;
	memset(&record_out, 0, sizeof(record_out));
	memset(&canonical_out, 0, sizeof(canonical_out));

	UT_ASSERT(cluster_cr_r4_extract_resident_record(
		block, &locator, record_out.data, &record_length, &canonical_out));
	UT_ASSERT_EQ(record_length, expected_length);
	UT_ASSERT_EQ(canonical_out.tt_wrap, TEST_WRAP);
	UT_ASSERT_EQ(canonical_out.uba.raw[0], locator.uba.raw[0]);
	UT_ASSERT_EQ(canonical_out.uba.raw[1], locator.uba.raw[1]);
	UT_ASSERT_EQ(canonical_out.xid, locator.xid);
	UT_ASSERT_EQ(locator.tt_wrap, TT_WRAP_INVALID);
}

UT_TEST(test_r4_resident_record_rejects_canonical_wrap_mismatch)
{
	char block[BLCKSZ];
	PGAlignedBlock record_bytes;
	ClusterTxLocator locator;

	(void)make_resident_record_fixture(block, &record_bytes, &locator);
	locator.tt_wrap++;
	assert_resident_record_rejected_unchanged(block, &locator);
}

UT_TEST(test_r4_resident_record_rejects_unknown_wrap_without_record_wrap)
{
	char block[BLCKSZ];
	PGAlignedBlock record_bytes;
	ClusterTxLocator locator;
	UndoRecordHeader *record;

	(void)make_resident_record_fixture(block, &record_bytes, &locator);
	locator.tt_wrap = TT_WRAP_INVALID;
	record = (UndoRecordHeader *)(block + sizeof(UndoBlockHeader));
	record->tt_wrap_plus1 = 0;
	assert_resident_record_rejected_unchanged(block, &locator);
}

UT_TEST(test_r4_resident_record_rejects_bad_data_header)
{
	char block[BLCKSZ];
	PGAlignedBlock record_bytes;
	ClusterTxLocator locator;
	UndoBlockHeader *header;

	(void)make_resident_record_fixture(block, &record_bytes, &locator);
	header = (UndoBlockHeader *)block;
	header->magic++;
	assert_resident_record_rejected_unchanged(block, &locator);

	(void)make_resident_record_fixture(block, &record_bytes, &locator);
	header->block_version++;
	assert_resident_record_rejected_unchanged(block, &locator);
}

UT_TEST(test_r4_resident_record_rejects_bad_row_and_bounds)
{
	char block[BLCKSZ];
	PGAlignedBlock record_bytes;
	ClusterTxLocator locator;
	UndoBlockHeader *header;
	UndoSlotDirEntry *slot;

	(void)make_resident_record_fixture(block, &record_bytes, &locator);
	locator.uba = make_record_uba(7, 1);
	assert_resident_record_rejected_unchanged(block, &locator);

	(void)make_resident_record_fixture(block, &record_bytes, &locator);
	header = (UndoBlockHeader *)block;
	header->free_offset = sizeof(UndoBlockHeader) - 1;
	assert_resident_record_rejected_unchanged(block, &locator);

	(void)make_resident_record_fixture(block, &record_bytes, &locator);
	slot = UNDO_SLOT_DIR_PTR(block, 0);
	slot->record_length++;
	assert_resident_record_rejected_unchanged(block, &locator);
}

UT_TEST(test_r4_resident_record_rejects_directory_type_and_flags_mismatch)
{
	char block[BLCKSZ];
	PGAlignedBlock record_bytes;
	ClusterTxLocator locator;
	UndoSlotDirEntry *slot;

	(void)make_resident_record_fixture(block, &record_bytes, &locator);
	slot = UNDO_SLOT_DIR_PTR(block, 0);
	slot->record_type = UNDO_RECORD_DELETE;
	assert_resident_record_rejected_unchanged(block, &locator);

	(void)make_resident_record_fixture(block, &record_bytes, &locator);
	slot = UNDO_SLOT_DIR_PTR(block, 0);
	slot->flags = 0;
	assert_resident_record_rejected_unchanged(block, &locator);
}

UT_TEST(test_r4_resident_record_rejects_xid_mismatch)
{
	char block[BLCKSZ];
	PGAlignedBlock record_bytes;
	ClusterTxLocator locator;
	UndoRecordHeader *record;

	(void)make_resident_record_fixture(block, &record_bytes, &locator);
	record = (UndoRecordHeader *)(block + sizeof(UndoBlockHeader));
	record->xid++;
	assert_resident_record_rejected_unchanged(block, &locator);
}

UT_TEST(test_r4_resident_record_rejects_malformed_and_block_zero_uba)
{
	char block[BLCKSZ];
	PGAlignedBlock record_bytes;
	ClusterTxLocator locator;

	(void)make_resident_record_fixture(block, &record_bytes, &locator);
	locator.uba.raw[1] |= UINT64CONST(1) << 63;
	assert_resident_record_rejected_unchanged(block, &locator);

	(void)make_resident_record_fixture(block, &record_bytes, &locator);
	locator.uba = uba_encode(TEST_SEGMENT, 0, TEST_TT_OFFSET, 0);
	assert_resident_record_rejected_unchanged(block, &locator);
}

/* A preceding writer record can have an odd total length, so the next
 * directory entry is allowed to identify a record whose header is not
 * naturally aligned within the immutable DATA image. */
static void
make_unaligned_second_record_undo_block(char block[BLCKSZ],
									const char *record_bytes,
									size_t record_length)
{
	PGAlignedBlock prefix_buffer;
	UndoRecordHeader *prefix = (UndoRecordHeader *)prefix_buffer.data;
	UndoUpdatePayload *prefix_payload
		= (UndoUpdatePayload *)(prefix_buffer.data + sizeof(UndoRecordHeader));
	UndoBlockHeader *header = (UndoBlockHeader *)block;
	UndoSlotDirEntry *prefix_slot;
	UndoSlotDirEntry *record_slot;
	const size_t prefix_tuple_length = TEST_TUPLE_LENGTH + 1;
	const size_t prefix_length = sizeof(UndoRecordHeader)
								 + sizeof(UndoUpdatePayload)
								 + prefix_tuple_length;
	const size_t record_offset = sizeof(UndoBlockHeader) + prefix_length;

	UT_ASSERT(record_bytes != NULL);
	UT_ASSERT(record_length >= sizeof(UndoRecordHeader));
	UT_ASSERT(record_length <= UINT16_MAX);
	UT_ASSERT(record_offset % MAXIMUM_ALIGNOF != 0);
	UT_ASSERT(record_offset + record_length < UNDO_SLOT_DIR_OFFSET(1));
	memset(block, 0, BLCKSZ);
	memset(&prefix_buffer, 0, sizeof(prefix_buffer));
	prefix->record_type = UNDO_RECORD_UPDATE;
	prefix->flags = UNDO_REC_FLAG_FIRST_IN_TX;
	prefix->payload_length
		= (uint16)(sizeof(UndoUpdatePayload) + prefix_tuple_length);
	prefix_payload->new_block = InvalidBlockNumber;
	prefix_payload->new_offset = InvalidOffsetNumber;
	prefix_payload->old_tuple_length = (uint16)prefix_tuple_length;
	prefix_payload->old_tuple_offset = sizeof(UndoUpdatePayload);

	header->magic = PGRAC_UNDO_BLOCK_MAGIC;
	header->block_version = UNDO_BLOCK_VERSION_1;
	header->slot_count = 2;
	header->free_offset = (uint32)(record_offset + record_length);
	memcpy(block + sizeof(UndoBlockHeader), prefix_buffer.data, prefix_length);
	memcpy(block + record_offset, record_bytes, record_length);
	prefix_slot = UNDO_SLOT_DIR_PTR(block, 0);
	prefix_slot->record_offset = sizeof(UndoBlockHeader);
	prefix_slot->record_length = (uint16)prefix_length;
	prefix_slot->record_type = prefix->record_type;
	prefix_slot->flags = prefix->flags;
	record_slot = UNDO_SLOT_DIR_PTR(block, 1);
	record_slot->record_offset = (uint32)record_offset;
	record_slot->record_length = (uint16)record_length;
	record_slot->record_type = (uint8)record_bytes[0];
	record_slot->flags = (uint8)record_bytes[1];
}

static void
make_two_foreign_update_candidate_page(
	char page[BLCKSZ], ClusterR4CrSlotExtension *extension,
	char expected_oldest[TEST_TUPLE_LENGTH], char head_block[BLCKSZ],
	char tail_block[BLCKSZ], UBA *head_uba_out, UBA *tail_uba_out)
{
	ClusterItlSlotData *slot;
	UndoRecordHeader *head = (UndoRecordHeader *)ut_undo_record;
	UndoRecordHeader *tail = (UndoRecordHeader *)ut_second_undo_record;
	UBA head_uba = make_record_uba_in_segment(257, 9, 0);
	UBA tail_uba = make_record_uba_in_segment(257, 8, 0);

	make_two_update_candidate_page(page, extension, expected_oldest);
	slot = &ClusterPageGetItlSlots((Page)page)[0];
	slot->undo_segment_head = head_uba;
	head->origin_node_id = 1;
	head->tt_slot_segment_id = 257;
	head->prev_uba = tail_uba;
	tail->origin_node_id = 1;
	tail->tt_slot_segment_id = 257;
	make_single_record_undo_block(
		head_block, ut_undo_record, ut_undo_record_length);
	make_single_record_undo_block(
		tail_block, ut_second_undo_record, ut_second_undo_record_length);

	/* Both records are foreign; the holder-local reader must remain unused. */
	ut_expected_record_uba = head_uba;
	ut_undo_record_length = 0;
	ut_second_undo_record_length = 0;
	ut_two_record_sequence = false;
	ut_undo_get_record_calls = 0;
	*head_uba_out = head_uba;
	*tail_uba_out = tail_uba;
}

static void
make_target_other_target_update_candidate_page(
	char page[BLCKSZ], ClusterR4CrSlotExtension *extension,
	char expected_oldest[TEST_TUPLE_LENGTH])
{
	ClusterItlSlotData *slot;
	UndoRecordHeader *head = (UndoRecordHeader *)ut_undo_record;
	UndoRecordHeader *middle = (UndoRecordHeader *)ut_second_undo_record;
	UBA head_uba = make_record_uba(8, 6);
	UBA middle_uba = make_record_uba(8, 5);
	UBA tail_uba = make_record_uba(8, 4);

	make_two_update_candidate_page(page, extension, expected_oldest);
	memcpy(ut_third_undo_record, ut_second_undo_record,
		   ut_second_undo_record_length);
	ut_third_undo_record_length = ut_second_undo_record_length;
	ut_expected_third_record_uba = tail_uba;

	memcpy(ut_second_undo_record, ut_undo_record, ut_undo_record_length);
	ut_second_undo_record_length = ut_undo_record_length;
	middle->write_scn = (SCN)250;
	middle->prev_uba = tail_uba;
	middle->target_block = extension->route_proof.tag.blockNum + 1;
	middle->target_offset = FirstOffsetNumber;

	head->prev_uba = middle_uba;
	slot = &ClusterPageGetItlSlots((Page)page)[0];
	slot->undo_segment_head = head_uba;
	ut_expected_record_uba = head_uba;
	ut_expected_second_record_uba = middle_uba;
	ut_three_record_sequence = true;
	ut_undo_get_record_calls = 0;
}

static void
make_two_xid_update_candidate_page(char page[BLCKSZ],
							   ClusterR4CrSlotExtension *extension,
							   char expected_oldest[TEST_TUPLE_LENGTH])
{
	ClusterItlSlotData *slots;
	ItemId middle_item;
	ItemId newest_item;
	HeapTupleHeader middle_current;
	HeapTupleHeader newest_current;
	UndoRecordHeader *newer_record = (UndoRecordHeader *)ut_undo_record;
	UndoRecordHeader *older_record = (UndoRecordHeader *)ut_second_undo_record;
	const TransactionId newer_xid = (TransactionId)(TEST_XID + 1);
	const uint16 newer_wrap = (uint16)(TEST_WRAP + 1);
	const uint16 newer_tt_offset = (uint16)(TEST_TT_OFFSET + 1);
	UBA older_uba = make_record_uba(8, 4);
	UBA newer_uba = uba_encode(TEST_SEGMENT, 8, newer_tt_offset, 5);

	make_two_update_candidate_page(page, extension, expected_oldest);
	middle_item = PageGetItemId((Page)page, OffsetNumberNext(FirstOffsetNumber));
	newest_item = PageGetItemId(
		(Page)page, OffsetNumberNext(OffsetNumberNext(FirstOffsetNumber)));
	middle_current = (HeapTupleHeader)PageGetItem((Page)page, middle_item);
	newest_current = (HeapTupleHeader)PageGetItem((Page)page, newest_item);
	HeapTupleHeaderSetXmax(middle_current, newer_xid);
	middle_current->t_itl_slot_idx = 1;
	HeapTupleHeaderSetXmin(newest_current, newer_xid);
	newest_current->t_itl_slot_idx = 1;

	/* Physical slot order is deliberately oldest first, opposite the walk. */
	slots = ClusterPageGetItlSlots((Page)page);
	slots[0].xid = TEST_XID;
	slots[0].wrap = TEST_WRAP;
	slots[0].flags = ITL_FLAG_ACTIVE;
	slots[0].undo_segment_head = older_uba;
	slots[0].write_scn = (SCN)200;
	slots[1].xid = newer_xid;
	slots[1].wrap = newer_wrap;
	slots[1].flags = ITL_FLAG_ACTIVE;
	slots[1].undo_segment_head = newer_uba;
	slots[1].write_scn = (SCN)300;

	newer_record->flags = UNDO_REC_FLAG_FIRST_IN_TX;
	memset(&newer_record->prev_uba, 0, sizeof(newer_record->prev_uba));
	newer_record->xid = newer_xid;
	newer_record->tt_slot_id = (uint32)newer_tt_offset + 1;
	newer_record->tt_wrap_plus1 = newer_wrap + 1;
	older_record->flags = UNDO_REC_FLAG_FIRST_IN_TX;
	memset(&older_record->prev_uba, 0, sizeof(older_record->prev_uba));

	/* The reader double makes write_scn-DESC observable: newer, then older. */
	ut_expected_record_uba = newer_uba;
	ut_expected_second_record_uba = older_uba;
	ut_two_record_sequence = true;
	ut_undo_get_record_calls = 0;
}

UT_TEST(test_r4_builder_zero_candidate_is_full_and_exact_forget_reopens_key)
{
	char page[BLCKSZ];
	char page_before[BLCKSZ];
	char foreign_page[BLCKSZ];
	char foreign_before[BLCKSZ];
	ClusterR4CrSlotExtension extension = make_builder_extension(41, 73);
	ClusterR4CrSlotExtension extension_before = extension;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;

	make_zero_candidate_page(page);
	memcpy(page_before, page, sizeof(page));
	memset(foreign_page, 0x5a, sizeof(foreign_page));
	memcpy(foreign_before, foreign_page, sizeof(foreign_page));

	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(0, 41, false, &extension, page,
											 foreign_page, &reason),
			 CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT(memcmp(page, page_before, sizeof(page)) == 0);
	UT_ASSERT(memcmp(&extension, &extension_before, sizeof(extension)) == 0);
	UT_ASSERT(memcmp(foreign_page, foreign_before, sizeof(foreign_page)) == 0);

	reason = CLUSTER_CR_BUILD_NONE;
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(0, 41, false, &extension, page,
											 foreign_page, &reason),
			 CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_PROTOCOL);

	cluster_cr_build_on_holder_forget(0, 42);
	reason = CLUSTER_CR_BUILD_NONE;
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(0, 41, false, &extension, page,
											 foreign_page, &reason),
			 CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_PROTOCOL);

	cluster_cr_build_on_holder_forget(0, 41);
	reason = CLUSTER_CR_BUILD_PROTOCOL;
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(0, 41, false, &extension, page,
											 foreign_page, &reason),
			 CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	cluster_cr_build_on_holder_forget(0, 41);
}

UT_TEST(test_r4_builder_recycle_watermark_above_read_scn_fails_closed)
{
	char page[BLCKSZ];
	char page_before[BLCKSZ];
	char foreign_page[BLCKSZ];
	char foreign_before[BLCKSZ];
	ClusterR4CrSlotExtension extension = make_builder_extension(42, 74);
	ClusterR4CrSlotExtension extension_before = extension;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_NONE;

	make_zero_candidate_page(page);
	ClusterPageGetItlHeader((Page)page)->itl_recycle_watermark_scn = (SCN)200;
	memcpy(page_before, page, sizeof(page));
	memset(foreign_page, 0xa5, sizeof(foreign_page));
	memcpy(foreign_before, foreign_page, sizeof(foreign_page));

	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(0, 42, false, &extension, page,
											 foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_SNAPSHOT_TOO_OLD);
	UT_ASSERT_EQ(extension.build_steps, 0);
	UT_ASSERT(memcmp(page, page_before, sizeof(page)) == 0);
	UT_ASSERT(memcmp(&extension, &extension_before, sizeof(extension)) == 0);
	UT_ASSERT(memcmp(foreign_page, foreign_before, sizeof(foreign_page)) == 0);

	cluster_cr_build_on_holder_forget(0, 42);
}

UT_TEST(test_r4_builder_rejects_invalid_page_layout_before_full)
{
	char page[BLCKSZ];
	char page_before[BLCKSZ];
	char foreign_page[BLCKSZ];
	char foreign_before[BLCKSZ];
	PageHeader page_header;
	ClusterR4CrSlotExtension extension = make_builder_extension(43, 75);
	ClusterR4CrSlotExtension extension_before = extension;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_NONE;

	make_zero_candidate_page(page);
	page_header = (PageHeader)page;
	page_header->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	page_header->pd_upper = SizeOfPageHeaderData;
	memcpy(page_before, page, sizeof(page));
	memset(foreign_page, 0xc3, sizeof(foreign_page));
	memcpy(foreign_before, foreign_page, sizeof(foreign_page));

	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(1, 43, false, &extension, page,
											 foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_BAD_UNDO);
	UT_ASSERT_EQ(extension.build_steps, 0);
	UT_ASSERT(memcmp(page, page_before, sizeof(page)) == 0);
	UT_ASSERT(memcmp(&extension, &extension_before, sizeof(extension)) == 0);
	UT_ASSERT(memcmp(foreign_page, foreign_before, sizeof(foreign_page)) == 0);

	cluster_cr_build_on_holder_forget(1, 43);
}

UT_TEST(test_r4_builder_refuses_resume_without_context)
{
	char page[BLCKSZ];
	char foreign_page[BLCKSZ];
	ClusterR4CrSlotExtension extension = make_builder_extension(51, 83);
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_NONE;

	make_zero_candidate_page(page);
	memset(foreign_page, 0xa5, sizeof(foreign_page));
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(1, 51, true, &extension, page,
											 foreign_page, &reason),
			 CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_PROTOCOL);
}

UT_TEST(test_r4_builder_context_is_isolated_per_physical_slot)
{
	char page0[BLCKSZ];
	char page1[BLCKSZ];
	char foreign_page[BLCKSZ];
	ClusterR4CrSlotExtension extension0 = make_builder_extension(61, 91);
	ClusterR4CrSlotExtension extension1 = make_builder_extension(62, 92);
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;

	make_zero_candidate_page(page0);
	make_zero_candidate_page(page1);
	memset(foreign_page, 0, sizeof(foreign_page));
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(2, 61, false, &extension0, page0,
											 foreign_page, &reason),
			 CLUSTER_R4_CR_STEP_FULL);
	reason = CLUSTER_CR_BUILD_PROTOCOL;
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(3, 62, false, &extension1, page1,
											 foreign_page, &reason),
			 CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	cluster_cr_build_on_holder_forget(2, 61);
	cluster_cr_build_on_holder_forget(3, 62);
}

UT_TEST(test_r4_builder_single_local_insert_inverse_reaches_full)
{
	char page[BLCKSZ];
	char foreign_page[BLCKSZ];
	char foreign_before[BLCKSZ];
	ClusterR4CrSlotExtension extension = make_builder_extension(71, 101);
	ClusterR4CrSlotExtension expected_extension;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;

	make_one_insert_candidate_page(page, &extension);
	expected_extension = extension;
	expected_extension.build_steps = 1;
	memset(foreign_page, 0xa5, sizeof(foreign_page));
	memcpy(foreign_before, foreign_page, sizeof(foreign_page));

	UT_ASSERT(ItemIdIsNormal(PageGetItemId((Page)page, FirstOffsetNumber)));
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(0, 71, false, &extension, page,
											 foreign_page, &reason),
			 CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(ut_undo_get_record_calls, 1);
	UT_ASSERT(!ItemIdIsNormal(PageGetItemId((Page)page, FirstOffsetNumber)));
	UT_ASSERT(memcmp(&extension, &expected_extension, sizeof(extension)) == 0);
	UT_ASSERT(memcmp(foreign_page, foreign_before, sizeof(foreign_page)) == 0);

	reason = CLUSTER_CR_BUILD_NONE;
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(0, 71, false, &extension, page,
											 foreign_page, &reason),
			 CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_PROTOCOL);
	cluster_cr_build_on_holder_forget(0, 71);
}

UT_TEST(test_r4_builder_single_local_update_hot_inverse_reaches_full)
{
	char page[BLCKSZ];
	char expected_old[TEST_TUPLE_LENGTH];
	char foreign_page[BLCKSZ];
	char foreign_before[BLCKSZ];
	ClusterR4CrSlotExtension extension = make_builder_extension(72, 102);
	ClusterR4CrSlotExtension expected_extension;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	ItemId old_item;
	ItemId new_item;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	make_one_update_candidate_page(page, &extension, expected_old);
	expected_extension = extension;
	expected_extension.build_steps = 1;
	memset(foreign_page, 0x5a, sizeof(foreign_page));
	memcpy(foreign_before, foreign_page, sizeof(foreign_page));
	old_item = PageGetItemId((Page)page, FirstOffsetNumber);
	new_item = PageGetItemId((Page)page, OffsetNumberNext(FirstOffsetNumber));

	UT_ASSERT(ItemIdIsNormal(old_item));
	UT_ASSERT(ItemIdIsNormal(new_item));
	UT_ASSERT(memcmp(PageGetItem((Page)page, old_item), expected_old,
				 TEST_TUPLE_LENGTH)
			  != 0);
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(1, 72, false, &extension, page,
											 foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(ut_undo_get_record_calls, 1);
	UT_ASSERT(ItemIdIsNormal(old_item));
	UT_ASSERT(memcmp(PageGetItem((Page)page, old_item), expected_old,
				 TEST_TUPLE_LENGTH)
			  == 0);
	UT_ASSERT(!ItemIdIsNormal(new_item));
	UT_ASSERT(memcmp(&extension, &expected_extension, sizeof(extension)) == 0);
	UT_ASSERT(memcmp(foreign_page, foreign_before, sizeof(foreign_page)) == 0);

	reason = CLUSTER_CR_BUILD_NONE;
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(1, 72, false, &extension, page,
											 foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_PROTOCOL);
	cluster_cr_build_on_holder_forget(1, 72);
}

UT_TEST(test_r4_builder_single_local_delete_inverse_reaches_full)
{
	char page[BLCKSZ];
	char expected_old[TEST_TUPLE_LENGTH];
	char foreign_page[BLCKSZ];
	char foreign_before[BLCKSZ];
	ClusterR4CrSlotExtension extension = make_builder_extension(73, 103);
	ClusterR4CrSlotExtension expected_extension;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	ItemId item;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	make_one_delete_candidate_page(page, &extension, expected_old);
	expected_extension = extension;
	expected_extension.build_steps = 1;
	memset(foreign_page, 0xa5, sizeof(foreign_page));
	memcpy(foreign_before, foreign_page, sizeof(foreign_page));
	item = PageGetItemId((Page)page, FirstOffsetNumber);

	UT_ASSERT(ItemIdIsNormal(item));
	UT_ASSERT(memcmp(PageGetItem((Page)page, item), expected_old,
				 TEST_TUPLE_LENGTH)
			  != 0);
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(2, 73, false, &extension, page,
										 foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(ut_undo_get_record_calls, 1);
	UT_ASSERT(ItemIdIsNormal(item));
	UT_ASSERT(memcmp(PageGetItem((Page)page, item), expected_old,
				 TEST_TUPLE_LENGTH)
			  == 0);
	UT_ASSERT(memcmp(&extension, &expected_extension, sizeof(extension)) == 0);
	UT_ASSERT(memcmp(foreign_page, foreign_before, sizeof(foreign_page)) == 0);

	reason = CLUSTER_CR_BUILD_NONE;
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(2, 73, false, &extension, page,
										 foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_PROTOCOL);
	cluster_cr_build_on_holder_forget(2, 73);
}

UT_TEST(test_r4_builder_single_local_itl_inverse_reaches_full)
{
	char page[BLCKSZ];
	char foreign_page[BLCKSZ];
	char foreign_before[BLCKSZ];
	ClusterR4CrSlotExtension extension = make_builder_extension(74, 104);
	ClusterR4CrSlotExtension expected_extension;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	HeapTupleHeader tuple;
	ClusterItlSlotData *slot;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	make_one_itl_candidate_page(page, &extension);
	expected_extension = extension;
	expected_extension.build_steps = 1;
	memset(foreign_page, 0x4b, sizeof(foreign_page));
	memcpy(foreign_before, foreign_page, sizeof(foreign_page));
	tuple = (HeapTupleHeader)PageGetItem(
		(Page)page, PageGetItemId((Page)page, FirstOffsetNumber));
	slot = &ClusterPageGetItlSlots((Page)page)[0];

	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmax(tuple), TEST_XID);
	UT_ASSERT_EQ(slot->flags, ITL_FLAG_LOCK_ONLY_ACTIVE);
	UT_ASSERT(!UBA_is_invalid(slot->undo_segment_head));
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(3, 74, false, &extension, page,
											 foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(ut_undo_get_record_calls, 1);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmax(tuple), InvalidTransactionId);
	UT_ASSERT_EQ(tuple->t_infomask, HEAP_XMAX_INVALID);
	UT_ASSERT_EQ(tuple->t_infomask2, 0);
	UT_ASSERT_EQ(slot->flags, ITL_FLAG_FREE);
	UT_ASSERT(!SCN_VALID(slot->commit_scn));
	UT_ASSERT(UBA_is_invalid(slot->undo_segment_head));
	UT_ASSERT(memcmp(&extension, &expected_extension, sizeof(extension)) == 0);
	UT_ASSERT(memcmp(foreign_page, foreign_before, sizeof(foreign_page)) == 0);

	cluster_cr_build_on_holder_forget(3, 74);
}

UT_TEST(test_r4_builder_record_at_read_scn_is_normal_horizon_stop)
{
	char page[BLCKSZ];
	char page_before[BLCKSZ];
	char foreign_page[BLCKSZ];
	char foreign_before[BLCKSZ];
	UndoRecordHeader *record = (UndoRecordHeader *)ut_undo_record;
	ClusterR4CrSlotExtension extension = make_builder_extension(82, 112);
	ClusterR4CrSlotExtension expected_extension;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	make_one_itl_candidate_page(page, &extension);
	record->write_scn = extension.route_proof.read_scn;
	memcpy(page_before, page, sizeof(page));
	expected_extension = extension;
	expected_extension.build_steps = 1;
	memset(foreign_page, 0xb4, sizeof(foreign_page));
	memcpy(foreign_before, foreign_page, sizeof(foreign_page));

	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(1, 82, false, &extension, page,
											 foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(ut_undo_get_record_calls, 1);
	UT_ASSERT(memcmp(page, page_before, sizeof(page)) == 0);
	UT_ASSERT(memcmp(&extension, &expected_extension, sizeof(extension)) == 0);
	UT_ASSERT(memcmp(foreign_page, foreign_before, sizeof(foreign_page)) == 0);

	cluster_cr_build_on_holder_forget(1, 82);
}

UT_TEST(test_r4_builder_two_local_updates_walk_newest_to_oldest)
{
	char page[BLCKSZ];
	char expected_oldest[TEST_TUPLE_LENGTH];
	char foreign_page[BLCKSZ];
	char foreign_before[BLCKSZ];
	ClusterR4CrSlotExtension extension = make_builder_extension(74, 104);
	ClusterR4CrSlotExtension expected_extension;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	ItemId oldest_item;
	ItemId middle_item;
	ItemId newest_item;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	make_two_update_candidate_page(page, &extension, expected_oldest);
	expected_extension = extension;
	expected_extension.build_steps = 2;
	memset(foreign_page, 0x96, sizeof(foreign_page));
	memcpy(foreign_before, foreign_page, sizeof(foreign_page));
	oldest_item = PageGetItemId((Page)page, FirstOffsetNumber);
	middle_item = PageGetItemId((Page)page, OffsetNumberNext(FirstOffsetNumber));
	newest_item = PageGetItemId((Page)page,
							  OffsetNumberNext(OffsetNumberNext(FirstOffsetNumber)));

	UT_ASSERT(ItemIdIsNormal(oldest_item));
	UT_ASSERT(ItemIdIsNormal(middle_item));
	UT_ASSERT(ItemIdIsNormal(newest_item));
	UT_ASSERT(memcmp(PageGetItem((Page)page, oldest_item), expected_oldest,
				 TEST_TUPLE_LENGTH)
			  != 0);
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(3, 74, false, &extension, page,
										 foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(ut_undo_get_record_calls, 2);
	UT_ASSERT(ItemIdIsNormal(oldest_item));
	UT_ASSERT(memcmp(PageGetItem((Page)page, oldest_item), expected_oldest,
				 TEST_TUPLE_LENGTH)
			  == 0);
	UT_ASSERT(!ItemIdIsNormal(middle_item));
	UT_ASSERT(!ItemIdIsNormal(newest_item));
	UT_ASSERT(memcmp(&extension, &expected_extension, sizeof(extension)) == 0);
	UT_ASSERT(memcmp(foreign_page, foreign_before, sizeof(foreign_page)) == 0);

	reason = CLUSTER_CR_BUILD_NONE;
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(3, 74, false, &extension, page,
										 foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_PROTOCOL);
	cluster_cr_build_on_holder_forget(3, 74);
}

UT_TEST(test_r4_builder_two_local_candidate_xids_use_write_scn_desc)
{
	char page[BLCKSZ];
	char expected_oldest[TEST_TUPLE_LENGTH];
	char foreign_page[BLCKSZ];
	char foreign_before[BLCKSZ];
	ClusterR4CrSlotExtension extension = make_builder_extension(75, 105);
	ClusterR4CrSlotExtension expected_extension;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	ItemId oldest_item;
	ItemId middle_item;
	ItemId newest_item;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	make_two_xid_update_candidate_page(page, &extension, expected_oldest);
	expected_extension = extension;
	expected_extension.build_steps = 2;
	memset(foreign_page, 0x69, sizeof(foreign_page));
	memcpy(foreign_before, foreign_page, sizeof(foreign_page));
	oldest_item = PageGetItemId((Page)page, FirstOffsetNumber);
	middle_item = PageGetItemId((Page)page, OffsetNumberNext(FirstOffsetNumber));
	newest_item = PageGetItemId(
		(Page)page, OffsetNumberNext(OffsetNumberNext(FirstOffsetNumber)));

	UT_ASSERT(ItemIdIsNormal(oldest_item));
	UT_ASSERT(ItemIdIsNormal(middle_item));
	UT_ASSERT(ItemIdIsNormal(newest_item));
	UT_ASSERT(memcmp(PageGetItem((Page)page, oldest_item), expected_oldest,
				 TEST_TUPLE_LENGTH)
			  != 0);
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(0, 75, false, &extension, page,
										 foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(ut_undo_get_record_calls, 2);
	UT_ASSERT(ItemIdIsNormal(oldest_item));
	UT_ASSERT(memcmp(PageGetItem((Page)page, oldest_item), expected_oldest,
				 TEST_TUPLE_LENGTH)
			  == 0);
	UT_ASSERT(!ItemIdIsNormal(middle_item));
	UT_ASSERT(!ItemIdIsNormal(newest_item));
	UT_ASSERT(memcmp(&extension, &expected_extension, sizeof(extension)) == 0);
	UT_ASSERT(memcmp(foreign_page, foreign_before, sizeof(foreign_page)) == 0);

	reason = CLUSTER_CR_BUILD_NONE;
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(0, 75, false, &extension, page,
										 foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_PROTOCOL);
	cluster_cr_build_on_holder_forget(0, 75);
}

UT_TEST(test_r4_builder_foreign_head_freezes_one_need_undo)
{
	char page[BLCKSZ];
	char page_before[BLCKSZ];
	char expected_old[TEST_TUPLE_LENGTH];
	char foreign_page[BLCKSZ];
	char foreign_before[BLCKSZ];
	ClusterR4CrSlotExtension extension = make_builder_extension(76, 106);
	ClusterR4CrSlotExtension expected_extension;
	ClusterR4CrSlotExtension extension_after_pause;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	ClusterR4CrBuildStepResult step_result;
	ClusterTxLocator expected_locator;
	ClusterTxLocator pending_locator;
	UBA foreign_uba;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	extension.route_proof.formation_epoch = 19;
	foreign_uba
		= make_one_foreign_update_candidate_page(page, &extension, expected_old);
	UT_ASSERT_EQ(foreign_uba.raw[0], UINT64CONST(0x0000000900000101));
	UT_ASSERT_EQ(foreign_uba.raw[1], UINT64CONST(0x0000000000060003));

	expected_extension = extension;
	expected_extension.foreign_request_id = (UINT64CONST(76) << 18) | 1;
	expected_extension.foreign_uba = foreign_uba;
	expected_extension.origin_formation_epoch = UINT64CONST(19);
	expected_extension.foreign_origin_node = 1;
	expected_extension.foreign_segment_id = 257;
	expected_extension.foreign_block_no = 9;
	expected_extension.foreign_xid = TEST_XID;
	expected_extension.foreign_wrap = TT_WRAP_INVALID;
	expected_extension.build_steps = 1;
	expected_extension.foreign_tt_slot_offset = TEST_TT_OFFSET;
	expected_extension.foreign_row_offset = 6;
	memcpy(page_before, page, sizeof(page));
	memset(foreign_page, 0x3c, sizeof(foreign_page));
	memcpy(foreign_before, foreign_page, sizeof(foreign_page));

	step_result = cluster_cr_build_on_holder_step(1, 76, false, &extension, page,
											  foreign_page, &reason);
	UT_ASSERT_EQ(step_result, CLUSTER_R4_CR_STEP_NEED_UNDO);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(ut_undo_get_record_calls, 0);
	UT_ASSERT(memcmp(page, page_before, sizeof(page)) == 0);
	UT_ASSERT(memcmp(foreign_page, foreign_before, sizeof(foreign_page)) == 0);
	UT_ASSERT(memcmp(&extension, &expected_extension, sizeof(extension)) == 0);

	memset(&expected_locator, 0, sizeof(expected_locator));
	expected_locator.uba = foreign_uba;
	expected_locator.xid = TEST_XID;
	expected_locator.tt_wrap = TT_WRAP_INVALID;
	expected_locator.itl_kind = ITL_FLAG_ACTIVE;
	expected_locator.itl_slot_index = 0;
	memset(&pending_locator, 0, sizeof(pending_locator));
	UT_ASSERT(cluster_cr_build_on_holder_pending_locator(
		1, 76, &pending_locator));
	UT_ASSERT_EQ(memcmp(&pending_locator, &expected_locator,
					 sizeof(expected_locator)), 0);
	UT_ASSERT(!cluster_cr_build_on_holder_pending_locator(
		1, 77, &pending_locator));

	extension_after_pause = extension;
	reason = CLUSTER_CR_BUILD_NONE;
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(1, 76, false, &extension, page,
											 foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_PROTOCOL);
	UT_ASSERT_EQ(ut_undo_get_record_calls, 0);
	UT_ASSERT(memcmp(page, page_before, sizeof(page)) == 0);
	UT_ASSERT(memcmp(foreign_page, foreign_before, sizeof(foreign_page)) == 0);
	UT_ASSERT(memcmp(&extension, &extension_after_pause, sizeof(extension)) == 0);
	cluster_cr_build_on_holder_forget(1, 76);
	UT_ASSERT(!cluster_cr_build_on_holder_pending_locator(
		1, 76, &pending_locator));
}

/* Both records live on the same foreign origin.  The first delivered block
 * advances to a second foreign UBA; the second delivery can validate that
 * tail only with the process-local immediate predecessor saved across the
 * pause.  Each pending record is charged once, never once per resume call. */
UT_TEST(test_r4_builder_foreign_resume_preserves_predecessor_and_step_count)
{
	char page[BLCKSZ];
	char expected_oldest[TEST_TUPLE_LENGTH];
	char head_block[BLCKSZ];
	char tail_block[BLCKSZ];
	char foreign_page[BLCKSZ];
	ClusterR4CrSlotExtension extension = make_builder_extension(78, 108);
	ClusterR4CrSlotExtension expected_extension;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	ClusterTxLocator pending_locator;
	UBA head_uba;
	UBA tail_uba;
	ItemId oldest_item;
	ItemId middle_item;
	ItemId newest_item;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	extension.route_proof.formation_epoch = 20;
	make_two_foreign_update_candidate_page(
		page, &extension, expected_oldest, head_block, tail_block,
		&head_uba, &tail_uba);
	expected_extension = extension;
	expected_extension.build_steps = 2;
	oldest_item = PageGetItemId((Page)page, FirstOffsetNumber);
	middle_item = PageGetItemId((Page)page, OffsetNumberNext(FirstOffsetNumber));
	newest_item = PageGetItemId(
		(Page)page, OffsetNumberNext(OffsetNumberNext(FirstOffsetNumber)));
	memset(foreign_page, 0xa7, sizeof(foreign_page));

	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(
		3, 78, false, &extension, page, foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_NEED_UNDO);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(extension.build_steps, 1);
	UT_ASSERT_EQ(extension.foreign_request_id, (UINT64CONST(78) << 18) | 3);
	UT_ASSERT(extension.foreign_uba.raw[0] == head_uba.raw[0]
			  && extension.foreign_uba.raw[1] == head_uba.raw[1]);
	memset(&pending_locator, 0, sizeof(pending_locator));
	UT_ASSERT(cluster_cr_build_on_holder_pending_locator(
		3, 78, &pending_locator));
	UT_ASSERT(pending_locator.uba.raw[0] == head_uba.raw[0]
			  && pending_locator.uba.raw[1] == head_uba.raw[1]);

	memcpy(foreign_page, head_block, sizeof(foreign_page));
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(
		3, 78, true, &extension, page, foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_NEED_UNDO);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(extension.build_steps, 2);
	UT_ASSERT_EQ(extension.foreign_request_id, (UINT64CONST(78) << 18) | 7);
	UT_ASSERT(extension.foreign_uba.raw[0] == tail_uba.raw[0]
			  && extension.foreign_uba.raw[1] == tail_uba.raw[1]);
	memset(&pending_locator, 0, sizeof(pending_locator));
	UT_ASSERT(cluster_cr_build_on_holder_pending_locator(
		3, 78, &pending_locator));
	UT_ASSERT(pending_locator.uba.raw[0] == tail_uba.raw[0]
			  && pending_locator.uba.raw[1] == tail_uba.raw[1]);

	memcpy(foreign_page, tail_block, sizeof(foreign_page));
	{
		char before_late[BLCKSZ];
		uint64 exact_id = extension.foreign_request_id;

		memcpy(before_late, page, BLCKSZ);
		extension.foreign_request_id = (UINT64CONST(78) << 18) | 3;
		UT_ASSERT_EQ(
			cluster_cr_build_on_holder_step(3, 78, true, &extension, page, foreign_page, &reason),
			CLUSTER_R4_CR_STEP_FAIL);
		UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_PROTOCOL);
		UT_ASSERT(memcmp(before_late, page, BLCKSZ) == 0);
		extension.foreign_request_id = exact_id;
	}
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(
		3, 78, true, &extension, page, foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(ut_undo_get_record_calls, 0);
	UT_ASSERT_EQ(memcmp(&extension, &expected_extension, sizeof(extension)), 0);
	UT_ASSERT(ItemIdIsNormal(oldest_item));
	UT_ASSERT_EQ(memcmp(PageGetItem((Page)page, oldest_item), expected_oldest,
					 TEST_TUPLE_LENGTH),
				 0);
	UT_ASSERT(!ItemIdIsNormal(middle_item));
	UT_ASSERT(!ItemIdIsNormal(newest_item));
	UT_ASSERT_EQ(memcmp(foreign_page, tail_block, sizeof(foreign_page)), 0);
	UT_ASSERT(!cluster_cr_build_on_holder_pending_locator(
		3, 78, &pending_locator));
	cluster_cr_build_on_holder_forget(3, 78);
}

UT_TEST(test_r4_builder_foreign_resume_accepts_unaligned_record_offset)
{
	char page[BLCKSZ];
	char expected_old[TEST_TUPLE_LENGTH];
	PGAlignedBlock foreign_block;
	ClusterItlSlotData *slot;
	UndoRecordHeader *record = (UndoRecordHeader *)ut_undo_record;
	ClusterR4CrSlotExtension extension = make_builder_extension(79, 109);
	ClusterR4CrSlotExtension expected_extension;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	UBA foreign_uba;
	ItemId old_item;
	ItemId replacement_item;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	extension.route_proof.formation_epoch = 21;
	(void)make_one_foreign_update_candidate_page(page, &extension,
											 expected_old);
	foreign_uba = make_record_uba_in_segment(257, 9, 1);
	slot = &ClusterPageGetItlSlots((Page)page)[0];
	slot->undo_segment_head = foreign_uba;
	record->origin_node_id = 1;
	record->tt_slot_segment_id = 257;
	make_unaligned_second_record_undo_block(
		foreign_block.data, ut_undo_record, TEST_UNDO_RECORD_CAPACITY);
	expected_extension = extension;
	expected_extension.build_steps = 1;
	old_item = PageGetItemId((Page)page, FirstOffsetNumber);
	replacement_item
		= PageGetItemId((Page)page, OffsetNumberNext(FirstOffsetNumber));

	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(
		0, 79, false, &extension, page, foreign_block.data, &reason),
				 CLUSTER_R4_CR_STEP_NEED_UNDO);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(extension.foreign_row_offset, 1);
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(
		0, 79, true, &extension, page, foreign_block.data, &reason),
				 CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(ut_undo_get_record_calls, 0);
	UT_ASSERT_EQ(memcmp(&extension, &expected_extension, sizeof(extension)), 0);
	UT_ASSERT(ItemIdIsNormal(old_item));
	UT_ASSERT_EQ(memcmp(PageGetItem((Page)page, old_item), expected_old,
					 TEST_TUPLE_LENGTH),
				 0);
	UT_ASSERT(!ItemIdIsNormal(replacement_item));
	cluster_cr_build_on_holder_forget(0, 79);
}

UT_TEST(test_r4_builder_transaction_global_chain_skips_other_block)
{
	char page[BLCKSZ];
	char expected_oldest[TEST_TUPLE_LENGTH];
	char foreign_page[BLCKSZ];
	char foreign_before[BLCKSZ];
	ClusterR4CrSlotExtension extension = make_builder_extension(77, 107);
	ClusterR4CrSlotExtension expected_extension;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	ItemId oldest_item;
	ItemId middle_item;
	ItemId newest_item;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	make_target_other_target_update_candidate_page(page, &extension,
											expected_oldest);
	expected_extension = extension;
	expected_extension.build_steps = 3;
	memset(foreign_page, 0x87, sizeof(foreign_page));
	memcpy(foreign_before, foreign_page, sizeof(foreign_page));
	oldest_item = PageGetItemId((Page)page, FirstOffsetNumber);
	middle_item = PageGetItemId((Page)page, OffsetNumberNext(FirstOffsetNumber));
	newest_item = PageGetItemId(
		(Page)page, OffsetNumberNext(OffsetNumberNext(FirstOffsetNumber)));

	UT_ASSERT(ItemIdIsNormal(oldest_item));
	UT_ASSERT(ItemIdIsNormal(middle_item));
	UT_ASSERT(ItemIdIsNormal(newest_item));
	UT_ASSERT(memcmp(PageGetItem((Page)page, oldest_item), expected_oldest,
				 TEST_TUPLE_LENGTH)
			  != 0);
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(2, 77, false, &extension, page,
											 foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(ut_undo_get_record_calls, 3);
	UT_ASSERT(ItemIdIsNormal(oldest_item));
	UT_ASSERT(memcmp(PageGetItem((Page)page, oldest_item), expected_oldest,
				 TEST_TUPLE_LENGTH)
			  == 0);
	UT_ASSERT(!ItemIdIsNormal(middle_item));
	UT_ASSERT(!ItemIdIsNormal(newest_item));
	UT_ASSERT(memcmp(&extension, &expected_extension, sizeof(extension)) == 0);
	UT_ASSERT(memcmp(foreign_page, foreign_before, sizeof(foreign_page)) == 0);
	ut_three_record_sequence = false;
	cluster_cr_build_on_holder_forget(2, 77);
}

UT_TEST(test_r4_builder_rejects_malformed_off_target_relation_before_filter)
{
	char page[BLCKSZ];
	char expected_oldest[TEST_TUPLE_LENGTH];
	char foreign_page[BLCKSZ];
	char foreign_before[BLCKSZ];
	UndoRecordHeader *middle = (UndoRecordHeader *)ut_second_undo_record;
	ClusterR4CrSlotExtension extension = make_builder_extension(80, 110);
	ClusterR4CrSlotExtension expected_extension;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_NONE;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	make_target_other_target_update_candidate_page(page, &extension,
											expected_oldest);
	middle->target_locator.relNumber = InvalidRelFileNumber;
	expected_extension = extension;
	expected_extension.build_steps = 2;
	memset(foreign_page, 0x78, sizeof(foreign_page));
	memcpy(foreign_before, foreign_page, sizeof(foreign_page));

	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(3, 80, false, &extension, page,
											 foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_BAD_UNDO);
	UT_ASSERT_EQ(ut_undo_get_record_calls, 2);
	UT_ASSERT(memcmp(&extension, &expected_extension, sizeof(extension)) == 0);
	UT_ASSERT(memcmp(foreign_page, foreign_before, sizeof(foreign_page)) == 0);

	ut_three_record_sequence = false;
	cluster_cr_build_on_holder_forget(3, 80);
}

UT_TEST(test_r4_builder_rejects_off_target_itl_invalid_slot_before_filter)
{
	char page[BLCKSZ];
	char expected_oldest[TEST_TUPLE_LENGTH];
	char foreign_page[BLCKSZ];
	char foreign_before[BLCKSZ];
	UndoRecordHeader *middle = (UndoRecordHeader *)ut_second_undo_record;
	UndoItlPayload *payload
		= (UndoItlPayload *)(ut_second_undo_record + sizeof(UndoRecordHeader));
	ClusterR4CrSlotExtension extension = make_builder_extension(81, 111);
	ClusterR4CrSlotExtension expected_extension;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_NONE;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	make_target_other_target_update_candidate_page(page, &extension,
											expected_oldest);
	middle->record_type = UNDO_RECORD_ITL;
	middle->payload_length = sizeof(*payload);
	memset(payload, 0, sizeof(*payload));
	payload->itl_slot_idx = CLUSTER_ITL_INITRANS_DEFAULT;
	ut_second_undo_record_length = sizeof(UndoRecordHeader) + sizeof(*payload);
	expected_extension = extension;
	expected_extension.build_steps = 2;
	memset(foreign_page, 0x69, sizeof(foreign_page));
	memcpy(foreign_before, foreign_page, sizeof(foreign_page));

	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(0, 81, false, &extension, page,
											 foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_BAD_UNDO);
	UT_ASSERT_EQ(ut_undo_get_record_calls, 2);
	UT_ASSERT(memcmp(&extension, &expected_extension, sizeof(extension)) == 0);
	UT_ASSERT(memcmp(foreign_page, foreign_before, sizeof(foreign_page)) == 0);

	ut_three_record_sequence = false;
	cluster_cr_build_on_holder_forget(0, 81);
}

UT_TEST(test_r4_builder_cross_segment_cycle_fails_before_repeat_fetch)
{
	char page[BLCKSZ];
	char page_before[BLCKSZ];
	char foreign_page[BLCKSZ];
	char foreign_before[BLCKSZ];
	ClusterItlSlotData *slot;
	UndoRecordHeader *first = (UndoRecordHeader *)ut_undo_record;
	UndoRecordHeader *second = (UndoRecordHeader *)ut_second_undo_record;
	UBA first_uba = make_record_uba_in_segment(1, 10, 2);
	UBA second_uba = make_record_uba_in_segment(2, 9, 2);
	ClusterR4CrSlotExtension extension = make_builder_extension(83, 113);
	ClusterR4CrSlotExtension expected_extension;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_NONE;

	make_one_insert_candidate_page(page, &extension);
	slot = &ClusterPageGetItlSlots((Page)page)[0];
	slot->undo_segment_head = first_uba;
	first->flags = 0;
	first->write_scn = (SCN)300;
	first->prev_uba = second_uba;
	first->target_block = extension.route_proof.tag.blockNum + 1;
	memcpy(ut_second_undo_record, ut_undo_record, ut_undo_record_length);
	second->write_scn = (SCN)200;
	second->prev_uba = first_uba;
	ut_expected_record_uba = first_uba;
	ut_expected_second_record_uba = second_uba;
	ut_second_undo_record_length = ut_undo_record_length;
	ut_two_record_sequence = true;
	ut_three_record_sequence = false;
	ut_cycle_record_sequence = true;
	ut_undo_get_record_calls = 0;
	cluster_cr_chain_walk_max_steps = 64;
	memcpy(page_before, page, sizeof(page));
	expected_extension = extension;
	expected_extension.build_steps = 3;
	memset(foreign_page, 0x78, sizeof(foreign_page));
	memcpy(foreign_before, foreign_page, sizeof(foreign_page));

	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(2, 83, false, &extension, page,
											 foreign_page, &reason),
				 CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_BAD_UNDO);
	UT_ASSERT_EQ(ut_undo_get_record_calls, 2);
	UT_ASSERT(memcmp(page, page_before, sizeof(page)) == 0);
	UT_ASSERT(memcmp(&extension, &expected_extension, sizeof(extension)) == 0);
	UT_ASSERT(memcmp(foreign_page, foreign_before, sizeof(foreign_page)) == 0);

	cluster_cr_chain_walk_max_steps = 4096;
	ut_cycle_record_sequence = false;
	ut_two_record_sequence = false;
	cluster_cr_build_on_holder_forget(2, 83);
}

/* The ordinary heap producer records the replacement TID; it is not a
 * legacy in-place record with both successor fields unset. */
UT_TEST(test_r4_builder_ordinary_update_successor_pair_reaches_full)
{
	char page[BLCKSZ];
	char expected_old[TEST_TUPLE_LENGTH];
	char foreign_page[BLCKSZ];
	ClusterR4CrSlotExtension extension = make_builder_extension(84, 114);
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	UndoUpdatePayload *payload = (UndoUpdatePayload *)(ut_undo_record + sizeof(UndoRecordHeader));
	ClusterR4CrBuildStepResult result;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	make_one_update_candidate_page(page, &extension, expected_old);
	payload->new_block = 37;
	payload->new_offset = 2;
	memset(foreign_page, 0, sizeof(foreign_page));

	result = cluster_cr_build_on_holder_step(0, 84, false, &extension, page, foreign_page, &reason);
	cluster_cr_build_on_holder_forget(0, 84);
	UT_ASSERT_EQ(result, CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(ut_undo_get_record_calls, 1);
	UT_ASSERT(memcmp(PageGetItem((Page)page, PageGetItemId((Page)page, 1)), expected_old,
					 TEST_TUPLE_LENGTH)
			  == 0);
	UT_ASSERT(!ItemIdIsNormal(PageGetItemId((Page)page, 2)));
}

UT_TEST(test_r4_builder_cross_page_update_does_not_fetch_successor)
{
	char page[BLCKSZ];
	char expected_old[TEST_TUPLE_LENGTH];
	char foreign_page[BLCKSZ];
	char foreign_before[BLCKSZ];
	ClusterR4CrSlotExtension extension = make_builder_extension(85, 115);
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	UndoUpdatePayload *payload = (UndoUpdatePayload *)(ut_undo_record + sizeof(UndoRecordHeader));
	HeapTupleHeader old_current;
	ClusterR4CrBuildStepResult result;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	make_one_update_candidate_page(page, &extension, expected_old);
	payload->new_block = 38;
	payload->new_offset = 2;
	old_current = (HeapTupleHeader)PageGetItem((Page)page, PageGetItemId((Page)page, 1));
	ItemPointerSet(&old_current->t_ctid, 38, 2);
	ItemIdSetUnused(PageGetItemId((Page)page, 2));
	memset(foreign_page, 0xad, sizeof(foreign_page));
	memcpy(foreign_before, foreign_page, sizeof(foreign_page));
	result = cluster_cr_build_on_holder_step(0, 85, false, &extension, page, foreign_page, &reason);
	cluster_cr_build_on_holder_forget(0, 85);
	UT_ASSERT_EQ(result, CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(ut_undo_get_record_calls, 1);
	UT_ASSERT(memcmp(PageGetItem((Page)page, PageGetItemId((Page)page, 1)), expected_old,
					 TEST_TUPLE_LENGTH)
			  == 0);
	UT_ASSERT(memcmp(foreign_page, foreign_before, sizeof(foreign_page)) == 0);
}

UT_TEST(test_r4_builder_successor_page_skips_old_target_inverse)
{
	char page[BLCKSZ];
	char expected_old[TEST_TUPLE_LENGTH];
	char foreign_page[BLCKSZ] = { 0 };
	ClusterR4CrSlotExtension extension = make_builder_extension(86, 116);
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	UndoUpdatePayload *payload = (UndoUpdatePayload *)(ut_undo_record + sizeof(UndoRecordHeader));
	ClusterR4CrBuildStepResult result;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	make_one_update_candidate_page(page, &extension, expected_old);
	payload->new_block = 38;
	payload->new_offset = 1;
	/* The query page is the replacement page; the record still names old37/1. */
	extension.route_proof.tag.blockNum = 38;
	*PageGetItemId((Page)page, 1) = *PageGetItemId((Page)page, 2);
	((PageHeader)page)->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	result = cluster_cr_build_on_holder_step(0, 86, false, &extension, page, foreign_page, &reason);
	cluster_cr_build_on_holder_forget(0, 86);
	UT_ASSERT_EQ(result, CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(ut_undo_get_record_calls, 1);
	UT_ASSERT(!ItemIdIsNormal(PageGetItemId((Page)page, 1)));
}

UT_TEST(test_r4_builder_update_pair_horizon_does_not_restore_old_image)
{
	char page[BLCKSZ];
	char expected_old[TEST_TUPLE_LENGTH];
	char target_before[TEST_TUPLE_LENGTH];
	char foreign_page[BLCKSZ] = { 0 };
	ClusterR4CrSlotExtension extension = make_builder_extension(87, 117);
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	UndoRecordHeader *record = (UndoRecordHeader *)ut_undo_record;
	ClusterR4CrBuildStepResult result;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	make_one_update_candidate_page(page, &extension, expected_old);
	record->write_scn = extension.route_proof.read_scn;
	memcpy(target_before, PageGetItem((Page)page, PageGetItemId((Page)page, 1)), TEST_TUPLE_LENGTH);
	result = cluster_cr_build_on_holder_step(0, 87, false, &extension, page, foreign_page, &reason);
	cluster_cr_build_on_holder_forget(0, 87);
	UT_ASSERT_EQ(result, CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(ut_undo_get_record_calls, 1);
	UT_ASSERT(memcmp(PageGetItem((Page)page, PageGetItemId((Page)page, 1)), target_before,
					 TEST_TUPLE_LENGTH)
			  == 0);
}

UT_TEST(test_r4_builder_update_payload_rejects_incomplete_or_corrupt_shapes)
{
	int case_index;

	for (case_index = 0; case_index < 10; case_index++) {
		char page[BLCKSZ];
		char before[BLCKSZ];
		char expected_old[TEST_TUPLE_LENGTH];
		char foreign_page[BLCKSZ] = { 0 };
		ClusterR4CrSlotExtension extension = make_builder_extension(88, 118);
		ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
		UndoRecordHeader *record = (UndoRecordHeader *)ut_undo_record;
		UndoUpdatePayload *payload
			= (UndoUpdatePayload *)(ut_undo_record + sizeof(UndoRecordHeader));
		HeapTupleHeader old_image = (HeapTupleHeader)((char *)payload + sizeof(*payload));
		ClusterR4CrBuildStepResult result;

		extension.route_proof.tag.spcOid = 1663;
		extension.route_proof.tag.dbOid = 5;
		extension.route_proof.tag.relNumber = 20000;
		extension.route_proof.tag.forkNum = MAIN_FORKNUM;
		extension.route_proof.tag.blockNum = 37;
		make_one_update_candidate_page(page, &extension, expected_old);
		switch (case_index) {
		case 0:
			payload->new_offset = InvalidOffsetNumber;
			break;
		case 1:
			payload->new_block = InvalidBlockNumber;
			break;
		case 2:
			payload->new_offset = MaxHeapTuplesPerPage + 1;
			break;
		case 3:
			payload->new_offset = record->target_offset;
			break;
		case 4:
			payload->flags = 1;
			break;
		case 5:
			payload->old_tuple_offset++;
			break;
		case 6:
			payload->old_tuple_length = SizeofHeapTupleHeader - 1;
			record->payload_length = sizeof(*payload) + payload->old_tuple_length;
			ut_undo_record_length = sizeof(*record) + record->payload_length;
			break;
		case 7:
			old_image->t_hoff = SizeofHeapTupleHeader - 1;
			break;
		case 8:
			old_image->t_hoff = TEST_TUPLE_LENGTH + 1;
			break;
		case 9:
			payload->new_block = InvalidBlockNumber;
			payload->new_offset = InvalidOffsetNumber;
			payload->flags = 1;
			break;
		}
		memcpy(before, page, sizeof(page));
		result = cluster_cr_build_on_holder_step(0, 88, false, &extension, page, foreign_page,
												 &reason);
		cluster_cr_build_on_holder_forget(0, 88);
		UT_ASSERT_EQ(result, CLUSTER_R4_CR_STEP_FAIL);
		UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_BAD_UNDO);
		UT_ASSERT(memcmp(page, before, sizeof(page)) == 0);
	}
}

UT_TEST(test_r4_builder_update_pair_keeps_foreign_occupant_identity_guard)
{
	char page[BLCKSZ];
	char before[BLCKSZ];
	char expected_old[TEST_TUPLE_LENGTH];
	char foreign_page[BLCKSZ] = { 0 };
	ClusterR4CrSlotExtension extension = make_builder_extension(89, 119);
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	HeapTupleHeader occupant;
	ClusterR4CrBuildStepResult result;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	make_one_update_candidate_page(page, &extension, expected_old);
	occupant = (HeapTupleHeader)PageGetItem((Page)page, PageGetItemId((Page)page, 1));
	HeapTupleHeaderSetXmin(occupant, TEST_XID + 999);
	memcpy(before, page, sizeof(page));
	result = cluster_cr_build_on_holder_step(0, 89, false, &extension, page, foreign_page, &reason);
	cluster_cr_build_on_holder_forget(0, 89);
	UT_ASSERT_EQ(result, CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_BAD_UNDO);
	UT_ASSERT(memcmp(page, before, sizeof(page)) == 0);
}

UT_TEST(test_r4_builder_legacy_update_still_restores_exact_old_image)
{
	char page[BLCKSZ];
	char expected_old[TEST_TUPLE_LENGTH];
	char foreign_page[BLCKSZ] = { 0 };
	ClusterR4CrSlotExtension extension = make_builder_extension(90, 120);
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	UndoUpdatePayload *payload = (UndoUpdatePayload *)(ut_undo_record + sizeof(UndoRecordHeader));
	ClusterR4CrBuildStepResult result;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	make_one_update_candidate_page(page, &extension, expected_old);
	payload->new_block = InvalidBlockNumber;
	payload->new_offset = InvalidOffsetNumber;
	result = cluster_cr_build_on_holder_step(0, 90, false, &extension, page, foreign_page, &reason);
	cluster_cr_build_on_holder_forget(0, 90);
	UT_ASSERT_EQ(result, CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT(memcmp(PageGetItem((Page)page, PageGetItemId((Page)page, 1)), expected_old,
					 TEST_TUPLE_LENGTH)
			  == 0);
}

UT_TEST(test_r4_builder_successor_tid_is_not_unrelated_tuple_delete_authority)
{
	char page[BLCKSZ];
	char expected_old[TEST_TUPLE_LENGTH];
	char occupant_before[TEST_TUPLE_LENGTH];
	char foreign_page[BLCKSZ] = { 0 };
	ClusterR4CrSlotExtension extension = make_builder_extension(91, 121);
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	HeapTupleHeader occupant;
	ClusterR4CrBuildStepResult result;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	make_one_update_candidate_page(page, &extension, expected_old);
	occupant = (HeapTupleHeader)PageGetItem((Page)page, PageGetItemId((Page)page, 2));
	HeapTupleHeaderSetXmin(occupant, TEST_XID + 999);
	memcpy(occupant_before, occupant, TEST_TUPLE_LENGTH);
	result = cluster_cr_build_on_holder_step(0, 91, false, &extension, page, foreign_page, &reason);
	cluster_cr_build_on_holder_forget(0, 91);
	UT_ASSERT_EQ(result, CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT(memcmp(PageGetItem((Page)page, PageGetItemId((Page)page, 1)), expected_old,
					 TEST_TUPLE_LENGTH)
			  == 0);
	UT_ASSERT(ItemIdIsNormal(PageGetItemId((Page)page, 2)));
	UT_ASSERT(memcmp(occupant, occupant_before, TEST_TUPLE_LENGTH) == 0);
}

static void
check_independent_record_generation(bool foreign, uint16 page_wrap, uint16 tt_wrap, bool landed,
									int corrupt)
{
	PGAlignedBlock page, before, foreign_page;
	char expected_old[TEST_TUPLE_LENGTH];
	ClusterR4CrSlotExtension extension = make_builder_extension(192, 115);
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	ClusterR4CrBuildStepResult result;
	UndoRecordHeader *record = (UndoRecordHeader *)ut_undo_record;
	ClusterItlSlotData *slot;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	make_one_update_candidate_page(page.data, &extension, expected_old);
	slot = &ClusterPageGetItlSlots((Page)page.data)[0];
	slot->wrap = page_wrap;
	record->tt_wrap_plus1 = tt_wrap + 1;
	if (foreign) {
		slot->undo_segment_head = uba_encode(257, 9, TEST_TT_OFFSET, 0);
		record->origin_node_id = 1;
		record->tt_slot_segment_id = 257;
	}
	if (corrupt == 1)
		record->tt_wrap_plus1 = 0;
	else if (corrupt == 2)
		record->xid++;
	else if (corrupt == 3)
		record->tt_slot_id++;
	else if (corrupt == 4)
		record->origin_node_id ^= 1;
	make_single_record_undo_block(foreign_page.data, ut_undo_record, ut_undo_record_length);
	memcpy(before.data, page.data, BLCKSZ);
	result = cluster_cr_build_on_holder_step(0, 192, false, &extension, page.data,
											 foreign_page.data, &reason);
	if (foreign) {
		UT_ASSERT_EQ(result, CLUSTER_R4_CR_STEP_NEED_UNDO);
		UT_ASSERT_EQ(extension.foreign_wrap, TT_WRAP_INVALID);
		if (landed)
			extension.foreign_wrap = tt_wrap + (corrupt == 5 ? 1 : 0);
		result = cluster_cr_build_on_holder_step(0, 192, true, &extension, page.data,
												 foreign_page.data, &reason);
	}
	cluster_cr_build_on_holder_forget(0, 192);
	if (corrupt) {
		UT_ASSERT_EQ(result, CLUSTER_R4_CR_STEP_FAIL);
		UT_ASSERT(memcmp(page.data, before.data, BLCKSZ) == 0);
	} else {
		UT_ASSERT_EQ(result, CLUSTER_R4_CR_STEP_FULL);
		UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_NONE);
		UT_ASSERT(memcmp(PageGetItem((Page)page.data, PageGetItemId((Page)page.data, 1)),
						 expected_old, TEST_TUPLE_LENGTH)
				  == 0);
		UT_ASSERT(!ItemIdIsNormal(PageGetItemId((Page)page.data, 2)));
		UT_ASSERT_EQ(ClusterPageGetItlSlots((Page)page.data)[0].wrap, page_wrap);
	}
}

UT_TEST(test_r4_builder_local_page_wrap_is_not_tt_generation)
{
	check_independent_record_generation(false, 1, 0, false, 0);
	check_independent_record_generation(false, 0, TEST_WRAP, false, 0);
	check_independent_record_generation(false, TT_WRAP_MAX, 0, false, 0);
}

UT_TEST(test_r4_builder_foreign_wrap_canonicalizes_once_after_landing)
{
	check_independent_record_generation(true, 1, 0, false, 0);
	check_independent_record_generation(true, 0, TEST_WRAP, true, 0);
	check_independent_record_generation(true, TT_WRAP_MAX, 0, true, 0);
}

UT_TEST(test_r4_builder_partial_locator_keeps_record_identity_refusals)
{
	int corrupt;
	for (corrupt = 1; corrupt <= 4; corrupt++) {
		check_independent_record_generation(false, 1, 0, false, corrupt);
		check_independent_record_generation(true, 1, 0, true, corrupt);
	}
}

UT_TEST(test_r4_builder_known_landed_wrap_must_match_exact_record)
{
	check_independent_record_generation(true, 1, 0, true, 5);
}

UT_TEST(test_r4_dependency_id_is_bounded_and_does_not_alias_next_record)
{
	uint32 step;
	uint64 previous = 0;

	for (step = 1; step <= 65536; step++) {
		uint64 id = cluster_cr_r4_dependency_request_id(3, 7, step);
		UT_ASSERT(id > previous);
		UT_ASSERT_EQ(id & 3, 3);
		UT_ASSERT(id < cluster_cr_r4_dependency_request_id(0, 8, 1));
		previous = id;
	}
	UT_ASSERT_EQ(cluster_cr_r4_dependency_request_id(4, 1, 1), 0);
	UT_ASSERT_EQ(cluster_cr_r4_dependency_request_id(0, 0, 1), 0);
	UT_ASSERT_EQ(cluster_cr_r4_dependency_request_id(0, (UINT64_MAX >> 18) + 1, 1), 0);
	UT_ASSERT_EQ(cluster_cr_r4_dependency_request_id(0, 1, 0), 0);
	UT_ASSERT_EQ(cluster_cr_r4_dependency_request_id(0, 1, 65537), 0);
	UT_ASSERT_EQ(cluster_cr_r4_dependency_request_id(3, UINT64_MAX >> 18, 65536), UINT64_MAX);
}

UT_TEST(test_r4_canonical_generation_does_not_change_on_foreign_predecessor)
{
	PGAlignedBlock page, head, tail;
	char expected_old[TEST_TUPLE_LENGTH];
	UBA head_uba, tail_uba;
	ClusterR4CrSlotExtension extension = make_builder_extension(193, 115);
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	UndoSlotDirEntry *tail_slot;
	UndoRecordHeader *tail_record;

	extension.route_proof.tag.spcOid = 1663;
	extension.route_proof.tag.dbOid = 5;
	extension.route_proof.tag.relNumber = 20000;
	extension.route_proof.tag.forkNum = MAIN_FORKNUM;
	extension.route_proof.tag.blockNum = 37;
	make_two_foreign_update_candidate_page(page.data, &extension, expected_old, head.data,
										   tail.data, &head_uba, &tail_uba);
	ClusterPageGetItlSlots((Page)page.data)[0].wrap = TEST_WRAP + 20;
	UT_ASSERT_EQ(
		cluster_cr_build_on_holder_step(0, 193, false, &extension, page.data, head.data, &reason),
		CLUSTER_R4_CR_STEP_NEED_UNDO);
	UT_ASSERT_EQ(
		cluster_cr_build_on_holder_step(0, 193, true, &extension, page.data, head.data, &reason),
		CLUSTER_R4_CR_STEP_NEED_UNDO);
	UT_ASSERT_EQ(extension.foreign_wrap, TEST_WRAP);
	tail_slot = UNDO_SLOT_DIR_PTR(tail.data, 0);
	tail_record = (UndoRecordHeader *)(tail.data + tail_slot->record_offset);
	tail_record->tt_wrap_plus1++;
	UT_ASSERT_EQ(
		cluster_cr_build_on_holder_step(0, 193, true, &extension, page.data, tail.data, &reason),
		CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(reason, CLUSTER_CR_BUILD_BAD_UNDO);
	cluster_cr_build_on_holder_forget(0, 193);
}

int
main(int argc, char **argv)
{
	UT_PLAN(63);

	UT_RUN(test_head_identity_accepts_exact_uba_xid_wrap_and_tt_slot);
	UT_RUN(test_head_target_offset_is_not_transaction_identity);
	UT_RUN(test_head_exact_uba_mismatch_is_rejected);
	UT_RUN(test_head_malformed_uba_is_rejected);
	UT_RUN(test_head_xid_mismatch_is_rejected);
	UT_RUN(test_head_missing_wrap_is_rejected);
	UT_RUN(test_head_wrap_mismatch_is_rejected);
	UT_RUN(test_head_tt_segment_mismatch_is_rejected);
	UT_RUN(test_head_tt_slot_mismatch_is_rejected);
	UT_RUN(test_head_origin_mismatch_is_rejected);
	UT_RUN(test_previous_edge_accepts_same_tx_different_target_offset);
	UT_RUN(test_previous_edge_accepts_same_owner_physical_segment_rollover);
	UT_RUN(test_previous_edge_pointer_mismatch_is_rejected);
	UT_RUN(test_previous_edge_malformed_uba_is_rejected);
	UT_RUN(test_previous_edge_self_cycle_is_rejected);
	UT_RUN(test_previous_edge_forward_motion_is_rejected);
	UT_RUN(test_previous_edge_xid_mismatch_is_rejected);
	UT_RUN(test_previous_edge_wrap_mismatch_is_rejected);
	UT_RUN(test_previous_edge_tt_segment_mismatch_is_rejected);
	UT_RUN(test_previous_edge_tt_slot_mismatch_is_rejected);
	UT_RUN(test_previous_edge_origin_mismatch_is_rejected);
	UT_RUN(test_r4_resident_record_extracts_exact_canonical_hit);
	UT_RUN(test_r4_resident_record_upgrades_unknown_wrap_once);
	UT_RUN(test_r4_resident_record_rejects_canonical_wrap_mismatch);
	UT_RUN(test_r4_resident_record_rejects_unknown_wrap_without_record_wrap);
	UT_RUN(test_r4_resident_record_rejects_bad_data_header);
	UT_RUN(test_r4_resident_record_rejects_bad_row_and_bounds);
	UT_RUN(test_r4_resident_record_rejects_directory_type_and_flags_mismatch);
	UT_RUN(test_r4_resident_record_rejects_xid_mismatch);
	UT_RUN(test_r4_resident_record_rejects_malformed_and_block_zero_uba);
	UT_RUN(test_r4_builder_zero_candidate_is_full_and_exact_forget_reopens_key);
	UT_RUN(test_r4_builder_recycle_watermark_above_read_scn_fails_closed);
	UT_RUN(test_r4_builder_rejects_invalid_page_layout_before_full);
	UT_RUN(test_r4_builder_refuses_resume_without_context);
	UT_RUN(test_r4_builder_context_is_isolated_per_physical_slot);
	UT_RUN(test_r4_builder_single_local_insert_inverse_reaches_full);
	UT_RUN(test_r4_builder_single_local_update_hot_inverse_reaches_full);
	UT_RUN(test_r4_builder_single_local_delete_inverse_reaches_full);
	UT_RUN(test_r4_builder_single_local_itl_inverse_reaches_full);
	UT_RUN(test_r4_builder_record_at_read_scn_is_normal_horizon_stop);
	UT_RUN(test_r4_builder_two_local_updates_walk_newest_to_oldest);
	UT_RUN(test_r4_builder_two_local_candidate_xids_use_write_scn_desc);
	UT_RUN(test_r4_builder_foreign_head_freezes_one_need_undo);
	UT_RUN(test_r4_builder_foreign_resume_preserves_predecessor_and_step_count);
	UT_RUN(test_r4_builder_foreign_resume_accepts_unaligned_record_offset);
	UT_RUN(test_r4_builder_transaction_global_chain_skips_other_block);
	UT_RUN(test_r4_builder_rejects_malformed_off_target_relation_before_filter);
	UT_RUN(test_r4_builder_rejects_off_target_itl_invalid_slot_before_filter);
	UT_RUN(test_r4_builder_cross_segment_cycle_fails_before_repeat_fetch);
	UT_RUN(test_r4_builder_ordinary_update_successor_pair_reaches_full);
	UT_RUN(test_r4_builder_cross_page_update_does_not_fetch_successor);
	UT_RUN(test_r4_builder_successor_page_skips_old_target_inverse);
	UT_RUN(test_r4_builder_update_pair_horizon_does_not_restore_old_image);
	UT_RUN(test_r4_builder_update_payload_rejects_incomplete_or_corrupt_shapes);
	UT_RUN(test_r4_builder_update_pair_keeps_foreign_occupant_identity_guard);
	UT_RUN(test_r4_builder_legacy_update_still_restores_exact_old_image);
	UT_RUN(test_r4_builder_successor_tid_is_not_unrelated_tuple_delete_authority);
	UT_RUN(test_r4_builder_local_page_wrap_is_not_tt_generation);
	UT_RUN(test_r4_builder_foreign_wrap_canonicalizes_once_after_landing);
	UT_RUN(test_r4_builder_partial_locator_keeps_record_identity_refusals);
	UT_RUN(test_r4_builder_known_landed_wrap_must_match_exact_record);
	UT_RUN(test_r4_dependency_id_is_bounded_and_does_not_alias_next_record);
	UT_RUN(test_r4_canonical_generation_does_not_change_on_foreign_predecessor);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
