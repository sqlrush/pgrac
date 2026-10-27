/*-------------------------------------------------------------------------
 *
 * test_cluster_savepoint_reader.c
 *   Exact operation identity and scratch inverse on real heap page routines.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_savepoint_reader.c
 *
 * NOTES
 *   Pgrac-original standalone tests; no live authority or visibility fixture.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cr_history.h"
#include "cluster/cluster_savepoint.h"
#include "cluster/cluster_savepoint_reader.h"

#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition pg_attribute_unused(),
					 const char *file pg_attribute_unused(), int line pg_attribute_unused())
{
	abort();
}

/* Same local-time comparison as the product SCN primitive; actual UBA, page
 * and inverse functions are linked, with unexpected backend errors fatal. */
int
scn_time_cmp(SCN left, SCN right)
{
	return scn_local(left) < scn_local(right) ? -1 : scn_local(left) > scn_local(right);
}

int
errdetail_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

#define TUPLE_BYTES 64
typedef struct ReaderFixture {
	PGAlignedBlock page;
	BufferTag tag;
	ClusterTxLocator locator;
	ClusterSavepointOperation expected;
	UBA current;
	UndoRecordHeader record;
	UndoItlHistoryTrailer history;
	UndoOperationTrailer operation;
	char body[512];
	uint16 body_length;
} ReaderFixture;

static HeapTupleHeader
tuple_at(ReaderFixture *f, OffsetNumber offset)
{
	return (HeapTupleHeader)PageGetItem((Page)f->page.data,
										PageGetItemId((Page)f->page.data, offset));
}

static void
make_tuple(char *bytes, TransactionId xmin, TransactionId xmax)
{
	HeapTupleHeader tuple = (HeapTupleHeader)bytes;

	memset(bytes, 0, TUPLE_BYTES);
	tuple->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
	HeapTupleHeaderSetXmin(tuple, xmin);
	HeapTupleHeaderSetXmax(tuple, xmax);
	tuple->t_infomask = HEAP_XMAX_INVALID;
	ItemPointerSet(&tuple->t_ctid, 17, 1);
	bytes[TUPLE_BYTES - 1] = 0x5a;
}

static void
seal_fixture(ReaderFixture *f)
{
	memcpy(f->body + f->body_length, &f->history, sizeof(f->history));
	memcpy(f->body + f->body_length + sizeof(f->history), &f->operation, sizeof(f->operation));
	f->record.payload_length = f->body_length + sizeof(f->history) + sizeof(f->operation);
}

static void
init_fixture(ReaderFixture *f)
{
	ClusterItlSlotData *slot;
	UndoInsertPayload insert = { TUPLE_BYTES, 0 };
	char tuple[TUPLE_BYTES];

	memset(f, 0, sizeof(*f));
	f->tag.spcOid = 1663;
	f->tag.dbOid = 5;
	f->tag.relNumber = 12345;
	f->tag.forkNum = MAIN_FORKNUM;
	f->tag.blockNum = 17;
	f->current = uba_encode(2, 7, 2, 0);
	f->expected.top
		= (ClusterCanonicalTxnBinding){ .segment_id = 1,
										.segment_generation = 7,
										.xid = 100,
										.slot_offset = 2,
										.slot_wrap = 3,
										.origin_instance = 1,
										.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED,
										.active_lsn = 99 };
	f->expected.top_fxid = FullTransactionIdFromU64((UINT64CONST(1) << 32) | 100);
	f->expected.operation_fxid = FullTransactionIdFromU64((UINT64CONST(1) << 32) | 101);
	f->expected.operation_sequence = 19;
	f->expected.command_id = 4;
	f->expected.subid = 3;
	f->locator = (ClusterTxLocator){ .uba = f->current,
									 .xid = 100,
									 .tt_wrap = 3,
									 .itl_kind = ITL_FLAG_ACTIVE,
									 .itl_slot_index = 0 };
	f->record
		= (UndoRecordHeader){ .record_type = UNDO_RECORD_INSERT,
							  .flags = UNDO_REC_FLAG_HAS_ITL_HISTORY | UNDO_REC_FLAG_HAS_OPERATION,
							  .xid = 101,
							  .origin_node_id = 0,
							  .tt_slot_segment_id = 1,
							  .tt_slot_id = 3,
							  .write_scn = scn_encode(0, 1000),
							  .prev_uba = uba_encode(1, 6, 2, 0),
							  .target_locator = { .spcOid = 1663, .dbOid = 5, .relNumber = 12345 },
							  .target_fork = MAIN_FORKNUM,
							  .target_block = 17,
							  .target_offset = 1,
							  .tt_wrap_plus1 = 4 };
	f->history.magic = UNDO_ITL_HISTORY_MAGIC;
	f->history.version = UNDO_ITL_HISTORY_VERSION;
	f->history.count = 1;
	f->history.entries[0]
		= (UndoItlHistoryEntry){ .block = 17,
								 .slot_index = 0,
								 .after_kind = ITL_FLAG_ACTIVE,
								 .after_write_scn = f->record.write_scn,
								 .prior = { .xid = 100,
											.wrap = 8,
											.flags = ITL_FLAG_ACTIVE,
											.write_scn = scn_encode(0, 900),
											.undo_segment_head = f->record.prev_uba } };
	f->operation = (UndoOperationTrailer){ .version = UNDO_OPERATION_TRAILER_VERSION,
										   .length = sizeof(f->operation),
										   .effect_state = CLUSTER_SAVEPOINT_EFFECT_DISCARDED,
										   .canonical_top_fxid
										   = U64FromFullTransactionId(f->expected.top_fxid),
										   .operation_fxid
										   = U64FromFullTransactionId(f->expected.operation_fxid),
										   .operation_sequence = 19,
										   .canonical_segment_generation = 7,
										   .command_id = 4,
										   .subid = 3 };
	f->body_length = sizeof(insert);
	memcpy(f->body, &insert, sizeof(insert));
	seal_fixture(f);
	PageInit((Page)f->page.data, BLCKSZ, CLUSTER_ITL_SPECIAL_SIZE);
	((PageHeader)f->page.data)->pd_flags |= PD_HAS_ITL;
	make_tuple(tuple, 101, InvalidTransactionId);
	UT_ASSERT_EQ(PageAddItem((Page)f->page.data, tuple, TUPLE_BYTES, 1, false, true), 1);
	UT_ASSERT_EQ(PageAddItem((Page)f->page.data, tuple, TUPLE_BYTES, 2, false, true), 2);
	slot = ClusterPageGetItlSlots((Page)f->page.data);
	*slot = f->history.entries[0].prior;
	slot->write_scn = f->record.write_scn;
	slot->undo_segment_head = f->current;
}

static bool
decode_fixture(ReaderFixture *f, ClusterSavepointReaderRecord *out)
{
	return cluster_savepoint_reader_decode(&f->expected, &f->locator, f->current, &f->record,
										   f->body, f->record.payload_length, out);
}

static bool
inverse_fixture(ReaderFixture *f)
{
	return cluster_cr_history_inverse_operation(f->page.data, &f->tag, &f->locator, f->current,
												&f->record, f->body, f->record.payload_length,
												&f->expected);
}

static void
make_update(ReaderFixture *f, bool cross_page)
{
	UndoUpdatePayload update = { .new_block = cross_page ? 18 : 17,
								 .new_offset = 2,
								 .old_tuple_length = TUPLE_BYTES,
								 .old_tuple_offset = sizeof(update) };
	HeapTupleHeader tuple = tuple_at(f, 1);

	f->record.record_type = UNDO_RECORD_UPDATE;
	memcpy(f->body, &update, sizeof(update));
	make_tuple(f->body + sizeof(update), 42, InvalidTransactionId);
	f->body_length = sizeof(update) + TUPLE_BYTES;
	HeapTupleHeaderSetXmin(tuple, 42);
	HeapTupleHeaderSetXmax(tuple, 101);
	tuple->t_infomask &= ~HEAP_XMAX_INVALID;
	tuple->t_infomask2 |= HEAP_HOT_UPDATED;
	ItemPointerSet(&tuple->t_ctid, update.new_block, 2);
	tuple_at(f, 2)->t_infomask2 |= HEAP_ONLY_TUPLE;
	if (cross_page) {
		f->history.count = 2;
		f->history.entries[1] = f->history.entries[0];
		f->history.entries[1].block = 18;
	}
	seal_fixture(f);
}

UT_TEST(test_exact_decode_and_unaligned_body)
{
	ReaderFixture f;
	ClusterSavepointReaderRecord decoded;
	char unaligned[513];
	UndoRecordHeader before;

	init_fixture(&f);
	before = f.record;
	memcpy(unaligned + 1, f.body, f.record.payload_length);
	UT_ASSERT(cluster_savepoint_reader_decode(&f.expected, &f.locator, f.current, &f.record,
											  unaligned + 1, f.record.payload_length, &decoded));
	UT_ASSERT_EQ(decoded.body_length, sizeof(UndoInsertPayload));
	UT_ASSERT_EQ(decoded.operation.effect_state, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	UT_ASSERT(memcmp(&f.record, &before, sizeof(before)) == 0);
	UT_ASSERT_EQ(f.record.xid, 101);
	f.operation.effect_state = CLUSTER_SAVEPOINT_EFFECT_LIVE;
	seal_fixture(&f);
	UT_ASSERT(decode_fixture(&f, &decoded));
	UT_ASSERT_EQ(decoded.operation.effect_state, CLUSTER_SAVEPOINT_EFFECT_LIVE);
}

UT_TEST(test_expected_identity_mismatch_clears_output)
{
	int fault;

	for (fault = 0; fault < 18; fault++) {
		ReaderFixture f;
		ClusterSavepointReaderRecord decoded;
		ClusterSavepointReaderRecord empty = { 0 };

		init_fixture(&f);
		switch (fault) {
		case 0:
			f.expected.top.segment_id++;
			break;
		case 1:
			f.expected.top.segment_generation++;
			break;
		case 2:
			f.expected.top.slot_offset++;
			break;
		case 3:
			f.expected.top.slot_wrap++;
			break;
		case 4:
			f.expected.top.origin_instance++;
			break;
		case 5:
			f.expected.top.publish_state = 0;
			break;
		case 6:
			f.expected.top.active_lsn = InvalidXLogRecPtr;
			break;
		case 7:
			f.expected.top.reserved16 = 1;
			break;
		case 8:
			f.expected.top.xid++;
			break;
		case 9:
			f.expected.top_fxid = FullTransactionIdFromU64(UINT64CONST(2) << 32 | 100);
			break;
		case 10:
			f.expected.operation_fxid = FullTransactionIdFromU64(UINT64CONST(2) << 32 | 101);
			break;
		case 11:
			f.expected.operation_sequence++;
			break;
		case 12:
			f.expected.command_id++;
			break;
		case 13:
			f.expected.subid++;
			break;
		case 14:
			f.locator.xid++;
			break;
		case 15:
			f.locator.tt_wrap++;
			break;
		case 16:
			f.locator.uba = f.record.prev_uba;
			break;
		case 17:
			f.current = uba_encode(2, UNDO_BLOCKS_PER_SEGMENT, 2, 0);
			f.locator.uba = f.current;
			break;
		}
		memset(&decoded, 0xa5, sizeof(decoded));
		UT_ASSERT(!decode_fixture(&f, &decoded));
		UT_ASSERT(memcmp(&decoded, &empty, sizeof(empty)) == 0);
	}
}

UT_TEST(test_unknown_or_malformed_record_refused)
{
	int fault;

	for (fault = 0; fault < 13; fault++) {
		ReaderFixture f;
		ClusterSavepointReaderRecord decoded;

		init_fixture(&f);
		switch (fault) {
		case 0:
			f.operation.effect_state = 0;
			break;
		case 1:
			f.operation.version++;
			break;
		case 2:
			f.operation.length--;
			break;
		case 3:
			f.operation.reserved[1] = 1;
			break;
		case 4:
			f.operation.reserved_end = 1;
			break;
		case 5:
			f.operation.operation_sequence = 0;
			break;
		case 6:
			f.record.xid++;
			break;
		case 7:
			f.record.tt_slot_segment_id++;
			break;
		case 8:
			f.record.tt_slot_id++;
			break;
		case 9:
			f.record.origin_node_id++;
			break;
		case 10:
			f.record.flags |= UNDO_REC_FLAG_TOAST;
			break;
		case 11:
			f.history.entries[0].reserved = 1;
			break;
		case 12:
			f.record.prev_uba = f.current;
			break;
		}
		seal_fixture(&f);
		UT_ASSERT(!decode_fixture(&f, &decoded));
	}
}

UT_TEST(test_short_or_absent_inputs_refused)
{
	ReaderFixture f;
	ClusterSavepointReaderRecord decoded;

	init_fixture(&f);
	UT_ASSERT(!cluster_savepoint_reader_decode(&f.expected, &f.locator, f.current, &f.record,
											   f.body, f.record.payload_length - 1, &decoded));
	UT_ASSERT(!cluster_savepoint_reader_decode(NULL, &f.locator, f.current, &f.record, f.body,
											   f.record.payload_length, &decoded));
	UT_ASSERT(!cluster_savepoint_reader_decode(&f.expected, NULL, f.current, &f.record, f.body,
											   f.record.payload_length, &decoded));
	UT_ASSERT(!cluster_savepoint_reader_decode(&f.expected, &f.locator, f.current, NULL, f.body,
											   f.record.payload_length, &decoded));
	UT_ASSERT(!cluster_savepoint_reader_decode(&f.expected, &f.locator, f.current, &f.record, NULL,
											   f.record.payload_length, &decoded));
	UT_ASSERT(!cluster_savepoint_reader_decode(&f.expected, &f.locator, f.current, &f.record,
											   f.body, f.record.payload_length, NULL));
}

UT_TEST(test_insert_uses_top_slot_and_exact_child_tuple)
{
	ReaderFixture f;

	init_fixture(&f);
	UT_ASSERT(inverse_fixture(&f));
	UT_ASSERT(!ItemIdIsNormal(PageGetItemId((Page)f.page.data, 1)));
	UT_ASSERT(ItemIdIsNormal(PageGetItemId((Page)f.page.data, 2)));
	UT_ASSERT(memcmp(ClusterPageGetItlSlots((Page)f.page.data), &f.history.entries[0].prior,
					 sizeof(ClusterItlSlotData))
			  == 0);
}

UT_TEST(test_prior_child_owner_increments_wrap)
{
	ReaderFixture f;

	init_fixture(&f);
	f.history.entries[0].prior.xid = 101;
	ClusterPageGetItlSlots((Page)f.page.data)->wrap++;
	seal_fixture(&f);
	UT_ASSERT(inverse_fixture(&f));
	UT_ASSERT_EQ(ClusterPageGetItlSlots((Page)f.page.data)->xid, 101);
	UT_ASSERT_EQ(ClusterPageGetItlSlots((Page)f.page.data)->wrap, 8);
}

UT_TEST(test_free_prior_preserves_wrap)
{
	ReaderFixture f;

	init_fixture(&f);
	memset(&f.history.entries[0].prior, 0, sizeof(ClusterItlSlotData));
	f.history.entries[0].prior.wrap = PG_UINT16_MAX;
	ClusterPageGetItlSlots((Page)f.page.data)->wrap = PG_UINT16_MAX;
	seal_fixture(&f);
	UT_ASSERT(inverse_fixture(&f));
	UT_ASSERT_EQ(ClusterPageGetItlSlots((Page)f.page.data)->flags, ITL_FLAG_FREE);
	UT_ASSERT_EQ(ClusterPageGetItlSlots((Page)f.page.data)->wrap, PG_UINT16_MAX);
}

UT_TEST(test_slot_mismatch_preserves_page)
{
	int fault;

	for (fault = 0; fault < 7; fault++) {
		ReaderFixture f;
		PGAlignedBlock before;
		ClusterItlSlotData *slot;

		init_fixture(&f);
		slot = ClusterPageGetItlSlots((Page)f.page.data);
		switch (fault) {
		case 0:
			slot->xid = f.record.xid;
			break;
		case 1:
			slot->wrap++;
			break;
		case 2:
			slot->write_scn++;
			break;
		case 3:
			slot->undo_segment_head = f.record.prev_uba;
			break;
		case 4:
			slot->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
			break;
		case 5:
			f.locator.itl_slot_index = 1;
			break;
		case 6:
			f.tag.relNumber++;
			break;
		}
		before = f.page;
		UT_ASSERT(!inverse_fixture(&f));
		UT_ASSERT(memcmp(before.data, f.page.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_raw_tuple_xmin_mismatch_refused)
{
	ReaderFixture f;
	PGAlignedBlock before;

	init_fixture(&f);
	HeapTupleHeaderSetXmin(tuple_at(&f, 1), 100);
	before = f.page;
	UT_ASSERT(!inverse_fixture(&f));
	UT_ASSERT(memcmp(before.data, f.page.data, BLCKSZ) == 0);
}

UT_TEST(test_hot_update_restores_root_and_removes_member)
{
	ReaderFixture f;

	init_fixture(&f);
	make_update(&f, false);
	UT_ASSERT(inverse_fixture(&f));
	UT_ASSERT(memcmp(tuple_at(&f, 1), f.body + sizeof(UndoUpdatePayload), TUPLE_BYTES) == 0);
	UT_ASSERT(!ItemIdIsNormal(PageGetItemId((Page)f.page.data, 2)));
}

UT_TEST(test_cross_page_update_restores_both_images)
{
	ReaderFixture old;
	ReaderFixture replacement;
	char old_root[TUPLE_BYTES];

	init_fixture(&old);
	make_update(&old, true);
	replacement = old;
	replacement.tag.blockNum = 18;
	memcpy(old_root, tuple_at(&replacement, 1), TUPLE_BYTES);
	UT_ASSERT(inverse_fixture(&old));
	UT_ASSERT(memcmp(tuple_at(&old, 1), old.body + sizeof(UndoUpdatePayload), TUPLE_BYTES) == 0);
	UT_ASSERT(ItemIdIsNormal(PageGetItemId((Page)old.page.data, 2)));
	UT_ASSERT(inverse_fixture(&replacement));
	UT_ASSERT(!ItemIdIsNormal(PageGetItemId((Page)replacement.page.data, 2)));
	UT_ASSERT(memcmp(tuple_at(&replacement, 1), old_root, TUPLE_BYTES) == 0);
}

static void
make_delete(ReaderFixture *f)
{
	UndoDeletePayload deleted
		= { .full_tuple_length = TUPLE_BYTES, .full_tuple_offset = sizeof(deleted) };
	HeapTupleHeader tuple = tuple_at(f, 1);

	f->record.record_type = UNDO_RECORD_DELETE;
	memcpy(f->body, &deleted, sizeof(deleted));
	make_tuple(f->body + sizeof(deleted), 42, InvalidTransactionId);
	f->body_length = sizeof(deleted) + TUPLE_BYTES;
	HeapTupleHeaderSetXmin(tuple, 42);
	HeapTupleHeaderSetXmax(tuple, 101);
	tuple->t_infomask &= ~HEAP_XMAX_INVALID;
	seal_fixture(f);
}

UT_TEST(test_update_and_delete_raw_identity_required)
{
	int kind;
	int fault;

	for (kind = 0; kind < 2; kind++) {
		for (fault = 0; fault < 4; fault++) {
			ReaderFixture f;
			PGAlignedBlock before;
			HeapTupleHeader tuple;

			init_fixture(&f);
			if (kind == 0)
				make_update(&f, false);
			else
				make_delete(&f);
			tuple = tuple_at(&f, 1);
			if (fault == 0)
				HeapTupleHeaderSetXmin(tuple, 43);
			else if (fault == 1)
				HeapTupleHeaderSetXmax(tuple, 100);
			else if (fault == 2)
				tuple->t_infomask |= HEAP_XMAX_IS_MULTI;
			else
				tuple->t_infomask |= HEAP_XMAX_LOCK_ONLY;
			before = f.page;
			UT_ASSERT(!inverse_fixture(&f));
			UT_ASSERT(memcmp(before.data, f.page.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_delete_current_child_raw_xmax_restored)
{
	ReaderFixture f;

	init_fixture(&f);
	make_delete(&f);
	UT_ASSERT(inverse_fixture(&f));
	UT_ASSERT(memcmp(tuple_at(&f, 1), f.body + sizeof(UndoDeletePayload), TUPLE_BYTES) == 0);
}

UT_TEST(test_delete_restores_missing_tuple_with_real_compaction)
{
	ReaderFixture f;
	char filler[BLCKSZ] = { 0 };
	Size available;

	init_fixture(&f);
	make_delete(&f);
	available = PageGetExactFreeSpace((Page)f.page.data) - MAXALIGN(sizeof(ItemIdData)) - 8;
	UT_ASSERT_EQ(PageAddItem((Page)f.page.data, filler, available, 3, false, true), 3);
	ItemIdSetUnused(PageGetItemId((Page)f.page.data, 1));
	UT_ASSERT(inverse_fixture(&f));
	UT_ASSERT(ItemIdIsNormal(PageGetItemId((Page)f.page.data, 1)));
	if (ItemIdIsNormal(PageGetItemId((Page)f.page.data, 1)))
		UT_ASSERT(memcmp(tuple_at(&f, 1), f.body + sizeof(UndoDeletePayload), TUPLE_BYTES) == 0);
	UT_ASSERT(ItemIdIsNormal(PageGetItemId((Page)f.page.data, 2)));
	UT_ASSERT_EQ(ItemIdGetLength(PageGetItemId((Page)f.page.data, 3)), available);
}

static void
make_lock(ReaderFixture *f)
{
	UndoItlPayload lock = { .itl_slot_idx = 0,
							.prev_flags = ITL_FLAG_ACTIVE,
							.new_flags = ITL_FLAG_LOCK_ONLY_ACTIVE,
							.lock_xid = 101,
							.prev_xmax = 77,
							.prev_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK };
	ClusterItlSlotData *slot = ClusterPageGetItlSlots((Page)f->page.data);
	HeapTupleHeader tuple = tuple_at(f, 1);

	f->record.record_type = UNDO_RECORD_ITL;
	f->history.entries[0].after_kind = ITL_FLAG_LOCK_ONLY_ACTIVE;
	f->body_length = sizeof(lock);
	memcpy(f->body, &lock, sizeof(lock));
	slot->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	slot->wrap = f->history.entries[0].prior.wrap + 1;
	f->locator.itl_kind = ITL_FLAG_LOCK_ONLY_ACTIVE;
	HeapTupleHeaderSetXmax(tuple, 101);
	tuple->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
	seal_fixture(f);
}

UT_TEST(test_lock_raw_child_and_role_wrap_restore)
{
	ReaderFixture f;

	init_fixture(&f);
	make_lock(&f);
	UT_ASSERT(inverse_fixture(&f));
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmax(tuple_at(&f, 1)), 77);
	UT_ASSERT_EQ(tuple_at(&f, 1)->t_infomask, HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK);
	UT_ASSERT_EQ(ClusterPageGetItlSlots((Page)f.page.data)->wrap, 8);
	UT_ASSERT_EQ(ClusterPageGetItlSlots((Page)f.page.data)->flags, ITL_FLAG_ACTIVE);
}

UT_TEST(test_lock_canonical_top_cannot_replace_raw_xmax)
{
	ReaderFixture f;
	PGAlignedBlock before;

	init_fixture(&f);
	make_lock(&f);
	HeapTupleHeaderSetXmax(tuple_at(&f, 1), 100);
	before = f.page;
	UT_ASSERT(!inverse_fixture(&f));
	UT_ASSERT(memcmp(before.data, f.page.data, BLCKSZ) == 0);
}

UT_TEST(test_original_readers_still_reject_operation)
{
	ReaderFixture f;
	uint16 body_length;
	UndoItlHistoryTrailer history;
	PGAlignedBlock before;

	init_fixture(&f);
	before = f.page;
	UT_ASSERT(!cluster_undo_record_validate_identity(&f.locator, f.current, &f.record, NULL));
	UT_ASSERT(!cluster_undo_record_decode_payload(&f.record, f.body, f.record.payload_length,
												  &body_length, &history));
	UT_ASSERT(!cluster_cr_history_inverse(f.page.data, &f.tag, &f.locator, f.current, &f.record,
										  f.body, f.record.payload_length));
	UT_ASSERT(memcmp(before.data, f.page.data, BLCKSZ) == 0);
}

UT_TEST(test_original_top_only_inverse_unchanged)
{
	ReaderFixture f;

	init_fixture(&f);
	f.record.flags &= ~UNDO_REC_FLAG_HAS_OPERATION;
	f.record.payload_length -= sizeof(UndoOperationTrailer);
	f.record.xid = 100;
	HeapTupleHeaderSetXmin(tuple_at(&f, 1), 100);
	UT_ASSERT(cluster_cr_history_inverse(f.page.data, &f.tag, &f.locator, f.current, &f.record,
										 f.body, f.record.payload_length));
	UT_ASSERT(!ItemIdIsNormal(PageGetItemId((Page)f.page.data, 1)));
	UT_ASSERT(ItemIdIsNormal(PageGetItemId((Page)f.page.data, 2)));
}

int
main(void)
{
	UT_PLAN(18);
	UT_RUN(test_exact_decode_and_unaligned_body);
	UT_RUN(test_expected_identity_mismatch_clears_output);
	UT_RUN(test_unknown_or_malformed_record_refused);
	UT_RUN(test_short_or_absent_inputs_refused);
	UT_RUN(test_insert_uses_top_slot_and_exact_child_tuple);
	UT_RUN(test_prior_child_owner_increments_wrap);
	UT_RUN(test_free_prior_preserves_wrap);
	UT_RUN(test_slot_mismatch_preserves_page);
	UT_RUN(test_raw_tuple_xmin_mismatch_refused);
	UT_RUN(test_hot_update_restores_root_and_removes_member);
	UT_RUN(test_cross_page_update_restores_both_images);
	UT_RUN(test_update_and_delete_raw_identity_required);
	UT_RUN(test_delete_current_child_raw_xmax_restored);
	UT_RUN(test_delete_restores_missing_tuple_with_real_compaction);
	UT_RUN(test_lock_raw_child_and_role_wrap_restore);
	UT_RUN(test_lock_canonical_top_cannot_replace_raw_xmax);
	UT_RUN(test_original_readers_still_reject_operation);
	UT_RUN(test_original_top_only_inverse_unchanged);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
