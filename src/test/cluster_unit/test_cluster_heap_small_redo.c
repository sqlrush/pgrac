/*-------------------------------------------------------------------------
 *
 * test_cluster_heap_small_redo.c
 *    Detached small-record/maintenance replay versus native heap redo.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_heap_small_redo.c
 *
 * NOTES
 *    Decoded record fixtures, actual native redo bodies and actual ITL/page
 *    helpers. Buffer and VM I/O are explicit boundaries. Does not prove a
 *    whole-record VM install, namespace admission or live recovery.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/xlog.h"
#include "access/visibilitymap.h"
#include "access/xlogutils.h"
#include "cluster/cluster_block_apply.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_itl.h"
#include "cluster/cluster_uba.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/standby.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_enabled = true, cluster_shared_config = true;
int cluster_node_id, NBuffers = 1, NLocBuffer;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
HotStandbyState standbyState = STANDBY_DISABLED;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;
static PGAlignedBlock native_page, detached_page, original, main_data;
static XLogReaderState reader;
static DecodedXLogRecord *decoded;
static unsigned dirties, releases, vm_calls;
static bool locked;
static const BlockNumber block_number = 17;
static char inplace_data[8] = "newvalue";

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# unexpected %s %s:%d\n", condition, file, line);
	abort();
}

XLogRedoAction
XLogReadBufferForRedo(XLogReaderState *record, uint8 id, Buffer *buffer)
{
	UT_ASSERT(record == &reader && id == 0 && !locked);
	locked = true;
	*buffer = 1;
	return BLK_NEEDS_REDO;
}
XLogRedoAction
XLogReadBufferForRedoExtended(XLogReaderState *record, uint8 id, ReadBufferMode mode, bool cleanup,
							  Buffer *buffer)
{
	UT_ASSERT(mode == RBM_NORMAL);
	UT_ASSERT_EQ(cleanup, (XLogRecGetInfo(record) & XLOG_HEAP_OPMASK) == XLOG_HEAP2_PRUNE);
	return XLogReadBufferForRedo(record, id, buffer);
}
void
ResolveRecoveryConflictWithSnapshot(TransactionId xid, bool catalog, RelFileLocator locator)
{
	(void)xid;
	(void)catalog;
	(void)locator;
	abort();
}
void
XLogRecordPageWithFreeSpace(RelFileLocator locator, BlockNumber block, Size space)
{
	UT_ASSERT(locator.relNumber == 900 && block == block_number && !locked);
	UT_ASSERT(space <= BLCKSZ);
}
BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	UT_ASSERT(buffer == 1 && locked);
	return block_number;
}
void
MarkBufferDirty(Buffer buffer)
{
	UT_ASSERT(buffer == 1 && locked);
	dirties++;
}
void
UnlockReleaseBuffer(Buffer buffer)
{
	UT_ASSERT(buffer == 1 && locked);
	locked = false;
	releases++;
}
void
visibilitymap_clear_redo(XLogReaderState *record, RelFileLocator locator, BlockNumber block,
						 uint8 flags)
{
	UT_ASSERT(record == &reader && locator.relNumber == 900 && block == block_number);
	UT_ASSERT(flags == 0 || flags == VISIBILITYMAP_ALL_FROZEN);
	vm_calls++;
}
char *
XLogRecGetBlockData(XLogReaderState *record, uint8 id, Size *length)
{
	const DecodedBkpBlock *block = XLogRecGetBlock(record, id);
	if (length != NULL)
		*length = block->data_len;
	return block->has_data ? block->data : NULL;
}
void
XLogRecGetBlockTag(XLogReaderState *record, uint8 id, RelFileLocator *locator, ForkNumber *forknum,
				   BlockNumber *blkno)
{
	const DecodedBkpBlock *block = XLogRecGetBlock(record, id);
	if (locator != NULL)
		*locator = block->rlocator;
	if (forknum != NULL)
		*forknum = block->forknum;
	if (blkno != NULL)
		*blkno = block->blkno;
}
bool
XLogRecGetBlockTagExtended(XLogReaderState *record, uint8 id, RelFileLocator *locator,
						   ForkNumber *forknum, BlockNumber *blkno, Buffer *prefetch_buffer)
{
	if (!XLogRecHasBlockRef(record, id))
		return false;
	XLogRecGetBlockTag(record, id, locator, forknum, blkno);
	if (prefetch_buffer != NULL)
		*prefetch_buffer = InvalidBuffer;
	return true;
}
bool
RestoreBlockImage(XLogReaderState *record, uint8 id, char *page)
{
	(void)record;
	(void)id;
	(void)page;
	abort();
}

#include "test_cluster_heap_small_scn.inc"
#include "test_cluster_heap_small_native.inc"
static void page_verify_redirects(Page page);
#include "test_cluster_heap_prune_helpers.inc"
#include "test_cluster_heap_maintenance_native.inc"

static HeapTupleHeader
tuple_at(Page page)
{
	return (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 1));
}

static void
reset(uint8 rmid, uint8 opcode, int itl_format)
{
	PGAlignedBlock tuple;
	HeapTupleHeader htup = (HeapTupleHeader)tuple.data;
	DecodedBkpBlock *block;
	xl_heap_lock rec;
	Size base_size;
	free(decoded);
	decoded = calloc(1, offsetof(DecodedXLogRecord, blocks) + sizeof(DecodedBkpBlock));
	memset(&reader, 0, sizeof(reader));
	memset(&main_data, 0, sizeof(main_data));
	memset(&tuple, 0, sizeof(tuple));
	reader.record = decoded;
	reader.EndRecPtr = 800;
	decoded->header.xl_rmid = rmid;
	decoded->header.xl_info = opcode;
	decoded->header.xl_xid = 200;
	decoded->max_block_id = 0;
	decoded->main_data = main_data.data;
	block = &decoded->blocks[0];
	block->in_use = true;
	block->rlocator = (RelFileLocator){ 1663, 5, 900 };
	block->forknum = MAIN_FORKNUM;
	block->blkno = block_number;
	if (itl_format >= 0)
		PageInitHeapPage(original.data, BLCKSZ, 0);
	else
		PageInit(original.data, BLCKSZ, 0);
	((PageHeader)original.data)->pd_block_scn = 100;
	htup->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
	htup->t_infomask = HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_COMMITTED | HEAP_COMBOCID;
	htup->t_infomask2 = HEAP_HOT_UPDATED | HEAP_KEYS_UPDATED | 1;
	ClusterHeapTupleHeaderInitItlSlot(htup);
	HeapTupleHeaderSetXmin(htup, 100);
	HeapTupleHeaderSetXmax(htup, 150);
	HeapTupleHeaderSetCmax(htup, 99, true);
	ItemPointerSet(&htup->t_ctid, 19, 3);
	memcpy(tuple.data + htup->t_hoff, "oldvalue", 8);
	if (PageAddItem(original.data, (Item)tuple.data, htup->t_hoff + 8, 1, false, true) != 1)
		abort();
	memset(&rec, 0, sizeof(rec));
	rec.xmax = 200;
	rec.offnum = 1;
	rec.infobits_set = XLHL_XMAX_KEYSHR_LOCK | XLHL_XMAX_LOCK_ONLY;
	if (rmid == RM_HEAP_ID && (opcode == XLOG_HEAP_CONFIRM || opcode == XLOG_HEAP_INPLACE)) {
		OffsetNumber offset = 1;
		memcpy(main_data.data, &offset, sizeof(offset));
		base_size = sizeof(offset);
		if (opcode == XLOG_HEAP_INPLACE) {
			block->has_data = true;
			block->data = inplace_data;
			block->data_len = sizeof(inplace_data);
		}
	} else {
		base_size = SizeOfHeapLock;
		rec.flags = XLH_LOCK_ALL_FROZEN_CLEARED;
		if (itl_format >= 0)
			rec.flags |= XLH_LOCK_ITL_DELTA;
		memcpy(main_data.data, &rec, base_size);
	}
	decoded->main_data_len = base_size;
	if (itl_format >= 0) {
		xl_heap_itl_delta_block header;
		xl_heap_itl_delta_v2 delta;
		xl_heap_itl_delta_v3 short_delta;
		Size size;
		memset(&header, 0, sizeof(header));
		memset(&delta, 0, sizeof(delta));
		memset(&short_delta, 0, sizeof(short_delta));
		header.ndeltas = 1;
		header.format_version = itl_format;
		delta.slot_idx = short_delta.slot_idx = 2;
		delta.flags_after = short_delta.flags_after = ITL_FLAG_LOCK_ONLY_ACTIVE;
		delta.xid = short_delta.xid = 200;
		delta.write_scn = short_delta.write_scn = 400;
		delta.undo_segment_head = short_delta.undo_segment_head = uba_encode(1, 3, 4, 5);
		memcpy(main_data.data + base_size, &header, offsetof(xl_heap_itl_delta_block, deltas));
		base_size += offsetof(xl_heap_itl_delta_block, deltas);
		size = itl_format == 0	 ? sizeof(xl_heap_itl_delta)
			   : itl_format == 1 ? sizeof(delta)
								 : sizeof(short_delta);
		memcpy(main_data.data + base_size, itl_format < 2 ? (char *)&delta : (char *)&short_delta,
			   size);
		decoded->main_data_len = base_size + size;
	}
	native_page = detached_page = original;
	BufferBlocks = native_page.data;
	dirties = releases = vm_calls = 0;
	locked = false;
}

static void
compare_native(void (*native)(XLogReaderState *))
{
	ClusterBlkApplyResult result;
	native(&reader);
	result = cluster_block_apply_one(&reader, 0, detached_page.data);
	UT_ASSERT_EQ(result, CLUSTER_BLKAPPLY_OK);
	if (result == CLUSTER_BLKAPPLY_OK)
		UT_ASSERT(memcmp(native_page.data, detached_page.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(dirties, 1);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT(!locked);
}

UT_TEST(test_confirm_and_inplace_match_native)
{
	for (int shared = 0; shared < 2; shared++) {
		cluster_shared_config = shared;
		reset(RM_HEAP_ID, XLOG_HEAP_CONFIRM, -1);
		HeapTupleHeaderSetSpeculativeToken(tuple_at(original.data), 1234);
		native_page = detached_page = original;
		compare_native(heap_xlog_confirm);
		UT_ASSERT_EQ(ItemPointerGetBlockNumber(&tuple_at(native_page.data)->t_ctid), block_number);
		reset(RM_HEAP_ID, XLOG_HEAP_INPLACE, -1);
		compare_native(heap_xlog_inplace);
		UT_ASSERT(memcmp((char *)tuple_at(native_page.data) + tuple_at(native_page.data)->t_hoff,
						 inplace_data, sizeof(inplace_data))
				  == 0);
	}
}

UT_TEST(test_lock_and_updated_lock_match_native)
{
	for (int shared = 0; shared < 2; shared++)
		for (int updated = 0; updated < 2; updated++)
			for (int format = -1; format <= 3; format++)
				for (int lock_only = 0; lock_only < 2; lock_only++) {
					xl_heap_lock *rec;
					cluster_shared_config = shared;
					reset(updated ? RM_HEAP2_ID : RM_HEAP_ID,
						  updated ? XLOG_HEAP2_LOCK_UPDATED : XLOG_HEAP_LOCK, format);
					rec = (xl_heap_lock *)main_data.data;
					if (!lock_only)
						rec->infobits_set
							= XLHL_XMAX_IS_MULTI | XLHL_XMAX_EXCL_LOCK | XLHL_KEYS_UPDATED;
					compare_native(updated ? heap_xlog_lock_updated : heap_xlog_lock);
					UT_ASSERT_EQ(vm_calls, 1);
					if (format >= 0) {
						ClusterItlSlotData *slot = &ClusterPageGetItlSlots(native_page.data)[2];
						UT_ASSERT_EQ(slot->xid, 200);
						UT_ASSERT_EQ(slot->flags, ITL_FLAG_LOCK_ONLY_ACTIVE);
					}
				}
}

UT_TEST(test_zero_length_inplace_payload_matches_native)
{
	ItemId item;
	reset(RM_HEAP_ID, XLOG_HEAP_INPLACE, -1);
	item = PageGetItemId(original.data, 1);
	ItemIdSetNormal(item, ItemIdGetOffset(item), tuple_at(original.data)->t_hoff);
	decoded->blocks[0].data_len = 0;
	native_page = detached_page = original;
	compare_native(heap_xlog_inplace);
	/* Native reader may return NULL for an absent zero-length block payload. */
	detached_page = original;
	decoded->blocks[0].has_data = false;
	UT_ASSERT_EQ(cluster_block_apply_one(&reader, 0, detached_page.data), CLUSTER_BLKAPPLY_OK);
	UT_ASSERT(memcmp(native_page.data, detached_page.data, BLCKSZ) == 0);
}

UT_TEST(test_replaced_locker_refs_and_history_match_native)
{
	for (int format = 2; format <= 3; format++) {
		PGAlignedBlock tuple;
		HeapTupleHeader old = (HeapTupleHeader)tuple.data;
		ClusterItlSlotData *slot;
		reset(RM_HEAP_ID, XLOG_HEAP_LOCK, format);
		memcpy(tuple.data, tuple_at(original.data), tuple_at(original.data)->t_hoff + 8);
		old->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK;
		UT_ASSERT_EQ(PageAddItem(original.data, (Item)old, old->t_hoff + 8, 2, false, true), 2);
		slot = &ClusterPageGetItlSlots(original.data)[2];
		slot->xid = 150;
		slot->flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
		slot->wrap = 9;
		slot->write_scn = 50;
		slot->commit_scn = 60;
		native_page = detached_page = original;
		compare_native(heap_xlog_lock);
		old = (HeapTupleHeader)PageGetItem(native_page.data, PageGetItemId(native_page.data, 2));
		UT_ASSERT((old->t_infomask & HEAP_XMAX_INVALID) != 0);
		UT_ASSERT_EQ(ClusterPageGetItlSlots(native_page.data)[2].wrap, format == 3 ? 10 : 9);
	}
}

UT_TEST(test_bad_shapes_do_not_modify_output)
{
	for (int bad = 0; bad < 18; bad++) {
		PGAlignedBlock saved;
		ClusterBlkApplyResult result;
		reset(RM_HEAP_ID, XLOG_HEAP_INPLACE, -1);
		switch (bad) {
		case 0:
			decoded->main_data_len = 1;
			break;
		case 1:
			((xl_heap_inplace *)main_data.data)->offnum = 0;
			break;
		case 2:
			((xl_heap_inplace *)main_data.data)->offnum = 2;
			break;
		case 3:
			ItemIdSetDead(PageGetItemId(detached_page.data, 1));
			break;
		case 4:
			tuple_at(detached_page.data)->t_hoff = 200;
			break;
		case 5:
			decoded->blocks[0].data_len--;
			break;
		case 6:
			decoded->blocks[0].has_data = false;
			break;
		case 7:
			((PageHeader)detached_page.data)->pd_lower = BLCKSZ;
			break;
		case 8:
			decoded->blocks[0].forknum = VISIBILITYMAP_FORKNUM;
			break;
		case 9:
			decoded->header.xl_info |= XLOG_HEAP_INIT_PAGE;
			break;
		case 10:
			reset(RM_HEAP_ID, XLOG_HEAP_LOCK, 3);
			decoded->main_data_len--;
			break;
		case 11:
			reset(RM_HEAP2_ID, XLOG_HEAP2_LOCK_UPDATED, 3);
			((xl_heap_itl_delta_block *)(main_data.data + SizeOfHeapLock))->format_version = 100;
			break;
		case 12:
			reset(RM_HEAP_ID, XLOG_HEAP_LOCK, 3);
			((xl_heap_itl_delta_v3 *)(main_data.data + SizeOfHeapLock
									  + offsetof(xl_heap_itl_delta_block, deltas)))
				->undo_segment_head = uba_encode(1, 3, 4, 65535);
			break;
		case 13:
			reset(RM_HEAP_ID, XLOG_HEAP_LOCK, 3);
			((xl_heap_itl_delta_v3 *)(main_data.data + SizeOfHeapLock
									  + offsetof(xl_heap_itl_delta_block, deltas)))
				->slot_idx = 8;
			break;
		case 14:
			reset(RM_HEAP_ID, XLOG_HEAP_LOCK, 3);
			/* Invalid declared page size must not wrap the ITL-space check. */
			((PageHeader)detached_page.data)->pd_pagesize_version = 0;
			break;
		case 15:
			reset(RM_HEAP_ID, XLOG_HEAP_LOCK, 3);
			((PageHeader)detached_page.data)->pd_pagesize_version = 0;
			((PageHeader)detached_page.data)->pd_special = BLCKSZ - 8;
			break;
		case 16:
			reset(RM_HEAP_ID, XLOG_HEAP_LOCK, 3);
			((PageHeader)detached_page.data)->pd_special = BLCKSZ - 8;
			break;
		case 17:
			((PageHeader)detached_page.data)->pd_pagesize_version = BLCKSZ;
			break;
		}
		saved = detached_page;
		result = cluster_block_apply_one(&reader, 0, detached_page.data);
		UT_ASSERT(result == CLUSTER_BLKAPPLY_FAILED || result == CLUSTER_BLKAPPLY_UNSUPPORTED);
		UT_ASSERT(memcmp(saved.data, detached_page.data, BLCKSZ) == 0);
	}
}

static PGAlignedBlock maintenance_data;

static void
reset_maintenance(uint8 opcode, bool itl_page)
{
	PGAlignedBlock tuple;
	HeapTupleHeader htup = (HeapTupleHeader)tuple.data;
	reset(RM_HEAP2_ID, opcode, itl_page ? 3 : -1);
	/* ITL special space is retained byte-for-byte, not a maintenance delta. */
	memset(&main_data, 0, sizeof(main_data));
	memset(&maintenance_data, 0, sizeof(maintenance_data));
	memcpy(tuple.data, tuple_at(original.data), tuple_at(original.data)->t_hoff + 8);
	for (OffsetNumber off = 2; off <= 4; off++) {
		htup->t_infomask2 = off == 3 ? 1 : HEAP_ONLY_TUPLE | 1;
		UT_ASSERT_EQ(
			PageAddItem(original.data, (Item)tuple.data, htup->t_hoff + 8, off, false, true), off);
	}
	decoded->blocks[0].has_data = true;
	decoded->blocks[0].data = maintenance_data.data;
	if (opcode == XLOG_HEAP2_PRUNE) {
		xl_heap_prune *rec = (xl_heap_prune *)main_data.data;
		OffsetNumber offsets[] = { 1, 2, 3, 4 };
		rec->nredirected = 1;
		rec->ndead = 1;
		decoded->main_data_len = SizeOfHeapPrune;
		memcpy(maintenance_data.data, offsets, sizeof(offsets));
		decoded->blocks[0].data_len = sizeof(offsets);
	} else if (opcode == XLOG_HEAP2_VACUUM) {
		xl_heap_vacuum *rec = (xl_heap_vacuum *)main_data.data;
		OffsetNumber offsets[] = { 3, 4 };
		rec->nunused = 2;
		ItemIdSetDead(PageGetItemId(original.data, 3));
		ItemIdSetDead(PageGetItemId(original.data, 4));
		decoded->main_data_len = SizeOfHeapVacuum;
		memcpy(maintenance_data.data, offsets, sizeof(offsets));
		decoded->blocks[0].data_len = sizeof(offsets);
	} else {
		xl_heap_freeze_page *rec = (xl_heap_freeze_page *)main_data.data;
		xl_heap_freeze_plan *plans = (xl_heap_freeze_plan *)maintenance_data.data;
		OffsetNumber offsets[] = { 1, 2, 3 };
		rec->nplans = 2;
		plans[0].ntuples = 2;
		plans[0].t_infomask = HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID;
		plans[0].t_infomask2 = 1;
		plans[1] = plans[0];
		plans[1].ntuples = 1;
		plans[1].frzflags = XLH_INVALID_XVAC;
		htup = (HeapTupleHeader)PageGetItem(original.data, PageGetItemId(original.data, 3));
		htup->t_infomask |= HEAP_MOVED_OFF;
		decoded->main_data_len = SizeOfHeapFreezePage;
		memcpy(maintenance_data.data + 2 * sizeof(*plans), offsets, sizeof(offsets));
		decoded->blocks[0].data_len = 2 * sizeof(*plans) + sizeof(offsets);
	}
	native_page = detached_page = original;
}

UT_TEST(test_maintenance_matches_native)
{
	for (int shared = 0; shared < 2; shared++)
		for (int itl = 0; itl < 2; itl++) {
			cluster_shared_config = shared;
			reset_maintenance(XLOG_HEAP2_PRUNE, itl);
			compare_native(heap_xlog_prune);
			UT_ASSERT(ItemIdIsRedirected(PageGetItemId(native_page.data, 1)));
			UT_ASSERT(ItemIdIsDead(PageGetItemId(native_page.data, 3)));
			reset_maintenance(XLOG_HEAP2_VACUUM, itl);
			compare_native(heap_xlog_vacuum);
			UT_ASSERT_EQ(PageGetMaxOffsetNumber(native_page.data), 2);
			reset_maintenance(XLOG_HEAP2_FREEZE_PAGE, itl);
			compare_native(heap_xlog_freeze_page);
			UT_ASSERT(HeapTupleHeaderXminFrozen(tuple_at(native_page.data)));
		}
}

UT_TEST(test_maintenance_terminal_shapes_match_native)
{
	HeapTupleHeader tuple;
	xl_heap_prune *prune;
	OffsetNumber all[] = { 1, 2, 3, 4 };
	reset_maintenance(XLOG_HEAP2_PRUNE, true);
	/* The root was already redirected by an earlier prune cycle. */
	ItemIdSetRedirect(PageGetItemId(original.data, 1), 4);
	native_page = detached_page = original;
	compare_native(heap_xlog_prune);
	reset_maintenance(XLOG_HEAP2_PRUNE, true);
	prune = (xl_heap_prune *)main_data.data;
	prune->nredirected = 0;
	prune->ndead = 4;
	memcpy(maintenance_data.data, all, sizeof(all));
	for (OffsetNumber off = 1; off <= 4; off++) {
		tuple = (HeapTupleHeader)PageGetItem(original.data, PageGetItemId(original.data, off));
		HeapTupleHeaderClearHeapOnly(tuple);
	}
	native_page = detached_page = original;
	compare_native(heap_xlog_prune);
	reset_maintenance(XLOG_HEAP2_VACUUM, true);
	((xl_heap_vacuum *)main_data.data)->nunused = 4;
	memcpy(maintenance_data.data, all, sizeof(all));
	decoded->blocks[0].data_len = sizeof(all);
	for (OffsetNumber off = 1; off <= 4; off++)
		ItemIdSetDead(PageGetItemId(original.data, off));
	native_page = detached_page = original;
	compare_native(heap_xlog_vacuum);
	/* Native truncation retains the first unused line pointer. */
	UT_ASSERT_EQ(PageGetMaxOffsetNumber(native_page.data), 1);
	UT_ASSERT(!ItemIdIsUsed(PageGetItemId(native_page.data, 1)));
	reset_maintenance(XLOG_HEAP2_FREEZE_PAGE, true);
	((xl_heap_freeze_plan *)maintenance_data.data)[1].frzflags = XLH_FREEZE_XVAC;
	compare_native(heap_xlog_freeze_page);
}

UT_TEST(test_bad_maintenance_does_not_modify_output)
{
	for (int bad = 0; bad < 22; bad++) {
		PGAlignedBlock saved;
		ClusterBlkApplyResult result;
		reset_maintenance(XLOG_HEAP2_PRUNE, true);
		switch (bad) {
		case 0:
			decoded->main_data_len--;
			break;
		case 1:
			decoded->blocks[0].data_len--;
			break;
		case 2:
			((xl_heap_prune *)main_data.data)->nredirected = 10;
			break;
		case 3:
			((OffsetNumber *)maintenance_data.data)[0] = 0;
			break;
		case 4:
			((OffsetNumber *)maintenance_data.data)[1] = 5;
			break;
		case 5:
			((OffsetNumber *)maintenance_data.data)[3] = 2;
			break;
		case 6:
			reset_maintenance(XLOG_HEAP2_VACUUM, true);
			((OffsetNumber *)maintenance_data.data)[0] = 1;
			break;
		case 7:
			reset_maintenance(XLOG_HEAP2_FREEZE_PAGE, true);
			decoded->blocks[0].data_len--;
			break;
		case 8:
			reset_maintenance(XLOG_HEAP2_FREEZE_PAGE, true);
			((xl_heap_freeze_plan *)maintenance_data.data)[1].ntuples = 10;
			break;
		case 9:
			reset_maintenance(XLOG_HEAP2_FREEZE_PAGE, true);
			((OffsetNumber *)(maintenance_data.data + 2 * sizeof(xl_heap_freeze_plan)))[2] = 0;
			break;
		case 10:
			((PageHeader)detached_page.data)->pd_pagesize_version = 0;
			break;
		case 11:
			decoded->blocks[0].has_data = false;
			break;
		case 12:
			/* Overlapping physical tuples must not reach native compaction. */
			*PageGetItemId(detached_page.data, 3) = *PageGetItemId(detached_page.data, 2);
			break;
		case 13:
			((OffsetNumber *)maintenance_data.data)[2] = 1;
			break;
		case 14:
			reset_maintenance(XLOG_HEAP2_FREEZE_PAGE, true);
			((OffsetNumber *)(maintenance_data.data + 2 * sizeof(xl_heap_freeze_plan)))[2] = 1;
			break;
		case 15:
			reset_maintenance(XLOG_HEAP2_FREEZE_PAGE, true);
			((xl_heap_freeze_plan *)maintenance_data.data)[1].frzflags = 128;
			break;
		case 16:
			reset_maintenance(XLOG_HEAP2_FREEZE_PAGE, true);
			decoded->blocks[0].data_len += 2;
			break;
		case 17:
			reset_maintenance(XLOG_HEAP2_FREEZE_PAGE, true);
			((xl_heap_freeze_plan *)maintenance_data.data)[0].frzflags = XLH_FREEZE_XVAC;
			break;
		case 18:
			reset_maintenance(XLOG_HEAP2_VACUUM, true);
			((OffsetNumber *)maintenance_data.data)[1] = 3;
			break;
		case 19:
			((OffsetNumber *)maintenance_data.data)[0] = 65535;
			break;
		case 20:
		case 21: {
			PGAlignedBlock small;
			HeapTupleHeader tuple = (HeapTupleHeader)small.data;
			/* No native compaction in this RED: show its shared preflight
			 * wrongly accepts more tuples than the native work-array limit. */
			reset_maintenance(bad == 20 ? XLOG_HEAP2_VACUUM : XLOG_HEAP2_PRUNE, false);
			PageInit(detached_page.data, BLCKSZ, 0);
			memset(&small, 0, sizeof(small));
			tuple->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
			for (OffsetNumber off = 1; off <= MaxHeapTuplesPerPage + 2; off++)
				UT_ASSERT_EQ(
					PageAddItem(detached_page.data, (Item)tuple, tuple->t_hoff, off, false, false),
					off);
			if (bad == 20) {
				ItemIdSetDead(PageGetItemId(detached_page.data, 3));
				ItemIdSetDead(PageGetItemId(detached_page.data, 4));
			} else {
				((xl_heap_prune *)main_data.data)->nredirected = 0;
				((xl_heap_prune *)main_data.data)->ndead = 1;
				decoded->blocks[0].data_len = sizeof(OffsetNumber);
			}
			break;
		}
		}
		saved = detached_page;
		result = cluster_block_apply_one(&reader, 0, detached_page.data);
		UT_ASSERT(result == CLUSTER_BLKAPPLY_FAILED || result == CLUSTER_BLKAPPLY_UNSUPPORTED);
		UT_ASSERT(memcmp(saved.data, detached_page.data, BLCKSZ) == 0);
	}
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_confirm_and_inplace_match_native);
	UT_RUN(test_lock_and_updated_lock_match_native);
	UT_RUN(test_zero_length_inplace_payload_matches_native);
	UT_RUN(test_replaced_locker_refs_and_history_match_native);
	UT_RUN(test_bad_shapes_do_not_modify_output);
	UT_RUN(test_maintenance_matches_native);
	UT_RUN(test_maintenance_terminal_shapes_match_native);
	UT_RUN(test_bad_maintenance_does_not_modify_output);
	UT_DONE();
	free(decoded);
	return ut_failed_count != 0;
}
