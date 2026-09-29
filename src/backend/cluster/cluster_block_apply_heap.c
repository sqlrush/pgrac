/*-------------------------------------------------------------------------
 *
 * cluster_block_apply_heap.c
 *	  pgrac heap (RM_HEAP_ID) single-block redo-apply matrix (spec-4.10 D3b).
 *
 *	  One matrix entry per heap record type.  Each applies ONE delta record's
 *	  effect on the target block to a DETACHED char[BLCKSZ] page, mirroring the
 *	  BLK_NEEDS_REDO branch of the matching heap_xlog_* function (heapam.c) but
 *	  stripped of the buffer-pool fetch, visibility-map clear, FSM update and
 *	  critical section -- only the target block's bytes are touched.
 *
 *	  CORRECTNESS CONTRACT (8.A, R11 "极高"): for every supported record type
 *	  the result must be BYTE-FOR-BYTE identical to PG's real redo on that
 *	  block.  The original matrix has the crash-recovery differential in
 *	  src/test/cluster_tap/t/256; retained heap/heap2 variants also have
 *	  real native-redo differential unit coverage. PANIC conditions in the
 *	  original handler
 *	  become fail-closed (FAILED): a page we cannot rebuild exactly is never
 *	  installed.  Record types / flag combinations not on the differential are
 *	  fail-closed (UNSUPPORTED), never a silent wrong-block install.
 *
 *	  heapam.c is NOT modified (spec-4.10 §5 hard boundary); the per-block
 *	  logic is re-expressed here against a detached page.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_block_apply_heap.c
 *
 * NOTES
 *	  This is a pgrac-original file.  The per-block apply logic re-expresses
 *	  PostgreSQL's heap_xlog_* redo (src/backend/access/heap/heapam.c) for a
 *	  detached page; see that file for the authoritative redo semantics.
 *	  Spec: spec-4.10-online-block-recovery.md (FROZEN v0.4)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/heapam_xlog.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/rmgr.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "storage/bufpage.h"
#include "storage/itemptr.h"
#include "storage/off.h"

#include "cluster/cluster_block_apply.h"
#include "cluster/cluster_itl.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_format.h"

/*
 * fix_infomask_from_infobits -- local copy of heapam.c's static helper
 *		(unchanged); maps xl_heap_delete/lock infobits to infomask bits.
 */
static void
fix_infomask_from_infobits(uint8 infobits, uint16 *infomask, uint16 *infomask2)
{
	*infomask &= ~(HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK
				   | HEAP_XMAX_EXCL_LOCK);
	*infomask2 &= ~HEAP_KEYS_UPDATED;

	if (infobits & XLHL_XMAX_IS_MULTI)
		*infomask |= HEAP_XMAX_IS_MULTI;
	if (infobits & XLHL_XMAX_LOCK_ONLY)
		*infomask |= HEAP_XMAX_LOCK_ONLY;
	if (infobits & XLHL_XMAX_EXCL_LOCK)
		*infomask |= HEAP_XMAX_EXCL_LOCK;
	/* note HEAP_XMAX_SHR_LOCK isn't considered here */
	if (infobits & XLHL_XMAX_KEYSHR_LOCK)
		*infomask |= HEAP_XMAX_KEYSHR_LOCK;

	if (infobits & XLHL_KEYS_UPDATED)
		*infomask2 |= HEAP_KEYS_UPDATED;
}

/*
 * apply_heap_update -- mirror heap_xlog_update()'s BLK_NEEDS_REDO branches on a
 *		detached page, for the SAME-PAGE case only (old and new tuple on the
 *		one block being reconstructed).
 *
 *	Same-page is the dominant case (HOT updates, and any update with room on the
 *	page); both the old-tuple modification and the new-tuple insertion happen on
 *	block 0, so a single detached page is self-contained -- the new tuple's
 *	prefix/suffix come from the old tuple on the same page.
 *
 *	Off the starter matrix -> fail closed (UNSUPPORTED), not differential-proven:
 *	  - CROSS-PAGE update (separate old block 1): the new tuple's reconstruction
 *	    and the old tuple's update live on different pages; single-block
 *	    reconstruction of one cannot see the other.  A later matrix entry.
 *	  - XLOG_HEAP_INIT_PAGE, XLH_UPDATE_{OLD,NEW}_ALL_VISIBLE_CLEARED: as for
 *	    insert/delete, never reached on an FPI-base delta chain here.
 */
static ClusterBlkApplyResult
apply_heap_update(XLogReaderState *record, uint8 block_id, char *page, bool hot_update)
{
	xl_heap_update *xlrec = (xl_heap_update *)XLogRecGetData(record);
	BlockNumber oldblk;
	BlockNumber newblk;
	bool has_old_block;
	ItemPointerData newtid;
	OffsetNumber offnum;
	ItemId lp;
	HeapTupleData oldtup;
	HeapTupleHeader htup;
	uint16 prefixlen = 0;
	uint16 suffixlen = 0;
	char *newp;
	union {
		HeapTupleHeaderData hdr;
		/* cppcheck-suppress unusedStructMember */
		char data[MaxHeapTupleSize]; /* sizes the union for a max tuple */
	} tbuf;
	xl_heap_header xlhdr;
	uint32 newlen;
	char *recdata;
	const char *recdata_end;
	Size datalen;
	Size tuplen;
	bool cluster_itl_new_replay_active = false;
	uint8 cluster_itl_new_replay_slot = CLUSTER_ITL_SLOT_UNALLOCATED;

	oldtup.t_data = NULL;
	oldtup.t_len = 0;

	XLogRecGetBlockTag(record, 0, NULL, NULL, &newblk);
	has_old_block = XLogRecGetBlockTagExtended(record, 1, NULL, NULL, &oldblk, NULL);
	if (!has_old_block)
		oldblk = newblk;

	/* Starter matrix: same-page update only. */
	if (block_id != 0 || oldblk != newblk)
		return CLUSTER_BLKAPPLY_UNSUPPORTED;

	/* Off the matrix (see header): fail closed. */
	if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE)
		return CLUSTER_BLKAPPLY_UNSUPPORTED;
	if (xlrec->flags & (XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED | XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED))
		return CLUSTER_BLKAPPLY_UNSUPPORTED;

	ItemPointerSet(&newtid, newblk, xlrec->new_offnum);

	/* ---- old tuple version (same page) ---- */
	offnum = xlrec->old_offnum;
	if (PageGetMaxOffsetNumber(page) < offnum)
		return CLUSTER_BLKAPPLY_FAILED;
	lp = PageGetItemId(page, offnum);
	if (!ItemIdIsNormal(lp))
		return CLUSTER_BLKAPPLY_FAILED;

	htup = (HeapTupleHeader)PageGetItem(page, lp);
	oldtup.t_data = htup;
	oldtup.t_len = ItemIdGetLength(lp);

	htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	if (hot_update)
		HeapTupleHeaderSetHotUpdated(htup);
	else
		HeapTupleHeaderClearHotUpdated(htup);
	fix_infomask_from_infobits(xlrec->old_infobits_set, &htup->t_infomask, &htup->t_infomask2);
	HeapTupleHeaderSetXmax(htup, xlrec->old_xmax);
	HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
	/* Set forward chain link in t_ctid */
	htup->t_ctid = newtid;
	/* Mark the page as a candidate for pruning */
	PageSetPrunable(page, XLogRecGetXid(record));
	/* (same-page: the old-block ITL delta branch is cross-page only) */

	/* ---- new tuple version (same page) ---- */
	recdata = XLogRecGetBlockData(record, 0, &datalen);
	if (recdata == NULL)
		return CLUSTER_BLKAPPLY_FAILED;
	recdata_end = recdata + datalen;

	if (xlrec->flags & XLH_UPDATE_ITL_DELTA) {
		const char *itl_cursor = (char *)xlrec + SizeOfHeapUpdate;

		cluster_itl_new_replay_slot = (uint8)cluster_itl_wal_block_first_slot_idx(itl_cursor);
		cluster_itl_new_replay_active = true;
	}

	offnum = xlrec->new_offnum;
	if (PageGetMaxOffsetNumber(page) + 1 < offnum)
		return CLUSTER_BLKAPPLY_FAILED;

	if (xlrec->flags & XLH_UPDATE_PREFIX_FROM_OLD) {
		memcpy(&prefixlen, recdata, sizeof(uint16));
		recdata += sizeof(uint16);
	}
	if (xlrec->flags & XLH_UPDATE_SUFFIX_FROM_OLD) {
		memcpy(&suffixlen, recdata, sizeof(uint16));
		recdata += sizeof(uint16);
	}

	memcpy((char *)&xlhdr, recdata, SizeOfHeapHeader);
	recdata += SizeOfHeapHeader;

	tuplen = recdata_end - recdata;
	if (tuplen > MaxHeapTupleSize)
		return CLUSTER_BLKAPPLY_FAILED;

	htup = &tbuf.hdr;
	memset((char *)htup, 0, SizeofHeapTupleHeader);

	/*
	 * Reconstruct the new tuple from the prefix/suffix of the old tuple (on
	 * this same page) and the data in the WAL record.
	 */
	newp = (char *)htup + SizeofHeapTupleHeader;
	if (prefixlen > 0) {
		int len;

		/* copy bitmap [+ padding] [+ oid] from WAL record */
		len = xlhdr.t_hoff - SizeofHeapTupleHeader;
		memcpy(newp, recdata, len);
		recdata += len;
		newp += len;

		/* copy prefix from old tuple */
		memcpy(newp, (char *)oldtup.t_data + oldtup.t_data->t_hoff, prefixlen);
		newp += prefixlen;

		/* copy new tuple data from WAL record */
		len = tuplen - (xlhdr.t_hoff - SizeofHeapTupleHeader);
		memcpy(newp, recdata, len);
		recdata += len;
		newp += len;
	} else {
		/* copy bitmap [+ padding] [+ oid] + data from record, all in one go */
		memcpy(newp, recdata, tuplen);
		recdata += tuplen;
		newp += tuplen;
	}
	if (recdata != recdata_end)
		return CLUSTER_BLKAPPLY_FAILED;

	/* copy suffix from old tuple */
	if (suffixlen > 0)
		memcpy(newp, (char *)oldtup.t_data + oldtup.t_len - suffixlen, suffixlen);

	newlen = SizeofHeapTupleHeader + tuplen + prefixlen + suffixlen;
	htup->t_infomask2 = xlhdr.t_infomask2;
	htup->t_infomask = xlhdr.t_infomask;
	htup->t_hoff = xlhdr.t_hoff;
	ClusterHeapTupleHeaderInitItlSlot(htup);
	if (cluster_itl_new_replay_active)
		htup->t_itl_slot_idx = cluster_itl_new_replay_slot;

	HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
	HeapTupleHeaderSetCmin(htup, FirstCommandId);
	HeapTupleHeaderSetXmax(htup, xlrec->new_xmax);
	/* Make sure there is no forward chain link in t_ctid */
	htup->t_ctid = newtid;

	if (PageAddItem(page, (Item)htup, newlen, offnum, true, true) == InvalidOffsetNumber)
		return CLUSTER_BLKAPPLY_FAILED;

	/*
	 * Replay the block-local ITL delta from MAIN data (same-page: one array,
	 * patches the shared (page, top_xid) slot; htup_patch is the old tuple).
	 */
	if (xlrec->flags & XLH_UPDATE_ITL_DELTA) {
		const char *itl_cursor = (char *)xlrec + SizeOfHeapUpdate;

		cluster_itl_redo_apply_block_local_delta(page, oldtup.t_data, itl_cursor);
	}

	PageSetLSN(page, record->EndRecPtr);
	return CLUSTER_BLKAPPLY_OK;
}

static bool
heap_delta_page_header_valid(Page page)
{
	PageHeader header = (PageHeader)page;

	return !PageIsNew(page) && PageGetPageSize(page) == BLCKSZ
		   && PageGetPageLayoutVersion(page) == PG_PAGE_LAYOUT_VERSION
		   && header->pd_lower >= SizeOfPageHeaderData && header->pd_lower <= header->pd_upper
		   && header->pd_upper <= header->pd_special && header->pd_special <= BLCKSZ
		   && header->pd_special == MAXALIGN(header->pd_special)
		   && (header->pd_lower - SizeOfPageHeaderData) % sizeof(ItemIdData) == 0;
}

/* Validate a native heap item before any detached mutation or ITL scan. */
static HeapTupleHeader
heap_small_tuple(Page page, OffsetNumber offset)
{
	PageHeader header = (PageHeader)page;
	ItemId item;
	HeapTupleHeader tuple;
	Size start;
	Size length;

	if (!heap_delta_page_header_valid(page) || offset < FirstOffsetNumber
		|| offset > PageGetMaxOffsetNumber(page))
		return NULL;
	item = PageGetItemId(page, offset);
	start = ItemIdGetOffset(item);
	length = ItemIdGetLength(item);
	if (!ItemIdIsNormal(item) || length < SizeofHeapTupleHeader || start < header->pd_upper
		|| start != MAXALIGN(start) || start + length > header->pd_special)
		return NULL;
	tuple = (HeapTupleHeader)(page + start);
	if (tuple->t_hoff < SizeofHeapTupleHeader || tuple->t_hoff > length)
		return NULL;
	return tuple;
}

/* Validate the complete native ITL array before invoking its mutation helper.
 * DELETE/UPDATE may append logical tuple bytes, so return the exact extent. */
static bool
heap_delta_array_valid(Page page, const char *data, Size length, uint16 expected_flags,
					   bool require_nonempty, Size *consumed)
{
	xl_heap_itl_delta_block header;
	Size prefix = offsetof(xl_heap_itl_delta_block, deltas);
	Size width;
	uint8 seen = 0;

	if (length < prefix || !PageHasItl(page)
		|| ((PageHeader)page)->pd_special > BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE)
		return false;
	memcpy(&header, data, prefix);
	if (header.reserved != 0 || (require_nonempty && header.ndeltas == 0)
		|| header.ndeltas > CLUSTER_ITL_INITRANS_DEFAULT
		|| header.format_version > CLUSTER_ITL_DELTA_FORMAT_V4)
		return false;
	width = header.format_version == CLUSTER_ITL_DELTA_FORMAT_V1   ? sizeof(xl_heap_itl_delta)
			: header.format_version == CLUSTER_ITL_DELTA_FORMAT_V2 ? sizeof(xl_heap_itl_delta_v2)
																   : sizeof(xl_heap_itl_delta_v3);
	if (length < prefix + header.ndeltas * width)
		return false;
	for (uint16 i = 0; i < header.ndeltas; i++) {
		xl_heap_itl_delta delta;
		const char *entry = data + prefix + i * width;

		/* Every format has the same first 16 bytes. */
		memset(&delta, 0, sizeof(delta));
		memcpy(&delta, entry, offsetof(xl_heap_itl_delta, commit_scn));
		if (delta.slot_idx >= CLUSTER_ITL_INITRANS_DEFAULT || delta.flags_after != expected_flags
			|| !TransactionIdIsNormal(delta.xid))
			return false;
		if (header.format_version == CLUSTER_ITL_DELTA_FORMAT_V4) {
			xl_heap_itl_delta_v3 retained;
			uint32 segment, block;
			uint16 tt, row;

			memcpy(&retained, entry, sizeof(retained));
			if ((seen & (UINT8_C(1) << delta.slot_idx)) != 0 || !SCN_VALID(delta.write_scn)
				|| !uba_decode_record(retained.undo_segment_head, &segment, &block, &tt, &row)
				|| row >= (BLCKSZ - sizeof(UndoBlockHeader)) / sizeof(UndoSlotDirEntry))
				return false;
			seen |= UINT8_C(1) << delta.slot_idx;
		}
	}
	/* The native helper may clear references to a replaced terminal locker. */
	for (OffsetNumber off = FirstOffsetNumber; off <= PageGetMaxOffsetNumber(page); off++)
		if (ItemIdIsNormal(PageGetItemId(page, off)) && heap_small_tuple(page, off) == NULL)
			return false;
	*consumed = prefix + header.ndeltas * width;
	return true;
}

/* Native small-record effects, built privately. VM is a separate component
 * of the whole-record plan, never installed implicitly by this page codec. */
static ClusterBlkApplyResult
apply_heap_small(XLogReaderState *record, uint8 block_id, char *page, uint8 operation,
				 bool lock_updated)
{
	PGAlignedBlock scratch;
	const DecodedBkpBlock *block = XLogRecGetBlock(record, block_id);
	const char *main_data = XLogRecGetData(record);
	Size main_length = XLogRecGetDataLen(record);
	OffsetNumber offset;
	HeapTupleHeader tuple;

	if (block_id != 0 || block->forknum != MAIN_FORKNUM || (block->flags & BKPBLOCK_WILL_INIT) != 0
		|| (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) != 0)
		return CLUSTER_BLKAPPLY_FAILED;
	if (main_data == NULL)
		return CLUSTER_BLKAPPLY_FAILED;
	memcpy(scratch.data, page, BLCKSZ);
	if (operation == XLOG_HEAP_LOCK) {
		xl_heap_lock lock;
		Size base = lock_updated ? SizeOfHeapLockUpdated : SizeOfHeapLock;
		const char *delta;

		if (main_length < base)
			return CLUSTER_BLKAPPLY_FAILED;
		if (lock_updated) {
			xl_heap_lock_updated updated;
			memcpy(&updated, main_data, base);
			lock.xmax = updated.xmax;
			lock.offnum = updated.offnum;
			lock.infobits_set = updated.infobits_set;
			lock.flags = updated.flags;
		} else
			memcpy(&lock, main_data, base);
		offset = lock.offnum;
		tuple = heap_small_tuple(scratch.data, offset);
		if (tuple == NULL || (lock.flags & ~(XLH_LOCK_ALL_FROZEN_CLEARED | XLH_LOCK_ITL_DELTA)) != 0
			|| (lock.infobits_set
				& ~(XLHL_XMAX_IS_MULTI | XLHL_XMAX_LOCK_ONLY | XLHL_XMAX_EXCL_LOCK
					| XLHL_XMAX_KEYSHR_LOCK | XLHL_KEYS_UPDATED))
				   != 0)
			return CLUSTER_BLKAPPLY_FAILED;
		delta = main_data + base;
		if (lock.flags & XLH_LOCK_ITL_DELTA) {
			Size consumed;

			if (!heap_delta_array_valid(scratch.data, delta, main_length - base,
										ITL_FLAG_LOCK_ONLY_ACTIVE, false, &consumed)
				|| consumed != main_length - base)
				return CLUSTER_BLKAPPLY_FAILED;
		} else if (main_length != base)
			return CLUSTER_BLKAPPLY_FAILED;

		tuple->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
		tuple->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		fix_infomask_from_infobits(lock.infobits_set, &tuple->t_infomask, &tuple->t_infomask2);
		if (!lock_updated) {
			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)) {
				HeapTupleHeaderClearHotUpdated(tuple);
				ItemPointerSet(&tuple->t_ctid, block->blkno, offset);
			}
			HeapTupleHeaderSetCmax(tuple, FirstCommandId, false);
		}
		HeapTupleHeaderSetXmax(tuple, lock.xmax);
		if (lock.flags & XLH_LOCK_ITL_DELTA)
			cluster_itl_redo_apply_block_local_delta(scratch.data, NULL, delta);
	} else {
		/* CONFIRM and INPLACE both carry just the target OffsetNumber. */
		if (main_length != sizeof(offset))
			return CLUSTER_BLKAPPLY_FAILED;
		memcpy(&offset, main_data, sizeof(offset));
		tuple = heap_small_tuple(scratch.data, offset);
		if (tuple == NULL)
			return CLUSTER_BLKAPPLY_FAILED;
		if (operation == XLOG_HEAP_CONFIRM)
			ItemPointerSet(&tuple->t_ctid, block->blkno, offset);
		else {
			Size length;
			const char *data = XLogRecGetBlockData(record, block_id, &length);
			Size oldlen = ItemIdGetLength(PageGetItemId(scratch.data, offset)) - tuple->t_hoff;

			if (length != oldlen || (length != 0 && data == NULL))
				return CLUSTER_BLKAPPLY_FAILED;
			if (length != 0)
				memcpy((char *)tuple + tuple->t_hoff, data, length);
		}
	}
	PageSetLSN(scratch.data, record->EndRecPtr);
	memcpy(page, scratch.data, BLCKSZ);
	return CLUSTER_BLKAPPLY_OK;
}

/* PageRepairFragmentation assumes in-page, disjoint tuple extents. */
static bool
heap_maintenance_page_valid(Page page)
{
	bool occupied[BLCKSZ] = { false };

	if (!heap_delta_page_header_valid(page) || PageGetMaxOffsetNumber(page) > MaxHeapTuplesPerPage)
		return false;
	for (OffsetNumber off = FirstOffsetNumber; off <= PageGetMaxOffsetNumber(page); off++) {
		ItemId item = PageGetItemId(page, off);

		if (ItemIdIsNormal(item)) {
			Size start = ItemIdGetOffset(item);
			Size end = start + MAXALIGN(ItemIdGetLength(item));

			if (heap_small_tuple(page, off) == NULL || end > ((PageHeader)page)->pd_special)
				return false;
			for (Size byte = start; byte < end; byte++) {
				if (occupied[byte])
					return false;
				occupied[byte] = true;
			}
		} else if (ItemIdHasStorage(item))
			return false;
	}
	return true;
}

static bool
heap_maintenance_redirects_valid(Page page)
{
	for (OffsetNumber off = FirstOffsetNumber; off <= PageGetMaxOffsetNumber(page); off++) {
		ItemId item = PageGetItemId(page, off);

		if (ItemIdIsRedirected(item)) {
			HeapTupleHeader target = heap_small_tuple(page, ItemIdGetRedirect(item));

			if (target == NULL || !HeapTupleHeaderIsHeapOnly(target))
				return false;
		}
	}
	return true;
}

/* The maintenance owner already decided which tuples are removable/frozen.
 * Snapshot conflicts and FSM belong to the recovery orchestrator. */
static ClusterBlkApplyResult
apply_heap_maintenance(XLogReaderState *record, uint8 block_id, char *page, uint8 operation)
{
	PGAlignedBlock scratch;
	const DecodedBkpBlock *block = XLogRecGetBlock(record, block_id);
	const char *main_data = XLogRecGetData(record);
	Size main_length = XLogRecGetDataLen(record);
	Size length;
	const char *data = XLogRecGetBlockData(record, block_id, &length);
	bool modified[MaxOffsetNumber + 1] = { false };
	OffsetNumber maxoff;

	if (block_id != 0 || block->forknum != MAIN_FORKNUM || (block->flags & BKPBLOCK_WILL_INIT)
		|| (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) || main_data == NULL || data == NULL
		|| length == 0 || length > BLCKSZ || !heap_maintenance_page_valid(page))
		return CLUSTER_BLKAPPLY_FAILED;
	memcpy(scratch.data, page, BLCKSZ);
	maxoff = PageGetMaxOffsetNumber(scratch.data);
	if (operation == XLOG_HEAP2_FREEZE_PAGE) {
		xl_heap_freeze_page rec;
		Size offsets_start, cursor;

		if (main_length != SizeOfHeapFreezePage)
			return CLUSTER_BLKAPPLY_FAILED;
		memcpy(&rec, main_data, main_length);
		offsets_start = (Size)rec.nplans * sizeof(xl_heap_freeze_plan);
		if (rec.nplans == 0 || offsets_start > length
			|| (length - offsets_start) % sizeof(OffsetNumber) != 0)
			return CLUSTER_BLKAPPLY_FAILED;
		cursor = offsets_start;
		for (uint16 p = 0; p < rec.nplans; p++) {
			xl_heap_freeze_plan plan;

			memcpy(&plan, data + (Size)p * sizeof(plan), sizeof(plan));
			if (plan.ntuples == 0 || (plan.frzflags & ~(XLH_FREEZE_XVAC | XLH_INVALID_XVAC)) != 0
				|| (Size)plan.ntuples * sizeof(OffsetNumber) > length - cursor)
				return CLUSTER_BLKAPPLY_FAILED;
			for (uint16 t = 0; t < plan.ntuples; t++) {
				OffsetNumber off;
				HeapTupleHeader tuple;

				memcpy(&off, data + cursor, sizeof(off));
				cursor += sizeof(off);
				tuple = heap_small_tuple(scratch.data, off);
				if (tuple == NULL || modified[off]
					|| (plan.frzflags != 0 && (tuple->t_infomask & HEAP_MOVED) == 0))
					return CLUSTER_BLKAPPLY_FAILED;
				modified[off] = true;
				/* Exact heap_execute_freeze_tuple order, on private bytes. */
				HeapTupleHeaderSetXmax(tuple, plan.xmax);
				if (plan.frzflags & XLH_FREEZE_XVAC)
					HeapTupleHeaderSetXvac(tuple, FrozenTransactionId);
				if (plan.frzflags & XLH_INVALID_XVAC)
					HeapTupleHeaderSetXvac(tuple, InvalidTransactionId);
				tuple->t_infomask = plan.t_infomask;
				tuple->t_infomask2 = plan.t_infomask2;
			}
		}
		if (cursor != length)
			return CLUSTER_BLKAPPLY_FAILED;
	} else {
		OffsetNumber offsets[MaxOffsetNumber];
		uint16 nredirected = 0, ndead = 0;
		Size count = length / sizeof(OffsetNumber);
		Size cursor = 0;

		if (length % sizeof(OffsetNumber) != 0 || length > sizeof(offsets))
			return CLUSTER_BLKAPPLY_FAILED;
		memcpy(offsets, data, length);
		if (operation == XLOG_HEAP2_PRUNE) {
			xl_heap_prune rec;

			if (main_length != SizeOfHeapPrune)
				return CLUSTER_BLKAPPLY_FAILED;
			memcpy(&rec, main_data, main_length);
			nredirected = rec.nredirected;
			ndead = rec.ndead;
			if ((Size)nredirected * 2 + ndead > count)
				return CLUSTER_BLKAPPLY_FAILED;
		} else {
			xl_heap_vacuum rec;

			if (main_length != SizeOfHeapVacuum)
				return CLUSTER_BLKAPPLY_FAILED;
			memcpy(&rec, main_data, main_length);
			if (rec.nunused == 0 || rec.nunused != count)
				return CLUSTER_BLKAPPLY_FAILED;
		}
		for (uint16 i = 0; i < nredirected; i++) {
			OffsetNumber from = offsets[cursor++], to = offsets[cursor++];
			HeapTupleHeader target = heap_small_tuple(scratch.data, to);
			ItemId item;

			if (from < FirstOffsetNumber || from > maxoff || modified[from] || from == to
				|| target == NULL || !HeapTupleHeaderIsHeapOnly(target))
				return CLUSTER_BLKAPPLY_FAILED;
			item = PageGetItemId(scratch.data, from);
			if ((!ItemIdIsRedirected(item)
				 && (!ItemIdIsNormal(item)
					 || HeapTupleHeaderIsHeapOnly(heap_small_tuple(scratch.data, from))))
				|| (ItemIdIsRedirected(item) && ItemIdGetRedirect(item) == to))
				return CLUSTER_BLKAPPLY_FAILED;
			modified[from] = true;
			ItemIdSetRedirect(item, to);
		}
		for (Size i = cursor; i < count; i++) {
			OffsetNumber off = offsets[i];
			ItemId item;

			if (off < FirstOffsetNumber || off > maxoff || modified[off])
				return CLUSTER_BLKAPPLY_FAILED;
			modified[off] = true;
			item = PageGetItemId(scratch.data, off);
			if (operation == XLOG_HEAP2_VACUUM) {
				if (!ItemIdIsDead(item) || ItemIdHasStorage(item))
					return CLUSTER_BLKAPPLY_FAILED;
				ItemIdSetUnused(item);
			} else if (i - cursor < ndead) {
				if (!ItemIdIsRedirected(item)
					&& (!ItemIdIsNormal(item)
						|| HeapTupleHeaderIsHeapOnly(heap_small_tuple(scratch.data, off))))
					return CLUSTER_BLKAPPLY_FAILED;
				ItemIdSetDead(item);
			} else {
				if (!ItemIdIsNormal(item)
					|| !HeapTupleHeaderIsHeapOnly(heap_small_tuple(scratch.data, off)))
					return CLUSTER_BLKAPPLY_FAILED;
				ItemIdSetUnused(item);
			}
		}
		if (!heap_maintenance_redirects_valid(scratch.data))
			return CLUSTER_BLKAPPLY_FAILED;
		if (operation == XLOG_HEAP2_PRUNE)
			PageRepairFragmentation(scratch.data);
		else
			PageTruncateLinePointerArray(scratch.data);
	}
	PageSetLSN(scratch.data, record->EndRecPtr);
	memcpy(page, scratch.data, BLCKSZ);
	return CLUSTER_BLKAPPLY_OK;
}

/* INSERT and MULTI_INSERT differ only in native headers and tuple alignment.
 * INIT_PAGE constructs a private native heap page; namespace/version proof is
 * still the caller's obligation and cannot be inferred from an empty page. */
static ClusterBlkApplyResult
apply_heap_insert(XLogReaderState *record, uint8 block_id, char *page, bool multi)
{
	PGAlignedBlock scratch, tuple_space;
	const DecodedBkpBlock *block = XLogRecGetBlock(record, block_id);
	const char *main_data = XLogRecGetData(record);
	Size main_length = XLogRecGetDataLen(record), length;
	const char *data = XLogRecGetBlockData(record, block_id, &length);
	bool init = (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) != 0;
	xl_heap_insert single;
	xl_heap_multi_insert multiple;
	uint8 flags, itl_slot = CLUSTER_ITL_SLOT_UNALLOCATED;
	uint16 ntuples;
	Size base, cursor = 0;
	const char *delta;

	if (block_id != 0 || block->forknum != MAIN_FORKNUM
		|| init != ((block->flags & BKPBLOCK_WILL_INIT) != 0) || main_data == NULL || data == NULL)
		return CLUSTER_BLKAPPLY_FAILED;
	base = multi ? SizeOfHeapMultiInsert : SizeOfHeapInsert;
	if (main_length < base)
		return CLUSTER_BLKAPPLY_FAILED;
	if (multi) {
		memcpy(&multiple, main_data, base);
		flags = multiple.flags;
		ntuples = multiple.ntuples;
		if (!init)
			base += (Size)ntuples * sizeof(OffsetNumber);
	} else {
		memcpy(&single, main_data, base);
		flags = single.flags;
		ntuples = 1;
	}
	if (ntuples == 0 || ntuples > MaxHeapTuplesPerPage || main_length < base
		|| (flags
			& ~(XLH_INSERT_ALL_VISIBLE_CLEARED | XLH_INSERT_LAST_IN_MULTI
				| XLH_INSERT_IS_SPECULATIVE | XLH_INSERT_CONTAINS_NEW_TUPLE
				| XLH_INSERT_ON_TOAST_RELATION | XLH_INSERT_ALL_FROZEN_SET | XLH_INSERT_ITL_DELTA))
		|| ((flags & XLH_INSERT_ALL_FROZEN_SET)
			&& (!multi || (flags & XLH_INSERT_ALL_VISIBLE_CLEARED))))
		return CLUSTER_BLKAPPLY_FAILED;
	if (init)
		PageInitHeapPage(scratch.data, BLCKSZ, 0);
	else {
		if (!heap_maintenance_page_valid(page))
			return CLUSTER_BLKAPPLY_FAILED;
		memcpy(scratch.data, page, BLCKSZ);
	}
	delta = main_data + base;
	if (flags & XLH_INSERT_ITL_DELTA) {
		Size consumed;

		if (!heap_delta_array_valid(scratch.data, delta, main_length - base, ITL_FLAG_ACTIVE, true,
									&consumed)
			|| consumed != main_length - base)
			return CLUSTER_BLKAPPLY_FAILED;
		itl_slot = (uint8)cluster_itl_wal_block_first_slot_idx(delta);
	} else if (main_length != base)
		return CLUSTER_BLKAPPLY_FAILED;
	for (uint16 i = 0; i < ntuples; i++) {
		OffsetNumber off;
		Size payload, tuple_length;
		uint16 infomask, infomask2;
		uint8 hoff;
		HeapTupleHeader tuple = (HeapTupleHeader)tuple_space.data;

		if (multi) {
			xl_multi_insert_tuple header;

			if (init)
				off = FirstOffsetNumber + i;
			else
				memcpy(&off, main_data + SizeOfHeapMultiInsert + (Size)i * sizeof(off),
					   sizeof(off));
			cursor = SHORTALIGN(cursor);
			if (cursor > length || length - cursor < SizeOfMultiInsertTuple)
				return CLUSTER_BLKAPPLY_FAILED;
			memcpy(&header, data + cursor, SizeOfMultiInsertTuple);
			cursor += SizeOfMultiInsertTuple;
			payload = header.datalen;
			infomask = header.t_infomask;
			infomask2 = header.t_infomask2;
			hoff = header.t_hoff;
		} else {
			xl_heap_header header;

			off = single.offnum;
			if (length <= SizeOfHeapHeader)
				return CLUSTER_BLKAPPLY_FAILED;
			memcpy(&header, data, SizeOfHeapHeader);
			cursor = SizeOfHeapHeader;
			payload = length - cursor;
			infomask = header.t_infomask;
			infomask2 = header.t_infomask2;
			hoff = header.t_hoff;
		}
		tuple_length = SizeofHeapTupleHeader + payload;
		if (payload > length - cursor || tuple_length > MaxHeapTupleSize
			|| hoff < SizeofHeapTupleHeader || hoff > tuple_length || off < FirstOffsetNumber
			|| off > MaxHeapTuplesPerPage || off > PageGetMaxOffsetNumber(scratch.data) + 1
			|| (off <= PageGetMaxOffsetNumber(scratch.data)
				&& ItemIdIsUsed(PageGetItemId(scratch.data, off))))
			return CLUSTER_BLKAPPLY_FAILED;
		memset(tuple, 0, SizeofHeapTupleHeader);
		memcpy((char *)tuple + SizeofHeapTupleHeader, data + cursor, payload);
		cursor += payload;
		tuple->t_infomask = infomask;
		tuple->t_infomask2 = infomask2;
		tuple->t_hoff = hoff;
		ClusterHeapTupleHeaderInitItlSlot(tuple);
		if (flags & XLH_INSERT_ITL_DELTA)
			tuple->t_itl_slot_idx = itl_slot;
		HeapTupleHeaderSetXmin(tuple, XLogRecGetXid(record));
		HeapTupleHeaderSetCmin(tuple, FirstCommandId);
		ItemPointerSet(&tuple->t_ctid, block->blkno, off);
		if (PageAddItem(scratch.data, (Item)tuple, tuple_length, off, true, true)
			== InvalidOffsetNumber)
			return CLUSTER_BLKAPPLY_FAILED;
	}
	if (cursor != length)
		return CLUSTER_BLKAPPLY_FAILED;
	if (flags & XLH_INSERT_ITL_DELTA)
		cluster_itl_redo_apply_block_local_delta(
			scratch.data, multi ? NULL : (HeapTupleHeader)tuple_space.data, delta);
	PageSetLSN(scratch.data, record->EndRecPtr);
	if (flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
		PageClearAllVisible(scratch.data);
	if (flags & XLH_INSERT_ALL_FROZEN_SET)
		PageSetAllVisible(scratch.data);
	memcpy(page, scratch.data, BLCKSZ);
	return CLUSTER_BLKAPPLY_OK;
}

static ClusterBlkApplyResult
apply_heap_delete(XLogReaderState *record, uint8 block_id, char *page)
{
	PGAlignedBlock scratch;
	xl_heap_delete rec;
	const DecodedBkpBlock *block = XLogRecGetBlock(record, block_id);
	const char *main_data = XLogRecGetData(record);
	Size length = XLogRecGetDataLen(record), used = SizeOfHeapDelete;
	const char *delta;
	HeapTupleHeader tuple;

	if (block_id != 0 || block->forknum != MAIN_FORKNUM || (block->flags & BKPBLOCK_WILL_INIT)
		|| (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) || main_data == NULL || length < used)
		return CLUSTER_BLKAPPLY_FAILED;
	memcpy(&rec, main_data, used);
	memcpy(scratch.data, page, BLCKSZ);
	tuple = heap_small_tuple(scratch.data, rec.offnum);
	if (tuple == NULL
		|| (rec.flags
			& ~(XLH_DELETE_ALL_VISIBLE_CLEARED | XLH_DELETE_CONTAINS_OLD | XLH_DELETE_IS_SUPER
				| XLH_DELETE_IS_PARTITION_MOVE | XLH_DELETE_ITL_DELTA))
		|| (rec.infobits_set
			& ~(XLHL_XMAX_IS_MULTI | XLHL_XMAX_LOCK_ONLY | XLHL_XMAX_EXCL_LOCK
				| XLHL_XMAX_KEYSHR_LOCK | XLHL_KEYS_UPDATED)))
		return CLUSTER_BLKAPPLY_FAILED;
	delta = main_data + used;
	if (rec.flags & XLH_DELETE_ITL_DELTA) {
		Size consumed;

		if (!heap_delta_array_valid(scratch.data, delta, length - used, ITL_FLAG_ACTIVE, true,
									&consumed))
			return CLUSTER_BLKAPPLY_FAILED;
		used += consumed;
	}
	if (!(rec.flags & XLH_DELETE_CONTAINS_OLD) && used != length)
		return CLUSTER_BLKAPPLY_FAILED;
	tuple->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	tuple->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	HeapTupleHeaderClearHotUpdated(tuple);
	fix_infomask_from_infobits(rec.infobits_set, &tuple->t_infomask, &tuple->t_infomask2);
	if (rec.flags & XLH_DELETE_IS_SUPER)
		HeapTupleHeaderSetXmin(tuple, InvalidTransactionId);
	else
		HeapTupleHeaderSetXmax(tuple, rec.xmax);
	HeapTupleHeaderSetCmax(tuple, FirstCommandId, false);
	PageSetPrunable(scratch.data, XLogRecGetXid(record));
	if (rec.flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
		PageClearAllVisible(scratch.data);
	if (rec.flags & XLH_DELETE_IS_PARTITION_MOVE)
		HeapTupleHeaderSetMovedPartitions(tuple);
	else
		ItemPointerSet(&tuple->t_ctid, block->blkno, rec.offnum);
	if (rec.flags & XLH_DELETE_ITL_DELTA)
		cluster_itl_redo_apply_block_local_delta(scratch.data, tuple, delta);
	PageSetLSN(scratch.data, record->EndRecPtr);
	memcpy(page, scratch.data, BLCKSZ);
	return CLUSTER_BLKAPPLY_OK;
}

/*
 * cluster_block_apply_heap -- dispatch a no-image heap delta to its per-record
 *		single-block applicator.  Record types not on the differential matrix
 *		fail closed (8.A / R11).
 *
 *
 *	WAL trust boundary (8.A threat model): online recovery rebuilds a corrupt
 *	PAGE from CRC-validated WAL (the only caller reads records via
 *	XLogReadRecord, which validates each record's CRC); the WAL is the trusted
 *	source of truth.  Record-internal structure (incl. the ITL delta array read
 *	by cluster_itl_redo_apply_block_local_delta) is therefore trusted to the
 *	same degree as PG's own heap_xlog_* redo, which uses the identical helper.
 *	A future caller that ingests WAL through any path weaker than XLogReadRecord
 *	must add ITL-region bounds before reaching these handlers.
 */
ClusterBlkApplyResult
cluster_block_apply_heap(XLogReaderState *record, uint8 block_id, char *page)
{
	uint8 info = XLogRecGetInfo(record) & XLOG_HEAP_OPMASK;

	if (XLogRecGetRmid(record) == RM_HEAP2_ID) {
		switch (info) {
		case XLOG_HEAP2_LOCK_UPDATED:
			return apply_heap_small(record, block_id, page, XLOG_HEAP_LOCK, true);
		case XLOG_HEAP2_MULTI_INSERT:
			return apply_heap_insert(record, block_id, page, true);
		case XLOG_HEAP2_PRUNE:
		case XLOG_HEAP2_VACUUM:
		case XLOG_HEAP2_FREEZE_PAGE:
			return apply_heap_maintenance(record, block_id, page, info);
		default:
			return CLUSTER_BLKAPPLY_UNSUPPORTED;
		}
	}
	if (XLogRecGetRmid(record) != RM_HEAP_ID)
		return CLUSTER_BLKAPPLY_UNSUPPORTED;
	switch (info) {
	case XLOG_HEAP_CONFIRM:
	case XLOG_HEAP_LOCK:
	case XLOG_HEAP_INPLACE:
		return apply_heap_small(record, block_id, page, info, false);

	case XLOG_HEAP_INSERT:
		return apply_heap_insert(record, block_id, page, false);

	case XLOG_HEAP_DELETE:
		return apply_heap_delete(record, block_id, page);

	case XLOG_HEAP_UPDATE:
		return apply_heap_update(record, block_id, page, false);

	case XLOG_HEAP_HOT_UPDATE:
		return apply_heap_update(record, block_id, page, true);

	default:
		return CLUSTER_BLKAPPLY_UNSUPPORTED;
	}
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
