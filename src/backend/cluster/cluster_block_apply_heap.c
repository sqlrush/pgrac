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
 *	  block.  This is proven by the crash-recovery differential in
 *	  src/test/cluster_tap/t/256.  PANIC conditions in the original handler
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
 * apply_heap_insert -- mirror heap_xlog_insert()'s BLK_NEEDS_REDO branch on a
 *		detached page.
 *
 *	The detached page already holds the FPI base and any prior deltas, so there
 *	is no buffer fetch; the visibility-map clear and FSM update are skipped
 *	(separable hints, re-established by the normal path after install).  PANIC
 *	conditions become fail-closed (8.A).
 *
 *	Off the starter matrix -> fail closed (UNSUPPORTED), not differential-proven:
 *	  - XLOG_HEAP_INIT_PAGE: reinitializes a fresh page; an FPI-base chain never
 *	    contains it (INIT records carry no image and start from an empty page).
 *	  - XLH_INSERT_ALL_VISIBLE_CLEARED: only set on the FIRST touch of an
 *	    all-visible page, which also bears the FPI and is handled by the FPI
 *	    path -- never reached here as a delta.
 */
static ClusterBlkApplyResult
apply_heap_insert(XLogReaderState *record, uint8 block_id, char *page)
{
	xl_heap_insert *xlrec = (xl_heap_insert *)XLogRecGetData(record);
	BlockNumber blkno;
	ItemPointerData target_tid;
	char *data;
	Size datalen;
	xl_heap_header xlhdr;
	union {
		HeapTupleHeaderData hdr;
		char data[MaxHeapTupleSize];
	} tbuf;
	HeapTupleHeader htup;
	uint32 newlen;
	uint8 cluster_itl_replay_slot = CLUSTER_ITL_SLOT_UNALLOCATED;
	bool cluster_itl_replay_active = false;

	/* A heap INSERT references only block 0. */
	if (block_id != 0)
		return CLUSTER_BLKAPPLY_UNSUPPORTED;

	/* Off the matrix (see header): fail closed. */
	if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE)
		return CLUSTER_BLKAPPLY_UNSUPPORTED;
	if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
		return CLUSTER_BLKAPPLY_UNSUPPORTED;

	XLogRecGetBlockTag(record, 0, NULL, NULL, &blkno);
	ItemPointerSetBlockNumber(&target_tid, blkno);
	ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

	/* The base page must already exist and have room for this offset. */
	if (PageGetMaxOffsetNumber(page) + 1 < xlrec->offnum)
		return CLUSTER_BLKAPPLY_FAILED;

	data = XLogRecGetBlockData(record, 0, &datalen);
	if (data == NULL || datalen <= SizeOfHeapHeader)
		return CLUSTER_BLKAPPLY_FAILED;

	if (xlrec->flags & XLH_INSERT_ITL_DELTA) {
		const char *itl_start = (char *)xlrec + SizeOfHeapInsert;

		/* slot_idx is at offset 0 of both v1 and v2 ITL deltas. */
		cluster_itl_replay_slot = (uint8)cluster_itl_wal_block_first_slot_idx(itl_start);
		cluster_itl_replay_active = true;
	}

	newlen = datalen - SizeOfHeapHeader;
	if (newlen > MaxHeapTupleSize)
		return CLUSTER_BLKAPPLY_FAILED;
	memcpy((char *)&xlhdr, data, SizeOfHeapHeader);
	data += SizeOfHeapHeader;

	htup = &tbuf.hdr;
	MemSet((char *)htup, 0, SizeofHeapTupleHeader);
	/* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
	memcpy((char *)htup + SizeofHeapTupleHeader, data, newlen);
	newlen += SizeofHeapTupleHeader;
	htup->t_infomask2 = xlhdr.t_infomask2;
	htup->t_infomask = xlhdr.t_infomask;
	htup->t_hoff = xlhdr.t_hoff;
	/* WAL header does not carry t_itl_slot_idx; mirror heap_xlog_insert. */
	ClusterHeapTupleHeaderInitItlSlot(htup);
	if (cluster_itl_replay_active)
		htup->t_itl_slot_idx = cluster_itl_replay_slot;
	HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
	HeapTupleHeaderSetCmin(htup, FirstCommandId);
	htup->t_ctid = target_tid;

	if (PageAddItem(page, (Item)htup, newlen, xlrec->offnum, true, true) == InvalidOffsetNumber)
		return CLUSTER_BLKAPPLY_FAILED;

	/* Replay the block-local ITL delta array (spec-3.4a/3.4b), as redo does. */
	if (xlrec->flags & XLH_INSERT_ITL_DELTA) {
		const char *itl_start = (char *)xlrec + SizeOfHeapInsert;

		cluster_itl_redo_apply_block_local_delta(page, htup, itl_start);
	}

	PageSetLSN(page, record->EndRecPtr);
	return CLUSTER_BLKAPPLY_OK;
}

/*
 * apply_heap_delete -- mirror heap_xlog_delete()'s BLK_NEEDS_REDO branch on a
 *		detached page.  Marks the target tuple deleted (xmax/infomask/ctid),
 *		then replays the block-local ITL delta.
 *
 *	Off the starter matrix -> fail closed (UNSUPPORTED), not differential-proven:
 *	  - XLH_DELETE_ALL_VISIBLE_CLEARED (only on the FPI-bearing first touch),
 *	  - XLH_DELETE_IS_PARTITION_MOVE  (row movement; sets MovedPartitions),
 *	  - XLH_DELETE_IS_SUPER           (speculative-insert abort; clears xmin).
 */
static ClusterBlkApplyResult
apply_heap_delete(XLogReaderState *record, uint8 block_id, char *page)
{
	xl_heap_delete *xlrec = (xl_heap_delete *)XLogRecGetData(record);
	BlockNumber blkno;
	ItemPointerData target_tid;
	ItemId lp;
	HeapTupleHeader htup;

	/* A heap DELETE references only block 0. */
	if (block_id != 0)
		return CLUSTER_BLKAPPLY_UNSUPPORTED;

	/* Off the matrix (see header): fail closed. */
	if (xlrec->flags
		& (XLH_DELETE_ALL_VISIBLE_CLEARED | XLH_DELETE_IS_PARTITION_MOVE | XLH_DELETE_IS_SUPER))
		return CLUSTER_BLKAPPLY_UNSUPPORTED;

	XLogRecGetBlockTag(record, 0, NULL, NULL, &blkno);
	ItemPointerSetBlockNumber(&target_tid, blkno);
	ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

	/* The target line pointer must exist and be a normal tuple. */
	if (PageGetMaxOffsetNumber(page) < xlrec->offnum)
		return CLUSTER_BLKAPPLY_FAILED;
	lp = PageGetItemId(page, xlrec->offnum);
	if (!ItemIdIsNormal(lp))
		return CLUSTER_BLKAPPLY_FAILED;

	htup = (HeapTupleHeader)PageGetItem(page, lp);
	htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	HeapTupleHeaderClearHotUpdated(htup);
	fix_infomask_from_infobits(xlrec->infobits_set, &htup->t_infomask, &htup->t_infomask2);
	HeapTupleHeaderSetXmax(htup, xlrec->xmax);
	HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
	/* Mark the page as a candidate for pruning */
	PageSetPrunable(page, XLogRecGetXid(record));
	htup->t_ctid = target_tid;

	/* Replay the block-local ITL delta array (spec-3.4a/3.4b), as redo does. */
	if (xlrec->flags & XLH_DELETE_ITL_DELTA) {
		const char *itl_start = (char *)xlrec + SizeOfHeapDelete;

		cluster_itl_redo_apply_block_local_delta(page, htup, itl_start);
	}

	PageSetLSN(page, record->EndRecPtr);
	return CLUSTER_BLKAPPLY_OK;
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
		char data[MaxHeapTupleSize];
	} tbuf;
	xl_heap_header xlhdr;
	uint32 newlen;
	char *recdata;
	char *recdata_end;
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
	MemSet((char *)htup, 0, SizeofHeapTupleHeader);

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

/*
 * cluster_block_apply_heap -- dispatch a no-image heap delta to its per-record
 *		single-block applicator.  Record types not on the differential matrix
 *		fail closed (8.A / R11).
 *
 *	Deliberately OFF the starter matrix (all fall through to UNSUPPORTED, never a
 *	silent wrong-block install): XLOG_HEAP_LOCK / XLOG_HEAP_CONFIRM /
 *	XLOG_HEAP_INPLACE here, and the whole RM_HEAP2 family (multi-insert, prune,
 *	freeze, visible) at the delta dispatcher.  These DO mutate heap-block bytes,
 *	so a block whose own-thread chain contains one as a delta (no FPI) is
 *	correctly declared unrecoverable until a future differential-proven entry
 *	adds it.  The heap matrix is intentionally NOT complete.
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

	switch (info) {
	case XLOG_HEAP_INSERT:
		return apply_heap_insert(record, block_id, page);

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
