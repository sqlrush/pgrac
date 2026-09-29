/*-------------------------------------------------------------------------
 *
 * visibilitymap.c
 *	  bitmap for tracking visibility of heap tuples
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/visibilitymap.c
 *
 * INTERFACE ROUTINES
 *		visibilitymap_clear  - clear bits for one page in the visibility map
 *		visibilitymap_pin	 - pin a map page for setting a bit
 *		visibilitymap_pin_ok - check whether correct map page is already pinned
 *		visibilitymap_set	 - set a bit in a previously pinned page
 *		visibilitymap_get_status - get status of bits
 *		visibilitymap_count  - count number of bits set in visibility map
 *		visibilitymap_prepare_truncate -
 *			prepare for truncation of the visibility map
 *
 * PGRAC MODIFICATIONS by SqlRush <sqlrush@gmail.com>
 *
 *	visibilitymap_clear is split into a lock/unlock shell plus
 *	visibilitymap_clear_locked (the bit clear itself, caller holds the map
 *	page's content lock).  Heap mutators clear map bits from INSIDE their
 *	WAL critical section, and in cluster mode the LockBuffer hidden inside
 *	the old monolithic clear could need a cross-node PCM X transfer that
 *	can fail: an ereport(ERROR) with CritSectionCount > 0 escalates to
 *	PANIC and fail-stops the node (observed live: heap_update ->
 *	visibilitymap_clear -> X-transfer timeout -> PANIC).  The heapam
 *	callers therefore take the map page's content lock BEFORE
 *	START_CRIT_SECTION (where a cross-node acquire may safely ERROR) and
 *	call the _locked variant inside the critical section, releasing after
 *	END_CRIT_SECTION.  Holding the content lock across the section also
 *	makes the coverage IRREVOCABLE: a cluster BAST X->S downgrade is
 *	content-lock serialized, so no revoke can slip in between the pre-crit
 *	acquire and the in-crit clear (no residual PANIC window at all).
 *	Out-of-crit callers keep using visibilitymap_clear unchanged, and
 *	single-node builds see only the (behavior-identical) split.
 *
 *	Spec: spec-2.36-gcs-block-transfer.md
 *
 * NOTES
 *
 * The visibility map is a bitmap with two bits (all-visible and all-frozen)
 * per heap page. A set all-visible bit means that all tuples on the page are
 * known visible to all transactions, and therefore the page doesn't need to
 * be vacuumed. A set all-frozen bit means that all tuples on the page are
 * completely frozen, and therefore the page doesn't need to be vacuumed even
 * if whole table scanning vacuum is required (e.g. anti-wraparound vacuum).
 * The all-frozen bit must be set only when the page is already all-visible.
 *
 * The map is conservative in the sense that we make sure that whenever a bit
 * is set, we know the condition is true, but if a bit is not set, it might or
 * might not be true.
 *
 * Clearing visibility map bits is not separately WAL-logged.  The callers
 * must make sure that whenever a bit is cleared, the bit is cleared on WAL
 * replay of the updating operation as well.
 *
 * When we *set* a visibility map during VACUUM, we must write WAL.  This may
 * seem counterintuitive, since the bit is basically a hint: if it is clear,
 * it may still be the case that every tuple on the page is visible to all
 * transactions; we just don't know that for certain.  The difficulty is that
 * there are two bits which are typically set together: the PD_ALL_VISIBLE bit
 * on the page itself, and the visibility map bit.  If a crash occurs after the
 * visibility map page makes it to disk and before the updated heap page makes
 * it to disk, redo must set the bit on the heap page.  Otherwise, the next
 * insert, update, or delete on the heap page will fail to realize that the
 * visibility map bit must be cleared, possibly causing index-only scans to
 * return wrong answers.
 *
 * VACUUM will normally skip pages for which the visibility map bit is set;
 * such pages can't contain any dead tuples and therefore don't need vacuuming.
 *
 * LOCKING
 *
 * In heapam.c, whenever a page is modified so that not all tuples on the
 * page are visible to everyone anymore, the corresponding bit in the
 * visibility map is cleared. In order to be crash-safe, we need to do this
 * while still holding a lock on the heap page and in the same critical
 * section that logs the page modification. However, we don't want to hold
 * the buffer lock over any I/O that may be required to read in the visibility
 * map page.  To avoid this, we examine the heap page before locking it;
 * if the page-level PD_ALL_VISIBLE bit is set, we pin the visibility map
 * bit.  Then, we lock the buffer.  But this creates a race condition: there
 * is a possibility that in the time it takes to lock the buffer, the
 * PD_ALL_VISIBLE bit gets set.  If that happens, we have to unlock the
 * buffer, pin the visibility map page, and relock the buffer.  This shouldn't
 * happen often, because only VACUUM currently sets visibility map bits,
 * and the race will only occur if VACUUM processes a given page at almost
 * exactly the same time that someone tries to further modify it.
 *
 * To set a bit, you need to hold a lock on the heap page. That prevents
 * the race condition where VACUUM sees that all tuples on the page are
 * visible to everyone, but another backend modifies the page before VACUUM
 * sets the bit in the visibility map.
 *
 * When a bit is set, the LSN of the visibility map page is updated to make
 * sure that the visibility map update doesn't get written to disk before the
 * WAL record of the changes that made it possible to set the bit is flushed.
 * But when a bit is cleared, we don't have to do that because it's always
 * safe to clear a bit in the map from correctness point of view.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam_xlog.h"
#include "access/visibilitymap.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/inval.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_guc.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/storage/cluster_smgr.h"
#endif



/*#define TRACE_VISIBILITYMAP */

/*
 * Size of the bitmap on each visibility map page, in bytes. There's no
 * extra headers, so the whole page minus the standard page header is
 * used for the bitmap.
 */
#define MAPSIZE (BLCKSZ - MAXALIGN(SizeOfPageHeaderData))

/* Number of heap blocks we can represent in one byte */
#define HEAPBLOCKS_PER_BYTE (BITS_PER_BYTE / BITS_PER_HEAPBLOCK)

/* Number of heap blocks we can represent in one visibility map page. */
#define HEAPBLOCKS_PER_PAGE (MAPSIZE * HEAPBLOCKS_PER_BYTE)

/* Mapping from heap block number to the right bit in the visibility map */
#define HEAPBLK_TO_MAPBLOCK(x) ((x) / HEAPBLOCKS_PER_PAGE)
#define HEAPBLK_TO_MAPBYTE(x) (((x) % HEAPBLOCKS_PER_PAGE) / HEAPBLOCKS_PER_BYTE)
#define HEAPBLK_TO_OFFSET(x) (((x) % HEAPBLOCKS_PER_BYTE) * BITS_PER_HEAPBLOCK)

/* Masks for counting subsets of bits in the visibility map. */
#define VISIBLE_MASK64	UINT64CONST(0x5555555555555555) /* The lower bit of each
														 * bit pair */
#define FROZEN_MASK64	UINT64CONST(0xaaaaaaaaaaaaaaaa) /* The upper bit of each
														 * bit pair */

/* prototypes for internal routines */
static Buffer vm_readbuf(Relation rel, BlockNumber blkno, bool extend);
static Buffer vm_extend(Relation rel, BlockNumber vm_nblocks);
static void visibilitymap_set_locked(Relation rel, BlockNumber heapBlk, Buffer heapBuf,
									 XLogRecPtr recptr, Buffer vmBuf, TransactionId cutoff_xid,
									 uint8 flags);


/*
 *	visibilitymap_clear - clear specified bits for one page in visibility map
 *
 * You must pass a buffer containing the correct map page to this function.
 * Call visibilitymap_pin first to pin the right one. This function doesn't do
 * any I/O.  Returns true if any bits have been cleared and false otherwise.
 */
bool
visibilitymap_clear(Relation rel, BlockNumber heapBlk, Buffer vmbuf, uint8 flags)
{
	BlockNumber mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);
	bool		cleared = false;

	/* Must never clear all_visible bit while leaving all_frozen bit set */
	Assert(flags & VISIBILITYMAP_VALID_BITS);
	Assert(flags != VISIBILITYMAP_ALL_VISIBLE);

#ifdef TRACE_VISIBILITYMAP
	elog(DEBUG1, "vm_clear %s %d", RelationGetRelationName(rel), heapBlk);
#endif

	if (!BufferIsValid(vmbuf) || BufferGetBlockNumber(vmbuf) != mapBlock)
		elog(ERROR, "wrong buffer passed to visibilitymap_clear");

	LockBuffer(vmbuf, BUFFER_LOCK_EXCLUSIVE);
	cleared = visibilitymap_clear_locked(rel, heapBlk, vmbuf, flags);
	LockBuffer(vmbuf, BUFFER_LOCK_UNLOCK);

	return cleared;
}

#ifdef USE_PGRAC_CLUSTER
/* PGRAC: VM has no items, special area, or ITL. The bitmap fills the page. */
static bool
vm_redo_page_valid(Page page)
{
	PageHeader header = (PageHeader) page;

	return PageGetPageSize(page) == BLCKSZ &&
		PageGetPageLayoutVersion(page) == PG_PAGE_LAYOUT_VERSION &&
		header->pd_lower == MAXALIGN(SizeOfPageHeaderData) &&
		header->pd_upper == BLCKSZ && header->pd_special == BLCKSZ &&
		header->pd_prune_xid == InvalidTransactionId &&
		(header->pd_flags & ~(PD_CLUSTER_FORCE_FPI | PD_LSN_ORIGIN_VALID | PD_LSN_ORIGIN_MASK)) == 0 &&
		((header->pd_flags & PD_LSN_ORIGIN_VALID) != 0 ||
		 (header->pd_flags & PD_LSN_ORIGIN_MASK) == 0) &&
		header->pd_block_scn != 0;
}

/* PGRAC: apply the already-selected VM edge, not a runtime mutation. Global
 * recovery isolation/dependency selection remain the executor's obligation.
 * No numeric token/foreign LSN comparison, new identity, token, or WAL. */
static void
vm_clear_versioned_redo(XLogReaderState *record, RelFileLocator locator,
						BlockNumber heapBlk, uint8 flags)
{
	BlockNumber mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);
	ClusterSpaceIdentity identity;
	const RfPageVersionEdgeV1 *edge;
	const RfPageVersionEdgeEntryV1 *entry = NULL;
	PGAlignedBlock image;
	Buffer buffer;
	Page page;
	int block_id = -1;
	int i;
	uint8 mask = flags << HEAPBLK_TO_OFFSET(heapBlk);

	for (i = 0; i <= XLogRecMaxBlockId(record); i++)
	{
		DecodedBkpBlock *block;

		if (!XLogRecHasBlockRef(record, i))
			continue;
		block = XLogRecGetBlock(record, i);
		if (block->forknum != VISIBILITYMAP_FORKNUM || block->blkno != mapBlock ||
			!RelFileLocatorEquals(block->rlocator, locator))
			continue;
		if (block_id != -1)
			elog(ERROR, "shared VM redo has duplicate target images");
		block_id = i;
	}
	/* A heap record can carry only a heap edge. Conversely, a pinned VM can
	 * have a new recorded version even when its bits were already clear. */
	if (block_id < 0 && flags == 0)
		return;
	if (!RecoveryInProgress() || !XLogRecHasPageVersionEdge(record) ||
		XLogRecPtrIsInvalid(record->EndRecPtr) ||
		!cluster_space_relation_read_redo_identity(locator, &identity))
		elog(ERROR, "shared VM redo requires restart SPACE identity and version edge");
	edge = XLogRecGetPageVersionEdge(record);
	for (i = 0; i < edge->entry_count; i++)
	{
		if (edge->entries[i].block_id != block_id)
			continue;
		if (entry != NULL)
			elog(ERROR, "shared VM redo has duplicate version edges");
		entry = &edge->entries[i];
	}
	if (block_id < 0 || entry == NULL ||
		entry->page_class != RF_PAGE_CLASS_ORDINARY ||
		entry->before_kind != RF_PAGE_STATE_PRESENT ||
		entry->result_kind != RF_PAGE_STATE_PRESENT ||
		entry->edge_flags != (RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE) ||
		entry->component_ordinal != XLogRecGetBlock(record, block_id)->component_ordinal ||
		entry->before.mutation_token == 0 || edge->result_token == 0 ||
		entry->before.mutation_token == edge->result_token ||
		memcmp(entry->before.segment_incarnation, identity.incarnation, 16) != 0 ||
		memcmp(entry->result_incarnation, identity.incarnation, 16) != 0 ||
		!XLogRecHasBlockImage(record, block_id) || !XLogRecBlockImageApply(record, block_id) ||
		(XLogRecGetBlock(record, block_id)->flags & BKPBLOCK_WILL_INIT) != 0 ||
		!RestoreBlockImage(record, block_id, image.data) || !vm_redo_page_valid(image.data) ||
		((PageHeader) image.data)->pd_block_scn != edge->result_token ||
		(PageGetContents(image.data)[HEAPBLK_TO_MAPBYTE(heapBlk)] & mask) != 0)
		elog(ERROR, "shared VM redo has no exact recorded result image");

	/* Unlike XLogReadBufferForRedo, this cannot restore the FPI before checking
	 * its predecessor. Missing/zero/torn bases require the separate recovery
	 * base/initialization owner; do not invent one from a fake Relation. */
	buffer = XLogReadBufferExtended(locator, VISIBILITYMAP_FORKNUM, mapBlock,
								   RBM_NORMAL, InvalidBuffer);
	if (!BufferIsValid(buffer))
		elog(ERROR, "shared VM redo is missing its versioned predecessor");
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buffer);
	if (!vm_redo_page_valid(page) ||
		(((PageHeader) page)->pd_block_scn != entry->before.mutation_token &&
		 ((PageHeader) page)->pd_block_scn != edge->result_token))
	{
		UnlockReleaseBuffer(buffer);
		elog(ERROR, "shared VM redo predecessor version does not match");
	}
	if (((PageHeader) page)->pd_block_scn == edge->result_token)
	{
		/* LSN, origin and checksum can legitimately differ after replay/write;
		 * a matching token with different bitmap bytes is not an idempotent hit. */
		bool same = memcmp(PageGetContents(page), PageGetContents(image.data), MAPSIZE) == 0;

		UnlockReleaseBuffer(buffer);
		if (!same)
			elog(ERROR, "shared VM redo result token has different contents");
		return;
	}
	START_CRIT_SECTION();
	memcpy(page, image.data, BLCKSZ);
	PageSetLSN(page, record->EndRecPtr);
	/* The legacy merged-recovery LSN hook must not replace the result token. */
	((PageHeader) page)->pd_block_scn = edge->result_token;
	MarkBufferDirty(buffer);
	END_CRIT_SECTION();
	UnlockReleaseBuffer(buffer);
}
#endif

void
visibilitymap_clear_redo(XLogReaderState *record, RelFileLocator locator,
						 BlockNumber heapBlk, uint8 flags)
{
	Relation rel;
	Buffer buffer = InvalidBuffer;

	Assert(RecoveryInProgress());
	Assert(flags == 0 || flags == VISIBILITYMAP_VALID_BITS || flags == VISIBILITYMAP_ALL_FROZEN);
#ifdef USE_PGRAC_CLUSTER
	if (cluster_shared_config && cluster_smgr_which_for(locator, InvalidBackendId) == 1)
	{
		vm_clear_versioned_redo(record, locator, heapBlk, flags);
		return;
	}
#endif
	if (flags == 0)
		return;
	/* Preserve the native unversioned profile's implicit, conservative clear. */
	rel = CreateFakeRelcacheEntry(locator);
	visibilitymap_pin(rel, heapBlk, &buffer);
	visibilitymap_clear(rel, heapBlk, buffer, flags);
	ReleaseBuffer(buffer);
	FreeFakeRelcacheEntry(rel);
}

bool
visibilitymap_clear_retry_aware(Relation rel, BlockNumber heapBlk, Buffer *vmbuf, uint8 flags,
								struct ResourceXAuxiliaryAcquireContext *context, bool *cleared)
{
	Assert(vmbuf != NULL && cleared != NULL && context != NULL);
	*cleared = false;
	if (!BufferIsValid(*vmbuf) || !visibilitymap_pin_ok(heapBlk, *vmbuf))
		elog(ERROR, "wrong buffer passed to visibilitymap_clear_retry_aware");
	if (!ClusterLockBufferExclusiveAuxiliaryAware(vmbuf, context))
		return false;
	*cleared = visibilitymap_clear_locked(rel, heapBlk, *vmbuf, flags);
	LockBuffer(*vmbuf, BUFFER_LOCK_UNLOCK);
	return true;
}

/*
 *	visibilitymap_clear_locked - clear bits, map page content lock HELD
 *
 * PGRAC: the body of visibilitymap_clear without the lock/unlock shell, for
 * callers that must clear from inside a WAL critical section: they acquire
 * the map page's content lock BEFORE the section (where a cluster PCM
 * cross-node acquire may safely ereport) and release it after, so no
 * failure-capable work remains inside (see the file header).
 */
bool
visibilitymap_clear_locked(Relation rel, BlockNumber heapBlk, Buffer vmbuf, uint8 flags)
{
	BlockNumber mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);
	int			mapByte = HEAPBLK_TO_MAPBYTE(heapBlk);
	int			mapOffset = HEAPBLK_TO_OFFSET(heapBlk);
	uint8		mask = flags << mapOffset;
	char	   *map;
	bool		cleared = false;

	Assert(flags & VISIBILITYMAP_VALID_BITS);
	Assert(flags != VISIBILITYMAP_ALL_VISIBLE);
	Assert(BufferIsValid(vmbuf) && BufferGetBlockNumber(vmbuf) == mapBlock);

	map = PageGetContents(BufferGetPage(vmbuf));

	if (map[mapByte] & mask)
	{
		map[mapByte] &= ~mask;

		MarkBufferDirty(vmbuf);
		cleared = true;
	}

	return cleared;
}

/*
 *	visibilitymap_pin - pin a map page for setting a bit
 *
 * Setting a bit in the visibility map is a two-phase operation. First, call
 * visibilitymap_pin, to pin the visibility map page containing the bit for
 * the heap page. Because that can require I/O to read the map page, you
 * shouldn't hold a lock on the heap page while doing that. Then, call
 * visibilitymap_set to actually set the bit.
 *
 * On entry, *vmbuf should be InvalidBuffer or a valid buffer returned by
 * an earlier call to visibilitymap_pin or visibilitymap_get_status on the same
 * relation. On return, *vmbuf is a valid buffer with the map page containing
 * the bit for heapBlk.
 *
 * If the page doesn't exist in the map file yet, it is extended.
 */
void
visibilitymap_pin(Relation rel, BlockNumber heapBlk, Buffer *vmbuf)
{
	BlockNumber mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);

	/* Reuse the old pinned buffer if possible */
	if (BufferIsValid(*vmbuf))
	{
		if (visibilitymap_pin_ok(heapBlk, *vmbuf))
			return;

		ReleaseBuffer(*vmbuf);
	}
	*vmbuf = vm_readbuf(rel, mapBlock, true);
}

/*
 * Repin the exact visibility-map block in a recently observed descriptor.
 *
 * Unlike visibilitymap_pin(), this never performs a mapping lookup, storage
 * read, or relation extension.  A heap mutation can therefore release its VM
 * pin before a potentially blocking cluster PCM acquire, take heap content
 * authority, and use this helper without introducing I/O below that lock.  A
 * false result means the descriptor was invalidated or retagged; the caller
 * must release heap content authority and retry through visibilitymap_pin().
 */
bool
visibilitymap_pin_recent(Relation rel, BlockNumber heapBlk, Buffer recent_buffer,
						 Buffer *vmbuf)
{
	BlockNumber mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);

	if (rel == NULL || vmbuf == NULL || BufferIsValid(*vmbuf)
		|| !BufferIsValid(recent_buffer))
		return false;
	if (!ReadRecentBuffer(RelationGetSmgr(rel)->smgr_rlocator.locator,
						  VISIBILITYMAP_FORKNUM, mapBlock, recent_buffer))
		return false;
#ifdef USE_PGRAC_CLUSTER
	/* A read-only zero page has no versioned write base yet. Do not do I/O
	 * below the caller's heap content lock; return to the original pin path. */
	if (cluster_shared_config && PageIsNew(BufferGetPage(recent_buffer)))
	{
		ReleaseBuffer(recent_buffer);
		return false;
	}
#endif
	*vmbuf = recent_buffer;
	return true;
}

/*
 *	visibilitymap_pin_ok - do we already have the correct page pinned?
 *
 * On entry, vmbuf should be InvalidBuffer or a valid buffer returned by
 * an earlier call to visibilitymap_pin or visibilitymap_get_status on the same
 * relation.  The return value indicates whether the buffer covers the
 * given heapBlk.
 */
bool
visibilitymap_pin_ok(BlockNumber heapBlk, Buffer vmbuf)
{
	BlockNumber mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);

	if (!BufferIsValid(vmbuf) || BufferGetBlockNumber(vmbuf) != mapBlock)
		return false;
#ifdef USE_PGRAC_CLUSTER
	if (cluster_shared_config && PageIsNew(BufferGetPage(vmbuf)))
		return false;
#endif
	return true;
}

/*
 *	visibilitymap_set - set bit(s) on a previously pinned page
 *
 * recptr is the LSN of the XLOG record we're replaying, if we're in recovery,
 * or InvalidXLogRecPtr in normal running.  The VM page LSN is advanced to the
 * one provided; in normal running, we generate a new XLOG record and set the
 * page LSN to that value (though the heap page's LSN may *not* be updated;
 * see below).  cutoff_xid is the largest xmin on the page being marked
 * all-visible; it is needed for Hot Standby, and can be InvalidTransactionId
 * if the page contains no tuples.  It can also be set to InvalidTransactionId
 * when a page that is already all-visible is being marked all-frozen.
 *
 * Caller is expected to set the heap page's PD_ALL_VISIBLE bit before calling
 * this function. Except in recovery, caller should also pass the heap
 * buffer. When checksums are enabled and we're not in recovery, we must add
 * the heap buffer to the WAL chain to protect it from being torn.
 *
 * You must pass a buffer containing the correct map page to this function.
 * Call visibilitymap_pin first to pin the right one. This function doesn't do
 * any I/O.
 */
void
visibilitymap_set(Relation rel, BlockNumber heapBlk, Buffer heapBuf,
				  XLogRecPtr recptr, Buffer vmBuf, TransactionId cutoff_xid,
				  uint8 flags)
{
	if (!BufferIsValid(vmBuf) || !visibilitymap_pin_ok(heapBlk, vmBuf))
		elog(ERROR, "wrong VM buffer passed to visibilitymap_set");
	LockBuffer(vmBuf, BUFFER_LOCK_EXCLUSIVE);
	visibilitymap_set_locked(rel, heapBlk, heapBuf, recptr, vmBuf, cutoff_xid, flags);
	LockBuffer(vmBuf, BUFFER_LOCK_UNLOCK);
}

bool
visibilitymap_set_retry_aware(Relation rel, BlockNumber heapBlk, Buffer heapBuf, XLogRecPtr recptr,
							  Buffer *vmbuf, TransactionId cutoff_xid, uint8 flags,
							  struct ResourceXAuxiliaryAcquireContext *context)
{
	Assert(vmbuf != NULL && context != NULL);
	if (!BufferIsValid(*vmbuf) || !visibilitymap_pin_ok(heapBlk, *vmbuf))
		elog(ERROR, "wrong VM buffer passed to visibilitymap_set_retry_aware");
	if (!ClusterLockBufferExclusiveAuxiliaryAware(vmbuf, context))
		return false;
	visibilitymap_set_locked(rel, heapBlk, heapBuf, recptr, *vmbuf, cutoff_xid, flags);
	LockBuffer(*vmbuf, BUFFER_LOCK_UNLOCK);
	return true;
}

static void
visibilitymap_set_locked(Relation rel, BlockNumber heapBlk, Buffer heapBuf, XLogRecPtr recptr,
						 Buffer vmBuf, TransactionId cutoff_xid, uint8 flags)
{
	BlockNumber mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);
	uint32		mapByte = HEAPBLK_TO_MAPBYTE(heapBlk);
	uint8		mapOffset = HEAPBLK_TO_OFFSET(heapBlk);
	Page		page;
	uint8	   *map;

#ifdef TRACE_VISIBILITYMAP
	elog(DEBUG1, "vm_set %s %d", RelationGetRelationName(rel), heapBlk);
#endif

	Assert(InRecovery || XLogRecPtrIsInvalid(recptr));
	Assert(InRecovery || PageIsAllVisible((Page) BufferGetPage(heapBuf)));
	Assert((flags & VISIBILITYMAP_VALID_BITS) == flags);

	/* Must never set all_frozen bit without also setting all_visible bit */
	Assert(flags != VISIBILITYMAP_ALL_FROZEN);

	/* Check that we have the right heap page pinned, if present */
	if (BufferIsValid(heapBuf) && BufferGetBlockNumber(heapBuf) != heapBlk)
		elog(ERROR, "wrong heap buffer passed to visibilitymap_set");

	/* Check that we have the right VM page pinned */
	if (!BufferIsValid(vmBuf) || BufferGetBlockNumber(vmBuf) != mapBlock)
		elog(ERROR, "wrong VM buffer passed to visibilitymap_set");

	page = BufferGetPage(vmBuf);
	map = (uint8 *)PageGetContents(page);

	if (flags != (map[mapByte] >> mapOffset & VISIBILITYMAP_VALID_BITS))
	{
		START_CRIT_SECTION();

		map[mapByte] |= (flags << mapOffset);
		MarkBufferDirty(vmBuf);

		if (RelationNeedsWAL(rel))
		{
			if (XLogRecPtrIsInvalid(recptr))
			{
				Assert(!InRecovery);
				recptr = log_heap_visible(rel, heapBuf, vmBuf, cutoff_xid, flags);

				/*
				 * If data checksums are enabled (or wal_log_hints=on), we
				 * need to protect the heap page from being torn.
				 *
				 * If not, then we must *not* update the heap page's LSN. In
				 * this case, the FPI for the heap page was omitted from the
				 * WAL record inserted above, so it would be incorrect to
				 * update the heap page's LSN.
				 */
				if (XLogHintBitIsNeeded())
				{
					Page		heapPage = BufferGetPage(heapBuf);

					PageSetLSN(heapPage, recptr);
				}
			}
			PageSetLSN(page, recptr);
		}

		END_CRIT_SECTION();
	}
}

/*
 *	visibilitymap_get_status - get status of bits
 *
 * Are all tuples on heapBlk visible to all or are marked frozen, according
 * to the visibility map?
 *
 * On entry, *vmbuf should be InvalidBuffer or a valid buffer returned by an
 * earlier call to visibilitymap_pin or visibilitymap_get_status on the same
 * relation. On return, *vmbuf is a valid buffer with the map page containing
 * the bit for heapBlk, or InvalidBuffer. The caller is responsible for
 * releasing *vmbuf after it's done testing and setting bits.
 *
 * NOTE: This function is typically called without a lock on the heap page,
 * so somebody else could change the bit just after we look at it.  In fact,
 * since we don't lock the visibility map page either, it's even possible that
 * someone else could have changed the bit just before we look at it, but yet
 * we might see the old value.  It is the caller's responsibility to deal with
 * all concurrency issues!
 */
uint8
visibilitymap_get_status(Relation rel, BlockNumber heapBlk, Buffer *vmbuf)
{
	BlockNumber mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);
	uint32		mapByte = HEAPBLK_TO_MAPBYTE(heapBlk);
	uint8		mapOffset = HEAPBLK_TO_OFFSET(heapBlk);
	char	   *map;
	uint8		result;

#ifdef TRACE_VISIBILITYMAP
	elog(DEBUG1, "vm_get_status %s %d", RelationGetRelationName(rel), heapBlk);
#endif

	/* Reuse the old pinned buffer if possible */
	if (BufferIsValid(*vmbuf))
	{
		if (BufferGetBlockNumber(*vmbuf) != mapBlock)
		{
			ReleaseBuffer(*vmbuf);
			*vmbuf = InvalidBuffer;
		}
	}

	if (!BufferIsValid(*vmbuf))
	{
		*vmbuf = vm_readbuf(rel, mapBlock, false);
		if (!BufferIsValid(*vmbuf))
			return false;
	}

	map = PageGetContents(BufferGetPage(*vmbuf));

	/*
	 * A single byte read is atomic.  There could be memory-ordering effects
	 * here, but for performance reasons we make it the caller's job to worry
	 * about that.
	 */
	result = ((map[mapByte] >> mapOffset) & VISIBILITYMAP_VALID_BITS);
	return result;
}

/*
 *	visibilitymap_count  - count number of bits set in visibility map
 *
 * Note: we ignore the possibility of race conditions when the table is being
 * extended concurrently with the call.  New pages added to the table aren't
 * going to be marked all-visible or all-frozen, so they won't affect the result.
 */
void
visibilitymap_count(Relation rel, BlockNumber *all_visible, BlockNumber *all_frozen)
{
	BlockNumber mapBlock;
	BlockNumber nvisible = 0;
	BlockNumber nfrozen = 0;

	/* all_visible must be specified */
	Assert(all_visible);

	for (mapBlock = 0;; mapBlock++)
	{
		Buffer		mapBuffer;
		uint64	   *map;
		int			i;

		/*
		 * Read till we fall off the end of the map.  We assume that any extra
		 * bytes in the last page are zeroed, so we don't bother excluding
		 * them from the count.
		 */
		mapBuffer = vm_readbuf(rel, mapBlock, false);
		if (!BufferIsValid(mapBuffer))
			break;

		/*
		 * We choose not to lock the page, since the result is going to be
		 * immediately stale anyway if anyone is concurrently setting or
		 * clearing bits, and we only really need an approximate value.
		 */
		map = (uint64 *) PageGetContents(BufferGetPage(mapBuffer));

		StaticAssertStmt(MAPSIZE % sizeof(uint64) == 0,
						 "unsupported MAPSIZE");
		if (all_frozen == NULL)
		{
			for (i = 0; i < MAPSIZE / sizeof(uint64); i++)
				nvisible += pg_popcount64(map[i] & VISIBLE_MASK64);
		}
		else
		{
			for (i = 0; i < MAPSIZE / sizeof(uint64); i++)
			{
				nvisible += pg_popcount64(map[i] & VISIBLE_MASK64);
				nfrozen += pg_popcount64(map[i] & FROZEN_MASK64);
			}
		}

		ReleaseBuffer(mapBuffer);
	}

	*all_visible = nvisible;
	if (all_frozen)
		*all_frozen = nfrozen;
}

/*
 *	visibilitymap_prepare_truncate -
 *			prepare for truncation of the visibility map
 *
 * nheapblocks is the new size of the heap.
 *
 * Return the number of blocks of new visibility map.
 * If it's InvalidBlockNumber, there is nothing to truncate;
 * otherwise the caller is responsible for calling smgrtruncate()
 * to truncate the visibility map pages.
 */
BlockNumber
visibilitymap_prepare_truncate(Relation rel, BlockNumber nheapblocks)
{
	BlockNumber newnblocks;

	/* last remaining block, byte, and bit */
	BlockNumber truncBlock = HEAPBLK_TO_MAPBLOCK(nheapblocks);
	uint32		truncByte = HEAPBLK_TO_MAPBYTE(nheapblocks);
	uint8		truncOffset = HEAPBLK_TO_OFFSET(nheapblocks);

#ifdef TRACE_VISIBILITYMAP
	elog(DEBUG1, "vm_truncate %s %d", RelationGetRelationName(rel), nheapblocks);
#endif

	/*
	 * If no visibility map has been created yet for this relation, there's
	 * nothing to truncate.
	 */
	if (!smgrexists(RelationGetSmgr(rel), VISIBILITYMAP_FORKNUM))
		return InvalidBlockNumber;

	/*
	 * Unless the new size is exactly at a visibility map page boundary, the
	 * tail bits in the last remaining map page, representing truncated heap
	 * blocks, need to be cleared. This is not only tidy, but also necessary
	 * because we don't get a chance to clear the bits if the heap is extended
	 * again.
	 */
	if (truncByte != 0 || truncOffset != 0)
	{
		Buffer		mapBuffer;
		Page		page;
		char	   *map;

		newnblocks = truncBlock + 1;

		mapBuffer = vm_readbuf(rel, truncBlock, false);
		if (!BufferIsValid(mapBuffer))
		{
			/* nothing to do, the file was already smaller */
			return InvalidBlockNumber;
		}

		page = BufferGetPage(mapBuffer);
		map = PageGetContents(page);

		LockBuffer(mapBuffer, BUFFER_LOCK_EXCLUSIVE);

#ifdef USE_PGRAC_CLUSTER
		/* A real zero VM tail already represents no visible blocks. Recheck
		 * under authority, but do not create an unversioned dirty header. */
		if (cluster_shared_config && PageIsNew(page))
		{
			UnlockReleaseBuffer(mapBuffer);
			goto check_vm_size;
		}
#endif

		/* NO EREPORT(ERROR) from here till changes are logged */
		START_CRIT_SECTION();

		/* Clear out the unwanted bytes. */
		MemSet(&map[truncByte + 1], 0, MAPSIZE - (truncByte + 1));

		/*----
		 * Mask out the unwanted bits of the last remaining byte.
		 *
		 * ((1 << 0) - 1) = 00000000
		 * ((1 << 1) - 1) = 00000001
		 * ...
		 * ((1 << 6) - 1) = 00111111
		 * ((1 << 7) - 1) = 01111111
		 *----
		 */
		map[truncByte] &= (1 << truncOffset) - 1;

		/*
		 * Truncation of a relation is WAL-logged at a higher-level, and we
		 * will be called at WAL replay. But if checksums are enabled, we need
		 * to still write a WAL record to protect against a torn page, if the
		 * page is flushed to disk before the truncation WAL record. We cannot
		 * use MarkBufferDirtyHint here, because that will not dirty the page
		 * during recovery.
		 */
		MarkBufferDirty(mapBuffer);
		if (!InRecovery && RelationNeedsWAL(rel) && XLogHintBitIsNeeded())
			log_newpage_buffer(mapBuffer, false);

		END_CRIT_SECTION();

		UnlockReleaseBuffer(mapBuffer);
	}
	else
		newnblocks = truncBlock;

#ifdef USE_PGRAC_CLUSTER
check_vm_size:
#endif
	if (smgrnblocks(RelationGetSmgr(rel), VISIBILITYMAP_FORKNUM) <= newnblocks)
	{
		/* nothing to do, the file was already smaller than requested size */
		return InvalidBlockNumber;
	}

	return newnblocks;
}

/*
 * Read a visibility map page.
 *
 * If the page doesn't exist, InvalidBuffer is returned, or if 'extend' is
 * true, the visibility map file is extended.
 */
static Buffer
vm_readbuf(Relation rel, BlockNumber blkno, bool extend)
{
	Buffer		buf;
	SMgrRelation reln;
	bool		versioned = false;
#ifdef USE_PGRAC_CLUSTER
	ClusterSpaceIdentity identity;

	versioned = cluster_shared_config && RelationIsPermanent(rel)
		&& cluster_smgr_which_for(rel->rd_locator, InvalidBackendId) == 1;
	/* Cache miss can read SPACE; resolve before any VM pin/content lock. */
	if (versioned && extend
		&& (!RelationNeedsWAL(rel) || !cluster_space_relation_get_identity(rel, &identity)))
		elog(ERROR, "shared VM initialization requires a live SPACE identity");
#endif

	/*
	 * Caution: re-using this smgr pointer could fail if the relcache entry
	 * gets closed.  It's safe as long as we only do smgr-level operations
	 * between here and the last use of the pointer.
	 */
	/*
	 * The native profile uses ZERO_ON_ERROR and initializes when necessary.
	 * The versioned shared profile must preserve the actual predecessor:
	 * corrupt storage refuses, while genuine zero pages stay read-only until
	 * their write-pin owner records a versioned initialization.
	 *
	 * We use the same path below to initialize pages when extending the
	 * relation, as a concurrent extension can end up with vm_extend()
	 * returning an already-initialized page.
	 */
	for (;;)
	{
		/* The init wait can release the pin and invalidate SMgr; reacquire
		 * the original relation's mapping on every retry. */
		reln = RelationGetSmgr(rel);
		if (reln->smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] == InvalidBlockNumber)
		{
			if (smgrexists(reln, VISIBILITYMAP_FORKNUM))
				smgrnblocks(reln, VISIBILITYMAP_FORKNUM);
			else
				reln->smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 0;
		}
		if (blkno >= reln->smgr_cached_nblocks[VISIBILITYMAP_FORKNUM])
		{
			if (extend)
				buf = vm_extend(rel, blkno + 1);
			else
				return InvalidBuffer;
		}
		else
			buf = ReadBufferExtended(rel, VISIBILITYMAP_FORKNUM, blkno,
									 versioned ? RBM_NORMAL : RBM_ZERO_ON_ERROR, NULL);

		/* Reads of a genuine zero page already return an all-clear bitmap;
		 * they must not create an unlogged/unversioned initialization. */
		if (versioned && !extend)
			return buf;

		/*
		 * Initializing the page when needed is trickier than it looks, because
		 * multiple backends can do this concurrently.  PGRAC may deliberately
		 * release the only VM pin while waiting for Resource-X.  If the old
		 * descriptor is reused, the init wrapper returns InvalidBuffer and this
		 * relation-aware loop obtains a fresh exact mapping before inspecting
		 * or modifying any page bytes.
		 */
		if (PageIsNew(BufferGetPage(buf)))
		{
			buf = LockBufferForVisibilityMapPageInit(buf);
			if (!BufferIsValid(buf))
				continue;
			if (PageIsNew(BufferGetPage(buf)))
			{
#ifdef USE_PGRAC_CLUSTER
				if (versioned)
				{
					if (!cluster_space_init_vm_buffer_wal(&identity, buf))
						elog(ERROR, "shared VM initialization requires a zero version base");
				}
				else
#endif
					PageInit(BufferGetPage(buf), BLCKSZ, 0);
			}
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		}
		return buf;
	}
}

/*
 * Ensure that the visibility map fork is at least vm_nblocks long, extending
 * it if necessary with zeroed pages.
 */
static Buffer
vm_extend(Relation rel, BlockNumber vm_nblocks)
{
	Buffer		buf;
	ReadBufferMode mode = RBM_ZERO_ON_ERROR;

#ifdef USE_PGRAC_CLUSTER
	/* PGRAC: a concurrent extension can return an existing target page.
	 * Corruption there must not be converted into a fresh zero predecessor. */
	if (cluster_shared_config && RelationIsPermanent(rel) &&
		cluster_smgr_which_for(rel->rd_locator, InvalidBackendId) == 1)
		mode = RBM_NORMAL;
#endif

	buf = ExtendBufferedRelTo(BMR_REL(rel), VISIBILITYMAP_FORKNUM, NULL,
							  EB_CREATE_FORK_IF_NEEDED |
							  EB_CLEAR_SIZE_CACHE,
							  vm_nblocks,
							  mode);

	/*
	 * Send a shared-inval message to force other backends to close any smgr
	 * references they may have for this rel, which we are about to change.
	 * This is a useful optimization because it means that backends don't have
	 * to keep checking for creation or extension of the file, which happens
	 * infrequently.
	 */
	CacheInvalidateSmgr(RelationGetSmgr(rel)->smgr_rlocator);

	return buf;
}
