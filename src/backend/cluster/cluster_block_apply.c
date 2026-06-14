/*-------------------------------------------------------------------------
 *
 * cluster_block_apply.c
 *	  pgrac backend single-block redo-apply framework (spec-4.10 D3a).
 *
 *	  Applies one decoded WAL record's effect on a single block to a DETACHED
 *	  char[BLCKSZ] page, stripping the buffer-pool / VM-FSM / multi-block /
 *	  recovery-context machinery of PG's normal redo path.  Used by online
 *	  block recovery to reconstruct a corrupt/lost-write block from WAL.
 *
 *	  Dispatch is FPI-first (rmgr-agnostic RestoreBlockImage) then, for delta
 *	  records, by rmgr into a per-rmgr handler.  The D3b apply matrix covers
 *	  RM_GENERIC_ID (cluster ITL-touch, applyPageRedo mirror, here) and
 *	  RM_HEAP_ID (insert/delete/update, cluster_block_apply_heap.c); record
 *	  types off the matrix fail closed (UNSUPPORTED).  Each matrix entry has
 *	  passed the byte-for-byte differential against PG real redo (t/256)
 *	  before joining -- never a silent wrong-block install (8.A / R11).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_block_apply.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.10-online-block-recovery.md (FROZEN v0.4)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/rmgr.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "storage/bufpage.h"
#include "storage/off.h"
#include "cluster/cluster_block_apply.h"

/*
 * cluster_block_apply_fpi -- restore a full-page image onto a detached page.
 *
 *	FPI reconstruction is rmgr-agnostic: RestoreBlockImage() decompresses the
 *	stored image (incl. zero-filling the hole) into the caller's BLCKSZ page.
 *	It does NOT set the page LSN, so we stamp it here, mirroring PG's redo
 *	(XLogReadBufferForRedoExtended, xlogutils.c): the record's end LSN is the
 *	page version, but an uninitialized (all-zero) page is left untouched --
 *	setting its LSN would corrupt it.
 *
 *	In a backend (the online-recovery context) PageSetLSN runs OUTSIDE the
 *	merged-recovery window, so it stamps pd_lsn only; pd_block_scn is stamped
 *	by the orchestrator at install time (D4).
 */
static ClusterBlkApplyResult
cluster_block_apply_fpi(XLogReaderState *record, uint8 block_id, char *page)
{
	if (!RestoreBlockImage(record, block_id, page))
		return CLUSTER_BLKAPPLY_FAILED;

	if (!PageIsNew(page))
		PageSetLSN(page, record->EndRecPtr);

	return CLUSTER_BLKAPPLY_OK;
}

/*
 * cluster_block_apply_generic -- apply one Generic (RM_GENERIC_ID) record's
 *		effect on a single block to a detached page.
 *
 *	Generic WAL is rmgr-agnostic and detached-page-friendly: the block data is
 *	a sequence of fragments {OffsetNumber offset, OffsetNumber length, bytes}
 *	memcpy'd onto the page, after which the hole (pd_lower..pd_upper) is zeroed
 *	-- byte-for-byte mirroring PG's generic_redo()/applyPageRedo()
 *	(generic_xlog.c).  pgrac emits these for the cluster ITL-touch finish
 *	(cluster_itl_touch.c), so every reconstructed heap block's own-thread chain
 *	includes them.
 *
 *	Defensive bounds checks (8.A): a malformed/corrupt fragment header, an
 *	out-of-page fragment, or insane pd_lower/pd_upper fails closed (FAILED)
 *	rather than writing out of bounds.  For VALID WAL every check passes and
 *	the result is identical to generic_redo, which the byte-for-byte
 *	differential (t/256) verifies.
 */
static ClusterBlkApplyResult
cluster_block_apply_generic(XLogReaderState *record, uint8 block_id, char *page)
{
	char *delta;
	Size delta_size;
	const char *ptr;
	const char *end;
	PageHeader page_header;

	delta = XLogRecGetBlockData(record, block_id, &delta_size);
	if (delta == NULL)
		/*
		 * A registered-but-empty block delta (has_data == false, len == 0)
		 * would, in generic_redo, still zero the hole and set the LSN.  pgrac's
		 * ITL-touch always emits a non-empty delta, so this is never exercised;
		 * failing closed here is a safe-direction divergence (the block is
		 * declared unrecoverable rather than rebuilt).  If a non-ITL generic
		 * producer with empty deltas ever appears, add a differential leg and
		 * fall through to the hole-zero + PageSetLSN instead.
		 */
		return CLUSTER_BLKAPPLY_FAILED;

	ptr = delta;
	end = delta + delta_size;
	while (ptr < end) {
		OffsetNumber offset;
		OffsetNumber length;

		/* fragment header = two OffsetNumbers */
		if ((Size)(end - ptr) < sizeof(offset) + sizeof(length))
			return CLUSTER_BLKAPPLY_FAILED;
		memcpy(&offset, ptr, sizeof(offset));
		ptr += sizeof(offset);
		memcpy(&length, ptr, sizeof(length));
		ptr += sizeof(length);

		/* fragment body present and in-page */
		if ((Size)(end - ptr) < (Size)length)
			return CLUSTER_BLKAPPLY_FAILED;
		if ((Size)offset + (Size)length > BLCKSZ)
			return CLUSTER_BLKAPPLY_FAILED;

		memcpy(page + offset, ptr, length);
		ptr += length;
	}

	/*
	 * Zero the hole, mirroring generic_redo: the delta carries no hole bytes,
	 * so they must be zero to match GenericXLogFinish's page state.
	 */
	page_header = (PageHeader)page;
	if (page_header->pd_lower < SizeOfPageHeaderData
		|| page_header->pd_lower > page_header->pd_upper || page_header->pd_upper > BLCKSZ)
		return CLUSTER_BLKAPPLY_FAILED;
	memset(page + page_header->pd_lower, 0, page_header->pd_upper - page_header->pd_lower);

	PageSetLSN(page, record->EndRecPtr);
	return CLUSTER_BLKAPPLY_OK;
}

/*
 * cluster_block_apply_delta -- dispatch a no-image (delta) record to its
 *		per-rmgr single-block applicator.
 *
 *	Only record types whose single-block apply has passed the byte-for-byte
 *	differential against PG real redo (t/256) are on the matrix; everything
 *	else fails closed (UNSUPPORTED) -- never a silent wrong-block install
 *	(8.A / R11).  Notably RM_HEAP2_ID (multi-insert / prune / freeze / visible)
 *	DOES mutate heap-block bytes but is deliberately off the matrix: it falls
 *	through to UNSUPPORTED here, so such a block fails closed (unrecoverable)
 *	rather than risking a wrong-block install.
 */
static ClusterBlkApplyResult
cluster_block_apply_delta(XLogReaderState *record, uint8 block_id, char *page)
{
	switch (XLogRecGetRmid(record)) {
	case RM_GENERIC_ID:
		return cluster_block_apply_generic(record, block_id, page);

	case RM_HEAP_ID:
		return cluster_block_apply_heap(record, block_id, page);

	default:
		return CLUSTER_BLKAPPLY_UNSUPPORTED;
	}
}

/*
 * cluster_block_apply_one -- apply one decoded WAL record to a single block
 *		on a detached page.
 *
 *	Returns CLUSTER_BLKAPPLY_OK when the block was reconstructed (FPI restored
 *	or, from D3b, a per-rmgr delta applied).  Any other result is a
 *	fail-closed signal the caller must treat as block-unrecoverable -- the
 *	page is never installed on uncertainty (8.A).
 *
 * Inputs:
 *	record:    a decoded WAL record (read-only) known to be in the block's
 *	           own-thread reconstruction chain (caller's whole-chain
 *	           own-thread gate, D2/D3b).
 *	block_id:  the record's block reference id for the target block.
 *	page:      BLCKSZ detached buffer; written only on OK.
 *
 * Returns:
 *	CLUSTER_BLKAPPLY_OK / NOOP / UNSUPPORTED / FAILED (see header).
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterBlkApplyResult
cluster_block_apply_one(XLogReaderState *record, uint8 block_id, char *page)
{
	bool has_block_ref = XLogRecHasBlockRef(record, block_id);
	bool has_image = has_block_ref && XLogRecHasBlockImage(record, block_id);
	bool apply_image = has_image && XLogRecBlockImageApply(record, block_id);
	ClusterBlkApplyAction action;

	action = cluster_block_apply_decide(has_block_ref, has_image, apply_image);

	switch (action) {
	case CLUSTER_BLKAPPLY_ACT_NOOP:
		return CLUSTER_BLKAPPLY_NOOP;

	case CLUSTER_BLKAPPLY_ACT_FPI:
		return cluster_block_apply_fpi(record, block_id, page);

	case CLUSTER_BLKAPPLY_ACT_DELTA:

		/*
		 * Dispatch the delta to its per-rmgr applicator.  Only record types
		 * that have passed the byte-for-byte differential (t/256) are on the
		 * matrix; everything else fails closed (8.A / R11 -- never a silent
		 * wrong-block install).
		 */
		return cluster_block_apply_delta(record, block_id, page);
	}

	/* cluster_block_apply_decide returns one of the cases above. */
	return CLUSTER_BLKAPPLY_FAILED;
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
