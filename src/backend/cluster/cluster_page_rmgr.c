/*-------------------------------------------------------------------------
 *
 * cluster_page_rmgr.c
 *	  RF-PAGE PGDEL-02 — applicable-redo rmgr census + PageVersion hints
 *	  decoder (implementation).
 *
 *	  Spec: specs/spec-rf-page-crash-safe-page-replay-journal.md
 *	  §2.1 PGDEL-02 / §3.1 / §3.4 / §4.1 / §11.1 G1, G3, G9.
 *
 *	  Census row evidence discipline (G1/G9 — do not erase):
 *	    - known_delta=true is granted ONLY to (rmid, opcode) pairs whose
 *	      single-block delta passed the byte-for-byte differential against
 *	      PG real redo (the existing apply matrix, t/256): RM_HEAP_ID
 *	      INSERT/DELETE/UPDATE/HOT_UPDATE and RM_GENERIC_ID.  Everything
 *	      else — including XLOG_HEAP_LOCK/CONFIRM/INPLACE, the whole
 *	      RM_HEAP2 family and every other rmgr — has known_delta=false
 *	      (the existing matrix deliberately fails them closed, 8.A/R11),
 *	      so they never register into the PGDEL-01 classifier known-set
 *	      and classify UNKNOWN (BLOCKED).
 *	    - decoder_registered=false for EVERY row: the §3.4 deterministic-
 *	      mutation declaration + the §3.1 VersionToken producer contract
 *	      are not proven yet (PGDEL-03/06).  cluster_page_redo_decode
 *	      therefore reports facts, never a version judgement.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_page_rmgr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/heapam_xlog.h" /* XLOG_HEAP_* opcodes */
#include "access/rmgr.h"		/* RM_* IDs (PG_RMGR over rmgrlist.h) */
#include "access/xlogrecord.h"	/* BKPBLOCK_WILL_INIT, XLR_INFO_MASK */
#include "cluster/cluster_page_rmgr.h"

/* ---------------------------------------------------------------------
 * Census table
 * --------------------------------------------------------------------- */

/*
 * Opcode-granular rows: the byte-for-byte differenced apply matrix
 * (cluster_block_apply_heap.c / cluster_block_apply.c, t/256 evidence).
 * XLOG_HEAP_OPMASK covers 0x10..0x70; INIT_PAGE (0x80) is a separate
 * attribute row.  XLOG_HEAP_LOCK/CONFIRM/INPLACE are deliberately NOT
 * known_delta (the matrix fails them closed: a silent wrong-block install
 * is forbidden, 8.A/R11).
 */
static const ClusterPageRmgrCensusEntry cluster_page_census_opcode[] = {
	{ RM_HEAP_ID, XLOG_HEAP_INSERT, 0, CLUSTER_PAGE_CLASS_NORMAL, true, true, false, false },
	{ RM_HEAP_ID, XLOG_HEAP_DELETE, 0, CLUSTER_PAGE_CLASS_NORMAL, true, true, false, false },
	{ RM_HEAP_ID, XLOG_HEAP_UPDATE, 0, CLUSTER_PAGE_CLASS_NORMAL, true, true, false, false },
	{ RM_HEAP_ID, XLOG_HEAP_HOT_UPDATE, 0, CLUSTER_PAGE_CLASS_NORMAL, true, true, false, false },
	{ /* LOCK/CONFIRM/INPLACE: page-affecting NORMAL row, but no
		 * differential evidence -> never registers -> UNKNOWN at runtime. */
	  RM_HEAP_ID, XLOG_HEAP_LOCK, XLOG_HEAP_OPMASK, CLUSTER_PAGE_CLASS_NORMAL, true, false, false,
	  false },
	{ /* XLOG_HEAP_INIT_PAGE: full page re-initialization ATTRIBUTE.
		 * This row fact is not a §4.6 full-init RULE (expected class state
		 * + result-version proof are RED), so the classifier still
		 * returns UNKNOWN for WILL_INIT records (PU-17). */
	  RM_HEAP_ID, XLOG_HEAP_INIT_PAGE, 0, CLUSTER_PAGE_CLASS_NEW, true, false, true, false },
	{ RM_GENERIC_ID, 0, 0, CLUSTER_PAGE_CLASS_NORMAL, true, true, false, false },
};

/*
 * Rmgr-granular rows: the closed-table answer for every other rmgr.
 * unknown rmgr IDs are not rows at all (lookup fails -> UNKNOWN).
 * SLRU/relmap/undo pages are HEADER-class pages with a typed owner
 * (PG core / PGRAC core) — generic page replay must never touch them
 * (§4.5), and PGRAC's single-block matrix has no differential evidence
 * for them anyway.
 */
static const ClusterPageRmgrCensusEntry cluster_page_census_rmgr[] = {
	/* Non-page-affecting: control/transaction/file-level records. */
	{ RM_XLOG_ID, 0, 0, CLUSTER_PAGE_CLASS_UNKNOWN, false, false, false, false },
	{ RM_XACT_ID, 0, 0, CLUSTER_PAGE_CLASS_UNKNOWN, false, false, false, false },
	{ RM_SMGR_ID, 0, 0, CLUSTER_PAGE_CLASS_UNKNOWN, false, false, false, false },
	{ RM_DBASE_ID, 0, 0, CLUSTER_PAGE_CLASS_UNKNOWN, false, false, false, false },
	{ RM_TBLSPC_ID, 0, 0, CLUSTER_PAGE_CLASS_UNKNOWN, false, false, false, false },
	{ RM_STANDBY_ID, 0, 0, CLUSTER_PAGE_CLASS_UNKNOWN, false, false, false, false },
	{ RM_REPLORIGIN_ID, 0, 0, CLUSTER_PAGE_CLASS_UNKNOWN, false, false, false, false },
	{ RM_LOGICALMSG_ID, 0, 0, CLUSTER_PAGE_CLASS_UNKNOWN, false, false, false, false },
	{ RM_CLUSTER_ADG_ID, 0, 0, CLUSTER_PAGE_CLASS_UNKNOWN, false, false, false, false },
	{ RM_CLUSTER_XID_STRIPE_ID, 0, 0, CLUSTER_PAGE_CLASS_UNKNOWN, false, false, false, false },
	/* SLRU / relmapper / undo segment pages: HEADER-class, typed owner. */
	{ RM_CLOG_ID, 0, 0, CLUSTER_PAGE_CLASS_HEADER, true, false, false, false },
	{ RM_MULTIXACT_ID, 0, 0, CLUSTER_PAGE_CLASS_HEADER, true, false, false, false },
	{ RM_COMMIT_TS_ID, 0, 0, CLUSTER_PAGE_CLASS_HEADER, true, false, false, false },
	{ RM_RELMAP_ID, 0, 0, CLUSTER_PAGE_CLASS_HEADER, true, false, false, false },
	{ RM_CLUSTER_UNDO_ID, 0, 0, CLUSTER_PAGE_CLASS_HEADER, true, false, false, false },
	{ RM_CLUSTER_RAW_LAYOUT_ID, 0, 0, CLUSTER_PAGE_CLASS_HEADER, true, false, false, false },
	/* Index/sequence data pages: NORMAL row, no differential evidence yet. */
	{ RM_HEAP2_ID, 0, 0, CLUSTER_PAGE_CLASS_NORMAL, true, false, false, false },
	{ RM_BTREE_ID, 0, 0, CLUSTER_PAGE_CLASS_NORMAL, true, false, false, false },
	{ RM_HASH_ID, 0, 0, CLUSTER_PAGE_CLASS_NORMAL, true, false, false, false },
	{ RM_GIN_ID, 0, 0, CLUSTER_PAGE_CLASS_NORMAL, true, false, false, false },
	{ RM_GIST_ID, 0, 0, CLUSTER_PAGE_CLASS_NORMAL, true, false, false, false },
	{ RM_SPGIST_ID, 0, 0, CLUSTER_PAGE_CLASS_NORMAL, true, false, false, false },
	{ RM_BRIN_ID, 0, 0, CLUSTER_PAGE_CLASS_NORMAL, true, false, false, false },
	{ RM_SEQ_ID, 0, 0, CLUSTER_PAGE_CLASS_NORMAL, true, false, false, false },
};

bool
cluster_page_rmgr_census_lookup(uint8 rmid, uint16 opcode, ClusterPageRmgrCensusEntry *out)
{
	int i;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));

	/* Opcode-granular rows first (exact or masked match). */
	for (i = 0; i < (int)lengthof(cluster_page_census_opcode); i++) {
		const ClusterPageRmgrCensusEntry *row = &cluster_page_census_opcode[i];

		if (row->rmid != rmid)
			continue;
		if (row->opcode_mask != 0) {
			if ((opcode & row->opcode_mask) != (row->opcode & row->opcode_mask))
				continue;
		} else if (row->opcode != opcode)
			continue;
		*out = *row;
		return true;
	}

	/* Rmgr-granular rows (any opcode of that rmgr). */
	for (i = 0; i < (int)lengthof(cluster_page_census_rmgr); i++) {
		const ClusterPageRmgrCensusEntry *row = &cluster_page_census_rmgr[i];

		if (row->rmid == rmid) {
			*out = *row;
			return true;
		}
	}
	return false; /* unknown rmgr: fail closed */
}

/* ---------------------------------------------------------------------
 * Census -> classifier wiring
 * --------------------------------------------------------------------- */

void
cluster_page_rmgr_populate_known_set(void)
{
	int i;

	/* Only rows with byte-for-byte differential evidence register: an
	 * unproven opcode must keep classifying UNKNOWN (spec §4.2: a NORMAL
	 * row without every producer/decoder/verifier is BLOCKED). */
	for (i = 0; i < (int)lengthof(cluster_page_census_opcode); i++) {
		const ClusterPageRmgrCensusEntry *row = &cluster_page_census_opcode[i];

		if (row->known_delta && row->affects_page)
			(void)cluster_page_class_register_known_opcode(row->rmid, row->opcode);
	}
}

/* ---------------------------------------------------------------------
 * Redo decode (facts only — no version judgement, no mutation)
 * --------------------------------------------------------------------- */

bool
cluster_page_redo_decode(XLogReaderState *record, uint8 block_id, ClusterPageRedoDecoded *out)
{
	ClusterPageRmgrCensusEntry row;
	DecodedBkpBlock *blk;
	uint8 rmid;

	if (record == NULL || out == NULL)
		return false;
	memset(out, 0, sizeof(*out));

	if (!XLogRecHasBlockRef(record, block_id))
		return false; /* no block reference: nothing to decode */
	rmid = XLogRecGetRmid(record);
	if (!cluster_page_rmgr_census_lookup(rmid, XLogRecGetInfo(record) & XLR_RMGR_INFO_MASK, &row))
		return false; /* unknown rmgr/opcode: fail closed */
	if (!row.affects_page)
		return false; /* not a page-affecting record */

	/* Identity comes straight from the decoded block reference (the
	 * XLogRecGetBlockTag backend helper is not linked into the standalone
	 * unit, and the DecodedXLogRecord fields are the same public data). */
	blk = &record->record->blocks[block_id];
	out->page_class = row.page_class;
	out->identity.rlocator = blk->rlocator;
	out->identity.forknum = blk->forknum;
	out->identity.blocknum = blk->blkno;
	out->full_image
		= XLogRecHasBlockImage(record, block_id) && XLogRecBlockImageApply(record, block_id);
	/* BKPBLOCK_WILL_INIT rides the block's fork_flags (xlogrecord.h). */
	out->will_init = (blk->flags & BKPBLOCK_WILL_INIT) != 0;
	/* §3.1 hints: locator/hint only, never VersionToken. */
	out->hints.record_scn = XLogRecGetScn(record);
	out->hints.record_lsn = record->EndRecPtr;
	/* page_scn/page_lsn are filled by the caller who reads the page. */
	return true;
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
