/*-------------------------------------------------------------------------
 *
 * cluster_block_apply.h
 *	  pgrac backend single-block redo-apply framework (spec-4.10 D3a).
 *
 *	  Online block recovery reconstructs one corrupt/lost-write block from
 *	  WAL without a full-database PANIC.  The reconstruction replays, onto a
 *	  DETACHED char[BLCKSZ] page, the chain of WAL records that touch the
 *	  block (latest FPI <= block_target, then the post-FPI deltas).  This
 *	  header holds the framework contract; cluster_block_apply.c implements
 *	  the per-record applicator.
 *
 *	  cluster_block_apply_decide() is the PURE, rmgr-agnostic routing
 *	  decision (unit-testable, mirrors cluster_recovery_record_class):
 *	    - no block reference for this block      -> NOOP
 *	    - an apply-able full-page image present   -> FPI (RestoreBlockImage,
 *	                                                 rmgr-agnostic)
 *	    - otherwise                               -> DELTA (per-rmgr handler)
 *
 *	  cluster_block_apply_one() applies ONE decoded record to the block on a
 *	  detached page.  CORRECTNESS CONTRACT (8.A, R11 "极高"): for every
 *	  supported record type the result must be byte-for-byte identical to
 *	  what PG's real redo would leave on that block (in a backend, i.e.
 *	  outside the merged-recovery window: PageSetLSN stamps pd_lsn only).
 *	  A record type that has not passed the byte-for-byte differential is
 *	  NOT on the apply matrix and fails closed (UNSUPPORTED) -- never a
 *	  silent wrong-block install.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_block_apply.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.10-online-block-recovery.md (FROZEN v0.4)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_BLOCK_APPLY_H
#define CLUSTER_BLOCK_APPLY_H

#include "c.h"

struct XLogReaderState;

/*
 * Routing decision for one (record, block) pair.  Pure; depends only on the
 * block-reference / full-page-image facts of the record, not on shared state.
 */
typedef enum ClusterBlkApplyAction {
	CLUSTER_BLKAPPLY_ACT_NOOP,	/* record does not reference this block */
	CLUSTER_BLKAPPLY_ACT_FPI,	/* apply-able full-page image -> restore */
	CLUSTER_BLKAPPLY_ACT_DELTA, /* no image -> per-rmgr delta handler needed */
} ClusterBlkApplyAction;

/*
 * Outcome of applying one record to the detached page.  Anything other than
 * OK / NOOP is a fail-closed signal the caller (orchestrator, D1+) must treat
 * as block-unrecoverable -- never install the page (8.A).
 */
typedef enum ClusterBlkApplyResult {
	CLUSTER_BLKAPPLY_OK,		  /* FPI restored / delta applied to page */
	CLUSTER_BLKAPPLY_NOOP,		  /* record does not touch this block */
	CLUSTER_BLKAPPLY_UNSUPPORTED, /* rmgr/record-type not on the apply matrix */
	CLUSTER_BLKAPPLY_FAILED,	  /* on matrix but image/delta unusable */
} ClusterBlkApplyResult;

/*
 * cluster_block_apply_decide -- pure routing decision (see header comment).
 *
 *	apply_image implies has_image (PG only sets apply_image when an image is
 *	present); both are accepted explicitly for defensiveness.
 */
static inline ClusterBlkApplyAction
cluster_block_apply_decide(bool has_block_ref, bool has_image, bool apply_image)
{
	if (!has_block_ref)
		return CLUSTER_BLKAPPLY_ACT_NOOP;
	if (has_image && apply_image)
		return CLUSTER_BLKAPPLY_ACT_FPI;
	return CLUSTER_BLKAPPLY_ACT_DELTA;
}

extern ClusterBlkApplyResult cluster_block_apply_one(struct XLogReaderState *record, uint8 block_id,
													 char *page);

/*
 * Per-rmgr single-block applicators (one matrix entry per rmgr), implemented in
 * cluster_block_apply_<rmgr>.c.  Each applies ONE delta record's effect on the
 * target block to a detached page, byte-for-byte identical to PG's redo
 * (verified by the t/256 differential), or fails closed (8.A / R11).
 */
extern ClusterBlkApplyResult cluster_block_apply_heap(struct XLogReaderState *record,
													  uint8 block_id, char *page);

#endif /* CLUSTER_BLOCK_APPLY_H */
