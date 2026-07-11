/*-------------------------------------------------------------------------
 *
 * cluster_undo_verdict.h
 *	  Cross-node xid -> transaction-outcome verdict taxonomy over shared
 *	  undo (spec-5.22c D3, shared undo physical data plane under GCS,
 *	  Layer-2).
 *
 *	  D2 (spec-5.22b) shipped the owner-as-master coherent block-read
 *	  primitive (cluster_undo_block_acquire_shared).  D3 consumes it to
 *	  answer "did xid commit, and at what SCN?" across nodes, producing the
 *	  five-value verdict below.  Amendment-2 splits the brief's single
 *	  COMMITTED_EXACT into EXACT (a true commit SCN, may be stamped/cached)
 *	  and BOUND (a retention horizon bound valid only against the asking
 *	  read_scn, never stamped/cached) so a consumer switch that handles only
 *	  EXACT falls through to a fail-closed default on a BOUND -- a horizon
 *	  bound can never be mistaken for an exact commit SCN.
 *
 *	  The two mapping helpers here are PURE (no scn_time_cmp, no shmem, no
 *	  elog), so the taxonomy layer links standalone against the
 *	  dependency-free runtime-visibility policy object.  The read_scn
 *	  admissibility gate of a COMMITTED_BOUND (requester leg (e)) is applied
 *	  by the D3-3 consumer, deliberately outside these mappers -- exactly as
 *	  cluster_vis_undo_verdict_page_usable defers it.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_undo_verdict.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.22c-ttslot-verdict-shared-undo.md (D3, §2.1)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_VERDICT_H
#define CLUSTER_UNDO_VERDICT_H

#include "cluster/cluster_runtime_visibility.h" /* ClusterVisTtProof */
#include "cluster/cluster_scn.h"				/* SCN, InvalidScn */

struct ClusterGcsUndoVerdictPage; /* cluster_gcs_block.h */

/*
 * Cross-node xid -> terminal-state verdict (brief line 33 taxonomy;
 * Amendment-2 refines COMMITTED_EXACT into EXACT / BOUND -> five values).
 *
 * 0 == UNKNOWN_FAIL_CLOSED: any path that cannot PROVE a terminal outcome
 * lands here so the caller keeps the 53R97 fail-closed boundary (Rule 8.A /
 * L10: the zero value is fail-closed, never defaults to visible).
 */
typedef enum ClusterUndoVerdictKind {
	CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED = 0,
	CLUSTER_UNDO_VERDICT_COMMITTED_EXACT = 1, /* true commit SCN (may stamp/cache) */
	CLUSTER_UNDO_VERDICT_COMMITTED_BOUND = 2, /* horizon bound only (never stamp/cache) */
	CLUSTER_UNDO_VERDICT_ABORTED = 3,		  /* proven rolled back (CLOG abort) */
	CLUSTER_UNDO_VERDICT_IN_PROGRESS = 4	  /* proven live (Q6: D3 folds to UNKNOWN) */
} ClusterUndoVerdictKind;

/*
 * kind alone distinguishes EXACT vs BOUND (there is deliberately no
 * commit_scn_is_bound side axis, Amendment-2): a switch that only handles
 * COMMITTED_EXACT lets a BOUND fall to default -> fail-closed, so a horizon
 * bound is never stamped/cached as an exact commit_scn.
 */
typedef struct ClusterUndoVerdictResult {
	uint8 kind;		/* ClusterUndoVerdictKind */
	SCN commit_scn; /* EXACT=true commit SCN / BOUND=horizon bound; else InvalidScn */
	uint16 wrap;	/* COMMITTED_EXACT slot wrap evidence */
} ClusterUndoVerdictResult;

/*
 * cluster_undo_verdict_from_wire_page -- map a CP5 origin verdict page to the
 * taxonomy.  Refuses (UNKNOWN_FAIL_CLOSED) any page that does not structurally
 * answer asked_xid (cluster_vis_undo_verdict_page_usable).  A BELOW_HORIZON
 * page maps to COMMITTED_BOUND unconditionally; the read_scn admissibility of
 * that bound is the consumer's gate, not this pure mapper's.  Pure: no I/O, no
 * shmem, no elog.
 */
extern ClusterUndoVerdictResult
cluster_undo_verdict_from_wire_page(const struct ClusterGcsUndoVerdictPage *v,
									TransactionId asked_xid);

/*
 * spec-5.22d D4-6 authority wire-page pair: the serve side fills a version-2
 * page from one block0 prove result (refusing every kind the prove cannot
 * produce), the requester side maps it back through the authority structural
 * gate.  One fill + one mapper so the two wire ends can never disagree.
 * Pure: no I/O, no shmem, no elog.
 */
extern bool cluster_undo_authority_verdict_page_fill(struct ClusterGcsUndoVerdictPage *v,
													 TransactionId xid,
													 const ClusterUndoVerdictResult *r);
extern ClusterUndoVerdictResult
cluster_undo_verdict_from_authority_wire_page(const struct ClusterGcsUndoVerdictPage *v,
											  TransactionId asked_xid);

/*
 * cluster_undo_verdict_from_block_proof -- map a CP3 single-block positive
 * proof to the taxonomy: COMMITTED -> COMMITTED_EXACT{commit_scn,wrap} (the
 * shipped block0 bytes carry the true commit SCN), ABORTED -> ABORTED, NONE ->
 * UNKNOWN_FAIL_CLOSED (the orchestrator reads a CP3 UNKNOWN as "fall to the
 * CP5 origin verdict", it is not the caller's terminal answer).  Pure.
 */
extern ClusterUndoVerdictResult cluster_undo_verdict_from_block_proof(ClusterVisTtProof proof,
																	  SCN commit_scn, uint16 wrap);

/*
 * cluster_undo_verdict_from_resolve -- fold the runtime resolver's outcome
 * (ok, committed, commit_scn, is_bound) into the taxonomy.  This is the
 * Amendment-2 folding point: the legacy is_bound boolean becomes the
 * COMMITTED_BOUND kind (never COMMITTED_EXACT), so a consumer switch that
 * handles only EXACT falls to fail-closed on a bound.  !ok -> UNKNOWN;
 * committed && is_bound -> BOUND; committed && !is_bound -> EXACT; !committed
 * -> ABORTED.  Pure.
 */
extern ClusterUndoVerdictResult cluster_undo_verdict_from_resolve(bool ok, bool committed,
																  SCN commit_scn, bool is_bound);

/*
 * cluster_undo_verdict_resolve — D3 cross-node xid -> commit_scn verdict entry
 * (D3-3), the primitive the D6 consumer calls to decide a seed / fresh-ref
 * tuple.  origin_node = the xid's owner; undo_segment_id = the ITL-ref segment
 * (CP3 block0 locator); anchor_lsn = this tuple's page LSN (version-coverage
 * gate); read_scn = the snapshot SCN (COMMITTED_BOUND admissibility), or
 * InvalidScn for callers without snapshot semantics.  master==self routes to
 * the local durable resolve; master!=self to the CP3 S-grant + CP5 verdict.
 * kind == UNKNOWN_FAIL_CLOSED => the caller keeps 53R97 (never false-visible).
 *
 * authoritative (spec-5.22f D6-7) = the origin was chosen from the tuple page's
 * PHYSICAL ITL binding (a fresh-ref consumer), not derived from the xid value.
 * When true the origin serves underivable own xids over its own durable-TT +
 * CLOG authority (skipping the stripe self-check that guards the derived-path
 * 6.12i P0); the positive-proof gates are unchanged.  Derived (recycled) callers
 * pass false to keep cluster_xid_is_mine.  Compiled only in --enable-cluster builds.
 */
extern ClusterUndoVerdictResult cluster_undo_verdict_resolve(int origin_node,
															 uint32 undo_segment_id,
															 TransactionId raw_xid, SCN read_scn,
															 bool authoritative);

#endif /* CLUSTER_UNDO_VERDICT_H */
