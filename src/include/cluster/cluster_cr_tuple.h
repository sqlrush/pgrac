/*-------------------------------------------------------------------------
 *
 * cluster_cr_tuple.h
 *	  pgrac tuple-level / verdict-only CR read fast path (spec-5.54).
 *
 *	  A compute-only, verdict-only fast path layered IN FRONT OF the spec-3.9
 *	  full-block CR construct (cluster_cr.c), taken only when a snapshot needs
 *	  the visibility verdict of ONE post-read_scn tuple whose block has exactly
 *	  ONE candidate transaction (nchains == 1).  Instead of materializing the
 *	  whole read_scn block image (and caching it), it reconstructs ONLY the
 *	  queried offnum on a backend-local scratch via a single-chain target-offnum
 *	  walk, then runs the SAME per-tuple verdict the full-block gate runs.
 *
 *	  CORRECTNESS MODEL (spec-5.54 §3.1, rule 8.A — visibility verdict path):
 *	    The fast-path verdict is BIT-EQUIVALENT to the full-block verdict, OR it
 *	    fail-safe FALLS BACK to the authoritative full-block path.  It NEVER
 *	    guesses, NEVER returns a stale verdict, NEVER bypasses the spec-5.53
 *	    reuse fence, and NEVER decides corruption from the target-only (narrower)
 *	    view -- identity uncertainty is a fallback, not a fail-close.  The six
 *	    invariants:
 *	      INV-T1  verdict bit-equivalent to full-block (DECIDED case).
 *	      INV-T2  cannot prove equivalence -> NOT_APPLICABLE -> full-block.
 *	      INV-T3  fail-closed ONLY for unambiguous physical terminals
 *	              (missing-undo 53R9F / cross-instance 53R9G / cliff
 *	              data_corrupted), view-independent; identity uncertainty -> fallback.
 *	      INV-T4  lineage identity is checked at WALK time per restore
 *	              (occ.xmin == img.xmin || occ.xmin == img.xmax), never a static
 *	              predicate (the undo image only exists once the record is read).
 *	      INV-T5  consume the existing resolver/verdict helpers; never re-create.
 *	      INV-T6  v1 is restricted to nchains == 1; nchains > 1 falls back.
 *
 *	  v1 is COMPUTE-ONLY VERDICT-ONLY: it emits no tuple bytes, builds no
 *	  cross-backend cache, and keeps its counters in an INDEPENDENT shmem region
 *	  (cluster_cr_tuple_stat.c) so the held spec-5.51 ClusterCRShared layout is
 *	  untouched.  catversion is NOT bumped.  The GUC
 *	  cluster.cr_tuple_level_fastpath defaults OFF (measure-first; spec-5.58
 *	  validates equivalence + perf before any default flip).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.54-tuple-level-cr-verdict-only.md (FROZEN v1.0)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_cr_tuple.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CR_TUPLE_H
#define CLUSTER_CR_TUPLE_H

#ifndef FRONTEND

#include "postgres.h"

#include "access/htup.h"
#include "cluster/cluster_cr.h" /* ClusterCrVerdict */
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_scn.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "utils/snapshot.h"

/* GUC cluster.cr_tuple_level_fastpath (default OFF, PGC_USERSET, spec-5.54 D5)
 * is declared in cluster_guc.h and defined in cluster_guc.c (the project's
 * GUC-backing-variable home, alongside cluster_cr_gate_no_peer_fastpath). */

/*
 * ClusterCRTupleOutcome -- classification of one fast-path attempt, indexed
 * into the independent counter region (cluster_cr_tuple_stat.c).  Exactly one
 * VERDICT (the fast path produced a DECIDED answer) plus seven FALLBACK reasons
 * (the attempt fell back to the authoritative full-block path).  __COUNT bounds
 * the counter array.  Advisory only -- corruption/staleness affects
 * observability, never correctness (these never feed a verdict).
 */
typedef enum ClusterCRTupleOutcome {
	CR_TUPLE_OUTCOME_VERDICT = 0,		   /* fast path returned DECIDED */
	CR_TUPLE_OUTCOME_FALLBACK_REMOTE,	   /* materialized-remote (eligibility) */
	CR_TUPLE_OUTCOME_FALLBACK_RECYCLE_WM,  /* recycle watermark > read_scn (eligibility) */
	CR_TUPLE_OUTCOME_FALLBACK_MULTICHAIN,  /* nchains > 1 (eligibility, INV-T6) */
	CR_TUPLE_OUTCOME_FALLBACK_CLIFF,	   /* chain-length estimate > cliff (eligibility) */
	CR_TUPLE_OUTCOME_FALLBACK_IDENTITY,	   /* walk-time lineage identity unprovable */
	CR_TUPLE_OUTCOME_FALLBACK_CROSS_BLOCK, /* cross-block dependency / cannot isolate */
	CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN,   /* offnum out of range / no space / other */
	CR_TUPLE_OUTCOME__COUNT,
} ClusterCRTupleOutcome;

/*
 * cluster_cr_tuple_eligible_page -- the PURE static eligibility predicate
 * (spec-5.54 D1 / §3.2).  No undo image is read here (lineage identity is a
 * WALK-time check, INV-T4), so this is a side-effect-free predicate over the
 * page + scalar inputs and is exercised standalone in cluster_unit.
 *
 *   Returns true (fast path may proceed) IFF ALL hold:
 *     - tuple_origin_node_id == self_node_id   (own-instance; F0-4)
 *     - recycle_watermark <= read_scn          (page candidate set complete; F0-16)
 *     - nchains == 1                            (single post-read_scn txn; INV-T6)
 *     - FirstOffsetNumber <= offnum <= max_off  (queried offnum in page)
 *   On false, *reason is set to the matching FALLBACK outcome.
 *
 *   The chain-walk cliff (F0-18) is NOT estimable statically (depth is unknown
 *   before walking), so it is enforced during the D2 walk: exceeding
 *   cluster_cr_chain_walk_max_steps -> FALLBACK_CLIFF (the authoritative
 *   full-block re-walk then produces the data_corrupted SQLSTATE).
 */
extern bool cluster_cr_tuple_eligible_page(Page page, OffsetNumber offnum, SCN read_scn,
										   int nchains, int32 tuple_origin_node_id,
										   int32 self_node_id, ClusterCRTupleOutcome *reason);

/*
 * cluster_cr_tuple_eligible -- thin Buffer wrapper over the pure predicate
 * (spec-5.54 D1).  Resolves the page, queried offnum, recycle watermark and the
 * tuple origin from the live buffer + slot, then defers to the pure predicate.
 * Pure / no correctness side effects; nchains is supplied by the caller (it
 * already collected the candidate chains for the full-block path).
 */
extern bool cluster_cr_tuple_eligible(Buffer buf, HeapTuple htup, Snapshot snapshot,
									  const ClusterItlSlotData *slot, int nchains,
									  ClusterCRTupleOutcome *reason);

/*
 * ClusterCRTupleApplyResult -- per-record outcome the D2 walk driver acts on
 * (spec-5.54 §3.3 TargetTupleCrState).
 */
typedef enum ClusterCRTupleApplyResult {
	CR_TUPLE_APPLY_OK = 0,	 /* applied or no-op; continue walking prev_uba */
	CR_TUPLE_APPLY_FALLBACK, /* uncertain (identity/no-space/malformed) -> NOT_APPLICABLE */
} ClusterCRTupleApplyResult;

/*
 * cluster_cr_tuple_prune_target -- per-step target-only prune (spec-5.54 §3.3,
 * mirrors full-block whole-page prune restricted to the queried offnum, F0-25).
 * Mark the queried offnum LP_UNUSED IFF its occupant's raw xmin == candidate_xid
 * (a post-read_scn version that did not exist at read_scn).  Pure page mutation.
 */
extern void cluster_cr_tuple_prune_target(char *scratch_page, OffsetNumber queried_offnum,
										  TransactionId candidate_xid);

/*
 * cluster_cr_tuple_apply_record -- apply ONE undo record to the queried offnum on
 * the fast scratch per TargetTupleCrState (spec-5.54 D2; pure, unit-tested).
 * Precondition (enforced by the driver): the record is post-read_scn AND targets
 * THIS block.  record_buf points at UndoRecordHeader + payload; len is its size.
 *   - ITL records: restore the ITL slot (+ its target header if NORMAL) for EVERY
 *     record -- the verdict's recycled-deleter resolve reads the scratch ITL slot,
 *     so the slot array must match full-block (D0b §B).
 *   - INSERT/UPDATE/DELETE: inverse-apply ONLY when target_offset == queried_offnum
 *     (the verdict never reads non-queried tuples); otherwise a no-op.
 *   Returns CR_TUPLE_APPLY_FALLBACK + *reason on any apply failure (foreign
 *   identity / no space / malformed) -- the driver then returns NOT_APPLICABLE so
 *   the authoritative full-block path decides (INV-T2/T3, never a fast-path guess).
 */
extern ClusterCRTupleApplyResult cluster_cr_tuple_apply_record(char *scratch_page,
															   OffsetNumber queried_offnum,
															   const char *record_buf, size_t len,
															   ClusterCRTupleOutcome *reason);

/*
 * cluster_cr_tuple_verdict -- single-chain target-offnum CR verdict
 * (spec-5.54 D2/D4).  Precondition: the caller already passed the 3-tier gate +
 * live-xmin guard + cluster_cr_tuple_eligible (so nchains == 1, own-instance).
 *
 *   Reconstructs ONLY the queried offnum on a backend-local scratch by walking
 *   the single candidate chain (TargetTupleCrState contract, walk-time identity
 *   guard), then runs the SAME per-tuple verdict the full-block gate runs
 *   (cluster_cr_verdict_on_image, D4) -- so a DECIDED verdict is bit-equivalent
 *   to full-block (INV-T1).
 *
 *   Returns:
 *     CLUSTER_CR_DECIDED        *out_visible authoritative (equivalent full-block)
 *     CLUSTER_CR_FAILCLOSED     unambiguous physical terminal: the caller ereports
 *                               (the walk already ereported the precise SQLSTATE)
 *     CLUSTER_CR_NOT_APPLICABLE could not prove equivalence -> caller falls back
 *                               to the authoritative full-block (INV-T2/T3)
 *   *out_reason (optional) receives the outcome classification for the counters.
 */
extern ClusterCrVerdict cluster_cr_tuple_verdict(Buffer buf, HeapTuple htup, Snapshot snapshot,
												 bool *out_visible,
												 ClusterCRTupleOutcome *out_reason);

/* ---- independent counter region (spec-5.54 D5; mirrors cluster_cr_admit_stat.c) ---- */
extern Size cluster_cr_tuple_stat_shmem_size(void);
extern void cluster_cr_tuple_stat_shmem_init(void);
extern void cluster_cr_tuple_stat_shmem_register(void);
extern void cluster_cr_tuple_stat_bump(ClusterCRTupleOutcome outcome);
extern uint64 cluster_cr_tuple_stat_count(ClusterCRTupleOutcome outcome);

#endif /* !FRONTEND */

#endif /* CLUSTER_CR_TUPLE_H */
