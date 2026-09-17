/*-------------------------------------------------------------------------
 *
 * cluster_page_handoff.h
 *	  RF-PAGE PGDEL-07 — per-resource page-proof handoff to RF-ROOT/
 *	  RF-SIDE with the §7.4 FND-10 retention judgement.
 *
 *	  Spec: specs/spec-rf-page-crash-safe-page-replay-journal.md
 *	  §2.1 PGDEL-07 ("per-resource proof handoff 给 RF-ROOT/SIDE；不创建
 *	  global barrier"), §7.3 page proof export, §7.4 FND-10 handoff,
 *	  §6.4 release scope.
 *
 *	  DELIVERED HERE:
 *	    - cluster_page_handoff_ready: the FND-10 conjunction for ONE
 *	      resource — the typed page proof (PGDEL-06) + the RF-SIDE proof
 *	      + the failed-origin retention interval still pinned + zero
 *	      remaining consumers.  Only when all four hold may the failed-
 *	      origin redo be retired for that interval; retaining redo never
 *	      by itself proves the torn target has a stable base (§7.4).
 *	    - cluster_page_handoff_retention_denied: the fail-closed answer
 *	      for a retire request that arrives before the PAGE proof exists
 *	      (PL-12: RF-ROOT denies removal).
 *
 *	  HARD CONTRACT (G1/G3 — do not erase): this layer judges the
 *	  handoff; the production wiring that actually delivers the proof to
 *	  the RF-ROOT/SIDE consumers (and the §10.3 production-caller test)
 *	  is the PGDEL-09/10 work.  The handoff is PER-RESOURCE: one block
 *	  ready never proves relation/thread/instance/WAL-interval readiness
 *	  (§6.4), and this API carries no global barrier state.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_page_handoff.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_HANDOFF_H
#define CLUSTER_PAGE_HANDOFF_H

#include "cluster/cluster_page_apply.h"

/*
 * One resource's handoff judgement inputs.  All are typed facts:
 * `proof` is the §7.3 page proof produced by the §7.2 sequence;
 * `side_proof_ok` is the RF-SIDE page-dependency proof; `retention_pinned`
 * says the failed-origin interval covering ALL this resource's
 * contributors is still retained; `consumers_zero` says no consumer
 * remains for this resource.
 */
typedef struct ClusterPageHandoffInput {
	const ClusterPageProof *proof;
	bool side_proof_ok;
	bool retention_pinned;
	bool consumers_zero;
} ClusterPageHandoffInput;

/*
 * §7.4 FND-10: true only when the typed page proof is complete
 * (contributor coverage + durability + post-read + authority
 * revalidation), the RF-SIDE dependency proof holds, the failed-origin
 * interval is still pinned, and no consumer remains.  Retaining the redo
 * alone is never the correctness closure (§7.4 last sentence).
 */
extern bool cluster_page_handoff_ready(const ClusterPageHandoffInput *in);

/*
 * PL-12: a redo-retire request before the PAGE proof exists is DENIED —
 * the resource stays retained, fail-closed.  (The judge also covers a
 * retire request racing an in-flight handoff: not-yet-ready is a retry,
 * never authority to remove the interval.)
 */
extern bool cluster_page_handoff_retention_denied(const ClusterPageHandoffInput *in);

#endif /* CLUSTER_PAGE_HANDOFF_H */
