/*-------------------------------------------------------------------------
 *
 * cluster_page_handoff.c
 *	  RF-PAGE PGDEL-07 — per-resource proof handoff + §7.4 FND-10
 *	  retention judgement (implementation).
 *
 *	  Spec: specs/spec-rf-page-crash-safe-page-replay-journal.md
 *	  §2.1 PGDEL-07 / §6.4 / §7.3 / §7.4 / §10.2 PL-12.
 *
 *	  Judgement-only, per-resource, no global barrier state.  The
 *	  production delivery of the proof to the RF-ROOT/RF-SIDE consumers
 *	  is the PGDEL-09/10 wiring; this file decides readiness/denial.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_page_handoff.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_page_handoff.h"

bool
cluster_page_handoff_ready(const ClusterPageHandoffInput *in)
{
	const ClusterPageProof *p;

	if (in == NULL || in->proof == NULL)
		return false;
	p = in->proof;

	/*
	 * §7.4 FND-10 conjunction for ONE resource:
	 *   - the typed page proof is complete: contributor coverage closed,
	 *     durability barrier done, canonical post-read verified, RF-ROOT
	 *     authority revalidated (§7.3 — without any of them the proof is
	 *     not a proof);
	 *   - the RF-SIDE page-dependency proof holds;
	 *   - the failed-origin interval covering this resource's
	 *     contributors is still retained (a retire request may only
	 *     consume this exact interval);
	 *   - no consumer remains.
	 * Retaining the failed-origin redo only solves retirement — it never
	 * provides the torn target's stable base (§7.4 last sentence), so
	 * retention_pinned alone cannot make this true.
	 */
	return p->contributor_coverage && p->durability_barrier_ok && p->post_read_ok
		   && p->authority_revalidated && in->side_proof_ok && in->retention_pinned
		   && in->consumers_zero;
}

bool
cluster_page_handoff_retention_denied(const ClusterPageHandoffInput *in)
{
	/*
	 * PL-12: a retire request that arrives before the PAGE proof exists
	 * is denied (fail-closed).  Not-ready is a retry, never authority to
	 * remove the retained interval: the deny is the default answer for
	 * every input that is not a complete FND-10 handoff.
	 */
	return !cluster_page_handoff_ready(in);
}
