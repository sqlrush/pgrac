/*-------------------------------------------------------------------------
 *
 * cluster_side_recovery.c
 *	  RF-SIDE D-SIDE-06/07/08 — RF-PAGE integration judgements,
 *	  per-resource serve readiness, retention proof exporter
 *	  (implementation).
 *
 *	  Spec: specs/spec-rf-side-typed-recovery-and-shared-terminal.md
 *	  §1.2 D-SIDE-06/07/08 / §2.2 / §4 / §5.
 *
 *	  Judgement-only: the production TT/undo/space owners execute the
 *	  mutations and durability; this file decides the gates.  No ABI
 *	  change, no WAL deletion, no global barrier.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_side_recovery.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_side_recovery.h"

bool
cluster_side_page_consumer_ready(const ClusterSidePageConsumeInput *in)
{
	/*
	 * D-SIDE-06: consume the RF-PAGE proof without re-parsing the page.
	 * The domain binds the exact expected-before version; the complete
	 * PGDEL-06 proof (coverage + durability + post-read + authority) must
	 * hold.  Unknown/absent class or identity fails closed (§2.2: 只令
	 * 交集 resource BLOCKED).
	 */
	if (in == NULL || in->identity == NULL || in->expected_before == NULL)
		return false;
	if (!cluster_page_identity_valid(in->identity))
		return false;
	if (!cluster_page_version_valid(in->expected_before))
		return false;
	if (in->page_class == CLUSTER_PAGE_CLASS_UNKNOWN
		|| in->page_class == CLUSTER_PAGE_CLASS_UNCLASSIFIED)
		return false;
	return in->contributor_coverage && in->durability_barrier_ok && in->post_read_ok
		   && in->authority_revalidated;
}

bool
cluster_side_resource_readiness(const ClusterSideReadinessInput *in)
{
	/*
	 * D-SIDE-07: strictly per-resource (FND-09).  The input contains only
	 * THIS resource's facts — the function cannot see any other resource,
	 * so a healthy unrelated resource can never be blocked by this one's
	 * failure, and no whole-instance barrier exists.
	 */
	if (in == NULL)
		return false;
	return in->page_proof_ok && in->side_proof_ok && in->authority_fresh;
}

ClusterSideRetentionVerdict
cluster_side_retention_proof_ready(const ClusterSideRetentionProof *proof)
{
	/*
	 * D-SIDE-08: the SIDE side of FND-10.  Every affected TT/undo/
	 * pending/space resource must be durable AND canonically post-read,
	 * and no exact consumer may remain.  A missing post-read is a
	 * precise denial — logical DONE never substitutes (spec §4 rows).
	 */
	if (proof == NULL || proof->failed_origin_thread == 0 || proof->affected_count == 0)
		return CLUSTER_SIDE_RETENTION_DENY_INVALID;
	if (!proof->all_bytes_durable)
		return CLUSTER_SIDE_RETENTION_DENY_NOT_DURABLE;
	if (!proof->all_post_read_ok)
		return CLUSTER_SIDE_RETENTION_DENY_NO_POST_READ;
	if (!proof->consumers_zero)
		return CLUSTER_SIDE_RETENTION_DENY_CONSUMER;
	return CLUSTER_SIDE_RETENTION_READY;
}
