/*-------------------------------------------------------------------------
 *
 * cluster_page_set.c
 *	  RF-PAGE PGDEL-05 — per-block recovery set + §6.3 contributor
 *	  closure (implementation).
 *
 *	  Spec: specs/spec-rf-page-crash-safe-page-replay-journal.md
 *	  §2.1 PGDEL-05 / §3.3 / §3.4 / §6.1 / §6.3 / §11.1 G9.
 *
 *	  Pure judgement over the ordered contributor list: exact-version
 *	  adjacency only; any gap, unknown class, incarnation boundary
 *	  crossing or terminal mismatch fails closed (mutation=0, retain
 *	  interval).  The recovery set itself is never persisted.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_page_set.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_page_set.h"

/* Exact adjacency: c[i].result == c[i+1].expected, including incarnation
 * (equal SCN with different incarnation/identity is NOT adjacency,
 * spec §3.4/PU-06/PU-27). */
static bool
cluster_page_chain_adjacent(const ClusterPageRedoChange *a, const ClusterPageRedoChange *b)
{
	if (!cluster_page_version_valid(&a->result_version)
		|| !cluster_page_version_valid(&b->expected_before))
		return false;
	return cluster_page_version_equal(&a->result_version, &b->expected_before);
}

ClusterPageClosureResult
cluster_page_contributor_closure(const ClusterBlockRecoverySet *set)
{
	int i;

	if (set == NULL || set->n_contributors < 0)
		return CLUSTER_PAGE_CLOSURE_INVALID_INPUT;
	/* An empty chain is a NULL contributor list (n==0, contributors==NULL);
	 * a non-NULL list with n==0, or a NULL list with n>0, is contradictory. */
	if ((set->n_contributors == 0) != (set->contributors == NULL))
		return CLUSTER_PAGE_CLOSURE_INVALID_INPUT;
	if (!cluster_page_identity_valid(&set->identity)
		|| !cluster_page_version_valid(&set->source_version)
		|| !cluster_page_version_valid(&set->terminal_version))
		return CLUSTER_PAGE_CLOSURE_INVALID_INPUT;
	if (set->page_class == CLUSTER_PAGE_CLASS_UNKNOWN
		|| set->page_class == CLUSTER_PAGE_CLASS_UNCLASSIFIED)
		return CLUSTER_PAGE_CLOSURE_UNKNOWN_CLASS;

	/* Empty chain: covered only when the source already IS the terminal
	 * (§6.2-4: an empty replay set is a valid conclusion, but only with
	 * the production stability witness the PGDEL-04 CURRENT validator
	 * carries — the version equality here is the chain half of that). */
	if (set->n_contributors == 0) {
		if (cluster_page_version_equal(&set->source_version, &set->terminal_version))
			return CLUSTER_PAGE_CLOSURE_OK;
		return CLUSTER_PAGE_CLOSURE_GAP;
	}

	for (i = 0; i < set->n_contributors; i++) {
		const ClusterPageRedoChange *c = &set->contributors[i];

		/* Same-block, same-origin, classified, valid versions: a
		 * contributor that is not classifiable or whose versions are
		 * unknown cannot join, and a contributor of ANOTHER failed origin
		 * is never part of this block's chain (spec §6.3: raw cross-thread
		 * LSN has no global ordering — PU-27; sequential failures bind each
		 * origin to its own root per §6.2-6). */
		if (!cluster_page_identity_equal(&c->identity, &set->identity))
			return CLUSTER_PAGE_CLOSURE_GAP;
		if (c->failed_origin_thread != set->failed_origin_thread)
			return CLUSTER_PAGE_CLOSURE_THREAD_MISMATCH;
		if (c->page_class == CLUSTER_PAGE_CLASS_UNKNOWN
			|| c->page_class == CLUSTER_PAGE_CLASS_UNCLASSIFIED)
			return CLUSTER_PAGE_CLOSURE_UNKNOWN_CLASS;
		if (!cluster_page_version_valid(&c->expected_before)
			|| !cluster_page_version_valid(&c->result_version))
			return CLUSTER_PAGE_CLOSURE_GAP;

		/* First contributor must join from the selected source version. */
		if (i == 0) {
			if (!cluster_page_version_equal(&set->source_version, &c->expected_before))
				return CLUSTER_PAGE_CLOSURE_GAP;
		} else if (!cluster_page_chain_adjacent(&set->contributors[i - 1], c)) {
			/* Incarnation boundary: the versions are unequal (equal SCN
			 * with different incarnation is a mismatch, PU-06).  Report
			 * the incarnation cross distinctly so the caller can route
			 * to the PC-INCARNATION lifecycle instead of a silent gap. */
			if (cluster_page_identity_equal(&set->contributors[i - 1].identity, &c->identity)
				&& set->contributors[i - 1].result_version.incarnation
					   != c->expected_before.incarnation)
				return CLUSTER_PAGE_CLOSURE_INCARNATION_CROSS;
			return CLUSTER_PAGE_CLOSURE_GAP;
		}
	}

	/* Terminal: the last result must equal the required terminal EXACTLY
	 * (terminal uniqueness by construction — a mismatch fails instead of
	 * guessing). */
	if (!cluster_page_version_equal(&set->contributors[set->n_contributors - 1].result_version,
									&set->terminal_version))
		return CLUSTER_PAGE_CLOSURE_TERMINAL_MISMATCH;
	return CLUSTER_PAGE_CLOSURE_OK;
}

bool
cluster_page_contributor_chain_covers(const ClusterBlockRecoverySet *set,
									  const ClusterPageVersion *from_version)
{
	int start;
	int i;

	if (set == NULL || from_version == NULL || !cluster_page_version_valid(from_version))
		return false;
	if (!cluster_page_version_valid(&set->source_version)
		|| !cluster_page_version_valid(&set->terminal_version))
		return false;
	if (set->contributors == NULL || set->n_contributors <= 0)
		return false;

	/* Find the change whose result_version == from_version: the chain
	 * from that change's result is closed when the remaining chain is
	 * adjacent and lands on the terminal. */
	start = -1;
	for (i = 0; i < set->n_contributors; i++) {
		if (cluster_page_version_equal(&set->contributors[i].result_version, from_version)) {
			start = i;
			break;
		}
	}
	if (start < 0)
		return false; /* from_version not produced by any change */

	for (i = start; i < set->n_contributors; i++) {
		const ClusterPageRedoChange *c = &set->contributors[i];

		if (c->page_class == CLUSTER_PAGE_CLASS_UNKNOWN
			|| c->page_class == CLUSTER_PAGE_CLASS_UNCLASSIFIED)
			return false;
		if (!cluster_page_version_valid(&c->expected_before)
			|| !cluster_page_version_valid(&c->result_version))
			return false;
		if (i > start && !cluster_page_chain_adjacent(&set->contributors[i - 1], c))
			return false;
	}
	return cluster_page_version_equal(&set->contributors[set->n_contributors - 1].result_version,
									  &set->terminal_version);
}
