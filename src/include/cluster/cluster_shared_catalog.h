/*-------------------------------------------------------------------------
 *
 * cluster_shared_catalog.h
 *	  Shared system-catalog single-authority: pure decision helpers.
 *
 *	  spec-6.14 D1.  This header hosts the dependency-light, side-effect-free
 *	  predicates that gate the shared_catalog feature: the startup vet (which
 *	  hard dependencies must be satisfied before cluster.shared_catalog=on may
 *	  boot) and the single "non-temp = shared" routing criterion consumed by
 *	  the smgr (G1), PCM-track (G2) and lock-globalization (G3) sites.  Keeping
 *	  the judgement in one pure translation unit lets cluster_unit exercise it
 *	  standalone (U8-U10, U14-U16) and guarantees a future gate cannot drift
 *	  away from the others (spec-6.14 R6).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_shared_catalog.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D1)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SHARED_CATALOG_H
#define CLUSTER_SHARED_CATALOG_H

#include "c.h"

/*
 * Result of the shared_catalog startup vet.  Values other than _OK name the
 * FIRST unsatisfied hard dependency, in a fixed priority order, so the FATAL
 * errhint can point the operator at exactly one thing to fix.
 */
typedef enum ClusterSharedCatalogVetResult
{
	CLUSTER_SHARED_CATALOG_VET_OK = 0,
	CLUSTER_SHARED_CATALOG_VET_MISSING_SMGR_USER_RELATIONS,
	CLUSTER_SHARED_CATALOG_VET_MISSING_SHARED_DATA_DIR,
	CLUSTER_SHARED_CATALOG_VET_MISSING_CF_AUTHORITY,
	CLUSTER_SHARED_CATALOG_VET_MISSING_MERGED_RECOVERY
} ClusterSharedCatalogVetResult;

/*
 * cluster_shared_catalog_vet -- pure startup dependency check.
 *
 *	Given the shared_catalog master switch and the state of its four hard
 *	dependencies, return the first unmet dependency (priority order:
 *	smgr_user_relations, shared_data_dir, controlfile_shared_authority,
 *	merged_recovery) or _OK.  When shared_catalog is off the result is
 *	always _OK (the feature imposes no requirements and the off path is
 *	byte-identical to stock PG).
 *
 *	The four dependencies exist because shared_catalog can only be sound
 *	when (a) permanent relations already route through cluster_smgr, (b) a
 *	shared data root is configured to hold the single catalog tree, (c)
 *	a shared pg_control authority exists for join nodes to adopt the system
 *	identifier from (spec-6.14 §2.3 / §3.5), and (d) cold crash recovery
 *	runs under the k-way merged-replay window: with the catalog PCM-tracked,
 *	single-stream redo of a DDL-bearing WAL tail would issue live GCS
 *	requests from the startup process, which has no backend identity and no
 *	recovery ownership of the shared pages (spec-6.14 D9 amend INV-D9-R).
 */
extern ClusterSharedCatalogVetResult
			cluster_shared_catalog_vet(bool shared_catalog,
									   bool smgr_user_relations,
									   bool have_shared_data_dir,
									   bool controlfile_shared_authority,
									   bool merged_recovery);

/*
 * cluster_shared_catalog_vet_missing_dep_name -- human-readable dependency
 *	name for a non-OK vet result, for use in the FATAL errhint.  Returns a
 *	static string; returns "" for _OK.
 */
extern const char *cluster_shared_catalog_vet_missing_dep_name(ClusterSharedCatalogVetResult r);

/*
 * cluster_shared_catalog_is_shared_rel -- the single "non-temp = shared"
 *	routing criterion (spec-6.14 §2.4).  Only meaningful under
 *	shared_catalog=on; callers gate on the master switch first.  is_temp is
 *	true for temp/local (backend-qualified) relations, which always stay
 *	node-local.  Everything else is cluster-shared -- the historic
 *	relNumber >= FirstNormalObjectId catalog/user split collapses to this,
 *	so a rewritten catalog (VACUUM FULL pg_class) no longer creates routing
 *	ambiguity.  This is the ONE place the judgement lives (R6).
 */
static inline bool
cluster_shared_catalog_is_shared_rel(bool is_temp)
{
	return !is_temp;
}

/* ---- D13 temp-namespace identity (pure) -------------------------------- */

/*
 * cluster_temp_namespace_format_suffix -- format the numeric suffix of a temp
 *	namespace name (the part after the "pg_temp_" / "pg_toast_temp_" prefix).
 *	Under shared_catalog=on the suffix is node-qualified "n<node>_<backendId>"
 *	so a temp namespace created on one node can never be mistaken for a
 *	same-backendId namespace on another node (spec-6.14 D13 / R10).  Off mode
 *	(and the leading 'n' absence) yields the stock "<backendId>" format, so
 *	existing clusters and PG regression are byte-identical.
 */
extern void cluster_temp_namespace_format_suffix(char *buf, size_t buflen,
												 bool shared_catalog,
												 int node, int backend_id);

/*
 * cluster_temp_namespace_parse_suffix -- parse the numeric suffix (the part
 *	after "pg_temp_" / "pg_toast_temp_").  Returns the backend id; sets
 *	*out_node to the node id, or -1 for the stock (un-node-qualified) format.
 *	The leading 'n' distinguishes the node-qualified format from the stock
 *	one, so both parse unambiguously.  Returns -1 (invalid backend id) when the
 *	suffix is malformed.
 */
extern int	cluster_temp_namespace_parse_suffix(const char *suffix, int *out_node);

#endif							/* CLUSTER_SHARED_CATALOG_H */
