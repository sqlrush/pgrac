/*-------------------------------------------------------------------------
 *
 * cluster_undo_gcs.c
 *	  Shared-undo block GCS integration -- owner-as-master routing +
 *	  coherent grant / PI data plane (spec-5.22b D2).
 *
 *	  D2-1 (this increment) ships owner-as-master routing: the two routing
 *	  predicates that keep undo resources off the GRD/GCS hash-master path
 *	  (their authority lives at the owning instance).  The grant / PI /
 *	  serve-gate / physical-migration legs (D2-2 .. D2-5) land here as they
 *	  are implemented.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_undo_gcs.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.22b-undo-block-gcs-integration.md (D2, §2.1)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_mode.h" /* cluster_node_id */
#include "cluster/cluster_undo_gcs.h"
#include "cluster/cluster_undo_resid.h"

/*
 * cluster_undo_block_lookup_master -- owner-as-master routing entry.
 *
 *	The master of an undo resource is the encoded owner_node (D1's
 *	cluster_undo_resid_master), never a shard hash: the undo authority lives
 *	at the owning instance.  This is the single legal master-lookup entry for
 *	the undo class; cluster_grd_lookup_master / cluster_gcs_lookup_master fail
 *	closed on it (D1-5 guard, 53R9Q).
 */
int32
cluster_undo_block_lookup_master(const ClusterResId *undo_resid)
{
	Assert(undo_resid != NULL);
	Assert(cluster_undo_resid_is_undo(undo_resid));

	return cluster_undo_resid_master(undo_resid);
}

/*
 * cluster_undo_block_master_is_self -- local fast-path routing gate.
 *
 *	true iff this instance owns the undo resource (owner_node ==
 *	cluster_node_id), so the owner can read/write its own undo without a
 *	network grant.  The owner-incarnation epoch self-check that L364 requires
 *	before actually serving from the local fast path is applied on the
 *	grant/acquire path (D2-3), where the co-sampled live-authority triple is
 *	available; this predicate is the pure node-id half.
 */
bool
cluster_undo_block_master_is_self(const ClusterResId *undo_resid)
{
	Assert(undo_resid != NULL);
	Assert(cluster_undo_resid_is_undo(undo_resid));

	return cluster_undo_resid_master(undo_resid) == cluster_node_id;
}

/*
 * cluster_undo_path_uses_shared_root -- pure physical-root decision (D2-2).
 *
 *	The single source of truth that both undo path builders consult:
 *	cluster_undo_path_resolve (runtime segment I/O) and the redo write
 *	surface in cluster_undo_xlog.c.  Keeping the decision here (not
 *	duplicated in each builder) is what prevents own-instance undo
 *	split-brain -- runtime reading the shared copy while redo stamps the
 *	local copy (Hardening v1.0.1 裁决 A).
 */
bool
cluster_undo_path_uses_shared_root(ClusterUndoPathIntent intent, bool peer_mode, bool coherence_on)
{
	/*
	 * P1-3 hard contract: a dead-origin materialized copy (recovery rebuilt
	 * it in the local DataDir) NEVER migrates to the shared root, in any
	 * mode.  Redirecting it would send dead-origin resolve / CR reads to an
	 * empty shared path (spec-5.22b §3.6, R1).
	 */
	if (intent == CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL)
		return false;

	Assert(intent == CLUSTER_UNDO_PATH_RUNTIME_SHARED);

	/*
	 * Own live runtime undo migrates to the shared cluster_fs root only when
	 * the whole physical migration is armed: a declared multi-node
	 * deployment (peer_mode) AND cluster.undo_gcs_coherence on.  Either off
	 * => local DataDir, so D2 lands inert at the default (裁决 A).
	 */
	return peer_mode && coherence_on;
}
