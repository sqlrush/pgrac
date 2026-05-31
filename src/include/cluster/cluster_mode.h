/*-------------------------------------------------------------------------
 *
 * cluster_mode.h
 *	  pgrac cluster mode gates — storage/MVCC vs peer/network.
 *
 *	  P0 (2026-05-31):  cluster.enabled is a DEPLOYMENT MODE, not a switch
 *	  that starts/stops with the current topology size.  Two orthogonal gates:
 *
 *	    cluster_storage_mode_enabled()
 *	        = cluster.enabled && a valid local node_id is configured.
 *	        Gates the pgrac storage/MVCC semantics that an instance owns by
 *	        itself, INDEPENDENT of how many nodes are currently declared/alive:
 *	        ITL/TT/Undo write side, xact-end ITL finish / cleanout, own-instance
 *	        undo emit, own-instance CR construction/cache, snapshot read_scn /
 *	        SNAPSHOT_SOURCE_CLUSTER, local TT slot / undo-segment lifecycle,
 *	        commit/abort SCN advance.  A single-node cluster.enabled deployment
 *	        runs full cluster storage so that when peers join (or recover) the
 *	        on-page / TT / undo metadata is already consistent.
 *
 *	    cluster_peer_mode_enabled()
 *	        = cluster_storage_mode_enabled() && cluster_conf_has_peers().
 *	        Gates the peer/network/distributed paths that only make sense with
 *	        more than one declared node: TT_STATUS_HINT / SUBTRANS / MULTIXACT
 *	        wire fanout, sinval broadcast, GCS / PCM remote ownership transfer,
 *	        Cache Fusion remote block request/send, IC message broadcast,
 *	        LMON / LMS / remote wait / peer reconfig.
 *
 *	  Rule:  cluster.enabled decides storage/MVCC; node_count > 1 decides
 *	  peer/network.  Do NOT gate ITL/TT/Undo/CR on cluster_conf_has_peers().
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.10-cr-block-cache.md (P0 gate split)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_mode.h
 *
 * NOTES
 *	  pgrac-original.  Both predicates are inline hot-path gates (per-tuple /
 *	  per-transaction).  cluster_enabled / cluster_node_id are re-declared
 *	  extern here (matching the cluster_pcm_lock.h pattern) so this leaf header
 *	  does not drag in the full GUC header.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_MODE_H
#define CLUSTER_MODE_H

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_conf.h" /* cluster_conf_has_peers */
#include "cluster/cluster_scn.h"  /* SCN_MAX_VALID_NODE_ID */

extern bool cluster_enabled; /* cluster_guc.c */
extern int cluster_node_id;	 /* cluster_guc.c */

/*
 * cluster_storage_mode_enabled -- pgrac storage/MVCC semantics gate
 * (cluster.enabled + valid node_id; INDEPENDENT of node_count).
 */
static inline bool
cluster_storage_mode_enabled(void)
{
	return cluster_enabled && cluster_node_id >= 0 && cluster_node_id <= SCN_MAX_VALID_NODE_ID;
}

/*
 * cluster_peer_mode_enabled -- peer/network/distributed gate
 * (storage mode AND a declared topology with > 1 node).
 */
static inline bool
cluster_peer_mode_enabled(void)
{
	return cluster_storage_mode_enabled() && cluster_conf_has_peers();
}

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_MODE_H */
