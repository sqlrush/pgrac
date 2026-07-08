/*-------------------------------------------------------------------------
 *
 * cluster_undo_gcs.h
 *	  Shared-undo block GCS integration -- owner-as-master routing +
 *	  coherent grant / PI data plane (spec-5.22b D2).
 *
 *	  D1 (cluster_undo_resid.h) named an undo block as a first-class cluster
 *	  resource whose master IS the owning instance.  D2 wires that identity
 *	  into the real data plane: owner-as-master routing (this file's D2-1
 *	  layer), reader S-grant / writer X-grant with PI invalidation, a
 *	  remaster serve-gate, and the physical migration of undo segments onto
 *	  shared storage.
 *
 *	  D2-1 (owner-as-master routing, this increment) declares the two
 *	  routing predicates.  An undo resource NEVER hash-routes: its master is
 *	  the encoded owner_node (cluster_undo_resid_master), and the GRD/GCS
 *	  hash-master lookups fail closed on the undo class (D1-5 guard, 53R9Q).
 *	  master==self selects the local fast path (no network grant); the
 *	  owner-incarnation epoch self-check that L364 requires before serving
 *	  from that fast path is applied on the grant/acquire path (D2-3), where
 *	  the co-sampled live-authority triple is available -- this pure routing
 *	  predicate is the node-id half only, so cluster_unit links it
 *	  standalone.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_undo_gcs.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.22b-undo-block-gcs-integration.md (D2, §2.1)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_GCS_H
#define CLUSTER_UNDO_GCS_H

#include "cluster/cluster_grd.h" /* ClusterResId */

/*
 * cluster_undo_block_lookup_master -- owner-as-master routing entry.
 *
 *	Returns the encoded owner_node (via cluster_undo_resid_master); an undo
 *	resource is NEVER hash-routed.  cluster_grd_lookup_master /
 *	cluster_gcs_lookup_master fail closed on the undo class (D1-5 guard,
 *	ERRCODE_CLUSTER_UNDO_RESID_HASH_ROUTED), so this is the single legal
 *	master-lookup entry for undo resources.
 */
extern int32 cluster_undo_block_lookup_master(const ClusterResId *undo_resid);

/*
 * cluster_undo_block_master_is_self -- local fast-path routing gate.
 *
 *	true iff this instance is the owning master of the undo resource
 *	(owner_node == cluster_node_id), i.e. the owner reading/writing its own
 *	undo can take the local path without a network grant.  This is the pure
 *	routing half; the owner-incarnation epoch self-check that guards actually
 *	serving from the local fast path (L364) lands on the grant/acquire path
 *	(D2-3), not here.
 */
extern bool cluster_undo_block_master_is_self(const ClusterResId *undo_resid);

#endif /* CLUSTER_UNDO_GCS_H */
