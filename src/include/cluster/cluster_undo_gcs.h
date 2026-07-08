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

#include "cluster/cluster_grd.h"				/* ClusterResId */
#include "cluster/storage/cluster_undo_alloc.h" /* ClusterUndoPathIntent */

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

/*
 * cluster_undo_path_uses_shared_root -- pure physical-root decision (D2-2).
 *
 *	Returns true iff an undo segment with the given path intent resolves to
 *	the shared cluster_fs root (cluster.shared_data_dir) rather than the
 *	local DataDir.  Pure: takes the mode inputs explicitly (no globals), so
 *	cluster_unit drives every mode combination standalone.
 *
 *	  RUNTIME_SHARED     -> shared iff (peer_mode && coherence_on)
 *	  MATERIALIZED_LOCAL -> always local (P1-3 hard contract, spec-5.22b §3.6)
 *
 *	cluster_undo_path_resolve (cluster_undo_alloc.c) and the redo write
 *	surface (cluster_undo_xlog.c) read the live peer_mode / coherence globals
 *	and delegate the decision here so the two path builders never diverge
 *	(own-instance undo split-brain, Hardening v1.0.1 裁决 A).
 */
extern bool cluster_undo_path_uses_shared_root(ClusterUndoPathIntent intent, bool peer_mode,
											   bool coherence_on);

/*
 * cluster_undo_intent_for_owner (the per-call intent derivation every undo
 * smgr / path call site uses) is a static inline in cluster_undo_alloc.h so
 * the ~30 call sites take no link dependency on this routing object.
 */

#endif /* CLUSTER_UNDO_GCS_H */
