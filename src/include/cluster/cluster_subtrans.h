/*-------------------------------------------------------------------------
 *
 * cluster_subtrans.h
 *	  pgrac SUBTRANS cross-node visibility — eager state propagation +
 *	  lazy reader follow.
 *
 *	  spec-3.5 D4 (NEW;Stage 3 第 9 sub-spec).
 *
 *	  This module wires PG SUBTRANS subxact lifecycle (subcommit /
 *	  subabort / top commit) into the cluster TT status overlay +
 *	  V3 wire propagation.  Origin emits eagerly;remote reader resolves
 *	  parent chain lazily on visibility check.
 *
 *	  Q3 = C' (brainstorm):  eager state propagation + lazy consumer
 *	  resolution.  Remote node has no local pg_subtrans;origin MUST
 *	  emit child status + parent_key or the reader will never resolve.
 *
 *	  Single-node fast path (cluster_conf_has_peers() == false):  all
 *	  helpers early-return.  L195 zero tax guarantee preserved.
 *
 *	  HC contracts in this header (HC203-HC205 3 NEW):
 *	    HC203 ensure-parent-binding-first — caller MUST install a
 *	          parent IN_PROGRESS overlay entry BEFORE emitting child
 *	          SUBCOMMITTED.  Otherwise remote lazy follow misses.
 *	    HC204 lifecycle-in-xact-c — all subxact lifecycle hooks (subcommit
 *	          / subabort / top commit) live in xact.c D7.  subtrans.c
 *	          (D6) only records SubTransSetParent metadata (1-2 callsite,
 *	          optional lightweight).
 *	    HC205 bounded-chain-depth — reader lazy follow recursion is
 *	          bounded by cluster.subtrans_max_chain_depth (GUC, default
 *	          32);depth exceeded → 53R97 fail-closed (L199;NOT PG-native
 *	          fallback).
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.5-subtrans-cross-node-visibility.md (v0.3 FROZEN 2026-05-26)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_subtrans.h
 *
 * NOTES
 *	  pgrac-original file (spec-3.5 D4 NEW,2026-05-26).  All types use
 *	  the ClusterSubtrans prefix.  All exported functions use the
 *	  cluster_subtrans_ prefix.  Companion implementation lives in
 *	  src/backend/cluster/cluster_subtrans.c (D5).  Frontend-safe —
 *	  depends only on cluster_tt_status.h types.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SUBTRANS_H
#define CLUSTER_SUBTRANS_H

#include "c.h"
#include "access/transam.h"
#include "cluster/cluster_tt_status.h"

/*
 * cluster_subtrans_emit_subcommit (spec-3.5 D5 NEW)
 *
 *	  Called by xact.c CommitSubTransaction PGRAC MODIFICATIONS when a
 *	  savepoint commits (subxact subcommit, before parent commits).
 *
 *	  Builds child_key + parent_key via ensure_parent_binding, then
 *	  installs SUBCOMMITTED locally + emits TT_STATUS_HINT V3 to peers.
 *	  Single-node fast path:  early return if !cluster_conf_has_peers().
 *
 *	  Idempotent:  safe to call multiple times for same child_xid.
 *	  Returns true on emit, false on cluster disabled / no-peer.
 */
extern bool cluster_subtrans_emit_subcommit(TransactionId child_xid, TransactionId parent_xid);

/*
 * cluster_subtrans_emit_subabort (spec-3.5 D5 NEW)
 *
 *	  Called when savepoint aborts.  Installs ABORTED locally + emits
 *	  TT_STATUS_HINT V2 (status=ABORTED, commit_scn=InvalidScn) to peers.
 *	  No parent_key needed for ABORTED.
 *
 *	  Returns true on emit, false on cluster disabled / no-peer.
 */
extern bool cluster_subtrans_emit_subabort(TransactionId child_xid);

/*
 * cluster_subtrans_lookup_parent (spec-3.5 D5 NEW)
 *
 *	  Reader-side lazy follow.  Given a SUBCOMMITTED result, resolve
 *	  parent chain bounded by cluster.subtrans_max_chain_depth.
 *
 *	  Returns final non-SUBCOMMITTED status (COMMITTED/ABORTED/
 *	  IN_PROGRESS/UNKNOWN).  If depth exceeded:  result.status =
 *	  CLUSTER_TT_STATUS_UNKNOWN + result.authoritative=false (caller
 *	  raises 53R97 fail-closed per L199).
 *
 *	  Bumps cluster_subtrans_parent_chain_follow_count per recurse +
 *	  cluster_subtrans_chain_depth_exceeded_count if depth bound hit.
 *
 *	  Pure / no syscall / no wait (L177 hot path).
 */
extern ClusterTTStatusResult
cluster_subtrans_lookup_parent(const ClusterTTStatusResult *child_result, int depth_remaining);

/*
 * cluster_subtrans_xact_has_state (spec-3.5 D5 NEW)
 *
 *	  Returns true if the given top-level xid has installed any cluster
 *	  SUBTRANS state (subxact ACTIVE or SUBCOMMITTED overlay) in the
 *	  current epoch.  Used by twophase.c PGRAC MODIFICATIONS (D10) to
 *	  gate PREPARE TRANSACTION (HW1 P0 fail-closed guard 53R9B).
 */
extern bool cluster_subtrans_xact_has_state(TransactionId top_xid);

/*
 * cluster_subtrans_ensure_parent_binding (spec-3.5 D5 NEW)
 *
 *	  Called before installing the first child SUBTRANS state for a
 *	  top/parent xid.  Creates or finds the parent's exact TT binding
 *	  and installs IN_PROGRESS locally so V3 SUBCOMMITTED parent_key
 *	  resolves on remote readers (HC203).
 *
 *	  Raw parent xid is not a cluster identity and MUST NEVER be placed
 *	  on the wire — this helper enforces that contract by minting the
 *	  exact key from cluster_node_id + epoch + raw xid.
 *
 *	  Returns true on success and writes parent_key_out;false on
 *	  cluster disabled / no-peer (parent_key_out untouched).
 */
extern bool cluster_subtrans_ensure_parent_binding(TransactionId parent_xid,
												   ClusterTTStatusKey *parent_key_out);

/*
 * Counter getters (always linked;return 0 in disabled-cluster build).
 */
extern uint64 cluster_subtrans_get_chain_depth_exceeded_count(void);
extern uint64 cluster_subtrans_get_xact_has_state_check_count(void);

/*
 * shmem hooks (defined in cluster_subtrans.c when USE_PGRAC_CLUSTER;
 * disable-cluster stubs return 0 / no-op so the same shmem registry
 * call site in cluster_shmem.c links unconditionally).
 */
extern Size cluster_subtrans_shmem_size(void);
extern void cluster_subtrans_shmem_init(void);
extern void cluster_subtrans_shmem_register(void);

#endif /* CLUSTER_SUBTRANS_H */
