/*-------------------------------------------------------------------------
 *
 * cluster_grd_pending.h
 *	  Pending GES request table — spec-2.16 D3.
 *
 *	  Per-backend pending request tracker keyed by 4-tuple
 *	  (node_id, cluster_epoch, procno, request_id).  Backends enqueue
 *	  a pending entry before sending GES_REQUEST to the outbound queue
 *	  (Step 4 D9);  the LMON-driven reply path (Step 3 D6) looks up the
 *	  pending entry by 4-tuple key and signals the backend latch with
 *	  the GRANT / REJECT verdict.
 *
 *	  Skeleton phase (Step 1):  typedef + struct + extern only;
 *	  HTAB init + state machine activation lands Step 2-3.
 *
 *	  Spec: spec-2.16-cross-node-grant-convert-mvp.md (DRAFT v0.1)
 *	  Design: docs/ges-lock-protocol-design.md v1.0
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_grd_pending.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  All symbols are backend-only (#ifndef FRONTEND) per L8 inheritance.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GRD_PENDING_H
#define CLUSTER_GRD_PENDING_H

#ifndef FRONTEND

#include "port/atomics.h"
#include "cluster/cluster_ges.h" /* ClusterGrdHolderId 4-tuple */

/*
 * Pending request lifecycle state (spec-2.16 v0.3 L1.4 — handler
 * cannot directly grant;  pending state tracks the round-trip).
 *
 *   ENQUEUED:   backend put into outbound;  reply not yet received
 *   GRANTED:    reply opcode==GRANT;  backend latch signaled
 *   REJECTED:   reply opcode==REJECT (any reject_reason);  signaled
 *   TIMEOUT:    cluster.ges_request_timeout_ms expired;  no reply
 *   CANCELED:   backend abort path called pending_release before reply
 *               (spec-2.17 真激活;  Step 1-7 skeleton only)
 */
typedef enum ClusterGrdPendingState {
	CLUSTER_GRD_PENDING_ENQUEUED = 1,
	CLUSTER_GRD_PENDING_GRANTED = 2,
	CLUSTER_GRD_PENDING_REJECTED = 3,
	CLUSTER_GRD_PENDING_TIMEOUT = 4,
	CLUSTER_GRD_PENDING_CANCELED = 5
} ClusterGrdPendingState;

/*
 * Pending HTAB key — 4-tuple ClusterGrdHolderId.
 *
 *   PG dynahash HASHCTL.keysize = sizeof(ClusterGrdHolderId) = 24.
 *   tag_hash_uint32 hash composes (node_id, procno, cluster_epoch_lo,
 *   cluster_epoch_hi, request_id_lo, request_id_hi) — uint32-grain.
 *
 *   spec-2.16 Step 3 D6:  reply handler probes by 4-tuple → finds
 *   pending entry → atomic state CAS → SetLatch backend.
 */
typedef ClusterGrdHolderId ClusterGrdPendingKey;

/* Skeleton:  shmem size + init + register only (HTAB allocation
 * gated on GUC cluster.grd_max_entries == cluster.ges_pending_max
 * indirectly per Step 5 D12 GUC introduction).  Step 1 stub returns
 * 0 size when GUC unwired. */
extern Size cluster_grd_pending_shmem_size(void);
extern void cluster_grd_pending_shmem_init(void);
extern void cluster_grd_pending_shmem_register(void);

/*
 * Pending table API (Step 2-3 真激活).  Skeleton stubs in Step 1.
 *
 *   pending_register:    backend pre-send;  inserts ENQUEUED state
 *   pending_signal:      LMON reply path;  CAS state + SetLatch
 *   pending_release:     backend post-wait;  HASH_REMOVE
 *   pending_lookup_state: backend latch wake;  fetch state
 *   pending_count:       observability (total live entries)
 */
extern void cluster_grd_pending_register(const ClusterGrdPendingKey *key);
extern void cluster_grd_pending_signal(const ClusterGrdPendingKey *key,
									   ClusterGrdPendingState verdict);
extern void cluster_grd_pending_release(const ClusterGrdPendingKey *key);
extern ClusterGrdPendingState
cluster_grd_pending_lookup_state(const ClusterGrdPendingKey *key);
extern uint64 cluster_grd_pending_count(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_GRD_PENDING_H */
