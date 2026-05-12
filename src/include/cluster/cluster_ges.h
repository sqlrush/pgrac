/*-------------------------------------------------------------------------
 *
 * cluster_ges.h
 *	  GES (Global Enqueue Service) request protocol skeleton — spec-2.13.
 *
 *	  This module is the protocol-layer entry point for cross-instance
 *	  non-block lock coordination (TX / TM / SQ / CF / UL / etc per
 *	  AD-002 PCM vs GES 分工).  Skeleton phase (spec-2.13 ship) provides
 *	  ONLY:
 *	    - 2 ICMsgType handler stubs (GES_REQUEST=4, GES_REPLY=5) that
 *	      atomically increment a defer counter and return without state
 *	      change.  Caller MUST fall back to PG-native lock manager when
 *	      the skeleton stub fires (future spec-2.16+ caller contract).
 *	    - 2 atomic uint64 defer counters in ClusterGesSharedState.
 *	    - 2 read accessors used by cluster_debug emit_row surface.
 *	    - cluster_ges shmem region lifecycle (size / init / register).
 *
 *	  Skeleton is INTENTIONALLY incomplete — real GES granting / convert
 *	  queue / cross-node routing / deadlock detection / DRM land in:
 *	    - spec-2.14: GES resource identity + GRD shard table (hash
 *	                 routing, 4096 shard, single master init)
 *	    - spec-2.15: lock mode compatibility + local grant table
 *	                 (PG 8 mode + Oracle 6 mode 映射, per-shard hash)
 *	    - spec-2.16: cross-node grant/convert protocol (skeleton DEFER
 *	                 → real routing + reply wire round-trip)
 *	    - spec-2.17: deadlock detection (LMD daemon cross-node wait-for
 *	                 graph)
 *	    - Stage 6: DRM (dynamic mastering, affinity-based remaster)
 *
 *	  AD-002 PCM vs GES 分工:  GES owns NON-block locks; buffer-cache
 *	  block-level coordination goes via PCM protocol (spec-3.X真激活).
 *
 *	  AD-011 不移植 LC/RC Lock:  PG has no SGA shared pool范式;
 *	  Library Cache / Row Cache Lock are NOT migrated.
 *
 *	  Spec: spec-2.13-ges-request-protocol-skeleton.md (frozen v0.2)
 *	  Design: docs/ges-lock-protocol-design.md v1.0
 *	  AD: AD-002 / AD-011
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_ges.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  All symbols are backend-only (#ifndef FRONTEND) to prevent
 *	  frontend tools (pg_waldump / pg_dump / pg_resetwal) from
 *	  accidentally pulling in cluster_ges_state references via indirect
 *	  include (L8 inheritance + spec-2.11 P2 pattern).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GES_H
#define CLUSTER_GES_H

#ifndef FRONTEND

#include "port/atomics.h"
#include "cluster/cluster_ic_envelope.h"

/*
 * ClusterGesSharedState -- spec-2.13 D2 skeleton shmem.
 *
 *	Lives in dedicated shmem region "pgrac cluster ges" (Q3.1=B —
 *	NOT追加 to cluster_scn_state).  Subsystem边界 clean: GES (lock
 *	coordination) vs SCN (Lamport global counter) are orthogonal.
 *	Future spec-2.14+ extends this struct with GRD shard table, grant
 *	table, convert queue, deadlock graph, etc.
 *
 *	Skeleton fields (spec-2.13):
 *	  - request_defer_count:  bumped on every GES_REQUEST handler call
 *	  - reply_defer_count:    bumped on every GES_REPLY handler call
 *
 *	Both are pg_atomic_uint64 — handlers run lock-free (Q4.1=A; no
 *	LWLock acquire on hot path; L106 inherit).
 */
typedef struct ClusterGesSharedState {
	pg_atomic_uint64 request_defer_count;
	pg_atomic_uint64 reply_defer_count;
	/* Future spec-2.14+ adds: GRD shard table, hash routing state,
	 * grant table, convert queue, deadlock graph, etc. */
} ClusterGesSharedState;

/*
 * Shmem region lifecycle (mirror cluster_scn / cluster_lmon pattern).
 *
 *	Postmaster wiring:
 *	  - cluster_shmem.c calls cluster_ges_shmem_register() at startup
 *	    to declare the region (size_fn + init_fn) into the cluster
 *	    shmem framework.
 *	  - Framework subsequently calls cluster_ges_shmem_init() at the
 *	    right phase to allocate + zero-init the buffer.
 */
extern Size cluster_ges_shmem_size(void);
extern void cluster_ges_shmem_init(void);
extern void cluster_ges_shmem_register(void);

/*
 * GES request/reply handler stubs -- spec-2.13 D2.
 *
 *	Signature aligned with ClusterICMsgTypeInfo.handler typedef in
 *	cluster_ic_router.h:111:
 *	  void (*handler)(const ClusterICEnvelope *env, const void *payload)
 *
 *	Contract (skeleton phase):
 *	  - Never crash, never ERROR/FATAL (handler 4 硬约束 per spec-2.3
 *	    Q6: no block / no LWLock wait / no catalog SQL / no error).
 *	  - Atomically increment {request,reply}_defer_count.
 *	  - Log DEBUG2 only (do NOT INFO / NOTICE / WARNING — would spam
 *	    production logs once spec-2.14+ caller-side is活).
 *	  - Return without state change to caller.
 *
 *	Caller contract (spec-2.16+ when reply path is real):
 *	  When caller sees a reply marked DEFER (skeleton phase: implicit
 *	  via reply_defer_count bump + no state grant), it MUST fall back
 *	  to PG-native lock manager.  Treating DEFER as "resource not
 *	  granted" is a violation.
 *
 *	Future spec-2.14+:
 *	  Real granted / waiting / converting paths replace the永远-DEFER
 *	  stub; counters get split by state (GRANTED / WAITING / etc).
 */
extern void cluster_ges_request_handler(const ClusterICEnvelope *env, const void *payload);
extern void cluster_ges_reply_handler(const ClusterICEnvelope *env, const void *payload);

/*
 * Counter accessors -- spec-2.13 D4.
 *
 *	Used by cluster_debug emit_row to surface counters in
 *	pg_cluster_state (category='ges', SQL keys ges_request_defer_count
 *	and ges_reply_defer_count).  Mirror cluster_scn counter accessor
 *	pattern (pg_atomic_read_u64 wrapper + Assert on state pointer).
 */
extern uint64 cluster_ges_request_defer_count(void);
extern uint64 cluster_ges_reply_defer_count(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_GES_H */
