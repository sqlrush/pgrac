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
extern int cluster_ges_lmon_drain_work_queue(void);

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


/* ============================================================
 * spec-2.16 D7:  GES payload struct + opcode + REJECT_REASON.
 *
 *   Wire-ABI extension over spec-2.13 skeleton:  GES_REQUEST = 4 and
 *   GES_REPLY = 5 ICMsgType values stay unchanged (cluster_ic_envelope
 *   header unchanged).  Payload follows the 36-byte envelope:
 *
 *     [ ClusterICEnvelope 36B ][ GesRequestPayload | GesReplyPayload ]
 *
 *   The opcode field inside the payload distinguishes the operation
 *   within each msg_type family (per spec-2.16 v0.3 L1.2 decision —
 *   reuse 2 wire msg_type + payload opcode rather than 6 wire msg_type).
 *
 *   GesRequestOpcode (within GES_REQUEST):
 *     1 = REQUEST   (initial grant request)
 *     2 = CONVERT   (lockmode upgrade)
 *     3 = RELEASE   (cleanup release on abort/exit)
 *
 *   GesReplyOpcode (within GES_REPLY):
 *     1 = GRANT
 *     2 = REJECT  (reject_reason field carries why)
 *
 *   GesRejectReason (REPLY opcode 2 only):
 *     1 = WORK_QUEUE_FULL  (handler enqueue failed; spec-2.16 L1.3)
 *     2 = LOCK_CONFLICT    (incompatible mode held)
 *     3 = EPOCH_MISMATCH   (payload.epoch != local accepted_epoch)
 *     4 = TIMEOUT          (handler-side timeout; rare)
 *
 *   spec-2.16+:  Real handlers (cluster_ges_request_handler /
 *   cluster_ges_reply_handler) cast `payload` to these structs and
 *   dispatch on opcode.  Skeleton stub继续 DEFER counter bump until
 *   Step 3 D6 真激活 5 项 inbound validation + work queue enqueue.
 * ============================================================ */

typedef enum GesRequestOpcode {
	GES_REQ_OPCODE_REQUEST = 1,
	GES_REQ_OPCODE_CONVERT = 2,
	GES_REQ_OPCODE_RELEASE = 3,
	/* spec-2.17 NEW 4 opcode (Q5 v0.6) — BAST/CANCEL/DEADLOCK family */
	GES_REQ_OPCODE_BAST = 4,		   /* master → holder advisory notify */
	GES_REQ_OPCODE_BAST_ACK = 5,	   /* holder → master after natural release */
	GES_REQ_OPCODE_DEADLOCK_PROBE = 6, /* coordinator → all nodes probe req */
	GES_REQ_OPCODE_CANCEL_PENDING = 7  /* backend → master cancel pending */
} GesRequestOpcode;

typedef enum GesReplyOpcode {
	GES_REPLY_OPCODE_GRANT = 1,
	GES_REPLY_OPCODE_REJECT = 2
} GesReplyOpcode;

typedef enum GesRejectReason {
	GES_REJECT_REASON_NONE = 0, /* GRANT or undefined */
	GES_REJECT_REASON_WORK_QUEUE_FULL = 1,
	GES_REJECT_REASON_LOCK_CONFLICT = 2,
	GES_REJECT_REASON_EPOCH_MISMATCH = 3,
	GES_REJECT_REASON_TIMEOUT = 4
} GesRejectReason;

/* ClusterGrdHolderId 4-tuple typedef defined in cluster_grd.h (semantic
 * layer:  GRD entity identity).  cluster_ges.h consumers must include
 * cluster_grd.h before this header (cluster_shmem.c already does). */
struct ClusterGrdHolderId;

/*
 * GES request payload (variant on GES_REQUEST msg_type=4).
 *
 *   Layout (all little-endian on wire):
 *     [ 0,  4)  opcode          uint32 LE  (GesRequestOpcode)
 *     [ 4,  8)  lockmode        uint32 LE  (PG LOCKMODE: 1..8)
 *     [ 8, 32)  holder_id       24 bytes   (ClusterGrdHolderId)
 *     [32, 48)  resid           16 bytes   (ClusterResId)
 *
 *   Total: 48 bytes.  Aligned to 8.
 */
typedef struct GesRequestPayload {
	uint32 opcode;	 /* GesRequestOpcode */
	uint32 lockmode; /* PG LOCKMODE (AccessShareLock..AccessExclusiveLock) */
	/* 24-byte ClusterGrdHolderId inlined as 6 uint32 to avoid forward-decl
	 * size dependency.  Layout matches ClusterGrdHolderId field-for-field
	 * (StaticAssertDecl in cluster_grd.h locks both ABIs). */
	uint32 holder_node_id;
	uint32 holder_procno;
	uint32 holder_cluster_epoch_lo;
	uint32 holder_cluster_epoch_hi;
	uint32 holder_request_id_lo;
	uint32 holder_request_id_hi;
	uint32 resid[4]; /* ClusterResId byte-image (16B) */
} GesRequestPayload;

StaticAssertDecl(sizeof(GesRequestPayload) == 48, "GesRequestPayload wire ABI 48-byte lock");

/*
 * GES reply payload (variant on GES_REPLY msg_type=5).
 *
 *   Layout:
 *     [ 0,  4)  opcode          uint32 LE  (GesReplyOpcode)
 *     [ 4,  8)  reject_reason   uint32 LE  (GesRejectReason; 0 for GRANT)
 *     [ 8, 32)  holder_id       24 bytes   (echoes request)
 *     [32, 48)  resid           16 bytes   (echoes request)
 *
 *   Total: 48 bytes.  Aligned to 8.
 */
typedef struct GesReplyPayload {
	uint32 opcode;		  /* GesReplyOpcode */
	uint32 reject_reason; /* GesRejectReason; 0 if GRANT */
	/* 24-byte ClusterGrdHolderId inlined (mirror GesRequestPayload). */
	uint32 holder_node_id;
	uint32 holder_procno;
	uint32 holder_cluster_epoch_lo;
	uint32 holder_cluster_epoch_hi;
	uint32 holder_request_id_lo;
	uint32 holder_request_id_hi;
	uint32 resid[4];
} GesReplyPayload;

StaticAssertDecl(sizeof(GesReplyPayload) == 48, "GesReplyPayload wire ABI 48-byte lock");


/*
 * spec-2.21 D8 NEW:GES request/release send-and-wait helpers.
 *
 *	cluster_ges_send_request_and_wait():S4 remote master path 调用,
 *	  send GES_REQUEST(opcode=ACQUIRE)→ block 在 ConditionVariable
 *	  等 GES_REPLY(GRANT / REJECT)→ 返回 reject_reason(0=GRANT)。
 *	  timeout_ms 0 表示 dontwait(立即 ConditionalLock 语义)。
 *
 *	cluster_ges_send_release_and_wait():S6 normal release 调用,
 *	  send GES_RELEASE → bounded ACK wait(no retransmit;spec-2.23 BAST
 *	  配套补 retry/retransmit)。返回 0 = ACK OK,non-zero = timeout/error。
 *
 *	返回 0 即成功;非 0 = GesRejectReason 枚举(timeout / conflict /
 *	  deadlock_pending / cancel)。
 *
 *	stub semantics for spec-2.21:本 spec 仅 wire send call site + counter;
 *	真 send/reply pipeline ship 仍 LMS local-handle(D8 minimal grant);
 *	远端 master pipeline 推 spec-2.23 BAST 配套 ship。
 */
struct ClusterResId;
extern uint32 cluster_ges_send_request_and_wait(const struct ClusterResId *resid, uint32 lockmode,
												const struct ClusterGrdHolderId *holder,
												uint64 request_id, int timeout_ms);

extern uint32 cluster_ges_send_release_and_wait(const struct ClusterResId *resid,
												const struct ClusterGrdHolderId *holder,
												uint64 request_id);

#endif /* !FRONTEND */

#endif /* CLUSTER_GES_H */
