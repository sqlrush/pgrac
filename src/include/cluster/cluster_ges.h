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
	GES_REQ_OPCODE_CANCEL_PENDING = 7, /* backend → master cancel pending */
	GES_REQ_OPCODE_DEADLOCK_REPORT
	= 8, /* spec-2.22 D6: probed node → coordinator (read-only graph snapshot) */
	/* spec-2.25 NEW 2 opcode (Q11 v0.2) — native-lock probe protocol pair.
	 *
	 *	LMS → peer node:  NATIVE_LOCK_PROBE asks peer to scan local PG lock
	 *	state for conflict on (LOCKTAG, lockmode);  peer replies with one
	 *	of CLEAR / HOLDER_CONFLICT / WAITER_CONFLICT (see
	 *	cluster_native_lock_probe.h ClusterNativeLockProbeReply enum).
	 *
	 *	Uses dedicated payload structs (GesNativeLockProbePayload /
	 *	GesNativeLockProbeReplyPayload) — not GesRequestPayload.
	 *	Dispatched via early opcode fork in cluster_ges_request_handler
	 *	(mirrors DEADLOCK_PROBE / DEADLOCK_REPORT pattern at line 205+).
	 *	Main ges_validate_inbound opcode_max stays at CANCEL_PENDING (7);
	 *	these two opcodes use bespoke validation per HC33.
	 */
	GES_REQ_OPCODE_NATIVE_LOCK_PROBE = 9,
	GES_REQ_OPCODE_NATIVE_LOCK_PROBE_REPLY = 10,
	/*
	 * spec-2.27 D8 / HC54 — reserved opcode for future fairness escalation.
	 *
	 *	**NOT SENT / NOT RECEIVED in this spec**.  The wire opcode and
	 *	payload struct (GesPriorityBoostPayload below) are ABI-locked so
	 *	a future spec-2.28+ that ships the real PG core lock manager
	 *	`LockWaitQueueInsertAtHead`改造 can wire send + receiver in one
	 *	commit — integrated, no stub.  Wiring this opcode without an
	 *	integrated receiver would be a wire-with-stub-receiver反模式
	 *	(L107 family N+5;  spec-2.27 brainstorm Q3 catch).
	 */
	GES_REQ_OPCODE_PRIORITY_BOOST = 11,
	/*
	 * spec-4.6 D3 — cooperative holder rebind after failure-driven
	 * remaster.  Payload is the standard GesRequestPayload carrying the
	 * NEW (current-epoch) holder 4-tuple;  the wire ABI is unchanged
	 * (Q3-C).  The OLD holder identity does not ride the wire:  the
	 * master-side match key is (node_id, procno, lockmode) + resid —
	 * a backend holds at most one grant per (resid, mode), so the stale
	 * epoch/request_id components carry no extra information the master
	 * could verify.  Master semantics = insert-or-rebind:
	 *	  entry holder with same (node_id, procno, mode) → overwrite its
	 *	  identity with the new 4-tuple (unaffected-shard in-place rebind;
	 *	  idempotent for retransmits);
	 *	  no match → insert as holder (remastered-shard rebuild;  the new
	 *	  master fills holders[] from these re-declarations ONLY).
	 */
	GES_REQ_OPCODE_REDECLARE = 12,
	/*
	 * spec-4.6 P0#3 cluster gate — fire-and-forget "my local rebind
	 * barrier is complete for the current epoch" announcement.  Standard
	 * GesRequestPayload (holder.node_id = sender, epoch = current;  resid
	 * unused) — zero wire-ABI change.  The post-barrier GLOBAL stale
	 * sweep (P6) on ANY node must wait until EVERY survivor announced:
	 * the sweep removes old-epoch holders owned by REMOTE backends, so a
	 * node-local barrier alone would delete a live-but-not-yet-rebound
	 * remote holder → double grant.  No reply;  senders re-announce每
	 * tick until their own P6 gate passes (lost-message tolerance).
	 */
	GES_REQ_OPCODE_REDECLARE_DONE = 13,
	/*
	 * spec-5.3 D2 — post-commit convert backout (T4).  After a backend
	 * upgraded an existing grant in place (opcode-2 CONVERT, master mutated
	 * the holder slot to the stronger mode and rebound its request_id), a
	 * local subtransaction rollback / cancel / ERROR may need to back the
	 * upgrade out while keeping the original weaker grant.  A plain
	 * GES_RELEASE deletes the holder slot (cluster_grd_entry_release_holder),
	 * so it cannot restore the pre-convert (old_mode, old_request_id);  this
	 * dedicated opcode carries old_request_id in holder_request_id and
	 * old_mode in current_mode so the master can run the strict inverse of
	 * the convert (cluster_grd_entry_rollback_convert) — restore, not delete.
	 * Reuses the 64B GesRequestPayload (per-opcode field semantics differ;
	 * lockmode locates the upgraded slot).
	 */
	GES_REQ_OPCODE_CONVERT_ROLLBACK = 14,
	/*
	 * spec-5.5 D5 (Q11) — conditional (NOWAIT) acquire for try-locks
	 * (pg_try_advisory_lock).  Same 64B GesRequestPayload + REQUEST field
	 * semantics; the ONLY behavioural difference is at the master: a conflict
	 * is rejected immediately with GES_REJECT_REASON_LOCK_CONFLICT and does
	 * NOT enqueue a waiter or fan out a BAST (non-enqueuing conditional grant).
	 * 15 is the next free opcode (1-14 all used); the payload ABI is unchanged,
	 * so no catversion bump.  Shares the REQUEST dedup / reply-wait path so a
	 * lost reply retransmits idempotently (§3.5 T6).
	 */
	GES_REQ_OPCODE_REQUEST_NOWAIT = 15,
	/*
	 * spec-5.9 D4 — CANCEL_WAIT: a deadlock victim (or the coordinator on its
	 * behalf) tells the resource master to remove the victim's queued waiter /
	 * convert by its exact identity + spec-5.8 wait_seq, so a stale /
	 * retransmitted cancel cannot dequeue a since-reused identity (Rule 8.A
	 * P0#2).  Carries its OWN dedicated 64B payload (GesCancelWaitPayload),
	 * early-dispatched like DEADLOCK_PROBE — never the generic GesRequestPayload
	 * path, never the work_queue.  16 is the next free opcode; the new wire
	 * payload bumps catversion.
	 */
	GES_REQ_OPCODE_CANCEL_WAIT = 16,
	/*
	 * spec-5.9 D5 — CANCEL_ACK: the victim node tells the coordinator the
	 * outcome of a cross-node cancel (correlated by cancel_id), so the
	 * coordinator stops retransmitting / escalates instead of silently waiting
	 * out the timeout.  Dedicated 48B payload, early-dispatched.  17 is the next
	 * free opcode.
	 */
	GES_REQ_OPCODE_CANCEL_ACK = 17
} GesRequestOpcode;

/* spec-5.9 D4 — which queue the CANCEL_WAIT targets on the master. */
typedef enum GesCancelWaitKind {
	GES_CANCEL_WAIT_KIND_REQUEST = 0, /* waiters[]  (cancel_waiter_by_id_seq) */
	GES_CANCEL_WAIT_KIND_CONVERT = 1, /* converts[] (cancel_convert_by_id) */
} GesCancelWaitKind;

/*
 * spec-5.9 D5 — CANCEL_ACK outcome (the coordinator's pending-cancel table
 * reacts per state).  CONSUMED is the only one that clears a pending entry
 * (the victim truly matched + aborted); INSTALLED keeps it pending awaiting
 * CONSUMED; NOT_WAITING clears + re-probes; STALE_EPOCH/PROTECTED route to
 * reconfig-discard / escalation; QUEUE_BUSY triggers a backoff retransmit.
 */
typedef enum GesCancelAckStatus {
	GES_CANCEL_ACK_INSTALLED = 0,
	GES_CANCEL_ACK_CONSUMED = 1,
	GES_CANCEL_ACK_NOT_WAITING = 2,
	GES_CANCEL_ACK_STALE_EPOCH = 3,
	GES_CANCEL_ACK_QUEUE_BUSY = 4,
	GES_CANCEL_ACK_PROTECTED = 5,
} GesCancelAckStatus;

typedef enum GesReplyOpcode {
	GES_REPLY_OPCODE_GRANT = 1,
	GES_REPLY_OPCODE_REJECT = 2
} GesReplyOpcode;

typedef enum GesRejectReason {
	GES_REJECT_REASON_NONE = 0, /* GRANT or undefined */
	GES_REJECT_REASON_WORK_QUEUE_FULL = 1,
	GES_REJECT_REASON_LOCK_CONFLICT = 2,
	GES_REJECT_REASON_EPOCH_MISMATCH = 3,
	GES_REJECT_REASON_TIMEOUT = 4,
	GES_REJECT_REASON_SHARD_FROZEN = 5, /* spec-4.6 D4: shard FROZEN/REBUILDING */
	GES_REJECT_REASON_FEATURE_NOT_SUPPORTED = 6,
	/*
	 * spec-5.3 D4 — illegal lock conversion.  The opcode-2 CONVERT state
	 * machine is now a live consumer (TM table-lock S->X upgrade).  This
	 * reason is returned when the requested conversion is not a valid
	 * partial-order upgrade (LATERAL / incomparable modes) or the requester
	 * claims to hold a (node,procno,current_mode) slot the master has no
	 * record of.  The requester maps it to
	 * ERRCODE_CLUSTER_GES_ILLEGAL_LOCK_CONVERSION (53R74).
	 *
	 * GES_REJECT_REASON_FEATURE_NOT_SUPPORTED stays valid for genuinely
	 * unsupported convert sub-cases (e.g. cross-node down-convert, which
	 * remains forward-deferred to the CF/PCM block layer per spec-5.3 §3.4).
	 */
	GES_REJECT_REASON_ILLEGAL_CONVERT = 7,
	/*
	 * spec-5.7 Direction B — LOCAL-ONLY internal result (never placed on the
	 * wire, never produced by a master reply).  Returned by the requester's own
	 * REQUEST wait loop when the target master is declared CLUSTER_CSSD_PEER_DEAD
	 * *and* a second liveness reclassify proves no alive peer remains
	 * (cluster_extend_liveness_is_sole_native()).  In that state the in-flight
	 * remote REQUEST can never be answered and no peer can hold a conflicting
	 * lock, so the acquire is safe to complete on the PG-native path.  S4 maps
	 * this to CLUSTER_LOCK_ACQUIRE_OK_NATIVE (the dispatcher then cancels the S3
	 * reservation via S7 and the lock is taken natively with no cluster holder,
	 * so the later 1->0 release is also native — it never re-contacts the dead
	 * master).  Value 8 is out of the wire-used 0..7 range; the GesReplyPayload
	 * reject_reason field is unchanged (no catversion / wire ABI change).
	 */
	GES_REJECT_REASON_MASTER_DEAD_NATIVE = 8,
	/*
	 * spec-5.8 D8 — this backend was chosen as a cross-node deadlock victim by
	 * the LMD coordinator while blocked in a GES reply wait.  Generated LOCALLY
	 * (the PROCSIG_CLUSTER_GES_CANCEL handler set cluster_ges_cancel_pending);
	 * NEVER travels on the wire in a GesReplyPayload.  The GES wait loops break
	 * on the flag and return this reason; cluster_lock_acquire maps it to
	 * CLUSTER_LOCK_ACQUIRE_FAIL_DEADLOCK -> 40P01.  This is the spec-2.22 "check
	 * point (b)" (in-wait-loop victim honoring) that was forward-linked and is
	 * activated here so a confirmed cross-node deadlock actually cancels its
	 * victim instead of waiting out the finite timeout.  Value 9 (8 taken by
	 * spec-5.7's MASTER_DEAD_NATIVE on the post-5.7 rebase); also out of the
	 * wire-used 0..7 range, local-only, no wire ABI change.
	 */
	GES_REJECT_REASON_DEADLOCK_VICTIM = 9
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
 *   Total: 64 bytes (spec-5.3 D2 ABI bump from 56B — adds current_mode at
 *   offset 56; spec-2.27 D2 / HC49 had bumped 48B->56B for the
 *   shard_master_generation dedup field).  Aligned to 8.
 *
 *   PGRAC: spec-5.3 D2 extends the 56B payload to 64B WITHOUT moving any
 *   existing field: opcode/lockmode/holder/resid/shard_master_generation
 *   keep their offsets so the generic inbound decode (cluster_ges_request_
 *   handler cast at line ~400 + epoch read at offset 16-23 +
 *   ges_validate_inbound) works UNCHANGED.  The new current_mode tail field
 *   is read only by the CONVERT / CONVERT_ROLLBACK opcode arms.  Per-opcode
 *   field semantics:
 *     REQUEST/RELEASE/REDECLARE  current_mode unused (0)
 *     CONVERT(2)                 lockmode=requested_mode,
 *                                holder_request_id=convert_request_id (R_new),
 *                                current_mode=old_mode (locates the OLD slot
 *                                via the REDECLARE convention
 *                                (node,procno,current_mode)+resid)
 *     CONVERT_ROLLBACK(14)       lockmode=upgraded_mode (locates the upgraded
 *                                slot), holder_request_id=R_old (restore
 *                                target id), current_mode=old_mode (restore
 *                                target mode)
 *
 *   shard_master_generation = (cluster_epoch << 32) | lms_restart_generation
 *   composite supplied by the calling backend at REQUEST/RELEASE send time.
 *   LMS receiver uses this as part of the dedup HTAB key (HC51) so retransmit
 *   from a caller using an earlier generation is recognised as stale.
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
	/* spec-2.27 D2 / HC49 — composite shard_master_generation:
	 *   HIGH 32-bit = cluster_epoch (reconfig events)
	 *   LOW  32-bit = lms_restart_generation (LMS spawn events)
	 * caller samples cluster_lms_get_shard_master_generation() at send time. */
	uint32 shard_master_generation_lo;
	uint32 shard_master_generation_hi;
	/* spec-5.3 D2 — convert mode field at offset 56.  Semantics per opcode
	 * (see the per-opcode layout note above); 0 for non-convert opcodes. */
	uint8 current_mode;
	/* spec-5.8 D1c — waiter xid carried in the former tail padding (size-
	 * stable at 64B): the waiting backend's GetTopTransactionIdIfAny() at
	 * REQUEST / CONVERT send, read by the master to stamp the WFG waiter
	 * vertex (TX-edge resolution).  InvalidTransactionId (0) for non-waiter
	 * opcodes (RELEASE / REDECLARE) and pre-write waiters. */
	uint8 _pad0[3]; /* keep waiter_xid 4-byte aligned at offset 60 */
	uint32 waiter_xid;
	/* spec-5.8 D1e — the waiter's D1d wait-state publish sequence, sampled at
	 * REQUEST / CONVERT send so the master stamps it on the WFG waiter vertex
	 * and the cross-node cancel echoes it for D5 ABA revalidate.  0 for
	 * non-waiter opcodes. */
	uint64 wait_seq;
} GesRequestPayload;

StaticAssertDecl(sizeof(GesRequestPayload) == 72,
				 "GesRequestPayload wire ABI 72-byte lock (spec-5.3 D2 56->64; spec-5.8 D1c "
				 "waiter_xid in tail pad; spec-5.8 D1e +8 wait_seq -> 72)");

/*
 * GES reply payload (variant on GES_REPLY msg_type=5).
 *
 *   Layout (spec-2.23 D1 / FU-5 ABI bump 48B → 52B):
 *     [ 0,  4)  opcode            uint32 LE  (GesReplyOpcode)
 *     [ 4,  8)  reply_for_opcode  uint32 LE  (original GesRequestOpcode)
 *     [ 8, 12)  reject_reason     uint32 LE  (GesRejectReason; 0 for GRANT)
 *     [12, 36)  holder_id         24 bytes   (echoes request)
 *     [36, 52)  resid             16 bytes   (echoes request)
 *
 *   Total: 52 bytes.  Aligned to 4.
 *
 *	 PGRAC: spec-2.23 D1 inserts reply_for_opcode after opcode so D1's
 *	 5-tuple reply wait key (request_id, source_node, dest_node,
 *	 request_opcode, cluster_epoch) can distinguish REQUEST vs RELEASE
 *	 replies that share the same request_id slot.  HC17 invariant.
 *	 Cross-node wire ABI break — rolling upgrade already prohibited at
 *	 stage 2.x, so the break does not introduce new constraint.
 *	 catversion 202605340 (spec-2.23 D18) covers this bump.
 */
typedef struct GesReplyPayload {
	uint32 opcode;			 /* GesReplyOpcode */
	uint32 reply_for_opcode; /* original GesRequestOpcode (spec-2.23 D1 / FU-5) */
	uint32 reject_reason;	 /* GesRejectReason; 0 if GRANT */
	/* 24-byte ClusterGrdHolderId inlined (mirror GesRequestPayload). */
	uint32 holder_node_id;
	uint32 holder_procno;
	uint32 holder_cluster_epoch_lo;
	uint32 holder_cluster_epoch_hi;
	uint32 holder_request_id_lo;
	uint32 holder_request_id_hi;
	uint32 resid[4];
} GesReplyPayload;

StaticAssertDecl(sizeof(GesReplyPayload) == 52,
				 "GesReplyPayload wire ABI 52-byte lock (spec-2.23 D1 / FU-5 bump from 48B)");


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
/*
 * spec-5.6 Dc4b: wait_event lets a caller label the acquire wait (0 = the
 * default WAIT_EVENT_CLUSTER_GES_REPLY_WAIT).  CF passes
 * WAIT_EVENT_CLUSTER_CF_ENQUEUE; TM/UL pass 0.
 */
extern uint32 cluster_ges_send_request_and_wait(const struct ClusterResId *resid, uint32 lockmode,
												const struct ClusterGrdHolderId *holder,
												uint64 request_id, int timeout_ms,
												uint32 wait_event);

/*
 * spec-5.5 D5 — conditional (NOWAIT) acquire for try-locks.  Returns
 * GES_REJECT_REASON_NONE (granted) / GES_REJECT_REASON_LOCK_CONFLICT (held
 * elsewhere -> caller maps NOT_AVAIL) / other reject reasons (fail-closed).
 */
extern uint32 cluster_ges_send_request_nowait_and_wait(const struct ClusterResId *resid,
													   uint32 lockmode,
													   const struct ClusterGrdHolderId *holder,
													   uint64 request_id, int timeout_ms,
													   uint32 wait_event);

extern uint32 cluster_ges_send_release_and_wait(const struct ClusterResId *resid,
												const struct ClusterGrdHolderId *holder,
												uint64 request_id);

/*
 * spec-5.5 P0 — local-master normal-release drain.  When the resource master is
 * this node, drain + grant + WAKE queued waiters (mirror of the remote
 * GES_RELEASE handler);  release_and_drain removes the holder, so the caller
 * must NOT also call cluster_grd_release_holder_by_id on this path.
 */
extern void cluster_ges_release_and_drain_local(const struct ClusterResId *resid,
												const struct ClusterGrdHolderId *holder);

/* spec-4.6 D3 — send GES_REDECLARE (NEW current-epoch holder) to the
 * resource's current master and wait for the GRANT/REJECT ack.  Returns
 * GES_REJECT_REASON_NONE (0) on ack;  non-zero reason on reject/timeout
 * (caller leaves the old holder in place — fail-closed, no overwrite). */
extern uint32 cluster_ges_send_redeclare_and_wait(const struct ClusterResId *resid, uint32 lockmode,
												  const struct ClusterGrdHolderId *new_holder,
												  uint64 request_id);

/* spec-5.3 D2/D3 — send opcode-2 CONVERT (same-backend upgrade) to the
 * resource's master (local master goes through the in-process work queue,
 * remote master over the wire) and wait on WAIT_EVENT_GES_CONVERT_WAIT for
 * the GRANT/REJECT.  The holder carries request_id = convert_request_id
 * (R_new);  current_mode is the REDECLARE locator key for the OLD slot.
 * Returns GES_REJECT_REASON_NONE on OK_CONVERTED, ILLEGAL_CONVERT on a
 * non-partial-order conversion (→ 53R74), TIMEOUT on convert wait timeout. */
extern uint32 cluster_ges_send_convert_and_wait(const struct ClusterResId *resid,
												uint32 requested_mode, uint32 current_mode,
												const struct ClusterGrdHolderId *holder,
												uint64 convert_request_id, int timeout_ms);

/* spec-5.3 D2/D6 — send opcode-14 CONVERT_ROLLBACK (T4 post-commit backout):
 * restore the upgraded slot to (old_mode, old_request_id).  Idempotent /
 * best-effort (a no-op at the master if it never mutated);  used by the
 * acquire-path cleanup so a subxact rollback / cancel after OK_CONVERTED
 * cannot leave a false-grant.  upgraded_mode locates the slot. */
extern void cluster_ges_send_convert_rollback(const struct ClusterResId *resid,
											  uint32 upgraded_mode, uint32 old_mode,
											  const struct ClusterGrdHolderId *holder,
											  uint64 old_request_id, uint64 convert_request_id);


/* ============================================================
 * spec-2.23 D4 + D5 — targeted BAST + release-coupled BAST_ACK.
 *
 *	HC18:  cluster_ges_send_bast_targeted MUST filter the holder list
 *	through DoLockModesConflict before sending;  peer broadcast fanout
 *	is forbidden.  For local-node holders the routine signals the
 *	holder backend via SendProcSignal(PROCSIG_CLUSTER_GES_BAST);  for
 *	remote-node holders it sends a GES_REQUEST envelope with
 *	opcode=GES_REQ_OPCODE_BAST through the outbound ring.
 *
 *	HC19:  BAST_ACK is holder→master only.  spec-2.23 does NOT send a
 *	standalone BAST_ACK packet — the GES_RELEASE that carries the
 *	holder's natural release doubles as a logical BAST_ACK (Step 5 D5
 *	clears cluster_grd_bast_pending + bumps the BAST ack counter).
 *	The GES_REQ_OPCODE_BAST_ACK enum value remains reserved for the
 *	spec-2.24 retransmit / compound reliability layer (HC22).
 *
 *	Forward-declare ClusterGrdConflictHolder via header include order;
 *	cluster_grd.h must be included before cluster_ges.h at the call site.
 */
struct ClusterGrdConflictHolder;

extern void cluster_ges_send_bast_targeted(const struct ClusterResId *resid,
										   int /* LOCKMODE */ requested_mode,
										   const struct ClusterGrdConflictHolder *holders,
										   int n_holders);

/*
 * spec-2.24 D4 — cross-node victim cancel forwarding (HC23/HC24).
 *
 *	LMD coordinator (cluster_lmd_tarjan_run_coordinator_scan D3 wire)
 *	calls this when cross-node deadlock cycle victim resides on a
 *	remote node.  Routes via CLUSTER_GRD_OUTBOUND_LMD_CANCEL origin
 *	(reserved pool + cleanup dirty-list nofail).
 */
extern void cluster_ges_send_cancel_pending(int32 victim_node_id,
											const struct ClusterGrdHolderId *victim_target,
											uint64 wait_seq, uint64 cancel_id);

/*
 * spec-5.9 D4 — tell a REMOTE resource master to dequeue the victim's queued
 * waiter (kind == GES_CANCEL_WAIT_KIND_REQUEST) or convert (..._CONVERT) by its
 * exact identity + wait_seq.  Reliable outbound origin (loss => leaked WFG
 * edge).  Local-master victims call the GRD primitives directly.
 */
extern void cluster_ges_send_cancel_wait(int32 master_node_id, const struct ClusterResId *resid,
										 const struct ClusterGrdHolderId *waiter, uint64 wait_seq,
										 uint64 cancel_id, uint8 kind);

/*
 * spec-5.9 D5 — report a cross-node cancel outcome back to the coordinator
 * (ack_status is a GesCancelAckStatus).  Reliable outbound origin.
 */
extern void cluster_ges_send_cancel_ack(int32 coordinator_node_id,
										const struct ClusterGrdHolderId *victim, uint64 wait_seq,
										uint64 cancel_id, uint8 ack_status);


/* ============================================================
 * spec-2.22 D6 — DEADLOCK_PROBE / DEADLOCK_REPORT payload format.
 *
 *	Production cross-node broadcast/collection 推 spec-2.23 BAST 配套;
 *	本 spec ships:
 *	  - payload struct definitions (wire ABI lock via StaticAssertDecl)
 *	  - handler scaffold (PROBE → snapshot_copy → REPORT encode prep)
 *	  - opcode = 6 (PROBE) / 8 (REPORT)
 * ============================================================ */

typedef struct GesDeadlockProbePayload {
	uint32 opcode; /* = GES_REQ_OPCODE_DEADLOCK_PROBE (6) */
	uint32 coordinator_node_id;
	uint64 probe_id;
	uint64 generation_snapshot; /* coordinator's graph gen at probe time */
} GesDeadlockProbePayload;

StaticAssertDecl(sizeof(GesDeadlockProbePayload) == 24,
				 "GesDeadlockProbePayload wire ABI 24-byte lock");

/*
 * spec-5.9 D4 — CANCEL_WAIT dedicated payload (64B, fits the 72B outbound ring
 * slot so no ring growth).  waiter_* is the victim's identity 4-tuple; the
 * master matches it AND wait_seq exactly (ABA guard).  For kind == CONVERT,
 * waiter_request_id carries the convert_request_id.  cancel_id correlates the
 * spec-5.9 D5 CANCEL_ACK.
 */
typedef struct GesCancelWaitPayload {
	uint32 opcode; /* = GES_REQ_OPCODE_CANCEL_WAIT (16) */
	uint32 kind;   /* GesCancelWaitKind */
	uint32 waiter_node_id;
	uint32 waiter_procno;
	uint64 waiter_cluster_epoch;
	uint64 waiter_request_id; /* request_id, or convert_request_id (CONVERT) */
	uint64 wait_seq;		  /* spec-5.8 wait_seq — exact match */
	uint64 cancel_id;		  /* coordinator correlation id */
	uint32 resid[4];		  /* ClusterResId byte image (16B) */
} GesCancelWaitPayload;

StaticAssertDecl(sizeof(GesCancelWaitPayload) == 64, "GesCancelWaitPayload wire ABI 64-byte lock");

/*
 * spec-5.9 D5 — CANCEL_ACK dedicated payload (48B).  victim_* + wait_seq +
 * cancel_id echo the cancel the coordinator issued so its pending-cancel table
 * can correlate the outcome; ack_status is a GesCancelAckStatus.
 */
typedef struct GesCancelAckPayload {
	uint32 opcode;	   /* = GES_REQ_OPCODE_CANCEL_ACK (17) */
	uint32 ack_status; /* GesCancelAckStatus */
	uint32 victim_node_id;
	uint32 victim_procno;
	uint64 victim_cluster_epoch;
	uint64 victim_request_id;
	uint64 wait_seq;
	uint64 cancel_id;
} GesCancelAckPayload;

StaticAssertDecl(sizeof(GesCancelAckPayload) == 48, "GesCancelAckPayload wire ABI 48-byte lock");

/*
 * Header for variable-length REPORT.  Followed by nedges *
 * sizeof(ClusterLmdWaitEdge) (96 bytes each) when nedges > 0.
 *
 *	HC15 read-only:  REPORT body MUST NOT mutate remote LMD state.
 *	Handler scaffolding only collects own graph snapshot and prepares
 *	REPORT for send.  Production send path 推 spec-2.23.
 */
typedef struct GesDeadlockReportHeader {
	uint32 opcode; /* = GES_REQ_OPCODE_DEADLOCK_REPORT (8) */
	uint32 responding_node_id;
	uint64 probe_id; /* echo from PROBE */
	uint64 graph_generation;
	uint32 lmd_ready_state; /* mirror cluster_lmd_state enum */
	uint32 nedges;
	/* followed by nedges * ClusterLmdWaitEdge (96B each) */
} GesDeadlockReportHeader;

StaticAssertDecl(sizeof(GesDeadlockReportHeader) == 32,
				 "GesDeadlockReportHeader wire ABI 32-byte lock");

/*
 * Handler scaffold — receives a PROBE payload, prepares a REPORT.
 *
 *	Returns 0 on success, non-zero on dispatch error.  Production send
 *	uses the routed envelope (spec-2.13 ship);本 spec scope handler 内部
 *	仅 snapshot_copy + encode REPORT 准备 send,实际 send 推 spec-2.23.
 *
 *	out_buf:  caller-provided buffer (header + edges);out_buflen 输入是
 *	  buf 容量,输出是实际 encode 字节数.
 */
extern int cluster_ges_deadlock_probe_handler(const GesDeadlockProbePayload *probe, void *out_buf,
											  Size *inout_buflen);

/* ============================================================
 * spec-2.25 D6:  Native-lock probe protocol payload structs
 *
 *	NATIVE_LOCK_PROBE asks a peer node to scan its local PG lock
 *	state for conflicts on (LOCKTAG, lockmode);  peer replies with
 *	one of CLEAR / HOLDER_CONFLICT / WAITER_CONFLICT.  Used by LMS
 *	to satisfy the per-node native-lock reverse-check before
 *	granting a cluster lock (HC30..HC32a).
 *
 *	Both payloads are fixed 32 bytes to keep the GES wire ABI
 *	multi-opcode alignment uniform with GesDeadlockProbePayload
 *	(24B) and GesDeadlockReportHeader (32B).
 * ============================================================ */

typedef struct GesNativeLockProbePayload {
	uint32 opcode;			 /* [0,4)   = GES_REQ_OPCODE_NATIVE_LOCK_PROBE (9) */
	uint32 lockmode;		 /* [4,8)   PG LOCKMODE (1..8) */
	uint8 locktag_bytes[16]; /* [8,24)  LOCKTAG byte-image (16B serialized) */
	uint64 probe_id;		 /* [24,32) monotonic per-shard collector slot id */
	/* spec-5.3 — the ORIGINAL requester's (node_id, procno).  The peer scan
	 * must skip the requester's own holder (HC32a self-exclusion); without
	 * this the peer fell back to env->source_node_id (the LMS master, NOT the
	 * requester), so a requester on a NON-master peer could not exclude its own
	 * lock and self-conflicted — invisible to REQUEST (a fresh requester holds
	 * no conflicting lock; Share+Share is compatible) but fatal to CONVERT
	 * (the requester holds a weaker lock that conflicts with the upgrade). */
	uint32 requester_node_id; /* [32,36) original requester node */
	uint32 requester_procno;  /* [36,40) original requester PGPROC index */
} GesNativeLockProbePayload;

StaticAssertDecl(sizeof(GesNativeLockProbePayload) == 40,
				 "GesNativeLockProbePayload wire ABI 40-byte lock (spec-5.3: +requester id)");

typedef struct GesNativeLockProbeReplyPayload {
	uint32 opcode;		   /* = GES_REQ_OPCODE_NATIVE_LOCK_PROBE_REPLY (10) */
	uint32 status;		   /* ClusterNativeLockProbeReply enum value */
	uint64 probe_id;	   /* echoes request (collector slot correlation) */
	uint32 sender_node_id; /* node that performed the local scan (HC33 dual-check) */
	uint8 reserved[12];	   /* pad to 32B */
} GesNativeLockProbeReplyPayload;

StaticAssertDecl(sizeof(GesNativeLockProbeReplyPayload) == 32,
				 "GesNativeLockProbeReplyPayload wire ABI 32-byte lock");

/* ============================================================
 * spec-2.27 D8 / HC54 — RESERVED priority boost payload.
 *
 *	GES_REQ_OPCODE_PRIORITY_BOOST = 11 is reserved for future fairness
 *	escalation;  the wire ABI is locked here so a future spec-2.28+ can
 *	ship the integrated PG core lock manager改造 + send + receiver in a
 *	single commit (no wire-with-stub-receiver反模式 — L107 family N+5).
 *
 *	**NOT SENT / NOT RECEIVED by spec-2.27 code paths**.  Any future
 *	caller of cluster_grd_outbound_enqueue_* with opcode = 11 must come
 *	in the same commit as a non-stub receiver registered with
 *	`cluster_ges_request_handler`.
 * ============================================================ */

typedef struct GesPriorityBoostPayload {
	uint32 opcode;	 /* = GES_REQ_OPCODE_PRIORITY_BOOST (11) */
	uint32 lockmode; /* PG LOCKMODE the requester is waiting on */
	uint64 request_id;
	uint64 cluster_epoch;
	uint64 shard_master_generation;
} GesPriorityBoostPayload;

StaticAssertDecl(
	sizeof(GesPriorityBoostPayload) == 32,
	"GesPriorityBoostPayload wire ABI 32-byte lock (RESERVED — NOT SENT in spec-2.27)");

/*
 * spec-2.25 D6 / Step 1 — native-lock probe handler entry points.  Wire
 * dispatch from cluster_ges_request_handler;  body activation lands at
 * Step 6 (D5 — request body scans local PG lock state via D8 helper;
 * reply body feeds LMS collector via D4 cluster_lms_native_probe_recv_reply).
 */
extern void cluster_ges_handle_native_lock_probe_request(const ClusterICEnvelope *env,
														 const GesNativeLockProbePayload *probe);
extern void cluster_ges_handle_native_lock_probe_reply(const ClusterICEnvelope *env,
													   const GesNativeLockProbeReplyPayload *reply);

#endif /* !FRONTEND */

#endif /* CLUSTER_GES_H */
