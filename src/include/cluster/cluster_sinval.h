/*-------------------------------------------------------------------------
 *
 * cluster_sinval.h
 *	  pgrac cluster SI Broadcaster — cross-node sinval message propagation.
 *
 *	  spec-2.38 真激活 PGRAC_IC_MSG_SINVAL = 7 wire msg type + B_SINVAL_BCAST
 *	  BackendType + 3 wait events (all占位至 spec-2.38 起 wire-up real).
 *	  Skeleton MVP scope:
 *	    - aux process spawn + main loop (drain inbound queue →
 *	      SIInsertDataEntries; fail-safe SIResetAll on inbound overflow)
 *	    - LMON drains outbound queue → IC fanout, because tier1 TCP fds are
 *	      LMON process-local
 *	    - wire ABI: SinvalBroadcastHeader 24B fixed + variable-length tail
 *	      of N × SharedInvalidationMessage (PG-native 16B union)
 *	    - public API cluster_sinval_enqueue_batch() — only entry point for
 *	      outbound broadcast; spec-2.39 接 DDL commit hook 时 production caller
 *	    - IC handler: validate-only path (checksum L164 + epoch HC100 +
 *	      source_node HC135) + nonblocking inbound try-enqueue + SetLatch
 *	    - 2 NEW shmem regions: ClusterSinvalOutbound + ClusterSinvalInbound
 *	    - 1 NEW SQLSTATE: 53R94 ERRCODE_CLUSTER_SINVAL_QUEUE_FULL
 *
 *	  HC contracts in this header (HC132-HC139 8 NEW):
 *	    HC132 outbound queue 独立(防 echo loop;唯一硬防线)— proxy 永不
 *	          读 PG SI queue
 *	    HC133 IC inbound handler nonblocking 约束 — handler 只用
 *	          LWLockConditionalAcquire,不调 SIInsertDataEntries
 *	    HC134 enqueue_batch fail-closed (return bool);inbound overflow
 *	          fail-safe SIResetAll()
 *	    HC135 source_node 辅助 echo defense (envelope-level only;不可替代
 *	          HC132 硬防线)
 *	    HC136 split drain ownership (Hardening v1.0.1): LMON drains
 *	          outbound queue + fanouts wire envelopes (tier1 fd LMON-only);
 *	          SinvalBcast aux process drains inbound queue + applies
 *	          SendSharedInvalidMessages locally + executes fail-safe
 *	          SIResetAll() on overflow.  See L172.
 *	    HC137 StaticAssertDecl(SharedInvalidationMessage == 16) 锁 PG ABI
 *	    HC138 wire ABI variable-length tail (reuse spec-2.4 chunked framing)
 *	    HC139 producer mask = CLUSTER_IC_PRODUCER_SINVAL_FANOUT (LMON) only;
 *	          backend / SinvalBcast 不可 bypass outbound queue 直发 SINVAL
 *	          wire
 *
 *	  Wire layout (envelope payload):
 *	    [  0,  24)  SinvalBroadcastHeader (HC138 fixed-size prefix)
 *	    [ 24, 24 + 16 * nmsgs)  N × SharedInvalidationMessage tail
 *	  envelope.payload_length = 24 + 16 * nmsgs;  checksum lives in
 *	  ClusterICEnvelope.payload_crc32c (covers full payload, L164).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_sinval.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-2.38-si-broadcaster-skeleton.md (FROZEN v0.3)
 *	  Design: docs/cache-fusion-protocol-design.md (SI Broadcast subsystem) +
 *	          docs/background-process-design.md §3.6.3
 *	  AD-011 (LC/RC Lock 废弃) — 本 spec 是消息传播承担者
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SINVAL_H
#define CLUSTER_SINVAL_H

#include "c.h"
#include "miscadmin.h"		 /* B_SINVAL_BCAST */
#include "storage/sinval.h"	 /* SharedInvalidationMessage */
#include "access/xlogdefs.h" /* XLogRecPtr */

#ifdef USE_PGRAC_CLUSTER

/*
 * HC137:  lock PG SharedInvalidationMessage ABI boundary.  If PG upgrade
 * changes union sizeof, assertion fires → spec amend required (wire ABI
 * carries raw 16B bytes; size drift would silently corrupt cross-node
 * sinval propagation).
 */
StaticAssertDecl(sizeof(SharedInvalidationMessage) == 16,
				 "spec-2.38 D1 HC137 wire ABI assumes PG SharedInvalidationMessage == 16B; "
				 "amend wire ABI + StaticAssertDecl on PG upgrade if union sizeof drifts");

/*
 * Upper bound for sinval messages per broadcast envelope.  Together with
 * sizeof(SinvalBroadcastHeader) == 24 this caps wire payload at
 * 24 + 64 * 16 = 1048B per envelope (well below PGRAC_IC_PAYLOAD_MAX).
 *
 * cluster.sinval_broadcast_batch_size GUC has a check hook enforcing
 * 1 <= value <= CLUSTER_SINVAL_BATCH_MAX to prevent runtime misconfigure.
 */
#define CLUSTER_SINVAL_BATCH_MAX 64

/*
 * SinvalBroadcastHeader -- wire ABI prefix for PGRAC_IC_MSG_SINVAL envelope.
 *
 *   Layout (24B fixed; HC138 variable-length tail follows):
 *     [  0,   8) batch_id      -- monotone batch id assigned at enqueue time
 *     [  8,  16) epoch         -- HC100 stale-reply 校验 (cluster_epoch)
 *     [ 16,  20) source_node   -- HC135 echo defense (sender cluster_node_id)
 *     [ 20,  22) nmsgs         -- actual msg count in tail (1..CLUSTER_SINVAL_BATCH_MAX)
 *     [ 22,  24) flags         -- reserved for future use (must be 0)
 *
 *   Followed by `nmsgs × SharedInvalidationMessage` (each 16B per HC137).
 *   Envelope.payload_length = 24 + 16 * nmsgs;  checksum lives in
 *   ClusterICEnvelope.payload_crc32c (envelope covers full payload).
 */
typedef struct SinvalBroadcastHeader {
	uint64 batch_id;   /*  8B [  0,   8) */
	uint64 epoch;	   /*  8B [  8,  16) HC100 */
	int32 source_node; /*  4B [ 16,  20) HC135 envelope-level echo defense */
	uint16 nmsgs;	   /*  2B [ 20,  22) tail message count */
	uint16 flags;	   /*  2B [ 22,  24) reserved (0) */
} SinvalBroadcastHeader;

StaticAssertDecl(sizeof(SinvalBroadcastHeader) == 24,
				 "spec-2.38 D1 SinvalBroadcastHeader wire ABI 24B fixed; HC138 "
				 "variable-length tail nmsgs × SharedInvalidationMessage (16B each) follows");

/* ============================================================
 * HC139:  producer mask — only LMON may send PGRAC_IC_MSG_SINVAL wire
 *         envelopes because it owns tier1 TCP fds.  Backends MUST enqueue
 *         into ClusterSinvalOutbound via cluster_sinval_enqueue_batch();
 *         SinvalBcast only applies inbound messages locally.
 * ============================================================ */
#define CLUSTER_IC_PRODUCER_SINVAL_FANOUT ((uint32)(1u << B_LMON))

/* ============================================================
 * spec-2.39 D4 — SinvalAckHeader wire ABI (PGRAC_IC_MSG_SINVAL_ACK=19).
 *
 *   Layout (24B fixed; no tail):
 *     [  0,   8) batch_id      -- echo of the batch_id from the original
 *                                 SinvalBroadcastHeader being ACK'd
 *     [  8,  16) epoch         -- HC100 stale-reply 校验 (cluster_epoch)
 *     [ 16,  20) acker_node    -- sender (the peer applying SI locally)
 *     [ 20,  22) status        -- ClusterSinvalAckStatus enum
 *     [ 22,  24) flags         -- reserved (must be 0)
 *
 *   envelope.payload_length = 24.  Envelope CRC32C covers all 24 bytes
 *   (Hardening v1.0.1 L164 extension).
 *
 *   HC140 (NEW):  ack envelope wire ABI invariants:
 *     - sizeof(SinvalAckHeader) == 24 锁 (StaticAssertDecl)
 *     - status MUST be valid ClusterSinvalAckStatus value (0/1/2)
 *     - flags MUST be 0 (reserved for future)
 *     - acker_node MUST be in [0, CLUSTER_MAX_NODES)
 *     - epoch MUST match current cluster_epoch (HC100)
 * ============================================================ */
typedef struct SinvalAckHeader {
	uint64 batch_id;  /*  8B [  0,   8) */
	uint64 epoch;	  /*  8B [  8,  16) HC100 */
	int32 acker_node; /*  4B [ 16,  20) sender peer id */
	uint16 status;	  /*  2B [ 20,  22) ClusterSinvalAckStatus */
	uint16 flags;	  /*  2B [ 22,  24) reserved (0) */
} SinvalAckHeader;

StaticAssertDecl(sizeof(SinvalAckHeader) == 24,
				 "spec-2.39 D4 SinvalAckHeader wire ABI 24B fixed; HC140");

/*
 * ClusterSinvalAckStatus -- v0.3 P2 显式三态.
 *
 *   SINVAL_ACK_DONE          -- peer cluster_sinval_inbound_try_enqueue OK;
 *                               sender ack_received_mask bit-set fulfilled.
 *   SINVAL_ACK_DROPPED       -- peer validation 失败 (echo defense / epoch
 *                               stale / source_node bound 等);sender 不计
 *                               fulfilled (normally not emitted on wire —
 *                               validation drop 在 ack send 之前;included
 *                               for wire ABI completeness + future extension).
 *   SINVAL_ACK_RESET_PENDING -- peer inbound queue full → 触发 fail-safe
 *                               SIResetAll() path;sender 视为 fulfilled
 *                               (远端 catalog 通过 RESET-all 兜底).
 *
 *   Sender bit-set fulfilled rule (HC141 NEW):
 *     DONE | RESET_PENDING → bit-set;DROPPED → 不 bit-set (走 timeout).
 */
typedef enum ClusterSinvalAckStatus {
	SINVAL_ACK_DONE = 0,
	SINVAL_ACK_DROPPED = 1,
	SINVAL_ACK_RESET_PENDING = 2
} ClusterSinvalAckStatus;

/* HC139 extension:  SINVAL_ACK msg_type producer mask.  L172 LMON-only. */
#define CLUSTER_IC_PRODUCER_SINVAL_ACK ((uint32)(1u << B_LMON))

/* ============================================================
 * spec-2.39 D7 + v0.3 P1 — SinvalBroadcastHeader.flags bits.
 *
 *   SINVAL_REQUIRES_ACK         -- enqueuer 等 ACK (peer_enqueued barrier).
 *                                  When set, every receiving peer MUST
 *                                  emit a PGRAC_IC_MSG_SINVAL_ACK envelope.
 *   SINVAL_RESET_ALL_BROADCAST  -- (v0.3 P1) outbound queue full fallback
 *                                  sentinel.  Remote handler 见此 flag →
 *                                  直接调 SIResetAll() + bump inbound_
 *                                  overflow_reset_count + 不 enqueue inbound
 *                                  + 不发 ACK (REQUIRES_ACK 与此 flag 互斥;
 *                                  wire ABI 强制).  本 sentinel batch
 *                                  nmsgs MUST == 0,payload_length MUST
 *                                  == sizeof(SinvalBroadcastHeader) = 24.
 *
 *   HC142 (NEW):  SINVAL_KNOWN_FLAGS 必须包含所有 known flag bits;
 *                 hdr->flags & ~SINVAL_KNOWN_FLAGS != 0 → drop (HC135 extension).
 * ============================================================ */
#define SINVAL_REQUIRES_ACK ((uint16)0x0001)
#define SINVAL_RESET_ALL_BROADCAST ((uint16)0x0002)
#define SINVAL_KNOWN_FLAGS (SINVAL_REQUIRES_ACK | SINVAL_RESET_ALL_BROADCAST)


/* ============================================================
 * Public API.
 * ============================================================ */

/*
 * cluster_sinval_enqueue_batch -- enqueue a batch of SharedInvalidation-
 * Message into the outbound queue for cross-node broadcast.
 *
 *   Returns true if all N messages were successfully enqueued.  Returns
 *   false on queue full (HC134 fail-closed);  caller is responsible for
 *   handling failure:
 *     (a) ereport(ERROR, ERRCODE_CLUSTER_SINVAL_QUEUE_FULL = 53R94)  —
 *         abort current DDL/transaction;  fallback for spec-2.39 production
 *         DDL hook path
 *     (b) fallback local SIResetAll() + future RESET-all broadcast — coarse
 *         but safe;  spec-2.39+ option
 *     (c) caller log + continue — only acceptable for test inject path;
 *         forbidden in production paths
 *
 *   spec-2.38 MVP scope:  this API is the ONLY entry point for outbound
 *   broadcast.  PG DDL commit hook (AtEOXact_Inval) is NOT wired in this
 *   spec — spec-2.39 will接 production caller + ack/barrier semantics.
 */
extern bool cluster_sinval_enqueue_batch(const SharedInvalidationMessage *msgs, int n);

/*
 * cluster_sinval_inbound_try_enqueue -- HC133 nonblocking inbound enqueue
 * used by the IC inbound handler.  Uses LWLockConditionalAcquire only;
 * returns false if lock is busy OR ring full.  Caller (IC handler) sets
 * inbound_overflow_reset_pending flag on false and lets SI Broadcaster
 * aux process apply SIResetAll() as fail-safe (HC134).
 */
extern bool cluster_sinval_inbound_try_enqueue(uint64 batch_id,
											   const SharedInvalidationMessage *msgs, int n,
											   int32 source_node);

/*
 * Drain helpers.
 *
 *   cluster_sinval_drain_outbound_and_broadcast is LMON-only because only
 *   LMON owns tier1 TCP fds and may call cluster_ic_send_envelope_fanout().
 *   The SI Broadcaster aux process owns inbound apply + reset only.
 *
 *   IC inbound handler MUST NOT call these directly.
 */
extern void cluster_sinval_drain_outbound_and_broadcast(void);
extern void cluster_sinval_drain_inbound_and_apply(void);
extern void cluster_sinval_apply_inbound_overflow_reset_if_pending(void);

/*
 * Shmem region size / init — registered with the cluster shmem region
 * registry (spec-1.X cluster shmem framework).
 */
extern Size cluster_sinval_outbound_shmem_size(void);
extern void cluster_sinval_outbound_shmem_init(void);
extern Size cluster_sinval_inbound_shmem_size(void);
extern void cluster_sinval_inbound_shmem_init(void);

/*
 * IC msg type registration — called once at postmaster start.
 */
extern void cluster_sinval_register_msg_type(void);

/*
 * Module init — registers outbound + inbound shmem regions with the
 * cluster shmem region registry.  Called from postmaster startup before
 * the SI Broadcaster aux process spawns.
 */
extern void cluster_sinval_module_init(void);

/*
 * Counter accessors (D10 — exposed via cluster_state dump_sinval category;
 * 9 NEW rows).
 */
extern uint64 cluster_sinval_get_broadcast_send_count(void);
extern uint64 cluster_sinval_get_broadcast_receive_count(void);
extern uint64 cluster_sinval_get_inject_local_queue_count(void);
extern uint64 cluster_sinval_get_outbound_queue_full_count(void);
extern uint64 cluster_sinval_get_inbound_queue_full_count(void);
extern uint64 cluster_sinval_get_inbound_overflow_reset_count(void);
extern uint64 cluster_sinval_get_validation_drop_count(void);
extern uint64 cluster_sinval_get_stale_epoch_drop_count(void);
extern uint64 cluster_sinval_get_echo_dropped_count(void);

/*
 * spec-2.39 D8/D9 NEW counter accessors — exposed via dump_sinval
 * category (sinval keys 9 → 15).
 *   D8 (3 fanout):  fanout_would_block / fanout_hard_error / fanout_peer_down
 *   D9 (3 ack):     ack_received / ack_timeout / ack_orphan
 */
extern uint64 cluster_sinval_get_fanout_would_block_count(void);
extern uint64 cluster_sinval_get_fanout_hard_error_count(void);
extern uint64 cluster_sinval_get_fanout_peer_down_count(void);
extern uint64 cluster_sinval_get_ack_received_count(void);
extern uint64 cluster_sinval_get_ack_timeout_count(void);
extern uint64 cluster_sinval_get_ack_orphan_count(void);

/*
 * spec-2.39 D2 — peer_enqueued ack/barrier blocking variant.
 *
 *   Used by AtEOXact_Inval(true) + COMMIT PREPARED production hook (D1).
 *   Caller blocks (WaitLatch) until all declared+CSSD-ALIVE peers ACK
 *   received the batch (status DONE or RESET_PENDING — both視 fulfilled
 *   per HC141) or cluster.sinval_ack_timeout_ms elapses.
 *
 *   GUC cluster.sinval_ack_mode controls behavior:
 *     none           -- skip ack wait entirely;equivalent to plain
 *                       cluster_sinval_enqueue_batch (fire-and-forget).
 *     peer_enqueued  -- (default) wait for IC ack envelope from each
 *                       alive peer (v0.3 P2 semantics).
 *
 *   Returns:
 *     CLUSTER_SINVAL_ACK_DONE          -- all alive peers ACK'd in time
 *     CLUSTER_SINVAL_ACK_TIMEOUT       -- timeout elapsed;bump
 *                                         ack_timeout_count;WARN 53R95
 *     CLUSTER_SINVAL_ACK_ENQUEUE_FAILED -- enqueue_batch returned false
 *                                         (outbound queue full);caller
 *                                         WARNS 53R94 + LMON broadcasts
 *                                         SINVAL_RESET_ALL_BROADCAST
 *                                         sentinel as fail-safe (v0.3 P1).
 */
typedef enum ClusterSinvalAckResult {
	CLUSTER_SINVAL_ACK_DONE = 0,
	CLUSTER_SINVAL_ACK_TIMEOUT,
	CLUSTER_SINVAL_ACK_ENQUEUE_FAILED
} ClusterSinvalAckResult;

extern ClusterSinvalAckResult
cluster_sinval_enqueue_and_wait_ack(const SharedInvalidationMessage *msgs, int n);

/*
 * spec-2.39 D7 + v0.3 P1 — RESET-all broadcast sentinel emit.
 *
 *   LMON-only.  Builds a SinvalBroadcastHeader with nmsgs=0 +
 *   flags=SINVAL_RESET_ALL_BROADCAST and fanouts to all declared+
 *   CSSD-ALIVE peers via cluster_ic_send_envelope_fanout (L172).
 *   Called from LMON main loop tick when reset_all_broadcast_pending
 *   atomic is set (by enqueuer-side ENQUEUE_FAILED path).
 */
extern void cluster_sinval_broadcast_reset_all(void);

/*
 * spec-2.39 D14 — reconfig RESET-all hook (local-only).
 *
 *   Called from cluster_reconfig.c tick path on epoch bump.
 *   Performs local SIResetAll + clears ack_wait_table stale entries.
 *   Note:  remote peers also independently reconfig + RESET-all, so
 *   弹性收敛 across cluster (no cross-node coordination needed).
 */
extern void cluster_sinval_reset_all_on_reconfig(void);

/*
 * spec-2.39 D3 — ack_wait shmem region size / init.
 */
extern Size cluster_sinval_ack_wait_shmem_size(void);
extern void cluster_sinval_ack_wait_shmem_init(void);

/*
 * spec-2.39 D5 — ack_outbound shmem region size / init.
 */
extern Size cluster_sinval_ack_outbound_shmem_size(void);
extern void cluster_sinval_ack_outbound_shmem_init(void);

/*
 * spec-2.39 D5 — LMON ack-outbound drain hook (LMON-only).
 *
 *   Called from LMON main loop after the regular outbound drain.
 *   Drains ClusterSinvalAckOutbound + emits single-peer ACK envelopes.
 */
extern void cluster_sinval_drain_ack_outbound_and_send(void);

/*
 * spec-2.39 D6 — IC handler for PGRAC_IC_MSG_SINVAL_ACK.
 *   Registered with dispatch table via cluster_sinval_register_msg_type.
 */
struct ClusterICEnvelope;
extern void cluster_sinval_handle_ack_envelope(const struct ClusterICEnvelope *env,
											   const void *payload);

/*
 * SI Broadcaster proc latch — set by IC handler / public API to wake the
 * aux process for drain.
 */
extern void cluster_sinval_set_proc_latch(void);
extern void cluster_sinval_register_proc_latch(struct Latch *latch);
extern void cluster_sinval_unregister_proc_latch(void);

#endif /* USE_PGRAC_CLUSTER */
#endif /* CLUSTER_SINVAL_H */
