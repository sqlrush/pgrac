/*-------------------------------------------------------------------------
 *
 * cluster_ic_router.h
 *	  pgrac cluster IC message-type registration + send/dispatch
 *	  abstraction (spec-2.3 D4).
 *
 *	  Replaces the spec-2.2 hard-coded scope guard (LMON-only +
 *	  HEARTBEAT-only) with a declarative registration model:
 *
 *	    (1) Each sub-spec registers its msg_type in postmaster phase 1
 *	        via cluster_ic_register_msg_type(), declaring:
 *	          - allowed_producer_mask: bitmask of BackendType values
 *	            that may send this msg_type (spec-2.2 §3.9 LMON-only
 *	            preserved by setting mask = (1 << B_LMON))
 *	          - broadcast_ok: whether dest = PGRAC_IC_BROADCAST is allowed
 *	          - handler: dispatched on inbound recv (LMON context;
 *	            must obey §3.5 hard constraints -- nonblocking, no
 *	            LWLock wait, no catalog SQL, no ereport ERROR/FATAL/PANIC)
 *
 *	    (2) cluster_ic_send_envelope() looks up dispatch_table[msg_type]
 *	        and rejects (ereport ERROR) if:
 *	          - msg_type not registered
 *	          - MyBackendType not in allowed_producer_mask
 *	          - dest_node_id == PGRAC_IC_BROADCAST but !broadcast_ok
 *	          - payload_length > PGRAC_IC_PAYLOAD_MAX (spec-2.3 §3.5b
 *	            outbound 16 MB rule)
 *
 *	    (3) cluster_ic_dispatch_envelope() (LMON-internal recv path)
 *	        looks up handler; if NULL it's a peer-level failure
 *	        (returns false; LMON closes peer + LOG/WARNING + metric;
 *	        spec-2.3 §3.5b inbound rule -- NEVER ereport ERROR LMON);
 *	        if non-NULL, invokes handler inside PG_TRY/PG_CATCH so
 *	        a buggy handler raising ERROR drops the frame instead of
 *	        crashing LMON (spec-2.3 §3.5 + Q14 防御层语义).
 *
 *	  dispatch_table is a static process-local array
 *	  (CLUSTER_IC_MSG_TYPE_MAX = 256 slots; ~8 KB process-local).
 *	  Function pointers are L61 process-resource and CANNOT be in
 *	  shmem; postmaster phase 1 registers (cluster_init_shmem)
 *	  populate the table BEFORE fork, so all backend / aux process
 *	  copies inherit the same set via fork COW (Linux/macOS) or
 *	  re-init via EXEC_BACKEND (Windows).  Register-once contract:
 *	  duplicate cluster_ic_register_msg_type() = ereport(FATAL).
 *
 *	  Spec authority: pgrac:specs/spec-2.3-...md frozen v0.2
 *	  (2026-05-07; user approve Q1-Q14 + 4 hard 修订).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_ic_router.h
 *
 * NOTES
 *	  pgrac-original file.  Built only in --enable-cluster mode
 *	  (USE_PGRAC_CLUSTER); declarations are gated.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_IC_ROUTER_H
#define CLUSTER_IC_ROUTER_H

#include "cluster/cluster_ic.h" /* ClusterICSendResult (F1 L68) */
#include "cluster/cluster_ic_envelope.h"
#include "miscadmin.h" /* BackendType */


#ifdef USE_PGRAC_CLUSTER

/* ============================================================
 * Producer mask helpers.
 *
 *   Each msg_type's allowed_producer_mask is a uint32 bitmask
 *   indexed by BackendType (defined in miscadmin.h).  spec-2.X
 *   sub-specs OR these macros together to declare which backend
 *   types may invoke cluster_ic_send_envelope for the msg_type.
 *
 *   spec-2.2 §3.9 LMON-only invariant for HEARTBEAT is preserved
 *   by registering with mask = CLUSTER_IC_PRODUCER_LMON.  Future
 *   sub-specs widen via OR (e.g. SCN_BROADCAST: LMON | WALWRITER).
 * ============================================================ */

#define CLUSTER_IC_PRODUCER_NONE ((uint32)0u)
#define CLUSTER_IC_PRODUCER_LMON ((uint32)(1u << B_LMON))
#define CLUSTER_IC_PRODUCER_WALWRITER ((uint32)(1u << B_WAL_WRITER))
#define CLUSTER_IC_PRODUCER_BACKEND ((uint32)(1u << B_BACKEND))
#define CLUSTER_IC_PRODUCER_AUTOVAC                                                                \
	((uint32)((1u << B_AUTOVAC_LAUNCHER) | (1u << B_AUTOVAC_WORKER)))
#define CLUSTER_IC_PRODUCER_BGWORKER ((uint32)(1u << B_BG_WORKER))
#define CLUSTER_IC_PRODUCER_BGWRITER ((uint32)(1u << B_BG_WRITER))
#define CLUSTER_IC_PRODUCER_CHECKPOINTER ((uint32)(1u << B_CHECKPOINTER))
#define CLUSTER_IC_PRODUCER_BUFFER_CLIENTS                                                         \
	(CLUSTER_IC_PRODUCER_BACKEND | CLUSTER_IC_PRODUCER_AUTOVAC | CLUSTER_IC_PRODUCER_BGWORKER      \
	 | CLUSTER_IC_PRODUCER_BGWRITER | CLUSTER_IC_PRODUCER_CHECKPOINTER)
/* spec-2.X may add more as new BackendType slots get assigned */

/*
 * PGRAC: spec-7.2 D3 (r3/r4 amend) — LMS producer bits for the DATA-plane
 * msg_type family.  _LMS_DATA pre-includes the 7.3 worker bit so the
 * five GCS block registrations widen exactly once.  REDECLARE stays on
 * the CONTROL mask untouched (r4 ruling), and CONTROL-only msg_types
 * never gain these bits — a migrated handler that must emit a CONTROL
 * message stages it into the CONTROL outbound ring for LMON to send
 * (direct send from LMS = producer-gate ERROR by construction).
 */
#define CLUSTER_IC_PRODUCER_LMS ((uint32)(1u << B_LMS))
#define CLUSTER_IC_PRODUCER_LMS_WORKER ((uint32)(1u << B_LMS_WORKER))
#define CLUSTER_IC_PRODUCER_LMS_DATA (CLUSTER_IC_PRODUCER_LMS | CLUSTER_IC_PRODUCER_LMS_WORKER)


/* ============================================================
 * ClusterICMsgTypeInfo -- registration record for one msg_type.
 *
 *   Sub-specs construct a const struct value at module load and
 *   pass it to cluster_ic_register_msg_type during postmaster
 *   phase 1 (cluster_init_shmem).  All fields are read-only after
 *   registration.
 * ============================================================ */

typedef struct ClusterICMsgTypeInfo {
	uint8 msg_type;				  /* ClusterICMsgType enum value */
	const char *name;			  /* "heartbeat" / "scn_broadcast" / ... */
	uint32 allowed_producer_mask; /* OR of CLUSTER_IC_PRODUCER_* */
	bool broadcast_ok;			  /* dest = PGRAC_IC_BROADCAST allowed? */
	void (*handler)(const ClusterICEnvelope *env, const void *payload);

	/*
	 * PGRAC: spec-7.2 D3 — owning plane (ClusterICPlane; uint8 keeps the
	 * const-initializer shape).  Zero = CONTROL, so every existing
	 * registration is CONTROL without edits.  A msg_type is sent and
	 * dispatched ONLY in its plane's owner process:  the physical-send
	 * layer ereports on a cross-plane send, dispatch drops the frame
	 * fail-closed (plane_misroute_reject).  Flip discipline (H-5 + user
	 * ruling): the five GCS block types turn DATA in ONE commit at the
	 * end of the D3+D4 sequence — no half-migrated window.
	 */
	uint8 plane;
} ClusterICMsgTypeInfo;


/* ============================================================
 * Registration API (postmaster phase 1; read-only after fork).
 * ============================================================ */

/*
 * cluster_ic_register_msg_type -- register a msg_type with the
 *   dispatch table.
 *
 *   Caller invariants:
 *     - info->msg_type ∈ [1, CLUSTER_IC_MSG_TYPE_MAX) (msg_type 0
 *       is reserved sentinel)
 *     - info->name non-NULL
 *     - info->handler may be NULL only if the msg_type is
 *       send-only (no inbound dispatch -- not used in spec-2.3 but
 *       allowed by API)
 *
 *   Behavior on duplicate registration (per Q9 + spec-2.3 §1.4
 *   invariant 4): ereport(FATAL) with errcode INTERNAL_ERROR.
 *   The init-layer guard in cluster_init_shmem() prevents
 *   accidental re-entry from causing this; only direct duplicate
 *   register() calls trigger the FATAL.
 *
 *   Lifecycle: must be called in postmaster phase 1 (per Q10).
 *   The dispatch_table is process-local static (per Q11), populated
 *   in postmaster context, then inherited via fork (Linux/macOS) or
 *   re-populated via EXEC_BACKEND child re-init (Windows).
 */
extern void cluster_ic_register_msg_type(const ClusterICMsgTypeInfo *info);


/* ============================================================
 * Send / dispatch API.
 * ============================================================ */

/*
 * cluster_ic_send_envelope -- top-level send entry point for any
 *   non-LMON or LMON producer.
 *
 *   Validation (in order):
 *     1. msg_type registered (handler != NULL OR non-NULL name --
 *        we accept name as the "registered" signal since handler
 *        may be NULL for send-only types)
 *     2. MyBackendType ∈ allowed_producer_mask (per spec-2.3 §3.4
 *        send path scope guard)
 *     3. dest_node_id == self → no-op success; this is the spec-2.2
 *        stub-tier semantic preserved (msg sent to self = local-only)
 *     4. dest_node_id == PGRAC_IC_BROADCAST → require broadcast_ok
 *     5. payload_length <= PGRAC_IC_PAYLOAD_MAX (spec-2.3 §3.5b
 *        outbound 16 MB rule -- ereport ERROR + errhint to spec-2.4)
 *
 *   Then build envelope (cluster_ic_envelope_build) and delegate
 *   to cluster_ic_send_bytes (the existing tier vtable from
 *   cluster_ic.h).
 *
 *   Returns true on success, false on transport-level failure
 *   (peer not reachable etc).  Validation failures ereport(ERROR).
 *
 *   spec-2.3 D5 NOTE: in this spec, only LMON actually sends
 *   anything (HEARTBEAT mask = CLUSTER_IC_PRODUCER_LMON); non-LMON
 *   producer support requires the queue/enqueue API forward-linked
 *   in spec-2.3 §3.6 + Q5 D' (must land before spec-2.9/2.10 SCN
 *   broadcast).  Until then, non-LMON callers will be rejected by
 *   the producer_mask check at step 2 (validating that the spec-2.3
 *   scope is honored).
 */
extern ClusterICSendResult cluster_ic_send_envelope(uint8 msg_type, int32 dest_node_id,
													const void *payload, uint32 payload_len);

/*
 * cluster_ic_dispatch_envelope -- LMON-internal recv path.
 *
 *   Caller (LMON main loop after recv'ing a complete envelope +
 *   payload + verifying via cluster_ic_envelope_verify) invokes
 *   this to route the msg to the registered handler.
 *
 *   Behavior:
 *     - dispatch_table[env->msg_type].handler == NULL
 *       → unregistered msg_type from peer.  Returns false (caller
 *         is expected to peer-level-fail per spec-2.3 §3.5b inbound:
 *         close peer + LOG/WARNING + metric).  Does NOT ereport
 *         ERROR -- LMON main loop continues.
 *     - handler != NULL → invoke inside PG_TRY/PG_CATCH (per Q14
 *         + R3 防御层): catch ereport(ERROR) only; LOG + drop frame
 *         + reset MemoryContext + return true (frame counted as
 *         dispatched-with-handler-violation, which is spec drift
 *         caught at PR review).  ereport(FATAL) and ereport(PANIC)
 *         are NOT caught -- they propagate per PG semantics and
 *         terminate LMON (postmaster crash recovery restarts).
 *
 *   Returns true if handler was invoked (with or without ERROR
 *   caught); false if msg_type unregistered.
 */
/*
 * spec-2.4 hardening v1.0.1 F1 (L76 register-vs-handler-signature-coupling):
 * peer_id parameter NEW.  Required because:
 *   1. msg_type == PGRAC_IC_CHUNK_MSG_TYPE (255) short-circuits to
 *      cluster_ic_chunk_dispatch_frame which needs caller's peer_id
 *      for per-peer reassembly state machine.
 *   2. Future spec-2.X handlers may want peer-aware metadata.
 *
 * peer_id == -1 is allowed for pre-handshake / unit-test paths
 * (chunk fast path will reject in that case).
 */
extern bool cluster_ic_dispatch_envelope(const ClusterICEnvelope *env, const void *payload,
										 int32 peer_id);


/* ============================================================
 * Diagnostic accessors (read-only; safe to call from any backend).
 * ============================================================ */

/*
 * cluster_ic_get_msg_type_info -- read-only lookup of registration
 *   metadata for diagnostic / view rendering.
 *
 *   Returns NULL if msg_type not registered (handler == NULL AND
 *   name == NULL); otherwise returns pointer to the in-table
 *   const-style record (caller must not mutate; lifetime is the
 *   process lifetime).
 *
 *   Used by:
 *     - cluster_ic_router.c internals (send_envelope validation)
 *     - future spec-2.3 SRF cluster_get_ic_msg_types() (Step 6 +
 *       D-future) for the pg_cluster_ic_msg_types view (Q8 ★ A)
 *     - cluster_unit U6 / U7 / U8 tests (read mask + handler)
 */
extern const ClusterICMsgTypeInfo *cluster_ic_get_msg_type_info(uint8 msg_type);

/*
 * cluster_ic_router_count_registered -- number of msg_types currently
 *   in the dispatch table.  msg_type 0 (sentinel) excluded.  Used
 *   by tests + diagnostic snapshot.
 */
extern int cluster_ic_router_count_registered(void);


/* ============================================================
 * Fanout API (spec-2.5 D2.5; v0.2 Q3 修订采纳).
 *
 *   spec-2.4 ship 时 cluster_ic_send_envelope(dest=PGRAC_IC_BROADCAST=
 *   0xFFFFFFFF) 一路传到 tier1_send_bytes(target_node_id) → tier1 把
 *   广播 sentinel 当 invalid target 拒掉;**broadcast 真路径未实装**。
 *   spec-2.5 引入 explicit fanout 层填补此 gap;本 spec 是首个 caller,
 *   后续 spec-2.6 quorum / 2.18 sinval / 2.X SCN broadcast 全复用此 API。
 *
 *   架构 invariant (spec-2.5 v0.2 §1.4 例外 #1 + L61 process-resource-
 *   vs-shmem):fanout API 只能从 **LMON process context** 调用 — 因为
 *   tier1 TCP fd 是 LMON process-local kernel resource;非-LMON context
 *   通过 spec-2.5 D2 CSSD outbound queue + LMON tick drain 模式间接调用
 *   (CSSD 写 shmem outbound slot;LMON tick read + 调 single-peer send,
 *   不经过 fanout API 因为已在 per-peer 迭代里)。
 * ============================================================ */

/*
 * ClusterICFanoutResult -- per-peer result enum 4 态 (扩 spec-2.3
 *   v1.0.1 L68 三态加 PEER_DOWN).
 *
 *   PEER_DOWN distinguishes "peer fd 不存在 / 未 connect / phase 4
 *   first-tick" 类 nonfatal "未连上" 情况 vs "曾经 up 现在 down" 类
 *   HARD_ERROR (tier1 已 close peer)。caller 必须区分以决定:
 *
 *     - DONE:        accepted; update last_send_at + counter
 *     - WOULD_BLOCK: nonfatal backpressure; retry next tick;
 *                    last_send_at 不更新 (per spec-2.3 v1.0.1 L68)
 *     - HARD_ERROR:  peer fd 已 close (LMON 自管 reconnect);
 *                    last_send_at 不更新; caller 不 close peer
 *                    (close 是 LMON-only 操作; per spec-2.4 v1.0.1 L74)
 *     - PEER_DOWN:   peer 还没 connect 上 / phase 4 first-tick;
 *                    last_send_at 不更新; LOG advisory; 等下次 tick
 *                    重试。绝不 ereport ERROR/FATAL/PANIC。
 */
typedef enum ClusterICFanoutResult {
	CLUSTER_IC_FANOUT_DONE = 0,
	CLUSTER_IC_FANOUT_WOULD_BLOCK,
	CLUSTER_IC_FANOUT_HARD_ERROR,
	CLUSTER_IC_FANOUT_PEER_DOWN
} ClusterICFanoutResult;

/*
 * cluster_ic_send_envelope_fanout -- explicit broadcast fanout.
 *
 *   For each peer in [0, CLUSTER_MAX_NODES):
 *     - peer == cluster_node_id (self):                 PEER_DOWN
 *     - cluster_conf_lookup_node(peer) == NULL:         PEER_DOWN
 *     - cluster_ic_tier1_get_peer_fd(peer) < 0:         PEER_DOWN
 *     - else: build envelope (dest=peer) + cluster_ic_send_bytes;
 *             map ClusterICSendResult → ClusterICFanoutResult
 *
 *   Pre-loop validation (caller-visible ereport ERROR):
 *     - msg_type ∈ [1, CLUSTER_IC_MSG_TYPE_MAX): registered + has
 *       handler != NULL (or send-only;name != NULL)
 *     - info->broadcast_ok == true (broadcast_ok=false 调 fanout API
 *       是 spec drift → ereport ERROR + errhint)
 *     - MyBackendType ∈ allowed_producer_mask (per-peer send 路径 also
 *       enforces; fanout 提前 fail-fast)
 *     - payload_len ≤ PGRAC_IC_PAYLOAD_MAX (16 MB)
 *
 *   per_peer[] is OUT-only (caller-allocated; fanout writes all
 *   CLUSTER_MAX_NODES slots before returning).
 *
 *   spec authority:
 *     - pgrac:specs/spec-2.5-...md §1.4 例外 #2 + §2.2 + §2.2.1
 *     - L61 process-resource-vs-shmem (LMON-only context)
 *     - L68 backpressure-≠-peer-death (WOULD_BLOCK / HARD_ERROR 区分)
 *     - L71 metadata-symmetric-enforce (broadcast_ok send-side 对称)
 *     - L74 cross-aux-process-close-must-be-LMON-mediated (caller 不
 *       close peer)
 */
extern void cluster_ic_send_envelope_fanout(uint8 msg_type, const void *payload, uint32 payload_len,
											ClusterICFanoutResult per_peer[]);

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_IC_ROUTER_H */
