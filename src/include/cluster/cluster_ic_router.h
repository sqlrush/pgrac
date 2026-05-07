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
#define CLUSTER_IC_PRODUCER_AUTOVAC ((uint32)(1u << B_AUTOVAC_LAUNCHER))
#define CLUSTER_IC_PRODUCER_BGWRITER ((uint32)(1u << B_BG_WRITER))
#define CLUSTER_IC_PRODUCER_CHECKPOINTER ((uint32)(1u << B_CHECKPOINTER))
/* spec-2.X may add more as new BackendType slots get assigned */


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
extern bool cluster_ic_send_envelope(uint8 msg_type, int32 dest_node_id, const void *payload,
									 uint32 payload_len);

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
extern bool cluster_ic_dispatch_envelope(const ClusterICEnvelope *env, const void *payload);


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

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_IC_ROUTER_H */
