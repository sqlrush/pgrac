/*-------------------------------------------------------------------------
 *
 * cluster_gcs.h
 *	  pgrac cluster GCS (Global Cache Service) request protocol skeleton.
 *
 *	  spec-2.32 activates the cross-node GCS request/reply wire protocol
 *	  framework on top of the existing cluster_ic envelope dispatcher.
 *	  Wire opcodes PGRAC_IC_MSG_GCS_REQUEST=12 / PGRAC_IC_MSG_GCS_REPLY=13
 *	  carry PCM transition requests between PCM client backends and the
 *	  master node that owns the GrdEntry for a given BufferTag.
 *
 *	  Skeleton scope (FROZEN v0.4):
 *	    - Wire ABI definition (GcsRequestPayload 48B / GcsReplyPayload 24B)
 *	    - cluster_gcs_lookup_master placeholder (single-node return self)
 *	    - cluster_gcs_send_transition_and_wait sender (loopback only)
 *	    - cluster_gcs_handle_request/reply receiver handlers (real, non-stub)
 *	    - PCM lock acquire / release / upgrade / downgrade integration via
 *	      master-lookup branch (HC72 self-short-circuit production hot path)
 *
 *	  Forward-link status:
 *	    - spec-2.33 replaces the master-cache placeholder with deterministic
 *	      hash routing, enables 2-node cross-node wire send, and adds the
 *	      block payload shipping substrate.
 *	    - GCS control-message timeout GUC + retransmit remain spec-2.34+.
 *	    - Reconfig epoch invalidation handling
 *
 *	  HC contracts in this header (HC70-HC78 7+2 NEW):
 *	    HC70 sender must traverse real cluster_ic_send_envelope path
 *	    HC71 receiver handler must be real (validate + dispatch + counter
 *	         + status/reply); stub receiver forbidden (L107 N+5 red line)
 *	    HC72 production master==self short-circuit; wire path is test-only
 *	    HC73 reply must carry request_id + transition_id + status + epoch
 *	    HC74 per-backend outstanding-request table, LWLock-protected
 *	    HC75 transition_id goes through spec-2.30 validator; illegal fail-closed
 *	    HC76 remote wait does not pollute BufferDesc.buffer_type/pcm_state
 *	    HC77 master-side handler is single transition-apply owner; sender
 *	         must not double-apply on GRANTED reply
 *	    HC78 release/upgrade/downgrade symmetric wire when master is remote;
 *	         local-only release after remote-acquire leaks master-side entry
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_gcs.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-2.32-gcs-request-protocol-skeleton.md (FROZEN v0.4)
 *	  Design: docs/cache-fusion-protocol-design.md
 *	  AD-005 (Cache Fusion full) + AD-002 (PCM lock state machine)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GCS_H
#define CLUSTER_GCS_H

#include "c.h"
#include "cluster/cluster_pcm_lock.h" /* PcmLockTransition */
#include "storage/buf_internals.h"	  /* BufferTag */

#ifdef USE_PGRAC_CLUSTER

/* ============================================================
 * GcsReplyStatus -- reply status code carried in GcsReplyPayload.
 *
 *  HC73:  one of these values must be set in every reply payload.
 * ============================================================ */
typedef enum GcsReplyStatus {
	GCS_REPLY_GRANTED = 0,				   /* transition applied master-side */
	GCS_REPLY_DENIED_INCOMPATIBLE = 1,	   /* state conflict (X holder etc.) */
	GCS_REPLY_DENIED_VALIDATOR_REJECT = 2, /* HC75 transition_id illegal */
	GCS_REPLY_DENIED_EPOCH_STALE = 3	   /* request epoch < current */
} GcsReplyStatus;


/* ============================================================
 * GcsRequestPayload -- wire ABI for PGRAC_IC_MSG_GCS_REQUEST envelopes.
 *
 *  Field ordering rationale (spec-2.32 v0.2 codex review F5):
 *  request_id + epoch at offset [0, 16) come first for early stale-reply
 *  rejection without decoding the full payload (receiver can compare
 *  epoch against snapshot before reading tag / sender / transition_id).
 *
 *  Layout (48B):
 *    [  0,   8) request_id          -- per-sender-backend monotone (HC74)
 *    [  8,  16) epoch               -- cluster_epoch snapshot at send (HC73)
 *    [ 16,  36) tag                 -- BufferTag (PG-fact 20B per spec-2.30 F1)
 *    [ 36,  40) sender_node         -- int32 cluster_node_id of sender
 *    [ 40,  41) transition_id       -- PcmLockTransition (1..9) (HC75)
 *    [ 41,  48) reserved_0[7]       -- align + future fields (priority, etc)
 * ============================================================ */
typedef struct GcsRequestPayload {
	uint64 request_id;	 /*  8B [  0,   8) */
	uint64 epoch;		 /*  8B [  8,  16) */
	BufferTag tag;		 /* 20B [ 16,  36) */
	int32 sender_node;	 /*  4B [ 36,  40) */
	uint8 transition_id; /*  1B [ 40,  41) */
	uint8 reserved_0[7]; /*  7B [ 41,  48) */
} GcsRequestPayload;

StaticAssertDecl(sizeof(GcsRequestPayload) == 48,
				 "spec-2.32 D1 GcsRequestPayload wire ABI 48B "
				 "(request_id 8 + epoch 8 + tag 20 + sender_node 4 + "
				 "transition_id 1 + reserved 7)");


/* ============================================================
 * GcsReplyPayload -- wire ABI for PGRAC_IC_MSG_GCS_REPLY envelopes.
 *
 *  Layout (24B):
 *    [  0,   8) request_id          -- match outstanding (HC74)
 *    [  8,   9) transition_id       -- echo from request (HC73)
 *    [  9,  10) status              -- GcsReplyStatus (HC73)
 *    [ 10,  12) reserved_0[2]       -- align
 *    [ 12,  16) sender_node         -- int32 of replying node
 *    [ 16,  24) epoch               -- cluster_epoch when reply emitted
 * ============================================================ */
typedef struct GcsReplyPayload {
	uint64 request_id;	 /*  8B [  0,   8) */
	uint8 transition_id; /*  1B [  8,   9) */
	uint8 status;		 /*  1B [  9,  10) */
	uint8 reserved_0[2]; /*  2B [ 10,  12) */
	int32 sender_node;	 /*  4B [ 12,  16) */
	uint64 epoch;		 /*  8B [ 16,  24) */
} GcsReplyPayload;

StaticAssertDecl(sizeof(GcsReplyPayload) == 24,
				 "spec-2.32 D1 GcsReplyPayload wire ABI 24B "
				 "(request_id 8 + transition_id 1 + status 1 + reserved 2 + "
				 "sender_node 4 + epoch 8)");


/* ============================================================
 * Public API.
 * ============================================================ */

/*
 * cluster_gcs_lookup_master — return master node id for the given BufferTag.
 *
 *  spec-2.32 placeholder:  always returns cluster_node_id (single-node
 *  always-self semantics).  Forward-link spec-2.33+ replaces with real
 *  GRD master cache lookup using deterministic hash / cluster_grd module
 *  / DRM remastering integration.
 *
 *  Test-only injection is available only in cluster_unit / TAP harness
 *  builds (cluster_gcs_test_force_remote_master_node global).  Production
 *  guc.c does not expose a runtime master override (avoids production
 *  attack/misconfig surface).
 */
extern int cluster_gcs_lookup_master(BufferTag tag);
/* spec-4.7 D7 — PURE static declared-list master (no recovery re-route);  the
 * block's original (possibly dead) master, used by the recovery-phase gate. */
extern int cluster_gcs_lookup_master_static(BufferTag tag);

/*
 * cluster_gcs_send_transition_and_wait — send GCS_REQUEST envelope to
 * remote master, block on reply CV, return when transition is applied
 * master-side (HC77 sender does not double-apply).
 *
 *  Steps (HC70 + HC73 + HC74):
 *    1. Reserve per-backend outstanding-request slot
 *    2. Bump per-backend request_id monotone counter
 *    3. Build GcsRequestPayload(tag, transition_id, sender_node=self,
 *       request_id, epoch=cluster_epoch_snapshot())
 *    4. cluster_ic_send_envelope(master_node, GCS_REQUEST, &payload, 48)
 *    5. ConditionVariableSleep(slot.reply_cv, WAIT_EVENT_GCS_REPLY_WAIT)
 *       with internal safety deadline (5s default; public control-message
 *       GUC + retransmit推 spec-2.34+)
 *    6. On wake: check status
 *       - GRANTED:  return (master already applied transition; HC77)
 *       - DENIED_*: ereport
 *       - TIMEOUT:  cleanup + ereport ERRCODE_QUERY_CANCELED
 *    7. Release slot
 */
extern void cluster_gcs_send_transition_and_wait(BufferTag tag, PcmLockTransition transition_id,
												 int master_node);

/*
 * cluster_gcs_send_transition_nowait — fire-and-forget GCS_REQUEST to a
 * remote master (spec-6.12a ㉕ remote-holder downgrade notify).
 *
 *	Sends the transition envelope with request_id = 0 (never allocated by the
 *	per-backend counters, which start at 1) and does NOT reserve a reply slot
 *	or wait: the master's GCS_REPLY comes back to this node's reply handler,
 *	finds no matching slot and is dropped as a stale reply (HC74) — by
 *	design.  Built for the LMON IC-dispatch context, which structurally
 *	CANNOT block on a reply CV (the reply would be delivered by the very
 *	dispatch loop that is sleeping).  Returns true when the envelope was
 *	handed to the transport; false on send failure — the caller must treat
 *	false as "master not notified" and leave all local state untouched.
 *
 *	The master applies the transition through the same HC77 single-owner
 *	handler; a lost/denied notify is converged by the requester's follow-up
 *	registration attempt failing closed (degrade to one-shot semantics).
 */
extern bool cluster_gcs_send_transition_nowait(BufferTag tag, PcmLockTransition transition_id,
											   int master_node);

/*
 * cluster_gcs_try_send_transition_and_wait — spec-6.12a ㉕ non-throwing
 * twin of cluster_gcs_send_transition_and_wait: same slot + wait machinery,
 * but every non-GRANTED outcome (denied, timeout, send failure) returns
 * false instead of ereport.  Built for the requester's S-holder
 * registration after a remote-holder downgrade grant, where a denial must
 * degrade to one-shot read semantics rather than abort the query.
 */
extern bool cluster_gcs_try_send_transition_and_wait(BufferTag tag, PcmLockTransition transition_id,
													 int master_node);

/*
 * cluster_gcs_register_msg_types — postmaster-once registration of
 * GCS_REQUEST + GCS_REPLY in cluster_ic dispatch table.  Called from
 * the same phase that registers GES / SCN / CSSD message types.
 *
 *  broadcast_ok = false (point-to-point only).
 *  producer mask covers buffer clients that enqueue through the LMON-drained
 *  outbound ring for true cross-node send.
 */
extern void cluster_gcs_register_msg_types(void);

/*
 * cluster_gcs_shmem_size / cluster_gcs_shmem_init — outstanding-request
 * shmem region (per-backend table + LWLock).  Registered via cluster_shmem
 * registry like other cluster modules.
 */
extern Size cluster_gcs_shmem_size(void);
extern void cluster_gcs_shmem_init(void);
extern void cluster_gcs_module_init(void);

/* ============================================================
 * Receiver handlers — installed into cluster_ic dispatch table by
 * cluster_gcs_register_msg_types().  Exposed for cluster_unit tests
 * to exercise dispatch directly via cluster_ic_dispatch_envelope
 * (the production send-self path is no-op per cluster_ic_router.c:202).
 * ============================================================ */

/* Forward decl — definition lives in cluster_ic_envelope.h */
struct ClusterICEnvelope;

extern void cluster_gcs_handle_request_envelope(const struct ClusterICEnvelope *env,
												const void *payload);
extern void cluster_gcs_handle_reply_envelope(const struct ClusterICEnvelope *env,
											  const void *payload);


/* ============================================================
 * Observability accessors (dump_gcs 22 rows as of spec-2.33).
 *
 *  Each accessor returns a uint64 counter for SQL surface emit_row.
 *  When cluster_gcs module is not initialized (cluster_pcm_is_active
 *  returned false at startup), accessors return 0.
 * ============================================================ */
extern uint64 cluster_gcs_get_lookup_master_self_count(void);
extern uint64 cluster_gcs_get_lookup_master_remote_count(void);
extern uint64 cluster_gcs_get_send_request_count(void);
extern uint64 cluster_gcs_get_handle_request_count(void);
extern uint64 cluster_gcs_get_handle_reply_count(void);
extern uint64 cluster_gcs_get_outstanding_count(void);
extern uint64 cluster_gcs_get_reply_late_drop_count(void);
extern uint64 cluster_gcs_get_reply_timeout_count(void);
extern uint64 cluster_gcs_get_encode_payload_bytes(void);
extern uint64 cluster_gcs_get_decode_payload_bytes(void);
extern uint64 cluster_gcs_get_dispatch_loop_iterations(void);
extern uint64 cluster_gcs_get_max_outstanding(void);

/*
 * cluster_gcs_get_api_state -- "active" or "stub" depending on whether
 * cluster_gcs_module_init has actually wired the module (single source
 * of truth for dump_gcs.api_state row).
 */
extern const char *cluster_gcs_get_api_state(void);


/* ============================================================
 * Test-only injection (cluster_unit / TAP harness builds only).
 *
 *  Production builds do not honor this knob -- the helper is gated
 *  behind USE_CLUSTER_UNIT compile flag (spec-2.32 §1.4 例外说明).
 * ============================================================ */
#ifdef USE_CLUSTER_UNIT
/*
 * If >= 0, cluster_gcs_lookup_master returns this value instead of
 * cluster_node_id.  Set by cluster_unit fixtures / TAP harness only.
 */
extern int cluster_gcs_test_force_remote_master_node;

/*
 * Test loopback helper: build a ClusterICEnvelope around the supplied
 * payload then invoke cluster_ic_dispatch_envelope directly (bypassing
 * the cluster_ic_send_envelope self-target no-op fast path at
 * cluster_ic_router.c:202).  Used by test_cluster_gcs_dispatch to
 * exercise the full receiver pipeline in single-node setups where
 * the production send path returns DONE without dispatching.
 */
extern void cluster_gcs_test_loopback_dispatch(uint8 msg_type, const void *payload,
											   uint32 payload_len);
#endif /* USE_CLUSTER_UNIT */


/* ============================================================
 * Internal constants.
 * ============================================================ */

/* Maximum concurrent outstanding GCS requests per backend (HC74).  This
 * keeps the outstanding table fixed-size (no shmem regrow during runtime).
 * spec-2.34+ may promote to a GUC if true cross-node concurrency needs it. */
#define MAX_OUTSTANDING_REQUESTS_PER_BACKEND 8

/* Internal GCS control reply safety deadline before public GUC lands (spec-2.34+).
 * 5 seconds is generous for loopback (microseconds typical) yet still
 * caps the worst case in case the receiver leaks a reply due to bugs. */
#define GCS_REPLY_INTERNAL_DEADLINE_MS 5000


#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_GCS_H */
