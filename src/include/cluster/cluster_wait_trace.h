/*-------------------------------------------------------------------------
 * cluster_wait_trace.h
 *   Bounded opt-in resource observations across TX and PCM waits.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_WAIT_TRACE_H
#define CLUSTER_WAIT_TRACE_H

#include "storage/buf_internals.h"

#ifdef USE_PGRAC_CLUSTER
#define CLUSTER_WAIT_TRACE_LIMIT 32768U
#define CLUSTER_WAIT_TRACE_RESOURCES 32U

typedef enum ClusterWaitTraceKind {
	CLWAIT_PCM_WAIT, CLWAIT_PCM_QUEUE, CLWAIT_PCM_ADMISSION,
	CLWAIT_TX_BUFFER, CLWAIT_TX_LEDGER, CLWAIT_TX_SUMMARY, CLWAIT_PCM_GUARD,
	CLWAIT_PCM_SEND_GATE, CLWAIT_PCM_REQUESTER_GATE, CLWAIT_NKINDS
} ClusterWaitTraceKind;

typedef enum ClusterWaitTraceSite {
	CLWAIT_SITE_PCM_EXECUTOR = 1, CLWAIT_SITE_PCM_COMPATIBLE,
	CLWAIT_SITE_PCM_BOOTSTRAP, CLWAIT_SITE_PCM_PREDECESSOR,
	CLWAIT_SITE_PCM_INSTALL, CLWAIT_SITE_PCM_ADMISSION,
	CLWAIT_SITE_TX_SOURCE, CLWAIT_SITE_TX_TARGET, CLWAIT_SITE_GES_TX,
	CLWAIT_SITE_PCM_SOURCE_FINISH, CLWAIT_SITE_PCM_RETAINED_DRAIN,
	CLWAIT_SITE_PCM_TARGET_PREPARE, CLWAIT_SITE_PCM_SOURCE_SERVICE
} ClusterWaitTraceSite;

typedef enum ClusterWaitTraceGuard {
	CLWAIT_GUARD_NONE, CLWAIT_GUARD_CONTENT, CLWAIT_GUARD_IO,
	CLWAIT_GUARD_ACTIVATION, CLWAIT_GUARD_GENERATION, CLWAIT_GUARD_FLAGS,
	CLWAIT_GUARD_RETAINED, CLWAIT_GUARD_SNAPSHOT, CLWAIT_GUARD_FINISH
} ClusterWaitTraceGuard;

/* All fields are observations, never authority. Zero/unknown fields must not
 * be used to complete a remote identity. master_view is set only by the
 * authenticated master admission call, not by the local entry projection. */
/* Owned admission-to-canonical-request projection, captured under entry_lock.
 * head may still be zero for a queued request; this is not a wire receipt. */
typedef struct ClusterWaitAdmissionBridge {
	uint64 r4, base, head, request_seq, enqueue_order;
	uint64 requester_connection, ingress_connection;
	int32 phase, source;
	bool known;
} ClusterWaitAdmissionBridge;

/* Local scheduling predicates only; never grant or transport-delivery proof. */
typedef enum ClusterWaitSendReason {
	CLWAIT_SEND_UNKNOWN, CLWAIT_SEND_DUE, CLWAIT_SEND_CLOCK,
	CLWAIT_SEND_PHYSICAL_OWNER, CLWAIT_SEND_STREAM_BUSY, CLWAIT_SEND_RETRY_AGE
} ClusterWaitSendReason;

typedef enum ClusterWaitSendSite {
	CLWAIT_SEND_STAGE = 1, CLWAIT_SEND_PROBE
} ClusterWaitSendSite;

typedef struct ClusterWaitSendGate {
	uint64 owner_generation, logical_generation, authority_generation, observed_us;
	uint64 last_attempt_us, last_send_us, episode;
	uint64 stream_generation, busy_stream_generation;
	uint32 capability_generation, busy_capability_generation;
	int32 owner, owner_node, owner_index, requester, state, kind, destination, reason, result, site;
	bool known, busy;
} ClusterWaitSendGate;

/* Requester-local physical send eligibility; distinct from retained slots. */
typedef enum ClusterWaitRequesterSite {
	CLWAIT_REQUESTER_BEGIN = 1, CLWAIT_REQUESTER_RECHECK, CLWAIT_REQUESTER_ROUND
} ClusterWaitRequesterSite;

typedef struct ClusterWaitRequesterGate {
	BufferTag tag;
	uint64 binding, formation, master_session, r4, request_seq, base, authority;
	uint64 observed_us, episode, completed_us, response_us, sent_us;
	uint64 last_dispatch_us, retry_slice_us;
	uint32 pending, requester_connection, master_ingress;
	int32 requester, master, phase, kind, send_kind, reason, site;
	bool known;
} ClusterWaitRequesterGate;

typedef struct ClusterWaitTrace {
	BufferTag tag;
	ClusterWaitAdmissionBridge admission;
	uint64 snapshot, wait_seq, request_seq;
	uint64 binding, formation, master_session, r4, acquisition;
	uint64 base, authority, head, enqueue_order;
	uint64 requester_connection, ingress_connection, pending_cookie;
	uint64 wire_sender_connection;
	uint64 own_generation, reservation, writer_activation, rx_activation;
	uint64 holder_epoch, holder_request;
	uint64 master_request, master_head_request;
	uint64 round_request, head_request;
	/* Caller key above is captured with the wait predicate. The following
	 * projections are sampled later and must never overwrite that key. */
	uint64 entry_formation, entry_acquisition, entry_base;
	uint64 round_formation, round_session, round_r4, round_base;
	uint64 round_requester_connection, round_ingress_connection;
	uint64 head_formation, head_session, head_base;
	uint64 head_requester_connection, head_ingress_connection;
	uint64 wait_head_change, head_change;
	uint64 continuation_pending, continuation_expected_x, continuation_reservation;
	uint64 continuation_claim_pending, continuation_claim_reservation;
	uint64 refusal_token;
	uint32 s_holders, pi_holders, progress, incompatible, blocked;
	uint32 holder_node, holder_proc;
	uint32 buffer_state, own_flags;
	uint32 refusal_flags, refusal_pins;
	uint32 tx_xid;
	uint32 pins_seen, ledger_seen, local_pins, rows, omitted;
	uint32 ordered_lane, semantic_crc32c;
	uint8 source_candidate, retain_pi_if_dirty, common_flags, common_outcome;
	int32 site, mode, expected_master, requester, source, queue_kind;
	int32 state, x_holder, pending_requester, phase, round_phase, owner_phase;
	int32 round_requester, head_requester;
	int32 round_master, guard, guard_result, refusal_reason;
	int32 blocking_instance, blocking_session, blocking_pid;
	int32 final_blocking_instance, final_blocking_session, final_blocking_pid;
	int32 master_holder_instance, master_holder_session, master_holder_pid;
	int32 buffer, private_pins, shared_pins, ledger_phase, own_state, buffer_type;
	bool tag_known, entry_known, request_known, master_view, content_held;
	bool round_request_known, head_request_known, caller_matches_round, caller_matches_head;
	bool continuation_known, refusal_io, transport_known;
	bool blocking_known, final_blocking_known, master_holder_known;
	bool master_head_known, master_request_head_known;
} ClusterWaitTrace;

extern uint64 cluster_wait_trace_begin(ClusterWaitTraceKind kind);
extern void cluster_wait_trace_emit(ClusterWaitTraceKind kind, const ClusterWaitTrace *row);
/* Non-throwing stderr record: safe before returning a transport rejection. */
extern void cluster_wait_trace_send_gate_emit(const BufferTag *tag,
	const ClusterWaitSendGate *gate);
extern void cluster_wait_trace_requester_gate_emit(const ClusterWaitRequesterGate *gate);
extern void cluster_buffer_trace_tx_wait(ClusterWaitTraceSite site, uint64 wait_seq,
	uint32 tx_xid, uint64 request_seq);
#endif
#endif
