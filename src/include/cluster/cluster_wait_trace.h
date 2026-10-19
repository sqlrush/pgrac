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
	CLWAIT_TX_BUFFER, CLWAIT_TX_LEDGER, CLWAIT_TX_SUMMARY, CLWAIT_NKINDS
} ClusterWaitTraceKind;

typedef enum ClusterWaitTraceSite {
	CLWAIT_SITE_PCM_EXECUTOR = 1, CLWAIT_SITE_PCM_COMPATIBLE,
	CLWAIT_SITE_PCM_BOOTSTRAP, CLWAIT_SITE_PCM_PREDECESSOR,
	CLWAIT_SITE_PCM_INSTALL, CLWAIT_SITE_PCM_ADMISSION,
	CLWAIT_SITE_TX_SOURCE, CLWAIT_SITE_TX_TARGET, CLWAIT_SITE_GES_TX
} ClusterWaitTraceSite;

/* All fields are observations, never authority. Zero/unknown fields must not
 * be used to complete a remote identity. master_view is set only by the
 * authenticated master admission call, not by the local entry projection. */
typedef struct ClusterWaitTrace {
	BufferTag tag;
	uint64 snapshot, wait_seq, request_seq;
	uint64 binding, formation, master_session, r4, acquisition;
	uint64 base, authority, head, enqueue_order;
	uint64 requester_connection, ingress_connection, pending_cookie;
	uint64 own_generation, reservation, writer_activation, rx_activation;
	uint64 holder_epoch, holder_request;
	uint64 round_request, head_request;
	uint32 s_holders, pi_holders, progress, incompatible, blocked;
	uint32 holder_node, holder_proc;
	uint32 buffer_state, own_flags;
	uint32 tx_xid;
	uint32 pins_seen, ledger_seen, local_pins, rows, omitted;
	int32 site, mode, expected_master, requester, source, queue_kind;
	int32 state, x_holder, pending_requester, phase, round_phase, owner_phase;
	int32 round_requester, head_requester;
	int32 buffer, private_pins, shared_pins, ledger_phase, own_state, buffer_type;
	bool tag_known, entry_known, request_known, master_view, content_held;
} ClusterWaitTrace;

extern uint64 cluster_wait_trace_begin(ClusterWaitTraceKind kind);
extern void cluster_wait_trace_emit(ClusterWaitTraceKind kind, const ClusterWaitTrace *row);
extern void cluster_buffer_trace_tx_wait(ClusterWaitTraceSite site, uint64 wait_seq,
	uint32 tx_xid, uint64 request_seq);
#endif
#endif
