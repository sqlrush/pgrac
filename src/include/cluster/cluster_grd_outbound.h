/*-------------------------------------------------------------------------
 *
 * cluster_grd_outbound.h
 *	  GES outbound ring + reserved reply pool + dirty-list — spec-2.16 D4.
 *
 *	  LMON-owned generic outbound ring for GES path (mirror spec-2.5 CSSD
 *	  outbound LMON drain pattern, generalized for 3 origin_kind):
 *
 *	    origin_kind ∈ {
 *	      BACKEND_REQUEST,  -- backend pre-S4 enqueues GES_REQUEST
 *	      LMON_REPLY,       -- work_queue drain produces GRANT/REJECT
 *	      CLEANUP_RELEASE   -- LockReleaseAll / abort enqueues GES_RELEASE
 *	    }
 *
 *	  Three nofail bounded behaviors (spec-2.16 v0.4 L1.3 + v0.6 L1.1):
 *
 *	    BACKEND_REQUEST full:    backend wait latch + timeout (producer
 *	                             side blocking; bounded by GUC
 *	                             cluster.ges_request_timeout_ms).
 *	    LMON_REPLY full:         reserved pool (LMON_REPLY_RESERVED_BUDGET
 *	                             slots, only-LMON_REPLY consumers).  If
 *	                             reserved池 also full → reply dirty-list.
 *	                             If dirty-list ALSO full → drop oldest +
 *	                             ges_reply_dropped_count++ (backend retry
 *	                             via timeout converges) — REJECT_BUSY reply
 *	                             100% 可落地 (递归 nofail 五检查 I54).
 *	    CLEANUP_RELEASE full:    cleanup dirty-list (LMON-private,
 *	                             physically separate from reply dirty-list;
 *	                             tick body continuously drains).
 *
 *	  spec-2.16 v0.6 L1.1 nofail 五检查 (I54):
 *	    (a) shmem 预分配固定容量 (compile-time constant)
 *	    (b) bounded ring-buffer (no dynamic resize)
 *	    (c) handler path 禁 palloc / malloc / ereport ERROR / wait
 *	    (d) full → drop oldest + counter
 *	    (e) backend timeout retry converges via GES_REQUEST re-route
 *
 *	  Step 2 (this spec) ships the ring + 5 API + reserved pool + 2
 *	  dirty-list infrastructure.  Step 3 D6 wires LMON tick body drain.
 *	  Step 4 D9 wires backend enqueue.
 *
 *	  Spec: spec-2.16-cross-node-grant-convert-mvp.md (DRAFT v0.1)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_grd_outbound.h
 *
 * NOTES
 *	  pgrac-original file.  All symbols backend-only per L8.
 *	  Step 2 ship:  ring + dirty-list + counters真激活;0 caller
 *	  enqueue path 在本 Step (Step 3/4 wires real producers).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GRD_OUTBOUND_H
#define CLUSTER_GRD_OUTBOUND_H

#ifndef FRONTEND

#include "port/atomics.h"
#include "cluster/cluster_ic_envelope.h"

/*
 * origin_kind — 3 producer paths sharing the same outbound ring with
 * different full-behavior contract per spec-2.16 v0.4 L1.3.
 */
typedef enum ClusterGrdOutboundOrigin {
	CLUSTER_GRD_OUTBOUND_BACKEND_REQUEST = 1, /* backend pre-S4 GES_REQUEST */
	CLUSTER_GRD_OUTBOUND_LMON_REPLY = 2,	  /* work_queue drain reply */
	CLUSTER_GRD_OUTBOUND_CLEANUP_RELEASE = 3, /* LockReleaseAll / abort */
	CLUSTER_GRD_OUTBOUND_LMD_CANCEL
	= 4, /* spec-2.24 D4 — cross-node victim cancel forward (nofail reserved pool + dirty-list) */
	CLUSTER_GRD_OUTBOUND_LMS_NATIVE_PROBE
	= 5 /* spec-2.25 D7 — LMS native-lock probe request + reply (reserved pool + dirty-list nofail) */
} ClusterGrdOutboundOrigin;

/*
 * Compile-time capacity constants (P1.1 nofail 五检查 (a) (b)).
 *
 *   Ring capacity sized to handle worst-case concurrent backend
 *   request burst + reserved reply slots + cleanup release burst.
 *   Conservative: NBackends ≈ 100 (MaxBackends typical) → 200 + 64 + 64.
 *   Final tuning via Step 5 D12 GUC overrides;  Step 2 fixed compile-time.
 */
#define PGRAC_GES_OUTBOUND_RING_CAPACITY 256
#define PGRAC_GES_OUTBOUND_LMON_REPLY_RESERVED_BUDGET 64
#define PGRAC_GES_OUTBOUND_CLEANUP_BUDGET 64

/* dirty-list bounded sizes — separately预分配 reply vs cleanup paths
 * (I54 + I46 — physically separated; do not share buffer). */
#define PGRAC_GES_REPLY_DIRTY_BUDGET 64
#define PGRAC_GES_CLEANUP_DIRTY_BUDGET 64

/* Max payload bytes per ring slot.  GES_REQUEST/REPLY payload = 48B
 * (cluster_ges.h);  pad to 64B for cache-line alignment + forward compat. */
#define PGRAC_GES_OUTBOUND_PAYLOAD_MAX 64

/*
 * Ring slot — fixed 64B payload + envelope metadata.
 *
 *   dest_node_id:  receiver cluster_node_id (single peer; broadcast
 *                  reserved for spec-2.18 LMS)
 *   msg_type:      ClusterICMsgType (GES_REQUEST=4 or GES_REPLY=5)
 *   origin:        ClusterGrdOutboundOrigin (one of 3)
 *   payload_len:   actual bytes valid in payload[]
 *   payload[]:     wire bytes (GesRequestPayload or GesReplyPayload
 *                  serialized image)
 */
typedef struct ClusterGrdOutboundSlot {
	uint32 dest_node_id;
	uint8 msg_type; /* ClusterICMsgType */
	uint8 origin;	/* ClusterGrdOutboundOrigin */
	uint16 payload_len;
	uint8 payload[PGRAC_GES_OUTBOUND_PAYLOAD_MAX];
} ClusterGrdOutboundSlot;

StaticAssertDecl(sizeof(ClusterGrdOutboundSlot) == 72,
				 "ClusterGrdOutboundSlot ABI lock (8 metadata + 64 payload)");

/* Shmem lifecycle */
extern Size cluster_grd_outbound_shmem_size(void);
extern void cluster_grd_outbound_shmem_init(void);
extern void cluster_grd_outbound_shmem_register(void);

/*
 * Enqueue API — 5 producer variants matching 3 origin_kind.
 *
 *   Return:  true if slot inserted into ring or dirty-list (i.e. will
 *            eventually be sent);  false ONLY on caller contract
 *            violation (invalid msg_type / origin / payload_len
 *            > PAYLOAD_MAX);  never false due to "full" — full path
 *            walks reserved → dirty-list → drop oldest per origin.
 *
 *   enqueue_backend_request:  backend producer.  Ring full →
 *                             returns false (caller waits latch +
 *                             timeout per S4).
 *   enqueue_lmon_reply:       LMON producer.  Ring full → reserved
 *                             → reply dirty-list → drop oldest +
 *                             ges_reply_dropped_count++ (NEVER returns
 *                             false; REJECT_BUSY 100% 可落地 contract).
 *   enqueue_cleanup_release:  LockReleaseAll / abort producer.
 *                             Ring full → cleanup dirty-list +
 *                             ges_cleanup_deferred_count++ (NEVER
 *                             returns false).
 */
extern bool cluster_grd_outbound_enqueue_backend_request(uint32 dest_node_id, const void *payload,
														 uint16 payload_len);
extern void cluster_grd_outbound_enqueue_lmon_reply(uint32 dest_node_id, const void *payload,
													uint16 payload_len);
extern void cluster_grd_outbound_enqueue_cleanup_release(uint32 dest_node_id, const void *payload,
														 uint16 payload_len);

/*
 *   enqueue_lmd_cancel:        spec-2.24 D4 — LMD coordinator cross-node
 *                              victim cancel forwarding.  Ring full →
 *                              cleanup dirty-list(complex reliable path,
 *                              复用 cleanup_release pool 语义)+ counter
 *                              ++ (NEVER returns false).
 */
extern void cluster_grd_outbound_enqueue_lmd_cancel(uint32 dest_node_id, const void *payload,
													uint16 payload_len);

/*
 *   enqueue_lms_native_probe:  spec-2.25 D7 — LMS native-lock probe request
 *                              or reply (32B payload).  Used by both LMS
 *                              fan-out (request) and peer reply paths.
 *                              Ring full → cleanup dirty-list (reusing the
 *                              cleanup pool semantics like LMD_CANCEL) +
 *                              native_probe_dirty_count++ (NEVER returns
 *                              false — fail-closed correctness gate at
 *                              the LMS layer relies on dispatch reaching
 *                              the wire).  Producer must CV-broadcast LMON
 *                              after enqueue per L141 family.
 */
extern void cluster_grd_outbound_enqueue_lms_native_probe(uint32 dest_node_id, const void *payload,
														  uint16 payload_len);

/*
 * LMON-side consumer API (Step 3 D6 wires real drain).
 *
 *   dequeue:  pull next slot from ring (FIFO);  returns false on empty.
 *   drain_dirty_lists:  tick body called periodically to drain reply +
 *                       cleanup dirty-lists into ring as capacity allows.
 *                       Returns count drained.
 */
extern bool cluster_grd_outbound_dequeue(ClusterGrdOutboundSlot *out);
extern int cluster_grd_outbound_drain_dirty_lists(void);
extern int cluster_grd_outbound_lmon_drain_send(void);

/* Observability accessor (debug emit_row + view) */
extern uint32 cluster_grd_outbound_ring_depth(void); /* in-ring slot count */
extern uint32 cluster_grd_outbound_reply_dirty_depth(void);
extern uint32 cluster_grd_outbound_cleanup_dirty_depth(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_GRD_OUTBOUND_H */
