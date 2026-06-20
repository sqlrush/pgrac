/*-------------------------------------------------------------------------
 *
 * cluster_grd_work_queue.h
 *	  GES work queue — spec-2.16 D5.
 *
 *	  FIFO queue holding inbound GES_REQUEST work items pending grant
 *	  decision.  Producer:  GES handler (Phase 1, ICMsgType GES_REQUEST
 *	  dispatch — after 5-item inbound validation passes).  Consumer:
 *	  LMON tick body (Phase 2, grant decision callback).
 *
 *	  spec-2.16 v0.4 L1.3 — 3 类 queue full nofail behavior:
 *	    work_queue full → handler 立即 enqueue REJECT_BUSY reply via
 *	    cluster_grd_outbound_enqueue_lmon_reply (reserved pool 保底);
 *	    ges_work_queue_full_count atomic counter++.
 *
 *	  Hot-path (handler) discipline (I46):
 *	    - No palloc / malloc / ereport ERROR / wait.
 *	    - Bounded capacity (compile-time);  full → REJECT_BUSY reply.
 *	    - Single LWLock cluster_grd_work_queue_lock (mostly uncontended).
 *
 *	  Step 2 ship:  queue infrastructure + 3 API + counter.
 *	  Step 3 D6:  handler 产 enqueue;  LMON tick body 出队 grant.
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
 *	  src/include/cluster/cluster_grd_work_queue.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GRD_WORK_QUEUE_H
#define CLUSTER_GRD_WORK_QUEUE_H

#ifndef FRONTEND

#include "port/atomics.h"

/*
 * Work-queue capacity (compile-time).  Sized for typical NBackends *
 * inflight requests; tunable later via GUC.
 */
#define PGRAC_GES_WORK_QUEUE_CAPACITY 256

/*
 * Work item — captures everything LMON tick body needs to make the
 * grant decision (lookup entry → check conflict → enqueue reply).
 *
 *   source_node_id:  envelope source (validated == payload.holder.node_id)
 *   payload:         GesRequestPayload byte image (spec-5.3 D2: 64B)
 */
typedef struct ClusterGrdWorkItem {
	uint32 source_node_id;
	uint16 payload_len;
	uint8 payload[64]; /* GES_REQUEST payload (spec-5.3 D2: exactly 64B) */
} ClusterGrdWorkItem;

StaticAssertDecl(sizeof(ClusterGrdWorkItem) == 72, "ClusterGrdWorkItem ABI lock");

extern Size cluster_grd_work_queue_shmem_size(void);
extern void cluster_grd_work_queue_shmem_init(void);
extern void cluster_grd_work_queue_shmem_register(void);

/*
 * Handler producer API.
 *
 *   Returns true if enqueued;  false on queue full (handler MUST then
 *   enqueue REJECT_BUSY reply via cluster_grd_outbound_enqueue_lmon_reply
 *   + bump ges_work_queue_full_count).  Never blocks / never ERROR.
 */
extern bool cluster_grd_work_queue_enqueue(uint32 source_node_id, const void *payload,
										   uint16 payload_len);

/*
 * LMON consumer API.  Returns false on empty.
 */
extern bool cluster_grd_work_queue_dequeue(ClusterGrdWorkItem *out);

/* Observability accessor */
extern uint32 cluster_grd_work_queue_depth(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_GRD_WORK_QUEUE_H */
