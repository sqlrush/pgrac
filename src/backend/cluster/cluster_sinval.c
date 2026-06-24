/*-------------------------------------------------------------------------
 *
 * cluster_sinval.c
 *	  pgrac cluster SI Broadcaster — outbound/inbound ring buffer + IC handler.
 *
 *	  spec-2.38 D2/D3/D5/D6/D10 skeleton implementation.  See cluster_sinval.h
 *	  for HC132-HC139 contracts and architecture overview.
 *
 *	  This file implements:
 *	    - outbound queue (HC132 防 echo loop;唯一硬防线)
 *	    - inbound queue with try-enqueue (HC133 IC handler nonblocking)
 *	    - cluster_sinval_handle_envelope IC handler (validate-only path)
 *	    - cluster_sinval_enqueue_batch public API (HC134 fail-closed)
 *	    - 9 counter accessors (HC134 observability)
 *
 *	  Drain paths live in this file but have split ownership:
 *	    - outbound fanout is called only from LMON (tier1 fd owner);
 *	    - inbound apply/reset is called only from the SI Broadcaster aux
 *	      process (cluster_sinval_bcast.c main loop).
 *	  Backend / IC handler contexts MUST NOT call drain helpers directly.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_sinval.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-2.38-si-broadcaster-skeleton.md (FROZEN v0.3)
 *	  Design: docs/cache-fusion-protocol-design.md + docs/background-process-
 *	  design.md §3.6.3
 *	  AD-011 (LC/RC Lock 废弃) — SI Broadcast is the 7th independent cluster
 *	  subsystem (与 GCS/GES/GRD/Interconnect/SCN/Reconfiguration 并列)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <signal.h>
#include <unistd.h>

#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_sinval.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/sinvaladt.h" /* SIInsertDataEntries / SInvalShmemSize */
#include "utils/elog.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/* PG core doesn't define USECS_PER_MSEC;  define locally. */
#ifndef USECS_PER_MSEC
#define USECS_PER_MSEC INT64CONST(1000)
#endif

/* Forward decl for spec-2.39 D5 helper (IC handler emit path). */
extern void cluster_sinval_ack_outbound_enqueue(uint64 batch_id, int32 sender_node, uint16 status);
extern bool cluster_sinval_enqueue_batch_with_ack_flag(const SharedInvalidationMessage *msgs, int n,
													   uint64 batch_id);


/* ============================================================
 * Shared memory layout — 2 ring buffer queues + counters + reset flag.
 *
 *	Both outbound and inbound queues share the same struct layout but are
 *	separate shmem regions with separate LWLocks (LWTRANCHE_CLUSTER_SINVAL).
 *	HC132:  outbound queue 独立 — proxy 永不读 PG SI queue。
 *	HC133:  inbound enqueue via LWLockConditionalAcquire only from IC handler.
 * ============================================================ */

typedef struct ClusterSinvalQueueEntry {
	uint64 batch_id;
	int16 nmsgs;
	int16 source_node;
	uint16 flags; /* spec-2.39 D7:  SINVAL_REQUIRES_ACK / SINVAL_RESET_ALL_BROADCAST */
	uint16 pad;
	SharedInvalidationMessage msgs[CLUSTER_SINVAL_BATCH_MAX];
	TimestampTz created_at_ts;
} ClusterSinvalQueueEntry;

typedef struct ClusterSinvalQueue {
	pg_atomic_uint32 head; /* consumer cursor */
	pg_atomic_uint32 tail; /* producer cursor */
	uint32 capacity;
	LWLockPadded lock;
	ClusterSinvalQueueEntry slots[FLEXIBLE_ARRAY_MEMBER];
} ClusterSinvalQueue;

typedef struct ClusterSinvalShared {
	pg_atomic_uint32 inbound_overflow_reset_pending; /* HC134 fail-safe flag */
	pg_atomic_uint32 sinval_bcast_pid;				 /* shared latch wake target */
	pg_atomic_uint32 reset_all_broadcast_pending;	 /* spec-2.39 v0.3 P1:  enqueuer-
													  * side ENQUEUE_FAILED → LMON
													  * broadcasts SINVAL_RESET_ALL_
													  * BROADCAST sentinel */
	pg_atomic_uint64 next_batch_id;					 /* monotone allocator */
	/* 9 spec-2.38 counters + 6 NEW spec-2.39 D8/D9 counters */
	pg_atomic_uint64 broadcast_send_count;
	pg_atomic_uint64 broadcast_receive_count;
	pg_atomic_uint64 inject_local_queue_count;
	pg_atomic_uint64 outbound_queue_full_count;
	pg_atomic_uint64 inbound_queue_full_count;
	pg_atomic_uint64 inbound_overflow_reset_count;
	pg_atomic_uint64 validation_drop_count;
	pg_atomic_uint64 stale_epoch_drop_count;
	pg_atomic_uint64 echo_dropped_count;
	/* spec-2.39 D8:  3 fanout partial-fail counters. */
	pg_atomic_uint64 fanout_would_block_count;
	pg_atomic_uint64 fanout_hard_error_count;
	pg_atomic_uint64 fanout_peer_down_count;
	/* spec-2.39 D9:  3 ack counters. */
	pg_atomic_uint64 ack_received_count;
	pg_atomic_uint64 ack_timeout_count;
	pg_atomic_uint64 ack_orphan_count;
	/* spec-5.2 D1 (G3):  count of SHAREDINVALSMGR_ID messages drained into
	 * the local SI queue from peers.  This is the relsize apply barrier —
	 * once it advances past a baseline, the next lock-acquire on the
	 * relation drops the stale SMgrRelation and re-stats the shared file.
	 * Distinct from broadcast_receive_count, which counts whole batches. */
	pg_atomic_uint64 smgr_inval_applied_count;
} ClusterSinvalShared;


static ClusterSinvalQueue *ClusterSinvalOutbound = NULL;
static ClusterSinvalQueue *ClusterSinvalInbound = NULL;
static ClusterSinvalShared *ClusterSinval = NULL;
static Latch *ClusterSinvalBcastLatch = NULL;


/* ============================================================
 * Shmem region size + init.
 * ============================================================ */

static Size
cluster_sinval_queue_size(int capacity)
{
	return offsetof(ClusterSinvalQueue, slots)
		   + mul_size(sizeof(ClusterSinvalQueueEntry), capacity);
}

Size
cluster_sinval_outbound_shmem_size(void)
{
	Size queue_sz;

	/* Bootstrap mode (initdb) has very small shmem;  skip the large ring
	 * buffer slot allocation and reserve only the ClusterSinvalShared
	 * counters block.  The full queue gets allocated on next postmaster
	 * boot once cluster_enabled is set + max_queue_size GUC honored. */
	if (IsBootstrapProcessingMode() || !cluster_enabled)
		return MAXALIGN(sizeof(ClusterSinvalShared));

	queue_sz = MAXALIGN(cluster_sinval_queue_size(cluster_sinval_broadcast_max_queue_size));
	return add_size(queue_sz, MAXALIGN(sizeof(ClusterSinvalShared)));
}

void
cluster_sinval_outbound_shmem_init(void)
{
	bool found;
	bool bootstrap_or_disabled;
	Size queue_size;
	Size total;
	char *block;

	bootstrap_or_disabled = (IsBootstrapProcessingMode() || !cluster_enabled);
	queue_size = bootstrap_or_disabled
					 ? 0
					 : MAXALIGN(cluster_sinval_queue_size(cluster_sinval_broadcast_max_queue_size));
	total = add_size(queue_size, MAXALIGN(sizeof(ClusterSinvalShared)));

	block = (char *)ShmemInitStruct("ClusterSinvalOutbound", total, &found);
	if (!found) {
		MemSet(block, 0, total);
		if (!bootstrap_or_disabled) {
			ClusterSinvalOutbound = (ClusterSinvalQueue *)block;
			ClusterSinvalOutbound->capacity = cluster_sinval_broadcast_max_queue_size;
			pg_atomic_init_u32(&ClusterSinvalOutbound->head, 0);
			pg_atomic_init_u32(&ClusterSinvalOutbound->tail, 0);
			LWLockInitialize(&ClusterSinvalOutbound->lock.lock, LWTRANCHE_CLUSTER_SINVAL);
		}

		ClusterSinval = (ClusterSinvalShared *)(block + queue_size);
		pg_atomic_init_u32(&ClusterSinval->inbound_overflow_reset_pending, 0);
		pg_atomic_init_u32(&ClusterSinval->sinval_bcast_pid, 0);
		pg_atomic_init_u32(&ClusterSinval->reset_all_broadcast_pending, 0);
		pg_atomic_init_u64(&ClusterSinval->next_batch_id, 1);
		pg_atomic_init_u64(&ClusterSinval->broadcast_send_count, 0);
		pg_atomic_init_u64(&ClusterSinval->broadcast_receive_count, 0);
		pg_atomic_init_u64(&ClusterSinval->inject_local_queue_count, 0);
		pg_atomic_init_u64(&ClusterSinval->outbound_queue_full_count, 0);
		pg_atomic_init_u64(&ClusterSinval->inbound_queue_full_count, 0);
		pg_atomic_init_u64(&ClusterSinval->inbound_overflow_reset_count, 0);
		pg_atomic_init_u64(&ClusterSinval->validation_drop_count, 0);
		pg_atomic_init_u64(&ClusterSinval->stale_epoch_drop_count, 0);
		pg_atomic_init_u64(&ClusterSinval->echo_dropped_count, 0);
		/* spec-2.39 D8:  fanout partial-fail counters. */
		pg_atomic_init_u64(&ClusterSinval->fanout_would_block_count, 0);
		pg_atomic_init_u64(&ClusterSinval->fanout_hard_error_count, 0);
		pg_atomic_init_u64(&ClusterSinval->fanout_peer_down_count, 0);
		/* spec-2.39 D9:  ack counters. */
		pg_atomic_init_u64(&ClusterSinval->ack_received_count, 0);
		pg_atomic_init_u64(&ClusterSinval->ack_timeout_count, 0);
		pg_atomic_init_u64(&ClusterSinval->ack_orphan_count, 0);
		/* spec-5.2 D1:  relsize apply barrier counter. */
		pg_atomic_init_u64(&ClusterSinval->smgr_inval_applied_count, 0);
	} else {
		if (!bootstrap_or_disabled)
			ClusterSinvalOutbound = (ClusterSinvalQueue *)block;
		ClusterSinval = (ClusterSinvalShared *)(block + queue_size);
	}
}

Size
cluster_sinval_inbound_shmem_size(void)
{
	if (IsBootstrapProcessingMode() || !cluster_enabled)
		return 0;
	return MAXALIGN(cluster_sinval_queue_size(cluster_sinval_broadcast_max_queue_size));
}

void
cluster_sinval_inbound_shmem_init(void)
{
	bool found;
	Size size;

	if (IsBootstrapProcessingMode() || !cluster_enabled)
		return;

	size = MAXALIGN(cluster_sinval_queue_size(cluster_sinval_broadcast_max_queue_size));
	ClusterSinvalInbound
		= (ClusterSinvalQueue *)ShmemInitStruct("ClusterSinvalInbound", size, &found);
	if (!found) {
		MemSet(ClusterSinvalInbound, 0, size);
		ClusterSinvalInbound->capacity = cluster_sinval_broadcast_max_queue_size;
		pg_atomic_init_u32(&ClusterSinvalInbound->head, 0);
		pg_atomic_init_u32(&ClusterSinvalInbound->tail, 0);
		LWLockInitialize(&ClusterSinvalInbound->lock.lock, LWTRANCHE_CLUSTER_SINVAL);
	}
}


/* ============================================================
 * Ring buffer helpers (caller holds queue->lock).
 * ============================================================ */

static inline bool
queue_is_full_locked(ClusterSinvalQueue *q)
{
	uint32 tail = pg_atomic_read_u32(&q->tail);
	uint32 head = pg_atomic_read_u32(&q->head);

	return ((tail + 1) % q->capacity) == head;
}

static inline bool
queue_is_empty_locked(ClusterSinvalQueue *q)
{
	return pg_atomic_read_u32(&q->tail) == pg_atomic_read_u32(&q->head);
}


/* ============================================================
 * HC134 + HC139:  outbound public API.
 *
 *	Backend / test inject path enters here.  Returns false on queue full
 *	(fail-closed);  caller responsibility to handle:  ereport 53R94,
 *	fallback RESET, or log (test only).
 * ============================================================ */
bool
cluster_sinval_enqueue_batch(const SharedInvalidationMessage *msgs, int n)
{
	uint32 tail;
	ClusterSinvalQueueEntry *slot;
	int i;

	if (msgs == NULL || n <= 0 || n > CLUSTER_SINVAL_BATCH_MAX || ClusterSinvalOutbound == NULL)
		return false;

	LWLockAcquire(&ClusterSinvalOutbound->lock.lock, LW_EXCLUSIVE);

	if (queue_is_full_locked(ClusterSinvalOutbound)) {
		LWLockRelease(&ClusterSinvalOutbound->lock.lock);
		pg_atomic_fetch_add_u64(&ClusterSinval->outbound_queue_full_count, 1);
		return false;
	}

	tail = pg_atomic_read_u32(&ClusterSinvalOutbound->tail);
	slot = &ClusterSinvalOutbound->slots[tail];

	slot->batch_id = pg_atomic_fetch_add_u64(&ClusterSinval->next_batch_id, 1);
	slot->nmsgs = (int16)n;
	slot->source_node = (int16)cluster_node_id;
	slot->flags = 0;
	slot->pad = 0;
	for (i = 0; i < n; i++)
		slot->msgs[i] = msgs[i];
	slot->created_at_ts = GetCurrentTimestamp();

	pg_atomic_write_u32(&ClusterSinvalOutbound->tail, (tail + 1) % ClusterSinvalOutbound->capacity);
	LWLockRelease(&ClusterSinvalOutbound->lock.lock);

	/*
	 * Outbound fanout is LMON-owned because LMON owns the process-local
	 * tier1 TCP fds.  Wake LMON immediately; its main loop drains the
	 * outbound queue via cluster_ic_send_envelope_fanout().
	 */
	cluster_lmon_wakeup();
	return true;
}

/*
 * spec-2.39 D2 internal helper:  enqueue with pre-assigned batch_id +
 * SINVAL_REQUIRES_ACK flag.  Used by cluster_sinval_enqueue_and_wait_ack
 * to correlate the outbound batch with the ack_wait HTAB entry.
 */
bool
cluster_sinval_enqueue_batch_with_ack_flag(const SharedInvalidationMessage *msgs, int n,
										   uint64 batch_id)
{
	uint32 tail;
	ClusterSinvalQueueEntry *slot;
	int i;

	if (msgs == NULL || n <= 0 || n > CLUSTER_SINVAL_BATCH_MAX || ClusterSinvalOutbound == NULL)
		return false;

	LWLockAcquire(&ClusterSinvalOutbound->lock.lock, LW_EXCLUSIVE);
	if (queue_is_full_locked(ClusterSinvalOutbound)) {
		LWLockRelease(&ClusterSinvalOutbound->lock.lock);
		pg_atomic_fetch_add_u64(&ClusterSinval->outbound_queue_full_count, 1);
		return false;
	}

	tail = pg_atomic_read_u32(&ClusterSinvalOutbound->tail);
	slot = &ClusterSinvalOutbound->slots[tail];
	slot->batch_id = batch_id;
	slot->nmsgs = (int16)n;
	slot->source_node = (int16)cluster_node_id;
	slot->flags = SINVAL_REQUIRES_ACK;
	slot->pad = 0;
	for (i = 0; i < n; i++)
		slot->msgs[i] = msgs[i];
	slot->created_at_ts = GetCurrentTimestamp();
	pg_atomic_write_u32(&ClusterSinvalOutbound->tail, (tail + 1) % ClusterSinvalOutbound->capacity);
	LWLockRelease(&ClusterSinvalOutbound->lock.lock);

	cluster_lmon_wakeup();
	return true;
}


/* ============================================================
 * HC133:  inbound nonblocking try-enqueue.  IC handler ONLY.
 *
 *	Uses LWLockConditionalAcquire.  Returns false if lock busy OR ring full;
 *	caller (IC handler) sets inbound_overflow_reset_pending and lets SI
 *	Broadcaster aux proc apply SIResetAll() as fail-safe (HC134).
 * ============================================================ */
bool
cluster_sinval_inbound_try_enqueue(uint64 batch_id, const SharedInvalidationMessage *msgs, int n,
								   int32 source_node)
{
	uint32 tail;
	ClusterSinvalQueueEntry *slot;
	int i;

	if (msgs == NULL || n <= 0 || n > CLUSTER_SINVAL_BATCH_MAX || ClusterSinvalInbound == NULL)
		return false;

	if (!LWLockConditionalAcquire(&ClusterSinvalInbound->lock.lock, LW_EXCLUSIVE))
		return false;

	if (queue_is_full_locked(ClusterSinvalInbound)) {
		LWLockRelease(&ClusterSinvalInbound->lock.lock);
		return false;
	}

	tail = pg_atomic_read_u32(&ClusterSinvalInbound->tail);
	slot = &ClusterSinvalInbound->slots[tail];

	slot->batch_id = batch_id;
	slot->nmsgs = (int16)n;
	slot->source_node = (int16)source_node;
	for (i = 0; i < n; i++)
		slot->msgs[i] = msgs[i];
	slot->created_at_ts = GetCurrentTimestamp();

	pg_atomic_write_u32(&ClusterSinvalInbound->tail, (tail + 1) % ClusterSinvalInbound->capacity);
	LWLockRelease(&ClusterSinvalInbound->lock.lock);

	return true;
}


/* ============================================================
 * D5:  IC inbound handler — validate-only path.
 *
 *	HC133:  nonblocking;  no LWLockAcquire (only LWLockConditionalAcquire
 *	via try-enqueue);  no SIInsertDataEntries (deferred to aux process).
 *
 *	HC135:  source_node ≠ self envelope-level echo defense (auxiliary;
 *	HC132 outbound queue 独立 is the only hard echo防线).
 * ============================================================ */
static void
cluster_sinval_handle_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const SinvalBroadcastHeader *hdr = (const SinvalBroadcastHeader *)payload;
	const SharedInvalidationMessage *msgs;
	uint64 current_epoch;

	if (ClusterSinval == NULL)
		return;

	/* spec-2.38 D14:  fault inject — SKIP bypasses validation entirely.
	 * Used by TAP to reproduce HC135 echo-defense violation scenarios. */
	CLUSTER_INJECTION_POINT("cluster-sinval-receive-skip-validate");
	if (cluster_injection_should_skip("cluster-sinval-receive-skip-validate")) {
		msgs = (const SharedInvalidationMessage *)((const char *)hdr + sizeof(*hdr));
		if (cluster_sinval_inbound_try_enqueue(hdr->batch_id, msgs, hdr->nmsgs, hdr->source_node))
			pg_atomic_fetch_add_u64(&ClusterSinval->broadcast_receive_count, 1);
		cluster_sinval_set_proc_latch();
		return;
	}

	if (env->payload_length < (uint32)sizeof(SinvalBroadcastHeader)) {
		pg_atomic_fetch_add_u64(&ClusterSinval->validation_drop_count, 1);
		return;
	}

	/* spec-2.39 v0.3 P1:  RESET-all broadcast sentinel — nmsgs MUST == 0 +
	 * payload_length MUST == sizeof(header).  No tail validation, no ACK,
	 * no inbound enqueue;  remote直调 SIResetAll fail-safe.  REQUIRES_ACK
	 * 与本 flag 互斥 (wire ABI 强制). */
	if ((hdr->flags & SINVAL_RESET_ALL_BROADCAST) != 0) {
		if ((hdr->flags & SINVAL_REQUIRES_ACK) != 0 || (hdr->flags & ~SINVAL_KNOWN_FLAGS) != 0
			|| hdr->nmsgs != 0 || env->payload_length != (uint32)sizeof(SinvalBroadcastHeader)) {
			pg_atomic_fetch_add_u64(&ClusterSinval->validation_drop_count, 1);
			return;
		}
		current_epoch = cluster_epoch_get_current();
		if (hdr->epoch < current_epoch) {
			pg_atomic_fetch_add_u64(&ClusterSinval->stale_epoch_drop_count, 1);
			return;
		}
		if (hdr->source_node < 0 || hdr->source_node >= CLUSTER_MAX_NODES
			|| hdr->source_node == cluster_node_id
			|| env->source_node_id != (uint32)hdr->source_node
			|| cluster_conf_lookup_node(hdr->source_node) == NULL) {
			pg_atomic_fetch_add_u64(&ClusterSinval->validation_drop_count, 1);
			return;
		}
		/* Fail-safe propagation:  ask SinvalBcast aux to do SIResetAll
		 * via the existing spec-2.38 inbound_overflow_reset_pending path
		 * (reuse,  not加 new counter). */
		pg_atomic_write_u32(&ClusterSinval->inbound_overflow_reset_pending, 1);
		pg_atomic_fetch_add_u64(&ClusterSinval->inbound_overflow_reset_count, 1);
		cluster_sinval_set_proc_latch();
		return;
	}

	if (hdr->nmsgs == 0 || hdr->nmsgs > CLUSTER_SINVAL_BATCH_MAX) {
		pg_atomic_fetch_add_u64(&ClusterSinval->validation_drop_count, 1);
		return;
	}
	if (env->payload_length
		!= (uint32)(sizeof(SinvalBroadcastHeader)
					+ hdr->nmsgs * sizeof(SharedInvalidationMessage))) {
		pg_atomic_fetch_add_u64(&ClusterSinval->validation_drop_count, 1);
		return;
	}
	/* spec-2.39 D7 (v0.3 P1):  reject unknown flag bits;known bits =
	 * SINVAL_KNOWN_FLAGS (REQUIRES_ACK | RESET_ALL_BROADCAST).  Bound
	 * checks + envelope source_node consistency + declared membership. */
	if ((hdr->flags & ~SINVAL_KNOWN_FLAGS) != 0 || hdr->source_node < 0
		|| hdr->source_node >= CLUSTER_MAX_NODES || env->source_node_id != (uint32)hdr->source_node
		|| cluster_conf_lookup_node(hdr->source_node) == NULL) {
		pg_atomic_fetch_add_u64(&ClusterSinval->validation_drop_count, 1);
		return;
	}

	/* HC100 epoch check */
	current_epoch = cluster_epoch_get_current();
	if (hdr->epoch < current_epoch) {
		pg_atomic_fetch_add_u64(&ClusterSinval->stale_epoch_drop_count, 1);
		return;
	}

	/* HC135 envelope-level echo defense */
	if (hdr->source_node == cluster_node_id) {
		pg_atomic_fetch_add_u64(&ClusterSinval->echo_dropped_count, 1);
		return;
	}

	msgs = (const SharedInvalidationMessage *)((const char *)hdr + sizeof(*hdr));

	/* HC133 nonblocking try-enqueue;  failure → fail-safe SIResetAll
	 * by aux process (set flag,  no silent stale).
	 *
	 * spec-2.39 D5 + v0.3 P2:  if SINVAL_REQUIRES_ACK is set, emit ACK
	 * envelope (status DONE on success, RESET_PENDING on inbound full).
	 * Sender views both DONE and RESET_PENDING as fulfilled (HC141). */
	{
		bool enq_ok
			= cluster_sinval_inbound_try_enqueue(hdr->batch_id, msgs, hdr->nmsgs, hdr->source_node);

		if (!enq_ok) {
			pg_atomic_fetch_add_u64(&ClusterSinval->inbound_queue_full_count, 1);
			pg_atomic_write_u32(&ClusterSinval->inbound_overflow_reset_pending, 1);
		} else {
			pg_atomic_fetch_add_u64(&ClusterSinval->broadcast_receive_count, 1);
		}

		if ((hdr->flags & SINVAL_REQUIRES_ACK) != 0) {
			ClusterSinvalAckStatus ack_status = enq_ok ? SINVAL_ACK_DONE : SINVAL_ACK_RESET_PENDING;
			cluster_sinval_ack_outbound_enqueue(hdr->batch_id, hdr->source_node, ack_status);
		}
	}

	cluster_sinval_set_proc_latch();
}


/* ============================================================
 * D4:  aux process drain helpers — called from SI Broadcaster main loop.
 * ============================================================ */
void
cluster_sinval_drain_outbound_and_broadcast(void)
{
	int drained = 0;
	int batch_limit = cluster_sinval_broadcast_batch_size;

	if (ClusterSinvalOutbound == NULL)
		return;

	if (MyBackendType != B_LMON)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster_sinval_drain_outbound_and_broadcast: callable only "
							   "from LMON process context"),
						errhint("SINVAL outbound broadcast must be LMON-mediated because tier1 "
								"TCP file descriptors are LMON process-local.")));

	while (drained < batch_limit) {
		ClusterSinvalQueueEntry local;
		uint32 head;
		SinvalBroadcastHeader hdr;
		Size payload_len;
		char *buf;
		ClusterICFanoutResult per_peer[CLUSTER_MAX_NODES];
		int peer;
		bool sent_any = false;

		LWLockAcquire(&ClusterSinvalOutbound->lock.lock, LW_EXCLUSIVE);
		if (queue_is_empty_locked(ClusterSinvalOutbound)) {
			LWLockRelease(&ClusterSinvalOutbound->lock.lock);
			break;
		}
		head = pg_atomic_read_u32(&ClusterSinvalOutbound->head);
		local = ClusterSinvalOutbound->slots[head];
		pg_atomic_write_u32(&ClusterSinvalOutbound->head,
							(head + 1) % ClusterSinvalOutbound->capacity);
		LWLockRelease(&ClusterSinvalOutbound->lock.lock);

		memset(&hdr, 0, sizeof(hdr));
		hdr.batch_id = local.batch_id;
		hdr.epoch = cluster_epoch_get_current();
		hdr.source_node = cluster_node_id;
		hdr.nmsgs = (uint16)local.nmsgs;
		hdr.flags = local.flags; /* spec-2.39 D7:  carry REQUIRES_ACK / RESET_ALL_BROADCAST */

		payload_len
			= sizeof(SinvalBroadcastHeader) + local.nmsgs * sizeof(SharedInvalidationMessage);
		buf = (char *)palloc0(payload_len);
		memcpy(buf, &hdr, sizeof(hdr));
		if (local.nmsgs > 0)
			memcpy(buf + sizeof(hdr), local.msgs, local.nmsgs * sizeof(SharedInvalidationMessage));

		/* spec-2.38 D14:  fault inject — SKIP makes broadcaster silently
		 * drop the batch (still consumes from outbound queue for fast-
		 * forward semantics) and bumps echo_dropped_count for TAP
		 * observability without surfacing failure to caller. */
		CLUSTER_INJECTION_POINT("cluster-sinval-broadcast-drop-send");
		if (cluster_injection_should_skip("cluster-sinval-broadcast-drop-send")) {
			pg_atomic_fetch_add_u64(&ClusterSinval->echo_dropped_count, 1);
			pfree(buf);
			drained++;
			continue;
		}

		cluster_ic_send_envelope_fanout(PGRAC_IC_MSG_SINVAL, buf, payload_len, per_peer);
		/* spec-2.39 D8:  3-way partial-fail counter classification. */
		for (peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
			switch (per_peer[peer]) {
			case CLUSTER_IC_FANOUT_DONE:
				sent_any = true;
				break;
			case CLUSTER_IC_FANOUT_WOULD_BLOCK:
				pg_atomic_fetch_add_u64(&ClusterSinval->fanout_would_block_count, 1);
				break;
			case CLUSTER_IC_FANOUT_HARD_ERROR:
				pg_atomic_fetch_add_u64(&ClusterSinval->fanout_hard_error_count, 1);
				break;
			case CLUSTER_IC_FANOUT_PEER_DOWN:
				/* PEER_DOWN includes self + non-declared peers — only count
				 * declared peers via cluster_conf_lookup_node check. */
				if (peer != cluster_node_id && cluster_conf_lookup_node(peer) != NULL)
					pg_atomic_fetch_add_u64(&ClusterSinval->fanout_peer_down_count, 1);
				break;
			}
		}
		if (sent_any)
			pg_atomic_fetch_add_u64(&ClusterSinval->broadcast_send_count, 1);

		pfree(buf);
		drained++;
	}
}

void
cluster_sinval_drain_inbound_and_apply(void)
{
	int drained = 0;
	int batch_limit = cluster_sinval_broadcast_batch_size;

	if (ClusterSinvalInbound == NULL)
		return;

	while (drained < batch_limit) {
		ClusterSinvalQueueEntry local;
		uint32 head;

		LWLockAcquire(&ClusterSinvalInbound->lock.lock, LW_EXCLUSIVE);
		if (queue_is_empty_locked(ClusterSinvalInbound)) {
			LWLockRelease(&ClusterSinvalInbound->lock.lock);
			break;
		}
		head = pg_atomic_read_u32(&ClusterSinvalInbound->head);
		local = ClusterSinvalInbound->slots[head];
		pg_atomic_write_u32(&ClusterSinvalInbound->head,
							(head + 1) % ClusterSinvalInbound->capacity);
		LWLockRelease(&ClusterSinvalInbound->lock.lock);

		/* aux process context — LWLock OK; SIInsertDataEntries can take
		 * SInvalWriteLock and trigger reset broadcast internally. */
		SendSharedInvalidMessages(local.msgs, local.nmsgs);
		pg_atomic_fetch_add_u64(&ClusterSinval->inject_local_queue_count, 1);

		/* spec-5.2 D1 (G3):  advance the relsize apply barrier for every
		 * SHAREDINVALSMGR_ID message that just entered the local SI queue.
		 * Backends pick these up on their next AcceptInvalidationMessages
		 * (lock acquire) and re-stat the shared file. */
		{
			int i;

			for (i = 0; i < local.nmsgs; i++)
				if (local.msgs[i].id == SHAREDINVALSMGR_ID)
					pg_atomic_fetch_add_u64(&ClusterSinval->smgr_inval_applied_count, 1);
		}

		drained++;
	}
}

void
cluster_sinval_apply_inbound_overflow_reset_if_pending(void)
{
	uint32 pending;

	if (ClusterSinval == NULL)
		return;

	pending = pg_atomic_read_u32(&ClusterSinval->inbound_overflow_reset_pending);
	if (pending != 0) {
		pg_atomic_write_u32(&ClusterSinval->inbound_overflow_reset_pending, 0);
		/* Fail-safe local cache reset (HC134) — backend AcceptInvalidation-
		 * Messages will reset its local caches on next lock acquire. */
		SIResetAll();
		pg_atomic_fetch_add_u64(&ClusterSinval->inbound_overflow_reset_count, 1);
	}
}


/* ============================================================
 * Shmem region registration — registered with the cluster shmem registry.
 * ============================================================ */

static const ClusterShmemRegion cluster_sinval_outbound_region = {
	.name = "pgrac cluster sinval outbound",
	.size_fn = cluster_sinval_outbound_shmem_size,
	.init_fn = cluster_sinval_outbound_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_sinval",
	.reserved_flags = 0,
};

static const ClusterShmemRegion cluster_sinval_inbound_region = {
	.name = "pgrac cluster sinval inbound",
	.size_fn = cluster_sinval_inbound_shmem_size,
	.init_fn = cluster_sinval_inbound_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_sinval",
	.reserved_flags = 0,
};

/* spec-2.39 D3:  ack_wait HTAB shmem region. */
static const ClusterShmemRegion cluster_sinval_ack_wait_region = {
	.name = "pgrac cluster sinval ack wait",
	.size_fn = cluster_sinval_ack_wait_shmem_size,
	.init_fn = cluster_sinval_ack_wait_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_sinval",
	.reserved_flags = 0,
};

/* spec-2.39 D5:  ack_outbound ring shmem region. */
static const ClusterShmemRegion cluster_sinval_ack_outbound_region = {
	.name = "pgrac cluster sinval ack outbound",
	.size_fn = cluster_sinval_ack_outbound_shmem_size,
	.init_fn = cluster_sinval_ack_outbound_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_sinval",
	.reserved_flags = 0,
};

void
cluster_sinval_module_init(void)
{
	cluster_shmem_register_region(&cluster_sinval_outbound_region);
	cluster_shmem_register_region(&cluster_sinval_inbound_region);
	cluster_shmem_register_region(&cluster_sinval_ack_wait_region);
	cluster_shmem_register_region(&cluster_sinval_ack_outbound_region);
}


/* ============================================================
 * IC msg type registration (HC139 producer mask).
 * ============================================================ */
static const ClusterICMsgTypeInfo cluster_sinval_msg_info = {
	.msg_type = PGRAC_IC_MSG_SINVAL,
	.name = "cluster_sinval",
	.allowed_producer_mask = CLUSTER_IC_PRODUCER_SINVAL_FANOUT,
	.broadcast_ok = true,
	.handler = cluster_sinval_handle_envelope,
};

/* spec-2.39 D4 + D6:  SINVAL_ACK msg_type 19 dispatch table entry. */
static const ClusterICMsgTypeInfo cluster_sinval_ack_msg_info = {
	.msg_type = PGRAC_IC_MSG_SINVAL_ACK,
	.name = "cluster_sinval_ack",
	.allowed_producer_mask = CLUSTER_IC_PRODUCER_SINVAL_ACK,
	.broadcast_ok = false, /* single-peer (sender) not fanout */
	.handler = cluster_sinval_handle_ack_envelope,
};

void
cluster_sinval_register_msg_type(void)
{
	cluster_ic_register_msg_type(&cluster_sinval_msg_info);
	cluster_ic_register_msg_type(&cluster_sinval_ack_msg_info);
}


/* ============================================================
 * Latch wake-up — used by enqueue / IC handler.
 * ============================================================ */
void
cluster_sinval_register_proc_latch(Latch *latch)
{
	ClusterSinvalBcastLatch = latch;
	if (ClusterSinval != NULL)
		pg_atomic_write_u32(&ClusterSinval->sinval_bcast_pid, (uint32)MyProcPid);
}

void
cluster_sinval_unregister_proc_latch(void)
{
	if (ClusterSinval != NULL
		&& (pid_t)pg_atomic_read_u32(&ClusterSinval->sinval_bcast_pid) == MyProcPid)
		pg_atomic_write_u32(&ClusterSinval->sinval_bcast_pid, 0);
	if (ClusterSinvalBcastLatch != NULL)
		ClusterSinvalBcastLatch = NULL;
}

/*
 * cluster_sinval_set_proc_latch -- wake SinvalBcast aux process.
 *
 *	In-process(SinvalBcast itself):  SetLatch(local latch)直接走。
 *	Cross-process(LMON / IC handler context / backend after enqueue_batch
 *	had previously woken SinvalBcast directly — now wakes LMON instead):
 *	  从 shmem 读 sinval_bcast_pid + kill(SIGUSR1)。Wake 依赖
 *	  procsignal_sigusr1_handler 末尾 SetLatch(MyLatch) 兜底
 *	  (src/backend/storage/ipc/procsignal.c:711),**不依赖** ProcSignalInit
 *	  slot —— SinvalBcast 故意跳过 ProcSignalInit(spec-2.18 LMS / 2.19 LMD
 *	  / 2.38 SinvalBcast 同款 lifecycle 简化),CheckProcSignal 各分支因
 *	  MyProcSignalSlot=NULL 自动 no-op,handler 末尾 SetLatch(MyLatch) 仍
 *	  fire → WaitLatch 解阻。Hardening v1.0.1 L172 family。
 */
void
cluster_sinval_set_proc_latch(void)
{
	pid_t pid;

	if (ClusterSinvalBcastLatch != NULL) {
		SetLatch(ClusterSinvalBcastLatch);
		return;
	}

	if (ClusterSinval == NULL)
		return;

	pid = (pid_t)pg_atomic_read_u32(&ClusterSinval->sinval_bcast_pid);
	if (pid > 0 && pid != MyProcPid)
		(void)kill(pid, SIGUSR1);
}


/* ============================================================
 * D10:  counter accessors — exposed via dump_sinval (cluster_debug.c).
 * ============================================================ */
uint64
cluster_sinval_get_broadcast_send_count(void)
{
	return ClusterSinval ? pg_atomic_read_u64(&ClusterSinval->broadcast_send_count) : 0;
}
uint64
cluster_sinval_get_broadcast_receive_count(void)
{
	return ClusterSinval ? pg_atomic_read_u64(&ClusterSinval->broadcast_receive_count) : 0;
}
uint64
cluster_sinval_get_inject_local_queue_count(void)
{
	return ClusterSinval ? pg_atomic_read_u64(&ClusterSinval->inject_local_queue_count) : 0;
}
uint64
cluster_sinval_get_outbound_queue_full_count(void)
{
	return ClusterSinval ? pg_atomic_read_u64(&ClusterSinval->outbound_queue_full_count) : 0;
}
uint64
cluster_sinval_get_inbound_queue_full_count(void)
{
	return ClusterSinval ? pg_atomic_read_u64(&ClusterSinval->inbound_queue_full_count) : 0;
}
uint64
cluster_sinval_get_inbound_overflow_reset_count(void)
{
	return ClusterSinval ? pg_atomic_read_u64(&ClusterSinval->inbound_overflow_reset_count) : 0;
}
uint64
cluster_sinval_get_validation_drop_count(void)
{
	return ClusterSinval ? pg_atomic_read_u64(&ClusterSinval->validation_drop_count) : 0;
}
uint64
cluster_sinval_get_stale_epoch_drop_count(void)
{
	return ClusterSinval ? pg_atomic_read_u64(&ClusterSinval->stale_epoch_drop_count) : 0;
}
uint64
cluster_sinval_get_echo_dropped_count(void)
{
	return ClusterSinval ? pg_atomic_read_u64(&ClusterSinval->echo_dropped_count) : 0;
}

/* ============================================================
 * spec-2.39 D8/D9:  6 NEW counter accessors (3 fanout + 3 ack).
 * ============================================================ */
uint64
cluster_sinval_get_fanout_would_block_count(void)
{
	return ClusterSinval ? pg_atomic_read_u64(&ClusterSinval->fanout_would_block_count) : 0;
}
uint64
cluster_sinval_get_fanout_hard_error_count(void)
{
	return ClusterSinval ? pg_atomic_read_u64(&ClusterSinval->fanout_hard_error_count) : 0;
}
uint64
cluster_sinval_get_fanout_peer_down_count(void)
{
	return ClusterSinval ? pg_atomic_read_u64(&ClusterSinval->fanout_peer_down_count) : 0;
}
uint64
cluster_sinval_get_ack_received_count(void)
{
	return ClusterSinval ? pg_atomic_read_u64(&ClusterSinval->ack_received_count) : 0;
}
uint64
cluster_sinval_get_ack_timeout_count(void)
{
	return ClusterSinval ? pg_atomic_read_u64(&ClusterSinval->ack_timeout_count) : 0;
}
uint64
cluster_sinval_get_ack_orphan_count(void)
{
	return ClusterSinval ? pg_atomic_read_u64(&ClusterSinval->ack_orphan_count) : 0;
}

uint64
cluster_sinval_get_smgr_inval_applied_count(void)
{
	return ClusterSinval ? pg_atomic_read_u64(&ClusterSinval->smgr_inval_applied_count) : 0;
}

/*
 * spec-5.2 D1 — is the outbound broadcast path attached in this process?
 *
 *	cluster_sinval_enqueue_batch() returns false BOTH when the outbound
 *	queue is genuinely full AND when it is simply not attached (single
 *	node, bootstrap, cluster disabled).  Callers that must fail-closed on
 *	a full queue (e.g. cluster_smgr_invalidate_relation, H2) use this
 *	predicate first to tell the two apart — a NULL outbound is a benign
 *	"no peers to tell", not a backpressure failure.
 */
bool
cluster_sinval_is_active(void)
{
	return ClusterSinvalOutbound != NULL;
}

/*
 * spec-5.2 D1 (H2):  public request for a coarse RESET-all broadcast.
 *
 *	Called by cluster_smgr_invalidate_relation() when an outbound relsize
 *	invalidation cannot be enqueued (queue full) inside a critical section,
 *	where ereport(ERROR) is not allowed.  Sets the enqueuer-side pending
 *	flag and wakes LMON, which fans out the SINVAL_RESET_ALL_BROADCAST
 *	sentinel (same mechanism the spec-2.39 ack path uses).  NULL-guarded so
 *	pre-shmem / cluster-disabled call sites are safe no-ops.
 */
void
cluster_sinval_request_reset_all_broadcast(void)
{
	if (ClusterSinval == NULL)
		return;
	pg_atomic_write_u32(&ClusterSinval->reset_all_broadcast_pending, 1);
	cluster_lmon_wakeup();
}


/* ============================================================
 * spec-2.39 D3 + D5:  ack_wait HTAB + ack_outbound ring.
 * ============================================================ */

#include "cluster/cluster_cssd.h" /* cluster_cssd_get_peer_state */
#include "storage/standby.h"	  /* MyBackendId not actually needed but keeps header chain */

typedef struct ClusterSinvalAckWaitEntry {
	uint64 batch_id; /* HTAB key */
	int32 enqueuer_pgprocno;
	uint32 alive_peer_mask;
	uint32 ack_received_mask;
	TimestampTz deadline_us;
	uint16 status; /* ClusterSinvalAckStatus aggregate (DONE if all DONE) */
	uint16 completion_signaled;
} ClusterSinvalAckWaitEntry;

typedef struct ClusterSinvalAckOutboundEntry {
	uint64 batch_id;
	int32 sender_node; /* peer to ACK back */
	uint16 status;	   /* ClusterSinvalAckStatus */
	uint16 pad;
} ClusterSinvalAckOutboundEntry;

typedef struct ClusterSinvalAckOutboundRing {
	pg_atomic_uint32 head;
	pg_atomic_uint32 tail;
	uint32 capacity;
	LWLockPadded lock;
	ClusterSinvalAckOutboundEntry slots[FLEXIBLE_ARRAY_MEMBER];
} ClusterSinvalAckOutboundRing;

#define CLUSTER_SINVAL_ACK_OUTBOUND_CAPACITY 64

static HTAB *ClusterSinvalAckWaitHTAB = NULL;
static LWLock *ClusterSinvalAckWaitLock = NULL;
static ClusterSinvalAckOutboundRing *ClusterSinvalAckOutbound = NULL;


Size
cluster_sinval_ack_wait_shmem_size(void)
{
	if (IsBootstrapProcessingMode() || !cluster_enabled)
		return 0;
	return hash_estimate_size(cluster_sinval_ack_wait_slots, sizeof(ClusterSinvalAckWaitEntry))
		   + MAXALIGN(sizeof(LWLockPadded));
}

void
cluster_sinval_ack_wait_shmem_init(void)
{
	HASHCTL info;
	bool found;
	LWLockPadded *lockblock;

	if (IsBootstrapProcessingMode() || !cluster_enabled)
		return;

	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(uint64);
	info.entrysize = sizeof(ClusterSinvalAckWaitEntry);
	info.num_partitions = 1;
	ClusterSinvalAckWaitHTAB
		= ShmemInitHash("ClusterSinvalAckWait", cluster_sinval_ack_wait_slots,
						cluster_sinval_ack_wait_slots, &info, HASH_ELEM | HASH_BLOBS);

	lockblock = (LWLockPadded *)ShmemInitStruct("ClusterSinvalAckWaitLock",
												MAXALIGN(sizeof(LWLockPadded)), &found);
	if (!found) {
		LWLockInitialize(&lockblock->lock, LWTRANCHE_CLUSTER_SINVAL);
	}
	ClusterSinvalAckWaitLock = &lockblock->lock;
}

static Size
cluster_sinval_ack_outbound_struct_size(int capacity)
{
	return offsetof(ClusterSinvalAckOutboundRing, slots)
		   + mul_size(sizeof(ClusterSinvalAckOutboundEntry), capacity);
}

Size
cluster_sinval_ack_outbound_shmem_size(void)
{
	if (IsBootstrapProcessingMode() || !cluster_enabled)
		return 0;
	return MAXALIGN(cluster_sinval_ack_outbound_struct_size(CLUSTER_SINVAL_ACK_OUTBOUND_CAPACITY));
}

void
cluster_sinval_ack_outbound_shmem_init(void)
{
	bool found;
	Size size;

	if (IsBootstrapProcessingMode() || !cluster_enabled)
		return;

	size = MAXALIGN(cluster_sinval_ack_outbound_struct_size(CLUSTER_SINVAL_ACK_OUTBOUND_CAPACITY));
	ClusterSinvalAckOutbound
		= (ClusterSinvalAckOutboundRing *)ShmemInitStruct("ClusterSinvalAckOutbound", size, &found);
	if (!found) {
		MemSet(ClusterSinvalAckOutbound, 0, size);
		ClusterSinvalAckOutbound->capacity = CLUSTER_SINVAL_ACK_OUTBOUND_CAPACITY;
		pg_atomic_init_u32(&ClusterSinvalAckOutbound->head, 0);
		pg_atomic_init_u32(&ClusterSinvalAckOutbound->tail, 0);
		LWLockInitialize(&ClusterSinvalAckOutbound->lock.lock, LWTRANCHE_CLUSTER_SINVAL);
	}
}


/*
 * cluster_sinval_ack_outbound_enqueue -- IC handler context (D5).
 *
 *   Enqueue an ACK entry for LMON drain to fanout-single-peer.  Best-
 *   effort:  if ring full, drop ACK silently (sender will time out;
 *   acceptable since sender already has WARN 53R95 path).  Bumps
 *   spec-2.38 echo_dropped_count by 0 (uses validation_drop_count for
 *   self-emit overflow if needed).  Pattern matches inbound_try_enqueue
 *   nonblocking constraint (HC133 family).
 */
void
cluster_sinval_ack_outbound_enqueue(uint64 batch_id, int32 sender_node, uint16 status)
{
	uint32 tail, head, next_tail;

	if (ClusterSinvalAckOutbound == NULL || ClusterSinval == NULL)
		return;

	if (!LWLockConditionalAcquire(&ClusterSinvalAckOutbound->lock.lock, LW_EXCLUSIVE))
		return; /* best-effort drop */

	tail = pg_atomic_read_u32(&ClusterSinvalAckOutbound->tail);
	head = pg_atomic_read_u32(&ClusterSinvalAckOutbound->head);
	next_tail = (tail + 1) % ClusterSinvalAckOutbound->capacity;
	if (next_tail == head) {
		LWLockRelease(&ClusterSinvalAckOutbound->lock.lock);
		return; /* full — best-effort drop;  sender path走 timeout WARN */
	}

	ClusterSinvalAckOutbound->slots[tail].batch_id = batch_id;
	ClusterSinvalAckOutbound->slots[tail].sender_node = sender_node;
	ClusterSinvalAckOutbound->slots[tail].status = status;
	ClusterSinvalAckOutbound->slots[tail].pad = 0;
	pg_atomic_write_u32(&ClusterSinvalAckOutbound->tail, next_tail);
	LWLockRelease(&ClusterSinvalAckOutbound->lock.lock);

	cluster_lmon_wakeup();
}


/*
 * cluster_sinval_drain_ack_outbound_and_send -- LMON-only (D5).
 *
 *   Called from LMON main loop after the regular outbound drain.
 *   Drains ClusterSinvalAckOutbound ring + emits per-entry single-peer
 *   PGRAC_IC_MSG_SINVAL_ACK envelopes via cluster_ic_send_envelope (NOT
 *   fanout — each ACK targets the original sender只).  L172 LMON-only
 *   producer mask enforcement.
 */
void
cluster_sinval_drain_ack_outbound_and_send(void)
{
	if (ClusterSinvalAckOutbound == NULL || ClusterSinval == NULL)
		return;
	if (MyBackendType != B_LMON)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster_sinval_drain_ack_outbound_and_send: callable only "
							   "from LMON process context"),
						errhint("L172 + HC139 ext — SINVAL_ACK msg_type producer mask = LMON.")));

	for (;;) {
		ClusterSinvalAckOutboundEntry local;
		uint32 head;
		SinvalAckHeader hdr;

		/* spec-2.39 D17:  inject — SKIP drops the ACK silently (sender
		 * will see ack_timeout WARN 53R95). */
		CLUSTER_INJECTION_POINT("cluster-sinval-ack-drop-send");

		LWLockAcquire(&ClusterSinvalAckOutbound->lock.lock, LW_EXCLUSIVE);
		head = pg_atomic_read_u32(&ClusterSinvalAckOutbound->head);
		if (head == pg_atomic_read_u32(&ClusterSinvalAckOutbound->tail)) {
			LWLockRelease(&ClusterSinvalAckOutbound->lock.lock);
			break;
		}
		local = ClusterSinvalAckOutbound->slots[head];
		pg_atomic_write_u32(&ClusterSinvalAckOutbound->head,
							(head + 1) % ClusterSinvalAckOutbound->capacity);
		LWLockRelease(&ClusterSinvalAckOutbound->lock.lock);

		if (cluster_injection_should_skip("cluster-sinval-ack-drop-send"))
			continue;

		memset(&hdr, 0, sizeof(hdr));
		hdr.batch_id = local.batch_id;
		hdr.epoch = cluster_epoch_get_current();
		hdr.acker_node = cluster_node_id;
		hdr.status = local.status;
		hdr.flags = 0;

		(void)cluster_ic_send_envelope(PGRAC_IC_MSG_SINVAL_ACK, local.sender_node, &hdr,
									   sizeof(hdr));
	}
}


/* ============================================================
 * spec-2.39 D6:  ACK envelope IC handler (enqueuer-side).
 * ============================================================ */
void
cluster_sinval_handle_ack_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const SinvalAckHeader *hdr = (const SinvalAckHeader *)payload;
	ClusterSinvalAckWaitEntry *entry;
	bool found;
	uint32 alive_mask, recv_mask, acker_bit;
	int32 enqueuer_pgprocno = -1;
	uint64 current_epoch;

	if (ClusterSinval == NULL || ClusterSinvalAckWaitHTAB == NULL)
		return;

	/* spec-2.39 D17:  inject — SKIP bypasses validation entirely. */
	CLUSTER_INJECTION_POINT("cluster-sinval-ack-skip-validate");
	if (cluster_injection_should_skip("cluster-sinval-ack-skip-validate"))
		goto match_batch;

	if (env->payload_length != (uint32)sizeof(SinvalAckHeader)) {
		pg_atomic_fetch_add_u64(&ClusterSinval->validation_drop_count, 1);
		return;
	}
	if (hdr->flags != 0 || hdr->acker_node < 0 || hdr->acker_node >= CLUSTER_MAX_NODES
		|| env->source_node_id != (uint32)hdr->acker_node
		|| cluster_conf_lookup_node(hdr->acker_node) == NULL) {
		pg_atomic_fetch_add_u64(&ClusterSinval->validation_drop_count, 1);
		return;
	}
	current_epoch = cluster_epoch_get_current();
	if (hdr->epoch < current_epoch) {
		pg_atomic_fetch_add_u64(&ClusterSinval->stale_epoch_drop_count, 1);
		return;
	}
	if (hdr->status > (uint16)SINVAL_ACK_RESET_PENDING) {
		pg_atomic_fetch_add_u64(&ClusterSinval->validation_drop_count, 1);
		return;
	}

match_batch:
	LWLockAcquire(ClusterSinvalAckWaitLock, LW_EXCLUSIVE);
	entry = (ClusterSinvalAckWaitEntry *)hash_search(ClusterSinvalAckWaitHTAB, &hdr->batch_id,
													 HASH_FIND, &found);
	if (!found) {
		LWLockRelease(ClusterSinvalAckWaitLock);
		pg_atomic_fetch_add_u64(&ClusterSinval->ack_orphan_count, 1);
		return;
	}

	/* HC141 fulfilled rule:  DONE or RESET_PENDING → bit-set;DROPPED 不. */
	if (hdr->status == SINVAL_ACK_DONE || hdr->status == SINVAL_ACK_RESET_PENDING) {
		acker_bit = (1u << hdr->acker_node);
		if ((entry->alive_peer_mask & acker_bit) == 0) {
			LWLockRelease(ClusterSinvalAckWaitLock);
			pg_atomic_fetch_add_u64(&ClusterSinval->ack_orphan_count, 1);
			return;
		}

		entry->ack_received_mask |= acker_bit;
		alive_mask = entry->alive_peer_mask;
		recv_mask = entry->ack_received_mask;
		if ((recv_mask & alive_mask) == alive_mask && entry->completion_signaled == 0) {
			entry->completion_signaled = 1;
			enqueuer_pgprocno = entry->enqueuer_pgprocno;
		}
		LWLockRelease(ClusterSinvalAckWaitLock);

		if (enqueuer_pgprocno >= 0) {
			pg_atomic_fetch_add_u64(&ClusterSinval->ack_received_count, 1);
			SetLatch(&GetPGProcByNumber(enqueuer_pgprocno)->procLatch);
		}
	} else {
		LWLockRelease(ClusterSinvalAckWaitLock);
	}
}


/* ============================================================
 * spec-2.39 D2:  enqueue_and_wait_ack public API (production caller).
 * ============================================================ */
uint32
cluster_sinval_compute_alive_peer_mask(void)
{
	uint32 mask = 0;
	int32 n;

	for (n = 0; n < CLUSTER_MAX_NODES; n++) {
		if (n == cluster_node_id)
			continue;
		if (cluster_conf_lookup_node(n) == NULL)
			continue;
		if (cluster_cssd_get_peer_state(n) != CLUSTER_CSSD_PEER_ALIVE)
			continue;
		mask |= (1u << n);
	}
	return mask;
}

/* ============================================================
 * spec-5.7 D6 (KO-B2):  ack_wait correlation infra reuse.
 *
 *   KO reuses ONLY the ack_wait correlation framework (this shared HTAB +
 *   the monotone batch_id allocator + the alive-peer mask + the WaitLatch
 *   loop), NOT the generic peer_enqueued semantics of
 *   cluster_sinval_enqueue_and_wait_ack (§3.6 KO-B2).  KO drives the same
 *   correlation entries with its OWN message (PGRAC_IC_MSG_KO_FLUSH) and its
 *   OWN apply-after-drop ACK timing: a peer records its bit (via
 *   cluster_sinval_ack_wait_record, called from the KO_FLUSH_ACK handler)
 *   only after it has really dropped the relfilenode's buffers.  These
 *   helpers are additive -- they do not change any existing sinval behavior.
 *
 *   batch_id uniqueness:  KO and sinval share next_batch_id, so a KO entry and
 *   a sinval entry never collide in the HTAB.
 * ============================================================ */

/* Allocate a globally-unique batch id from the shared monotone allocator. */
uint64
cluster_sinval_ack_wait_alloc_batch_id(void)
{
	if (ClusterSinval == NULL)
		return 0;
	return pg_atomic_fetch_add_u64(&ClusterSinval->next_batch_id, 1);
}

/*
 * Insert an ack_wait entry keyed by batch_id, owned by the CURRENT backend
 * (its procLatch is signalled on completion), waiting for the alive_mask peers
 * until deadline_us.  Returns false if the HTAB is full (caller fails closed).
 */
bool
cluster_sinval_ack_wait_begin(uint64 batch_id, uint32 alive_mask, TimestampTz deadline_us)
{
	ClusterSinvalAckWaitEntry *entry;
	bool found;

	if (ClusterSinval == NULL || ClusterSinvalAckWaitHTAB == NULL)
		return false;

	LWLockAcquire(ClusterSinvalAckWaitLock, LW_EXCLUSIVE);
	entry = (ClusterSinvalAckWaitEntry *)hash_search(ClusterSinvalAckWaitHTAB, &batch_id,
													 HASH_ENTER_NULL, &found);
	if (entry == NULL) {
		LWLockRelease(ClusterSinvalAckWaitLock);
		return false;
	}
	entry->enqueuer_pgprocno = MyProc->pgprocno;
	entry->alive_peer_mask = alive_mask;
	entry->ack_received_mask = 0;
	entry->deadline_us = deadline_us;
	entry->status = SINVAL_ACK_DONE;
	entry->completion_signaled = 0;
	LWLockRelease(ClusterSinvalAckWaitLock);
	return true;
}

/*
 * Record an ACK from acker_node for batch_id: set its bit and, when all alive
 * peers have ACK'd, wake the waiting enqueuer.  A no-op if the entry is gone
 * (already completed / timed out / removed) or acker_node is not in the entry's
 * alive mask.  Unlike the generic sinval ACK handler this bumps no sinval
 * counters -- the KO ACK handler owns KO's own observability.
 */
void
cluster_sinval_ack_wait_record(uint64 batch_id, int32 acker_node)
{
	ClusterSinvalAckWaitEntry *entry;
	bool found;
	uint32 acker_bit;
	int32 enqueuer_pgprocno = -1;

	if (ClusterSinval == NULL || ClusterSinvalAckWaitHTAB == NULL)
		return;
	if (acker_node < 0 || acker_node >= CLUSTER_MAX_NODES)
		return;
	acker_bit = (1u << acker_node);

	LWLockAcquire(ClusterSinvalAckWaitLock, LW_EXCLUSIVE);
	entry = (ClusterSinvalAckWaitEntry *)hash_search(ClusterSinvalAckWaitHTAB, &batch_id, HASH_FIND,
													 &found);
	if (found && (entry->alive_peer_mask & acker_bit) != 0) {
		entry->ack_received_mask |= acker_bit;
		if ((entry->ack_received_mask & entry->alive_peer_mask) == entry->alive_peer_mask
			&& entry->completion_signaled == 0) {
			entry->completion_signaled = 1;
			enqueuer_pgprocno = entry->enqueuer_pgprocno;
		}
	}
	LWLockRelease(ClusterSinvalAckWaitLock);

	if (enqueuer_pgprocno >= 0)
		SetLatch(&GetPGProcByNumber(enqueuer_pgprocno)->procLatch);
}

/* True when every alive peer has ACK'd for batch_id. */
bool
cluster_sinval_ack_wait_is_complete(uint64 batch_id)
{
	ClusterSinvalAckWaitEntry *entry;
	bool found;
	bool complete = false;

	if (ClusterSinval == NULL || ClusterSinvalAckWaitHTAB == NULL)
		return false;

	LWLockAcquire(ClusterSinvalAckWaitLock, LW_SHARED);
	entry = (ClusterSinvalAckWaitEntry *)hash_search(ClusterSinvalAckWaitHTAB, &batch_id, HASH_FIND,
													 &found);
	if (found)
		complete = (entry->ack_received_mask & entry->alive_peer_mask) == entry->alive_peer_mask;
	LWLockRelease(ClusterSinvalAckWaitLock);
	return complete;
}

/* Remove the ack_wait entry for batch_id (cleanup after completion / timeout). */
void
cluster_sinval_ack_wait_remove(uint64 batch_id)
{
	bool found;

	if (ClusterSinval == NULL || ClusterSinvalAckWaitHTAB == NULL)
		return;

	LWLockAcquire(ClusterSinvalAckWaitLock, LW_EXCLUSIVE);
	(void)hash_search(ClusterSinvalAckWaitHTAB, &batch_id, HASH_REMOVE, &found);
	LWLockRelease(ClusterSinvalAckWaitLock);
}

ClusterSinvalAckResult
cluster_sinval_enqueue_and_wait_ack(const SharedInvalidationMessage *msgs, int n)
{
	uint64 batch_id;
	uint32 alive_mask;
	TimestampTz deadline_us;
	ClusterSinvalAckWaitEntry *entry;
	bool found;
	int rc;
	ClusterSinvalAckResult result = CLUSTER_SINVAL_ACK_DONE;

	if (cluster_sinval_ack_mode == CLUSTER_SINVAL_ACK_MODE_NONE) {
		(void)cluster_sinval_enqueue_batch(msgs, n);
		return CLUSTER_SINVAL_ACK_DONE;
	}

	if (ClusterSinval == NULL || ClusterSinvalAckWaitHTAB == NULL)
		return CLUSTER_SINVAL_ACK_DONE; /* not cluster-enabled */

	alive_mask = cluster_sinval_compute_alive_peer_mask();
	if (alive_mask == 0) {
		/* No alive peers — just enqueue (no ACK to wait for). */
		(void)cluster_sinval_enqueue_batch(msgs, n);
		return CLUSTER_SINVAL_ACK_DONE;
	}

	batch_id = pg_atomic_fetch_add_u64(&ClusterSinval->next_batch_id, 1);
	deadline_us = GetCurrentTimestamp() + (int64)cluster_sinval_ack_timeout_ms * USECS_PER_MSEC;

	LWLockAcquire(ClusterSinvalAckWaitLock, LW_EXCLUSIVE);
	entry = (ClusterSinvalAckWaitEntry *)hash_search(ClusterSinvalAckWaitHTAB, &batch_id,
													 HASH_ENTER_NULL, &found);
	if (entry == NULL) {
		LWLockRelease(ClusterSinvalAckWaitLock);
		ereport(WARNING,
				(errcode(ERRCODE_CLUSTER_SINVAL_QUEUE_FULL),
				 errmsg("cluster sinval ack_wait_table full — bumping outbound RESET-all"),
				 errhint("raise cluster.sinval_ack_wait_slots or check ack_orphan_count.")));
		pg_atomic_fetch_add_u64(&ClusterSinval->outbound_queue_full_count, 1);
		pg_atomic_write_u32(&ClusterSinval->reset_all_broadcast_pending, 1);
		cluster_lmon_wakeup();
		return CLUSTER_SINVAL_ACK_ENQUEUE_FAILED;
	}
	entry->enqueuer_pgprocno = MyProc->pgprocno;
	entry->alive_peer_mask = alive_mask;
	entry->ack_received_mask = 0;
	entry->deadline_us = deadline_us;
	entry->status = SINVAL_ACK_DONE;
	entry->completion_signaled = 0;
	LWLockRelease(ClusterSinvalAckWaitLock);

	/* Reset latch before publishing the batch so we don't miss early ACKs. */
	ResetLatch(MyLatch);

	/* Publish the batch with the preallocated batch_id and REQUIRES_ACK flag
	 * so the returning ACK can be correlated with the wait entry above. */
	if (!cluster_sinval_enqueue_batch_with_ack_flag(msgs, n, batch_id)) {
		LWLockAcquire(ClusterSinvalAckWaitLock, LW_EXCLUSIVE);
		(void)hash_search(ClusterSinvalAckWaitHTAB, &batch_id, HASH_REMOVE, &found);
		LWLockRelease(ClusterSinvalAckWaitLock);
		ereport(WARNING,
				(errcode(ERRCODE_CLUSTER_SINVAL_QUEUE_FULL),
				 errmsg("cluster sinval outbound queue full — broadcasting RESET-all fallback"),
				 errhint("raise cluster.sinval_broadcast_max_queue_size or "
						 "monitor outbound_queue_full_count.")));
		pg_atomic_fetch_add_u64(&ClusterSinval->outbound_queue_full_count, 1);
		pg_atomic_write_u32(&ClusterSinval->reset_all_broadcast_pending, 1);
		cluster_lmon_wakeup();
		return CLUSTER_SINVAL_ACK_ENQUEUE_FAILED;
	}

	/* Wait loop. */
	for (;;) {
		bool done = false;
		TimestampTz now_us;
		long timeout_ms;

		LWLockAcquire(ClusterSinvalAckWaitLock, LW_SHARED);
		entry = (ClusterSinvalAckWaitEntry *)hash_search(ClusterSinvalAckWaitHTAB, &batch_id,
														 HASH_FIND, &found);
		if (found && entry->ack_received_mask == entry->alive_peer_mask)
			done = true;
		LWLockRelease(ClusterSinvalAckWaitLock);
		if (done)
			break;

		now_us = GetCurrentTimestamp();
		if (now_us >= deadline_us) {
			result = CLUSTER_SINVAL_ACK_TIMEOUT;
			break;
		}
		timeout_ms = (deadline_us - now_us + USECS_PER_MSEC - 1) / USECS_PER_MSEC;
		if (timeout_ms <= 0)
			timeout_ms = 1;
		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, timeout_ms,
					   WAIT_EVENT_SINVAL_ACK_WAIT);
		ResetLatch(MyLatch);
		if (rc & WL_POSTMASTER_DEATH)
			break;
	}

	/* Cleanup entry. */
	LWLockAcquire(ClusterSinvalAckWaitLock, LW_EXCLUSIVE);
	(void)hash_search(ClusterSinvalAckWaitHTAB, &batch_id, HASH_REMOVE, &found);
	LWLockRelease(ClusterSinvalAckWaitLock);

	if (result == CLUSTER_SINVAL_ACK_TIMEOUT) {
		pg_atomic_fetch_add_u64(&ClusterSinval->ack_timeout_count, 1);
		ereport(WARNING, (errcode(ERRCODE_CLUSTER_SINVAL_ACK_TIMEOUT),
						  errmsg("sinval ack timeout — some peers未 propagate within %d ms",
								 cluster_sinval_ack_timeout_ms),
						  errhint("check cluster health via pg_stat_cluster_counters.")));
	}
	return result;
}


/* ============================================================
 * spec-2.39 D7 + v0.3 P1:  RESET-all broadcast sentinel emit (LMON-only).
 * ============================================================ */
void
cluster_sinval_broadcast_reset_all(void)
{
	SinvalBroadcastHeader hdr;
	ClusterICFanoutResult per_peer[CLUSTER_MAX_NODES];

	if (ClusterSinval == NULL)
		return;
	if (MyBackendType != B_LMON)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster_sinval_broadcast_reset_all: callable only from LMON")));

	if (pg_atomic_read_u32(&ClusterSinval->reset_all_broadcast_pending) == 0)
		return;
	pg_atomic_write_u32(&ClusterSinval->reset_all_broadcast_pending, 0);

	memset(&hdr, 0, sizeof(hdr));
	hdr.batch_id = pg_atomic_fetch_add_u64(&ClusterSinval->next_batch_id, 1);
	hdr.epoch = cluster_epoch_get_current();
	hdr.source_node = cluster_node_id;
	hdr.nmsgs = 0;
	hdr.flags = SINVAL_RESET_ALL_BROADCAST;

	cluster_ic_send_envelope_fanout(PGRAC_IC_MSG_SINVAL, &hdr, sizeof(hdr), per_peer);
}


/* ============================================================
 * spec-2.39 D14:  reconfig RESET-all hook (local-only).
 * ============================================================ */
void
cluster_sinval_reset_all_on_reconfig(void)
{
	HASH_SEQ_STATUS scan;
	ClusterSinvalAckWaitEntry *entry;
	uint64 current_epoch;

	if (ClusterSinval == NULL)
		return;

	pg_atomic_write_u32(&ClusterSinval->inbound_overflow_reset_pending, 1);
	pg_atomic_fetch_add_u64(&ClusterSinval->inbound_overflow_reset_count, 1);

	if (ClusterSinvalAckWaitHTAB == NULL)
		return;

	current_epoch = cluster_epoch_get_current();
	LWLockAcquire(ClusterSinvalAckWaitLock, LW_EXCLUSIVE);
	hash_seq_init(&scan, ClusterSinvalAckWaitHTAB);
	while ((entry = hash_seq_search(&scan)) != NULL) {
		Latch *latch = &GetPGProcByNumber(entry->enqueuer_pgprocno)->procLatch;
		entry->ack_received_mask = entry->alive_peer_mask;
		entry->completion_signaled = 1;
		SetLatch(latch);
	}
	LWLockRelease(ClusterSinvalAckWaitLock);
	(void)current_epoch; /* reserved for future epoch-based cleanup */
}

#endif /* USE_PGRAC_CLUSTER */
