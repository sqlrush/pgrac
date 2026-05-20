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
 *	  Drain paths (drain_outbound_and_broadcast / drain_inbound_and_apply /
 *	  apply_inbound_overflow_reset_if_pending) live in this file but are
 *	  called only from the SI Broadcaster aux process (cluster_sinval_bcast.c
 *	  main loop).  Backend / IC handler contexts MUST NOT call them.
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

#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_inject.h"
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
	pg_atomic_uint64 next_batch_id;					 /* monotone allocator */
	/* 9 D10 counters */
	pg_atomic_uint64 broadcast_send_count;
	pg_atomic_uint64 broadcast_receive_count;
	pg_atomic_uint64 inject_local_queue_count;
	pg_atomic_uint64 outbound_queue_full_count;
	pg_atomic_uint64 inbound_queue_full_count;
	pg_atomic_uint64 inbound_overflow_reset_count;
	pg_atomic_uint64 validation_drop_count;
	pg_atomic_uint64 stale_epoch_drop_count;
	pg_atomic_uint64 echo_dropped_count;
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
	for (i = 0; i < n; i++)
		slot->msgs[i] = msgs[i];
	slot->created_at_ts = GetCurrentTimestamp();

	pg_atomic_write_u32(&ClusterSinvalOutbound->tail, (tail + 1) % ClusterSinvalOutbound->capacity);
	LWLockRelease(&ClusterSinvalOutbound->lock.lock);

	cluster_sinval_set_proc_latch();
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
static uint32
sinval_payload_checksum(const SinvalBroadcastHeader *hdr, const SharedInvalidationMessage *msgs,
						int n)
{
	const uint8 *bytes;
	uint32 c = 0;
	int i;

	bytes = (const uint8 *)hdr;
	for (i = 0; i < (int)sizeof(SinvalBroadcastHeader); i++)
		c = (c * 31u) + bytes[i];
	bytes = (const uint8 *)msgs;
	for (i = 0; i < n * (int)sizeof(SharedInvalidationMessage); i++)
		c = (c * 31u) + bytes[i];
	return c;
}

uint32
cluster_sinval_compute_payload_checksum(const SinvalBroadcastHeader *hdr,
										const SharedInvalidationMessage *msgs, int n)
{
	return sinval_payload_checksum(hdr, msgs, n);
}

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
	 * by aux process (set flag,  no silent stale). */
	if (!cluster_sinval_inbound_try_enqueue(hdr->batch_id, msgs, hdr->nmsgs, hdr->source_node)) {
		pg_atomic_fetch_add_u64(&ClusterSinval->inbound_queue_full_count, 1);
		pg_atomic_write_u32(&ClusterSinval->inbound_overflow_reset_pending, 1);
	} else {
		pg_atomic_fetch_add_u64(&ClusterSinval->broadcast_receive_count, 1);
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

	while (drained < batch_limit) {
		ClusterSinvalQueueEntry local;
		uint32 head;
		SinvalBroadcastHeader hdr;
		Size payload_len;
		char *buf;

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
		hdr.flags = 0;

		payload_len
			= sizeof(SinvalBroadcastHeader) + local.nmsgs * sizeof(SharedInvalidationMessage);
		buf = (char *)palloc0(payload_len);
		memcpy(buf, &hdr, sizeof(hdr));
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

		if (cluster_ic_send_envelope(PGRAC_IC_MSG_SINVAL, PGRAC_IC_BROADCAST, buf, payload_len)
			== CLUSTER_IC_SEND_DONE)
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

void
cluster_sinval_module_init(void)
{
	cluster_shmem_register_region(&cluster_sinval_outbound_region);
	cluster_shmem_register_region(&cluster_sinval_inbound_region);
}


/* ============================================================
 * IC msg type registration (HC139 producer mask).
 * ============================================================ */
static const ClusterICMsgTypeInfo cluster_sinval_msg_info = {
	.msg_type = PGRAC_IC_MSG_SINVAL,
	.name = "cluster_sinval",
	.allowed_producer_mask = CLUSTER_IC_PRODUCER_SINVAL_BCAST,
	.broadcast_ok = true,
	.handler = cluster_sinval_handle_envelope,
};

void
cluster_sinval_register_msg_type(void)
{
	cluster_ic_register_msg_type(&cluster_sinval_msg_info);
}


/* ============================================================
 * Latch wake-up — used by enqueue / IC handler.
 * ============================================================ */
void
cluster_sinval_register_proc_latch(Latch *latch)
{
	ClusterSinvalBcastLatch = latch;
}

void
cluster_sinval_set_proc_latch(void)
{
	if (ClusterSinvalBcastLatch != NULL)
		SetLatch(ClusterSinvalBcastLatch);
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

#endif /* USE_PGRAC_CLUSTER */
