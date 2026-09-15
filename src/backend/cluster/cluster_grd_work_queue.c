/*-------------------------------------------------------------------------
 *
 * cluster_grd_work_queue.c
 *	  GES work queue — spec-2.16 D5.
 *
 *	  FIFO queue from GES handler (Phase 1) to LMON tick body (Phase 2)
 *	  grant decision.  Bounded capacity + full → enqueue REJECT_BUSY
 *	  reply per spec-2.16 v0.4 L1.3.
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
 *	  src/backend/cluster/cluster_grd_work_queue.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_conf.h"

#include "cluster/cluster_ges.h" /* GesRequestPayload (spec-5.8 D8 coupling assert) */
#include "cluster/cluster_grd_work_queue.h"
#include "cluster/cluster_lmon.h" /* PGRAC: spec-7.2 D1 enqueue wakeup */
#include "cluster/cluster_shmem.h"
#include "miscadmin.h" /* IsBootstrapProcessingMode */
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"

/*
 * spec-5.8 D8 — couple the work-item payload buffer to the largest wire payload
 * the master enqueues here (GesRequestPayload, 72B after D1c/D1e).  A future
 * payload growth that forgets this buffer fails at compile time instead of
 * making the master reject every cross-node REQUEST with WORK_QUEUE_FULL
 * (latent until a 2-node run, as the D1e miss was).
 */
StaticAssertDecl(sizeof(((ClusterGrdWorkItem *)0)->payload) >= sizeof(GesRequestPayload),
				 "GES work-item payload buffer must hold a full GesRequestPayload");


typedef struct ClusterGrdWorkQueueShared {
	uint32 head;
	uint32 tail;
	uint32 count;
	ClusterGrdWorkItem items[PGRAC_GES_WORK_QUEUE_CAPACITY];
} ClusterGrdWorkQueueShared;

static ClusterGrdWorkQueueShared *cluster_grd_work_queue_state = NULL;
static LWLock *cluster_grd_work_queue_lock = NULL;

ClusterNormalStopPollResult
cluster_grd_work_queue_normal_stop_poll(uint32 *slot_out, const char **reason_out)
{
	ClusterNormalStopPollResult result = CLUSTER_NORMAL_STOP_READY;
	const ClusterGrdWorkQueueShared *q = cluster_grd_work_queue_state;

	if (slot_out == NULL || reason_out == NULL)
		return CLUSTER_NORMAL_STOP_INVALID;
	*slot_out = UINT32_MAX;
	*reason_out = "GRD_WORK_QUEUE_UNINITIALIZED";
	if (q == NULL || cluster_grd_work_queue_lock == NULL)
		return CLUSTER_NORMAL_STOP_INVALID;
	if (LWLockHeldByMe(cluster_grd_work_queue_lock)) {
		*reason_out = "GRD_WORK_QUEUE_LOCK_HELD";
		return CLUSTER_NORMAL_STOP_INVALID;
	}
	LWLockAcquire(cluster_grd_work_queue_lock, LW_SHARED);
	*reason_out = "NONE";
	if (q->head >= PGRAC_GES_WORK_QUEUE_CAPACITY || q->tail >= PGRAC_GES_WORK_QUEUE_CAPACITY
		|| q->count > PGRAC_GES_WORK_QUEUE_CAPACITY
		|| (q->tail + q->count) % PGRAC_GES_WORK_QUEUE_CAPACITY != q->head) {
		result = CLUSTER_NORMAL_STOP_INVALID;
		*reason_out = "GRD_WORK_QUEUE_GEOMETRY";
	} else {
		for (uint32 offset = 0; offset < q->count; offset++) {
			uint32 index = (q->tail + offset) % PGRAC_GES_WORK_QUEUE_CAPACITY;
			const ClusterGrdWorkItem *item = &q->items[index];
			if (item->source_node_id >= CLUSTER_MAX_NODES || item->payload_len == 0
				|| item->payload_len > sizeof(item->payload)) {
				result = CLUSTER_NORMAL_STOP_INVALID;
				*slot_out = index;
				*reason_out = "GRD_WORK_QUEUE_ITEM_INVALID";
				break;
			}
			if (result == CLUSTER_NORMAL_STOP_READY) {
				result = CLUSTER_NORMAL_STOP_PENDING;
				*slot_out = index;
				*reason_out = "GRD_WORK_QUEUE";
			}
		}
	}
	LWLockRelease(cluster_grd_work_queue_lock);
	/* A dequeued item still belongs to the LMON outer work bracket. */
	return result;
}


Size
cluster_grd_work_queue_shmem_size(void)
{
	return sizeof(ClusterGrdWorkQueueShared);
}

void
cluster_grd_work_queue_shmem_init(void)
{
	bool found;

	cluster_grd_work_queue_state = ShmemInitStruct("pgrac cluster grd work queue",
												   cluster_grd_work_queue_shmem_size(), &found);
	if (!found)
		memset(cluster_grd_work_queue_state, 0, sizeof(*cluster_grd_work_queue_state));

	/* Same bootstrap-safe gate as cluster_grd_outbound:  bootstrap mode
	 * skips process_shmem_requests so tranche is not registered. */
	if (!IsBootstrapProcessingMode())
		cluster_grd_work_queue_lock = &(GetNamedLWLockTranche("ClusterGrdWorkQueue"))[0].lock;
}

static const ClusterShmemRegion cluster_grd_work_queue_region = {
	.name = "pgrac cluster grd work queue",
	.size_fn = cluster_grd_work_queue_shmem_size,
	.init_fn = cluster_grd_work_queue_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_grd_work_queue",
	.reserved_flags = 0,
};

void
cluster_grd_work_queue_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_grd_work_queue_region);
}


bool
cluster_grd_work_queue_enqueue(uint32 source_node_id, const void *payload, uint16 payload_len)
{
	ClusterGrdWorkItem *slot;

	Assert(cluster_grd_work_queue_state != NULL);
	if (cluster_grd_work_queue_state == NULL || cluster_grd_work_queue_lock == NULL)
		return false;
	if (payload_len > sizeof(((ClusterGrdWorkItem *)0)->payload))
		return false;

	LWLockAcquire(cluster_grd_work_queue_lock, LW_EXCLUSIVE);
	if (cluster_grd_work_queue_state->count >= PGRAC_GES_WORK_QUEUE_CAPACITY) {
		LWLockRelease(cluster_grd_work_queue_lock);
		return false;
	}

	slot = &cluster_grd_work_queue_state->items[cluster_grd_work_queue_state->head];
	slot->source_node_id = source_node_id;
	slot->payload_len = payload_len;
	if (payload_len > 0)
		memcpy(slot->payload, payload, payload_len);

	cluster_grd_work_queue_state->head
		= (cluster_grd_work_queue_state->head + 1) % PGRAC_GES_WORK_QUEUE_CAPACITY;
	cluster_grd_work_queue_state->count++;

	LWLockRelease(cluster_grd_work_queue_lock);

	/*
	 * PGRAC: spec-7.2 D1 -- wake the drain consumer (LMON) on enqueue,
	 * mirroring the cluster_grd_outbound enqueue family.  Without this a
	 * locally-mastered GES request sat in the queue until LMON woke for
	 * some other reason (worst case a full heartbeat interval, <= 1s),
	 * which made local-master lock requests slower than remote-master
	 * ones.  Publish-before-signal: the slot is visible (released the
	 * LWLock above) before the wakeup fires.
	 * Spec: spec-7.2-ic-data-plane-decoupling.md
	 */
	cluster_lmon_duty_mark_dirty(CLUSTER_LMON_DUTY_GES_WORK_QUEUE);
	cluster_lmon_wakeup();

	return true;
}

bool
cluster_grd_work_queue_dequeue(ClusterGrdWorkItem *out)
{
	bool got = false;

	Assert(cluster_grd_work_queue_state != NULL);
	Assert(out != NULL);
	if (cluster_grd_work_queue_state == NULL || cluster_grd_work_queue_lock == NULL || out == NULL)
		return false;

	LWLockAcquire(cluster_grd_work_queue_lock, LW_EXCLUSIVE);
	if (cluster_grd_work_queue_state->count > 0) {
		*out = cluster_grd_work_queue_state->items[cluster_grd_work_queue_state->tail];
		cluster_grd_work_queue_state->tail
			= (cluster_grd_work_queue_state->tail + 1) % PGRAC_GES_WORK_QUEUE_CAPACITY;
		cluster_grd_work_queue_state->count--;
		got = true;
	}
	LWLockRelease(cluster_grd_work_queue_lock);
	return got;
}

uint32
cluster_grd_work_queue_depth(void)
{
	uint32 depth;
	if (cluster_grd_work_queue_state == NULL || cluster_grd_work_queue_lock == NULL)
		return 0;
	LWLockAcquire(cluster_grd_work_queue_lock, LW_SHARED);
	depth = cluster_grd_work_queue_state->count;
	LWLockRelease(cluster_grd_work_queue_lock);
	return depth;
}
