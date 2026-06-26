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

#include "cluster/cluster_ges.h" /* GesRequestPayload (spec-5.8 D8 coupling assert) */
#include "cluster/cluster_grd_work_queue.h"
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
