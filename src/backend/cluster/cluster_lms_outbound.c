/*-------------------------------------------------------------------------
 *
 * cluster_lms_outbound.c
 *	  pgrac DATA-plane outbound ring — spec-7.2 D4 (Q6-B twin ring).
 *
 *	  Backend-context producers of DATA-plane messages (GCS block
 *	  REQUEST / INVALIDATE) stage frames here;  the LMS data-plane loop
 *	  drains and sends them over its own fds.  This is the DATA twin of
 *	  the CONTROL-plane cluster_grd_outbound ring, kept to the same
 *	  minimal single-tail-FIFO semantics (Q6-B: one consumer, one lock,
 *	  no claim/compaction machinery — the r1-F3 argument against a
 *	  shared dual-reader ring).  Enqueue marks no LMON duty and wakes
 *	  LMS, not LMON.
 *
 *	  Ordering note (INV-7.2-DATA-FIFO): per-peer DATA frames keep a
 *	  single ordered stream because (a) this ring is FIFO, (b) LMS is
 *	  its only consumer, and (c) LMS sends on per-peer sockets with the
 *	  tier1 partial-frame buffer (WOULD_BLOCK requeues at the head
 *	  before anything newer is sent to that peer).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_lms_outbound.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.  Spec: spec-7.2-ic-data-plane-decoupling.md (D4).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_gcs_block.h" /* GcsBlockRequestPayload (pre-send hook) */
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"

#ifdef USE_PGRAC_CLUSTER

#define PGRAC_LMS_OUTBOUND_CAPACITY 256
#define PGRAC_LMS_OUTBOUND_PAYLOAD_MAX 128

typedef struct ClusterLmsOutboundSlot {
	uint32 dest_node_id;
	uint8 msg_type;
	uint16 payload_len;
	uint8 payload[PGRAC_LMS_OUTBOUND_PAYLOAD_MAX];
} ClusterLmsOutboundSlot;

typedef struct ClusterLmsOutboundState {
	uint32 head; /* next slot to fill */
	uint32 tail; /* next slot to drain */
	uint32 count;
	ClusterLmsOutboundSlot ring[PGRAC_LMS_OUTBOUND_CAPACITY];
} ClusterLmsOutboundState;

/*
 * spec-7.3 D4 — one ring per DATA worker channel.  rings[0] is worker 0 (the
 * spec-7.2 ring; lms_workers=1 uses only it — byte-identical), rings[c] is
 * worker c.  Sizing is the compile-time cap (CLUSTER_LMS_MAX_WORKERS); the
 * live count follows cluster.lms_workers.  Each ring keeps the Q6-B single-
 * consumer single-tail FIFO semantics, and each has its own lock so worker c
 * drains rings[c] without contending on the other workers.
 */
static ClusterLmsOutboundState *cluster_lms_outbound_rings = NULL;
static LWLock *cluster_lms_outbound_locks[CLUSTER_LMS_MAX_WORKERS];

#define OB_RING(w) (&cluster_lms_outbound_rings[(w)])
#define OB_LOCK(w) (cluster_lms_outbound_locks[(w)])

static Size
cluster_lms_outbound_shmem_size(void)
{
	/* Contiguous array of CLUSTER_LMS_MAX_WORKERS rings; the C array stride is
	 * sizeof(ClusterLmsOutboundState), so size the whole block then MAXALIGN. */
	return MAXALIGN(mul_size(CLUSTER_LMS_MAX_WORKERS, sizeof(ClusterLmsOutboundState)));
}

static void
cluster_lms_outbound_shmem_init(void)
{
	bool found;
	int i;

	cluster_lms_outbound_rings = (ClusterLmsOutboundState *)ShmemInitStruct(
		"pgrac cluster lms data outbound", cluster_lms_outbound_shmem_size(), &found);
	if (!found)
		memset(cluster_lms_outbound_rings, 0,
			   CLUSTER_LMS_MAX_WORKERS * sizeof(ClusterLmsOutboundState));

	if (!IsBootstrapProcessingMode())
		for (i = 0; i < CLUSTER_LMS_MAX_WORKERS; i++)
			cluster_lms_outbound_locks[i]
				= &(GetNamedLWLockTranche("ClusterLmsDataOutbound"))[i].lock;
}

static const ClusterShmemRegion cluster_lms_outbound_region = {
	.name = "pgrac cluster lms data outbound",
	.size_fn = cluster_lms_outbound_shmem_size,
	.init_fn = cluster_lms_outbound_shmem_init,
	.lwlock_count = CLUSTER_LMS_MAX_WORKERS,
	.owner_subsys = "cluster_lms_outbound",
	.reserved_flags = 0,
};

void
cluster_lms_outbound_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_lms_outbound_region);
}

/* Named-tranche request (process_shmem_requests window;  I15 pattern). */
void
cluster_lms_outbound_request_lwlocks(void)
{
	RequestNamedLWLockTranche("ClusterLmsDataOutbound", CLUSTER_LMS_MAX_WORKERS);
}

/*
 * cluster_lms_outbound_enqueue — stage one DATA-plane frame for LMS.
 *
 *	Returns false on full ring / oversized payload / pre-shmem call;
 *	callers treat false as WOULD_BLOCK (retry via the normal request
 *	retry machinery).  Publish-before-signal: the slot is visible
 *	before the LMS wakeup fires.
 */
bool
cluster_lms_outbound_enqueue(int worker_id, uint8 msg_type, uint32 dest_node_id,
							 const void *payload, uint16 payload_len)
{
	ClusterLmsOutboundState *ring;
	LWLock *lock;
	ClusterLmsOutboundSlot *slot;

	if (worker_id < 0 || worker_id >= CLUSTER_LMS_MAX_WORKERS)
		return false;
	if (cluster_lms_outbound_rings == NULL || OB_LOCK(worker_id) == NULL)
		return false;
	if (payload_len > PGRAC_LMS_OUTBOUND_PAYLOAD_MAX)
		return false;

	ring = OB_RING(worker_id);
	lock = OB_LOCK(worker_id);

	LWLockAcquire(lock, LW_EXCLUSIVE);
	if (ring->count >= PGRAC_LMS_OUTBOUND_CAPACITY) {
		LWLockRelease(lock);
		return false;
	}
	slot = &ring->ring[ring->head];
	slot->dest_node_id = dest_node_id;
	slot->msg_type = msg_type;
	slot->payload_len = payload_len;
	if (payload_len > 0)
		memcpy(slot->payload, payload, payload_len);
	ring->head = (ring->head + 1) % PGRAC_LMS_OUTBOUND_CAPACITY;
	ring->count++;
	LWLockRelease(lock);

	cluster_lms_wakeup(worker_id);
	return true;
}

/* Head-requeue for WOULD_BLOCK (preserves this worker's per-peer FIFO). */
static void
lms_outbound_requeue_head(int worker_id, const ClusterLmsOutboundSlot *slot)
{
	ClusterLmsOutboundState *ring = OB_RING(worker_id);
	LWLock *lock = OB_LOCK(worker_id);

	LWLockAcquire(lock, LW_EXCLUSIVE);
	if (ring->count < PGRAC_LMS_OUTBOUND_CAPACITY) {
		ring->tail = (ring->tail + PGRAC_LMS_OUTBOUND_CAPACITY - 1) % PGRAC_LMS_OUTBOUND_CAPACITY;
		ring->ring[ring->tail] = *slot;
		ring->count++;
	}
	/* full → drop;  fire-and-forget layers self-heal via retransmit */
	LWLockRelease(lock);
}

/*
 * cluster_lms_outbound_drain_send — one worker drains + sends its own ring.
 *
 *	Bounded batch per call.  WOULD_BLOCK requeues at the HEAD so the
 *	per-peer byte stream is never reordered (INV-7.2-DATA-FIFO).  worker c
 *	only ever touches rings[c], so the single-consumer-single-tail
 *	guarantee holds per worker (spec-7.3 D4).  The GCS block REQUEST
 *	pre-send hook (direct-land arm) rides along with the DATA consumer.
 */
int
cluster_lms_outbound_drain_send(int worker_id)
{
	ClusterLmsOutboundState *ring;
	LWLock *lock;
	int sent = 0;

	if (worker_id < 0 || worker_id >= CLUSTER_LMS_MAX_WORKERS)
		return 0;
	if (cluster_lms_outbound_rings == NULL || OB_LOCK(worker_id) == NULL)
		return 0;

	Assert(MyBackendType == B_LMS || MyBackendType == B_LMS_WORKER);

	ring = OB_RING(worker_id);
	lock = OB_LOCK(worker_id);

	while (sent < 64) {
		ClusterLmsOutboundSlot slot;
		ClusterICSendResult rc;
		bool got = false;

		LWLockAcquire(lock, LW_EXCLUSIVE);
		if (ring->count > 0) {
			slot = ring->ring[ring->tail];
			ring->tail = (ring->tail + 1) % PGRAC_LMS_OUTBOUND_CAPACITY;
			ring->count--;
			got = true;
		}
		LWLockRelease(lock);
		if (!got)
			break;

		if (slot.msg_type == PGRAC_IC_MSG_GCS_BLOCK_REQUEST
			&& slot.payload_len == sizeof(GcsBlockRequestPayload))
			cluster_gcs_block_lmon_prepare_outbound_request((GcsBlockRequestPayload *)slot.payload,
															(int32)slot.dest_node_id);

		rc = cluster_ic_send_envelope(slot.msg_type, (int32)slot.dest_node_id,
									  slot.payload_len > 0 ? slot.payload : NULL, slot.payload_len);
		if (rc == CLUSTER_IC_SEND_DONE) {
			sent++;
			continue;
		}
		if (rc == CLUSTER_IC_SEND_WOULD_BLOCK) {
			lms_outbound_requeue_head(worker_id, &slot);
			break;
		}
		/* HARD_ERROR: peer down — drop;  requesters retry fail-closed. */
	}

	return sent;
}

uint32
cluster_lms_outbound_depth(int worker_id)
{
	ClusterLmsOutboundState *ring;
	LWLock *lock;
	uint32 depth;

	if (worker_id < 0 || worker_id >= CLUSTER_LMS_MAX_WORKERS)
		return 0;
	if (cluster_lms_outbound_rings == NULL || OB_LOCK(worker_id) == NULL)
		return 0;
	ring = OB_RING(worker_id);
	lock = OB_LOCK(worker_id);
	LWLockAcquire(lock, LW_SHARED);
	depth = ring->count;
	LWLockRelease(lock);
	return depth;
}

#endif /* USE_PGRAC_CLUSTER */
