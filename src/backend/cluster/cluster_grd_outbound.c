/*-------------------------------------------------------------------------
 *
 * cluster_grd_outbound.c
 *	  GES outbound ring + reserved reply pool + dirty-list — spec-2.16 D4.
 *
 *	  Implements the LMON-owned generic outbound ring per spec-2.16 v0.4
 *	  L1.1 + v0.5 P1.1 + v0.6 L1.1.  Three origin_kind producer paths
 *	  share one ring with separate nofail bounded behaviors.
 *
 *	  Step 2 ship:  ring + 2 dirty-list + 3 enqueue + dequeue + drain
 *	    + 3 depth accessor真激活.  0 producer caller in this Step
 *	    (Step 3 D6 wires LMON reply path; Step 4 D9 wires backend
 *	    request + cleanup release paths).
 *
 *	  Hot-path discipline (I46 + I54 nofail):
 *	    - All enqueue/dequeue under cluster_grd_outbound_lock (LWLock).
 *	    - Counter bumps are atomic outside any other lock.
 *	    - No palloc / malloc / ereport ERROR / wait inside enqueue.
 *	    - dirty-list overflow → drop oldest (NOT crash;NOT ERROR).
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
 *	  src/backend/cluster/cluster_grd_outbound.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_grd.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"


/* ============================================================
 * Shmem layout — fixed compile-time capacity per I54 (a)(b).
 *
 *   Ring: FIFO byte buffer of ClusterGrdOutboundSlot[].
 *   Reserved pool: separately accounted slots reserved for LMON_REPLY;
 *     when ring main occupancy > (CAPACITY - RESERVED_BUDGET), only
 *     LMON_REPLY producers may consume the remaining space.
 *   Reply dirty-list: bounded ring-buffer for LMON_REPLY overflow.
 *   Cleanup dirty-list: bounded ring-buffer for CLEANUP_RELEASE overflow.
 *
 *   All under single LWLock cluster_grd_outbound_lock (low contention:
 *   single consumer LMON tick body + bursty multi-producer backends).
 * ============================================================ */

typedef struct ClusterGrdOutboundShared {
	/* Main ring (FIFO).  head + tail wrapping. */
	uint32 ring_head; /* next free slot index */
	uint32 ring_tail; /* next consumer slot index */
	uint32 ring_count;
	ClusterGrdOutboundSlot ring[PGRAC_GES_OUTBOUND_RING_CAPACITY];

	/* Reply dirty-list (bounded ring;  no palloc per I54(c)) */
	uint32 reply_dirty_head;
	uint32 reply_dirty_tail;
	uint32 reply_dirty_count;
	ClusterGrdOutboundSlot reply_dirty[PGRAC_GES_REPLY_DIRTY_BUDGET];

	/* Cleanup dirty-list */
	uint32 cleanup_dirty_head;
	uint32 cleanup_dirty_tail;
	uint32 cleanup_dirty_count;
	ClusterGrdOutboundSlot cleanup_dirty[PGRAC_GES_CLEANUP_DIRTY_BUDGET];
} ClusterGrdOutboundShared;

static ClusterGrdOutboundShared *cluster_grd_outbound_state = NULL;
static LWLock *cluster_grd_outbound_lock = NULL;


/* ============================================================
 * Shmem lifecycle.
 * ============================================================ */

Size
cluster_grd_outbound_shmem_size(void)
{
	return sizeof(ClusterGrdOutboundShared);
}

void
cluster_grd_outbound_shmem_init(void)
{
	bool found;

	cluster_grd_outbound_state =
		ShmemInitStruct("pgrac cluster grd outbound", cluster_grd_outbound_shmem_size(), &found);
	if (!found) {
		memset(cluster_grd_outbound_state, 0, sizeof(*cluster_grd_outbound_state));
	}

	/* Resolve LWLock tranche (registered via cluster_request_shmem). */
	cluster_grd_outbound_lock = &(GetNamedLWLockTranche("ClusterGrdOutbound"))[0].lock;
}

static const ClusterShmemRegion cluster_grd_outbound_region = {
	.name = "pgrac cluster grd outbound",
	.size_fn = cluster_grd_outbound_shmem_size,
	.init_fn = cluster_grd_outbound_shmem_init,
	.lwlock_count = 1, /* single ring lock — low contention */
	.owner_subsys = "cluster_grd_outbound",
	.reserved_flags = 0,
};

void
cluster_grd_outbound_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_grd_outbound_region);
}


/* ============================================================
 * Internal helpers (caller MUST hold cluster_grd_outbound_lock).
 * ============================================================ */

static bool
ring_push(uint8 msg_type, uint8 origin, uint32 dest_node_id, const void *payload,
		  uint16 payload_len)
{
	ClusterGrdOutboundSlot *slot;

	if (cluster_grd_outbound_state->ring_count >= PGRAC_GES_OUTBOUND_RING_CAPACITY)
		return false;
	if (payload_len > PGRAC_GES_OUTBOUND_PAYLOAD_MAX)
		return false;

	slot = &cluster_grd_outbound_state->ring[cluster_grd_outbound_state->ring_head];
	slot->dest_node_id = dest_node_id;
	slot->msg_type = msg_type;
	slot->origin = origin;
	slot->payload_len = payload_len;
	if (payload_len > 0)
		memcpy(slot->payload, payload, payload_len);

	cluster_grd_outbound_state->ring_head =
		(cluster_grd_outbound_state->ring_head + 1) % PGRAC_GES_OUTBOUND_RING_CAPACITY;
	cluster_grd_outbound_state->ring_count++;
	return true;
}

static void
reply_dirty_push(uint32 dest_node_id, const void *payload, uint16 payload_len)
{
	ClusterGrdOutboundSlot *slot;

	if (payload_len > PGRAC_GES_OUTBOUND_PAYLOAD_MAX)
		return;

	/* Bounded:  if full → drop oldest (advance tail) + counter (I54(d)). */
	if (cluster_grd_outbound_state->reply_dirty_count >= PGRAC_GES_REPLY_DIRTY_BUDGET) {
		cluster_grd_outbound_state->reply_dirty_tail =
			(cluster_grd_outbound_state->reply_dirty_tail + 1) % PGRAC_GES_REPLY_DIRTY_BUDGET;
		cluster_grd_outbound_state->reply_dirty_count--;
		cluster_grd_inc_ges_reply_dropped();
	}

	slot = &cluster_grd_outbound_state->reply_dirty[cluster_grd_outbound_state->reply_dirty_head];
	slot->dest_node_id = dest_node_id;
	slot->msg_type = 5; /* GES_REPLY */
	slot->origin = CLUSTER_GRD_OUTBOUND_LMON_REPLY;
	slot->payload_len = payload_len;
	if (payload_len > 0)
		memcpy(slot->payload, payload, payload_len);

	cluster_grd_outbound_state->reply_dirty_head =
		(cluster_grd_outbound_state->reply_dirty_head + 1) % PGRAC_GES_REPLY_DIRTY_BUDGET;
	cluster_grd_outbound_state->reply_dirty_count++;
	cluster_grd_inc_ges_reply_deferred();
}

static void
cleanup_dirty_push(uint32 dest_node_id, const void *payload, uint16 payload_len)
{
	ClusterGrdOutboundSlot *slot;

	if (payload_len > PGRAC_GES_OUTBOUND_PAYLOAD_MAX)
		return;

	/* Bounded same as reply_dirty.  Cleanup release is best-effort:
	 * if dirty-list also full, drop (LockReleaseAll cannot wait). */
	if (cluster_grd_outbound_state->cleanup_dirty_count >= PGRAC_GES_CLEANUP_DIRTY_BUDGET) {
		cluster_grd_outbound_state->cleanup_dirty_tail =
			(cluster_grd_outbound_state->cleanup_dirty_tail + 1) % PGRAC_GES_CLEANUP_DIRTY_BUDGET;
		cluster_grd_outbound_state->cleanup_dirty_count--;
	}

	slot = &cluster_grd_outbound_state
				->cleanup_dirty[cluster_grd_outbound_state->cleanup_dirty_head];
	slot->dest_node_id = dest_node_id;
	slot->msg_type = 4; /* GES_REQUEST opcode=RELEASE */
	slot->origin = CLUSTER_GRD_OUTBOUND_CLEANUP_RELEASE;
	slot->payload_len = payload_len;
	if (payload_len > 0)
		memcpy(slot->payload, payload, payload_len);

	cluster_grd_outbound_state->cleanup_dirty_head =
		(cluster_grd_outbound_state->cleanup_dirty_head + 1) % PGRAC_GES_CLEANUP_DIRTY_BUDGET;
	cluster_grd_outbound_state->cleanup_dirty_count++;
	cluster_grd_inc_ges_cleanup_deferred();
}


/* ============================================================
 * 3 producer enqueue paths.
 * ============================================================ */

bool
cluster_grd_outbound_enqueue_backend_request(uint32 dest_node_id, const void *payload,
											 uint16 payload_len)
{
	bool ok;

	Assert(cluster_grd_outbound_state != NULL);

	LWLockAcquire(cluster_grd_outbound_lock, LW_EXCLUSIVE);

	/* Reserved pool: BACKEND_REQUEST may consume ring slots only up to
	 * CAPACITY - RESERVED_BUDGET (leaves room for LMON_REPLY).  Above
	 * that boundary, return false → backend wait latch + timeout. */
	if (cluster_grd_outbound_state->ring_count >=
		(PGRAC_GES_OUTBOUND_RING_CAPACITY - PGRAC_GES_OUTBOUND_LMON_REPLY_RESERVED_BUDGET)) {
		LWLockRelease(cluster_grd_outbound_lock);
		return false;
	}

	ok = ring_push(4 /* GES_REQUEST */, CLUSTER_GRD_OUTBOUND_BACKEND_REQUEST, dest_node_id,
				   payload, payload_len);
	LWLockRelease(cluster_grd_outbound_lock);
	return ok;
}

void
cluster_grd_outbound_enqueue_lmon_reply(uint32 dest_node_id, const void *payload,
										uint16 payload_len)
{
	Assert(cluster_grd_outbound_state != NULL);

	LWLockAcquire(cluster_grd_outbound_lock, LW_EXCLUSIVE);

	/* LMON_REPLY may consume the full ring capacity (it owns the
	 * reserved pool).  If ring saturated → reply dirty-list (P1.1
	 * REJECT_BUSY 100% 可落地 contract). */
	if (!ring_push(5 /* GES_REPLY */, CLUSTER_GRD_OUTBOUND_LMON_REPLY, dest_node_id, payload,
				   payload_len))
		reply_dirty_push(dest_node_id, payload, payload_len);

	LWLockRelease(cluster_grd_outbound_lock);
}

void
cluster_grd_outbound_enqueue_cleanup_release(uint32 dest_node_id, const void *payload,
											 uint16 payload_len)
{
	Assert(cluster_grd_outbound_state != NULL);

	LWLockAcquire(cluster_grd_outbound_lock, LW_EXCLUSIVE);

	/* CLEANUP_RELEASE shares main ring with backend pool.  If full →
	 * cleanup dirty-list (LockReleaseAll cannot wait). */
	if (!ring_push(4 /* GES_REQUEST opcode=RELEASE */, CLUSTER_GRD_OUTBOUND_CLEANUP_RELEASE,
				   dest_node_id, payload, payload_len))
		cleanup_dirty_push(dest_node_id, payload, payload_len);

	LWLockRelease(cluster_grd_outbound_lock);
}


/* ============================================================
 * LMON-side consumer.
 * ============================================================ */

bool
cluster_grd_outbound_dequeue(ClusterGrdOutboundSlot *out)
{
	bool got = false;

	Assert(cluster_grd_outbound_state != NULL);
	Assert(out != NULL);

	LWLockAcquire(cluster_grd_outbound_lock, LW_EXCLUSIVE);
	if (cluster_grd_outbound_state->ring_count > 0) {
		*out = cluster_grd_outbound_state->ring[cluster_grd_outbound_state->ring_tail];
		cluster_grd_outbound_state->ring_tail =
			(cluster_grd_outbound_state->ring_tail + 1) % PGRAC_GES_OUTBOUND_RING_CAPACITY;
		cluster_grd_outbound_state->ring_count--;
		got = true;
	}
	LWLockRelease(cluster_grd_outbound_lock);
	return got;
}

int
cluster_grd_outbound_drain_dirty_lists(void)
{
	int drained = 0;

	Assert(cluster_grd_outbound_state != NULL);

	LWLockAcquire(cluster_grd_outbound_lock, LW_EXCLUSIVE);

	/* Drain reply dirty first (P1.1 priority — REJECT_BUSY must converge). */
	while (cluster_grd_outbound_state->reply_dirty_count > 0 &&
		   cluster_grd_outbound_state->ring_count < PGRAC_GES_OUTBOUND_RING_CAPACITY) {
		ClusterGrdOutboundSlot *src =
			&cluster_grd_outbound_state->reply_dirty[cluster_grd_outbound_state->reply_dirty_tail];
		if (!ring_push(src->msg_type, src->origin, src->dest_node_id, src->payload,
					   src->payload_len))
			break;
		cluster_grd_outbound_state->reply_dirty_tail =
			(cluster_grd_outbound_state->reply_dirty_tail + 1) % PGRAC_GES_REPLY_DIRTY_BUDGET;
		cluster_grd_outbound_state->reply_dirty_count--;
		drained++;
	}

	/* Drain cleanup dirty after reply. */
	while (cluster_grd_outbound_state->cleanup_dirty_count > 0 &&
		   cluster_grd_outbound_state->ring_count < PGRAC_GES_OUTBOUND_RING_CAPACITY) {
		ClusterGrdOutboundSlot *src =
			&cluster_grd_outbound_state
				 ->cleanup_dirty[cluster_grd_outbound_state->cleanup_dirty_tail];
		if (!ring_push(src->msg_type, src->origin, src->dest_node_id, src->payload,
					   src->payload_len))
			break;
		cluster_grd_outbound_state->cleanup_dirty_tail =
			(cluster_grd_outbound_state->cleanup_dirty_tail + 1) % PGRAC_GES_CLEANUP_DIRTY_BUDGET;
		cluster_grd_outbound_state->cleanup_dirty_count--;
		drained++;
	}

	LWLockRelease(cluster_grd_outbound_lock);
	return drained;
}


/* ============================================================
 * Observability accessor.
 * ============================================================ */

uint32
cluster_grd_outbound_ring_depth(void)
{
	uint32 depth;
	if (cluster_grd_outbound_state == NULL)
		return 0;
	LWLockAcquire(cluster_grd_outbound_lock, LW_SHARED);
	depth = cluster_grd_outbound_state->ring_count;
	LWLockRelease(cluster_grd_outbound_lock);
	return depth;
}

uint32
cluster_grd_outbound_reply_dirty_depth(void)
{
	uint32 depth;
	if (cluster_grd_outbound_state == NULL)
		return 0;
	LWLockAcquire(cluster_grd_outbound_lock, LW_SHARED);
	depth = cluster_grd_outbound_state->reply_dirty_count;
	LWLockRelease(cluster_grd_outbound_lock);
	return depth;
}

uint32
cluster_grd_outbound_cleanup_dirty_depth(void)
{
	uint32 depth;
	if (cluster_grd_outbound_state == NULL)
		return 0;
	LWLockAcquire(cluster_grd_outbound_lock, LW_SHARED);
	depth = cluster_grd_outbound_state->cleanup_dirty_count;
	LWLockRelease(cluster_grd_outbound_lock);
	return depth;
}
