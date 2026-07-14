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
 *	    - reply dirty overflow → drop oldest + counter; correctness cleanup
 *	      overflow → explicit fail-closed PANIC (never overwrite silently).
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

#include "cluster/cluster_ges.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_ic_rdma.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_lms.h" /* PGRAC: spec-7.2 D4 DATA-ring routing */
#include "cluster/cluster_guc.h" /* PGRAC: spec-7.3 D4 — cluster_lms_workers */
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"


/*
 * spec-5.8 D8 — couple the ring slot payload capacity to the largest wire
 * payload that traverses it.  GesRequestPayload is the biggest (72B after the
 * D1c/D1e growth) and is what cluster_grd_outbound_enqueue_backend_request
 * sends on the cross-node GES request path.  If a future change grows a ring
 * payload past PGRAC_GES_OUTBOUND_PAYLOAD_MAX this fails at compile time —
 * preventing a recurrence of the D1e miss where the payload grew 64->72 but
 * this slot stayed 64, so ring_push silently rejected every cross-node GES
 * REQUEST (latent until a 2-node run).
 */
StaticAssertDecl(PGRAC_GES_OUTBOUND_PAYLOAD_MAX >= sizeof(GesRequestPayload),
				 "GES outbound ring slot must hold a full GesRequestPayload");
StaticAssertDecl(PGRAC_GES_OUTBOUND_PAYLOAD_MAX >= sizeof(GesReplyPayload),
				 "GES outbound ring slot must hold a full GesReplyPayload");


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

	/* Lifetime LOG-once state + exported threshold-crossing counters. */
	uint8 cleanup_retry_warned_mask;
	uint64 cleanup_retry_warn50_count;
	uint64 cleanup_retry_warn90_count;
} ClusterGrdOutboundShared;

static ClusterGrdOutboundShared *cluster_grd_outbound_state = NULL;
static LWLock *cluster_grd_outbound_lock = NULL;

#define CLEANUP_RETRY_WARN50_BIT 0x01
#define CLEANUP_RETRY_WARN90_BIT 0x02


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

	cluster_grd_outbound_state
		= ShmemInitStruct("pgrac cluster grd outbound", cluster_grd_outbound_shmem_size(), &found);
	if (!found) {
		memset(cluster_grd_outbound_state, 0, sizeof(*cluster_grd_outbound_state));
	}

	/* Resolve LWLock tranche (registered via cluster_grd_request_lwlocks
	 * → process_shmem_requests).  Bootstrap mode skips request phase →
	 * tranche not registered → skip lock resolution (no consumer runs
	 * in bootstrap;  postmaster re-runs init_fn after process_shmem_
	 * requests has populated the tranche). */
	if (!IsBootstrapProcessingMode())
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

	cluster_grd_outbound_state->ring_head
		= (cluster_grd_outbound_state->ring_head + 1) % PGRAC_GES_OUTBOUND_RING_CAPACITY;
	cluster_grd_outbound_state->ring_count++;
	/* PGRAC: spec-7.2 D1 — mark the drain family dirty inside the push
	 * helper so every producer (current and future) is covered. */
	cluster_lmon_duty_mark_dirty(CLUSTER_LMON_DUTY_GRD_OUTBOUND);
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
		cluster_grd_outbound_state->reply_dirty_tail
			= (cluster_grd_outbound_state->reply_dirty_tail + 1) % PGRAC_GES_REPLY_DIRTY_BUDGET;
		cluster_grd_outbound_state->reply_dirty_count--;
		cluster_grd_inc_ges_reply_dropped();
	}

	slot = &cluster_grd_outbound_state->reply_dirty[cluster_grd_outbound_state->reply_dirty_head];
	slot->dest_node_id = dest_node_id;
	slot->msg_type = PGRAC_IC_MSG_GES_REPLY;
	slot->origin = CLUSTER_GRD_OUTBOUND_LMON_REPLY;
	slot->payload_len = payload_len;
	if (payload_len > 0)
		memcpy(slot->payload, payload, payload_len);

	cluster_grd_outbound_state->reply_dirty_head
		= (cluster_grd_outbound_state->reply_dirty_head + 1) % PGRAC_GES_REPLY_DIRTY_BUDGET;
	cluster_grd_outbound_state->reply_dirty_count++;
	cluster_grd_inc_ges_reply_deferred();
	cluster_lmon_duty_mark_dirty(CLUSTER_LMON_DUTY_GRD_OUTBOUND); /* spec-7.2 D1 */
}

static bool
cleanup_dirty_push(uint8 msg_type, uint8 origin, uint32 dest_node_id, const void *payload,
				   uint16 payload_len, uint8 *new_warnings, uint32 *depth_after)
{
	ClusterGrdOutboundSlot *slot;
	uint8 warnings = 0;

	if (new_warnings != NULL)
		*new_warnings = 0;
	if (depth_after != NULL)
		*depth_after = cluster_grd_outbound_state->cleanup_dirty_count;

	if (payload_len > PGRAC_GES_OUTBOUND_PAYLOAD_MAX)
		return false;

	/*
	 * P0#3: cleanup frames carry correctness state (exact CANCEL_WAIT / RELEASE).
	 * Never overwrite the oldest entry.  The void producer APIs turn false into
	 * an explicit fail-closed PANIC after releasing the outbound LWLock.
	 */
	if (cluster_grd_outbound_state->cleanup_dirty_count >= PGRAC_GES_CLEANUP_DIRTY_BUDGET)
		return false;

	slot = &cluster_grd_outbound_state
				->cleanup_dirty[cluster_grd_outbound_state->cleanup_dirty_head];
	slot->dest_node_id = dest_node_id;
	slot->msg_type = msg_type;
	slot->origin = origin;
	slot->payload_len = payload_len;
	if (payload_len > 0)
		memcpy(slot->payload, payload, payload_len);

	cluster_grd_outbound_state->cleanup_dirty_head
		= (cluster_grd_outbound_state->cleanup_dirty_head + 1) % PGRAC_GES_CLEANUP_DIRTY_BUDGET;
	cluster_grd_outbound_state->cleanup_dirty_count++;
	if (cluster_grd_outbound_state->cleanup_dirty_count >= PGRAC_GES_CLEANUP_DIRTY_WARN50_DEPTH
		&& (cluster_grd_outbound_state->cleanup_retry_warned_mask & CLEANUP_RETRY_WARN50_BIT)
			   == 0) {
		cluster_grd_outbound_state->cleanup_retry_warned_mask |= CLEANUP_RETRY_WARN50_BIT;
		cluster_grd_outbound_state->cleanup_retry_warn50_count++;
		warnings |= CLEANUP_RETRY_WARN50_BIT;
	}
	if (cluster_grd_outbound_state->cleanup_dirty_count >= PGRAC_GES_CLEANUP_DIRTY_WARN90_DEPTH
		&& (cluster_grd_outbound_state->cleanup_retry_warned_mask & CLEANUP_RETRY_WARN90_BIT)
			   == 0) {
		cluster_grd_outbound_state->cleanup_retry_warned_mask |= CLEANUP_RETRY_WARN90_BIT;
		cluster_grd_outbound_state->cleanup_retry_warn90_count++;
		warnings |= CLEANUP_RETRY_WARN90_BIT;
	}
	if (new_warnings != NULL)
		*new_warnings = warnings;
	if (depth_after != NULL)
		*depth_after = cluster_grd_outbound_state->cleanup_dirty_count;
	cluster_grd_inc_ges_cleanup_deferred();
	cluster_lmon_duty_mark_dirty(CLUSTER_LMON_DUTY_GRD_OUTBOUND); /* spec-7.2 D1 */
	return true;
}

static bool
requeue_slot(const ClusterGrdOutboundSlot *slot, uint8 *new_warnings, uint32 *depth_after)
{
	Assert(slot != NULL);

	switch ((ClusterGrdOutboundOrigin)slot->origin) {
	case CLUSTER_GRD_OUTBOUND_LMON_REPLY:
		reply_dirty_push(slot->dest_node_id, slot->payload, slot->payload_len);
		return true;
	case CLUSTER_GRD_OUTBOUND_CLEANUP_RELEASE:
	case CLUSTER_GRD_OUTBOUND_LMD_CANCEL:
	case CLUSTER_GRD_OUTBOUND_LMS_NATIVE_PROBE:
		return cleanup_dirty_push(slot->msg_type, slot->origin, slot->dest_node_id, slot->payload,
								  slot->payload_len, new_warnings, depth_after);
	case CLUSTER_GRD_OUTBOUND_BACKEND_REQUEST:
	default:
		return ring_push(slot->msg_type, slot->origin, slot->dest_node_id, slot->payload,
						 slot->payload_len);
	}
}

static void
cleanup_retry_log_pressure(uint8 new_warnings, uint32 depth)
{
	if ((new_warnings & CLEANUP_RETRY_WARN50_BIT) != 0)
		ereport(LOG, (errmsg_internal("cluster GES reliable cleanup retry queue reached 50%% "
									  "(depth=%u capacity=%u max_backends=%d lmon_interval_ms=%d); "
									  "warning is emitted once per postmaster lifetime",
									  depth, PGRAC_GES_CLEANUP_DIRTY_BUDGET, MaxBackends,
									  cluster_lmon_main_loop_interval)));
	if ((new_warnings & CLEANUP_RETRY_WARN90_BIT) != 0)
		ereport(LOG, (errmsg_internal("cluster GES reliable cleanup retry queue reached 90%% "
									  "(depth=%u capacity=%u max_backends=%d lmon_interval_ms=%d); "
									  "exhaustion will PANIC fail closed, warning is emitted once "
									  "per postmaster lifetime",
									  depth, PGRAC_GES_CLEANUP_DIRTY_BUDGET, MaxBackends,
									  cluster_lmon_main_loop_interval)));
}

static inline bool
cleanup_origin_requires_retry(uint8 origin)
{
	return origin == CLUSTER_GRD_OUTBOUND_CLEANUP_RELEASE
		   || origin == CLUSTER_GRD_OUTBOUND_LMD_CANCEL
		   || origin == CLUSTER_GRD_OUTBOUND_LMS_NATIVE_PROBE;
}

static void
cleanup_retry_exhausted(uint8 origin, uint32 dest_node_id)
{
	ereport(PANIC,
			(errmsg_internal("cluster GES reliable cleanup retry queue exhausted "
							 "(capacity=%u origin=%u dest=%u); refusing to lose cleanup state",
							 PGRAC_GES_CLEANUP_DIRTY_BUDGET, (uint32)origin, dest_node_id)));
}


/* ============================================================
 * 3 producer enqueue paths.
 * ============================================================ */

bool
cluster_grd_outbound_enqueue_backend_request(uint32 dest_node_id, const void *payload,
											 uint16 payload_len)
{
	return cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_GES_REQUEST, dest_node_id, payload,
													payload_len);
}

bool
cluster_grd_outbound_enqueue_backend_msg(uint8 msg_type, uint32 dest_node_id, const void *payload,
										 uint16 payload_len)
{
	bool ok;

	Assert(cluster_grd_outbound_state != NULL);

	/*
	 * PGRAC: spec-7.2 D4 — plane routing at the single backend staging
	 * entry:  a msg_type registered on the DATA plane goes to the DATA
	 * twin ring (LMS drains + sends);  everything else stays on this
	 * CONTROL ring (LMON).  Before the plane flip every type reads
	 * CONTROL here, so this branch is dormant until the flip commit.
	 */
	{
		const ClusterICMsgTypeInfo *pinfo = cluster_ic_get_msg_type_info(msg_type);

		if (pinfo != NULL && (ClusterICPlane)pinfo->plane == CLUSTER_IC_PLANE_DATA) {
			/*
			 * PGRAC: spec-7.3 D4 (8.A) — route this frame to the worker that
			 * owns its tag's shard, so every message of a tag rides one
			 * worker<->worker stream (per-tag FIFO).  -1 = a DATA frame with
			 * no routable tag → refuse to stage it fail-closed rather than
			 * default a worker (a misroute would break message order).
			 */
			int worker = cluster_gcs_block_payload_shard(msg_type, payload, payload_len,
														 cluster_lms_workers);

			if (worker < 0)
				return false;
			return cluster_lms_outbound_enqueue(worker, msg_type, dest_node_id, payload,
												payload_len);
		}
	}

	LWLockAcquire(cluster_grd_outbound_lock, LW_EXCLUSIVE);

	/* Reserved pool: BACKEND_REQUEST may consume ring slots only up to
	 * CAPACITY - RESERVED_BUDGET (leaves room for LMON_REPLY).  Above
	 * that boundary, return false → backend wait latch + timeout. */
	if (cluster_grd_outbound_state->ring_count
		>= (PGRAC_GES_OUTBOUND_RING_CAPACITY - PGRAC_GES_OUTBOUND_LMON_REPLY_RESERVED_BUDGET)) {
		LWLockRelease(cluster_grd_outbound_lock);
		return false;
	}

	ok = ring_push(msg_type, CLUSTER_GRD_OUTBOUND_BACKEND_REQUEST, dest_node_id, payload,
				   payload_len);
	LWLockRelease(cluster_grd_outbound_lock);
	if (ok)
		cluster_lmon_wakeup();
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
	if (!ring_push(PGRAC_IC_MSG_GES_REPLY, CLUSTER_GRD_OUTBOUND_LMON_REPLY, dest_node_id, payload,
				   payload_len))
		reply_dirty_push(dest_node_id, payload, payload_len);

	LWLockRelease(cluster_grd_outbound_lock);
	cluster_lmon_wakeup();
}

void
cluster_grd_outbound_enqueue_cleanup_release(uint32 dest_node_id, const void *payload,
											 uint16 payload_len)
{
	bool queued;
	uint8 new_warnings = 0;
	uint32 depth_after = 0;

	Assert(cluster_grd_outbound_state != NULL);

	LWLockAcquire(cluster_grd_outbound_lock, LW_EXCLUSIVE);

	/* CLEANUP_RELEASE shares main ring with backend pool.  If full →
	 * fixed reliable retry list (LockReleaseAll cannot wait). */
	queued = ring_push(PGRAC_IC_MSG_GES_REQUEST, CLUSTER_GRD_OUTBOUND_CLEANUP_RELEASE, dest_node_id,
					   payload, payload_len);
	if (!queued)
		queued
			= cleanup_dirty_push(PGRAC_IC_MSG_GES_REQUEST, CLUSTER_GRD_OUTBOUND_CLEANUP_RELEASE,
								 dest_node_id, payload, payload_len, &new_warnings, &depth_after);

	LWLockRelease(cluster_grd_outbound_lock);
	cleanup_retry_log_pressure(new_warnings, depth_after);
	if (!queued)
		cleanup_retry_exhausted(CLUSTER_GRD_OUTBOUND_CLEANUP_RELEASE, dest_node_id);
	cluster_lmon_wakeup();
}

/*
 * spec-2.24 D4 — LMD coordinator cross-node victim cancel forwarding.
 *
 *	Reliability contract per spec-2.24 §1.4 example 2 — cancel forward
 *	MUST NOT use the silent-fail backend_request path (loss → deadlock
 *	not resolved).  Mirror CLEANUP_RELEASE semantics:  share main ring;
 *	on full → reliable cleanup retry list (no overwrite).  cluster_grd_outbound origin
 *	CLUSTER_GRD_OUTBOUND_LMD_CANCEL identifies the producer in pg_stat
 *	rollups.
 */
void
cluster_grd_outbound_enqueue_lmd_cancel(uint32 dest_node_id, const void *payload,
										uint16 payload_len)
{
	bool queued;
	uint8 new_warnings = 0;
	uint32 depth_after = 0;

	Assert(cluster_grd_outbound_state != NULL);

	LWLockAcquire(cluster_grd_outbound_lock, LW_EXCLUSIVE);

	queued = ring_push(PGRAC_IC_MSG_GES_REQUEST, CLUSTER_GRD_OUTBOUND_LMD_CANCEL, dest_node_id,
					   payload, payload_len);
	if (!queued)
		queued
			= cleanup_dirty_push(PGRAC_IC_MSG_GES_REQUEST, CLUSTER_GRD_OUTBOUND_LMD_CANCEL,
								 dest_node_id, payload, payload_len, &new_warnings, &depth_after);

	LWLockRelease(cluster_grd_outbound_lock);
	cleanup_retry_log_pressure(new_warnings, depth_after);
	if (!queued)
		cleanup_retry_exhausted(CLUSTER_GRD_OUTBOUND_LMD_CANCEL, dest_node_id);
	cluster_lmon_wakeup();
}

/*
 * spec-2.25 D7 — LMS native-lock probe nofail enqueue.
 *
 *	Reliability contract per spec-2.25 §1.4:  probe correctness gate at
 *	the LMS layer requires fan-out reach the wire (lost request → peer
 *	never replies → LMS collector slot retries on poll, but if every probe
 *	silently drops the requester eventually hits the retry budget and
 *	returns 53R83 erroneously).  Mirror CLEANUP_RELEASE / LMD_CANCEL
 *	semantics:  ring full → reliable cleanup retry list per L141 family
 *	pattern (the CV-broadcast wake is owned by the LMS collector slot,
 *	not this enqueue path — outbound ring is LMON-polled).
 *
 *	Same call site for both request (opcode 9) and reply (opcode 10) —
 *	origin = CLUSTER_GRD_OUTBOUND_LMS_NATIVE_PROBE identifies both in
 *	pg_stat_cluster_state rollups.
 */
void
cluster_grd_outbound_enqueue_lms_native_probe(uint32 dest_node_id, const void *payload,
											  uint16 payload_len)
{
	bool queued;
	uint8 new_warnings = 0;
	uint32 depth_after = 0;

	Assert(cluster_grd_outbound_state != NULL);

	LWLockAcquire(cluster_grd_outbound_lock, LW_EXCLUSIVE);

	queued = ring_push(PGRAC_IC_MSG_GES_REQUEST, CLUSTER_GRD_OUTBOUND_LMS_NATIVE_PROBE,
					   dest_node_id, payload, payload_len);
	if (!queued)
		queued
			= cleanup_dirty_push(PGRAC_IC_MSG_GES_REQUEST, CLUSTER_GRD_OUTBOUND_LMS_NATIVE_PROBE,
								 dest_node_id, payload, payload_len, &new_warnings, &depth_after);

	LWLockRelease(cluster_grd_outbound_lock);
	cleanup_retry_log_pressure(new_warnings, depth_after);
	if (!queued)
		cleanup_retry_exhausted(CLUSTER_GRD_OUTBOUND_LMS_NATIVE_PROBE, dest_node_id);
	cluster_lmon_wakeup();
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
		cluster_grd_outbound_state->ring_tail
			= (cluster_grd_outbound_state->ring_tail + 1) % PGRAC_GES_OUTBOUND_RING_CAPACITY;
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
	while (cluster_grd_outbound_state->reply_dirty_count > 0
		   && cluster_grd_outbound_state->ring_count < PGRAC_GES_OUTBOUND_RING_CAPACITY) {
		ClusterGrdOutboundSlot *src
			= &cluster_grd_outbound_state
				   ->reply_dirty[cluster_grd_outbound_state->reply_dirty_tail];
		if (!ring_push(src->msg_type, src->origin, src->dest_node_id, src->payload,
					   src->payload_len))
			break;
		cluster_grd_outbound_state->reply_dirty_tail
			= (cluster_grd_outbound_state->reply_dirty_tail + 1) % PGRAC_GES_REPLY_DIRTY_BUDGET;
		cluster_grd_outbound_state->reply_dirty_count--;
		drained++;
	}

	/* Drain cleanup dirty after reply. */
	while (cluster_grd_outbound_state->cleanup_dirty_count > 0
		   && cluster_grd_outbound_state->ring_count < PGRAC_GES_OUTBOUND_RING_CAPACITY) {
		ClusterGrdOutboundSlot *src
			= &cluster_grd_outbound_state
				   ->cleanup_dirty[cluster_grd_outbound_state->cleanup_dirty_tail];
		if (!ring_push(src->msg_type, src->origin, src->dest_node_id, src->payload,
					   src->payload_len))
			break;
		cluster_grd_outbound_state->cleanup_dirty_tail
			= (cluster_grd_outbound_state->cleanup_dirty_tail + 1) % PGRAC_GES_CLEANUP_DIRTY_BUDGET;
		cluster_grd_outbound_state->cleanup_dirty_count--;
		drained++;
	}

	LWLockRelease(cluster_grd_outbound_lock);
	return drained;
}

static void
requeue_after_send_refusal(const ClusterGrdOutboundSlot *slot)
{
	bool queued;
	uint8 new_warnings = 0;
	uint32 depth_after = 0;

	LWLockAcquire(cluster_grd_outbound_lock, LW_EXCLUSIVE);
	queued = requeue_slot(slot, &new_warnings, &depth_after);
	LWLockRelease(cluster_grd_outbound_lock);
	cleanup_retry_log_pressure(new_warnings, depth_after);

	if (!queued && cleanup_origin_requires_retry(slot->origin))
		cleanup_retry_exhausted(slot->origin, slot->dest_node_id);
}

int
cluster_grd_outbound_lmon_drain_send(void)
{
	ClusterGrdOutboundSlot slot;
	int sent = 0;
	int scanned = 0;
	bool peer_blocked[CLUSTER_MAX_NODES] = { 0 };

	if (cluster_grd_outbound_state == NULL || cluster_grd_outbound_lock == NULL)
		return 0;

	(void)cluster_grd_outbound_drain_dirty_lists();

	/*
	 * GCS serve-stall round-5 — four-state send ownership contract (see
	 * ClusterICSendResult).  WOULD_BLOCK now always means the transport
	 * ADMITTED the frame (tier1 per-peer FIFO), so the old
	 * "WOULD_BLOCK && peer_has_pending" accepted/pending probe and the
	 * pre-send pending check are gone;  NOT_ADMITTED is an explicit
	 * refusal — requeue the slot (its origin-class queue) and stop
	 * handing this peer frames for the rest of the batch, WITHOUT
	 * breaking the batch: one mid-HELLO peer must not head-of-line
	 * block GCS_REQUEST traffic to every other master (this ring is the
	 * backend REQUEST staging path, gcs.c owner-plane rule).  scanned
	 * bounds the batch so requeued frames cannot spin it forever.
	 */
	while (sent < PGRAC_GES_OUTBOUND_LMON_DRAIN_BATCH
		   && scanned < PGRAC_GES_OUTBOUND_LMON_DRAIN_BATCH
		   && cluster_grd_outbound_dequeue(&slot)) {
		ClusterICSendResult rc;

		scanned++;

		if (slot.dest_node_id < CLUSTER_MAX_NODES && peer_blocked[slot.dest_node_id]) {
			requeue_after_send_refusal(&slot);
			continue;
		}

		if (slot.msg_type == PGRAC_IC_MSG_GCS_BLOCK_REQUEST
			&& slot.payload_len == sizeof(GcsBlockRequestPayload))
			cluster_gcs_block_lmon_prepare_outbound_request((GcsBlockRequestPayload *)slot.payload,
															(int32)slot.dest_node_id);

		rc = cluster_ic_send_envelope(slot.msg_type, (int32)slot.dest_node_id,
									  slot.payload_len > 0 ? slot.payload : NULL, slot.payload_len);
		switch (rc) {
		case CLUSTER_IC_SEND_DONE:
		case CLUSTER_IC_SEND_WOULD_BLOCK:
			/* On the wire or admitted (transport owns a copy). */
			sent++;
			break;
		case CLUSTER_IC_SEND_NOT_ADMITTED:
			if (slot.dest_node_id < CLUSTER_MAX_NODES)
				peer_blocked[slot.dest_node_id] = true;
			requeue_after_send_refusal(&slot);
			break;
		case CLUSTER_IC_SEND_HARD_ERROR:
			/* Backend requests/replies have higher-layer retry.  Exact cleanup
			 * frames do not: retain them in the fixed retry list until transport
			 * admission (or explicit fail-closed exhaustion). */
			if (cleanup_origin_requires_retry(slot.origin)) {
				if (slot.dest_node_id < CLUSTER_MAX_NODES)
					peer_blocked[slot.dest_node_id] = true;
				requeue_after_send_refusal(&slot);
			}
			break;
		}
	}

	return sent;
}


/* ============================================================
 * Observability accessor.
 * ============================================================ */

uint32
cluster_grd_outbound_ring_depth(void)
{
	uint32 depth;
	if (cluster_grd_outbound_state == NULL || cluster_grd_outbound_lock == NULL)
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
	if (cluster_grd_outbound_state == NULL || cluster_grd_outbound_lock == NULL)
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
	if (cluster_grd_outbound_state == NULL || cluster_grd_outbound_lock == NULL)
		return 0;
	LWLockAcquire(cluster_grd_outbound_lock, LW_SHARED);
	depth = cluster_grd_outbound_state->cleanup_dirty_count;
	LWLockRelease(cluster_grd_outbound_lock);
	return depth;
}

uint64
cluster_grd_outbound_cleanup_retry_warn50_count(void)
{
	uint64 count;

	if (cluster_grd_outbound_state == NULL || cluster_grd_outbound_lock == NULL)
		return 0;
	LWLockAcquire(cluster_grd_outbound_lock, LW_SHARED);
	count = cluster_grd_outbound_state->cleanup_retry_warn50_count;
	LWLockRelease(cluster_grd_outbound_lock);
	return count;
}

uint64
cluster_grd_outbound_cleanup_retry_warn90_count(void)
{
	uint64 count;

	if (cluster_grd_outbound_state == NULL || cluster_grd_outbound_lock == NULL)
		return 0;
	LWLockAcquire(cluster_grd_outbound_lock, LW_SHARED);
	count = cluster_grd_outbound_state->cleanup_retry_warn90_count;
	LWLockRelease(cluster_grd_outbound_lock);
	return count;
}
