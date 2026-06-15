/*-------------------------------------------------------------------------
 *
 * cluster_write_fence.c
 *	  pgrac internal cooperative write-fence -- local token region + hot-path
 *	  judge wrapper (spec-4.12 D3, split-brain recovery guard).
 *
 *	  The "pgrac cluster write fence" shmem region holds the local write-fence
 *	  token: the authorized_epoch + lease + self_fenced bit that qvotec (D2)
 *	  refreshes every poll from the durable voting-disk fence marker.  The hot
 *	  shared-storage write paths (D5) call cluster_write_fence_allowed() -- a
 *	  pure-read, no-throw, critical-section-safe check (no LWLock, no durable
 *	  I/O) -- and fail closed by context (ERROR / PANIC / BLOCKED) when this node
 *	  is stale / superseded / lease-expired / self-fenced.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_write_fence.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.12-cooperative-write-fence.md (FROZEN v1.0)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "storage/shmem.h"
#include "utils/timestamp.h"

#include "cluster/cluster_epoch.h"		 /* cluster_epoch_get_current */
#include "cluster/cluster_guc.h"		 /* cluster_node_id */
#include "cluster/cluster_qvotec.h"		 /* ClusterVotingSlot (marker layout asserts) */
#include "cluster/cluster_shmem.h"		 /* cluster_shmem_register_region */
#include "cluster/cluster_write_fence.h" /* region + judge + wrapper + marker */

/*
 * spec-4.12 D1: pin the fence-marker on-disk layout against the voting slot, so a
 * future change to either side fails to compile rather than silently corrupting
 * the durable marker (the marker rides in _reserved1, protected by the slot CRC).
 */
StaticAssertDecl(offsetof(ClusterVotingSlot, _reserved1) == 128,
				 "fence marker assumes voting slot _reserved1 at offset 128");
StaticAssertDecl(sizeof(ClusterFenceMarker) == CLUSTER_FENCE_MARKER_BYTES,
				 "ClusterFenceMarker must be exactly CLUSTER_FENCE_MARKER_BYTES");
StaticAssertDecl(CLUSTER_FENCE_MARKER_BYTES <= sizeof(((ClusterVotingSlot *)0)->_reserved1),
				 "fence marker must fit in the voting slot _reserved1 area");

/*
 * GUC storage (registered by cluster_guc.c in D7).  Default OFF until the
 * mechanism is fully wired (D2 lease refresh + D4 marker write); D7 flips the
 * registered default to ON for multi-node + shared-fs.
 */
int cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_OFF;
int cluster_write_fence_lease_ms = 6000;

static ClusterWriteFenceShmem *cluster_write_fence_shmem = NULL;

static Size
cluster_write_fence_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterWriteFenceShmem));
}

static void
cluster_write_fence_shmem_init(void)
{
	bool found;

	cluster_write_fence_shmem = (ClusterWriteFenceShmem *)ShmemInitStruct(
		"pgrac cluster write fence", cluster_write_fence_shmem_size(), &found);
	if (!found) {
		/*
		 * Postmaster restart rebuilds shmem: a node starts with no fence token
		 * (authorized_epoch 0, lease 0).  With enforcement ON the hot gate then
		 * fails closed (lease 0 -> expired) until qvotec's first poll refreshes
		 * the token from the durable marker (D2) -- fail-closed-until-proven is
		 * the correct startup posture (8.A).
		 */
		pg_atomic_init_u64(&cluster_write_fence_shmem->authorized_epoch, 0);
		pg_atomic_init_u64(&cluster_write_fence_shmem->lease_expire_at_us, 0);
		pg_atomic_init_u32(&cluster_write_fence_shmem->self_fenced, 0);
		memset(cluster_write_fence_shmem->fenced_dead_bitmap, 0,
			   sizeof(cluster_write_fence_shmem->fenced_dead_bitmap));
		pg_atomic_init_u64(&cluster_write_fence_shmem->fence_event_id, 0);
		pg_atomic_init_u64(&cluster_write_fence_shmem->hot_gate_blocked, 0);
		pg_atomic_init_u64(&cluster_write_fence_shmem->durable_check_blocked, 0);
		pg_atomic_init_u64(&cluster_write_fence_shmem->minority_marker_ignored, 0);
	}
}

static const ClusterShmemRegion cluster_write_fence_region = {
	.name = "pgrac cluster write fence",
	.size_fn = cluster_write_fence_shmem_size,
	.init_fn = cluster_write_fence_shmem_init,
};

void
cluster_write_fence_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_write_fence_region);
}

/*
 * cluster_write_fence_allowed -- the hot-path wrapper (spec-4.12 D3).  Reads the
 *	live epoch + the shmem token and applies the PURE judge.  No LWLock, no durable
 *	I/O -- safe to call from a critical section (the caller decides the fail-closed
 *	action by context: ERROR / PANIC / BLOCKED).  L110: a detached region (token
 *	not attached) yields enforcement_on with region_attached=false -> fail-closed.
 */
bool
cluster_write_fence_allowed(void)
{
	bool enforcement_on = (cluster_write_fence_enforcement == CLUSTER_WRITE_FENCE_ENFORCE_ON);
	bool region_attached = (cluster_write_fence_shmem != NULL);
	uint64 epoch_current = cluster_epoch_get_current();
	uint64 authorized_epoch = 0;
	uint64 lease_expire_us = 0;
	bool self_fenced = false;
	uint64 now_us;

	if (region_attached) {
		/*
		 * Read the lease FIRST, then barrier, then epoch + self_fenced.  qvotec
		 * (the sole writer, cluster_write_fence_refresh_from_marker) invalidates
		 * the lease, writes epoch/self_fenced, write-barriers, then publishes the
		 * lease LAST.  So a non-expired lease observed here guarantees the epoch /
		 * self_fenced we read next are the matching freshly-published values -- a
		 * stale-epoch torn read can only come with a stale (already-invalidated)
		 * lease, which fails closed.  The only residual is the bounded R4 lease
		 * window (marker on disk but lease not yet aged out), an accepted risk.
		 */
		lease_expire_us = pg_atomic_read_u64(&cluster_write_fence_shmem->lease_expire_at_us);
		pg_read_barrier();
		authorized_epoch = pg_atomic_read_u64(&cluster_write_fence_shmem->authorized_epoch);
		self_fenced = (pg_atomic_read_u32(&cluster_write_fence_shmem->self_fenced) != 0);
	}

	/* TimestampTz is microseconds since 2000-01-01; the lease is in the same unit. */
	now_us = (uint64)GetCurrentTimestamp();

	return cluster_write_fence_decide(enforcement_on, region_attached, epoch_current,
									  authorized_epoch, now_us, lease_expire_us, self_fenced);
}

/*
 * cluster_write_fence_refresh_from_marker -- qvotec poll refresh (spec-4.12 D2).
 *	Publishes the authoritative fence tuple into the local token.  Lease-published-
 *	last protocol (see the read order above): invalidate lease -> write barrier ->
 *	epoch + self_fenced + event_id + bitmap -> write barrier -> publish lease.
 */
void
cluster_write_fence_refresh_from_marker(const ClusterFenceMarker *m, uint64 lease_expire_us)
{
	bool self_fenced;

	if (cluster_write_fence_shmem == NULL)
		return; /* region not attached: nothing to refresh (hot gate fails closed) */

	self_fenced = cluster_fence_marker_node_is_fenced(m->fenced_dead_bitmap, cluster_node_id);

	/* 1. invalidate the lease so any concurrent reader fails closed mid-update. */
	pg_atomic_write_u64(&cluster_write_fence_shmem->lease_expire_at_us, 0);
	pg_write_barrier();

	/* 2. publish the authoritative epoch / self_fenced / identity. */
	pg_atomic_write_u64(&cluster_write_fence_shmem->authorized_epoch, m->fence_epoch);
	pg_atomic_write_u32(&cluster_write_fence_shmem->self_fenced, self_fenced ? 1 : 0);
	pg_atomic_write_u64(&cluster_write_fence_shmem->fence_event_id, m->fence_event_id);
	memcpy(cluster_write_fence_shmem->fenced_dead_bitmap, m->fenced_dead_bitmap,
		   Min(sizeof(cluster_write_fence_shmem->fenced_dead_bitmap),
			   (size_t)CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES));
	pg_write_barrier();

	/* 3. publish the lease LAST -- this is what makes the token usable. */
	pg_atomic_write_u64(&cluster_write_fence_shmem->lease_expire_at_us, lease_expire_us);
}

/*
 * cluster_write_fence_note_minority_marker -- a CRC-ok marker existed but did not
 *	reach quorum-majority (P0a); ignored.  Bump the counter; leave the token alone
 *	so its lease ages out (fail-closed) if no authority is found.
 */
void
cluster_write_fence_note_minority_marker(void)
{
	if (cluster_write_fence_shmem == NULL)
		return;
	pg_atomic_fetch_add_u64(&cluster_write_fence_shmem->minority_marker_ignored, 1);
}

#endif /* USE_PGRAC_CLUSTER */
