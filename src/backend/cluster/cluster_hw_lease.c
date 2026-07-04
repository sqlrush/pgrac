/*-------------------------------------------------------------------------
 *
 * cluster_hw_lease.c
 *	  pgrac spec-6.12d (static) -- per-node HW space leases.
 *
 *	  See cluster_hw_lease.h for the design and the Q19-A materialization
 *	  contract.  This module owns only the node-local bookkeeping of the
 *	  parked (unconsumed) tail of an oversized HW grant; the durable
 *	  double-allocation guard is the existing XLOG_HW_RESERVE HWM
 *	  authority, untouched.  Losing this bookkeeping (crash, LRU
 *	  recycling, GUC flip) therefore degrades to orphan zero pages --
 *	  bloat reclaimed by vacuum, never corruption.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_hw_lease.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_hw_lease.h"
#include "cluster/cluster_shmem.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

ClusterHwLeaseShared *ClusterHwLeaseCtl = NULL;

Size
cluster_hw_lease_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterHwLeaseShared));
}

void
cluster_hw_lease_shmem_init(void)
{
	bool found;

	ClusterHwLeaseCtl = (ClusterHwLeaseShared *)ShmemInitStruct(
		"pgrac cluster hw lease", cluster_hw_lease_shmem_size(), &found);
	if (!found) {
		int i;

		memset(ClusterHwLeaseCtl, 0, sizeof(ClusterHwLeaseShared));
		LWLockInitialize(&ClusterHwLeaseCtl->lock.lock, LWTRANCHE_CLUSTER_HW_LEASE);
		for (i = 0; i < CLUSTER_HW_LEASE_SLOTS; i++)
			ClusterHwLeaseCtl->slots[i].fork = -1;
		pg_atomic_init_u64(&ClusterHwLeaseCtl->d_leased_total, 0);
		pg_atomic_init_u64(&ClusterHwLeaseCtl->d_consumed, 0);
		pg_atomic_init_u64(&ClusterHwLeaseCtl->d_orphan_zero, 0);
		pg_atomic_init_u64(&ClusterHwLeaseCtl->d_lease_grants, 0);
	}
}

static const ClusterShmemRegion cluster_hw_lease_region = {
	.name = "pgrac cluster hw lease",
	.size_fn = cluster_hw_lease_shmem_size,
	.init_fn = cluster_hw_lease_shmem_init,
	.lwlock_count = 0, /* embedded LWLockPadded, tranche-initialized */
	.owner_subsys = "cluster_hw_lease",
	.reserved_flags = 0,
};

void
cluster_hw_lease_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_hw_lease_region);
}

/*
 * cluster_hw_lease_active -- true when the static space-affinity lease
 * path applies.  dynamic is rejected at GUC check time (spec-6.3 DRM),
 * so only the static value arms this module.
 */
bool
cluster_hw_lease_active(void)
{
	return cluster_enabled && ClusterHwLeaseCtl != NULL
		   && cluster_space_affinity == CLUSTER_SPACE_AFFINITY_STATIC;
}

/*
 * Slot lookup under the caller-held lock; returns NULL when absent.
 */
static ClusterHwLeaseSlot *
lease_slot_find(RelFileLocator rloc, ForkNumber fork)
{
	int i;

	for (i = 0; i < CLUSTER_HW_LEASE_SLOTS; i++) {
		ClusterHwLeaseSlot *slot = &ClusterHwLeaseCtl->slots[i];

		if (slot->fork == (int32)fork && RelFileLocatorEquals(slot->rloc, rloc))
			return slot;
	}
	return NULL;
}

/*
 * cluster_hw_lease_install -- park [start, start+len) as this node's
 * lease for (rloc, fork).
 *
 *	The range is an already-durable HW grant (XLOG_HW_RESERVE flushed) and
 *	already physically zero-extended by the caller, so the file stays
 *	dense and crash simply orphans the range (Q19-A #3).  Replaces any
 *	existing lease for the pair and recycles the LRU slot when the table
 *	is full; both drop paths count the dropped remainder as orphan zero
 *	pages -- the documented fail-safe, never an error.
 */
void
cluster_hw_lease_install(RelFileLocator rloc, ForkNumber fork, BlockNumber start, uint32 len)
{
	ClusterHwLeaseSlot *slot;

	if (len == 0 || !cluster_hw_lease_active())
		return;

	LWLockAcquire(&ClusterHwLeaseCtl->lock.lock, LW_EXCLUSIVE);

	slot = lease_slot_find(rloc, fork);
	if (slot == NULL) {
		int i;
		ClusterHwLeaseSlot *victim = NULL;

		for (i = 0; i < CLUSTER_HW_LEASE_SLOTS; i++) {
			ClusterHwLeaseSlot *cand = &ClusterHwLeaseCtl->slots[i];

			if (cand->fork == -1) {
				victim = cand;
				break;
			}
			if (victim == NULL || cand->use_seq < victim->use_seq)
				victim = cand;
		}
		slot = victim;
		if (slot->fork != -1 && slot->next_block < slot->end_block)
			pg_atomic_fetch_add_u64(&ClusterHwLeaseCtl->d_orphan_zero,
									slot->end_block - slot->next_block);
	} else if (slot->next_block < slot->end_block) {
		/* replacing a live lease orphans its unconsumed remainder */
		pg_atomic_fetch_add_u64(&ClusterHwLeaseCtl->d_orphan_zero,
								slot->end_block - slot->next_block);
	}

	slot->rloc = rloc;
	slot->fork = (int32)fork;
	slot->next_block = start;
	slot->end_block = start + len;
	slot->use_seq = ++ClusterHwLeaseCtl->use_seq;

	pg_atomic_fetch_add_u64(&ClusterHwLeaseCtl->d_leased_total, len);
	pg_atomic_fetch_add_u64(&ClusterHwLeaseCtl->d_lease_grants, 1);

	LWLockRelease(&ClusterHwLeaseCtl->lock.lock);
}

/*
 * cluster_hw_lease_next_block -- consume one parked block for (rloc,
 * fork); InvalidBlockNumber when the pair has no lease or it is
 * exhausted (caller falls through to FSM / authority extend -- the
 * Q10-A HASH_FALLBACK degradation).
 */
BlockNumber
cluster_hw_lease_next_block(RelFileLocator rloc, ForkNumber fork)
{
	ClusterHwLeaseSlot *slot;
	BlockNumber blk = InvalidBlockNumber;

	if (!cluster_hw_lease_active())
		return InvalidBlockNumber;

	LWLockAcquire(&ClusterHwLeaseCtl->lock.lock, LW_EXCLUSIVE);
	slot = lease_slot_find(rloc, fork);
	if (slot != NULL && slot->next_block < slot->end_block) {
		blk = slot->next_block++;
		slot->use_seq = ++ClusterHwLeaseCtl->use_seq;
		pg_atomic_fetch_add_u64(&ClusterHwLeaseCtl->d_consumed, 1);
		if (slot->next_block >= slot->end_block)
			slot->fork = -1; /* exhausted: free the slot */
	}
	LWLockRelease(&ClusterHwLeaseCtl->lock.lock);

	return blk;
}

/*
 * cluster_hw_lease_discard -- drop the lease for (rloc, fork), if any.
 *
 *	Consumer-driven invalidation: hio discovered the parked range points
 *	past the relation's current EOF (VACUUM truncated the trailing zero
 *	pages while the lease was live).  The truncated blocks are already
 *	reclaimed storage, but they still count into d_orphan_zero so the
 *	leased_total >= consumed + orphan bookkeeping invariant holds.
 */
void
cluster_hw_lease_discard(RelFileLocator rloc, ForkNumber fork)
{
	ClusterHwLeaseSlot *slot;

	if (ClusterHwLeaseCtl == NULL)
		return;

	LWLockAcquire(&ClusterHwLeaseCtl->lock.lock, LW_EXCLUSIVE);
	slot = lease_slot_find(rloc, fork);
	if (slot != NULL) {
		if (slot->next_block < slot->end_block)
			pg_atomic_fetch_add_u64(&ClusterHwLeaseCtl->d_orphan_zero,
									slot->end_block - slot->next_block);
		slot->fork = -1;
	}
	LWLockRelease(&ClusterHwLeaseCtl->lock.lock);
}

/*
 * cluster_hw_lease_slot_snapshot -- copy slot idx for observability;
 * false when the slot is free / idx out of range.
 */
bool
cluster_hw_lease_slot_snapshot(int idx, ClusterHwLeaseSlot *out)
{
	bool live = false;

	if (idx < 0 || idx >= CLUSTER_HW_LEASE_SLOTS || ClusterHwLeaseCtl == NULL)
		return false;

	LWLockAcquire(&ClusterHwLeaseCtl->lock.lock, LW_SHARED);
	if (ClusterHwLeaseCtl->slots[idx].fork != -1) {
		*out = ClusterHwLeaseCtl->slots[idx];
		live = true;
	}
	LWLockRelease(&ClusterHwLeaseCtl->lock.lock);

	return live;
}
