/*-------------------------------------------------------------------------
 *
 * cluster_sequence_shmem.c
 *	  SQ sequence lock: per-node instance cache shmem region + counters.
 *
 *	  Holds the node-level instance cache (an HTAB keyed by ClusterResId,
 *	  guarded by a single LWLock) and the six SQ observability counters.
 *	  The pure allocation math lives in cluster_sequence.c; the GES(X)
 *	  refill protocol and the authority live in cluster_sequence_refill.c /
 *	  cluster_sequence_authority.c.
 *
 *	  Lock granularity: a single region LWLock guards the whole instance
 *	  cache.  The cache is only touched on a per-backend-cache miss (once
 *	  per seqcache values), not on every nextval, so contention is low; a
 *	  partitioned lock array is a forward optimisation (R13 / spec §2.1).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_sequence_shmem.c
 *
 * NOTES
 *	  Spec: spec-5.4-sq-sequence-lock.md (D1 + D9)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_sequence.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

/*
 * Maximum number of distinct sequences cached on one node.  Fixed for now
 * (a GUC is a forward refinement); overflow degrades to "always refill"
 * for the surplus sequences, never to incorrect values.
 */
#define CLUSTER_SQ_INSTANCE_CACHE_MAX 1024

/*
 * ClusterSeqShared -- the SQ region header: one LWLock + six counters.
 */
typedef struct ClusterSeqShared {
	LWLock lwlock; /* guards the instance cache HTAB */
	pg_atomic_uint64 refill_count;
	pg_atomic_uint64 refill_wait_count;
	pg_atomic_uint64 dup_guard_fail_count;
	pg_atomic_uint64 failover_fail_closed_count;
	pg_atomic_uint64 page_writeback_count;
	pg_atomic_uint64 cycle_rejected_count;
} ClusterSeqShared;

static ClusterSeqShared *sq_state = NULL;
static HTAB *sq_cache_htab = NULL;

static const ClusterShmemRegion cluster_sequence_region = {
	.name = "pgrac cluster sequence",
	.size_fn = cluster_sequence_shmem_size,
	.init_fn = cluster_sequence_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "spec-5.4 SQ sequence",
	.reserved_flags = 0,
};


/* ============================================================
 * Shmem region size / init / register.
 * ============================================================ */

Size
cluster_sequence_shmem_size(void)
{
	Size sz = MAXALIGN(sizeof(ClusterSeqShared));

	sz = add_size(sz, hash_estimate_size((Size)CLUSTER_SQ_INSTANCE_CACHE_MAX,
										 sizeof(ClusterSeqInstanceCache)));
	return sz;
}

void
cluster_sequence_shmem_init(void)
{
	bool found;
	HASHCTL hctl;

	sq_state = (ClusterSeqShared *)ShmemInitStruct("pgrac cluster sequence",
												   MAXALIGN(sizeof(ClusterSeqShared)), &found);

	if (!IsUnderPostmaster) {
		LWLockInitialize(&sq_state->lwlock, LWTRANCHE_CLUSTER_SQ);
		pg_atomic_init_u64(&sq_state->refill_count, 0);
		pg_atomic_init_u64(&sq_state->refill_wait_count, 0);
		pg_atomic_init_u64(&sq_state->dup_guard_fail_count, 0);
		pg_atomic_init_u64(&sq_state->failover_fail_closed_count, 0);
		pg_atomic_init_u64(&sq_state->page_writeback_count, 0);
		pg_atomic_init_u64(&sq_state->cycle_rejected_count, 0);
	}

	MemSet(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(ClusterResId);
	hctl.entrysize = sizeof(ClusterSeqInstanceCache);
	sq_cache_htab
		= ShmemInitHash("pgrac cluster sequence instance cache", CLUSTER_SQ_INSTANCE_CACHE_MAX,
						CLUSTER_SQ_INSTANCE_CACHE_MAX, &hctl, HASH_ELEM | HASH_BLOBS);
}

void
cluster_sequence_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_sequence_region);
}


/* ============================================================
 * Instance cache primitives.
 * ============================================================ */

/*
 * cluster_sq_instance_cache_serve -- slice the next value from the cache.
 *
 *	Returns true and stores the served value in *out_value when this node's
 *	instance cache for `resid` still holds an unconsumed value.  Returns
 *	false when the entry is absent or exhausted, in which case the caller
 *	must run the SQ(X) refill protocol.
 */
bool
cluster_sq_instance_cache_serve(const ClusterResId *resid, int64 *out_value)
{
	ClusterSeqInstanceCache *entry;
	bool served = false;

	Assert(resid != NULL);
	Assert(out_value != NULL);

	if (sq_cache_htab == NULL)
		return false;

	LWLockAcquire(&sq_state->lwlock, LW_EXCLUSIVE);

	entry = (ClusterSeqInstanceCache *)hash_search(sq_cache_htab, resid, HASH_FIND, NULL);
	if (entry != NULL
		&& cluster_sq_cache_has_value(entry->local_next, entry->local_end, entry->increment)) {
		*out_value = entry->local_next;
		entry->local_next += entry->increment;
		served = true;
	}

	LWLockRelease(&sq_state->lwlock);
	return served;
}

/*
 * cluster_sq_instance_cache_fill -- install a granted segment.
 *
 *	Called after the refill protocol obtains [seg_start, seg_end] from the
 *	authority.  Replaces any existing segment for `resid` (the prior one is
 *	by contract fully consumed before a refill is triggered).
 */
void
cluster_sq_instance_cache_fill(const ClusterResId *resid, uint32 generation, int64 increment,
							   int64 seg_start, int64 seg_end)
{
	ClusterSeqInstanceCache *entry;
	bool found;

	Assert(resid != NULL);

	if (sq_cache_htab == NULL)
		return;

	LWLockAcquire(&sq_state->lwlock, LW_EXCLUSIVE);

	entry = (ClusterSeqInstanceCache *)hash_search(sq_cache_htab, resid, HASH_ENTER_NULL, &found);
	if (entry == NULL) {
		/* Cache full: surplus sequences degrade to always-refill, never to
		 * an incorrect value.  Drop on the floor and let the caller serve
		 * directly from the freshly granted segment. */
		LWLockRelease(&sq_state->lwlock);
		return;
	}

	entry->generation = generation;
	entry->increment = increment;
	entry->local_next = seg_start;
	entry->local_end = seg_end;
	entry->refill_in_progress = false;

	LWLockRelease(&sq_state->lwlock);
}

/*
 * cluster_sq_instance_cache_invalidate -- drop the cached segment.
 *
 *	Used by setval / ALTER SEQUENCE strong invalidation so a subsequent
 *	nextval refills from the authority rather than serving a stale value.
 */
void
cluster_sq_instance_cache_invalidate(const ClusterResId *resid)
{
	Assert(resid != NULL);

	if (sq_cache_htab == NULL)
		return;

	LWLockAcquire(&sq_state->lwlock, LW_EXCLUSIVE);
	hash_search(sq_cache_htab, resid, HASH_REMOVE, NULL);
	LWLockRelease(&sq_state->lwlock);
}


/* ============================================================
 * Counters: bumps + accessors.
 * ============================================================ */

#define SQ_BUMP(field)                                                                             \
	do {                                                                                           \
		if (sq_state != NULL)                                                                      \
			pg_atomic_fetch_add_u64(&sq_state->field, 1);                                          \
	} while (0)

#define SQ_READ(field) (sq_state != NULL ? pg_atomic_read_u64(&sq_state->field) : 0)

void
cluster_sq_bump_refill(void)
{
	SQ_BUMP(refill_count);
}
void
cluster_sq_bump_refill_wait(void)
{
	SQ_BUMP(refill_wait_count);
}
void
cluster_sq_bump_dup_guard_fail(void)
{
	SQ_BUMP(dup_guard_fail_count);
}
void
cluster_sq_bump_failover_fail_closed(void)
{
	SQ_BUMP(failover_fail_closed_count);
}
void
cluster_sq_bump_page_writeback(void)
{
	SQ_BUMP(page_writeback_count);
}
void
cluster_sq_bump_cycle_rejected(void)
{
	SQ_BUMP(cycle_rejected_count);
}

uint64
cluster_sq_refill_count(void)
{
	return SQ_READ(refill_count);
}
uint64
cluster_sq_refill_wait_count(void)
{
	return SQ_READ(refill_wait_count);
}
uint64
cluster_sq_dup_guard_fail_count(void)
{
	return SQ_READ(dup_guard_fail_count);
}
uint64
cluster_sq_failover_fail_closed_count(void)
{
	return SQ_READ(failover_fail_closed_count);
}
uint64
cluster_sq_page_writeback_count(void)
{
	return SQ_READ(page_writeback_count);
}
uint64
cluster_sq_cycle_rejected_count(void)
{
	return SQ_READ(cycle_rejected_count);
}
