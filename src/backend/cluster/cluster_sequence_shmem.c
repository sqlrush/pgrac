/*-------------------------------------------------------------------------
 *
 * cluster_sequence_shmem.c
 *	  SQ sequence lock: per-node instance cache shmem region + counters.
 *
 *	  Holds the node-level instance cache (an HTAB keyed by ClusterResId,
 *	  guarded by a single LWLock + a refill CV) and the six SQ observability
 *	  counters.  The pure allocation math lives in cluster_sequence.c; the
 *	  node-local refill orchestration + the shared-page advance live in
 *	  commands/sequence.c (spec-5.4 v2.0 Q2-B, option B: the shared sequence
 *	  page is the cross-node boundary; no cross-node GES SQ enqueue).
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
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/wait_event.h"

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
	LWLock lwlock;				 /* guards the instance cache HTAB */
	ConditionVariable refill_cv; /* node-local refill serialisation (D3) */
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
		ConditionVariableInit(&sq_state->refill_cv);
		pg_atomic_init_u64(&sq_state->refill_count, 0);
		pg_atomic_init_u64(&sq_state->refill_wait_count, 0);
		pg_atomic_init_u64(&sq_state->dup_guard_fail_count, 0);
		pg_atomic_init_u64(&sq_state->failover_fail_closed_count, 0);
		pg_atomic_init_u64(&sq_state->page_writeback_count, 0);
		pg_atomic_init_u64(&sq_state->cycle_rejected_count, 0);
	}

	memset(&hctl, 0, sizeof(hctl));
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
 * cluster_sq_instance_cache_publish_and_take -- the refill claimant atomically
 *	takes the first granted value AND publishes the remainder, under a single
 *	lock acquisition.
 *
 *	The page advance grants [seg_start, seg_end] (seg_end == seg_start for a
 *	single-value / CACHE 1 grant).  The claimant keeps seg_start for ITSELF
 *	(*out_value, always set) and installs only the remainder
 *	[seg_start + increment, seg_end] into the cache with refill_in_progress
 *	cleared.  Doing the take + publish under one lock closes the window where a
 *	peer backend, woken by the broadcast, serves seg_start before the claimant
 *	does -- which at CACHE 1 (single-value segment) would leave the claimant
 *	with an exhausted cache and a fall-back to seg_start too: the same value
 *	from two same-node backends (Rule 8.A same-node duplicate).
 *
 *	A single-value grant publishes an empty (has_segment = false) entry so the
 *	next begin_refill re-claims cleanly (and never reads seg_start + increment,
 *	which could overflow at seqmax); a full HTAB drops the remainder (the
 *	claimant still keeps seg_start).  Wakes any waiter and bumps refill_count.
 */
void
cluster_sq_instance_cache_publish_and_take(const ClusterResId *resid, uint32 generation,
										   int64 increment, int64 seg_start, int64 seg_end,
										   int64 *out_value)
{
	Assert(resid != NULL);
	Assert(out_value != NULL);

	*out_value = seg_start; /* the claimant's value; never published */

	if (sq_cache_htab != NULL) {
		ClusterSeqInstanceCache *entry;
		bool found;

		LWLockAcquire(&sq_state->lwlock, LW_EXCLUSIVE);
		entry
			= (ClusterSeqInstanceCache *)hash_search(sq_cache_htab, resid, HASH_ENTER_NULL, &found);
		if (entry != NULL) {
			entry->generation = generation;
			entry->increment = increment;
			entry->refill_in_progress = false;
			if (seg_end != seg_start) {
				/* Multi-value grant: publish the remainder.  seg_start +
				 * increment is the SECOND granted value, which is at or before
				 * seg_end <= seqmax, so it never overflows. */
				entry->local_next = seg_start + increment;
				entry->local_end = seg_end;
				entry->has_segment = true;
			} else {
				/* Single value: the claimant consumed it -> empty entry. */
				entry->local_next = 0;
				entry->local_end = 0;
				entry->has_segment = false;
			}
		}
		/* entry == NULL (HTAB full): remainder dropped; claimant keeps seg_start. */
		LWLockRelease(&sq_state->lwlock);
	}

	cluster_sq_bump_refill();
	if (sq_state != NULL)
		ConditionVariableBroadcast(&sq_state->refill_cv);
}

/*
 * cluster_sq_instance_cache_invalidate -- drop the cached segment.
 *
 *	Used by setval / ALTER SEQUENCE invalidation so a subsequent nextval
 *	refills from the (now updated) shared page rather than serving a stale
 *	value.  This node's cache only; other nodes drain on their next refill
 *	(spec §v2.0.5; full cross-node strong invalidation is forward).
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
 * D3 (v2.0 option B) node-local refill lock.
 *
 *	The instance-cache LWLock is a short-held spin guard: it must NOT be held
 *	across the page advance (read_seq_tuple does buffer I/O, page X / CF round
 *	trips, CHECK_FOR_INTERRUPTS).  So begin_refill claims under the lock by
 *	flagging refill_in_progress, the caller runs the page advance lock-free,
 *	and finish/abort re-take the lock to publish/clear.  refill_in_progress
 *	serialises the node (one page advance per sequence at a time -> no
 *	thundering-herd segment waste); the shared page X serialises cross-node.
 * ============================================================ */

ClusterSqRefillClaim
cluster_sq_instance_cache_begin_refill(const ClusterResId *resid, uint32 generation,
									   int64 increment, int64 *out_value)
{
	ClusterSeqInstanceCache *entry;
	ClusterSqRefillClaim claim;
	bool found;

	Assert(resid != NULL);
	Assert(out_value != NULL);
	Assert(increment != 0);

	if (sq_cache_htab == NULL)
		return CLUSTER_SQ_REFILL_CLAIMED; /* shmem absent: caller serves directly */

	LWLockAcquire(&sq_state->lwlock, LW_EXCLUSIVE);

	entry = (ClusterSeqInstanceCache *)hash_search(sq_cache_htab, resid, HASH_FIND, NULL);
	if (entry != NULL && entry->has_segment
		&& cluster_sq_cache_has_value(entry->local_next, entry->local_end, entry->increment)) {
		/* Live value available -> slice and return (no page touch). */
		*out_value = entry->local_next;
		entry->local_next += entry->increment;
		claim = CLUSTER_SQ_REFILL_SERVED;
	} else if (entry != NULL && entry->refill_in_progress) {
		/* Another backend on this node is already advancing the page. */
		claim = CLUSTER_SQ_REFILL_WAIT;
	} else {
		/*
		 * Win the refill race: create (or reuse) the entry as a bare claim
		 * (has_segment = false so no stale value is ever served while the
		 * page advance is in flight) and flag refill_in_progress.  A cache
		 * full HTAB degrades to "serve directly from the granted segment"
		 * (entry == NULL) -- never to an incorrect value.
		 */
		entry
			= (ClusterSeqInstanceCache *)hash_search(sq_cache_htab, resid, HASH_ENTER_NULL, &found);
		if (entry == NULL) {
			claim = CLUSTER_SQ_REFILL_CLAIMED; /* degraded: no node-level cache */
		} else {
			entry->generation = generation;
			entry->increment = increment;
			entry->local_next = 0;
			entry->local_end = 0;
			entry->has_segment = false;
			entry->refill_in_progress = true;
			claim = CLUSTER_SQ_REFILL_CLAIMED;
		}
	}

	LWLockRelease(&sq_state->lwlock);
	return claim;
}

void
cluster_sq_instance_cache_abort_refill(const ClusterResId *resid)
{
	Assert(resid != NULL);

	if (sq_cache_htab != NULL) {
		ClusterSeqInstanceCache *entry;

		LWLockAcquire(&sq_state->lwlock, LW_EXCLUSIVE);
		entry = (ClusterSeqInstanceCache *)hash_search(sq_cache_htab, resid, HASH_FIND, NULL);
		if (entry != NULL)
			entry->refill_in_progress = false;
		LWLockRelease(&sq_state->lwlock);
	}

	/* Let a waiter retry the refill (this backend's page advance failed). */
	if (sq_state != NULL)
		ConditionVariableBroadcast(&sq_state->refill_cv);
}

/* ---- refill-wait CV (region-wide; ClusterSqRefillWait) --------------- */

void
cluster_sq_refill_prepare_wait(void)
{
	if (sq_state != NULL)
		ConditionVariablePrepareToSleep(&sq_state->refill_cv);
}

void
cluster_sq_refill_sleep(long timeout_ms)
{
	if (sq_state == NULL)
		return;
	cluster_sq_bump_refill_wait();
	(void)ConditionVariableTimedSleep(&sq_state->refill_cv, timeout_ms,
									  WAIT_EVENT_CLUSTER_SQ_REFILL_WAIT);
}

void
cluster_sq_refill_cancel_wait(void)
{
	ConditionVariableCancelSleep();
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
