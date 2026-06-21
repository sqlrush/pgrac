/*-------------------------------------------------------------------------
 *
 * cluster_sequence.h
 *	  SQ sequence lock: pure allocation math + resource-id encoding for
 *	  cross-node nextval uniqueness.
 *
 *	  PostgreSQL sequences are 100% node-local (a single on-disk page per
 *	  sequence, a per-backend SeqTableData cache).  In a shared-storage
 *	  cluster that means two nodes can hand out the same nextval value.
 *	  spec-5.4 v2.0 (Q2-B, option B): the single SHARED sequence page is the
 *	  cross-node allocation boundary (kept coherent by the page X / PCM+CF and
 *	  per-node WAL, spec-5.2 / 5.2a).  Each node adds a per-node instance cache
 *	  refilled from that page under a node-local refill lock, so a backend only
 *	  ever emits values from a segment durably advanced on the shared page.  The
 *	  cross-node GES SQ enqueue (Oracle SQ) is a forward perf optimisation; this
 *	  release serialises the node + shared page, NOT a cross-node enqueue.
 *
 *	  Two API layers:
 *	    - Pure layer (backend-pure: no elog/shmem/lock; standalone-linkable
 *	      so the cluster_unit test links it without the full backend):
 *	      cluster_sq_resid_encode, cluster_sq_alloc_segment,
 *	      cluster_sq_cache_has_value.  Out-of-range input asserts (debug)
 *	      and fails closed to the conservative answer; never ereports.
 *	    - Backend layer (#ifndef FRONTEND, in cluster_sequence_shmem.c): the
 *	      shmem instance cache + node-local refill lock (begin / publish_and_take
 *	      / abort + refill CV) and the six observability counters.  The refill
 *	      orchestration + shared-page advance live in commands/sequence.c.
 *	      These may ereport.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_sequence.h
 *
 * NOTES
 *	  Spec: spec-5.4-sq-sequence-lock.md
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SEQUENCE_H
#define CLUSTER_SEQUENCE_H

#include "cluster/cluster_grd.h" /* ClusterResId + LOCKTAG_* namespace */

/*
 * CLUSTER_SQ_RESID_TYPE -- SQ resource-id namespace marker.
 *
 *	The SQ resid maps (database_oid, sequence_oid, generation) into a
 *	ClusterResId.  It uses a dedicated type byte so the SQ(X) enqueue lives
 *	in its own namespace, distinct from the PG relation lock the sequence
 *	relation itself takes (LOCKTAG_RELATION via lock_and_open_sequence).
 *	The GES request/grant path only hashes the resid bytes for shard/master
 *	routing; it never decodes an SQ resid back to a real LOCKTAG, so a
 *	synthetic high type value is safe.
 */
#define CLUSTER_SQ_RESID_TYPE 0xF0

StaticAssertDecl(CLUSTER_SQ_RESID_TYPE > LOCKTAG_LAST_TYPE,
				 "SQ resid namespace must not collide with any PG LockTagType");

/*
 * ClusterSqAllocStatus -- outcome of one authority batch allocation.
 *
 *	OK         the full requested count was granted.
 *	TRUNCATED  fewer than requested were granted because the segment hit
 *	           seqmax (ascending) / seqmin (descending); the caller still
 *	           gets a valid (shorter) segment.
 *	EXHAUSTED  the boundary is already at the limit; zero granted.  The
 *	           caller raises the PG "sequence exceeded" error (2200H).
 */
typedef enum ClusterSqAllocStatus {
	CLUSTER_SQ_ALLOC_OK = 0,
	CLUSTER_SQ_ALLOC_TRUNCATED = 1,
	CLUSTER_SQ_ALLOC_EXHAUSTED = 2,
} ClusterSqAllocStatus;

/* ---- pure layer (standalone-linkable; never ereports) --------------- */

/*
 * cluster_sq_resid_encode -- build the SQ resource id for a sequence.
 *
 *	dst is filled deterministically from (dboid, seqoid, generation).
 *	generation is the sequence's pg_class.relfilenode so a DROP+CREATE of
 *	the same oid yields a distinct resource (ABA defence).
 */
extern void cluster_sq_resid_encode(Oid dboid, Oid seqoid, uint32 generation, ClusterResId *dst);

/*
 * cluster_sq_alloc_segment -- direction-aware batch allocator (authority).
 *
 *	Given the current allocation boundary (the last value allocated along
 *	the seqincrement direction, mirroring PG sequence last_value), whether
 *	it has been called (is_called), the increment, and the [minv,maxv]
 *	range, grant up to want_count consecutive legal values.
 *
 *	  *granted_start  first value of the segment
 *	  *granted_end    last value of the segment (== *new_boundary)
 *	  *granted_count  number of values actually granted (<= want_count)
 *	  *new_boundary   the new durable allocation boundary
 *
 *	The boundary is direction-aware: for an ascending sequence it is the
 *	largest allocated value, for a descending sequence the smallest.  This
 *	module does NOT implement CYCLE: cluster CYCLE is rejected upstream, so
 *	the allocator never wraps and the boundary stays monotone along the
 *	increment direction.
 */
extern ClusterSqAllocStatus cluster_sq_alloc_segment(int64 boundary, bool is_called,
													 int64 increment, int64 minv, int64 maxv,
													 int64 want_count, int64 *granted_start,
													 int64 *granted_end, int64 *granted_count,
													 int64 *new_boundary);

/*
 * cluster_sq_cache_has_value -- can the instance cache still serve a value?
 *
 *	[local_next .. local_end] is a half-consumed segment interpreted along
 *	the increment sign: ascending has a value while local_next <= local_end,
 *	descending while local_next >= local_end.
 */
extern bool cluster_sq_cache_has_value(int64 local_next, int64 local_end, int64 increment);

#ifndef FRONTEND

#include "port/atomics.h"

/*
 * ClusterSeqInstanceCache -- per-sequence node-level (instance) cache entry.
 *
 *	Sits between the PG per-backend SeqTableData fast-path (unchanged) and
 *	the shared sequence page (the cross-node durable allocation boundary).
 *	When a backend's own cache is exhausted it slices the next value from
 *	here under the SQ shmem lock; when this entry is empty exactly one
 *	backend on the node refills it from the shared page (refill_in_progress
 *	serialises the node; the shared page X / PCM+CF serialises cross-node).
 *	spec-5.4 v2.0 (Q2-B, option B): node-local refill lock + shared page X,
 *	no cross-node GES SQ(X) enqueue (forward; see spec §v2.0.1).
 *
 *	The HTAB is keyed by the full ClusterResId, which already embeds the
 *	generation (relfilenode) in field3 -- a DROP+CREATE therefore lands a
 *	new key and never reuses a stale segment.  The generation field below
 *	is kept for observability and defence-in-depth assertions.
 */
typedef struct ClusterSeqInstanceCache {
	ClusterResId resid;		 /* HTAB key (must be first; HASH_BLOBS) */
	uint32 generation;		 /* drop/recreate identity (== resid.field3) */
	int64 local_next;		 /* next value this node may emit */
	int64 local_end;		 /* segment end (inclusive; sign per increment) */
	int64 increment;		 /* copy of seqincrement (sign = direction) */
	bool refill_in_progress; /* a backend on this node is refilling */
	bool has_segment;		 /* false = bare refill claim / no live segment */
} ClusterSeqInstanceCache;

/*
 * ClusterSqRefillClaim -- outcome of cluster_sq_instance_cache_begin_refill.
 *
 *	SERVED   a value was sliced from a live segment (*out_value set); done.
 *	CLAIMED  this backend won the refill race (refill_in_progress now set);
 *	         it MUST run the page advance then publish_and_take / abort_refill.
 *	WAIT     another backend on this node is refilling the same sequence;
 *	         the caller waits on the refill CV and retries.
 */
typedef enum ClusterSqRefillClaim {
	CLUSTER_SQ_REFILL_SERVED = 0,
	CLUSTER_SQ_REFILL_CLAIMED = 1,
	CLUSTER_SQ_REFILL_WAIT = 2,
} ClusterSqRefillClaim;

/* D1 shmem region lifecycle (mirror cluster_ges_reply_wait_shmem_*). */
extern Size cluster_sequence_shmem_size(void);
extern void cluster_sequence_shmem_init(void);
extern void cluster_sequence_shmem_register(void);

/*
 * D1 instance-cache primitive: drop the entry (setval / ALTER invalidation).
 * Takes the SQ shmem lock internally.
 */
extern void cluster_sq_instance_cache_invalidate(const ClusterResId *resid);

/*
 * D3 (v2.0 option B) node-local refill lock.
 *
 *	begin_refill      atomically (under the SQ lock) serves a value, claims the
 *	                  refill, or reports another backend is refilling.
 *	publish_and_take  the claimant atomically takes the first granted value and
 *	                  publishes the remainder (one lock acquisition -> no peer
 *	                  can serve the first value first; Rule 8.A), bumps
 *	                  refill_count, and wakes any waiter.
 *	abort_refill      clears the in-progress flag + wakes waiters on an error
 *	                  path (page advance raised) so the next backend can retry.
 *	The refill-wait CV helpers wrap the region-wide refill_cv with the
 *	ClusterSqRefillWait wait event (caller owns the prepare / sleep / cancel
 *	dance + its own deadline; see cluster_sq_refill_timeout for the bound).
 */
extern ClusterSqRefillClaim cluster_sq_instance_cache_begin_refill(const ClusterResId *resid,
																   uint32 generation,
																   int64 increment,
																   int64 *out_value);
extern void cluster_sq_instance_cache_publish_and_take(const ClusterResId *resid, uint32 generation,
													   int64 increment, int64 seg_start,
													   int64 seg_end, int64 *out_value);
extern void cluster_sq_instance_cache_abort_refill(const ClusterResId *resid);

extern void cluster_sq_refill_prepare_wait(void);
extern void cluster_sq_refill_sleep(long timeout_ms);
extern void cluster_sq_refill_cancel_wait(void);

/* D9 counter accessors (observability) + bumps (SQ-internal call sites). */
extern uint64 cluster_sq_refill_count(void);
extern uint64 cluster_sq_refill_wait_count(void);
extern uint64 cluster_sq_dup_guard_fail_count(void);
extern uint64 cluster_sq_failover_fail_closed_count(void);
extern uint64 cluster_sq_page_writeback_count(void);
extern uint64 cluster_sq_cycle_rejected_count(void);

extern void cluster_sq_bump_refill(void);
extern void cluster_sq_bump_refill_wait(void);
extern void cluster_sq_bump_dup_guard_fail(void);
extern void cluster_sq_bump_failover_fail_closed(void);
extern void cluster_sq_bump_page_writeback(void);
extern void cluster_sq_bump_cycle_rejected(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_SEQUENCE_H */
