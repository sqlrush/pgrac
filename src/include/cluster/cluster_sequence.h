/*-------------------------------------------------------------------------
 *
 * cluster_sequence.h
 *	  SQ sequence lock: pure allocation math + resource-id encoding for
 *	  cross-node nextval uniqueness.
 *
 *	  PostgreSQL sequences are 100% node-local (a single on-disk page per
 *	  sequence, a per-backend SeqTableData cache).  In a shared-storage
 *	  cluster that means two nodes can hand out the same nextval value.
 *	  The SQ ("sequence") enqueue serialises batch allocation against a
 *	  per-sequence authority so that each node only ever emits values from
 *	  a segment it has durably been granted.  SQ is a short-held X-only GES
 *	  enqueue (mutual exclusion), NOT a convert consumer.
 *
 *	  Two API layers:
 *	    - Pure layer (backend-pure: no elog/shmem/lock; standalone-linkable
 *	      so the cluster_unit test links it without the full backend):
 *	      cluster_sq_resid_encode, cluster_sq_alloc_segment,
 *	      cluster_sq_cache_has_value.  Out-of-range input asserts (debug)
 *	      and fails closed to the conservative answer; never ereports.
 *	    - Backend layer (#ifndef FRONTEND, in cluster_sequence_*.c): the
 *	      shmem instance cache, the GES(X) refill protocol, the authority
 *	      batch allocator with WAL-before-grant + boundary writeback, and
 *	      the fail-closed paths.  These may ereport.
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

#endif /* CLUSTER_SEQUENCE_H */
