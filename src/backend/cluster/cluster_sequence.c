/*-------------------------------------------------------------------------
 *
 * cluster_sequence.c
 *	  SQ sequence lock: pure allocation math + resource-id encoding.
 *
 *	  See cluster_sequence.h for the layering rationale.  This file holds
 *	  ONLY the pure layer (no elog/shmem/lock; standalone-linkable); the
 *	  shmem instance cache, the GES(X) refill protocol, and the authority
 *	  with WAL-before-grant + boundary writeback live in cluster_sequence_*
 *	  backend files.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_sequence.c
 *
 * NOTES
 *	  Spec: spec-5.4-sq-sequence-lock.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_sequence.h"

/*
 * cluster_sq_resid_encode -- build the SQ resource id for a sequence.
 *
 *	(database_oid, sequence_oid, generation) map to field1/2/3; the type
 *	byte carries the SQ namespace marker, the lockmethod is the default
 *	method (the substrate hashes all of these for shard/master routing).
 */
void
cluster_sq_resid_encode(Oid dboid, Oid seqoid, uint32 generation, ClusterResId *dst)
{
	Assert(dst != NULL);
	if (dst == NULL)
		return;

	dst->field1 = (uint32)dboid;
	dst->field2 = (uint32)seqoid;
	dst->field3 = generation;
	dst->field4 = 0;
	dst->type = CLUSTER_SQ_RESID_TYPE;
	dst->lockmethodid = DEFAULT_LOCKMETHOD;
}

/*
 * cluster_sq_alloc_segment -- direction-aware batch allocator (authority).
 *
 *	The allocation boundary is the last value granted along the increment
 *	direction (mirror of PG sequence last_value).  This module never wraps
 *	(cluster CYCLE is rejected upstream), so the boundary stays monotone:
 *	ascending grants increase it, descending grants decrease it.  The gap
 *	to the seqmax/seqmin limit is computed in uint64 so a full int64 span
 *	(e.g. minv = INT64_MIN) does not overflow; -fwrapv additionally makes
 *	the boundary+increment probes well-defined at the extremes.
 */
ClusterSqAllocStatus
cluster_sq_alloc_segment(int64 boundary, bool is_called, int64 increment, int64 minv, int64 maxv,
						 int64 want_count, int64 *granted_start, int64 *granted_end,
						 int64 *granted_count, int64 *new_boundary)
{
	int64 first;
	int64 limit;
	uint64 gap;
	uint64 abs_inc;
	uint64 max_steps;
	int64 count;

	Assert(increment != 0);
	Assert(want_count >= 1);
	Assert(minv <= maxv);

	/* Fail-closed defaults: zero granted, boundary unchanged. */
	*granted_start = 0;
	*granted_end = 0;
	*granted_count = 0;
	*new_boundary = boundary;

	if (increment == 0 || want_count < 1 || minv > maxv)
		return CLUSTER_SQ_ALLOC_EXHAUSTED;

	/* First value of the new segment. */
	if (!is_called) {
		/* START not yet consumed: the first nextval returns the boundary. */
		first = boundary;
	} else if (increment > 0) {
		/* boundary + increment, but reject overflow past seqmax. */
		if (boundary > maxv - increment)
			return CLUSTER_SQ_ALLOC_EXHAUSTED;
		first = boundary + increment;
	} else {
		/* descending: reject underflow past seqmin (minv - increment = minv + |inc|). */
		if (boundary < minv - increment)
			return CLUSTER_SQ_ALLOC_EXHAUSTED;
		first = boundary + increment;
	}

	/* `first` must be a legal value of the sequence. */
	if (first < minv || first > maxv)
		return CLUSTER_SQ_ALLOC_EXHAUSTED;

	abs_inc = (increment > 0) ? (uint64)increment : (uint64)(-increment);
	if (increment > 0) {
		limit = maxv;
		gap = (uint64)limit - (uint64)first; /* limit >= first */
	} else {
		limit = minv;
		gap = (uint64)first - (uint64)limit; /* first >= limit */
	}

	/* abs_inc is |increment| with increment != 0 above, so it is always
	 * positive here; make the non-zero divisor explicit (defence in depth
	 * + silences a static-analysis zero-division false positive). */
	if (abs_inc == 0)
		return CLUSTER_SQ_ALLOC_EXHAUSTED;

	/* Additional values that fit after `first`, then clamp to want_count.
	 * Compare against want_count-1 to avoid a (max_steps + 1) overflow. */
	max_steps = gap / abs_inc;
	if (max_steps >= (uint64)(want_count - 1))
		count = want_count;
	else
		count = (int64)(max_steps + 1);

	*granted_start = first;
	*granted_end = first + (count - 1) * increment;
	*granted_count = count;
	*new_boundary = *granted_end;

	return (count == want_count) ? CLUSTER_SQ_ALLOC_OK : CLUSTER_SQ_ALLOC_TRUNCATED;
}

/*
 * cluster_sq_cache_has_value -- can the instance cache still serve a value?
 *
 *	Interpreted along the increment sign; increment must be non-zero (the
 *	pure layer asserts and fails closed otherwise).
 */
bool
cluster_sq_cache_has_value(int64 local_next, int64 local_end, int64 increment)
{
	Assert(increment != 0);
	if (increment > 0)
		return local_next <= local_end;
	if (increment < 0)
		return local_next >= local_end;
	return false; /* increment == 0: conservative, never serve */
}
