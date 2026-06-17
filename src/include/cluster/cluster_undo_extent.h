/*-------------------------------------------------------------------------
 *
 * cluster_undo_extent.h
 *	  pgrac spec-3.18 D3 — per-transaction undo extent partitioning (interface).
 *
 *	  D3 kills the single per-instance cursor_lock serial hot-spot: instead of
 *	  every backend advancing one shared cursor (active_segment_id /
 *	  current_block / free_offset / slot_count) under cursor_lock for EVERY
 *	  undo record, a backend CLAIMS an extent -- a run of N consecutive data
 *	  blocks (GUC cluster.undo_extent_blocks, default 4) -- and then advances a
 *	  BACKEND-LOCAL cursor within it.  Only an extent claim touches the shared
 *	  lifecycle lock + the shared high-water mark;  per-record writes are
 *	  lock-free w.r.t. other backends (the blocks of an extent are private to
 *	  the claiming backend, and per-block content is still protected by the D1
 *	  undo buffer pool's content_lock).
 *
 *	  Recycling granularity is UNCHANGED (whole-segment, spec-3.12/3.13):
 *	  extents only partition WRITE-TIME block allocation within the active
 *	  segment.  When a segment is recycled/reborn its blocks (former extents)
 *	  are reused wholesale, gated on the segment's TT slots being recyclable
 *	  (existing gate) -- so retention contracts C1/C5 hold (spec §3.5).  A
 *	  backend that claims an extent then crashes leaves those blocks unused
 *	  until the whole segment recycles;  no permanent leak (the high-water
 *	  advance is atomic under lifecycle_lock).
 *
 *	  UBA / undo chain / 64B UndoRecordHeader are unchanged: block_no in the
 *	  UBA now comes from the backend-local extent cursor instead of the shared
 *	  cursor.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.18-write-path-performance-overhaul.md (FROZEN, §2.3 + §3.5 + Q2)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_undo_extent.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_EXTENT_H
#define CLUSTER_UNDO_EXTENT_H

#include "c.h"

#include "cluster/cluster_undo_format.h" /* sizeof(UndoBlockHeader) */

/*
 * A backend-local extent: a run of consecutive data blocks claimed from the
 * active undo segment, advanced lock-free within the owning backend.
 *
 *	The spec §2.3 sketch carried {segment_id, first_block, nblocks,
 *	next_free_block, next_free_offset};  cur_slot_count is added so the
 *	backend can test block fullness without re-reading the block header on
 *	every record (the block content is still authoritative via the pool).
 */
typedef struct ClusterUndoExtent {
	uint32 segment_id;		/* segment this extent lives in (0 => no extent held) */
	uint32 first_block;		/* first data block of the extent (>= 1) */
	uint32 nblocks;			/* number of blocks claimed (1..undo_extent_blocks) */
	uint32 cur_block;		/* backend-local cursor: block currently being written */
	uint32 cur_free_offset; /* free byte offset within cur_block */
	uint16 cur_slot_count;	/* records already in cur_block */
} ClusterUndoExtent;

/* "no extent held" sentinel. */
#define CLUSTER_UNDO_EXTENT_NONE 0

/*
 * cluster_undo_extent_compute -- pure extent-bounds arithmetic (cluster_unit-
 *	tested).  Given the segment's high-water (first free data block) and the
 *	desired block count, compute the [first_block, first_block+nblocks) range,
 *	clamped to the segment's data-block range [1, blocks_per_segment).  Returns
 *	the number of blocks claimable (0 when the segment is full -> caller
 *	autoextends).  On success *out is initialized with the cursor parked at the
 *	first block (fresh: free_offset = sizeof(UndoBlockHeader), slot_count = 0);
 *	the caller fills in out->segment_id.
 */
static inline uint32
cluster_undo_extent_compute(uint32 first_free_block, uint32 want_blocks, uint32 blocks_per_segment,
							ClusterUndoExtent *out)
{
	uint32 nblocks;

	if (want_blocks == 0)
		want_blocks = 1;
	/* Data blocks are [1, blocks_per_segment);  block 0 is the segment header. */
	if (first_free_block < 1 || first_free_block >= blocks_per_segment)
		return 0; /* segment full */

	nblocks = blocks_per_segment - first_free_block; /* room to the segment end */
	if (nblocks > want_blocks)
		nblocks = want_blocks;

	out->segment_id = 0; /* caller fills in */
	out->first_block = first_free_block;
	out->nblocks = nblocks;
	out->cur_block = first_free_block;
	out->cur_free_offset = (uint32)sizeof(UndoBlockHeader);
	out->cur_slot_count = 0;
	return nblocks;
}

/* True if cur_block is past the last block of the extent (extent exhausted). */
static inline bool
cluster_undo_extent_exhausted(const ClusterUndoExtent *ext)
{
	return ext->segment_id == CLUSTER_UNDO_EXTENT_NONE
		   || ext->cur_block >= ext->first_block + ext->nblocks;
}

/*
 * cluster_undo_residual_reusable -- spec-4.12a Hardening v1.0.1 (P0 8.A): pure
 *	predicate deciding whether a residual extent carried over from a previous
 *	transaction may be reused.  Reusable iff it still holds an extent
 *	(segment_id != NONE) AND its segment is still the instance's active record
 *	segment.  The active record segment is never sealed / rolled away / recycled
 *	(a rollover seals it and publishes a new active_segment_id under
 *	lifecycle_lock; recycle requires COMMITTED/RECYCLABLE state; the cleaner
 *	excludes the active segment), so equality with active_segment_id implies the
 *	residual's segment is ACTIVE and unsealed -- a sealed/rolled/recycled segment
 *	necessarily differs from active_segment_id and is dropped.
 *
 *	The CALLER MUST read active_segment_id under lifecycle_lock: the rollover
 *	seals + publishes active_segment_id under that same lock, so the lock
 *	acquire establishes the happens-before this predicate relies on.  A relaxed
 *	(unlocked) read can be stale and wrongly approve reuse of a segment another
 *	backend already sealed, which 4.12a can then drain ACTIVE -> COMMITTED while
 *	this txn still writes undo into it (false reclaim of live undo).
 *	cluster_unit testable.
 */
static inline bool
cluster_undo_residual_reusable(uint32 residual_segment_id, uint32 active_segment_id)
{
	return residual_segment_id != CLUSTER_UNDO_EXTENT_NONE
		   && residual_segment_id == active_segment_id;
}

/*
 * Advance the backend-local cursor to the next block of the extent (fresh).
 * Caller must have verified !cluster_undo_extent_exhausted() for the result
 * to be a valid block;  after the last block this parks cur_block one past the
 * end so the next exhausted() check is true.
 */
static inline void
cluster_undo_extent_next_block(ClusterUndoExtent *ext)
{
	ext->cur_block++;
	ext->cur_free_offset = (uint32)sizeof(UndoBlockHeader);
	ext->cur_slot_count = 0;
}

#endif /* CLUSTER_UNDO_EXTENT_H */
