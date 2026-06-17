/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_extent.c
 *	  pgrac spec-3.18 D3 — pure extent-arithmetic unit tests.
 *
 *	  Covers the lock-free extent math (cluster_undo_extent.h);  the stateful
 *	  claim (shared high-water + lifecycle_lock + autoextend) and its e2e
 *	  behavior land in D3.2 (cluster_undo_record.c + cluster_tap).
 *
 *	    U-E1  sequential claims from a moving high-water never overlap and are
 *	          gap-free (the no-overlap / unique-block invariant)
 *	    U-E5  undo_extent_blocks = 1 degenerates to per-block (pre-D3 behavior)
 *	    UE-B  segment-boundary clamp: a claim near the end is shortened; at/past
 *	          the end returns 0 (caller autoextends)
 *	    UE-C  backend-local cursor advance + exhaustion across the extent
 *	    UE-D  fresh-block cursor init (free_offset = sizeof header, slot_count 0)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.18-write-path-performance-overhaul.md (FROZEN, §2.3 + §3.5)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <string.h>

#include "cluster/cluster_undo_extent.h"
#include "cluster/cluster_undo_segment.h" /* UNDO_BLOCKS_PER_SEGMENT */

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/* ---- U-E1: sequential claims never overlap, no gaps (unique-block) ---- */
UT_TEST(test_extent_sequential_no_overlap)
{
	ClusterUndoExtent e;
	uint32 hw = 1; /* high-water starts at first data block */
	uint32 prev_end = 1;

	for (int i = 0; i < 5; i++) {
		uint32 n = cluster_undo_extent_compute(hw, 4, UNDO_BLOCKS_PER_SEGMENT, &e);

		UT_ASSERT_EQ((int)n, 4);						 /* full extent, far from end */
		UT_ASSERT_EQ((int)e.first_block, (int)hw);		 /* starts at high-water */
		UT_ASSERT_EQ((int)e.first_block, (int)prev_end); /* gap-free vs previous */
		prev_end = e.first_block + e.nblocks;			 /* next claim starts here */
		hw = prev_end;									 /* caller advances high-water by n */
	}
	/* 5 claims of 4 => blocks [1,21), no overlap, no gap. */
	UT_ASSERT_EQ((int)hw, 21);
}


/* ---- U-E5: undo_extent_blocks = 1 degenerates to per-block ---- */
UT_TEST(test_extent_degenerate_one_block)
{
	ClusterUndoExtent e;
	uint32 hw = 1;

	for (int i = 0; i < 3; i++) {
		uint32 n = cluster_undo_extent_compute(hw, 1, UNDO_BLOCKS_PER_SEGMENT, &e);

		UT_ASSERT_EQ((int)n, 1);
		UT_ASSERT_EQ((int)e.nblocks, 1);
		UT_ASSERT_EQ((int)e.first_block, (int)hw);
		e.segment_id = 1; /* caller fills in segment_id to make the extent "held" */
		/* one-block extent is immediately exhausted after its single block */
		UT_ASSERT(!cluster_undo_extent_exhausted(&e)); /* cur_block == first_block, valid */
		cluster_undo_extent_next_block(&e);
		UT_ASSERT(cluster_undo_extent_exhausted(&e)); /* now past the end */
		hw += n;
	}
	/* want_blocks == 0 is treated as 1 (defensive). */
	UT_ASSERT_EQ((int)cluster_undo_extent_compute(hw, 0, UNDO_BLOCKS_PER_SEGMENT, &e), 1);
}


/* ---- UE-B: segment-boundary clamp ---- */
UT_TEST(test_extent_boundary_clamp)
{
	ClusterUndoExtent e;
	uint32 last = UNDO_BLOCKS_PER_SEGMENT; /* 8192; data blocks are [1, 8192) */

	/* 2 blocks from the end -> clamped to 2 even if want 4. */
	UT_ASSERT_EQ((int)cluster_undo_extent_compute(last - 2, 4, UNDO_BLOCKS_PER_SEGMENT, &e), 2);
	UT_ASSERT_EQ((int)e.first_block, (int)(last - 2));
	UT_ASSERT_EQ((int)e.nblocks, 2);

	/* exactly 1 block from the end -> clamped to 1. */
	UT_ASSERT_EQ((int)cluster_undo_extent_compute(last - 1, 4, UNDO_BLOCKS_PER_SEGMENT, &e), 1);

	/* at the end (== blocks_per_segment) -> segment full, 0. */
	UT_ASSERT_EQ((int)cluster_undo_extent_compute(last, 4, UNDO_BLOCKS_PER_SEGMENT, &e), 0);
	/* past the end -> 0. */
	UT_ASSERT_EQ((int)cluster_undo_extent_compute(last + 5, 4, UNDO_BLOCKS_PER_SEGMENT, &e), 0);
	/* block 0 (the header) is not a valid data-block start -> 0. */
	UT_ASSERT_EQ((int)cluster_undo_extent_compute(0, 4, UNDO_BLOCKS_PER_SEGMENT, &e), 0);
}


/* ---- UE-C: backend-local cursor advance + exhaustion ---- */
UT_TEST(test_extent_cursor_advance)
{
	ClusterUndoExtent e;

	cluster_undo_extent_compute(100, 4, UNDO_BLOCKS_PER_SEGMENT, &e);
	e.segment_id = 1; /* caller fills in segment_id to make the extent "held" */
	UT_ASSERT_EQ((int)e.cur_block, 100);
	UT_ASSERT(!cluster_undo_extent_exhausted(&e));

	cluster_undo_extent_next_block(&e); /* 101 */
	UT_ASSERT_EQ((int)e.cur_block, 101);
	UT_ASSERT(!cluster_undo_extent_exhausted(&e));

	cluster_undo_extent_next_block(&e); /* 102 */
	cluster_undo_extent_next_block(&e); /* 103 = last (first_block+nblocks-1) */
	UT_ASSERT_EQ((int)e.cur_block, 103);
	UT_ASSERT(!cluster_undo_extent_exhausted(&e));

	cluster_undo_extent_next_block(&e); /* 104 = one past end */
	UT_ASSERT_EQ((int)e.cur_block, 104);
	UT_ASSERT(cluster_undo_extent_exhausted(&e));

	/* the NONE sentinel is always exhausted. */
	memset(&e, 0, sizeof(e));
	UT_ASSERT_EQ((int)e.segment_id, CLUSTER_UNDO_EXTENT_NONE);
	UT_ASSERT(cluster_undo_extent_exhausted(&e));
}


/* ---- UE-D: fresh-block cursor init ---- */
UT_TEST(test_extent_fresh_block_init)
{
	ClusterUndoExtent e;

	cluster_undo_extent_compute(7, 4, UNDO_BLOCKS_PER_SEGMENT, &e);
	UT_ASSERT_EQ((int)e.cur_free_offset, (int)sizeof(UndoBlockHeader)); /* 48B */
	UT_ASSERT_EQ((int)e.cur_slot_count, 0);

	/* dirtying the cursor then advancing re-inits the next block fresh. */
	e.cur_free_offset = 1234;
	e.cur_slot_count = 9;
	cluster_undo_extent_next_block(&e);
	UT_ASSERT_EQ((int)e.cur_free_offset, (int)sizeof(UndoBlockHeader));
	UT_ASSERT_EQ((int)e.cur_slot_count, 0);
}


/*
 * spec-4.12a Hardening v1.0.1 (P0 8.A): cluster_undo_residual_reusable -- a
 * carried-over residual extent may be reused iff it still holds an extent AND
 * its segment is still the active record segment.  The caller reads
 * active_segment_id under lifecycle_lock; here we just pin the pure decision.
 * A residual in a rolled-away (hence sealed) / recycled / reborn segment has
 * segment_id != active_segment_id and must be dropped, so the cleaner cannot
 * later false-COMMIT a segment this txn is still writing undo into.
 */
UT_TEST(test_residual_reusable_active_segment_only)
{
	/* held residual, still the active segment -> reusable. */
	UT_ASSERT(cluster_undo_residual_reusable(5, 5));

	/* held residual, segment rolled away (active moved on) -> drop. */
	UT_ASSERT(!cluster_undo_residual_reusable(5, 6)); /* rolled forward */
	UT_ASSERT(!cluster_undo_residual_reusable(9, 3)); /* reused low id */

	/* no residual held (NONE sentinel) -> never reusable, regardless of active. */
	UT_ASSERT(!cluster_undo_residual_reusable(CLUSTER_UNDO_EXTENT_NONE, 5));
	UT_ASSERT(!cluster_undo_residual_reusable(CLUSTER_UNDO_EXTENT_NONE, CLUSTER_UNDO_EXTENT_NONE));

	/* a sealed/rolled segment is, by the active-segment invariant, never equal to
	 * active_segment_id -> the "!= active" branch covers the sealed/non-active
	 * cases without reading on-disk state/seal (no block-0 I/O on the hot path). */
	UT_ASSERT(!cluster_undo_residual_reusable(7, 8)); /* 7 sealed+rolled, active=8 */
}


int
main(void)
{
	UT_RUN(test_extent_sequential_no_overlap);
	UT_RUN(test_extent_degenerate_one_block);
	UT_RUN(test_extent_boundary_clamp);
	UT_RUN(test_extent_cursor_advance);
	UT_RUN(test_extent_fresh_block_init);
	UT_RUN(test_residual_reusable_active_segment_only); /* spec-4.12a Hardening v1.0.1 */
	UT_DONE();
}
