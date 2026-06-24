/*-------------------------------------------------------------------------
 *
 * test_cluster_hw.c
 *	  Unit tests for the HW (relation extend) cluster block-number authority
 *	  pure layer (spec-5.7a D1).
 *
 *	  U1 covers the HW resid encoder (per (db,rel,spc,fork), namespace 0xF2,
 *	  distinct from SQ 0xF0 / CF 0xF1).  U2 covers the three-state extend
 *	  classifier (GLOBALIZE / NATIVE_LOCAL / FAIL_CLOSED -- spec-5.7 HW-M7)
 *	  and the master-side batch allocator math (monotone, non-overlapping,
 *	  truncating at the block-number ceiling -- spec-5.7 §3.1a M1b).  The real
 *	  cross-node authority (IC HW_ALLOC to the master + reservation WAL) is the
 *	  2-node TAP (t/287); this file pins the pure, master-side allocation math
 *	  and the persistence classifier that decides whether a relation extend is
 *	  globalized at all.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_hw.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D1, §3.1a v1.1)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_class.h" /* RELPERSISTENCE_* */
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_hw.h"
#include "cluster/cluster_sequence.h"
#include "storage/block.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

/* ======================================================================
 * U1 -- HW resid encoding: per (db,rel,spc,fork), namespace 0xF2
 * ====================================================================== */
UT_TEST(test_hw_resid_encode)
{
	RelFileLocator rloc;
	ClusterResId r;

	rloc.spcOid = 1663;		/* pg_default */
	rloc.dbOid = 5;			/* some database */
	rloc.relNumber = 16384; /* relfilenode (ABA defence) */

	memset(&r, 0xEE, sizeof(r));
	cluster_hw_resid_encode(rloc, MAIN_FORKNUM, &r);

	UT_ASSERT_EQ(r.field1, 5);	   /* dbOid */
	UT_ASSERT_EQ(r.field2, 16384); /* relNumber */
	UT_ASSERT_EQ(r.field3, 1663);  /* spcOid */
	UT_ASSERT_EQ(r.field4, MAIN_FORKNUM);
	UT_ASSERT_EQ(r.type, CLUSTER_HW_RESID_TYPE);
	UT_ASSERT_EQ(r.type, 0xF2);
	UT_ASSERT_NE(r.type, CLUSTER_SQ_RESID_TYPE); /* != 0xF0 */
	UT_ASSERT_NE(r.type, CLUSTER_CF_RESID_TYPE); /* != 0xF1 */
	UT_ASSERT_EQ(r.lockmethodid, DEFAULT_LOCKMETHOD);
}

/* the fork byte distinguishes main / fsm / vm extents of the same relation */
UT_TEST(test_hw_resid_fork_distinct)
{
	RelFileLocator rloc;
	ClusterResId main_r, fsm_r;

	rloc.spcOid = 1663;
	rloc.dbOid = 5;
	rloc.relNumber = 16384;

	cluster_hw_resid_encode(rloc, MAIN_FORKNUM, &main_r);
	cluster_hw_resid_encode(rloc, FSM_FORKNUM, &fsm_r);

	UT_ASSERT_NE(main_r.field4, fsm_r.field4);
	UT_ASSERT_EQ(main_r.field1, fsm_r.field1); /* same relation otherwise */
}

/* ======================================================================
 * U2 -- three-state extend classifier (spec-5.7 HW-M7)
 *
 *	GLOBALIZE     WAL-logged shared permanent (RelationNeedsWAL true).
 *	NATIVE_LOCAL  temp; single-node unlogged; new-in-txn minimal-WAL
 *	              permanent (uncommitted -> invisible to peers).
 *	FAIL_CLOSED   unlogged in a multi-node cluster (no WAL authority to
 *	              rebuild the HWM -> must never silently extend locally).
 * ====================================================================== */
UT_TEST(test_hw_classify_permanent_globalize)
{
	/* any permanent shared relation (incl new-in-txn / minimal-WAL, amend #3)
	 * is owned by the authority from block 0 -> GLOBALIZE */
	UT_ASSERT_EQ(cluster_hw_classify_persistence(RELPERSISTENCE_PERMANENT, /*multi_node=*/true),
				 CLUSTER_HW_GLOBALIZE);
}

UT_TEST(test_hw_classify_temp_native)
{
	/* temp relations use PG-local buffers; never globalized */
	UT_ASSERT_EQ(cluster_hw_classify_persistence(RELPERSISTENCE_TEMP, /*multi_node=*/true),
				 CLUSTER_HW_NATIVE_LOCAL);
}

UT_TEST(test_hw_classify_permanent_singlenode_globalize)
{
	/* permanent is GLOBALIZE regardless of node count (amend #3: classification
	 * is persistence-driven, not needs_wal / new-in-txn driven) */
	UT_ASSERT_EQ(cluster_hw_classify_persistence(RELPERSISTENCE_PERMANENT, /*multi_node=*/false),
				 CLUSTER_HW_GLOBALIZE);
}

UT_TEST(test_hw_classify_unlogged_multinode_failclosed)
{
	/* unlogged multi-node: no WAL to rebuild the HWM -> must fail closed,
	 * never silently fall back to a local FileSize extend (spec-5.7 HW-M7) */
	UT_ASSERT_EQ(cluster_hw_classify_persistence(RELPERSISTENCE_UNLOGGED, /*multi_node=*/true),
				 CLUSTER_HW_FAIL_CLOSED);
}

UT_TEST(test_hw_classify_unlogged_singlenode_native)
{
	/* single-node unlogged has no cross-node coherence problem */
	UT_ASSERT_EQ(cluster_hw_classify_persistence(RELPERSISTENCE_UNLOGGED, /*multi_node=*/false),
				 CLUSTER_HW_NATIVE_LOCAL);
}

/* ======================================================================
 * U2 -- master-side batch allocator math (spec-5.7 §3.1a M1b)
 * ====================================================================== */
UT_TEST(test_hw_alloc_monotone_nonoverlap)
{
	BlockNumber hwm = 100;
	uint32 granted = 0;
	BlockNumber new_hwm = 0;
	BlockNumber first;

	first = cluster_hw_alloc_segment(hwm, 8, &granted, &new_hwm);
	UT_ASSERT_EQ(first, 100);
	UT_ASSERT_EQ(granted, 8);
	UT_ASSERT_EQ(new_hwm, 108);

	/* the next allocation starts exactly where the previous ended (no gap,
	 * no overlap) -- this is the cross-node no-duplicate-block invariant */
	first = cluster_hw_alloc_segment(new_hwm, 4, &granted, &new_hwm);
	UT_ASSERT_EQ(first, 108);
	UT_ASSERT_EQ(granted, 4);
	UT_ASSERT_EQ(new_hwm, 112);
}

UT_TEST(test_hw_alloc_truncates_at_ceiling)
{
	BlockNumber hwm = MaxBlockNumber - 2; /* only 3 blocks left (incl. ceiling) */
	uint32 granted = 0;
	BlockNumber new_hwm = 0;
	BlockNumber first;

	first = cluster_hw_alloc_segment(hwm, 8, &granted, &new_hwm);
	UT_ASSERT_EQ(first, MaxBlockNumber - 2);
	UT_ASSERT_EQ(granted, 3); /* clamped: blocks (Max-2),(Max-1),(Max) */
	UT_ASSERT_EQ(new_hwm, (BlockNumber)MaxBlockNumber + 1);
}

UT_TEST(test_hw_alloc_exhausted)
{
	BlockNumber hwm = (BlockNumber)MaxBlockNumber + 1; /* == InvalidBlockNumber */
	uint32 granted = 99;
	BlockNumber new_hwm = 0;
	BlockNumber first;

	first = cluster_hw_alloc_segment(hwm, 8, &granted, &new_hwm);
	UT_ASSERT_EQ(first, InvalidBlockNumber);
	UT_ASSERT_EQ(granted, 0); /* nothing granted */
}

/* ======================================================================
 * U3 -- HW serve gate (spec-5.7 §3.1b R4/R6, 8.A)
 *
 *	The authority may serve an HW_ALLOC for a resid ONLY when that resid's
 *	GRD shard is steady for the CURRENT remaster generation:
 *	  phase == NORMAL  AND  hw_rebuilt_generation == master_generation.
 *	Otherwise the master just adopted the shard (reconfig freeze / rebuild in
 *	flight, or the per-shard HWM not yet rebuilt from the dead master's
 *	snapshot+tail) and auto-creating an absent entry at block 0 would re-hand
 *	an already-allocated range -> silent block reuse (R9).  This truth table
 *	is the single tested definition of "safe to serve / safe to create at 0".
 * ====================================================================== */

/* boot / steady state, never remastered: gen 0 == 0, NORMAL -> serve OK */
UT_TEST(test_hw_serve_gate_boot_normal)
{
	UT_ASSERT_EQ(cluster_hw_serve_allowed(GRD_SHARD_NORMAL, 0, 0), true);
}

/* remastered AND rebuilt: master_generation caught up by hw_rebuilt_generation */
UT_TEST(test_hw_serve_gate_remastered_rebuilt)
{
	UT_ASSERT_EQ(cluster_hw_serve_allowed(GRD_SHARD_NORMAL, 5, 5), true);
}

/* just remastered, NOT yet rebuilt (gen bumped past rebuilt) -> fail closed */
UT_TEST(test_hw_serve_gate_remastered_unrebuilt_failclosed)
{
	UT_ASSERT_EQ(cluster_hw_serve_allowed(GRD_SHARD_NORMAL, 1, 0), false);
	UT_ASSERT_EQ(cluster_hw_serve_allowed(GRD_SHARD_NORMAL, 5, 4), false);
}

/* mid-reconfig freeze: never serve, even at a matched generation */
UT_TEST(test_hw_serve_gate_frozen_failclosed)
{
	UT_ASSERT_EQ(cluster_hw_serve_allowed(GRD_SHARD_FROZEN, 0, 0), false);
	UT_ASSERT_EQ(cluster_hw_serve_allowed(GRD_SHARD_FROZEN, 5, 5), false);
}

/* rebuild barrier in flight: never serve, even at a matched generation */
UT_TEST(test_hw_serve_gate_rebuilding_failclosed)
{
	UT_ASSERT_EQ(cluster_hw_serve_allowed(GRD_SHARD_REBUILDING, 0, 0), false);
	UT_ASSERT_EQ(cluster_hw_serve_allowed(GRD_SHARD_REBUILDING, 5, 5), false);
}

int
main(void)
{
	UT_PLAN(15);
	UT_RUN(test_hw_resid_encode);
	UT_RUN(test_hw_resid_fork_distinct);
	UT_RUN(test_hw_classify_permanent_globalize);
	UT_RUN(test_hw_classify_temp_native);
	UT_RUN(test_hw_classify_permanent_singlenode_globalize);
	UT_RUN(test_hw_classify_unlogged_multinode_failclosed);
	UT_RUN(test_hw_classify_unlogged_singlenode_native);
	UT_RUN(test_hw_alloc_monotone_nonoverlap);
	UT_RUN(test_hw_alloc_truncates_at_ceiling);
	UT_RUN(test_hw_alloc_exhausted);
	UT_RUN(test_hw_serve_gate_boot_normal);
	UT_RUN(test_hw_serve_gate_remastered_rebuilt);
	UT_RUN(test_hw_serve_gate_remastered_unrebuilt_failclosed);
	UT_RUN(test_hw_serve_gate_frozen_failclosed);
	UT_RUN(test_hw_serve_gate_rebuilding_failclosed);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
