/*-------------------------------------------------------------------------
 *
 * test_cluster_thread_replay.c
 *	  pgrac spec-4.11 D1 increment 3a — cluster_unit tests for the RMW replay
 *	  engine's PURE block-classification gate (corruption-critical 8.A).
 *
 *	  cluster_thread_recovery_replay_stream() reads a dead thread's WAL and, for
 *	  every block reference, must decide one of three things BEFORE touching the
 *	  page: read-modify-write it (TARGET), skip it as per-node state
 *	  (OUT_OF_SCOPE), or fail closed (BLOCKED).  That decision is extracted into
 *	  the pure cluster_thread_replay_classify_block() so it can be pinned in
 *	  isolation here -- the smgr I/O wrapper merely computes the inputs and acts.
 *
 *	  The two fail-closed branches are the whole 8.A surface this gate adds:
 *	    - a relation that no longer exists -> BLOCKED, NOT a BLK_NOTFOUND-style
 *	      skip (amend 1): 3a runs only the data-page apply matrix and never the
 *	      storage create/drop/truncate rmgr, so it cannot prove a missing file is
 *	      a legitimate drop;
 *	    - a block at/beyond EOF (relation extension / new init page) -> BLOCKED
 *	      and forwarded (Stage 5), never an out-of-range read.
 *
 *	  Standalone executable per spec-0.4 §9.2.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_thread_replay.c
 *
 * Spec: spec-4.11-thread-recovery.md (FROZEN v0.3)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "cluster/cluster_thread_recovery.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/*
 * libpgport's snprintf.c references ExceptionalCondition in a cassert build; this
 * pure-inline test links libpgport for the unit harness but no backend object, so
 * provide a local stub (mirrors test_cluster_thread_apply.c).  It must never fire.
 */
void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# unexpected Assert: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}


/* ==========================================================================
 * cluster_thread_replay_classify_block() — pure gate truth table
 * ========================================================================== */

UT_TEST(test_classify_non_shared_is_out_of_scope)
{
	/* which_for != 1 (temp / catalog / cluster_fs off): per-node data-pass skip,
	 * regardless of the (here irrelevant) existence / range arguments. */
	UT_ASSERT_EQ((int)cluster_thread_replay_classify_block(0, true, 0, 1),
				 (int)CLUSTER_THREADREPLAY_BLK_OUT_OF_SCOPE);
}

UT_TEST(test_classify_out_of_scope_ignores_other_args)
{
	/* which_for != 1 short-circuits: a would-be BLOCKED (missing / out of range)
	 * is still OUT_OF_SCOPE because the survivor owns its own per-node copy. */
	UT_ASSERT_EQ((int)cluster_thread_replay_classify_block(0, false, 9, 3),
				 (int)CLUSTER_THREADREPLAY_BLK_OUT_OF_SCOPE);
}

UT_TEST(test_classify_dropped_relation_is_blocked)
{
	/* amend 1: shared rel that no longer exists -> fail-closed, NOT a skip. */
	UT_ASSERT_EQ((int)cluster_thread_replay_classify_block(1, false, 0, 0),
				 (int)CLUSTER_THREADREPLAY_BLK_BLOCKED);
}

UT_TEST(test_classify_dropped_takes_precedence_over_in_range)
{
	/* !rel_exists is checked before the range: a missing file is fail-closed
	 * even if blocknum would be in range against a stale nblocks. */
	UT_ASSERT_EQ((int)cluster_thread_replay_classify_block(1, false, 0, 8),
				 (int)CLUSTER_THREADREPLAY_BLK_BLOCKED);
}

UT_TEST(test_classify_extension_block_is_blocked)
{
	/* block at EOF (blocknum == nblocks): relation extension / new page -> BLOCKED. */
	UT_ASSERT_EQ((int)cluster_thread_replay_classify_block(1, true, 5, 5),
				 (int)CLUSTER_THREADREPLAY_BLK_BLOCKED);
}

UT_TEST(test_classify_beyond_eof_is_blocked)
{
	/* block well beyond EOF -> BLOCKED, never an out-of-range read. */
	UT_ASSERT_EQ((int)cluster_thread_replay_classify_block(1, true, 9, 3),
				 (int)CLUSTER_THREADREPLAY_BLK_BLOCKED);
}

UT_TEST(test_classify_in_range_is_target)
{
	/* shared, exists, blocknum < nblocks -> read-modify-write. */
	UT_ASSERT_EQ((int)cluster_thread_replay_classify_block(1, true, 0, 1),
				 (int)CLUSTER_THREADREPLAY_BLK_TARGET);
}

UT_TEST(test_classify_last_in_range_is_target)
{
	/* boundary: blocknum == nblocks - 1 is the last valid block -> TARGET. */
	UT_ASSERT_EQ((int)cluster_thread_replay_classify_block(1, true, 4, 5),
				 (int)CLUSTER_THREADREPLAY_BLK_TARGET);
}

UT_TEST(test_classify_classes_distinct)
{
	UT_ASSERT_NE((int)CLUSTER_THREADREPLAY_BLK_TARGET, (int)CLUSTER_THREADREPLAY_BLK_OUT_OF_SCOPE);
	UT_ASSERT_NE((int)CLUSTER_THREADREPLAY_BLK_TARGET, (int)CLUSTER_THREADREPLAY_BLK_BLOCKED);
	UT_ASSERT_NE((int)CLUSTER_THREADREPLAY_BLK_OUT_OF_SCOPE, (int)CLUSTER_THREADREPLAY_BLK_BLOCKED);
}


int
main(void)
{
	UT_PLAN(9);

	UT_RUN(test_classify_non_shared_is_out_of_scope);
	UT_RUN(test_classify_out_of_scope_ignores_other_args);
	UT_RUN(test_classify_dropped_relation_is_blocked);
	UT_RUN(test_classify_dropped_takes_precedence_over_in_range);
	UT_RUN(test_classify_extension_block_is_blocked);
	UT_RUN(test_classify_beyond_eof_is_blocked);
	UT_RUN(test_classify_in_range_is_target);
	UT_RUN(test_classify_last_in_range_is_target);
	UT_RUN(test_classify_classes_distinct);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
