/*-------------------------------------------------------------------------
 *
 * test_cluster_thread_driver.c
 *	  pgrac spec-4.11 D1 increment 3b-1 — cluster_unit tests for the online
 *	  thread-recovery driver's PURE decision helpers.
 *
 *	  Two pure gates are pinned here in isolation; the .c driver computes the
 *	  live runtime facts and calls them, so the corruption-critical branches are
 *	  unit-testable without a backend:
 *
 *	    1. cluster_thread_recovery_should_rethrow(elevel) — the R13 harness
 *	       (amend 1): a catchable ERROR caught in the driver's PG_CATCH is
 *	       demoted to a result-returning BLOCKED (the worker survives), but a
 *	       FATAL/PANIC must NEVER be swallowed -- it is re-thrown.  This pins the
 *	       elevel boundary so the harness can never silently turn a survivor
 *	       crash into "recovery blocked".
 *
 *	    2. cluster_thread_recovery_decide_scope(...) — the capability/scope gate
 *	       (increment 1, §3 behaviour contract): when does a survivor attempt
 *	       online thread recovery at all (APPLICABLE) versus fall back to PG
 *	       native crash recovery (SINGLE_NODE), refuse for lack of a shared data
 *	       backend (NO_SHARED_BACKEND), refuse for >2-node multi-survivor
 *	       (MULTI_SURVIVOR, Q9), or stay off (DISABLED).
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
 *	  src/test/cluster_unit/test_cluster_thread_driver.c
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


/* ==========================================================================
 * cluster_thread_recovery_should_rethrow() — R13 elevel boundary (amend 1)
 * ========================================================================== */

UT_TEST(test_rethrow_error_is_demoted)
{
	/* A catchable ERROR is demoted to BLOCKED (worker survives): NOT re-thrown. */
	UT_ASSERT(!cluster_thread_recovery_should_rethrow(ERROR));
}

UT_TEST(test_rethrow_fatal_is_rethrown)
{
	/* amend 1: never swallow a FATAL -- re-throw so the survivor crashes as the
	 * cold component intended, instead of masquerading as "recovery blocked". */
	UT_ASSERT(cluster_thread_recovery_should_rethrow(FATAL));
}

UT_TEST(test_rethrow_panic_is_rethrown)
{
	/* PANIC (>= FATAL) is likewise never swallowed. */
	UT_ASSERT(cluster_thread_recovery_should_rethrow(PANIC));
}

UT_TEST(test_rethrow_warning_below_error_not_rethrown)
{
	/* Below ERROR never reaches PG_CATCH, but the boundary is elevel >= FATAL:
	 * anything under FATAL is demotable. */
	UT_ASSERT(!cluster_thread_recovery_should_rethrow(WARNING));
}


/* ==========================================================================
 * cluster_thread_recovery_decide_scope() — capability / scope gate
 * ========================================================================== */

UT_TEST(test_scope_disabled_when_guc_off)
{
	/* GUC off short-circuits before any other consideration. */
	UT_ASSERT_EQ((int)cluster_thread_recovery_decide_scope(false, true, true, 1),
				 (int)CLUSTER_THREADREC_SCOPE_DISABLED);
}

UT_TEST(test_scope_single_node_when_no_peers)
{
	/* No peers -> PG-native crash recovery, never online thread recovery. */
	UT_ASSERT_EQ((int)cluster_thread_recovery_decide_scope(true, false, true, 1),
				 (int)CLUSTER_THREADREC_SCOPE_SINGLE_NODE);
}

UT_TEST(test_scope_no_shared_backend)
{
	/* Peers but no genuinely shared data backend -> FEATURE_NOT_SUPPORTED
	 * (mirror spec-4.5a 53RA3 capability gate). */
	UT_ASSERT_EQ((int)cluster_thread_recovery_decide_scope(true, true, false, 1),
				 (int)CLUSTER_THREADREC_SCOPE_NO_SHARED_BACKEND);
}

UT_TEST(test_scope_multi_survivor_unsupported)
{
	/* >2-node scope: more than one survivor -> FEATURE_NOT_SUPPORTED (Q9). */
	UT_ASSERT_EQ((int)cluster_thread_recovery_decide_scope(true, true, true, 2),
				 (int)CLUSTER_THREADREC_SCOPE_MULTI_SURVIVOR);
}

UT_TEST(test_scope_zero_survivors_unsupported)
{
	/* Exactly one survivor is the 2-node scope; zero is also not the single
	 * applicable owner -> MULTI_SURVIVOR (live_survivor_count != 1). */
	UT_ASSERT_EQ((int)cluster_thread_recovery_decide_scope(true, true, true, 0),
				 (int)CLUSTER_THREADREC_SCOPE_MULTI_SURVIVOR);
}

UT_TEST(test_scope_applicable_2node_single_survivor)
{
	/* GUC on + peers + shared backend + exactly one survivor -> attempt. */
	UT_ASSERT_EQ((int)cluster_thread_recovery_decide_scope(true, true, true, 1),
				 (int)CLUSTER_THREADREC_SCOPE_APPLICABLE);
}


int
main(void)
{
	UT_PLAN(10);

	UT_RUN(test_rethrow_error_is_demoted);
	UT_RUN(test_rethrow_fatal_is_rethrown);
	UT_RUN(test_rethrow_panic_is_rethrown);
	UT_RUN(test_rethrow_warning_below_error_not_rethrown);

	UT_RUN(test_scope_disabled_when_guc_off);
	UT_RUN(test_scope_single_node_when_no_peers);
	UT_RUN(test_scope_no_shared_backend);
	UT_RUN(test_scope_multi_survivor_unsupported);
	UT_RUN(test_scope_zero_survivors_unsupported);
	UT_RUN(test_scope_applicable_2node_single_survivor);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
