/*-------------------------------------------------------------------------
 *
 * test_cluster_thread_orchestrator.c
 *	  pgrac spec-4.11 D1 increment 3b-2 — cluster_unit tests for the online
 *	  thread-recovery orchestrator's PURE decision helper.
 *
 *	  cluster_thread_recovery_decide_on_blocked(policy) -- the on_unrecoverable
 *	  escalation (Q5): the .c orchestrator reaches a FINAL BLOCKED and calls this
 *	  on the cluster.thread_recovery_on_unrecoverable GUC to decide whether to
 *	  return the BLOCKED (keep_frozen -- survivor lives) or PANIC the survivor.
 *	  Pinned in isolation so the escalation can NEVER turn the default keep_frozen
 *	  into a crash.  (The R13 elevel boundary and R14 writer admission live in
 *	  test_cluster_remote_xact.c; the scope gate in test_cluster_thread_driver.c.)
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
 *	  src/test/cluster_unit/test_cluster_thread_orchestrator.c
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


UT_TEST(test_on_blocked_keep_frozen_default)
{
	/* The default keep_frozen policy returns the BLOCKED: only that thread's
	 * resources stay frozen, the survivor keeps running (minimum blast radius). */
	UT_ASSERT_EQ(
		(int)cluster_thread_recovery_decide_on_blocked(CLUSTER_THREADREC_ACTION_KEEP_FROZEN),
		(int)CLUSTER_THREADREC_ONBLOCKED_KEEP_FROZEN);
}

UT_TEST(test_on_blocked_panic_when_policy_panic)
{
	/* The panic escape valve crashes the survivor at postmaster level. */
	UT_ASSERT_EQ((int)cluster_thread_recovery_decide_on_blocked(CLUSTER_THREADREC_ACTION_PANIC),
				 (int)CLUSTER_THREADREC_ONBLOCKED_PANIC);
}

UT_TEST(test_on_blocked_unknown_policy_is_keep_frozen)
{
	/* Any value other than the explicit PANIC enum maps to keep_frozen: the
	 * escalation defaults to the SAFE (non-crashing) direction, never to PANIC. */
	UT_ASSERT_EQ((int)cluster_thread_recovery_decide_on_blocked(-1),
				 (int)CLUSTER_THREADREC_ONBLOCKED_KEEP_FROZEN);
	UT_ASSERT_EQ((int)cluster_thread_recovery_decide_on_blocked(999),
				 (int)CLUSTER_THREADREC_ONBLOCKED_KEEP_FROZEN);
}

UT_TEST(test_on_blocked_default_enum_is_keep_frozen)
{
	/* KEEP_FROZEN must be 0 so a zero-initialised / default policy is safe. */
	UT_ASSERT_EQ((int)CLUSTER_THREADREC_ONBLOCKED_KEEP_FROZEN, 0);
	UT_ASSERT((int)CLUSTER_THREADREC_ONBLOCKED_PANIC != 0);
}


int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_on_blocked_keep_frozen_default);
	UT_RUN(test_on_blocked_panic_when_policy_panic);
	UT_RUN(test_on_blocked_unknown_policy_is_keep_frozen);
	UT_RUN(test_on_blocked_default_enum_is_keep_frozen);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
