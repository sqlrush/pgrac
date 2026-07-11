/*-------------------------------------------------------------------------
 *
 * test_cluster_hw_remaster_retry.c
 *    Unit tests for spec-4.6a same-episode HW remaster retry decisions.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_hw_remaster_retry.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_hw_remaster.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

static ClusterHwRemasterRelaunchDecision
decide(uint64 launched, uint64 episode, ClusterHwRemasterResult result, uint32 attempts,
	   uint64 next_attempt_at, uint64 now, int max_attempts)
{
	return cluster_hw_remaster_relaunch_decide(launched, episode, result, attempts, next_attempt_at,
											   now, max_attempts);
}

UT_TEST(test_new_episode_initial_launch_resets_state)
{
	ClusterHwRemasterRelaunchDecision d;

	d = decide(10, 11, CLUSTER_HW_REMASTER_BLOCKED, 7, 1234, 2000, 16);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_INITIAL);
	UT_ASSERT_EQ(d.next_attempts, 0);
	UT_ASSERT_EQ(d.next_attempt_at, 0);
	UT_ASSERT_EQ(d.next_result, CLUSTER_HW_REMASTER_RUNNING);
}

UT_TEST(test_running_done_and_not_applicable_do_not_relaunch)
{
	UT_ASSERT_EQ(decide(11, 11, CLUSTER_HW_REMASTER_RUNNING, 0, 0, 2000, 16).action,
				 CLUSTER_HW_REMASTER_LAUNCH_SKIP);
	UT_ASSERT_EQ(decide(11, 11, CLUSTER_HW_REMASTER_DONE, 0, 0, 2000, 16).action,
				 CLUSTER_HW_REMASTER_LAUNCH_SKIP);
	UT_ASSERT_EQ(decide(11, 11, CLUSTER_HW_REMASTER_NOT_APPLICABLE, 0, 0, 2000, 16).action,
				 CLUSTER_HW_REMASTER_LAUNCH_SKIP);
}

UT_TEST(test_structural_blocked_warns_once_and_never_retries)
{
	ClusterHwRemasterRelaunchDecision d;

	d = decide(11, 11, CLUSTER_HW_REMASTER_BLOCKED_STRUCTURAL, 0, 0, 2000, 16);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_MARK_STRUCTURAL);
	UT_ASSERT_EQ(d.next_attempt_at, CLUSTER_HW_REMASTER_NO_DEADLINE);

	d = decide(11, 11, CLUSTER_HW_REMASTER_BLOCKED_STRUCTURAL, 0, CLUSTER_HW_REMASTER_NO_DEADLINE,
			   3000, 16);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_SKIP);
}

UT_TEST(test_blocked_waits_until_backoff_deadline)
{
	ClusterHwRemasterRelaunchDecision d;

	d = decide(11, 11, CLUSTER_HW_REMASTER_BLOCKED, 0, 5000, 4999, 16);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_SKIP);

	d = decide(11, 11, CLUSTER_HW_REMASTER_BLOCKED, 0, 5000, 5000, 16);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_RETRY);
	UT_ASSERT_EQ(d.next_attempts, 1);
	UT_ASSERT_EQ(d.next_result, CLUSTER_HW_REMASTER_RUNNING);
}

UT_TEST(test_cap_exhausts_once_and_sighup_raise_recovers)
{
	ClusterHwRemasterRelaunchDecision d;

	d = decide(11, 11, CLUSTER_HW_REMASTER_BLOCKED, 2, 0, 5000, 2);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_MARK_EXHAUSTED);
	UT_ASSERT_EQ(d.next_attempt_at, CLUSTER_HW_REMASTER_NO_DEADLINE);

	d = decide(11, 11, CLUSTER_HW_REMASTER_BLOCKED, 2, CLUSTER_HW_REMASTER_NO_DEADLINE, 6000, 2);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_SKIP);

	/* Operator raised cluster.hw_remaster_retry_max_attempts via SIGHUP. */
	d = decide(11, 11, CLUSTER_HW_REMASTER_BLOCKED, 2, CLUSTER_HW_REMASTER_NO_DEADLINE, 7000, 3);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_RETRY);
	UT_ASSERT_EQ(d.next_attempts, 3);
}

UT_TEST(test_zero_max_attempts_disables_retry)
{
	ClusterHwRemasterRelaunchDecision d;

	d = decide(11, 11, CLUSTER_HW_REMASTER_BLOCKED, 0, 0, 5000, 0);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_MARK_EXHAUSTED);
}

/* spec-4.6a section 2.1 truth-table row 8: latched == episode but the result
 * slot reads NONE (registration failed and was reverted) -> take the
 * first-launch row again; no attempt is charged. */
UT_TEST(test_none_result_registration_failed_falls_back_to_initial)
{
	ClusterHwRemasterRelaunchDecision d;

	d = decide(11, 11, CLUSTER_HW_REMASTER_NONE, 3, 999, 2000, 16);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_INITIAL);
	UT_ASSERT_EQ(d.next_attempts, 0);
	UT_ASSERT_EQ(d.next_attempt_at, 0);
	UT_ASSERT_EQ(d.next_result, CLUSTER_HW_REMASTER_RUNNING);
}

UT_TEST(test_backoff_exponential_cap)
{
	UT_ASSERT_EQ(cluster_hw_remaster_compute_backoff_ms(1000, 0), 1000);
	UT_ASSERT_EQ(cluster_hw_remaster_compute_backoff_ms(1000, 1), 1000);
	UT_ASSERT_EQ(cluster_hw_remaster_compute_backoff_ms(1000, 2), 2000);
	UT_ASSERT_EQ(cluster_hw_remaster_compute_backoff_ms(1000, 7), 60000);
	UT_ASSERT_EQ(cluster_hw_remaster_compute_backoff_ms(100, 20), 60000);
}


int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_new_episode_initial_launch_resets_state);
	UT_RUN(test_running_done_and_not_applicable_do_not_relaunch);
	UT_RUN(test_structural_blocked_warns_once_and_never_retries);
	UT_RUN(test_blocked_waits_until_backoff_deadline);
	UT_RUN(test_cap_exhausts_once_and_sighup_raise_recovers);
	UT_RUN(test_zero_max_attempts_disables_retry);
	UT_RUN(test_none_result_registration_failed_falls_back_to_initial);
	UT_RUN(test_backoff_exponential_cap);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
