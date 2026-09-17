/*-------------------------------------------------------------------------
 *
 * test_cluster_page_recovery.c
 *    RF-PAGE PGDEL-03 focused unit tests: the §4.1 recovery-action table
 *    (closed class -> action mapping), the §3.5 per-block state machine
 *    (strictly adjacent advance), and the §8.1 dispatcher verdict
 *    combination (class + §3.2 decide -> one outcome).
 *
 *    RED mapping (spec §10.1 / §3.5 / §8.1 / G9):
 *      - UNKNOWN / UNCLASSIFIED / WILLINIT (no rule) -> BLOCKED action;
 *      - every class maps to exactly one action;
 *      - state machine: adjacent advance only; jump/repeat/terminal
 *        re-advance rejected; NULL rejected;
 *      - verdict: APPLY/SKIP from decide; any blocked verdict from the
 *        class layer is BLOCKED_CLASS (finer subdivisions belong to
 *        PGDEL-04..06, never guessed here).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_page_recovery.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

#include <stdio.h>

UT_TEST(test_action_table_closed_rows)
{
	/* §4.1 recovery-action column, one row per class. */
	UT_ASSERT_EQ((int)cluster_page_class_recovery_action(CLUSTER_PAGE_CLASS_NORMAL),
				 (int)CLUSTER_PAGE_ACTION_APPLY);
	UT_ASSERT_EQ((int)cluster_page_class_recovery_action(CLUSTER_PAGE_CLASS_CLEANOUT),
				 (int)CLUSTER_PAGE_ACTION_APPLY);
	UT_ASSERT_EQ((int)cluster_page_class_recovery_action(CLUSTER_PAGE_CLASS_NEW),
				 (int)CLUSTER_PAGE_ACTION_INIT);
	UT_ASSERT_EQ((int)cluster_page_class_recovery_action(CLUSTER_PAGE_CLASS_INCARNATION),
				 (int)CLUSTER_PAGE_ACTION_INCARNATE);
	UT_ASSERT_EQ((int)cluster_page_class_recovery_action(CLUSTER_PAGE_CLASS_TEMP),
				 (int)CLUSTER_PAGE_ACTION_DISCARD);
	UT_ASSERT_EQ((int)cluster_page_class_recovery_action(CLUSTER_PAGE_CLASS_REBUILDABLE),
				 (int)CLUSTER_PAGE_ACTION_REBUILD);
	UT_ASSERT_EQ((int)cluster_page_class_recovery_action(CLUSTER_PAGE_CLASS_HEADER),
				 (int)CLUSTER_PAGE_ACTION_ROUTE);
	UT_ASSERT_EQ((int)cluster_page_class_recovery_action(CLUSTER_PAGE_CLASS_FULLIMAGE),
				 (int)CLUSTER_PAGE_ACTION_IMAGE);
	UT_ASSERT_EQ((int)cluster_page_class_recovery_action(CLUSTER_PAGE_CLASS_NONLOGGED),
				 (int)CLUSTER_PAGE_ACTION_REBUILD);
}

UT_TEST(test_action_unknown_default_blocked)
{
	/* "unknown default 必须 BLOCKED" (PGDEL-03) — UNKNOWN, UNCLASSIFIED
	 * and the WILLINIT-without-rule row are all mutation=0. */
	UT_ASSERT_EQ((int)cluster_page_class_recovery_action(CLUSTER_PAGE_CLASS_UNKNOWN),
				 (int)CLUSTER_PAGE_ACTION_BLOCKED);
	UT_ASSERT_EQ((int)cluster_page_class_recovery_action(CLUSTER_PAGE_CLASS_UNCLASSIFIED),
				 (int)CLUSTER_PAGE_ACTION_BLOCKED);
	UT_ASSERT_EQ((int)cluster_page_class_recovery_action(CLUSTER_PAGE_CLASS_WILLINIT),
				 (int)CLUSTER_PAGE_ACTION_BLOCKED);
}

UT_TEST(test_state_machine_adjacent_advance)
{
	ClusterPageRecoveryState st = CLUSTER_PAGE_STATE_UNCLASSIFIED;

	/* The full §3.5 chain, one adjacent step at a time. */
	UT_ASSERT(cluster_page_state_advance(&st, CLUSTER_PAGE_STATE_CLASSIFIED));
	UT_ASSERT_EQ((int)st, (int)CLUSTER_PAGE_STATE_CLASSIFIED);
	UT_ASSERT(cluster_page_state_advance(&st, CLUSTER_PAGE_STATE_SOURCE_PROVEN));
	UT_ASSERT(cluster_page_state_advance(&st, CLUSTER_PAGE_STATE_CONTRIBUTORS_CLOSED));
	UT_ASSERT(cluster_page_state_advance(&st, CLUSTER_PAGE_STATE_VERSION_CHAIN_VERIFIED));
	UT_ASSERT(cluster_page_state_advance(&st, CLUSTER_PAGE_STATE_AUTHORITY_REVALIDATED));
	UT_ASSERT(cluster_page_state_advance(&st, CLUSTER_PAGE_STATE_MUTATED_IN_MEMORY));
	UT_ASSERT(cluster_page_state_advance(&st, CLUSTER_PAGE_STATE_WAL_BEFORE_DATA_SATISFIED));
	UT_ASSERT(cluster_page_state_advance(&st, CLUSTER_PAGE_STATE_PAGE_WRITE_DURABLE));
	UT_ASSERT(cluster_page_state_advance(&st, CLUSTER_PAGE_STATE_POST_READ_VERIFIED));
	UT_ASSERT(cluster_page_state_advance(&st, CLUSTER_PAGE_STATE_RESOURCE_RELEASED));
	/* Terminal: no further advance. */
	UT_ASSERT(!cluster_page_state_advance(&st, CLUSTER_PAGE_STATE_RESOURCE_RELEASED + 1));
	UT_ASSERT_EQ((int)st, (int)CLUSTER_PAGE_STATE_RESOURCE_RELEASED);
}

UT_TEST(test_state_machine_rejects_jumps_and_repeats)
{
	ClusterPageRecoveryState st = CLUSTER_PAGE_STATE_UNCLASSIFIED;

	/* A jump skips a proof step: rejected, state unchanged. */
	UT_ASSERT(!cluster_page_state_advance(&st, CLUSTER_PAGE_STATE_SOURCE_PROVEN));
	UT_ASSERT_EQ((int)st, (int)CLUSTER_PAGE_STATE_UNCLASSIFIED);
	/* A repeat of the same step is not an advance. */
	UT_ASSERT(!cluster_page_state_advance(&st, CLUSTER_PAGE_STATE_UNCLASSIFIED));
	/* NULL state. */
	UT_ASSERT(!cluster_page_state_advance(NULL, CLUSTER_PAGE_STATE_CLASSIFIED));
	/* Advancing from a valid state to itself is rejected too. */
	UT_ASSERT(cluster_page_state_advance(&st, CLUSTER_PAGE_STATE_CLASSIFIED));
	UT_ASSERT(!cluster_page_state_advance(&st, CLUSTER_PAGE_STATE_CLASSIFIED));
	UT_ASSERT_EQ((int)st, (int)CLUSTER_PAGE_STATE_CLASSIFIED);
}

UT_TEST(test_dispatcher_verdict_combination)
{
	/* APPLY / SKIP come straight from the §3.2 gate. */
	UT_ASSERT_EQ(
		(int)cluster_page_dispatcher_verdict(CLUSTER_PAGE_CLASS_NORMAL, CLUSTER_PAGE_APPLY_APPLY),
		(int)CLUSTER_PAGE_OUTCOME_APPLY);
	UT_ASSERT_EQ(
		(int)cluster_page_dispatcher_verdict(CLUSTER_PAGE_CLASS_NORMAL, CLUSTER_PAGE_APPLY_SKIP),
		(int)CLUSTER_PAGE_OUTCOME_SKIP);
	/* Any blocked verdict from the class layer is BLOCKED_CLASS (the
	 * finer subdivisions belong to the PGDEL-04..06 proof owners). */
	UT_ASSERT_EQ(
		(int)cluster_page_dispatcher_verdict(CLUSTER_PAGE_CLASS_NORMAL, CLUSTER_PAGE_APPLY_BLOCKED),
		(int)CLUSTER_PAGE_OUTCOME_BLOCKED_CLASS);
	UT_ASSERT_EQ((int)cluster_page_dispatcher_verdict(CLUSTER_PAGE_CLASS_UNKNOWN,
													  CLUSTER_PAGE_APPLY_BLOCKED),
				 (int)CLUSTER_PAGE_OUTCOME_BLOCKED_CLASS);
}

int
main(void)
{
	UT_PLAN(5);

	UT_RUN(test_action_table_closed_rows);
	UT_RUN(test_action_unknown_default_blocked);
	UT_RUN(test_state_machine_adjacent_advance);
	UT_RUN(test_state_machine_rejects_jumps_and_repeats);
	UT_RUN(test_dispatcher_verdict_combination);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
