/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_recovery_space.c
 *	  Typed cold-crash replay plan with SPACE effects: relation extension,
 *	  CREATE, TRUNCATE (VACUUM tail truncation) and DROP across retained
 *	  writer generations.
 *
 *	  Pure inputs only: decoded-record identities, PageVersion components,
 *	  SPACE effects and DATA observations are fixtures
 *	  (test_cluster_cold_recovery_space_fixture.h).  No WAL is read and no
 *	  page or SPACE fork is written.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cold_recovery_space.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "cluster/cluster_cold_recovery.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

#include "test_cluster_cold_recovery_space_fixture.h"

/*
 * Relation extension moves only the SPACE reservation: the page records are
 * planned as before, and the reservation updates after the native redo start
 * are collected for the SPACE owner, never scheduled.
 */
UT_TEST(test_space_advance_collected_not_scheduled)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1100, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdSpaceInputV1 input;
	RelFileLocator locator;
	uint32 count = 0;
	char mutable_payload[4] = { 'A', 'D', 'V', '1' };
	ClusterColdSpaceOpV1 adv = space_op(CLUSTER_COLD_SPACE_ADVANCE, REL_R, 0, INC_OLD, 0);

	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x1100, 1, adv), CLUSTER_COLD_OK); /* history */
	UT_ASSERT_EQ(feed_page(plan, 0, 0x1100, 0x1200, 2, delta(INC_OLD, REL_R, 0, 1, 2)),
				 CLUSTER_COLD_OK);
	adv.payload = mutable_payload;
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1200, 0x2000, 3, adv), CLUSTER_COLD_OK);
	mutable_payload[0] = 'X'; /* the plan keeps its own copy */
	UT_ASSERT_EQ(feed_space(plan, 1, 0x1000, 0x1100, 4, adv), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x1100, 0x2000), CLUSTER_COLD_OK);
	observe_set(&table, REL_R, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_OLD, 1), VERIFIED);

	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 1);
	UT_ASSERT_EQ(cluster_cold_plan_space_relation_count_v1(plan), 1);
	UT_ASSERT(cluster_cold_plan_space_relation_v1(plan, 0, &locator, &count));
	UT_ASSERT_EQ(locator.relNumber, REL_R);
	UT_ASSERT_EQ(count, 2);
	UT_ASSERT(cluster_cold_plan_space_input_v1(plan, 0, 0, &input));
	UT_ASSERT_EQ(input.kind, CLUSTER_COLD_SPACE_ADVANCE);
	UT_ASSERT_EQ(input.participant, 0);
	UT_ASSERT_EQ(input.read_rec_ptr, 0x1200);
	UT_ASSERT_EQ(input.payload_length, 4);
	UT_ASSERT(memcmp(input.payload, "ADV1", 4) == 0);
	UT_ASSERT(cluster_cold_plan_space_input_v1(plan, 0, 1, &input));
	UT_ASSERT_EQ(input.participant, 1);
	UT_ASSERT(!cluster_cold_plan_space_input_v1(plan, 0, 2, &input));
	/* the SPACE owner checked the relation once, with the same inputs */
	UT_ASSERT_EQ(check_log.calls, 1);
	UT_ASSERT_EQ(check_log.last_count, 2);
	UT_ASSERT_EQ(check_log.last_locator.relNumber, REL_R);
	UT_ASSERT_EQ(check_log.last_inputs[0].read_rec_ptr, 0x1200);
	UT_ASSERT_EQ(check_log.last_inputs[1].participant, 1);
	UT_ASSERT_EQ(cluster_cold_plan_replay_record_count_v1(plan, 0), 2);
	cluster_cold_plan_destroy_v1(&plan);
}

/* A CREATE after a native redo start is replayed by the SPACE owner before
 * any page of the new relation, whatever the SCN order. */
UT_TEST(test_space_create_orders_new_relation_pages)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdStepV1 step;

	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x1100, 10,
							space_op(CLUSTER_COLD_SPACE_CREATE, REL_S, 0, INC_C, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1100, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1000, 0x1100, 1, init_new(INC_C, REL_S, 0, 5)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x1100, 0x2000), CLUSTER_COLD_OK);
	observe_set(&table, REL_S, 0, CLUSTER_COLD_DATA_ABSENT, ver(0, 0), VERIFIED);

	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 2);
	UT_ASSERT(step_at(plan, 0, 0, 0x1000, &step));
	UT_ASSERT_EQ(step.step_kind, CLUSTER_COLD_STEP_SPACE);
	UT_ASSERT_EQ(step.space_kind, CLUSTER_COLD_SPACE_CREATE);
	UT_ASSERT_EQ(step.space_relation, 0);
	UT_ASSERT_EQ(step.space_input, 0);
	UT_ASSERT(!step.all_skip);
	UT_ASSERT(step_at(plan, 1, 1, 0x1000, &step));
	UT_ASSERT_EQ(step.step_kind, CLUSTER_COLD_STEP_PAGE);
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_INIT);
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_INVALID);
	cluster_cold_plan_destroy_v1(&plan);
}

/*
 * VACUUM tail truncation (truncate_fixture): every change before it is
 * durable, blocks below nblocks continue in the new incarnation, blocks at or
 * past it are retired and re-extended from ABSENT.
 */
UT_TEST(test_space_truncate_boundary_rebase_and_retire)
{
	ObserveTable table;
	ClusterColdDiagV1 diag;
	ClusterColdStepV1 step;
	ClusterColdPlanV1 *plan;
	int pass;

	/* Identity durable (new) or still the old one: the same page state. */
	for (pass = 0; pass < 2; pass++) {
		plan = truncate_fixture(&table, ver(pass == 0 ? INC_NEW : INC_OLD, 2),
								CLUSTER_COLD_DATA_PRESENT, ver(INC_OLD, 6));
		UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 5);
		UT_ASSERT(step_at(plan, 0, 0, 0x1000, &step));
		UT_ASSERT(step.all_skip); /* durable before the truncation */
		UT_ASSERT(step_at(plan, 1, 0, 0x1100, &step));
		UT_ASSERT(step.all_skip); /* retired by the truncation */
		UT_ASSERT(step_at(plan, 2, 0, 0x1200, &step));
		UT_ASSERT_EQ(step.step_kind, CLUSTER_COLD_STEP_SPACE);
		UT_ASSERT_EQ(step.space_kind, CLUSTER_COLD_SPACE_TRUNCATE);
		UT_ASSERT(step_at(plan, 3, 1, 0x1000, &step));
		UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
		UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_PRESENT);
		UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_NEW, 2));
		UT_ASSERT(step_at(plan, 4, 1, 0x1100, &step));
		UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_INIT);
		/* the stale retired content is replaced, not trusted */
		UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_INVALID);
		cluster_cold_plan_destroy_v1(&plan);
	}

	/* A shrink that reached disk leaves the retired block absent: the init
	 * replaces it (or the zero page a later block's extension leaves). */
	plan = truncate_fixture(&table, ver(INC_NEW, 2), CLUSTER_COLD_DATA_ABSENT, ver(0, 0));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT(step_at(plan, 4, 1, 0x1100, &step));
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_INVALID);
	cluster_cold_plan_destroy_v1(&plan);

	/* Old content under the new identity, or a zeroed block under the old
	 * one, at a retired block: replaced by the new page's init. */
	plan = truncate_fixture(&table, ver(INC_NEW, 2), CLUSTER_COLD_DATA_PRESENT, ver(INC_NEW, 6));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT(step_at(plan, 4, 1, 0x1100, &step));
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_INVALID);
	cluster_cold_plan_destroy_v1(&plan);
	plan
		= truncate_fixture(&table, ver(INC_NEW, 2), CLUSTER_COLD_DATA_UNFORMATTED, ver(INC_OLD, 0));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT(step_at(plan, 4, 1, 0x1100, &step));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_INIT);
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_INVALID);
	cluster_cold_plan_destroy_v1(&plan);

	/* DATA behind the object checkpoint cannot be: refused. */
	plan = truncate_fixture(&table, ver(INC_OLD, 1), CLUSTER_COLD_DATA_ABSENT, ver(0, 0));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_HISTORY_GAP);
	cluster_cold_plan_destroy_v1(&plan);
}

/* A page written after the truncation while the SPACE identity on disk is
 * still the old one: its token alone places it. */
UT_TEST(test_space_stale_identity_newer_page)
{
	ObserveTable table;
	ClusterColdDiagV1 diag;
	ClusterColdStepV1 step;
	ClusterColdPlanV1 *plan
		= truncate_fixture(&table, ver(INC_OLD, 3), CLUSTER_COLD_DATA_ABSENT, ver(0, 0));

	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT(step_at(plan, 3, 1, 0x1000, &step));
	UT_ASSERT(step.all_skip);
	cluster_cold_plan_destroy_v1(&plan);

	/* An incarnation outside the relation's lineage is not this page. */
	plan = truncate_fixture(&table, ver(INC_C, 3), CLUSTER_COLD_DATA_ABSENT, ver(0, 0));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_INCARNATION_MISMATCH);
	cluster_cold_plan_destroy_v1(&plan);
}

/* A truncation carries no block at or past its size: a version there that
 * claims to continue an older incarnation is not this page. */
UT_TEST(test_space_truncate_does_not_carry_past_size)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;

	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x1100, 1,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_OLD, INC_NEW, 1)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1100, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1000, 0x2000, 2, delta(INC_NEW, REL_R, 1, 6, 7)),
				 CLUSTER_COLD_OK);
	observe_set(&table, REL_R, 1, CLUSTER_COLD_DATA_PRESENT, ver(INC_OLD, 6), VERIFIED);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_INCARNATION_MISMATCH);
	cluster_cold_plan_destroy_v1(&plan);

	/* ...and a block it carried does not start new over unrelated content. */
	plan = make_plan(parts, 2);
	memset(&table, 0, sizeof(table));
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x1100, 1,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_OLD, INC_NEW, 2)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1100, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1000, 0x2000, 2, init_new(INC_NEW, REL_R, 1, 7)),
				 CLUSTER_COLD_OK);
	observe_set(&table, REL_R, 1, CLUSTER_COLD_DATA_PRESENT, ver(INC_OLD, 6), VERIFIED);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_ANCESTOR_MISSING);
	cluster_cold_plan_destroy_v1(&plan);
}

/*
 * Two truncations, the first two in history.  The block is carried through
 * an incarnation without changes to it, a header may name that one, and
 * nothing waits for a durable truncation.
 */
UT_TEST(test_space_carried_through_two_truncations)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1300, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdStepV1 step;

	UT_ASSERT_EQ(feed_page(plan, 0, 0x1000, 0x1100, 1, delta(INC_OLD, REL_R, 0, 1, 2)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1100, 0x1200, 2,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_OLD, INC_MID, 2)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1200, 0x1300, 3,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_MID, INC_NEW, 1)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1300, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1000, 0x2000, 4, delta(INC_NEW, REL_R, 0, 2, 3)),
				 CLUSTER_COLD_OK);
	observe_set(&table, REL_R, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_MID, 2), VERIFIED);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 1);
	UT_ASSERT(step_at(plan, 0, 1, 0x1000, &step));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_NEW, 2));
	UT_ASSERT_EQ(cluster_cold_plan_space_relation_count_v1(plan), 0);
	cluster_cold_plan_destroy_v1(&plan);
}

/*
 * Outside the main fork the truncated size is not in the effect: a block
 * that continues keeps its version, and a block that starts new in the
 * new incarnation was removed.
 */
UT_TEST(test_space_truncate_visibility_map_fork)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdStepV1 step;

	/* the map page cleared past the new size, logged as a full image */
	UT_ASSERT_EQ(
		feed_page(plan, 0, 0x1000, 0x1100, 1,
				  in_fork(full_image(delta(INC_OLD, REL_R, 0, 1, 2)), VISIBILITYMAP_FORKNUM)),
		CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 0, 0x1100, 0x1200, 2,
						   in_fork(delta(INC_OLD, REL_R, 1, 5, 6), VISIBILITYMAP_FORKNUM)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1200, 0x1300, 3,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_OLD, INC_NEW, 1)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1300, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1000, 0x1100, 4,
						   in_fork(delta(INC_NEW, REL_R, 0, 2, 3), VISIBILITYMAP_FORKNUM)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1100, 0x2000, 5,
						   in_fork(init_new(INC_NEW, REL_R, 1, 7), VISIBILITYMAP_FORKNUM)),
				 CLUSTER_COLD_OK);
	observe_fork(&table, REL_R, VISIBILITYMAP_FORKNUM, 0, CLUSTER_COLD_DATA_PRESENT,
				 ver(INC_OLD, 2), VERIFIED);
	observe_fork(&table, REL_R, VISIBILITYMAP_FORKNUM, 1, CLUSTER_COLD_DATA_PRESENT,
				 ver(INC_OLD, 6), VERIFIED);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 5);
	/* that image is durable: never restored, though it is an anchor */
	UT_ASSERT(step_at(plan, 0, 0, 0x1000, &step));
	UT_ASSERT(step.all_skip);
	UT_ASSERT(step_at(plan, 3, 1, 0x1000, &step));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_NEW, 2));
	UT_ASSERT(step_at(plan, 4, 1, 0x1100, &step));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_INIT);
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_INVALID);
	cluster_cold_plan_destroy_v1(&plan);
}

/* A token reached in two incarnations of the lineage places nothing. */
UT_TEST(test_space_token_repeated_across_incarnations)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;

	UT_ASSERT_EQ(feed_page(plan, 0, 0x1000, 0x1100, 1, delta(INC_OLD, REL_R, 0, 1, 2)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1100, 0x1200, 2,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_OLD, INC_NEW, 1)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 0, 0x1200, 0x1300, 3, delta(INC_NEW, REL_R, 0, 2, 3)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1300, 0x1400, 4,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_NEW, INC_D, 1)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 0, 0x1400, 0x2000, 5, delta(INC_D, REL_R, 0, 3, 2)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x1000, 0x2000), CLUSTER_COLD_OK);
	observe_set(&table, REL_R, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_D, 2), VERIFIED);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_CHAIN_AMBIGUOUS);
	cluster_cold_plan_destroy_v1(&plan);
}

/* Changes to a dropped relation are irrelevant: skipped, its pages not
 * observed, its drop handed to the SPACE owner. */
UT_TEST(test_space_drop_makes_changes_irrelevant)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdStepV1 step;
	ClusterColdSpaceInputV1 input;
	ClusterColdSpaceOpV1 drop = space_op(CLUSTER_COLD_SPACE_DROP, REL_R, INC_OLD, 0, 0);

	UT_ASSERT_EQ(feed_page(plan, 0, 0x1000, 0x1100, 1, delta(INC_OLD, REL_R, 0, 1, 2)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1100, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_any(plan, 1, 0x1000, 0x1100, 5, 0, NULL, 1, &drop), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x1100, 0x2000), CLUSTER_COLD_OK);

	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(table.calls, 0);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 2);
	UT_ASSERT(step_at(plan, 0, 0, 0x1000, &step));
	UT_ASSERT(step.all_skip);
	/* the drop is a step: the SPACE owner installs it before its commit */
	UT_ASSERT(step_at(plan, 1, 1, 0x1000, &step));
	UT_ASSERT_EQ(step.step_kind, CLUSTER_COLD_STEP_SPACE);
	UT_ASSERT_EQ(step.space_kind, CLUSTER_COLD_SPACE_DROP);
	UT_ASSERT_EQ(step.space_count, 1);
	UT_ASSERT_EQ(cluster_cold_plan_space_relation_count_v1(plan), 1);
	UT_ASSERT(cluster_cold_plan_space_input_v1(plan, 0, 0, &input));
	UT_ASSERT_EQ(input.kind, CLUSTER_COLD_SPACE_DROP);
	cluster_cold_plan_destroy_v1(&plan);

	/* The relation file number is reused by a new incarnation: the dropped
	 * one's changes were never written and do not join its pages. */
	plan = make_plan(parts, 2);
	memset(&table, 0, sizeof(table));
	UT_ASSERT_EQ(feed_page(plan, 0, 0x1000, 0x1100, 1, delta(INC_OLD, REL_R, 0, 1, 2)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1100, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_any(plan, 1, 0x1000, 0x1100, 5, 0, NULL, 1, &drop), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 1, 0x1100, 0x1200, 6,
							space_op(CLUSTER_COLD_SPACE_CREATE, REL_R, 0, INC_C, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1200, 0x2000, 7, init_new(INC_C, REL_R, 0, 3)),
				 CLUSTER_COLD_OK);
	observe_set(&table, REL_R, 0, CLUSTER_COLD_DATA_ABSENT, ver(0, 0), VERIFIED);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 4);
	UT_ASSERT(step_at(plan, 3, 1, 0x1200, &step));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_INIT);
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_INVALID);
	cluster_cold_plan_destroy_v1(&plan);
}

/* A relation created after a native redo start may have pages on disk before
 * its SPACE identity: placed by token.  Any other page without an identity is
 * refused. */
UT_TEST(test_space_created_relation_without_identity)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdStepV1 step;

	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x1100, 1,
							space_op(CLUSTER_COLD_SPACE_CREATE, REL_S, 0, INC_C, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1100, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1000, 0x1100, 2, init_new(INC_C, REL_S, 0, 5)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1100, 0x1200, 3, delta(INC_C, REL_S, 0, 5, 6)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1200, 0x2000, 4, delta(INC_OLD, REL_R, 0, 1, 2)),
				 CLUSTER_COLD_OK);
	observe_set(&table, REL_S, 0, CLUSTER_COLD_DATA_PRESENT, ver(0, 5),
				VERIFIED | CLUSTER_COLD_DATA_FLAG_NO_IDENTITY);
	observe_set(&table, REL_R, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_OLD, 1), VERIFIED);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	/* placed on the init's result: that anchor is restored, as always */
	UT_ASSERT(step_at(plan, 1, 1, 0x1000, &step));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_INIT);
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_PRESENT);
	UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_C, 5));
	UT_ASSERT(step_at(plan, 2, 1, 0x1100, &step));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_C, 5));
	cluster_cold_plan_destroy_v1(&plan);

	/* Created and then truncated here: still no identity on disk. */
	plan = make_plan(parts, 2);
	memset(&table, 0, sizeof(table));
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x1100, 1,
							space_op(CLUSTER_COLD_SPACE_CREATE, REL_S, 0, INC_C, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1100, 0x2000, 2,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_S, INC_C, INC_D, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1000, 0x1100, 3, init_new(INC_D, REL_S, 0, 5)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1100, 0x2000, 4, delta(INC_D, REL_S, 0, 5, 6)),
				 CLUSTER_COLD_OK);
	observe_set(&table, REL_S, 0, CLUSTER_COLD_DATA_PRESENT, ver(0, 6),
				VERIFIED | CLUSTER_COLD_DATA_FLAG_NO_IDENTITY);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 4);
	UT_ASSERT(step_at(plan, 3, 1, 0x1100, &step));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_D, 5));
	cluster_cold_plan_destroy_v1(&plan);

	/* Created in history: its identity is on disk, so a page without one
	 * is refused. */
	{
		ClusterColdParticipantV1 late[2]
			= { part(1, 11, 0x1000, 0x1100, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };

		plan = make_plan(late, 2);
	}
	memset(&table, 0, sizeof(table));
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x1100, 1,
							space_op(CLUSTER_COLD_SPACE_CREATE, REL_S, 0, INC_C, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1100, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1000, 0x1100, 2, init_new(INC_C, REL_S, 0, 5)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1100, 0x2000, 3, delta(INC_C, REL_S, 0, 5, 6)),
				 CLUSTER_COLD_OK);
	observe_set(&table, REL_S, 0, CLUSTER_COLD_DATA_PRESENT, ver(0, 5),
				VERIFIED | CLUSTER_COLD_DATA_FLAG_NO_IDENTITY);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_IDENTITY_MISSING);
	cluster_cold_plan_destroy_v1(&plan);

	/* R was not created here: a page without an identity is refused. */
	plan = make_plan(parts, 2);
	memset(&table, 0, sizeof(table));
	UT_ASSERT_EQ(feed_page(plan, 0, 0x1000, 0x2000, 1, delta(INC_OLD, REL_R, 0, 1, 2)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x1000, 0x2000), CLUSTER_COLD_OK);
	observe_set(&table, REL_R, 0, CLUSTER_COLD_DATA_PRESENT, ver(0, 1),
				VERIFIED | CLUSTER_COLD_DATA_FLAG_NO_IDENTITY);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_IDENTITY_MISSING);
	cluster_cold_plan_destroy_v1(&plan);
}

/*
 * An input whose incarnation a structural change in another generation's
 * history already ended is covered by the SPACE pages on disk: it is not
 * handed to the SPACE owner and is not a step.  The owner then sees one
 * chain from the durable state.
 */
UT_TEST(test_space_inputs_covered_by_durable_end)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1300, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdSpaceInputV1 input;
	RelFileLocator locator;
	uint32 count = 0;

	/* thread 1 history: truncate R, drop S; thread 2 replays around them */
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x1100, 3,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_OLD, INC_NEW, 4)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1100, 0x1300, 4,
							space_op(CLUSTER_COLD_SPACE_DROP, REL_S, INC_C, 0, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1300, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 1, 0x1000, 0x1100, 1,
							space_op(CLUSTER_COLD_SPACE_ADVANCE, REL_R, 0, INC_OLD, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 1, 0x1100, 0x1200, 2,
							space_op(CLUSTER_COLD_SPACE_CREATE, REL_S, 0, INC_C, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1200, 0x1300, 2, init_new(INC_C, REL_S, 0, 5)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 1, 0x1300, 0x2000, 5,
							space_op(CLUSTER_COLD_SPACE_ADVANCE, REL_R, 0, INC_NEW, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	/* R: only the advance on the new incarnation; S: nothing left */
	UT_ASSERT_EQ(cluster_cold_plan_space_relation_count_v1(plan), 1);
	UT_ASSERT(cluster_cold_plan_space_relation_v1(plan, 0, &locator, &count));
	UT_ASSERT_EQ(locator.relNumber, REL_R);
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT(cluster_cold_plan_space_input_v1(plan, 0, 0, &input));
	UT_ASSERT_EQ(input.read_rec_ptr, 0x1300);
	UT_ASSERT_EQ(check_log.calls, 1);
	UT_ASSERT_EQ(check_log.last_count, 1);
	/* the dropped relation's create and init are not replayed */
	UT_ASSERT_EQ(table.calls, 0);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 1);
	cluster_cold_plan_destroy_v1(&plan);
}

/* One commit drops several relations: one step, each effect located. */
UT_TEST(test_space_commit_drops_several_relations)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdStepV1 step;
	ClusterColdSpaceInputV1 input;
	RelFileLocator locator;
	uint32 relation = 99;
	uint32 position = 99;
	uint32 count = 0;
	ClusterColdSpaceOpV1 drops[2] = { space_op(CLUSTER_COLD_SPACE_DROP, REL_R, INC_OLD, 0, 0),
									  space_op(CLUSTER_COLD_SPACE_DROP, REL_S, INC_C, 0, 0) };

	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x2000, 1,
							space_op(CLUSTER_COLD_SPACE_ADVANCE, REL_S, 0, INC_C, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_any(plan, 1, 0x1000, 0x2000, 2, 0, NULL, 2, drops), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 1);
	UT_ASSERT(step_at(plan, 0, 1, 0x1000, &step));
	UT_ASSERT_EQ(step.space_kind, CLUSTER_COLD_SPACE_DROP);
	UT_ASSERT_EQ(step.space_count, 2);
	UT_ASSERT(cluster_cold_plan_step_space_v1(plan, 0, 1, &relation, &position));
	UT_ASSERT(cluster_cold_plan_space_relation_v1(plan, relation, &locator, &count));
	UT_ASSERT_EQ(locator.relNumber, REL_S);
	UT_ASSERT_EQ(count, 2);
	UT_ASSERT_EQ(position, 1);
	UT_ASSERT(cluster_cold_plan_space_input_v1(plan, relation, position, &input));
	UT_ASSERT_EQ(input.kind, CLUSTER_COLD_SPACE_DROP);
	UT_ASSERT(cluster_cold_plan_step_space_v1(plan, 0, 0, &relation, &position));
	UT_ASSERT(cluster_cold_plan_space_relation_v1(plan, relation, &locator, &count));
	UT_ASSERT_EQ(locator.relNumber, REL_R);
	UT_ASSERT(!cluster_cold_plan_step_space_v1(plan, 0, 2, &relation, &position));
	cluster_cold_plan_destroy_v1(&plan);
}

/*
 * The SPACE owner's order, not SCN, orders a relation's SPACE steps: the
 * CREATE is installed before the TRUNCATE of the incarnation it made.
 */
UT_TEST(test_space_owner_order_orders_steps)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan_checked(parts, 2, CHECK_CREATE_FIRST);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdStepV1 step;
	ClusterColdSpaceInputV1 input;

	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x2000, 1,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_S, INC_C, INC_D, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 1, 0x1000, 0x2000, 9,
							space_op(CLUSTER_COLD_SPACE_CREATE, REL_S, 0, INC_C, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(check_log.last_inputs[0].kind, CLUSTER_COLD_SPACE_TRUNCATE);
	UT_ASSERT(cluster_cold_plan_space_input_v1(plan, 0, 0, &input));
	UT_ASSERT_EQ(input.kind, CLUSTER_COLD_SPACE_CREATE);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 2);
	UT_ASSERT(step_at(plan, 0, 1, 0x1000, &step));
	UT_ASSERT_EQ(step.space_kind, CLUSTER_COLD_SPACE_CREATE);
	UT_ASSERT_EQ(step.space_input, 0);
	UT_ASSERT(step_at(plan, 1, 0, 0x1000, &step));
	UT_ASSERT_EQ(step.space_kind, CLUSTER_COLD_SPACE_TRUNCATE);
	UT_ASSERT_EQ(step.space_input, 1);
	cluster_cold_plan_destroy_v1(&plan);
}

/* Without the SPACE owner's agreement nothing is planned. */
UT_TEST(test_space_owner_check_required)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan;
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdSpaceOpV1 adv = space_op(CLUSTER_COLD_SPACE_ADVANCE, REL_R, 0, INC_OLD, 0);
	CheckMode modes[3] = { CHECK_REFUSE, CHECK_BAD_ORDER, CHECK_OUT_OF_RANGE };
	int i;

	/* A refusal, or an order that is not a permutation of the inputs. */
	for (i = 0; i < 3; i++) {
		plan = make_plan_checked(parts, 2, modes[i]);
		UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x2000, 1, adv), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(feed_space(plan, 1, 0x1000, 0x2000, 2, adv), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
					 CLUSTER_COLD_SPACE_REFUSED);
		UT_ASSERT(diag.has_record);
		UT_ASSERT_EQ(diag.read_rec_ptr, 0x1000);
		UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 0);
		cluster_cold_plan_destroy_v1(&plan);
	}

	/* No owner at all: refused; a plan without SPACE inputs needs none. */
	chains_reset();
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(parts, 2, BUDGET, &plan), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x2000, 1, adv), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x1000, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_SIDE_OWNER_MISSING);
	UT_ASSERT_EQ(cluster_cold_plan_set_space_check_v1(plan, space_check, &check_log),
				 CLUSTER_COLD_STATE);
	cluster_cold_plan_destroy_v1(&plan);
	chains_reset();
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(parts, 2, BUDGET, &plan), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1000, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x1000, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	cluster_cold_plan_destroy_v1(&plan);
	UT_ASSERT_EQ(cluster_cold_plan_set_space_check_v1(NULL, space_check, NULL),
				 CLUSTER_COLD_INVALID_ARGUMENT);
}

UT_TEST(test_space_effect_validation)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan;
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdComponentV1 c = delta(INC_OLD, REL_R, 0, 1, 2);
	ClusterColdSpaceOpV1 op = space_op(CLUSTER_COLD_SPACE_ADVANCE, REL_R, 0, INC_OLD, 0);

	/* A record carries page components or SPACE effects, not both. */
	plan = make_plan(parts, 2);
	UT_ASSERT_EQ(feed_any(plan, 0, 0x1000, 0x2000, 1, 1, &c, 1, &op),
				 CLUSTER_COLD_COMPONENT_INVALID);
	cluster_cold_plan_destroy_v1(&plan);

	plan = make_plan(parts, 2);
	op.kind = 9;
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x2000, 1, op), CLUSTER_COLD_SPACE_INVALID);
	cluster_cold_plan_destroy_v1(&plan);

	plan = make_plan(parts, 2);
	op = space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, 0, INC_NEW, 1);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x2000, 1, op), CLUSTER_COLD_SPACE_INVALID);
	cluster_cold_plan_destroy_v1(&plan);

	plan = make_plan(parts, 2);
	op = space_op(CLUSTER_COLD_SPACE_ADVANCE, REL_R, 0, INC_OLD, 0);
	op.payload = NULL;
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x2000, 1, op), CLUSTER_COLD_SPACE_INVALID);
	cluster_cold_plan_destroy_v1(&plan);

	/* Two creations of one incarnation are a branch. */
	plan = make_plan(parts, 2);
	op = space_op(CLUSTER_COLD_SPACE_CREATE, REL_S, 0, INC_C, 0);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x2000, 1, op), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 1, 0x1000, 0x2000, 2, op), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_SPACE_INVALID);
	UT_ASSERT(diag.has_record);
	UT_ASSERT_EQ(diag.participant, 1);
	cluster_cold_plan_destroy_v1(&plan);

	/* Also when one of them is in history. */
	parts[0].native_redo = 0x1100;
	plan = make_plan(parts, 2);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x1100, 1, op), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1100, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 1, 0x1000, 0x2000, 2, op), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_SPACE_INVALID);
	cluster_cold_plan_destroy_v1(&plan);
	parts[0].native_redo = 0x1000;

	/* So are two ends of one: a truncation and a drop of the same one. */
	plan = make_plan(parts, 2);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x2000, 1,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_OLD, INC_NEW, 1)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 1, 0x1000, 0x2000, 2,
							space_op(CLUSTER_COLD_SPACE_DROP, REL_R, INC_OLD, 0, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_SPACE_INVALID);
	cluster_cold_plan_destroy_v1(&plan);

	/* A page without an identity names no incarnation. */
	plan = make_plan(parts, 2);
	memset(&table, 0, sizeof(table));
	UT_ASSERT_EQ(feed_page(plan, 0, 0x1000, 0x2000, 1, delta(INC_OLD, REL_R, 0, 1, 2)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x1000, 0x2000), CLUSTER_COLD_OK);
	observe_set(&table, REL_R, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_OLD, 1),
				VERIFIED | CLUSTER_COLD_DATA_FLAG_NO_IDENTITY);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_OBSERVATION_FAILED);
	cluster_cold_plan_destroy_v1(&plan);
}

int
main(void)
{
	UT_RUN(test_space_advance_collected_not_scheduled);
	UT_RUN(test_space_create_orders_new_relation_pages);
	UT_RUN(test_space_truncate_boundary_rebase_and_retire);
	UT_RUN(test_space_stale_identity_newer_page);
	UT_RUN(test_space_truncate_does_not_carry_past_size);
	UT_RUN(test_space_carried_through_two_truncations);
	UT_RUN(test_space_truncate_visibility_map_fork);
	UT_RUN(test_space_token_repeated_across_incarnations);
	UT_RUN(test_space_drop_makes_changes_irrelevant);
	UT_RUN(test_space_commit_drops_several_relations);
	UT_RUN(test_space_inputs_covered_by_durable_end);
	UT_RUN(test_space_owner_order_orders_steps);
	UT_RUN(test_space_owner_check_required);
	UT_RUN(test_space_created_relation_without_identity);
	UT_RUN(test_space_effect_validation);
	UT_DONE();
	return ut_failed_count != 0;
}
