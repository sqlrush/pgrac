/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_recovery_space_refail.c
 *	  Typed cold-crash replay across relation lifecycle changes, failing
 *	  again.  Generated timelines create, extend, truncate and drop
 *	  relations across generations; DATA holds any state a crash can leave
 *	  (SPACE identity behind or ahead of the pages, a truncation's shrink
 *	  durable or not, torn pages).  Pass 2 is simulated with the SPACE
 *	  owner's installs, stopped and planned again at random: every applied
 *	  block must find its expected state under its own incarnation, and every
 *	  relation must end with its final identity, pages and size.
 *
 *	  Pure inputs only: no WAL is read and no page is written.  The SPACE
 *	  owner is a stand-in that checks the inputs it is given against the
 *	  generated timeline and orders them by it.  The world model and
 *	  generator are in test_cluster_cold_recovery_space_gen.h, planning and
 *	  the pass-2 simulation in test_cluster_cold_recovery_space_sim.h.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cold_recovery_space_refail.c
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

#define TRIALS 3000
#define MAX_REFAILS 3

#include "test_cluster_cold_recovery_space_gen.h"
#include "test_cluster_cold_recovery_space_sim.h"

/* Hand-built timelines: records in time order, each of one thread. */
static uint32
hand_event(World *w, uint32 thread)
{
	uint32 e = w->events++;
	Event *ev = &w->event[e];

	ev->thread = thread;
	ev->ordinal = w->thread_records[thread]++;
	ev->scn = e + 1;
	ev->history = ev->ordinal < w->redo_ordinal[thread];
	return e;
}

static uint64
hand_page(World *w, uint32 thread, int r, int b, bool anchor)
{
	uint32 e = hand_event(w, thread);
	Event *ev = &w->event[e];

	ev->kind = EV_PAGE;
	ev->count = 1;
	gen_change(w, e, 0, r, b);
	if (ev->components[0].before_kind == RF_PAGE_STATE_PRESENT)
		ev->components[0].edge_flags
			= anchor ? RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE : 0;
	return ev->components[0].result.mutation_token;
}

static void
hand_space(World *w, uint32 thread, int r, uint8 kind, uint32 nblocks)
{
	gen_space(w, hand_event(w, thread), r, kind, nblocks);
}

/* DATA at every floor (nothing written after the last durable state). */
static void
hand_disk_floor(World *w)
{
	int r;
	int b;

	for (r = 0; r < (int)w->rels; r++) {
		for (b = 0; b < MAX_BLOCKS; b++)
			w->rel[r].disk[b] = w->rel[r].hist[b][w->rel[r].floor[b]];
		normalize(w->rel[r].disk);
		w->rel[r].space_pos = w->rel[r].ident_floor;
	}
}

static void
hand_disk(World *w, int r, int b, uint8 kind, uint64 token)
{
	memset(&w->rel[r].disk[b], 0, sizeof(Phys));
	w->rel[r].disk[b].kind = kind;
	w->rel[r].disk[b].token = token;
}

/* The shrink_forks of the SPACE input that event e is. */
static uint8
input_shrink(ClusterColdPlanV1 *plan, const World *w, uint32 e)
{
	uint32 relation;

	for (relation = 0; relation < cluster_cold_plan_space_relation_count_v1(plan); relation++) {
		RelFileLocator locator;
		uint32 count;
		uint32 i;

		UT_ASSERT(cluster_cold_plan_space_relation_v1(plan, relation, &locator, &count));
		for (i = 0; i < count; i++) {
			ClusterColdSpaceInputV1 in;

			UT_ASSERT(cluster_cold_plan_space_input_v1(plan, relation, i, &in));
			if (find_event(w, w->order[in.participant], in.read_rec_ptr) == (int)e)
				return in.shrink_forks;
		}
	}
	UT_ASSERT(false && "no such SPACE input");
	return 0;
}

/* The step replaying event e. */
static ClusterColdStepV1
event_step(ClusterColdPlanV1 *plan, const World *w, uint32 e)
{
	ClusterColdStepV1 step;
	uint32 i;

	for (i = 0; i < cluster_cold_plan_step_count_v1(plan); i++) {
		UT_ASSERT(cluster_cold_plan_step_v1(plan, i, &step));
		if (find_event(w, w->order[step.participant], step.read_rec_ptr) == (int)e)
			return step;
	}
	memset(&step, 0, sizeof(step));
	UT_ASSERT(false && "no step for the event");
	return step;
}

static void
hand_converges(World *w)
{
	bool finished = false;

	UT_ASSERT_EQ(run_once(w, false, &finished), RUN_OK);
	UT_ASSERT(finished);
	UT_ASSERT(converged(w));
}

#define DATA_FORKS ((uint8)((1 << FSM_FORKNUM) | (1 << VISIBILITYMAP_FORKNUM)))

/*
 * Another generation re-extended the relation after a replayed TRUNCATE
 * and checkpointed: its checkpoint synced the main fork after the shrink.
 * Shrinking it again would remove a page whose changes are never replayed.
 */
UT_TEST(test_space_refail_history_extension_keeps_files)
{
	static World w;
	uint32 sizes[1] = { 2 };
	ClusterColdPlanV1 *plan = NULL;
	uint64 extended;

	rng_state = 1;
	world_begin(&w, true, 2, 1, sizes);
	w.redo_ordinal[1] = 1;
	hand_space(&w, 0, 0, CLUSTER_COLD_SPACE_TRUNCATE, 1);
	extended = hand_page(&w, 1, 0, 1, true);
	hand_page(&w, 0, 0, 0, true);
	world_finish(&w);
	hand_disk_floor(&w);
	hand_disk(&w, 0, 0, CLUSTER_COLD_DATA_PRESENT, w.rel[0].hist[0][0].token);
	hand_disk(&w, 0, 1, CLUSTER_COLD_DATA_PRESENT, extended);
	w.rel[0].space_pos = 0;
	UT_ASSERT_EQ(plan_world(&w, &plan), CLUSTER_COLD_OK);
	/* the main fork's shrink is proven; the maps' are not */
	UT_ASSERT_EQ(input_shrink(plan, &w, 0), DATA_FORKS);
	cluster_cold_plan_destroy_v1(&plan);
	hand_converges(&w);
	UT_ASSERT_EQ(w.rel[0].mem[1].token, extended);
}

/*
 * A later TRUNCATE's preparation synced every fork, so the earlier shrink
 * and the page re-extended between them are durable: only the last
 * TRUNCATE is shrunk again.
 */
UT_TEST(test_space_refail_later_truncate_proves_shrink)
{
	static World w;
	uint32 sizes[1] = { 2 };
	ClusterColdPlanV1 *plan = NULL;
	uint64 between;

	rng_state = 1;
	world_begin(&w, true, 1, 1, sizes);
	hand_space(&w, 0, 0, CLUSTER_COLD_SPACE_TRUNCATE, 1);
	between = hand_page(&w, 0, 0, 1, true);
	hand_space(&w, 0, 0, CLUSTER_COLD_SPACE_TRUNCATE, 2);
	hand_page(&w, 0, 0, 1, false);
	world_finish(&w);
	hand_disk_floor(&w);
	hand_disk(&w, 0, 0, CLUSTER_COLD_DATA_PRESENT, w.rel[0].hist[0][0].token);
	hand_disk(&w, 0, 1, CLUSTER_COLD_DATA_PRESENT, between);
	/* the later preparation synced the SPACE fork too, and the change after
	 * the later TRUNCATE follows its durable identity */
	UT_ASSERT_EQ(w.rel[0].space_pos, 2);
	UT_ASSERT_EQ(plan_world(&w, &plan), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(input_shrink(plan, &w, 0), 0);
	UT_ASSERT_EQ(input_shrink(plan, &w, 2), CLUSTER_COLD_SHRINK_FORKS);
	/* durable before the later TRUNCATE: skipped, never rebuilt */
	UT_ASSERT(event_step(plan, &w, 1).all_skip);
	cluster_cold_plan_destroy_v1(&plan);
	hand_converges(&w);
}

/*
 * Nothing proves the shrink, so pass 2 shrinks again at the TRUNCATE's step:
 * a page past the size that an earlier attempt wrote is not a redo base,
 * and the re-extension's init replaces whatever is there.
 */
UT_TEST(test_space_refail_pending_shrink_trusts_no_page_past_size)
{
	static World w;
	uint32 sizes[1] = { 2 };
	ClusterColdPlanV1 *plan = NULL;
	ClusterColdStepV1 step;
	uint64 init;

	rng_state = 1;
	world_begin(&w, true, 1, 1, sizes);
	hand_space(&w, 0, 0, CLUSTER_COLD_SPACE_TRUNCATE, 1);
	init = hand_page(&w, 0, 0, 1, true);
	hand_page(&w, 0, 0, 1, false);
	world_finish(&w);
	hand_disk_floor(&w);
	hand_disk(&w, 0, 0, CLUSTER_COLD_DATA_PRESENT, w.rel[0].hist[0][0].token);
	hand_disk(&w, 0, 1, CLUSTER_COLD_DATA_PRESENT, init);
	w.rel[0].space_pos = 1;
	UT_ASSERT_EQ(plan_world(&w, &plan), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(input_shrink(plan, &w, 0), CLUSTER_COLD_SHRINK_FORKS);
	step = event_step(plan, &w, 1);
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_INIT);
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_INVALID);
	step = event_step(plan, &w, 2);
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	UT_ASSERT_EQ(step.blocks[0].expected_before.mutation_token, init);
	cluster_cold_plan_destroy_v1(&plan);
	hand_converges(&w);
}

/*
 * A block carried by one TRUNCATE and retired by a later one: its changes
 * before the later one take no part in the new page's chain.
 */
UT_TEST(test_space_refail_retired_after_carry)
{
	static World w;
	uint32 sizes[1] = { 1 };
	ClusterColdPlanV1 *plan = NULL;
	ClusterColdStepV1 step;
	uint64 carried;

	rng_state = 1;
	world_begin(&w, true, 1, 1, sizes);
	hand_page(&w, 0, 0, 0, true);
	hand_space(&w, 0, 0, CLUSTER_COLD_SPACE_TRUNCATE, 1);
	carried = hand_page(&w, 0, 0, 0, false);
	hand_space(&w, 0, 0, CLUSTER_COLD_SPACE_TRUNCATE, 0);
	hand_page(&w, 0, 0, 0, true);
	world_finish(&w);
	hand_disk_floor(&w);
	hand_disk(&w, 0, 0, CLUSTER_COLD_DATA_PRESENT, carried);
	w.rel[0].space_pos = 0;
	UT_ASSERT_EQ(plan_world(&w, &plan), CLUSTER_COLD_OK);
	UT_ASSERT(event_step(plan, &w, 0).all_skip);
	UT_ASSERT(event_step(plan, &w, 2).all_skip);
	step = event_step(plan, &w, 4);
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_INIT);
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_INVALID);
	cluster_cold_plan_destroy_v1(&plan);
	hand_converges(&w);
}

/*
 * History re-extended a block a replayed TRUNCATE retired while the SPACE
 * identity on disk still predates that TRUNCATE (worklog F-D-21).  A
 * truncation now persists its identity before releasing the relation, so
 * this DATA cannot arise; should it, the page's header cannot be told from
 * the retired content and nothing replayable rebuilds it: refused before
 * any change, never trusted.
 */
UT_TEST(test_space_refail_stale_identity_after_retire_refuses)
{
	static World w;
	uint32 sizes[1] = { 2 };
	ClusterColdPlanV1 *plan = NULL;
	uint64 extended;

	rng_state = 1;
	world_begin(&w, true, 2, 1, sizes);
	w.redo_ordinal[1] = 1;
	hand_page(&w, 0, 0, 1, true);
	hand_space(&w, 0, 0, CLUSTER_COLD_SPACE_TRUNCATE, 1);
	extended = hand_page(&w, 1, 0, 1, true);
	hand_page(&w, 0, 0, 1, false);
	world_finish(&w);
	hand_disk_floor(&w);
	hand_disk(&w, 0, 0, CLUSTER_COLD_DATA_PRESENT, w.rel[0].hist[0][0].token);
	hand_disk(&w, 0, 1, CLUSTER_COLD_DATA_PRESENT, extended);
	w.rel[0].space_pos = 0;
	UT_ASSERT_EQ(plan_world(&w, &plan), CLUSTER_COLD_ANCHOR_MISSING);
	cluster_cold_plan_destroy_v1(&plan);

	/* Once the TRUNCATE's identity is on disk the page is placed. */
	w.rel[0].space_pos = 1;
	UT_ASSERT_EQ(plan_world(&w, &plan), CLUSTER_COLD_OK);
	cluster_cold_plan_destroy_v1(&plan);
	hand_converges(&w);
}

/*
 * Property: from any DATA a crash can leave around relation lifecycle
 * changes, replaying a prefix of the schedule (installs included), failing
 * again and planning again always converges to every relation's final
 * identity, pages and size, and no block is applied to a state other than
 * the expected one or under another incarnation.  The SPACE owner is handed
 * exactly the uncovered inputs.  The only accepted refusals are pages no
 * anchor after their last settled change can rebuild.  (A stale SPACE
 * identity over a page re-extended after a replayed truncation cannot
 * arise: the truncation persists its identity before releasing the
 * relation; test_space_refail_stale_identity_after_retire_refuses keeps
 * the plan's answer to such input fail-closed.)
 */
UT_TEST(test_space_refail_converges_from_any_prefix)
{
	uint32 trial;
	uint32 converged_trials = 0;
	uint32 refused_trials = 0;
	uint32 refails = 0;
	uint32 truncating = 0;
	uint32 dropping = 0;
	uint32 creating = 0;

	const char *only = getenv("SPACE_REFAIL_TRIAL");

	uint32 first = only != NULL ? (uint32)atoi(only) : 0;

	for (trial = first; trial < (only != NULL ? first + 1 : TRIALS); trial++) {
		static World w;
		uint32 attempt;
		uint32 e;
		bool finished = false;
		RunOutcome outcome = RUN_OK;

		rng_state = UINT64CONST(0xD1B54A32D192ED03) ^ ((uint64)trial * 7919 + 1);
		generate(&w);
		for (attempt = 0; attempt <= MAX_REFAILS && !finished; attempt++) {
			outcome = run_once(&w, attempt < MAX_REFAILS && rng(4) != 0, &finished);
			if (outcome != RUN_OK)
				break;
			refails += !finished;
		}
		if (only != NULL)
			dump_world(&w);
		if (outcome == RUN_REFUSED) {
			refused_trials++;
			continue;
		}
		if (outcome != RUN_OK || !finished || !converged(&w)) {
			printf("# trial %u did not converge (outcome %d, finished %d)\n", trial, (int)outcome,
				   (int)finished);
		}
		UT_ASSERT_EQ(outcome, RUN_OK);
		UT_ASSERT(finished);
		UT_ASSERT(converged(&w));
		converged_trials++;
		for (e = 0; e < w.events; e++) {
			uint8 kind
				= w.event[e].kind == EV_SPACE && !w.event[e].history ? w.event[e].op.kind : 0;

			truncating += kind == CLUSTER_COLD_SPACE_TRUNCATE;
			dropping += kind == CLUSTER_COLD_SPACE_DROP;
			creating += kind == CLUSTER_COLD_SPACE_CREATE;
		}
	}
	printf("# converged %u, refused (no anchor) %u of %u; "
		   "%u re-failures; replayed truncations %u, drops %u, creations %u; truncation steps "
		   "shrinking again %u, keeping proven files %u\n",
		   converged_trials, refused_trials, TRIALS, refails, truncating, dropping, creating,
		   shrinks_repeated, shrinks_kept);
	if (only != NULL)
		return;
	/* The generator must exercise the interesting paths, not refuse them. */
	UT_ASSERT(converged_trials > TRIALS / 2);
	UT_ASSERT(refails > TRIALS);
	UT_ASSERT(truncating > TRIALS / 2);
	UT_ASSERT(dropping > TRIALS / 20);
	UT_ASSERT(creating > TRIALS / 20);
	UT_ASSERT(shrinks_repeated > TRIALS / 2);
	UT_ASSERT(shrinks_kept > TRIALS / 2);
}

int
main(void)
{
	UT_RUN(test_space_refail_history_extension_keeps_files);
	UT_RUN(test_space_refail_later_truncate_proves_shrink);
	UT_RUN(test_space_refail_pending_shrink_trusts_no_page_past_size);
	UT_RUN(test_space_refail_retired_after_carry);
	UT_RUN(test_space_refail_stale_identity_after_retire_refuses);
	UT_RUN(test_space_refail_converges_from_any_prefix);
	UT_DONE();
	return ut_failed_count != 0;
}
