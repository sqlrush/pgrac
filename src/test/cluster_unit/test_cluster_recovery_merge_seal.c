/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_merge_seal.c
 *	  Cold readonly preflight of the merge module: foreign writer
 *	  generations that crashed without anyone sealing their input are
 *	  sealed by the founder (OPEN -> RECOVERY_REQUIRED, then the validated
 *	  tail), after every external admission and before the plan is
 *	  sealed; every refusal leaves no plan.
 *
 *	  The product source is compiled into this test.  Control roots, the
 *	  external fence, the recovery plan and the stream checks are fixtures
 *	  (test_cluster_recovery_merge_seal_boundary.h); nothing is written.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_recovery_merge_seal.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <setjmp.h>
#include <stdarg.h>
#include <stdlib.h>

/* Every ereport of the module lands in the fixture; ERROR and above jump
 * back to the test (see the boundary). */
static void test_ereport_begin(int elevel);
static void test_ereport_end(void);

#undef ereport
#define ereport(elevel, ...)                                                                       \
	do {                                                                                           \
		test_ereport_begin(elevel);                                                                \
		(void)(__VA_ARGS__);                                                                       \
		test_ereport_end();                                                                        \
	} while (0)

#include "../../backend/cluster/cluster_recovery_merge.c"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

#include "test_cluster_recovery_merge_seal_boundary.h"

#define REQUIRED_FLAGS (SEAL_FLAGS | TAIL_FLAGS)
#define OWN_REDO 0x900

static void
fixture(void)
{
	memset(&census, 0, sizeof(census));
	census.generated = true;
	census.own_thread = 1;
	census.n_crashed_candidate = 3;
	census.candidate_bitmap[0] = 14; /* threads 2, 3 and 4 crashed */
	census.dbstate_at_startup = DB_IN_PRODUCTION;
	census.local_recovery_needed = true;
	memset(&pool, 0, sizeof(pool));
	pool_present = false;
	memset(roots, 0, sizeof(roots));
	memset(tokens, 0, sizeof(tokens));
	for (uint16 tid = 1; tid <= NODES; tid++) {
		ClusterControlRootIdentity *id = &roots[tid].identity;
		ClusterWalThreadClaim claim;

		id->system_identifier = 9;
		id->storage_uuid[0] = 1;
		id->authority_uuid[6] = 0x40;
		id->authority_uuid[8] = 0x80;
		id->origin_thread_id = tid;
		id->origin_node_id = tid - 1;
		id->thread_claim_created_at = 42;
		id->origin_owner_incarnation = 11;
		id->root_lineage_seq = 3;
		cluster_wal_thread_claim_fill(&claim, tid, tid - 1, 42);
		id->thread_claim_crc32c = claim.crc;
		/* sealed and tail-validated by a recoverer that failed */
		roots[tid].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
		roots[tid].root_flags = REQUIRED_FLAGS;
		roots[tid].checkpoint_tli = roots[tid].tail_tli = 1;
		roots[tid].checkpoint_lower_lsn = redo[tid] = 0x800;
		roots[tid].validated_tail_lsn_exclusive = tail_end[tid] = 0x1000;
		roots[tid].root_publish_seq = 5;
		tokens[tid].origin_thread_id = tid;
		tokens[tid].root_lineage_seq = 3;
		tokens[tid].lifecycle = roots[tid].lifecycle;
		tokens[tid].root_flags = roots[tid].root_flags;
		tokens[tid].file_txn_seq = 77;
		tokens[tid].root_publish_seq = 5;
		open_result[tid] = tail_result[tid] = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		fence_current[tid] = true;
		readback_cleared[tid] = 0;
		recover_on_open[tid] = 0;
		open_calls[tid] = tail_calls[tid] = source_calls[tid] = revalidate_calls[tid] = 0;
	}
	formations_built = formations_destroyed = 0;
	events[0] = '\0';
	cluster_shared_config = true;
	fatal_armed = false;
}

/* Crashed while open: nobody sealed it.  Its checkpoint-advanced extent
 * is shorter than the tail the seal will validate. */
static void
make_open(uint16 tid)
{
	roots[tid].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	roots[tid].root_flags = SEAL_FLAGS | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID;
	roots[tid].validated_tail_lsn_exclusive = 0xc00;
	tokens[tid].lifecycle = roots[tid].lifecycle;
	tokens[tid].root_flags = roots[tid].root_flags;
}

/* Sealed, but its sealer failed before validating the tail. */
static void
make_sealed_without_tail(uint16 tid)
{
	roots[tid].root_flags = SEAL_FLAGS;
	roots[tid].tail_tli = 0;
	roots[tid].validated_tail_lsn_exclusive = 0;
	tokens[tid].root_flags = roots[tid].root_flags;
}

/* Run the preflight; a FATAL is reported (not aborted on) and returns
 * false with *plan left NULL. */
static bool
preflight(ClusterRecoveryFencePlan **plan, ClusterMergeEngage *engage)
{
	*plan = NULL;
	fatal_armed = true;
	if (setjmp(fatal_jump) != 0)
		return false;
	*engage = cluster_recovery_merge_preflight_readonly(1, OWN_REDO, plan);
	fatal_armed = false;
	return true;
}

static void
expect_engaged(ClusterRecoveryFencePlan **plan)
{
	ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;

	if (!preflight(plan, &engage)) {
		printf("# unexpected FATAL: %s (%s)\n", last_message, last_detail);
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT_EQ(engage, CLUSTER_MERGE_ENGAGE);
	UT_ASSERT(*plan != NULL);
}

/* A refusal leaves no plan.  The preflight's own refusals release every
 * member first; the projection's refusal (53RA3) exits the process with
 * them, which releases what the startup process holds. */
static void
expect_refused_by(ClusterRecoveryFencePlan **plan, int sqlstate, const char *detail, bool released)
{
	ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;

	UT_ASSERT(!preflight(plan, &engage));
	UT_ASSERT(*plan == NULL);
	UT_ASSERT_EQ(last_sqlstate, sqlstate);
	if (strstr(last_detail, detail) == NULL) {
		printf("# detail \"%s\" lacks \"%s\"\n", last_detail, detail);
		UT_ASSERT(false);
	}
	if (released)
		UT_ASSERT_EQ(formations_destroyed, formations_built);
}

static void
expect_refused(ClusterRecoveryFencePlan **plan, int sqlstate, const char *detail)
{
	expect_refused_by(plan, sqlstate, detail, true);
}

static int
event_at(const char *what, bool last)
{
	const char *at = NULL;
	const char *p = events;

	while ((p = strstr(p, what)) != NULL) {
		at = p;
		if (!last)
			break;
		p++;
	}
	return at == NULL ? -1 : (int)(at - events);
}

static bool
origin_root(const ClusterRecoveryFencePlan *plan, uint16 tid, ClusterControlRootSnapshot *root,
			ClusterControlRootReadToken *token)
{
	for (uint16 i = 0; i < cluster_recovery_merge_fence_plan_origin_count(plan); i++) {
		uint16 origin;

		if (cluster_recovery_merge_fence_plan_origin(plan, i, &origin, root, token)
			&& origin == tid)
			return true;
	}
	return false;
}

/* The plan holds the root each origin has now. */
static void
expect_origin_sealed(const ClusterRecoveryFencePlan *plan, uint16 tid)
{
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;

	UT_ASSERT(origin_root(plan, tid, &root, &token));
	UT_ASSERT_EQ(root.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED);
	UT_ASSERT_EQ(root.root_flags & REQUIRED_FLAGS, REQUIRED_FLAGS);
	UT_ASSERT_EQ(memcmp(&root, &roots[tid], sizeof(root)), 0);
	UT_ASSERT_EQ(memcmp(&token, &tokens[tid], sizeof(token)), 0);
	UT_ASSERT_EQ(plan->start_lsn[tid], redo[tid]);
}

/* As if the serial set were held: the commit re-projects and must agree. */
static bool
commit_as_held(ClusterRecoveryFencePlan *plan)
{
	bool committed;

	plan->serial_held = true;
	plan->serial_guards.count = plan->origin_count;
	plan->retention_pin = (ClusterWalRetentionPin *)&pin_obj;
	fatal_armed = true;
	if (setjmp(fatal_jump) != 0)
		committed = false;
	else
		committed = cluster_recovery_merge_commit_plan_nowait(plan);
	fatal_armed = false;
	plan->serial_held = false;
	plan->serial_guards.count = 0;
	plan->retention_pin = NULL;
	return committed;
}

/*
 * After every instance failed, generations that crashed while OPEN are
 * sealed by the founder: input first, then the validated tail, only after
 * every origin's external admission.  The plan then carries the sealed
 * roots and the native redo proven for them, and a later commit agrees.
 */
UT_TEST(test_open_generations_are_sealed_after_admission)
{
	ClusterRecoveryFencePlan *plan;

	fixture();
	make_open(2);
	make_open(3);
	expect_engaged(&plan);
	if (plan == NULL)
		return;
	UT_ASSERT_EQ(open_calls[2], 1);
	UT_ASSERT_EQ(tail_calls[2], 1);
	UT_ASSERT_EQ(open_calls[3], 1);
	UT_ASSERT_EQ(tail_calls[3], 1);
	UT_ASSERT_EQ(open_calls[4] + tail_calls[4], 0);
	UT_ASSERT(event_at("admit:", true) >= 0);
	UT_ASSERT(event_at("admit:", true) < event_at("open:", false));
	UT_ASSERT(strstr(events, "open:2;tail:2;read:2;open:3;tail:3;read:3;") != NULL);
	UT_ASSERT_EQ(plan->origin_count, 3);
	expect_origin_sealed(plan, 2);
	expect_origin_sealed(plan, 3);
	expect_origin_sealed(plan, 4);
	UT_ASSERT_EQ(plan->start_lsn[1], OWN_REDO);
	UT_ASSERT(commit_as_held(plan));
	cluster_recovery_merge_fence_plan_destroy(&plan);
	UT_ASSERT(plan == NULL);
}

/*
 * Restart after a failed cold recovery: one generation is fully sealed,
 * one lost its sealer before the tail, one was never sealed.  Each gets
 * exactly what it lacks.  The workers' stream verdict from before the seal
 * (no tail yet: unreadable) is not reused.
 */
UT_TEST(test_partial_seal_resumes)
{
	ClusterRecoveryFencePlan *plan;

	fixture();
	make_sealed_without_tail(3);
	make_open(4);
	pool_present = true;
	pool.stream_verdict[2] = CLUSTER_RECOVERY_STREAM_OK;
	pool.stream_verdict[3] = CLUSTER_RECOVERY_STREAM_UNREADABLE;
	pool.stream_verdict[4] = CLUSTER_RECOVERY_STREAM_OK;
	expect_engaged(&plan);
	if (plan == NULL)
		return;
	UT_ASSERT_EQ(open_calls[2] + tail_calls[2], 0);
	UT_ASSERT_EQ(open_calls[3], 0);
	UT_ASSERT_EQ(tail_calls[3], 1);
	UT_ASSERT_EQ(open_calls[4], 1);
	UT_ASSERT_EQ(tail_calls[4], 1);
	UT_ASSERT(revalidate_calls[3] >= 1);
	expect_origin_sealed(plan, 2);
	expect_origin_sealed(plan, 3);
	expect_origin_sealed(plan, 4);
	UT_ASSERT(commit_as_held(plan));
	cluster_recovery_merge_fence_plan_destroy(&plan);
}

/* An admission that is no longer current refuses before any root changes. */
UT_TEST(test_stale_admission_refuses_before_any_seal)
{
	ClusterRecoveryFencePlan *plan;

	fixture();
	make_open(2);
	make_open(4);
	fence_current[4] = false;
	expect_refused(&plan, ERRCODE_CLUSTER_EXTERNAL_FENCE_UNAVAILABLE, "stage seal");
	UT_ASSERT_EQ(open_calls[2] + open_calls[4] + tail_calls[2] + tail_calls[4], 0);
	UT_ASSERT_EQ(formations_built, 3);
}

/* A failed transition refuses with no plan; later origins are not sealed. */
UT_TEST(test_seal_failure_refuses)
{
	ClusterRecoveryFencePlan *plan;

	fixture();
	make_open(2);
	make_open(3);
	make_open(4);
	open_result[3] = CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	expect_refused(&plan, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED, "input seal");
	UT_ASSERT_EQ(tail_calls[2], 1);
	UT_ASSERT_EQ(tail_calls[3], 0);
	UT_ASSERT_EQ(open_calls[4] + tail_calls[4], 0);

	fixture();
	make_open(2);
	tail_result[2] = CLUSTER_CONTROL_ROOT_IO_ERROR;
	expect_refused(&plan, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED, "tail seal");
	UT_ASSERT_EQ(open_calls[2], 1);
	UT_ASSERT_EQ(roots[2].lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED);
}

/* Both transitions reported success, but the root read back lacks the
 * recovered bound; and another founder recovered a generation while this
 * one sealed, so the merge set changed under the plan -- refused rather
 * than shrunk. */
UT_TEST(test_seal_outcome_is_checked)
{
	ClusterRecoveryFencePlan *plan;

	fixture();
	make_open(2);
	readback_cleared[2] = CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
	expect_refused(&plan, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED, "sealed read-back");

	fixture();
	make_open(2);
	recover_on_open[2] = 3;
	expect_refused(&plan, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED, "set of crashed generations");
	UT_ASSERT_EQ(tail_calls[2], 1);
}

/* An OPEN generation without a checkpoint cannot be sealed: refused
 * before any external admission is requested. */
UT_TEST(test_unsealable_open_refused_before_admission)
{
	ClusterRecoveryFencePlan *plan;

	fixture();
	make_open(3);
	roots[3].root_flags &= ~CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID;
	tokens[3].root_flags = roots[3].root_flags;
	expect_refused(&plan, ERRCODE_CLUSTER_EXTERNAL_FENCE_UNAVAILABLE, "root-lifecycle");
	UT_ASSERT_EQ(formations_built, 0);
	UT_ASSERT_EQ(open_calls[3], 0);

	/* A recovery-required root with a tail but no recovered bound is no
	 * shape a publisher produces. */
	fixture();
	roots[2].root_flags &= ~CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
	tokens[2].root_flags = roots[2].root_flags;
	expect_refused(&plan, ERRCODE_CLUSTER_EXTERNAL_FENCE_UNAVAILABLE, "root-lifecycle");
	UT_ASSERT_EQ(formations_built, 0);
}

/* A generation another founder recovered meanwhile leaves the plan. */
UT_TEST(test_recovered_candidate_leaves_the_plan)
{
	ClusterRecoveryFencePlan *plan;

	fixture();
	roots[3].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	tokens[3].lifecycle = roots[3].lifecycle;
	make_open(4);
	expect_engaged(&plan);
	if (plan == NULL)
		return;
	UT_ASSERT_EQ(plan->origin_count, 2);
	UT_ASSERT_EQ(plan->replay_thread_bitmap[0], 11);
	UT_ASSERT_EQ(source_calls[3], 0);
	UT_ASSERT_EQ(open_calls[3] + tail_calls[3], 0);
	expect_origin_sealed(plan, 4);
	UT_ASSERT(commit_as_held(plan));
	cluster_recovery_merge_fence_plan_destroy(&plan);
}

/*
 * The projection defers what needs a seal: an unsealed generation keeps
 * its place without a start and without a source proof; one with no
 * validated tail is not stream-checked yet.  A peer seen alive still
 * refuses, and the unshared profile is unchanged.
 */
UT_TEST(test_projection_defers_unsealed_inputs)
{
	uint64 bitmap[2] = { 0, 0 };
	XLogRecPtr starts[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };

	fixture();
	make_open(2);
	make_sealed_without_tail(3);
	fatal_armed = true;
	if (setjmp(fatal_jump) != 0) {
		printf("# unexpected FATAL: %s\n", last_detail);
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT_EQ(cluster_recovery_merge_project_readonly(1, OWN_REDO, bitmap, starts),
				 CLUSTER_MERGE_ENGAGE);
	fatal_armed = false;
	UT_ASSERT_EQ(bitmap[0], 15);
	UT_ASSERT_EQ(starts[2], InvalidXLogRecPtr);
	UT_ASSERT_EQ(starts[3], InvalidXLogRecPtr);
	UT_ASSERT_EQ(starts[4], 0x800);
	UT_ASSERT_EQ(source_calls[2] + source_calls[3], 0);
	UT_ASSERT_EQ(revalidate_calls[2], 1);
	UT_ASSERT_EQ(revalidate_calls[3], 0);

	fixture();
	make_open(2);
	pool_present = true;
	pool.stream_verdict[4] = CLUSTER_RECOVERY_STREAM_SKIPPED;
	fatal_armed = true;
	if (setjmp(fatal_jump) == 0) {
		(void)cluster_recovery_merge_project_readonly(1, OWN_REDO, bitmap, starts);
		fatal_armed = false;
		UT_ASSERT(false);
	}
	UT_ASSERT(strstr(last_detail, "thread 4 stream SKIPPED") != NULL);

	fixture();
	make_open(2);
	cluster_shared_config = false;
	memset(starts, 0, sizeof(starts));
	fatal_armed = true;
	if (setjmp(fatal_jump) != 0) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT_EQ(cluster_recovery_merge_project_readonly(1, OWN_REDO, bitmap, starts),
				 CLUSTER_MERGE_ENGAGE);
	fatal_armed = false;
	UT_ASSERT_EQ(starts[2], 0x800);
	UT_ASSERT_EQ(source_calls[2], 0);
}

/* What the seal produced is proven like any sealed input: a native redo
 * past the validated tail refuses after the seal, with no plan. */
UT_TEST(test_sealed_input_is_proven)
{
	ClusterRecoveryFencePlan *plan;

	fixture();
	make_open(2);
	redo[2] = 0x1800;
	expect_refused_by(&plan, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED, "native checkpoint", false);
	UT_ASSERT_EQ(open_calls[2], 1);
	UT_ASSERT_EQ(tail_calls[2], 1);

	/* Flags claiming a tail without one prove no start: nothing to seal,
	 * still refused. */
	fixture();
	roots[2].tail_tli = 0;
	roots[2].validated_tail_lsn_exclusive = 0;
	expect_refused(&plan, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED, "prove every redo start");
	UT_ASSERT_EQ(open_calls[2] + tail_calls[2] + source_calls[2], 0);
}

/* Unsafe-FPW history still refuses before anything is sealed. */
UT_TEST(test_unsafe_history_refused_before_seal)
{
	ClusterRecoveryFencePlan *plan;

	fixture();
	make_open(2);
	roots[2].root_flags |= CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF;
	tokens[2].root_flags = roots[2].root_flags;
	expect_refused(&plan, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED, "full_page_writes=off");
	UT_ASSERT_EQ(formations_built, 0);
	UT_ASSERT_EQ(open_calls[2], 0);
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_open_generations_are_sealed_after_admission);
	UT_RUN(test_partial_seal_resumes);
	UT_RUN(test_stale_admission_refuses_before_any_seal);
	UT_RUN(test_seal_failure_refuses);
	UT_RUN(test_seal_outcome_is_checked);
	UT_RUN(test_unsealable_open_refused_before_admission);
	UT_RUN(test_recovered_candidate_leaves_the_plan);
	UT_RUN(test_projection_defers_unsealed_inputs);
	UT_RUN(test_sealed_input_is_proven);
	UT_RUN(test_unsafe_history_refused_before_seal);
	UT_DONE();
	return ut_failed_count != 0;
}
