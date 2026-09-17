/*-------------------------------------------------------------------------
 *
 * test_cluster_page_apply.c
 *    RF-PAGE PGDEL-06 focused unit tests: §7.1 mutation admission, the
 *    §7.2 sequence-step judgements, the §7.3 typed page proof surface,
 *    the §7.6 stable-base STOP and the §7.7 crash-matrix verdicts.
 *
 *    RED mapping (spec §10.1 / §7 / §10.2 PL-03):
 *      - every §7.1 fact is required (PU-28: stale authority before
 *        mutation -> zero mutation);
 *      - sequence steps advance the §3.5 state machine only on their
 *        owning proof; a failed durability/post-read/authority step
 *        stops with the owning outcome and never releases;
 *      - PU-30: post-read wrong version/checksum -> no proof/release;
 *      - PU-29: stale authority before release -> no release;
 *      - PL-03: the mid-write cut ALWAYS reports STABLE_BASE_UNRESOLVED
 *        (RED/STOP — no skip, no mock carrier, no false DONE);
 *      - crash matrix: all seven §7.7 rows map to exactly one outcome;
 *        unknown cuts fail closed.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_page_apply.h"

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

static ClusterPageMutationAdmission
ut_full_admission(void)
{
	ClusterPageMutationAdmission a;

	memset(&a, 0, sizeof(a));
	a.duty_root_ok = true;
	a.recoverer_active_ok = true;
	a.fence_ok = true;
	a.serialization_ok = true;
	a.source_proof_fresh = true;
	a.working_version_exact = true;
	a.retention_covers = true;
	return a;
}

UT_TEST(test_admission_conjunction)
{
	ClusterPageMutationAdmission a = ut_full_admission();

	UT_ASSERT(cluster_page_mutation_admission(&a));
	UT_ASSERT(!cluster_page_mutation_admission(NULL));

	/* Every §7.1 fact is required; a stale authority before mutation
	 * means zero mutation (PU-28). */
	a.duty_root_ok = false;
	UT_ASSERT(!cluster_page_mutation_admission(&a));
	a = ut_full_admission();
	a.recoverer_active_ok = false;
	UT_ASSERT(!cluster_page_mutation_admission(&a));
	a = ut_full_admission();
	a.fence_ok = false;
	UT_ASSERT(!cluster_page_mutation_admission(&a));
	a = ut_full_admission();
	a.serialization_ok = false;
	UT_ASSERT(!cluster_page_mutation_admission(&a));
	a = ut_full_admission();
	a.source_proof_fresh = false;
	UT_ASSERT(!cluster_page_mutation_admission(&a));
	a = ut_full_admission();
	a.working_version_exact = false;
	UT_ASSERT(!cluster_page_mutation_admission(&a));
	a = ut_full_admission();
	a.retention_covers = false;
	UT_ASSERT(!cluster_page_mutation_admission(&a));
}

UT_TEST(test_sequence_steps_advance_on_owning_proof)
{
	ClusterPageSequenceResult r;

	/* A step outside its owning state is rejected (state unchanged). */
	r = cluster_page_apply_step_durability(CLUSTER_PAGE_STATE_MUTATED_IN_MEMORY, true);
	UT_ASSERT_EQ((int)r.state, (int)CLUSTER_PAGE_STATE_MUTATED_IN_MEMORY);
	UT_ASSERT_EQ((int)r.outcome, (int)CLUSTER_PAGE_OUTCOME_BLOCKED_CONTRIBUTOR);

	/* durability gate: fail -> BLOCKED_SOURCE, state unchanged. */
	r = cluster_page_apply_step_durability(CLUSTER_PAGE_STATE_WAL_BEFORE_DATA_SATISFIED, false);
	UT_ASSERT_EQ((int)r.state, (int)CLUSTER_PAGE_STATE_WAL_BEFORE_DATA_SATISFIED);
	UT_ASSERT_EQ((int)r.outcome, (int)CLUSTER_PAGE_OUTCOME_BLOCKED_SOURCE);

	/* durability ok -> PAGE_WRITE_DURABLE. */
	r = cluster_page_apply_step_durability(CLUSTER_PAGE_STATE_WAL_BEFORE_DATA_SATISFIED, true);
	UT_ASSERT_EQ((int)r.state, (int)CLUSTER_PAGE_STATE_PAGE_WRITE_DURABLE);

	/* PU-30: post-read wrong version/checksum -> CORRUPTION_VERSION, no
	 * proof/release. */
	r = cluster_page_apply_step_post_read(CLUSTER_PAGE_STATE_PAGE_WRITE_DURABLE, false);
	UT_ASSERT_EQ((int)r.state, (int)CLUSTER_PAGE_STATE_PAGE_WRITE_DURABLE);
	UT_ASSERT_EQ((int)r.outcome, (int)CLUSTER_PAGE_OUTCOME_CORRUPTION_VERSION);

	r = cluster_page_apply_step_post_read(CLUSTER_PAGE_STATE_PAGE_WRITE_DURABLE, true);
	UT_ASSERT_EQ((int)r.state, (int)CLUSTER_PAGE_STATE_POST_READ_VERIFIED);

	/* PU-29: stale authority before release -> STALE_AUTHORITY, no
	 * release. */
	r = cluster_page_apply_step_authority(CLUSTER_PAGE_STATE_POST_READ_VERIFIED, false);
	UT_ASSERT_EQ((int)r.state, (int)CLUSTER_PAGE_STATE_POST_READ_VERIFIED);
	UT_ASSERT_EQ((int)r.outcome, (int)CLUSTER_PAGE_OUTCOME_STALE_AUTHORITY);

	r = cluster_page_apply_step_authority(CLUSTER_PAGE_STATE_POST_READ_VERIFIED, true);
	UT_ASSERT_EQ((int)r.state, (int)CLUSTER_PAGE_STATE_RESOURCE_RELEASED);
}

UT_TEST(test_midwrite_cut_is_permanent_stop)
{
	/* PL-03 / §7.6: the repeated-recoverer target-write cut is a
	 * permanent RED/STOP — never SKIP, never an expected-failure green,
	 * never a mock carrier. */
	UT_ASSERT_EQ((int)cluster_page_apply_midwrite_cut(),
				 (int)CLUSTER_PAGE_OUTCOME_STABLE_BASE_UNRESOLVED);
	UT_ASSERT_EQ((int)cluster_page_apply_midwrite_cut(),
				 (int)CLUSTER_PAGE_OUTCOME_STABLE_BASE_UNRESOLVED);
}

UT_TEST(test_crash_matrix_rows)
{
	/* §7.7: all seven rows, exactly one outcome each. */
	UT_ASSERT_EQ((int)cluster_page_crash_matrix_verdict(CLUSTER_PAGE_CUT_BEFORE_SOURCE_PROOF),
				 (int)CLUSTER_PAGE_OUTCOME_BLOCKED_SOURCE);
	UT_ASSERT_EQ((int)cluster_page_crash_matrix_verdict(CLUSTER_PAGE_CUT_AFTER_SOURCE_PROOF),
				 (int)CLUSTER_PAGE_OUTCOME_BLOCKED_SOURCE);
	UT_ASSERT_EQ((int)cluster_page_crash_matrix_verdict(CLUSTER_PAGE_CUT_DURING_TARGET_WRITE),
				 (int)CLUSTER_PAGE_OUTCOME_STABLE_BASE_UNRESOLVED);
	UT_ASSERT_EQ(
		(int)cluster_page_crash_matrix_verdict(CLUSTER_PAGE_CUT_AFTER_WRITE_BEFORE_DURABILITY),
		(int)CLUSTER_PAGE_OUTCOME_BLOCKED_SOURCE);
	UT_ASSERT_EQ(
		(int)cluster_page_crash_matrix_verdict(CLUSTER_PAGE_CUT_AFTER_DURABILITY_BEFORE_POST_READ),
		(int)CLUSTER_PAGE_OUTCOME_BLOCKED_SOURCE);
	UT_ASSERT_EQ(
		(int)cluster_page_crash_matrix_verdict(CLUSTER_PAGE_CUT_AFTER_POST_READ_BEFORE_RELEASE),
		(int)CLUSTER_PAGE_OUTCOME_STALE_AUTHORITY);
	UT_ASSERT_EQ((int)cluster_page_crash_matrix_verdict(CLUSTER_PAGE_CUT_AFTER_RELEASE),
				 (int)CLUSTER_PAGE_OUTCOME_APPLY);
	/* Unknown cut: fail closed. */
	UT_ASSERT_EQ((int)cluster_page_crash_matrix_verdict((ClusterPageCrashCut)99),
				 (int)CLUSTER_PAGE_OUTCOME_BLOCKED_CLASS);
}

UT_TEST(test_proof_surface_fields)
{
	/* §7.3: the typed page proof carries only recomputable facts. */
	ClusterPageProof p;

	memset(&p, 0, sizeof(p));
	p.failed_origin_thread = 2;
	p.page_class = CLUSTER_PAGE_CLASS_NORMAL;
	p.contributor_coverage = true;
	p.durability_barrier_ok = true;
	p.post_read_ok = true;
	p.authority_revalidated = true;
	UT_ASSERT_EQ((int)p.failed_origin_thread, 2);
	UT_ASSERT_EQ((int)p.page_class, (int)CLUSTER_PAGE_CLASS_NORMAL);
	UT_ASSERT(p.contributor_coverage && p.durability_barrier_ok && p.post_read_ok
			  && p.authority_revalidated);
	/* A proof without the post-read result is not a proof: the caller
	 * must fail to produce it (field-level contract, PU-30). */
	UT_ASSERT_EQ((unsigned long long)p.post_read_version.token, 0ULL);
}

/* STOP-06 product TDD (implementation): the §7.2 mutation chain
 * as a full sequence — the seven admission facts and the durability ->
 * post-read -> authority order, plus the STOP gate's chain-level meaning
 * (the mid-write cut wedges the chain before any mutation state). */
UT_TEST(test_mutation_chain_full_sequence)
{
	ClusterPageMutationAdmission a;
	ClusterPageSequenceResult r;

	memset(&a, 0, sizeof(a));
	a.duty_root_ok = true;
	a.recoverer_active_ok = true;
	a.fence_ok = true;
	a.serialization_ok = true;
	a.source_proof_fresh = true;
	a.working_version_exact = true;
	a.retention_covers = true;
	UT_ASSERT(cluster_page_mutation_admission(&a));
	/* §7.1: any false fact -> no mutation. */
	a.retention_covers = false;
	UT_ASSERT(!cluster_page_mutation_admission(&a));
	a.retention_covers = true;
	a.working_version_exact = false;
	UT_ASSERT(!cluster_page_mutation_admission(&a));
	a.working_version_exact = true;
	UT_ASSERT(cluster_page_mutation_admission(&a));

	/* Full §7.2 chain: WAL-before-data -> durability barrier -> canonical
	 * post-read -> fresh authority -> released. */
	r = cluster_page_apply_step_durability(CLUSTER_PAGE_STATE_WAL_BEFORE_DATA_SATISFIED, true);
	UT_ASSERT_EQ((int)r.state, (int)CLUSTER_PAGE_STATE_PAGE_WRITE_DURABLE);
	UT_ASSERT_EQ((int)r.outcome, (int)CLUSTER_PAGE_OUTCOME_APPLY);
	r = cluster_page_apply_step_post_read(r.state, true);
	UT_ASSERT_EQ((int)r.state, (int)CLUSTER_PAGE_STATE_POST_READ_VERIFIED);
	r = cluster_page_apply_step_authority(r.state, true);
	UT_ASSERT_EQ((int)r.state, (int)CLUSTER_PAGE_STATE_RESOURCE_RELEASED);
	UT_ASSERT_EQ((int)r.outcome, (int)CLUSTER_PAGE_OUTCOME_APPLY);

	/* STOP gate at chain level (PL-03 / §7.6): the mid-write cut is a
	 * permanent wedge — the chain outcome is STABLE_BASE_UNRESOLVED and
	 * no step may be attempted past it (mutation = 0 while the STOP
	 * holds; U-SIDE-18: no config/test override exists). */
	UT_ASSERT_EQ((int)cluster_page_apply_midwrite_cut(),
				 (int)CLUSTER_PAGE_OUTCOME_STABLE_BASE_UNRESOLVED);
	UT_ASSERT_EQ((int)cluster_page_apply_midwrite_cut(),
				 (int)CLUSTER_PAGE_OUTCOME_STABLE_BASE_UNRESOLVED);
}

int
main(void)
{
	UT_PLAN(6);

	UT_RUN(test_admission_conjunction);
	UT_RUN(test_sequence_steps_advance_on_owning_proof);
	UT_RUN(test_midwrite_cut_is_permanent_stop);
	UT_RUN(test_crash_matrix_rows);
	UT_RUN(test_proof_surface_fields);
	UT_RUN(test_mutation_chain_full_sequence);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
