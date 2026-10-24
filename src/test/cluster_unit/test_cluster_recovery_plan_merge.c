/* PGRAC: compose the actual plan producer, publication/snapshot and cold
 * preflight. ROOT/WAL/fence owners remain boundary doubles; no native claim.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include <setjmp.h>
#include <stdarg.h>
#include <stdlib.h>
#include "storage/spin.h"

static void test_ereport_begin(int elevel);
static void test_ereport_end(void);
#undef ereport
#define ereport(elevel, ...) do { test_ereport_begin(elevel); \
    (void)(__VA_ARGS__); test_ereport_end(); } while (0)
#include "../../backend/cluster/cluster_recovery_merge.c"
#include "cluster/cluster_semantic_activation.h"
#include "unit_test.h"
UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, file, line);
	abort();
}

#define TEST_RECOVERY_PLAN_PUBLISHED 1
#include "test_cluster_recovery_merge_seal_boundary.h"
#include "test_cluster_recovery_merge_cold_fixture.h"
#include "test_cluster_recovery_plan_shmem.inc"

static ClusterRecoveryPlanShmem plan_cell;
static unsigned registry_checks, registry_reads, root_reads, latch_restores;
static uint16 discovery_bad_thread;
int cluster_recovery_stale_active_ms = 1000;
int CheckPointTimeout = 300;

TimestampTz
GetCurrentTimestamp(void)
{
	return INT64CONST(1000000000);
}

bool cluster_r4_bit22_cutover_active(void) { return false; }
bool cluster_control_root_restore_bit22_latch_if_active(void)
{
	latch_restores++;
	return false;
}
bool cluster_wal_state_registry_ready(void)
{
	registry_checks++;
	return false;
}
ClusterWalSlotVerdict
cluster_wal_state_read_slot(uint16 tid, ClusterWalStateSlot *out)
{
	(void)tid;
	memset(out, 0, sizeof(*out));
	registry_reads++;
	return CLUSTER_WAL_SLOT_EMPTY;
}
ClusterControlRootResult
cluster_control_root_read_canonical_discovered(uint16 tid,
	ClusterControlRootSnapshot *out, ClusterControlRootReadToken *token)
{
	root_reads++;
	memset(out, 0, sizeof(*out));
	memset(token, 0, sizeof(*token));
	if (tid > NODES)
		return CLUSTER_CONTROL_ROOT_ABSENT;
	if (tid == discovery_bad_thread)
		return CLUSTER_CONTROL_ROOT_BAD_BODY_CRC;
	*out = roots[tid];
	*token = tokens[tid];
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* These are the original private types and complete functions, extracted
 * verbatim. No fixture snapshot or hand-filled candidate census is consumed. */
#include "test_cluster_recovery_plan_publication.inc"
#include "test_cluster_recovery_plan_generate.inc"

static void
pipeline_fixture(void)
{
	fixture();
	memset(&plan_cell, 0, sizeof(plan_cell));
	pg_atomic_init_u32(&plan_cell.published, 0);
	cluster_recovery_plan_shmem = &plan_cell;
	registry_checks = registry_reads = root_reads = latch_restores = 0;
	discovery_bad_thread = 0;
}

UT_TEST(test_published_pre2_census_reaches_actual_cold_preflight)
{
	ClusterRecoveryPlan observed;
	ClusterRecoveryFencePlan *plan = NULL;
	ClusterMergeEngage engage;

	pipeline_fixture();
	cluster_recovery_plan_generate(DB_IN_PRODUCTION, true);
	UT_ASSERT_EQ(pg_atomic_read_u32(&plan_cell.published), 1);
	UT_ASSERT(cluster_recovery_plan_snapshot(&observed));
	UT_ASSERT(observed.generated && !observed.failed);
	UT_ASSERT_EQ(observed.own_thread, 1);
	UT_ASSERT_EQ(observed.n_crashed_candidate, 3);
	UT_ASSERT_EQ(observed.n_unknown + observed.n_alive, 0);
	UT_ASSERT_EQ(root_reads, CLUSTER_RECOVERY_PLAN_THREADS);
	UT_ASSERT_EQ(registry_checks + registry_reads + latch_restores, 0);
	for (uint16 tid = 1; tid <= NODES; tid++)
		UT_ASSERT_EQ(cluster_recovery_plan_candidate_test(&observed, tid), tid != 1);
	UT_ASSERT(preflight(&plan, &engage));
	UT_ASSERT_EQ(engage, CLUSTER_MERGE_ENGAGE);
	UT_ASSERT(plan != NULL);
	UT_ASSERT_EQ(cluster_recovery_merge_fence_plan_origin_count(plan), 3);
	cluster_recovery_merge_fence_plan_destroy(&plan);
}

UT_TEST(test_snapshot_is_a_copy_and_unpublished_input_is_not_consumed)
{
	ClusterRecoveryPlan observed;
	ClusterRecoveryFencePlan *plan = NULL;
	ClusterMergeEngage engage;

	pipeline_fixture();
	cluster_recovery_plan_generate(DB_IN_PRODUCTION, true);
	UT_ASSERT(cluster_recovery_plan_snapshot(&observed));
	observed.failed = true;
	memset(observed.candidate_bitmap, 0, sizeof(observed.candidate_bitmap));
	UT_ASSERT(cluster_recovery_plan_snapshot(&observed));
	UT_ASSERT(!observed.failed && observed.n_crashed_candidate == 3);
	UT_ASSERT(cluster_recovery_plan_candidate_test(&observed, 2));
	pg_atomic_write_u32(&plan_cell.published, 0);
	UT_ASSERT(preflight(&plan, &engage));
	UT_ASSERT_EQ(engage, CLUSTER_MERGE_NO_NO_PLAN);
	UT_ASSERT(plan == NULL && formations_built == 0);
	UT_ASSERT(strstr(last_plan_log_detail, "PLAN_SNAPSHOT_UNAVAILABLE") != NULL);
}

UT_TEST(test_published_census_reprojects_after_original_foreign_seals)
{
	ClusterRecoveryPlan observed;
	ClusterRecoveryFencePlan *plan = NULL;

	pipeline_fixture();
	make_open(2);
	make_sealed_without_tail(3);
	cluster_recovery_plan_generate(DB_IN_PRODUCTION, true);
	UT_ASSERT(cluster_recovery_plan_snapshot(&observed));
	UT_ASSERT_EQ(observed.n_crashed_candidate, 3);
	expect_engaged(&plan);
	if (plan == NULL)
		return;
	UT_ASSERT_EQ(open_calls[2], 1);
	UT_ASSERT_EQ(tail_calls[2], 1);
	UT_ASSERT_EQ(open_calls[3], 0);
	UT_ASSERT_EQ(tail_calls[3], 1);
	UT_ASSERT_EQ(plan->origin_count, 3);
	UT_ASSERT_EQ(plan->start_lsn[1], OWN_REDO);
	UT_ASSERT_EQ(plan->start_lsn[2], redo[2]);
	UT_ASSERT_EQ(plan->start_lsn[3], redo[3]);
	UT_ASSERT(cluster_recovery_plan_snapshot(&observed));
	UT_ASSERT(observed.n_crashed_candidate == 3 && !observed.failed);
	cluster_recovery_merge_fence_plan_destroy(&plan);
	UT_ASSERT(plan == NULL);
}

UT_TEST(test_generated_failed_source_is_consumed_as_no_plan)
{
	ClusterRecoveryPlan observed;
	uint64 bitmap[2] = { 0, 0 };
	XLogRecPtr start[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };

	pipeline_fixture();
	cluster_shared_config = false;
	cluster_recovery_plan_generate(DB_IN_PRODUCTION, true);
	UT_ASSERT(cluster_recovery_plan_snapshot(&observed));
	UT_ASSERT(observed.generated && observed.failed);
	UT_ASSERT_EQ(registry_checks, 1);
	UT_ASSERT_EQ(root_reads + registry_reads, 0);
	UT_ASSERT_EQ(cluster_recovery_merge_project_readonly(1, OWN_REDO, bitmap, start),
		CLUSTER_MERGE_NO_NO_PLAN);
	UT_ASSERT(strstr(last_plan_log_detail, "predicate=PLAN_FAILED") != NULL);
	UT_ASSERT(strstr(last_plan_log_detail, "generated=1") != NULL);
	UT_ASSERT_EQ(formations_built, 0);
	cluster_shared_config = true;
}

UT_TEST(test_generated_unknown_never_reaches_fence_admission)
{
	ClusterRecoveryPlan observed;
	ClusterRecoveryFencePlan *plan = NULL;
	ClusterMergeEngage engage;

	pipeline_fixture();
	discovery_bad_thread = 2;
	cluster_recovery_plan_generate(DB_IN_PRODUCTION, true);
	UT_ASSERT(cluster_recovery_plan_snapshot(&observed));
	UT_ASSERT_EQ(observed.n_unknown, 1);
	UT_ASSERT(!cluster_recovery_plan_candidate_test(&observed, 2));
	UT_ASSERT(!preflight(&plan, &engage));
	UT_ASSERT(plan == NULL && formations_built == 0);
	UT_ASSERT_EQ(last_sqlstate, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT(strstr(last_detail, "UNKNOWN") != NULL);
}

UT_TEST(test_generated_alive_peer_keeps_cold_preflight_closed)
{
	ClusterRecoveryPlan observed;
	ClusterRecoveryFencePlan *plan = NULL;
	ClusterMergeEngage engage;

	pipeline_fixture();
	roots[2].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	roots[2].published_at_usec = GetCurrentTimestamp();
	cluster_recovery_plan_generate(DB_IN_PRODUCTION, true);
	UT_ASSERT(cluster_recovery_plan_snapshot(&observed));
	UT_ASSERT_EQ(observed.n_alive, 1);
	UT_ASSERT(preflight(&plan, &engage));
	UT_ASSERT_EQ(engage, CLUSTER_MERGE_NO_NOT_COLD);
	UT_ASSERT(plan == NULL && formations_built == 0);
}

UT_TEST(test_published_alive_observation_is_refreshed_before_admission)
{
	ClusterRecoveryPlan observed;
	ClusterRecoveryFencePlan *plan = NULL;
	ClusterMergeEngage engage;
	unsigned prior_reads;

	pipeline_fixture();
	roots[2].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	roots[2].published_at_usec = GetCurrentTimestamp();
	cluster_recovery_plan_generate(DB_IN_PRODUCTION, true);
	UT_ASSERT(cluster_recovery_plan_snapshot(&observed));
	UT_ASSERT_EQ(observed.n_alive, 1);
	/* The original peer self-seal completed before this consumer began.
	 * The published observational census still describes its prior OPEN. */
	roots[2].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	roots[2].root_publish_seq++;
	tokens[2].root_publish_seq++;
	prior_reads = root_reads;
	UT_ASSERT(preflight(&plan, &engage));
	UT_ASSERT_EQ(engage, CLUSTER_MERGE_ENGAGE);
	UT_ASSERT(plan != NULL);
	UT_ASSERT(root_reads > prior_reads);
	UT_ASSERT(cluster_recovery_plan_snapshot(&observed));
	UT_ASSERT_EQ(observed.n_alive, 0);
	UT_ASSERT_EQ(observed.n_crashed_candidate, 3);
	cluster_recovery_merge_fence_plan_destroy(&plan);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_published_pre2_census_reaches_actual_cold_preflight);
	UT_RUN(test_snapshot_is_a_copy_and_unpublished_input_is_not_consumed);
	UT_RUN(test_published_census_reprojects_after_original_foreign_seals);
	UT_RUN(test_generated_failed_source_is_consumed_as_no_plan);
	UT_RUN(test_generated_unknown_never_reaches_fence_admission);
	UT_RUN(test_generated_alive_peer_keeps_cold_preflight_closed);
	UT_RUN(test_published_alive_observation_is_refreshed_before_admission);
	UT_DONE();
	return ut_failed_count != 0;
}
