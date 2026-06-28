/*-------------------------------------------------------------------------
 *
 * test_cluster_cr_coordinator.c
 *	  pgrac spec-5.57 D9 — cluster_unit tests for the cross-instance CR/current
 *	  read-path coordinator boundary module (cluster_cr_coordinator_stat.c):
 *	  the independent observability counter region, the pure origin classifier
 *	  (own / merged-materialized remote / runtime-warm remote / invalid), the
 *	  origin(0-based) <-> owner_instance(1-based) namespace conversion, and the
 *	  GUC defaults.
 *
 *	  Links cluster_cr_coordinator_stat.o standalone; stubs ShmemInitStruct
 *	  (malloc-backed), cluster_shmem_register_region (no-op),
 *	  cluster_merged_instance_is_materialized (test-controlled), and provides
 *	  cluster_node_id storage.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.57-cross-instance-cr-current-coordinator.md (FROZEN v1.0)
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cr_coordinator.c
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stdlib.h>
#include <string.h>

#include "cluster/cluster_cr_coordinator_stat.h"
#include "cluster/cluster_recovery_merge.h" /* cluster_merged_instance_is_materialized proto */
#include "cluster/cluster_scn.h"			/* SCN_MAX_VALID_NODE_ID, NodeId */
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_uba.h" /* InvalidNodeId */
#include "storage/shmem.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* classifier reads this global (the local instance's 0-based node id) */
int cluster_node_id = -1;

/* test-controlled materialized-origin oracle (spec-4.5a class②) */
static bool stub_is_materialized = false;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* ---- stubs ---- *
 * ShmemInitStruct returns a fresh malloc each call (found=false) for isolation;
 * the region registration is a no-op (the test drives init directly). */
void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	void *p = malloc(size);

	if (p != NULL)
		memset(p, 0, size);
	if (foundPtr != NULL)
		*foundPtr = false;
	return p;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{
	/* link-only stub; the test calls cluster_cr_coordinator_shmem_init() directly */
}

bool
cluster_merged_instance_is_materialized(int origin_node pg_attribute_unused())
{
	return stub_is_materialized;
}

/* ===== tests ===== */

/* U2 leg: the region initialises all four counters to zero. */
UT_TEST(test_stat_init_zero)
{
	int i;

	cluster_cr_coordinator_shmem_init();
	for (i = 0; i < CR_COORD_COUNTER__COUNT; i++)
		UT_ASSERT_EQ(cluster_cr_coordinator_stat_count((ClusterCrCoordCounter)i), 0);

	/* exactly four counters are frozen by the spec (§2.6) */
	UT_ASSERT_EQ((int)CR_COORD_COUNTER__COUNT, 4);
}

/* U2 leg: per-counter atomic bump is isolated. */
UT_TEST(test_stat_bump_counts)
{
	cluster_cr_coordinator_shmem_init();

	cluster_cr_coordinator_stat_bump(CR_COORD_CROSS_INSTANCE_CR_REFUSED);
	cluster_cr_coordinator_stat_bump(CR_COORD_CROSS_INSTANCE_CR_REFUSED);
	cluster_cr_coordinator_stat_bump(CR_COORD_REMOTE_UNDO_READ_REFUSED);
	cluster_cr_coordinator_stat_bump(CR_COORD_MATERIALIZED_REMOTE_SERVED);
	cluster_cr_coordinator_stat_bump(CR_COORD_MATERIALIZED_REMOTE_SERVED);
	cluster_cr_coordinator_stat_bump(CR_COORD_MATERIALIZED_REMOTE_SERVED);

	UT_ASSERT_EQ(cluster_cr_coordinator_stat_count(CR_COORD_CROSS_INSTANCE_CR_REFUSED), 2);
	UT_ASSERT_EQ(cluster_cr_coordinator_stat_count(CR_COORD_REMOTE_UNDO_READ_REFUSED), 1);
	UT_ASSERT_EQ(cluster_cr_coordinator_stat_count(CR_COORD_MATERIALIZED_REMOTE_SERVED), 3);
	UT_ASSERT_EQ(cluster_cr_coordinator_stat_count(CR_COORD_CROSS_INSTANCE_BOUNDARY_PROBE), 0);
}

/* U2 leg: out-of-range bumps/reads must not crash or corrupt neighbours. */
UT_TEST(test_stat_bump_invalid_noop)
{
	cluster_cr_coordinator_shmem_init();

	cluster_cr_coordinator_stat_bump((ClusterCrCoordCounter)-1);
	cluster_cr_coordinator_stat_bump((ClusterCrCoordCounter)CR_COORD_COUNTER__COUNT);
	cluster_cr_coordinator_stat_bump((ClusterCrCoordCounter)(CR_COORD_COUNTER__COUNT + 7));

	UT_ASSERT_EQ(cluster_cr_coordinator_stat_count(CR_COORD_CROSS_INSTANCE_CR_REFUSED), 0);
	UT_ASSERT_EQ(cluster_cr_coordinator_stat_count((ClusterCrCoordCounter)-1), 0);
	UT_ASSERT_EQ(cluster_cr_coordinator_stat_count((ClusterCrCoordCounter)CR_COORD_COUNTER__COUNT),
				 0);
}

/*
 * U4: the pure origin classifier — the heart of the read-path coordinator
 * boundary (§0.1 three CR origin classes + invalid).
 */
UT_TEST(test_classify_origin)
{
	cluster_node_id = 2; /* this instance is node 2 (0-based) */

	/* ① own-instance */
	stub_is_materialized = false;
	UT_ASSERT_EQ((int)cluster_cr_coordinator_classify_origin(2), (int)CR_COORD_ORIGIN_OWN);

	/* node 0 is a perfectly legal own-instance id (L10/L48: 0-based, not a sentinel) */
	cluster_node_id = 0;
	UT_ASSERT_EQ((int)cluster_cr_coordinator_classify_origin(0), (int)CR_COORD_ORIGIN_OWN);
	cluster_node_id = 2;

	/* ② merged-materialized remote => served from the local tree */
	stub_is_materialized = true;
	UT_ASSERT_EQ((int)cluster_cr_coordinator_classify_origin(5),
				 (int)CR_COORD_ORIGIN_MATERIALIZED_REMOTE);

	/* ③ runtime-warm remote => fail-closed boundary (53R9G data plane → Stage 6) */
	stub_is_materialized = false;
	UT_ASSERT_EQ((int)cluster_cr_coordinator_classify_origin(5),
				 (int)CR_COORD_ORIGIN_RUNTIME_REMOTE);

	/* invalid: InvalidNodeId and out-of-range owners are NOT silently 'own' */
	UT_ASSERT_EQ((int)cluster_cr_coordinator_classify_origin(InvalidNodeId),
				 (int)CR_COORD_ORIGIN_INVALID);
	UT_ASSERT_EQ((int)cluster_cr_coordinator_classify_origin(SCN_MAX_VALID_NODE_ID + 1),
				 (int)CR_COORD_ORIGIN_INVALID);
}

/*
 * U3: origin(0-based) <-> owner_instance(1-based) namespace conversion
 * (L48 namespace separation; same arithmetic the undo segment-owner path uses).
 */
UT_TEST(test_origin_owner_namespace)
{
	NodeId origin;

	for (origin = 0; origin <= SCN_MAX_VALID_NODE_ID; origin++) {
		int owner_instance = cluster_cr_coordinator_origin_to_owner_instance(origin);

		/* owner_instance is 1-based and round-trips back to the 0-based origin */
		UT_ASSERT_EQ(owner_instance, (int)origin + 1);
		UT_ASSERT_EQ(cluster_cr_coordinator_owner_instance_to_origin(owner_instance), (int)origin);
	}
	/* the lowest legal owner_instance is 1 (origin 0), never 0 */
	UT_ASSERT_EQ(cluster_cr_coordinator_origin_to_owner_instance(0), 1);
}

/* GUC defaults + the four frozen counter key names (shared with the dump). */
UT_TEST(test_guc_defaults_and_keys)
{
	/* boundary is the default mode; probe defaults off (§2.2) */
	UT_ASSERT_EQ(cluster_cross_instance_cr_coordinator, (int)CR_COORD_MODE_BOUNDARY);
	UT_ASSERT_EQ((int)cluster_cross_instance_cr_probe, 0);

	UT_ASSERT_STR_EQ(cluster_cr_coordinator_counter_key(CR_COORD_CROSS_INSTANCE_CR_REFUSED),
					 "cross_instance_cr_refused");
	UT_ASSERT_STR_EQ(cluster_cr_coordinator_counter_key(CR_COORD_REMOTE_UNDO_READ_REFUSED),
					 "remote_undo_read_refused");
	UT_ASSERT_STR_EQ(cluster_cr_coordinator_counter_key(CR_COORD_MATERIALIZED_REMOTE_SERVED),
					 "materialized_remote_served");
	UT_ASSERT_STR_EQ(cluster_cr_coordinator_counter_key(CR_COORD_CROSS_INSTANCE_BOUNDARY_PROBE),
					 "cross_instance_boundary_probe");
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_stat_init_zero);
	UT_RUN(test_stat_bump_counts);
	UT_RUN(test_stat_bump_invalid_noop);
	UT_RUN(test_classify_origin);
	UT_RUN(test_origin_owner_namespace);
	UT_RUN(test_guc_defaults_and_keys);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
