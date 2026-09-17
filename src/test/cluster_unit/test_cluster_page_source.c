/*-------------------------------------------------------------------------
 *
 * test_cluster_page_source.c
 *    RF-PAGE PGDEL-04 focused unit tests: the CURRENT/PI/STORAGE
 *    provenance validators (§5.2/§5.3/§5.4 conjunctions) and the
 *    §5.5/§6.2 source-selection rule.
 *
 *    RED mapping (spec §10.1 / §5):
 *      - invalid/absent source version, wrong identity -> every
 *        validator false (never a proof from raw locators);
 *      - CURRENT requires the GCS stability witness (PU-22: an empty
 *        set without a witness is not a valid source);
 *      - PI stale/corrupt/wrong lineage -> rejected, bytes never reused
 *        (PU-23);
 *      - STORAGE without a checkpoint anchor -> rejected (PU-24);
 *      - STORAGE while contributors_closed=false (PGDEL-05 not landed)
 *        -> rejected: the STORAGE base is honestly closed (G3);
 *      - no valid source -> selection BLOCKED (PU-26);
 *      - conflicting valid versions -> BLOCKED, never max-SCN/LSN;
 *      - equal valid versions -> CURRENT > PI > STORAGE (§6.2).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_page_source.h"

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

static ClusterPageIdentity ut_identity;
static ClusterPageVersion ut_version_a; /* token 100 */
static ClusterPageVersion ut_version_b; /* token 200 (conflicting) */
static ClusterPageVersion ut_invalid;

static void
ut_setup_globals(void)
{
	memset(&ut_identity, 0, sizeof(ut_identity));
	ut_identity.rlocator.spcOid = 1;
	ut_identity.rlocator.dbOid = 2;
	ut_identity.rlocator.relNumber = 3;
	ut_identity.forknum = MAIN_FORKNUM;
	ut_identity.blocknum = 42;

	ut_version_a.identity = ut_identity;
	ut_version_a.incarnation = 7;
	ut_version_a.token = 100;
	ut_version_b.identity = ut_identity;
	ut_version_b.incarnation = 7;
	ut_version_b.token = 200;

	memset(&ut_invalid, 0, sizeof(ut_invalid));
}

static ClusterPageSourceValidateInput
ut_full_current_input(void)
{
	ClusterPageSourceValidateInput in;

	memset(&in, 0, sizeof(in));
	in.identity = &ut_identity;
	in.source_version = &ut_version_a;
	in.integrity_ok = true;
	in.stability_ok = true;
	in.lineage_ok = true;
	in.owner_ok = true;
	return in;
}

UT_TEST(test_validate_current_conjunction)
{
	ClusterPageSourceValidateInput in = ut_full_current_input();

	UT_ASSERT(cluster_page_source_validate_current(&in));

	/* Every §5.2 fact is required: no witness, no lineage, no owner,
	 * no integrity -> fail. */
	in.stability_ok = false;
	UT_ASSERT(!cluster_page_source_validate_current(&in));
	in = ut_full_current_input();
	in.lineage_ok = false;
	UT_ASSERT(!cluster_page_source_validate_current(&in));
	in = ut_full_current_input();
	in.owner_ok = false;
	UT_ASSERT(!cluster_page_source_validate_current(&in));
	in = ut_full_current_input();
	in.integrity_ok = false;
	UT_ASSERT(!cluster_page_source_validate_current(&in));

	/* PU-22: CURRENT without the GCS stability witness is not a source,
	 * even with every other fact true. */
	in = ut_full_current_input();
	in.stability_ok = false;
	UT_ASSERT(!cluster_page_source_validate_current(&in));

	/* Invalid version -> fail. */
	in = ut_full_current_input();
	in.source_version = &ut_invalid;
	UT_ASSERT(!cluster_page_source_validate_current(&in));
	/* ut_version_b shares ut_identity: a DIFFERENT version of the same
	 * block is still a valid CURRENT source — CURRENT admits the source;
	 * the terminal-version comparison is the apply layer's (§5.2-6). */
	in = ut_full_current_input();
	in.source_version = &ut_version_b;
	UT_ASSERT(cluster_page_source_validate_current(&in));

	/* NULLs. */
	UT_ASSERT(!cluster_page_source_validate_current(NULL));
}

UT_TEST(test_validate_pi_conjunction)
{
	ClusterPageSourceValidateInput in = ut_full_current_input();

	/* PI additionally requires the ship/boundary SCN proof (§5.3). */
	UT_ASSERT(!cluster_page_source_validate_pi(&in));
	in.ship_boundary_ok = true;
	UT_ASSERT(cluster_page_source_validate_pi(&in));

	/* PU-23: PI stale/corrupt/wrong lineage -> rejected (bytes never
	 * reused; the caller discards the PI). */
	in = ut_full_current_input();
	in.ship_boundary_ok = true;
	in.stability_ok = false; /* holder changed */
	UT_ASSERT(!cluster_page_source_validate_pi(&in));
	in = ut_full_current_input();
	in.ship_boundary_ok = true;
	in.lineage_ok = false; /* wrong failure-generation lineage */
	UT_ASSERT(!cluster_page_source_validate_pi(&in));
	in = ut_full_current_input();
	in.ship_boundary_ok = true;
	in.integrity_ok = false; /* corrupt past image */
	UT_ASSERT(!cluster_page_source_validate_pi(&in));
	in = ut_full_current_input();
	in.ship_boundary_ok = true;
	in.source_version = &ut_invalid;
	UT_ASSERT(!cluster_page_source_validate_pi(&in));
}

UT_TEST(test_validate_storage_conjunction)
{
	ClusterPageSourceValidateInput in = ut_full_current_input();

	/* STORAGE requires anchor + coverage + freshness + contributor
	 * closure.  While contributors_closed is false (PGDEL-05 not landed)
	 * STORAGE is honestly CLOSED (G3). */
	UT_ASSERT(!cluster_page_source_validate_storage(&in));
	in.anchored_ok = true;
	in.coverage_ok = true;
	in.fresh_ok = true;
	UT_ASSERT(!cluster_page_source_validate_storage(&in)); /* closure missing */
	in.contributors_closed = true;
	UT_ASSERT(cluster_page_source_validate_storage(&in));

	/* PU-24: no checkpoint anchor -> reject. */
	in = ut_full_current_input();
	in.contributors_closed = true;
	in.coverage_ok = true;
	in.fresh_ok = true;
	UT_ASSERT(!cluster_page_source_validate_storage(&in)); /* anchored missing */
	in.anchored_ok = true;
	UT_ASSERT(cluster_page_source_validate_storage(&in));

	/* Missing coverage / freshness / lineage each fail the conjunction. */
	in.coverage_ok = false;
	UT_ASSERT(!cluster_page_source_validate_storage(&in));
	in = ut_full_current_input();
	in.anchored_ok = true;
	in.contributors_closed = true;
	in.fresh_ok = false;
	UT_ASSERT(!cluster_page_source_validate_storage(&in));
}

UT_TEST(test_source_select_no_valid_blocked)
{
	ClusterPageSourceKind kinds[2];
	ClusterPageSourceValidateInput inputs[2];

	memset(inputs, 0, sizeof(inputs));
	inputs[0] = ut_full_current_input();
	inputs[0].stability_ok = false; /* CURRENT without witness */
	inputs[1] = ut_full_current_input();
	inputs[1].ship_boundary_ok = false; /* PI without ship proof */
	kinds[0] = CLUSTER_PAGE_SOURCE_CURRENT;
	kinds[1] = CLUSTER_PAGE_SOURCE_PI;

	/* PU-26: CURRENT/PI absent -> no source; only the per-block
	 * failed-redo path may be considered (PGDEL-05/06). */
	UT_ASSERT_EQ(cluster_page_source_select(kinds, inputs, 2), -1);
	UT_ASSERT_EQ(cluster_page_source_select(NULL, inputs, 2), -1);
	UT_ASSERT_EQ(cluster_page_source_select(kinds, NULL, 2), -1);
}

UT_TEST(test_source_select_conflict_blocked)
{
	ClusterPageSourceKind kinds[2];
	ClusterPageSourceValidateInput inputs[2];

	inputs[0] = ut_full_current_input();
	inputs[0].source_version = &ut_version_a;
	inputs[1] = ut_full_current_input();
	inputs[1].source_version = &ut_version_b; /* conflicting token */
	inputs[1].ship_boundary_ok = true; /* PI otherwise invalid: conflict needs two valid sources */
	kinds[0] = CLUSTER_PAGE_SOURCE_CURRENT;
	kinds[1] = CLUSTER_PAGE_SOURCE_PI;

	/* §5.5: conflicting valid versions are corruption/BLOCKED — never
	 * max-SCN/max-LSN/majority. */
	UT_ASSERT_EQ(cluster_page_source_select(kinds, inputs, 2), -1);
}

UT_TEST(test_source_select_preference_order)
{
	ClusterPageSourceKind kinds[3];
	ClusterPageSourceValidateInput inputs[3];
	ClusterPageSourceKind kinds2[3];

	/* Two valid sources with EQUAL versions: CURRENT > PI > STORAGE
	 * (§6.2). */
	inputs[0] = ut_full_current_input();
	inputs[1] = ut_full_current_input();
	inputs[1].ship_boundary_ok = true;
	inputs[2] = ut_full_current_input();
	inputs[2].anchored_ok = true;
	inputs[2].coverage_ok = true;
	inputs[2].fresh_ok = true;
	inputs[2].contributors_closed = true;
	kinds[0] = CLUSTER_PAGE_SOURCE_CURRENT;
	kinds[1] = CLUSTER_PAGE_SOURCE_PI;
	kinds[2] = CLUSTER_PAGE_SOURCE_STORAGE;
	UT_ASSERT_EQ(cluster_page_source_select(kinds, inputs, 3), 0);

	/* CURRENT invalid: PI wins over STORAGE. */
	inputs[0].stability_ok = false;
	UT_ASSERT_EQ(cluster_page_source_select(kinds, inputs, 3), 1);

	/* CURRENT+PI invalid: STORAGE alone (closure proof present). */
	inputs[1].ship_boundary_ok = false;
	UT_ASSERT_EQ(cluster_page_source_select(kinds, inputs, 3), 2);

	/* Only CURRENT valid: single valid source selected regardless of
	 * position. */
	inputs[0] = ut_full_current_input();
	kinds2[0] = CLUSTER_PAGE_SOURCE_STORAGE;
	kinds2[1] = CLUSTER_PAGE_SOURCE_CURRENT;
	kinds2[2] = CLUSTER_PAGE_SOURCE_PI;
	UT_ASSERT_EQ(cluster_page_source_select(kinds2, inputs, 3), 1);
}

int
main(void)
{
	UT_PLAN(6);

	ut_setup_globals();

	UT_RUN(test_validate_current_conjunction);
	UT_RUN(test_validate_pi_conjunction);
	UT_RUN(test_validate_storage_conjunction);
	UT_RUN(test_source_select_no_valid_blocked);
	UT_RUN(test_source_select_conflict_blocked);
	UT_RUN(test_source_select_preference_order);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
