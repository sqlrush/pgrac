/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_drain.c
 *    Real consumer tests for complete, current target drain evidence.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_pgrac_fenced_drain.c
 *
 * NOTES
 *    PGRAC-original unit; fixture facts are not a live storage certificate.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "pgrac_fenced_drain.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static void
fixture(PgracFencedDrainIdentityV1 *expected, PgracFencedDrainEvidenceV1 *observed,
		PgracFencedDrainRouteV1 routes[4])
{
	unsigned int i;

	memset(expected, 0, sizeof(*expected));
	memset(observed, 0, sizeof(*observed));
	memset(expected->operation_id, 1, 16);
	expected->attempt = 17;
	memset(expected->daemon_boot_id, 2, 16);
	memset(expected->target_boot_id, 3, 16);
	memset(expected->challenge, 4, 16);
	memset(expected->guest_uuid, 5, 16);
	expected->mapping_generation = 7;
	memset(expected->protected_set_digest, 6, 32);
	expected->route_count = 4;
	observed->identity = *expected;
	observed->target_state = PGRAC_FENCED_TARGET_OFF;
	observed->completed = PGRAC_DRAIN_GLOBAL_COMPLETE;
	observed->route_count = 4;
	observed->routes = routes;
	for (i = 0; i < 4; i++) {
		routes[i].ordinal = i;
		routes[i].completed = PGRAC_DRAIN_ROUTE_COMPLETE;
	}
}

static void
expect_failure(const PgracFencedDrainIdentityV1 *expected,
			   const PgracFencedDrainEvidenceV1 *observed, PgracFencedDrainResult result)
{
	PgracFencedReadbackV1 out;
	const PgracFencedReadbackV1 zero = { 0 };

	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(pgrac_fenced_drain_verify(expected, observed, &out), result);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
}

UT_TEST(test_complete_exact_set_and_reordered_evidence)
{
	PgracFencedDrainIdentityV1 expected;
	PgracFencedDrainEvidenceV1 observed;
	PgracFencedDrainRouteV1 routes[4];
	PgracFencedReadbackV1 out;

	fixture(&expected, &observed, routes);
	UT_ASSERT_EQ(pgrac_fenced_drain_verify(&expected, &observed, &out), PGRAC_DRAIN_PROVEN);
	UT_ASSERT_EQ(out.state, PGRAC_FENCED_TARGET_OFF);
	UT_ASSERT_EQ(out.io_drain_state, PGRAC_FENCED_IO_DRAIN_DRAINED);
	UT_ASSERT(memcmp(out.observed_target_uuid, expected.guest_uuid, 16) == 0);
	routes[0].ordinal = 3;
	routes[3].ordinal = 0;
	UT_ASSERT_EQ(pgrac_fenced_drain_verify(&expected, &observed, &out), PGRAC_DRAIN_PROVEN);
}

UT_TEST(test_every_operation_identity_is_bound)
{
	PgracFencedDrainIdentityV1 expected;
	PgracFencedDrainEvidenceV1 observed;
	PgracFencedDrainRouteV1 routes[4];
	unsigned int i;

	for (i = 0; i < 9; i++) {
		fixture(&expected, &observed, routes);
		switch (i) {
		case 0:
			observed.identity.operation_id[0]++;
			break;
		case 1:
			observed.identity.attempt++;
			break;
		case 2:
			observed.identity.daemon_boot_id[0]++;
			break;
		case 3:
			observed.identity.target_boot_id[0]++;
			break;
		case 4:
			observed.identity.challenge[0]++;
			break;
		case 5:
			observed.identity.guest_uuid[0]++;
			break;
		case 6:
			observed.identity.mapping_generation++;
			break;
		case 7:
			observed.identity.protected_set_digest[0]++;
			break;
		case 8:
			observed.identity.route_count++;
			break;
		}
		expect_failure(&expected, &observed, PGRAC_DRAIN_IDENTITY_MISMATCH);
	}
}

UT_TEST(test_empty_duplicate_missing_extra_and_unmapped_routes)
{
	PgracFencedDrainIdentityV1 expected;
	PgracFencedDrainEvidenceV1 observed;
	PgracFencedDrainRouteV1 routes[4];

	fixture(&expected, &observed, routes);
	observed.route_count = 0;
	expect_failure(&expected, &observed, PGRAC_DRAIN_INCOMPLETE);
	observed.route_count = 3;
	expect_failure(&expected, &observed, PGRAC_DRAIN_INCOMPLETE);
	observed.route_count = 5; /* Reject length before dereferencing a fifth entry. */
	expect_failure(&expected, &observed, PGRAC_DRAIN_MALFORMED);
	observed.route_count = 4;
	routes[3].ordinal = 2;
	expect_failure(&expected, &observed, PGRAC_DRAIN_MALFORMED);
	routes[3].ordinal = 4;
	expect_failure(&expected, &observed, PGRAC_DRAIN_MALFORMED);
	observed.routes = NULL;
	expect_failure(&expected, &observed, PGRAC_DRAIN_MALFORMED);
}

UT_TEST(test_off_and_empty_sessions_do_not_substitute_for_drain)
{
	PgracFencedDrainIdentityV1 expected;
	PgracFencedDrainEvidenceV1 observed;
	PgracFencedDrainRouteV1 routes[4];
	unsigned int i;

	for (i = 0; i < 5; i++) {
		fixture(&expected, &observed, routes);
		routes[2].completed &= ~(UINT32_C(1) << i);
		expect_failure(&expected, &observed, PGRAC_DRAIN_INCOMPLETE);
	}
	fixture(&expected, &observed, routes);
	routes[2].completed
		= PGRAC_DRAIN_DENY_DURABLE | PGRAC_DRAIN_ADMISSION_DISABLED | PGRAC_DRAIN_SESSION_STOPPED;
	expect_failure(&expected, &observed, PGRAC_DRAIN_INCOMPLETE);
	observed.completed &= ~PGRAC_DRAIN_INVENTORY_COMPLETE;
	expect_failure(&expected, &observed, PGRAC_DRAIN_INCOMPLETE);
}

UT_TEST(test_target_state_inventory_and_on_exclusion)
{
	PgracFencedDrainIdentityV1 expected;
	PgracFencedDrainEvidenceV1 observed;
	PgracFencedDrainRouteV1 routes[4];
	uint32 state;

	for (state = PGRAC_FENCED_TARGET_ON; state <= PGRAC_FENCED_TARGET_UNKNOWN; state++) {
		fixture(&expected, &observed, routes);
		observed.target_state = state;
		expect_failure(&expected, &observed, PGRAC_DRAIN_TARGET_NOT_OFF);
	}
	fixture(&expected, &observed, routes);
	observed.completed = PGRAC_DRAIN_INVENTORY_COMPLETE;
	expect_failure(&expected, &observed, PGRAC_DRAIN_INCOMPLETE);
	observed.completed = PGRAC_DRAIN_UNSOLICITED_ON_BLOCKED;
	expect_failure(&expected, &observed, PGRAC_DRAIN_INCOMPLETE);
	observed.completed = PGRAC_DRAIN_GLOBAL_COMPLETE | 4;
	expect_failure(&expected, &observed, PGRAC_DRAIN_MALFORMED);
	observed.completed = PGRAC_DRAIN_GLOBAL_COMPLETE;
	routes[1].completed |= 32;
	expect_failure(&expected, &observed, PGRAC_DRAIN_MALFORMED);
}

UT_TEST(test_unproved_expectation_cannot_be_echoed_as_success)
{
	PgracFencedDrainIdentityV1 expected;
	PgracFencedDrainEvidenceV1 observed;
	PgracFencedDrainRouteV1 routes[4];

	fixture(&expected, &observed, routes);
	memset(expected.challenge, 0, 16);
	observed.identity = expected;
	expect_failure(&expected, &observed, PGRAC_DRAIN_BAD_ARGUMENT);
	fixture(&expected, &observed, routes);
	expected.route_count = PGRAC_PROTECTED_SET_V2_MAX_ROUTES + 1;
	observed.identity = expected;
	expect_failure(&expected, &observed, PGRAC_DRAIN_BAD_ARGUMENT);
	expect_failure(NULL, &observed, PGRAC_DRAIN_BAD_ARGUMENT);
	fixture(&expected, &observed, routes);
	expect_failure(&expected, NULL, PGRAC_DRAIN_BAD_ARGUMENT);
	UT_ASSERT_EQ(pgrac_fenced_drain_verify(&expected, &observed, NULL), PGRAC_DRAIN_BAD_ARGUMENT);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_complete_exact_set_and_reordered_evidence);
	UT_RUN(test_every_operation_identity_is_bound);
	UT_RUN(test_empty_duplicate_missing_extra_and_unmapped_routes);
	UT_RUN(test_off_and_empty_sessions_do_not_substitute_for_drain);
	UT_RUN(test_target_state_inventory_and_on_exclusion);
	UT_RUN(test_unproved_expectation_cannot_be_echoed_as_success);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
