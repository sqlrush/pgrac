/*-------------------------------------------------------------------------
 *
 * test_cluster_write_fence.c
 *	  Unit tests for the spec-4.12 cooperative write-fence PURE judge
 *	  cluster_write_fence_decide -- the truth table the hot write paths
 *	  consult before any shared-storage write.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_write_fence.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_write_fence.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/*
 * libpgport's snprintf.c references ExceptionalCondition in a cassert build; this
 * pure-inline test links libpgport for the unit harness but no backend object, so
 * provide a local stub (mirrors test_cluster_thread_apply.c).  It must never fire.
 */
void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# unexpected Assert: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

/* Named-field helper so the truth table reads clearly. */
#define DECIDE(enf, attached, ep, auth, now, expire, self)                                          \
	cluster_write_fence_decide((enf), (attached), (ep), (auth), (now), (expire), (self))

/* A fully-authorized baseline (allowed): on, attached, epoch matches, lease valid, not fenced. */
#define ALLOWED_BASELINE DECIDE(true, true, 42, 42, 100, 200, false)


UT_TEST(test_enforcement_off_is_escape_hatch)
{
	/* enforcement off -> always allowed, regardless of every other input. */
	UT_ASSERT(DECIDE(false, false, 7, 9, 999, 0, true));
	UT_ASSERT(DECIDE(false, true, 42, 42, 100, 200, false));
}

UT_TEST(test_baseline_authorized_is_allowed)
{
	UT_ASSERT(ALLOWED_BASELINE);
}

UT_TEST(test_detached_region_fails_closed)
{
	/* L110: enforcement on but the token region is not attached -> fail closed. */
	UT_ASSERT(!DECIDE(true, false, 42, 42, 100, 200, false));
}

UT_TEST(test_self_fenced_fails_closed)
{
	UT_ASSERT(!DECIDE(true, true, 42, 42, 100, 200, true));
}

UT_TEST(test_stale_epoch_fails_closed_exact_compare)
{
	/* exact == : neither ahead nor behind passes (a stale node must not write). */
	UT_ASSERT(!DECIDE(true, true, 41, 42, 100, 200, false)); /* behind */
	UT_ASSERT(!DECIDE(true, true, 43, 42, 100, 200, false)); /* ahead (NOT >=) */
}

UT_TEST(test_lease_expired_fails_closed)
{
	/* now >= lease_expire -> the node failed to refresh (partition) -> fail closed. */
	UT_ASSERT(!DECIDE(true, true, 42, 42, 200, 200, false)); /* now == expire */
	UT_ASSERT(!DECIDE(true, true, 42, 42, 201, 200, false)); /* now > expire */
}

UT_TEST(test_lease_just_valid_is_allowed)
{
	UT_ASSERT(DECIDE(true, true, 42, 42, 199, 200, false)); /* now < expire */
}


int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_enforcement_off_is_escape_hatch);
	UT_RUN(test_baseline_authorized_is_allowed);
	UT_RUN(test_detached_region_fails_closed);
	UT_RUN(test_self_fenced_fails_closed);
	UT_RUN(test_stale_epoch_fails_closed_exact_compare);
	UT_RUN(test_lease_expired_fails_closed);
	UT_RUN(test_lease_just_valid_is_allowed);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
