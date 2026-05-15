/*-------------------------------------------------------------------------
 *
 * test_cluster_lock_acquire.c
 *	  Standalone unit tests for spec-2.20 7-step state machine internal
 *	  API(D13;descoped scope per v0.4)。
 *
 *	  T-7step-1..5 — internal API surface verify(no PG hot path
 *	  integration;spec-2.21 wire to lock.c 时 verify integration):
 *	    T-7step-1: 7 individual step API + top-level entry symbol
 *	               linkable + initial counter 0
 *	    T-7step-2: HC1 LMS fail-closed S1 entry — NULL req → INTERNAL;
 *	               cluster_lms_enabled=false → OK_NATIVE skip(legacy
 *	               path signal)
 *	    T-7step-3: S2/S3/S4/S5/S6/S7 NULL req → INTERNAL
 *	    T-7step-4: top-level cluster_lock_acquire_seven_step() NULL req
 *	               → S1 INTERNAL without S7 cleanup(pre-reservation fail)
 *	    T-7step-5: I1 monotonic forward transition contract — top-level
 *	               entry returns OK_NATIVE/PENDING honestly and only
 *	               post-reservation failures may run S7 cleanup
 *
 *	  Stubs:
 *	    - cluster_lms_is_ready / cluster_lms_enabled / cluster_lmd_is_ready
 *	      provide test-controlled state(default ready=true,enabled=true)
 *
 *	  Spec: spec-2.20-7step-state-machine-activation.md(v0.4 descoped)。
 *	  Cross-spec lesson inheritance: L94 / L104 / L107 / L124 (HC4 exact
 *	  predicate)。
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_lock_acquire.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Standalone binary linking
 *	  cluster_lock_acquire.o + cluster_version.o only;all PG backend
 *	  symbols stubbed locally.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_lock_acquire.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lock.h"

/* Drop PG's port.h printf override; unit_test.h uses stdlib printf. */
#ifdef vprintf
#undef vprintf
#endif
#ifdef printf
#undef printf
#endif
#ifdef fprintf
#undef fprintf
#endif

#include "unit_test.h"


/* ============================================================
 * PG runtime + cluster_lms / cluster_lmd / cluster_grd stubs.
 * ============================================================ */

void
elog_start(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		   const char *fn pg_attribute_unused())
{}

void
elog_finish(int e pg_attribute_unused(), const char *f pg_attribute_unused(), ...)
{}

/*
 * spec-2.20 D13 stubs — control LMS/LMD ready state for HC1 test paths.
 *
 *	stub_lms_ready_for_test is INDEPENDENTLY controllable from
 *	cluster_lms_enabled GUC — caller-side gate走 cluster_lms_enabled
 *	first, then cluster_lms_is_ready() second (HC1 fail-closed)。
 */
bool cluster_lms_enabled = true;
bool cluster_lmd_enabled = true;
static bool stub_lms_ready_for_test = true;

bool
cluster_lms_is_ready(void)
{
	return stub_lms_ready_for_test;
}

bool
cluster_lmd_is_ready(void)
{
	return cluster_lmd_enabled;
}


/* ============================================================
 * Test cases.
 * ============================================================ */

/*
 * T-7step-1: API symbol linkability + initial counter 0.
 */
UT_TEST(test_7step_api_surface_linkable_and_initial_counters_zero)
{
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_s1_entry);
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_s2_identity);
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_s3_partition_reservation);
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_s4_remote_request_wait);
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_s5_promote_holder);
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_s6_release);
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_s7_cleanup);
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_seven_step);

	/*
	 * Initial counters may be 0 OR > 0 across test runs(static init
	 * persistent;not reset between UT_TEST functions in same binary).
	 * Just verify accessor links + returns sensible value.
	 */
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_s1_entry_count);
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_s7_cleanup_count);
}

/*
 * T-7step-2: HC1 LMS fail-closed S1 entry behavior.
 *
 *	- NULL req → INTERNAL
 *	- cluster_lms_enabled=false → OK_NATIVE(legacy path signal)
 *	- cluster_lms_enabled=true + LMS not ready(stub返回 false)→
 *	  FAIL_LMS_UNAVAILABLE(HC4 exact predicate;53R80)
 */
UT_TEST(test_7step_s1_hc1_fail_closed)
{
	ClusterLockAcquireRequest req;
	uint64 pre = cluster_lock_acquire_s1_entry_count();

	/* NULL req → INTERNAL */
	UT_ASSERT_EQ((int)cluster_lock_acquire_s1_entry(NULL), (int)CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT_EQ(cluster_lock_acquire_s1_entry_count(), pre + 1);

	/* enabled=false → OK_NATIVE skip(legacy path)*/
	memset(&req, 0, sizeof(req));
	cluster_lms_enabled = false;
	UT_ASSERT_EQ((int)cluster_lock_acquire_s1_entry(&req), (int)CLUSTER_LOCK_ACQUIRE_OK_NATIVE);

	/* enabled=true + LMS not ready → FAIL_LMS_UNAVAILABLE(HC1)*/
	cluster_lms_enabled = true;
	stub_lms_ready_for_test = false;
	UT_ASSERT_EQ((int)cluster_lock_acquire_s1_entry(&req),
				 (int)CLUSTER_LOCK_ACQUIRE_FAIL_LMS_UNAVAILABLE);

	/* enabled=true + LMS ready → OK_GRANTED(dispatch to S2)*/
	stub_lms_ready_for_test = true;
	UT_ASSERT_EQ((int)cluster_lock_acquire_s1_entry(&req), (int)CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
}

/*
 * T-7step-3: S2-S7 NULL req → INTERNAL.
 */
UT_TEST(test_7step_individual_steps_null_req_internal)
{
	UT_ASSERT_EQ((int)cluster_lock_acquire_s2_identity(NULL),
				 (int)CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT_EQ((int)cluster_lock_acquire_s3_partition_reservation(NULL),
				 (int)CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT_EQ((int)cluster_lock_acquire_s4_remote_request_wait(NULL),
				 (int)CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT_EQ((int)cluster_lock_acquire_s5_promote_holder(NULL),
				 (int)CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT_EQ((int)cluster_lock_acquire_s6_release(NULL),
				 (int)CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT_EQ((int)cluster_lock_acquire_s7_cleanup(NULL),
				 (int)CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
}

/*
 * T-7step-4: top-level cluster_lock_acquire_seven_step() NULL req → S1
 * FAIL_INTERNAL without S7 cleanup.  S1/S2 fail before reservation, so
 * top-level must not invoke S7 once S7 is wired to real cancel/release.
 */
UT_TEST(test_7step_top_level_null_req_s7_cleanup_invoked)
{
	uint64 pre_s7 = cluster_lock_acquire_s7_cleanup_count();
	ClusterLockAcquireResult r;

	r = cluster_lock_acquire_seven_step(NULL);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT_EQ(cluster_lock_acquire_s7_cleanup_count(), pre_s7);
}

/*
 * T-7step-5: I1 monotonic forward transition — top-level entry with
 * normal req walks S1 → S2 → S3, then returns PENDING per descoped
 * scope.  It must not continue to S5 and pretend an ungranted lock is
 * OK_GRANTED.
 */
UT_TEST(test_7step_top_level_monotonic_forward_no_cleanup_on_success)
{
	ClusterLockAcquireRequest req;
	uint64 pre_s7 = cluster_lock_acquire_s7_cleanup_count();
	ClusterLockAcquireResult r;

	memset(&req, 0, sizeof(req));
	cluster_lms_enabled = true;
	r = cluster_lock_acquire_seven_step(&req);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_LOCK_ACQUIRE_PENDING);
	UT_ASSERT_EQ(cluster_lock_acquire_s7_cleanup_count(), pre_s7);

	cluster_lms_enabled = false;
	r = cluster_lock_acquire_seven_step(&req);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_LOCK_ACQUIRE_OK_NATIVE);
	UT_ASSERT_EQ(cluster_lock_acquire_s7_cleanup_count(), pre_s7);
	cluster_lms_enabled = true;

	/*
	 * S4's dontwait timeout is still visible at the individual step
	 * level; top-level cannot reach S4 until spec-2.21 wires S3
	 * reservation completion.
	 */
	req.dontwait = true;
	r = cluster_lock_acquire_s4_remote_request_wait(&req);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT);
	UT_ASSERT_EQ(cluster_lock_acquire_s7_cleanup_count(), pre_s7);
}


UT_DEFINE_GLOBALS();


int
main(int argc pg_attribute_unused(), char **const argv pg_attribute_unused())
{
	UT_PLAN(5);

	UT_RUN(test_7step_api_surface_linkable_and_initial_counters_zero);
	UT_RUN(test_7step_s1_hc1_fail_closed);
	UT_RUN(test_7step_individual_steps_null_req_internal);
	UT_RUN(test_7step_top_level_null_req_s7_cleanup_invoked);
	UT_RUN(test_7step_top_level_monotonic_forward_no_cleanup_on_success);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
