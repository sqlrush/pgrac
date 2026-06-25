/*-------------------------------------------------------------------------
 *
 * test_cluster_extend_gate.c
 *	  Unit tests for the shared relation-extend engage gate (spec-5.7 §3.1d,
 *	  v1.5 amend).
 *
 *	  Two layers are covered:
 *	    - cluster_extend_engage_classify(): the pure decision core, tested
 *	      exhaustively over the (liveness, lms_ready, no_fencing, cssd_ready,
 *	      any_alive_peer) input space, including the no-fencing single-node-
 *	      compat carve-out (Q21, the t/066 case).
 *	    - cluster_extend_liveness_engage(): the public wrapper, tested by
 *	      mocking the six runtime accessors it reads so the bounded LMS wait
 *	      dispatch is exercised deterministically (a TAP test cannot force the
 *	      warmup window reliably).
 *
 *	  The wrapper's real cross-node behaviour (single-alive native extend,
 *	  PEER_ALIVE warmup wait) is additionally exercised by the TAP harness.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_extend_gate.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.7-misc-enqueue-classes.md (§3.1d, v1.5 amend)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cssd.h"
#include "cluster/cluster_extend_gate.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_qvotec.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

/* ----------------------------------------------------------------------
 * Mocks for the six runtime accessors cluster_extend_liveness_engage()
 * reads.  Only cluster_extend_gate.o is linked into this test, so these
 * definitions resolve its externs without pulling in the real backend.
 * ---------------------------------------------------------------------- */
static ClusterCfLiveness mock_liveness;
static bool mock_lms_ready;
static bool mock_wait_result;
static bool mock_wait_called;
static int mock_disks_total;
static ClusterCssdStatus mock_cssd_status;
static int mock_alive_peers;

/* GUC referenced by the wrapper for the bounded-wait budget. */
int cluster_ges_request_timeout_ms = 5000;

ClusterCfLiveness
cluster_cf_assess_liveness(void)
{
	return mock_liveness;
}

bool
cluster_lms_is_ready(void)
{
	return mock_lms_ready;
}

bool
cluster_lms_wait_for_ready(int timeout_ms)
{
	mock_wait_called = true;
	(void)timeout_ms;
	return mock_wait_result;
}

int
cluster_qvotec_get_disks_total_count(void)
{
	return mock_disks_total;
}

ClusterCssdStatus
cluster_cssd_get_status(void)
{
	return mock_cssd_status;
}

int
cluster_cssd_get_alive_peer_count(void)
{
	return mock_alive_peers;
}

static void
mocks_reset(void)
{
	mock_liveness = CLUSTER_CF_LIVENESS_UNKNOWN;
	mock_lms_ready = false;
	mock_wait_result = false;
	mock_wait_called = false;
	mock_disks_total = 0;
	mock_cssd_status = CLUSTER_CSSD_STARTING;
	mock_alive_peers = 0;
}

/* ======================================================================
 * Pure classify -- exhaustive over the meaningful cells.
 * ====================================================================== */

/* PEER_ALIVE + LMS ready -> coordinate (the steady-state success path). */
UT_TEST(test_classify_peer_alive_ready)
{
	UT_ASSERT_EQ(
		cluster_extend_engage_classify(CLUSTER_CF_LIVENESS_PEER_ALIVE, true, false, true, true),
		CLUSTER_EXTEND_ENGAGE_COORDINATE);
}

/* PEER_ALIVE + LMS not ready -> WAIT_LMS (caller resolves via bounded wait). */
UT_TEST(test_classify_peer_alive_warmup)
{
	UT_ASSERT_EQ(
		cluster_extend_engage_classify(CLUSTER_CF_LIVENESS_PEER_ALIVE, false, false, true, true),
		CLUSTER_EXTEND_ENGAGE_WAIT_LMS);
}

/* SOLE -> native (degraded-survivor-with-voting-disks / proven alone). */
UT_TEST(test_classify_sole_native)
{
	/* SOLE wins regardless of the fencing/cssd/peer inputs. */
	UT_ASSERT_EQ(
		cluster_extend_engage_classify(CLUSTER_CF_LIVENESS_SOLE, false, false, false, true),
		CLUSTER_EXTEND_ENGAGE_NATIVE);
}

/* UNKNOWN + no fencing + CSSD ready + no alive peer -> native (Q21 / t/066). */
UT_TEST(test_classify_unknown_nofencing_alone_native)
{
	UT_ASSERT_EQ(
		cluster_extend_engage_classify(CLUSTER_CF_LIVENESS_UNKNOWN, false, true, true, false),
		CLUSTER_EXTEND_ENGAGE_NATIVE);
}

/* UNKNOWN + no fencing but a peer IS alive -> fail closed (cannot native). */
UT_TEST(test_classify_unknown_nofencing_haspeer_failclosed)
{
	UT_ASSERT_EQ(
		cluster_extend_engage_classify(CLUSTER_CF_LIVENESS_UNKNOWN, false, true, true, true),
		CLUSTER_EXTEND_ENGAGE_FAIL_CLOSED);
}

/* UNKNOWN + no fencing + CSSD NOT ready -> fail closed (peer count untrusted). */
UT_TEST(test_classify_unknown_nofencing_cssd_notready_failclosed)
{
	UT_ASSERT_EQ(
		cluster_extend_engage_classify(CLUSTER_CF_LIVENESS_UNKNOWN, false, true, false, false),
		CLUSTER_EXTEND_ENGAGE_FAIL_CLOSED);
}

/* UNKNOWN + fencing configured but quorum not proven -> fail closed. */
UT_TEST(test_classify_unknown_fencing_failclosed)
{
	UT_ASSERT_EQ(
		cluster_extend_engage_classify(CLUSTER_CF_LIVENESS_UNKNOWN, false, false, true, false),
		CLUSTER_EXTEND_ENGAGE_FAIL_CLOSED);
}

/* ======================================================================
 * Public wrapper -- via mocks; resolves WAIT_LMS through the bounded wait.
 * ====================================================================== */

/* SOLE -> native; the LMS wait must not be consulted. */
UT_TEST(test_engage_sole_native)
{
	mocks_reset();
	mock_liveness = CLUSTER_CF_LIVENESS_SOLE;
	UT_ASSERT_EQ(cluster_extend_liveness_engage(true), CLUSTER_EXTEND_ENGAGE_NATIVE);
	UT_ASSERT_EQ(mock_wait_called, false);
}

/* PEER_ALIVE + LMS already ready -> coordinate, no wait. */
UT_TEST(test_engage_peer_alive_ready)
{
	mocks_reset();
	mock_liveness = CLUSTER_CF_LIVENESS_PEER_ALIVE;
	mock_lms_ready = true;
	mock_alive_peers = 1;
	UT_ASSERT_EQ(cluster_extend_liveness_engage(true), CLUSTER_EXTEND_ENGAGE_COORDINATE);
	UT_ASSERT_EQ(mock_wait_called, false);
}

/* PEER_ALIVE + warmup + wait succeeds -> coordinate (wait consulted). */
UT_TEST(test_engage_peer_alive_warmup_wait_ok)
{
	mocks_reset();
	mock_liveness = CLUSTER_CF_LIVENESS_PEER_ALIVE;
	mock_lms_ready = false;
	mock_wait_result = true;
	mock_alive_peers = 1;
	UT_ASSERT_EQ(cluster_extend_liveness_engage(true), CLUSTER_EXTEND_ENGAGE_COORDINATE);
	UT_ASSERT_EQ(mock_wait_called, true);
}

/* PEER_ALIVE + warmup + wait times out -> fail closed (never native). */
UT_TEST(test_engage_peer_alive_warmup_wait_timeout)
{
	mocks_reset();
	mock_liveness = CLUSTER_CF_LIVENESS_PEER_ALIVE;
	mock_lms_ready = false;
	mock_wait_result = false;
	mock_alive_peers = 1;
	UT_ASSERT_EQ(cluster_extend_liveness_engage(true), CLUSTER_EXTEND_ENGAGE_FAIL_CLOSED);
	UT_ASSERT_EQ(mock_wait_called, true);
}

/* PEER_ALIVE + warmup + caller does NOT wait -> fail closed, wait not called. */
UT_TEST(test_engage_peer_alive_warmup_no_wait)
{
	mocks_reset();
	mock_liveness = CLUSTER_CF_LIVENESS_PEER_ALIVE;
	mock_lms_ready = false;
	mock_alive_peers = 1;
	UT_ASSERT_EQ(cluster_extend_liveness_engage(false), CLUSTER_EXTEND_ENGAGE_FAIL_CLOSED);
	UT_ASSERT_EQ(mock_wait_called, false);
}

/* Class A (t/066): UNKNOWN + no voting disks + CSSD ready + no peer -> native. */
UT_TEST(test_engage_unknown_nofencing_alone_native)
{
	mocks_reset();
	mock_liveness = CLUSTER_CF_LIVENESS_UNKNOWN;
	mock_disks_total = 0;
	mock_cssd_status = CLUSTER_CSSD_READY;
	mock_alive_peers = 0;
	UT_ASSERT_EQ(cluster_extend_liveness_engage(true), CLUSTER_EXTEND_ENGAGE_NATIVE);
	UT_ASSERT_EQ(mock_wait_called, false);
}

/* UNKNOWN + fencing configured (disks present) -> fail closed. */
UT_TEST(test_engage_unknown_fencing_failclosed)
{
	mocks_reset();
	mock_liveness = CLUSTER_CF_LIVENESS_UNKNOWN;
	mock_disks_total = 3;
	mock_cssd_status = CLUSTER_CSSD_READY;
	mock_alive_peers = 0;
	UT_ASSERT_EQ(cluster_extend_liveness_engage(true), CLUSTER_EXTEND_ENGAGE_FAIL_CLOSED);
}

/* ======================================================================
 * SOLE->native predicate -- the thin boolean reused by the lock.c globalize
 * gate (spec-5.7 §3.1d, t/066 single-alive fix).  It must return true iff the
 * shared classifier is NATIVE, and it must NOT introduce a bounded LMS wait
 * (the lock hot path can never block on the coordination layer warming up).
 * These pins double as the guard against PEER_ALIVE / fenced-no-quorum being
 * mis-routed to native.
 * ====================================================================== */

/* SOLE -> sole-native true (skip globalization, take the PG-native lock). */
UT_TEST(test_is_sole_native_sole_true)
{
	mocks_reset();
	mock_liveness = CLUSTER_CF_LIVENESS_SOLE;
	UT_ASSERT_EQ(cluster_extend_liveness_is_sole_native(), true);
	UT_ASSERT_EQ(mock_wait_called, false);
}

/* Q21 / t/066: UNKNOWN + no voting disks + CSSD ready + no peer -> true. */
UT_TEST(test_is_sole_native_nofencing_alone_true)
{
	mocks_reset();
	mock_liveness = CLUSTER_CF_LIVENESS_UNKNOWN;
	mock_disks_total = 0;
	mock_cssd_status = CLUSTER_CSSD_READY;
	mock_alive_peers = 0;
	UT_ASSERT_EQ(cluster_extend_liveness_is_sole_native(), true);
	UT_ASSERT_EQ(mock_wait_called, false);
}

/* PEER_ALIVE + LMS ready -> false (keep globalizing; coordination needed). */
UT_TEST(test_is_sole_native_peer_alive_false)
{
	mocks_reset();
	mock_liveness = CLUSTER_CF_LIVENESS_PEER_ALIVE;
	mock_lms_ready = true;
	mock_alive_peers = 1;
	UT_ASSERT_EQ(cluster_extend_liveness_is_sole_native(), false);
	UT_ASSERT_EQ(mock_wait_called, false);
}

/* PEER_ALIVE + LMS warming -> false AND the bounded wait is never consulted
 * (wait_for_lms is forced false for the lock gate). */
UT_TEST(test_is_sole_native_peer_alive_warmup_false_no_wait)
{
	mocks_reset();
	mock_liveness = CLUSTER_CF_LIVENESS_PEER_ALIVE;
	mock_lms_ready = false;
	mock_alive_peers = 1;
	UT_ASSERT_EQ(cluster_extend_liveness_is_sole_native(), false);
	UT_ASSERT_EQ(mock_wait_called, false);
}

/* UNKNOWN + fencing configured but quorum not proven -> false (fail-closed
 * stays globalize-side; never mis-routed to native). */
UT_TEST(test_is_sole_native_fenced_no_quorum_false)
{
	mocks_reset();
	mock_liveness = CLUSTER_CF_LIVENESS_UNKNOWN;
	mock_disks_total = 3;
	mock_cssd_status = CLUSTER_CSSD_READY;
	mock_alive_peers = 0;
	UT_ASSERT_EQ(cluster_extend_liveness_is_sole_native(), false);
}

int
main(void)
{
	UT_PLAN(19);
	UT_RUN(test_classify_peer_alive_ready);
	UT_RUN(test_classify_peer_alive_warmup);
	UT_RUN(test_classify_sole_native);
	UT_RUN(test_classify_unknown_nofencing_alone_native);
	UT_RUN(test_classify_unknown_nofencing_haspeer_failclosed);
	UT_RUN(test_classify_unknown_nofencing_cssd_notready_failclosed);
	UT_RUN(test_classify_unknown_fencing_failclosed);
	UT_RUN(test_engage_sole_native);
	UT_RUN(test_engage_peer_alive_ready);
	UT_RUN(test_engage_peer_alive_warmup_wait_ok);
	UT_RUN(test_engage_peer_alive_warmup_wait_timeout);
	UT_RUN(test_engage_peer_alive_warmup_no_wait);
	UT_RUN(test_engage_unknown_nofencing_alone_native);
	UT_RUN(test_engage_unknown_fencing_failclosed);
	UT_RUN(test_is_sole_native_sole_true);
	UT_RUN(test_is_sole_native_nofencing_alone_true);
	UT_RUN(test_is_sole_native_peer_alive_false);
	UT_RUN(test_is_sole_native_peer_alive_warmup_false_no_wait);
	UT_RUN(test_is_sole_native_fenced_no_quorum_false);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
