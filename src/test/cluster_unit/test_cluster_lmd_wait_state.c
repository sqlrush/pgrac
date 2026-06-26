/*-------------------------------------------------------------------------
 *
 * test_cluster_lmd_wait_state.c
 *	  Standalone unit tests for the per-proc cluster wait-state (spec-5.8
 *	  D1d) — the record the LMD resolver reads to revalidate a deadlock
 *	  victim is still genuinely waiting before it cancels the victim.
 *
 *	  The tests exercise the publish / clear / read / init / reset API
 *	  directly on a ClusterLmdProcWaitState (the record PGPROC embeds), so
 *	  no PGPROC / shmem machinery is required.  Coverage:
 *
 *	    U1: publish records {kind, request_id, cluster_epoch, xid} and marks
 *	        the proc active; read returns the same tuple + wait_seq.
 *	    U2: clear marks the proc inactive (the single funnel every wait exit
 *	        path — grant / timeout / ERROR / cancel — calls).
 *	    U3: wait_seq is strictly monotonic across publishes (the ABA guard).
 *	    U4: reset (the InitProcess backstop for a backend that reused a slot
 *	        whose predecessor died mid-wait) clears active but PRESERVES
 *	        wait_seq, so a stale victim tuple can never re-match — procno
 *	        reuse ABA defeated.
 *	    U5: read of a never-published / cleared record reports inactive.
 *	    U6: publish's returned wait_seq equals the value a reader observes
 *	        (the value the caller stamps on the WFG vertex, spec-5.8 D1e).
 *	    U7: a published-but-not-cleared record stays active — documents why
 *	        the wiring must clear on EVERY exit path (leak => false-kill).
 *
 *	  The real GES / TX wait sites and their per-exit clear wiring are
 *	  covered end to end by the spec-5.8 TAP suite (D8); this file pins the
 *	  record's own contract.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_lmd_wait_state.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Standalone binary linking
 *	  cluster_lmd_wait_state.o only;all PG backend symbols stubbed locally.
 *	  Spec: spec-5.8-full-cross-node-deadlock-detector.md.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "access/transam.h"
#include "cluster/cluster_lmd_wait_state.h"

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

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/* ============================================================
 * U1 — publish records the full tuple + marks active.
 * ============================================================ */

UT_TEST(test_publish_records_tuple_and_active)
{
	ClusterLmdProcWaitState ws;
	ClusterLmdWaitStateSnapshot snap;
	uint64 seq;

	cluster_lmd_wait_state_init(&ws);

	seq = cluster_lmd_wait_state_publish(&ws, CLUSTER_LMD_WAIT_GES, 42, 7, 100);
	UT_ASSERT_EQ((int)seq, 1); /* first publish after init */

	UT_ASSERT(cluster_lmd_wait_state_read(&ws, &snap));
	UT_ASSERT(snap.active);
	UT_ASSERT_EQ((int)snap.kind, (int)CLUSTER_LMD_WAIT_GES);
	UT_ASSERT_EQ((int)snap.request_id, 42);
	UT_ASSERT_EQ((int)snap.cluster_epoch, 7);
	UT_ASSERT_EQ((int)snap.xid, 100);
	UT_ASSERT_EQ((int)snap.wait_seq, 1);
}


/* ============================================================
 * U2 — clear marks inactive (every wait-exit path's funnel).
 * ============================================================ */

UT_TEST(test_clear_marks_inactive)
{
	ClusterLmdProcWaitState ws;
	ClusterLmdWaitStateSnapshot snap;

	cluster_lmd_wait_state_init(&ws);
	(void)cluster_lmd_wait_state_publish(&ws, CLUSTER_LMD_WAIT_TX, 9, 3, 0);
	UT_ASSERT(cluster_lmd_wait_state_read(&ws, &snap));

	cluster_lmd_wait_state_clear(&ws);

	UT_ASSERT(!cluster_lmd_wait_state_read(&ws, &snap));
	UT_ASSERT(!snap.active);
}


/* ============================================================
 * U3 — wait_seq strictly monotonic across publishes (ABA guard).
 * ============================================================ */

UT_TEST(test_wait_seq_monotonic)
{
	ClusterLmdProcWaitState ws;
	uint64 s1, s2, s3;

	cluster_lmd_wait_state_init(&ws);

	s1 = cluster_lmd_wait_state_publish(&ws, CLUSTER_LMD_WAIT_GES, 1, 1, 0);
	cluster_lmd_wait_state_clear(&ws);
	s2 = cluster_lmd_wait_state_publish(&ws, CLUSTER_LMD_WAIT_GES, 2, 1, 0);
	cluster_lmd_wait_state_clear(&ws);
	s3 = cluster_lmd_wait_state_publish(&ws, CLUSTER_LMD_WAIT_GES, 3, 1, 0);

	UT_ASSERT(s2 > s1);
	UT_ASSERT(s3 > s2);
}


/* ============================================================
 * U4 — reset clears active but PRESERVES wait_seq (procno reuse ABA).
 * ============================================================ */

UT_TEST(test_reset_preserves_wait_seq)
{
	ClusterLmdProcWaitState ws;
	ClusterLmdWaitStateSnapshot snap;
	uint64 seq_before, seq_after;

	cluster_lmd_wait_state_init(&ws);

	/* Predecessor backend publishes then dies mid-wait (no clear). */
	seq_before = cluster_lmd_wait_state_publish(&ws, CLUSTER_LMD_WAIT_GES, 77, 5, 200);

	/* A new backend reuses the slot — InitProcess reset runs. */
	cluster_lmd_wait_state_reset(&ws);

	/* The stale active flag is gone... */
	UT_ASSERT(!cluster_lmd_wait_state_read(&ws, &snap));

	/* ...and the next publish does NOT restart wait_seq, so a stale victim
	 * tuple recorded for the predecessor (seq_before) can never re-match. */
	seq_after = cluster_lmd_wait_state_publish(&ws, CLUSTER_LMD_WAIT_GES, 78, 5, 201);
	UT_ASSERT(seq_after > seq_before);
}


/* ============================================================
 * U5 — read of a fresh / cleared record reports inactive.
 * ============================================================ */

UT_TEST(test_read_fresh_is_inactive)
{
	ClusterLmdProcWaitState ws;
	ClusterLmdWaitStateSnapshot snap;

	cluster_lmd_wait_state_init(&ws);

	UT_ASSERT(!cluster_lmd_wait_state_read(&ws, &snap));
	UT_ASSERT(!snap.active);
}


/* ============================================================
 * U6 — publish's returned wait_seq equals what a reader observes.
 * ============================================================ */

UT_TEST(test_publish_return_matches_read)
{
	ClusterLmdProcWaitState ws;
	ClusterLmdWaitStateSnapshot snap;
	uint64 seq;

	cluster_lmd_wait_state_init(&ws);
	seq = cluster_lmd_wait_state_publish(&ws, CLUSTER_LMD_WAIT_TX, 555, 11, 9000);

	UT_ASSERT(cluster_lmd_wait_state_read(&ws, &snap));
	UT_ASSERT_EQ((int)snap.wait_seq, (int)seq);
}


/* ============================================================
 * U7 — published-but-not-cleared record stays active (leak => false-kill).
 * ============================================================ */

UT_TEST(test_uncleared_publish_stays_active)
{
	ClusterLmdProcWaitState ws;
	ClusterLmdWaitStateSnapshot snap;

	cluster_lmd_wait_state_init(&ws);
	(void)cluster_lmd_wait_state_publish(&ws, CLUSTER_LMD_WAIT_GES, 1, 1, 0);

	/* No clear — the record is still active.  This is exactly the leak the
	 * wiring's clear-on-every-exit-path prevents. */
	UT_ASSERT(cluster_lmd_wait_state_read(&ws, &snap));
	UT_ASSERT(snap.active);
}


int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_publish_records_tuple_and_active);
	UT_RUN(test_clear_marks_inactive);
	UT_RUN(test_wait_seq_monotonic);
	UT_RUN(test_reset_preserves_wait_seq);
	UT_RUN(test_read_fresh_is_inactive);
	UT_RUN(test_publish_return_matches_read);
	UT_RUN(test_uncleared_publish_stays_active);
	UT_DONE();
	return 0;
}
