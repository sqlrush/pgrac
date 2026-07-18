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
 *	    U8: clear + republish never returns a cross-generation snapshot.
 *	    U9: an in-progress writer is bounded and fails closed, rather than
 *	        returning a partially updated tuple.
 *	   U10: PCM_CONVERT is a distinct wait kind and preserves the full tuple.
 *	   U11: the embedded PGPROC record keeps its 48-byte layout footprint.
 *	   U12: exact read distinguishes a stable active tuple.
 *	   U13: exact read distinguishes a stable inactive tuple.
 *	   U14: exact read reports BUSY after bounded odd-sequence exhaustion.
 *	   U15: exact read recovers to ACTIVE once the sequence becomes stable.
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

#include <pthread.h>
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
	/* It may also die after opening, but before closing, the seqlock. */
	pg_atomic_write_u32(&ws.change_seq, pg_atomic_read_u32(&ws.change_seq) | 1U);

	/* A new backend reuses the slot — InitProcess reset runs. */
	cluster_lmd_wait_state_reset(&ws);

	/* The stale active flag is gone and the abandoned write is closed... */
	UT_ASSERT(!cluster_lmd_wait_state_read(&ws, &snap));
	UT_ASSERT_EQ((int)(pg_atomic_read_u32(&ws.change_seq) & 1U), 0);

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


/* ============================================================
 * U8 — clear + republish never exposes a cross-generation tuple.
 * ============================================================ */

#define TEAR_RACE_PUBLISHES 2000000

typedef struct WaitStateTearRace {
	ClusterLmdProcWaitState ws;
	pg_atomic_uint32 start;
	pg_atomic_uint32 stop;
	pg_atomic_uint32 mixed;
} WaitStateTearRace;

static void *
tear_race_writer(void *arg)
{
	WaitStateTearRace *race = (WaitStateTearRace *)arg;
	int i;

	while (pg_atomic_read_u32(&race->start) == 0)
		;

	for (i = 0; i < TEAR_RACE_PUBLISHES; i++) {
		cluster_lmd_wait_state_clear(&race->ws);
		if ((i & 1) == 0)
			(void)cluster_lmd_wait_state_publish(
				&race->ws, CLUSTER_LMD_WAIT_TX, UINT64CONST(0x4444444444444444),
				UINT64CONST(0x5555555555555555), (TransactionId)0x66666666);
		else
			(void)cluster_lmd_wait_state_publish(
				&race->ws, CLUSTER_LMD_WAIT_GES, UINT64CONST(0x1111111111111111),
				UINT64CONST(0x2222222222222222), (TransactionId)0x33333333);
	}

	pg_atomic_write_u32(&race->stop, 1);
	return NULL;
}

UT_TEST(test_clear_republish_snapshot_is_one_generation)
{
	WaitStateTearRace race;
	ClusterLmdWaitStateSnapshot snap;
	pthread_t writer;
	int rc;

	cluster_lmd_wait_state_init(&race.ws);
	pg_atomic_init_u32(&race.start, 0);
	pg_atomic_init_u32(&race.stop, 0);
	pg_atomic_init_u32(&race.mixed, 0);
	(void)cluster_lmd_wait_state_publish(
		&race.ws, CLUSTER_LMD_WAIT_GES, UINT64CONST(0x1111111111111111),
		UINT64CONST(0x2222222222222222), (TransactionId)0x33333333);

	rc = pthread_create(&writer, NULL, tear_race_writer, &race);
	UT_ASSERT_EQ(rc, 0);
	pg_atomic_write_u32(&race.start, 1);

	while (pg_atomic_read_u32(&race.stop) == 0 && pg_atomic_read_u32(&race.mixed) == 0) {
		if (cluster_lmd_wait_state_read(&race.ws, &snap)) {
			bool odd_tuple;
			bool even_tuple;

			odd_tuple = (snap.wait_seq & 1) != 0 && snap.kind == CLUSTER_LMD_WAIT_GES
						&& snap.request_id == UINT64CONST(0x1111111111111111)
						&& snap.cluster_epoch == UINT64CONST(0x2222222222222222)
						&& snap.xid == (TransactionId)0x33333333;
			even_tuple = (snap.wait_seq & 1) == 0 && snap.kind == CLUSTER_LMD_WAIT_TX
						 && snap.request_id == UINT64CONST(0x4444444444444444)
						 && snap.cluster_epoch == UINT64CONST(0x5555555555555555)
						 && snap.xid == (TransactionId)0x66666666;
			if (!odd_tuple && !even_tuple)
				pg_atomic_write_u32(&race.mixed, 1);
		}
	}

	rc = pthread_join(writer, NULL);
	UT_ASSERT_EQ(rc, 0);
	UT_ASSERT_EQ((int)pg_atomic_read_u32(&race.mixed), 0);
}


/* ============================================================
 * U9 — an in-progress writer is bounded and fails closed.
 * ============================================================ */

UT_TEST(test_writer_in_progress_fails_closed)
{
	ClusterLmdProcWaitState ws;
	ClusterLmdWaitStateSnapshot snap;
	uint32 stable_seq;

	cluster_lmd_wait_state_init(&ws);
	(void)cluster_lmd_wait_state_publish(&ws, CLUSTER_LMD_WAIT_GES, 9, 8, 7);
	stable_seq = pg_atomic_read_u32(&ws.change_seq);
	pg_atomic_write_u32(&ws.change_seq, stable_seq | 1U);

	memset(&snap, 0x7f, sizeof(snap));
	UT_ASSERT(!cluster_lmd_wait_state_read(&ws, &snap));
	UT_ASSERT(!snap.active);
	UT_ASSERT_EQ((int)snap.kind, (int)CLUSTER_LMD_WAIT_NONE);
	UT_ASSERT_EQ((int)snap.request_id, 0);
	UT_ASSERT_EQ((int)snap.cluster_epoch, 0);
	UT_ASSERT_EQ((int)snap.xid, (int)InvalidTransactionId);
	UT_ASSERT_EQ((int)snap.wait_seq, 0);
}


/* ============================================================
 * U10 — PCM_CONVERT is distinct and carries the complete tuple.
 * ============================================================ */

UT_TEST(test_pcm_convert_kind_records_complete_tuple)
{
	ClusterLmdProcWaitState ws;
	ClusterLmdWaitStateSnapshot snap;
	uint64 wait_seq;

	cluster_lmd_wait_state_init(&ws);
	wait_seq = cluster_lmd_wait_state_publish(
		&ws, CLUSTER_LMD_WAIT_PCM_CONVERT, UINT64CONST(0x123456789abcdef0),
		UINT64CONST(0x0fedcba987654321), (TransactionId)0xa5a5a5a5);

	UT_ASSERT_EQ((int)CLUSTER_LMD_WAIT_PCM_CONVERT, 3);
	UT_ASSERT(cluster_lmd_wait_state_read(&ws, &snap));
	UT_ASSERT_EQ((int)snap.kind, (int)CLUSTER_LMD_WAIT_PCM_CONVERT);
	UT_ASSERT(snap.request_id == UINT64CONST(0x123456789abcdef0));
	UT_ASSERT(snap.cluster_epoch == UINT64CONST(0x0fedcba987654321));
	UT_ASSERT_EQ((uint32)snap.xid, (uint32)0xa5a5a5a5);
	UT_ASSERT(snap.wait_seq == wait_seq);
}


/* ============================================================
 * U11 — seqlock metadata consumes existing padding (no PGPROC growth).
 * ============================================================ */

UT_TEST(test_wait_state_layout_stays_48_bytes)
{
	UT_ASSERT_EQ((int)sizeof(ClusterLmdProcWaitState), 48);
	UT_ASSERT_EQ((int)offsetof(ClusterLmdProcWaitState, change_seq), 0);
	UT_ASSERT_EQ((int)offsetof(ClusterLmdProcWaitState, active), 4);
	UT_ASSERT_EQ((int)offsetof(ClusterLmdProcWaitState, wait_seq), 8);
	UT_ASSERT_EQ((int)offsetof(ClusterLmdProcWaitState, kind), 16);
	UT_ASSERT_EQ((int)offsetof(ClusterLmdProcWaitState, request_id), 24);
	UT_ASSERT_EQ((int)offsetof(ClusterLmdProcWaitState, cluster_epoch), 32);
	UT_ASSERT_EQ((int)offsetof(ClusterLmdProcWaitState, xid), 40);
}


/* ============================================================
 * U12 — exact read returns ACTIVE with the complete stable tuple.
 * ============================================================ */

UT_TEST(test_exact_read_reports_stable_active)
{
	ClusterLmdProcWaitState ws;
	ClusterLmdWaitStateSnapshot snap;
	ClusterLmdWaitStateReadResult result;
	uint64 wait_seq;

	cluster_lmd_wait_state_init(&ws);
	wait_seq = cluster_lmd_wait_state_publish(
		&ws, CLUSTER_LMD_WAIT_PCM_CONVERT, UINT64CONST(0x1020304050607080),
		UINT64CONST(0x8877665544332211), (TransactionId)0x12345678);

	result = cluster_lmd_wait_state_read_exact(&ws, &snap);
	UT_ASSERT_EQ((int)result, (int)CLUSTER_LMD_WAIT_STATE_READ_ACTIVE);
	UT_ASSERT(snap.active);
	UT_ASSERT_EQ((int)snap.kind, (int)CLUSTER_LMD_WAIT_PCM_CONVERT);
	UT_ASSERT(snap.request_id == UINT64CONST(0x1020304050607080));
	UT_ASSERT(snap.cluster_epoch == UINT64CONST(0x8877665544332211));
	UT_ASSERT_EQ((uint32)snap.xid, (uint32)0x12345678);
	UT_ASSERT(snap.wait_seq == wait_seq);
}


/* ============================================================
 * U13 — exact read returns INACTIVE only from a stable even tuple.
 * ============================================================ */

UT_TEST(test_exact_read_reports_stable_inactive)
{
	ClusterLmdProcWaitState ws;
	ClusterLmdWaitStateSnapshot snap;
	ClusterLmdWaitStateReadResult result;

	cluster_lmd_wait_state_init(&ws);
	(void)cluster_lmd_wait_state_publish(&ws, CLUSTER_LMD_WAIT_GES, 39, 38, 37);
	cluster_lmd_wait_state_clear(&ws);
	memset(&snap, 0x7f, sizeof(snap));

	result = cluster_lmd_wait_state_read_exact(&ws, &snap);
	UT_ASSERT_EQ((int)result, (int)CLUSTER_LMD_WAIT_STATE_READ_INACTIVE);
	UT_ASSERT(!snap.active);
	UT_ASSERT_EQ((int)snap.kind, (int)CLUSTER_LMD_WAIT_NONE);
	UT_ASSERT_EQ((int)snap.request_id, 0);
	UT_ASSERT_EQ((int)snap.cluster_epoch, 0);
	UT_ASSERT_EQ((int)snap.xid, (int)InvalidTransactionId);
	UT_ASSERT_EQ((int)snap.wait_seq, 0);
}


/* ============================================================
 * U14 — an odd writer sequence exhausts the bounded reader as BUSY.
 * ============================================================ */

UT_TEST(test_exact_read_reports_busy_on_odd_exhaustion)
{
	ClusterLmdProcWaitState ws;
	ClusterLmdWaitStateSnapshot snap;
	ClusterLmdWaitStateReadResult result;

	cluster_lmd_wait_state_init(&ws);
	(void)cluster_lmd_wait_state_publish(&ws, CLUSTER_LMD_WAIT_GES, 19, 18, 17);
	pg_atomic_write_u32(&ws.change_seq, pg_atomic_read_u32(&ws.change_seq) | 1U);
	memset(&snap, 0x7f, sizeof(snap));

	result = cluster_lmd_wait_state_read_exact(&ws, &snap);
	UT_ASSERT_EQ((int)result, (int)CLUSTER_LMD_WAIT_STATE_READ_BUSY);
	UT_ASSERT(!snap.active);
	UT_ASSERT_EQ((int)snap.kind, (int)CLUSTER_LMD_WAIT_NONE);
	UT_ASSERT_EQ((int)snap.request_id, 0);
	UT_ASSERT_EQ((int)snap.cluster_epoch, 0);
	UT_ASSERT_EQ((int)snap.xid, (int)InvalidTransactionId);
	UT_ASSERT_EQ((int)snap.wait_seq, 0);
}


/* ============================================================
 * U15 — a BUSY read recovers to ACTIVE after the writer closes.
 * ============================================================ */

UT_TEST(test_exact_read_recovers_after_writer_closes)
{
	ClusterLmdProcWaitState ws;
	ClusterLmdWaitStateSnapshot snap;
	ClusterLmdWaitStateReadResult result;
	uint32 stable_seq;

	cluster_lmd_wait_state_init(&ws);
	(void)cluster_lmd_wait_state_publish(&ws, CLUSTER_LMD_WAIT_TX, 29, 28, 27);
	stable_seq = pg_atomic_read_u32(&ws.change_seq);
	pg_atomic_write_u32(&ws.change_seq, stable_seq | 1U);

	result = cluster_lmd_wait_state_read_exact(&ws, &snap);
	UT_ASSERT_EQ((int)result, (int)CLUSTER_LMD_WAIT_STATE_READ_BUSY);

	pg_atomic_write_u32(&ws.change_seq, stable_seq + 2U);
	result = cluster_lmd_wait_state_read_exact(&ws, &snap);
	UT_ASSERT_EQ((int)result, (int)CLUSTER_LMD_WAIT_STATE_READ_ACTIVE);
	UT_ASSERT(snap.active);
	UT_ASSERT_EQ((int)snap.kind, (int)CLUSTER_LMD_WAIT_TX);
	UT_ASSERT_EQ((int)snap.request_id, 29);
	UT_ASSERT_EQ((int)snap.cluster_epoch, 28);
	UT_ASSERT_EQ((int)snap.xid, 27);
}


int
main(void)
{
	UT_PLAN(15);
	UT_RUN(test_publish_records_tuple_and_active);
	UT_RUN(test_clear_marks_inactive);
	UT_RUN(test_wait_seq_monotonic);
	UT_RUN(test_reset_preserves_wait_seq);
	UT_RUN(test_read_fresh_is_inactive);
	UT_RUN(test_publish_return_matches_read);
	UT_RUN(test_uncleared_publish_stays_active);
	UT_RUN(test_clear_republish_snapshot_is_one_generation);
	UT_RUN(test_writer_in_progress_fails_closed);
	UT_RUN(test_pcm_convert_kind_records_complete_tuple);
	UT_RUN(test_wait_state_layout_stays_48_bytes);
	UT_RUN(test_exact_read_reports_stable_active);
	UT_RUN(test_exact_read_reports_stable_inactive);
	UT_RUN(test_exact_read_reports_busy_on_odd_exhaustion);
	UT_RUN(test_exact_read_recovers_after_writer_closes);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
