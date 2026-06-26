/*-------------------------------------------------------------------------
 *
 * test_cluster_cancel_token.c
 *	  Standalone unit tests for the per-proc deadlock-cancel token
 *	  (spec-5.9 D3) — the identity-matched cancel consume that replaces the
 *	  spec-2.24 global boolean so a stale / retransmitted signal that arrives
 *	  after slot (procno) reuse cannot kill the wrong transaction (the P0#1
 *	  Rule 8.A invariant).
 *
 *	  The tests exercise cluster_cancel_token_consume_against() directly with an
 *	  explicit wait-state snapshot, so no PGPROC / shmem machinery is required.
 *	  Coverage (U4):
 *
 *	    U4a: exact match (same request_id + cluster_epoch + wait_seq, current
 *	         epoch) -> honor (true) + CONSUMED marker carrying the cancel_id.
 *	    U4b: wait_seq mismatch (procno reuse ABA) -> NOT honored, STALE_CLEARED.
 *	    U4c: request_id mismatch -> NOT honored.
 *	    U4d: cluster_epoch mismatch -> NOT honored.
 *	    U4e: stale epoch (token epoch != current) -> NOT honored.
 *	    U4f: wait-state inactive (victim already left the wait) -> NOT honored.
 *	    U4g: not installed -> false, no marker change.
 *	    U4h: token is one-shot (installed cleared) + marker_seq monotonic.
 *
 *	  The cross-process install -> signal -> consume path and the marker ->
 *	  CANCEL_ACK bridge are covered end to end by the spec-5.9 TAP suite (D12).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cancel_token.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Standalone binary linking
 *	  cluster_cancel_token.o only; all PG backend symbols stubbed locally.
 *	  Spec: spec-5.9-deadlock-victim-policy-cancel-robustness.md.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <string.h>

#include "cluster/cluster_cancel_token.h"
#include "cluster/cluster_lmd_wait_state.h"

/* Drop PG's port.h printf override; unit_test.h uses stdlib printf. */
#ifdef vprintf
#undef vprintf
#endif
#ifdef printf
#undef printf
#endif

#include "unit_test.h"

/* ============================================================
 * Stubs for cluster_cancel_token.o's external references.  The match logic
 * under test (consume_against) only touches the spinlock + the counter incs;
 * the consume() wrapper's globals (MyProc / flag / wait-state read / epoch) are
 * never exercised here but must resolve at link time.
 * ============================================================ */

uint64 stub_installed_inc = 0;
uint64 stub_consumed_inc = 0;
uint64 stub_stale_cleared_inc = 0;

void
cluster_lmd_cancel_token_installed_count_inc(uint64 delta)
{
	stub_installed_inc += delta;
}
void
cluster_lmd_cancel_consumed_count_inc(uint64 delta)
{
	stub_consumed_inc += delta;
}
void
cluster_lmd_cancel_stale_cleared_count_inc(uint64 delta)
{
	stub_stale_cleared_inc += delta;
}

uint64 stub_protected_skip_inc = 0;
void
cluster_lmd_victim_protected_skip_count_inc(uint64 delta)
{
	stub_protected_skip_inc += delta;
}

/* RecoveryInProgress — consume()'s HARD-skip predicate; never reached via
 * consume_against (which takes hard_skip explicitly). */
bool
RecoveryInProgress(void)
{
	return false;
}

/* consume() wrapper deps — never called from these tests. */
struct PGPROC *MyProc = NULL;
volatile sig_atomic_t cluster_ges_cancel_pending = false;

bool
cluster_lmd_wait_state_read(ClusterLmdProcWaitState *ws pg_attribute_unused(),
							ClusterLmdWaitStateSnapshot *out pg_attribute_unused())
{
	return false;
}

uint64
cluster_epoch_get_current(void)
{
	return 1;
}

/* Spinlock contention path — never reached single-threaded; link stub only. */
int
s_lock(volatile slock_t *lock pg_attribute_unused(), const char *file pg_attribute_unused(),
	   int line pg_attribute_unused(), const char *func pg_attribute_unused())
{
	return 0;
}

/* ============================================================
 * Test helpers.
 * ============================================================ */

#define R_ID 4242u
#define EPOCH 7u
#define W_SEQ 99u
#define C_ID 555u

/* A token freshly "installed" by the LMD (simulating cluster_cancel_token_install). */
static void
make_installed_token(ClusterCancelToken *tok)
{
	cluster_cancel_token_init(tok);
	tok->installed = true;
	tok->request_id = R_ID;
	tok->cluster_epoch = EPOCH;
	tok->wait_seq = W_SEQ;
	tok->cancel_id = C_ID;
}

/* The victim's live wait-state — by default matches the installed token. */
static void
make_matching_ws(ClusterLmdWaitStateSnapshot *ws)
{
	memset(ws, 0, sizeof(*ws));
	ws->active = true;
	ws->kind = CLUSTER_LMD_WAIT_GES;
	ws->request_id = R_ID;
	ws->cluster_epoch = EPOCH;
	ws->wait_seq = W_SEQ;
}

/* ============================================================
 * Tests.
 * ============================================================ */

UT_TEST(test_cancel_token_exact_match_honors)
{
	ClusterCancelToken tok;
	ClusterLmdWaitStateSnapshot ws;

	make_installed_token(&tok);
	make_matching_ws(&ws);

	UT_ASSERT(cluster_cancel_token_consume_against(&tok, &ws, EPOCH, false));
	UT_ASSERT_EQ((int)tok.marker_status, (int)CLUSTER_CANCEL_MARKER_CONSUMED);
	UT_ASSERT_EQ((int)tok.marker_cancel_id, (int)C_ID);
	UT_ASSERT(!tok.installed); /* one-shot */
}

UT_TEST(test_cancel_token_wait_seq_mismatch_no_misfire)
{
	ClusterCancelToken tok;
	ClusterLmdWaitStateSnapshot ws;

	/* procno reuse ABA: same 4-tuple but a fresh wait_seq -> must NOT honor. */
	make_installed_token(&tok);
	make_matching_ws(&ws);
	ws.wait_seq = W_SEQ + 1;

	UT_ASSERT(!cluster_cancel_token_consume_against(&tok, &ws, EPOCH, false));
	UT_ASSERT_EQ((int)tok.marker_status, (int)CLUSTER_CANCEL_MARKER_STALE_CLEARED);
	UT_ASSERT(!tok.installed);
}

UT_TEST(test_cancel_token_request_id_mismatch_no_misfire)
{
	ClusterCancelToken tok;
	ClusterLmdWaitStateSnapshot ws;

	make_installed_token(&tok);
	make_matching_ws(&ws);
	ws.request_id = R_ID + 1;

	UT_ASSERT(!cluster_cancel_token_consume_against(&tok, &ws, EPOCH, false));
	UT_ASSERT_EQ((int)tok.marker_status, (int)CLUSTER_CANCEL_MARKER_STALE_CLEARED);
}

UT_TEST(test_cancel_token_epoch_mismatch_no_misfire)
{
	ClusterCancelToken tok;
	ClusterLmdWaitStateSnapshot ws;

	make_installed_token(&tok);
	make_matching_ws(&ws);
	ws.cluster_epoch = EPOCH + 1;

	UT_ASSERT(!cluster_cancel_token_consume_against(&tok, &ws, EPOCH, false));
	UT_ASSERT_EQ((int)tok.marker_status, (int)CLUSTER_CANCEL_MARKER_STALE_CLEARED);
}

UT_TEST(test_cancel_token_stale_epoch_no_misfire)
{
	ClusterCancelToken tok;
	ClusterLmdWaitStateSnapshot ws;

	/* Token + wait-state agree on EPOCH, but the cluster has moved past it. */
	make_installed_token(&tok);
	make_matching_ws(&ws);

	UT_ASSERT(!cluster_cancel_token_consume_against(&tok, &ws, EPOCH + 1, false)); /* stale epoch */
	UT_ASSERT_EQ((int)tok.marker_status, (int)CLUSTER_CANCEL_MARKER_STALE_CLEARED);
}

UT_TEST(test_cancel_token_inactive_wait_no_misfire)
{
	ClusterCancelToken tok;
	ClusterLmdWaitStateSnapshot ws;

	/* Victim already left the wait (e.g. granted) -> stale signal must not kill
	 * the next acquire (the spec-5.9 F0-6 fix). */
	make_installed_token(&tok);
	make_matching_ws(&ws);
	ws.active = false;

	UT_ASSERT(!cluster_cancel_token_consume_against(&tok, &ws, EPOCH, false));
	UT_ASSERT_EQ((int)tok.marker_status, (int)CLUSTER_CANCEL_MARKER_STALE_CLEARED);
}

UT_TEST(test_cancel_token_not_installed_is_noop)
{
	ClusterCancelToken tok;
	ClusterLmdWaitStateSnapshot ws;

	cluster_cancel_token_init(&tok); /* installed == false */
	make_matching_ws(&ws);

	UT_ASSERT(!cluster_cancel_token_consume_against(&tok, &ws, EPOCH, false));
	/* No token -> no marker stamped. */
	UT_ASSERT_EQ((int)tok.marker_status, (int)CLUSTER_CANCEL_MARKER_NONE);
}

UT_TEST(test_cancel_token_one_shot_and_marker_seq_monotonic)
{
	ClusterCancelToken tok;
	ClusterLmdWaitStateSnapshot ws;
	uint64 seq1;

	make_installed_token(&tok);
	make_matching_ws(&ws);

	UT_ASSERT(cluster_cancel_token_consume_against(&tok, &ws, EPOCH, false));
	seq1 = tok.marker_seq;

	/* Second consume of the now-cleared token is a no-op (installed false): no
	 * second honor and the marker_seq does not advance again. */
	UT_ASSERT(!cluster_cancel_token_consume_against(&tok, &ws, EPOCH, false));
	UT_ASSERT_EQ((int)tok.marker_seq, (int)seq1);

	/* Re-install + consume bumps the seq again (monotonic). */
	make_installed_token(&tok);
	tok.marker_seq = seq1;
	UT_ASSERT(cluster_cancel_token_consume_against(&tok, &ws, EPOCH, false));
	UT_ASSERT(tok.marker_seq > seq1);
}


/*
 * spec-5.9 D7 (U2) — a matching token on a HARD-skip (unsafe-to-cancel) victim
 * is NOT honored: refuse (return false) + stamp a PROTECTED marker so the
 * coordinator escalates to an alternate instead of force-killing it (Rule 8.A).
 */
UT_TEST(test_cancel_token_hard_skip_protected)
{
	ClusterCancelToken tok;
	ClusterLmdWaitStateSnapshot ws;

	make_installed_token(&tok);
	make_matching_ws(&ws);

	UT_ASSERT(!cluster_cancel_token_consume_against(&tok, &ws, EPOCH, true));
	UT_ASSERT_EQ((int)tok.marker_status, (int)CLUSTER_CANCEL_MARKER_PROTECTED);
	UT_ASSERT(!tok.installed);
}


UT_DEFINE_GLOBALS();


int
main(int argc pg_attribute_unused(), char *argv[] pg_attribute_unused())
{
	UT_PLAN(9);

	UT_RUN(test_cancel_token_exact_match_honors);
	UT_RUN(test_cancel_token_wait_seq_mismatch_no_misfire);
	UT_RUN(test_cancel_token_request_id_mismatch_no_misfire);
	UT_RUN(test_cancel_token_epoch_mismatch_no_misfire);
	UT_RUN(test_cancel_token_stale_epoch_no_misfire);
	UT_RUN(test_cancel_token_inactive_wait_no_misfire);
	UT_RUN(test_cancel_token_not_installed_is_noop);
	UT_RUN(test_cancel_token_one_shot_and_marker_seq_monotonic);
	UT_RUN(test_cancel_token_hard_skip_protected);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
