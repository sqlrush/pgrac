/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_authority.c
 *	  Unit tests for the dead/absent-owner undo serve-authority election
 *	  pure layer (spec-5.22d D4-1).
 *
 *	  Covers the pure decision core cluster_undo_authority_decide(): the
 *	  deterministic lowest-fresh-survivor election, and every fail-closed
 *	  edge (epoch mismatch, owner-live short-circuit, owner-undecided,
 *	  no-survivor, malformed owner).  Election NEVER returns a node that
 *	  is not both declared and fresh-alive (the "no ad-hoc / no hash
 *	  fallback" property, spec-5.22d §3.3 约束 #2 at the derivation layer).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_undo_authority.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.22d-undo-dead-owner-verdict-serve.md (D4-1, §4.1)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_undo_authority.h"

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

/* ---- bitmap helpers (mirror the on-wire 128-bit membership shape) ---- */
static void
bm_set(uint8 *bm, int node)
{
	bm[node >> 3] |= (uint8)(1u << (node & 7));
}

/*
 * Build a canonical input: declared = {0..n_declared-1}; alive_fresh and
 * dead_decided start empty; epochs both = epoch.  Callers then mark the
 * fresh / dead members they want.
 */
static void
input_init(ClusterUndoAuthorityInput *in, int32 owner, uint64 epoch, int n_declared)
{
	int i;

	memset(in, 0, sizeof(*in));
	in->owner_node = owner;
	in->request_epoch = epoch;
	in->snapshot_epoch = epoch;
	for (i = 0; i < n_declared; i++)
		bm_set(in->declared, i);
}

/* ======================================================================
 * U1 -- dead owner elects the lowest fresh-alive survivor
 * ====================================================================== */
UT_TEST(test_authority_dead_owner_elects_lowest_survivor)
{
	ClusterUndoAuthorityInput in;
	ClusterUndoAuthorityDecision out;

	/* declared {0,1,2,3}; owner 0 dead-decided; 1,2,3 fresh-alive */
	input_init(&in, 0 /*owner*/, 42 /*epoch*/, 4);
	bm_set(in.dead_decided, 0);
	bm_set(in.alive_fresh, 1);
	bm_set(in.alive_fresh, 2);
	bm_set(in.alive_fresh, 3);

	UT_ASSERT_EQ(cluster_undo_authority_decide(&in, &out), CLUSTER_UNDO_AUTHORITY_OK);
	UT_ASSERT_EQ(out.status, CLUSTER_UNDO_AUTHORITY_OK);
	UT_ASSERT_EQ(out.authority_node, 1); /* lowest fresh survivor */
}

/* ======================================================================
 * U2 -- owner is fresh-alive: no election, stay on live-owner (D6) path
 * ====================================================================== */
UT_TEST(test_authority_owner_live_no_election)
{
	ClusterUndoAuthorityInput in;
	ClusterUndoAuthorityDecision out;

	input_init(&in, 2 /*owner*/, 42, 4);
	bm_set(in.alive_fresh, 1);
	bm_set(in.alive_fresh, 2); /* owner itself fresh */
	bm_set(in.alive_fresh, 3);

	UT_ASSERT_EQ(cluster_undo_authority_decide(&in, &out), CLUSTER_UNDO_AUTHORITY_OWNER_LIVE);
	UT_ASSERT_EQ(out.authority_node, -1);
}

/* ======================================================================
 * U7 -- owner neither fresh-alive nor dead-decided: 生死未定 => UNKNOWN
 * (fail closed; NEVER guess dead and elect an authority)
 * ====================================================================== */
UT_TEST(test_authority_owner_undecided_unknown)
{
	ClusterUndoAuthorityInput in;
	ClusterUndoAuthorityDecision out;

	input_init(&in, 0 /*owner*/, 42, 4);
	/* owner 0 NOT in alive_fresh and NOT in dead_decided */
	bm_set(in.alive_fresh, 1);
	bm_set(in.alive_fresh, 2);

	UT_ASSERT_EQ(cluster_undo_authority_decide(&in, &out), CLUSTER_UNDO_AUTHORITY_UNKNOWN);
	UT_ASSERT_EQ(out.authority_node, -1);
}

/* ======================================================================
 * U1b -- epoch mismatch: caller's scope is stale => RECOVERING
 * ====================================================================== */
UT_TEST(test_authority_epoch_mismatch_recovering)
{
	ClusterUndoAuthorityInput in;
	ClusterUndoAuthorityDecision out;

	input_init(&in, 0, 42, 4);
	bm_set(in.dead_decided, 0);
	bm_set(in.alive_fresh, 1);
	in.snapshot_epoch = 43; /* moved on */

	UT_ASSERT_EQ(cluster_undo_authority_decide(&in, &out), CLUSTER_UNDO_AUTHORITY_RECOVERING);
	UT_ASSERT_EQ(out.authority_node, -1);
}

/* ======================================================================
 * U1c -- dead owner but no fresh survivor exists => RECOVERING
 * (never elect a non-fresh node; no hash / ad-hoc fallback)
 * ====================================================================== */
UT_TEST(test_authority_no_fresh_survivor_recovering)
{
	ClusterUndoAuthorityInput in;
	ClusterUndoAuthorityDecision out;

	input_init(&in, 0, 42, 4);
	bm_set(in.dead_decided, 0);
	/* declared {0,1,2,3} but NONE fresh-alive */

	UT_ASSERT_EQ(cluster_undo_authority_decide(&in, &out), CLUSTER_UNDO_AUTHORITY_RECOVERING);
	UT_ASSERT_EQ(out.authority_node, -1);
}

/* ======================================================================
 * U1d -- lowest survivor skips a lower non-fresh (dead) node; the dead
 * owner is itself excluded from survivors even if it would be lowest
 * ====================================================================== */
UT_TEST(test_authority_skips_lower_nonfresh)
{
	ClusterUndoAuthorityInput in;
	ClusterUndoAuthorityDecision out;

	/* declared {0,1,2,3}; owner 0 dead; node 1 also NOT fresh (down);
	 * node 2,3 fresh -> authority must be 2, not 0 or 1 */
	input_init(&in, 0, 42, 4);
	bm_set(in.dead_decided, 0);
	bm_set(in.alive_fresh, 2);
	bm_set(in.alive_fresh, 3);

	UT_ASSERT_EQ(cluster_undo_authority_decide(&in, &out), CLUSTER_UNDO_AUTHORITY_OK);
	UT_ASSERT_EQ(out.authority_node, 2);
}

/* ======================================================================
 * U1e -- authority must be a DECLARED node: a fresh bit outside the
 * declared set is never elected
 * ====================================================================== */
UT_TEST(test_authority_requires_declared)
{
	ClusterUndoAuthorityInput in;
	ClusterUndoAuthorityDecision out;

	/* declared {0,1}; owner 0 dead; node 1 NOT fresh; node 5 fresh but
	 * NOT declared -> no eligible survivor -> RECOVERING */
	input_init(&in, 0, 42, 2);
	bm_set(in.dead_decided, 0);
	bm_set(in.alive_fresh, 5); /* undeclared */

	UT_ASSERT_EQ(cluster_undo_authority_decide(&in, &out), CLUSTER_UNDO_AUTHORITY_RECOVERING);
	UT_ASSERT_EQ(out.authority_node, -1);
}

/* ======================================================================
 * U1f -- malformed owner (out of [0, SCN_MAX_VALID_NODE_ID]) => UNKNOWN
 * ====================================================================== */
UT_TEST(test_authority_invalid_owner_unknown)
{
	ClusterUndoAuthorityInput in;
	ClusterUndoAuthorityDecision out;

	input_init(&in, -1 /*owner*/, 42, 4);
	bm_set(in.alive_fresh, 1);
	UT_ASSERT_EQ(cluster_undo_authority_decide(&in, &out), CLUSTER_UNDO_AUTHORITY_UNKNOWN);
	UT_ASSERT_EQ(out.authority_node, -1);

	input_init(&in, 128 /*owner > 127*/, 42, 4);
	bm_set(in.alive_fresh, 1);
	UT_ASSERT_EQ(cluster_undo_authority_decide(&in, &out), CLUSTER_UNDO_AUTHORITY_UNKNOWN);
	UT_ASSERT_EQ(out.authority_node, -1);
}

/* ======================================================================
 * D4-2 route mapper (U3 family) -- identity(owner) vs destination(authority)
 * separation; a fail-closed status NEVER yields a node (no hash fallback).
 * ====================================================================== */

/* U3a -- OWNER_LIVE routes to the owner itself (live-owner / D6 path) */
UT_TEST(test_route_owner_live_routes_owner)
{
	ClusterUndoServeRoute r
		= cluster_undo_route_decide(2 /*owner*/, 42, CLUSTER_UNDO_AUTHORITY_OWNER_LIVE, -1);

	UT_ASSERT_EQ(r.status, CLUSTER_UNDO_AUTHORITY_OWNER_LIVE);
	UT_ASSERT_EQ(r.destination_node, 2);
	UT_ASSERT_EQ((int)r.reconfig_epoch, 42);
}

/* U3b -- OK routes to the elected authority, NOT the (dead) owner */
UT_TEST(test_route_ok_routes_authority)
{
	ClusterUndoServeRoute r = cluster_undo_route_decide(0 /*dead owner*/, 42,
														CLUSTER_UNDO_AUTHORITY_OK, 3 /*authority*/);

	UT_ASSERT_EQ(r.status, CLUSTER_UNDO_AUTHORITY_OK);
	UT_ASSERT_EQ(r.destination_node, 3);
}

/* U3c -- RECOVERING fails closed to -1; NEVER a node / hash master (约束 #2) */
UT_TEST(test_route_recovering_failclosed_not_node)
{
	ClusterUndoServeRoute r
		= cluster_undo_route_decide(0, 42, CLUSTER_UNDO_AUTHORITY_RECOVERING, -1);

	UT_ASSERT_EQ(r.status, CLUSTER_UNDO_AUTHORITY_RECOVERING);
	UT_ASSERT_EQ(r.destination_node, -1);
}

/* U3d -- UNKNOWN fails closed to -1 (no ad-hoc / hash fallback) */
UT_TEST(test_route_unknown_failclosed)
{
	ClusterUndoServeRoute r = cluster_undo_route_decide(0, 42, CLUSTER_UNDO_AUTHORITY_UNKNOWN, -1);

	UT_ASSERT_EQ(r.status, CLUSTER_UNDO_AUTHORITY_UNKNOWN);
	UT_ASSERT_EQ(r.destination_node, -1);
}

/* ======================================================================
 * U5 -- coverage predicate (spec-5.22d D4-4, §2.4 约束 #3): the authority
 * block0 serve is admissible ONLY under the three-way AND
 *   claimed-at-epoch ∧ block0-readable ∧ wrap-match.
 * Each term singly false must fail the whole predicate (three negative
 * sub-cases) -- a future edit cannot silently drop one term.
 * ====================================================================== */
UT_TEST(test_coverage_three_way_and)
{
	/* all three proven -> covered */
	UT_ASSERT(cluster_undo_authority_coverage_ok(true, true, true));

	/* (i) authority not claimed at the current epoch -> not covered */
	UT_ASSERT(!cluster_undo_authority_coverage_ok(false, true, true));

	/* (ii) shared block0 not readable -> not covered */
	UT_ASSERT(!cluster_undo_authority_coverage_ok(true, false, true));

	/* (iii) generation/wrap mismatch (ABA suspect) -> not covered */
	UT_ASSERT(!cluster_undo_authority_coverage_ok(true, true, false));
}

/* ======================================================================
 * D4-4 serve-decision mapper -- route -> consumer action.  OWNER_LIVE keeps
 * the D6 live-owner path; OK with destination == self serves the dead
 * owner's shared block0 locally; EVERYTHING else fails closed, including
 * an elected PEER authority (its wire serve lands with D4-5/D4-6 -- until
 * then routing a request there would be an unproven path).
 * ====================================================================== */

/* owner live -> stay on the live-owner (D6) path, never block0-serve */
UT_TEST(test_serve_decide_owner_live)
{
	ClusterUndoServeRoute r
		= cluster_undo_route_decide(2 /*owner*/, 42, CLUSTER_UNDO_AUTHORITY_OWNER_LIVE, -1);

	UT_ASSERT_EQ(cluster_undo_authority_serve_decide(&r, 1 /*self*/),
				 CLUSTER_UNDO_AUTHORITY_SERVE_OWNER_LIVE);
}

/* elected authority == self -> serve the dead owner's shared block0 */
UT_TEST(test_serve_decide_self_block0)
{
	ClusterUndoServeRoute r
		= cluster_undo_route_decide(0 /*dead owner*/, 42, CLUSTER_UNDO_AUTHORITY_OK, 1);

	UT_ASSERT_EQ(cluster_undo_authority_serve_decide(&r, 1 /*self*/),
				 CLUSTER_UNDO_AUTHORITY_SERVE_SELF_BLOCK0);
}

/* elected authority == a PEER -> fail closed (peer wire serve = D4-5/D4-6) */
UT_TEST(test_serve_decide_peer_failclosed)
{
	ClusterUndoServeRoute r
		= cluster_undo_route_decide(0 /*dead owner*/, 42, CLUSTER_UNDO_AUTHORITY_OK, 3);

	UT_ASSERT_EQ(cluster_undo_authority_serve_decide(&r, 1 /*self*/),
				 CLUSTER_UNDO_AUTHORITY_SERVE_FAIL_CLOSED);
}

/* RECOVERING / UNKNOWN -> fail closed (never native, never a guess) */
UT_TEST(test_serve_decide_recovering_unknown_failclosed)
{
	ClusterUndoServeRoute r
		= cluster_undo_route_decide(0, 42, CLUSTER_UNDO_AUTHORITY_RECOVERING, -1);

	UT_ASSERT_EQ(cluster_undo_authority_serve_decide(&r, 1),
				 CLUSTER_UNDO_AUTHORITY_SERVE_FAIL_CLOSED);

	r = cluster_undo_route_decide(0, 42, CLUSTER_UNDO_AUTHORITY_UNKNOWN, -1);
	UT_ASSERT_EQ(cluster_undo_authority_serve_decide(&r, 1),
				 CLUSTER_UNDO_AUTHORITY_SERVE_FAIL_CLOSED);
}

/* defense in depth: an OK route carrying an invalid destination, or an
 * invalid self node, can never yield a serve decision */
UT_TEST(test_serve_decide_invalid_dest_failclosed)
{
	ClusterUndoServeRoute r;

	/* malformed: OK status but destination -1 (cannot happen through
	 * route_decide; guard against a hand-rolled route) */
	r.status = CLUSTER_UNDO_AUTHORITY_OK;
	r.destination_node = -1;
	r.reconfig_epoch = 42;
	UT_ASSERT_EQ(cluster_undo_authority_serve_decide(&r, -1),
				 CLUSTER_UNDO_AUTHORITY_SERVE_FAIL_CLOSED);
	UT_ASSERT_EQ(cluster_undo_authority_serve_decide(&r, 1),
				 CLUSTER_UNDO_AUTHORITY_SERVE_FAIL_CLOSED);
}

int
main(void)
{
	UT_PLAN(18);
	UT_RUN(test_authority_dead_owner_elects_lowest_survivor);
	UT_RUN(test_authority_owner_live_no_election);
	UT_RUN(test_authority_owner_undecided_unknown);
	UT_RUN(test_authority_epoch_mismatch_recovering);
	UT_RUN(test_authority_no_fresh_survivor_recovering);
	UT_RUN(test_authority_skips_lower_nonfresh);
	UT_RUN(test_authority_requires_declared);
	UT_RUN(test_authority_invalid_owner_unknown);
	UT_RUN(test_route_owner_live_routes_owner);
	UT_RUN(test_route_ok_routes_authority);
	UT_RUN(test_route_recovering_failclosed_not_node);
	UT_RUN(test_route_unknown_failclosed);
	UT_RUN(test_coverage_three_way_and);
	UT_RUN(test_serve_decide_owner_live);
	UT_RUN(test_serve_decide_self_block0);
	UT_RUN(test_serve_decide_peer_failclosed);
	UT_RUN(test_serve_decide_recovering_unknown_failclosed);
	UT_RUN(test_serve_decide_invalid_dest_failclosed);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
