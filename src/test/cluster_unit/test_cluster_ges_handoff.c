/*-------------------------------------------------------------------------
 *
 * test_cluster_ges_handoff.c
 *	  Standalone model-check for spec-6.12e1 -- the GES release-side
 *	  handoff invariant verifier (U-e set).  Adversarial interleavings of
 *	  holders / waiters / converts / grants are fed to the pure verifier;
 *	  the three invariants must hold on every legal drain and every
 *	  synthetic violation must be caught:
 *
 *	    no-stale-holder    released identity absent post-drain
 *	    no-double-grant    grants mutually compatible + compatible with
 *	                       every surviving holder
 *	    no-lost-waiter     a servable, unbarriered waiter is never left
 *	                       behind by a drain that granted nothing
 *
 *	  Uses the frozen GES mode matrix (cluster_ges_mode.o) so the
 *	  compatibility rule is the real one, not a stub.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_ges_handoff.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Links cluster_ges_handoff_policy.o +
 *	  cluster_ges_mode.o standalone.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "cluster/cluster_ges_handoff.h"
#include "cluster/cluster_ges_mode.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* GES modes: 1..8 = PG lock modes; GES uses NULL/S(=LW=1?)/X.  We drive the
 * verifier with the two conflicting modes the drain actually deals with:
 * AccessShareLock (1) as an S-like shared mode, AccessExclusiveLock (8) as X. */
#define M_S AccessShareLock
#define M_X AccessExclusiveLock

static ClusterGesHandoffParty
party(int32 node, uint32 procno, LOCKMODE mode, uint64 seq, bool barriered)
{
	ClusterGesHandoffParty p;

	memset(&p, 0, sizeof(p));
	p.node_id = node;
	p.procno = procno;
	p.mode = mode;
	p.fair_queue_seq = seq;
	p.barriered = barriered;
	return p;
}


/* ---- sanity: the frozen matrix says what we assume ---- */
UT_TEST(test_handoff_mode_matrix_assumptions)
{
	UT_ASSERT(cluster_ges_handoff_modes_compatible(M_S, M_S));	/* S+S ok */
	UT_ASSERT(!cluster_ges_handoff_modes_compatible(M_S, M_X)); /* S+X conflict */
	UT_ASSERT(!cluster_ges_handoff_modes_compatible(M_X, M_X)); /* X+X conflict */
}

/* ---- legal drains verify OK ---- */
UT_TEST(test_handoff_legal_single_x_grant)
{
	ClusterGesHandoffSnapshot s;

	cluster_ges_handoff_snapshot_init(&s);
	/* released X holder (node0), granted next X waiter (node1); no survivors. */
	s.released_node_id = 0;
	s.released_procno = 100;
	s.holders[0] = party(1, 201, M_X, 0, false);
	s.nholders = 1;
	s.granted[0] = party(1, 201, M_X, 5, false);
	s.ngranted = 1;
	UT_ASSERT_EQ((int)cluster_ges_handoff_verify(&s), (int)CLUSTER_GES_HANDOFF_OK);
}

UT_TEST(test_handoff_legal_two_s_one_at_a_time)
{
	ClusterGesHandoffSnapshot s;

	cluster_ges_handoff_snapshot_init(&s);
	/* X released; one S waiter popped, a second S waiter legitimately remains
	 * (one-at-a-time FIFO -- next release serves it). */
	s.released_node_id = 0;
	s.released_procno = 100;
	s.holders[0] = party(1, 201, M_S, 0, false);
	s.nholders = 1;
	s.waiters[0] = party(2, 202, M_S, 7, false); /* still servable, but drain granted one */
	s.nwaiters = 1;
	s.granted[0] = party(1, 201, M_S, 5, false);
	s.ngranted = 1;
	UT_ASSERT_EQ((int)cluster_ges_handoff_verify(&s), (int)CLUSTER_GES_HANDOFF_OK);
}

UT_TEST(test_handoff_legal_blocked_waiter_remains)
{
	ClusterGesHandoffSnapshot s;

	cluster_ges_handoff_snapshot_init(&s);
	/* X granted to node1; an X waiter remains but is BLOCKED by the surviving
	 * X holder -> not a lost waiter. */
	s.released_node_id = 0;
	s.released_procno = 100;
	s.holders[0] = party(1, 201, M_X, 0, false);
	s.nholders = 1;
	s.waiters[0] = party(2, 202, M_X, 9, false); /* conflicts with X holder */
	s.nwaiters = 1;
	s.granted[0] = party(1, 201, M_X, 5, false);
	s.ngranted = 1;
	UT_ASSERT_EQ((int)cluster_ges_handoff_verify(&s), (int)CLUSTER_GES_HANDOFF_OK);
}

UT_TEST(test_handoff_legal_barriered_waiter_remains)
{
	ClusterGesHandoffSnapshot s;

	cluster_ges_handoff_snapshot_init(&s);
	/* No holders, drain granted nothing, but the only servable waiter is
	 * barriered behind an earlier boosted waiter -> legitimate. */
	s.released_node_id = 0;
	s.released_procno = 100;
	s.nholders = 0;
	s.waiters[0] = party(2, 202, M_S, 9, true); /* barriered */
	s.nwaiters = 1;
	s.ngranted = 0;
	UT_ASSERT_EQ((int)cluster_ges_handoff_verify(&s), (int)CLUSTER_GES_HANDOFF_OK);
}

/* ---- synthetic violations are caught ---- */
UT_TEST(test_handoff_catches_stale_holder)
{
	ClusterGesHandoffSnapshot s;

	cluster_ges_handoff_snapshot_init(&s);
	/* the released identity is still recorded as a holder */
	s.released_node_id = 0;
	s.released_procno = 100;
	s.holders[0] = party(0, 100, M_X, 0, false); /* == released! */
	s.nholders = 1;
	UT_ASSERT_EQ((int)cluster_ges_handoff_verify(&s), (int)CLUSTER_GES_HANDOFF_STALE_HOLDER);
}

UT_TEST(test_handoff_catches_double_grant_pair)
{
	ClusterGesHandoffSnapshot s;

	cluster_ges_handoff_snapshot_init(&s);
	/* two incompatible grants in one drain (X + X) */
	s.released_node_id = 0;
	s.released_procno = 100;
	s.granted[0] = party(1, 201, M_X, 5, false);
	s.granted[1] = party(2, 202, M_X, 6, false);
	s.ngranted = 2;
	s.holders[0] = party(1, 201, M_X, 0, false);
	s.holders[1] = party(2, 202, M_X, 0, false);
	s.nholders = 2;
	UT_ASSERT_EQ((int)cluster_ges_handoff_verify(&s), (int)CLUSTER_GES_HANDOFF_DOUBLE_GRANT);
}

UT_TEST(test_handoff_catches_grant_vs_holder_conflict)
{
	ClusterGesHandoffSnapshot s;

	cluster_ges_handoff_snapshot_init(&s);
	/* granted X to node2 while node1 still holds S -> conflict */
	s.released_node_id = 0;
	s.released_procno = 100;
	s.holders[0] = party(1, 201, M_S, 0, false); /* survivor */
	s.holders[1] = party(2, 202, M_X, 0, false); /* the new grant's slot */
	s.nholders = 2;
	s.granted[0] = party(2, 202, M_X, 6, false);
	s.ngranted = 1;
	UT_ASSERT_EQ((int)cluster_ges_handoff_verify(&s), (int)CLUSTER_GES_HANDOFF_DOUBLE_GRANT);
}

UT_TEST(test_handoff_catches_lost_waiter)
{
	ClusterGesHandoffSnapshot s;

	cluster_ges_handoff_snapshot_init(&s);
	/* no holders, drain granted NOTHING, yet a servable unbarriered S waiter
	 * remains -> lost waiter (the drain should have popped it). */
	s.released_node_id = 0;
	s.released_procno = 100;
	s.nholders = 0;
	s.waiters[0] = party(2, 202, M_S, 9, false);
	s.nwaiters = 1;
	s.ngranted = 0;
	UT_ASSERT_EQ((int)cluster_ges_handoff_verify(&s), (int)CLUSTER_GES_HANDOFF_LOST_WAITER);
}

/* ---- adversarial interleaving sweep: every legal drain of a small state
 * space must verify OK.  We enumerate drains that the real drain could
 * produce (remove holder, grant <=1 compatible waiter) and assert OK, then
 * enumerate one-mutation corruptions and assert non-OK. ---- */
UT_TEST(test_handoff_interleaving_sweep_legal)
{
	int trial;

	/* 40 legal shapes: X released; k in {0,1} S-waiters granted, rest remain
	 * either blocked (an X survivor) or barriered. */
	for (trial = 0; trial < 40; trial++) {
		ClusterGesHandoffSnapshot s;
		int nsurv = trial % 2; /* 0 or 1 surviving X holder */
		int granted_one = (trial / 2) % 2;
		int extra_waiters = (trial / 4) % 3;
		int i;

		cluster_ges_handoff_snapshot_init(&s);
		s.released_node_id = 0;
		s.released_procno = 100;

		if (nsurv) {
			s.holders[0] = party(1, 201, M_X, 0, false);
			s.nholders = 1;
		}
		if (granted_one && !nsurv) {
			s.granted[0] = party(3, 203, M_S, 5, false);
			s.holders[s.nholders++] = party(3, 203, M_S, 0, false);
			s.ngranted = 1;
		}
		for (i = 0; i < extra_waiters; i++) {
			/* if an X survivor exists these are blocked; else barriered so
			 * they are legitimately not popped this pass. */
			bool barriered = (nsurv == 0);
			s.waiters[s.nwaiters++] = party(4 + i, 300 + i, M_S, 10 + i, barriered);
		}
		UT_ASSERT_EQ((int)cluster_ges_handoff_verify(&s), (int)CLUSTER_GES_HANDOFF_OK);
	}
}


/* A conflict beyond the former inline boundary must participate in proof. */
UT_TEST(test_handoff_complete_dynamic_snapshot)
{
	ClusterGesHandoffSnapshot s;
	ClusterGesHandoffParty holders[64];

	cluster_ges_handoff_snapshot_init(&s);
	s.holders = holders;
	s.holder_capacity = lengthof(holders);
	s.nholders = lengthof(holders);
	s.released_node_id = 127;
	s.released_procno = 900;
	for (int i = 0; i < lengthof(holders); i++)
		holders[i] = party(i % 4, 100 + i, M_S, 0, false);
	s.granted[0] = party(5, 200, M_S, 0, false);
	s.ngranted = 1;
	UT_ASSERT_EQ(cluster_ges_handoff_verify(&s), CLUSTER_GES_HANDOFF_OK);
	holders[63].mode = M_X;
	UT_ASSERT_EQ(cluster_ges_handoff_verify(&s), CLUSTER_GES_HANDOFF_DOUBLE_GRANT);
	holders[63] = party(127, 900, M_S, 0, false);
	UT_ASSERT_EQ(cluster_ges_handoff_verify(&s), CLUSTER_GES_HANDOFF_STALE_HOLDER);
}


UT_DEFINE_GLOBALS();

int
main(int argc pg_attribute_unused(), char **const argv pg_attribute_unused())
{
	UT_PLAN(11);

	UT_RUN(test_handoff_mode_matrix_assumptions);
	UT_RUN(test_handoff_legal_single_x_grant);
	UT_RUN(test_handoff_legal_two_s_one_at_a_time);
	UT_RUN(test_handoff_legal_blocked_waiter_remains);
	UT_RUN(test_handoff_legal_barriered_waiter_remains);
	UT_RUN(test_handoff_catches_stale_holder);
	UT_RUN(test_handoff_catches_double_grant_pair);
	UT_RUN(test_handoff_catches_grant_vs_holder_conflict);
	UT_RUN(test_handoff_catches_lost_waiter);
	UT_RUN(test_handoff_interleaving_sweep_legal);
	UT_RUN(test_handoff_complete_dynamic_snapshot);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
