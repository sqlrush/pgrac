/*-------------------------------------------------------------------------
 *
 * test_cluster_membership.c
 *	  spec-5.15 D9 unit tests — cluster_membership foundation slice.
 *
 *	  Covers the dependency-free foundation of spec-5.15 (the incarnation
 *	  monotonic guard + membership-state decision SSOT).  These are the
 *	  rule-8.A correctness primitives; the join-edge / two-phase publish /
 *	  joiner-protocol deliverables are gated on spec-5.13 ship and are not
 *	  exercised here.
 *
 *	  Cases (spec-5.15 §4.1):
 *	    U1   vet: presented >  last_admitted            -> ACCEPT
 *	    U2   vet: presented == last_admitted            -> REJECT_STALE
 *	    U3   vet: presented <  last_admitted            -> REJECT_STALE
 *	    U4   vet: node_id out of range                  -> REJECT_NOT_READY
 *	    U4b  vet: uint64 values > 2^32 are NOT truncated (P1a)
 *	    U5   record_admitted is monotonic non-regressing
 *	    U11  is_member keys off membership_state, not CSSD
 *	    Ux   accessors are range-defensive (no OOB read)
 *
 *	  Pure layer: cluster_membership.o has no PG-backend dependencies, so
 *	  nothing is stubbed beyond the Assert ExceptionalCondition hook.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.15-online-declared-node-join-membership.md (FROZEN v1.0)
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_membership.c
 *
 * NOTES
 *	  pgrac-original file.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "cluster/cluster_membership.h"
#include "cluster/cluster_conf.h" /* CLUSTER_MAX_NODES */

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

/*
 * Stub for the quorum sub-gate.  cluster_membership_vet_joiner consults
 * cluster_qvotec_in_quorum() (-> REJECT_QUORUM) before the incarnation check;
 * the pure layer controls it via this flag.  Defaults to in-quorum so the
 * monotonic-incarnation cases (U1-U5) reach the floor compare.
 */
static bool test_in_quorum = true;

bool
cluster_qvotec_in_quorum(void)
{
	return test_in_quorum;
}

/*
 * A file-static backing table for the marker-seed tests (U10/U13/U15).  They
 * attach a fresh table to simulate a restart; it must NOT be a stack local
 * (the accessors keep the pointer after the test returns).  build a COMMITTED /
 * PREPARE marker with a valid CRC.
 */
static ClusterMembershipTable seed_tab;

static void
make_marker(ClusterJoinCommitMarker *m, int32 node, uint8 phase, uint64 incarnation, uint64 epoch)
{
	memset(m, 0, sizeof(*m));
	m->magic = CLUSTER_JCMK_MAGIC;
	m->version = CLUSTER_JCMK_VERSION;
	m->node_id = node;
	m->phase = phase;
	m->generation = 1;
	m->admitted_incarnation = incarnation;
	m->admitted_epoch = epoch;
	cluster_join_marker_compute_crc(m);
}

/* ======================================================================
 * U1 -- a strictly-fresher incarnation is admitted
 * ====================================================================== */
UT_TEST(test_vet_fresh_above_accept)
{
	cluster_membership_record_admitted(3, 5);
	UT_ASSERT_EQ(cluster_membership_vet_joiner(3, 6, 1), CLUSTER_JOIN_ACCEPT);
}

/* ======================================================================
 * U2 -- an equal incarnation is a stale rejoin (INV-J1, <= is closed)
 * ====================================================================== */
UT_TEST(test_vet_equal_reject_stale)
{
	cluster_membership_record_admitted(4, 5);
	UT_ASSERT_EQ(cluster_membership_vet_joiner(4, 5, 1), CLUSTER_JOIN_REJECT_STALE_INCARNATION);
}

/* ======================================================================
 * U3 -- a lower incarnation is a stale rejoin
 * ====================================================================== */
UT_TEST(test_vet_below_reject_stale)
{
	cluster_membership_record_admitted(5, 9);
	UT_ASSERT_EQ(cluster_membership_vet_joiner(5, 4, 1), CLUSTER_JOIN_REJECT_STALE_INCARNATION);
}

/* ======================================================================
 * U4 -- an out-of-range node_id is never admitted (fail-closed defense)
 * ====================================================================== */
UT_TEST(test_vet_node_id_out_of_range_failclosed)
{
	UT_ASSERT_EQ(cluster_membership_vet_joiner(-1, 100, 1), CLUSTER_JOIN_REJECT_NOT_READY);
	UT_ASSERT_EQ(cluster_membership_vet_joiner(CLUSTER_MAX_NODES, 100, 1),
				 CLUSTER_JOIN_REJECT_NOT_READY);
	UT_ASSERT_EQ(cluster_membership_vet_joiner(CLUSTER_MAX_NODES + 7, 100, 1),
				 CLUSTER_JOIN_REJECT_NOT_READY);
}

/* ======================================================================
 * U4b (P1a) -- uint64 incarnations above 2^32 are compared without
 * truncation.  With last_admitted=5, a presented value of 2^32+5 is
 * strictly fresher and MUST be admitted.  A uint32 truncation of the
 * compare would see low32(2^32+5)==5 == floor and wrongly REJECT.
 * ====================================================================== */
UT_TEST(test_vet_uint64_no_truncation)
{
	uint64 big = (UINT64CONST(1) << 32) + 5;

	cluster_membership_record_admitted(7, 5);
	UT_ASSERT_EQ(cluster_membership_vet_joiner(7, big, 1), CLUSTER_JOIN_ACCEPT);

	/* and a high-valued equal incarnation is still stale */
	cluster_membership_record_admitted(8, (UINT64CONST(1) << 40) + 9);
	UT_ASSERT_EQ(cluster_membership_vet_joiner(8, (UINT64CONST(1) << 40) + 9, 1),
				 CLUSTER_JOIN_REJECT_STALE_INCARNATION);
	UT_ASSERT_EQ(cluster_membership_vet_joiner(8, (UINT64CONST(1) << 40) + 10, 1),
				 CLUSTER_JOIN_ACCEPT);
}

/* ======================================================================
 * U5 -- the admitted floor never regresses (INV-J7): recording a lower
 * incarnation must not lower the floor, so a replay of the old value
 * stays rejected.
 * ====================================================================== */
UT_TEST(test_record_admitted_monotonic_nonregression)
{
	cluster_membership_record_admitted(9, 5);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(9), 5);

	cluster_membership_record_admitted(9, 7);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(9), 7);

	/* a stale/lower record must be ignored, not lower the floor */
	cluster_membership_record_admitted(9, 3);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(9), 7);

	/* and the gate stays closed against the now-stale 7 / open to 8 */
	UT_ASSERT_EQ(cluster_membership_vet_joiner(9, 7, 1), CLUSTER_JOIN_REJECT_STALE_INCARNATION);
	UT_ASSERT_EQ(cluster_membership_vet_joiner(9, 8, 1), CLUSTER_JOIN_ACCEPT);
}

/* ======================================================================
 * U11 -- is_member keys off membership_state (decision SSOT), NOT CSSD.
 * A node is a member iff state == MEMBER; JOINING/REJECTED/DEAD/ABSENT
 * are not counted.
 * ====================================================================== */
UT_TEST(test_state_decision_key_is_member)
{
	/* default is ABSENT and not a member */
	UT_ASSERT_EQ(cluster_membership_get_state(10), CLUSTER_MEMBER_ABSENT);
	UT_ASSERT_EQ(cluster_membership_is_member(10), false);

	cluster_membership_set_state(10, CLUSTER_MEMBER_JOINING);
	UT_ASSERT_EQ(cluster_membership_get_state(10), CLUSTER_MEMBER_JOINING);
	UT_ASSERT_EQ(cluster_membership_is_member(10), false);

	cluster_membership_set_state(10, CLUSTER_MEMBER_MEMBER);
	UT_ASSERT_EQ(cluster_membership_get_state(10), CLUSTER_MEMBER_MEMBER);
	UT_ASSERT_EQ(cluster_membership_is_member(10), true);

	cluster_membership_set_state(10, CLUSTER_MEMBER_REJECTED);
	UT_ASSERT_EQ(cluster_membership_is_member(10), false);
}

/* ======================================================================
 * Ux -- accessors are range-defensive: an out-of-range node_id never
 * reads/writes out of bounds and never reports membership.
 * ====================================================================== */
UT_TEST(test_accessors_range_defensive)
{
	UT_ASSERT_EQ(cluster_membership_is_member(-1), false);
	UT_ASSERT_EQ(cluster_membership_is_member(CLUSTER_MAX_NODES), false);
	UT_ASSERT_EQ(cluster_membership_get_state(-1), CLUSTER_MEMBER_ABSENT);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(-1), 0);

	/* OOB mutations are no-ops, not crashes */
	cluster_membership_record_admitted(-1, 99);
	cluster_membership_set_state(CLUSTER_MAX_NODES, CLUSTER_MEMBER_MEMBER);
	UT_ASSERT_EQ(cluster_membership_is_member(CLUSTER_MAX_NODES), false);
}

/* ======================================================================
 * Uq -- the quorum sub-gate holds a join when this node is not in quorum
 * (REJECT_QUORUM, transient).  A fresh-above incarnation that would ACCEPT
 * in quorum becomes a hold out of quorum; restoring quorum lets it through.
 * ====================================================================== */
UT_TEST(test_vet_quorum_subgate)
{
	cluster_membership_record_admitted(20, 5);

	test_in_quorum = false;
	UT_ASSERT_EQ(cluster_membership_vet_joiner(20, 9, 1), CLUSTER_JOIN_REJECT_QUORUM);

	test_in_quorum = true;
	UT_ASSERT_EQ(cluster_membership_vet_joiner(20, 9, 1), CLUSTER_JOIN_ACCEPT);
}

/* ======================================================================
 * Ur -- the readiness sub-gate: a joiner that has not published a valid
 * voting slot (generation 0) is not ready and is held (REJECT_NOT_READY),
 * never admitted, even with a fresh-above incarnation.
 * ====================================================================== */
UT_TEST(test_vet_readiness_subgate)
{
	cluster_membership_record_admitted(21, 5);

	UT_ASSERT_EQ(cluster_membership_vet_joiner(21, 9, 0), CLUSTER_JOIN_REJECT_NOT_READY);
	/* a published slot (generation >= 1) clears the readiness gate */
	UT_ASSERT_EQ(cluster_membership_vet_joiner(21, 9, 1), CLUSTER_JOIN_ACCEPT);
}

/* ======================================================================
 * U10 (P1b/INV-J7) -- the durable COMMITTED marker re-seeds the floor across
 * a restart: without the seed a stale incarnation re-passes (the INV-J1 hole);
 * applying the marker restores the floor and the stale rejoin is rejected again.
 * ====================================================================== */
UT_TEST(test_seed_committed_marker_reseeds_floor)
{
	ClusterJoinCommitMarker m;

	test_in_quorum = true;
	memset(&seed_tab, 0, sizeof(seed_tab));
	cluster_membership_attach(&seed_tab);

	/* old life: N=12 admitted at incarnation 5 -> replay of 5 is stale */
	cluster_membership_record_admitted(12, 5);
	UT_ASSERT_EQ(cluster_membership_vet_joiner(12, 5, 1), CLUSTER_JOIN_REJECT_STALE_INCARNATION);

	/* restart: a zeroed table would re-accept the stale 5 (the hole) */
	memset(&seed_tab, 0, sizeof(seed_tab));
	cluster_membership_attach(&seed_tab);
	UT_ASSERT_EQ(cluster_membership_vet_joiner(12, 5, 1), CLUSTER_JOIN_ACCEPT);

	/* seed from the durable COMMITTED marker -> floor restored, stale rejected */
	make_marker(&m, 12, CLUSTER_JCMK_PHASE_COMMITTED, 5, 9);
	UT_ASSERT(cluster_membership_seed_apply_marker(&m));
	UT_ASSERT_EQ(cluster_membership_vet_joiner(12, 5, 1), CLUSTER_JOIN_REJECT_STALE_INCARNATION);
}

/* ======================================================================
 * U13 (P1#2) -- seen != admitted: a PREPARE-only marker (the joiner wrote a
 * fresh incarnation but its join did NOT commit) does NOT seed the floor, so a
 * legitimate mid-join presenting that incarnation is still ACCEPTed.
 * ====================================================================== */
UT_TEST(test_seed_prepare_marker_not_a_basis)
{
	ClusterJoinCommitMarker m;

	test_in_quorum = true;
	memset(&seed_tab, 0, sizeof(seed_tab));
	cluster_membership_attach(&seed_tab);

	make_marker(&m, 13, CLUSTER_JCMK_PHASE_PREPARE, 6, 0);
	UT_ASSERT(!cluster_membership_seed_apply_marker(&m));
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(13), 0);
	UT_ASSERT_EQ(cluster_membership_vet_joiner(13, 6, 1), CLUSTER_JOIN_ACCEPT);
}

/* ======================================================================
 * U15 (P1#1-r4) -- epoch-reset does NOT lose the seed: a COMMITTED marker
 * admitted@admitted_epoch=7 still seeds when the local epoch is 0 (a restart),
 * because the trust gate is phase==COMMITTED, NEVER an epoch comparison.  A
 * corrupt (bad-CRC) marker is rejected.
 * ====================================================================== */
UT_TEST(test_seed_committed_marker_epoch_reset_still_seeds)
{
	ClusterJoinCommitMarker m;

	test_in_quorum = true;
	memset(&seed_tab, 0, sizeof(seed_tab));
	cluster_membership_attach(&seed_tab);

	make_marker(&m, 14, CLUSTER_JCMK_PHASE_COMMITTED, 8, 7);
	UT_ASSERT(cluster_membership_seed_apply_marker(&m));
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(14), 8);
	UT_ASSERT_EQ(cluster_membership_vet_joiner(14, 8, 1), CLUSTER_JOIN_REJECT_STALE_INCARNATION);

	/* a corrupt marker is not a basis (fail-closed integrity) */
	m.crc32c ^= 0xFFFFFFFFu;
	UT_ASSERT(!cluster_join_marker_is_committed_basis(&m, 14));
}

/* ======================================================================
 * U16 (HF-3 / INV-J13) -- same_commit groups by full identity (incl. nonce).
 * Two markers of the SAME commit attempt are same_commit; differing by nonce or
 * by admitted_epoch (a different coordinator / attempt) are NOT, so two minority
 * writes from different attempts cannot aggregate into a false majority (P1-3).
 * ====================================================================== */
UT_TEST(test_marker_same_commit_identity_group)
{
	ClusterJoinCommitMarker a, b;

	make_marker(&a, 7, CLUSTER_JCMK_PHASE_COMMITTED, 100, 5);
	make_marker(&b, 7, CLUSTER_JCMK_PHASE_COMMITTED, 100, 5);
	a.commit_nonce = b.commit_nonce = 0xABCDEFu;
	cluster_join_marker_compute_crc(&a);
	cluster_join_marker_compute_crc(&b);
	UT_ASSERT(cluster_join_marker_same_commit(&a, &b)); /* byte-identical -> same */

	/* a different commit_nonce (a different attempt) -> NOT the same commit */
	b.commit_nonce = 0x123456u;
	cluster_join_marker_compute_crc(&b);
	UT_ASSERT(!cluster_join_marker_same_commit(&a, &b));

	/* same nonce but a different admitted_epoch (different coordinator/epoch) */
	make_marker(&b, 7, CLUSTER_JCMK_PHASE_COMMITTED, 100, 9);
	b.commit_nonce = 0xABCDEFu;
	cluster_join_marker_compute_crc(&b);
	UT_ASSERT(!cluster_join_marker_same_commit(&a, &b));

	UT_ASSERT(!cluster_join_marker_same_commit(&a, NULL)); /* NULL defensive */
}

/* ======================================================================
 * U17 (HF-3) -- a stale on-disk format (version != current) is fail-closed
 * rejected: a marker whose layout we cannot parse is never trusted.
 * ====================================================================== */
UT_TEST(test_marker_version_mismatch_failclosed)
{
	ClusterJoinCommitMarker m;

	make_marker(&m, 8, CLUSTER_JCMK_PHASE_COMMITTED, 3, 1);
	UT_ASSERT(cluster_join_marker_is_committed_basis(&m, 8)); /* current version */

	m.version = CLUSTER_JCMK_VERSION - 1; /* an older on-disk format */
	cluster_join_marker_compute_crc(&m);
	UT_ASSERT(!cluster_join_marker_struct_valid(&m, 8));
	UT_ASSERT(!cluster_join_marker_is_committed_basis(&m, 8));
}

/* ======================================================================
 * spec-5.18 U5 -- a REMOVED node is rejected REMOVED_FENCED, even with a fresh
 * incarnation, and definitively (not a transient quorum/readiness hold).
 * ====================================================================== */
UT_TEST(test_vet_removed_fenced)
{
	test_in_quorum = true;
	cluster_membership_attach(NULL); /* process-local default table */

	/* admit node 30 at incarnation 5, then permanently remove it. */
	cluster_membership_record_admitted(30, 5);
	cluster_membership_set_state(30, CLUSTER_MEMBER_MEMBER);
	cluster_membership_shrink_to_removed(30, 5);
	UT_ASSERT_EQ(cluster_membership_get_state(30), CLUSTER_MEMBER_REMOVED);

	/* even a far-fresher incarnation is rejected REMOVED_FENCED (INV-LF1). */
	UT_ASSERT_EQ(cluster_membership_vet_joiner(30, 9999, 1), CLUSTER_JOIN_REJECT_REMOVED_FENCED);

	/* definitive: out-of-quorum does NOT downgrade it to the transient hold. */
	test_in_quorum = false;
	UT_ASSERT_EQ(cluster_membership_vet_joiner(30, 9999, 1), CLUSTER_JOIN_REJECT_REMOVED_FENCED);
	test_in_quorum = true;
}

/* ======================================================================
 * spec-5.18 U7 -- member_count is the MEMBER-only denominator; shrink_to_removed
 * shrinks it by one and pins the incarnation floor.
 * ====================================================================== */
UT_TEST(test_member_count_shrink)
{
	int before;

	test_in_quorum = true;
	cluster_membership_attach(&seed_tab);
	memset(&seed_tab, 0, sizeof(seed_tab));

	cluster_membership_set_state(40, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(41, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(42, CLUSTER_MEMBER_MEMBER);
	cluster_membership_set_state(43, CLUSTER_MEMBER_DEAD);	  /* not counted */
	cluster_membership_set_state(44, CLUSTER_MEMBER_JOINING); /* not counted */
	before = cluster_membership_member_count();
	UT_ASSERT_EQ(before, 3);

	/* permanent removal shrinks the denominator by one + pins the floor. */
	cluster_membership_shrink_to_removed(41, 7);
	UT_ASSERT_EQ(cluster_membership_member_count(), 2);
	UT_ASSERT_EQ(cluster_membership_get_state(41), CLUSTER_MEMBER_REMOVED);
	UT_ASSERT_EQ(cluster_membership_get_last_admitted_incarnation(41), 7);

	cluster_membership_attach(NULL);
}

int
main(void)
{
	UT_PLAN(17);
	UT_RUN(test_vet_fresh_above_accept);
	UT_RUN(test_vet_equal_reject_stale);
	UT_RUN(test_vet_below_reject_stale);
	UT_RUN(test_vet_node_id_out_of_range_failclosed);
	UT_RUN(test_vet_uint64_no_truncation);
	UT_RUN(test_record_admitted_monotonic_nonregression);
	UT_RUN(test_state_decision_key_is_member);
	UT_RUN(test_accessors_range_defensive);
	UT_RUN(test_vet_quorum_subgate);
	UT_RUN(test_vet_readiness_subgate);
	UT_RUN(test_seed_committed_marker_reseeds_floor);
	UT_RUN(test_seed_prepare_marker_not_a_basis);
	UT_RUN(test_seed_committed_marker_epoch_reset_still_seeds);
	UT_RUN(test_marker_same_commit_identity_group);
	UT_RUN(test_marker_version_mismatch_failclosed);
	UT_RUN(test_vet_removed_fenced);
	UT_RUN(test_member_count_shrink);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
