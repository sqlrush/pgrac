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

/* spec-5.22e D5-8 stub: cluster_membership_set_state notes self-admission
 * into the undo horizon shmem (cluster_undo_horizon_ic.c not linked here);
 * also satisfies the cluster_node_id extern via cluster_guc.o linkage or
 * local definition in this binary. */
void cluster_undo_horizon_note_self_member(void);
void
cluster_undo_horizon_note_self_member(void)
{}
int cluster_node_id = -1; /* GUC global stub (cluster_guc.o not linked) */


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

/* cluster_membership.c invalidates this backend-local cache after a commit. */
void
cluster_write_fence_authority_cache_invalidate(void)
{}

/*
 * A file-static backing table for the marker-seed tests (U10/U13/U15).  They
 * attach a fresh table to simulate a restart; it must NOT be a stack local
 * (the accessors keep the pointer after the test returns).  build a COMMITTED /
 * PREPARE marker with a valid CRC.
 */
static ClusterMembershipTable seed_tab;

static uint32
test_get_le32(const uint8 *p)
{
	return (uint32)p[0] | ((uint32)p[1] << 8) | ((uint32)p[2] << 16) | ((uint32)p[3] << 24);
}

static uint64
test_get_le64(const uint8 *p)
{
	return (uint64)test_get_le32(p) | ((uint64)test_get_le32(p + 4) << 32);
}

static void
make_replacement_marker(ClusterReplacementCommitMarkerV3 *m, uint8 phase, uint32 ready_generation)
{
	int i;

	memset(m, 0, sizeof(*m));
	m->magic = CLUSTER_JCMK_MAGIC;
	m->version = CLUSTER_JCMK_REPLACEMENT_VERSION;
	m->target_node_id = 7;
	m->phase = phase;
	m->generation = UINT64CONST(0x0102030405060708);
	m->old_admitted_incarnation = UINT64CONST(0x1112131415161718);
	m->fresh_incarnation = UINT64CONST(0x2122232425262728);
	m->baseline_epoch = UINT64CONST(0x3132333435363738);
	m->reserved_or_committed_epoch = UINT64CONST(0x4142434445464748);
	m->request_nonce = UINT64CONST(0x5152535455565758);
	for (i = 0; i < 16; i++)
		m->expected_purge_survivors[i] = (uint8)(0x80 + i);
	m->grammar_fingerprint = UINT64CONST(0x6162636465666768);
	m->ready_state_generation = ready_generation;
}

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

/* ======================================================================
 * U8 (reviewer P1 #2; Hardening v1.4 / INV-J13) -- select_majority groups
 * voting-disk join markers by commit IDENTITY.  Distinct-attempt (different
 * nonce) minority markers must NOT aggregate into a false majority — the bug
 * the qvotec peer-observe path had (it counted any COMMITTED marker), unlike
 * the self-admit and startup-seed paths which already required a same-commit
 * majority.  This is the shared helper all three now use.
 * ====================================================================== */
UT_TEST(test_marker_select_majority_groups_by_commit)
{
	ClusterJoinCommitMarker mk[3];
	uint32 majority = 2; /* 3 voting disks */
	uint32 agree = 99;
	int win;

	/* Three COMMITTED markers for node 7, each a DIFFERENT attempt (distinct
	 * nonce): every one is committed-basis, but NONE reached a same-commit
	 * majority.  Must select nothing — a false-majority readmit would be 8.A. */
	make_marker(&mk[0], 7, CLUSTER_JCMK_PHASE_COMMITTED, 5, 10);
	mk[0].commit_nonce = 0x111;
	cluster_join_marker_compute_crc(&mk[0]);
	make_marker(&mk[1], 7, CLUSTER_JCMK_PHASE_COMMITTED, 6, 12);
	mk[1].commit_nonce = 0x222;
	cluster_join_marker_compute_crc(&mk[1]);
	make_marker(&mk[2], 7, CLUSTER_JCMK_PHASE_COMMITTED, 7, 14);
	mk[2].commit_nonce = 0x333;
	cluster_join_marker_compute_crc(&mk[2]);
	UT_ASSERT_EQ(cluster_join_marker_select_majority(mk, 3, majority, &agree), -1);
	UT_ASSERT_EQ((int)agree, 0); /* no winning group */

	/* Two of the SAME commit (identical identity/nonce) + one different: the
	 * same-commit pair IS a majority -> a member of that commit is selected. */
	make_marker(&mk[0], 7, CLUSTER_JCMK_PHASE_COMMITTED, 9, 20);
	mk[0].commit_nonce = 0xAAA;
	cluster_join_marker_compute_crc(&mk[0]);
	make_marker(&mk[1], 7, CLUSTER_JCMK_PHASE_COMMITTED, 9, 20);
	mk[1].commit_nonce = 0xAAA;
	cluster_join_marker_compute_crc(&mk[1]);
	make_marker(&mk[2], 7, CLUSTER_JCMK_PHASE_COMMITTED, 8, 18);
	mk[2].commit_nonce = 0xBBB;
	cluster_join_marker_compute_crc(&mk[2]);
	win = cluster_join_marker_select_majority(mk, 3, majority, &agree);
	UT_ASSERT(win == 0 || win == 1); /* a member of the winning same-commit set */
	UT_ASSERT_EQ((int)agree, 2);	 /* exactly the same-commit pair */
	UT_ASSERT_EQ((int)mk[win].admitted_incarnation, 9);
	UT_ASSERT_EQ((int)mk[win].commit_nonce, (int)0xAAA);

	/* Empty / defensive input selects nothing. */
	UT_ASSERT_EQ(cluster_join_marker_select_majority(mk, 0, majority, NULL), -1);
	UT_ASSERT_EQ(cluster_join_marker_select_majority(NULL, 3, majority, NULL), -1);
}

/* ======================================================================
 * 5.15A §2.2 -- replacement JCMK v3 is an exact 96-byte little-endian
 * durable image.  Offset 88 carries the ADMITTED-only s_ready and offset 92
 * carries CRC32C over the preceding bytes.
 * ====================================================================== */
UT_TEST(test_replacement_marker_v3_exact_codec)
{
	ClusterReplacementCommitMarkerV3 in;
	ClusterReplacementCommitMarkerV3 out;
	uint8 bytes[CLUSTER_JCMK_REPLACEMENT_BYTES];

	make_replacement_marker(&in, CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED, UINT32_C(0x71727374));
	memset(bytes, 0xA5, sizeof(bytes));
	UT_ASSERT(cluster_replacement_marker_v3_encode(&in, bytes));

	UT_ASSERT_EQ((int)test_get_le32(bytes + 0), (int)CLUSTER_JCMK_MAGIC);
	UT_ASSERT_EQ((int)test_get_le32(bytes + 4), (int)CLUSTER_JCMK_REPLACEMENT_VERSION);
	UT_ASSERT_EQ((int)test_get_le32(bytes + 8), 7);
	UT_ASSERT_EQ((int)bytes[12], CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED);
	UT_ASSERT_EQ((int)bytes[13], 0);
	UT_ASSERT_EQ((int)bytes[14], 0);
	UT_ASSERT_EQ((int)bytes[15], 0);
	UT_ASSERT(test_get_le64(bytes + 16) == in.generation);
	UT_ASSERT(test_get_le64(bytes + 24) == in.old_admitted_incarnation);
	UT_ASSERT(test_get_le64(bytes + 32) == in.fresh_incarnation);
	UT_ASSERT(test_get_le64(bytes + 40) == in.baseline_epoch);
	UT_ASSERT(test_get_le64(bytes + 48) == in.reserved_or_committed_epoch);
	UT_ASSERT(test_get_le64(bytes + 56) == in.request_nonce);
	UT_ASSERT(memcmp(bytes + 64, in.expected_purge_survivors, 16) == 0);
	UT_ASSERT(test_get_le64(bytes + 80) == in.grammar_fingerprint);
	UT_ASSERT_EQ((int)test_get_le32(bytes + 88), (int)in.ready_state_generation);

	memset(&out, 0xCC, sizeof(out));
	UT_ASSERT(cluster_replacement_marker_v3_decode(bytes, 7, &out));
	UT_ASSERT_EQ((int)out.target_node_id, 7);
	UT_ASSERT_EQ((int)out.phase, CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED);
	UT_ASSERT(out.generation == in.generation);
	UT_ASSERT(out.old_admitted_incarnation == in.old_admitted_incarnation);
	UT_ASSERT(out.fresh_incarnation == in.fresh_incarnation);
	UT_ASSERT(out.baseline_epoch == in.baseline_epoch);
	UT_ASSERT(out.reserved_or_committed_epoch == in.reserved_or_committed_epoch);
	UT_ASSERT(out.request_nonce == in.request_nonce);
	UT_ASSERT(memcmp(out.expected_purge_survivors, in.expected_purge_survivors, 16) == 0);
	UT_ASSERT(out.grammar_fingerprint == in.grammar_fingerprint);
	UT_ASSERT_EQ((int)out.ready_state_generation, (int)in.ready_state_generation);
	UT_ASSERT_EQ((int)out.crc32c, (int)test_get_le32(bytes + 92));
}

/* ======================================================================
 * 5.15A §2.2 -- offset-88 is phase-specific and every malformed image is
 * rejected without changing the caller's decoded output.
 * ====================================================================== */
UT_TEST(test_replacement_marker_v3_phase_and_integrity_gates)
{
	ClusterReplacementCommitMarkerV3 m;
	ClusterReplacementCommitMarkerV3 out;
	ClusterReplacementCommitMarkerV3 before;
	uint8 bytes[CLUSTER_JCMK_REPLACEMENT_BYTES];

	make_replacement_marker(&m, CLUSTER_JCMK_REPLACEMENT_PHASE_PREPARE, 0);
	UT_ASSERT(cluster_replacement_marker_v3_encode(&m, bytes));

	memset(&out, 0x5A, sizeof(out));
	before = out;
	bytes[44] ^= 0x01; /* CRC-covered corruption */
	UT_ASSERT(!cluster_replacement_marker_v3_decode(bytes, 7, &out));
	UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
	bytes[44] ^= 0x01;

	UT_ASSERT(!cluster_replacement_marker_v3_decode(bytes, 8, &out));
	UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);

	m.ready_state_generation = 9;
	UT_ASSERT(!cluster_replacement_marker_v3_encode(&m, bytes));
	make_replacement_marker(&m, CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED, 0);
	UT_ASSERT(!cluster_replacement_marker_v3_encode(&m, bytes));
	make_replacement_marker(&m, CLUSTER_JCMK_REPLACEMENT_PHASE_ABORTED_CLOSED, 0);
	m.reserved0[1] = 1;
	UT_ASSERT(!cluster_replacement_marker_v3_encode(&m, bytes));
	make_replacement_marker(&m, UINT8_C(99), 0);
	UT_ASSERT(!cluster_replacement_marker_v3_encode(&m, bytes));
}

/* ======================================================================
 * 5.15A A1-I3 -- a replacement marker majority is a majority of one exact,
 * valid canonical 96-byte image.  Different episodes never aggregate, and a
 * malformed member of an otherwise matching pair contributes no vote.
 * ====================================================================== */
UT_TEST(test_replacement_marker_v3_same_image_majority)
{
	ClusterReplacementCommitMarkerV3 a;
	ClusterReplacementCommitMarkerV3 b;
	ClusterReplacementCommitMarkerV3 selected;
	ClusterReplacementCommitMarkerV3 before;
	uint8 images[3][CLUSTER_JCMK_REPLACEMENT_BYTES];
	uint32 agree;
	int win;

	make_replacement_marker(&a, CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED, 0);
	b = a;
	UT_ASSERT(cluster_replacement_marker_v3_same_image(&a, &b));
	b.request_nonce++;
	UT_ASSERT(!cluster_replacement_marker_v3_same_image(&a, &b));
	b = a;
	b.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED;
	b.ready_state_generation = 1;
	UT_ASSERT(!cluster_replacement_marker_v3_same_image(&a, &b));
	b.generation = 0;
	UT_ASSERT(!cluster_replacement_marker_v3_same_image(&a, &b));

	UT_ASSERT(cluster_replacement_marker_v3_encode(&a, images[0]));
	memcpy(images[1], images[0], sizeof(images[1]));
	b = a;
	b.request_nonce++;
	UT_ASSERT(cluster_replacement_marker_v3_encode(&b, images[2]));

	memset(&selected, 0xA5, sizeof(selected));
	before = selected;
	agree = 77;
	win = cluster_replacement_marker_v3_select_majority(images, 3, 2, 7, &selected, &agree);
	UT_ASSERT(win == 0 || win == 1);
	UT_ASSERT_EQ((int)agree, 2);
	UT_ASSERT(selected.request_nonce == a.request_nonce);

	/* Three valid but different images cannot synthesize one majority. */
	memcpy(images[1], images[2], sizeof(images[1]));
	b.request_nonce++;
	UT_ASSERT(cluster_replacement_marker_v3_encode(&b, images[2]));
	selected = before;
	agree = 77;
	UT_ASSERT_EQ(cluster_replacement_marker_v3_select_majority(images, 3, 2, 7, &selected, &agree),
				 -1);
	UT_ASSERT(memcmp(&selected, &before, sizeof(selected)) == 0);
	UT_ASSERT_EQ((int)agree, 77);

	/* A CRC-bad image is invalid, not an identical vote. */
	memcpy(images[1], images[0], sizeof(images[1]));
	images[1][44] ^= 1;
	selected = before;
	agree = 77;
	UT_ASSERT_EQ(cluster_replacement_marker_v3_select_majority(images, 3, 2, 7, &selected, &agree),
				 -1);
	UT_ASSERT(memcmp(&selected, &before, sizeof(selected)) == 0);
	UT_ASSERT_EQ((int)agree, 77);

	/* Invalid cardinalities and target mismatch preserve every output. */
	UT_ASSERT_EQ(cluster_replacement_marker_v3_select_majority(images, 3, 0, 7, &selected, &agree),
				 -1);
	UT_ASSERT_EQ(cluster_replacement_marker_v3_select_majority(images, 3, 1, 7, &selected, &agree),
				 -1);
	UT_ASSERT_EQ(cluster_replacement_marker_v3_select_majority(images, 2, 1, 7, &selected, &agree),
				 -1);
	UT_ASSERT_EQ(cluster_replacement_marker_v3_select_majority(images, 0, 1, 7, &selected, &agree),
				 -1);
	UT_ASSERT_EQ(cluster_replacement_marker_v3_select_majority(images, 3, 4, 7, &selected, &agree),
				 -1);
	memcpy(images[1], images[0], sizeof(images[1]));
	UT_ASSERT_EQ(cluster_replacement_marker_v3_select_majority(images, 3, 2, 8, &selected, &agree),
				 -1);
	UT_ASSERT(memcmp(&selected, &before, sizeof(selected)) == 0);
	UT_ASSERT_EQ((int)agree, 77);
}

/* ======================================================================
 * 5.15A §2.2 -- the phase decides which incarnation is a durable floor.
 * PREPARE/ABORTED_CLOSED seed old; COMMITTED_CLOSED/ADMITTED seed fresh.
 * Only ADMITTED is the membership-publication basis and exposes s_ready.
 * ====================================================================== */
UT_TEST(test_replacement_marker_v3_phase_bases_and_floors)
{
	ClusterReplacementCommitMarkerV3 m;
	uint8 bytes[CLUSTER_JCMK_REPLACEMENT_BYTES];
	uint64 floor;
	uint32 ready;

	make_replacement_marker(&m, CLUSTER_JCMK_REPLACEMENT_PHASE_PREPARE, 0);
	UT_ASSERT(cluster_replacement_marker_v3_encode(&m, bytes));
	floor = UINT64CONST(0xDEADBEEF);
	UT_ASSERT(cluster_replacement_marker_v3_floor_basis(bytes, 7, &floor));
	UT_ASSERT(floor == m.old_admitted_incarnation);

	make_replacement_marker(&m, CLUSTER_JCMK_REPLACEMENT_PHASE_ABORTED_CLOSED, 0);
	UT_ASSERT(cluster_replacement_marker_v3_encode(&m, bytes));
	UT_ASSERT(cluster_replacement_marker_v3_floor_basis(bytes, 7, &floor));
	UT_ASSERT(floor == m.old_admitted_incarnation);

	make_replacement_marker(&m, CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED, 0);
	UT_ASSERT(cluster_replacement_marker_v3_encode(&m, bytes));
	UT_ASSERT(cluster_replacement_marker_v3_floor_basis(bytes, 7, &floor));
	UT_ASSERT(floor == m.fresh_incarnation);
	UT_ASSERT(cluster_replacement_marker_v3_is_committed_closed_basis(bytes, 7, &floor));
	UT_ASSERT(floor == m.fresh_incarnation);
	ready = UINT32_C(0xA5A5A5A5);
	UT_ASSERT(!cluster_replacement_marker_v3_is_admitted_basis(bytes, 7, &floor, &ready));
	UT_ASSERT(floor == m.fresh_incarnation);
	UT_ASSERT_EQ((int)ready, (int)UINT32_C(0xA5A5A5A5));

	make_replacement_marker(&m, CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED, UINT32_C(0x71727374));
	UT_ASSERT(cluster_replacement_marker_v3_encode(&m, bytes));
	UT_ASSERT(cluster_replacement_marker_v3_floor_basis(bytes, 7, &floor));
	UT_ASSERT(floor == m.fresh_incarnation);
	ready = 0;
	UT_ASSERT(cluster_replacement_marker_v3_is_admitted_basis(bytes, 7, &floor, &ready));
	UT_ASSERT(floor == m.fresh_incarnation);
	UT_ASSERT_EQ((int)ready, (int)m.ready_state_generation);
	floor = UINT64CONST(0xDEADBEEF);
	UT_ASSERT(!cluster_replacement_marker_v3_is_committed_closed_basis(bytes, 7, &floor));
	UT_ASSERT(floor == UINT64CONST(0xDEADBEEF));

	/* A phase cannot seed a zero corresponding incarnation. */
	make_replacement_marker(&m, CLUSTER_JCMK_REPLACEMENT_PHASE_PREPARE, 0);
	m.old_admitted_incarnation = 0;
	UT_ASSERT(cluster_replacement_marker_v3_encode(&m, bytes));
	floor = UINT64CONST(0xDEADBEEF);
	UT_ASSERT(!cluster_replacement_marker_v3_floor_basis(bytes, 7, &floor));
	UT_ASSERT(floor == UINT64CONST(0xDEADBEEF));

	make_replacement_marker(&m, CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED, 0);
	m.fresh_incarnation = 0;
	UT_ASSERT(cluster_replacement_marker_v3_encode(&m, bytes));
	UT_ASSERT(!cluster_replacement_marker_v3_floor_basis(bytes, 7, &floor));
	UT_ASSERT(!cluster_replacement_marker_v3_is_committed_closed_basis(bytes, 7, &floor));
	UT_ASSERT(floor == UINT64CONST(0xDEADBEEF));

	make_replacement_marker(&m, CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED, 9);
	m.fresh_incarnation = 0;
	UT_ASSERT(cluster_replacement_marker_v3_encode(&m, bytes));
	ready = UINT32_C(0xA5A5A5A5);
	UT_ASSERT(!cluster_replacement_marker_v3_is_admitted_basis(bytes, 7, &floor, &ready));
	UT_ASSERT(floor == UINT64CONST(0xDEADBEEF));
	UT_ASSERT_EQ((int)ready, (int)UINT32_C(0xA5A5A5A5));
}

int
main(void)
{
	UT_PLAN(22);
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
	UT_RUN(test_marker_select_majority_groups_by_commit);
	UT_RUN(test_marker_version_mismatch_failclosed);
	UT_RUN(test_vet_removed_fenced);
	UT_RUN(test_member_count_shrink);
	UT_RUN(test_replacement_marker_v3_exact_codec);
	UT_RUN(test_replacement_marker_v3_phase_and_integrity_gates);
	UT_RUN(test_replacement_marker_v3_same_image_majority);
	UT_RUN(test_replacement_marker_v3_phase_bases_and_floors);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
