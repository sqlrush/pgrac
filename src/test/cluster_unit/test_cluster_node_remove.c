/*-------------------------------------------------------------------------
 *
 * test_cluster_node_remove.c
 *	  Unit tests for spec-5.18 online node leave (permanent removal) policy
 *	  helpers.
 *
 *	  Exercises the pure decision layer (cluster_node_remove_policy.c):
 *	  phase-FSM transition validity, the precheck behaviour matrix, the
 *	  INV-LF2 fence-before-shrink ordering gate, the fence-arm-timeout
 *	  conservative classify, the version-coherent contest classify, the
 *	  three-phase removal-marker structural validation + majority authority
 *	  decide + carry-forward, the INV-LF7 crash-recovery matrix, and IC
 *	  payload integrity; plus the shmem struct-layout invariants.  The runtime
 *	  driver (shmem, voting-disk marker I/O, fence-arm, shrink commit, LMON
 *	  orchestration, GRD/PCM cleanup) lives in cluster_node_remove.c and is
 *	  covered by cluster_tap t/NNN (2-node ClusterPair).  membership/reconfig
 *	  side (vet_joiner REMOVED, effective_dead, member_count, event_id_v2) is
 *	  covered in test_cluster_membership.c / test_cluster_reconfig.c (those
 *	  binaries link the respective objects).
 *
 *	  Test IDs map to spec-5.18 §4.1:
 *	    U1   shmem region size + 10-phase enum + marker size/offset invariants
 *	    U2   phase state machine: legal path + illegal edges + CLEANUP_BLOCKED
 *	    U3   INV-LF2 ordering gate (no fence-armed => no SHRINK)
 *	    U3b  fence-arm timeout conservative classify (no-majority => clean abort)
 *	    U4   version-coherent contest classify (pre/post-SHRUNK)
 *	    U9   IC payload magic / version / CRC / range validation
 *	    U10  precheck behaviour matrix (all reject reasons + accept/resume/noop)
 *	    U11  INV-LF7 fence x removal-marker crash-recovery matrix (incl corruption)
 *	    U13  removal-marker pack/unpack/preserve + majority authority decide
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_node_remove.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.18-online-node-leave-fence-cleanup.md.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "cluster/cluster_node_remove.h"

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


/* Assert hook for cassert builds (policy layer takes no other PG runtime dep;
 * pg_crc32c comes from libpgport). */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


UT_DEFINE_GLOBALS();


/* ============================================================
 * U1 — struct-layout invariants
 * ============================================================ */

UT_TEST(test_struct_layout)
{
	/* shmem state fits the reserved budget (StaticAssert mirror, §2.1). */
	UT_ASSERT(sizeof(ClusterNodeRemoveState) <= 512);
	/* durable marker fits one 512B voting-disk slot (§2.5). */
	UT_ASSERT(sizeof(ClusterRemovalMarker) <= 512);
	/* removal marker lives right after the 64B fence marker in _reserved1. */
	UT_ASSERT_EQ((int)CLUSTER_REMOVAL_MARKER_RESERVED1_OFFSET, (int)CLUSTER_FENCE_MARKER_BYTES);
	/* marker + offset stays clear of the slot crc32c@508 (_reserved1 = 368B). */
	UT_ASSERT(CLUSTER_REMOVAL_MARKER_RESERVED1_OFFSET + (int)sizeof(ClusterRemovalMarker) <= 368);

	/* 10-state phase enum, IDLE=0 (atomic-stored default). */
	UT_ASSERT_EQ((int)CLUSTER_REMOVE_IDLE, 0);
	UT_ASSERT_EQ((int)CLUSTER_REMOVE_REQUESTED, 1);
	UT_ASSERT_EQ((int)CLUSTER_REMOVE_PRECHECK, 2);
	UT_ASSERT_EQ((int)CLUSTER_REMOVE_FENCE_ARMING, 3);
	UT_ASSERT_EQ((int)CLUSTER_REMOVE_SHRINK_COMMITTING, 4);
	UT_ASSERT_EQ((int)CLUSTER_REMOVE_CLEANUP, 5);
	UT_ASSERT_EQ((int)CLUSTER_REMOVE_CLEANUP_BLOCKED, 6);
	UT_ASSERT_EQ((int)CLUSTER_REMOVE_COMMITTED, 7);
	UT_ASSERT_EQ((int)CLUSTER_REMOVE_ABORTED, 8);
	UT_ASSERT_EQ((int)CLUSTER_REMOVE_ABORTED_ESCALATE, 9);

	/* three-phase marker enum (REMOVING=1 .. REMOVED=3). */
	UT_ASSERT_EQ((int)CLUSTER_REMOVAL_MARKER_REMOVING, 1);
	UT_ASSERT_EQ((int)CLUSTER_REMOVAL_MARKER_SHRUNK, 2);
	UT_ASSERT_EQ((int)CLUSTER_REMOVAL_MARKER_REMOVED, 3);

	/* phase + request-result canonical strings are non-null and distinct. */
	UT_ASSERT_STR_EQ(cluster_node_remove_phase_str(CLUSTER_REMOVE_CLEANUP_BLOCKED), "cleanup_blocked");
	UT_ASSERT_STR_EQ(cluster_node_remove_phase_str(CLUSTER_REMOVE_COMMITTED), "committed");
	UT_ASSERT_STR_EQ(cluster_node_remove_request_result_str(CLUSTER_REMOVE_REQ_RESUME),
					 "resume:cleanup_pending");
	UT_ASSERT_STR_EQ(cluster_node_remove_request_result_str(CLUSTER_REMOVE_REQ_ALREADY_REMOVED),
					 "noop:already_removed");
}


/* ============================================================
 * U2 — phase state machine
 * ============================================================ */

UT_TEST(test_phase_transitions)
{
	/* legal forward path */
	UT_ASSERT(cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_IDLE, CLUSTER_REMOVE_REQUESTED));
	UT_ASSERT(cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_REQUESTED, CLUSTER_REMOVE_PRECHECK));
	UT_ASSERT(cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_PRECHECK, CLUSTER_REMOVE_FENCE_ARMING));
	UT_ASSERT(cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_FENCE_ARMING, CLUSTER_REMOVE_SHRINK_COMMITTING));
	UT_ASSERT(cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_SHRINK_COMMITTING, CLUSTER_REMOVE_CLEANUP));
	UT_ASSERT(cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_CLEANUP, CLUSTER_REMOVE_COMMITTED));

	/* CLEANUP <-> CLEANUP_BLOCKED is a resumable round-trip (post-SHRUNK) */
	UT_ASSERT(cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_CLEANUP, CLUSTER_REMOVE_CLEANUP_BLOCKED));
	UT_ASSERT(cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_CLEANUP_BLOCKED, CLUSTER_REMOVE_CLEANUP));
	UT_ASSERT(cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_CLEANUP_BLOCKED, CLUSTER_REMOVE_COMMITTED));

	/* clean ABORTED only pre-fence-commit (PRECHECK / FENCE_ARMING) */
	UT_ASSERT(cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_PRECHECK, CLUSTER_REMOVE_ABORTED));
	UT_ASSERT(cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_FENCE_ARMING, CLUSTER_REMOVE_ABORTED));

	/* ABORTED_ESCALATE only PRE-SHRUNK (REQUESTED..SHRINK_COMMITTING) */
	UT_ASSERT(cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_FENCE_ARMING, CLUSTER_REMOVE_ABORTED_ESCALATE));
	UT_ASSERT(cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_SHRINK_COMMITTING, CLUSTER_REMOVE_ABORTED_ESCALATE));

	/* illegal edges rejected */
	UT_ASSERT(!cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_PRECHECK, CLUSTER_REMOVE_SHRINK_COMMITTING)); /* must fence first */
	UT_ASSERT(!cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_IDLE, CLUSTER_REMOVE_CLEANUP));
	/* post-SHRUNK must NOT clean-abort or escalate (irreversible) */
	UT_ASSERT(!cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_CLEANUP, CLUSTER_REMOVE_ABORTED));
	UT_ASSERT(!cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_CLEANUP, CLUSTER_REMOVE_ABORTED_ESCALATE));
	UT_ASSERT(!cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_CLEANUP_BLOCKED, CLUSTER_REMOVE_ABORTED_ESCALATE));

	/* terminal phases reset to IDLE */
	UT_ASSERT(cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_COMMITTED, CLUSTER_REMOVE_IDLE));
	UT_ASSERT(cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_ABORTED, CLUSTER_REMOVE_IDLE));
	UT_ASSERT(cluster_node_remove_phase_valid_transition(CLUSTER_REMOVE_ABORTED_ESCALATE, CLUSTER_REMOVE_IDLE));
}


/* ============================================================
 * U3 / U3b — INV-LF2 ordering gate + fence-timeout classify
 * ============================================================ */

UT_TEST(test_ordering_gate)
{
	/* INV-LF2: SHRINK only after fence confirmed majority-durable */
	UT_ASSERT(!cluster_node_remove_may_enter_shrink(false));
	UT_ASSERT(cluster_node_remove_may_enter_shrink(true));
}

UT_TEST(test_fence_timeout_classify)
{
	/* P2-1: clean ABORTED ONLY when confirmed-no-majority on re-decide */
	UT_ASSERT(cluster_node_remove_fence_timeout_is_clean_abort(true));
	/* majority-or-uncertain => NOT a clean abort (conservatively continue fenced) */
	UT_ASSERT(!cluster_node_remove_fence_timeout_is_clean_abort(false));
}


/* ============================================================
 * U4 — version-coherent contest classify
 * ============================================================ */

UT_TEST(test_contest_classify)
{
	/* coherent helper */
	UT_ASSERT(cluster_node_remove_version_coherent(5, 5, 2, 2));
	UT_ASSERT(!cluster_node_remove_version_coherent(5, 6, 2, 2));	/* epoch moved */
	UT_ASSERT(!cluster_node_remove_version_coherent(5, 5, 2, 3));	/* dead_gen moved */

	/* pre-SHRUNK contest -> ABORTED_ESCALATE (removal not committed -> fail-stop) */
	UT_ASSERT_EQ((int)cluster_node_remove_classify_contest(false), (int)CLUSTER_REMOVE_ABORTED_ESCALATE);
	/* post-SHRUNK contest -> CLEANUP_BLOCKED (irreversible; resumable; never escalate) */
	UT_ASSERT_EQ((int)cluster_node_remove_classify_contest(true), (int)CLUSTER_REMOVE_CLEANUP_BLOCKED);
}


/* ============================================================
 * U10 — precheck behaviour matrix
 * ============================================================ */

UT_TEST(test_precheck_matrix)
{
	/* arg order: feature_enabled, is_self, is_declared, is_drained, in_quorum,
	 *            marker_phase, cleanup_blocked, drive_active */

	/* GUC off -> feature_disabled (checked first) */
	UT_ASSERT_EQ((int)cluster_node_remove_precheck(false, false, true, true, true, 0, false, false),
				 (int)CLUSTER_REMOVE_REQ_FEATURE_DISABLED);
	/* self -> cannot_remove_self */
	UT_ASSERT_EQ((int)cluster_node_remove_precheck(true, true, true, true, true, 0, false, false),
				 (int)CLUSTER_REMOVE_REQ_CANNOT_REMOVE_SELF);
	/* not declared */
	UT_ASSERT_EQ((int)cluster_node_remove_precheck(true, false, false, true, true, 0, false, false),
				 (int)CLUSTER_REMOVE_REQ_NOT_DECLARED);
	/* marker==REMOVED -> noop:already_removed (idempotent, before drained check) */
	UT_ASSERT_EQ((int)cluster_node_remove_precheck(true, false, true, false, true,
												   CLUSTER_REMOVAL_MARKER_REMOVED, false, false),
				 (int)CLUSTER_REMOVE_REQ_ALREADY_REMOVED);
	/* marker==SHRUNK (committed but cleanup pending) -> resume:cleanup_pending */
	UT_ASSERT_EQ((int)cluster_node_remove_precheck(true, false, true, false, true,
												   CLUSTER_REMOVAL_MARKER_SHRUNK, false, false),
				 (int)CLUSTER_REMOVE_REQ_RESUME);
	/* phase==CLEANUP_BLOCKED -> resume:cleanup_pending */
	UT_ASSERT_EQ((int)cluster_node_remove_precheck(true, false, true, false, true, 0, true, false),
				 (int)CLUSTER_REMOVE_REQ_RESUME);
	/* not drained (no 5.13/5.14 precondition) -> node_not_drained (INV-LF4) */
	UT_ASSERT_EQ((int)cluster_node_remove_precheck(true, false, true, false, true, 0, false, false),
				 (int)CLUSTER_REMOVE_REQ_NOT_DRAINED);
	/* not in quorum */
	UT_ASSERT_EQ((int)cluster_node_remove_precheck(true, false, true, true, false, 0, false, false),
				 (int)CLUSTER_REMOVE_REQ_NOT_IN_QUORUM);
	/* active non-blocked drive already running -> removal_in_progress */
	UT_ASSERT_EQ((int)cluster_node_remove_precheck(true, false, true, true, true, 0, false, true),
				 (int)CLUSTER_REMOVE_REQ_IN_PROGRESS);
	/* all good -> accepted */
	UT_ASSERT_EQ((int)cluster_node_remove_precheck(true, false, true, true, true, 0, false, false),
				 (int)CLUSTER_REMOVE_REQ_ACCEPTED);
}


/* ============================================================
 * U11 — INV-LF7 fence x removal-marker crash-recovery matrix
 * ============================================================ */

UT_TEST(test_recover_matrix)
{
	bool corrupt;
	ClusterRemovePhase p;

	/* fence=no, rm=none -> IDLE (nothing happened) */
	corrupt = true;
	p = cluster_node_remove_recover_phase(false, 0, &corrupt);
	UT_ASSERT_EQ((int)p, (int)CLUSTER_REMOVE_IDLE);
	UT_ASSERT(!corrupt);

	/* fence=YES, rm=none -> re-drive (do not leave fenced-member) -> SHRINK_COMMITTING */
	corrupt = true;
	p = cluster_node_remove_recover_phase(true, 0, &corrupt);
	UT_ASSERT_EQ((int)p, (int)CLUSTER_REMOVE_SHRINK_COMMITTING);
	UT_ASSERT(!corrupt);

	/* fence=YES, rm=REMOVING -> re-drive to SHRINK_COMMITTING */
	corrupt = true;
	p = cluster_node_remove_recover_phase(true, CLUSTER_REMOVAL_MARKER_REMOVING, &corrupt);
	UT_ASSERT_EQ((int)p, (int)CLUSTER_REMOVE_SHRINK_COMMITTING);
	UT_ASSERT(!corrupt);

	/* fence=YES, rm=SHRUNK -> resume CLEANUP (never report COMMITTED early) */
	corrupt = true;
	p = cluster_node_remove_recover_phase(true, CLUSTER_REMOVAL_MARKER_SHRUNK, &corrupt);
	UT_ASSERT_EQ((int)p, (int)CLUSTER_REMOVE_CLEANUP);
	UT_ASSERT(!corrupt);

	/* fence=YES, rm=REMOVED -> done (COMMITTED) */
	corrupt = true;
	p = cluster_node_remove_recover_phase(true, CLUSTER_REMOVAL_MARKER_REMOVED, &corrupt);
	UT_ASSERT_EQ((int)p, (int)CLUSTER_REMOVE_COMMITTED);
	UT_ASSERT(!corrupt);

	/* fence=no, rm=SHRUNK -> IMPOSSIBLE (INV-LF2) -> corruption */
	corrupt = false;
	(void) cluster_node_remove_recover_phase(false, CLUSTER_REMOVAL_MARKER_SHRUNK, &corrupt);
	UT_ASSERT(corrupt);

	/* fence=no, rm=REMOVED -> IMPOSSIBLE -> corruption */
	corrupt = false;
	(void) cluster_node_remove_recover_phase(false, CLUSTER_REMOVAL_MARKER_REMOVED, &corrupt);
	UT_ASSERT(corrupt);
}


/* ============================================================
 * U13 — removal-marker pack/unpack/preserve + majority authority decide
 * ============================================================ */

static void
mk_marker(ClusterRemovalMarker *m, int32 node, uint64 epoch, int phase, uint64 incarnation,
		  uint64 event_id)
{
	memset(m, 0, sizeof(*m));
	m->magic = CLUSTER_REMOVAL_MARKER_MAGIC;
	m->version = CLUSTER_REMOVAL_MARKER_VERSION;
	m->phase = (uint16) phase;
	m->removed_node_id = node;
	m->remove_epoch = epoch;
	m->removed_incarnation = incarnation;
	m->removal_event_id = event_id;
	cluster_removal_marker_compute_crc(m);
}

UT_TEST(test_marker_struct_valid)
{
	ClusterRemovalMarker m;

	mk_marker(&m, 1, 7, CLUSTER_REMOVAL_MARKER_SHRUNK, 42, 0xABCDEF);
	UT_ASSERT(cluster_removal_marker_struct_valid(&m, 1));

	/* wrong expected node */
	UT_ASSERT(!cluster_removal_marker_struct_valid(&m, 2));

	/* bad magic */
	m.magic = 0xDEADBEEF;
	UT_ASSERT(!cluster_removal_marker_struct_valid(&m, 1));

	/* bad crc (tamper after compute) */
	mk_marker(&m, 1, 7, CLUSTER_REMOVAL_MARKER_SHRUNK, 42, 0xABCDEF);
	m.remove_epoch = 8;
	UT_ASSERT(!cluster_removal_marker_struct_valid(&m, 1));

	/* unknown phase rejected */
	mk_marker(&m, 1, 7, 9 /* bogus */, 42, 0xABCDEF);
	UT_ASSERT(!cluster_removal_marker_struct_valid(&m, 1));
}

UT_TEST(test_marker_pack_unpack_preserve)
{
	ClusterRemovalMarker m, out;
	uint8 buf[368];
	uint8 buf2[368];

	/* pack into reserved1[64..], unpack reads it back identically */
	mk_marker(&m, 3, 11, CLUSTER_REMOVAL_MARKER_REMOVED, 99, 0x1234);
	memset(buf, 0, sizeof(buf));
	cluster_removal_marker_pack(buf, &m);
	UT_ASSERT(cluster_removal_marker_unpack(buf, &out));
	UT_ASSERT_EQ((int)out.removed_node_id, 3);
	UT_ASSERT_EQ((int)out.phase, (int)CLUSTER_REMOVAL_MARKER_REMOVED);
	UT_ASSERT(cluster_removal_marker_struct_valid(&out, 3));

	/* pack must NOT touch the fence-marker region [0..64) */
	{
		int i;
		bool clean = true;
		for (i = 0; i < CLUSTER_REMOVAL_MARKER_RESERVED1_OFFSET; i++)
			if (buf[i] != 0)
				clean = false;
		UT_ASSERT(clean);
	}

	/* empty slot -> unpack fails (magic miss) */
	memset(buf2, 0, sizeof(buf2));
	UT_ASSERT(!cluster_removal_marker_unpack(buf2, &out));

	/* preserve_per_disk carries this disk's own prior marker forward */
	memset(buf2, 0, sizeof(buf2));
	cluster_removal_marker_preserve_per_disk(buf2, buf);
	UT_ASSERT(cluster_removal_marker_unpack(buf2, &out));
	UT_ASSERT_EQ((int)out.removed_node_id, 3);

	/* preserve from an empty prior leaves the new slot empty (no marker) */
	memset(buf, 0, sizeof(buf));
	memset(buf2, 0, sizeof(buf2));
	cluster_removal_marker_preserve_per_disk(buf2, buf);
	UT_ASSERT(!cluster_removal_marker_unpack(buf2, &out));
}

UT_TEST(test_marker_authority_decide)
{
	ClusterRemovalMarker disk[3];
	bool has[3];
	ClusterRemovalMarker out;

	/* all three identical SHRUNK -> authority = that marker */
	mk_marker(&disk[0], 1, 7, CLUSTER_REMOVAL_MARKER_SHRUNK, 42, 0xAA);
	mk_marker(&disk[1], 1, 7, CLUSTER_REMOVAL_MARKER_SHRUNK, 42, 0xAA);
	mk_marker(&disk[2], 1, 7, CLUSTER_REMOVAL_MARKER_SHRUNK, 42, 0xAA);
	has[0] = has[1] = has[2] = true;
	UT_ASSERT(cluster_removal_marker_authority_decide(disk, has, 3, &out));
	UT_ASSERT_EQ((int)out.phase, (int)CLUSTER_REMOVAL_MARKER_SHRUNK);

	/* majority (2/3) advanced to REMOVED -> authority = REMOVED */
	mk_marker(&disk[0], 1, 7, CLUSTER_REMOVAL_MARKER_REMOVED, 42, 0xAA);
	mk_marker(&disk[1], 1, 7, CLUSTER_REMOVAL_MARKER_REMOVED, 42, 0xAA);
	mk_marker(&disk[2], 1, 7, CLUSTER_REMOVAL_MARKER_SHRUNK, 42, 0xAA);
	has[0] = has[1] = has[2] = true;
	UT_ASSERT(cluster_removal_marker_authority_decide(disk, has, 3, &out));
	UT_ASSERT_EQ((int)out.phase, (int)CLUSTER_REMOVAL_MARKER_REMOVED);

	/* no identical tuple reaches majority -> no authority (anti-amplification) */
	mk_marker(&disk[0], 1, 7, CLUSTER_REMOVAL_MARKER_REMOVED, 42, 0xAA);
	mk_marker(&disk[1], 1, 7, CLUSTER_REMOVAL_MARKER_SHRUNK, 42, 0xAA);
	has[0] = true;
	has[1] = true;
	has[2] = false; /* one disk has no marker */
	UT_ASSERT(!cluster_removal_marker_authority_decide(disk, has, 3, &out));

	/* a single disk's marker (1/3) is a minority -> not authority */
	mk_marker(&disk[0], 1, 7, CLUSTER_REMOVAL_MARKER_SHRUNK, 42, 0xAA);
	has[0] = true;
	has[1] = false;
	has[2] = false;
	UT_ASSERT(!cluster_removal_marker_authority_decide(disk, has, 3, &out));
}


/* ============================================================
 * U9 — IC payload integrity
 * ============================================================ */

UT_TEST(test_ic_payloads)
{
	ClusterNodeRemoveAnnouncePayload a;
	ClusterNodeRemoveCleanupAckPayload k;

	/* announce: compute crc -> valid; tamper -> invalid */
	memset(&a, 0, sizeof(a));
	a.magic = CLUSTER_NODE_REMOVE_IC_MAGIC;
	a.version = CLUSTER_NODE_REMOVE_IC_VERSION;
	a.coordinator_node_id = 0;
	a.target_node_id = 1;
	a.remove_epoch = 5;
	a.removal_event_id = 0xBEEF;
	cluster_node_remove_announce_compute_crc(&a);
	UT_ASSERT(cluster_node_remove_announce_payload_valid(&a));

	a.remove_epoch = 6; /* tamper after crc */
	UT_ASSERT(!cluster_node_remove_announce_payload_valid(&a));

	/* bad magic */
	cluster_node_remove_announce_compute_crc(&a);
	a.magic = 0xDEADBEEF;
	UT_ASSERT(!cluster_node_remove_announce_payload_valid(&a));

	/* bad version */
	a.magic = CLUSTER_NODE_REMOVE_IC_MAGIC;
	a.version = 0;
	cluster_node_remove_announce_compute_crc(&a);
	UT_ASSERT(!cluster_node_remove_announce_payload_valid(&a));

	/* out-of-range target node id */
	memset(&a, 0, sizeof(a));
	a.magic = CLUSTER_NODE_REMOVE_IC_MAGIC;
	a.version = CLUSTER_NODE_REMOVE_IC_VERSION;
	a.target_node_id = 99999;
	cluster_node_remove_announce_compute_crc(&a);
	UT_ASSERT(!cluster_node_remove_announce_payload_valid(&a));

	/* ack: compute crc -> valid; tamper -> invalid */
	memset(&k, 0, sizeof(k));
	k.magic = CLUSTER_NODE_REMOVE_IC_MAGIC;
	k.version = CLUSTER_NODE_REMOVE_IC_VERSION;
	k.survivor_node_id = 0;
	k.target_node_id = 1;
	k.remove_epoch = 5;
	k.removal_event_id = 0xBEEF;
	cluster_node_remove_ack_compute_crc(&k);
	UT_ASSERT(cluster_node_remove_ack_payload_valid(&k));

	k.removal_event_id = 0xFEED; /* tamper */
	UT_ASSERT(!cluster_node_remove_ack_payload_valid(&k));
}


int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_struct_layout);
	UT_RUN(test_phase_transitions);
	UT_RUN(test_ordering_gate);
	UT_RUN(test_fence_timeout_classify);
	UT_RUN(test_contest_classify);
	UT_RUN(test_precheck_matrix);
	UT_RUN(test_recover_matrix);
	UT_RUN(test_marker_struct_valid);
	UT_RUN(test_marker_pack_unpack_preserve);
	UT_RUN(test_marker_authority_decide);
	UT_RUN(test_ic_payloads);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
