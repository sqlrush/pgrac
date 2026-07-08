/*-------------------------------------------------------------------------
 *
 * test_cluster_clean_leave.c
 *	  Unit tests for spec-5.13 clean leave reconfiguration policy helpers.
 *
 *	  Exercises the pure decision layer (cluster_clean_leave_policy.c):
 *	  phase-FSM transition validity, the writable-only quiesce gate, the
 *	  version-coherent leave check, and leave-intent marker structural
 *	  validation; plus the shmem struct-layout invariants.  The runtime
 *	  driver (shmem, voting-disk marker I/O, ProcSignal quiesce, GES/GCS
 *	  drain, LMON orchestration) lives in cluster_clean_leave.c and is
 *	  covered by cluster_tap t/NNN (2-node ClusterPair).
 *
 *	  Test IDs map to spec-5.13 §D15:
 *	    U1  shmem region size + phase enum + marker size invariants
 *	    U2  phase state machine: legal forward path + illegal edges rejected
 *	    U3  version-coherent abort (epoch / dead_generation bump => incoherent)
 *	    U5  writable-only quiesce gate (writable abort / read-only+idle absorb)
 *	    U7  survivor leave-epoch observe gate (CL-I10 stable baseline)
 *	    U8  CL-I5 storage-fallback serve gate (true + false reject branch)
 *	    U9  leave-intent marker magic / version / CRC / identity validation
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_clean_leave.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.13-clean-leave-reconfig.md.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "cluster/cluster_clean_leave.h"

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
	/* shmem state fits the reserved budget (StaticAssert mirror, §2.1; the §2.5
	 * marker mailbox + Hardening v1.0.1/v1.0.2 fields put it past the original
	 * 256B budget, so the bound is 512 — same as the production StaticAssertDecl). */
	UT_ASSERT(sizeof(ClusterLeaveState) <= 512);
	/* durable marker fits one 512B voting-disk slot (§2.5) */
	UT_ASSERT(sizeof(ClusterLeaveIntentMarker) <= 512);

	/* 9-state phase enum, IDLE=0 (atomic-stored default) */
	UT_ASSERT_EQ((int)CLUSTER_LEAVE_IDLE, 0);
	UT_ASSERT_EQ((int)CLUSTER_LEAVE_REQUESTED, 1);
	UT_ASSERT_EQ((int)CLUSTER_LEAVE_COMMITTED, 6);
	UT_ASSERT_EQ((int)CLUSTER_LEAVE_ABORTED, 7);
	UT_ASSERT_EQ((int)CLUSTER_LEAVE_ABORTED_ESCALATE, 8);

	UT_ASSERT_STR_EQ(cluster_clean_leave_phase_str(CLUSTER_LEAVE_GCS_FLUSHING), "gcs_flushing");
	UT_ASSERT_STR_EQ(cluster_clean_leave_phase_str(CLUSTER_LEAVE_ABORTED_ESCALATE),
					 "aborted_escalate");
	UT_ASSERT_STR_EQ(cluster_clean_leave_phase_str(999), "(unknown)");
}


/* ============================================================
 * U2 — phase state machine transitions
 * ============================================================ */

UT_TEST(test_phase_fsm)
{
	/* legal forward drain path */
	UT_ASSERT(
		cluster_clean_leave_phase_valid_transition(CLUSTER_LEAVE_IDLE, CLUSTER_LEAVE_REQUESTED));
	UT_ASSERT(cluster_clean_leave_phase_valid_transition(CLUSTER_LEAVE_REQUESTED,
														 CLUSTER_LEAVE_QUIESCING));
	UT_ASSERT(cluster_clean_leave_phase_valid_transition(CLUSTER_LEAVE_QUIESCING,
														 CLUSTER_LEAVE_GES_DRAINING));
	UT_ASSERT(cluster_clean_leave_phase_valid_transition(CLUSTER_LEAVE_GES_DRAINING,
														 CLUSTER_LEAVE_GCS_FLUSHING));
	UT_ASSERT(cluster_clean_leave_phase_valid_transition(CLUSTER_LEAVE_GCS_FLUSHING,
														 CLUSTER_LEAVE_BARRIER_WAIT));
	UT_ASSERT(cluster_clean_leave_phase_valid_transition(CLUSTER_LEAVE_BARRIER_WAIT,
														 CLUSTER_LEAVE_COMMITTED));
	UT_ASSERT(
		cluster_clean_leave_phase_valid_transition(CLUSTER_LEAVE_COMMITTED, CLUSTER_LEAVE_IDLE));

	/* clean abort only from REQUESTED (nothing drained yet) */
	UT_ASSERT(
		cluster_clean_leave_phase_valid_transition(CLUSTER_LEAVE_REQUESTED, CLUSTER_LEAVE_ABORTED));
	UT_ASSERT(
		cluster_clean_leave_phase_valid_transition(CLUSTER_LEAVE_ABORTED, CLUSTER_LEAVE_IDLE));
	/* a drained phase may NOT clean-abort (must escalate) */
	UT_ASSERT(!cluster_clean_leave_phase_valid_transition(CLUSTER_LEAVE_GES_DRAINING,
														  CLUSTER_LEAVE_ABORTED));

	/* escalate reachable from every active phase */
	UT_ASSERT(cluster_clean_leave_phase_valid_transition(CLUSTER_LEAVE_REQUESTED,
														 CLUSTER_LEAVE_ABORTED_ESCALATE));
	UT_ASSERT(cluster_clean_leave_phase_valid_transition(CLUSTER_LEAVE_GCS_FLUSHING,
														 CLUSTER_LEAVE_ABORTED_ESCALATE));
	UT_ASSERT(cluster_clean_leave_phase_valid_transition(CLUSTER_LEAVE_BARRIER_WAIT,
														 CLUSTER_LEAVE_ABORTED_ESCALATE));
	/* but NOT from IDLE (no leave in progress) or terminal phases */
	UT_ASSERT(!cluster_clean_leave_phase_valid_transition(CLUSTER_LEAVE_IDLE,
														  CLUSTER_LEAVE_ABORTED_ESCALATE));
	UT_ASSERT(!cluster_clean_leave_phase_valid_transition(CLUSTER_LEAVE_COMMITTED,
														  CLUSTER_LEAVE_ABORTED_ESCALATE));

	/* illegal: skipping phases / going backward */
	UT_ASSERT(
		!cluster_clean_leave_phase_valid_transition(CLUSTER_LEAVE_IDLE, CLUSTER_LEAVE_QUIESCING));
	UT_ASSERT(!cluster_clean_leave_phase_valid_transition(CLUSTER_LEAVE_GES_DRAINING,
														  CLUSTER_LEAVE_COMMITTED));
	UT_ASSERT(!cluster_clean_leave_phase_valid_transition(CLUSTER_LEAVE_QUIESCING,
														  CLUSTER_LEAVE_REQUESTED));
	UT_ASSERT(!cluster_clean_leave_phase_valid_transition(CLUSTER_LEAVE_COMMITTED,
														  CLUSTER_LEAVE_QUIESCING));
}


/* ============================================================
 * U3 — version-coherent leave
 * ============================================================ */

UT_TEST(test_version_coherent)
{
	/* spec-2.29a ②b: coherence = epoch unchanged AND the others-dead bitmap
	 * (dead set EXCLUDING the leaving node) unchanged.  The reflexive-case
	 * matrix from the spec §②b 8.A argument, exercised on the pure predicate. */
	uint8 od_none[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES] = { 0 };
	uint8 od_third[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES] = { 0 };
	const int n = CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES;

	od_third[0] = 0x04; /* a THIRD-PARTY node (e.g. node 2) became DEAD */

	/* (i) nothing moved (leaving node's own DEAD is already excluded upstream,
	 *     so its transition does not appear here) -> coherent */
	UT_ASSERT(cluster_clean_leave_version_coherent(7, 7, od_none, od_none, n));
	/* (iii) epoch bumped by an external fail-stop -> incoherent */
	UT_ASSERT(!cluster_clean_leave_version_coherent(7, 8, od_none, od_none, n));
	/* (ii) a third-party death entered the others-dead set, epoch not yet
	 *      bumped (the P1-b window) -> incoherent */
	UT_ASSERT(!cluster_clean_leave_version_coherent(7, 7, od_none, od_third, n));
	/* (iv) both moved -> incoherent */
	UT_ASSERT(!cluster_clean_leave_version_coherent(7, 8, od_none, od_third, n));
	/* fail-closed on a missing view */
	UT_ASSERT(!cluster_clean_leave_version_coherent(7, 7, NULL, od_none, n));
	UT_ASSERT(!cluster_clean_leave_version_coherent(7, 7, od_none, NULL, n));
}


/* ============================================================
 * U3b — leaver barrier-tick own-commit latch (spec-2.29a r2 P2-1)
 * ============================================================ */

UT_TEST(test_own_commit_latched)
{
	/* own commit latches only when the epoch advanced AND both the others-dead
	 * bitmap and the scalar dead_generation are unchanged. */

	/* clean commit: epoch advanced, nothing else moved -> latch */
	UT_ASSERT(cluster_clean_leave_own_commit_latched(true, true, true));

	/* epoch has not advanced yet -> not our commit (keep waiting) */
	UT_ASSERT(!cluster_clean_leave_own_commit_latched(false, true, true));

	/* a third-party death is currently in the others-dead set -> escalate */
	UT_ASSERT(!cluster_clean_leave_own_commit_latched(true, false, true));

	/* r2 P2-1 rebound case: the others-dead bitmap has REBOUND to its bound
	 * value (a third-party false-DEAD then recovered) but the scalar
	 * dead_generation ADVANCED — the non-monotone bitmap must NOT be trusted;
	 * the scalar conjunct forces escalate instead of a false latch/hang. */
	UT_ASSERT(!cluster_clean_leave_own_commit_latched(true, true, false));

	/* both diverged -> escalate */
	UT_ASSERT(!cluster_clean_leave_own_commit_latched(true, false, false));
}


/* ============================================================
 * U5 — writable-only quiesce gate
 * ============================================================ */

UT_TEST(test_writable_only_gate)
{
	/* writable tx (in tx + has top xid) -> abort */
	UT_ASSERT(cluster_clean_leave_should_abort_writable(true, true));
	/* read-only (in tx, no xid) -> absorb */
	UT_ASSERT(!cluster_clean_leave_should_abort_writable(true, false));
	/* idle / post-commit (not in tx) -> absorb */
	UT_ASSERT(!cluster_clean_leave_should_abort_writable(false, false));
	UT_ASSERT(!cluster_clean_leave_should_abort_writable(false, true));
}


/* ============================================================
 * U9 — leave-intent marker validation
 * ============================================================ */

static ClusterLeaveIntentMarker
make_marker(int32 leaving_node, uint8 phase)
{
	ClusterLeaveIntentMarker m;

	memset(&m, 0, sizeof(m));
	m.magic = CLUSTER_LEAVE_MARKER_MAGIC;
	m.version = CLUSTER_LEAVE_MARKER_VERSION;
	m.leaving_node_id = leaving_node;
	m.leave_epoch = 5;
	m.event_id = 42;
	m.dead_bitmap[leaving_node / 8] = (uint8)(1u << (leaving_node % 8));
	m.cssd_dead_generation = 1;
	m.written_at = 0;
	m.phase = phase;
	cluster_clean_leave_marker_compute_crc(&m);
	return m;
}

UT_TEST(test_marker_validation)
{
	ClusterLeaveIntentMarker m;

	/* well-formed COMMITTED marker for node 1 -> struct-valid + committed basis */
	m = make_marker(1, CLUSTER_LEAVE_MARKER_PHASE_COMMITTED);
	UT_ASSERT(cluster_clean_leave_marker_struct_valid(&m, 1));
	UT_ASSERT(cluster_clean_leave_marker_is_committed_basis(&m, 1));

	/* COMMITTING / REQUESTED are struct-valid but NOT a committed rebuild basis */
	m = make_marker(1, CLUSTER_LEAVE_MARKER_PHASE_COMMITTING);
	UT_ASSERT(cluster_clean_leave_marker_struct_valid(&m, 1));
	UT_ASSERT(!cluster_clean_leave_marker_is_committed_basis(&m, 1));
	m = make_marker(1, CLUSTER_LEAVE_MARKER_PHASE_REQUESTED);
	UT_ASSERT(!cluster_clean_leave_marker_is_committed_basis(&m, 1));

	/* wrong expected leaving node -> invalid */
	m = make_marker(1, CLUSTER_LEAVE_MARKER_PHASE_COMMITTED);
	UT_ASSERT(!cluster_clean_leave_marker_struct_valid(&m, 2));

	/* bad magic -> invalid */
	m = make_marker(1, CLUSTER_LEAVE_MARKER_PHASE_COMMITTED);
	m.magic = 0xDEADBEEF;
	UT_ASSERT(!cluster_clean_leave_marker_struct_valid(&m, 1));

	/* corrupted CRC (tamper a field after CRC) -> invalid */
	m = make_marker(1, CLUSTER_LEAVE_MARKER_PHASE_COMMITTED);
	m.leave_epoch = 9999; /* not re-CRC'd */
	UT_ASSERT(!cluster_clean_leave_marker_struct_valid(&m, 1));

	/* dead_bitmap naming an extra node -> invalid (must be exactly {N}) */
	m = make_marker(1, CLUSTER_LEAVE_MARKER_PHASE_COMMITTED);
	m.dead_bitmap[0] |= 0x04; /* set node 2 too */
	cluster_clean_leave_marker_compute_crc(&m);
	UT_ASSERT(!cluster_clean_leave_marker_struct_valid(&m, 1));

	/* version 0 / future version -> invalid */
	m = make_marker(1, CLUSTER_LEAVE_MARKER_PHASE_COMMITTED);
	m.version = 0;
	cluster_clean_leave_marker_compute_crc(&m);
	UT_ASSERT(!cluster_clean_leave_marker_struct_valid(&m, 1));
}


/* ============================================================
 * U7 — survivor leave-epoch observe gate (CL-I10)
 * ============================================================ */

UT_TEST(test_should_invalidate)
{
	/* no leave bound -> never invalidate (even at high observed epoch) */
	UT_ASSERT(!cluster_clean_leave_should_invalidate(100, 0));
	/* observed epoch not yet reached the bound leave epoch -> wait */
	UT_ASSERT(!cluster_clean_leave_should_invalidate(6, 7));
	/* observed epoch == leave epoch -> invalidate (boundary reached) */
	UT_ASSERT(cluster_clean_leave_should_invalidate(7, 7));
	/* observed epoch past the leave epoch -> invalidate */
	UT_ASSERT(cluster_clean_leave_should_invalidate(8, 7));
}


/* ============================================================
 * U8 — CL-I5 storage-fallback serve gate (true AND false branch)
 * ============================================================ */

UT_TEST(test_serve_gate)
{
	/* a non-leaving block always serves the normal way */
	UT_ASSERT(cluster_clean_leave_serve_gate_allows(false, false));
	UT_ASSERT(cluster_clean_leave_serve_gate_allows(false, true));
	/* a leaving-node block NOT yet flushed+invalidated -> fail-closed
	 * (the real reject branch; L362) */
	UT_ASSERT(!cluster_clean_leave_serve_gate_allows(true, false));
	/* a leaving-node block flushed + cache invalidated -> storage-current OK */
	UT_ASSERT(cluster_clean_leave_serve_gate_allows(true, true));
}


/* ============================================================
 * U9 — IC payload magic / version / CRC / identity validation (D8)
 * ============================================================ */

UT_TEST(test_ic_payload_validation)
{
	ClusterLeaveAnnouncePayload a;
	ClusterLeaveAckPayload ack;

	/* well-formed announce -> valid */
	memset(&a, 0, sizeof(a));
	a.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	a.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	a.leaving_node_id = 1;
	a.preflight = 0;
	a.leave_epoch = 7;
	a.cssd_dead_generation = 3;
	cluster_clean_leave_announce_compute_crc(&a);
	UT_ASSERT(cluster_clean_leave_announce_payload_valid(&a));

	/* bad magic -> invalid */
	a.magic = 0xDEADBEEF;
	UT_ASSERT(!cluster_clean_leave_announce_payload_valid(&a));
	a.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;

	/* version 0 / future version -> invalid (re-CRC each time) */
	a.version = 0;
	cluster_clean_leave_announce_compute_crc(&a);
	UT_ASSERT(!cluster_clean_leave_announce_payload_valid(&a));
	a.version = CLUSTER_CLEAN_LEAVE_IC_VERSION + 1;
	cluster_clean_leave_announce_compute_crc(&a);
	UT_ASSERT(!cluster_clean_leave_announce_payload_valid(&a));

	/*
	 * Hardening v1.0.3 (P2): an OLD payload version (v1, pre-nonce) must be
	 * DROPPED, not accepted — the wider v2 frame would misparse a v1 sender's
	 * narrower bytes, and mixed-version fail-closed must not rely on a CRC-offset
	 * accident.  CRC is recomputed over the v2 layout so it is valid; ONLY the
	 * exact-version gate may reject this, isolating the fix.
	 */
	a.version = 1;
	cluster_clean_leave_announce_compute_crc(&a);
	UT_ASSERT(!cluster_clean_leave_announce_payload_valid(&a));
	a.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;

	/* tampered field after CRC -> invalid */
	cluster_clean_leave_announce_compute_crc(&a);
	a.leave_epoch = 9999;
	UT_ASSERT(!cluster_clean_leave_announce_payload_valid(&a));

	/* out-of-range leaving node id -> invalid */
	a.leaving_node_id = -1;
	cluster_clean_leave_announce_compute_crc(&a);
	UT_ASSERT(!cluster_clean_leave_announce_payload_valid(&a));
	a.leaving_node_id = 100000;
	cluster_clean_leave_announce_compute_crc(&a);
	UT_ASSERT(!cluster_clean_leave_announce_payload_valid(&a));

	/* well-formed ACK -> valid */
	memset(&ack, 0, sizeof(ack));
	ack.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	ack.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	ack.survivor_node_id = 0;
	ack.leaving_node_id = 1;
	ack.leave_epoch = 7;
	ack.nak = 0;
	cluster_clean_leave_ack_compute_crc(&ack);
	UT_ASSERT(cluster_clean_leave_ack_payload_valid(&ack));

	/* Hardening v1.0.3 (P2): old ACK version (v1) dropped too (exact-version gate). */
	ack.version = 1;
	cluster_clean_leave_ack_compute_crc(&ack);
	UT_ASSERT(!cluster_clean_leave_ack_payload_valid(&ack));
	ack.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	cluster_clean_leave_ack_compute_crc(&ack);

	/* tampered ACK (flip nak after CRC) -> invalid */
	ack.nak = 1;
	UT_ASSERT(!cluster_clean_leave_ack_payload_valid(&ack));

	/* bad magic ACK -> invalid */
	ack.nak = 0;
	cluster_clean_leave_ack_compute_crc(&ack);
	ack.magic = 0;
	UT_ASSERT(!cluster_clean_leave_ack_payload_valid(&ack));
}


/* ============================================================
 * Test runner
 * ============================================================ */

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_struct_layout);
	UT_RUN(test_phase_fsm);
	UT_RUN(test_version_coherent);
	UT_RUN(test_own_commit_latched);
	UT_RUN(test_writable_only_gate);
	UT_RUN(test_marker_validation);
	UT_RUN(test_should_invalidate);
	UT_RUN(test_serve_gate);
	UT_RUN(test_ic_payload_validation);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
