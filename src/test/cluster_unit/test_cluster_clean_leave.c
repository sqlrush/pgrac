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


static char *
read_source_path(const char *path)
{
	FILE *file;
	long length;
	char *source;

	file = fopen(path, "rb");
	UT_ASSERT_NOT_NULL(file);
	if (file == NULL)
		return NULL;
	UT_ASSERT_EQ(fseek(file, 0, SEEK_END), 0);
	length = ftell(file);
	UT_ASSERT(length > 0);
	UT_ASSERT_EQ(fseek(file, 0, SEEK_SET), 0);
	source = malloc((size_t)length + 1);
	UT_ASSERT_NOT_NULL(source);
	if (source == NULL) {
		fclose(file);
		return NULL;
	}
	UT_ASSERT_EQ(fread(source, 1, (size_t)length, file), (size_t)length);
	source[length] = '\0';
	fclose(file);
	return source;
}


static const char *
find_in_order(const char *cursor, const char *end, const char *needle)
{
	const char *found = strstr(cursor, needle);

	UT_ASSERT_NOT_NULL(found);
	if (found == NULL)
		return NULL;
	UT_ASSERT(found < end);
	if (found >= end)
		return NULL;
	return found + strlen(needle);
}


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
 * U3b — leaver barrier-tick own-commit latch (spec-2.29a r3, evidence
 *        over inference)
 * ============================================================ */

UT_TEST(test_own_commit_latched)
{
	/* The latch verdict is EVIDENCE-only: the first argument is "the durable
	 * COMMITTED marker for THIS leave attempt was confirmed" (the coordinator's
	 * nonce-bound LEAVE_COMMITTED attestation).  The two coherence observations
	 * (others-dead bitmap / scalar dead_generation) stay in the signature as
	 * contract inputs the verdict must IGNORE — a third-party transient
	 * false-DEAD flap on the leaver's local CSSD view advances the (monotone)
	 * dead_generation and can transiently disturb the bitmap, and neither may
	 * refuse a latch that marker evidence backs (the t/331 C1/C4 false-
	 * escalation), nor may any combination latch without evidence (the r2 P2-1
	 * refused-leave mis-latch hang). */

	/* (a) evidence present, no flap noise -> latch */
	UT_ASSERT(cluster_clean_leave_own_commit_latched(true, true, true));

	/* (d) PINNING LEG (t/331 C1/C4 regression): a third-party flap advanced the
	 * scalar dead_generation (it never rebounds) while the bitmap rebounded to
	 * its bound value — WITH marker evidence the leave still latches; the flap
	 * must not false-escalate a committed leave. */
	UT_ASSERT(cluster_clean_leave_own_commit_latched(true, true, false));

	/* (d) flap currently visible in the others-dead bitmap too -> still latch */
	UT_ASSERT(cluster_clean_leave_own_commit_latched(true, false, true));
	UT_ASSERT(cluster_clean_leave_own_commit_latched(true, false, false));

	/* (c) no evidence -> never latch, whatever the coherence observations say
	 * (a refused leave never produces a COMMITTED marker, so the barrier
	 * deadline escalation stays armed and bounds the wait — the r2 P2-1
	 * mis-latch wedge stays closed). */
	UT_ASSERT(!cluster_clean_leave_own_commit_latched(false, true, true));
	UT_ASSERT(!cluster_clean_leave_own_commit_latched(false, true, false));
	UT_ASSERT(!cluster_clean_leave_own_commit_latched(false, false, true));
	UT_ASSERT(!cluster_clean_leave_own_commit_latched(false, false, false));
}


/* ============================================================
 * U3c — LEAVE_COMMITTED evidence identity gate (spec-2.29a r3)
 * ============================================================ */

UT_TEST(test_committed_evidence_matches)
{
	/* args: payload_leaving_node, payload_nonce, payload_epoch,
	 *       self_node, current_leaving_node, current_attempt_nonce,
	 *       bound_leave_epoch */

	/* (a) confirmation for THIS node's CURRENT attempt, committed epoch past
	 * the bound baseline -> evidence accepted */
	UT_ASSERT(cluster_clean_leave_committed_evidence_matches(1, 42, 8, 1, 1, 42, 7));

	/* (b) stale identity: a LEAVE_COMMITTED (and through it a COMMITTED
	 * marker) from a PREVIOUS leave attempt of the same node — nonce differs
	 * -> fail-closed, no latch */
	UT_ASSERT(!cluster_clean_leave_committed_evidence_matches(1, 41, 8, 1, 1, 42, 7));

	/* (b) misrouted: confirmation addressed to a different leaving node */
	UT_ASSERT(!cluster_clean_leave_committed_evidence_matches(2, 42, 8, 1, 1, 42, 7));

	/* (b) this node is not currently leaving (idle: leaving_node_id == -1,
	 * or tracking someone else's leave) */
	UT_ASSERT(!cluster_clean_leave_committed_evidence_matches(1, 42, 8, 1, -1, 42, 7));
	UT_ASSERT(!cluster_clean_leave_committed_evidence_matches(1, 42, 8, 1, 2, 42, 7));

	/* (b) committed epoch not past the bound baseline (attested epoch must be
	 * the guarded-CAS successor of the baseline this leave bound) */
	UT_ASSERT(!cluster_clean_leave_committed_evidence_matches(1, 42, 7, 1, 1, 42, 7));
	UT_ASSERT(!cluster_clean_leave_committed_evidence_matches(1, 42, 0, 1, 1, 42, 7));
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

UT_TEST(test_startup_serving_gate)
{
	/* Legacy/unmanaged startup keeps the established behavior.  Once Scheme A
	 * owns readiness, clean leave is ordinary serving work and must wait for
	 * the generation-bound SERVING_READY publication. */
	UT_ASSERT(cluster_clean_leave_startup_serving_allows(false, false));
	UT_ASSERT(cluster_clean_leave_startup_serving_allows(false, true));
	UT_ASSERT(!cluster_clean_leave_startup_serving_allows(true, false));
	UT_ASSERT(cluster_clean_leave_startup_serving_allows(true, true));
}


/* ============================================================
 * U10 — phase-1 coordinated full-cluster clean-stop policy
 * ============================================================ */

UT_TEST(test_phase1_full_stop_probe_policy)
{
	ClusterPhase1FullStopPlan plan;

	memset(&plan, 0, sizeof(plan));
	plan.valid = true;
	plan.epoch = 0;
	plan.attempt_nonce = 41;
	plan.absolute_deadline_us = 9000;
	plan.own_wal_started_at = 77;
	plan.member_incarnations[0] = 101;
	plan.member_incarnations[1] = 102;
	plan.member_incarnations[2] = 103;
	plan.member_incarnations[3] = 104;

	UT_ASSERT(cluster_clean_leave_phase1_full_stop_plan_valid(&plan));
	UT_ASSERT(cluster_clean_leave_phase1_full_stop_probe_accepts(CLUSTER_LEAVE_PRODUCER_SHUTDOWN,
																 true, 2, 2, 0, 0, 51, true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_probe_accepts(
		CLUSTER_LEAVE_PRODUCER_OPERATOR, true, 2, 2, 0, 0, 51, true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_probe_accepts(
		CLUSTER_LEAVE_PRODUCER_SHUTDOWN, false, 2, 2, 0, 0, 51, true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_probe_accepts(
		CLUSTER_LEAVE_PRODUCER_SHUTDOWN, true, 1, 2, 0, 0, 51, true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_probe_accepts(
		CLUSTER_LEAVE_PRODUCER_SHUTDOWN, true, 2, 2, 0, 1, 51, true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_probe_accepts(CLUSTER_LEAVE_PRODUCER_SHUTDOWN,
																  true, 2, 2, 0, 0, 0, true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_probe_accepts(
		CLUSTER_LEAVE_PRODUCER_SHUTDOWN, true, 2, 2, 0, 0, 51, false, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_probe_accepts(
		CLUSTER_LEAVE_PRODUCER_SHUTDOWN, true, 2, 2, 0, 0, 51, true, false));
}

UT_TEST(test_phase1_full_stop_probe_phase_policy)
{
	/* The authenticated sender's existing WAL evidence distinguishes the two
	 * same-wire rounds.  A peer already in the post-STOPPED round must never
	 * receive an ACK from a receiver which is still ACTIVE. */
	UT_ASSERT(cluster_clean_leave_phase1_full_stop_probe_phase_accepts(true, false, true, false));
	UT_ASSERT(cluster_clean_leave_phase1_full_stop_probe_phase_accepts(true, false, false, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_probe_phase_accepts(false, true, true, false));
	UT_ASSERT(cluster_clean_leave_phase1_full_stop_probe_phase_accepts(false, true, false, true));

	/* Missing or contradictory source/local evidence is fail-closed. */
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_probe_phase_accepts(false, false, true, false));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_probe_phase_accepts(true, true, true, false));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_probe_phase_accepts(true, false, false, false));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_probe_phase_accepts(true, false, true, true));
}

UT_TEST(test_phase1_full_stop_post_stopped_request_ahead_consumption_policy)
{
	/* WAL STOPPED publication and local post-STOPPED round arming are distinct
	 * lifecycle edges.  A peer request arriving in that narrow interval must
	 * stay bound to the predecessor local nonce and become consumable after the
	 * one legal nonce transition. */
	UT_ASSERT(cluster_clean_leave_phase1_full_stop_request_ahead_uses_predecessor_nonce(true, false,
																						false));
	UT_ASSERT(cluster_clean_leave_phase1_full_stop_request_ahead_uses_predecessor_nonce(false, true,
																						false));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_request_ahead_uses_predecessor_nonce(
		false, true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_request_ahead_uses_predecessor_nonce(
		false, false, false));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_request_ahead_uses_predecessor_nonce(true, true,
																						 false));

	/* A transport-consumed request which arrived while this receiver was still
	 * ACTIVE remains owned by the receiver.  It becomes consumable exactly once
	 * after the same local attempt advances to STOPPED, dispatches its own peer
	 * requests, and retains the original absolute deadline. */
	UT_ASSERT(cluster_clean_leave_phase1_full_stop_request_ahead_can_consume(
		true, true, 101, 9000, 102, 9000, true, true, true, true));
	/* A request retained after local STOPPED but before dispatch keeps the same
	 * local round nonce rather than accepting another transition. */
	UT_ASSERT(cluster_clean_leave_phase1_full_stop_request_ahead_can_consume(
		true, false, 102, 9000, 102, 9000, true, true, true, true));

	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_request_ahead_can_consume(
		false, true, 101, 9000, 102, 9000, true, true, true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_request_ahead_can_consume(
		true, true, 101, 9000, 101, 9000, true, true, true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_request_ahead_can_consume(
		true, false, 102, 9000, 103, 9000, true, true, true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_request_ahead_can_consume(
		true, true, 101, 9000, 102, 9001, true, true, true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_request_ahead_can_consume(
		true, true, 101, 9000, 102, 9000, false, true, true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_request_ahead_can_consume(
		true, true, 101, 9000, 102, 9000, true, false, true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_request_ahead_can_consume(
		true, true, 101, 9000, 102, 9000, true, true, false, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_request_ahead_can_consume(
		true, true, 101, 9000, 102, 9000, true, true, true, false));
}

UT_TEST(test_phase1_full_stop_post_stopped_request_retains_per_peer_transport_ownership)
{
	char *clean_leave = read_source_path(CLEAN_LEAVE_SOURCE_PATH);
	const char *tick;
	const char *tick_end;

	UT_ASSERT_NOT_NULL(clean_leave);
	if (clean_leave == NULL)
		return;

	/* The post-STOPPED probe is not a best-effort fanout.  Each peer keeps an
	 * exact local ownership bit until the current transport consumes the frame;
	 * NOT_ADMITTED therefore leaves only that peer pending for an event-loop
	 * retry under the original absolute deadline. */
	UT_ASSERT_NOT_NULL(strstr(clean_leave, "phase1_post_stopped_request_sent"));
	tick = strstr(clean_leave, "\ncl_phase1_full_stop_post_stopped_request_lmon_tick(");
	UT_ASSERT_NOT_NULL(tick);
	if (tick == NULL)
		goto out;
	tick_end
		= strstr(tick, "\n}\n\nstatic void\ncl_phase1_full_stop_post_stopped_reply_lmon_tick(");
	UT_ASSERT_NOT_NULL(tick_end);
	if (tick_end == NULL)
		goto out;
	UT_ASSERT_NOT_NULL(find_in_order(tick, tick_end, "phase1_post_stopped_request_sent"));
	UT_ASSERT_NOT_NULL(
		find_in_order(tick, tick_end, "cl_phase1_full_stop_capture_barrier_identity("));
	UT_ASSERT_NOT_NULL(
		find_in_order(tick, tick_end, "local_wal_state != CLUSTER_WAL_SLOT_STATE_STOPPED"));
	UT_ASSERT_NOT_NULL(
		find_in_order(tick, tick_end, "cl_phase1_full_stop_send_post_stopped_request("));
	UT_ASSERT_NOT_NULL(find_in_order(tick, tick_end, "CLUSTER_IC_SEND_NOT_ADMITTED"));
	UT_ASSERT(strstr(tick, "cluster_clean_leave_ic_broadcast_announce(") == NULL
			  || strstr(tick, "cluster_clean_leave_ic_broadcast_announce(") >= tick_end);
out:
	free(clean_leave);
}

UT_TEST(test_phase1_full_stop_early_receiver_retains_semantic_ownership)
{
	char *clean_leave = read_source_path(CLEAN_LEAVE_SOURCE_PATH);
	const char *handler;
	const char *handler_end;
	const char *consume;
	const char *consume_end;
	const char *consume_locked;
	const char *consume_locked_end;

	UT_ASSERT_NOT_NULL(clean_leave);
	if (clean_leave == NULL)
		return;
	handler = strstr(clean_leave, "\ncl_announce_handler(");
	UT_ASSERT_NOT_NULL(handler);
	if (handler == NULL)
		goto out;
	handler_end = strstr(handler, "\n\t/* Disabled survivor:");
	UT_ASSERT_NOT_NULL(handler_end);
	if (handler_end == NULL)
		goto out;

	/* DONE/WOULD_BLOCK has transferred the frame to this LMON.  A valid peer
	 * STOPPED request which is one stage ahead must enter the bounded exact
	 * receiver slot; the handler may not silently return and discard it. */
	UT_ASSERT_NOT_NULL(
		find_in_order(handler, handler_end, "cl_phase1_full_stop_retain_request_ahead_locked("));
	UT_ASSERT(
		strstr(handler,
			   "cl_phase1_member_bit_clear(\n\t\t\t\t\t\tcl_phase1_post_stopped_request_sent")
			== NULL
		|| strstr(handler,
				  "cl_phase1_member_bit_clear(\n\t\t\t\t\t\tcl_phase1_post_stopped_request_sent")
			   >= handler_end);

	consume = strstr(clean_leave, "\ncl_phase1_full_stop_consume_request_ahead_lmon_tick(");
	UT_ASSERT_NOT_NULL(consume);
	if (consume == NULL)
		goto out;
	consume_end
		= strstr(consume, "\n}\n\nstatic void\ncl_phase1_full_stop_post_stopped_reply_lmon_tick(");
	UT_ASSERT_NOT_NULL(consume_end);
	if (consume_end == NULL)
		goto out;
	UT_ASSERT_NOT_NULL(
		find_in_order(consume, consume_end, "cl_phase1_full_stop_consume_request_ahead_locked("));

	consume_locked = strstr(clean_leave, "\ncl_phase1_full_stop_consume_request_ahead_locked(");
	UT_ASSERT_NOT_NULL(consume_locked);
	if (consume_locked == NULL)
		goto out;
	consume_locked_end
		= strstr(consume_locked, "\n}\n\n\nstatic void\ncl_phase1_full_stop_release(");
	UT_ASSERT_NOT_NULL(consume_locked_end);
	if (consume_locked_end == NULL)
		goto out;
	UT_ASSERT_NOT_NULL(
		find_in_order(consume_locked, consume_locked_end,
					  "cluster_clean_leave_phase1_full_stop_request_ahead_can_consume("));
	UT_ASSERT_NOT_NULL(
		find_in_order(consume_locked, consume_locked_end, "phase1_post_stopped_reply_pending"));
	UT_ASSERT_NOT_NULL(
		find_in_order(consume_locked, consume_locked_end, "memset(ahead, 0, sizeof(*ahead))"));
out:
	free(clean_leave);
}

UT_TEST(test_phase1_full_stop_post_stopped_receiver_requires_confirmed_local_round)
{
	/* A physically visible STOPPED after-image is not enough: its publisher
	 * may still return RELEASE_UNCERTAIN.  The receiver can ACK the second
	 * round only after its own confirmed publisher has armed and dispatched
	 * the existing post-STOPPED round.  Once that round completes locally, the
	 * exact receive window remains open through the bounded release phase so a
	 * slower peer's same-round replay cannot be discarded. */
	UT_ASSERT(cluster_clean_leave_phase1_full_stop_post_stopped_receiver_ready(true, true, true,
																			   true, true, false));
	UT_ASSERT(cluster_clean_leave_phase1_full_stop_post_stopped_receiver_ready(true, true, true,
																			   false, true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_post_stopped_receiver_ready(
		true, true, true, false, true, false));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_post_stopped_receiver_ready(
		true, true, true, true, false, false));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_post_stopped_receiver_ready(false, true, true,
																				true, true, false));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_post_stopped_receiver_ready(true, false, true,
																				true, true, false));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_post_stopped_receiver_ready(true, true, false,
																				true, true, false));
}

UT_TEST(test_phase1_full_stop_release_probe_is_exact_and_phase1_only)
{
	UT_ASSERT(cluster_clean_leave_phase1_full_stop_release_probe_accepts(
		CLUSTER_LEAVE_PRODUCER_SHUTDOWN, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE, 2, 2, 0, 0, 91,
		true, true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_release_probe_accepts(
		CLUSTER_LEAVE_PRODUCER_OPERATOR, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE, 2, 2, 0, 0, 91,
		true, true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_release_probe_accepts(
		CLUSTER_LEAVE_PRODUCER_SHUTDOWN, CLUSTER_PHASE1_FULL_STOP_WIRE_BARRIER, 2, 2, 0, 0, 91,
		true, true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_release_probe_accepts(
		CLUSTER_LEAVE_PRODUCER_SHUTDOWN, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE, 1, 2, 0, 0, 91,
		true, true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_release_probe_accepts(
		CLUSTER_LEAVE_PRODUCER_SHUTDOWN, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE, 2, 2, 1, 0, 91,
		true, true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_release_probe_accepts(
		CLUSTER_LEAVE_PRODUCER_SHUTDOWN, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE, 2, 2, 0, 0, 0, true,
		true, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_release_probe_accepts(
		CLUSTER_LEAVE_PRODUCER_SHUTDOWN, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE, 2, 2, 0, 0, 91,
		false, true, true));
}

UT_TEST(test_phase1_full_stop_exact_receipt_may_precede_local_reply_publication)
{
	/* Receipt delivery proves that the peer consumed our exact reply.  The
	 * sender's local reply_sent publication may lag that cross-process fact;
	 * final completion still checks reply_sent independently. */
	UT_ASSERT(cluster_clean_leave_phase1_full_stop_receipt_accepts(
		CLUSTER_LEAVE_PRODUCER_SHUTDOWN, CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT, true, 91, 91,
		true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_receipt_accepts(
		CLUSTER_LEAVE_PRODUCER_OPERATOR, CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT, true, 91, 91,
		true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_receipt_accepts(
		CLUSTER_LEAVE_PRODUCER_SHUTDOWN, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE, true, 91, 91,
		true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_receipt_accepts(
		CLUSTER_LEAVE_PRODUCER_SHUTDOWN, CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT, false, 91, 91,
		true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_receipt_accepts(
		CLUSTER_LEAVE_PRODUCER_SHUTDOWN, CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT, true, 0, 91, true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_receipt_accepts(
		CLUSTER_LEAVE_PRODUCER_SHUTDOWN, CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT, true, 90, 91,
		true));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_receipt_accepts(
		CLUSTER_LEAVE_PRODUCER_SHUTDOWN, CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT, true, 91, 91,
		false));
}

UT_TEST(test_phase1_full_stop_release_completion_requires_four_way_delivery)
{
	ClusterPhase1FullStopPlan plan;
	uint8 request_sent[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES] = { 0 };
	uint8 request_seen[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES] = { 0 };
	uint8 reply_sent[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES] = { 0 };
	uint8 reply_seen[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES] = { 0 };
	uint8 receipt_sent[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES] = { 0 };
	uint8 receipt_seen[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES] = { 0 };

	memset(&plan, 0, sizeof(plan));
	plan.valid = true;
	plan.epoch = 0;
	plan.attempt_nonce = 91;
	plan.absolute_deadline_us = 9000;
	plan.own_wal_started_at = 77;
	plan.member_incarnations[0] = 101;
	plan.member_incarnations[1] = 102;
	plan.member_incarnations[2] = 103;
	plan.member_incarnations[3] = 104;
	request_sent[0] = request_seen[0] = reply_sent[0] = reply_seen[0] = receipt_sent[0]
		= receipt_seen[0] = 0x0e;

	UT_ASSERT(cluster_clean_leave_phase1_full_stop_release_complete(
		&plan, 0, request_sent, request_seen, reply_sent, reply_seen, receipt_sent, receipt_seen,
		sizeof(request_sent), true));
	request_seen[0] = 0x06;
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_release_complete(
		&plan, 0, request_sent, request_seen, reply_sent, reply_seen, receipt_sent, receipt_seen,
		sizeof(request_sent), true));
	request_seen[0] = 0x0e;
	reply_sent[0] = 0x06;
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_release_complete(
		&plan, 0, request_sent, request_seen, reply_sent, reply_seen, receipt_sent, receipt_seen,
		sizeof(request_sent), true));
	reply_sent[0] = 0x0e;
	receipt_seen[0] = 0x06;
	/* Local egress drain is not evidence that every peer consumed our final
	 * release reply.  Exact per-peer receipts are mandatory. */
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_release_complete(
		&plan, 0, request_sent, request_seen, reply_sent, reply_seen, receipt_sent, receipt_seen,
		sizeof(request_sent), true));
	receipt_seen[0] = 0x0e;
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_release_complete(
		&plan, 0, request_sent, request_seen, reply_sent, reply_seen, receipt_sent, receipt_seen,
		sizeof(request_sent), false));
}

UT_TEST(test_phase1_full_stop_exact_ack_barrier)
{
	ClusterPhase1FullStopPlan plan;
	uint8 ack_bitmap[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES] = { 0 };

	memset(&plan, 0, sizeof(plan));
	plan.valid = true;
	plan.epoch = 0;
	plan.attempt_nonce = 61;
	plan.absolute_deadline_us = 9000;
	plan.own_wal_started_at = 77;
	plan.member_incarnations[0] = 101;
	plan.member_incarnations[1] = 102;
	plan.member_incarnations[2] = 103;
	plan.member_incarnations[3] = 104;

	UT_ASSERT(cluster_clean_leave_phase1_full_stop_ack_matches(&plan, 0, 2, 2, 0, 0, 61, 103));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_ack_matches(&plan, 0, 1, 2, 0, 0, 61, 103));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_ack_matches(&plan, 0, 2, 2, 0, 0, 60, 103));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_ack_matches(&plan, 0, 2, 2, 0, 0, 61, 999));

	ack_bitmap[0] = 0x0e; /* exact peers 1,2,3 for self node 0 */
	UT_ASSERT(cluster_clean_leave_phase1_full_stop_ack_complete(&plan, 0, ack_bitmap,
																sizeof(ack_bitmap)));
	ack_bitmap[0] = 0x06;
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_ack_complete(&plan, 0, ack_bitmap,
																 sizeof(ack_bitmap)));
	ack_bitmap[0] = 0x1e; /* an out-of-contract fifth member/ACK is a drift */
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_ack_complete(&plan, 0, ack_bitmap,
																 sizeof(ack_bitmap)));
}

UT_TEST(test_phase1_full_stop_barrier_waits_for_local_positive_ack_fanout)
{
	char *clean_leave = read_source_path(CLEAN_LEAVE_SOURCE_PATH);
	const char *barrier;
	const char *barrier_end;

	UT_ASSERT_NOT_NULL(clean_leave);
	if (clean_leave == NULL)
		return;
	barrier = strstr(clean_leave, "\ncl_phase1_full_stop_post_stopped_barrier(");
	UT_ASSERT_NOT_NULL(barrier);
	barrier_end
		= barrier == NULL
			  ? NULL
			  : strstr(barrier, "\n}\n\nstatic bool\ncl_phase1_full_stop_release_completion(");
	UT_ASSERT_NOT_NULL(barrier_end);
	if (barrier != NULL && barrier_end != NULL)
		UT_ASSERT_NOT_NULL(find_in_order(barrier, barrier_end, "phase1_post_stopped_reply_sent"));
	free(clean_leave);
}

UT_TEST(test_phase1_full_stop_post_stopped_nonce_is_fresh)
{
	UT_ASSERT(cluster_clean_leave_phase1_full_stop_nonce_fresh(61, 62));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_nonce_fresh(61, 61));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_nonce_fresh(61, 0));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_nonce_fresh(61, UINT64_MAX));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_nonce_fresh(0, 62));
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_nonce_fresh(UINT64_MAX, 62));
}

UT_TEST(test_phase1_full_stop_two_round_nonce_lineage_is_closed)
{
	UT_ASSERT_EQ(cluster_clean_leave_phase1_full_stop_probe_nonce_decide(11, 0, 11),
				 CLUSTER_PHASE1_PROBE_NONCE_STALE_ACTIVE);
	UT_ASSERT_EQ(cluster_clean_leave_phase1_full_stop_probe_nonce_decide(11, 0, 12),
				 CLUSTER_PHASE1_PROBE_NONCE_ACCEPT_STOPPED);
	UT_ASSERT_EQ(cluster_clean_leave_phase1_full_stop_probe_nonce_decide(11, 12, 12),
				 CLUSTER_PHASE1_PROBE_NONCE_DUPLICATE_STOPPED);
	UT_ASSERT_EQ(cluster_clean_leave_phase1_full_stop_probe_nonce_decide(11, 12, 11),
				 CLUSTER_PHASE1_PROBE_NONCE_STALE_ACTIVE);
	UT_ASSERT_EQ(cluster_clean_leave_phase1_full_stop_probe_nonce_decide(11, 12, 13),
				 CLUSTER_PHASE1_PROBE_NONCE_CONFLICT);
	UT_ASSERT_EQ(cluster_clean_leave_phase1_full_stop_probe_nonce_decide(0, 0, 12),
				 CLUSTER_PHASE1_PROBE_NONCE_CONFLICT);
	UT_ASSERT_EQ(cluster_clean_leave_phase1_full_stop_probe_nonce_decide(11, 0, UINT64_MAX),
				 CLUSTER_PHASE1_PROBE_NONCE_CONFLICT);
}

UT_TEST(test_phase1_full_stop_prepare_disposition_is_closed)
{
	UT_ASSERT_EQ(CLUSTER_PHASE1_FULL_STOP_NOT_APPLICABLE, 0);
	UT_ASSERT_EQ(CLUSTER_PHASE1_FULL_STOP_READY, 1);
	UT_ASSERT_EQ(CLUSTER_PHASE1_FULL_STOP_ATTEMPT_FAILED, 2);
}

UT_TEST(test_phase1_full_stop_pgstat_follower_uses_existing_aux_hook)
{
	char *auxprocess = read_source_path(AUXPROCESS_SOURCE_PATH);
	char *pgstat = read_source_path(PGSTAT_SOURCE_PATH);
	const char *aux_hook;
	const char *aux_hook_end;
	const char *pgstat_hook;
	const char *pgstat_hook_end;
	const char *follower;
	const char *report;

	UT_ASSERT_NOT_NULL(auxprocess);
	UT_ASSERT_NOT_NULL(pgstat);
	if (auxprocess == NULL || pgstat == NULL)
		goto out;

	aux_hook = strstr(auxprocess, "\nShutdownAuxiliaryProcess(");
	aux_hook_end = aux_hook == NULL ? NULL : strstr(aux_hook, "\n}");
	UT_ASSERT_NOT_NULL(aux_hook);
	UT_ASSERT_NOT_NULL(aux_hook_end);
	if (aux_hook == NULL || aux_hook_end == NULL)
		goto out;
	UT_ASSERT(find_in_order(aux_hook, aux_hook_end, "pgstat_prepare_for_server_shutdown_follower()")
			  != NULL);

	pgstat_hook = strstr(pgstat, "\npgstat_shutdown_hook(");
	pgstat_hook_end = pgstat_hook == NULL ? NULL : strstr(pgstat_hook, "\n}");
	UT_ASSERT_NOT_NULL(pgstat_hook);
	UT_ASSERT_NOT_NULL(pgstat_hook_end);
	if (pgstat_hook == NULL || pgstat_hook_end == NULL)
		goto out;
	follower = find_in_order(pgstat_hook, pgstat_hook_end, "pgstat_server_shutdown_follower");
	report = find_in_order(pgstat_hook, pgstat_hook_end, "pgstat_report_stat(true)");
	UT_ASSERT_NOT_NULL(follower);
	UT_ASSERT_NOT_NULL(report);
	UT_ASSERT(follower != NULL && report != NULL && follower < report);
	UT_ASSERT_NOT_NULL(strstr(pgstat, "pgstat_prepare_for_server_shutdown_follower(void)"));
out:
	free(auxprocess);
	free(pgstat);
}

UT_TEST(test_phase1_full_stop_runtime_order_and_no_polling)
{
	char *checkpointer = read_source_path(CHECKPOINTER_SOURCE_PATH);
	char *clean_leave = read_source_path(CLEAN_LEAVE_SOURCE_PATH);
	const char *shutdown;
	const char *shutdown_end;
	const char *prepare;
	const char *prepare_failed;
	const char *fail_closed;
	const char *stopped;
	const char *stopped_failure;
	const char *stopped_success;
	const char *close;
	const char *close_failure;
	const char *close_success;
	const char *release_success;
	const char *post_stopped;
	const char *post_stopped_end;
	const char *release_completion;
	const char *release_completion_end;
	const char *close_body;
	const char *close_end;
	const char *legacy;
	const char *prepare_body;
	const char *prepare_end;

	UT_ASSERT_NOT_NULL(checkpointer);
	UT_ASSERT_NOT_NULL(clean_leave);
	if (checkpointer == NULL || clean_leave == NULL)
		goto out;

	shutdown = strstr(checkpointer, "if (ShutdownRequestPending)");
	shutdown_end = shutdown == NULL ? NULL : strstr(shutdown, "proc_exit(0);");
	UT_ASSERT_NOT_NULL(shutdown);
	UT_ASSERT_NOT_NULL(shutdown_end);
	if (shutdown == NULL || shutdown_end == NULL)
		goto out;
	prepare = find_in_order(shutdown, shutdown_end,
							"cluster_clean_leave_phase1_full_stop_prepare_exact(");
	if (prepare == NULL)
		goto out;
	prepare = find_in_order(prepare, shutdown_end, "&phase1_full_stop_plan");
	if (prepare == NULL)
		goto out;
	prepare_failed
		= find_in_order(prepare, shutdown_end, "CLUSTER_PHASE1_FULL_STOP_ATTEMPT_FAILED");
	if (prepare_failed == NULL)
		goto out;
	fail_closed = find_in_order(prepare_failed, shutdown_end, "ereport(PANIC");
	if (fail_closed == NULL)
		goto out;
	prepare = find_in_order(prepare, shutdown_end, "ShutdownXLOG(0, 0)");
	if (prepare == NULL)
		goto out;
	stopped = find_in_order(prepare, shutdown_end,
							"wal_stopped_ok = cluster_wal_state_publish_stopped()");
	if (stopped == NULL)
		goto out;
	stopped_failure = find_in_order(stopped, shutdown_end, "if (!wal_stopped_ok");
	if (stopped_failure == NULL)
		goto out;
	stopped_failure = find_in_order(stopped_failure, shutdown_end, "ereport(PANIC");
	if (stopped_failure == NULL)
		goto out;
	stopped_failure
		= find_in_order(stopped_failure, shutdown_end, "WAL STOPPED publication failed");
	if (stopped_failure == NULL)
		goto out;
	stopped_success = find_in_order(stopped, shutdown_end, "WAL STOPPED published");
	if (stopped_success == NULL)
		goto out;
	close = find_in_order(stopped_success, shutdown_end,
						  "cluster_clean_leave_phase1_full_stop_close_exact(");
	if (close == NULL)
		goto out;
	close = find_in_order(close, shutdown_end, "&phase1_full_stop_plan");
	if (close == NULL)
		goto out;
	close_failure = find_in_order(close, shutdown_end, "if (!clean_handoff_ok");
	if (close_failure == NULL)
		goto out;
	close_failure = find_in_order(close_failure, shutdown_end, "ereport(PANIC");
	if (close_failure == NULL)
		goto out;
	close_failure = find_in_order(close_failure, shutdown_end, "post-STOPPED ACK barrier failed");
	if (close_failure == NULL)
		goto out;
	close_success = find_in_order(close, shutdown_end, "exact four-member ACK barrier complete");
	if (close_success == NULL)
		goto out;
	release_success = find_in_order(close_success, shutdown_end, "release/completion complete");
	UT_ASSERT_NOT_NULL(release_success);
	post_stopped = strstr(clean_leave, "\ncl_phase1_full_stop_post_stopped_barrier(");
	UT_ASSERT_NOT_NULL(post_stopped);
	if (post_stopped == NULL)
		goto out;
	post_stopped_end
		= strstr(post_stopped, "\n}\n\nbool\ncluster_clean_leave_phase1_full_stop_close_exact(");
	UT_ASSERT_NOT_NULL(post_stopped_end);
	if (post_stopped_end == NULL)
		goto out;
	UT_ASSERT_NOT_NULL(strstr(post_stopped, "CLUSTER_WAL_SLOT_STATE_STOPPED"));
	UT_ASSERT_NOT_NULL(strstr(post_stopped, "cl_phase1_full_stop_identity_matches("));
	UT_ASSERT_NOT_NULL(strstr(post_stopped, "plan->absolute_deadline_us"));
	UT_ASSERT_NOT_NULL(strstr(post_stopped, "WaitLatch("));
	UT_ASSERT_NOT_NULL(strstr(post_stopped, "failure-domain stage=post-STOPPED"));
	UT_ASSERT(strstr(post_stopped, "pg_usleep(") == NULL
			  || strstr(post_stopped, "pg_usleep(") >= post_stopped_end);
	UT_ASSERT_NOT_NULL(strstr(clean_leave, "cl_phase1_full_stop_retain_request_ahead_locked("));
	UT_ASSERT_NOT_NULL(strstr(clean_leave, "cl_phase1_full_stop_consume_request_ahead_lmon_tick("));
	UT_ASSERT_NOT_NULL(
		strstr(clean_leave, "cluster_clean_leave_phase1_full_stop_post_stopped_receiver_ready("));
	UT_ASSERT_NOT_NULL(strstr(clean_leave, "phase1_post_stopped_reply_pending"));
	UT_ASSERT_NOT_NULL(strstr(clean_leave, "cl_phase1_full_stop_send_post_stopped_reply("));
	UT_ASSERT_NOT_NULL(strstr(clean_leave, "CLUSTER_IC_SEND_NOT_ADMITTED"));
	release_completion = strstr(clean_leave, "\ncl_phase1_full_stop_release_completion(");
	release_completion_end
		= release_completion == NULL
			  ? NULL
			  : strstr(release_completion,
					   "\n}\n\nbool\ncluster_clean_leave_phase1_full_stop_close_exact(");
	UT_ASSERT_NOT_NULL(release_completion);
	UT_ASSERT_NOT_NULL(release_completion_end);
	if (release_completion != NULL && release_completion_end != NULL) {
		UT_ASSERT_NOT_NULL(strstr(release_completion, "plan->absolute_deadline_us"));
		UT_ASSERT_NOT_NULL(strstr(release_completion, "phase1_release_receipt_seen"));
		UT_ASSERT_NOT_NULL(strstr(release_completion, "failure-domain stage=release-completion"));
		UT_ASSERT_NOT_NULL(strstr(release_completion, "WaitLatch("));
		UT_ASSERT(strstr(release_completion, "pg_usleep(") == NULL
				  || strstr(release_completion, "pg_usleep(") >= release_completion_end);
	}
	close_body = strstr(clean_leave, "\ncluster_clean_leave_phase1_full_stop_close_exact(");
	close_end = close_body == NULL
					? NULL
					: strstr(close_body, "\n}\n\n/*\n * cluster_clean_leave_drive_drain");
	UT_ASSERT_NOT_NULL(close_body);
	UT_ASSERT_NOT_NULL(close_end);
	if (close_body == NULL || close_end == NULL)
		goto out;
	UT_ASSERT(strstr(close_body, "cluster_control_root_thread_clean_close_publish(") == NULL
			  || strstr(close_body, "cluster_control_root_thread_clean_close_publish(")
					 >= close_end);
	post_stopped
		= find_in_order(close_body, close_end, "cl_phase1_full_stop_post_stopped_barrier(plan)");
	UT_ASSERT_NOT_NULL(post_stopped);
	release_completion = post_stopped == NULL
							 ? NULL
							 : find_in_order(post_stopped, close_end,
											 "cl_phase1_full_stop_release_completion(plan)");
	UT_ASSERT_NOT_NULL(release_completion);
	UT_ASSERT_NOT_NULL(strstr(clean_leave, "CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT"));
	legacy = find_in_order(close, shutdown_end, "cluster_clean_leave_shutdown_drain()");
	UT_ASSERT_NOT_NULL(legacy);

	prepare_body = strstr(clean_leave, "\ncluster_clean_leave_phase1_full_stop_prepare_exact(");
	prepare_end = prepare_body == NULL
					  ? NULL
					  : strstr(prepare_body,
							   "\n}\n\nbool\ncluster_clean_leave_phase1_full_stop_close_exact(");
	UT_ASSERT_NOT_NULL(prepare_body);
	UT_ASSERT_NOT_NULL(prepare_end);
	if (prepare_body == NULL || prepare_end == NULL)
		goto out;
	UT_ASSERT(strstr(prepare_body, "WaitLatch(") < prepare_end);
	UT_ASSERT(strstr(prepare_body, "pg_usleep(") == NULL
			  || strstr(prepare_body, "pg_usleep(") >= prepare_end);
	UT_ASSERT(strstr(prepare_body, "cluster_clean_leave_drive_drain(") == NULL
			  || strstr(prepare_body, "cluster_clean_leave_drive_drain(") >= prepare_end);
	UT_ASSERT(strstr(prepare_body, "cluster_authority_serving_rebind_leaver(") == NULL
			  || strstr(prepare_body, "cluster_authority_serving_rebind_leaver(") >= prepare_end);
out:
	free(checkpointer);
	free(clean_leave);
}

UT_TEST(test_phase1_full_stop_retains_coordination_without_r4_admission)
{
	char *postmaster = read_source_path(POSTMASTER_SOURCE_PATH);
	const char *shutdown;
	const char *shutdown_end;
	const char *rf_admission;
	const char *phase1_candidate;
	const char *combined;
	const char *stop_coordination;
	const char *suppress;
	const char *wait_backends;
	const char *wait_backends_end;

	UT_ASSERT_NOT_NULL(postmaster);
	if (postmaster == NULL)
		return;

	shutdown = strstr(postmaster, "if (pmState == PM_STOP_BACKENDS)");
	shutdown_end = shutdown == NULL ? NULL : strstr(shutdown, "pmState = PM_WAIT_BACKENDS;");
	UT_ASSERT_NOT_NULL(shutdown);
	UT_ASSERT_NOT_NULL(shutdown_end);
	if (shutdown == NULL || shutdown_end == NULL)
		goto out;
	rf_admission = find_in_order(shutdown, shutdown_end, "cluster_registry_holds_admission()");
	phase1_candidate = find_in_order(rf_admission, shutdown_end,
									 "cluster_clean_leave_phase1_full_stop_candidate()");
	combined = find_in_order(phase1_candidate, shutdown_end, "retain_shutdown_coordination");
	stop_coordination = find_in_order(combined, shutdown_end, "if (!retain_shutdown_coordination)");
	suppress
		= find_in_order(stop_coordination, shutdown_end,
						"if (!retain_normal_stop && retain_shutdown_coordination && LmonPID != 0)");
	UT_ASSERT_NOT_NULL(suppress);
	/* The new current branch publishes atomic intent; only this unchanged
	 * legacy branch uses the old suppression setter. Native behavior is
	 * exercised in test_cluster_postmaster_stop, not inferred from text. */

	wait_backends = strstr(shutdown_end, "if (pmState == PM_WAIT_BACKENDS)");
	wait_backends_end
		= wait_backends == NULL ? NULL : strstr(wait_backends, "if (pmState == PM_SHUTDOWN_2)");
	UT_ASSERT_NOT_NULL(wait_backends);
	UT_ASSERT_NOT_NULL(wait_backends_end);
	if (wait_backends == NULL || wait_backends_end == NULL)
		goto out;
	UT_ASSERT_NOT_NULL(
		find_in_order(wait_backends, wait_backends_end, "cluster_lmon_reconfig_suppressed()"));
out:
	free(postmaster);
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

	/* The release discriminator is legal only on exact phase-1 SHUTDOWN
	 * request/reply shapes; the wire sizes and version remain unchanged. */
	memset(&a, 0, sizeof(a));
	a.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	a.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	a.leaving_node_id = 1;
	a.preflight = CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE;
	a.producer_kind = CLUSTER_LEAVE_PRODUCER_SHUTDOWN;
	a.leave_epoch = 0;
	a.leave_nonce = 91;
	cluster_clean_leave_announce_compute_crc(&a);
	UT_ASSERT(cluster_clean_leave_announce_payload_valid(&a));
	a.producer_kind = CLUSTER_LEAVE_PRODUCER_OPERATOR;
	cluster_clean_leave_announce_compute_crc(&a);
	UT_ASSERT(!cluster_clean_leave_announce_payload_valid(&a));
	a.producer_kind = CLUSTER_LEAVE_PRODUCER_SHUTDOWN;
	a.preflight = CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT;
	cluster_clean_leave_announce_compute_crc(&a);
	UT_ASSERT(cluster_clean_leave_announce_payload_valid(&a));
	a._pad1[0] = 1;
	cluster_clean_leave_announce_compute_crc(&a);
	UT_ASSERT(!cluster_clean_leave_announce_payload_valid(&a));
	a._pad1[0] = 0;
	a.leaving_node_id = CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT;
	cluster_clean_leave_announce_compute_crc(&a);
	UT_ASSERT(!cluster_clean_leave_announce_payload_valid(&a));
	a.leaving_node_id = 1;
	a.preflight = CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT + 1;
	cluster_clean_leave_announce_compute_crc(&a);
	UT_ASSERT(!cluster_clean_leave_announce_payload_valid(&a));

	memset(&ack, 0, sizeof(ack));
	ack.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	ack.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	ack.survivor_node_id = 0;
	ack.leaving_node_id = 1;
	ack.leave_epoch = 0;
	ack.leave_nonce = 91;
	ack.phase1_round = CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE;
	cluster_clean_leave_ack_compute_crc(&ack);
	UT_ASSERT(cluster_clean_leave_ack_payload_valid(&ack));
	ack._pad1 = 1;
	cluster_clean_leave_ack_compute_crc(&ack);
	UT_ASSERT(!cluster_clean_leave_ack_payload_valid(&ack));
	ack._pad1 = 0;
	ack.survivor_node_id = CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT;
	cluster_clean_leave_ack_compute_crc(&ack);
	UT_ASSERT(!cluster_clean_leave_ack_payload_valid(&ack));
	ack.survivor_node_id = 0;
	ack.nak = 1;
	cluster_clean_leave_ack_compute_crc(&ack);
	UT_ASSERT(!cluster_clean_leave_ack_payload_valid(&ack));

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
	UT_PLAN(28);
	UT_RUN(test_struct_layout);
	UT_RUN(test_phase_fsm);
	UT_RUN(test_version_coherent);
	UT_RUN(test_own_commit_latched);
	UT_RUN(test_committed_evidence_matches);
	UT_RUN(test_writable_only_gate);
	UT_RUN(test_marker_validation);
	UT_RUN(test_should_invalidate);
	UT_RUN(test_serve_gate);
	UT_RUN(test_startup_serving_gate);
	UT_RUN(test_phase1_full_stop_probe_policy);
	UT_RUN(test_phase1_full_stop_probe_phase_policy);
	UT_RUN(test_phase1_full_stop_post_stopped_request_ahead_consumption_policy);
	UT_RUN(test_phase1_full_stop_post_stopped_request_retains_per_peer_transport_ownership);
	UT_RUN(test_phase1_full_stop_early_receiver_retains_semantic_ownership);
	UT_RUN(test_phase1_full_stop_post_stopped_receiver_requires_confirmed_local_round);
	UT_RUN(test_phase1_full_stop_release_probe_is_exact_and_phase1_only);
	UT_RUN(test_phase1_full_stop_exact_receipt_may_precede_local_reply_publication);
	UT_RUN(test_phase1_full_stop_release_completion_requires_four_way_delivery);
	UT_RUN(test_phase1_full_stop_exact_ack_barrier);
	UT_RUN(test_phase1_full_stop_barrier_waits_for_local_positive_ack_fanout);
	UT_RUN(test_phase1_full_stop_post_stopped_nonce_is_fresh);
	UT_RUN(test_phase1_full_stop_two_round_nonce_lineage_is_closed);
	UT_RUN(test_phase1_full_stop_prepare_disposition_is_closed);
	UT_RUN(test_phase1_full_stop_pgstat_follower_uses_existing_aux_hook);
	UT_RUN(test_phase1_full_stop_runtime_order_and_no_polling);
	UT_RUN(test_phase1_full_stop_retains_coordination_without_r4_admission);
	UT_RUN(test_ic_payload_validation);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
