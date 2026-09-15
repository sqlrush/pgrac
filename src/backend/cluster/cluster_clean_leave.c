/*-------------------------------------------------------------------------
 *
 * cluster_clean_leave.c
 *	  pgrac clean leave reconfiguration (spec-5.13) — runtime driver.
 *
 *	  Runtime half of the clean-leave module: the shmem ClusterLeaveState,
 *	  the leaving-node phase state machine (cluster_clean_leave_request +
 *	  _drive_drain), the ProcSignal quiesce path, the voting-disk leave-intent
 *	  marker two-phase commit (qvotec-mediated), the IC announce/ack/commit-ready
 *	  glue, and the LMON orchestration (cluster_clean_leave_lmon_tick: leaving-node
 *	  barrier + survivor drop/ack + coordinator two-phase commit).  The pure
 *	  decisions it feeds on live in cluster_clean_leave_policy.c.
 *
 *	  Execution model (Option A): the leaving node self-drives the drain in its
 *	  OWN backend (REQUESTED -> QUIESCE -> GES drain -> GCS flush -> PCM release ->
 *	  BARRIER_WAIT; the flush is in a backend, never LMON, CL-I9), then the LMON
 *	  collects survivor ACKs and asks the survivor coordinator (min node id) to
 *	  bump the epoch (LEAVE_COMMIT_READY).  Still pending: CL-I12 touched
 *	  drain-grace dispatch + the CL-I5 block-serve gate call-site (spec-5.13 S6),
 *	  the SRF/UDF/inject/catversion surface (S7), and the 2-node TAP e2e (S8).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_clean_leave.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-5.13-clean-leave-reconfig.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_lmd.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "miscadmin.h"
#include "storage/ipc.h" /* on_shmem_exit (qvotec latch publish) */
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/buf_internals.h"
#include "storage/proc.h"		/* PGPROC (quiesce broadcast) */
#include "storage/procsignal.h" /* SendProcSignal + PROCSIG_CLUSTER_CLEAN_LEAVE_QUIESCE */
#include "storage/shmem.h"
#include "storage/sinvaladt.h" /* BackendIdGetProc */
#include "utils/timestamp.h"
#include "utils/wait_event.h" /* RF-ROOT P6: WAIT_EVENT_RECONFIG_BARRIER_WAIT */

#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_cssd.h"
#include "cluster/cluster_epoch.h"	   /* cluster_epoch_get_current (version-coherent) */
#include "cluster/cluster_gcs_block.h" /* GCS flush-all-self orchestration (D5) */
#include "cluster/cluster_ges_reply_wait.h"
#include "cluster/cluster_grd.h" /* GES cooperative drain + no-leftover verify (D4) */
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_rdma.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_inject.h" /* CLUSTER_INJECTION_POINT (D12) */
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_pcm_lock.h" /* PCM release-all-self + no-leftover verify (D5) */
#include "cluster/cluster_qvotec.h"	  /* cluster_qvotec_in_quorum (request gate) */
#include "cluster/cluster_reconfig.h" /* apply_clean_leave + record/is_clean_departed + join_in_progress */
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_membership.h" /* v1.0.4 — cluster_membership_is_member (P1-2 INV-J8) */
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_undo_cleaner.h"
#include "cluster/cluster_voting_disk_io.h" /* leave-slot raw I/O + CLUSTER_VOTING_SLOT_BYTES */
#include "cluster/cluster_wal_state.h"
#include "cluster/cluster_wal_thread.h"

/*
 * The shmem state must stay small enough to embed cheaply (§2.1).  The §2.5
 * leave-marker submit mailbox (an embedded ClusterLeaveIntentMarker + handshake
 * atomics) pushes it past the original 256-byte budget; 512 is the new bound.
 */
StaticAssertDecl(sizeof(ClusterLeaveState) <= 512, "ClusterLeaveState exceeds 512-byte budget");
StaticAssertDecl(sizeof(ClusterNormalStopState) == 744, "normal-stop tail layout changed");
StaticAssertDecl(offsetof(ClusterNormalStopState, open_record) == 80,
				 "normal-stop OPEN offset changed");
StaticAssertDecl(offsetof(ClusterNormalStopState, root_descriptor) == 160,
				 "normal-stop root offset changed");
StaticAssertDecl(offsetof(ClusterNormalStopState, service_seal) == 728,
				 "normal-stop seal offset changed");
StaticAssertDecl(sizeof(ClusterCleanLeaveSharedState) == 1216, "clean-leave region layout changed");
StaticAssertDecl(sizeof(ClusterCleanLeaveSharedState) <= 1280, "clean-leave region exceeds budget");

/*
 * ProcSignal pending flag for the quiesce request (D7).  Set async-signal-safe
 * in the handler; the real work runs from ProcessInterrupts.
 */
volatile sig_atomic_t cluster_clean_leave_quiesce_pending = false;

/* shmem singleton (NULL until attached). */
static ClusterLeaveState *cl_state = NULL;
static ClusterNormalStopState *cl_normal_stop = NULL;

/* Always track outer work, including a request published inside that work. */
static uint32 cl_normal_stop_service_depth;
static uint32 cl_normal_stop_service_bit;
/* Checkpointer-local immutable roster; never recomputed from surviving PIDs. */
static uint32 cl_normal_stop_expected_services;

/* LMON owns transport-consumed early requests until the original durable
 * identity reader finishes. No shared layout, new message or authority. */
typedef struct ClNormalStopFrontInbox {
	bool pending;
	ClusterICEnvelope envelope;
	ClusterLeaveAnnouncePayload request;
	bool ack_pending;
	ClusterICEnvelope ack_envelope;
	ClusterLeaveAckPayload ack;
	/* Same LMON receiver owns already-consumed release control until the
	 * original identity observation is ready. No shared/wire authority. */
	bool release_pending[2];
	ClusterICEnvelope release_envelope[2];
	ClusterLeaveAnnouncePayload release[2];
	bool release_ack_pending;
	ClusterICEnvelope release_ack_envelope;
	ClusterLeaveAckPayload release_ack;
} ClNormalStopFrontInbox;
static ClNormalStopFrontInbox cl_normal_stop_front_inbox[CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT];

static void cl_normal_stop_fronts_lmon_tick(void);
static void cl_normal_stop_post_lmon_tick(void);
static bool cl_phase1_full_stop_send_admitted(ClusterICSendResult result);
static ClusterICSendResult cl_phase1_full_stop_send_release_announce(int32 dest_node,
																	 uint8 wire_round, uint64 nonce,
																	 uint64 epoch);
static ClusterICSendResult cl_phase1_full_stop_send_release_reply(int32 dest_node, uint64 nonce,
																  uint64 epoch);
static ClusterICSendResult
cl_phase1_full_stop_send_post_stopped_request(int32 dest_node, uint64 nonce, uint64 epoch);
static ClusterICSendResult cl_phase1_full_stop_send_post_stopped_reply(int32 dest_node,
																	   uint64 nonce, uint64 epoch);
static void cl_normal_stop_fronts_announce(const ClusterICEnvelope *env,
										   const ClusterLeaveAnnouncePayload *request);
static void cl_normal_stop_fronts_ack(const ClusterICEnvelope *env,
									  const ClusterLeaveAckPayload *ack);

/* LMON-only, boot-lifetime predecessor identity for the two same-wire Phase-1
 * barrier rounds.  It is neither authority nor shared ABI: the receive handler
 * records the exact ACTIVE-round nonce, then uses it only to reject a delayed
 * ACTIVE frame after the sender's durable WAL state has advanced to STOPPED. */
static uint64 cl_phase1_active_request_nonce[CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT];
static uint64 cl_phase1_post_stopped_request_round_nonce;
static uint8 cl_phase1_post_stopped_request_sent[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];

typedef struct ClPhase1PostStoppedRequestAhead {
	bool valid;
	bool retained_before_local_round;
	uint8 _pad0[6];
	uint64 source_active_nonce;
	uint64 source_stopped_nonce;
	uint64 local_attempt_nonce;
	uint64 local_deadline_us;
	uint64 epoch;
	int64 own_wal_started_at;
	uint64 member_incarnations[CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT];
} ClPhase1PostStoppedRequestAhead;

/* LMON-local receiver ownership for a transport-consumed STOPPED request that
 * is exactly one lifecycle stage ahead of this node.  These four bounded slots
 * are evidence retention only: they do not grant authority, extend a deadline,
 * or survive the LMON process. */
static ClPhase1PostStoppedRequestAhead
	cl_phase1_post_stopped_request_ahead[CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT];


static bool
cl_phase1_bytes_zero(const uint8 *bytes, Size nbytes)
{
	Size i;

	for (i = 0; i < nbytes; i++) {
		if (bytes[i] != 0)
			return false;
	}
	return true;
}

static bool
cl_phase1_member_bit_is_set(const uint8 *bitmap, int32 node)
{
	return node >= 0 && node < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT
		   && (bitmap[node / 8] & (uint8)(UINT8_C(1) << (node % 8))) != 0;
}

static void
cl_phase1_member_bit_set(uint8 *bitmap, int32 node)
{
	Assert(node >= 0 && node < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT);
	bitmap[node / 8] |= (uint8)(UINT8_C(1) << (node % 8));
}

static void
cl_phase1_full_stop_release_state_reset_locked(void)
{
	pg_atomic_write_u32(&cl_state->phase1_release_pending, 0);
	pg_atomic_write_u32(&cl_state->phase1_release_transport_drained, 0);
	memset(cl_state->phase1_post_stopped_reply_pending, 0,
		   sizeof(cl_state->phase1_post_stopped_reply_pending));
	memset(cl_state->phase1_post_stopped_reply_sent, 0,
		   sizeof(cl_state->phase1_post_stopped_reply_sent));
	memset(cl_state->phase1_release_request_sent, 0, sizeof(cl_state->phase1_release_request_sent));
	memset(cl_state->phase1_release_request_seen, 0, sizeof(cl_state->phase1_release_request_seen));
	memset(cl_state->phase1_release_reply_sent, 0, sizeof(cl_state->phase1_release_reply_sent));
	memset(cl_state->phase1_release_reply_seen, 0, sizeof(cl_state->phase1_release_reply_seen));
	memset(cl_state->phase1_release_receipt_sent, 0, sizeof(cl_state->phase1_release_receipt_sent));
	memset(cl_state->phase1_release_receipt_seen, 0, sizeof(cl_state->phase1_release_receipt_seen));
	memset(cl_state->phase1_release_request_nonce, 0,
		   sizeof(cl_state->phase1_release_request_nonce));
}


/* Capture only existing authority/evidence.  The caller adds the local nonce
 * and absolute deadline; neither is shared formation identity. */
bool
cluster_normal_stop_native_wal_mode(void)
{
	return cluster_enabled
		   && (cluster_wal_threads_dir == NULL || cluster_wal_threads_dir[0] == '\0')
		   && !cluster_controlfile_shared_authority && !cluster_merged_recovery;
}

static ClusterNormalStopPollResult
cl_full_stop_capture_formation_identity(uint32 expected_wal_state, bool require_shutdown_suppressed,
										uint64 expected_epoch, bool allow_native,
										ClusterPhase1FullStopPlan *out, const char **reason_out)
{
	ClusterFormationSnapshotV1 formation;
	ClusterWalStateSlot wal_slot;
	ReconfigEvent empty_event;
	uint16 own_thread;
	int node;
	const char *reason = "NORMAL_STOP_FORMATION_ARGUMENT";

	if (out == NULL)
		goto invalid;
	memset(out, 0, sizeof(*out));
	if (cl_state == NULL || !cluster_enabled || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT)
		goto invalid;
	if (reason_out != NULL)
		*reason_out = "NORMAL_STOP_FORMATION_OBSERVATION_PENDING";
	if ((require_shutdown_suppressed && !cluster_lmon_reconfig_suppressed())
		|| cluster_qvotec_get_quorum_state() != CLUSTER_QVOTEC_QUORUM_OK
		|| !cluster_qvotec_in_quorum())
		return CLUSTER_NORMAL_STOP_PENDING;
	own_thread = cluster_wal_thread_id();
	reason = "NORMAL_STOP_WAL_THREAD_IDENTITY";
	if (own_thread == 0)
		goto invalid;
	if (!cluster_reconfig_capture_formation_snapshot_v1(own_thread, &formation))
		return CLUSTER_NORMAL_STOP_PENDING;
	reason = "NORMAL_STOP_FORMATION_EPOCH";
	if (formation.local_epoch != expected_epoch)
		goto invalid;
	reason = "NORMAL_STOP_FORMATION_PREBUMP";
	if (formation.prebump_sync_active != 0)
		goto invalid;
	reason = "NORMAL_STOP_FORMATION_SELF_JOIN";
	if (formation.self_join_admitted == 0 || formation.self_join_failed != 0)
		goto invalid;
	memset(&empty_event, 0, sizeof(empty_event));
	reason = "NORMAL_STOP_FORMATION_APPLIED_EVENT";
	if (memcmp(&formation.applied, &empty_event, sizeof(empty_event)) != 0)
		goto invalid;
	reason = "NORMAL_STOP_FORMATION_PENDING_JOIN";
	if (!cl_phase1_bytes_zero(formation.pending_join_bitmap, sizeof(formation.pending_join_bitmap)))
		goto invalid;
	reason = "NORMAL_STOP_FORMATION_CLEAN_DEPARTED";
	if (!cl_phase1_bytes_zero(formation.clean_departed_bitmap,
							  sizeof(formation.clean_departed_bitmap)))
		goto invalid;
	reason = "NORMAL_STOP_FORMATION_REMOVED";
	if (!cl_phase1_bytes_zero(formation.removed_bitmap, sizeof(formation.removed_bitmap)))
		goto invalid;
	reason = "NORMAL_STOP_FORMATION_EXCLUDED";
	if (!cl_phase1_bytes_zero(formation.excluded_bitmap, sizeof(formation.excluded_bitmap)))
		goto invalid;
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		uint8 expected_state = node < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT ? CLUSTER_MEMBER_MEMBER
																			: CLUSTER_MEMBER_ABSENT;
		uint64 incarnation = formation.membership.last_admitted_incarnation[node];

		reason = "NORMAL_STOP_FORMATION_MEMBER_STATE";
		if (formation.membership.membership_state[node] != expected_state)
			goto invalid;
		reason = "NORMAL_STOP_FORMATION_MEMBER_INCARNATION";
		if (node < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT) {
			if (incarnation == 0 || incarnation == UINT64_MAX)
				goto invalid;
			out->member_incarnations[node] = incarnation;
		} else if (incarnation != 0)
			goto invalid;
	}
	reason = "NORMAL_STOP_FORMATION_OWN_INCARNATION";
	if (formation.victim_incarnation != out->member_incarnations[cluster_node_id]
		|| cluster_qvotec_get_self_incarnation() != out->member_incarnations[cluster_node_id])
		goto invalid;
	if (allow_native && cluster_normal_stop_native_wal_mode()) {
		reason = "NORMAL_STOP_NATIVE_CONTROL_INVALID";
		if ((expected_wal_state != CLUSTER_WAL_SLOT_STATE_ACTIVE
			 && expected_wal_state != CLUSTER_WAL_SLOT_STATE_STOPPED)
			|| !cluster_native_wal_shutdown_observe(
				expected_wal_state == CLUSTER_WAL_SLOT_STATE_STOPPED, &out->own_wal_started_at))
			goto invalid;
	} else {
		reason = "NORMAL_STOP_SHARED_WAL_IDENTITY";
		if (cluster_wal_state_read_slot(own_thread, &wal_slot) != CLUSTER_WAL_SLOT_OK
			|| wal_slot.thread_id != own_thread || wal_slot.node_id != cluster_node_id
			|| wal_slot.state != expected_wal_state || wal_slot.started_at <= 0)
			goto invalid;
		out->own_wal_started_at = wal_slot.started_at;
	}
	out->epoch = formation.local_epoch;
	if (reason_out != NULL)
		*reason_out = "NORMAL_STOP_FORMATION_READY";
	return CLUSTER_NORMAL_STOP_READY;

invalid:
	if (reason_out != NULL)
		*reason_out = reason;
	return CLUSTER_NORMAL_STOP_INVALID;
}

/* Keep the old pristine contract separate; current OPEN never enters it. */
static bool
cl_phase1_full_stop_capture_identity(uint32 expected_wal_state, bool require_shutdown_suppressed,
									 ClusterPhase1FullStopPlan *out)
{
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	return cluster_semantic_activation_phase1_pristine()
		   && cl_full_stop_capture_formation_identity(
				  expected_wal_state, require_shutdown_suppressed, 0, false, out, NULL)
				  == CLUSTER_NORMAL_STOP_READY;
}

static bool
cl_phase1_full_stop_identity_matches(const ClusterPhase1FullStopPlan *plan,
									 uint32 expected_wal_state)
{
	ClusterPhase1FullStopPlan current;

	if (!cluster_clean_leave_phase1_full_stop_plan_valid(plan)
		|| !cl_phase1_full_stop_capture_identity(expected_wal_state, true, &current))
		return false;
	return current.epoch == plan->epoch && current.own_wal_started_at == plan->own_wal_started_at
		   && memcmp(current.member_incarnations, plan->member_incarnations,
					 sizeof(plan->member_incarnations))
				  == 0;
}


static bool
cl_phase1_full_stop_capture_barrier_identity(ClusterPhase1FullStopPlan *out, uint32 *wal_state_out)
{
	if (cl_phase1_full_stop_capture_identity(CLUSTER_WAL_SLOT_STATE_ACTIVE, true, out)) {
		if (wal_state_out != NULL)
			*wal_state_out = CLUSTER_WAL_SLOT_STATE_ACTIVE;
		return true;
	}
	if (cl_phase1_full_stop_capture_identity(CLUSTER_WAL_SLOT_STATE_STOPPED, true, out)) {
		if (wal_state_out != NULL)
			*wal_state_out = CLUSTER_WAL_SLOT_STATE_STOPPED;
		return true;
	}
	return false;
}


/*
 * Pre-shutdown retention predicate.  This reads the same exact phase-1
 * formation/WAL identity as prepare_exact(), but intentionally does not
 * require LMON suppression yet: the postmaster uses this result to retain the
 * existing coordination stack and then establishes that suppression fence.
 */
bool
cluster_clean_leave_phase1_full_stop_candidate(void)
{
	ClusterPhase1FullStopPlan candidate;

	return cl_phase1_full_stop_capture_identity(CLUSTER_WAL_SLOT_STATE_ACTIVE, false, &candidate);
}


/* Classify the authenticated sender from existing shared evidence.  The
 * source's deterministic WAL thread identifies its current WAL slot.  In the
 * pristine pre-R4 phase, exact current-formation STOPPED is the user-approved
 * clean terminal; no control-root lifecycle exists or is inferred. */
static bool
cl_phase1_full_stop_capture_source_phase(const ClusterPhase1FullStopPlan *current,
										 int32 source_node, bool *source_active_out,
										 bool *source_stopped_out)
{
	ClusterWalStateSlot source_slot;
	uint16 source_thread;

	if (current == NULL || source_active_out == NULL || source_stopped_out == NULL
		|| source_node < 0 || source_node >= CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT)
		return false;
	*source_active_out = false;
	*source_stopped_out = false;
	source_thread = cluster_wal_thread_id_for(true, source_node);
	if (source_thread == 0
		|| cluster_wal_state_read_slot(source_thread, &source_slot) != CLUSTER_WAL_SLOT_OK
		|| source_slot.thread_id != source_thread || source_slot.node_id != source_node
		|| source_slot.started_at <= 0)
		return false;
	if (source_slot.state == CLUSTER_WAL_SLOT_STATE_ACTIVE) {
		*source_active_out = true;
		return true;
	}
	if (source_slot.state == CLUSTER_WAL_SLOT_STATE_STOPPED) {
		*source_stopped_out = true;
		return true;
	}
	return false;
}


static bool
cl_phase1_full_stop_request_ahead_identity_matches(const ClPhase1PostStoppedRequestAhead *ahead,
												   const ClusterPhase1FullStopPlan *current)
{
	return ahead != NULL && ahead->valid && current != NULL && ahead->epoch == current->epoch
		   && ahead->own_wal_started_at == current->own_wal_started_at
		   && memcmp(ahead->member_incarnations, current->member_incarnations,
					 sizeof(ahead->member_incarnations))
				  == 0;
}


static ClusterPhase1FullStopProbeNonceDecision
cl_phase1_full_stop_retain_request_ahead_locked(const ClusterPhase1FullStopPlan *current,
												uint32 local_wal_state, int32 source_node,
												uint64 incoming_nonce)
{
	ClPhase1PostStoppedRequestAhead *ahead;
	ClusterPhase1FullStopProbeNonceDecision decision;
	uint64 active_nonce;
	uint64 local_nonce;
	uint64 deadline_us;
	bool same_local_round = false;

	Assert(LWLockHeldByMeInMode(&cl_state->lock, LW_EXCLUSIVE));
	Assert(source_node >= 0 && source_node < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT);
	ahead = &cl_phase1_post_stopped_request_ahead[source_node];
	active_nonce = cl_phase1_active_request_nonce[source_node];
	decision = cluster_clean_leave_phase1_full_stop_probe_nonce_decide(
		active_nonce, ahead->valid ? ahead->source_stopped_nonce : 0, incoming_nonce);
	if (decision == CLUSTER_PHASE1_PROBE_NONCE_STALE_ACTIVE
		|| decision == CLUSTER_PHASE1_PROBE_NONCE_CONFLICT)
		return decision;

	local_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	deadline_us = cl_state->barrier_deadline_us;
	if (ahead->valid) {
		if (ahead->retained_before_local_round) {
			if (local_wal_state == CLUSTER_WAL_SLOT_STATE_ACTIVE)
				same_local_round = local_nonce == ahead->local_attempt_nonce;
			else if (local_wal_state == CLUSTER_WAL_SLOT_STATE_STOPPED)
				same_local_round = cluster_clean_leave_phase1_full_stop_nonce_fresh(
					ahead->local_attempt_nonce, local_nonce);
		} else if (local_wal_state == CLUSTER_WAL_SLOT_STATE_STOPPED)
			same_local_round = local_nonce == ahead->local_attempt_nonce;
		if (!same_local_round || ahead->source_active_nonce != active_nonce
			|| ahead->local_deadline_us != deadline_us
			|| !cl_phase1_full_stop_request_ahead_identity_matches(ahead, current))
			return CLUSTER_PHASE1_PROBE_NONCE_CONFLICT;
		return decision;
	}

	if (decision != CLUSTER_PHASE1_PROBE_NONCE_ACCEPT_STOPPED || current == NULL
		|| (local_wal_state != CLUSTER_WAL_SLOT_STATE_ACTIVE
			&& local_wal_state != CLUSTER_WAL_SLOT_STATE_STOPPED)
		|| pg_atomic_read_u32(&cl_state->request_in_progress) == 0
		|| pg_atomic_read_u32(&cl_state->shutdown_driven) == 0 || active_nonce == 0
		|| active_nonce == UINT64_MAX || local_nonce == 0 || local_nonce == UINT64_MAX
		|| deadline_us == 0 || deadline_us == UINT64_MAX)
		return CLUSTER_PHASE1_PROBE_NONCE_CONFLICT;

	memset(ahead, 0, sizeof(*ahead));
	ahead->valid = true;
	ahead->retained_before_local_round
		= cluster_clean_leave_phase1_full_stop_request_ahead_uses_predecessor_nonce(
			local_wal_state == CLUSTER_WAL_SLOT_STATE_ACTIVE,
			local_wal_state == CLUSTER_WAL_SLOT_STATE_STOPPED,
			pg_atomic_read_u32(&cl_state->preflight_pending) != 0);
	ahead->source_active_nonce = active_nonce;
	ahead->source_stopped_nonce = incoming_nonce;
	ahead->local_attempt_nonce = local_nonce;
	ahead->local_deadline_us = deadline_us;
	ahead->epoch = current->epoch;
	ahead->own_wal_started_at = current->own_wal_started_at;
	memcpy(ahead->member_incarnations, current->member_incarnations,
		   sizeof(ahead->member_incarnations));
	return decision;
}


static bool
cl_phase1_full_stop_consume_request_ahead_locked(const ClusterPhase1FullStopPlan *current,
												 int32 source_node, uint64 local_nonce,
												 uint64 deadline_us, bool post_requests_sent)
{
	ClPhase1PostStoppedRequestAhead *ahead;
	ClusterPhase1FullStopProbeNonceDecision decision;
	uint64 stored_nonce;
	bool exact_identity;

	Assert(LWLockHeldByMeInMode(&cl_state->lock, LW_EXCLUSIVE));
	Assert(source_node >= 0 && source_node < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT);
	ahead = &cl_phase1_post_stopped_request_ahead[source_node];
	if (!ahead->valid)
		return true;
	exact_identity = cl_phase1_full_stop_request_ahead_identity_matches(ahead, current)
					 && ahead->source_active_nonce == cl_phase1_active_request_nonce[source_node];
	if (!cluster_clean_leave_phase1_full_stop_request_ahead_can_consume(
			ahead->valid, ahead->retained_before_local_round, ahead->local_attempt_nonce,
			ahead->local_deadline_us, local_nonce, deadline_us, exact_identity,
			pg_atomic_read_u32(&cl_state->request_in_progress) != 0,
			pg_atomic_read_u32(&cl_state->shutdown_driven) != 0, post_requests_sent))
		return false;

	stored_nonce = cl_state->phase1_release_request_nonce[source_node];
	decision = cluster_clean_leave_phase1_full_stop_probe_nonce_decide(
		ahead->source_active_nonce, stored_nonce, ahead->source_stopped_nonce);
	if (decision == CLUSTER_PHASE1_PROBE_NONCE_ACCEPT_STOPPED)
		cl_state->phase1_release_request_nonce[source_node] = ahead->source_stopped_nonce;
	else if (decision != CLUSTER_PHASE1_PROBE_NONCE_DUPLICATE_STOPPED)
		return false;
	cl_phase1_member_bit_set(cl_state->phase1_post_stopped_reply_pending, source_node);
	memset(ahead, 0, sizeof(*ahead));
	return true;
}


static void
cl_phase1_full_stop_release(const ClusterPhase1FullStopPlan *plan)
{
	if (cl_state == NULL || plan == NULL)
		return;
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	if (pg_atomic_read_u64(&cl_state->leave_attempt_nonce) == plan->attempt_nonce) {
		pg_atomic_write_u32(&cl_state->preflight_pending, 0);
		pg_atomic_write_u32(&cl_state->preflight_sent, 0);
		pg_atomic_write_u32(&cl_state->shutdown_driven, 0);
		pg_atomic_write_u32(&cl_state->nak_received, 0);
		pg_atomic_write_u32(&cl_state->nak_reason, (uint32)CLUSTER_LEAVE_NAK_NONE);
		pg_atomic_write_u64(&cl_state->leave_attempt_nonce, 0);
		cl_state->barrier_deadline_us = 0;
		memset(cl_state->ack_bitmap, 0, sizeof(cl_state->ack_bitmap));
		cl_phase1_full_stop_release_state_reset_locked();
		pg_atomic_write_u32(&cl_state->request_in_progress, 0);
	}
	LWLockRelease(&cl_state->lock);
}


/* ============================================================
 * shmem region (D2)
 * ============================================================ */

Size
cluster_clean_leave_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterCleanLeaveSharedState));
}

void
cluster_clean_leave_shmem_init(void)
{
	bool found;
	ClusterCleanLeaveSharedState *state;

	state = ShmemInitStruct("pgrac cluster clean_leave", cluster_clean_leave_shmem_size(), &found);
	cl_state = &state->leave;
	cl_normal_stop = &state->normal_stop;
	if (!found) {
		memset(state, 0, sizeof(*state));
		LWLockInitialize(&cl_state->lock, LWTRANCHE_CLUSTER_CLEAN_LEAVE);
		pg_atomic_init_u32(&cl_state->phase, CLUSTER_LEAVE_IDLE);
		cl_state->leaving_node_id = -1;
		cl_state->leave_epoch = 0;
		cl_state->barrier_deadline_us = 0;
		pg_atomic_init_u64(&cl_state->ges_drained_count, 0);
		pg_atomic_init_u64(&cl_state->gcs_flushed_count, 0);
		pg_atomic_init_u64(&cl_state->shards_remastered, 0);
		pg_atomic_init_u64(&cl_state->escalate_count, 0);
		pg_atomic_init_u32(&cl_state->nak_received, 0);
		pg_atomic_init_u32(&cl_state->survivor_acked, 0);
		pg_atomic_init_u32(&cl_state->commit_ready_received, 0);
		pg_atomic_init_u32(&cl_state->announce_sent, 0);
		pg_atomic_init_u64(&cl_state->serve_gate_fail_closed_count, 0);
		/* Hardening v1.0.1 (P2 / P1-1). */
		pg_atomic_init_u32(&cl_state->nak_reason, (uint32)CLUSTER_LEAVE_NAK_NONE);
		pg_atomic_init_u32(&cl_state->commit_point_observed, 0);
		pg_atomic_init_u32(&cl_state->committed_durable_confirmed, 0);
		pg_atomic_init_u32(&cl_state->committed_marker_durable, 0);
		/* spec-2.29a r3 (evidence latch). */
		cl_state->committed_confirmed_epoch = 0;
		/* Hardening v1.0.2 (P1 preflight / P2 nonce). */
		pg_atomic_init_u64(&cl_state->leave_attempt_nonce, 0);
		pg_atomic_init_u32(&cl_state->preflight_pending, 0);
		pg_atomic_init_u32(&cl_state->preflight_sent, 0);
		/* RF-ROOT P6 (shutdown-handoff wiring). */
		pg_atomic_init_u32(&cl_state->shutdown_driven, 0);
		/* Hardening v1.0.3 (P1 same-node serialization + preflight-incomplete reason). */
		pg_atomic_init_u32(&cl_state->request_in_progress, 0);
		pg_atomic_init_u32(&cl_state->phase1_release_pending, 0);
		pg_atomic_init_u32(&cl_state->phase1_release_transport_drained, 0);
		pg_atomic_init_u32(&cl_state->abort_reason, (uint32)CLUSTER_LEAVE_ABORT_NONE);
		/* §2.5 leave-marker submit mailbox. */
		cl_state->qvotec_latch = NULL;
		pg_atomic_init_u64(&cl_state->marker_request_seq, 0);
		pg_atomic_init_u64(&cl_state->marker_completion_seq, 0);
		pg_atomic_init_u32(&cl_state->marker_result, CLUSTER_LEAVE_MARKER_SUBMIT_ACK);
		memset(&cl_state->pending_marker, 0, sizeof(cl_state->pending_marker));
		pg_atomic_init_u32(&cl_normal_stop->requested, 0);
		pg_atomic_init_u32(&cl_normal_stop->frontends_gone, 0);
		pg_atomic_init_u32(&cl_normal_stop->phase, CLUSTER_NORMAL_STOP_IDLE);
		pg_atomic_init_u32(&cl_normal_stop->failure_reason, 0);
		pg_atomic_init_u32(&cl_normal_stop->cleaner_quiesce_requested, 0);
		pg_atomic_init_u32(&cl_normal_stop->cleaner_quiesced_mask, 0);
		pg_atomic_init_u32(&cl_normal_stop->qvotec_clear_result, 0);
		pg_atomic_init_u32(&cl_normal_stop->identity_published, 0);
		pg_atomic_init_u32(&cl_normal_stop->service_active_mask, 0);
		pg_atomic_init_u32(&cl_normal_stop->service_idle_mask, 0);
		pg_atomic_init_u32(&cl_normal_stop->service_seal, 0);
	}
}

static const ClusterShmemRegion cluster_clean_leave_region = {
	.name = "pgrac cluster clean_leave",
	.size_fn = cluster_clean_leave_shmem_size,
	.init_fn = cluster_clean_leave_shmem_init,
	.lwlock_count = 1, /* ClusterLeaveState.lock (LWTRANCHE_CLUSTER_CLEAN_LEAVE) */
	.owner_subsys = "cluster_clean_leave",
	.reserved_flags = 0,
};

bool
cluster_normal_stop_postmaster_request(void)
{
	if (IsUnderPostmaster || !IsPostmasterEnvironment || cl_normal_stop == NULL
		|| cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		return false;
	pg_atomic_write_u32(&cl_normal_stop->requested, 1);
	return true;
}

bool
cluster_normal_stop_postmaster_frontends_gone(void)
{
	if (IsUnderPostmaster || !IsPostmasterEnvironment || !cluster_normal_stop_requested()
		|| cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		return false;
	pg_atomic_write_u32(&cl_normal_stop->frontends_gone, 1);
	return true;
}

bool
cluster_normal_stop_requested(void)
{
	return cl_normal_stop != NULL && pg_atomic_read_u32(&cl_normal_stop->requested) == 1;
}

void
cluster_normal_stop_fail(ClusterNormalStopFailure reason)
{
	uint32 expected = CLUSTER_NORMAL_STOP_FAILURE_NONE;

	if (cl_normal_stop == NULL || reason == CLUSTER_NORMAL_STOP_FAILURE_NONE)
		return;
	if (reason < CLUSTER_NORMAL_STOP_FAILURE_NONE
		|| reason > CLUSTER_NORMAL_STOP_FAILURE_LATE_UNSEALED_WORK)
		reason = CLUSTER_NORMAL_STOP_FAILURE_STATE;
	(void)pg_atomic_compare_exchange_u32(&cl_normal_stop->failure_reason, &expected, reason);
}

ClusterNormalStopFailure
cluster_normal_stop_failure(void)
{
	return cl_normal_stop == NULL
			   ? CLUSTER_NORMAL_STOP_FAILURE_STATE
			   : (ClusterNormalStopFailure)pg_atomic_read_u32(&cl_normal_stop->failure_reason);
}

static ClusterNormalStopPollResult
cl_normal_stop_observe_identity(const ClusterSemanticActivationRecord *open,
								const uint8 root[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES],
								uint32 wal_state, ClusterPhase1FullStopPlan *out,
								const char **reason_out)
{
	ClusterNormalStopPollResult result;
	ClusterPhase1FullStopPlan before, after;
	uint64 incarnations[CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT];
	unsigned pass;

	/* This is the current pre-bit22 OPEN branch, not SOURCE cutover. All
	 * original formation/WAL fields keep their original validation. */
	if (cluster_wal_thread_id() != (uint16)(cluster_node_id + 1)) {
		if (reason_out != NULL)
			*reason_out = "NORMAL_STOP_WAL_THREAD_IDENTITY";
		return CLUSTER_NORMAL_STOP_INVALID;
	}
	for (pass = 0; pass < 2; pass++) {
		ClusterPhase1FullStopPlan *sample = pass == 0 ? &before : &after;
		result = cluster_semantic_normal_stop_match(open, root, incarnations, reason_out);
		if (result != CLUSTER_NORMAL_STOP_READY)
			return result;
		result = cl_full_stop_capture_formation_identity(wal_state, cluster_normal_stop_requested(),
														 open->transition_epoch, true, sample,
														 reason_out);
		if (result != CLUSTER_NORMAL_STOP_READY)
			return result;
		if (memcmp(sample->member_incarnations, incarnations, sizeof(incarnations)) != 0) {
			if (reason_out != NULL)
				*reason_out = "NORMAL_STOP_MEMBER_IDENTITY_CHANGED";
			return CLUSTER_NORMAL_STOP_INVALID;
		}
	}
	/* WAL reads may block: the semantic side of the sample must also be
	 * checked after the LAST original WAL read, not just before it. */
	result = cluster_semantic_normal_stop_match(open, root, incarnations, reason_out);
	if (result != CLUSTER_NORMAL_STOP_READY)
		return result;
	if (before.epoch != after.epoch || before.own_wal_started_at != after.own_wal_started_at
		|| memcmp(before.member_incarnations, after.member_incarnations,
				  sizeof(before.member_incarnations))
			   != 0
		|| memcmp(after.member_incarnations, incarnations, sizeof(incarnations)) != 0) {
		if (reason_out != NULL)
			*reason_out = "NORMAL_STOP_IDENTITY_CHANGED_DURING_SAMPLE";
		return CLUSTER_NORMAL_STOP_INVALID;
	}
	*out = after;
	return CLUSTER_NORMAL_STOP_READY;
}

/* Caller holds only the original leave lock, never a module lock. */
static bool
cl_normal_stop_bound_identity_matches(const ClusterSemanticActivationRecord *open,
									  const uint8 root[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES],
									  const ClusterPhase1FullStopPlan *observed)
{
	return pg_atomic_read_u32(&cl_normal_stop->identity_published) == 1
		   && cl_normal_stop->epoch == observed->epoch
		   && cl_normal_stop->own_wal_started_at == observed->own_wal_started_at
		   && memcmp(cl_normal_stop->member_incarnations, observed->member_incarnations,
					 sizeof(observed->member_incarnations))
				  == 0
		   && memcmp(&cl_normal_stop->open_record, open, sizeof(*open)) == 0
		   && memcmp(cl_normal_stop->root_descriptor, root, CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES)
				  == 0;
}

/* Native own-WAL has no shared slot producer. The existing authenticated
 * message can be a checkpoint successor only after our real drain ACK:
 * that source cannot checkpoint without receiving that ACK. No nonce here
 * writes control state, grants a drain vote, or advances our checkpoint. */
static bool
cl_normal_stop_capture_source_phase(const ClusterPhase1FullStopPlan *current, int peer,
									uint64 nonce, bool *active, bool *stopped)
{
	bool valid = false;
	uint64 predecessor;
	ClusterPhase1FullStopProbeNonceDecision decision;

	if (!cluster_normal_stop_native_wal_mode())
		return cl_phase1_full_stop_capture_source_phase(current, peer, active, stopped);
	*active = *stopped = false;
	if (current == NULL || peer < 0 || peer >= CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT
		|| peer == cluster_node_id || nonce == 0 || nonce == UINT64_MAX)
		return false;
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	predecessor = cl_normal_stop->peer_request_nonce[peer];
	if (pg_atomic_read_u32(&cl_normal_stop->identity_published) == 1
		&& current->epoch == cl_normal_stop->epoch
		&& cluster_normal_stop_failure() == CLUSTER_NORMAL_STOP_FAILURE_NONE) {
		if ((predecessor == 0
			 && pg_atomic_read_u32(&cl_normal_stop->phase) < CLUSTER_NORMAL_STOP_CHECKPOINT)
			|| predecessor == nonce) {
			*active = true;
			valid = true;
		} else if (predecessor != 0 && cluster_normal_stop_requested()
				   && pg_atomic_read_u32(&cl_normal_stop->frontends_gone) == 1
				   && pg_atomic_read_u32(&cl_normal_stop->phase) >= CLUSTER_NORMAL_STOP_DRAIN
				   && cl_normal_stop->peer_requests_seen == 15
				   && (cl_normal_stop->peer_reply_sent & (UINT32_C(1) << peer)) != 0) {
			decision = cluster_clean_leave_phase1_full_stop_probe_nonce_decide(
				predecessor, cl_state->phase1_release_request_nonce[peer], nonce);
			valid = decision == CLUSTER_PHASE1_PROBE_NONCE_ACCEPT_STOPPED
					|| decision == CLUSTER_PHASE1_PROBE_NONCE_DUPLICATE_STOPPED;
			*stopped = valid;
		}
	}
	LWLockRelease(&cl_state->lock);
	return valid;
}

bool
cluster_normal_stop_peer_receipt_tail(
	const ClusterSemanticActivationRecord *open_record,
	const uint8 root_descriptor[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES], int peer,
	uint64 admitted_incarnation)
{
	uint32 phase, seal, peers, bit;
	bool valid;
	if (!IsUnderPostmaster || (!AmLmonProcess() && !AmCheckpointerProcess()) || open_record == NULL
		|| root_descriptor == NULL || cl_state == NULL || cl_normal_stop == NULL || !cluster_enabled
		|| !cluster_normal_stop_requested() || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT || peer < 0
		|| peer >= CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT || peer == cluster_node_id
		|| admitted_incarnation == 0 || admitted_incarnation == UINT64_MAX)
		return false;
	peers = UINT32_C(15) & ~(UINT32_C(1) << cluster_node_id);
	bit = UINT32_C(1) << peer;
	LWLockAcquire(&cl_state->lock, LW_SHARED);
	phase = pg_atomic_read_u32(&cl_normal_stop->phase);
	seal = pg_atomic_read_u32(&cl_normal_stop->service_seal);
	valid = cluster_normal_stop_failure() == CLUSTER_NORMAL_STOP_FAILURE_NONE
			&& pg_atomic_read_u32(&cl_normal_stop->identity_published) == 1
			&& ((phase == CLUSTER_NORMAL_STOP_POST_STOPPED && seal == 1)
				|| (phase == CLUSTER_NORMAL_STOP_PROTOCOL_CLOSED && seal == 2))
			&& pg_atomic_read_u32(&cl_normal_stop->frontends_gone) == 1
			&& pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesced_mask) == UINT32_C(255)
			&& pg_atomic_read_u32(&cl_state->phase1_release_pending) == 1
			&& cl_state->leaving_node_id == -1
			&& pg_atomic_read_u32(&cl_state->phase) == CLUSTER_LEAVE_IDLE
			&& (pg_atomic_read_u32(&cl_state->request_in_progress) == 0
				|| pg_atomic_read_u32(&cl_state->shutdown_driven) == 1)
			&& cl_normal_stop->epoch == open_record->transition_epoch
			&& cl_normal_stop->member_incarnations[peer] == admitted_incarnation
			&& memcmp(&cl_normal_stop->open_record, open_record, sizeof(*open_record)) == 0
			&& memcmp(cl_normal_stop->root_descriptor, root_descriptor,
					  CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES)
				   == 0
			&& cl_normal_stop->peer_requests_seen == 15
			&& (cl_normal_stop->peer_reply_sent & bit) != 0 && (cl_state->ack_bitmap[0] & bit) != 0
			&& (cl_state->phase1_post_stopped_reply_sent[0] & bit) != 0
			&& (cl_state->phase1_post_stopped_reply_pending[0] & bit) == 0
			&& cluster_clean_leave_phase1_full_stop_nonce_fresh(
				cl_normal_stop->peer_request_nonce[peer],
				cl_state->phase1_release_request_nonce[peer]);
	if (valid) {
		const uint8 *maps[] = { cl_state->phase1_release_request_sent,
								cl_state->phase1_release_request_seen,
								cl_state->phase1_release_reply_sent,
								cl_state->phase1_release_reply_seen,
								cl_state->phase1_release_receipt_sent,
								cl_state->phase1_release_receipt_seen,
								cl_state->ack_bitmap,
								cl_state->phase1_post_stopped_reply_sent,
								cl_state->phase1_post_stopped_reply_pending };
		for (unsigned i = 0; i < lengthof(maps); i++)
			if ((maps[i][0] & ~peers) != 0
				|| !cl_phase1_bytes_zero(maps[i] + 1, CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES - 1)
				|| (i < 5 && (maps[i][0] & bit) == 0))
				valid = false;
	}
	LWLockRelease(&cl_state->lock);
	/* The missing sixth leg is not signed here. This only lets its real
	 * consumer continue after a terminal peer's connection disappears. */
	return valid;
}

static ClusterNormalStopPollResult
cl_normal_stop_identity_poll(bool post_checkpoint, ClusterPhase1FullStopPlan *out,
							 const char **reason_out)
{
	ClusterNormalStopPollResult result = CLUSTER_NORMAL_STOP_INVALID;
	ClusterSemanticActivationRecord open;
	ClusterPhase1FullStopPlan observed, verified;
	uint8 root[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint32 published;
	uint32 wal_state
		= post_checkpoint ? CLUSTER_WAL_SLOT_STATE_STOPPED : CLUSTER_WAL_SLOT_STATE_ACTIVE;
	const char *reason = "NORMAL_STOP_IDENTITY_INVALID";

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = reason;
	if (!IsUnderPostmaster || (!AmLmonProcess() && !AmCheckpointerProcess()) || out == NULL
		|| cl_state == NULL || cl_normal_stop == NULL || !cluster_enabled || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT)
		return CLUSTER_NORMAL_STOP_INVALID;
	if (cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		return CLUSTER_NORMAL_STOP_INVALID;

	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	published = pg_atomic_read_u32(&cl_normal_stop->identity_published);
	if (published == 1) {
		open = cl_normal_stop->open_record;
		memcpy(root, cl_normal_stop->root_descriptor, sizeof(root));
	}
	LWLockRelease(&cl_state->lock);
	if (published > 1 || (published == 0 && post_checkpoint))
		goto done;
	if (published == 0) {
		/* Only LMON can consume the original authority mailbox. A peer's
		 * authenticated early request may cause this read before local stop;
		 * neither the read nor publication sets requested or any ACK bit. */
		if (!AmLmonProcess()) {
			result = CLUSTER_NORMAL_STOP_PENDING;
			reason = "NORMAL_STOP_IDENTITY_AWAIT_LMON";
			goto done;
		}
		result = cluster_semantic_normal_stop_read_identity(&open, root, NULL, &reason);
		if (result != CLUSTER_NORMAL_STOP_READY)
			goto done;
	}
	result = cl_normal_stop_observe_identity(&open, root, wal_state, &observed, &reason);
	if (result != CLUSTER_NORMAL_STOP_READY)
		goto done;

	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	result = CLUSTER_NORMAL_STOP_INVALID;
	reason = "NORMAL_STOP_BOUND_IDENTITY_CONTRADICTION";
	if (cluster_normal_stop_failure() == CLUSTER_NORMAL_STOP_FAILURE_NONE
		&& cl_state->leaving_node_id == -1
		&& pg_atomic_read_u32(&cl_state->phase) == CLUSTER_LEAVE_IDLE
		&& (pg_atomic_read_u32(&cl_state->request_in_progress) == 0
			|| pg_atomic_read_u32(&cl_state->shutdown_driven) == 1)) {
		if (pg_atomic_read_u32(&cl_normal_stop->identity_published) == 0 && AmLmonProcess()) {
			cl_normal_stop->epoch = observed.epoch;
			cl_normal_stop->own_wal_started_at = observed.own_wal_started_at;
			memcpy(cl_normal_stop->member_incarnations, observed.member_incarnations,
				   sizeof(observed.member_incarnations));
			cl_normal_stop->open_record = open;
			memcpy(cl_normal_stop->root_descriptor, root, sizeof(root));
			pg_write_barrier();
			pg_atomic_write_u32(&cl_normal_stop->identity_published, 1);
		}
		if (cl_normal_stop_bound_identity_matches(&open, root, &observed))
			result = CLUSTER_NORMAL_STOP_READY;
	}
	LWLockRelease(&cl_state->lock);
	if (result != CLUSTER_NORMAL_STOP_READY)
		goto done;
	/* Publication is not permission: revalidate after dropping the leave
	 * lock, then compare the still-immutable binding before returning it. */
	result = cl_normal_stop_observe_identity(&open, root, wal_state, &verified, &reason);
	if (result != CLUSTER_NORMAL_STOP_READY)
		goto done;
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	if (cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE
		|| cl_state->leaving_node_id != -1
		|| pg_atomic_read_u32(&cl_state->phase) != CLUSTER_LEAVE_IDLE
		|| (pg_atomic_read_u32(&cl_state->request_in_progress) != 0
			&& pg_atomic_read_u32(&cl_state->shutdown_driven) != 1)
		|| !cl_normal_stop_bound_identity_matches(&open, root, &verified)) {
		result = CLUSTER_NORMAL_STOP_INVALID;
		reason = "NORMAL_STOP_BOUND_IDENTITY_CONTRADICTION";
	}
	LWLockRelease(&cl_state->lock);
	if (result == CLUSTER_NORMAL_STOP_READY)
		*out = verified;
done:
	if (result == CLUSTER_NORMAL_STOP_INVALID) {
		if (cluster_normal_stop_failure() == CLUSTER_NORMAL_STOP_FAILURE_NONE)
			ereport(LOG, (errmsg("cluster normal-stop: first identity rejection"),
						  errdetail("node=%d post_checkpoint=%d native_wal=%d reason=%s",
									cluster_node_id, post_checkpoint,
									cluster_normal_stop_native_wal_mode(), reason)));
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
	}
	if (reason_out != NULL)
		*reason_out = result == CLUSTER_NORMAL_STOP_READY ? "NORMAL_STOP_IDENTITY_READY" : reason;
	return result;
}

bool
cluster_normal_stop_protocol_closed(void)
{
	return cluster_normal_stop_requested()
		   && pg_atomic_read_u32(&cl_normal_stop->phase) == CLUSTER_NORMAL_STOP_PROTOCOL_CLOSED
		   && cluster_normal_stop_failure() == CLUSTER_NORMAL_STOP_FAILURE_NONE;
}

ClusterNormalStopPollResult
cluster_normal_stop_fronts_poll(ClusterPhase1FullStopPlan *plan_out, const char **reason_out)
{
	ClusterPhase1FullStopPlan observed;
	ClusterNormalStopPollResult result = CLUSTER_NORMAL_STOP_INVALID;
	const char *reason = "NORMAL_STOP_FRONTS_STATE_INVALID";
	uint64 now, nonce, deadline;
	uint32 phase, own_bit, peer_bits;
	int peer;

	if (plan_out != NULL)
		memset(plan_out, 0, sizeof(*plan_out));
	if (reason_out != NULL)
		*reason_out = reason;
	if (!IsUnderPostmaster || !AmCheckpointerProcess() || plan_out == NULL || cl_state == NULL
		|| cl_normal_stop == NULL || !cluster_enabled || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT)
		return CLUSTER_NORMAL_STOP_INVALID;
	if (cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		return CLUSTER_NORMAL_STOP_INVALID;
	if (!cluster_normal_stop_requested()
		|| pg_atomic_read_u32(&cl_normal_stop->frontends_gone) != 1) {
		if (reason_out != NULL)
			*reason_out = "NORMAL_STOP_LOCAL_FRONTENDS_PENDING";
		return CLUSTER_NORMAL_STOP_PENDING;
	}

	/* Reserve one attempt BEFORE waiting for the durable identity reader.
	 * Repeated polls never renew its deadline or erase a peer's early request. */
	now = (uint64)GetCurrentTimestamp();
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	phase = pg_atomic_read_u32(&cl_normal_stop->phase);
	if (cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE
		|| phase > CLUSTER_NORMAL_STOP_DRAIN || cl_state->leaving_node_id != -1
		|| pg_atomic_read_u32(&cl_state->phase) != CLUSTER_LEAVE_IDLE) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
	} else if (pg_atomic_read_u32(&cl_state->request_in_progress) == 0) {
		uint32 expected = 0;
		uint64 prior_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
		uint64 timeout = cluster_clean_leave_drain_timeout_ms > 0
							 ? (uint64)cluster_clean_leave_drain_timeout_ms * UINT64_C(1000)
							 : 0;

		nonce = now == prior_nonce ? now + 1 : now;
		if (phase != CLUSTER_NORMAL_STOP_IDLE || now == 0 || nonce == 0 || nonce == UINT64_MAX
			|| timeout == 0 || now >= UINT64_MAX - timeout
			|| pg_atomic_read_u32(&cl_state->shutdown_driven) != 0
			|| pg_atomic_read_u32(&cl_state->preflight_pending) != 0
			|| pg_atomic_read_u32(&cl_state->preflight_sent) != 0
			|| pg_atomic_read_u32(&cl_state->phase1_release_pending) != 0
			|| pg_atomic_read_u32(&cl_state->nak_received) != 0
			|| !cl_phase1_bytes_zero(cl_state->ack_bitmap, sizeof(cl_state->ack_bitmap))
			|| !pg_atomic_compare_exchange_u32(&cl_state->request_in_progress, &expected, 1))
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
		else {
			pg_atomic_write_u32(&cl_state->shutdown_driven, 1);
			pg_atomic_write_u64(&cl_state->leave_attempt_nonce, nonce);
			cl_state->barrier_deadline_us = now + timeout;
		}
	}
	nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	deadline = cl_state->barrier_deadline_us;
	if (pg_atomic_read_u32(&cl_state->request_in_progress) != 1
		|| pg_atomic_read_u32(&cl_state->shutdown_driven) != 1 || nonce == 0 || nonce == UINT64_MAX
		|| deadline == 0 || deadline == UINT64_MAX)
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
	else if (now >= deadline) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_DEADLINE);
		reason = "NORMAL_STOP_FRONT_CUT_DEADLINE";
	}
	LWLockRelease(&cl_state->lock);
	if (cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		goto done;

	result = cl_normal_stop_identity_poll(false, &observed, &reason);
	if (result != CLUSTER_NORMAL_STOP_READY)
		goto done;
	result = CLUSTER_NORMAL_STOP_PENDING;
	reason = "NORMAL_STOP_PEER_FRONTENDS_PENDING";
	own_bit = UINT32_C(1) << cluster_node_id;
	peer_bits = UINT32_C(15) & ~own_bit;
	now = (uint64)GetCurrentTimestamp();
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	phase = pg_atomic_read_u32(&cl_normal_stop->phase);
	if (now >= deadline)
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_DEADLINE);
	if (phase > CLUSTER_NORMAL_STOP_DRAIN || pg_atomic_read_u32(&cl_state->request_in_progress) != 1
		|| pg_atomic_read_u32(&cl_state->shutdown_driven) != 1
		|| pg_atomic_read_u64(&cl_state->leave_attempt_nonce) != nonce
		|| cl_state->barrier_deadline_us != deadline
		|| (cl_normal_stop->peer_requests_seen & ~UINT32_C(15)) != 0
		|| (cl_normal_stop->peer_request_sent & ~peer_bits) != 0)
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
	for (peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
		bool seen = (cl_normal_stop->peer_requests_seen & (UINT32_C(1) << peer)) != 0;
		uint64 stored = cl_normal_stop->peer_request_nonce[peer];
		if (seen != (stored != 0) || stored == UINT64_MAX
			|| (peer == cluster_node_id && stored != 0 && stored != nonce))
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
	}
	if (cluster_normal_stop_failure() == CLUSTER_NORMAL_STOP_FAILURE_NONE) {
		cl_normal_stop->peer_request_nonce[cluster_node_id] = nonce;
		cl_normal_stop->peer_requests_seen |= own_bit;
		if (phase == CLUSTER_NORMAL_STOP_IDLE)
			pg_atomic_write_u32(&cl_normal_stop->phase, CLUSTER_NORMAL_STOP_WAIT_PEER_FRONTS);
		if (cl_normal_stop->peer_requests_seen == 15
			&& cl_normal_stop->peer_request_sent == peer_bits) {
			pg_atomic_write_u32(&cl_normal_stop->phase, CLUSTER_NORMAL_STOP_DRAIN);
			observed.valid = true;
			observed.attempt_nonce = nonce;
			observed.absolute_deadline_us = deadline;
			*plan_out = observed;
			result = CLUSTER_NORMAL_STOP_READY;
			reason = "NORMAL_STOP_ALL_FRONTENDS_CUT_NOT_DRAIN_ACK";
		}
	}
	LWLockRelease(&cl_state->lock);
done:
	if (cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		result = CLUSTER_NORMAL_STOP_INVALID;
	if (reason_out != NULL)
		*reason_out = reason;
	return result;
}

static void
cl_normal_stop_fronts_announce(const ClusterICEnvelope *env,
							   const ClusterLeaveAnnouncePayload *request)
{
	ClNormalStopFrontInbox *inbox;
	uint64 known;
	int peer;

	if (!IsUnderPostmaster || !AmLmonProcess() || cl_state == NULL || cl_normal_stop == NULL
		|| env == NULL || request == NULL || !cluster_enabled)
		return;
	peer = (int)env->source_node_id;
	if (peer < 0 || peer >= CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT || peer == cluster_node_id
		|| env->msg_type != PGRAC_IC_MSG_CLEAN_LEAVE_ANNOUNCE
		|| (env->dest_node_id != (uint32)cluster_node_id && env->dest_node_id != PGRAC_IC_BROADCAST)
		|| env->payload_length != sizeof(*request)
		|| !cluster_clean_leave_announce_payload_valid(request)
		|| request->producer_kind != CLUSTER_LEAVE_PRODUCER_SHUTDOWN
		|| request->preflight != CLUSTER_PHASE1_FULL_STOP_WIRE_BARRIER
		|| request->leaving_node_id != peer || request->leave_epoch != env->epoch
		|| request->leave_nonce == 0 || request->leave_nonce == UINT64_MAX) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		return;
	}
	if (cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		return;
	inbox = &cl_normal_stop_front_inbox[peer];
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	known = cl_normal_stop->peer_request_nonce[peer];
	if (known != 0 && cl_normal_stop->epoch != env->epoch)
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
	else if (known == request->leave_nonce) {
		/* A predecessor duplicate cannot displace a retained successor. */
	} else if (cl_state->phase1_release_request_nonce[peer] != 0) {
		/* This STOPPED round was already consumed, possibly before local
		 * release arm. Exact replay creates no new ownership. */
		if (cl_state->phase1_release_request_nonce[peer] != request->leave_nonce)
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
	} else if (inbox->pending
			   && (inbox->request.leave_nonce != request->leave_nonce
				   || inbox->envelope.epoch != env->epoch))
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
	else if (known != request->leave_nonce && !inbox->pending) {
		inbox->envelope = *env;
		inbox->request = *request;
		inbox->pending = true;
	}
	LWLockRelease(&cl_state->lock);
}

static void
cl_normal_stop_fronts_ack(const ClusterICEnvelope *env, const ClusterLeaveAckPayload *ack)
{
	ClNormalStopFrontInbox *inbox;
	bool nak;
	int peer;

	if (!IsUnderPostmaster || !AmLmonProcess() || cl_state == NULL || cl_normal_stop == NULL
		|| env == NULL || ack == NULL || !cluster_enabled
		|| cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		return;
	peer = (int)env->source_node_id;
	nak = env->msg_type == PGRAC_IC_MSG_LEAVE_DRAIN_NAK;
	if (peer < 0 || peer >= CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT || peer == cluster_node_id
		|| (!nak && env->msg_type != PGRAC_IC_MSG_LEAVE_DRAIN_ACK)
		|| env->dest_node_id != (uint32)cluster_node_id || env->payload_length != sizeof(*ack)
		|| !cluster_clean_leave_ack_payload_valid(ack) || ack->survivor_node_id != peer
		|| ack->leaving_node_id != cluster_node_id || ack->leave_epoch != env->epoch
		|| ack->phase1_round != 0 || ack->nak != (uint8)nak
		|| (!nak && ack->nak_reason != CLUSTER_LEAVE_NAK_NONE)) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		return;
	}
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	/* Like the original wire consumer, a delayed ACK for another attempt is
	 * not a vote. It cannot replace this attempt or extend its budget. */
	if (pg_atomic_read_u32(&cl_state->request_in_progress) != 1
		|| pg_atomic_read_u32(&cl_state->shutdown_driven) != 1
		|| ack->leave_nonce != pg_atomic_read_u64(&cl_state->leave_attempt_nonce)) {
		LWLockRelease(&cl_state->lock);
		return;
	}
	if (pg_atomic_read_u32(&cl_state->phase1_release_pending) != 0) {
		if (ack->leave_epoch != cl_normal_stop->epoch || nak
			|| !cl_phase1_member_bit_is_set(cl_state->ack_bitmap, peer))
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		LWLockRelease(&cl_state->lock);
		return;
	}
	inbox = &cl_normal_stop_front_inbox[peer];
	if (inbox->ack_pending && inbox->ack.leave_nonce != ack->leave_nonce)
		inbox->ack_pending = false; /* completed predecessor, never a new vote */
	if (inbox->ack_pending
		&& (inbox->ack.leave_nonce != ack->leave_nonce || inbox->ack.leave_epoch != ack->leave_epoch
			|| inbox->ack.nak != ack->nak || inbox->ack.nak_reason != ack->nak_reason))
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
	else if (!inbox->ack_pending) {
		inbox->ack_envelope = *env;
		inbox->ack = *ack;
		inbox->ack_pending = true;
	}
	LWLockRelease(&cl_state->lock);
}

static void
cl_normal_stop_fronts_lmon_tick(void)
{
	ClusterPhase1FullStopPlan observed;
	ClusterNormalStopPollResult result;
	bool inbox_work = false;
	int peer;
	uint32 phase;

	if (!IsUnderPostmaster || !AmLmonProcess() || cl_state == NULL || cl_normal_stop == NULL
		|| !cluster_enabled || cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		return;
	for (peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++)
		inbox_work |= cl_normal_stop_front_inbox[peer].pending
					  || cl_normal_stop_front_inbox[peer].ack_pending;
	if (!inbox_work
		&& (!cluster_normal_stop_requested()
			|| (pg_atomic_read_u32(&cl_normal_stop->identity_published) == 0
				&& cluster_semantic_activation_phase1_pristine())))
		return;
	phase = pg_atomic_read_u32(&cl_normal_stop->phase);
	/* The checkpoint/STOPPED successor has its own WAL-state and nonce cut. */
	if (phase >= CLUSTER_NORMAL_STOP_CHECKPOINT)
		return;
	result = cl_normal_stop_identity_poll(false, &observed, NULL);
	if (result != CLUSTER_NORMAL_STOP_READY)
		return;

	for (peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
		ClNormalStopFrontInbox *inbox = &cl_normal_stop_front_inbox[peer];
		ClusterPhase1FullStopPlan verified;
		bool source_active = false, source_stopped = false;
		uint32 bit = UINT32_C(1) << peer;

		if (!inbox->pending)
			continue;
		if (inbox->envelope.epoch != observed.epoch
			|| !cl_normal_stop_capture_source_phase(&observed, peer, inbox->request.leave_nonce,
													&source_active, &source_stopped)) {
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
			return;
		}
		/* A transport-consumed frame is retained if it is a STOPPED successor;
		 * it must never be used as an ACTIVE frontend-cut request. */
		if (!source_active || source_stopped)
			continue;
		result = cl_normal_stop_identity_poll(false, &verified, NULL);
		if (result != CLUSTER_NORMAL_STOP_READY)
			return;
		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		if (cl_normal_stop->peer_request_nonce[peer] != 0
			&& cl_normal_stop->peer_request_nonce[peer] != inbox->request.leave_nonce)
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		if (cluster_normal_stop_failure() == CLUSTER_NORMAL_STOP_FAILURE_NONE
			&& pg_atomic_read_u32(&cl_normal_stop->phase) < CLUSTER_NORMAL_STOP_CHECKPOINT
			&& verified.epoch == inbox->envelope.epoch
			&& cl_normal_stop->peer_request_nonce[peer] == 0) {
			cl_normal_stop->peer_request_nonce[peer] = inbox->request.leave_nonce;
			cl_normal_stop->peer_requests_seen |= bit;
			cl_normal_stop->peer_reply_pending |= bit;
			inbox->pending = false;
		}
		LWLockRelease(&cl_state->lock);
	}
	if (cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		return;

	for (peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
		ClNormalStopFrontInbox *inbox = &cl_normal_stop_front_inbox[peer];
		ClusterPhase1FullStopPlan verified;
		bool source_active = false, source_stopped = false;
		if (!inbox->ack_pending)
			continue;
		if (inbox->ack_envelope.epoch != observed.epoch
			|| (!cluster_normal_stop_native_wal_mode()
				&& !cl_phase1_full_stop_capture_source_phase(&observed, peer, &source_active,
															 &source_stopped))) {
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
			return;
		}
		result = cl_normal_stop_identity_poll(false, &verified, NULL);
		if (result != CLUSTER_NORMAL_STOP_READY)
			return;
		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		if (inbox->ack.leave_nonce != pg_atomic_read_u64(&cl_state->leave_attempt_nonce)
			|| verified.epoch != inbox->ack_envelope.epoch)
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		else if (inbox->ack.nak) {
			pg_atomic_write_u32(&cl_state->nak_reason, inbox->ack.nak_reason);
			pg_atomic_write_u32(&cl_state->nak_received, 1);
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_MODULE);
		} else if ((cl_normal_stop->peer_requests_seen & (UINT32_C(1) << peer)) != 0) {
			/* Evidence may arrive before our own drain finishes. Recording it
			 * neither advances the local phase nor permits a checkpoint. */
			cl_phase1_member_bit_set(cl_state->ack_bitmap, peer);
			inbox->ack_pending = false;
		}
		LWLockRelease(&cl_state->lock);
	}
	if (cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		return;

	/* Send one existing ACTIVE request per peer. NOT_ADMITTED keeps its
	 * original owner; DONE/WOULD_BLOCK transfers it to the existing transport.
	 * This tick NEVER emits a drain ACK: only the later sealed DRAIN cut may. */
	for (peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
		ClusterLeaveAnnouncePayload request;
		ClusterICSendResult sent;
		uint64 nonce = 0, deadline = 0;
		uint32 bit = UINT32_C(1) << peer;
		bool needed;

		if (peer == cluster_node_id)
			continue;
		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		needed
			= cluster_normal_stop_requested()
			  && pg_atomic_read_u32(&cl_normal_stop->frontends_gone) == 1
			  && pg_atomic_read_u32(&cl_normal_stop->phase) == CLUSTER_NORMAL_STOP_WAIT_PEER_FRONTS
			  && (cl_normal_stop->peer_request_sent & bit) == 0;
		if (needed) {
			nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
			deadline = cl_state->barrier_deadline_us;
			if (nonce == 0 || nonce == UINT64_MAX || deadline == 0
				|| pg_atomic_read_u32(&cl_state->request_in_progress) != 1
				|| pg_atomic_read_u32(&cl_state->shutdown_driven) != 1
				|| cl_normal_stop->peer_request_nonce[cluster_node_id] != nonce)
				cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
		}
		LWLockRelease(&cl_state->lock);
		if (cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
			return;
		if (!needed)
			continue;
		if ((uint64)GetCurrentTimestamp() >= deadline) {
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_DEADLINE);
			return;
		}
		memset(&request, 0, sizeof(request));
		request.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
		request.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
		request.leaving_node_id = cluster_node_id;
		request.producer_kind = CLUSTER_LEAVE_PRODUCER_SHUTDOWN;
		request.preflight = CLUSTER_PHASE1_FULL_STOP_WIRE_BARRIER;
		request.leave_epoch = observed.epoch;
		request.leave_nonce = nonce;
		cluster_clean_leave_announce_compute_crc(&request);
		sent = cluster_ic_send_envelope(PGRAC_IC_MSG_CLEAN_LEAVE_ANNOUNCE, peer, &request,
										(uint32)sizeof(request));
		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		if (pg_atomic_read_u64(&cl_state->leave_attempt_nonce) != nonce
			|| cl_state->barrier_deadline_us != deadline
			|| pg_atomic_read_u32(&cl_normal_stop->phase) != CLUSTER_NORMAL_STOP_WAIT_PEER_FRONTS)
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
		else if (sent == CLUSTER_IC_SEND_DONE || sent == CLUSTER_IC_SEND_WOULD_BLOCK)
			cl_normal_stop->peer_request_sent |= bit;
		else if (sent != CLUSTER_IC_SEND_NOT_ADMITTED)
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
		LWLockRelease(&cl_state->lock);
	}

	/* The checkpointer alone advances to WAIT_DRAIN_ACK, after the full
	 * module/cleaner/service seal. Earlier peer requests only set pending. */
	for (peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
		ClusterLeaveAckPayload ack;
		ClusterICSendResult sent;
		uint32 bit = UINT32_C(1) << peer;
		uint64 nonce = 0, deadline = 0;
		bool needed;
		if (peer == cluster_node_id)
			continue;
		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		needed = pg_atomic_read_u32(&cl_normal_stop->phase) == CLUSTER_NORMAL_STOP_WAIT_DRAIN_ACK
				 && (cl_normal_stop->peer_reply_pending & bit) != 0
				 && (cl_normal_stop->peer_reply_sent & bit) == 0;
		if (needed) {
			nonce = cl_normal_stop->peer_request_nonce[peer];
			deadline = cl_state->barrier_deadline_us;
			if (!cluster_normal_stop_requested()
				|| pg_atomic_read_u32(&cl_normal_stop->frontends_gone) != 1
				|| pg_atomic_read_u32(&cl_state->request_in_progress) != 1
				|| pg_atomic_read_u32(&cl_state->shutdown_driven) != 1
				|| cl_normal_stop->peer_requests_seen != 15
				|| pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesce_requested) != 1
				|| pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesced_mask) != 255
				|| pg_atomic_read_u32(&cl_normal_stop->service_seal) != 1 || nonce == 0
				|| nonce == UINT64_MAX || deadline == 0)
				cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
		}
		LWLockRelease(&cl_state->lock);
		if (cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
			return;
		if (!needed)
			continue;
		if ((uint64)GetCurrentTimestamp() >= deadline) {
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_DEADLINE);
			return;
		}
		memset(&ack, 0, sizeof(ack));
		ack.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
		ack.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
		ack.survivor_node_id = cluster_node_id;
		ack.leaving_node_id = peer;
		ack.leave_epoch = observed.epoch;
		ack.leave_nonce = nonce;
		cluster_clean_leave_ack_compute_crc(&ack);
		sent = cluster_ic_send_envelope(PGRAC_IC_MSG_LEAVE_DRAIN_ACK, peer, &ack, sizeof(ack));
		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		if (pg_atomic_read_u32(&cl_normal_stop->phase) != CLUSTER_NORMAL_STOP_WAIT_DRAIN_ACK
			|| cl_normal_stop->peer_request_nonce[peer] != nonce
			|| cl_state->barrier_deadline_us != deadline)
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
		else if (sent == CLUSTER_IC_SEND_DONE || sent == CLUSTER_IC_SEND_WOULD_BLOCK) {
			cl_normal_stop->peer_reply_sent |= bit;
			cl_normal_stop->peer_reply_pending &= ~bit;
		} else if (sent != CLUSTER_IC_SEND_NOT_ADMITTED)
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
		LWLockRelease(&cl_state->lock);
	}
	if (ProcGlobal != NULL && ProcGlobal->checkpointerLatch != NULL)
		SetLatch(ProcGlobal->checkpointerLatch);
}

ClusterNormalStopPollResult
cluster_normal_stop_modules_poll(bool post_checkpoint,
								 ClusterNormalStopModuleObservation *observation)
{
	static const char *const names[]
		= { "ACTIVE_WRITE", "TT_SLOT", "UNDO_BLOCK0", "CTRC", "PCM",	  "GCS", "SF",
			"GES_REPLY",	"GRD",	   "BUFMGR",	  "OID",  "SEQUENCE", "HW",	 "CF" };
	ClusterNormalStopModuleObservation ignored;
	ClusterPhase1FullStopPlan identity;
	ClusterNormalStopPollResult result, aggregate = CLUSTER_NORMAL_STOP_READY;
	uint8 root[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint64 epoch;
	uint32 phase;
	const char *reason;

	if (observation == NULL)
		observation = &ignored;
	memset(observation, 0, sizeof(*observation));
	observation->module = "COORDINATOR";
	observation->reason = "NORMAL_STOP_MODULE_CONTEXT_INVALID";
	if (!IsUnderPostmaster || (!AmCheckpointerProcess() && !(post_checkpoint && AmLmonProcess()))
		|| cl_state == NULL || cl_normal_stop == NULL || !cluster_normal_stop_requested()
		|| cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		return CLUSTER_NORMAL_STOP_INVALID;
	phase = pg_atomic_read_u32(&cl_normal_stop->phase);
	if ((!post_checkpoint
		 && (phase < CLUSTER_NORMAL_STOP_DRAIN || phase > CLUSTER_NORMAL_STOP_CHECKPOINT))
		|| (post_checkpoint && phase != CLUSTER_NORMAL_STOP_CHECKPOINT
			&& phase != CLUSTER_NORMAL_STOP_POST_STOPPED)) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
		return CLUSTER_NORMAL_STOP_INVALID;
	}
	result = cl_normal_stop_identity_poll(post_checkpoint, &identity, &reason);
	if (result != CLUSTER_NORMAL_STOP_READY) {
		observation->reason = reason;
		return result;
	}
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	memcpy(root, cl_normal_stop->root_descriptor, sizeof(root));
	epoch = cl_normal_stop->epoch;
	LWLockRelease(&cl_state->lock);

	/* Observations are deliberately not an atomic snapshot. Each owner uses
	 * its original locks; the caller must also establish the producer/actor
	 * cut. A later INVALID must not be hidden by an earlier PENDING. */
	for (unsigned owner = 0; owner < lengthof(names); owner++) {
		ClusterNormalStopModuleObservation current = { .module = names[owner] };
		BufferTag tag = { 0 };
		ClusterResId resid = { 0 };
		GesReplyWaitKey reply = { 0 };
		ClusterCtrcNormalStopObservation ctrc = { 0 };
		uint32 index = 0;
		uint64 key = 0;
		int backend = -1, slot = -1;
		const char *domain = NULL;
		current.reason = "NORMAL_STOP_OWNER_RESULT_INVALID";
		switch (owner) {
		case 0:
			result = cluster_undo_active_write_normal_stop_poll(&backend, &current.reason);
			snprintf(current.object, sizeof(current.object), "backend=%d", backend);
			break;
		case 1:
			result = cluster_tt_slot_normal_stop_poll(&index, &slot, &current.reason);
			snprintf(current.object, sizeof(current.object), "segment=%u slot=%d", index, slot);
			break;
		case 2:
			result = cluster_undo_block0_normal_stop_poll(post_checkpoint, root, epoch, &index,
														  &slot, &current.reason);
			snprintf(current.object, sizeof(current.object), "segment=%u slot=%d", index, slot);
			break;
		case 3:
			result = cluster_ctrc_normal_stop_poll(&ctrc);
			current.reason = "NORMAL_STOP_CTRC_OWNER_RESULT";
			snprintf(current.object, sizeof(current.object),
					 "domain=%u reason=%u index=" UINT64_FORMAT
					 " state=%u segment=%u generation=%u slot=%u wrap=%u xid=%u",
					 (unsigned)ctrc.domain, (unsigned)ctrc.reason, ctrc.object_index, ctrc.state,
					 ctrc.key.segment_id, ctrc.key.segment_generation, ctrc.key.slot_offset,
					 ctrc.key.slot_wrap, ctrc.key.xid);
			break;
		case 4:
			result = cluster_pcm_normal_stop_poll(post_checkpoint, &tag, &index, &current.reason);
			break;
		case 5:
			result = cluster_gcs_block_normal_stop_poll(post_checkpoint, &backend, &slot,
														&current.reason);
			snprintf(current.object, sizeof(current.object), "backend=%d slot=%d", backend, slot);
			break;
		case 6:
			result = cluster_sf_dep_normal_stop_poll(post_checkpoint, &slot, &backend,
													 &current.reason);
			snprintf(current.object, sizeof(current.object), "slot=%d origin=%d", slot, backend);
			break;
		case 7:
			result = cluster_ges_reply_wait_normal_stop_poll(&reply, &current.reason);
			snprintf(current.object, sizeof(current.object),
					 "request=" UINT64_FORMAT " source=%d dest=%d opcode=%u epoch=" UINT64_FORMAT,
					 reply.request_id, reply.source_node_id, reply.dest_node_id,
					 reply.request_opcode, reply.cluster_epoch);
			break;
		case 8:
			result = cluster_grd_normal_stop_poll(&resid, &index, &current.reason);
			break;
		case 9:
			result
				= cluster_bufmgr_normal_stop_poll(post_checkpoint, &tag, &backend, &current.reason);
			break;
		case 10:
			result = cluster_oid_lease_normal_stop_poll(&current.reason);
			strlcpy(current.object, "oid-lease", sizeof(current.object));
			break;
		case 11:
			result = cluster_sequence_normal_stop_poll(&resid, &current.reason);
			break;
		case 12:
			result = cluster_hw_normal_stop_poll(&domain, &key, &backend, &current.reason);
			snprintf(current.object, sizeof(current.object),
					 "domain=%s key=" UINT64_FORMAT " backend=%d",
					 domain != NULL ? domain : "UNKNOWN", key, backend);
			break;
		case 13:
			result = cluster_cf_normal_stop_poll(post_checkpoint, &current.reason);
			strlcpy(current.object, "cf-local-and-shared", sizeof(current.object));
			break;
		default:
			result = CLUSTER_NORMAL_STOP_INVALID;
		}
		if (owner == 4 || owner == 9)
			snprintf(current.object, sizeof(current.object), "tag=%u/%u/%u/%u/%u slot=%u buffer=%d",
					 tag.spcOid, tag.dbOid, tag.relNumber, (unsigned)tag.forkNum, tag.blockNum,
					 index, backend);
		if (owner == 8 || owner == 11)
			snprintf(current.object, sizeof(current.object), "resid=%u/%u/%u/%u/%u/%u shard=%u",
					 resid.field1, resid.field2, resid.field3, resid.field4, resid.type,
					 resid.lockmethodid, index);
		if (result != CLUSTER_NORMAL_STOP_READY && result != CLUSTER_NORMAL_STOP_PENDING)
			result = CLUSTER_NORMAL_STOP_INVALID;
		if (result < aggregate) {
			aggregate = result;
			*observation = current;
		}
	}
	if (aggregate == CLUSTER_NORMAL_STOP_INVALID) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_MODULE);
		return aggregate;
	}
	/* The final original WAL/formation/root sample closes the observation
	 * interval, not the producer cut. Neither check mutates a module. */
	result = cl_normal_stop_identity_poll(post_checkpoint, &identity, &reason);
	if (result != CLUSTER_NORMAL_STOP_READY) {
		observation->module = "IDENTITY";
		observation->reason = reason;
		observation->object[0] = '\0';
		return result;
	}
	if (aggregate == CLUSTER_NORMAL_STOP_READY) {
		observation->module = "ALL_SHARED_OWNERS";
		observation->reason = "NORMAL_STOP_MODULES_READY_NOT_A_SERVICE_CUT";
	}
	return aggregate;
}

static bool
cl_normal_stop_service_mask_valid(uint32 expected)
{
	uint32 fixed = UINT32_C(513) | (cluster_lmd_enabled ? UINT32_C(1024) : 0);
	uint32 pool = (expected & ~UINT32_C(1537)) >> 1;
	return expected <= 2047 && (expected & 1537) == fixed && pool != 0 && (pool & (pool + 1)) == 0;
}

/* This immutable PGC_POSTMASTER mask never consults surviving PIDs. The
 * postmaster separately verifies and retains every actual-start process. */
static uint32
cl_normal_stop_config_service_mask(void)
{
	return cluster_lms_workers >= 1 && cluster_lms_workers <= 8
			   ? UINT32_C(513) | (cluster_lmd_enabled ? UINT32_C(1024) : 0)
					 | (((UINT32_C(1) << cluster_lms_workers) - 1) << 1)
			   : 0;
}

/* The original plan and tail remain the identity; the next existing wire
 * round changes only the request nonce. Original frontend nonces stay put. */
static bool
cl_normal_stop_post_state_locked(const ClusterPhase1FullStopPlan *plan, uint32 phase, uint64 now,
								 uint32 expected_services, bool release)
{
	uint32 peers = UINT32_C(15) & ~(UINT32_C(1) << cluster_node_id);
	uint32 active = pg_atomic_read_u32(&cl_normal_stop->service_active_mask);
	uint32 idle = pg_atomic_read_u32(&cl_normal_stop->service_idle_mask);
	uint64 nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	if (!plan->valid || plan->epoch != cl_normal_stop->epoch
		|| plan->own_wal_started_at != cl_normal_stop->own_wal_started_at
		|| memcmp(plan->member_incarnations, cl_normal_stop->member_incarnations,
				  sizeof(plan->member_incarnations))
			   != 0
		|| plan->attempt_nonce == 0 || plan->attempt_nonce == UINT64_MAX
		|| plan->attempt_nonce != nonce || plan->absolute_deadline_us == 0
		|| plan->absolute_deadline_us == UINT64_MAX
		|| plan->absolute_deadline_us != cl_state->barrier_deadline_us)
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
	if ((phase != CLUSTER_NORMAL_STOP_CHECKPOINT && phase != CLUSTER_NORMAL_STOP_POST_STOPPED
		 && !(release && phase == CLUSTER_NORMAL_STOP_PROTOCOL_CLOSED))
		|| (release && phase == CLUSTER_NORMAL_STOP_CHECKPOINT)
		|| pg_atomic_read_u32(&cl_normal_stop->phase) != phase
		|| pg_atomic_read_u32(&cl_normal_stop->identity_published) != 1
		|| !cluster_normal_stop_requested()
		|| pg_atomic_read_u32(&cl_normal_stop->frontends_gone) != 1
		|| pg_atomic_read_u32(&cl_state->request_in_progress) != 1
		|| pg_atomic_read_u32(&cl_state->shutdown_driven) != 1 || cl_state->leaving_node_id != -1
		|| pg_atomic_read_u32(&cl_state->phase) != CLUSTER_LEAVE_IDLE
		|| pg_atomic_read_u32(&cl_state->nak_received) != 0
		|| pg_atomic_read_u32(&cl_state->phase1_release_pending) != (uint32)release
		|| cl_normal_stop->peer_requests_seen != 15 || cl_normal_stop->peer_request_sent != peers
		|| cl_normal_stop->peer_reply_pending != 0 || cl_normal_stop->peer_reply_sent != peers
		|| pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesce_requested) != 1
		|| pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesced_mask) != 255
		|| pg_atomic_read_u32(&cl_normal_stop->service_seal)
			   != (phase == CLUSTER_NORMAL_STOP_PROTOCOL_CLOSED ? 2 : 1)
		|| !cl_normal_stop_service_mask_valid(expected_services))
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
	if ((phase == CLUSTER_NORMAL_STOP_CHECKPOINT
		 && (cl_normal_stop->peer_request_nonce[cluster_node_id] != nonce
			 || pg_atomic_read_u32(&cl_state->preflight_pending) != 0
			 || pg_atomic_read_u32(&cl_state->preflight_sent) != 0
			 || cl_state->ack_bitmap[0] != peers))
		|| (phase >= CLUSTER_NORMAL_STOP_POST_STOPPED
			&& (!cluster_clean_leave_phase1_full_stop_nonce_fresh(
					cl_normal_stop->peer_request_nonce[cluster_node_id], nonce)
				|| pg_atomic_read_u32(&cl_state->preflight_pending) != (release ? 0 : 1)
				|| pg_atomic_read_u32(&cl_state->preflight_sent) != 1
				|| (cl_state->ack_bitmap[0] & ~peers) != 0))
		|| !cl_phase1_bytes_zero(cl_state->ack_bitmap + 1, sizeof(cl_state->ack_bitmap) - 1))
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
	if (((active | idle) & ~expected_services) != 0 || (active & idle) != 0)
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
	/* The real checkpoint is between the two bounded protocol phases. Only
	 * its first post-arm call may cross the old pre-checkpoint deadline.
	 * Once armed, pending owners and retries must use the exact tail budget. */
	if (cl_normal_stop->post_checkpoint_deadline_us != 0
		&& cl_normal_stop->post_checkpoint_deadline_us != cl_state->barrier_deadline_us)
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
	if ((phase != CLUSTER_NORMAL_STOP_CHECKPOINT
		 || cl_normal_stop->post_checkpoint_deadline_us != 0)
		&& now >= cl_state->barrier_deadline_us)
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_DEADLINE);
	return cluster_normal_stop_failure() == CLUSTER_NORMAL_STOP_FAILURE_NONE;
}

static bool
cl_normal_stop_post_arm_state_locked(const ClusterPhase1FullStopPlan *plan, uint32 phase,
									 uint64 now, uint32 expected_services)
{
	return cl_normal_stop_post_state_locked(plan, phase, now, expected_services, false);
}

ClusterNormalStopPollResult
cluster_normal_stop_post_checkpoint_arm(ClusterPhase1FullStopPlan *plan,
										ClusterNormalStopModuleObservation *observation)
{
	ClusterNormalStopModuleObservation ignored;
	ClusterNormalStopPollResult result;
	uint32 phase;
	uint64 now, next_nonce;
	bool wake = false;
	if (observation == NULL)
		observation = &ignored;
	memset(observation, 0, sizeof(*observation));
	observation->module = "COORDINATOR";
	observation->reason = "NORMAL_STOP_POST_CHECKPOINT_CONTEXT_INVALID";
	if (!IsUnderPostmaster || !AmCheckpointerProcess() || plan == NULL || cl_state == NULL
		|| cl_normal_stop == NULL || !cluster_enabled || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT
		|| cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		return CLUSTER_NORMAL_STOP_INVALID;
	now = (uint64)GetCurrentTimestamp();
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	phase = pg_atomic_read_u32(&cl_normal_stop->phase);
	result
		= cl_normal_stop_post_arm_state_locked(plan, phase, now, cl_normal_stop_expected_services)
			  ? CLUSTER_NORMAL_STOP_READY
			  : CLUSTER_NORMAL_STOP_INVALID;
	if (result == CLUSTER_NORMAL_STOP_READY && cl_normal_stop->post_checkpoint_deadline_us == 0) {
		uint64 budget = cluster_clean_leave_drain_timeout_ms > 0
							? (uint64)cluster_clean_leave_drain_timeout_ms * UINT64_C(1000)
							: 0;

		if (phase != CLUSTER_NORMAL_STOP_CHECKPOINT || now == 0 || budget == 0
			|| now >= UINT64_MAX - budget) {
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
			result = CLUSTER_NORMAL_STOP_INVALID;
		} else {
			cl_normal_stop->post_checkpoint_deadline_us = now + budget;
			cl_state->barrier_deadline_us = now + budget;
			plan->absolute_deadline_us = now + budget;
		}
	}
	LWLockRelease(&cl_state->lock);
	if (result != CLUSTER_NORMAL_STOP_READY)
		return result;
	/* This requires the original durable own STOPPED, not the fact that the
	 * caller reached phase5. PI/undo/HW post-cut obligations must also finish. */
	result = cluster_normal_stop_modules_poll(true, observation);
	if (result != CLUSTER_NORMAL_STOP_READY)
		return result;
	now = (uint64)GetCurrentTimestamp();
	next_nonce = now;
	if (!cluster_clean_leave_phase1_full_stop_nonce_fresh(plan->attempt_nonce, next_nonce))
		next_nonce = plan->attempt_nonce < UINT64_MAX - 1 ? plan->attempt_nonce + 1
														  : plan->attempt_nonce - 1;
	result = CLUSTER_NORMAL_STOP_PENDING;
	observation->module = "SERVICE";
	observation->reason = "NORMAL_STOP_POST_CHECKPOINT_AWAIT_IDLE_CUT";
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	if (!cl_normal_stop_post_arm_state_locked(plan, phase, now, cl_normal_stop_expected_services))
		result = CLUSTER_NORMAL_STOP_INVALID;
	else if (pg_atomic_read_u32(&cl_normal_stop->service_active_mask) == 0
			 && pg_atomic_read_u32(&cl_normal_stop->service_idle_mask)
					== cl_normal_stop_expected_services) {
		if (phase == CLUSTER_NORMAL_STOP_CHECKPOINT) {
			/* Preserve all early peer ownership. Only the caller's completed
			 * predecessor ACK bitmap is consumed when arming this next round. */
			pg_atomic_write_u64(&cl_state->leave_attempt_nonce, next_nonce);
			memset(cl_state->ack_bitmap, 0, sizeof(cl_state->ack_bitmap));
			pg_atomic_write_u32(&cl_state->preflight_sent, 1);
			pg_atomic_write_u32(&cl_state->preflight_pending, 1);
			pg_atomic_write_u32(&cl_normal_stop->phase, CLUSTER_NORMAL_STOP_POST_STOPPED);
			plan->attempt_nonce = next_nonce;
			wake = true;
		}
		result = CLUSTER_NORMAL_STOP_READY;
		observation->reason = "NORMAL_STOP_POST_STOPPED_ROUND_ARMED_NOT_COMPLETE";
	}
	LWLockRelease(&cl_state->lock);
	if (wake)
		cluster_lmon_wakeup();
	return result;
}

static bool
cl_normal_stop_post_tick_state_locked(const ClusterPhase1FullStopPlan *plan, uint32 expected)
{
	/* A lawful successor may have been published while a WAL read or send
	 * was outside the leave lock. An obsolete tick is not an identity error. */
	if (pg_atomic_read_u32(&cl_normal_stop->phase) == CLUSTER_NORMAL_STOP_PROTOCOL_CLOSED
		|| (pg_atomic_read_u32(&cl_normal_stop->phase) == CLUSTER_NORMAL_STOP_POST_STOPPED
			&& pg_atomic_read_u32(&cl_state->preflight_pending) == 0
			&& pg_atomic_read_u32(&cl_state->phase1_release_pending) == 1))
		return false;
	return cl_normal_stop_post_arm_state_locked(plan, CLUSTER_NORMAL_STOP_POST_STOPPED,
												(uint64)GetCurrentTimestamp(), expected);
}

static bool
cl_normal_stop_post_peer_matches(const ClusterPhase1FullStopPlan *plan, int peer, uint64 epoch,
								 uint64 source_nonce)
{
	ClusterPhase1FullStopPlan verified;
	bool active = false, stopped = false;
	if (source_nonce == 0 && peer >= 0 && peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT)
		source_nonce = cl_state->phase1_release_request_nonce[peer];
	if (epoch != plan->epoch
		|| !cl_normal_stop_capture_source_phase(plan, peer, source_nonce, &active, &stopped)
		|| active || !stopped) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		return false;
	}
	return cl_normal_stop_identity_poll(true, &verified, NULL) == CLUSTER_NORMAL_STOP_READY;
}

static bool
cl_normal_stop_post_send(const ClusterPhase1FullStopPlan *plan, uint32 expected, int peer,
						 bool reply, uint64 nonce)
{
	ClusterICSendResult sent;
	ClusterNormalStopPollResult result;
	bool valid;
	/* Arm is not a cached permission. Reinspect this executing LMON's
	 * original private owners and the shared post-cut responsibilities
	 * before every request/reply admission. No idle is signed here. */
	result = cluster_lmon_normal_stop_poll();
	if (result != CLUSTER_NORMAL_STOP_READY) {
		if (result != CLUSTER_NORMAL_STOP_PENDING)
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_MODULE);
		return false;
	}
	result = cluster_normal_stop_modules_poll(true, NULL);
	if (result != CLUSTER_NORMAL_STOP_READY)
		return false;
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	valid = cl_normal_stop_post_tick_state_locked(plan, expected);
	if (valid)
		valid = (pg_atomic_read_u32(&cl_normal_stop->service_active_mask) & ~UINT32_C(1)) == 0
				&& ((pg_atomic_read_u32(&cl_normal_stop->service_idle_mask)
					 | pg_atomic_read_u32(&cl_normal_stop->service_active_mask))
					& expected)
					   == expected;
	LWLockRelease(&cl_state->lock);
	if (!valid)
		return false;
	sent = reply ? cl_phase1_full_stop_send_post_stopped_reply(peer, nonce, plan->epoch)
				 : cl_phase1_full_stop_send_post_stopped_request(peer, nonce, plan->epoch);
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	valid = cl_normal_stop_post_tick_state_locked(plan, expected);
	if (valid && reply && cl_state->phase1_release_request_nonce[peer] != nonce) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		valid = false;
	}
	if (valid) {
		if (cl_phase1_full_stop_send_admitted(sent)) {
			if (reply) {
				cl_phase1_member_bit_set(cl_state->phase1_post_stopped_reply_sent, peer);
				cl_state->phase1_post_stopped_reply_pending[peer / 8]
					&= ~(UINT8_C(1) << (peer % 8));
			} else
				cl_phase1_member_bit_set(cl_phase1_post_stopped_request_sent, peer);
		} else if (sent != CLUSTER_IC_SEND_NOT_ADMITTED) {
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
			valid = false;
		}
	}
	LWLockRelease(&cl_state->lock);
	return valid;
}

static void
cl_normal_stop_post_lmon_tick(void)
{
	ClusterPhase1FullStopPlan plan;
	uint32 expected, peer_bits;
	bool valid;
	if (!IsUnderPostmaster || !AmLmonProcess() || cl_state == NULL || cl_normal_stop == NULL
		|| !cluster_enabled || !cluster_normal_stop_requested()
		|| cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE
		|| pg_atomic_read_u32(&cl_normal_stop->phase) != CLUSTER_NORMAL_STOP_POST_STOPPED
		|| pg_atomic_read_u32(&cl_state->preflight_pending) == 0)
		return;
	expected = cl_normal_stop_config_service_mask();
	if (cl_normal_stop_identity_poll(true, &plan, NULL) != CLUSTER_NORMAL_STOP_READY)
		return;
	peer_bits = UINT32_C(15) & ~(UINT32_C(1) << cluster_node_id);
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	plan.valid = true;
	plan.attempt_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	plan.absolute_deadline_us = cl_state->barrier_deadline_us;
	valid = cl_normal_stop_post_tick_state_locked(&plan, expected);
	if (valid && cl_phase1_post_stopped_request_round_nonce == 0) {
		if (!cl_phase1_bytes_zero(cl_phase1_post_stopped_request_sent,
								  sizeof(cl_phase1_post_stopped_request_sent))) {
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
			valid = false;
		} else
			cl_phase1_post_stopped_request_round_nonce = plan.attempt_nonce;
	} else if (valid && cl_phase1_post_stopped_request_round_nonce != plan.attempt_nonce) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		valid = false;
	}
	LWLockRelease(&cl_state->lock);
	if (!valid)
		return;
	for (int peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
		ClNormalStopFrontInbox *inbox = &cl_normal_stop_front_inbox[peer];
		if (inbox->pending) {
			ClusterPhase1FullStopProbeNonceDecision decision;
			if (!cl_normal_stop_post_peer_matches(&plan, peer, inbox->envelope.epoch,
												  inbox->request.leave_nonce))
				return;
			LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
			valid = cl_normal_stop_post_tick_state_locked(&plan, expected);
			if (valid) {
				decision = cluster_clean_leave_phase1_full_stop_probe_nonce_decide(
					cl_normal_stop->peer_request_nonce[peer],
					cl_state->phase1_release_request_nonce[peer], inbox->request.leave_nonce);
				if (decision == CLUSTER_PHASE1_PROBE_NONCE_CONFLICT) {
					cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
					valid = false;
				} else {
					if (decision == CLUSTER_PHASE1_PROBE_NONCE_ACCEPT_STOPPED) {
						cl_state->phase1_release_request_nonce[peer] = inbox->request.leave_nonce;
						cl_phase1_member_bit_set(cl_state->phase1_post_stopped_reply_pending, peer);
					}
					inbox->pending = false;
				}
			}
			LWLockRelease(&cl_state->lock);
			if (!valid)
				return;
		}
		if (!inbox->ack_pending)
			continue;
		if (inbox->ack.leave_nonce != plan.attempt_nonce) {
			inbox->ack_pending = false;
			continue;
		}
		if (cl_state->phase1_release_request_nonce[peer] == 0)
			continue;
		if (!cl_normal_stop_post_peer_matches(&plan, peer, inbox->ack_envelope.epoch, 0))
			return;
		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		valid = cl_normal_stop_post_tick_state_locked(&plan, expected);
		if (valid) {
			if (inbox->ack.nak) {
				pg_atomic_write_u32(&cl_state->nak_reason, inbox->ack.nak_reason);
				pg_atomic_write_u32(&cl_state->nak_received, 1);
				cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_MODULE);
				valid = false;
			} else {
				cl_phase1_member_bit_set(cl_state->ack_bitmap, peer);
				inbox->ack_pending = false;
			}
		}
		LWLockRelease(&cl_state->lock);
		if (!valid)
			return;
	}
	for (int peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++)
		if (peer != cluster_node_id
			&& !cl_phase1_member_bit_is_set(cl_phase1_post_stopped_request_sent, peer)
			&& !cl_normal_stop_post_send(&plan, expected, peer, false, plan.attempt_nonce))
			return;
	for (int peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
		uint64 nonce = 0;
		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		valid = cl_normal_stop_post_tick_state_locked(&plan, expected);
		if (valid && cl_phase1_post_stopped_request_sent[0] == peer_bits
			&& cl_phase1_member_bit_is_set(cl_state->phase1_post_stopped_reply_pending, peer)
			&& !cl_phase1_member_bit_is_set(cl_state->phase1_post_stopped_reply_sent, peer)
			&& (pg_atomic_read_u32(&cl_normal_stop->service_active_mask) & ~UINT32_C(1)) == 0
			&& ((pg_atomic_read_u32(&cl_normal_stop->service_idle_mask)
				 | pg_atomic_read_u32(&cl_normal_stop->service_active_mask))
				& expected)
				   == expected)
			nonce = cl_state->phase1_release_request_nonce[peer];
		LWLockRelease(&cl_state->lock);
		if (!valid)
			return;
		if (nonce != 0 && !cl_normal_stop_post_send(&plan, expected, peer, true, nonce))
			return;
	}
	if (ProcGlobal != NULL && ProcGlobal->checkpointerLatch != NULL)
		SetLatch(ProcGlobal->checkpointerLatch);
}

static bool
cl_normal_stop_release_state_locked(const ClusterPhase1FullStopPlan *plan, uint32 expected,
									bool armed)
{
	uint32 phase = pg_atomic_read_u32(&cl_normal_stop->phase);
	uint32 peers = UINT32_C(15) & ~(UINT32_C(1) << cluster_node_id);
	const uint8 *maps[]
		= { cl_state->phase1_post_stopped_reply_pending, cl_state->phase1_post_stopped_reply_sent,
			cl_state->phase1_release_request_sent,		 cl_state->phase1_release_request_seen,
			cl_state->phase1_release_reply_sent,		 cl_state->phase1_release_reply_seen,
			cl_state->phase1_release_receipt_sent,		 cl_state->phase1_release_receipt_seen };
	if (!cl_normal_stop_post_state_locked(plan, phase, (uint64)GetCurrentTimestamp(), expected,
										  armed))
		return false;
	if (phase < CLUSTER_NORMAL_STOP_POST_STOPPED)
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
	for (unsigned i = 0; i < lengthof(maps); i++)
		if ((maps[i][0] & ~peers) != 0
			|| !cl_phase1_bytes_zero(maps[i] + 1, CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES - 1))
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
	if (pg_atomic_read_u32(&cl_state->phase1_release_transport_drained) > 1)
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
	if (armed
		&& (cl_state->ack_bitmap[0] != peers || cl_state->phase1_post_stopped_reply_pending[0] != 0
			|| cl_state->phase1_post_stopped_reply_sent[0] != peers))
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
	for (int peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
		uint64 nonce = cl_state->phase1_release_request_nonce[peer];
		if (peer == cluster_node_id) {
			if (nonce != 0)
				cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		} else if (nonce != 0
				   && !cluster_clean_leave_phase1_full_stop_nonce_fresh(
					   cl_normal_stop->peer_request_nonce[peer], nonce))
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		else if (armed && nonce == 0)
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
	}
	if (!armed
		&& (cl_state->phase1_release_request_sent[0] != 0
			|| cl_state->phase1_release_reply_sent[0] != 0
			|| cl_state->phase1_release_reply_seen[0] != 0
			|| cl_state->phase1_release_receipt_sent[0] != 0
			|| cl_state->phase1_release_receipt_seen[0] != 0
			|| pg_atomic_read_u32(&cl_state->phase1_release_transport_drained) != 0))
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
	return cluster_normal_stop_failure() == CLUSTER_NORMAL_STOP_FAILURE_NONE;
}

static bool
cl_normal_stop_release_complete_locked(void)
{
	uint32 peers = UINT32_C(15) & ~(UINT32_C(1) << cluster_node_id);
	return cl_state->phase1_release_request_sent[0] == peers
		   && cl_state->phase1_release_request_seen[0] == peers
		   && cl_state->phase1_release_reply_sent[0] == peers
		   && cl_state->phase1_release_reply_seen[0] == peers
		   && cl_state->phase1_release_receipt_sent[0] == peers
		   && cl_state->phase1_release_receipt_seen[0] == peers;
}

ClusterNormalStopPollResult
cluster_normal_stop_close_poll(const ClusterPhase1FullStopPlan *plan,
							   ClusterNormalStopModuleObservation *observation)
{
	ClusterNormalStopModuleObservation ignored;
	ClusterNormalStopPollResult result;
	bool armed, valid, wake = false;
	uint32 expected, peers;
	if (observation == NULL)
		observation = &ignored;
	memset(observation, 0, sizeof(*observation));
	observation->module = "COORDINATOR";
	observation->reason = "NORMAL_STOP_CLOSE_CONTEXT_INVALID";
	if (!IsUnderPostmaster || !AmCheckpointerProcess() || plan == NULL || cl_state == NULL
		|| cl_normal_stop == NULL || !cluster_enabled || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT
		|| cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		return CLUSTER_NORMAL_STOP_INVALID;
	expected = cl_normal_stop_expected_services;
	peers = UINT32_C(15) & ~(UINT32_C(1) << cluster_node_id);
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	armed = pg_atomic_read_u32(&cl_state->phase1_release_pending) != 0;
	valid = cl_normal_stop_release_state_locked(plan, expected, armed);
	LWLockRelease(&cl_state->lock);
	if (!valid)
		return CLUSTER_NORMAL_STOP_INVALID;
	result = cluster_normal_stop_modules_poll(true, observation);
	if (result != CLUSTER_NORMAL_STOP_READY)
		return result;
	observation->module = "COORDINATOR";
	observation->reason = armed ? "NORMAL_STOP_RELEASE_RECEIPT_OR_TRANSPORT_PENDING"
								: "NORMAL_STOP_POST_BARRIER_PENDING";
	result = CLUSTER_NORMAL_STOP_PENDING;
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	if (!cl_normal_stop_release_state_locked(plan, expected, armed))
		result = CLUSTER_NORMAL_STOP_INVALID;
	else if (pg_atomic_read_u32(&cl_normal_stop->service_active_mask) == 0
			 && pg_atomic_read_u32(&cl_normal_stop->service_idle_mask) == expected) {
		if (!armed && cl_state->ack_bitmap[0] == peers
			&& cl_state->phase1_post_stopped_reply_sent[0] == peers
			&& cl_state->phase1_post_stopped_reply_pending[0] == 0) {
			/* Consume no receipt or early peer request. Outgoing maps were
			 * verified empty, not erased to manufacture a new attempt. */
			pg_atomic_write_u32(&cl_state->preflight_pending, 0);
			pg_atomic_write_u32(&cl_state->phase1_release_pending, 1);
			wake = true;
		} else if (armed && cl_normal_stop_release_complete_locked()
				   && pg_atomic_read_u32(&cl_state->phase1_release_transport_drained) == 1) {
			/* Same leave-lock cut as actor enter/idle. No owner/transport
			 * operation occurs under this lock, and no field is reset. */
			pg_atomic_write_u32(&cl_normal_stop->service_seal, 2);
			pg_atomic_write_u32(&cl_normal_stop->phase, CLUSTER_NORMAL_STOP_PROTOCOL_CLOSED);
			observation->reason = "NORMAL_STOP_PROTOCOL_CLOSED_NOT_OS_OR_DISK_EXIT";
			result = CLUSTER_NORMAL_STOP_READY;
		}
	}
	LWLockRelease(&cl_state->lock);
	if (wake)
		cluster_lmon_wakeup();
	return result;
}

static bool
cl_normal_stop_release_receive_context(const ClusterICEnvelope *env, int peer, uint64 epoch,
									   ClusterPhase1FullStopPlan *plan)
{
	bool armed, valid;
	if (!IsUnderPostmaster || !AmLmonProcess() || cl_state == NULL || cl_normal_stop == NULL
		|| !cluster_enabled || !cluster_normal_stop_requested()
		|| cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		return false;
	if (peer < 0 || peer >= CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT || peer == cluster_node_id
		|| env->source_node_id != (uint32)peer || env->dest_node_id != (uint32)cluster_node_id
		|| env->epoch != epoch || epoch != cl_normal_stop->epoch) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		return false;
	}
	if (cl_normal_stop_identity_poll(true, plan, NULL) != CLUSTER_NORMAL_STOP_READY)
		return false;
	if (!cl_normal_stop_post_peer_matches(plan, peer, epoch, 0))
		return false;
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	plan->valid = true;
	plan->attempt_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	plan->absolute_deadline_us = cl_state->barrier_deadline_us;
	armed = pg_atomic_read_u32(&cl_state->phase1_release_pending) != 0;
	valid = cl_normal_stop_release_state_locked(plan, cl_normal_stop_config_service_mask(), armed);
	LWLockRelease(&cl_state->lock);
	return valid;
}

static bool
cl_normal_stop_release_stage_valid(const ClusterICEnvelope *env, int peer, uint64 epoch)
{
	if (!IsUnderPostmaster || !AmLmonProcess() || cl_state == NULL || cl_normal_stop == NULL
		|| !cluster_enabled || !cluster_normal_stop_requested()
		|| cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		return false;
	if (peer < 0 || peer >= CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT || peer == cluster_node_id
		|| env->source_node_id != (uint32)peer || env->dest_node_id != (uint32)cluster_node_id
		|| env->epoch != epoch || pg_atomic_read_u32(&cl_normal_stop->identity_published) != 1
		|| epoch != cl_normal_stop->epoch
		|| pg_atomic_read_u32(&cl_normal_stop->phase) < CLUSTER_NORMAL_STOP_POST_STOPPED) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		return false;
	}
	return true;
}

static void
cl_normal_stop_release_announce(const ClusterICEnvelope *env, const ClusterLeaveAnnouncePayload *p)
{
	ClusterPhase1FullStopPlan plan;
	ClNormalStopFrontInbox *inbox;
	bool armed, valid, closed;
	int peer, leg;
	if (env == NULL || p == NULL)
		return;
	peer = p->leaving_node_id;
	if (env->msg_type != PGRAC_IC_MSG_CLEAN_LEAVE_ANNOUNCE || env->payload_length != sizeof(*p)
		|| !cluster_clean_leave_announce_payload_valid(p)
		|| p->producer_kind != CLUSTER_LEAVE_PRODUCER_SHUTDOWN
		|| (p->preflight != CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE
			&& p->preflight != CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT)) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		return;
	}
	if (!cl_normal_stop_release_stage_valid(env, peer, p->leave_epoch))
		return;
	inbox = &cl_normal_stop_front_inbox[peer];
	leg = p->preflight == CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE ? 0 : 1;
	if (inbox->release_pending[leg] && memcmp(&inbox->release[leg], p, sizeof(*p)) != 0) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		return;
	}
	inbox->release_envelope[leg] = *env;
	inbox->release[leg] = *p;
	inbox->release_pending[leg] = true;
	if (!cl_normal_stop_release_receive_context(env, peer, p->leave_epoch, &plan))
		return;
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	armed = pg_atomic_read_u32(&cl_state->phase1_release_pending) != 0;
	valid = cl_normal_stop_release_state_locked(&plan, cl_normal_stop_config_service_mask(), armed);
	closed = pg_atomic_read_u32(&cl_normal_stop->phase) == CLUSTER_NORMAL_STOP_PROTOCOL_CLOSED;
	if (valid
		&& (cl_state->phase1_release_request_nonce[peer] == 0
			|| cl_state->phase1_release_request_nonce[peer] != p->leave_nonce)) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		valid = false;
	}
	if (valid && p->preflight == CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE) {
		if (closed && !cl_phase1_member_bit_is_set(cl_state->phase1_release_request_seen, peer))
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
		else
			cl_phase1_member_bit_set(cl_state->phase1_release_request_seen, peer);
	} else if (valid) {
		/* As in the original suffix, exact receipt may arrive between
		 * transport admission and local reply_sent publication. Final
		 * completion still requires that publication, not just this frame. */
		if (!armed || !cl_phase1_member_bit_is_set(cl_state->phase1_release_request_seen, peer)
			|| (closed
				&& !cl_phase1_member_bit_is_set(cl_state->phase1_release_receipt_seen, peer)))
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
		else
			cl_phase1_member_bit_set(cl_state->phase1_release_receipt_seen, peer);
	}
	if (cluster_normal_stop_failure() == CLUSTER_NORMAL_STOP_FAILURE_NONE)
		inbox->release_pending[leg] = false;
	LWLockRelease(&cl_state->lock);
	if (ProcGlobal != NULL && ProcGlobal->checkpointerLatch != NULL)
		SetLatch(ProcGlobal->checkpointerLatch);
}

static void
cl_normal_stop_release_ack(const ClusterICEnvelope *env, const ClusterLeaveAckPayload *p)
{
	ClusterPhase1FullStopPlan plan;
	ClNormalStopFrontInbox *inbox;
	bool valid, closed;
	int peer;
	if (env == NULL || p == NULL)
		return;
	peer = p->survivor_node_id;
	if (env->msg_type != PGRAC_IC_MSG_LEAVE_DRAIN_ACK || env->payload_length != sizeof(*p)
		|| !cluster_clean_leave_ack_payload_valid(p) || p->leaving_node_id != cluster_node_id
		|| p->phase1_round != CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE || p->nak != 0) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		return;
	}
	/* A delayed reply to the completed predecessor cannot fill this tally. */
	if (cl_state != NULL && p->leave_nonce != pg_atomic_read_u64(&cl_state->leave_attempt_nonce))
		return;
	if (!cl_normal_stop_release_stage_valid(env, peer, p->leave_epoch))
		return;
	inbox = &cl_normal_stop_front_inbox[peer];
	if (inbox->release_ack_pending && memcmp(&inbox->release_ack, p, sizeof(*p)) != 0) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
		return;
	}
	inbox->release_ack_envelope = *env;
	inbox->release_ack = *p;
	inbox->release_ack_pending = true;
	if (!cl_normal_stop_release_receive_context(env, peer, p->leave_epoch, &plan))
		return;
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	valid = cl_normal_stop_release_state_locked(&plan, cl_normal_stop_config_service_mask(), true);
	closed = pg_atomic_read_u32(&cl_normal_stop->phase) == CLUSTER_NORMAL_STOP_PROTOCOL_CLOSED;
	if (valid) {
		if (closed && !cl_phase1_member_bit_is_set(cl_state->phase1_release_reply_seen, peer))
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
		else
			cl_phase1_member_bit_set(cl_state->phase1_release_reply_seen, peer);
	}
	if (cluster_normal_stop_failure() == CLUSTER_NORMAL_STOP_FAILURE_NONE)
		inbox->release_ack_pending = false;
	LWLockRelease(&cl_state->lock);
	if (ProcGlobal != NULL && ProcGlobal->checkpointerLatch != NULL)
		SetLatch(ProcGlobal->checkpointerLatch);
}

/* Original six-leg maps are the pending/sent ownership. No extra queue,
 * retransmission round, deadline or force-close is introduced here. */
static void
cl_normal_stop_release_lmon_tick(void)
{
	ClusterPhase1FullStopPlan plan;
	ClusterNormalStopPollResult result;
	const char *domain, *reason;
	int object;
	uint32 sequence, expected;
	bool valid, send, drained;
	if (!IsUnderPostmaster || !AmLmonProcess() || cl_state == NULL || cl_normal_stop == NULL
		|| !cluster_normal_stop_requested()
		|| cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE
		|| pg_atomic_read_u32(&cl_normal_stop->phase) < CLUSTER_NORMAL_STOP_POST_STOPPED)
		return;
	/* Early peer RELEASE may precede this checkpointer's local arm. It is
	 * consumed under the same STOPPED identity, not dropped or a new round. */
	for (int peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
		ClNormalStopFrontInbox *inbox = &cl_normal_stop_front_inbox[peer];
		for (int leg = 0; leg < 2; leg++)
			if (inbox->release_pending[leg])
				cl_normal_stop_release_announce(&inbox->release_envelope[leg],
												&inbox->release[leg]);
		if (inbox->release_ack_pending)
			cl_normal_stop_release_ack(&inbox->release_ack_envelope, &inbox->release_ack);
	}
	if (cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE
		|| pg_atomic_read_u32(&cl_state->phase1_release_pending) != 1
		|| pg_atomic_read_u32(&cl_normal_stop->phase) != CLUSTER_NORMAL_STOP_POST_STOPPED)
		return;
	if (cl_normal_stop_identity_poll(true, &plan, NULL) != CLUSTER_NORMAL_STOP_READY)
		return;
	expected = cl_normal_stop_config_service_mask();
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	plan.valid = true;
	plan.attempt_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	plan.absolute_deadline_us = cl_state->barrier_deadline_us;
	valid = cl_normal_stop_release_state_locked(&plan, expected, true);
	LWLockRelease(&cl_state->lock);
	if (!valid)
		return;
	for (int peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
		if (peer == cluster_node_id)
			continue;
		for (int leg = 0; leg < 3; leg++) {
			uint64 nonce;
			uint8 *sent_map;
			ClusterICSendResult sent;
			LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
			valid = cl_normal_stop_release_state_locked(&plan, expected, true);
			nonce = leg == 1 ? cl_state->phase1_release_request_nonce[peer] : plan.attempt_nonce;
			sent_map = leg == 0	  ? cl_state->phase1_release_request_sent
					   : leg == 1 ? cl_state->phase1_release_reply_sent
								  : cl_state->phase1_release_receipt_sent;
			send = valid && !cl_phase1_member_bit_is_set(sent_map, peer)
				   && (leg != 1
					   || cl_phase1_member_bit_is_set(cl_state->phase1_release_request_seen, peer))
				   && (leg != 2
					   || cl_phase1_member_bit_is_set(cl_state->phase1_release_reply_seen, peer));
			LWLockRelease(&cl_state->lock);
			if (!valid)
				return;
			if (!send)
				continue;
			sent = leg == 1 ? cl_phase1_full_stop_send_release_reply(peer, nonce, plan.epoch)
							: cl_phase1_full_stop_send_release_announce(
								  peer,
								  leg == 0 ? CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE
										   : CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT,
								  nonce, plan.epoch);
			LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
			valid = cl_normal_stop_release_state_locked(&plan, expected, true);
			if (valid) {
				if (cl_phase1_full_stop_send_admitted(sent))
					cl_phase1_member_bit_set(sent_map, peer);
				else if (sent != CLUSTER_IC_SEND_NOT_ADMITTED) {
					cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
					valid = false;
				}
			}
			LWLockRelease(&cl_state->lock);
			if (!valid)
				return;
		}
	}
	/* The original IC owner includes retained FIFO, partial receive/chunks,
	 * enabled RDMA completions and callbacks. Queued is not consumed. */
	result = cluster_ic_normal_stop_poll(&domain, &object, &sequence, &reason);
	if (result != CLUSTER_NORMAL_STOP_READY && result != CLUSTER_NORMAL_STOP_PENDING) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_MODULE);
		return;
	}
	drained = result == CLUSTER_NORMAL_STOP_READY;
	for (int peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++)
		if (cl_normal_stop_front_inbox[peer].pending || cl_normal_stop_front_inbox[peer].ack_pending
			|| cl_normal_stop_front_inbox[peer].release_pending[0]
			|| cl_normal_stop_front_inbox[peer].release_pending[1]
			|| cl_normal_stop_front_inbox[peer].release_ack_pending)
			drained = false;
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	valid = cl_normal_stop_release_state_locked(&plan, expected, true);
	if (valid && cl_normal_stop_release_complete_locked())
		pg_atomic_write_u32(&cl_state->phase1_release_transport_drained, drained ? 1 : 0);
	LWLockRelease(&cl_state->lock);
	if (ProcGlobal != NULL && ProcGlobal->checkpointerLatch != NULL)
		SetLatch(ProcGlobal->checkpointerLatch);
}

ClusterNormalStopPollResult
cluster_clean_leave_normal_stop_local_poll(int *peer_out, const char **reason_out)
{
	if (peer_out != NULL)
		*peer_out = -1;
	if (reason_out != NULL)
		*reason_out = "NORMAL_STOP_CONTROL_CONTEXT_INVALID";
	if (!IsUnderPostmaster || !AmLmonProcess() || cl_state == NULL || cl_normal_stop == NULL
		|| !cluster_normal_stop_requested()
		|| cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		return CLUSTER_NORMAL_STOP_INVALID;
	/* An early STOPPED successor is intentionally retained while this node
	 * finishes CHECKPOINT. It is a control obligation, not a page producer;
	 * the post-STOPPED tick must consume it before the final close cut. */
	if (pg_atomic_read_u32(&cl_normal_stop->phase) >= CLUSTER_NORMAL_STOP_POST_STOPPED)
		for (int peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
			ClNormalStopFrontInbox *inbox = &cl_normal_stop_front_inbox[peer];
			if (inbox->pending || inbox->ack_pending || inbox->release_pending[0]
				|| inbox->release_pending[1] || inbox->release_ack_pending) {
				if (peer_out != NULL)
					*peer_out = peer;
				if (reason_out != NULL)
					*reason_out = "NORMAL_STOP_CONTROL_RETAINED_INPUT";
				return CLUSTER_NORMAL_STOP_PENDING;
			}
		}
	if (reason_out != NULL)
		*reason_out = "NORMAL_STOP_CONTROL_NO_RETAINED_INPUT";
	return CLUSTER_NORMAL_STOP_READY;
}

/* Original leave lock only. No module lookup, send, I/O or deadline renewal. */
static bool
cl_normal_stop_checkpoint_state_locked(uint32 expected_services, uint32 phase, uint64 now)
{
	uint32 peer_bits = UINT32_C(15) & ~(UINT32_C(1) << cluster_node_id);
	uint32 active = pg_atomic_read_u32(&cl_normal_stop->service_active_mask);
	uint32 idle = pg_atomic_read_u32(&cl_normal_stop->service_idle_mask);
	uint64 nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	uint64 deadline = cl_state->barrier_deadline_us;
	uint32 seal = pg_atomic_read_u32(&cl_normal_stop->service_seal);
	if (phase < CLUSTER_NORMAL_STOP_DRAIN || phase > CLUSTER_NORMAL_STOP_WAIT_DRAIN_ACK
		|| pg_atomic_read_u32(&cl_normal_stop->phase) != phase || !cluster_normal_stop_requested()
		|| pg_atomic_read_u32(&cl_normal_stop->frontends_gone) != 1
		|| pg_atomic_read_u32(&cl_normal_stop->identity_published) != 1
		|| pg_atomic_read_u32(&cl_state->request_in_progress) != 1
		|| pg_atomic_read_u32(&cl_state->shutdown_driven) != 1 || cl_state->leaving_node_id != -1
		|| pg_atomic_read_u32(&cl_state->phase) != CLUSTER_LEAVE_IDLE
		|| pg_atomic_read_u32(&cl_state->preflight_pending) != 0
		|| pg_atomic_read_u32(&cl_state->preflight_sent) != 0
		|| pg_atomic_read_u32(&cl_state->phase1_release_pending) != 0
		|| pg_atomic_read_u32(&cl_state->nak_received) != 0 || nonce == 0 || nonce == UINT64_MAX
		|| deadline == 0 || deadline == UINT64_MAX || cl_normal_stop->peer_requests_seen != 15
		|| cl_normal_stop->peer_request_sent != peer_bits
		|| ((cl_normal_stop->peer_reply_pending | cl_normal_stop->peer_reply_sent) & ~peer_bits)
			   != 0
		|| (cl_state->ack_bitmap[0] & ~peer_bits) != 0
		|| !cl_phase1_bytes_zero(cl_state->ack_bitmap + 1, sizeof(cl_state->ack_bitmap) - 1)
		|| (pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesced_mask) & ~UINT32_C(255)) != 0
		|| (phase == CLUSTER_NORMAL_STOP_DRAIN && seal != 0)
		|| (phase == CLUSTER_NORMAL_STOP_QUIESCE && seal > 1)
		|| (phase == CLUSTER_NORMAL_STOP_WAIT_DRAIN_ACK && seal != 1)
		|| (phase >= CLUSTER_NORMAL_STOP_QUIESCE
			&& pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesce_requested) != 1))
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
	for (int peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++)
		if (cl_normal_stop->peer_request_nonce[peer] == 0
			|| cl_normal_stop->peer_request_nonce[peer] == UINT64_MAX
			|| (peer == cluster_node_id && cl_normal_stop->peer_request_nonce[peer] != nonce))
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_IDENTITY);
	if (((active | idle) & ~expected_services) != 0 || (active & idle) != 0)
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
	if (now >= deadline)
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_DEADLINE);
	return cluster_normal_stop_failure() == CLUSTER_NORMAL_STOP_FAILURE_NONE;
}

ClusterNormalStopPollResult
cluster_normal_stop_checkpoint_poll(uint32 expected_services, ClusterPhase1FullStopPlan *plan_out,
									ClusterNormalStopModuleObservation *observation)
{
	ClusterNormalStopModuleObservation ignored;
	ClusterPhase1FullStopPlan observed;
	ClusterNormalStopPollResult result = CLUSTER_NORMAL_STOP_INVALID;
	uint32 phase, peer_bits;
	uint64 now;
	if (plan_out != NULL)
		memset(plan_out, 0, sizeof(*plan_out));
	if (observation == NULL)
		observation = &ignored;
	memset(observation, 0, sizeof(*observation));
	observation->module = "COORDINATOR";
	observation->reason = "NORMAL_STOP_CHECKPOINT_CONTEXT_INVALID";
	if (!IsUnderPostmaster || !AmCheckpointerProcess() || plan_out == NULL || cl_state == NULL
		|| cl_normal_stop == NULL || !cluster_enabled || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT
		|| cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		return CLUSTER_NORMAL_STOP_INVALID;
	if (!cl_normal_stop_service_mask_valid(expected_services)
		|| expected_services != cl_normal_stop_config_service_mask()
		|| (cl_normal_stop_expected_services != 0
			&& cl_normal_stop_expected_services != expected_services)) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
		return CLUSTER_NORMAL_STOP_INVALID;
	}
	cl_normal_stop_expected_services = expected_services;
	phase = pg_atomic_read_u32(&cl_normal_stop->phase);
	if (phase <= CLUSTER_NORMAL_STOP_DRAIN) {
		result = cluster_normal_stop_fronts_poll(&observed, &observation->reason);
		if (result != CLUSTER_NORMAL_STOP_READY)
			return result;
		phase = CLUSTER_NORMAL_STOP_DRAIN;
	}
	now = (uint64)GetCurrentTimestamp();
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	result = cl_normal_stop_checkpoint_state_locked(expected_services, phase, now)
				 ? CLUSTER_NORMAL_STOP_READY
				 : CLUSTER_NORMAL_STOP_INVALID;
	LWLockRelease(&cl_state->lock);
	if (result != CLUSTER_NORMAL_STOP_READY)
		return result;
	result = cluster_normal_stop_modules_poll(false, observation);
	if (result != CLUSTER_NORMAL_STOP_READY)
		return result;
	if (phase == CLUSTER_NORMAL_STOP_DRAIN) {
		result = cluster_normal_stop_request_cleaner_quiesce();
		observation->module = "CLEANER";
		observation->reason = "NORMAL_STOP_AWAIT_EACH_OUTER_PASS_PARK";
		return result == CLUSTER_NORMAL_STOP_READY ? CLUSTER_NORMAL_STOP_PENDING : result;
	}
	if (!cluster_normal_stop_cleaners_are_parked()) {
		observation->module = "CLEANER";
		observation->reason = "NORMAL_STOP_AWAIT_EACH_OUTER_PASS_PARK";
		return CLUSTER_NORMAL_STOP_PENDING;
	}
	if (phase == CLUSTER_NORMAL_STOP_QUIESCE) {
		result = cluster_normal_stop_service_seal(expected_services, 1);
		if (result != CLUSTER_NORMAL_STOP_READY) {
			observation->module = "SERVICE";
			observation->reason = "NORMAL_STOP_AWAIT_OWNER_IDLE_CUT";
			return result;
		}
		/* Work admitted before seal 1 may have finished after the earlier
		 * census and left shared debt despite a now-idle actor. The sealed
		 * producer cut must precede the census that authorizes our ACK. */
		result = cluster_normal_stop_modules_poll(false, observation);
		if (result != CLUSTER_NORMAL_STOP_READY)
			return result;
	}
	/* A service may have entered after the module poll or seal 1. Recheck
	 * its original active/idle publication under the SAME leave lock that
	 * admits work, immediately before either phase transition. */
	result = cl_normal_stop_identity_poll(false, &observed, &observation->reason);
	if (result != CLUSTER_NORMAL_STOP_READY)
		return result;
	result = CLUSTER_NORMAL_STOP_PENDING;
	observation->module = "COORDINATOR";
	observation->reason = "NORMAL_STOP_AWAIT_DRAIN_ACK_OR_OWNER_IDLE";
	now = (uint64)GetCurrentTimestamp();
	peer_bits = UINT32_C(15) & ~(UINT32_C(1) << cluster_node_id);
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	if (!cl_normal_stop_checkpoint_state_locked(expected_services, phase, now))
		result = CLUSTER_NORMAL_STOP_INVALID;
	else if (pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesced_mask) == 255
			 && pg_atomic_read_u32(&cl_normal_stop->service_seal) == 1
			 && pg_atomic_read_u32(&cl_normal_stop->service_active_mask) == 0
			 && pg_atomic_read_u32(&cl_normal_stop->service_idle_mask) == expected_services) {
		if (phase == CLUSTER_NORMAL_STOP_QUIESCE)
			pg_atomic_write_u32(&cl_normal_stop->phase, CLUSTER_NORMAL_STOP_WAIT_DRAIN_ACK);
		else if (cl_state->ack_bitmap[0] == peer_bits
				 && cl_normal_stop->peer_reply_sent == peer_bits
				 && cl_normal_stop->peer_reply_pending == 0) {
			observed.valid = true;
			observed.attempt_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
			observed.absolute_deadline_us = cl_state->barrier_deadline_us;
			pg_atomic_write_u32(&cl_normal_stop->phase, CLUSTER_NORMAL_STOP_CHECKPOINT);
			*plan_out = observed;
			result = CLUSTER_NORMAL_STOP_READY;
			observation->reason = "NORMAL_STOP_SHUTDOWN_CHECKPOINT_ADMITTED_NOT_COMPLETE";
		}
	}
	LWLockRelease(&cl_state->lock);
	return result;
}

/* Read-only diagnostics, never permission to progress the shutdown. */
static bool
cl_normal_stop_wait_snapshot(char *out, Size out_size)
{
	uint32 phase, active, idle, parked, seal, seen, sent, reply_sent, reply_pending, ack;
	if (out == NULL || out_size == 0 || cl_state == NULL || cl_normal_stop == NULL)
		return false;
	LWLockAcquire(&cl_state->lock, LW_SHARED);
	phase = pg_atomic_read_u32(&cl_normal_stop->phase);
	active = pg_atomic_read_u32(&cl_normal_stop->service_active_mask);
	idle = pg_atomic_read_u32(&cl_normal_stop->service_idle_mask);
	parked = pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesced_mask);
	seal = pg_atomic_read_u32(&cl_normal_stop->service_seal);
	seen = cl_normal_stop->peer_requests_seen;
	sent = cl_normal_stop->peer_request_sent;
	reply_sent = cl_normal_stop->peer_reply_sent;
	reply_pending = cl_normal_stop->peer_reply_pending;
	ack = cl_state->ack_bitmap[0];
	LWLockRelease(&cl_state->lock);
	snprintf(out, out_size,
			 "phase=%u active=%u idle=%u expected=%u parked=%u seal=%u seen=%u sent=%u "
			 "reply_sent=%u reply_pending=%u ack=%u",
			 phase, active, idle, cl_normal_stop_expected_services, parked, seal, seen, sent,
			 reply_sent, reply_pending, ack);
	return true;
}

/* Wait on the original checkpointer latch and the attempt's existing absolute
 * deadline. LMON's original completed duty and each cleaner's park wake this
 * latch; no sleep timer, renewed budget or second shutdown driver is added. */
static bool
cl_normal_stop_checkpoint_run(ClusterPhase1FullStopPlan *plan,
							  ClusterNormalStopModuleObservation *observation,
							  bool after_checkpoint)
{
	ClusterNormalStopModuleObservation ignored;
	unsigned step = after_checkpoint ? 1 : 0;
	uint32 expected = cl_normal_stop_config_service_mask();
	uint64 last_diagnostic_us = 0;
	if (observation == NULL)
		observation = &ignored;
	memset(observation, 0, sizeof(*observation));
	observation->module = "COORDINATOR";
	observation->reason = "NORMAL_STOP_CHECKPOINTER_CONTEXT_INVALID";
	if (!IsUnderPostmaster || !AmCheckpointerProcess() || plan == NULL || cl_state == NULL
		|| cl_normal_stop == NULL || !cluster_enabled || !cluster_normal_stop_requested())
		return false;
	for (;;) {
		ClusterNormalStopPollResult result;
		uint64 deadline, now;
		long timeout_ms;
		/* Reset BEFORE observing, so a completion between poll and wait
		 * remains visible. READY is consumed once, never repolled as a new
		 * checkpoint or a new post-STOPPED nonce. */
		ResetLatch(MyLatch);
		if (step == 0)
			result = cluster_normal_stop_checkpoint_poll(expected, plan, observation);
		else if (step == 1)
			result = cluster_normal_stop_post_checkpoint_arm(plan, observation);
		else
			result = cluster_normal_stop_close_poll(plan, observation);
		if (result != CLUSTER_NORMAL_STOP_READY) {
			char snapshot[256];
			uint64 diagnostic_now = (uint64)GetCurrentTimestamp();
			/* Passive on the existing wake path: no new timer or retry. */
			if ((result == CLUSTER_NORMAL_STOP_INVALID
				 || cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE
				 || last_diagnostic_us == 0
				 || (diagnostic_now >= last_diagnostic_us
					 && diagnostic_now - last_diagnostic_us >= UINT64_C(1000000)))
				&& cl_normal_stop_wait_snapshot(snapshot, sizeof(snapshot))) {
				last_diagnostic_us = diagnostic_now;
				ereport(LOG, (errmsg("cluster normal-stop: pending cut observation"),
							  errdetail("step=%u result=%d module=%s reason=%s object=%s %s", step,
										(int)result, observation->module, observation->reason,
										observation->object, snapshot)));
			}
		}
		if (cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
			return false;
		if (result == CLUSTER_NORMAL_STOP_READY) {
			if (step != 1)
				return step == 0 || cluster_normal_stop_protocol_closed();
			step = 2;
			continue;
		}
		if (result != CLUSTER_NORMAL_STOP_PENDING) {
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
			return false;
		}
		LWLockAcquire(&cl_state->lock, LW_SHARED);
		deadline = cl_state->barrier_deadline_us;
		LWLockRelease(&cl_state->lock);
		now = (uint64)GetCurrentTimestamp();
		if (deadline == 0 || deadline == UINT64_MAX || now == 0 || now >= deadline) {
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_DEADLINE);
			return false;
		}
		timeout_ms = (long)Min((deadline - now + UINT64_C(999)) / UINT64_C(1000), (uint64)INT_MAX);
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, timeout_ms,
						WAIT_EVENT_RECONFIG_BARRIER_WAIT);
		CHECK_FOR_INTERRUPTS();
	}
}

bool
cluster_normal_stop_checkpoint_prepare(ClusterPhase1FullStopPlan *plan,
									   ClusterNormalStopModuleObservation *observation)
{
	return cl_normal_stop_checkpoint_run(plan, observation, false);
}

bool
cluster_normal_stop_checkpoint_complete(ClusterPhase1FullStopPlan *plan,
										ClusterNormalStopModuleObservation *observation)
{
	return cl_normal_stop_checkpoint_run(plan, observation, true);
}

ClusterNormalStopPollResult
cluster_normal_stop_request_cleaner_quiesce(void)
{
	ClusterCtrcNormalStopObservation observation;
	ClusterNormalStopPollResult result;

	if (!IsUnderPostmaster || !AmCheckpointerProcess() || !cluster_normal_stop_requested())
		return CLUSTER_NORMAL_STOP_INVALID;
	if (!cl_normal_stop_service_mask_valid(cl_normal_stop_expected_services)) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
		return CLUSTER_NORMAL_STOP_INVALID;
	}
	/* The coordinator already checked the other DRAIN owners. Never take
	 * module locks while holding the leave lock, and never manufacture GC. */
	result = cluster_ctrc_normal_stop_poll(&observation);
	if (result != CLUSTER_NORMAL_STOP_READY) {
		if (result == CLUSTER_NORMAL_STOP_INVALID)
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_MODULE);
		return result;
	}
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	if (cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE
		|| pg_atomic_read_u32(&cl_normal_stop->phase) != CLUSTER_NORMAL_STOP_DRAIN
		|| pg_atomic_read_u32(&cl_normal_stop->frontends_gone) != 1
		|| pg_atomic_read_u32(&cl_normal_stop->identity_published) != 1
		|| pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesce_requested) != 0
		|| pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesced_mask) != 0) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
		result = CLUSTER_NORMAL_STOP_INVALID;
	} else if ((pg_atomic_read_u32(&cl_normal_stop->service_active_mask)
				| pg_atomic_read_u32(&cl_normal_stop->service_idle_mask))
			   & ~cl_normal_stop_expected_services) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
		result = CLUSTER_NORMAL_STOP_INVALID;
	} else if (pg_atomic_read_u32(&cl_normal_stop->service_active_mask) != 0
			   || pg_atomic_read_u32(&cl_normal_stop->service_idle_mask)
					  != cl_normal_stop_expected_services) {
		/* A private input is still owned by a service: shared tables being
		 * empty cannot park its eventual completion worker. */
		result = CLUSTER_NORMAL_STOP_PENDING;
	} else {
		pg_atomic_write_u32(&cl_normal_stop->phase, CLUSTER_NORMAL_STOP_QUIESCE);
		pg_atomic_write_u32(&cl_normal_stop->cleaner_quiesce_requested, 1);
	}
	LWLockRelease(&cl_state->lock);
	if (result == CLUSTER_NORMAL_STOP_READY)
		cluster_undo_cleaner_wakeup();
	return result;
}

bool
cluster_normal_stop_cleaner_park_requested(void)
{
	return cluster_normal_stop_requested()
		   && pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesce_requested) == 1;
}

bool
cluster_normal_stop_cleaner_park(void)
{
	int worker = ClusterUndoCleanerWorkerIdForType(MyAuxProcType);
	uint32 bit;
	bool parked = false;

	if (!IsUnderPostmaster || worker < 0 || !cluster_normal_stop_cleaner_park_requested())
		return false;
	if (!cluster_ctrc_cleaner_local_idle()) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_CLEANER);
		return false;
	}
	bit = UINT32_C(1) << worker;
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	if (cluster_normal_stop_failure() == CLUSTER_NORMAL_STOP_FAILURE_NONE
		&& pg_atomic_read_u32(&cl_normal_stop->phase) == CLUSTER_NORMAL_STOP_QUIESCE
		&& pg_atomic_read_u32(&cl_normal_stop->frontends_gone) == 1
		&& pg_atomic_read_u32(&cl_normal_stop->identity_published) == 1
		&& (pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesced_mask) & bit) == 0) {
		(void)pg_atomic_fetch_or_u32(&cl_normal_stop->cleaner_quiesced_mask, bit);
		parked = true;
	} else
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_CLEANER);
	LWLockRelease(&cl_state->lock);
	if (parked && ProcGlobal != NULL && ProcGlobal->checkpointerLatch != NULL)
		SetLatch(ProcGlobal->checkpointerLatch);
	return parked;
}

bool
cluster_normal_stop_cleaners_are_parked(void)
{
	return IsUnderPostmaster && AmCheckpointerProcess()
		   && cluster_normal_stop_cleaner_park_requested()
		   && cluster_normal_stop_failure() == CLUSTER_NORMAL_STOP_FAILURE_NONE
		   && pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesced_mask)
				  == (UINT32_C(1) << CLUSTER_UNDO_CLEANER_WORKER_TYPES) - 1;
}

bool
cluster_normal_stop_qvotec_complete(bool all_disks_cleared)
{
	uint32 expected = 0;

	if (!IsUnderPostmaster || !AmQvotecProcess() || !cluster_normal_stop_requested())
		return false;
	if (!all_disks_cleared || !cluster_normal_stop_protocol_closed()
		|| !pg_atomic_compare_exchange_u32(&cl_normal_stop->qvotec_clear_result, &expected, 1)) {
		pg_atomic_write_u32(&cl_normal_stop->qvotec_clear_result, 2);
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_QVOTEC);
		return false;
	}
	return true;
}

bool
cluster_normal_stop_qvotec_cleared(void)
{
	return cluster_normal_stop_protocol_closed()
		   && pg_atomic_read_u32(&cl_normal_stop->qvotec_clear_result) == 1;
}

static uint32
cl_normal_stop_actor_bit(void)
{
	if (!IsUnderPostmaster)
		return 0;
	if (AmLmonProcess())
		return UINT32_C(1);
	if (AmLmsProcess())
		return UINT32_C(1) << 1;
	if (AmLmsWorkerProcess())
		return UINT32_C(1) << (1 + ClusterLmsWorkerIdForType(MyAuxProcType));
	if (AmSinvalBcastProcess())
		return UINT32_C(1) << 9;
	if (AmLmdProcess() && cluster_lmd_enabled)
		return UINT32_C(1) << 10;
	return 0;
}

bool
cluster_normal_stop_service_enter(void)
{
	uint32 bit = cl_normal_stop_actor_bit();
	bool requested = cluster_normal_stop_requested();

	if (bit == 0 || cl_normal_stop_service_depth == UINT32_MAX
		|| (cl_normal_stop_service_depth != 0 && cl_normal_stop_service_bit != bit)) {
		if (requested)
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
		return false;
	}
	if (requested) {
		uint32 active;

		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		active = pg_atomic_read_u32(&cl_normal_stop->service_active_mask);
		if (cl_normal_stop_service_depth == 0 && (active & bit) != 0)
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
		if (cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE) {
			LWLockRelease(&cl_state->lock);
			return false;
		}
		pg_atomic_write_u32(&cl_normal_stop->service_idle_mask,
							pg_atomic_read_u32(&cl_normal_stop->service_idle_mask) & ~bit);
		pg_atomic_write_u32(&cl_normal_stop->service_active_mask, active | bit);
		LWLockRelease(&cl_state->lock);
	}
	cl_normal_stop_service_bit = bit;
	cl_normal_stop_service_depth++;
	return true;
}

bool
cluster_normal_stop_service_leave(bool completed)
{
	uint32 bit = cl_normal_stop_actor_bit();
	bool requested = cluster_normal_stop_requested();

	if (bit == 0 || cl_normal_stop_service_depth == 0 || cl_normal_stop_service_bit != bit) {
		if (requested)
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
		return false;
	}
	cl_normal_stop_service_depth--;
	if (requested) {
		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		if (!completed)
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
		if (cl_normal_stop_service_depth == 0) {
			pg_atomic_write_u32(&cl_normal_stop->service_active_mask,
								pg_atomic_read_u32(&cl_normal_stop->service_active_mask) & ~bit);
			pg_atomic_write_u32(&cl_normal_stop->service_idle_mask,
								pg_atomic_read_u32(&cl_normal_stop->service_idle_mask) & ~bit);
		}
		LWLockRelease(&cl_state->lock);
	}
	if (cl_normal_stop_service_depth == 0)
		cl_normal_stop_service_bit = 0;
	return completed
		   && (!requested || cluster_normal_stop_failure() == CLUSTER_NORMAL_STOP_FAILURE_NONE);
}

ClusterNormalStopPollResult
cluster_normal_stop_service_idle(ClusterNormalStopPollResult modules)
{
	uint32 bit = cl_normal_stop_actor_bit();
	ClusterNormalStopPollResult result = CLUSTER_NORMAL_STOP_PENDING;

	if (!cluster_normal_stop_requested())
		return CLUSTER_NORMAL_STOP_READY;
	if (bit == 0) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
		return CLUSTER_NORMAL_STOP_INVALID;
	}
	if (cl_normal_stop_service_depth != 0)
		return CLUSTER_NORMAL_STOP_PENDING;
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	if ((pg_atomic_read_u32(&cl_normal_stop->service_active_mask) & bit) != 0)
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
	if (modules != CLUSTER_NORMAL_STOP_READY && modules != CLUSTER_NORMAL_STOP_PENDING)
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_MODULE);
	pg_atomic_write_u32(&cl_normal_stop->service_idle_mask,
						pg_atomic_read_u32(&cl_normal_stop->service_idle_mask) & ~bit);
	if (cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		result = CLUSTER_NORMAL_STOP_INVALID;
	else if (pg_atomic_read_u32(&cl_normal_stop->identity_published) != 0
			 && pg_atomic_read_u32(&cl_normal_stop->phase) >= CLUSTER_NORMAL_STOP_DRAIN
			 && modules == CLUSTER_NORMAL_STOP_READY) {
		pg_atomic_write_u32(&cl_normal_stop->service_idle_mask,
							pg_atomic_read_u32(&cl_normal_stop->service_idle_mask) | bit);
		result = CLUSTER_NORMAL_STOP_READY;
	}
	LWLockRelease(&cl_state->lock);
	/* The existing LMON duty is the wake source even while a shared owner
	 * remains PENDING. Do not add a controller polling interval or wake
	 * LMON back from every checkpointer poll (which would create a spin). */
	if (AmLmonProcess() && ProcGlobal != NULL && ProcGlobal->checkpointerLatch != NULL)
		SetLatch(ProcGlobal->checkpointerLatch);
	return result;
}

ClusterNormalStopPollResult
cluster_normal_stop_service_seal(uint32 expected_services, uint32 next_seal)
{
	uint32 phase, seal, active, idle;
	ClusterNormalStopPollResult result = CLUSTER_NORMAL_STOP_PENDING;

	if (!IsUnderPostmaster || !AmCheckpointerProcess() || !cluster_normal_stop_requested())
		return CLUSTER_NORMAL_STOP_INVALID;
	/* LMON, a contiguous original LMS pool, SINVAL and configured LMD. Keep
	 * this same roster through the attempt, including a missing/dead worker. */
	if (!cl_normal_stop_service_mask_valid(expected_services)
		|| (next_seal != 1 && next_seal != 2)) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
		return CLUSTER_NORMAL_STOP_INVALID;
	}
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	phase = pg_atomic_read_u32(&cl_normal_stop->phase);
	seal = pg_atomic_read_u32(&cl_normal_stop->service_seal);
	active = pg_atomic_read_u32(&cl_normal_stop->service_active_mask);
	idle = pg_atomic_read_u32(&cl_normal_stop->service_idle_mask);
	if ((next_seal == 1 && phase != CLUSTER_NORMAL_STOP_QUIESCE)
		|| (next_seal == 2 && phase != CLUSTER_NORMAL_STOP_POST_STOPPED)
		|| (seal != next_seal - 1 && seal != next_seal))
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
	if (((active | idle) & ~expected_services) != 0)
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
	if (cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE)
		result = CLUSTER_NORMAL_STOP_INVALID;
	else if (pg_atomic_read_u32(&cl_normal_stop->frontends_gone) == 1
			 && pg_atomic_read_u32(&cl_normal_stop->identity_published) == 1
			 && cl_normal_stop->peer_requests_seen == 15
			 && pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesce_requested) == 1
			 && pg_atomic_read_u32(&cl_normal_stop->cleaner_quiesced_mask) == 255 && active == 0
			 && idle == expected_services) {
		pg_atomic_write_u32(&cl_normal_stop->service_seal, next_seal);
		result = CLUSTER_NORMAL_STOP_READY;
	}
	LWLockRelease(&cl_state->lock);
	return result;
}

bool
cluster_normal_stop_service_new_work(bool modifies_data)
{
	uint32 bit, seal;

	if (!cluster_normal_stop_requested())
		return true;
	bit = cl_normal_stop_actor_bit();
	if (bit == 0 || cl_normal_stop_service_depth == 0 || cl_normal_stop_service_bit != bit) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
		return false;
	}
	/* May be called under a module lock: never acquire the leave lock here.
	 * The actor bracket holds active until the complete outer work returns.
	 * If requested arrived during an unregistered ordinary outer pass, this
	 * actor has not yet published any idle; that missing bit prevents seal 1. */
	seal = pg_atomic_read_u32(&cl_normal_stop->service_seal);
	if (seal > 2) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
		return false;
	}
	if ((pg_atomic_read_u32(&cl_normal_stop->service_active_mask) & bit) == 0
		&& (seal != 0 || (pg_atomic_read_u32(&cl_normal_stop->service_idle_mask) & bit) != 0)) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
		return false;
	}
	if (seal == 2 || (seal == 1 && modifies_data)) {
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_LATE_UNSEALED_WORK);
		return false;
	}
	return cluster_normal_stop_failure() == CLUSTER_NORMAL_STOP_FAILURE_NONE;
}

bool
cluster_normal_stop_service_data_sealed(void)
{
	uint32 seal;
	if (!cluster_normal_stop_requested())
		return false;
	seal = pg_atomic_read_u32(&cl_normal_stop->service_seal);
	if (seal > 2)
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
	return seal != 0;
}

bool
cluster_normal_stop_service_control_sealed(void)
{
	uint32 seal;
	if (!cluster_normal_stop_requested())
		return false;
	seal = pg_atomic_read_u32(&cl_normal_stop->service_seal);
	if (seal > 2)
		cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_STATE);
	return seal >= 2;
}

void
cluster_clean_leave_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_clean_leave_region);
}


/* ============================================================
 * CL-I2 no-leftover proof + survivor readiness ACK (D4/D5/§3.2)
 * ============================================================ */

/*
 * cluster_clean_leave_verify_no_leftover -- CL-I2 acceptance gate.  After the
 * leaving node drained + the survivors dropped their refs + the leave epoch
 * committed, NO GRD holder/waiter/convert/master and NO PCM X/S/PI record may
 * still name the leaving node.  A leftover would be a cross-node double-grant
 * hazard (rule 8.A).  Read-only; safe to call from any node as an assertion /
 * acceptance helper.
 */
bool
cluster_clean_leave_verify_no_leftover(int32 leaving_node_id)
{
	if (!cluster_grd_clean_leave_verify_no_leftover(leaving_node_id))
		return false;
	if (!cluster_pcm_lock_clean_leave_verify_no_leftover(leaving_node_id))
		return false;
	return true;
}

/*
 * cluster_clean_leave_block_serve_gate_allows -- CL-I5 storage-fallback gate (S6).
 *
 *	Called from the GCS block-serve path (cluster_gcs_send_block_request_and_wait)
 *	right before it serves a block from the storage fallback.  The hazard: a
 *	leaving node remasters its shards to survivors (GES_DRAINING) BEFORE it flushes
 *	its dirty/X blocks to shared storage (GCS_FLUSHING).  In that window a survivor
 *	request finds the new master holding no copy → storage fallback → reads the
 *	PRE-flush stale image (false-visible, 8.A).  This gate withholds the fallback
 *	until the leave commits (the leaving node is clean_departed → it flushed, and
 *	this survivor invalidated its cache at the epoch advance — see cl_survivor_tick).
 *
 *	Coarse v1 (CL-I4 conservative): while ANY uncommitted leave is in progress on
 *	this node, withhold ALL storage-fallbacks (over-approximate "leaving-node
 *	block"); per-block precision is a forward perf optimization.  The common
 *	no-leave path is a single unlocked read of leaving_node_id (cheap).  The
 *	leaving_node_id read is unlocked: the announce sets it BEFORE the leaving node
 *	remasters, so by the time a leaving-block storage-fallback is even possible the
 *	flag is already visible (a spurious set only over-withholds = still sound).
 */
bool
cluster_clean_leave_block_serve_gate_allows(void)
{
	int32 leaving;

	if (cl_state == NULL)
		return true; /* subsystem not attached → normal serve */

	leaving = cl_state->leaving_node_id;
	if (leaving < 0 || leaving == cluster_node_id)
		return true; /* no survivor-side leave in progress here */

	/* an uncommitted leave: withhold; once committed (flushed + invalidated)
	 * the node is clean_departed and the fallback is allowed (reads current). */
	if (cluster_clean_leave_serve_gate_allows(/*block_from_leaving*/ true,
											  cluster_reconfig_is_clean_departed(leaving)))
		return true;

	pg_atomic_fetch_add_u64(&cl_state->serve_gate_fail_closed_count, 1);
	return false;
}

/*
 * cluster_clean_leave_node_refuses_writes -- §3.1 "refuse new writes" gate.
 *
 *	Returns true when THIS node has an active clean leave in progress
 *	(REQUESTED..COMMITTED): from the moment it commits to leaving until it
 *	departs, it must accept NO new writable transaction.  The one-shot quiesce
 *	PROCSIG only aborts the backends that existed when it fired; a writable
 *	transaction that starts AFTER the GCS flush would dirty a block the leave
 *	already snapshotted-and-flushed, which the survivor could then read stale
 *	from storage (false-visible) or which would be lost if the node exits before
 *	a checkpoint (lost commit) — both 8.A.  Callers fail-close such writes with
 *	53R62 at xid assignment (AssignTransactionId) and again at the commit
 *	boundary (CommitTransaction), so nothing the leave did not flush can ever
 *	become durable.  Cheap on the hot path: the common non-leaving case is one
 *	unlocked int read.
 */
bool
cluster_clean_leave_node_refuses_writes(void)
{
	ClusterLeavePhase phase;

	if (cl_state == NULL || cl_state->leaving_node_id != cluster_node_id)
		return false;
	phase = (ClusterLeavePhase)pg_atomic_read_u32(&cl_state->phase);
	return phase >= CLUSTER_LEAVE_REQUESTED && phase <= CLUSTER_LEAVE_COMMITTED;
}

/*
 * cluster_clean_leave_in_progress -- is THIS node participating in any clean
 * leave right now (as the leaver OR as a survivor tracking someone else's
 * leave, OR mid-request before it has bound its slot)?  Hardening v1.0.4
 * (P1-1/P2): the spec-5.15 online-join driver consults this to enforce "one
 * membership reconfig at a time" — it must NOT start or commit a join (which
 * bumps the epoch with dead_gen unchanged, indistinguishable from a clean-leave
 * commit, CL barrier-observe hang) while a clean leave is active anywhere this
 * node can see it.  Cheap: a few unlocked atomic reads; the authoritative race
 * resolution is the re-check the join driver does under the reconfig lock at its
 * own commit point, plumbed symmetrically with cluster_reconfig_join_in_progress.
 */
bool
cluster_clean_leave_in_progress(void)
{
	if (cl_state == NULL)
		return false;
	if (pg_atomic_read_u32(&cl_state->request_in_progress) != 0)
		return true; /* mid-request: reserved before phase/leaving_node_id are set */
	if (cl_state->leaving_node_id != -1)
		return true; /* leaver or survivor tracking a leave */
	return pg_atomic_read_u32(&cl_state->phase) != CLUSTER_LEAVE_IDLE;
}

/*
 * cluster_clean_leave_survivor_ack -- a survivor signals readiness (§3.2 step 3,
 * F1 PRE-epoch).  Sent to the leaving node AFTER this survivor has dropped all
 * refs to it (the LMON tick survivor branch runs the drop, then calls this).
 * Carries no cache-invalidate and does not wait for the epoch — the epoch bump
 * happens only after every survivor acks, and the POST-epoch invalidate is
 * automatic (on_epoch_advance), so ACK and epoch do not form a cycle (R13/F1).
 */
void
cluster_clean_leave_survivor_ack(int32 leaving_node_id, uint64 leave_epoch)
{
	if (!cluster_enabled || cl_state == NULL)
		return;
	/* echo the tracked attempt's nonce so the leaver binds this ACK to it (P2). */
	cluster_clean_leave_ic_send_ack(leaving_node_id, leaving_node_id, leave_epoch,
									pg_atomic_read_u64(&cl_state->leave_attempt_nonce),
									/*nak*/ false, (uint8)CLUSTER_LEAVE_NAK_NONE);
}


/* ============================================================
 * observability — always 1 row (D13 backing)
 * ============================================================ */

void
cluster_clean_leave_get_state(ClusterLeaveState *out)
{
	if (cl_state == NULL) {
		memset(out, 0, sizeof(*out));
		out->leaving_node_id = -1;
		return;
	}
	LWLockAcquire(&cl_state->lock, LW_SHARED);
	*out = *cl_state;
	LWLockRelease(&cl_state->lock);
}


/* ============================================================
 * ProcSignal quiesce (D7) — three-step handler + ProcessInterrupts gate
 * ============================================================ */

/*
 * cluster_clean_leave_handle_quiesce_interrupt -- ProcSignal handler.
 *
 *	Async-signal-safe three-step (CL-I8 / L100/L118): set the pending flag,
 *	raise InterruptPending so ProcessInterrupts runs, and wake the latch.  The
 *	real decision (abort writable / absorb read-only) runs in
 *	cluster_clean_leave_check_pending_in_proc_interrupts() from normal backend
 *	context.  Missing InterruptPending would make the quiesce dead code → the
 *	leaving node could commit a write across the leave boundary.
 */
void
cluster_clean_leave_handle_quiesce_interrupt(void)
{
	cluster_clean_leave_quiesce_pending = true;
	InterruptPending = true;
	SetLatch(MyLatch);
}

/*
 * cluster_clean_leave_check_pending_in_proc_interrupts -- run from
 * ProcessInterrupts (normal backend context).  §2.2 / CL-I6 (writable-only):
 * abort only a writable transaction (one with a real top-level xid) with
 * 53R62; read-only / idle / post-commit absorb the quiesce silently.
 */
void
cluster_clean_leave_check_pending_in_proc_interrupts(void)
{
	if (!cluster_enabled)
		return; /* L20 first-line gate */
	if (cluster_clean_leave_quiesce_pending == 0)
		return;									 /* hot-path early return */
	cluster_clean_leave_quiesce_pending = false; /* read-clear FIRST */

	/*
	 * PG ProcessInterrupts already returned early when CritSectionCount > 0,
	 * so this is unreachable inside a critical section; commit-durable safety
	 * is naturally covered by the !IsTransactionState() / no-top-xid checks.
	 */
	if (!IsTransactionState())
		return; /* idle / post-commit absorb (CL-I6) */

	/* writable-only: read-only SELECT is in a transaction but has no top xid */
	if (!cluster_clean_leave_should_abort_writable(
			true, TransactionIdIsValid(GetTopTransactionIdIfAny())))
		return; /* read-only absorb */

	ereport(ERROR, (errcode(ERRCODE_CLUSTER_CLEAN_LEAVE_IN_PROGRESS),
					errmsg("transaction aborted: this node is leaving the cluster"),
					errhint("this node initiated a clean leave; in-flight writes were "
							"rolled back before departure; reconnect to a surviving node "
							"and retry — retry is safe")));
}


/* ============================================================
 * IC wire (D8) — CLEAN_LEAVE_ANNOUNCE / LEAVE_DRAIN_ACK / LEAVE_DRAIN_NAK
 *
 *	Sends are LMON-owned (single IC-owner invariant): the producer mask is
 *	CLUSTER_IC_PRODUCER_LMON, mirroring GES.  The announce handler runs in the
 *	survivor's LMON recv context, so it may reply (ACK/NAK) inline.  The
 *	leaving node's announce broadcast + the survivor's readiness ACK are driven
 *	from cluster_clean_leave_lmon_tick (D6); the helpers here are the wire.
 * ============================================================ */

/*
 * cl_announce_handler -- survivor side: consume a CLEAN_LEAVE_ANNOUNCE.
 *
 *	Membership-layer consume — NOT gated by clean_leave_enabled (§3.4): even a
 *	disabled survivor MUST reply LEAVE_DRAIN_NAK rather than go silent, else the
 *	leaving node waits for an ACK that never comes and false-times-out.
 */

/* spec-2.29a ②b: forward decls — the bind sites (below) snapshot the
 * others-dead set that the coherence gate (defined near drive_drain) compares. */
static void cl_others_dead_snapshot(int32 leaving, uint8 *out);
static bool cl_phase1_full_stop_all_peer_bits(const uint8 *bitmap);

static ClusterICSendResult
cl_phase1_full_stop_send_release_announce(int32 dest_node, uint8 wire_round, uint64 nonce,
										  uint64 epoch)
{
	ClusterLeaveAnnouncePayload p;

	Assert(wire_round == CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE
		   || wire_round == CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT);
	memset(&p, 0, sizeof(p));
	p.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	p.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	p.leaving_node_id = cluster_node_id;
	p.preflight = wire_round;
	p.producer_kind = CLUSTER_LEAVE_PRODUCER_SHUTDOWN;
	p.leave_epoch = epoch;
	p.cssd_dead_generation = 0;
	p.leave_nonce = nonce;
	cluster_clean_leave_announce_compute_crc(&p);
	return cluster_ic_send_envelope(PGRAC_IC_MSG_CLEAN_LEAVE_ANNOUNCE, dest_node, &p,
									(uint32)sizeof(p));
}

static ClusterICSendResult
cl_phase1_full_stop_send_release_reply(int32 dest_node, uint64 nonce, uint64 epoch)
{
	ClusterLeaveAckPayload p;

	memset(&p, 0, sizeof(p));
	p.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	p.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	p.survivor_node_id = cluster_node_id;
	p.leaving_node_id = dest_node;
	p.leave_epoch = epoch;
	p.leave_nonce = nonce;
	p.nak = 0;
	p.nak_reason = CLUSTER_LEAVE_NAK_NONE;
	p.phase1_round = CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE;
	cluster_clean_leave_ack_compute_crc(&p);
	return cluster_ic_send_envelope(PGRAC_IC_MSG_LEAVE_DRAIN_ACK, dest_node, &p, (uint32)sizeof(p));
}

static ClusterICSendResult
cl_phase1_full_stop_send_post_stopped_request(int32 dest_node, uint64 nonce, uint64 epoch)
{
	ClusterLeaveAnnouncePayload p;

	memset(&p, 0, sizeof(p));
	p.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	p.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	p.leaving_node_id = cluster_node_id;
	p.preflight = CLUSTER_PHASE1_FULL_STOP_WIRE_BARRIER;
	p.producer_kind = CLUSTER_LEAVE_PRODUCER_SHUTDOWN;
	p.leave_epoch = epoch;
	p.cssd_dead_generation = cluster_cssd_get_dead_generation();
	p.leave_nonce = nonce;
	cluster_clean_leave_announce_compute_crc(&p);
	return cluster_ic_send_envelope(PGRAC_IC_MSG_CLEAN_LEAVE_ANNOUNCE, dest_node, &p,
									(uint32)sizeof(p));
}

/* The post-STOPPED barrier reply uses the existing ACK shape.  Unlike the
 * generic inline ACK helper, its transport-admission result is retained by the
 * current phase-1 round so NOT_ADMITTED can be retried by LMON without
 * refreshing the round's absolute deadline. */
static ClusterICSendResult
cl_phase1_full_stop_send_post_stopped_reply(int32 dest_node, uint64 nonce, uint64 epoch)
{
	ClusterLeaveAckPayload p;

	memset(&p, 0, sizeof(p));
	p.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	p.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	p.survivor_node_id = cluster_node_id;
	p.leaving_node_id = dest_node;
	p.leave_epoch = epoch;
	p.leave_nonce = nonce;
	p.nak = 0;
	p.nak_reason = CLUSTER_LEAVE_NAK_NONE;
	p.phase1_round = 0;
	cluster_clean_leave_ack_compute_crc(&p);
	return cluster_ic_send_envelope(PGRAC_IC_MSG_LEAVE_DRAIN_ACK, dest_node, &p, (uint32)sizeof(p));
}

static void
cl_phase1_full_stop_release_announce_handler(const ClusterICEnvelope *env,
											 const ClusterLeaveAnnouncePayload *p)
{
	ClusterPhase1FullStopPlan current;
	uint64 stored_nonce;
	uint64 now_us;
	int32 source_node = (int32)env->source_node_id;
	bool source_active = false;
	bool source_stopped = false;
	bool phase_exact;
	bool exact;
	bool round_active;
	bool wake = false;

	exact = cl_phase1_full_stop_capture_identity(CLUSTER_WAL_SLOT_STATE_STOPPED, true, &current);
	phase_exact = exact
				  && cl_phase1_full_stop_capture_source_phase(&current, source_node, &source_active,
															  &source_stopped);
	(void)source_active;
	exact = phase_exact && source_stopped && p->leaving_node_id == source_node
			&& env->epoch == p->leave_epoch && p->leave_epoch == 0 && p->leave_nonce != 0
			&& p->leave_nonce != UINT64_MAX;

	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	current.valid = true;
	current.attempt_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	current.absolute_deadline_us = cl_state->barrier_deadline_us;
	now_us = (uint64)GetCurrentTimestamp();
	round_active = pg_atomic_read_u32(&cl_state->request_in_progress) != 0
				   && pg_atomic_read_u32(&cl_state->shutdown_driven) != 0;
	exact = exact && cluster_clean_leave_phase1_full_stop_plan_valid(&current) && round_active
			&& now_us < current.absolute_deadline_us;
	if (!exact) {
		if (round_active) {
			pg_atomic_write_u32(&cl_state->nak_received, 1);
			wake = true;
		}
		LWLockRelease(&cl_state->lock);
		goto out;
	}

	stored_nonce = cl_state->phase1_release_request_nonce[source_node];
	if (p->preflight == CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE) {
		exact = cluster_clean_leave_phase1_full_stop_release_probe_accepts(
			p->producer_kind, p->preflight, source_node, p->leaving_node_id, env->epoch,
			p->leave_epoch, p->leave_nonce, source_stopped, true, true);
		if (!exact || (stored_nonce != 0 && stored_nonce != p->leave_nonce)) {
			pg_atomic_write_u32(&cl_state->nak_received, 1);
			wake = true;
		} else {
			if (stored_nonce == 0)
				cl_state->phase1_release_request_nonce[source_node] = p->leave_nonce;
			if (!cl_phase1_member_bit_is_set(cl_state->phase1_release_request_seen, source_node)) {
				cl_phase1_member_bit_set(cl_state->phase1_release_request_seen, source_node);
				wake = true;
			}
		}
	} else if (p->preflight == CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT) {
		/* A receipt is accepted only for the exact reply already admitted for
		 * this peer's retained request.  The peer may consume and return it
		 * before this LMON publishes reply_sent after dispatch; final completion
		 * still requires that local bit.  A receipt is terminal and receives no
		 * reply. */
		if (!cluster_clean_leave_phase1_full_stop_receipt_accepts(
				p->producer_kind, p->preflight,
				pg_atomic_read_u32(&cl_state->phase1_release_pending) != 0, stored_nonce,
				p->leave_nonce,
				cl_phase1_member_bit_is_set(cl_state->phase1_release_request_seen, source_node))) {
			pg_atomic_write_u32(&cl_state->nak_received, 1);
			wake = true;
		} else if (!cl_phase1_member_bit_is_set(cl_state->phase1_release_receipt_seen,
												source_node)) {
			cl_phase1_member_bit_set(cl_state->phase1_release_receipt_seen, source_node);
			wake = true;
		}
	} else {
		pg_atomic_write_u32(&cl_state->nak_received, 1);
		wake = true;
	}
	LWLockRelease(&cl_state->lock);

out:
	if (wake && ProcGlobal->checkpointerLatch != NULL)
		SetLatch(ProcGlobal->checkpointerLatch);
}

static void
cl_announce_handler(const ClusterICEnvelope *env, const void *payload)
{
	const ClusterLeaveAnnouncePayload *p = (const ClusterLeaveAnnouncePayload *)payload;
	int32 leaving;

	if (!cluster_enabled || cl_state == NULL)
		return;
	if (!cluster_clean_leave_announce_payload_valid(p)) {
		ereport(DEBUG1,
				(errmsg_internal("cluster clean-leave: dropping invalid ANNOUNCE from node %d",
								 env->source_node_id)));
		return;
	}
	leaving = p->leaving_node_id;
	if (p->producer_kind == CLUSTER_LEAVE_PRODUCER_SHUTDOWN
		&& (p->preflight == CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE
			|| p->preflight == CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT)) {
		if (cl_normal_stop != NULL
			&& (pg_atomic_read_u32(&cl_normal_stop->identity_published) != 0
				|| !cluster_semantic_activation_phase1_pristine())) {
			cl_normal_stop_release_announce(env, p);
			return;
		}
		cl_phase1_full_stop_release_announce_handler(env, p);
		return;
	}

	/* User-approved phase-1 full-cluster clean stop: the existing SHUTDOWN
	 * preflight kind is an exact, side-effect-free barrier probe.  It must run
	 * before the ordinary single-leave busy gate because every participant owns
	 * its own simultaneous local request reservation. */
	if (p->producer_kind == CLUSTER_LEAVE_PRODUCER_SHUTDOWN
		&& p->preflight == CLUSTER_PHASE1_FULL_STOP_WIRE_BARRIER) {
		if (cl_normal_stop != NULL
			&& (pg_atomic_read_u32(&cl_normal_stop->identity_published) != 0
				|| !cluster_semantic_activation_phase1_pristine())) {
			cl_normal_stop_fronts_announce(env, p);
			return;
		}
	}
	if (p->producer_kind == CLUSTER_LEAVE_PRODUCER_SHUTDOWN
		&& p->preflight == CLUSTER_PHASE1_FULL_STOP_WIRE_BARRIER) {
		ClusterPhase1FullStopPlan current;
		uint32 local_wal_state = 0;
		bool source_active = false;
		bool source_stopped = false;
		bool local_post_stopped_receiver_ready = false;
		bool local_post_stopped_requests_sent = false;
		bool phase_exact = false;
		bool base_eligible;
		bool base_accept;
		bool accept;
		bool wake = false;

		base_eligible = cl_phase1_full_stop_capture_barrier_identity(&current, &local_wal_state);
		if (base_eligible)
			phase_exact = cl_phase1_full_stop_capture_source_phase(
				&current, (int32)env->source_node_id, &source_active, &source_stopped);
		base_eligible = base_eligible && phase_exact;
		base_accept = cluster_clean_leave_phase1_full_stop_probe_accepts(
			p->producer_kind, p->preflight != 0, (int32)env->source_node_id, leaving, env->epoch,
			p->leave_epoch, p->leave_nonce, cluster_lmon_reconfig_suppressed(), base_eligible);
		if (base_accept && source_stopped) {
			LWLockAcquire(&cl_state->lock, LW_SHARED);
			local_post_stopped_requests_sent
				= cl_phase1_post_stopped_request_round_nonce
					  == pg_atomic_read_u64(&cl_state->leave_attempt_nonce)
				  && cl_phase1_full_stop_all_peer_bits(cl_phase1_post_stopped_request_sent);
			local_post_stopped_receiver_ready
				= cluster_clean_leave_phase1_full_stop_post_stopped_receiver_ready(
					local_wal_state == CLUSTER_WAL_SLOT_STATE_STOPPED,
					pg_atomic_read_u32(&cl_state->request_in_progress) != 0,
					pg_atomic_read_u32(&cl_state->shutdown_driven) != 0,
					pg_atomic_read_u32(&cl_state->preflight_pending) != 0,
					local_post_stopped_requests_sent,
					pg_atomic_read_u32(&cl_state->phase1_release_pending) != 0);
			LWLockRelease(&cl_state->lock);
		}
		/* DONE/WOULD_BLOCK transferred semantic ownership of this exact frame
		 * to the receiver.  If the peer is one lifecycle stage ahead, retain it
		 * in the bounded LMON-local slot; never discard it and hope for a resend
		 * after transport ownership has already moved. */
		if (base_accept && source_stopped && !local_post_stopped_receiver_ready) {
			ClusterPhase1FullStopProbeNonceDecision ahead_decision;

			LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
			ahead_decision = cl_phase1_full_stop_retain_request_ahead_locked(
				&current, local_wal_state, (int32)env->source_node_id, p->leave_nonce);
			if (ahead_decision == CLUSTER_PHASE1_PROBE_NONCE_CONFLICT) {
				pg_atomic_write_u32(&cl_state->nak_received, 1);
				wake = true;
			}
			LWLockRelease(&cl_state->lock);
			if (wake && ProcGlobal->checkpointerLatch != NULL)
				SetLatch(ProcGlobal->checkpointerLatch);
			return;
		}
		accept
			= base_accept
			  && cluster_clean_leave_phase1_full_stop_probe_phase_accepts(
				  source_active, source_stopped, local_wal_state == CLUSTER_WAL_SLOT_STATE_ACTIVE,
				  local_wal_state == CLUSTER_WAL_SLOT_STATE_STOPPED);
		if (accept && source_active) {
			int32 source_node = (int32)env->source_node_id;
			uint64 active_nonce;

			LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
			active_nonce = cl_phase1_active_request_nonce[source_node];
			if (active_nonce == 0)
				cl_phase1_active_request_nonce[source_node] = p->leave_nonce;
			else if (active_nonce != p->leave_nonce) {
				pg_atomic_write_u32(&cl_state->nak_received, 1);
				accept = false;
				wake = true;
			}
			LWLockRelease(&cl_state->lock);
		}
		if (accept && source_stopped && local_wal_state == CLUSTER_WAL_SLOT_STATE_STOPPED) {
			int32 source_node = (int32)env->source_node_id;
			uint64 active_nonce;
			uint64 local_nonce;
			uint64 deadline_us;
			uint64 stored_nonce;
			ClusterPhase1FullStopProbeNonceDecision nonce_decision;

			LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
			local_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
			deadline_us = cl_state->barrier_deadline_us;
			if (!cl_phase1_full_stop_consume_request_ahead_locked(
					&current, source_node, local_nonce, deadline_us,
					local_post_stopped_requests_sent)) {
				pg_atomic_write_u32(&cl_state->nak_received, 1);
				accept = false;
				wake = true;
				nonce_decision = CLUSTER_PHASE1_PROBE_NONCE_CONFLICT;
			} else {
				active_nonce = cl_phase1_active_request_nonce[source_node];
				stored_nonce = cl_state->phase1_release_request_nonce[source_node];
				nonce_decision = cluster_clean_leave_phase1_full_stop_probe_nonce_decide(
					active_nonce, stored_nonce, p->leave_nonce);
			}
			if (nonce_decision == CLUSTER_PHASE1_PROBE_NONCE_STALE_ACTIVE) {
				LWLockRelease(&cl_state->lock);
				return;
			}
			if (nonce_decision == CLUSTER_PHASE1_PROBE_NONCE_CONFLICT) {
				pg_atomic_write_u32(&cl_state->nak_received, 1);
				wake = true;
			} else {
				if (nonce_decision == CLUSTER_PHASE1_PROBE_NONCE_ACCEPT_STOPPED)
					cl_state->phase1_release_request_nonce[source_node] = p->leave_nonce;
				cl_phase1_member_bit_set(cl_state->phase1_post_stopped_reply_pending, source_node);
				wake = true;
			}
			LWLockRelease(&cl_state->lock);
		} else {
			cluster_clean_leave_ic_send_ack((int32)env->source_node_id, leaving, p->leave_epoch,
											p->leave_nonce, !accept,
											accept ? (uint8)CLUSTER_LEAVE_NAK_NONE
												   : (uint8)CLUSTER_LEAVE_NAK_LEAVE_IN_PROGRESS);
		}
		if (wake && ProcGlobal->checkpointerLatch != NULL)
			SetLatch(ProcGlobal->checkpointerLatch);
		return;
	}

	/* Disabled survivor: fail-closed reply NAK(disabled), never silent.  Echoes
	 * the announce nonce (P2) and is reachable on BOTH the preflight probe and the
	 * real announce — so a disabled survivor is caught at layer-1 preflight before
	 * any side effect (P1).  RF-ROOT P6: the shutdown-driven handoff (the
	 * STOP-01 clean-close mainline) is NOT an opt-in operator feature — a
	 * disabled survivor still performs the full membership-layer consume and
	 * ACKs, so the §3.4 mixed-mode NAK applies to the operator producer only. */
	if (!cluster_clean_leave_enabled && p->producer_kind != CLUSTER_LEAVE_PRODUCER_SHUTDOWN) {
		cluster_clean_leave_ic_send_ack(env->source_node_id, leaving, p->leave_epoch,
										p->leave_nonce, true, (uint8)CLUSTER_LEAVE_NAK_DISABLED);
		return;
	}

	/*
	 * Hardening v1.0.4 (P1-1 + P1-2 + P2) — decided BEFORE the preflight/real split
	 * so it gates BOTH; gating only the real announce is NOT enough (the leaver would
	 * pass preflight, broadcast the real announce, and OTHER survivors would accept +
	 * track it before this node's NAK arrived, then the leaver clean-aborts, leaving
	 * those survivors tracking an aborted leave).
	 *   - Non-MEMBER (INV-J8): silently ignore.  A JOINING/ABSENT node is not a
	 *     survivor, must not ACK/track/drop — and must NOT NAK either, because the
	 *     leaver's cl_all_survivors_acked already excludes it; a NAK would wrongly
	 *     abort the leave.
	 *   - MEMBER but busy (this node is itself mid-request to leave, or a membership
	 *     join is in flight): NAK LEAVE_IN_PROGRESS at preflight so the leaver rejects
	 *     before any survivor enters tracking (one membership reconfig at a time).
	 */
	if (!cluster_membership_is_member(cluster_node_id))
		return;
	if (pg_atomic_read_u32(&cl_state->request_in_progress) != 0
		|| cluster_reconfig_join_in_progress()) {
		cluster_clean_leave_ic_send_ack(env->source_node_id, leaving, p->leave_epoch,
										p->leave_nonce, true,
										(uint8)CLUSTER_LEAVE_NAK_LEAVE_IN_PROGRESS);
		return;
	}

	/* preflight probe (F6 layer-1): we are enabled → ACK (enablement only, NO state
	 * change, NO GRD/PCM cleanup).  This is the side-effect-free half of the
	 * two-layer mixed-mode defense — the leaver gathers these ACKs before it ever
	 * enters REQUESTED / broadcasts the real announce. */
	if (p->preflight) {
		/*
		 * Hardening v1.0.3 (P1 test seam): a :skip arm makes this survivor SILENT
		 * on the probe — neither ACK nor NAK — modelling a version-skewed / IC-
		 * dropping / slow survivor.  The leaver must then fail-closed
		 * (rejected:preflight_incomplete), not fail-open past the deadline.
		 */
		CLUSTER_INJECTION_POINT("cluster-clean-leave-survivor-suppress-preflight-ack");
		if (cluster_injection_should_skip("cluster-clean-leave-survivor-suppress-preflight-ack"))
			return;
		cluster_clean_leave_ic_send_ack(env->source_node_id, leaving, p->leave_epoch,
										p->leave_nonce, false, (uint8)CLUSTER_LEAVE_NAK_NONE);
		return;
	}

	/*
	 * Hardening v1.0.1 (P1-3, single-leave-at-a-time): the survivor state is a
	 * single slot (one leave tracked).  If we are ALREADY tracking a DIFFERENT,
	 * uncommitted leave, do NOT overwrite leaving_node_id — that would silently
	 * drop the first leave's serve-gate protection (its still-unflushed blocks
	 * could then be served stale from storage = false-visible, 8.A).  Reject the
	 * second leave with a NAK so its leaving node clean-aborts and retries later;
	 * the wire handler ENFORCES the invariant the struct comment only documented.
	 * A re-announce of the SAME leave is idempotent (fall through, keep baseline).
	 */
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	if (cl_state->leaving_node_id != -1 && cl_state->leaving_node_id != leaving) {
		LWLockRelease(&cl_state->lock);
		cluster_clean_leave_ic_send_ack(env->source_node_id, leaving, p->leave_epoch,
										p->leave_nonce, true,
										(uint8)CLUSTER_LEAVE_NAK_LEAVE_IN_PROGRESS);
		ereport(LOG,
				(errmsg("cluster clean-leave: node %d announced a leave while node %d's leave is "
						"in progress; NAK (single-leave-at-a-time)",
						leaving, cl_state->leaving_node_id)));
		return;
	}
	/*
	 * Hardening v1.0.4 (P1-1, TOCTOU re-check under the lock): the membership/busy
	 * gate before the preflight split was lock-free; if this node reserved its OWN
	 * request between that gate and here, NAK now rather than become a survivor
	 * (the requester's self-bind re-check is the symmetric guard).
	 */
	if (pg_atomic_read_u32(&cl_state->request_in_progress) != 0) {
		LWLockRelease(&cl_state->lock);
		cluster_clean_leave_ic_send_ack(env->source_node_id, leaving, p->leave_epoch,
										p->leave_nonce, true,
										(uint8)CLUSTER_LEAVE_NAK_LEAVE_IN_PROGRESS);
		return;
	}

	/*
	 * Real announce: enter leave-aware reconfig (record the leaving node + bound
	 * epoch; CL-I4 fail-closed-until-drained applies from here).  The readiness
	 * ACK is sent later from the LMON tick (D6) AFTER dropping refs — NOT here.
	 * Only capture state on the FIRST announce of this leave (idempotent re-
	 * announce keeps the original baseline dead_gen).
	 */
	if (cl_state->leaving_node_id != leaving) {
		cl_state->leaving_node_id = leaving;
		cl_state->leave_epoch = p->leave_epoch;
		/* Hardening v1.0.2 (P2): bind this survivor to the announced attempt's
		 * nonce so a stale ACK/READY/COMMITTED from a prior same-epoch attempt is
		 * dropped by the control-message handlers. */
		pg_atomic_write_u64(&cl_state->leave_attempt_nonce, p->leave_nonce);
		/*
		 * Hardening v1.0.1 (P1-2, CL-I3 commit-handoff coherence): snapshot THIS
		 * survivor's CSSD dead_generation when it starts tracking the leave.  The
		 * coordinator re-checks it at the commit point so a real death intruding
		 * between BARRIER_WAIT and the epoch bump (dead_gen bumped, but the death's
		 * fail-stop epoch not yet advanced — invisible to the epoch-only guard)
		 * fails the commit closed instead of committing on a stale membership view.
		 */
		cl_state->leave_baseline_dead_gen = cluster_cssd_get_dead_generation();
		/* spec-2.29a ②b: snapshot the others-dead set (excludes the leaving node)
		 * the coherence gate compares against. */
		cl_others_dead_snapshot(leaving, cl_state->leave_baseline_others_dead);
		pg_atomic_write_u32(&cl_state->survivor_acked, 0);
		pg_atomic_write_u32(&cl_state->commit_ready_received, 0);
		pg_atomic_write_u32(&cl_state->committed_marker_durable, 0);
	}
	LWLockRelease(&cl_state->lock);
}

/*
 * cl_commit_ready_handler -- coordinator side: the leaving node has drained and
 * every survivor acked, and asks us (the min-survivor coordinator, Q6-A) to bump
 * the leave epoch.  Kept light: just latch commit_ready_received; the heavy
 * two-phase commit runs in the lmon_tick coordinator branch.  Idempotent — a
 * re-sent LEAVE_COMMIT_READY after we already committed is a no-op there (the
 * node is clean_departed).
 */
static void
cl_commit_ready_handler(const ClusterICEnvelope *env, const void *payload)
{
	const ClusterLeaveAnnouncePayload *p = (const ClusterLeaveAnnouncePayload *)payload;

	if (!cluster_enabled || cl_state == NULL)
		return;
	if (!cluster_clean_leave_announce_payload_valid(p))
		return;
	/* must be about the leave we are tracking as a survivor. */
	if (cl_state->leaving_node_id != p->leaving_node_id)
		return;
	/* Hardening v1.0.2 (P2): bind to THIS attempt — drop a stale/delayed READY
	 * from a prior same-epoch attempt (nonce) or a different epoch. */
	if (p->leave_nonce != pg_atomic_read_u64(&cl_state->leave_attempt_nonce)
		|| p->leave_epoch != cl_state->leave_epoch)
		return;

	pg_atomic_write_u32(&cl_state->commit_ready_received, 1);
}

/*
 * cl_committed_handler -- leaving node side (Hardening v1.0.1, P1-V0.7 exit gate;
 * spec-2.29a r3 evidence latch): the survivor coordinator has made the COMMITTED
 * marker majority-durable and attests that the durable truth-source now exists.
 * This confirmation is the leaver's own-commit MARKER EVIDENCE: the barrier tick
 * latches on it (and only on it) and proceeds to COMMITTED/exit.  Identity is
 * checked by the pure evidence gate (self-addressed + currently leaving +
 * per-attempt nonce + committed epoch past the bound baseline) — any mismatch is
 * dropped fail-closed.  Idempotent (the coordinator re-sends each tick until we
 * are gone).
 */
static void
cl_committed_handler(const ClusterICEnvelope *env, const void *payload)
{
	const ClusterLeaveAnnouncePayload *p = (const ClusterLeaveAnnouncePayload *)payload;

	(void)env;
	if (!cluster_enabled || cl_state == NULL)
		return;
	if (!cluster_clean_leave_announce_payload_valid(p))
		return;
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	if (!cluster_clean_leave_committed_evidence_matches(
			p->leaving_node_id, p->leave_nonce, p->leave_epoch, cluster_node_id,
			cl_state->leaving_node_id, pg_atomic_read_u64(&cl_state->leave_attempt_nonce),
			cl_state->leave_epoch)) {
		LWLockRelease(&cl_state->lock);
		return;
	}
	/* the committed epoch E is recorded BEFORE the evidence flag is published,
	 * inside the same critical section the barrier tick reads it back under. */
	cl_state->committed_confirmed_epoch = p->leave_epoch;
	pg_atomic_write_u32(&cl_state->committed_durable_confirmed, 1);
	LWLockRelease(&cl_state->lock);
}

/*
 * cl_send_committed -- coordinator -> leaving node: "COMMITTED marker majority-
 * durable; you may exit" (Hardening v1.0.1).  Point-to-point, re-sent each tick
 * while the leaver is alive (best-effort; the leaving node's gate is idempotent).
 */
static void
cl_send_committed(int32 leaving, uint64 leave_epoch)
{
	ClusterLeaveAnnouncePayload p;

	memset(&p, 0, sizeof(p));
	p.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	p.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	p.leaving_node_id = leaving;
	p.preflight = 0;
	p.leave_epoch = leave_epoch;
	p.cssd_dead_generation = cluster_cssd_get_dead_generation();
	p.leave_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce); /* P2: bind to attempt */
	cluster_clean_leave_announce_compute_crc(&p);

	(void)cluster_ic_send_envelope(PGRAC_IC_MSG_LEAVE_COMMITTED, leaving, &p, (uint32)sizeof(p));
}

/*
 * cl_ack_handler -- leaving node side: consume a LEAVE_DRAIN_ACK / _NAK.
 *	env->msg_type is the authoritative ACK-vs-NAK routing (envelope CRC-
 *	protected).  Only acted on if the message is about OUR leave.
 */
static void
cl_ack_handler(const ClusterICEnvelope *env, const void *payload)
{
	const ClusterLeaveAckPayload *p = (const ClusterLeaveAckPayload *)payload;
	bool is_nak;

	if (!cluster_enabled || cl_state == NULL)
		return;
	if (!cluster_clean_leave_ack_payload_valid(p))
		return;
	if (p->leaving_node_id != cluster_node_id)
		return; /* not about our leave */
	/* Hardening v1.0.2 (P2): bind to THIS attempt's nonce — a stale ACK/NAK from a
	 * prior preflight or drain (same baseline epoch) must not fill our tally.  The
	 * leaver sets leave_attempt_nonce before both the preflight and the real
	 * announce, so both layers' replies are checked against the current value. */
	if (p->leave_nonce != pg_atomic_read_u64(&cl_state->leave_attempt_nonce))
		return;
	if (cl_normal_stop != NULL && cluster_normal_stop_requested()
		&& pg_atomic_read_u32(&cl_normal_stop->identity_published) != 0) {
		if (p->phase1_round == CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE)
			cl_normal_stop_release_ack(env, p);
		else
			cl_normal_stop_fronts_ack(env, p);
		return;
	}

	is_nak = (env->msg_type == PGRAC_IC_MSG_LEAVE_DRAIN_NAK);
	if (p->phase1_round == CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE) {
		ClusterPhase1FullStopPlan current;
		bool exact;

		/* A release reply is the only nonzero ACK discriminator.  It is
		 * consumed only in the exact STOPPED release round; a matching receipt
		 * is then staged by LMON rather than emitted from this callback. */
		exact = !is_nak && env->msg_type == PGRAC_IC_MSG_LEAVE_DRAIN_ACK
				&& pg_atomic_read_u32(&cl_state->phase1_release_pending) != 0
				&& cl_phase1_full_stop_capture_identity(CLUSTER_WAL_SLOT_STATE_STOPPED, true,
														&current);
		if (exact) {
			current.valid = true;
			current.attempt_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
			current.absolute_deadline_us = cl_state->barrier_deadline_us;
			exact = env->epoch == p->leave_epoch && p->survivor_node_id >= 0
					&& p->survivor_node_id < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT
					&& cluster_clean_leave_phase1_full_stop_ack_matches(
						&current, cluster_node_id, (int32)env->source_node_id, p->survivor_node_id,
						p->leaving_node_id, p->leave_epoch, p->leave_nonce,
						current.member_incarnations[p->survivor_node_id]);
		}
		if (!exact)
			return;
		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		if (pg_atomic_read_u32(&cl_state->phase1_release_pending) != 0
			&& p->leave_nonce == pg_atomic_read_u64(&cl_state->leave_attempt_nonce))
			cl_phase1_member_bit_set(cl_state->phase1_release_reply_seen, p->survivor_node_id);
		LWLockRelease(&cl_state->lock);
		if (ProcGlobal->checkpointerLatch != NULL)
			SetLatch(ProcGlobal->checkpointerLatch);
		return;
	}

	if (pg_atomic_read_u32(&cl_state->shutdown_driven) != 0
		&& pg_atomic_read_u32(&cl_state->preflight_pending) != 0) {
		ClusterPhase1FullStopPlan current;
		bool exact;

		exact = cl_phase1_full_stop_capture_barrier_identity(&current, NULL);
		if (exact) {
			current.valid = true;
			current.attempt_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
			current.absolute_deadline_us = cl_state->barrier_deadline_us;
			exact = env->epoch == p->leave_epoch && p->survivor_node_id >= 0
					&& p->survivor_node_id < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT
					&& cluster_clean_leave_phase1_full_stop_ack_matches(
						&current, cluster_node_id, (int32)env->source_node_id, p->survivor_node_id,
						p->leaving_node_id, p->leave_epoch, p->leave_nonce,
						current.member_incarnations[p->survivor_node_id]);
		}
		if (!exact)
			return;
		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		if (p->leave_nonce == pg_atomic_read_u64(&cl_state->leave_attempt_nonce)) {
			if (is_nak) {
				pg_atomic_write_u32(&cl_state->nak_reason, (uint32)p->nak_reason);
				pg_atomic_write_u32(&cl_state->nak_received, 1);
			} else
				cl_state->ack_bitmap[p->survivor_node_id / 8]
					|= (uint8)(1u << (p->survivor_node_id % 8));
		}
		LWLockRelease(&cl_state->lock);
		if (ProcGlobal->checkpointerLatch != NULL)
			SetLatch(ProcGlobal->checkpointerLatch);
		return;
	}

	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	if (is_nak) {
		/* any NAK → clean ABORTED (driven by the driver/LMON tick).  Record the
		 * reason (Hardening v1.0.1 P2) so the request can map a DISABLED NAK to
		 * rejected:peers_not_all_enabled (F6 preflight) rather than bare ACCEPTED. */
		pg_atomic_write_u32(&cl_state->nak_reason, (uint32)p->nak_reason);
		pg_atomic_write_u32(&cl_state->nak_received, 1);
	} else if (p->survivor_node_id >= 0
			   && p->survivor_node_id < CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES * 8) {
		cl_state->ack_bitmap[p->survivor_node_id / 8] |= (uint8)(1u << (p->survivor_node_id % 8));
	}
	LWLockRelease(&cl_state->lock);
}

void
cluster_clean_leave_register_ic_msg_types(void)
{
	const ClusterICMsgTypeInfo announce_info = {
		.msg_type = PGRAC_IC_MSG_CLEAN_LEAVE_ANNOUNCE,
		.name = "clean_leave_announce",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
		.broadcast_ok = true, /* fanned out to all survivors */
		.handler = cl_announce_handler,
	};
	const ClusterICMsgTypeInfo ack_info = {
		.msg_type = PGRAC_IC_MSG_LEAVE_DRAIN_ACK,
		.name = "leave_drain_ack",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
		.broadcast_ok = false, /* point-to-point back to the leaving node */
		.handler = cl_ack_handler,
	};
	const ClusterICMsgTypeInfo nak_info = {
		.msg_type = PGRAC_IC_MSG_LEAVE_DRAIN_NAK,
		.name = "leave_drain_nak",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
		.broadcast_ok = false,
		.handler = cl_ack_handler, /* shared; branches on env->msg_type */
	};
	const ClusterICMsgTypeInfo commit_ready_info = {
		.msg_type = PGRAC_IC_MSG_LEAVE_COMMIT_READY,
		.name = "leave_commit_ready",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
		.broadcast_ok = false, /* point-to-point to the coordinator */
		.handler = cl_commit_ready_handler,
	};
	const ClusterICMsgTypeInfo committed_info = {
		.msg_type = PGRAC_IC_MSG_LEAVE_COMMITTED,
		.name = "leave_committed",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
		.broadcast_ok = false, /* point-to-point back to the leaving node */
		.handler = cl_committed_handler,
	};

	cluster_ic_register_msg_type(&announce_info);
	cluster_ic_register_msg_type(&ack_info);
	cluster_ic_register_msg_type(&nak_info);
	cluster_ic_register_msg_type(&commit_ready_info);
	cluster_ic_register_msg_type(&committed_info);
}

void
cluster_clean_leave_ic_broadcast_announce(uint64 leave_epoch, uint64 leave_nonce, bool preflight)
{
	ClusterLeaveAnnouncePayload p;
	ClusterICFanoutResult per_peer[CLUSTER_MAX_NODES];

	memset(&p, 0, sizeof(p));
	p.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	p.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	p.leaving_node_id = cluster_node_id;
	p.preflight = preflight ? 1 : 0;
	/* RF-ROOT P6: the shutdown-driven handoff announces as SHUTDOWN so the
	 * survivor skips the §3.4 disabled-NAK (the clean-close mainline is not
	 * an opt-in operator feature). */
	p.producer_kind = (pg_atomic_read_u32(&cl_state->shutdown_driven) != 0)
						  ? CLUSTER_LEAVE_PRODUCER_SHUTDOWN
						  : CLUSTER_LEAVE_PRODUCER_OPERATOR;
	p.leave_epoch = leave_epoch;
	p.cssd_dead_generation = cluster_cssd_get_dead_generation();
	p.leave_nonce = leave_nonce;
	cluster_clean_leave_announce_compute_crc(&p);

	cluster_ic_send_envelope_fanout(PGRAC_IC_MSG_CLEAN_LEAVE_ANNOUNCE, &p, (uint32)sizeof(p),
									per_peer);
}

void
cluster_clean_leave_ic_send_ack(int32 dest_node_id, int32 leaving_node_id, uint64 leave_epoch,
								uint64 leave_nonce, bool nak, uint8 nak_reason)
{
	ClusterLeaveAckPayload p;

	memset(&p, 0, sizeof(p));
	p.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	p.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	p.survivor_node_id = cluster_node_id;
	p.leaving_node_id = leaving_node_id;
	p.leave_epoch = leave_epoch;
	p.leave_nonce = leave_nonce;
	p.nak = nak ? 1 : 0;
	p.nak_reason = nak ? nak_reason : (uint8)CLUSTER_LEAVE_NAK_NONE;
	cluster_clean_leave_ack_compute_crc(&p);

	(void)cluster_ic_send_envelope(nak ? PGRAC_IC_MSG_LEAVE_DRAIN_NAK
									   : PGRAC_IC_MSG_LEAVE_DRAIN_ACK,
								   dest_node_id, &p, (uint32)sizeof(p));
}


/* ============================================================
 * Voting-disk leave-marker two-phase commit (§2.5) — qvotec-mediated.
 *
 *	The §2.5 marker is durable on the voting disk, but the voting disk is
 *	written ONLY by qvotec (the sole-writer invariant that protects the torn-
 *	write / generation / CRC protocol).  So the marker rides the same submit ->
 *	write-to-majority -> ack handshake as the spec-4.12 fence marker: the driver
 *	(REQUESTED) / the coordinator's LMON (COMMITTING/COMMITTED) stage a marker in
 *	this node's ClusterLeaveState, wake qvotec, and block (bounded) until qvotec
 *	has written it to THIS node's own leave-slot on a quorum-majority of disks.
 *	A marker reaches majority because qvotec replicates that one slot to every
 *	disk; cross-node visibility (the startup rebuild) reads every node's leave-
 *	slot across a majority of disks.
 * ============================================================ */

/* qvotec-side per-process handshake cursors (mirror the fence-marker ones). */
static uint64 cl_qvotec_last_processed_marker_seq = 0;
static uint64 cl_qvotec_inflight_marker_seq = 0;
static ClusterMarkerAsync cl_lmon_marker_async;
static ClusterLeaveIntentMarker cl_lmon_marker;
static int cl_lmon_marker_phase = 0;
static int32 cl_lmon_marker_leaving = -1;
static uint64 cl_lmon_marker_epoch = 0;
static bool cl_lmon_marker_submitted = false;

static void cl_build_marker(ClusterLeaveIntentMarker *m, uint8 marker_phase, int32 leaving,
							uint64 epoch);

typedef enum ClusterLeaveAsyncMarkerResult {
	CL_LEAVE_ASYNC_PENDING = 0,
	CL_LEAVE_ASYNC_ACKED,
	CL_LEAVE_ASYNC_FAILED
} ClusterLeaveAsyncMarkerResult;

static ClusterMarkerAsyncKind
cl_marker_phase_kind(int phase)
{
	if (phase == CLUSTER_LEAVE_MARKER_PHASE_COMMITTING)
		return CLUSTER_MARKER_KIND_CLEAN_LEAVE_COMMITTING;
	if (phase == CLUSTER_LEAVE_MARKER_PHASE_COMMITTED)
		return CLUSTER_MARKER_KIND_CLEAN_LEAVE_COMMITTED;
	return CLUSTER_MARKER_KIND_UNKNOWN;
}

static void
cl_release_lmon_marker_stage(void)
{
	cluster_marker_async_release_stage(&cl_lmon_marker_async);
	memset(&cl_lmon_marker, 0, sizeof(cl_lmon_marker));
	cl_lmon_marker_phase = 0;
	cl_lmon_marker_leaving = -1;
	cl_lmon_marker_epoch = 0;
	cl_lmon_marker_submitted = false;
}

static bool
cl_start_lmon_marker_stage(const ClusterLeaveIntentMarker *m, int phase, int32 leaving,
						   uint64 epoch)
{
	if (cl_lmon_marker_async.has_staged_event)
		return false;
	cl_lmon_marker = *m;
	cl_lmon_marker_phase = phase;
	cl_lmon_marker_leaving = leaving;
	cl_lmon_marker_epoch = epoch;
	cl_lmon_marker_async.has_staged_event = true;
	cl_lmon_marker_submitted = false;
	return true;
}

static ClusterLeaveAsyncMarkerResult
cl_poll_lmon_marker_stage(void)
{
	TimestampTz now;
	uint32 result = CLUSTER_LEAVE_MARKER_SUBMIT_FAILED;
	uint64 elapsed_us = 0;
	ClusterMarkerPollResult pr;
	ClusterMarkerAsyncKind kind;

	if (!cl_lmon_marker_async.has_staged_event)
		return CL_LEAVE_ASYNC_FAILED;

	now = GetCurrentTimestamp();
	kind = cl_marker_phase_kind(cl_lmon_marker_phase);
	if (!cl_lmon_marker_submitted) {
		if (!cluster_clean_leave_submit_marker_async(&cl_lmon_marker_async, &cl_lmon_marker, kind,
													 cl_lmon_marker_leaving, now))
			return CL_LEAVE_ASYNC_PENDING;
		cl_lmon_marker_submitted = true;
		return CL_LEAVE_ASYNC_PENDING;
	}

	pr = cluster_clean_leave_poll_marker_async(&cl_lmon_marker_async, now, &result, &elapsed_us);
	if (pr == CLUSTER_MARKER_POLL_PENDING || pr == CLUSTER_MARKER_POLL_IDLE)
		return CL_LEAVE_ASYNC_PENDING;
	if (pr == CLUSTER_MARKER_POLL_TIMEOUT) {
		cluster_reconfig_note_marker_timeout(kind, cl_lmon_marker_leaving, elapsed_us);
		cl_release_lmon_marker_stage();
		return CL_LEAVE_ASYNC_FAILED;
	}

	cluster_reconfig_note_marker_slow_ack(kind, cl_lmon_marker_leaving, elapsed_us);
	if (result != CLUSTER_LEAVE_MARKER_SUBMIT_ACK) {
		cl_release_lmon_marker_stage();
		return CL_LEAVE_ASYNC_FAILED;
	}
	return CL_LEAVE_ASYNC_ACKED;
}

static void
cl_drive_committed_marker_stage(int32 leaving, uint64 committed_epoch)
{
	ClusterLeaveIntentMarker cm;
	ClusterLeaveAsyncMarkerResult ar;

	if (cl_lmon_marker_async.has_staged_event) {
		if (cl_lmon_marker_phase != CLUSTER_LEAVE_MARKER_PHASE_COMMITTED
			|| cl_lmon_marker_leaving != leaving || cl_lmon_marker_epoch != committed_epoch)
			return;
	} else {
		cl_build_marker(&cm, CLUSTER_LEAVE_MARKER_PHASE_COMMITTED, leaving, committed_epoch);
		(void)cl_start_lmon_marker_stage(&cm, CLUSTER_LEAVE_MARKER_PHASE_COMMITTED, leaving,
										 committed_epoch);
	}

	ar = cl_poll_lmon_marker_stage();
	if (ar == CL_LEAVE_ASYNC_ACKED) {
		pg_atomic_write_u32(&cl_state->committed_marker_durable, 1);
		/* RF-ROOT P6 (post-commit survivor confirmation):  LEAVE_COMMITTED
		 * doubles as "this survivor confirmed the new generation and its
		 * shards are NORMAL again" (the serving rebind only re-confirms after
		 * the clean-leave GRD episode closed).  Hold the FIRST send until the
		 * rebind confirms; the step-2a resend loop delivers it the moment it
		 * does, so the leaver never departs into this survivor's freeze
		 * window. */
		if (cluster_serving_ready_is_current())
			cl_send_committed(leaving, committed_epoch);
		cl_release_lmon_marker_stage();
	} else if (ar == CL_LEAVE_ASYNC_FAILED) {
		ereport(LOG, (errmsg("cluster clean-leave: committed node %d at epoch %llu but the "
							 "COMMITTED marker is not yet majority-durable; retrying each tick "
							 "(leaving node waits)",
							 leaving, (unsigned long long)committed_epoch)));
	}
}

ClusterLeaveMarkerSubmitResult
cluster_clean_leave_submit_marker(const ClusterLeaveIntentMarker *m)
{
	uint64 seq;
	Latch *qlatch;
	uint64 deadline_us;
	int wait_ms;

	if (cl_state == NULL || m == NULL)
		return CLUSTER_LEAVE_MARKER_SUBMIT_FAILED;

	/* Stage the marker, then publish the request (write barrier between so
	 * qvotec never reads a half-written marker). */
	cl_state->pending_marker = *m;
	pg_write_barrier();
	seq = pg_atomic_add_fetch_u64(&cl_state->marker_request_seq, 1);

	/* latch-wake; a NULL latch (qvotec not running) → we time out below =
	 * fail-closed (the driver escalates rather than assuming durable). */
	qlatch = cl_state->qvotec_latch;
	if (qlatch != NULL)
		SetLatch(qlatch);

	/*
	 * Bounded synchronous wait for qvotec to complete THIS exact request.  A few
	 * poll cycles is enough for qvotec to pick it up + write + fdatasync; on
	 * timeout the driver fails closed (no false "durable").
	 */
	wait_ms = cluster_quorum_poll_interval_ms * 3 + 2000;
	deadline_us = (uint64)GetCurrentTimestamp() + (uint64)wait_ms * 1000ULL;
	for (;;) {
		if (pg_atomic_read_u64(&cl_state->marker_completion_seq) == seq) {
			pg_read_barrier();
			return (ClusterLeaveMarkerSubmitResult)pg_atomic_read_u32(&cl_state->marker_result);
		}
		if ((uint64)GetCurrentTimestamp() >= deadline_us)
			return CLUSTER_LEAVE_MARKER_SUBMIT_TIMEOUT;
		pg_usleep(2 * 1000); /* 2 ms */
	}
}

bool
cluster_clean_leave_submit_marker_async(ClusterMarkerAsync *a, const ClusterLeaveIntentMarker *m,
										ClusterMarkerAsyncKind kind, int32 target_node,
										TimestampTz now)
{
	int wait_ms;

	if (cl_state == NULL || m == NULL || a == NULL)
		return false;
	if (cluster_marker_async_is_submitted(a))
		return true;
	if (cluster_marker_async_mailbox_busy(&cl_state->marker_request_seq,
										  &cl_state->marker_completion_seq))
		return false;

	cl_state->pending_marker = *m;
	wait_ms = cluster_quorum_poll_interval_ms * 3 + 2000;
	return cluster_marker_async_submit(a, &cl_state->marker_request_seq,
									   &cl_state->marker_completion_seq, cl_state->qvotec_latch,
									   now, (uint64)wait_ms * 1000ULL, kind, target_node);
}

ClusterMarkerPollResult
cluster_clean_leave_poll_marker_async(ClusterMarkerAsync *a, TimestampTz now, uint32 *out_result,
									  uint64 *out_elapsed_us)
{
	if (cl_state == NULL || a == NULL)
		return CLUSTER_MARKER_POLL_IDLE;
	return cluster_marker_async_poll(a, &cl_state->marker_completion_seq, &cl_state->marker_result,
									 now, out_result, out_elapsed_us);
}

bool
cluster_clean_leave_qvotec_poll_pending(void *out_slot512)
{
	uint64 req;

	if (cl_state == NULL || out_slot512 == NULL)
		return false;

	req = pg_atomic_read_u64(&cl_state->marker_request_seq);
	if (req == cl_qvotec_last_processed_marker_seq)
		return false; /* nothing new */

	pg_read_barrier();
	/* Pack the staged marker into a 512-byte slot buffer (rest zeroed). */
	memset(out_slot512, 0, CLUSTER_VOTING_SLOT_BYTES);
	memcpy(out_slot512, &cl_state->pending_marker, sizeof(cl_state->pending_marker));
	cl_qvotec_inflight_marker_seq = req;
	return true;
}

void
cluster_clean_leave_qvotec_complete(bool acked)
{
	if (cl_state == NULL)
		return;

	pg_atomic_write_u32(&cl_state->marker_result, acked ? CLUSTER_LEAVE_MARKER_SUBMIT_ACK
														: CLUSTER_LEAVE_MARKER_SUBMIT_FAILED);
	pg_write_barrier();
	pg_atomic_write_u64(&cl_state->marker_completion_seq, cl_qvotec_inflight_marker_seq);
	cl_qvotec_last_processed_marker_seq = cl_qvotec_inflight_marker_seq;
	cluster_lmon_marker_complete_wakeup();
}

static void
cl_clear_qvotec_latch(int code, Datum arg)
{
	if (cl_state != NULL)
		cl_state->qvotec_latch = NULL;
}

void
cluster_clean_leave_publish_qvotec_latch(struct Latch *latch)
{
	if (cl_state == NULL)
		return;
	cl_state->qvotec_latch = latch;
	on_shmem_exit(cl_clear_qvotec_latch, (Datum)0);
}

/*
 * cluster_clean_leave_rebuild_from_disks -- startup recovery (P1-V0.7).
 *
 *	Scan every node's leave-slot across the voting disks.  A COMMITTED marker is
 *	written by the coordinator into ITS OWN leave-slot naming the DEPARTED node
 *	(not the slot index), so for each slot we read the marker's own
 *	leaving_node_id.  When a struct-valid COMMITTED marker for the same departed
 *	node (a declared peer) appears on a quorum-majority of disks, rebuild
 *	clean_departed + raise the epoch floor to its leave_epoch (the membership
 *	epoch is not durable, so the marker is the only proof the cluster reached it,
 *	§2.5).  A REQUESTED / COMMITTING marker is NOT a trust basis (skipped); a
 *	minority / torn / CRC-bad marker is ignored (fail-closed → that node's later
 *	CSSD DEAD escalates to fail-stop, a safe no-op since it already drained).
 */
void
cluster_clean_leave_rebuild_from_disks(const int *fds, int n_disks)
{
	int s;
	uint32 majority;

	if (cl_state == NULL || fds == NULL || n_disks <= 0)
		return;

	if (n_disks > CLUSTER_MAX_VOTING_DISKS)
		n_disks = CLUSTER_MAX_VOTING_DISKS; /* defensive clamp (fixed-array bound, no VLA) */
	majority = ((uint32)n_disks / 2u) + 1u;

	for (s = 0; s < CLUSTER_MAX_NODES; s++) {
		ClusterLeaveIntentMarker markers[CLUSTER_MAX_VOTING_DISKS];
		bool valid[CLUSTER_MAX_VOTING_DISKS];
		int n_read = 0;
		int d, e;

		/*
		 * Read this slot from every disk, keeping each disk's COMMITTED-basis marker
		 * for a declared peer.  Hardening v1.0.4 (finding 4): the majority must be of
		 * the SAME durable proof.  The earlier code counted agreement by
		 * leaving_node_id and took max(leave_epoch); a stale ghost leave-slot is NOT
		 * zeroed on rejoin (qvotec read-only ghost mitigation), so an OLD attempt's
		 * COMMITTED marker can persist on most disks while a NEWER attempt's COMMITTED
		 * marker reached only a minority — node-id counting then synthesized a
		 * "majority" and rebuilt at the newer epoch that itself never reached majority
		 * (trusting a non-majority durable fact, 8.A-adjacent).  Keep the markers and
		 * count by full identity below instead.
		 */
		for (d = 0; d < n_disks; d++) {
			union {
				uint8 bytes[CLUSTER_VOTING_SLOT_BYTES];
				uint64 _align;
			} slot;

			valid[d] = false;
			if (cluster_voting_disk_read_leave_slot(fds[d], (uint32)s, slot.bytes)
				!= CLUSTER_VOTING_DISK_IO_OK)
				continue;
			memcpy(&markers[d], slot.bytes, sizeof(markers[d]));
			/* COMMITTED + magic/version/CRC/dead_bitmap valid for its own
			 * leaving_node_id, which must be a declared peer. */
			if (!cluster_clean_leave_marker_is_committed_basis(&markers[d],
															   markers[d].leaving_node_id))
				continue;
			if (cluster_conf_lookup_node(markers[d].leaving_node_id) == NULL)
				continue;
			valid[d] = true;
			n_read++;
		}
		if (n_read == 0)
			continue;

		/*
		 * Find a marker IDENTITY {leaving_node_id, leave_epoch, event_id,
		 * cssd_dead_generation} present on a quorum-majority of disks — THAT is the
		 * durable proof.  At most one identity can hold a majority, so the first
		 * match is unique; rebuild at its own leave_epoch (never a different
		 * attempt's higher epoch).
		 */
		for (d = 0; d < n_disks; d++) {
			uint32 agree = 0;

			if (!valid[d])
				continue;
			for (e = 0; e < n_disks; e++) {
				if (valid[e] && markers[e].leaving_node_id == markers[d].leaving_node_id
					&& markers[e].leave_epoch == markers[d].leave_epoch
					&& markers[e].event_id == markers[d].event_id
					&& markers[e].cssd_dead_generation == markers[d].cssd_dead_generation)
					agree++;
			}
			if (agree >= majority) {
				cluster_reconfig_record_clean_departed(markers[d].leaving_node_id,
													   markers[d].leave_epoch,
													   /*raise_epoch_floor*/ true);
				ereport(LOG,
						(errmsg("cluster clean-leave: rebuilt clean-departed node %d at epoch %llu "
								"from %u/%d durable COMMITTED marker(s) of one identity",
								markers[d].leaving_node_id,
								(unsigned long long)markers[d].leave_epoch, agree, n_disks)));
				break; /* one identity per slot can reach majority */
			}
		}
	}
}


/* ============================================================
 * Leaving-node driver FSM + survivor/coordinator orchestration (D2/D6).
 *
 *	Execution model (user-confirmed, Option A): the leaving node self-drives the
 *	drain (Q6-A) in its OWN backend — the pg_cluster_clean_leave_request() entry
 *	runs REQUESTED -> QUIESCE -> GES drain -> GCS flush -> PCM release ->
 *	BARRIER_WAIT synchronously, so the force-WAL-log + FlushBuffer runs in a real
 *	backend (CL-I9, never in LMON), then returns.  The leaving-node LMON tick then
 *	collects survivor ACKs and sends LEAVE_COMMIT_READY to the survivor
 *	coordinator (min node id), which owns the epoch bump (Q6-A): the coordinator's
 *	LMON runs the §3.1 two-phase commit (COMMITTING marker -> bump+publish
 *	CLEAN_LEAVE -> COMMITTED marker).  The new epoch propagates back; the leaving
 *	node observes its CLEAN_LEAVE event and reaches COMMITTED.
 *
 *	NOTE (review): the readiness handoff (LEAVE_COMMIT_READY) is the impl
 *	mechanism for the spec's under-specified commit trigger; the spec pins the
 *	decision (leaving self-drives, survivor coordinator commits, flush-before-
 *	commit for CL-I5) but not the trigger wire.  Flagged for the mandated opus
 *	review (DoD).  CL-I12 touched drain-grace dispatch + the CL-I5 block-serve
 *	gate call-site are spec-5.13 S6.
 * ============================================================ */

/* atomic phase write with a debug-time legal-transition assert. */
static void
cl_set_phase(ClusterLeavePhase to)
{
	Assert(cluster_clean_leave_phase_valid_transition(
		(ClusterLeavePhase)pg_atomic_read_u32(&cl_state->phase), to));
	pg_atomic_write_u32(&cl_state->phase, to);
}

/* min MEMBER node that is alive (CSSD not DEAD) and != leaving; -1 if none. */
static int32
cl_compute_coordinator(int32 leaving)
{
	int32 i;

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == leaving)
			continue;
		/*
		 * Hardening v1.0.4 (P1-2, INV-J8): only a MEMBER may be elected clean-leave
		 * coordinator (it drives the two-phase commit + epoch bump).  A JOINING /
		 * ABSENT (but CSSD-alive) declared node must never be chosen — that would let
		 * a non-member bump the membership epoch, conflicting with safe-publication.
		 * Subsumes the old declared-peer check (a member is a declared peer).
		 */
		if (!cluster_membership_is_member(i))
			continue;
		if (i == cluster_node_id)
			return i; /* self is alive by definition */
		if (cluster_cssd_get_peer_state(i) != CLUSTER_CSSD_PEER_DEAD)
			return i;
	}
	return -1;
}

/* every alive MEMBER survivor (!= leaving) has set its ack bit? */
static bool
cl_all_survivors_acked(int32 leaving)
{
	int32 i;
	bool all = true;

	LWLockAcquire(&cl_state->lock, LW_SHARED);
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == leaving)
			continue;
		/*
		 * Hardening v1.0.4 (P1-2, INV-J8): only MEMBERs are expected to ACK a clean
		 * leave.  A JOINING / ABSENT (but CSSD-alive) node is NOT a survivor — it
		 * ignores the announce (mirror gate in the handler), so counting it here
		 * would wait forever for an ACK it must never send.  Subsumes the old
		 * declared-peer check (a member is a declared peer).
		 */
		if (!cluster_membership_is_member(i))
			continue;
		if (cluster_cssd_get_peer_state(i) == CLUSTER_CSSD_PEER_DEAD)
			continue; /* a dead survivor is not expected to ack (CL-I7 escalate handles it) */
		if (!(cl_state->ack_bitmap[i / 8] & (uint8)(1u << (i % 8)))) {
			all = false;
			break;
		}
	}
	LWLockRelease(&cl_state->lock);
	return all;
}

/* broadcast PROCSIG_CLUSTER_CLEAN_LEAVE_QUIESCE to local backends (not self —
 * the leaving session is driving the leave, not a victim).  Mirrors
 * cluster_reconfig_broadcast_local_procsig. */
static void
cl_broadcast_quiesce_local(void)
{
	int beid;
	pid_t self_pid = MyProcPid;

	for (beid = 1; beid <= MaxBackends; beid++) {
		PGPROC *proc = BackendIdGetProc((BackendId)beid);
		pid_t pid;

		if (proc == NULL)
			continue;
		pid = proc->pid;
		if (pid == 0 || pid == self_pid)
			continue;
		(void)SendProcSignal(pid, PROCSIG_CLUSTER_CLEAN_LEAVE_QUIESCE, (BackendId)beid);
	}
}

/* build a leave-intent marker for `leaving` at `epoch` in the given phase. */
static void
cl_build_marker(ClusterLeaveIntentMarker *m, uint8 marker_phase, int32 leaving, uint64 epoch)
{
	memset(m, 0, sizeof(*m));
	m->magic = CLUSTER_LEAVE_MARKER_MAGIC;
	m->version = CLUSTER_LEAVE_MARKER_VERSION;
	m->leaving_node_id = leaving;
	m->leave_epoch = epoch;
	if (leaving >= 0 && leaving < CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES * 8)
		m->dead_bitmap[leaving / 8] = (uint8)(1u << (leaving % 8));
	m->cssd_dead_generation = cluster_cssd_get_dead_generation();
	m->event_id = cluster_reconfig_compute_event_id(m->dead_bitmap, m->cssd_dead_generation);
	m->written_at = GetCurrentTimestamp();
	m->phase = marker_phase;
	cluster_clean_leave_marker_compute_crc(m);
}

/* send LEAVE_COMMIT_READY (reuse the announce payload, preflight=0) to the
 * coordinator: "I have drained + every survivor acked; bump the epoch". */
static void
cl_send_commit_ready(int32 coordinator, uint64 baseline_epoch)
{
	ClusterLeaveAnnouncePayload p;

	memset(&p, 0, sizeof(p));
	p.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	p.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	p.leaving_node_id = cluster_node_id;
	p.preflight = 0;
	p.leave_epoch = baseline_epoch;
	p.cssd_dead_generation = cluster_cssd_get_dead_generation();
	p.leave_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce); /* P2: bind to attempt */
	cluster_clean_leave_announce_compute_crc(&p);

	(void)cluster_ic_send_envelope(PGRAC_IC_MSG_LEAVE_COMMIT_READY, coordinator, &p,
								   (uint32)sizeof(p));
}

/* clean abort (mixed-mode NAK / preflight reject): clear the marker, revert to
 * IDLE — NO escalate, NO epoch bump, nothing was drained (only reachable from
 * REQUESTED, before any drain side effect). */
static void
cl_clean_abort(void)
{
	ClusterLeaveIntentMarker zero;

	cl_set_phase(CLUSTER_LEAVE_ABORTED);
	/* best-effort clear of the durable marker (magic=0 → no marker). */
	memset(&zero, 0, sizeof(zero));
	(void)cluster_clean_leave_submit_marker(&zero);

	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	cl_state->leaving_node_id = -1;
	cl_state->leave_epoch = 0;
	cl_state->barrier_deadline_us = 0;
	LWLockRelease(&cl_state->lock);
	cl_set_phase(CLUSTER_LEAVE_IDLE);
}

/* escalate (real death / deadline mid-drain): abandon the optimistic clean-leave
 * path; if the node truly dies the existing death-driven fail-stop (5.14) takes
 * over.  Records the failure and resets so the operator can retry. */
static void
cl_escalate(void)
{
	cl_set_phase(CLUSTER_LEAVE_ABORTED_ESCALATE);
	CLUSTER_INJECTION_POINT("cluster-clean-leave-escalate-to-failstop");
	pg_atomic_fetch_add_u64(&cl_state->escalate_count, 1);
	ereport(LOG, (errmsg("cluster clean-leave: drain abandoned (version changed / deadline / NAK); "
						 "escalating to fail-stop fallback")));
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	cl_state->leaving_node_id = -1;
	cl_state->leave_epoch = 0;
	cl_state->barrier_deadline_us = 0;
	LWLockRelease(&cl_state->lock);
	cl_set_phase(CLUSTER_LEAVE_IDLE);
}

/*
 * cl_request_body -- the reserved core of a clean-leave request.  The public
 * entry cluster_clean_leave_request holds the request_in_progress reservation
 * around this (Hardening v1.0.3); the operator SQL UDF maps the result to text,
 * D13b.  Gates on in_quorum + a live peer + no leave already in progress, runs the
 * F6 preflight (fail-CLOSED, Hardening v1.0.3), then records intent (durable
 * REQUESTED marker) and drives the local drain synchronously (Option A: the flush
 * runs in THIS backend, CL-I9).
 *
 *	IC-send ownership (architectural): cluster_ic_send_envelope* is LMON-only
 *	(single-producer invariant), so this backend does NO IC send.  The
 *	CLEAN_LEAVE_ANNOUNCE is broadcast by the leaving-node LMON tick once it sees
 *	the REQUESTED state; the survivor ACK / NAK and the LEAVE_COMMIT_READY are
 *	likewise LMON-driven.  The F6 preflight (Hardening v1.0.2) probes every survivor
 *	with a side-effect-free preflight=true announce BEFORE any state change: a
 *	disabled survivor NAKs (rejected:peers_not_all_enabled), and a silent / version-
 *	skewed survivor that never ACKs makes the request fail-CLOSED
 *	(rejected:preflight_incomplete, Hardening v1.0.3) instead of falling open into
 *	the drain.  Only an escalate (real death / deadline mid-drain) returns ACCEPTED
 *	and leaves via fail-stop.
 */
static ClusterLeaveRequestResult
cl_request_body(void)
{
	uint64 baseline_epoch;
	int32 coordinator;
	ClusterLeaveIntentMarker m;

	if (pg_atomic_read_u32(&cl_state->phase) != CLUSTER_LEAVE_IDLE)
		return CLUSTER_LEAVE_REQ_REJECTED_IN_PROGRESS;
	/*
	 * Hardening v1.0.1 (P1-3, single-leave-at-a-time, request side): this node may
	 * already be tracking ANOTHER node's in-progress leave as a survivor (phase
	 * stays IDLE in that role, so the check above does not catch it).  Starting our
	 * own leave here would overwrite that tracking and drop the other leave's
	 * serve-gate protection (8.A).  Reject locally; the announce handler is the
	 * second enforcement point for the race where our announce races another's.
	 */
	if (cl_state->leaving_node_id != -1 && cl_state->leaving_node_id != cluster_node_id)
		return CLUSTER_LEAVE_REQ_REJECTED_IN_PROGRESS;
	/*
	 * Hardening v1.0.4 (P2 serialization): one membership reconfig at a time.  If a
	 * spec-5.15 online JOIN is in its pending window, do NOT start a clean leave —
	 * the join bumps the membership epoch with dead_gen unchanged, which the leaving
	 * node would mis-observe as its own commit and wedge in BARRIER_WAIT.  The join
	 * driver defers symmetrically while a clean leave is active.
	 */
	if (cluster_reconfig_join_in_progress())
		return CLUSTER_LEAVE_REQ_REJECTED_IN_PROGRESS;
	if (!cluster_qvotec_in_quorum())
		return CLUSTER_LEAVE_REQ_REJECTED_NOT_IN_QUORUM;
	coordinator = cl_compute_coordinator(cluster_node_id);
	if (coordinator < 0)
		return CLUSTER_LEAVE_REQ_NOOP_NO_PEER;

	CLUSTER_INJECTION_POINT("cluster-clean-leave-request");

	/*
	 * F6 layer-1 TRUE preflight (Hardening v1.0.2, P1): BEFORE entering REQUESTED
	 * or touching any survivor's state, probe every survivor's enablement with a
	 * side-effect-free preflight=true announce.  Survivors only ACK/NAK — NO state
	 * change, NO GRD/PCM cleanup (§3.4 two-layer defense); a disabled survivor
	 * NAKs.  We proceed only when every alive survivor preflight-ACKs, so a
	 * mixed-mode request triggers NO survivor side effect.  (v1.0.1 only made the
	 * real-announce NAK synchronous, but by then an enabled survivor had already
	 * dropped its GRD/PCM refs to the still-alive leaver — 8.A at >=3 nodes.)  IC
	 * sends are LMON-only, so this is a backend<->LMON handshake: stage the probe,
	 * the LMON broadcasts it, we wait for the replies, then proceed or reject.
	 */
	{
		uint64 nonce = (uint64)GetCurrentTimestamp();
		int i;

		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		/*
		 * Hardening v1.0.4 (P1-1): re-check leaving_node_id UNDER the lock.  The
		 * test above was unlocked; in between, an incoming real announce could have
		 * made this node a survivor of someone else's leave (leaving_node_id :=
		 * other).  Bail before we stamp our own nonce/bitmap over that tracking.
		 */
		if (cl_state->leaving_node_id != -1 && cl_state->leaving_node_id != cluster_node_id) {
			LWLockRelease(&cl_state->lock);
			return CLUSTER_LEAVE_REQ_REJECTED_IN_PROGRESS;
		}
		pg_atomic_write_u64(&cl_state->leave_attempt_nonce, nonce);
		memset(cl_state->ack_bitmap, 0, sizeof(cl_state->ack_bitmap));
		pg_atomic_write_u32(&cl_state->nak_received, 0);
		pg_atomic_write_u32(&cl_state->nak_reason, (uint32)CLUSTER_LEAVE_NAK_NONE);
		pg_atomic_write_u32(&cl_state->preflight_sent, 0);
		pg_atomic_write_u32(&cl_state->preflight_pending, 1);
		LWLockRelease(&cl_state->lock);

		for (i = 0; i < 500; i++) { /* up to ~5s — a few LMON ticks */
			if (pg_atomic_read_u32(&cl_state->nak_received))
				break;
			if (pg_atomic_read_u32(&cl_state->preflight_sent)
				&& cl_all_survivors_acked(cluster_node_id))
				break; /* probe out + every alive survivor is enabled */
			pg_usleep(10 * 1000);
		}
		pg_atomic_write_u32(&cl_state->preflight_pending, 0);

		if (pg_atomic_read_u32(&cl_state->nak_received)) {
			ClusterLeaveNakReason reason
				= (ClusterLeaveNakReason)pg_atomic_read_u32(&cl_state->nak_reason);

			/* fail-closed BEFORE any state / marker / survivor side effect. */
			if (reason == CLUSTER_LEAVE_NAK_NOT_IN_QUORUM)
				return CLUSTER_LEAVE_REQ_REJECTED_NOT_IN_QUORUM;
			if (reason == CLUSTER_LEAVE_NAK_LEAVE_IN_PROGRESS)
				return CLUSTER_LEAVE_REQ_REJECTED_IN_PROGRESS;
			return CLUSTER_LEAVE_REQ_REJECTED_PEERS_NOT_ENABLED;
		}

		/*
		 * Hardening v1.0.3 (P1, fail-CLOSED): the loop breaks early ONLY when every
		 * alive survivor preflight-ACKed.  Reaching here with no NAK but not-all-
		 * acked means the ~5s deadline expired with a survivor still silent (version
		 * skew drops the v2 frame / IC loss / slow).  v1.0.2 fell THROUGH to
		 * REQUESTED here — fail-OPEN: it then broadcast the real announce and an
		 * enabled-but-silent survivor would drop its GRD/PCM refs to a leaver whose
		 * readiness was never confirmed (8.A).  Fail closed instead: no marker, no
		 * state, no survivor side effect — the operator retries / diagnoses the
		 * silent peer.  Distinct from peers_not_all_enabled (a definite DISABLED
		 * NAK); incomplete = the handshake never finished.
		 */
		if (!cl_all_survivors_acked(cluster_node_id))
			return CLUSTER_LEAVE_REQ_REJECTED_PREFLIGHT_INCOMPLETE;
	}

	baseline_epoch = cluster_epoch_get_current();

	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	/*
	 * Hardening v1.0.4 (P1-1, final recheck): the ~5s preflight wait above ran
	 * with the lock released, so an incoming real announce could have made this
	 * node a survivor (leaving_node_id := other) during it.  Binding self here
	 * would silently drop that other leave's serve-gate protection (false-visible,
	 * 8.A).  Re-check under the lock and bail if so; the announce handler is the
	 * symmetric enforcement point (it NAKs while our request is in progress).
	 */
	if (cl_state->leaving_node_id != -1 && cl_state->leaving_node_id != cluster_node_id) {
		LWLockRelease(&cl_state->lock);
		return CLUSTER_LEAVE_REQ_REJECTED_IN_PROGRESS;
	}
	cl_state->leaving_node_id = cluster_node_id;
	cl_state->leave_epoch = baseline_epoch;
	cl_state->leave_baseline_dead_gen = cluster_cssd_get_dead_generation();
	/* spec-2.29a ②b: snapshot the others-dead set (excludes self, the leaver). */
	cl_others_dead_snapshot(cluster_node_id, cl_state->leave_baseline_others_dead);
	cl_state->barrier_deadline_us
		= (uint64)GetCurrentTimestamp() + (uint64)cluster_clean_leave_drain_timeout_ms * 1000ULL;
	/*
	 * Hardening v1.0.3 (P1, nonce pollution): the REAL announce gets a FRESH nonce,
	 * distinct from the preflight's.  The ack handler binds an ACK to the current
	 * leave_attempt_nonce only; with the preflight and real announce sharing one
	 * nonce (v1.0.2), a preflight ACK arriving AFTER the ack_bitmap memset below
	 * could re-set a survivor's bit and pollute the real readiness tally.  A fresh
	 * nonce makes any late preflight ACK/NAK fail the nonce gate and be dropped.
	 * GetCurrentTimestamp() is microseconds-monotonic and the preflight wait was
	 * seconds, so it already differs; the guard is belt-and-suspenders.
	 */
	{
		uint64 preflight_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
		uint64 real_nonce = (uint64)GetCurrentTimestamp();

		if (real_nonce == preflight_nonce)
			real_nonce++;
		pg_atomic_write_u64(&cl_state->leave_attempt_nonce, real_nonce);
	}
	memset(cl_state->ack_bitmap, 0, sizeof(cl_state->ack_bitmap));
	pg_atomic_write_u32(&cl_state->nak_received, 0);
	pg_atomic_write_u32(&cl_state->nak_reason, (uint32)CLUSTER_LEAVE_NAK_NONE);
	pg_atomic_write_u32(&cl_state->announce_sent, 0);	/* LMON broadcasts the announce */
	pg_atomic_write_u32(&cl_state->shutdown_driven, 0); /* operator producer */
	pg_atomic_write_u32(&cl_state->commit_point_observed, 0);
	pg_atomic_write_u32(&cl_state->committed_durable_confirmed, 0);
	pg_atomic_write_u32(&cl_state->committed_marker_durable, 0);
	cl_state->committed_confirmed_epoch = 0; /* spec-2.29a r3: per-attempt evidence */
	LWLockRelease(&cl_state->lock);
	cl_set_phase(CLUSTER_LEAVE_REQUESTED);

	/* REQUESTED marker must be majority-durable before we start the drain (no
	 * durable evidence → do not start; a mid-leave crash must be identifiable).
	 * The LMON broadcasts the announce concurrently while this marker write
	 * blocks, so a disabled survivor's NAK is already pending by the time
	 * drive_drain checks it. */
	cl_build_marker(&m, CLUSTER_LEAVE_MARKER_PHASE_REQUESTED, cluster_node_id, baseline_epoch);
	if (cluster_clean_leave_submit_marker(&m) != CLUSTER_LEAVE_MARKER_SUBMIT_ACK) {
		ereport(LOG, (errmsg("cluster clean-leave: REQUESTED marker did not reach a voting-disk "
							 "majority; aborting the leave before drain")));
		cl_clean_abort();
		return CLUSTER_LEAVE_REQ_REJECTED_NOT_DURABLE;
	}

	cluster_clean_leave_drive_drain();

	/*
	 * Hardening v1.0.1 (P2 / F6 preflight, D13b): drive_drain runs INLINE in this
	 * backend and clean-aborts BEFORE any destructive step on a survivor NAK
	 * (nothing drained), leaving nak_received set (cl_clean_abort ends in IDLE, so
	 * we key on nak_received, not the phase).  Surface that as the spec's
	 * synchronous reject code instead of the bare ACCEPTED: a DISABLED NAK
	 * (mixed-mode) → rejected:peers_not_all_enabled; a NOT_IN_QUORUM NAK →
	 * rejected:not_in_quorum.  An escalate (real death/deadline) sets no NAK and
	 * still returns ACCEPTED — the node leaves via fail-stop, honoring the intent.
	 */
	if (pg_atomic_read_u32(&cl_state->nak_received)) {
		ClusterLeaveNakReason reason
			= (ClusterLeaveNakReason)pg_atomic_read_u32(&cl_state->nak_reason);

		if (reason == CLUSTER_LEAVE_NAK_NOT_IN_QUORUM)
			return CLUSTER_LEAVE_REQ_REJECTED_NOT_IN_QUORUM;
		if (reason == CLUSTER_LEAVE_NAK_LEAVE_IN_PROGRESS)
			return CLUSTER_LEAVE_REQ_REJECTED_IN_PROGRESS; /* another leave is committing (P1-3) */
		/* DISABLED (or an unspecified refusal): a survivor is not enabled. */
		return CLUSTER_LEAVE_REQ_REJECTED_PEERS_NOT_ENABLED;
	}
	/*
	 * Hardening v1.0.3 (P1): drive_drain clean-aborts (no NAK) when its own ~5s
	 * announce-ACK wait expires with a survivor still silent.  cl_clean_abort lands
	 * back in IDLE and sets NO nak, so the bare return below would mis-report
	 * ACCEPTED.  It records CLUSTER_LEAVE_ABORT_PREFLIGHT_INCOMPLETE; map it to the
	 * same fail-closed reject the request-side preflight uses.
	 */
	if (pg_atomic_read_u32(&cl_state->abort_reason)
		== (uint32)CLUSTER_LEAVE_ABORT_PREFLIGHT_INCOMPLETE)
		return CLUSTER_LEAVE_REQ_REJECTED_PREFLIGHT_INCOMPLETE;
	return CLUSTER_LEAVE_REQ_ACCEPTED;
}

/*
 * cluster_clean_leave_request -- public C driver entry (the operator SQL UDF
 * pg_cluster_clean_leave_request maps the result to its text return, D13b).
 *
 *	Hardening v1.0.3 (P1, same-node serialization): the unlocked phase==IDLE test
 *	in cl_request_body cannot serialize two same-node callers — phase stays IDLE
 *	through the multi-second preflight window (REQUESTED is set only AFTER the
 *	preflight), so without a reservation BOTH could pass it, both set REQUESTED, and
 *	both run cluster_clean_leave_drive_drain (which gates only on phase==REQUESTED)
 *	= a double GES drain / double PCM-X release.  Reserve request_in_progress with a
 *	CAS held for the WHOLE request (entry..return, including the inline drain); a
 *	second caller whose CAS fails is rejected:leave_in_progress.  The reservation is
 *	released on every path, including an ereport(ERROR) escape (PG_CATCH +
 *	PG_RE_THROW) — a leaked flag would wedge the node (no future leave) until restart.
 */
ClusterLeaveRequestResult
cluster_clean_leave_request(void)
{
	ClusterLeaveRequestResult result;
	uint32 expected = 0;

	if (cl_state == NULL || !cluster_enabled || !cluster_clean_leave_enabled)
		return CLUSTER_LEAVE_REQ_REJECTED_DISABLED;
	if (!cluster_clean_leave_startup_serving_allows(cluster_authority_readiness_managed(),
													cluster_serving_ready_is_current()))
		return CLUSTER_LEAVE_REQ_REJECTED_NOT_SERVING;

	/* Reserve before any other work; a second concurrent caller is rejected. */
	if (!pg_atomic_compare_exchange_u32(&cl_state->request_in_progress, &expected, 1))
		return CLUSTER_LEAVE_REQ_REJECTED_IN_PROGRESS;

	/* Clear any prior attempt's abort reason so the post-drain mapping is fresh. */
	pg_atomic_write_u32(&cl_state->abort_reason, (uint32)CLUSTER_LEAVE_ABORT_NONE);

	PG_TRY();
	{
		result = cl_request_body();
	}
	PG_CATCH();
	{
		pg_atomic_write_u32(&cl_state->request_in_progress, 0);
		PG_RE_THROW();
	}
	PG_END_TRY();

	pg_atomic_write_u32(&cl_state->request_in_progress, 0);
	return result;
}

/*
 * User-approved phase-1 coordinated full-cluster clean stop.  This is a
 * pre-checkpoint, non-authorizing barrier over the existing SHUTDOWN preflight
 * frame.  It does not publish a leave marker, enter the leave FSM, drain any
 * holder, remaster, or rebind a survivor.
 */
ClusterPhase1FullStopPrepareResult
cluster_clean_leave_phase1_full_stop_prepare_exact(ClusterPhase1FullStopPlan *plan_out)
{
	ClusterPhase1FullStopPlan plan;
	uint64 now_us;
	uint64 timeout_us;
	uint64 prior_nonce;
	uint32 expected = 0;
	bool complete = false;

	if (plan_out == NULL)
		return CLUSTER_PHASE1_FULL_STOP_ATTEMPT_FAILED;
	memset(plan_out, 0, sizeof(*plan_out));
	if (!cl_phase1_full_stop_capture_identity(CLUSTER_WAL_SLOT_STATE_ACTIVE, true, &plan))
		return CLUSTER_PHASE1_FULL_STOP_NOT_APPLICABLE;
	now_us = (uint64)GetCurrentTimestamp();
	if (cluster_clean_leave_drain_timeout_ms <= 0
		|| (uint64)cluster_clean_leave_drain_timeout_ms > UINT64_MAX / UINT64_C(1000))
		return CLUSTER_PHASE1_FULL_STOP_ATTEMPT_FAILED;
	timeout_us = (uint64)cluster_clean_leave_drain_timeout_ms * UINT64_C(1000);
	if (now_us == 0 || now_us == UINT64_MAX || now_us > UINT64_MAX - timeout_us)
		return CLUSTER_PHASE1_FULL_STOP_ATTEMPT_FAILED;
	if (!pg_atomic_compare_exchange_u32(&cl_state->request_in_progress, &expected, 1))
		return CLUSTER_PHASE1_FULL_STOP_ATTEMPT_FAILED;

	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	if (cl_state->leaving_node_id != -1
		|| pg_atomic_read_u32(&cl_state->phase) != CLUSTER_LEAVE_IDLE) {
		LWLockRelease(&cl_state->lock);
		pg_atomic_write_u32(&cl_state->request_in_progress, 0);
		return CLUSTER_PHASE1_FULL_STOP_ATTEMPT_FAILED;
	}
	prior_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	plan.attempt_nonce = now_us;
	if (plan.attempt_nonce == prior_nonce) {
		if (plan.attempt_nonce == UINT64_MAX - 1) {
			LWLockRelease(&cl_state->lock);
			pg_atomic_write_u32(&cl_state->request_in_progress, 0);
			return CLUSTER_PHASE1_FULL_STOP_ATTEMPT_FAILED;
		}
		plan.attempt_nonce++;
	}
	plan.absolute_deadline_us = now_us + timeout_us;
	plan.valid = true;
	if (!cluster_clean_leave_phase1_full_stop_plan_valid(&plan)) {
		LWLockRelease(&cl_state->lock);
		pg_atomic_write_u32(&cl_state->request_in_progress, 0);
		return CLUSTER_PHASE1_FULL_STOP_ATTEMPT_FAILED;
	}
	pg_atomic_write_u64(&cl_state->leave_attempt_nonce, plan.attempt_nonce);
	cl_state->barrier_deadline_us = plan.absolute_deadline_us;
	memset(cl_state->ack_bitmap, 0, sizeof(cl_state->ack_bitmap));
	cl_phase1_full_stop_release_state_reset_locked();
	pg_atomic_write_u32(&cl_state->nak_received, 0);
	pg_atomic_write_u32(&cl_state->nak_reason, (uint32)CLUSTER_LEAVE_NAK_NONE);
	pg_atomic_write_u32(&cl_state->preflight_sent, 0);
	pg_atomic_write_u32(&cl_state->shutdown_driven, 1);
	pg_atomic_write_u32(&cl_state->preflight_pending, 1);
	LWLockRelease(&cl_state->lock);

	for (;;) {
		uint8 ack_bitmap[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];
		bool nak;
		long timeout_ms;

		ResetLatch(MyLatch);
		LWLockAcquire(&cl_state->lock, LW_SHARED);
		memcpy(ack_bitmap, cl_state->ack_bitmap, sizeof(ack_bitmap));
		nak = pg_atomic_read_u32(&cl_state->nak_received) != 0;
		LWLockRelease(&cl_state->lock);
		if (nak)
			break;
		if (cluster_clean_leave_phase1_full_stop_ack_complete(&plan, cluster_node_id, ack_bitmap,
															  sizeof(ack_bitmap))) {
			complete = cl_phase1_full_stop_identity_matches(&plan, CLUSTER_WAL_SLOT_STATE_ACTIVE);
			break;
		}
		now_us = (uint64)GetCurrentTimestamp();
		if (now_us >= plan.absolute_deadline_us)
			break;
		timeout_ms = (long)((plan.absolute_deadline_us - now_us + UINT64_C(999)) / UINT64_C(1000));
		if (timeout_ms > INT_MAX)
			timeout_ms = INT_MAX;
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, timeout_ms,
						WAIT_EVENT_RECONFIG_BARRIER_WAIT);
		CHECK_FOR_INTERRUPTS();
	}

	if (!complete) {
		cl_phase1_full_stop_release(&plan);
		return CLUSTER_PHASE1_FULL_STOP_ATTEMPT_FAILED;
	}
	pg_atomic_write_u32(&cl_state->preflight_pending, 0);
	memcpy(plan_out, &plan, sizeof(plan));
	return CLUSTER_PHASE1_FULL_STOP_READY;
}

static bool
cl_phase1_full_stop_post_stopped_barrier(ClusterPhase1FullStopPlan *plan)
{
	uint64 prior_nonce;
	uint64 next_nonce;
	uint64 now_us;
	bool complete = false;

	if (!cluster_clean_leave_phase1_full_stop_plan_valid(plan)
		|| !cl_phase1_full_stop_identity_matches(plan, CLUSTER_WAL_SLOT_STATE_STOPPED))
		return false;
	now_us = (uint64)GetCurrentTimestamp();
	if (now_us == 0 || now_us == UINT64_MAX || now_us >= plan->absolute_deadline_us)
		return false;
	prior_nonce = plan->attempt_nonce;
	next_nonce = now_us;
	if (!cluster_clean_leave_phase1_full_stop_nonce_fresh(prior_nonce, next_nonce)) {
		if (prior_nonce < UINT64_MAX - 1)
			next_nonce = prior_nonce + 1;
		else
			next_nonce = prior_nonce - 1;
	}
	if (!cluster_clean_leave_phase1_full_stop_nonce_fresh(prior_nonce, next_nonce))
		return false;

	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	if (pg_atomic_read_u32(&cl_state->request_in_progress) == 0
		|| pg_atomic_read_u32(&cl_state->shutdown_driven) == 0
		|| pg_atomic_read_u32(&cl_state->preflight_pending) != 0
		|| pg_atomic_read_u64(&cl_state->leave_attempt_nonce) != prior_nonce
		|| cl_state->barrier_deadline_us != plan->absolute_deadline_us) {
		LWLockRelease(&cl_state->lock);
		return false;
	}
	plan->attempt_nonce = next_nonce;
	pg_atomic_write_u64(&cl_state->leave_attempt_nonce, next_nonce);
	memset(cl_state->ack_bitmap, 0, sizeof(cl_state->ack_bitmap));
	pg_atomic_write_u32(&cl_state->nak_received, 0);
	pg_atomic_write_u32(&cl_state->nak_reason, (uint32)CLUSTER_LEAVE_NAK_NONE);
	/* The generic preflight fanout is ACTIVE-round-only.  STOPPED requests use
	 * the exact per-peer transport owner below, so a NOT_ADMITTED peer remains
	 * pending without replaying peers whose frames were already consumed. */
	pg_atomic_write_u32(&cl_state->preflight_sent, 1);
	pg_atomic_write_u32(&cl_state->preflight_pending, 1);
	LWLockRelease(&cl_state->lock);

	for (;;) {
		uint8 ack_bitmap[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];
		uint8 reply_sent[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];
		bool nak;
		long timeout_ms;

		ResetLatch(MyLatch);
		LWLockAcquire(&cl_state->lock, LW_SHARED);
		memcpy(ack_bitmap, cl_state->ack_bitmap, sizeof(ack_bitmap));
		memcpy(reply_sent, cl_state->phase1_post_stopped_reply_sent, sizeof(reply_sent));
		nak = pg_atomic_read_u32(&cl_state->nak_received) != 0;
		LWLockRelease(&cl_state->lock);
		if (nak)
			break;
		if (cluster_clean_leave_phase1_full_stop_ack_complete(plan, cluster_node_id, ack_bitmap,
															  sizeof(ack_bitmap))
			&& cluster_clean_leave_phase1_full_stop_ack_complete(plan, cluster_node_id, reply_sent,
																 sizeof(reply_sent))) {
			complete = cl_phase1_full_stop_identity_matches(plan, CLUSTER_WAL_SLOT_STATE_STOPPED);
			break;
		}
		now_us = (uint64)GetCurrentTimestamp();
		if (now_us >= plan->absolute_deadline_us)
			break;
		timeout_ms = (long)((plan->absolute_deadline_us - now_us + UINT64_C(999)) / UINT64_C(1000));
		if (timeout_ms > INT_MAX)
			timeout_ms = INT_MAX;
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, timeout_ms,
						WAIT_EVENT_RECONFIG_BARRIER_WAIT);
		CHECK_FOR_INTERRUPTS();
	}
	if (complete)
		pg_atomic_write_u32(&cl_state->preflight_pending, 0);
	else {
		uint8 diagnostic_ack[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];
		uint32 diagnostic_pending;
		uint32 diagnostic_sent;
		uint32 diagnostic_nak;

		LWLockAcquire(&cl_state->lock, LW_SHARED);
		memcpy(diagnostic_ack, cl_state->ack_bitmap, sizeof(diagnostic_ack));
		diagnostic_pending = pg_atomic_read_u32(&cl_state->preflight_pending);
		diagnostic_sent = pg_atomic_read_u32(&cl_state->preflight_sent);
		diagnostic_nak = pg_atomic_read_u32(&cl_state->nak_received);
		LWLockRelease(&cl_state->lock);
		ereport(LOG, (errmsg_internal("cluster clean-leave: phase-1 full-stop "
									  "failure-domain stage=post-STOPPED self=%d "
									  "ack0=0x%02x pending=%u sent=%u nak=%u "
									  "now=%llu deadline=%llu",
									  cluster_node_id, (unsigned int)diagnostic_ack[0],
									  diagnostic_pending, diagnostic_sent, diagnostic_nak,
									  (unsigned long long)now_us,
									  (unsigned long long)plan->absolute_deadline_us)));
	}
	return complete;
}

static bool
cl_phase1_full_stop_release_completion(ClusterPhase1FullStopPlan *plan)
{
	uint64 now_us;
	bool complete = false;
	int32 peer;

	if (!cluster_clean_leave_phase1_full_stop_plan_valid(plan)
		|| !cl_phase1_full_stop_identity_matches(plan, CLUSTER_WAL_SLOT_STATE_STOPPED))
		return false;
	now_us = (uint64)GetCurrentTimestamp();
	if (now_us >= plan->absolute_deadline_us)
		return false;

	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	if (pg_atomic_read_u32(&cl_state->request_in_progress) == 0
		|| pg_atomic_read_u32(&cl_state->shutdown_driven) == 0
		|| pg_atomic_read_u32(&cl_state->preflight_pending) != 0
		|| pg_atomic_read_u64(&cl_state->leave_attempt_nonce) != plan->attempt_nonce
		|| cl_state->barrier_deadline_us != plan->absolute_deadline_us
		|| pg_atomic_read_u32(&cl_state->nak_received) != 0) {
		LWLockRelease(&cl_state->lock);
		return false;
	}
	for (peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
		bool request_seen;
		uint64 request_nonce;

		request_seen = cl_phase1_member_bit_is_set(cl_state->phase1_release_request_seen, peer);
		request_nonce = cl_state->phase1_release_request_nonce[peer];
		if (peer == cluster_node_id) {
			if (request_seen || request_nonce != 0) {
				LWLockRelease(&cl_state->lock);
				return false;
			}
		} else if (request_seen && (request_nonce == 0 || request_nonce == UINT64_MAX)) {
			LWLockRelease(&cl_state->lock);
			return false;
		}
	}
	memset(cl_state->phase1_release_request_sent, 0, sizeof(cl_state->phase1_release_request_sent));
	memset(cl_state->phase1_release_reply_sent, 0, sizeof(cl_state->phase1_release_reply_sent));
	memset(cl_state->phase1_release_reply_seen, 0, sizeof(cl_state->phase1_release_reply_seen));
	memset(cl_state->phase1_release_receipt_sent, 0, sizeof(cl_state->phase1_release_receipt_sent));
	memset(cl_state->phase1_release_receipt_seen, 0, sizeof(cl_state->phase1_release_receipt_seen));
	pg_atomic_write_u32(&cl_state->phase1_release_transport_drained, 0);
	pg_atomic_write_u32(&cl_state->phase1_release_pending, 1);
	LWLockRelease(&cl_state->lock);
	cluster_lmon_wakeup();

	for (;;) {
		uint8 request_sent[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];
		uint8 request_seen[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];
		uint8 reply_sent[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];
		uint8 reply_seen[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];
		uint8 receipt_sent[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];
		uint8 receipt_seen[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];
		bool transport_drained;
		bool exact_state;
		bool nak;
		long timeout_ms;

		ResetLatch(MyLatch);
		LWLockAcquire(&cl_state->lock, LW_SHARED);
		memcpy(request_sent, cl_state->phase1_release_request_sent, sizeof(request_sent));
		memcpy(request_seen, cl_state->phase1_release_request_seen, sizeof(request_seen));
		memcpy(reply_sent, cl_state->phase1_release_reply_sent, sizeof(reply_sent));
		memcpy(reply_seen, cl_state->phase1_release_reply_seen, sizeof(reply_seen));
		memcpy(receipt_sent, cl_state->phase1_release_receipt_sent, sizeof(receipt_sent));
		memcpy(receipt_seen, cl_state->phase1_release_receipt_seen, sizeof(receipt_seen));
		transport_drained = pg_atomic_read_u32(&cl_state->phase1_release_transport_drained) != 0;
		nak = pg_atomic_read_u32(&cl_state->nak_received) != 0;
		exact_state = pg_atomic_read_u32(&cl_state->phase1_release_pending) != 0
					  && pg_atomic_read_u64(&cl_state->leave_attempt_nonce) == plan->attempt_nonce
					  && cl_state->barrier_deadline_us == plan->absolute_deadline_us;
		LWLockRelease(&cl_state->lock);
		if (nak || !exact_state)
			break;
		if (cluster_clean_leave_phase1_full_stop_release_complete(
				plan, cluster_node_id, request_sent, request_seen, reply_sent, reply_seen,
				receipt_sent, receipt_seen, sizeof(request_sent), transport_drained)) {
			complete = cl_phase1_full_stop_identity_matches(plan, CLUSTER_WAL_SLOT_STATE_STOPPED);
			break;
		}
		now_us = (uint64)GetCurrentTimestamp();
		if (now_us >= plan->absolute_deadline_us)
			break;
		timeout_ms = (long)((plan->absolute_deadline_us - now_us + UINT64_C(999)) / UINT64_C(1000));
		if (timeout_ms > INT_MAX)
			timeout_ms = INT_MAX;
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, timeout_ms,
						WAIT_EVENT_RECONFIG_BARRIER_WAIT);
		CHECK_FOR_INTERRUPTS();
	}
	if (!complete) {
		uint8 request_sent[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];
		uint8 request_seen[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];
		uint8 reply_sent[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];
		uint8 reply_seen[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];
		uint8 receipt_sent[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];
		uint8 receipt_seen[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];
		uint32 transport_drained;
		uint32 nak;

		LWLockAcquire(&cl_state->lock, LW_SHARED);
		memcpy(request_sent, cl_state->phase1_release_request_sent, sizeof(request_sent));
		memcpy(request_seen, cl_state->phase1_release_request_seen, sizeof(request_seen));
		memcpy(reply_sent, cl_state->phase1_release_reply_sent, sizeof(reply_sent));
		memcpy(reply_seen, cl_state->phase1_release_reply_seen, sizeof(reply_seen));
		memcpy(receipt_sent, cl_state->phase1_release_receipt_sent, sizeof(receipt_sent));
		memcpy(receipt_seen, cl_state->phase1_release_receipt_seen, sizeof(receipt_seen));
		transport_drained = pg_atomic_read_u32(&cl_state->phase1_release_transport_drained);
		nak = pg_atomic_read_u32(&cl_state->nak_received);
		LWLockRelease(&cl_state->lock);
		ereport(LOG, (errmsg_internal("cluster clean-leave: phase-1 full-stop "
									  "failure-domain stage=release-completion self=%d "
									  "request=%02x/%02x reply=%02x/%02x "
									  "receipt=%02x/%02x drained=%u nak=%u "
									  "now=%llu deadline=%llu",
									  cluster_node_id, (unsigned int)request_sent[0],
									  (unsigned int)request_seen[0], (unsigned int)reply_sent[0],
									  (unsigned int)reply_seen[0], (unsigned int)receipt_sent[0],
									  (unsigned int)receipt_seen[0], transport_drained, nak,
									  (unsigned long long)now_us,
									  (unsigned long long)plan->absolute_deadline_us)));
	}
	return complete;
}

bool
cluster_clean_leave_phase1_full_stop_close_exact(ClusterPhase1FullStopPlan *plan)
{
	bool exact_state;
	bool terminal = false;
	uint8 ack_bitmap[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];

	if (!cluster_clean_leave_phase1_full_stop_plan_valid(plan))
		return false;
	LWLockAcquire(&cl_state->lock, LW_SHARED);
	memcpy(ack_bitmap, cl_state->ack_bitmap, sizeof(ack_bitmap));
	exact_state = pg_atomic_read_u32(&cl_state->request_in_progress) != 0
				  && pg_atomic_read_u32(&cl_state->shutdown_driven) != 0
				  && pg_atomic_read_u64(&cl_state->leave_attempt_nonce) == plan->attempt_nonce
				  && cl_state->barrier_deadline_us == plan->absolute_deadline_us;
	LWLockRelease(&cl_state->lock);
	if (exact_state
		&& cluster_clean_leave_phase1_full_stop_ack_complete(plan, cluster_node_id, ack_bitmap,
															 sizeof(ack_bitmap))
		&& cl_phase1_full_stop_identity_matches(plan, CLUSTER_WAL_SLOT_STATE_STOPPED))
		terminal = cl_phase1_full_stop_post_stopped_barrier(plan)
				   && cl_phase1_full_stop_release_completion(plan);
	cl_phase1_full_stop_release(plan);
	memset(plan, 0, sizeof(*plan));
	return terminal;
}

/*
 * cluster_clean_leave_drive_drain -- the synchronous leaving-node phases, run in
 * the requesting backend (CL-I9: the GCS flush is here, never in LMON).  Advances
 * REQUESTED -> QUIESCING -> GES_DRAINING -> GCS_FLUSHING -> BARRIER_WAIT, then
 * returns; the leaving-node LMON drives BARRIER_WAIT -> COMMITTED.  Every step
 * re-checks version coherence (CL-I3: an external epoch bump = a real death
 * intruded -> escalate) and the mixed-mode NAK (clean abort).
 */

/*
 * spec-2.29a ②b: snapshot the CSSD dead set EXCLUDING the leaving node.  The
 * coherence gate compares this "others-dead" set rather than the scalar global
 * dead_generation, so the leaving node's own expected alive->DEAD transition
 * (it stops heart-beating once its drain finishes) never falsely escalates the
 * leave.  A third-party death still changes this set and escalates (CL-I3).
 */
static void
cl_others_dead_snapshot(int32 leaving, uint8 *out /* CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES */)
{
	int i;

	memset(out, 0, CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES);
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == leaving)
			continue; /* the leaving node's own DEAD is expected, not incoherence */
		if (i >= CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES * 8)
			break;
		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		if (cluster_cssd_get_peer_state(i) == CLUSTER_CSSD_PEER_DEAD)
			out[i / 8] |= (uint8)(1u << (i % 8));
	}
}

/*
 * spec-2.29a ②b: the coherence gate used by every clean-leave step.  Coherent
 * iff the epoch has not moved AND no THIRD-PARTY death changed the others-dead
 * set since the leave was bound.  cl_state->leaving_node_id names the node
 * whose own DEAD is excluded.
 */
static bool
cl_coherent(uint64 baseline_epoch)
{
	uint8 now_others_dead[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];

	cl_others_dead_snapshot(cl_state->leaving_node_id, now_others_dead);
	return cluster_clean_leave_version_coherent(
		baseline_epoch, cluster_epoch_get_current(), cl_state->leave_baseline_others_dead,
		now_others_dead, CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES);
}

void
cluster_clean_leave_drive_drain(void)
{
	uint64 baseline_epoch;
	uint32 moved;
	int i;

	if (cl_state == NULL || !cluster_enabled)
		return;
	if (!cluster_clean_leave_startup_serving_allows(cluster_authority_readiness_managed(),
													cluster_serving_ready_is_current()))
		return;
	if (pg_atomic_read_u32(&cl_state->phase) != CLUSTER_LEAVE_REQUESTED)
		return;

	baseline_epoch = cl_state->leave_epoch;

	/*
	 * F6 preflight (Hardening v1.0.1, P2): wait until the LMON has actually
	 * broadcast the announce AND every alive survivor has replied — either a NAK
	 * (a disabled / not-in-quorum survivor) or the readiness ACK from all of them
	 * — BEFORE any destructive drain step.  The announce is LMON-driven (IC sends
	 * are LMON-only), so its dispatch can lag a tick behind this backend; gating on
	 * announce_sent + the replies (not a fixed sleep) makes the mixed-mode reject
	 * deterministic, so the request can return the spec's synchronous
	 * rejected:peers_not_all_enabled (D13b).  Bounded (~5s, a few LMON ticks) so a
	 * silently-slow survivor falls through to the drain (a late NAK is still caught
	 * by the BARRIER_WAIT async abort); a NAK here is a clean abort, no drain.
	 */
	for (i = 0; i < 500; i++) { /* up to ~5s */
		if (pg_atomic_read_u32(&cl_state->nak_received))
			break;
		if (pg_atomic_read_u32(&cl_state->announce_sent) && cl_all_survivors_acked(cluster_node_id))
			break; /* announce out + all alive survivors ready; no refusal */
		pg_usleep(10 * 1000);
	}
	if (pg_atomic_read_u32(&cl_state->nak_received)) {
		cl_clean_abort();
		return;
	}
	/*
	 * Hardening v1.0.3 (P1, fail-CLOSED; mirrors the request-side preflight gate):
	 * the loop also exits on the ~5s deadline.  If it expired with no NAK but not
	 * every alive survivor ACKed, a survivor is silent — do NOT fall through into
	 * the destructive drain (quiesce / GES drain / GCS flush + PCM-X release) on an
	 * unconfirmed barrier (8.A: a survivor that never dropped its refs could serve
	 * a leaving-node block stale after the flush).  Record the reason so the
	 * request maps it to rejected:preflight_incomplete, then clean-abort (revert
	 * the REQUESTED marker -> IDLE).  NOT an escalate: a silent survivor is not a
	 * real death, so the fail-stop fallback must not fire.
	 */
	if (!cl_all_survivors_acked(cluster_node_id)) {
		pg_atomic_write_u32(&cl_state->abort_reason,
							(uint32)CLUSTER_LEAVE_ABORT_PREFLIGHT_INCOMPLETE);
		ereport(LOG, (errmsg("cluster clean-leave: not every alive survivor acknowledged the leave "
							 "before the deadline; clean-aborting (no drain, no commit)")));
		cl_clean_abort();
		return;
	}

	/*
	 * CL-I3 coherence gate, re-checked before every drain step.  Uses the
	 * dead_gen-aware helper, not an epoch-only compare: CSSD increments the
	 * others-dead set the moment it declares a THIRD-PARTY peer dead, which is
	 * STRICTLY BEFORE the reconfig coordinator bumps the membership epoch.
	 * Checking that set too lets us escalate in that earlier window instead of
	 * draining into a death that has not yet reached the epoch.  The leaving
	 * node's OWN DEAD is excluded (spec-2.29a ②b).  At commit the guarded CAS
	 * in apply_clean_leave_as_coordinator is the final authority.
	 */
#define CL_COHERENT() cl_coherent(baseline_epoch)

	/* REQUESTED -> QUIESCING: abort local writable backends (53R62). */
	if (!CL_COHERENT()) {
		cl_escalate();
		return;
	}
	CLUSTER_INJECTION_POINT("cluster-clean-leave-quiesce-pre");
	cl_broadcast_quiesce_local();
	cl_set_phase(CLUSTER_LEAVE_QUIESCING);

	/* QUIESCING -> GES_DRAINING: release GES grants + remaster shards. */
	if (pg_atomic_read_u32(&cl_state->nak_received)) {
		cl_clean_abort();
		return;
	}
	if (!CL_COHERENT()) {
		cl_escalate();
		return;
	}
	moved = cluster_grd_clean_leave_drain_self(cluster_node_id, baseline_epoch);
	pg_atomic_fetch_add_u64(&cl_state->shards_remastered, moved);
	pg_atomic_fetch_add_u64(&cl_state->ges_drained_count, 1);
	cl_set_phase(CLUSTER_LEAVE_GES_DRAINING);
	CLUSTER_INJECTION_POINT("cluster-clean-leave-ges-drained");

	/* GES_DRAINING -> GCS_FLUSHING: force-WAL-log + FlushBuffer dirty/X to shared
	 * storage (CL-I9 backend ctx) then release PCM X.  A writeback failure is
	 * fail-closed: the block keeps its PCM X, we escalate, and never claim flush. */
	if (!CL_COHERENT()) {
		cl_escalate();
		return;
	}
	PG_TRY();
	{
		uint32 flushed = cluster_gcs_block_clean_leave_flush_all_dirty();
		uint64 released;

		pg_atomic_fetch_add_u64(&cl_state->gcs_flushed_count, flushed);
		released = cluster_pcm_lock_clean_leave_release_all_self(baseline_epoch);
		(void)released;
	}
	PG_CATCH();
	{
		cl_set_phase(CLUSTER_LEAVE_ABORTED_ESCALATE);
		pg_atomic_fetch_add_u64(&cl_state->escalate_count, 1);
		ereport(LOG,
				(errmsg("cluster clean-leave: GCS flush failed; leave fail-closed (the "
						"unflushed blocks keep PCM X and follow the fail-stop recovery path)")));
		PG_RE_THROW();
	}
	PG_END_TRY();
	cl_set_phase(CLUSTER_LEAVE_GCS_FLUSHING);
	CLUSTER_INJECTION_POINT("cluster-clean-leave-gcs-flushed");

	/* GCS_FLUSHING -> BARRIER_WAIT: drain side is done; the LMON tick now collects
	 * survivor ACKs and drives the commit. */
	if (!CL_COHERENT()) {
		cl_escalate();
		return;
	}
	cl_set_phase(CLUSTER_LEAVE_BARRIER_WAIT);
#undef CL_COHERENT
}

/*
 * cluster_clean_leave_shutdown_drain -- RF-ROOT P6 (L5 clean-reopen mainline,
 * the STOP-01 shutdown-handoff wiring; contract in cluster_clean_leave.h).
 *
 *	Run by the CHECKPOINTER inside the clean-shutdown sequence (fast stop),
 *	AFTER the shutdown checkpoint and the STOPPED wal-state publish (STOP-01
 *	I7 / RF-ROOT P6 contract 1: STOPPED occurs only after the clean shutdown
 *	checkpoint and before coordination drain) and BEFORE THREAD_CLEAN_CLOSE.
 *	Reuses the frozen 5.13 cooperative remaster/holder handoff verbatim: bind
 *	self as the leaver (no operator preflight — the SHUTDOWN producer is not
 *	GUC-gated), durable REQUESTED marker, the ordinary drive_drain phases,
 *	then block here until the survivor coordinator's two-phase commit reaches
 *	COMMITTED (the survivor-confirmed new generation, with the COMMITTED
 *	marker majority-durable — the §2.5 P1-V0.7 exit gate) or the handoff
 *	fails closed.
 *
 *	The node must NOT claim a clean-close unless this returns true (a failed
 *	handoff means the survivors treat the departure as an ordinary death and
 *	run the existing fail-stop reconfiguration — no fake clean-leave, 8.B).
 */
bool
cluster_clean_leave_shutdown_drain(void)
{
	ClusterLeaveIntentMarker m;
	uint64 baseline_epoch;
	uint64 real_nonce;
	uint64 preflight_nonce;
	bool have_alive_peer = false;
	bool result = false;
	bool bound = false;
	uint32 expected = 0;
	int i;

	if (cl_state == NULL || !cluster_enabled)
		return false;
	if (!cluster_clean_leave_startup_serving_allows(cluster_authority_readiness_managed(),
													cluster_serving_ready_is_current()))
		return false;
	if (cl_state->leaving_node_id != -1
		|| pg_atomic_read_u32(&cl_state->phase) != CLUSTER_LEAVE_IDLE)
		return false;

	/* Vacuous when no alive MEMBER peer exists to hand off to (single-node
	 * boot, or the last member standing after a peer already clean-left and
	 * its membership is DEAD): there is nothing to remaster toward and the
	 * cluster is fully down after this node exits, so a clean-close is still
	 * sound without a leave reconfig. */
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == cluster_node_id)
			continue;
		if (!cluster_membership_is_member(i))
			continue;
		if (cluster_cssd_get_peer_state(i) != CLUSTER_CSSD_PEER_DEAD) {
			have_alive_peer = true;
			break;
		}
	}
	if (!have_alive_peer)
		return true;

	/* Reserve the whole attempt (same-node serialization, mirrors the
	 * operator entry). */
	if (!pg_atomic_compare_exchange_u32(&cl_state->request_in_progress, &expected, 1))
		return false;
	pg_atomic_write_u32(&cl_state->abort_reason, (uint32)CLUSTER_LEAVE_ABORT_NONE);

	PG_TRY();
	{
		baseline_epoch = cluster_epoch_get_current();

		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		if (cl_state->leaving_node_id == -1) {
			cl_state->leaving_node_id = cluster_node_id;
			cl_state->leave_epoch = baseline_epoch;
			cl_state->leave_baseline_dead_gen = cluster_cssd_get_dead_generation();
			cl_others_dead_snapshot(cluster_node_id, cl_state->leave_baseline_others_dead);
			cl_state->barrier_deadline_us
				= (uint64)GetCurrentTimestamp()
				  + (uint64)cluster_clean_leave_drain_timeout_ms * 1000ULL;
			preflight_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
			real_nonce = (uint64)GetCurrentTimestamp();
			if (real_nonce == preflight_nonce)
				real_nonce++;
			pg_atomic_write_u64(&cl_state->leave_attempt_nonce, real_nonce);
			memset(cl_state->ack_bitmap, 0, sizeof(cl_state->ack_bitmap));
			pg_atomic_write_u32(&cl_state->nak_received, 0);
			pg_atomic_write_u32(&cl_state->nak_reason, (uint32)CLUSTER_LEAVE_NAK_NONE);
			pg_atomic_write_u32(&cl_state->announce_sent, 0);
			pg_atomic_write_u32(&cl_state->shutdown_driven, 1);
			pg_atomic_write_u32(&cl_state->commit_point_observed, 0);
			pg_atomic_write_u32(&cl_state->committed_durable_confirmed, 0);
			pg_atomic_write_u32(&cl_state->committed_marker_durable, 0);
			cl_state->committed_confirmed_epoch = 0;
			bound = true;
		}
		LWLockRelease(&cl_state->lock);

		if (bound) {
			cl_set_phase(CLUSTER_LEAVE_REQUESTED);
			cl_build_marker(&m, CLUSTER_LEAVE_MARKER_PHASE_REQUESTED, cluster_node_id,
							baseline_epoch);
			if (cluster_clean_leave_submit_marker(&m) != CLUSTER_LEAVE_MARKER_SUBMIT_ACK) {
				ereport(LOG, (errmsg("cluster clean-leave: shutdown handoff REQUESTED marker "
									 "did not reach a voting-disk majority; failing closed")));
				cl_clean_abort();
				result = false;
			} else {
				cluster_clean_leave_drive_drain();
				/* Block until the LMON-driven commit latches (survivor
				 * coordinator two-phase commit + COMMITTED marker
				 * majority-durable) or the handoff fails closed. */
				for (;;) {
					ClusterLeavePhase phase
						= (ClusterLeavePhase)pg_atomic_read_u32(&cl_state->phase);

					if (phase == CLUSTER_LEAVE_COMMITTED) {
						result = true;
						break;
					}
					if (phase == CLUSTER_LEAVE_ABORTED || phase == CLUSTER_LEAVE_ABORTED_ESCALATE
						|| phase == CLUSTER_LEAVE_IDLE) {
						result = false;
						break;
					}
					if ((uint64)GetCurrentTimestamp() > cl_state->barrier_deadline_us) {
						ereport(LOG, (errmsg("cluster clean-leave: shutdown handoff did not "
											 "commit before the barrier deadline; failing "
											 "closed (the departure is handled as an "
											 "ordinary death)")));
						result = false;
						break;
					}
					(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 20,
									WAIT_EVENT_RECONFIG_BARRIER_WAIT);
					ResetLatch(MyLatch);
					CHECK_FOR_INTERRUPTS();
				}
			}
		}

		/*
		 * RF-ROOT P6 phase 2 (post-commit, P1-V0.7: a committed leave is never
		 * un-committed):  the clean-leave epoch advance moved this leaver's own
		 * formation, so its serving binding is stale until the ordinary LMON
		 * serving rebind re-confirms after the local GRD episode closes.  The
		 * THREAD_CLEAN_CLOSE publish that follows needs CF(S) through that
		 * binding, so wait for the rebind (bounded by the same barrier
		 * deadline).  Past the deadline the commit is still irrevocable —
		 * proceed with a warning rather than fake a failure (the survivors
		 * already hold the durable clean-departed evidence either way).
		 */
		if (result) {
			for (;;) {
				if (cluster_serving_ready_is_current())
					break;
				if ((uint64)GetCurrentTimestamp() > cl_state->barrier_deadline_us) {
					ereport(WARNING, (errmsg("cluster clean-leave: committed but the local "
											 "serving authority did not re-confirm before the "
											 "barrier deadline; proceeding with the shutdown "
											 "checkpoint")));
					break;
				}
				(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 20,
								WAIT_EVENT_RECONFIG_BARRIER_WAIT);
				ResetLatch(MyLatch);
				CHECK_FOR_INTERRUPTS();
			}
		}
	}
	PG_CATCH();
	{
		pg_atomic_write_u32(&cl_state->request_in_progress, 0);
		PG_RE_THROW();
	}
	PG_END_TRY();

	pg_atomic_write_u32(&cl_state->request_in_progress, 0);
	return result;
}

/* leaving-node LMON: BARRIER_WAIT -> (COMMIT_READY ->) observe CLEAN_LEAVE commit
 * -> COMMITTED, or abort/escalate. */
static void
cl_leaving_barrier_tick(void)
{
	uint64 baseline_epoch = cl_state->leave_epoch;
	int32 coordinator;
	uint8 now_others_dead[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];
	bool committed_evidence;
	bool others_dead_unchanged;
	bool dead_gen_unchanged;

	/*
	 * 1. observe the commit — EVIDENCE over inference (spec-2.29a r3).  The
	 * survivor coordinator runs the actual two-phase commit, publishes the
	 * CLEAN_LEAVE event into ITS OWN reconfig state, drives the COMMITTED marker
	 * to voting-disk majority-durability, and only then sends the nonce-bound
	 * LEAVE_COMMITTED (re-sent each tick while we are alive).  That confirmation
	 * — validated by the pure identity gate in cl_committed_handler — is the ONLY
	 * basis on which this node latches its own commit: latch <=> a valid
	 * COMMITTED marker for THIS leave attempt exists.
	 *
	 *	Two inference generations preceded this and each failed one way (see the
	 *	predicate comment in cluster_clean_leave_policy.c): "epoch advanced +
	 *	others-dead bitmap unchanged" could mis-latch a REFUSED leave after a
	 *	third-party false-DEAD rebound (r2 P2-1 wedge); adding the monotone scalar
	 *	dead_generation conjunct then false-escalated a healthy committed leave
	 *	whenever a transient third-party flap advanced the leaver's local
	 *	dead_generation during the leave window (nightly t/331 C1/C4).  Marker
	 *	evidence is immune to both: flaps cannot erase a durable marker, and a
	 *	refused leave never produces one — no latch, so the barrier deadline below
	 *	still bounds the wait (fail-closed escalation, unchanged semantics).
	 *
	 *	The coherence observations are still taken, but ONLY for the flap-noise
	 *	LOG at the latch (the predicate contract pins that they never affect the
	 *	verdict).  A real third-party death intruding mid-leave is refused on the
	 *	survivor side (cl_coherent pre-check + guarded CAS, CL-I3), so no evidence
	 *	arrives here and the deadline escalates this leaver — same outcome as the
	 *	old immediate escalate arm, now bounded by the barrier deadline instead.
	 *
	 *	Latching collapses the old two-step gate: the evidence IS the P1-V0.7
	 *	durable-truth-source confirmation, so the leave can no longer be
	 *	un-committed (deadline must not escalate past here, Hardening v1.0.1
	 *	P1-1) AND the node may exit (COMMITTED) in the same tick.
	 */
	committed_evidence = (pg_atomic_read_u32(&cl_state->committed_durable_confirmed) != 0);
	cl_others_dead_snapshot(cl_state->leaving_node_id, now_others_dead);
	others_dead_unchanged = (memcmp(now_others_dead, cl_state->leave_baseline_others_dead,
									CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES)
							 == 0);
	dead_gen_unchanged = (cluster_cssd_get_dead_generation() == cl_state->leave_baseline_dead_gen);
	if (cluster_clean_leave_own_commit_latched(committed_evidence, others_dead_unchanged,
											   dead_gen_unchanged)) {
		uint64 committed_epoch;

		pg_atomic_write_u32(&cl_state->commit_point_observed, 1);
		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		committed_epoch = cl_state->committed_confirmed_epoch; /* the committed epoch E */
		cl_state->leave_epoch = committed_epoch;
		LWLockRelease(&cl_state->lock);

		if (!others_dead_unchanged || !dead_gen_unchanged)
			ereport(LOG, (errmsg("cluster clean-leave: third-party liveness flap observed at the "
								 "commit latch (others-dead %s, dead_generation %s); durable "
								 "COMMITTED marker evidence overrides it",
								 others_dead_unchanged ? "unchanged" : "changed",
								 dead_gen_unchanged ? "unchanged" : "advanced")));

		cl_set_phase(CLUSTER_LEAVE_COMMITTED);
		CLUSTER_INJECTION_POINT("cluster-clean-leave-barrier-complete");
		ereport(LOG, (errmsg("cluster clean-leave: committed at epoch %llu + COMMITTED marker "
							 "majority-durable; this node has drained and may exit",
							 (unsigned long long)committed_epoch)));
		return;
	}

	/* 2. a survivor refused (mixed-mode) -> clean abort. */
	if (pg_atomic_read_u32(&cl_state->nak_received)) {
		cl_clean_abort();
		return;
	}

	/* 3. fail-closed deadline — ONLY before the commit point (P1-1: a committed
	 * leave is never un-committed).  With the r3 evidence latch this arm is what
	 * bounds EVERY no-evidence outcome: a refused leave, a foreign death that
	 * moved the version (the coordinator then refuses, CL-I3), or a lost/never-
	 * sent confirmation all end here instead of hanging in BARRIER_WAIT.  The
	 * commit_point_observed guard is belt-and-braces: the latch above transitions
	 * out of BARRIER_WAIT in the same tick it sets the flag. */
	if (!pg_atomic_read_u32(&cl_state->commit_point_observed)
		&& (uint64)GetCurrentTimestamp() > cl_state->barrier_deadline_us) {
		cl_escalate();
		return;
	}

	/* 4. all survivors ready -> ask the coordinator to bump the epoch (idempotent
	 * re-send each tick until the commit is observed above). */
	if (cl_all_survivors_acked(cluster_node_id)) {
		coordinator = cl_compute_coordinator(cluster_node_id);
		if (coordinator >= 0)
			cl_send_commit_ready(coordinator, baseline_epoch);
	}
}

/* survivor coordinator: run the §3.1 two-phase commit for `leaving`. */
static void
cl_coordinator_commit(int32 leaving)
{
	uint64 baseline_epoch = cl_state->leave_epoch;
	uint64 new_epoch;
	ClusterLeaveIntentMarker m;

	if (cl_lmon_marker_async.has_staged_event) {
		ClusterLeaveAsyncMarkerResult ar = cl_poll_lmon_marker_stage();
		int phase = cl_lmon_marker_phase;
		uint64 staged_epoch = cl_lmon_marker_epoch;

		if (ar == CL_LEAVE_ASYNC_PENDING)
			return;
		if (ar == CL_LEAVE_ASYNC_FAILED)
			return;
		if (phase == CLUSTER_LEAVE_MARKER_PHASE_COMMITTING) {
			cl_release_lmon_marker_stage();

			/*
			 * spec-2.29a review r1 P1 — CL-I3 re-check at the staged-ACK
			 * handoff.  The COMMITTING marker wait now spans LMON ticks, so a
			 * real death can bump CSSD dead_generation INSIDE the wait window
			 * while the fail-stop epoch has not yet advanced (the >=3-node
			 * window of Hardening v1.0.1 P1-2).  The guarded CAS inside
			 * apply_clean_leave_as_coordinator only catches the epoch move;
			 * re-run the same dead_gen-aware coherence check the non-staged
			 * pre-check uses, at the last observable point before the commit
			 * applies.  On failure the leave does not commit and the leaving
			 * node escalates to fail-stop (identical to the pre-check path).
			 */
			if (!cl_coherent(baseline_epoch)) {
				ereport(LOG,
						(errmsg("cluster clean-leave: version moved (epoch or third-party death) "
								"across the COMMITTING marker wait for node %d; not committing "
								"(escalate to fail-stop, CL-I3)",
								leaving)));
				return;
			}

			new_epoch = cluster_reconfig_apply_clean_leave_as_coordinator(leaving, baseline_epoch);
			if (new_epoch == 0) {
				ereport(
					LOG,
					(errmsg("cluster clean-leave: epoch moved off baseline %llu before commit "
							"for node %d; not committing (the leaving node escalates to fail-stop)",
							(unsigned long long)baseline_epoch, leaving)));
				return;
			}
			cl_build_marker(&m, CLUSTER_LEAVE_MARKER_PHASE_COMMITTED, leaving, new_epoch);
			(void)cl_start_lmon_marker_stage(&m, CLUSTER_LEAVE_MARKER_PHASE_COMMITTED, leaving,
											 new_epoch);
			(void)cl_poll_lmon_marker_stage();
			ereport(LOG,
					(errmsg("cluster clean-leave: committed departure of node %d at epoch %llu",
							leaving, (unsigned long long)new_epoch)));
			return;
		}
		if (phase == CLUSTER_LEAVE_MARKER_PHASE_COMMITTED) {
			pg_atomic_write_u32(&cl_state->committed_marker_durable, 1);
			cl_send_committed(leaving, staged_epoch);
			cl_release_lmon_marker_stage();
			return;
		}
		cl_release_lmon_marker_stage();
		return;
	}

	/* CL-I3 pre-check: refuse to commit a clean leave on a version a real death
	 * already bumped — the leaving node will then observe a non-CLEAN_LEAVE event
	 * and escalate.  This is a cheap early-out; the authoritative guard is the
	 * guarded CAS inside apply_clean_leave_as_coordinator below, which closes the
	 * check-then-bump TOCTOU at >=3 nodes. */
	/*
	 * CL-I3 commit-handoff coherence (epoch AND others-dead): refuse to commit if
	 * a THIRD-PARTY death intruded since this survivor started tracking the leave
	 * — either the death's fail-stop already bumped the epoch, OR (the >=3-node
	 * window, Hardening v1.0.1 P1-2) CSSD added it to the others-dead set but the
	 * fail-stop epoch has NOT yet advanced, which an epoch-only check (and the
	 * guarded CAS below) would miss and commit on a stale membership view.  Uses
	 * the same others-dead coherence helper as drive_drain (spec-2.29a ②b: the
	 * leaving node's own expected DEAD is excluded so it never falsely escalates);
	 * the leaving node then observes the eventual foreign event and escalates.
	 */
	if (!cl_coherent(baseline_epoch)) {
		ereport(LOG,
				(errmsg("cluster clean-leave: version moved (epoch or third-party death) before "
						"committing node %d; not committing (escalate to fail-stop, CL-I3)",
						leaving)));
		return;
	}

	/* (1) COMMITTING(E) marker (coordinator's own slot, before the bump; NOT a
	 * trust basis).  Not durable -> do not commit. */
	cl_build_marker(&m, CLUSTER_LEAVE_MARKER_PHASE_COMMITTING, leaving, baseline_epoch + 1);
	(void)cl_start_lmon_marker_stage(&m, CLUSTER_LEAVE_MARKER_PHASE_COMMITTING, leaving,
									 baseline_epoch + 1);
	(void)cl_poll_lmon_marker_stage();
}

/* RF-ROOT P6 (STOP-01 contract): has the leaving node's OLD
 * process provably exited?  The CSSD peer state is node-scoped, so a fast
 * restart (heartbeats resumed inside the DEAD window) never reads DEAD; but
 * an observed slot with a coherent, fresh-alive incarnation DIFFERENT from
 * the incarnation the cluster last admitted for the node (which a
 * clean-departed node keeps until it is re-admitted by a join) can only
 * belong to a NEW process — the old one is gone. */
static bool
cl_leaver_reincarnated(int32 leaving)
{
	uint64 obs_inc = 0;
	uint64 obs_gen = 0;
	uint64 admitted;

	if (leaving < 0 || leaving >= CLUSTER_MAX_NODES)
		return false;
	if (!cluster_reconfig_get_observed_slot(leaving, &obs_inc, &obs_gen))
		return false;
	if (!cluster_reconfig_get_observed_fresh_alive(leaving))
		return false;
	admitted = cluster_membership_get_last_admitted_incarnation(leaving);
	/* verification P2: strictly-newer (monotonic) comparison.  `!=` would
	 * also fire on an out-of-order STALE observation frame (an old
	 * incarnation from before the admitted floor), which is not proof the
	 * old process exited; only an incarnation ABOVE the admitted floor can
	 * belong to a fresh process. */
	return admitted != 0 && obs_inc != 0 && obs_inc > admitted;
}

/* survivor (incl. coordinator) side of another node's leave. */
static void
cl_survivor_tick(int32 leaving)
{
	int32 coordinator;

	/*
	 * 0. CL-I7 escalate: if the leaving node died (CSSD DEAD) BEFORE its leave
	 * committed, the cooperative leave was abandoned mid-flight.  Clear this
	 * survivor's leave state so its serve-gate stops withholding (the node is not
	 * clean_departed, so effective_dead still includes it) and the existing
	 * death-driven fail-stop path takes over — clean leave never weakens fail-stop
	 * safety, and we never assume the drain completed (8.B).  A node that DID
	 * commit is clean_departed (handled in step 3); its later CSSD DEAD is the
	 * expected dormant exit, suppressed by CL-I13, and must NOT clear here.
	 */
	if (cluster_cssd_get_peer_state(leaving) == CLUSTER_CSSD_PEER_DEAD
		&& !cluster_reconfig_is_clean_departed(leaving)) {
		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		if (cl_state->leaving_node_id == leaving) {
			cl_state->leaving_node_id = -1;
			cl_state->leave_epoch = 0;
			pg_atomic_write_u32(&cl_state->survivor_acked, 0);
			pg_atomic_write_u32(&cl_state->commit_ready_received, 0);
		}
		LWLockRelease(&cl_state->lock);
		ereport(LOG, (errmsg("cluster clean-leave: leaving node %d died before committing; "
							 "abandoning the cooperative path, fail-stop takes over (CL-I7)",
							 leaving)));
		return;
	}

	/* 1. drop refs to the leaving node + send the PRE-epoch readiness ACK (once). */
	if (pg_atomic_read_u32(&cl_state->survivor_acked) == 0) {
		cluster_grd_cleanup_on_node_dead(leaving); /* drop GRD holders/waiters of leaving */
		cluster_clean_leave_survivor_ack(leaving, cl_state->leave_epoch);
		pg_atomic_write_u32(&cl_state->survivor_acked, 1);
	}

	/* 2. coordinator: on LEAVE_COMMIT_READY, run the two-phase commit (once —
	 * is_clean_departed gates re-entry). */
	coordinator = cl_compute_coordinator(leaving);
	if (coordinator == cluster_node_id && pg_atomic_read_u32(&cl_state->commit_ready_received)
		&& !cluster_reconfig_is_clean_departed(leaving))
		cl_coordinator_commit(leaving);

	/*
	 * 2a (Hardening v1.0.1, P1-V0.7 exit gate): after the commit, the coordinator
	 * must drive the COMMITTED marker to majority-durability — never give up.  If
	 * the first attempt in cl_coordinator_commit did not reach majority, retry it
	 * here every tick until it does; once durable, tell the leaving node it may
	 * exit (LEAVE_COMMITTED), re-sending each tick while the leaver is alive (best-
	 * effort delivery, idempotent gate).  Until durable the leaving node stays in
	 * BARRIER_WAIT and never departs without a durable truth-source.
	 */
	if (coordinator == cluster_node_id && cluster_reconfig_is_clean_departed(leaving)) {
		uint64 committed_epoch = cluster_reconfig_get_clean_departed_epoch(leaving);

		if (!pg_atomic_read_u32(&cl_state->committed_marker_durable))
			cl_drive_committed_marker_stage(leaving, committed_epoch);

		/* Re-send LEAVE_COMMITTED every tick once durable until the leaver is gone
		 * (best-effort IC): the leaving node will not depart until it receives one,
		 * and step 3 holds the slot until it is CSSD-dead, so delivery is assured.
		 *
		 * RF-ROOT P6 (L5 shutdown handoff, post-commit survivor confirmation):
		 * the send is additionally gated on this survivor's own serving authority
		 * being current again — which the ordinary LMON serving rebind only
		 * confirms AFTER the clean-leave GRD episode closed (shards unfrozen,
		 * NORMAL, new generation bound).  The leaver treats LEAVE_COMMITTED as
		 * "new generation confirmed AND shard=NORMAL", so its shutdown checkpoint
		 * and THREAD_CLEAN_CLOSE CF acquires run against a survivor-mastered,
		 * NORMAL CF shard instead of wedging on the episode freeze window.  If the
		 * rebind never confirms, the send never happens and the leaver fails
		 * closed at its barrier deadline (no fake clean-leave completion).
		 *
		 * RF-ROOT P6 (STOP-01 contract): the "every tick" re-send
		 * is floored to 1 Hz — the LMON iteration can be driven at inbound-frame
		 * rate, and a per-iteration re-send combined with the rejoining node's
		 * per-iteration authority done-key broadcast forms a self-sustaining
		 * frame ping-pong that starves the cssd heartbeat path (t243 L5 restore
		 * boot: node0 falsely DEAD at +3s, phase-3 broken, 60s bail).  The
		 * confirmation is idempotent and delivery-assured at 1 Hz. */
		if (pg_atomic_read_u32(&cl_state->committed_marker_durable)
			&& cluster_serving_ready_is_current()) {
			static TimestampTz last_committed_send_at = 0;
			TimestampTz now_ts = GetCurrentTimestamp();

			if (now_ts == 0 || last_committed_send_at == 0
				|| now_ts - last_committed_send_at >= INT64CONST(1000000)) {
				cl_send_committed(leaving, committed_epoch);
				last_committed_send_at = now_ts;
			}
		}
	}

	/* 2b. EVERY survivor (not just the coordinator) must observe the CLEAN_LEAVE
	 * commit and, before recording the node clean_departed, invalidate its own
	 * cached copies of the leaving node's blocks (CL-I5 happens-before: once
	 * is_clean_departed is true the serve-gate allows the storage fallback, so the
	 * cache MUST already be invalidated here, else a stale cached read could
	 * slip through).  The coordinator did this in apply_clean_leave_as_coordinator
	 * (on_epoch_advance); a non-coordinator survivor does it here on observe. */
	if (!cluster_reconfig_is_clean_departed(leaving)) {
		ReconfigEvent ev;

		cluster_reconfig_get_last_event(&ev);
		if (ev.reconfig_kind == RECONFIG_KIND_CLEAN_LEAVE && ev.new_epoch > 0
			&& (ev.dead_bitmap[leaving / 8] & (uint8)(1u << (leaving % 8)))) {
			cluster_gcs_block_clean_leave_invalidate_for(leaving, ev.new_epoch);
			cluster_reconfig_record_clean_departed(leaving, ev.new_epoch, /*raise_floor*/ false);
		}
	}

	/* 3. release the local leave-tracking slot only once the leave is committed
	 * (clean_departed) AND the leaving node has actually departed (CSSD DEAD).
	 * Holding it until departure (a) lets the coordinator keep re-sending
	 * LEAVE_COMMITTED until the leaver is gone (assured delivery, P1-1), and (b)
	 * serializes leaves (a second leave is NAK'd until this one fully departs,
	 * single-leave-at-a-time, P1-3).  clean_departed persists in the reconfig
	 * region and suppresses the node's CSSD DEAD from a spurious fail-stop (CL-I13).
	 *
	 * RF-ROOT P6 (STOP-01 contract): CSSD peer state is
	 * node-scoped, not process-scoped — when the leaving node restarts quickly
	 * (heartbeats resume inside the 3 s DEAD window) the peer never reads DEAD
	 * and the slot is held forever, which pins cluster_clean_leave_in_progress()
	 * and the P2 join-serialization gate permanently blocks the fast-rejoin
	 * chain on the survivor (t243 L5 restore boot: eviction fired, join-drive
	 * blocked with clean_leave=1, rejoiner phase-3 starved the full 60 s
	 * window).  An observed NEW incarnation is conclusive proof the old
	 * process exited — a fresh process cannot be alive while the old one is —
	 * so the release fires on that too. */
	if (cluster_reconfig_is_clean_departed(leaving)
		&& (cluster_cssd_get_peer_state(leaving) == CLUSTER_CSSD_PEER_DEAD
			|| cl_leaver_reincarnated(leaving))) {
		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		if (cl_state->leaving_node_id == leaving) {
			cl_state->leaving_node_id = -1;
			cl_state->leave_epoch = 0;
			pg_atomic_write_u32(&cl_state->survivor_acked, 0);
			pg_atomic_write_u32(&cl_state->commit_ready_received, 0);
		}
		LWLockRelease(&cl_state->lock);
	}
}

static bool
cl_phase1_full_stop_send_admitted(ClusterICSendResult result)
{
	return result == CLUSTER_IC_SEND_DONE || result == CLUSTER_IC_SEND_WOULD_BLOCK;
}

static bool
cl_phase1_full_stop_all_peer_bits(const uint8 *bitmap)
{
	int32 peer;

	for (peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
		if (peer != cluster_node_id && !cl_phase1_member_bit_is_set(bitmap, peer))
			return false;
	}
	return true;
}

static void
cl_phase1_full_stop_post_stopped_request_lmon_tick(void)
{
	ClusterPhase1FullStopPlan current;
	uint64 local_nonce;
	uint64 deadline_us;
	uint64 now_us;
	uint32 local_wal_state = 0;
	bool pending;
	bool wake = false;
	int32 peer;

	LWLockAcquire(&cl_state->lock, LW_SHARED);
	pending = pg_atomic_read_u32(&cl_state->request_in_progress) != 0
			  && pg_atomic_read_u32(&cl_state->shutdown_driven) != 0
			  && pg_atomic_read_u32(&cl_state->preflight_pending) != 0
			  && pg_atomic_read_u32(&cl_state->preflight_sent) != 0
			  && pg_atomic_read_u32(&cl_state->phase1_release_pending) == 0;
	local_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	deadline_us = cl_state->barrier_deadline_us;
	LWLockRelease(&cl_state->lock);
	if (!pending)
		return;

	/* The same shared preflight flags also describe the predecessor ACTIVE
	 * round.  Existing WAL state is the frozen phase discriminator: ACTIVE is
	 * owned by the generic pre-checkpoint fanout and must remain untouched here. */
	if (!cl_phase1_full_stop_capture_barrier_identity(&current, &local_wal_state))
		return;
	if (local_wal_state != CLUSTER_WAL_SLOT_STATE_STOPPED)
		return;
	if (cl_phase1_post_stopped_request_round_nonce != local_nonce) {
		cl_phase1_post_stopped_request_round_nonce = local_nonce;
		memset(cl_phase1_post_stopped_request_sent, 0, sizeof(cl_phase1_post_stopped_request_sent));
	}

	now_us = (uint64)GetCurrentTimestamp();
	if (local_nonce == 0 || local_nonce == UINT64_MAX || deadline_us == 0 || now_us == 0
		|| now_us >= deadline_us) {
		pg_atomic_write_u32(&cl_state->nak_received, 1);
		wake = true;
		goto out;
	}

	for (peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
		ClusterICSendResult send_result;
		bool send_request;

		if (peer == cluster_node_id)
			continue;
		LWLockAcquire(&cl_state->lock, LW_SHARED);
		send_request = pg_atomic_read_u32(&cl_state->request_in_progress) != 0
					   && pg_atomic_read_u32(&cl_state->shutdown_driven) != 0
					   && pg_atomic_read_u32(&cl_state->preflight_pending) != 0
					   && pg_atomic_read_u32(&cl_state->preflight_sent) != 0
					   && pg_atomic_read_u32(&cl_state->phase1_release_pending) == 0
					   && pg_atomic_read_u64(&cl_state->leave_attempt_nonce) == local_nonce
					   && cl_state->barrier_deadline_us == deadline_us
					   && !cl_phase1_member_bit_is_set(cl_phase1_post_stopped_request_sent, peer);
		LWLockRelease(&cl_state->lock);
		if (!send_request)
			continue;

		send_result = cl_phase1_full_stop_send_post_stopped_request(peer, local_nonce, 0);
		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		if (pg_atomic_read_u32(&cl_state->request_in_progress) != 0
			&& pg_atomic_read_u32(&cl_state->shutdown_driven) != 0
			&& pg_atomic_read_u32(&cl_state->preflight_pending) != 0
			&& pg_atomic_read_u32(&cl_state->preflight_sent) != 0
			&& pg_atomic_read_u32(&cl_state->phase1_release_pending) == 0
			&& pg_atomic_read_u64(&cl_state->leave_attempt_nonce) == local_nonce
			&& cl_state->barrier_deadline_us == deadline_us
			&& !cl_phase1_member_bit_is_set(cl_phase1_post_stopped_request_sent, peer)) {
			if (cl_phase1_full_stop_send_admitted(send_result))
				cl_phase1_member_bit_set(cl_phase1_post_stopped_request_sent, peer);
			else if (send_result == CLUSTER_IC_SEND_HARD_ERROR)
				pg_atomic_write_u32(&cl_state->nak_received, 1);
			else if (send_result == CLUSTER_IC_SEND_NOT_ADMITTED) {
				/* Retain this exact peer owner for the next LMON tick. */
			}
		}
		LWLockRelease(&cl_state->lock);
		if (send_result == CLUSTER_IC_SEND_HARD_ERROR) {
			wake = true;
			cluster_ic_tier1_close_peer(peer, "phase-1 post-STOPPED request send hard error");
		}
	}

out:
	if (wake && ProcGlobal->checkpointerLatch != NULL)
		SetLatch(ProcGlobal->checkpointerLatch);
}

static void
cl_phase1_full_stop_consume_request_ahead_lmon_tick(void)
{
	ClusterPhase1FullStopPlan current;
	uint32 local_wal_state = 0;
	uint64 local_nonce;
	uint64 deadline_us;
	uint64 now_us;
	bool have_ahead = false;
	bool post_requests_sent;
	bool fail_closed = false;
	bool wake = false;
	int32 peer;

	for (peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
		if (peer != cluster_node_id && cl_phase1_post_stopped_request_ahead[peer].valid) {
			have_ahead = true;
			break;
		}
	}
	if (!have_ahead)
		return;
	if (!cl_phase1_full_stop_capture_barrier_identity(&current, &local_wal_state)) {
		fail_closed = true;
		goto out;
	}
	if (local_wal_state == CLUSTER_WAL_SLOT_STATE_ACTIVE)
		return;
	if (local_wal_state != CLUSTER_WAL_SLOT_STATE_STOPPED) {
		fail_closed = true;
		goto out;
	}

	LWLockAcquire(&cl_state->lock, LW_SHARED);
	local_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	deadline_us = cl_state->barrier_deadline_us;
	post_requests_sent = cl_phase1_post_stopped_request_round_nonce == local_nonce
						 && cl_phase1_full_stop_all_peer_bits(cl_phase1_post_stopped_request_sent);
	LWLockRelease(&cl_state->lock);
	if (!post_requests_sent)
		return;
	now_us = (uint64)GetCurrentTimestamp();
	if (now_us == 0 || now_us >= deadline_us) {
		fail_closed = true;
		goto out;
	}

	for (peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
		bool source_active = false;
		bool source_stopped = false;

		if (peer == cluster_node_id || !cl_phase1_post_stopped_request_ahead[peer].valid)
			continue;
		if (!cl_phase1_full_stop_capture_source_phase(&current, peer, &source_active,
													  &source_stopped)
			|| source_active || !source_stopped) {
			fail_closed = true;
			break;
		}

		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		post_requests_sent
			= cl_phase1_post_stopped_request_round_nonce == local_nonce
			  && cl_phase1_full_stop_all_peer_bits(cl_phase1_post_stopped_request_sent);
		if (pg_atomic_read_u64(&cl_state->leave_attempt_nonce) != local_nonce
			|| cl_state->barrier_deadline_us != deadline_us
			|| !cl_phase1_full_stop_consume_request_ahead_locked(&current, peer, local_nonce,
																 deadline_us, post_requests_sent))
			fail_closed = true;
		else
			wake = true;
		LWLockRelease(&cl_state->lock);
		if (fail_closed)
			break;
	}

out:
	if (fail_closed) {
		memset(cl_phase1_post_stopped_request_ahead, 0,
			   sizeof(cl_phase1_post_stopped_request_ahead));
		pg_atomic_write_u32(&cl_state->nak_received, 1);
		wake = true;
	}
	if (wake && ProcGlobal->checkpointerLatch != NULL)
		SetLatch(ProcGlobal->checkpointerLatch);
}

static void
cl_phase1_full_stop_post_stopped_reply_lmon_tick(void)
{
	ClusterPhase1FullStopPlan current;
	uint64 deadline_us;
	uint64 now_us;
	bool have_pending = false;
	bool round_active;
	bool wake = false;
	int32 peer;

	LWLockAcquire(&cl_state->lock, LW_SHARED);
	round_active = pg_atomic_read_u32(&cl_state->request_in_progress) != 0
				   && pg_atomic_read_u32(&cl_state->shutdown_driven) != 0;
	deadline_us = cl_state->barrier_deadline_us;
	for (peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
		if (peer != cluster_node_id
			&& cl_phase1_member_bit_is_set(cl_state->phase1_post_stopped_reply_pending, peer)
			&& !cl_phase1_member_bit_is_set(cl_state->phase1_post_stopped_reply_sent, peer)) {
			have_pending = true;
			break;
		}
	}
	LWLockRelease(&cl_state->lock);
	if (!have_pending)
		return;

	now_us = (uint64)GetCurrentTimestamp();
	if (!round_active || deadline_us == 0 || now_us == 0 || now_us >= deadline_us
		|| !cl_phase1_full_stop_capture_identity(CLUSTER_WAL_SLOT_STATE_STOPPED, true, &current)) {
		pg_atomic_write_u32(&cl_state->nak_received, 1);
		wake = true;
		goto out;
	}

	for (peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
		ClusterICSendResult send_result;
		uint64 peer_nonce;
		bool send_reply;

		if (peer == cluster_node_id)
			continue;
		LWLockAcquire(&cl_state->lock, LW_SHARED);
		send_reply
			= pg_atomic_read_u32(&cl_state->request_in_progress) != 0
			  && pg_atomic_read_u32(&cl_state->shutdown_driven) != 0
			  && cl_state->barrier_deadline_us == deadline_us
			  && cl_phase1_member_bit_is_set(cl_state->phase1_post_stopped_reply_pending, peer)
			  && !cl_phase1_member_bit_is_set(cl_state->phase1_post_stopped_reply_sent, peer);
		peer_nonce = cl_state->phase1_release_request_nonce[peer];
		LWLockRelease(&cl_state->lock);
		if (!send_reply)
			continue;
		if (peer_nonce == 0 || peer_nonce == UINT64_MAX) {
			pg_atomic_write_u32(&cl_state->nak_received, 1);
			wake = true;
			continue;
		}

		send_result = cl_phase1_full_stop_send_post_stopped_reply(peer, peer_nonce, 0);
		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		if (pg_atomic_read_u32(&cl_state->request_in_progress) != 0
			&& pg_atomic_read_u32(&cl_state->shutdown_driven) != 0
			&& cl_state->barrier_deadline_us == deadline_us
			&& cl_state->phase1_release_request_nonce[peer] == peer_nonce
			&& cl_phase1_member_bit_is_set(cl_state->phase1_post_stopped_reply_pending, peer)
			&& !cl_phase1_member_bit_is_set(cl_state->phase1_post_stopped_reply_sent, peer)) {
			if (cl_phase1_full_stop_send_admitted(send_result))
				cl_phase1_member_bit_set(cl_state->phase1_post_stopped_reply_sent, peer);
			else if (send_result == CLUSTER_IC_SEND_HARD_ERROR)
				pg_atomic_write_u32(&cl_state->nak_received, 1);
			else if (send_result == CLUSTER_IC_SEND_NOT_ADMITTED) {
				/* Retain exact ownership for the next existing LMON tick. */
			}
		}
		LWLockRelease(&cl_state->lock);
		if (send_result == CLUSTER_IC_SEND_HARD_ERROR) {
			wake = true;
			cluster_ic_tier1_close_peer(peer, "phase-1 post-STOPPED reply send hard error");
		}
	}

out:
	if (wake && ProcGlobal->checkpointerLatch != NULL)
		SetLatch(ProcGlobal->checkpointerLatch);
}

static void
cl_phase1_full_stop_release_lmon_tick(void)
{
	uint64 local_nonce;
	uint64 deadline_us;
	uint64 now_us;
	bool pending;
	bool wake = false;
	int32 peer;

	LWLockAcquire(&cl_state->lock, LW_SHARED);
	pending = pg_atomic_read_u32(&cl_state->phase1_release_pending) != 0;
	local_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
	deadline_us = cl_state->barrier_deadline_us;
	LWLockRelease(&cl_state->lock);
	if (!pending)
		return;
	now_us = (uint64)GetCurrentTimestamp();
	if (local_nonce == 0 || local_nonce == UINT64_MAX || deadline_us == 0
		|| now_us >= deadline_us) {
		pg_atomic_write_u32(&cl_state->nak_received, 1);
		wake = true;
		goto out;
	}

	for (peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
		ClusterICSendResult send_result;
		uint64 peer_nonce;
		bool send_request;
		bool send_reply;
		bool send_receipt;

		if (peer == cluster_node_id)
			continue;
		LWLockAcquire(&cl_state->lock, LW_SHARED);
		send_request = pg_atomic_read_u32(&cl_state->phase1_release_pending) != 0
					   && !cl_phase1_member_bit_is_set(cl_state->phase1_release_request_sent, peer);
		peer_nonce = cl_state->phase1_release_request_nonce[peer];
		send_reply = peer_nonce != 0
					 && cl_phase1_member_bit_is_set(cl_state->phase1_release_request_seen, peer)
					 && !cl_phase1_member_bit_is_set(cl_state->phase1_release_reply_sent, peer);
		send_receipt = cl_phase1_member_bit_is_set(cl_state->phase1_release_reply_seen, peer)
					   && !cl_phase1_member_bit_is_set(cl_state->phase1_release_receipt_sent, peer);
		LWLockRelease(&cl_state->lock);

		if (send_request) {
			send_result = cl_phase1_full_stop_send_release_announce(
				peer, CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE, local_nonce, 0);
			LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
			if (pg_atomic_read_u32(&cl_state->phase1_release_pending) != 0
				&& pg_atomic_read_u64(&cl_state->leave_attempt_nonce) == local_nonce) {
				if (cl_phase1_full_stop_send_admitted(send_result))
					cl_phase1_member_bit_set(cl_state->phase1_release_request_sent, peer);
				else if (send_result == CLUSTER_IC_SEND_HARD_ERROR)
					pg_atomic_write_u32(&cl_state->nak_received, 1);
			}
			LWLockRelease(&cl_state->lock);
			if (send_result == CLUSTER_IC_SEND_HARD_ERROR)
				wake = true;
			if (send_result == CLUSTER_IC_SEND_HARD_ERROR)
				cluster_ic_tier1_close_peer(peer, "phase-1 release request send hard error");
		}
		if (send_reply) {
			send_result = cl_phase1_full_stop_send_release_reply(peer, peer_nonce, 0);
			LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
			if (pg_atomic_read_u32(&cl_state->phase1_release_pending) != 0
				&& cl_state->phase1_release_request_nonce[peer] == peer_nonce) {
				if (cl_phase1_full_stop_send_admitted(send_result))
					cl_phase1_member_bit_set(cl_state->phase1_release_reply_sent, peer);
				else if (send_result == CLUSTER_IC_SEND_HARD_ERROR)
					pg_atomic_write_u32(&cl_state->nak_received, 1);
			}
			LWLockRelease(&cl_state->lock);
			if (send_result == CLUSTER_IC_SEND_HARD_ERROR)
				wake = true;
			if (send_result == CLUSTER_IC_SEND_HARD_ERROR)
				cluster_ic_tier1_close_peer(peer, "phase-1 release reply send hard error");
		}
		if (send_receipt) {
			send_result = cl_phase1_full_stop_send_release_announce(
				peer, CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT, local_nonce, 0);
			LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
			if (pg_atomic_read_u32(&cl_state->phase1_release_pending) != 0
				&& pg_atomic_read_u64(&cl_state->leave_attempt_nonce) == local_nonce) {
				if (cl_phase1_full_stop_send_admitted(send_result))
					cl_phase1_member_bit_set(cl_state->phase1_release_receipt_sent, peer);
				else if (send_result == CLUSTER_IC_SEND_HARD_ERROR)
					pg_atomic_write_u32(&cl_state->nak_received, 1);
			}
			LWLockRelease(&cl_state->lock);
			if (send_result == CLUSTER_IC_SEND_HARD_ERROR)
				wake = true;
			if (send_result == CLUSTER_IC_SEND_HARD_ERROR)
				cluster_ic_tier1_close_peer(peer, "phase-1 release receipt send hard error");
		}
	}

	LWLockAcquire(&cl_state->lock, LW_SHARED);
	pending = pg_atomic_read_u32(&cl_state->phase1_release_pending) != 0
			  && cl_phase1_full_stop_all_peer_bits(cl_state->phase1_release_request_sent)
			  && cl_phase1_full_stop_all_peer_bits(cl_state->phase1_release_reply_sent)
			  && cl_phase1_full_stop_all_peer_bits(cl_state->phase1_release_receipt_sent);
	LWLockRelease(&cl_state->lock);
	if (pending) {
		bool drained = true;

		for (peer = 0; peer < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; peer++) {
			if (peer != cluster_node_id && cluster_ic_mux_peer_has_pending_outbound(peer)) {
				drained = false;
				break;
			}
		}
		if (drained) {
			LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
			if (pg_atomic_read_u32(&cl_state->phase1_release_pending) != 0
				&& pg_atomic_read_u64(&cl_state->leave_attempt_nonce) == local_nonce
				&& cl_state->barrier_deadline_us == deadline_us) {
				pg_atomic_write_u32(&cl_state->phase1_release_transport_drained, 1);
				wake = true;
			}
			LWLockRelease(&cl_state->lock);
		}
	}

out:
	if (wake && ProcGlobal->checkpointerLatch != NULL)
		SetLatch(ProcGlobal->checkpointerLatch);
}

/*
 * cluster_clean_leave_lmon_tick -- D6 orchestration, called every LMON tick
 * (before cluster_reconfig_lmon_tick).  Branches on whether THIS node is the
 * leaver or a survivor of someone else's leave.  LMON only orchestrates (IC +
 * shmem); it never flushes (CL-I9).
 */
void
cluster_clean_leave_lmon_tick(void)
{
	int32 leaving;

	if (cl_state == NULL || !cluster_enabled)
		return;

	cl_normal_stop_fronts_lmon_tick();
	if (cluster_normal_stop_requested()
		&& pg_atomic_read_u32(&cl_normal_stop->identity_published) != 0) {
		cl_normal_stop_post_lmon_tick();
		cl_normal_stop_release_lmon_tick();
		return; /* current OPEN never enters the epoch-zero pristine consumers */
	}

	cl_phase1_full_stop_post_stopped_request_lmon_tick();
	cl_phase1_full_stop_consume_request_ahead_lmon_tick();
	cl_phase1_full_stop_post_stopped_reply_lmon_tick();

	/*
	 * Hardening v1.0.2 (P1, F6 layer-1 preflight): the request backend staged a
	 * side-effect-free preflight probe (IC sends are LMON-only).  Broadcast it once
	 * here, BEFORE the leaving_node_id check below — during the preflight the
	 * leaver has NOT yet entered REQUESTED, so leaving_node_id is still -1.
	 */
	if (pg_atomic_read_u32(&cl_state->preflight_pending) == 1
		&& pg_atomic_read_u32(&cl_state->preflight_sent) == 0) {
		cluster_clean_leave_ic_broadcast_announce(
			0 /* leave_epoch=0 for a probe */, pg_atomic_read_u64(&cl_state->leave_attempt_nonce),
			/*preflight*/ true);
		pg_atomic_write_u32(&cl_state->preflight_sent, 1);
		if (ProcGlobal->checkpointerLatch != NULL)
			SetLatch(ProcGlobal->checkpointerLatch);
	}

	cl_phase1_full_stop_release_lmon_tick();

	leaving = cl_state->leaving_node_id;
	if (leaving < 0)
		return; /* no leave in progress */

	if (leaving == cluster_node_id) {
		ClusterLeavePhase phase = (ClusterLeavePhase)pg_atomic_read_u32(&cl_state->phase);

		/*
		 * Broadcast the real CLEAN_LEAVE_ANNOUNCE once, here in LMON context
		 * (IC sends are LMON-only).  The backend set REQUESTED and is draining
		 * concurrently; the announce makes survivors drop refs + ACK so the
		 * BARRIER_WAIT below can collect them.  A disabled survivor replies NAK
		 * (caught by the drain's pre-quiesce NAK wait).
		 */
		if (phase >= CLUSTER_LEAVE_REQUESTED && phase <= CLUSTER_LEAVE_BARRIER_WAIT
			&& pg_atomic_read_u32(&cl_state->announce_sent) == 0) {
			cluster_clean_leave_ic_broadcast_announce(
				cl_state->leave_epoch, pg_atomic_read_u64(&cl_state->leave_attempt_nonce),
				/*preflight*/ false);
			pg_atomic_write_u32(&cl_state->announce_sent, 1);
		}

		/* the backend drives up to BARRIER_WAIT; LMON finishes the commit. */
		if (phase == CLUSTER_LEAVE_BARRIER_WAIT)
			cl_leaving_barrier_tick();
	} else {
		cl_survivor_tick(leaving);
	}
}
