/*-------------------------------------------------------------------------
 *
 * cluster_reconfig.c
 *	  pgrac cluster reconfig coordinator — internal-only A scope
 *	  (spec-2.29 Sprint A Step 1 skeleton).
 *
 *	  Step 1 shipped scope (this file):
 *	    - ClusterReconfigState shmem region with LWLock-guarded
 *	      last_applied + 3 atomic counters
 *	    - StaticAssertDecl on ReconfigEvent + ClusterReconfigState
 *	      sizeof bounds (P2.8 — natural-aligned, NOT 64B literal)
 *	    - cluster_reconfig_shmem_size / init / register helpers
 *	    - cluster_reconfig_get_last_event (always-1-row contract P2.9)
 *	    - cluster_reconfig_publish_event (LWLock-acquired)
 *	    - Stubs for lmon_tick / broadcast_local_procsig /
 *	      apply_epoch_bump_as_coordinator / check_pending — bodies
 *	      land in Step 2
 *
 *	  Steps 2-7: lmon_tick body (Q2 A'' coordinator decision +
 *	  declared-peer filter F11), ProcessInterrupts I6 guard, envelope
 *	  observe path D20, SRF view body, TAP 099 L1-L10, regress + manuals,
 *	  catalog surface delta + baseline sync (L98), ship gate.
 *
 *	  Spec authority: pgrac:specs/spec-2.29-reconfig-coordinator-
 *	  internal.md (DRAFT v0.3).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_reconfig.c
 *
 * NOTES
 *	  pgrac-original file.  Compiled only in --enable-cluster builds.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "port/pg_bitutils.h" /* pg_number_of_ones (cold-formation bitmap) */

#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_wal_state.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_xid_stripe_boot.h" /* spec-6.15 D5b joiner gate */

/* RF-ROOT P4 online launch producer.  The carrier contains only the exact
 * current FAIL_STOP attempt stamp and full canonical root duty.  Formation,
 * external admissions and IR remain process-local and are rebuilt by the
 * worker; no pointer or dead-bitmap pair crosses bgw_extra. */
bool
cluster_reconfig_thread_recovery_eligibility_consume(uint16 origin_thread,
													 ClusterThreadRecLaunchEligibility *out)
{
	ReconfigEvent event = { 0 };
	ClusterControlRootIdentity identity;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;
	ClusterControlRootResult root_result;
	int32 origin_node;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || origin_thread == 0 || origin_thread > CLUSTER_MAX_NODES)
		return false;
#ifndef USE_PGRAC_CLUSTER
	return false;
#else
	origin_node = (int32)origin_thread - 1;
	cluster_reconfig_get_last_event(&event);
	if (event.reconfig_kind != RECONFIG_KIND_FAIL_STOP || event.event_id == 0
		|| event.new_epoch == 0
		|| (event.dead_bitmap[origin_node / 8] & (uint8)(UINT8_C(1) << (origin_node % 8))) == 0)
		return false;
	root_result = cluster_control_root_lookup_owner_by_node_runtime(origin_node, &identity,
																	&snapshot, &token);
	if ((root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 && root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		|| !cluster_recovery_duty_key_valid_v1(&identity)
		|| identity.origin_thread_id != origin_thread || identity.origin_node_id != origin_node
		|| snapshot.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		|| memcmp(&snapshot.identity, &identity, sizeof(identity)) != 0)
		return false;
	out->origin_thread = origin_thread;
	out->attempt_stamp = event.new_epoch;
	out->duty = identity;
	return true;
#endif
}

#ifdef USE_PGRAC_CLUSTER

#include <string.h>

#include "access/transam.h"		  /* TransactionIdIsValid */
#include "access/xact.h"		  /* IsTransactionState (Step 2 D4) */
#include "access/xlog.h"		  /* GetXLogInsertRecPtr (Step 2 D2) */
#include "common/hashfn.h"		  /* hash_bytes_extended */
#include "fmgr.h"				  /* PG_FUNCTION_ARGS (Step 3 D5b SRF) */
#include "funcapi.h"			  /* InitMaterializedSRF (Step 3 D5b SRF) */
#include "miscadmin.h"			  /* MyProcPid */
#include "postmaster/interrupt.h" /* ShutdownRequestPending */
#include "storage/lwlock.h"
#include "storage/proc.h"		/* PGPROC */
#include "storage/procsignal.h" /* SendProcSignal + PROCSIG_CLUSTER_RECONFIG_START */
#include "storage/shmem.h"
#include "storage/sinvaladt.h" /* BackendIdGetProc */
#include "utils/builtins.h"	   /* cstring_to_text */
#include "utils/timestamp.h"
#include "utils/wait_event.h" /* WAIT_EVENT_CLUSTER_BGPROC_LMON_RECONFIG_TICK (D9) */

#include "cluster/cluster_conf.h"			/* cluster_conf_lookup_node */
#include "cluster/cluster_cssd.h"			/* cluster_cssd_get_peer_state, get_dead_generation */
#include "cluster/cluster_elog.h"			/* cluster_node_id */
#include "cluster/cluster_epoch.h"			/* advance + observe + set_changed_at_lsn */
#include "cluster/cluster_external_fence.h" /* STOP04 rejoin snapshots */
#include "cluster/cluster_gcs_block.h"		/* spec-2.34 D4 — eager epoch wake hook */
#include "cluster/cluster_grd.h"			/* spec-5.16 D3b — arm join PCM block fence */
#include "cluster/cluster_ic.h"				/* Candidate2 HELLO capability */
#include "cluster/cluster_startup_phase.h"	/* RF-ROOT P04 serving split */
#include "cluster/cluster_ic_router.h"		/* phase-3 target-LMON CONTROL send */
#include "cluster/cluster_ic_tier1.h"		/* close peer after send HARD_ERROR */
#include "cluster/cluster_sinval.h"			/* spec-2.39 D14 — RESET-all reconfig hook */
#include "cluster/cluster_tt_status.h"		/* spec-3.1 D7 — TT status overlay flush hook */
#include "cluster/storage/cluster_undo_block0.h" /* instantaneous R4A READY */
#include "storage/ipc.h"						 /* on_shmem_exit (spec-5.15 D4 latch clear) */
#include "storage/latch.h"				 /* Latch / SetLatch (spec-5.15 D4 marker mailbox) */
#include "cluster/cluster_clean_leave.h" /* v1.0.4 — cluster_clean_leave_in_progress (serialize) */
#include "cluster/cluster_guc.h"		 /* cluster_enabled, cluster_online_join */
#include "cluster/cluster_inject.h"		 /* CLUSTER_INJECTION_POINT */
#include "cluster/cluster_lms.h"		 /* current LMS restart generation */
#include "cluster/cluster_lmon.h"		 /* cluster_lmon_marker_complete_wakeup */
#include "cluster/cluster_voting_disk_io.h" /* spec-5.15 D4 — region-3 join-marker slot I/O */
#include "cluster/cluster_write_fence.h"	/* spec-4.12 D4 — durable fence marker submit */
#include "cluster/cluster_qvotec.h"			/* cluster_qvotec_in_quorum */
#include "cluster/cluster_shmem.h"			/* cluster_shmem_register_region */
#include "cluster/cluster_signal.h"			/* cluster_reconfig_start_pending */
#include "cluster/cluster_sf_dep.h"			/* current peer capability record */
#include "cluster/cluster_touched_peers.h"	/* spec-5.14 D4 — touched ∩ dead dispatch */


/*
 * StaticAssertDecl: bound ReconfigEvent + ClusterReconfigState sizeof.
 *
 *	  Per spec-2.29 P2.8 fix — v0.1 wrote sizeof(ReconfigEvent) == 64
 *	  packed which was wrong (natural fields sum > 64).  v0.3 uses
 *	  natural alignment + upper bound assertion;exact size doesn't
 *	  matter because shmem reservation walks sizeof() expression.
 *
 *	  ReconfigEvent natural fields (64-bit ABI):
 *	    8 event_id + 4 coord + 4 _pad0 + 8 old_epoch + 8 new_epoch
 *	    + 16 dead_bitmap + 8 applied_at + 4 observer_role + 4 _pad1
 *	    + 8 event_seq + 8 cssd_dead_generation = 80 bytes exactly.
 *	  Allow up to 96 bytes for future field append without bump.
 */
/* spec-5.15 D3 widened 96 -> 112: join_bitmap[16] grows ReconfigEvent 88 -> 104. */
StaticAssertDecl(sizeof(ReconfigEvent) <= 112, "ReconfigEvent must fit within 112 bytes");
StaticAssertDecl(sizeof(ReconfigEvent) >= 64,
				 "ReconfigEvent must be at least 64 bytes (defensive — fields enumerated)");

/* spec-5.14 D6 — the per-kind counter array must cover every touch class. */
StaticAssertDecl(CLUSTER_RECONFIG_TOUCH_KIND_COUNT == CLUSTER_TOUCH_KIND_COUNT,
				 "reconfig touched-kind counter array must match ClusterTouchKind count");


/*
 * Shmem region (single instance;pointer set by shmem_init).
 */
static ClusterReconfigState *ReconfigShmem = NULL;
/* Process-local early-boot classifier. */
static bool joiner_gate_decided = false;
static bool offpath_fast_rejoin_active_local = false;

/* Temporary 8.15-PRE D1 evidence.  A MEMBER byte without its exact admitted
 * floor is the current t/430 first failed predicate.  Remember only the last
 * LMON-local observation so a regression can be attributed to the interval
 * that produced it; this creates no shared state and grants no authority. */
static void
cluster_reconfig_membership_floor_diagnostic(const char *stage)
{
	static uint64 prior_floor[4];
	static uint8 prior_state[4];
	static bool initialized;
	uint64 floor[4];
	uint8 state[4];
	bool invalid = false;
	bool regressed = false;
	int node;

	if (ReconfigShmem == NULL || stage == NULL)
		return;
	for (node = 0; node < 4; node++) {
		floor[node] = cluster_membership_get_last_admitted_incarnation(node);
		state[node] = (uint8)cluster_membership_get_state(node);
		if (state[node] == (uint8)CLUSTER_MEMBER_MEMBER && floor[node] == 0)
			invalid = true;
		if (initialized && prior_floor[node] != 0 && floor[node] < prior_floor[node])
			regressed = true;
	}
	if (invalid || regressed)
		ereport(
			LOG,
			(errmsg_internal("cluster membership floor diagnostic"),
			 errdetail("stage=%s invalid=%u regressed=%u "
					   "m0=%u/%llu<- %u/%llu "
					   "m1=%u/%llu<- %u/%llu "
					   "m2=%u/%llu<- %u/%llu "
					   "m3=%u/%llu<- %u/%llu",
					   stage, invalid ? 1U : 0U, regressed ? 1U : 0U, state[0],
					   (unsigned long long)floor[0], prior_state[0],
					   (unsigned long long)prior_floor[0], state[1], (unsigned long long)floor[1],
					   prior_state[1], (unsigned long long)prior_floor[1], state[2],
					   (unsigned long long)floor[2], prior_state[2],
					   (unsigned long long)prior_floor[2], state[3], (unsigned long long)floor[3],
					   prior_state[3], (unsigned long long)prior_floor[3])));
	memcpy(prior_floor, floor, sizeof(prior_floor));
	memcpy(prior_state, state, sizeof(prior_state));
	initialized = true;
}

/*
 * RF-ROOT P9 verification / cold-formation cold-formation ruling —
 * per-node cold-formation state machine (observation window -> arbiter
 * marker -> admission).  Declared here (before the membership tick that
 * consumes admission_done) so the self-state decision can keep the
 * marker-admitted node MEMBER instead of demoting it back to JOINING.
 */
typedef struct ClusterColdFormationState {
	bool observe_passed;
	uint64 observe_ticks;
	uint64 coboot_lo;
	uint64 coboot_hi;
	bool arbiter_submitted;
	uint64 arbiter_seq;
	bool admission_done;
} ClusterColdFormationState;

static ClusterColdFormationState cold_formation_state;
static uint64 cold_formation_commit_nonce = 0;

/*
 * Test-only reset (unit harness drives the state machine directly across
 * tests; in production the state is per-postmaster and never reset).
 * Mirrors the cluster_qvotec_test_* hook pattern.
 */
extern void cluster_reconfig_test_reset_cold_formation(void);
void
cluster_reconfig_test_reset_cold_formation(void)
{
	memset(&cold_formation_state, 0, sizeof(cold_formation_state));
	cold_formation_commit_nonce = 0;
}

typedef struct ClusterReconfigFenceStage {
	ClusterMarkerAsync async;
	ReconfigEvent event;
	ClusterFenceMarker marker;
	int32 node_id;
	uint64 last_incarnation;
	uint64 removal_event_id;
	bool submitted;
} ClusterReconfigFenceStage;

typedef struct ClusterReconfigJoinPrepareStage {
	ClusterMarkerAsync async;
	ReconfigEvent event;
	uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint64 joiner_incarnations[CLUSTER_MAX_NODES];
	int next_node;
	bool submitted;
} ClusterReconfigJoinPrepareStage;

typedef struct ClusterReconfigJoinCommitStage {
	ClusterMarkerAsync async;
	ClusterMarkerAsync fence_async;
	ClusterJoinCommitMarker marker;
	ClusterFenceMarker fence_marker;
	ReconfigEvent event;
	uint64 expected_last_event_id;
	int32 node_id;
	uint64 admitted_incarnation;
	bool submitted;
	bool fence_ready;
	bool external_rejoin_consumed;
} ClusterReconfigJoinCommitStage;

typedef enum ClusterExternalRejoinPhase {
	CLUSTER_EXTERNAL_REJOIN_EMPTY = 0,
	CLUSTER_EXTERNAL_REJOIN_OFFERED,
	CLUSTER_EXTERNAL_REJOIN_WAITING_ROOT,
	CLUSTER_EXTERNAL_REJOIN_WAITING_ON,
	CLUSTER_EXTERNAL_REJOIN_WAITING_JOINER,
	CLUSTER_EXTERNAL_REJOIN_WAITING_REFRESH,
	CLUSTER_EXTERNAL_REJOIN_READY,
	CLUSTER_EXTERNAL_REJOIN_COMMIT_READY,
	CLUSTER_EXTERNAL_REJOIN_COMMITTING
} ClusterExternalRejoinPhase;

/* STOP04 §11.8/§11.9: one opaque, process-local slot per old node.  Neither
 * the operation nor authority-clear object is portable across LMON exit. */
typedef struct ClusterExternalRejoinSlot {
	PgracExternalFenceRejoinOpV1 *op;
	PgracExternalFenceRejoinAuthorityClearV1 *authority_clear;
	ClusterReconfigRejoinFailureSnapshotV1 failure;
	ClusterReconfigRejoinPendingSnapshotV1 commit_pending;
	uint64 candidate_incarnation;
	ClusterExternalRejoinPhase phase;
} ClusterExternalRejoinSlot;

static ClusterReconfigFenceStage failstop_fence_stage;
static ClusterReconfigFenceStage node_removed_fence_stage;
static ClusterReconfigJoinPrepareStage join_prepare_stage;
static ClusterReconfigJoinCommitStage join_commit_stage;

/* RF-ROOT P04 A2 Scheme B.  This capability is deliberately process-local:
 * losing/restarting LMON invalidates it instead of reconstructing authority
 * from old shared state.  It carries one exact shared-CF rollover episode
 * through the existing two-phase JOIN transaction while ordinary serving is
 * closed; it is never a serving or general reconfiguration credential. */
typedef struct ClusterFastRejoinControlCapability {
	bool active;
	int32 target_node_id;
	uint64 target_incarnation;
	uint64 failstop_event_id;
	uint64 failstop_epoch;
	uint64 boot_incarnation;
	uint64 lms_generation;
} ClusterFastRejoinControlCapability;

static ClusterFastRejoinControlCapability fast_rejoin_control;
static ClusterExternalRejoinSlot external_rejoin_slots[CLUSTER_MAX_NODES];
static PgracExternalFenceRejoinOpV1 *external_rejoin_claim_op;
static bool external_rejoin_exit_registered;

typedef enum ClusterReplacementAdmitStagePhase {
	CLUSTER_REPLACEMENT_ADMIT_IDLE = 0,
	CLUSTER_REPLACEMENT_ADMIT_PRE_HEAD_WAIT,
	CLUSTER_REPLACEMENT_ADMIT_MARKER_SUBMIT,
	CLUSTER_REPLACEMENT_ADMIT_MARKER_WAIT,
	CLUSTER_REPLACEMENT_ADMIT_POST_HEAD_SUBMIT,
	CLUSTER_REPLACEMENT_ADMIT_POST_HEAD_WAIT
} ClusterReplacementAdmitStagePhase;

typedef struct ClusterReplacementAdmitStage {
	ClusterReplacementAdmitStagePhase phase;
	uint64 authority_request_seq;
	uint8 pre_head_value[CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES];
	uint8 pre_head_ballot[CLUSTER_QVOTEC_BALLOT_BYTES];
	ClusterReplacementCommitMarkerV3 admitted_marker;
	ClusterMarkerAsync marker_async;
} ClusterReplacementAdmitStage;

static ClusterReplacementAdmitStage replacement_admit_stage;

typedef enum ClusterReplacementClosedStagePhase {
	CLUSTER_REPLACEMENT_CLOSED_STAGE_IDLE = 0,
	CLUSTER_REPLACEMENT_CLOSED_STAGE_WAIT_PAIR,
	CLUSTER_REPLACEMENT_CLOSED_STAGE_DRAIN_AUTHORITY
} ClusterReplacementClosedStagePhase;

typedef struct ClusterReplacementClosedStage {
	ClusterReplacementClosedStagePhase phase;
	uint64 authority_request_seq;
	ClusterMarkerAsync marker_async;
	bool marker_completed;
} ClusterReplacementClosedStage;

typedef enum ClusterReplacementReadyStagePhase {
	CLUSTER_REPLACEMENT_READY_STAGE_IDLE = 0,
	CLUSTER_REPLACEMENT_READY_STAGE_WAIT_PAIR,
	CLUSTER_REPLACEMENT_READY_STAGE_CACHED,
	CLUSTER_REPLACEMENT_READY_STAGE_DRAIN_AUTHORITY
} ClusterReplacementReadyStagePhase;

typedef struct ClusterReplacementReadyStage {
	ClusterReplacementReadyStagePhase phase;
	uint64 authority_request_seq;
	uint64 marker_request_seq;
	ClusterMarkerAsync marker_async;
	bool marker_completed;
	ClusterR4PrerequisiteSnapshot cached_snapshot;
	uint8 cached_image[CLUSTER_REPLACEMENT_WIRE_BYTES];
} ClusterReplacementReadyStage;

typedef enum ClusterJoinMarkerLmonPurpose {
	CLUSTER_JOIN_MARKER_LMON_NONE = 0,
	CLUSTER_JOIN_MARKER_LMON_CLOSED_APPLY,
	CLUSTER_JOIN_MARKER_LMON_READY_SERIALIZE
} ClusterJoinMarkerLmonPurpose;

typedef struct ClusterJoinMarkerLmonOwner {
	ClusterJoinMarkerLmonPurpose purpose;
	ClusterJoinMarkerMailboxOperationV1 operation;
	uint64 marker_request_seq;
	bool reserved;
} ClusterJoinMarkerLmonOwner;

static ClusterReplacementClosedStage replacement_closed_stage;
static ClusterReplacementReadyStage replacement_ready_stage;
static ClusterJoinMarkerLmonOwner join_marker_lmon_owner;

typedef struct ReconfigStopObservation {
	ClusterNormalStopPollResult result;
	const char *domain;
	uint64 key;
} ReconfigStopObservation;

static void
reconfig_stop_note(ReconfigStopObservation *out, bool pending, bool invalid, const char *domain,
				   uint64 key)
{
	if (out->result == CLUSTER_NORMAL_STOP_INVALID || (!pending && !invalid))
		return;
	if (invalid || out->result == CLUSTER_NORMAL_STOP_READY) {
		out->result = invalid ? CLUSTER_NORMAL_STOP_INVALID : CLUSTER_NORMAL_STOP_PENDING;
		out->domain = domain;
		out->key = key;
	}
}

static void
reconfig_stop_mailbox(ReconfigStopObservation *out, const char *domain, pg_atomic_uint64 *requested,
					  pg_atomic_uint64 *completed)
{
	uint64 request = pg_atomic_read_u64(requested);
	uint64 completion = pg_atomic_read_u64(completed);
	bool changed;

	pg_read_barrier();
	changed
		= request != pg_atomic_read_u64(requested) || completion != pg_atomic_read_u64(completed);
	reconfig_stop_note(out, changed || request != completion,
					   !changed && (completion > request || request - completion > 1), domain,
					   request);
}

/* Original LMON responsibilities only. Stable OPEN/full formation identity,
 * other processes' private work and the producer seal are separate cuts.
 * No marker, exclusion, reservation or cached proof is reset by this poll. */
ClusterNormalStopPollResult
cluster_reconfig_normal_stop_poll(const char **domain_out, uint64 *key_out, const char **reason_out)
{
	ReconfigStopObservation out = { CLUSTER_NORMAL_STOP_READY, NULL, 0 };
	const struct {
		const char *domain;
		const ClusterMarkerAsync *async;
		bool submitted;
	} stages[]
		= { { "FAILSTOP_FENCE", &failstop_fence_stage.async, failstop_fence_stage.submitted },
			{ "NODE_REMOVED_FENCE", &node_removed_fence_stage.async,
			  node_removed_fence_stage.submitted },
			{ "JOIN_PREPARE", &join_prepare_stage.async, join_prepare_stage.submitted },
			{ "JOIN_COMMIT", &join_commit_stage.async, join_commit_stage.submitted },
			{ "JOIN_COMMIT_FENCE", &join_commit_stage.fence_async, join_commit_stage.fence_ready },
			{ "REPLACEMENT_ADMIT_MARKER", &replacement_admit_stage.marker_async, false },
			{ "REPLACEMENT_CLOSED_MARKER", &replacement_closed_stage.marker_async, false },
			{ "REPLACEMENT_READY_MARKER", &replacement_ready_stage.marker_async, false } };

	if (!IsUnderPostmaster || MyBackendType != B_LMON || ReconfigShmem == NULL) {
		reconfig_stop_note(&out, false, true, "RECONFIG_OWNER_OR_SHMEM", 0);
		goto done;
	}

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	{
		uint32 prebump = pg_atomic_read_u32(&ReconfigShmem->prebump_sync_active);
		const ClusterReplacementEpisode *episode = &ReconfigShmem->replacement_episode;
		bool empty = cluster_replacement_episode_is_empty(episode);

		reconfig_stop_note(&out, prebump != 0, prebump > 1, "RECONFIG_PREBUMP", 0);
		reconfig_stop_note(&out, false,
						   ReconfigShmem->self_join_failed != 0
							   || ReconfigShmem->self_join_admitted > 1,
						   "RECONFIG_SELF_JOIN", 0);
		for (int node = 0; node < CLUSTER_MAX_NODES; node++) {
			uint8 bit = (uint8)(1u << (node % 8));
			reconfig_stop_note(&out, (ReconfigShmem->pending_join_bitmap[node / 8] & bit) != 0,
							   false, "PENDING_JOIN", node);
			reconfig_stop_note(&out, (ReconfigShmem->fast_rejoin_bitmap[node / 8] & bit) != 0,
							   false, "FAST_REJOIN", node);
		}
		reconfig_stop_note(&out, !empty && episode->phase != CLUSTER_REPLACEMENT_EPISODE_ADMITTED,
						   !empty
							   && (!cluster_replacement_episode_is_valid(episode)
								   || episode->phase == CLUSTER_REPLACEMENT_EPISODE_HOLD),
						   "REPLACEMENT_EPISODE", episode->request_nonce);
		reconfig_stop_mailbox(&out, "JOIN_MAILBOX", &ReconfigShmem->join_marker_request_seq,
							  &ReconfigShmem->join_marker_completion_seq);
		reconfig_stop_mailbox(&out, "FORMATION_MAILBOX",
							  &ReconfigShmem->formation_marker_request_seq,
							  &ReconfigShmem->formation_marker_completion_seq);
	}
	LWLockRelease(&ReconfigShmem->lock);

	for (int i = 0; i < lengthof(stages); i++) {
		const ClusterMarkerAsync *a = stages[i].async;
		reconfig_stop_note(&out,
						   stages[i].submitted || a->has_staged_event
							   || a->state == CLUSTER_MARKER_ASYNC_SUBMITTED,
						   a->state != CLUSTER_MARKER_ASYNC_IDLE
							   && a->state != CLUSTER_MARKER_ASYNC_SUBMITTED,
						   stages[i].domain, a->inflight_seq);
	}
	/* Admission deliberately retains arbiter_submitted/seq as boot evidence.
	 * The marker's ACK alone is insufficient, but successful original admission
	 * ends that private responsibility without erasing the retained evidence. */
	reconfig_stop_note(
		&out,
		!cold_formation_state.admission_done
			&& (cold_formation_state.arbiter_submitted || cold_formation_state.observe_passed),
		false, "COLD_FORMATION", cold_formation_state.arbiter_seq);
	reconfig_stop_note(&out,
					   offpath_fast_rejoin_active_local || fast_rejoin_control.active
						   || join_commit_stage.external_rejoin_consumed
						   || external_rejoin_claim_op != NULL,
					   false, "EXTERNAL_REJOIN_CLAIM", 0);
	for (int node = 0; node < CLUSTER_MAX_NODES; node++) {
		const ClusterExternalRejoinSlot *slot = &external_rejoin_slots[node];
		reconfig_stop_note(&out,
						   slot->phase != CLUSTER_EXTERNAL_REJOIN_EMPTY || slot->op != NULL
							   || slot->authority_clear != NULL,
						   slot->phase < CLUSTER_EXTERNAL_REJOIN_EMPTY
							   || slot->phase > CLUSTER_EXTERNAL_REJOIN_COMMITTING,
						   "EXTERNAL_REJOIN_SLOT", node);
	}
	reconfig_stop_note(&out, replacement_admit_stage.phase != CLUSTER_REPLACEMENT_ADMIT_IDLE,
					   replacement_admit_stage.phase < CLUSTER_REPLACEMENT_ADMIT_IDLE
						   || replacement_admit_stage.phase
								  > CLUSTER_REPLACEMENT_ADMIT_POST_HEAD_WAIT,
					   "REPLACEMENT_ADMIT", replacement_admit_stage.authority_request_seq);
	reconfig_stop_note(
		&out, replacement_closed_stage.phase != CLUSTER_REPLACEMENT_CLOSED_STAGE_IDLE,
		replacement_closed_stage.phase < CLUSTER_REPLACEMENT_CLOSED_STAGE_IDLE
			|| replacement_closed_stage.phase > CLUSTER_REPLACEMENT_CLOSED_STAGE_DRAIN_AUTHORITY,
		"REPLACEMENT_CLOSED", replacement_closed_stage.authority_request_seq);
	reconfig_stop_note(
		&out,
		replacement_ready_stage.phase != CLUSTER_REPLACEMENT_READY_STAGE_IDLE
			&& replacement_ready_stage.phase != CLUSTER_REPLACEMENT_READY_STAGE_CACHED,
		replacement_ready_stage.phase < CLUSTER_REPLACEMENT_READY_STAGE_IDLE
			|| replacement_ready_stage.phase > CLUSTER_REPLACEMENT_READY_STAGE_DRAIN_AUTHORITY,
		"REPLACEMENT_READY", replacement_ready_stage.authority_request_seq);
	reconfig_stop_note(&out, join_marker_lmon_owner.reserved,
					   join_marker_lmon_owner.purpose < CLUSTER_JOIN_MARKER_LMON_NONE
						   || join_marker_lmon_owner.purpose
								  > CLUSTER_JOIN_MARKER_LMON_READY_SERIALIZE,
					   "JOIN_MARKER_OWNER", join_marker_lmon_owner.marker_request_seq);
done:
	if (domain_out != NULL)
		*domain_out = out.domain;
	if (key_out != NULL)
		*key_out = out.key;
	if (reason_out != NULL)
		*reason_out = out.result == CLUSTER_NORMAL_STOP_READY	  ? "RECONFIG_STOP_READY"
					  : out.result == CLUSTER_NORMAL_STOP_PENDING ? "RECONFIG_IN_PROGRESS"
																  : "RECONFIG_STATE_INVALID";
	return out.result;
}

static bool cluster_reconfig_join_marker_request_word_decode(
	uint32 word, ClusterJoinMarkerMailboxOperationV1 *operation_out, int32 *target_node_out);
static bool cluster_reconfig_stage_join_marker_locked(int32 target_node,
													  ClusterJoinMarkerMailboxOperationV1 operation,
													  uint32 version, const void *image,
													  Size image_len);
static bool cluster_reconfig_terminal_closed_matches_episode(
	const ClusterEpochAuthorityValue *head, const ClusterEpochBallotId *ballot,
	const ClusterReplacementCommitMarkerV3 *marker, const ClusterReplacementEpisode *episode);
static void cluster_reconfig_release_ready_stage(void);
static bool cluster_reconfig_lmon_submit_ready_observer_pair(TimestampTz now);

static bool cluster_reconfig_lmon_ready_cache_current(int32 *coordinator_node_id);


/* ============================================================
 * Shmem region lifecycle.
 * ============================================================
 */

Size
cluster_reconfig_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterReconfigState));
}


void
cluster_reconfig_shmem_init(void)
{
	bool found;

	ReconfigShmem = (ClusterReconfigState *)ShmemInitStruct("pgrac cluster reconfig",
															cluster_reconfig_shmem_size(), &found);

	if (!found) {
		/* First-time init — zero everything, then set up LWLock +
		 * never-applied sentinel (event_id=0, observer_role=NONE).
		 */
		memset(ReconfigShmem, 0, sizeof(ClusterReconfigState));
		LWLockInitialize(&ReconfigShmem->lock, LWTRANCHE_CLUSTER_RECONFIG);
		pg_atomic_init_u64(&ReconfigShmem->apply_counter, 0);
		pg_atomic_init_u64(&ReconfigShmem->dedup_skip_counter, 0);
		pg_atomic_init_u64(&ReconfigShmem->procsig_broadcast_count, 0);
		pg_atomic_init_u32(&ReconfigShmem->prebump_sync_active, 0); /* spec-2.29a r2 t/274 */
		/* spec-5.14 D6 — touched_peers observability counters. */
		pg_atomic_init_u64(&ReconfigShmem->touched_abort_count, 0);
		pg_atomic_init_u64(&ReconfigShmem->touched_stamp_count, 0);
		for (int k = 0; k < CLUSTER_RECONFIG_TOUCH_KIND_COUNT; k++)
			pg_atomic_init_u64(&ReconfigShmem->touched_stamp_by_kind[k], 0);
		pg_atomic_init_u64(&ReconfigShmem->clean_leave_rejected_count, 0);
		pg_atomic_init_u64(&ReconfigShmem->clean_leave_drain_grace_count, 0);
		/* spec-5.13 D3 — clean_departed_bitmap + clean_departed_epoch[] left
		 * zeroed by memset (no node departed); init the lifetime counter. */
		pg_atomic_init_u64(&ReconfigShmem->clean_departed_count, 0);
		/* spec-5.18 D3 — removed_bitmap + removed_epoch[] left zeroed by memset
		 * (no node removed); init the lifetime counter. */
		pg_atomic_init_u64(&ReconfigShmem->removed_count, 0);
		/* last_applied left zeroed by memset — event_id=0 =
		 * CLUSTER_RECONFIG_OBSERVER_NONE = never-applied sentinel. */

		/* spec-5.15 D2/D5 — membership table + pending_join_bitmap left zeroed by
		 * memset (all nodes CLUSTER_MEMBER_ABSENT, no pending join).
		 *
		 * Hardening v1.1 (HF-2): default the joiner write gate CLOSED when
		 * online_join is on (fail-closed — a freshly-booted node must PROVE it is
		 * a cold-bootstrap member or be admitted before it may write; this closes
		 * the boot-to-first-LMON-tick fail-open window, P1-2).  When online_join
		 * is off the gate is open: no online membership gating, so bootstrap and
		 * steady-state writes are unaffected. */
		ReconfigShmem->self_join_admitted = cluster_online_join ? 0 : 1;

		/*
		 * spec-5.16 D6 — startup invariant (postmaster-once, in the !found shmem
		 * init).  The joiner-home PCM block snap-back fence is bound to
		 * cluster.online_join (forced correctness, INV-R13), NOT to
		 * cluster.join_remaster_enabled: online_join=on therefore ALWAYS arms the
		 * fence (note_self_admitted, before opening the joiner write gate), so
		 * there is no config that leaves the block hazard open.  join_remaster_
		 * enabled only gates the OPTIONAL GRD logical-lock move.  Log the
		 * meaningless combo (rebalance on but joins off) so it is not mistaken
		 * for an active feature. */
		if (cluster_enabled && cluster_join_remaster_enabled && !cluster_online_join)
			ereport(LOG,
					(errmsg("cluster.join_remaster_enabled is on but cluster.online_join is off; "
							"no node will rejoin, so the GRD logical-lock rebalance never runs"),
					 errhint("Enable cluster.online_join to allow online node rejoin.")));

		/* spec-5.15 D1/D4 — no slot observed yet (generation 0 = absent/not ready). */
		for (int n = 0; n < CLUSTER_MAX_NODES; n++) {
			pg_atomic_init_u64(&ReconfigShmem->observed_incarnation[n], 0);
			pg_atomic_init_u64(&ReconfigShmem->observed_generation[n], 0);
			pg_atomic_init_u64(&ReconfigShmem->observed_epoch[n], 0);
			/* spec-5.16 — no peer COMMITTED join marker observed yet. */
			pg_atomic_init_u64(&ReconfigShmem->observed_committed_join_incarnation[n], 0);
			pg_atomic_init_u64(&ReconfigShmem->observed_committed_join_epoch[n], 0);
		}
		/* RF-ROOT P9 verification (cold-formation): bootstrap publication
		 * seqlock starts even (stable, seq 0) + in-quorum snapshot closed. */
		pg_atomic_init_u64(&ReconfigShmem->observed_bootstrap_seq, 0);
		pg_atomic_init_u64(&ReconfigShmem->bootstrap_in_quorum, 0);

		/* RF-ROOT P9 verification (cold-formation): cold-formation marker
		 * mailbox + qvotec observations start idle/absent. */
		ReconfigShmem->formation_qvotec_latch = NULL;
		pg_atomic_init_u64(&ReconfigShmem->formation_marker_request_seq, 0);
		pg_atomic_init_u64(&ReconfigShmem->formation_marker_completion_seq, 0);
		pg_atomic_init_u32(&ReconfigShmem->formation_marker_result, 0);
		pg_atomic_init_u64(&ReconfigShmem->formation_marker_max_generation, 0);
		pg_atomic_init_u64(&ReconfigShmem->observed_formation_marker_generation, 0);
		pg_atomic_init_u64(&ReconfigShmem->observed_formation_marker_epoch, 0);
		pg_atomic_init_u64(&ReconfigShmem->observed_formation_marker_arbiter_node, 0);
		pg_atomic_init_u64(&ReconfigShmem->observed_formation_marker_arbiter_incarnation, 0);
		for (int n = 0; n < CLUSTER_MAX_NODES; n++)
			pg_atomic_init_u64(&ReconfigShmem->observed_formation_marker_incarnation[n], 0);

		/* spec-5.15 D4 — join-marker submit mailbox (latch published by qvotec). */
		ReconfigShmem->join_qvotec_latch = NULL;
		pg_atomic_init_u64(&ReconfigShmem->join_marker_request_seq, 0);
		pg_atomic_init_u64(&ReconfigShmem->join_marker_completion_seq, 0);
		pg_atomic_init_u32(&ReconfigShmem->join_marker_result, CLUSTER_JOIN_MARKER_SUBMIT_FAILED);
		ReconfigShmem->join_marker_request_word = 0;
		/* join_pending_marker left zeroed by memset. */

		/* spec-5.15 D6 — online-join observability counters. */
		pg_atomic_init_u64(&ReconfigShmem->join_pending_count, 0);
		pg_atomic_init_u64(&ReconfigShmem->join_apply_count, 0);
		pg_atomic_init_u64(&ReconfigShmem->join_reject_count, 0);
		pg_atomic_init_u64(&ReconfigShmem->join_timeout_count, 0);
		pg_atomic_init_u64(&ReconfigShmem->clean_departed_cleared_count, 0);
		pg_atomic_init_u64(&ReconfigShmem->marker_slow_ack_count, 0);
		pg_atomic_init_u64(&ReconfigShmem->marker_timeout_count, 0);
	}

	/*
	 * spec-5.15 D2 — every backend points the cluster_membership accessors at the
	 * shared table in this region (outside the !found block: EXEC_BACKEND children
	 * re-attach their process-local pointer to the inherited shmem).
	 */
	cluster_membership_attach(&ReconfigShmem->membership);
}


static const ClusterShmemRegion cluster_reconfig_region = {
	.name = "pgrac cluster reconfig",
	.size_fn = cluster_reconfig_shmem_size,
	.init_fn = cluster_reconfig_shmem_init,
	.lwlock_count = 1, /* single LWLock guarding last_applied publish */
	.owner_subsys = "cluster_reconfig",
	.reserved_flags = 0,
};


void
cluster_reconfig_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_reconfig_region);
}


/* ============================================================
 * Observability accessor — always-1-row contract (P2.9).
 *
 *	Caller (Step 3 D5b SRF entry) MUST always return 1 row to
 *	pg_cluster_reconfig_state regardless of never-applied state.
 *	This helper populates *out unconditionally;event_id=0 +
 *	observer_role=CLUSTER_RECONFIG_OBSERVER_NONE means never applied.
 * ============================================================
 */

void
cluster_reconfig_get_last_event(ReconfigEvent *out)
{
	Assert(out != NULL);

	if (ReconfigShmem == NULL) {
		/* Defense: shmem not initialized (e.g. cluster.enabled=off
		 * path or pre-postmaster).  Caller still gets a well-defined
		 * never-applied state. */
		memset(out, 0, sizeof(ReconfigEvent));
		return;
	}

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	memcpy(out, &ReconfigShmem->last_applied, sizeof(ReconfigEvent));
	LWLockRelease(&ReconfigShmem->lock);
}

bool
cluster_reconfig_capture_formation_snapshot_v1(uint16 origin_thread,
											   ClusterFormationSnapshotV1 *out)
{
	const ReconfigEvent *src;
	int32 origin_node;
	int i;

	if (out == NULL || origin_thread == 0 || origin_thread > CLUSTER_MAX_NODES
		|| ReconfigShmem == NULL)
		return false;
	origin_node = (int32)origin_thread - 1;
	memset(out, 0, sizeof(*out));
	/* A1: Postmaster drives phase 3 before StartupProcess exists, so it has
	 * no PGPROC with which LWLockAcquire could queue.  Preserve blocking
	 * snapshot semantics for ordinary processes; the no-PGPROC caller may
	 * only take an immediately available shared lock and lets the existing
	 * phase-3 deadline loop retry contention. */
	if (MyProc == NULL) {
		if (!LWLockConditionalAcquire(&ReconfigShmem->lock, LW_SHARED)) {
			return false;
		}
	} else
		LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	src = &ReconfigShmem->last_applied;
	out->applied.event_id = src->event_id;
	out->applied.coordinator_node_id = src->coordinator_node_id;
	out->applied.old_epoch = src->old_epoch;
	out->applied.new_epoch = src->new_epoch;
	memcpy(out->applied.dead_bitmap, src->dead_bitmap, sizeof(out->applied.dead_bitmap));
	out->applied.applied_at = src->applied_at;
	out->applied.observer_role = src->observer_role;
	out->applied.event_seq = src->event_seq;
	out->applied.cssd_dead_generation = src->cssd_dead_generation;
	out->applied.reconfig_kind = src->reconfig_kind;
	memcpy(out->applied.join_bitmap, src->join_bitmap, sizeof(out->applied.join_bitmap));
	out->membership = ReconfigShmem->membership;
	memcpy(out->pending_join_bitmap, ReconfigShmem->pending_join_bitmap,
		   sizeof(out->pending_join_bitmap));
	memcpy(out->clean_departed_bitmap, ReconfigShmem->clean_departed_bitmap,
		   sizeof(out->clean_departed_bitmap));
	memcpy(out->removed_bitmap, ReconfigShmem->removed_bitmap, sizeof(out->removed_bitmap));
	for (i = 0; i < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; i++)
		out->excluded_bitmap[i] = src->dead_bitmap[i] | ReconfigShmem->removed_bitmap[i];
	out->victim_incarnation = ReconfigShmem->membership.last_admitted_incarnation[origin_node];
	out->prebump_sync_active = pg_atomic_read_u32(&ReconfigShmem->prebump_sync_active);
	out->self_join_admitted = ReconfigShmem->self_join_admitted;
	out->self_join_failed = ReconfigShmem->self_join_failed;
	LWLockRelease(&ReconfigShmem->lock);
	out->local_epoch = cluster_epoch_get_current();
	return true;
}

static void
cluster_reconfig_set_prebump_sync_active(uint32 value)
{
	uint64 cache_mutation;

	if (ReconfigShmem == NULL || pg_atomic_read_u32(&ReconfigShmem->prebump_sync_active) == value)
		return;
	cache_mutation = cluster_write_fence_authority_cache_mutation_begin();
	pg_atomic_write_u32(&ReconfigShmem->prebump_sync_active, value);
	cluster_write_fence_authority_cache_mutation_end(cache_mutation);
}


/* ============================================================
 * Internal publish helper.
 *
 *	Per L23 lesson (compound atomic + counter inc must share same
 *	critical section): apply_counter increment + last_applied copy
 *	both happen inside the LWLock-exclusive window so that
 *	concurrent SRF reads see a consistent snapshot — never see
 *	apply_counter > last_applied.event_seq.
 * ============================================================
 */

void
cluster_reconfig_publish_event(const ReconfigEvent *evt)
{
	ReconfigEvent published;
	uint64 event_seq;

	Assert(evt != NULL);

	if (ReconfigShmem == NULL)
		return;

	memcpy(&published, evt, sizeof(ReconfigEvent));

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	cluster_write_fence_authority_cache_invalidate();
	event_seq = pg_atomic_fetch_add_u64(&ReconfigShmem->apply_counter, 1) + 1;
	published.event_seq = event_seq;
	memcpy(&ReconfigShmem->last_applied, &published, sizeof(ReconfigEvent));
	LWLockRelease(&ReconfigShmem->lock);

	elog(DEBUG1,
		 "cluster_reconfig: event %lu applied (coord=%d old=%lu new=%lu role=%d dead_gen=%lu)",
		 (unsigned long)published.event_id, published.coordinator_node_id,
		 (unsigned long)published.old_epoch, (unsigned long)published.new_epoch,
		 published.observer_role, (unsigned long)published.cssd_dead_generation);
}


static void
cluster_reconfig_log_failstop_epoch_bump(const ReconfigEvent *evt)
{
	char dead[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES * 8 * 4 + 1];
	int off = 0;
	int n;

	Assert(evt != NULL);

	dead[0] = '\0';
	for (n = 0; n < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES * 8; n++) {
		if (evt->dead_bitmap[n / 8] & (1 << (n % 8)))
			off += snprintf(dead + off, sizeof(dead) - off, "%s%d", off ? "," : "", n);
	}

	ereport(LOG, (errmsg("cluster reconfig: fail-stop epoch bump %llu -> %llu published "
						 "(coordinator node %d, dead node(s) {%s})",
						 (unsigned long long)evt->old_epoch, (unsigned long long)evt->new_epoch,
						 (int)evt->coordinator_node_id, dead)));
}


/* ============================================================
 * Counter accessors (Step 2 + Step 3 SRF support).
 * ============================================================
 */

uint64
cluster_reconfig_get_apply_counter(void)
{
	if (ReconfigShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->apply_counter);
}

uint64
cluster_reconfig_get_dedup_skip_counter(void)
{
	if (ReconfigShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->dedup_skip_counter);
}

uint64
cluster_reconfig_get_procsig_broadcast_count(void)
{
	if (ReconfigShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->procsig_broadcast_count);
}


/* ============================================================
 * spec-5.14 D6 — touched_peers observability counter mutators + getters.
 *
 *	  Called from the hot cross-node ingress path (note_touched_stamp,
 *	  via cluster_touched_peers_stamp) and the D4 dispatch
 *	  (note_touched_abort / note_clean_leave_rejected).  All atomic, no
 *	  lock, never ereport (L213).
 * ============================================================
 */
void
cluster_reconfig_note_touched_stamp(int kind)
{
	if (ReconfigShmem == NULL)
		return;
	pg_atomic_fetch_add_u64(&ReconfigShmem->touched_stamp_count, 1);
	if (kind >= 0 && kind < CLUSTER_RECONFIG_TOUCH_KIND_COUNT)
		pg_atomic_fetch_add_u64(&ReconfigShmem->touched_stamp_by_kind[kind], 1);
}

uint64
cluster_reconfig_note_touched_abort(void)
{
	if (ReconfigShmem == NULL)
		return 1; /* pretend non-zero so caller skips the LOG-once */
	return pg_atomic_fetch_add_u64(&ReconfigShmem->touched_abort_count, 1);
}

void
cluster_reconfig_note_clean_leave_rejected(void)
{
	if (ReconfigShmem == NULL)
		return;
	pg_atomic_fetch_add_u64(&ReconfigShmem->clean_leave_rejected_count, 1);
}

uint64
cluster_reconfig_note_clean_leave_drain_grace(void)
{
	if (ReconfigShmem == NULL)
		return 1; /* pretend non-zero so the caller skips the LOG-once */
	return pg_atomic_fetch_add_u64(&ReconfigShmem->clean_leave_drain_grace_count, 1);
}

uint64
cluster_reconfig_get_touched_abort_count(void)
{
	if (ReconfigShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->touched_abort_count);
}

uint64
cluster_reconfig_get_touched_stamp_count(void)
{
	if (ReconfigShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->touched_stamp_count);
}

uint64
cluster_reconfig_get_touched_stamp_by_kind(int kind)
{
	if (ReconfigShmem == NULL || kind < 0 || kind >= CLUSTER_RECONFIG_TOUCH_KIND_COUNT)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->touched_stamp_by_kind[kind]);
}

uint64
cluster_reconfig_get_clean_leave_rejected_count(void)
{
	if (ReconfigShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->clean_leave_rejected_count);
}

uint64
cluster_reconfig_get_clean_leave_drain_grace_count(void)
{
	if (ReconfigShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->clean_leave_drain_grace_count);
}


/* ============================================================
 * Step 2 internal helpers.
 * ============================================================
 */

/*
 * Set / test bit i (0-based) in a 128-bit bitmap stored as uint8[16].
 * Bit i is byte (i/8) bit (i%8).  Little-endian byte order (consistent
 * with hex serialization in pg_cluster_reconfig_state.dead_bitmap).
 */
static inline void
dead_bitmap_set_bit(uint8 *bmp, int i)
{
	Assert(i >= 0 && i < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES * 8);
	bmp[i / 8] |= (uint8)(1u << (i % 8));
}


static inline bool
dead_bitmap_test_bit(const uint8 *bmp, int i)
{
	Assert(i >= 0 && i < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES * 8);
	return (bmp[i / 8] & (uint8)(1u << (i % 8))) != 0;
}


static void
replacement_event_id_put_le32(uint8 *out, uint32 value)
{
	int i;

	for (i = 0; i < 4; i++)
		out[i] = (uint8)(value >> (i * 8));
}


static void
replacement_event_id_put_le64(uint8 *out, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++)
		out[i] = (uint8)(value >> (i * 8));
}


bool
cluster_reconfig_build_replacement_committed_event(const ClusterReplacementEpisode *episode,
												   int32 observer_role, TimestampTz applied_at,
												   ReconfigEvent *out_event)
{
	uint8 hash_input[1 + 4 + 8 + 8 + 8 + 8 + 8 + CLUSTER_RECONFIG_DEAD_BITMAP_BYTES + 8];
	ReconfigEvent event;
	Size off = 0;

	if (episode == NULL || out_event == NULL || !cluster_replacement_episode_is_valid(episode)
		|| (episode->phase != CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED
			&& episode->phase != CLUSTER_REPLACEMENT_EPISODE_POST_EPOCH)
		|| memcmp(episode->acknowledgements, episode->expected_survivors,
				  sizeof(episode->acknowledgements))
			   != 0
		|| (observer_role != CLUSTER_RECONFIG_OBSERVER_COORDINATOR
			&& observer_role != CLUSTER_RECONFIG_OBSERVER_SURVIVOR))
		return false;

	/* Spec-5.15A §2.4 exact identity, encoded explicitly so identical
	 * observers hash identical bytes independent of host padding/layout. */
	hash_input[off++] = RECONFIG_KIND_REPLACEMENT_COMMITTED;
	replacement_event_id_put_le32(hash_input + off, (uint32)episode->target_node_id);
	off += 4;
	replacement_event_id_put_le64(hash_input + off, episode->baseline_epoch);
	off += 8;
	replacement_event_id_put_le64(hash_input + off, episode->reserved_or_committed_epoch);
	off += 8;
	replacement_event_id_put_le64(hash_input + off, episode->old_admitted_incarnation);
	off += 8;
	replacement_event_id_put_le64(hash_input + off, episode->fresh_incarnation);
	off += 8;
	replacement_event_id_put_le64(hash_input + off, episode->request_nonce);
	off += 8;
	memcpy(hash_input + off, episode->expected_survivors, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	off += CLUSTER_RECONFIG_DEAD_BITMAP_BYTES;
	replacement_event_id_put_le64(hash_input + off, episode->grammar_fingerprint);
	off += 8;
	Assert(off == sizeof(hash_input));

	memset(&event, 0, sizeof(event));
	event.event_id = hash_bytes_extended(hash_input, sizeof(hash_input), 0);
	if (event.event_id == 0)
		return false;
	event.coordinator_node_id = episode->coordinator_node_id;
	event.old_epoch = episode->baseline_epoch;
	event.new_epoch = episode->reserved_or_committed_epoch;
	event.applied_at = applied_at;
	event.observer_role = observer_role;
	event.reconfig_kind = RECONFIG_KIND_REPLACEMENT_COMMITTED;
	dead_bitmap_set_bit(event.join_bitmap, episode->target_node_id);
	*out_event = event;
	return true;
}


bool
cluster_reconfig_replacement_grd_basis_authorized(
	const ReconfigEvent *event, const ClusterReplacementEpisode *episode,
	const ClusterReplacementCommitMarkerV3 *committed_marker, int32 local_node_id,
	uint8 out_survivors[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES], uint64 *out_epoch)
{
	ReconfigEvent expected_event;
	uint8 marker_image[CLUSTER_JCMK_REPLACEMENT_BYTES];
	int32 expected_role;

	if (event == NULL || episode == NULL || committed_marker == NULL || out_survivors == NULL
		|| out_epoch == NULL || local_node_id < 0 || local_node_id >= CLUSTER_MAX_NODES
		|| !cluster_replacement_episode_is_valid(episode)
		|| (episode->phase != CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED
			&& episode->phase != CLUSTER_REPLACEMENT_EPISODE_POST_EPOCH)
		|| memcmp(episode->acknowledgements, episode->expected_survivors,
				  sizeof(episode->acknowledgements))
			   != 0
		|| local_node_id == episode->target_node_id
		|| !dead_bitmap_test_bit(episode->expected_survivors, local_node_id)
		|| !cluster_replacement_marker_v3_encode(committed_marker, marker_image)
		|| committed_marker->phase != CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED
		|| committed_marker->ready_state_generation != 0
		|| committed_marker->target_node_id != episode->target_node_id
		|| committed_marker->old_admitted_incarnation != episode->old_admitted_incarnation
		|| committed_marker->fresh_incarnation != episode->fresh_incarnation
		|| committed_marker->baseline_epoch != episode->baseline_epoch
		|| committed_marker->reserved_or_committed_epoch != episode->reserved_or_committed_epoch
		|| committed_marker->request_nonce != episode->request_nonce
		|| memcmp(committed_marker->expected_purge_survivors, episode->expected_survivors,
				  sizeof(episode->expected_survivors))
			   != 0
		|| committed_marker->grammar_fingerprint != episode->grammar_fingerprint)
		return false;

	expected_role = local_node_id == episode->coordinator_node_id
						? CLUSTER_RECONFIG_OBSERVER_COORDINATOR
						: CLUSTER_RECONFIG_OBSERVER_SURVIVOR;
	if (event->event_seq == 0 || event->observer_role != expected_role
		|| !cluster_reconfig_build_replacement_committed_event(episode, expected_role,
															   event->applied_at, &expected_event))
		return false;
	expected_event.event_seq = event->event_seq;
	if (memcmp(event, &expected_event, sizeof(*event)) != 0)
		return false;

	memcpy(out_survivors, episode->expected_survivors, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	*out_epoch = episode->reserved_or_committed_epoch;
	return true;
}


bool
cluster_reconfig_lmon_snapshot_replacement_grd_basis(
	uint8 out_survivors[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES], uint64 *out_epoch)
{
	ClusterReplacementEpisode episode;
	ClusterReplacementCommitMarkerV3 committed;
	ReconfigEvent event;
	uint8 survivors[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint64 epoch;
	uint64 request_seq;
	uint64 completion_seq;
	bool valid = false;

	if (ReconfigShmem == NULL || out_survivors == NULL || out_epoch == NULL || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES)
		return false;

	request_seq = pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq);
	completion_seq = pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq);
	if (request_seq == 0 || request_seq != completion_seq
		|| pg_atomic_read_u32(&ReconfigShmem->join_marker_result) != CLUSTER_JOIN_MARKER_SUBMIT_ACK)
		return false;
	pg_read_barrier();

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	episode = ReconfigShmem->replacement_episode;
	event = ReconfigShmem->last_applied;
	if (pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq) != request_seq
		|| pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq) != completion_seq
		|| pg_atomic_read_u32(&ReconfigShmem->join_marker_result) != CLUSTER_JOIN_MARKER_SUBMIT_ACK
		|| episode.reserved_or_committed_epoch != cluster_epoch_get_current()
		|| !cluster_replacement_marker_v3_decode(ReconfigShmem->join_pending_marker,
												 episode.target_node_id, &committed)
		|| !cluster_reconfig_replacement_grd_basis_authorized(&event, &episode, &committed,
															  cluster_node_id, survivors, &epoch))
		goto out;
	valid = true;

out:
	LWLockRelease(&ReconfigShmem->lock);
	if (!valid)
		return false;
	memcpy(out_survivors, survivors, sizeof(survivors));
	*out_epoch = epoch;
	return true;
}


/*
 * Snapshot the exact replacement episode, then verify the Candidate2 grammar
 * bit on every remote participant while holding no reconfiguration lock.  A
 * caller must exact-compare the returned snapshot after reacquiring that lock
 * before deriving or publishing ADMITTED state.
 */
static bool
cluster_reconfig_replacement_candidate2_capabilities_current(ClusterReplacementEpisode *out_episode)
{
	ClusterReplacementEpisode snapshot;
	int node;

	if (ReconfigShmem == NULL || out_episode == NULL || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES)
		return false;

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	snapshot = ReconfigShmem->replacement_episode;
	LWLockRelease(&ReconfigShmem->lock);
	if (!cluster_replacement_episode_is_valid(&snapshot))
		return false;

	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		bool required = node == snapshot.target_node_id
						|| dead_bitmap_test_bit(snapshot.expected_survivors, node);

		/* The running coordinator binary is its own compiled proof. */
		if (!required || node == cluster_node_id)
			continue;
		if (!cluster_sf_peer_capability_family_sample(
				node, PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1, 0, NULL, NULL))
			return false;
	}

	*out_episode = snapshot;
	return true;
}


/* Caller holds ReconfigShmem->lock.  Before ADMITTED, the immutable survivor
 * bitmap is the complete live MEMBER set and the fenced target is still out. */
static bool
cluster_reconfig_replacement_membership_current_locked(const ClusterReplacementEpisode *episode)
{
	int node;

	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		bool expected_member = dead_bitmap_test_bit(episode->expected_survivors, node);

		if (cluster_membership_is_member(node) != expected_member)
			return false;
	}
	return true;
}


/*
 * Spec-5.15A §2.3: validate a decoded PURGE_REQUEST against authority that
 * the caller has already recovered from the common ballot and voting-disk
 * marker.  This is intentionally a pre-mutation predicate: it neither treats
 * these host-order inputs as proof of durability nor publishes the survivor
 * fence.  The eventual ingress owner must supply the recovered objects and
 * recheck this predicate immediately before its separately specified fence.
 */
bool
cluster_reconfig_replacement_purge_request_authorized(
	const ClusterReplacementWireMessage *request, int32 authenticated_source_node_id,
	int32 local_receiver_node_id, const ClusterEpochAuthorityValue *settled_reserve,
	const ClusterEpochBallotId *settled_ballot,
	const ClusterReplacementCommitMarkerV3 *durable_prepare)
{
	ClusterReplacementEpisode capability_episode;
	ClusterReplacementEpisode *episode;
	uint8 prepare_image[CLUSTER_JCMK_REPLACEMENT_BYTES];
	uint8 subject[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint8 zero_acks[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	bool authorized = false;

	if (ReconfigShmem == NULL || request == NULL || settled_reserve == NULL
		|| settled_ballot == NULL || durable_prepare == NULL || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES || local_receiver_node_id != cluster_node_id
		|| authenticated_source_node_id < 0 || authenticated_source_node_id >= CLUSTER_MAX_NODES
		|| authenticated_source_node_id == local_receiver_node_id
		|| request->phase != CLUSTER_REPLACEMENT_WIRE_PHASE_PURGE_REQUEST
		|| request->grammar_fingerprint != CLUSTER_REPLACEMENT_EPISODE_GRAMMAR_FINGERPRINT
		|| !cluster_epoch_authority_value_is_valid(settled_reserve,
												   CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT)
		|| settled_reserve->transition != CLUSTER_EPOCH_AUTHORITY_RESERVE
		|| settled_reserve->event_kind != CLUSTER_EPOCH_EVENT_SAME_NODE_REPLACEMENT
		|| !cluster_epoch_ballot_id_is_valid(settled_ballot)
		|| settled_ballot->proposer_node_id != authenticated_source_node_id
		|| !cluster_replacement_marker_v3_encode(durable_prepare, prepare_image)
		|| durable_prepare->phase != CLUSTER_JCMK_REPLACEMENT_PHASE_PREPARE
		|| durable_prepare->ready_state_generation != 0)
		return false;

	if (!cluster_reconfig_replacement_candidate2_capabilities_current(&capability_episode))
		return false;

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	episode = &ReconfigShmem->replacement_episode;
	if (episode->target_node_id >= 0 && episode->target_node_id < CLUSTER_MAX_NODES)
		subject[episode->target_node_id / 8] = (uint8)(1u << (episode->target_node_id % 8));
	if (memcmp(episode, &capability_episode, sizeof(*episode)) != 0
		|| !cluster_replacement_episode_is_valid(episode)
		|| (episode->phase != CLUSTER_REPLACEMENT_EPISODE_PREPARE_DURABLE
			&& episode->phase != CLUSTER_REPLACEMENT_EPISODE_PURGING)
		|| episode->readiness_flags != 0
		|| memcmp(episode->acknowledgements, zero_acks, sizeof(zero_acks)) != 0
		|| episode->coordinator_node_id != authenticated_source_node_id
		|| !dead_bitmap_test_bit(episode->expected_survivors, authenticated_source_node_id)
		|| !dead_bitmap_test_bit(episode->expected_survivors, local_receiver_node_id)
		|| !cluster_reconfig_replacement_membership_current_locked(episode)
		|| cluster_epoch_get_current() != episode->baseline_epoch
		|| cluster_membership_get_last_admitted_incarnation(authenticated_source_node_id)
			   != settled_ballot->proposer_admitted_incarnation
		|| cluster_membership_get_last_admitted_incarnation(episode->target_node_id)
			   != episode->old_admitted_incarnation
		|| request->target_node_id != episode->target_node_id
		|| request->epoch != episode->baseline_epoch
		|| request->request_nonce != episode->request_nonce
		|| request->identity0 != episode->old_admitted_incarnation
		|| request->identity1 != episode->fresh_incarnation
		|| memcmp(request->body.bitmap, episode->expected_survivors, sizeof(request->body.bitmap))
			   != 0
		|| request->grammar_fingerprint != episode->grammar_fingerprint
		|| settled_reserve->request_origin_node != episode->target_node_id
		|| settled_reserve->target_node_id != episode->target_node_id
		|| settled_reserve->baseline_epoch != episode->baseline_epoch
		|| settled_reserve->reserved_epoch != episode->reserved_or_committed_epoch
		|| settled_reserve->old_incarnation != episode->old_admitted_incarnation
		|| settled_reserve->fresh_incarnation != episode->fresh_incarnation
		|| settled_reserve->request_nonce != episode->request_nonce
		|| memcmp(settled_reserve->authority_member_bitmap, episode->expected_survivors,
				  sizeof(settled_reserve->authority_member_bitmap))
			   != 0
		|| memcmp(settled_reserve->event_subject_bitmap, subject, sizeof(subject)) != 0
		|| settled_reserve->grammar_fingerprint != episode->grammar_fingerprint
		|| durable_prepare->target_node_id != episode->target_node_id
		|| durable_prepare->old_admitted_incarnation != episode->old_admitted_incarnation
		|| durable_prepare->fresh_incarnation != episode->fresh_incarnation
		|| durable_prepare->baseline_epoch != episode->baseline_epoch
		|| durable_prepare->reserved_or_committed_epoch != episode->reserved_or_committed_epoch
		|| durable_prepare->request_nonce != episode->request_nonce
		|| memcmp(durable_prepare->expected_purge_survivors, episode->expected_survivors,
				  sizeof(durable_prepare->expected_purge_survivors))
			   != 0
		|| durable_prepare->grammar_fingerprint != episode->grammar_fingerprint)
		goto out;

	authorized = true;
out:
	LWLockRelease(&ReconfigShmem->lock);
	return authorized;
}


/* Authenticated CONTROL-envelope adapter for the pure phase-1 gate above.
 * Decode into private storage and publish the caller's observation only after
 * every outer and inner identity check succeeds. */
bool
cluster_reconfig_replacement_purge_request_ingress_authorized(
	const ClusterICEnvelope *env, const void *payload, uint32 payload_length,
	int32 authenticated_source_node_id, int32 local_receiver_node_id,
	const ClusterEpochAuthorityValue *settled_reserve, const ClusterEpochBallotId *settled_ballot,
	const ClusterReplacementCommitMarkerV3 *durable_prepare,
	ClusterReplacementWireMessage *out_request)
{
	ClusterReplacementWireMessage request;

	if (env == NULL || payload == NULL || out_request == NULL
		|| payload_length != CLUSTER_REPLACEMENT_WIRE_BYTES
		|| env->msg_type != PGRAC_IC_MSG_GES_REQUEST
		|| env->payload_length != CLUSTER_REPLACEMENT_WIRE_BYTES || authenticated_source_node_id < 0
		|| authenticated_source_node_id >= CLUSTER_MAX_NODES || local_receiver_node_id < 0
		|| local_receiver_node_id >= CLUSTER_MAX_NODES
		|| authenticated_source_node_id == local_receiver_node_id
		|| env->source_node_id != (uint32)authenticated_source_node_id
		|| env->dest_node_id != (uint32)local_receiver_node_id
		|| env->epoch != cluster_epoch_get_current()
		|| !cluster_replacement_wire_decode((const uint8 *)payload, &request)
		|| request.epoch != env->epoch
		|| !cluster_reconfig_replacement_purge_request_authorized(
			&request, authenticated_source_node_id, local_receiver_node_id, settled_reserve,
			settled_ballot, durable_prepare))
		return false;
	*out_request = request;
	return true;
}


/*
 * Phase-2 counterpart of the request gate.  The ACK node comes only from the
 * authenticated CONTROL endpoint; body.bitmap remains episode identity and
 * cannot self-select a bit.  This predicate deliberately accepts an already-
 * present bit as an idempotent candidate and never mutates the episode.
 */
bool
cluster_reconfig_replacement_purge_ack_authorized(
	const ClusterReplacementWireMessage *ack, int32 authenticated_source_node_id,
	int32 local_receiver_node_id, const ClusterEpochAuthorityValue *settled_reserve,
	const ClusterEpochBallotId *settled_ballot,
	const ClusterReplacementCommitMarkerV3 *durable_prepare, int32 *out_ack_node_id)
{
	ClusterReplacementEpisode capability_episode;
	ClusterReplacementEpisode *episode;
	uint8 prepare_image[CLUSTER_JCMK_REPLACEMENT_BYTES];
	uint8 subject[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	bool authorized = false;

	if (ReconfigShmem == NULL || ack == NULL || settled_reserve == NULL || settled_ballot == NULL
		|| durable_prepare == NULL || out_ack_node_id == NULL || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES || local_receiver_node_id != cluster_node_id
		|| authenticated_source_node_id < 0 || authenticated_source_node_id >= CLUSTER_MAX_NODES
		|| authenticated_source_node_id == local_receiver_node_id
		|| ack->phase != CLUSTER_REPLACEMENT_WIRE_PHASE_PURGE_ACK
		|| ack->grammar_fingerprint != CLUSTER_REPLACEMENT_EPISODE_GRAMMAR_FINGERPRINT
		|| !cluster_epoch_authority_value_is_valid(settled_reserve,
												   CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT)
		|| settled_reserve->transition != CLUSTER_EPOCH_AUTHORITY_RESERVE
		|| settled_reserve->event_kind != CLUSTER_EPOCH_EVENT_SAME_NODE_REPLACEMENT
		|| !cluster_epoch_ballot_id_is_valid(settled_ballot)
		|| settled_ballot->proposer_node_id != local_receiver_node_id
		|| !cluster_replacement_marker_v3_encode(durable_prepare, prepare_image)
		|| durable_prepare->phase != CLUSTER_JCMK_REPLACEMENT_PHASE_PREPARE
		|| durable_prepare->ready_state_generation != 0)
		return false;

	if (!cluster_reconfig_replacement_candidate2_capabilities_current(&capability_episode))
		return false;

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	episode = &ReconfigShmem->replacement_episode;
	if (episode->target_node_id >= 0 && episode->target_node_id < CLUSTER_MAX_NODES)
		subject[episode->target_node_id / 8] = (uint8)(1u << (episode->target_node_id % 8));
	if (memcmp(episode, &capability_episode, sizeof(*episode)) != 0
		|| !cluster_replacement_episode_is_valid(episode)
		|| (episode->phase != CLUSTER_REPLACEMENT_EPISODE_PURGING
			&& episode->phase != CLUSTER_REPLACEMENT_EPISODE_PURGE_COMPLETE)
		|| episode->readiness_flags != 0 || episode->coordinator_node_id != local_receiver_node_id
		|| !dead_bitmap_test_bit(episode->expected_survivors, authenticated_source_node_id)
		|| !dead_bitmap_test_bit(episode->expected_survivors, local_receiver_node_id)
		|| !cluster_reconfig_replacement_membership_current_locked(episode)
		|| cluster_epoch_get_current() != episode->baseline_epoch
		|| cluster_membership_get_last_admitted_incarnation(local_receiver_node_id)
			   != settled_ballot->proposer_admitted_incarnation
		|| cluster_membership_get_last_admitted_incarnation(episode->target_node_id)
			   != episode->old_admitted_incarnation
		|| ack->target_node_id != episode->target_node_id || ack->epoch != episode->baseline_epoch
		|| ack->request_nonce != episode->request_nonce
		|| ack->identity0 != episode->old_admitted_incarnation
		|| ack->identity1 != episode->fresh_incarnation
		|| memcmp(ack->body.bitmap, episode->expected_survivors, sizeof(ack->body.bitmap)) != 0
		|| ack->grammar_fingerprint != episode->grammar_fingerprint
		|| settled_reserve->request_origin_node != episode->target_node_id
		|| settled_reserve->target_node_id != episode->target_node_id
		|| settled_reserve->baseline_epoch != episode->baseline_epoch
		|| settled_reserve->reserved_epoch != episode->reserved_or_committed_epoch
		|| settled_reserve->old_incarnation != episode->old_admitted_incarnation
		|| settled_reserve->fresh_incarnation != episode->fresh_incarnation
		|| settled_reserve->request_nonce != episode->request_nonce
		|| memcmp(settled_reserve->authority_member_bitmap, episode->expected_survivors,
				  sizeof(settled_reserve->authority_member_bitmap))
			   != 0
		|| memcmp(settled_reserve->event_subject_bitmap, subject, sizeof(subject)) != 0
		|| settled_reserve->grammar_fingerprint != episode->grammar_fingerprint
		|| durable_prepare->target_node_id != episode->target_node_id
		|| durable_prepare->old_admitted_incarnation != episode->old_admitted_incarnation
		|| durable_prepare->fresh_incarnation != episode->fresh_incarnation
		|| durable_prepare->baseline_epoch != episode->baseline_epoch
		|| durable_prepare->reserved_or_committed_epoch != episode->reserved_or_committed_epoch
		|| durable_prepare->request_nonce != episode->request_nonce
		|| memcmp(durable_prepare->expected_purge_survivors, episode->expected_survivors,
				  sizeof(durable_prepare->expected_purge_survivors))
			   != 0
		|| durable_prepare->grammar_fingerprint != episode->grammar_fingerprint)
		goto out;

	authorized = true;
out:
	LWLockRelease(&ReconfigShmem->lock);
	if (authorized)
		*out_ack_node_id = authenticated_source_node_id;
	return authorized;
}


/* Authenticated phase-2 envelope adapter.  The caller receives only the
 * derived endpoint id, never a payload-selected bit. */
bool
cluster_reconfig_replacement_purge_ack_ingress_authorized(
	const ClusterICEnvelope *env, const void *payload, uint32 payload_length,
	int32 authenticated_source_node_id, int32 local_receiver_node_id,
	const ClusterEpochAuthorityValue *settled_reserve, const ClusterEpochBallotId *settled_ballot,
	const ClusterReplacementCommitMarkerV3 *durable_prepare, int32 *out_ack_node_id)
{
	ClusterReplacementWireMessage ack;

	if (env == NULL || payload == NULL || out_ack_node_id == NULL
		|| payload_length != CLUSTER_REPLACEMENT_WIRE_BYTES
		|| env->msg_type != PGRAC_IC_MSG_GES_REQUEST
		|| env->payload_length != CLUSTER_REPLACEMENT_WIRE_BYTES || authenticated_source_node_id < 0
		|| authenticated_source_node_id >= CLUSTER_MAX_NODES || local_receiver_node_id < 0
		|| local_receiver_node_id >= CLUSTER_MAX_NODES
		|| authenticated_source_node_id == local_receiver_node_id
		|| env->source_node_id != (uint32)authenticated_source_node_id
		|| env->dest_node_id != (uint32)local_receiver_node_id
		|| env->epoch != cluster_epoch_get_current()
		|| !cluster_replacement_wire_decode((const uint8 *)payload, &ack)
		|| ack.epoch != env->epoch)
		return false;
	return cluster_reconfig_replacement_purge_ack_authorized(
		&ack, authenticated_source_node_id, local_receiver_node_id, settled_reserve, settled_ballot,
		durable_prepare, out_ack_node_id);
}


static inline bool
dead_bitmap_is_zero(const uint8 *bmp)
{
	int i;
	for (i = 0; i < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; i++)
		if (bmp[i] != 0)
			return false;
	return true;
}


/* Returns lowest bit index set in bmp, or -1 if all zero. */
static int
dead_bitmap_lowest_bit_set(const uint8 *bmp)
{
	int i, j;
	for (i = 0; i < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; i++) {
		if (bmp[i] == 0)
			continue;
		for (j = 0; j < 8; j++)
			if (bmp[i] & (uint8)(1u << j))
				return i * 8 + j;
	}
	return -1;
}


/* Snapshot the exact shared-CF rollover episodes this LMON is authorized to
 * drive through the pre-existing ordinary join/redeclare machinery. */
static bool
cluster_reconfig_fast_rejoin_actions_snapshot(void)
{
	bool active;

	if (ReconfigShmem == NULL || cluster_online_join || !cluster_controlfile_shared_authority)
		return false;
	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	active = !dead_bitmap_is_zero(ReconfigShmem->fast_rejoin_bitmap);
	LWLockRelease(&ReconfigShmem->lock);
	return active;
}


/*
 * spec-2.29 P1.2: event_id = hash_bytes_extended(dead_bitmap[16] || cssd_dead_generation).
 *
 *	  NOT hash(old_epoch, ...) — old_epoch would self-loop per P1.2 finding.
 *	  hash_bytes_extended is PG's 64-bit murmurhash-style;collision-resistance
 *	  is sufficient for dedup (R2 mitigation).  event_id=0 reserved as
 *	  never-applied sentinel;in the astronomically rare case real hash
 *	  yields 0 we treat that as fresh-tick (re-publish, no harm).
 */
uint64
cluster_reconfig_compute_event_id(const uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES],
								  uint64 cssd_dead_generation)
{
	uint8 hash_input[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES + sizeof(uint64)];

	memcpy(hash_input, dead_bitmap, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	memcpy(hash_input + CLUSTER_RECONFIG_DEAD_BITMAP_BYTES, &cssd_dead_generation, sizeof(uint64));
	return hash_bytes_extended(hash_input, sizeof(hash_input), 0);
}


/*
 * spec-5.15 D3 (INV-J11) — kind-aware event_id, folding the discriminator for
 * the kinds that need it.
 *
 *	FAIL_STOP / CLEAN_LEAVE  -> the LEGACY cluster_reconfig_compute_event_id over
 *	    (dead_bitmap, cssd_dead_generation).  This is byte-identical to the 2.29
 *	    death path AND to what shipped spec-5.13 binds into the durable leave
 *	    marker (RC-1, verified against linkdb: cluster_clean_leave.c binds the
 *	    legacy id — folding CLEAN_LEAVE would break that binding).  FAIL_STOP and
 *	    CLEAN_LEAVE never share a (dead_bitmap, cssd_dead_generation): each
 *	    death / leave episode advances cssd_dead_generation, which is the actual
 *	    non-collision basis (NOT a folded kind byte).
 *	JOIN_PENDING / JOIN_COMMITTED -> FOLD kind || join_bitmap ||
 *	    joiner_incarnations || cssd_dead_generation.  Join events have an empty
 *	    dead_bitmap, so the legacy hash would collide across the two phases and
 *	    across distinct joins (R12); folding the kind distinguishes PENDING from
 *	    COMMITTED and the incarnations distinguish distinct joiner sets.
 *	NONE -> 0 (the never-applied sentinel; not a real event).
 */
uint64
cluster_reconfig_compute_event_id_v2(uint8 reconfig_kind,
									 const uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES],
									 const uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES],
									 const uint64 joiner_incarnations[CLUSTER_MAX_NODES],
									 uint64 cssd_dead_generation)
{
	uint8 hash_input[1 + CLUSTER_RECONFIG_DEAD_BITMAP_BYTES + sizeof(uint64) * CLUSTER_MAX_NODES
					 + sizeof(uint64)];
	Size off = 0;

	switch (reconfig_kind) {
	case RECONFIG_KIND_NONE:
		return 0; /* sentinel — not a real event */

	case RECONFIG_KIND_FAIL_STOP:
	case RECONFIG_KIND_CLEAN_LEAVE:
		return cluster_reconfig_compute_event_id(dead_bitmap, cssd_dead_generation);

	case RECONFIG_KIND_JOIN_PENDING:
	case RECONFIG_KIND_JOIN_COMMITTED:
	default:
		Assert(join_bitmap != NULL && joiner_incarnations != NULL);
		hash_input[off++] = reconfig_kind;
		memcpy(hash_input + off, join_bitmap, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
		off += CLUSTER_RECONFIG_DEAD_BITMAP_BYTES;
		memcpy(hash_input + off, joiner_incarnations, sizeof(uint64) * CLUSTER_MAX_NODES);
		off += sizeof(uint64) * CLUSTER_MAX_NODES;
		memcpy(hash_input + off, &cssd_dead_generation, sizeof(uint64));
		off += sizeof(uint64);
		return hash_bytes_extended(hash_input, off, 0);
	}
}

/*
 * spec-5.18 D3 (R14) — NODE_REMOVED event identity.
 *
 *	The legacy event_id hashes only (dead_bitmap, cssd_dead_generation).  A
 *	clean-left removal leaves dead_bitmap unchanged (the node already departed)
 *	and does not bump cssd_dead_generation, so the legacy id would collide with
 *	the prior event and be deduped away — the removal would never publish (打穿
 *	INV-LF1).  Fold the kind + removed_bitmap + removal_event_id (the per-attempt
 *	identity) — NOT old_epoch (2.29 P1.2 anti-self-loop) — so each removal attempt
 *	produces a distinct, non-deduped event id.
 */
uint64
cluster_reconfig_compute_removal_event_id(
	const uint8 removed_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES], uint64 removal_event_id)
{
	uint8 hash_input[1 + CLUSTER_RECONFIG_DEAD_BITMAP_BYTES + sizeof(uint64)];
	Size off = 0;

	hash_input[off++] = RECONFIG_KIND_NODE_REMOVED;
	memcpy(hash_input + off, removed_bitmap, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	off += CLUSTER_RECONFIG_DEAD_BITMAP_BYTES;
	memcpy(hash_input + off, &removal_event_id, sizeof(uint64));
	off += sizeof(uint64);
	return hash_bytes_extended(hash_input, off, 0);
}


/*
 * spec-5.15 D1 — cluster_reconfig_compute_join_bitmap.
 *
 *	Compute the set of declared peers that transitioned from a non-member state
 *	(DEAD / ABSENT) into a fresh-ALIVE state since the last applied reconfig (the
 *	"join edge"), mirroring the death-edge computation in the tick.  A bit i is
 *	set iff ALL hold:
 *	  - cluster_conf_lookup_node(i) != NULL          (declared-peer filter)
 *	  - cluster_membership_get_state(i) is DEAD/ABSENT (not currently a member)
 *	  - ABSENT at INITIAL is excluded: the founding-quorum path owns it
 *	  - cluster_cssd_get_peer_state(i) == ALIVE      (now heart-beating)
 *	  - the peer's observed voting-slot incarnation is strictly greater than
 *	    last_admitted_incarnation[i]                 (freshness, anti-stale: a
 *	    stale rejoin never even raises a join edge; the coordinator vet (D4) is
 *	    the authoritative gate that issues REJECT_STALE on the TOCTOU window)
 *
 *	Pure w.r.t. shmem reads (uses the qvotec-published observed slot, not a disk
 *	read).  Caller holds the reconfig LWLock (membership_state reads).  Returns
 *	the number of join-edge bits set.
 */
int
cluster_reconfig_compute_join_bitmap(uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES])
{
	int i;
	int count = 0;

	memset(join_bitmap, 0, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);

	if (ReconfigShmem == NULL)
		return 0;

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		ClusterMembershipState ms;
		uint64 observed_incarnation = 0;
		uint64 observed_generation = 0;

		if (i == cluster_node_id)
			continue; /* self is never a join candidate */
		if (cluster_conf_lookup_node(i) == NULL)
			continue; /* declared-peer filter */

		ms = cluster_membership_get_state(i);
		if (ms != CLUSTER_MEMBER_DEAD && ms != CLUSTER_MEMBER_ABSENT)
			continue; /* already a member / mid-join (JOINING) — not a new edge */
		/* QVOTEC may publish a founding peer after this tick's membership
		 * loop.  It is still bootstrap work, not a runtime join edge: creating
		 * JOIN_PENDING here would advance the epoch and destroy the proof the
		 * next bootstrap tick needs.  Keep the existing exact founding proof
		 * as the only ABSENT admission owner at INITIAL. */
		if (ms == CLUSTER_MEMBER_ABSENT && cluster_epoch_get_current() == CLUSTER_EPOCH_INITIAL)
			continue;

		if (cluster_cssd_get_peer_state(i) != CLUSTER_CSSD_PEER_ALIVE)
			continue; /* not heart-beating yet */

		if (!cluster_reconfig_get_observed_slot(i, &observed_incarnation, &observed_generation))
			continue; /* no valid published slot — not ready */
		if (observed_incarnation <= cluster_membership_get_last_admitted_incarnation(i))
			continue; /* stale incarnation (INV-J1 pre-filter); vet REJECTs at commit */

		dead_bitmap_set_bit(join_bitmap, i);
		count++;
	}

	return count;
}


/*
 * spec-5.15 D1 — qvotec publishes the freshest observed voting-slot incarnation
 * + generation per node here each poll (it is the sole disk reader).  pg_atomic
 * write so qvotec, a different process, publishes lock-free.  NULL-safe / range-
 * checked.
 */
void
cluster_reconfig_record_observed_slot(int32 node_id, uint64 incarnation, uint64 generation,
									  uint64 epoch)
{
	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return;
	/* RF-ROOT P6: generation is the publish gate -- write the data fields
	 * first and the generation LAST so a reader that re-checks the
	 * generation before/after sees one exact torn-free sample. */
	pg_atomic_write_u64(&ReconfigShmem->observed_incarnation[node_id], incarnation);
	pg_atomic_write_u64(&ReconfigShmem->observed_epoch[node_id], epoch);
	pg_atomic_write_u64(&ReconfigShmem->observed_generation[node_id], generation);
}

uint64
cluster_reconfig_get_observed_epoch(int32 node_id)
{
	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->observed_epoch[node_id]);
}

/*
 * spec-5.16 (3-node join participation) — qvotec publishes / the LMON reads the
 * per-peer durable COMMITTED join marker (admitted incarnation + epoch) it
 * observed on a quorum-majority of that peer's region-3 slots.  This is the
 * runtime signal a SURVIVOR uses to recognize a rejoined peer as MEMBER (the
 * symmetric observer half of the LEAVE detection), so its GRD FSM joins the
 * re-declare barrier.  pg_atomic — qvotec (writer) and LMON (reader) are
 * different processes.
 */
void
cluster_reconfig_record_observed_committed_join(int32 node_id, uint64 incarnation, uint64 epoch)
{
	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return;
	/* RF-ROOT P6: epoch is written BEFORE the incarnation; the incarnation
	 * is the publish gate for this pair, and it is monotone across boots,
	 * so a reader that re-checks the incarnation sees one exact sample. */
	pg_atomic_write_u64(&ReconfigShmem->observed_committed_join_epoch[node_id], epoch);
	pg_atomic_write_u64(&ReconfigShmem->observed_committed_join_incarnation[node_id], incarnation);
}

bool
cluster_reconfig_get_observed_committed_join(int32 node_id, uint64 *incarnation, uint64 *epoch)
{
	uint64 inc;
	uint64 inc_after;
	uint64 ep;

	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES) {
		if (incarnation != NULL)
			*incarnation = 0;
		if (epoch != NULL)
			*epoch = 0;
		return false;
	}
	/* Exact torn-free sample: incarnation is the publish gate; read it
	 * before and after the epoch and retry on any concurrent publish. */
	do {
		inc = pg_atomic_read_u64(&ReconfigShmem->observed_committed_join_incarnation[node_id]);
		ep = pg_atomic_read_u64(&ReconfigShmem->observed_committed_join_epoch[node_id]);
		inc_after
			= pg_atomic_read_u64(&ReconfigShmem->observed_committed_join_incarnation[node_id]);
	} while (inc != inc_after);
	if (incarnation != NULL)
		*incarnation = inc;
	if (epoch != NULL)
		*epoch = ep;
	return inc > 0;
}


/*
 * spec-5.15 D1 — read the freshest observed voting-slot incarnation + generation
 * for node_id.  Returns true iff a valid slot was observed (generation > 0);
 * out-params always written (0 when absent).  Used by the join-edge detector and
 * the coordinator vet.
 */
bool
cluster_reconfig_get_observed_slot(int32 node_id, uint64 *incarnation, uint64 *generation)
{
	uint64 generation_before;
	uint64 generation_after;
	uint64 inc;

	if (incarnation != NULL)
		*incarnation = 0;
	if (generation != NULL)
		*generation = 0;

	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return false;

	/* RF-ROOT P6: generation is the publish gate (written last by the
	 * producer); re-checking it before/after the data reads yields one
	 * exact torn-free sample. */
	do {
		generation_before = pg_atomic_read_u64(&ReconfigShmem->observed_generation[node_id]);
		inc = pg_atomic_read_u64(&ReconfigShmem->observed_incarnation[node_id]);
		generation_after = pg_atomic_read_u64(&ReconfigShmem->observed_generation[node_id]);
	} while (generation_before != generation_after);

	if (incarnation != NULL)
		*incarnation = inc;
	if (generation != NULL)
		*generation = generation_after;
	return generation_after > 0;
}

/* STOP04 §2.4.1: generation/incarnation is one coherent qvotec sample.  The
 * generation is a torn-write counter only and must never be substituted with
 * an incarnation, membership epoch or failure generation. */
bool
cluster_reconfig_get_observed_slot_coherent(int32 node_id, uint64 *out_incarnation,
											uint64 *out_generation)
{
	uint64 generation_before;
	uint64 generation_after;
	uint64 incarnation;

	if (out_incarnation != NULL)
		*out_incarnation = 0;
	if (out_generation != NULL)
		*out_generation = 0;
	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES
		|| out_incarnation == NULL || out_generation == NULL)
		return false;

	do {
		generation_before = pg_atomic_read_u64(&ReconfigShmem->observed_generation[node_id]);
		incarnation = pg_atomic_read_u64(&ReconfigShmem->observed_incarnation[node_id]);
		generation_after = pg_atomic_read_u64(&ReconfigShmem->observed_generation[node_id]);
	} while (generation_before != generation_after);

	if (generation_after == 0)
		return false;
	*out_incarnation = incarnation;
	*out_generation = generation_after;
	return true;
}

static bool
rejoin_bitmap_shape_valid(const uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES],
						  const uint8 survivor_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES],
						  int32 old_node_id)
{
	int survivor_count = 0;
	int i;

	if (old_node_id < 0 || old_node_id >= CLUSTER_MAX_NODES
		|| !dead_bitmap_test_bit(dead_bitmap, old_node_id)
		|| dead_bitmap_test_bit(survivor_bitmap, old_node_id))
		return false;
	for (i = 0; i < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; i++) {
		uint8 survivors = survivor_bitmap[i];

		if ((dead_bitmap[i] & survivors) != 0)
			return false;
		while (survivors != 0) {
			survivor_count += survivors & 1;
			survivors >>= 1;
		}
	}
	return survivor_count >= 1 && survivor_count < CLUSTER_MAX_NODES;
}

static bool
rejoin_failure_value_valid(const ClusterReconfigRejoinFailureSnapshotV1 *failure)
{
	return failure != NULL && failure->reconfig_kind == RECONFIG_KIND_FAIL_STOP
		   && failure->reserved0 == 0 && failure->reserved68 == 0 && failure->event_id != 0
		   && failure->new_epoch != 0 && failure->cssd_dead_generation != 0
		   && failure->old_incarnation != 0
		   && rejoin_bitmap_shape_valid(failure->dead_bitmap, failure->survivor_bitmap,
										failure->old_node_id);
}

bool
cluster_reconfig_rejoin_failure_snapshot(int32 old_node_id, uint64 old_incarnation,
										 ClusterReconfigRejoinFailureSnapshotV1 *out_failure)
{
	ClusterReconfigRejoinFailureSnapshotV1 failure;
	const ReconfigEvent *event;
	int survivor_count = 0;
	int i;

	if (out_failure != NULL)
		memset(out_failure, 0, sizeof(*out_failure));
	if (out_failure == NULL || ReconfigShmem == NULL || old_node_id < 0
		|| old_node_id >= CLUSTER_MAX_NODES || old_incarnation == 0)
		return false;

	memset(&failure, 0, sizeof(failure));
	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	event = &ReconfigShmem->last_applied;
	if (event->reconfig_kind != RECONFIG_KIND_FAIL_STOP || event->event_id == 0
		|| event->new_epoch == 0 || event->cssd_dead_generation == 0
		|| !dead_bitmap_test_bit(event->dead_bitmap, old_node_id)
		|| ReconfigShmem->membership.last_admitted_incarnation[old_node_id] != old_incarnation)
		goto fail;

	failure.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	failure.event_id = event->event_id;
	failure.new_epoch = event->new_epoch;
	failure.cssd_dead_generation = event->cssd_dead_generation;
	memcpy(failure.dead_bitmap, event->dead_bitmap, sizeof(failure.dead_bitmap));
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (ReconfigShmem->membership.membership_state[i] == (uint8)CLUSTER_MEMBER_MEMBER) {
			dead_bitmap_set_bit(failure.survivor_bitmap, i);
			survivor_count++;
		}
	}
	failure.old_node_id = old_node_id;
	failure.old_incarnation = old_incarnation;
	if (survivor_count < 1 || survivor_count >= CLUSTER_MAX_NODES
		|| !rejoin_failure_value_valid(&failure))
		goto fail;
	LWLockRelease(&ReconfigShmem->lock);
	*out_failure = failure;
	return true;

fail:
	LWLockRelease(&ReconfigShmem->lock);
	return false;
}

static bool
rejoin_bitmap_is_exact_singleton(const uint8 bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES],
								 int32 node_id)
{
	int i;

	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return false;
	for (i = 0; i < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; i++) {
		uint8 expected = (i == node_id / 8) ? (uint8)(UINT8_C(1) << (node_id % 8)) : 0;

		if (bitmap[i] != expected)
			return false;
	}
	return true;
}


static void
cluster_reconfig_fast_rejoin_control_clear(void)
{
	memset(&fast_rejoin_control, 0, sizeof(fast_rejoin_control));
	fast_rejoin_control.target_node_id = -1;
}


/* Arm only at the successful coordinator FAIL_STOP publication edge.  A
 * second simultaneous rollover is outside the approved singleton P04 slice
 * and therefore fails closed rather than selecting one implicitly. */
static void
cluster_reconfig_fast_rejoin_control_arm(const ReconfigEvent *event)
{
	uint64 boot_incarnation;
	uint64 lms_generation;
	uint64 target_incarnation = 0;
	uint64 target_generation = 0;
	uint64 prior_incarnation = 0;
	int32 target = -1;
	int i;

	if (fast_rejoin_control.active) {
		cluster_reconfig_fast_rejoin_control_clear();
		return;
	}
	if (event == NULL || ReconfigShmem == NULL || MyBackendType != B_LMON || cluster_online_join
		|| !cluster_controlfile_shared_authority || event->reconfig_kind != RECONFIG_KIND_FAIL_STOP
		|| event->observer_role != CLUSTER_RECONFIG_OBSERVER_COORDINATOR || event->event_id == 0
		|| event->new_epoch == 0)
		return;

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (!dead_bitmap_test_bit(ReconfigShmem->fast_rejoin_bitmap, i)
			|| !dead_bitmap_test_bit(event->dead_bitmap, i))
			continue;
		if (target >= 0) {
			LWLockRelease(&ReconfigShmem->lock);
			return;
		}
		target = i;
		prior_incarnation = ReconfigShmem->fast_rejoin_incarnation[i];
	}
	if (target < 0 || cluster_membership_get_state(target) != CLUSTER_MEMBER_DEAD) {
		LWLockRelease(&ReconfigShmem->lock);
		return;
	}
	LWLockRelease(&ReconfigShmem->lock);

	if (!cluster_reconfig_get_observed_slot_coherent(target, &target_incarnation,
													 &target_generation)
		|| target_generation == 0 || target_incarnation <= prior_incarnation
		|| !cluster_reconfig_get_observed_fresh_alive(target))
		return;
	boot_incarnation = cluster_qvotec_get_self_incarnation();
	lms_generation = cluster_lms_get_lms_restart_generation();
	if (boot_incarnation == 0 || lms_generation == 0)
		return;

	fast_rejoin_control.active = true;
	fast_rejoin_control.target_node_id = target;
	fast_rejoin_control.target_incarnation = target_incarnation;
	fast_rejoin_control.failstop_event_id = event->event_id;
	fast_rejoin_control.failstop_epoch = event->new_epoch;
	fast_rejoin_control.boot_incarnation = boot_incarnation;
	fast_rejoin_control.lms_generation = lms_generation;
}


/* Return the exact authorized target, or -1 after fail-closed invalidation.
 * The only legal lineage is the original FAIL_STOP (including its staged
 * singleton Phase 1) followed by the exact singleton JOIN_PENDING. */
static int32
cluster_reconfig_fast_rejoin_control_snapshot(uint64 *target_incarnation_out)
{
	ReconfigEvent event;
	uint64 observed_incarnation = 0;
	uint64 observed_generation = 0;
	ClusterMembershipState membership_state = CLUSTER_MEMBER_ABSENT;
	bool fast_bit = false;
	bool pending_bit = false;
	int32 target;

	if (target_incarnation_out != NULL)
		*target_incarnation_out = 0;
	if (!fast_rejoin_control.active)
		return -1;
	target = fast_rejoin_control.target_node_id;
	if (ReconfigShmem == NULL || target < 0 || target >= CLUSTER_MAX_NODES || cluster_online_join
		|| !cluster_controlfile_shared_authority
		|| cluster_qvotec_get_self_incarnation() != fast_rejoin_control.boot_incarnation
		|| cluster_lms_get_lms_restart_generation() != fast_rejoin_control.lms_generation
		|| !cluster_reconfig_get_observed_slot_coherent(target, &observed_incarnation,
														&observed_generation)
		|| observed_generation == 0
		|| observed_incarnation != fast_rejoin_control.target_incarnation
		|| !cluster_reconfig_get_observed_fresh_alive(target))
		goto invalid;

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	event = ReconfigShmem->last_applied;
	fast_bit = dead_bitmap_test_bit(ReconfigShmem->fast_rejoin_bitmap, target);
	pending_bit = dead_bitmap_test_bit(ReconfigShmem->pending_join_bitmap, target);
	membership_state = cluster_membership_get_state(target);
	LWLockRelease(&ReconfigShmem->lock);
	if (!fast_bit || membership_state == CLUSTER_MEMBER_REMOVED)
		goto invalid;

	if (event.reconfig_kind == RECONFIG_KIND_FAIL_STOP) {
		if (event.event_id != fast_rejoin_control.failstop_event_id
			|| event.new_epoch != fast_rejoin_control.failstop_epoch
			|| !dead_bitmap_test_bit(event.dead_bitmap, target))
			goto invalid;
		if (pending_bit) {
			if (!join_prepare_stage.async.has_staged_event
				|| !rejoin_bitmap_is_exact_singleton(join_prepare_stage.join_bitmap, target)
				|| join_prepare_stage.joiner_incarnations[target]
					   != fast_rejoin_control.target_incarnation
				|| join_prepare_stage.event.old_epoch != fast_rejoin_control.failstop_epoch
				|| join_prepare_stage.event.new_epoch != fast_rejoin_control.failstop_epoch + 1)
				goto invalid;
		} else if (membership_state != CLUSTER_MEMBER_DEAD)
			goto invalid;
	} else if (event.reconfig_kind == RECONFIG_KIND_JOIN_PENDING) {
		if (!pending_bit || membership_state != CLUSTER_MEMBER_JOINING
			|| !rejoin_bitmap_is_exact_singleton(event.join_bitmap, target)
			|| event.old_epoch != fast_rejoin_control.failstop_epoch
			|| event.new_epoch != fast_rejoin_control.failstop_epoch + 1)
			goto invalid;
	} else
		goto invalid;

	if (target_incarnation_out != NULL)
		*target_incarnation_out = fast_rejoin_control.target_incarnation;
	return target;

invalid:
	cluster_reconfig_fast_rejoin_control_clear();
	return -1;
}


static void
cluster_reconfig_fast_rejoin_control_finish(int32 node_id, uint64 admitted_incarnation)
{
	if (fast_rejoin_control.active && fast_rejoin_control.target_node_id == node_id
		&& fast_rejoin_control.target_incarnation == admitted_incarnation)
		cluster_reconfig_fast_rejoin_control_clear();
}

bool
cluster_reconfig_rejoin_pending_snapshot(const ClusterReconfigRejoinFailureSnapshotV1 *failure,
										 uint64 candidate_incarnation,
										 ClusterReconfigRejoinPendingSnapshotV1 *out_pending)
{
	ClusterReconfigRejoinPendingSnapshotV1 pending;
	uint64 incarnations[CLUSTER_MAX_NODES];
	uint64 observed_incarnation;
	uint64 observed_generation;
	const ReconfigEvent *event;
	int32 node_id;
	int i;

	if (out_pending != NULL)
		memset(out_pending, 0, sizeof(*out_pending));
	if (out_pending == NULL || ReconfigShmem == NULL || !rejoin_failure_value_valid(failure)
		|| candidate_incarnation <= failure->old_incarnation)
		return false;
	node_id = failure->old_node_id;
	if (!cluster_reconfig_get_observed_slot_coherent(node_id, &observed_incarnation,
													 &observed_generation)
		|| observed_incarnation != candidate_incarnation)
		return false;

	memset(&pending, 0, sizeof(pending));
	memset(incarnations, 0, sizeof(incarnations));
	incarnations[node_id] = candidate_incarnation;
	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	event = &ReconfigShmem->last_applied;
	if (event->reconfig_kind != RECONFIG_KIND_JOIN_PENDING || event->event_id == 0
		|| event->old_epoch != failure->new_epoch || failure->new_epoch == UINT64_MAX
		|| event->new_epoch != event->old_epoch + 1 || event->cssd_dead_generation == 0
		|| ReconfigShmem->membership.membership_state[node_id] != (uint8)CLUSTER_MEMBER_JOINING
		|| ReconfigShmem->membership.last_admitted_incarnation[node_id] != failure->old_incarnation
		|| !rejoin_bitmap_is_exact_singleton(event->join_bitmap, node_id)
		|| !rejoin_bitmap_is_exact_singleton(ReconfigShmem->pending_join_bitmap, node_id))
		goto pending_fail;
	for (i = 0; i < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; i++) {
		if (event->dead_bitmap[i] != 0)
			goto pending_fail;
	}
	if (event->event_id
		!= cluster_reconfig_compute_event_id_v2(RECONFIG_KIND_JOIN_PENDING, event->dead_bitmap,
												event->join_bitmap, incarnations,
												event->cssd_dead_generation))
		goto pending_fail;

	pending.reconfig_kind = RECONFIG_KIND_JOIN_PENDING;
	pending.event_id = event->event_id;
	pending.old_epoch = event->old_epoch;
	pending.new_epoch = event->new_epoch;
	pending.cssd_dead_generation = event->cssd_dead_generation;
	memcpy(pending.dead_bitmap, event->dead_bitmap, sizeof(pending.dead_bitmap));
	memcpy(pending.join_bitmap, event->join_bitmap, sizeof(pending.join_bitmap));
	pending.node_id = node_id;
	pending.candidate_incarnation = candidate_incarnation;
	pending.observed_slot_generation = observed_generation;
	LWLockRelease(&ReconfigShmem->lock);
	*out_pending = pending;
	return true;

pending_fail:
	LWLockRelease(&ReconfigShmem->lock);
	return false;
}

/*
 * spec-5.15 Hardening v1.3 (INV-J14 stale-slot fail-open) — publish / read the
 * per-node FRESH-ALIVE liveness qvotec derived from decide_quorum_view's
 * heartbeat-freshness gate (P2.1).  The cold-bootstrap proof counts a peer only
 * when it is fresh-alive at epoch INITIAL — a generation > 0 slot alone may be a
 * crashed peer's stale leftover.  Anchored on the durable voting-disk heartbeat,
 * not live CSSD, so the v1.2 IC-churn race fix is preserved.
 */
void
cluster_reconfig_record_observed_fresh_alive(int32 node_id, bool fresh_alive)
{
	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return;
	pg_atomic_write_u64(&ReconfigShmem->observed_fresh_alive[node_id], fresh_alive ? 1 : 0);
}

bool
cluster_reconfig_get_observed_fresh_alive(int32 node_id)
{
	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return false;
	return pg_atomic_read_u64(&ReconfigShmem->observed_fresh_alive[node_id]) != 0;
}

/*
 * RF-ROOT P9 verification / cold-formation ruling (2026-08-19): QVOTEC
 * bootstrap-publication seqlock helpers.  qvotec calls begin / (per-node
 * observed-slot + fresh-alive records + in-quorum snapshot) / end once per
 * poll; the seq goes ODD while the window is open and EVEN when stable.
 * The founding-formation ABSENT admission reads the whole window through
 * cluster_reconfig_bootstrap_proof_node below — it retries on odd/CHANGED
 * seq, so a "new incarnation + stale fresh-alive" combination can never
 * form an admission proof.
 */
void
cluster_reconfig_bootstrap_publish_begin(void)
{
	if (ReconfigShmem != NULL) {
		pg_atomic_fetch_add_u64(&ReconfigShmem->observed_bootstrap_seq, 1);
		pg_write_barrier();
	}
}

void
cluster_reconfig_bootstrap_publish_in_quorum(bool in_quorum)
{
	if (ReconfigShmem != NULL)
		pg_atomic_write_u64(&ReconfigShmem->bootstrap_in_quorum, in_quorum ? 1 : 0);
}

void
cluster_reconfig_bootstrap_publish_end(void)
{
	if (ReconfigShmem != NULL) {
		pg_write_barrier();
		pg_atomic_fetch_add_u64(&ReconfigShmem->observed_bootstrap_seq, 1);
	}
}

/*
 * cluster_reconfig_bootstrap_proof_node — RF-ROOT P9 verification /
 * cold-formation ruling: the WHOLE-round founding-formation proof for ONE target
 * node, sampled inside one stable EVEN observed_bootstrap_seq window.
 *
 * Reads, from the coherent snapshot: the same-round in-quorum flag and
 * every declared node's observed incarnation/generation/epoch/fresh-alive,
 * then recomputes the founding bootstrap quorum (self + fresh co-boot
 * slots at INITIAL, strict majority — the same test as
 * cluster_reconfig_bootstrap_quorum_at_initial, but from the snapshot
 * instead of live atomics).  Returns true only when the quorum holds AND
 * the target is fresh-alive at INITIAL with a non-zero incarnation, and
 * writes that incarnation to *out_incarnation.  Any odd or CHANGED seq
 * retries; every require is evaluated on the STABLE snapshot only
 * (fail-closed — a transient miss just retries on the next tick).
 * Lock-free: shmem atomic reads only, so the ABSENT branch can call it
 * under the reconfig EXCLUSIVE lock without any lock-order risk.
 */
bool
cluster_reconfig_bootstrap_proof_node(int32 node_id, uint64 *out_incarnation)
{
	/* cold-formation ruling: bounded retries only — this runs under the reconfig
	 * EXCLUSIVE lock, so an odd/CHANGED seqlock window must return false
	 * (retry next tick), never spin forever. */
	int attempt;

	for (attempt = 0; attempt < 8; attempt++) {
		uint64 seq_before;
		uint64 seq_after;
		uint64 target_inc = 0;
		bool in_quorum;
		bool target_ok = false;
		bool running_cluster = false;
		uint32 declared = 0;
		uint32 proven = 0;
		int i;

		if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
			return false;
		if (out_incarnation != NULL)
			*out_incarnation = 0;
		seq_before = pg_atomic_read_u64(&ReconfigShmem->observed_bootstrap_seq);
		if ((seq_before & UINT64_C(1)) != 0)
			continue; /* writer in progress: retry */
		pg_read_barrier();
		in_quorum = pg_atomic_read_u64(&ReconfigShmem->bootstrap_in_quorum) != 0;
		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			uint64 inc;
			uint64 gen;
			uint64 ep;
			bool fresh;

			if (cluster_conf_lookup_node(i) == NULL)
				continue;
			declared++;
			if (i == cluster_node_id) {
				/* self is up, at INITIAL (not yet admitted) */
				proven++;
				continue;
			}
			inc = pg_atomic_read_u64(&ReconfigShmem->observed_incarnation[i]);
			gen = pg_atomic_read_u64(&ReconfigShmem->observed_generation[i]);
			ep = pg_atomic_read_u64(&ReconfigShmem->observed_epoch[i]);
			fresh = pg_atomic_read_u64(&ReconfigShmem->observed_fresh_alive[i]) != 0;
			/* any declared peer past INITIAL => a running cluster, NOT a
			 * founding bootstrap (same rule as
			 * cluster_reconfig_bootstrap_quorum_at_initial) */
			if (ep > CLUSTER_EPOCH_INITIAL)
				running_cluster = true;
			if (i == node_id && fresh && gen > 0 && inc != 0 && ep == CLUSTER_EPOCH_INITIAL) {
				target_inc = inc;
				target_ok = true;
			}
			if (fresh && gen > 0 && ep == CLUSTER_EPOCH_INITIAL)
				proven++;
		}
		pg_read_barrier();
		seq_after = pg_atomic_read_u64(&ReconfigShmem->observed_bootstrap_seq);
		if (seq_after != seq_before)
			continue; /* writer crossed: retry */
		if (declared == 0 || running_cluster || !in_quorum || !target_ok
			|| proven < (declared / 2u) + 1u)
			return false; /* fail-closed on the stable snapshot */
		if (out_incarnation != NULL)
			*out_incarnation = target_inc;
		return true;
	}
	/* No stable publication was observed; retry on a later membership tick. */
	return false;
}


/* ============================================================
 * spec-5.13 D3 (CL-I13) — clean-departed bitmap record / clear / query.
 *
 *	A node enters clean_departed_bitmap when a CLEAN_LEAVE reconfig commits
 *	naming it (survivor observe), or at startup rebuilt from a durable §2.5
 *	COMMITTED marker.  cluster_reconfig_lmon_tick masks it out of the dead set
 *	so its later CSSD DEAD never re-triggers fail-stop (the spurious second
 *	reconfig of R18 / 40R01 of a drain-grace tx).  Mutations take `lock`
 *	EXCLUSIVE; the lmon_tick masking reads under SHARED.
 * ============================================================
 */

static inline bool
clean_departed_test_bit_locked(const uint8 *bmp, int i)
{
	if (i < 0 || i >= CLUSTER_RECONFIG_DEAD_BITMAP_BYTES * 8)
		return false;
	return (bmp[i / 8] & (uint8)(1u << (i % 8))) != 0;
}

void
cluster_reconfig_record_clean_departed(int32 node_id, uint64 leave_epoch, bool raise_epoch_floor)
{
	bool newly_set = false;

	if (ReconfigShmem == NULL)
		return;
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return;

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	if (!clean_departed_test_bit_locked(ReconfigShmem->clean_departed_bitmap, node_id)
		|| leave_epoch > ReconfigShmem->clean_departed_epoch[node_id])
		cluster_write_fence_authority_cache_invalidate();
	if (!clean_departed_test_bit_locked(ReconfigShmem->clean_departed_bitmap, node_id)) {
		dead_bitmap_set_bit(ReconfigShmem->clean_departed_bitmap, node_id);
		newly_set = true;
	}
	/* Always record the latest leave epoch (a re-departure raises it). */
	if (leave_epoch > ReconfigShmem->clean_departed_epoch[node_id])
		ReconfigShmem->clean_departed_epoch[node_id] = leave_epoch;
	LWLockRelease(&ReconfigShmem->lock);

	if (newly_set)
		pg_atomic_fetch_add_u64(&ReconfigShmem->clean_departed_count, 1);

	/*
	 * P1-V0.7 epoch-floor recovery: the membership epoch is not durable, so a
	 * durable COMMITTED marker is the only proof the cluster reached
	 * leave_epoch.  Raise the local floor (max-merge, monotone, never retreats)
	 * — exempt from OBSERVE_MAX_JUMP since this is startup recovery, not a
	 * hostile inbound envelope.  Done outside the reconfig lock (epoch is its
	 * own shmem with its own CAS).
	 */
	if (raise_epoch_floor && leave_epoch > 0)
		(void)cluster_epoch_observe_remote(leave_epoch);
}

void
cluster_reconfig_clear_clean_departed(int32 node_id)
{
	if (ReconfigShmem == NULL)
		return;
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return;

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	if (clean_departed_test_bit_locked(ReconfigShmem->clean_departed_bitmap, node_id)
		|| ReconfigShmem->clean_departed_epoch[node_id] != 0)
		cluster_write_fence_authority_cache_invalidate();
	if (clean_departed_test_bit_locked(ReconfigShmem->clean_departed_bitmap, node_id))
		ReconfigShmem->clean_departed_bitmap[node_id / 8] &= (uint8) ~(1u << (node_id % 8));
	ReconfigShmem->clean_departed_epoch[node_id] = 0;
	LWLockRelease(&ReconfigShmem->lock);
}

bool
cluster_reconfig_is_clean_departed(int32 node_id)
{
	bool departed;

	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return false;

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	departed = clean_departed_test_bit_locked(ReconfigShmem->clean_departed_bitmap, node_id);
	LWLockRelease(&ReconfigShmem->lock);
	return departed;
}

uint64
cluster_reconfig_get_clean_departed_epoch(int32 node_id)
{
	uint64 epoch;

	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return 0;

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	epoch = ReconfigShmem->clean_departed_epoch[node_id];
	LWLockRelease(&ReconfigShmem->lock);
	return epoch;
}

uint64
cluster_reconfig_get_clean_departed_count(void)
{
	if (ReconfigShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->clean_departed_count);
}

/* ============================================================
 * spec-5.18 D3 — permanently-removed set accessors (INV-LF1).
 * ============================================================ */

void
cluster_reconfig_record_removed(int32 node_id, uint64 remove_epoch, bool raise_epoch_floor)
{
	bool newly_set = false;

	if (ReconfigShmem == NULL)
		return;
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return;

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	if (!clean_departed_test_bit_locked(ReconfigShmem->removed_bitmap, node_id)
		|| remove_epoch > ReconfigShmem->removed_epoch[node_id]
		|| clean_departed_test_bit_locked(ReconfigShmem->clean_departed_bitmap, node_id))
		cluster_write_fence_authority_cache_invalidate();
	if (!clean_departed_test_bit_locked(ReconfigShmem->removed_bitmap, node_id)) {
		dead_bitmap_set_bit(ReconfigShmem->removed_bitmap, node_id);
		newly_set = true;
	}
	if (remove_epoch > ReconfigShmem->removed_epoch[node_id])
		ReconfigShmem->removed_epoch[node_id] = remove_epoch;
	/* a removed node supersedes a dormant clean-departed one (no auto re-admit). */
	if (clean_departed_test_bit_locked(ReconfigShmem->clean_departed_bitmap, node_id)) {
		ReconfigShmem->clean_departed_bitmap[node_id / 8] &= (uint8) ~(1u << (node_id % 8));
		ReconfigShmem->clean_departed_epoch[node_id] = 0;
	}
	LWLockRelease(&ReconfigShmem->lock);

	if (newly_set)
		pg_atomic_fetch_add_u64(&ReconfigShmem->removed_count, 1);

	/* P1-V0.7 epoch-floor recovery (mirror clean_departed): the removal marker is
	 * the only durable proof the cluster reached remove_epoch. */
	if (raise_epoch_floor && remove_epoch > 0)
		(void)cluster_epoch_observe_remote(remove_epoch);
}

bool
cluster_reconfig_is_removed(int32 node_id)
{
	bool removed;

	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return false;

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	removed = clean_departed_test_bit_locked(ReconfigShmem->removed_bitmap, node_id);
	LWLockRelease(&ReconfigShmem->lock);
	return removed;
}

/*
 * spec-5.18 HF-2: lock-free removed test for the 53R64 self-demote write gate
 * (cluster_node_remove_self_is_removed), called at every writable-xid assignment.
 * Reads a single removed_bitmap bit without the reconfig lock — safe because the
 * removed set is monotonic at runtime (a removal is terminal, INV-LF1; only an
 * operator un-fence, not implemented, could clear it), so a one-byte read cannot
 * tear and the bit never spuriously clears.  The durable bitmap is the
 * authoritative floor: unlike membership_state[self] (which the joiner / lmon
 * self-state paths rewrite each tick), it cannot be flipped REMOVED -> not-removed
 * by membership churn, so the write gate stays fail-closed for a removed node.
 */
bool
cluster_reconfig_is_removed_unlocked(int32 node_id)
{
	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_RECONFIG_DEAD_BITMAP_BYTES * 8)
		return false;
	return (ReconfigShmem->removed_bitmap[node_id / 8] & (uint8)(1u << (node_id % 8))) != 0;
}

uint64
cluster_reconfig_get_removed_epoch(int32 node_id)
{
	uint64 epoch;

	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return 0;

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	epoch = ReconfigShmem->removed_epoch[node_id];
	LWLockRelease(&ReconfigShmem->lock);
	return epoch;
}

uint64
cluster_reconfig_get_removed_count(void)
{
	if (ReconfigShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&ReconfigShmem->removed_count);
}

/*
 * spec-5.18 D3 — startup-rebuild seed: record the node removed (removed_bitmap +
 * epoch floor) AND shrink its membership_state to REMOVED, under the reconfig lock.
 * Used by cluster_node_remove_rebuild_from_disks so the driver does not need direct
 * access to ReconfigShmem->lock for the membership mutation.
 */
void
cluster_reconfig_seed_removed_membership(int32 node_id, uint64 remove_epoch,
										 uint64 removed_incarnation, bool raise_epoch_floor)
{
	cluster_reconfig_record_removed(node_id, remove_epoch, raise_epoch_floor);
	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return;
	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	cluster_membership_shrink_to_removed(node_id, removed_incarnation);
	LWLockRelease(&ReconfigShmem->lock);
}

/*
 * Snapshot the removed_bitmap under SHARED lock (qvotec ORs it into the fence
 * baseline, INV-LF10; the driver folds it into the removal event_id).
 */
void
cluster_reconfig_snapshot_removed_bitmap(uint8 *out)
{
	if (out == NULL)
		return;
	if (ReconfigShmem == NULL) {
		memset(out, 0, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
		return;
	}
	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	memcpy(out, ReconfigShmem->removed_bitmap, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	LWLockRelease(&ReconfigShmem->lock);
}

/* spec-5.15 D6 — online-join observability counter accessors. */
uint64
cluster_reconfig_get_join_pending_count(void)
{
	return ReconfigShmem == NULL ? 0 : pg_atomic_read_u64(&ReconfigShmem->join_pending_count);
}
uint64
cluster_reconfig_get_join_apply_count(void)
{
	return ReconfigShmem == NULL ? 0 : pg_atomic_read_u64(&ReconfigShmem->join_apply_count);
}
uint64
cluster_reconfig_get_join_reject_count(void)
{
	return ReconfigShmem == NULL ? 0 : pg_atomic_read_u64(&ReconfigShmem->join_reject_count);
}
uint64
cluster_reconfig_get_join_timeout_count(void)
{
	return ReconfigShmem == NULL ? 0 : pg_atomic_read_u64(&ReconfigShmem->join_timeout_count);
}
uint64
cluster_reconfig_get_clean_departed_cleared_count(void)
{
	return ReconfigShmem == NULL ? 0
								 : pg_atomic_read_u64(&ReconfigShmem->clean_departed_cleared_count);
}

void
cluster_reconfig_note_marker_slow_ack(ClusterMarkerAsyncKind kind, int32 target_node,
									  uint64 elapsed_us)
{
	if (ReconfigShmem == NULL || elapsed_us < 1000000ULL)
		return;

	pg_atomic_fetch_add_u64(&ReconfigShmem->marker_slow_ack_count, 1);
	ereport(LOG, (errmsg("cluster marker: slow qvotec ACK for %s target node %d took %llu ms",
						 cluster_marker_async_kind_name(kind), target_node,
						 (unsigned long long)(elapsed_us / 1000ULL))));
}

void
cluster_reconfig_note_marker_timeout(ClusterMarkerAsyncKind kind, int32 target_node,
									 uint64 elapsed_us)
{
	if (ReconfigShmem != NULL)
		pg_atomic_fetch_add_u64(&ReconfigShmem->marker_timeout_count, 1);
	ereport(LOG, (errmsg("cluster marker: qvotec marker %s target node %d timed out after %llu ms",
						 cluster_marker_async_kind_name(kind), target_node,
						 (unsigned long long)(elapsed_us / 1000ULL))));
}

uint64
cluster_reconfig_get_marker_slow_ack_count(void)
{
	return ReconfigShmem == NULL ? 0 : pg_atomic_read_u64(&ReconfigShmem->marker_slow_ack_count);
}

uint64
cluster_reconfig_get_marker_timeout_count(void)
{
	return ReconfigShmem == NULL ? 0 : pg_atomic_read_u64(&ReconfigShmem->marker_timeout_count);
}

/*
 * cluster_reconfig_join_in_progress -- Hardening v1.0.4 (spec-5.13 clean-leave x
 * spec-5.15 online-join serialization, P1-1/P2): is a membership JOIN currently in
 * its pending window anywhere this node can observe?  The clean-leave request +
 * real-announce paths consult this so a clean leave does not START (or a survivor
 * does not ACCEPT an announce) while a join is mid-flight — the symmetric half of
 * "one membership reconfig at a time" (the join driver checks the mirror predicate
 * cluster_clean_leave_in_progress()).  A join bumps the membership epoch with
 * dead_gen unchanged, which the leaving node would otherwise mis-observe as its own
 * clean-leave commit and wedge in BARRIER_WAIT (P2).  Read under the reconfig lock
 * (consistent snapshot of the 16-byte pending bitmap); called only at reconfig-rate
 * boundaries, never on a hot path.
 */
bool
cluster_reconfig_join_in_progress(void)
{
	bool in_progress = false;
	int i;

	if (ReconfigShmem == NULL)
		return false;

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	if (ReconfigShmem->self_join_admitted == 0)
		in_progress = true; /* this node is itself a not-yet-admitted joiner */
	else if (ReconfigShmem->last_applied.reconfig_kind == RECONFIG_KIND_JOIN_PENDING)
		in_progress = true; /* the last published edge is a JOIN Phase-1 */
	else {
		for (i = 0; i < (int)sizeof(ReconfigShmem->pending_join_bitmap); i++) {
			if (ReconfigShmem->pending_join_bitmap[i] != 0) {
				in_progress = true; /* a declared peer is in the join pending window */
				break;
			}
		}
	}
	LWLockRelease(&ReconfigShmem->lock);
	return in_progress;
}


/*
 * spec-5.15 D4 §3.3 — has the JOIN_PENDING converged?  Every existing MEMBER
 * survivor (other than self and the joiner) must have observed the coordinator's
 * current membership epoch (the joiner is NOT in the convergence set — INV-J12 —
 * it adopts from the COMMITTED marker, breaking the commit<-converge<-adopt
 * cycle).  At 2 nodes the only MEMBER survivor is self, so this is trivially true
 * and Phase-2 follows on the next tick.
 */
static bool
cluster_reconfig_join_converged(int joiner)
{
	uint64 target_epoch = cluster_epoch_get_current();
	int i;

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == cluster_node_id || i == joiner)
			continue;
		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		if (!cluster_membership_is_member(i))
			continue; /* not a member -> not in the convergence set */
		if (cluster_reconfig_get_observed_epoch(i) < target_epoch)
			return false; /* this MEMBER survivor has not caught up */
	}
	return true;
}

bool
cluster_reconfig_rejoin_pending_ready(const ClusterReconfigRejoinPendingSnapshotV1 *pending)
{
	uint64 observed_incarnation;
	uint64 observed_generation;

	if (pending == NULL || pending->reconfig_kind != RECONFIG_KIND_JOIN_PENDING
		|| pending->reserved0 != 0 || pending->node_id < 0 || pending->node_id >= CLUSTER_MAX_NODES
		|| pending->candidate_incarnation == 0 || pending->observed_slot_generation == 0
		|| !cluster_reconfig_get_observed_slot_coherent(pending->node_id, &observed_incarnation,
														&observed_generation)
		|| observed_incarnation != pending->candidate_incarnation
		|| observed_generation != pending->observed_slot_generation
		|| !cluster_reconfig_join_converged(pending->node_id))
		return false;
	return cluster_membership_vet_joiner(pending->node_id, pending->candidate_incarnation,
										 pending->observed_slot_generation)
		   == CLUSTER_JOIN_ACCEPT;
}

static void
cluster_reconfig_external_rejoin_release_slot(int node_id)
{
	ClusterExternalRejoinSlot *slot;

	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return;
	slot = &external_rejoin_slots[node_id];
	cluster_external_fence_rejoin_authority_clear_release(&slot->authority_clear);
	cluster_external_fence_rejoin_release(&slot->op);
	memset(slot, 0, sizeof(*slot));
}

static void
cluster_reconfig_external_rejoin_release_all(void)
{
	int i;

	cluster_external_fence_rejoin_release(&external_rejoin_claim_op);
	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		cluster_reconfig_external_rejoin_release_slot(i);
}

static void
cluster_reconfig_external_rejoin_on_exit(int code pg_attribute_unused(),
										 Datum arg pg_attribute_unused())
{
	cluster_reconfig_external_rejoin_release_all();
}

static bool
cluster_reconfig_external_rejoin_has_slot(void)
{
	int i;

	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		if (external_rejoin_slots[i].phase != CLUSTER_EXTERNAL_REJOIN_EMPTY)
			return true;
	return false;
}

static bool
cluster_reconfig_external_rejoin_terminal(PgracExternalFenceRejoinStatus status)
{
	return status == PGRAC_EXTERNAL_FENCE_REJOIN_REJECTED
		   || status == PGRAC_EXTERNAL_FENCE_REJOIN_UNKNOWN
		   || status == PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE
		   || status == PGRAC_EXTERNAL_FENCE_REJOIN_STALE
		   || status == PGRAC_EXTERNAL_FENCE_REJOIN_CONSUMED;
}

static bool
cluster_reconfig_external_rejoin_required(int node_id)
{
	return cluster_external_fence_runtime_active() && node_id >= 0 && node_id < CLUSTER_MAX_NODES
		   && cluster_membership_get_last_admitted_incarnation(node_id) > 0
		   && !cluster_reconfig_is_clean_departed(node_id);
}

static bool
cluster_reconfig_external_rejoin_authorized(int node_id, uint64 candidate_incarnation)
{
	ClusterExternalRejoinSlot *slot;

	if (!cluster_reconfig_external_rejoin_required(node_id) || candidate_incarnation == 0)
		return false;
	slot = &external_rejoin_slots[node_id];
	return slot->op != NULL && slot->candidate_incarnation == candidate_incarnation
		   && slot->failure.old_node_id == node_id
		   && slot->phase >= CLUSTER_EXTERNAL_REJOIN_WAITING_JOINER
		   && slot->phase <= CLUSTER_EXTERNAL_REJOIN_COMMITTING;
}

/* STOP04 §11.8: coordinator-LMON drives the opaque provider object without
 * waits and advances at most one local phase per tick.  The current production
 * policy never calls this path because provider 0 keeps bit24 inactive. */
static void
cluster_reconfig_external_rejoin_tick(void)
{
	PgracExternalFenceDenyReason reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	PgracExternalFenceRejoinStatus status;
	int advance_node = -1;
	int i;

	if (!cluster_external_fence_runtime_active()) {
		cluster_reconfig_external_rejoin_release_all();
		return;
	}
	if (!external_rejoin_exit_registered) {
		on_shmem_exit(cluster_reconfig_external_rejoin_on_exit, (Datum)0);
		external_rejoin_exit_registered = true;
	}

	/* Poll every bound operation once, in deterministic old-node order. */
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		ClusterExternalRejoinSlot *slot = &external_rejoin_slots[i];

		if (slot->op == NULL || slot->phase == CLUSTER_EXTERNAL_REJOIN_COMMITTING)
			continue;
		status = cluster_external_fence_rejoin_poll_nowait(slot->op, &reason);
		if (cluster_reconfig_external_rejoin_terminal(status)) {
			cluster_reconfig_external_rejoin_release_slot(i);
			continue;
		}
		if (slot->phase == CLUSTER_EXTERNAL_REJOIN_WAITING_ON
			&& status == PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_JOINER)
			slot->phase = CLUSTER_EXTERNAL_REJOIN_WAITING_JOINER;
		else if (slot->phase == CLUSTER_EXTERNAL_REJOIN_WAITING_REFRESH
				 && status == PGRAC_EXTERNAL_FENCE_REJOIN_READY)
			slot->phase = CLUSTER_EXTERNAL_REJOIN_READY;
	}

	/* A CLAIM is unindexed until the provider returns its exact OFFER. */
	if (external_rejoin_claim_op != NULL) {
		const PgracExternalFenceRejoinOfferV1 *offer;

		status = cluster_external_fence_rejoin_poll_nowait(external_rejoin_claim_op, &reason);
		if (status == PGRAC_EXTERNAL_FENCE_REJOIN_OFFERED) {
			offer = cluster_external_fence_rejoin_offer(external_rejoin_claim_op);
			if (offer == NULL || offer->old_node_id < 0 || offer->old_node_id >= CLUSTER_MAX_NODES
				|| offer->old_incarnation == 0 || offer->candidate_incarnation == 0
				|| external_rejoin_slots[offer->old_node_id].phase != CLUSTER_EXTERNAL_REJOIN_EMPTY)
				cluster_external_fence_rejoin_release(&external_rejoin_claim_op);
			else {
				ClusterExternalRejoinSlot *slot = &external_rejoin_slots[offer->old_node_id];

				slot->op = external_rejoin_claim_op;
				external_rejoin_claim_op = NULL;
				slot->candidate_incarnation = offer->candidate_incarnation;
				slot->phase = CLUSTER_EXTERNAL_REJOIN_OFFERED;
			}
		} else if (cluster_reconfig_external_rejoin_terminal(status))
			cluster_external_fence_rejoin_release(&external_rejoin_claim_op);
	}

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		ClusterExternalRejoinPhase phase = external_rejoin_slots[i].phase;

		if (phase == CLUSTER_EXTERNAL_REJOIN_OFFERED
			|| phase == CLUSTER_EXTERNAL_REJOIN_WAITING_ROOT
			|| phase == CLUSTER_EXTERNAL_REJOIN_WAITING_JOINER) {
			advance_node = i;
			break;
		}
	}

	if (advance_node >= 0) {
		ClusterExternalRejoinSlot *slot = &external_rejoin_slots[advance_node];

		if (slot->phase == CLUSTER_EXTERNAL_REJOIN_OFFERED) {
			const PgracExternalFenceRejoinOfferV1 *offer
				= cluster_external_fence_rejoin_offer(slot->op);
			ClusterGrdRejoinClearSnapshotV1 grd_clear;

			memset(&slot->failure, 0, sizeof(slot->failure));
			memset(&grd_clear, 0, sizeof(grd_clear));
			if (offer == NULL
				|| !cluster_reconfig_rejoin_failure_snapshot(
					offer->old_node_id, offer->old_incarnation, &slot->failure)) {
				cluster_reconfig_external_rejoin_release_slot(advance_node);
				return;
			}
			/* G is a positive all-survivor cut.  Not-yet-DONE is a retry,
			 * not authority to discard the still-fresh OFFER or actuate ON. */
			if (!cluster_grd_rejoin_clear_snapshot(&slot->failure, &grd_clear))
				return;
			status = cluster_external_fence_rejoin_authority_clear_build(
				slot->op, &slot->failure, &grd_clear, &slot->authority_clear, &reason);
			if (status != PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_ROOT
				|| slot->authority_clear == NULL) {
				cluster_reconfig_external_rejoin_release_slot(advance_node);
				return;
			}
			slot->phase = CLUSTER_EXTERNAL_REJOIN_WAITING_ROOT;
		} else if (slot->phase == CLUSTER_EXTERNAL_REJOIN_WAITING_ROOT) {
			ClusterControlRootIdentity identity;
			ClusterControlRootSnapshot snapshot;
			ClusterControlRootReadToken token;
			ClusterControlRootResult root_result;
			uint8 protected_set_digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES];

			memset(&identity, 0, sizeof(identity));
			memset(&snapshot, 0, sizeof(snapshot));
			memset(&token, 0, sizeof(token));
			memset(protected_set_digest, 0, sizeof(protected_set_digest));
			root_result = cluster_control_root_lookup_owner_by_node_runtime(advance_node, &identity,
																			&snapshot, &token);
			if ((root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
				 && root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
				|| snapshot.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
				|| !cluster_external_fence_rejoin_protected_set_digest(protected_set_digest))
				return;
			status = cluster_external_fence_rejoin_authorize_on_async(
				slot->op, &slot->authority_clear, &identity, &snapshot, &token,
				protected_set_digest, &reason);
			if (status == PGRAC_EXTERNAL_FENCE_REJOIN_PENDING && slot->authority_clear == NULL)
				slot->phase = CLUSTER_EXTERNAL_REJOIN_WAITING_ON;
			else if (status != PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_ROOT)
				cluster_reconfig_external_rejoin_release_slot(advance_node);
		} else {
			ClusterReconfigRejoinPendingSnapshotV1 pending;

			memset(&pending, 0, sizeof(pending));
			if (!cluster_reconfig_rejoin_pending_snapshot(&slot->failure,
														  slot->candidate_incarnation, &pending)
				|| !cluster_reconfig_rejoin_pending_ready(&pending))
				return;
			status = cluster_external_fence_rejoin_refresh_on_async(slot->op, &pending, &reason);
			if (status == PGRAC_EXTERNAL_FENCE_REJOIN_PENDING) {
				slot->commit_pending = pending;
				slot->phase = CLUSTER_EXTERNAL_REJOIN_WAITING_REFRESH;
			} else if (status != PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_JOINER)
				cluster_reconfig_external_rejoin_release_slot(advance_node);
		}
	}

	/* The frozen linear chain admits only one live CLAIM/OFFER at a time. */
	if (external_rejoin_claim_op == NULL && !cluster_reconfig_external_rejoin_has_slot()) {
		status = cluster_external_fence_rejoin_start_async(
			PGRAC_EXTERNAL_FENCE_ACQUIRE_TIMEOUT_DEFAULT_MS, &external_rejoin_claim_op);
		if (cluster_reconfig_external_rejoin_terminal(status))
			cluster_external_fence_rejoin_release(&external_rejoin_claim_op);
	}
}

static bool
cluster_reconfig_external_rejoin_prepare_commit(int node_id, uint64 candidate_incarnation)
{
	ClusterExternalRejoinSlot *slot;
	ClusterReconfigRejoinPendingSnapshotV1 pending;
	ClusterControlRootSnapshot fresh_root;
	PgracExternalFenceDenyReason reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;

	if (!cluster_reconfig_external_rejoin_required(node_id))
		return true;
	slot = &external_rejoin_slots[node_id];
	if (slot->op == NULL || slot->phase != CLUSTER_EXTERNAL_REJOIN_READY
		|| slot->candidate_incarnation != candidate_incarnation)
		return false;
	memset(&pending, 0, sizeof(pending));
	memset(&fresh_root, 0, sizeof(fresh_root));
	if (!cluster_reconfig_rejoin_pending_snapshot(&slot->failure, candidate_incarnation, &pending)
		|| !cluster_reconfig_rejoin_pending_ready(&pending)
		|| memcmp(&pending, &slot->commit_pending, sizeof(pending)) != 0
		|| !cluster_external_fence_rejoin_revalidate_root(slot->op, &fresh_root, &reason)) {
		cluster_reconfig_external_rejoin_release_slot(node_id);
		return false;
	}
	slot->commit_pending = pending;
	slot->phase = CLUSTER_EXTERNAL_REJOIN_COMMIT_READY;
	return true;
}

/*
 * spec-5.15 Hardening v1.1 (HF-1 / INV-J9 strengthened) — proof that the
 * coordinator's JOIN_COMMITTED publish actually propagated to a quorum of the
 * membership, NOT merely that the COMMITTED marker is durable.  A joiner opens
 * its write gate only after a majority of the current MEMBER survivors have
 * advanced their durable (voting-disk-observed) epoch to >= admitted_epoch.
 *
 * The survivor epoch is observable because qvotec publishes the LIVE
 * cluster_epoch_get_current() into each slot (5.15 ship substrate-fix #1);
 * after the coordinator's publish advances the cluster epoch to admitted_epoch
 * (== new_epoch == current+1 at marker-write time) the members reach it via the
 * normal bounded observe and persist it, and the joiner reads it back through
 * observed_epoch[].  If the coordinator crashes AFTER the COMMITTED marker is
 * durable but BEFORE the publish (injection cluster-reconfig-join-commit-marker-
 * durable), the survivors never advance, this proof never holds, the gate stays
 * closed, and the joiner times out -> 53R61 -> restarts with a fresh incarnation
 * (P1-1: the half-publish window the v1.0 note_self_admitted left open).
 *
 * Lock-free, mirroring cluster_reconfig_join_converged: is_member is a single-
 * byte read and get_observed_epoch is an atomic read.  Fail-closed: zero visible
 * MEMBER survivor (e.g. transient) -> cannot prove -> false.
 */
bool
cluster_reconfig_join_publish_proven(uint64 admitted_epoch)
{
	uint32 members = 0;
	uint32 advanced = 0;
	int i;

	if (ReconfigShmem == NULL || admitted_epoch == 0)
		return false;

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == cluster_node_id)
			continue;
		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		if (!cluster_membership_is_member(i))
			continue; /* only existing MEMBER survivors carry the publish */
		members++;
		if (cluster_reconfig_get_observed_epoch(i) >= admitted_epoch)
			advanced++;
	}

	if (members == 0)
		return false; /* nobody to prove it -> fail-closed */
	return advanced >= ((members / 2u) + 1u);
}

static void
cluster_reconfig_release_fence_stage(ClusterReconfigFenceStage *stage)
{
	cluster_marker_async_release_stage(&stage->async);
	stage->submitted = false;
	memset(&stage->event, 0, sizeof(stage->event));
	memset(&stage->marker, 0, sizeof(stage->marker));
	stage->node_id = -1;
	stage->last_incarnation = 0;
	stage->removal_event_id = 0;
}

/*
 * spec-2.29a nightly-regression fix — pre-bump staging window guard.
 *
 *	The three pre-bump coordinator paths (fail-stop fence, node-remove,
 *	join Phase-1) advance the membership epoch at stage-entry but only
 *	publish the reconfig event once the voting-disk marker ACKs, which now
 *	spans several LMON ticks instead of the pre-async single-tick spin.
 *	Between the bump and the publish the GRD recovery IDLE tick would
 *	re-capture recovery_event_old_epoch as the post-bump value, so its P0
 *	accept later reads old == cur and wedges WAIT_EPOCH forever (the
 *	spec-4.6a section 0 shape, here triggered by the coordinator on ITSELF
 *	— even in a 2-node cluster with no IC piggyback).  While any pre-bump
 *	stage is live the GRD IDLE tick must hold its last stable (genuine
 *	pre-reconfig) baseline instead of re-capturing.  JOIN Phase-2 now also
 *	keeps the shared bit set across its bump -> durable-baseline -> publish
 *	window, so it needs no separate process-local flag here.
 */
bool
cluster_reconfig_has_pending_prebump_stage(void)
{
	/* spec-2.29a r2 t/274: the shmem bit covers a BACKEND-context coordinator's
	 * bump→publish window (LMON cannot see that process's local stage); the
	 * three LMON-local staged flags cover the LMON-driven paths. */
	if (ReconfigShmem != NULL && pg_atomic_read_u32(&ReconfigShmem->prebump_sync_active) != 0)
		return true;
	return failstop_fence_stage.async.has_staged_event
		   || node_removed_fence_stage.async.has_staged_event
		   || join_prepare_stage.async.has_staged_event;
}

static bool
cluster_reconfig_submit_fence_stage(ClusterReconfigFenceStage *stage, ClusterMarkerAsyncKind kind,
									int32 target_node, TimestampTz now)
{
	if (stage->submitted)
		return true;
	if (!cluster_write_fence_submit_marker_async(&stage->async, &stage->marker, kind, target_node,
												 now))
		return false;
	stage->submitted = true;
	return true;
}

static bool
cluster_reconfig_poll_failstop_fence_stage(void)
{
	TimestampTz now;
	uint32 result = CLUSTER_FENCE_MARKER_SUBMIT_FAILED;
	uint64 elapsed_us = 0;
	ClusterMarkerPollResult pr;

	if (!failstop_fence_stage.async.has_staged_event)
		return false;

	now = GetCurrentTimestamp();
	if (!cluster_reconfig_submit_fence_stage(&failstop_fence_stage,
											 CLUSTER_MARKER_KIND_FENCE_FAILSTOP,
											 failstop_fence_stage.event.coordinator_node_id, now))
		return true;

	pr = cluster_write_fence_poll_marker_async(&failstop_fence_stage.async, now, &result,
											   &elapsed_us);
	if (pr == CLUSTER_MARKER_POLL_PENDING || pr == CLUSTER_MARKER_POLL_IDLE)
		return true;
	if (pr == CLUSTER_MARKER_POLL_TIMEOUT) {
		cluster_reconfig_note_marker_timeout(CLUSTER_MARKER_KIND_FENCE_FAILSTOP,
											 failstop_fence_stage.event.coordinator_node_id,
											 elapsed_us);
		cluster_reconfig_release_fence_stage(&failstop_fence_stage);
		return true;
	}

	cluster_reconfig_note_marker_slow_ack(CLUSTER_MARKER_KIND_FENCE_FAILSTOP,
										  failstop_fence_stage.event.coordinator_node_id,
										  elapsed_us);
	if (result == CLUSTER_FENCE_MARKER_SUBMIT_ACK) {
		cluster_reconfig_publish_event(&failstop_fence_stage.event);
		cluster_reconfig_fast_rejoin_control_arm(&failstop_fence_stage.event);
		cluster_reconfig_log_failstop_epoch_bump(&failstop_fence_stage.event);
		cluster_reconfig_broadcast_local_procsig();
	} else {
		ereport(LOG,
				(errmsg("cluster reconfig: fence marker did not reach a voting-disk majority "
						"for epoch %llu; not publishing reconfig event (write-fenced, will retry)",
						(unsigned long long)failstop_fence_stage.event.new_epoch)));
	}
	cluster_reconfig_release_fence_stage(&failstop_fence_stage);
	return true;
}

static uint64
cluster_reconfig_poll_node_removed_fence_stage(int32 removed_node_id, uint64 removal_event_id,
											   uint64 last_incarnation)
{
	TimestampTz now;
	uint32 result = CLUSTER_FENCE_MARKER_SUBMIT_FAILED;
	uint64 elapsed_us = 0;
	ClusterMarkerPollResult pr;

	if (!node_removed_fence_stage.async.has_staged_event)
		return 0;
	if (node_removed_fence_stage.node_id != removed_node_id
		|| node_removed_fence_stage.removal_event_id != removal_event_id)
		return 0;

	now = GetCurrentTimestamp();
	if (!cluster_reconfig_submit_fence_stage(&node_removed_fence_stage,
											 CLUSTER_MARKER_KIND_FENCE_NODE_REMOVED,
											 removed_node_id, now))
		return 0;

	pr = cluster_write_fence_poll_marker_async(&node_removed_fence_stage.async, now, &result,
											   &elapsed_us);
	if (pr == CLUSTER_MARKER_POLL_PENDING || pr == CLUSTER_MARKER_POLL_IDLE)
		return 0;
	if (pr == CLUSTER_MARKER_POLL_TIMEOUT) {
		cluster_reconfig_note_marker_timeout(CLUSTER_MARKER_KIND_FENCE_NODE_REMOVED,
											 removed_node_id, elapsed_us);
		cluster_reconfig_release_fence_stage(&node_removed_fence_stage);
		return 0;
	}

	cluster_reconfig_note_marker_slow_ack(CLUSTER_MARKER_KIND_FENCE_NODE_REMOVED, removed_node_id,
										  elapsed_us);
	if (result != CLUSTER_FENCE_MARKER_SUBMIT_ACK) {
		ereport(LOG, (errmsg("cluster node removal: fence marker for node %d did not reach a "
							 "voting-disk majority for epoch %llu; not publishing removal "
							 "(write-fenced, will retry)",
							 removed_node_id,
							 (unsigned long long)node_removed_fence_stage.event.new_epoch)));
		cluster_reconfig_release_fence_stage(&node_removed_fence_stage);
		return 0;
	}

	CLUSTER_INJECTION_POINT("cluster-node-remove-fence-armed");
	cluster_reconfig_publish_event(&node_removed_fence_stage.event);
	cluster_reconfig_record_removed(removed_node_id, node_removed_fence_stage.event.new_epoch,
									false);
	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	cluster_membership_shrink_to_removed(removed_node_id, last_incarnation);
	LWLockRelease(&ReconfigShmem->lock);

	{
		uint64 new_epoch = node_removed_fence_stage.event.new_epoch;

		cluster_reconfig_release_fence_stage(&node_removed_fence_stage);
		return new_epoch;
	}
}

static void
cluster_reconfig_release_join_prepare_stage(void)
{
	cluster_marker_async_release_stage(&join_prepare_stage.async);
	memset(&join_prepare_stage.event, 0, sizeof(join_prepare_stage.event));
	memset(join_prepare_stage.join_bitmap, 0, sizeof(join_prepare_stage.join_bitmap));
	memset(join_prepare_stage.joiner_incarnations, 0,
		   sizeof(join_prepare_stage.joiner_incarnations));
	join_prepare_stage.next_node = 0;
	join_prepare_stage.submitted = false;
}

/* (spec-2.29a review r1: the former abort_join_prepare_stage — revert
 * JOINING→DEAD + clear pending on PREPARE failure — was removed.  Q5=A makes
 * PREPARE strictly best-effort: TIMEOUT / non-ACK advances the queue and the
 * staged JOIN_PENDING still publishes, so no revert path exists; reverting
 * would let compute_join_bitmap re-detect the joiner and re-bump the epoch
 * every deadline period — the P1-1 bump storm.) */

static bool
cluster_reconfig_submit_join_prepare_current(TimestampTz now)
{
	ClusterJoinCommitMarker m;
	int i;

	for (i = join_prepare_stage.next_node; i < CLUSTER_MAX_NODES; i++) {
		if (dead_bitmap_test_bit(join_prepare_stage.join_bitmap, i))
			break;
	}
	join_prepare_stage.next_node = i;
	if (i >= CLUSTER_MAX_NODES) {
		cluster_reconfig_publish_event(&join_prepare_stage.event);
		pg_atomic_fetch_add_u64(&ReconfigShmem->join_pending_count, 1);
		cluster_reconfig_broadcast_local_procsig();
		cluster_reconfig_release_join_prepare_stage();
		return true;
	}

	if (join_prepare_stage.submitted)
		return true;

	memset(&m, 0, sizeof(m));
	m.magic = CLUSTER_JCMK_MAGIC;
	m.version = CLUSTER_JCMK_VERSION;
	m.node_id = i;
	m.phase = CLUSTER_JCMK_PHASE_PREPARE;
	m.admitted_incarnation = join_prepare_stage.joiner_incarnations[i];
	m.generation = join_prepare_stage.joiner_incarnations[i];
	m.admitted_epoch = join_prepare_stage.event.new_epoch;
	cluster_join_marker_compute_crc(&m);

	if (!cluster_reconfig_submit_join_marker_async(&join_prepare_stage.async, i, &m,
												   CLUSTER_MARKER_KIND_JOIN_PREPARE, now))
		return false;
	join_prepare_stage.submitted = true;
	return true;
}

/*
 * spec-2.29a review r1 P2 — node-remove driver pre-work gate.
 *
 *	While the staged node-removed fence marker from a previous FENCE_ARMING
 *	tick is still in flight, the driver must not re-run its pre-work
 *	(REMOVING marker write / stripe retire): the epoch has already been
 *	self-bumped by the stage-entry, so a re-run would key a SECOND REMOVING
 *	marker to the post-bump epoch and add voting-disk writes during the
 *	exact contention window the async stage exists to relieve.
 */
bool
cluster_reconfig_node_removed_fence_stage_pending(void)
{
	return node_removed_fence_stage.async.has_staged_event;
}

static bool
cluster_reconfig_poll_join_prepare_stage(void)
{
	TimestampTz now;
	uint32 result = CLUSTER_JOIN_MARKER_SUBMIT_FAILED;
	uint64 elapsed_us = 0;
	ClusterMarkerPollResult pr;
	int target;

	if (!join_prepare_stage.async.has_staged_event)
		return false;

	now = GetCurrentTimestamp();
	if (!join_prepare_stage.submitted) {
		(void)cluster_reconfig_submit_join_prepare_current(now);
		return true;
	}

	target = join_prepare_stage.next_node;
	pr = cluster_reconfig_poll_join_marker_async(&join_prepare_stage.async, now, &result,
												 &elapsed_us);
	if (pr == CLUSTER_MARKER_POLL_PENDING || pr == CLUSTER_MARKER_POLL_IDLE)
		return true;

	/*
	 * spec-2.29a Q5=A (review r1 P1): PREPARE is best-effort in OUTCOME — the
	 * pre-async code ignored the submit result and always published
	 * JOIN_PENDING (only the Phase-2 COMMITTED marker is a commit point, P1-r5).
	 * A TIMEOUT / non-ACK PREPARE therefore advances the queue instead of
	 * aborting: aborting would revert the joiner JOINING→DEAD, and the next
	 * tick's compute_join_bitmap would re-detect it CSSD-alive and RE-BUMP the
	 * epoch — exactly the per-tick epoch-bump storm the P1-1 staged record
	 * forbids.  Draining to publish keeps the joiner on a stable JOINING
	 * (one Phase-1 bump total, regardless of PREPARE outcomes).
	 */
	if (pr == CLUSTER_MARKER_POLL_TIMEOUT) {
		cluster_reconfig_note_marker_timeout(CLUSTER_MARKER_KIND_JOIN_PREPARE, target, elapsed_us);
	} else {
		cluster_reconfig_note_marker_slow_ack(CLUSTER_MARKER_KIND_JOIN_PREPARE, target, elapsed_us);
		if (result != CLUSTER_JOIN_MARKER_SUBMIT_ACK)
			ereport(LOG,
					(errmsg("cluster membership: PREPARE join marker for node %d did not reach "
							"a voting-disk majority; continuing best-effort (COMMITTED is the "
							"commit point)",
							target)));
	}

	join_prepare_stage.submitted = false;
	join_prepare_stage.next_node++;
	(void)cluster_reconfig_submit_join_prepare_current(now);
	return true;
}

static void
cluster_reconfig_release_join_commit_stage(void)
{
	bool had_prebump = join_commit_stage.fence_ready;
	bool had_external_rejoin = join_commit_stage.external_rejoin_consumed;
	int32 external_node_id = join_commit_stage.node_id;

	cluster_marker_async_release_stage(&join_commit_stage.async);
	cluster_marker_async_release_stage(&join_commit_stage.fence_async);
	memset(&join_commit_stage.marker, 0, sizeof(join_commit_stage.marker));
	memset(&join_commit_stage.fence_marker, 0, sizeof(join_commit_stage.fence_marker));
	memset(&join_commit_stage.event, 0, sizeof(join_commit_stage.event));
	join_commit_stage.expected_last_event_id = 0;
	join_commit_stage.node_id = -1;
	join_commit_stage.admitted_incarnation = 0;
	join_commit_stage.submitted = false;
	join_commit_stage.fence_ready = false;
	join_commit_stage.external_rejoin_consumed = false;
	if (had_prebump)
		cluster_reconfig_set_prebump_sync_active(0);
	if (had_external_rejoin)
		cluster_reconfig_external_rejoin_release_slot(external_node_id);
}

static bool
cluster_reconfig_prepare_join_commit(int32 node_id, uint64 admitted_incarnation,
									 uint64 expected_epoch)
{
	uint64 old_epoch, new_epoch;
	XLogRecPtr lsn;
	uint8 jb[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint8 remaining_dead[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint8 removed_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint64 incs[CLUSTER_MAX_NODES];
	uint64 cssd_dead_generation;
	uint64 expected_last_event_id;
	ReconfigEvent evt;
	ClusterFenceMarker fence_marker;
	int b;

	if (cluster_epoch_get_current() + 1 != expected_epoch)
		return false;

	/* The durable JCMK and ROOT gate authorize only the still-pending owner.
	 * Snapshot the exact predecessor before advancing the epoch; the publish
	 * side revalidates it after the baseline marker reaches a majority. */
	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	if (cluster_membership_get_state(node_id) != CLUSTER_MEMBER_JOINING
		|| !dead_bitmap_test_bit(ReconfigShmem->pending_join_bitmap, node_id)) {
		LWLockRelease(&ReconfigShmem->lock);
		return false;
	}
	expected_last_event_id = ReconfigShmem->last_applied.event_id;
	memcpy(remaining_dead, ReconfigShmem->last_applied.dead_bitmap, sizeof(remaining_dead));
	memcpy(removed_bitmap, ReconfigShmem->removed_bitmap, sizeof(removed_bitmap));
	LWLockRelease(&ReconfigShmem->lock);
	remaining_dead[node_id / 8] &= (uint8) ~(1u << (node_id % 8));
	cssd_dead_generation = cluster_cssd_get_dead_generation();

	CLUSTER_INJECTION_POINT("cluster-reconfig-join-commit-marker-durable");

	/* This is now a pre-bump staged path: keep GRD/cache readers unavailable
	 * until the matching majority-durable baseline is either published or the
	 * attempt fails closed. */
	cluster_reconfig_set_prebump_sync_active(1);
	cluster_epoch_advance_for_reconfig(&old_epoch, &new_epoch);
	if (new_epoch != expected_epoch) {
		cluster_reconfig_set_prebump_sync_active(0);
		return false;
	}
	lsn = GetXLogInsertRecPtr();
	cluster_epoch_set_changed_at_lsn((uint64)lsn);
	cluster_gcs_block_on_epoch_advance_exact(new_epoch, remaining_dead);
	cluster_sinval_reset_all_on_reconfig();
	cluster_tt_status_flush_all((uint32)new_epoch);

	dead_bitmap_set_bit(jb, node_id);
	memset(incs, 0, sizeof(incs));
	incs[node_id] = admitted_incarnation;

	memset(&evt, 0, sizeof(evt));
	evt.event_id = cluster_reconfig_compute_event_id_v2(
		RECONFIG_KIND_JOIN_COMMITTED, remaining_dead, jb, incs, cssd_dead_generation);
	evt.coordinator_node_id = cluster_node_id;
	evt.old_epoch = old_epoch;
	evt.new_epoch = new_epoch;
	memcpy(evt.dead_bitmap, remaining_dead, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	memcpy(evt.join_bitmap, jb, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	evt.applied_at = GetCurrentTimestamp();
	evt.observer_role = CLUSTER_RECONFIG_OBSERVER_COORDINATOR;
	evt.cssd_dead_generation = cssd_dead_generation;
	evt.reconfig_kind = RECONFIG_KIND_JOIN_COMMITTED;

	memset(&fence_marker, 0, sizeof(fence_marker));
	fence_marker.magic = CLUSTER_FENCE_MARKER_MAGIC;
	fence_marker.version = CLUSTER_FENCE_MARKER_VERSION;
	fence_marker.fence_epoch = new_epoch;
	fence_marker.fence_event_id = evt.event_id;
	fence_marker.fence_generation = cssd_dead_generation;
	fence_marker.issuer_node_id = cluster_node_id;
	/* A JOIN shrinks the excluded set, so this is the new membership baseline,
	 * including every still-dead and permanently removed origin.  BASELINE is
	 * valid even when the admitted owner was the final excluded origin. */
	fence_marker.marker_kind = CLUSTER_FENCE_MARKER_KIND_BASELINE;
	for (b = 0; b < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; b++)
		fence_marker.fenced_dead_bitmap[b] = remaining_dead[b] | removed_bitmap[b];

	join_commit_stage.event = evt;
	join_commit_stage.fence_marker = fence_marker;
	join_commit_stage.expected_last_event_id = expected_last_event_id;
	join_commit_stage.fence_async.has_staged_event = true;
	join_commit_stage.fence_async.staged_expect_epoch = new_epoch;
	join_commit_stage.fence_ready = true;

	return true;
}

static bool
cluster_reconfig_publish_prepared_join_commit(void)
{
	uint8 expected_dead[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint8 expected_fenced[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	bool publish = false;
	int b;

	if (!join_commit_stage.fence_ready
		|| cluster_epoch_get_current() != join_commit_stage.event.new_epoch) {
		return false;
	}

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	memcpy(expected_dead, ReconfigShmem->last_applied.dead_bitmap, sizeof(expected_dead));
	expected_dead[join_commit_stage.node_id / 8]
		&= (uint8) ~(1u << (join_commit_stage.node_id % 8));
	for (b = 0; b < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; b++)
		expected_fenced[b] = expected_dead[b] | ReconfigShmem->removed_bitmap[b];

	if (cluster_membership_get_state(join_commit_stage.node_id) == CLUSTER_MEMBER_JOINING
		&& dead_bitmap_test_bit(ReconfigShmem->pending_join_bitmap, join_commit_stage.node_id)
		&& ReconfigShmem->last_applied.event_id == join_commit_stage.expected_last_event_id
		&& memcmp(expected_dead, join_commit_stage.event.dead_bitmap, sizeof(expected_dead)) == 0
		&& memcmp(expected_fenced, join_commit_stage.fence_marker.fenced_dead_bitmap,
				  sizeof(expected_fenced))
			   == 0) {
		/* fall through to publish */
		cluster_write_fence_authority_cache_invalidate();
		cluster_membership_set_state(join_commit_stage.node_id, CLUSTER_MEMBER_MEMBER);
		cluster_membership_record_admitted(join_commit_stage.node_id,
										   join_commit_stage.admitted_incarnation);
		ReconfigShmem->fast_rejoin_incarnation[join_commit_stage.node_id]
			= join_commit_stage.admitted_incarnation;
		ReconfigShmem->fast_rejoin_bitmap[join_commit_stage.node_id / 8]
			&= (uint8) ~(1u << (join_commit_stage.node_id % 8));
		ReconfigShmem->pending_join_bitmap[join_commit_stage.node_id / 8]
			&= (uint8) ~(1u << (join_commit_stage.node_id % 8));
		publish = true;
	}
	LWLockRelease(&ReconfigShmem->lock);

	if (!publish)
		return false;
	if (cluster_reconfig_is_clean_departed(join_commit_stage.node_id))
		pg_atomic_fetch_add_u64(&ReconfigShmem->clean_departed_cleared_count, 1);
	cluster_reconfig_clear_clean_departed(join_commit_stage.node_id);
	cluster_reconfig_publish_event(&join_commit_stage.event);
	cluster_reconfig_fast_rejoin_control_finish(join_commit_stage.node_id,
												join_commit_stage.admitted_incarnation);
	pg_atomic_fetch_add_u64(&ReconfigShmem->join_apply_count, 1);
	return true;
}

static bool
cluster_reconfig_poll_join_fence_stage(TimestampTz now)
{
	uint32 result = CLUSTER_FENCE_MARKER_SUBMIT_FAILED;
	uint64 elapsed_us = 0;
	ClusterMarkerPollResult pr;

	if (!cluster_marker_async_is_submitted(&join_commit_stage.fence_async)) {
		bool fence_submit_ok;

		fence_submit_ok = cluster_write_fence_submit_marker_async(
			&join_commit_stage.fence_async, &join_commit_stage.fence_marker,
			CLUSTER_MARKER_KIND_JOIN_COMMITTED, join_commit_stage.node_id, now);
		if (!fence_submit_ok)
			return true;
		return true;
	}

	pr = cluster_write_fence_poll_marker_async(&join_commit_stage.fence_async, now, &result,
											   &elapsed_us);
	if (pr == CLUSTER_MARKER_POLL_PENDING || pr == CLUSTER_MARKER_POLL_IDLE)
		return true;
	if (pr == CLUSTER_MARKER_POLL_TIMEOUT) {
		cluster_reconfig_note_marker_timeout(CLUSTER_MARKER_KIND_JOIN_COMMITTED,
											 join_commit_stage.node_id, elapsed_us);
		pg_atomic_fetch_add_u64(&ReconfigShmem->join_reject_count, 1);
		cluster_reconfig_release_join_commit_stage();
		return true;
	}

	cluster_reconfig_note_marker_slow_ack(CLUSTER_MARKER_KIND_JOIN_COMMITTED,
										  join_commit_stage.node_id, elapsed_us);
	if (result != CLUSTER_FENCE_MARKER_SUBMIT_ACK
		|| !cluster_reconfig_publish_prepared_join_commit()) {
		ereport(LOG, (errmsg("cluster membership: JOIN baseline marker for node %d did not "
							 "reach a voting-disk majority or its predecessor changed; "
							 "not committing (will retry)",
							 join_commit_stage.node_id)));
		pg_atomic_fetch_add_u64(&ReconfigShmem->join_reject_count, 1);
		cluster_reconfig_release_join_commit_stage();
		return true;
	}

	cluster_reconfig_release_join_commit_stage();
	return true;
}

static bool
cluster_reconfig_poll_join_commit_stage(void)
{
	TimestampTz now;
	uint32 result = CLUSTER_JOIN_MARKER_SUBMIT_FAILED;
	uint64 elapsed_us = 0;
	uint64 admitted_incarnation = 0;
	uint64 admitted_generation = 0;
	ClusterMarkerPollResult pr;

	if (!join_commit_stage.async.has_staged_event)
		return false;

	now = GetCurrentTimestamp();
	if (join_commit_stage.fence_ready)
		return cluster_reconfig_poll_join_fence_stage(now);
	if (!join_commit_stage.submitted) {
		if (!cluster_reconfig_submit_join_marker_async(
				&join_commit_stage.async, join_commit_stage.node_id, &join_commit_stage.marker,
				CLUSTER_MARKER_KIND_JOIN_COMMITTED, now))
			return true;
		join_commit_stage.submitted = true;
		return true;
	}

	pr = cluster_reconfig_poll_join_marker_async(&join_commit_stage.async, now, &result,
												 &elapsed_us);
	if (pr == CLUSTER_MARKER_POLL_PENDING || pr == CLUSTER_MARKER_POLL_IDLE)
		return true;
	if (pr == CLUSTER_MARKER_POLL_TIMEOUT) {
		cluster_reconfig_note_marker_timeout(CLUSTER_MARKER_KIND_JOIN_COMMITTED,
											 join_commit_stage.node_id, elapsed_us);
		cluster_reconfig_release_join_commit_stage();
		return true;
	}

	cluster_reconfig_note_marker_slow_ack(CLUSTER_MARKER_KIND_JOIN_COMMITTED,
										  join_commit_stage.node_id, elapsed_us);
	if (result != CLUSTER_JOIN_MARKER_SUBMIT_ACK) {
		ereport(LOG,
				(errmsg("cluster membership: COMMITTED join marker for node %d did not reach a "
						"voting-disk majority; not committing (will retry)",
						join_commit_stage.node_id)));
		cluster_reconfig_release_join_commit_stage();
		return true;
	}
	/* STOP04 §11.7/§11.9: a consumed external rejoin can never publish MEMBER
	 * without the mandatory durable excluded-set shrink. */
	if (join_commit_stage.external_rejoin_consumed
		&& cluster_write_fence_enforcement != CLUSTER_WRITE_FENCE_ENFORCE_ON) {
		pg_atomic_fetch_add_u64(&ReconfigShmem->join_reject_count, 1);
		cluster_reconfig_release_join_commit_stage();
		return true;
	}

	if (!cluster_reconfig_get_observed_slot(join_commit_stage.node_id, &admitted_incarnation,
											&admitted_generation)
		|| admitted_incarnation != join_commit_stage.admitted_incarnation
		|| cluster_membership_vet_joiner(join_commit_stage.node_id, admitted_incarnation,
										 admitted_generation)
			   != CLUSTER_JOIN_ACCEPT
		|| (!join_commit_stage.external_rejoin_consumed
			&& !cluster_recovery_owner_rejoin_v1(join_commit_stage.node_id,
												 join_commit_stage.admitted_incarnation))
		|| !cluster_reconfig_prepare_join_commit(join_commit_stage.node_id,
												 join_commit_stage.admitted_incarnation,
												 join_commit_stage.async.staged_expect_epoch)) {
		pg_atomic_fetch_add_u64(&ReconfigShmem->join_reject_count, 1);
		cluster_reconfig_release_join_commit_stage();
		return true;
	}

	if (cluster_write_fence_enforcement == CLUSTER_WRITE_FENCE_ENFORCE_ON)
		return cluster_reconfig_poll_join_fence_stage(now);
	if (!cluster_reconfig_publish_prepared_join_commit())
		pg_atomic_fetch_add_u64(&ReconfigShmem->join_reject_count, 1);
	cluster_reconfig_release_join_commit_stage();
	return true;
}

/*
 * spec-5.15 D4 — coordinator-side join driver (called from the tick when
 * online_join is on and self is the min-MEMBER coordinator).  Phase-1: fresh
 * join edges -> apply_join_as_coordinator (JOIN_PENDING).  Phase-2: pending joins
 * whose convergence is met -> re-vet (TOCTOU, INV-J1) -> commit_member.
 */
static void
cluster_reconfig_drive_joins(int coordinator, int32 control_target, uint64 control_incarnation)
{
	ClusterReconfigState *state = ReconfigShmem;
	uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint8 pending_snapshot[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint64 joiner_incarnations[CLUSTER_MAX_NODES];
	int n_join;
	int i;

	if (state == NULL)
		return;
	if (cluster_reconfig_poll_join_prepare_stage())
		return;
	if (cluster_reconfig_poll_join_commit_stage())
		return;

	/* Phase-1 detection + a snapshot of the current pending set, under the lock
	 * (compute_join_bitmap reads membership_state). */
	LWLockAcquire(&state->lock, LW_SHARED);
	n_join = cluster_reconfig_compute_join_bitmap(join_bitmap);
	memcpy(pending_snapshot, state->pending_join_bitmap, sizeof(pending_snapshot));
	LWLockRelease(&state->lock);
	if (control_target >= 0) {
		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			if (i == control_target)
				continue;
			join_bitmap[i / 8] &= (uint8) ~(1u << (i % 8));
			pending_snapshot[i / 8] &= (uint8) ~(1u << (i % 8));
		}
		n_join = dead_bitmap_test_bit(join_bitmap, control_target) ? 1 : 0;
	}

	if (n_join > 0) {
		int external_selected = -1;

		memset(joiner_incarnations, 0, sizeof(joiner_incarnations));
		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			if (!dead_bitmap_test_bit(join_bitmap, i))
				continue;
			(void)cluster_reconfig_get_observed_slot(i, &joiner_incarnations[i], NULL);
		}
		if (control_target >= 0 && joiner_incarnations[control_target] != control_incarnation)
			return;
		/* STOP04 §11.9 F→...→P: a previously admitted/excluded origin
		 * cannot enter JOIN_PENDING before its exact provider operation has
		 * reached physical ON.  If present, serialize that candidate into the
		 * required singleton P; clean first joins remain on the ordinary path. */
		if (cluster_external_fence_runtime_active()) {
			for (i = 0; i < CLUSTER_MAX_NODES; i++) {
				if (!dead_bitmap_test_bit(join_bitmap, i)
					|| !cluster_reconfig_external_rejoin_required(i))
					continue;
				if (external_selected < 0
					&& cluster_reconfig_external_rejoin_authorized(i, joiner_incarnations[i]))
					external_selected = i;
				else
					join_bitmap[i / 8] &= (uint8) ~(1u << (i % 8));
			}
			if (external_selected >= 0) {
				memset(join_bitmap, 0, sizeof(join_bitmap));
				dead_bitmap_set_bit(join_bitmap, external_selected);
			}
		}
		n_join = 0;
		for (i = 0; i < CLUSTER_MAX_NODES; i++)
			if (dead_bitmap_test_bit(join_bitmap, i))
				n_join++;
		if (n_join == 0)
			return;
		cluster_reconfig_apply_join_as_coordinator(join_bitmap, coordinator, joiner_incarnations);
		return;
	}

	/* Phase-2: commit pending joins that have converged.  The just-added Phase-1
	 * joiner is NOT in pending_snapshot (set after the snapshot), so it commits on
	 * a later tick — the intended two-phase, two-tick bracket. */
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		uint64 admitted_incarnation = 0;
		uint64 admitted_generation = 0;

		if (!dead_bitmap_test_bit(pending_snapshot, i))
			continue;
		if (!cluster_reconfig_join_converged(i))
			continue;
		if (!cluster_reconfig_get_observed_slot(i, &admitted_incarnation, &admitted_generation))
			continue; /* no valid slot now -> wait */
		if (control_target >= 0
			&& (i != control_target || admitted_incarnation != control_incarnation))
			continue;
		/* authoritative re-vet at the commit point (TOCTOU, INV-J1): stale /
		 * not-ready / out-of-quorum -> skip (the joiner times out -> REJECT). */
		if (cluster_membership_vet_joiner(i, admitted_incarnation, admitted_generation)
			!= CLUSTER_JOIN_ACCEPT) {
			pg_atomic_fetch_add_u64(&ReconfigShmem->join_reject_count, 1);
			continue;
		}
		if (!cluster_reconfig_external_rejoin_prepare_commit(i, admitted_incarnation))
			continue;
		(void)cluster_reconfig_commit_member(i, admitted_incarnation);
	}
}


/* ============================================================
 * spec-5.15 D5 — joiner-side write gate + admission.
 * ============================================================
 */

ClusterJoinGateVerdict
cluster_reconfig_self_join_gate_verdict(void)
{
	if (ReconfigShmem == NULL)
		return CLUSTER_JOIN_GATE_ALLOW;
	/* single-byte reads — naturally atomic; this is a hot xact-entry check. */
	if (ReconfigShmem->self_join_failed)
		return CLUSTER_JOIN_GATE_BLOCK_53R61;
	if (!ReconfigShmem->self_join_admitted)
		return CLUSTER_JOIN_GATE_BLOCK_53R60;
	return CLUSTER_JOIN_GATE_ALLOW;
}

static bool
cluster_reconfig_is_local_admitted_replacement(const ClusterReplacementEpisode *episode)
{
	return cluster_replacement_episode_is_valid(episode)
		   && episode->phase == CLUSTER_REPLACEMENT_EPISODE_ADMITTED
		   && episode->readiness_flags == CLUSTER_REPLACEMENT_EPISODE_READINESS_MASK
		   && episode->target_node_id == cluster_node_id;
}

static bool
cluster_reconfig_has_replacement_episode(const ClusterReplacementEpisode *episode)
{
	/* Only the all-zero canonical image means the ordinary-join lane.  A torn
	 * or otherwise invalid nonempty mirror remains replacement state and must
	 * fail closed; validation failure is never an ordinary-open fallback. */
	return !cluster_replacement_episode_is_empty(episode);
}

bool
cluster_reconfig_lmon_observe_replacement_ready(const ClusterReplacementPhase3HandoffItem *item)
{
	ClusterReplacementCommitMarkerV3 marker;
	ClusterReplacementEpisode *episode;
	uint64 request_seq;
	uint64 completion_seq;
	bool accepted = false;

	if (ReconfigShmem == NULL || item == NULL || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES)
		return false;

	request_seq = pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq);
	completion_seq = pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq);
	if (request_seq == 0 || request_seq != completion_seq
		|| pg_atomic_read_u32(&ReconfigShmem->join_marker_result) != CLUSTER_JOIN_MARKER_SUBMIT_ACK)
		return false;
	pg_read_barrier();

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	episode = &ReconfigShmem->replacement_episode;
	if (pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq) != request_seq
		|| pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq) != completion_seq
		|| pg_atomic_read_u32(&ReconfigShmem->join_marker_result) != CLUSTER_JOIN_MARKER_SUBMIT_ACK
		|| !cluster_replacement_marker_v3_decode(ReconfigShmem->join_pending_marker,
												 item->message.target_node_id, &marker)
		|| !cluster_replacement_episode_is_valid(episode)
		|| (episode->phase != CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED
			&& episode->phase != CLUSTER_REPLACEMENT_EPISODE_POST_EPOCH)
		|| (episode->readiness_flags & CLUSTER_REPLACEMENT_EPISODE_INTENT_CLEARED) == 0
		|| marker.phase != CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED
		|| marker.ready_state_generation != 0
		|| marker.generation != item->message.body.phase3.jcmk_generation
		|| episode->state_generation != item->message.body.phase3.episode_state_generation
		|| episode->target_node_id != item->authenticated_source_node_id
		|| episode->target_node_id != item->message.target_node_id
		|| episode->coordinator_node_id != cluster_node_id
		|| episode->coordinator_node_id != item->local_receiver_node_id
		|| episode->baseline_epoch != item->message.epoch
		|| episode->reserved_or_committed_epoch != cluster_epoch_get_current()
		|| episode->request_nonce != item->message.request_nonce
		|| episode->old_admitted_incarnation != item->message.identity0
		|| episode->fresh_incarnation != item->message.identity1
		|| episode->grammar_fingerprint != item->message.grammar_fingerprint
		|| marker.target_node_id != episode->target_node_id
		|| marker.old_admitted_incarnation != episode->old_admitted_incarnation
		|| marker.fresh_incarnation != episode->fresh_incarnation
		|| marker.baseline_epoch != episode->baseline_epoch
		|| marker.reserved_or_committed_epoch != episode->reserved_or_committed_epoch
		|| marker.request_nonce != episode->request_nonce
		|| memcmp(marker.expected_purge_survivors, episode->expected_survivors,
				  sizeof(marker.expected_purge_survivors))
			   != 0
		|| marker.grammar_fingerprint != episode->grammar_fingerprint)
		goto out;

	episode->readiness_flags |= CLUSTER_REPLACEMENT_EPISODE_R4A_TARGET_READY;
	accepted = true;

out:
	LWLockRelease(&ReconfigShmem->lock);
	return accepted;
}

bool
cluster_reconfig_lmon_build_replacement_admitted(const ClusterEpochAuthorityValue *terminal_head,
												 ClusterReplacementCommitMarkerV3 *out_marker)
{
	ClusterReplacementCommitMarkerV3 committed;
	ClusterReplacementCommitMarkerV3 candidate;
	ClusterReplacementEpisode capability_episode;
	ClusterReplacementEpisode *episode;
	uint8 encoded[CLUSTER_JCMK_REPLACEMENT_BYTES];
	uint8 subject[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint64 request_seq;
	uint64 completion_seq;
	bool built = false;

	if (ReconfigShmem == NULL || terminal_head == NULL || out_marker == NULL || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES
		|| !cluster_epoch_authority_value_is_valid(terminal_head,
												   CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT)
		|| terminal_head->transition != CLUSTER_EPOCH_AUTHORITY_COMMIT_CLOSED
		|| terminal_head->event_kind != CLUSTER_EPOCH_EVENT_SAME_NODE_REPLACEMENT)
		return false;
	if (!cluster_reconfig_replacement_candidate2_capabilities_current(&capability_episode))
		return false;

	request_seq = pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq);
	completion_seq = pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq);
	if (request_seq == 0 || request_seq != completion_seq
		|| pg_atomic_read_u32(&ReconfigShmem->join_marker_result) != CLUSTER_JOIN_MARKER_SUBMIT_ACK)
		return false;
	pg_read_barrier();

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	episode = &ReconfigShmem->replacement_episode;
	if (episode->target_node_id >= 0 && episode->target_node_id < CLUSTER_MAX_NODES)
		subject[episode->target_node_id / 8] = (uint8)(1u << (episode->target_node_id % 8));
	if (pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq) != request_seq
		|| pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq) != completion_seq
		|| pg_atomic_read_u32(&ReconfigShmem->join_marker_result) != CLUSTER_JOIN_MARKER_SUBMIT_ACK
		|| !cluster_replacement_marker_v3_decode(ReconfigShmem->join_pending_marker,
												 episode->target_node_id, &committed)
		|| memcmp(episode, &capability_episode, sizeof(*episode)) != 0
		|| !cluster_replacement_episode_is_valid(episode)
		|| !cluster_reconfig_replacement_membership_current_locked(episode)
		|| (episode->phase != CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED
			&& episode->phase != CLUSTER_REPLACEMENT_EPISODE_POST_EPOCH)
		|| episode->readiness_flags != CLUSTER_REPLACEMENT_EPISODE_READINESS_MASK
		|| episode->state_generation == 0
		|| episode->reserved_or_committed_epoch != cluster_epoch_get_current()
		|| committed.phase != CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED
		|| committed.ready_state_generation != 0 || committed.generation == UINT64_MAX
		|| committed.target_node_id != episode->target_node_id
		|| committed.old_admitted_incarnation != episode->old_admitted_incarnation
		|| committed.fresh_incarnation != episode->fresh_incarnation
		|| committed.baseline_epoch != episode->baseline_epoch
		|| committed.reserved_or_committed_epoch != episode->reserved_or_committed_epoch
		|| committed.request_nonce != episode->request_nonce
		|| memcmp(committed.expected_purge_survivors, episode->expected_survivors,
				  sizeof(committed.expected_purge_survivors))
			   != 0
		|| committed.grammar_fingerprint != episode->grammar_fingerprint
		|| terminal_head->request_origin_node != episode->target_node_id
		|| terminal_head->target_node_id != episode->target_node_id
		|| terminal_head->baseline_epoch != episode->baseline_epoch
		|| terminal_head->reserved_epoch != episode->reserved_or_committed_epoch
		|| terminal_head->old_incarnation != episode->old_admitted_incarnation
		|| terminal_head->fresh_incarnation != episode->fresh_incarnation
		|| terminal_head->request_nonce != episode->request_nonce
		|| memcmp(terminal_head->authority_member_bitmap, episode->expected_survivors,
				  sizeof(terminal_head->authority_member_bitmap))
			   != 0
		|| memcmp(terminal_head->event_subject_bitmap, subject,
				  sizeof(terminal_head->event_subject_bitmap))
			   != 0
		|| terminal_head->grammar_fingerprint != episode->grammar_fingerprint)
		goto out;

	candidate = committed;
	candidate.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED;
	candidate.generation++;
	candidate.ready_state_generation = episode->state_generation;
	if (!cluster_replacement_marker_v3_encode(&candidate, encoded))
		goto out;
	*out_marker = candidate;
	built = true;

out:
	LWLockRelease(&ReconfigShmem->lock);
	return built;
}

bool
cluster_reconfig_lmon_finalize_replacement_admitted(
	const ClusterEpochAuthorityValue *terminal_head,
	const ClusterReplacementCommitMarkerV3 *admitted_marker)
{
	ClusterReplacementCommitMarkerV3 durable;
	ClusterReplacementEpisode capability_episode;
	ClusterReplacementEpisode *episode;
	uint8 subject[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint64 request_seq;
	uint64 completion_seq;
	bool finalized = false;

	if (ReconfigShmem == NULL || terminal_head == NULL || admitted_marker == NULL
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| !cluster_epoch_authority_value_is_valid(terminal_head,
												   CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT)
		|| terminal_head->transition != CLUSTER_EPOCH_AUTHORITY_COMMIT_CLOSED
		|| terminal_head->event_kind != CLUSTER_EPOCH_EVENT_SAME_NODE_REPLACEMENT
		|| admitted_marker->phase != CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED
		|| admitted_marker->ready_state_generation == 0)
		return false;
	if (!cluster_reconfig_replacement_candidate2_capabilities_current(&capability_episode))
		return false;

	request_seq = pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq);
	completion_seq = pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq);
	if (request_seq == 0 || request_seq != completion_seq
		|| pg_atomic_read_u32(&ReconfigShmem->join_marker_result) != CLUSTER_JOIN_MARKER_SUBMIT_ACK)
		return false;
	pg_read_barrier();

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	episode = &ReconfigShmem->replacement_episode;
	if (episode->target_node_id >= 0 && episode->target_node_id < CLUSTER_MAX_NODES)
		subject[episode->target_node_id / 8] = (uint8)(1u << (episode->target_node_id % 8));
	if (pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq) != request_seq
		|| pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq) != completion_seq
		|| pg_atomic_read_u32(&ReconfigShmem->join_marker_result) != CLUSTER_JOIN_MARKER_SUBMIT_ACK
		|| !cluster_replacement_marker_v3_decode(ReconfigShmem->join_pending_marker,
												 admitted_marker->target_node_id, &durable)
		|| !cluster_replacement_marker_v3_same_image(&durable, admitted_marker)
		|| memcmp(episode, &capability_episode, sizeof(*episode)) != 0
		|| !cluster_replacement_episode_is_valid(episode)
		|| !cluster_reconfig_replacement_membership_current_locked(episode)
		|| (episode->phase != CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED
			&& episode->phase != CLUSTER_REPLACEMENT_EPISODE_POST_EPOCH)
		|| episode->readiness_flags != CLUSTER_REPLACEMENT_EPISODE_READINESS_MASK
		|| episode->state_generation != admitted_marker->ready_state_generation
		|| episode->coordinator_node_id != cluster_node_id
		|| episode->target_node_id != admitted_marker->target_node_id
		|| episode->old_admitted_incarnation != admitted_marker->old_admitted_incarnation
		|| episode->fresh_incarnation != admitted_marker->fresh_incarnation
		|| episode->baseline_epoch != admitted_marker->baseline_epoch
		|| episode->reserved_or_committed_epoch != admitted_marker->reserved_or_committed_epoch
		|| episode->reserved_or_committed_epoch != cluster_epoch_get_current()
		|| episode->request_nonce != admitted_marker->request_nonce
		|| memcmp(episode->expected_survivors, admitted_marker->expected_purge_survivors,
				  sizeof(episode->expected_survivors))
			   != 0
		|| episode->grammar_fingerprint != admitted_marker->grammar_fingerprint
		|| terminal_head->request_origin_node != episode->target_node_id
		|| terminal_head->target_node_id != episode->target_node_id
		|| terminal_head->baseline_epoch != episode->baseline_epoch
		|| terminal_head->reserved_epoch != episode->reserved_or_committed_epoch
		|| terminal_head->old_incarnation != episode->old_admitted_incarnation
		|| terminal_head->fresh_incarnation != episode->fresh_incarnation
		|| terminal_head->request_nonce != episode->request_nonce
		|| memcmp(terminal_head->authority_member_bitmap, episode->expected_survivors,
				  sizeof(terminal_head->authority_member_bitmap))
			   != 0
		|| memcmp(terminal_head->event_subject_bitmap, subject,
				  sizeof(terminal_head->event_subject_bitmap))
			   != 0
		|| terminal_head->grammar_fingerprint != episode->grammar_fingerprint)
		goto out;

	episode->phase = CLUSTER_REPLACEMENT_EPISODE_ADMITTED;
	cluster_membership_set_state(episode->target_node_id, CLUSTER_MEMBER_MEMBER);
	cluster_membership_record_admitted(episode->target_node_id, episode->fresh_incarnation);
	finalized = true;

out:
	LWLockRelease(&ReconfigShmem->lock);
	return finalized;
}

/*
 * Snapshot the current-coordinator happy-path handoff into D13.  The marker
 * mailbox ACK is the completion of the existing voting-disk majority writer;
 * this routine performs no I/O and grants no service.  It only co-samples the
 * exact local ADMITTED image, episode, epoch and closed MEMBER metadata under
 * the reconfig lock.  Formation takeover still needs its own majority-read
 * carrier and is deliberately not inferred from this process-local mailbox.
 */
bool
cluster_reconfig_lmon_snapshot_replacement_admitted(ClusterReplacementEpisode *out_episode,
													ClusterReplacementCommitMarkerV3 *out_marker)
{
	ClusterReplacementEpisode observed_episode;
	ClusterReplacementCommitMarkerV3 observed_marker;
	ClusterReplacementEpisode *episode;
	uint64 request_seq;
	uint64 completion_seq;
	bool valid = false;
	int node;

	if (ReconfigShmem == NULL || out_episode == NULL || out_marker == NULL || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES)
		return false;

	request_seq = pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq);
	completion_seq = pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq);
	if (request_seq == 0 || request_seq != completion_seq
		|| pg_atomic_read_u32(&ReconfigShmem->join_marker_result) != CLUSTER_JOIN_MARKER_SUBMIT_ACK)
		return false;
	pg_read_barrier();

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	episode = &ReconfigShmem->replacement_episode;
	if (pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq) != request_seq
		|| pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq) != completion_seq
		|| pg_atomic_read_u32(&ReconfigShmem->join_marker_result) != CLUSTER_JOIN_MARKER_SUBMIT_ACK
		|| !cluster_replacement_episode_is_valid(episode)
		|| episode->phase != CLUSTER_REPLACEMENT_EPISODE_ADMITTED
		|| episode->readiness_flags != CLUSTER_REPLACEMENT_EPISODE_READINESS_MASK
		|| episode->coordinator_node_id != cluster_node_id
		|| episode->reserved_or_committed_epoch != cluster_epoch_get_current()
		|| !cluster_replacement_marker_v3_decode(ReconfigShmem->join_pending_marker,
												 episode->target_node_id, &observed_marker)
		|| observed_marker.phase != CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED
		|| observed_marker.ready_state_generation != episode->state_generation
		|| observed_marker.target_node_id != episode->target_node_id
		|| observed_marker.old_admitted_incarnation != episode->old_admitted_incarnation
		|| observed_marker.fresh_incarnation != episode->fresh_incarnation
		|| observed_marker.baseline_epoch != episode->baseline_epoch
		|| observed_marker.reserved_or_committed_epoch != episode->reserved_or_committed_epoch
		|| observed_marker.request_nonce != episode->request_nonce
		|| memcmp(observed_marker.expected_purge_survivors, episode->expected_survivors,
				  sizeof(observed_marker.expected_purge_survivors))
			   != 0
		|| observed_marker.grammar_fingerprint != episode->grammar_fingerprint
		|| cluster_membership_get_last_admitted_incarnation(episode->target_node_id)
			   != episode->fresh_incarnation)
		goto out;

	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		bool expected_member = node == episode->target_node_id
							   || dead_bitmap_test_bit(episode->expected_survivors, node);

		if (cluster_membership_is_member(node) != expected_member)
			goto out;
	}

	observed_episode = *episode;
	valid = true;

out:
	LWLockRelease(&ReconfigShmem->lock);
	if (!valid)
		return false;
	*out_episode = observed_episode;
	*out_marker = observed_marker;
	return true;
}

/*
 * Expose the already-majority-selected cold-formation marker only while the
 * local reconfiguration state still proves an untouched four-node formation.
 * The marker generation is qvotec's publication latch: all marker fields are
 * written before it and the generation is sampled again after the locked
 * membership/reconfiguration image.  This creates no authority and retains
 * no evidence outside the caller's stack.
 */
bool
cluster_reconfig_snapshot_initial_clean_formation(ClusterInitialCleanFormationSnapshot *out)
{
	ClusterInitialCleanFormationSnapshot snapshot;
	uint64 bootstrap_incarnation[4] = { 0 };
	uint64 bootstrap_generation[4] = { 0 };
	uint64 bootstrap_epoch[4] = { 0 };
	bool bootstrap_fresh[4] = { false };
	uint64 bootstrap_seq = 0;
	uint64 bootstrap_seq_after = 0;
	uint64 marker_generation;
	uint64 marker_generation_after;
	uint64 current_epoch;
	uint64 self_incarnation;
	uint32 bootstrap_proven = 0;
	int node;
	bool initial_basis;
	bool valid = false;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (ReconfigShmem == NULL || cluster_node_id < 0 || cluster_node_id >= 4)
		return false;

	marker_generation = pg_atomic_read_u64(&ReconfigShmem->observed_formation_marker_generation);
	memset(&snapshot, 0, sizeof(snapshot));
	current_epoch = cluster_epoch_get_current();
	initial_basis = marker_generation == 0;
	if (initial_basis) {
		if (current_epoch != CLUSTER_EPOCH_INITIAL)
			return false;
		bootstrap_seq = pg_atomic_read_u64(&ReconfigShmem->observed_bootstrap_seq);
		if (bootstrap_seq == 0 || (bootstrap_seq & UINT64_C(1)) != 0)
			return false;
		pg_read_barrier();
		if (pg_atomic_read_u64(&ReconfigShmem->bootstrap_in_quorum) == 0)
			return false;
		for (node = 0; node < 4; node++) {
			bootstrap_incarnation[node]
				= pg_atomic_read_u64(&ReconfigShmem->observed_incarnation[node]);
			bootstrap_generation[node]
				= pg_atomic_read_u64(&ReconfigShmem->observed_generation[node]);
			bootstrap_epoch[node] = pg_atomic_read_u64(&ReconfigShmem->observed_epoch[node]);
			bootstrap_fresh[node]
				= pg_atomic_read_u64(&ReconfigShmem->observed_fresh_alive[node]) != 0;
		}
		pg_read_barrier();
		if (pg_atomic_read_u64(&ReconfigShmem->observed_bootstrap_seq) != bootstrap_seq
			|| pg_atomic_read_u64(&ReconfigShmem->observed_formation_marker_generation) != 0)
			return false;
		snapshot.formation_epoch = current_epoch;
	} else {
		pg_read_barrier();
		snapshot.formation_marker_generation = marker_generation;
		snapshot.formation_epoch
			= pg_atomic_read_u64(&ReconfigShmem->observed_formation_marker_epoch);
		snapshot.arbiter_node
			= pg_atomic_read_u64(&ReconfigShmem->observed_formation_marker_arbiter_node);
		snapshot.arbiter_incarnation
			= pg_atomic_read_u64(&ReconfigShmem->observed_formation_marker_arbiter_incarnation);
		for (node = 0; node < 4; node++)
			snapshot.admitted_incarnation[node]
				= pg_atomic_read_u64(&ReconfigShmem->observed_formation_marker_incarnation[node]);
		pg_read_barrier();
		if (pg_atomic_read_u64(&ReconfigShmem->observed_formation_marker_generation)
			!= marker_generation)
			return false;
	}

	self_incarnation = cluster_qvotec_get_self_incarnation();
	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	if (snapshot.formation_epoch != current_epoch || ReconfigShmem->last_applied.event_id != 0
		|| ReconfigShmem->last_applied.reconfig_kind != RECONFIG_KIND_NONE
		|| pg_atomic_read_u64(&ReconfigShmem->apply_counter) != 0
		|| pg_atomic_read_u32(&ReconfigShmem->prebump_sync_active) != 0
		|| ReconfigShmem->self_join_admitted != 1 || ReconfigShmem->self_join_failed != 0
		|| !dead_bitmap_is_zero(ReconfigShmem->pending_join_bitmap)
		|| !dead_bitmap_is_zero(ReconfigShmem->clean_departed_bitmap)
		|| !dead_bitmap_is_zero(ReconfigShmem->removed_bitmap)
		|| !dead_bitmap_is_zero(ReconfigShmem->fast_rejoin_bitmap)
		|| !cluster_replacement_episode_is_empty(&ReconfigShmem->replacement_episode)
		|| pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq) != 0
		|| pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq) != 0
		|| (initial_basis
			&& (pg_atomic_read_u64(&ReconfigShmem->formation_marker_request_seq) != 0
				|| pg_atomic_read_u64(&ReconfigShmem->formation_marker_completion_seq) != 0
				|| pg_atomic_read_u64(&ReconfigShmem->formation_marker_max_generation) != 0))
		|| (!initial_basis
			&& (snapshot.arbiter_node >= 4 || snapshot.arbiter_incarnation == 0
				|| snapshot.admitted_incarnation[snapshot.arbiter_node]
					   != snapshot.arbiter_incarnation)))
		goto out;

	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		if ((node < 4) != (cluster_conf_lookup_node(node) != NULL))
			goto out;
		if (node < 4) {
			uint64 admitted_incarnation = cluster_membership_get_last_admitted_incarnation(node);

			if (admitted_incarnation == 0
				|| cluster_membership_get_state(node) != CLUSTER_MEMBER_MEMBER
				|| (!initial_basis && admitted_incarnation != snapshot.admitted_incarnation[node]))
				goto out;
			if (initial_basis) {
				snapshot.admitted_incarnation[node] = admitted_incarnation;
				if (node == cluster_node_id) {
					if (self_incarnation == 0 || admitted_incarnation != self_incarnation)
						goto out;
					bootstrap_proven++;
				} else if (bootstrap_fresh[node] && bootstrap_generation[node] > 0
						   && bootstrap_epoch[node] == CLUSTER_EPOCH_INITIAL) {
					if (bootstrap_incarnation[node] != admitted_incarnation)
						goto out;
					bootstrap_proven++;
				} else if (bootstrap_epoch[node] > CLUSTER_EPOCH_INITIAL)
					goto out;
			}
			snapshot.members_lo |= UINT64_C(1) << node;
		} else if (cluster_membership_get_state(node) == CLUSTER_MEMBER_MEMBER
				   || (!initial_basis
					   && pg_atomic_read_u64(
							  &ReconfigShmem->observed_formation_marker_incarnation[node])
							  != 0))
			goto out;
	}
	if (initial_basis && bootstrap_proven < UINT32_C(3))
		goto out;
	marker_generation_after
		= pg_atomic_read_u64(&ReconfigShmem->observed_formation_marker_generation);
	bootstrap_seq_after = pg_atomic_read_u64(&ReconfigShmem->observed_bootstrap_seq);
	if ((!initial_basis && marker_generation_after != marker_generation)
		|| (initial_basis
			&& (marker_generation_after != 0 || bootstrap_seq_after != bootstrap_seq
				|| (bootstrap_seq_after & UINT64_C(1)) != 0
				|| pg_atomic_read_u64(&ReconfigShmem->bootstrap_in_quorum) == 0))
		|| cluster_epoch_get_current() != current_epoch)
		goto out;
	valid = !initial_basis || cluster_qvotec_in_quorum();

out:
	LWLockRelease(&ReconfigShmem->lock);
	if (valid)
		*out = snapshot;
	return valid;
}

void
cluster_reconfig_lmon_replacement_ready_tick(void)
{
	ClusterR4PrerequisiteSnapshot snapshot;
	ClusterICSendResult send_result;
	int32 coordinator_node_id;
	ClusterQvotecMailboxCompletion ignored_completion;
	ClusterMarkerPollResult marker_poll;
	uint32 marker_result = CLUSTER_JOIN_MARKER_SUBMIT_FAILED;
	uint64 elapsed_us = 0;

	if (!cluster_enabled || MyBackendType != B_LMON || !cluster_qvotec_in_quorum()
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		return;
	if (replacement_ready_stage.phase == CLUSTER_REPLACEMENT_READY_STAGE_IDLE) {
		cluster_marker_async_init(&replacement_ready_stage.marker_async);
		(void)cluster_reconfig_lmon_submit_ready_observer_pair(GetCurrentTimestamp());
		return;
	}
	if (replacement_ready_stage.phase == CLUSTER_REPLACEMENT_READY_STAGE_DRAIN_AUTHORITY) {
		/* The pair was irrevocably discarded.  Poll once only to consume a
		 * visible completion; a pending or restart-reset authority mailbox is
		 * handled by the next fresh submit's existing BUSY/ACCEPTED result. */
		(void)cluster_qvotec_authority_lmon_poll_completion(
			replacement_ready_stage.authority_request_seq, &ignored_completion);
		cluster_reconfig_release_ready_stage();
		return;
	}
	if (replacement_ready_stage.phase == CLUSTER_REPLACEMENT_READY_STAGE_WAIT_PAIR) {
		if (!replacement_ready_stage.marker_completed) {
			marker_poll = cluster_reconfig_poll_join_marker_async(
				&replacement_ready_stage.marker_async, GetCurrentTimestamp(), &marker_result,
				&elapsed_us);
			if (marker_poll == CLUSTER_MARKER_POLL_PENDING
				|| marker_poll == CLUSTER_MARKER_POLL_IDLE)
				return;
			if (marker_poll != CLUSTER_MARKER_POLL_ACKED
				|| marker_result != CLUSTER_JOIN_MARKER_SUBMIT_ACK) {
				memset(&join_marker_lmon_owner, 0, sizeof(join_marker_lmon_owner));
				replacement_ready_stage.phase = CLUSTER_REPLACEMENT_READY_STAGE_DRAIN_AUTHORITY;
				return;
			}
			replacement_ready_stage.marker_completed = true;
		}

		snapshot = cluster_undo_block0_r4_prerequisite_snapshot();
		if (snapshot.status != CLUSTER_R4_PREREQUISITE_R4A_READY || !snapshot.ready)
			return;
		if (!cluster_replacement_wire_encode_phase3_snapshot(
				&snapshot, replacement_ready_stage.cached_image)) {
			cluster_reconfig_release_ready_stage();
			return;
		}
		replacement_ready_stage.cached_snapshot = snapshot;
		replacement_ready_stage.phase = CLUSTER_REPLACEMENT_READY_STAGE_CACHED;
	}

	if (!cluster_reconfig_lmon_ready_cache_current(&coordinator_node_id))
		return;

	send_result = cluster_ic_send_envelope(PGRAC_IC_MSG_GES_REQUEST, coordinator_node_id,
										   replacement_ready_stage.cached_image,
										   CLUSTER_REPLACEMENT_WIRE_BYTES);
	if (send_result == CLUSTER_IC_SEND_HARD_ERROR)
		cluster_ic_tier1_close_peer(coordinator_node_id, "replacement READY send hard error");
}

ClusterR4PrerequisiteSnapshot
cluster_reconfig_r4_prerequisite_snapshot(void)
{
	ClusterR4PrerequisiteSnapshot dormant = {
		.status = CLUSTER_R4_PREREQUISITE_RF_DEFERRED,
		.ready = false,
		.reserved0 = { 0, 0, 0 },
		.target_node_id = -1,
	};
	ClusterR4PrerequisiteSnapshot snapshot;
	ClusterQvotecMailboxCompletion completion;
	ClusterQvotecMailboxCompletion repeated_completion;
	ClusterEpochAuthorityValue head;
	ClusterEpochBallotId ballot;
	ClusterReplacementCommitMarkerV3 marker;
	ClusterReplacementEpisode capability_episode;
	ClusterReplacementEpisode *episode;
	ClusterJoinMarkerMailboxOperationV1 operation;
	uint32 request_word;
	int32 request_target;
	bool capabilities_current;
	bool keep_waiting = false;
	bool valid = false;

	if (ReconfigShmem == NULL || MyBackendType != B_LMON || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES)
		return dormant;
	capabilities_current
		= cluster_reconfig_replacement_candidate2_capabilities_current(&capability_episode);
	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	if (!join_marker_lmon_owner.reserved
		|| join_marker_lmon_owner.purpose != CLUSTER_JOIN_MARKER_LMON_READY_SERIALIZE
		|| join_marker_lmon_owner.operation != CLUSTER_JOIN_MARKER_MAILBOX_VERIFY_COMMITTED_CLOSED
		|| join_marker_lmon_owner.marker_request_seq == 0
		|| replacement_ready_stage.phase != CLUSTER_REPLACEMENT_READY_STAGE_WAIT_PAIR
		|| !replacement_ready_stage.marker_completed
		|| replacement_ready_stage.authority_request_seq == 0
		|| replacement_ready_stage.marker_request_seq != join_marker_lmon_owner.marker_request_seq)
		goto out;
	if (!capabilities_current
		|| memcmp(&ReconfigShmem->replacement_episode, &capability_episode,
				  sizeof(capability_episode))
			   != 0
		|| cluster_qvotec_get_status() != CLUSTER_QVOTEC_READY)
		goto out;
	if (!cluster_qvotec_authority_lmon_poll_completion(
			replacement_ready_stage.authority_request_seq, &completion)) {
		keep_waiting = true;
		goto out;
	}
	if (completion.result != CLUSTER_QVOTEC_MAILBOX_CHOSEN
		|| completion.actor_phase != CLUSTER_QVOTEC_ACTOR_RECOVER_SCAN_B
		|| !cluster_epoch_authority_value_decode(completion.completion_value,
												 CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, &head)
		|| !cluster_epoch_ballot_id_decode(completion.completion_ballot, &ballot))
		goto out;
	if (pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq)
			!= replacement_ready_stage.marker_request_seq
		|| pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq)
			   != replacement_ready_stage.marker_request_seq
		|| pg_atomic_read_u32(&ReconfigShmem->join_marker_result) != CLUSTER_JOIN_MARKER_SUBMIT_ACK)
		goto out;
	request_word = ReconfigShmem->join_marker_request_word;
	if (!cluster_reconfig_join_marker_request_word_decode(request_word, &operation, &request_target)
		|| operation != CLUSTER_JOIN_MARKER_MAILBOX_VERIFY_COMMITTED_CLOSED)
		goto out;
	episode = &ReconfigShmem->replacement_episode;
	if (cluster_replacement_episode_is_valid(episode)
		&& (episode->phase == CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED
			|| episode->phase == CLUSTER_REPLACEMENT_EPISODE_POST_EPOCH)
		&& (episode->readiness_flags & CLUSTER_REPLACEMENT_EPISODE_R4A_TARGET_READY) != 0
		&& episode->state_generation != 0 && episode->target_node_id == cluster_node_id
		&& episode->reserved_or_committed_epoch == cluster_epoch_get_current()
		&& ReconfigShmem->self_join_admitted == 0 && ReconfigShmem->self_join_failed == 0
		&& cluster_reconfig_replacement_membership_current_locked(episode)
		&& cluster_membership_get_last_admitted_incarnation(episode->target_node_id)
			   == episode->old_admitted_incarnation
		&& cluster_replacement_marker_v3_decode(ReconfigShmem->join_pending_marker, request_target,
												&marker)
		&& marker.generation != 0
		&& cluster_reconfig_terminal_closed_matches_episode(&head, &ballot, &marker, episode)
		&& cluster_qvotec_authority_lmon_poll_completion(
			replacement_ready_stage.authority_request_seq, &repeated_completion)
		&& memcmp(&completion, &repeated_completion, sizeof(completion)) == 0
		&& pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq)
			   == replacement_ready_stage.marker_request_seq
		&& pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq)
			   == replacement_ready_stage.marker_request_seq
		&& pg_atomic_read_u32(&ReconfigShmem->join_marker_result) == CLUSTER_JOIN_MARKER_SUBMIT_ACK
		&& ReconfigShmem->join_marker_request_word == request_word
		&& cluster_qvotec_get_status() == CLUSTER_QVOTEC_READY) {
		memset(&snapshot, 0, sizeof(snapshot));
		snapshot.status = CLUSTER_R4_PREREQUISITE_R4A_READY;
		snapshot.ready = true;
		snapshot.target_node_id = episode->target_node_id;
		snapshot.episode_state_generation = episode->state_generation;
		snapshot.jcmk_generation = marker.generation;
		snapshot.request_nonce = episode->request_nonce;
		snapshot.old_admitted_incarnation = episode->old_admitted_incarnation;
		snapshot.fresh_incarnation = episode->fresh_incarnation;
		snapshot.committed_epoch = episode->reserved_or_committed_epoch;
		snapshot.grammar_fingerprint = episode->grammar_fingerprint;
		valid = true;
		memset(&join_marker_lmon_owner, 0, sizeof(join_marker_lmon_owner));
	}

out:
	if (!valid && !keep_waiting)
		cluster_reconfig_release_ready_stage();
	LWLockRelease(&ReconfigShmem->lock);
	return valid ? snapshot : dormant;
}

bool
cluster_reconfig_r4_publish_ready(const ClusterR4PrerequisiteSnapshot *expected)
{
#if 0 /* Await the frozen Startup closure-proof carrier; see active talk STOP. */
	ClusterReplacementCommitMarkerV3 marker;
	ClusterReplacementEpisode *episode;
	ClusterR4PrerequisiteSnapshot candidate;
	uint64 request_seq;
	uint64 completion_seq;
	uint32 next_generation;
	bool published = false;

	if (ReconfigShmem == NULL || expected == NULL || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES
		|| expected->status != CLUSTER_R4_PREREQUISITE_R4A_READY
		|| !expected->ready || expected->reserved0[0] != 0
		|| expected->reserved0[1] != 0 || expected->reserved0[2] != 0
		|| expected->target_node_id != cluster_node_id
		|| expected->episode_state_generation == 0
		|| expected->jcmk_generation == 0
		|| expected->request_nonce == 0
		|| expected->old_admitted_incarnation == 0
		|| expected->fresh_incarnation
			   <= expected->old_admitted_incarnation
		|| expected->committed_epoch == 0
		|| expected->grammar_fingerprint
			   != CLUSTER_REPLACEMENT_EPISODE_GRAMMAR_FINGERPRINT)
		return false;
	request_seq = pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq);
	completion_seq
		= pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq);
	if (request_seq == 0 || request_seq != completion_seq
		|| pg_atomic_read_u32(&ReconfigShmem->join_marker_result)
			   != CLUSTER_JOIN_MARKER_SUBMIT_ACK)
		return false;
	pg_read_barrier();

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	episode = &ReconfigShmem->replacement_episode;
	if (pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq)
			!= request_seq
		|| pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq)
			   != completion_seq
		|| pg_atomic_read_u32(&ReconfigShmem->join_marker_result)
			   != CLUSTER_JOIN_MARKER_SUBMIT_ACK
		|| !cluster_replacement_episode_is_valid(episode)
		|| (episode->phase != CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED
			&& episode->phase != CLUSTER_REPLACEMENT_EPISODE_POST_EPOCH)
		|| episode->readiness_flags
			   != (CLUSTER_REPLACEMENT_EPISODE_GRD_POSTEPOCH_READY
				   | CLUSTER_REPLACEMENT_EPISODE_INTENT_CLEARED)
		|| episode->target_node_id != cluster_node_id
		|| episode->reserved_or_committed_epoch
			   != cluster_epoch_get_current()
		|| ReconfigShmem->self_join_admitted != 0
		|| ReconfigShmem->self_join_failed != 0
		|| !cluster_reconfig_replacement_membership_current_locked(episode)
		|| !cluster_replacement_episode_next_generation(
			episode->state_generation, &next_generation)
		|| !cluster_replacement_marker_v3_decode(
			ReconfigShmem->join_pending_marker, episode->target_node_id,
			&marker)
		|| marker.phase
			   != CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED
		|| marker.ready_state_generation != 0
		|| marker.generation == 0
		|| marker.target_node_id != episode->target_node_id
		|| marker.old_admitted_incarnation
			   != episode->old_admitted_incarnation
		|| marker.fresh_incarnation != episode->fresh_incarnation
		|| marker.baseline_epoch != episode->baseline_epoch
		|| marker.reserved_or_committed_epoch
			   != episode->reserved_or_committed_epoch
		|| marker.request_nonce != episode->request_nonce
		|| memcmp(marker.expected_purge_survivors,
				  episode->expected_survivors,
				  sizeof(marker.expected_purge_survivors)) != 0
		|| marker.grammar_fingerprint != episode->grammar_fingerprint)
		goto out;

	memset(&candidate, 0, sizeof(candidate));
	candidate.status = CLUSTER_R4_PREREQUISITE_R4A_READY;
	candidate.ready = true;
	candidate.target_node_id = episode->target_node_id;
	candidate.episode_state_generation = next_generation;
	candidate.jcmk_generation = marker.generation;
	candidate.request_nonce = episode->request_nonce;
	candidate.old_admitted_incarnation = episode->old_admitted_incarnation;
	candidate.fresh_incarnation = episode->fresh_incarnation;
	candidate.committed_epoch = episode->reserved_or_committed_epoch;
	candidate.grammar_fingerprint = episode->grammar_fingerprint;
	if (memcmp(&candidate, expected, sizeof(candidate)) != 0)
		goto out;

	episode->state_generation = next_generation;
	episode->readiness_flags
		|= CLUSTER_REPLACEMENT_EPISODE_R4A_TARGET_READY;
	published = true;

out:
	LWLockRelease(&ReconfigShmem->lock);
	return published;
#else
	(void)expected;
	return false;
#endif
}

static void
cluster_reconfig_reset_replacement_admit_stage(void)
{
	memset(&replacement_admit_stage, 0, sizeof(replacement_admit_stage));
	replacement_admit_stage.phase = CLUSTER_REPLACEMENT_ADMIT_IDLE;
	cluster_marker_async_init(&replacement_admit_stage.marker_async);
}

static bool
cluster_reconfig_replacement_admit_preflight(void)
{
	ClusterReplacementCommitMarkerV3 committed;
	ClusterReplacementEpisode *episode;
	uint64 request_seq;
	uint64 completion_seq;
	bool ready = false;

	if (ReconfigShmem == NULL || cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		return false;
	request_seq = pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq);
	completion_seq = pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq);
	if (request_seq == 0 || request_seq != completion_seq
		|| pg_atomic_read_u32(&ReconfigShmem->join_marker_result) != CLUSTER_JOIN_MARKER_SUBMIT_ACK)
		return false;
	pg_read_barrier();

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	episode = &ReconfigShmem->replacement_episode;
	if (pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq) == request_seq
		&& pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq) == completion_seq
		&& pg_atomic_read_u32(&ReconfigShmem->join_marker_result) == CLUSTER_JOIN_MARKER_SUBMIT_ACK
		&& cluster_replacement_episode_is_valid(episode)
		&& (episode->phase == CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED
			|| episode->phase == CLUSTER_REPLACEMENT_EPISODE_POST_EPOCH)
		&& episode->readiness_flags == CLUSTER_REPLACEMENT_EPISODE_READINESS_MASK
		&& episode->coordinator_node_id == cluster_node_id
		&& episode->reserved_or_committed_epoch == cluster_epoch_get_current()
		&& cluster_replacement_marker_v3_decode(ReconfigShmem->join_pending_marker,
												episode->target_node_id, &committed)
		&& committed.phase == CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED
		&& committed.ready_state_generation == 0
		&& committed.target_node_id == episode->target_node_id
		&& committed.old_admitted_incarnation == episode->old_admitted_incarnation
		&& committed.fresh_incarnation == episode->fresh_incarnation
		&& committed.baseline_epoch == episode->baseline_epoch
		&& committed.reserved_or_committed_epoch == episode->reserved_or_committed_epoch
		&& committed.request_nonce == episode->request_nonce
		&& memcmp(committed.expected_purge_survivors, episode->expected_survivors,
				  sizeof(committed.expected_purge_survivors))
			   == 0
		&& committed.grammar_fingerprint == episode->grammar_fingerprint)
		ready = true;
	LWLockRelease(&ReconfigShmem->lock);
	return ready;
}

void
cluster_reconfig_lmon_replacement_admit_tick(void)
{
	static const uint8 zero_value[CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES] = { 0 };
	ClusterQvotecMailboxCompletion completion;
	ClusterEpochAuthorityValue terminal_head;
	ClusterEpochBallotId terminal_ballot;
	ClusterQvotecMailboxSubmitStatus submit_status;
	ClusterMarkerPollResult marker_poll;
	uint32 marker_result = CLUSTER_JOIN_MARKER_SUBMIT_FAILED;
	uint64 elapsed_us = 0;
	TimestampTz now;

	switch (replacement_admit_stage.phase) {
	case CLUSTER_REPLACEMENT_ADMIT_IDLE:
		if (!cluster_reconfig_replacement_admit_preflight())
			return;
		submit_status
			= cluster_qvotec_authority_lmon_submit(CLUSTER_QVOTEC_MAILBOX_RECOVER_HEAD, zero_value,
												   &replacement_admit_stage.authority_request_seq);
		if (submit_status == CLUSTER_QVOTEC_MAILBOX_SUBMIT_ACCEPTED)
			replacement_admit_stage.phase = CLUSTER_REPLACEMENT_ADMIT_PRE_HEAD_WAIT;
		return;

	case CLUSTER_REPLACEMENT_ADMIT_PRE_HEAD_WAIT:
		if (!cluster_qvotec_authority_lmon_poll_completion(
				replacement_admit_stage.authority_request_seq, &completion))
			return;
		if (completion.result != CLUSTER_QVOTEC_MAILBOX_CHOSEN
			|| !cluster_epoch_authority_value_decode(completion.completion_value,
													 CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT,
													 &terminal_head)
			|| !cluster_epoch_ballot_id_decode(completion.completion_ballot, &terminal_ballot)
			|| terminal_ballot.proposer_node_id != cluster_node_id
			|| terminal_ballot.proposer_admitted_incarnation
				   != cluster_membership_get_last_admitted_incarnation(cluster_node_id)
			|| !cluster_reconfig_lmon_build_replacement_admitted(
				&terminal_head, &replacement_admit_stage.admitted_marker)) {
			cluster_reconfig_reset_replacement_admit_stage();
			return;
		}
		memcpy(replacement_admit_stage.pre_head_value, completion.completion_value,
			   sizeof(replacement_admit_stage.pre_head_value));
		memcpy(replacement_admit_stage.pre_head_ballot, completion.completion_ballot,
			   sizeof(replacement_admit_stage.pre_head_ballot));
		replacement_admit_stage.phase = CLUSTER_REPLACEMENT_ADMIT_MARKER_SUBMIT;
		return;

	case CLUSTER_REPLACEMENT_ADMIT_MARKER_SUBMIT: {
		ClusterReplacementCommitMarkerV3 revalidated_marker;

		/* The candidate can wait one or more LMON ticks before its
			 * majority write.  Rebuild it from the retained exact terminal
			 * head so a capability/episode/marker drift in that window resets
			 * the stage without writing stale ADMITTED bytes. */
		if (!cluster_epoch_authority_value_decode(replacement_admit_stage.pre_head_value,
												  CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT,
												  &terminal_head)
			|| !cluster_reconfig_lmon_build_replacement_admitted(&terminal_head,
																 &revalidated_marker)
			|| !cluster_replacement_marker_v3_same_image(
				&revalidated_marker, &replacement_admit_stage.admitted_marker)) {
			cluster_reconfig_reset_replacement_admit_stage();
			return;
		}
	}
		now = GetCurrentTimestamp();
		if (cluster_reconfig_submit_replacement_marker_v3_async(
				&replacement_admit_stage.marker_async,
				replacement_admit_stage.admitted_marker.target_node_id,
				&replacement_admit_stage.admitted_marker, CLUSTER_MARKER_KIND_JOIN_COMMITTED, now))
			replacement_admit_stage.phase = CLUSTER_REPLACEMENT_ADMIT_MARKER_WAIT;
		return;

	case CLUSTER_REPLACEMENT_ADMIT_MARKER_WAIT:
		now = GetCurrentTimestamp();
		marker_poll = cluster_reconfig_poll_join_marker_async(&replacement_admit_stage.marker_async,
															  now, &marker_result, &elapsed_us);
		if (marker_poll == CLUSTER_MARKER_POLL_PENDING || marker_poll == CLUSTER_MARKER_POLL_IDLE)
			return;
		if (marker_poll != CLUSTER_MARKER_POLL_ACKED
			|| marker_result != CLUSTER_JOIN_MARKER_SUBMIT_ACK) {
			cluster_reconfig_reset_replacement_admit_stage();
			return;
		}
		replacement_admit_stage.phase = CLUSTER_REPLACEMENT_ADMIT_POST_HEAD_SUBMIT;
		return;

	case CLUSTER_REPLACEMENT_ADMIT_POST_HEAD_SUBMIT:
		submit_status
			= cluster_qvotec_authority_lmon_submit(CLUSTER_QVOTEC_MAILBOX_RECOVER_HEAD, zero_value,
												   &replacement_admit_stage.authority_request_seq);
		if (submit_status == CLUSTER_QVOTEC_MAILBOX_SUBMIT_ACCEPTED)
			replacement_admit_stage.phase = CLUSTER_REPLACEMENT_ADMIT_POST_HEAD_WAIT;
		return;

	case CLUSTER_REPLACEMENT_ADMIT_POST_HEAD_WAIT:
		if (!cluster_qvotec_authority_lmon_poll_completion(
				replacement_admit_stage.authority_request_seq, &completion))
			return;
		if (completion.result == CLUSTER_QVOTEC_MAILBOX_CHOSEN
			&& memcmp(completion.completion_value, replacement_admit_stage.pre_head_value,
					  sizeof(completion.completion_value))
				   == 0
			&& memcmp(completion.completion_ballot, replacement_admit_stage.pre_head_ballot,
					  sizeof(completion.completion_ballot))
				   == 0
			&& cluster_epoch_authority_value_decode(completion.completion_value,
													CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT,
													&terminal_head))
			(void)cluster_reconfig_lmon_finalize_replacement_admitted(
				&terminal_head, &replacement_admit_stage.admitted_marker);
		cluster_reconfig_reset_replacement_admit_stage();
		return;
	}

	cluster_reconfig_reset_replacement_admit_stage();
}

bool
cluster_reconfig_publish_replacement_member_closed(
	const ClusterReplacementEpisode *admitted_episode)
{
	bool current_empty;

	if (ReconfigShmem == NULL || cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| !cluster_reconfig_is_local_admitted_replacement(admitted_episode))
		return false;

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	current_empty = cluster_replacement_episode_is_empty(&ReconfigShmem->replacement_episode);
	if ((!current_empty
		 && memcmp(&ReconfigShmem->replacement_episode, admitted_episode, sizeof(*admitted_episode))
				!= 0)
		|| clean_departed_test_bit_locked(ReconfigShmem->removed_bitmap, cluster_node_id)
		|| cluster_membership_get_state(cluster_node_id) == CLUSTER_MEMBER_REMOVED) {
		LWLockRelease(&ReconfigShmem->lock);
		return false;
	}

	/* A delayed duplicate after uniform OPEN must not re-close service. */
	if (!current_empty && ReconfigShmem->self_join_admitted != 0) {
		LWLockRelease(&ReconfigShmem->lock);
		return false;
	}

	cluster_write_fence_authority_cache_invalidate();
	memcpy(&ReconfigShmem->replacement_episode, admitted_episode, sizeof(*admitted_episode));
	cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_MEMBER);
	ReconfigShmem->self_join_admitted = 0;
	ReconfigShmem->self_join_failed = 0;
	ReconfigShmem->self_join_deadline_us = 0;
	LWLockRelease(&ReconfigShmem->lock);
	return true;
}

/* Target-QVOTEC observation of the durable historical completion seal.  Disk
 * I/O and exact-image majority selection happen before the reconfig lock; the
 * lock then co-samples the live episode, immutable formation tuple, current
 * epoch and publish proof before changing only MEMBER metadata. */
bool
cluster_reconfig_qvotec_observe_replacement_admitted(const int *fds, int n_disks,
													 uint64 live_incarnation)
{
	uint8 images[CLUSTER_MAX_VOTING_DISKS][CLUSTER_JCMK_REPLACEMENT_BYTES];
	ClusterReplacementCommitMarkerV3 admitted;
	ClusterReplacementEpisode capability_episode;
	ClusterReplacementEpisode *episode;
	uint32 majority;
	uint32 advanced = 0;
	uint32 expected = 0;
	int n_images = 0;
	bool published = false;
	int d;
	int node;

	if (ReconfigShmem == NULL || fds == NULL || n_disks <= 0 || n_disks > CLUSTER_MAX_VOTING_DISKS
		|| live_incarnation == 0 || cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		return false;
	majority = ((uint32)n_disks / 2u) + 1u;
	for (d = 0; d < n_disks; d++) {
		uint8 slot[CLUSTER_VOTING_SLOT_BYTES];

		memset(slot, 0, sizeof(slot));
		if (cluster_voting_disk_read_join_slot(fds[d], (uint32)cluster_node_id, slot)
			!= CLUSTER_VOTING_DISK_IO_OK)
			continue;
		memcpy(images[n_images++], slot, CLUSTER_JCMK_REPLACEMENT_BYTES);
	}
	if (cluster_replacement_marker_v3_select_majority(images, n_images, majority, cluster_node_id,
													  &admitted, NULL)
			< 0
		|| admitted.phase != CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED
		|| admitted.fresh_incarnation != live_incarnation || admitted.ready_state_generation == 0)
		return false;
	if (!cluster_reconfig_replacement_candidate2_capabilities_current(&capability_episode))
		return false;

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	episode = &ReconfigShmem->replacement_episode;
	if (memcmp(episode, &capability_episode, sizeof(*episode)) != 0
		|| !cluster_replacement_episode_is_valid(episode)
		|| (episode->phase != CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED
			&& episode->phase != CLUSTER_REPLACEMENT_EPISODE_POST_EPOCH)
		|| episode->readiness_flags != CLUSTER_REPLACEMENT_EPISODE_READINESS_MASK
		|| episode->target_node_id != cluster_node_id
		|| episode->state_generation != admitted.ready_state_generation
		|| episode->old_admitted_incarnation != admitted.old_admitted_incarnation
		|| episode->fresh_incarnation != admitted.fresh_incarnation
		|| episode->baseline_epoch != admitted.baseline_epoch
		|| episode->reserved_or_committed_epoch != admitted.reserved_or_committed_epoch
		|| episode->reserved_or_committed_epoch != cluster_epoch_get_current()
		|| episode->request_nonce != admitted.request_nonce
		|| memcmp(episode->expected_survivors, admitted.expected_purge_survivors,
				  sizeof(episode->expected_survivors))
			   != 0
		|| episode->grammar_fingerprint != admitted.grammar_fingerprint
		|| cluster_membership_get_state(cluster_node_id) == CLUSTER_MEMBER_REMOVED
		|| clean_departed_test_bit_locked(ReconfigShmem->removed_bitmap, cluster_node_id))
		goto out;

	/* Publish-proven is evaluated over the immutable pre-replacement survivor
	 * bitmap, not caller-relative mutable membership. */
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		bool in_expected = dead_bitmap_test_bit(episode->expected_survivors, node);

		if (node == cluster_node_id)
			continue;
		if (in_expected) {
			if (cluster_conf_lookup_node(node) == NULL || !cluster_membership_is_member(node))
				goto out;
			expected++;
			if (cluster_reconfig_get_observed_epoch(node) >= episode->reserved_or_committed_epoch)
				advanced++;
		} else if (cluster_membership_is_member(node))
			goto out;
	}
	if (expected == 0 || advanced < ((expected / 2u) + 1u))
		goto out;

	cluster_write_fence_authority_cache_invalidate();
	episode->phase = CLUSTER_REPLACEMENT_EPISODE_ADMITTED;
	cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_MEMBER);
	cluster_membership_record_admitted(cluster_node_id, admitted.fresh_incarnation);
	ReconfigShmem->self_join_admitted = 0;
	ReconfigShmem->self_join_failed = 0;
	ReconfigShmem->self_join_deadline_us = 0;
	published = true;

out:
	LWLockRelease(&ReconfigShmem->lock);
	return published;
}

bool
cluster_reconfig_open_replacement_admission(const ClusterReplacementEpisode *expected_episode,
											uint32 expected_state_generation)
{
	if (ReconfigShmem == NULL || expected_state_generation == 0
		|| !cluster_reconfig_is_local_admitted_replacement(expected_episode)
		|| expected_episode->state_generation != expected_state_generation)
		return false;

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	if (ReconfigShmem->replacement_episode.state_generation != expected_state_generation
		|| memcmp(&ReconfigShmem->replacement_episode, expected_episode, sizeof(*expected_episode))
			   != 0
		|| cluster_membership_get_state(cluster_node_id) != CLUSTER_MEMBER_MEMBER
		|| ReconfigShmem->self_join_failed != 0) {
		LWLockRelease(&ReconfigShmem->lock);
		return false;
	}

	cluster_write_fence_authority_cache_invalidate();
	ReconfigShmem->self_join_admitted = 1;
	ReconfigShmem->self_join_deadline_us = 0;
	LWLockRelease(&ReconfigShmem->lock);
	return true;
}

void
cluster_reconfig_note_self_admitted(uint64 admitted_epoch)
{
	bool replacement_admitted;

	if (ReconfigShmem == NULL)
		return;
	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		return;

	/* The ordinary v2 callback never opens a replacement episode, including a
	 * nonempty image that fails structural validation. */
	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	replacement_admitted
		= cluster_reconfig_has_replacement_episode(&ReconfigShmem->replacement_episode);
	LWLockRelease(&ReconfigShmem->lock);
	if (replacement_admitted)
		return;

	/*
	 * spec-6.15 D5b: the xid-stripe face must be resolved before admission
	 * finalizes — a striping-on node needs the published activation record
	 * (a running cluster either has one or was never activated; a rejoiner
	 * never seeds), and a striping-off node must NOT join an activated
	 * cluster (its unstriped allocator would violate the striped uniqueness
	 * invariant, 8.A).  Returning here is fail-closed and retried: qvotec
	 * re-detects the COMMITTED marker every poll, so admission completes on
	 * a later call once the stripe face resolves; a permanent mismatch runs
	 * into the join convergence deadline (53R61) with the 53RB1 cause
	 * logged once below.
	 */
	{
		ClusterXidStripeJoinVerdict sv = cluster_xid_stripe_join_gate(false);

		if (sv != CLUSTER_XID_STRIPE_JOIN_PROCEED) {
			static bool stripe_admit_logged = false;

			if (!stripe_admit_logged) {
				stripe_admit_logged = true;
				if (sv == CLUSTER_XID_STRIPE_JOIN_REFUSE)
					ereport(LOG,
							(errcode(ERRCODE_CLUSTER_XID_STRIPE_JOIN_MISMATCH),
							 errmsg("cluster xid stripe: refusing to finalize admission of "
									"node %d — stripe mode handshake mismatch (SQLSTATE 53RB1)",
									cluster_node_id),
							 errhint("cluster.xid_striping must match the cluster's durable "
									 "activation state on every node; repair the voting-disk "
									 "stripe region if it is corrupt.")));
				else
					ereport(LOG, (errmsg("cluster xid stripe: holding admission of node %d until "
										 "the stripe activation state is resolved",
										 cluster_node_id)));
			}
			return;
		}
	}

	/*
	 * Hardening v1.1 (HF-1 / INV-J9): this is called ONLY after the caller
	 * (qvotec) has confirmed BOTH a same-commit majority COMMITTED marker (HF-3)
	 * AND the publish-proof — cluster_reconfig_join_publish_proven — i.e. a
	 * member quorum has reached admitted_epoch, proving the coordinator's
	 * JOIN_COMMITTED publish actually propagated.  The v1.0 "gate-open guard =
	 * adopt && state==MEMBER" was vacuous (this function self-set state==MEMBER),
	 * which left the half-publish window open (P1-1); the real guard is now the
	 * publish-proof at the caller.  Here we just adopt the admitted epoch
	 * (quorum-authenticated, may jump >16 — INV-J12), set self MEMBER and open
	 * the gate.
	 */
	cluster_epoch_adopt_admitted(admitted_epoch);

	/*
	 * spec-5.16 D3b (INV-R13) — arm the joiner-home PCM block fence SYNCHRONOUSLY,
	 * BEFORE setting self MEMBER below.  Setting MEMBER flips cluster_membership_
	 * is_member(self) true and opens the write gate; the async GRD LMON tick that
	 * consumes JOIN_COMMITTED is structurally LATER, so a fence armed there would
	 * leave a window where self is a writable MEMBER but its home blocks are
	 * unfenced — a cold-serve / double-grant hazard (8.A).  The fence epoch is the
	 * just-adopted admitted (JOIN_COMMITTED) epoch; rejoining_set = {self}.  No-op
	 * when the GRD region is absent (cluster off).
	 */
	{
		uint8 self_set[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };

		self_set[cluster_node_id >> 3] = (uint8)(1u << (cluster_node_id & 7));
		cluster_grd_arm_join_pcm_fence(self_set);
	}

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	/*
	 * RF-ROOT P6 (crash-rejoin): invalidate the fence-authority cache ONLY
	 * when this call actually mutates admission state.  The qvotec self-admit
	 * detection re-runs note_self_admitted on EVERY poll while the COMMITTED
	 * join marker remains proven; the unconditional invalidation therefore
	 * bumped the cache sequence every ~1s forever, starving the phase-3
	 * live-formation witness's cached revalidate_nowait (its build can prove
	 * READY but the revalidation never matches) — phase 3 wedged in
	 * wait_for_live_formation until the phase3_timeout on every rejoiner.
	 * An idempotent re-run (already admitted ∧ already MEMBER ∧ no
	 * replacement episode) mutates nothing, so the cache must stay valid.
	 * The replacement branch below still invalidates (it can flip
	 * self_join_admitted back to 0), and first admission / state changes
	 * still invalidate through this same gate.
	 */
	if (!ReconfigShmem->self_join_admitted
		|| cluster_membership_get_state(cluster_node_id) != CLUSTER_MEMBER_MEMBER
		|| cluster_reconfig_has_replacement_episode(&ReconfigShmem->replacement_episode))
		cluster_write_fence_authority_cache_invalidate();
	/* Close the check/use race with a concurrent replacement publisher. */
	if (cluster_reconfig_has_replacement_episode(&ReconfigShmem->replacement_episode)) {
		ReconfigShmem->self_join_admitted = 0;
		if (cluster_reconfig_is_local_admitted_replacement(&ReconfigShmem->replacement_episode))
			cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_MEMBER);
		LWLockRelease(&ReconfigShmem->lock);
		return;
	}
	/*
	 * RF-ROOT P6 (crash-rejoin): publish the admitted incarnation floor for
	 * SELF before flipping MEMBER.  The coordinator's commit_member records
	 * it on the survivor; without the joiner-side mirror the live-formation
	 * witness's owner floor (last_admitted_incarnation == 0) stays unproven
	 * forever on a rejoiner and phase 3 can never leave wait_for_live_formation
	 * (the witness comment's "LMON publishes the exact admitted-incarnation
	 * floor" transient).  Monotonic-max: a stale lower value never regresses.
	 */
	cluster_membership_record_admitted(cluster_node_id, cluster_qvotec_get_self_incarnation());
	cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_MEMBER);
	ReconfigShmem->self_join_admitted = 1;
	ReconfigShmem->self_join_failed = 0;
	ReconfigShmem->self_join_deadline_us = 0;
	LWLockRelease(&ReconfigShmem->lock);
}

/*
 * cluster_reconfig_self_join_admitted -- RF-ROOT P6.
 *
 *	Lock-shared read of the durable self-join admission flag.  The
 *	recovery-transport components predicate (crash-rejoin DONE ingress)
 *	uses it as the membership proof while the LMON self-state byte can
 *	transiently read JOINING under the boot-decided latch.
 *
 *	AD-023 A1 (STOP-01 contract): the postmaster drives
 *	phase 3 without a PGPROC, and this accessor is reachable from its
 *	transport/preseal/thread-open paths (e.g. the THREAD_OPEN failure
 *	decomposition, which evaluates the components predicate while the
 *	LMON may hold the reconfig lock EXCLUSIVE for a membership mutation).
 *	A blocking shared acquire would PANIC in LWLockQueueSelf; the
 *	no-PGPROC caller may only take an immediately available shared lock
 *	and reads contention as not-yet-admitted (fail-closed — every
 *	consumer is an AND gate that retries on the next tick/frame, and the
 *	admission flag is a monotonic latch, so a transient false only
 *	delays, never grants).
 */
bool
cluster_reconfig_self_join_admitted(void)
{
	bool admitted;

	if (ReconfigShmem == NULL)
		return false;
	if (MyProc == NULL) {
		if (!LWLockConditionalAcquire(&ReconfigShmem->lock, LW_SHARED))
			return false;
	} else
		LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	admitted = ReconfigShmem->self_join_admitted != 0;
	LWLockRelease(&ReconfigShmem->lock);
	return admitted;
}

/* Read one peer's qvotec-published slot identity, epoch, and freshness from a
 * single stable poll window.  A writer-in-progress or changed window is not
 * current evidence; the LMON retries on a later tick instead of combining
 * fields from adjacent slot generations. */
static bool
cluster_reconfig_observed_peer_epoch_current(int32 node_id, uint64 *out_epoch)
{
	int attempt;

	if (out_epoch != NULL)
		*out_epoch = 0;
	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES || out_epoch == NULL)
		return false;

	for (attempt = 0; attempt < 8; attempt++) {
		uint64 epoch;
		uint64 fresh;
		uint64 generation;
		uint64 incarnation;
		uint64 seq_after;
		uint64 seq_before;

		seq_before = pg_atomic_read_u64(&ReconfigShmem->observed_bootstrap_seq);
		if ((seq_before & UINT64_C(1)) != 0)
			continue;
		pg_read_barrier();
		incarnation = pg_atomic_read_u64(&ReconfigShmem->observed_incarnation[node_id]);
		generation = pg_atomic_read_u64(&ReconfigShmem->observed_generation[node_id]);
		epoch = pg_atomic_read_u64(&ReconfigShmem->observed_epoch[node_id]);
		fresh = pg_atomic_read_u64(&ReconfigShmem->observed_fresh_alive[node_id]);
		pg_read_barrier();
		seq_after = pg_atomic_read_u64(&ReconfigShmem->observed_bootstrap_seq);
		if (seq_after != seq_before)
			continue;
		if (incarnation == 0 || generation == 0 || fresh == 0)
			return false;
		*out_epoch = epoch;
		return true;
	}
	return false;
}

typedef struct ClusterEpoch0LateFounderPeer {
	int32 node_id;
	uint64 incarnation;
	uint64 generation;
} ClusterEpoch0LateFounderPeer;

/* Capture one admitted peer from one stable QVOTEC publication window.  The
 * no-PGPROC phase-3 caller must never queue on this lock. */
static bool
cluster_reconfig_epoch0_peer_capture_current(int32 node_id, ClusterEpoch0LateFounderPeer *out)
{
	bool current = false;
	int attempt;

	memset(out, 0, sizeof(*out));
	if (ReconfigShmem == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES
		|| node_id == cluster_node_id || cluster_conf_lookup_node(node_id) == NULL)
		return false;
	if (MyProc == NULL) {
		if (!LWLockConditionalAcquire(&ReconfigShmem->lock, LW_SHARED))
			return false;
	} else
		LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);

	for (attempt = 0; attempt < 8; attempt++) {
		uint64 epoch;
		uint64 fresh;
		uint64 generation;
		uint64 incarnation;
		uint64 seq_after;
		uint64 seq_before;

		seq_before = pg_atomic_read_u64(&ReconfigShmem->observed_bootstrap_seq);
		if ((seq_before & UINT64_C(1)) != 0)
			continue;
		pg_read_barrier();
		incarnation = pg_atomic_read_u64(&ReconfigShmem->observed_incarnation[node_id]);
		generation = pg_atomic_read_u64(&ReconfigShmem->observed_generation[node_id]);
		epoch = pg_atomic_read_u64(&ReconfigShmem->observed_epoch[node_id]);
		fresh = pg_atomic_read_u64(&ReconfigShmem->observed_fresh_alive[node_id]);
		current = incarnation != 0 && generation != 0 && fresh != 0
				  && epoch == CLUSTER_EPOCH_INITIAL
				  && cluster_membership_get_state(node_id) == CLUSTER_MEMBER_MEMBER
				  && cluster_membership_get_last_admitted_incarnation(node_id) == incarnation;
		pg_read_barrier();
		seq_after = pg_atomic_read_u64(&ReconfigShmem->observed_bootstrap_seq);
		if (seq_after != seq_before) {
			current = false;
			continue;
		}
		if (current) {
			out->node_id = node_id;
			out->incarnation = incarnation;
			out->generation = generation;
		}
		break;
	}

	LWLockRelease(&ReconfigShmem->lock);
	return current;
}

static bool
cluster_reconfig_epoch0_peer_active_matches(const ClusterEpoch0LateFounderPeer *peer,
											const ClusterWalStateSlot *slot)
{
	uint16 thread_id;

	if (peer == NULL || slot == NULL || peer->incarnation == 0
		|| peer->incarnation > (uint64)PG_INT64_MAX)
		return false;
	thread_id = cluster_wal_thread_id_for(true, peer->node_id);
	return thread_id != XLP_THREAD_ID_LEGACY && slot->thread_id == thread_id
		   && slot->node_id == peer->node_id && slot->state == CLUSTER_WAL_SLOT_STATE_ACTIVE
		   && slot->started_at >= (int64)peer->incarnation
		   && slot->last_updated >= slot->started_at;
}

/*
 * Exact epoch-0 late-founder evidence approved for the narrow JOIN_READONLY
 * bridge.  A current admitted peer and shared PGXA are not enough: Phase 1 can
 * leave a valid old ACTIVE image while every Phase-2 postmaster is still below
 * StartupXLOG.  Deferral therefore requires that the peer's existing WAL slot
 * was published after the exact current QVOTEC boot incarnation.  Two
 * byte-identical reads bracket a complete QVOTEC/membership revalidation; all
 * file I/O remains outside the reconfig LWLock.
 *
 * This predicate is observation only: it neither latches a bootstrap cohort
 * nor admits self, stages a stripe claim, or starts a deadline.
 */
bool
cluster_reconfig_epoch0_late_founder_evidence_current(void)
{
	int i;

	if (ReconfigShmem == NULL || !cluster_online_join || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES
		|| cluster_epoch_get_current() != CLUSTER_EPOCH_INITIAL || !cluster_qvotec_in_quorum()
		|| cluster_xid_stripe_disk_state() != CLUSTER_XID_STRIPE_DISK_PUBLISHED)
		return false;

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		ClusterEpoch0LateFounderPeer before;
		ClusterEpoch0LateFounderPeer after;
		ClusterWalStateSlot wal_before;
		ClusterWalStateSlot wal_after;
		uint16 thread_id;

		if (!cluster_reconfig_epoch0_peer_capture_current(i, &before))
			continue;
		thread_id = cluster_wal_thread_id_for(true, i);
		if (cluster_wal_state_read_slot(thread_id, &wal_before) != CLUSTER_WAL_SLOT_OK
			|| !cluster_reconfig_epoch0_peer_active_matches(&before, &wal_before))
			continue;
		if (!cluster_reconfig_epoch0_peer_capture_current(i, &after)
			|| memcmp(&before, &after, sizeof(before)) != 0
			|| cluster_epoch_get_current() != CLUSTER_EPOCH_INITIAL || !cluster_qvotec_in_quorum()
			|| cluster_xid_stripe_disk_state() != CLUSTER_XID_STRIPE_DISK_PUBLISHED)
			continue;
		if (cluster_wal_state_read_slot(thread_id, &wal_after) != CLUSTER_WAL_SLOT_OK
			|| memcmp(&wal_before, &wal_after, sizeof(wal_before)) != 0)
			continue;
		return true;
	}
	return false;
}

/* The phase-3 postmaster has no PGPROC and therefore cannot enter an LWLock
 * wait queue.  Ordinary callers retain the established blocking semantics. */
static bool
cluster_reconfig_handoff_lock_acquire(LWLockMode mode)
{
	if (MyProc == NULL)
		return LWLockConditionalAcquire(&ReconfigShmem->lock, mode);
	LWLockAcquire(&ReconfigShmem->lock, mode);
	return true;
}

/* External/current half of the frozen handoff identity.  All potentially
 * changing evidence is sampled outside the reconfig lock and repeated after
 * the locked transition; a changed sample leaves JOINING write-closed and is
 * retried from current evidence rather than rolling membership back. */
static bool
cluster_reconfig_pre_publish_handoff_external_current(uint64 expected_self_incarnation,
													  uint64 expected_predecessor_floor,
													  bool require_recovery_complete)
{
	return ReconfigShmem != NULL && cluster_online_join && cluster_node_id >= 0
		   && cluster_node_id < CLUSTER_MAX_NODES && expected_self_incarnation != 0
		   && (expected_predecessor_floor == 0
			   || expected_predecessor_floor < expected_self_incarnation)
		   && cluster_authority_handoff_identity_current(expected_self_incarnation,
														 expected_predecessor_floor)
		   && cluster_authority_readiness_managed()
		   && cluster_authority_readiness_get() == CLUSTER_AUTHORITY_OFF
		   && cluster_epoch_get_current() == CLUSTER_EPOCH_INITIAL
		   && (!require_recovery_complete || !RecoveryInProgress())
		   && !cluster_clean_leave_in_progress()
		   && !cluster_reconfig_is_removed_unlocked(cluster_node_id)
		   && cluster_qvotec_get_self_incarnation() == expected_self_incarnation
		   && cluster_membership_get_last_admitted_incarnation(cluster_node_id)
				  == expected_predecessor_floor
		   && cluster_reconfig_epoch0_late_founder_evidence_current();
}

static bool
cluster_reconfig_pre_publish_handoff_locked_current(uint64 expected_self_incarnation,
													uint64 expected_predecessor_floor,
													bool allow_member)
{
	ClusterMembershipState membership;

	membership = cluster_membership_get_state(cluster_node_id);
	return ReconfigShmem->self_join_admitted == 0 && ReconfigShmem->self_join_failed == 0
		   && ReconfigShmem->self_join_deadline_us == 0
		   && cluster_replacement_episode_is_empty(&ReconfigShmem->replacement_episode)
		   && !clean_departed_test_bit_locked(ReconfigShmem->removed_bitmap, cluster_node_id)
		   && cluster_membership_get_last_admitted_incarnation(cluster_node_id)
				  == expected_predecessor_floor
		   && (expected_predecessor_floor == 0
			   || expected_predecessor_floor < expected_self_incarnation)
		   && (membership == CLUSTER_MEMBER_JOINING
			   || (allow_member && membership == CLUSTER_MEMBER_MEMBER));
}

/*
 * Transfer the exact, reversible pre-publish recovery binding to the existing
 * ordinary join owner.  The only mutation is MEMBER -> JOINING in the existing
 * shared membership table.  A byte-identical staged JOINING tuple is an
 * idempotent success; no deadline, admission bit, or auxiliary latch is reset.
 */
bool
cluster_reconfig_stage_pre_publish_join_handoff(uint64 expected_self_incarnation,
												uint64 expected_predecessor_floor)
{
	bool exact;

	if (!cluster_reconfig_pre_publish_handoff_external_current(expected_self_incarnation,
															   expected_predecessor_floor, false))
		return false;
	if (!cluster_reconfig_handoff_lock_acquire(LW_EXCLUSIVE))
		return false;
	exact = cluster_reconfig_pre_publish_handoff_locked_current(expected_self_incarnation,
																expected_predecessor_floor, true);
	if (exact && cluster_membership_get_state(cluster_node_id) == CLUSTER_MEMBER_MEMBER)
		cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_JOINING);
	LWLockRelease(&ReconfigShmem->lock);
	if (!exact)
		return false;
	return cluster_reconfig_pre_publish_handoff_external_current(expected_self_incarnation,
																 expected_predecessor_floor, false);
}

/* Read-only LMON classifier for the staged tuple.  Both external evidence and
 * the locked shared tuple are sampled twice; this accessor neither starts the
 * join deadline nor claims stripe/JCMK authority. */
bool
cluster_reconfig_pre_publish_join_handoff_current(void)
{
	uint64 expected_predecessor_floor;
	uint64 expected_self_incarnation;
	bool exact;

	if (ReconfigShmem == NULL)
		return false;
	expected_self_incarnation = cluster_qvotec_get_self_incarnation();
	expected_predecessor_floor = cluster_membership_get_last_admitted_incarnation(cluster_node_id);
	if (!cluster_reconfig_pre_publish_handoff_external_current(expected_self_incarnation,
															   expected_predecessor_floor, true))
		return false;
	if (!cluster_reconfig_handoff_lock_acquire(LW_SHARED))
		return false;
	exact = cluster_reconfig_pre_publish_handoff_locked_current(expected_self_incarnation,
																expected_predecessor_floor, false);
	LWLockRelease(&ReconfigShmem->lock);
	if (!exact)
		return false;
	return cluster_reconfig_pre_publish_handoff_external_current(expected_self_incarnation,
																 expected_predecessor_floor, true);
}

/*
 * Is the cluster already running (so a freshly-booted node is REJOINING, not
 * bootstrapping — §3.4)?  Signal: a declared peer observed at a committed epoch
 * > CLUSTER_EPOCH_INITIAL.  A node only becomes absent after the survivors
 * noticed and reconfigured (which advances the epoch), so by the time it reboots
 * its peers are past epoch 0; a cold bootstrap has every node at epoch 0.
 */
static bool
cluster_reconfig_cluster_already_running(void)
{
	int i;

	if (ReconfigShmem == NULL)
		return false;
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		uint64 peer_epoch;

		if (i == cluster_node_id)
			continue;
		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		/*
		 * RF-ROOT P9 verification / cold-formation cold-formation ruling:
		 * a peer counts as "running" ONLY when its slot is FRESH-ALIVE past
		 * INITIAL.  A stale leftover slot (a clean stop blanked ALIVE but
		 * left the old epoch byte) is a DEAD process's residue — counting it
		 * made every cold co-boot past INITIAL self-classify as "clean
		 * reopen", self-admit MEMBER, publish the live epoch, and thereby
		 * deny the 5.22 observation window on the peers (each side saw the
		 * other "past INITIAL" and refused cold formation).
		 */
		if (cluster_reconfig_observed_peer_epoch_current(i, &peer_epoch)
			&& peer_epoch > CLUSTER_EPOCH_INITIAL)
			return true;
	}
	return false;
}

/*
 * spec-5.15 Hardening v1.1 (HF-2 / INV-J14) — positive cold-bootstrap proof,
 * REVISED by Hardening v1.2 (INV-J14 self-join-gate race) to rest on the durable
 * voting-disk slot rather than live CSSD.
 *
 * A node may keep its write gate open WITHOUT online-join admission only when a
 * majority of declared nodes are observed CO-BOOTING at CLUSTER_EPOCH_INITIAL on
 * a VALID durable slot (qvotec saw a real voting-disk slot, generation > 0, at
 * epoch INITIAL) AND no declared peer is observed past CLUSTER_EPOCH_INITIAL.
 * This is an EPOCH proof, not a timing grace: a slow qvotec leaves the decision
 * UNDECIDED (gate stays closed, fail-closed) instead of mis-deciding bootstrap
 * and permanently fail-opening (P1-2).  A rejoiner can never satisfy it — by the
 * time it sees its peers they are already at epoch > INITIAL.
 *
 * v1.2 RATIONALE: the v1.1 proof counted live CSSD-alive peers, which a
 * transient IC / heartbeat churn could momentarily drop below quorum — leaving a
 * GENUINE founding member UNDECIDED (never latched).  An UNRELATED peer's later
 * fail-stop then advanced the epoch, and joiner_self_tick reclassified that
 * still-UNDECIDED member as a rejoiner: it closed its own write gate and timed
 * out to 53R61 (refused its own writes), never participating again.  Anchoring
 * the quorum on the DURABLE slot (stable across CSSD churn) lets a founding
 * member latch reliably during formation, closing the UNDECIDED window before
 * any unrelated epoch advance.  A default-0 placeholder (generation 0) is NOT
 * proof and must never count (else a node with no real evidence fail-opens) —
 * the v1.2 user constraint: latch only on a valid co-boot slot, never on 0.
 * Quorum (not all-declared) is retained so a degraded co-boot (e.g. 2 of 3) can
 * still form, and because requiring every peer would only WIDEN the UNDECIDED
 * window the race exploits.
 */
bool
cluster_reconfig_bootstrap_quorum_at_initial(void)
{
	uint32 declared = 0;
	uint32 proven_at_initial = 0;
	int i;

	/*
	 * RF-ROOT P9 verification / cold-formation cold-formation ruling
	 * (2026-08-19): the founding bootstrap proof is ONLY for a genuine
	 * first formation — the LOCAL epoch must still be INITIAL.  A cold
	 * co-boot of an EXISTING cluster (clean shutdown + restart) recovers
	 * epoch > INITIAL from the voting disk, yet every co-booting node
	 * publishes INITIAL in its slot (slot-epoch A-semantics: un-formed
	 * nodes write CLUSTER_EPOCH_INITIAL).  Without this local-epoch gate
	 * that co-boot satisfies the proof (peer fresh + gen > 0 + slot at
	 * INITIAL) and each node self-admits MEMBER from its own incarnation
	 * — the exact "epoch>INITIAL ABSENT cold formation" bug B′ must fix,
	 * now reached through the joiner gate instead of the ABSENT branch.
	 * Past INITIAL the joiner gate must stay UNDECIDED (fail-closed) so
	 * the 5.22 observation window + quorum-majority formation marker
	 * state machine owns the admission.
	 */
	if (cluster_epoch_get_current() != CLUSTER_EPOCH_INITIAL)
		return false;

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		uint64 inc = 0;
		uint64 gen = 0;
		uint64 ep;

		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		declared++;
		if (i == cluster_node_id) {
			proven_at_initial++; /* self is up, at INITIAL (not yet admitted) */
			continue;
		}
		ep = cluster_reconfig_get_observed_epoch(i);
		/* any declared peer past INITIAL => a running cluster, NOT a bootstrap */
		if (ep > CLUSTER_EPOCH_INITIAL)
			return false;
		/*
		 * Count a peer only on a FRESH-ALIVE co-boot slot: a real observed
		 * voting-disk slot (generation > 0) that qvotec's decide_quorum_view saw
		 * FRESH-ALIVE this poll (heartbeat_ts_us recent, the P2.1 freshness gate),
		 * at epoch INITIAL.  Hardening v1.3: the generation > 0 test alone is NOT
		 * liveness — a CRASHED peer leaves a stale leftover slot (gen > 0, epoch
		 * INITIAL) that v1.2 wrongly counted, letting a node fail-open (latch
		 * BOOTSTRAP on self + a stale peer slot, with no live co-boot quorum).  The
		 * fresh-alive signal is anchored on the durable voting-disk heartbeat (NOT
		 * live CSSD), so it rejects stale slots WITHOUT reintroducing the v1.2
		 * IC-churn race (the disk heartbeat keeps flowing while CSSD/tier1 churns).
		 * A default-0 placeholder (generation 0) never counts either.
		 */
		if (cluster_reconfig_get_observed_slot(i, &inc, &gen) && gen > 0
			&& cluster_reconfig_get_observed_fresh_alive(i) && ep == CLUSTER_EPOCH_INITIAL)
			proven_at_initial++;
	}
	if (declared == 0)
		return false;
	return proven_at_initial >= ((declared / 2u) + 1u);
}

/*
 * spec-6.15 D5b — is THIS node the deterministic activation-seed candidate?
 * Lowest declared node that is provably part of the cold-boot formation
 * (self, or a FRESH-ALIVE co-boot slot at INITIAL — the same evidence the
 * bootstrap quorum proof counts).  Only the candidate stages the activation
 * seed, so concurrent bootstrapping nodes do not race to write region 5;
 * the qvotec adopt-not-overwrite re-scan covers the residual window when
 * the alive view shifts between ticks.
 */
static bool
cluster_reconfig_self_is_stripe_seed_candidate(void)
{
	int i;

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		uint64 inc = 0;
		uint64 gen = 0;

		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		if (i == cluster_node_id)
			return true; /* no lower declared node is provably alive */
		if (cluster_reconfig_get_observed_slot(i, &inc, &gen) && gen > 0
			&& cluster_reconfig_get_observed_fresh_alive(i)
			&& cluster_reconfig_get_observed_epoch(i) == CLUSTER_EPOCH_INITIAL)
			return false; /* a lower fresh-alive declared node seeds instead */
	}
	return false;
}

/*
 * spec-5.15 D5 — joiner self-tick (runs on every node each LMON tick).
 *
 *	Decides, fail-closed, whether THIS node may write before it is a confirmed
 *	member.  Hardening v1.1 (HF-2 / INV-J14) replaces the v1.0 timing-grace
 *	heuristic — which permanently fail-opened a rejoiner that a slow qvotec
 *	mis-saw as a cold bootstrap (P1-2) — with a POSITIVE epoch proof:
 *	  - any declared peer past INITIAL          -> REJOINER: close the gate
 *	    (53R60), start the join, latch a convergence deadline -> 53R61 on timeout
 *	    (restart with a fresh incarnation, INV-J4 never half-admit).  qvotec's
 *	    note_self_admitted opens the gate once the COMMITTED marker AND the
 *	    publish-proof (HF-1) both hold.
 *	  - quorum of declared CSSD-alive at INITIAL -> BOOTSTRAP: open the gate
 *	    (boot-time formation is not gated by online_join) and latch it so a later
 *	    epoch advance does not re-close this genuine member.
 *	  - neither proven yet                       -> UNDECIDED: keep the gate
 *	    closed (fail-closed); a slow qvotec waits here, it never mis-opens.
 */
/* Caller holds ReconfigShmem->lock LW_EXCLUSIVE and sampled_incarnation was
 * obtained before acquiring it.  The three exact samples close both sides of
 * the monotonic floor write: a changed or zero QVOTEC formation may raise a
 * floor, but it never publishes MEMBER until the current formation and the
 * stored floor are byte-for-byte equal. */
static bool
cluster_reconfig_publish_self_current_floor_locked(int32 self_id, uint64 sampled_incarnation)
{
	uint64 current_incarnation;

	if (sampled_incarnation == 0)
		return false;
	current_incarnation = cluster_qvotec_get_self_incarnation();
	if (current_incarnation == 0 || current_incarnation != sampled_incarnation)
		return false;

	/* A steady LMON tick must not destroy the live-formation proof that phase 3
	 * just published for the recovery-authority barrier.  The membership
	 * mutators below already invalidate on changed bytes; retain the leading
	 * invalidation only when this compound edge will actually change its floor
	 * or state. */
	if (cluster_membership_get_last_admitted_incarnation(self_id) != sampled_incarnation
		|| cluster_membership_get_state(self_id) != CLUSTER_MEMBER_MEMBER)
		cluster_write_fence_authority_cache_invalidate();
	cluster_membership_record_admitted(self_id, sampled_incarnation);
	current_incarnation = cluster_qvotec_get_self_incarnation();
	if (current_incarnation != sampled_incarnation
		|| cluster_membership_get_last_admitted_incarnation(self_id) != sampled_incarnation)
		return false;

	cluster_membership_set_state(self_id, CLUSTER_MEMBER_MEMBER);
	return true;
}

static void
cluster_reconfig_joiner_self_tick(void)
{
	bool clean_leave_active;
	bool epoch0_late_founder;
	bool managed_join_readonly_pivot;
	bool replacement_admitted;
	bool in_quorum;
	uint64 now_us;

	if (ReconfigShmem == NULL || (!cluster_online_join && !offpath_fast_rejoin_active_local))
		return;
	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		return;

	/*
	 * spec-5.18 INV-LF9 (HF-2): a durably-removed node (this node was permanently
	 * removed; startup rebuild seeded removed_bitmap[self]) must NOT run the joiner
	 * gate — doing so would flip its own membership_state REMOVED -> JOINING/MEMBER
	 * (the REJOINER/bootstrap branches below) and defeat the 53R64 self-demote write
	 * gate.  A removed node can only return via operator un-fence + a fresh-
	 * incarnation join (external plane, §1.3) — never by re-running this gate.
	 */
	if (cluster_reconfig_is_removed_unlocked(cluster_node_id))
		return;

	/* spec-5.15A: ordinary bootstrap/rejoin classification has no authority to
	 * open or time out an exact replacement ADMITTED episode. */
	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	replacement_admitted
		= cluster_reconfig_has_replacement_episode(&ReconfigShmem->replacement_episode);
	LWLockRelease(&ReconfigShmem->lock);
	if (replacement_admitted)
		return;

	now_us = (uint64)GetCurrentTimestamp();
	in_quorum = cluster_qvotec_in_quorum();
	clean_leave_active = cluster_clean_leave_in_progress();
	/* The managed bridge is not inferred from OFF+JOINING.  It is the exact
	 * phase-3 staged tuple: current self incarnation/floor, zero admission/
	 * failure/deadline, no competing membership episode, recovery complete,
	 * and the same current epoch-0 late-founder evidence. */
	managed_join_readonly_pivot = cluster_reconfig_pre_publish_join_handoff_current();
	epoch0_late_founder = !RecoveryInProgress()
						  && (!cluster_authority_readiness_managed() || managed_join_readonly_pivot)
						  && cluster_reconfig_epoch0_late_founder_evidence_current();

	/*
	 * Catch up to the cluster epoch observed on the durable voting disk (quorum-
	 * authenticated — qvotec is the sole CRC-checked slot writer) so a rejoiner
	 * that booted at CLUSTER_EPOCH_INITIAL can COMMUNICATE.  The IC envelope drops
	 * a frame whose epoch is BELOW the receiver's (anti-stale, spec-2.4): until a
	 * rejoiner reaches the cluster epoch, its own CSSD heartbeats are stale-dropped
	 * by the survivors, so it is never seen ALIVE → never detected → never
	 * admitted (a join deadlock).  Adopting the max observed in-quorum peer epoch
	 * bridges that gap WITHOUT bypassing admission: the incarnation vet (INV-J1) +
	 * the §2.6 COMMITTED marker still gate MEMBER; this only unblocks the
	 * transport.  Runs every tick so the joiner tracks the cluster while joining.
	 */
	{
		uint64 self_epoch = cluster_epoch_get_current();
		uint64 max_peer_epoch = self_epoch;
		int i;

		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			uint64 pe;

			if (i == cluster_node_id || cluster_conf_lookup_node(i) == NULL)
				continue;
			/* Transport catch-up uses the exact current QVOTEC tuple also used
			 * by running-peer classification; an odd/changed poll or stale slot
			 * cannot contribute an epoch. */
			if (!cluster_reconfig_observed_peer_epoch_current(i, &pe))
				continue;
			if (pe > max_peer_epoch)
				max_peer_epoch = pe;
		}
		if (max_peer_epoch > self_epoch)
			cluster_epoch_adopt_admitted(max_peer_epoch);
	}

	/*
	 * Hardening v1.0.4 (P2 serialization): do NOT start driving this node's own join
	 * while a clean leave is active here.  Deferring is safe — joiner_self_tick is
	 * LMON-driven and retries next tick once the leave reaches a terminal state (one
	 * membership reconfig at a time; the leave side refuses to start while a join is
	 * pending via cluster_reconfig_join_in_progress).
	 */
	if (!joiner_gate_decided && !clean_leave_active) {
		if (offpath_fast_rejoin_active_local || cluster_reconfig_cluster_already_running()
			|| epoch0_late_founder) {
			/* REJOINER: a running cluster exists.  Close the gate and classify
			 * the ordinary join.  Its convergence deadline remains unowned until
			 * local quorum can drive the exact xid-stripe prerequisite.  A
			 * prior-unclean shared-CF classification is already positive rejoin
			 * evidence and may never fall back to the generic epoch-0 bootstrap
			 * proof while the survivor's eviction bump is still in flight. */
			if (managed_join_readonly_pivot) {
				/* The phase-3 Writer already installed the exact shared tuple.
				 * Consume it without resetting any byte.  A second complete
				 * sample closes the evidence-to-process-local-owner race. */
				if (!cluster_reconfig_pre_publish_join_handoff_current())
					return;
				joiner_gate_decided = true;
				if (!cluster_reconfig_pre_publish_join_handoff_current()) {
					joiner_gate_decided = false;
					return;
				}
			} else {
				joiner_gate_decided = true;
				LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
				cluster_write_fence_authority_cache_invalidate();
				ReconfigShmem->self_join_admitted = 0;
				ReconfigShmem->self_join_failed = 0;
				ReconfigShmem->self_join_deadline_us = 0;
				cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_JOINING);
				LWLockRelease(&ReconfigShmem->lock);
			}
			ereport(LOG,
					(errmsg("cluster membership: node %d joining a running cluster — write gate "
							"closed (53R60) pending admission",
							cluster_node_id)));
		} else if (!in_quorum) {
			/* A slow founding node can fall out of the current local quorum
			 * view when its peers advance formation first.  Their fresh,
			 * QVOTEC-authenticated past-INITIAL slots above may still classify
			 * this node as an ordinary joiner and close its gate, but absence of
			 * local quorum is never authority to bootstrap, publish MEMBER, or
			 * start an admission deadline.  Retry from current evidence. */
			return;
		} else if (cluster_reconfig_bootstrap_quorum_at_initial()) {
			uint64 bootstrap_incarnation;

			/*
			 * spec-6.15 D5b: boot formation additionally requires the xid-stripe
			 * face resolved — striping-on holds until the activation record is
			 * published (staging the seed when self is the candidate), striping-
			 * off refuses to form an activated cluster (53RB1).  Keeping the gate
			 * undecided (fail-closed, like UNDECIDED below) retries next tick.
			 */
			ClusterXidStripeJoinVerdict sv
				= cluster_xid_stripe_join_gate(cluster_reconfig_self_is_stripe_seed_candidate());

			if (sv != CLUSTER_XID_STRIPE_JOIN_PROCEED) {
				static bool stripe_boot_logged = false;
				uint64 control_incarnation = 0;

				if (!stripe_boot_logged) {
					stripe_boot_logged = true;
					if (sv == CLUSTER_XID_STRIPE_JOIN_REFUSE)
						ereport(LOG,
								(errcode(ERRCODE_CLUSTER_XID_STRIPE_JOIN_MISMATCH),
								 errmsg("cluster xid stripe: refusing cold-bootstrap membership "
										"of node %d — stripe mode handshake mismatch "
										"(SQLSTATE 53RB1)",
										cluster_node_id),
								 errhint("cluster.xid_striping must match the cluster's durable "
										 "activation state on every node; repair the voting-disk "
										 "stripe region if it is corrupt.")));
					else
						ereport(LOG,
								(errmsg("cluster xid stripe: holding cold-bootstrap membership "
										"of node %d until the stripe activation state is "
										"resolved",
										cluster_node_id)));
				}
				/* AD-023: HOLD may publish the exact current MEMBER floor for
				 * recovery control, but must keep ordinary writes closed.  REFUSE
				 * retains the original fail-closed demotion. */
				if (sv == CLUSTER_XID_STRIPE_JOIN_HOLD)
					control_incarnation = cluster_qvotec_get_self_incarnation();
				LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
				ReconfigShmem->self_join_admitted = 0;
				ReconfigShmem->self_join_failed = 0;
				ReconfigShmem->self_join_deadline_us = 0;
				if (sv != CLUSTER_XID_STRIPE_JOIN_HOLD
					|| !cluster_reconfig_publish_self_current_floor_locked(cluster_node_id,
																		   control_incarnation)) {
					cluster_write_fence_authority_cache_invalidate();
					cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_JOINING);
				}
				LWLockRelease(&ReconfigShmem->lock);
				return;
			}

			/* BOOTSTRAP proven: establish the exact current-formation admitted
			 * floor before MEMBER or the shared write byte becomes visible.  A
			 * zero/drifting incarnation keeps this decision retryable. */
			bootstrap_incarnation = cluster_qvotec_get_self_incarnation();
			LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
			if (!cluster_reconfig_publish_self_current_floor_locked(cluster_node_id,
																	bootstrap_incarnation)) {
				cluster_write_fence_authority_cache_invalidate();
				ReconfigShmem->self_join_admitted = 0;
				ReconfigShmem->self_join_failed = 0;
				ReconfigShmem->self_join_deadline_us = 0;
				cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_JOINING);
				LWLockRelease(&ReconfigShmem->lock);
				return;
			}
			ReconfigShmem->self_join_failed = 0;
			ReconfigShmem->self_join_deadline_us = 0;
			/* MEMBER is already visible with an equal floor.  Only now may the
			 * write gate and process-local decision latch open. */
			ReconfigShmem->self_join_admitted = 1;
			joiner_gate_decided = true;
			LWLockRelease(&ReconfigShmem->lock);
			ereport(LOG,
					(errmsg("cluster membership: node %d cold-bootstrap membership formation — "
							"write gate open",
							cluster_node_id)));
		} else {
			/*
			 * UNDECIDED: neither proof holds yet.  Keep the gate CLOSED
			 * (fail-closed) and re-evaluate next tick — a slow qvotec
			 * waits here rather than mis-opening as bootstrap (P1-2).
			 *
			 * RF-SIDE integration closure (external-rejoin leg): while an
			 * EXTERNAL REJOIN FENCE is active, a node whose gate is
			 * ALREADY OPEN (self_join_admitted) carries independent strong
			 * admission evidence — the JOIN_COMMITTED marker +
			 * publish-proof (note_self_admitted), or the B' cold-
			 * formation marker admission — and its admission is managed
			 * by the external provider, so the boot classifier's LACK of
			 * evidence (this node never ran a founding classification)
			 * must not revoke it.  Without this guard every tick demoted
			 * the admitted node back to JOINING, the survivor set lost a
			 * member, no coordinator existed, and the external rejoin
			 * fence could never start its operation.
			 *
			 * implementation review (2026-08-20): the preservation is a CONJUNCTION,
			 * not a bare flag check — the admission is kept only while
			 * the node is a genuine formed member: self still in the
			 * current formation (membership MEMBER + in-quorum), the
			 * admitted floor is nonzero AND exactly equals the current
			 * self incarnation (a new postmaster incarnation does not
			 * inherit the old admission), and the external fence is
			 * active (the provider manages this admission).  Any missing
			 * fact demotes exactly like the ordinary UNDECIDED arm.
			 */
			LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
			cluster_write_fence_authority_cache_invalidate();
			{
				bool preserve;
				uint64 self_incarnation;

				self_incarnation = cluster_qvotec_get_self_incarnation();
				preserve = ReconfigShmem->self_join_admitted != 0
						   && cluster_external_fence_runtime_active() && cluster_qvotec_in_quorum()
						   && cluster_membership_get_state(cluster_node_id) == CLUSTER_MEMBER_MEMBER
						   && cluster_membership_get_last_admitted_incarnation(cluster_node_id) != 0
						   && cluster_membership_get_last_admitted_incarnation(cluster_node_id)
								  == self_incarnation;
				if (!preserve) {
					ReconfigShmem->self_join_admitted = 0;
					cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_JOINING);
				}
			}
			LWLockRelease(&ReconfigShmem->lock);
		}
	}
	/* A running-cluster classification continues below in this same tick when
	 * quorum is present.  Clean-leave suppression and every other undecided arm
	 * own no stripe progress and no join deadline. */
	if (!joiner_gate_decided)
		return;
	/* Preserve the historical timeout domain: after classification, loss of
	 * local quorum remains a fail-closed wait and does not consume the join
	 * convergence deadline from an LMON tick that cannot drive admission. */
	if (!in_quorum)
		return;

	/*
	 * The ordinary joiner owns its prerequisite progress on every in-quorum
	 * LMON tick.  WAIT_EVIDENCE cannot start the
	 * timeout.  The first exact staged/pending self CLAIM (or a fully resolved
	 * stripe face) arms the existing absolute deadline once; repeated ticks and
	 * quorum wobble never refresh it.
	 */
	if (!clean_leave_active && !ReconfigShmem->self_join_admitted
		&& !ReconfigShmem->self_join_failed) {
		ClusterXidStripeJoinProgress progress = cluster_xid_stripe_join_progress(false);

		if (progress == STRIPE_JOIN_REFUSE) {
			static bool stripe_join_refuse_logged = false;

			LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
			if (!ReconfigShmem->self_join_admitted && !ReconfigShmem->self_join_failed) {
				cluster_write_fence_authority_cache_invalidate();
				ReconfigShmem->self_join_failed = 1;
				ReconfigShmem->self_join_deadline_us = 0;
				cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_REJECTED);
			}
			LWLockRelease(&ReconfigShmem->lock);
			if (!stripe_join_refuse_logged) {
				stripe_join_refuse_logged = true;
				ereport(LOG, (errcode(ERRCODE_CLUSTER_XID_STRIPE_JOIN_MISMATCH),
							  errmsg("cluster xid stripe: refusing ordinary join of node %d — "
									 "stripe prerequisite mismatch (SQLSTATE 53RB1)",
									 cluster_node_id)));
			}
			return;
		}
		if (progress == STRIPE_JOIN_CLAIM_OWNED || progress == STRIPE_JOIN_PROCEED) {
			LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
			if (!ReconfigShmem->self_join_admitted && !ReconfigShmem->self_join_failed
				&& ReconfigShmem->self_join_deadline_us == 0)
				ReconfigShmem->self_join_deadline_us
					= now_us + (uint64)cluster_join_convergence_timeout_ms * 1000ULL;
			LWLockRelease(&ReconfigShmem->lock);
		}
	}

	/* Gate decided.  If closed + not yet admitted, fail closed on timeout (53R61);
	 * note_self_admitted opens the gate directly when self's COMMITTED marker is
	 * observed. */
	if (!ReconfigShmem->self_join_admitted && !ReconfigShmem->self_join_failed
		&& ReconfigShmem->self_join_deadline_us != 0
		&& now_us >= ReconfigShmem->self_join_deadline_us) {
		LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
		cluster_write_fence_authority_cache_invalidate();
		ReconfigShmem->self_join_failed = 1;
		cluster_membership_set_state(cluster_node_id, CLUSTER_MEMBER_REJECTED);
		LWLockRelease(&ReconfigShmem->lock);
		pg_atomic_fetch_add_u64(&ReconfigShmem->join_timeout_count, 1);
		ereport(LOG, (errmsg("cluster membership: node %d join did not converge within %d ms — "
							 "writes now 53R61 (restart with a fresh incarnation)",
							 cluster_node_id, cluster_join_convergence_timeout_ms)));
	}
}


/*
 * cluster_reconfig_offpath_rejoin_tick -- crash-rejoin re-declare barrier
 * (Shape A), the cluster.online_join=off counterpart of the joiner self-tick.
 *
 *	With online_join=off the joiner self-tick above early-returns, so a node
 *	that crash-restarts into a running cluster keeps its shmem-init
 *	self_join_admitted = 1 (cluster_reconfig.c:206) and boots straight to a
 *	writable MEMBER with an EMPTY GRD and no re-declare episode.  For a block
 *	whose static home is self, the acquire path then cold-grants from the
 *	stale/empty disk page (silent stale read / silently-diverging write — the
 *	P0).  The phase-gate boot barrier (cluster_gcs_block.c) fences self-home
 *	blocks RECOVERING from process start (the flag defaults 0) until THIS tick
 *	classifies the incarnation, so there is zero cold-serve window.
 *
 *	Decision (LMON single-writer; the barrier flag and self_join_admitted are
 *	both flipped here):
 *	  - single declared node       -> decided, no fence (no peer can conflict)
 *	  - crash-rejoin (already run)  -> arm the self-fence, THEN demote
 *	                                   self_join_admitted to 0 (Rule 8.A: never
 *	                                   raised to 1 before the fence is armed);
 *	                                   home blocks stay RECOVERING via the join
 *	                                   fence, writes fail-closed 53R60.  The
 *	                                   survivor re-declare self-heal (fence
 *	                                   lift) is a separate spec (Shape B).
 *	  - cold bootstrap at INITIAL   -> decided, no fence (fresh cluster, no
 *	                                   stale home blocks)
 *	  - undecided                   -> leave the barrier up (fail-closed),
 *	                                   retry next tick
 *
 *	online_join=on takes its own joiner_self_tick / note_self_admitted path
 *	and never enters here.
 */
/* Forward: three-state cold-formation observation window (defined below
 * with the cold-formation state machine). */
typedef enum ClusterColdFormationVerdict {
	CLUSTER_COLD_FORMATION_PENDING = 0, /* no survivor seen; quorum not yet fresh */
	CLUSTER_COLD_FORMATION_SURVIVOR,	/* a fresh slot is past INITIAL: never form */
	CLUSTER_COLD_FORMATION_COBOOT		/* quorum fresh, no survivor: window passed */
} ClusterColdFormationVerdict;

/* 连续满足观察窗的 tick 数（~300 ms at the 100 ms LMON tick）。 */
#define CLUSTER_COLD_FORMATION_WINDOW_TICKS 3

static ClusterColdFormationVerdict cluster_reconfig_cold_formation_window(uint64 *out_coboot_lo,
																		  uint64 *out_coboot_hi);

/*
 * RF-ROOT P9 verification / cold-formation cold-formation ruling: true only
 * after CLUSTER_COLD_FORMATION_WINDOW_TICKS CONSECUTIVE SURVIVOR verdicts —
 * a genuinely formed, continuously fresh peer past INITIAL.  A clean stop's
 * leftover slot can stay fresh for a tick or two with the OLD epoch byte;
 * the stability requirement prevents that residue from self-admitting a
 * cold co-boot as "clean reopen".
 */
static bool
cold_formation_survivor_verdict(void)
{
	static uint64 survivor_ticks = 0;

	switch (cluster_reconfig_cold_formation_window(NULL, NULL)) {
	case CLUSTER_COLD_FORMATION_SURVIVOR:
		if (++survivor_ticks >= CLUSTER_COLD_FORMATION_WINDOW_TICKS)
			return true;
		return false;
	case CLUSTER_COLD_FORMATION_COBOOT:
	case CLUSTER_COLD_FORMATION_PENDING:
	default:
		survivor_ticks = 0;
		return false;
	}
}

void
cluster_reconfig_offpath_rejoin_tick(void)
{
	static bool offpath_decided_local = false;

	if (ReconfigShmem == NULL || cluster_online_join)
		return; /* off path only */
	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		return;
	if (offpath_decided_local) {
		/* The approved shared-CF fast-rejoin ends only after the durable
		 * admission callback opened the existing gate and every survivor's
		 * existing JOIN re-declare barrier is complete.  Until both are true,
		 * the epoch-independent boot barrier remains the read fence. */
		if (offpath_fast_rejoin_active_local && ReconfigShmem->self_join_admitted
			&& cluster_grd_join_view_rebuilt()) {
			cluster_grd_set_offpath_boot_decided();
			offpath_fast_rejoin_active_local = false;
			ereport(LOG, (errmsg("cluster membership: node %d shared-CF fast-rejoin "
								 "admission and re-declare complete — boot fence lifted",
								 cluster_node_id)));
		}
		return; /* once per incarnation (LMON-local) */
	}
	if (cluster_reconfig_is_removed_unlocked(cluster_node_id))
		return; /* a removed node keeps its 53R64 self-demote gate */

	if (cluster_conf_node_count() <= 1) {
		/* Lone declared node: no peer can hold a conflicting copy, so there is
		 * nothing to re-declare — decide at once so the boot barrier never
		 * fences a single-node deployment. */
		cluster_grd_set_offpath_boot_decided();
		offpath_decided_local = true;
		return;
	}

	/*
	 * RF-ROOT P9 verification / cold-formation cold-formation ruling:
	 * a node admitted by the quorum-majority COMMITTED formation marker
	 * is NOT a crash-rejoiner — the marker proves no live survivor
	 * existed at co-boot (the observation window saw only fresh slots at
	 * INITIAL) and names this boot's exact incarnations, so there are no
	 * stale holder/buffer states for a survivor to re-declare.  Without
	 * this arm the rejoin tick fired moments later anyway: once the
	 * marker admission flips self to MEMBER the slot epoch rises past
	 * INITIAL (slot-epoch A-semantics), the peer observes fresh + past
	 * INITIAL and already_running() turns true, and the clean-reopen
	 * arm below — which needs 3 consecutive SURVIVOR verdicts — had not
	 * latched yet, so the REJOIN arm fenced self (53R60) and zeroed
	 * self_join_admitted, wedging the freshly formed pair in phase 3.
	 */
	if (cold_formation_state.admission_done) {
		cluster_grd_set_offpath_boot_decided();
		offpath_decided_local = true;
		ereport(LOG, (errmsg("cluster membership: node %d cold-formation admitted "
							 "(online_join=off) — no re-declare fence armed",
							 cluster_node_id)));
		return;
	}

	/*
	 * RF-ROOT P6 (STOP-01 frozen THREAD_OPEN / THREAD_CLEAN_CLOSE, the
	 * Oracle clean-reopen mainline):  a node that CLEANLY closed its own
	 * redo thread (shutdown checkpoint durable + CLOSED root, so
	 * prior_unclean_death is false) and now restarts into a running
	 * cluster is a clean REOPEN, not a crash-rejoin.  No self-fence, no
	 * self_join_admitted demotion:  the thread was cleanly closed, so
	 * there are no stale holder/buffer states a survivor must re-declare
	 * for, and the phase gate + serving predicate keep writes closed
	 * until the ordinary admission re-lands.  The THREAD_OPEN publish in
	 * phase 3 reopens the root (CLOSED -> OPEN with the fresh boot
	 * incarnation) so the survivor's join chain can commit.
	 *
	 * A crash / immediate-stop leaves the ALIVE bit set
	 * (prior_unclean_death), so that path still takes the REJOIN arm
	 * below with the full self-fence + re-declare barrier.
	 */
	if (cluster_reconfig_cluster_already_running() && !cluster_qvotec_prior_unclean_death()) {
		/*
		 * RF-ROOT P9 verification / cold-formation cold-formation ruling:
		 * the clean-reopen self-admission must NOT fire while a cold
		 * co-boot is possible.  A clean stop's leftover slot can stay
		 * fresh-alive for a tick or two (heartbeat_ts not yet expired)
		 * with the OLD epoch byte — counting that as "a running cluster"
		 * made every cold co-boot past INITIAL self-admit MEMBER, publish
		 * the live epoch, and deny the 5.22 observation window on the
		 * peers.  The cold-formation window verdict is the arbiter:
		 * COBOOT (or PENDING while the window may still turn COBOOT)
		 * defers; only a STABLE SURVIVOR verdict (a genuinely formed,
		 * continuously fresh peer past INITIAL) self-admits.
		 *
		 * CRITICAL: a clean-reopen CANDIDATE (a peer is observed running
		 * AND this node did NOT die uncleanly) must NEVER fall through to
		 * the REJOIN arm below while the survivor verdict is still
		 * stabilizing.  The verdict needs
		 * CLUSTER_COLD_FORMATION_WINDOW_TICKS consecutive SURVIVOR
		 * ticks; on the first ticks after boot the observation window
		 * may still be PENDING (peer slot not yet re-observed) — with a
		 * fall-through, already_running() (true) sent the node into the
		 * crash-rejoin arm: self-fence armed, self_join_admitted = 0,
		 * and a clean restart into a live cluster (t/274 L4/L5) wedged
		 * in 53R61.  Stay UNDECIDED (fail-closed, no fence) and retry
		 * next tick.
		 */
		if (cold_formation_survivor_verdict()) {
			cluster_grd_set_offpath_boot_decided();
			offpath_decided_local = true;
			ereport(
				LOG,
				(errmsg(
					"cluster membership: node %d clean reopen detected (cluster.online_join=off) "
					"— thread clean-closed, no re-declare fence armed",
					cluster_node_id)));
			return;
		}
		return; /* clean-reopen candidate: never crash-rejoin */
	}

	/*
	 * REJOIN when EITHER signal fires:
	 *   already_running        -- a declared peer is observed past INITIAL (the
	 *                             survivor already reconfigured; slow rejoin).
	 *   prior_unclean_death    -- this node's prior-incarnation voting-disk
	 *                             self-slot still carried ALIVE (an unclean
	 *                             death).  This is the ONLY signal that fires on
	 *                             a FAST rejoin, where the node restarts inside
	 *                             the survivor's dead-deadband so BOTH sides are
	 *                             still at epoch INITIAL and the epoch signal is
	 *                             blind (守门裁决 07-15: ALIVE bit, not epoch).
	 * The clean-shutdown blank clears ALIVE (keeps epoch), so a genuine clean
	 * co-boot never trips prior_unclean_death.
	 */
	if (cluster_reconfig_cluster_already_running() || cluster_qvotec_prior_unclean_death()) {
		uint8 self_set[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };

		/*
		 * Arm the epoch-keyed join fence too (belt: it engages on a SLOW
		 * rejoin where this node adopted a peer epoch > INITIAL).  On a FAST
		 * rejoin this node is still at CLUSTER_EPOCH_INITIAL, so the monotonic-
		 * max fence epoch cannot rise above 0 and the join fence is a no-op —
		 * the READ fence is therefore carried by the boot barrier below, which
		 * is epoch-independent: we deliberately DO NOT set offpath_boot_decided,
		 * so the phase gate keeps self-home blocks RECOVERING for the whole
		 * incarnation (fail-closed until a clean restart / online_join).
		 */
		self_set[cluster_node_id >> 3] = (uint8)(1u << (cluster_node_id & 7));
		cluster_grd_arm_join_pcm_fence(self_set); /* fence FIRST (8.A) */

		LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
		cluster_write_fence_authority_cache_invalidate();
		ReconfigShmem->self_join_admitted = 0; /* then close the write gate */
		LWLockRelease(&ReconfigShmem->lock);

		/* NB: offpath_boot_decided stays 0 -> the boot barrier persists as the
		 * read fence for this incarnation.  offpath_decided_local latches the
		 * tick so it does not re-arm / re-log every cycle. */
		cluster_grd_inc_offpath_crash_rejoin_fenced();
		if (cluster_controlfile_shared_authority)
			offpath_fast_rejoin_active_local = true;
		offpath_decided_local = true;

		ereport(LOG,
				(errmsg("cluster membership: node %d crash-rejoin detected (cluster.online_join="
						"off%s) — home blocks fenced and writes closed (53R60) to avoid serving "
						"stale ownership",
						cluster_node_id,
						cluster_qvotec_prior_unclean_death() ? ", prior unclean shutdown" : ""),
				 errhint("Enable cluster.online_join for an online re-declare rejoin (admission "
						 "self-heal is spec-5.22 follow-up), or cold-restart the cluster after a "
						 "full clean shutdown. Reads of peer-mastered blocks and non-home work "
						 "are unaffected.")));
		return;
	}

	if (cluster_reconfig_bootstrap_quorum_at_initial()) {
		/* Clean cold bootstrap: fresh cluster co-booting at INITIAL, ALIVE was
		 * cleared on the prior clean shutdown (or never written) — no stale
		 * home blocks.  The !prior_unclean_death predicate is already proven by
		 * the REJOIN arm above (守门裁决 07-15 point 5②). */
		cluster_grd_set_offpath_boot_decided();
		offpath_decided_local = true;
		return;
	}

	/* UNDECIDED: keep the boot barrier up (fail-closed) and retry next tick. */
}


/* ============================================================
 * Step 2 D2 — cluster_reconfig_lmon_tick body.
 *
 *	  Stateless deterministic per Q6 C.  Runs every LMON tick (~100ms).
 *	  Implements:
 *	    §3.1  CSSD DEAD edge detection (declared-peer filter F11)
 *	    §3.2  Q2 A'' coordinator decision (P1.1 — CSSD survivor SSOT)
 *	    §3.2  event_id hash dedup (P1.2 — dead_gen, NOT old_epoch)
 *	    §3.4  I7 every-in_quorum-survivor PROCSIG broadcast (P1.3)
 *	    §3.3  I7 coordinator-only epoch++ (P1.3)
 * ============================================================
 */

void
cluster_reconfig_lmon_tick(void)
{
	uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint8 new_failure_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint8 alive_set[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	int32 self_id;
	int coordinator;
	uint64 cssd_dead_generation;
	uint64 event_id;
	int i;
	bool failstop_stage_handled = false;
	bool failure_generation_changed = false;
	bool external_rejoin_active;
	bool ordinary_actions_allowed;
	bool offpath_fast_rejoin_actions;
	bool fast_rejoin_control_actions;
	bool runtime_join_allowed;
	bool self_floor_authority = false;
	uint64 self_floor_incarnation = 0;
	int32 root_gated_join_node = -1;
	uint64 root_gated_join_incarnation = 0;
	uint64 fast_rejoin_control_incarnation = 0;
	int32 fast_rejoin_control_target = -1;

	/* L20: runtime feature flag check first line. */
	if (!cluster_enabled) {
		cluster_reconfig_fast_rejoin_control_clear();
		cluster_reconfig_external_rejoin_release_all();
		if (join_commit_stage.external_rejoin_consumed)
			cluster_reconfig_release_join_commit_stage();
		return;
	}
	/* Scheme A: membership/quorum formation and fail-stop authority recovery
	 * remain live before OPEN, but runtime replacement/readmission is serving
	 * work.  A cluster that entered the managed startup lifecycle may not drive
	 * those actions from STARTING or RECOVERY_AUTHORITY_READY. */
	ordinary_actions_allowed = !cluster_authority_readiness_managed()
							   || cluster_authority_serving_rebind_lmon()
							   /* RF-ROOT P6 (L5 shutdown handoff): the committed LEAVER re-binds
		   * its serving authority from its own applied CLEAN_LEAVE evidence
		   * (no local episode closes for its own departure), so its shutdown
		   * checkpoint / THREAD_CLEAN_CLOSE CF acquires keep working. */
							   || cluster_authority_serving_rebind_leaver();

	/*
	 * RF-ROOT P6 (t/243 cast wedge + L5 shutdown handoff):  a shutdown-
	 * requested / reconfig-suppressed LMON must not publish NEW reconfig
	 * events (fail-stop / join) — publishing a FAIL_STOP while the node is
	 * exiting drives the GRD into a recovery episode whose non-current
	 * authority then rejects the checkpointer's shutdown-checkpoint CF(X).
	 * The gate therefore runs BELOW the serving rebind:  the retained LMON
	 * must keep re-confirming the serving authority (the shutdown handoff's
	 * post-commit step depends on it) while only the event PUBLICATION paths
	 * are silenced.
	 */
	if (ShutdownRequestPending || cluster_lmon_reconfig_suppressed())
		return;
	offpath_fast_rejoin_actions = cluster_reconfig_fast_rejoin_actions_snapshot();
	fast_rejoin_control_target
		= cluster_reconfig_fast_rejoin_control_snapshot(&fast_rejoin_control_incarnation);
	fast_rejoin_control_actions = fast_rejoin_control_target >= 0;
	runtime_join_allowed = cluster_online_join || offpath_fast_rejoin_actions;
	external_rejoin_active = ordinary_actions_allowed && cluster_external_fence_runtime_active();
	if (!external_rejoin_active) {
		cluster_reconfig_external_rejoin_release_all();
		if (join_commit_stage.external_rejoin_consumed)
			cluster_reconfig_release_join_commit_stage();
	}

	CLUSTER_INJECTION_POINT("cluster-reconfig-tick-entry");


	/* spec-2.29 D9 wait event registered for pg_stat_cluster_wait_events
	 * SRF visibility;pgstat_report_wait_start wrapping deferred to Sprint
	 * A hardening (lmon_tick has many early returns; clean wait_start/
	 * wait_end pairing needs cleanup refactor). */

	/* A fresh peer past INITIAL is sufficient only for the fail-closed
	 * bootstrap->ordinary-join classification.  Run that narrow self tick before
	 * the participation gate so a slow node can adopt the current transport epoch
	 * and close its service gate.  The tick itself keeps bootstrap admission and
	 * deadline consumption behind local in-quorum evidence. */
	cluster_reconfig_membership_floor_diagnostic("before-joiner-self");
	cluster_reconfig_joiner_self_tick();
	cluster_reconfig_membership_floor_diagnostic("after-joiner-self");

	/* I2 + I8: only in_quorum nodes participate in reconfig. */
	if (!cluster_qvotec_in_quorum()) {
		cluster_reconfig_fast_rejoin_control_clear();
		cluster_reconfig_external_rejoin_release_all();
		if (join_commit_stage.external_rejoin_consumed)
			cluster_reconfig_release_join_commit_stage();
		return;
	}

	self_id = cluster_node_id;
	if (self_id < 0 || self_id >= CLUSTER_MAX_NODES) {
		cluster_reconfig_fast_rejoin_control_clear();
		cluster_reconfig_external_rejoin_release_all();
		return; /* defensive: bad self id, cannot participate */
	}
	if (ordinary_actions_allowed) {
		cluster_reconfig_lmon_replacement_closed_tick();
		cluster_reconfig_lmon_replacement_ready_tick();
		cluster_reconfig_lmon_replacement_admit_tick();
	}
	failstop_stage_handled = cluster_reconfig_poll_failstop_fence_stage();

	/*
	 * RF-ROOT P6 (STOP-01 contract): drain an in-flight join
	 * PREPARE stage UNGATED, exactly like the fail-stop fence stage above.
	 * The P04 fast-rejoin eviction stages JOIN_PENDING without a fail-stop
	 * event;  gating its drain behind the join-drive (which requires the
	 * serving rebind, which requires the GRD JOIN episode, which requires
	 * the JOIN_PENDING event) is a circular deadlock that strands the
	 * joiner until 53R61 (observed: L4 crash-rejoin).  The drain only
	 * completes an already-staged publication — no new admission decision —
	 * and is a no-op when nothing is staged.
	 */
	(void)cluster_reconfig_poll_join_prepare_stage();

	/*
	 * RF-ROOT P6 (STOP-01 contract): drain an in-flight
	 * join COMMIT stage UNGATED, for the same circularity one stage later.
	 * The L5 restore boot stages the commit (JCMK majority-durable, re-vet
	 * passed, epoch pre-bumped, fence marker submitted) and then the
	 * join-drive gate closes: the pending-join formation (node1 JOINING)
	 * drifts from the serving binding the evict-prior rebind re-stamped
	 * (node1 ABSENT), so ordinary_actions_allowed=0 and drive_joins — and
	 * with it the commit-stage poll that would publish the already-
	 * authorized JOIN_COMMITTED event — is never called.  The stage only
	 * completes an already-staged, majority-durable publication; every
	 * safety gate (re-vet, owner, epoch, predecessor, bitmaps) lives inside
	 * the poll, so the ungated drain cannot open a new admission decision.
	 * No-op when nothing is staged.
	 */
	(void)cluster_reconfig_poll_join_commit_stage();

	/*
	 * §3.1 + F11: build the raw CSSD DEAD bitmap, filtering out un-declared
	 * peers.  Self is alive by construction (it is running this tick, in
	 * quorum).  Lock-free snapshot — the membership-state maintenance and the
	 * survivor-set build below run under the reconfig lock.
	 */
	if (cluster_conf_lookup_node(self_id) == NULL) {
		cluster_reconfig_fast_rejoin_control_clear();
		cluster_reconfig_external_rejoin_release_all();
		return; /* self un-declared — must not be coordinator */
	}

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == self_id)
			continue;
		if (cluster_conf_lookup_node(i) == NULL)
			continue; /* F11: skip un-declared peer */

		if (cluster_cssd_get_peer_state(i) == CLUSTER_CSSD_PEER_DEAD) {
			dead_bitmap_set_bit(dead_bitmap, i);
		}
	}
	cssd_dead_generation = cluster_cssd_get_dead_generation();

	/*
	 * Shape A (crash-rejoin re-declare barrier) — the online_join=off
	 * counterpart: arm the self-fence + close the write gate if THIS node
	 * crash-rejoined a running cluster, and lift the boot barrier once the
	 * bootstrap-vs-rejoin classification is proven.  No-op on online_join=on.
	 */
	cluster_reconfig_offpath_rejoin_tick();

	/* Sample the candidate formation before the membership lock.  Online join
	 * reaches this choke only after its separate admission edge opened the byte;
	 * config-off additionally needs Shape-A's positive boot decision because the
	 * shmem default byte is not authority by itself. */
	self_floor_authority = cluster_online_join || offpath_fast_rejoin_active_local
						   || cluster_grd_offpath_boot_decided()
						   || cold_formation_state.admission_done;
	if (self_floor_authority)
		self_floor_incarnation = cluster_qvotec_get_self_incarnation();

	/*
	 * Frozen STOP-02 §17.6: survivor-side observation of a durable COMMITTED
	 * JCMK is not by itself authority to clear an excluded origin.  Select one
	 * exact DEAD->ALIVE candidate while holding the reconfig snapshot lock, then
	 * perform the direct-majority JCMK + ROOT owner CAS outside that lock.  The
	 * exclusive mutation below revalidates the exact incarnation before use.
	 * Serializing one origin per tick also preserves the one-bit JOIN clear rule.
	 */
	if (ordinary_actions_allowed && ReconfigShmem != NULL && runtime_join_allowed) {
		uint64 candidate_incarnation = 0;

		LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			if (i == self_id || cluster_conf_lookup_node(i) == NULL
				|| cluster_membership_get_state(i) != CLUSTER_MEMBER_DEAD
				|| cluster_cssd_get_peer_state(i) != CLUSTER_CSSD_PEER_ALIVE)
				continue;
			if (cluster_reconfig_get_observed_committed_join(i, &candidate_incarnation, NULL)
				&& candidate_incarnation > cluster_membership_get_last_admitted_incarnation(i)) {
				root_gated_join_node = i;
				break;
			}
		}
		LWLockRelease(&ReconfigShmem->lock);

		if (root_gated_join_node >= 0
			&& cluster_recovery_owner_rejoin_v1(root_gated_join_node, candidate_incarnation))
			root_gated_join_incarnation = candidate_incarnation;
		else
			root_gated_join_node = -1;
	}

	/*
	 * spec-5.15 D1 (INV-J8): the membership-state table — NOT raw CSSD — is the
	 * decision SSOT for the survivor / coordinator set.  Maintain it and build
	 * alive_set from it, under EXCLUSIVE (we mutate membership_state):
	 *   self                         -> MEMBER (running + in quorum)
	 *   peer CSSD DEAD               -> DEAD   (a member that died; demote)
	 *   peer CSSD alive + state ABSENT -> MEMBER (bootstrap join, §3.4: a
	 *       never-seen declared node forming the initial membership; NOT gated
	 *       by online_join, which gates only runtime readmission)
	 *   peer CSSD alive + state DEAD -> stays DEAD (a recovered node is NOT
	 *       auto-readmitted — a JOIN_COMMITTED reconfig (D4) is the only
	 *       DEAD->MEMBER path; this is the §3.4 online_join=off isolation + the
	 *       P1c fix that closes "CSSD ALIVE silently counts as a member")
	 *   peer CSSD alive + JOINING/MEMBER -> unchanged
	 *
	 * Then apply spec-5.13 CL-I13 effective_dead = cssd_dead & ~clean_departed
	 * (a cleanly-departed member stops heart-beating and shows up CSSD DEAD;
	 * masking it out suppresses the spurious SECOND fail-stop reconfig).  Folded
	 * into the same EXCLUSIVE section (was a separate SHARED read pre-5.15).
	 *
	 * When the region is absent (cluster off / pre-postmaster) fall back to the
	 * pre-5.15 raw-CSSD survivor set so the degraded path is unchanged.
	 */
	if (ReconfigShmem != NULL) {
		int b;
		/* spec-5.16 — survivor-side runtime readmit: peers recognized DEAD->MEMBER
		 * from their durable COMMITTED join marker this tick (an observer JOIN
		 * event is published for them after the lock is released). */
		uint8 newly_joined[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
		uint8 join_remaining_dead[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
		bool any_joined = false;

		memset(newly_joined, 0, sizeof(newly_joined));
		memset(join_remaining_dead, 0, sizeof(join_remaining_dead));

		LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);

		/*
		 * spec-5.18 INV-LF9 (HF-2): REMOVED is TERMINAL for self too.  A removed
		 * node still running / restarted (rebuild seeded removed_bitmap[self] +
		 * membership_state[self]=REMOVED) must keep self REMOVED — the joiner gate
		 * below must NOT flip it back to MEMBER/JOINING, which would defeat the
		 * 53R64 self-demote write gate and let a removed node serve writes.  The
		 * durable removed_bitmap is the authoritative floor (mirrors the peer
		 * REMOVED terminal guard further down + the joiner_self_tick guard).
		 */
		if (clean_departed_test_bit_locked(ReconfigShmem->removed_bitmap, self_id)
			|| cluster_membership_get_state(self_id) == CLUSTER_MEMBER_REMOVED)
			cluster_membership_set_state(self_id, CLUSTER_MEMBER_REMOVED);
		/* Replacement ADMITTED is a deliberate MEMBER-with-service-closed state;
		 * do not collapse it back to ordinary JOINING merely because the shared
		 * write byte remains zero.  Otherwise self-state follows the v2 gate. */
		else if (cluster_reconfig_is_local_admitted_replacement(
					 &ReconfigShmem->replacement_episode))
			cluster_membership_set_state(self_id, CLUSTER_MEMBER_MEMBER);
		/* A terminal stripe prerequisite refusal is stronger than the generic
		 * closed-gate JOINING fallback below and remains fail-closed. */
		else if (ReconfigShmem->self_join_failed)
			cluster_membership_set_state(self_id, CLUSTER_MEMBER_REJECTED);
		/* AD-023 recovery-control formation: joiner_self_tick may have
		 * published this exact epoch-0 MEMBER floor while the xid-stripe gate
		 * is still HOLD.  Preserve only that same retryable, write-closed
		 * identity; the process-local bootstrap decision remains unlatched. */
		else if (cluster_online_join && !joiner_gate_decided && !ReconfigShmem->self_join_admitted
				 && !ReconfigShmem->self_join_failed
				 && cluster_epoch_get_current() == CLUSTER_EPOCH_INITIAL
				 && cluster_reconfig_bootstrap_quorum_at_initial()
				 && cluster_membership_get_state(self_id) == CLUSTER_MEMBER_MEMBER
				 && cluster_reconfig_publish_self_current_floor_locked(self_id,
																	   self_floor_incarnation)) {
			/* Exact helper retained MEMBER; ordinary writes remain closed. */
		} else if ((ReconfigShmem->self_join_admitted && !ReconfigShmem->self_join_failed
						&& (cluster_online_join
							/*
					   * RF-ROOT P6 (crash-rejoin): a durably admitted
					   * shared-CF fast rejoiner keeps self MEMBER while the
					   * boot-decided latch is still held.  The admission is
					   * the membership proof (quorum-majority COMMITTED
					   * marker + publish-proof); the epoch-independent
					   * boot fence stays armed and keeps self-home blocks
					   * RECOVERING until the re-declare barrier completes,
					   * so no cold-serve window opens.  Demoting self to
					   * JOINING here instead deadlocks the rejoin: the
					   * phase-3 recovery-authority barrier's request check
					   * and the GES transport gate both read the live
					   * membership byte, and the re-declare ingress that
					   * would lift the latch is exactly what they gate.
					   */
							|| offpath_fast_rejoin_active_local
							|| (self_floor_authority && cluster_grd_offpath_boot_decided()))
					/*
				  * RF-ROOT P9 verification / cold-formation cold-formation
				  * ruling: after the quorum-majority COMMITTED formation
				  * marker admitted this co-boot (self_join_admitted is set
				  * by that admission), the per-tick self-state decision
				  * must NOT demote self back to JOINING.  The
				  * online_join=off + non-fast-rejoin gate arm above is
				  * unreachable for a cold-formation admittee (the marker
				  * path is neither online_join nor offpath fast-rejoin),
				  * so without this arm every tick overwrote the marker
				  * admission back to JOINING while peers kept MEMBER —
				  * the cluster wedged in phase 3 with both nodes seeing
				  * each other MEMBER and themselves JOINING.
				  */
					|| cold_formation_state.admission_done)
				   && cluster_reconfig_publish_self_current_floor_locked(self_id,
																		 self_floor_incarnation)) {
			/* Exact helper already published MEMBER. */
		} else {
			cluster_membership_set_state(self_id, CLUSTER_MEMBER_JOINING);
		}

		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			ClusterMembershipState ms;
			uint64 observed_incarnation = 0;
			uint64 observed_generation = 0;
			uint64 prior_incarnation;

			if (i == self_id)
				continue;
			if (cluster_conf_lookup_node(i) == NULL)
				continue;

			ms = cluster_membership_get_state(i);
			prior_incarnation = ReconfigShmem->fast_rejoin_incarnation[i];
			if (!cluster_online_join && cluster_controlfile_shared_authority
				&& ms == CLUSTER_MEMBER_MEMBER && prior_incarnation == 0
				&& cluster_reconfig_get_observed_fresh_alive(i)
				&& cluster_reconfig_get_observed_slot(i, &observed_incarnation,
													  &observed_generation)
				&& observed_incarnation > 0) {
				/* A stale provisioning slot is not identity authority.  The first
				 * fresh-alive slot seen for an already-formed peer establishes the
				 * volatile rollover baseline; only a later larger value is a
				 * rollover.  Do not mutate the admitted serving formation here. */
				ReconfigShmem->fast_rejoin_incarnation[i] = observed_incarnation;
				prior_incarnation = observed_incarnation;
			}

			/* P04 fast restart can replace a process inside CSSD's deadband, so
			 * liveness never presents a DEAD edge.  Under verified shared-CF
			 * authority, a CRC-checked slot incarnation strictly above an existing
			 * MEMBER floor is the missing eviction edge: exclude the prior
			 * incarnation first, then let the unchanged join protocol admit the
			 * newer one.  A zero floor is bootstrap, never rollover evidence.
			 *
			 * RF-ROOT P6 (STOP-01 contract): the gate also accepts a
			 * DEAD-state peer (cssd ALIVE + fresh slot + newer incarnation):
			 * when the deadband wins the fast-restart race, the fail-stop
			 * already demoted the peer, and without this arm the join has no
			 * runtime leg (runtime_join_allowed is only armed here) -- the
			 * joiner then waits for a JCMK that can never be written. */
			if (!cluster_online_join && cluster_controlfile_shared_authority
				&& (ms == CLUSTER_MEMBER_MEMBER || ms == CLUSTER_MEMBER_DEAD)
				&& prior_incarnation > 0
				&& cluster_cssd_get_peer_state(i) == CLUSTER_CSSD_PEER_ALIVE
				&& cluster_reconfig_get_observed_fresh_alive(i)
				&& cluster_reconfig_get_observed_slot(i, &observed_incarnation,
													  &observed_generation)
				&& observed_incarnation > prior_incarnation) {
				if (!dead_bitmap_test_bit(ReconfigShmem->fast_rejoin_bitmap, i))
					ereport(LOG, (errmsg("cluster membership: shared-CF fast-rejoin "
										 "evicting prior incarnation of node %d "
										 "(%llu -> %llu)",
										 i, (unsigned long long)prior_incarnation,
										 (unsigned long long)observed_incarnation)));
				dead_bitmap_set_bit(ReconfigShmem->fast_rejoin_bitmap, i);
				dead_bitmap_set_bit(dead_bitmap, i);
				offpath_fast_rejoin_actions = true;
				runtime_join_allowed = true;
			}
			/*
			 * spec-5.18 INV-LF1 (P0): REMOVED is TERMINAL.  This loop reads the RAW
			 * CSSD dead set (the removed mask is applied later, below), so a
			 * fail-stopped-then-removed node is still CSSD-DEAD here — without this
			 * guard the next branch would flip its membership_state REMOVED -> DEAD
			 * every tick, silently defeating the vet_joiner REJECT_REMOVED_FENCED
			 * gate (which keys on state==REMOVED) and letting a fresh-incarnation
			 * zombie passively re-admit.  A removed node never leaves REMOVED here.
			 */
			if (ms == CLUSTER_MEMBER_REMOVED)
				continue;
			if (dead_bitmap_test_bit(dead_bitmap, i))
				cluster_membership_set_state(i, CLUSTER_MEMBER_DEAD);
			else if (ms == CLUSTER_MEMBER_ABSENT) {
				/*
				 * RF-ROOT P9 verification / cold-formation ruling
				 * (2026-08-19): ABSENT -> MEMBER is the founding-formation
				 * admission ONLY at the INITIAL epoch, and it publishes the
				 * founding-quorum-vetted incarnation as the formal admitted
				 * floor BEFORE MEMBER — D13 stays strict (last_admitted != 0
				 * && exact, spec-8.4c-stage8-d13; no presented carve-out).
				 * The peer identity comes from the WHOLE-round QVOTEC
				 * bootstrap proof — cluster_reconfig_bootstrap_proof_node
				 * samples in-quorum + per-node observed incarnation/
				 * generation/epoch/fresh-alive under ONE seqlock window and
				 * recomputes the founding quorum from that coherent
				 * snapshot, so a "new incarnation + stale fresh-alive"
				 * combination can never admit.  The local-epoch require
				 * stays here (reconfig-local state, not part of the QVOTEC
				 * window).  Every require is a lock-free shmem read, so this
				 * branch can never block the tick; on any failure the peer
				 * stays ABSENT and the next tick retries (fail-closed).
				 * DEAD stays DEAD (JCMK-only readmission).
				 *
				 * PAST the initial epoch (clean shutdown + restart / full
				 * outage co-boot) the ABSENT admission is the 5.22
				 * cold-formation path (cold-formation cold-formation ruling): a
				 * quorum-stable observation window with no fresh slot past
				 * INITIAL proves no live survivor, the lowest co-boot node
				 * writes a quorum-majority COMMITTED formation marker, and
				 * every member admits from it.  The state machine runs once
				 * per tick below the membership loop; a runtime never-seen
				 * node in a LIVE cluster (fresh slot past INITIAL) never
				 * passes the window and keeps the ordinary join/JCMK path.
				 */
				uint64 obs_inc = 0;

				if (cluster_epoch_get_current() == CLUSTER_EPOCH_INITIAL) {
					if (!cluster_reconfig_bootstrap_proof_node(i, &obs_inc))
						continue; /* stays ABSENT; retried next tick */
					cluster_membership_record_admitted(i, obs_inc);
					if (cluster_membership_get_last_admitted_incarnation(i) != obs_inc)
						continue; /* floor did not land exactly — do not admit */
				} else if (cluster_reconfig_get_observed_fresh_alive(i)
						   && cluster_reconfig_get_observed_epoch(i) > CLUSTER_EPOCH_INITIAL) {
					/*
					 * RF-ROOT P9 verification / cold-formation cold-formation
					 * ruling — LIVE-SURVIVOR identity: an ABSENT peer whose
					 * slot is FRESH and PAST INITIAL is a live survivor of
					 * the running cluster (cold co-booters publish INITIAL —
					 * A-semantics; a formed node publishes the live epoch).
					 * Its EXACT incarnation is recovered, in order of
					 * evidence strength:
					 *   1. the formation marker the arbiter durably wrote
					 *      into EVERY member's region-7 slot (the full
					 *      per-member incarnation table, majority readback)
					 *      — the B′ cold-formation case (Spec-5.15
					 *      INV-J7/J8: an admitted floor must come from
					 *      majority-durable commit evidence);
					 *   2. otherwise (a cluster formed by the INITIAL
					 *      epoch bootstrap writes NO marker), the peer's
					 *      own fresh voting slot: a LIVE survivor has not
					 *      restarted, so its slot incarnation IS its
					 *      admitted floor — mirroring its already-durable
					 *      MEMBER state, not admitting it anew (the slot
					 *      is qvotec-CRC-checked on a majority of disks;
					 *      the peer's MEMBER state was itself established
					 *      by majority-durable founding evidence).
					 * D13 stays strict in both cases:
					 * record_admitted(exact) -> exact-equality check ->
					 * only then MEMBER.  This node's OWN incarnation is
					 * never recovered from the marker (boundary: a new
					 * postmaster incarnation does not inherit the old
					 * formation incarnation) — self still rejoins through
					 * the ordinary JCMK chain.  Without this arm a
					 * restarting node (t/274 L4/L5 clean restart of the
					 * peer / t243 L4 kill -9 + restart) can never prove
					 * the coordinator's JOIN_COMMITTED publish
					 * (publish-proof counts only MEMBER survivors) and
					 * either times out 53R61 or wedges phase 3.
					 */
					uint64 marker_inc = pg_atomic_read_u64(
						&ReconfigShmem->observed_formation_marker_incarnation[i]);
					uint64 survivor_inc;
					uint64 survivor_gen = 0;

					if (marker_inc != 0)
						survivor_inc = marker_inc;
					else {
						(void)cluster_reconfig_get_observed_slot(i, &survivor_inc, &survivor_gen);
						if (survivor_gen == 0 || survivor_inc == 0)
							continue; /* no durable identity yet: retry */
					}
					cluster_membership_record_admitted(i, survivor_inc);
					if (cluster_membership_get_last_admitted_incarnation(i) != survivor_inc)
						continue; /* floor did not land exactly — no MEMBER */
				} else {
					/* Cold-formation path: admission is marker-driven and
					 * happens for the whole co-boot set at once (the state
					 * machine below).  This peer waits for the marker. */
					continue;
				}
				cluster_membership_set_state(i, CLUSTER_MEMBER_MEMBER);
				/* RF-ROOT P6 (STOP-01 contract): the bootstrap
				 * re-admission re-proves the node present (founding
				 * quorum vetting + fresh slot), so the CL-I13
				 * clean-departed mask no longer applies — clear it here
				 * (inline: we already hold the reconfig EXCLUSIVE lock;
				 * the out-of-line clearer re-acquires it).  The join-
				 * commit clear remains the readmission-path twin.
				 * Without this, a re-joined node's LATER real crash is
				 * masked (its fail-stop suppressed) and the survivor
				 * wedges:  observed t243 L4 (kill -9 after a clean-
				 * departed rejoin). */
				if (clean_departed_test_bit_locked(ReconfigShmem->clean_departed_bitmap, i)
					|| ReconfigShmem->clean_departed_epoch[i] != 0)
					cluster_write_fence_authority_cache_invalidate();
				ReconfigShmem->clean_departed_bitmap[i / 8] &= (uint8) ~(1u << (i % 8));
				ReconfigShmem->clean_departed_epoch[i] = 0;
			} else if (runtime_join_allowed && i == root_gated_join_node
					   && ms == CLUSTER_MEMBER_DEAD
					   && cluster_cssd_get_peer_state(i) == CLUSTER_CSSD_PEER_ALIVE) {
				/*
				 * spec-5.16 (3-node join participation) — survivor-side runtime
				 * readmit.  A DEAD peer whose durable COMMITTED join marker (observed
				 * by qvotec) carries a fresh admitted incarnation is a node the
				 * coordinator just rejoined.  Recognize it as MEMBER here too (the
				 * coordinator did so in commit_member), and remember it so an
				 * observer JOIN_COMMITTED event is published below — making THIS
				 * survivor's GRD FSM join the re-declare barrier.  record_admitted
				 * floors the incarnation (INV-J1, monotonic).
				 */
				uint64 obs_inc = 0;
				uint64 obs_epoch = 0;

				if (cluster_reconfig_get_observed_committed_join(i, &obs_inc, &obs_epoch)
					&& obs_inc == root_gated_join_incarnation
					&& obs_inc > cluster_membership_get_last_admitted_incarnation(i)) {
					cluster_membership_set_state(i, CLUSTER_MEMBER_MEMBER);
					cluster_membership_record_admitted(i, obs_inc);
					ReconfigShmem->fast_rejoin_incarnation[i] = obs_inc;
					ReconfigShmem->fast_rejoin_bitmap[i / 8] &= (uint8) ~(1u << (i % 8));
					dead_bitmap_set_bit(newly_joined, i);
					any_joined = true;
				}
			}
			/* else DEAD stays DEAD (no auto-readmit); JOINING/MEMBER unchanged */
		}

		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			if (cluster_conf_lookup_node(i) == NULL)
				continue;
			if (cluster_membership_is_member(i))
				dead_bitmap_set_bit(alive_set, i);
		}

		/*
		 * effective_dead = cssd_dead & ~clean_departed_bitmap & ~removed_bitmap.
		 * A permanently-removed node (spec-5.18, INV-LF1) is masked out so its
		 * subsequent CSSD DEAD/ALIVE never re-triggers a reconfig nor passively
		 * re-admits it — it is no longer a member.
		 */
		for (b = 0; b < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; b++) {
			/* STOP-02 §17.6: an older excluded origin remains fenced even if
			 * CSSD sees its restarted process ALIVE.  Only the individually
			 * ROOT-gated JOIN above may remove its bit. */
			dead_bitmap[b] &= (uint8) ~(ReconfigShmem->clean_departed_bitmap[b]
										| ReconfigShmem->removed_bitmap[b]);
			if (dead_bitmap[b] != 0
				&& cssd_dead_generation != ReconfigShmem->last_applied.cssd_dead_generation)
				failure_generation_changed = true;
			new_failure_bitmap[b]
				= dead_bitmap[b] & (uint8)~ReconfigShmem->last_applied.dead_bitmap[b];
			dead_bitmap[b] |= ReconfigShmem->last_applied.dead_bitmap[b];
			dead_bitmap[b] &= (uint8)~newly_joined[b];
		}
		if (any_joined) {
			memcpy(join_remaining_dead, ReconfigShmem->last_applied.dead_bitmap,
				   sizeof(join_remaining_dead));
			join_remaining_dead[root_gated_join_node / 8]
				&= (uint8) ~(1u << (root_gated_join_node % 8));
		}

		/*
		 * RF-ROOT P9 verification / cold-formation cold-formation ruling:
		 * past the INITIAL epoch with an ABSENT declared peer, drive the
		 * 5.22 cold-formation state machine (observation window -> arbiter
		 * formation marker -> marker admission).  Runs under this
		 * EXCLUSIVE lock: the admission mutates the membership table, and
		 * the state machine itself only does lock-free shmem reads plus
		 * the mailbox submit (no blocking).  No-op once every declared
		 * peer is formed.
		 */
		if (cluster_epoch_get_current() > CLUSTER_EPOCH_INITIAL) {
			bool any_absent = false;

			for (i = 0; i < CLUSTER_MAX_NODES; i++) {
				if (cluster_conf_lookup_node(i) == NULL)
					continue;
				if (cluster_membership_get_state(i) == CLUSTER_MEMBER_ABSENT) {
					any_absent = true;
					break;
				}
			}
			if (any_absent)
				cluster_reconfig_cold_formation_tick();
		}

		LWLockRelease(&ReconfigShmem->lock);
		cluster_reconfig_membership_floor_diagnostic("after-membership-reconcile");

		/*
		 * spec-5.16 (3-node join participation) — publish an observer-role
		 * JOIN_COMMITTED event for peers this survivor just recognized as MEMBER,
		 * so its GRD FSM runs the JOIN remaster episode (re-declares its held
		 * joiner-home blocks to the joiner + announces REDECLARE_DONE).  Done AFTER
		 * the lock release (publish_event takes the lock).  Only survivors reach
		 * here — on the coordinator the peer is already MEMBER, so the readmit
		 * branch never fires.  Mirrors the observer-role FAIL_STOP publish in the
		 * leave dispatch below;  clean_departed is cleared here (INV-J10) like
		 * commit_member.
		 */
		if (any_joined) {
			ReconfigEvent jevt;
			uint64 incs[CLUSTER_MAX_NODES];
			int jn;

			memset(incs, 0, sizeof(incs));
			for (jn = 0; jn < CLUSTER_MAX_NODES; jn++) {
				if (dead_bitmap_test_bit(newly_joined, jn)) {
					incs[jn] = root_gated_join_incarnation;
					if (cluster_reconfig_is_clean_departed(jn))
						cluster_reconfig_clear_clean_departed(jn);
				}
			}

			memset(&jevt, 0, sizeof(jevt));
			jevt.event_id = cluster_reconfig_compute_event_id_v2(
				RECONFIG_KIND_JOIN_COMMITTED, join_remaining_dead, newly_joined, incs,
				cluster_cssd_get_dead_generation());
			jevt.coordinator_node_id = self_id; /* observer; informational */
			jevt.old_epoch = cluster_epoch_get_current();
			/*
			 * AD-023 A2 §9.2.2: the JOIN_COMMITTED epoch is the exact
			 * committed epoch of the single gated target; the incarnation
			 * is re-verified against the gated value on the second read.
			 * An inconsistent or zero observation fails closed (epoch
			 * unchanged) -- never a max merge across unrelated
			 * observations.
			 */
			jevt.new_epoch = jevt.old_epoch;
			for (jn = 0; jn < CLUSTER_MAX_NODES; jn++) {
				uint64 jinc;
				uint64 jepoch;

				if (!dead_bitmap_test_bit(newly_joined, jn))
					continue;
				if (cluster_reconfig_get_observed_committed_join(jn, &jinc, &jepoch)
					&& jinc == root_gated_join_incarnation && jepoch != 0)
					jevt.new_epoch = jepoch;
				else
					jevt.new_epoch = jevt.old_epoch;
			}
			memcpy(jevt.dead_bitmap, join_remaining_dead, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
			memcpy(jevt.join_bitmap, newly_joined, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
			jevt.applied_at = GetCurrentTimestamp();
			jevt.observer_role = CLUSTER_RECONFIG_OBSERVER_SURVIVOR;
			jevt.cssd_dead_generation = cluster_cssd_get_dead_generation();
			jevt.reconfig_kind = RECONFIG_KIND_JOIN_COMMITTED;
			cluster_reconfig_publish_event(&jevt);
		}
	} else {
		memcpy(new_failure_bitmap, dead_bitmap, sizeof(new_failure_bitmap));
		dead_bitmap_set_bit(alive_set, self_id);
		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			if (i == self_id)
				continue;
			if (cluster_conf_lookup_node(i) == NULL)
				continue;
			if (!dead_bitmap_test_bit(dead_bitmap, i))
				dead_bitmap_set_bit(alive_set, i);
		}
	}

	/*
	 * survivor_set / coordinator = the min MEMBER survivor (INV-J8; alive_set is
	 * the MEMBER set built above).  Computed before the death/join dispatch since
	 * both directions use it.
	 */
	coordinator = dead_bitmap_lowest_bit_set(alive_set);
	if (coordinator < 0) {
		cluster_reconfig_external_rejoin_release_all();
		return; /* total cluster failure;fail-closed already via QVOTEC */
	}

	/*
	 * §3.5 ordering: handle the leave/death edge FIRST (it shrinks the MEMBER set,
	 * stabilizing the survivor base), THEN the join edge.  Each is an independent
	 * ReconfigEvent; neither early-returns past the other.
	 */
	if (!failstop_stage_handled
		&& (!dead_bitmap_is_zero(new_failure_bitmap) || failure_generation_changed)) {
		CLUSTER_INJECTION_POINT("cluster-reconfig-decide-coordinator");

		/* §3.2 P1.2: event_id from dead_bitmap + dead_generation snapshot. */
		event_id = cluster_reconfig_compute_event_id(dead_bitmap, cssd_dead_generation);

		/* Dedup against last_applied.  Same dead_bitmap within one DEAD episode →
		 * same dead_gen → same event_id → skip.  Rejoin-then-redeath bumps
		 * dead_gen → different event_id → re-fire.
		 * Read the field directly: we already hold the reconfig lock EXCLUSIVE
		 * in this section, and get_last_event_id() re-acquires SHARED, which
		 * self-deadlocks this single-threaded LMON tick (found by t/243). */
		if (event_id == ReconfigShmem->last_applied.event_id) {
			if (ReconfigShmem != NULL)
				pg_atomic_fetch_add_u64(&ReconfigShmem->dedup_skip_counter, 1);
		} else {
			/*
			 * P1.3 (b) + I7: ONLY the deterministic coordinator advances epoch +
			 * publishes coordinator-role; non-coordinator survivors publish
			 * observer-role for local observability.  spec-5.14 ordering: publish
			 * last_applied BEFORE broadcasting the PROCSIG (the read-side touched
			 * abort reads reconfig_kind + dead_bitmap from last_applied).
			 */
			if (self_id == coordinator) {
				cluster_reconfig_apply_epoch_bump_as_coordinator(dead_bitmap, coordinator,
																 cssd_dead_generation);
			} else {
				ReconfigEvent evt;

				memset(&evt, 0, sizeof(evt));
				evt.event_id = event_id;
				evt.coordinator_node_id = coordinator;
				evt.old_epoch = cluster_epoch_get_current();
				evt.new_epoch = evt.old_epoch; /* survivor not yet observed via piggyback */
				memcpy(evt.dead_bitmap, dead_bitmap, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
				evt.applied_at = GetCurrentTimestamp();
				evt.observer_role = CLUSTER_RECONFIG_OBSERVER_SURVIVOR;
				evt.cssd_dead_generation = cssd_dead_generation;
				evt.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
				cluster_reconfig_publish_event(&evt);
			}

			/*
			 * P1.3 (a) + I7: EVERY in_quorum survivor broadcasts PROCSIG — AFTER
			 * publish (spec-5.14 ordering), and ONLY if the event was actually
			 * published (review P1-A: a coordinator fence-marker fail-close does
			 * not publish; the next tick re-fires).
			 */
			/* Direct read again — the reconfig lock is held EXCLUSIVE here. */
			if (ReconfigShmem->last_applied.event_id == event_id)
				cluster_reconfig_broadcast_local_procsig();
		}
	}


	/*
	 * §3.5 join edge (spec-5.15 D4): online declared-node readmission, driven by
	 * the coordinator.  Runs whether or not there was a death this tick; gated by
	 * online_join (off = no runtime readmit, §3.4 fail-closed-safe via INV-J8).
	 */
	/*
	 * Hardening v1.0.4 (P2 serialization, one membership reconfig at a time): do NOT
	 * drive any node's join while a clean leave is active on this node (leaver or
	 * survivor).  A join Phase-1 / commit bumps the membership epoch with dead_gen
	 * unchanged, which the leaving node mis-observes as its own clean-leave commit
	 * and wedges in BARRIER_WAIT (the leave's epoch-observe premise holds ONLY under
	 * no concurrent dead_gen-unchanged reconfig).  Joins are LMON-driven and simply
	 * retry next tick once the leave finishes; the leave side symmetrically refuses
	 * to start while a join is pending.
	 */
	if (runtime_join_allowed && (ordinary_actions_allowed || fast_rejoin_control_actions)
		&& self_id == coordinator && !cluster_clean_leave_in_progress()
		&& (cluster_online_join
			|| (dead_bitmap_is_zero(new_failure_bitmap) && !failure_generation_changed))) {
		if (external_rejoin_active)
			cluster_reconfig_external_rejoin_tick();
		cluster_reconfig_drive_joins(
			coordinator, fast_rejoin_control_actions ? fast_rejoin_control_target : -1,
			fast_rejoin_control_actions ? fast_rejoin_control_incarnation : 0);
	} else {
		cluster_reconfig_external_rejoin_release_all();
		if (join_commit_stage.external_rejoin_consumed)
			cluster_reconfig_release_join_commit_stage();
	}
}


/*
 * Step 2 D2 — cluster_reconfig_broadcast_local_procsig.
 *
 *	  P1.3 (a) + I7:  every in_quorum survivor calls this on a fresh
 *	  dead_bitmap event_id.  Walks ProcArray (1..MaxBackends) and
 *	  SendProcSignal(PROCSIG_CLUSTER_RECONFIG_START) to every live
 *	  backend's pid.  Pattern mirrors cluster_fence_broadcast_freeze
 *	  (spec-2.28 D5):  no lock held during SendProcSignal, ProcArray
 *	  snapshot read is safe-stale.
 */
void
cluster_reconfig_broadcast_local_procsig(void)
{
	int beid;
	int signaled = 0;
	pid_t self_pid = MyProcPid;

	if (!cluster_enabled)
		return;
	if (ReconfigShmem == NULL)
		return;

	CLUSTER_INJECTION_POINT("cluster-reconfig-broadcast-procsig-pre");

	for (beid = 1; beid <= MaxBackends; beid++) {
		PGPROC *proc = BackendIdGetProc((BackendId)beid);
		pid_t pid;

		if (proc == NULL)
			continue;
		pid = proc->pid;
		if (pid == 0 || pid == self_pid)
			continue; /* skip LMON self */
		(void)SendProcSignal(pid, PROCSIG_CLUSTER_RECONFIG_START, (BackendId)beid);
		signaled++;
	}

	pg_atomic_fetch_add_u64(&ReconfigShmem->procsig_broadcast_count, 1);

	elog(DEBUG1, "cluster_reconfig: broadcast PROCSIG_CLUSTER_RECONFIG_START to %d backend(s)",
		 signaled);
}


/*
 * Step 2 D2 — cluster_reconfig_apply_epoch_bump_as_coordinator.
 *
 *	  P1.3 (b):  only the deterministic coordinator (min(survivor)) calls
 *	  this.  Atomically advances epoch via D18 cluster_epoch_advance_
 *	  for_reconfig, stamps the WAL insert LSN, publishes a coordinator-
 *	  role ReconfigEvent.  IC envelope piggyback (spec-2.4 + D20 receive
 *	  path observe) propagates the new epoch to non-coord survivors.
 */
void
cluster_reconfig_apply_epoch_bump_as_coordinator(
	const uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES], int32 coordinator_node_id,
	uint64 cssd_dead_generation)
{
	uint64 old_epoch, new_epoch;
	XLogRecPtr lsn;
	ReconfigEvent evt;
	ReconfigEvent previous;
	uint8 full_dead[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int b;

	if (!cluster_enabled)
		return;
	memcpy(full_dead, dead_bitmap, sizeof(full_dead));
	if (ReconfigShmem != NULL) {
		memset(&previous, 0, sizeof(previous));
		cluster_reconfig_get_last_event(&previous);
		for (b = 0; b < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; b++)
			full_dead[b] |= previous.dead_bitmap[b];
	}

	CLUSTER_INJECTION_POINT("cluster-reconfig-epoch-bump-pre");

	/*
	 * spec-2.29a r2 t/274: mark the pre-bump→publish window BEFORE bumping the
	 * epoch, so a concurrent LMON GRD IDLE tick holds its WAIT_EPOCH baseline
	 * while this (possibly backend-context, synchronous) path bumps but has not
	 * yet published.  Cleared at every return below.  Harmless on the LMON /
	 * fence-off paths (they either hand off to the local staged flag or publish
	 * within this same call with no intervening tick).
	 */
	cluster_reconfig_set_prebump_sync_active(1);

	/* D18:  atomic CAS-loop increment.  Returns pre/post snapshots. */
	cluster_epoch_advance_for_reconfig(&old_epoch, &new_epoch);

	/* D18:  stamp the LSN at which epoch changed (for SRF observability
	 * + future WAL replay).  GetXLogInsertRecPtr is the next insert
	 * position;adequate for "approximately when" semantics. */
	lsn = GetXLogInsertRecPtr();
	cluster_epoch_set_changed_at_lsn((uint64)lsn);

	/*
	 * PGRAC: spec-2.34 D4 (HC95) — eager wake of GCS block-shipping
	 * outstanding slots.  Must run AFTER cluster_epoch_advance_for_reconfig
	 * + cluster_epoch_set_changed_at_lsn (so slot.request_epoch <
	 * new_epoch comparison is well-defined) and BEFORE
	 * cluster_reconfig_publish_event (so peer backends start retrying
	 * before the reconfig event broadcast hits them).  Callsite uniqueness
	 * enforced by DoD grep (spec-2.34 §7).
	 */
	cluster_gcs_block_on_epoch_advance_exact(new_epoch, full_dead);

	/*
	 * spec-2.39 D14:  reconfig RESET-all hook.  Triggers local SIResetAll
	 * via the SinvalBcast aux process + clears stale ack_wait entries so
	 * blocked enqueuers don't wait forever on a peer that just died /
	 * was added.  Local-only (each surviving node runs this for itself);
	 * cluster弹性收敛.
	 */
	cluster_sinval_reset_all_on_reconfig();

	/*
	 * spec-3.1 D7 (v0.4 N11):  flush cluster Undo TT status overlay on
	 * reconfig epoch bump.  Adopt the spec-2.39 D14 hardcoded-callsite
	 * pattern (linkdb has no register-based reconfig callback API).
	 *
	 * Why here:  old-epoch overlay entries become invalid when the
	 * cluster epoch advances (HC182);  a fresh epoch must start with a
	 * clean overlay to avoid stale-status leaks across reconfig.
	 * Generation bump inside flush_all means future readers naturally
	 * skip pre-flush entries even if the flush races with concurrent
	 * lookups (HC181 fail-closed).
	 *
	 * PG CLOG is intentionally NOT touched (feature-069 L176).
	 */
	cluster_tt_status_flush_all((uint32)new_epoch);

	memset(&evt, 0, sizeof(evt));
	evt.event_id = cluster_reconfig_compute_event_id(full_dead, cssd_dead_generation);
	evt.coordinator_node_id = coordinator_node_id;
	evt.old_epoch = old_epoch;
	evt.new_epoch = new_epoch;
	memcpy(evt.dead_bitmap, full_dead, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	evt.applied_at = GetCurrentTimestamp();
	evt.observer_role = CLUSTER_RECONFIG_OBSERVER_COORDINATOR;
	evt.cssd_dead_generation = cssd_dead_generation;
	/* spec-5.14 D3: CSSD DEAD edge → fail-stop (see survivor path note). */
	evt.reconfig_kind = RECONFIG_KIND_FAIL_STOP;

	/*
	 * spec-4.12 D4 (core 8.A order):  when the write fence is enforced, the durable
	 * fence marker MUST be on >= quorum-majority voting disks BEFORE we publish the
	 * coordinator event (publishing is what starts recovery on the survivors).  We
	 * hand the marker to qvotec (the sole voting-disk writer) and wait for a
	 * quorum-majority ack.  If the ack does not come (write failure / qvotec down /
	 * timeout) we FAIL CLOSED:  do NOT publish, do NOT start recovery.  The epoch is
	 * already bumped (a safe frozen/write-fenced state -- stale tokens no longer
	 * match), and the next LMON tick retries (last_event_id is only set by
	 * publish_event, so a failed submit re-fires rather than dedup-skipping).
	 *
	 * Skipped entirely when enforcement is off/dev so a non-fenced cluster pays no
	 * marker-write cost and reconfig behaves exactly as before (zero regression).
	 */
	if (cluster_write_fence_enforcement == CLUSTER_WRITE_FENCE_ENFORCE_ON) {
		ClusterFenceMarker marker;

		memset(&marker, 0, sizeof(marker));
		marker.magic = CLUSTER_FENCE_MARKER_MAGIC;
		marker.version = CLUSTER_FENCE_MARKER_VERSION;
		marker.fence_epoch = new_epoch;
		marker.fence_event_id = evt.event_id; /* identity only */
		marker.fence_generation = cssd_dead_generation;
		marker.issuer_node_id = coordinator_node_id;
		memcpy(marker.fenced_dead_bitmap, full_dead, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
		/*
		 * spec-5.18 INV-LF10: when there ARE permanently-removed nodes, every
		 * reconfig-issued fence marker must also carry them, so a later fail-stop
		 * fence (dead = {M}) does not drop a previously-removed node {N} from the
		 * authority — the fenced set is dead | removed (superset-monotone, removed
		 * only grows).  Guarded on removed_count so a cluster that never removed a
		 * node pays nothing and the fence marker is byte-identical to pre-5.18.
		 */
		if (cluster_reconfig_get_removed_count() > 0) {
			uint8 removed_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
			int b;

			cluster_reconfig_snapshot_removed_bitmap(removed_bitmap);
			for (b = 0; b < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; b++)
				marker.fenced_dead_bitmap[b] |= removed_bitmap[b];
		}

		/*
		 * The async stage is process-local by design and is driven only by LMON
		 * ticks.  Non-LMON backend callers would otherwise stage the marker in their
		 * own process and return with no LMON-visible pending publish record.  Keep
		 * those non-LMON paths on the old bounded wait: they are not
		 * transport-liveness actors, so this does not reintroduce the BUG-C1 LMON
		 * park.
		 */
		if (MyBackendType != B_LMON) {
			ClusterFenceMarkerSubmitResult result;

			result = cluster_write_fence_submit_marker(&marker);
			if (result != CLUSTER_FENCE_MARKER_SUBMIT_ACK) {
				ereport(LOG, (errmsg("cluster reconfig: fence marker did not reach a voting-disk "
									 "majority for epoch %llu; not publishing reconfig event "
									 "(write-fenced, will retry)",
									 (unsigned long long)new_epoch)));
				cluster_reconfig_set_prebump_sync_active(0);
				return;
			}

			cluster_reconfig_publish_event(&evt);
			cluster_reconfig_fast_rejoin_control_arm(&evt);
			cluster_reconfig_log_failstop_epoch_bump(&evt);
			cluster_reconfig_set_prebump_sync_active(0);
			return;
		}

		failstop_fence_stage.event = evt;
		failstop_fence_stage.marker = marker;
		failstop_fence_stage.node_id = coordinator_node_id;
		failstop_fence_stage.async.has_staged_event = true;
		(void)cluster_reconfig_submit_fence_stage(&failstop_fence_stage,
												  CLUSTER_MARKER_KIND_FENCE_FAILSTOP,
												  coordinator_node_id, GetCurrentTimestamp());
		/* LMON path: the local staged flag now covers the pending window. */
		cluster_reconfig_set_prebump_sync_active(0);
		return; /* fail-closed until the staged marker is majority-durable */
	}

	cluster_reconfig_publish_event(&evt);
	cluster_reconfig_fast_rejoin_control_arm(&evt);

	/*
	 * PGRAC: spec-6.14 D9 amend (F5) — unconditional operator evidence for a
	 * fail-stop membership epoch bump (rule 17: key state changes must be
	 * traceable).  Every pre-existing message on this path is traffic-driven
	 * (stale-epoch replies, GRD rebuild), so a bump on an idle survivor left
	 * no log line at all and harnesses had to poll SQL state instead.  A
	 * fixed stack buffer (worst case: 128 node ids x 4 chars) keeps the LMON
	 * tick free of allocator traffic.
	 */
	cluster_reconfig_log_failstop_epoch_bump(&evt);

	/* spec-2.29a r2 t/274: fence-off path published within this call. */
	cluster_reconfig_set_prebump_sync_active(0);
}


/*
 * spec-5.13 D3 — cluster_reconfig_apply_clean_leave_as_coordinator.
 *
 *	The commit point of the §3.1 two-phase clean-leave commit, run on the
 *	survivor coordinator (min node id, Q6-A).  Bumps the membership epoch and
 *	publishes a CLEAN_LEAVE reconfig event naming the leaving node in
 *	dead_bitmap, then records it clean-departed at the new epoch so the lmon_tick
 *	mask suppresses its later CSSD DEAD (CL-I13).  Mirrors
 *	apply_epoch_bump_as_coordinator's epoch-advance side effects (eager GCS wake
 *	+ sinval reset + TT overlay flush) so survivors invalidate stale leaving-node
 *	cache at epoch advance (CL-I5 happens-before).  No write-fence marker: the
 *	leaving node drained cooperatively (nothing to fence); the durable record is
 *	the §2.5 leave-intent marker, written by the driver BEFORE (COMMITTING) and
 *	AFTER (COMMITTED) this call.  PROCSIG broadcast is intentionally NOT done
 *	here — the touched drain-grace dispatch (CL-I12) + serve-gate are wired in
 *	spec-5.13 S6; the driver/LMON owns any survivor-side wake.  Returns the new
 *	epoch E (the driver stamps it into the COMMITTED marker).
 */
uint64
cluster_reconfig_apply_clean_leave_as_coordinator(int32 leaving_node_id, uint64 baseline_epoch)
{
	uint64 old_epoch, new_epoch;
	XLogRecPtr lsn;
	uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	uint64 cssd_dead_generation;
	ReconfigEvent evt;
	ReconfigEvent previous;
	int b;

	if (!cluster_enabled || ReconfigShmem == NULL)
		return 0;
	if (leaving_node_id < 0 || leaving_node_id >= CLUSTER_MAX_NODES)
		return 0;

	/* inject points for the clean-leave path are D12 (spec-5.13 S7). */

	dead_bitmap_set_bit(dead_bitmap, leaving_node_id);
	memset(&previous, 0, sizeof(previous));
	cluster_reconfig_get_last_event(&previous);
	for (b = 0; b < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; b++)
		dead_bitmap[b] |= previous.dead_bitmap[b];
	cssd_dead_generation = cluster_cssd_get_dead_generation();

	/*
	 * CL-I3 guarded advance: bump to baseline+1 ONLY if the epoch is still the
	 * baseline the leave committed against.  At >=3 nodes a third node's death
	 * could bump the epoch between the driver's pre-check and here; an
	 * unconditional CAS-loop would then stack the clean-leave on top of that
	 * death's view (a stale-baseline commit, CL-I3 violation).  The single
	 * compare_exchange fails closed in that case — return 0 so the driver does
	 * NOT write the COMMITTED marker; the leaving node observes the foreign
	 * event and escalates to fail-stop.  At 2 nodes this is exactly equivalent
	 * to the old unconditional bump (no third party can move the epoch).
	 */
	old_epoch = baseline_epoch;
	if (!cluster_epoch_advance_for_reconfig_if_baseline(baseline_epoch, &new_epoch))
		return 0;
	lsn = GetXLogInsertRecPtr();
	cluster_epoch_set_changed_at_lsn((uint64)lsn);

	/*
	 * Same epoch-advance side effects as the fail-stop coordinator path: wake
	 * GCS block-shipping slots, RESET sinval, flush the TT status overlay.  On
	 * each survivor these run when it observes the new epoch; on the coordinator
	 * they run here.  This is what invalidates stale leaving-node cache so the
	 * post-epoch storage read returns the just-flushed current (CL-I5).
	 */
	cluster_gcs_block_on_epoch_advance_exact(new_epoch, dead_bitmap);
	cluster_sinval_reset_all_on_reconfig();
	cluster_tt_status_flush_all((uint32)new_epoch);

	memset(&evt, 0, sizeof(evt));
	evt.event_id = cluster_reconfig_compute_event_id(dead_bitmap, cssd_dead_generation);
	evt.coordinator_node_id = cluster_node_id;
	evt.old_epoch = old_epoch;
	evt.new_epoch = new_epoch;
	memcpy(evt.dead_bitmap, dead_bitmap, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	evt.applied_at = GetCurrentTimestamp();
	evt.observer_role = CLUSTER_RECONFIG_OBSERVER_COORDINATOR;
	evt.cssd_dead_generation = cssd_dead_generation;
	evt.reconfig_kind = RECONFIG_KIND_CLEAN_LEAVE;
	cluster_reconfig_publish_event(&evt);

	/* CL-I13: record clean-departed at E so the lmon_tick mask suppresses the
	 * node's later CSSD DEAD (no epoch-floor raise — the epoch is live here). */
	cluster_reconfig_record_clean_departed(leaving_node_id, new_epoch, false);

	return new_epoch;
}


/*
 * spec-5.18 D3 — cluster_reconfig_apply_node_removed_as_coordinator.
 *
 *	The membership-shrink commit point of permanent removal (§3.1
 *	SHRINK_COMMITTING), run on the survivor coordinator AFTER the 4.12 fence
 *	marker for the removed node is majority-durable (INV-LF2, enforced by the
 *	driver).  Mirrors apply_clean_leave_as_coordinator's guarded epoch advance +
 *	epoch-advance side effects, but publishes a NODE_REMOVED event whose id folds
 *	removed_bitmap + removal_event_id (R14 — never deduped even when dead_bitmap is
 *	unchanged), then records the node removed (removed_bitmap + epoch, masking it
 *	out of effective_dead) and shrinks membership_state to REMOVED.  The published
	 *	event's dead_bitmap retains the previously applied DEAD set: the removed node
	 *	is a membership change, but a later baseline must not release unrelated
	 *	excluded origins.  Returns the
 *	new epoch, or 0 if the guarded advance lost (a real death intruded — the driver
 *	ABORTED_ESCALATEs, pre-SHRUNK).
 */
uint64
cluster_reconfig_apply_node_removed_as_coordinator(int32 removed_node_id, uint64 baseline_epoch,
												   uint64 removal_event_id, uint64 last_incarnation,
												   bool *out_contest)
{
	uint64 old_epoch, new_epoch;
	XLogRecPtr lsn;
	uint8 current_dead[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint8 removed_with_n[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint8 excluded_with_n[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint64 cssd_dead_generation;
	ReconfigEvent evt;
	int b;

	/*
	 * *out_contest distinguishes the two zero-returns for the driver (P1-A): a lost
	 * guarded-advance means ANOTHER node moved the epoch (a real contest -> the
	 * driver classifies escalate vs cleanup-blocked); a fence-submit failure is a
	 * transient self-bumped retry (NOT a contest -> the driver just retries).
	 */
	if (out_contest != NULL)
		*out_contest = false;

	if (!cluster_enabled || ReconfigShmem == NULL)
		return 0;
	if (removed_node_id < 0 || removed_node_id >= CLUSTER_MAX_NODES)
		return 0;
	if (node_removed_fence_stage.async.has_staged_event)
		return cluster_reconfig_poll_node_removed_fence_stage(removed_node_id, removal_event_id,
															  last_incarnation);

	CLUSTER_INJECTION_POINT("cluster-node-remove-shrink-committing");

	cssd_dead_generation = cluster_cssd_get_dead_generation();

	/* Freeze the exact excluded set before the epoch callback.  Resource-X D2
	 * consumes it during that callback, so the removed node must be present
	 * even when a prior clean leave kept it out of last_applied.dead_bitmap. */
	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	memcpy(current_dead, ReconfigShmem->last_applied.dead_bitmap, sizeof(current_dead));
	memcpy(removed_with_n, ReconfigShmem->removed_bitmap, sizeof(removed_with_n));
	for (b = 0; b < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; b++)
		excluded_with_n[b]
			= ReconfigShmem->last_applied.dead_bitmap[b] | ReconfigShmem->removed_bitmap[b];
	LWLockRelease(&ReconfigShmem->lock);
	removed_with_n[removed_node_id / 8] |= (uint8)(1u << (removed_node_id % 8));
	excluded_with_n[removed_node_id / 8] |= (uint8)(1u << (removed_node_id % 8));

	/* CL-I3-style guarded advance: fail closed if a real death moved the epoch. */
	old_epoch = baseline_epoch;
	if (!cluster_epoch_advance_for_reconfig_if_baseline(baseline_epoch, &new_epoch)) {
		if (out_contest != NULL)
			*out_contest = true; /* another node moved the epoch -> real contest */
		return 0;
	}
	lsn = GetXLogInsertRecPtr();
	cluster_epoch_set_changed_at_lsn((uint64)lsn);

	/* same epoch-advance side effects as the other coordinator paths (cache
	 * invalidation happens-before): wake GCS slots, reset sinval, flush TT overlay. */
	cluster_gcs_block_on_epoch_advance_exact(new_epoch, excluded_with_n);
	cluster_sinval_reset_all_on_reconfig();
	cluster_tt_status_flush_all((uint32)new_epoch);

	/* R14: event_id folds the removed set (current removed_bitmap | {N}) + the
	 * per-attempt removal_event_id, so a clean-left removal (dead_bitmap unchanged)
	 * still produces a distinct, non-deduped id. */
	/* Frozen §17.6 marker-producer rule: take the currently applied DEAD set
	 * and durable REMOVED set from one reconfig-lock snapshot, then add this
	 * not-yet-applied removal delta.  A NODE_REMOVED marker is an authority
	 * image for the full excluded set, not merely the event-local delta. */
	/*
	 * INV-LF2 (fence-before-shrink): arm the 4.12 write fence for the removed node
	 * BEFORE publishing the membership shrink — exactly like the fail-stop coordinator
	 * submits its fence before publishing.  The marker is at NEW epoch with the
	 * removed node in the fenced set, so the lower-epoch steady-state baseline is
	 * stale-guarded and cannot drop it in the arm->publish window.  Fail-closed: if
	 * the marker does not reach a voting-disk majority, do NOT publish / shrink (the
	 * epoch is bumped = a safe frozen state; the driver retries).  Skipped when
	 * enforcement is off (single-node / non-fenced cluster pays nothing).
	 */
	if (cluster_write_fence_enforcement == CLUSTER_WRITE_FENCE_ENFORCE_ON) {
		ClusterFenceMarker marker;

		memset(&marker, 0, sizeof(marker));
		marker.magic = CLUSTER_FENCE_MARKER_MAGIC;
		marker.version = CLUSTER_FENCE_MARKER_VERSION;
		marker.fence_epoch = new_epoch;
		marker.fence_event_id
			= cluster_reconfig_compute_removal_event_id(removed_with_n, removal_event_id);
		marker.fence_generation = cssd_dead_generation;
		marker.issuer_node_id = cluster_node_id;
		marker.marker_kind = CLUSTER_FENCE_MARKER_KIND_NODE_REMOVED;
		memcpy(marker.fenced_dead_bitmap, excluded_with_n, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);

		node_removed_fence_stage.event.event_id
			= cluster_reconfig_compute_removal_event_id(removed_with_n, removal_event_id);
		node_removed_fence_stage.event.coordinator_node_id = cluster_node_id;
		node_removed_fence_stage.event.old_epoch = old_epoch;
		node_removed_fence_stage.event.new_epoch = new_epoch;
		memcpy(node_removed_fence_stage.event.dead_bitmap, current_dead,
			   CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
		node_removed_fence_stage.event.applied_at = GetCurrentTimestamp();
		node_removed_fence_stage.event.observer_role = CLUSTER_RECONFIG_OBSERVER_COORDINATOR;
		node_removed_fence_stage.event.cssd_dead_generation = cssd_dead_generation;
		node_removed_fence_stage.event.reconfig_kind = RECONFIG_KIND_NODE_REMOVED;
		node_removed_fence_stage.marker = marker;
		node_removed_fence_stage.node_id = removed_node_id;
		node_removed_fence_stage.last_incarnation = last_incarnation;
		node_removed_fence_stage.removal_event_id = removal_event_id;
		node_removed_fence_stage.async.has_staged_event = true;
		(void)cluster_reconfig_poll_node_removed_fence_stage(removed_node_id, removal_event_id,
															 last_incarnation);
		return 0; /* fail-closed until the staged fence marker ACKs */
	}

	CLUSTER_INJECTION_POINT("cluster-node-remove-fence-armed");

	memset(&evt, 0, sizeof(evt));
	evt.event_id = cluster_reconfig_compute_removal_event_id(removed_with_n, removal_event_id);
	evt.coordinator_node_id = cluster_node_id;
	evt.old_epoch = old_epoch;
	evt.new_epoch = new_epoch;
	memcpy(evt.dead_bitmap, current_dead, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	evt.applied_at = GetCurrentTimestamp();
	evt.observer_role = CLUSTER_RECONFIG_OBSERVER_COORDINATOR;
	evt.cssd_dead_generation = cssd_dead_generation;
	evt.reconfig_kind = RECONFIG_KIND_NODE_REMOVED;
	cluster_reconfig_publish_event(&evt);

	/* durable removed set (masks the node out of effective_dead, INV-LF1) + the
	 * member-set shrink (membership_state -> REMOVED), pinning the incarnation floor. */
	cluster_reconfig_record_removed(removed_node_id, new_epoch, false);
	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	cluster_membership_shrink_to_removed(removed_node_id, last_incarnation);
	LWLockRelease(&ReconfigShmem->lock);

	return new_epoch;
}


/* ============================================================
 * spec-5.15 D4 — §2.6 join-commit-marker qvotec handshake.
 *
 *	Mirrors the spec-4.12 fence / spec-5.13 leave mailbox: the coordinator stages
 *	a marker for the joiner, wakes qvotec, and blocks (bounded) until qvotec — the
 *	sole voting-disk writer — has written it to the joiner's region-3 slot on a
 *	quorum-majority of disks.  Unlike the leave marker (written to the writer's
 *	OWN slot), the join marker is written to the JOINER's slot (so each joiner's
 *	admit record persists independently of how many joins the coordinator drives).
 * ============================================================
 */

static uint64 join_qvotec_inflight_marker_seq = 0;
static uint64 join_qvotec_last_processed_marker_seq = 0;
static ClusterJoinMarkerMailboxOperationV1 join_qvotec_inflight_marker_operation
	= CLUSTER_JOIN_MARKER_MAILBOX_WRITE_EXACT;
static bool join_qvotec_marker_inflight = false;

bool
cluster_reconfig_qvotec_lifecycle_transition(ClusterQvotecMailbox *authority_mailbox,
											 pg_atomic_uint32 *qvotec_status,
											 ClusterQvotecStatus next_status)
{
	bool invalidate;

	if (ReconfigShmem == NULL || authority_mailbox == NULL || qvotec_status == NULL
		|| (next_status != CLUSTER_QVOTEC_STARTING && next_status != CLUSTER_QVOTEC_READY
			&& next_status != CLUSTER_QVOTEC_SHUTTING_DOWN && next_status != CLUSTER_QVOTEC_DOWN))
		return false;
	invalidate
		= next_status == CLUSTER_QVOTEC_STARTING || next_status == CLUSTER_QVOTEC_SHUTTING_DOWN;

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	pg_atomic_write_u32(qvotec_status, (uint32)next_status);
	if (invalidate) {
		cluster_qvotec_mailbox_restart_reset(authority_mailbox);
		pg_atomic_write_u32(&ReconfigShmem->join_marker_result, CLUSTER_JOIN_MARKER_SUBMIT_FAILED);
		pg_atomic_write_u64(&ReconfigShmem->join_marker_completion_seq, 0);
		pg_write_barrier();
		join_qvotec_inflight_marker_seq = 0;
		join_qvotec_last_processed_marker_seq = 0;
		join_qvotec_inflight_marker_operation = CLUSTER_JOIN_MARKER_MAILBOX_WRITE_EXACT;
		join_qvotec_marker_inflight = false;
	}
	LWLockRelease(&ReconfigShmem->lock);
	if (invalidate)
		cluster_lmon_marker_complete_wakeup();
	return true;
}

static bool
cluster_reconfig_join_marker_request_word_encode(ClusterJoinMarkerMailboxOperationV1 operation,
												 int32 target_node, uint32 *word_out)
{
	uint32 word;

	if (word_out == NULL || target_node < 0 || target_node >= CLUSTER_MAX_NODES
		|| (operation != CLUSTER_JOIN_MARKER_MAILBOX_WRITE_EXACT
			&& operation != CLUSTER_JOIN_MARKER_MAILBOX_VERIFY_COMMITTED_CLOSED))
		return false;
	word = (uint32)target_node;
	if (operation == CLUSTER_JOIN_MARKER_MAILBOX_VERIFY_COMMITTED_CLOSED)
		word |= CLUSTER_JOIN_MARKER_REQUEST_VERIFY_COMMITTED_CLOSED;
	*word_out = word;
	return true;
}

static bool
cluster_reconfig_join_marker_request_word_decode(uint32 word,
												 ClusterJoinMarkerMailboxOperationV1 *operation_out,
												 int32 *target_node_out)
{
	uint32 target;

	if (operation_out == NULL || target_node_out == NULL
		|| (word & CLUSTER_JOIN_MARKER_REQUEST_RESERVED_MASK) != 0)
		return false;
	target = word & CLUSTER_JOIN_MARKER_REQUEST_TARGET_MASK;
	if (target >= CLUSTER_MAX_NODES)
		return false;
	*operation_out = (word & CLUSTER_JOIN_MARKER_REQUEST_VERIFY_COMMITTED_CLOSED) != 0
						 ? CLUSTER_JOIN_MARKER_MAILBOX_VERIFY_COMMITTED_CLOSED
						 : CLUSTER_JOIN_MARKER_MAILBOX_WRITE_EXACT;
	*target_node_out = (int32)target;
	return true;
}

/*
 * Stage one already-encoded region-3 marker image.  The embedded marker
 * version selects the only two supported exact lengths; no discriminator is
 * added to shared memory or to the disk image.  The request-sequence publish
 * remains the caller's commit point.
 */
static bool
cluster_reconfig_stage_join_marker_locked(int32 target_node,
										  ClusterJoinMarkerMailboxOperationV1 operation,
										  uint32 version, const void *image, Size image_len)
{
	Size expected_len;
	uint32 request_word;

	if (ReconfigShmem == NULL
		|| !cluster_reconfig_join_marker_request_word_encode(operation, target_node, &request_word))
		return false;

	if (operation == CLUSTER_JOIN_MARKER_MAILBOX_VERIFY_COMMITTED_CLOSED) {
		if (image != NULL || image_len != 0 || version != 0)
			return false;
		ReconfigShmem->join_marker_request_word = request_word;
		memset(ReconfigShmem->join_pending_marker, 0, sizeof(ReconfigShmem->join_pending_marker));
		return true;
	}
	if (image == NULL)
		return false;

	switch (version) {
	case CLUSTER_JCMK_VERSION:
		expected_len = sizeof(ClusterJoinCommitMarker);
		break;
	case CLUSTER_JCMK_REPLACEMENT_VERSION:
		expected_len = CLUSTER_JCMK_REPLACEMENT_BYTES;
		break;
	default:
		return false;
	}
	if (image_len != expected_len)
		return false;

	ReconfigShmem->join_marker_request_word = request_word;
	memset(ReconfigShmem->join_pending_marker, 0, sizeof(ReconfigShmem->join_pending_marker));
	memcpy(ReconfigShmem->join_pending_marker, image, image_len);
	return true;
}

static void
cluster_reconfig_release_ready_stage(void)
{
	if (join_marker_lmon_owner.reserved
		&& join_marker_lmon_owner.purpose == CLUSTER_JOIN_MARKER_LMON_READY_SERIALIZE)
		memset(&join_marker_lmon_owner, 0, sizeof(join_marker_lmon_owner));
	memset(&replacement_ready_stage, 0, sizeof(replacement_ready_stage));
	cluster_marker_async_init(&replacement_ready_stage.marker_async);
}

static bool
cluster_reconfig_lmon_submit_ready_observer_pair(TimestampTz now)
{
	static const uint8 zero_value[CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES] = { 0 };
	ClusterReplacementEpisode capability_episode;
	ClusterReplacementEpisode *episode;
	ClusterQvotecMailboxSubmitStatus authority_status;
	struct Latch *qlatch;
	bool marker_submitted;
	int wait_ms;

	if (!cluster_reconfig_replacement_candidate2_capabilities_current(&capability_episode))
		return false;
	wait_ms = cluster_quorum_poll_interval_ms * 3 + 2000;
	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	episode = &ReconfigShmem->replacement_episode;
	if (cluster_qvotec_get_status() != CLUSTER_QVOTEC_READY
		|| memcmp(episode, &capability_episode, sizeof(*episode)) != 0
		|| !cluster_replacement_episode_is_valid(episode)
		|| (episode->phase != CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED
			&& episode->phase != CLUSTER_REPLACEMENT_EPISODE_POST_EPOCH)
		|| (episode->readiness_flags & CLUSTER_REPLACEMENT_EPISODE_R4A_TARGET_READY) == 0
		|| episode->state_generation == 0 || episode->target_node_id != cluster_node_id
		|| episode->reserved_or_committed_epoch != cluster_epoch_get_current()
		|| ReconfigShmem->self_join_admitted != 0 || ReconfigShmem->self_join_failed != 0
		|| !cluster_reconfig_replacement_membership_current_locked(episode)
		|| cluster_membership_get_last_admitted_incarnation(episode->target_node_id)
			   != episode->old_admitted_incarnation
		|| join_marker_lmon_owner.reserved
		|| cluster_marker_async_mailbox_busy(&ReconfigShmem->join_marker_request_seq,
											 &ReconfigShmem->join_marker_completion_seq)) {
		LWLockRelease(&ReconfigShmem->lock);
		return false;
	}

	join_marker_lmon_owner.reserved = true;
	join_marker_lmon_owner.purpose = CLUSTER_JOIN_MARKER_LMON_READY_SERIALIZE;
	join_marker_lmon_owner.operation = CLUSTER_JOIN_MARKER_MAILBOX_VERIFY_COMMITTED_CLOSED;
	join_marker_lmon_owner.marker_request_seq = 0;
	authority_status
		= cluster_qvotec_authority_lmon_submit(CLUSTER_QVOTEC_MAILBOX_RECOVER_HEAD, zero_value,
											   &replacement_ready_stage.authority_request_seq);
	if (authority_status != CLUSTER_QVOTEC_MAILBOX_SUBMIT_ACCEPTED) {
		memset(&join_marker_lmon_owner, 0, sizeof(join_marker_lmon_owner));
		LWLockRelease(&ReconfigShmem->lock);
		return false;
	}
	if (!cluster_reconfig_stage_join_marker_locked(
			episode->target_node_id, CLUSTER_JOIN_MARKER_MAILBOX_VERIFY_COMMITTED_CLOSED, 0, NULL,
			0)) {
		memset(&join_marker_lmon_owner, 0, sizeof(join_marker_lmon_owner));
		replacement_ready_stage.phase = CLUSTER_REPLACEMENT_READY_STAGE_DRAIN_AUTHORITY;
		LWLockRelease(&ReconfigShmem->lock);
		return false;
	}
	marker_submitted = cluster_marker_async_submit(
		&replacement_ready_stage.marker_async, &ReconfigShmem->join_marker_request_seq,
		&ReconfigShmem->join_marker_completion_seq, NULL, now, (uint64)wait_ms * 1000ULL,
		CLUSTER_MARKER_KIND_REPLACEMENT_VERIFY_COMMITTED_CLOSED, episode->target_node_id);
	if (!marker_submitted) {
		memset(&join_marker_lmon_owner, 0, sizeof(join_marker_lmon_owner));
		replacement_ready_stage.phase = CLUSTER_REPLACEMENT_READY_STAGE_DRAIN_AUTHORITY;
		LWLockRelease(&ReconfigShmem->lock);
		return false;
	}
	replacement_ready_stage.marker_request_seq = replacement_ready_stage.marker_async.inflight_seq;
	join_marker_lmon_owner.marker_request_seq = replacement_ready_stage.marker_request_seq;
	replacement_ready_stage.marker_completed = false;
	replacement_ready_stage.phase = CLUSTER_REPLACEMENT_READY_STAGE_WAIT_PAIR;
	qlatch = ReconfigShmem->join_qvotec_latch;
	LWLockRelease(&ReconfigShmem->lock);
	if (qlatch != NULL)
		SetLatch(qlatch);
	return true;
}

static bool
cluster_reconfig_lmon_ready_cache_current(int32 *coordinator_node_id)
{
	ClusterQvotecMailboxCompletion completion;
	ClusterQvotecMailboxCompletion repeated_completion;
	ClusterEpochAuthorityValue head;
	ClusterEpochBallotId ballot;
	ClusterReplacementCommitMarkerV3 marker;
	ClusterReplacementEpisode capability_episode;
	ClusterReplacementEpisode *episode;
	ClusterJoinMarkerMailboxOperationV1 operation;
	uint32 request_word;
	int32 request_target;
	bool valid = false;

	if (coordinator_node_id == NULL
		|| replacement_ready_stage.phase != CLUSTER_REPLACEMENT_READY_STAGE_CACHED
		|| !cluster_reconfig_replacement_candidate2_capabilities_current(&capability_episode)) {
		cluster_reconfig_release_ready_stage();
		return false;
	}

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	episode = &ReconfigShmem->replacement_episode;
	if (join_marker_lmon_owner.reserved || cluster_qvotec_get_status() != CLUSTER_QVOTEC_READY
		|| memcmp(episode, &capability_episode, sizeof(*episode)) != 0
		|| !cluster_replacement_episode_is_valid(episode)
		|| (episode->phase != CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED
			&& episode->phase != CLUSTER_REPLACEMENT_EPISODE_POST_EPOCH)
		|| (episode->readiness_flags & CLUSTER_REPLACEMENT_EPISODE_R4A_TARGET_READY) == 0
		|| episode->target_node_id != cluster_node_id
		|| episode->target_node_id != replacement_ready_stage.cached_snapshot.target_node_id
		|| episode->state_generation
			   != replacement_ready_stage.cached_snapshot.episode_state_generation
		|| episode->request_nonce != replacement_ready_stage.cached_snapshot.request_nonce
		|| episode->old_admitted_incarnation
			   != replacement_ready_stage.cached_snapshot.old_admitted_incarnation
		|| episode->fresh_incarnation != replacement_ready_stage.cached_snapshot.fresh_incarnation
		|| episode->reserved_or_committed_epoch
			   != replacement_ready_stage.cached_snapshot.committed_epoch
		|| episode->grammar_fingerprint
			   != replacement_ready_stage.cached_snapshot.grammar_fingerprint
		|| episode->reserved_or_committed_epoch != cluster_epoch_get_current()
		|| ReconfigShmem->self_join_admitted != 0 || ReconfigShmem->self_join_failed != 0
		|| !cluster_reconfig_replacement_membership_current_locked(episode)
		|| cluster_membership_get_last_admitted_incarnation(episode->target_node_id)
			   != episode->old_admitted_incarnation
		|| !cluster_qvotec_authority_lmon_poll_completion(
			replacement_ready_stage.authority_request_seq, &completion)
		|| completion.result != CLUSTER_QVOTEC_MAILBOX_CHOSEN
		|| completion.actor_phase != CLUSTER_QVOTEC_ACTOR_RECOVER_SCAN_B
		|| !cluster_epoch_authority_value_decode(completion.completion_value,
												 CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, &head)
		|| !cluster_epoch_ballot_id_decode(completion.completion_ballot, &ballot)
		|| pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq)
			   != replacement_ready_stage.marker_request_seq
		|| pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq)
			   != replacement_ready_stage.marker_request_seq
		|| pg_atomic_read_u32(&ReconfigShmem->join_marker_result) != CLUSTER_JOIN_MARKER_SUBMIT_ACK)
		goto out;
	request_word = ReconfigShmem->join_marker_request_word;
	if (!cluster_reconfig_join_marker_request_word_decode(request_word, &operation, &request_target)
		|| operation != CLUSTER_JOIN_MARKER_MAILBOX_VERIFY_COMMITTED_CLOSED
		|| request_target != episode->target_node_id
		|| !cluster_replacement_marker_v3_decode(ReconfigShmem->join_pending_marker, request_target,
												 &marker)
		|| marker.generation != replacement_ready_stage.cached_snapshot.jcmk_generation
		|| !cluster_reconfig_terminal_closed_matches_episode(&head, &ballot, &marker, episode)
		|| !cluster_qvotec_authority_lmon_poll_completion(
			replacement_ready_stage.authority_request_seq, &repeated_completion)
		|| memcmp(&completion, &repeated_completion, sizeof(completion)) != 0
		|| pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq)
			   != replacement_ready_stage.marker_request_seq
		|| pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq)
			   != replacement_ready_stage.marker_request_seq
		|| pg_atomic_read_u32(&ReconfigShmem->join_marker_result) != CLUSTER_JOIN_MARKER_SUBMIT_ACK
		|| ReconfigShmem->join_marker_request_word != request_word
		|| cluster_qvotec_get_status() != CLUSTER_QVOTEC_READY)
		goto out;
	*coordinator_node_id = episode->coordinator_node_id;
	valid = true;

out:
	LWLockRelease(&ReconfigShmem->lock);
	if (!valid)
		cluster_reconfig_release_ready_stage();
	return valid;
}

static bool
cluster_reconfig_r4_membership_observations_current(
	ClusterR4MembershipSnapshot *candidate, bool freeze_generations,
	const ClusterSemanticActivationRecord *stop_record, const uint8 *stop_root)
{
	int node;

	if (candidate == NULL || cluster_qvotec_get_status() != CLUSTER_QVOTEC_READY
		|| !cluster_qvotec_in_quorum() || cluster_epoch_get_current() != candidate->formation_epoch
		|| cluster_qvotec_get_self_incarnation() != candidate->local_self_boot_incarnation)
		return false;
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		uint64 generation = 0;
		uint64 incarnation = 0;
		bool admitted = node < 64
							? (candidate->admitted_members_lo & (UINT64_C(1) << node)) != 0
							: (candidate->admitted_members_hi & (UINT64_C(1) << (node - 64))) != 0;

		if (!admitted)
			continue;
		if (cluster_conf_lookup_node(node) == NULL
			|| !cluster_reconfig_get_observed_slot(node, &incarnation, &generation)
			|| generation == 0 || incarnation != candidate->admitted_incarnation[node]
			|| (!cluster_reconfig_get_observed_fresh_alive(node)
				&& (stop_record == NULL || stop_root == NULL || node == cluster_node_id
					|| !cluster_normal_stop_peer_receipt_tail(stop_record, stop_root, node,
															  incarnation)))
			|| cluster_reconfig_get_observed_epoch(node) != candidate->formation_epoch)
			return false;
		if (freeze_generations)
			candidate->observed_generation[node] = generation;
		else if (candidate->observed_generation[node] != generation)
			return false;
	}
	return true;
}

/* Formation-LMON-only exact projection of the admitted MEMBER SSOT.  This
 * helper is observation-only: it copies under the reconfiguration lock,
 * validates QVOTEC currentness outside the lock, then byte-revalidates the
 * shared tuple and the same receiver-local observations. */
static bool
cluster_reconfig_lmon_snapshot_r4_membership_internal(
	ClusterR4MembershipSnapshot *out, const ClusterSemanticActivationRecord *stop_record,
	const uint8 *stop_root)
{
	ClusterR4MembershipSnapshot candidate;
	uint64 self_bit;
	int node;
	bool exact = true;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (ReconfigShmem == NULL || out == NULL || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES
		|| cluster_qvotec_get_status() != CLUSTER_QVOTEC_READY || !cluster_qvotec_in_quorum())
		return false;
	memset(&candidate, 0, sizeof(candidate));
	candidate.local_self_boot_incarnation = cluster_qvotec_get_self_incarnation();
	if (candidate.local_self_boot_incarnation == 0)
		return false;

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	candidate.formation_epoch = cluster_epoch_get_current();
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		uint64 floor;

		if (cluster_membership_get_state(node) != CLUSTER_MEMBER_MEMBER)
			continue;
		floor = cluster_membership_get_last_admitted_incarnation(node);
		if (cluster_conf_lookup_node(node) == NULL || floor == 0) {
			exact = false;
			break;
		}
		if (node < 64)
			candidate.admitted_members_lo |= UINT64_C(1) << node;
		else
			candidate.admitted_members_hi |= UINT64_C(1) << (node - 64);
		candidate.admitted_incarnation[node] = floor;
	}
	LWLockRelease(&ReconfigShmem->lock);

	self_bit = UINT64_C(1) << (cluster_node_id < 64 ? cluster_node_id : cluster_node_id - 64);
	if (!exact || (candidate.admitted_members_lo | candidate.admitted_members_hi) == 0
		|| (cluster_node_id < 64 ? (candidate.admitted_members_lo & self_bit) == 0
								 : (candidate.admitted_members_hi & self_bit) == 0)
		|| candidate.admitted_incarnation[cluster_node_id] != candidate.local_self_boot_incarnation
		|| !cluster_reconfig_r4_membership_observations_current(&candidate, true, stop_record,
																stop_root))
		return false;

	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	if (cluster_epoch_get_current() != candidate.formation_epoch)
		exact = false;
	for (node = 0; exact && node < CLUSTER_MAX_NODES; node++) {
		bool admitted = node < 64
							? (candidate.admitted_members_lo & (UINT64_C(1) << node)) != 0
							: (candidate.admitted_members_hi & (UINT64_C(1) << (node - 64))) != 0;

		if ((cluster_membership_get_state(node) == CLUSTER_MEMBER_MEMBER) != admitted
			|| (admitted
				&& (cluster_conf_lookup_node(node) == NULL
					|| cluster_membership_get_last_admitted_incarnation(node)
						   != candidate.admitted_incarnation[node])))
			exact = false;
	}
	LWLockRelease(&ReconfigShmem->lock);
	if (!exact
		|| !cluster_reconfig_r4_membership_observations_current(&candidate, false, stop_record,
																stop_root))
		return false;
	*out = candidate;
	return true;
}

bool
cluster_reconfig_lmon_snapshot_r4_membership(ClusterR4MembershipSnapshot *out)
{
	return cluster_reconfig_lmon_snapshot_r4_membership_internal(out, NULL, NULL);
}

/* Compatibility projection for callers that consume only the global bitmap
 * and formation epoch.  Exactness comes from the stack-only snapshot above. */
bool
cluster_reconfig_lmon_snapshot_admitted_membership(uint64 *out_members_lo, uint64 *out_members_hi,
												   uint64 *out_formation_epoch)
{
	ClusterR4MembershipSnapshot snapshot;

	if (out_members_lo != NULL)
		*out_members_lo = 0;
	if (out_members_hi != NULL)
		*out_members_hi = 0;
	if (out_formation_epoch != NULL)
		*out_formation_epoch = 0;
	if (out_members_lo == NULL || out_members_hi == NULL || out_formation_epoch == NULL
		|| !cluster_reconfig_lmon_snapshot_r4_membership(&snapshot))
		return false;
	*out_members_lo = snapshot.admitted_members_lo;
	*out_members_hi = snapshot.admitted_members_hi;
	*out_formation_epoch = snapshot.formation_epoch;
	return true;
}

bool
cluster_reconfig_normal_stop_snapshot_admitted_membership(
	const ClusterSemanticActivationRecord *open_record, const uint8 *root_descriptor,
	uint64 *out_members_lo, uint64 *out_members_hi, uint64 *out_formation_epoch)
{
	ClusterR4MembershipSnapshot snapshot;
	if (out_members_lo != NULL)
		*out_members_lo = 0;
	if (out_members_hi != NULL)
		*out_members_hi = 0;
	if (out_formation_epoch != NULL)
		*out_formation_epoch = 0;
	if (open_record == NULL || root_descriptor == NULL || out_members_lo == NULL
		|| out_members_hi == NULL || out_formation_epoch == NULL
		|| !cluster_reconfig_lmon_snapshot_r4_membership_internal(&snapshot, open_record,
																  root_descriptor))
		return false;
	*out_members_lo = snapshot.admitted_members_lo;
	*out_members_hi = snapshot.admitted_members_hi;
	*out_formation_epoch = snapshot.formation_epoch;
	return true;
}

static void
cluster_reconfig_release_closed_stage(void)
{
	if (join_marker_lmon_owner.reserved
		&& join_marker_lmon_owner.purpose == CLUSTER_JOIN_MARKER_LMON_CLOSED_APPLY)
		memset(&join_marker_lmon_owner, 0, sizeof(join_marker_lmon_owner));
	memset(&replacement_closed_stage, 0, sizeof(replacement_closed_stage));
	cluster_marker_async_init(&replacement_closed_stage.marker_async);
}

static bool
cluster_reconfig_lmon_submit_closed_observer_pair(TimestampTz now)
{
	static const uint8 zero_value[CLUSTER_QVOTEC_AUTHORITY_VALUE_BYTES] = { 0 };
	ClusterReplacementEpisode capability_episode;
	ClusterReplacementEpisode *episode;
	ClusterQvotecMailboxSubmitStatus authority_status;
	struct Latch *qlatch;
	bool marker_submitted;
	int wait_ms;

	if (!cluster_reconfig_replacement_candidate2_capabilities_current(&capability_episode))
		return false;
	wait_ms = cluster_quorum_poll_interval_ms * 3 + 2000;
	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	episode = &ReconfigShmem->replacement_episode;
	if (cluster_qvotec_get_status() != CLUSTER_QVOTEC_READY
		|| memcmp(episode, &capability_episode, sizeof(*episode)) != 0
		|| !cluster_replacement_episode_is_valid(episode)
		|| episode->phase != CLUSTER_REPLACEMENT_EPISODE_PURGE_COMPLETE
		|| episode->readiness_flags != 0
		|| memcmp(episode->acknowledgements, episode->expected_survivors,
				  sizeof(episode->acknowledgements))
			   != 0
		|| (cluster_node_id != episode->target_node_id
			&& !dead_bitmap_test_bit(episode->expected_survivors, cluster_node_id))
		|| !cluster_reconfig_replacement_membership_current_locked(episode)
		|| cluster_membership_get_last_admitted_incarnation(episode->target_node_id)
			   != episode->old_admitted_incarnation
		|| cluster_epoch_get_current() != episode->baseline_epoch || join_marker_lmon_owner.reserved
		|| cluster_marker_async_mailbox_busy(&ReconfigShmem->join_marker_request_seq,
											 &ReconfigShmem->join_marker_completion_seq)) {
		LWLockRelease(&ReconfigShmem->lock);
		return false;
	}

	join_marker_lmon_owner.reserved = true;
	join_marker_lmon_owner.purpose = CLUSTER_JOIN_MARKER_LMON_CLOSED_APPLY;
	join_marker_lmon_owner.operation = CLUSTER_JOIN_MARKER_MAILBOX_VERIFY_COMMITTED_CLOSED;
	join_marker_lmon_owner.marker_request_seq = 0;
	authority_status
		= cluster_qvotec_authority_lmon_submit(CLUSTER_QVOTEC_MAILBOX_RECOVER_HEAD, zero_value,
											   &replacement_closed_stage.authority_request_seq);
	if (authority_status != CLUSTER_QVOTEC_MAILBOX_SUBMIT_ACCEPTED) {
		memset(&join_marker_lmon_owner, 0, sizeof(join_marker_lmon_owner));
		LWLockRelease(&ReconfigShmem->lock);
		return false;
	}
	if (!cluster_reconfig_stage_join_marker_locked(
			episode->target_node_id, CLUSTER_JOIN_MARKER_MAILBOX_VERIFY_COMMITTED_CLOSED, 0, NULL,
			0)) {
		memset(&join_marker_lmon_owner, 0, sizeof(join_marker_lmon_owner));
		replacement_closed_stage.phase = CLUSTER_REPLACEMENT_CLOSED_STAGE_DRAIN_AUTHORITY;
		LWLockRelease(&ReconfigShmem->lock);
		return false;
	}
	marker_submitted = cluster_marker_async_submit(
		&replacement_closed_stage.marker_async, &ReconfigShmem->join_marker_request_seq,
		&ReconfigShmem->join_marker_completion_seq, NULL, now, (uint64)wait_ms * 1000ULL,
		CLUSTER_MARKER_KIND_REPLACEMENT_VERIFY_COMMITTED_CLOSED, episode->target_node_id);
	if (!marker_submitted) {
		memset(&join_marker_lmon_owner, 0, sizeof(join_marker_lmon_owner));
		replacement_closed_stage.phase = CLUSTER_REPLACEMENT_CLOSED_STAGE_DRAIN_AUTHORITY;
		LWLockRelease(&ReconfigShmem->lock);
		return false;
	}
	join_marker_lmon_owner.marker_request_seq = replacement_closed_stage.marker_async.inflight_seq;
	replacement_closed_stage.marker_completed = false;
	replacement_closed_stage.phase = CLUSTER_REPLACEMENT_CLOSED_STAGE_WAIT_PAIR;
	qlatch = ReconfigShmem->join_qvotec_latch;
	LWLockRelease(&ReconfigShmem->lock);
	if (qlatch != NULL)
		SetLatch(qlatch);
	return true;
}

static bool
cluster_reconfig_terminal_closed_matches_episode(const ClusterEpochAuthorityValue *head,
												 const ClusterEpochBallotId *ballot,
												 const ClusterReplacementCommitMarkerV3 *marker,
												 const ClusterReplacementEpisode *episode)
{
	uint8 subject[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };

	if (head == NULL || ballot == NULL || marker == NULL || episode == NULL)
		return false;
	subject[episode->target_node_id / 8] = (uint8)(1u << (episode->target_node_id % 8));
	return head->transition == CLUSTER_EPOCH_AUTHORITY_COMMIT_CLOSED
		   && head->event_kind == CLUSTER_EPOCH_EVENT_SAME_NODE_REPLACEMENT
		   && head->request_origin_node == episode->target_node_id
		   && head->target_node_id == episode->target_node_id
		   && head->baseline_epoch == episode->baseline_epoch
		   && head->reserved_epoch == episode->reserved_or_committed_epoch
		   && head->old_incarnation == episode->old_admitted_incarnation
		   && head->fresh_incarnation == episode->fresh_incarnation
		   && head->request_nonce == episode->request_nonce
		   && memcmp(head->authority_member_bitmap, episode->expected_survivors,
					 sizeof(head->authority_member_bitmap))
				  == 0
		   && memcmp(head->event_subject_bitmap, subject, sizeof(subject)) == 0
		   && head->grammar_fingerprint == episode->grammar_fingerprint
		   && ballot->proposer_node_id == episode->coordinator_node_id
		   && ballot->proposer_admitted_incarnation
				  == cluster_membership_get_last_admitted_incarnation(episode->coordinator_node_id)
		   && marker->phase == CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED
		   && marker->ready_state_generation == 0
		   && marker->target_node_id == episode->target_node_id
		   && marker->old_admitted_incarnation == episode->old_admitted_incarnation
		   && marker->fresh_incarnation == episode->fresh_incarnation
		   && marker->baseline_epoch == episode->baseline_epoch
		   && marker->reserved_or_committed_epoch == episode->reserved_or_committed_epoch
		   && marker->request_nonce == episode->request_nonce
		   && memcmp(marker->expected_purge_survivors, episode->expected_survivors,
					 sizeof(marker->expected_purge_survivors))
				  == 0
		   && marker->grammar_fingerprint == episode->grammar_fingerprint;
}

ClusterReplacementCommittedClosedPublishResultV1
cluster_reconfig_lmon_publish_replacement_committed_closed(uint64 authority_request_seq,
														   uint64 marker_request_seq)
{
	ClusterReplacementCommittedClosedPublishResultV1 result = CLUSTER_REPLACEMENT_CLOSED_RETRY;
	ClusterQvotecMailboxCompletion completion;
	ClusterEpochAuthorityValue head;
	ClusterEpochBallotId ballot;
	ClusterReplacementCommitMarkerV3 marker;
	ClusterReplacementEpisode capability_episode;
	ClusterReplacementEpisode candidate;
	ClusterJoinMarkerMailboxOperationV1 operation;
	ReconfigEvent event;
	ReconfigEvent expected_event;
	uint32 next_generation;
	uint32 request_word;
	int32 request_target;
	int32 observer_role;
	uint64 current_epoch;
	bool release_owner = true;

	if (ReconfigShmem == NULL || MyBackendType != B_LMON || authority_request_seq == 0
		|| (authority_request_seq & UINT64_C(1)) != 0 || marker_request_seq == 0)
		return CLUSTER_REPLACEMENT_CLOSED_INVALID;
	if (!cluster_reconfig_replacement_candidate2_capabilities_current(&capability_episode))
		return CLUSTER_REPLACEMENT_CLOSED_HOLD_IDENTITY;

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	if (!join_marker_lmon_owner.reserved
		|| join_marker_lmon_owner.purpose != CLUSTER_JOIN_MARKER_LMON_CLOSED_APPLY
		|| join_marker_lmon_owner.marker_request_seq != marker_request_seq) {
		result = CLUSTER_REPLACEMENT_CLOSED_RETRY;
		goto out;
	}
	if (cluster_qvotec_get_status() != CLUSTER_QVOTEC_READY) {
		result = CLUSTER_REPLACEMENT_CLOSED_RETRY;
		goto out;
	}
	if (!cluster_qvotec_authority_lmon_poll_completion(authority_request_seq, &completion)) {
		result = CLUSTER_REPLACEMENT_CLOSED_BLOCKED_QVOTEC;
		release_owner = false;
		goto out;
	}
	if (completion.result != CLUSTER_QVOTEC_MAILBOX_CHOSEN
		|| !cluster_epoch_authority_value_decode(completion.completion_value,
												 CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, &head)
		|| !cluster_epoch_ballot_id_decode(completion.completion_ballot, &ballot)) {
		result = CLUSTER_REPLACEMENT_CLOSED_BLOCKED_QVOTEC;
		goto out;
	}
	if (pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq) != marker_request_seq
		|| pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq) != marker_request_seq
		|| pg_atomic_read_u32(&ReconfigShmem->join_marker_result)
			   != CLUSTER_JOIN_MARKER_SUBMIT_ACK) {
		result = CLUSTER_REPLACEMENT_CLOSED_BLOCKED_JCMK;
		release_owner = false;
		goto out;
	}
	request_word = ReconfigShmem->join_marker_request_word;
	if (!cluster_reconfig_join_marker_request_word_decode(request_word, &operation, &request_target)
		|| operation != join_marker_lmon_owner.operation
		|| !((completion.actor_phase == CLUSTER_QVOTEC_ACTOR_RECOVER_SCAN_B
			  && operation == CLUSTER_JOIN_MARKER_MAILBOX_VERIFY_COMMITTED_CLOSED)
			 || (completion.actor_phase == CLUSTER_QVOTEC_ACTOR_SETTLE_WRITE
				 && operation == CLUSTER_JOIN_MARKER_MAILBOX_WRITE_EXACT))) {
		result = CLUSTER_REPLACEMENT_CLOSED_HOLD_IDENTITY;
		goto out;
	}
	if (!cluster_replacement_marker_v3_decode(ReconfigShmem->join_pending_marker, request_target,
											  &marker)) {
		result = CLUSTER_REPLACEMENT_CLOSED_BLOCKED_JCMK;
		goto out;
	}
	if (memcmp(&ReconfigShmem->replacement_episode, &capability_episode, sizeof(capability_episode))
			!= 0
		|| !cluster_replacement_episode_is_valid(&ReconfigShmem->replacement_episode)
		|| (ReconfigShmem->replacement_episode.phase != CLUSTER_REPLACEMENT_EPISODE_PURGE_COMPLETE
			&& ReconfigShmem->replacement_episode.phase
				   != CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED)
		|| ReconfigShmem->replacement_episode.readiness_flags != 0
		|| memcmp(ReconfigShmem->replacement_episode.acknowledgements,
				  ReconfigShmem->replacement_episode.expected_survivors,
				  sizeof(ReconfigShmem->replacement_episode.acknowledgements))
			   != 0
		|| (cluster_node_id != ReconfigShmem->replacement_episode.target_node_id
			&& !dead_bitmap_test_bit(ReconfigShmem->replacement_episode.expected_survivors,
									 cluster_node_id))
		|| !cluster_reconfig_replacement_membership_current_locked(
			&ReconfigShmem->replacement_episode)
		|| cluster_membership_get_last_admitted_incarnation(
			   ReconfigShmem->replacement_episode.target_node_id)
			   != ReconfigShmem->replacement_episode.old_admitted_incarnation
		|| !cluster_reconfig_terminal_closed_matches_episode(&head, &ballot, &marker,
															 &ReconfigShmem->replacement_episode)) {
		result = CLUSTER_REPLACEMENT_CLOSED_HOLD_IDENTITY;
		goto out;
	}

	current_epoch = cluster_epoch_get_current();
	if (current_epoch != ReconfigShmem->replacement_episode.baseline_epoch
		&& current_epoch != ReconfigShmem->replacement_episode.reserved_or_committed_epoch) {
		result = CLUSTER_REPLACEMENT_CLOSED_HOLD_IDENTITY;
		goto out;
	}
	observer_role = cluster_node_id == ReconfigShmem->replacement_episode.coordinator_node_id
						? CLUSTER_RECONFIG_OBSERVER_COORDINATOR
						: CLUSTER_RECONFIG_OBSERVER_SURVIVOR;
	if (ReconfigShmem->replacement_episode.phase == CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED) {
		if (!cluster_reconfig_build_replacement_committed_event(
				&ReconfigShmem->replacement_episode, observer_role,
				ReconfigShmem->last_applied.applied_at, &expected_event)) {
			result = CLUSTER_REPLACEMENT_CLOSED_HOLD_IDENTITY;
			goto out;
		}
		expected_event.event_seq = ReconfigShmem->last_applied.event_seq;
		result = memcmp(&expected_event, &ReconfigShmem->last_applied, sizeof(expected_event)) == 0
					 ? CLUSTER_REPLACEMENT_CLOSED_ALREADY_CURRENT
					 : CLUSTER_REPLACEMENT_CLOSED_HOLD_IDENTITY;
		goto out;
	}

	candidate = ReconfigShmem->replacement_episode;
	if (!cluster_replacement_episode_next_generation(candidate.state_generation,
													 &next_generation)) {
		result = CLUSTER_REPLACEMENT_CLOSED_HOLD_IDENTITY;
		goto out;
	}
	candidate.state_generation = next_generation;
	candidate.phase = CLUSTER_REPLACEMENT_EPISODE_COMMITTED_CLOSED;
	if (!cluster_reconfig_build_replacement_committed_event(&candidate, observer_role,
															GetCurrentTimestamp(), &event)) {
		result = CLUSTER_REPLACEMENT_CLOSED_HOLD_IDENTITY;
		goto out;
	}
	if (pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq) != marker_request_seq
		|| pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq) != marker_request_seq
		|| ReconfigShmem->join_marker_request_word != request_word) {
		result = CLUSTER_REPLACEMENT_CLOSED_RETRY;
		goto out;
	}
	if (current_epoch == candidate.baseline_epoch)
		(void)cluster_epoch_observe_remote(candidate.reserved_or_committed_epoch);
	if (cluster_epoch_get_current() != candidate.reserved_or_committed_epoch) {
		result = CLUSTER_REPLACEMENT_CLOSED_HOLD_IDENTITY;
		goto out;
	}
	event.event_seq = pg_atomic_fetch_add_u64(&ReconfigShmem->apply_counter, 1) + 1;
	ReconfigShmem->replacement_episode = candidate;
	ReconfigShmem->last_applied = event;
	result = CLUSTER_REPLACEMENT_CLOSED_PUBLISHED;

out:
	if (release_owner)
		memset(&join_marker_lmon_owner, 0, sizeof(join_marker_lmon_owner));
	LWLockRelease(&ReconfigShmem->lock);
	return result;
}

void
cluster_reconfig_lmon_replacement_closed_tick(void)
{
	ClusterReplacementCommittedClosedPublishResultV1 publish_result;
	ClusterQvotecMailboxCompletion ignored_completion;
	ClusterMarkerPollResult marker_poll;
	uint32 marker_result = CLUSTER_JOIN_MARKER_SUBMIT_FAILED;
	uint64 elapsed_us = 0;
	TimestampTz now;

	if (!cluster_enabled || MyBackendType != B_LMON || !cluster_qvotec_in_quorum()
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		return;
	if (replacement_closed_stage.phase == CLUSTER_REPLACEMENT_CLOSED_STAGE_IDLE) {
		cluster_marker_async_init(&replacement_closed_stage.marker_async);
		(void)cluster_reconfig_lmon_submit_closed_observer_pair(GetCurrentTimestamp());
		return;
	}
	if (replacement_closed_stage.phase == CLUSTER_REPLACEMENT_CLOSED_STAGE_DRAIN_AUTHORITY) {
		(void)cluster_qvotec_authority_lmon_poll_completion(
			replacement_closed_stage.authority_request_seq, &ignored_completion);
		cluster_reconfig_release_closed_stage();
		return;
	}

	now = GetCurrentTimestamp();
	if (!replacement_closed_stage.marker_completed) {
		marker_poll = cluster_reconfig_poll_join_marker_async(
			&replacement_closed_stage.marker_async, now, &marker_result, &elapsed_us);
		if (marker_poll == CLUSTER_MARKER_POLL_PENDING || marker_poll == CLUSTER_MARKER_POLL_IDLE)
			return;
		if (marker_poll != CLUSTER_MARKER_POLL_ACKED
			|| marker_result != CLUSTER_JOIN_MARKER_SUBMIT_ACK) {
			cluster_reconfig_release_closed_stage();
			return;
		}
		replacement_closed_stage.marker_completed = true;
	}
	publish_result = cluster_reconfig_lmon_publish_replacement_committed_closed(
		replacement_closed_stage.authority_request_seq,
		replacement_closed_stage.marker_async.inflight_seq);
	if (publish_result == CLUSTER_REPLACEMENT_CLOSED_BLOCKED_QVOTEC
		|| publish_result == CLUSTER_REPLACEMENT_CLOSED_BLOCKED_JCMK)
		return;
	cluster_reconfig_release_closed_stage();
}

ClusterJoinMarkerSubmitResult
cluster_reconfig_submit_join_marker(int32 target_node, const ClusterJoinCommitMarker *m)
{
	uint64 seq;
	struct Latch *qlatch;
	uint64 deadline_us;
	int wait_ms;

	if (ReconfigShmem == NULL || m == NULL)
		return CLUSTER_JOIN_MARKER_SUBMIT_FAILED;
	if (target_node < 0 || target_node >= CLUSTER_MAX_NODES)
		return CLUSTER_JOIN_MARKER_SUBMIT_FAILED;

	/* Serialize staging with QVOTEC lifecycle/final receipt use. */
	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	if (join_marker_lmon_owner.reserved
		|| cluster_marker_async_mailbox_busy(&ReconfigShmem->join_marker_request_seq,
											 &ReconfigShmem->join_marker_completion_seq)
		|| !cluster_reconfig_stage_join_marker_locked(target_node,
													  CLUSTER_JOIN_MARKER_MAILBOX_WRITE_EXACT,
													  CLUSTER_JCMK_VERSION, m, sizeof(*m))) {
		LWLockRelease(&ReconfigShmem->lock);
		return CLUSTER_JOIN_MARKER_SUBMIT_FAILED;
	}
	pg_write_barrier();
	seq = pg_atomic_add_fetch_u64(&ReconfigShmem->join_marker_request_seq, 1);
	qlatch = ReconfigShmem->join_qvotec_latch;
	LWLockRelease(&ReconfigShmem->lock);
	if (qlatch != NULL)
		SetLatch(qlatch);

	wait_ms = cluster_quorum_poll_interval_ms * 3 + 2000;
	deadline_us = (uint64)GetCurrentTimestamp() + (uint64)wait_ms * 1000ULL;
	pgstat_report_wait_start(WAIT_EVENT_RECONFIG_JOIN_CONVERGENCE);
	for (;;) {
		if (pg_atomic_read_u64(&ReconfigShmem->join_marker_completion_seq) == seq) {
			pgstat_report_wait_end();
			pg_read_barrier();
			return (ClusterJoinMarkerSubmitResult)pg_atomic_read_u32(
				&ReconfigShmem->join_marker_result);
		}
		if ((uint64)GetCurrentTimestamp() >= deadline_us) {
			pgstat_report_wait_end();
			return CLUSTER_JOIN_MARKER_SUBMIT_TIMEOUT;
		}
		pg_usleep(2 * 1000); /* 2 ms */
	}
}

bool
cluster_reconfig_submit_join_marker_async(ClusterMarkerAsync *a, int32 target_node,
										  const ClusterJoinCommitMarker *m,
										  ClusterMarkerAsyncKind kind, TimestampTz now)
{
	int wait_ms;
	struct Latch *qlatch;
	bool submitted;

	if (ReconfigShmem == NULL || m == NULL || a == NULL)
		return false;
	if (target_node < 0 || target_node >= CLUSTER_MAX_NODES)
		return false;
	if (cluster_marker_async_is_submitted(a))
		return true;
	wait_ms = cluster_quorum_poll_interval_ms * 3 + 2000;
	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	if (join_marker_lmon_owner.reserved
		|| cluster_marker_async_mailbox_busy(&ReconfigShmem->join_marker_request_seq,
											 &ReconfigShmem->join_marker_completion_seq)
		|| !cluster_reconfig_stage_join_marker_locked(target_node,
													  CLUSTER_JOIN_MARKER_MAILBOX_WRITE_EXACT,
													  CLUSTER_JCMK_VERSION, m, sizeof(*m))) {
		LWLockRelease(&ReconfigShmem->lock);
		return false;
	}
	submitted = cluster_marker_async_submit(a, &ReconfigShmem->join_marker_request_seq,
											&ReconfigShmem->join_marker_completion_seq, NULL, now,
											(uint64)wait_ms * 1000ULL, kind, target_node);
	qlatch = ReconfigShmem->join_qvotec_latch;
	LWLockRelease(&ReconfigShmem->lock);
	if (submitted && qlatch != NULL)
		SetLatch(qlatch);
	return submitted;
}

bool
cluster_reconfig_submit_replacement_marker_v3_async(ClusterMarkerAsync *a, int32 target_node,
													const ClusterReplacementCommitMarkerV3 *marker,
													ClusterMarkerAsyncKind kind, TimestampTz now)
{
	uint8 image[CLUSTER_JCMK_REPLACEMENT_BYTES];
	int wait_ms;
	struct Latch *qlatch;
	bool submitted;

	if (ReconfigShmem == NULL || marker == NULL || a == NULL)
		return false;
	if (target_node < 0 || target_node >= CLUSTER_MAX_NODES
		|| marker->target_node_id != target_node)
		return false;
	if (!cluster_replacement_marker_v3_encode(marker, image))
		return false;
	if (cluster_marker_async_is_submitted(a))
		return true;
	wait_ms = cluster_quorum_poll_interval_ms * 3 + 2000;
	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	if (join_marker_lmon_owner.reserved
		|| cluster_marker_async_mailbox_busy(&ReconfigShmem->join_marker_request_seq,
											 &ReconfigShmem->join_marker_completion_seq)
		|| !cluster_reconfig_stage_join_marker_locked(
			target_node, CLUSTER_JOIN_MARKER_MAILBOX_WRITE_EXACT, CLUSTER_JCMK_REPLACEMENT_VERSION,
			image, sizeof(image))) {
		LWLockRelease(&ReconfigShmem->lock);
		return false;
	}
	submitted = cluster_marker_async_submit(a, &ReconfigShmem->join_marker_request_seq,
											&ReconfigShmem->join_marker_completion_seq, NULL, now,
											(uint64)wait_ms * 1000ULL, kind, target_node);
	qlatch = ReconfigShmem->join_qvotec_latch;
	LWLockRelease(&ReconfigShmem->lock);
	if (submitted && qlatch != NULL)
		SetLatch(qlatch);
	return submitted;
}

bool
cluster_reconfig_verify_replacement_committed_closed_async(ClusterMarkerAsync *a, int32 target_node,
														   TimestampTz now)
{
	int wait_ms;
	struct Latch *qlatch;
	bool submitted;

	if (ReconfigShmem == NULL || a == NULL || target_node < 0 || target_node >= CLUSTER_MAX_NODES)
		return false;
	if (cluster_marker_async_is_submitted(a))
		return true;
	wait_ms = cluster_quorum_poll_interval_ms * 3 + 2000;
	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	if (join_marker_lmon_owner.reserved
		|| cluster_marker_async_mailbox_busy(&ReconfigShmem->join_marker_request_seq,
											 &ReconfigShmem->join_marker_completion_seq)
		|| !cluster_reconfig_stage_join_marker_locked(
			target_node, CLUSTER_JOIN_MARKER_MAILBOX_VERIFY_COMMITTED_CLOSED, 0, NULL, 0)) {
		LWLockRelease(&ReconfigShmem->lock);
		return false;
	}
	submitted = cluster_marker_async_submit(
		a, &ReconfigShmem->join_marker_request_seq, &ReconfigShmem->join_marker_completion_seq,
		NULL, now, (uint64)wait_ms * 1000ULL,
		CLUSTER_MARKER_KIND_REPLACEMENT_VERIFY_COMMITTED_CLOSED, target_node);
	qlatch = ReconfigShmem->join_qvotec_latch;
	LWLockRelease(&ReconfigShmem->lock);
	if (submitted && qlatch != NULL)
		SetLatch(qlatch);
	return submitted;
}

ClusterMarkerPollResult
cluster_reconfig_poll_join_marker_async(ClusterMarkerAsync *a, TimestampTz now, uint32 *out_result,
										uint64 *out_elapsed_us)
{
	if (ReconfigShmem == NULL || a == NULL)
		return CLUSTER_MARKER_POLL_IDLE;
	return cluster_marker_async_poll(a, &ReconfigShmem->join_marker_completion_seq,
									 &ReconfigShmem->join_marker_result, now, out_result,
									 out_elapsed_us);
}

bool
cluster_reconfig_join_qvotec_poll_pending(ClusterJoinMarkerMailboxOperationV1 *operation_out,
										  int32 *target_node_out, void *write_slot512_out)
{
	uint64 request_seq_before;
	uint64 request_seq_after;
	uint32 request_word;
	ClusterJoinMarkerMailboxOperationV1 operation;
	int32 target_node;

	if (ReconfigShmem == NULL || operation_out == NULL || target_node_out == NULL
		|| write_slot512_out == NULL || join_qvotec_marker_inflight)
		return false;
	*operation_out = CLUSTER_JOIN_MARKER_MAILBOX_WRITE_EXACT;
	*target_node_out = -1;
	memset(write_slot512_out, 0, CLUSTER_VOTING_SLOT_BYTES);

	request_seq_before = pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq);
	if (request_seq_before == 0 || request_seq_before == join_qvotec_last_processed_marker_seq)
		return false; /* nothing new */

	pg_read_barrier();
	request_word = ReconfigShmem->join_marker_request_word;
	memcpy(write_slot512_out, ReconfigShmem->join_pending_marker,
		   sizeof(ReconfigShmem->join_pending_marker));
	pg_read_barrier();
	request_seq_after = pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq);
	if (request_seq_before != request_seq_after)
		return false;
	if (!cluster_reconfig_join_marker_request_word_decode(request_word, &operation, &target_node)) {
		LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
		if (pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq) == request_seq_before) {
			memset(ReconfigShmem->join_pending_marker, 0,
				   sizeof(ReconfigShmem->join_pending_marker));
			pg_atomic_write_u32(&ReconfigShmem->join_marker_result,
								CLUSTER_JOIN_MARKER_SUBMIT_FAILED);
			pg_write_barrier();
			pg_atomic_write_u64(&ReconfigShmem->join_marker_completion_seq, request_seq_before);
			join_qvotec_last_processed_marker_seq = request_seq_before;
		}
		LWLockRelease(&ReconfigShmem->lock);
		cluster_lmon_marker_complete_wakeup();
		memset(write_slot512_out, 0, CLUSTER_VOTING_SLOT_BYTES);
		return false;
	}
	if (operation == CLUSTER_JOIN_MARKER_MAILBOX_VERIFY_COMMITTED_CLOSED)
		memset(write_slot512_out, 0, CLUSTER_VOTING_SLOT_BYTES);
	*operation_out = operation;
	*target_node_out = target_node;
	join_qvotec_inflight_marker_seq = request_seq_before;
	join_qvotec_inflight_marker_operation = operation;
	join_qvotec_marker_inflight = true;
	return true;
}

void
cluster_reconfig_join_qvotec_complete(ClusterJoinMarkerMailboxOperationV1 operation, bool acked,
									  const uint8 *verified_image96)
{
	ClusterJoinMarkerMailboxOperationV1 request_operation;
	ClusterReplacementCommitMarkerV3 verified_marker;
	uint8 canonical[CLUSTER_JCMK_REPLACEMENT_BYTES];
	int32 target_node;
	bool call_matches;
	bool clear_payload = false;

	if (ReconfigShmem == NULL || !join_qvotec_marker_inflight)
		return;

	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	call_matches = operation == join_qvotec_inflight_marker_operation
				   && pg_atomic_read_u64(&ReconfigShmem->join_marker_request_seq)
						  == join_qvotec_inflight_marker_seq
				   && cluster_reconfig_join_marker_request_word_decode(
					   ReconfigShmem->join_marker_request_word, &request_operation, &target_node)
				   && request_operation == join_qvotec_inflight_marker_operation;
	if (call_matches && operation == CLUSTER_JOIN_MARKER_MAILBOX_WRITE_EXACT) {
		if (verified_image96 != NULL) {
			call_matches = false;
			clear_payload = true;
		}
	} else if (call_matches) {
		clear_payload = true;
		if (acked) {
			call_matches
				= verified_image96 != NULL
				  && cluster_replacement_marker_v3_decode(verified_image96, target_node,
														  &verified_marker)
				  && verified_marker.phase == CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED
				  && verified_marker.ready_state_generation == 0
				  && cluster_replacement_marker_v3_encode(&verified_marker, canonical)
				  && memcmp(canonical, verified_image96, CLUSTER_JCMK_REPLACEMENT_BYTES) == 0;
		} else if (verified_image96 != NULL)
			call_matches = false;
	}
	if (!call_matches)
		clear_payload = true;
	if (clear_payload)
		memset(ReconfigShmem->join_pending_marker, 0, sizeof(ReconfigShmem->join_pending_marker));
	if (call_matches && acked && operation == CLUSTER_JOIN_MARKER_MAILBOX_VERIFY_COMMITTED_CLOSED)
		memcpy(ReconfigShmem->join_pending_marker, verified_image96,
			   CLUSTER_JCMK_REPLACEMENT_BYTES);
	pg_atomic_write_u32(&ReconfigShmem->join_marker_result,
						call_matches && acked ? CLUSTER_JOIN_MARKER_SUBMIT_ACK
											  : CLUSTER_JOIN_MARKER_SUBMIT_FAILED);
	pg_write_barrier();
	pg_atomic_write_u64(&ReconfigShmem->join_marker_completion_seq,
						join_qvotec_inflight_marker_seq);
	join_qvotec_last_processed_marker_seq = join_qvotec_inflight_marker_seq;
	join_qvotec_marker_inflight = false;
	LWLockRelease(&ReconfigShmem->lock);
	cluster_lmon_marker_complete_wakeup();
}

/* ============================================================
 * RF-ROOT P9 verification / cold-formation cold-formation ruling —
 * cold-formation marker mailbox + observation-window state machine.
 *
 *	A cluster that cold-co-boots PAST the initial epoch (clean shutdown
 *	+ restart, full-outage crash co-boot) has no live survivor and no
 *	coordinator.  The co-booting nodes observe a quorum-stable window
 *	with no fresh slot past INITIAL (slot-epoch semantics: an UN-FORMED
 *	node publishes INITIAL — cluster_qvotec.c), pick the LOWEST co-boot
 *	node as arbiter, the arbiter submits a COMMITTED formation marker
 *	naming the exact generation / epoch / arbiter / per-member current
 *	incarnations through the qvotec mailbox (region-7 write + majority
 *	readback), and every member then publishes record_admitted(exact
 *	incarnation) -> exact equality -> MEMBER.  Live-cluster survivors
 *	(slot past INITIAL) never enter this path; never-seen / returning
 *	nodes keep the ordinary JCMK join.  The root never seeds membership.
 * ============================================================ */

static uint64 formation_qvotec_last_processed_seq = 0;
static uint32
formation_bitmap_popcount(const uint8 *bmp, int bytes)
{
	uint32 pop = 0;
	int i;

	for (i = 0; i < bytes; i++)
		pop += pg_number_of_ones[bmp[i]];
	return pop;
}

/* Freshest observed voting-slot incarnation for a declared peer (0 when
 * no valid slot). */
static uint64
cluster_reconfig_cold_formation_observed_incarnation(int32 node_id)
{
	uint64 inc = 0;
	uint64 gen = 0;

	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return 0;
	(void)cluster_reconfig_get_observed_slot(node_id, &inc, &gen);
	return gen > 0 ? inc : 0;
}

/*
 * qvotec side of the formation-marker mailbox.  Mirrors the join-marker
 * poll/completion pattern (single producer = arbiter LMON, single
 * consumer = qvotec).
 */
bool
cluster_reconfig_formation_qvotec_poll_pending(ClusterFormationMarkerSubmitRequest *out)
{
	uint64 seq_before;
	uint64 seq_after;

	if (ReconfigShmem == NULL || out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	seq_before = pg_atomic_read_u64(&ReconfigShmem->formation_marker_request_seq);
	if (seq_before == 0 || seq_before == formation_qvotec_last_processed_seq)
		return false; /* nothing new */
	pg_read_barrier();
	*out = ReconfigShmem->formation_marker_request;
	pg_read_barrier();
	seq_after = pg_atomic_read_u64(&ReconfigShmem->formation_marker_request_seq);
	if (seq_before != seq_after)
		return false;
	formation_qvotec_last_processed_seq = seq_before;
	return true;
}

void
cluster_reconfig_formation_qvotec_complete(bool success)
{
	if (ReconfigShmem == NULL)
		return;
	pg_atomic_write_u32(&ReconfigShmem->formation_marker_result, success ? 1 : 0);
	pg_write_barrier();
	pg_atomic_write_u64(&ReconfigShmem->formation_marker_completion_seq,
						pg_atomic_read_u64(&ReconfigShmem->formation_marker_request_seq));
	cluster_lmon_marker_complete_wakeup();
}

/* Startup seed: the highest COMMITTED formation generation qvotec found
 * across region 7 (0 when none).  The arbiter writes max+1 (monotonic
 * takeover). */
void
cluster_reconfig_formation_qvotec_note_max_generation(uint64 generation)
{
	if (ReconfigShmem == NULL)
		return;
	if (generation > pg_atomic_read_u64(&ReconfigShmem->formation_marker_max_generation))
		pg_atomic_write_u64(&ReconfigShmem->formation_marker_max_generation, generation);
}

/* qvotec publishes this node's OWN region-7 slot observation (CRC-valid
 * COMMITTED marker only).  The cold-formation admission consumes it. */
void
cluster_reconfig_formation_qvotec_publish_observed(const ClusterFormationCommitMarker *marker,
												   const uint64 *incarnation_by_node)
{
	int i;

	if (ReconfigShmem == NULL || marker == NULL)
		return;
	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		pg_atomic_write_u64(&ReconfigShmem->observed_formation_marker_incarnation[i],
							incarnation_by_node != NULL ? incarnation_by_node[i] : 0);
	pg_atomic_write_u64(&ReconfigShmem->observed_formation_marker_epoch, marker->formation_epoch);
	pg_atomic_write_u64(&ReconfigShmem->observed_formation_marker_arbiter_node,
						marker->arbiter_node);
	pg_atomic_write_u64(&ReconfigShmem->observed_formation_marker_arbiter_incarnation,
						marker->arbiter_incarnation);
	pg_write_barrier();
	pg_atomic_write_u64(&ReconfigShmem->observed_formation_marker_generation,
						marker->formation_generation);
}

void
cluster_reconfig_formation_qvotec_clear_observed(void)
{
	if (ReconfigShmem == NULL)
		return;
	pg_atomic_write_u64(&ReconfigShmem->observed_formation_marker_generation, 0);
}

/* Arbiter submit: stage the marker image + target member set into the
 * mailbox and wake qvotec. */
static bool
cluster_reconfig_formation_marker_submit(const uint8 marker_bytes[CLUSTER_VOTING_SLOT_BYTES],
										 const uint8 *target_members)
{
	uint64 seq;

	if (ReconfigShmem == NULL || marker_bytes == NULL || target_members == NULL)
		return false;
	/* Caller (cold-formation tick) holds ReconfigShmem->lock EXCLUSIVE;
	 * the mailbox is single-producer (arbiter LMON) so no lock is needed
	 * for the submit itself — just the torn-free seq pairing. */
	seq = pg_atomic_read_u64(&ReconfigShmem->formation_marker_request_seq);
	if (seq != pg_atomic_read_u64(&ReconfigShmem->formation_marker_completion_seq)
		|| seq == UINT64_MAX)
		return false; /* in-flight or exhausted */
	memset(&ReconfigShmem->formation_marker_request, 0,
		   sizeof(ReconfigShmem->formation_marker_request));
	memcpy(ReconfigShmem->formation_marker_request.marker_bytes, marker_bytes,
		   CLUSTER_VOTING_SLOT_BYTES);
	memcpy(ReconfigShmem->formation_marker_request.target_members, target_members,
		   CLUSTER_FORMATION_MARKER_BITMAP_BYTES);
	ReconfigShmem->formation_marker_request.active = true;
	pg_write_barrier();
	pg_atomic_write_u64(&ReconfigShmem->formation_marker_request_seq, seq + 1);
	if (ReconfigShmem->formation_qvotec_latch != NULL)
		SetLatch(ReconfigShmem->formation_qvotec_latch);
	return true;
}

/*
 * Observation window (three-state): quorum of declared nodes (self +
 * peers) are FRESH-ALIVE this tick, and NO fresh slot is past INITIAL (a
 * formed survivor publishes the live epoch; an un-formed co-boot node
 * publishes INITIAL — the slot-epoch semantics in cluster_qvotec.c).
 * Returns COBOOT + the co-boot member bitmap when the window passes,
 * SURVIVOR when any fresh slot is past INITIAL (never a cold formation),
 * PENDING when neither proof holds yet (fail-closed: keep waiting).
 * Self must be UN-FORMED for COBOOT (an already-MEMBER node is a formed
 * cluster, not a cold co-boot).
 */
static ClusterColdFormationVerdict
cluster_reconfig_cold_formation_window(uint64 *out_coboot_lo, uint64 *out_coboot_hi)
{
	uint64 fresh_lo = 0;
	uint64 fresh_hi = 0;
	uint32 declared = 0;
	uint32 fresh_count = 0;
	bool survivor = false;
	int i;

	if (cluster_membership_get_state(cluster_node_id) == CLUSTER_MEMBER_MEMBER)
		return CLUSTER_COLD_FORMATION_SURVIVOR; /* this node is already formed */

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		uint64 inc = 0;
		uint64 gen = 0;
		uint64 ep;
		bool fresh;

		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		declared++;
		if (i == cluster_node_id) {
			fresh_count++;
			if (i < 64)
				fresh_lo |= UINT64_C(1) << i;
			else
				fresh_hi |= UINT64_C(1) << (i - 64);
			continue;
		}
		(void)cluster_reconfig_get_observed_slot(i, &inc, &gen);
		ep = cluster_reconfig_get_observed_epoch(i);
		fresh = cluster_reconfig_get_observed_fresh_alive(i);
		if (fresh && gen > 0) {
			fresh_count++;
			if (i < 64)
				fresh_lo |= UINT64_C(1) << i;
			else
				fresh_hi |= UINT64_C(1) << (i - 64);
			/* A formed survivor publishes the live epoch — never a
			 * cold formation.  Fail the window. */
			if (ep > CLUSTER_EPOCH_INITIAL)
				survivor = true;
		}
	}
	if (survivor)
		return CLUSTER_COLD_FORMATION_SURVIVOR;
	if (declared == 0)
		return CLUSTER_COLD_FORMATION_PENDING;
	if (fresh_count < (declared / 2u) + 1u)
		return CLUSTER_COLD_FORMATION_PENDING; /* not yet a quorum */
	*out_coboot_lo = fresh_lo;
	*out_coboot_hi = fresh_hi;
	return CLUSTER_COLD_FORMATION_COBOOT;
}

/* Admission from the marker: record_admitted(exact incarnation) for every
 * co-boot member, verify exact equality, then MEMBER (D13 strict order). */
static void
cluster_reconfig_cold_formation_admit(const ClusterFormationCommitMarker *marker,
									  const uint64 *incarnation_by_node)
{
	ClusterXidStripeJoinVerdict stripe_verdict;
	uint64 self_incarnation;
	int i;

	if (marker == NULL || incarnation_by_node == NULL)
		return;
	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| (marker->admitted_nodes[cluster_node_id / 8] & (uint8)(1u << (cluster_node_id % 8)))
			   == 0)
		return;
	self_incarnation = cluster_qvotec_get_self_incarnation();
	if (self_incarnation == 0 || incarnation_by_node[cluster_node_id] != self_incarnation)
		return;
	if (cluster_membership_get_last_admitted_incarnation(cluster_node_id) != 0) {
		cold_formation_state.admission_done = true;
		return; /* already admitted */
	}

	/*
	 * spec-6.15 D5b / Oracle-first admission ordering: the exact durable
	 * formation marker is membership evidence, not an xid-allocation bypass.
	 * Reuse the same PGRAC xid-stripe prerequisite as ordinary JCMK admission:
	 * PGXA must be current, this node's PGXS must be durable MINE, and the JOIN
	 * record must be insertable.  HOLD leaves the marker available for the next
	 * QVOTEC/LMON tick; REFUSE remains fail closed.  A cold-formation consumer
	 * is never allowed to seed PGXA here.
	 */
	stripe_verdict = cluster_xid_stripe_join_gate(false);
	if (stripe_verdict != CLUSTER_XID_STRIPE_JOIN_PROCEED) {
		static bool stripe_formation_admit_logged = false;

		if (!stripe_formation_admit_logged) {
			stripe_formation_admit_logged = true;
			if (stripe_verdict == CLUSTER_XID_STRIPE_JOIN_REFUSE)
				ereport(LOG, (errcode(ERRCODE_CLUSTER_XID_STRIPE_JOIN_MISMATCH),
							  errmsg("cluster xid stripe: refusing cold-formation "
									 "admission of node %d — stripe mode handshake "
									 "mismatch (SQLSTATE 53RB1)",
									 cluster_node_id),
							  errhint("cluster.xid_striping must match the cluster's "
									  "durable activation state on every node; repair the "
									  "voting-disk stripe region if it is corrupt.")));
			else
				ereport(LOG, (errmsg("cluster xid stripe: holding cold-formation "
									 "admission of node %d until the stripe activation "
									 "state is resolved",
									 cluster_node_id)));
		}
		return;
	}
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if ((marker->admitted_nodes[i / 8] & (uint8)(1u << (i % 8))) == 0)
			continue;
		if (incarnation_by_node[i] == 0)
			return; /* marker incomplete: never admit */
		cluster_membership_record_admitted(i, incarnation_by_node[i]);
	}
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if ((marker->admitted_nodes[i / 8] & (uint8)(1u << (i % 8))) == 0)
			continue;
		/* exact equality — the floor must have landed exactly */
		if (cluster_membership_get_last_admitted_incarnation(i) != incarnation_by_node[i])
			return; /* did not land: no MEMBER */
	}
	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		if ((marker->admitted_nodes[i / 8] & (uint8)(1u << (i % 8))) != 0)
			cluster_membership_set_state(i, CLUSTER_MEMBER_MEMBER);
	/*
	 * RF-ROOT P9 verification / cold-formation cold-formation ruling:
	 * the marker admission IS the join gate's positive outcome — the
	 * COMMITTED marker is quorum-majority durable with our exact
	 * incarnation, so the gate must close OPEN (self_join_admitted =
	 * 1) and stop re-deciding.  Without this, the joiner gate's
	 * UNDECIDED arm re-ran every tick and overwrote MEMBER back to
	 * JOINING on the non-arbiter (the arbiter had latched decided via
	 * the rejoin arm earlier, which is why only one side wedged).
	 */
	joiner_gate_decided = true;
	ReconfigShmem->self_join_admitted = 1;
	ReconfigShmem->self_join_failed = 0;
	ReconfigShmem->self_join_deadline_us = 0;
	cold_formation_state.admission_done = true;
	ereport(LOG, (errmsg("cluster membership: node %d cold formation admitted "
						 "(generation %llu, %u members) via quorum-majority "
						 "formation marker",
						 cluster_node_id, (unsigned long long)marker->formation_generation,
						 (unsigned)marker->n_admitted)));
}

/*
 * Cold-formation state machine — one call per LMON tick while this node is
 * un-formed and the cluster is past INITIAL with an ABSENT peer.
 */
void
cluster_reconfig_cold_formation_tick(void)
{
	uint64 coboot_lo;
	uint64 coboot_hi;
	int arbiter;

	if (cold_formation_state.admission_done)
		return;

	/* 1. Observation window (three-state; quorum-stable, no survivor
	 * past INITIAL). */
	switch (cluster_reconfig_cold_formation_window(&coboot_lo, &coboot_hi)) {
	case CLUSTER_COLD_FORMATION_PENDING:
		cold_formation_state.observe_ticks = 0;
		cold_formation_state.observe_passed = false;
		cold_formation_state.arbiter_submitted = false;
		return;
	case CLUSTER_COLD_FORMATION_SURVIVOR:
		/* A formed survivor is live: never a cold formation.  (This
			 * node stays un-formed and the ordinary join/JCMK path takes
			 * over.) */
		cold_formation_state.observe_ticks = 0;
		cold_formation_state.observe_passed = false;
		cold_formation_state.arbiter_submitted = false;
		return;
	case CLUSTER_COLD_FORMATION_COBOOT:
		break;
	}
	if (++cold_formation_state.observe_ticks < CLUSTER_COLD_FORMATION_WINDOW_TICKS)
		return;
	if (!cold_formation_state.observe_passed) {
		cold_formation_state.observe_passed = true;
		cold_formation_state.coboot_lo = coboot_lo;
		cold_formation_state.coboot_hi = coboot_hi;
		cold_formation_state.arbiter_submitted = false;
		ereport(LOG, (errmsg("cluster membership: node %d cold-formation window "
							 "passed (co-boot set %llx/%llx)",
							 cluster_node_id, (unsigned long long)coboot_lo,
							 (unsigned long long)coboot_hi)));
	}

	{
		uint8 coboot_bmp[CLUSTER_FORMATION_MARKER_BITMAP_BYTES];

		memset(coboot_bmp, 0, sizeof(coboot_bmp));
		for (int i = 0; i < 64 && i < CLUSTER_MAX_NODES; i++)
			if ((coboot_lo & (UINT64_C(1) << i)) != 0)
				coboot_bmp[i / 8] |= (uint8)(1u << (i % 8));
		arbiter = dead_bitmap_lowest_bit_set(coboot_bmp);
	}
	if (arbiter < 0) {
		cold_formation_state.observe_passed = false;
		return;
	}

	if (arbiter == cluster_node_id) {
		/* ---- arbiter: submit the COMMITTED marker ---- */
		ClusterFormationCommitMarker marker;
		uint64 incarnation_by_node[CLUSTER_MAX_NODES];
		uint8 marker_bytes[CLUSTER_VOTING_SLOT_BYTES];
		uint64 generation;
		uint64 comp_seq;
		uint32 result;
		int i;

		if (cold_formation_state.arbiter_submitted) {
			comp_seq = pg_atomic_read_u64(&ReconfigShmem->formation_marker_completion_seq);
			if (comp_seq != cold_formation_state.arbiter_seq)
				return; /* qvotec still writing */
			result = pg_atomic_read_u32(&ReconfigShmem->formation_marker_result);
			if (result == 0) {
				/* Failed to reach majority — retry the whole submission. */
				cold_formation_state.arbiter_submitted = false;
				return;
			}
			/* Marker durable: admit from the marker we built. */
			memset(incarnation_by_node, 0, sizeof(incarnation_by_node));
			for (i = 0; i < CLUSTER_MAX_NODES; i++) {
				uint64 bit = i < 64 ? (cold_formation_state.coboot_lo & (UINT64_C(1) << i))
									: (cold_formation_state.coboot_hi & (UINT64_C(1) << (i - 64)));

				if (bit != 0)
					incarnation_by_node[i]
						= i == cluster_node_id
							  ? cluster_qvotec_get_self_incarnation()
							  : cluster_reconfig_cold_formation_observed_incarnation(i);
			}
			/* Re-decode the marker we submitted (self-proof). */
			if (!cluster_formation_marker_decode(
					ReconfigShmem->formation_marker_request.marker_bytes, &marker,
					incarnation_by_node))
				return; /* cannot happen: we encoded it */
			cluster_reconfig_cold_formation_admit(&marker, incarnation_by_node);
			return;
		}

		/* Build the marker: max existing generation + 1 (monotonic
		 * takeover), CURRENT boot incarnations (never inherited). */
		generation = pg_atomic_read_u64(&ReconfigShmem->formation_marker_max_generation) + 1;
		memset(&marker, 0, sizeof(marker));
		marker.magic = CLUSTER_FORMATION_MARKER_MAGIC;
		marker.version = CLUSTER_FORMATION_MARKER_VERSION;
		marker.phase = CLUSTER_FORMATION_MARKER_PHASE_COMMITTED;
		marker.formation_generation = generation;
		marker.formation_epoch = cluster_epoch_get_current();
		marker.arbiter_node = (uint64)cluster_node_id;
		marker.arbiter_incarnation = cluster_qvotec_get_self_incarnation();
		cold_formation_commit_nonce++;
		marker.commit_nonce = cold_formation_commit_nonce;
		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			uint64 bit = i < 64 ? (UINT64_C(1) << i)
								: (cold_formation_state.coboot_hi & (UINT64_C(1) << (i - 64)));

			if (i < 64) {
				if ((cold_formation_state.coboot_lo & bit) != 0)
					marker.admitted_nodes[i / 8] |= (uint8)(1u << (i % 8));
			} else if (bit != 0)
				marker.admitted_nodes[i / 8] |= (uint8)(1u << (i % 8));
		}
		memset(incarnation_by_node, 0, sizeof(incarnation_by_node));
		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			if ((marker.admitted_nodes[i / 8] & (uint8)(1u << (i % 8))) == 0)
				continue;
			incarnation_by_node[i] = i == cluster_node_id
										 ? cluster_qvotec_get_self_incarnation()
										 : cluster_reconfig_cold_formation_observed_incarnation(i);
			if (incarnation_by_node[i] == 0)
				return; /* peer not observed yet: retry next tick */
		}
		marker.n_admitted = (uint16)formation_bitmap_popcount(
			marker.admitted_nodes, CLUSTER_FORMATION_MARKER_BITMAP_BYTES);
		cluster_formation_marker_compute_crc(&marker);
		if (!cluster_formation_marker_encode(&marker, incarnation_by_node, marker_bytes))
			return;
		{
			uint8 targets[CLUSTER_FORMATION_MARKER_BITMAP_BYTES];

			memcpy(targets, marker.admitted_nodes, CLUSTER_FORMATION_MARKER_BITMAP_BYTES);
			if (!cluster_reconfig_formation_marker_submit(marker_bytes, targets))
				return;
		}
		cold_formation_state.arbiter_seq
			= pg_atomic_read_u64(&ReconfigShmem->formation_marker_request_seq);
		cold_formation_state.arbiter_submitted = true;
		ereport(LOG, (errmsg("cluster membership: node %d cold-formation arbiter "
							 "submitted formation marker (generation %llu, "
							 "members %u)",
							 cluster_node_id, (unsigned long long)generation,
							 (unsigned)marker.n_admitted)));
		return;
	}

	/* ---- non-arbiter: wait for our own region-7 slot marker ---- */
	{
		uint64 generation;
		uint64 incarnation_by_node[CLUSTER_MAX_NODES];
		int i;

		generation = pg_atomic_read_u64(&ReconfigShmem->observed_formation_marker_generation);
		if (generation == 0)
			return; /* arbiter has not written yet */
		memset(incarnation_by_node, 0, sizeof(incarnation_by_node));
		for (i = 0; i < CLUSTER_MAX_NODES; i++)
			incarnation_by_node[i]
				= pg_atomic_read_u64(&ReconfigShmem->observed_formation_marker_incarnation[i]);
		/* Our own incarnation in the marker must be EXACTLY ours. */
		if (incarnation_by_node[cluster_node_id] != cluster_qvotec_get_self_incarnation())
			return; /* not our marker: keep waiting */
		{
			ClusterFormationCommitMarker marker;

			memset(&marker, 0, sizeof(marker));
			marker.formation_generation = generation;
			marker.formation_epoch
				= pg_atomic_read_u64(&ReconfigShmem->observed_formation_marker_epoch);
			marker.arbiter_node
				= pg_atomic_read_u64(&ReconfigShmem->observed_formation_marker_arbiter_node);
			marker.arbiter_incarnation
				= pg_atomic_read_u64(&ReconfigShmem->observed_formation_marker_arbiter_incarnation);
			for (i = 0; i < CLUSTER_MAX_NODES; i++)
				if (incarnation_by_node[i] != 0)
					marker.admitted_nodes[i / 8] |= (uint8)(1u << (i % 8));
			marker.n_admitted = (uint16)formation_bitmap_popcount(
				marker.admitted_nodes, CLUSTER_FORMATION_MARKER_BITMAP_BYTES);
			cluster_reconfig_cold_formation_admit(&marker, incarnation_by_node);
		}
	}
}

/* Clear the per-process cold-formation latch pointer at exit. */
static void
formation_clear_qvotec_latch(int code, Datum arg)
{
	if (ReconfigShmem != NULL)
		ReconfigShmem->formation_qvotec_latch = NULL;
}

void
cluster_reconfig_publish_formation_qvotec_latch(struct Latch *latch)
{
	if (ReconfigShmem == NULL)
		return;
	if (ReconfigShmem->formation_qvotec_latch == NULL) {
		ReconfigShmem->formation_qvotec_latch = latch;
		before_shmem_exit(formation_clear_qvotec_latch, 0);
	}
}

static void
join_clear_qvotec_latch(int code, Datum arg)
{
	if (ReconfigShmem != NULL)
		ReconfigShmem->join_qvotec_latch = NULL;
}

void
cluster_reconfig_publish_join_qvotec_latch(struct Latch *latch)
{
	if (ReconfigShmem == NULL)
		return;
	ReconfigShmem->join_qvotec_latch = latch;
	on_shmem_exit(join_clear_qvotec_latch, (Datum)0);
}


/*
 * spec-5.15 D2/D4 — cluster_membership_seed_last_admitted_from_voting_disk.
 *
 *	Startup bring-up (INV-J7): scan region 3, and for each declared node N with a
 *	struct-valid COMMITTED join marker (node_id == slot) on a quorum-majority of
 *	disks, seed last_admitted[N] from the marker's admitted_incarnation — so a
 *	restart does not zero the floor and re-open the gate to a stale incarnation.
 *	The trust gate is phase==COMMITTED + majority + crc, NEVER an epoch compare.
 *
 *	Also resolves RC-5 / INV-J10 across restart: 5.13's leave-marker rebuild (run
 *	earlier in startup) may have re-set clean_departed[N] from the still-COMMITTED
 *	leave marker of a node that has since rejoined.  If this node's COMMITTED join
 *	marker is newer than that leave (admitted_epoch > clean_departed_epoch[N]),
 *	clear clean_departed[N] so N's later real fail-stop is not masked.  MUST run
 *	AFTER the leave rebuild (see the startup wiring).
 */
void
cluster_membership_seed_last_admitted_from_voting_disk(const int *fds, int n_disks)
{
	int s;
	uint32 majority;

	if (ReconfigShmem == NULL || fds == NULL || n_disks <= 0)
		return;

	majority = ((uint32)n_disks / 2u) + 1u;

	for (s = 0; s < CLUSTER_MAX_NODES; s++) {
		union {
			uint8 bytes[CLUSTER_VOTING_SLOT_BYTES];
			uint64 _align;
		} slot;
		ClusterJoinCommitMarker committed[CLUSTER_MAX_VOTING_DISKS];
		int n_committed = 0;
		int win;
		uint32 win_agree = 0;
		int d;

		if (cluster_conf_lookup_node(s) == NULL)
			continue;

		/* Collect every committed-basis marker for slot s across the disks. */
		for (d = 0; d < n_disks; d++) {
			ClusterJoinCommitMarker m;

			if (cluster_voting_disk_read_join_slot(fds[d], (uint32)s, slot.bytes)
				!= CLUSTER_VOTING_DISK_IO_OK)
				continue;
			memcpy(&m, slot.bytes, sizeof(m));
			if (!cluster_join_marker_is_committed_basis(&m, s))
				continue;
			committed[n_committed++] = m;
		}

		/*
		 * INV-J13: require a MAJORITY of the SAME commit (identical identity /
		 * nonce), not "any COMMITTED marker".  Two minority writes from
		 * different attempts (different coordinator / epoch) must not aggregate.
		 * Only a marker that actually reached a disk majority represents a real
		 * admission, so only it raises the monotonic floor (P1-3).  Shared
		 * selector — same logic as self-admit and qvotec peer-observe.
		 */
		win = cluster_join_marker_select_majority(committed, n_committed, majority, &win_agree);

		if (win >= 0 && committed[win].admitted_incarnation > 0) {
			uint64 win_incarnation = committed[win].admitted_incarnation;
			uint64 win_admitted_epoch = committed[win].admitted_epoch;

			LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
			cluster_membership_record_admitted(s, win_incarnation);
			LWLockRelease(&ReconfigShmem->lock);

			/* RC-5 / INV-J10 durable supersede: a rejoin newer than the leave
			 * clears clean_departed[N] so a survivor restart does not keep
			 * masking N's later real fail-stop. */
			if (cluster_reconfig_is_clean_departed(s)
				&& win_admitted_epoch > cluster_reconfig_get_clean_departed_epoch(s))
				cluster_reconfig_clear_clean_departed(s);

			ereport(LOG,
					(errmsg("cluster membership: seeded last_admitted[%d]=%llu from %u/%d durable "
							"COMMITTED join marker(s) of one commit (INV-J13)",
							s, (unsigned long long)win_incarnation, win_agree, n_disks)));
		}
	}
}


/* ============================================================
 * spec-5.15 D4 — two-phase online-join publication.
 * ============================================================
 */

void
cluster_reconfig_apply_join_as_coordinator(
	const uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES], int32 coordinator_node_id,
	const uint64 joiner_incarnations[CLUSTER_MAX_NODES])
{
	uint64 old_epoch, new_epoch;
	XLogRecPtr lsn;
	uint64 cssd_dead_generation;
	uint8 pending_dead[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	bool external_authorized[CLUSTER_MAX_NODES];
	ReconfigEvent evt;
	int i;

	if (!cluster_enabled || ReconfigShmem == NULL)
		return;

	CLUSTER_INJECTION_POINT("cluster-reconfig-join-pending-pre");
	memset(external_authorized, 0, sizeof(external_authorized));
	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		if (dead_bitmap_test_bit(join_bitmap, i))
			external_authorized[i]
				= cluster_reconfig_external_rejoin_authorized(i, joiner_incarnations[i]);

	cssd_dead_generation = cluster_cssd_get_dead_generation();
	LWLockAcquire(&ReconfigShmem->lock, LW_SHARED);
	memcpy(pending_dead, ReconfigShmem->last_applied.dead_bitmap, sizeof(pending_dead));
	for (i = 0; i < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; i++)
		pending_dead[i] |= ReconfigShmem->fast_rejoin_bitmap[i];
	LWLockRelease(&ReconfigShmem->lock);
	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		if (dead_bitmap_test_bit(join_bitmap, i) && external_authorized[i])
			pending_dead[i / 8] &= (uint8) ~(1u << (i % 8));

	/* Phase-1 epoch bump (regular advance new=old+1) + the same epoch side
	 * effects as the other coordinator paths. */
	cluster_epoch_advance_for_reconfig(&old_epoch, &new_epoch);
	lsn = GetXLogInsertRecPtr();
	cluster_epoch_set_changed_at_lsn((uint64)lsn);
	cluster_gcs_block_on_epoch_advance_exact(new_epoch, pending_dead);
	cluster_sinval_reset_all_on_reconfig();
	cluster_tt_status_flush_all((uint32)new_epoch);

	/* Mark joiners JOINING + pending (candidates, NOT members yet — INV-J2). */
	LWLockAcquire(&ReconfigShmem->lock, LW_EXCLUSIVE);
	cluster_write_fence_authority_cache_invalidate();
	/* RF-ROOT P6 (STOP-01 contract): a fast-rejoin eviction is the
	 * fail-stop-equivalent death of the PRIOR incarnation (P04 "exclude the
	 * prior incarnation first").  Carry the evicted set into the JOIN_PENDING
	 * dead set so the JOIN episode's barrier skips the joiner exactly like
	 * the fail-stop flow (its DONE is structurally impossible until the
	 * admission this very event feeds);  the COMMITTED clears it again. */
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (!dead_bitmap_test_bit(join_bitmap, i))
			continue;
		cluster_membership_set_state(i, CLUSTER_MEMBER_JOINING);
		dead_bitmap_set_bit(ReconfigShmem->pending_join_bitmap, i);
	}
	LWLockRelease(&ReconfigShmem->lock);

	memset(&evt, 0, sizeof(evt));
	evt.event_id = cluster_reconfig_compute_event_id_v2(RECONFIG_KIND_JOIN_PENDING, pending_dead,
														join_bitmap, joiner_incarnations,
														cssd_dead_generation);
	evt.coordinator_node_id = coordinator_node_id;
	evt.old_epoch = old_epoch;
	evt.new_epoch = new_epoch;
	memcpy(evt.dead_bitmap, pending_dead, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	memcpy(evt.join_bitmap, join_bitmap, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	evt.applied_at = GetCurrentTimestamp();
	evt.observer_role = CLUSTER_RECONFIG_OBSERVER_COORDINATOR;
	evt.cssd_dead_generation = cssd_dead_generation;
	evt.reconfig_kind = RECONFIG_KIND_JOIN_PENDING;

	join_prepare_stage.event = evt;
	memcpy(join_prepare_stage.join_bitmap, join_bitmap, sizeof(join_prepare_stage.join_bitmap));
	memcpy(join_prepare_stage.joiner_incarnations, joiner_incarnations,
		   sizeof(join_prepare_stage.joiner_incarnations));
	join_prepare_stage.next_node = 0;
	join_prepare_stage.submitted = false;
	join_prepare_stage.async.has_staged_event = true;
	(void)cluster_reconfig_poll_join_prepare_stage();
}

bool
cluster_reconfig_commit_member(int32 node_id, uint64 admitted_incarnation)
{
	ClusterJoinCommitMarker m;
	bool external_required;
	PgracExternalFenceDenyReason reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;

	if (!cluster_enabled || ReconfigShmem == NULL)
		return false;
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return false;
	/*
	 * Hardening v1.0.4 (P2 serialization, defense re-check at the hard-commit point):
	 * resolve the residual race where drive_joins started before a clean leave became
	 * active on this node.  If a clean leave is active now, do NOT commit the join
	 * (which would bump the epoch and wedge the leaver, P2).  Return false so the
	 * coordinator retries next tick once the leave finishes; the joiner stays JOINING.
	 */
	if (cluster_clean_leave_in_progress())
		return false;
	if (join_commit_stage.async.has_staged_event)
		return false;

	CLUSTER_INJECTION_POINT("cluster-reconfig-join-commit-pre");

	/*
	 * ① COMMITTED marker majority-durable = the commit point (P1-r5).  Written
	 * BEFORE the publish; if it does not reach a disk majority we FAIL CLOSED —
	 * no publish, no MEMBER, no gate-open; the joiner stays JOINING and the next
	 * tick retries (or times out joiner-side -> REJECT).
	 */
	memset(&m, 0, sizeof(m));
	m.magic = CLUSTER_JCMK_MAGIC;
	m.version = CLUSTER_JCMK_VERSION;
	m.node_id = node_id;
	m.phase = CLUSTER_JCMK_PHASE_COMMITTED;
	m.admitted_incarnation = admitted_incarnation;
	m.generation = admitted_incarnation;
	m.admitted_epoch = cluster_epoch_get_current() + 1; /* the epoch we publish below */
	m.supersedes_leave_epoch = cluster_reconfig_get_clean_departed_epoch(node_id);
	/*
	 * INV-J13 (Hardening v1.1): per-attempt nonce so the majority judgement
	 * groups by full commit identity.  Computed ONCE here -> all disks of THIS
	 * attempt share it; a later attempt (>= 1 LMON tick away -> distinct µs
	 * timestamp) or a different coordinator (distinct node_id high bits) gets a
	 * distinct nonce, so two minority writes from different attempts cannot be
	 * mis-counted as one majority (P1-3).  No new shmem field needed.
	 */
	m.commit_nonce = ((uint64)cluster_node_id << 56) ^ (uint64)GetCurrentTimestamp();
	cluster_join_marker_compute_crc(&m);

	/* STOP04 §11.9 C: after the fully normalized COMMITTED candidate exists,
	 * consume the exact READY operation once and only then expose the JCMK to
	 * qvotec.  No marker mailbox operation may precede this call. */
	external_required = cluster_reconfig_external_rejoin_required(node_id);
	if (external_required) {
		ClusterExternalRejoinSlot *slot = &external_rejoin_slots[node_id];
		ClusterReconfigRejoinPendingSnapshotV1 current_pending;

		memset(&current_pending, 0, sizeof(current_pending));
		if (slot->op == NULL || slot->phase != CLUSTER_EXTERNAL_REJOIN_COMMIT_READY
			|| slot->candidate_incarnation != admitted_incarnation
			|| !cluster_reconfig_rejoin_pending_snapshot(&slot->failure, admitted_incarnation,
														 &current_pending)
			|| memcmp(&current_pending, &slot->commit_pending, sizeof(current_pending)) != 0
			|| !cluster_external_fence_rejoin_consume_nowait(slot->op, &current_pending, &m,
															 &reason)) {
			cluster_reconfig_external_rejoin_release_slot(node_id);
			return false;
		}
		slot->phase = CLUSTER_EXTERNAL_REJOIN_COMMITTING;
	}
	cluster_marker_async_init(&join_commit_stage.async);
	cluster_marker_async_init(&join_commit_stage.fence_async);
	join_commit_stage.marker = m;
	memset(&join_commit_stage.fence_marker, 0, sizeof(join_commit_stage.fence_marker));
	memset(&join_commit_stage.event, 0, sizeof(join_commit_stage.event));
	join_commit_stage.expected_last_event_id = 0;
	join_commit_stage.node_id = node_id;
	join_commit_stage.admitted_incarnation = admitted_incarnation;
	join_commit_stage.async.has_staged_event = true;
	join_commit_stage.async.staged_expect_epoch = m.admitted_epoch;
	join_commit_stage.submitted = false;
	join_commit_stage.fence_ready = false;
	join_commit_stage.external_rejoin_consumed = external_required;
	(void)cluster_reconfig_poll_join_commit_stage();

	return false;
}


/*
 * Step 2 D4 — cluster_reconfig_check_pending_in_proc_interrupts.
 *
 *	  Called from tcop/postgres.c::ProcessInterrupts after
 *	  cluster_fence_check_interrupts.  PG's ProcessInterrupts already
 *	  returns early when CritSectionCount > 0, so the I6 commit-
 *	  durable safety guard (P1.5) is partially enforced by PG itself.
 *	  We additionally absorb when IsTransactionState() is false (idle /
 *	  post-commit cleanup completed) or when no top-level xid has been
 *	  assigned yet (read-only transaction so far) to avoid 53R60 firing
 *	  on non-writes.
 *
 *	  Read-clear-then-decide pattern (per Q5 A' + spec-2.28 §3.7 C4):
 *	    1. cheap pre-check on sig_atomic_t (avoid hot-loop write)
 *	    2. clear flag BEFORE GUC / tx-state checks (prevents stale
 *	       pending after disable + re-enable + new tx)
 *	    3. decide whether to ereport based on GUC + writable tx state
 *	       + quorum state
 *
 *	  Error code routing (spec-2.29 §2.4):
 *	    - 53R50 ERRCODE_CLUSTER_QUORUM_LOST_BACKEND  — not in_quorum
 *	    - 53R60 ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS — in_quorum + epoch changed
 */
ClusterReconfigVerdict
cluster_reconfig_classify_verdict(bool touched, bool has_top_xid, bool in_quorum)
{
	/* touched (read OR write): abort; lost quorum escalates to terminal. */
	if (touched)
		return in_quorum ? RECONFIG_VERDICT_ABORT_TOUCHED : RECONFIG_VERDICT_ABORT_QUORUM;

	/* non-touched read-only: absorb (INV-TP5).  Even on lost quorum a
	 * read-only tx is not aborted here — the spec-2.28 fence path owns that. */
	if (!has_top_xid)
		return RECONFIG_VERDICT_ABSORB;

	/* non-touched writable: 53R60 normally, 53R50 if quorum was lost. */
	return in_quorum ? RECONFIG_VERDICT_ABORT_RECONFIG : RECONFIG_VERDICT_ABORT_QUORUM;
}


void
cluster_reconfig_check_pending_in_proc_interrupts(void)
{
	ReconfigEvent ev;
	bool touched;
	ClusterReconfigVerdict verdict;

	if (!cluster_enabled)
		return;

	if (cluster_reconfig_start_pending == 0)
		return; /* hot-path early return */

	cluster_reconfig_start_pending = false; /* read-clear FIRST */

	/* I6:  PG ProcessInterrupts already guards CritSectionCount > 0
	 * (postgres.c top of function).  We add IsTransactionState absorb
	 * to silently no-op on idle / post-commit cleanup tail. */
	if (!IsTransactionState())
		return;

	cluster_reconfig_get_last_event(&ev); /* shared-lock copy */

	/*
	 * spec-5.14 D4 — fold any exited parallel workers' touches into this
	 * leader's bitmap (Q7) before deciding, then test touched ∩ dead.  A
	 * touched transaction (read OR write) aborts, breaking the no-top-xid
	 * read-only absorb below and closing the read-side 8.A hole (INV-TP2);
	 * a non-touched transaction keeps the unchanged spec-2.29 behaviour so an
	 * innocent local-only read-only transaction is never killed (INV-TP5).
	 */
	cluster_touched_peers_merge_active_parallel_workers();
	touched = (ev.reconfig_kind != RECONFIG_KIND_NONE)
			  && cluster_touched_peers_intersects(ev.dead_bitmap);

	/*
	 * spec-5.13 S6 (CL-I12) — touched-tx drain-grace dispatch by reconfig_kind.
	 * FAIL_STOP: the departed member's volatile state may be torn/lost, so a
	 * survivor tx that touched it MUST abort (40R01, the classify_verdict path
	 * below).  CLEAN_LEAVE: the leaving member flushed all its dirty blocks to
	 * shared storage before the commit, so the data is PRESERVED — a touched
	 * survivor tx is NOT aborted; it continues (drain-grace).  Its reads of any
	 * leaving-node block are gated by the CL-I5 serve-gate
	 * (cluster_clean_leave_block_serve_gate_allows in the GCS block-serve path):
	 * fail-closed (53R62 retry) until the leave commits + the cache invalidates,
	 * then it reads the just-flushed current.  This is the spec-5.14 Q1=B
	 * consumer contract: FAIL_STOP → abort (data lost); CLEAN_LEAVE →
	 * wait-then-continue (data preserved).
	 */
	if (touched && ev.reconfig_kind == RECONFIG_KIND_CLEAN_LEAVE) {
		if (cluster_reconfig_note_clean_leave_drain_grace() == 0)
			ereport(LOG,
					(errmsg("cluster clean-leave: in-flight transactions that touched the "
							"leaving node continue under drain-grace (data preserved); reads of "
							"leaving-node blocks are serve-gated until the leave commits")));
		return; /* ABSORB — drain-grace, NOT 40R01 */
	}

	/* diag (default off): dump this tx's touched-set hex on any touched abort. */
	if (touched && cluster_touched_peers_trace) {
		char hexbuf[24];

		cluster_touched_peers_self_hex(hexbuf, sizeof(hexbuf));
		ereport(LOG, (errmsg("cluster fail-stop touched-set (low 64 nodes): %s", hexbuf)));
	}

	verdict = cluster_reconfig_classify_verdict(
		touched, TransactionIdIsValid(GetTopTransactionIdIfAny()), cluster_qvotec_in_quorum());

	switch (verdict) {
	case RECONFIG_VERDICT_ABSORB:
		return;

	case RECONFIG_VERDICT_ABORT_TOUCHED:
		/* L213: LOG once per cold start, not per aborted backend. */
		if (cluster_reconfig_note_touched_abort() == 0)
			ereport(LOG, (errmsg("cluster fail-stop: aborting in-flight transactions that "
								 "consumed volatile state from a failed cluster member")));
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_RECONFIG_ABORT), /* 40R01, Class 40 retry-safe */
						errmsg("transaction aborted: cluster member fail-stop during "
							   "reconfiguration"),
						errdetail("this transaction read or held volatile state from a node that "
								  "fail-stopped"),
						errhint("retry the transaction;affected resources will be remastered")));
		break;

	case RECONFIG_VERDICT_ABORT_QUORUM:
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_QUORUM_LOST_BACKEND),
						errmsg("transaction aborted: cluster quorum lost during reconfig"),
						errhint("the cluster lost majority quorum;all uncommitted writes "
								"have been rolled back;retry after quorum recovery")));
		break;

	case RECONFIG_VERDICT_ABORT_RECONFIG:
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
						errmsg("transaction aborted: cluster reconfiguration in progress"),
						errhint("cluster membership changed during your transaction;"
								" the transaction was aborted before commit;retry is safe")));
		break;
	}
}


/* ============================================================
 * Step 3 D5b — SRF body for pg_cluster_reconfig_state view.
 *
 *	P2.9 always-1-row contract:  never-applied state surfaces as
 *	event_id=0 + observer_role='none' + applied_at NULL.  Disabled
 *	cluster path (cluster.enabled=off) returns 0 rows so that
 *	observability tooling can distinguish "feature off" from
 *	"feature on, no event yet".
 *
 *	Columns (9):  event_id int8 / coordinator_node_id int4 /
 *	old_epoch int8 / new_epoch int8 / dead_bitmap text /
 *	applied_at timestamptz / observer_role text /
 *	event_seq int8 / cssd_dead_generation int8
 * ============================================================
 */

static const char *
reconfig_observer_role_to_string(int32 role)
{
	switch (role) {
	case CLUSTER_RECONFIG_OBSERVER_COORDINATOR:
		return "coordinator";
	case CLUSTER_RECONFIG_OBSERVER_SURVIVOR:
		return "survivor";
	case CLUSTER_RECONFIG_OBSERVER_NONE:
	default:
		return "none";
	}
}


/* spec-5.14 D6 — render ReconfigEvent.reconfig_kind for the SRF view. */
static const char *
reconfig_kind_to_string(uint8 kind)
{
	switch (kind) {
	case RECONFIG_KIND_FAIL_STOP:
		return "fail_stop";
	case RECONFIG_KIND_CLEAN_LEAVE:
		return "clean_leave";
	case RECONFIG_KIND_JOIN_PENDING:
		return "join_pending"; /* spec-5.15 D6 */
	case RECONFIG_KIND_JOIN_COMMITTED:
		return "join_committed"; /* spec-5.15 D6 */
	case RECONFIG_KIND_NODE_REMOVED:
		return "node_removed"; /* spec-5.18 D3 */
	case RECONFIG_KIND_NONE:
	default:
		return "none";
	}
}

/* spec-5.15 D6 — render ClusterMembershipState for pg_cluster_membership. */
static const char *
membership_state_to_string(ClusterMembershipState st)
{
	switch (st) {
	case CLUSTER_MEMBER_ABSENT:
		return "absent";
	case CLUSTER_MEMBER_DEAD:
		return "dead";
	case CLUSTER_MEMBER_JOINING:
		return "joining";
	case CLUSTER_MEMBER_MEMBER:
		return "member";
	case CLUSTER_MEMBER_REJECTED:
		return "rejected";
	case CLUSTER_MEMBER_REMOVED:
		return "removed"; /* spec-5.18 D4 */
	default:
		return "unknown";
	}
}


static text *
reconfig_dead_bitmap_to_hex_text(const uint8 *bmp)
{
	/* "0x" + 32 hex digits + NUL = 35 bytes. */
	char buf[40];
	int i;

	buf[0] = '0';
	buf[1] = 'x';
	for (i = 0; i < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; i++)
		snprintf(buf + 2 + (i * 2), 3, "%02x", bmp[i]);
	buf[2 + CLUSTER_RECONFIG_DEAD_BITMAP_BYTES * 2] = '\0';
	return cstring_to_text(buf);
}


Datum
cluster_get_reconfig_state(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	ReconfigEvent evt;
	Datum values[10];
	bool nulls[10];

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

	if (!cluster_enabled)
		return (Datum)0; /* disabled — 0 rows */

	cluster_reconfig_get_last_event(&evt);

	memset(nulls, false, sizeof(nulls));

	values[0] = Int64GetDatum((int64)evt.event_id);
	values[1] = Int32GetDatum(evt.coordinator_node_id);
	values[2] = Int64GetDatum((int64)evt.old_epoch);
	values[3] = Int64GetDatum((int64)evt.new_epoch);
	values[4] = PointerGetDatum(reconfig_dead_bitmap_to_hex_text(evt.dead_bitmap));

	if (evt.applied_at == 0)
		nulls[5] = true; /* never-applied: applied_at NULL */
	else
		values[5] = TimestampTzGetDatum(evt.applied_at);

	values[6]
		= PointerGetDatum(cstring_to_text(reconfig_observer_role_to_string(evt.observer_role)));
	values[7] = Int64GetDatum((int64)evt.event_seq);
	values[8] = Int64GetDatum((int64)evt.cssd_dead_generation);
	/* spec-5.14 D6 — fail-stop vs clean-leave membership-event kind. */
	values[9] = PointerGetDatum(cstring_to_text(reconfig_kind_to_string(evt.reconfig_kind)));

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	return (Datum)0;
}


/*
 * spec-5.15 D6 — pg_cluster_membership SRF.  One row per declared node showing
 * the decision-SSOT membership state + the incarnation floor / observed values.
 */
Datum
cluster_get_membership(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	int i;

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

	if (!cluster_enabled)
		return (Datum)0; /* disabled — 0 rows */

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		Datum values[8];
		bool nulls[8];
		uint64 presented = 0;

		if (cluster_conf_lookup_node(i) == NULL)
			continue; /* only declared nodes */

		(void)cluster_reconfig_get_observed_slot(i, &presented, NULL);

		memset(nulls, false, sizeof(nulls));
		values[0] = Int32GetDatum(i);
		values[1] = BoolGetDatum(true); /* declared (only declared rows emitted) */
		values[2] = PointerGetDatum(
			cstring_to_text(membership_state_to_string(cluster_membership_get_state(i))));
		values[3] = Int64GetDatum((int64)presented);
		values[4] = Int64GetDatum((int64)cluster_membership_get_last_admitted_incarnation(i));
		values[5] = Int64GetDatum((int64)cluster_reconfig_get_observed_epoch(i));
		/* spec-5.18 D15: +2 cols — permanently-removed flag + removal epoch. */
		values[6] = BoolGetDatum(cluster_reconfig_is_removed(i));
		values[7] = Int64GetDatum((int64)cluster_reconfig_get_removed_epoch(i));

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	return (Datum)0;
}


#else /* !USE_PGRAC_CLUSTER */

/*
 * Disable-cluster stubs.  Same symbol surface so envelope receive
 * paths + LMON tick wiring + ProcessInterrupts integration compile
 * cleanly in both modes.  All stubs are silent no-ops.
 */

Size
cluster_reconfig_shmem_size(void)
{
	return 0;
}

void
cluster_reconfig_shmem_init(void)
{}

void
cluster_reconfig_shmem_register(void)
{}

void
cluster_reconfig_get_last_event(ReconfigEvent *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(ReconfigEvent));
}

void
cluster_reconfig_publish_event(const ReconfigEvent *evt pg_attribute_unused())
{}

void
cluster_reconfig_lmon_tick(void)
{}

void
cluster_reconfig_lmon_replacement_admit_tick(void)
{}

void
cluster_reconfig_lmon_replacement_ready_tick(void)
{}

void
cluster_reconfig_lmon_replacement_closed_tick(void)
{}

ClusterReplacementCommittedClosedPublishResultV1
cluster_reconfig_lmon_publish_replacement_committed_closed(
	uint64 authority_request_seq pg_attribute_unused(),
	uint64 marker_request_seq pg_attribute_unused())
{
	return CLUSTER_REPLACEMENT_CLOSED_INVALID;
}

void
cluster_reconfig_broadcast_local_procsig(void)
{}

void
cluster_reconfig_apply_epoch_bump_as_coordinator(
	const uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] pg_attribute_unused(),
	int32 coordinator_node_id pg_attribute_unused(),
	uint64 cssd_dead_generation pg_attribute_unused())
{}

void
cluster_reconfig_check_pending_in_proc_interrupts(void)
{}

int
cluster_reconfig_compute_join_bitmap(uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES])
{
	if (join_bitmap != NULL)
		memset(join_bitmap, 0, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	return 0;
}

uint64
cluster_reconfig_compute_event_id_v2(
	uint8 reconfig_kind pg_attribute_unused(),
	const uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] pg_attribute_unused(),
	const uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] pg_attribute_unused(),
	const uint64 joiner_incarnations[CLUSTER_MAX_NODES] pg_attribute_unused(),
	uint64 cssd_dead_generation pg_attribute_unused())
{
	return 0;
}

void
cluster_reconfig_record_observed_slot(int32 node_id pg_attribute_unused(),
									  uint64 incarnation pg_attribute_unused(),
									  uint64 generation pg_attribute_unused(),
									  uint64 epoch pg_attribute_unused())
{}

bool
cluster_reconfig_get_observed_slot(int32 node_id pg_attribute_unused(), uint64 *incarnation,
								   uint64 *generation)
{
	if (incarnation != NULL)
		*incarnation = 0;
	if (generation != NULL)
		*generation = 0;
	return false;
}

uint64
cluster_reconfig_get_observed_epoch(int32 node_id pg_attribute_unused())
{
	return 0;
}

void
cluster_reconfig_record_observed_fresh_alive(int32 node_id pg_attribute_unused(),
											 bool fresh_alive pg_attribute_unused())
{}

bool
cluster_reconfig_get_observed_fresh_alive(int32 node_id pg_attribute_unused())
{
	return false;
}

void
cluster_reconfig_bootstrap_publish_begin(void)
{}

void
cluster_reconfig_bootstrap_publish_in_quorum(bool in_quorum pg_attribute_unused())
{}

void
cluster_reconfig_bootstrap_publish_end(void)
{}

bool
cluster_reconfig_bootstrap_proof_node(int32 node_id pg_attribute_unused(),
									  uint64 *out_incarnation pg_attribute_unused())
{
	return false;
}

ClusterJoinMarkerSubmitResult
cluster_reconfig_submit_join_marker(int32 target_node pg_attribute_unused(),
									const ClusterJoinCommitMarker *m pg_attribute_unused())
{
	return CLUSTER_JOIN_MARKER_SUBMIT_FAILED;
}

bool
cluster_reconfig_submit_replacement_marker_v3_async(
	ClusterMarkerAsync *a pg_attribute_unused(), int32 target_node pg_attribute_unused(),
	const ClusterReplacementCommitMarkerV3 *marker pg_attribute_unused(),
	ClusterMarkerAsyncKind kind pg_attribute_unused(), TimestampTz now pg_attribute_unused())
{
	return false;
}

bool
cluster_reconfig_verify_replacement_committed_closed_async(
	ClusterMarkerAsync *a pg_attribute_unused(), int32 target_node pg_attribute_unused(),
	TimestampTz now pg_attribute_unused())
{
	return false;
}

bool
cluster_reconfig_join_qvotec_poll_pending(ClusterJoinMarkerMailboxOperationV1 *operation_out,
										  int32 *target_node_out,
										  void *write_slot512_out pg_attribute_unused())
{
	if (operation_out != NULL)
		*operation_out = CLUSTER_JOIN_MARKER_MAILBOX_WRITE_EXACT;
	if (target_node_out != NULL)
		*target_node_out = -1;
	return false;
}

void
cluster_reconfig_join_qvotec_complete(
	ClusterJoinMarkerMailboxOperationV1 operation pg_attribute_unused(),
	bool acked pg_attribute_unused(), const uint8 *verified_image96 pg_attribute_unused())
{}

bool
cluster_reconfig_formation_qvotec_poll_pending(
	ClusterFormationMarkerSubmitRequest *out pg_attribute_unused())
{
	return false;
}

void
cluster_reconfig_formation_qvotec_complete(bool success pg_attribute_unused())
{}

void
cluster_reconfig_formation_qvotec_note_max_generation(uint64 generation pg_attribute_unused())
{}

void
cluster_reconfig_formation_qvotec_publish_observed(
	const ClusterFormationCommitMarker *marker pg_attribute_unused(),
	const uint64 *incarnation_by_node pg_attribute_unused())
{}

void
cluster_reconfig_formation_qvotec_clear_observed(void)
{}

void
cluster_reconfig_publish_formation_qvotec_latch(struct Latch *latch pg_attribute_unused())
{}

void
cluster_reconfig_cold_formation_tick(void)
{}

bool
cluster_reconfig_qvotec_lifecycle_transition(ClusterQvotecMailbox *authority_mailbox
												 pg_attribute_unused(),
											 pg_atomic_uint32 *qvotec_status pg_attribute_unused(),
											 ClusterQvotecStatus next_status pg_attribute_unused())
{
	return false;
}

void
cluster_reconfig_publish_join_qvotec_latch(struct Latch *latch pg_attribute_unused())
{}

void
cluster_membership_seed_last_admitted_from_voting_disk(const int *fds pg_attribute_unused(),
													   int n_disks pg_attribute_unused())
{}

void
cluster_reconfig_apply_join_as_coordinator(
	const uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] pg_attribute_unused(),
	int32 coordinator_node_id pg_attribute_unused(),
	const uint64 joiner_incarnations[CLUSTER_MAX_NODES] pg_attribute_unused())
{}

bool
cluster_reconfig_commit_member(int32 node_id pg_attribute_unused(),
							   uint64 admitted_incarnation pg_attribute_unused())
{
	return false;
}

ClusterJoinGateVerdict
cluster_reconfig_self_join_gate_verdict(void)
{
	return CLUSTER_JOIN_GATE_ALLOW;
}

void
cluster_reconfig_note_self_admitted(uint64 admitted_epoch pg_attribute_unused())
{}

bool
cluster_reconfig_publish_replacement_member_closed(
	const ClusterReplacementEpisode *admitted_episode pg_attribute_unused())
{
	return false;
}

bool
cluster_reconfig_qvotec_observe_replacement_admitted(const int *fds pg_attribute_unused(),
													 int n_disks pg_attribute_unused(),
													 uint64 live_incarnation pg_attribute_unused())
{
	return false;
}

bool
cluster_reconfig_open_replacement_admission(const ClusterReplacementEpisode *expected_episode
												pg_attribute_unused(),
											uint32 expected_state_generation pg_attribute_unused())
{
	return false;
}

bool
cluster_reconfig_replacement_grd_basis_authorized(
	const ReconfigEvent *event pg_attribute_unused(),
	const ClusterReplacementEpisode *episode pg_attribute_unused(),
	const ClusterReplacementCommitMarkerV3 *committed_marker pg_attribute_unused(),
	int32 local_node_id pg_attribute_unused(),
	uint8 out_survivors[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] pg_attribute_unused(),
	uint64 *out_epoch pg_attribute_unused())
{
	return false;
}

bool
cluster_reconfig_lmon_snapshot_replacement_grd_basis(
	uint8 out_survivors[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] pg_attribute_unused(),
	uint64 *out_epoch pg_attribute_unused())
{
	return false;
}

bool
cluster_reconfig_lmon_observe_replacement_ready(
	const ClusterReplacementPhase3HandoffItem *item pg_attribute_unused())
{
	return false;
}

bool
cluster_reconfig_lmon_build_replacement_admitted(
	const ClusterEpochAuthorityValue *terminal_head pg_attribute_unused(),
	ClusterReplacementCommitMarkerV3 *out_marker pg_attribute_unused())
{
	return false;
}

bool
cluster_reconfig_lmon_finalize_replacement_admitted(
	const ClusterEpochAuthorityValue *terminal_head pg_attribute_unused(),
	const ClusterReplacementCommitMarkerV3 *admitted_marker pg_attribute_unused())
{
	return false;
}

bool
cluster_reconfig_lmon_snapshot_replacement_admitted(
	ClusterReplacementEpisode *out_episode pg_attribute_unused(),
	ClusterReplacementCommitMarkerV3 *out_marker pg_attribute_unused())
{
	return false;
}

bool
cluster_reconfig_snapshot_initial_clean_formation(ClusterInitialCleanFormationSnapshot *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_reconfig_lmon_snapshot_admitted_membership(uint64 *out_members_lo, uint64 *out_members_hi,
												   uint64 *out_formation_epoch)
{
	if (out_members_lo != NULL)
		*out_members_lo = 0;
	if (out_members_hi != NULL)
		*out_members_hi = 0;
	if (out_formation_epoch != NULL)
		*out_formation_epoch = 0;
	return false;
}

bool
cluster_reconfig_lmon_snapshot_r4_membership(ClusterR4MembershipSnapshot *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_reconfig_normal_stop_snapshot_admitted_membership(
	const ClusterSemanticActivationRecord *open_record pg_attribute_unused(),
	const uint8 *root_descriptor pg_attribute_unused(), uint64 *out_members_lo,
	uint64 *out_members_hi, uint64 *out_formation_epoch)
{
	return cluster_reconfig_lmon_snapshot_admitted_membership(out_members_lo, out_members_hi,
															  out_formation_epoch);
}

ClusterR4PrerequisiteSnapshot
cluster_reconfig_r4_prerequisite_snapshot(void)
{
	return (ClusterR4PrerequisiteSnapshot){
		.status = CLUSTER_R4_PREREQUISITE_RF_DEFERRED,
		.ready = false,
		.reserved0 = { 0, 0, 0 },
		.target_node_id = -1,
	};
}

bool
cluster_reconfig_r4_publish_ready(
	const ClusterR4PrerequisiteSnapshot *expected pg_attribute_unused())
{
	return false;
}

bool
cluster_reconfig_join_publish_proven(uint64 admitted_epoch pg_attribute_unused())
{
	return false;
}

bool
cluster_reconfig_bootstrap_quorum_at_initial(void)
{
	return false;
}

uint64
cluster_reconfig_get_join_pending_count(void)
{
	return 0;
}
uint64
cluster_reconfig_get_join_apply_count(void)
{
	return 0;
}
uint64
cluster_reconfig_get_join_reject_count(void)
{
	return 0;
}
uint64
cluster_reconfig_get_join_timeout_count(void)
{
	return 0;
}
uint64
cluster_reconfig_get_clean_departed_cleared_count(void)
{
	return 0;
}

#endif /* USE_PGRAC_CLUSTER */
