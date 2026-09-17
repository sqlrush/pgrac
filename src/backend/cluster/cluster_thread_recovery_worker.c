/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery_worker.c
 *	  pgrac online thread-recovery EXECUTOR (spec-4.11 D1, contractb
 *	  Part 2).
 *
 *	  LMON owns one process-local dynamic-background-worker handle per in-scope
 *	  dead thread and relaunches only after exact matching STOPPED evidence.  This
 *	  file owns that A2 scheduler lifecycle and the executor entry point:
 *
 *	    thread_recovery_worker_run -- the executor core.  It reads the
 *	      per-thread replay slot (the launch marked it REPLAYING and stamped the
 *	      launch episode), enforces the L235 episode-staleness guard against the
 *	      live GRD recovery episode (a superseded worker aborts and publishes
 *	      nothing -- keep frozen).  RF-ROOT P3 deliberately stops at the strong
 *	      fail-closed seam until P4 supplies typed NeedSet/AdmissionSet evidence;
 *	      no replay or authority publication is reachable before then.
 *	      The slot is OBSERVABILITY + episode coordination ONLY -- the
 *	      authoritative unfreeze/serve gate reads the node-local merged.authority,
 *	      NOT this slot (spec-4.11 §2.4 Q4).
 *
 *	    cluster_thread_recovery_worker_main -- the dynamic-bgworker entry point
 *	      (BGWORKER_SHMEM_ACCESS only, like the spec-4.4 recovery worker: no
 *	      database connection).  A thin wrapper: validate the full bgw_extra duty
 *	      against the live slot, register authority cleanup, unblock signals, run,
 *	      and log.
 *
 *	  Exit discipline: the existing GRD cleanup callback releases this process's
 *	  holder state.  It never writes scheduler slots; LMON owns STOPPED reaping
 *	  and the sole matched REPLAYING->IDLE reset.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_thread_recovery_worker.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.11-thread-recovery.md (FROZEN v0.3)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "cluster/cluster_conf.h"			/* node_count / has_peers / CLUSTER_MAX_NODES */
#include "cluster/cluster_external_fence.h" /* STOP04 NeedSet/AdmissionSet */
#include "cluster/cluster_grd.h"			/* live recovery episode epoch (L235)        */
#include "cluster/cluster_guc.h"			/* cluster_online_thread_recovery (scope)    */
#include "cluster/cluster_ir.h"				/* spec-5.7 D8 — IR(X) recovery-owner gate    */
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_recovery_plan.h"		 /* RF-ROOT P7 G1b: pinned projection API      */
#include "cluster/cluster_semantic_activation.h" /* bit22 cutover latch (contract §B) */
#include "cluster/cluster_thread_recovery.h"	 /* slot helpers + replay_one + gates          */
#include "cluster/cluster_thread_recovery_authority.h"
#include "cluster/storage/cluster_shared_fs.h" /* shared backend (scope)                    */

/*
 * thread_recovery_worker_run -- online-recover ONE dead thread in the calling
 *	process.  Returns the replay verdict.  NOT_APPLICABLE also covers
 *	"the slot is not REPLAYING / the launch epoch is stale": the worker aborts,
 *	publishes nothing, and leaves the slot for the live episode (keep frozen).
 */
static ClusterThreadRecResult
thread_recovery_worker_run(const ClusterThreadRecLaunchEligibility *eligibility)
{
	ClusterFormationWitnessV1 *formation = NULL;
	PgracExternalFenceNeedSetV1 *needs = NULL;
	PgracExternalFenceAdmissionSetV1 *admissions = NULL;
	ClusterControlRootSnapshot root_snapshot;
	ClusterControlRootReadToken root_token;
	ClusterRecoverySerialRequest serial_request;
	ClusterRecoverySerialGuard serial_guard;
	ClusterWalRetentionInterval pin_interval;
	ClusterWalRetentionPinThreadRequest pin_request;
	ClusterWalRetentionPin *retention_pin = NULL;
	ClusterThreadRecoveryAuthorityV1 authority;
	PgracExternalFenceDenyReason deny_reason;
	ClusterFormationWitnessResult formation_result;
	PgracExternalFenceNeedSetResult need_result;
	PgracExternalFenceVerdict fence_verdict;
	ClusterRecoverySerialAcquireResult serial_result;
	ClusterRecoverySerialReleaseResult release_result;
	ClusterWalPinResult pin_result;
	ClusterWalrReleaseResult walr_release_result;
	ClusterControlRootResult root_result;
	ClusterThreadRecResult result;
	ClusterThreadRecReplayState state;
	uint64 launch_epoch;
	uint16 dead_tid;
	bool slot_read;
	int fence_timeout_ms;

	fence_timeout_ms = cluster_external_fence_acquire_timeout_ms;
	if (eligibility == NULL)
		return CLUSTER_THREADREC_DEFERRED;
	dead_tid = eligibility->origin_thread;

	/* The launch must have marked the slot REPLAYING; anything else means this
	 * spawn raced a reset or a newer launch -> moot (do not touch the slot). */
	slot_read = cluster_thread_recovery_replay_read(dead_tid, &state, &launch_epoch);
	if (!cluster_thread_recovery_worker_start_valid(eligibility, dead_tid, slot_read, state,
													launch_epoch))
		return CLUSTER_THREADREC_DEFERRED;

	/* L235 BEFORE: a stale launch epoch means the reconfig episode advanced past
	 * this worker.  Abort -- never run replay_one (which would publish authority)
	 * into a different episode.  Leave the slot for the new episode's launch. */
	if (cluster_thread_recovery_replay_epoch_aborts(launch_epoch,
													cluster_grd_redeclare_episode_epoch()))
		return CLUSTER_THREADREC_NOT_APPLICABLE;

	/* All waitable evidence is obtained before IR.  The current provider-0
	 * package deterministically stops at NeedSet/admit with BLOCKED and performs
	 * zero GES/replay/publish; a future certified provider uses this same order. */
	formation_result
		= cluster_formation_witness_build_wait(dead_tid, false, fence_timeout_ms, &formation);
	if (formation_result != CLUSTER_FORMATION_WITNESS_READY)
		return formation_result == CLUSTER_FORMATION_WITNESS_UNSTABLE
					   || formation_result == CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN
					   || formation_result == CLUSTER_FORMATION_WITNESS_IO_FAILED
				   ? CLUSTER_THREADREC_DEFERRED
				   : CLUSTER_THREADREC_BLOCKED;

	need_result = cluster_external_fence_need_set_build(&eligibility->duty, formation, &needs);
	if (need_result != PGRAC_EXTERNAL_FENCE_NEED_SET_OK) {
		cluster_formation_witness_destroy(&formation);
		return need_result == PGRAC_EXTERNAL_FENCE_NEED_SET_MEMBERSHIP_UNSTABLE
					   || need_result == PGRAC_EXTERNAL_FENCE_NEED_SET_FENCE_AUTHORITY_UNAVAILABLE
				   ? CLUSTER_THREADREC_DEFERRED
				   : CLUSTER_THREADREC_BLOCKED;
	}
	fence_verdict
		= cluster_external_fence_admit_set_wait(needs, formation, fence_timeout_ms, &admissions);
	if (fence_verdict != PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED) {
		cluster_external_fence_admission_set_release(&admissions);
		cluster_external_fence_need_set_release(&needs);
		cluster_formation_witness_destroy(&formation);
		return CLUSTER_THREADREC_BLOCKED;
	}
	if (!cluster_external_fence_need_set_revalidate_nowait(needs, formation, &deny_reason)
		|| !cluster_external_fence_revalidate_set_nowait(admissions, needs, formation,
														 &deny_reason)) {
		cluster_external_fence_admission_set_release(&admissions);
		cluster_external_fence_need_set_release(&needs);
		cluster_formation_witness_destroy(&formation);
		return CLUSTER_THREADREC_DEFERRED;
	}

	root_result = cluster_control_root_read_canonical(
		eligibility->duty.origin_thread_id, &eligibility->duty, CLUSTER_CONTROL_ROOT_READ_STRONG,
		&root_snapshot, &root_token);
	if ((root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 && root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		|| root_snapshot.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		|| memcmp(&root_snapshot.identity, &eligibility->duty, sizeof(eligibility->duty)) != 0) {
		cluster_external_fence_admission_set_release(&admissions);
		cluster_external_fence_need_set_release(&needs);
		cluster_formation_witness_destroy(&formation);
		return CLUSTER_THREADREC_DEFERRED;
	}
	if (!cluster_thread_recovery_pin_request_build_v1(dead_tid, &eligibility->duty, &root_snapshot,
													  &root_token, formation, needs, admissions,
													  &pin_interval, &pin_request)) {
		cluster_external_fence_admission_set_release(&admissions);
		cluster_external_fence_need_set_release(&needs);
		cluster_formation_witness_destroy(&formation);
		return CLUSTER_THREADREC_DEFERRED;
	}
	pin_result = cluster_wal_retention_pin_acquire(&pin_request, 1, &retention_pin);
	if (pin_result != CLUSTER_WAL_PIN_OK) {
		/* A non-NULL failed acquisition is release-uncertain.  Its ResourceOwner
		 * callback must retain the borrowed fence owners until process exit. */
		if (retention_pin != NULL)
			return CLUSTER_THREADREC_BLOCKED;
		cluster_external_fence_admission_set_release(&admissions);
		cluster_external_fence_need_set_release(&needs);
		cluster_formation_witness_destroy(&formation);
		return pin_result == CLUSTER_WAL_PIN_UNAVAILABLE || pin_result == CLUSTER_WAL_PIN_STALE
				   ? CLUSTER_THREADREC_DEFERRED
				   : CLUSTER_THREADREC_BLOCKED;
	}
	memset(&serial_request, 0, sizeof(serial_request));
	serial_request.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
	serial_request.duty = eligibility->duty;
	serial_request.expected_root_token = root_token;
	serial_request.formation = formation;
	serial_request.fence_need_set = needs;
	serial_request.fence_admission_set = admissions;
	serial_request.acquire_timeout_ms = fence_timeout_ms;
	serial_request.release_timeout_ms = fence_timeout_ms;
	serial_result = cluster_recovery_serial_acquire(&serial_request, &serial_guard);
	if (serial_result != CLUSTER_RECOVERY_SERIAL_GRANTED) {
		walr_release_result = cluster_wal_retention_pin_release(&retention_pin);
		if (walr_release_result != CLUSTER_WALR_RELEASE_CONFIRMED)
			return CLUSTER_THREADREC_BLOCKED;
		cluster_external_fence_admission_set_release(&admissions);
		cluster_external_fence_need_set_release(&needs);
		cluster_formation_witness_destroy(&formation);
		return serial_result == CLUSTER_RECOVERY_SERIAL_INTERNAL_FAILURE
				   ? CLUSTER_THREADREC_BLOCKED
				   : CLUSTER_THREADREC_DEFERRED;
	}
	pin_result = cluster_wal_retention_pin_bind_one(retention_pin, &serial_guard);
	if (pin_result != CLUSTER_WAL_PIN_OK) {
		release_result = cluster_recovery_serial_release(&serial_guard);
		walr_release_result = cluster_wal_retention_pin_release(&retention_pin);
		if (release_result != CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED
			|| walr_release_result != CLUSTER_WALR_RELEASE_CONFIRMED)
			return CLUSTER_THREADREC_BLOCKED;
		cluster_external_fence_admission_set_release(&admissions);
		cluster_external_fence_need_set_release(&needs);
		cluster_formation_witness_destroy(&formation);
		return pin_result == CLUSTER_WAL_PIN_STALE ? CLUSTER_THREADREC_DEFERRED
												   : CLUSTER_THREADREC_BLOCKED;
	}
	memset(&authority, 0, sizeof(authority));
	authority.duty = &eligibility->duty;
	authority.root_snapshot = &root_snapshot;
	authority.root_token = &root_token;
	authority.formation = formation;
	authority.fence_need_set = needs;
	authority.fence_admission_set = admissions;
	authority.retention_pin = retention_pin;
	authority.serial_guard = &serial_guard;
	if (cluster_thread_recovery_authority_revalidate_nowait_v1(&authority)
		!= CLUSTER_THREAD_AUTHORITY_OK) {
		cluster_write_fence_note_external_mutation_gate_blocked();
		result = CLUSTER_THREADREC_DEFERRED;
	} else {
		PG_TRY();
		{
			result = cluster_thread_recovery_replay_one(dead_tid, launch_epoch, &authority);
		}
		PG_CATCH();
		{
			release_result = cluster_recovery_serial_release(&serial_guard);
			walr_release_result = cluster_wal_retention_pin_release(&retention_pin);
			if (release_result == CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED
				&& walr_release_result == CLUSTER_WALR_RELEASE_CONFIRMED) {
				cluster_external_fence_admission_set_release(&admissions);
				cluster_external_fence_need_set_release(&needs);
				cluster_formation_witness_destroy(&formation);
			}
			PG_RE_THROW();
		}
		PG_END_TRY();
	}

	/* IR and WAL retention stay held through replay_one's publish.  A stale
	 * final bundle forbids DONE even if replay completed. */
	if (cluster_thread_recovery_authority_revalidate_nowait_v1(&authority)
		!= CLUSTER_THREAD_AUTHORITY_OK)
		result = CLUSTER_THREADREC_BLOCKED;
	release_result = cluster_recovery_serial_release(&serial_guard);
	walr_release_result = cluster_wal_retention_pin_release(&retention_pin);
	if (release_result != CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED
		|| walr_release_result != CLUSTER_WALR_RELEASE_CONFIRMED)
		return CLUSTER_THREADREC_BLOCKED;
	cluster_external_fence_admission_set_release(&admissions);
	cluster_external_fence_need_set_release(&needs);
	cluster_formation_witness_destroy(&formation);
	return result;
}

/*
 * cluster_thread_recovery_worker_main -- dynamic-bgworker entry point
 *	(InternalBGWorkers); main_arg carries the dead thread id.
 */
void
cluster_thread_recovery_worker_main(Datum main_arg)
{
	int32 dead_tid = DatumGetInt32(main_arg);
	ClusterThreadRecLaunchEligibility eligibility;
	ClusterThreadRecReplayState state = CLUSTER_THREADREC_REPLAY_IDLE;
	ClusterThreadRecReplayState terminal_state;
	ClusterThreadReplayMatchResult match_result;
	uint64 launch_epoch = 0;
	ClusterThreadRecResult res;
	bool slot_read;

	/* Returning from a bgworker entry point is a clean exit(0); cppcheck does
	 * not model proc_exit as noreturn, so guards use return (mirrors the
	 * spec-4.4 recovery worker). */
	if (dead_tid < XLP_THREAD_ID_FIRST_REAL || dead_tid > CLUSTER_WAL_THREAD_MAX)
		return;

	/* bgw_extra is a non-authoritative carrier.  Revalidate its full duty and
	 * exact attempt identity against the live slot before any authority acquire. */
	if (MyBgworkerEntry == NULL)
		return;
	memcpy(&eligibility, MyBgworkerEntry->bgw_extra, sizeof(eligibility));
	slot_read = cluster_thread_recovery_replay_read((uint16)dead_tid, &state, &launch_epoch);
	if (!cluster_thread_recovery_worker_start_valid(&eligibility, (uint16)dead_tid, slot_read,
													state, launch_epoch))
		return;

	/* Cleanup authority on every controlled exit; scheduler state is reaped only
	 * by the LMON owner after exact BGWH_STOPPED evidence. */
	before_shmem_exit(cluster_grd_cleanup_on_backend_exit_callback, 0);

	/* The bgworker framework starts the entry point with signals blocked;
	 * unblock before any I/O so a SIGTERM during a stuck shared-storage read or
	 * a shutdown is delivered (the default bgworker_die handler FATALs, landing
	 * in the BLOCKED exit path above). */
	BackgroundWorkerUnblockSignals();

	res = thread_recovery_worker_run(&eligibility);
	if (cluster_thread_recovery_worker_terminal_state(res, &terminal_state)) {
		match_result = cluster_thread_recovery_replay_transition_if_match(
			(uint16)dead_tid, eligibility.attempt_stamp, CLUSTER_THREADREC_REPLAY_REPLAYING,
			terminal_state);
		if (match_result != CLUSTER_THREADREC_MATCH_CHANGED)
			ereport(PANIC, (errmsg("online thread-recovery terminal slot transition failed"),
							errdetail("thread=%u stamp=" UINT64_FORMAT " result=%d match=%d",
									  (unsigned)dead_tid, eligibility.attempt_stamp, (int)res,
									  (int)match_result)));
	}

	ereport(
		LOG,
		(errmsg("online thread recovery: dead thread %d -> %s", dead_tid,
				res == CLUSTER_THREADREC_DONE
					? "done"
					: (res == CLUSTER_THREADREC_BLOCKED
						   ? "blocked (kept frozen)"
						   : (res == CLUSTER_THREADREC_DEFERRED ? "deferred" : "not applicable"))),
		 res == CLUSTER_THREADREC_BLOCKED
			 ? errhint("The dead thread's resources stay frozen; check the shared WAL "
					   "storage and cluster.thread_recovery_on_unrecoverable.")
			 : 0));
}

typedef struct ClusterThreadRecOwnedWorker {
	BackgroundWorkerHandle *handle;
	uint64 attempt_stamp;
	bool terminate_sent;
} ClusterThreadRecOwnedWorker;

static ClusterThreadRecOwnedWorker thread_recovery_owned[CLUSTER_WAL_THREAD_MAX + 1];

StaticAssertDecl(sizeof(ClusterThreadRecLaunchEligibility) <= BGW_EXTRALEN,
				 "thread recovery eligibility must fit bgw_extra");

static bool
thread_recovery_eligibility_valid(const ClusterThreadRecLaunchEligibility *eligibility)
{
	return eligibility != NULL
		   && cluster_thread_recovery_worker_start_valid(eligibility, eligibility->origin_thread,
														 true, CLUSTER_THREADREC_REPLAY_REPLAYING,
														 eligibility->attempt_stamp);
}

static bool
register_one_worker(const ClusterThreadRecLaunchEligibility *eligibility,
					BackgroundWorkerHandle **handle_out)
{
	BackgroundWorker bgw;
	MemoryContext old_context;
	bool registered;
	uint16 dead_tid;

	if (handle_out == NULL || !thread_recovery_eligibility_valid(eligibility))
		return false;
	*handle_out = NULL;
	dead_tid = eligibility->origin_thread;

	memset(&bgw, 0, sizeof(bgw));
	/* SHMEM_ACCESS only -- no database connection (replay_one works by raw
	 * relfilelocator over the shared smgr + per-origin SLRU). */
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = BgWorkerStart_PostmasterStart;
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	strlcpy(bgw.bgw_library_name, "postgres", sizeof(bgw.bgw_library_name));
	strlcpy(bgw.bgw_function_name, "cluster_thread_recovery_worker_main",
			sizeof(bgw.bgw_function_name));
	snprintf(bgw.bgw_name, sizeof(bgw.bgw_name), "pgrac thread recovery %u", (unsigned)dead_tid);
	strlcpy(bgw.bgw_type, "cluster thread recovery", sizeof(bgw.bgw_type));
	bgw.bgw_main_arg = Int32GetDatum((int32)dead_tid);
	memcpy(bgw.bgw_extra, eligibility, sizeof(*eligibility));
	bgw.bgw_notify_pid = 0;

	old_context = MemoryContextSwitchTo(TopMemoryContext);
	registered = RegisterDynamicBackgroundWorker(&bgw, handle_out);
	MemoryContextSwitchTo(old_context);
	return registered;
}

static void
thread_recovery_reap_one(uint16 dead_tid, bool *retained_out)
{
	ClusterThreadRecOwnedWorker *owned = &thread_recovery_owned[dead_tid];
	ClusterThreadRecReplayState state = CLUSTER_THREADREC_REPLAY_IDLE;
	ClusterThreadRecReapDecision decision;
	BgwHandleStatus status;
	uint64 slot_stamp = 0;
	pid_t pid = InvalidPid;
	bool slot_read = false;

	if (owned->handle == NULL)
		return;
	status = GetBackgroundWorkerPid(owned->handle, &pid);
	if (status == BGWH_STOPPED)
		slot_read = cluster_thread_recovery_replay_read(dead_tid, &state, &slot_stamp);
	decision = cluster_thread_recovery_reap_decide(status, slot_read, owned->attempt_stamp, state,
												   slot_stamp);
	if (decision == CLUSTER_THREADREC_REAP_RETAIN) {
		if (retained_out != NULL)
			*retained_out = true;
		return;
	}
	if (decision == CLUSTER_THREADREC_REAP_INVALID)
		ereport(FATAL, (errmsg("invalid online thread-recovery worker/slot relation"),
						errdetail("thread=%u owned_stamp=" UINT64_FORMAT
								  " status=%d slot_read=%s slot_state=%d slot_stamp=" UINT64_FORMAT,
								  (unsigned)dead_tid, owned->attempt_stamp, (int)status,
								  slot_read ? "true" : "false", (int)state, slot_stamp)));
	if (decision == CLUSTER_THREADREC_REAP_RESET_IDLE) {
		ClusterThreadReplayMatchResult match_result
			= cluster_thread_recovery_replay_transition_if_match(dead_tid, owned->attempt_stamp,
																 CLUSTER_THREADREC_REPLAY_REPLAYING,
																 CLUSTER_THREADREC_REPLAY_IDLE);

		if (match_result != CLUSTER_THREADREC_MATCH_CHANGED)
			ereport(FATAL,
					(errmsg("online thread-recovery STOPPED reap could not reset slot"),
					 errdetail("thread=%u stamp=" UINT64_FORMAT " match=%d", (unsigned)dead_tid,
							   owned->attempt_stamp, (int)match_result)));
	}
	pfree(owned->handle);
	memset(owned, 0, sizeof(*owned));
}

static void
thread_recovery_launch_one(const ClusterThreadRecLaunchEligibility *eligibility)
{
	ClusterThreadRecOwnedWorker *owned;
	ClusterThreadRecReplayState state;
	ClusterThreadReplayMatchResult match_result;
	uint64 slot_stamp;
	uint16 dead_tid = eligibility->origin_thread;

	owned = &thread_recovery_owned[dead_tid];
	if (owned->handle != NULL)
		return;
	if (!cluster_thread_recovery_replay_read(dead_tid, &state, &slot_stamp)
		|| state != CLUSTER_THREADREC_REPLAY_IDLE)
		ereport(FATAL, (errmsg("invalid online thread-recovery launch slot"),
						errdetail("thread=%u state=%d stamp=" UINT64_FORMAT, (unsigned)dead_tid,
								  (int)state, slot_stamp)));
	if (!cluster_thread_recovery_replay_mark_replaying(dead_tid, eligibility->attempt_stamp))
		ereport(FATAL, (errmsg("could not stamp online thread-recovery slot for dead thread %u",
							   (unsigned)dead_tid)));
	/*
	 * RF-ROOT P7 (contract §B): pin the canonical-root projection BEFORE
	 * the worker spawns — post-bit22 only (pre-bit22 replay_one derives
	 * its window from the registry directly under the gate idiom).  This
	 * LMON tick runs before the GRD episode freeze (cluster_lmon.c
	 * ordering: reconfig -> semantic -> thread_recovery -> grd) at zero
	 * resource locks; the worker (replay_one) then consumes ONLY the
	 * pinned fields and never re-acquires CF(S) inside the episode
	 * (follow-up item 2).  The launch attempt stamp is the projection's
	 * episode identity.  A pin failure leaves the projection absent ->
	 * the worker fails closed (window derivation BLOCKED).
	 */
	if (cluster_r4_bit22_cutover_active()) {
		(void)cluster_thread_recovery_pin_projection(dead_tid, eligibility->attempt_stamp);
	}
	if (register_one_worker(eligibility, &owned->handle)) {
		owned->attempt_stamp = eligibility->attempt_stamp;
		owned->terminate_sent = false;
		return;
	}

	match_result = cluster_thread_recovery_replay_transition_if_match(
		dead_tid, eligibility->attempt_stamp, CLUSTER_THREADREC_REPLAY_REPLAYING,
		CLUSTER_THREADREC_REPLAY_IDLE);
	if (match_result != CLUSTER_THREADREC_MATCH_CHANGED)
		ereport(FATAL, (errmsg("could not reset failed online thread-recovery launch"),
						errdetail("thread=%u stamp=" UINT64_FORMAT " match=%d", (unsigned)dead_tid,
								  eligibility->attempt_stamp, (int)match_result)));
	ereport(WARNING, (errmsg("could not register online thread-recovery worker for dead thread %u",
							 (unsigned)dead_tid),
					  errhint("Background worker slots are exhausted (max_worker_processes); the "
							  "dead thread stays frozen until recovery can run.")));
}

void
cluster_thread_recovery_lmon_tick(void)
{
	ClusterThreadRecScope scope;
	bool shared_fs;
	int survivors;
	uint16 dead_tid;

	for (dead_tid = XLP_THREAD_ID_FIRST_REAL; dead_tid <= CLUSTER_WAL_THREAD_MAX; dead_tid++)
		thread_recovery_reap_one(dead_tid, NULL);

	shared_fs = (cluster_shared_storage_backend == CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS);
	survivors = cluster_conf_node_count() - 1;
	scope = cluster_thread_recovery_decide_scope(cluster_online_thread_recovery,
												 cluster_conf_has_peers(), shared_fs, survivors);
	if (scope != CLUSTER_THREADREC_SCOPE_APPLICABLE)
		return;

	for (dead_tid = XLP_THREAD_ID_FIRST_REAL; dead_tid <= CLUSTER_WAL_THREAD_MAX; dead_tid++) {
		ClusterThreadRecLaunchEligibility eligibility;
		ClusterThreadRecOwnedWorker *owned = &thread_recovery_owned[dead_tid];

		memset(&eligibility, 0, sizeof(eligibility));
		if (!cluster_reconfig_thread_recovery_eligibility_consume(dead_tid, &eligibility))
			continue;
		if (!thread_recovery_eligibility_valid(&eligibility)
			|| eligibility.origin_thread != dead_tid)
			ereport(FATAL, (errmsg("invalid online thread-recovery launch eligibility"),
							errdetail("requested_thread=%u carrier_thread=%u stamp=" UINT64_FORMAT,
									  (unsigned)dead_tid, (unsigned)eligibility.origin_thread,
									  eligibility.attempt_stamp)));
		if (owned->handle != NULL) {
			if (owned->attempt_stamp != eligibility.attempt_stamp && !owned->terminate_sent) {
				TerminateBackgroundWorker(owned->handle);
				owned->terminate_sent = true;
			}
			continue;
		}
		thread_recovery_launch_one(&eligibility);
	}
}

void
cluster_thread_recovery_lmon_shutdown(void)
{
	TimestampTz deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 5000);
	uint16 dead_tid;

	for (dead_tid = XLP_THREAD_ID_FIRST_REAL; dead_tid <= CLUSTER_WAL_THREAD_MAX; dead_tid++) {
		ClusterThreadRecOwnedWorker *owned = &thread_recovery_owned[dead_tid];

		if (owned->handle != NULL && !owned->terminate_sent) {
			TerminateBackgroundWorker(owned->handle);
			owned->terminate_sent = true;
		}
	}
	for (;;) {
		TimestampTz now;
		bool retained = false;
		long wait_ms;
		int rc;

		for (dead_tid = XLP_THREAD_ID_FIRST_REAL; dead_tid <= CLUSTER_WAL_THREAD_MAX; dead_tid++)
			thread_recovery_reap_one(dead_tid, &retained);
		if (!retained)
			return;
		now = GetCurrentTimestamp();
		if (now >= deadline)
			ereport(
				FATAL,
				(errmsg("timed out reaping online thread-recovery workers during LMON shutdown")));
		wait_ms = (long)((deadline - now + INT64CONST(999)) / INT64CONST(1000));
		if (wait_ms > 50)
			wait_ms = 50;
		if (wait_ms < 1)
			wait_ms = 1;
		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, wait_ms,
					   WAIT_EVENT_CLUSTER_BGPROC_LMON_MAIN_LOOP);
		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
