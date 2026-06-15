/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery_worker.c
 *	  pgrac online thread-recovery EXECUTOR (spec-4.11 D1, increment 3b-4b
 *	  Part 2).
 *
 *	  The reconfig FSM (lmon, Part 3) launches one per-episode dynamic
 *	  background worker per in-scope dead thread; each worker online-recovers
 *	  exactly one dead thread's WAL data + visibility through to shared storage
 *	  while the survivor holds the freeze window, then exits (BGW_NEVER_RESTART).
 *	  This file is the worker:
 *
 *	    cluster_thread_recovery_worker_run -- the testable core.  It reads the
 *	      per-thread replay slot (the launch marked it REPLAYING and stamped the
 *	      launch episode), enforces the L235 episode-staleness guard against the
 *	      live GRD recovery episode (a superseded worker aborts and publishes
 *	      nothing -- keep frozen), drives cluster_thread_recovery_replay_one
 *	      (which owns the data + visibility pass, durability barrier, and the
 *	      3-way authority publish on DONE), and writes the terminal slot state.
 *	      The slot is OBSERVABILITY + episode coordination ONLY -- the
 *	      authoritative unfreeze/serve gate reads the node-local merged.authority,
 *	      NOT this slot (spec-4.11 §2.4 Q4).
 *
 *	    cluster_thread_recovery_worker_main -- the dynamic-bgworker entry point
 *	      (BGWORKER_SHMEM_ACCESS only, like the spec-4.4 recovery worker: no
 *	      database connection -- replay_one works by raw relfilelocator over the
 *	      shared smgr + per-origin SLRU, exactly as the cold startup path does).
 *	      A thin wrapper: arm the abnormal-exit fail-closed callback, unblock
 *	      signals, run, log.
 *
 *	  Exit discipline: a before_shmem_exit callback flips a still-REPLAYING slot
 *	  to BLOCKED on any abnormal exit (a FATAL / SIGTERM -> proc_exit leaves the
 *	  recovery unfinished), so the dead thread stays frozen (8.A) -- but ONLY for
 *	  the launch epoch this worker owns, so a newer episode that has already
 *	  re-stamped the slot is never clobbered.
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

#include "cluster/cluster_conf.h"			   /* node_count / has_peers / CLUSTER_MAX_NODES */
#include "cluster/cluster_grd.h"			   /* live recovery episode epoch (L235)        */
#include "cluster/cluster_guc.h"			   /* cluster_online_thread_recovery (scope)    */
#include "cluster/cluster_thread_recovery.h"   /* slot helpers + replay_one + gates          */
#include "cluster/storage/cluster_shared_fs.h" /* shared backend (scope)                    */

/*
 * The launch epoch this worker owns, captured before the run so the
 * abnormal-exit callback fail-closes ONLY our own attempt (an epoch the
 * before_shmem_exit Datum -- a single uint16 -- cannot carry).  One recovery per
 * worker process, so a file static is exact.
 */
static uint64 worker_launch_epoch = 0;
static bool worker_armed = false;

/*
 * mark_blocked_on_exit -- before_shmem_exit callback.  Any exit that leaves the
 *	slot REPLAYING under OUR launch epoch (a FATAL / SIGTERM -> proc_exit before
 *	the normal terminal write) flips it to BLOCKED: the dead thread stays frozen
 *	(8.A), the survivor keeps running.  A newer episode that has re-stamped the
 *	slot (different epoch) is NOT clobbered.
 */
static void
mark_blocked_on_exit(int code, Datum arg)
{
	uint16 dead_tid = (uint16)DatumGetInt32(arg);
	ClusterThreadRecReplayState state;
	uint64 epoch;

	if (!worker_armed)
		return;
	if (!cluster_thread_recovery_replay_read(dead_tid, &state, &epoch))
		return;
	if (state == CLUSTER_THREADREC_REPLAY_REPLAYING && epoch == worker_launch_epoch)
		cluster_thread_recovery_replay_set_state(dead_tid, CLUSTER_THREADREC_REPLAY_BLOCKED);
}

/*
 * cluster_thread_recovery_worker_run -- online-recover ONE dead thread in the
 *	calling process (the testable core; the bgworker main and a TEST-ONLY SRF
 *	both call it).  Returns the replay_one verdict.  NOT_APPLICABLE also covers
 *	"the slot is not REPLAYING / the launch epoch is stale": the worker aborts,
 *	publishes nothing, and leaves the slot for the live episode (keep frozen).
 */
ClusterThreadRecResult
cluster_thread_recovery_worker_run(uint16 dead_tid)
{
	ClusterThreadRecReplayState state;
	uint64 launch_epoch;
	ClusterThreadRecResult res;

	/* The launch must have marked the slot REPLAYING; anything else means this
	 * spawn raced a reset or a newer launch -> moot (do not touch the slot). */
	if (!cluster_thread_recovery_replay_read(dead_tid, &state, &launch_epoch))
		return CLUSTER_THREADREC_NOT_APPLICABLE;
	if (state != CLUSTER_THREADREC_REPLAY_REPLAYING)
		return CLUSTER_THREADREC_NOT_APPLICABLE;

	/* L235 BEFORE: a stale launch epoch means the reconfig episode advanced past
	 * this worker.  Abort -- never run replay_one (which would publish authority)
	 * into a different episode.  Leave the slot for the new episode's launch. */
	if (cluster_thread_recovery_replay_epoch_aborts(launch_epoch,
													cluster_grd_redeclare_episode_epoch()))
		return CLUSTER_THREADREC_NOT_APPLICABLE;

	res = cluster_thread_recovery_replay_one(dead_tid, launch_epoch);

	/* L235 AFTER: if the episode advanced DURING the replay, do not write the
	 * terminal state -- the new episode owns the slot now.  Any authority
	 * replay_one published on DONE reflects real durable materialization and
	 * stays valid; the new episode re-evaluates (redo is idempotent). */
	if (cluster_thread_recovery_replay_epoch_aborts(launch_epoch,
													cluster_grd_redeclare_episode_epoch()))
		return res;

	cluster_thread_recovery_replay_set_state(dead_tid,
											 cluster_thread_recovery_worker_terminal_state(res));
	return res;
}

/*
 * cluster_thread_recovery_worker_main -- dynamic-bgworker entry point
 *	(InternalBGWorkers); main_arg carries the dead thread id.
 */
void
cluster_thread_recovery_worker_main(Datum main_arg)
{
	int32 dead_tid = DatumGetInt32(main_arg);
	ClusterThreadRecReplayState state;
	uint64 launch_epoch;
	ClusterThreadRecResult res;

	/* Returning from a bgworker entry point is a clean exit(0); cppcheck does
	 * not model proc_exit as noreturn, so guards use return (mirrors the
	 * spec-4.4 recovery worker). */
	if (dead_tid < XLP_THREAD_ID_FIRST_REAL || dead_tid > CLUSTER_WAL_THREAD_MAX)
		return;

	/* The launch marked the slot REPLAYING; if not, this spawn is moot. */
	if (!cluster_thread_recovery_replay_read((uint16)dead_tid, &state, &launch_epoch))
		return;
	if (state != CLUSTER_THREADREC_REPLAY_REPLAYING)
		return;

	/* Arm the abnormal-exit fail-closed BEFORE the run, so a FATAL inside
	 * replay_one leaves the dead thread frozen rather than half-recovered. */
	worker_launch_epoch = launch_epoch;
	worker_armed = true;
	before_shmem_exit(mark_blocked_on_exit, Int32GetDatum(dead_tid));

	/* The bgworker framework starts the entry point with signals blocked;
	 * unblock before any I/O so a SIGTERM during a stuck shared-storage read or
	 * a shutdown is delivered (the default bgworker_die handler FATALs, landing
	 * in the BLOCKED exit path above). */
	BackgroundWorkerUnblockSignals();

	res = cluster_thread_recovery_worker_run((uint16)dead_tid);

	/*
	 * worker_run returned NORMALLY: it already wrote the terminal slot state (DONE
	 * / BLOCKED) or DELIBERATELY left the slot REPLAYING for a superseding episode
	 * (a stale-epoch abort -> NOT_APPLICABLE).  Disarm the abnormal-exit callback so
	 * the clean exit below does NOT flip a deliberately-left REPLAYING slot to
	 * BLOCKED (that would diverge from worker_run's contract -- and from the SRF
	 * test core -- and would mis-record a superseded abort as a fail-closed).  The
	 * callback's sole purpose is the ABNORMAL exit (a FATAL inside worker_run, which
	 * never returns here): fail-closed the in-flight recovery to keep frozen (8.A).
	 */
	worker_armed = false;

	ereport(LOG, (errmsg("online thread recovery: dead thread %d -> %s", dead_tid,
						 res == CLUSTER_THREADREC_DONE
							 ? "done"
							 : (res == CLUSTER_THREADREC_BLOCKED ? "blocked (kept frozen)"
																 : "not applicable")),
				  res == CLUSTER_THREADREC_BLOCKED
					  ? errhint("The dead thread's resources stay frozen; check the shared WAL "
								"storage and cluster.thread_recovery_on_unrecoverable.")
					  : 0));
}

/*
 * register_one_worker -- register one dynamic bgworker for dead_tid (the launch
 *	side, mirroring the spec-4.4 cluster_recovery_workers_launch).  Returns false
 *	when registration fails (bgworker slots exhausted) so the caller can revert
 *	the slot and retry on a later tick.
 */
static bool
register_one_worker(uint16 dead_tid)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *handle = NULL;

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
	bgw.bgw_notify_pid = 0;

	return RegisterDynamicBackgroundWorker(&bgw, &handle);
}

/*
 * cluster_thread_recovery_launch_workers -- the lmon launch side (spec-4.11
 *	3b-4b Part 3).  Called each WAIT_CLUSTER tick of the reconfig FSM with the
 *	episode's dead-node bitmap (LSB = node 0) and the locked episode epoch.  For
 *	each in-scope dead origin not already handled this episode, stamp the slot
 *	REPLAYING and register a per-episode executor worker.
 *
 *	Out of scope (online_thread_recovery off by default / no shared backend /
 *	single node / >2-node) the SAME decide_scope as replay_one yields non-
 *	APPLICABLE -> a NO-OP, so the spec-4.6/4.7 reconfig FSM is unchanged (no
 *	regression -- the t/249-252 guarantee).  Idempotent: should_launch skips a
 *	slot already REPLAYING/DONE/BLOCKED at the current epoch, so a per-tick
 *	re-attempt never double-registers.
 */
void
cluster_thread_recovery_launch_workers(const uint64 *dead, int nwords, uint64 episode_epoch)
{
	ClusterThreadRecScope scope;
	bool shared_fs;
	int survivors;
	int node;
	int max_node;

	if (dead == NULL || nwords <= 0)
		return;

	/* Same scope decision as replay_one (kept in lockstep): out of scope is a
	 * no-op so the reconfig FSM is unchanged. */
	shared_fs = (cluster_shared_storage_backend == CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS);
	survivors = cluster_conf_node_count() - 1;
	scope = cluster_thread_recovery_decide_scope(cluster_online_thread_recovery,
												 cluster_conf_has_peers(), shared_fs, survivors);
	if (scope != CLUSTER_THREADREC_SCOPE_APPLICABLE)
		return;

	max_node = nwords * 64;
	if (max_node > CLUSTER_MAX_NODES)
		max_node = CLUSTER_MAX_NODES;

	for (node = 0; node < max_node; node++) {
		uint16 dead_tid;
		ClusterThreadRecReplayState state;
		uint64 slot_epoch;

		if ((dead[node / 64] & (UINT64CONST(1) << (node % 64))) == 0)
			continue;

		/* spec-4.1: thread_id = node_id + 1. */
		dead_tid = (uint16)(node + 1);

		/* No slot for this id (out of the real thread range) -> nothing to do. */
		if (!cluster_thread_recovery_replay_read(dead_tid, &state, &slot_epoch))
			continue;
		if (!cluster_thread_recovery_should_launch(scope, state, slot_epoch, episode_epoch))
			continue;

		/* Stamp REPLAYING + the launch episode BEFORE registering, so the worker
		 * sees a coherent slot the instant it starts (and the L235 guard has the
		 * launch epoch).  On a registration failure revert to IDLE so a later
		 * tick retries; the dead origin stays frozen meanwhile (the gate sees it
		 * unmaterialized). */
		cluster_thread_recovery_replay_mark_replaying(dead_tid, episode_epoch);
		if (!register_one_worker(dead_tid)) {
			cluster_thread_recovery_replay_set_state(dead_tid, CLUSTER_THREADREC_REPLAY_IDLE);
			ereport(WARNING,
					(errmsg("could not register online thread-recovery worker for dead thread %u",
							(unsigned)dead_tid),
					 errhint("Background worker slots are exhausted (max_worker_processes); the "
							 "dead thread stays frozen until recovery can run.")));
		}
	}
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
