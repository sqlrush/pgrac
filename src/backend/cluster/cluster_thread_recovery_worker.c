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

#include "cluster/cluster_grd.h"			 /* live recovery episode epoch (L235)   */
#include "cluster/cluster_thread_recovery.h" /* slot helpers + replay_one + gates     */

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

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
