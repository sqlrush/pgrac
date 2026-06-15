/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery_orchestrator.c
 *	  pgrac online thread-recovery ORCHESTRATOR (spec-4.11 D1, increment 3b-2).
 *
 *	  Increment 3a built the RMW data engine; 3b-1 built the R13-guarded data
 *	  driver.  This layer assembles a COMPLETE online recovery of one dead thread
 *	  and is the corruption-critical core of the feature:
 *
 *	    1. visibility pass -- the driver now runs the COMBINED engine
 *	       (cluster_thread_recovery_drive with a visibility ctx): each foreign
 *	       XACT/CLOG/MULTIXACT/COMMIT_TS record's commit/abort OUTCOME is diverted
 *	       to the per-origin store, so the survivor can judge the dead thread's
 *	       transactions.  Without it, recovered pages would be served with an
 *	       unknown commit state -> false-visible (8.A).
 *	    2. R14 -- the visibility apply writes the per-origin store from this
 *	       bgworker; the drive brackets it in an episode-fenced online-writer
 *	       scope so the historically startup-only writer assert admits it.
 *	    3. R13 -- the drive demotes a catchable ERROR (an online unmaterializable
 *	       record, or a barrier / publish I/O failure) to a result-returning
 *	       BLOCKED; the survivor keeps running (keep_frozen) unless the operator
 *	       set cluster.thread_recovery_on_unrecoverable=panic.
 *	    4. durability barrier (amend 2) -- on DONE, smgrimmedsync every touched
 *	       relation (cluster_fs write-back is not fsync'd and is invisible to the
 *	       survivor's checkpointer) and flush the per-origin outcome store BEFORE
 *	       publishing any authority, or a published "recovered" authority could
 *	       outlive un-fsync'd pages.
 *	    5. 3-way authority (Q4) -- only after a full DONE + durable barrier:
 *	       registry merge_recovered_lsn (the dead origin's own-bound skip) then
 *	       the node-local merged.authority (the reader/serving gate, written
 *	       LAST).  partial-apply (v0.3 P2): any failure before that LAST write
 *	       publishes no serving authority, so the thread stays frozen and never
 *	       serves a stale page (8.A).
 *
 *	  SCOPE (increment 3b-2).  replay_one is assembled but has NO live FSM caller
 *	  (3b-3 wires the GRD reconfig FSM + the D3 unfreeze gate that reads the
 *	  node-local authority this publishes).  The episode_epoch is threaded through
 *	  but its L235 abort-on-bump enforcement is 3b-3 (shmem replay-state).  The
 *	  window for replay_one(dead_tid, epoch) is a BASIC derivation from the
 *	  wal-state slot; the PRECISE validated-complete-record boundary (4.4
 *	  validated highest_lsn + torn-tail gate) is D4 (3b-4).  Both are 8.A-safe:
 *	  the engine never advances recovered_through past a complete record, and an
 *	  imprecise / short window fails closed.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_thread_recovery_orchestrator.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.11-thread-recovery.md (FROZEN v0.3)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/xlogdefs.h"
#include "miscadmin.h"
#include "utils/elog.h"

#include "port/atomics.h" /* replay-slot atomics + barriers (3b-4b)      */

#include "cluster/cluster_conf.h"			   /* node_count / has_peers (scope gate)        */
#include "cluster/cluster_elog.h"			   /* ERRCODE_CLUSTER_THREAD_RECOVERY_BLOCKED     */
#include "cluster/cluster_guc.h"			   /* GUCs: online flag + shared backend + policy */
#include "cluster/cluster_recovery_merge.h"	   /* node-local authority publish (online)       */
#include "cluster/cluster_recovery_plan.h"	   /* ClusterThreadReplaySlot + slot accessor     */
#include "cluster/cluster_remote_xact.h"	   /* per-origin outcome store flush              */
#include "cluster/cluster_thread_recovery.h"   /* engine / driver / pure gates                */
#include "cluster/cluster_wal_state.h"		   /* slot read + registry skip-bound publish     */
#include "cluster/storage/cluster_shared_fs.h" /* CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS         */

/*
 * Per-thread online replay-state helpers (spec-4.11 3b-4b).  The executor's
 * bookkeeping over the recovery-plan shmem slot reached through the accessor;
 * see cluster_thread_recovery.h for the contract.  OBSERVABILITY + episode
 * coordination ONLY (the reader gate uses merged.authority, §2.4 Q4).
 */
bool
cluster_thread_recovery_replay_mark_replaying(uint16 dead_tid, uint64 episode_epoch)
{
	ClusterThreadReplaySlot *slot = cluster_thread_recovery_replay_slot(dead_tid);

	if (slot == NULL)
		return false;
	/* Publish the launch episode BEFORE REPLAYING so a worker that observes
	 * REPLAYING also observes the epoch it was launched under (L235). */
	pg_atomic_write_u64(&slot->episode_epoch, episode_epoch);
	pg_write_barrier();
	pg_atomic_write_u32(&slot->state, (uint32)CLUSTER_THREADREC_REPLAY_REPLAYING);
	return true;
}

bool
cluster_thread_recovery_replay_set_state(uint16 dead_tid, ClusterThreadRecReplayState state)
{
	ClusterThreadReplaySlot *slot = cluster_thread_recovery_replay_slot(dead_tid);

	if (slot == NULL)
		return false;
	pg_atomic_write_u32(&slot->state, (uint32)state);
	return true;
}

bool
cluster_thread_recovery_replay_read(uint16 dead_tid, ClusterThreadRecReplayState *state_out,
									uint64 *epoch_out)
{
	ClusterThreadReplaySlot *slot = cluster_thread_recovery_replay_slot(dead_tid);
	uint32 st;

	if (slot == NULL)
		return false;
	st = pg_atomic_read_u32(&slot->state);
	pg_read_barrier();
	if (state_out != NULL)
		*state_out = (ClusterThreadRecReplayState)st;
	if (epoch_out != NULL)
		*epoch_out = pg_atomic_read_u64(&slot->episode_epoch);
	return true;
}

/*
 * cluster_thread_recovery_replay_one_window -- the orchestrator core, window
 *		EXPLICIT (spec-4.11 3b-2).  Online-recover ONE dead thread over
 *		[scan_lower, scan_upper]: drive the combined data + visibility pass under
 *		the R13 harness + episode-fenced online-writer scope; on DONE, issue the
 *		durability barrier and publish the 3-way authority; on BLOCKED, publish
 *		NOTHING and apply the on_unrecoverable policy.  This is what the public
 *		replay_one calls after deriving the window, and the TEST entry (the SRF
 *		drives it with a deterministic window on one machine).
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterThreadRecResult
cluster_thread_recovery_replay_one_window(uint16 dead_tid, XLogRecPtr scan_lower,
										  XLogRecPtr scan_upper, uint64 episode_epoch,
										  ClusterThreadReplayStats *stats)
{
	int origin = (int)dead_tid - 1;
	ClusterThreadVisCtx vis;
	ClusterThreadTouchedRels touched;
	ClusterThreadReplayStats local_stats;
	ClusterThreadRecResult drive_res;
	ClusterThreadRecResult result;
	MemoryContext caller_ctx = CurrentMemoryContext;

	if (stats == NULL)
		stats = &local_stats;
	memset(stats, 0, sizeof(*stats));

	/* dead_tid must name a real thread slot (origin in range). */
	if (dead_tid < 1 || dead_tid > CLUSTER_WAL_STATE_SLOT_COUNT)
		return CLUSTER_THREADREC_BLOCKED;

	/* L235 episode-epoch abort-on-bump enforcement is 3b-3 (needs shmem
	 * replay-state); 3b-2 threads the value through unchanged. */
	(void)episode_epoch;

	memset(&touched, 0, sizeof(touched));
	touched.mcxt = caller_ctx; /* the items array must outlive the drive */
	vis.do_visibility = true;
	vis.origin_node = origin;

	/*
	 * Drive the COMBINED data + visibility pass.  cluster_thread_recovery_drive
	 * runs it under the R13 harness inside the online-writer scope (R14) and
	 * returns DONE / BLOCKED -- never throws a catchable ERROR (it demotes one),
	 * only re-throws a FATAL/PANIC.
	 */
	drive_res
		= cluster_thread_recovery_drive(dead_tid, scan_lower, scan_upper, &vis, &touched, stats);

	if (drive_res != CLUSTER_THREADREC_DONE) {
		/*
		 * Data / visibility did not fully complete -> publish NOTHING
		 * (partial-apply = "never recovered", 8.A); drive already reset stats.
		 */
		cluster_thread_recovery_touched_free(&touched);
		result = CLUSTER_THREADREC_BLOCKED;
	} else {
		volatile bool published = false;

		/*
		 * DONE: data + visibility are applied to shared storage (write-back).
		 * Durability barrier + 3-way authority publish, under a guard so an I/O
		 * failure demotes to BLOCKED (keep_frozen) instead of crashing the
		 * survivor (R13).  Order mirrors the cold path (xlogrecovery.c): barrier
		 * -> registry skip-bound -> node-local reader authority LAST, so a
		 * failure before the reader authority leaves the dead thread un-servable
		 * (frozen) -- never a stale-page serve.  published stays false unless
		 * ALL of the barrier + both authority writes succeed.
		 */
		PG_TRY();
		{
			XLogRecPtr through = stats->recovered_through;

			/*
			 * 1. data pages durable (amend 2): cluster_fs write-back is not
			 * fsync'd and does not register a checkpointer sync request, so a
			 * published authority could otherwise outlive un-fsync'd pages.
			 */
			cluster_thread_recovery_touched_sync_all(&touched);

			/*
			 * 2. per-origin outcome store durable (mirror the cold path's
			 * cluster_remote_xact_flush).  The TT undo-header cross-check store
			 * is WAL-protected and reaches disk via the survivor's normal
			 * checkpoint; a crash before that checkpoint degrades to fail-closed
			 * INDOUBT at read time (never false-commit, 8.A) -- a strict
			 * TT-segment fsync here is an 8.A-safe forward (3b-4+).
			 */
			cluster_remote_xact_flush();

			/* 3a. registry skip-bound for the dead origin's own self-recovery. */
			cluster_wal_state_publish_merge_recovered(dead_tid, through);

			/*
			 * 3b. node-local reader authority LAST (the serving gate the D3
			 * unfreeze check reads).  Online variant: a failure raises a
			 * catchable ERROR caught below -> BLOCKED, no serving authority.
			 */
			cluster_merged_authority_publish_online(origin, through);

			published = true;
		}
		PG_CATCH();
		{
			ErrorData *edata;

			/* CopyErrorData requires a context other than ErrorContext. */
			MemoryContextSwitchTo(caller_ctx);
			edata = CopyErrorData();

			/* R13: a FATAL/PANIC is never demoted -- re-throw it. */
			if (cluster_thread_recovery_should_rethrow(edata->elevel)) {
				FreeErrorData(edata);
				cluster_thread_recovery_touched_free(&touched);
				PG_RE_THROW();
			}
			FlushErrorState();
			FreeErrorData(edata);
			published = false;
		}
		PG_END_TRY();

		cluster_thread_recovery_touched_free(&touched);

		if (published)
			result = CLUSTER_THREADREC_DONE;
		else {
			/* barrier / publish failed -> nothing usable was published. */
			memset(stats, 0, sizeof(*stats));
			result = CLUSTER_THREADREC_BLOCKED;
		}
	}

	/*
	 * on_unrecoverable policy (Q5): the default keep_frozen returns the BLOCKED
	 * to the caller (the dead thread's resources stay frozen, the survivor keeps
	 * running); panic is an operator escape valve that crashes the survivor at
	 * postmaster level.  decide_on_blocked pins the escalation so keep_frozen can
	 * never become a crash.
	 */
	if (result == CLUSTER_THREADREC_BLOCKED
		&& cluster_thread_recovery_decide_on_blocked(cluster_thread_recovery_on_unrecoverable)
			   == CLUSTER_THREADREC_ONBLOCKED_PANIC)
		ereport(PANIC,
				(errcode(ERRCODE_CLUSTER_THREAD_RECOVERY_BLOCKED),
				 errmsg("online thread recovery for dead thread %u could not complete", dead_tid),
				 errhint("cluster.thread_recovery_on_unrecoverable=panic crashes the survivor; "
						 "set it to keep_frozen to leave only that thread's resources frozen.")));

	return result;
}

/*
 * cluster_thread_recovery_replay_one -- the FSM-facing entry (spec-4.11 §2.2).
 *
 *	Scope-gate from the live runtime, derive a basic window from the dead
 *	thread's wal-state slot, and run the orchestrator core.  NO live FSM caller
 *	yet (3b-3 wires the GRD reconfig FSM); only a TEST SRF and the future FSM
 *	call it.  The precise validated boundary (D4) and the FSM-precise
 *	per-reconfig survivor count (3b-3) refine the two basic derivations below.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterThreadRecResult
cluster_thread_recovery_replay_one(uint16 dead_tid, uint64 episode_epoch)
{
	ClusterThreadRecScope scope;
	ClusterWalStateSlot slot;
	XLogRecPtr lower;
	XLogRecPtr validated_min;
	XLogRecPtr scan_upper;
	bool shared_fs;
	int survivors;

	shared_fs = (cluster_shared_storage_backend == CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS);

	/*
	 * 2-node scope: a single death leaves node_count - 1 survivors.  The
	 * FSM-precise per-reconfig survivor count lands with the live caller (3b-3);
	 * for >2 nodes this yields >= 2 -> MULTI_SURVIVOR (FEATURE_NOT_SUPPORTED, Q9).
	 */
	survivors = cluster_conf_node_count() - 1;

	scope = cluster_thread_recovery_decide_scope(cluster_online_thread_recovery,
												 cluster_conf_has_peers(), shared_fs, survivors);
	if (scope != CLUSTER_THREADREC_SCOPE_APPLICABLE)
		/*
		 * DISABLED / SINGLE_NODE / NO_SHARED_BACKEND / MULTI_SURVIVOR: do not
		 * online-recover (the thread stays frozen / cold-restart handles it).
		 * D7 (3b-4) refines NO_SHARED_BACKEND and MULTI_SURVIVOR into explicit
		 * FEATURE_NOT_SUPPORTED ereports; here every non-applicable scope is a
		 * fail-closed NOT_APPLICABLE.
		 */
		return CLUSTER_THREADREC_NOT_APPLICABLE;

	/*
	 * Window derivation: lower = the dead thread's last checkpoint redo (a sound,
	 * redo-idempotent replay start); the registry's observational highest_lsn is
	 * the durable-write watermark (validated_min), NOT the replay upper.  A slot
	 * read failure or an unusable slot (missing checkpoint history / nothing
	 * written / inverted) fails closed.
	 */
	if (cluster_wal_state_read_slot(dead_tid, &slot) != CLUSTER_WAL_SLOT_OK)
		return CLUSTER_THREADREC_BLOCKED;
	if (slot.checkpoint_redo_lsn == 0 || slot.highest_lsn == 0
		|| slot.highest_lsn <= slot.checkpoint_redo_lsn)
		return CLUSTER_THREADREC_BLOCKED;

	lower = (XLogRecPtr)slot.checkpoint_redo_lsn;
	validated_min = (XLogRecPtr)slot.highest_lsn;

	/*
	 * D4 (spec-4.11 3b-4a): the replay upper is the VALIDATED torn-tail boundary
	 * (last complete record decoded from lower), NOT the observational highest_lsn
	 * (which can sit mid-record, or behind committed records written after the
	 * watermark refresh).  validated_end fail-closes a decode that stops below
	 * validated_min (mid-stream corruption, never a silent truncation of the dead
	 * thread's committed WAL; 8.A).  BLOCKED here keeps the thread frozen.
	 */
	if (cluster_thread_recovery_validated_end(dead_tid, lower, validated_min, &scan_upper)
		!= CLUSTER_THREADREC_DONE)
		return CLUSTER_THREADREC_BLOCKED;

	return cluster_thread_recovery_replay_one_window(dead_tid, lower, scan_upper, episode_epoch,
													 NULL);
}

/*
 * cluster_thread_recovery_local_complete -- the D3 unfreeze precondition
 * (spec-4.11 3b-3): is dead_tid's WAL data online-recovered on THIS node?
 *
 *	Reads the NODE-LOCAL merged materialization authority (Q4 3-way authority,
 *	R11): cluster_merged_instance_is_materialized() is the reader gate the
 *	orchestrator publishes to on DONE (cluster_merged_authority_publish_online),
 *	NOT the cluster-wide registry (which is a skip-bound, not a serve gate, and
 *	mixing them would let a peer's self-recovery cut this node's authority).
 *
 *	required_lsn == InvalidXLogRecPtr asks only "materialized at all" (the
 *	node-level question the reconfig FSM uses); a real required_lsn additionally
 *	demands recovered_through >= it (the precise per-block check the
 *	gcs_block phase gate makes with the block's PI watermark).
 *
 *	fail-closed (8.A): a thread id that names no origin, an unmaterialized
 *	origin, or recovered_through short of required_lsn all return false -- the
 *	resource stays frozen, never served as current.
 */
bool
cluster_thread_recovery_local_complete(uint16 dead_tid, XLogRecPtr required_lsn)
{
	int origin = cluster_thread_recovery_origin_for_tid(dead_tid);

	if (origin < 0)
		return false; /* not a real thread id -> fail-closed (keep frozen) */

	if (!cluster_merged_instance_is_materialized(origin))
		return false; /* dead origin's merged WAL not materialized here */

	if (XLogRecPtrIsInvalid(required_lsn))
		return true; /* node-level: materialization suffices */

	return cluster_merged_instance_recovered_through(origin) >= (uint64)required_lsn;
}

/*
 * cluster_thread_recovery_gate_unfreeze -- the reconfig-FSM unfreeze gate
 * (spec-4.11 D3, 3b-3).  See the header for the full contract.
 *
 *	Returns true == "STAY FROZEN": online thread recovery is in scope AND a dead
 *	origin is not yet materialized here.  Returns false == "may unfreeze": out
 *	of scope (so the existing spec-4.6/4.7 path is unchanged) or every dead
 *	origin is complete.
 *
 *	Scope reuses the pure cluster_thread_recovery_decide_scope() so the gate
 *	engages on EXACTLY the same conditions under which a replay would run: the
 *	GUC is on (dev=off by default), a shared data backend exists, and the
 *	cluster is 2-node (a single survivor = single replay owner).  Out of scope
 *	-> false: never freeze waiting on a recovery that will not run.
 */
bool
cluster_thread_recovery_gate_unfreeze(const uint64 *dead_bitmap, int nwords)
{
	ClusterThreadRecScope scope;
	uint64 materialized[(CLUSTER_MAX_NODES + 63) / 64];
	const int mwords = (CLUSTER_MAX_NODES + 63) / 64;
	bool shared_fs;
	int survivors;
	int effective;
	int i;

	shared_fs = (cluster_shared_storage_backend == CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS);
	survivors = cluster_conf_node_count() - 1;
	scope = cluster_thread_recovery_decide_scope(cluster_online_thread_recovery,
												 cluster_conf_has_peers(), shared_fs, survivors);

	if (scope != CLUSTER_THREADREC_SCOPE_APPLICABLE)
		return false; /* recovery not active -> do not gate (no regression) */

	if (dead_bitmap == NULL || nwords <= 0)
		return false; /* nothing to gate */

	/*
	 * Build the per-node materialized bitmap for THIS episode's dead origins:
	 * node i (origin) <- dead_tid i + 1.  A bad id maps to no origin ->
	 * local_complete returns false -> the bit stays clear -> gate_decide keeps
	 * it frozen (fail-closed, 8.A).  The node-level question (InvalidXLogRecPtr)
	 * asks only "materialized at all"; the per-block gcs_block phase gate carries
	 * the precise recovered_through >= page_lsn check for individual blocks.
	 */
	memset(materialized, 0, sizeof(materialized));
	for (i = 0; i < CLUSTER_MAX_NODES && (i / 64) < nwords; i++) {
		if (((dead_bitmap[i / 64] >> (i % 64)) & UINT64CONST(1)) == 0)
			continue;
		if (cluster_thread_recovery_local_complete((uint16)(i + 1), InvalidXLogRecPtr))
			materialized[i / 64] |= (UINT64CONST(1) << (i % 64));
	}

	/* Decide over the words covered by BOTH bitmaps (dead nodes live in
	 * [0, CLUSTER_MAX_NODES), so any dead_bitmap words past mwords are 0). */
	effective = (nwords < mwords) ? nwords : mwords;
	return cluster_thread_recovery_gate_decide(scope, dead_bitmap, materialized, effective);
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
