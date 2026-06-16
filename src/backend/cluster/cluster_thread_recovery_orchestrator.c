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
#include "utils/errcodes.h"	  /* ERRCODE_FEATURE_NOT_SUPPORTED (D7 gate)     */
#include "utils/wait_event.h" /* WAIT_EVENT_CLUSTER_THREAD_RECOVERY (D5)      */

#include "port/atomics.h"			/* replay-slot atomics + barriers (3b-4b)      */
#include "portability/instr_time.h" /* PGRAC: spec-4.13 D5 LOG-only latency        */

#include "cluster/cluster_conf.h"			   /* node_count / has_peers (scope gate)        */
#include "cluster/cluster_elog.h"			   /* ERRCODE_CLUSTER_THREAD_RECOVERY_BLOCKED     */
#include "cluster/cluster_guc.h"			   /* GUCs: online flag + shared backend + policy */
#include "cluster/cluster_recovery_merge.h"	   /* node-local authority publish (online)       */
#include "cluster/cluster_recovery_plan.h"	   /* ClusterThreadReplaySlot + slot accessor     */
#include "cluster/cluster_remote_xact.h"	   /* per-origin outcome store flush              */
#include "cluster/cluster_thread_recovery.h"   /* engine / driver / pure gates                */
#include "cluster/cluster_wal_state.h"		   /* slot read + registry skip-bound publish     */
#include "cluster/cluster_write_fence.h"	   /* spec-4.12 D6 durable authority verify        */
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
 * D5 observability counters (spec-4.11 §D5).  Cumulative online thread-recovery
 * outcomes over the region-level counter block; L110-safe -- with no shmem
 * attached the writes are no-ops and the reads return the frozen-safe sentinel.
 */
void
cluster_thread_recovery_count_done(XLogRecPtr recovered_through)
{
	ClusterThreadRecoveryCounters *c = cluster_thread_recovery_counters();
	uint64 cur;

	if (c == NULL)
		return;
	pg_atomic_fetch_add_u64(&c->threads_recovered, 1);
	/* recovered_through is a high-watermark: reconfig episodes advance the LSN
	 * monotonically, but guard against a stale-epoch retry regressing it. */
	cur = pg_atomic_read_u64(&c->recovered_through);
	if ((uint64)recovered_through > cur)
		pg_atomic_write_u64(&c->recovered_through, (uint64)recovered_through);
}

void
cluster_thread_recovery_count_blocked(void)
{
	ClusterThreadRecoveryCounters *c = cluster_thread_recovery_counters();

	if (c == NULL)
		return;
	pg_atomic_fetch_add_u64(&c->replay_failclosed, 1);
}

uint64
cluster_thread_recovery_get_threads_recovered(void)
{
	ClusterThreadRecoveryCounters *c = cluster_thread_recovery_counters();

	return (c == NULL) ? 0 : pg_atomic_read_u64(&c->threads_recovered);
}

uint64
cluster_thread_recovery_get_replay_failclosed(void)
{
	ClusterThreadRecoveryCounters *c = cluster_thread_recovery_counters();

	return (c == NULL) ? 0 : pg_atomic_read_u64(&c->replay_failclosed);
}

XLogRecPtr
cluster_thread_recovery_get_recovered_through(void)
{
	ClusterThreadRecoveryCounters *c = cluster_thread_recovery_counters();

	return (c == NULL) ? InvalidXLogRecPtr : (XLogRecPtr)pg_atomic_read_u64(&c->recovered_through);
}

/*
 * cluster_thread_recovery_state_name -- an aggregate state over the per-thread
 *	replay slots for the recovery dump (spec-4.11 D5).  Precedence REPLAYING >
 *	BLOCKED > DONE > IDLE so the most active concern shows; "-" when no shmem is
 *	attached (L110 sentinel).
 */
const char *
cluster_thread_recovery_state_name(void)
{
	bool any = false;
	bool any_blocked = false;
	bool any_done = false;
	uint16 tid;

	for (tid = XLP_THREAD_ID_FIRST_REAL; tid <= CLUSTER_RECOVERY_PLAN_THREADS; tid++) {
		ClusterThreadReplaySlot *slot = cluster_thread_recovery_replay_slot(tid);
		uint32 st;

		if (slot == NULL)
			continue; /* tid beyond the accessor range, or region not attached */
		any = true;
		st = pg_atomic_read_u32(&slot->state);
		if (st == (uint32)CLUSTER_THREADREC_REPLAY_REPLAYING)
			return "replaying";
		if (st == (uint32)CLUSTER_THREADREC_REPLAY_BLOCKED)
			any_blocked = true;
		else if (st == (uint32)CLUSTER_THREADREC_REPLAY_DONE)
			any_done = true;
	}

	if (!any)
		return "-"; /* region not attached (L110 sentinel) */
	if (any_blocked)
		return "blocked";
	if (any_done)
		return "done";
	return "idle";
}

/*
 * cluster_thread_recovery_current_scope -- resolve the live-runtime D7 scope
 *	(spec-4.11 §D7): the GUC, has_peers, a genuinely shared data backend, and the
 *	2-node survivor count.  Mirrors replay_one's resolution so a capability probe
 *	sees exactly what the launch path would decide.
 */
ClusterThreadRecScope
cluster_thread_recovery_current_scope(void)
{
	bool shared_fs = (cluster_shared_storage_backend == CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS);
	int survivors = cluster_conf_node_count() - 1;

	return cluster_thread_recovery_decide_scope(cluster_online_thread_recovery,
												cluster_conf_has_peers(), shared_fs, survivors);
}

/*
 * cluster_thread_recovery_capability_gate -- the D7 FEATURE_NOT_SUPPORTED gate
 *	(spec-4.11 §3, §D7).  For a hard-unsupported scope (no genuinely shared data
 *	backend, or a >2-node multi-survivor cluster out of the v0.2 2-node scope)
 *	raise FEATURE_NOT_SUPPORTED (mirror spec-4.5a's 53RA3 backend gate); any other
 *	scope is a no-op -- APPLICABLE proceeds, DISABLED / SINGLE_NODE fall back to
 *	PG-native / cold recovery with no error.  This is the EXPLICIT capability
 *	surface (a probe / test consults it), NOT the live reconfig FSM: the FSM stays
 *	a no-op for every non-applicable scope so a single-node / GUC-off / >2-node
 *	reconfig never crashes (the t/249-252 no-regression line).  scope_is_unsupported
 *	pins the branch so the gate NEVER fires for a merely not-applicable scope.
 */
void
cluster_thread_recovery_capability_gate(ClusterThreadRecScope scope)
{
	if (!cluster_thread_recovery_scope_is_unsupported(scope))
		return;

	if (scope == CLUSTER_THREADREC_SCOPE_NO_SHARED_BACKEND)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("online thread recovery requires a shared data backend"),
				 errhint("Set cluster.shared_storage_backend=cluster_fs, or let the dead node "
						 "recover on cold restart.")));

	/* CLUSTER_THREADREC_SCOPE_MULTI_SURVIVOR */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("online thread recovery is not supported with more than one survivor"),
			 errhint("Online thread recovery is limited to a 2-node cluster (a single survivor) "
					 "in this release; the dead node recovers on cold restart.")));
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
	/* PGRAC: spec-4.13 D5 LOG-only phase timing (no semantic effect; the captures
	 * never touch the 8.A authority-publish ordering below). */
	instr_time tr_start;
	instr_time tr_replay_end;
	instr_time tr_publish_end;
	instr_time tr_post_end;

	if (stats == NULL)
		stats = &local_stats;
	memset(stats, 0, sizeof(*stats));

	/* dead_tid must name a real thread slot (origin in range). */
	if (dead_tid < 1 || dead_tid > CLUSTER_WAL_STATE_SLOT_COUNT)
		return CLUSTER_THREADREC_BLOCKED;

	/* episode_epoch: the L235 in-memory abort-on-bump gate uses it upstream; here
	 * spec-4.12 D6 re-checks it against the DURABLE voting-disk marker before the
	 * authority publish below (8.A ground-truth cross-check). */

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
	INSTR_TIME_SET_CURRENT(tr_start); /* PGRAC: spec-4.13 D5 replay-phase start */
	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_THREAD_RECOVERY);
	drive_res
		= cluster_thread_recovery_drive(dead_tid, scan_lower, scan_upper, &vis, &touched, stats);
	pgstat_report_wait_end();
	INSTR_TIME_SET_CURRENT(tr_replay_end); /* PGRAC: spec-4.13 D5 replay/visibility done */

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
			 * 0. spec-4.12 D6 (8.A): direct durable re-check BEFORE publishing any
			 * authority.  The 4.11 in-memory episode-epoch gate proved this worker
			 * is current; the durable voting-disk fence marker is the GROUND TRUTH.
			 * If a newer reconfig has superseded this episode (the durable majority
			 * marker's fence_epoch no longer equals episode_epoch), publish NOTHING
			 * -- this catchable ERROR is demoted to BLOCKED (keep_frozen) by the R13
			 * harness below.  Enforcement off -> verify returns true (no-op).  We do
			 * NOT bound replay here (full redo already applied, Oracle-aligned).
			 */
			if (!cluster_write_fence_verify_durable(episode_epoch))
				ereport(ERROR, (errcode(ERRCODE_CLUSTER_WRITE_FENCED),
								errmsg("thread recovery authority rejected: the durable fence "
									   "marker does not confirm episode epoch %llu",
									   (unsigned long long)episode_epoch),
								errdetail("A newer membership reconfiguration has superseded this "
										  "recovery episode; the dead thread stays frozen (no "
										  "serving authority is published) until the current "
										  "episode recovers it.")));

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

	INSTR_TIME_SET_CURRENT(tr_publish_end); /* PGRAC: spec-4.13 D5 publish/barrier done */

	/*
	 * D5 observability: record the outcome before applying the panic policy -- a
	 * DONE bumps threads_recovered and advances the recovered_through watermark; a
	 * BLOCKED bumps the failclosed (53RA4) counter.  stats->recovered_through is
	 * the published boundary on DONE and 0 on BLOCKED (cleared above).
	 */
	if (result == CLUSTER_THREADREC_DONE)
		cluster_thread_recovery_count_done(stats->recovered_through);
	else
		cluster_thread_recovery_count_blocked();

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

	/*
	 * PGRAC: spec-4.13 D5 -- one window-summary LOG (replay includes visibility,
	 * which is interleaved in the combined engine and cannot be isolated without
	 * refactoring the per-record loop; see the spec D2/Q7 note).  freeze is the
	 * caller's (replay_one) concern; total comes from the worker.
	 */
	INSTR_TIME_SET_CURRENT(tr_post_end);
	{
		instr_time d_replay = tr_replay_end;
		instr_time d_publish = tr_publish_end;
		instr_time d_post = tr_post_end;

		INSTR_TIME_SUBTRACT(d_replay, tr_start);
		INSTR_TIME_SUBTRACT(d_publish, tr_replay_end);
		INSTR_TIME_SUBTRACT(d_post, tr_publish_end);
		ereport(LOG,
				(errmsg("cluster thread recovery window: tid=%u outcome=%s replay_us=" INT64_FORMAT
						" publish_us=" INT64_FORMAT " post_us=" INT64_FORMAT,
						dead_tid, result == CLUSTER_THREADREC_DONE ? "DONE" : "BLOCKED",
						INSTR_TIME_GET_MICROSEC(d_replay), INSTR_TIME_GET_MICROSEC(d_publish),
						INSTR_TIME_GET_MICROSEC(d_post))));
	}

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
	 * written / inverted) fails closed.  A window-derivation BLOCKED is a real
	 * fail-closed outcome of the live FSM path (the executor worker reaches it),
	 * so it must bump the D5 failclosed counter too -- replay_one_window's own
	 * counting is only reached once a window is derived (otherwise the most common
	 * live fail-closed would be invisible in thread_recovery_replay_failclosed).
	 */
	if (cluster_wal_state_read_slot(dead_tid, &slot) != CLUSTER_WAL_SLOT_OK) {
		cluster_thread_recovery_count_blocked();
		return CLUSTER_THREADREC_BLOCKED;
	}
	if (slot.checkpoint_redo_lsn == 0 || slot.highest_lsn == 0
		|| slot.highest_lsn <= slot.checkpoint_redo_lsn) {
		cluster_thread_recovery_count_blocked();
		return CLUSTER_THREADREC_BLOCKED;
	}

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
		!= CLUSTER_THREADREC_DONE) {
		cluster_thread_recovery_count_blocked();
		return CLUSTER_THREADREC_BLOCKED;
	}

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
