/*-------------------------------------------------------------------------
 *
 * cluster_undo_cleaner.c
 *	  pgrac Undo Cleaner cluster background process — Stage 3.13.
 *
 *	  See cluster_undo_cleaner.h for the architectural overview and the
 *	  HC1-HC6 hard constraints.  Lifecycle skeleton mirrors
 *	  cluster_stats.c (spec-1.14) with two deliberate differences:
 *
 *	  1. ServerLoop-managed (NOT phase-4 gated): postmaster spawns the
 *	     cleaner once pmState == PM_RUN and respawns after normal exit.
 *	     Absence degrades to spec-3.12 lazy-only recycling — never a
 *	     startup failure, so there is no wait_for_ready and no spawn
 *	     SQLSTATE pair.
 *	  2. Pressure wakeup: allocator retention-pressure paths SetLatch
 *	     the cleaner through UndoCleanerSharedState.latch so a pass
 *	     runs when RECYCLABLE supply is needed, without waiting out
 *	     cluster.undo_cleaner_interval_ms.
 *
 *	  The actual cleaning work (shmem TT slot GC, durable header scan,
 *	  segment state advancement) lives in undo_cleaner_run_pass(); at
 *	  step 2 (D1 skeleton) the pass body is a counter-only no-op and is
 *	  filled by steps 3-8 (D2/D3/D5/D6).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_undo_cleaner.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-3.13-undo-cleaner-tt-gc.md (FROZEN v0.3, 2026-06-04).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <unistd.h>

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/auxprocess.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h" /* WAIT_EVENT_CLUSTER_BGPROC_UNDO_CLEANER_MAIN_LOOP */

#include "cluster/cluster_guc.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_mode.h"			 /* cluster_storage_mode_enabled */
#include "cluster/cluster_undo_horizon.h"	 /* cluster floor + fence (spec-5.22e D5-3) */
#include "cluster/cluster_undo_retention.h"	 /* horizon (C17: once per pass) */
#include "cluster/cluster_tt_slot.h"		 /* current TT segment (exclusion) */
#include "cluster/cluster_undo_record_api.h" /* active segment + advance (D3) */
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_undo_cleaner.h"


/*
 * Module-level pointer to the Undo Cleaner shmem region.  Set by
 * cluster_undo_cleaner_shmem_init().  NULL only inside the
 * cluster_unit test harness when init was not invoked.
 */
static UndoCleanerSharedState *undo_cleaner_state = NULL;


/* ============================================================
 * Status enum -> string lookup.
 * ============================================================ */

static const char *const undo_cleaner_status_strings[] = {
	"not_started",	 /* UNDO_CLEANER_NOT_STARTED   = 0 */
	"spawning",		 /* UNDO_CLEANER_SPAWNING      = 1 */
	"ready",		 /* UNDO_CLEANER_READY         = 2 */
	"shutting_down", /* UNDO_CLEANER_SHUTTING_DOWN = 3 */
	"exited"		 /* UNDO_CLEANER_EXITED        = 4 */
};


const char *
cluster_undo_cleaner_status_to_string(UndoCleanerStatus s)
{
	if ((int)s < 0 || (int)s > UNDO_CLEANER_STATUS_LAST)
		return "(unknown)";
	return undo_cleaner_status_strings[(int)s];
}


/* ============================================================
 * shmem region helpers (spec-1.3 registry-backed; L206 五步).
 * ============================================================ */

Size
cluster_undo_cleaner_shmem_size(void)
{
	return MAXALIGN(sizeof(UndoCleanerSharedState));
}


void
cluster_undo_cleaner_shmem_init(void)
{
	bool found;

	undo_cleaner_state = (UndoCleanerSharedState *)ShmemInitStruct(
		"pgrac cluster undo cleaner", sizeof(UndoCleanerSharedState), &found);

	if (!found) {
		memset(undo_cleaner_state, 0, sizeof(*undo_cleaner_state));
		LWLockInitialize(&undo_cleaner_state->lwlock, LWTRANCHE_CLUSTER_UNDO_CLEANER);
		undo_cleaner_state->status = UNDO_CLEANER_NOT_STARTED;
	}
}


static const ClusterShmemRegion undo_cleaner_region = {
	.name = "pgrac cluster undo cleaner",
	.size_fn = cluster_undo_cleaner_shmem_size,
	.init_fn = cluster_undo_cleaner_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_undo_cleaner",
	.reserved_flags = 0,
};


void
cluster_undo_cleaner_shmem_register(void)
{
	cluster_shmem_register_region(&undo_cleaner_region);
}


/* ============================================================
 * Cross-backend API.
 * ============================================================ */

void
cluster_undo_cleaner_request_shutdown(void)
{
	Assert(!IsUnderPostmaster);

	if (undo_cleaner_state == NULL)
		return;

	LWLockAcquire(&undo_cleaner_state->lwlock, LW_EXCLUSIVE);
	undo_cleaner_state->shutdown_requested = true;
	LWLockRelease(&undo_cleaner_state->lwlock);
}


void
cluster_undo_cleaner_wakeup(void)
{
	Latch *latch;

	/*
	 * Q8 pressure wakeup.  Callers sit on allocator hot-pressure paths
	 * (retention rollover / hard cap), so this must be cheap and must
	 * never throw: copy the latch pointer under LW_SHARED, SetLatch
	 * outside the lock.  Latch points at the cleaner's PGPROC
	 * procLatch (shmem), so SetLatch from any backend is safe; a
	 * concurrently-exiting cleaner leaves a harmless set latch.
	 */
	if (undo_cleaner_state == NULL)
		return;

	LWLockAcquire(&undo_cleaner_state->lwlock, LW_SHARED);
	latch = undo_cleaner_state->latch;
	LWLockRelease(&undo_cleaner_state->lwlock);

	if (latch != NULL)
		SetLatch(latch);
}


UndoCleanerStatus
cluster_undo_cleaner_status(void)
{
	UndoCleanerStatus result;

	if (undo_cleaner_state == NULL)
		return UNDO_CLEANER_NOT_STARTED;

	LWLockAcquire(&undo_cleaner_state->lwlock, LW_SHARED);
	result = undo_cleaner_state->status;
	LWLockRelease(&undo_cleaner_state->lwlock);
	return result;
}

pid_t
cluster_undo_cleaner_pid(void)
{
	pid_t result;

	if (undo_cleaner_state == NULL)
		return 0;

	LWLockAcquire(&undo_cleaner_state->lwlock, LW_SHARED);
	result = undo_cleaner_state->pid;
	LWLockRelease(&undo_cleaner_state->lwlock);
	return result;
}

TimestampTz
cluster_undo_cleaner_spawned_at(void)
{
	TimestampTz result;

	if (undo_cleaner_state == NULL)
		return 0;

	LWLockAcquire(&undo_cleaner_state->lwlock, LW_SHARED);
	result = undo_cleaner_state->spawned_at;
	LWLockRelease(&undo_cleaner_state->lwlock);
	return result;
}

TimestampTz
cluster_undo_cleaner_ready_at(void)
{
	TimestampTz result;

	if (undo_cleaner_state == NULL)
		return 0;

	LWLockAcquire(&undo_cleaner_state->lwlock, LW_SHARED);
	result = undo_cleaner_state->ready_at;
	LWLockRelease(&undo_cleaner_state->lwlock);
	return result;
}

TimestampTz
cluster_undo_cleaner_last_liveness_tick_at(void)
{
	TimestampTz result;

	if (undo_cleaner_state == NULL)
		return 0;

	LWLockAcquire(&undo_cleaner_state->lwlock, LW_SHARED);
	result = undo_cleaner_state->last_liveness_tick_at;
	LWLockRelease(&undo_cleaner_state->lwlock);
	return result;
}

int64
cluster_undo_cleaner_main_loop_iters(void)
{
	int64 result;

	if (undo_cleaner_state == NULL)
		return 0;

	LWLockAcquire(&undo_cleaner_state->lwlock, LW_SHARED);
	result = undo_cleaner_state->main_loop_iters;
	LWLockRelease(&undo_cleaner_state->lwlock);
	return result;
}


/* ============================================================
 * Undo Cleaner main entry (AuxiliaryProcessMain dispatch target).
 * ============================================================ */

static void
undo_cleaner_publish_status(UndoCleanerStatus status)
{
	TimestampTz now = GetCurrentTimestamp();

	Assert(undo_cleaner_state != NULL);

	LWLockAcquire(&undo_cleaner_state->lwlock, LW_EXCLUSIVE);
	undo_cleaner_state->status = status;
	/*
	 * F16 (spec-1.14 lineage): SPAWNING marks a new incarnation —
	 * refresh every incarnation-scoped field unconditionally so SQL
	 * views never report stale PID/timestamps after a ServerLoop
	 * respawn.
	 */
	if (status == UNDO_CLEANER_SPAWNING) {
		undo_cleaner_state->pid = MyProcPid;
		undo_cleaner_state->spawned_at = now;
		undo_cleaner_state->ready_at = 0;
		undo_cleaner_state->last_liveness_tick_at = 0;
		undo_cleaner_state->main_loop_iters = 0;
		undo_cleaner_state->latch = (MyProc != NULL) ? &MyProc->procLatch : NULL;
	} else if (status == UNDO_CLEANER_READY) {
		undo_cleaner_state->ready_at = now;
	} else if (status == UNDO_CLEANER_SHUTTING_DOWN) {
		/* stop accepting pressure wakeups against a dying latch */
		undo_cleaner_state->latch = NULL;
	}
	LWLockRelease(&undo_cleaner_state->lwlock);
}


static bool
undo_cleaner_shutdown_requested(void)
{
	bool requested;

	Assert(undo_cleaner_state != NULL);

	LWLockAcquire(&undo_cleaner_state->lwlock, LW_SHARED);
	requested = undo_cleaner_state->shutdown_requested;
	LWLockRelease(&undo_cleaner_state->lwlock);
	return requested;
}


static void
undo_cleaner_advance_liveness_tick(void)
{
	TimestampTz now = GetCurrentTimestamp();

	Assert(undo_cleaner_state != NULL);

	LWLockAcquire(&undo_cleaner_state->lwlock, LW_EXCLUSIVE);
	undo_cleaner_state->last_liveness_tick_at = now;
	undo_cleaner_state->main_loop_iters++;
	LWLockRelease(&undo_cleaner_state->lwlock);
}


/*
 * undo_cleaner_run_pass -- one proactive cleaning pass.
 *
 *	Step 2 (D1) skeleton: counter-only no-op.  Steps 3-8 fill in:
 *	  D2-A shmem TT slot GC on the current active segment,
 *	  D2-B durable header scan-only pass over rolled-away segments,
 *	  D3   SEGMENT_COMMITTED -> SEGMENT_RECYCLABLE advancement,
 *	  D5   TT_WRAP_MAX retire bookkeeping,
 *	  D6   counters + LOG-once horizon-pinned observability.
 *
 *	Contract already binding at the skeleton stage:
 *	  - horizon is computed ONCE per pass BEFORE any seg->lock /
 *	    lifecycle_lock (spec-3.12 C17 order);
 *	  - storage-mode gate: no cluster storage mode -> no work (HC4);
 *	  - cluster.undo_cleaner_enabled=off -> no work (diagnostic
 *	    parity with 3.12 lazy-only mode).
 */
static void
undo_cleaner_run_pass(void)
{
	ClusterUndoCleanerPassStats stats;
	if (!cluster_undo_cleaner_enabled)
		return;
	if (!cluster_storage_mode_enabled())
		return;

	/*
	 * D2 (step 3): horizon ONCE per pass, BEFORE any seg->lock (C17).
	 * With the retention gate GUC off there is nothing to pre-free —
	 * alloc Pass-2 recycles immediately (C6) — so the pass only ticks.
	 *
	 * spec-5.22e D5-3: the pass input is no longer the bare local horizon
	 * but the CLUSTER floor {scn, epoch}: the scn_time_cmp-min of the local
	 * horizon and every required MEMBER peer's accepted report.  An empty
	 * required set (single node / cold formation) folds to the local value
	 * on today's path; any unproven required peer STALLS the pass — the
	 * whole recycle stage (shmem TT GC included, Q8: one horizon input) is
	 * skipped, never run against a floor we could not prove (rule 8.A /
	 * Q3'': NO fallback to local recycling, whatever the stall reason).
	 * The floor's epoch rides through the pass and is re-verified at every
	 * mutation (F-D2 fence); a mid-pass bump aborts the pass immediately.
	 */
	memset(&stats, 0, sizeof(stats));
	if (cluster_undo_retention_horizon_enabled) {
		SCN local_horizon = cluster_undo_retention_horizon();
		ClusterUndoHorizonFloor floor;
		ClusterUndoHorizonStallReason stall_reason = CLUSTER_UNDO_HORIZON_STALL_NONE;
		int32 stall_blame = -1;
		bool stalled = false;
		bool fence_aborted = false;
		static bool stall_logged = false;
		SCN horizon;

		{
			ClusterUndoHorizonReportView views[CLUSTER_MAX_NODES];
			uint8 required[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
			uint64 fold_epoch;
			int nviews;

			nviews = cluster_undo_horizon_sample_views(views, CLUSTER_MAX_NODES);
			if (!cluster_undo_horizon_required_members(required, &fold_epoch)) {
				/* reconfig in flight: the member set would not hold still */
				stalled = true;
				stall_reason = CLUSTER_UNDO_HORIZON_STALL_EPOCH;
			} else if (cluster_undo_horizon_cluster_floor(
						   local_horizon, views, nviews, required, cluster_node_id, fold_epoch,
						   (uint64)GetCurrentTimestamp(), (uint32)cluster_lmon_main_loop_interval,
						   &floor, &stall_reason, &stall_blame)
					   == CLUSTER_UNDO_HORIZON_FOLD_STALLED)
				stalled = true;
		}

		if (stalled) {
			cluster_undo_horizon_note_stall();
			if (stall_blame >= 0 && stall_blame != cluster_node_id)
				cluster_undo_horizon_note_peer_stale();
			if (!stall_logged) {
				stall_logged = true;
				ereport(LOG,
						(errmsg("cluster undo cleaner: recycle stalled, cluster horizon "
								"unproven (reason \"%s\", node %d)",
								cluster_undo_horizon_stall_reason_name(stall_reason), stall_blame),
						 errhint("Recycling pauses until every MEMBER peer publishes a "
								 "fresh horizon report at the current epoch. Undo segment "
								 "pool may grow until then (53R9E at the hard cap).")));
			}
			goto pass_account; /* 只扫不收: skip the whole recycle stage */
		}
		if (stall_logged) {
			stall_logged = false;
			ereport(LOG, (errmsg("cluster undo cleaner: cluster horizon proven again; "
								 "recycling resumes")));
		}
		horizon = floor.scn;
		cluster_undo_horizon_note_floor(floor.scn);

		if (!cluster_tt_slot_gc_current_pass(horizon, floor.epoch, &stats)) {
			cluster_undo_horizon_note_pass_abort();
			goto pass_account; /* F-D2: epoch moved mid-scan; abort the pass */
		}

		/*
		 * D2-B + D3: walk this instance's rolled-away segment inventory,
		 * batch-bounded (R7).  For each segment that is neither the record
		 * cursor's active segment nor the TT allocator's current segment:
		 * read-only TT inventory scan, then COMMITTED -> RECYCLABLE
		 * advancement under lifecycle_lock (Q6; horizon already computed
		 * above, C17).
		 */
		{
			uint8 owner = (uint8)(cluster_node_id + 1);
			uint32 base = (uint32)cluster_node_id * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
			uint32 max_seg = cluster_undo_segment_scan_max_existing(owner);
			uint32 active_seg = cluster_undo_record_active_segment_id();
			uint32 tt_seg = cluster_tt_slot_current_segment(cluster_node_id);
			int batch = cluster_undo_cleaner_batch_segments;
			uint32 inventory = cluster_undo_cleaner_scan_inventory(base, max_seg);
			uint32 visited = 0;
			uint32 seg;

			Assert(undo_cleaner_state != NULL);

			/*
			 * spec-4.12a D4 (Finding C): round-robin sweep.  Resume from the
			 * cursor the previous pass left off at (normalized into the current
			 * window) rather than restarting at base every pass.  `batch` bounds
			 * the real work per pass (R7); `visited < inventory` bounds the wrap
			 * so a pass whose entire window is active/tt/unreadable segments
			 * (which spend no batch) still terminates.  Without this the low ids
			 * consumed the whole batch budget every pass and high-id ACTIVE
			 * segments were never scanned -> never drained -> the pool grew to
			 * the hard cap and never fell back even at quiesce (the leak).
			 */
			seg = cluster_undo_cleaner_scan_cursor_start(undo_cleaner_state->scan_resume_seg, base,
														 max_seg);

			while (batch > 0 && visited < inventory) {
				uint32 cur = seg;

				visited++;
				seg = cluster_undo_cleaner_scan_cursor_next(cur, base, max_seg);

				if (cur == active_seg || cur == tt_seg)
					continue;

				CHECK_FOR_INTERRUPTS();

				if (!cluster_undo_segment_tt_header_scan_pass(cur, owner, horizon, &stats))
					continue; /* absent / unreadable: retry next pass */
				batch--;

				/*
				 * spec-4.12a D2 (Q3-C): a record segment that was in-flight when
				 * the record cursor rolled away from it stays SEGMENT_ACTIVE
				 * after its writers commit -- the rollover-time drain retained
				 * it and nothing re-triggers that path.  Re-evaluate it here for
				 * ACTIVE -> COMMITTED so the recyclable advance below can reclaim
				 * it (the leak fix).  GUC off keeps the legacy lazy behaviour and
				 * never gates the 8.A guard (spec §2.3).
				 */
				if (cluster_undo_record_segment_commit_on_rollover)
					cluster_undo_segment_advance_committed(cur);

				{
					ClusterUndoSegTryRecycle rr
						= cluster_undo_segment_advance_recyclable(cur, horizon, floor.epoch);

					if (rr == CLUSTER_SEG_RECYCLE_EPOCH_CHANGED) {
						/* F-D2: epoch moved inside the mutation lock; the
						 * mutation did not run.  Abort the whole pass NOW. */
						fence_aborted = true;
						break;
					}
					if (rr == CLUSTER_SEG_RECYCLE_ADVANCED)
						stats.segments_marked_recyclable++;
				}
			}

			/*
			 * Persist where this pass stopped so the next pass resumes past it
			 * (cleaner is the sole reader/writer of scan_resume_seg, so no lock
			 * is needed; same-process happens-before across passes).
			 */
			undo_cleaner_state->scan_resume_seg = seg;

			if (fence_aborted)
				cluster_undo_horizon_note_pass_abort();
		}
	}

pass_account:
	Assert(undo_cleaner_state != NULL);
	LWLockAcquire(&undo_cleaner_state->lwlock, LW_EXCLUSIVE);
	undo_cleaner_state->pass_count++;
	undo_cleaner_state->shmem_tt_slots_gcd += stats.shmem_tt_slots_gcd;
	undo_cleaner_state->header_tt_slots_below_horizon += stats.header_tt_slots_below_horizon;
	undo_cleaner_state->segments_marked_recyclable += stats.segments_marked_recyclable;
	undo_cleaner_state->stale_active_skipped += stats.stale_active_skipped;
	LWLockRelease(&undo_cleaner_state->lwlock);

	/*
	 * L213 pinned-horizon observability: a pass that found retained
	 * inventory but made zero recycle progress means a long reader is
	 * pinning the horizon.  LOG once per pinned episode (re-arms when
	 * progress resumes); counters above stay per-event.
	 */
	{
		static bool pinned_logged = false;
		bool pinned = (stats.header_retained_committed > 0 && stats.shmem_tt_slots_gcd == 0
					   && stats.header_tt_slots_below_horizon == 0
					   && stats.segments_marked_recyclable == 0);

		if (pinned && !pinned_logged) {
			pinned_logged = true;
			ereport(LOG, (errmsg("cluster undo cleaner: retention horizon pinned; no recyclable "
								 "inventory this pass"),
						  errhint("A long-running snapshot is holding the horizon. Undo segment "
								  "pool may grow until it ends (53R9E at the hard cap).")));
		} else if (!pinned && pinned_logged) {
			pinned_logged = false;
		}
	}
}


void
UndoCleanerMain(void)
{
	/* HC1 reverse defense: we must be a postmaster child. */
	Assert(IsUnderPostmaster);

	MyBackendType = B_UNDO_CLEANER;
	init_ps_display(NULL);

	/*
	 * Standard PG aux-process signal layout (modeled on walwriter.c /
	 * cluster_stats.c):
	 *	SIGHUP  -> ProcessConfigFile reload (interval / enabled GUCs)
	 *	SIGTERM/SIGINT -> ShutdownRequestPending (graceful exit)
	 *	SIGQUIT -> installed by InitPostmasterChild (immediate)
	 *	SIGUSR1 -> ignored (this stage uses direct signals + latch wakeups,
	 *	           not ProcSignal reasons)
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	/* SIGQUIT installed by InitPostmasterChild */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, SIG_IGN);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGCHLD, SIG_DFL);

	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	if (undo_cleaner_state == NULL)
		ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster undo cleaner shmem region not attached"),
						errhint("cluster_undo_cleaner_shmem_init() must run during "
								"CreateSharedMemoryAndSemaphores().")));

	/* Publish SPAWNING (records pid + spawned_at + latch). */
	undo_cleaner_publish_status(UNDO_CLEANER_SPAWNING);

	CLUSTER_INJECTION_POINT("undo-cleaner-ready-publish");

	undo_cleaner_publish_status(UNDO_CLEANER_READY);

	/*
	 * Main loop — WaitLatch with GUC-driven timeout (re-read each
	 * iteration so SIGHUP propagates on the next tick).  A set latch
	 * is either a pressure wakeup (Q8) or a procsignal; both just
	 * cause an immediate pass.
	 */
	for (;;) {
		int rc;
		int timeout_ms;

		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending) {
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (ShutdownRequestPending || undo_cleaner_shutdown_requested())
			break;

		undo_cleaner_advance_liveness_tick();

		CLUSTER_INJECTION_POINT("undo-cleaner-main-loop-iter");

		undo_cleaner_run_pass();

		/*
		 * interval 0 = pressure-wakeup only (Q8): block without
		 * timeout; otherwise wake at the configured cadence.
		 */
		timeout_ms = cluster_undo_cleaner_interval_ms;
		rc = WaitLatch(
			MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | (timeout_ms > 0 ? WL_TIMEOUT : 0),
			timeout_ms > 0 ? timeout_ms : -1L, WAIT_EVENT_CLUSTER_BGPROC_UNDO_CLEANER_MAIN_LOOP);
		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}

	CLUSTER_INJECTION_POINT("undo-cleaner-shutdown-pre");

	/* Graceful shutdown path — HC5 normal exit. */
	undo_cleaner_publish_status(UNDO_CLEANER_SHUTTING_DOWN);

	undo_cleaner_publish_status(UNDO_CLEANER_EXITED);

	CLUSTER_INJECTION_POINT("undo-cleaner-shutdown-post");

	/*
	 * proc_exit(0) -> reaper sees WIFEXITED + WEXITSTATUS=0 -> normal
	 * exit -> ServerLoop respawns us on the next iteration (HC5).
	 */
	proc_exit(0);
}


/* ============================================================
 * D6 counter accessors (single-writer uint64 under the region lock;
 * read for dump_undo + tests).
 * ============================================================ */

#define UNDO_CLEANER_COUNTER_ACCESSOR(name)                                                        \
	uint64 cluster_undo_cleaner_##name(void)                                                       \
	{                                                                                              \
		uint64 v;                                                                                  \
		if (undo_cleaner_state == NULL)                                                            \
			return 0;                                                                              \
		LWLockAcquire(&undo_cleaner_state->lwlock, LW_SHARED);                                     \
		v = undo_cleaner_state->name;                                                              \
		LWLockRelease(&undo_cleaner_state->lwlock);                                                \
		return v;                                                                                  \
	}

UNDO_CLEANER_COUNTER_ACCESSOR(pass_count)
UNDO_CLEANER_COUNTER_ACCESSOR(shmem_tt_slots_gcd)
UNDO_CLEANER_COUNTER_ACCESSOR(segments_marked_recyclable)
UNDO_CLEANER_COUNTER_ACCESSOR(stale_active_skipped)


/* ============================================================
 * D6 segment-scan wait event wrappers (mirrors the spec-3.11
 * cluster_tt_durable_io_wait_* indirection so cluster_unit binaries
 * can stub them without pulling pgstat).
 * ============================================================ */

void
cluster_undo_cleaner_scan_wait_start(void)
{
	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_UNDO_CLEANER_SEGMENT_SCAN);
}

void
cluster_undo_cleaner_scan_wait_end(void)
{
	pgstat_report_wait_end();
}

#endif /* USE_PGRAC_CLUSTER */
