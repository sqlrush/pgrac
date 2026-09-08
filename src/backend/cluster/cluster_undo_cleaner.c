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

#include "access/xact.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/auxprocess.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/bufmgr.h"
#include "storage/lock.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/hsearch.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h" /* WAIT_EVENT_CLUSTER_BGPROC_UNDO_CLEANER_MAIN_LOOP */

#include "cluster/cluster_guc.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_mode.h"			 /* cluster_storage_mode_enabled */
#include "cluster/cluster_reconfig.h"		 /* ordinary/replacement write gate */
#include "cluster/cluster_semantic_activation.h" /* operation-scoped modifier gate */
#include "cluster/cluster_undo_horizon.h"	 /* cluster floor + fence (spec-5.22e D5-3) */
#include "cluster/cluster_undo_retention.h"	 /* horizon (C17: once per pass) */
#include "cluster/cluster_tt_slot.h"		 /* current TT segment (exclusion) */
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_undo_record_api.h" /* active segment + advance (D3) */
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_undo_cleaner.h"
#include "cluster/storage/cluster_undo_block0.h"
#include "cluster/storage/cluster_undo_block0_current.h"


/*
 * Module-level pointer to the Undo Cleaner shmem region.  Set by
 * cluster_undo_cleaner_shmem_init().  NULL only inside the
 * cluster_unit test harness when init was not invoked.
 */
static UndoCleanerSharedState *undo_cleaner_state = NULL;
static int undo_cleaner_worker = -1;

StaticAssertDecl(CLUSTER_UNDO_CLEANER_WORKER_TYPES == CLUSTER_CTRC_CLEANER_WORKERS,
				 "each terminal-supply shard needs one distinct auxiliary process");


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
		ConditionVariableInit(&undo_cleaner_state->capacity_cv);
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
	Latch *latches[CLUSTER_UNDO_CLEANER_WORKER_TYPES];

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
	for (unsigned i = 0; i < lengthof(latches); i++)
		latches[i] = undo_cleaner_state->workers[i].latch;
	LWLockRelease(&undo_cleaner_state->lwlock);

	for (unsigned i = 0; i < lengthof(latches); i++)
		if (latches[i] != NULL)
			SetLatch(latches[i]);
}

bool
cluster_undo_cleaner_worker_snapshot(unsigned worker_id, UndoCleanerWorkerState *out)
{
	if (out == NULL)
		return false;
	MemSet(out, 0, sizeof(*out));
	if (worker_id >= CLUSTER_UNDO_CLEANER_WORKER_TYPES || undo_cleaner_state == NULL)
		return false;
	LWLockAcquire(&undo_cleaner_state->lwlock, LW_SHARED);
	*out = undo_cleaner_state->workers[worker_id];
	LWLockRelease(&undo_cleaner_state->lwlock);
	return true;
}

static void
undo_cleaner_note_held_lwlock(LWLock *lock pg_attribute_unused(),
							  LWLockMode mode pg_attribute_unused(), void *context)
{
	*(bool *)context = true;
}

/* No blocking resource may survive into capacity sleep. Inspect only the
 * backend's LOCALLOCK tag/count; shared lock/proclock pointers can be stale
 * in zero-count entries and are deliberately never dereferenced here. */
static bool
undo_cleaner_capacity_context_safe(void)
{
	HTAB *locks;
	HASH_SEQ_STATUS scan;
	LOCALLOCK *lock;
	LOCKTAG own_xid, own_vxid;
	VirtualTransactionId vxid;
	TransactionId xid;
	bool held_lwlock = false;

	if (MyBackendType != B_BACKEND || MyProc == NULL || CritSectionCount != 0
		|| InterruptHoldoffCount != 0 || cluster_buffer_backend_has_pins()
		|| cluster_undo_block0_current_backend_has_guards()
		|| cluster_undo_block0_backend_has_resources()
		|| cluster_semantic_activation_backend_has_admission())
		return false;
	ForEachLWLockHeldByMe(undo_cleaner_note_held_lwlock, &held_lwlock);
	if (held_lwlock)
		return false;
	locks = GetLockMethodLocalHash();
	if (locks == NULL)
		return true;
	xid = GetTopTransactionIdIfAny();
	SET_LOCKTAG_TRANSACTION(own_xid, xid);
	GET_VXID_FROM_PGPROC(vxid, *MyProc);
	SET_LOCKTAG_VIRTUALTRANSACTION(own_vxid, vxid);
	hash_seq_init(&scan, locks);
	while ((lock = hash_seq_search(&scan)) != NULL) {
		const LOCKTAG *tag = &lock->tag.lock;
		bool allowed = false;

		if (lock->nLocks == 0)
			continue;
		if (lock->nLocks > 0 && tag->locktag_lockmethodid == DEFAULT_LOCKMETHOD
			&& !lock->holdsStrongLockCount) {
			if (tag->locktag_type == LOCKTAG_RELATION)
				allowed = lock->tag.mode == AccessShareLock || lock->tag.mode == RowShareLock
						  || lock->tag.mode == RowExclusiveLock;
			else if (lock->tag.mode == ExclusiveLock)
				allowed = (TransactionIdIsNormal(xid) && memcmp(tag, &own_xid, sizeof(*tag)) == 0)
						  || (VirtualTransactionIdIsValid(vxid)
							  && memcmp(tag, &own_vxid, sizeof(*tag)) == 0);
		}
		if (!allowed) {
			hash_seq_term(&scan);
			return false;
		}
	}
	return true;
}

static bool
undo_cleaner_capacity_floor(ClusterUndoHorizonFloor *floor)
{
	ClusterUndoHorizonReportView views[CLUSTER_MAX_NODES];
	uint8 required[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	ClusterUndoHorizonStallReason reason;
	uint64 epoch;
	int32 blame;
	int nviews;
	SCN horizon;

	if (!cluster_undo_retention_horizon_enabled)
		return false;
	horizon = cluster_undo_retention_horizon();
	nviews = cluster_undo_horizon_sample_views(views, CLUSTER_MAX_NODES);
	return cluster_undo_horizon_required_members(required, &epoch)
		   && cluster_undo_horizon_cluster_floor(horizon, views, nviews, required, cluster_node_id,
												 epoch, (uint64)GetCurrentTimestamp(),
												 (uint32)cluster_lmon_main_loop_interval, floor,
												 &reason, &blame)
				  == CLUSTER_UNDO_HORIZON_FOLD_OK
		   && !cluster_undo_horizon_epoch_fence_tripped(floor->epoch);
}

bool
cluster_undo_cleaner_wait_for_capacity(uint32 segment_id, ClusterCtrcTxnKeyV1 *continuation)
{
	bool retry = false;

	if (undo_cleaner_state == NULL || continuation == NULL)
		return false;
	if (!undo_cleaner_capacity_context_safe()) {
		LWLockAcquire(&undo_cleaner_state->lwlock, LW_EXCLUSIVE);
		undo_cleaner_state->capacity_wait_refused_context++;
		LWLockRelease(&undo_cleaner_state->lwlock);
		return false;
	}
	ConditionVariablePrepareToSleep(&undo_cleaner_state->capacity_cv);
	PG_TRY();
	{
		ClusterUndoHorizonFloor floor;
		ClusterCtrcCapacityProbeResult proof = CLUSTER_CTRC_CAPACITY_REFUSE;

		CHECK_FOR_INTERRUPTS();
		if (cluster_undo_cleaner_enabled && cluster_storage_mode_enabled()
			&& undo_cleaner_capacity_floor(&floor))
			proof = cluster_ctrc_capacity_probe_current(segment_id, floor.scn, floor.epoch,
														continuation);
		if (proof == CLUSTER_CTRC_CAPACITY_RETRY)
			retry = true;
		else if (proof == CLUSTER_CTRC_CAPACITY_WAIT && undo_cleaner_capacity_context_safe()) {
			unsigned worker = (continuation->segment_id - 1) % CLUSTER_UNDO_CLEANER_WORKER_TYPES;
			bool supplied;

			LWLockAcquire(&undo_cleaner_state->lwlock, LW_EXCLUSIVE);
			supplied = !undo_cleaner_state->shutdown_requested
					   && undo_cleaner_state->workers[0].status == UNDO_CLEANER_READY
					   && undo_cleaner_state->workers[worker].status == UNDO_CLEANER_READY;
			if (supplied)
				undo_cleaner_state->capacity_wait_entered++;
			else
				undo_cleaner_state->capacity_wait_refused_proof++;
			LWLockRelease(&undo_cleaner_state->lwlock);
			if (supplied) {
				cluster_undo_cleaner_wakeup();
				/* Timeout is only the existing floor-report recheck cadence,
				 * not a new failure deadline or a reason to assume FREE. */
				(void)ConditionVariableTimedSleep(&undo_cleaner_state->capacity_cv,
												  Max(cluster_lmon_main_loop_interval, 200),
												  WAIT_EVENT_CLUSTER_BGPROC_UNDO_CLEANER_MAIN_LOOP);
				LWLockAcquire(&undo_cleaner_state->lwlock, LW_EXCLUSIVE);
				undo_cleaner_state->capacity_wait_repolled++;
				LWLockRelease(&undo_cleaner_state->lwlock);
				retry = true;
			}
		} else {
			LWLockAcquire(&undo_cleaner_state->lwlock, LW_EXCLUSIVE);
			if (proof == CLUSTER_CTRC_CAPACITY_WAIT)
				undo_cleaner_state->capacity_wait_refused_context++;
			else
				undo_cleaner_state->capacity_wait_refused_proof++;
			LWLockRelease(&undo_cleaner_state->lwlock);
		}
	}
	PG_FINALLY();
	{
		ConditionVariableCancelSleep();
	}
	PG_END_TRY();
	return retry;
}


UndoCleanerStatus
cluster_undo_cleaner_status(void)
{
	UndoCleanerStatus result;

	if (undo_cleaner_state == NULL)
		return UNDO_CLEANER_NOT_STARTED;

	LWLockAcquire(&undo_cleaner_state->lwlock, LW_SHARED);
	result = undo_cleaner_state->workers[0].status;
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
	result = undo_cleaner_state->workers[0].pid;
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
	result = undo_cleaner_state->workers[0].spawned_at;
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
	result = undo_cleaner_state->workers[0].ready_at;
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
	result = undo_cleaner_state->workers[0].last_liveness_tick_at;
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
	result = undo_cleaner_state->workers[0].main_loop_iters;
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
	UndoCleanerWorkerState *worker;

	Assert(undo_cleaner_state != NULL);
	Assert(undo_cleaner_worker >= 0 && undo_cleaner_worker < CLUSTER_UNDO_CLEANER_WORKER_TYPES);

	LWLockAcquire(&undo_cleaner_state->lwlock, LW_EXCLUSIVE);
	worker = &undo_cleaner_state->workers[undo_cleaner_worker];
	/*
	 * F16 (spec-1.14 lineage): SPAWNING marks a new incarnation —
	 * refresh every incarnation-scoped field unconditionally so SQL
	 * views never report stale PID/timestamps after a ServerLoop
	 * respawn.
	 */
	if (status == UNDO_CLEANER_SPAWNING) {
		MemSet(worker, 0, sizeof(*worker));
		worker->pid = MyProcPid;
		worker->spawned_at = now;
		worker->latch = (MyProc != NULL) ? &MyProc->procLatch : NULL;
	} else if (status == UNDO_CLEANER_READY) {
		worker->ready_at = now;
	} else if (status == UNDO_CLEANER_SHUTTING_DOWN) {
		/* stop accepting pressure wakeups against a dying latch */
		worker->latch = NULL;
	}
	worker->status = status;
	LWLockRelease(&undo_cleaner_state->lwlock);
	ConditionVariableBroadcast(&undo_cleaner_state->capacity_cv);
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
	undo_cleaner_state->workers[undo_cleaner_worker].last_liveness_tick_at = now;
	undo_cleaner_state->workers[undo_cleaner_worker].main_loop_iters++;
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
 *
 *	Returns true when the recycle stage could NOT run to completion
 *	against a proven cluster floor (fold stalled, member set unstable,
 *	or the F-D2 epoch fence aborted the pass).  These causes normally
 *	clear at the horizon-report cadence (~one LMON tick: epoch views
 *	converge and every peer re-publishes), so the caller retries the
 *	pass at that cadence instead of sleeping a full recycle interval —
 *	the S3 undo-pool exhaustion amplifier (a ~1s transient froze
 *	cluster-wide COMMITTED recycling for 30-60s).  Rule 8.A unchanged:
 *	a retry still recycles ONLY on a proven floor; a persistent cause
 *	keeps every retry stalled.
 *
 *	*out_work_remaining (TT lane, S3 idle-peer floor pin H2): true when
 *	this pass BOTH consumed its whole segment batch AND made recycle
 *	progress — the backlog is bigger than one batch, so the caller
 *	re-runs immediately (pressure-driven continuous mode) instead of
 *	sleeping the recycle interval.  Pure cadence: what may be recycled
 *	is still decided only by the proven floor above.  A pinned pass
 *	(zero progress) or a drained backlog (batch not exhausted) reports
 *	false, so the continuous mode can never busy-spin.
 */
static bool
undo_cleaner_run_pass(bool *out_work_remaining)
{
	ClusterUndoCleanerPassStats stats;
	ClusterSemanticAdmissionToken modifier_token;
	bool writable_admission;
	bool floor_retry_needed = false;
	bool ctrc_local_progress;

	*out_work_remaining = false;

	if (!cluster_undo_cleaner_enabled)
		return false;
	if (!cluster_storage_mode_enabled())
		return false;

	/* Each bound worker advances only its canonical TT segment shard.
	 * The helper holds no cleaner lock while sampling or doing physical work. */
	ctrc_local_progress = cluster_ctrc_cleaner_run_pass();
	/* SetLatch notifications coalesce.  A completed local CTRC edge proves
	 * useful work remains in the bounded pipeline, so keep this sole owner
	 * running until a pass makes no local progress.  Remote enqueue is not a
	 * completed edge and is excluded by cluster_ctrc_cleaner_run_pass(). */
	*out_work_remaining = ctrc_local_progress;
	/* Only coordinator zero owns GC, floor capture, the scan cursor and
	 * segment advancement. Per-worker progress never duplicates those roles. */
	if (undo_cleaner_worker != 0)
		return false;
	writable_admission
		= cluster_reconfig_self_join_gate_verdict() == CLUSTER_JOIN_GATE_ALLOW;
	if (cluster_semantic_activation_modifier_enter(writable_admission, &modifier_token)
		!= CLUSTER_SEMANTIC_ADMISSION_OK)
		return false;

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
			floor_retry_needed = true;
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

		if (!cluster_semantic_activation_modifier_recheck(
				&modifier_token,
				cluster_reconfig_self_join_gate_verdict() == CLUSTER_JOIN_GATE_ALLOW)
			|| !cluster_tt_slot_gc_current_pass(horizon, floor.epoch, &stats)) {
			floor_retry_needed = true;
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
				if (cluster_undo_record_segment_commit_on_rollover
					&& cluster_semantic_activation_modifier_recheck(
						&modifier_token,
						cluster_reconfig_self_join_gate_verdict()
							== CLUSTER_JOIN_GATE_ALLOW))
					cluster_undo_segment_advance_committed(cur);

				{
					ClusterUndoSegTryRecycle rr;

					if (!cluster_semantic_activation_modifier_recheck(
							&modifier_token,
							cluster_reconfig_self_join_gate_verdict()
								== CLUSTER_JOIN_GATE_ALLOW)) {
						fence_aborted = true;
						break;
					}
					rr = cluster_undo_segment_advance_recyclable(cur, horizon, floor.epoch);

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

			if (fence_aborted) {
				floor_retry_needed = true;
				cluster_undo_horizon_note_pass_abort();
			} else if (batch == 0
					   && (stats.segments_marked_recyclable > 0 || stats.shmem_tt_slots_gcd > 0
						   || stats.header_tt_slots_below_horizon > 0)) {
				/*
				 * H2 continuous mode: the batch budget ran out while
				 * recycle progress was still being made -- more eligible
				 * inventory is waiting than one batch covers.  Ask the
				 * caller to re-run now rather than let a write storm
				 * outrun the interval cadence.
				 */
				*out_work_remaining = true;
			}
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

	if (stats.shmem_tt_slots_gcd > 0 || stats.segments_marked_recyclable > 0)
		ConditionVariableBroadcast(&undo_cleaner_state->capacity_cv);

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

	cluster_semantic_activation_leave(&modifier_token);
	return floor_retry_needed;
}


void
UndoCleanerMain(void)
{
	/* HC1 reverse defense: we must be a postmaster child. */
	Assert(IsUnderPostmaster);
	undo_cleaner_worker = ClusterUndoCleanerWorkerIdForType(MyAuxProcType);
	if (undo_cleaner_worker < 0 || !cluster_ctrc_cleaner_bind_worker((unsigned)undo_cleaner_worker))
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("invalid undo cleaner worker identity")));

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
		bool floor_retry;
		bool work_remaining;

		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending) {
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (ShutdownRequestPending || undo_cleaner_shutdown_requested())
			break;

		undo_cleaner_advance_liveness_tick();

		CLUSTER_INJECTION_POINT("undo-cleaner-main-loop-iter");

		floor_retry = undo_cleaner_run_pass(&work_remaining);
		LWLockAcquire(&undo_cleaner_state->lwlock, LW_EXCLUSIVE);
		undo_cleaner_state->workers[undo_cleaner_worker].local_completed_passes
			= cluster_ctrc_cleaner_local_passes();
		undo_cleaner_state->workers[undo_cleaner_worker].local_progress_events
			= cluster_ctrc_cleaner_local_progress();
		LWLockRelease(&undo_cleaner_state->lwlock);

		/*
		 * TT lane H2 (pressure-driven continuous mode): a pass that
		 * exhausted its segment batch while still recycling has a backlog
		 * bigger than one batch -- under a write storm the interval
		 * cadence is orders of magnitude too slow (12k xacts/s consume a
		 * 48-slot segment every ~4ms), so loop straight into the next
		 * pass.  The loop head re-checks interrupts / reload / shutdown,
		 * and the signal requires PROGRESS, so a pinned or stalled floor
		 * always falls through to the waits below.
		 */
		if (work_remaining)
			continue;

		/*
		 * interval 0 = pressure-wakeup only (Q8): block without
		 * timeout; otherwise wake at the configured cadence.
		 *
		 * TT lane (P1#4): a pass that could not prove the cluster floor
		 * (stall / member churn / F-D2 abort) re-arms at the horizon-
		 * report cadence instead — that is when new reports and epoch
		 * convergence can actually arrive (one LMON tick).  Sleeping the
		 * full recycle interval here turned ~1s transients into 30-60s
		 * cluster-wide COMMITTED-recycle freezes (S3 undo-pool
		 * exhaustion, 25,715 x 53R9E on node0).  Fail-closed is
		 * untouched: the retried pass re-proves or re-stalls; this also
		 * bounds the interval=0 mode, whose stalled pass previously
		 * waited on a latch nobody was obliged to set.
		 */
		timeout_ms = cluster_undo_cleaner_interval_ms;
		if (floor_retry) {
			int retry_ms = Max(cluster_lmon_main_loop_interval, 200);

			if (timeout_ms <= 0 || retry_ms < timeout_ms)
				timeout_ms = retry_ms;
		}
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
UNDO_CLEANER_COUNTER_ACCESSOR(capacity_wait_entered)
UNDO_CLEANER_COUNTER_ACCESSOR(capacity_wait_repolled)
UNDO_CLEANER_COUNTER_ACCESSOR(capacity_wait_refused_context)
UNDO_CLEANER_COUNTER_ACCESSOR(capacity_wait_refused_proof)
UNDO_CLEANER_COUNTER_ACCESSOR(header_tt_slots_below_horizon) /* spec-5.22e D5-5 */


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
