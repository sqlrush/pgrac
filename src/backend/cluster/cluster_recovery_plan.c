/*-------------------------------------------------------------------------
 *
 * cluster_recovery_plan.c
 *	  pgrac Recovery Coordinator skeleton -- plan pass (spec-4.3).
 *
 *	  cluster_recovery_plan_generate() runs ONCE per plain local
 *	  startup in the startup process (InitWalRecovery, right after the
 *	  spec-4.1 RL1 reader hook; same gate, so archive recovery and
 *	  standby mode never produce a plan).  It preads all 128 registry
 *	  slots (spec-4.2 reader API), classifies each thread with the
 *	  pure §3.2 truth table (cluster_recovery_plan.h), publishes the
 *	  aggregate into a small shmem mirror, and LOGs a one-line
 *	  summary.  The plan is OBSERVATIONAL ONLY: nothing acts on it in
 *	  this stage (merged replay = spec-4.5; worker fork = spec-4.4),
 *	  so every failure path is fail-open -- WARNING, never an error
 *	  that blocks startup.  Persistent-format damage is still caught
 *	  by the spec-4.1/4.2 FATAL gates which run before this pass.
 *
 *	  Shmem ordering: single writer (startup process).  published=0 ->
 *	  write barrier -> plan copy -> write barrier -> published=1; the
 *	  snapshot reader pairs with read barriers.  During a regeneration
 *	  window (crash-loop restart) no SQL backends exist under the
 *	  plain-local gate, so torn reads are not reachable in practice;
 *	  the barriers keep the protocol honest anyway.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_recovery_plan.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.3-recovery-coordinator-skeleton.md FROZEN v1.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_guc.h"
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_recovery_worker.h" /* pool lives in this wrapper (spec-4.4 D5) */
#include "cluster/cluster_scn.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_thread_recovery.h" /* ClusterThreadRecReplayState (slot init) */
#include "cluster/cluster_wal_thread.h"
#include "lib/stringinfo.h"
#include "port/atomics.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"

typedef struct ClusterRecoveryPlanShmem {
	pg_atomic_uint32 published;
	ClusterRecoveryPlan plan;
	/* spec-4.4 D5 (round-1 P1-2): the worker pool shares this region;
	 * the plan semantics and the ClusterRecoveryPlan ABI are untouched. */
	ClusterRecoveryWorkerPool pool;
	/* spec-4.11 3b-4b: per-dead-thread online replay state shares this
	 * region too; indexed by thread id 1..CLUSTER_RECOVERY_PLAN_THREADS,
	 * [0] unused (the plan/pool ABIs are untouched). */
	ClusterThreadReplaySlot thread_replay[CLUSTER_RECOVERY_PLAN_THREADS + 1];
} ClusterRecoveryPlanShmem;

static ClusterRecoveryPlanShmem *cluster_recovery_plan_shmem = NULL;

static Size
cluster_recovery_plan_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterRecoveryPlanShmem));
}

static void
cluster_recovery_plan_shmem_init(void)
{
	bool found;

	cluster_recovery_plan_shmem = (ClusterRecoveryPlanShmem *)ShmemInitStruct(
		"pgrac recovery plan", cluster_recovery_plan_shmem_size(), &found);
	if (!found) {
		int slot;

		pg_atomic_init_u32(&cluster_recovery_plan_shmem->published, 0);
		memset(&cluster_recovery_plan_shmem->plan, 0, sizeof(ClusterRecoveryPlan));
		memset(&cluster_recovery_plan_shmem->pool, 0, sizeof(ClusterRecoveryWorkerPool));
		for (slot = 0; slot < CLUSTER_RECOVERY_WORKER_MAX_SLOTS; slot++)
			pg_atomic_init_u32(&cluster_recovery_plan_shmem->pool.slot_state[slot],
							   CLUSTER_RECOVERY_WORKER_UNUSED);

		/* spec-4.11 3b-4b: every replay slot starts IDLE / unstamped. */
		memset(cluster_recovery_plan_shmem->thread_replay, 0,
			   sizeof(cluster_recovery_plan_shmem->thread_replay));
		for (slot = 0; slot <= CLUSTER_RECOVERY_PLAN_THREADS; slot++) {
			pg_atomic_init_u32(&cluster_recovery_plan_shmem->thread_replay[slot].state,
							   CLUSTER_THREADREC_REPLAY_IDLE);
			pg_atomic_init_u64(&cluster_recovery_plan_shmem->thread_replay[slot].episode_epoch, 0);
		}
	}
}

static const ClusterShmemRegion cluster_recovery_plan_region = {
	.name = "pgrac recovery plan",
	.size_fn = cluster_recovery_plan_shmem_size,
	.init_fn = cluster_recovery_plan_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_recovery_plan",
	.reserved_flags = 0,
};

void
cluster_recovery_plan_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_recovery_plan_region);
}

/*
 * publish -- copy the locally-built plan into the shmem mirror.
 */
static void
publish_plan(const ClusterRecoveryPlan *plan)
{
	pg_atomic_write_u32(&cluster_recovery_plan_shmem->published, 0);
	pg_write_barrier();
	memcpy(&cluster_recovery_plan_shmem->plan, plan, sizeof(ClusterRecoveryPlan));
	pg_write_barrier();
	pg_atomic_write_u32(&cluster_recovery_plan_shmem->published, 1);
}

/*
 * cluster_recovery_plan_generate
 *
 *	dbstate_at_startup / local_recovery_needed are captured by the
 *	caller from ControlFile at the hook site (P1-3 observability: this
 *	pass runs BEFORE InRecovery is determined, so a plan generated on
 *	a clean local start must be readable as exactly that).
 */
void
cluster_recovery_plan_generate(uint32 dbstate_at_startup, bool local_recovery_needed)
{
	ClusterRecoveryPlan plan;
	StringInfoData candidates;
	uint16 own_thread;
	int64 now_us;
	uint16 tid;
	XLogRecPtr max_scn_lsn = 0; /* scn_recovery_cmp tie-break carriers */
	NodeId max_scn_node = 0;

	if (cluster_wal_threads_dir == NULL || cluster_wal_threads_dir[0] == '\0')
		return;
	own_thread = cluster_wal_thread_id();
	if (own_thread == XLP_THREAD_ID_LEGACY)
		return;
	if (cluster_recovery_plan_shmem == NULL)
		return;

	memset(&plan, 0, sizeof(plan));
	plan.own_thread = own_thread;
	plan.dbstate_at_startup = dbstate_at_startup;
	plan.local_recovery_needed = local_recovery_needed;
	now_us = (int64)GetCurrentTimestamp();
	plan.generated_at = now_us;

	/*
	 * Defensive pass-level gate.  Under today's startup ordering the
	 * spec-4.2 ensure() FATAL gate has already validated the registry
	 * before the startup process runs, so this branch is not reachable
	 * in practice; it exists so a future reordering degrades to an
	 * honest 'failed' plan instead of 128 bogus EMPTY verdicts
	 * (read_slot maps a missing file to EMPTY).
	 */
	if (!cluster_wal_state_registry_ready()) {
		plan.failed = true;
		plan.generated = true;
		publish_plan(&plan);
		ereport(WARNING, (errcode(ERRCODE_CLUSTER_WAL_STATE_IO_FAILURE),
						  errmsg("recovery plan pass could not read the WAL state registry"),
						  errhint("The plan is observational only; startup continues.  Check the "
								  "shared WAL storage.")));
		return;
	}

	initStringInfo(&candidates);
	for (tid = 1; tid <= CLUSTER_RECOVERY_PLAN_THREADS; tid++) {
		ClusterWalStateSlot slot;
		ClusterWalSlotVerdict v;
		ClusterRecoveryThreadVerdict verdict;

		v = cluster_wal_state_read_slot(tid, &slot);
		verdict = cluster_recovery_classify_slot(v, &slot, own_thread, tid, now_us,
												 cluster_recovery_stale_active_ms);
		plan.verdict[tid] = (uint8)verdict;
		plan.threads_scanned++;

		if (v == CLUSTER_WAL_SLOT_OK) {
			if (slot.highest_lsn > plan.max_highest_lsn)
				plan.max_highest_lsn = slot.highest_lsn;
			/* SCN ordering goes through the spec-1.15 recovery comparator
			 * (raw operators are CI-gated); the three-level tie-break
			 * (local_scn -> LSN -> node) carries the winner's lsn/node. */
			if (scn_recovery_cmp((SCN)slot.highest_scn, (XLogRecPtr)slot.highest_lsn,
								 (NodeId)(tid - 1), (SCN)plan.max_highest_scn, max_scn_lsn,
								 max_scn_node)
				> 0) {
				plan.max_highest_scn = slot.highest_scn;
				max_scn_lsn = (XLogRecPtr)slot.highest_lsn;
				max_scn_node = (NodeId)(tid - 1);
			}
		}

		switch (verdict) {
		case CLUSTER_RECOVERY_THREAD_CLEAN:
			plan.n_clean++;
			break;
		case CLUSTER_RECOVERY_THREAD_EMPTY:
			plan.n_empty++;
			break;
		case CLUSTER_RECOVERY_THREAD_CRASHED_CANDIDATE:
			plan.n_crashed_candidate++;
			cluster_recovery_plan_candidate_set(&plan, tid);
			appendStringInfo(&candidates, "%s%u", candidates.len > 0 ? "," : "", (unsigned)tid);
			break;
		case CLUSTER_RECOVERY_THREAD_ALIVE:
			plan.n_alive++;
			break;
		case CLUSTER_RECOVERY_THREAD_UNKNOWN:
			plan.n_unknown++;
			break;
		default:
			break;
		}

		if (verdict != CLUSTER_RECOVERY_THREAD_EMPTY)
			ereport(DEBUG1, (errmsg("recovery plan: thread %u verdict %d (slot verdict %d)",
									(unsigned)tid, (int)verdict, (int)v)));
	}
	plan.generated = true;

	publish_plan(&plan);

	ereport(LOG, (errmsg("recovery plan (not acted upon): own thread %u, %u clean, %u empty, "
						 "%u crashed candidate%s%s%s, %u alive, %u unknown",
						 (unsigned)own_thread, (unsigned)plan.n_clean, (unsigned)plan.n_empty,
						 (unsigned)plan.n_crashed_candidate, candidates.len > 0 ? " [" : "",
						 candidates.len > 0 ? candidates.data : "", candidates.len > 0 ? "]" : "",
						 (unsigned)plan.n_alive, (unsigned)plan.n_unknown),
				  plan.n_unknown > 0
					  ? errhint("UNKNOWN slots are never treated as crashed; check the shared "
								"WAL storage if they persist.")
					  : 0));
	pfree(candidates.data);
}

/*
 * cluster_recovery_worker_pool_ptr -- the spec-4.4 worker pool rides
 *	in this region (declared in cluster_recovery_worker.h).
 */
ClusterRecoveryWorkerPool *
cluster_recovery_worker_pool_ptr(void)
{
	if (cluster_recovery_plan_shmem == NULL)
		return NULL;
	return &cluster_recovery_plan_shmem->pool;
}

/*
 * cluster_thread_recovery_replay_slot -- the per-dead-thread online replay slot
 *	(spec-4.11 3b-4b), declared in cluster_recovery_plan.h.  Bounds-checked to
 *	the real thread-id range so a bad id fails closed (NULL) and NEVER aliases
 *	the unused slot 0.  Returns NULL when the region is not attached.
 */
ClusterThreadReplaySlot *
cluster_thread_recovery_replay_slot(uint16 dead_tid)
{
	if (cluster_recovery_plan_shmem == NULL)
		return NULL;
	if (dead_tid < XLP_THREAD_ID_FIRST_REAL || dead_tid > CLUSTER_WAL_THREAD_MAX)
		return NULL;
	return &cluster_recovery_plan_shmem->thread_replay[dead_tid];
}

/*
 * cluster_recovery_plan_snapshot -- acquire-ordered copy for readers.
 */
bool
cluster_recovery_plan_snapshot(ClusterRecoveryPlan *out)
{
	if (cluster_recovery_plan_shmem == NULL)
		return false;
	if (pg_atomic_read_u32(&cluster_recovery_plan_shmem->published) == 0)
		return false;
	pg_read_barrier();
	memcpy(out, &cluster_recovery_plan_shmem->plan, sizeof(ClusterRecoveryPlan));
	return true;
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
