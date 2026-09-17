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
#include "cluster/cluster_clean_leave.h"
#include "miscadmin.h"
#include "cluster/cluster_control_root.h" /* RF-ROOT P7 G1b step 4: canonical verdict source */
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_recovery_worker.h" /* pool lives in this wrapper (spec-4.4 D5) */
#include "cluster/cluster_scn.h"
#include "cluster/cluster_semantic_activation.h" /* bit22 cutover latch (contract §B) */
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_thread_recovery.h" /* ClusterThreadRecReplayState (slot init) */
#include "cluster/cluster_wal_thread.h"
#include "lib/stringinfo.h"
#include "port/atomics.h"
#include "postmaster/bgwriter.h" /* CheckPointTimeout (liveness threshold, contract) */
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
	/* spec-4.11 D5: region-level cumulative online thread-recovery counters
	 * (observability); shares the region, no new region (Q3). */
	ClusterThreadRecoveryCounters thread_recovery_counters;
} ClusterRecoveryPlanShmem;

static ClusterRecoveryPlanShmem *cluster_recovery_plan_shmem = NULL;

static void
recovery_stop_note(ClusterNormalStopPollResult result, const char *domain, uint64 key,
				   const char *reason, ClusterNormalStopPollResult *aggregate,
				   const char **first_domain, uint64 *first_key, const char **first_reason)
{
	if (result < *aggregate) {
		*aggregate = result;
		*first_domain = domain;
		*first_key = key;
		*first_reason = reason;
	}
}

/* Original per-slot ownership, not the observational plan or counters. Native
 * Startup/worker lifetime and the actor seal are separate enclosing cuts;
 * only the original worker/LMON retires work. No replay, launch or reset here. */
ClusterNormalStopPollResult
cluster_recovery_normal_stop_poll(const char **domain, uint64 *key, const char **reason)
{
	ClusterNormalStopPollResult aggregate = CLUSTER_NORMAL_STOP_INVALID;
	const char *first_domain = "RECOVERY", *first_reason = "RECOVERY_OWNER_OR_SHMEM";
	uint64 first_key = 0;
	if (!IsUnderPostmaster || MyBackendType != B_LMON || cluster_recovery_plan_shmem == NULL)
		goto done;
	aggregate = CLUSTER_NORMAL_STOP_READY;
	first_reason = "NONE";
	for (unsigned i = 0; i < CLUSTER_RECOVERY_WORKER_MAX_SLOTS; i++) {
		uint32 state = pg_atomic_read_u32(&cluster_recovery_plan_shmem->pool.slot_state[i]);
		ClusterNormalStopPollResult result = CLUSTER_NORMAL_STOP_READY;
		if (state > CLUSTER_RECOVERY_WORKER_SPAWN_FAILED)
			result = CLUSTER_NORMAL_STOP_INVALID;
		else if (state == CLUSTER_RECOVERY_WORKER_REQUESTED
				 || state == CLUSTER_RECOVERY_WORKER_RUNNING)
			result = CLUSTER_NORMAL_STOP_PENDING;
		/* Validation workers do not replay; DONE/FAILED/SPAWN_FAILED are
		 * terminal observations, never a proof that data recovery succeeded. */
		recovery_stop_note(result, "RECOVERY_WORKER", i,
						   result == CLUSTER_NORMAL_STOP_INVALID ? "RECOVERY_WORKER_STATE_INVALID"
																 : "RECOVERY_WORKER_PENDING",
						   &aggregate, &first_domain, &first_key, &first_reason);
	}
	for (uint16 tid = 0; tid <= CLUSTER_RECOVERY_PLAN_THREADS; tid++) {
		ClusterThreadReplaySlot *slot = &cluster_recovery_plan_shmem->thread_replay[tid];
		uint32 state = pg_atomic_read_u32(&slot->state), after;
		uint64 stamp, stamp_after;
		ClusterNormalStopPollResult result = CLUSTER_NORMAL_STOP_READY;
		const char *why = "THREAD_REPLAY_PENDING";
		pg_read_barrier();
		stamp = pg_atomic_read_u64(&slot->episode_epoch);
		pg_read_barrier();
		after = pg_atomic_read_u32(&slot->state);
		stamp_after = pg_atomic_read_u64(&slot->episode_epoch);
		if (state > CLUSTER_THREADREC_REPLAY_BLOCKED
			|| (tid == 0 && (state != CLUSTER_THREADREC_REPLAY_IDLE || stamp != 0))
			|| (state != CLUSTER_THREADREC_REPLAY_IDLE && stamp == 0)) {
			result = CLUSTER_NORMAL_STOP_INVALID;
			why = "THREAD_REPLAY_STATE_INVALID";
		} else if (state == CLUSTER_THREADREC_REPLAY_BLOCKED) {
			result = CLUSTER_NORMAL_STOP_INVALID;
			why = "THREAD_REPLAY_BLOCKED";
		} else if (state == CLUSTER_THREADREC_REPLAY_REPLAYING || state != after
				   || stamp != stamp_after)
			result = CLUSTER_NORMAL_STOP_PENDING;
		/* Terminal/retired episode stamps and copied projection tokens are
		 * retained history, not buffer pins or a new reader authority. */
		recovery_stop_note(result, "THREAD_REPLAY", tid, why, &aggregate, &first_domain, &first_key,
						   &first_reason);
	}
done:
	if (domain)
		*domain = first_domain;
	if (key)
		*key = first_key;
	if (reason)
		*reason = first_reason;
	return aggregate;
}

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

		/* spec-4.11 D5: cumulative counters start at zero. */
		pg_atomic_init_u64(&cluster_recovery_plan_shmem->thread_recovery_counters.threads_recovered,
						   0);
		pg_atomic_init_u64(&cluster_recovery_plan_shmem->thread_recovery_counters.replay_failclosed,
						   0);
		pg_atomic_init_u64(&cluster_recovery_plan_shmem->thread_recovery_counters.recovered_through,
						   0);
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
	bool bit22_active;

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

	/* RF-ROOT P9 verification (contract): the bit22 latch is shmem-only and dies
	 * with the postmaster — re-arm it from the durable root when the
	 * cutover round completed (ACTIVE + bit22 target) before the dual-path
	 * sample below.  Fail-closed: absent/not-ACTIVE/census-RED roots leave
	 * the gate pre-bit22 (frozen registry authority). */
	cluster_control_root_restore_bit22_latch_if_active();

	/* RF-ROOT P7 (contract §B / follow-up): dual-path by the bit22 cutover
	 * latch, sampled once per pass so one plan is coherent.  false =
	 * pre-bit22 (frozen §17.8: the wal-state registry remains the selected
	 * authority); true = post-bit22 (root-only). */
	bit22_active = cluster_r4_bit22_cutover_active();

	/*
	 * Defensive pass-level gate.  Under today's startup ordering the
	 * spec-4.2 ensure() FATAL gate has already validated the registry
	 * before the startup process runs, so this branch is not reachable
	 * in practice; it exists so a future reordering degrades to an
	 * honest 'failed' plan instead of 128 bogus EMPTY verdicts
	 * (read_slot maps a missing file to EMPTY).  Post-bit22 the registry
	 * is telemetry-only (§17.9), so the precondition no longer applies.
	 */
	if (!bit22_active && !cluster_wal_state_registry_ready()) {
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
		ClusterRecoveryThreadVerdict verdict;
		int read_verdict; /* root result or slot verdict, for the DEBUG1 line */

		if (bit22_active) {
			ClusterControlRootSnapshot snapshot;
			ClusterControlRootReadToken token;
			ClusterControlRootResult root_result;

			/*
			 * Post-bit22 (§17.8 Target OPEN): the plan's per-thread source
			 * is the canonical control root — STRONG read via the contract
			 * §A two-step discovered-identity pattern (the startup process
			 * is the frozen CF(S)-capable recovery admission, AD-023 §4).
			 */
			root_result = cluster_control_root_read_canonical_discovered(tid, &snapshot, &token);
			read_verdict = (int)root_result;
			verdict = cluster_recovery_classify_root_slot(root_result, &snapshot, own_thread, tid,
														  now_us, CheckPointTimeout);

			if (root_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
				|| root_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
				/*
				 * Observation (max_highest_lsn): the registry's highest_lsn
				 * is a write-position watermark the root does not carry; use
				 * the canonical checkpoint/tail bounds as the conservative
				 * observation (Stage 8 contract).
				 */
				if (snapshot.validated_tail_lsn_exclusive > plan.max_highest_lsn)
					plan.max_highest_lsn = snapshot.validated_tail_lsn_exclusive;
				if (snapshot.checkpoint_lower_lsn > plan.max_highest_lsn)
					plan.max_highest_lsn = snapshot.checkpoint_lower_lsn;
				/* follow-up item 3: max_highest_scn has no consumer; the SCN
				 * ordering dimension is removed from correctness. */
			}
		} else {
			ClusterWalStateSlot slot;
			ClusterWalSlotVerdict v;

			/*
			 * Pre-bit22 (frozen §17.8 Source R4 OPEN): the wal-state
			 * registry remains the selected authority — the restored
			 * pre-migration shape (29efc553b0^).  The SCN ordering removed
			 * by follow-up item 3 stays removed (no consumer).
			 */
			v = cluster_wal_state_read_slot(tid, &slot);
			read_verdict = (int)v;
			verdict = cluster_recovery_classify_slot(v, &slot, own_thread, tid, now_us,
													 cluster_recovery_stale_active_ms);
			if (v == CLUSTER_WAL_SLOT_OK) {
				if (slot.highest_lsn > plan.max_highest_lsn)
					plan.max_highest_lsn = slot.highest_lsn;
			}
		}

		plan.verdict[tid] = (uint8)verdict;
		plan.threads_scanned++;

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
			ereport(DEBUG1,
					(errmsg("recovery plan: thread %u verdict %d (read verdict %d, bit22=%d)",
							(unsigned)tid, (int)verdict, read_verdict, bit22_active ? 1 : 0)));
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
					  ? errhint("UNKNOWN verdicts are never treated as crashed; check the shared "
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
 * cluster_thread_recovery_pin_projection -- RF-ROOT P7 (contract §B): pin the
 * canonical-root projection for one dead thread BEFORE the episode freeze.
 * Post-bit22-only: the callers gate on cluster_r4_bit22_cutover_active
 * (pre-bit22 consumers read the registry directly, follow-up minor item).  Must
 * be called from a zero-resource-lock point (LMON tick before grd P1 freeze /
 * startup pre-IR); the caller holds no CF.  The contract §A two-step read
 * (BOOTSTRAP discover + STRONG bound) replaces the inert STRONG+NULL call —
 * the token is minted by the STRONG step, and ABSENT / any read failure
 * fails closed (returns false; the consumer's projection_current then
 * refuses the thread — contract's never-minted/minted-lost semantics live in
 * the post-bit22 branch).  Then stamp the slot under the given episode_epoch;
 * the worker consumes only this immutable projection and never re-acquires
 * CF(S) inside the episode (follow-up item 2, STOP-02 §1.3 projection
 * discipline).  Returns false when the root read fails (the episode then
 * fails closed on this thread) or the slot is absent.
 */
bool
cluster_thread_recovery_pin_projection(uint16 dead_tid, uint64 episode_epoch)
{
	ClusterThreadReplaySlot *slot;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;
	ClusterControlRootResult root_result;

	slot = cluster_thread_recovery_replay_slot(dead_tid);
	if (slot == NULL)
		return false;
	root_result = cluster_control_root_read_canonical_discovered(dead_tid, &snapshot, &token);
	if (root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return false;

	/*
	 * Single-writer (LMON/startup): plain stores via the header-only fill,
	 * then the episode_epoch stamp is the publication fence the consumer
	 * pairs with.
	 */
	cluster_thread_recovery_pin_fill(slot, &snapshot, &token);
	pg_write_barrier();
	pg_atomic_write_u64(&slot->episode_epoch, episode_epoch);
	return true;
}

/*
 * cluster_thread_recovery_projection_current -- RF-ROOT P7 G1b step 4: the
 * episode bgworker's read of the pinned projection.  Fail-closed: returns
 * false unless the slot exists AND is stamped with exactly the current
 * episode (the worker's own launch episode).  The caller then uses the
 * pinned fields directly; it must NOT re-read the canonical root (no CF(S)
 * inside the episode — follow-up item 2).
 */
bool
cluster_thread_recovery_projection_current(uint16 dead_tid, uint64 episode_epoch,
										   ClusterControlRootReadToken *token_out,
										   uint64 *validated_tail_out, uint64 *checkpoint_lower_out,
										   uint64 *lifecycle_out, uint32 *tail_tli_out,
										   uint32 *checkpoint_tli_out)
{
	ClusterThreadReplaySlot *slot;

	slot = cluster_thread_recovery_replay_slot(dead_tid);
	return cluster_thread_recovery_projection_read(slot, episode_epoch, token_out,
												   validated_tail_out, checkpoint_lower_out,
												   lifecycle_out, tail_tli_out, checkpoint_tli_out);
}

/*
 * cluster_thread_recovery_counters -- the region-level online thread-recovery
 *	counter block (spec-4.11 D5), declared in cluster_recovery_plan.h.  Returns
 *	NULL when the region is not attached (L110: the orchestrator getters then
 *	report the frozen-safe sentinel 0 / InvalidXLogRecPtr).
 */
ClusterThreadRecoveryCounters *
cluster_thread_recovery_counters(void)
{
	if (cluster_recovery_plan_shmem == NULL)
		return NULL;
	return &cluster_recovery_plan_shmem->thread_recovery_counters;
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
