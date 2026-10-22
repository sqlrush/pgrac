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

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_clean_leave.h"
#include "miscadmin.h"
#include "cluster/cluster_control_root.h" /* RF-ROOT P7 G1b step 4: canonical verdict source */
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_recovery_ctrc_record.h"
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

static bool stamp_shape(const ClusterRecoveryStampSlotV1 *slot, uint16 thread);
static void stamp_snapshot(ClusterRecoveryStampSlotV1 *slot, ClusterRecoveryStampSlotV1 *out);
StaticAssertDecl(CLUSTER_RECOVERY_STAMP_SLOTS == CLUSTER_RECOVERY_PLAN_THREADS,
				 "stamp windows use the original recovery slots");

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
		if (tid != 0) {
			ClusterRecoveryStampSlotV1 stamp;
			stamp_snapshot(&slot->stamp, &stamp);
			result = !stamp_shape(&stamp, tid) || stamp.failed	   ? CLUSTER_NORMAL_STOP_INVALID
					 : stamp.phase != CLUSTER_RECOVERY_STAMP_EMPTY ? CLUSTER_NORMAL_STOP_PENDING
																   : CLUSTER_NORMAL_STOP_READY;
			recovery_stop_note(result, "RECOVERY_STAMP", tid,
							   result == CLUSTER_NORMAL_STOP_INVALID ? "RECOVERY_STAMP_INVALID"
																	 : "RECOVERY_STAMP_PENDING",
							   &aggregate, &first_domain, &first_key, &first_reason);
		}
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
			SpinLockInit(&cluster_recovery_plan_shmem->thread_replay[slot].stamp.lock);
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

/* Value-owned checkpoint interlock in the original recovery result slots. */
static bool
stamp_actor(void)
{
	return cluster_shared_config && cluster_enabled && IsUnderPostmaster && MyProcPid > 0
		   && (MyBackendType == B_STARTUP || MyBackendType == B_BG_WORKER);
}

static bool
stamp_owner_valid(const ClusterRecoveryStampOwnerV1 *owner)
{
	const ClusterControlRootIdentity *writer, *target;
	if (owner == NULL || owner->executor_pid <= 0 || owner->executor_boot == 0
		|| owner->recovery_serial == 0 || owner->writer.epoch == 0
		|| owner->writer.startup_first_lsn == InvalidXLogRecPtr || owner->writer.ref.timeline == 0
		|| !cluster_wal_claim_v2_ref_valid(&owner->writer.ref.claim)
		|| !cluster_wal_claim_v2_ref_valid(&owner->target))
		return false;
	writer = &owner->writer.ref.claim.identity;
	target = &owner->target.identity;
	return owner->executor_boot == writer->origin_owner_incarnation
		   && writer->system_identifier == target->system_identifier
		   && memcmp(writer->storage_uuid, target->storage_uuid, 16) == 0
		   && memcmp(writer->authority_uuid, target->authority_uuid, 16) == 0
		   && owner->writer.ref.claim.database_incarnation == owner->target.database_incarnation;
}

static bool
stamp_owner_equal(const ClusterRecoveryStampOwnerV1 *a, const ClusterRecoveryStampOwnerV1 *b)
{
	return memcmp(&a->writer.ref.claim, &b->writer.ref.claim, sizeof(a->writer.ref.claim)) == 0
		   && a->writer.ref.timeline == b->writer.ref.timeline && a->writer.epoch == b->writer.epoch
		   && a->writer.startup_first_lsn == b->writer.startup_first_lsn
		   && memcmp(&a->target, &b->target, sizeof(a->target)) == 0
		   && a->executor_pid == b->executor_pid && a->executor_boot == b->executor_boot
		   && a->recovery_serial == b->recovery_serial;
}

static bool
stamp_owner_payload_matches(const ClusterRecoveryStampOwnerV1 *owner,
							const ClusterRecoveryTTAbortV1 *record)
{
	return stamp_owner_valid(owner)
		   && record->writer_thread == owner->writer.ref.claim.identity.origin_thread_id
		   && record->writer_incarnation
				  == owner->writer.ref.claim.identity.origin_owner_incarnation
		   && record->target_thread == owner->target.identity.origin_thread_id
		   && record->target_incarnation == owner->target.identity.origin_owner_incarnation
		   && record->recovery_serial == owner->recovery_serial
		   && memcmp(record->target_claim_sha256, owner->target.claim_sha256, 32) == 0;
}

/* Full codec work is only performed on caller-owned input or a snapshot,
 * never while holding a spinlock or an insertion critical section. */
static bool
stamp_payload_matches(const ClusterRecoveryStampOwnerV1 *owner, ClusterRecoveryStampKindV1 kind,
					  const uint8 *payload, Size length, uint16 *target_thread)
{
	if (!stamp_owner_valid(owner))
		return false;
	if (kind == CLUSTER_RECOVERY_STAMP_C0 && length == CLUSTER_RECOVERY_TT_ABORT_BYTES) {
		ClusterRecoveryTTAbortV1 record;
		if (!cluster_recovery_tt_abort_decode_v1(payload, length, &record)
			|| !stamp_owner_payload_matches(owner, &record))
			return false;
		*target_thread = record.target_thread;
		return true;
	}
	if (kind == CLUSTER_RECOVERY_STAMP_D0 && length == CLUSTER_RECOVERY_CTRC_BYTES
		&& CritSectionCount == 0) {
		ClusterRecoveryCTRCReleaseV2 record;
		if (!cluster_recovery_ctrc_decode_v2(
				payload, length, owner->writer.ref.claim.identity.system_identifier, &record)
			|| record.writer_thread != owner->writer.ref.claim.identity.origin_thread_id
			|| record.writer_boot != owner->executor_boot
			|| record.target_owner != owner->target.identity.origin_thread_id
			|| record.origin_node_id != owner->target.identity.origin_node_id
			|| record.database_incarnation != owner->target.database_incarnation
			|| record.formation_epoch != owner->writer.epoch
			|| record.recovery_serial != owner->recovery_serial)
			return false;
		*target_thread = record.target_owner;
		return true;
	}
	return false;
}

/* Constant-sized pure metadata checks; safe under the slot spinlock. The
 * prepared operation is immutable until retirement and matched in full by
 * the executor, so transitions do not decode or allocate. */
static bool
stamp_state_shape(const ClusterRecoveryStampSlotV1 *slot, uint16 thread)
{
	const ClusterRecoveryStampHandleV1 *handle = &slot->handle;
	const ClusterRecoveryStampKeyV1 *key = &handle->key;
	if (slot->phase < CLUSTER_RECOVERY_STAMP_EMPTY || slot->phase > CLUSTER_RECOVERY_STAMP_DURABLE)
		return false;
	if (slot->phase == CLUSTER_RECOVERY_STAMP_EMPTY)
		return !slot->failed && slot->record_start == 0 && slot->record_end == 0
			   && handle->kind == 0 && handle->payload_length == 0
			   && ((key->slot_generation == 0 && key->window_seq == 0 && key->target_thread == 0)
				   || (key->slot_generation > 0 && key->window_seq >= key->slot_generation
					   && key->target_thread == thread && stamp_owner_valid(&handle->owner)));
	if (key->target_thread != thread || key->slot_generation == 0
		|| key->window_seq < key->slot_generation || !stamp_owner_valid(&handle->owner)
		|| !((handle->kind == CLUSTER_RECOVERY_STAMP_C0
			  && handle->payload_length == CLUSTER_RECOVERY_TT_ABORT_BYTES)
			 || (handle->kind == CLUSTER_RECOVERY_STAMP_D0
				 && handle->payload_length == CLUSTER_RECOVERY_CTRC_BYTES)))
		return false;
	if (slot->phase <= CLUSTER_RECOVERY_STAMP_INSERTING)
		return slot->record_start == 0 && slot->record_end == 0;
	return slot->record_start >= handle->owner.writer.startup_first_lsn
		   && slot->record_end > slot->record_start;
}

static bool
stamp_shape(const ClusterRecoveryStampSlotV1 *slot, uint16 thread)
{
	uint16 target_thread;
	return stamp_state_shape(slot, thread)
		   && (slot->phase == CLUSTER_RECOVERY_STAMP_EMPTY
			   || (stamp_payload_matches(&slot->handle.owner, slot->handle.kind,
										 slot->handle.payload, slot->handle.payload_length,
										 &target_thread)
				   && target_thread == thread));
}

static void
stamp_snapshot(ClusterRecoveryStampSlotV1 *slot, ClusterRecoveryStampSlotV1 *out)
{
	SpinLockAcquire(&slot->lock);
	out->handle = slot->handle;
	out->phase = slot->phase;
	out->record_start = slot->record_start;
	out->record_end = slot->record_end;
	out->failed = slot->failed;
	SpinLockRelease(&slot->lock);
}

/* Read a snapshot, or call with the slot lock held. No codec or IO here. */
static bool
stamp_handle_matches(const ClusterRecoveryStampSlotV1 *slot,
					 const ClusterRecoveryStampHandleV1 *handle)
{
	return stamp_state_shape(slot, handle->key.target_thread)
		   && slot->phase != CLUSTER_RECOVERY_STAMP_EMPTY
		   && slot->handle.key.slot_generation == handle->key.slot_generation
		   && slot->handle.key.window_seq == handle->key.window_seq
		   && slot->handle.key.target_thread == handle->key.target_thread
		   && stamp_owner_equal(&slot->handle.owner, &handle->owner)
		   && slot->handle.kind == handle->kind
		   && slot->handle.payload_length == handle->payload_length
		   && memcmp(slot->handle.payload, handle->payload, sizeof(handle->payload)) == 0;
}

bool
cluster_recovery_stamp_prepare_v1(const ClusterRecoveryStampOwnerV1 *owner,
								  ClusterRecoveryStampKindV1 kind, const uint8 *payload,
								  Size length, ClusterRecoveryStampHandleV1 *out)
{
	uint16 target_thread;
	ClusterThreadReplaySlot *replay;
	ClusterRecoveryStampSlotV1 *slot;
	bool accepted = false;
	uint64 generation, sequence;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!stamp_actor() || CritSectionCount != 0 || owner == NULL || owner->executor_pid != MyProcPid
		|| !stamp_payload_matches(owner, kind, payload, length, &target_thread))
		return false;
	replay = cluster_thread_recovery_replay_slot(target_thread);
	if (replay == NULL)
		return false;
	/* The native executor is single-threaded; only this live PID can publish
	 * its windows. Check every original slot before starting another target. */
	for (uint16 tid = 1; tid <= CLUSTER_RECOVERY_STAMP_SLOTS; tid++) {
		ClusterRecoveryStampSlotV1 *other = &cluster_recovery_plan_shmem->thread_replay[tid].stamp;
		ClusterRecoveryStampSlotV1 snapshot;
		bool unavailable;
		stamp_snapshot(other, &snapshot);
		unavailable = !stamp_shape(&snapshot, tid)
					  || (snapshot.phase != CLUSTER_RECOVERY_STAMP_EMPTY
						  && snapshot.handle.owner.executor_pid == MyProcPid);
		if (unavailable)
			return false;
	}
	slot = &replay->stamp;
	SpinLockAcquire(&slot->lock);
	if (!stamp_state_shape(slot, target_thread) || slot->phase != CLUSTER_RECOVERY_STAMP_EMPTY
		|| slot->handle.key.window_seq == UINT64_MAX)
		goto done;
	generation = slot->handle.key.slot_generation;
	/* Handles are value-owned copies, never reconstructed from a PID/epoch. */
	if (generation == 0 || !stamp_owner_equal(&slot->handle.owner, owner)) {
		if (generation == UINT64_MAX)
			goto done;
		generation++;
	}
	sequence = slot->handle.key.window_seq + 1;
	memset(&slot->handle, 0, sizeof(slot->handle));
	slot->handle.key.slot_generation = generation;
	slot->handle.key.window_seq = sequence;
	slot->handle.key.target_thread = target_thread;
	slot->handle.owner = *owner;
	slot->handle.kind = kind;
	slot->handle.payload_length = length;
	memset(slot->handle.payload, 0, sizeof(slot->handle.payload));
	memcpy(slot->handle.payload, payload, length);
	slot->phase = CLUSTER_RECOVERY_STAMP_PRE_INSERT;
	*out = slot->handle;
	accepted = true;
done:
	SpinLockRelease(&slot->lock);
	return accepted;
}

static bool
stamp_transition(const ClusterRecoveryStampHandleV1 *handle, ClusterRecoveryStampPhaseV1 expected,
				 ClusterRecoveryStampPhaseV1 next, XLogRecPtr start, XLogRecPtr end, bool failed)
{
	ClusterThreadReplaySlot *replay;
	ClusterRecoveryStampSlotV1 *slot;
	bool accepted = false;
	if (!stamp_actor() || handle == NULL || handle->owner.executor_pid != MyProcPid
		|| (replay = cluster_thread_recovery_replay_slot(handle->key.target_thread)) == NULL)
		return false;
	slot = &replay->stamp;
	SpinLockAcquire(&slot->lock);
	if (!stamp_handle_matches(slot, handle))
		goto done;
	if (failed) {
		slot->failed = true;
		accepted = true;
		goto done;
	}
	if (slot->phase != expected
		|| (slot->failed
			&& !(expected == CLUSTER_RECOVERY_STAMP_PRE_INSERT
				 && next == CLUSTER_RECOVERY_STAMP_EMPTY)))
		goto done;
	if (next == CLUSTER_RECOVERY_STAMP_INSERTED) {
		if (start < slot->handle.owner.writer.startup_first_lsn || end <= start)
			goto done;
		slot->record_start = start;
		slot->record_end = end;
	}
	slot->phase = next;
	if (next == CLUSTER_RECOVERY_STAMP_EMPTY) {
		slot->record_start = slot->record_end = 0;
		slot->handle.kind = 0;
		slot->handle.payload_length = 0;
		memset(slot->handle.payload, 0, sizeof(slot->handle.payload));
		slot->failed = false;
	}
	accepted = true;
done:
	SpinLockRelease(&slot->lock);
	return accepted;
}

bool
cluster_recovery_stamp_inserting_v1(const ClusterRecoveryStampHandleV1 *handle)
{
	return CritSectionCount > 0
		   && stamp_transition(handle, CLUSTER_RECOVERY_STAMP_PRE_INSERT,
							   CLUSTER_RECOVERY_STAMP_INSERTING, 0, 0, false);
}
bool
cluster_recovery_stamp_inserted_v1(const ClusterRecoveryStampHandleV1 *handle, XLogRecPtr start,
								   XLogRecPtr end)
{
	return CritSectionCount > 0
		   && stamp_transition(handle, CLUSTER_RECOVERY_STAMP_INSERTING,
							   CLUSTER_RECOVERY_STAMP_INSERTED, start, end, false);
}
bool
cluster_recovery_stamp_ctrc_inserted_v2(const ClusterRecoveryStampHandleV1 *handle, XLogRecPtr end)
{
	XLogRecPtr start;
	XLogRecord record;

	/* The native accessor only copies the immediate inserter's header and
	 * actual positions. It cannot establish the prepared payload's identity
	 * or the full raw record's CRC/SHA; those remain independent premises. */
	if (CritSectionCount == 0 || handle == NULL || handle->kind != CLUSTER_RECOVERY_STAMP_D0
		|| handle->payload_length != CLUSTER_RECOVERY_CTRC_BYTES
		|| !XLogGetLastInsertRecord(end, &start, &record) || record.xl_rmid != RM_CLUSTER_UNDO_ID
		|| record.xl_info != CLUSTER_RECOVERY_CTRC_WAL_INFO || TransactionIdIsValid(record.xl_xid)
		|| record.xl_tot_len
			   != SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong + CLUSTER_RECOVERY_CTRC_BYTES)
		return false;
	return cluster_recovery_stamp_inserted_v1(handle, start, end);
}
XLogRecPtr
cluster_recovery_stamp_ctrc_insert_v2(const ClusterRecoveryStampHandleV1 *handle)
{
	XLogRecPtr end;

	/* The caller owns every readiness proof. This adapter only consumes an
	 * immutable prepared handle while the original executor is in its critical
	 * section; it does not manufacture recovery, graph, retention or IO
	 * authority. A live native XID would make this a different WAL operation,
	 * so refuse before changing the window. */
	if (CritSectionCount == 0 || handle == NULL
		|| handle->kind != CLUSTER_RECOVERY_STAMP_D0
		|| handle->payload_length != CLUSTER_RECOVERY_CTRC_BYTES
		|| !XLogInsertAllowed() || TransactionIdIsValid(GetCurrentTransactionIdIfAny())
		|| !cluster_recovery_stamp_inserting_v1(handle))
		return InvalidXLogRecPtr;

	/* The original XLogInsert assembler was prepared before PRE_INSERT. The
	 * borrowed payload remains owned by the handle until this call returns;
	 * no allocation, wait, flush, or target write is introduced here. */
	XLogBeginInsert();
	XLogRegisterData((char *) handle->payload, CLUSTER_RECOVERY_CTRC_BYTES);
	end = XLogInsert(RM_CLUSTER_UNDO_ID, CLUSTER_RECOVERY_CTRC_WAL_INFO);
	if (end == InvalidXLogRecPtr || !cluster_recovery_stamp_ctrc_inserted_v2(handle, end)) {
		/* Once native insertion began, neither an ERROR/PANIC boundary nor a
		 * post-insert mismatch may cancel or fake completion of this window. */
		(void) cluster_recovery_stamp_fail_v1(handle);
		return InvalidXLogRecPtr;
	}
	return end;
}

static bool
stamp_ranges_overlap(const void *a, Size a_length, const void *b, Size b_length)
{
	uintptr_t av = (uintptr_t) a, bv = (uintptr_t) b;

	return a_length != 0 && b_length != 0
		&& (av >= bv ? av - bv < b_length : bv - av < a_length);
}

XLogRecPtr
cluster_recovery_stamp_ctrc_insert_record_v2(const ClusterRecoveryStampHandleV1 *handle,
											  void *raw, Size raw_length, XLogRecPtr *start)
{
	uint8 copy[SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong + CLUSTER_RECOVERY_CTRC_BYTES];
	ClusterThreadReplaySlot *replay;
	ClusterRecoveryStampSlotV1 snapshot;
	XLogRecPtr actual_start, end;
	uint32 payload_length;

	if (CritSectionCount == 0 || handle == NULL || raw == NULL || start == NULL
		|| handle->kind != CLUSTER_RECOVERY_STAMP_D0
		|| handle->payload_length != CLUSTER_RECOVERY_CTRC_BYTES || raw_length != sizeof(copy)
		|| (replay = cluster_thread_recovery_replay_slot(handle->key.target_thread)) == NULL
		|| stamp_ranges_overlap(raw, raw_length, start, sizeof(*start))
		|| stamp_ranges_overlap(raw, raw_length, handle, sizeof(*handle))
		|| stamp_ranges_overlap(start, sizeof(*start), handle, sizeof(*handle))
		|| stamp_ranges_overlap(raw, raw_length, &replay->stamp, sizeof(replay->stamp))
		|| stamp_ranges_overlap(start, sizeof(*start), &replay->stamp, sizeof(replay->stamp)))
		return InvalidXLogRecPtr;

	/* Preserve the original single insertion and all post-insert duty. A
	 * fixed private temporary keeps caller outputs unchanged on any refusal.
	 * All fragment storage must remain immutable until this copy returns. */
	end = cluster_recovery_stamp_ctrc_insert_v2(handle);
	if (end == InvalidXLogRecPtr)
		return InvalidXLogRecPtr;
	if (!XLogCopyLastInsertRecord(end, copy, sizeof(copy), &actual_start))
		goto failed;
	memcpy(&payload_length, copy + SizeOfXLogRecord + 1, sizeof(payload_length));
	if (copy[SizeOfXLogRecord] != XLR_BLOCK_ID_DATA_LONG
		|| payload_length != CLUSTER_RECOVERY_CTRC_BYTES
		|| memcmp(copy + SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong,
				  handle->payload, CLUSTER_RECOVERY_CTRC_BYTES) != 0)
		goto failed;
	stamp_snapshot(&replay->stamp, &snapshot);
	if (!stamp_handle_matches(&snapshot, handle) || snapshot.failed
		|| snapshot.phase != CLUSTER_RECOVERY_STAMP_INSERTED
		|| snapshot.record_start != actual_start || snapshot.record_end != end)
		goto failed;

	/* The existing finish still owes WAL flush, target sync/readback and
	 * exact retirement. CRC/SHA may only run after its original critical exit. */
	memcpy(raw, copy, sizeof(copy));
	memcpy(start, &actual_start, sizeof(actual_start));
	return end;
failed:
	(void) cluster_recovery_stamp_fail_v1(handle);
	return InvalidXLogRecPtr;
}

bool
cluster_recovery_stamp_ctrc_flush_v2(const ClusterRecoveryStampHandleV1 *handle)
{
	ClusterThreadReplaySlot *replay;
	ClusterRecoveryStampSlotV1 before, after;

	if (!stamp_actor() || MyBackendType != B_STARTUP || CritSectionCount == 0 || handle == NULL
		|| handle->owner.executor_pid != MyProcPid || handle->kind != CLUSTER_RECOVERY_STAMP_D0
		|| handle->payload_length != CLUSTER_RECOVERY_CTRC_BYTES
		|| (replay = cluster_thread_recovery_replay_slot(handle->key.target_thread)) == NULL)
		return false;
	stamp_snapshot(&replay->stamp, &before);
	if (!stamp_handle_matches(&before, handle) || before.phase != CLUSTER_RECOVERY_STAMP_INSERTED
		|| before.failed)
		return false;

	/* Keep native insertion/flush waits and errors intact. XLogFlush can
	 * return without flushing when insertion is forbidden, so require both
	 * the original writer and independent startup durable coverage. Never
	 * carry a slot lock across these calls or change the inserted addresses. */
	if (!XLogInsertAllowed()
		|| cluster_wal_writer_check(&before.handle.owner.writer) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto failed;
	XLogFlush(before.record_end);
	if (!ClusterXLogStartupFlushCovers(before.record_end, before.handle.owner.writer.ref.timeline)
		|| cluster_wal_writer_check(&before.handle.owner.writer) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto failed;
	stamp_snapshot(&replay->stamp, &after);
	if (!stamp_handle_matches(&after, &before.handle)
		|| after.phase != CLUSTER_RECOVERY_STAMP_INSERTED || after.failed
		|| after.record_start != before.record_start || after.record_end != before.record_end)
		goto failed;
	/* Target sync and postread are still owed; no durable/clear transition. */
	return true;
failed:
	/* The saved exact operation cannot mark a different window failed. */
	(void)cluster_recovery_stamp_fail_v1(&before.handle);
	return false;
}
bool
cluster_recovery_stamp_ctrc_finish_v2(const ClusterRecoveryStampHandleV1 *handle,
									  ClusterUndoSmgrRecoveryStampV1 stamp)
{
	ClusterThreadReplaySlot *replay;
	ClusterRecoveryStampSlotV1 before, after;

	if (!stamp_actor() || MyBackendType != B_STARTUP || CritSectionCount == 0 || handle == NULL
		|| handle->owner.executor_pid != MyProcPid || handle->kind != CLUSTER_RECOVERY_STAMP_D0
		|| handle->payload_length != CLUSTER_RECOVERY_CTRC_BYTES
		|| (replay = cluster_thread_recovery_replay_slot(handle->key.target_thread)) == NULL)
		return false;
	stamp_snapshot(&replay->stamp, &before);
	if (!stamp_handle_matches(&before, handle) || before.phase != CLUSTER_RECOVERY_STAMP_INSERTED
		|| before.failed)
		return false;

	/* Preparation owns the held descriptor and complete payload. Flush the
	 * actual native end before the bound target write; no slot lock spans
	 * either operation, and native ERROR/PANIC retains the inserted duty. */
	if (stamp.token == 0 || !cluster_recovery_stamp_ctrc_flush_v2(&before.handle)
		|| !cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, before.handle.payload,
														   before.handle.payload_length))
		goto failed;
	if (!XLogInsertAllowed()
		|| cluster_wal_writer_check(&before.handle.owner.writer) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| !ClusterXLogStartupFlushCovers(before.record_end,
										  before.handle.owner.writer.ref.timeline))
		goto failed;
	stamp_snapshot(&replay->stamp, &after);
	if (!stamp_handle_matches(&after, &before.handle)
		|| after.phase != CLUSTER_RECOVERY_STAMP_INSERTED || after.failed
		|| after.record_start != before.record_start || after.record_end != before.record_end)
		goto failed;
	if (!cluster_recovery_stamp_durable_v1(&before.handle)
		|| !cluster_recovery_stamp_clear_v1(&before.handle))
		goto failed;
	return true;

failed:
	(void)cluster_recovery_stamp_fail_v1(&before.handle);
	return false;
}
XLogRecPtr
cluster_recovery_stamp_ctrc_apply_v2(const ClusterRecoveryStampHandleV1 *handle,
								   ClusterUndoSmgrRecoveryStampV1 stamp,
								   void *raw, Size raw_length, XLogRecPtr *start)
{
	uint8 copy[SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong + CLUSTER_RECOVERY_CTRC_BYTES];
	ClusterThreadReplaySlot *replay;
	XLogRecPtr actual_start, end;

	/* Validate public outputs before the one native insertion. The nested
	 * snapshot uses private storage; it must not hide a caller alias until
	 * after the original window has already been retired. */
	if (!stamp_actor() || MyBackendType != B_STARTUP || CritSectionCount == 0
		|| handle == NULL || raw == NULL || start == NULL || stamp.token == 0
		|| handle->owner.executor_pid != MyProcPid || handle->kind != CLUSTER_RECOVERY_STAMP_D0
		|| handle->payload_length != CLUSTER_RECOVERY_CTRC_BYTES || raw_length != sizeof(copy)
		|| (replay = cluster_thread_recovery_replay_slot(handle->key.target_thread)) == NULL
		|| stamp_ranges_overlap(raw, raw_length, start, sizeof(*start))
		|| stamp_ranges_overlap(raw, raw_length, handle, sizeof(*handle))
		|| stamp_ranges_overlap(start, sizeof(*start), handle, sizeof(*handle))
		|| stamp_ranges_overlap(raw, raw_length, &replay->stamp, sizeof(replay->stamp))
		|| stamp_ranges_overlap(start, sizeof(*start), &replay->stamp, sizeof(replay->stamp)))
		return InvalidXLogRecPtr;

	/* Keep the caller's original critical section and post-insert duty. No
	 * hash/allocation, cancellation or error interception is introduced. */
	end = cluster_recovery_stamp_ctrc_insert_record_v2(handle, copy, sizeof(copy), &actual_start);
	if (end == InvalidXLogRecPtr || !cluster_recovery_stamp_ctrc_finish_v2(handle, stamp))
		return InvalidXLogRecPtr;
	memcpy(raw, copy, sizeof(copy));
	memcpy(start, &actual_start, sizeof(actual_start));
	return end;
}

bool
cluster_recovery_stamp_durable_v1(const ClusterRecoveryStampHandleV1 *handle)
{
	return stamp_transition(handle, CLUSTER_RECOVERY_STAMP_INSERTED, CLUSTER_RECOVERY_STAMP_DURABLE,
							0, 0, false);
}
bool
cluster_recovery_stamp_clear_v1(const ClusterRecoveryStampHandleV1 *handle)
{
	return stamp_transition(handle, CLUSTER_RECOVERY_STAMP_DURABLE, CLUSTER_RECOVERY_STAMP_EMPTY, 0,
							0, false);
}
bool
cluster_recovery_stamp_cancel_v1(const ClusterRecoveryStampHandleV1 *handle)
{
	return stamp_transition(handle, CLUSTER_RECOVERY_STAMP_PRE_INSERT, CLUSTER_RECOVERY_STAMP_EMPTY,
							0, 0, false);
}
bool
cluster_recovery_stamp_fail_v1(const ClusterRecoveryStampHandleV1 *handle)
{
	return stamp_transition(handle, CLUSTER_RECOVERY_STAMP_EMPTY, CLUSTER_RECOVERY_STAMP_EMPTY, 0,
							0, true);
}

bool
cluster_recovery_stamp_capture_v1(ClusterRecoveryStampCaptureV1 *out)
{
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (cluster_recovery_plan_shmem == NULL)
		return false;
	for (uint16 tid = 1; tid <= CLUSTER_RECOVERY_STAMP_SLOTS; tid++) {
		ClusterRecoveryStampSlotV1 *slot = &cluster_recovery_plan_shmem->thread_replay[tid].stamp;
		ClusterRecoveryStampSlotV1 snapshot;
		bool valid;
		stamp_snapshot(slot, &snapshot);
		valid = stamp_shape(&snapshot, tid);
		if (valid && snapshot.phase != CLUSTER_RECOVERY_STAMP_EMPTY) {
			out->keys[out->count++] = snapshot.handle.key;
			if (snapshot.handle.owner.executor_pid == MyProcPid)
				out->caller_owned = true;
		}
		if (!valid) {
			memset(out, 0, sizeof(*out));
			return false;
		}
	}
	out->captured = true;
	return true;
}

ClusterRecoveryStampPollV1
cluster_recovery_stamp_poll_v1(const ClusterRecoveryStampCaptureV1 *capture)
{
	ClusterRecoveryStampPollV1 result = CLUSTER_RECOVERY_STAMP_COMPLETE;
	uint16 previous = 0;
	if (capture == NULL || !capture->captured || capture->count > CLUSTER_RECOVERY_STAMP_SLOTS
		|| cluster_recovery_plan_shmem == NULL)
		return CLUSTER_RECOVERY_STAMP_INVALID;
	for (uint32 i = 0; i < capture->count; i++) {
		const ClusterRecoveryStampKeyV1 *key = &capture->keys[i];
		ClusterRecoveryStampSlotV1 snapshot;
		bool valid, pending;
		if (key->target_thread <= previous || key->target_thread > CLUSTER_RECOVERY_STAMP_SLOTS
			|| key->slot_generation == 0 || key->window_seq < key->slot_generation)
			return CLUSTER_RECOVERY_STAMP_INVALID;
		previous = key->target_thread;
		stamp_snapshot(&cluster_recovery_plan_shmem->thread_replay[key->target_thread].stamp,
					   &snapshot);
		valid = stamp_shape(&snapshot, key->target_thread)
				&& snapshot.handle.key.slot_generation >= key->slot_generation
				&& snapshot.handle.key.window_seq >= key->window_seq
				&& (snapshot.handle.key.slot_generation == key->slot_generation
					|| snapshot.handle.key.window_seq > key->window_seq);
		pending = valid && snapshot.handle.key.window_seq == key->window_seq
				  && snapshot.phase != CLUSTER_RECOVERY_STAMP_EMPTY;
		if (pending && snapshot.failed)
			valid = false;
		if (!valid)
			return CLUSTER_RECOVERY_STAMP_INVALID;
		if (pending)
			result = CLUSTER_RECOVERY_STAMP_PENDING;
	}
	return result;
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
