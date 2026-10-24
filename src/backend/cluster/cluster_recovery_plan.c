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

#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_clean_leave.h"
#include "miscadmin.h"
#include "cluster/cluster_control_root.h" /* RF-ROOT P7 G1b step 4: canonical verdict source */
#include "cluster/cluster_cold_recovery.h"
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
#include "postmaster/interrupt.h"
#include "postmaster/startup.h"
#include "storage/latch.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/* Volatile original-start input, never a durable certificate or service bit.
 * The lock protects only bounded memory copies, never ROOT/WAL/storage I/O. */
typedef struct ClusterRecoveryCheckpointSlotV1 {
	slock_t lock;
	bool valid;
	uint64 generation;
	ClusterRecoveryCheckpointOwnerV1 owner;
	ClusterRecoveryTxResultValueV1 tx_result;
	uint64 tx_accepted_generation;
} ClusterRecoveryCheckpointSlotV1;

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
	ClusterRecoveryCheckpointSlotV1 checkpoint_owner;
} ClusterRecoveryPlanShmem;

static ClusterRecoveryPlanShmem *cluster_recovery_plan_shmem = NULL;

static bool stamp_shape(const ClusterRecoveryStampSlotV1 *slot, uint16 thread);
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
			ClusterRecoveryStampSlotV1 *stamp = &slot->stamp;
			SpinLockAcquire(&stamp->lock);
			result = !stamp_shape(stamp, tid) || stamp->failed		? CLUSTER_NORMAL_STOP_INVALID
					 : stamp->phase != CLUSTER_RECOVERY_STAMP_EMPTY ? CLUSTER_NORMAL_STOP_PENDING
																	: CLUSTER_NORMAL_STOP_READY;
			SpinLockRelease(&stamp->lock);
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

		memset(&cluster_recovery_plan_shmem->checkpoint_owner, 0,
			   sizeof(cluster_recovery_plan_shmem->checkpoint_owner));
		SpinLockInit(&cluster_recovery_plan_shmem->checkpoint_owner.lock);
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
	bool root_source;

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

	/* PRE2 has a CREATION or migrated v3 canonical ROOT, independently of
	 * the volatile legacy migration latch. The original discovered reader
	 * already selects v3 under shared_config and validates the exact ROOT;
	 * a failed read remains UNKNOWN, never a registry fallback. This only
	 * selects observational inputs, not service/replay/claim authority.
	 * Non-PRE2 retains its original durable migration-OPEN restoration. */
	if (!cluster_shared_config)
		cluster_control_root_restore_bit22_latch_if_active();
	root_source = cluster_shared_config || cluster_r4_bit22_cutover_active();

	/*
	 * Defensive pass-level gate.  Under today's startup ordering the
	 * spec-4.2 ensure() FATAL gate has already validated the registry
	 * before the startup process runs, so this branch is not reachable
	 * in practice; it exists so a future reordering degrades to an
	 * honest 'failed' plan instead of 128 bogus EMPTY verdicts
	 * (read_slot maps a missing file to EMPTY).  Post-bit22 the registry
	 * is telemetry-only (§17.9), so the precondition no longer applies.
	 */
	if (!root_source && !cluster_wal_state_registry_ready()) {
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

		if (root_source) {
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
					(errmsg("recovery plan: thread %u verdict %d (read verdict %d, root_source=%d)",
							(unsigned)tid, (int)verdict, read_verdict, root_source ? 1 : 0)));
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

/* Only the original process can withdraw its own shared publication. The
 * saved opaque pointer is compared, never dereferenced during revocation. */
static const struct ClusterRecoveryStartV1 *checkpoint_registered_start;
static ClusterRecoveryCheckpointOwnerV1 checkpoint_registered_owner;

struct ClusterRecoveryCheckpointScopeV1 {
	bool live;
	uint64 slot_generation;
	ClusterRecoveryCheckpointOwnerV1 owner;
	int pid;
	ResourceOwner resource_owner;
};
static struct ClusterRecoveryCheckpointScopeV1 checkpoint_scope;
static bool checkpoint_scope_callback_registered;

static bool
checkpoint_ref_same(const ClusterWalSourceRef *a, const ClusterWalSourceRef *b)
{
	return memcmp(&a->claim.identity, &b->claim.identity, sizeof(a->claim.identity)) == 0
		&& a->claim.database_incarnation == b->claim.database_incarnation
		&& a->claim.max_config_generation == b->claim.max_config_generation
		&& memcmp(a->claim.claim_sha256, b->claim.claim_sha256, 32) == 0
		&& a->timeline == b->timeline;
}

void
cluster_recovery_checkpoint_revoke_v1(const struct ClusterRecoveryStartV1 *start)
{
	ClusterRecoveryCheckpointSlotV1 *slot;

	if (start == NULL || start != checkpoint_registered_start
		|| MyProcPid != checkpoint_registered_owner.owner_pid)
		return;
	if (cluster_recovery_plan_shmem != NULL) {
		slot = &cluster_recovery_plan_shmem->checkpoint_owner;
		SpinLockAcquire(&slot->lock);
		if (slot->valid && memcmp(&slot->owner, &checkpoint_registered_owner,
								 sizeof(slot->owner)) == 0) {
			slot->valid = false;
			memset(&slot->owner, 0, sizeof(slot->owner));
			memset(&slot->tx_result, 0, sizeof(slot->tx_result));
			slot->tx_accepted_generation = 0;
		}
		SpinLockRelease(&slot->lock);
	}
	checkpoint_registered_start = NULL;
	memset(&checkpoint_registered_owner, 0, sizeof(checkpoint_registered_owner));
}

bool
cluster_recovery_checkpoint_register_v1(const struct ClusterRecoveryStartV1 *start)
{
	ClusterRecoveryCheckpointSlotV1 *slot;
	ClusterRecoveryCheckpointOwnerV1 owner, confirmed;
	bool published = false;

	if (MyBackendType != B_STARTUP || !IsUnderPostmaster
		|| cluster_recovery_plan_shmem == NULL || checkpoint_registered_start != NULL
		|| !ClusterColdRecoveryTxCheckpointOwnerV1(start, &owner))
		return false;
	slot = &cluster_recovery_plan_shmem->checkpoint_owner;
	SpinLockAcquire(&slot->lock);
	if (!slot->valid && slot->generation != PG_UINT64_MAX) {
		slot->generation++;
		slot->owner = owner;
		memset(&slot->tx_result, 0, sizeof(slot->tx_result));
		slot->tx_accepted_generation = 0;
		slot->valid = true;
		published = true;
	}
	SpinLockRelease(&slot->lock);
	if (!published)
		return false;
	checkpoint_registered_start = start;
	checkpoint_registered_owner = owner;
	if (!ClusterColdRecoveryTxCheckpointOwnerV1(start, &confirmed)
		|| memcmp(&confirmed, &owner, sizeof(owner)) != 0) {
		cluster_recovery_checkpoint_revoke_v1(start);
		return false;
	}
	return true;
}

/* Startup's successful release must not destroy the original owner needed
 * by the subsequent LMON observation and native checkpointer. The shared
 * slot retains the same generation and value; only the private cleanup
 * binding is detached. ERROR/exit without this handoff still revokes it. */
bool
cluster_recovery_checkpoint_handoff_v1(const struct ClusterRecoveryStartV1 *start)
{
	ClusterRecoveryCheckpointSlotV1 *slot;
	ClusterRecoveryCheckpointOwnerV1 owner;
	bool same;

	if (MyBackendType != B_STARTUP || !IsUnderPostmaster
		|| start == NULL || start != checkpoint_registered_start
		|| cluster_recovery_plan_shmem == NULL
		|| !ClusterColdRecoveryTxCheckpointOwnerV1(start, &owner)
		|| memcmp(&owner, &checkpoint_registered_owner, sizeof(owner)) != 0)
		return false;
	slot = &cluster_recovery_plan_shmem->checkpoint_owner;
	SpinLockAcquire(&slot->lock);
	same = slot->valid && memcmp(&slot->owner, &owner, sizeof(owner)) == 0;
	SpinLockRelease(&slot->lock);
	if (!same)
		return false;
	checkpoint_registered_start = NULL;
	memset(&checkpoint_registered_owner, 0, sizeof(checkpoint_registered_owner));
	return true;
}

typedef enum RecoveryTxResultAction {
	RECOVERY_TX_RESULT_BEGIN,
	RECOVERY_TX_RESULT_STEP,
	RECOVERY_TX_RESULT_FINISH
} RecoveryTxResultAction;

static bool
recovery_tx_result_update(const struct ClusterRecoveryStartV1 *start, uint32 ordinal,
						 RecoveryTxResultAction action)
{
	ClusterRecoveryCheckpointSlotV1 *slot;
	ClusterRecoveryCheckpointOwnerV1 owner;
	ClusterRecoveryTxResultValueV1 *result;
	const ClusterColdTypedV1 *typed;
	uint8 zero[32] = {0};
	bool proven = false, ok = false;

	if (start == NULL || start != checkpoint_registered_start
		|| cluster_recovery_plan_shmem == NULL
		|| !ClusterColdRecoveryTxCheckpointOwnerV1(start, &owner)
		|| memcmp(&owner, &checkpoint_registered_owner, sizeof(owner)) != 0)
		return false;
	typed = ClusterColdRecoveryTxInputs(start);
	if (typed == NULL || typed->top_tx_census == NULL || !typed->census_root_captured
		|| typed->census_root.system_identifier != owner.writer.ref.claim.identity.system_identifier
		|| memcmp(typed->census_root.image_sha256, zero, sizeof(zero)) == 0)
		return false;
	if (action == RECOVERY_TX_RESULT_STEP) {
		if (ordinal >= typed->top_tx_census->count)
			return false;
		proven = typed->top_tx_census->entries[ordinal].loser_proven;
	}
	slot = &cluster_recovery_plan_shmem->checkpoint_owner;
	SpinLockAcquire(&slot->lock);
	result = &slot->tx_result;
	if (!slot->valid || memcmp(&slot->owner, &owner, sizeof(owner)) != 0)
		goto done;
	if (action == RECOVERY_TX_RESULT_BEGIN && result->state == CLUSTER_RECOVERY_TX_RESULT_EMPTY) {
		result->result_generation = 1;
		result->subject_count = typed->top_tx_census->count;
		result->state = CLUSTER_RECOVERY_TX_RESULT_PENDING;
		memcpy(result->input_root_sha256, typed->census_root.image_sha256, 32);
		ok = true;
		goto done;
	}
	if (result->state != CLUSTER_RECOVERY_TX_RESULT_PENDING
		|| result->subject_count != typed->top_tx_census->count
		|| memcmp(result->input_root_sha256, typed->census_root.image_sha256, 32) != 0
		|| result->result_generation != (uint64) result->processed_count + 1)
		goto done;
	if (action == RECOVERY_TX_RESULT_BEGIN) {
		/* A pre-execution refusal may retry without erasing any result. */
		ok = result->processed_count == 0;
	} else if (action == RECOVERY_TX_RESULT_STEP && ordinal == result->processed_count) {
		result->processed_count++;
		if (proven)
			result->completed_count++;
		else
			result->unresolved_count++;
		result->result_generation++;
		ok = true;
	} else if (action == RECOVERY_TX_RESULT_FINISH
			   && result->processed_count == result->subject_count) {
		if (result->unresolved_count == 0)
			result->state = CLUSTER_RECOVERY_TX_RESULT_COMPLETE;
		result->result_generation++;
		ok = true;
	}
done:
	SpinLockRelease(&slot->lock);
	return ok;
}

bool
cluster_recovery_tx_result_begin_v1(const struct ClusterRecoveryStartV1 *start)
{
	return recovery_tx_result_update(start, 0, RECOVERY_TX_RESULT_BEGIN);
}

bool
cluster_recovery_tx_result_step_v1(const struct ClusterRecoveryStartV1 *start, uint32 ordinal)
{
	return recovery_tx_result_update(start, ordinal, RECOVERY_TX_RESULT_STEP);
}

bool
cluster_recovery_tx_result_finish_v1(const struct ClusterRecoveryStartV1 *start)
{
	return recovery_tx_result_update(start, 0, RECOVERY_TX_RESULT_FINISH);
}

static void
checkpoint_scope_resource_release(ResourceReleasePhase phase, bool isCommit,
								  bool isTopLevel, void *arg)
{
	(void)isCommit;
	(void)isTopLevel;
	(void)arg;
	if (phase == RESOURCE_RELEASE_BEFORE_LOCKS && checkpoint_scope.live
		&& checkpoint_scope.resource_owner == CurrentResourceOwner)
		memset(&checkpoint_scope, 0, sizeof(checkpoint_scope));
}

bool
cluster_recovery_checkpoint_current_v1(const struct ClusterRecoveryCheckpointScopeV1 *scope,
									 ClusterWalWriterToken *writer)
{
	ClusterRecoveryCheckpointSlotV1 *slot;
	ClusterWalSourceRef current;
	bool same;

	if (writer != NULL)
		memset(writer, 0, sizeof(*writer));
	if (writer == NULL || scope != &checkpoint_scope || !checkpoint_scope.live
		|| MyBackendType != B_CHECKPOINTER || !IsUnderPostmaster || MyProcPid <= 0
		|| checkpoint_scope.pid != MyProcPid || CurrentResourceOwner == NULL
		|| checkpoint_scope.resource_owner != CurrentResourceOwner
		|| !cluster_enabled || !cluster_shared_config || CritSectionCount != 0
		|| ShutdownRequestPending || RecoveryInProgress() || cluster_recovery_plan_shmem == NULL
		|| scope->owner.owner_boot == 0
		|| scope->owner.writer.ref.claim.identity.system_identifier != GetSystemIdentifier()
		|| cluster_qvotec_get_self_incarnation() != scope->owner.owner_boot
		|| cluster_epoch_get_current() != scope->owner.writer.epoch
		|| !cluster_wal_thread_current_v2_ref(&current)
		|| !checkpoint_ref_same(&current, &scope->owner.writer.ref))
		return false;
	slot = &cluster_recovery_plan_shmem->checkpoint_owner;
	SpinLockAcquire(&slot->lock);
	same = slot->valid && slot->generation == scope->slot_generation
		&& memcmp(&slot->owner, &scope->owner, sizeof(slot->owner)) == 0;
	SpinLockRelease(&slot->lock);
	if (!same || cluster_qvotec_get_self_incarnation() != scope->owner.owner_boot
		|| cluster_epoch_get_current() != scope->owner.writer.epoch)
		return false;
	*writer = scope->owner.writer;
	return true;
}

bool
cluster_recovery_checkpoint_observe_v1(ClusterRecoveryCheckpointObservationV1 *out)
{
	ClusterRecoveryCheckpointSlotV1 *slot;
	ClusterRecoveryCheckpointObservationV1 observed;
	ClusterWalSourceRef current;
	bool same;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || MyBackendType != B_LMON || !IsUnderPostmaster || MyProcPid <= 0
		|| !cluster_enabled || !cluster_shared_config || CritSectionCount != 0
		|| ShutdownRequestPending || RecoveryInProgress() || cluster_recovery_plan_shmem == NULL)
		return false;
	memset(&observed, 0, sizeof(observed));
	slot = &cluster_recovery_plan_shmem->checkpoint_owner;
	SpinLockAcquire(&slot->lock);
	if (slot->valid) {
		observed.slot_generation = slot->generation;
		observed.owner = slot->owner;
	}
	SpinLockRelease(&slot->lock);
	if (observed.slot_generation == 0 || observed.owner.owner_boot == 0
		|| observed.owner.writer.ref.claim.identity.system_identifier != GetSystemIdentifier()
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| observed.owner.writer.ref.claim.identity.origin_node_id != cluster_node_id
		|| observed.owner.writer.ref.claim.identity.origin_thread_id != cluster_node_id + 1
		|| observed.owner.writer.ref.claim.identity.origin_owner_incarnation
			   != observed.owner.owner_boot
		|| cluster_qvotec_get_self_incarnation() != observed.owner.owner_boot
		|| cluster_epoch_get_current() != observed.owner.writer.epoch
		|| !cluster_wal_thread_current_v2_ref(&current)
		|| !checkpoint_ref_same(&current, &observed.owner.writer.ref))
		return false;
	SpinLockAcquire(&slot->lock);
	same = slot->valid && slot->generation == observed.slot_generation
		&& memcmp(&slot->owner, &observed.owner, sizeof(slot->owner)) == 0;
	SpinLockRelease(&slot->lock);
	if (!same || cluster_qvotec_get_self_incarnation() != observed.owner.owner_boot
		|| cluster_epoch_get_current() != observed.owner.writer.epoch)
		return false;
	*out = observed;
	return true;
}

bool
cluster_recovery_tx_result_observe_v1(ClusterRecoveryTxResultObservationV1 *out)
{
	ClusterRecoveryCheckpointObservationV1 before, after;
	ClusterRecoveryTxResultValueV1 result;
	ClusterRecoveryCheckpointSlotV1 *slot;
	bool same;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || !cluster_recovery_checkpoint_observe_v1(&before))
		return false;
	slot = &cluster_recovery_plan_shmem->checkpoint_owner;
	SpinLockAcquire(&slot->lock);
	same = slot->valid && slot->generation == before.slot_generation
		&& memcmp(&slot->owner, &before.owner, sizeof(slot->owner)) == 0;
	result = slot->tx_result;
	SpinLockRelease(&slot->lock);
	if (!same || result.state == CLUSTER_RECOVERY_TX_RESULT_EMPTY
		|| result.result_generation == 0 || !cluster_recovery_checkpoint_observe_v1(&after)
		|| memcmp(&before, &after, sizeof(before)) != 0)
		return false;
	SpinLockAcquire(&slot->lock);
	same = slot->valid && slot->generation == before.slot_generation
		&& memcmp(&slot->owner, &before.owner, sizeof(slot->owner)) == 0
		&& memcmp(&slot->tx_result, &result, sizeof(result)) == 0;
	SpinLockRelease(&slot->lock);
	if (!same)
		return false;
	out->checkpoint = after;
	out->result = result;
	return true;
}

bool
cluster_recovery_tx_result_accept_v1(void)
{
	ClusterRecoveryTxResultObservationV1 observed;
	ClusterRecoveryCheckpointSlotV1 *slot;
	bool same;

	if (!cluster_recovery_tx_result_observe_v1(&observed)
		|| observed.result.state != CLUSTER_RECOVERY_TX_RESULT_COMPLETE)
		return false;
	slot = &cluster_recovery_plan_shmem->checkpoint_owner;
	SpinLockAcquire(&slot->lock);
	same = slot->valid && slot->generation == observed.checkpoint.slot_generation
		&& memcmp(&slot->owner, &observed.checkpoint.owner, sizeof(slot->owner)) == 0
		&& memcmp(&slot->tx_result, &observed.result, sizeof(slot->tx_result)) == 0;
	if (same)
		slot->tx_accepted_generation = observed.result.result_generation;
	SpinLockRelease(&slot->lock);
	return same;
}

typedef enum RecoveryTxHandoffStatus {
	RECOVERY_TX_HANDOFF_BLOCKED,
	RECOVERY_TX_HANDOFF_WAIT,
	RECOVERY_TX_HANDOFF_ACCEPTED
} RecoveryTxHandoffStatus;

static RecoveryTxHandoffStatus
recovery_tx_result_handoff_status(const struct ClusterRecoveryStartV1 *start)
{
	ClusterRecoveryCheckpointOwnerV1 owner;
	ClusterRecoveryCheckpointSlotV1 *slot;
	RecoveryTxHandoffStatus status = RECOVERY_TX_HANDOFF_BLOCKED;

	if (start == NULL || start != checkpoint_registered_start
		|| cluster_recovery_plan_shmem == NULL
		|| !ClusterColdRecoveryTxCheckpointOwnerV1(start, &owner)
		|| memcmp(&owner, &checkpoint_registered_owner, sizeof(owner)) != 0)
		return RECOVERY_TX_HANDOFF_BLOCKED;
	slot = &cluster_recovery_plan_shmem->checkpoint_owner;
	SpinLockAcquire(&slot->lock);
	if (slot->valid && memcmp(&slot->owner, &owner, sizeof(owner)) == 0
		&& slot->tx_result.state == CLUSTER_RECOVERY_TX_RESULT_COMPLETE
		&& slot->tx_result.result_generation != 0)
		status = slot->tx_accepted_generation == slot->tx_result.result_generation
			? RECOVERY_TX_HANDOFF_ACCEPTED : RECOVERY_TX_HANDOFF_WAIT;
	SpinLockRelease(&slot->lock);
	return status;
}

bool
cluster_recovery_tx_result_accepted_v1(const struct ClusterRecoveryStartV1 *start)
{
	return recovery_tx_result_handoff_status(start) == RECOVERY_TX_HANDOFF_ACCEPTED;
}

bool
cluster_recovery_tx_result_wait_v1(const struct ClusterRecoveryStartV1 *start)
{
	if (start == NULL)
		return true;
	for (;;) {
		RecoveryTxHandoffStatus status;

		status = recovery_tx_result_handoff_status(start);
		if (status == RECOVERY_TX_HANDOFF_BLOCKED)
			return false;
		ResetLatch(MyLatch);
		HandleStartupProcInterrupts();
		if (status == RECOVERY_TX_HANDOFF_ACCEPTED)
			return true;
		/* The original LMON is already running. Poll wakeups are local;
		 * elapsed time alone cannot invalidate this retained component. */
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						100L, PG_WAIT_EXTENSION);
	}
}

bool
cluster_recovery_checkpoint_begin_v1(struct ClusterRecoveryCheckpointScopeV1 **out)
{
	ClusterRecoveryCheckpointSlotV1 *slot;
	ClusterWalWriterToken writer;

	if (out == NULL || *out != NULL || checkpoint_scope.live
		|| MyBackendType != B_CHECKPOINTER || MyProcPid <= 0 || CurrentResourceOwner == NULL
		|| cluster_recovery_plan_shmem == NULL)
		return false;
	if (!checkpoint_scope_callback_registered) {
		RegisterResourceReleaseCallback(checkpoint_scope_resource_release, NULL);
		checkpoint_scope_callback_registered = true;
	}
	slot = &cluster_recovery_plan_shmem->checkpoint_owner;
	SpinLockAcquire(&slot->lock);
	if (slot->valid) {
		checkpoint_scope.owner = slot->owner;
		checkpoint_scope.slot_generation = slot->generation;
		checkpoint_scope.live = true;
	}
	SpinLockRelease(&slot->lock);
	checkpoint_scope.pid = MyProcPid;
	checkpoint_scope.resource_owner = CurrentResourceOwner;
	if (!cluster_recovery_checkpoint_current_v1(&checkpoint_scope, &writer)) {
		memset(&checkpoint_scope, 0, sizeof(checkpoint_scope));
		return false;
	}
	*out = &checkpoint_scope;
	return true;
}

void
cluster_recovery_checkpoint_end_v1(struct ClusterRecoveryCheckpointScopeV1 **scope)
{
	if (scope == NULL)
		return;
	if (*scope == &checkpoint_scope)
		memset(&checkpoint_scope, 0, sizeof(checkpoint_scope));
	*scope = NULL;
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

/* Constant-sized, pure checks; caller holds only the slot spinlock. */
static bool
stamp_shape(const ClusterRecoveryStampSlotV1 *slot, uint16 thread)
{
	const ClusterRecoveryStampKeyV1 *key = &slot->handle.key;
	ClusterRecoveryTTAbortV1 record;
	ClusterRecoveryStampPhaseV1 effective = slot->phase;
	if (slot->phase < CLUSTER_RECOVERY_STAMP_EMPTY
		|| slot->phase > CLUSTER_RECOVERY_STAMP_ADOPT_PENDING
		|| ((slot->bound_worker_attempt == 0) != (slot->bound_worker_pid == 0))
		|| slot->bound_worker_pid < 0)
		return false;
	if (slot->phase == CLUSTER_RECOVERY_STAMP_ADOPT_PENDING) {
		if (slot->adopt_from < CLUSTER_RECOVERY_STAMP_PRE_INSERT
			|| slot->adopt_from > CLUSTER_RECOVERY_STAMP_DURABLE
			|| slot->executor_attempt_stamp == 0 || slot->bound_worker_attempt != 0)
			return false;
		effective = slot->adopt_from;
	} else if (slot->adopt_from != CLUSTER_RECOVERY_STAMP_EMPTY)
		return false;
	if (slot->phase == CLUSTER_RECOVERY_STAMP_EMPTY)
		return !slot->failed && slot->record_start == 0 && slot->record_end == 0 && slot->kind == 0
			   && slot->payload_length == 0 && slot->executor_attempt_stamp == 0
			   && ((key->slot_generation == 0 && key->window_seq == 0 && key->target_thread == 0)
				   || (key->slot_generation > 0 && key->window_seq >= key->slot_generation
					   && key->target_thread == thread && stamp_owner_valid(&slot->handle.owner)));
	if (key->target_thread != thread || key->slot_generation == 0
		|| key->window_seq < key->slot_generation || slot->kind != CLUSTER_RECOVERY_STAMP_C0
		|| slot->payload_length != CLUSTER_RECOVERY_TT_ABORT_BYTES
		|| !cluster_recovery_tt_abort_decode_v1(slot->payload, slot->payload_length, &record)
		|| !stamp_owner_payload_matches(&slot->handle.owner, &record))
		return false;
	if (slot->phase != CLUSTER_RECOVERY_STAMP_ADOPT_PENDING && slot->executor_attempt_stamp != 0
		&& (slot->executor_attempt_stamp != slot->bound_worker_attempt
			|| slot->handle.owner.executor_pid != slot->bound_worker_pid))
		return false;
	if (effective <= CLUSTER_RECOVERY_STAMP_INSERTING)
		return slot->record_start == 0 && slot->record_end == 0;
	return slot->record_start >= slot->handle.owner.writer.startup_first_lsn
		   && slot->record_end > slot->record_start;
}

bool
cluster_recovery_stamp_worker_begin_v1(uint16 thread, uint64 attempt)
{
	ClusterThreadReplaySlot *replay;
	ClusterRecoveryStampSlotV1 *slot;
	bool accepted = false;
	if (!stamp_actor() || MyBackendType != B_BG_WORKER || CritSectionCount != 0 || attempt == 0
		|| (replay = cluster_thread_recovery_replay_slot(thread)) == NULL)
		return false;
	slot = &replay->stamp;
	SpinLockAcquire(&slot->lock);
	if (pg_atomic_read_u64(&replay->episode_epoch) == attempt
		&& pg_atomic_read_u32(&replay->state) == CLUSTER_THREADREC_REPLAY_REPLAYING
		&& stamp_shape(slot, thread) && slot->phase == CLUSTER_RECOVERY_STAMP_EMPTY
		&& (slot->bound_worker_attempt == 0
			|| (slot->bound_worker_attempt == attempt && slot->bound_worker_pid == MyProcPid))) {
		slot->bound_worker_attempt = attempt;
		slot->bound_worker_pid = MyProcPid;
		accepted = true;
	}
	SpinLockRelease(&slot->lock);
	return accepted;
}

bool
cluster_recovery_stamp_worker_stopped_v1(uint16 thread, uint64 attempt)
{
	ClusterThreadReplaySlot *replay;
	ClusterRecoveryStampSlotV1 *slot;
	bool accepted = false;
	if (!cluster_shared_config || !cluster_enabled || !IsUnderPostmaster || MyProcPid <= 0
		|| MyBackendType != B_LMON || CritSectionCount != 0 || attempt == 0
		|| (replay = cluster_thread_recovery_replay_slot(thread)) == NULL)
		return false;
	slot = &replay->stamp;
	SpinLockAcquire(&slot->lock);
	if (!stamp_shape(slot, thread))
		goto done;
	if (slot->bound_worker_attempt == 0) {
		/* Entry may have refused before registering, including a replacement
		 * blocked by an older orphan. Nothing belongs to this launch: leave
		 * the existing window untouched. Native-handle provenance is still
		 * checked by the sole LMON caller before this no-op observation. */
		accepted = true;
		goto done;
	}
	if (slot->bound_worker_attempt != attempt)
		goto done;
	if (slot->phase != CLUSTER_RECOVERY_STAMP_EMPTY && slot->executor_attempt_stamp != 0) {
		if (slot->executor_attempt_stamp != attempt
			|| slot->handle.owner.executor_pid != slot->bound_worker_pid)
			goto done;
		slot->adopt_from = slot->phase;
		slot->phase = CLUSTER_RECOVERY_STAMP_ADOPT_PENDING;
	}
	slot->bound_worker_attempt = 0;
	slot->bound_worker_pid = 0;
	accepted = true;
done:
	SpinLockRelease(&slot->lock);
	return accepted;
}

bool
cluster_recovery_stamp_prepare_v1(const ClusterRecoveryStampOwnerV1 *owner,
								  ClusterRecoveryStampKindV1 kind, const uint8 *payload,
								  Size length, ClusterRecoveryStampHandleV1 *out)
{
	ClusterRecoveryTTAbortV1 record;
	ClusterThreadReplaySlot *replay;
	ClusterRecoveryStampSlotV1 *slot;
	bool accepted = false;
	uint64 generation, sequence;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!stamp_actor() || CritSectionCount != 0 || owner == NULL || owner->executor_pid != MyProcPid
		|| kind != CLUSTER_RECOVERY_STAMP_C0 || length != CLUSTER_RECOVERY_TT_ABORT_BYTES
		|| !cluster_recovery_tt_abort_decode_v1(payload, length, &record)
		|| !stamp_owner_payload_matches(owner, &record))
		return false;
	replay = cluster_thread_recovery_replay_slot(record.target_thread);
	if (replay == NULL)
		return false;
	/* The native executor is single-threaded; only this live PID can publish
	 * its windows. Check every original slot before starting another target. */
	for (uint16 tid = 1; tid <= CLUSTER_RECOVERY_STAMP_SLOTS; tid++) {
		ClusterRecoveryStampSlotV1 *other = &cluster_recovery_plan_shmem->thread_replay[tid].stamp;
		bool unavailable;
		SpinLockAcquire(&other->lock);
		unavailable = !stamp_shape(other, tid)
					  || (other->phase != CLUSTER_RECOVERY_STAMP_EMPTY
						  && other->handle.owner.executor_pid == MyProcPid);
		SpinLockRelease(&other->lock);
		if (unavailable)
			return false;
	}
	slot = &replay->stamp;
	SpinLockAcquire(&slot->lock);
	if (!stamp_shape(slot, record.target_thread) || slot->phase != CLUSTER_RECOVERY_STAMP_EMPTY
		|| slot->handle.key.window_seq == UINT64_MAX)
		goto done;
	if (MyBackendType == B_BG_WORKER
		&& (slot->bound_worker_pid != MyProcPid || slot->bound_worker_attempt == 0
			|| pg_atomic_read_u64(&replay->episode_epoch) != slot->bound_worker_attempt
			|| pg_atomic_read_u32(&replay->state) != CLUSTER_THREADREC_REPLAY_REPLAYING))
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
	slot->handle.key.target_thread = record.target_thread;
	slot->handle.owner = *owner;
	slot->kind = kind;
	slot->payload_length = length;
	memset(slot->payload, 0, sizeof(slot->payload));
	memcpy(slot->payload, payload, length);
	slot->phase = CLUSTER_RECOVERY_STAMP_PRE_INSERT;
	slot->adopt_from = CLUSTER_RECOVERY_STAMP_EMPTY;
	slot->executor_attempt_stamp = MyBackendType == B_BG_WORKER ? slot->bound_worker_attempt : 0;
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
	if (!stamp_shape(slot, handle->key.target_thread)
		|| slot->handle.key.slot_generation != handle->key.slot_generation
		|| slot->handle.key.window_seq != handle->key.window_seq
		|| slot->handle.key.target_thread != handle->key.target_thread
		|| !stamp_owner_equal(&slot->handle.owner, &handle->owner)
		|| slot->phase == CLUSTER_RECOVERY_STAMP_EMPTY
		|| slot->phase == CLUSTER_RECOVERY_STAMP_ADOPT_PENDING)
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
		slot->kind = 0;
		slot->payload_length = 0;
		memset(slot->payload, 0, sizeof(slot->payload));
		slot->failed = false;
		slot->executor_attempt_stamp = 0;
		slot->adopt_from = CLUSTER_RECOVERY_STAMP_EMPTY;
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
		bool valid;
		SpinLockAcquire(&slot->lock);
		valid = stamp_shape(slot, tid);
		if (valid && slot->phase != CLUSTER_RECOVERY_STAMP_EMPTY) {
			out->keys[out->count++] = slot->handle.key;
			if (slot->handle.owner.executor_pid == MyProcPid)
				out->caller_owned = true;
		}
		SpinLockRelease(&slot->lock);
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
		ClusterRecoveryStampSlotV1 *slot;
		bool valid, pending;
		if (key->target_thread <= previous || key->target_thread > CLUSTER_RECOVERY_STAMP_SLOTS
			|| key->slot_generation == 0 || key->window_seq < key->slot_generation)
			return CLUSTER_RECOVERY_STAMP_INVALID;
		previous = key->target_thread;
		slot = &cluster_recovery_plan_shmem->thread_replay[key->target_thread].stamp;
		SpinLockAcquire(&slot->lock);
		valid = stamp_shape(slot, key->target_thread)
				&& slot->handle.key.slot_generation >= key->slot_generation
				&& slot->handle.key.window_seq >= key->window_seq
				&& (slot->handle.key.slot_generation == key->slot_generation
					|| slot->handle.key.window_seq > key->window_seq);
		pending = valid && slot->handle.key.window_seq == key->window_seq
				  && slot->phase != CLUSTER_RECOVERY_STAMP_EMPTY;
		if (pending && slot->failed && slot->phase != CLUSTER_RECOVERY_STAMP_ADOPT_PENDING)
			valid = false;
		SpinLockRelease(&slot->lock);
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
