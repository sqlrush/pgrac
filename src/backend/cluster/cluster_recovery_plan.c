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
#include "access/xact.h"
#include "access/xloginsert.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_clean_leave.h"
#include "miscadmin.h"
#include "cluster/cluster_control_root.h" /* RF-ROOT P7 G1b step 4: canonical verdict source */
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_recovery_ctrc_record.h"
#include "cluster/cluster_recovery_undo_scan.h"
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
#include "utils/memutils.h"
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
	ClusterRecoveryCtrcResultValueV1 ctrc_result;
	uint64 ctrc_accepted_generation;
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
			result = !stamp_shape(&stamp, tid) || stamp.failed		? CLUSTER_NORMAL_STOP_INVALID
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
	ClusterFormationSnapshotV1 membership;
	bool membership_current = false;

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
	if (cluster_shared_config)
		membership_current = cluster_reconfig_capture_formation_snapshot_v1(own_thread, &membership)
			&& membership.startup_formation_generation != 0;

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
			if (cluster_shared_config && tid != own_thread
				&& (root_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
					|| root_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
				&& snapshot.identity.origin_node_id == (int32)tid - 1
				&& snapshot.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN) {
				uint32 node = tid - 1;
				uint64 boot = membership_current
					? membership.membership.last_admitted_incarnation[node] : 0;

				/* PRE2 membership belongs to the accepted formation, not a
				 * checkpoint-aged ROOT publication. A predecessor's OPEN is
				 * unsealed input, never evidence of a live or dead old writer.
				 * The original cold cohort/self-seal and isolation owners must
				 * still establish recoverable inputs before admission. */
				verdict = membership_current && boot != 0
					&& membership.membership.membership_state[node] == CLUSTER_MEMBER_MEMBER
					&& snapshot.identity.origin_owner_incarnation == boot
					? CLUSTER_RECOVERY_THREAD_ALIVE : CLUSTER_RECOVERY_THREAD_UNKNOWN;
				ereport(LOG, (errmsg("recovery ROOT membership observation thread=%u root_boot="
					UINT64_FORMAT " member_boot=" UINT64_FORMAT " formation_current=%d verdict=%d",
					(unsigned)tid, snapshot.identity.origin_owner_incarnation, boot,
					membership_current ? 1 : 0, (int)verdict)));
			}
			else
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
			memset(&slot->ctrc_result, 0, sizeof(slot->ctrc_result));
			slot->ctrc_accepted_generation = 0;
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
		memset(&slot->ctrc_result, 0, sizeof(slot->ctrc_result));
		slot->ctrc_accepted_generation = 0;
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

bool
cluster_recovery_ctrc_result_collect_v1(const struct ClusterRecoveryStartV1 *start)
{
	ClusterRecoveryCheckpointSlotV1 *slot;
	ClusterRecoveryCheckpointOwnerV1 owner, confirmed;
	ClusterRecoveryCtrcResultValueV1 result;
	ClusterColdCtrcReleaseCensusV1 census;
	const ClusterColdCtrcWorkV1 *work;
	const ClusterColdTypedV1 *typed;
	uint8 zero[32] = {0};
	bool qualified, ok = false;

	if (MyBackendType != B_STARTUP || !IsUnderPostmaster
		|| start == NULL || start != checkpoint_registered_start
		|| cluster_recovery_plan_shmem == NULL
		|| !ClusterColdRecoveryTxCheckpointOwnerV1(start, &owner)
		|| memcmp(&owner, &checkpoint_registered_owner, sizeof(owner)) != 0)
		return false;
	typed = ClusterColdRecoveryTxInputs(start);
	if (typed == NULL || typed->top_tx_census == NULL
		|| typed->top_tx_census->plan != typed->plan || !typed->census_root_captured
		|| typed->census_root.system_identifier != owner.writer.ref.claim.identity.system_identifier
		|| memcmp(typed->census_root.image_sha256, zero, sizeof(zero)) == 0)
		return false;

	/* No caller polarity, fresh shared-memory emptiness or TX result is evidence
	 * for this component.  Sampling is outside the shared slot lock. */
	memset(&result, 0, sizeof(result));
	memset(&census, 0, sizeof(census));
	work = ClusterColdRecoveryCtrcWorkPrepare(start);
	qualified = work != NULL;
	if (qualified) census = work->counts;
	result.subject_count = census.subject_count;
	result.released_count = census.released_count;
	result.unresolved_count = census.unresolved_count;
	result.unqualified_count = qualified ? census.unqualified_count : 1;
	result.state = qualified && result.unresolved_count == 0
		&& result.unqualified_count == 0
		&& result.released_count == result.subject_count
		? CLUSTER_RECOVERY_CTRC_RESULT_COMPLETE : CLUSTER_RECOVERY_CTRC_RESULT_PENDING;
	memcpy(result.input_root_sha256, typed->census_root.image_sha256, 32);
	if (!ClusterColdRecoveryTxCheckpointOwnerV1(start, &confirmed)
		|| memcmp(&owner, &confirmed, sizeof(owner)) != 0
		|| ClusterColdRecoveryTxInputs(start) != typed
		|| (qualified && !ClusterColdRecoveryCtrcWorkCurrent(start, work)))
		return false;

	slot = &cluster_recovery_plan_shmem->checkpoint_owner;
	SpinLockAcquire(&slot->lock);
	if (slot->valid && memcmp(&slot->owner, &owner, sizeof(owner)) == 0
		&& slot->ctrc_result.result_generation != PG_UINT64_MAX) {
		result.result_generation = slot->ctrc_result.result_generation + 1;
		slot->ctrc_result = result;
		slot->ctrc_accepted_generation = 0;
		ok = true;
	}
	SpinLockRelease(&slot->lock);
	return ok;
}

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
cluster_recovery_ctrc_result_observe_v1(ClusterRecoveryCtrcResultObservationV1 *out)
{
	ClusterRecoveryCheckpointObservationV1 before, after;
	ClusterRecoveryCtrcResultValueV1 result;
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
	result = slot->ctrc_result;
	SpinLockRelease(&slot->lock);
	if (!same || result.state == CLUSTER_RECOVERY_CTRC_RESULT_EMPTY
		|| result.result_generation == 0 || !cluster_recovery_checkpoint_observe_v1(&after)
		|| memcmp(&before, &after, sizeof(before)) != 0)
		return false;
	SpinLockAcquire(&slot->lock);
	same = slot->valid && slot->generation == before.slot_generation
		&& memcmp(&slot->owner, &before.owner, sizeof(slot->owner)) == 0
		&& memcmp(&slot->ctrc_result, &result, sizeof(result)) == 0;
	SpinLockRelease(&slot->lock);
	if (!same)
		return false;
	out->checkpoint = after;
	out->result = result;
	return true;
}

bool
cluster_recovery_ctrc_result_accept_v1(void)
{
	ClusterRecoveryCtrcResultObservationV1 observed;
	ClusterRecoveryCheckpointSlotV1 *slot;
	bool same;

	if (!cluster_recovery_ctrc_result_observe_v1(&observed)
		|| observed.result.state != CLUSTER_RECOVERY_CTRC_RESULT_COMPLETE)
		return false;
	slot = &cluster_recovery_plan_shmem->checkpoint_owner;
	SpinLockAcquire(&slot->lock);
	same = slot->valid && slot->generation == observed.checkpoint.slot_generation
		&& memcmp(&slot->owner, &observed.checkpoint.owner, sizeof(slot->owner)) == 0
		&& memcmp(&slot->ctrc_result, &observed.result, sizeof(slot->ctrc_result)) == 0;
	if (same)
		slot->ctrc_accepted_generation = observed.result.result_generation;
	SpinLockRelease(&slot->lock);
	return same;
}

bool
cluster_recovery_ctrc_result_observe_accepted_v1(ClusterRecoveryCtrcResultObservationV1 *out)
{
	ClusterRecoveryCtrcResultObservationV1 observed;
	ClusterRecoveryCheckpointSlotV1 *slot;
	bool same;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || !cluster_recovery_ctrc_result_observe_v1(&observed)
		|| observed.result.state != CLUSTER_RECOVERY_CTRC_RESULT_COMPLETE)
		return false;
	slot = &cluster_recovery_plan_shmem->checkpoint_owner;
	SpinLockAcquire(&slot->lock);
	same = slot->valid && slot->generation == observed.checkpoint.slot_generation
		&& memcmp(&slot->owner, &observed.checkpoint.owner, sizeof(slot->owner)) == 0
		&& memcmp(&slot->ctrc_result, &observed.result, sizeof(slot->ctrc_result)) == 0
		&& slot->ctrc_accepted_generation == observed.result.result_generation;
	SpinLockRelease(&slot->lock);
	if (!same)
		return false;
	*out = observed;
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

bool
cluster_recovery_tx_result_observe_accepted_v1(ClusterRecoveryTxResultObservationV1 *out)
{
	ClusterRecoveryTxResultObservationV1 observed;
	ClusterRecoveryCheckpointSlotV1 *slot;
	bool same;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || !cluster_recovery_tx_result_observe_v1(&observed)
		|| observed.result.state != CLUSTER_RECOVERY_TX_RESULT_COMPLETE)
		return false;
	slot = &cluster_recovery_plan_shmem->checkpoint_owner;
	SpinLockAcquire(&slot->lock);
	same = slot->valid && slot->generation == observed.checkpoint.slot_generation
		&& memcmp(&slot->owner, &observed.checkpoint.owner, sizeof(slot->owner)) == 0
		&& memcmp(&slot->tx_result, &observed.result, sizeof(slot->tx_result)) == 0
		&& slot->tx_accepted_generation == observed.result.result_generation;
	SpinLockRelease(&slot->lock);
	if (!same)
		return false;
	*out = observed;
	return true;
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

static bool
recovery_ctrc_subject_overlap(const void *out, Size bytes, const void *input, Size length)
{
	uintptr_t a = (uintptr_t)out, b = (uintptr_t)input;

	return bytes != 0 && length != 0 && (a <= b ? b - a < bytes : a - b < length);
}

static bool
recovery_ctrc_subject_key_same(const ClusterColdTtSlotKeyV1 *a, const ClusterColdTtSlotKeyV1 *b)
{
	return a->owner_instance == b->owner_instance && a->segment_id == b->segment_id
		&& a->segment_generation == b->segment_generation && a->slot_offset == b->slot_offset
		&& a->xid == b->xid && a->slot_wrap == b->slot_wrap;
}

bool
cluster_recovery_ctrc_terminal_subject_v1(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, uint32 index, const ClusterRecoveryUndoEntry *observed,
	ClusterColdTtSlotInputV1 *out)
{
	const ClusterColdTypedV1 *typed;
	const ClusterColdTopTxCensusEntryV1 *top = NULL;
	ClusterRecoveryCheckpointSlotV1 *slot;
	ClusterRecoveryCheckpointOwnerV1 owner, confirmed;
	ClusterRecoveryTxResultValueV1 result;
	ClusterColdCtrcReleaseCensusV1 counts;
	ClusterColdTtSlotInputV1 original, value;
	ClusterRecoveryUndoEntry physical;
	uint64 slot_generation, work_generation;
	Size work_bytes, census_bytes;
	bool same;

	if (out == NULL || observed == NULL || CritSectionCount != 0
		|| start == NULL || start != checkpoint_registered_start
		|| cluster_recovery_plan_shmem == NULL
		|| !ClusterColdRecoveryCtrcWorkCurrent(start, work)
		|| !ClusterColdRecoveryTxCheckpointOwnerV1(start, &owner)
		|| memcmp(&owner, &checkpoint_registered_owner, sizeof(owner)) != 0)
		return false;
	typed = ClusterColdRecoveryTxInputs(start);
	if (typed == NULL || typed->top_tx_census == NULL || !typed->census_root_captured
		|| typed->census_root.system_identifier != owner.writer.ref.claim.identity.system_identifier
		|| index >= work->counts.subject_count || work->counts.unqualified_count != 0
		|| work->counts.subject_count
			> (MaxAllocSize - offsetof(ClusterColdCtrcWorkV1, entries)) / sizeof(work->entries[0])
		|| typed->top_tx_census->count > (MaxAllocSize - offsetof(ClusterColdTopTxCensusV1, entries))
			/ sizeof(typed->top_tx_census->entries[0]))
		return false;
	work_bytes = offsetof(ClusterColdCtrcWorkV1, entries)
		+ (Size)work->counts.subject_count * sizeof(work->entries[0]);
	census_bytes = offsetof(ClusterColdTopTxCensusV1, entries)
		+ (Size)typed->top_tx_census->count * sizeof(typed->top_tx_census->entries[0]);
	if (recovery_ctrc_subject_overlap(out, sizeof(*out), work, work_bytes)
		|| recovery_ctrc_subject_overlap(out, sizeof(*out), typed, sizeof(*typed))
		|| recovery_ctrc_subject_overlap(out, sizeof(*out), typed->top_tx_census, census_bytes)
		|| recovery_ctrc_subject_overlap(out, sizeof(*out), observed, sizeof(*observed))
		|| recovery_ctrc_subject_overlap(out, sizeof(*out), cluster_recovery_plan_shmem,
			sizeof(*cluster_recovery_plan_shmem))
		|| !ClusterColdRecoveryStartOutputDistinct(start, out, sizeof(*out)))
		return false;
	counts = work->counts;
	work_generation = work->generation;
	original = value = work->entries[index];
	physical = *observed;
	if (original.key.owner_instance != physical.owner_instance
		|| original.key.segment_id != physical.segment_id
		|| original.key.segment_generation != physical.segment_generation
		|| original.key.slot_offset != physical.slot_offset || physical.slot_offset >= TT_SLOTS_PER_SEGMENT
		|| original.key.xid != original.slot.xid || original.key.slot_wrap != original.slot.wrap
		|| original.key.xid != physical.slot.xid || original.key.slot_wrap != physical.slot.wrap
		|| !TransactionIdIsNormal(original.key.xid) || (original.slot.flags & ~TT_SLOT_FLAGS_KNOWN) != 0)
		return false;

	/* Only the original completed C0 loop may supply this receipt. Observe its
	 * entire local generation/value; never infer completion from physical ABORT. */
	slot = &cluster_recovery_plan_shmem->checkpoint_owner;
	SpinLockAcquire(&slot->lock);
	result = slot->tx_result;
	slot_generation = slot->generation;
	same = slot->valid && memcmp(&slot->owner, &owner, sizeof(owner)) == 0
		&& result.state == CLUSTER_RECOVERY_TX_RESULT_COMPLETE && result.result_generation != 0
		&& slot->tx_accepted_generation == result.result_generation
		&& result.subject_count == typed->top_tx_census->count
		&& result.processed_count == result.subject_count && result.completed_count == result.subject_count
		&& result.unresolved_count == 0
		&& memcmp(result.input_root_sha256, typed->census_root.image_sha256, 32) == 0;
	SpinLockRelease(&slot->lock);
	if (!same)
		return false;
	if (original.slot.status == TT_SLOT_ACTIVE) {
		ClusterRecoveryTTAbortDerivationV1 c0;

		for (uint32 i = 0; i < typed->top_tx_census->count; i++) {
			const ClusterColdTopTxCensusEntryV1 *entry = &typed->top_tx_census->entries[i];
			if (!recovery_ctrc_subject_key_same(&entry->input.key, &original.key))
				continue;
			if (top != NULL || memcmp(&entry->input.slot, &original.slot, sizeof(original.slot)) != 0)
				return false;
			top = entry;
		}
		if (top == NULL || !ClusterColdRecoveryTxC0Candidate(start, top, &c0)
			|| cluster_recovery_tt_abort_match_v1(&c0.record, physical.segment_generation,
				&physical.slot) != CLUSTER_RECOVERY_TT_ABORT_IDEMPOTENT)
			return false;
		value.slot = physical.slot;
	} else if (((original.slot.status != TT_SLOT_COMMITTED || !SCN_VALID(original.slot.commit_scn))
			&& (original.slot.status != TT_SLOT_ABORTED || SCN_VALID(original.slot.commit_scn)))
		|| memcmp(&original.slot, &physical.slot, sizeof(original.slot)) != 0)
		return false;
	if (!ClusterColdRecoveryCtrcWorkCurrent(start, work) || work->generation != work_generation
		|| memcmp(&counts, &work->counts, sizeof(counts)) != 0
		|| memcmp(&original, &work->entries[index], sizeof(original)) != 0
		|| memcmp(&physical, observed, sizeof(physical)) != 0
		|| !ClusterColdRecoveryTxCheckpointOwnerV1(start, &confirmed)
		|| memcmp(&owner, &confirmed, sizeof(owner)) != 0
		|| (top != NULL && (!ClusterColdRecoveryTxTopProofCurrent(start, top)
			|| !ClusterColdRecoveryTxEffectsCurrent(start))))
		return false;
	SpinLockAcquire(&slot->lock);
	same = slot->valid && slot->generation == slot_generation
		&& memcmp(&slot->owner, &owner, sizeof(owner)) == 0
		&& memcmp(&slot->tx_result, &result, sizeof(result)) == 0
		&& slot->tx_accepted_generation == result.result_generation;
	SpinLockRelease(&slot->lock);
	if (!same)
		return false;
	*out = value;
	return true;
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

typedef struct RecoveryCtrcHandoffV1 {
	const struct ClusterRecoveryStartV1 *start;
	const ClusterColdTypedV1 *typed;
	const ClusterColdCtrcWorkV1 *work;
	const ClusterColdPlanV1 *plan;
	const ClusterColdTopTxCensusV1 *census;
	uint64 work_generation, slot_generation, first_result_generation;
	ClusterColdCtrcReleaseCensusV1 counts;
	ClusterControlRootFileToken root;
	ClusterRecoveryCheckpointOwnerV1 owner;
	ClusterRecoveryTxResultValueV1 tx;
} RecoveryCtrcHandoffV1;

static RecoveryTxHandoffStatus
recovery_ctrc_result_handoff_status(const RecoveryCtrcHandoffV1 *fixed)
{
	ClusterRecoveryCheckpointSlotV1 *slot;
	ClusterRecoveryCheckpointOwnerV1 owner;
	const ClusterRecoveryCtrcResultValueV1 *result;
	RecoveryTxHandoffStatus status = RECOVERY_TX_HANDOFF_BLOCKED;
	uint8 zero[32] = {0};

	if (MyBackendType != B_STARTUP || !IsUnderPostmaster || CritSectionCount != 0
		|| ShutdownRequestPending || fixed->start != checkpoint_registered_start
		|| cluster_recovery_plan_shmem == NULL
		|| !ClusterColdRecoveryCtrcWorkCurrent(fixed->start, fixed->work)
		|| ClusterColdRecoveryTxInputs(fixed->start) != fixed->typed
		|| fixed->work->generation != fixed->work_generation
		|| memcmp(&fixed->work->counts, &fixed->counts, sizeof(fixed->counts)) != 0
		|| fixed->typed->plan != fixed->plan || fixed->typed->top_tx_census != fixed->census
		|| fixed->census->plan != fixed->plan || !fixed->typed->census_root_captured
		|| memcmp(&fixed->typed->census_root, &fixed->root, sizeof(fixed->root)) != 0
		|| fixed->root.system_identifier != fixed->owner.writer.ref.claim.identity.system_identifier
		|| memcmp(fixed->root.image_sha256, zero, sizeof(zero)) == 0
		|| !ClusterColdRecoveryTxCheckpointOwnerV1(fixed->start, &owner)
		|| memcmp(&owner, &fixed->owner, sizeof(owner)) != 0
		|| memcmp(&checkpoint_registered_owner, &owner, sizeof(owner)) != 0
		|| cluster_epoch_get_current() != owner.writer.epoch
		|| fixed->tx.state != CLUSTER_RECOVERY_TX_RESULT_COMPLETE
		|| fixed->tx.result_generation == 0 || fixed->tx.subject_count != fixed->census->count
		|| fixed->tx.processed_count != fixed->tx.subject_count
		|| fixed->tx.completed_count != fixed->tx.subject_count || fixed->tx.unresolved_count != 0
		|| memcmp(fixed->tx.input_root_sha256, fixed->root.image_sha256, 32) != 0
		|| fixed->first_result_generation == 0)
		return status;
	slot = &cluster_recovery_plan_shmem->checkpoint_owner;
	SpinLockAcquire(&slot->lock);
	result = &slot->ctrc_result;
	if (slot->valid && slot->generation == fixed->slot_generation
		&& memcmp(&slot->owner, &fixed->owner, sizeof(slot->owner)) == 0
		&& memcmp(&slot->tx_result, &fixed->tx, sizeof(fixed->tx)) == 0
		&& slot->tx_accepted_generation == fixed->tx.result_generation
		&& result->result_generation >= fixed->first_result_generation
		&& result->subject_count == fixed->counts.subject_count
		&& result->released_count <= result->subject_count
		&& result->unresolved_count == result->subject_count - result->released_count
		&& memcmp(result->input_root_sha256, fixed->root.image_sha256, 32) == 0) {
		if (result->state == CLUSTER_RECOVERY_CTRC_RESULT_PENDING
			&& slot->ctrc_accepted_generation == 0)
			status = RECOVERY_TX_HANDOFF_WAIT;
		else if (result->state == CLUSTER_RECOVERY_CTRC_RESULT_COMPLETE
			&& result->released_count == result->subject_count && result->unqualified_count == 0) {
			if (slot->ctrc_accepted_generation == result->result_generation)
				status = RECOVERY_TX_HANDOFF_ACCEPTED;
			else if (slot->ctrc_accepted_generation == 0)
				status = RECOVERY_TX_HANDOFF_WAIT;
		}
	}
	SpinLockRelease(&slot->lock);
	return status;
}

bool
cluster_recovery_ctrc_result_wait_v1(const struct ClusterRecoveryStartV1 *start)
{
	RecoveryCtrcHandoffV1 fixed = {0};
	ClusterRecoveryCheckpointSlotV1 *slot;

	if (start == NULL)
		return true;
	if (start != checkpoint_registered_start || cluster_recovery_plan_shmem == NULL
		|| !ClusterColdRecoveryTxCheckpointOwnerV1(start, &fixed.owner))
		return false;
	fixed.start = start;
	fixed.typed = ClusterColdRecoveryTxInputs(start);
	if (fixed.typed == NULL || fixed.typed->top_tx_census == NULL
		|| !ClusterColdRecoveryCtrcWorkCurrent(start, fixed.typed->ctrc_work))
		return false;
	fixed.work = fixed.typed->ctrc_work;
	fixed.work_generation = fixed.work->generation;
	fixed.counts = fixed.work->counts;
	fixed.plan = fixed.typed->plan;
	fixed.census = fixed.typed->top_tx_census;
	fixed.root = fixed.typed->census_root;
	slot = &cluster_recovery_plan_shmem->checkpoint_owner;
	SpinLockAcquire(&slot->lock);
	fixed.slot_generation = slot->generation;
	fixed.first_result_generation = slot->ctrc_result.result_generation;
	fixed.tx = slot->tx_result;
	SpinLockRelease(&slot->lock);
	if (recovery_ctrc_result_handoff_status(&fixed) == RECOVERY_TX_HANDOFF_BLOCKED)
		return false;
	for (;;) {
		RecoveryTxHandoffStatus status;

		ResetLatch(MyLatch);
		HandleStartupProcInterrupts();
		/* Interrupts may revoke this owner. Accept only a current result
		 * after that boundary, never a pre-interrupt COMPLETE snapshot. */
		status = recovery_ctrc_result_handoff_status(&fixed);
		if (status == RECOVERY_TX_HANDOFF_BLOCKED)
			return false;
		if (status == RECOVERY_TX_HANDOFF_ACCEPTED)
			return true;
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

/* Canonical SPACE and version checks only. The digest is a comparison value,
 * never custody; the dedicated preparation path consumes live protection. */
static bool
stamp_fpi_image_matches(const ClusterRecoveryStampOwnerV1 *owner,
	const ClusterRecoveryStampFpiV1 *fpi)
{
	const GenericXLogRecoveryImageDescriptorV1 *image = &fpi->image;
	ClusterSpaceIdentityKey key = {0};
	ClusterSpaceIdentity identity;
	RfPageVersionEdgeEntryV1 expected = {0};
	uint8 digest = 0;

	key.system_identifier = owner->target.identity.system_identifier;
	key.database_incarnation = owner->target.database_incarnation;
	memcpy(key.storage_uuid, owner->target.identity.storage_uuid, 16);
	key.locator = fpi->locator;
	if ((image->owner != GENERIC_XLOG_CTRC_MX && image->owner != GENERIC_XLOG_CTRC_ITL)
		|| image->fork_number != MAIN_FORKNUM || image->block_number == InvalidBlockNumber
		|| image->result_token == 0 || image->version.before.mutation_token == 0
		|| image->result_token == image->version.before.mutation_token
		|| fpi->reserved_zero != 0
		|| !cluster_space_identity_decode(image->space_identity, sizeof(image->space_identity),
			&key, &identity) || identity.state != CLUSTER_SPACE_IDENTITY_LIVE)
		return false;
	expected.page_class = RF_PAGE_CLASS_ORDINARY;
	expected.before_kind = expected.result_kind = RF_PAGE_STATE_PRESENT;
	memcpy(expected.before.segment_incarnation, identity.incarnation, 16);
	expected.before.mutation_token = image->version.before.mutation_token;
	memcpy(expected.result_incarnation, identity.incarnation, 16);
	if (memcmp(&expected, &image->version, sizeof(expected)) != 0)
		return false;
	for (unsigned i = 0; i < sizeof(image->after_sha256); i++)
		digest |= image->after_sha256[i];
	return digest != 0;
}

/* Decode only caller-owned inputs or snapshots. D0 hashing may allocate;
 * neither a slot spinlock nor an insertion critical section may cover it. */
static bool
stamp_payload_matches(const ClusterRecoveryStampOwnerV1 *owner, ClusterRecoveryStampKindV1 kind,
					  const uint8 *payload, Size length, uint16 *target_thread)
{
	if (!stamp_owner_valid(owner))
		return false;
	if (kind == CLUSTER_RECOVERY_STAMP_FPI && length == sizeof(ClusterRecoveryStampFpiV1)
		&& payload != NULL && CritSectionCount == 0) {
		ClusterRecoveryStampFpiV1 fpi;
		memcpy(&fpi, payload, sizeof(fpi));
		if (!stamp_fpi_image_matches(owner, &fpi)
			|| fpi.protection.source_binding_cookie == 0
			|| fpi.protection.protection_fingerprint == 0
			|| fpi.protection.partition >= RF_PAGE_GUARD_PARTITIONS
			|| fpi.protection.reserved_zero != 0)
			return false;
		*target_thread = owner->target.identity.origin_thread_id;
		return true;
	}
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

/* Constant-sized pure metadata checks under the slot spinlock. Keep the
 * original worker attempt/adoption contract independent of payload decoding. */
static bool
stamp_state_shape(const ClusterRecoveryStampSlotV1 *slot, uint16 thread)
{
	const ClusterRecoveryStampHandleV1 *handle = &slot->handle;
	const ClusterRecoveryStampKeyV1 *key = &handle->key;
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
		return !slot->failed && slot->record_start == 0 && slot->record_end == 0 && handle->kind == 0
			   && handle->payload_length == 0 && slot->executor_attempt_stamp == 0
			   && ((key->slot_generation == 0 && key->window_seq == 0 && key->target_thread == 0)
				   || (key->slot_generation > 0 && key->window_seq >= key->slot_generation
					   && key->target_thread == thread && stamp_owner_valid(&slot->handle.owner)));
	if (key->target_thread != thread || key->slot_generation == 0
		|| key->window_seq < key->slot_generation || !stamp_owner_valid(&handle->owner)
		|| !((handle->kind == CLUSTER_RECOVERY_STAMP_C0
			  && handle->payload_length == CLUSTER_RECOVERY_TT_ABORT_BYTES)
			 || (handle->kind == CLUSTER_RECOVERY_STAMP_FPI
				 && handle->payload_length == sizeof(ClusterRecoveryStampFpiV1))
			 || (handle->kind == CLUSTER_RECOVERY_STAMP_D0
				 && handle->payload_length == CLUSTER_RECOVERY_CTRC_BYTES)))
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
	out->adopt_from = slot->adopt_from;
	out->executor_attempt_stamp = slot->executor_attempt_stamp;
	out->bound_worker_attempt = slot->bound_worker_attempt;
	out->bound_worker_pid = slot->bound_worker_pid;
	out->record_start = slot->record_start;
	out->record_end = slot->record_end;
	out->failed = slot->failed;
	SpinLockRelease(&slot->lock);
}

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
		&& stamp_state_shape(slot, thread) && slot->phase == CLUSTER_RECOVERY_STAMP_EMPTY
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
	ClusterRecoveryStampSlotV1 snapshot;
	bool accepted = false;
	if (!cluster_shared_config || !cluster_enabled || !IsUnderPostmaster || MyProcPid <= 0
		|| MyBackendType != B_LMON || CritSectionCount != 0 || attempt == 0
		|| (replay = cluster_thread_recovery_replay_slot(thread)) == NULL)
		return false;
	slot = &replay->stamp;
	stamp_snapshot(slot, &snapshot);
	if (!stamp_shape(&snapshot, thread))
		return false;
	SpinLockAcquire(&slot->lock);
	/* Validate payload outside the lock, then consume that exact operation
	 * and worker binding. A stopped worker cannot gain a replacement window. */
	if (!stamp_state_shape(slot, thread)
		|| memcmp(&slot->handle, &snapshot.handle, sizeof(slot->handle)) != 0
		|| slot->phase != snapshot.phase || slot->adopt_from != snapshot.adopt_from
		|| slot->executor_attempt_stamp != snapshot.executor_attempt_stamp
		|| slot->bound_worker_attempt != snapshot.bound_worker_attempt
		|| slot->bound_worker_pid != snapshot.bound_worker_pid
		|| slot->record_start != snapshot.record_start || slot->record_end != snapshot.record_end
		|| slot->failed != snapshot.failed)
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

static bool
stamp_prepare(const ClusterRecoveryStampOwnerV1 *owner,
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
	slot->handle.key.target_thread = target_thread;
	slot->handle.owner = *owner;
	slot->handle.kind = kind;
	slot->handle.payload_length = length;
	memset(slot->handle.payload, 0, sizeof(slot->handle.payload));
	memcpy(slot->handle.payload, payload, length);
	slot->phase = CLUSTER_RECOVERY_STAMP_PRE_INSERT;
	slot->adopt_from = CLUSTER_RECOVERY_STAMP_EMPTY;
	slot->executor_attempt_stamp = MyBackendType == B_BG_WORKER ? slot->bound_worker_attempt : 0;
	*out = slot->handle;
	accepted = true;
done:
	SpinLockRelease(&slot->lock);
	return accepted;
}

bool
cluster_recovery_stamp_prepare_v1(const ClusterRecoveryStampOwnerV1 *owner,
	ClusterRecoveryStampKindV1 kind, const uint8 *payload, Size length,
	ClusterRecoveryStampHandleV1 *out)
{
	if (kind == CLUSTER_RECOVERY_STAMP_FPI) {
		if (out != NULL)
			memset(out, 0, sizeof(*out));
		return false;
	}
	return stamp_prepare(owner, kind, payload, length, out);
}

/* All fallible owner checks precede the original short publication lock.
 * No image hash, new protection acquisition or target mutation occurs here. */
bool
cluster_recovery_stamp_prepare_fpi_v1(const ClusterRecoveryStampOwnerV1 *owner,
	const RfPageIdentityV1 *target, const GenericXLogRecoveryImageDescriptorV1 *image,
	const RfPageAuthorityGuardV1 *guard, ClusterRecoverySerialGuard *serial_guard,
	ClusterWalRetentionPin *retention_pin, ClusterRecoveryStampHandleV1 *out)
{
	ClusterRecoveryStampFpiV1 fpi = {0};

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!stamp_actor() || CritSectionCount != 0 || !stamp_owner_valid(owner)
		|| owner->executor_pid != MyProcPid || target == NULL || image == NULL
		|| serial_guard == NULL || !serial_guard->held || serial_guard->release_uncertain
		|| (serial_guard->mode != CLUSTER_RECOVERY_SERIAL_ONLINE
			&& serial_guard->mode != CLUSTER_RECOVERY_SERIAL_COLD_FORMED)
		|| memcmp(&serial_guard->duty, &owner->target.identity, sizeof(serial_guard->duty)) != 0
		|| target->system_identifier != owner->target.identity.system_identifier
		|| memcmp(target->storage_uuid, owner->target.identity.storage_uuid, 16) != 0
		|| target->reserved_zero != 0 || target->forknum != image->fork_number
		|| target->blockno != image->block_number)
		return false;
	fpi.image = *image;
	fpi.locator = target->locator;
	if (!stamp_fpi_image_matches(owner, &fpi)
		|| !rf_page_authority_recovery_reference_v1(guard, serial_guard, retention_pin, target,
			&image->version.before, &fpi.protection))
		return false;
	return stamp_prepare(owner, CLUSTER_RECOVERY_STAMP_FPI, (const uint8 *)&fpi, sizeof(fpi), out);
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
	if (!stamp_handle_matches(slot, handle)
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
		slot->handle.kind = 0;
		slot->handle.payload_length = 0;
		memset(slot->handle.payload, 0, sizeof(slot->handle.payload));
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
bool
cluster_recovery_stamp_ctrc_prepare_v2(const ClusterRecoveryStampOwnerV1 *owner,
	const uint8 *payload, Size length, ClusterRecoveryStampHandleV1 *out,
	ClusterUndoSmgrRecoveryStampV1 *target)
{
	ClusterUndoSmgrRecoveryStampV1 prepared;
	ClusterRecoveryStampHandleV1 handle;
	uint16 target_thread;
	volatile bool accepted = false;

	/* Keep public outputs private and unchanged until both original owners
	 * have accepted. In particular, do not zero an aliased input/live slot. */
	if (!stamp_actor() || MyBackendType != B_STARTUP || CritSectionCount != 0
		|| owner == NULL || payload == NULL || out == NULL || target == NULL
		|| owner->executor_pid != MyProcPid || length != CLUSTER_RECOVERY_CTRC_BYTES
		|| stamp_ranges_overlap(out, sizeof(*out), target, sizeof(*target))
		|| stamp_ranges_overlap(out, sizeof(*out), owner, sizeof(*owner))
		|| stamp_ranges_overlap(target, sizeof(*target), owner, sizeof(*owner))
		|| stamp_ranges_overlap(out, sizeof(*out), payload, length)
		|| stamp_ranges_overlap(target, sizeof(*target), payload, length)
		|| stamp_ranges_overlap(out, sizeof(*out), cluster_recovery_plan_shmem,
			   sizeof(*cluster_recovery_plan_shmem))
		|| stamp_ranges_overlap(target, sizeof(*target), cluster_recovery_plan_shmem,
			   sizeof(*cluster_recovery_plan_shmem))
		|| !stamp_payload_matches(owner, CLUSTER_RECOVERY_STAMP_D0, payload, length,
			   &target_thread))
		return false;

	/* All codec work and descriptor acquisition precede PRE_INSERT. The
	 * target API owns its own failed preparation; only a successful token
	 * belongs to this wrapper and may be closed on window refusal/ERROR. */
	if (!cluster_undo_smgr_recovery_ctrc_prepare_v2(payload, length,
			owner->writer.ref.claim.identity.system_identifier, &prepared))
		return false;
	PG_TRY();
	{
		accepted = cluster_recovery_stamp_prepare_v1(owner, CLUSTER_RECOVERY_STAMP_D0,
			payload, length, &handle);
	}
	PG_CATCH();
	{
		(void)cluster_undo_smgr_recovery_ctrc_close_v2(prepared);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!accepted) {
		(void)cluster_undo_smgr_recovery_ctrc_close_v2(prepared);
		return false;
	}

	memcpy(out, &handle, sizeof(handle));
	memcpy(target, &prepared, sizeof(prepared));
	return true;
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

	/* Refuse a stale token or another prepared payload before the first
	 * irreversible WAL insertion. This read-only check leaves PRE_INSERT
	 * cancelable; finish still rechecks the bound stage after WAL flush. */
	if (!cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, handle->payload,
			handle->payload_length))
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

XLogRecPtr
cluster_recovery_stamp_ctrc_execute_v2(const ClusterRecoveryStampOwnerV1 *owner,
	const uint8 *payload, Size length, void *raw, Size raw_length, XLogRecPtr *start)
{
	uint8 copy[SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong + CLUSTER_RECOVERY_CTRC_BYTES];
	ClusterRecoveryStampHandleV1 handle;
	ClusterUndoSmgrRecoveryStampV1 target;
	XLogRecPtr actual_start, end;

	/* Check every public output before descriptor acquisition or PRE_INSERT.
	 * Both original nested operations receive only private stack storage. */
	if (!stamp_actor() || MyBackendType != B_STARTUP || CritSectionCount != 0
		|| cluster_recovery_plan_shmem == NULL || owner == NULL || payload == NULL
		|| raw == NULL || start == NULL || length != CLUSTER_RECOVERY_CTRC_BYTES
		|| raw_length != sizeof(copy)
		|| stamp_ranges_overlap(raw, raw_length, start, sizeof(*start))
		|| stamp_ranges_overlap(raw, raw_length, owner, sizeof(*owner))
		|| stamp_ranges_overlap(start, sizeof(*start), owner, sizeof(*owner))
		|| stamp_ranges_overlap(raw, raw_length, payload, length)
		|| stamp_ranges_overlap(start, sizeof(*start), payload, length)
		|| stamp_ranges_overlap(raw, raw_length, cluster_recovery_plan_shmem,
			   sizeof(*cluster_recovery_plan_shmem))
		|| stamp_ranges_overlap(start, sizeof(*start), cluster_recovery_plan_shmem,
			   sizeof(*cluster_recovery_plan_shmem)))
		return InvalidXLogRecPtr;
	if (!cluster_recovery_stamp_ctrc_prepare_v2(owner, payload, length, &handle, &target))
		return InvalidXLogRecPtr;

	/* The original insertion/flush/target sequence owns one critical section.
	 * Only an exact PRE_INSERT cancellation permits a normal refusal. Once
	 * insertion starts, never exit the section or close the held target on
	 * failure; native ERROR/PANIC is not intercepted or converted to success. */
	START_CRIT_SECTION();
	end = cluster_recovery_stamp_ctrc_apply_v2(&handle, target, copy, sizeof(copy), &actual_start);
	if (end == InvalidXLogRecPtr) {
		if (!cluster_recovery_stamp_cancel_v1(&handle))
			ereport(PANIC, (errmsg("D0 recovery stamp could not complete its inserted operation")));
		END_CRIT_SECTION();
		(void)cluster_undo_smgr_recovery_ctrc_close_v2(target);
		return InvalidXLogRecPtr;
	}
	END_CRIT_SECTION();

	/* Real target durability and exact-window clear preceded this close.
	 * A close refusal cannot undo them, but must not publish partial outputs. */
	if (!cluster_undo_smgr_recovery_ctrc_close_v2(target))
		return InvalidXLogRecPtr;
	memcpy(raw, copy, sizeof(copy));
	memcpy(start, &actual_start, sizeof(actual_start));
	return end;
}

bool
cluster_recovery_stamp_ctrc_execute_reference_v2(const ClusterRecoveryStampOwnerV1 *owner,
	const uint8 *payload, Size length, uint32 source_index,
	const uint8 source_identity_digest[32], void *raw, Size raw_length,
	ClusterRecoveryCTRCReferenceV2 *out)
{
	uint8 copy[SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong + CLUSTER_RECOVERY_CTRC_BYTES];
	ClusterRecoveryCTRCReferenceV2 reference = { 0 };
	uint8 source_nonzero = 0;

	/* A malformed external source/output must not be discovered only after
	 * emitting a durable record. Do not overwrite an input or a live slot. */
	if (!stamp_actor() || MyBackendType != B_STARTUP || CritSectionCount != 0
		|| cluster_recovery_plan_shmem == NULL || owner == NULL || payload == NULL
		|| source_identity_digest == NULL || raw == NULL || out == NULL
		|| length != CLUSTER_RECOVERY_CTRC_BYTES || raw_length != sizeof(copy)
		|| stamp_ranges_overlap(raw, raw_length, out, sizeof(*out))
		|| stamp_ranges_overlap(raw, raw_length, owner, sizeof(*owner))
		|| stamp_ranges_overlap(out, sizeof(*out), owner, sizeof(*owner))
		|| stamp_ranges_overlap(raw, raw_length, payload, length)
		|| stamp_ranges_overlap(out, sizeof(*out), payload, length)
		|| stamp_ranges_overlap(raw, raw_length, source_identity_digest, 32)
		|| stamp_ranges_overlap(out, sizeof(*out), source_identity_digest, 32)
		|| stamp_ranges_overlap(raw, raw_length, cluster_recovery_plan_shmem,
			   sizeof(*cluster_recovery_plan_shmem))
		|| stamp_ranges_overlap(out, sizeof(*out), cluster_recovery_plan_shmem,
			   sizeof(*cluster_recovery_plan_shmem)))
		return false;
	for (unsigned i = 0; i < 32; i++)
		source_nonzero |= source_identity_digest[i];
	if (source_nonzero == 0)
		return false;

	/* Only the original owner supplies the final raw bytes and actual end.
	 * No source qualification is inferred from a nonzero digest/index. */
	reference.end = cluster_recovery_stamp_ctrc_execute_v2(owner, payload, length,
		copy, sizeof(copy), &reference.start);
	if (reference.end == InvalidXLogRecPtr)
		return false;
	reference.source_index = source_index;
	reference.raw_length = sizeof(copy);
	/* The window is already complete and the target closed. The existing
	 * full-record SHA may refuse/raise ERROR here; never reopen or relabel
	 * that completed operation, or publish either partial public output. */
	if (!cluster_recovery_ctrc_record_hash_v2(source_identity_digest, reference.start,
			reference.end, copy, sizeof(copy), reference.raw_record_sha256))
		return false;
	memcpy(raw, copy, sizeof(copy));
	memcpy(out, &reference, sizeof(reference));
	return true;
}

bool
cluster_recovery_stamp_durable_v1(const ClusterRecoveryStampHandleV1 *handle)
{
	return stamp_transition(handle, CLUSTER_RECOVERY_STAMP_INSERTED, CLUSTER_RECOVERY_STAMP_DURABLE,
							0, 0, false);
}

bool
cluster_recovery_stamp_fpi_inserted_current_v1(const ClusterRecoveryStampHandleV1 *handle,
	XLogRecPtr start, XLogRecPtr end)
{
	ClusterThreadReplaySlot *replay;
	ClusterRecoveryStampSlotV1 snapshot;

	if (!stamp_actor() || CritSectionCount != 0 || handle == NULL
		|| handle->owner.executor_pid != MyProcPid || handle->kind != CLUSTER_RECOVERY_STAMP_FPI
		|| start < handle->owner.writer.startup_first_lsn || end <= start
		|| (replay = cluster_thread_recovery_replay_slot(handle->key.target_thread)) == NULL)
		return false;
	stamp_snapshot(&replay->stamp, &snapshot);
	/* Complete codec checks apply to this copied snapshot outside the slot lock. */
	return stamp_shape(&snapshot, handle->key.target_thread)
		&& stamp_handle_matches(&snapshot, handle) && !snapshot.failed
		&& snapshot.phase == CLUSTER_RECOVERY_STAMP_INSERTED
		&& snapshot.record_start == start && snapshot.record_end == end;
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
		if (pending && snapshot.failed && snapshot.phase != CLUSTER_RECOVERY_STAMP_ADOPT_PENDING)
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
