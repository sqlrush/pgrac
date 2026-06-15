/*-------------------------------------------------------------------------
 *
 * cluster_recovery_plan.h
 *	  pgrac Recovery Coordinator skeleton -- per-thread recovery plan
 *	  (spec-4.3).
 *
 *	  The plan pass runs in the startup process on every PLAIN LOCAL
 *	  startup (clean start included; archive recovery and standby mode
 *	  excluded -- the same gate as the spec-4.1 RL1 reader hook).  It
 *	  is a PLANNING pass, not a "crash recovery confirmed" pass:
 *	  InitWalRecovery() calls it before InRecovery is determined.  The
 *	  spec-4.5 merged-replay activation must add its own hard gate
 *	  after the InRecovery decision; the plan is observational only
 *	  and nothing may act on it in this stage.
 *
 *	  Classification (spec-4.3 §3.2, priority order):
 *	    0  tid == own_thread                      -> OWN (independent of
 *	       the slot verdict: at plan time the own slot is usually EMPTY
 *	       on a first boot or STOPPED after a clean shutdown, because
 *	       ACTIVE is only published at the RUNNING transition)
 *	    1  slot EMPTY                             -> EMPTY
 *	    2  slot OK but node_id != tid - 1         -> UNKNOWN (violates
 *	       the spec-4.1 identity invariant thread_id = node_id + 1;
 *	       a CRC-valid slot with an impossible owner must never be
 *	       classified ALIVE/CRASHED.  This check lives HERE and not in
 *	       the spec-4.2 classifier: the 4.2 publish-side foreign-owner
 *	       FATAL gate relies on such slots still classifying OK so the
 *	       evidence is preserved rather than self-repaired.)
 *	    3  slot OK + STOPPED                      -> CLEAN
 *	    4  slot OK + ACTIVE + fresh last_updated  -> ALIVE (a future
 *	       timestamp counts as fresh: clock skew must err towards NOT
 *	       reporting a live node as crashed)
 *	    5  slot OK + ACTIVE + stale last_updated  -> CRASHED_CANDIDATE
 *	    6  slot CORRUPT (incl. read failure)      -> UNKNOWN
 *
 *	  UNKNOWN is never merged into the crashed-candidate set (absence
 *	  of evidence is not evidence; spec-4.2 round-2 family).  When
 *	  spec-4.5 activates merged replay, UNKNOWN > 0 or a failed plan
 *	  must become fail-closed (SQLSTATE 53RA3 reserved); spec-4.3
 *	  itself never blocks startup on plan problems.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_recovery_plan.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.3-recovery-coordinator-skeleton.md FROZEN v1.0
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_PLAN_H
#define CLUSTER_RECOVERY_PLAN_H

#include "cluster/cluster_wal_state.h"

/* Per-thread recovery verdict (spec-4.3 §3.2 truth table). */
typedef enum ClusterRecoveryThreadVerdict {
	CLUSTER_RECOVERY_THREAD_NONE = 0,		   /* not scanned / plan absent       */
	CLUSTER_RECOVERY_THREAD_OWN,			   /* this node's thread (PG native)  */
	CLUSTER_RECOVERY_THREAD_CLEAN,			   /* STOPPED: clean shutdown         */
	CLUSTER_RECOVERY_THREAD_EMPTY,			   /* never published                 */
	CLUSTER_RECOVERY_THREAD_CRASHED_CANDIDATE, /* stale ACTIVE           */
	CLUSTER_RECOVERY_THREAD_ALIVE,			   /* fresh ACTIVE: live foreign node */
	CLUSTER_RECOVERY_THREAD_UNKNOWN,		   /* CORRUPT / IO / unclassifiable   */
} ClusterRecoveryThreadVerdict;

#define CLUSTER_RECOVERY_PLAN_THREADS CLUSTER_WAL_STATE_SLOT_COUNT

/*
 * The aggregated plan.  Lives in a small shmem mirror (single writer:
 * the startup process; readers attach via the dump accessors).  This
 * is NOT an on-disk structure -- no byte-layout locks needed (L45
 * N/A); verdict[] is indexed by thread_id 1..128, [0] unused.
 */
typedef struct ClusterRecoveryPlan {
	bool generated; /* a pass completed this incarnation  */
	bool failed;	/* pass aborted: counts incomplete    */
	int64 generated_at;
	uint16 own_thread;
	uint16 threads_scanned;
	uint16 n_clean;
	uint16 n_empty;
	uint16 n_crashed_candidate;
	uint16 n_alive;
	uint16 n_unknown;
	uint64 candidate_bitmap[2]; /* derived cache of verdict[t]==CRASHED;
								 * bit (tid-1); unit locks coherence    */
	uint64 max_highest_lsn;		/* max watermark over OK slots          */
	uint64 max_highest_scn;
	uint8 verdict[CLUSTER_RECOVERY_PLAN_THREADS + 1]; /* P1-2: full set */
	uint32 dbstate_at_startup;						  /* ControlFile->state at the hook (P1-3) */
	bool local_recovery_needed;						  /* best-effort InRecovery-input mirror
								 * (state != DB_SHUTDOWNED under the
								 * plain-local gate); NOT the final
								 * InRecovery verdict (P1-3)            */
} ClusterRecoveryPlan;

/* ---- pure helpers (header-only; unit-testable, no backend deps) ---- */

/*
 * cluster_recovery_classify_slot -- the §3.2 truth table.
 *
 *	v / slot come from cluster_wal_state_read_slot (reader mode, so
 *	FOREIGN never appears; any non-OK/EMPTY verdict lands UNKNOWN).
 *	now_us / last_updated are GetCurrentTimestamp() microseconds;
 *	stale_active_ms is cluster.recovery_stale_active_ms.
 */
static inline ClusterRecoveryThreadVerdict
cluster_recovery_classify_slot(ClusterWalSlotVerdict v, const ClusterWalStateSlot *slot,
							   uint16 own_thread, uint16 tid, int64 now_us, int stale_active_ms)
{
	if (tid == own_thread)
		return CLUSTER_RECOVERY_THREAD_OWN;
	if (v == CLUSTER_WAL_SLOT_EMPTY)
		return CLUSTER_RECOVERY_THREAD_EMPTY;
	if (v != CLUSTER_WAL_SLOT_OK)
		return CLUSTER_RECOVERY_THREAD_UNKNOWN;
	if (slot->node_id != (int32)tid - 1)
		return CLUSTER_RECOVERY_THREAD_UNKNOWN;
	if (slot->state == CLUSTER_WAL_SLOT_STATE_STOPPED)
		return CLUSTER_RECOVERY_THREAD_CLEAN;
	/* OK guarantees state is ACTIVE or STOPPED (spec-4.2 classify). */
	if (now_us < slot->last_updated)
		return CLUSTER_RECOVERY_THREAD_ALIVE;
	if (now_us - slot->last_updated <= (int64)stale_active_ms * 1000)
		return CLUSTER_RECOVERY_THREAD_ALIVE;
	return CLUSTER_RECOVERY_THREAD_CRASHED_CANDIDATE;
}

/* Candidate bitmap addressing: bit (tid-1) in word (tid-1)/64. */
static inline bool
cluster_recovery_plan_candidate_test(const ClusterRecoveryPlan *plan, uint16 tid)
{
	return (plan->candidate_bitmap[(tid - 1) / 64] & ((uint64)1 << ((tid - 1) % 64))) != 0;
}

static inline void
cluster_recovery_plan_candidate_set(ClusterRecoveryPlan *plan, uint16 tid)
{
	plan->candidate_bitmap[(tid - 1) / 64] |= ((uint64)1 << ((tid - 1) % 64));
}

#ifndef FRONTEND

#include "port/atomics.h"

/*
 * Per-dead-thread online replay slot (spec-4.11 3b-4b).  Bookkeeping for the
 * online thread-recovery executor: the lmon launch path stamps episode_epoch
 * (the GRD recovery_episode_epoch this attempt belongs to) and marks REPLAYING;
 * the per-episode worker re-reads episode_epoch (the L235 staleness guard) and
 * writes the terminal DONE/BLOCKED.  state holds a ClusterThreadRecReplayState
 * (cluster_thread_recovery.h), stored raw so this header carries no dependency
 * on that enum.  OBSERVABILITY + episode coordination ONLY: the authoritative
 * reader gate reads the node-local merged.authority, NOT this slot (spec-4.11
 * §2.4 Q4 3-way authority).  Rides in the "pgrac recovery plan" shmem region; a
 * small per-thread mirror, no on-disk byte layout (L45 N/A).
 */
typedef struct ClusterThreadReplaySlot {
	pg_atomic_uint32 state;			/* ClusterThreadRecReplayState (raw) */
	pg_atomic_uint64 episode_epoch; /* GRD recovery_episode_epoch when REPLAYING */
} ClusterThreadReplaySlot;

/*
 * Backend API (cluster_recovery_plan.c).  All paths are no-ops when
 * cluster.wal_threads_dir is unset / the thread id is LEGACY.
 */

/* Startup-process single pass (spec-4.3 §3.1).  Caller supplies the
 * ControlFile facts captured at the hook site (P1-3 observability).
 * Never raises above WARNING: the plan is observational and a plan
 * problem must not block startup (fail-open is the spec'd exception;
 * spec-4.5 flips this to fail-closed when merged replay consumes it). */
extern void cluster_recovery_plan_generate(uint32 dbstate_at_startup, bool local_recovery_needed);

/* Acquire-ordered snapshot of the shmem mirror; false when no plan
 * was published this incarnation. */
extern bool cluster_recovery_plan_snapshot(ClusterRecoveryPlan *out);

/* shmem region plumbing (cluster_shmem.c registry). */
extern void cluster_recovery_plan_shmem_register(void);

/*
 * cluster_thread_recovery_replay_slot -- the per-dead-thread online replay slot
 * for dead_tid, or NULL when no shmem is attached or dead_tid is outside the
 * real range [XLP_THREAD_ID_FIRST_REAL, CLUSTER_WAL_THREAD_MAX] (the array rides
 * in the recovery-plan region, indexed by thread id, [0] unused).  The
 * orchestrator's state helpers (mark/read/set, spec-4.11 3b-4b) reach the slot
 * only through this accessor.
 */
extern ClusterThreadReplaySlot *cluster_thread_recovery_replay_slot(uint16 dead_tid);

#endif /* !FRONTEND */

#endif /* CLUSTER_RECOVERY_PLAN_H */
