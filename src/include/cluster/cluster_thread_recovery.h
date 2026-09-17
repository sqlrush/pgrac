/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery.h
 *	  pgrac online single-thread recovery -- a survivor online-replays a
 *	  dead WAL thread's data to shared storage within the reconfig freeze
 *	  window, instead of waiting for the dead node's cold restart
 *	  (spec-4.11, #84 Thread recovery).
 *
 *	  Scope: every positive formed survivor count is applicable; candidates
 *	  compete through the STOP03 IR(X) serialization guard.  The apply path is
 *	  Q10-B (D0 verdict, Impl note v0.1): a
 *	  per-rmgr apply-through matrix that mirrors spec-4.10's
 *	  recovery-context-stripped model -- ApplyWalRecord is NOT online-safe
 *	  (AdvanceNextFullTransactionIdPastXid would push the live survivor's
 *	  global nextXid with a foreign-segment xid; xlogrecovery.c:2331).
 *
 *	  fail-closed (8.A): when a dead thread cannot be online-replayed
 *	  completely / in order / to a known target LSN, its resources stay
 *	  frozen (never serve a possibly stale page); the survivor is NOT
 *	  crashed unless cluster.thread_recovery_on_unrecoverable=panic.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_thread_recovery.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.11-thread-recovery.md (FROZEN v0.3)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_THREAD_RECOVERY_H
#define CLUSTER_THREAD_RECOVERY_H

#include "access/xlogdefs.h"
#include "storage/block.h"
#include "cluster/cluster_wal_thread.h"

struct XLogReaderState;
typedef struct ClusterRecoverySerialGuard ClusterRecoverySerialGuard;
typedef struct ClusterThreadRecoveryAuthorityV1 ClusterThreadRecoveryAuthorityV1;

/*
 * Result of attempting to online-recover one dead thread (spec-4.11 §2.2).
 * NEVER DONE without a proven recovered_through up to the validated
 * complete-record boundary; any 8.A precondition failure is BLOCKED
 * (result-returning, not FATAL -- R13).
 */
typedef enum ClusterThreadRecResult {
	CLUSTER_THREADREC_DONE = 0,			  /* recovered_through_local published */
	CLUSTER_THREADREC_BLOCKED = 1,		  /* verified semantic failure (53RA4) */
	CLUSTER_THREADREC_NOT_APPLICABLE = 2, /* no online recovery attempt */
	CLUSTER_THREADREC_DEFERRED = 3,		  /* transient; keep REPLAYING for reap */
} ClusterThreadRecResult;

/*
 * cluster.thread_recovery_on_unrecoverable policy (spec-4.11 §2.1, Q5).
 * Default keep_frozen = result-returning BLOCKED, the survivor is not
 * crashed; panic = PANIC the survivor (postmaster-level), an operator
 * escape valve only.
 */
typedef enum ClusterThreadRecAction {
	CLUSTER_THREADREC_ACTION_KEEP_FROZEN = 0,
	CLUSTER_THREADREC_ACTION_PANIC,
} ClusterThreadRecAction;

/*
 * Per-thread online replay state (observability + episode coordination;
 * the authoritative reader gate reads the node-local merged.authority,
 * NOT this state -- spec-4.11 §2.4 Q4 3-way authority).
 */
typedef enum ClusterThreadRecReplayState {
	CLUSTER_THREADREC_REPLAY_IDLE = 0,
	CLUSTER_THREADREC_REPLAY_REPLAYING,
	CLUSTER_THREADREC_REPLAY_DONE,
	CLUSTER_THREADREC_REPLAY_BLOCKED,
} ClusterThreadRecReplayState;

typedef enum ClusterThreadReplayMatchResult {
	CLUSTER_THREADREC_MATCH_CHANGED = 0,
	CLUSTER_THREADREC_MATCH_STATE_MISMATCH = 1,
	CLUSTER_THREADREC_MATCH_STAMP_MISMATCH = 2,
	CLUSTER_THREADREC_MATCH_INVALID = 3
} ClusterThreadReplayMatchResult;

static inline bool
cluster_thread_recovery_replay_transition_shape_valid(ClusterThreadRecReplayState expected,
													  ClusterThreadRecReplayState target)
{
	return expected == CLUSTER_THREADREC_REPLAY_REPLAYING
		   && (target == CLUSTER_THREADREC_REPLAY_IDLE || target == CLUSTER_THREADREC_REPLAY_DONE
			   || target == CLUSTER_THREADREC_REPLAY_BLOCKED);
}

/*
 * Scope / capability decision (spec-4.11 §3 behaviour contract).  Pure,
 * so it is unit-testable in isolation (L106 family: decide from facts,
 * the .c wrapper reads the live runtime and calls this).
 *
 *	guc_on            cluster.online_thread_recovery
 *	has_peers         cluster_conf_has_peers() (single-node -> N/A)
 *	shared_fs_backend a genuinely shared data backend is configured
 *	                  (cluster_fs); without it online apply-through is
 *	                  not supported (mirror spec-4.5a 53RA3 capability gate)
 *	live_node_count   nodes still alive after the death; every positive formed
 *	                  survivor count competes under the STOP03 IR resource
 */
typedef enum ClusterThreadRecScope {
	CLUSTER_THREADREC_SCOPE_APPLICABLE = 0,	   /* attempt online recovery     */
	CLUSTER_THREADREC_SCOPE_DISABLED,		   /* GUC off                      */
	CLUSTER_THREADREC_SCOPE_SINGLE_NODE,	   /* no peers -> PG-native crash  */
	CLUSTER_THREADREC_SCOPE_NO_SHARED_BACKEND, /* FEATURE_NOT_SUPPORTED        */
} ClusterThreadRecScope;

static inline ClusterThreadRecScope
cluster_thread_recovery_decide_scope(bool guc_on, bool has_peers, bool shared_fs_backend,
									 int live_survivor_count)
{
	if (!guc_on)
		return CLUSTER_THREADREC_SCOPE_DISABLED;
	if (!has_peers)
		return CLUSTER_THREADREC_SCOPE_SINGLE_NODE;
	if (!shared_fs_backend)
		return CLUSTER_THREADREC_SCOPE_NO_SHARED_BACKEND;
	if (live_survivor_count < 1)
		return CLUSTER_THREADREC_SCOPE_SINGLE_NODE;
	return CLUSTER_THREADREC_SCOPE_APPLICABLE;
}

/*
 * cluster_thread_recovery_scope_is_unsupported -- the D7 capability-gate
 * predicate (spec-4.11 §3, §D7).  A scope is hard-UNSUPPORTED (the operator
 * asked for online thread recovery but this configuration cannot provide it ->
 * FEATURE_NOT_SUPPORTED, mirror spec-4.5a's 53RA3) when there is no genuinely
 * shared data backend (NO_SHARED_BACKEND).  DISABLED
 * (GUC off) and SINGLE_NODE (no peers -> PG-native crash recovery) are NOT
 * unsupported -- they are ordinary not-applicable fall-throughs, no error.  PURE
 * so the gate boundary is unit-pinned: it must NEVER raise FEATURE_NOT_SUPPORTED
 * for a merely not-applicable scope (that would crash a single-node / GUC-off
 * reconfig path -- the t/249-252 no-regression guarantee).
 */
static inline bool
cluster_thread_recovery_scope_is_unsupported(ClusterThreadRecScope scope)
{
	return scope == CLUSTER_THREADREC_SCOPE_NO_SHARED_BACKEND;
}

/*
 * What the orchestrator does on a FINAL BLOCKED (spec-4.11 §3, Q5 policy).
 * Maps the cluster.thread_recovery_on_unrecoverable GUC (a
 * ClusterThreadRecAction) to the action: keep_frozen returns the BLOCKED to the
 * reconfig FSM (the dead thread's resources stay frozen, the survivor keeps
 * running -- minimum blast radius, 8.A), panic crashes the survivor at
 * postmaster level (an operator escape valve only).  PURE so the escalation
 * boundary is unit-pinned (it must NEVER turn keep_frozen into a crash).
 */
typedef enum ClusterThreadRecOnBlocked {
	CLUSTER_THREADREC_ONBLOCKED_KEEP_FROZEN = 0, /* return BLOCKED, survivor lives */
	CLUSTER_THREADREC_ONBLOCKED_PANIC,			 /* PANIC survivor (escape valve) */
} ClusterThreadRecOnBlocked;

static inline ClusterThreadRecOnBlocked
cluster_thread_recovery_decide_on_blocked(int on_unrecoverable_policy)
{
	return (on_unrecoverable_policy == CLUSTER_THREADREC_ACTION_PANIC)
			   ? CLUSTER_THREADREC_ONBLOCKED_PANIC
			   : CLUSTER_THREADREC_ONBLOCKED_KEEP_FROZEN;
}

/*
 * Per-block-reference classification for the RMW replay engine (spec-4.11 D1
 * contract).  PURE, so it is the unit-testable authority for the
 * corruption-critical gate (mirrors cluster_thread_apply_decide): the .c engine
 * computes the smgr facts and calls this, the unit pins every branch.
 *
 *	TARGET        a genuinely shared user-relation block that exists and is in
 *	              range -> read-modify-write it on shared storage.
 *	OUT_OF_SCOPE  not a shared user-relation block (temp / catalog / cluster_fs
 *	              off / opt-in GUC off): a per-node concern the survivor owns, so
 *	              it is a data-pass skip, NOT a recovery failure.
 *	BLOCKED       fail-closed (8.A): either the relation no longer exists, or the
 *	              record references a block at/beyond EOF (relation extension /
 *	              new init page).  3a runs only the data-page apply matrix -- it
 *	              does NOT replay the smgr create/drop/truncate records that would
 *	              explain a missing file or an extension, so it cannot safely skip
 *	              them the way a full PG redo pass can (BLK_NOTFOUND).  Both stay
 *	              frozen and forward (extension/truncation -> Stage 5).
 */
typedef enum ClusterThreadReplayBlockClass {
	CLUSTER_THREADREPLAY_BLK_TARGET = 0,
	CLUSTER_THREADREPLAY_BLK_OUT_OF_SCOPE,
	CLUSTER_THREADREPLAY_BLK_BLOCKED,
} ClusterThreadReplayBlockClass;

/*
 *	which_for    cluster_smgr_which_for(rlocator, InvalidBackendId): 1 == routes
 *	             to genuinely shared storage, 0 == per-node md.c.
 *	rel_exists   smgrexists(reln, forknum) (amend 1: a missing file fails closed,
 *	             never a BLK_NOTFOUND-style skip -- there is no storage rmgr here).
 *	blocknum     the record's target block; nblocks = smgrnblocks(reln, forknum).
 */
static inline ClusterThreadReplayBlockClass
cluster_thread_replay_classify_block(int which_for, bool rel_exists, BlockNumber blocknum,
									 BlockNumber nblocks)
{
	if (which_for != 1)
		return CLUSTER_THREADREPLAY_BLK_OUT_OF_SCOPE; /* per-node: data-pass skip */
	if (!rel_exists)
		return CLUSTER_THREADREPLAY_BLK_BLOCKED; /* dropped: fail-closed (amend 1) */
	if (blocknum >= nblocks)
		return CLUSTER_THREADREPLAY_BLK_BLOCKED; /* extension / new page: forward */
	return CLUSTER_THREADREPLAY_BLK_TARGET;
}

/*
 * Streaming-replay outcome counters (spec-4.11 D1 contract, observability).
 * recovered_through is the EndRecPtr of the LAST record every block reference of
 * which was handled cleanly; it advances only after a whole record is processed
 * without a BLOCKED, so a fail-closed never leaves it claiming an unfinished
 * record (8.A).
 *
 * blocks_applied counts smgrwrite WRITE-BACKs, NOT durable writes: cluster_fs
 * write is a bare pwrite with no inline fsync (amend 2).  3a does not publish
 * authority, so a write-back that has not reached disk is safe (a crash before
 * 3b's durability barrier re-replays from a validated lower bound, redo-
 * idempotent via the LSN-gate).  3b MUST fsync the touched relations BEFORE
 * publishing any 3-way authority, or authority could outlive un-fsync'd pages.
 */
typedef struct ClusterThreadReplayStats {
	uint64 records_scanned;
	uint64 blocks_applied;			/* APPLIED -> smgrwrite (write-back; NOT durable) */
	uint64 blocks_gated;			/* DONE: LSN-gate idempotent skip */
	uint64 blocks_out_of_scope;		/* OUT_OF_SCOPE: per-node block refs skipped */
	uint64 blocks_missing_deferred; /* spec-6.14 D9: missing-file refs deferred to a later drop */
	XLogRecPtr recovered_through;	/* EndRecPtr of last fully-processed record */
} ClusterThreadReplayStats;

#ifndef FRONTEND

#include "cluster/cluster_control_root.h"
#include "cluster/cluster_recovery_duty.h"
#include "postmaster/bgworker.h"
#include "utils/elog.h"				/* elevel constants for the R13 rethrow boundary */
#include "storage/relfilelocator.h" /* RelFileLocator for the touched-rel collector */

typedef enum ClusterThreadRecReapDecision {
	CLUSTER_THREADREC_REAP_RETAIN = 0,
	CLUSTER_THREADREC_REAP_RESET_IDLE = 1,
	CLUSTER_THREADREC_REAP_KEEP_TERMINAL = 2,
	CLUSTER_THREADREC_REAP_INVALID = 3
} ClusterThreadRecReapDecision;

static inline ClusterThreadRecReapDecision
cluster_thread_recovery_reap_decide(BgwHandleStatus handle_status, bool slot_read,
									uint64 owned_stamp, ClusterThreadRecReplayState slot_state,
									uint64 slot_stamp)
{
	if (handle_status == BGWH_STARTED || handle_status == BGWH_NOT_YET_STARTED
		|| handle_status == BGWH_POSTMASTER_DIED)
		return CLUSTER_THREADREC_REAP_RETAIN;
	if (handle_status != BGWH_STOPPED || !slot_read || owned_stamp == 0
		|| slot_stamp != owned_stamp)
		return CLUSTER_THREADREC_REAP_INVALID;
	if (slot_state == CLUSTER_THREADREC_REPLAY_REPLAYING)
		return CLUSTER_THREADREC_REAP_RESET_IDLE;
	if (slot_state == CLUSTER_THREADREC_REPLAY_DONE
		|| slot_state == CLUSTER_THREADREC_REPLAY_BLOCKED)
		return CLUSTER_THREADREC_REAP_KEEP_TERMINAL;
	return CLUSTER_THREADREC_REAP_INVALID;
}

typedef struct ClusterThreadRecLaunchEligibility {
	uint16 origin_thread;
	uint64 attempt_stamp;
	ClusterRecoveryDutyKey duty;
} ClusterThreadRecLaunchEligibility;

/* The registration payload is only a one-shot carrier.  Before the worker can
 * acquire any authority it must match the exact main argument and the live
 * REPLAYING slot/stamp, and it must still carry a valid full duty identity. */
static inline bool
cluster_thread_recovery_worker_start_valid(const ClusterThreadRecLaunchEligibility *eligibility,
										   uint16 main_thread, bool slot_read,
										   ClusterThreadRecReplayState slot_state,
										   uint64 slot_stamp)
{
	return eligibility != NULL && main_thread >= XLP_THREAD_ID_FIRST_REAL
		   && main_thread <= CLUSTER_WAL_THREAD_MAX && eligibility->origin_thread == main_thread
		   && eligibility->attempt_stamp != 0 && slot_read
		   && slot_state == CLUSTER_THREADREC_REPLAY_REPLAYING
		   && slot_stamp == eligibility->attempt_stamp
		   && cluster_recovery_duty_key_valid_v1(&eligibility->duty)
		   && eligibility->duty.origin_thread_id == main_thread;
}

extern bool
cluster_reconfig_thread_recovery_eligibility_consume(uint16 origin_thread,
													 ClusterThreadRecLaunchEligibility *out);
extern void cluster_thread_recovery_lmon_tick(void);
extern void cluster_thread_recovery_lmon_shutdown(void);

/* GUC storage (defined in cluster_guc.c). */
extern bool cluster_online_thread_recovery;
extern int cluster_thread_recovery_on_unrecoverable;

/*
 * D7 capability gate (spec-4.11 §D7).  cluster_thread_recovery_capability_gate
 * raises FEATURE_NOT_SUPPORTED (errcode 0A000, mirror spec-4.5a's 53RA3 backend
 * gate) for a hard-unsupported scope (NO_SHARED_BACKEND, per
 * scope_is_unsupported) and is a no-op for APPLICABLE / DISABLED / SINGLE_NODE.
 * It does NOT run in the live reconfig FSM (which stays a no-op fall-back to cold
 * restart for any non-applicable scope -- no crash, no regression); it is the
 * explicit capability surface a probe / test consults.  cluster_thread_recovery_
 * current_scope resolves the live-runtime scope (the GUC + has_peers + shared
 * backend + survivor count) for that probe.
 */
extern ClusterThreadRecScope cluster_thread_recovery_current_scope(void);
extern void cluster_thread_recovery_capability_gate(ClusterThreadRecScope scope);

/*
 * D5 observability counters (spec-4.11 §D5).  Cumulative online thread-recovery
 * outcomes, kept in the "pgrac recovery plan" shmem region (region-level atomics,
 * not new region -- Q3) and surfaced by the recovery dump category.  _count_done
 * records a successful DONE (bumps threads_recovered + advances the
 * recovered_through high-watermark); _count_blocked records a fail-closed BLOCKED
 * (53RA4).  Both are bumped from replay_one_window's terminal point, so the
 * counters scope to a DRIVE-REACHED outcome: an early input-validation / window-
 * derivation reject in replay_one(_window) returns BLOCKED without a drive and is
 * NOT counted here (it never published authority either -- still 8.A-safe).  The
 * getters are L110-safe: with no shmem attached they return 0 /
 * InvalidXLogRecPtr.  _state_name returns an aggregate over the per-thread slots
 * ("replaying" / "blocked" / "done" / "idle", or "-" when no shmem is attached).
 */
extern void cluster_thread_recovery_count_done(XLogRecPtr recovered_through);
extern void cluster_thread_recovery_count_blocked(void);
extern uint64 cluster_thread_recovery_get_threads_recovered(void);
extern uint64 cluster_thread_recovery_get_replay_failclosed(void);
extern XLogRecPtr cluster_thread_recovery_get_recovered_through(void);
extern const char *cluster_thread_recovery_state_name(void);

/*
 * Visibility context for the combined replay pass (spec-4.11 3b-2).  3a replayed
 * DATA only; 3b-2 weaves the foreign-outcome visibility pass into the SAME
 * per-record loop so one completeness gate covers both: after a record's data
 * block refs apply cleanly, an RM_XACT / RM_CLOG / RM_MULTIXACT / RM_COMMIT_TS
 * record is diverted to cluster_remote_xact_apply(origin_node, .., online=true)
 * -- the online analog of the cold merge loop's XACT/CLOG divert
 * (xlogrecovery.c).  Without it, a survivor recovers the dead thread's pages but
 * not its commit/abort outcomes -> false-visible (8.A).  do_visibility=false
 * reproduces the 3a data-only stream exactly (the 3a-local / test callers).
 */
typedef struct ClusterThreadVisCtx {
	bool do_visibility; /* false = 3a data-only; true = data + visibility pass */
	int origin_node;	/* per-origin outcome store key = dead thread id - 1 */
} ClusterThreadVisCtx;

/*
 * Touched-relation collector for the durability barrier (spec-4.11 3b-2,
 * amend 2).  The engine's smgrwrite is a WRITE-BACK (cluster_fs is a bare pwrite
 * with no inline fsync and does NOT register a checkpointer sync request), so a
 * live survivor's checkpoint never learns of these dead-origin page writes.
 * The orchestrator MUST therefore smgrimmedsync every relation the engine wrote
 * BEFORE publishing any 3-way authority, or a published "recovered" authority
 * could outlive un-fsync'd pages -> serve a stale page after a crash (8.A).  The
 * engine appends each APPLIED (RelFileLocator, ForkNumber) here (deduplicated);
 * the orchestrator syncs them all on DONE.  NULL = do not collect (data-only /
 * test callers that publish nothing).
 */
typedef struct ClusterThreadTouchedRel {
	RelFileLocator rlocator;
	ForkNumber forknum;
} ClusterThreadTouchedRel;

typedef struct ClusterThreadTouchedRels {
	ClusterThreadTouchedRel *items;
	int n;
	int cap;
	MemoryContext mcxt; /* context the items array grows in (set by the caller) */
	/* D-SIDE-08 production caller (implementation): the dead origin this
	 * touched set was recovered for.  Set by the orchestrator from the
	 * real dead_tid so the retention proof's failed_origin_thread is a
	 * true identity, not a placeholder.  0 = unset (fail-closed). */
	uint16 origin_thread_id;
} ClusterThreadTouchedRels;

/*
 * R13 error-demotion boundary (spec-4.11 3b-1, amend 1).  The online driver
 * runs the replay under PG_TRY/PG_CATCH and demotes a catchable ERROR to a
 * result-returning BLOCKED so the recovery-apply worker survives.  But a
 * FATAL/PANIC must NEVER be swallowed -- it is re-thrown, so a survivor crash
 * the cold component intended can never masquerade as "recovery blocked".  PURE
 * so the boundary is unit-pinned (the .c PG_CATCH copies the error data and
 * calls this on edata->elevel).
 */
static inline bool
cluster_thread_recovery_should_rethrow(int elevel)
{
	return elevel >= FATAL;
}

/*
 * cluster_thread_recovery_origin_for_tid -- map a dead WAL thread id to its
 * origin (cluster node id), or -1 (spec-4.11 §2.4).
 *
 *	spec-4.1 stamps thread_id = cluster.node_id + 1, so origin = dead_tid - 1.
 *	An id outside the real range [XLP_THREAD_ID_FIRST_REAL,
 *	CLUSTER_WAL_THREAD_MAX] (the legacy 0, a reserved/invalid sentinel, or an
 *	out-of-range value) names no origin and returns -1.  The callers
 *	(local_complete / gate_unfreeze) treat -1 as fail-closed -- a bad id keeps
 *	the resource frozen, NEVER aliases to a valid node 0.  PURE so the bound is
 *	unit-pinned (test_cluster_thread_orchestrator.c).
 */
static inline int
cluster_thread_recovery_origin_for_tid(uint16 dead_tid)
{
	if (dead_tid < XLP_THREAD_ID_FIRST_REAL || dead_tid > CLUSTER_WAL_THREAD_MAX)
		return -1;
	return (int)dead_tid - 1;
}

/*
 * cluster_thread_recovery_gate_decide -- the PURE unfreeze decision (spec-4.11
 * D3, 3b-3).  Given the resolved scope and two per-node bitmaps over the same
 * nwords words (LSB = node 0): dead = nodes that died this episode, materialized
 * = dead origins whose WAL data is recovered HERE.  Returns true == STAY FROZEN:
 * recovery is in scope AND some dead origin is NOT yet materialized
 * (dead & ~materialized has a set bit).  Out of scope -> false (no gating, so the
 * existing spec-4.6/4.7 path is unchanged).  fail-closed: a dead bit with no
 * matching materialized bit counts as not-complete -> frozen; a NULL/empty input
 * returns false (nothing to gate).  PURE so the corruption-critical decision is
 * unit-pinned; the .c wrapper resolves the scope (decide_scope) and builds the
 * materialized bitmap (local_complete per dead origin) from the live runtime.
 */
static inline bool
cluster_thread_recovery_gate_decide(ClusterThreadRecScope scope, const uint64 *dead,
									const uint64 *materialized, int nwords)
{
	int w;

	if (scope != CLUSTER_THREADREC_SCOPE_APPLICABLE)
		return false;
	if (dead == NULL || materialized == NULL || nwords <= 0)
		return false;

	for (w = 0; w < nwords; w++) {
		if ((dead[w] & ~materialized[w]) != 0)
			return true; /* a dead origin not yet materialized -> stay frozen */
	}
	return false; /* every dead origin materialized -> ready to unfreeze */
}

/*
 * cluster_thread_recovery_replay_epoch_aborts -- the L235 episode-epoch
 * staleness guard (spec-4.11 3b-4b).  The per-thread replay slot stamps the GRD
 * recovery_episode_epoch it was launched under; the executor worker re-reads the
 * LIVE episode and ABORTS (returns true -- keep the dead thread frozen, publish
 * nothing) when the live episode no longer matches the stamp.  Returns true ==
 * ABORT.  PURE so the boundary is unit-pinned: any inequality is suspect (a
 * later live episode means a newer reconfig has superseded this worker; an
 * unstamped slot epoch 0 was never launched for the live episode), and a stale
 * worker must NEVER publish authority into a different episode (8.A).
 */
static inline bool
cluster_thread_recovery_replay_epoch_aborts(uint64 slot_epoch, uint64 current_epoch)
{
	return slot_epoch != current_epoch;
}

/*
 * cluster_thread_recovery_worker_terminal_state -- map an executor worker's
 * replay_one verdict to the terminal replay-slot state (spec-4.11 3b-4b Part 2).
 * PURE so the fail-closed direction is unit-pinned: only DONE and verified
 * semantic BLOCKED are terminal.  DEFERRED and NOT_APPLICABLE leave the output
 * untouched and return false, so result value 3 can never be cast/stored as the
 * numerically-equal replay-slot BLOCKED state.
 */
static inline bool
cluster_thread_recovery_worker_terminal_state(ClusterThreadRecResult res,
											  ClusterThreadRecReplayState *state)
{
	if (state == NULL)
		return false;
	if (res == CLUSTER_THREADREC_DONE) {
		*state = CLUSTER_THREADREC_REPLAY_DONE;
		return true;
	}
	if (res == CLUSTER_THREADREC_BLOCKED) {
		*state = CLUSTER_THREADREC_REPLAY_BLOCKED;
		return true;
	}
	return false;
}

/*
 * Online-replay ONE dead thread's WAL data through to shared storage
 * within the reconfig freeze window (spec-4.11 D2).  Implemented in a
 * later increment (the Q10-B apply matrix); declared here for the
 * reconfig FSM driver call site.
 */
extern ClusterThreadRecResult
cluster_thread_recovery_replay_one(uint16 dead_tid, uint64 episode_epoch,
								   const ClusterThreadRecoveryAuthorityV1 *authority);

/*
 * Unfreeze precondition (spec-4.11 D3, 3b-3): has dead_tid been fully
 * online-recovered up to required_lsn from THIS node's local materialization
 * authority (cluster_merged_instance_is_materialized / _recovered_through,
 * Q4 3-way authority -- NOT the cluster-wide registry, R11)?  required_lsn ==
 * InvalidXLogRecPtr asks the node-level question (materialized at all); a real
 * required_lsn additionally demands recovered_through >= it.  fail-closed: a
 * bad thread id or any unmet condition returns false (keep frozen).
 */
extern bool cluster_thread_recovery_local_complete(uint16 dead_tid, XLogRecPtr required_lsn);

/*
 * Reconfig-FSM unfreeze gate (spec-4.11 D3, 3b-3).  Given the episode's
 * dead-node bitmap (GRD recovery_dead_bitmap words, LSB = node 0), returns
 * true when the survivor must STAY frozen: online thread recovery is in scope
 * (cluster.online_thread_recovery on + a shared data backend) AND at
 * least one dead origin's WAL data is not yet materialized here.  Returns false
 * when recovery is out of scope (GUC off by default / no shared backend) -- no
 * gating, so the existing spec-4.6/4.7 unfreeze path is
 * unchanged (no regression) -- or when every dead origin is complete (ready to
 * unfreeze).  fail-closed: NULL/empty bitmap returns false (nothing to gate);
 * a bad dead id maps to no origin and is treated as not-complete (frozen).
 */
extern bool cluster_thread_recovery_gate_unfreeze(const uint64 *dead_bitmap, int nwords);

/*
 * Per-thread online replay-state helpers (spec-4.11 3b-4b).  Thin wrappers over
 * the recovery-plan shmem slot (cluster_thread_recovery_replay_slot): LMON
 * stamps the attempt and marks REPLAYING; the per-attempt
 * executor worker writes the terminal DONE/BLOCKED and reads the episode for the
 * L235 staleness guard.  All return false (and leave outputs untouched) when no
 * slot is available -- no shmem attached, or a bad thread id.  This is
 * OBSERVABILITY + episode coordination ONLY: the authoritative reader gate reads
 * the node-local merged.authority, NOT these slots (§2.4 Q4 3-way authority).
 *
 *	mark_replaying stamps episode_epoch BEFORE state, so a worker that observes
 *	REPLAYING also observes the epoch it was launched under (the L235 guard);
 *	read pairs the barrier on the way out.  Every later state change uses the
 *	exact stamp plus REPLAYING CAS; no unconditional production writer remains.
 */
extern bool cluster_thread_recovery_replay_mark_replaying(uint16 dead_tid, uint64 episode_epoch);
extern ClusterThreadReplayMatchResult
cluster_thread_recovery_replay_transition_if_match(uint16 dead_tid, uint64 attempt_stamp,
												   ClusterThreadRecReplayState expected,
												   ClusterThreadRecReplayState target);
extern bool cluster_thread_recovery_replay_read(uint16 dead_tid,
												ClusterThreadRecReplayState *state_out,
												uint64 *epoch_out);

/*
 * Online thread-recovery executor (spec-4.11 3b-4b Part 2).
 *
 * The private executor reads the per-thread replay slot (the LMON launch marked
 * it REPLAYING and stamped the exact attempt), validates the full bgw_extra duty,
 * and enforces the L235 episode-staleness guard.  RF-ROOT P3 then fails closed at
 * the P4 boundary: without typed NeedSet/AdmissionSet evidence it performs no
 * GES acquire, replay, or authority publication and returns DEFERRED.
 *
 * cluster_thread_recovery_worker_main is the dynamic-bgworker entry point the
 * permanent LMON tick registers (bgw_function_name); main_arg carries the dead
 * thread id.  A thin wrapper installs authority cleanup, unblocks signals,
 * runs, and logs.  It never writes IDLE or synthesizes BLOCKED on exit.
 */
extern void cluster_thread_recovery_worker_main(Datum main_arg);

/*
 * RMW replay engine (spec-4.11 D1 contract).  Read each record of a
 * positioned WAL reader, and for every block reference to a genuinely shared
 * user-relation page: read the LIVE page from shared storage, apply the record
 * (LSN-gated, idempotent), and write the page back -- bypassing the buffer pool
 * (the dead thread's pages are fenced for the freeze window, so there is no
 * concurrent access = the coherence precondition).
 *
 * cluster_thread_recovery_replay_stream is the SOURCE-AGNOSTIC core: the caller
 * owns the positioned reader (amend 4).  3a builds a local-WAL reader; 3b will
 * build the foreign dead-thread reader and call the same core.  It does NOT
 * publish authority, start a worker, or unfreeze -- that is 3b.
 *
 * PRECONDITION (amend 3): scan_upper MUST be a validated-complete AND durable
 * dead-thread WAL boundary; the engine does NOT flush WAL.  Reaching clean
 * end-of-WAL short of scan_upper means the WAL is incomplete -> BLOCKED (8.A).
 *
 * Returns DONE (recovered_through reached scan_upper) or BLOCKED (fail-closed:
 * read error, off-matrix/unusable record, dropped relation, extension, or an
 * incomplete window).  *stats is always written (NULL allowed).
 */
extern ClusterThreadRecResult
cluster_thread_recovery_replay_stream(struct XLogReaderState *reader, XLogRecPtr scan_upper,
									  ClusterThreadReplayStats *stats);

/*
 * cluster_thread_recovery_replay_stream_ex (spec-4.11 3b-2) -- the same core,
 * extended with the COMBINED pass: when vis->do_visibility, each foreign
 * XACT/CLOG/MULTIXACT/COMMIT_TS record is diverted to the per-origin outcome
 * store (online), so one completeness gate covers data AND visibility; and when
 * touched != NULL, every APPLIED (rel, fork) is recorded for the orchestrator's
 * durability barrier.  vis == NULL / touched == NULL reproduce the 3a data-only
 * behaviour exactly (cluster_thread_recovery_replay_stream is the vis=NULL,
 * touched=NULL wrapper).  An online visibility ERROR propagates out (the
 * caller's R13 harness demotes it -> BLOCKED).
 */
extern ClusterThreadRecResult cluster_thread_recovery_replay_stream_ex(
	struct XLogReaderState *reader, XLogRecPtr scan_upper, const ClusterThreadVisCtx *vis,
	ClusterThreadTouchedRels *touched, ClusterThreadReplayStats *stats);

/*
 * Durability-barrier helpers for the touched-rel collector (spec-4.11 3b-2).
 * _sync_all smgrimmedsyncs every collected (rel, fork) -- the orchestrator calls
 * it on DONE, before publishing authority.  _free releases the items array.
 */
extern void cluster_thread_recovery_touched_sync_all(const ClusterThreadTouchedRels *touched);
extern void cluster_thread_recovery_touched_free(ClusterThreadTouchedRels *touched);

/*
 * 3a-LOCAL / TEST convenience: build a local-WAL reader over [scan_lower,
 * scan_upper], position it, and drive cluster_thread_recovery_replay_stream.
 * LOCAL source only (single-machine test simulates a foreign thread with local
 * WAL); 3b adds the foreign-source entry calling the same core.
 */
extern ClusterThreadRecResult cluster_thread_recovery_replay_data(XLogRecPtr scan_lower,
																  XLogRecPtr scan_upper,
																  ClusterThreadReplayStats *stats);

/*
 * DATA DRIVER (spec-4.11 D1 contract).  Turn a dead thread id into a
 * driven replay: build a reader over <cluster.wal_threads_dir>/thread_<tid> and
 * drive cluster_thread_recovery_replay_stream under an R13 error-demotion
 * harness (a catchable ERROR -> BLOCKED + worker survives; a FATAL/PANIC is
 * re-thrown, never swallowed).
 *
 * DATA ONLY (3b-1): publishes NO authority, does NO visibility pass, issues NO
 * durability barrier, and has NO live FSM caller -- only the TEST SRF and the
 * future replay_one orchestrator (3b-2).  A data-only DONE is therefore consumed
 * by nobody and the thread stays frozen (8.A).
 *
 * scan_lower/scan_upper are the CALLER'S validated-durable-boundary contract
 * (amend 3); this driver only checks basic legality.  Any bad source / window
 * (dead_tid out of range, missing per-thread WAL dir, unpositionable reader,
 * in-window read error) is BLOCKED, never a silent success (amend 4).
 */
extern ClusterThreadRecResult cluster_thread_recovery_drive_data(uint16 dead_tid,
																 XLogRecPtr scan_lower,
																 XLogRecPtr scan_upper,
																 ClusterThreadReplayStats *stats);

/*
 * GENERAL R13-guarded driver (spec-4.11 3b-2).  Builds the dead thread's reader
 * and drives cluster_thread_recovery_replay_stream_ex under the same R13
 * harness as drive_data, but with an optional visibility pass + touched-rel
 * collector.  cluster_thread_recovery_drive_data is the vis-off / no-collector
 * wrapper (3b-1).  vis/touched may be NULL.  Same fail-closed + FATAL-rethrow
 * contract as drive_data.
 */
extern ClusterThreadRecResult cluster_thread_recovery_drive(uint16 dead_tid, XLogRecPtr scan_lower,
															XLogRecPtr scan_upper,
															const ClusterThreadVisCtx *vis,
															ClusterThreadTouchedRels *touched,
															ClusterThreadReplayStats *stats);

/*
 * VALIDATED-END boundary pass (spec-4.11 D4, 3b-4a).  Decode-only over the dead
 * thread's per-thread WAL from scan_lower; on DONE *out_valid_end is the EndRecPtr
 * of the last COMPLETE record -- the validated torn-tail boundary the replay must
 * reach.  validated_min (the registry's durable highest_lsn, a safe lower bound)
 * fail-closes a decode that stops below it (mid-stream corruption, not a torn
 * tail; 8.A).  R13: a catchable ERROR -> BLOCKED, a FATAL/PANIC is re-thrown.
 * This replaces replay_one's basic (observational highest_lsn) upper bound with a
 * validated one; replay_one_window (the explicit-window TEST path) is unchanged.
 */
extern ClusterThreadRecResult cluster_thread_recovery_validated_end(uint16 dead_tid,
																	XLogRecPtr scan_lower,
																	XLogRecPtr validated_min,
																	XLogRecPtr *out_valid_end);

/*
 * ORCHESTRATOR core (spec-4.11 3b-2), window-EXPLICIT.  Online-recover ONE dead
 * thread over [scan_lower, scan_upper]: build one immutable PAGE+SIDE fabric,
 * preflight every target, durably install/post-read PAGE, then apply protected
 * SIDE.  Only after a durable-fence and fresh ROOT recheck does it publish the
 * node-local reader authority; on BLOCKED it publishes NOTHING (partial-apply
 * = "never recovered", 8.A) and applies the on_unrecoverable policy.  This is
 * what the public replay_one calls after deriving the window; it is also the
 * TEST entry (the SRF drives it with an explicit, deterministic window on one
 * machine).
 * Real validated-boundary derivation for replay_one(dead_tid, epoch) is D4
 * (3b-4); 3b-2 derives only a basic window.
 */
extern ClusterThreadRecResult cluster_thread_recovery_replay_one_window(
	uint16 dead_tid, XLogRecPtr scan_lower, XLogRecPtr scan_upper, uint64 episode_epoch,
	const ClusterThreadRecoveryAuthorityV1 *authority, ClusterThreadReplayStats *stats);

/*
 * PGRAC: spec-6.12h D-h3b -- per-thread WAL reader factory, exported for the
 * PI-recovery differential SRF.  The same source the data driver reads: a
 * reader over <cluster.wal_threads_dir>/thread_<tid>.  NULL on any
 * fail-closed condition (bad tid / unset or missing dir / OOM); on success
 * the caller owns both handles and must release them through
 * cluster_thread_wal_reader_free on every exit.  The reader is NOT
 * positioned -- the caller runs XLogFindNextRecord.
 */
extern struct XLogReaderState *cluster_thread_wal_reader_make(uint16 tid, void **priv_out);
extern void cluster_thread_wal_reader_free(struct XLogReaderState *reader, void *priv);

#endif /* !FRONTEND */

#endif /* CLUSTER_THREAD_RECOVERY_H */
