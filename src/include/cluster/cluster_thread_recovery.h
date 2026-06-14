/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery.h
 *	  pgrac online single-thread recovery -- a survivor online-replays a
 *	  dead WAL thread's data to shared storage within the reconfig freeze
 *	  window, instead of waiting for the dead node's cold restart
 *	  (spec-4.11, #84 Thread recovery).
 *
 *	  Scope (spec-4.11 §1, FROZEN v0.3): 2-node only (single survivor =
 *	  single replay owner = single local materialization authority);
 *	  >2-node multi-survivor authority is FEATURE_NOT_SUPPORTED and
 *	  forwarded.  The apply path is Q10-B (D0 verdict, Impl note v0.1): a
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
#include "cluster/cluster_wal_thread.h"

/*
 * Result of attempting to online-recover one dead thread (spec-4.11 §2.2).
 * NEVER DONE without a proven recovered_through up to the validated
 * complete-record boundary; any 8.A precondition failure is BLOCKED
 * (result-returning, not FATAL -- R13).
 */
typedef enum ClusterThreadRecResult {
	CLUSTER_THREADREC_DONE = 0,		  /* recovered_through_local published */
	CLUSTER_THREADREC_BLOCKED,		  /* 8.A fail-closed -> keep frozen (53RA4) */
	CLUSTER_THREADREC_NOT_APPLICABLE, /* single-node / no shared-fs / >2-node */
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
 *	live_node_count   nodes still alive after the death (2-node scope: a
 *	                  single survivor; >2 survivors -> not supported, Q9)
 */
typedef enum ClusterThreadRecScope {
	CLUSTER_THREADREC_SCOPE_APPLICABLE = 0,	   /* attempt online recovery     */
	CLUSTER_THREADREC_SCOPE_DISABLED,		   /* GUC off                      */
	CLUSTER_THREADREC_SCOPE_SINGLE_NODE,	   /* no peers -> PG-native crash  */
	CLUSTER_THREADREC_SCOPE_NO_SHARED_BACKEND, /* FEATURE_NOT_SUPPORTED        */
	CLUSTER_THREADREC_SCOPE_MULTI_SURVIVOR,	   /* >2-node, FEATURE_NOT_SUPPORTED (Q9) */
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
	/* 2-node scope: exactly one survivor performs the recovery (Q9). */
	if (live_survivor_count != 1)
		return CLUSTER_THREADREC_SCOPE_MULTI_SURVIVOR;
	return CLUSTER_THREADREC_SCOPE_APPLICABLE;
}

#ifndef FRONTEND

/* GUC storage (defined in cluster_guc.c). */
extern bool cluster_online_thread_recovery;
extern int cluster_thread_recovery_on_unrecoverable;

/*
 * Online-replay ONE dead thread's WAL data through to shared storage
 * within the reconfig freeze window (spec-4.11 D2).  Implemented in a
 * later increment (the Q10-B apply matrix); declared here for the
 * reconfig FSM driver call site.
 */
extern ClusterThreadRecResult cluster_thread_recovery_replay_one(uint16 dead_tid,
																 uint64 episode_epoch);

/*
 * Unfreeze precondition (spec-4.11 D3): has dead_tid been fully
 * online-recovered up to required_lsn from THIS node's local
 * materialization authority?  fail-closed on any doubt.  Implemented in a
 * later increment.
 */
extern bool cluster_thread_recovery_local_complete(uint16 dead_tid, XLogRecPtr required_lsn);

#endif /* !FRONTEND */

#endif /* CLUSTER_THREAD_RECOVERY_H */
