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
#include "storage/block.h"
#include "cluster/cluster_wal_thread.h"

struct XLogReaderState;

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
 * increment 3a).  PURE, so it is the unit-testable authority for the
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
 * Streaming-replay outcome counters (spec-4.11 D1 increment 3a, observability).
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
	uint64 blocks_applied;		  /* APPLIED -> smgrwrite (write-back; NOT durable) */
	uint64 blocks_gated;		  /* DONE: LSN-gate idempotent skip */
	uint64 blocks_out_of_scope;	  /* OUT_OF_SCOPE: per-node block refs skipped */
	XLogRecPtr recovered_through; /* EndRecPtr of last fully-processed record */
} ClusterThreadReplayStats;

#ifndef FRONTEND

#include "utils/elog.h"				/* elevel constants for the R13 rethrow boundary */
#include "storage/relfilelocator.h" /* RelFileLocator for the touched-rel collector */

/* GUC storage (defined in cluster_guc.c). */
extern bool cluster_online_thread_recovery;
extern int cluster_thread_recovery_on_unrecoverable;

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

/*
 * RMW replay engine (spec-4.11 D1 increment 3a).  Read each record of a
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
 * DATA DRIVER (spec-4.11 D1 increment 3b-1).  Turn a dead thread id into a
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
 * ORCHESTRATOR core (spec-4.11 3b-2), window-EXPLICIT.  Online-recover ONE dead
 * thread over [scan_lower, scan_upper]: drive the combined data+visibility pass
 * under the R13 harness inside an episode-fenced online-writer scope; on DONE,
 * issue the durability barrier (immedsync touched rels + flush the outcome
 * store) and publish the 3-way authority (registry skip-bound + node-local
 * reader authority); on BLOCKED, publish NOTHING (partial-apply = "never
 * recovered", 8.A) and apply the on_unrecoverable policy.  This is what the
 * public replay_one calls after deriving the window; it is also the TEST entry
 * (the SRF drives it with an explicit, deterministic window on one machine).
 * Real validated-boundary derivation for replay_one(dead_tid, epoch) is D4
 * (3b-4); 3b-2 derives only a basic window.
 */
extern ClusterThreadRecResult
cluster_thread_recovery_replay_one_window(uint16 dead_tid, XLogRecPtr scan_lower,
										  XLogRecPtr scan_upper, uint64 episode_epoch,
										  ClusterThreadReplayStats *stats);

#endif /* !FRONTEND */

#endif /* CLUSTER_THREAD_RECOVERY_H */
