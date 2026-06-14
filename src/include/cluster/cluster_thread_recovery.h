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
 * 3a-LOCAL / TEST convenience: build a local-WAL reader over [scan_lower,
 * scan_upper], position it, and drive cluster_thread_recovery_replay_stream.
 * LOCAL source only (single-machine test simulates a foreign thread with local
 * WAL); 3b adds the foreign-source entry calling the same core.
 */
extern ClusterThreadRecResult cluster_thread_recovery_replay_data(XLogRecPtr scan_lower,
																  XLogRecPtr scan_upper,
																  ClusterThreadReplayStats *stats);

#endif /* !FRONTEND */

#endif /* CLUSTER_THREAD_RECOVERY_H */
