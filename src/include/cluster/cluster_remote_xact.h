/*-------------------------------------------------------------------------
 *
 * cluster_remote_xact.h
 *	  Per-origin materialized transaction outcomes (spec-4.5a G5, D10a).
 *
 *	  pg_xact_remote/ is an origin-partitioned SLRU recording, for every
 *	  foreign xid whose outcome records this node replayed during k-way
 *	  merged recovery, the REAL outcome B durably logged: COMMITTED (with
 *	  the commit record's SCN, spec-1.18) or ABORTED.  A foreign PREPARE is
 *	  not an outcome and deliberately leaves no entry, so readers see INDOUBT
 *	  until a matching COMMIT/ABORT PREPARED is replayed.  It exists because
 *	  neither of the two local stores may answer for a remote xid:
 *
 *	    - the local pg_xact is indexed by raw 32-bit xid and would alias
 *	      across instances (AD-012 例外 9) -- merged replay therefore
 *	      DIVERTS foreign XACT/CLOG records here instead of applying them
 *	      into pg_xact (D10b);
 *	    - the materialized durable TT slot is stamped at PRE-commit
 *	      (spec-3.11 C1b) and alone cannot prove the xact committed.
 *
 *	  The resolver (cluster_remote_commit_outcome) returns COMMITTED /
 *	  ABORTED / INDOUBT; INDOUBT (no materialized outcome record) is the
 *	  B-stamped-TT-then-crashed window and MUST fail closed at the caller
 *	  (53R9G), never report visible (规则 8.A).
 *
 *	  Keyed {origin_node_id, xid} with the SLRU page space partitioned by
 *	  origin (page = origin << 23 | xid-page).  The xid-wraparound epoch
 *	  dimension rides the retention scope: entries are written only by
 *	  merged replay of a SINGLE cold-crash WAL window, whose xids span far
 *	  less than one epoch; a future epoch-spanning store extends the entry
 *	  (8 spare bits + entry version live in the page header word).
 *
 *	  Durability: pages are flushed once at merged-replay completion
 *	  (cluster_remote_xact_flush); a crash mid-merge simply reruns the
 *	  merge from WAL (the SLRU is rebuilt, never the authority for redo).
 *	  Backends read the flushed pages after recovery finishes.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_remote_xact.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.5a-shared-storage-data-backend.md (FROZEN v1.0, D10)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_REMOTE_XACT_H
#define CLUSTER_REMOTE_XACT_H

#include "access/transam.h" /* TransactionId */
#include "access/xlogdefs.h"
#include "cluster/cluster_scn.h"
#include "storage/block.h" /* BLCKSZ */
#include "utils/elog.h"	   /* ERROR / FATAL for the R13 online-blocked elevel */

struct XLogReaderState; /* forward; avoid xlogreader.h in this header */

/* On-disk entry width + page partitioning (pure; unit-testable). */
#define CLUSTER_REMOTE_XACT_ENTRY_BYTES 16
#define CLUSTER_REMOTE_XACT_ENTRIES_PER_PAGE (BLCKSZ / CLUSTER_REMOTE_XACT_ENTRY_BYTES)
#define CLUSTER_REMOTE_XACT_ORIGIN_PAGE_SHIFT 23

/*
 * cluster_remote_xact_pageno / _entryno -- {origin, xid} -> SLRU page +
 * in-page slot.  Page space is partitioned by origin (origin << 23), so two
 * distinct origins NEVER share a page (F2): a wrapped same-valued xid of a
 * different origin lands in a different partition, not the same entry.
 */
static inline int
cluster_remote_xact_pageno(int origin_node, TransactionId xid)
{
	/* Unsigned shift: origin_node is validated non-negative by every
	 * caller, but a signed LHS is technically UB (cppcheck portability). */
	return (int)(((uint32)origin_node << CLUSTER_REMOTE_XACT_ORIGIN_PAGE_SHIFT)
				 | (uint32)(xid / CLUSTER_REMOTE_XACT_ENTRIES_PER_PAGE));
}

static inline int
cluster_remote_xact_entryno(TransactionId xid)
{
	return (int)(xid % CLUSTER_REMOTE_XACT_ENTRIES_PER_PAGE);
}

/*
 * cluster_remote_xact_commit_blocked -- P1-1 pure predicate: a foreign commit
 * record may materialize ONLY as a pure outcome.  Any cross-instance side
 * effect (relfile drop / invalidation / stats drop / subxacts / 2PC / AE
 * locks) makes it unmergeable -> the caller fails closed (53RA3).  The paired
 * prepared-commit predicate admits the expected XACT_XINFO_HAS_TWOPHASE and
 * XACT_XINFO_HAS_AE_LOCKS bits: prepared-xact lock release is not durable state
 * to materialize after PITR; relfile/inval/stats/subxact side effects still
 * fail closed.
 */
static inline bool
cluster_remote_xact_commit_common_blocked(int nrels, int nmsgs, int nstats, int nsubxacts,
										  uint32 xinfo, uint32 twophase_bit, uint32 ae_locks_bit,
										  bool allow_twophase)
{
	uint32 disallowed_xinfo = ae_locks_bit;

	if (!allow_twophase)
		disallowed_xinfo |= twophase_bit;
	return nrels > 0 || nmsgs > 0 || nstats > 0 || nsubxacts > 0 || (xinfo & disallowed_xinfo) != 0;
}

static inline bool
cluster_remote_xact_commit_blocked(int nrels, int nmsgs, int nstats, int nsubxacts, uint32 xinfo,
								   uint32 twophase_bit, uint32 ae_locks_bit)
{
	return cluster_remote_xact_commit_common_blocked(nrels, nmsgs, nstats, nsubxacts, xinfo,
													 twophase_bit, ae_locks_bit, false);
}

static inline bool
cluster_remote_xact_commit_prepared_blocked(int nrels, int nmsgs, int nstats, int nsubxacts,
											uint32 xinfo, uint32 twophase_bit, uint32 ae_locks_bit)
{
	(void)xinfo;
	(void)twophase_bit;
	(void)ae_locks_bit;
	return nrels > 0 || nmsgs > 0 || nstats > 0 || nsubxacts > 0;
}

/*
 * R13 (spec-4.11 3b-2): the elevel cluster_remote_xact_apply raises when a
 * foreign record cannot be materialized (multixact/commit_ts, an unsupported
 * commit/abort side effect, a missing SCN, or a 2PC/assignment record).
 *
 *	COLD merged replay (startup, online=false) -> FATAL: a clean re-merge on the
 *	next start, exactly as before this spec.
 *	ONLINE thread recovery (online=true) -> ERROR: catchable, so the 3b-1 R13
 *	harness (cluster_thread_recovery_drive*) demotes it to a result-returning
 *	BLOCKED and the recovery-apply worker SURVIVES + keep_frozen, instead of
 *	crashing the live survivor.
 *
 * PURE so the boundary is unit-pinned.  It is the producer side of the R13
 * contract whose consumer side is cluster_thread_recovery_should_rethrow:
 * ERROR < FATAL, so an online block is always demotable and a cold FATAL never
 * is (a survivor crash the cold path intended can never masquerade as BLOCKED).
 */
static inline int
cluster_remote_xact_blocked_elevel(bool online)
{
	return online ? ERROR : FATAL;
}

/*
 * R14 (spec-4.11 3b-2): may THIS process write the per-origin materialization
 * store right now?  Historically the writer was the startup process ONLY
 * (single-threaded cold merged replay; see cluster_remote_xact_set's assert).
 * Online thread recovery adds one more legitimate writer -- the recovery-apply
 * bgworker -- but ONLY inside an episode-fenced online-writer scope
 * (cluster_remote_xact_online_writer_push/pop).  Outside startup AND outside
 * that scope, a writer is in an illegal context: the assert must fail closed.
 * PURE so the corruption-critical writer assert is unit-pinned.
 */
static inline bool
cluster_remote_xact_writer_allowed(bool is_startup, int online_writer_depth)
{
	return is_startup || online_writer_depth > 0;
}

/*
 * Resolver verdict.  INDOUBT is the fail-closed arm: a TT pre-commit
 * stamp without a materialized commit/abort record proves nothing.
 */
typedef enum ClusterRemoteXactOutcome {
	CLUSTER_REMOTE_XACT_INDOUBT = 0, /* no materialized outcome -> 53R9G */
	CLUSTER_REMOTE_XACT_COMMITTED,	 /* B's commit record seen; *scn set  */
	CLUSTER_REMOTE_XACT_ABORTED,	 /* B's abort record seen             */
} ClusterRemoteXactOutcome;

/* shmem request/init plumbing (cluster_shmem.c / ipci path). */
extern Size cluster_remote_xact_shmem_size(void);
extern void cluster_remote_xact_shmem_request(void);
extern void cluster_remote_xact_shmem_init(void);

/*
 * cluster_remote_xact_apply -- D10b divert target.  Called by merged
 *	replay INSTEAD of ApplyWalRecord for a foreign stream's RM_XACT /
 *	RM_CLOG record.  Parses the record; a pure outcome (commit/abort
 *	with no cross-instance side effects, or a proven COMMIT/ABORT PREPARED
 *	outcome) lands in pg_xact_remote; a foreign PREPARE only consumes the WAL
 *	record and leaves the xid INDOUBT.  Any unsupported side effect (nrels /
 *	nmsgs / nstats / nsubxacts; foreign MULTIXACT and COMMIT_TS divert here
 *	too) is fail-closed 53RA3 (P1-1) -- never silently skipped.
 *
 *	online (spec-4.11 3b-2, R13): false = cold merged replay (startup) -> an
 *	unmaterializable record FATALs (re-merge next start).  true = online thread
 *	recovery -> it raises a catchable ERROR (cluster_remote_xact_blocked_elevel)
 *	so the R13 harness demotes it to BLOCKED and the survivor keeps running.
 */
extern void cluster_remote_xact_apply(int origin_node, struct XLogReaderState *record, bool online);

/* Flush dirty SLRU pages to pg_xact_remote/ (merged-replay completion). */
extern void cluster_remote_xact_flush(void);

/*
 * Online-writer scope (spec-4.11 3b-2, R14).  The online thread-recovery
 * orchestrator brackets its visibility apply so cluster_remote_xact_set's
 * startup-only assert admits the episode-fenced recovery-apply bgworker writer.
 * Re-entrant (depth-counted); push/pop MUST balance.  _depth() is for
 * observability / unit tests.
 */
extern void cluster_remote_xact_online_writer_push(void);
extern void cluster_remote_xact_online_writer_pop(void);
extern int cluster_remote_xact_online_writer_depth(void);

/*
 * cluster_remote_commit_outcome -- D10c authority read.
 *	COMMITTED additionally returns the commit record's SCN via *commit_scn.
 *	Missing dir/segment/page or a zeroed entry -> INDOUBT (fail-closed).
 */
extern ClusterRemoteXactOutcome cluster_remote_commit_outcome_ex(int origin_node, TransactionId xid,
																 SCN *commit_scn, uint16 *out_wrap,
																 bool *out_wrap_valid);
extern ClusterRemoteXactOutcome cluster_remote_outcome_terminal_authorized(
	int origin_node, TransactionId xid, uint64 observed_epoch, uint64 current_epoch,
	bool retention_required, bool retention_proven, SCN *out_scn);
extern ClusterRemoteXactOutcome
cluster_remote_outcome_durable_checked(int origin_node, TransactionId xid, SCN *out_scn);
extern ClusterRemoteXactOutcome cluster_remote_commit_outcome(int origin_node, TransactionId xid,
															  SCN *commit_scn);

/* Observation counters (D11 dump). */
extern uint64 cluster_remote_xact_diverted_commit_count(void);
extern uint64 cluster_remote_xact_diverted_abort_count(void);
extern uint64 cluster_remote_xact_outcome_indoubt_count(void);

#endif /* CLUSTER_REMOTE_XACT_H */
