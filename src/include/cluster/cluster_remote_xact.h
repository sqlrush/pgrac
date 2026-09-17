/*-------------------------------------------------------------------------
 *
 * cluster_remote_xact.h
 *	  Per-origin materialized transaction outcomes (spec-4.5a G5, D10a).
 *
 *	  pg_xact_remote_v2/ is an origin-partitioned durable materialization for
 *	  failed-origin transaction recovery.  It records PREPARED as an in-doubt
 *	  database/GID binding and terminal COMMITTED/ABORTED projections.  It is
 *	  never transaction authority: consumers still require matching TT/undo
 *	  and terminal-redo proof.  A PREPARED entry therefore resolves only via
 *	  an exact matching COMMIT/ABORT PREPARED transition.  It exists because
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
 *	  origin (page = origin << 24 | xid-page).  The xid-wraparound epoch
 *	  dimension rides the retention scope: entries are written only by
 *	  merged replay of a SINGLE cold-crash WAL window, whose xids span far
 *	  less than one epoch; a future epoch-spanning store extends the entry
 *	  (8 spare bits + entry version live in the page header word).
 *
 *	  Durability: each accepted state transition writes and synchronously
 *	  fsyncs its exact SLRU segment before returning.  A crash mid-recovery
 *	  simply reruns the retained WAL and takes the idempotent transition; the
 *	  SLRU remains rebuildable materialization, never redo authority.
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
#define CLUSTER_REMOTE_XACT_ENTRY_BYTES 32
#define CLUSTER_REMOTE_XACT_ENTRIES_PER_PAGE (BLCKSZ / CLUSTER_REMOTE_XACT_ENTRY_BYTES)
#define CLUSTER_REMOTE_XACT_ORIGIN_PAGE_SHIFT 24

/*
 * cluster_remote_xact_pageno / _entryno -- {origin, xid} -> SLRU page +
 * in-page slot.  Page space is partitioned by origin (origin << 24), so two
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
 * cluster_remote_xact_terminal_blocked_shared_catalog -- spec-6.14 D9 pure
 * predicate: under cluster.shared_catalog the relfile-drop / invalidation /
 * stats side effects of a foreign TERMINAL record (COMMIT / COMMIT PREPARED /
 * ABORT / ABORT PREPARED) are executed for real against the shared tree, so
 * they no longer make the record unmergeable.  What still blocks:
 *
 *	- subxact outcomes (nsubxacts > 0): no per-subxact durable TT wrap proof
 *	  exists (the commit record's TT delta covers the top-level slot only), and
 *	  a materialized COMMITTED entry without a wrap proof reads INDOUBT through
 *	  the durable-checked path anyway -- materializing them would claim a
 *	  usefulness the reader cannot honor.  Fail closed instead.
 *	- any xinfo bit the calling arm declares malformed (2PC on a plain
 *	  COMMIT/ABORT record; the *_PREPARED arms pass 0 because the 2PC bit is
 *	  expected there).  AE-lock bits are consumed: standby lock release is
 *	  hot-standby machinery with no merged-recovery analog.
 */
static inline bool
cluster_remote_xact_terminal_blocked_shared_catalog(int nsubxacts, uint32 xinfo,
													uint32 disallowed_xinfo)
{
	return nsubxacts > 0 || (xinfo & disallowed_xinfo) != 0;
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

/*
 * RF-SIDE v2 materialized entry.  The store remains a projection over
 * terminal redo plus durable TT/undo, never transaction authority.  PREPARED
 * uses the payload as the complete canonical 224-bit binding emitted by
 * cluster_remote_xact_prepare_digest_v2; terminal entries use the fixed
 * offsets below for commit SCN/timestamp/wrap.
 */
#define CLUSTER_REMOTE_XACT_ENTRY_FORMAT_V2 UINT8_C(2)
#define CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES 28
#define CLUSTER_REMOTE_XACT_ENTRY_PAYLOAD_BYTES 28
#define CLUSTER_REMOTE_XACT_ENTRY_FLAG_WRAP_VALID UINT8_C(0x01)
#define CLUSTER_REMOTE_XACT_RESET_MAX_ENTRIES UINT32_C(32768)

typedef enum ClusterRemoteXactStoredStateV2 {
	CLUSTER_REMOTE_XACT_STORED_EMPTY = 0,
	CLUSTER_REMOTE_XACT_STORED_COMMITTED = 1,
	CLUSTER_REMOTE_XACT_STORED_ABORTED = 2,
	CLUSTER_REMOTE_XACT_STORED_PREPARED = 3
} ClusterRemoteXactStoredStateV2;

typedef struct ClusterRemoteXactEntryV2 {
	uint8 payload[CLUSTER_REMOTE_XACT_ENTRY_PAYLOAD_BYTES];
	uint8 status;
	uint8 format_version;
	uint8 flags;
	uint8 reserved_zero;
} ClusterRemoteXactEntryV2;

StaticAssertDecl(sizeof(ClusterRemoteXactEntryV2) == CLUSTER_REMOTE_XACT_ENTRY_BYTES,
				 "remote-xact v2 entry must be 32 bytes");

typedef struct ClusterRemoteXactEntryDecodedV2 {
	ClusterRemoteXactOutcome outcome;
	SCN commit_scn;
	int64 commit_timestamp;
	uint16 wrap;
	bool wrap_valid;
} ClusterRemoteXactEntryDecodedV2;

static inline bool
cluster_remote_xact_entry_encode_pending_v2(
	ClusterRemoteXactEntryV2 *entry, const uint8 digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES])
{
	if (entry == NULL || digest == NULL)
		return false;
	memset(entry, 0, sizeof(*entry));
	memcpy(entry->payload, digest, CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES);
	entry->status = CLUSTER_REMOTE_XACT_STORED_PREPARED;
	entry->format_version = CLUSTER_REMOTE_XACT_ENTRY_FORMAT_V2;
	return true;
}

static inline bool
cluster_remote_xact_entry_pending_matches_v2(
	const ClusterRemoteXactEntryV2 *entry,
	const uint8 digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES])
{
	if (entry == NULL || digest == NULL || entry->status != CLUSTER_REMOTE_XACT_STORED_PREPARED
		|| entry->format_version != CLUSTER_REMOTE_XACT_ENTRY_FORMAT_V2 || entry->flags != 0
		|| entry->reserved_zero != 0)
		return false;
	return memcmp(entry->payload, digest, CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES) == 0;
}

static inline bool
cluster_remote_xact_entry_encode_terminal_v2(ClusterRemoteXactEntryV2 *entry,
											 ClusterRemoteXactOutcome outcome, SCN commit_scn,
											 int64 commit_timestamp, bool wrap_valid, uint16 wrap)
{
	if (entry == NULL
		|| (outcome != CLUSTER_REMOTE_XACT_COMMITTED && outcome != CLUSTER_REMOTE_XACT_ABORTED))
		return false;
	if ((outcome == CLUSTER_REMOTE_XACT_COMMITTED
		 && (commit_scn == InvalidScn || commit_scn == 0 || commit_timestamp == 0
			 || (wrap_valid && wrap == 0)))
		|| (outcome == CLUSTER_REMOTE_XACT_ABORTED
			&& (commit_scn != InvalidScn || commit_timestamp != 0 || wrap_valid || wrap != 0)))
		return false;
	memset(entry, 0, sizeof(*entry));
	if (outcome == CLUSTER_REMOTE_XACT_COMMITTED) {
		memcpy(entry->payload, &commit_scn, sizeof(commit_scn));
		memcpy(entry->payload + 8, &commit_timestamp, sizeof(commit_timestamp));
		memcpy(entry->payload + 16, &wrap, sizeof(wrap));
		entry->status = CLUSTER_REMOTE_XACT_STORED_COMMITTED;
		if (wrap_valid)
			entry->flags = CLUSTER_REMOTE_XACT_ENTRY_FLAG_WRAP_VALID;
	} else
		entry->status = CLUSTER_REMOTE_XACT_STORED_ABORTED;
	entry->format_version = CLUSTER_REMOTE_XACT_ENTRY_FORMAT_V2;
	return true;
}

static inline bool
cluster_remote_xact_entry_decode_terminal_v2(const ClusterRemoteXactEntryV2 *entry,
											 ClusterRemoteXactEntryDecodedV2 *decoded)
{
	ClusterRemoteXactEntryDecodedV2 candidate;
	uint8 reserved = 0;
	int i;

	if (entry == NULL || decoded == NULL
		|| entry->format_version != CLUSTER_REMOTE_XACT_ENTRY_FORMAT_V2 || entry->reserved_zero != 0
		|| (entry->flags & ~CLUSTER_REMOTE_XACT_ENTRY_FLAG_WRAP_VALID) != 0
		|| (entry->status != CLUSTER_REMOTE_XACT_STORED_COMMITTED
			&& entry->status != CLUSTER_REMOTE_XACT_STORED_ABORTED))
		return false;
	for (i = 18; i < CLUSTER_REMOTE_XACT_ENTRY_PAYLOAD_BYTES; i++)
		reserved |= entry->payload[i];
	if (reserved != 0)
		return false;
	memset(&candidate, 0, sizeof(candidate));
	if (entry->status == CLUSTER_REMOTE_XACT_STORED_COMMITTED) {
		candidate.outcome = CLUSTER_REMOTE_XACT_COMMITTED;
		memcpy(&candidate.commit_scn, entry->payload, sizeof(candidate.commit_scn));
		memcpy(&candidate.commit_timestamp, entry->payload + 8, sizeof(candidate.commit_timestamp));
		memcpy(&candidate.wrap, entry->payload + 16, sizeof(candidate.wrap));
		candidate.wrap_valid = (entry->flags & CLUSTER_REMOTE_XACT_ENTRY_FLAG_WRAP_VALID) != 0;
		if (candidate.commit_scn == InvalidScn || candidate.commit_scn == 0
			|| candidate.commit_timestamp == 0 || (candidate.wrap_valid && candidate.wrap == 0)
			|| (!candidate.wrap_valid && candidate.wrap != 0))
			return false;
	} else {
		candidate.outcome = CLUSTER_REMOTE_XACT_ABORTED;
		if (entry->flags != 0 || entry->payload[0] != 0 || entry->payload[1] != 0
			|| entry->payload[2] != 0 || entry->payload[3] != 0 || entry->payload[4] != 0
			|| entry->payload[5] != 0 || entry->payload[6] != 0 || entry->payload[7] != 0
			|| entry->payload[8] != 0 || entry->payload[9] != 0 || entry->payload[10] != 0
			|| entry->payload[11] != 0 || entry->payload[12] != 0 || entry->payload[13] != 0
			|| entry->payload[14] != 0 || entry->payload[15] != 0 || entry->payload[16] != 0
			|| entry->payload[17] != 0)
			return false;
	}
	*decoded = candidate;
	return true;
}

typedef enum ClusterRemoteXactEntryTransitionV2 {
	CLUSTER_REMOTE_XACT_ENTRY_WRITE = 0,
	CLUSTER_REMOTE_XACT_ENTRY_NOOP = 1,
	CLUSTER_REMOTE_XACT_ENTRY_CONFLICT = 2,
	CLUSTER_REMOTE_XACT_ENTRY_INVALID = 3
} ClusterRemoteXactEntryTransitionV2;

typedef enum ClusterRemoteXactMutationV2 {
	CLUSTER_REMOTE_XACT_MUTATION_STORED = 0,
	CLUSTER_REMOTE_XACT_MUTATION_UNCHANGED = 1,
	CLUSTER_REMOTE_XACT_MUTATION_CONFLICT = 2,
	CLUSTER_REMOTE_XACT_MUTATION_INVALID = 3
} ClusterRemoteXactMutationV2;

static inline bool
cluster_remote_xact_entry_is_empty_v2(const ClusterRemoteXactEntryV2 *entry)
{
	const uint8 *bytes = (const uint8 *)entry;
	uint8 seen = 0;
	Size i;

	if (entry == NULL)
		return false;
	for (i = 0; i < sizeof(*entry); i++)
		seen |= bytes[i];
	return seen == 0;
}

static inline ClusterRemoteXactEntryTransitionV2
cluster_remote_xact_entry_prepare_transition_v2(
	const ClusterRemoteXactEntryV2 *current,
	const uint8 digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES], ClusterRemoteXactEntryV2 *next)
{
	if (current == NULL || digest == NULL || next == NULL)
		return CLUSTER_REMOTE_XACT_ENTRY_INVALID;
	if (cluster_remote_xact_entry_is_empty_v2(current)) {
		if (!cluster_remote_xact_entry_encode_pending_v2(next, digest))
			return CLUSTER_REMOTE_XACT_ENTRY_INVALID;
		return CLUSTER_REMOTE_XACT_ENTRY_WRITE;
	}
	if (cluster_remote_xact_entry_pending_matches_v2(current, digest)) {
		*next = *current;
		return CLUSTER_REMOTE_XACT_ENTRY_NOOP;
	}
	if (current->format_version != CLUSTER_REMOTE_XACT_ENTRY_FORMAT_V2
		|| current->reserved_zero != 0)
		return CLUSTER_REMOTE_XACT_ENTRY_INVALID;
	return CLUSTER_REMOTE_XACT_ENTRY_CONFLICT;
}

static inline ClusterRemoteXactEntryTransitionV2
cluster_remote_xact_entry_terminal_transition_v2(
	const ClusterRemoteXactEntryV2 *current, bool require_prepared,
	const uint8 expected_prepare_digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES],
	ClusterRemoteXactOutcome outcome, SCN commit_scn, int64 commit_timestamp, bool wrap_valid,
	uint16 wrap, ClusterRemoteXactEntryV2 *next)
{
	ClusterRemoteXactEntryV2 candidate;
	ClusterRemoteXactEntryDecodedV2 decoded;

	if (current == NULL || next == NULL || (require_prepared && expected_prepare_digest == NULL)
		|| !cluster_remote_xact_entry_encode_terminal_v2(&candidate, outcome, commit_scn,
														 commit_timestamp, wrap_valid, wrap))
		return CLUSTER_REMOTE_XACT_ENTRY_INVALID;
	if (cluster_remote_xact_entry_is_empty_v2(current)) {
		if (require_prepared)
			return CLUSTER_REMOTE_XACT_ENTRY_CONFLICT;
		*next = candidate;
		return CLUSTER_REMOTE_XACT_ENTRY_WRITE;
	}
	if (memcmp(current, &candidate, sizeof(candidate)) == 0
		&& cluster_remote_xact_entry_decode_terminal_v2(current, &decoded)) {
		*next = *current;
		return CLUSTER_REMOTE_XACT_ENTRY_NOOP;
	}
	if (require_prepared
		&& cluster_remote_xact_entry_pending_matches_v2(current, expected_prepare_digest)) {
		*next = candidate;
		return CLUSTER_REMOTE_XACT_ENTRY_WRITE;
	}
	if (current->format_version != CLUSTER_REMOTE_XACT_ENTRY_FORMAT_V2
		|| current->reserved_zero != 0)
		return CLUSTER_REMOTE_XACT_ENTRY_INVALID;
	return CLUSTER_REMOTE_XACT_ENTRY_CONFLICT;
}

static inline ClusterRemoteXactEntryTransitionV2
cluster_remote_xact_entry_reset_transition_v2(const ClusterRemoteXactEntryV2 *current,
											  ClusterRemoteXactEntryV2 *next)
{
	if (current == NULL || next == NULL)
		return CLUSTER_REMOTE_XACT_ENTRY_INVALID;
	memset(next, 0, sizeof(*next));
	/* Reset invalidates a projection scope.  Corrupt or old bytes are safe to
	 * erase because they are never canonical transaction truth. */
	return cluster_remote_xact_entry_is_empty_v2(current) ? CLUSTER_REMOTE_XACT_ENTRY_NOOP
														  : CLUSTER_REMOTE_XACT_ENTRY_WRITE;
}

static inline bool
cluster_remote_xact_reset_range_valid_v2(int origin_node, TransactionId first_xid, uint32 count)
{
	return origin_node >= 0 && origin_node < (1 << 7) && count > 0
		   && count <= CLUSTER_REMOTE_XACT_RESET_MAX_ENTRIES
		   && (uint64)first_xid + (uint64)count - 1 <= UINT32_MAX;
}

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

/*
 * RF-SIDE durable non-authoritative projection.  Every STORED return means
 * the exact SLRU segment and directory entry have been forced to stable
 * storage, but never that PREPARED, terminal, readiness or OPEN is granted.
 * Those decisions additionally require the database-scoped pending state,
 * exact identity, TT/undo and prepare/terminal redo.  A prepared terminal
 * must supply the digest frozen by the immutable SIDE plan;
 * missing/different projected state fails closed.
 */
extern bool
cluster_remote_xact_prepare_digest_v2(uint64 system_identifier, int origin_node, TransactionId xid,
									  Oid database, const char *gid,
									  uint8 digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES]);
extern ClusterRemoteXactMutationV2
cluster_remote_xact_store_prepared_v2(int origin_node, TransactionId xid,
									  const uint8 digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES]);
extern ClusterRemoteXactMutationV2 cluster_remote_xact_store_terminal_v2(
	int origin_node, TransactionId xid, bool require_prepared,
	const uint8 expected_prepare_digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES],
	ClusterRemoteXactOutcome outcome, SCN commit_scn, TimestampTz commit_timestamp, bool wrap_valid,
	uint16 wrap);
extern bool cluster_remote_xact_pending_matches_v2(
	int origin_node, TransactionId xid,
	const uint8 digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES]);
extern bool cluster_remote_xact_reset_range_v2(int origin_node, TransactionId first_xid,
											   uint32 count);
extern bool cluster_remote_xact_range_empty_v2(int origin_node, TransactionId first_xid,
											   uint32 count);
extern bool cluster_remote_xact_truncate_before_v2(int origin_node, TransactionId oldest_xid);

/* Flush any residual dirty SLRU pages (normal v2 mutations fsync inline). */
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
extern bool cluster_remote_commit_timestamp(int origin_node, TransactionId xid,
											TimestampTz *commit_timestamp);

/* Observation counters (D11 dump). */
extern uint64 cluster_remote_xact_diverted_commit_count(void);
extern uint64 cluster_remote_xact_diverted_abort_count(void);
/* spec-6.14 D9: foreign terminal-record side effects executed for real. */
extern uint64 cluster_remote_xact_side_effect_record_count(void);
extern uint64 cluster_remote_xact_side_effect_drop_count(void);
extern uint64 cluster_remote_xact_outcome_indoubt_count(void);

#endif /* CLUSTER_REMOTE_XACT_H */
