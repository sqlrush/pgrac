/*-------------------------------------------------------------------------
 *
 * cluster_scn.h
 *	  pgrac cluster SCN (System Change Number) typedef + invariants.
 *
 *	  Stage 1.4 ships only the typedef + InvalidScn constant + SCN_VALID
 *	  / SCN_FORMAT macros.  The full encoding layer (scn_encode /
 *	  scn_decode / SCN_NODE_ID_BITS / SCN_LOCAL_BITS) and the comparison
 *	  contract (scn_time_cmp / scn_total_cmp / scn_recovery_cmp + grep
 *	  gate) land at spec-1.15 when local_scn maintenance starts (refer
 *	  to docs/scn-protocol-design.md §3.2 + §3.2.1 v1.1).
 *
 *	  On disk: pd_block_scn (PageHeaderData, stage 1.4), commit_scn
 *	  (ITL slot, stage 1.5), write_scn (ITL slot, stage 1.5), xl_scn
 *	  (WAL record, stage 1.18), and TT slot fields (undo segment,
 *	  stage 1.20) all use this typedef.
 *
 *	  InvalidScn locked to 0 (spec-1.4 §8 Q2 = A) -- matches PG's
 *	  InvalidTransactionId convention; lets MemSet zero-init occupy the
 *	  "not yet set" semantics naturally.  SCN protocol guarantees all
 *	  real values are >= 1 (monotonically advancing per local node), so
 *	  0 is permanently reserved as the "absent" sentinel.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_scn.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-1.4-block-format-pageheader-scn.md
 *	  Design: docs/scn-protocol-design.md v1.1 §3.2 + §3.2.1
 *	  AD-008 (SCN protocol; Lamport-style distributed counters).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SCN_H
#define CLUSTER_SCN_H

#include "c.h" /* uint64 */


/*
 * SCN -- System Change Number.
 *
 *	Stage 1.4 stub: 8-byte unsigned integer.  Stage 1.15 will overlay
 *	the encoding layer (8b node_id || 56b local_scn) on top of this
 *	typedef; the stub already reserves the byte width so that no on-
 *	disk format change is needed when the encoding layer lands.
 */
typedef uint64 SCN;


/*
 * InvalidScn -- the "absent" / "not yet set" sentinel.
 *
 *	Locked to 0 by spec-1.4 §8 Q2 = A.  Real SCN values always >= 1
 *	(monotonically advancing per local node, per AD-008).
 */
#define InvalidScn ((SCN)0)


/*
 * SCN_VALID -- hot-path validity check.
 *
 *	Prefer this macro over direct comparison so future encoding-layer
 *	changes (spec-1.15+) can centralize semantics here.  In particular,
 *	spec-1.15 will introduce scn_time_cmp / scn_total_cmp /
 *	scn_recovery_cmp comparison functions; business code MUST NOT use
 *	`a < b` / `a == b` / `a > b` on SCN values (CI grep gate enforces;
 *	exception: `scn != InvalidScn` via this macro).
 *
 *	See docs/scn-protocol-design.md §3.2.1.
 */
#define SCN_VALID(scn) ((scn) != InvalidScn)


/*
 * SCN_FORMAT / SCN_FORMAT_ARG -- printf-style format helpers.
 *
 *	Hardening v1.0.1 (round 8 P2): use PG's UINT64_FORMAT macro instead
 *	of "%lu" + (unsigned long) cast.  Rationale:
 *	  - Windows LLP64: unsigned long = 32 bits; "%lu" truncates the
 *	    upper 32 bits of a 56-bit local_scn.
 *	  - 32-bit platforms: same truncation.
 *	  - PG's UINT64_FORMAT expands to "%lu" / "%llu" / "%I64u" per ABI
 *	    and accepts a uint64 argument directly; SCN_FORMAT_ARG returns
 *	    the SCN as uint64 unchanged.
 */
#define SCN_FORMAT UINT64_FORMAT
#define SCN_FORMAT_ARG(scn) ((uint64)(scn))


/* ============================================================
 * Spec-1.15: SCN encoding layer (8 bit node_id + 56 bit local_scn)
 * ============================================================
 *
 *	docs/scn-protocol-design.md v1.1 §3.1 / §3.2 + AD-008 distributed
 *	Lamport SCN.  Stage 1.4 stub above is preserved unchanged; the
 *	encoding layer below extends the typedef without changing on-disk
 *	width or alignment.
 *
 *	Layout:
 *	    bit 63..56 : node_id  (8 bits;  0..127 valid, 128..255 reserved)
 *	    bit 55..0  : local_scn (56 bits; monotonically advancing)
 *
 *	Stage 1.15 single-node mode: only the local node advance() path
 *	produces real SCN values.  observe(remote_scn) updates a
 *	max_observed_remote statistic but does NOT bump local_scn (that
 *	lands at spec-1.16 Lamport observe).
 *
 *	Frontend visibility: bufpage.h pulls cluster_scn.h into frontend
 *	tools (pg_checksums / pg_resetwal / ...) via the PageHeaderData
 *	layout.  The encoding constants + inline helpers below are pure
 *	bit math (no LWLock / TimestampTz / palloc) and remain available
 *	to frontend code.  Backend-only APIs (LWLock-protected advance /
 *	observe / TimestampTz accessors / Size shmem hooks) are gated
 *	behind #ifndef FRONTEND below.
 */


/*
 * NodeId — pgrac cluster node identifier (spec-0.10).
 *
 *	cluster.node_id GUC range: -1 (unset / single-node fallback) and
 *	0..127 (valid).  SCN encoding only uses 8 bits; valid range is
 *	0..127, 128..255 reserved.  -1 is INVALID for SCN advance —
 *	cluster_scn_advance() ereport(ERROR) when node_id == -1.
 */
typedef int32 NodeId;


#define SCN_NODE_ID_BITS 8
#define SCN_LOCAL_BITS 56
#define SCN_NODE_ID_SHIFT 56
#define SCN_LOCAL_MASK ((uint64)((1ULL << SCN_LOCAL_BITS) - 1))
/* Spec-1.15 L3: node_id 三段 valid/reserved/invalid. */
#define SCN_MAX_VALID_NODE_ID 127
#define SCN_MAX_NODE_ID ((1 << SCN_NODE_ID_BITS) - 1)
#define SCN_MAX_LOCAL SCN_LOCAL_MASK
#define SCN_NODE_ID_VALID(n) ((n) >= 0 && (n) <= SCN_MAX_VALID_NODE_ID)

/*
 * spec-6.4 ADG pending-commit registry capacity.
 *
 * The registry fences the precommit gap between commit_scn allocation and
 * commit WAL record insertion.  It is intentionally small and fail-closed:
 * overflow freezes ADG thread-safe-SCN publication rather than making an
 * unsafe read floor claim.
 */
#define CLUSTER_SCN_ADG_PENDING_MAX 128


/*
 * Wraparound watermark thresholds (spec-1.15 D7 stub).
 *
 *	Spec-1.15 L6 reality: at 100us per advance,
 *	  2^50 advances ≈ 3568 years (WARNING throttled 1/min)
 *	  2^55 advances ≈ 228000 years (PANIC)
 *	Theoretical sentinels for monitoring discipline; full wrap
 *	protection (freeze table + reset protocol) lands spec-1.16+.
 */
#define SCN_WRAP_WARNING_THRESHOLD ((uint64)1ULL << 50)
#define SCN_WRAP_PANIC_THRESHOLD ((uint64)1ULL << 55)


/* StaticAssertDecl invariants (cluster_scn.c) */
#define SCN_INVARIANT_BITS_SUM (SCN_NODE_ID_BITS + SCN_LOCAL_BITS)
#define SCN_INVARIANT_SIZE 8


/*
 * Encoding inline helpers.
 */
static inline SCN
scn_encode(NodeId node, uint64 local)
{
	Assert(SCN_NODE_ID_VALID(node));
	Assert(local <= SCN_MAX_LOCAL);
	return ((SCN)((uint8)node) << SCN_NODE_ID_SHIFT) | (local & SCN_LOCAL_MASK);
}

static inline NodeId
scn_node_id(SCN scn)
{
	return (NodeId)(uint8)(scn >> SCN_NODE_ID_SHIFT);
}

static inline uint64
scn_local(SCN scn)
{
	return scn & SCN_LOCAL_MASK;
}


#ifndef FRONTEND

#include "datatype/timestamp.h" /* TimestampTz (backend only) */
#include "access/xlogdefs.h"	/* XLogRecPtr */


/*
 * Comparison contract (spec-1.15 §3.2.1; Q2 + L4):
 *
 *	scn_time_cmp     -- visibility / MVCC; only local_scn matters.
 *	scn_total_cmp    -- ITL / global ordering; local_scn → node_id
 *	                    tie-break (NOT raw uint64 — high node_id bits
 *	                    would corrupt time-order on cross-node ties).
 *	scn_recovery_cmp -- WAL k-way merge / standby apply; local_scn →
 *	                    LSN → node_id three-level tie-break.
 *
 *	CI gate (scripts/ci/check-scn-cmp-gate.sh): outside cluster_scn.{c,h},
 *	business code MUST NOT use `<` / `==` / `>` on SCN-typed values.
 */
extern int scn_time_cmp(SCN a, SCN b);
extern int scn_total_cmp(SCN a, SCN b);
extern int scn_recovery_cmp(SCN a, XLogRecPtr a_lsn, NodeId a_node, SCN b, XLogRecPtr b_lsn,
							NodeId b_node);

/*
 * cluster_scn_time_predecessor -- return the greatest SCN strictly before
 * `scn` in time order while preserving the origin node bits.
 *
 * ADG thread barriers use this for "safe below the earliest pending commit".
 * The helper fails closed to InvalidScn when the input has no predecessor
 * (local_scn == 0) or carries a reserved node id.  Callers must not derive the
 * predecessor with raw integer arithmetic outside the SCN layer.
 */
extern SCN cluster_scn_time_predecessor(SCN scn);


/*
 * Public advance / observe API (single-node; spec-1.15 + spec-1.16).
 */
extern SCN cluster_scn_advance(void);
extern void cluster_scn_observe(SCN remote_scn);
extern SCN cluster_scn_current(void);
extern uint64 cluster_scn_advance_count(void);
extern uint64 cluster_scn_max_observed_remote(void);
extern NodeId cluster_scn_node_id(void);
extern TimestampTz cluster_scn_initialized_at(void);
extern TimestampTz cluster_scn_last_advance_at(void);

/*
 * spec-1.16: commit / abort hooks (xact.c + twophase.c).
 *
 *	cluster_scn_advance_for_commit -- wraps cluster_scn_advance() with
 *	commit-specific instrumentation (bumps commit_advance_count + fires
 *	cluster-scn-commit-pre/post-advance inject points).  Returns the
 *	committed SCN for callers; spec-1.16 only-bump (no WAL wiring),
 *	spec-1.18 will pass commit_scn into XactLogCommitRecord for the
 *	xl_scn field.
 *
 *	Caller contract (per spec-1.16 v0.2 Q1):
 *	  - xact.c RecordTransactionCommit: in markXidCommitted else branch,
 *	    BEFORE START_CRIT_SECTION (xact.c:1404).  ereport(ERROR) safe.
 *	  - twophase.c FinishPreparedTransaction(isCommit=true): before
 *	    RecordTransactionCommitPrepared (twophase.c:1554).
 *	  - DO NOT call from PrepareTransaction (PREPARE is not durable
 *	    commit point per spec-1.16 v0.2 Q5).
 *	  - DO NOT call for read-only transactions (markXidCommitted == false
 *	    per spec-1.16 v0.2 Q7).
 *	  - DO NOT call for subtransaction commit.
 *
 *	cluster_scn_advance_for_abort -- mirrors commit hook for abort path
 *	(bumps abort_advance_count + fires cluster-scn-abort-pre/post-advance
 *	inject points).
 *
 *	Caller contract (per spec-1.16 v0.2 Q2):
 *	  - xact.c RecordTransactionAbort: in if (!isSubXact) branch BEFORE
 *	    START_CRIT_SECTION (xact.c:1767).  isSubXact gate is explicit
 *	    (xact.c:5173 calls RecordTransactionAbort(true) for subxacts).
 *	  - twophase.c FinishPreparedTransaction(isCommit=false): before
 *	    RecordTransactionAbortPrepared (twophase.c:1554; ROLLBACK PREPARED
 *	    per spec-1.16 v0.2 Q8).
 */
extern SCN cluster_scn_advance_for_commit(void);
extern SCN cluster_scn_advance_for_abort(void);

/*
 * spec-6.4 ADG pending-commit barrier helpers.
 *
 * cluster_scn_advance_for_commit() registers its returned SCN before exposing
 * it to barrier computation.  Normal commit paths clear the SCN after the
 * commit WAL record has been inserted.  Abort/ERROR cleanup calls the no-arg
 * helper to clear this backend's current pending SCN.
 */
extern bool cluster_scn_pending_commit_clear(SCN commit_scn);
extern bool cluster_scn_pending_commit_clear_my_pending(void);
extern SCN cluster_scn_adg_pending_min_scn(void);
extern SCN cluster_scn_adg_thread_safe_scn(void);
extern uint64 cluster_scn_adg_pending_count(void);
extern bool cluster_scn_adg_pending_overflowed(void);

/*
 * spec-7.4 D1 durable_safe_scn frontier registry.
 *
 * The frontier is the CONTIGUOUS durable watermark this origin publishes on
 * the BOC channel:  every own-origin commit SCN <= the frontier is durable
 * on WAL.  A plain atomic-max of flushed commit SCNs is forbidden (P0):
 * a stalled earlier allocation would be falsely claimed durable.
 *
 * Lifecycle:  cluster_scn_advance_for_commit() registers the returned SCN
 * with an unknown LSN;  the commit path fills in the commit-record LSN once
 * known;  discharge happens on one of three paths (sync commit after its
 * own XLogFlush, walwriter behind XLogBackgroundFlush for async commits,
 * abort cleanup).  Registry overflow freezes the frontier stickily (until
 * restart) rather than publishing an unsafe claim;  consumers fall back to
 * the existing fetch-reply piggyback sampling.
 */
#define CLUSTER_SCN_DURABLE_PENDING_MAX 256

extern SCN cluster_scn_durable_safe_scn(void);
extern bool cluster_scn_durable_pending_fill_lsn(SCN commit_scn, XLogRecPtr commit_lsn);
extern bool cluster_scn_durable_pending_discharge_scn(SCN commit_scn);
extern bool cluster_scn_durable_pending_abort_self(void);
extern uint32 cluster_scn_durable_pending_discharge_upto(XLogRecPtr flushed_lsn);
extern uint64 cluster_scn_durable_pending_count(void);
extern bool cluster_scn_durable_frontier_frozen(void);
extern uint64 cluster_scn_durable_frontier_overflow_count(void);
extern uint64 cluster_scn_durable_frontier_regression_count(void);

/*
 * spec-7.4 D1: BOC payload v1 (wire) + per-origin remote frontier cache.
 *
 * Wire contract (spec-2.9 I5 v0.5 amend):  BOC_BROADCAST payload_length is
 * 0 (pre-v1 sender / event publish off) or 8 (v1 = origin_durable_safe_scn,
 * explicit little-endian).  Any other length is a counted fail-closed drop.
 * The v2 WAL-LSN field (7.5 Smart Fusion) will be a separate appended
 * field;  SCN and WAL-LSN domains never share bytes (L457).
 *
 * Accepted values land in a per-(origin, epoch) seqlock cache (single
 * writer:  the LMON dispatch that runs the handler).  Readers use
 * cluster_scn_remote_durable_safe();  a bounded-spin failure returns false
 * so consumers fall back to the fetch-reply piggyback sampling.
 */
#define CLUSTER_SCN_BOC_PAYLOAD_V1_LEN 8

extern void cluster_scn_boc_payload_encode(SCN scn, uint8 *buf);
extern SCN cluster_scn_boc_payload_decode(const uint8 *buf);
extern bool cluster_scn_remote_durable_safe(NodeId origin, uint64 *epoch_out, SCN *scn_out);
extern uint64 cluster_scn_boc_payload_accept_count(void);
extern uint64 cluster_scn_boc_payload_bad_length_count(void);
extern uint64 cluster_scn_boc_payload_node_mismatch_count(void);
extern uint64 cluster_scn_boc_payload_regression_count(void);

/* spec-1.16 stat accessors (LW_SHARED). */
extern uint64 cluster_scn_commit_advance_count(void);
extern uint64 cluster_scn_abort_advance_count(void);
extern uint64 cluster_scn_observe_bump_count(void);

/*
 * spec-1.17: walwriter BOC tick + 4 stat accessors.
 *
 *	cluster_scn_boc_tick -- walwriter periodic sweep (called from
 *	WalWriterMain after XLogBackgroundFlush, before pgstat_report_wal).
 *	Internal gating: spec-1.17 v0.2 Q4 -- skips if cluster.enabled=off
 *	or sweep interval not elapsed.  Sweep work: refresh
 *	last_advance_at, run wraparound watermark check, bump
 *	boc_sweep_count, compute pending_at_last_sweep, update
 *	boc_max_batch_size, mark a BOC broadcast pulse for LMON-mediated
 *	fanout.
 *
 *	cluster_scn_lmon_drain_boc_broadcast -- LMON-side drain of walwriter
 *	BOC sweeps.  This owns the actual IC fanout because tier1 TCP fds
 *	are LMON process-local (L61).
 *
 *	cluster_scn_boc_pending_since_last_sweep -- lock-free read of
 *	(current_local_scn - boc_last_sweep_local_scn).  walwriter uses
 *	this to inhibit hibernation when SCN advanced since last sweep.
 */
extern void cluster_scn_boc_tick(void);
extern uint64 cluster_scn_boc_pending_since_last_sweep(void);

extern uint64 cluster_scn_boc_sweep_count(void);
extern TimestampTz cluster_scn_boc_last_sweep_at(void);
extern uint64 cluster_scn_boc_pending_at_last_sweep(void);
extern uint64 cluster_scn_boc_max_batch_size(void);
extern void cluster_scn_lmon_drain_boc_broadcast(void);

/*
 * spec-2.10 D4:  LMON drain-side counter accessor.  Counts successful
 * LMON drain batches (>= 1 peer DONE), not per-peer delivered frames.
 * See cluster_scn.c ClusterScnSharedState.boc_broadcast_fanout_count.
 */
extern uint64 cluster_scn_boc_broadcast_fanout_count(void);


/*
 * spec-2.11 D1:  cross-instance commit_scn lookup result enum.
 *
 *	Skeleton phase (spec-2.11 ship):  stub always returns DEFER.
 *	spec-2.26 dual-dim entry skeleton + Stage 3 真激活 will populate
 *	FOUND / NOT_FOUND / ERROR semantics.
 *
 *	Caller contract (spec-2.11 §3.0 I3 + I4):
 *	  - MUST use `switch (result) { ... }` not `if (result) ...`
 *	    (FOUND = 0 triggers false-positive on bool truth test).
 *	  - On DEFER, caller MUST fall back to PG-native visibility path
 *	    (TransactionIdDidCommit + xact_redo etc).
 *	  - DO NOT treat DEFER as INVISIBLE (would silently hide rows).
 *	  - On NOT_FOUND / ERROR (future), caller decides per its own
 *	    fallback policy (likely fall back to PG-native too).
 */
typedef enum ClusterScnLookupResult {
	CLUSTER_SCN_LOOKUP_FOUND = 0,	  /* commit_scn found and written
									   * to *out_commit_scn;  caller may
									   * compare against snapshot SCN */
	CLUSTER_SCN_LOOKUP_DEFER = 1,	  /* lookup not yet implemented
									   * (spec-2.11 skeleton);  caller
									   * MUST fall back to PG-native */
	CLUSTER_SCN_LOOKUP_NOT_FOUND = 2, /* xid not found in any TT slot
									   * (aborted / recycled / unknown);
									   * caller per its fallback policy */
	CLUSTER_SCN_LOOKUP_ERROR = 3,	  /* transient failure (shmem not
									   * init / network / etc);  caller
									   * per its fallback policy */
} ClusterScnLookupResult;

/*
 * spec-2.11 D2:  cross-instance commit_scn lookup API.
 *
 *	Header contract (spec-2.11 §3.0 I1-I4):
 *	  - `out_commit_scn` MUST be non-NULL (D2 stub asserts;Q3-extra).
 *	  - Only valid on result == FOUND (future Stage 3+);  skeleton
 *	    phase always returns DEFER without writing *out_commit_scn.
 *	  - Caller MUST `switch (result)`,  never `if (result)` (FOUND=0).
 *
 *	spec-2.11 skeleton stub:  always returns CLUSTER_SCN_LOOKUP_DEFER
 *	+ bumps cluster_scn.commit_lookup_defer_count (observable via
 *	pg_cluster_state.scn.scn_commit_lookup_defer_count).
 *
 *	Forward-link:  spec-2.26 dual-dim visibility entry skeleton +
 *	Stage 3 真激活 will replace stub body with real cross-instance
 *	protocol;  caller landing point is spec-2.26 entry in cluster-
 *	side visibility path (NOT heapam_visibility.c per AD-012 例外 9).
 */
extern ClusterScnLookupResult cluster_scn_lookup_commit_remote(TransactionId xid,
															   SCN *out_commit_scn);

/*
 * spec-2.11 D4:  lookup invocation defer counter accessor.
 *
 *	Skeleton phase counter — bumped atomically by lookup stub every
 *	call (always returns DEFER).  Lock-free atomic read.  Future
 *	spec-2.26 / Stage 3 真激活 may add per-state counters via amend.
 */
extern uint64 cluster_scn_commit_lookup_defer_count(void);


/*
 * spec-2.12 D4:  SCN convergence boundary verification accessors.
 *
 *	last_observe_at:  TimestampTz of most recent cluster_scn_observe()
 *	  CAS-Lamport bump (cross-node SCN real advance;  NOT idle envelope
 *	  arrival).  Lock-free atomic read of pg_atomic_uint64 raw bits
 *	  (StaticAssertDecl in cluster_scn.c defends 8-byte invariant).
 *	observed_max_observe_gap_ms:  historical peak (now - prev_observe_at)
 *	  across all CAS-Lamport bump events;  atomic-max CAS in observe
 *	  path;  monotonic non-decreasing since shmem init.
 *
 *	Both are local-proxy staleness metrics — NOT true cross-node
 *	propagation lag (which requires NTP and is measured externally
 *	by TAP 102 via poll-based wall clock from harness perspective).
 *
 *	GUC cluster.scn_max_propagation_lag_ms is the configuration bound
 *	(config semantic intent = "propagation lag");  TAP 102 uses it as
 *	hard assertion threshold;  future Hardening may add overrun
 *	counter / WARNING alarms.
 */
extern TimestampTz cluster_scn_last_observe_at(void);
extern uint64 cluster_scn_observed_max_observe_gap_ms(void);


/*
 * spec-2.9 D3: PGRAC_IC_MSG_BOC_BROADCAST dispatch handler.
 *
 *	Registered by cluster_lmon.c phase 1 (spec-2.9 D1) as the recv-side
 *	dispatch entry for BOC broadcast frames.  Body is a deliberate NO-OP
 *	(spec-2.9 §3.0 I6 SCN-via-envelope-piggyback): envelope.scn is
 *	observed via cluster_ic_envelope_verify -> cluster_ic_envelope_observe_scn
 *	(spec-2.4 D5) BEFORE dispatch fires; handler MUST NOT call
 *	cluster_scn_observe directly (spec-2.9 §3.0 I6 + T-scn-13c grep
 *	invariant).
 *
 *	Forward declaration of struct ClusterICEnvelope avoids pulling
 *	cluster_ic_envelope.h into this header (minimizes header coupling;
 *	cluster_scn.h is included by many backend translation units that
 *	have no other reason to pull IC headers in).
 */
struct ClusterICEnvelope;
extern void cluster_scn_boc_broadcast_handler(const struct ClusterICEnvelope *env,
											  const void *payload);


/*
 * spec-1.18: WAL-replay-side observe wrapper.
 *
 *	cluster_scn_recovery_replay_observe -- safely catch cluster_scn_state
 *	up to a SCN parsed from a commit/abort WAL record (ParseCommitRecord
 *	/ ParseAbortRecord populate parsed->scn from the optional
 *	XACT_XINFO_HAS_SCN section; xact_redo_commit / xact_redo_abort
 *	forward it here).
 *
 *	Three-layer gate (spec-1.18 v0.2 HC4):
 *	  1. cluster_enabled is false  -> no-op (vanilla PG behaviour).
 *	  2. cluster_scn_state == NULL -> no-op (early replay before
 *	     cluster_shmem is initialised; plain cluster_scn_observe()
 *	     asserts state != NULL, which would crash recovery).
 *	  3. !SCN_VALID(scn)           -> no-op (record carried InvalidScn
 *	     / record predates spec-1.18 / cluster.enabled was off at emit).
 *	Otherwise calls cluster_scn_observe(scn) which CAS-Lamport bumps
 *	current_local_scn / max_observed_remote_scn under the existing
 *	wraparound + statistics contract.
 *
 *	HC5 note: this wrapper itself never ereport(ERROR) -- recovery code
 *	cannot tolerate ERROR (PANIC-only).  The cluster-scn-replay-observe-
 *	pre inject point added in spec-1.18 fires *here* (ERROR-safe inject
 *	context), distinct from cluster-scn-wal-write-pre which fires inside
 *	XactLogCommitRecord's critical section (PANIC-only).
 */
extern void cluster_scn_recovery_replay_observe(SCN scn);


/*
 * Shmem hookup.
 */
extern Size cluster_scn_shmem_size(void);
extern void cluster_scn_shmem_init(void);
extern void cluster_scn_shmem_register(void);

#endif /* !FRONTEND */


#endif /* CLUSTER_SCN_H */
