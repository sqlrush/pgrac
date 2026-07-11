/*-------------------------------------------------------------------------
 *
 * cluster_scn.c
 *	  pgrac cluster SCN encoding + comparison + single-node advance —
 *	  Stage 1.15 implementation of docs/scn-protocol-design.md §3.1 +
 *	  §3.2.1 cmp contract.
 *
 *	  Spec-1.15 single-node scope:
 *	    - 3 cmp functions (time / total / recovery) per §3.2.1; raw
 *	      `<` / `==` / `>` on SCN forbidden by CI grep gate (D8).
 *	    - cluster_scn_advance() under LW_EXCLUSIVE; ++current_local_scn
 *	      with wraparound watermark hooks.
 *	    - cluster_scn_observe(remote) updates max_observed_remote
 *	      statistic only; does NOT bump local_scn (spec-1.16 Lamport
 *	      observe).
 *	    - cluster_scn_current() / 6 read-only accessors via LW_SHARED.
 *
 *	  Stage 1.15 NOT included (deferred):
 *	    - BOC (broadcast on commit) 100us flush — spec-1.16+
 *	    - Piggyback (cross-instance SCN tracking) — Stage 2+
 *	    - Persistence (control file / shared FS / WAL hosting) — spec-1.16
 *	    - Lamport bump in observe() — spec-1.16
 *	    - Reconfig SCN freeze protocol — Stage 2+ reconfig
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_scn.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-1.15-scn-encoding-layer.md (frozen 2026-05-04)
 *	  Design: docs/scn-protocol-design.md v1.1 §3.2 + §3.2.1
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_scn.h"

#include "cluster/cluster_conf.h" /* cluster_conf_has_peers */
#include "cluster/cluster_cssd.h" /* cluster_cssd_get_alive_peer_count (spec-2.9 D2 Q7 zero-peer short-circuit) */
#include "cluster/cluster_guc.h"		 /* cluster_node_id GUC */
#include "cluster/cluster_ic_envelope.h" /* ClusterICEnvelope (spec-2.9 D3) */
#include "cluster/cluster_ic_router.h" /* cluster_ic_send_envelope_fanout + PGRAC_IC_MSG_BOC_BROADCAST */
#include "cluster/cluster_inject.h" /* CLUSTER_INJECTION_POINT */
#include "cluster/cluster_lmon.h"	/* cluster_lmon_marker_complete_wakeup (spec-7.4 D1-2) */
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_xnode_profile.h" /* PGRAC: spec-5.59 D2 profiling */
#include "access/xlog.h"				   /* GetFlushRecPtr (spec-7.4 D1 walwriter discharge) */
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"


/*
 * StaticAssertDecl: encoding invariants (compile-time sanity).
 */
StaticAssertDecl(SCN_INVARIANT_BITS_SUM == 64, "SCN_NODE_ID_BITS + SCN_LOCAL_BITS must equal 64");
StaticAssertDecl(sizeof(SCN) == SCN_INVARIANT_SIZE, "SCN must be 8 bytes (uint64 alias)");


/*
 * Shmem state struct.  Single writer for current_local_scn / pid /
 * timestamps: cluster_scn_advance() under LW_EXCLUSIVE.  Multi-reader
 * via cluster_scn_current() / accessor functions under LW_SHARED.
 */
typedef struct ClusterScnSharedState {
	LWLock lwlock;						/* spec-1.15 (now: BOC tick + observe CAS-fail tail) */
	NodeId node_id;						/* set-once at shmem_init; lock-free read safe */
	pg_atomic_uint64 current_local_scn; /* spec-1.17: atomic for fetch_add hot path */
	pg_atomic_uint64 max_observed_remote_scn; /* spec-1.17: atomic-max via CAS */
	pg_atomic_uint64 total_advance_count; /* manual advance counter; accessor adds event counters */
	TimestampTz initialized_at;
	TimestampTz last_advance_at; /* refreshed by BOC tick (≤ boc_sweep_interval_ms staleness) */
	/* spec-1.16 additions: per-decision counters */
	pg_atomic_uint64 commit_advance_count; /* incremented by _for_commit */
	pg_atomic_uint64 abort_advance_count;  /* incremented by _for_abort  */
	pg_atomic_uint64 observe_bump_count;   /* incremented by observe bump */
	/* spec-1.17 additions: BOC sweep stats */
	pg_atomic_uint64 boc_sweep_count;		   /* incremented per actual sweep */
	TimestampTz boc_last_sweep_at;			   /* set under LWLock at sweep entry */
	pg_atomic_uint64 boc_last_sweep_local_scn; /* local_scn at last sweep entry */
	pg_atomic_uint64 boc_max_batch_size;	   /* atomic-max via CAS */
	/*
	 * Hardening v1.0.1 (round 10 P2): boc_last_batch_size stores the
	 * delta computed at the LAST sweep (cur_local at sweep entry minus
	 * prev_local recorded by the previous sweep).  Distinct from the
	 * "pending since last sweep" lock-free helper which always returns
	 * the live delta from now.  The exposed pg_cluster_state key
	 * `scn_boc_pending_at_last_sweep` reads this field so the name
	 * matches semantics: "what was pending at the moment the last
	 * sweep ran" (a snapshot, not a live value).
	 */
	pg_atomic_uint64 boc_last_batch_size; /* set under LWLock at sweep */
	/*
	 * spec-2.10 D2:  scn_boc_broadcast_fanout_count counts successful LMON
	 * drain batches, NOT per-peer delivered frames.  Incremented atomically
	 * by cluster_scn_lmon_drain_boc_broadcast whenever fanout produces at
	 * least 1 DONE result (i.e. >= 1 peer actually received the BOC frame
	 * in this drain iteration).
	 *
	 * Diff semantics:  sweep_count - fanout_count is PRIMARILY a measure of
	 * LMON coalescing (walwriter triggers N sweeps per LMON main loop
	 * interval window;  LMON drain coalesces them into 1 fanout batch).
	 * It ALSO includes drain iterations that produced no successful fanout
	 * (0-peer short-circuit, all-PEER_DOWN, all-WOULD_BLOCK, all-HARD_ERROR).
	 * It is NOT a precise "lost frame" count.  Diagnostic value is at ratio
	 * level, not exact-loss-count level.  See spec-2.10 §3.0 I3 + §2.2.
	 */
	pg_atomic_uint64 boc_broadcast_fanout_count;
	/*
	 * spec-2.11 D3:  cross-instance commit_scn lookup invocation defer
	 * counter.
	 *
	 *	Skeleton-only counter.  Bumped atomically by
	 *	cluster_scn_lookup_commit_remote() stub body every call (which
	 *	always returns CLUSTER_SCN_LOOKUP_DEFER in spec-2.11).
	 *
	 *	Future amend (spec-2.26 / Stage 3 真激活):  may expand to
	 *	per-state counters (found / not_found / error) — but skeleton
	 *	阶段 1 counter only (Q4.1 spec frozen).
	 *
	 *	Skeleton phase invocation_count == defer_count by definition;
	 *	an explicit invocation_total is NOT created here to avoid
	 *	premature shmem layout commitment.  See spec-2.11 §3.0 I1.
	 */
	pg_atomic_uint64 commit_lookup_defer_count;
	/*
	 * spec-2.12 D2:  SCN observe local-proxy staleness metrics.
	 *
	 *	last_observe_at_us:  TimestampTz raw bits (int64 μs since 2000)
	 *	of most recent cluster_scn_observe() CAS-Lamport bump (real SCN
	 *	advance from remote);  NOT just envelope arrival (Q3.2 —
	 *	distinguishes idle heartbeat from real cross-node SCN advance).
	 *
	 *	observed_max_observe_gap_ms:  historical peak of (now -
	 *	prev_last_observe_at) across all CAS-Lamport bump events;
	 *	atomic-max CAS pattern (mirror boc_max_batch_size from spec-1.17).
	 *	Reset on shmem init (postmaster restart);  no SQL reset mechanism.
	 *
	 *	v0.2 P1.1 fix (L106 lesson):  USE pg_atomic_uint64 raw bits
	 *	(NOT LWLock-protected TimestampTz).  cluster_scn_observe() CAS
	 *	bump path is lock-free per spec-1.17 v0.2 Q2;  introducing
	 *	LWLock would regress that invariant.  StaticAssertDecl below
	 *	defends 8-byte raw-bits cast.
	 */
	pg_atomic_uint64 last_observe_at_us; /* TimestampTz raw bits */
	pg_atomic_uint64 observed_max_observe_gap_ms;
	/* spec-6.4 D5b: ADG pending-commit SCN registry. */
	uint16 adg_pending_count;
	bool adg_pending_overflowed;
	SCN adg_pending[CLUSTER_SCN_ADG_PENDING_MAX];
	pg_atomic_uint64 adg_pending_register_count;
	pg_atomic_uint64 adg_pending_clear_count;
	pg_atomic_uint64 adg_pending_overflow_count;
	/*
	 * spec-7.4 D1: durable_safe_scn frontier registry (own origin).
	 *
	 *	Tracks commit SCNs allocated but not yet proven durable, so the
	 *	published frontier stays a CONTIGUOUS durability claim:  every
	 *	own-origin commit SCN <= durable_safe_scn is durable on WAL.  A
	 *	plain atomic-max of flushed commit SCNs would falsely cover a
	 *	stalled earlier allocation (P0;  mini-plan v1.1 §1.1).
	 *
	 *	durable_pending_scn/lsn are parallel arrays under lwlock (lsn is
	 *	InvalidXLogRecPtr until the commit record is inserted).  Overflow
	 *	freezes the frontier stickily (until restart);  consumers fall
	 *	back to the fetch-reply piggyback sampling.  durable_safe_scn is
	 *	read lock-free (single u64;  monotonic under the lock).
	 */
	uint16 durable_pending_count;
	bool durable_frozen;
	SCN durable_pending_scn[CLUSTER_SCN_DURABLE_PENDING_MAX];
	XLogRecPtr durable_pending_lsn[CLUSTER_SCN_DURABLE_PENDING_MAX];
	SCN durable_last_allocated;
	pg_atomic_uint64 durable_safe_scn;
	pg_atomic_uint64 durable_overflow_count;
	pg_atomic_uint64 durable_regression_count;
	/*
	 * spec-7.4 D1: per-origin remote durable frontier cache.
	 *
	 *	{epoch, scn} pairs consistency-protected by a per-origin seqlock
	 *	(remote_durable_seq;  odd = write in progress).  Single writer:
	 *	the LMON dispatch running the BOC handler.  Readers bounded-spin
	 *	and fall back (return false) rather than blocking -- the write
	 *	side has no failure point between seq bumps, and a crashed LMON
	 *	takes the whole cluster through PG crash-restart (shmem rebuilt),
	 *	so a stuck-odd seqlock cannot outlive its readers.
	 */
	pg_atomic_uint32 remote_durable_seq[CLUSTER_MAX_NODES];
	uint64 remote_durable_epoch[CLUSTER_MAX_NODES];
	SCN remote_durable_scn[CLUSTER_MAX_NODES];
	pg_atomic_uint64 boc_payload_accept_count;
	pg_atomic_uint64 boc_payload_bad_length_count;
	pg_atomic_uint64 boc_payload_node_mismatch_count;
	pg_atomic_uint64 boc_payload_regression_count;
	/*
	 * spec-7.4 D1-2: commit-event publish protocol.  Producer (any
	 * discharge path that ADVANCES the published frontier):  publish
	 * under the lwlock -> exchange-set dirty -> wake LMON only on the
	 * 0->1 transition (publish-before-signal, L387;  the exchange makes
	 * concurrent producers race-free on the single wakeup).  Consumer
	 * (LMON drain):  exchange-clear dirty BEFORE snapshotting the
	 * frontier, so a publish racing past the snapshot re-arms a wakeup
	 * instead of being lost.
	 */
	pg_atomic_uint32 boc_event_dirty;
} ClusterScnSharedState;

/* spec-2.12 D2:  defensive — raw-bits storage of TimestampTz in
 * pg_atomic_uint64 assumes both are 8 bytes.  PG TimestampTz is int64
 * μs since 2000 (PG 8.0+);  invariant has held for 25+ years. */
StaticAssertDecl(sizeof(TimestampTz) == sizeof(uint64),
				 "spec-2.12 D2: TimestampTz must be 8-byte to fit pg_atomic_uint64");


static ClusterScnSharedState *cluster_scn_state = NULL;
static SCN cluster_scn_backend_pending_commit_scn = InvalidScn;
/* spec-7.4 D1: this backend's in-flight durable-pending commit SCN. */
static SCN cluster_scn_backend_durable_pending_scn = InvalidScn;

/* WARNING throttle: at most 1/min */
static TimestampTz last_warn_emitted_at = 0;

static bool cluster_scn_pending_commit_register_locked(SCN commit_scn);
static bool cluster_scn_pending_commit_remove_locked(SCN commit_scn);
static void cluster_scn_durable_pending_register_locked(SCN commit_scn);
static bool cluster_scn_durable_pending_remove_locked(SCN commit_scn);
static bool cluster_scn_durable_frontier_publish_locked(void);
static void cluster_scn_boc_event_signal(void);


/*
 * ============================================================
 * Comparison functions (spec-1.15 §3.2.1; Q2 + L4)
 * ============================================================
 */

/*
 * scn_time_cmp -- visibility / MVCC ordering.
 *
 *	Only local_scn matters; node_id high bits ignored.  Critical
 *	contract for cross-instance visibility (HeapTupleSatisfiesMVCC
 *	etc.).
 */
int
scn_time_cmp(SCN a, SCN b)
{
	uint64 la = scn_local(a);
	uint64 lb = scn_local(b);

	if (la < lb)
		return -1;
	if (la > lb)
		return 1;
	return 0;
}

/*
 * scn_total_cmp -- ITL / global unique ordering.
 *
 *	Spec-1.15 L4: NOT raw `CMP(a, b)`.  Raw uint64 comparison would
 *	let high node_id bits dominate when local_scn is equal across
 *	nodes; ITL slot ordering would degenerate to node_id-priority on
 *	cross-node ties, breaking the time-priority contract.  Implement
 *	as local_scn → node_id two-level tie-break.
 */
int
scn_total_cmp(SCN a, SCN b)
{
	int c = scn_time_cmp(a, b);

	if (c != 0)
		return c;

	{
		NodeId na = scn_node_id(a);
		NodeId nb = scn_node_id(b);

		if (na < nb)
			return -1;
		if (na > nb)
			return 1;
		return 0;
	}
}

/*
 * scn_recovery_cmp -- WAL k-way merge / standby apply ordering.
 *
 *	Three-level tie-break: local_scn → LSN → node_id.  Ensures
 *	deterministic recovery order across instances on identical SCN
 *	values (rare but possible at cluster boundaries).
 */
int
scn_recovery_cmp(SCN a, XLogRecPtr a_lsn, NodeId a_node, SCN b, XLogRecPtr b_lsn, NodeId b_node)
{
	int c = scn_time_cmp(a, b);

	if (c != 0)
		return c;

	if (a_lsn < b_lsn)
		return -1;
	if (a_lsn > b_lsn)
		return 1;

	if (a_node < b_node)
		return -1;
	if (a_node > b_node)
		return 1;
	return 0;
}

SCN
cluster_scn_time_predecessor(SCN scn)
{
	NodeId node = scn_node_id(scn);
	uint64 local = scn_local(scn);

	if (!SCN_NODE_ID_VALID(node))
		return InvalidScn;
	if (local == 0)
		return InvalidScn;

	return scn_encode(node, local - 1);
}


/*
 * ============================================================
 * Wraparound watermark check (spec-1.15 D7; Q9 + L6)
 * ============================================================
 *
 *	Hardening v1.0.1 (round 10 P3): the WARNING throttle uses a
 *	module-static `last_warn_emitted_at` (TimestampTz).  This function
 *	must be safe to call without holding cluster_scn_state->lwlock --
 *	spec-1.17 observe() invokes it from inside the lock-free CAS retry
 *	loop, while boc_tick() invokes it under LW_EXCLUSIVE for snapshot
 *	coherence with boc_last_sweep_at.  Both call paths are correct in
 *	v1.0.0: the static variable is single-writer-via-throttle (a brief
 *	race produces at most a duplicate WARNING within the 1/min window,
 *	never a missed PANIC).  PANIC ereport is unconditional and not
 *	state-dependent.
 *
 *	If a future change adds shared state with stronger consistency
 *	requirements, split this function into a lock-free threshold gate
 *	+ a separately-locked WARNING throttle.
 */
static void
scn_check_wraparound_watermark(uint64 current)
{
	if (current >= SCN_WRAP_PANIC_THRESHOLD) {
		/* Hardening v1.0.1 (round 8 P3): use the registered SQLSTATE
		 * (ERRCODE_CLUSTER_SCN_WRAPAROUND_PANIC = 53R12) instead of the
		 * generic INTERNAL_ERROR -- the catalog entry was previously
		 * unreachable, which made the registry surface decorative.  */
		ereport(
			PANIC,
			(errcode(ERRCODE_CLUSTER_SCN_WRAPAROUND_PANIC),
			 errmsg("cluster_scn: local_scn (" UINT64_FORMAT
					") reached PANIC threshold (2^55 ≈ 228000 years of advance)",
					current),
			 errhint(
				 "This is a theoretical sentinel; reaching it indicates a runaway advance loop or "
				 "external manipulation.  spec-1.16 introduces real wraparound protection.")));
	} else if (current >= SCN_WRAP_WARNING_THRESHOLD) {
		TimestampTz now_ts = GetCurrentTimestamp();

		/* Hardening v1.0.1 (round 8 P3): the cluster-scn-wraparound-
		 * warning inject point was registered in cluster_inject.c but
		 * had no CLUSTER_INJECTION_POINT() call site -- making it
		 * unreachable.  Add the call site here so injection :error /
		 * :warning at this point actually fires. */
		CLUSTER_INJECTION_POINT("cluster-scn-wraparound-warning");

		/* Throttle: at most 1 WARNING per minute. */
		if (last_warn_emitted_at == 0
			|| TimestampDifferenceExceeds(last_warn_emitted_at, now_ts, 60 * 1000)) {
			last_warn_emitted_at = now_ts;
			ereport(WARNING,
					(errcode(ERRCODE_WARNING),
					 errmsg("cluster_scn: local_scn (" UINT64_FORMAT
							") crossed WARNING threshold (2^50 ≈ 3568 years of advance)",
							current),
					 errhint("Theoretical sentinel for monitoring discipline; spec-1.16 implements "
							 "full wrap protection.  WARNING throttled to 1/min.")));
		}
	}
}


/*
 * ============================================================
 * Public API
 * ============================================================
 */

/*
 * cluster_scn_advance -- bump local SCN by 1 and return encoded SCN.
 *
 *	Spec-1.17 v0.2 Q1: hot path goes through pg_atomic_fetch_add_u64
 *	with no LWLock.  node_id is set-once at shmem_init, so lock-free
 *	read is safe.  Wraparound watermark + last_advance_at refresh are
 *	deferred to cluster_scn_boc_tick (walwriter periodic sweep) ->
 *	staleness ≤ cluster.boc_sweep_interval_ms (default 100ms).
 *
 *	Performance rationale: spec-1.16 LWLock path showed p99 abnormality
 *	on pgbench 5k tps (cacheline ping-pong / spinlock backoff / cold
 *	cache); spec-1.17 atomic path eliminates that abnormality.  The
 *	~50ns vs ~5ns nominal difference is small; the win is removing
 *	contention pathology, not raw cycle savings.
 *
 *	Spec-1.15 L3: ereport(ERROR) when cluster.node_id is unset (-1) or
 *	out of valid range (>127).  This branch is rare (D13 already
 *	WARNs at startup) and uses the same exception path as before.
 */
SCN
cluster_scn_advance(void)
{
	SCN encoded;
	uint64 new_local;
	NodeId node;

	Assert(cluster_scn_state != NULL);

	CLUSTER_INJECTION_POINT("cluster-scn-advance-pre");

	/* Lock-free read: node_id is set-once at shmem_init. */
	node = cluster_scn_state->node_id;
	if (!SCN_NODE_ID_VALID(node))
		ereport(
			ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("cluster_scn_advance: cluster.node_id (%d) is not in the valid range 0..%d",
					node, SCN_MAX_VALID_NODE_ID),
			 errhint("Set cluster.node_id to a value in 0..127 before advancing SCN.  -1 is the "
					 "unset / single-node-fallback sentinel and is not valid for SCN encoding.")));

	/* Hot path: atomic fetch_add returns the OLD value, so add 1.
	 * Wraparound check + last_advance_at refresh moved to BOC tick.
	 * total_advance_count is bumped only by direct SQL/manual callers;
	 * the accessor derives commit/abort/observe counts separately to
	 * avoid one extra contended atomic write per transaction. */
	new_local = pg_atomic_fetch_add_u64(&cluster_scn_state->current_local_scn, 1) + 1;

	encoded = scn_encode(node, new_local);

	CLUSTER_INJECTION_POINT("cluster-scn-advance-post");

	return encoded;
}

/*
 * cluster_scn_observe -- Lamport-bump local SCN from a remote SCN.
 *
 *	Spec-1.16 Q3 (real Lamport bump; upgraded from spec-1.15 stat-only):
 *
 *	   if (remote_local > current_local_scn) {
 *	     current_local_scn = remote_local + 1;  -- Lamport bump
 *	     observe_bump_count++;
 *	     last_advance_at = now;
 *	   }
 *	   if (remote_local > max_observed_remote_scn)
 *	     max_observed_remote_scn = remote_local;  -- stat
 *
 *	Whole compound (max bump + counter inc + stat update + timestamp)
 *	runs under a single LW_EXCLUSIVE per spec-1.16 v0.2 Q3 -- atomic CAS
 *	+ atomic counter would create observation windows where
 *	observe_bump_count and current_local_scn disagree.  observe()
 *	frequency in single-node Stage 1.16 is rare (SQL UDF only); LWLock
 *	contention is not a concern.
 *
 *	Silently ignore InvalidScn input to ease forward-compat with multi-
 *	node code paths that may pass remote SCN before any cross-node
 *	traffic exists.
 */
void
cluster_scn_observe(SCN remote_scn)
{
	uint64 remote_local;
	bool bumped = false;

	Assert(cluster_scn_state != NULL);

	CLUSTER_INJECTION_POINT("cluster-scn-observe-entry");

	if (!SCN_VALID(remote_scn))
		return;

	CLUSTER_INJECTION_POINT("cluster-scn-observe-bump-pre");

	remote_local = scn_local(remote_scn);

	/*
	 * Wraparound guard before any CAS attempt (spec-1.16.1 L22 lesson
	 * inherited; spec-1.17 v0.2 Q9 HC).  Without the guard,
	 * remote_local + 1 may overflow the 56-bit field and scn_encode()
	 * would mask back to 0 -- silent SCN reuse disaster.
	 */
	if (remote_local >= SCN_MAX_LOCAL) {
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster_scn_observe: remote local_scn (" UINT64_FORMAT
						") at or above SCN_MAX_LOCAL; observe rejected to prevent overflow",
						remote_local),
				 errhint("Remote SCN approaching 2^56 sentinel; investigate runaway advance / "
						 "external manipulation upstream.")));
		return;
	}

	/*
	 * Lamport receive: current = max(current, remote) + 1 (covers the
	 * remote == current equality case per spec-1.16.1 L21 lesson; CAS
	 * loop continues unless cur is STRICTLY GREATER than remote_local).
	 *
	 * spec-1.17 v0.2 Q2: lock-free CAS retry loop (no LWLock).  CAS
	 * failure means a concurrent advance() bumped current_local_scn;
	 * reload and retry.  Loop is lock-free progress (PG primitive
	 * guarantee), not wait-free fixed bound -- exit condition is
	 * `cur > remote_local` which becomes true monotonically as
	 * advance() pushes cur upward.
	 */
	for (;;) {
		uint64 cur = pg_atomic_read_u64(&cluster_scn_state->current_local_scn);
		uint64 target;

		if (cur > remote_local)
			break; /* current already strictly greater; no bump needed */

		/* cur <= remote_local: must bump to remote + 1 (covers equality
		 * case per L21).  Wraparound watermark check inside loop (any
		 * iteration may be the one that succeeds and could trip 2^50
		 * threshold; cheaper than running once before CAS).  scn_check
		 * holds its own LWLock for WARNING throttle state. */
		target = remote_local + 1;
		scn_check_wraparound_watermark(target);

		if (pg_atomic_compare_exchange_u64(&cluster_scn_state->current_local_scn, &cur, target)) {
			/* Success.  Bump observe_bump_count.
			 * Atomic compound consistency: spec-1.17 atomic-only path
			 * means dump may observe ns-scale partial state windows --
			 * acceptable for monitoring (vs spec-1.16 LWLock-protected
			 * compound which had the same issue caught by round 9 P2). */
			pg_atomic_fetch_add_u64(&cluster_scn_state->observe_bump_count, 1);

			/*
			 * spec-2.12 D3:  staleness / lag metric update (all-atomic,
			 * NO LWLock per L106 lesson — matches spec-1.17 v0.2 Q2
			 * lock-free CAS invariant).
			 *
			 *   Q3.2 semantic:  this site = "cross-node SCN real advance"
			 *   (CAS-Lamport bump succeeded).  NOT envelope_observe_scn
			 *   entry (idle heartbeat with no SCN advance 不刷新).
			 *
			 *   Race tolerated:  if concurrent observe overlaps,either
			 *   ordering yields acceptable race results (each successful
			 *   observe gets its own delta_ms vs whatever prev_bits was
			 *   at swap time;  atomic-max ensures monotonic non-decreasing
			 *   peak preserved).
			 */
			{
				TimestampTz now_ts = GetCurrentTimestamp();
				uint64 now_bits = (uint64)now_ts;
				uint64 prev_bits;
				TimestampTz prev_ts;

				/* Step 1:  atomic swap last_observe_at_us;  recover prev. */
				prev_bits
					= pg_atomic_exchange_u64(&cluster_scn_state->last_observe_at_us, now_bits);
				prev_ts = (TimestampTz)prev_bits;

				if (prev_ts != 0) {
					/* μs → ms.  TimestampTz is signed int64;  cast to
					 * unsigned uint64 for delta_ms (guaranteed positive
					 * because now_ts is monotonic forward — wall clock
					 * may jump back but only via NTP step,not normal). */
					int64 delta_us = (int64)now_ts - (int64)prev_ts;

					if (delta_us > 0) {
						uint64 delta_ms = (uint64)(delta_us / 1000);

						/* Step 2:  atomic-max CAS (mirror
						 * boc_max_batch_size pattern from spec-1.17). */
						uint64 cur_max;

						for (;;) {
							cur_max = pg_atomic_read_u64(
								&cluster_scn_state->observed_max_observe_gap_ms);
							if (delta_ms <= cur_max)
								break;
							if (pg_atomic_compare_exchange_u64(
									&cluster_scn_state->observed_max_observe_gap_ms, &cur_max,
									delta_ms))
								break;
						}
					}
				}
			}

			bumped = true;
			break;
		}
		/* CAS failed: cur was reloaded by primitive; retry. */
	}

	/* Stat: atomic-max via CAS loop on max_observed_remote_scn. */
	for (;;) {
		uint64 cur_max = pg_atomic_read_u64(&cluster_scn_state->max_observed_remote_scn);

		if (remote_local <= cur_max)
			break;
		if (pg_atomic_compare_exchange_u64(&cluster_scn_state->max_observed_remote_scn, &cur_max,
										   remote_local))
			break;
	}

	(void)bumped; /* result not currently used by callers */
}

/*
 * cluster_scn_advance_for_commit / _for_abort -- spec-1.16 commit/abort
 * hooks.  Wrap cluster_scn_advance() with per-decision counters and
 * inject points.  Caller contract documented in cluster_scn.h.
 *
 * Bootstrap / pre-RUNNING tolerance: initdb's bootstrap mode runs
 * RecordTransactionCommit before cluster.node_id is set (postgresql.conf
 * is not parsed in single-user bootstrap), so cluster_scn_advance would
 * ereport(ERROR) and PANIC the bootstrap.  spec-1.16 v0.2 Q9 + D13:
 * cluster_finalize_startup_running() FATALs if cluster_enabled=on and
 * node_id is invalid before postmaster reaches RUNNING.  Therefore we
 * can safely silently skip the hooks when cluster_scn_state is NULL
 * (early bootstrap before shmem init) or node_id is invalid (bootstrap
 * + initdb post-bootstrap SQL).  Once postmaster reaches RUNNING, D13
 * has confirmed node_id is valid; commits there always advance SCN.
 */
static inline bool
cluster_scn_skip_hook_in_pre_running(void)
{
	/*
	 * Hardening v1.0.1 (round 9 P1 finding 1): cluster.enabled=off
	 * runtime toggle must silence commit/abort SCN advance to satisfy
	 * cluster_finalize_startup_running() docstring "set cluster_enabled
	 * =off for vanilla PG behaviour".  Without this guard, a user with
	 * cluster.enabled=off + cluster.node_id=7 would still see SCN
	 * advance on every commit, contradicting the documented contract.
	 */
	if (!cluster_enabled)
		return true; /* runtime "vanilla PG" toggle */
	if (cluster_scn_state == NULL)
		return true; /* shmem not yet initialised */
	if (!SCN_NODE_ID_VALID(cluster_scn_state->node_id))
		return true; /* bootstrap / no node_id configured */
	/*
	 * P0 (2026-05-31):  do NOT skip on single-node (no-peer).  cluster.enabled
	 * + valid node_id == storage mode, and own-instance CR / cluster snapshots
	 * need read_scn / commit_scn to advance even with one node.  The guards
	 * above (cluster_enabled, shmem ready, valid node_id) already encode the
	 * storage gate; peers only add the cross-node wire on top.
	 */
	return false;
}

SCN
cluster_scn_advance_for_commit(void)
{
	SCN scn;
	ClusterXpScope xps; /* PGRAC: spec-5.59 D2 profiling */

	if (cluster_scn_skip_hook_in_pre_running())
		return InvalidScn;

	/* PGRAC: spec-5.59 D2 profiling */
	cluster_xp_begin(&xps, CLXP_C_SCN_COMMIT_ADVANCE);

	CLUSTER_INJECTION_POINT("cluster-scn-commit-pre-advance");

	{
		NodeId node;

		Assert(cluster_scn_state != NULL);

		/*
		 * INV-ADG8: a barrier must not be able to observe an advanced commit
		 * SCN before the pending registry can see it.  The normal
		 * cluster_scn_advance() hot path publishes with fetch_add first, so
		 * ADG commit advance uses a register-before-CAS reservation loop.
		 *
		 * spec-7.4 D1 extends the same invariant to the durable frontier:
		 * an allocated commit SCN must be in the durable pending registry
		 * BEFORE it becomes observable, or a concurrent discharge could
		 * publish a frontier past it (the atomic-max P0).  The reservation
		 * loop therefore runs for EVERY commit advance;  the ADG registry
		 * registration inside it stays gated on cluster_enable_adg.
		 *
		 * Other, non-commit SCN advances may still race through the lock-free
		 * path.  A failed CAS removes only this backend's tentative entry and
		 * retries with the new current value; a barrier seeing the tentative
		 * entry before the failed CAS is conservative.
		 */
		node = cluster_scn_state->node_id;
		if (!SCN_NODE_ID_VALID(node))
			ereport(
				ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster_scn_advance: cluster.node_id (%d) is not in the valid range 0..%d",
						node, SCN_MAX_VALID_NODE_ID),
				 errhint(
					 "Set cluster.node_id to a value in 0..127 before advancing SCN.  -1 is the "
					 "unset / single-node-fallback sentinel and is not valid for SCN encoding.")));

		for (;;) {
			uint64 old_local;
			uint64 new_local;
			uint64 expected;

			old_local = pg_atomic_read_u64(&cluster_scn_state->current_local_scn);
			new_local = old_local + 1;
			scn = scn_encode(node, new_local);

			LWLockAcquire(&cluster_scn_state->lwlock, LW_EXCLUSIVE);
			if (pg_atomic_read_u64(&cluster_scn_state->current_local_scn) != old_local) {
				LWLockRelease(&cluster_scn_state->lwlock);
				continue;
			}

			if (cluster_enable_adg)
				(void)cluster_scn_pending_commit_register_locked(scn);
			cluster_scn_durable_pending_register_locked(scn);

			expected = old_local;
			if (pg_atomic_compare_exchange_u64(&cluster_scn_state->current_local_scn, &expected,
											   new_local)) {
				LWLockRelease(&cluster_scn_state->lwlock);
				break;
			}

			if (cluster_enable_adg)
				(void)cluster_scn_pending_commit_remove_locked(scn);
			(void)cluster_scn_durable_pending_remove_locked(scn);
			LWLockRelease(&cluster_scn_state->lwlock);
		}
	}
	pg_atomic_fetch_add_u64(&cluster_scn_state->commit_advance_count, 1);

	CLUSTER_INJECTION_POINT("cluster-scn-commit-post-advance");

	cluster_xp_end(&xps); /* PGRAC: spec-5.59 D2 profiling */

	return scn;
}

SCN
cluster_scn_advance_for_abort(void)
{
	SCN scn;

	if (cluster_scn_skip_hook_in_pre_running())
		return InvalidScn;

	CLUSTER_INJECTION_POINT("cluster-scn-abort-pre-advance");

	scn = cluster_scn_advance();
	pg_atomic_fetch_add_u64(&cluster_scn_state->abort_advance_count, 1);

	CLUSTER_INJECTION_POINT("cluster-scn-abort-post-advance");

	return scn;
}

/*
 * cluster_scn_recovery_replay_observe -- spec-1.18 WAL-replay observe
 *	wrapper.  Called from xact_redo_commit / xact_redo_abort with the
 *	SCN parsed from the optional XACT_XINFO_HAS_SCN section of a
 *	commit/abort WAL record (parsed->scn).
 *
 *	HC4 three-layer gate: the plain cluster_scn_observe() entry asserts
 *	cluster_scn_state != NULL and does not check cluster_enabled, so it
 *	is unsafe to call directly from recovery code (early replay can
 *	land here before cluster_shmem_init runs; --disable-cluster builds
 *	must not advance SCN at all).  This wrapper applies the three
 *	checks and forwards real values to the existing CAS-Lamport
 *	machinery.
 *
 *	HC5 note: this wrapper's pre-inject point (cluster-scn-replay-
 *	observe-pre) fires in xact_redo_commit / xact_redo_abort entry
 *	context, which has not entered a critical section yet — :error
 *	fault here is ERROR-safe.
 *
 *	Caveat (Hardening v1.0.1 P3-2; codex review 2026-05-05): this only
 *	covers the wrapper's own inject point.  cluster_scn_observe()
 *	called below has additional inject points (cluster-scn-observe-
 *	entry, cluster-scn-observe-bump-pre) and may also raise WARNING via
 *	scn_check_wraparound_watermark; arming :error on those points
 *	independently can still surface ERROR in this code path.  Tests
 *	that need a guaranteed ERROR-safe path should arm only this
 *	wrapper's inject point.
 */
void
cluster_scn_recovery_replay_observe(SCN scn)
{
	/* Layer 1: runtime feature toggle (vanilla PG behaviour when off). */
	if (!cluster_enabled)
		return;
	/* Layer 2: shmem may not yet be initialised in early replay. */
	if (cluster_scn_state == NULL)
		return;
	/* Layer 3: record predates spec-1.18 / cluster.enabled was off at emit. */
	if (!SCN_VALID(scn))
		return;

	CLUSTER_INJECTION_POINT("cluster-scn-replay-observe-pre");

	cluster_scn_observe(scn);
}

static bool
cluster_scn_pending_commit_register_locked(SCN commit_scn)
{
	uint16 i;

	if (cluster_scn_state == NULL || !SCN_VALID(commit_scn))
		return false;

	if (cluster_scn_state->adg_pending_overflowed) {
		return false;
	}
	for (i = 0; i < cluster_scn_state->adg_pending_count; i++) {
		if (scn_total_cmp(cluster_scn_state->adg_pending[i], commit_scn) == 0) {
			cluster_scn_backend_pending_commit_scn = commit_scn;
			return true;
		}
	}
	if (cluster_scn_state->adg_pending_count >= CLUSTER_SCN_ADG_PENDING_MAX) {
		cluster_scn_state->adg_pending_overflowed = true;
		pg_atomic_fetch_add_u64(&cluster_scn_state->adg_pending_overflow_count, 1);
		return false;
	}
	cluster_scn_state->adg_pending[cluster_scn_state->adg_pending_count++] = commit_scn;
	cluster_scn_backend_pending_commit_scn = commit_scn;
	pg_atomic_fetch_add_u64(&cluster_scn_state->adg_pending_register_count, 1);
	return true;
}

static bool
cluster_scn_pending_commit_remove_locked(SCN commit_scn)
{
	uint16 i;

	if (cluster_scn_state == NULL || !SCN_VALID(commit_scn))
		return false;

	for (i = 0; i < cluster_scn_state->adg_pending_count; i++) {
		if (scn_total_cmp(cluster_scn_state->adg_pending[i], commit_scn) == 0) {
			uint16 j;

			for (j = (uint16)(i + 1); j < cluster_scn_state->adg_pending_count; j++)
				cluster_scn_state->adg_pending[j - 1] = cluster_scn_state->adg_pending[j];
			cluster_scn_state->adg_pending_count--;
			cluster_scn_state->adg_pending[cluster_scn_state->adg_pending_count] = InvalidScn;
			pg_atomic_fetch_add_u64(&cluster_scn_state->adg_pending_clear_count, 1);
			if (scn_total_cmp(cluster_scn_backend_pending_commit_scn, commit_scn) == 0)
				cluster_scn_backend_pending_commit_scn = InvalidScn;
			return true;
		}
	}
	return false;
}

bool
cluster_scn_pending_commit_clear(SCN commit_scn)
{
	bool cleared = false;

	if (cluster_scn_state == NULL || !SCN_VALID(commit_scn))
		return false;

	LWLockAcquire(&cluster_scn_state->lwlock, LW_EXCLUSIVE);
	cleared = cluster_scn_pending_commit_remove_locked(commit_scn);
	LWLockRelease(&cluster_scn_state->lwlock);

	if (scn_total_cmp(cluster_scn_backend_pending_commit_scn, commit_scn) == 0)
		cluster_scn_backend_pending_commit_scn = InvalidScn;
	return cleared;
}

bool
cluster_scn_pending_commit_clear_my_pending(void)
{
	SCN pending = cluster_scn_backend_pending_commit_scn;

	if (!SCN_VALID(pending))
		return false;
	return cluster_scn_pending_commit_clear(pending);
}

SCN
cluster_scn_adg_pending_min_scn(void)
{
	SCN min_scn = InvalidScn;
	uint16 i;

	if (cluster_scn_state == NULL)
		return InvalidScn;

	LWLockAcquire(&cluster_scn_state->lwlock, LW_SHARED);
	if (cluster_scn_state->adg_pending_overflowed) {
		LWLockRelease(&cluster_scn_state->lwlock);
		return InvalidScn;
	}
	for (i = 0; i < cluster_scn_state->adg_pending_count; i++) {
		SCN candidate = cluster_scn_state->adg_pending[i];

		if (!SCN_VALID(candidate))
			continue;
		if (!SCN_VALID(min_scn) || scn_time_cmp(candidate, min_scn) < 0
			|| (scn_time_cmp(candidate, min_scn) == 0 && scn_total_cmp(candidate, min_scn) < 0))
			min_scn = candidate;
	}
	LWLockRelease(&cluster_scn_state->lwlock);
	return min_scn;
}

SCN
cluster_scn_adg_thread_safe_scn(void)
{
	SCN current_scn;
	SCN min_pending;

	if (cluster_scn_state == NULL)
		return InvalidScn;
	if (cluster_scn_adg_pending_overflowed())
		return InvalidScn;

	current_scn = cluster_scn_current();
	if (!SCN_VALID(current_scn))
		return InvalidScn;

	min_pending = cluster_scn_adg_pending_min_scn();
	if (!SCN_VALID(min_pending))
		return current_scn;
	return cluster_scn_time_predecessor(min_pending);
}

uint64
cluster_scn_adg_pending_count(void)
{
	uint64 count;

	if (cluster_scn_state == NULL)
		return 0;
	LWLockAcquire(&cluster_scn_state->lwlock, LW_SHARED);
	count = cluster_scn_state->adg_pending_count;
	LWLockRelease(&cluster_scn_state->lwlock);
	return count;
}

bool
cluster_scn_adg_pending_overflowed(void)
{
	bool overflowed;

	if (cluster_scn_state == NULL)
		return false;
	LWLockAcquire(&cluster_scn_state->lwlock, LW_SHARED);
	overflowed = cluster_scn_state->adg_pending_overflowed;
	LWLockRelease(&cluster_scn_state->lwlock);
	return overflowed;
}

/*
 * ============================================================
 * spec-7.4 D1: durable_safe_scn frontier registry.
 * ============================================================
 *
 *	Register at commit-SCN allocation (inside the reservation loop in
 *	cluster_scn_advance_for_commit, under lwlock), fill in the commit
 *	record LSN once XactLogCommitRecord returns, discharge on one of
 *	three paths:
 *
 *	  (a) sync commit -- the committing backend proved its own record
 *	      durable via XLogFlush;  it discharges by SCN after leaving
 *	      the commit critical section.
 *	  (b) async commit -- walwriter discharges every entry whose LSN
 *	      is at or below the background-flush horizon.
 *	  (c) abort -- allocation never became a commit;  remove it so the
 *	      frontier is not blocked forever.
 *
 *	The published frontier is min(pending)-1 (time predecessor) while
 *	entries are pending, else the last allocated commit SCN.  Both are
 *	safe:  no own-origin commit SCN above the published value is ever
 *	claimed durable, and every commit SCN at or below it has been
 *	discharged as durable (aborted allocations are not commits and make
 *	no durability claim).
 */

/*
 * cluster_scn_durable_pending_register_locked -- caller holds lwlock
 *	LW_EXCLUSIVE.  Registers a freshly allocated commit SCN with an
 *	unknown LSN.  On overflow the frontier freezes stickily instead of
 *	risking an unsafe claim (LOG once on the transition;  counter for
 *	monitoring).
 */
static void
cluster_scn_durable_pending_register_locked(SCN commit_scn)
{
	if (cluster_scn_state == NULL || !SCN_VALID(commit_scn))
		return;

	/* Track every commit allocation, frozen or not:  the empty-registry
	 * frontier re-derivation depends on it. */
	if (!SCN_VALID(cluster_scn_state->durable_last_allocated)
		|| scn_total_cmp(commit_scn, cluster_scn_state->durable_last_allocated) > 0)
		cluster_scn_state->durable_last_allocated = commit_scn;

	if (cluster_scn_state->durable_frozen)
		return;

	if (cluster_scn_state->durable_pending_count >= CLUSTER_SCN_DURABLE_PENDING_MAX) {
		cluster_scn_state->durable_frozen = true;
		pg_atomic_fetch_add_u64(&cluster_scn_state->durable_overflow_count, 1);
		ereport(LOG, (errmsg("cluster durable SCN frontier frozen: pending registry overflow "
							 "(more than %d in-flight commits)",
							 CLUSTER_SCN_DURABLE_PENDING_MAX),
					  errhint("Remote nodes fall back to fetch-reply SCN sampling until this "
							  "node restarts.")));
		return;
	}

	cluster_scn_state->durable_pending_scn[cluster_scn_state->durable_pending_count] = commit_scn;
	cluster_scn_state->durable_pending_lsn[cluster_scn_state->durable_pending_count]
		= InvalidXLogRecPtr;
	cluster_scn_state->durable_pending_count++;
	cluster_scn_backend_durable_pending_scn = commit_scn;
}

/*
 * cluster_scn_durable_pending_remove_locked -- caller holds lwlock
 *	LW_EXCLUSIVE.  Swap-removes the entry for commit_scn (order is not
 *	meaningful;  the frontier is derived by a min scan).  Does NOT
 *	publish;  callers decide whether removal was a durability proof.
 */
static bool
cluster_scn_durable_pending_remove_locked(SCN commit_scn)
{
	uint16 i;

	if (cluster_scn_state == NULL || !SCN_VALID(commit_scn))
		return false;

	for (i = 0; i < cluster_scn_state->durable_pending_count; i++) {
		if (scn_total_cmp(cluster_scn_state->durable_pending_scn[i], commit_scn) == 0) {
			uint16 last = (uint16)(cluster_scn_state->durable_pending_count - 1);

			cluster_scn_state->durable_pending_scn[i]
				= cluster_scn_state->durable_pending_scn[last];
			cluster_scn_state->durable_pending_lsn[i]
				= cluster_scn_state->durable_pending_lsn[last];
			cluster_scn_state->durable_pending_scn[last] = InvalidScn;
			cluster_scn_state->durable_pending_lsn[last] = InvalidXLogRecPtr;
			cluster_scn_state->durable_pending_count = last;
			if (scn_total_cmp(cluster_scn_backend_durable_pending_scn, commit_scn) == 0)
				cluster_scn_backend_durable_pending_scn = InvalidScn;
			return true;
		}
	}
	return false;
}

/*
 * cluster_scn_durable_frontier_publish_locked -- caller holds lwlock
 *	LW_EXCLUSIVE.  Recomputes the contiguous durable frontier and
 *	publishes it monotonically.  A recomputed value below the published
 *	one is a bug in the discharge protocol:  refuse + count (L441
 *	monotonic discipline), never regress the public claim.  Returns true
 *	only when the published value STRICTLY advanced (drives the D1-2
 *	event signal;  republishing an equal claim is not an event).
 */
static bool
cluster_scn_durable_frontier_publish_locked(void)
{
	SCN frontier;
	SCN current;

	if (cluster_scn_state == NULL || cluster_scn_state->durable_frozen)
		return false;

	if (cluster_scn_state->durable_pending_count > 0) {
		SCN min_pending = cluster_scn_state->durable_pending_scn[0];
		uint16 i;

		for (i = 1; i < cluster_scn_state->durable_pending_count; i++) {
			if (scn_total_cmp(cluster_scn_state->durable_pending_scn[i], min_pending) < 0)
				min_pending = cluster_scn_state->durable_pending_scn[i];
		}
		frontier = cluster_scn_time_predecessor(min_pending);
	} else
		frontier = cluster_scn_state->durable_last_allocated;

	if (!SCN_VALID(frontier))
		return false;

	current = (SCN)pg_atomic_read_u64(&cluster_scn_state->durable_safe_scn);
	if (SCN_VALID(current)) {
		int cmp = scn_total_cmp(frontier, current);

		if (cmp < 0) {
			pg_atomic_fetch_add_u64(&cluster_scn_state->durable_regression_count, 1);
			return false;
		}
		if (cmp == 0)
			return false; /* republish of the same claim; no event */
	}
	pg_atomic_write_u64(&cluster_scn_state->durable_safe_scn, frontier);
	return true; /* strictly advanced */
}

/*
 * cluster_scn_boc_event_signal -- producer half of the D1-2 event
 *	protocol.  Call AFTER releasing the SCN lwlock, only when the
 *	publish strictly advanced the frontier.  Publish-before-signal
 *	(L387) is inherent:  the frontier write happened under the lock
 *	before this runs.  The injection point suppresses the event to
 *	prove the sweep cadence remains a sufficient fallback (L2).
 */
static void
cluster_scn_boc_event_signal(void)
{
	if (!cluster_boc_event_publish)
		return;
	if (cluster_scn_state == NULL)
		return;
	if (cluster_injection_should_skip("cluster-boc-event-publish"))
		return;
	if (pg_atomic_exchange_u32(&cluster_scn_state->boc_event_dirty, 1) == 0)
		cluster_lmon_marker_complete_wakeup();
}

/*
 * cluster_scn_boc_event_consume -- consumer half:  LMON drain clears
 *	the dirty flag BEFORE snapshotting the frontier, so a publish that
 *	races in after the snapshot re-arms a wakeup (no lost update).
 */
bool
cluster_scn_boc_event_consume(void)
{
	if (cluster_scn_state == NULL)
		return false;
	return pg_atomic_exchange_u32(&cluster_scn_state->boc_event_dirty, 0) != 0;
}

/*
 * cluster_scn_durable_safe_scn -- lock-free read of the published
 *	contiguous durable frontier (InvalidScn until anything published).
 */
SCN
cluster_scn_durable_safe_scn(void)
{
	if (cluster_scn_state == NULL)
		return InvalidScn;
	return (SCN)pg_atomic_read_u64(&cluster_scn_state->durable_safe_scn);
}

/*
 * cluster_scn_durable_pending_fill_lsn -- record the commit-record LSN
 *	for a pending commit SCN once XactLogCommitRecord has returned.
 *	Until filled, the walwriter flush horizon can never discharge the
 *	entry (an unwritten record has no durability horizon).  Pure shmem
 *	write under the lock;  safe inside the commit critical section.
 */
bool
cluster_scn_durable_pending_fill_lsn(SCN commit_scn, XLogRecPtr commit_lsn)
{
	uint16 i;
	bool found = false;

	if (cluster_scn_state == NULL || !SCN_VALID(commit_scn) || XLogRecPtrIsInvalid(commit_lsn))
		return false;

	LWLockAcquire(&cluster_scn_state->lwlock, LW_EXCLUSIVE);
	for (i = 0; i < cluster_scn_state->durable_pending_count; i++) {
		if (scn_total_cmp(cluster_scn_state->durable_pending_scn[i], commit_scn) == 0) {
			cluster_scn_state->durable_pending_lsn[i] = commit_lsn;
			found = true;
			break;
		}
	}
	LWLockRelease(&cluster_scn_state->lwlock);
	return found;
}

/*
 * cluster_scn_durable_pending_discharge_scn -- sync-commit discharge:
 *	the caller proved this commit record durable (its own XLogFlush
 *	returned).  Runs AFTER the commit critical section (mini-plan v1.1
 *	ruling #3:  no publish work inside the crit section).
 */
bool
cluster_scn_durable_pending_discharge_scn(SCN commit_scn)
{
	bool removed;
	bool advanced = false;

	if (cluster_scn_state == NULL || !SCN_VALID(commit_scn))
		return false;

	LWLockAcquire(&cluster_scn_state->lwlock, LW_EXCLUSIVE);
	removed = cluster_scn_durable_pending_remove_locked(commit_scn);
	if (removed)
		advanced = cluster_scn_durable_frontier_publish_locked();
	LWLockRelease(&cluster_scn_state->lwlock);
	if (advanced)
		cluster_scn_boc_event_signal();
	return removed;
}

/*
 * cluster_scn_durable_pending_abort_self -- abort-path cleanup:  this
 *	backend's in-flight allocation never became a commit;  unblock the
 *	frontier.  Mirrors cluster_scn_pending_commit_clear_my_pending.
 *
 *	Guard:  an entry with a FILLED lsn is a real commit (its commit
 *	record was inserted;  abort cannot happen past that point without
 *	PANIC, and an async commit leaves its entry filled for the
 *	walwriter).  Such an entry is only detached from this backend, never
 *	removed -- removing it would let the frontier cover an unflushed
 *	commit (the P0 this registry exists to prevent).
 */
bool
cluster_scn_durable_pending_abort_self(void)
{
	SCN pending = cluster_scn_backend_durable_pending_scn;
	bool removed = false;
	bool advanced = false;
	uint16 i;

	if (cluster_scn_state == NULL || !SCN_VALID(pending))
		return false;

	LWLockAcquire(&cluster_scn_state->lwlock, LW_EXCLUSIVE);
	for (i = 0; i < cluster_scn_state->durable_pending_count; i++) {
		if (scn_total_cmp(cluster_scn_state->durable_pending_scn[i], pending) == 0) {
			if (XLogRecPtrIsInvalid(cluster_scn_state->durable_pending_lsn[i])) {
				removed = cluster_scn_durable_pending_remove_locked(pending);
				if (removed)
					advanced = cluster_scn_durable_frontier_publish_locked();
			}
			break;
		}
	}
	LWLockRelease(&cluster_scn_state->lwlock);
	cluster_scn_backend_durable_pending_scn = InvalidScn;
	if (advanced)
		cluster_scn_boc_event_signal();
	return removed;
}

/*
 * cluster_scn_durable_pending_discharge_upto -- walwriter discharge:
 *	every pending entry whose (known) commit-record LSN is at or below
 *	the background-flush horizon is durable.  Entries with an unknown
 *	LSN stay pending regardless of the horizon.  Returns the number of
 *	entries discharged.
 */
uint32
cluster_scn_durable_pending_discharge_upto(XLogRecPtr flushed_lsn)
{
	uint32 discharged = 0;
	bool advanced = false;
	uint16 i;

	if (cluster_scn_state == NULL || XLogRecPtrIsInvalid(flushed_lsn))
		return 0;

	LWLockAcquire(&cluster_scn_state->lwlock, LW_EXCLUSIVE);
	i = 0;
	while (i < cluster_scn_state->durable_pending_count) {
		XLogRecPtr entry_lsn = cluster_scn_state->durable_pending_lsn[i];

		if (!XLogRecPtrIsInvalid(entry_lsn) && entry_lsn <= flushed_lsn) {
			/* swap-remove keeps index i pointing at an unvisited entry */
			(void)cluster_scn_durable_pending_remove_locked(
				cluster_scn_state->durable_pending_scn[i]);
			discharged++;
		} else
			i++;
	}
	if (discharged > 0)
		advanced = cluster_scn_durable_frontier_publish_locked();
	LWLockRelease(&cluster_scn_state->lwlock);
	if (advanced)
		cluster_scn_boc_event_signal();
	return discharged;
}

uint64
cluster_scn_durable_pending_count(void)
{
	uint64 count;

	if (cluster_scn_state == NULL)
		return 0;
	LWLockAcquire(&cluster_scn_state->lwlock, LW_SHARED);
	count = cluster_scn_state->durable_pending_count;
	LWLockRelease(&cluster_scn_state->lwlock);
	return count;
}

bool
cluster_scn_durable_frontier_frozen(void)
{
	bool frozen;

	if (cluster_scn_state == NULL)
		return false;
	LWLockAcquire(&cluster_scn_state->lwlock, LW_SHARED);
	frozen = cluster_scn_state->durable_frozen;
	LWLockRelease(&cluster_scn_state->lwlock);
	return frozen;
}

uint64
cluster_scn_durable_frontier_overflow_count(void)
{
	if (cluster_scn_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_scn_state->durable_overflow_count);
}

uint64
cluster_scn_durable_frontier_regression_count(void)
{
	if (cluster_scn_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_scn_state->durable_regression_count);
}

/*
 * cluster_scn_current -- read current encoded SCN without advancing.
 *
 *	Returns InvalidScn (= 0) in two cases:
 *	  (1) cluster.node_id = -1: encoding cannot produce a real SCN
 *	      (spec-1.15 L3); cluster_scn_advance() reports via ereport.
 *	  (2) current_local_scn = 0 (post-init / pre-first-advance):
 *	      Hardening v1.0.1 (round 8 P1) -- otherwise scn_encode(node!=0,
 *	      0) returns a non-zero bit pattern that PASSES SCN_VALID() but
 *	      compares equal to InvalidScn under scn_time_cmp() (both have
 *	      local_scn=0).  That ambiguity propagates into visibility code
 *	      paths once they consume cluster_scn_current().  Treating
 *	      local_scn=0 as "absent" sentinel matches PG's
 *	      InvalidTransactionId convention and the spec-1.4 §8 Q2 = A
 *	      docstring already locking InvalidScn=0 to "real values >= 1".
 */
SCN
cluster_scn_current(void)
{
	SCN encoded;
	NodeId node;
	uint64 local;

	Assert(cluster_scn_state != NULL);

	/* spec-1.17: current_local_scn / max_observed_remote_scn are atomic.
	 * node_id is set-once at shmem_init.  All lock-free reads safe. */
	node = cluster_scn_state->node_id;
	local = pg_atomic_read_u64(&cluster_scn_state->current_local_scn);

	if (!SCN_NODE_ID_VALID(node))
		return InvalidScn;

	if (local == 0)
		return InvalidScn; /* spec-1.4 §8 Q2: real values >= 1 */

	encoded = scn_encode(node, local);
	return encoded;
}

/*
 * Read-only accessors.  spec-1.17: lock-free atomic reads where applicable.
 * Used by dump_scn (cluster_debug.c) and TAP regression.
 */
uint64
cluster_scn_advance_count(void)
{
	Assert(cluster_scn_state != NULL);
	return pg_atomic_read_u64(&cluster_scn_state->total_advance_count)
		   + pg_atomic_read_u64(&cluster_scn_state->commit_advance_count)
		   + pg_atomic_read_u64(&cluster_scn_state->abort_advance_count)
		   + pg_atomic_read_u64(&cluster_scn_state->observe_bump_count);
}

uint64
cluster_scn_max_observed_remote(void)
{
	Assert(cluster_scn_state != NULL);
	return pg_atomic_read_u64(&cluster_scn_state->max_observed_remote_scn);
}

NodeId
cluster_scn_node_id(void)
{
	Assert(cluster_scn_state != NULL);
	return cluster_scn_state->node_id; /* set once at shmem_init */
}

TimestampTz
cluster_scn_initialized_at(void)
{
	Assert(cluster_scn_state != NULL);
	return cluster_scn_state->initialized_at;
}

TimestampTz
cluster_scn_last_advance_at(void)
{
	TimestampTz v;

	Assert(cluster_scn_state != NULL);
	LWLockAcquire(&cluster_scn_state->lwlock, LW_SHARED);
	v = cluster_scn_state->last_advance_at;
	LWLockRelease(&cluster_scn_state->lwlock);
	return v;
}

/* spec-1.16 stat accessors. */
uint64
cluster_scn_commit_advance_count(void)
{
	Assert(cluster_scn_state != NULL);
	return pg_atomic_read_u64(&cluster_scn_state->commit_advance_count);
}

uint64
cluster_scn_abort_advance_count(void)
{
	Assert(cluster_scn_state != NULL);
	return pg_atomic_read_u64(&cluster_scn_state->abort_advance_count);
}

uint64
cluster_scn_observe_bump_count(void)
{
	Assert(cluster_scn_state != NULL);
	return pg_atomic_read_u64(&cluster_scn_state->observe_bump_count);
}


/*
 * ============================================================
 * spec-1.17 BOC tick (walwriter periodic sweep)
 * + spec-2.9 BOC broadcast send/recv (Q1-Q10 frozen 2026-05-11)
 * ============================================================
 */

/*
 * cluster_scn_boc_payload_encode / _decode -- spec-7.4 D1 BOC payload v1.
 *
 *	Explicit little-endian, independent of host byte order (the envelope
 *	wire is LE per spec-2.3;  the payload follows the same convention).
 *	v1 carries exactly one field:  origin_durable_safe_scn (8 bytes).
 */
void
cluster_scn_boc_payload_encode(SCN scn, uint8 *buf)
{
	int i;

	for (i = 0; i < CLUSTER_SCN_BOC_PAYLOAD_V1_LEN; i++)
		buf[i] = (uint8)((scn >> (8 * i)) & 0xFF);
}

SCN
cluster_scn_boc_payload_decode(const uint8 *buf)
{
	SCN scn = 0;
	int i;

	for (i = 0; i < CLUSTER_SCN_BOC_PAYLOAD_V1_LEN; i++)
		scn |= ((SCN)buf[i]) << (8 * i);
	return scn;
}

/*
 * cluster_scn_boc_broadcast_handler -- spec-2.9 D3 dispatch handler.
 *
 *	Registered via cluster_lmon.c phase 1 (spec-2.9 D1) for msg_type =
 *	PGRAC_IC_MSG_BOC_BROADCAST (= 3).
 *
 *	Envelope-side Lamport piggyback is unchanged (spec-2.9 §3.0 I6):
 *	envelope.scn (frozen at offset 20 per spec-2.3) is observed via
 *	cluster_ic_envelope_verify -> cluster_ic_envelope_observe_scn
 *	(spec-2.4 D5) BEFORE this handler fires.  Handler MUST NOT call
 *	cluster_scn_observe directly (spec-2.9 §3.0 I6 + T-scn-13c grep
 *	invariant).
 *
 *	spec-7.4 D1 (spec-2.9 I5 v0.5 amend):  payload_length in {0, 8}.
 *	0 = pre-v1 sender or event publish off -- pulse-only frame, DEBUG2
 *	trace only.  8 = payload v1 origin_durable_safe_scn (LE):  validate
 *	length / decodability / SCN node bits == env.source_node_id, then
 *	update the per-(origin, epoch) cache.  Every reject is a counted
 *	fail-closed drop;  a lower SCN within the same epoch is a counted
 *	regression reject (reordered frame);  an epoch change rebuilds the
 *	entry (R5).
 *
 * PGRAC modifications by SqlRush <sqlrush@gmail.com>:
 * What changed: spec-7.4 D1 replaced the payload-zero Assert with the
 * versioned {0, 8} contract above.
 * Why: the durable frontier rides the existing BOC pulse (zero new
 * msg_type);  mixed-version peers keep sending 0-byte frames, which
 * remain valid pulse-only traffic.
 */
void
cluster_scn_boc_broadcast_handler(const ClusterICEnvelope *env, const void *payload)
{
	SCN scn;
	uint32 origin;
	uint32 seq;
	bool same_epoch;
	SCN cached;

	Assert(env != NULL);

	if (env->payload_length == 0) {
		/* Pre-v1 sender / publish off:  pulse-only frame (I5 v0.4
		 * behavior preserved bit-for-bit). */
		ereport(
			DEBUG2,
			(errmsg("cluster_scn: BOC broadcast received from peer %u (env.scn=" UINT64_FORMAT ")",
					env->source_node_id, env->scn)));
		return;
	}

	if (cluster_scn_state == NULL)
		return; /* shmem not up; nothing to update */

	if (env->payload_length != CLUSTER_SCN_BOC_PAYLOAD_V1_LEN || payload == NULL) {
		pg_atomic_fetch_add_u64(&cluster_scn_state->boc_payload_bad_length_count, 1);
		ereport(DEBUG1, (errmsg("cluster_scn: dropped BOC payload with bad length %u from peer %u",
								env->payload_length, env->source_node_id)));
		return;
	}

	scn = cluster_scn_boc_payload_decode(payload);
	if (!SCN_VALID(scn)) {
		/* Structurally 8 bytes but undecodable (InvalidScn):  count it
		 * with the malformed-frame drops. */
		pg_atomic_fetch_add_u64(&cluster_scn_state->boc_payload_bad_length_count, 1);
		ereport(DEBUG1, (errmsg("cluster_scn: dropped undecodable BOC payload from peer %u",
								env->source_node_id)));
		return;
	}

	origin = env->source_node_id;
	if (origin >= CLUSTER_MAX_NODES || (uint32)scn_node_id(scn) != origin) {
		pg_atomic_fetch_add_u64(&cluster_scn_state->boc_payload_node_mismatch_count, 1);
		ereport(DEBUG1, (errmsg("cluster_scn: dropped BOC payload with node mismatch (scn node %d, "
								"envelope peer %u)",
								(int)scn_node_id(scn), env->source_node_id)));
		return;
	}

	/*
	 * Per-(origin, epoch) cache update.  Single writer (the LMON dispatch
	 * running this handler), so reading the current fields directly is
	 * safe;  the seqlock protects concurrent readers only.
	 */
	same_epoch = (cluster_scn_state->remote_durable_epoch[origin] == env->epoch);
	cached = cluster_scn_state->remote_durable_scn[origin];
	if (same_epoch && SCN_VALID(cached) && scn_total_cmp(scn, cached) < 0) {
		pg_atomic_fetch_add_u64(&cluster_scn_state->boc_payload_regression_count, 1);
		return; /* reordered older frame; keep the newer claim */
	}

	seq = pg_atomic_read_u32(&cluster_scn_state->remote_durable_seq[origin]);
	pg_atomic_write_u32(&cluster_scn_state->remote_durable_seq[origin], seq + 1);
	pg_write_barrier();
	cluster_scn_state->remote_durable_epoch[origin] = env->epoch;
	cluster_scn_state->remote_durable_scn[origin] = scn;
	pg_write_barrier();
	pg_atomic_write_u32(&cluster_scn_state->remote_durable_seq[origin], seq + 2);
	pg_atomic_fetch_add_u64(&cluster_scn_state->boc_payload_accept_count, 1);

	ereport(DEBUG2, (errmsg("cluster_scn: BOC durable frontier from peer %u = " UINT64_FORMAT
							" (epoch " UINT64_FORMAT ")",
							env->source_node_id, scn, env->epoch)));
}

/*
 * cluster_scn_remote_durable_safe -- read the cached durable frontier
 *	claim for a remote origin.  Returns false when the origin is out of
 *	range, nothing has been cached yet, or the bounded seqlock spin is
 *	exhausted -- callers fall back to the fetch-reply piggyback sampling
 *	(never block on this cache).
 */
bool
cluster_scn_remote_durable_safe(NodeId origin, uint64 *epoch_out, SCN *scn_out)
{
	int retries;

	if (cluster_scn_state == NULL || origin < 0 || origin >= CLUSTER_MAX_NODES)
		return false;

	for (retries = 0; retries < 1000; retries++) {
		uint32 s1 = pg_atomic_read_u32(&cluster_scn_state->remote_durable_seq[origin]);
		uint64 epoch;
		SCN scn;

		if (s1 & 1)
			continue; /* write in progress */
		pg_read_barrier();
		epoch = cluster_scn_state->remote_durable_epoch[origin];
		scn = cluster_scn_state->remote_durable_scn[origin];
		pg_read_barrier();
		if (pg_atomic_read_u32(&cluster_scn_state->remote_durable_seq[origin]) != s1)
			continue; /* torn read; retry */

		if (!SCN_VALID(scn))
			return false; /* nothing cached for this origin yet */
		if (epoch_out != NULL)
			*epoch_out = epoch;
		if (scn_out != NULL)
			*scn_out = scn;
		return true;
	}
	return false; /* bounded spin exhausted; fail open to fallback path */
}

uint64
cluster_scn_boc_payload_accept_count(void)
{
	if (cluster_scn_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_scn_state->boc_payload_accept_count);
}

uint64
cluster_scn_boc_payload_bad_length_count(void)
{
	if (cluster_scn_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_scn_state->boc_payload_bad_length_count);
}

uint64
cluster_scn_boc_payload_node_mismatch_count(void)
{
	if (cluster_scn_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_scn_state->boc_payload_node_mismatch_count);
}

uint64
cluster_scn_boc_payload_regression_count(void)
{
	if (cluster_scn_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_scn_state->boc_payload_regression_count);
}

/*
 * cluster_scn_emit_broadcast_pulse -- spec-2.9 D2 walwriter marker.
 *
 *	Replaces the spec-1.17 single-node DEBUG2 stub.  The walwriter BOC
 *	tick owns the sweep cadence, but it must NOT touch the tier1 TCP
 *	send path: tier1 fds are LMON process-local (L61) and fanout is
 *	explicitly LMON-only.  Therefore the actual wire send is drained by
 *	cluster_scn_lmon_drain_boc_broadcast(), which observes the monotone
 *	boc_sweep_count advanced immediately before this marker is called.
 *
 *	v0.3 Q6 / I8 — walwriter-not-in-crit-section invariant:
 *	  Caller walwriter::cluster_scn_boc_tick runs in WalWriterMain main
 *	  loop body, which by spec-1.17 design carries no critical section.
 *	  First-line Assert pins this invariant; future walwriter refactor
 *	  that accidentally introduces a crit section is caught at debug
 *	  build time before silently breaking IC send safety.
 *
 *	v0.3 Q7 / I9 zero-peer-short-circuit is enforced in the LMON drain
 *	path, where the router/fanout is actually reachable.
 */
static void
cluster_scn_emit_broadcast_pulse(void)
{
	/* v0.3 Q6 / I8: walwriter BOC tick site invariant — never in crit
	 * section.  Cheap enforcement that turns a verbal assumption into a
	 * code constraint.  Silent return defensive path is explicitly
	 * forbidden per spec-2.9 Q6 (would mask broken caller context). */
	Assert(CritSectionCount == 0);

	ereport(DEBUG3, (errmsg("cluster_scn: BOC broadcast pending (sweep_count=" UINT64_FORMAT
							", local_scn=" UINT64_FORMAT ")",
							pg_atomic_read_u64(&cluster_scn_state->boc_sweep_count),
							pg_atomic_read_u64(&cluster_scn_state->current_local_scn))));
}

/*
 * cluster_scn_lmon_drain_boc_broadcast -- spec-2.9 D2 review fix.
 *
 *	LMON-mediated fanout for walwriter-owned BOC sweeps.  This preserves
 *	Q1=A's walwriter cadence without violating the IC ownership model:
 *	only LMON may use cluster_ic_send_envelope_fanout because only LMON
 *	owns tier1 TCP fds.  The handoff signal is the monotone
 *	boc_sweep_count in ClusterScnSharedState, so no new shmem fields are
 *	needed and the spec remains 0 catalog/shmem-surface churn.
 */
void
cluster_scn_lmon_drain_boc_broadcast(void)
{
	static uint64 last_drained_sweep_count = 0;
	ClusterICFanoutResult per_peer[CLUSTER_MAX_NODES];
	uint64 sweep_count;
	bool event_pending;
	SCN frontier;
	uint8 payload[CLUSTER_SCN_BOC_PAYLOAD_V1_LEN];
	const void *send_payload = NULL;
	uint32 send_len = 0;
	int peer;
	int done = 0;
	int would_block = 0;
	int hard_error = 0;
	ClusterXpScope xps; /* PGRAC: spec-5.59 D2 profiling */

	if (!cluster_enabled)
		return;
	if (cluster_scn_state == NULL)
		return;

	Assert(MyBackendType == B_LMON);

	/* v0.3 Q7 / I9: 0 peer means no router/fanout work and no log spam.
	 * Checked BEFORE consuming the event flag so neither the sweep nor a
	 * pending commit event is marked drained;  if a peer becomes alive
	 * later, LMON still emits the latest state.  (spec-7.4 D1-2 moved
	 * this check ahead of the sweep gate for exactly that reason.) */
	if (cluster_cssd_get_alive_peer_count() == 0)
		return;

	/* spec-7.4 D1-2: exchange-clear the event flag BEFORE snapshotting
	 * (a publish racing past the snapshot re-arms a wakeup).  Either a
	 * new sweep beat or a commit event justifies a fanout. */
	event_pending = cluster_scn_boc_event_consume();
	sweep_count = pg_atomic_read_u64(&cluster_scn_state->boc_sweep_count);
	if (sweep_count == last_drained_sweep_count && !event_pending)
		return;

	/* PGRAC: spec-5.59 D2 profiling */
	cluster_xp_begin(&xps, CLXP_C_SCN_BOC_BROADCAST);

	/* spec-7.4 D1-2: attach payload v1 (the published durable frontier)
	 * when event publish is on and a frontier exists;  otherwise keep
	 * the pre-D1 0-length pulse (byte equivalence when the GUC is off,
	 * and old receivers treat 0-length as pulse-only either way). */
	frontier = cluster_scn_durable_safe_scn();
	if (cluster_boc_event_publish && SCN_VALID(frontier)) {
		cluster_scn_boc_payload_encode(frontier, payload);
		send_payload = payload;
		send_len = CLUSTER_SCN_BOC_PAYLOAD_V1_LEN;
	}

	cluster_ic_send_envelope_fanout(PGRAC_IC_MSG_BOC_BROADCAST, send_payload, send_len, per_peer);
	last_drained_sweep_count = sweep_count;

	for (peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		switch (per_peer[peer]) {
		case CLUSTER_IC_FANOUT_DONE:
			done++;
			break;
		case CLUSTER_IC_FANOUT_WOULD_BLOCK:
			would_block++;
			break;
		case CLUSTER_IC_FANOUT_HARD_ERROR:
			hard_error++;
			break;
		case CLUSTER_IC_FANOUT_PEER_DOWN:
			break;
		}
	}

	if (done > 0) {
		/* PGRAC: spec-2.10 D3 — bump aggregate fanout_count when >= 1 peer
		 * actually received the frame.  WOULD_BLOCK / HARD_ERROR / PEER_DOWN
		 * not counted (failed fanout cases).  See spec-2.10 §3.0 I3 + §2.2
		 * for diff semantic vs sweep_count. */
		pg_atomic_fetch_add_u64(&cluster_scn_state->boc_broadcast_fanout_count, 1);

		ereport(DEBUG3, (errmsg("cluster_scn: BOC broadcast fanout done "
								"(sweep_count=" UINT64_FORMAT ", done=%d)",
								sweep_count, done)));
	}

	cluster_xp_end(&xps); /* PGRAC: spec-5.59 D2 profiling */

	if (would_block > 0 || hard_error > 0)
		ereport(DEBUG2, (errmsg("cluster_scn: BOC broadcast fanout partial "
								"(sweep_count=" UINT64_FORMAT ", would_block=%d, hard_error=%d)",
								sweep_count, would_block, hard_error)));
}

/*
 * cluster_scn_boc_tick -- walwriter periodic sweep entry.
 *
 *	Caller: WalWriterMain after XLogBackgroundFlush, before
 *	pgstat_report_wal (walwriter.c PGRAC MODIFICATIONS hook, spec-1.17
 *	v0.2 Q4).  Frequency is bounded by Min(WalWriterDelay,
 *	cluster.boc_sweep_interval_ms); walwriter wake rate dictates upper
 *	bound on sweep frequency.
 *
 *	Internal gating (spec-1.17 v0.2 Q4 + Q9):
 *	  - cluster.enabled=off: skip entirely (vanilla PG semantic;
 *	    inherits spec-1.16.1 L20 lesson)
 *	  - cluster_scn_state == NULL: shmem not yet init; skip
 *	  - elapsed since last sweep < cluster_boc_sweep_interval_ms: skip
 *
 *	Sweep work:
 *	  - bump boc_sweep_count
 *	  - refresh boc_last_sweep_at + last_advance_at
 *	  - compute pending = current_local_scn - boc_last_sweep_local_scn
 *	  - update boc_max_batch_size (atomic-max via CAS)
 *	  - run scn_check_wraparound_watermark on current_local_scn
 *	  - emit broadcast pulse stub
 */
void
cluster_scn_boc_tick(void)
{
	/* L9 lesson: avoid `now` (shadows time.h::now()).  Use now_ts. */
	TimestampTz now_ts;
	uint64 cur_local;
	uint64 prev_local;
	uint64 batch;
	uint64 cur_max;

	/* spec-1.16.1 L20 inheritance + spec-1.17 v0.2 Q4 cluster_enabled gate */
	if (!cluster_enabled)
		return;
	if (cluster_scn_state == NULL)
		return;

	/*
	 * spec-7.4 D1:  async-commit durable frontier discharge.  Runs on
	 * every walwriter wakeup (NOT throttled by the sweep interval):  the
	 * caller sits right behind XLogBackgroundFlush, so the flush horizon
	 * proves every filled pending entry at or below it durable.  Cheap
	 * no-op when nothing is pending.
	 */
	(void)cluster_scn_durable_pending_discharge_upto(GetFlushRecPtr(NULL));

	now_ts = GetCurrentTimestamp();

	/* Throttle: skip if elapsed < cluster.boc_sweep_interval_ms.
	 * cluster_boc_sweep_interval_ms is millisecond-typed; convert
	 * boc_last_sweep_at delta to ms via PG TimestampDifference. */
	{
		TimestampTz last;
		long secs;
		int usecs;

		LWLockAcquire(&cluster_scn_state->lwlock, LW_SHARED);
		last = cluster_scn_state->boc_last_sweep_at;
		LWLockRelease(&cluster_scn_state->lwlock);

		if (last != 0) {
			/* delta_ms scope reduced into this branch (L23 lesson form). */
			long delta_ms;

			TimestampDifference(last, now_ts, &secs, &usecs);
			delta_ms = secs * 1000 + usecs / 1000;
			if (delta_ms < cluster_boc_sweep_interval_ms)
				return;
		}
	}

	CLUSTER_INJECTION_POINT("cluster-scn-boc-sweep-pre");

	/* Sweep under LW_EXCLUSIVE for boc_last_sweep_at + watermark
	 * WARNING throttle state coherence (spec-1.17 v0.2 Q6 cold path). */
	LWLockAcquire(&cluster_scn_state->lwlock, LW_EXCLUSIVE);
	cur_local = pg_atomic_read_u64(&cluster_scn_state->current_local_scn);
	prev_local = pg_atomic_read_u64(&cluster_scn_state->boc_last_sweep_local_scn);
	batch = (cur_local >= prev_local) ? (cur_local - prev_local) : 0;
	pg_atomic_write_u64(&cluster_scn_state->boc_last_sweep_local_scn, cur_local);
	pg_atomic_write_u64(&cluster_scn_state->boc_last_batch_size, batch);
	cluster_scn_state->boc_last_sweep_at = now_ts;
	/*
	 * Hardening v1.0.1 (round 10 P2): only refresh last_advance_at
	 * when current_local_scn actually advanced since the last sweep.
	 * Pre fix: every sweep refreshed last_advance_at, making it
	 * indistinguishable from boc_last_sweep_at and misleading
	 * operators about whether transactions actually progressed.
	 */
	if (cur_local != prev_local)
		cluster_scn_state->last_advance_at = now_ts;
	pg_atomic_fetch_add_u64(&cluster_scn_state->boc_sweep_count, 1);
	scn_check_wraparound_watermark(cur_local);
	LWLockRelease(&cluster_scn_state->lwlock);

	/* atomic-max via CAS for boc_max_batch_size */
	for (;;) {
		cur_max = pg_atomic_read_u64(&cluster_scn_state->boc_max_batch_size);
		if (batch <= cur_max)
			break;
		if (pg_atomic_compare_exchange_u64(&cluster_scn_state->boc_max_batch_size, &cur_max, batch))
			break;
	}

	cluster_scn_emit_broadcast_pulse();

	CLUSTER_INJECTION_POINT("cluster-scn-boc-sweep-post");
}

/*
 * cluster_scn_boc_pending_since_last_sweep -- lock-free helper for
 * walwriter.c hibernate-inhibition logic (spec-1.17 v0.2 Q4).
 *
 *	Reads atomic current_local_scn and boc_last_sweep_local_scn;
 *	returns delta (or 0 if shmem not init).
 */
uint64
cluster_scn_boc_pending_since_last_sweep(void)
{
	uint64 cur, prev;

	if (cluster_scn_state == NULL)
		return 0;

	cur = pg_atomic_read_u64(&cluster_scn_state->current_local_scn);
	prev = pg_atomic_read_u64(&cluster_scn_state->boc_last_sweep_local_scn);
	return (cur >= prev) ? (cur - prev) : 0;
}

/* spec-1.17 BOC stat accessors. */
uint64
cluster_scn_boc_sweep_count(void)
{
	Assert(cluster_scn_state != NULL);
	return pg_atomic_read_u64(&cluster_scn_state->boc_sweep_count);
}

TimestampTz
cluster_scn_boc_last_sweep_at(void)
{
	TimestampTz v;

	Assert(cluster_scn_state != NULL);
	LWLockAcquire(&cluster_scn_state->lwlock, LW_SHARED);
	v = cluster_scn_state->boc_last_sweep_at;
	LWLockRelease(&cluster_scn_state->lwlock);
	return v;
}

uint64
cluster_scn_boc_pending_at_last_sweep(void)
{
	/*
	 * Hardening v1.0.1 (round 10 P2): this accessor backs the SQL
	 * key `scn_boc_pending_at_last_sweep`, which the name promises to
	 * be "what was pending at the moment the last sweep ran" -- a
	 * snapshot.  Pre fix: returned the live delta (cur - boc_last_
	 * sweep_local_scn) which is the delta SINCE the last sweep, not
	 * AT it.  Now reads the dedicated boc_last_batch_size field
	 * written under LWLock at sweep entry.
	 */
	Assert(cluster_scn_state != NULL);
	return pg_atomic_read_u64(&cluster_scn_state->boc_last_batch_size);
}

uint64
cluster_scn_boc_max_batch_size(void)
{
	Assert(cluster_scn_state != NULL);
	return pg_atomic_read_u64(&cluster_scn_state->boc_max_batch_size);
}

/*
 * spec-2.10 D4:  accessor for LMON drain-side success-batch counter.
 *
 *	Counts successful LMON drain batches (>= 1 peer DONE per fanout
 *	iteration), NOT per-peer delivered frames.  See ClusterScnSharedState
 *	boc_broadcast_fanout_count comment + spec-2.10 §3.0 I3 / §2.2 for full
 *	semantics.  Lock-free atomic read (mirror cluster_scn_boc_sweep_count).
 */
uint64
cluster_scn_boc_broadcast_fanout_count(void)
{
	Assert(cluster_scn_state != NULL);
	return pg_atomic_read_u64(&cluster_scn_state->boc_broadcast_fanout_count);
}

/*
 * spec-2.11 D2:  cross-instance commit_scn lookup stub.
 *
 *	Skeleton phase:  always returns CLUSTER_SCN_LOOKUP_DEFER + bumps
 *	commit_lookup_defer_count atomically.  out_commit_scn is NOT written
 *	(caller's sentinel preserved).
 *
 *	Caller contract (spec-2.11 §3.0 I1-I4 + header doxygen):
 *	  - out_commit_scn MUST be non-NULL (Q3-extra;defensive Assert
 *	    catches future FOUND path NULL deref early at debug build).
 *	  - Caller MUST `switch (result)`;  never `if (result)` (FOUND=0).
 *	  - On DEFER, caller MUST fall back to PG-native visibility path.
 *	  - DO NOT treat DEFER as INVISIBLE (would silently hide rows).
 *
 *	Forward-link:  spec-2.26 dual-dim visibility entry skeleton +
 *	Stage 3 真激活 will replace this stub body with real cross-instance
 *	commit_scn protocol (cluster_ic msg replier + TT slot lookup +
 *	per-state result mapping).
 */
ClusterScnLookupResult
cluster_scn_lookup_commit_remote(TransactionId xid pg_attribute_unused(), SCN *out_commit_scn)
{
	/* Q3-extra (user-mandated):  Header contract requires out_commit_scn
	 * non-NULL.  Stub body asserts even though it doesn't write — this
	 * catches caller bugs early at debug build before spec-2.26+ FOUND
	 * path lands.  Runtime semantics unchanged (still DEFER). */
	Assert(out_commit_scn != NULL);

	/* spec-2.11 D2 skeleton stub:  always DEFER.  out_commit_scn left
	 * unchanged (caller's sentinel preserved).  spec-2.26 / Stage 3 真激活
	 * will replace this body. */
	pg_atomic_fetch_add_u64(&cluster_scn_state->commit_lookup_defer_count, 1);

	return CLUSTER_SCN_LOOKUP_DEFER;
}

/*
 * spec-2.11 D4:  accessor for skeleton-phase defer counter.
 *
 *	Lock-free atomic read (mirror cluster_scn_boc_broadcast_fanout_count
 *	pattern from spec-2.10 D4).  Bumped by D2 stub on every call.
 *	Future spec-2.26 / Stage 3 真激活 may add per-state counters via
 *	amend (FOUND / NOT_FOUND / ERROR);  skeleton has 1 counter only
 *	(Q4.1 spec frozen).
 */
uint64
cluster_scn_commit_lookup_defer_count(void)
{
	Assert(cluster_scn_state != NULL);
	return pg_atomic_read_u64(&cluster_scn_state->commit_lookup_defer_count);
}

/*
 * spec-2.12 D4:  SCN convergence boundary verification accessors.
 *
 *	last_observe_at:  TimestampTz of most recent CAS-Lamport bump
 *	  (cross-node SCN real advance;  NOT idle envelope arrival).
 *	observed_max_observe_gap_ms:  historical peak inter-observe gap
 *	  since shmem init.
 *
 *	Both are local-proxy staleness metrics — NOT true cross-node
 *	propagation lag (which requires NTP and is measured by TAP 102).
 *
 *	Lock-free atomic read (L106 lesson — observe path is lock-free
 *	per spec-1.17 v0.2 Q2;  these accessors mirror that invariant).
 */
TimestampTz
cluster_scn_last_observe_at(void)
{
	Assert(cluster_scn_state != NULL);
	return (TimestampTz)pg_atomic_read_u64(&cluster_scn_state->last_observe_at_us);
}

uint64
cluster_scn_observed_max_observe_gap_ms(void)
{
	Assert(cluster_scn_state != NULL);
	return pg_atomic_read_u64(&cluster_scn_state->observed_max_observe_gap_ms);
}


/*
 * ============================================================
 * Shmem hookup
 * ============================================================
 */

Size
cluster_scn_shmem_size(void)
{
	return sizeof(ClusterScnSharedState);
}

void
cluster_scn_shmem_init(void)
{
	bool found;

	cluster_scn_state = ShmemInitStruct("pgrac cluster scn", cluster_scn_shmem_size(), &found);
	if (!found) {
		LWLockInitialize(&cluster_scn_state->lwlock, LWTRANCHE_CLUSTER_SCN);
		cluster_scn_state->node_id = cluster_node_id; /* may be -1; advance() rejects */
		/* spec-1.17: current_local_scn / max_observed_remote_scn now atomic */
		pg_atomic_init_u64(&cluster_scn_state->current_local_scn, 0);
		pg_atomic_init_u64(&cluster_scn_state->max_observed_remote_scn, 0);
		pg_atomic_init_u64(&cluster_scn_state->total_advance_count, 0);
		cluster_scn_state->initialized_at = GetCurrentTimestamp();
		cluster_scn_state->last_advance_at = 0;
		/* spec-1.16 counters */
		pg_atomic_init_u64(&cluster_scn_state->commit_advance_count, 0);
		pg_atomic_init_u64(&cluster_scn_state->abort_advance_count, 0);
		pg_atomic_init_u64(&cluster_scn_state->observe_bump_count, 0);
		/* spec-1.17 BOC sweep stats */
		pg_atomic_init_u64(&cluster_scn_state->boc_sweep_count, 0);
		cluster_scn_state->boc_last_sweep_at = 0;
		pg_atomic_init_u64(&cluster_scn_state->boc_last_sweep_local_scn, 0);
		pg_atomic_init_u64(&cluster_scn_state->boc_max_batch_size, 0);
		pg_atomic_init_u64(&cluster_scn_state->boc_last_batch_size, 0);
		pg_atomic_init_u64(&cluster_scn_state->boc_broadcast_fanout_count, 0);
		/* spec-2.11 D3: init skeleton-phase commit_scn lookup defer counter. */
		pg_atomic_init_u64(&cluster_scn_state->commit_lookup_defer_count, 0);
		/* spec-2.12 D2 init zero (TimestampTz raw bits + atomic counter). */
		pg_atomic_init_u64(&cluster_scn_state->last_observe_at_us, 0);
		pg_atomic_init_u64(&cluster_scn_state->observed_max_observe_gap_ms, 0);
		cluster_scn_state->adg_pending_count = 0;
		cluster_scn_state->adg_pending_overflowed = false;
		memset(cluster_scn_state->adg_pending, 0, sizeof(cluster_scn_state->adg_pending));
		pg_atomic_init_u64(&cluster_scn_state->adg_pending_register_count, 0);
		pg_atomic_init_u64(&cluster_scn_state->adg_pending_clear_count, 0);
		pg_atomic_init_u64(&cluster_scn_state->adg_pending_overflow_count, 0);
		/* spec-7.4 D1: durable_safe_scn frontier registry. */
		cluster_scn_state->durable_pending_count = 0;
		cluster_scn_state->durable_frozen = false;
		memset(cluster_scn_state->durable_pending_scn, 0,
			   sizeof(cluster_scn_state->durable_pending_scn));
		memset(cluster_scn_state->durable_pending_lsn, 0,
			   sizeof(cluster_scn_state->durable_pending_lsn));
		cluster_scn_state->durable_last_allocated = InvalidScn;
		pg_atomic_init_u64(&cluster_scn_state->durable_safe_scn, InvalidScn);
		pg_atomic_init_u64(&cluster_scn_state->durable_overflow_count, 0);
		pg_atomic_init_u64(&cluster_scn_state->durable_regression_count, 0);
		/* spec-7.4 D1: per-origin remote frontier cache + payload counters. */
		{
			int i;

			for (i = 0; i < CLUSTER_MAX_NODES; i++) {
				pg_atomic_init_u32(&cluster_scn_state->remote_durable_seq[i], 0);
				cluster_scn_state->remote_durable_epoch[i] = 0;
				cluster_scn_state->remote_durable_scn[i] = InvalidScn;
			}
		}
		pg_atomic_init_u64(&cluster_scn_state->boc_payload_accept_count, 0);
		pg_atomic_init_u64(&cluster_scn_state->boc_payload_bad_length_count, 0);
		pg_atomic_init_u64(&cluster_scn_state->boc_payload_node_mismatch_count, 0);
		pg_atomic_init_u64(&cluster_scn_state->boc_payload_regression_count, 0);
		pg_atomic_init_u32(&cluster_scn_state->boc_event_dirty, 0);
	}
}

static const ClusterShmemRegion cluster_scn_region = {
	.name = "pgrac cluster scn",
	.size_fn = cluster_scn_shmem_size,
	.init_fn = cluster_scn_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_scn",
	.reserved_flags = 0,
};


void
cluster_scn_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_scn_region);
}


/*
 * ============================================================
 * SQL UDF wrappers (spec-1.15 D11; Q7 + L2 + L7)
 *
 *	Wrappers take _sql suffix to avoid C symbol collision with the
 *	C API names (cluster_scn_advance / current / observe).  Mutating
 *	APIs (advance / observe) gate on superuser; current is read-only
 *	and public.
 * ============================================================
 */

#endif /* USE_PGRAC_CLUSTER */


/* ============================================================
 * SQL UDF wrappers (always linked; body guarded by USE_PGRAC_CLUSTER).
 * ============================================================
 *
 *	pg_proc.dat references cluster_scn_advance_sql / current_sql /
 *	observe_sql unconditionally, so these symbols must resolve at link
 *	time even in --disable-cluster builds.  In disable mode the bodies
 *	raise ERRCODE_FEATURE_NOT_SUPPORTED.
 *
 *	Wrappers take _sql suffix to avoid C symbol collision with the
 *	C API names (cluster_scn_advance / current / observe).  Mutating
 *	APIs (advance / observe) gate on superuser; current is read-only
 *	and public.
 */

PG_FUNCTION_INFO_V1(cluster_scn_advance_sql);
PG_FUNCTION_INFO_V1(cluster_scn_current_sql);
PG_FUNCTION_INFO_V1(cluster_scn_observe_sql);

Datum
cluster_scn_advance_sql(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	SCN scn;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_scn_advance() is restricted to superuser")));

	scn = cluster_scn_advance();
	pg_atomic_fetch_add_u64(&cluster_scn_state->total_advance_count, 1);
	PG_RETURN_INT64((int64)scn);
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_scn_advance() requires --enable-cluster")));
	PG_RETURN_INT64(0);
#endif
}

Datum
cluster_scn_current_sql(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	SCN scn = cluster_scn_current();

	PG_RETURN_INT64((int64)scn);
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_scn_current() requires --enable-cluster")));
	PG_RETURN_INT64(0);
#endif
}

Datum
cluster_scn_observe_sql(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	int64 remote_int = PG_GETARG_INT64(0);
	NodeId remote_node;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_scn_observe() is restricted to superuser")));

	/* Hardening v1.0.1 (round 8 P2): reject negative SQL input.  int8 is
	 * signed; -1 cast to uint64 becomes 0xFFFFFFFFFFFFFFFF and scn_local()
	 * extracts 2^56-1, which would permanently poison
	 * max_observed_remote_scn until restart.  Legal encoded SCNs with
	 * node_id 0..127 always fit non-negative int8, so rejecting
	 * remote_int < 0 is safe. */
	if (remote_int < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster_scn_observe(): remote_scn must be non-negative (got %ld)",
						(long)remote_int),
				 errhint("Encoded SCN values for valid node_id (0..127) always fit in non-negative "
						 "int8.  Caller passing a synthetic SCN must construct it via "
						 "(node_id::bigint << 56) | local_scn.")));

	/* Hardening v1.0.1 (round 8 P2): reject reserved / invalid node_id.
	 * The encoding allocates 8 bits but only 0..127 are valid; 128..255
	 * are reserved for forward-compat (Stage 2+ thousand-node).  Letting
	 * remote SCNs with reserved node_id leak into max_observed_remote_scn
	 * is a forward-compat hazard. */
	remote_node = scn_node_id((SCN)remote_int);
	if ((SCN)remote_int != InvalidScn && !SCN_NODE_ID_VALID(remote_node))
		ereport(
			ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("cluster_scn_observe(): remote_scn carries reserved node_id %d (valid 0..%d)",
					remote_node, SCN_MAX_VALID_NODE_ID),
			 errhint("Reserved node_id range 128..255 is for forward-compatibility; "
					 "single-node Stage 1.15 only emits 0..127.")));

	cluster_scn_observe((SCN)remote_int);
	PG_RETURN_VOID();
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_scn_observe() requires --enable-cluster")));
	PG_RETURN_VOID();
#endif
}
