/*-------------------------------------------------------------------------
 *
 * cluster_undo_horizon.h
 *	  Cluster-wide undo retention horizon: per-peer report views and the
 *	  pure fold that turns them into a recycle floor (spec-5.22e D5-1).
 *
 *	  The local retention horizon (cluster_undo_retention_horizon(),
 *	  spec-3.12) covers only this node's ProcArray.  Under cross-node undo
 *	  consumption (live verdict / CP3 / fresh-ref / authority serve) a
 *	  peer's snapshot can reference this node's TT slots, so the cleaner
 *	  must not recycle past the CLUSTER floor: the scn_time_cmp-min of the
 *	  local horizon and every required MEMBER peer's accepted horizon
 *	  report.  When any required peer's report cannot be proven fresh,
 *	  consistent and monotone at the current reconfig epoch, the fold
 *	  STALLS: the caller must not advance recycling at all (fail-closed
 *	  direction: a stall pauses reclaim, it never mis-recycles).
 *
 *	  Everything in this header is a pure decision layer: no shmem, no
 *	  locks, no GUC reads.  The heavy caller (undo cleaner pass) samples
 *	  the per-peer seqlock slots into ClusterUndoHorizonReportView[] and
 *	  the required MEMBER bitmap from the same membership/epoch snapshot,
 *	  then calls the fold.  Mirrors the spec-5.22d D4-1 pure-layer shape
 *	  (ClusterUndoAuthorityInput).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_undo_horizon.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.22e-undo-cluster-retention-horizon.md (D5-1, §2.1)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_HORIZON_H
#define CLUSTER_UNDO_HORIZON_H

#include "cluster/cluster_reconfig.h" /* CLUSTER_RECONFIG_DEAD_BITMAP_BYTES */
#include "cluster/cluster_scn.h"	  /* SCN / scn_time_cmp */

/*
 * Freshness factor: a required peer's report is fresh while
 *	  now - recv_at <= FACTOR * max(sender_interval, local_interval).
 * Fixed at compile time (spec-5.22e Q5': a tunable would add a
 * misconfiguration surface -- window < interval = permanent stall -- with
 * no benefit).  Intervals are clamped to [MIN,MAX] ms before the window
 * is computed; the window math is done in uint64 microseconds.
 */
#define CLUSTER_UNDO_HORIZON_FRESHNESS_FACTOR 3
#define CLUSTER_UNDO_HORIZON_INTERVAL_MIN_MS 100
#define CLUSTER_UNDO_HORIZON_INTERVAL_MAX_MS 60000

typedef enum ClusterUndoHorizonFoldStatus {
	CLUSTER_UNDO_HORIZON_FOLD_OK = 0,
	CLUSTER_UNDO_HORIZON_FOLD_STALLED
} ClusterUndoHorizonFoldStatus;

/*
 * Stall attribution, surfaced through the fold's out_reason/out_blame so
 * the caller can count and LOG-once with a concrete culprit.  Per-peer
 * check order (first failing required peer, ascending node id, wins the
 * blame): NOCAP -> MISSING -> TORN -> EPOCH -> MALFORMED -> REGRESSION ->
 * STALE.
 */
typedef enum ClusterUndoHorizonStallReason {
	CLUSTER_UNDO_HORIZON_STALL_NONE = 0,
	CLUSTER_UNDO_HORIZON_STALL_NOCAP,	   /* required peer lacks a
											 * current-connection
											 * UNDO_HORIZON_V1 capability;
											 * NEVER a fallback to the
											 * local horizon (Q3'') */
	CLUSTER_UNDO_HORIZON_STALL_MISSING,	   /* no report this incarnation */
	CLUSTER_UNDO_HORIZON_STALL_TORN,	   /* seqlock double-read failed */
	CLUSTER_UNDO_HORIZON_STALL_EPOCH,	   /* report epoch != current */
	CLUSTER_UNDO_HORIZON_STALL_MALFORMED,  /* invalid SCN / interval out
											 * of range / invalid local
											 * horizon */
	CLUSTER_UNDO_HORIZON_STALL_REGRESSION, /* peer violated same-epoch
											 * report monotonicity; the
											 * previously accepted (higher)
											 * value must NOT be used
											 * (spec-5.22e S3.0 corollary) */
	CLUSTER_UNDO_HORIZON_STALL_STALE	   /* report older than the
											 * freshness window, or
											 * recv_at in the future */
} ClusterUndoHorizonStallReason;

/*
 * The recycle floor: scn paired with the reconfig epoch it was proven at.
 * The cleaner threads this pair through the whole pass and re-verifies
 * epoch equality before every TT-slot FREE and inside the segment
 * mutation lock (spec-5.22e F-D2 epoch fence); a mismatch aborts the
 * pass immediately.
 */
typedef struct ClusterUndoHorizonFloor {
	SCN scn;
	uint64 epoch;
} ClusterUndoHorizonFloor;

/*
 * One peer's report as sampled by the heavy caller from the seqlock slot.
 *
 *	valid              -- a report was accepted this incarnation.
 *	stable             -- the seqlock double-read converged (false after
 *	                      bounded retries => TORN, U12).
 *	has_capability     -- the peer advertised UNDO_HORIZON_V1 on its
 *	                      CURRENT connection (Q1' amend: capability dies
 *	                      with the connection, no stale reuse).
 *	regression_flagged -- the receive handler rejected a same-epoch
 *	                      regressing report and latched the violation;
 *	                      cleared when a conforming report is accepted.
 *	epoch              -- sender's reconfig epoch at sampling.
 *	horizon_scn        -- sender's published lower bound (S2.1 sampling
 *	                      rule: min of its ProcArray floor and its
 *	                      origin-bound).
 *	recv_at_us         -- RECEIVER-local clock at accept (clock-skew safe).
 *	sender_interval_ms -- sender's lmon_main_loop_interval, carried in the
 *	                      wire payload so a slow-tick sender is not
 *	                      misjudged stale by a fast-tick receiver (U13).
 */
typedef struct ClusterUndoHorizonReportView {
	bool valid;
	bool stable;
	bool has_capability;
	bool regression_flagged;
	uint64 epoch;
	SCN horizon_scn;
	uint64 recv_at_us;
	uint32 sender_interval_ms;
} ClusterUndoHorizonReportView;

/*
 * Pure cluster-floor fold (spec-5.22e S2.1).
 *
 *	required   -- 128-bit bitmap of node ids whose reports are mandatory:
 *	              the membership_state==MEMBER peer set sampled at
 *	              current_epoch from the same membership snapshot
 *	              (INV-J8; ABSENT/JOINING/DEAD/REMOVED are excluded --
 *	              their cross-node consumption is refused by the D5-8
 *	              read-admission gate, so they need no coverage here).
 *	              self_node is skipped (local_horizon covers self).
 *	views      -- indexed by node id, nviews entries; ghost entries for
 *	              nodes outside `required` are ignored (U8).
 *
 *	Returns OK with *out_floor = {scn_time_cmp-min(local, required
 *	reports), current_epoch} when every required peer has a stable,
 *	capable, current-epoch, well-formed, monotone, fresh report.  An
 *	empty required set yields {local_horizon, current_epoch} (single
 *	node / cold formation: U1/U11).  Any unproven required peer =>
 *	STALLED with attribution; the caller MUST NOT advance recycling
 *	(rule 8.A: unproven is never treated as proven).
 */
extern ClusterUndoHorizonFoldStatus cluster_undo_horizon_cluster_floor(
	SCN local_horizon, const ClusterUndoHorizonReportView *views, int nviews,
	const uint8 *required, /* CLUSTER_RECONFIG_DEAD_BITMAP_BYTES */
	int32 self_node, uint64 current_epoch, uint64 now_us, uint32 local_interval_ms,
	ClusterUndoHorizonFloor *out_floor, ClusterUndoHorizonStallReason *out_reason,
	int32 *out_blame_node);

/* Attribution name for LOG-once / dump lines ("nocap", "stale", ...). */
extern const char *cluster_undo_horizon_stall_reason_name(ClusterUndoHorizonStallReason reason);

/*
 * Wire payload (PGRAC_IC_MSG_UNDO_HORIZON, D5-2): 20 bytes packed.  The
 * sender's lmon_main_loop_interval rides along so a slow-tick sender is
 * not misjudged stale by a fast-tick receiver (freshness window =
 * FACTOR * max(sender, local)).  The receive handler validates length,
 * sender, SCN, interval range and same-epoch monotonicity before
 * publishing (Q5' amend); invalid frames are counted, never published.
 */
typedef struct pg_attribute_packed() ClusterUndoHorizonWire {
	uint64 epoch;			   /* sender's reconfig epoch at sampling */
	uint64 horizon_scn;		   /* S2.1 sampling rule output */
	uint32 sender_interval_ms; /* validated to [MIN,MAX] */
} ClusterUndoHorizonWire;

/* ---- heavy path (cluster_undo_horizon_ic.c; LMON + cleaner) --------- */

extern void cluster_undo_horizon_shmem_register(void);
extern void cluster_undo_horizon_register_ic_msg_types(void);
extern void cluster_undo_horizon_lmon_tick(void);

extern int cluster_undo_horizon_sample_views(ClusterUndoHorizonReportView *views, int maxviews);
extern bool cluster_undo_horizon_required_members(uint8 *required, uint64 *out_epoch);
/* F-D2 epoch fence (recycle mutation points; injection-forceable, t/370 L6) */
extern bool cluster_undo_horizon_epoch_fence_tripped(uint64 expected_epoch);
/* D5-8 read admission: self-MEMBER capture + consumer-side enforce (53R60 on
 * not-member / pre-join snapshot; false = mixed-capability, caller keeps its
 * UNKNOWN/53R97 fail-closed shape).  Foreign arms only; own reads ungated. */
extern void cluster_undo_horizon_note_self_member(void);
extern bool cluster_undo_horizon_read_admission_enforce(SCN read_scn);

/* observability (D5-5) */
extern void cluster_undo_horizon_note_stall(void);
extern uint64 cluster_undo_horizon_stall_count(void);
extern void cluster_undo_horizon_note_peer_stale(void);
extern uint64 cluster_undo_horizon_peer_stale_count(void);
extern void cluster_undo_horizon_note_pass_abort(void);
extern uint64 cluster_undo_horizon_pass_abort_count(void);
extern void cluster_undo_horizon_note_wire_reject(void);
extern uint64 cluster_undo_horizon_wire_reject_count(void);
extern void cluster_undo_horizon_note_admission_refuse(void);
extern uint64 cluster_undo_horizon_admission_refuse_count(void);
extern void cluster_undo_horizon_note_floor(SCN scn);
extern SCN cluster_undo_horizon_last_floor(void);

#endif /* CLUSTER_UNDO_HORIZON_H */
