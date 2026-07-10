/*-------------------------------------------------------------------------
 *
 * cluster_undo_horizon.c
 *	  Cluster-wide undo retention horizon: pure fold from per-peer report
 *	  views to a recycle floor (spec-5.22e D5-1).
 *
 *	  The fold implements the prevention half of the workstream hard
 *	  invariant #4 ("the undo cleaner must not unilaterally recycle a
 *	  slot a peer may still need"): the cleaner may only advance recycling
 *	  past watermarks strictly below the CLUSTER floor, and the floor is
 *	  only proven when every required MEMBER peer has a report that is
 *	  capable, present, stable, current-epoch, well-formed, monotone and
 *	  fresh.  Anything unproven STALLS the fold -- the caller pauses
 *	  reclaim entirely (a stall is always safe; using an unproven floor
 *	  never is).  There is deliberately NO fallback to the local horizon
 *	  on any unproven edge (spec-5.22e Q3'': a required peer without a
 *	  current-connection capability may be an old binary that still holds
 *	  old snapshots -- falling back to local recycling would reopen the
 *	  exact mis-recycle window this spec closes).
 *
 *	  Everything here is a pure decision layer: no shmem, no locks, no
 *	  GUC reads, unit-pinned by the U1-U16 truth table.  The heavy caller
 *	  (the undo cleaner pass, D5-3) samples the per-peer seqlock slots and
 *	  the required MEMBER bitmap from one membership/epoch snapshot and
 *	  threads the returned {scn, epoch} floor through the pass, re-checking
 *	  the epoch before every mutation (F-D2 epoch fence).
 *
 *	  SCN comparisons all go through scn_time_cmp (L457/AD-008: raw
 *	  integer comparison of SCNs is forbidden; only the local_scn
 *	  time-axis is comparable across nodes).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_undo_horizon.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.22e-undo-cluster-retention-horizon.md (D5-1, §2.1/§3.0)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_undo_horizon.h"

static inline bool
required_has(const uint8 *required, int node)
{
	return (required[node >> 3] & (uint8)(1u << (node & 7))) != 0;
}

static inline uint32
clamp_interval_ms(uint32 iv)
{
	if (iv < CLUSTER_UNDO_HORIZON_INTERVAL_MIN_MS)
		return CLUSTER_UNDO_HORIZON_INTERVAL_MIN_MS;
	if (iv > CLUSTER_UNDO_HORIZON_INTERVAL_MAX_MS)
		return CLUSTER_UNDO_HORIZON_INTERVAL_MAX_MS;
	return iv;
}

/*
 * cluster_undo_horizon_cluster_floor
 *
 *	Fold the local horizon and every required peer's report view into the
 *	cluster recycle floor.  See the header contract; the per-peer check
 *	order (NOCAP -> MISSING -> TORN -> EPOCH -> MALFORMED -> REGRESSION ->
 *	STALE, first failing peer in ascending node id wins the blame) is part
 *	of the contract so attribution is deterministic (U16b).
 *
 *	Returns OK and fills *out_floor = {min, current_epoch}; or STALLED and
 *	fills *out_reason / *out_blame_node.  On STALLED, *out_floor is set to
 *	{InvalidScn, current_epoch} so a caller that ignores the status cannot
 *	accidentally recycle against a live value (belt for rule 8.A; the
 *	cleaner checks the status first).
 */
ClusterUndoHorizonFoldStatus
cluster_undo_horizon_cluster_floor(SCN local_horizon, const ClusterUndoHorizonReportView *views,
								   int nviews, const uint8 *required, int32 self_node,
								   uint64 current_epoch, uint64 now_us, uint32 local_interval_ms,
								   ClusterUndoHorizonFloor *out_floor,
								   ClusterUndoHorizonStallReason *out_reason, int32 *out_blame_node)
{
	SCN floor_scn = local_horizon;
	int node;

	Assert(out_floor != NULL && out_reason != NULL && out_blame_node != NULL);

	out_floor->scn = InvalidScn;
	out_floor->epoch = current_epoch;
	*out_reason = CLUSTER_UNDO_HORIZON_STALL_NONE;
	*out_blame_node = -1;

	/*
	 * A fold with an unusable local horizon cannot prove anything: the
	 * caller is expected to hand us the spec-3.12 own-instance value,
	 * which is valid whenever the storage gate is on.  Defensive stall,
	 * blamed on self (U7b) -- never "recycle a little anyway".
	 */
	if (local_horizon == InvalidScn) {
		*out_reason = CLUSTER_UNDO_HORIZON_STALL_MALFORMED;
		*out_blame_node = self_node;
		return CLUSTER_UNDO_HORIZON_FOLD_STALLED;
	}

	for (node = 0; node < nviews; node++) {
		const ClusterUndoHorizonReportView *v = &views[node];
		uint64 window_us;
		uint32 iv;

		if (node == self_node)
			continue; /* local_horizon covers self */
		if (!required_has(required, node))
			continue; /* ghost / non-MEMBER slot: ignored (U8) */

		/*
		 * Q3'' (F-D1): a required MEMBER without a CURRENT-connection
		 * capability stalls the fold.  Capability gates sending and
		 * consumer admission elsewhere; here its absence just means this
		 * peer's coverage cannot be proven -- which is a stall, never a
		 * "fall back to local recycling".
		 */
		if (!v->has_capability) {
			*out_reason = CLUSTER_UNDO_HORIZON_STALL_NOCAP;
			*out_blame_node = node;
			return CLUSTER_UNDO_HORIZON_FOLD_STALLED;
		}
		if (!v->valid) {
			*out_reason = CLUSTER_UNDO_HORIZON_STALL_MISSING;
			*out_blame_node = node;
			return CLUSTER_UNDO_HORIZON_FOLD_STALLED;
		}
		if (!v->stable) {
			*out_reason = CLUSTER_UNDO_HORIZON_STALL_TORN;
			*out_blame_node = node;
			return CLUSTER_UNDO_HORIZON_FOLD_STALLED;
		}
		if (v->epoch != current_epoch) {
			*out_reason = CLUSTER_UNDO_HORIZON_STALL_EPOCH;
			*out_blame_node = node;
			return CLUSTER_UNDO_HORIZON_FOLD_STALLED;
		}

		/*
		 * Well-formedness: the wire handler (D5-2) already rejects these
		 * before publishing; the fold re-checks defensively so a slot
		 * corrupted by any future path still fails closed.
		 */
		if (v->horizon_scn == InvalidScn
			|| v->sender_interval_ms < CLUSTER_UNDO_HORIZON_INTERVAL_MIN_MS
			|| v->sender_interval_ms > CLUSTER_UNDO_HORIZON_INTERVAL_MAX_MS) {
			*out_reason = CLUSTER_UNDO_HORIZON_STALL_MALFORMED;
			*out_blame_node = node;
			return CLUSTER_UNDO_HORIZON_FOLD_STALLED;
		}

		/*
		 * Monotonicity violation latch (S3.0 corollary): the peer sent a
		 * same-epoch report BELOW its previously accepted value, i.e. a
		 * snapshot below the accepted lower bound exists over there.  The
		 * old (higher) slot value is exactly what must NOT be consumed;
		 * stall until the peer publishes a conforming report again.
		 */
		if (v->regression_flagged) {
			*out_reason = CLUSTER_UNDO_HORIZON_STALL_REGRESSION;
			*out_blame_node = node;
			return CLUSTER_UNDO_HORIZON_FOLD_STALLED;
		}

		/*
		 * Freshness: window = FACTOR * max(sender, local) interval, in
		 * uint64 microseconds (no overflow: 3 * 60000 * 1000 << 2^64).
		 * The receiver-local recv_at makes this clock-skew safe; a
		 * recv_at in the future means the receiving clock went backwards
		 * -- treat as stale, never fresh (U10).
		 */
		iv = Max(clamp_interval_ms(v->sender_interval_ms), clamp_interval_ms(local_interval_ms));
		window_us = (uint64)CLUSTER_UNDO_HORIZON_FRESHNESS_FACTOR * (uint64)iv * (uint64)1000;
		if (v->recv_at_us > now_us || now_us - v->recv_at_us > window_us) {
			*out_reason = CLUSTER_UNDO_HORIZON_STALL_STALE;
			*out_blame_node = node;
			return CLUSTER_UNDO_HORIZON_FOLD_STALLED;
		}

		/* proven: fold the peer's bound in (time-axis min, L457) */
		if (scn_time_cmp(v->horizon_scn, floor_scn) < 0)
			floor_scn = v->horizon_scn;
	}

	out_floor->scn = floor_scn;
	out_floor->epoch = current_epoch;
	return CLUSTER_UNDO_HORIZON_FOLD_OK;
}

const char *
cluster_undo_horizon_stall_reason_name(ClusterUndoHorizonStallReason reason)
{
	switch (reason) {
	case CLUSTER_UNDO_HORIZON_STALL_NONE:
		return "none";
	case CLUSTER_UNDO_HORIZON_STALL_NOCAP:
		return "nocap";
	case CLUSTER_UNDO_HORIZON_STALL_MISSING:
		return "missing";
	case CLUSTER_UNDO_HORIZON_STALL_TORN:
		return "torn";
	case CLUSTER_UNDO_HORIZON_STALL_EPOCH:
		return "epoch";
	case CLUSTER_UNDO_HORIZON_STALL_MALFORMED:
		return "malformed";
	case CLUSTER_UNDO_HORIZON_STALL_REGRESSION:
		return "regression";
	case CLUSTER_UNDO_HORIZON_STALL_STALE:
		return "stale";
	}
	return "unknown";
}
