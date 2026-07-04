/*-------------------------------------------------------------------------
 *
 * cluster_xid_stripe.c
 *	  pgrac xid space segmentation -- pure stripe derivation layer.
 *
 *	  See cluster_xid_stripe.h for the layer contract (signed widening
 *	  window, fail-closed underivable semantics, latch ownership).
 *
 *	  Everything here is deliberately pure: no shmem, no locks, no
 *	  ereport.  The only process state is the latched stripe runtime
 *	  (populated once at startup by the activation / join path); the
 *	  only backend dependency is ReadNextFullTransactionId() inside the
 *	  runtime wrappers.  This keeps the whole truth table unit-testable
 *	  (test_cluster_xid_stripe) and lets varsup.c consume the candidate
 *	  arithmetic under XidGenLock without layering concerns.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_xid_stripe.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-6.15-xid-space-segmentation.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/transam.h"
#include "cluster/cluster_xid_stripe.h"

/*
 * Process-local latched stripe runtime.  Immutable after the single
 * latch call at process startup (see header); zero-initialized state
 * means "inactive" and every wrapper reports underivable.
 */
typedef struct ClusterXidStripeRuntime {
	bool active;
	int my_slot;
	FullTransactionId floor_full;
} ClusterXidStripeRuntime;

static ClusterXidStripeRuntime stripe_runtime = { false, -1, { 0 } };

/*
 * Widen a raw xid to full form in the signed +/- 2^31 window around
 * next_full.  See header for the window rationale.
 */
FullTransactionId
cluster_xid_widen(TransactionId xid, FullTransactionId next_full)
{
	uint64 next_val;
	int32 diff;

	if (!TransactionIdIsNormal(xid))
		return InvalidFullTransactionId;
	if (!FullTransactionIdIsValid(next_full))
		return InvalidFullTransactionId;

	next_val = U64FromFullTransactionId(next_full);

	/*
	 * Signed 32-bit distance from next_full's low word.  Because the
	 * distance is computed mod 2^32, next_val + diff always has xid as
	 * its low 32 bits; the sign picks the unique interpretation inside
	 * the +/- 2^31 window.
	 */
	diff = (int32)(xid - XidFromFullTransactionId(next_full));

	if (diff >= 0)
		return FullTransactionIdFromU64(next_val + (uint64)diff);

	/*
	 * Behind-window interpretation would precede full xid 0: such a
	 * live xid cannot exist -- fail closed.  (The one-sided upstream
	 * idiom, xid8funcs.c widen_snapshot_xid, does not guard this;
	 * the stripe layer must, per spec §3.2.)
	 */
	if (next_val < (uint64)(-(int64)diff))
		return InvalidFullTransactionId;

	return FullTransactionIdFromU64(next_val - (uint64)(-(int64)diff));
}

/*
 * Derive the origin slot of fxid against the activation floor, or -1
 * when underivable.
 */
int
cluster_xid_origin_slot_full(FullTransactionId fxid, FullTransactionId floor_full)
{
	if (!FullTransactionIdIsValid(fxid) || !FullTransactionIdIsValid(floor_full))
		return -1;
	if (FullTransactionIdPrecedes(fxid, floor_full))
		return -1;

	return (int)(XidFromFullTransactionId(fxid) % CLUSTER_XID_STRIDE);
}

/*
 * Smallest FullTransactionId >= next_full with 32-bit value in slot's
 * congruence class, skipping special-value positions (0-2).
 */
FullTransactionId
cluster_xid_next_striped_full(FullTransactionId next_full, int slot)
{
	uint64 val = U64FromFullTransactionId(next_full);
	uint32 rem;
	uint64 cand;

	Assert(slot >= 0 && slot < CLUSTER_XID_STRIDE);

	/*
	 * Round up to the congruence class: add the mod-STRIDE distance
	 * from the current remainder to the target slot (0 when already in
	 * class).  64-bit addition carries into the epoch naturally when
	 * the rounding crosses 2^32.
	 */
	rem = (uint32)(val % CLUSTER_XID_STRIDE);
	cand = val + (((uint32)slot - rem) & (CLUSTER_XID_STRIDE - 1));

	/*
	 * In-class positions whose 32-bit value is a special xid (0-2,
	 * only reachable for slots 0-2 right after an epoch carry) are
	 * never issued: skip one full stride to the next in-class
	 * position.  A single skip suffices (slot + STRIDE >= 16 > 2).
	 */
	if ((uint32)cand < FirstNormalTransactionId)
		cand += CLUSTER_XID_STRIDE;

	return FullTransactionIdFromU64(cand);
}

/*
 * Herding predicate: cluster_max leads local_next by strictly more
 * than threshold (see header).
 */
bool
cluster_xid_lag_exceeds(FullTransactionId local_next, FullTransactionId cluster_max,
						uint64 threshold)
{
	uint64 local_val = U64FromFullTransactionId(local_next);
	uint64 max_val = U64FromFullTransactionId(cluster_max);

	if (max_val <= local_val)
		return false;

	return (max_val - local_val) > threshold;
}

/*
 * Runtime wrapper: derive the origin slot of a raw xid using the
 * latched floor and the live local nextXid, or -1 when the stripe
 * runtime is inactive or derivation fails.
 */
int
cluster_xid_origin_slot(TransactionId xid)
{
	FullTransactionId fxid;

	if (!stripe_runtime.active)
		return -1;

	fxid = cluster_xid_widen(xid, ReadNextFullTransactionId());

	return cluster_xid_origin_slot_full(fxid, stripe_runtime.floor_full);
}

/*
 * Runtime wrapper: does xid derive to this node's slot?  False
 * whenever underivable (fail-closed direction for origin-side deny).
 */
bool
cluster_xid_is_mine(TransactionId xid)
{
	int slot = cluster_xid_origin_slot(xid);

	return slot >= 0 && slot == stripe_runtime.my_slot;
}

/*
 * Latch the process-local stripe runtime.  See header for ownership;
 * inconsistent arguments latch the inactive state (fail-closed).
 */
void
cluster_xid_stripe_latch_runtime(bool active, int my_slot, FullTransactionId floor_full)
{
	if (active
		&& (my_slot < 0 || my_slot >= CLUSTER_XID_STRIDE || !FullTransactionIdIsValid(floor_full)))
		active = false;

	stripe_runtime.active = active;
	stripe_runtime.my_slot = active ? my_slot : -1;
	stripe_runtime.floor_full = active ? floor_full : InvalidFullTransactionId;
}
