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
#include "cluster/cluster_guc.h" /* cluster_enabled / cluster_node_id / cluster_xid_striping */
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h" /* lazy latch (D5b) */
#include "port/pg_crc32c.h"					 /* durable stripe record integrity (D5) */

/*
 * Region-4/5 payloads must fit one voting-disk slot (CLUSTER_VOTING_
 * SLOT_BYTES == 512; the literal keeps this pure layer free of the
 * voting-disk header -- the I/O wiring cross-checks the real macro).
 */
StaticAssertDecl(sizeof(ClusterXidStripeSlotRecord) <= 512,
				 "stripe slot record must fit a 512-byte voting slot");
StaticAssertDecl(sizeof(ClusterXidStripeActivationRecord) <= 512,
				 "stripe activation record must fit a 512-byte voting slot");

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
 * Allocation-side slot gate (see header).  Pure function of the GUC
 * face and the declared node identity, so it is safe under
 * EXEC_BACKEND (children recompute instead of inheriting) and cheap
 * enough for the GetNewTransactionId hot path (two branches).
 */
int
cluster_xid_allocation_slot(void)
{
	if (!cluster_enabled || !cluster_xid_striping)
		return -1;
	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_XID_STRIDE)
		return -1;

	return cluster_node_id;
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

	if (!stripe_runtime.active) {
#ifdef USE_PGRAC_CLUSTER
		/*
		 * spec-6.15 D5b lazy latch: activation resolves after process
		 * start (the membership join gate publishes it), so latch from
		 * shmem on first use.  GUC-gated so striping-off clusters never
		 * pay the shmem lookup on this path.
		 */
		if (cluster_enabled && cluster_xid_striping)
			cluster_xid_stripe_lazy_latch();
#endif
		if (!stripe_runtime.active)
			return -1;
	}

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
 * Runtime wrapper: does xid PROVABLY derive to another node's slot?
 * True only above the activation floor with the stripe runtime latched
 * — the one case where value-space uniqueness proves the xid is not
 * ours, so PG-native CLOG / ProcArray / snapshot answers about it are
 * void (AD-012 exception 9).  Underivable (below floor / striping off /
 * non-normal) returns false: pre-striping history keeps its
 * pre-existing native treatment.
 *
 * The congruence-class short-circuit keeps the common case (our own
 * xids) free of the ReadNextFullTransactionId() shared-lock read the
 * widening needs: an own-class xid can never be provably foreign.
 */
bool
cluster_xid_provably_foreign(TransactionId xid)
{
	if (!stripe_runtime.active) {
#ifdef USE_PGRAC_CLUSTER
		if (cluster_enabled && cluster_xid_striping)
			cluster_xid_stripe_lazy_latch();
#endif
		if (!stripe_runtime.active)
			return false;
	}
	if (!TransactionIdIsNormal(xid))
		return false;
	if ((int)(xid % CLUSTER_XID_STRIDE) == stripe_runtime.my_slot)
		return false;

	return cluster_xid_origin_slot(xid) >= 0;
}

/*
 * Cheap stamp-suppression test: is xid in another node's congruence
 * class while striping is latched?  No floor proof and no widening
 * (lock-free), so a below-floor pre-striping local xid of a foreign
 * class also answers true — callers may only use this to SUPPRESS an
 * optional action (hint stamping), where over-suppression is always
 * safe and under-suppression is the pre-striping status quo.
 */
bool
cluster_xid_foreign_class_cheap(TransactionId xid)
{
	if (!stripe_runtime.active) {
#ifdef USE_PGRAC_CLUSTER
		if (cluster_enabled && cluster_xid_striping)
			cluster_xid_stripe_lazy_latch();
#endif
		if (!stripe_runtime.active)
			return false;
	}
	if (!TransactionIdIsNormal(xid))
		return false;

	return (int)(xid % CLUSTER_XID_STRIDE) != stripe_runtime.my_slot;
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

/* ============================================================
 * Durable stripe records (spec-6.15 D5, appendix B.1) -- pure
 * integrity layer (rule 15), mirroring the spec-5.15 join-commit
 * marker discipline.  Any validation failure means "treat the
 * record as absent and fail closed"; readers never guess.
 * ============================================================
 */

/* Set rec->crc32c = CRC32C over [magic .. generation]. */
void
cluster_xid_stripe_slot_record_compute_crc(ClusterXidStripeSlotRecord *rec)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, rec, offsetof(ClusterXidStripeSlotRecord, crc32c));
	FIN_CRC32C(crc);
	rec->crc32c = (uint32)crc;
}

/*
 * Structural + sanity validity of a per-slot stripe record read from
 * region-4 slot expected_node.  FAIL-CLOSED: magic / version / slot
 * identity / flag domain / zero owner / invalid floor / hwm below
 * floor / zero epoch / zero generation / CRC mismatch all reject.
 */
bool
cluster_xid_stripe_slot_record_valid(const ClusterXidStripeSlotRecord *rec, int32 expected_node)
{
	pg_crc32c crc;

	if (rec == NULL)
		return false;
	if (rec->magic != CLUSTER_PGXS_MAGIC || rec->version != CLUSTER_PGXS_VERSION)
		return false;
	if (rec->node_id < 0 || rec->node_id >= CLUSTER_XID_STRIDE || rec->node_id != expected_node)
		return false;
	if (rec->retired > 1)
		return false;
	if (rec->owner_incarnation == 0)
		return false;
	if (rec->floor_full == 0)
		return false;
	if (rec->next_xid_hwm_full < rec->floor_full)
		return false;
	if (rec->stride_mode_epoch == 0)
		return false;
	if (rec->generation == 0)
		return false;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, rec, offsetof(ClusterXidStripeSlotRecord, crc32c));
	FIN_CRC32C(crc);
	return (uint32)crc == rec->crc32c;
}

/* Set rec->crc32c = CRC32C over [magic .. generation]. */
void
cluster_xid_stripe_activation_record_compute_crc(ClusterXidStripeActivationRecord *rec)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, rec, offsetof(ClusterXidStripeActivationRecord, crc32c));
	FIN_CRC32C(crc);
	rec->crc32c = (uint32)crc;
}

/*
 * Structural + sanity validity of the cluster-wide activation record
 * (region 5).  FAIL-CLOSED on magic / version / invalid floor / zero
 * epoch / zero generation / CRC mismatch.  Epoch-rewind rejection
 * against a previously accepted record is the READER's cross-record
 * check (activation path), not a single-record property.
 */
bool
cluster_xid_stripe_activation_record_valid(const ClusterXidStripeActivationRecord *rec)
{
	pg_crc32c crc;

	if (rec == NULL)
		return false;
	if (rec->magic != CLUSTER_PGXA_MAGIC || rec->version != CLUSTER_PGXA_VERSION)
		return false;
	if (rec->activated_floor_full == 0)
		return false;
	if (rec->stride_mode_epoch == 0)
		return false;
	if (rec->generation == 0)
		return false;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, rec, offsetof(ClusterXidStripeActivationRecord, crc32c));
	FIN_CRC32C(crc);
	return (uint32)crc == rec->crc32c;
}
