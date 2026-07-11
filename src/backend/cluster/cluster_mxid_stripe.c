/*-------------------------------------------------------------------------
 *
 * cluster_mxid_stripe.c
 *	  pgrac MultiXactId space segmentation -- pure stripe derivation layer.
 *
 *	  See cluster_mxid_stripe.h for the layer contract (half-space
 *	  derivation window, fail-closed underivable semantics, latch
 *	  ownership).
 *
 *	  Everything here is deliberately pure: no shmem, no locks, no
 *	  ereport.  The only process state is the latched mxid stripe
 *	  runtime (populated once per process by the boot lazy latch).
 *	  Unlike the xid layer there is no widening step and no counter
 *	  herding: the half-space distance is computed directly against the
 *	  durable activation floor, so derivation never reads the live
 *	  nextMXact.  This keeps the whole truth table unit-testable
 *	  (test_cluster_mxid_stripe) and lets multixact.c consume the
 *	  candidate arithmetic under MultiXactGenLock without layering
 *	  concerns.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_mxid_stripe.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-7.1-cross-instance-positive-interread.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/multixact.h"
#include "cluster/cluster_guc.h" /* cluster_enabled / cluster_node_id / cluster_xid_striping */
#include "cluster/cluster_mxid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h" /* mxid lazy latch */
#include "port/pg_crc32c.h"					 /* extension record integrity */

/*
 * Process-local latched mxid stripe runtime.  Immutable after the
 * single latch call (see header); zero-initialized state means
 * "inactive" and every wrapper reports underivable.
 */
typedef struct ClusterMxidStripeRuntime {
	bool active;
	bool poisoned; /* POISONED disk state latched: sticky, dominates active */
	int my_slot;
	MultiXactId floor_mxid;
} ClusterMxidStripeRuntime;

static ClusterMxidStripeRuntime mxid_stripe_runtime = { false, false, -1, InvalidMultiXactId };

/*
 * Derive the origin slot of mxid against the mxid activation floor, or
 * -1 when underivable.  The uint32 modular distance is unambiguous
 * inside the half-space [floor, floor + 2^31) and stays exact when the
 * window spans the 2^32 wrap.
 */
int
cluster_mxid_origin_slot_floored(MultiXactId mxid, MultiXactId floor_mxid)
{
	if (mxid < FirstMultiXactId || floor_mxid < FirstMultiXactId)
		return -1;
	if ((uint32)(mxid - floor_mxid) >= CLUSTER_MXID_HALFSPACE)
		return -1;

	return (int)(mxid % CLUSTER_MXID_STRIDE);
}

/*
 * Smallest MultiXactId >= next with value in slot's congruence class,
 * skipping the InvalidMultiXactId (0) in-class position.
 */
MultiXactId
cluster_mxid_next_striped(MultiXactId next, int slot)
{
	uint32 val = (uint32)next;
	uint32 cand;

	Assert(slot >= 0 && slot < CLUSTER_MXID_STRIDE);

	/*
	 * Round up to the congruence class: add the mod-STRIDE distance
	 * from the current remainder to the target slot (0 when already in
	 * class).  uint32 addition wraps at 2^32 like the multixact
	 * counter itself.
	 */
	cand = val + (((uint32)slot - (val % CLUSTER_MXID_STRIDE)) & (CLUSTER_MXID_STRIDE - 1));

	/*
	 * The in-class position at value 0 is InvalidMultiXactId (only
	 * reachable for slot 0 right at the wrap): never issued, skip one
	 * full stride to the next in-class position.  A single skip
	 * suffices (0 + STRIDE = 16 >= FirstMultiXactId).
	 */
	if (cand < FirstMultiXactId)
		cand += CLUSTER_MXID_STRIDE;

	return (MultiXactId)cand;
}

/*
 * Guardrail predicate: candidate at/beyond floor + 2^31 (see header).
 * Invalid floor fails closed.
 */
bool
cluster_mxid_halfspace_exceeded(MultiXactId candidate, MultiXactId floor_mxid)
{
	if (floor_mxid < FirstMultiXactId)
		return true;

	return (uint32)(candidate - floor_mxid) >= CLUSTER_MXID_HALFSPACE;
}

/*
 * Runtime wrapper: derive the origin slot of a raw mxid using the
 * latched floor, or -1 when the mxid stripe runtime is inactive or
 * derivation fails.
 */
int
cluster_mxid_origin_slot(MultiXactId mxid)
{
	if (!mxid_stripe_runtime.active) {
#ifdef USE_PGRAC_CLUSTER
		/*
		 * Lazy latch mirroring cluster_xid_origin_slot: activation
		 * resolves after process start, so latch from shmem on first
		 * use.  GUC-gated so striping-off clusters never pay the shmem
		 * lookup on this path.
		 */
		if (cluster_enabled && cluster_xid_striping)
			cluster_mxid_stripe_lazy_latch();
#endif
		if (!mxid_stripe_runtime.active)
			return -1;
	}

	return cluster_mxid_origin_slot_floored(mxid, mxid_stripe_runtime.floor_mxid);
}

/*
 * Runtime wrapper: does mxid derive to this node's slot?  False
 * whenever underivable (fail-closed direction for origin-side deny).
 */
bool
cluster_mxid_is_mine(MultiXactId mxid)
{
	int slot = cluster_mxid_origin_slot(mxid);

	return slot >= 0 && slot == mxid_stripe_runtime.my_slot;
}

/*
 * Allocation-side slot gate (see header).  Unlike the xid gate this
 * also requires the latched mxid floor: without a floor the allocator
 * cannot clamp candidates above it nor police the half-space boundary,
 * so allocation stays vanilla dense (below-floor ids remain
 * underivable -- honest degrade, never a misattribution).
 */
int
cluster_mxid_allocation_slot(void)
{
	if (!cluster_enabled || !cluster_xid_striping)
		return -1;
	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_MXID_STRIDE)
		return -1;

	if (!mxid_stripe_runtime.active) {
#ifdef USE_PGRAC_CLUSTER
		cluster_mxid_stripe_lazy_latch();
#endif
		if (!mxid_stripe_runtime.active)
			return -1;
	}

	return cluster_node_id;
}

/* Latched mxid activation floor (InvalidMultiXactId until latched). */
MultiXactId
cluster_mxid_stripe_floor(void)
{
	return mxid_stripe_runtime.active ? mxid_stripe_runtime.floor_mxid : InvalidMultiXactId;
}

/*
 * Latch the process-local mxid stripe runtime.  See header for
 * ownership; inconsistent arguments latch the inactive state
 * (fail-closed).
 */
void
cluster_mxid_stripe_latch_runtime(bool active, int my_slot, MultiXactId floor_mxid)
{
	if (active && (my_slot < 0 || my_slot >= CLUSTER_MXID_STRIDE || floor_mxid < FirstMultiXactId))
		active = false;
	/* A poisoned runtime never activates (sticky; see header). */
	if (mxid_stripe_runtime.poisoned)
		active = false;

	mxid_stripe_runtime.active = active;
	mxid_stripe_runtime.my_slot = active ? my_slot : -1;
	mxid_stripe_runtime.floor_mxid = active ? floor_mxid : InvalidMultiXactId;
}

/*
 * Latch the poisoned mxid stripe runtime (spec-7.1 integration review,
 * P0).  Sticky for the life of the process: derivation stays
 * underivable (foreign reads keep their 53R9C fail-closed boundary)
 * and the allocation gate below refuses instead of degrading to the
 * vanilla dense allocator.
 */
void
cluster_mxid_stripe_latch_poisoned(void)
{
	mxid_stripe_runtime.poisoned = true;
	mxid_stripe_runtime.active = false;
	mxid_stripe_runtime.my_slot = -1;
	mxid_stripe_runtime.floor_mxid = InvalidMultiXactId;
}

/*
 * Runtime poisoned query (allocation-side consumer: GetNewMultiXactId
 * must refuse rather than allocate dense).  Retries the boot lazy
 * latch until the runtime settles either active or poisoned, mirroring
 * cluster_mxid_origin_slot.
 */
bool
cluster_mxid_stripe_poisoned(void)
{
	if (!mxid_stripe_runtime.active && !mxid_stripe_runtime.poisoned) {
#ifdef USE_PGRAC_CLUSTER
		if (cluster_enabled && cluster_xid_striping)
			cluster_mxid_stripe_lazy_latch();
#endif
	}
	return mxid_stripe_runtime.poisoned;
}

/*
 * cluster_mxid_stripe_resolve_floor — fold per-copy "PGXM" evidence
 * into one verdict (pure; spec-7.1 integration review, P0).
 *
 *	ev[i] describes the extension area of one valid-PGXA activation
 *	copy: the PGXA generation it rides, its read class, and (when
 *	VALID) its floor.  Verdict rules, in dominance order:
 *
 *	  1. two VALID extensions with different floors    -> POISONED
 *	     (the floor is written once with the activation seed and never
 *	     changes; two coherent records disagreeing cannot come from a
 *	     torn write -- each carries its own CRC).
 *	  2. a VALID extension exists, but no copy at the NEWEST PGXA
 *	     generation carries one                        -> POISONED
 *	     (activation cannot regress to absent: adopting the newest
 *	     copy's "absent" would silently re-open dense allocation while
 *	     other nodes stay striped -- the probe-F misattribution class).
 *	  3. a VALID extension at the newest generation     -> VALID
 *	     (an ABSENT/CORRUPT sibling copy is torn-write damage the
 *	     valid CRC'd record repairs, same tolerance as the PGXA face).
 *	  4. no VALID anywhere but CORRUPT bytes somewhere  -> POISONED
 *	     ("never activated" is unprovable under the damage; treating
 *	     it as absent could seed a second, different floor).
 *	  5. all ABSENT                                     -> ABSENT.
 */
ClusterMxidFloorResolve
cluster_mxid_stripe_resolve_floor(const ClusterMxidFloorEvidence *ev, int n, uint32 *out_floor)
{
	uint64 wgen = 0;
	bool any_valid = false;
	bool any_corrupt = false;
	bool valid_at_wgen = false;
	bool have_floor = false;
	bool floor_conflict = false;
	uint32 floor = 0;
	int i;

	if (out_floor)
		*out_floor = 0;
	if (ev == NULL || n <= 0)
		return CLUSTER_MXID_FLOOR_ABSENT;

	for (i = 0; i < n; i++)
		if (ev[i].generation > wgen)
			wgen = ev[i].generation;

	for (i = 0; i < n; i++) {
		switch (ev[i].ext_class) {
		case CLUSTER_MXID_EXT_VALID:
			any_valid = true;
			if (ev[i].generation == wgen)
				valid_at_wgen = true;
			if (!have_floor) {
				floor = ev[i].floor;
				have_floor = true;
			} else if (floor != ev[i].floor)
				floor_conflict = true;
			break;
		case CLUSTER_MXID_EXT_CORRUPT:
			any_corrupt = true;
			break;
		default:
			break;
		}
	}

	if (floor_conflict)
		return CLUSTER_MXID_FLOOR_POISONED;
	if (any_valid && !valid_at_wgen)
		return CLUSTER_MXID_FLOOR_POISONED;
	if (any_valid) {
		if (out_floor)
			*out_floor = floor;
		return CLUSTER_MXID_FLOOR_VALID;
	}
	if (any_corrupt)
		return CLUSTER_MXID_FLOOR_POISONED;
	return CLUSTER_MXID_FLOOR_ABSENT;
}

/* Set rec->crc32c = CRC32C over [magic .. generation]. */
void
cluster_mxid_stripe_extension_record_compute_crc(ClusterMxidStripeExtensionRecord *rec)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, rec, offsetof(ClusterMxidStripeExtensionRecord, crc32c));
	FIN_CRC32C(crc);
	rec->crc32c = (uint32)crc;
}

/*
 * Structural + sanity validity of the mxid activation extension record
 * (region-6 slot, byte offset CLUSTER_PGXM_SLOT_OFFSET).  FAIL-CLOSED
 * on magic / version / invalid floor / zero generation / CRC mismatch;
 * an all-zeros extension area (slot written by a pre-extension binary)
 * rejects on the magic check -- the record-absent empty state.
 */
bool
cluster_mxid_stripe_extension_record_valid(const ClusterMxidStripeExtensionRecord *rec)
{
	pg_crc32c crc;

	if (rec == NULL)
		return false;
	if (rec->magic != CLUSTER_PGXM_MAGIC || rec->version != CLUSTER_PGXM_VERSION)
		return false;
	if (rec->activated_mxid_floor < FirstMultiXactId)
		return false;
	if (rec->generation == 0)
		return false;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, rec, offsetof(ClusterMxidStripeExtensionRecord, crc32c));
	FIN_CRC32C(crc);
	return (uint32)crc == rec->crc32c;
}
