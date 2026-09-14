/*-------------------------------------------------------------------------
 *
 * cluster_tt_slot.c
 *	  pgrac per-undo-segment TT slot allocator (spec-3.4b D3).
 *
 *	  Shmem-backed allocator that hands out slot offsets in [0, 47] for
 *	  each active undo segment.  spec-3.4b MVP uses a single active
 *	  segment per node, so the shmem array is dimensioned by node_id
 *	  rather than by all possible segment_ids — current callers always
 *	  pass `segment_id == cluster_undo_active_segment_for_node_or_create(
 *	  cluster_node_id)` which derives back to the current node.  Stage 4+
 *	  multi-active-segment support can extend the shmem layout without
 *	  breaking the public alloc/free/get_wrap API.
 *
 *	  Allocation policy (three-tier, L189 recycle):
 *	    1) reuse a slot already owned by `top_xid`  (idempotent)
 *	    2) take any FREE slot
 *	    3) recycle a COMMITTED or ABORTED slot, wrap++
 *	  Returns INVALID_TT_SLOT_OFFSET when all slots are ACTIVE.
 *
 *	  In spec-3.4b MVP, commit and abort paths both call
 *	  cluster_tt_slot_free, so the in-shmem state machine effectively
 *	  toggles between FREE and ACTIVE.  COMMITTED / ABORTED status
 *	  recognition is wired so that spec-3.4c delayed cleanout can extend
 *	  this allocator without changing the public API.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.4b-real-tt-allocator-uba-encoding-production-cross-node.md
 *       (v0.3 FROZEN 2026-05-24)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_tt_slot.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/transam.h"
#include "cluster/cluster_guc.h"   /* cluster_undo_retention_horizon_enabled */
#include "cluster/cluster_mode.h"  /* cluster_peer_mode_enabled */
#include "cluster/cluster_scn.h"   /* SCN_MAX_VALID_NODE_ID */
#include "cluster/cluster_shmem.h" /* ClusterShmemRegion */
#include "cluster/cluster_terminal_ref_census.h" /* L11/L12 release sample */
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_undo_retention.h"		/* horizon + recyclable predicate */
#include "cluster/cluster_undo_cleaner.h"		/* spec-3.13 D2-A gc pass + stats */
#include "storage/spin.h"						/* protected-slot map lock (spec-3.15 D6) */
#include "cluster/storage/cluster_undo_alloc.h" /* CLUSTER_UNDO_SEGS_PER_INSTANCE */
#include "cluster/cluster_undo_horizon.h"		/* epoch fence (spec-5.22e F-D2) */
#include "miscadmin.h"
#include "port/atomics.h" /* spec-3.12 D5 retention counters */
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"


/*
 * Per-slot allocator state.  Distinct from the on-disk TTSlot ABI (see
 * cluster_tt_slot.h); allocator state tracks only what's needed to pick
 * the next free slot and recycle completed ones.
 *
 * The ClusterTTSlotAllocStatus enum (CTS_FREE / ACTIVE / COMMITTED / ABORTED)
 * now lives in cluster_tt_slot.h so the pure retention predicate
 * cluster_tt_slot_recyclable() can name its values (spec-3.12 D2).
 *
 * spec-3.12 D2: commit_scn is recorded here at commit so the retention gate
 * (cluster_tt_slot_alloc Pass 2) can keep a COMMITTED slot alive while a
 * reader's read_scn still needs the durable segment-header TT slot.  It is
 * InvalidScn for every status other than CTS_COMMITTED.  This is shmem-only
 * allocator state, NOT the on-disk TTSlot ABI, so growing the entry does not
 * change any on-disk format or catversion.
 */
typedef struct ClusterTTSlotAllocEntry {
	TransactionId xid; /* InvalidTransactionId when CTS_FREE */
	uint16 wrap;	   /* reuse counter (L189) */
	uint8 status;	   /* ClusterTTSlotAllocStatus */
	uint8 _pad;		   /* explicit padding before commit_scn */
	SCN commit_scn;	   /* spec-3.12 D2: commit_scn when CTS_COMMITTED; else InvalidScn */
} ClusterTTSlotAllocEntry;

StaticAssertDecl(sizeof(ClusterTTSlotAllocEntry) == 16,
				 "spec-3.12 D2: allocator entry is 16 bytes (8B header + 8B commit_scn) "
				 "for predictable shmem sizing");


typedef struct ClusterTTSlotAllocPerSegment {
	LWLock lock;
	uint32 segment_id; /* 0 = not yet initialised; otherwise == derived id */
	/* Monotonic binding identity.  It changes on every initial bind/rebind so
	 * the two-phase GC cannot accept an away-and-back segment ABA whose slot
	 * bytes happen to match its pre-I/O snapshot. */
	uint64 binding_generation;
	ClusterTTSlotAllocEntry slots[TT_SLOTS_PER_SEGMENT];
} ClusterTTSlotAllocPerSegment;


#define CLUSTER_TT_SLOT_MAX_NODES 128 /* matches SCN_MAX_VALID_NODE_ID + 1 */

typedef struct ClusterTTSlotShmem {
	/*
	 * spec-3.12 D5 retention observability (lock-free atomics; updated on the
	 * alloc slow path, not per-DML).  retention_horizon_scn is a GAUGE sampled
	 * at the recycle-decision point (C16); the others are monotonic event
	 * counters.
	 */
	pg_atomic_uint64 retention_horizon_scn;		/* last sampled horizon (gauge) */
	pg_atomic_uint64 tt_slot_retain_skip_count; /* COMMITTED slot kept (>= horizon) */
	pg_atomic_uint64 retention_recycle_count;	/* COMMITTED slot recycled (< horizon) */

	/*
	 * spec-3.22: a COMMITTED slot recycled while the retention gate was NOT
	 * enforcing (GUC off, or an InvalidScn horizon) -- an UNSOUND recycle for the
	 * spec-3.22 0-match theorem (the slot's commit_scn was not proven below the
	 * horizon).  Incarnation-scoped (reset to 0 on shmem init): a pre-restart
	 * unsound recycle cannot false-hide a row from any post-restart reader, whose
	 * read_scn is at or after every pre-restart commit.  Non-zero disqualifies the
	 * RECYCLED_ZERO_MATCH -> invisible shortcut for this node's lifetime
	 * (cluster_cr_retention_proof_valid fails closed).
	 */
	pg_atomic_uint64 retention_off_recycle_count;

	/*
	 * spec-6.12i CP5 (D-i4): monotonic MAX over the gate horizon of every
	 * HORIZON-GATED COMMITTED recycle this incarnation.  This is the bound
	 * the cross-instance origin verdict ships for a complete-scan 0-match:
	 * every gated recycle proved its commit_scn at/below the gate horizon,
	 * so the max over those horizons upper-bounds every recycled commit_scn
	 * — and unlike the CURRENT horizon (which falls back to the live SCN
	 * clock and Lamport-chases any observing peer forever), it only moves
	 * when a recycle actually happens, so a peer that observed it can catch
	 * up (requester leg (e) converges).  InvalidScn until the first gated
	 * recycle this incarnation -> the verdict serve refuses (fail-closed;
	 * pre-restart recycles are not covered by this incarnation's tracker).
	 */
	pg_atomic_uint64 retention_max_recycle_horizon;

	/* A validated own shutdown checkpoint, never a new GC permission. */
	pg_atomic_uint64 startup_checkpoint_scn;
	pg_atomic_uint64 startup_checkpoint_next_xid;
	pg_atomic_uint32 startup_checkpoint_state; /* 0 absent, 1 captured, 2 clean, 3 refused */

	/* spec-3.13 D5: entries refused for recycle because wrap reached
	 * TT_WRAP_MAX (ABA fail-fast guard; cleared only by whole-segment
	 * rollover/reuse which resets wraps to 0).  Bumped by BOTH selection
	 * points (alloc Pass-1 and the cleaner GC pass). */
	pg_atomic_uint64 tt_slot_wrap_retired_count;

	/* spec-3.15 D6 (V-4): prepared-xact protected slots (alloc gate). */
	slock_t protected_lock;
	uint32 protected_count;
	struct {
		uint32 segment_id;
		uint16 slot_offset;
		uint16 wrap;
		TransactionId xid;
		uint32 _pad;
	} protected_slots[CLUSTER_TT_PROTECTED_MAX];

	ClusterTTSlotAllocPerSegment per_node[CLUSTER_TT_SLOT_MAX_NODES];
} ClusterTTSlotShmem;


static ClusterTTSlotShmem *ClusterTTSlotShm = NULL;


/*
 * Map segment_id → owning node_id using the per-instance range encoding
 * (CLUSTER_UNDO_SEGS_PER_INSTANCE).  Used to index the shmem array.
 */
static inline int
cluster_tt_slot_segment_to_node(uint32 segment_id)
{
	Assert(segment_id != 0);
	return (int)((segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE);
}


/*
 * Return the per-segment allocator state, lazily initialising the
 * (segment_id, lock) fields on first use.  Caller must NOT hold the
 * lock (this function will take it briefly for init when needed).
 */
static ClusterTTSlotAllocPerSegment *
cluster_tt_slot_get_or_init(uint32 segment_id)
{
	int node_id;
	ClusterTTSlotAllocPerSegment *seg;

	if (ClusterTTSlotShm == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster TT slot allocator shmem not initialised")));

	if (segment_id == 0 || segment_id > UINT16_MAX)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_tt_slot: segment_id %u out of range (1, %u]", segment_id,
							   (unsigned)UINT16_MAX)));

	node_id = cluster_tt_slot_segment_to_node(segment_id);
	if (node_id < 0 || node_id >= CLUSTER_TT_SLOT_MAX_NODES)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_tt_slot: segment_id %u derives node_id %d outside [0, %d)",
							   segment_id, node_id, CLUSTER_TT_SLOT_MAX_NODES)));

	seg = &ClusterTTSlotShm->per_node[node_id];

	if (seg->segment_id == 0) {
		/*
		 * First-touch initialisation.  Take the lock so concurrent first
		 * touches on the same node race-free.  All other fields are zero
		 * by shmem init (CTS_FREE == 0, wrap == 0, xid == 0).
		 */
		LWLockAcquire(&seg->lock, LW_EXCLUSIVE);
		if (seg->segment_id == 0) {
			Assert(seg->binding_generation == 0);
			seg->binding_generation = 1;
			seg->segment_id = segment_id;
		}
		LWLockRelease(&seg->lock);
	} else if (seg->segment_id != segment_id) {
		/*
		 * spec-3.4b MVP: single active segment per node.  A node that
		 * presents two distinct segment_ids would either mean the per-node
		 * range arithmetic is wrong or a future caller has expanded the
		 * design without updating this allocator.  Either way, refuse.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cluster_tt_slot: node %d already bound to segment %u, refusing segment %u",
						node_id, seg->segment_id, segment_id),
				 errhint("spec-3.4b MVP allocates one active undo segment per node; "
						 "multi-segment-per-node support lands in a later spec.")));
	}

	return seg;
}


/*
 * tt_slot_entry_wrap_retired -- spec-3.13 D5.
 *
 *	A terminal-state entry whose wrap counter has reached TT_WRAP_MAX is
 *	retired for the remainder of this segment generation: recycling it
 *	again could not bump wrap (saturation) and would erode the ABA
 *	fail-fast guard.  Retire is shmem-only and generation-local (v0.2 ④):
 *	rollover/reuse reset wraps to 0 and the slot becomes usable again.
 *	Applies regardless of the retention GUC -- this is ABA safety, not
 *	retention policy.
 */
static inline bool
tt_slot_entry_wrap_retired(const ClusterTTSlotAllocEntry *e)
{
	return e->wrap >= TT_WRAP_MAX;
}

static inline void
tt_slot_count_wrap_retired(void)
{
	if (ClusterTTSlotShm != NULL)
		pg_atomic_fetch_add_u64(&ClusterTTSlotShm->tt_slot_wrap_retired_count, 1);
}

/*
 * tt_slot_note_gated_recycle_horizon / cluster_tt_slot_note_gated_recycle_
 * horizon -- spec-6.12i CP5 (D-i4): fold the gate horizon of a HORIZON-GATED
 * COMMITTED recycle into the monotonic max tracker (see the shmem field
 * comment).  Lock-free CAS max: callers hold different per-segment locks (or
 * the lifecycle lock, for the segment-granularity feed), so the max must be
 * its own atomic protocol.  scn_time_cmp keeps the SCN ordering discipline.
 */
static void
tt_slot_note_gated_recycle_horizon(SCN horizon)
{
	uint64 old;

	if (ClusterTTSlotShm == NULL || !SCN_VALID(horizon))
		return;
	old = pg_atomic_read_u64(&ClusterTTSlotShm->retention_max_recycle_horizon);
	while (!SCN_VALID((SCN)old) || scn_time_cmp(horizon, (SCN)old) > 0) {
		if (pg_atomic_compare_exchange_u64(&ClusterTTSlotShm->retention_max_recycle_horizon, &old,
										   (uint64)horizon))
			break;
	}
}

void
cluster_tt_slot_note_gated_recycle_horizon(SCN horizon)
{
	tt_slot_note_gated_recycle_horizon(horizon);
}

void
cluster_tt_slot_capture_startup_checkpoint(SCN scn, FullTransactionId next_xid)
{
	if (ClusterTTSlotShm == NULL)
		return;
	if (pg_atomic_read_u32(&ClusterTTSlotShm->startup_checkpoint_state) != 0 || !SCN_VALID(scn)
		|| scn_node_id(scn) != cluster_node_id || EpochFromFullTransactionId(next_xid) != 0
		|| !TransactionIdIsNormal(XidFromFullTransactionId(next_xid))) {
		pg_atomic_write_u32(&ClusterTTSlotShm->startup_checkpoint_state, 3);
		return;
	}
	pg_atomic_write_u64(&ClusterTTSlotShm->startup_checkpoint_scn, scn);
	pg_atomic_write_u64(&ClusterTTSlotShm->startup_checkpoint_next_xid,
						U64FromFullTransactionId(next_xid));
	pg_write_barrier();
	pg_atomic_write_u32(&ClusterTTSlotShm->startup_checkpoint_state, 1);
}

void
cluster_tt_slot_confirm_clean_start(bool clean, int startup_prepared_count)
{
	uint32 state;
	if (ClusterTTSlotShm == NULL)
		return;
	state = pg_atomic_read_u32(&ClusterTTSlotShm->startup_checkpoint_state);
	/* Prepared transactions survive clean shutdown and may commit later.
	 * Refuse the whole startup bound before admission if any were recovered;
	 * a later empty runtime list must never undo this decision. */
	pg_atomic_write_u32(&ClusterTTSlotShm->startup_checkpoint_state,
						clean && startup_prepared_count == 0 && (state == 1 || state == 2) ? 2 : 3);
}

/* Only the origin resolver may combine this bound with a complete scan,
 * no-raw-reuse fence and explicit own CLOG terminal proof. */
SCN
cluster_tt_slot_startup_committed_bound(TransactionId xid)
{
	uint64 cutoff;
	SCN scn;
	if (ClusterTTSlotShm == NULL || !TransactionIdIsNormal(xid)
		|| pg_atomic_read_u32(&ClusterTTSlotShm->startup_checkpoint_state) != 2)
		return InvalidScn;
	pg_read_barrier();
	cutoff = pg_atomic_read_u64(&ClusterTTSlotShm->startup_checkpoint_next_xid);
	scn = pg_atomic_read_u64(&ClusterTTSlotShm->startup_checkpoint_scn);
	if (cutoff == 0 || cutoff > UINT32_MAX || xid >= cutoff
		|| pg_atomic_read_u32(&ClusterTTSlotShm->startup_checkpoint_state) != 2)
		return InvalidScn;
	return scn;
}


/*
 * tt_slot_entry_recycle_locked -- the single typed recycle transition
 * (spec-3.13 C-R1).  Caller holds seg->lock and has already decided
 * recyclability via cluster_tt_slot_recyclable / C6 bypass.
 *
 *	new_owner valid   -> COMMITTED/ABORTED becomes ACTIVE(new_owner)
 *	                     (alloc Pass-2 direct reuse path);
 *	new_owner invalid -> becomes CTS_FREE (cleaner D2-A proactive GC).
 *
 *	wrap++ happens HERE, at the recycle moment, in BOTH modes (saturate
 *	at TT_WRAP_MAX).  FREE -> ACTIVE allocation never bumps wrap, so a
 *	cleaner-mediated cycle (COMMITTED -> FREE -> ACTIVE) and a direct
 *	recycle (COMMITTED -> ACTIVE) leave byte-identical end states —
 *	the R1 anti-divergence property, unit-locked by T40.
 */
static void
tt_slot_entry_recycle_locked(ClusterTTSlotAllocEntry *e, TransactionId new_owner)
{
	e->xid = new_owner;
	e->status = TransactionIdIsValid(new_owner) ? CTS_ACTIVE : CTS_FREE;
	e->commit_scn = InvalidScn;
	if (e->wrap < TT_WRAP_MAX)
		e->wrap++;
}


/*
 * Allocate from one already-locked allocator generation.  The caller samples
 * the retention horizon before taking seg->lock (C17) and owns the exact
 * current-segment check.  Returning the assigned wrap from this same critical
 * section prevents a rollover from separating slot assignment from the
 * binding's ABA token capture.
 */
static uint16
cluster_tt_slot_alloc_locked(ClusterTTSlotAllocPerSegment *seg, uint32 segment_id,
							 TransactionId top_xid, bool gate_enabled, bool peer_mode,
							 SCN horizon, bool *out_retained_pressure, uint16 *out_wrap)
{
	int reusable_idx = -1;
	int free_idx = -1;
	bool retained_pressure = false;
	uint16 chosen;
	uint64 retain_skip_seen = 0; /* spec-3.12 D5 */
	int i;

	/* Pass 1: classify slots (idempotent reuse short-circuits). */
	for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		const ClusterTTSlotAllocEntry *e = &seg->slots[i];

		if (e->status == CTS_ACTIVE && e->xid == top_xid) {
			if (out_wrap)
				*out_wrap = e->wrap;
			return (uint16)i;
		}
		if (e->status == CTS_FREE) {
			if (free_idx < 0 && !cluster_tt_slot_is_protected(segment_id, (uint16)i))
				free_idx = i;
		} else if (e->status == CTS_COMMITTED || e->status == CTS_ABORTED) {
			/* A local ProcArray horizon cannot authorize destruction of evidence
			 * still needed by a peer snapshot or current-MX canonical proof. */
			bool recyclable
				= (peer_mode
				   && (e->status == CTS_COMMITTED || e->status == CTS_ABORTED))
					  ? false
					  : (gate_enabled
							 ? cluster_tt_slot_recyclable(e->status, e->commit_scn, horizon)
							 : true);

			if (recyclable && tt_slot_entry_wrap_retired(e)) {
				recyclable = false;
				retained_pressure = true;
				tt_slot_count_wrap_retired();
			}

			if (recyclable) {
				if (reusable_idx < 0
					&& !cluster_tt_slot_is_protected(segment_id, (uint16)i))
					reusable_idx = i;
			} else {
				retained_pressure = true;
				retain_skip_seen++;
			}
		}
	}

	if (retain_skip_seen > 0 && ClusterTTSlotShm != NULL)
		pg_atomic_fetch_add_u64(&ClusterTTSlotShm->tt_slot_retain_skip_count,
								retain_skip_seen);

	/* Pass 2: prefer FREE over a retention-eligible recyclable slot. */
	if (free_idx >= 0) {
		ClusterTTSlotAllocEntry *e = &seg->slots[free_idx];

		e->xid = top_xid;
		e->status = CTS_ACTIVE;
		e->commit_scn = InvalidScn;
		chosen = (uint16)free_idx;
	} else if (reusable_idx >= 0) {
		ClusterTTSlotAllocEntry *e = &seg->slots[reusable_idx];
		bool was_committed = (e->status == CTS_COMMITTED);

		tt_slot_entry_recycle_locked(e, top_xid);
		chosen = (uint16)reusable_idx;

		if (was_committed && ClusterTTSlotShm != NULL) {
			pg_atomic_fetch_add_u64(&ClusterTTSlotShm->retention_recycle_count, 1);
			if (!gate_enabled || !SCN_VALID(horizon))
				pg_atomic_fetch_add_u64(&ClusterTTSlotShm->retention_off_recycle_count, 1);
			else
				tt_slot_note_gated_recycle_horizon(horizon);
		}
	} else {
		if (out_retained_pressure)
			*out_retained_pressure = retained_pressure;
		return INVALID_TT_SLOT_OFFSET;
	}

	if (out_wrap)
		*out_wrap = seg->slots[chosen].wrap;
	return chosen;
}


/*
 * cluster_tt_slot_alloc_ext
 *
 *	Three-tier fallback (L189 recycle policy) with the spec-3.12 retention
 *	gate layered on Pass 2:
 *	    1) reuse a slot already owned by `top_xid` (idempotent)
 *	    2) take any FREE slot
 *	    3) recycle a *retention-eligible* COMMITTED / ABORTED slot, wrap++
 *	A COMMITTED slot is retention-eligible only when commit_scn is older than
 *	the horizon
 *	(cluster_tt_slot_recyclable); single-node ABORTED is eligible (C7).  When the
 *	retention GUC is off, the gate is bypassed (spec-3.11 immediate recycle, C6).
 *	In peer mode, allocator Pass 2 never recycles COMMITTED directly: the local
 *	ProcArray horizon does not cover remote snapshots.  The cluster undo cleaner
 *	is the sole COMMITTED recycler there; it folds every required peer report
 *	into an epoch-paired cluster floor and fences each COMMITTED -> FREE mutation.
 *	Allocator Pass 1 may then consume that proven FREE slot.  In peer mode,
 *	ABORTED is also retained as canonical current-MX evidence until an exact
 *	reference-release owner exists.
 *
 *	Returns INVALID_TT_SLOT_OFFSET when no slot can be handed out.  In that
 *	case *out_retained_pressure (when non-NULL) distinguishes the two reasons:
 *	    true  -> at least one terminal slot is being kept alive by retention
 *	             (not all-ACTIVE); the caller may roll over to a new active
 *	             segment instead of failing (spec-3.12 D2b).
 *	    false -> every slot is ACTIVE (genuine in-flight concurrency limit).
 *
 *	spec-3.12 C17 lock ordering: the horizon is computed (taking ProcArrayLock
 *	SHARED) BEFORE seg->lock is acquired, so seg->lock is never held while
 *	ProcArrayLock is taken.
 */
uint16
cluster_tt_slot_alloc_ext(uint32 segment_id, TransactionId top_xid, bool *out_retained_pressure)
{
	ClusterTTSlotAllocPerSegment *seg;
	bool gate_enabled;
	bool peer_mode;
	SCN horizon = InvalidScn;
	uint16 result;

	if (out_retained_pressure)
		*out_retained_pressure = false;

	if (!TransactionIdIsValid(top_xid))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_tt_slot_alloc: top_xid must be valid")));

	/*
	 * C17: compute the retention horizon before taking seg->lock.  When the
	 * GUC is off we skip the scan entirely and bypass the gate.
	 */
	gate_enabled = cluster_undo_retention_horizon_enabled;
	peer_mode = cluster_peer_mode_enabled();
	if (gate_enabled) {
		horizon = cluster_undo_retention_horizon();
		/* spec-3.12 D5 / C16: sample the horizon gauge at the decision point. */
		if (ClusterTTSlotShm != NULL)
			pg_atomic_write_u64(&ClusterTTSlotShm->retention_horizon_scn, (uint64)horizon);
	}

	seg = cluster_tt_slot_get_or_init(segment_id);

	LWLockAcquire(&seg->lock, LW_EXCLUSIVE);
	result = cluster_tt_slot_alloc_locked(seg, segment_id, top_xid, gate_enabled,
								 peer_mode, horizon, out_retained_pressure, NULL);
	LWLockRelease(&seg->lock);
	return result;
}


/*
 * Allocate only if expected_segment_id is still the node's exact CURRENT
 * allocator generation.  A concurrent rollover/reuse is ordinary local
 * backpressure: report current_drift with zero mutation so the binding owner
 * can reread CURRENT.  The legacy segment-addressed API above intentionally
 * keeps its strict mismatch ERROR contract for callers that name a non-current
 * segment unexpectedly.
 */
uint16
cluster_tt_slot_alloc_current_exact(int node_id, uint32 expected_segment_id,
								TransactionId top_xid, bool *out_retained_pressure,
								bool *out_current_drift, uint16 *out_wrap)
{
	ClusterTTSlotAllocPerSegment *seg;
	bool gate_enabled;
	bool peer_mode;
	SCN horizon = InvalidScn;
	uint16 result;

	if (out_retained_pressure)
		*out_retained_pressure = false;
	if (out_current_drift)
		*out_current_drift = false;
	if (out_wrap)
		*out_wrap = TT_WRAP_INVALID;

	if (ClusterTTSlotShm == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster TT slot allocator shmem not initialised")));
	if (node_id < 0 || node_id >= CLUSTER_TT_SLOT_MAX_NODES
		|| expected_segment_id == 0 || expected_segment_id > UINT16_MAX
		|| cluster_tt_slot_segment_to_node(expected_segment_id) != node_id)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_tt_slot_alloc_current_exact: invalid node/segment identity")));
	if (!TransactionIdIsValid(top_xid))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_tt_slot_alloc_current_exact: top_xid must be valid")));

	gate_enabled = cluster_undo_retention_horizon_enabled;
	peer_mode = cluster_peer_mode_enabled();
	if (gate_enabled) {
		horizon = cluster_undo_retention_horizon();
		pg_atomic_write_u64(&ClusterTTSlotShm->retention_horizon_scn, (uint64)horizon);
	}

	seg = &ClusterTTSlotShm->per_node[node_id];
	LWLockAcquire(&seg->lock, LW_EXCLUSIVE);
	if (seg->segment_id == 0) {
		Assert(seg->binding_generation == 0);
		seg->binding_generation = 1;
		seg->segment_id = expected_segment_id;
	}
	else if (seg->segment_id != expected_segment_id) {
		if (out_current_drift)
			*out_current_drift = true;
		LWLockRelease(&seg->lock);
		return INVALID_TT_SLOT_OFFSET;
	}

	result = cluster_tt_slot_alloc_locked(seg, expected_segment_id, top_xid,
								 gate_enabled, peer_mode, horizon,
								 out_retained_pressure, out_wrap);
	LWLockRelease(&seg->lock);
	return result;
}


/*
 * cluster_tt_slot_gc_current_pass -- spec-3.13 D2-A.
 *
 *	Proactively recycle retention-eligible COMMITTED/ABORTED entries of
 *	this node's CURRENT allocator segment to CTS_FREE, so the alloc fast
 *	path finds FREE slots (Pass 1) instead of paying the Pass-2 horizon
 *	gate.  Rolled-away segments have no shmem allocator state — their
 *	durable headers are covered by the read-only scan pass (D2-B).
 *
 *	Caller computed `horizon` BEFORE any seg->lock (C17) and only calls
 *	with the retention gate enabled: with the GUC off, alloc Pass-2
 *	already recycles immediately (C6) and there is nothing to pre-free.
 *
 *	Peer-mode ABORTED entries are canonical current-MX evidence and are skipped
 *	until an exact release owner exists.  CTS_ACTIVE entries here are live
 *	in-flight transactions (this is the
 *	current segment), NOT crash residue — they are simply skipped and
 *	NOT counted as stale (HC6 stale accounting is durable-side only).
 */
bool
cluster_tt_slot_gc_current_pass(SCN horizon, uint64 expected_epoch,
								ClusterUndoCleanerPassStats *stats)
{
	uint32 segment_id;
	ClusterTTSlotAllocPerSegment *seg;
	uint32 gcd = 0;
	bool fence_ok = true;
	bool peer_mode;
	int i;

	Assert(stats != NULL);

	if (ClusterTTSlotShm == NULL || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_TT_SLOT_MAX_NODES)
		return true;

	segment_id = cluster_tt_slot_current_segment(cluster_node_id);
	if (segment_id == 0)
		return true; /* no binding yet this incarnation */

	seg = &ClusterTTSlotShm->per_node[cluster_node_id];
	peer_mode = cluster_peer_mode_enabled();

	for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		ClusterTTSlotAllocEntry candidate;
		uint64 binding_generation;
		uint8 terminal_status;
		bool recyclable;

		/* Phase 1: freeze one exact allocator candidate and release the lock
		 * before any canonical block-0/current sampling. */
		LWLockAcquire(&seg->lock, LW_EXCLUSIVE);
		if (seg->segment_id != segment_id) {
			LWLockRelease(&seg->lock);
			break;
		}
		candidate = seg->slots[i];
		binding_generation = seg->binding_generation;
		LWLockRelease(&seg->lock);

		if (candidate.status != CTS_COMMITTED
			&& candidate.status != CTS_ABORTED)
			continue;
		if (tt_slot_entry_wrap_retired(&candidate)) {
			tt_slot_count_wrap_retired();
			stats->slots_wrap_retired++;
			continue; /* D5: ABA guard exhausted; wait for rollover/reuse */
		}

		/* Phase 2: evaluate the horizon and, in peer mode, sample the sole
		 * durable release authority with no allocator lock held.  The current
		 * 8.4D floor is inclusive; ABORTED has no invented SCN horizon. */
		if (peer_mode) {
			recyclable = candidate.status == CTS_ABORTED
				? candidate.commit_scn == InvalidScn
				: SCN_VALID(candidate.commit_scn)
				  && SCN_VALID(horizon)
				  && scn_time_cmp(candidate.commit_scn, horizon) <= 0;
			terminal_status = candidate.status == CTS_COMMITTED
				? (uint8)TT_SLOT_COMMITTED : (uint8)TT_SLOT_ABORTED;
			if (recyclable)
				recyclable = cluster_ctrc_terminal_release_sample_exact(
					segment_id, (uint16)i, candidate.xid,
					candidate.wrap, terminal_status, candidate.commit_scn,
					expected_epoch);
		} else
			recyclable = cluster_tt_slot_recyclable(candidate.status,
				candidate.commit_scn, horizon);
		if (!recyclable)
			continue;

		/*
		 * spec-5.22e F-D2 epoch fence: re-verify the reconfig epoch before
		 * every slot FREE.  A mid-pass epoch bump voids the folded floor's
		 * member coverage -- stop mutating and abort the whole pass.
		 */
		if (cluster_undo_horizon_epoch_fence_tripped(expected_epoch)) {
			fence_ok = false;
			break;
		}

		/* Phase 3: accept only the unchanged entry under the unchanged
		 * binding generation.  This is the mutation point and therefore the
		 * final folded-epoch fence is repeated while the candidate is owned. */
		LWLockAcquire(&seg->lock, LW_EXCLUSIVE);
		if (seg->segment_id != segment_id
			|| seg->binding_generation != binding_generation
			|| memcmp(&seg->slots[i], &candidate, sizeof(candidate)) != 0) {
			LWLockRelease(&seg->lock);
			continue;
		}
		if (cluster_undo_horizon_epoch_fence_tripped(expected_epoch)) {
			LWLockRelease(&seg->lock);
			fence_ok = false;
			break;
		}

		if (candidate.status == CTS_COMMITTED && ClusterTTSlotShm != NULL) {
			pg_atomic_fetch_add_u64(&ClusterTTSlotShm->retention_recycle_count, 1);
			/* spec-3.22: defense in depth -- this pass is meant to run only with a
			 * valid horizon (caller gate enabled), but an InvalidScn horizon would
			 * make recyclable() pass a COMMITTED slot ungated; record it so the
			 * xmax 0-match shortcut fails closed (cluster_cr_retention_proof_valid). */
			if (!SCN_VALID(horizon))
				pg_atomic_fetch_add_u64(&ClusterTTSlotShm->retention_off_recycle_count, 1);
			else
				tt_slot_note_gated_recycle_horizon(horizon); /* spec-6.12i CP5 */
		}
		tt_slot_entry_recycle_locked(&seg->slots[i], InvalidTransactionId);
		if (peer_mode)
			cluster_ctrc_stat_bump(CTRC_STAT_L12_RECYCLE);
		LWLockRelease(&seg->lock);
		gcd++;
	}

	stats->shmem_tt_slots_gcd += gcd;
	stats->segments_scanned++;
	return fence_ok;
}


/*
 * cluster_tt_slot_alloc
 *
 *	Back-compat 2-arg wrapper (drops the retained-pressure signal).  See
 *	cluster_tt_slot_alloc_ext for the contract.
 */
uint16
cluster_tt_slot_alloc(uint32 segment_id, TransactionId top_xid)
{
	return cluster_tt_slot_alloc_ext(segment_id, top_xid, NULL);
}


/*
 * cluster_tt_slot_free
 *
 *	Mark slot as FREE.  Called from end-of-xact path (commit + abort).
 *	Idempotent: freeing an already-FREE slot is a no-op (defensive
 *	against double-callback in xact cleanup).
 */
void
cluster_tt_slot_free(uint32 segment_id, uint16 slot_offset)
{
	ClusterTTSlotAllocPerSegment *seg;
	ClusterTTSlotAllocEntry *e;

	if (slot_offset >= TT_SLOTS_PER_SEGMENT)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_tt_slot_free: slot_offset %u out of range [0, %d)",
							   slot_offset, TT_SLOTS_PER_SEGMENT)));

	seg = cluster_tt_slot_get_or_init(segment_id);

	LWLockAcquire(&seg->lock, LW_EXCLUSIVE);
	e = &seg->slots[slot_offset];
	e->status = CTS_FREE;
	e->xid = InvalidTransactionId;
	e->commit_scn = InvalidScn;
	/* wrap is preserved across FREE → next ACTIVE; only recycle bumps it */
	LWLockRelease(&seg->lock);
}


/*
 * cluster_tt_slot_pernode_structural -- return the per-node allocator that owns
 * `segment_id`, validating only that segment_id is structurally in range.
 * Unlike cluster_tt_slot_get_or_init it does NOT enforce the node's current
 * binding, so end-of-xact mark transitions can no-op on a segment the node has
 * since rolled away from (spec-3.12 D2b): that segment's retention is tracked
 * durably (segment header), not in this shmem allocator.  Caller must check
 * seg->segment_id == segment_id under the lock to detect the stale case.
 */
static ClusterTTSlotAllocPerSegment *
cluster_tt_slot_pernode_structural(uint32 segment_id, const char *fn)
{
	int node_id;

	if (ClusterTTSlotShm == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster TT slot allocator shmem not initialised")));
	if (segment_id == 0 || segment_id > UINT16_MAX)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("%s: segment_id %u out of range (1, %u]", fn, segment_id,
							   (unsigned)UINT16_MAX)));
	node_id = cluster_tt_slot_segment_to_node(segment_id);
	if (node_id < 0 || node_id >= CLUSTER_TT_SLOT_MAX_NODES)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("%s: segment_id %u derives node_id %d outside [0, %d)", fn,
							   segment_id, node_id, CLUSTER_TT_SLOT_MAX_NODES)));

	return &ClusterTTSlotShm->per_node[node_id];
}


/*
 * cluster_tt_slot_mark_committed -- spec-3.12 D2.
 *
 *	ACTIVE -> COMMITTED, retaining owner xid + wrap + commit_scn.  The slot is
 *	NOT freed at commit (unlike the spec-3.4b MVP); the retention gate in
 *	cluster_tt_slot_alloc keeps it (and therefore the durable segment-header TT
 *	slot at this offset) addressable until commit_scn drops below the horizon.
 *	Defensive: only an ACTIVE slot owned by `xid` transitions, so a double
 *	end-of-xact callback (or a slot already recycled by a later xact) is a
 *	no-op rather than corrupting another owner's retention state.
 *
 *	spec-3.12 D2b: if the node has rolled this segment away (seg->segment_id no
 *	longer matches), this is a no-op -- the old segment's commit_scn is already
 *	durable in its header and its retention is tracked at segment granularity.
 */
void
cluster_tt_slot_mark_committed(uint32 segment_id, uint16 slot_offset, TransactionId xid,
							   SCN commit_scn)
{
	ClusterTTSlotAllocPerSegment *seg;
	bool		transitioned = false;

	if (slot_offset >= TT_SLOTS_PER_SEGMENT)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster_tt_slot_mark_committed: slot_offset %u out of range [0, %d)",
						slot_offset, TT_SLOTS_PER_SEGMENT)));

	/*
	 * rule 8.A: a COMMITTED slot without a real commit_scn would be retained
	 * forever (the gate cannot prove it recyclable) and could stall the
	 * horizon.  Commit always advances the SCN first, so this must hold.
	 */
	Assert(SCN_VALID(commit_scn));

	seg = cluster_tt_slot_pernode_structural(segment_id, "cluster_tt_slot_mark_committed");

	LWLockAcquire(&seg->lock, LW_EXCLUSIVE);
	if (seg->segment_id == segment_id) {
		ClusterTTSlotAllocEntry *e = &seg->slots[slot_offset];

		if (e->status == CTS_ACTIVE && e->xid == xid) {
			e->status = CTS_COMMITTED;
			e->commit_scn = commit_scn;
			transitioned = true;
			/* keep xid + wrap so the durable header slot stays addressable. */
		}
	}
	LWLockRelease(&seg->lock);
	if (transitioned)
		cluster_undo_cleaner_wakeup();
}


/*
 * cluster_tt_slot_mark_aborted -- spec-3.12 D2 / C7.
 *
 *	ACTIVE -> ABORTED.  commit_scn is cleared.  Single-node C7 may recycle it
 *	immediately; peer mode retains the physical slot as canonical current-MX
 *	evidence until an exact release owner exists.  Same defensive ownership +
 *	rolled-away (D2b) guards as mark_committed.
 */
void
cluster_tt_slot_mark_aborted(uint32 segment_id, uint16 slot_offset, TransactionId xid)
{
	ClusterTTSlotAllocPerSegment *seg;
	bool		transitioned = false;

	if (slot_offset >= TT_SLOTS_PER_SEGMENT)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_tt_slot_mark_aborted: slot_offset %u out of range [0, %d)",
							   slot_offset, TT_SLOTS_PER_SEGMENT)));

	seg = cluster_tt_slot_pernode_structural(segment_id, "cluster_tt_slot_mark_aborted");

	LWLockAcquire(&seg->lock, LW_EXCLUSIVE);
	if (seg->segment_id == segment_id) {
		ClusterTTSlotAllocEntry *e = &seg->slots[slot_offset];

		if (e->status == CTS_ACTIVE && e->xid == xid) {
			e->status = CTS_ABORTED;
			e->commit_scn = InvalidScn;
			transitioned = true;
		}
	}
	LWLockRelease(&seg->lock);
	if (transitioned)
		cluster_undo_cleaner_wakeup();
}


/*
 * cluster_tt_slot_current_segment -- spec-3.12 D2b.
 *
 *	Return the segment_id the node's TT-slot allocator is currently bound to,
 *	or 0 if the node has never bound one.  The binding path uses this to keep
 *	allocating on the active segment after a rollover (rather than re-deriving
 *	the fixed spec-3.4b id).  Read under the SHARED lock so it can't tear
 *	against a concurrent rollover.
 */
uint32
cluster_tt_slot_current_segment(int node_id)
{
	ClusterTTSlotAllocPerSegment *seg;
	uint32 segment_id;

	if (ClusterTTSlotShm == NULL || node_id < 0 || node_id >= CLUSTER_TT_SLOT_MAX_NODES)
		return 0;

	seg = &ClusterTTSlotShm->per_node[node_id];
	LWLockAcquire(&seg->lock, LW_SHARED);
	segment_id = seg->segment_id;
	LWLockRelease(&seg->lock);
	return segment_id;
}


/*
 * cluster_tt_slot_current_owner_by_xid
 *
 *	Return one exact owner snapshot from the allocator's current segment.
 *	This is a bounded shmem read (48 slots under one SHARED lock): it neither
 *	initialises an allocator binding nor consults durable TT storage.  A
 *	duplicate xid or an internally inconsistent status/commit tuple is not a
 *	canonical owner and therefore fails closed with a zeroed result.
 */
bool
cluster_tt_slot_current_owner_by_xid(int node_id, TransactionId xid,
									 ClusterTTSlotCurrentOwner *out)
{
	ClusterTTSlotAllocPerSegment *seg;
	ClusterTTSlotCurrentOwner found;
	bool have_owner = false;
	bool valid = true;
	int i;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (ClusterTTSlotShm == NULL || node_id < 0
		|| node_id >= CLUSTER_TT_SLOT_MAX_NODES
		|| !TransactionIdIsNormal(xid))
		return false;

	memset(&found, 0, sizeof(found));
	seg = &ClusterTTSlotShm->per_node[node_id];
	LWLockAcquire(&seg->lock, LW_SHARED);
	if (seg->segment_id == 0) {
		LWLockRelease(&seg->lock);
		return false;
	}

	for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		const ClusterTTSlotAllocEntry *entry = &seg->slots[i];

		if (entry->status == CTS_FREE || entry->xid != xid)
			continue;
		if (have_owner) {
			valid = false;
			break;
		}
		if ((entry->status == CTS_COMMITTED
			 && !SCN_VALID(entry->commit_scn))
			|| ((entry->status == CTS_ACTIVE || entry->status == CTS_ABORTED)
				&& entry->commit_scn != InvalidScn)
			|| (entry->status != CTS_ACTIVE
				&& entry->status != CTS_COMMITTED
				&& entry->status != CTS_ABORTED)) {
			valid = false;
			break;
		}

		found.segment_id = seg->segment_id;
		found.xid = entry->xid;
		found.commit_scn = entry->commit_scn;
		found.slot_offset = (uint16)i;
		found.wrap = entry->wrap;
		found.status = entry->status;
		have_owner = true;
	}
	LWLockRelease(&seg->lock);

	if (!valid || !have_owner)
		return false;
	*out = found;
	return true;
}


/*
 * cluster_tt_slot_rollover -- spec-3.12 D2b.
 *
 *	Rebind the node's TT-slot allocator to `new_segment_id` and reset its 48
 *	slots to FREE (the new segment's on-disk header is fresh, so wrap restarts
 *	at 0).  The previous segment's retained COMMITTED slots are abandoned at the
 *	shmem-allocator level: their commit_scn is already durable in that segment's
 *	header and their retention is now tracked at segment granularity (no offset
 *	on the old segment is ever reused, so its durable TT slots stay resolvable
 *	by-xid).  Caller MUST serialize rollovers with lifecycle_lock (C17:
 *	lifecycle_lock is held here, then seg->lock; never the reverse).
 *
 *	spec-3.12 D3: *out_old_had_active (when non-NULL) reports whether the old
 *	segment still had any CTS_ACTIVE (in-flight) slot at reset time.  If false,
 *	the old segment is drained and the caller may transition it to
 *	SEGMENT_COMMITTED for retention reclaim (spec-3.13).
 */
void
cluster_tt_slot_rollover(int node_id, uint32 new_segment_id, bool *out_old_had_active)
{
	ClusterTTSlotAllocPerSegment *seg;
	bool old_had_active = false;
	int i;

	if (out_old_had_active != NULL)
		*out_old_had_active = false;

	if (ClusterTTSlotShm == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster TT slot allocator shmem not initialised")));
	if (node_id < 0 || node_id >= CLUSTER_TT_SLOT_MAX_NODES)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_tt_slot_rollover: node_id %d outside [0, %d)", node_id,
							   CLUSTER_TT_SLOT_MAX_NODES)));
	if (new_segment_id == 0 || new_segment_id > UINT16_MAX)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_tt_slot_rollover: segment_id %u out of range (1, %u]",
							   new_segment_id, (unsigned)UINT16_MAX)));
	if (cluster_tt_slot_segment_to_node(new_segment_id) != node_id)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_tt_slot_rollover: segment_id %u does not belong to node %d",
							   new_segment_id, node_id)));

	seg = &ClusterTTSlotShm->per_node[node_id];
	LWLockAcquire(&seg->lock, LW_EXCLUSIVE);
	if (seg->binding_generation == UINT64_MAX) {
		LWLockRelease(&seg->lock);
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cluster_tt_slot_rollover: binding generation exhausted for node %d",
						node_id)));
	}
	for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		if (seg->slots[i].status == CTS_ACTIVE) {
			old_had_active = true;
			break;
		}
	}
	seg->binding_generation++;
	seg->segment_id = new_segment_id;
	memset(seg->slots, 0, sizeof(seg->slots)); /* all CTS_FREE, wrap 0, commit_scn 0 */
	LWLockRelease(&seg->lock);

	if (out_old_had_active != NULL)
		*out_old_had_active = old_had_active;
}


/*
 * cluster_tt_slot_get_wrap
 *
 *	Return the current wrap counter.  SHARED lock since this is a pure
 *	read.
 */
uint16
cluster_tt_slot_get_wrap(uint32 segment_id, uint16 slot_offset)
{
	ClusterTTSlotAllocPerSegment *seg;
	uint16 wrap;

	if (slot_offset >= TT_SLOTS_PER_SEGMENT)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_tt_slot_get_wrap: slot_offset %u out of range [0, %d)",
							   slot_offset, TT_SLOTS_PER_SEGMENT)));

	seg = cluster_tt_slot_get_or_init(segment_id);

	LWLockAcquire(&seg->lock, LW_SHARED);
	wrap = seg->slots[slot_offset].wrap;
	LWLockRelease(&seg->lock);

	return wrap;
}


/* ===== shmem lifecycle ===== */

/*
 * The allocator is only used when this node participates in the cluster
 * (cluster_tt_local_get_or_create_binding gates on the same condition), so size
 * to 0 when disabled.  This matters in bootstrap / single-user (cluster_init_guc
 * never runs, leaving cluster_node_id == -1): the per-node array is ~96 KB, and
 * allocating it from the small bootstrap shared-memory slop would exhaust it
 * (spec-3.12 D2 grew the entry 8 -> 16 bytes, doubling the region).
 */
Size
cluster_tt_slot_shmem_size(void)
{
	if (!cluster_enabled || cluster_node_id < 0)
		return 0;
	return MAXALIGN(sizeof(ClusterTTSlotShmem));
}


void
cluster_tt_slot_shmem_init(void)
{
	bool found;

	if (!cluster_enabled || cluster_node_id < 0)
		return; /* disabled: no allocator region (see cluster_tt_slot_shmem_size) */

	ClusterTTSlotShm = (ClusterTTSlotShmem *)ShmemInitStruct("ClusterTTSlotShmem",
															 cluster_tt_slot_shmem_size(), &found);

	if (!found) {
		int i;

		memset(ClusterTTSlotShm, 0, sizeof(ClusterTTSlotShmem));
		pg_atomic_init_u64(&ClusterTTSlotShm->retention_horizon_scn, 0);
		pg_atomic_init_u64(&ClusterTTSlotShm->tt_slot_retain_skip_count, 0);
		pg_atomic_init_u64(&ClusterTTSlotShm->retention_recycle_count, 0);
		pg_atomic_init_u64(&ClusterTTSlotShm->retention_off_recycle_count, 0);
		pg_atomic_init_u64(&ClusterTTSlotShm->retention_max_recycle_horizon, 0);
		pg_atomic_init_u64(&ClusterTTSlotShm->startup_checkpoint_scn, 0);
		pg_atomic_init_u64(&ClusterTTSlotShm->startup_checkpoint_next_xid, 0);
		pg_atomic_init_u32(&ClusterTTSlotShm->startup_checkpoint_state, 0);
		pg_atomic_init_u64(&ClusterTTSlotShm->tt_slot_wrap_retired_count, 0);
		SpinLockInit(&ClusterTTSlotShm->protected_lock);
		ClusterTTSlotShm->protected_count = 0;
		for (i = 0; i < CLUSTER_TT_SLOT_MAX_NODES; i++)
			LWLockInitialize(&ClusterTTSlotShm->per_node[i].lock, LWTRANCHE_CLUSTER_TT_SLOT);
	}
}


/* ===== spec-3.12 D5 retention counter accessors ===== */

uint64
cluster_tt_slot_retention_horizon_scn(void)
{
	if (ClusterTTSlotShm == NULL)
		return 0;
	return pg_atomic_read_u64(&ClusterTTSlotShm->retention_horizon_scn);
}

uint64
cluster_tt_slot_retain_skip_count(void)
{
	if (ClusterTTSlotShm == NULL)
		return 0;
	return pg_atomic_read_u64(&ClusterTTSlotShm->tt_slot_retain_skip_count);
}

uint64
cluster_tt_slot_retention_recycle_count(void)
{
	if (ClusterTTSlotShm == NULL)
		return 0;
	return pg_atomic_read_u64(&ClusterTTSlotShm->retention_recycle_count);
}

/*
 * cluster_tt_slot_max_recycle_horizon -- spec-6.12i CP5 (D-i4): the monotonic
 * max gate horizon over every horizon-gated COMMITTED recycle this
 * incarnation (slot-level AND segment-level feeds).  InvalidScn until the
 * first gated recycle -- the origin-verdict serve must then refuse a
 * below-horizon claim (fail-closed).
 */
SCN
cluster_tt_slot_max_recycle_horizon(void)
{
	if (ClusterTTSlotShm == NULL)
		return InvalidScn;
	return (SCN)pg_atomic_read_u64(&ClusterTTSlotShm->retention_max_recycle_horizon);
}

/*
 * cluster_tt_slot_retention_off_recycle_count -- spec-3.22: count of COMMITTED
 * slots recycled this incarnation while the retention gate was NOT enforcing.
 * A non-zero value means the spec-3.22 0-match theorem cannot be trusted (an
 * unsound recycle may have removed a committed-after slot), so the xmax gate's
 * RECYCLED_ZERO_MATCH -> invisible shortcut must fail closed (53R9F).
 */
uint64
cluster_tt_slot_retention_off_recycle_count(void)
{
	if (ClusterTTSlotShm == NULL)
		return 0;
	return pg_atomic_read_u64(&ClusterTTSlotShm->retention_off_recycle_count);
}

uint64
cluster_tt_slot_wrap_retired_count(void)
{
	if (ClusterTTSlotShm == NULL)
		return 0;
	return pg_atomic_read_u64(&ClusterTTSlotShm->tt_slot_wrap_retired_count);
}


/* ============================================================
 *	spec-3.15 D6 (V-4): protected-slot map.
 *
 *	Writers: twophase recover (startup, re-pin) and prefinish resolve
 *	(unprotect).  Reader: the allocator gate below.  A tiny spinlock
 *	guards the array; all critical sections are a few comparisons.
 *	protect() is idempotent on the exact same (seg, slot, wrap, xid)
 *	tuple (C-P2: recover may replay).
 * ============================================================ */

bool
cluster_tt_slot_protect(uint32 segment_id, uint16 slot_offset, uint16 wrap, TransactionId xid)
{
	uint32 i;
	bool ok = false;

	if (ClusterTTSlotShm == NULL)
		return false;

	SpinLockAcquire(&ClusterTTSlotShm->protected_lock);
	for (i = 0; i < ClusterTTSlotShm->protected_count; i++) {
		if (ClusterTTSlotShm->protected_slots[i].segment_id == segment_id
			&& ClusterTTSlotShm->protected_slots[i].slot_offset == slot_offset) {
			/* idempotent when identical; conflicting owner is a bug. */
			ok = (ClusterTTSlotShm->protected_slots[i].xid == xid
				  && ClusterTTSlotShm->protected_slots[i].wrap == wrap);
			SpinLockRelease(&ClusterTTSlotShm->protected_lock);
			return ok;
		}
	}
	if (ClusterTTSlotShm->protected_count < CLUSTER_TT_PROTECTED_MAX) {
		uint32 n = ClusterTTSlotShm->protected_count;

		ClusterTTSlotShm->protected_slots[n].segment_id = segment_id;
		ClusterTTSlotShm->protected_slots[n].slot_offset = slot_offset;
		ClusterTTSlotShm->protected_slots[n].wrap = wrap;
		ClusterTTSlotShm->protected_slots[n].xid = xid;
		ClusterTTSlotShm->protected_slots[n]._pad = 0;
		ClusterTTSlotShm->protected_count = n + 1;
		ok = true;
	}
	SpinLockRelease(&ClusterTTSlotShm->protected_lock);
	return ok;
}

uint32
cluster_tt_slot_unprotect_xid(TransactionId xid)
{
	uint32 i;
	uint32 removed = 0;

	if (ClusterTTSlotShm == NULL)
		return 0;

	SpinLockAcquire(&ClusterTTSlotShm->protected_lock);
	i = 0;
	while (i < ClusterTTSlotShm->protected_count) {
		if (ClusterTTSlotShm->protected_slots[i].xid == xid) {
			uint32 last = ClusterTTSlotShm->protected_count - 1;

			ClusterTTSlotShm->protected_slots[i] = ClusterTTSlotShm->protected_slots[last];
			ClusterTTSlotShm->protected_count = last;
			removed++;
			continue; /* re-check swapped-in entry at i */
		}
		i++;
	}
	SpinLockRelease(&ClusterTTSlotShm->protected_lock);
	return removed;
}

bool
cluster_tt_slot_is_protected(uint32 segment_id, uint16 slot_offset)
{
	uint32 i;
	bool hit = false;

	if (ClusterTTSlotShm == NULL || ClusterTTSlotShm->protected_count == 0)
		return false; /* empty-map fast path: no lock */

	SpinLockAcquire(&ClusterTTSlotShm->protected_lock);
	for (i = 0; i < ClusterTTSlotShm->protected_count; i++) {
		if (ClusterTTSlotShm->protected_slots[i].segment_id == segment_id
			&& ClusterTTSlotShm->protected_slots[i].slot_offset == slot_offset) {
			hit = true;
			break;
		}
	}
	SpinLockRelease(&ClusterTTSlotShm->protected_lock);
	return hit;
}

uint32
cluster_tt_slot_protected_count(void)
{
	uint32 n;

	if (ClusterTTSlotShm == NULL)
		return 0;
	SpinLockAcquire(&ClusterTTSlotShm->protected_lock);
	n = ClusterTTSlotShm->protected_count;
	SpinLockRelease(&ClusterTTSlotShm->protected_lock);
	return n;
}


/* ------------------------------------------------------------ */
/* shmem region registration                                    */
/* ------------------------------------------------------------ */

static const ClusterShmemRegion cluster_tt_slot_region = {
	.name = "pgrac cluster tt slot allocator",
	.size_fn = cluster_tt_slot_shmem_size,
	.init_fn = cluster_tt_slot_shmem_init,
	.lwlock_count = CLUSTER_TT_SLOT_MAX_NODES,
	.owner_subsys = "cluster_tt_slot",
	.reserved_flags = 0,
};

void
cluster_tt_slot_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_tt_slot_region);
}


/*
 * cluster_tt_slot_reset_all
 *
 *	Test-only: wipe all per-node allocator state back to zeroes.
 *	Used by cluster_unit harness to set a clean baseline between cases.
 *	Production code MUST NOT call this.
 */
void
cluster_tt_slot_reset_all(void)
{
	int n;

	if (ClusterTTSlotShm == NULL)
		return;

	for (n = 0; n < CLUSTER_TT_SLOT_MAX_NODES; n++) {
		ClusterTTSlotAllocPerSegment *seg = &ClusterTTSlotShm->per_node[n];

		LWLockAcquire(&seg->lock, LW_EXCLUSIVE);
		seg->segment_id = 0;
		seg->binding_generation = 0;
		memset(seg->slots, 0, sizeof(seg->slots));
		LWLockRelease(&seg->lock);
	}
}


/*
 * cluster_tt_slot_test_force_status -- test-only helper to drive the
 * L189 recycle policy (COMMITTED / ABORTED slots become recyclable).
 *
 *	Production code MUST NOT call this.  Production transitions to
 *	COMMITTED/ABORTED status happen via spec-3.4c eager cleanout (not
 *	yet wired).
 */
void
cluster_tt_slot_test_force_status(uint32 segment_id, uint16 slot_offset, uint8 new_status)
{
	ClusterTTSlotAllocPerSegment *seg;

	if (slot_offset >= TT_SLOTS_PER_SEGMENT)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_tt_slot_test_force_status: slot_offset %u out of range",
							   slot_offset)));
	if (new_status != CTS_COMMITTED && new_status != CTS_ABORTED)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_tt_slot_test_force_status: new_status must be %u or %u",
							   (unsigned)CTS_COMMITTED, (unsigned)CTS_ABORTED)));

	seg = cluster_tt_slot_get_or_init(segment_id);

	LWLockAcquire(&seg->lock, LW_EXCLUSIVE);
	seg->slots[slot_offset].status = new_status;
	LWLockRelease(&seg->lock);
}
