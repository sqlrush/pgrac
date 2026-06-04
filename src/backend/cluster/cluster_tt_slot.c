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
#include "cluster/cluster_scn.h"   /* SCN_MAX_VALID_NODE_ID */
#include "cluster/cluster_shmem.h" /* ClusterShmemRegion */
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_undo_retention.h"		/* horizon + recyclable predicate */
#include "cluster/storage/cluster_undo_alloc.h" /* CLUSTER_UNDO_SEGS_PER_INSTANCE */
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
		if (seg->segment_id == 0)
			seg->segment_id = segment_id;
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
 * cluster_tt_slot_alloc_ext
 *
 *	Three-tier fallback (L189 recycle policy) with the spec-3.12 retention
 *	gate layered on Pass 2:
 *	    1) reuse a slot already owned by `top_xid` (idempotent)
 *	    2) take any FREE slot
 *	    3) recycle a *retention-eligible* COMMITTED / ABORTED slot, wrap++
 *	A COMMITTED slot is retention-eligible only when commit_scn is older than
 *	the horizon
 *	(cluster_tt_slot_recyclable); ABORTED is always eligible (C7).  When the
 *	retention GUC is off, the gate is bypassed (spec-3.11 immediate recycle, C6).
 *
 *	Returns INVALID_TT_SLOT_OFFSET when no slot can be handed out.  In that
 *	case *out_retained_pressure (when non-NULL) distinguishes the two reasons:
 *	    true  -> at least one COMMITTED slot is being kept alive by retention
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
	int reusable_idx = -1;
	int free_idx = -1;
	bool retained_pressure = false;
	bool gate_enabled;
	SCN horizon = InvalidScn;
	uint16 chosen;
	uint64 retain_skip_seen = 0; /* spec-3.12 D5 */
	int i;

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
	if (gate_enabled) {
		horizon = cluster_undo_retention_horizon();
		/* spec-3.12 D5 / C16: sample the horizon gauge at the decision point. */
		if (ClusterTTSlotShm != NULL)
			pg_atomic_write_u64(&ClusterTTSlotShm->retention_horizon_scn, (uint64)horizon);
	}

	seg = cluster_tt_slot_get_or_init(segment_id);

	LWLockAcquire(&seg->lock, LW_EXCLUSIVE);

	/* Pass 1: classify slots (idempotent reuse short-circuits). */
	for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		const ClusterTTSlotAllocEntry *e = &seg->slots[i];

		if (e->status == CTS_ACTIVE && e->xid == top_xid) {
			LWLockRelease(&seg->lock);
			return (uint16)i;
		}
		if (e->status == CTS_FREE) {
			if (free_idx < 0)
				free_idx = i;
		} else if (e->status == CTS_COMMITTED || e->status == CTS_ABORTED) {
			bool recyclable = gate_enabled
								  ? cluster_tt_slot_recyclable(e->status, e->commit_scn, horizon)
								  : true; /* GUC off: spec-3.11 immediate recycle (C6) */

			if (recyclable) {
				if (reusable_idx < 0)
					reusable_idx = i;
			} else {
				/* COMMITTED and not older than horizon: retention keeps it alive. */
				retained_pressure = true;
				retain_skip_seen++; /* C16: per skip event, not de-duped */
			}
		}
		/* CTS_ACTIVE not owned by top_xid: in-flight; nothing to do. */
	}

	if (retain_skip_seen > 0 && ClusterTTSlotShm != NULL)
		pg_atomic_fetch_add_u64(&ClusterTTSlotShm->tt_slot_retain_skip_count, retain_skip_seen);

	/* Pass 2: prefer FREE over a retention-eligible recyclable slot. */
	if (free_idx >= 0) {
		ClusterTTSlotAllocEntry *e = &seg->slots[free_idx];

		e->xid = top_xid;
		e->status = CTS_ACTIVE;
		e->commit_scn = InvalidScn;
		/* wrap unchanged on FREE → ACTIVE; first allocation keeps wrap=0 */
		chosen = (uint16)free_idx;
	} else if (reusable_idx >= 0) {
		ClusterTTSlotAllocEntry *e = &seg->slots[reusable_idx];
		bool was_committed = (e->status == CTS_COMMITTED);

		e->xid = top_xid;
		e->status = CTS_ACTIVE;
		e->commit_scn = InvalidScn;
		/* L189 wrap++ on recycle (saturate at TT_WRAP_MAX, defends ABA) */
		if (e->wrap < TT_WRAP_MAX)
			e->wrap++;
		chosen = (uint16)reusable_idx;

		/* spec-3.12 D5: a COMMITTED slot recycled past the horizon (ABORTED was
		 * never retention-gated, so it does not count). */
		if (was_committed && ClusterTTSlotShm != NULL)
			pg_atomic_fetch_add_u64(&ClusterTTSlotShm->retention_recycle_count, 1);
	} else {
		/*
		 * No FREE and no retention-eligible slot.  Hand the reason back so the
		 * caller can decide rollover (retained pressure) vs hard error (all
		 * ACTIVE) -- spec-3.12 D2b / C3b.
		 */
		LWLockRelease(&seg->lock);
		if (out_retained_pressure)
			*out_retained_pressure = retained_pressure;
		return INVALID_TT_SLOT_OFFSET;
	}

	LWLockRelease(&seg->lock);
	return chosen;
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
			/* keep xid + wrap so the durable header slot stays addressable. */
		}
	}
	LWLockRelease(&seg->lock);
}


/*
 * cluster_tt_slot_mark_aborted -- spec-3.12 D2 / C7.
 *
 *	ACTIVE -> ABORTED.  Aborted versions are invisible to every read_scn (abort
 *	already rolled the row back in place; CR rebuilds committed history only),
 *	so the slot is immediately recyclable -- no horizon retention.  commit_scn
 *	is cleared.  Same defensive ownership + rolled-away (D2b) guards as
 *	mark_committed.
 */
void
cluster_tt_slot_mark_aborted(uint32 segment_id, uint16 slot_offset, TransactionId xid)
{
	ClusterTTSlotAllocPerSegment *seg;

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
		}
	}
	LWLockRelease(&seg->lock);
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
	for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		if (seg->slots[i].status == CTS_ACTIVE) {
			old_had_active = true;
			break;
		}
	}
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
