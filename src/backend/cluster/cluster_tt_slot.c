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
#include "cluster/cluster_scn.h"			   /* SCN_MAX_VALID_NODE_ID */
#include "cluster/cluster_shmem.h"			   /* ClusterShmemRegion */
#include "cluster/cluster_tt_slot.h"
#include "cluster/storage/cluster_undo_alloc.h" /* CLUSTER_UNDO_SEGS_PER_INSTANCE */
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"


/*
 * Per-slot allocator state.  Distinct from the on-disk TTSlot ABI (see
 * cluster_tt_slot.h); allocator state tracks only what's needed to pick
 * the next free slot and recycle completed ones.  The on-disk TTSlot
 * fields (commit_scn, first_undo_block, …) are wired by spec-3.4c+.
 */
typedef enum ClusterTTSlotAllocStatus
{
	CTS_FREE = 0,			/* available for allocation */
	CTS_ACTIVE = 1,			/* owned by an in-flight xact */
	CTS_COMMITTED = 2,		/* committed; recyclable */
	CTS_ABORTED = 3			/* aborted; recyclable */
} ClusterTTSlotAllocStatus;


typedef struct ClusterTTSlotAllocEntry
{
	TransactionId xid;		/* InvalidTransactionId when CTS_FREE */
	uint16 wrap;			/* reuse counter (L189) */
	uint8 status;			/* ClusterTTSlotAllocStatus */
	uint8 _pad;				/* explicit padding to 8 bytes */
} ClusterTTSlotAllocEntry;

StaticAssertDecl(sizeof(ClusterTTSlotAllocEntry) == 8,
				 "spec-3.4b D3: allocator entry must be 8 bytes for predictable shmem sizing");


typedef struct ClusterTTSlotAllocPerSegment
{
	LWLock lock;
	uint32 segment_id;		/* 0 = not yet initialised; otherwise == derived id */
	ClusterTTSlotAllocEntry slots[TT_SLOTS_PER_SEGMENT];
} ClusterTTSlotAllocPerSegment;


#define CLUSTER_TT_SLOT_MAX_NODES 128		/* matches SCN_MAX_VALID_NODE_ID + 1 */

typedef struct ClusterTTSlotShmem
{
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
	return (int) ((segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE);
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
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cluster TT slot allocator shmem not initialised")));

	if (segment_id == 0 || segment_id > UINT16_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster_tt_slot: segment_id %u out of range (1, %u]",
						segment_id, (unsigned) UINT16_MAX)));

	node_id = cluster_tt_slot_segment_to_node(segment_id);
	if (node_id < 0 || node_id >= CLUSTER_TT_SLOT_MAX_NODES)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster_tt_slot: segment_id %u derives node_id %d outside [0, %d)",
						segment_id, node_id, CLUSTER_TT_SLOT_MAX_NODES)));

	seg = &ClusterTTSlotShm->per_node[node_id];

	if (seg->segment_id == 0)
	{
		/*
		 * First-touch initialisation.  Take the lock so concurrent first
		 * touches on the same node race-free.  All other fields are zero
		 * by shmem init (CTS_FREE == 0, wrap == 0, xid == 0).
		 */
		LWLockAcquire(&seg->lock, LW_EXCLUSIVE);
		if (seg->segment_id == 0)
			seg->segment_id = segment_id;
		LWLockRelease(&seg->lock);
	}
	else if (seg->segment_id != segment_id)
	{
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
 * cluster_tt_slot_alloc
 *
 *	Three-tier fallback (L189 recycle policy).  See header for the
 *	contract.
 */
uint16
cluster_tt_slot_alloc(uint32 segment_id, TransactionId top_xid)
{
	ClusterTTSlotAllocPerSegment *seg;
	int reusable_idx = -1;
	int free_idx = -1;
	uint16 chosen;
	int i;

	if (!TransactionIdIsValid(top_xid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster_tt_slot_alloc: top_xid must be valid")));

	seg = cluster_tt_slot_get_or_init(segment_id);

	LWLockAcquire(&seg->lock, LW_EXCLUSIVE);

	/* Pass 1: idempotent reuse of own ACTIVE slot. */
	for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++)
	{
		ClusterTTSlotAllocEntry *e = &seg->slots[i];

		if (e->status == CTS_ACTIVE && e->xid == top_xid)
		{
			LWLockRelease(&seg->lock);
			return (uint16) i;
		}
		if (e->status == CTS_FREE && free_idx < 0)
			free_idx = i;
		else if ((e->status == CTS_COMMITTED || e->status == CTS_ABORTED)
				 && reusable_idx < 0)
			reusable_idx = i;
	}

	/* Pass 2: prefer FREE over recyclable. */
	if (free_idx >= 0)
	{
		ClusterTTSlotAllocEntry *e = &seg->slots[free_idx];

		e->xid = top_xid;
		e->status = CTS_ACTIVE;
		/* wrap unchanged on FREE → ACTIVE; first allocation keeps wrap=0 */
		chosen = (uint16) free_idx;
	}
	else if (reusable_idx >= 0)
	{
		ClusterTTSlotAllocEntry *e = &seg->slots[reusable_idx];

		e->xid = top_xid;
		e->status = CTS_ACTIVE;
		/* L189 wrap++ on recycle (saturate at TT_WRAP_MAX, defends ABA) */
		if (e->wrap < TT_WRAP_MAX)
			e->wrap++;
		chosen = (uint16) reusable_idx;
	}
	else
	{
		/* All 48 slots ACTIVE → OVERFLOW. */
		LWLockRelease(&seg->lock);
		return INVALID_TT_SLOT_OFFSET;
	}

	LWLockRelease(&seg->lock);
	return chosen;
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
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster_tt_slot_free: slot_offset %u out of range [0, %d)",
						slot_offset, TT_SLOTS_PER_SEGMENT)));

	seg = cluster_tt_slot_get_or_init(segment_id);

	LWLockAcquire(&seg->lock, LW_EXCLUSIVE);
	e = &seg->slots[slot_offset];
	e->status = CTS_FREE;
	e->xid = InvalidTransactionId;
	/* wrap is preserved across FREE → next ACTIVE; only recycle bumps it */
	LWLockRelease(&seg->lock);
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
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster_tt_slot_get_wrap: slot_offset %u out of range [0, %d)",
						slot_offset, TT_SLOTS_PER_SEGMENT)));

	seg = cluster_tt_slot_get_or_init(segment_id);

	LWLockAcquire(&seg->lock, LW_SHARED);
	wrap = seg->slots[slot_offset].wrap;
	LWLockRelease(&seg->lock);

	return wrap;
}


/* ===== shmem lifecycle ===== */

Size
cluster_tt_slot_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterTTSlotShmem));
}


void
cluster_tt_slot_shmem_init(void)
{
	bool found;
	int i;

	ClusterTTSlotShm = (ClusterTTSlotShmem *)
		ShmemInitStruct("ClusterTTSlotShmem",
						cluster_tt_slot_shmem_size(),
						&found);

	if (!found)
	{
		memset(ClusterTTSlotShm, 0, sizeof(ClusterTTSlotShmem));
		for (i = 0; i < CLUSTER_TT_SLOT_MAX_NODES; i++)
			LWLockInitialize(&ClusterTTSlotShm->per_node[i].lock,
							 LWTRANCHE_CLUSTER_TT_SLOT);
	}
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

	for (n = 0; n < CLUSTER_TT_SLOT_MAX_NODES; n++)
	{
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
cluster_tt_slot_test_force_status(uint32 segment_id, uint16 slot_offset,
								  uint8 new_status)
{
	ClusterTTSlotAllocPerSegment *seg;

	if (slot_offset >= TT_SLOTS_PER_SEGMENT)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster_tt_slot_test_force_status: slot_offset %u out of range",
						slot_offset)));
	if (new_status != CTS_COMMITTED && new_status != CTS_ABORTED)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster_tt_slot_test_force_status: new_status must be %u or %u",
						(unsigned) CTS_COMMITTED, (unsigned) CTS_ABORTED)));

	seg = cluster_tt_slot_get_or_init(segment_id);

	LWLockAcquire(&seg->lock, LW_EXCLUSIVE);
	seg->slots[slot_offset].status = new_status;
	LWLockRelease(&seg->lock);
}
