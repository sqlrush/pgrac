/*-------------------------------------------------------------------------
 *
 * cluster_itl.c
 *	  pgrac cluster ITL slot read-only helper — extract TT slot ref
 *	  from a heap page's ITL array (PD_HAS_ITL special area).
 *
 *	  spec-3.1 D4 (NEW).
 *
 *	  This is foundation read-only access only.  No mutation of the
 *	  ITL slot or the page;  no cleanout write-back;  no TT slot
 *	  allocation.  spec-3.4 will activate ITL writable path
 *	  (commit_scn persistence + delayed cleanout);  this file ships
 *	  the read side that spec-3.2 visibility code consumes.
 *
 *	  See cluster_itl.h for the public contract.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.1-cluster-xid-status-foundation.md
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_itl.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam_xlog.h"	 /* xl_heap_itl_delta / _v2 / _block (spec-3.4b D6) */
#include "access/htup_details.h" /* HeapTupleHeaderData for t_itl_slot_idx */
#include "access/xact.h"		 /* GetCurrentTransactionNestLevel (spec-3.4a N9) */
#include "storage/bufmgr.h"		 /* BufferGetPage, MarkBufferDirty (spec-3.4a D2) */
#include "storage/bufpage.h"
#include "access/multixact.h"		   /* MultiXactId / MultiXactIdIsValid */
#include "cluster/cluster_epoch.h"	   /* cluster_epoch_get_current (spec-3.4b D7) */
#include "cluster/cluster_gcs_block.h" /* spec-5.2 D11 cluster_bufmgr_block_write_permitted */
#include "cluster/cluster_guc.h"	   /* cluster_enabled */
#include "cluster/cluster_itl.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_uba.h"			/* uba_decode / uba_origin_node_id (spec-3.4b D7) */
#include "cluster/cluster_recovery_merge.h" /* spec-4.5a G6: materialized-origin slot pin */
#include "cluster/cluster_shmem.h"			/* cluster_shmem_register_region (spec-3.4e D6) */
#include "cluster/cluster_xnode_profile.h"	/* PGRAC: spec-5.59 D7 profiling */
#include "miscadmin.h"						/* IsBootstrapProcessingMode (spec-3.4e D6) */
#include "port/atomics.h"					/* pg_atomic_uint64 (spec-3.4d D11 counters) */
#include "storage/ipc.h"					/* ShmemInitStruct (spec-3.4e D6) */
#include "storage/shmem.h"

#ifdef USE_PGRAC_CLUSTER

/*
 * spec-3.4d D11:  5 NEW lock-path counter for row-lock observability.
 *
 *	Backend-local atomic counters for 4/5 — per backend's lifetime,
 *	snapshot via getter at view query time.  Full shmem-backed cross-
 *	backend aggregation for remaining 4 counter deferred to Hardening
 *	v1.0.1 when the dump_lock_path category lands in pg_stat_cluster_state.
 *
 *	spec-3.4e D6 / F2 P0 amendment:
 *	  ClusterRemoteRowLockFailClosedCount promoted to shmem-attached
 *	  atomic so pgbench collector backend can read deltas across N
 *	  client backends.  Without this, class 4 hot-row metric reads 0
 *	  from collector backend (per-backend counter increments invisible).
 *	  Other 4 counters remain per-backend (Sprint A scope control;
 *	  完整 5 counter aggregation 推 Hardening v1.0.1).
 */
static pg_atomic_uint64 ClusterItlOverflowLockCount;
static pg_atomic_uint64 ClusterMultixactLockRejectCount;
static pg_atomic_uint64 ClusterLockOnlyItlStampCount;
static pg_atomic_uint64 ClusterLockOnlyTtHintEmitCount;
static bool ClusterLockPathCountersInited = false;

/* spec-3.4e D6:  shmem-attached pointer for the cross-backend
 * aggregated fail_closed counter.  NULL fallback uses an unattached
 * per-backend atomic so cluster_unit / non-postmaster paths keep
 * working (Q9 minimal scope). */
typedef struct ClusterLockPathShmem {
	pg_atomic_uint64 remote_row_lock_fail_closed_count;
} ClusterLockPathShmem;

static ClusterLockPathShmem *ClusterLockPathShmemState = NULL;
static pg_atomic_uint64 ClusterRemoteRowLockFailClosedFallback;

static inline void
ensure_lock_counters_inited(void)
{
	if (!ClusterLockPathCountersInited) {
		pg_atomic_init_u64(&ClusterItlOverflowLockCount, 0);
		pg_atomic_init_u64(&ClusterMultixactLockRejectCount, 0);
		pg_atomic_init_u64(&ClusterRemoteRowLockFailClosedFallback, 0);
		pg_atomic_init_u64(&ClusterLockOnlyItlStampCount, 0);
		pg_atomic_init_u64(&ClusterLockOnlyTtHintEmitCount, 0);
		ClusterLockPathCountersInited = true;
	}
}

/*
 * spec-3.4e D6 shmem region:  fail_closed counter cross-backend
 * aggregation.  Other lock-path counters remain per-backend (Sprint A
 * scope control;  Hardening v1.0.1 expands).
 */
static Size
cluster_lock_path_shmem_size(void)
{
	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return 0;
	return MAXALIGN(sizeof(ClusterLockPathShmem));
}

static void
cluster_lock_path_shmem_init(void)
{
	bool found;

	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return;

	ClusterLockPathShmemState = (ClusterLockPathShmem *)ShmemInitStruct(
		"ClusterLockPathShmem", MAXALIGN(sizeof(ClusterLockPathShmem)), &found);
	if (!found)
		pg_atomic_init_u64(&ClusterLockPathShmemState->remote_row_lock_fail_closed_count, 0);
}

static const ClusterShmemRegion cluster_lock_path_region = {
	.name = "pgrac cluster lock-path counters",
	.size_fn = cluster_lock_path_shmem_size,
	.init_fn = cluster_lock_path_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_itl_lock_path",
	.reserved_flags = 0,
};

void
cluster_lock_path_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_lock_path_region);
}

/*
 * Resolve the active fail_closed counter pointer:
 *   - postmaster context with shmem attached → shmem-aggregated
 *   - otherwise → per-backend fallback (cluster_unit / bootstrap)
 */
static inline pg_atomic_uint64 *
fail_closed_counter(void)
{
	if (ClusterLockPathShmemState != NULL)
		return &ClusterLockPathShmemState->remote_row_lock_fail_closed_count;
	return &ClusterRemoteRowLockFailClosedFallback;
}

void
cluster_itl_bump_overflow_lock_count(void)
{
	ensure_lock_counters_inited();
	pg_atomic_fetch_add_u64(&ClusterItlOverflowLockCount, 1);
}

void
cluster_itl_bump_multixact_lock_reject_count(void)
{
	ensure_lock_counters_inited();
	pg_atomic_fetch_add_u64(&ClusterMultixactLockRejectCount, 1);
}

void
cluster_itl_bump_remote_row_lock_fail_closed_count(void)
{
	ensure_lock_counters_inited();
	pg_atomic_fetch_add_u64(fail_closed_counter(), 1);
}

void
cluster_itl_bump_lock_only_itl_stamp_count(void)
{
	ensure_lock_counters_inited();
	pg_atomic_fetch_add_u64(&ClusterLockOnlyItlStampCount, 1);
}

void
cluster_itl_bump_lock_only_tt_hint_emit_count(void)
{
	ensure_lock_counters_inited();
	pg_atomic_fetch_add_u64(&ClusterLockOnlyTtHintEmitCount, 1);
}

uint64
cluster_itl_get_overflow_lock_count(void)
{
	if (!ClusterLockPathCountersInited)
		return 0;
	return pg_atomic_read_u64(&ClusterItlOverflowLockCount);
}

uint64
cluster_itl_get_multixact_lock_reject_count(void)
{
	if (!ClusterLockPathCountersInited)
		return 0;
	return pg_atomic_read_u64(&ClusterMultixactLockRejectCount);
}

uint64
cluster_itl_get_remote_row_lock_fail_closed_count(void)
{
	if (ClusterLockPathShmemState != NULL)
		return pg_atomic_read_u64(&ClusterLockPathShmemState->remote_row_lock_fail_closed_count);
	if (!ClusterLockPathCountersInited)
		return 0;
	return pg_atomic_read_u64(&ClusterRemoteRowLockFailClosedFallback);
}

uint64
cluster_itl_get_lock_only_itl_stamp_count(void)
{
	if (!ClusterLockPathCountersInited)
		return 0;
	return pg_atomic_read_u64(&ClusterLockOnlyItlStampCount);
}

uint64
cluster_itl_get_lock_only_tt_hint_emit_count(void)
{
	if (!ClusterLockPathCountersInited)
		return 0;
	return pg_atomic_read_u64(&ClusterLockOnlyTtHintEmitCount);
}

bool
cluster_itl_get_tt_ref(Page page, uint8 itl_slot_idx, ClusterUndoTTSlotRef *ref)
{
	const ClusterItlSlotData *slot;
	const ClusterItlSlotData *slots;

	if (page == NULL || ref == NULL)
		return false;

	/* Page must declare it carries an ITL special area. */
	if (!PageHasItl(page))
		return false;

	/* Index range guard.  CLUSTER_ITL_INITRANS_DEFAULT is the
	 * fixed spec-1.5 placeholder count; spec-3.4 may make this
	 * per-table dynamic and at that point this guard becomes the
	 * page-derived bound. */
	if (itl_slot_idx >= CLUSTER_ITL_INITRANS_DEFAULT)
		return false;

	slots = ClusterPageGetItlSlots(page);
	slot = &slots[itl_slot_idx];

	/*
	 * spec-3.4b D7: three-branch reader.
	 *
	 *   1. FREE slot          → no binding to report (return false).
	 *   2. UBA_is_invalid(slot->undo_segment_head)
	 *                         → legacy (spec-3.4a) ACTIVE/COMMITTED slot
	 *                            with placeholder UBA.  Return zero triple
	 *                            so spec-3.2 D5 falls back to PG-native
	 *                            silent-invisible.  Backward-compat with
	 *                            spec-3.4a stamps + legacy v1 WAL records
	 *                            replayed without UBA bytes.
	 *   3. real UBA           → decode UBA + derive owner_node + map slot
	 *                            offset to exact-key tt_slot_id (offset+1).
	 *                            decode/owner failure raises
	 *                            ERRCODE_DATA_CORRUPTED (real corruption
	 *                            on a real binding is unrecoverable here).
	 */
	if (slot->flags == ITL_FLAG_FREE)
		return false;

	memset(ref, 0, sizeof(*ref));

	if (UBA_is_invalid(slot->undo_segment_head)) {
		/* Legacy spec-3.4a stamp; pre-3.4b ITL slots carry InvalidUba.
		 * Reader falls back to zero triple — spec-3.1/3.4a behavior. */
		ref->origin_node_id = 0;
		ref->undo_segment_id = 0;
		ref->tt_slot_id = 0;
	} else {
		uint32 seg_id;
		uint32 blk_no;
		uint16 tt_off;
		uint16 row_off;
		NodeId origin;

		if (!uba_decode(slot->undo_segment_head, &seg_id, &blk_no, &tt_off, &row_off))
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("malformed UBA in ITL slot %u", itl_slot_idx)));
		origin = uba_origin_node_id(slot->undo_segment_head);
		if (origin == InvalidNodeId)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("UBA decode: segment_id %u has no valid owner_instance", seg_id)));
		ref->origin_node_id = (uint16)origin;
		ref->undo_segment_id = (uint16)seg_id;
		ref->tt_slot_id = cluster_tt_slot_offset_to_id(tt_off);
	}

	ref->cluster_epoch = (uint32)cluster_epoch_get_current();
	ref->local_xid = slot->xid;
	ref->cached_commit_scn = slot->commit_scn;
	ref->has_cached_status = (slot->flags == ITL_FLAG_COMMITTED && SCN_VALID(slot->commit_scn));
	/* _padding cleared by memset above. */

	return true;
}

/*
 * cluster_itl_find_lock_tt_ref_by_xmax (spec-3.4d D1 / F2):
 *
 *	Scan the page's ITL slot array for a LOCK_ONLY slot whose xid
 *	matches raw_xmax + decode UBA + fill ref.  See header for full
 *	contract.  This is the derive-not-store path that
 *	replaces v0.1's t_lock_itl_slot_idx tuple header field (rejected
 *	by F2 due to MAXALIGN tax + disk format break).
 */
bool
cluster_itl_find_lock_tt_ref_by_xmax(Page page, TransactionId raw_xmax, ClusterUndoTTSlotRef *ref)
{
	const ClusterItlSlotData *slots;
	uint32 current_epoch;
	int match_idx = -1;
	uint16 match_wrap = 0;
	uint8 i;
	int match_count = 0;

	Assert(page != NULL);
	Assert(ref != NULL);

	if (!PageHasItl(page))
		return false;
	if (!TransactionIdIsValid(raw_xmax))
		return false;

	slots = ClusterPageGetItlSlots(page);
	current_epoch = (uint32)cluster_epoch_get_current();

	/* Scan all 8 slots looking for LOCK_ONLY + exact xid + valid UBA.
	 * Pick the highest wrap (generation counter) if multiple candidates
	 * match — recycled slot conservatism per spec-3.4d §6.1 step 6.
	 * Ambiguous duplicate (multiple LOCK_ONLY slots with same xid + same
	 * wrap) → fail closed:  page metadata corruption. */
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		const ClusterItlSlotData *slot = &slots[i];

		if (!ITL_FLAG_IS_LOCK_ONLY(slot->flags))
			continue;
		if (slot->xid != raw_xmax)
			continue;
		if (UBA_is_invalid(slot->undo_segment_head))
			continue;

		match_count++;
		if (match_idx < 0 || slot->wrap > match_wrap) {
			match_idx = (int)i;
			match_wrap = slot->wrap;
		} else if (slot->wrap == match_wrap) {
			/* Two slots with identical (xid, wrap) — true ambiguity. */
			match_count++; /* defensive — keep going to count duplicates */
		}
	}

	if (match_idx < 0)
		return false;

	/* If highest-wrap duplicates remain, the caller has no authoritative
	 * lock-only ref and must treat this as no match. */
	if (match_count > 1) {
		const ClusterItlSlotData *winner = &slots[match_idx];
		int ambiguous = 0;

		/* Re-scan to verify whether the highest-wrap slot is unique. */
		for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
			const ClusterItlSlotData *slot = &slots[i];

			if (!ITL_FLAG_IS_LOCK_ONLY(slot->flags))
				continue;
			if (slot->xid != raw_xmax)
				continue;
			if (UBA_is_invalid(slot->undo_segment_head))
				continue;
			if (slot->wrap == winner->wrap)
				ambiguous++;
		}

		if (ambiguous > 1)
			return false;
	}

	{
		const ClusterItlSlotData *slot = &slots[match_idx];
		uint32 seg_id;
		uint32 blk_no;
		uint16 tt_off;
		uint16 row_off;
		NodeId origin;

		memset(ref, 0, sizeof(*ref));

		if (!uba_decode(slot->undo_segment_head, &seg_id, &blk_no, &tt_off, &row_off))
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("malformed UBA in lock-only ITL slot %d", match_idx)));
		origin = uba_origin_node_id(slot->undo_segment_head);
		if (origin == InvalidNodeId)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("UBA decode: lock-only segment_id %u has no valid owner_instance",
							seg_id)));

		ref->origin_node_id = (uint16)origin;
		ref->undo_segment_id = (uint16)seg_id;
		ref->tt_slot_id = cluster_tt_slot_offset_to_id(tt_off);
		ref->cluster_epoch = current_epoch;
		ref->local_xid = slot->xid;
		ref->cached_commit_scn = InvalidScn; /* lock-only never carries commit_scn */
		ref->has_cached_status = false;
	}

	return true;
}

/* ---------- spec-3.4a D2 — writer API ---------- */

static inline bool
cluster_itl_slot_is_completed_reusable(uint8 flags)
{
	return flags == ITL_FLAG_COMMITTED || flags == ITL_FLAG_ABORTED
		   || flags == ITL_FLAG_NEEDS_CLEANOUT || ITL_FLAG_IS_LOCK_ONLY_COMPLETED(flags);
}

/*
 * spec-4.5a G6 (P1 #1): a completed DATA slot whose undo anchor points at a
 * foreign origin this node MATERIALIZED during merged recovery is the ONLY
 * surviving origin/wrap evidence for any live tuple still pointing at it (a
 * PG heap tuple carries a bare 32-bit xmin -- no origin, no wrap).  Recycling
 * it would strip that evidence and make a foreign tuple look local, aliasing
 * its raw xid into THIS node's CLOG (AD-012 例外 9 -> false-visible).  Pin
 * such slots: no allocator path may reuse one.  Lock-only / placeholder slots
 * (no data UBA) carry no tuple-origin evidence and are not pinned.
 *
 * Backend-lifetime caches keep the hot write path cheap: the any-materialized
 * gate short-circuits every non-merged node (no marker file I/O at all), and
 * a per-origin cache amortizes the marker read.  Materialization only happens
 * during startup recovery (no live backends), so neither cache can go stale
 * in the unsafe direction.
 */
static int8 itl_any_materialized_cache = 0;								 /* 0 ? / 1 yes / -1 no */
static int8 itl_origin_materialized_cache[CLUSTER_WAL_STATE_SLOT_COUNT]; /* 0 ? / 1 / -1 */

static inline bool
cluster_itl_slot_is_protected_foreign(const ClusterItlSlotData *slot)
{
	NodeId origin;
	int o;

	if (UBA_is_invalid(slot->undo_segment_head))
		return false; /* FREE / lock-only / placeholder: no data-origin anchor */

	/*
	 * Only completed DATA slots anchor a live tuple's origin: a heap tuple's
	 * t_itl_slot_idx never points at a lock-only slot, so a lock-only slot
	 * (even one that took a TT binding) carries no origin evidence to
	 * protect.  Pinning it would only over-restrict the lock allocator on a
	 * merged page (still fail-closed, never false-visible) -- exclude it so
	 * the pin stays precise.
	 */
	if (ITL_FLAG_IS_LOCK_ONLY_COMPLETED(slot->flags))
		return false;

	if (itl_any_materialized_cache == 0)
		itl_any_materialized_cache = cluster_merged_any_remote_materialized() ? 1 : -1;
	if (itl_any_materialized_cache != 1)
		return false; /* this node materialized nothing -> nothing to pin */

	origin = uba_origin_node_id(slot->undo_segment_head);
	if (origin == InvalidNodeId || (int32)origin == cluster_node_id)
		return false; /* own-origin (or unreadable) slot: freely reusable */

	o = (int)origin;
	if (o < 0 || o >= CLUSTER_WAL_STATE_SLOT_COUNT)
		return false;
	if (itl_origin_materialized_cache[o] == 0)
		itl_origin_materialized_cache[o] = cluster_merged_instance_is_materialized(o) ? 1 : -1;
	return itl_origin_materialized_cache[o] == 1;
}

/*
 * itl_require_block_x_for_write — spec-5.2 §3.5 D11 guard.
 *
 *	Allocating / stamping an ITL slot is the forward "I am about to write or
 *	lock this block" step and requires PCM X ownership.  A cluster writer can
 *	reach here holding only a DEFERRED READ-IMAGE (a remote node holds X and
 *	deferred its writer-transfer because it still has an uncommitted ITL slot)
 *	when the writer's OWN target row was not the contended one, so it never
 *	entered the cross-node TX wait that would have reacquired X.  Mutating that
 *	non-owned copy would diverge from the holder's X copy (silent lost-update,
 *	Rule 8.A).  Fail closed retryably; the contended-row path waits in the heap
 *	AM and reacquires X before reaching here, so it passes
 *	cluster_bufmgr_block_write_permitted() (which returns true for every
 *	pcm_state except the deferred-writer read-image marker).
 */
static void
itl_require_block_x_for_write(Buffer buf)
{
	if (!cluster_bufmgr_block_write_permitted(buf))
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT),
						errmsg("cannot write a block held in X by a remote node with an "
							   "uncommitted transaction"),
						errhint("Cross-node writer-transfer is deferred while the remote holder "
								"still has an uncommitted ITL slot; retry the transaction.")));
}

bool
cluster_itl_alloc_or_reuse_slot(Buffer buf, TransactionId top_xid, uint8 *out_slot_idx)
{
	Page page;
	const ClusterItlSlotData *slots;
	uint8 i;
	int free_idx;
	int reusable_idx;
	ClusterXpScope xps;

	/* PGRAC: spec-5.59 D7 profiling */
	cluster_xp_begin(&xps, CLXP_LOCAL_UNDO_ITL_WAL);

	Assert(BufferIsValid(buf));
	Assert(TransactionIdIsValid(top_xid));
	Assert(out_slot_idx != NULL);

	page = BufferGetPage(buf);

	if (!PageHasItl(page)) {
		cluster_xp_end(&xps); /* PGRAC: spec-5.59 D7 profiling */
		return false;
	}

	/* spec-5.2 §3.5 D11: a forward ITL write requires X. */
	itl_require_block_x_for_write(buf);

	slots = ClusterPageGetItlSlots(page);
	free_idx = -1;
	reusable_idx = -1;

	/*
	 * spec-3.4a N7: one ITL slot per (page, top_xid).  Reuse an
	 * existing ACTIVE slot if it already belongs to top_xid; otherwise
	 * remember the first FREE slot we see.  If all slots are occupied
	 * but none is ACTIVE for another transaction, recycle the first
	 * completed slot.  Full delayed cleanout/freeing lands in 3.4c, but
	 * 3.4a must not make every hot page fail after INITRANS completed
	 * transactions.
	 */
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		if (slots[i].flags == ITL_FLAG_ACTIVE && slots[i].xid == top_xid) {
			*out_slot_idx = i;
			cluster_xp_end(&xps); /* PGRAC: spec-5.59 D7 profiling */
			return true;
		}
		if (slots[i].flags == ITL_FLAG_FREE && free_idx < 0)
			free_idx = i;
		else if (cluster_itl_slot_is_completed_reusable(slots[i].flags) && reusable_idx < 0
				 && !cluster_itl_slot_is_protected_foreign(&slots[i]))
			reusable_idx = i; /* spec-4.5a G6: never reuse a pinned foreign slot */
	}

	if (free_idx >= 0) {
		*out_slot_idx = (uint8)free_idx;
		cluster_xp_end(&xps); /* PGRAC: spec-5.59 D7 profiling */
		return true;
	}

	if (reusable_idx >= 0) {
		*out_slot_idx = (uint8)reusable_idx;
		cluster_xp_end(&xps); /* PGRAC: spec-5.59 D7 profiling */
		return true;
	}

	cluster_xp_end(&xps); /* PGRAC: spec-5.59 D7 profiling */
	return false;		  /* OVERFLOW — caller raises ERROR before CRIT */
}

bool
cluster_itl_alloc_or_reuse_lock_slot(Buffer buf, TransactionId top_xid, uint8 *out_slot_idx)
{
	Page page;
	const ClusterItlSlotData *slots;
	uint8 i;
	int free_idx;
	int reusable_idx;

	Assert(BufferIsValid(buf));
	Assert(TransactionIdIsValid(top_xid));
	Assert(out_slot_idx != NULL);

	page = BufferGetPage(buf);

	if (!PageHasItl(page))
		return false;

	/* spec-5.2 §3.5 D11: a forward ITL row-lock requires X. */
	itl_require_block_x_for_write(buf);

	slots = ClusterPageGetItlSlots(page);
	free_idx = -1;
	reusable_idx = -1;

	/*
	 * spec-3.4d F9: lock-only allocation must not reuse a data ACTIVE
	 * slot for the same xid.  Data and lock-only slots have different
	 * consumers: data visibility uses t_itl_slot_idx, while lock visibility
	 * derives from raw_xmax + LOCK_ONLY scan.  Converting data ACTIVE to
	 * LOCK_ONLY_ACTIVE would orphan the tuple's data ITL metadata.
	 */
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		if (slots[i].flags == ITL_FLAG_LOCK_ONLY_ACTIVE && slots[i].xid == top_xid) {
			*out_slot_idx = i;
			return true;
		}
		if (slots[i].flags == ITL_FLAG_FREE && free_idx < 0)
			free_idx = i;
		else if (cluster_itl_slot_is_completed_reusable(slots[i].flags) && reusable_idx < 0
				 && !cluster_itl_slot_is_protected_foreign(&slots[i]))
			reusable_idx = i; /* spec-4.5a G6: never reuse a pinned foreign slot */
	}

	if (free_idx >= 0) {
		*out_slot_idx = (uint8)free_idx;
		return true;
	}

	if (reusable_idx >= 0) {
		*out_slot_idx = (uint8)reusable_idx;
		return true;
	}

	return false;
}

/*
 * cluster_itl_find_multixact_origin_by_xmax (spec-3.6 v0.3 D7b NEW)
 *
 *	Helper for D6 heapam_visibility reader.  Scan page ITL for marker
 *	slot stamped by D7b (cluster_itl_stamp_multixact_marker);  return
 *	origin info on hit.
 *
 *	**Caller MUST hold buffer content lock** (L200;  validated by
 *	debug-build LWLockHeldByMe assert at caller site).
 *
 *	Spec-3.6 partial coverage:  origin_node_id derived from cluster_node_id
 *	(ClusterPair writer == reader).
 */
bool
cluster_itl_find_multixact_origin_by_xmax(Page page, MultiXactId multixact_id,
										  uint16 *origin_node_id)
{
	const ClusterItlSlotData *slots;
	uint8 i;

	Assert(page != NULL);
	Assert(origin_node_id != NULL);

	if (!PageHasItl(page))
		return false;
	if (!MultiXactIdIsValid(multixact_id))
		return false;

	slots = ClusterPageGetItlSlots(page);
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		const ClusterItlSlotData *slot = &slots[i];

		if (slot->flags != ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI)
			continue;
		if ((MultiXactId)slot->xid != multixact_id)
			continue;

		/*
		 * Spec-3.6 partial coverage:  on ClusterPair fixture writer ==
		 * reader, so origin is the current node.  Stage 4+ shared-heap
		 * needs marker slot to encode origin via UBA / extra field.
		 */
		*origin_node_id = (uint16)cluster_node_id;
		return true;
	}
	return false;
}

/*
 * cluster_itl_stamp_multixact_marker (spec-3.6 v0.3 D7b NEW)
 *
 *	Stamp the MultiXact xmax marker into a free/reusable ITL slot on
 *	page backing `buf`.  Caller must hold EXCLUSIVE buffer content lock.
 *	Returns slot index on success, CLUSTER_ITL_SLOT_UNALLOCATED if
 *	page ITL is full (caller may skip emit;  V4 wire emit alone covers
 *	cluster propagation — page-side marker is a partial-coverage helper).
 */
uint8
cluster_itl_stamp_multixact_marker(Buffer buf, MultiXactId multixact_id)
{
	Page page;
	ClusterItlSlotData *slots;
	int found_idx = -1;
	int free_idx = -1;
	int reusable_idx = -1;
	uint8 i;

	Assert(BufferIsValid(buf));
	Assert(MultiXactIdIsValid(multixact_id));

	page = BufferGetPage(buf);
	if (!PageHasItl(page))
		return CLUSTER_ITL_SLOT_UNALLOCATED;

	/* spec-5.2 §3.5 D11: a forward ITL marker write requires X. */
	itl_require_block_x_for_write(buf);

	slots = ClusterPageGetItlSlots(page);
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		if (slots[i].flags == ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI
			&& (MultiXactId)slots[i].xid == multixact_id) {
			found_idx = (int)i;
			break;
		}
		if (slots[i].flags == ITL_FLAG_FREE && free_idx < 0)
			free_idx = (int)i;
		else if (cluster_itl_slot_is_completed_reusable(slots[i].flags) && reusable_idx < 0
				 && !cluster_itl_slot_is_protected_foreign(&slots[i]))
			reusable_idx = (int)i; /* spec-4.5a G6: never reuse a pinned foreign slot */
	}

	if (found_idx >= 0)
		return (uint8)found_idx; /* already stamped */

	{
		int idx = (free_idx >= 0) ? free_idx : reusable_idx;
		ClusterItlSlotData *slot;

		if (idx < 0)
			return CLUSTER_ITL_SLOT_UNALLOCATED; /* OVERFLOW */

		slot = &slots[idx];
		if (slot->flags != ITL_FLAG_FREE)
			slot->wrap++;
		slot->xid = (TransactionId)multixact_id; /* repurposed as MultiXactId */
		slot->flags = ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI;
		slot->commit_scn = InvalidScn;
		slot->write_scn = InvalidScn;
		memset(&slot->undo_segment_head, 0, sizeof(slot->undo_segment_head));

		MarkBufferDirty(buf);
		return (uint8)idx;
	}
}

/*
 * cluster_itl_recycle_watermark_contribution -- spec-3.10 §v0.5.
 *
 *	Given the PRE-overwrite state of an ITL slot about to be recycled by a
 *	new data writer (new_xid), return the write_scn that must be folded into
 *	the page's itl_recycle_watermark_scn, or InvalidScn if this is not a
 *	watermark-relevant recycle.
 *
 *	A recycle contributes its old write_scn iff the evicted slot was a
 *	*completed data* slot (not FREE, not lock-only, owned by a different xid)
 *	with a valid write_scn -- i.e. a committed/aborted/needs-cleanout DATA
 *	writer whose undo-chain anchor is about to be overwritten and thereby
 *	dropped from the per-page CR candidate set.  FREE slots, the same xid
 *	reusing its own slot, and lock-only slots (which create no tuple versions
 *	and carry InvalidScn write_scn) contribute nothing (§v0.5 B2 / Q1).
 *
 *	Shared by cluster_itl_stamp_active (primary) and
 *	cluster_itl_redo_apply_block_local_delta (redo) so a primary and a
 *	crash-recovered / standby node can never diverge on which recycles bump
 *	the watermark (refinement of §v0.5 B5: redo recomputes the watermark from
 *	the same inputs the primary used, mirroring the existing pd_block_scn
 *	redo-parity pattern, instead of carrying a byte the 7 emit sites could
 *	forget to set).
 */
SCN
cluster_itl_recycle_watermark_contribution(uint8 old_flags, TransactionId old_xid,
										   SCN old_write_scn, TransactionId new_xid)
{
	/*
	 * §v0.5 (user tightening 2026-06-02):  ONLY a completed DATA slot
	 * (COMMITTED / ABORTED / NEEDS_CLEANOUT) whose undo anchor is about to be
	 * overwritten contributes its write_scn.  FREE, lock-only, the same xid
	 * reusing its own slot, and -- explicitly -- an ACTIVE slot owned by a
	 * DIFFERENT xid contribute nothing.  An ACTIVE-different-xid slot must
	 * NEVER be recycled (cluster_itl_alloc_or_reuse_slot only recycles
	 * completed slots); if one ever reaches a recycle that is an allocator /
	 * state-machine bug and must surface as such (stamp_active Asserts it),
	 * not be silently masked by the watermark.  Exercised directly by
	 * cluster_unit E4-E6 + the ACTIVE-different-xid no-contribution case.
	 */
	switch (old_flags) {
	case ITL_FLAG_COMMITTED:
	case ITL_FLAG_ABORTED:
	case ITL_FLAG_NEEDS_CLEANOUT:
		break; /* completed data slot -- eligible */
	default:
		return InvalidScn; /* FREE / ACTIVE / lock-only-* -- never */
	}
	if (old_xid == new_xid)
		return InvalidScn; /* defensive: a completed slot should not hold new_xid */
	if (!SCN_VALID(old_write_scn))
		return InvalidScn;
	return old_write_scn;
}

/*
 * cluster_itl_block_watermark_advance -- spec-3.10 §v0.5.  Monotonically fold
 *	`contrib` into the page's itl_recycle_watermark_scn (no-op on InvalidScn
 *	or a not-newer value).  Caller holds the buffer content lock (primary: in
 *	the heap op critical section; redo: single-threaded apply).
 */
void
cluster_itl_block_watermark_advance(Page page, SCN contrib)
{
	ClusterItlPageHeader *h;

	if (!SCN_VALID(contrib))
		return;
	h = ClusterPageGetItlHeader(page);
	if (!SCN_VALID(h->itl_recycle_watermark_scn)
		|| scn_time_cmp(contrib, h->itl_recycle_watermark_scn) > 0)
		h->itl_recycle_watermark_scn = contrib;
}

void
cluster_itl_stamp_active(Buffer buf, uint8 slot_idx, TransactionId xid, SCN write_scn,
						 UBA undo_segment_head)
{
	Page page;
	ClusterItlSlotData *slot;
	ClusterXpScope xps;

	/* PGRAC: spec-5.59 D7 profiling (probe is critical-section safe) */
	cluster_xp_begin(&xps, CLXP_LOCAL_UNDO_ITL_WAL);

	Assert(BufferIsValid(buf));
	Assert(slot_idx < CLUSTER_ITL_INITRANS_DEFAULT);

	page = BufferGetPage(buf);
	Assert(PageHasItl(page));

	slot = &ClusterPageGetItlSlots(page)[slot_idx];
	/*
	 * §v0.5 invariant: cluster_itl_alloc_or_reuse_slot only ever hands
	 * stamp_active a FREE slot, this xid's own ACTIVE slot, or a COMPLETED
	 * slot -- never an ACTIVE slot owned by a different xid.  Recycling a
	 * live ACTIVE writer would be an allocator/state-machine bug, not a
	 * watermark case.
	 */
	Assert(!(slot->flags == ITL_FLAG_ACTIVE && slot->xid != xid));
	/*
	 * spec-3.10 §v0.5: fold the evicted (recycled) data slot's write_scn into
	 * the per-page recycle watermark BEFORE overwriting the slot, so own-
	 * instance CR fails closed (53R9F) when a post-snapshot writer's undo
	 * anchor has been dropped from the candidate set.  WAL-protected by the
	 * surrounding heap record; redo parity in
	 * cluster_itl_redo_apply_block_local_delta via the same shared helper.
	 */
	cluster_itl_block_watermark_advance(page, cluster_itl_recycle_watermark_contribution(
												  slot->flags, slot->xid, slot->write_scn, xid));
	if (slot->flags != ITL_FLAG_FREE && !(slot->flags == ITL_FLAG_ACTIVE && slot->xid == xid))
		slot->wrap++;
	slot->xid = xid;
	slot->flags = ITL_FLAG_ACTIVE;
	slot->commit_scn = InvalidScn;
	slot->write_scn = write_scn;
	/* spec-3.4b D5: real UBA from xact-local binding (F11). */
	slot->undo_segment_head = undo_segment_head;

	/*
	 * spec-3.9 D5 foundation: stamp the page-level block SCN watermark so the
	 * own-instance CR 3-tier page gate (cluster_cr_satisfies_mvcc tier 1) can
	 * cheaply skip blocks with no post-snapshot change.  write_scn comes from
	 * cluster_scn_advance() and is monotonically newer than any prior stamp on
	 * this page, so a plain assignment keeps pd_block_scn = "last modified at".
	 * Already inside the heap op's critical section + MarkBufferDirty below;
	 * the page change is WAL-protected by the surrounding heap record.  Redo
	 * parity is provided by cluster_itl_redo_apply_block_local_delta (spec-3.9
	 * Hardening L213), which re-applies this watermark from the ITL delta on
	 * crash recovery / standby replay; FPI redo restores it verbatim.
	 */
	if (SCN_VALID(write_scn))
		((PageHeader)page)->pd_block_scn = write_scn;
	/*
	 * spec-3.4c A2 / L194:  first_change_lsn is intentionally NOT populated.
	 * The intended value is the LSN of the WAL record about to be emitted
	 * for this stamp, but stamp_active runs in the critical section BEFORE
	 * XLogInsert returns recptr, and re-stamping the slot after PageSetLSN
	 * would require a second WAL emit (chicken-and-egg).  spec-3.4c MVP
	 * keeps first_change_lsn = InvalidXLogRecPtr (spec-1.5 placeholder)
	 * and defers true populate to a future spec when generic_xlog API
	 * extends to provide inserted-LSN feedback.
	 */

	MarkBufferDirty(buf);
	cluster_xp_end(&xps); /* PGRAC: spec-5.59 D7 profiling */
}

void
cluster_itl_stamp_committed(Buffer buf, uint8 slot_idx, SCN commit_scn)
{
	Page page;
	ClusterItlSlotData *slot;

	Assert(BufferIsValid(buf));
	Assert(slot_idx < CLUSTER_ITL_INITRANS_DEFAULT);
	Assert(SCN_VALID(commit_scn)); /* L181 — COMMITTED must carry valid SCN */

	page = BufferGetPage(buf);
	Assert(PageHasItl(page));

	slot = &ClusterPageGetItlSlots(page)[slot_idx];
	Assert(slot->flags == ITL_FLAG_ACTIVE);

	slot->flags = ITL_FLAG_COMMITTED;
	slot->commit_scn = commit_scn;

	MarkBufferDirty(buf);
}

void
cluster_itl_stamp_aborted(Buffer buf, uint8 slot_idx)
{
	Page page;
	ClusterItlSlotData *slot;

	Assert(BufferIsValid(buf));
	Assert(slot_idx < CLUSTER_ITL_INITRANS_DEFAULT);

	page = BufferGetPage(buf);
	Assert(PageHasItl(page));

	slot = &ClusterPageGetItlSlots(page)[slot_idx];
	Assert(slot->flags == ITL_FLAG_ACTIVE);

	slot->flags = ITL_FLAG_ABORTED;
	slot->commit_scn = InvalidScn;

	MarkBufferDirty(buf);
}

void
cluster_itl_check_subxact_or_error(void)
{
	/*
	 * Legacy spec-3.4a fence.  spec-3.5 adds SUBTRANS-aware ITL touch
	 * range ownership, so this helper must not reject savepoints anymore.
	 * Kept as an ABI-compatible no-op for older callsites/tests.
	 */
}


/*
 * cluster_itl_redo_apply_block_local_delta
 *
 *	spec-3.4b D6 / F9: replay a block-local ITL delta array onto `page`,
 *	dispatching by format_version (v1 24B legacy, v2 40B with UBA).
 *	See header for the full contract.
 */
Size
cluster_itl_redo_apply_block_local_delta(Page page, HeapTupleHeader htup,
										 const char *itl_block_start)
{
	xl_heap_itl_delta_block hdr;
	Size delta_size = 0; /* init silences cppcheck legacyUninitvar (elog PANIC no-return) */
	Size consumed;
	uint16 i;

	if (page == NULL || itl_block_start == NULL)
		elog(PANIC, "spec-3.4b D6: cluster_itl_redo_apply_block_local_delta got NULL arg");

	memcpy(&hdr, itl_block_start, offsetof(xl_heap_itl_delta_block, deltas));

	if (hdr.format_version == CLUSTER_ITL_DELTA_FORMAT_V1)
		delta_size = sizeof(xl_heap_itl_delta);
	else if (hdr.format_version == CLUSTER_ITL_DELTA_FORMAT_V2)
		delta_size = sizeof(xl_heap_itl_delta_v2);
	else if (hdr.format_version == CLUSTER_ITL_DELTA_FORMAT_V3)
		delta_size = sizeof(xl_heap_itl_delta_v3);
	else
		elog(PANIC, "spec-3.4b D6: unknown xl_heap_itl_delta_block.format_version %u",
			 (unsigned)hdr.format_version);

	for (i = 0; i < hdr.ndeltas; i++) {
		const char *p
			= itl_block_start + offsetof(xl_heap_itl_delta_block, deltas) + (Size)i * delta_size;
		ClusterItlSlotData *slot;
		uint16 slot_idx;
		uint16 flags_after;
		TransactionId d_xid;
		SCN d_write_scn;
		SCN d_commit_scn;
		UBA d_uba = InvalidUba_init;

		if (hdr.format_version == CLUSTER_ITL_DELTA_FORMAT_V1) {
			xl_heap_itl_delta d;

			memcpy(&d, p, sizeof(d));
			slot_idx = d.slot_idx;
			flags_after = d.flags_after;
			d_xid = d.xid;
			d_write_scn = d.write_scn;
			d_commit_scn = d.commit_scn;
			/* v1 carries no UBA -- leave d_uba = InvalidUba so the
			 * slot's existing UBA on page is preserved.  Legacy ACTIVE
			 * stamps wrote InvalidUba to the page anyway, so reader
			 * 3-branch (D7) will fall back to zero triple. */
		} else if (hdr.format_version == CLUSTER_ITL_DELTA_FORMAT_V2) {
			xl_heap_itl_delta_v2 d;

			memcpy(&d, p, sizeof(d));
			slot_idx = d.slot_idx;
			flags_after = d.flags_after;
			d_xid = d.xid;
			d_write_scn = d.write_scn;
			d_commit_scn = d.commit_scn;
			d_uba = d.undo_segment_head;
		} else {
			/* CLUSTER_ITL_DELTA_FORMAT_V3 (delta_size dispatch above already
			 * PANICked on any other value). */
			xl_heap_itl_delta_v3 d;

			memcpy(&d, p, sizeof(d));
			slot_idx = d.slot_idx;
			flags_after = d.flags_after;
			d_xid = d.xid;
			d_write_scn = d.write_scn;
			/* spec-5.19 MG-D: v3 elides the write-time commit_scn (it is
			 * always InvalidScn for the ACTIVE / LOCK_ONLY_ACTIVE transitions
			 * the write path emits).  Reconstruct it as InvalidScn.  If a v3
			 * delta ever carries ITL_FLAG_COMMITTED, the COMMITTED-requires-
			 * valid-SCN guard below fails closed (PANIC) -- v3 must never be
			 * used for a COMMITTED transition. */
			d_commit_scn = InvalidScn;
			d_uba = d.undo_segment_head;
		}

		if (flags_after == ITL_FLAG_COMMITTED && !SCN_VALID(d_commit_scn))
			elog(PANIC, "spec-3.4a D9: ITL COMMITTED delta with InvalidScn at heap redo");

		slot = &ClusterPageGetItlSlots(page)[slot_idx];
		/*
		 * spec-3.10 §v0.5: redo parity for itl_recycle_watermark_scn.
		 * Recompute the recycle contribution from the PRE-overwrite slot
		 * state via the SAME shared helper the primary used, before
		 * overwriting the slot below.  Only ACTIVE stamps recycle a prior
		 * occupant; commit/abort/lock deltas carry the same xid (or a
		 * lock-only old flag) so the helper returns InvalidScn.  FPI redo
		 * restores the watermark verbatim (it is a page field); this covers
		 * the incremental-delta path -- NOT FPI-dependent (§v0.5 B5).
		 */
		if (flags_after == ITL_FLAG_ACTIVE)
			cluster_itl_block_watermark_advance(
				page, cluster_itl_recycle_watermark_contribution(slot->flags, slot->xid,
																 slot->write_scn, d_xid));
		slot->xid = d_xid;
		slot->flags = (ClusterItlFlags)flags_after;
		slot->write_scn = d_write_scn;
		slot->commit_scn = d_commit_scn;
		/* spec-3.4b D6: preserve existing UBA when delta carries InvalidUba
		 * (e.g., legacy v1 record OR v2 finish delta that did not re-bind). */
		if (!UBA_is_invalid(d_uba))
			slot->undo_segment_head = d_uba;

		/*
		 * spec-3.9 Hardening (L213): redo parity for pd_block_scn -- the
		 * own-instance CR page-gate watermark (cluster_cr_satisfies_mvcc
		 * tier 1).  cluster_itl_stamp_active sets pd_block_scn = write_scn
		 * on the primary; the matching delta replay MUST reproduce it or a
		 * crash-recovered / standby page keeps pd_block_scn = InvalidScn,
		 * the CR page gate never fires there, and historical visibility on
		 * the recovered node silently falls back to the PG-native body
		 * (wrong for genuine cross-snapshot CR cases).  A full-page-image
		 * redo already restores pd_block_scn verbatim; this covers the
		 * incremental delta path only.  Monotonic max keeps the "last
		 * modified at" semantic regardless of delta replay order, and the
		 * SCN_VALID guard makes lock-only / commit / abort deltas (which
		 * carry InvalidScn or an unchanged write_scn) a no-op -- exactly
		 * mirroring the stamp_active side, which only writes pd_block_scn
		 * on a valid write_scn. */
		if (SCN_VALID(d_write_scn)
			&& (!SCN_VALID(((PageHeader)page)->pd_block_scn)
				|| scn_time_cmp(d_write_scn, ((PageHeader)page)->pd_block_scn) > 0))
			((PageHeader)page)->pd_block_scn = d_write_scn;

		/* L187 patch tuple pointer to the last applied slot_idx.  Same
		 * semantic as spec-3.4a A9. */
		if (htup != NULL)
			htup->t_itl_slot_idx = slot_idx;
	}

	consumed = offsetof(xl_heap_itl_delta_block, deltas) + (Size)hdr.ndeltas * delta_size;
	return consumed;
}


/*
 * cluster_itl_wal_block_consumed_bytes -- compute WAL footprint of one
 * block-local ITL delta array, dispatching by format_version.
 */
Size
cluster_itl_wal_block_consumed_bytes(const char *itl_block_start)
{
	xl_heap_itl_delta_block hdr;
	Size delta_size = 0; /* init silences cppcheck legacyUninitvar */

	if (itl_block_start == NULL)
		elog(PANIC, "spec-3.4b D6: cluster_itl_wal_block_consumed_bytes got NULL arg");
	memcpy(&hdr, itl_block_start, offsetof(xl_heap_itl_delta_block, deltas));

	if (hdr.format_version == CLUSTER_ITL_DELTA_FORMAT_V1)
		delta_size = sizeof(xl_heap_itl_delta);
	else if (hdr.format_version == CLUSTER_ITL_DELTA_FORMAT_V2)
		delta_size = sizeof(xl_heap_itl_delta_v2);
	else if (hdr.format_version == CLUSTER_ITL_DELTA_FORMAT_V3)
		delta_size = sizeof(xl_heap_itl_delta_v3);
	else
		elog(PANIC, "spec-3.4b D6: unknown xl_heap_itl_delta_block.format_version %u",
			 (unsigned)hdr.format_version);

	return offsetof(xl_heap_itl_delta_block, deltas) + (Size)hdr.ndeltas * delta_size;
}


/*
 * cluster_itl_wal_block_first_slot_idx -- read the first delta's
 * slot_idx (offset 0 of both v1 and v2 delta entries) without parsing
 * the full delta.  Caller should know ndeltas >= 1.
 */
uint16
cluster_itl_wal_block_first_slot_idx(const char *itl_block_start)
{
	uint16 slot_idx;

	Assert(itl_block_start != NULL);
	memcpy(&slot_idx, itl_block_start + offsetof(xl_heap_itl_delta_block, deltas), sizeof(uint16));
	return slot_idx;
}


/*
 * cluster_itl_page_has_active_slot (spec-5.2 §3.5 D11 — writer-transfer-revoke
 *	"active-ITL hard boundary").  See cluster_itl.h for the full contract.
 *
 *	Scans the page's 8 ITL slots for any in a non-terminal active state:
 *	ITL_FLAG_ACTIVE (data writer) or ITL_FLAG_LOCK_ONLY_ACTIVE (row-lock
 *	holder).  Conservative by design: ANY active slot means the local X
 *	holder has an uncommitted transaction whose commit still needs this
 *	page's slot, so the block must not be destructively transferred away.
 */
bool
cluster_itl_page_has_active_slot(Page page)
{
	const ClusterItlSlotData *slots;
	uint8 i;

	Assert(page != NULL);

	if (!PageHasItl(page))
		return false;
	/* Defensive: a malformed special area cannot carry a valid slot array. */
	if (PageGetSpecialSize(page) < CLUSTER_ITL_ARRAY_SIZE)
		return false;

	slots = ClusterPageGetItlSlots(page);
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		if (slots[i].flags == ITL_FLAG_ACTIVE || slots[i].flags == ITL_FLAG_LOCK_ONLY_ACTIVE)
			return true;
	}
	return false;
}


#else /* !USE_PGRAC_CLUSTER */

bool
cluster_itl_get_tt_ref(Page page, uint8 itl_slot_idx, ClusterUndoTTSlotRef *ref)
{
	(void)page;
	(void)itl_slot_idx;
	(void)ref;
	return false;
}

bool
cluster_itl_alloc_or_reuse_slot(Buffer buf pg_attribute_unused(),
								TransactionId top_xid pg_attribute_unused(),
								uint8 *out_slot_idx pg_attribute_unused())
{
	return false;
}

bool
cluster_itl_alloc_or_reuse_lock_slot(Buffer buf pg_attribute_unused(),
									 TransactionId top_xid pg_attribute_unused(),
									 uint8 *out_slot_idx pg_attribute_unused())
{
	return false;
}

bool
cluster_itl_find_multixact_origin_by_xmax(Page page pg_attribute_unused(),
										  MultiXactId multixact_id pg_attribute_unused(),
										  uint16 *origin_node_id pg_attribute_unused())
{
	return false;
}

bool
cluster_itl_page_has_active_slot(Page page pg_attribute_unused())
{
	return false;
}

uint8
cluster_itl_stamp_multixact_marker(Buffer buf pg_attribute_unused(),
								   MultiXactId multixact_id pg_attribute_unused())
{
	return 0xff; /* CLUSTER_ITL_SLOT_UNALLOCATED */
}

void
cluster_itl_stamp_active(Buffer buf pg_attribute_unused(), uint8 slot_idx pg_attribute_unused(),
						 TransactionId xid pg_attribute_unused(),
						 SCN write_scn pg_attribute_unused(),
						 UBA undo_segment_head pg_attribute_unused())
{}

void
cluster_itl_stamp_committed(Buffer buf pg_attribute_unused(), uint8 slot_idx pg_attribute_unused(),
							SCN commit_scn pg_attribute_unused())
{}

void
cluster_itl_stamp_aborted(Buffer buf pg_attribute_unused(), uint8 slot_idx pg_attribute_unused())
{}

void
cluster_itl_check_subxact_or_error(void)
{}

Size
cluster_itl_redo_apply_block_local_delta(Page page pg_attribute_unused(),
										 HeapTupleHeader htup pg_attribute_unused(),
										 const char *itl_block_start pg_attribute_unused())
{
	return 0;
}

Size
cluster_itl_wal_block_consumed_bytes(const char *itl_block_start pg_attribute_unused())
{
	return 0;
}

uint16
cluster_itl_wal_block_first_slot_idx(const char *itl_block_start pg_attribute_unused())
{
	return 0;
}

#endif /* USE_PGRAC_CLUSTER */
