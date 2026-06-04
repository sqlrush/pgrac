/*-------------------------------------------------------------------------
 *
 * cluster_tt_slot.h
 *	  pgrac Transaction Table slot type definition (placeholder).
 *
 *	  Stage 1 ships ONLY the 32-byte on-disk struct, the status enum,
 *	  five sentinel constants, four StaticAssertDecl invariants, and
 *	  two inline helpers.  No allocation / commit / reuse / recovery
 *	  logic in Stage 1 — that lands in subsequent specs (segment header
 *	  layout, dedicated undo tablespace, then real activation in
 *	  Stage 3 visibility).
 *
 *	  Status field semantics (offset 6, 1 byte):
 *	    0  = TT_SLOT_UNUSED        permanent placeholder; NOT "active"
 *	    1  = TT_SLOT_ACTIVE        tx in progress (Stage 3+)
 *	    2  = TT_SLOT_COMMITTED     tx committed; commit_scn populated
 *	    3  = TT_SLOT_ABORTED       tx aborted; awaits recyclable
 *	    4  = TT_SLOT_RECYCLABLE    slot may be reused on next allocate
 *	    0xFF = TT_SLOT_INVALID     corruption sentinel
 *
 *	  IMPORTANT: status value 0 is permanently "no tx occupies this
 *	  slot", NOT "tx running".  zero-init undo segment headers (memset)
 *	  and freshly allocated TTSlot[] arrays land at status==0; readers
 *	  that confuse this would generate phantom-tx visibility bugs.
 *
 *	  WRAP field (offset 4, 2 bytes): TT slot reuse counter, defends
 *	  against ABA when the same xid lands in the same slot index.
 *	  16-bit wrap is a fail-fast guard, not a production-lifetime
 *	  design.  Real production reuse requires extending this with an
 *	  epoch / wider wrap / segment expansion strategy in a subsequent
 *	  spec.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_tt_slot.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-1.20-tt-slot-data-structure.md
 *
 *	  Forward-link (spec-1.21): UndoSegmentHeaderData
 *	  (cluster_undo_segment.h) embeds an array of TT_SLOTS_PER_SEGMENT
 *	  TTSlot entries at byte offset 112 (C natural alignment shifts the
 *	  v0.2 body table value 108 by +4).  spec-1.22 will populate TTSlot
 *	  fields when binding transactions to segments.
 *
 *	  Frontend-safe: this header has no backend-only includes.  Future
 *	  pg_waldump / pg_resetwal extensions can read TTSlot bytes without
 *	  pulling in cluster runtime headers.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_TT_SLOT_H
#define CLUSTER_TT_SLOT_H

#include "c.h"						  /* uint8/16/32, bool */
#include "access/transam.h"			  /* TransactionId */
#include "cluster/cluster_itl_slot.h" /* UBA typedef + InvalidUba */
#include "cluster/cluster_scn.h"	  /* SCN typedef + InvalidScn */


/*
 * TTSlotStatus -- per-slot state.
 *
 *   Value 0 (TT_SLOT_UNUSED) is permanently the "no tx in this slot"
 *   sentinel and MUST NOT be reassigned.  See file header for the
 *   "0 is NOT active" note.
 */
typedef enum TTSlotStatus {
	TT_SLOT_UNUSED = 0,		/* permanent: no tx occupies this slot */
	TT_SLOT_ACTIVE = 1,		/* tx in progress (Stage 3+) */
	TT_SLOT_COMMITTED = 2,	/* tx committed; commit_scn populated */
	TT_SLOT_ABORTED = 3,	/* tx aborted; awaits recyclable */
	TT_SLOT_RECYCLABLE = 4, /* slot may be reused on next allocate */
	TT_SLOT_INVALID = 0xFF	/* corruption sentinel */
} TTSlotStatus;


/*
 * TTSlot -- 32-byte on-disk Transaction Table entry.
 *
 *   Field order is the on-disk ABI; offsets are locked at compile time
 *   by the StaticAssertDecl block below.  Future struct edits MUST
 *   keep the four anchors (sizeof, xid offset, commit_scn offset,
 *   first_undo_block offset) byte-identical or bump catversion.
 */
typedef struct TTSlot {
	/* 8 bytes: transaction identification */
	TransactionId xid; /* offset 0;  4 B; PG 32-bit xid */
	uint16 wrap;	   /* offset 4;  2 B; reuse counter (defends ABA) */
	uint8 status;	   /* offset 6;  1 B; TTSlotStatus enum value */
	uint8 flags;	   /* offset 7;  1 B; reserved + cleanout flags */

	/* 8 bytes: commit information */
	SCN commit_scn; /* offset 8;  8 B; populated on commit */

	/* 16 bytes: undo chain anchor */
	UBA first_undo_block; /* offset 16; 16 B; first undo record */
						  /* total: 32 bytes */
} TTSlot;


/*
 * On-disk ABI invariants -- enforced at compile time.
 *
 *   Spec: spec-1.20-tt-slot-data-structure.md §2.1.
 */
StaticAssertDecl(sizeof(TTSlot) == 32,
				 "spec-1.20 invariant: TTSlot on-disk size MUST stay 32 bytes; "
				 "growth requires catversion bump in a separate spec");
StaticAssertDecl(offsetof(TTSlot, xid) == 0, "spec-1.20 invariant: xid at byte 0");
StaticAssertDecl(offsetof(TTSlot, wrap) == 4, "spec-1.20 invariant: wrap at byte 4");
StaticAssertDecl(offsetof(TTSlot, status) == 6, "spec-1.20 invariant: status at byte 6");
StaticAssertDecl(offsetof(TTSlot, flags) == 7, "spec-1.20 invariant: flags at byte 7");
StaticAssertDecl(offsetof(TTSlot, commit_scn) == 8, "spec-1.20 invariant: commit_scn at byte 8");
StaticAssertDecl(offsetof(TTSlot, first_undo_block) == 16,
				 "spec-1.20 invariant: first_undo_block at byte 16");


/*
 * Sentinel constants.  Stage 2+ extensions MUST NOT reassign value 0
 * for any of these to anything other than its placeholder meaning.
 */
#define TT_WRAP_INITIAL ((uint16)0)		 /* fresh slot pre-reuse */
#define TT_WRAP_MAX ((uint16)0xFFFE)	 /* last permitted reuse */
#define TT_WRAP_INVALID ((uint16)0xFFFF) /* corruption sentinel */

#define TT_FLAGS_RESERVED ((uint8)0) /* Stage 1 placeholder */

#define TT_SLOTS_PER_SEGMENT 48 /* Spec: spec-1.20-tt-slot-data-structure.md */


/*
 * Inline helpers -- frontend-safe, no runtime state needed.
 *
 *   TTSlot_is_unused: returns true iff the slot is in a state that
 *   means no tx is currently using it.  Stage 1 always returns true
 *   for status==UNUSED; Stage 3+ also returns true for RECYCLABLE so
 *   callers don't break across stages.
 *
 *   TTSlot_is_committed: returns true iff the slot's tx committed AND
 *   commit_scn is populated (a real SCN, not InvalidScn).  Both
 *   conditions matter for visibility paths -- status alone could be
 *   COMMITTED with commit_scn==InvalidScn after a crash mid-write
 *   (Stage 3+ failure mode).  Conservative: treat such inconsistent
 *   slots as not-yet-committed so visibility code falls back to undo
 *   chain inspection.
 */
static inline bool
TTSlot_is_unused(const TTSlot *slot)
{
	return slot->status == TT_SLOT_UNUSED || slot->status == TT_SLOT_RECYCLABLE;
}

static inline bool
TTSlot_is_committed(const TTSlot *slot)
{
	return slot->status == TT_SLOT_COMMITTED && SCN_VALID(slot->commit_scn);
}


/*
 * ============================================================
 * PGRAC MODIFICATIONS by SqlRush <sqlrush@gmail.com>
 *
 * spec-3.1 D3 — extend with `ClusterUndoTTSlotRef` minimal read-only
 * descriptor that links an ITL slot to its owning TT slot status
 * lookup key (see cluster_tt_status.h ClusterTTStatusKey).
 *
 * What changed:
 *   - Added `ClusterUndoTTSlotRef` 32-byte struct + StaticAssertDecl
 *     enforcing explicit padding (v0.4 M4 — no implicit padding).
 *   - The reader function `cluster_itl_get_tt_ref` extern lives in
 *     cluster_itl.h (backend-only; needs `Page` type from
 *     storage/bufpage.h, kept out of this frontend-safe header).
 *
 * Why:
 *   spec-3.1 v1.0 FROZEN ITL/TT exact-key foundation;tuple xmin/xmax
 *   raw xid has no origin so visibility must reach the TT status via
 *   ITL slot → UBA → TT slot ref (HC180;L176;spec-3.1 Q3 ★ A).
 *
 * Frontend-safety preserved:  ClusterUndoTTSlotRef contains only
 *   uint16/uint32/TransactionId/SCN/bool/uint8[] — all already in
 *   c.h / transam.h / cluster_scn.h.
 * ============================================================
 */

/*
 * ClusterUndoTTSlotRef -- read-only descriptor of one ITL slot's TT
 * binding.  Filled by cluster_itl_get_tt_ref (cluster_itl.h, D4) and
 * consumed by visibility-side code (spec-3.2+).
 *
 * 32 bytes wire-stable (spec-3.1 v0.4 M4).  Field order is locked.
 */
typedef struct ClusterUndoTTSlotRef {
	uint16 origin_node_id;	 /* offset  0, 2B */
	uint16 undo_segment_id;	 /* offset  2, 2B */
	uint32 tt_slot_id;		 /* offset  4, 4B */
	uint32 cluster_epoch;	 /* offset  8, 4B */
	TransactionId local_xid; /* offset 12, 4B */
	SCN cached_commit_scn;	 /* offset 16, 8B; InvalidScn if no cleanout */
	bool has_cached_status;	 /* offset 24, 1B */
	uint8 _padding[7];		 /* offset 25, 7B explicit padding to 32B */
} ClusterUndoTTSlotRef;

StaticAssertDecl(sizeof(ClusterUndoTTSlotRef) == 32,
				 "spec-3.1 v0.4 M4: ClusterUndoTTSlotRef must be 32 bytes;"
				 " explicit _padding[7] required, no implicit padding");


/*
 * ============================================================
 * PGRAC MODIFICATIONS by SqlRush <sqlrush@gmail.com>
 *
 * spec-3.4b D3 — NEW per-segment TT slot allocator API + offset/id
 * separation helpers.
 *
 * What changed:
 *   - Added INVALID_TT_SLOT_OFFSET sentinel (0xFFFF; out of valid range
 *     [0, 47]) for cluster_tt_slot_alloc OVERFLOW signaling.
 *   - Added inline cluster_tt_slot_offset_to_id / _id_to_offset helpers
 *     (F1 — exact-key `tt_slot_id` 1..48 with 0 permanently invalid;
 *     on-disk offset 0..47 used by allocator).
 *   - Declared cluster_tt_slot_alloc / _free / _get_wrap as extern;
 *     implementation lives in cluster_tt_slot.c (backend-only; pulls
 *     LWLock state).  Shmem registration (request_shmem + init_shmem)
 *     also lives in cluster_tt_slot.c.
 *
 * Why:
 *   spec-3.4b Q3 ★ — production cluster path needs real TT slot
 *   allocations (not provisional ids); allocator gives every active
 *   xact a unique (segment_id, slot_offset) pair to write into
 *   ItlSlot.undo_segment_head and to key the commit-time TT status
 *   install (F11 xact-local binding).
 *
 * Frontend-safety preserved:  this header still has no backend-only
 *   includes; allocator extern decls take only plain integer types,
 *   so frontend tooling that reads slot bytes continues to compile.
 * ============================================================
 */

/*
 * INVALID_TT_SLOT_OFFSET — sentinel returned by cluster_tt_slot_alloc when
 * the per-segment 48-slot table is fully occupied by ACTIVE entries.
 * Value 0xFFFF is unambiguous: valid offsets are [0, TT_SLOTS_PER_SEGMENT - 1].
 */
#define INVALID_TT_SLOT_OFFSET ((uint16)0xFFFF)


/*
 * ClusterTTSlotAllocStatus -- per-slot allocator state (shmem-only; distinct
 * from the on-disk TTSlotStatus ABI above).  spec-3.4b tracked only what the
 * allocator needs to pick / recycle slots; spec-3.12 D2 promotes this enum to
 * the header so the pure retention predicate cluster_tt_slot_recyclable()
 * (cluster_undo_retention.c) and its cluster_unit tests can name the values.
 *
 *   Values are wire-stable with cluster_tt_slot_test_force_status (2 =
 *   COMMITTED, 3 = ABORTED) and MUST NOT be reassigned.
 */
typedef enum ClusterTTSlotAllocStatus {
	CTS_FREE = 0,	   /* available for allocation */
	CTS_ACTIVE = 1,	   /* owned by an in-flight xact */
	CTS_COMMITTED = 2, /* committed; recyclable once commit_scn is older than horizon */
	CTS_ABORTED = 3	   /* aborted; immediately recyclable (spec-3.12 C7) */
} ClusterTTSlotAllocStatus;


/*
 * cluster_tt_slot_offset_to_id / _id_to_offset
 *
 *	spec-3.4b F1: on-disk slot offset and exact-key tt_slot_id are
 *	intentionally separated.
 *
 *	    offset  ∈ [0, TT_SLOTS_PER_SEGMENT)  — allocator-internal index
 *	    id      ∈ [1, TT_SLOTS_PER_SEGMENT]  — exact-key (0 = invalid)
 *
 *	The invalid sentinel `tt_slot_id == 0` is consumed by spec-3.2 D5
 *	to decide whether to enter the cluster visibility path; an allocator
 *	that returned `tt_slot_id == 0` for a legitimate first-allocation
 *	would route real cluster work to the PG-native fallback path.
 */
static inline uint32
cluster_tt_slot_offset_to_id(uint16 slot_offset)
{
	Assert(slot_offset < TT_SLOTS_PER_SEGMENT);
	return ((uint32)slot_offset) + 1;
}

static inline uint16
cluster_tt_slot_id_to_offset(uint32 tt_slot_id)
{
	Assert(tt_slot_id >= 1 && tt_slot_id <= TT_SLOTS_PER_SEGMENT);
	return (uint16)(tt_slot_id - 1);
}


/*
 * Allocator API (impl in cluster_tt_slot.c).
 *
 *	cluster_tt_slot_alloc:
 *	  Allocate-or-reuse a slot on `segment_id` for `top_xid`.  Three-tier
 *	  fallback (L189 recycle policy):
 *	    1) reuse a slot already owned by `top_xid` (idempotent)
 *	    2) take any FREE slot
 *	    3) recycle a COMMITTED / ABORTED slot, wrap++
 *	  Returns offset in [0, TT_SLOTS_PER_SEGMENT) on success, or
 *	  INVALID_TT_SLOT_OFFSET when all slots are ACTIVE.  Caller MUST be
 *	  outside critical section (function takes LWLock).
 *
 *	cluster_tt_slot_free:
 *	  Mark `slot_offset` on `segment_id` as FREE.  Called from
 *	  cluster_tt_local end-of-xact path (commit + abort both free in
 *	  spec-3.4b MVP; spec-3.4c delayed cleanout may keep COMMITTED
 *	  status alive longer).
 *
 *	cluster_tt_slot_get_wrap:
 *	  Read the wrap counter of `slot_offset` on `segment_id`.  Used by
 *	  reader paths that want to detect slot reuse since a UBA was
 *	  encoded.
 */
extern uint16 cluster_tt_slot_alloc(uint32 segment_id, TransactionId top_xid);

/*
 * spec-3.12 D2b — alloc variant that reports WHY it failed.  On
 * INVALID_TT_SLOT_OFFSET, *out_retained_pressure (when non-NULL) is true iff
 * the segment is blocked by retained COMMITTED slots (not all-ACTIVE), so the
 * caller may roll over to a new active segment instead of erroring.  The 2-arg
 * cluster_tt_slot_alloc is a wrapper that passes NULL.
 */
extern uint16 cluster_tt_slot_alloc_ext(uint32 segment_id, TransactionId top_xid,
										bool *out_retained_pressure);
extern void cluster_tt_slot_free(uint32 segment_id, uint16 slot_offset);
extern uint16 cluster_tt_slot_get_wrap(uint32 segment_id, uint16 slot_offset);

/*
 * spec-3.12 D2 — end-of-xact allocator state transitions.
 *
 *	cluster_tt_slot_mark_committed:
 *	  ACTIVE -> COMMITTED, retaining owner xid + wrap + commit_scn so the
 *	  durable segment-header TT slot is NOT overwritten by a later writer
 *	  while a reader's read_scn still needs the pre-image.  The slot becomes
 *	  recyclable only once commit_scn is older than the retention horizon (gate in
 *	  cluster_tt_slot_alloc; predicate cluster_tt_slot_recyclable).  Replaces
 *	  the spec-3.4b commit-time cluster_tt_slot_free().
 *
 *	cluster_tt_slot_mark_aborted:
 *	  ACTIVE -> ABORTED.  Aborted versions are invisible to every read_scn so
 *	  the slot is immediately recyclable (spec-3.12 C7); commit_scn is cleared.
 *
 *	Both are idempotent / defensive: a slot not currently ACTIVE-owned by
 *	`xid` is left unchanged (guards against double end-of-xact callbacks).
 */
extern void cluster_tt_slot_mark_committed(uint32 segment_id, uint16 slot_offset, TransactionId xid,
										   SCN commit_scn);
extern void cluster_tt_slot_mark_aborted(uint32 segment_id, uint16 slot_offset, TransactionId xid);

/*
 * spec-3.12 D2b — retention-pressure segment rollover support.
 *
 *	cluster_tt_slot_current_segment:
 *	  segment_id the node's allocator is bound to (0 if never bound).  The
 *	  binding path keeps allocating here after a rollover.
 *
 *	cluster_tt_slot_rollover:
 *	  Rebind the node's allocator to a fresh segment + reset its 48 slots.
 *	  Caller MUST hold the undo lifecycle_lock (serializes rollovers, C17).
 *	  out_old_had_active (spec-3.12 D3; nullable) reports whether the old
 *	  segment still had an in-flight ACTIVE slot, so the caller can decide
 *	  the SEGMENT_ACTIVE -> SEGMENT_COMMITTED transition.
 */
extern uint32 cluster_tt_slot_current_segment(int node_id);
extern void cluster_tt_slot_rollover(int node_id, uint32 new_segment_id, bool *out_old_had_active);


/*
 * Shmem lifecycle (impl in cluster_tt_slot.c).
 */
extern Size cluster_tt_slot_shmem_size(void);
extern void cluster_tt_slot_shmem_init(void);
extern void cluster_tt_slot_shmem_register(void);


/*
 * spec-3.12 D5 retention observability accessors.  retention_horizon_scn is a
 * gauge (last sampled horizon); the others are monotonic event counters.
 */
extern uint64 cluster_tt_slot_retention_horizon_scn(void);
extern uint64 cluster_tt_slot_retain_skip_count(void);
extern uint64 cluster_tt_slot_retention_recycle_count(void);


/*
 * Test-only: wipe all per-node allocator state back to zeroes.  Used by
 * cluster_unit harness to set a clean baseline between cases.  Production
 * code MUST NOT call this.
 */
extern void cluster_tt_slot_reset_all(void);


/*
 * Test-only: forcibly transition a slot's status to COMMITTED or ABORTED.
 *
 *	spec-3.4b allocator does not yet expose a public mark_committed /
 *	mark_aborted API — those transitions happen via the spec-3.4c eager
 *	cleanout path which is not yet wired.  cluster_unit harness uses this
 *	helper to drive the L189 recycle policy (alloc returns a recycled
 *	COMMITTED/ABORTED slot with wrap++).
 *
 *	Production code MUST NOT call this.
 *
 *	new_status: 2 = COMMITTED, 3 = ABORTED (matches CTS_COMMITTED /
 *	CTS_ABORTED enum values inside cluster_tt_slot.c).
 */
extern void cluster_tt_slot_test_force_status(uint32 segment_id, uint16 slot_offset,
											  uint8 new_status);


#endif /* CLUSTER_TT_SLOT_H */
