/*-------------------------------------------------------------------------
 *
 * cluster_undo_segment.h
 *	  pgrac Undo Segment header type definition (placeholder).
 *
 *	  Stage 1.21 shipped ONLY the on-disk struct (8192-byte block),
 *	  the segment_state enum, seven sentinel constants, six
 *	  StaticAssertDecl invariants, and three inline helpers.
 *
 *	  Stage 1.22 (spec-1.22 ATOMIC BATCH) activated:
 *	    - PageInitUndoSegmentHeader caller in cluster_undo_alloc.c
 *	    - PD_UNDO_SEG_HEADER pd_flags bit 0x0010 in storage/bufpage.h
 *	    - dedicated undo tablespace pg_undo (UNDOTABLESPACE_OID = 9100)
 *	    - per-instance subdir layout $PGDATA/pg_undo/instance_<N>/
 *	    - initdb seed segment writer (frontend-safe via D14c helper)
 *	    - RM_CLUSTER_UNDO WAL resource manager + XLOG_UNDO_SEGMENT_INIT
 *	      record (D14a B-lite; XLOG_FPI rejected per v0.2 P1-A)
 *	    - catversion bump 202605181 -> 202605190
 *
 *	  Still deferred to feature-117 / feature-120: TT slot real
 *	  allocation + commit_scn write + segment recycling / retention.
 *
 *	  Byte layout (8192 bytes total; matches PG block size):
 *
 *	    offset  size  field group
 *	    ------  ----  ----------------------------------------
 *	    0       32    PG standard PageHeader prefix (mirrors
 *	                  PageHeaderData field layout; SizeOfPageHeaderData
 *	                  is 32 under USE_PGRAC_CLUSTER per spec-1.4).
 *	                  Reused for buffer manager LSN / checksum /
 *	                  pd_pagesize_version sanity checks; pd_prune_xid
 *	                  and pd_block_scn are present for layout parity
 *	                  but unused by the undo path.
 *	    32      32    Segment metadata (segment_id, segment_size_bytes,
 *	                  segment_state, owner_instance, tt_slots_count,
 *	                  head_block, tail_block, last_recyclable_block,
 *	                  segment_flags + 7-byte alignment pad to push the
 *	                  next SCN field to an 8-byte boundary).
 *	    64      32    Retention info (oldest_active_scn,
 *	                  commit_horizon_scn, created_at, last_used_at).
 *	    96      16    Statistics (total_records_written,
 *	                  total_bytes_used).
 *	    112     1536  TT slots array (48 × 32 B; spec-1.20 TTSlot).
 *	    1648    8     TT slot management counters (tt_next_free_slot,
 *	                  tt_active_count).
 *	    1656    1024  Free block bitmap (8192 blocks / 8 = 1024 B).
 *	    2680    5512  Reserved padding (future per-instance epoch +
 *	                  extent map + cleanout marker + cross-instance
 *	                  handoff state; all zero-init in Stage 1).
 *	    ----  total: 8192 bytes (one PG block).
 *
 *	  NOTE on offsets: the spec-1.21 v0.2 §2.1 body table listed segment
 *	  metadata as 28 bytes ending at offset 60, but C natural alignment
 *	  forces SCN (uint64) fields to 8-byte boundaries.  After
 *	  segment_flags lands at offset 56, _pad_57[7] (not [3]) pushes the
 *	  next SCN field to offset 64.  Net effect: every offset after
 *	  segment_flags shifts +4 vs the spec body table; total size remains
 *	  8192 bytes.  Hardening v1.0.1 amends spec-1.21; the on-disk
 *	  invariant is "all field offsets respect their type alignment, total
 *	  = 8192 bytes," not the literal byte values from v0.2.
 *
 *	  Status field semantics (offset 32, 1 byte):
 *	    0  = SEGMENT_ALLOCATED   freshly created, in pool, no tx yet
 *	                             (permanent placeholder; not "active")
 *	    1  = SEGMENT_ACTIVE      one or more tx using TT slots
 *	    2  = SEGMENT_COMMITTED   all tx committed; awaits retention
 *	    3  = SEGMENT_RECYCLABLE  safe to reuse for new segment
 *	    0xFF = SEGMENT_INVALID   corruption sentinel
 *
 *	  IMPORTANT: status value 0 is permanently "freshly allocated, no
 *	  tx activity yet", NOT "active" -- zero-init undo segment headers
 *	  (memset) and freshly allocated arrays land at status==0.
 *
 *	  owner_instance (offset 41, 1 byte; per current C struct layout
 *	  with 8-byte SCN alignment shift) holds (cluster_node_id + 1)
 *	  in [1, 128] when the segment is bound to an instance.  Value 0 is
 *	  the "unallocated" sentinel.  Mapping is implemented in spec-1.22
 *	  with explicit range validation; see Spec spec-1.21 §3.4.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_undo_segment.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-1.21-undo-segment-header.md
 *
 *	  The PageHeader prefix is mirrored manually (not embedded via
 *	  PageHeaderData) because PageHeaderData has a FLEXIBLE_ARRAY_MEMBER
 *	  pd_linp tail that prevents safe embedding as a non-last struct
 *	  member.  Field-by-field mirroring matches the on-disk byte layout
 *	  PG buffer manager expects when reading the segment header block.
 *
 *	  Frontend-safe: this header has no backend-only includes beyond
 *	  storage/bufpage.h (which itself is frontend-safe via the
 *	  POSTGRES_H gate).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_SEGMENT_H
#define CLUSTER_UNDO_SEGMENT_H

#include "c.h"						 /* uint8/16/32, bool, BLCKSZ */
#include "access/transam.h"			 /* TransactionId */
#include "datatype/timestamp.h"		 /* TimestampTz */
#include "storage/block.h"			 /* BlockNumber */
#include "storage/bufpage.h"		 /* PageXLogRecPtr, LocationIndex, etc. */
#include "cluster/cluster_scn.h"	 /* SCN, InvalidScn */
#include "cluster/cluster_tt_slot.h" /* TTSlot, TT_SLOTS_PER_SEGMENT */


/*
 * UndoSegmentState -- per-segment lifecycle state.
 *
 *   Value 0 (SEGMENT_ALLOCATED) is permanently the "freshly allocated"
 *   sentinel and MUST NOT be reassigned by future stages.  See file
 *   header for the "0 is NOT active" note.
 */
typedef enum UndoSegmentState {
	SEGMENT_ALLOCATED = 0,	/* permanent: freshly allocated, no tx yet */
	SEGMENT_ACTIVE = 1,		/* tx using TT slots (Stage 3+) */
	SEGMENT_COMMITTED = 2,	/* all tx committed; awaits retention */
	SEGMENT_RECYCLABLE = 3, /* safe to reuse for new segment */
	SEGMENT_INVALID = 0xFF	/* corruption sentinel */
} UndoSegmentState;


/*
 * Sentinel constants.  Stage 2+ extensions MUST NOT change these
 * values; cluster_unit invariants pin them.
 */
#define UNDO_SEGMENT_SIZE_BYTES (64 * 1024 * 1024) /* 64 MB */
#define UNDO_SEGMENT_HEADER_SIZE 8192			   /* 8 KB block */
#define UNDO_BLOCKS_PER_SEGMENT (UNDO_SEGMENT_SIZE_BYTES / BLCKSZ)
#define UNDO_FREE_BITMAP_BYTES 1024 /* 8192 blocks / 8 */

#define UNDO_OWNER_INSTANCE_INVALID ((uint8)0)
#define UNDO_OWNER_INSTANCE_MAX ((uint8)128) /* matches cluster.node_id 0..127 + 1 */

#define UNDO_SEGMENT_FLAGS_RESERVED ((uint8)0)

/*
 * UndoSegmentHeaderData.segment_flags bitfield (spec-3.8 D1 真激活).
 *
 *   UNDO_SEGMENT_FLAG_FULL (0x01): segment is write-closed — all blocks
 *   exhausted (free_block_bitmap full).  State remains ACTIVE per spec
 *   §3.3 I2 — 仅 fullness ≠ SEGMENT_COMMITTED (F3 codex review).  Set
 *   by cluster_undo_segment_mark_full() when bitmap shows no usable
 *   block left;  cleared on segment recycle (spec-3.12).
 */
#define UNDO_SEGMENT_FLAG_FULL ((uint8)0x01)


/*
 * State machine helper API (spec-3.8 D1):
 *   - cluster_undo_segment_mark_active() — ALLOCATED → ACTIVE
 *     transition on first successful record write
 *   - cluster_undo_segment_mark_full() — set UNDO_SEGMENT_FLAG_FULL
 *     when free_block_bitmap shows segment exhausted;  state remains
 *     ACTIVE (per spec §3.3 I2)
 *
 *   Per Hardening v1.0.1 H-1: spec-3.8 §3.3 I5 / §3.7 use
 *   "first_active_block" name;  linkdb SSOT field is tail_block
 *   (UndoSegmentHeaderData offset 48, 4B, spec-1.21 ship).
 *   Semantic: "oldest unrecycled block" = retention base.
 *   Implementation uses tail_block;  spec terminology maps:
 *   first_active_block ≡ tail_block (retention base).
 *
 *   Per Hardening v1.0.1 H-2: spec-3.8 §3.5 wrap_count text 暗示 field
 *   already exists;  linkdb SSOT has no segment-level wrap_count field.
 *   TT slot wrap (per-slot, spec-1.20) preserved as-is;  segment-level
 *   wrap_count field introduction deferred to spec-3.12 真 recycle.
 *   D4 changed to "TT slot wrap invariant preserve" (no-op per spec-1.20
 *   SSOT).
 */
extern bool cluster_undo_segment_mark_active(uint32 segment_id, uint8 owner_instance);
extern bool cluster_undo_segment_mark_full(uint32 segment_id, uint8 owner_instance);

/* D6 free_block_bitmap helpers (spec-3.8). */
extern void cluster_undo_segment_mark_block_used(uint32 segment_id, uint8 owner_instance,
												 uint32 block_no);
extern bool cluster_undo_segment_is_full(uint32 segment_id, uint8 owner_instance);

/* D5 retention base (tail_block) update (spec-3.8). */
extern bool cluster_undo_segment_tail_block_init(uint32 segment_id, uint8 owner_instance,
												 BlockNumber initial_tail);


/*
 * UndoSegmentHeaderData -- 8192-byte on-disk segment header.
 *
 *   Block 0 of every undo segment.  Field layout is the on-disk ABI;
 *   offsets are locked at compile time by the StaticAssertDecl block
 *   below.  spec-1.22 will populate fields when allocating segments;
 *   Stage 3 will update tt_slots when transactions write undo records.
 */
typedef struct UndoSegmentHeaderData {
	/* === 32 bytes: PG standard PageHeader prefix === */
	PageXLogRecPtr pd_lsn;		/* offset 0;  8 B */
	uint16 pd_checksum;			/* offset 8;  2 B */
	uint16 pd_flags;			/* offset 10; 2 B */
	LocationIndex pd_lower;		/* offset 12; 2 B */
	LocationIndex pd_upper;		/* offset 14; 2 B */
	LocationIndex pd_special;	/* offset 16; 2 B */
	uint16 pd_pagesize_version; /* offset 18; 2 B */
	TransactionId pd_prune_xid; /* offset 20; 4 B; unused by undo path */
	SCN pd_block_scn;			/* offset 24; 8 B; spec-1.4 cluster ext */

	/* === 32 bytes: segment metadata === */
	uint32 segment_id;				   /* offset 32; 4 B; global unique ID */
	uint32 segment_size_bytes;		   /* offset 36; 4 B; default 64 MB */
	uint8 segment_state;			   /* offset 40; 1 B; UndoSegmentState enum */
	uint8 owner_instance;			   /* offset 41; 1 B; (node_id + 1) ∈ [1, 128]; 0 = invalid */
	uint16 tt_slots_count;			   /* offset 42; 2 B; default TT_SLOTS_PER_SEGMENT (48) */
	BlockNumber head_block;			   /* offset 44; 4 B; current write position */
	BlockNumber tail_block;			   /* offset 48; 4 B; oldest unrecycled block */
	BlockNumber last_recyclable_block; /* offset 52; 4 B; recycle horizon */
	uint8 segment_flags;			   /* offset 56; 1 B; reserved + cleanout */
	uint8 _pad_57[7];				   /* offset 57-63; align next SCN to 8 */

	/* === 32 bytes: retention info === */
	SCN oldest_active_scn;	  /* offset 64; 8 B */
	SCN commit_horizon_scn;	  /* offset 72; 8 B */
	TimestampTz created_at;	  /* offset 80; 8 B */
	TimestampTz last_used_at; /* offset 88; 8 B */

	/* === 16 bytes: statistics === */
	uint64 total_records_written; /* offset 96;  8 B */
	uint64 total_bytes_used;	  /* offset 104; 8 B */

	/* === 1536 bytes: TT slots array (48 × 32 B) === */
	TTSlot tt_slots[TT_SLOTS_PER_SEGMENT]; /* offset 112 */

	/* === 8 bytes: TT slot management counters === */
	uint32 tt_next_free_slot; /* offset 1648; 4 B; next free slot index */
	uint32 tt_active_count;	  /* offset 1652; 4 B; active slot count */

	/* === 1024 bytes: free block bitmap === */
	uint8 free_block_bitmap[UNDO_FREE_BITMAP_BYTES]; /* offset 1656 */

	/* === 5512 bytes: reserved padding to 8192 byte total ===
	 *
	 *   Reserved for future fields: per-instance epoch (AD-012 例外
	 *   9-10), extent map, cleanout marker, cross-instance handoff
	 *   state, etc.  All zero-init in Stage 1; Stage 2+ specs may
	 *   carve named fields here (with catversion bump).
	 */
	uint8 _reserved[8192 - 1656 - UNDO_FREE_BITMAP_BYTES]; /* offset 2680 */
														   /* total: 8192 bytes (one PG block) */
} UndoSegmentHeaderData;


/*
 * On-disk ABI invariants -- enforced at compile time.
 *
 *   Spec: spec-1.21-undo-segment-header.md §2.3.
 */
StaticAssertDecl(sizeof(UndoSegmentHeaderData) == UNDO_SEGMENT_HEADER_SIZE,
				 "spec-1.21 invariant: UndoSegmentHeaderData on-disk size MUST stay "
				 "8192 bytes (one PG block)");
StaticAssertDecl(offsetof(UndoSegmentHeaderData, segment_id) == 32,
				 "spec-1.21 invariant: segment_id at byte 32 (after 32-byte PageHeader prefix)");
StaticAssertDecl(offsetof(UndoSegmentHeaderData, tt_slots) == 112,
				 "spec-1.21 invariant: tt_slots embedded at byte 112 "
				 "(C natural alignment shifts the v0.2 body table value 108 by +4)");
StaticAssertDecl(offsetof(UndoSegmentHeaderData, free_block_bitmap) == 1656,
				 "spec-1.21 invariant: free_block_bitmap at byte 1656 "
				 "(C natural alignment shifts the v0.2 body table value 1652 by +4)");
StaticAssertDecl(UNDO_BLOCKS_PER_SEGMENT == 8192,
				 "spec-1.21 invariant: 64 MB segment / 8 KB block = 8192 blocks");
/*
 * P2-2 (v0.2 amend): pgrac requires BLCKSZ == 8192.  All bitmap /
 * header / TT slot count math derives from this assumption.  If a
 * future PG fork build configures BLCKSZ != 8192, this assert fires
 * at compile time before any runtime corruption can occur.
 */
StaticAssertDecl(BLCKSZ == 8192, "spec-1.21 invariant: pgrac requires BLCKSZ == 8192 for "
								 "undo segment header layout; building with "
								 "configure --with-blocksize != 8 is not supported");


/*
 * Inline helpers -- frontend-safe; no runtime state.
 *
 *   UndoSegmentHeader_is_available: returns true iff the segment may
 *   be bound to a new transaction by the allocator.  Stage 1 always
 *   returns true for SEGMENT_ALLOCATED; Stage 3+ also returns true
 *   for SEGMENT_RECYCLABLE so callers don't break across stages.
 *
 *   Naming note (spec-1.21 v0.2 P3-1): an earlier draft called this
 *   is_unallocated() but that is misleading -- SEGMENT_ALLOCATED
 *   literally means "in the allocator pool" (not "unallocated").
 *   "is_available" reflects what the allocator actually checks.
 *
 *   UndoSegmentHeader_is_active: returns true iff the segment has at
 *   least one active transaction in its TT slots.
 *
 *   UndoSegmentHeader_owner: returns the owner instance value
 *   (cluster_node_id + 1; 0 if unallocated).  Stage 3+ visibility
 *   paths use this to route TT slot lookups.
 */
static inline bool
UndoSegmentHeader_is_available(const UndoSegmentHeaderData *hdr)
{
	return hdr->segment_state == SEGMENT_ALLOCATED || hdr->segment_state == SEGMENT_RECYCLABLE;
}

static inline bool
UndoSegmentHeader_is_active(const UndoSegmentHeaderData *hdr)
{
	return hdr->segment_state == SEGMENT_ACTIVE;
}

static inline uint8
UndoSegmentHeader_owner(const UndoSegmentHeaderData *hdr)
{
	return hdr->owner_instance;
}


/*
 * spec-3.8 Fix #375:  pure-logic kernels extracted from cluster_undo_alloc.c
 * so cluster_unit behavioural tests can exercise the lifecycle invariants
 * without postgres shmem / file I/O.  The backend wrappers in alloc.c call
 * these same kernels on the in-memory header buffer after pread(),  so the
 * cluster_unit tests cover the actual invariant logic.
 *
 *   UndoSegmentBitmap_mark_used:  set bit for block_no; returns true iff
 *	   the bit changed (idempotency proof). Caller must ensure block_no in
 *	   [0, UNDO_BLOCKS_PER_SEGMENT).
 *
 *   UndoSegmentBitmap_count_free_capped:  count clear bits up to cap + 1
 *	   then short-circuit.  Used by is_full(margin = 1).
 *
 *   UndoSegmentBitmap_is_full:  free count <= 1 (1 = safety margin per
 *	   spec §3.7).
 *
 *   UndoSegmentState_can_become_active:  ALLOCATED → true (legal transition),
 *	   ACTIVE → true (idempotent), others → false (fail-closed per spec §3.3 I3).
 *
 *   UndoSegmentFlags_is_full:  pure flag read.
 */
static inline bool
UndoSegmentBitmap_mark_used(uint8 *bitmap, uint32 block_no)
{
	uint32 byte_idx = block_no / 8;
	uint8  bit_mask = (uint8) (1u << (block_no % 8));

	if ((bitmap[byte_idx] & bit_mask) != 0)
		return false;
	bitmap[byte_idx] |= bit_mask;
	return true;
}

static inline uint32
UndoSegmentBitmap_count_free_capped(const uint8 *bitmap, uint32 bytes, uint32 cap)
{
	uint32 free_count = 0;
	uint32 i;

	for (i = 0; i < bytes; i++) {
		uint8 byte = bitmap[i];
		uint8 j;

		for (j = 0; j < 8; j++) {
			if ((byte & ((uint8) 1u << j)) == 0) {
				free_count++;
				if (free_count > cap)
					return free_count;
			}
		}
	}
	return free_count;
}

static inline bool
UndoSegmentBitmap_is_full(const uint8 *bitmap, uint32 bytes)
{
	return UndoSegmentBitmap_count_free_capped(bitmap, bytes, 1) <= 1;
}

static inline bool
UndoSegmentState_can_become_active(uint8 state)
{
	return state == SEGMENT_ALLOCATED || state == SEGMENT_ACTIVE;
}

static inline bool
UndoSegmentFlags_is_full(uint8 flags)
{
	return (flags & UNDO_SEGMENT_FLAG_FULL) != 0;
}


#endif /* CLUSTER_UNDO_SEGMENT_H */
