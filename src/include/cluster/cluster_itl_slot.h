/*-------------------------------------------------------------------------
 *
 * cluster_itl_slot.h
 *	  pgrac cluster ITL (Interested Transaction List) slot typedef +
 *	  invariants.
 *
 *	  Stage 1.5 ships only the typedef + UBA stub + constants + flags
 *	  enum.  All ITL slots are zero-init'd at PageInitHeapPage time and
 *	  remain in ITL_FLAG_FREE state for the duration of stage 1.5; the
 *	  actual write / reuse / cleanout state machine + INITRANS DDL
 *	  lands at Stage 3 (AD-006 第五轮 ~27000 LOC) when visibility
 *	  改造 真值 接管.
 *
 *	  Each heap page in pgrac 1.5+ contains an array of 8 (= INITRANS)
 *	  ClusterItlSlotData stored in PG's special area at the page tail
 *	  (offset 7808-8191 for 8 KB pages), totalling 384 bytes.  This is
 *	  the same place btree / hash / gin / gist / brin / spgist store
 *	  their per-page opaque metadata (BTPageOpaque etc.) -- pgrac uses
 *	  the special area for ITL on heap pages, identified by the
 *	  PD_HAS_ITL flag bit (0x0008) in pd_flags.  Index pages do NOT
 *	  contain ITL slots (docs/block-format-design.md v1.0 §17.8 + v1.2
 *	  修订: heap am 调 PageInitHeapPage; index am 调 PageInit).
 *
 *	  PIVOT A (2026-05-02 user 拍板): the original layout placed ITL
 *	  immediately after the PageHeader (offset 32-415), but that broke
 *	  PG's pd_linp[FLEXIBLE_ARRAY_MEMBER] struct field access (30+
 *	  page macros assume line pointers start at offsetof = 32).  The
 *	  special area is the only ABI-safe place to put 384 B of per-
 *	  heap-page metadata -- and it also happens to match Oracle's
 *	  block trailer layout for ITL.  See spec-1.5-itl-slot.md §1.4
 *	  例外说明 #6 + docs/block-format-design.md v1.2 §4.1.
 *
 *	  On disk: ITL slots store 4 SCN-typed fields (commit_scn /
 *	  write_scn) and 1 LSN-typed field (first_change_lsn).  Stage 1.5
 *	  writes all of them as InvalidScn / InvalidXLogRecPtr placeholders;
 *	  Stage 1.16+ takes over real write_scn / commit_scn at commit
 *	  time, and spec-1.20-1.22 (TT slot + undo segment) populates the
 *	  UBA pointer.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_itl_slot.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-1.5-itl-slot.md
 *	  Design: docs/block-format-design.md v1.2 §4.3 + v1.2 §15 阶段 2
 *	  AD-006 (MVCC 三件套; ITL is the physical anchor for
 *	  Oracle-style row-level lock + per-block transaction queue).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_ITL_SLOT_H
#define CLUSTER_ITL_SLOT_H

#include "c.h"					 /* uint8 / uint16 / uint64 */
#include "access/transam.h"		 /* TransactionId, InvalidTransactionId */
#include "access/xlogdefs.h"	 /* XLogRecPtr, InvalidXLogRecPtr */
#include "cluster/cluster_scn.h" /* SCN, InvalidScn (spec-1.4) */


/*
 * ClusterItlFlags -- ITL slot lifecycle state.
 *
 *	Stage 1.5 ships only the enum values; all slots are zero-init'd
 *	to ITL_FLAG_FREE.  Stage 3 (AD-006 第五轮) implements the actual
 *	state machine:
 *	    FREE -> ACTIVE (transaction touches block)
 *	    ACTIVE -> COMMITTED (commit_scn populated)
 *	         or -> ABORTED (abort path)
 *	    COMMITTED -> NEEDS_CLEANOUT (lazy row-level commit stamp)
 *	    NEEDS_CLEANOUT -> FREE (slot 复用 by next transaction)
 */
typedef enum {
	ITL_FLAG_FREE = 0,			 /* slot is reusable (zero-init occupies this state) */
	ITL_FLAG_ACTIVE = 1,		 /* xid is currently writing; commit_scn unset */
	ITL_FLAG_COMMITTED = 2,		 /* xid committed; commit_scn populated */
	ITL_FLAG_ABORTED = 3,		 /* xid aborted; commit_scn = InvalidScn */
	ITL_FLAG_NEEDS_CLEANOUT = 4, /* commit/abort done but heap rows not yet stamped */
	/* spec-3.4d (v0.2 F2 / Q1): lock-only ITL states for heap_lock_tuple.
	 * Distinct enum values keep existing equality checks unchanged
	 * (slot.flags == ITL_FLAG_ACTIVE etc.) while letting lock-only slot
	 * states be discovered by raw_xmax scan helper.  Use
	 * ITL_FLAG_IS_LOCK_ONLY() to test the category. */
	ITL_FLAG_LOCK_ONLY_ACTIVE = 5,	  /* lock_xid holds row-lock; commit_scn unset */
	ITL_FLAG_LOCK_ONLY_COMMITTED = 6, /* lock_xid committed; lock released */
	ITL_FLAG_LOCK_ONLY_ABORTED = 7,	  /* lock_xid aborted; lock released */
	/*
	 * spec-3.6 v0.3 D7b NEW page-format ITL flag:  MultiXact xmax marker.
	 *	slot->xid stores the local MultiXactId (NOT a TransactionId).
	 *	Used by cluster_itl_find_multixact_origin_by_xmax() reader to
	 *	derive origin info for building ClusterMultiXactKey overlay
	 *	lookup.  Page-format ABI extension (HC208 catversion bump).
	 *
	 *	Spec-3.6 partial coverage caveat:  origin_node_id derived from
	 *	current cluster_node_id (ClusterPair fixture writer = reader);
	 *	真 shared-heap Stage 4+ 时需 marker slot 自身编码 origin (UBA
	 *	high bytes 或扩 slot field;留 spec-3.6b/3.7).
	 */
	ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI = 8
} ClusterItlFlags;

/* spec-3.4d helper:  test if an ITL slot flag value is one of the
 * lock-only states (LOCK_ONLY_ACTIVE / LOCK_ONLY_COMMITTED /
 * LOCK_ONLY_ABORTED).  Used by raw_xmax scan + xact-end finish path. */
#define ITL_FLAG_IS_LOCK_ONLY(f)                                                                   \
	((f) == ITL_FLAG_LOCK_ONLY_ACTIVE || (f) == ITL_FLAG_LOCK_ONLY_COMMITTED                       \
	 || (f) == ITL_FLAG_LOCK_ONLY_ABORTED)

/* spec-3.4d helper:  test if an ITL slot is a completed lock-only slot
 * (LOCK_ONLY_COMMITTED / LOCK_ONLY_ABORTED).  Used by allocator to
 * recycle slot when scanning for FREE. */
#define ITL_FLAG_IS_LOCK_ONLY_COMPLETED(f)                                                         \
	((f) == ITL_FLAG_LOCK_ONLY_COMMITTED || (f) == ITL_FLAG_LOCK_ONLY_ABORTED)


/*
 * UBA -- Undo Block Address (16 bytes).
 *
 *	Stub at stage 1.5.  TTSlot in spec-1.20 reuses this typedef for
 *	first_undo_block; the 16-byte width is locked at stage 1.5 so the
 *	on-disk ITL slot format never changes when the encoding lands.
 *
 *	Full encoding (segment_id, block, slot, row) is deferred to a
 *	later spec; until then all UBA fields are zero-init'd (treated as
 *	InvalidUba).
 */
typedef struct UBA {
	uint64 raw[2]; /* 16 bytes; all zero = InvalidUba */
} UBA;

/*
 * InvalidUba -- the "absent" / "not yet allocated" sentinel.
 *
 *	Locked to all-zero by spec-1.5; matches the zero-init occupancy
 *	pattern used by InvalidScn (spec-1.4) and InvalidTransactionId
 *	(PG vanilla).
 */
#define InvalidUba_init                                                                            \
	{                                                                                              \
		{                                                                                          \
			0, 0                                                                                   \
		}                                                                                          \
	}

static inline bool
UBA_is_invalid(UBA u)
{
	return u.raw[0] == 0 && u.raw[1] == 0;
}


/*
 * ClusterItlSlotData -- 48-byte ITL slot.
 *
 *	One slot per concurrent transaction touching a heap block.  Default
 *	INITRANS = 8 → 384 bytes per heap page stored in PG special area
 *	at the page tail (PIVOT A, see file header).
 *
 *	Field layout (offsets MUST stay stable; cluster_unit
 *	test_cluster_itl_slot enforces via offsetof asserts):
 *
 *	    offset  size  field             stage-1.5 placeholder value
 *	    ------  ----  ----------------- ---------------------------
 *	    0       4     xid                InvalidTransactionId (= 0)
 *	    4       2     wrap               0
 *	    6       1     flags              ITL_FLAG_FREE (= 0)
 *	    7       1     lock_count         0
 *	    8       16    undo_segment_head  InvalidUba (all zero)
 *	    24      8     commit_scn         InvalidScn (= 0)
 *	    32      8     write_scn          InvalidScn (= 0)
 *	    40      8     first_change_lsn   InvalidXLogRecPtr (= 0)
 *	    ----  total: 48 bytes
 *
 *	Field semantics: see docs/block-format-design.md v1.2 §4.3.
 */
typedef struct ClusterItlSlotData {
	/* 8 bytes: transaction identification */
	TransactionId xid; /* offset 0;  InvalidTransactionId at stage 1.5 */
	uint16 wrap;	   /* offset 4;  slot reuse counter; 0 at stage 1.5 */
	uint8 flags;	   /* offset 6;  ClusterItlFlags; ITL_FLAG_FREE at stage 1.5 */
	uint8 lock_count;  /* offset 7;  row lock count; 0 at stage 1.5 */

	/* 16 bytes: UBA */
	UBA undo_segment_head; /* offset 8;  InvalidUba at stage 1.5 */

	/* 8 bytes: commit metadata */
	SCN commit_scn; /* offset 24; InvalidScn at stage 1.5 */

	/* 8 bytes: Lamport / xl_scn write watermark */
	SCN write_scn; /* offset 32; InvalidScn at stage 1.5 */

	/* 8 bytes: crash recovery anchor */
	XLogRecPtr first_change_lsn; /* offset 40; InvalidXLogRecPtr at stage 1.5 */
} ClusterItlSlotData;


/*
 * ClusterItlPageHeader -- 8-byte per-page ITL header (spec-3.10 §v0.5).
 *
 *	Stored at the END of the heap page special area, immediately AFTER the
 *	384-byte slot array (special layout: slots[8] (384B) || header (8B) =
 *	392B).  Placing the header after the slots keeps slot 0 at
 *	PageGetSpecialPointer(page), so every existing ClusterPageGetItlSlots
 *	caller and the 26 CLUSTER_ITL_ARRAY_SIZE slot-access sites are
 *	unchanged; only the special-area SIZE grows 384 -> 392.
 *
 *	itl_recycle_watermark_scn:  the maximum write_scn of any *completed
 *	data* ITL slot evicted from this page by slot reuse (i.e. recycled by
 *	cluster_itl_alloc_or_reuse_slot when no FREE slot was available).  CR
 *	construction fails closed (53R9F SNAPSHOT_TOO_OLD) when this watermark
 *	is newer than a reader's read_scn: a post-read_scn writer's undo-chain
 *	anchor may have been overwritten and is therefore absent from the
 *	candidate set, and page-level prune cannot distinguish that case from a
 *	legitimate pre-read_scn creator (spec-3.10 §v0.5.A).  Monotone
 *	non-decreasing; restored by ITL delta WAL redo (NOT FPI-only).
 *	InvalidScn at page init.  spec-3.11 durable TT replaces the fail-closed
 *	with precise per-writer commit_scn resolution.
 */
typedef struct ClusterItlPageHeader {
	SCN itl_recycle_watermark_scn; /* offset 0; InvalidScn at init */
} ClusterItlPageHeader;


/*
 * Compile-time invariants.
 *
 *	cluster_unit test_cluster_itl_slot enforces these via UT_ASSERT_EQ
 *	on sizeof / offsetof; cluster_unit test_cluster_block_format
 *	enforces SizeofHeapTupleHeader == 24 (PG +1B for t_itl_slot_idx)
 *	and PD_HAS_ITL == 0x0008.
 */
#define CLUSTER_ITL_SLOT_SIZE 48
#define CLUSTER_ITL_INITRANS_DEFAULT 8
#define CLUSTER_ITL_ARRAY_SIZE (CLUSTER_ITL_SLOT_SIZE * CLUSTER_ITL_INITRANS_DEFAULT) /* 384 */

/*
 * spec-3.10 §v0.5: heap page special area = 384-byte slot array + 8-byte
 * ClusterItlPageHeader (recycle watermark) = 392 bytes.  CLUSTER_ITL_ARRAY_SIZE
 * (384) is still the slot-array size (slots stay at special offset 0);
 * CLUSTER_ITL_SPECIAL_SIZE (392) is the total reserved special area and is what
 * PageInitHeapPage / MaxHeapTupleSize / HeapPageSpecialSize must use.
 */
#define CLUSTER_ITL_PAGE_HEADER_SIZE 8
#define CLUSTER_ITL_SPECIAL_SIZE (CLUSTER_ITL_ARRAY_SIZE + CLUSTER_ITL_PAGE_HEADER_SIZE) /* 392 */


/*
 * CLUSTER_ITL_SLOT_UNALLOCATED -- the "no ITL slot assigned" sentinel
 *	for HeapTupleHeader.t_itl_slot_idx.
 *
 *	Stage 1.5 always writes 255 (heap_form_tuple / heap_modify_tuple
 *	path); Stage 3 (AD-006 第五轮) populates real 0..N indexes when
 *	a transaction touches the row.
 */
#define CLUSTER_ITL_SLOT_UNALLOCATED 255U


#endif /* CLUSTER_ITL_SLOT_H */
