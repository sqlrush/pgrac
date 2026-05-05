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


#endif /* CLUSTER_TT_SLOT_H */
