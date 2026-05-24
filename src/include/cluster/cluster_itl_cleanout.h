/*-------------------------------------------------------------------------
 *
 * cluster_itl_cleanout.h
 *	  pgrac delayed cleanout MVP helpers (spec-3.4c D1).
 *
 *	  spec-3.4c lazy cleanout = reader-path opportunistic stamp.
 *	  HeapTupleSatisfiesMVCC spec-3.2 D5 fork after decide_by_scn
 *	  returns VISIBLE + slot is still ACTIVE (overlay already has
 *	  COMMITTED but the on-page ItlSlotData has not been stamped) →
 *	  this helper tries to take an EXCLUSIVE content lock on the
 *	  buffer non-blocking; on success, stamps slot.commit_scn +
 *	  slot.flags = COMMITTED, then MarkBufferDirtyHint(buf, true).
 *	  On any failure (lock would block, slot state changed, xid
 *	  mismatch, commit_scn mismatch) returns false immediately and
 *	  the reader proceeds with the overlay-derived decision.
 *
 *	  spec-3.4c F1 hint-style:  HeapTupleSatisfiesMVCC only has a
 *	  Buffer argument -- no Relation / RelationNeedsWAL() context --
 *	  so this helper MUST NOT emit generic WAL.  It marks the buffer
 *	  dirty as a hint (PG-standard reader-path mutation pattern,
 *	  same as HEAP_XMIN_COMMITTED hint bit).  Crash may lose the
 *	  hint; the next reader will re-resolve via the TT status
 *	  overlay.  Correctness is guaranteed by the overlay path; this
 *	  helper is a perf optimization only.
 *
 *	  spec-3.4c F2:  the helper requires expected_xid to defend
 *	  against the L189 slot recycle race -- a slot whose original
 *	  owner xid was X may have been recycled to xid Y by another
 *	  backend; stamping based on (slot_idx, commit_scn) without xid
 *	  verification would corrupt Y's slot with X's commit_scn.
 *
 *	  Eager (commit-time) cleanout is NOT in this header -- spec-3.4c
 *	  F3 deletes the fake `cluster_itl_cleanout_eager` helper because
 *	  the eager path = spec-3.4a `itl_finish_stamp_page()` already +
 *	  spec-3.4c D14 per-page aggregate performance hardening.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.4c-delayed-cleanout-d5b-commit-scn-yellow-perf-hardening.md
 *       (v0.3 FROZEN 2026-05-24)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_itl_cleanout.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_ITL_CLEANOUT_H
#define CLUSTER_ITL_CLEANOUT_H

#include "c.h"
#include "access/transam.h"			 /* TransactionId */
#include "cluster/cluster_itl_slot.h" /* ClusterItlSlotData */
#include "cluster/cluster_scn.h"	 /* SCN */
#include "storage/buf.h"			 /* Buffer */


/*
 * cluster_itl_cleanout_lazy
 *
 *	Reader-path opportunistic lazy cleanout.  Invoked by
 *	HeapTupleSatisfiesMVCC spec-3.2 D5 fork AFTER decide_by_scn
 *	returns VISIBLE and slot state is still ACTIVE on page.
 *
 *	Attempts ConditionalLockBuffer(buf) (non-blocking exclusive
 *	content lock).  On failure (would block), returns false
 *	immediately (L177 no-wait).
 *
 *	On lock success:
 *	  1. Re-read slot.  If state has changed (already COMMITTED, xid
 *	     mismatch, commit_scn mismatch, or slot transitioned to FREE),
 *	     release lock + return false.
 *	  2. Otherwise: stamp slot.commit_scn = expected_commit_scn;
 *	     slot.flags = ITL_FLAG_COMMITTED; MarkBufferDirtyHint(buf, true)
 *	     (F1 hint-style, no WAL); release lock; return true.
 *
 *	Caller MUST NOT depend on cleanup success.  Visibility decision is
 *	already made via the overlay path before this helper is called.
 *	Crash may lose the hint; correctness is guaranteed by the overlay.
 *
 *	F2:  expected_xid defends against L189 slot recycle race.  Caller
 *	should pass the tuple's xmin (TransactionId that allocated this
 *	ITL slot for this tuple version).
 */
extern bool cluster_itl_cleanout_lazy(Buffer buf, uint8 slot_idx,
									  TransactionId expected_xid,
									  SCN expected_commit_scn);


/*
 * cluster_itl_cleanout_can_stamp
 *
 *	Internal helper exported for unit testing.  Verifies that the
 *	slot is in a state where lazy cleanout is safe to apply:
 *	  - slot->flags == ITL_FLAG_ACTIVE  (not already stamped / freed)
 *	  - slot->xid == expected_xid       (no L189 recycle race)
 *	  - slot->commit_scn is InvalidScn  (no concurrent stamp from
 *	                                     another backend)
 *	The expected_commit_scn is the value the caller intends to write;
 *	it is the overlay-derived commit_scn and is independent of the
 *	slot's current commit_scn field (which should still be InvalidScn).
 *
 *	Caller MUST hold the buffer's content lock when checking.
 */
extern bool cluster_itl_cleanout_can_stamp(const ClusterItlSlotData *slot,
										   TransactionId expected_xid,
										   SCN expected_commit_scn);


#endif /* CLUSTER_ITL_CLEANOUT_H */
