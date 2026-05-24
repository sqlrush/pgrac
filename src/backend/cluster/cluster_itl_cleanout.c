/*-------------------------------------------------------------------------
 *
 * cluster_itl_cleanout.c
 *	  pgrac delayed cleanout MVP helpers — lazy (reader-path
 *	  opportunistic) only (spec-3.4c D1 + D5).
 *
 *	  See cluster_itl_cleanout.h for the full contract.  Notable
 *	  spec-3.4c F1/F2/F3 hard rules:
 *
 *	    F1  HeapTupleSatisfiesMVCC has no Relation, so this helper
 *	        MUST NOT emit generic WAL.  It uses MarkBufferDirtyHint
 *	        (hint-style, no WAL).  Crash may lose the hint; the
 *	        overlay path guarantees correctness.
 *
 *	    F2  expected_xid is required to defend against the L189 slot
 *	        recycle race.  can_stamp() verifies xid, flags, and that
 *	        commit_scn is still InvalidScn before mutating.
 *
 *	    F3  No eager helper here.  Commit-time eager cleanout =
 *	        spec-3.4a itl_finish_stamp_page() + spec-3.4c D14 per-
 *	        page aggregate.  This file is lazy-only.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.4c-delayed-cleanout-d5b-commit-scn-yellow-perf-hardening.md
 *       (v0.3 FROZEN 2026-05-24)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_itl_cleanout.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_itl_cleanout.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_scn.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "utils/elog.h"


#ifdef USE_PGRAC_CLUSTER

/*
 * cluster_itl_cleanout_can_stamp
 *
 *	See header.  Caller must already hold the buffer's content lock.
 */
bool
cluster_itl_cleanout_can_stamp(const ClusterItlSlotData *slot, TransactionId expected_xid,
							   SCN expected_commit_scn)
{
	if (slot == NULL)
		return false;
	if (slot->flags != ITL_FLAG_ACTIVE)
		return false;
	if (slot->xid != expected_xid)
		return false;
	/*
	 * commit_scn must still be Invalid (no other backend has raced ahead and
	 * stamped this slot already).  expected_commit_scn must itself be valid;
	 * a caller passing InvalidScn is a logic bug.
	 */
	if (SCN_VALID(slot->commit_scn))
		return false;
	if (!SCN_VALID(expected_commit_scn))
		return false;
	return true;
}


/*
 * cluster_itl_cleanout_lazy
 *
 *	See header.  L177 no-wait, F1 hint-style, F2 xid-guarded.
 */
bool
cluster_itl_cleanout_lazy(Buffer buf, uint8 slot_idx, TransactionId expected_xid,
						  SCN expected_commit_scn)
{
	Page page;
	ClusterItlSlotData *slot;

	if (!BufferIsValid(buf))
		return false;
	if (slot_idx >= CLUSTER_ITL_INITRANS_DEFAULT)
		return false;
	if (!TransactionIdIsValid(expected_xid))
		return false;
	if (!SCN_VALID(expected_commit_scn))
		return false;

	/*
	 * L177 no-wait fail-fast.  If the lock would block, return false
	 * immediately; the reader has already made its visibility decision via
	 * the overlay and does not need this stamp.
	 */
	if (!ConditionalLockBuffer(buf))
		return false;

	page = BufferGetPage(buf);
	if (!PageHasItl(page)) {
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		return false;
	}

	slot = &ClusterPageGetItlSlots(page)[slot_idx];

	/*
	 * F2 + concurrent-stamp defense.  Re-verify under lock; another backend
	 * may have raced ahead or the slot may have been recycled by L189.
	 */
	if (!cluster_itl_cleanout_can_stamp(slot, expected_xid, expected_commit_scn)) {
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		return false;
	}

	/* Hint-style page mutation: stamp + dirty-hint, no WAL (F1). */
	slot->commit_scn = expected_commit_scn;
	slot->flags = ITL_FLAG_COMMITTED;

	/*
	 * MarkBufferDirtyHint(buf, true): the buffer is "standard" (heap layout)
	 * so PG can compute the page checksum correctly when the dirty hint is
	 * eventually flushed.  No WAL is emitted; this matches the HEAP_XMIN_
	 * COMMITTED hint-bit pattern used by HeapTupleSatisfies* on the reader
	 * path.  If the buffer is evicted before flush, the hint is lost; the
	 * next reader will re-resolve via the TT status overlay.
	 */
	MarkBufferDirtyHint(buf, true);

	LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	return true;
}


#else /* !USE_PGRAC_CLUSTER */

bool
cluster_itl_cleanout_can_stamp(const ClusterItlSlotData *slot pg_attribute_unused(),
							   TransactionId expected_xid pg_attribute_unused(),
							   SCN expected_commit_scn pg_attribute_unused())
{
	return false;
}

bool
cluster_itl_cleanout_lazy(Buffer buf pg_attribute_unused(), uint8 slot_idx pg_attribute_unused(),
						  TransactionId expected_xid pg_attribute_unused(),
						  SCN expected_commit_scn pg_attribute_unused())
{
	return false;
}

#endif /* USE_PGRAC_CLUSTER */
