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

#include "access/xact.h"	/* GetCurrentTransactionNestLevel (spec-3.4a N9) */
#include "storage/bufmgr.h" /* BufferGetPage, MarkBufferDirty (spec-3.4a D2) */
#include "storage/bufpage.h"
#include "cluster/cluster_guc.h" /* cluster_enabled */
#include "cluster/cluster_itl.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_slot.h"

#ifdef USE_PGRAC_CLUSTER

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

	/* Empty / free slot — no TT binding to report. */
	if (slot->flags == ITL_FLAG_FREE)
		return false;

	/*
	 * Fill the read-only ref.  After spec-3.4a D10 the slot may carry
	 * real xid + flags + commit_scn from the heap AM write path (D3-D5)
	 * and the xact pre-commit hook (D6) -- this reader exposes those
	 * via cached_commit_scn / has_cached_status.
	 *
	 * The origin/segment/tt_slot triple is still ZEROED here: spec-3.4a
	 * does not yet land UBA encoding or per-undo-segment TT slot
	 * allocation (推 spec-3.4b).  spec-3.2 D5 visibility fork keys on
	 * `ref.tt_slot_id != 0`, so a zero triple causes that fork to fall
	 * back to PG-native silent-invisible.  Production cross-node
	 * visibility consequently remains "silent invisible" until spec-3.4b
	 * ships real UBA decode + segment->owner_inst mapping; that is the
	 * honest-scope boundary between 3.4a and 3.4b.
	 */
	ref->origin_node_id = 0;
	ref->undo_segment_id = 0;
	ref->tt_slot_id = 0;
	ref->cluster_epoch = 0;
	ref->local_xid = slot->xid;
	ref->cached_commit_scn = slot->commit_scn;
	ref->has_cached_status = (slot->flags == ITL_FLAG_COMMITTED && SCN_VALID(slot->commit_scn));
	memset(ref->_padding, 0, sizeof(ref->_padding));

	return true;
}

/* ---------- spec-3.4a D2 — writer API ---------- */

bool
cluster_itl_alloc_or_reuse_slot(Buffer buf, TransactionId top_xid, uint8 *out_slot_idx)
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
			return true;
		}
		if (slots[i].flags == ITL_FLAG_FREE && free_idx < 0)
			free_idx = i;
		else if (slots[i].flags != ITL_FLAG_ACTIVE && reusable_idx < 0)
			reusable_idx = i;
	}

	if (free_idx >= 0) {
		*out_slot_idx = (uint8)free_idx;
		return true;
	}

	if (reusable_idx >= 0) {
		*out_slot_idx = (uint8)reusable_idx;
		return true;
	}

	return false; /* OVERFLOW — caller raises ERROR before CRIT */
}

void
cluster_itl_stamp_active(Buffer buf, uint8 slot_idx, TransactionId xid, SCN write_scn)
{
	Page page;
	ClusterItlSlotData *slot;

	Assert(BufferIsValid(buf));
	Assert(slot_idx < CLUSTER_ITL_INITRANS_DEFAULT);

	page = BufferGetPage(buf);
	Assert(PageHasItl(page));

	slot = &ClusterPageGetItlSlots(page)[slot_idx];
	if (slot->flags != ITL_FLAG_FREE && !(slot->flags == ITL_FLAG_ACTIVE && slot->xid == xid))
		slot->wrap++;
	slot->xid = xid;
	slot->flags = ITL_FLAG_ACTIVE;
	slot->commit_scn = InvalidScn;
	slot->write_scn = write_scn;
	/* undo_segment_head / first_change_lsn untouched -- spec-3.4b/c populate. */

	MarkBufferDirty(buf);
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
	if (!cluster_enabled || cluster_node_id < 0)
		return;

	/*
	 * spec-3.4a N9: subxact / savepoint ITL writable path 推 spec-3.5
	 * SUBTRANS.  The production heap AM path skips cluster ITL touch
	 * registration for subtransactions via its relation-aware gate.  Keep
	 * this helper as a hard caller fence for any future direct ITL writable
	 * entry point that cannot use that gate.
	 */
	if (GetCurrentTransactionNestLevel() > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cluster ITL writable path does not support subtransactions yet"),
				 errhint("Retry without savepoints or wait for spec-3.5 SUBTRANS support.")));
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

void
cluster_itl_stamp_active(Buffer buf pg_attribute_unused(), uint8 slot_idx pg_attribute_unused(),
						 TransactionId xid pg_attribute_unused(),
						 SCN write_scn pg_attribute_unused())
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

#endif /* USE_PGRAC_CLUSTER */
