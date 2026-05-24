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

#include "access/heapam_xlog.h"	/* xl_heap_itl_delta / _v2 / _block (spec-3.4b D6) */
#include "access/htup_details.h" /* HeapTupleHeaderData for t_itl_slot_idx */
#include "access/xact.h"	/* GetCurrentTransactionNestLevel (spec-3.4a N9) */
#include "storage/bufmgr.h" /* BufferGetPage, MarkBufferDirty (spec-3.4a D2) */
#include "storage/bufpage.h"
#include "cluster/cluster_epoch.h"		 /* cluster_epoch_get_current (spec-3.4b D7) */
#include "cluster/cluster_guc.h" /* cluster_enabled */
#include "cluster/cluster_itl.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_uba.h"		 /* uba_decode / uba_origin_node_id (spec-3.4b D7) */

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

	if (UBA_is_invalid(slot->undo_segment_head))
	{
		/* Legacy spec-3.4a stamp; pre-3.4b ITL slots carry InvalidUba.
		 * Reader falls back to zero triple — spec-3.1/3.4a behavior. */
		ref->origin_node_id = 0;
		ref->undo_segment_id = 0;
		ref->tt_slot_id = 0;
	}
	else
	{
		uint32 seg_id;
		uint32 blk_no;
		uint16 tt_off;
		uint16 row_off;
		NodeId origin;

		if (!uba_decode(slot->undo_segment_head, &seg_id, &blk_no,
						&tt_off, &row_off))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("malformed UBA in ITL slot %u", itl_slot_idx)));
		origin = uba_origin_node_id(slot->undo_segment_head);
		if (origin == InvalidNodeId)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("UBA decode: segment_id %u has no valid owner_instance",
							seg_id)));
		ref->origin_node_id = (uint16) origin;
		ref->undo_segment_id = (uint16) seg_id;
		ref->tt_slot_id = cluster_tt_slot_offset_to_id(tt_off);
	}

	ref->cluster_epoch = (uint32) cluster_epoch_get_current();
	ref->local_xid = slot->xid;
	ref->cached_commit_scn = slot->commit_scn;
	ref->has_cached_status = (slot->flags == ITL_FLAG_COMMITTED && SCN_VALID(slot->commit_scn));
	/* _padding cleared by memset above. */

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
cluster_itl_stamp_active(Buffer buf, uint8 slot_idx, TransactionId xid, SCN write_scn,
						 UBA undo_segment_head)
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
	/* spec-3.4b D5: real UBA from xact-local binding (F11). */
	slot->undo_segment_head = undo_segment_head;
	/* first_change_lsn -- spec-3.4c populates. */

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
	Size delta_size;
	Size consumed;
	uint16 i;

	Assert(page != NULL);
	Assert(itl_block_start != NULL);

	memcpy(&hdr, itl_block_start, offsetof(xl_heap_itl_delta_block, deltas));

	if (hdr.format_version == CLUSTER_ITL_DELTA_FORMAT_V1)
		delta_size = sizeof(xl_heap_itl_delta);
	else if (hdr.format_version == CLUSTER_ITL_DELTA_FORMAT_V2)
		delta_size = sizeof(xl_heap_itl_delta_v2);
	else
		elog(PANIC,
			 "spec-3.4b D6: unknown xl_heap_itl_delta_block.format_version %u",
			 (unsigned) hdr.format_version);

	for (i = 0; i < hdr.ndeltas; i++)
	{
		const char *p = itl_block_start
						+ offsetof(xl_heap_itl_delta_block, deltas)
						+ (Size) i * delta_size;
		ClusterItlSlotData *slot;
		uint16 slot_idx;
		uint16 flags_after;
		TransactionId d_xid;
		SCN d_write_scn;
		SCN d_commit_scn;
		UBA d_uba = InvalidUba_init;

		if (hdr.format_version == CLUSTER_ITL_DELTA_FORMAT_V1)
		{
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
		}
		else
		{
			xl_heap_itl_delta_v2 d;

			memcpy(&d, p, sizeof(d));
			slot_idx = d.slot_idx;
			flags_after = d.flags_after;
			d_xid = d.xid;
			d_write_scn = d.write_scn;
			d_commit_scn = d.commit_scn;
			d_uba = d.undo_segment_head;
		}

		if (flags_after == ITL_FLAG_COMMITTED && !SCN_VALID(d_commit_scn))
			elog(PANIC,
				 "spec-3.4a D9: ITL COMMITTED delta with InvalidScn at heap redo");

		slot = &ClusterPageGetItlSlots(page)[slot_idx];
		slot->xid = d_xid;
		slot->flags = (ClusterItlFlags) flags_after;
		slot->write_scn = d_write_scn;
		slot->commit_scn = d_commit_scn;
		/* spec-3.4b D6: preserve existing UBA when delta carries InvalidUba
		 * (e.g., legacy v1 record OR v2 finish delta that did not re-bind). */
		if (!UBA_is_invalid(d_uba))
			slot->undo_segment_head = d_uba;

		/* L187 patch tuple pointer to the last applied slot_idx.  Same
		 * semantic as spec-3.4a A9. */
		if (htup != NULL)
			htup->t_itl_slot_idx = slot_idx;
	}

	consumed = offsetof(xl_heap_itl_delta_block, deltas)
			   + (Size) hdr.ndeltas * delta_size;
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
	Size delta_size;

	Assert(itl_block_start != NULL);
	memcpy(&hdr, itl_block_start, offsetof(xl_heap_itl_delta_block, deltas));

	if (hdr.format_version == CLUSTER_ITL_DELTA_FORMAT_V1)
		delta_size = sizeof(xl_heap_itl_delta);
	else if (hdr.format_version == CLUSTER_ITL_DELTA_FORMAT_V2)
		delta_size = sizeof(xl_heap_itl_delta_v2);
	else
		elog(PANIC,
			 "spec-3.4b D6: unknown xl_heap_itl_delta_block.format_version %u",
			 (unsigned) hdr.format_version);

	return offsetof(xl_heap_itl_delta_block, deltas)
		   + (Size) hdr.ndeltas * delta_size;
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
	memcpy(&slot_idx,
		   itl_block_start + offsetof(xl_heap_itl_delta_block, deltas),
		   sizeof(uint16));
	return slot_idx;
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
