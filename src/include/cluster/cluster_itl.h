/*-------------------------------------------------------------------------
 *
 * cluster_itl.h
 *	  pgrac cluster ITL slot read-only helpers — extract TT slot ref
 *	  from a heap page's ITL array.
 *
 *	  spec-3.1 D4 (NEW).
 *
 *	  Backend-only header (depends on storage/bufpage.h Page type;
 *	  kept out of frontend-safe cluster_tt_slot.h per spec-3.1 §1.4).
 *
 *	  Read-only contract:
 *	    - cluster_itl_get_tt_ref MUST NOT mutate the page.
 *	    - PD_HAS_ITL=false or invalid slot index → returns false, ref
 *	      untouched.
 *	    - If ITL slot has cached commit_scn from a prior cleanout, the
 *	      ref carries it as `cached_commit_scn` (read-only;  D4 does NOT
 *	      write back).
 *
 *	  spec-3.1 v1.0 FROZEN scope (D4):
 *	    - read-only ITL reader;  NO commit_scn persistent write (推
 *	      spec-3.4 delayed cleanout);  NO TT slot allocation (推
 *	      spec-3.4);  NO HeapTupleSatisfiesMVCC cluster-path activation
 *	      (推 spec-3.2).
 *	    - origin_node / undo_segment_id / tt_slot_id are NOT yet stored
 *	      in the ITL slot on-disk format (spec-1.5 placeholder layout);
 *	      D4 fills them from the helper's caller-side knowledge and the
 *	      provisional in-memory mint (spec-3.1 v0.4 N6).  Real persistent
 *	      origin/segment/slot landing requires spec-3.4 ITL slot writable
 *	      activation.
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
 *	  src/include/cluster/cluster_itl.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_ITL_H
#define CLUSTER_ITL_H

#include "c.h"
#include "access/transam.h"			 /* TransactionId */
#include "storage/buf.h"			 /* Buffer */
#include "storage/bufpage.h"		 /* Page typedef */
#include "cluster/cluster_scn.h"	 /* SCN */
#include "cluster/cluster_tt_slot.h" /* ClusterUndoTTSlotRef */

/*
 * cluster_itl_get_tt_ref -- read ITL slot at `itl_slot_idx` and fill a
 * ClusterUndoTTSlotRef descriptor.
 *
 * Returns:
 *	  true   ITL slot is valid and `*ref` is filled.
 *	  false  PD_HAS_ITL is not set / itl_slot_idx is out of range /
 *	         slot is empty (ITL_FLAG_FREE).  `*ref` untouched.
 *
 * The page is not modified.
 *
 * Caveat (spec-3.1 v1.0):  origin_node_id / undo_segment_id /
 * tt_slot_id fields in the on-disk ITL slot are spec-1.5 placeholders
 * (zero).  D4 reports what is physically present; visibility consumers
 * (spec-3.2) decide how to combine with caller-side origin knowledge
 * when constructing a ClusterTTStatusKey for cluster_tt_status_lookup_exact.
 */
extern bool cluster_itl_get_tt_ref(Page page, uint8 itl_slot_idx, ClusterUndoTTSlotRef *ref);

/* ---------- spec-3.4a D2 — writer API ---------- */

/*
 * cluster_itl_alloc_or_reuse_slot -- find or reserve an ITL slot for
 * `top_xid` on the page backing `buf`.
 *
 *	Called from heap_insert / heap_update / heap_delete /
 *	heap_multi_insert BEFORE entering START_CRIT_SECTION. The caller
 *	must already hold the target buffer with EXCLUSIVE content lock
 *	(or its equivalent write-protection per the heap AM callsite
 *	contract).
 *
 *	Behaviour:
 *	  - one ITL slot per (page, top_xid):  if the page already has an
 *	    ACTIVE slot for `top_xid`, reuse it (returns existing slot_idx).
 *	  - otherwise, scan the ITL slot array for the first FREE slot and
 *	    return its index (caller will stamp ACTIVE inside the critical
 *	    section).
 *	  - returns false on OVERFLOW (INITRANS=8 full and no slot owned by
 *	    top_xid).  Caller raises ereport(ERROR, ERRCODE_PROGRAM_LIMIT_
 *	    EXCEEDED) BEFORE entering the critical section (PG critical
 *	    sections forbid ERROR).
 *
 *	NOTE: This function does NOT write the page.  Stamping happens via
 *	cluster_itl_stamp_active() inside the critical section.
 *
 *	Subxact: spec-3.4a fails closed at the callsite when
 *	GetCurrentTransactionNestLevel() > 1; callers must check that
 *	before invoking this helper.
 */
extern bool cluster_itl_alloc_or_reuse_slot(Buffer buf, TransactionId top_xid,
											uint8 *out_slot_idx);

/*
 * cluster_itl_stamp_active -- write ACTIVE state into ITL slot
 * `slot_idx` on the page backing `buf`.
 *
 *	Must be called inside START_CRIT_SECTION (the heap AM critical
 *	section that also writes the tuple).  Sets:
 *	  slot->xid          = xid
 *	  slot->wrap         <unchanged>
 *	  slot->flags        = ITL_FLAG_ACTIVE
 *	  slot->lock_count   <unchanged>
 *	  slot->undo_segment_head = InvalidUba (spec-3.4b populates)
 *	  slot->commit_scn   = InvalidScn
 *	  slot->write_scn    = write_scn (cluster_scn_advance())
 *	  slot->first_change_lsn = InvalidXLogRecPtr (spec-3.4b/c populates)
 *
 *	Marks the buffer dirty (caller still emits the WAL record).
 */
extern void cluster_itl_stamp_active(Buffer buf, uint8 slot_idx,
									 TransactionId xid, SCN write_scn);

/*
 * cluster_itl_stamp_committed -- transition ACTIVE → COMMITTED with
 * the supplied `commit_scn` (must be valid; L181).
 *
 *	Called from the xact.c pre-commit hook (spec-3.4a D6) inside
 *	START_CRIT_SECTION.  Sets:
 *	  slot->flags     = ITL_FLAG_COMMITTED
 *	  slot->commit_scn = commit_scn
 *
 *	Caller emits the COMMITTED WAL delta (spec-3.4a D8) within the
 *	same critical section.
 */
extern void cluster_itl_stamp_committed(Buffer buf, uint8 slot_idx,
										SCN commit_scn);

/*
 * cluster_itl_stamp_aborted -- transition ACTIVE → ABORTED.
 *
 *	Called from the xact.c abort hook (spec-3.4a D6) inside
 *	START_CRIT_SECTION.  Sets:
 *	  slot->flags     = ITL_FLAG_ABORTED
 *	  slot->commit_scn = InvalidScn
 *
 *	Caller emits the ABORTED WAL delta within the same critical section.
 */
extern void cluster_itl_stamp_aborted(Buffer buf, uint8 slot_idx);

#endif /* CLUSTER_ITL_H */
