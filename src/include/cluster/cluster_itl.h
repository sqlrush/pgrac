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
#include "access/htup.h"			 /* HeapTupleHeader forward typedef */
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

/*
 * cluster_itl_find_lock_tt_ref_by_xmax (spec-3.4d D1 / F2 / F6):
 *
 *	Scan the ITL slot array on `page` for a LOCK_ONLY slot whose xid
 *	matches `raw_xmax`, then fill `*ref` from that slot (UBA decode +
 *	origin / segment / slot extraction same as cluster_itl_get_tt_ref).
 *
 *	Used by spec-3.4d remote ACTIVE detection in heap_lock_tuple /
 *	HeapTupleSatisfiesUpdate, where the tuple carries HEAP_XMAX_LOCK_ONLY
 *	infomask + xmax = lock_xid but there is NO tuple-header field
 *	pointing at the lock-only ITL slot (v0.1 t_lock_itl_slot_idx scheme
 *	rejected per F2 — derive instead of store, avoiding MAXALIGN tax).
 *
 *	Selection rules:
 *	  1. slot.flags has ITL_FLAG_LOCK_ONLY set,
 *	  2. slot.xid == raw_xmax (exact match),
 *	  3. UBA valid (not InvalidUba),
 *	  4. if multiple candidates qualify, pick the slot with highest
 *	     wrap (generation counter) — recycled slot conservatism;
 *	     if still ambiguous, return false.  Callers that have already
 *	     identified a remote ref fail closed only after exact TT lookup;
 *	     false here means "no authoritative lock-only ref".
 *
 *	Returns:
 *	  true   matching LOCK_ONLY slot found, `*ref` filled.
 *	  false  no match / PD_HAS_ITL false / ambiguous duplicate.
 *
 *	The page is not modified.
 *
 *	Spec-3.4d F2 lesson L198:  this is the canonical derive-not-store
 *	pattern for lock-only ITL ref discovery.  Adding a t_lock_itl_slot_idx
 *	byte would push SizeofHeapTupleHeader 24->25, and
 *	MAXALIGN(SizeofHeapTupleHeader) makes the real cost +8B/tuple on 8B
 *	align platforms (MinHeapTupleSize jumps from 24 to 32).  Scan over
 *	8 ITL slots is O(8) ~ 50ns; storage cost is zero.
 */
extern bool cluster_itl_find_lock_tt_ref_by_xmax(Page page, TransactionId raw_xmax,
												 ClusterUndoTTSlotRef *ref);

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
 *	  - if no FREE slot exists, recycle the first completed slot
 *	    (COMMITTED / ABORTED / NEEDS_CLEANOUT).  spec-3.4a does not yet
 *	    let production visibility consume the on-page history because
 *	    UBA/TT allocation stays zero until spec-3.4b; recycling completed
 *	    placeholder slots is therefore the minimal hot-page safety valve.
 *	    spec-3.4d: LOCK_ONLY_ACTIVE is also an active owner and MUST NOT
 *	    be recycled by data DML; only LOCK_ONLY_COMMITTED/ABORTED are
 *	    completed and reusable.
 *	  - returns false on OVERFLOW only when INITRANS=8 has no FREE or
 *	    completed slot and no slot owned by top_xid, i.e. all slots are
 *	    ACTIVE for other transactions.  Caller raises ereport(ERROR,
 *	    ERRCODE_PROGRAM_LIMIT_EXCEEDED) BEFORE entering the critical
 *	    section (PG critical sections forbid ERROR).
 *
 *	NOTE: This function does NOT write the page.  Stamping happens via
 *	cluster_itl_stamp_active() inside the critical section.
 *
 *	Subxact: spec-3.5 owns per-subxact touch ranges, so callers may invoke
 *	this helper from savepoint scopes as long as they use
 *	cluster_itl_touch_register() for xact-end finalization.
 */
extern bool cluster_itl_alloc_or_reuse_slot(Buffer buf, TransactionId top_xid, uint8 *out_slot_idx);

/*
 * cluster_itl_alloc_or_reuse_lock_slot -- lock-only allocator for
 * heap_lock_tuple (spec-3.4d F9).
 *
 * This is intentionally separate from cluster_itl_alloc_or_reuse_slot().
 * A data ACTIVE slot and a LOCK_ONLY_ACTIVE slot carry different tuple
 * semantics and must not overwrite each other even if they belong to the
 * same top_xid.  Behaviour:
 *   - reuse existing LOCK_ONLY_ACTIVE slot for `top_xid` on the page;
 *   - otherwise take the first FREE slot;
 *   - otherwise recycle a completed slot (data committed/aborted/needs
 *     cleanout or lock-only committed/aborted);
 *   - return false when every slot is actively owned by another
 *     transaction or by an incompatible active data/lock category.
 *
 * The caller stamps ITL_FLAG_LOCK_ONLY_ACTIVE inside the heap AM critical
 * section.  This function does not modify page bytes.
 */
extern bool cluster_itl_alloc_or_reuse_lock_slot(Buffer buf, TransactionId top_xid,
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
 *	  slot->undo_segment_head = undo_segment_head (spec-3.4b D5: real UBA
 *	                            from uba_encode(seg, 0, slot_off, 0),
 *	                            keyed to the xact-local TT binding;
 *	                            InvalidUba when binding is unavailable)
 *	  slot->commit_scn   = InvalidScn
 *	  slot->write_scn    = write_scn (cluster_scn_advance())
 *	  slot->first_change_lsn = InvalidXLogRecPtr (spec-3.4c populates)
 *
 *	Marks the buffer dirty (caller still emits the WAL record).
 *
 *	spec-3.4b D5: `undo_segment_head` MUST be the same UBA bytes for
 *	every stamp_active call within an xact (F11).  Heap callers pull
 *	the binding from cluster_tt_local_get_or_create_binding() outside
 *	the critical section and pass the encoded UBA in here.
 */
extern void cluster_itl_stamp_active(Buffer buf, uint8 slot_idx, TransactionId xid, SCN write_scn,
									 UBA undo_segment_head);

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
extern void cluster_itl_stamp_committed(Buffer buf, uint8 slot_idx, SCN commit_scn);

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

/*
 * cluster_itl_check_subxact_or_error -- legacy spec-3.4a N9 caller fence.
 *
 *	spec-3.5 adds SUBTRANS-aware touch range ownership, so the former hard
 *	savepoint fence is now an ABI-compatible no-op.  Keep the symbol to
 *	avoid breaking older static tests / direct helper callers.
 *
 *	Always no-op.
 */
extern void cluster_itl_check_subxact_or_error(void);


/*
 * cluster_itl_redo_apply_block_local_delta
 *
 *	spec-3.4b D6 / F9: redo helper that replays a single block-local ITL
 *	delta array onto `page`, dispatching by format_version:
 *
 *	  format_version == CLUSTER_ITL_DELTA_FORMAT_V1 (spec-3.4a, 24B deltas):
 *	      restore xid/flags/write_scn/commit_scn; leave the slot's
 *	      undo_segment_head unchanged (legacy stamps carry InvalidUba on
 *	      the page; reader 3-branch (D7) falls back to zero triple).
 *	  format_version == CLUSTER_ITL_DELTA_FORMAT_V2 (spec-3.4b, 40B deltas):
 *	      additionally restore undo_segment_head from delta.  When the
 *	      delta carries InvalidUba (e.g., COMMITTED/ABORTED finish stamps
 *	      that do not re-bind UBA), the slot's existing UBA is preserved.
 *	  Any other value -> PANIC.
 *
 *	`itl_block_start` MUST point to the start of an
 *	xl_heap_itl_delta_block (header + deltas) inside the WAL record's
 *	MAIN data area.  `htup` may be NULL when the caller has no tuple
 *	header to patch with the slot index; otherwise its t_itl_slot_idx
 *	is set to the last delta's slot_idx (spec-3.4a A9 L187).
 *
 *	Returns the total number of bytes consumed in the WAL record.
 */
extern Size cluster_itl_redo_apply_block_local_delta(Page page, HeapTupleHeader htup,
													 const char *itl_block_start);

/*
 * cluster_itl_wal_block_consumed_bytes
 *
 *	spec-3.4b D6: utility to compute the WAL size of an ITL delta block
 *	without applying it.  Dispatches by format_version (v1 24B / v2 40B).
 *	Used by heap_xlog_update cross-page redo to skip the NEW block's
 *	delta array before locating the OLD block's array.
 */
extern Size cluster_itl_wal_block_consumed_bytes(const char *itl_block_start);

/*
 * cluster_itl_wal_block_first_slot_idx
 *
 *	spec-3.4b D6: utility to read the first delta's slot_idx without
 *	full apply.  Both v1 and v2 layouts put slot_idx at offset 0 of the
 *	delta entry, so this is a format-agnostic uint16 read.  Used by
 *	heap_xlog_{insert,multi_insert,update} to patch the freshly-built
 *	tuple header's t_itl_slot_idx BEFORE PageAddItem (spec-3.4a A9 L187).
 */
extern uint16 cluster_itl_wal_block_first_slot_idx(const char *itl_block_start);


/* ---------- spec-3.4d D11 — 5 lock-path counters (per-backend) ---------- */

extern void cluster_itl_bump_overflow_lock_count(void);
extern void cluster_itl_bump_multixact_lock_reject_count(void);
extern void cluster_itl_bump_remote_row_lock_fail_closed_count(void);
extern void cluster_itl_bump_lock_only_itl_stamp_count(void);
extern void cluster_itl_bump_lock_only_tt_hint_emit_count(void);

extern uint64 cluster_itl_get_overflow_lock_count(void);
extern uint64 cluster_itl_get_multixact_lock_reject_count(void);
extern uint64 cluster_itl_get_remote_row_lock_fail_closed_count(void);
extern uint64 cluster_itl_get_lock_only_itl_stamp_count(void);
extern uint64 cluster_itl_get_lock_only_tt_hint_emit_count(void);

/* spec-3.4e D6 / F2 P0:  register shmem region for cross-backend
 * aggregated fail_closed counter.  Called from cluster_shmem_init at
 * postmaster startup. */
extern void cluster_lock_path_shmem_register(void);


#endif /* CLUSTER_ITL_H */
