/*-------------------------------------------------------------------------
 *
 * cluster_itl_touch.c
 *	  pgrac xact-local touched-ITL-record list (spec-3.4a D1).
 *
 *	  Backend-local, xact-scoped, palloc'd in TopTransactionContext.
 *	  Lifecycle:
 *	    - DML path (spec-3.4a D3/D4/D5) appends a record via
 *	      cluster_itl_touch_register_exact() after the critical section,
 *	      capturing the terminal-stamp authority proof while the content
 *	      lock and local PCM X round are still exact.
 *	    - xact.c pre-commit/abort hook (spec-3.4a D6) stamps touched ITL
 *	      slots through the no-fetch exact-proof acquire.
 *	    - The hook tail calls cluster_itl_touch_reset_at_end_xact()
 *	      to release the list.
 *
 *	  Storage:
 *	    Dynamic palloc'd array; grows by doubling when capacity
 *	    exhausted.  Initial capacity 16 records (small transactions
 *	    rarely touch more); capped by spec-3.4a R5 (single xact 100K+
 *	    DML is extreme and relies on PG OOM tolerance).
 *
 *	  Record stability:
 *	    Records store buffer-locator coordinates plus the captured
 *	    terminal-stamp proof -- no Page* or Buffer pin (spec-3.4a N11).
 *	    The xact finish hook revalidates the proof under the no-fetch
 *	    acquire; a mismatch is a safe typed skip and the slot's terminal
 *	    state stays with the TT/CLOG/undo authority.
 *
 *	  Subxact:
 *	    spec-3.5 removes the prior GetCurrentTransactionNestLevel() gate.
 *	    We track a stack of touch_count boundaries so aborting a child
 *	    subtransaction stamps only its own ITL slots ABORTED and truncates
 *	    them.  A subcommit only pops the boundary, promoting its touched
 *	    slots to the parent range.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.4a-itl-write-path-activation-minimal-wal.md (v1.0 FROZEN 2026-05-23)
 * Spec: spec-8.3-active-itl-current-block-transfer-semantics.md
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_itl_touch.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h" /* GenericXLog delta WAL (spec-3.4a D8) */
#include "access/xlog.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_gcs_block.h" /* exact-proof stamp acquire */
#include "cluster/cluster_guc.h"	   /* cluster_enabled */
#include "cluster/cluster_itl.h"	   /* stamp_committed / stamp_aborted */
#include "cluster/cluster_itl_touch.h"
#include "cluster/cluster_mode.h"		 /* cluster_storage_mode_enabled */
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_xnode_lever.h" /* stamp-skip counter */
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "utils/memutils.h"

#ifdef USE_PGRAC_CLUSTER

/* ---------- backend-local state ---------- */

#define CLUSTER_ITL_TOUCH_INITIAL_CAPACITY 16

static ClusterItlTouchRecord *touch_list = NULL;
static uint32 touch_count = 0;
static uint32 touch_capacity = 0;

typedef struct ClusterItlTouchSubxactBoundary {
	SubTransactionId subid;
	uint32 start_count;
} ClusterItlTouchSubxactBoundary;

static ClusterItlTouchSubxactBoundary *subxact_stack = NULL;
static uint32 subxact_depth = 0;
static uint32 subxact_capacity = 0;

static bool
itl_touch_handle_matches(const ClusterItlTouchHandle *left, const ClusterItlTouchHandle *right)
{
	return RelFileLocatorEquals(left->rloc, right->rloc) && left->block == right->block
		   && left->forknum == right->forknum && left->slot_idx == right->slot_idx;
}

/*
 * Append one record, growing the list as needed.  The caller has already
 * applied its dedupe policy.
 */
static void
itl_touch_append(const ClusterItlTouchHandle *handle,
				 const ClusterItlTerminalProof *proof,
				 const ClusterCtrcReceiptHandle *ctrc_handle)
{
	if (touch_list == NULL) {
		MemoryContext oldcxt;

		Assert(TopTransactionContext != NULL);
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);
		touch_capacity = CLUSTER_ITL_TOUCH_INITIAL_CAPACITY;
		touch_list
			= (ClusterItlTouchRecord *)palloc(sizeof(ClusterItlTouchRecord) * touch_capacity);
		MemoryContextSwitchTo(oldcxt);
		touch_count = 0;
	} else if (touch_count == touch_capacity) {
		uint32 new_capacity = touch_capacity * 2;

		touch_list = (ClusterItlTouchRecord *)repalloc(touch_list, sizeof(ClusterItlTouchRecord)
																	   * new_capacity);
		touch_capacity = new_capacity;
	}

	touch_list[touch_count].key = *handle;
	touch_list[touch_count].proof = *proof;
	MemSet(&touch_list[touch_count].ctrc_handle, 0,
		   sizeof(touch_list[touch_count].ctrc_handle));
	if (ctrc_handle != NULL && ctrc_handle->valid)
		touch_list[touch_count].ctrc_handle = *ctrc_handle;
	touch_count++;
}

/* ---------- public API ---------- */

void
cluster_itl_touch_register(const ClusterItlTouchHandle *handle)
{
	ClusterItlTerminalProof proof;
	uint32 i;

	Assert(handle != NULL);

	/*
	 * Legacy whole-list dedupe by (rloc, fork, block, slot): every record
	 * registered through this entry point carries no proof, so two entries
	 * for the same slot are indistinguishable and the historical semantic
	 * (first registration wins, later flags are not merged) is preserved
	 * for non-production callers.
	 */
	for (i = 0; i < touch_count; i++) {
		if (itl_touch_handle_matches(&touch_list[i].key, handle))
			return;
	}

	memset(&proof, 0, sizeof(proof));
	itl_touch_append(handle, &proof, NULL);
}

/*
 * Capture the terminal-stamp authority proof for one just-written ITL slot.
 *
 *	Runs while the caller still holds the buffer's content lock EXCLUSIVE
 *	and the same local PCM X round as the write (all production heap
 *	registration sites).  Reads the slot's xid, wrap and active class from
 *	the page under that lock, then projects ownership generation and
 *	acquisition epoch from the exact ACTIVE-holder ledger.  Any failure
 *	leaves proof.valid=false; the terminal stamp then skips this record
 *	safely (TT/CLOG/undo resolves the slot).
 */
static void
itl_touch_capture_proof(const ClusterItlTouchHandle *handle, Buffer buffer, TransactionId xid,
						ClusterItlTerminalProof *proof)
{
	Page page;
	const ClusterItlSlotData *slot;
	BufferTag expected_tag;
	uint64 own_generation = 0;
	uint64 acquisition_epoch = 0;
	uint8 pcm_state = (uint8)PCM_STATE_N;

	memset(proof, 0, sizeof(*proof));

	if (!BufferIsValid(buffer) || BufferIsLocal(buffer) || !TransactionIdIsValid(xid))
		return;

	page = BufferGetPage(buffer);
	if (!PageHasItl(page) || PageGetSpecialSize(page) < CLUSTER_ITL_ARRAY_SIZE
		|| handle->slot_idx >= CLUSTER_ITL_INITRANS_DEFAULT)
		return;

	slot = &ClusterPageGetItlSlots(page)[handle->slot_idx];
	if (slot->xid != xid
		|| (slot->flags != ITL_FLAG_ACTIVE && slot->flags != ITL_FLAG_LOCK_ONLY_ACTIVE))
		return;

	/*
	 * The slot incarnation is confirmed: record its identity even when the
	 * authority projection below fails, so the caller's dedupe can find and
	 * invalidate an existing record for the same (xid, wrap) incarnation.
	 * `valid` stays false on that path.
	 */
	proof->xid = xid;
	proof->buffer_id = buffer - 1;
	proof->undo_segment_head = slot->undo_segment_head;
	proof->slot_wrap = slot->wrap;
	proof->slot_class = slot->flags;

	InitBufferTag(&expected_tag, &handle->rloc, handle->forknum, handle->block);
	if (!cluster_bufmgr_terminal_stamp_authority(buffer, &expected_tag, &own_generation,
											 &acquisition_epoch, &pcm_state))
		return;

	proof->own_generation = own_generation;
	proof->acquisition_epoch = acquisition_epoch;
	proof->pcm_state = pcm_state;
	proof->valid = true;
}

static void
itl_touch_register_exact_internal(const ClusterItlTouchHandle *handle,
								  Buffer buffer, TransactionId xid,
								  const ClusterCtrcReceiptHandle *ctrc_handle)
{
	ClusterItlTerminalProof proof;
	uint32 range_start = 0;
	uint32 i;

	Assert(handle != NULL);
	Assert(handle->slot_idx < CLUSTER_ITL_INITRANS_DEFAULT);

	itl_touch_capture_proof(handle, buffer, xid, &proof);

	/*
	 * Dedupe by (rloc, fork, block, slot, xid, slot_wrap) within the
	 * current subtransaction owner range only: an entry before the range
	 * start belongs to an ancestor and must never be updated, or a nested
	 * subabort would stamp (or lose) an ancestor-owned slot.
	 */
	if (subxact_depth > 0)
		range_start = subxact_stack[subxact_depth - 1].start_count;

	for (i = range_start; i < touch_count; i++) {
		ClusterItlTouchRecord *existing = &touch_list[i];

		if (!itl_touch_handle_matches(&existing->key, handle))
			continue;
		if (existing->proof.xid != proof.xid || existing->proof.slot_wrap != proof.slot_wrap)
			continue; /* different incarnation: distinct entry */

		/*
		 * Same exact slot incarnation: OR only NEEDS_WAL and replace the
		 * authority proof with the newest capture.  If the newest capture
		 * failed, invalidate the record -- retaining an older valid
		 * generation would let the stamp trust a round that has already
		 * moved on.
		 */
		existing->key.flags |= (handle->flags & CLUSTER_ITL_TOUCH_FLAG_NEEDS_WAL);
		MemSet(&existing->ctrc_handle, 0, sizeof(existing->ctrc_handle));
		if (proof.valid)
		{
			existing->proof = proof;
			if (ctrc_handle != NULL && ctrc_handle->valid)
				existing->ctrc_handle = *ctrc_handle;
		}
		else
			existing->proof.valid = false;
		return;
	}

	/*
	 * A failed capture never appends: the authority proof is obtained
	 * first, and only then is a record appended.  The slot stays safe
	 * without an entry -- the terminal stamp is a hint and TT/CLOG/undo
	 * resolves it.
	 */
	if (!proof.valid)
		return;

	itl_touch_append(handle, &proof, ctrc_handle);
}

void
cluster_itl_touch_register_exact(const ClusterItlTouchHandle *handle,
								 Buffer buffer, TransactionId xid)
{
	itl_touch_register_exact_internal(handle, buffer, xid, NULL);
}

void
cluster_itl_touch_register_exact_ctrc(
	const ClusterItlTouchHandle *handle, Buffer buffer, TransactionId xid,
	const ClusterCtrcReceiptHandle *ctrc_handle)
{
	/* Every caller reaches this point only after the protected page/WAL
	 * mutation has completed.  Record the causal ordering while the exact
	 * receipt handle is still backend-owned. */
	if (ctrc_handle != NULL)
		cluster_ctrc_note_publication_after_apply(ctrc_handle, false);
	itl_touch_register_exact_internal(handle, buffer, xid, ctrc_handle);
}

bool
cluster_itl_touch_lookup_reusable_ctrc(
	const ClusterItlTouchHandle *handle, Buffer buffer, TransactionId xid,
	ClusterCtrcReceiptHandle *ctrc_handle_out)
{
	ClusterItlTerminalProof current;
	uint32 range_start = 0;
	uint32 i;

	if (ctrc_handle_out != NULL)
		MemSet(ctrc_handle_out, 0, sizeof(*ctrc_handle_out));
	if (handle == NULL || ctrc_handle_out == NULL
		|| handle->slot_idx >= CLUSTER_ITL_INITRANS_DEFAULT)
		return false;
	itl_touch_capture_proof(handle, buffer, xid, &current);
	if (!current.valid)
		return false;
	if (subxact_depth > 0)
		range_start = subxact_stack[subxact_depth - 1].start_count;

	for (i = touch_count; i > range_start; i--)
	{
		const ClusterItlTouchRecord *existing = &touch_list[i - 1];

		if (!itl_touch_handle_matches(&existing->key, handle)
			|| !existing->ctrc_handle.valid
			|| existing->proof.buffer_id != current.buffer_id
			|| !cluster_itl_terminal_proof_owner_exact(
				&existing->proof, current.own_generation,
				current.acquisition_epoch, current.pcm_state, true, 0, 0)
			|| !cluster_itl_terminal_proof_slot_exact(
				&existing->proof, current.xid, current.slot_wrap,
				current.slot_class, &current.undo_segment_head))
			continue;
		*ctrc_handle_out = existing->ctrc_handle;
		return true;
	}
	return false;
}

void
cluster_itl_touch_foreach(ClusterItlTouchCallback cb, void *arg)
{
	uint32 i;

	Assert(cb != NULL);

	for (i = 0; i < touch_count; i++)
		cb(&touch_list[i].key, arg);
}

void
cluster_itl_touch_subxact_start(SubTransactionId subid)
{
	MemoryContext oldcxt;

	/* P0 (2026-05-31): subxact touch tracking is a STORAGE path (feeds local
	 * ITL finish/cleanout); gate on storage mode, not cluster_conf_has_peers(). */
	if (!cluster_storage_mode_enabled())
		return;

	if (subxact_stack == NULL) {
		Assert(TopTransactionContext != NULL);
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);
		subxact_capacity = 8;
		subxact_stack = (ClusterItlTouchSubxactBoundary *)palloc(
			sizeof(ClusterItlTouchSubxactBoundary) * subxact_capacity);
		MemoryContextSwitchTo(oldcxt);
	} else if (subxact_depth == subxact_capacity) {
		subxact_capacity *= 2;
		subxact_stack = (ClusterItlTouchSubxactBoundary *)repalloc(
			subxact_stack, sizeof(ClusterItlTouchSubxactBoundary) * subxact_capacity);
	}

	subxact_stack[subxact_depth].subid = subid;
	subxact_stack[subxact_depth].start_count = touch_count;
	subxact_depth++;
}

static bool
itl_touch_pop_subxact(SubTransactionId subid, uint32 *start_count_out)
{
	if (start_count_out != NULL)
		*start_count_out = touch_count;

	if (subxact_depth == 0)
		return false;

	/*
	 * PG subxacts close in strict LIFO order.  If an error path ever
	 * reaches us out-of-order, fail soft: find the matching boundary and
	 * drop everything above it as part of the same cleanup rather than
	 * leaving stale child ranges to be committed by the parent.
	 */
	if (subxact_stack[subxact_depth - 1].subid != subid) {
		int i;

		for (i = (int)subxact_depth - 2; i >= 0; i--) {
			if (subxact_stack[i].subid == subid) {
				if (start_count_out != NULL)
					*start_count_out = subxact_stack[i].start_count;
				subxact_depth = (uint32)i;
				return true;
			}
		}
		return false;
	}

	subxact_depth--;
	if (start_count_out != NULL)
		*start_count_out = subxact_stack[subxact_depth].start_count;
	return true;
}

void
cluster_itl_touch_subxact_commit(SubTransactionId subid)
{
	uint32 ignored;

	/*
	 * Promote child touches to the parent by popping only the boundary.
	 * The records remain in touch_list and will be finalized by the parent
	 * commit/abort or an ancestor subabort.
	 */
	(void)itl_touch_pop_subxact(subid, &ignored);
}

/* ---------- spec-3.4c D14 — per-page aggregate iteration ---------- */

/*
 * Compare two touch records by (rloc, forknum, block, slot_idx), then by
 * (xid, wrap) for determinism.  The dedupe + aggregate pipeline relies on
 * this ordering: all records for the same page are consecutive, with
 * slot_idx ascending within that run.
 */
static int
itl_touch_record_cmp(const void *a, const void *b)
{
	const ClusterItlTouchRecord *lr = (const ClusterItlTouchRecord *)a;
	const ClusterItlTouchRecord *rr = (const ClusterItlTouchRecord *)b;
	const ClusterItlTouchHandle *l = &lr->key;
	const ClusterItlTouchHandle *r = &rr->key;

	if (l->rloc.dbOid != r->rloc.dbOid)
		return (l->rloc.dbOid < r->rloc.dbOid) ? -1 : 1;
	if (l->rloc.spcOid != r->rloc.spcOid)
		return (l->rloc.spcOid < r->rloc.spcOid) ? -1 : 1;
	if (l->rloc.relNumber != r->rloc.relNumber)
		return (l->rloc.relNumber < r->rloc.relNumber) ? -1 : 1;
	if (l->forknum != r->forknum)
		return (l->forknum < r->forknum) ? -1 : 1;
	if (l->block != r->block)
		return (l->block < r->block) ? -1 : 1;
	if (l->slot_idx != r->slot_idx)
		return (l->slot_idx < r->slot_idx) ? -1 : 1;
	if (lr->proof.xid != rr->proof.xid)
		return (lr->proof.xid < rr->proof.xid) ? -1 : 1;
	if (lr->proof.slot_wrap != rr->proof.slot_wrap)
		return (lr->proof.slot_wrap < rr->proof.slot_wrap) ? -1 : 1;
	return 0;
}

static void
itl_touch_range_per_page(uint32 start, uint32 end, ClusterItlTouchPagedCallback cb, void *arg)
{
	uint32 i;
	ClusterItlPagedHandle ph;
	ClusterItlTouchRecord *base;
	uint32 count;

	Assert(cb != NULL);
	Assert(start <= end);
	Assert(end <= touch_count);

	count = end - start;
	if (count == 0)
		return;

	base = &touch_list[start];
	qsort(base, count, sizeof(ClusterItlTouchRecord), itl_touch_record_cmp);

	memset(&ph, 0, sizeof(ph));
	ph.rloc = base[0].key.rloc;
	ph.forknum = base[0].key.forknum;
	ph.block = base[0].key.block;
	ph.nslots = 0;
	ph.flags = 0;

	for (i = 0; i < count; i++) {
		const ClusterItlTouchHandle *h = &base[i].key;
		bool same_page = (i > 0) && RelFileLocatorEquals(h->rloc, ph.rloc)
						 && h->forknum == ph.forknum && h->block == ph.block;

		if (!same_page && i > 0) {
			cb(&ph, arg);
			memset(&ph, 0, sizeof(ph));
			ph.rloc = h->rloc;
			ph.forknum = h->forknum;
			ph.block = h->block;
			ph.nslots = 0;
			ph.flags = 0;
		}

		/*
		 * Consecutive same page+slot entries (either exact duplicates fed
		 * by a test caller or distinct incarnations of a reused slot)
		 * aggregate into one slot index; the public per-page contract is
		 * page+slot based and only NEEDS_WAL is ORed.
		 */
		if (same_page && ph.nslots > 0 && ph.slot_indices[ph.nslots - 1] == h->slot_idx) {
			ph.flags |= (uint8)h->flags;
			continue;
		}

		/*
		 * The bound check defends against future bumps to
		 * CLUSTER_ITL_INITRANS_DEFAULT that bypass register's guard.
		 */
		Assert(ph.nslots < CLUSTER_ITL_INITRANS_DEFAULT);
		Assert(h->slot_idx < CLUSTER_ITL_INITRANS_DEFAULT);
		ph.slot_indices[ph.nslots++] = (uint8)h->slot_idx;
		ph.flags |= (uint8)h->flags;
	}

	cb(&ph, arg);
}

void
cluster_itl_touch_foreach_per_page(ClusterItlTouchPagedCallback cb, void *arg)
{
	itl_touch_range_per_page(0, touch_count, cb, arg);
}

void
cluster_itl_touch_reset_at_end_xact(void)
{
	/*
	 * TopTransactionContext destruction frees the palloc'd array
	 * automatically; we just reset the static pointer/state so the
	 * next xact starts fresh.  Explicit pfree is unnecessary and
	 * would double-free when PG tears the context down.
	 */
	touch_list = NULL;
	touch_count = 0;
	touch_capacity = 0;
	subxact_stack = NULL;
	subxact_depth = 0;
	subxact_capacity = 0;
}

uint32
cluster_itl_touch_count(void)
{
	return touch_count;
}

bool
cluster_itl_touch_has_pending(void)
{
	return touch_count != 0;
}

/* ---------- spec-3.4a D6 — xact.c pre-commit/abort hook ---------- */

typedef struct ItlFinishCtx {
	SCN commit_scn; /* InvalidScn for abort path */
	bool is_commit;
} ItlFinishCtx;

static void
itl_finish_stamp_page(Page page, uint8 slot_idx, const ItlFinishCtx *ctx)
{
	ClusterItlSlotData *slot;
	bool is_lock_only;

	slot = &ClusterPageGetItlSlots(page)[slot_idx];

	/*
	 * spec-3.4d D4:  touch list now contains lock-only ITL slots (LOCK_ONLY_
	 * ACTIVE) alongside the spec-3.4a data ITL slots (ACTIVE).  Both
	 * states transition at xact-end finish.  A data commit is still before
	 * TransactionIdCommitTree(), so retain its SCN as NEEDS_CLEANOUT rather
	 * than publishing a reusable terminal page state.  The exact C1b-backed
	 * lazy cleanout/census later promotes it to COMMITTED.  Distinguish the
	 * lock-only category via ITL_FLAG_IS_LOCK_ONLY().
	 */
	is_lock_only = ITL_FLAG_IS_LOCK_ONLY(slot->flags);

	Assert(slot->flags == ITL_FLAG_ACTIVE || slot->flags == ITL_FLAG_LOCK_ONLY_ACTIVE);

	if (ctx->is_commit) {
		/* Precommit is not terminal proof. Keep the exact lock locator and
		 * its shared receipt for the admitted terminal census/cleaner. */
		if (is_lock_only)
			return;
		slot->flags = ITL_FLAG_NEEDS_CLEANOUT;
		slot->commit_scn = ctx->commit_scn;
	} else {
		slot->flags = is_lock_only ? ITL_FLAG_LOCK_ONLY_ABORTED : ITL_FLAG_ABORTED;
		slot->commit_scn = InvalidScn;
		if (is_lock_only)
			(void)cluster_itl_clear_terminal_lock_refs(page, slot);
	}
}

/*
 * One consecutive same-page run of sorted touch records awaiting the
 * terminal stamp.
 */
typedef struct ItlFinishPageRun {
	uint32 first; /* absolute index into touch_list */
	uint32 count;
	bool needs_wal;
} ItlFinishPageRun;

typedef struct ItlFinishBatchCtx {
	ItlFinishCtx finish;
	ItlFinishPageRun runs[MAX_GENERIC_XLOG_PAGES];
	uint8 nruns;
	bool needs_wal;
} ItlFinishBatchCtx;

typedef struct ItlFinishDischarge {
	uint32 record_index;
	uint8 dependency_index;
} ItlFinishDischarge;

#define CLUSTER_ITL_FINISH_MAX_DISCHARGES \
	(MAX_GENERIC_XLOG_PAGES * CLUSTER_ITL_INITRANS_DEFAULT * 2)

static void
itl_finish_expected_target(const ClusterItlTouchRecord *record,
						   ClusterCtrcItlTargetIdentity *expected)
{
	MemSet(expected, 0, sizeof(*expected));
	expected->spc_oid = record->key.rloc.spcOid;
	expected->db_oid = record->key.rloc.dbOid;
	expected->rel_number = record->key.rloc.relNumber;
	expected->fork_number = record->key.forknum;
	expected->block_number = record->key.block;
	expected->itl_xid = record->proof.xid;
	expected->itl_slot_index = record->key.slot_idx;
	expected->itl_slot_wrap = record->proof.slot_wrap;
	expected->itl_class
		= record->proof.slot_class == ITL_FLAG_LOCK_ONLY_ACTIVE ? 2 : 1;
	expected->needs_wal
		= (record->key.flags & CLUSTER_ITL_TOUCH_FLAG_NEEDS_WAL) != 0;
	memcpy(expected->uba, &record->proof.undo_segment_head,
		   sizeof(record->proof.undo_segment_head));
}

/*
 * Flush one batch of same-WAL-requirement page runs as a single generic
 * WAL record.
 *
 *	Every page is acquired through the no-fetch exact-proof helper: the
 *	newest record whose captured proof still matches the live buffer's
 *	ownership tuple takes the page, then every record on the run is
 *	rechecked against that authority tuple and the live slot before its
 *	slot may be stamped.  A record that fails any comparison is a typed
 *	safe skip -- TT/CLOG/undo remains the terminal authority.  A page
 *	whose records all skip is not registered with GenericXLog (no empty
 *	WAL record).
 */
static void
itl_finish_flush_batch(ItlFinishBatchCtx *bctx)
{
	GenericXLogState *state;
	Buffer bufs[MAX_GENERIC_XLOG_PAGES];
	ClusterSfDepVec dependency_vecs[MAX_GENERIC_XLOG_PAGES];
	ItlFinishDischarge discharges[CLUSTER_ITL_FINISH_MAX_DISCHARGES];
	XLogRecPtr batch_lsn = InvalidXLogRecPtr;
	uint32 ndischarges = 0;
	uint8 nbufs = 0;
	uint8 p;

	if (bctx->nruns == 0)
		return;

	state = GenericXLogStartLogged(bctx->needs_wal);

	for (p = 0; p < bctx->nruns; p++) {
		const ItlFinishPageRun *run = &bctx->runs[p];
		const ClusterItlTerminalProof *used = NULL;
		ClusterItlStampSkipReason reason;
		Buffer buf = InvalidBuffer;
		Page live_page;
		Page image;
		bool stampable[CLUSTER_ITL_INITRANS_DEFAULT * 2];
		uint32 stampable_count = 0;
		uint32 r;

		/*
		 * Acquire the page under the newest record whose proof still
		 * matches the live exact X round.  Records were sorted with
		 * ascending (xid, wrap) last, so walk backwards; every proof that
		 * can acquire the page describes the same live ownership tuple.
		 */
		for (r = run->count; r > 0; r--) {
			const ClusterItlTouchRecord *record = &touch_list[run->first + r - 1];

			buf = cluster_bufmgr_lock_resident_for_exact_itl_stamp(record, &reason);
			if (BufferIsValid(buf)) {
				used = &record->proof;
				break;
			}
		}

		if (!BufferIsValid(buf)) {
			/* No proof can acquire this page: one typed skip per record. */
			for (r = 0; r < run->count; r++)
				cluster_lever_g_note_stamp_skipped();
			continue;
		}

		/*
		 * Recheck every record against the acquiring authority tuple and
		 * the live slot before mutation.  Only the proof matching the live
		 * exact X generation may stamp; a reused slot, a different
		 * incarnation or an ancestor round skips.
		 */
		live_page = BufferGetPage(buf);
		for (r = 0; r < run->count && r < lengthof(stampable); r++) {
			const ClusterItlTouchRecord *record = &touch_list[run->first + r];
			const ClusterItlSlotData *slot;
			bool ok;

			ok = record->proof.valid && record->proof.buffer_id == used->buffer_id
				 && record->proof.own_generation == used->own_generation
				 && record->proof.acquisition_epoch == used->acquisition_epoch
				 && record->key.slot_idx < CLUSTER_ITL_INITRANS_DEFAULT;
			if (ok) {
				slot = &ClusterPageGetItlSlots(live_page)[record->key.slot_idx];
				ok = slot->xid == record->proof.xid && slot->wrap == record->proof.slot_wrap
					 && slot->flags == record->proof.slot_class
					 && slot->undo_segment_head.raw[0]
						== record->proof.undo_segment_head.raw[0]
					 && slot->undo_segment_head.raw[1]
						== record->proof.undo_segment_head.raw[1];
			}
			stampable[r] = ok;
			if (ok)
				stampable_count++;
			else
				cluster_lever_g_note_stamp_skipped();
		}

		/*
		 * A run longer than the recheck window (an extreme pile-up of slot
		 * incarnations on one page) skips the overflow safely: the slot
		 * stays ACTIVE and TT/CLOG/undo resolves it, same as any other
		 * typed skip.
		 */
		for (r = lengthof(stampable); r < run->count; r++)
			cluster_lever_g_note_stamp_skipped();

		if (stampable_count == 0) {
			/* Nothing to mutate: do not register an empty page delta. */
			cluster_bufmgr_unlock_resident_stamp(buf);
			continue;
		}

		cluster_sf_dep_vec_reset(&dependency_vecs[p]);
		(void)cluster_sf_dep_vec_for_ship(buf, &dependency_vecs[p]);
		image = GenericXLogRegisterBuffer(state, buf, 0);
		for (r = 0; r < run->count && r < lengthof(stampable); r++) {
			const ClusterItlTouchRecord *record = &touch_list[run->first + r];

			if (stampable[r]) {
				itl_finish_stamp_page(image, (uint8)record->key.slot_idx, &bctx->finish);
				/* Every precommit receipt remains APPLIED. Only an abort is
				 * terminal independently of later status publication, and may
				 * discharge once this page WAL and its dependencies are durable. */
				if (record->ctrc_handle.valid && !bctx->finish.is_commit
					&& ndischarges < lengthof(discharges)) {
					discharges[ndischarges].record_index = run->first + r;
					discharges[ndischarges].dependency_index = p;
					ndischarges++;
				}
			}
		}

		bufs[nbufs] = buf;
		nbufs++;
	}

	/* Every page in this batch skipped -> nothing to log. */
	if (nbufs == 0)
		GenericXLogAbort(state);
	else
		batch_lsn = GenericXLogFinish(state);

	for (p = 0; p < nbufs; p++)
		cluster_bufmgr_unlock_resident_stamp(bufs[p]);

	/* Never wait for WAL or sample remote durability while holding a heap
	 * content lock.  A missing local WAL position simply retains every
	 * logged receipt; no state is weakened to manufacture cleanup. */
	if (ndischarges > 0
		&& (!bctx->needs_wal || !XLogRecPtrIsInvalid(batch_lsn))) {
		XLogRecPtr local_flush_lsn;
		uint32 d;

		if (bctx->needs_wal)
			XLogFlush(batch_lsn);
		local_flush_lsn = GetFlushRecPtr(NULL);
		for (d = 0; d < ndischarges; d++) {
			const ItlFinishDischarge *pending = &discharges[d];
			const ClusterItlTouchRecord *record
				= &touch_list[pending->record_index];
			ClusterCtrcItlTargetIdentity expected;
			ClusterCtrcDurability durability;
			int origin;

			MemSet(&durability, 0, sizeof(durability));
			durability.highest_local_lsn = batch_lsn;
			durability.local_flush_lsn = local_flush_lsn;
			memcpy(durability.required_lsn,
				   dependency_vecs[pending->dependency_index].required,
				   sizeof(durability.required_lsn));
			for (origin = 0; origin < CLUSTER_SF_DEP_MAX_ORIGINS; origin++)
				durability.durable_lsn[origin]
					= origin == cluster_node_id
					? local_flush_lsn
					: cluster_sf_observed_origin_durable_lsn(origin);
			itl_finish_expected_target(record, &expected);
			(void)cluster_ctrc_receipt_discharge_itl_shared(
				&record->ctrc_handle, &expected,
				CTRC_ITL_TERMINAL_INDEPENDENT, &durability);
		}
	}

	bctx->nruns = 0;
}

/*
 * Stamp the touched records in [start, end) with one terminal outcome.
 *
 *	Sorts the range so same-page records are consecutive, groups them into
 *	page runs and flushes runs in GenericXLog-sized batches of equal WAL
 *	requirement.  Used by top commit, top abort and subabort; the caller
 *	owns the range semantics (spec-3.4a D6 / spec-3.5 subxact ranges).
 */
static void
itl_finish_range(uint32 start, uint32 end, const ItlFinishCtx *finish)
{
	ItlFinishBatchCtx bctx;
	uint32 i;

	Assert(start <= end);
	Assert(end <= touch_count);

	if (start >= end)
		return;

	qsort(&touch_list[start], end - start, sizeof(ClusterItlTouchRecord), itl_touch_record_cmp);

	memset(&bctx, 0, sizeof(bctx));
	bctx.finish = *finish;

	i = start;
	while (i < end) {
		const ClusterItlTouchHandle *page_key = &touch_list[i].key;
		ItlFinishPageRun run;
		uint32 j = i;
		uint8 run_flags = 0;

		while (j < end) {
			const ClusterItlTouchHandle *h = &touch_list[j].key;

			if (!RelFileLocatorEquals(h->rloc, page_key->rloc) || h->forknum != page_key->forknum
				|| h->block != page_key->block)
				break;
			run_flags |= (uint8)h->flags;
			j++;
		}

		run.first = i;
		run.count = j - i;
		run.needs_wal = (run_flags & CLUSTER_ITL_TOUCH_FLAG_NEEDS_WAL) != 0;

		if (bctx.nruns > 0
			&& (bctx.nruns == MAX_GENERIC_XLOG_PAGES || bctx.needs_wal != run.needs_wal))
			itl_finish_flush_batch(&bctx);

		if (bctx.nruns == 0)
			bctx.needs_wal = run.needs_wal;
		bctx.runs[bctx.nruns++] = run;

		i = j;
	}

	itl_finish_flush_batch(&bctx);
}

void
cluster_itl_xact_precommit_finish(TransactionId xid, SCN commit_scn)
{
	ItlFinishCtx finish;

	(void)xid; /* xid currently unused; reserved for WAL emit */

	if (!cluster_enabled || cluster_node_id < 0) {
		cluster_itl_touch_reset_at_end_xact();
		return;
	}
	if (touch_count == 0)
		return;

	Assert(SCN_VALID(commit_scn)); /* L181 — COMMITTED must carry valid SCN */

	finish.commit_scn = commit_scn;
	finish.is_commit = true;
	itl_finish_range(0, touch_count, &finish);
	cluster_itl_touch_reset_at_end_xact();
}

void
cluster_itl_xact_abort_finish(TransactionId xid)
{
	ItlFinishCtx finish;

	(void)xid;

	if (!cluster_enabled || cluster_node_id < 0) {
		cluster_itl_touch_reset_at_end_xact();
		return;
	}
	if (touch_count == 0)
		return;

	finish.commit_scn = InvalidScn;
	finish.is_commit = false;
	itl_finish_range(0, touch_count, &finish);
	cluster_itl_touch_reset_at_end_xact();
}

void
cluster_itl_xact_subabort_finish(TransactionId xid, SubTransactionId subid)
{
	ItlFinishCtx finish;
	uint32 start_count;
	uint32 end_count;

	(void)xid;

	if (!cluster_enabled || cluster_node_id < 0)
		return;
	if (touch_count == 0) {
		(void)itl_touch_pop_subxact(subid, &start_count);
		return;
	}
	if (!itl_touch_pop_subxact(subid, &start_count))
		return;

	end_count = touch_count;
	if (start_count >= end_count) {
		touch_count = start_count;
		return;
	}

	finish.commit_scn = InvalidScn;
	finish.is_commit = false;
	itl_finish_range(start_count, end_count, &finish);

	/*
	 * Remove aborted child records so a later parent commit cannot stamp
	 * them COMMITTED.  This is the spec-3.5 SUBTRANS invariant that was
	 * absent while spec-3.4a kept subxacts on PG-native path.
	 */
	touch_count = start_count;
}

#else /* !USE_PGRAC_CLUSTER */

void
cluster_itl_touch_register(const ClusterItlTouchHandle *handle pg_attribute_unused())
{}

void
cluster_itl_touch_register_exact(const ClusterItlTouchHandle *handle pg_attribute_unused(),
								 Buffer buffer pg_attribute_unused(),
								 TransactionId xid pg_attribute_unused())
{}

void
cluster_itl_touch_register_exact_ctrc(
	const ClusterItlTouchHandle *handle pg_attribute_unused(),
	Buffer buffer pg_attribute_unused(),
	TransactionId xid pg_attribute_unused(),
	const ClusterCtrcReceiptHandle *ctrc_handle pg_attribute_unused())
{}

bool
cluster_itl_touch_lookup_reusable_ctrc(
	const ClusterItlTouchHandle *handle pg_attribute_unused(),
	Buffer buffer pg_attribute_unused(),
	TransactionId xid pg_attribute_unused(),
	ClusterCtrcReceiptHandle *ctrc_handle_out)
{
	if (ctrc_handle_out != NULL)
		MemSet(ctrc_handle_out, 0, sizeof(*ctrc_handle_out));
	return false;
}

void
cluster_itl_touch_foreach(ClusterItlTouchCallback cb pg_attribute_unused(),
						  void *arg pg_attribute_unused())
{}

void
cluster_itl_touch_foreach_per_page(ClusterItlTouchPagedCallback cb pg_attribute_unused(),
								   void *arg pg_attribute_unused())
{}

void
cluster_itl_touch_reset_at_end_xact(void)
{}

void
cluster_itl_touch_subxact_start(SubTransactionId subid pg_attribute_unused())
{}

void
cluster_itl_touch_subxact_commit(SubTransactionId subid pg_attribute_unused())
{}

uint32
cluster_itl_touch_count(void)
{
	return 0;
}

bool
cluster_itl_touch_has_pending(void)
{
	return false;
}

void
cluster_itl_xact_precommit_finish(TransactionId xid pg_attribute_unused(),
								  SCN commit_scn pg_attribute_unused())
{}

void
cluster_itl_xact_abort_finish(TransactionId xid pg_attribute_unused())
{}

void
cluster_itl_xact_subabort_finish(TransactionId xid pg_attribute_unused(),
								 SubTransactionId subid pg_attribute_unused())
{}

#endif /* USE_PGRAC_CLUSTER */
