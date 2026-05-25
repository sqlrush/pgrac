/*-------------------------------------------------------------------------
 *
 * cluster_itl_touch.c
 *	  pgrac xact-local touched-ITL-handle list (spec-3.4a D1).
 *
 *	  Backend-local, xact-scoped, palloc'd in TopTransactionContext.
 *	  Lifecycle:
 *	    - DML path (spec-3.4a D3/D4/D5) appends a handle via
 *	      cluster_itl_touch_register() after the critical section.
 *	    - xact.c pre-commit/abort hook (spec-3.4a D6) calls the
 *	      per-page aggregate iterator to stamp touched ITL slots.
 *	    - The hook tail calls cluster_itl_touch_reset_at_end_xact()
 *	      to release the list.
 *
 *	  Storage:
 *	    Dynamic palloc'd array; grows by doubling when capacity
 *	    exhausted.  Initial capacity 16 handles (small transactions
 *	    rarely touch more); capped by spec-3.4a R5 (single xact 100K+
 *	    DML is extreme and relies on PG OOM tolerance).
 *
 *	  Handle stability:
 *	    Handles store buffer-locator coordinates only -- no Page* or
 *	    Buffer pin (spec-3.4a N11).  The xact finish hook must
 *	    re-ReadBuffer the target page.
 *
 *	  Subxact:
 *	    spec-3.4a leaves subtransaction writes on the PG-native path at
 *	    the DML callsite, so this module never sees subxact-scoped
 *	    registers; no nested-list bookkeeping.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.4a-itl-write-path-activation-minimal-wal.md (v1.0 FROZEN 2026-05-23)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_itl_touch.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h" /* GenericXLog delta WAL (spec-3.4a D8) */
#include "cluster/cluster_guc.h" /* cluster_enabled */
#include "cluster/cluster_itl.h" /* stamp_committed / stamp_aborted */
#include "cluster/cluster_itl_touch.h"
#include "storage/bufmgr.h" /* ReadBufferWithoutRelcache / ReleaseBuffer */
#include "storage/buf_internals.h"
#include "utils/memutils.h"

#ifdef USE_PGRAC_CLUSTER

/* ---------- backend-local state ---------- */

#define CLUSTER_ITL_TOUCH_INITIAL_CAPACITY 16

static ClusterItlTouchHandle *touch_list = NULL;
static uint32 touch_count = 0;
static uint32 touch_capacity = 0;

static bool
itl_touch_handle_matches(const ClusterItlTouchHandle *left, const ClusterItlTouchHandle *right)
{
	return RelFileLocatorEquals(left->rloc, right->rloc) && left->block == right->block
		   && left->forknum == right->forknum && left->slot_idx == right->slot_idx;
}

/* ---------- public API ---------- */

void
cluster_itl_touch_register(const ClusterItlTouchHandle *handle)
{
	uint32 i;

	Assert(handle != NULL);

	for (i = 0; i < touch_count; i++) {
		if (itl_touch_handle_matches(&touch_list[i], handle))
			return;
	}

	/* Grow or allocate the list as needed. */
	if (touch_list == NULL) {
		MemoryContext oldcxt;

		Assert(TopTransactionContext != NULL);
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);
		touch_capacity = CLUSTER_ITL_TOUCH_INITIAL_CAPACITY;
		touch_list
			= (ClusterItlTouchHandle *)palloc(sizeof(ClusterItlTouchHandle) * touch_capacity);
		MemoryContextSwitchTo(oldcxt);
		touch_count = 0;
	} else if (touch_count == touch_capacity) {
		uint32 new_capacity = touch_capacity * 2;

		touch_list = (ClusterItlTouchHandle *)repalloc(touch_list, sizeof(ClusterItlTouchHandle)
																	   * new_capacity);
		touch_capacity = new_capacity;
	}

	touch_list[touch_count] = *handle;
	touch_count++;
}

void
cluster_itl_touch_foreach(ClusterItlTouchCallback cb, void *arg)
{
	uint32 i;

	Assert(cb != NULL);

	for (i = 0; i < touch_count; i++)
		cb(&touch_list[i], arg);
}

/* ---------- spec-3.4c D14 — per-page aggregate iteration ---------- */

/*
 * Compare two touch handles by (rloc, forknum, block, slot_idx).  Returns
 * <0 / 0 / >0 in qsort convention.  The dedupe + aggregate pipeline relies
 * on this ordering: all handles for the same page are consecutive, with
 * slot_idx ascending within that run.
 */
static int
itl_touch_handle_cmp(const void *a, const void *b)
{
	const ClusterItlTouchHandle *l = (const ClusterItlTouchHandle *)a;
	const ClusterItlTouchHandle *r = (const ClusterItlTouchHandle *)b;

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
	return 0;
}

void
cluster_itl_touch_foreach_per_page(ClusterItlTouchPagedCallback cb, void *arg)
{
	uint32 i;
	ClusterItlPagedHandle ph;

	Assert(cb != NULL);

	if (touch_count == 0)
		return;

	/*
	 * cluster_itl_touch_register already dedupes by (rloc, block, forknum,
	 * slot_idx) on append, so the list contains no duplicates -- but the
	 * insertion order is interleaved across pages.  Sort by page key so
	 * we can stream-aggregate consecutive same-page runs into a single
	 * ClusterItlPagedHandle.
	 */
	qsort(touch_list, touch_count, sizeof(ClusterItlTouchHandle), itl_touch_handle_cmp);

	memset(&ph, 0, sizeof(ph));
	ph.rloc = touch_list[0].rloc;
	ph.forknum = touch_list[0].forknum;
	ph.block = touch_list[0].block;
	ph.nslots = 0;
	ph.flags = 0;

	for (i = 0; i < touch_count; i++) {
		const ClusterItlTouchHandle *h = &touch_list[i];
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
		 * Register dedupes exact handles today, but keep the aggregate
		 * stage independently robust: callers/tests may eventually feed a
		 * pre-sorted duplicate, and the public header promises this helper
		 * dedupes consecutive identical page+slot keys.  Preserve flags
		 * from both entries before skipping the duplicate.
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

/*
 * Helper: re-read the buffer indicated by `handle`, acquire the raw
 * EXCLUSIVE content lock, return the Buffer to the caller.  Caller is
 * responsible for stamping + direct content-lock release + ReleaseBuffer.
 * Uses ReadBufferWithoutRelcache to avoid relcache lookup overhead and to
 * keep the hook lightweight even during shutdown sequences where relcache
 * may be torn down.
 *
 * Do not call LockBuffer() here.  The bufmgr LockBuffer wrapper drives the
 * Cache Fusion PCM state machine for user-visible content locks; transaction-
 * end ITL finish is a local page-metadata stamp and must not acquire/release
 * cache ownership.
 */
static Buffer
itl_touch_acquire_buffer(const ClusterItlTouchHandle *handle)
{
	Buffer buf;
	BufferDesc *buf_desc;

	buf = ReadBufferWithoutRelcache(handle->rloc, handle->forknum, handle->block, RBM_NORMAL, NULL,
									true /* permanent */);
	buf_desc = GetBufferDescriptor(buf - 1);
	LWLockAcquire(BufferDescriptorGetContentLock(buf_desc), LW_EXCLUSIVE);
	return buf;
}

static void
itl_touch_release_buffer(Buffer buf)
{
	BufferDesc *buf_desc;

	buf_desc = GetBufferDescriptor(buf - 1);
	LWLockRelease(BufferDescriptorGetContentLock(buf_desc));
	ReleaseBuffer(buf);
}

typedef struct ItlFinishCtx {
	SCN commit_scn; /* InvalidScn for abort path */
	bool is_commit;
} ItlFinishCtx;

static void
itl_finish_stamp_page(Page page, uint8 slot_idx, const ItlFinishCtx *ctx)
{
	ClusterItlSlotData *slot;

	slot = &ClusterPageGetItlSlots(page)[slot_idx];
	Assert(slot->flags == ITL_FLAG_ACTIVE);

	if (ctx->is_commit) {
		slot->flags = ITL_FLAG_COMMITTED;
		slot->commit_scn = ctx->commit_scn;
	} else {
		slot->flags = ITL_FLAG_ABORTED;
		slot->commit_scn = InvalidScn;
	}
}

/*
 * spec-3.4c D14 / A4 yellow perf hardening — per-page aggregate finish.
 *
 *	One open + lock + WAL emit per (rloc, forknum, block) page, stamping
 *	every touched slot on that page in a single critical section.
 *	Replaces spec-3.4a itl_finish_one (per-slot) which paid the open/lock/
 *	WAL cost per touched ITL slot.  Same crash-safety semantics: the page
 *	delta lands in WAL exactly once, with all slot mutations atomic from
 *	the recovery POV.
 */
typedef struct ItlFinishBatchCtx {
	ItlFinishCtx finish;
	ClusterItlPagedHandle pages[MAX_GENERIC_XLOG_PAGES];
	uint8 npages;
	bool needs_wal;
} ItlFinishBatchCtx;

static inline bool
itl_paged_handle_needs_wal(const ClusterItlPagedHandle *page_handle)
{
	return (page_handle->flags & CLUSTER_ITL_TOUCH_FLAG_NEEDS_WAL) != 0;
}

/*
 * Flush one batch of sorted page handles as a single generic WAL record.
 *
 * Generic WAL can cover multiple buffers in one record.  spec-3.4c D14
 * originally reduced the old per-slot finish path to one open/lock/WAL per
 * page; this hardening step reduces the remaining per-page XLogInsert cost by
 * grouping consecutive pages with the same WAL requirement.
 */
static void
itl_finish_flush_batch(ItlFinishBatchCtx *bctx)
{
	GenericXLogState *state;
	Buffer bufs[MAX_GENERIC_XLOG_PAGES];
	uint8 nbufs = 0;
	uint8 p;

	if (bctx->npages == 0)
		return;

	state = GenericXLogStartLogged(bctx->needs_wal);

	for (p = 0; p < bctx->npages; p++) {
		const ClusterItlPagedHandle *page_handle = &bctx->pages[p];
		ClusterItlTouchHandle key_handle;
		Page image;
		uint8 i;

		memset(&key_handle, 0, sizeof(key_handle));
		key_handle.rloc = page_handle->rloc;
		key_handle.forknum = page_handle->forknum;
		key_handle.block = page_handle->block;
		key_handle.slot_idx = 0;
		key_handle.flags = page_handle->flags;

		bufs[nbufs] = itl_touch_acquire_buffer(&key_handle);
		image = GenericXLogRegisterBuffer(state, bufs[nbufs], 0);
		nbufs++;

		for (i = 0; i < page_handle->nslots; i++)
			itl_finish_stamp_page(image, page_handle->slot_indices[i], &bctx->finish);
	}

	GenericXLogFinish(state);

	for (p = 0; p < nbufs; p++)
		itl_touch_release_buffer(bufs[p]);

	bctx->npages = 0;
}

static void
itl_finish_page_batched(const ClusterItlPagedHandle *page_handle, void *arg)
{
	ItlFinishBatchCtx *bctx = (ItlFinishBatchCtx *)arg;
	bool needs_wal = itl_paged_handle_needs_wal(page_handle);

	if (bctx->npages > 0
		&& (bctx->npages == MAX_GENERIC_XLOG_PAGES || bctx->needs_wal != needs_wal))
		itl_finish_flush_batch(bctx);

	if (bctx->npages == 0)
		bctx->needs_wal = needs_wal;

	bctx->pages[bctx->npages++] = *page_handle;
}

void
cluster_itl_xact_precommit_finish(TransactionId xid, SCN commit_scn)
{
	ItlFinishBatchCtx bctx;

	(void)xid; /* xid currently unused; reserved for WAL emit */

	if (!cluster_enabled || cluster_node_id < 0) {
		cluster_itl_touch_reset_at_end_xact();
		return;
	}
	if (touch_count == 0)
		return;

	Assert(SCN_VALID(commit_scn)); /* L181 — COMMITTED must carry valid SCN */

	memset(&bctx, 0, sizeof(bctx));
	bctx.finish.commit_scn = commit_scn;
	bctx.finish.is_commit = true;
	cluster_itl_touch_foreach_per_page(itl_finish_page_batched, &bctx);
	itl_finish_flush_batch(&bctx);
	cluster_itl_touch_reset_at_end_xact();
}

void
cluster_itl_xact_abort_finish(TransactionId xid)
{
	ItlFinishBatchCtx bctx;

	(void)xid;

	if (!cluster_enabled || cluster_node_id < 0) {
		cluster_itl_touch_reset_at_end_xact();
		return;
	}
	if (touch_count == 0)
		return;

	memset(&bctx, 0, sizeof(bctx));
	bctx.finish.commit_scn = InvalidScn;
	bctx.finish.is_commit = false;
	cluster_itl_touch_foreach_per_page(itl_finish_page_batched, &bctx);
	itl_finish_flush_batch(&bctx);
	cluster_itl_touch_reset_at_end_xact();
}

#else /* !USE_PGRAC_CLUSTER */

void
cluster_itl_touch_register(const ClusterItlTouchHandle *handle pg_attribute_unused())
{}

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

#endif /* USE_PGRAC_CLUSTER */
