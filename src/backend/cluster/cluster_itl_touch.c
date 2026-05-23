/*-------------------------------------------------------------------------
 *
 * cluster_itl_touch.c
 *	  pgrac xact-local touched-ITL-handle list (spec-3.4a D1).
 *
 *	  Backend-local, xact-scoped, palloc'd in TopTransactionContext.
 *	  Lifecycle:
 *	    - DML path (spec-3.4a D3/D4/D5) appends a handle via
 *	      cluster_itl_touch_register() after the critical section.
 *	    - xact.c pre-commit/abort hook (spec-3.4a D6) calls
 *	      cluster_itl_touch_foreach() to stamp each touched ITL slot.
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
 *	    spec-3.4a fails closed at the DML callsite when
 *	    GetCurrentTransactionNestLevel() > 1, so this module never
 *	    sees subxact-scoped registers; no nested-list bookkeeping.
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

#include "access/xloginsert.h"	 /* log_newpage_buffer (spec-3.4a D8) */
#include "cluster/cluster_guc.h" /* cluster_enabled */
#include "cluster/cluster_itl.h" /* stamp_committed / stamp_aborted */
#include "cluster/cluster_itl_touch.h"
#include "miscadmin.h"		/* InterruptHoldoffCount */
#include "storage/bufmgr.h" /* ReadBufferWithoutRelcache / LockBuffer */
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

	/*
	 * Spec-3.4a forbids registration from signal handlers or any
	 * async-unsafe context.  palloc + MemoryContextSwitchTo are not
	 * async-signal-safe.
	 */
	Assert(InterruptHoldoffCount == 0);

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

/* ---------- spec-3.4a D6 — xact.c pre-commit/abort hook ---------- */

/*
 * Helper: re-read the buffer indicated by `handle`, acquire EXCLUSIVE
 * content lock, return the Buffer to the caller.  Caller is responsible
 * for stamping + ReleaseBuffer.  Uses ReadBufferWithoutRelcache to
 * avoid relcache lookup overhead and to keep the hook lightweight even
 * during shutdown sequences where relcache may be torn down.
 */
static Buffer
itl_touch_acquire_buffer(const ClusterItlTouchHandle *handle)
{
	Buffer buf;

	buf = ReadBufferWithoutRelcache(handle->rloc, handle->forknum, handle->block, RBM_NORMAL, NULL,
									true /* permanent */);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	return buf;
}

typedef struct ItlFinishCtx {
	SCN commit_scn; /* InvalidScn for abort path */
	bool is_commit;
} ItlFinishCtx;

static void
itl_finish_one(const ClusterItlTouchHandle *handle, void *arg)
{
	ItlFinishCtx *ctx = (ItlFinishCtx *)arg;
	Buffer buf;

	buf = itl_touch_acquire_buffer(handle);

	START_CRIT_SECTION();
	if (ctx->is_commit)
		cluster_itl_stamp_committed(buf, (uint8)handle->slot_idx, ctx->commit_scn);
	else
		cluster_itl_stamp_aborted(buf, (uint8)handle->slot_idx);

	/*
	 * spec-3.4a D8 (Step 7): the ACTIVE -> COMMITTED / ACTIVE -> ABORTED
	 * transition mutates the page's special area; we must produce a WAL
	 * record so crash recovery sees the post-transition state.  Since
	 * RM_HEAP and RM_HEAP2 op-code spaces are already saturated and a
	 * dedicated RM_CLUSTER_ITL rmgr is out of scope for spec-3.4a, we
	 * fall back to log_newpage_buffer() which emits a full-page image
	 * (FPI) of the dirty buffer.  Heavy (8KB per touched ITL slot) but
	 * trivially correct.  spec-3.4c is expected to graduate to a
	 * compact ITL-only WAL record reusing xl_heap_itl_delta_block.
	 */
	log_newpage_buffer(buf, false /* page_std */);
	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);
}

void
cluster_itl_xact_precommit_finish(TransactionId xid, SCN commit_scn)
{
	ItlFinishCtx ctx;

	(void)xid; /* xid currently unused; reserved for WAL emit */

	if (!cluster_enabled || cluster_node_id < 0) {
		cluster_itl_touch_reset_at_end_xact();
		return;
	}
	if (touch_count == 0)
		return;

	Assert(SCN_VALID(commit_scn)); /* L181 — COMMITTED must carry valid SCN */

	ctx.commit_scn = commit_scn;
	ctx.is_commit = true;
	cluster_itl_touch_foreach(itl_finish_one, &ctx);
	cluster_itl_touch_reset_at_end_xact();
}

void
cluster_itl_xact_abort_finish(TransactionId xid)
{
	ItlFinishCtx ctx;

	(void)xid;

	if (!cluster_enabled || cluster_node_id < 0) {
		cluster_itl_touch_reset_at_end_xact();
		return;
	}
	if (touch_count == 0)
		return;

	ctx.commit_scn = InvalidScn;
	ctx.is_commit = false;
	cluster_itl_touch_foreach(itl_finish_one, &ctx);
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
cluster_itl_touch_reset_at_end_xact(void)
{}

uint32
cluster_itl_touch_count(void)
{
	return 0;
}

void
cluster_itl_xact_precommit_finish(TransactionId xid pg_attribute_unused(),
								  SCN commit_scn pg_attribute_unused())
{}

void
cluster_itl_xact_abort_finish(TransactionId xid pg_attribute_unused())
{}

#endif /* USE_PGRAC_CLUSTER */
