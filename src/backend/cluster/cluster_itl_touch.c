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

#include "cluster/cluster_itl_touch.h"
#include "miscadmin.h"			/* InterruptHoldoffCount */
#include "utils/memutils.h"

#ifdef USE_PGRAC_CLUSTER

/* ---------- backend-local state ---------- */

#define CLUSTER_ITL_TOUCH_INITIAL_CAPACITY 16

static ClusterItlTouchHandle *touch_list = NULL;
static uint32 touch_count = 0;
static uint32 touch_capacity = 0;

/* ---------- public API ---------- */

void
cluster_itl_touch_register(const ClusterItlTouchHandle *handle)
{
	Assert(handle != NULL);

	/*
	 * Spec-3.4a forbids registration from signal handlers or any
	 * async-unsafe context.  palloc + MemoryContextSwitchTo are not
	 * async-signal-safe.
	 */
	Assert(InterruptHoldoffCount == 0);

	/* Grow or allocate the list as needed. */
	if (touch_list == NULL)
	{
		MemoryContext oldcxt;

		Assert(TopTransactionContext != NULL);
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);
		touch_capacity = CLUSTER_ITL_TOUCH_INITIAL_CAPACITY;
		touch_list = (ClusterItlTouchHandle *)
			palloc(sizeof(ClusterItlTouchHandle) * touch_capacity);
		MemoryContextSwitchTo(oldcxt);
		touch_count = 0;
	}
	else if (touch_count == touch_capacity)
	{
		uint32 new_capacity = touch_capacity * 2;

		touch_list = (ClusterItlTouchHandle *)
			repalloc(touch_list, sizeof(ClusterItlTouchHandle) * new_capacity);
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

#else							/* !USE_PGRAC_CLUSTER */

void
cluster_itl_touch_register(const ClusterItlTouchHandle *handle pg_attribute_unused())
{
}

void
cluster_itl_touch_foreach(ClusterItlTouchCallback cb pg_attribute_unused(),
						  void *arg pg_attribute_unused())
{
}

void
cluster_itl_touch_reset_at_end_xact(void)
{
}

uint32
cluster_itl_touch_count(void)
{
	return 0;
}

#endif							/* USE_PGRAC_CLUSTER */
