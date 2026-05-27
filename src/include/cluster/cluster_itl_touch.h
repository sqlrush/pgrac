/*-------------------------------------------------------------------------
 *
 * cluster_itl_touch.h
 *	  pgrac xact-local touched-ITL-handle list (spec-3.4a D1).
 *
 *	  When heap_insert / heap_update / heap_delete / heap_multi_insert
 *	  allocates or reuses an ITL slot on a heap page (per spec-3.4a D3/
 *	  D4/D5), it registers a ClusterItlTouchHandle into a backend-local
 *	  xact-scoped list.  The xact.c explicit pre-commit/abort hook
 *	  (spec-3.4a D6) iterates that list to stamp each touched ITL slot
 *	  COMMITTED/ABORTED, emit a WAL delta, and release the buffer.
 *
 *	  The handle stores only persistent buffer-locator coordinates
 *	  (RelFileLocator + ForkNumber + BlockNumber + slot index) -- never
 *	  a Page* / Buffer pin.  L177 + the PG buffer manager require that
 *	  the xact finish hook re-`ReadBuffer` the target page, acquire the
 *	  raw EXCLUSIVE content lock, stamp through generic WAL, release the
 *	  content lock, ReleaseBuffer.  It intentionally bypasses LockBuffer()
 *	  so transaction-end ITL finish does not drive Cache Fusion PCM
 *	  acquire/release state.
 *	  Persisting Buffer pins across critical sections / xact end would
 *	  cause use-after-release or pin bloat (spec-3.4a N11).
 *
 *	  The list lives in TopTransactionContext; `cluster_itl_touch_reset_
 *	  at_end_xact` is invoked from the finish hook tail to release the
 *	  list explicitly (PG would free TopTransactionContext at xact end
 *	  anyway, but explicit reset prevents stale state across nested
 *	  recovery / parallel-worker dispatch).
 *
 *	  Subtransactions: spec-3.5 removes the nested-write barrier.  This
 *	  module therefore records a per-subxact touch_count boundary at
 *	  StartSubTransaction.  CommitSubTransaction promotes the range to the
 *	  parent by popping only the boundary; AbortSubTransaction stamps just
 *	  the range [start_count, touch_count) ABORTED and truncates it before
 *	  the subxact abort record is written.  Without this, a later parent
 *	  commit would incorrectly stamp an aborted child ITL slot COMMITTED.
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
 *	  src/include/cluster/cluster_itl_touch.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_ITL_TOUCH_H
#define CLUSTER_ITL_TOUCH_H

#include "c.h"
#include "postgres_ext.h"	/* Oid (used by RelFileLocator) */
#include "access/transam.h" /* TransactionId */
#include "access/xlogdefs.h"
#include "common/relpath.h"	 /* ForkNumber */
#include "storage/block.h"	 /* BlockNumber */
#include "storage/buf.h"	 /* Buffer */
#include "storage/itemptr.h" /* OffsetNumber */
#include "storage/relfilelocator.h"
#include "cluster/cluster_itl_slot.h" /* CLUSTER_ITL_INITRANS_DEFAULT (spec-3.4c D14) */
#include "cluster/cluster_scn.h"	  /* SCN */

/*
 * ClusterItlTouchHandle -- 24-byte fixed handle (HC: layout MUST stay
 * stable for cluster_unit ABI tests).
 *
 *	Field layout (offsets MUST match D11 T2 expectations):
 *	  offset  0, 12B : rloc (db_oid + spc_oid + rel_oid)
 *	  offset 12,  4B : block
 *	  offset 16,  4B : forknum (normally MAIN_FORKNUM)
 *	  offset 20,  2B : slot_idx (0 .. INITRANS-1)
 *	  offset 22,  2B : flags
 */
typedef struct ClusterItlTouchHandle {
	RelFileLocator rloc;   /* offset  0, 12B */
	BlockNumber block;	   /* offset 12,  4B */
	ForkNumber forknum;	   /* offset 16,  4B */
	OffsetNumber slot_idx; /* offset 20,  2B */
	uint16 flags;		   /* offset 22,  2B */
} ClusterItlTouchHandle;

StaticAssertDecl(sizeof(ClusterItlTouchHandle) == 24,
				 "spec-3.4a D1 — ClusterItlTouchHandle must be 24 bytes");
StaticAssertDecl(offsetof(ClusterItlTouchHandle, rloc) == 0, "spec-3.4a D1 — rloc at offset 0");
StaticAssertDecl(offsetof(ClusterItlTouchHandle, block) == 12, "spec-3.4a D1 — block at offset 12");
StaticAssertDecl(offsetof(ClusterItlTouchHandle, forknum) == 16,
				 "spec-3.4a D1 — forknum at offset 16");
StaticAssertDecl(offsetof(ClusterItlTouchHandle, slot_idx) == 20,
				 "spec-3.4a D1 — slot_idx at offset 20");
StaticAssertDecl(offsetof(ClusterItlTouchHandle, flags) == 22, "spec-3.4a D1 — flags at offset 22");

#define CLUSTER_ITL_TOUCH_FLAG_NEEDS_WAL 0x0001

/*
 * cluster_itl_touch_register -- append a handle to the xact-local
 * touched list.  Must be called AFTER the critical section that wrote
 * the ITL slot (no palloc inside critical sections).  Caller stores
 * the handle on the stack and passes by const pointer; this function
 * deep-copies into the list.
 *
 *	This may run while a surrounding utility/catalog path has interrupts
 *	held off; that is still normal backend context, not an async signal
 *	handler.
 *
 *	Subtransactions: spec-3.5 callers may register from a subxact; the
 *	subxact start/commit/abort APIs below maintain the range ownership.
 */
extern void cluster_itl_touch_register(const ClusterItlTouchHandle *handle);

/*
 * spec-3.5 hardening: subxact range ownership for touched ITL slots.
 *
 * StartSubTransaction records the current touch_count as the subxact range
 * start.  CommitSubTransaction promotes the range to the parent by popping
 * the boundary only.  AbortSubTransaction stamps and truncates the current
 * subxact range through cluster_itl_xact_subabort_finish().
 */
extern void cluster_itl_touch_subxact_start(SubTransactionId subid);
extern void cluster_itl_touch_subxact_commit(SubTransactionId subid);

/*
 * cluster_itl_touch_foreach -- iterate registered handles in insertion
 * order, invoking `cb(handle, arg)` for each.  Used by xact.c pre-
 * commit/abort hook (spec-3.4a D6).
 */
typedef void (*ClusterItlTouchCallback)(const ClusterItlTouchHandle *handle, void *arg);

extern void cluster_itl_touch_foreach(ClusterItlTouchCallback cb, void *arg);

/*
 * ClusterItlPagedHandle -- per-page aggregate of touched ITL slot indices
 * (spec-3.4c D14 / A4 yellow perf hardening).
 *
 *	Represents every touched ITL slot on a single (rloc, forknum, block)
 *	page; slot_indices[0 .. nslots-1] is sorted ascending and contains
 *	no duplicates.  flags is the OR of all aggregated handles' flags
 *	(currently just CLUSTER_ITL_TOUCH_FLAG_NEEDS_WAL).
 *
 *	The xact-end finish hook iterates pages instead of individual
 *	handles so each (rloc, forknum, block) page incurs ONE
 *	ReadBufferWithoutRelcache + ONE LWLockAcquire + ONE
 *	GenericXLogStartLogged/Finish, regardless of how many slots on
 *	that page were touched.  Replaces the spec-3.4a per-slot loop
 *	that opened/locked/WAL-logged once per slot.
 */
typedef struct ClusterItlPagedHandle {
	RelFileLocator rloc;
	ForkNumber forknum;
	BlockNumber block;
	uint8 slot_indices[CLUSTER_ITL_INITRANS_DEFAULT];
	uint8 nslots;
	uint8 flags;
} ClusterItlPagedHandle;

typedef void (*ClusterItlTouchPagedCallback)(const ClusterItlPagedHandle *page_handle, void *arg);

/*
 * cluster_itl_touch_foreach_per_page (spec-3.4c D14 / A4):
 *
 *	Iterate the registered touched-handle list grouped by
 *	(rloc, forknum, block).  Internally:
 *	  1. qsort handles by (rloc, forknum, block, slot)
 *	  2. dedupe consecutive entries with identical key
 *	  3. aggregate by (rloc, forknum, block) into ClusterItlPagedHandle
 *	  4. invoke `cb(page_handle, arg)` once per unique page
 *
 *	Caller (xact.c pre-commit/abort hook) opens / locks / WAL-emits
 *	exactly once per page rather than once per touched slot.
 *
 *	Performance target: spec-3.4a yellow 34.9% -> <=15% on pgbench
 *	enable/disable baseline.
 */
extern void cluster_itl_touch_foreach_per_page(ClusterItlTouchPagedCallback cb, void *arg);

/*
 * cluster_itl_touch_reset_at_end_xact -- release list memory.  Called
 * from the finish hook tail.  Idempotent (safe to call when no handles
 * were registered).
 */
extern void cluster_itl_touch_reset_at_end_xact(void);

/*
 * cluster_itl_touch_count -- snapshot the current list length (debug /
 * pg_cluster_state row in spec-3.4b+).  Cheap O(1) read.
 */
extern uint32 cluster_itl_touch_count(void);

/*
 * Cheap xact-end fast-path predicate.  Allows xact.c to skip the
 * heavier finish entry points entirely when this transaction never
 * touched an ITL slot.
 */
extern bool cluster_itl_touch_has_pending(void);

/*
 * cluster_itl_xact_precommit_finish / cluster_itl_xact_abort_finish --
 * spec-3.4a D6 xact.c hook entry points.
 *
 *	NOT a RegisterXactCallback (N10/N12).  Called explicitly from
 *	xact.c BEFORE the durable commit/abort XLOG record is written.
 *	The hook iterates the xact-local touched list (D1), re-ReadBuffer
 *	each handle, acquires the raw EXCLUSIVE content lock, stamps the ITL
 *	slot COMMITTED/ABORTED through PG generic WAL delta logging (or the
 *	same generic critical-section path without WAL for unlogged relations).
 *	Finally calls
 *	cluster_itl_touch_reset_at_end_xact().
 *
 *	No-op when cluster_enabled is false or the touched list is empty.
 */
extern void cluster_itl_xact_precommit_finish(TransactionId xid, SCN commit_scn);
extern void cluster_itl_xact_abort_finish(TransactionId xid);
extern void cluster_itl_xact_subabort_finish(TransactionId xid, SubTransactionId subid);

#endif /* CLUSTER_ITL_TOUCH_H */
