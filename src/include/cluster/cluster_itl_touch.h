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
 *	  the xact finish hook re-`ReadBuffer` the target page, acquire an
 *	  EXCLUSIVE content lock, stamp, MarkBufferDirty, ReleaseBuffer.
 *	  Persisting Buffer pins across critical sections / xact end would
 *	  cause use-after-release or pin bloat (spec-3.4a N11).
 *
 *	  The list lives in TopTransactionContext; `cluster_itl_touch_reset_
 *	  at_end_xact` is invoked from the finish hook tail to release the
 *	  list explicitly (PG would free TopTransactionContext at xact end
 *	  anyway, but explicit reset prevents stale state across nested
 *	  recovery / parallel-worker dispatch).
 *
 *	  Subtransactions: spec-3.4a fails closed (ERRCODE_FEATURE_NOT_
 *	  SUPPORTED) at the DML callsite if `GetCurrentTransactionNestLevel()
 *	  > 1`; this header therefore makes no provisions for subxact-scoped
 *	  list nesting.  Full SUBTRANS support is deferred to spec-3.5.
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
#include "cluster/cluster_scn.h" /* SCN */

/*
 * ClusterItlTouchHandle -- 24-byte fixed handle (HC: layout MUST stay
 * stable for cluster_unit ABI tests).
 *
 *	Field layout (offsets MUST match D11 T2 expectations):
 *	  offset  0, 12B : rloc (db_oid + spc_oid + rel_oid)
 *	  offset 12,  4B : block
 *	  offset 16,  4B : forknum (normally MAIN_FORKNUM)
 *	  offset 20,  2B : slot_idx (0 .. INITRANS-1)
 *	  offset 22,  2B : flags (reserved; zero in v1.0)
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
 *	Subtransactions: callers must have already rejected
 *	GetCurrentTransactionNestLevel() > 1 with ERRCODE_FEATURE_NOT_
 *	SUPPORTED (spec-3.4a N9).
 */
extern void cluster_itl_touch_register(const ClusterItlTouchHandle *handle);

/*
 * cluster_itl_touch_foreach -- iterate registered handles in insertion
 * order, invoking `cb(handle, arg)` for each.  Used by xact.c pre-
 * commit/abort hook (spec-3.4a D6).
 */
typedef void (*ClusterItlTouchCallback)(const ClusterItlTouchHandle *handle, void *arg);

extern void cluster_itl_touch_foreach(ClusterItlTouchCallback cb, void *arg);

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
 * cluster_itl_xact_precommit_finish / cluster_itl_xact_abort_finish --
 * spec-3.4a D6 xact.c hook entry points.
 *
 *	NOT a RegisterXactCallback (N10/N12).  Called explicitly from
 *	xact.c BEFORE the durable commit/abort XLOG record is written.
 *	The hook iterates the xact-local touched list (D1), re-ReadBuffer
 *	each handle, acquires EXCLUSIVE content lock, stamps the ITL slot
 *	COMMITTED/ABORTED inside a critical section that also emits the
 *	matching xl_heap_itl_delta_block WAL record (full WAL emit wired
 *	in spec-3.4a D8 / Step 7).  Finally calls
 *	cluster_itl_touch_reset_at_end_xact().
 *
 *	No-op when cluster_enabled is false or the touched list is empty.
 */
extern void cluster_itl_xact_precommit_finish(TransactionId xid, SCN commit_scn);
extern void cluster_itl_xact_abort_finish(TransactionId xid);

#endif /* CLUSTER_ITL_TOUCH_H */
