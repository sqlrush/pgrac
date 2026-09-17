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
#include "cluster/cluster_buffer_desc.h" /* PCM_STATE_N / PCM_STATE_X */
#include "cluster/cluster_itl_slot.h"	 /* CLUSTER_ITL_INITRANS_DEFAULT (spec-3.4c D14) */
#include "cluster/cluster_scn.h"		 /* SCN */
#include "cluster/cluster_terminal_ref_census.h"

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
 * ClusterItlTerminalProof -- backend-local exact-authority capture for the
 * transaction-terminal ITL stamp.
 *
 *	Captured at ITL registration while the writer still holds the exact
 *	content lock and local PCM X round.  The terminal stamp may mutate a
 *	slot only if every field still matches under the content lock; a
 *	mismatch is a safe typed skip (TT/CLOG/undo remains the visibility
 *	authority).  Residency or a same-tag refetch can never substitute for
 *	this proof: both ABA classes (block refetch/re-own and slot reuse)
 *	present the same tag with a different incarnation.
 *
 *	`valid` is explicit because the initial legal cluster epoch is zero;
 *	`acquisition_epoch != 0` is forbidden as a presence test.  `slot_class`
 *	stores the active ClusterItlFlags value (ITL_FLAG_ACTIVE or
 *	ITL_FLAG_LOCK_ONLY_ACTIVE); it routes the terminal flag and is never a
 *	visibility verdict.  Process memory only: no wire, WAL, page, catalog
 *	or shared-memory representation.
 *
 *	Spec: spec-8.3-active-itl-current-block-transfer-semantics.md
 */
typedef struct ClusterItlTerminalProof {
	TransactionId xid;
	int32 buffer_id;
	uint64 own_generation;
	uint64 acquisition_epoch;
	UBA undo_segment_head;
	uint16 slot_wrap;
	uint8 slot_class;
	uint8 pcm_state;
	bool valid;
} ClusterItlTerminalProof;

/*
 * Exact terminal-stamp ownership modes.
 *
 * Multi-node keeps the existing PCM-X contract.  A known single-node
 * storage deployment has no peer PCM lifecycle, so its only admissible
 * projection is the quiescent PCM-N tuple under the already-held page
 * content lock EXCLUSIVE.  Unknown topology, recovery merge, invalid local
 * identity and every busy ownership tuple fail closed.
 */
static inline bool
cluster_itl_terminal_stamp_authority_admissible(bool storage_mode, int local_node_id,
												int node_count, bool recovery_merge_active,
												uint8 pcm_state, uint32 own_flags,
												uint64 writer_activation_token)
{
	if (!storage_mode || node_count <= 0 || local_node_id < 0 || local_node_id >= node_count
		|| recovery_merge_active || own_flags != 0 || writer_activation_token != 0)
		return false;

	if (node_count == 1)
		return local_node_id == 0 && pcm_state == (uint8)PCM_STATE_N;
	return pcm_state == (uint8)PCM_STATE_X;
}

/* Exact local-owner and slot checks used by terminal hinting. */
static inline bool
cluster_itl_terminal_proof_owner_exact(const ClusterItlTerminalProof *proof, uint64 own_generation,
									   uint64 acquisition_epoch, uint8 pcm_state,
									   bool authority_admissible, uint32 own_flags,
									   uint64 writer_activation_token)
{
	return proof != NULL && proof->valid && authority_admissible && pcm_state == proof->pcm_state
		   && own_flags == 0 && writer_activation_token == 0
		   && own_generation == proof->own_generation
		   && acquisition_epoch == proof->acquisition_epoch;
}

static inline bool
cluster_itl_terminal_proof_slot_exact(const ClusterItlTerminalProof *proof, TransactionId xid,
									  uint16 slot_wrap, uint8 slot_class,
									  const UBA *undo_segment_head)
{
	return proof != NULL && proof->valid && xid == proof->xid && slot_wrap == proof->slot_wrap
		   && slot_class == proof->slot_class && undo_segment_head != NULL
		   && undo_segment_head->raw[0] == proof->undo_segment_head.raw[0]
		   && undo_segment_head->raw[1] == proof->undo_segment_head.raw[1];
}

/*
 * ClusterItlTouchRecord -- private touched-list element: the frozen public
 * 24-byte handle plus the terminal-stamp proof.  The public handle layout
 * and its StaticAssertDecl set above are unchanged.
 */
typedef struct ClusterItlTouchRecord {
	ClusterItlTouchHandle key; /* frozen 24-byte public value */
	ClusterItlTerminalProof proof;
	ClusterCtrcReceiptHandle ctrc_handle;
} ClusterItlTouchRecord;

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
 *
 *	This legacy entry point records no terminal-stamp proof; the finish
 *	hook resolves such records through the exact no-fetch helper and skips
 *	them safely.  Production heap write sites use
 *	cluster_itl_touch_register_exact below.
 */
extern void cluster_itl_touch_register(const ClusterItlTouchHandle *handle);

/*
 * cluster_itl_touch_register_exact -- register a touched ITL slot together
 * with its terminal-stamp authority proof.
 *
 *	Must run after the ITL/tuple change is WAL-protected and outside the
 *	critical section, while the caller still holds the same buffer's
 *	content lock EXCLUSIVE and the same local PCM X round (all seven heap
 *	production registration sites satisfy this).  Reads the slot's xid,
 *	wrap and active class from the page under that content lock and
 *	projects the local ownership generation and acquisition epoch from the
 *	exact ACTIVE-holder ledger.  A failed capture registers the record
 *	with proof.valid=false: the terminal stamp then skips it safely and
 *	TT/CLOG/undo resolves the slot.
 *
 *	Dedupe is by (rloc, fork, block, slot, xid, slot_wrap) within the
 *	current subtransaction owner range only.  The same exact slot
 *	incarnation ORs NEEDS_WAL and replaces the proof with the newest
 *	successful capture; if the newest capture fails, the existing record
 *	is invalidated (an older valid generation would be unsafe).  A
 *	different xid or wrap is a distinct entry, never an overwrite.
 */
extern void cluster_itl_touch_register_exact(const ClusterItlTouchHandle *handle, Buffer buffer,
											 TransactionId xid);

/* Receipt-bearing twin for an ordinary heap path that already crossed exact
 * APPLY.  The receipt handle is copied only into the private touch record;
 * ClusterItlTouchHandle remains the frozen 24-byte public ABI. */
extern void cluster_itl_touch_register_exact_ctrc(const ClusterItlTouchHandle *handle,
												  Buffer buffer, TransactionId xid,
												  const ClusterCtrcReceiptHandle *ctrc_handle);

/* Lookup-only accelerator for same-transaction, same-ITL receipt reuse.
 * The caller holds the page content lock EXCLUSIVE.  A hit requires the
 * current slot bytes and local X-owner proof to match the newest capture in
 * the current subtransaction range; a miss changes no shared state. */
extern bool cluster_itl_touch_lookup_reusable_ctrc(const ClusterItlTouchHandle *handle,
												   Buffer buffer, TransactionId xid,
												   ClusterCtrcReceiptHandle *ctrc_handle_out);

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
