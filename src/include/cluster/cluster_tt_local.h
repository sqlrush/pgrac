/*-------------------------------------------------------------------------
 *
 * cluster_tt_local.h
 *	  pgrac cluster Undo TT local install helper — invoked from xact.c
 *	  commit/abort path after PG native CLOG is finalized.
 *
 *	  spec-3.1 D5 (NEW).
 *
 *	  Companion to cluster_tt_status.c (D2 overlay).  Mints a
 *	  provisional `tt_slot_id` via a process-shared monotonic atomic
 *	  counter and installs an in-memory overlay entry.  Does NOT write
 *	  ITL commit_scn to page (推 spec-3.4) and does NOT broadcast to
 *	  peers (推 spec-3.2+).
 *
 *	  spec-3.1 v0.4 N6:  provisional `tt_slot_id` is minted by
 *	  `pg_atomic_fetch_add_u32(&cluster_tt_local_slot_seq, 1)`;
 *	  incompatible with future spec-3.4 real TT slot allocation —
 *	  cluster_tt_status_flush_all() must run before spec-3.4 swap-over.
 *
 *	  spec-3.1 v0.4 N7:  caller (D6 xact hook) MUST exercise the install
 *	  → lookup self-consumer path in every build to keep D5/D6 wired
 *	  (not dead helper; L173 defense).  Assert builds add a fast-fail check.
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
 *	  src/include/cluster/cluster_tt_local.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_TT_LOCAL_H
#define CLUSTER_TT_LOCAL_H

#include "c.h"
#include "access/transam.h"
#include "cluster/cluster_scn.h"	   /* SCN */
#include "cluster/cluster_tt_status.h" /* ClusterTTStatus */

/*
 * cluster_tt_local_record_commit -- install COMMITTED status for a
 * local transaction into the in-memory TT status overlay.
 *
 * Caller invokes this from xact.c RecordTransactionCommit after PG
 * native CLOG is finalized.  No-op when cluster mode disabled or
 * xid is not a normal transaction id.
 *
 * spec-3.3 D6 (L181 chain step 1): commit_scn parameter added so the
 * cluster TT status overlay carries the real commit_scn end-to-end
 * (xact.c -> here -> install_local -> wire V2 -> receiver -> snapshot
 * consumer). InvalidScn here would short-circuit the entire chain to
 * UNKNOWN at the consumer side and silently revert spec-3.2's
 * COMMITTED visibility -- L181 forbids partial chains.
 */
extern void cluster_tt_local_record_commit(TransactionId xid, SCN commit_scn);

/*
 * cluster_tt_local_precommit_durable_finish (spec-3.11 D4) -- durably stamp
 * commit_scn on this xact's TT slot (undo segment header) + emit
 * XLOG_UNDO_TT_SLOT_COMMIT, BEFORE the commit record (xact.c pre-commit hook;
 * spec-3.11 C1).  No-op when the xact has no TT binding.  Distinct from
 * cluster_tt_local_record_commit (post-commit overlay install) -- this is the
 * durable-write half (D4 / C1; record_commit must NOT do durable I/O).
 */
extern void cluster_tt_local_precommit_durable_finish(TransactionId xid, SCN commit_scn);

/*
 * cluster_tt_local_record_abort -- install ABORTED status for a local
 * transaction.  Same invariants as record_commit.
 */
extern void cluster_tt_local_record_abort(TransactionId xid);

/*
 * cluster_tt_local_record_active (spec-3.4d D4 / F3 P0):
 *	  Install local ACTIVE status + emit cross-node TT_STATUS_HINT ACTIVE
 *	  for `xid` from heap_lock_tuple / heap_lock_updated_tuple_rec.  Used
 *	  when peer mode lock-only ITL slot is stamped — without this peer
 *	  cluster_tt_status_lookup_exact returns UNKNOWN forever, breaking
 *	  the spec-3.4d Q1 wait_policy-aware fail-closed contract.
 *
 *	  Idempotent:  install_local + hint_emit are both safe to repeat
 *	  (overlay overwrite, fanout fire-and-forget).  Multiple lock
 *	  operations within the same xact bump the install_count counter
 *	  but do not break correctness.
 *
 *	  commit_scn = InvalidScn (lock-only status has no commit_scn);
 *	  peer reader sees ACTIVE status without commit_scn → returns
 *	  ACTIVE via lookup_exact + heap_lock_tuple consumer fail-closes
 *	  per wait_policy.
 *
 *	  No-op if cluster_enabled is false or xact-local TT binding is
 *	  missing (DDL-only / read-only xact never allocated a binding;
 *	  the lock-only ITL slot stamp call site is responsible for
 *	  ensuring the binding exists before invoking this).
 */
extern void cluster_tt_local_record_active(TransactionId xid);

/*
 * Counters / introspection used by test_cluster_tt_status (D9 T17).
 */
extern uint32 cluster_tt_local_slot_seq_peek(void);


/*
 * spec-3.4b D4 — xact-local real TT binding (F11).
 *
 *	Heap DML callsites (D5) invoke cluster_tt_local_get_or_create_binding
 *	*before* entering the critical section.  First call within an xact
 *	allocates a real (segment_id, slot_offset) tuple via the per-segment
 *	allocator; subsequent calls within the same xact return the cached
 *	binding.  cluster_tt_local_record_commit / _abort consume the same
 *	binding when installing the overlay entry, then reset it via
 *	cluster_tt_local_reset_binding().
 *
 *	F7: NO provisional fallback on allocator OVERFLOW -- the helper
 *	raises ERROR fail-closed; caller MUST NOT install via the provisional
 *	mint path.
 *
 *	Returns true and fills outs when binding is available; returns false
 *	(without raising) when cluster mode is disabled or top_xid is not a
 *	normal transaction id (caller falls back to PG-native silent per
 *	spec-3.4a A6).
 */
extern bool cluster_tt_local_get_or_create_binding(TransactionId top_xid, uint32 *out_segment_id,
												   uint16 *out_slot_offset, uint32 *out_tt_slot_id);

/* Read-only accessor; does not allocate. */
extern bool cluster_tt_local_peek_binding(TransactionId top_xid, uint32 *out_segment_id,
										  uint16 *out_slot_offset, uint32 *out_tt_slot_id,
										  uint32 *out_cluster_epoch);

/* Cheap xact-end predicate; true only when this xact owns a local TT binding. */
extern bool cluster_tt_local_has_binding(TransactionId top_xid);

/* Free the bound TT slot and clear the binding (idempotent). */
extern void cluster_tt_local_reset_binding(void);

/*
 * Shmem helpers (the monotonic counter lives in shmem so EXEC_BACKEND
 * children share state with postmaster).
 */
extern Size cluster_tt_local_shmem_size(void);
extern void cluster_tt_local_shmem_init(void);
extern void cluster_tt_local_shmem_register(void);

#endif /* CLUSTER_TT_LOCAL_H */
