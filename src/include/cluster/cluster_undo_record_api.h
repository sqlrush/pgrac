/*-------------------------------------------------------------------------
 *
 * cluster_undo_record_api.h
 *	  pgrac record-level allocator + reader public API (spec-3.7 D4).
 *
 *	  Stage 3 第 11 sub-spec — record-level API on top of spec-3.4b 已
 *	  ship segment-level allocator(`cluster_undo_alloc.h`).
 *
 *	  API surface(本 header):
 *	    - cluster_undo_record_alloc()  — write one undo record;
 *	      durable-flush before returning UBA per W2 self-contained
 *	      ordering(spec-3.7 §3.4)
 *	    - cluster_undo_get_record()  — read one undo record by UBA
 *	    - cluster_undo_shmem_register()  — register cluster_undo shmem
 *	      region(record-level cursor state per V-6 verify)
 *
 *	  Critical section safety:
 *	    - alloc + get NOT critical-section safe(may write/flush/palloc)
 *	    - DML caller MUST invoke alloc BEFORE START_CRIT_SECTION;
 *	      only publish returned UBA inside critical section
 *	      (per spec-3.7 §3.3 I1 + I3 invariants)
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.7-undo-record-format-allocator.md (FROZEN v0.4)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_undo_record_api.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Backend-only (uses palloc / shmem / smgr in implementation).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_RECORD_API_H
#define CLUSTER_UNDO_RECORD_API_H

#ifndef FRONTEND

#include "postgres.h"
#include "access/transam.h"						/* TransactionId */
#include "storage/relfilelocator.h"				/* RelFileLocator */
#include "storage/block.h"						/* BlockNumber */
#include "storage/itemptr.h"					/* OffsetNumber */
#include "common/relpath.h"						/* ForkNumber */
#include "cluster/cluster_itl_slot.h"			/* UBA */
#include "cluster/cluster_undo_record.h"		/* UndoRecordType */
#include "cluster/storage/cluster_undo_alloc.h" /* ClusterUndoSegTryRecycle (3.13) */


/*
 * ClusterUndoRecordTarget -- 24B physical target locator passed to
 *	cluster_undo_record_alloc().  MUST NOT be omitted;  callers MUST
 *	supply RelFileLocator + ForkNumber + BlockNumber + OffsetNumber
 *	(per codex review F4).
 */
typedef struct ClusterUndoRecordTarget {
	RelFileLocator locator;
	ForkNumber forknum;
	BlockNumber blockno;
	OffsetNumber offnum;
	uint16 _pad; /* alignment */
} ClusterUndoRecordTarget;

StaticAssertDecl(sizeof(ClusterUndoRecordTarget) == 24,
				 "ClusterUndoRecordTarget must be 24B — HC213a");


/*
 * cluster_undo_record_alloc -- write one undo record into per-instance
 *	undo segment.  Returns 16B UBA on success;  InvalidUba on failure.
 *
 *	Args:
 *	  record_type -- UNDO_RECORD_INSERT / UPDATE / DELETE / ITL
 *	  target      -- physical target locator (REQUIRED, F4)
 *	  payload     -- pointer to op-specific payload(see cluster_undo_record.h)
 *	  payload_len -- payload byte count (payload struct + var bytes)
 *	  prev_uba    -- backward chain (InvalidUba if first record in xid)
 *
 *	Returns:
 *	  UBA(16B,encoded via spec-3.4b uba_encode):segment_id + block_no +
 *	  tt_slot_offset(xact TT slot)+ row_offset(undo block slot-dir index).
 *	  Returns InvalidUba on failure(segment exhaustion / oversize / I/O fail /
 *	  durable flush fail).
 *
 *	Critical section safety:NOT critical-section safe.  Callers MUST
 *	invoke before START_CRIT_SECTION and ereport(53R9D)on InvalidUba
 *	outside critical section.  Per spec-3.7 §3.3 I1 + I3.
 *
 *	Durable ordering(per W2 self-contained,§3.4):returned UBA always
 *	references a durable record(undo block bytes fsync-d to shared
 *	storage before this function returns).  TT/ITL slot UBA publication
 *	occurs AFTER this function returns,inside START_CRIT_SECTION.
 */
extern UBA cluster_undo_record_alloc(uint8 record_type, const ClusterUndoRecordTarget *target,
									 uint16 tt_slot_segment_id, uint16 tt_slot_offset,
									 const void *payload, uint16 payload_len, UBA prev_uba);


/*
 * cluster_undo_get_record -- read one undo record by UBA.  Used by:
 *	  - cluster_undo_get_record(uba) SQL function (D8 sanity reader)
 *	  - spec-3.X rollback apply (forward-link)
 *	  - spec-3.9 CR construction (forward-link)
 *
 *	Args:
 *	  uba         -- target record UBA (16B)
 *	  out_buffer  -- caller-provided buffer
 *	  buffer_size -- buffer capacity
 *
 *	Returns: bytes written (header + payload);  0 if UBA invalid or
 *	buffer too small.  WARNING emitted on segment file missing.
 *
 *	NOT critical-section safe (may palloc internally if cross-segment read).
 *	Own-instance read only at spec-3.7;  cross-instance read推 spec-3.9.
 */
extern size_t cluster_undo_get_record(UBA uba, void *out_buffer, size_t buffer_size);

/*
 * cluster_undo_local_head_get -- spec-4.8 D7-A: backend-local latest undo-chain
 *	head for a TT slot (segment_id, slot_offset), or InvalidUba if none this
 *	xact.  Captured into the 2PC record at PREPARE (the local head is reset at
 *	xact end, so this must be read before PostPrepare/commit).
 */
extern UBA cluster_undo_local_head_get(uint16 tt_slot_segment_id, uint16 tt_slot_offset);


/*
 * Shmem region API.  cluster_undo_record_alloc() uses per-instance
 *	record-level cursor state(separate from segment-level alloc shmem
 *	already shipped by spec-3.4b).
 */
extern void cluster_undo_record_shmem_register(void);
extern Size cluster_undo_record_shmem_size(void);
extern void cluster_undo_record_shmem_init(void);


/*
 * cluster_undo_record_xact_reset -- reset per-xid binding at end of xact
 *	(commit / abort).  Called from xact.c CommitTransaction /
 *	AbortTransaction hook (spec-3.7 D6 integration).
 */
extern void cluster_undo_record_xact_reset(void);

/*
 * cluster_undo_record_xact_commit_release -- spec-4.12a D6: release this
 *	backend's active-write boundary slot at COMMIT (CommitTransaction hook).
 *	The full xact_reset only runs on the PREPARE / ABORT paths, so without this
 *	a persistent connection's committed transaction would pin the undo drain
 *	boundary forever.  8.A-safe: only enables ACTIVE->COMMITTED; the retention
 *	horizon still gates the actual reclaim.
 */
extern void cluster_undo_record_xact_commit_release(void);


/*
 * cluster_undo_record_is_touched -- did current xact write any undo record?
 *	Used by D16 PREPARE TRANSACTION guard.
 */
extern bool cluster_undo_record_is_touched(void);


/*
 * cluster_undo_xact_precommit_flush -- P0 perf hardening (2026-05-31).
 *	fsync this xact's dirtied undo segment files ONCE, on the commit path,
 *	BEFORE the commit becomes visible (replaces per-record fsync).  ereport(ERROR)
 *	on fsync failure (runs before the commit critical section -> clean abort).
 *	No-op for a xact that wrote no undo.
 */
extern void cluster_undo_xact_precommit_flush(void);


/*
 * Counter accessors -- for emit_row / cluster_tap verification + D10 counters.
 *	Spec-3.7 §2.6: 5 NEW counter(per Hardening v1.0.1 + D10).
 */
extern uint64 cluster_undo_record_alloc_count(void);
extern uint64 cluster_undo_segment_claim_count(void);
extern uint64 cluster_undo_extent_claim_count(void); /* spec-3.18 D3/D7 */
extern uint64 cluster_undo_block_write_count(void);
extern uint64 cluster_undo_block_flush_count(void);
extern uint64 cluster_undo_reader_lookup_count(void);

/* spec-3.8 D10: 4 NEW lifecycle counter accessors. */
extern uint64 cluster_undo_autoextend_count(void);
extern uint64 cluster_undo_segment_switch_count(void);
extern uint64 cluster_undo_segment_create_fail_count(void);
extern uint64 cluster_undo_segment_hard_cap_fail_count(void);

/*
 * spec-3.12 D2b: TT-slot retention-pressure segment rollover.
 *
 *	cluster_undo_tt_rollover_locked: rebind the node's TT-slot allocator to a
 *	fresh undo segment when retained COMMITTED slots fill the active one (takes
 *	lifecycle_lock internally).  Returns the new active TT segment_id, or 0 on
 *	extend failure (*out_at_hard_cap distinguishes the 53R9E hard cap).
 *	cluster_undo_tt_retention_rollover_count: observability counter accessor.
 */
extern uint32 cluster_undo_tt_rollover_locked(int node_id, uint32 old_segment_id,
											  bool *out_at_hard_cap);
extern uint64 cluster_undo_tt_retention_rollover_count(void);
extern uint64 cluster_undo_segment_retain_skip_count(void);

/* P0 perf hardening: per-commit undo fsync counters. */
extern uint64 cluster_undo_commit_fsync_count(void);
extern uint64 cluster_undo_commit_fsync_segment_count(void);
extern uint64 cluster_undo_commit_fsync_failure_count(void);

/* P0 perf hardening: undo smgr syscall observability.  Bumps are called from
 * cluster_undo_smgr.c; accessors back pg_cluster_state / TAP. */
extern void cluster_undo_record_note_smgr_open(void);
extern void cluster_undo_record_note_smgr_close(void);
extern void cluster_undo_record_note_smgr_pread(void);
extern void cluster_undo_record_note_smgr_pwrite(void);
/* spec-3.13 D4 */
extern void cluster_undo_record_note_segment_reuse(void);
extern uint64 cluster_undo_segment_reuse_count(void);
extern uint64 cluster_undo_smgr_open_count(void);
extern uint64 cluster_undo_smgr_close_count(void);
extern uint64 cluster_undo_smgr_pread_count(void);
extern uint64 cluster_undo_smgr_pwrite_count(void);

/* spec-3.8 Fix 6: deterministic autoextend trigger test hook. */
extern bool cluster_undo_test_force_segment_end(void);

/* spec-3.13 D3: cleaner-side segment lifecycle surface. */
extern uint32 cluster_undo_record_active_segment_id(void);
extern ClusterUndoSegTryRecycle cluster_undo_segment_advance_recyclable(uint32 segment_id,
																		SCN horizon);

/*
 * spec-4.12a D1: record-segment ACTIVE -> COMMITTED drain.  Called at the
 * record-cursor rollover (seal_scn = current SCN, stamps the seal) and from the
 * cleaner's skipped-ACTIVE fallback pass (seal_scn = InvalidScn, D2).  Caller
 * MUST hold the undo lifecycle_lock.  See cluster_undo_record.c for the six 8.A
 * hard gates.  Counter accessors back pg_cluster_state / TAP (D5).
 */
extern void cluster_undo_try_mark_record_segment_committed(uint32 segment_id, uint8 owner_instance,
														   SCN seal_scn);
/* spec-4.12a D2: cleaner-side fallback re-evaluation (acquires lifecycle_lock). */
extern void cluster_undo_segment_advance_committed(uint32 segment_id);
extern uint64 cluster_undo_record_segments_committed_count(void);
extern uint64 cluster_undo_record_seg_commit_skipped_inflight_count(void);

#endif /* !FRONTEND */
#endif /* CLUSTER_UNDO_RECORD_API_H */
