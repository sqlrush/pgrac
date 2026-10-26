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
#include "access/transam.h"			  /* TransactionId */
#include "storage/relfilelocator.h"	  /* RelFileLocator */
#include "storage/block.h"			  /* BlockNumber */
#include "storage/itemptr.h"		  /* OffsetNumber */
#include "common/relpath.h"			  /* ForkNumber */
#include "cluster/cluster_itl_slot.h" /* UBA */
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_savepoint.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_undo_record.h"		/* UndoRecordType */
#include "cluster/cluster_undo_extent.h"		/* ClusterUndoExtent */
#include "cluster/storage/cluster_undo_alloc.h" /* ClusterUndoSegTryRecycle (3.13) */
#include "cluster/storage/cluster_undo_block0_current.h"


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

typedef enum ClusterUndoRecordPrepareResult {
	CLUSTER_UNDO_RECORD_PREPARE_READY = 0,
	CLUSTER_UNDO_RECORD_PREPARE_RETRY_REQUIRED,
	CLUSTER_UNDO_RECORD_PREPARE_REFUSED
} ClusterUndoRecordPrepareResult;

typedef enum ClusterUndoRecordConsumeResult {
	CLUSTER_UNDO_RECORD_CONSUME_APPLIED = 0,
	CLUSTER_UNDO_RECORD_CONSUME_RETRY_REQUIRED,
	CLUSTER_UNDO_RECORD_CONSUME_REFUSED
} ClusterUndoRecordConsumeResult;

typedef enum ClusterUndoRecordConsumePreflightResult {
	CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_READY = 0,
	CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_RETRY_REQUIRED,
	CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_REFUSED
} ClusterUndoRecordConsumePreflightResult;

#define CLUSTER_UNDO_RECORD_CTRC_TARGETS 2
#define CLUSTER_UNDO_OPERATION_CAPTURE_PRESENT 1
#define CLUSTER_UNDO_SAVEPOINT_RESERVE_PRESENT 1
#define CLUSTER_UNDO_SAVEPOINT_BOUNDS_PRESENT 1
#define CLUSTER_UNDO_SAVEPOINT_PENDING_PRESENT 1
#define CLUSTER_UNDO_SAVEPOINT_HEAD_PRESENT 1
#define CLUSTER_UNDO_SAVEPOINT_CURSOR_PRESENT 1
#define CLUSTER_UNDO_SAVEPOINT_COMPLETE_PRESENT 1

typedef struct ClusterUndoSavepointControl ClusterUndoSavepointControl;
/* Before any data effect or DATA receipt, with no content/entry lock held.
 * Each returned handle owns an original pool reference to physical RESERVED
 * bytes, not a top-chain head or rollback completion. Failure clears every
 * output and retires only this batch's unused references; ERROR is rethrown.
 * Caller storage for the outputs must cover count entries. */
extern bool
cluster_undo_record_reserve_savepoint_controls(const ClusterSavepointOperation *operations,
											   uint32 count, uint64 absolute_deadline_us,
											   ClusterUndoSavepointControl **out);
/* No scope lookup/allocation: exact owned cleanup survives native scope pop. */
extern void cluster_undo_record_release_savepoint_control(ClusterUndoSavepointControl *control);
/* Live-frame preparation recheck only; not a durable or cleanup authority. */
extern bool cluster_undo_record_savepoint_control_owned(const ClusterUndoSavepointControl *control);
/* Native frame xact callback, before TopTransactionContext is deleted. */
extern void cluster_undo_record_release_savepoint_controls_at_xact_end(void);

/* Backend-local first-effect stop and a current head snapshot. The physical
 * RESERVED control still has invalid roots. No field is a chain-reachability,
 * current/CRC, durability or recovery proof, and this does not activate it. */
typedef struct ClusterUndoSavepointBounds {
	ClusterSavepointOperation operation;
	UBA control_uba;
	UBA stop_uba;
	UBA current_head;
} ClusterUndoSavepointBounds;
/* Live scope only, outside critical sections, with no active DATA reservation
 * or consume/ref ownership. Consumed APPLIED responsibilities remain intact.
 * No allocation, xid assignment, storage, WAL or receipt cleanup.
 * false zeroes *out; true with head==stop denotes an empty local range.
 * A caller must stop effects and prove the actual retained chain before use. */
extern bool cluster_undo_record_savepoint_control_bounds(const ClusterUndoSavepointControl *control,
														 ClusterUndoSavepointBounds *out);

/* Shared origin only, while the original frame is live and DATA is quiesced.
 * Install the exact RESERVED control as PENDING through the original segment
 * current publication, pool content-X and WAL/debt owner. The captured local
 * bounds must still match. A repeated stage requires the identical PENDING
 * image and original local WAL cut. false clears *cut_out without publication;
 * failures after WAL insertion are fatal. No new allocation or receipt cleanup.
 *
 * The original reference transfers to top cleanup, surviving frame release.
 * This stage does NOT publish either head, flush WAL, prove chain reachability
 * or grant rollback effects. The caller must complete original TT/head/ref and
 * durability publication before advancing a cursor or returning from rollback. */
extern bool cluster_undo_record_stage_savepoint_pending(ClusterUndoSavepointControl *control,
														const ClusterUndoSavepointBounds *expected,
														uint64 absolute_deadline_us,
														XLogRecPtr *cut_out);

/* First activation only, before rollback effects and outside content locks.
 * Validate the full original retained chain, including below the exclusive
 * stop, using original live-owner publications and own-origin pool pins.
 * expected_tt_head is a compare input, not authority: the original canonical
 * TT owner must prove it again. Nonempty expected heads must be retained
 * activated controls, never a DATA address or an operation-sequence guess.
 *
 * Install PENDING, publish/flush its canonical ACTIVE head through the original
 * TT owner, then advance the existing local head to this retained control.
 * Original control refs remain owned by top cleanup. false clears *cut_out
 * before staging; an uncertain failure after PENDING is fatal, not catchable
 * by a parent EXCEPTION block. Success proves head publication only: it does
 * not discard an operation, advance a cursor, complete rollback, or permit
 * native child cleanup. A second activation requires a new reserved control. */
extern bool cluster_undo_record_publish_savepoint_pending(
	ClusterUndoSavepointControl *control, const ClusterUndoSavepointBounds *expected,
	UBA expected_tt_head, uint64 absolute_deadline_us, XLogRecPtr *cut_out);

/* Advance one original undo block of an already published live control.
 * The original handle's compare copy must match the current physical control;
 * it is not a recovery/resume credential. Extended DATA is marked DISCARDED
 * under the original own-origin publication/pool X pin, then that pin is
 * released before cursor WAL and flush. Existing top references stay owned.
 *
 * false clears outputs for unknown or not-yet-published handles. Once a live
 * published control is accepted, uncertainty is fatal, never parent cleanup.
 * Success returns a flushed original-thread cut. at_stop means exact position
 * only: this leaves phase MARKING, does not publish COMPLETE, release locks or
 * permit native subtransaction cleanup. No allocation, top terminal stamp,
 * receipt cancellation, heap/index deletion, or foreign write is performed. */
extern bool cluster_undo_record_advance_savepoint(ClusterUndoSavepointControl *control,
												  uint64 absolute_deadline_us, XLogRecPtr *cut_out,
												  bool *at_stop_out);

/* Requalify the exact original range and require all DATA DISCARDED before
 * publishing COMPLETE through the original WAL/dirty owner. Flush, re-read
 * that range and compare the physical control before returning its source
 * cut. A repeated COMPLETE requalifies without new WAL or a new flush.
 * Unknown/unpublished handles return false with a cleared output; accepted
 * published uncertainty is fatal and retains original top references.
 * This result alone does not authorize native subtransaction cleanup, SQL
 * return, top terminal publication or commit acknowledgement. No top/ITL
 * terminal stamp, receipt cancellation or reference release is performed. */
extern bool cluster_undo_record_complete_savepoint(ClusterUndoSavepointControl *control,
												   uint64 absolute_deadline_us,
												   XLogRecPtr *cut_out);


/* Stack-only identity for one backend-local prepared DATA reservation.  It is
 * neither shared state nor authority: the block0 publication is a retained
 * exact proof, while reservation_sequence names the sole backend-local owner.
 * The absolute deadline bounds preparation only.  Once READY, it is a
 * historical diagnostic stamp, not a lease.  An exact invalidation that
 * requires a new preparation still uses that original, never-refreshed budget.
 * Record type and body capacity are immutable after preparation.  Capacity
 * excludes history/operation trailers, whose space is accounted internally. */
typedef struct ClusterUndoRecordPrepareReceipt {
	uint32 magic;
	uint8 record_type;
	uint8 owner_instance;
	uint16 payload_capacity;
	uint16 tt_slot_segment_id;
	uint16 tt_slot_offset;
	uint32 actual_segment_id;
	uint64 reservation_sequence;
	uint64 absolute_deadline_us;
	ClusterSavepointOperation operation; /* immutable before any shared APPLY */
	ClusterUndoExtent extent;
	ClusterUndoBlock0LiveOwnerPublication block0_publication;
	ClusterSemanticAdmissionToken modifier_admission;
	ClusterCtrcTargetV1 ctrc_pending_targets[CLUSTER_UNDO_RECORD_CTRC_TARGETS];
	ClusterCtrcReceiptHandle ctrc_handles[CLUSTER_UNDO_RECORD_CTRC_TARGETS];
	uint8 ctrc_pending_mask;
	uint8 ctrc_prepared_mask;
	uint8 ctrc_applied_mask;
	uint8 ctrc_reuse_mask;
	uint32 ctrc_attempt_generation;
	UndoItlHistoryEntry itl_history[UNDO_ITL_HISTORY_TARGETS];
	uint8 itl_history_mask;
	uint8 itl_history_reserved[7];
} ClusterUndoRecordPrepareReceipt;

/* Diagnostic counters, not receipt state or authority. */
typedef enum ClusterUndoReceiptMetric {
	CLUSTER_UNDO_RECEIPT_PREPARE_START,
	CLUSTER_UNDO_RECEIPT_PREPARE_TIMEOUT,
	CLUSTER_UNDO_RECEIPT_READY,
	CLUSTER_UNDO_RECEIPT_READY_SURVIVED_DEADLINE,
	CLUSTER_UNDO_RECEIPT_PREFLIGHT_IDENTITY_REFUSAL,
	CLUSTER_UNDO_RECEIPT_APPLY,
	CLUSTER_UNDO_RECEIPT_CANCEL,
	CLUSTER_UNDO_RECEIPT_CAPACITY_REFUSAL,
	CLUSTER_UNDO_RECEIPT_RESERVATION_MISMATCH,
	CLUSTER_UNDO_RECEIPT_REPREPARE_PREMATURE,
	CLUSTER_UNDO_RECEIPT_INVALIDATED_BUDGET_EXHAUSTED,
	CLUSTER_UNDO_RECEIPT_POST_APPLY_REPREPARE,
	CLUSTER_UNDO_RECEIPT_RETRY_PRESERVED,
	CLUSTER_UNDO_RECEIPT_METRIC_COUNT
} ClusterUndoReceiptMetric;

extern bool
cluster_undo_record_receipt_stats_snapshot(uint64 values[CLUSTER_UNDO_RECEIPT_METRIC_COUNT]);
extern const char *cluster_undo_record_receipt_last_reason(void);
extern bool cluster_undo_record_retry_evidence(uint64 reservation_sequence, bool *exact_ready,
											   bool *targets_invalidated);
/* Outside all heap content/recycle ownership: READY means retained unchanged;
 * RETRY_REQUIRED means exactly canceled and new preparation is permitted;
 * REFUSED means no reprepare is permitted.  The original budget never changes.
 * targets_invalidated must come from an exact target comparison, not a wait. */
extern ClusterUndoRecordPrepareResult
cluster_undo_record_requalify_for_retry(ClusterUndoRecordPrepareReceipt *receipt,
										uint16 payload_len, bool targets_invalidated);

/* No heap ownership held. Replace only UPDATE's unpublished page intents;
 * the exact READY undo reservation and its original deadline are retained. */
typedef enum ClusterUndoTargetResetResult {
	CLUSTER_UNDO_TARGET_RESET_NOT_APPLICABLE = 0,
	CLUSTER_UNDO_TARGET_RESET_READY,
	CLUSTER_UNDO_TARGET_RESET_REFUSED
} ClusterUndoTargetResetResult;

extern ClusterUndoTargetResetResult
cluster_undo_record_reset_update_targets(ClusterUndoRecordPrepareReceipt *receipt,
										 const ClusterCtrcTargetV1 *targets, uint8 required_mask);

extern uint64 cluster_undo_record_prepare_deadline_us(void);
extern ClusterUndoRecordPrepareResult
cluster_undo_record_prepare(uint8 record_type, uint16 payload_capacity, uint16 tt_slot_segment_id,
							uint16 tt_slot_offset, UBA prev_uba, uint64 absolute_deadline_us,
							ClusterUndoRecordPrepareReceipt *receipt);
extern bool cluster_undo_record_prepared_recheck(const ClusterUndoRecordPrepareReceipt *receipt,
												 uint16 payload_len);
extern bool cluster_undo_record_prepared_uba_exact(const ClusterUndoRecordPrepareReceipt *receipt,
												   uint16 payload_len, UBA *uba_out);
extern bool cluster_undo_record_stage_history(ClusterUndoRecordPrepareReceipt *receipt,
											  uint8 target_ordinal,
											  const UndoItlHistoryEntry *entry);
extern bool cluster_undo_record_history_matches(const ClusterUndoRecordPrepareReceipt *receipt,
												uint8 target_ordinal,
												const UndoItlHistoryEntry *entry);
extern bool cluster_undo_record_ctrc_stage_pending(ClusterUndoRecordPrepareReceipt *receipt,
												   uint8 target_ordinal,
												   const ClusterCtrcTargetV1 *pending_target);
extern bool cluster_undo_record_ctrc_prepare_pending(ClusterUndoRecordPrepareReceipt *receipt,
													 uint8 target_ordinal);
extern bool cluster_undo_record_ctrc_stage_reuse(ClusterUndoRecordPrepareReceipt *receipt,
												 uint8 target_ordinal,
												 const ClusterCtrcReceiptHandle *handle);
extern bool cluster_undo_record_ctrc_pending_matches(const ClusterUndoRecordPrepareReceipt *receipt,
													 uint8 target_ordinal,
													 const ClusterCtrcTargetV1 *pending_target);
extern bool cluster_undo_record_ctrc_pending_recheck(const ClusterUndoRecordPrepareReceipt *receipt,
													 uint8 target_ordinal,
													 const ClusterCtrcTargetV1 *pending_target);
extern bool
cluster_undo_record_ctrc_required_prepared(const ClusterUndoRecordPrepareReceipt *receipt,
										   uint8 required_mask);
extern ClusterCtrcApplyResult cluster_undo_record_ctrc_apply_prepared(
	ClusterUndoRecordPrepareReceipt *receipt, uint8 target_ordinal,
	const ClusterCtrcTargetV1 *final_target, ClusterCtrcApplyToken *token);
extern void cluster_undo_record_cancel_prepared(ClusterUndoRecordPrepareReceipt *receipt);
/* PGRAC: abort-only; the caller must have completed LWLockReleaseAll(). */
extern void cluster_undo_record_abort_prepared_after_lwlock_release(void);
/* Release only a branch-proven zero-effect preflight, once. Retain the
 * reservation, deadline and refs for the original out-of-heap requalifier. */
extern bool cluster_undo_record_release_zero_effect_preflight(
	ClusterUndoRecordPrepareReceipt *receipt);
extern ClusterUndoRecordConsumePreflightResult
cluster_undo_record_consume_preflight(ClusterUndoRecordPrepareReceipt *receipt, uint16 payload_len);
extern ClusterUndoRecordConsumeResult
cluster_undo_record_consume_prepared(ClusterUndoRecordPrepareReceipt *receipt,
									 const ClusterUndoRecordTarget *target, const void *payload,
									 uint16 payload_len, UBA *out_uba);


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
 *	xact.  Captured into the 2PC record at PREPARE, before PostPrepare performs
 *	the full local teardown.
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
 * cluster_undo_record_xact_reset -- full local teardown for PREPARE/ABORT.
 *	Normal COMMIT uses the dedicated O(1) cleanup hook below.
 */
extern void cluster_undo_record_xact_reset(void);

/*
 * cluster_undo_record_xact_commit_release -- O(1) normal-COMMIT cleanup.
 *	Releases the active-write boundary and clears transaction-local counts and
 *	flags while preserving the residual extent and fd cache.  The retention
 *	horizon continues to gate actual reclaim.
 */
extern void cluster_undo_record_xact_commit_release(void);


/*
 * cluster_undo_record_is_touched -- legacy transaction-local write marker.
 */
extern bool cluster_undo_record_is_touched(void);


/*
 * cluster_undo_xact_precommit_flush -- P0 perf hardening (2026-05-31).
 *	fsync this xact's dirtied undo segment files ONCE, on the commit path,
 *	BEFORE the commit becomes visible (replaces per-record fsync).  ereport(ERROR)
 *	on fsync failure (runs before the commit critical section -> clean abort).
 *	No-op for a xact that wrote no undo.  A release-build runtime guard rejects
 *	any deferred block that remains pending after the flush.
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

/* Passive pool-capacity observation. */
extern void cluster_undo_record_observation_ensure(void);
extern uint64 cluster_undo_segment_allocated_count(void);
extern uint64 cluster_undo_segment_allocated_high_water(void);
extern uint32 cluster_undo_segment_effective_cap(void);
extern const char *cluster_undo_segment_observation_status_string(void);

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
/* S3 forensics step 1a — TT-rollover FAILURE split (hard cap vs autoextend);
 * counts only cluster_undo_tt_rollover_locked fails, NOT the record-extent
 * CLAIM path's segment_hard_cap_fail_count. */
extern uint64 cluster_undo_tt_rollover_fail_hard_cap_count(void);
extern uint64 cluster_undo_tt_rollover_fail_extend_count(void);
extern uint64 cluster_undo_tt_rollover_fail_activate_count(void);
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
/* EPOCH_CHANGED when the spec-5.22e F-D2 fence tripped inside the mutation
 * lock: the mutation was not performed; abort the whole pass. */
extern ClusterUndoSegTryRecycle
cluster_undo_segment_advance_recyclable(uint32 segment_id, SCN horizon, uint64 expected_epoch);

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
/* spec-4.12a Hardening v1.0.1: residual extents dropped by locked revalidation. */
extern uint64 cluster_undo_record_seg_residual_revalidate_drop_count(void);

#endif /* !FRONTEND */
#endif /* CLUSTER_UNDO_RECORD_API_H */
