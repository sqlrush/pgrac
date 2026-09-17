/*-------------------------------------------------------------------------
 *
 * cluster_undo_record.c
 *	  pgrac record-level undo allocator + reader (spec-3.7 D5).
 *
 *	  Stage 3 第 11 sub-spec — record-level API on top of spec-3.4b 已
 *	  ship segment-level allocator(`cluster_undo_alloc.c`).
 *
 *	  Public APIs(declared in cluster_undo_record_api.h):
 *	    - cluster_undo_record_alloc()  — write one undo record + durable flush
 *	    - cluster_undo_get_record()    — read one undo record by UBA
 *	    - cluster_undo_record_shmem_register/size/init
 *	    - cluster_undo_record_xact_commit_release() — normal COMMIT cleanup
 *	    - cluster_undo_record_xact_reset()  — PREPARE/ABORT full teardown
 *	    - cluster_undo_record_is_touched()  — legacy write marker accessor
 *
 *	  Concurrency model(MVP):
 *	    - single per-instance LWLock(`cursor_lock`)protects active segment
 *	      cursor advance(active_segment_id / current_block / free_offset /
 *	      slot_count).多 backend 写同 instance → serialize on this lock.
 *	    - per-backend transaction-local state is reset by the normal-COMMIT
 *	      hook or the PREPARE/ABORT full teardown.
 *
 *	  Block boundary contract(per spec-3.7 §3.2):record 不跨 block。
 *	  block 不够时 advance 到下一 block;segment 不够时 ereport 53R9D
 *	  fail-closed(caller 在 critical section 之前调用).
 *
 *	  Durable ordering(per spec-3.7 §3.4 W2 self-contained):
 *	  cluster_undo_record_alloc() 返回 non-InvalidUba 前必须 undo block
 *	  bytes 已 fsync 到 shared storage。
 *
 *	  File I/O 模式:全部走 cluster_undo_smgr 抽象层
 *	  (cluster_undo_smgr_read_block / write_block) — spec-3.8 Fix #372
 *	  落地后,record.c + alloc.c lifecycle helpers 不再 inline
 *	  BasicOpenFile + pg_pread/pwrite。File create + WAL-protect 仍走
 *	  cluster_undo_segment_allocate(),smgr_create_segment_file 是其
 *	  公开 wrapper。
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.7-undo-record-format-allocator.md (FROZEN v0.4 +
 *       Hardening v1.0.1 H-1/H-2)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_undo_record.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "miscadmin.h"
#include "access/twophase.h" /* spec-4.12a D1: prepared-xact retention guard */
#include "access/xact.h"
#include "port/atomics.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h" /* spec-3.18 D2b MyProc->delayChkptFlags (DELAY_CHKPT_START) */
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h" /* spec-3.18 D7: ClusterUndoExtentClaim wait event */

#include "cluster/cluster_guc.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tt_local.h"		/* PGRAC: spec-4.5a G4 peek binding wrap */
#include "cluster/cluster_recovery_merge.h" /* PGRAC: spec-4.5a is_materialized */
#include "cluster/cluster_scn.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_tt_slot.h"	  /* spec-3.12 D2b: TT allocator rollover */
#include "cluster/cluster_tt_status.h"	  /* spec-4.5a D11: remote_uba_resolved */
#include "cluster/cluster_undo_cleaner.h" /* Q8 pressure wakeup (3.13) */
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_format.h"
#include "cluster/cluster_undo_record.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_undo_retention.h" /* spec-4.12a D1: drain gate + boundary */
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_undo_extent.h" /* spec-3.18 D3 per-txn extent */
#include "cluster/storage/cluster_undo_block0_current.h"
#include "cluster/storage/cluster_undo_buf.h" /* spec-3.18 D1 read/write-through */
#include "cluster/storage/cluster_undo_alloc.h"
#include "cluster/storage/cluster_undo_xlog.h" /* spec-3.18 D2a XLOG_UNDO_BLOCK_WRITE */
#include "cluster/cluster_xnode_profile.h"	   /* PGRAC: spec-5.59 D7 profiling */
#include "cluster/cluster_undo_horizon.h"	   /* epoch fence (spec-5.22e F-D2) */

#include "access/xlog.h" /* GetXLogWriteRecPtr */

/* Local helper to express InvalidUba sentinel (all-zero UBA). */
static const UBA InvalidUbaVal = InvalidUba_init;
#define InvalidUba (InvalidUbaVal)


/*
 * ClusterUndoRecordShared -- per-instance shmem state for record-level
 *	cursor advance + counters.
 *
 *	Layout(packed,~96B + LWLock):
 *	  - active_segment_id : 0 means no active segment yet (lazy claim)
 *	  - current_block     : data block index within active segment (block 0 is segment header, so data starts at 1)
 *	  - free_offset       : free byte offset within current_block
 *	  - slot_count        : record count in current_block
 *	  - block_first_scn   : SCN of first record in current_block
 *	  - block_dirty       : 1 if current_block has data not yet block_write_count'd
 *	  - 5 atomic counters
 *	  - cursor_lock       : LWLock protect cursor advance
 */
typedef struct ClusterUndoRecordShared {
	uint32 active_segment_id;
	uint32 current_block;
	uint32 free_offset;
	uint16 slot_count;
	uint16 block_dirty;
	SCN block_first_scn;

	/*
	 * spec-3.18 D3 extent high-water:  next free data block in active_segment_id
	 * available to claim as an extent (everything in [1, next_extent_block) is
	 * already claimed).  Advanced under lifecycle_lock at extent-claim time;
	 * shmem-only, rebuilt on restart from the segment's bitmap (B1).  The old
	 * current_block / free_offset / slot_count cursor stays for the
	 * undo_extent_blocks=1 / pre-D3 fallback path + observability.
	 */
	uint32 next_extent_block;
	uint32 _pad_align;

	pg_atomic_uint64 record_alloc_count;
	pg_atomic_uint64 segment_claim_count;
	pg_atomic_uint64 block_write_count;
	pg_atomic_uint64 block_flush_count;
	pg_atomic_uint64 reader_lookup_count;
	pg_atomic_uint64 receipt_metrics[CLUSTER_UNDO_RECEIPT_METRIC_COUNT];

	/* spec-3.8 D10: 4 NEW lifecycle counters. */
	pg_atomic_uint64 autoextend_count;			/* cluster_undo_segment_extend_or_create success */
	pg_atomic_uint64 segment_switch_count;		/* active_segment_id 切换 */
	pg_atomic_uint64 segment_create_fail_count; /* FS error / timeout */
	pg_atomic_uint64 segment_hard_cap_fail_count; /* 53R9E hard cap */

	/* spec-3.13 D4: RECYCLABLE segments reborn in place by the allocator
	 * (reuse-first extend path).  dump_undo row lands at step 8. */
	pg_atomic_uint64 segment_reuse_count;

	/* spec-3.12 D2b: TT-slot retention-pressure segment rollovers (a long
	 * reader's retained COMMITTED slots filled the active segment, so the
	 * allocator rebound to a fresh one instead of erroring "48 slots full"). */
	pg_atomic_uint64 tt_retention_rollover_count;

	/* S3 forensics step 1a: TT-rollover FAILURES, split by cause.  The
	 * record-extent CLAIM path has its own segment_hard_cap_fail_count
	 * above — these two count ONLY cluster_undo_tt_rollover_locked fails
	 * (the writer-facing "retention rollover failed" error), so the TT
	 * errdetail never cites another path's counter. */
	pg_atomic_uint64 tt_rollover_fail_hard_cap_count; /* pool at hard cap */
	pg_atomic_uint64 tt_rollover_fail_extend_count;	  /* autoextend / FS fail */
	pg_atomic_uint64 tt_rollover_fail_activate_count; /* mark_active refused (step 1b) */

	/* spec-3.18 D3: extent claims (one per ~undo_extent_blocks records per
	 * backend instead of one cursor_lock acquire per record). */
	pg_atomic_uint64 extent_claim_count;

	/* spec-3.12 D5: undo segments skipped for recycle because their retention
	 * watermark was >= the horizon.  In this lazy MVP the active segment is the
	 * one skipped at each retention rollover; spec-3.13's proactive scan will
	 * also bump this. */
	pg_atomic_uint64 segment_retain_skip_count;

	/* spec-4.12a D1: record-segment ACTIVE -> COMMITTED drain observability.
	 *   record_segments_committed             : record segments advanced to
	 *     COMMITTED by the rollover / cleaner drain gate (the leak fix).
	 *   record_seg_commit_skipped_inflight    : drain attempts retained by a
	 *     hard gate (proves the 8.A guard actually fires; D4 L4 asserts > 0). */
	pg_atomic_uint64 record_segments_committed;
	pg_atomic_uint64 record_seg_commit_skipped_inflight;

	/* spec-4.12a Hardening v1.0.1 (P0 8.A): count carried-over residual extents
	 * dropped by the under-lifecycle_lock revalidation because their segment is
	 * no longer the active one (rolled away / sealed since the previous xact).
	 * Proves the residual revalidation path fires (D-h1 TAP counter-delta) and
	 * gives ops a signal of cross-backend rollover churn. */
	pg_atomic_uint64 record_seg_residual_revalidate_drops;

	/* P0 perf hardening (2026-05-31): per-commit (group) undo fsync.
	 * Durability ordering unchanged (undo durable BEFORE commit visible), but
	 * fsync granularity moved from per-record (inside cursor_lock) to per-xact
	 * precommit (outside the lock).  commit_fsync_count ~= committing xacts that
	 * wrote undo;  segment_count = segment files actually fsync'd. */
	pg_atomic_uint64 commit_fsync_count;
	pg_atomic_uint64 commit_fsync_segment_count;
	pg_atomic_uint64 commit_fsync_failure_count;

	/* P0 perf hardening: undo segment-file syscall observability (bumped by
	 * cluster_undo_smgr).  Before the fd cache / active-block cache these track
	 * 1:1 with undo records; after, opens drop to per-segment-switch and
	 * preads/pwrites drop to per-block. */
	pg_atomic_uint64 smgr_open_count;
	pg_atomic_uint64 smgr_close_count;
	pg_atomic_uint64 smgr_pread_count;
	pg_atomic_uint64 smgr_pwrite_count;

	/* Passive pool-capacity observation.  Numeric gauges stay valid while
	 * readiness/status independently report whether the latest scan is usable. */
	pg_atomic_uint64 segment_allocated_count;
	pg_atomic_uint64 segment_allocated_high_water;
	pg_atomic_uint32 segment_observation_ready;
	pg_atomic_uint32 segment_observation_status;
	pg_atomic_uint32 segment_observation_attempted;

	LWLockPadded cursor_lock;
	/* spec-3.8 D3: lifecycle_lock — protects autoextend slow path
	 * (active_segment_id publication + state transitions).  Per spec
	 * §3.2:  写 record 只 cursor_lock;  撞满 → release cursor_lock →
	 * acquire lifecycle_lock → recheck active_segment_id → 必要
	 * re-acquire cursor_lock 发布 NEW active.  禁止同时长期持有两锁
	 * 做 I/O (但 lifecycle_lock 可持有期间 file create + fsync). */
	LWLockPadded lifecycle_lock;
	/* spec-4.12a D1: protects the active-write boundary registry (the
	 * per-backend first_undo_scn slot array that lives immediately after this
	 * struct in shmem).  Registration (write own slot) is once per top-xact;
	 * boundary computation (scan all slots) runs at rollover / cleaner drain.
	 * Lock order: lifecycle_lock -> registry_lock (the drain holds lifecycle_
	 * lock first); registration holds registry_lock alone. */
	LWLockPadded registry_lock;
} ClusterUndoRecordShared;


static ClusterUndoRecordShared *UndoRecordShared = NULL;

/*
 * spec-4.12a D1: active-write boundary registry.
 *
 *	cluster_undo_write_registry points at a MaxBackends-element SCN array that
 *	lives immediately after ClusterUndoRecordShared in the same shmem region
 *	(no NEW region -- spec Q6 / L254).  Slot [MyBackendId - 1] holds this
 *	backend's current top-transaction first_undo_scn, or InvalidScn when it has
 *	no in-flight undo.  The minimum over valid slots is the "oldest active-write
 *	boundary"; a record segment sealed strictly below it has no in-flight
 *	writer's undo and may drain (cluster_undo_record_segment_drainable).
 */
static SCN *cluster_undo_write_registry = NULL;

/* This backend already registered first_undo_scn for the current top-xact.
 * Cleared by normal-COMMIT cleanup or PREPARE/ABORT full teardown. */
static bool cluster_undo_write_registered = false;

/* spec-4.12a Hardening v1.0.1 (P0 8.A): this backend already revalidated its
 * carried-over residual extent for the current top-xact.  The carried residual
 * is only stale at the txn's first undo alloc (another backend may have rolled
 * the segment away since the previous xact); after the one locked revalidation
 * the extent is current-xact-owned and reused lock-free.  Re-armed by
 * normal-COMMIT cleanup or PREPARE/ABORT full teardown. */
static bool cluster_undo_residual_validated_this_xact = false;

/* Legacy write marker retained as transaction-local API state. */
static bool cluster_undo_touched_in_xact = false;

/*
 * spec-3.18 D3:  the backend's currently-held undo extent.  segment_id ==
 * CLUSTER_UNDO_EXTENT_NONE means "no extent held" (claim one on next write).
 * The cursor (cur_block / cur_free_offset / cur_slot_count) advances lock-free
 * within the extent; only claiming a new extent touches lifecycle_lock.  A
 * residual extent is deliberately carried across transactions and revalidated
 * under lifecycle_lock before the next top-xact reuses it.
 */
static ClusterUndoExtent cluster_undo_current_extent = { 0 };

/*
 * Exact, process-local receipt for the DATA segment whose records this
 * backend may expose.  It is not authority: the receipt is revalidated before
 * every record allocation, and every segment switch requires a fresh exact
 * publication before the first UBA for that segment is returned.
 */
static uint32 cluster_undo_record_block0_publication_segment_id = 0;
static ClusterUndoBlock0LiveOwnerPublication cluster_undo_record_block0_publication;

/*
 * spec-3.25 D1b: deferred per-(xact,block) undo WAL merge.
 *
 *	D0 (clean CI run 27257818226) measured the per-record XLOG_UNDO_BLOCK_WRITE
 *	emission at 63% of ALL WAL bytes (4.25 records/txn, 1:1 with record_alloc).
 *	Records one transaction appends to one block are physically contiguous, so
 *	ONE record (XLOG_UNDO_BLOCK_WRITE_MULTI) can carry them all: alloc mutates
 *	this backend-local image and extends the spans; the WAL emission + the pool
 *	copy-in + mark_dirty are deferred to the FLUSH points:
 *
 *	  - block switch / segment switch (cluster_undo_record_alloc),
 *	  - cluster_undo_xact_precommit_flush (commit AND 2PC PREPARE: the merged
 *	    record reaches WAL before the commit/prepare record -- same durable
 *	    ordering as the old per-record emission),
 *	  - cluster_undo_record_xact_reset (abort: emitting keeps exact parity
 *	    with the old per-record behaviour, where an aborted xact's records
 *	    were already WAL'd; on flush failure the pending image is dropped --
 *	    an aborted xact's undo needs no durability).
 *
 *	WAL-before-data holds by construction: the shared pool image and its dirty
 *	state advance ONLY inside the flush (write_undo_block with the new LSN), so
 *	write-back / checkpoint / eviction never see undo bytes whose WAL is not
 *	inserted.  A crash before the flush loses only an uncommitted transaction's
 *	undo, which no reader may need (own changes are native-self-visible, remote
 *	in-progress is invisible, the 3.21 exact-key resolver only consults
 *	non-own xids).  Write-through mode (writeback off) keeps the per-record
 *	path: t/228's D2a always-FPI semantics are unchanged.
 */
typedef struct ClusterUndoPendingBlock {
	bool active;
	uint8 owner_instance;
	uint32 segment_id;
	uint32 block_no;
	XLogRecPtr old_block_lsn; /* block LSN at image load (FPI-vs-delta input) */
	uint16 rec_lo;			  /* records span [rec_lo, rec_hi) */
	uint16 rec_hi;
	uint16 slot_min_off; /* slot-dir span endpoints (direction-agnostic) */
	uint16 slot_max_off;
	uint32 nrecords;
	int ref_slot;	  /* pool residency reference (eviction gate); -1 = none */
	char buf[BLCKSZ]; /* mutation source; pool image mirrors it per record */
} ClusterUndoPendingBlock;
static ClusterUndoPendingBlock cluster_undo_pending = { 0 };

#define CLUSTER_UNDO_RECORD_RECEIPT_MAGIC ((uint32)0x55525250)
#define CLUSTER_UNDO_RECORD_PREPARE_TIMEOUT_MS 10000

/* One backend can execute only one heap mutation at a time.  This volatile
 * reservation names the sole prepared DATA block and holds one buffer-pool
 * residency reference until exact consume or cancel.  It is not transaction
 * authority; canonical TT and block0-current remain the authority sources. */
typedef struct ClusterUndoRecordReservation {
	bool active;
	bool owns_ref;
	bool consume_locked;
	uint64 sequence;
	int ref_slot;
	int consume_local_head_idx;
	UBA consume_effective_prev_uba;
	ClusterUndoRecordPrepareReceipt receipt;
	PGAlignedBlock block;
} ClusterUndoRecordReservation;

static ClusterUndoRecordReservation cluster_undo_record_reservation
	= { .ref_slot = -1, .consume_local_head_idx = -1 };
static uint64 cluster_undo_record_reservation_floor = 0;

static const char *cluster_undo_receipt_reason = "NONE";
/* Before-cancel diagnostics only; never consulted to authorize a mutation. */
static struct {
	bool armed;
	bool exact_ready;
	bool targets_invalidated;
	uint64 sequence;
	uint8 applied_mask;
} cluster_undo_retry_cancel_snapshot;

static void
cluster_undo_receipt_metric_add(ClusterUndoReceiptMetric metric)
{
	Assert(metric >= 0 && metric < CLUSTER_UNDO_RECEIPT_METRIC_COUNT);
	if (UndoRecordShared != NULL)
		pg_atomic_fetch_add_u64(&UndoRecordShared->receipt_metrics[metric], 1);
}

bool
cluster_undo_record_receipt_stats_snapshot(uint64 values[CLUSTER_UNDO_RECEIPT_METRIC_COUNT])
{
	int i;

	if (values == NULL || UndoRecordShared == NULL)
		return false;
	for (i = 0; i < CLUSTER_UNDO_RECEIPT_METRIC_COUNT; i++)
		values[i] = pg_atomic_read_u64(&UndoRecordShared->receipt_metrics[i]);
	return true;
}

const char *
cluster_undo_record_receipt_last_reason(void)
{
	return cluster_undo_receipt_reason;
}

static bool cluster_undo_pending_flush_internal(bool error_on_fail);
static void cluster_undo_record_observation_apply_locked(uint8 owner_instance);

/*
 * P0 perf hardening (2026-05-31): per-backend touched-undo-segment list.
 *
 *	cluster_undo_record_alloc() no longer fsyncs each record (that serialized
 *	every backend inside cursor_lock).  Instead it records which undo segment
 *	files this xact has dirtied; cluster_undo_xact_precommit_flush() fsyncs them
 *	ONCE, BEFORE the commit becomes visible (commit_scn publish / commit record
 *	flush).  Granularity is the segment FILE (fsync is file-level), so the list
 *	dedups on (segment_id, owner_instance); the shared-cursor model keeps an
 *	xact's undo in 1-2 active segments, so this is typically a single fsync.
 *
 *	Top-xact aggregation: subxact undo writes append to the same per-backend
 *	list and are flushed by the parent's precommit (no per-savepoint fsync).
 *	Normal COMMIT clears the live count in its O(1) cleanup hook.  PREPARE and
 *	ABORT clear it in cluster_undo_record_xact_reset().
 *	Overflow (> MAX distinct segments in one xact — pathological) degrades to an
 *	inline fsync of the overflowing segment, never back to per-record fsync.
 */
#define CLUSTER_UNDO_TOUCHED_SEG_MAX 16
typedef struct ClusterUndoTouchedSeg {
	uint32 segment_id;
	uint8 owner_instance;
} ClusterUndoTouchedSeg;
static ClusterUndoTouchedSeg cluster_undo_touched_segs[CLUSTER_UNDO_TOUCHED_SEG_MAX];
static int cluster_undo_touched_seg_count = 0;

/*
 * cluster_undo_record_touched_segment -- note that this xact dirtied an undo
 *	segment file (to be fsync'd once at precommit).  Dedups on
 *	(segment_id, owner_instance).  MUST be called OUTSIDE cursor_lock (the whole
 *	point is to keep fsync — and this bookkeeping — off the serialized path).
 *	Pathological overflow (> MAX distinct segments in one xact) fsyncs the
 *	overflowing segment inline (still off the cursor_lock) rather than dropping
 *	it — correctness over the perf optimization, but never per-record fsync.
 */
static void
cluster_undo_record_touched_segment(uint32 segment_id, uint8 owner_instance)
{
	int i;

	for (i = 0; i < cluster_undo_touched_seg_count; i++)
		if (cluster_undo_touched_segs[i].segment_id == segment_id
			&& cluster_undo_touched_segs[i].owner_instance == owner_instance)
			return; /* already tracked — fsync'd once at precommit */

	if (cluster_undo_touched_seg_count >= CLUSTER_UNDO_TOUCHED_SEG_MAX) {
		/* pathological: degrade to inline per-segment fsync, never per-record. */
		if (!cluster_undo_smgr_fsync_segment_file(segment_id, owner_instance)) {
			if (UndoRecordShared != NULL)
				pg_atomic_fetch_add_u64(&UndoRecordShared->commit_fsync_failure_count, 1);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("cluster undo overflow fsync failed for segment %u (instance %u)",
							segment_id, owner_instance)));
		}
		if (UndoRecordShared != NULL)
			pg_atomic_fetch_add_u64(&UndoRecordShared->commit_fsync_segment_count, 1);
		return;
	}

	cluster_undo_touched_segs[cluster_undo_touched_seg_count].segment_id = segment_id;
	cluster_undo_touched_segs[cluster_undo_touched_seg_count].owner_instance = owner_instance;
	cluster_undo_touched_seg_count++;
}

/*
 * Backend-local latest undo head per TT slot.
 *
 * spec-3.7 v0.4 says TTSlot.first_undo_block is logically the latest/head
 * undo UBA.  Existing spec-3.4b DML callers still pass the TT-only UBA as
 * `prev_uba` on every operation, so the record allocator must maintain the
 * true per-xact undo chain locally until later specs persist the TT head in
 * the undo segment header.  Only entries below cluster_undo_local_head_count
 * are live; normal COMMIT resets that count in O(1), while PREPARE/ABORT also
 * zero the backing array during full teardown.
 */
typedef struct ClusterUndoLocalHead {
	uint16 tt_slot_segment_id;
	uint16 tt_slot_offset;
	UBA head;
} ClusterUndoLocalHead;

#define CLUSTER_UNDO_LOCAL_HEAD_MAX 1024
static ClusterUndoLocalHead cluster_undo_local_heads[CLUSTER_UNDO_LOCAL_HEAD_MAX];
static uint32 cluster_undo_local_head_count = 0;


static int
cluster_undo_local_head_find(uint16 tt_slot_segment_id, uint16 tt_slot_offset)
{
	uint32 i;

	for (i = 0; i < cluster_undo_local_head_count; i++) {
		if (cluster_undo_local_heads[i].tt_slot_segment_id == tt_slot_segment_id
			&& cluster_undo_local_heads[i].tt_slot_offset == tt_slot_offset)
			return (int)i;
	}
	return -1;
}


static bool
cluster_undo_local_head_ensure(uint16 tt_slot_segment_id, uint16 tt_slot_offset, int *out_idx)
{
	int idx = cluster_undo_local_head_find(tt_slot_segment_id, tt_slot_offset);

	if (idx >= 0) {
		*out_idx = idx;
		return true;
	}
	if (cluster_undo_local_head_count >= CLUSTER_UNDO_LOCAL_HEAD_MAX)
		return false;

	idx = (int)cluster_undo_local_head_count++;
	cluster_undo_local_heads[idx].tt_slot_segment_id = tt_slot_segment_id;
	cluster_undo_local_heads[idx].tt_slot_offset = tt_slot_offset;
	cluster_undo_local_heads[idx].head = InvalidUba;
	*out_idx = idx;
	return true;
}

/*
 * cluster_undo_local_head_get -- spec-4.8 D7-A: read the backend-local latest
 *	undo-chain head for a TT slot (segment_id, slot_offset), or InvalidUba if
 *	this backend wrote no undo for that slot this transaction.  Used by
 *	AtPrepare_ClusterTT to capture the head into the 2PC record while it is
 *	still in memory, before PostPrepare performs the full local teardown.
 */
UBA
cluster_undo_local_head_get(uint16 tt_slot_segment_id, uint16 tt_slot_offset)
{
	int idx = cluster_undo_local_head_find(tt_slot_segment_id, tt_slot_offset);

	if (idx < 0)
		return InvalidUba;
	return cluster_undo_local_heads[idx].head;
}


/*
 * cluster_undo_record_shmem_size
 *	  Bytes required by the record-level cursor + counter shmem region.
 */
Size
cluster_undo_record_shmem_size(void)
{
	Size sz = MAXALIGN(sizeof(ClusterUndoRecordShared));

	/* spec-4.12a D1: per-backend active-write registry slots trail the struct
	 * (GCS pattern).  Sized for MaxBackends; addressed by MyBackendId - 1. */
	sz = add_size(sz, mul_size((Size)MaxBackends, sizeof(SCN)));
	return sz;
}


/*
 * cluster_undo_record_shmem_init
 *	  Postmaster-once shmem layout + initial values.  Called via the
 *	  ClusterShmemRegion.init_fn callback during postmaster shmem
 *	  attachment.
 */
void
cluster_undo_record_shmem_init(void)
{
	bool found;
	char *base;

	base = ShmemInitStruct("ClusterUndoRecordShared", cluster_undo_record_shmem_size(), &found);
	UndoRecordShared = (ClusterUndoRecordShared *)base;
	/* spec-4.12a D1: registry slots immediately follow the struct. */
	cluster_undo_write_registry = (SCN *)(base + MAXALIGN(sizeof(ClusterUndoRecordShared)));

	if (!found) {
		int tranche_id = LWLockNewTrancheId();
		int i;

		memset(UndoRecordShared, 0, sizeof(*UndoRecordShared));
		UndoRecordShared->active_segment_id = 0;
		UndoRecordShared->current_block = 0;
		UndoRecordShared->free_offset = 0;
		UndoRecordShared->slot_count = 0;
		UndoRecordShared->block_dirty = 0;
		UndoRecordShared->block_first_scn = InvalidScn;
		UndoRecordShared->next_extent_block = 0; /* spec-3.18 D3: 0 => rebuild on first claim */

		pg_atomic_init_u64(&UndoRecordShared->record_alloc_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->segment_claim_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->block_write_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->block_flush_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->reader_lookup_count, 0);
		for (i = 0; i < CLUSTER_UNDO_RECEIPT_METRIC_COUNT; i++)
			pg_atomic_init_u64(&UndoRecordShared->receipt_metrics[i], 0);

		/* spec-3.8 D10: 4 NEW lifecycle counters. */
		pg_atomic_init_u64(&UndoRecordShared->autoextend_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->segment_switch_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->segment_create_fail_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->segment_hard_cap_fail_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->segment_reuse_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->tt_retention_rollover_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->tt_rollover_fail_hard_cap_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->tt_rollover_fail_extend_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->tt_rollover_fail_activate_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->segment_retain_skip_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->extent_claim_count, 0); /* spec-3.18 D3 */

		/* spec-4.12a D1: record-segment drain counters. */
		pg_atomic_init_u64(&UndoRecordShared->record_segments_committed, 0);
		pg_atomic_init_u64(&UndoRecordShared->record_seg_commit_skipped_inflight, 0);
		/* spec-4.12a Hardening v1.0.1: residual revalidation drop counter. */
		pg_atomic_init_u64(&UndoRecordShared->record_seg_residual_revalidate_drops, 0);

		/* P0 perf hardening: per-commit undo fsync counters. */
		pg_atomic_init_u64(&UndoRecordShared->commit_fsync_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->commit_fsync_segment_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->commit_fsync_failure_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->smgr_open_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->smgr_close_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->smgr_pread_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->smgr_pwrite_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->segment_allocated_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->segment_allocated_high_water, 0);
		pg_atomic_init_u32(&UndoRecordShared->segment_observation_ready, 0);
		pg_atomic_init_u32(&UndoRecordShared->segment_observation_status,
						   (uint32)CLUSTER_UNDO_POOL_OBS_INVALID_OWNER);
		pg_atomic_init_u32(&UndoRecordShared->segment_observation_attempted, 0);

		LWLockInitialize(&UndoRecordShared->cursor_lock.lock, tranche_id);
		LWLockInitialize(&UndoRecordShared->lifecycle_lock.lock, tranche_id);
		/* spec-4.12a D1: registry_lock shares the cursor tranche (no NEW
		 * wait-event tranche name -> no wait-event baseline ripple). */
		LWLockInitialize(&UndoRecordShared->registry_lock.lock, tranche_id);
		LWLockRegisterTranche(tranche_id, "cluster_undo_record_cursor");

		/* spec-4.12a D1: every active-write registry slot starts empty. */
		for (i = 0; i < MaxBackends; i++)
			cluster_undo_write_registry[i] = InvalidScn;
	}
}


/*
 * ClusterShmemRegion descriptor + registration entry.
 */
static const ClusterShmemRegion cluster_undo_record_region = {
	.name = "pgrac cluster undo record cursor",
	.size_fn = cluster_undo_record_shmem_size,
	.init_fn = cluster_undo_record_shmem_init,
	/* spec-3.8 D3: lwlock_count 1 → 2 (cursor_lock + lifecycle_lock).
	 * spec-4.12a D1: → 3 (+ registry_lock, same region/tranche — no NEW
	 * region/tranche per L206/L254).  Informational; LWLocks are embedded in
	 * the struct and initialized in shmem_init. */
	.lwlock_count = 3,
	.owner_subsys = "cluster_undo_record",
	.reserved_flags = 0,
};

void
cluster_undo_record_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_undo_record_region);
}


/* ============================================================
 * spec-4.12a D1: active-write boundary registry + record-segment drain gate.
 * ============================================================ */

/*
 * cluster_undo_active_write_register -- publish this top-transaction's
 *	first_undo_scn so a record segment it writes into is retained until it
 *	commits / aborts / prepares.
 *
 *	8.A ORDERING (R1c, spec-4.12a §3-2c): the caller invokes this BEFORE the
 *	transaction's first undo record can become visible in any segment (before
 *	the residual-extent reuse, the active_segment_id read, and the undo block
 *	write in cluster_undo_record_alloc) and BEFORE advancing the SCN for that
 *	record (first_undo_scn = cluster_scn_current() <= every write_scn it later
 *	stamps).  The registry_lock release here happens-before the eventual seal's
 *	registry_lock acquire (the segment can only seal after this writer's undo
 *	has filled it), so the drain always sees this entry.  Idempotent per
 *	top-xact via cluster_undo_write_registered.
 */
static void
cluster_undo_active_write_register(SCN first_undo_scn)
{
	int idx;

	if (cluster_undo_write_registered)
		return; /* already registered this top-xact */
	if (UndoRecordShared == NULL || cluster_undo_write_registry == NULL)
		return;
	if (!SCN_VALID(first_undo_scn))
		return; /* nothing to bound */

	idx = (int)MyBackendId - 1;
	if (idx < 0 || idx >= MaxBackends)
		return; /* non-backend writer (e.g. startup redo) does not register */

	LWLockAcquire(&UndoRecordShared->registry_lock.lock, LW_EXCLUSIVE);
	cluster_undo_write_registry[idx] = first_undo_scn;
	LWLockRelease(&UndoRecordShared->registry_lock.lock);
	cluster_undo_write_registered = true;
}

/*
 * cluster_undo_active_write_unregister -- drop this backend's registry entry at
 *	end of top-xact.  Normal COMMIT calls the dedicated cleanup hook; PREPARE
 *	and ABORT call the full reset.  On PREPARE, the prepared-xact guard takes
 *	over retention after EndPrepare and before PostPrepare_ClusterTT unregisters
 *	the active writer, so the handoff has no gap.
 */
static void
cluster_undo_active_write_unregister(void)
{
	int idx;

	if (!cluster_undo_write_registered)
		return;
	cluster_undo_write_registered = false;

	if (UndoRecordShared == NULL || cluster_undo_write_registry == NULL)
		return;
	idx = (int)MyBackendId - 1;
	if (idx < 0 || idx >= MaxBackends)
		return;

	LWLockAcquire(&UndoRecordShared->registry_lock.lock, LW_EXCLUSIVE);
	cluster_undo_write_registry[idx] = InvalidScn;
	LWLockRelease(&UndoRecordShared->registry_lock.lock);
}

/*
 * cluster_undo_record_xact_commit_release -- normal-COMMIT undo cleanup.
 *
 *	The active-write boundary (cluster_undo_active_write_boundary) keeps a record
 *	segment retained from the ACTIVE -> COMMITTED drain only while an in-flight
 *	writer may still add undo to it.  Once this top-transaction COMMITs, its undo
 *	is governed by the retention HORIZON instead (cluster_undo_segment_recyclable
 *	still gates COMMITTED -> RECYCLABLE), so the boundary entry must drop here --
 *	otherwise it pins the drain boundary for the life of the backend.
 *
 *	The full reset only runs on PREPARE and ABORT.  Normal COMMIT therefore
 *	releases the active-write registry entry and clears all O(1) top-transaction
 *	bookkeeping here: the touched marker, live local-head count, touched-segment
 *	count, and residual-revalidation gate.  The residual extent and fd cache are
 *	backend caches, not transaction state, and remain available for reuse.
 *
 *	8.A-safe: dropping the boundary only enables ACTIVE -> COMMITTED; the horizon
 *	still gates the actual reclaim (COMMITTED -> RECYCLABLE), so a committed xact's
 *	undo that an older snapshot still needs stays retained until the horizon
 *	passes.  PREPARE uses the full reset after its prepared-xact retention guard
 *	has taken over.
 *
 *	Spec: spec-4.12a-undo-record-segment-reclaim.md
 */
void
cluster_undo_record_xact_commit_release(void)
{
	/* Precommit is the release-build fail-closed boundary; keep a debug check at
	 * the post-commit cleanup site to catch lifecycle regressions early. */
	Assert(!cluster_undo_pending.active);
	if (cluster_undo_record_reservation.active)
		cluster_undo_record_cancel_prepared(&cluster_undo_record_reservation.receipt);

	cluster_undo_active_write_unregister();
	cluster_undo_touched_in_xact = false;
	cluster_undo_local_head_count = 0;
	cluster_undo_touched_seg_count = 0;

	/* spec-4.12a Hardening v1.0.1: re-arm residual revalidation for the next
	 * top-xact.  xact_reset (which also clears this) runs only on PREPARE/ABORT,
	 * so the commit path must re-arm it here -- otherwise a persistent connection
	 * that commits and stays open would carry the flag set and SKIP revalidating
	 * its residual on the next xact, reusing a possibly-rolled-away segment (the
	 * very window this hardening closes). */
	cluster_undo_residual_validated_this_xact = false;
}

/*
 * Observe the original active-write registry in a PGPROC context. Empty is
 * only this registry's result, not TT terminal, private undo, or disk proof.
 * Unlike the oldest-boundary helper, missing state is not an infinite bound.
 */
ClusterNormalStopPollResult
cluster_undo_active_write_normal_stop_poll(int *backend_out, const char **reason_out)
{
	ClusterNormalStopPollResult result = CLUSTER_NORMAL_STOP_INVALID;
	const char *reason = "UNDO_ACTIVE_WRITE_STATE_INVALID";
	int backend = 0;
	int i;

	if (!IsUnderPostmaster || MyProc == NULL || UndoRecordShared == NULL
		|| cluster_undo_write_registry == NULL || MaxBackends <= 0
		|| !SCN_NODE_ID_VALID(cluster_node_id))
		goto done;
	result = CLUSTER_NORMAL_STOP_READY;
	reason = "NONE";
	LWLockAcquire(&UndoRecordShared->registry_lock.lock, LW_SHARED);
	for (i = 0; i < MaxBackends; i++) {
		SCN scn = cluster_undo_write_registry[i];
		if (scn == InvalidScn)
			continue;
		if (scn_local(scn) == 0 || scn_node_id(scn) != cluster_node_id) {
			result = CLUSTER_NORMAL_STOP_INVALID;
			backend = i + 1;
			reason = "UNDO_ACTIVE_WRITE_SCN_INVALID";
			break;
		}
		if (result == CLUSTER_NORMAL_STOP_READY) {
			result = CLUSTER_NORMAL_STOP_PENDING;
			backend = i + 1;
			reason = "UNDO_ACTIVE_WRITE_PENDING";
		}
	}
	LWLockRelease(&UndoRecordShared->registry_lock.lock);
done:
	if (backend_out != NULL)
		*backend_out = backend;
	if (reason_out != NULL)
		*reason_out = reason;
	return result;
}

/* Oldest first_undo_scn, or infinite when empty. The drain caller holds
 * lifecycle_lock; the existing order is lifecycle_lock -> registry_lock. */
static ClusterUndoActiveBoundary
cluster_undo_active_write_boundary(void)
{
	ClusterUndoActiveBoundary b;
	int i;

	b.infinite = true;
	b.scn = InvalidScn;

	if (UndoRecordShared == NULL || cluster_undo_write_registry == NULL)
		return b; /* unreachable on the active drain path (caller guards). */

	LWLockAcquire(&UndoRecordShared->registry_lock.lock, LW_SHARED);
	for (i = 0; i < MaxBackends; i++) {
		SCN s = cluster_undo_write_registry[i];

		if (!SCN_VALID(s))
			continue;
		if (b.infinite || scn_time_cmp(s, b.scn) < 0) {
			b.infinite = false;
			b.scn = s;
		}
	}
	LWLockRelease(&UndoRecordShared->registry_lock.lock);
	return b;
}

/*
 * cluster_undo_any_unresolved_prepared -- 硬门 6 signal: is there ANY unresolved
 *	prepared transaction whose undo may still be consumed by ROLLBACK PREPARED
 *	(spec-4.8 D7-A)?  Authoritative source = TwoPhaseState (numPrepXacts), which
 *	holds BOTH live prepares and crash-recovered ones (RecoverPreparedTransactions
 *	calls MarkAsPrepared -> numPrepXacts++ before replaying the 2PC records), and
 *	is decremented exactly at FinishPreparedTransaction (COMMIT/ROLLBACK
 *	PREPARED).  Sound across the EndPrepare -> PostPrepare handoff: the
 *	GlobalTransaction is added at EndPrepare, BEFORE PostPrepare_ClusterTT clears
 *	this backend's active-write registry slot, so a drain that observes the
 *	cleared slot also observes count >= 1 (no gap).  Read under TwoPhaseStateLock
 *	SHARED; short-circuited when 2PC is disabled.  Conservative (counts
 *	non-cluster prepared xacts too): Q11-A minimal-safe.
 *
 *	NOT the spec-3.15 D6 protected-slot map: that pin has the wrong lifetime for
 *	this gate.  twophase recover re-pins a crash-recovered xact's TT slot, but
 *	the pin is released lazily (its slot stays protected until later recycled),
 *	so cluster_tt_slot_protected_count() can stay > 0 AFTER the prepared xact has
 *	resolved -- which would wedge the record-segment drain off permanently after
 *	any crash-with-prepared, re-opening the spec-4.13 leak (observed: t/270 L6,
 *	pg_prepared_xacts empty yet the drain kept retaining).  numPrepXacts releases
 *	at resolution, so it is the correct unresolved-prepared signal.  The
 *	recovery window before TwoPhaseState is rebuilt is covered by the D3
 *	RecoveryInProgress gate in cluster_undo_record_segment_drainable (which
 *	retains every drain while replaying WAL).
 */
static bool
cluster_undo_any_unresolved_prepared(void)
{
	return max_prepared_xacts > 0 && GetNumberOfPreparedTransactions() > 0;
}

/*
 * cluster_undo_try_mark_record_segment_committed -- spec-4.12a D1 (§2.1).
 *
 *	Attempt to advance record segment `seg` SEGMENT_ACTIVE -> COMMITTED under
 *	the six 8.A hard gates (cluster_undo_record_segment_drainable).  Two call
 *	sites:
 *	  - record-cursor autoextend rollover (primary): seal_scn = current SCN,
 *	    which is stamped as the segment's conservative upper bound the first
 *	    time it is sealed;
 *	  - the cleaner's skipped-ACTIVE fallback pass (D2): seal_scn = InvalidScn,
 *	    re-evaluating an already-sealed segment (covers "in-flight at rollover,
 *	    committed later", Q3-C).
 *	The cursor lock supplies the single local operation owner while the
 *	lifecycle lock is released for exact 0xFB current/content-X acquisition.
 *
 *	ONE header RMW: read block 0; stamp record_seal_upper_scn iff seal_scn is
 *	valid and the slot is still unsealed; evaluate the drain gate; set COMMITTED
 *	iff drainable; write back + fsync iff anything changed.  Every uncertainty
 *	(read fail, identity mismatch, non-ACTIVE state, gate false) leaves the
 *	segment ACTIVE -- fail-closed toward "retain" (硬门 1/4); the cleaner
 *	re-evaluates next pass.  Best-effort write (no ereport): a failed COMMITTED
 *	write just retains the segment for another pass.
 */
static void
cluster_undo_try_mark_record_segment_committed_owned(uint32 seg, uint8 owner_instance, SCN seal_scn)
{
	PGAlignedBlock predecessor;
	PGAlignedBlock successor;
	UndoSegmentHeaderData *hdr;
	UndoSegmentHeaderData *next;
	ClusterUndoBlock0LogicalKey key;
	ClusterUndoBlock0Generation generation;
	bool dirty = false;
	bool advanced = false;
	ClusterUndoBlock0Result result;

	if (UndoRecordShared == NULL || seg == 0)
		return;
	Assert(LWLockHeldByMeInMode(&UndoRecordShared->cursor_lock.lock, LW_EXCLUSIVE));

	LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);
	if (!cluster_undo_smgr_read_block(cluster_undo_intent_for_owner(owner_instance), seg,
									  owner_instance, 0, predecessor.data)) {
		LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
		return; /* read fail -> retain (best-effort) */
	}
	if (!cluster_undo_segment_header_identity_ok(predecessor.data, seg, owner_instance)) {
		LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
		return; /* L212: identity, not template bytes -> retain */
	}
	hdr = (UndoSegmentHeaderData *)predecessor.data;
	if (hdr->wrap_count == UINT32_MAX) {
		LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
		return;
	}
	/* The cleaner visits TT-only COMMITTED segments too.  A seal added
	 * after terminal publication would require a final drain bound that can
	 * only be produced by ACTIVE -> COMMITTED, permanently retaining supply.
	 * Observe terminal/non-active headers without changing their history. */
	if (hdr->segment_state != SEGMENT_ACTIVE) {
		LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
		return;
	}
	memcpy(successor.data, predecessor.data, BLCKSZ);
	next = (UndoSegmentHeaderData *)successor.data;

	/* Seal stamp (rollover path): persist the conservative upper bound the
	 * first time this segment is sealed; never overwrite an existing seal. */
	if (SCN_VALID(seal_scn) && !SCN_VALID(UndoSegmentHeader_record_seal_upper_scn(hdr))) {
		UndoSegmentHeader_set_record_seal_upper_scn(next, seal_scn);
		dirty = true;
	}

	if (hdr->segment_state == SEGMENT_ACTIVE) {
		int node_id = (int)owner_instance - 1;
		uint32 fixed_first = (uint32)node_id * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
		ClusterUndoActiveBoundary boundary = cluster_undo_active_write_boundary();
		bool any_prepared = cluster_undo_any_unresolved_prepared();
		uint32 active_rec = UndoRecordShared->active_segment_id;
		uint32 active_tt = cluster_tt_slot_current_segment(node_id);
		/* spec-4.12a D3 (硬门 4): a crash empties the in-memory active-write
		 * registry, so the boundary degrades to {infinite} and cannot detect
		 * prepared / in-flight undo until RecoverPreparedTransactions rebuilds
		 * the protected-slot view.  Both call sites are unreachable during
		 * recovery (backends do not allocate undo while replaying WAL; the undo
		 * cleaner is only spawned at PM_RUN, which is post-recovery), but pass
		 * the flag so the drain gate stays the single auditable fail-closed
		 * recovery decision point. */
		bool in_recovery = RecoveryInProgress();

		if (cluster_undo_record_segment_drainable(hdr, boundary, any_prepared, fixed_first,
												  active_rec, active_tt, in_recovery)) {
			SCN drain_scn = cluster_scn_advance();

			if (UndoSegmentHeader_record_drain_upper_scn(hdr) == InvalidScn && SCN_VALID(drain_scn)
				&& scn_time_cmp(drain_scn, UndoSegmentHeader_record_seal_upper_scn(hdr)) >= 0) {
				UndoSegmentHeader_set_record_drain_upper_scn(next, drain_scn);
				next->segment_state = SEGMENT_COMMITTED;
				dirty = true;
				advanced = true;
			}
		} else {
			pg_atomic_fetch_add_u64(&UndoRecordShared->record_seg_commit_skipped_inflight, 1);
		}
	}
	LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);

	if (!dirty)
		return;
	key.segment_id = seg;
	key.owner_instance = owner_instance;
	generation.known = true;
	generation.value = hdr->wrap_count;
	result = cluster_undo_block0_current_live_owner_mutate_exact(
		&key, &generation, predecessor.data, successor.data, 10000);
	if (advanced && result == CLUSTER_UNDO_BLOCK0_OK)
		pg_atomic_fetch_add_u64(&UndoRecordShared->record_segments_committed, 1);
}

void
cluster_undo_try_mark_record_segment_committed(uint32 seg, uint8 owner_instance, SCN seal_scn)
{
	if (UndoRecordShared == NULL || seg == 0)
		return;
	LWLockAcquire(&UndoRecordShared->cursor_lock.lock, LW_EXCLUSIVE);
	cluster_undo_try_mark_record_segment_committed_owned(seg, owner_instance, seal_scn);
	LWLockRelease(&UndoRecordShared->cursor_lock.lock);
}

uint64
cluster_undo_record_segments_committed_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->record_segments_committed);
}

uint64
cluster_undo_record_seg_commit_skipped_inflight_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->record_seg_commit_skipped_inflight);
}

/* spec-4.12a Hardening v1.0.1: residual extents dropped by the locked
 * revalidation (segment no longer active -> would-be stale reuse averted). */
uint64
cluster_undo_record_seg_residual_revalidate_drop_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->record_seg_residual_revalidate_drops);
}


/* ---- Helper: compute record total length per record_type ---- */

static inline uint16
undo_record_total_length(uint8 record_type, uint16 payload_len)
{
	return (uint16)(sizeof(UndoRecordHeader) + payload_len);
}

static inline uint32
undo_prepared_record_total_length(uint16 body_length)
{
	return sizeof(UndoRecordHeader) + (uint32)body_length + sizeof(UndoItlHistoryTrailer);
}


/*
 * spec-3.8 Fix #372:  inline BasicOpenFile + pg_pread/pwrite/fsync helpers
 * replaced by cluster_undo_smgr layer.  The smgr layer is the single I/O
 * surface for undo segment block I/O — used here by the record allocator,
 * by cluster_undo_alloc.c for state-machine helpers, and (future) by
 * spec-3.9 CR construction + spec-3.10 CR cache.
 *
 * Thin static wrappers preserve the call-site signatures so the autoextend
 * branch and reader path don't have to thread through new APIs.
 */

static bool
read_undo_block(uint32 segment_id, uint8 owner_instance, uint32 block_no, char *buf)
{
	ClusterUndoBufPin pin;
	const char *img;

	/*
	 * spec-3.18 D1:  read-through the undo buffer pool for DATA blocks.  A NULL
	 * pin means block 0 (not poolable) or the pool is disabled — fall back to
	 * the direct smgr read.  On a hit/miss-fill the pool owns the disk read.
	 */
	img = cluster_undo_buf_pin(segment_id, owner_instance, block_no, CLUSTER_UNDO_BUF_SHARED, &pin);
	if (img == NULL)
		return cluster_undo_smgr_read_block(cluster_undo_intent_for_owner(owner_instance),
											segment_id, owner_instance, block_no, buf);
	memcpy(buf, img, BLCKSZ);
	cluster_undo_buf_unpin(&pin);
	return true;
}


/*
 * keep_clean (spec-3.25 D1b): install the bytes into the pool image so READERS
 * see them immediately, but do NOT mark the slot dirty -- its WAL is deferred
 * to the merged-record flush, and an un-WAL'd dirty block must never reach
 * disk (write-back / checkpoint flush only dirty slots).  ref_slot_out, when
 * non-NULL, additionally takes a window residency reference (eviction gate)
 * while the EXCLUSIVE pin is still held and reports the slot index.
 */
static bool
write_undo_block_ext(uint32 segment_id, uint8 owner_instance, uint32 block_no, const char *buf,
					 bool do_fsync, XLogRecPtr wal_lsn, bool keep_clean, int *ref_slot_out)
{
	ClusterUndoBufPin pin;
	char *img;

	/*
	 * spec-3.18 D1:  do_fsync is only set for block-0 segment-header writes
	 * (cluster_undo_alloc), which are not poolable;  DATA-block record writes
	 * are always do_fsync=false (precommit flush owns the fsync).  So a
	 * do_fsync request goes straight to the direct smgr write.
	 */
	if (do_fsync)
		return cluster_undo_smgr_write_block(cluster_undo_intent_for_owner(owner_instance),
											 segment_id, owner_instance, block_no, buf, true);

	/*
	 * Write-through the pool for DATA blocks:  update the cached image and
	 * pwrite (do_fsync=false) so durability is identical to today while reads
	 * stay coherent.  NULL pin (block 0 / pool disabled) -> direct write.
	 */
	img = cluster_undo_buf_pin(segment_id, owner_instance, block_no, CLUSTER_UNDO_BUF_EXCLUSIVE,
							   &pin);
	if (img == NULL) {
		/*
		 * spec-3.25 D1b: a keep_clean install has no WAL yet -- a direct disk
		 * write here would put data ahead of its WAL.  Unreachable in practice
		 * (the deferral path requires writeback_allowed() => pool enabled);
		 * fail closed if it ever is.
		 */
		if (keep_clean)
			return false;
		return cluster_undo_smgr_write_block(cluster_undo_intent_for_owner(owner_instance),
											 segment_id, owner_instance, block_no, buf, false);
	}

	/*
	 * PG_FINALLY guarantees the EXCLUSIVE pin + content lock are released even
	 * if mark_dirty raises (it can ereport on a write-through pwrite failure,
	 * or the write-back monotone-LSN guard).  Without it an ERROR would leave
	 * the slot pinned + content-locked, deadlocking later readers/writers.
	 */
	PG_TRY();
	{
		memcpy(img, buf, BLCKSZ);
		/* wal_lsn = the XLOG_UNDO_BLOCK_WRITE LSN (spec-3.18 D2): write-through
		 * (gate off) writes now; write-back (gate on) defers behind this LSN.
		 * keep_clean (spec-3.25 D1b): readers see the bytes, dirty + WAL come
		 * at the merged-record flush. */
		if (!keep_clean)
			cluster_undo_buf_mark_dirty(&pin, wal_lsn);
		if (ref_slot_out != NULL && *ref_slot_out < 0) {
			cluster_undo_buf_addref(&pin);
			*ref_slot_out = pin.slot;
		}
	}
	PG_FINALLY();
	{
		cluster_undo_buf_unpin(&pin);
	}
	PG_END_TRY();
	return true;
}

/* Compatibility wrapper: the pre-D1b call shape. */
static bool
write_undo_block(uint32 segment_id, uint8 owner_instance, uint32 block_no, const char *buf,
				 bool do_fsync, XLogRecPtr wal_lsn)
{
	return write_undo_block_ext(segment_id, owner_instance, block_no, buf, do_fsync, wal_lsn, false,
								NULL);
}


/*
 * cluster_undo_wal_protect_block (spec-3.18 D2)
 *	  WAL-protect + persist one undo DATA block (block_no >= 1).  block_buf is
 *	  the full new image (block_lsn still old);  old_block_lsn / rec_off /
 *	  rec_len / slot_off describe the change for the FPI-vs-delta decision.
 *
 *	  Gate off (D2a): emit always-FPI then write-through, no DELAY_CHKPT_START.
 *	  Gate on (D2b): hold DELAY_CHKPT_START across the decision + XLogInsert +
 *	  block_lsn stamp + (write-back) mark_dirty, so a checkpoint that adopts a
 *	  redo point past old_block_lsn cannot complete until our block reaches
 *	  disk via the checkpoint-flush set -- closing the FPW race (§2.6 v0.8).
 *	  PG_FINALLY clears the flag on the error path (a real guard, not Assert).
 *	  Returns false on a block-write I/O failure (caller fail-closes).
 */
static bool
cluster_undo_wal_protect_block(uint32 segment_id, uint8 owner_instance, uint32 block_no,
							   char *block_buf, XLogRecPtr old_block_lsn, uint16 rec_off,
							   uint16 rec_len, uint16 slot_off)
{
	UndoBlockHeader *blkhdr = (UndoBlockHeader *)block_buf;
	XLogRecPtr lsn;
	int saved_delay_flags;
	bool ok = false; /* assigned in PG_TRY; init silences cppcheck uninitvar */

	if (!cluster_undo_buf_writeback_allowed()) {
		/* D2a: always-FPI, write-through (no checkpoint-race window). */
		lsn = cluster_undo_emit_block_write(owner_instance, segment_id, block_no, block_buf,
											old_block_lsn, rec_off, rec_len, slot_off);
		blkhdr->block_lsn = lsn;
		return write_undo_block(segment_id, owner_instance, block_no, block_buf,
								/* do_fsync = */ false, lsn);
	}

	/*
	 * D2b: DELAY_CHKPT_START spans the decision + insert + block write.  Save
	 * and restore the backend-local flags instead of blindly clearing START, so
	 * a future caller cannot accidentally lose an outer delay-checkpoint flag.
	 * The current undo-write path should not be nested inside another START
	 * window; keep that contract visible in assert builds.
	 */
	saved_delay_flags = MyProc->delayChkptFlags;
	Assert((saved_delay_flags & DELAY_CHKPT_START) == 0);
	MyProc->delayChkptFlags = saved_delay_flags | DELAY_CHKPT_START;
	PG_TRY();
	{
		lsn = cluster_undo_emit_block_write(owner_instance, segment_id, block_no, block_buf,
											old_block_lsn, rec_off, rec_len, slot_off);
		blkhdr->block_lsn = lsn;
		ok = write_undo_block(segment_id, owner_instance, block_no, block_buf,
							  /* do_fsync = */ false, lsn);
	}
	PG_FINALLY();
	{
		MyProc->delayChkptFlags = saved_delay_flags;
	}
	PG_END_TRY();
	return ok;
}

/*
 * cluster_undo_pending_flush_internal -- spec-3.25 D1b flush point.
 *
 *	Emit ONE XLOG_UNDO_BLOCK_WRITE_MULTI covering every record this xact
 *	appended to the pending block, then install the image (pool copy-in +
 *	mark_dirty with the new LSN via write_undo_block) -- the only point the
 *	shared image and dirty state advance, so WAL-before-data holds by
 *	construction.  Mirrors cluster_undo_wal_protect_block's two modes:
 *	write-back holds DELAY_CHKPT_START across decision+insert+install;
 *	write-through (GUC flipped off mid-window) drains always-FPI style.
 *
 *	error_on_fail=true (commit/prepare): failure ereport(ERROR)s -- never a
 *	silent half-durable commit.  false (block-switch: caller maps to
 *	InvalidUba; abort: caller drops the pending image).
 */
static bool
cluster_undo_pending_flush_internal(bool error_on_fail)
{
	ClusterUndoPendingBlock *p = &cluster_undo_pending;
	UndoBlockHeader *blkhdr;
	XLogRecPtr lsn;
	uint16 slot_off;
	uint16 slot_len;
	bool ok = false; /* assigned below; init silences cppcheck uninitvar */

	if (!p->active)
		return true;

	Assert(p->nrecords > 0);
	Assert(p->rec_hi > p->rec_lo);
	Assert(p->slot_min_off <= p->slot_max_off);
	blkhdr = (UndoBlockHeader *)p->buf;
	slot_off = p->slot_min_off;
	slot_len = (uint16)(p->slot_max_off - p->slot_min_off + sizeof(UndoSlotDirEntry));

	if (!cluster_undo_buf_writeback_allowed()) {
		lsn = cluster_undo_emit_block_write_multi(
			p->owner_instance, p->segment_id, p->block_no, p->buf, p->old_block_lsn, p->rec_lo,
			(uint16)(p->rec_hi - p->rec_lo), slot_off, slot_len);
		blkhdr->block_lsn = lsn;
		ok = write_undo_block(p->segment_id, p->owner_instance, p->block_no, p->buf,
							  /* do_fsync = */ false, lsn);
	} else {
		int saved_delay_flags = MyProc->delayChkptFlags;
		Assert((saved_delay_flags & DELAY_CHKPT_START) == 0);
		MyProc->delayChkptFlags = saved_delay_flags | DELAY_CHKPT_START;
		PG_TRY();
		{
			lsn = cluster_undo_emit_block_write_multi(
				p->owner_instance, p->segment_id, p->block_no, p->buf, p->old_block_lsn, p->rec_lo,
				(uint16)(p->rec_hi - p->rec_lo), slot_off, slot_len);
			blkhdr->block_lsn = lsn;
			ok = write_undo_block(p->segment_id, p->owner_instance, p->block_no, p->buf,
								  /* do_fsync = */ false, lsn);
		}
		PG_FINALLY();
		{
			MyProc->delayChkptFlags = saved_delay_flags;
		}
		PG_END_TRY();
	}

	if (ok) {
		if (p->ref_slot >= 0) {
			cluster_undo_buf_unref_slot(p->ref_slot);
			p->ref_slot = -1;
		}
		p->active = false;
		if (UndoRecordShared != NULL)
			pg_atomic_fetch_add_u64(&UndoRecordShared->block_write_count, 1);
	} else if (error_on_fail) {
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("cluster undo deferred block write failed (segment %u block %u)",
						p->segment_id, p->block_no),
				 errhint("The merged undo WAL record could not be installed before commit.")));
	}
	return ok;
}


/*
 * spec-3.18 D3:  result of an extent claim.  CLAIM_OK fills *ext;  the error
 * results map to the same SQLSTATEs the pre-D3 cursor path raised, but the
 * ereport is done by the caller OUTSIDE lifecycle_lock (the claim releases the
 * lock before returning any error).
 */
typedef enum UndoExtentClaimResult {
	CLAIM_OK = 0,
	CLAIM_HARD_CAP, /* 53R9E: segment pool hard cap */
	CLAIM_FS_FAIL,	/* 53R9D: autoextend FS error / timeout */
	CLAIM_IO_FAIL	/* block-0 header read/write failed -> InvalidUba */
} UndoExtentClaimResult;

/*
 * claim_undo_extent -- spec-3.18 D3 (mini-plan §1).
 *
 *	Claim a run of undo data blocks (cluster.undo_extent_blocks) for this
 *	backend's exclusive lock-free use, touching lifecycle_lock exactly once
 *	(vs the pre-D3 per-record cursor_lock).  Under lifecycle_lock:
 *	  - select the active segment (ensured_segment_id on first claim);
 *	  - resume the high-water from the segment bitmap when the shmem cache is
 *	    cold (B1 restart resume -- never reset to block 1 over existing data);
 *	  - autoextend (reuse-first; retention rollover inside) when the segment is
 *	    full, marking the old segment FULL + activating the new one;
 *	  - batch-mark the claimed range used (A1, one block-0 RMW + fsync);
 *	  - advance the shared high-water + publish active_segment_id.
 *
 *	On success *ext is a held extent parked at its first (fresh) block.  Error
 *	returns release the lock first (caller ereports);  ERROR thrown by lower
 *	file/WAL allocation paths is cleanup-rethrown after releasing the lock.
 *	Activation mirrors the pre-D3 path: mark_active + tail_block_init(1)
 *	whenever a segment becomes active (tail_block=1 is the conservative
 *	retention base, safe on restart resume).
 */
static UndoExtentClaimResult
claim_undo_extent(ClusterUndoExtent *ext, uint8 owner_instance, uint32 ensured_segment_id,
				  SCN current_scn)
{
	uint32 seg;
	uint32 hw;
	uint32 n;
	uint32 expected_active_segment;
	uint32 expected_next_extent;
	bool needs_activation;

	LWLockAcquire(&UndoRecordShared->cursor_lock.lock, LW_EXCLUSIVE);
	LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);

claim_retry_locked:
	seg = UndoRecordShared->active_segment_id;
	needs_activation = (seg == 0); /* first claim ever, or post-restart resume */
	if (seg == 0)
		seg = ensured_segment_id;

	/* High-water: steady-state shmem cache, else B1 rebuild from the bitmap. */
	hw = UndoRecordShared->next_extent_block;
	if (hw == 0)
		hw = cluster_undo_segment_first_free_block(seg, owner_instance); /* 0 => full/corrupt */

	n = (hw == 0) ? 0
				  : cluster_undo_extent_compute(hw, (uint32)cluster_undo_extent_blocks,
												UNDO_BLOCKS_PER_SEGMENT, ext);

	if (n == 0) {
		/* Segment full / corrupt bitmap -> select fresh or exact reuse. */
		ClusterUndoSegmentExtendPlan extend_plan;
		uint32 old_seg = seg;
		uint32 new_seg;
		uint32 reused = 0;
		bool selected = false;

		/*
		 * Attribute the candidate's header probes to the extent claim. First
		 * publication and reuse run separately after releasing lifecycle_lock;
		 * that lock's own wait is already attributed to its LWLock tranche.
		 */
		pgstat_report_wait_start(WAIT_EVENT_CLUSTER_UNDO_EXTENT_CLAIM);
		PG_TRY();
		{
			selected = cluster_undo_segment_extend_or_create(owner_instance, &extend_plan);
		}
		PG_CATCH();
		{
			pgstat_report_wait_end();
			/* ERROR resets InterruptHoldoffCount. The outer transaction
			 * handler releases LWLocks with LWLockReleaseAll, not retail
			 * LWLockRelease calls from this rethrow-only handler. */
			PG_RE_THROW();
		}
		PG_END_TRY();
		pgstat_report_wait_end();

		if (!selected) {
			if (extend_plan.at_hard_cap)
				pg_atomic_fetch_add_u64(&UndoRecordShared->segment_hard_cap_fail_count, 1);
			else
				pg_atomic_fetch_add_u64(&UndoRecordShared->segment_create_fail_count, 1);
			LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
			LWLockRelease(&UndoRecordShared->cursor_lock.lock);
			return extend_plan.at_hard_cap ? CLAIM_HARD_CAP : CLAIM_FS_FAIL;
		}
		new_seg = extend_plan.segment_id;
		if (extend_plan.needs_reuse || extend_plan.needs_provision) {
			/* 0xFB XCUR may wait or perform I/O.  Freeze the exact candidate
			 * under lifecycle_lock, then release it before the transition. */
			LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
			pgstat_report_wait_start(WAIT_EVENT_CLUSTER_UNDO_EXTENT_CLAIM);
			PG_TRY();
			{
				if (extend_plan.needs_provision) {
					cluster_undo_segment_allocate(new_seg, owner_instance);
					reused = new_seg;
				} else
					reused = cluster_undo_segment_reuse_in_place(new_seg, owner_instance,
																 extend_plan.generation);
			}
			PG_CATCH();
			{
				pgstat_report_wait_end();
				PG_RE_THROW();
			}
			PG_END_TRY();
			pgstat_report_wait_end();
			LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);

			/* A competing record claim may have installed the winner while
			 * XCUR was in flight.  Recompute from its current segment. */
			if (UndoRecordShared->active_segment_id != old_seg)
				goto claim_retry_locked;
			if (cluster_undo_segment_read_state(new_seg, owner_instance) == (uint8)SEGMENT_ACTIVE)
				goto claim_retry_locked;
			if (reused == 0
				|| (extend_plan.needs_provision
					&& cluster_undo_segment_generation(new_seg, owner_instance)
						   != extend_plan.generation)
				|| cluster_undo_segment_read_state(new_seg, owner_instance)
					   != (uint8)SEGMENT_ALLOCATED) {
				LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
				LWLockRelease(&UndoRecordShared->cursor_lock.lock);
				return CLAIM_FS_FAIL;
			}
		}
		pg_atomic_fetch_add_u64(&UndoRecordShared->autoextend_count, 1);
		pg_atomic_fetch_add_u64(&UndoRecordShared->segment_switch_count, 1);
		cluster_undo_record_observation_apply_locked(owner_instance);
		if (old_seg != 0 && old_seg != new_seg) {
			/*
			 * spec-4.12a D1: the record cursor just rolled away from old_seg.
			 * Seal it with the current SCN (a conservative upper bound on every
			 * record it holds; every in-flight writer registered first_undo_scn
			 * <= this) and try to advance it ACTIVE -> COMMITTED so the cleaner
			 * can reclaim it -- the leak fix.  cursor_lock remains the bounded
			 * local operation owner while lifecycle_lock is released for the
			 * exact block-0 current transition.  GUC off = legacy leak behaviour (does not gate the
			 * 8.A guard, only the optimization; see spec §2.3).  In-flight /
			 * prepared segments stay ACTIVE and the cleaner re-evaluates (D2).
			 */
			LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
			PG_TRY();
			{
				(void)cluster_undo_segment_mark_full(old_seg, owner_instance);
				if (cluster_undo_record_segment_commit_on_rollover)
					cluster_undo_try_mark_record_segment_committed_owned(old_seg, owner_instance,
																		 cluster_scn_current());
			}
			PG_CATCH();
			{
				PG_RE_THROW();
			}
			PG_END_TRY();
			LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);
			if (UndoRecordShared->active_segment_id != old_seg) {
				LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
				LWLockRelease(&UndoRecordShared->cursor_lock.lock);
				return CLAIM_IO_FAIL;
			}
		}
		seg = new_seg;
		needs_activation = true;
		hw = 1; /* fresh segment: data blocks from 1 */
		n = cluster_undo_extent_compute(hw, (uint32)cluster_undo_extent_blocks,
										UNDO_BLOCKS_PER_SEGMENT, ext);
		/* fresh segment has full room => n > 0 */
	}

	{
		bool mutation_ok = true;
		bool is_fresh = false;

		expected_active_segment = UndoRecordShared->active_segment_id;
		expected_next_extent = UndoRecordShared->next_extent_block;
		if (needs_activation)
			is_fresh
				= cluster_undo_segment_read_state(seg, owner_instance) == (uint8)SEGMENT_ALLOCATED;
		LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
		PG_TRY();
		{
			if (needs_activation) {
				/*
		 * review P1-C: init tail_block (the retention base) ONLY for a fresh
		 * (ALLOCATED) segment.  A restart-resumed segment is already ACTIVE on
		 * disk with a cleaner-advanced tail_block -- resetting it to 1 would
		 * make the cleaner re-scan + over-retain after every restart.
		 */
				mutation_ok = cluster_undo_segment_mark_active(seg, owner_instance)
							  && (!is_fresh
								  || cluster_undo_segment_tail_block_init(seg, owner_instance, 1));
			}

			/* A1: claim the range through the same exact current owner. */
			if (mutation_ok)
				mutation_ok
					= cluster_undo_segment_mark_block_range_used(seg, owner_instance, hw, n);
		}
		PG_CATCH();
		{
			PG_RE_THROW();
		}
		PG_END_TRY();
		if (!mutation_ok) {
			LWLockRelease(&UndoRecordShared->cursor_lock.lock);
			return CLAIM_IO_FAIL;
		}
		LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);
		if (UndoRecordShared->active_segment_id != expected_active_segment
			|| UndoRecordShared->next_extent_block != expected_next_extent) {
			LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
			LWLockRelease(&UndoRecordShared->cursor_lock.lock);
			return CLAIM_IO_FAIL;
		}
		if (needs_activation) {
			pg_atomic_fetch_add_u64(&UndoRecordShared->segment_claim_count, 1);
			UndoRecordShared->block_first_scn = current_scn;
		}
	}

	/* Publish: advance high-water + active segment. */
	UndoRecordShared->next_extent_block = hw + n;
	UndoRecordShared->active_segment_id = seg;
	ext->segment_id = seg;
	pg_atomic_fetch_add_u64(&UndoRecordShared->extent_claim_count, 1);

	LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
	LWLockRelease(&UndoRecordShared->cursor_lock.lock);
	return CLAIM_OK;
}


/*
 * Ensure that the actual record extent segment, rather than only the fixed
 * bootstrap segment, has a current block-zero resident publication before a
 * record can expose a UBA into it.  Autoextend can change ext->segment_id
 * behind cluster_undo_active_segment_for_node_or_create(); accepting its
 * segment-id-only cache left the new DATA segment's direct block0 slot EMPTY.
 *
 * The receipt remains non-authorizing.  Every reuse first rechecks its exact
 * admission/root/generation tuple.  A different segment or a failed recheck
 * clears the cache and runs the existing live-owner producer.
 * This helper is called with no lifecycle/content LWLock held.
 */
static bool
cluster_undo_record_ensure_block0_current(uint8 owner_instance, uint32 segment_id,
										  uint64 absolute_deadline_us,
										  ClusterUndoBlock0LiveOwnerPublication *out)
{
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0LiveOwnerPublication publication;
	TimestampTz now;
	int timeout_ms;

	if (segment_id == 0 || owner_instance < 1 || owner_instance > UNDO_OWNER_INSTANCE_MAX
		|| out == NULL)
		return false;
	memset(out, 0, sizeof(*out));

	if (cluster_undo_record_block0_publication_segment_id == segment_id
		&& cluster_undo_block0_current_live_owner_publication_recheck(
			&cluster_undo_record_block0_publication)) {
		*out = cluster_undo_record_block0_publication;
		return true;
	}

	now = GetCurrentTimestamp();
	if (absolute_deadline_us == 0 || absolute_deadline_us <= (uint64)now)
		return false;
	timeout_ms = (int)((absolute_deadline_us - (uint64)now + 999) / 1000);
	if (timeout_ms <= 0)
		return false;
	if (timeout_ms > CLUSTER_UNDO_RECORD_PREPARE_TIMEOUT_MS)
		timeout_ms = CLUSTER_UNDO_RECORD_PREPARE_TIMEOUT_MS;

	cluster_undo_record_block0_publication_segment_id = 0;
	memset(&cluster_undo_record_block0_publication, 0,
		   sizeof(cluster_undo_record_block0_publication));
	logical.owner_instance = owner_instance;
	logical.segment_id = segment_id;
	memset(&publication, 0, sizeof(publication));
	if (cluster_undo_block0_current_live_owner_ensure_resident_exact(&logical, timeout_ms,
																	 &publication)
			!= CLUSTER_UNDO_BLOCK0_OK
		|| !cluster_undo_block0_current_live_owner_publication_recheck(&publication))
		return false;

	cluster_undo_record_block0_publication = publication;
	cluster_undo_record_block0_publication_segment_id = segment_id;
	*out = publication;
	return true;
}


/*
 * cluster_undo_record_alloc -- record-level allocator main API.
 *
 *	Implementation outline:
 *	  1. Claim active segment if none(via existing segment-level API)
 *	  2. Lock cursor
 *	  3. Compute record total length;reject 53R9D if > inline_max GUC
 *	  4. If current block doesn't fit:
 *	     a. Flush current block (already happens per-record via fsync)
 *	     b. Advance current_block += 1, reset cursor
 *	     c. If exceed segment data blocks → 53R9D segment exhaustion
 *	  5. Read current block from segment file(or zeroed if first record)
 *	  6. Construct UndoRecordHeader + copy payload
 *	  7. Place record at free_offset;append slot dir at end
 *	  8. Update UndoBlockHeader(slot_count++, free_offset += rec_len)
 *	  9. Write block back to file
 *	  10. fsync segment file (durable per W2)
 *	  11. If this is a new segment, mark header ACTIVE + tail_block=1
 *	      after the durable record write
 *	  12. If this is a fresh block, mark free_block_bitmap used before
 *	      publishing cursor advance
 *	  13. Counter bumps
 *	  14. Unlock cursor
 *	  15. Mark backend touched
 *	  16. Encode UBA and return
 */
static UBA
cluster_undo_record_alloc_body(uint8 record_type, const ClusterUndoRecordTarget *target,
							   uint16 tt_slot_segment_id, uint16 tt_slot_offset,
							   const void *payload, uint16 payload_len, UBA prev_uba)
{
	UBA result;
	uint16 record_length;
	uint16 inline_cap;
	uint8 owner_instance;
	uint32 segment_id;
	uint32 current_block;
	uint32 free_offset;
	uint16 slot_count;
	uint16 new_slot_idx;
	/* cppcheck-suppress variableScope -- block_buf may alias this across the
	 * whole tail; declaring it inside the write-through branch would dangle. */
	char wt_block_buf[BLCKSZ]; /* write-through scratch (defer path uses pending.buf) */
	char *block_buf;
	bool use_defer;
	UndoBlockHeader *blkhdr;
	UndoRecordHeader *rechdr;
	UndoSlotDirEntry *slot;
	SCN current_scn;
	uint32 ensured_segment_id;
	int local_head_idx;
	UBA effective_prev_uba;
	bool first_in_tx;
	ClusterUndoBlock0LiveOwnerPublication record_publication;
	ClusterXpScope xps;

	/* PGRAC: spec-5.59 D7 profiling */
	cluster_xp_begin(&xps, CLXP_LOCAL_UNDO_ITL_WAL);

	/* Input validation. */
	if (record_type == UNDO_RECORD_INVALID || record_type > UNDO_RECORD_ITL) {
		cluster_xp_end(&xps); /* PGRAC: spec-5.59 D7 profiling */
		return InvalidUba;
	}
	if (target == NULL || payload == NULL) {
		cluster_xp_end(&xps); /* PGRAC: spec-5.59 D7 profiling */
		return InvalidUba;
	}

	if (UndoRecordShared == NULL) {
		cluster_xp_end(&xps); /* PGRAC: spec-5.59 D7 profiling */
		return InvalidUba;	  /* shmem not initialized */
	}

	record_length = undo_record_total_length(record_type, payload_len);

	/* Enforce inline cap GUC. */
	inline_cap = (uint16)cluster_undo_record_inline_max_bytes;
	if (payload_len > inline_cap) {
		cluster_xp_end(&xps); /* PGRAC: spec-5.59 D7 profiling */
		return InvalidUba;	  /* caller ereport 53R9D oversize */
	}

	/* Hard cap (must fit in single block). */
	if (record_length > UNDO_RECORD_HARD_CAP_BYTES) {
		cluster_xp_end(&xps); /* PGRAC: spec-5.59 D7 profiling */
		return InvalidUba;
	}

	/* Owner instance is current node_id + 1 (per cluster_undo_alloc.c
	 * convention: owner_instance is 1-indexed). */
	owner_instance = (uint8)(cluster_node_id + 1);

	/*
	 * Segment creation may ereport(ERROR), emit WAL, and perform filesystem I/O.
	 * Do it before taking the record cursor LWLock; otherwise an ERROR would
	 * leave the LWLock held and wedge all future undo writers in this backend.
	 */
	ensured_segment_id = cluster_undo_active_segment_for_node_or_create(cluster_node_id);
	if (ensured_segment_id == 0) {
		cluster_xp_end(&xps); /* PGRAC: spec-5.59 D7 profiling */
		return InvalidUba;
	}
	cluster_undo_record_observation_ensure();

	/*
	 * spec-3.8 Fix 4: post-restart resume.  If shared cursor is fresh
	 * (active_segment_id == 0) but on-disk pool already has higher segments
	 * from a previous incarnation's autoextend, resume to the highest
	 * existing segment instead of overwriting segment 1.  Cheap probe:
	 * BasicOpenFile + close in a tight loop, bounded by
	 * CLUSTER_UNDO_SEGS_PER_INSTANCE.
	 *
	 * Lock-free check is acceptable here: we are still pre-cursor_lock,
	 * and the worst case (race with another resuming backend) is that we
	 * both compute the same max -- the cursor_lock-protected first-claim
	 * block below picks one winner via active_segment_id publication.
	 */
	if (UndoRecordShared->active_segment_id == 0) {
		/*
		 * spec-3.18 D3.2 (review finding 2): resume to the LIVE active segment
		 * (SEGMENT_ACTIVE, not FULL), not merely the highest-numbered file --
		 * reuse-first can make the active a low-numbered reborn slot while a
		 * high-numbered one is COMMITTED / RECYCLABLE / ACTIVE-FULL.  The
		 * claim's B1 first_free_block then rebuilds the high-water from that
		 * segment's bitmap.  If none is resumable (or ambiguous), keep the
		 * ensured/created segment and let the claim activate / autoextend it.
		 */
		uint32 resumed = cluster_undo_segment_scan_resumable_active(owner_instance);
		if (resumed != 0)
			ensured_segment_id = resumed;
	}

	if (!cluster_undo_local_head_ensure(tt_slot_segment_id, tt_slot_offset, &local_head_idx)) {
		cluster_xp_end(&xps); /* PGRAC: spec-5.59 D7 profiling */
		return InvalidUba;
	}
	effective_prev_uba = cluster_undo_local_heads[local_head_idx].head;
	if (UBA_is_invalid(effective_prev_uba) && !UBA_is_invalid(prev_uba)) {
		uint32 prev_segment;
		uint32 prev_block;
		uint16 prev_tt_off;
		uint16 prev_row_off;

		/* Accept caller-supplied prev_uba only if it points to an actual undo
		 * record.  TT-only UBAs from spec-3.4b have block_no == 0 and must not
		 * be written into the undo chain. */
		if (uba_decode(prev_uba, &prev_segment, &prev_block, &prev_tt_off, &prev_row_off)
			&& prev_block != 0)
			effective_prev_uba = prev_uba;
	}
	first_in_tx = UBA_is_invalid(effective_prev_uba);

	/*
	 * spec-4.12a D1 (R1c happens-before): register this top-transaction's
	 * first_undo_scn in the active-write boundary registry BEFORE advancing the
	 * SCN for this record and BEFORE the residual-extent reuse / undo block
	 * write below.  first_undo_scn = current SCN <= every write_scn this xact
	 * stamps, so any record segment sealed at >= this value is retained until
	 * this writer resolves (8.A guard 1).  Idempotent per top-xact.
	 */
	cluster_undo_active_write_register(cluster_scn_current());

	/* SCN stamp for write_scn (advance Lamport). */
	current_scn = cluster_scn_advance();

	/*
	 * spec-3.18 D3:  obtain a writable block from this backend's extent.  The
	 * extent's blocks are private to us, so per-record writes take NO shared
	 * lock (only an extent claim touches lifecycle_lock);  per-block content is
	 * serialized by the D1 pool's content_lock inside write_undo_block.
	 *
	 *   current block has room          -> write there
	 *   current block full, extent more -> advance backend-local cursor (fresh)
	 *   extent NONE / exhausted         -> claim a new extent (lifecycle_lock)
	 */
	{
		ClusterUndoExtent *ext = &cluster_undo_current_extent;

		/*
		 * spec-3.18 D3.3 (Q2 hybrid): a residual extent carried over from a
		 * previous transaction is reusable ONLY while its segment is still the
		 * active one.  Once the allocator rolled over (segment_id != active), the
		 * old segment may be FULL / sealed / COMMITTED / RECYCLABLE / reborn, so
		 * the residual must be dropped and re-claimed.
		 *
		 * spec-4.12a Hardening v1.0.1 (P0 8.A): this validation MUST run under
		 * lifecycle_lock.  The pre-Hardening check read active_segment_id RELAXED
		 * (unlocked); a stale read could approve reusing a residual whose segment
		 * another backend already sealed + rolled away, and 4.12a can then drain
		 * that segment ACTIVE -> COMMITTED while this txn still writes undo into it
		 * (the residual writes carry a write_scn above the segment's stale seal, so
		 * the drain gate wrongly judges it idle -> false reclaim of live undo).  The
		 * rollover seals the old segment AND publishes active_segment_id under the
		 * same lifecycle_lock, so acquiring it here makes active_segment_id the
		 * authoritative truth source (cluster_undo_residual_reusable() then folds
		 * in the "active segment is never sealed/rolled/recyclable" invariant --
		 * no block-0 I/O needed).
		 *
		 * Validate ONCE per top-xact (the carried residual is only stale at the
		 * first alloc; a within-xact freshly-claimed extent is claimed under this
		 * same lock, so it is current-xact-owned and reused lock-free).  Only this
		 * check needs the lock; the per-record writes below stay lock-free.
		 * Soundness of validate-then-write-lock-free: if the residual is confirmed
		 * active here, any later rollover seals it at an SCN >= this txn's
		 * first_undo_scn (SCN is monotonic; the seal is taken after this point), so
		 * seal >= boundary -> the drain gate retains it while this writer is
		 * in-flight.
		 */
		if (!cluster_undo_residual_validated_this_xact) {
			if (ext->segment_id != CLUSTER_UNDO_EXTENT_NONE) {
				bool reusable;

				LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);
				reusable = cluster_undo_residual_reusable(ext->segment_id,
														  UndoRecordShared->active_segment_id);
				LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
				if (!reusable) {
					ext->segment_id = CLUSTER_UNDO_EXTENT_NONE; /* stale -> re-claim */
					pg_atomic_fetch_add_u64(&UndoRecordShared->record_seg_residual_revalidate_drops,
											1);
				}
			}
			cluster_undo_residual_validated_this_xact = true;
		}

		for (;;) {
			if (!cluster_undo_extent_exhausted(ext)
				&& cluster_undo_block_has_space(ext->cur_free_offset, ext->cur_slot_count,
												record_length))
				break; /* current block has room for this record */

			if (!cluster_undo_extent_exhausted(ext)) {
				/* Current block full but the extent has more blocks. */
				cluster_undo_extent_next_block(ext);
				continue; /* a fresh block always fits one <= hard-cap record */
			}

			/* No usable extent -> claim one (the only lifecycle_lock touch). */
			switch (claim_undo_extent(ext, owner_instance, ensured_segment_id, current_scn)) {
			case CLAIM_OK:
				break; /* held at a fresh first block;  re-loop confirms space */
			case CLAIM_HARD_CAP:
				cluster_undo_cleaner_wakeup(); /* Q8: every recyclable segment counts */
				ereport(ERROR, (errcode(ERRCODE_CLUSTER_UNDO_SEGMENTS_HARD_CAP_REACHED),
								errmsg("cluster undo segment pool hard cap reached for instance %u",
									   (unsigned)(cluster_node_id + 1)),
								errhint("Increase cluster.undo_segments_max_per_instance "
										"(current limit reached);  end the long-running reader "
										"holding the retention horizon, or wait for the spec-3.13 "
										"cleaner to reclaim recyclable segments.")));
				break; /* unreachable (ereport does not return) */
			case CLAIM_FS_FAIL:
				cluster_undo_cleaner_wakeup();
				ereport(ERROR, (errcode(ERRCODE_CLUSTER_UNDO_RECORD_INVALID_UBA),
								errmsg("cluster undo segment autoextend failed "
									   "(filesystem error or timeout)"),
								errhint("Check disk space on $PGDATA/pg_undo and "
										"cluster.undo_segment_create_timeout_ms.")));
				break; /* unreachable */
			case CLAIM_IO_FAIL:
				cluster_xp_end(&xps); /* PGRAC: spec-5.59 D7 profiling */
				return InvalidUba;	  /* block-0 header I/O fail */
			}
		}

		segment_id = ext->segment_id;
		current_block = ext->cur_block;
		free_offset = ext->cur_free_offset;
		slot_count = ext->cur_slot_count;
	}
	if (!cluster_undo_record_ensure_block0_current(owner_instance, segment_id,
												   cluster_undo_record_prepare_deadline_us(),
												   &record_publication)) {
		cluster_xp_end(&xps);
		return InvalidUba;
	}

	/*
	 * spec-3.25 D1b: choose the deferred-merge path.  A pending image for THIS
	 * block keeps draining even if the GUC flipped off mid-window (the cursor
	 * state describes pending.buf, not the stale pool copy).
	 */
	use_defer = cluster_undo_buf_writeback_allowed()
				|| (cluster_undo_pending.active && cluster_undo_pending.segment_id == segment_id
					&& cluster_undo_pending.owner_instance == owner_instance
					&& cluster_undo_pending.block_no == current_block);

	if (use_defer && cluster_undo_pending.active
		&& (cluster_undo_pending.segment_id != segment_id
			|| cluster_undo_pending.owner_instance != owner_instance
			|| cluster_undo_pending.block_no != current_block)) {
		/* Block/segment switch: emit the previous block's merged record. */
		if (!cluster_undo_pending_flush_internal(false)) {
			cluster_xp_end(&xps); /* PGRAC: spec-5.59 D7 profiling */
			return InvalidUba;
		}
	}

	if (use_defer && cluster_undo_pending.active) {
		/* Same (xact, block): keep appending into the pending image. */
		block_buf = cluster_undo_pending.buf;
		blkhdr = (UndoBlockHeader *)block_buf;
	} else {
		block_buf = use_defer ? cluster_undo_pending.buf : wt_block_buf;

		/* Read current block (or zeroed if first write to this block). */
		if (slot_count == 0) {
			/* Fresh block — init zeroed buffer + header. */
			memset(block_buf, 0, BLCKSZ);
			blkhdr = (UndoBlockHeader *)block_buf;
			blkhdr->magic = PGRAC_UNDO_BLOCK_MAGIC;
			blkhdr->block_version = UNDO_BLOCK_VERSION_1;
			blkhdr->slot_count = 0;
			blkhdr->free_offset = sizeof(UndoBlockHeader);
			blkhdr->first_change_scn = current_scn;
			blkhdr->first_change_lsn = GetXLogWriteRecPtr();
			blkhdr->crc64 = 0;
		} else {
			/* Mid-extent block we already wrote -> read it back from the pool. */
			if (!read_undo_block(segment_id, owner_instance, current_block, block_buf)) {
				cluster_xp_end(&xps); /* PGRAC: spec-5.59 D7 profiling */
				return InvalidUba;	  /* I/O fail (no shared lock held in D3 path) */
			}
			blkhdr = (UndoBlockHeader *)block_buf;
		}

		if (use_defer) {
			/* Start a pending window for this block. */
			cluster_undo_pending.active = true;
			cluster_undo_pending.owner_instance = owner_instance;
			cluster_undo_pending.segment_id = segment_id;
			cluster_undo_pending.block_no = current_block;
			cluster_undo_pending.old_block_lsn = blkhdr->block_lsn;
			cluster_undo_pending.rec_lo = (uint16)free_offset;
			cluster_undo_pending.rec_hi = (uint16)free_offset;
			cluster_undo_pending.slot_min_off = PG_UINT16_MAX;
			cluster_undo_pending.slot_max_off = 0;
			cluster_undo_pending.nrecords = 0;
			cluster_undo_pending.ref_slot = -1;
		}
	}

	/* Construct UndoRecordHeader at free_offset. */
	rechdr = (UndoRecordHeader *)(block_buf + free_offset);
	memset(rechdr, 0, sizeof(UndoRecordHeader));
	rechdr->record_type = record_type;
	rechdr->flags = first_in_tx ? UNDO_REC_FLAG_FIRST_IN_TX : 0;
	rechdr->payload_length = payload_len;
	rechdr->xid = GetCurrentTransactionIdIfAny();
	rechdr->origin_node_id = (uint16)cluster_node_id;
	rechdr->tt_slot_segment_id = tt_slot_segment_id;
	rechdr->tt_slot_id = cluster_tt_slot_offset_to_id(tt_slot_offset);

	/*
	 * spec-4.5a G4 (F3): record the bound TT slot's reuse generation
	 * (+1; 0 stays "unknown" for the memset default and for any path
	 * without a same-slot binding).  Readers turn it into expected_wrap
	 * so a durable slot recycled to a same-valued wrapped xid never
	 * matches.  The peek is the writer's own backend-local binding; the
	 * segment/offset cross-check keeps a stale or foreign binding from
	 * stamping the wrong generation.
	 */
	{
		uint32 bind_seg;
		uint16 bind_off;
		uint32 bind_tt_id;
		uint32 bind_epoch;
		uint16 bind_wrap;

		if (cluster_tt_local_peek_binding(rechdr->xid, &bind_seg, &bind_off, &bind_tt_id,
										  &bind_epoch, &bind_wrap)
			&& bind_seg == (uint32)tt_slot_segment_id && bind_off == tt_slot_offset)
			rechdr->tt_wrap_plus1 = (uint16)(bind_wrap + 1);
	}
	rechdr->write_scn = current_scn;
	rechdr->prev_uba = effective_prev_uba;
	rechdr->target_locator = target->locator;
	rechdr->target_fork = target->forknum;
	rechdr->target_block = target->blockno;
	rechdr->target_offset = target->offnum;

	/* Copy op-specific payload bytes after the header. */
	memcpy(block_buf + free_offset + sizeof(UndoRecordHeader), payload, payload_len);

	/* Append slot directory entry. */
	new_slot_idx = slot_count;
	slot = UNDO_SLOT_DIR_PTR(block_buf, new_slot_idx);
	slot->record_offset = free_offset;
	slot->record_length = record_length;
	slot->record_type = record_type;
	slot->flags = rechdr->flags;

	/* Update block header. */
	blkhdr->slot_count = (uint16)(slot_count + 1);
	blkhdr->free_offset = free_offset + record_length;

	/*
	 * spec-3.18 D2: WAL-protect + persist this undo data block.  The appended
	 * record sits at [free_offset, free_offset + record_length) and its slot
	 * dir entry at UNDO_SLOT_DIR_OFFSET(new_slot_idx) -- those are the 3-range
	 * delta the protector emits when write-back is on (else a full image).
	 * blkhdr->block_lsn still holds the block's PREVIOUS page-LSN, which drives
	 * the FPI-on-first-touch decision; the protector overwrites it with the new
	 * record LSN.  spec-3.18 D3:  the block is private to this backend's extent
	 * (no cursor_lock);  the D1 pool's per-block content_lock serializes the
	 * pool slot, and the block was marked used + segment activated at claim time
	 * (NOT here -- mark_block_range_used precedes any record write, the B1
	 * marked >= has-data invariant).  Write-through + precommit fsync (gate off)
	 * or write-back + checkpoint-flush (gate on) are chosen inside it.
	 */
	Assert(current_block >= 1); /* data blocks only; block 0 is the segment header */
	if (use_defer) {
		/*
		 * spec-3.25 D1b: extend the pending spans instead of emitting WAL per
		 * record.  block_write_count is bumped at the flush (its semantics is
		 * now per-EMISSION; record_alloc_count / block_write_count = the
		 * bundle factor).
		 */
		uint16 this_slot_off = (uint16)UNDO_SLOT_DIR_OFFSET(new_slot_idx);

		cluster_undo_pending.rec_hi = (uint16)(free_offset + record_length);
		if (this_slot_off < cluster_undo_pending.slot_min_off)
			cluster_undo_pending.slot_min_off = this_slot_off;
		if (this_slot_off > cluster_undo_pending.slot_max_off)
			cluster_undo_pending.slot_max_off = this_slot_off;
		cluster_undo_pending.nrecords++;

		/*
		 * Mirror the bytes into the pool image NOW (keep_clean: readers --
		 * e.g. a concurrent CR walking this still-open xact's chain -- see
		 * the record immediately, exactly as pre-D1b), take the window
		 * residency reference on the first record, and leave dirty + WAL to
		 * the merged flush.
		 */
		if (!write_undo_block_ext(segment_id, owner_instance, current_block, block_buf,
								  /* do_fsync = */ false, InvalidXLogRecPtr,
								  /* keep_clean = */ true, &cluster_undo_pending.ref_slot)) {
			cluster_xp_end(&xps); /* PGRAC: spec-5.59 D7 profiling */
			return InvalidUba;
		}
	} else {
		if (!cluster_undo_wal_protect_block(segment_id, owner_instance, current_block, block_buf,
											blkhdr->block_lsn, (uint16)free_offset,
											(uint16)record_length,
											(uint16)UNDO_SLOT_DIR_OFFSET(new_slot_idx))) {
			cluster_xp_end(&xps); /* PGRAC: spec-5.59 D7 profiling */
			return InvalidUba;	  /* I/O fail (no shared lock held in D3 path) */
		}

		pg_atomic_fetch_add_u64(&UndoRecordShared->block_write_count, 1);
	}
	/* block_flush_count is no longer bumped per-record: fsync is deferred to the
	 * per-xact precommit flush (P0 perf hardening). */

	/*
	 * spec-3.18 D3:  advance the BACKEND-LOCAL extent cursor (no shared publish;
	 * the shared high-water moved forward at claim time).  The old shared cursor
	 * fields (current_block / free_offset / slot_count) are left for the
	 * undo_extent_blocks=1 / pre-D3 observability path and updated best-effort.
	 */
	cluster_undo_current_extent.cur_free_offset = free_offset + record_length;
	cluster_undo_current_extent.cur_slot_count = (uint16)(slot_count + 1);

	pg_atomic_fetch_add_u64(&UndoRecordShared->record_alloc_count, 1);

	/* Mark transaction-local undo state as touched. */
	cluster_undo_touched_in_xact = true;

	/* P0 perf hardening: record the dirtied undo segment for a single per-xact
	 * precommit fsync.  Done OUTSIDE cursor_lock (released above). */
	cluster_undo_record_touched_segment(segment_id, owner_instance);

	/* Encode UBA per spec-3.4b: (segment_id, block_no, tt_slot_offset, row_offset).
	 * For undo records, row_offset = slot-dir index within block. */
	result = uba_encode((uint32)segment_id, current_block, tt_slot_offset, new_slot_idx);
	cluster_undo_local_heads[local_head_idx].head = result;

	cluster_xp_end(&xps); /* PGRAC: spec-5.59 D7 profiling */
	return result;
}


static bool
cluster_undo_record_writable_admission(void)
{
	ClusterJoinGateVerdict verdict = cluster_reconfig_self_join_gate_verdict();

	if (verdict == CLUSTER_JOIN_GATE_BLOCK_53R61)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_JOIN_REJECTED_STALE),
						errmsg("cannot allocate undo: this node's cluster join was rejected"),
						errhint("Restart this node so it presents a fresh cluster incarnation.")));
	return verdict == CLUSTER_JOIN_GATE_ALLOW;
}


uint64
cluster_undo_record_prepare_deadline_us(void)
{
	TimestampTz now = GetCurrentTimestamp();

	if (now <= 0
		|| now > PG_INT64_MAX - (CLUSTER_UNDO_RECORD_PREPARE_TIMEOUT_MS * INT64CONST(1000)))
		return 0;
	return (uint64)(now + (CLUSTER_UNDO_RECORD_PREPARE_TIMEOUT_MS * INT64CONST(1000)));
}


static bool
cluster_undo_record_receipt_extent_matches(const ClusterUndoRecordPrepareReceipt *receipt)
{
	const ClusterUndoExtent *live = &cluster_undo_current_extent;
	const ClusterUndoExtent *frozen;

	if (receipt == NULL || receipt->magic != CLUSTER_UNDO_RECORD_RECEIPT_MAGIC
		|| !cluster_undo_record_reservation.active || receipt->reservation_sequence == 0
		|| receipt->reservation_sequence == PG_UINT64_MAX
		|| receipt->reservation_sequence != cluster_undo_record_reservation.sequence
		|| memcmp(receipt, &cluster_undo_record_reservation.receipt, sizeof(*receipt)) != 0)
		return false;
	frozen = &receipt->extent;
	return live->segment_id == frozen->segment_id && live->first_block == frozen->first_block
		   && live->nblocks == frozen->nblocks && live->cur_block == frozen->cur_block
		   && live->cur_free_offset == frozen->cur_free_offset
		   && live->cur_slot_count == frozen->cur_slot_count;
}


static void
cluster_undo_record_install_prepared_resident_locked(const ClusterUndoRecordPrepareReceipt *receipt,
													 const char image[BLCKSZ])
{
	ClusterUndoRecordReservation *reservation = &cluster_undo_record_reservation;

	if (receipt == NULL || image == NULL || !reservation->consume_locked
		|| reservation->ref_slot < 0)
		elog(PANIC, "cluster undo prepared consume reached install without its exact lock");
	cluster_undo_buf_install_ref_locked(reservation->ref_slot, receipt->actual_segment_id,
										receipt->owner_instance, receipt->extent.cur_block, image);
	cluster_undo_buf_unlock_ref(reservation->ref_slot);
	reservation->consume_locked = false;
}


ClusterUndoRecordPrepareResult
cluster_undo_record_prepare(uint8 record_type, uint16 payload_capacity, uint16 tt_slot_segment_id,
							uint16 tt_slot_offset, UBA prev_uba, uint64 absolute_deadline_us,
							ClusterUndoRecordPrepareReceipt *receipt)
{
	ClusterUndoRecordReservation *reservation = &cluster_undo_record_reservation;
	ClusterSemanticAdmissionToken modifier_admission = { 0 };
	ClusterSemanticAdmissionResult admission;
	ClusterUndoBlock0LiveOwnerPublication publication;
	ClusterUndoBufPin pin;
	ClusterUndoExtent *ext = &cluster_undo_current_extent;
	UndoBlockHeader *blkhdr;
	uint32 record_length;
	uint8 owner_instance;
	uint32 ensured_segment_id;
	char *resident;

	cluster_undo_receipt_metric_add(CLUSTER_UNDO_RECEIPT_PREPARE_START);
	cluster_undo_receipt_reason = "PREPARE_REFUSED";
	if (receipt == NULL)
		return CLUSTER_UNDO_RECORD_PREPARE_REFUSED;
	memset(receipt, 0, sizeof(*receipt));
	if (record_type == UNDO_RECORD_INVALID || record_type > UNDO_RECORD_ITL
		|| payload_capacity > (uint16)cluster_undo_record_inline_max_bytes
		|| UndoRecordShared == NULL || absolute_deadline_us == 0)
		return CLUSTER_UNDO_RECORD_PREPARE_REFUSED;
	if (absolute_deadline_us <= (uint64)GetCurrentTimestamp()) {
		cluster_undo_receipt_metric_add(CLUSTER_UNDO_RECEIPT_PREPARE_TIMEOUT);
		cluster_undo_receipt_reason = "PREPARE_TIMEOUT_BEFORE_READY";
		return CLUSTER_UNDO_RECORD_PREPARE_REFUSED;
	}
	record_length = undo_prepared_record_total_length(payload_capacity);
	if (record_length > UNDO_RECORD_HARD_CAP_BYTES) {
		cluster_undo_receipt_metric_add(CLUSTER_UNDO_RECEIPT_CAPACITY_REFUSAL);
		cluster_undo_receipt_reason = "CAPACITY_REFUSAL";
		return CLUSTER_UNDO_RECORD_PREPARE_REFUSED;
	}

	if (reservation->active)
		cluster_undo_record_cancel_prepared(&reservation->receipt);
	if (cluster_undo_record_reservation_floor == PG_UINT64_MAX)
		return CLUSTER_UNDO_RECORD_PREPARE_REFUSED;

	owner_instance = (uint8)(cluster_node_id + 1);
	ensured_segment_id = cluster_undo_active_segment_for_node_or_create(cluster_node_id);
	if (ensured_segment_id == 0)
		return CLUSTER_UNDO_RECORD_PREPARE_REFUSED;
	cluster_undo_record_observation_ensure();
	if (UndoRecordShared->active_segment_id == 0) {
		uint32 resumed = cluster_undo_segment_scan_resumable_active(owner_instance);

		if (resumed != 0)
			ensured_segment_id = resumed;
	}

	/* The registry is the existing retention guard.  Registration may precede
	 * an ultimately canceled DML reservation; transaction cleanup removes it. */
	cluster_undo_active_write_register(cluster_scn_current());
	if (!cluster_undo_residual_validated_this_xact) {
		if (ext->segment_id != CLUSTER_UNDO_EXTENT_NONE) {
			bool reusable;

			LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);
			reusable = cluster_undo_residual_reusable(ext->segment_id,
													  UndoRecordShared->active_segment_id);
			LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
			if (!reusable) {
				ext->segment_id = CLUSTER_UNDO_EXTENT_NONE;
				pg_atomic_fetch_add_u64(&UndoRecordShared->record_seg_residual_revalidate_drops, 1);
			}
		}
		cluster_undo_residual_validated_this_xact = true;
	}

	for (;;) {
		UndoExtentClaimResult claim_result;

		if (!cluster_undo_extent_exhausted(ext)
			&& cluster_undo_block_has_space(ext->cur_free_offset, ext->cur_slot_count,
											record_length))
			break;
		if (!cluster_undo_extent_exhausted(ext)) {
			cluster_undo_extent_next_block(ext);
			continue;
		}
		claim_result
			= claim_undo_extent(ext, owner_instance, ensured_segment_id, cluster_scn_current());
		if (claim_result == CLAIM_OK)
			continue;
		if (claim_result == CLAIM_HARD_CAP) {
			cluster_undo_receipt_metric_add(CLUSTER_UNDO_RECEIPT_CAPACITY_REFUSAL);
			cluster_undo_receipt_reason = "CAPACITY_REFUSAL";
			cluster_undo_cleaner_wakeup();
			return CLUSTER_UNDO_RECORD_PREPARE_RETRY_REQUIRED;
		}
		if (claim_result == CLAIM_FS_FAIL)
			return CLUSTER_UNDO_RECORD_PREPARE_RETRY_REQUIRED;
		return CLUSTER_UNDO_RECORD_PREPARE_REFUSED;
	}

	if (!cluster_undo_record_ensure_block0_current(owner_instance, ext->segment_id,
												   absolute_deadline_us, &publication))
		return CLUSTER_UNDO_RECORD_PREPARE_RETRY_REQUIRED;

	if (cluster_undo_pending.active
		&& (cluster_undo_pending.segment_id != ext->segment_id
			|| cluster_undo_pending.owner_instance != owner_instance
			|| cluster_undo_pending.block_no != ext->cur_block)
		&& !cluster_undo_pending_flush_internal(false))
		return CLUSTER_UNDO_RECORD_PREPARE_REFUSED;

	reservation->ref_slot = -1;
	reservation->owns_ref = false;
	reservation->consume_locked = false;
	reservation->consume_local_head_idx = -1;
	reservation->consume_effective_prev_uba = InvalidUba;
	if (cluster_undo_pending.active) {
		if (cluster_undo_pending.ref_slot < 0)
			return CLUSTER_UNDO_RECORD_PREPARE_REFUSED;
		memcpy(reservation->block.data, cluster_undo_pending.buf, BLCKSZ);
		reservation->ref_slot = cluster_undo_pending.ref_slot;
	} else {
		resident = cluster_undo_buf_pin(ext->segment_id, owner_instance, ext->cur_block,
										CLUSTER_UNDO_BUF_SHARED, &pin);
		if (resident == NULL)
			return CLUSTER_UNDO_RECORD_PREPARE_REFUSED;
		memcpy(reservation->block.data, resident, BLCKSZ);
		cluster_undo_buf_addref(&pin);
		reservation->ref_slot = pin.slot;
		reservation->owns_ref = true;
		cluster_undo_buf_unpin(&pin);
		if (ext->cur_slot_count == 0) {
			memset(reservation->block.data, 0, BLCKSZ);
			blkhdr = (UndoBlockHeader *)reservation->block.data;
			blkhdr->magic = PGRAC_UNDO_BLOCK_MAGIC;
			blkhdr->block_version = UNDO_BLOCK_VERSION_1;
			blkhdr->free_offset = sizeof(UndoBlockHeader);
			blkhdr->first_change_scn = InvalidScn;
			blkhdr->first_change_lsn = GetXLogWriteRecPtr();
		}
	}

	/* Retain the existing modifier debt from publication through exact
	 * consume/cancel.  Enter happens after every slow producer and before the
	 * stack receipt becomes visible; the atomic recheck cannot wait on a
	 * cluster/undo producer while the heap content lock is later held. */
	admission = cluster_semantic_activation_modifier_enter(cluster_undo_record_writable_admission(),
														   &modifier_admission);
	if (absolute_deadline_us <= (uint64)GetCurrentTimestamp()
		|| admission != CLUSTER_SEMANTIC_ADMISSION_OK
		|| !cluster_semantic_activation_modifier_recheck(&modifier_admission,
														 cluster_undo_record_writable_admission())
		|| !cluster_undo_block0_current_live_owner_publication_recheck_conditional(&publication)) {
		if (absolute_deadline_us <= (uint64)GetCurrentTimestamp()) {
			cluster_undo_receipt_metric_add(CLUSTER_UNDO_RECEIPT_PREPARE_TIMEOUT);
			cluster_undo_receipt_reason = "PREPARE_TIMEOUT_BEFORE_READY";
		}
		if (modifier_admission.entered)
			cluster_semantic_activation_leave(&modifier_admission);
		if (reservation->owns_ref && reservation->ref_slot >= 0)
			cluster_undo_buf_unref_slot(reservation->ref_slot);
		reservation->owns_ref = false;
		reservation->ref_slot = -1;
		return CLUSTER_UNDO_RECORD_PREPARE_RETRY_REQUIRED;
	}

	reservation->sequence = ++cluster_undo_record_reservation_floor;
	memset(&reservation->receipt, 0, sizeof(reservation->receipt));
	reservation->receipt.magic = CLUSTER_UNDO_RECORD_RECEIPT_MAGIC;
	reservation->receipt.record_type = record_type;
	reservation->receipt.owner_instance = owner_instance;
	reservation->receipt.payload_capacity = payload_capacity;
	reservation->receipt.tt_slot_segment_id = tt_slot_segment_id;
	reservation->receipt.tt_slot_offset = tt_slot_offset;
	reservation->receipt.actual_segment_id = ext->segment_id;
	reservation->receipt.reservation_sequence = reservation->sequence;
	reservation->receipt.absolute_deadline_us = absolute_deadline_us;
	reservation->receipt.extent = *ext;
	reservation->receipt.block0_publication = publication;
	reservation->receipt.modifier_admission = modifier_admission;
	reservation->active = true;
	*receipt = reservation->receipt;
	/* Keep the caller-provided chain hint backend-local; it cannot authorize
	 * any slot and is consumed only after the exact receipt recheck. */
	(void)prev_uba;
	cluster_undo_record_touched_segment(ext->segment_id, owner_instance);
	cluster_undo_receipt_metric_add(CLUSTER_UNDO_RECEIPT_READY);
	cluster_undo_receipt_reason = "NONE";
	return CLUSTER_UNDO_RECORD_PREPARE_READY;
}

static bool
cluster_undo_record_receipt_sync(ClusterUndoRecordPrepareReceipt *receipt)
{
	ClusterUndoRecordReservation *reservation = &cluster_undo_record_reservation;

	if (receipt == NULL || !reservation->active || receipt->reservation_sequence == 0
		|| receipt->reservation_sequence != reservation->sequence)
		return false;
	reservation->receipt = *receipt;
	return true;
}

static bool
cluster_undo_record_bytes_zero(const void *address, Size length)
{
	const uint8 *bytes = (const uint8 *)address;
	Size i;

	if (address == NULL)
		return false;
	for (i = 0; i < length; i++)
		if (bytes[i] != 0)
			return false;
	return true;
}

bool
cluster_undo_record_stage_history(ClusterUndoRecordPrepareReceipt *receipt, uint8 target_ordinal,
								  const UndoItlHistoryEntry *entry)
{
	uint8 target_bit;

	if (receipt == NULL || entry == NULL || target_ordinal >= UNDO_ITL_HISTORY_TARGETS
		|| !cluster_undo_record_receipt_extent_matches(receipt) || receipt->ctrc_applied_mask != 0
		|| entry->reserved != 0 || entry->slot_index >= CLUSTER_ITL_INITRANS_DEFAULT
		|| entry->after_kind
			   != (receipt->record_type == UNDO_RECORD_ITL ? ITL_FLAG_LOCK_ONLY_ACTIVE
														   : ITL_FLAG_ACTIVE)
		|| !SCN_VALID(entry->after_write_scn) || !cluster_undo_history_prior_valid(&entry->prior)
		|| (SCN_VALID(entry->prior.write_scn)
			&& scn_time_cmp(entry->prior.write_scn, entry->after_write_scn) >= 0)
		|| !cluster_undo_record_bytes_zero(receipt->itl_history_reserved,
										   sizeof(receipt->itl_history_reserved)))
		return false;
	target_bit = UINT8_C(1) << target_ordinal;
	if ((receipt->ctrc_pending_mask & target_bit) == 0
		|| entry->block != receipt->ctrc_pending_targets[target_ordinal].block_number)
		return false;
	receipt->itl_history[target_ordinal] = *entry;
	receipt->itl_history_mask |= target_bit;
	return cluster_undo_record_receipt_sync(receipt);
}

bool
cluster_undo_record_history_matches(const ClusterUndoRecordPrepareReceipt *receipt,
									uint8 target_ordinal, const UndoItlHistoryEntry *entry)
{
	return receipt != NULL && entry != NULL && target_ordinal < UNDO_ITL_HISTORY_TARGETS
		   && cluster_undo_record_receipt_extent_matches(receipt)
		   && (receipt->itl_history_mask & (UINT8_C(1) << target_ordinal)) != 0
		   && memcmp(&receipt->itl_history[target_ordinal], entry, sizeof(*entry)) == 0;
}

bool
cluster_undo_record_ctrc_stage_pending(ClusterUndoRecordPrepareReceipt *receipt,
									   uint8 target_ordinal,
									   const ClusterCtrcTargetV1 *pending_target)
{
	uint8 target_bit;

	if (target_ordinal >= CLUSTER_UNDO_RECORD_CTRC_TARGETS)
		return false;
	target_bit = UINT8_C(1) << target_ordinal;
	if (receipt == NULL || pending_target == NULL
		|| pending_target->kind != CTRC_TARGET_PAGE_PENDING_ITL_SLOT
		|| !cluster_undo_record_receipt_extent_matches(receipt)
		|| (receipt->ctrc_prepared_mask & target_bit) != 0
		|| (receipt->ctrc_applied_mask & target_bit) != 0
		|| (receipt->ctrc_reuse_mask & target_bit) != 0
		|| receipt->ctrc_handles[target_ordinal].valid
		|| !cluster_undo_record_bytes_zero(receipt->ctrc_reserved8,
										   sizeof(receipt->ctrc_reserved8)))
		return false;
	if ((receipt->ctrc_pending_mask & target_bit) != 0)
		return cluster_undo_record_ctrc_pending_recheck(receipt, target_ordinal, pending_target);
	receipt->ctrc_pending_targets[target_ordinal] = *pending_target;
	receipt->ctrc_pending_mask |= target_bit;
	return cluster_undo_record_receipt_sync(receipt);
}

bool
cluster_undo_record_ctrc_stage_reuse(ClusterUndoRecordPrepareReceipt *receipt, uint8 target_ordinal,
									 const ClusterCtrcReceiptHandle *handle)
{
	ClusterCtrcReceiptHandle old_handle;
	uint8 old_prepared_mask;
	uint8 old_reuse_mask;
	uint8 target_bit;

	if (target_ordinal >= CLUSTER_UNDO_RECORD_CTRC_TARGETS)
		return false;
	target_bit = UINT8_C(1) << target_ordinal;
	if (receipt == NULL || handle == NULL || !handle->valid || handle->receipt == NULL
		|| pg_atomic_read_u32((pg_atomic_uint32 *)&handle->receipt->state) != CTRC_RECEIPT_APPLIED
		|| (receipt->ctrc_pending_mask & target_bit) == 0
		|| (receipt->ctrc_prepared_mask & target_bit) != 0
		|| (receipt->ctrc_applied_mask & target_bit) != 0
		|| (receipt->ctrc_reuse_mask & target_bit) != 0
		|| receipt->ctrc_handles[target_ordinal].valid
		|| !cluster_undo_record_receipt_extent_matches(receipt)
		|| !cluster_undo_record_bytes_zero(receipt->ctrc_reserved8,
										   sizeof(receipt->ctrc_reserved8)))
		return false;

	old_handle = receipt->ctrc_handles[target_ordinal];
	old_prepared_mask = receipt->ctrc_prepared_mask;
	old_reuse_mask = receipt->ctrc_reuse_mask;
	receipt->ctrc_handles[target_ordinal] = *handle;
	receipt->ctrc_prepared_mask |= target_bit;
	receipt->ctrc_reuse_mask |= target_bit;
	if (cluster_undo_record_receipt_sync(receipt))
		return true;
	receipt->ctrc_handles[target_ordinal] = old_handle;
	receipt->ctrc_prepared_mask = old_prepared_mask;
	receipt->ctrc_reuse_mask = old_reuse_mask;
	return false;
}

bool
cluster_undo_record_ctrc_pending_recheck(const ClusterUndoRecordPrepareReceipt *receipt,
										 uint8 target_ordinal,
										 const ClusterCtrcTargetV1 *pending_target)
{
	const ClusterCtrcTargetV1 *stored;
	uint8 target_bit;

	if (target_ordinal >= CLUSTER_UNDO_RECORD_CTRC_TARGETS || receipt == NULL
		|| pending_target == NULL)
		return false;
	target_bit = UINT8_C(1) << target_ordinal;
	if ((receipt->ctrc_pending_mask & target_bit) == 0)
		return false;
	stored = &receipt->ctrc_pending_targets[target_ordinal];
	/* Published/reused references keep their exact retarget contract. */
	if (receipt->ctrc_applied_mask != 0 || (receipt->ctrc_reuse_mask & target_bit) != 0)
		return memcmp(stored, pending_target, sizeof(*pending_target)) == 0;
	return cluster_ctrc_pending_itl_target_recheck(stored, pending_target);
}

bool
cluster_undo_record_ctrc_pending_matches(const ClusterUndoRecordPrepareReceipt *receipt,
										 uint8 target_ordinal,
										 const ClusterCtrcTargetV1 *pending_target)
{
	uint8 target_bit;

	if (target_ordinal >= CLUSTER_UNDO_RECORD_CTRC_TARGETS)
		return false;
	target_bit = UINT8_C(1) << target_ordinal;
	return receipt != NULL && pending_target != NULL
		   && (receipt->ctrc_pending_mask & target_bit) != 0
		   && (receipt->ctrc_prepared_mask & target_bit) != 0
		   && receipt->ctrc_handles[target_ordinal].valid
		   && cluster_undo_record_ctrc_pending_recheck(receipt, target_ordinal, pending_target);
}

bool
cluster_undo_record_ctrc_required_prepared(const ClusterUndoRecordPrepareReceipt *receipt,
										   uint8 required_mask)
{
	const uint8 known_mask = (UINT8_C(1) << CLUSTER_UNDO_RECORD_CTRC_TARGETS) - 1;
	uint8 target_ordinal;

	if (receipt == NULL || required_mask == 0 || (required_mask & ~known_mask) != 0
		|| (receipt->ctrc_pending_mask & known_mask) != required_mask
		|| (receipt->ctrc_prepared_mask & known_mask) != required_mask
		|| (receipt->ctrc_applied_mask & known_mask) != 0
		|| (receipt->ctrc_reuse_mask & ~required_mask) != 0
		|| !cluster_undo_record_bytes_zero(receipt->ctrc_reserved8,
										   sizeof(receipt->ctrc_reserved8)))
		return false;
	for (target_ordinal = 0; target_ordinal < CLUSTER_UNDO_RECORD_CTRC_TARGETS; target_ordinal++) {
		uint8 target_bit = UINT8_C(1) << target_ordinal;

		if (((required_mask & target_bit) != 0) != receipt->ctrc_handles[target_ordinal].valid)
			return false;
	}
	return true;
}

bool
cluster_undo_record_ctrc_prepare_pending(ClusterUndoRecordPrepareReceipt *receipt,
										 uint8 target_ordinal)
{
	ClusterTTStatusKey status_key;
	ClusterTTStatusResult status;
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcParticipantIdentity participant;
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcPrepareResult result;
	TransactionId xid = GetTopTransactionIdIfAny();
	uint32 grant = 0;
	uint8 target_bit;

	cluster_undo_receipt_reason = "CTRC_PENDING_IDENTITY_MISMATCH";
	if (target_ordinal >= CLUSTER_UNDO_RECORD_CTRC_TARGETS)
		return false;
	target_bit = UINT8_C(1) << target_ordinal;
	if (receipt == NULL || (receipt->ctrc_pending_mask & target_bit) == 0
		|| (receipt->ctrc_prepared_mask & target_bit) != 0
		|| (receipt->ctrc_applied_mask & target_bit) != 0
		|| (receipt->ctrc_reuse_mask & target_bit) != 0
		|| receipt->ctrc_handles[target_ordinal].valid || !TransactionIdIsValid(xid)
		|| MyBackendId <= 0 || !cluster_undo_record_receipt_extent_matches(receipt))
		return false;
	MemSet(&status_key, 0, sizeof(status_key));
	MemSet(&status, 0, sizeof(status));
	MemSet(&key, 0, sizeof(key));
	MemSet(&participant, 0, sizeof(participant));
	MemSet(&publication, 0, sizeof(publication));
	MemSet(&handle, 0, sizeof(handle));
	if (!cluster_runtime_visibility_current_owner_lookup_exact_ctrc_full(xid, &status_key, &status,
																		 &grant, &key, &participant)
		|| status.status != CLUSTER_TT_STATUS_IN_PROGRESS || grant == 0
		|| status_key.undo_segment_id != receipt->tt_slot_segment_id
		|| status_key.tt_slot_id != cluster_tt_slot_offset_to_id(receipt->tt_slot_offset))
		return false;

	publication.requester_node_id = participant.node_id;
	publication.requester_boot_incarnation = participant.boot_incarnation;
	publication.capability_record_generation = participant.capability_record_generation;
	publication.requester_backend_id = MyBackendId;
	publication.wire_request_id = receipt->reservation_sequence;
	publication.operation_id = receipt->reservation_sequence + target_ordinal;
	if (publication.operation_id < receipt->reservation_sequence)
		return false;
	publication.attempt_generation = 1;
	publication.descriptor_hash = 0;
	publication.member_ordinal = UINT16_MAX;
	publication.member_role = 0;
	publication.reference_kind = CTRC_REF_HEAP_ITL_UBA;
	publication.target_kind = CTRC_TARGET_PAGE_PENDING_ITL_SLOT;
	publication.grant_generation = grant;
	result = cluster_ctrc_receipt_prepare_shared(&key, &participant, grant, &publication,
												 &receipt->ctrc_pending_targets[target_ordinal],
												 &handle);
	if (result != CLUSTER_CTRC_PREPARE_READY && result != CLUSTER_CTRC_PREPARE_DUPLICATE) {
		cluster_undo_receipt_reason = "CTRC_PREPARE_REFUSED";
		return false;
	}
	receipt->ctrc_handles[target_ordinal] = handle;
	receipt->ctrc_prepared_mask |= target_bit;
	if (!cluster_undo_record_receipt_sync(receipt)) {
		(void)cluster_ctrc_receipt_cancel_shared(&handle);
		return false;
	}
	cluster_undo_receipt_reason = "NONE";
	return true;
}

ClusterCtrcApplyResult
cluster_undo_record_ctrc_apply_prepared(ClusterUndoRecordPrepareReceipt *receipt,
										uint8 target_ordinal,
										const ClusterCtrcTargetV1 *final_target,
										ClusterCtrcApplyToken *token)
{
	ClusterCtrcApplyResult result;
	uint8 target_bit;

	if (target_ordinal >= CLUSTER_UNDO_RECORD_CTRC_TARGETS)
		return CLUSTER_CTRC_APPLY_FAIL_CLOSED;
	target_bit = UINT8_C(1) << target_ordinal;
	if (receipt == NULL || final_target == NULL || token == NULL
		|| (receipt->ctrc_pending_mask & target_bit) == 0
		|| (receipt->ctrc_prepared_mask & target_bit) == 0
		|| (receipt->ctrc_applied_mask & target_bit) != 0
		|| !receipt->ctrc_handles[target_ordinal].valid
		|| receipt->itl_history_mask != receipt->ctrc_pending_mask
		|| receipt->itl_history[target_ordinal].block != final_target->block_number
		|| receipt->itl_history[target_ordinal].slot_index != final_target->itl_slot_index
		|| !cluster_undo_record_receipt_extent_matches(receipt))
		return CLUSTER_CTRC_APPLY_FAIL_CLOSED;
	result = (receipt->ctrc_reuse_mask & target_bit) != 0
				 ? cluster_ctrc_receipt_retarget_itl_shared(
					   &receipt->ctrc_handles[target_ordinal],
					   &receipt->ctrc_pending_targets[target_ordinal], final_target, token)
				 : cluster_ctrc_receipt_apply_shared(&receipt->ctrc_handles[target_ordinal],
													 final_target, token);
	if (result != CLUSTER_CTRC_APPLY_APPLIED)
		return result;
	receipt->ctrc_applied_mask |= target_bit;
	if (!cluster_undo_record_receipt_sync(receipt))
		return CLUSTER_CTRC_APPLY_FAIL_CLOSED;
	return CLUSTER_CTRC_APPLY_APPLIED;
}


/* READY identity has no wall-clock lease. */
bool
cluster_undo_record_prepared_recheck(const ClusterUndoRecordPrepareReceipt *receipt,
									 uint16 payload_len)
{
	static int diagnostic_budget = 8;
	const char *reason = NULL;

	if (receipt == NULL)
		reason = "NULL_RECEIPT";
	else if (payload_len > receipt->payload_capacity)
		reason = "CAPACITY_REFUSAL";
	else if (!cluster_undo_record_receipt_extent_matches(receipt))
		reason = "RESERVATION_MISMATCH";
	else if (!cluster_semantic_activation_modifier_recheck(
				 &receipt->modifier_admission, cluster_undo_record_writable_admission()))
		reason = "MODIFIER_ADMISSION_CHANGED";
	else if (!cluster_undo_block0_current_live_owner_publication_recheck_conditional(
				 &receipt->block0_publication))
		reason = "BLOCK0_PUBLICATION_CHANGED";
	else
		return true;

	cluster_undo_receipt_reason = reason;
	cluster_undo_receipt_metric_add(CLUSTER_UNDO_RECEIPT_RESERVATION_MISMATCH);
	if (diagnostic_budget > 0) {
		diagnostic_budget--;
		ereport(LOG, (errmsg_internal(
						 "undo prepared-recheck diagnostic: reason=%s record_type=%u pending=%u "
						 "prepared=%u deadline=" UINT64_FORMAT " now=" UINT64_FORMAT,
						 reason, receipt == NULL ? 0 : (unsigned int)receipt->record_type,
						 receipt == NULL ? 0 : (unsigned int)receipt->ctrc_pending_mask,
						 receipt == NULL ? 0 : (unsigned int)receipt->ctrc_prepared_mask,
						 receipt == NULL ? 0 : receipt->absolute_deadline_us,
						 (uint64)GetCurrentTimestamp())));
	}
	return false;
}

bool
cluster_undo_record_retry_evidence(uint64 reservation_sequence, bool *exact_ready,
								   bool *targets_invalidated)
{
	if (exact_ready != NULL)
		*exact_ready = false;
	if (targets_invalidated != NULL)
		*targets_invalidated = false;
	if (exact_ready == NULL || targets_invalidated == NULL || reservation_sequence == 0
		|| cluster_undo_retry_cancel_snapshot.sequence != reservation_sequence)
		return false;
	*exact_ready = cluster_undo_retry_cancel_snapshot.exact_ready;
	*targets_invalidated = cluster_undo_retry_cancel_snapshot.targets_invalidated;
	return true;
}

ClusterUndoRecordPrepareResult
cluster_undo_record_requalify_for_retry(ClusterUndoRecordPrepareReceipt *receipt,
										uint16 payload_len, bool targets_invalidated)
{
	bool exact_ready;
	uint64 original_deadline;
	uint64 refused_at;
	uint8 reuse_mask;
	const char *invalidated_reason = NULL;

	MemSet(&cluster_undo_retry_cancel_snapshot, 0, sizeof(cluster_undo_retry_cancel_snapshot));
	if (receipt == NULL || !cluster_undo_record_reservation.active
		|| receipt->magic != CLUSTER_UNDO_RECORD_RECEIPT_MAGIC
		|| memcmp(receipt, &cluster_undo_record_reservation.receipt, sizeof(*receipt)) != 0) {
		cluster_undo_receipt_metric_add(CLUSTER_UNDO_RECEIPT_RESERVATION_MISMATCH);
		cluster_undo_receipt_reason = "RESERVATION_MISMATCH";
		return CLUSTER_UNDO_RECORD_PREPARE_REFUSED;
	}
	if (receipt->ctrc_applied_mask != 0 || cluster_undo_record_reservation.consume_locked) {
		cluster_undo_receipt_metric_add(CLUSTER_UNDO_RECEIPT_POST_APPLY_REPREPARE);
		cluster_undo_receipt_reason = "POST_APPLY_REPREPARE_REFUSED";
		return CLUSTER_UNDO_RECORD_PREPARE_REFUSED;
	}
	/* All heap locks have been dropped.  Use the existing blocking resident
	 * sampler: a missed conditional content-lock probe is not invalidation. */
	if (payload_len > receipt->payload_capacity)
		invalidated_reason = "CAPACITY_REFUSAL";
	else if (!cluster_undo_record_receipt_extent_matches(receipt))
		invalidated_reason = "RESERVATION_MISMATCH";
	else if (!cluster_semantic_activation_modifier_recheck(
				 &receipt->modifier_admission, cluster_undo_record_writable_admission()))
		invalidated_reason = "MODIFIER_ADMISSION_CHANGED";
	else if (!cluster_undo_block0_current_live_owner_publication_recheck(
				 &receipt->block0_publication))
		invalidated_reason = "BLOCK0_PUBLICATION_CHANGED";
	exact_ready = invalidated_reason == NULL;
	if (exact_ready && !targets_invalidated) {
		cluster_undo_receipt_metric_add(CLUSTER_UNDO_RECEIPT_RETRY_PRESERVED);
		cluster_undo_receipt_reason = "NONE";
		return CLUSTER_UNDO_RECORD_PREPARE_READY;
	}
	original_deadline = receipt->absolute_deadline_us;
	reuse_mask = receipt->ctrc_reuse_mask;
	cluster_undo_retry_cancel_snapshot.exact_ready = exact_ready;
	cluster_undo_retry_cancel_snapshot.targets_invalidated = targets_invalidated;
	cluster_undo_retry_cancel_snapshot.sequence = receipt->reservation_sequence;
	cluster_undo_retry_cancel_snapshot.applied_mask = receipt->ctrc_applied_mask;
	cluster_undo_retry_cancel_snapshot.armed = true;
	cluster_undo_record_cancel_prepared(receipt);
	refused_at = (uint64)GetCurrentTimestamp();
	if (original_deadline <= refused_at) {
		cluster_undo_receipt_metric_add(CLUSTER_UNDO_RECEIPT_INVALIDATED_BUDGET_EXHAUSTED);
		cluster_undo_receipt_reason = "PREPARE_BUDGET_EXHAUSTED_AFTER_EXACT_INVALIDATION";
		/* Keep the actual pre-cancel predicate with every final refusal,
		 * including a budget crossed while releasing the exact reservation. */
		ereport(
			LOG,
			(errmsg_internal(
				"undo retry refusal evidence: reservation=" UINT64_FORMAT
				" exact_ready=%d targets_invalidated=%d cause=%s deadline=" UINT64_FORMAT
				" now=" UINT64_FORMAT " applied=%u reuse=%u",
				cluster_undo_retry_cancel_snapshot.sequence, exact_ready, targets_invalidated,
				invalidated_reason == NULL ? "TARGET_INVALIDATED" : invalidated_reason,
				original_deadline, refused_at,
				(unsigned)cluster_undo_retry_cancel_snapshot.applied_mask, (unsigned)reuse_mask)));
		return CLUSTER_UNDO_RECORD_PREPARE_REFUSED;
	}
	cluster_undo_receipt_reason = "EXACT_IDENTITY_INVALIDATED";
	return CLUSTER_UNDO_RECORD_PREPARE_RETRY_REQUIRED;
}

bool
cluster_undo_record_prepared_uba_exact(const ClusterUndoRecordPrepareReceipt *receipt,
									   uint16 payload_len, UBA *uba_out)
{
	uint32 record_length;
	UBA uba;

	if (uba_out != NULL)
		*uba_out = InvalidUba;
	if (receipt == NULL || uba_out == NULL || payload_len > receipt->payload_capacity
		|| !cluster_undo_record_receipt_extent_matches(receipt))
		return false;
	record_length = undo_prepared_record_total_length(payload_len);
	if (!cluster_undo_block_has_space(receipt->extent.cur_free_offset,
									  receipt->extent.cur_slot_count, record_length))
		return false;
	uba = uba_encode(receipt->actual_segment_id, receipt->extent.cur_block, receipt->tt_slot_offset,
					 receipt->extent.cur_slot_count);
	if (UBA_is_invalid(uba))
		return false;
	*uba_out = uba;
	return true;
}


void
cluster_undo_record_cancel_prepared(ClusterUndoRecordPrepareReceipt *receipt)
{
	ClusterUndoRecordReservation *reservation = &cluster_undo_record_reservation;

	if (receipt == NULL || !reservation->active || receipt->reservation_sequence == 0
		|| receipt->reservation_sequence != reservation->sequence
		|| memcmp(receipt, &reservation->receipt, sizeof(*receipt)) != 0)
		return;
	if (cluster_undo_retry_cancel_snapshot.armed) {
		if (cluster_undo_retry_cancel_snapshot.sequence == receipt->reservation_sequence
			&& cluster_undo_retry_cancel_snapshot.exact_ready
			&& !cluster_undo_retry_cancel_snapshot.targets_invalidated)
			cluster_undo_receipt_metric_add(CLUSTER_UNDO_RECEIPT_REPREPARE_PREMATURE);
		cluster_undo_retry_cancel_snapshot.armed = false;
	}
	cluster_undo_receipt_metric_add(CLUSTER_UNDO_RECEIPT_CANCEL);
	{
		uint8 target_ordinal;

		for (target_ordinal = 0; target_ordinal < CLUSTER_UNDO_RECORD_CTRC_TARGETS;
			 target_ordinal++) {
			uint8 target_bit = UINT8_C(1) << target_ordinal;

			if ((receipt->ctrc_prepared_mask & target_bit) != 0
				&& (receipt->ctrc_applied_mask & target_bit) == 0
				&& (receipt->ctrc_reuse_mask & target_bit) == 0
				&& receipt->ctrc_handles[target_ordinal].valid)
				(void)cluster_ctrc_receipt_cancel_shared(&receipt->ctrc_handles[target_ordinal]);
		}
	}
	if (reservation->consume_locked) {
		cluster_undo_buf_unlock_ref(reservation->ref_slot);
		reservation->consume_locked = false;
	}
	if (reservation->owns_ref && reservation->ref_slot >= 0)
		cluster_undo_buf_unref_slot(reservation->ref_slot);
	cluster_semantic_activation_leave(&reservation->receipt.modifier_admission);
	reservation->active = false;
	reservation->owns_ref = false;
	reservation->ref_slot = -1;
	reservation->consume_local_head_idx = -1;
	reservation->consume_effective_prev_uba = InvalidUba;
	memset(&reservation->receipt, 0, sizeof(reservation->receipt));
	memset(receipt, 0, sizeof(*receipt));
}


ClusterUndoRecordConsumePreflightResult
cluster_undo_record_consume_preflight(ClusterUndoRecordPrepareReceipt *receipt, uint16 payload_len)
{
	ClusterUndoRecordReservation *reservation = &cluster_undo_record_reservation;
	uint32 record_length;
	int local_head_idx;

	if (receipt == NULL || payload_len > receipt->payload_capacity || reservation->consume_locked
		|| receipt->ctrc_applied_mask != 0)
		return CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_REFUSED;
	if (!cluster_undo_record_receipt_extent_matches(receipt)
		|| !cluster_undo_block0_current_live_owner_publication_recheck_conditional(
			&receipt->block0_publication)
		|| !cluster_semantic_activation_modifier_recheck(
			&receipt->modifier_admission, cluster_undo_record_writable_admission())) {
		cluster_undo_receipt_metric_add(CLUSTER_UNDO_RECEIPT_PREFLIGHT_IDENTITY_REFUSAL);
		return CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_RETRY_REQUIRED;
	}

	record_length = undo_prepared_record_total_length(payload_len);
	if (!cluster_undo_block_has_space(receipt->extent.cur_free_offset,
									  receipt->extent.cur_slot_count, record_length)
		|| !cluster_undo_local_head_ensure(receipt->tt_slot_segment_id, receipt->tt_slot_offset,
										   &local_head_idx)) {
		cluster_undo_receipt_metric_add(CLUSTER_UNDO_RECEIPT_CAPACITY_REFUSAL);
		cluster_undo_receipt_reason = "CAPACITY_REFUSAL";
		return CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_RETRY_REQUIRED;
	}
	if (!cluster_undo_buf_lock_ref_conditional(reservation->ref_slot, receipt->actual_segment_id,
											   receipt->owner_instance, receipt->extent.cur_block))
		return CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_RETRY_REQUIRED;

	/* Close the check-to-lock window before publishing any irreversible CTRC
	 * state.  The retained ref prevents rebinding and the held content lock
	 * makes the later DATA-image install deterministic. */
	if (!cluster_undo_record_receipt_extent_matches(receipt)
		|| !cluster_undo_block0_current_live_owner_publication_recheck_conditional(
			&receipt->block0_publication)
		|| !cluster_semantic_activation_modifier_recheck(
			&receipt->modifier_admission, cluster_undo_record_writable_admission())) {
		cluster_undo_receipt_metric_add(CLUSTER_UNDO_RECEIPT_PREFLIGHT_IDENTITY_REFUSAL);
		cluster_undo_buf_unlock_ref(reservation->ref_slot);
		return CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_RETRY_REQUIRED;
	}
	reservation->consume_locked = true;
	reservation->consume_local_head_idx = local_head_idx;
	reservation->consume_effective_prev_uba = cluster_undo_local_heads[local_head_idx].head;
	return CLUSTER_UNDO_RECORD_CONSUME_PREFLIGHT_READY;
}


ClusterUndoRecordConsumeResult
cluster_undo_record_consume_prepared(ClusterUndoRecordPrepareReceipt *receipt,
									 const ClusterUndoRecordTarget *target, const void *payload,
									 uint16 payload_len, UBA *out_uba)
{
	ClusterUndoRecordReservation *reservation = &cluster_undo_record_reservation;
	PGAlignedBlock successor;
	UndoBlockHeader *blkhdr;
	UndoRecordHeader *rechdr;
	UndoSlotDirEntry *slot;
	UBA effective_prev_uba = InvalidUba;
	UBA result;
	SCN current_scn;
	uint32 record_length;
	UndoItlHistoryTrailer history = { 0 };
	UndoItlHistoryTrailer decoded_history;
	uint16 decoded_body_length;
	uint16 new_slot_idx;
	uint32 free_offset;
	uint16 slot_count;
	int local_head_idx;
	bool first_in_tx;

	if (out_uba != NULL)
		*out_uba = InvalidUba;
	if (receipt == NULL || target == NULL || payload == NULL || out_uba == NULL
		|| payload_len > receipt->payload_capacity || !reservation->consume_locked)
		return CLUSTER_UNDO_RECORD_CONSUME_REFUSED;
	if (!cluster_undo_record_receipt_extent_matches(receipt))
		return CLUSTER_UNDO_RECORD_CONSUME_REFUSED;
	if ((receipt->itl_history_mask != 1 && receipt->itl_history_mask != 3)
		|| receipt->itl_history_mask != receipt->ctrc_pending_mask
		|| receipt->itl_history_mask != receipt->ctrc_prepared_mask
		|| receipt->itl_history_mask != receipt->ctrc_applied_mask
		|| !cluster_undo_record_bytes_zero(receipt->itl_history_reserved,
										   sizeof(receipt->itl_history_reserved)))
		return CLUSTER_UNDO_RECORD_CONSUME_REFUSED;
	history.magic = UNDO_ITL_HISTORY_MAGIC;
	history.version = UNDO_ITL_HISTORY_VERSION;
	history.count = receipt->itl_history_mask == 3 ? 2 : 1;
	memcpy(history.entries, receipt->itl_history, sizeof(history.entries));

	record_length = undo_prepared_record_total_length(payload_len);
	free_offset = receipt->extent.cur_free_offset;
	slot_count = receipt->extent.cur_slot_count;
	local_head_idx = reservation->consume_local_head_idx;
	if (!cluster_undo_block_has_space(free_offset, slot_count, record_length) || local_head_idx < 0
		|| (uint32)local_head_idx >= cluster_undo_local_head_count
		|| cluster_undo_local_heads[local_head_idx].tt_slot_segment_id
			   != receipt->tt_slot_segment_id
		|| cluster_undo_local_heads[local_head_idx].tt_slot_offset != receipt->tt_slot_offset
		|| memcmp(&cluster_undo_local_heads[local_head_idx].head,
				  &reservation->consume_effective_prev_uba,
				  sizeof(reservation->consume_effective_prev_uba))
			   != 0)
		return CLUSTER_UNDO_RECORD_CONSUME_REFUSED;
	effective_prev_uba = reservation->consume_effective_prev_uba;
	first_in_tx = UBA_is_invalid(effective_prev_uba);

	memcpy(successor.data, reservation->block.data, BLCKSZ);
	blkhdr = (UndoBlockHeader *)successor.data;
	current_scn = cluster_scn_advance();
	if (slot_count == 0) {
		blkhdr->first_change_scn = current_scn;
		blkhdr->first_change_lsn = GetXLogWriteRecPtr();
	}
	rechdr = (UndoRecordHeader *)(successor.data + free_offset);
	memset(rechdr, 0, sizeof(*rechdr));
	rechdr->record_type = receipt->record_type;
	rechdr->flags = (first_in_tx ? UNDO_REC_FLAG_FIRST_IN_TX : 0) | UNDO_REC_FLAG_HAS_ITL_HISTORY;
	rechdr->payload_length = payload_len + sizeof(history);
	rechdr->xid = GetCurrentTransactionIdIfAny();
	rechdr->origin_node_id = (uint16)cluster_node_id;
	rechdr->tt_slot_segment_id = receipt->tt_slot_segment_id;
	rechdr->tt_slot_id = cluster_tt_slot_offset_to_id(receipt->tt_slot_offset);
	{
		uint32 bind_seg;
		uint16 bind_off;
		uint32 bind_tt_id;
		uint32 bind_epoch;
		uint16 bind_wrap;

		if (cluster_tt_local_peek_binding(rechdr->xid, &bind_seg, &bind_off, &bind_tt_id,
										  &bind_epoch, &bind_wrap)
			&& bind_seg == (uint32)receipt->tt_slot_segment_id
			&& bind_off == receipt->tt_slot_offset)
			rechdr->tt_wrap_plus1 = (uint16)(bind_wrap + 1);
	}
	rechdr->write_scn = current_scn;
	rechdr->prev_uba = effective_prev_uba;
	rechdr->target_locator = target->locator;
	rechdr->target_fork = target->forknum;
	rechdr->target_block = target->blockno;
	rechdr->target_offset = target->offnum;
	memcpy(successor.data + free_offset + sizeof(*rechdr), payload, payload_len);
	memcpy(successor.data + free_offset + sizeof(*rechdr) + payload_len, &history, sizeof(history));
	if (!cluster_undo_record_decode_payload(rechdr, successor.data + free_offset + sizeof(*rechdr),
											rechdr->payload_length, &decoded_body_length,
											&decoded_history)
		|| rechdr->tt_wrap_plus1 == 0)
		return CLUSTER_UNDO_RECORD_CONSUME_REFUSED;
	new_slot_idx = slot_count;
	slot = UNDO_SLOT_DIR_PTR(successor.data, new_slot_idx);
	slot->record_offset = free_offset;
	slot->record_length = record_length;
	slot->record_type = receipt->record_type;
	slot->flags = rechdr->flags;
	blkhdr->slot_count = (uint16)(slot_count + 1);
	blkhdr->free_offset = free_offset + record_length;

	cluster_undo_record_install_prepared_resident_locked(receipt, successor.data);

	if (!cluster_undo_pending.active) {
		cluster_undo_pending.active = true;
		cluster_undo_pending.owner_instance = receipt->owner_instance;
		cluster_undo_pending.segment_id = receipt->actual_segment_id;
		cluster_undo_pending.block_no = receipt->extent.cur_block;
		cluster_undo_pending.old_block_lsn
			= ((UndoBlockHeader *)reservation->block.data)->block_lsn;
		cluster_undo_pending.rec_lo = (uint16)free_offset;
		cluster_undo_pending.rec_hi = (uint16)free_offset;
		cluster_undo_pending.slot_min_off = PG_UINT16_MAX;
		cluster_undo_pending.slot_max_off = 0;
		cluster_undo_pending.nrecords = 0;
		cluster_undo_pending.ref_slot = reservation->ref_slot;
		reservation->owns_ref = false;
	}
	memcpy(cluster_undo_pending.buf, successor.data, BLCKSZ);
	cluster_undo_pending.rec_hi = (uint16)(free_offset + record_length);
	if ((uint16)UNDO_SLOT_DIR_OFFSET(new_slot_idx) < cluster_undo_pending.slot_min_off)
		cluster_undo_pending.slot_min_off = (uint16)UNDO_SLOT_DIR_OFFSET(new_slot_idx);
	if ((uint16)UNDO_SLOT_DIR_OFFSET(new_slot_idx) > cluster_undo_pending.slot_max_off)
		cluster_undo_pending.slot_max_off = (uint16)UNDO_SLOT_DIR_OFFSET(new_slot_idx);
	cluster_undo_pending.nrecords++;

	cluster_semantic_activation_leave(&cluster_undo_record_reservation.receipt.modifier_admission);
	cluster_undo_record_reservation.active = false;
	reservation->ref_slot = -1;
	reservation->owns_ref = false;
	reservation->consume_local_head_idx = -1;
	reservation->consume_effective_prev_uba = InvalidUba;
	cluster_undo_current_extent.cur_free_offset = free_offset + record_length;
	cluster_undo_current_extent.cur_slot_count = (uint16)(slot_count + 1);
	pg_atomic_fetch_add_u64(&UndoRecordShared->record_alloc_count, 1);
	cluster_undo_receipt_metric_add(CLUSTER_UNDO_RECEIPT_APPLY);
	if (receipt->absolute_deadline_us <= (uint64)GetCurrentTimestamp())
		cluster_undo_receipt_metric_add(CLUSTER_UNDO_RECEIPT_READY_SURVIVED_DEADLINE);
	cluster_undo_touched_in_xact = true;
	result = uba_encode(receipt->actual_segment_id, receipt->extent.cur_block,
						receipt->tt_slot_offset, new_slot_idx);
	cluster_undo_local_heads[local_head_idx].head = result;
	*out_uba = result;
	memset(&reservation->receipt, 0, sizeof(reservation->receipt));
	memset(receipt, 0, sizeof(*receipt));
	return CLUSTER_UNDO_RECORD_CONSUME_APPLIED;
}


UBA
cluster_undo_record_alloc(uint8 record_type, const ClusterUndoRecordTarget *target,
						  uint16 tt_slot_segment_id, uint16 tt_slot_offset, const void *payload,
						  uint16 payload_len, UBA prev_uba)
{
	ClusterSemanticAdmissionToken modifier_token;
	ClusterSemanticAdmissionResult admission;
	UBA result = InvalidUba;

	admission = cluster_semantic_activation_modifier_enter(cluster_undo_record_writable_admission(),
														   &modifier_token);
	if (admission != CLUSTER_SEMANTIC_ADMISSION_OK)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
						errmsg("cannot allocate undo: cluster reconfiguration in progress"),
						errhint("The write was refused before undo mutation; retry is safe.")));

	PG_TRY();
	{
		if (!cluster_semantic_activation_modifier_recheck(&modifier_token,
														  cluster_undo_record_writable_admission()))
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
							errmsg("cannot allocate undo: cluster reconfiguration in progress"),
							errhint("The write was refused before undo mutation; retry is safe.")));
		result = cluster_undo_record_alloc_body(record_type, target, tt_slot_segment_id,
												tt_slot_offset, payload, payload_len, prev_uba);
	}
	PG_FINALLY();
	{
		cluster_semantic_activation_leave(&modifier_token);
	}
	PG_END_TRY();

	return result;
}


/*
 * cluster_undo_get_record -- sanity reader, own-instance only at spec-3.7.
 */
size_t
cluster_undo_get_record(UBA uba, void *out_buffer, size_t buffer_size)
{
	uint32 segment_id, block_no;
	uint16 tt_slot_offset, row_offset;
	char block_buf[BLCKSZ];
	const UndoBlockHeader *blkhdr;
	const UndoSlotDirEntry *slot;
	uint8 owner_instance;
	size_t copy_bytes;

	if (UndoRecordShared == NULL)
		return 0;

	if (!uba_decode_record(uba, &segment_id, &block_no, &tt_slot_offset, &row_offset))
		return 0;

	/* Map segment_id back to owner_instance.  Per spec-3.4b convention:
	 *   owner_instance = (segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 */
	owner_instance = (uint8)((segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1);

	/* Own-instance, or a merged-materialized remote instance (spec-4.5a D8):
	 * the path builder below derives the directory from owner_instance, so a
	 * materialized peer's records read straight from the local
	 * pg_undo/instance_<origin> tree.  Other (runtime-warm) cross-instance reads
	 * stay unsupported -- the read-path coordinator boundary (Spec: spec-5.57).
	 *
	 * This is the W3 wall.  It keeps its RETURN-BASED fail-closed contract (the
	 * recovery (cluster_tt_recovery.c) and SQL SRF (cluster_undo_srf.c) callers
	 * treat 0 as stop/NULL, never visible); per spec-5.57 Q11-A it is NOT
	 * blanket-converted to ERROR.  The CR-construct caller no longer relies on
	 * this branch: the spec-5.57 D2 pre-check in cluster_cr.c fail-closes a
	 * runtime-warm cross-instance origin with 53R9G BEFORE this read, so for the
	 * CR path the boundary is consolidated to one errcode.
	 *
	 * spec-5.22b D2 (#119) — this wall now formally ENFORCES invariant #8: a
	 * peer NEVER self-reads a foreign owner's undo bytes.  The coherent runtime
	 * cross-instance undo read is cluster_undo_block_acquire_shared (D2-3): the
	 * owner is the master AND the holder, so it ships its own block image over
	 * owner-as-master routing and the requesting peer consumes THAT shipped
	 * image, never opening the foreign pg_undo/instance_<origin> tree here.
	 * Hence this local-open path correctly STAYS fail-closed for a runtime-warm
	 * foreign owner -- the coherent bytes come from the grant, not from this
	 * read (admitting a local foreign read here would BREAK invariant #8).  The
	 * admit set is unchanged (own-instance OR materialized-remote, both P1-3
	 * local): only this contract text is updated (spec-5.22b Q-D24-1=A, zero
	 * behaviour change).  Routing the recovery/SRF/CR callers onto the grant
	 * (so a runtime-warm foreign read reaches acquire_shared instead of this
	 * warning) is the D6 consumer wiring. */
	if (owner_instance != (uint8)(cluster_node_id + 1)
		&& !cluster_merged_instance_is_materialized((int)owner_instance - 1)) {
		ereport(WARNING,
				(errmsg("cluster_undo_get_record: runtime cross-instance undo read not supported"),
				 errhint("The coherent cross-instance undo path is the owner's shipped block "
						 "image (spec-5.22b #119 undo-block Cache Fusion), not a local read; "
						 "see Spec: spec-5.57.")));
		return 0;
	}
	if (owner_instance != (uint8)(cluster_node_id + 1))
		cluster_vis_bump_remote_uba_resolved(); /* spec-4.5a D11 */

	if (!read_undo_block(segment_id, owner_instance, block_no, block_buf))
		return 0;

	blkhdr = (UndoBlockHeader *)block_buf;
	if (blkhdr->magic != PGRAC_UNDO_BLOCK_MAGIC)
		return 0;

	if (!cluster_undo_record_slot_index_valid(blkhdr->slot_count, row_offset))
		return 0;

	slot = UNDO_SLOT_DIR_PTR(block_buf, row_offset);
	if (!cluster_undo_record_slot_range_valid(blkhdr->slot_count, row_offset, slot->record_offset,
											  slot->record_length))
		return 0;

	if (buffer_size < slot->record_length)
		return 0;

	copy_bytes = slot->record_length;
	memcpy(out_buffer, block_buf + slot->record_offset, copy_bytes);

	pg_atomic_fetch_add_u64(&UndoRecordShared->reader_lookup_count, 1);

	return copy_bytes;
}


/*
 * cluster_undo_xact_precommit_flush -- P0 perf hardening (2026-05-31).
 *
 *	fsync every undo segment file this xact dirtied, ONCE, replacing the old
 *	per-record fsync.  MUST be called on the commit path BEFORE the commit
 *	becomes visible — i.e. before commit_scn publish (ITL/TT) and before the
 *	commit XLOG record is flushed.  Durable ordering:
 *
 *	    undo segment fsync  ->  ITL/TT commit_scn publish  ->  commit XLogFlush
 *
 *	so a crash can never leave a visible commit whose undo is not durable.
 *
 *	On any fsync failure this ereport(ERROR)s: it runs before the commit's
 *	critical section, so the xact aborts cleanly (its un-fsync'd undo blocks are
 *	irrelevant to an aborted xact) — never a silent half-durable commit.
 *
 *	No-op for a xact that wrote no undo (DDL-only / read-only).  Normal COMMIT
 *	clears the touched list in its O(1) cleanup hook; PREPARE/ABORT use the full
 *	reset.
 */
void
cluster_undo_xact_precommit_flush(void)
{
	int i;

	/*
	 * spec-3.25 D1b: the deferred merged undo WAL record must be inserted
	 * BEFORE the commit / prepare record (same durable ordering the old
	 * per-record emission gave).  Runs before BOTH early returns below: the
	 * pending image exists exactly on the write-back path.
	 */
	(void)cluster_undo_pending_flush_internal(true);
	if (unlikely(cluster_undo_pending.active))
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster undo block remained pending before transaction finish"),
						errhint("Retry the transaction and inspect the server log for an undo WAL "
								"flush failure.")));

	if (cluster_undo_touched_seg_count == 0)
		return;

	/*
	 * spec-3.18 D2b:  with write-back on, undo blocks are made durable by the
	 * checkpoint write-back flush (CheckPointGuts -> cluster_undo_buf_flush_all)
	 * + WAL redo -- the commit record's XLogFlush makes the protecting
	 * XLOG_UNDO_BLOCK_WRITE records durable, and a crash replays them.  So the
	 * per-commit data fsync (the L1 write-path tax this spec targets) is no
	 * longer needed.  With write-back off (D2a) the write-through blocks are
	 * not otherwise fsync'd before a checkpoint recycles their WAL, so the
	 * precommit fsync stays.
	 */
	if (cluster_undo_buf_writeback_allowed())
		return;

	for (i = 0; i < cluster_undo_touched_seg_count; i++) {
		if (!cluster_undo_smgr_fsync_segment_file(cluster_undo_touched_segs[i].segment_id,
												  cluster_undo_touched_segs[i].owner_instance)) {
			if (UndoRecordShared != NULL)
				pg_atomic_fetch_add_u64(&UndoRecordShared->commit_fsync_failure_count, 1);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("cluster undo precommit fsync failed for segment %u (instance %u)",
							cluster_undo_touched_segs[i].segment_id,
							cluster_undo_touched_segs[i].owner_instance)));
		}
		if (UndoRecordShared != NULL)
			pg_atomic_fetch_add_u64(&UndoRecordShared->commit_fsync_segment_count, 1);
	}

	if (UndoRecordShared != NULL)
		pg_atomic_fetch_add_u64(&UndoRecordShared->commit_fsync_count, 1);
}


/*
 * cluster_undo_record_xact_reset -- PREPARE/ABORT full per-backend teardown.
 *
 *	PREPARE already flushed the touched undo segments before EndPrepare.  On
 *	ABORT the list is simply dropped because aborted undo needs no durability.
 *	Normal COMMIT uses cluster_undo_record_xact_commit_release() instead.
 */
void
cluster_undo_record_xact_reset(void)
{
	if (cluster_undo_record_reservation.active)
		cluster_undo_record_cancel_prepared(&cluster_undo_record_reservation.receipt);

	/*
	 * spec-3.25 D1b: drain a still-pending merged record.  Commit/prepare
	 * already drained at precommit (no-op here); this is the ABORT path --
	 * emitting keeps exact parity with the old per-record behaviour (an
	 * aborted xact's records were already WAL'd).  On failure just drop the
	 * pending image: an aborted xact's undo needs no durability.
	 */
	if (cluster_undo_pending.active && !cluster_undo_pending_flush_internal(false)) {
		ereport(WARNING, (errmsg("cluster undo deferred block write dropped at abort "
								 "(segment %u block %u, %u records)",
								 cluster_undo_pending.segment_id, cluster_undo_pending.block_no,
								 cluster_undo_pending.nrecords)));
		if (cluster_undo_pending.ref_slot >= 0) {
			cluster_undo_buf_unref_slot(cluster_undo_pending.ref_slot);
			cluster_undo_pending.ref_slot = -1;
		}
		cluster_undo_pending.active = false;
	}

	cluster_undo_touched_in_xact = false;
	/* PREPARE's durable guard keeps its undo alive after this handoff. */
	cluster_undo_active_write_unregister();
	/* spec-4.12a Hardening v1.0.1: re-arm residual revalidation for the next
	 * top-xact (its carried residual must be re-checked under lifecycle_lock). */
	cluster_undo_residual_validated_this_xact = false;
	memset(cluster_undo_local_heads, 0, sizeof(cluster_undo_local_heads));
	cluster_undo_local_head_count = 0;
	cluster_undo_touched_seg_count = 0;
	/*
	 * spec-3.18 D3.3 (Q2 hybrid): the backend-local extent is NOT dropped at
	 * xact end -- the next transaction on this backend reuses the residual
	 * blocks, amortizing the extent-claim frequency (lifecycle_lock + the A1
	 * batch-mark fsync) across small transactions.  cluster_undo_record_alloc
	 * validates the residual (ext->segment_id == active_segment_id) before
	 * reuse and drops it on a segment rollover (the active segment is never
	 * recycled, so a residual in it is always safe).  A block may then hold
	 * records from several transactions -- correct (UBA addresses by row).
	 */
	/* Full PREPARE/ABORT teardown closes the cache.  Normal COMMIT preserves it. */
	cluster_undo_smgr_fd_cache_reset();
}


/*
 * cluster_undo_record_is_touched -- legacy transaction-local write marker.
 */
bool
cluster_undo_record_is_touched(void)
{
	return cluster_undo_touched_in_xact;
}


/* ---- Counter accessors (for emit_row / TAP verification) ---- */

uint64
cluster_undo_record_alloc_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->record_alloc_count);
}

uint64
cluster_undo_segment_claim_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->segment_claim_count);
}

uint64
cluster_undo_extent_claim_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->extent_claim_count);
}

uint64
cluster_undo_block_write_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->block_write_count);
}

uint64
cluster_undo_block_flush_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->block_flush_count);
}

uint64
cluster_undo_reader_lookup_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->reader_lookup_count);
}

/* Caller holds lifecycle_lock EXCLUSIVE. */
static void
cluster_undo_record_observation_apply_locked(uint8 owner_instance)
{
	ClusterUndoPoolObservation observation;
	ClusterUndoPoolObservationResult scan_result;
	uint64 high_water;

	if (UndoRecordShared == NULL)
		return;
	Assert(LWLockHeldByMeInMode(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE));
	scan_result = cluster_undo_segment_observe_pool(owner_instance, &observation);
	pg_atomic_write_u32(&UndoRecordShared->segment_observation_status, (uint32)scan_result);
	if (scan_result != CLUSTER_UNDO_POOL_OBS_OK) {
		pg_atomic_write_u32(&UndoRecordShared->segment_observation_ready, 0);
		return;
	}

	pg_atomic_write_u64(&UndoRecordShared->segment_allocated_count,
						(uint64)observation.allocated_count);
	high_water = pg_atomic_read_u64(&UndoRecordShared->segment_allocated_high_water);
	while (high_water < (uint64)observation.allocated_count
		   && !pg_atomic_compare_exchange_u64(&UndoRecordShared->segment_allocated_high_water,
											  &high_water, (uint64)observation.allocated_count))
		;
	pg_write_barrier();
	pg_atomic_write_u32(&UndoRecordShared->segment_observation_ready, 1);
}

void
cluster_undo_record_observation_ensure(void)
{
	if (UndoRecordShared == NULL)
		return;
	if (pg_atomic_read_u32(&UndoRecordShared->segment_observation_ready) == 1
		|| pg_atomic_read_u32(&UndoRecordShared->segment_observation_attempted) == 1)
		return;
	if (cluster_node_id < 0 || cluster_node_id >= UNDO_OWNER_INSTANCE_MAX) {
		pg_atomic_write_u32(&UndoRecordShared->segment_observation_status,
							(uint32)CLUSTER_UNDO_POOL_OBS_INVALID_OWNER);
		return;
	}

	LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);
	if (pg_atomic_read_u32(&UndoRecordShared->segment_observation_ready) == 0
		&& pg_atomic_read_u32(&UndoRecordShared->segment_observation_attempted) == 0) {
		cluster_undo_record_observation_apply_locked((uint8)(cluster_node_id + 1));
		pg_atomic_write_u32(&UndoRecordShared->segment_observation_attempted, 1);
	}
	LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
}

uint64
cluster_undo_segment_allocated_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->segment_allocated_count);
}

uint64
cluster_undo_segment_allocated_high_water(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->segment_allocated_high_water);
}

uint32
cluster_undo_segment_effective_cap(void)
{
	int guc_val = cluster_undo_segments_max_per_instance;
	uint32 configured_cap;
	uint64 current = 0;

	if (guc_val < 16)
		configured_cap = 16;
	else if (guc_val > (int)CLUSTER_UNDO_SEGS_PER_INSTANCE)
		configured_cap = CLUSTER_UNDO_SEGS_PER_INSTANCE;
	else
		configured_cap = (uint32)guc_val;
	if (UndoRecordShared != NULL)
		current = pg_atomic_read_u64(&UndoRecordShared->segment_allocated_count);
	if (current > (uint64)configured_cap)
		return (uint32)current;
	return configured_cap;
}

const char *
cluster_undo_segment_observation_status_string(void)
{
	uint32 status;

	if (UndoRecordShared == NULL)
		return "UNAVAILABLE_INVALID_OWNER";
	status = pg_atomic_read_u32(&UndoRecordShared->segment_observation_status);
	if (pg_atomic_read_u32(&UndoRecordShared->segment_observation_ready) == 1
		&& status == (uint32)CLUSTER_UNDO_POOL_OBS_OK)
		return "READY";
	switch ((ClusterUndoPoolObservationResult)status) {
	case CLUSTER_UNDO_POOL_OBS_IO_ERROR:
		return "UNAVAILABLE_IO_ERROR";
	case CLUSTER_UNDO_POOL_OBS_INVALID_HEADER:
		return "UNAVAILABLE_INVALID_HEADER";
	case CLUSTER_UNDO_POOL_OBS_OK:
	case CLUSTER_UNDO_POOL_OBS_INVALID_OWNER:
	default:
		return "UNAVAILABLE_INVALID_OWNER";
	}
}

/* spec-3.8 D10: 4 NEW lifecycle counter accessors. */
uint64
cluster_undo_autoextend_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->autoextend_count);
}

/*
 * cluster_undo_tt_rollover_locked -- spec-3.12 D2b.
 *
 *	The node's TT-slot allocator filled its active segment with retained
 *	COMMITTED slots (a long reader holds the horizon below their commit_scns).
 *	Under lifecycle_lock, double-check no peer already rolled over, then extend
 *	the segment pool and rebind the allocator to the fresh segment.  Returns the
 *	new (or concurrently-rolled) active TT segment_id; 0 on extend failure, with
 *	*out_at_hard_cap set when the 53R9E hard cap was the cause.  Mirrors the
 *	record-write autoextend's double-checked lifecycle_lock pattern.
 *
 *	C17 lock ordering: lifecycle_lock is taken here, then seg->lock (inside
 *	cluster_tt_slot_current_segment / cluster_tt_slot_rollover) -- never the
 *	reverse.  The retention horizon (ProcArrayLock) is computed by the caller
 *	BEFORE this call and is not held across it.
 *
 *	On ERROR, the transaction error handler owns LWLockReleaseAll. ERROR has
 *	already reset InterruptHoldoffCount, so rethrow handlers must not call
 *	LWLockRelease on the allocator locks. Normal return paths still release
 *	their exact locks below.
 */
uint32
cluster_undo_tt_rollover_locked(int node_id, uint32 old_segment_id, bool *out_at_hard_cap)
{
	uint8 owner_instance = (uint8)(node_id + 1);
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0LiveOwnerPublication publication;
	ClusterUndoBlock0Result current_result;
	ClusterUndoSegmentExtendPlan extend_plan;
	uint32 cur;
	uint32 new_segment_id;
	uint32 reused = 0;
	bool selected = false;
	bool mark_old_committed = false;

	if (out_at_hard_cap != NULL)
		*out_at_hard_cap = false;

	if (UndoRecordShared == NULL)
		return 0;

	LWLockAcquire(&UndoRecordShared->cursor_lock.lock, LW_EXCLUSIVE);
	LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);

rollover_retry_locked:
	/* Double-checked: a peer may already have rolled this node over. */
	cur = cluster_tt_slot_current_segment(node_id);
	if (cur != 0 && cur != old_segment_id) {
		LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
		LWLockRelease(&UndoRecordShared->cursor_lock.lock);
		return cur;
	}

	PG_TRY();
	{
		selected = cluster_undo_segment_extend_or_create(owner_instance, &extend_plan);
	}
	PG_CATCH();
	{
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!selected) {
		if (out_at_hard_cap != NULL)
			*out_at_hard_cap = extend_plan.at_hard_cap;
		/* S3 forensics step 1a — TT-rollover-specific failure split (the
		 * record-extent CLAIM path counts its own hard-cap fails). */
		if (out_at_hard_cap != NULL && *out_at_hard_cap)
			pg_atomic_fetch_add_u64(&UndoRecordShared->tt_rollover_fail_hard_cap_count, 1);
		else
			pg_atomic_fetch_add_u64(&UndoRecordShared->tt_rollover_fail_extend_count, 1);
		LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
		/* step 1b: a rollover failure is EXACTLY when RECYCLABLE supply is the
		 * bottleneck — nudge the cleaner now instead of waiting out its tick
			 * (lifecycle_lock already released; wakeup is a latch set, lock-free). */
		cluster_undo_cleaner_wakeup();
		LWLockRelease(&UndoRecordShared->cursor_lock.lock);
		return 0;
	}
	new_segment_id = extend_plan.segment_id;
	if (extend_plan.needs_reuse || extend_plan.needs_provision) {
		LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
		PG_TRY();
		{
			if (extend_plan.needs_provision) {
				cluster_undo_segment_allocate(new_segment_id, owner_instance);
				reused = new_segment_id;
			} else
				reused = cluster_undo_segment_reuse_in_place(new_segment_id, owner_instance,
															 extend_plan.generation);
		}
		PG_CATCH();
		{
			PG_RE_THROW();
		}
		PG_END_TRY();
		LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);

		cur = cluster_tt_slot_current_segment(node_id);
		if (cur != 0 && cur != old_segment_id) {
			LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
			LWLockRelease(&UndoRecordShared->cursor_lock.lock);
			return cur;
		}
		if (cluster_undo_segment_read_state(new_segment_id, owner_instance)
			== (uint8)SEGMENT_ACTIVE)
			goto rollover_retry_locked;
		if (reused == 0
			|| (extend_plan.needs_provision
				&& cluster_undo_segment_generation(new_segment_id, owner_instance)
					   != extend_plan.generation)
			|| cluster_undo_segment_read_state(new_segment_id, owner_instance)
				   != (uint8)SEGMENT_ALLOCATED) {
			pg_atomic_fetch_add_u64(&UndoRecordShared->tt_rollover_fail_extend_count, 1);
			LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
			cluster_undo_cleaner_wakeup();
			LWLockRelease(&UndoRecordShared->cursor_lock.lock);
			return 0;
		}
	}
	cluster_undo_record_observation_apply_locked(owner_instance);

	/*
	 * A TT-only rollover segment does not receive record writes, so it would
	 * otherwise remain SEGMENT_ALLOCATED forever.  Put it into the same ACTIVE
	 * lifecycle state as record segments so a later drained rollover can advance
	 * ACTIVE -> COMMITTED -> RECYCLABLE.
	 */
	LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
	PG_TRY();
	{
		selected = cluster_undo_segment_mark_active(new_segment_id, owner_instance);
	}
	PG_CATCH();
	{
		PG_RE_THROW();
	}
	PG_END_TRY();
	LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);
	if (!selected) {
		/* step 1b: activation refused on a fresh segment — its own bucket (the
		 * extend/hard-cap split above cannot see this failure). */
		pg_atomic_fetch_add_u64(&UndoRecordShared->tt_rollover_fail_activate_count, 1);
		LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
		cluster_undo_cleaner_wakeup();
		LWLockRelease(&UndoRecordShared->cursor_lock.lock);
		return 0;
	}
	cur = cluster_tt_slot_current_segment(node_id);
	if ((cur != 0 && cur != old_segment_id)
		|| cluster_undo_segment_read_state(new_segment_id, owner_instance)
			   != (uint8)SEGMENT_ACTIVE) {
		LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
		LWLockRelease(&UndoRecordShared->cursor_lock.lock);
		return cur != 0 && cur != old_segment_id ? cur : 0;
	}

	/* Candidate-2 current publication can acquire GES/XCUR and therefore
	 * cannot run while lifecycle_lock is held.  The new segment is not yet
	 * visible as the allocator's canonical TT binding, so release the local
	 * lifecycle lock, publish through the sole live-owner producer, then
	 * relock and revalidate before cluster_tt_slot_rollover exposes it. */
	logical.owner_instance = owner_instance;
	logical.segment_id = new_segment_id;
	memset(&publication, 0, sizeof(publication));
	LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
	PG_TRY();
	{
		current_result = cluster_undo_block0_current_live_owner_ensure_resident_exact(
			&logical, 10000, &publication);
	}
	PG_CATCH();
	{
		PG_RE_THROW();
	}
	PG_END_TRY();
	LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);

	/* A peer may have completed a different exact rollover while XCUR was in
	 * flight.  Adopt that already-published canonical segment and leave this
	 * unused ACTIVE segment unbound; never overwrite the winner. */
	cur = cluster_tt_slot_current_segment(node_id);
	if (cur != 0 && cur != old_segment_id) {
		LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
		LWLockRelease(&UndoRecordShared->cursor_lock.lock);
		return cur;
	}
	if (cur != old_segment_id || current_result != CLUSTER_UNDO_BLOCK0_OK
		|| cluster_undo_segment_read_state(new_segment_id, owner_instance) != (uint8)SEGMENT_ACTIVE
		|| !cluster_undo_block0_current_live_owner_publication_recheck(&publication)) {
		LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
		cluster_undo_cleaner_wakeup();
		LWLockRelease(&UndoRecordShared->cursor_lock.lock);
		return 0;
	}

	{
		bool old_had_active = false;
		uint32 fixed_first = (uint32)node_id * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;

		cluster_tt_slot_rollover(node_id, new_segment_id, &old_had_active);

		/*
		 * spec-3.12 D3: transition the drained old segment to SEGMENT_COMMITTED
		 * so retention reclaim (spec-3.13) can pick it up once the horizon
		 * passes its watermark.  Guards keep it strictly safe in this lazy MVP:
		 *   - !old_had_active: no in-flight ACTIVE TT slot remains, so "all tx
		 *     committed" holds for the segment.
		 *   - old != fixed_first: the spec-3.4b fixed segment is shared with the
		 *     record-write cursor (both start there); never mark it COMMITTED.
		 *   - old != record active_segment_id: the segment is not the record
		 *     cursor's current write target.
		 * A rolled-over TT segment is otherwise TT-exclusive (extend_or_create
		 * hands disjoint ids to the record vs TT paths), so this is conflict-free.
		 */
		if (old_segment_id != 0 && !old_had_active && old_segment_id != fixed_first
			&& old_segment_id != UndoRecordShared->active_segment_id)
			mark_old_committed = true;
	}

	if (old_segment_id != 0)
		pg_atomic_fetch_add_u64(&UndoRecordShared->tt_retention_rollover_count, 1);

	/* Q8: a retention-pressure rollover is exactly when RECYCLABLE supply
	 * matters -- nudge the cleaner instead of waiting out its interval. */
	cluster_undo_cleaner_wakeup();
	/*
	 * spec-3.12 D5: the rolled-away segment's committed slots all have
	 * commit_scn at or newer than the horizon (that retention is exactly why
	 * the rollover fired), so its retention watermark is not older than the
	 * horizon -> it was skipped for recycle.
	 */
	if (old_segment_id != 0)
		pg_atomic_fetch_add_u64(&UndoRecordShared->segment_retain_skip_count, 1);

	LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
	if (mark_old_committed) {
		PG_TRY();
		{
			(void)cluster_undo_segment_mark_committed(old_segment_id, owner_instance);
		}
		PG_CATCH();
		{
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
	LWLockRelease(&UndoRecordShared->cursor_lock.lock);
	return new_segment_id;
}

uint64
cluster_undo_tt_retention_rollover_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->tt_retention_rollover_count);
}

/* S3 forensics step 1a — TT-rollover failure split accessors. */
uint64
cluster_undo_tt_rollover_fail_hard_cap_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->tt_rollover_fail_hard_cap_count);
}

uint64
cluster_undo_tt_rollover_fail_extend_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->tt_rollover_fail_extend_count);
}

uint64
cluster_undo_tt_rollover_fail_activate_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->tt_rollover_fail_activate_count);
}

uint64
cluster_undo_segment_retain_skip_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->segment_retain_skip_count);
}

uint64
cluster_undo_segment_switch_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->segment_switch_count);
}

uint64
cluster_undo_segment_create_fail_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->segment_create_fail_count);
}

uint64
cluster_undo_segment_hard_cap_fail_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->segment_hard_cap_fail_count);
}

/* P0 perf hardening: per-commit undo fsync counter accessors. */
uint64
cluster_undo_commit_fsync_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->commit_fsync_count);
}

uint64
cluster_undo_commit_fsync_segment_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->commit_fsync_segment_count);
}

uint64
cluster_undo_commit_fsync_failure_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->commit_fsync_failure_count);
}

/* P0 perf hardening: smgr syscall counter bumps (called from
 * cluster_undo_smgr.c) + accessors. */
void
cluster_undo_record_note_smgr_open(void)
{
	if (UndoRecordShared != NULL)
		pg_atomic_fetch_add_u64(&UndoRecordShared->smgr_open_count, 1);
}
void
cluster_undo_record_note_smgr_close(void)
{
	if (UndoRecordShared != NULL)
		pg_atomic_fetch_add_u64(&UndoRecordShared->smgr_close_count, 1);
}
void
cluster_undo_record_note_smgr_pread(void)
{
	if (UndoRecordShared != NULL)
		pg_atomic_fetch_add_u64(&UndoRecordShared->smgr_pread_count, 1);
}
void
cluster_undo_record_note_smgr_pwrite(void)
{
	if (UndoRecordShared != NULL)
		pg_atomic_fetch_add_u64(&UndoRecordShared->smgr_pwrite_count, 1);
}

uint64
cluster_undo_smgr_open_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->smgr_open_count);
}
uint64
cluster_undo_smgr_close_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->smgr_close_count);
}
uint64
cluster_undo_smgr_pread_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->smgr_pread_count);
}
uint64
cluster_undo_smgr_pwrite_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->smgr_pwrite_count);
}


/*
 * spec-3.8 Fix 6: deterministic autoextend trigger test hook.
 *
 *	Forces the active segment cursor to point at the last data block,
 *	then sets free_offset to the block-tail boundary so the next
 *	record write triggers cluster_undo_block_has_space() == false and
 *	current_block++ pushes past UNDO_BLOCKS_PER_SEGMENT — same path the
 *	natural exhaustion runs through, so autoextend / hard-cap / 53R9E
 *	behaviors get exercised without writing 64 MB of records.
 *
 *	Returns true if the hook published a forced cursor;  false if no
 *	active segment yet (caller must allocate one first).
 *
 *	Caller MUST be superuser (TAP test wraps with security definer
 *	function or runs as initdb superuser).  cursor_lock taken EXCLUSIVE.
 */
bool
cluster_undo_test_force_segment_end(void)
{
	if (UndoRecordShared == NULL)
		return false;

	/*
	 * spec-3.18 D3: drive the EXTENT high-water (next_extent_block) to the
	 * segment end, so the next claim_undo_extent computes a 0-block extent and
	 * runs the autoextend / reuse / hard-cap path -- without writing 64 MB.
	 * next_extent_block is protected by lifecycle_lock (the claim lock), not
	 * cursor_lock.  Also reset THIS backend's held extent so a same-session DML
	 * re-claims;  a fresh-backend DML (new connection) starts with no extent
	 * anyway.  The old cursor fields are set too for pre-D3 observability.
	 */
	LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);

	if (UndoRecordShared->active_segment_id == 0) {
		LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
		return false;
	}

	UndoRecordShared->next_extent_block = UNDO_BLOCKS_PER_SEGMENT; /* claim -> 0 -> autoextend */
	UndoRecordShared->current_block = UNDO_BLOCKS_PER_SEGMENT - 1;
	UndoRecordShared->free_offset = BLCKSZ; /* triggers has_space() == false */
	UndoRecordShared->slot_count = 0;
	UndoRecordShared->block_dirty = 0;

	LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);

	memset(&cluster_undo_current_extent, 0, sizeof(cluster_undo_current_extent));
	return true;
}


/*
 * cluster_undo_record_active_segment_id -- spec-3.13 D3 accessor.
 *
 *	Snapshot of the shared record cursor's active segment (0 = none yet).
 *	cursor_lock SHARED for a consistent read; callers use it as an
 *	exclusion hint (the lifecycle_lock recheck in the advance wrapper is
 *	the authoritative guard).
 */
uint32
cluster_undo_record_active_segment_id(void)
{
	uint32 seg;

	if (UndoRecordShared == NULL)
		return 0;

	LWLockAcquire(&UndoRecordShared->cursor_lock.lock, LW_SHARED);
	seg = UndoRecordShared->active_segment_id;
	LWLockRelease(&UndoRecordShared->cursor_lock.lock);
	return seg;
}


/*
 * cluster_undo_segment_advance_recyclable -- spec-3.13 D3 orchestration.
 *
 *	Takes the undo lifecycle_lock (Q6: the cleaner is just another
 *	low-frequency allocator caller -- same lock order, no new lock
 *	level), double-checks the segment is not the active record cursor
 *	segment, then runs the COMMITTED -> RECYCLABLE transition with the
 *	v0.3 (1) durability contract.  Horizon was computed by the caller
 *	BEFORE this lock (C17).
 */
ClusterUndoSegTryRecycle
cluster_undo_segment_advance_recyclable(uint32 segment_id, SCN horizon, uint64 expected_epoch)
{
	ClusterUndoSegTryRecycle result;
	uint8 owner;

	if (UndoRecordShared == NULL || cluster_node_id < 0)
		return CLUSTER_SEG_RECYCLE_READ_FAIL;

	owner = (uint8)(cluster_node_id + 1);

	LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);
	if (segment_id == UndoRecordShared->active_segment_id) {
		LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
		return CLUSTER_SEG_RECYCLE_NOT_COMMITTED; /* writer-active: never a candidate */
	}

	/*
	 * spec-5.22e F-D2 epoch fence, INSIDE the mutation lock and before the
	 * WAL/pwrite/fsync three-step: a reconfig epoch bump after the floor was
	 * folded voids its member coverage.  Refuse the mutation; the caller
	 * aborts the whole pass.
	 */
	if (cluster_undo_horizon_epoch_fence_tripped(expected_epoch)) {
		LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
		return CLUSTER_SEG_RECYCLE_EPOCH_CHANGED;
	}

	LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
	result = cluster_undo_segment_try_mark_recyclable(segment_id, owner, horizon, expected_epoch);

	/*
	 * PGRAC: spec-6.12i CP5 (D-i4) -- a RECYCLABLE segment's durable TT
	 * slots vanish for the by-xid complete scan once the segment file is
	 * reused (wiped), so this segment-granularity recycle must feed the max
	 * gate-horizon tracker exactly like a slot-level recycle: the transition
	 * proved every slot's commit_scn at/below `horizon`.  Fed at the
	 * ADVANCED moment (happens-before any wipe of this segment).
	 */
	if (result == CLUSTER_SEG_RECYCLE_ADVANCED)
		cluster_tt_slot_note_gated_recycle_horizon(horizon);

	return result;
}


/*
 * cluster_undo_segment_advance_committed -- spec-4.12a D2 (Q3-C cleaner
 *	fallback, correctness-necessary).
 *
 *	Re-evaluate a rolled-away record segment for the ACTIVE -> COMMITTED drain.
 *	The rollover-time attempt (cluster_undo_record_alloc) always runs while the
 *	writing transaction is still in-flight, so it stamps the seal but RETAINS
 *	the segment (guard 1); only after that writer commits / aborts can the
 *	segment drain.  Nothing re-triggers the rollover path, so without this
 *	cleaner pass the segment would stay SEGMENT_ACTIVE forever -- the spec-4.13
	 *	leak.  The shared drain helper acquires the cursor owner, snapshots under
	 *	lifecycle_lock, releases it, and then enters exact block0 current with
	 *	seal_scn =
 *	cluster_scn_current(): for an already-sealed segment the stamp is a no-op
 *	(the helper never overwrites an existing seal); for a segment rolled away
 *	with the GUC off (never sealed) `now` is a safe conservative upper bound (a
 *	rolled-away non-active segment receives no further writes and every
 *	in-flight writer's registered first_undo_scn is at or below now).  The active record / TT
 *	segment is excluded by the drain gate itself (guard 3).
 */
void
cluster_undo_segment_advance_committed(uint32 segment_id)
{
	uint8 owner;

	if (UndoRecordShared == NULL || cluster_node_id < 0)
		return;

	owner = (uint8)(cluster_node_id + 1);

	cluster_undo_try_mark_record_segment_committed(segment_id, owner, cluster_scn_current());
}


/* spec-3.13 D4: allocator-side reuse counter hook (alloc.c has no shmem view). */
void
cluster_undo_record_note_segment_reuse(void)
{
	if (UndoRecordShared != NULL)
		pg_atomic_fetch_add_u64(&UndoRecordShared->segment_reuse_count, 1);
}

uint64
cluster_undo_segment_reuse_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->segment_reuse_count);
}
