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
 *	    - cluster_undo_record_xact_reset()  — end-of-xact hook
 *	    - cluster_undo_record_is_touched()  — for D16 PREPARE guard
 *
 *	  Concurrency model(MVP):
 *	    - single per-instance LWLock(`cursor_lock`)protects active segment
 *	      cursor advance(active_segment_id / current_block / free_offset /
 *	      slot_count).多 backend 写同 instance → serialize on this lock.
 *	    - per-backend `cluster_undo_touched` static bool tracks D16 state;
 *	      reset on xact end.
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
#include "access/xact.h"
#include "port/atomics.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h" /* spec-3.18 D2b MyProc->delayChkptFlags (DELAY_CHKPT_START) */
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/wait_event.h" /* spec-3.18 D7: ClusterUndoExtentClaim wait event */

#include "cluster/cluster_guc.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_tt_slot.h"	  /* spec-3.12 D2b: TT allocator rollover */
#include "cluster/cluster_undo_cleaner.h" /* Q8 pressure wakeup (3.13) */
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_format.h"
#include "cluster/cluster_undo_record.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_undo_extent.h"	  /* spec-3.18 D3 per-txn extent */
#include "cluster/storage/cluster_undo_buf.h" /* spec-3.18 D1 read/write-through */
#include "cluster/storage/cluster_undo_alloc.h"
#include "cluster/storage/cluster_undo_xlog.h" /* spec-3.18 D2a XLOG_UNDO_BLOCK_WRITE */

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

	/* spec-3.18 D3: extent claims (one per ~undo_extent_blocks records per
	 * backend instead of one cursor_lock acquire per record). */
	pg_atomic_uint64 extent_claim_count;

	/* spec-3.12 D5: undo segments skipped for recycle because their retention
	 * watermark was >= the horizon.  In this lazy MVP the active segment is the
	 * one skipped at each retention rollover; spec-3.13's proactive scan will
	 * also bump this. */
	pg_atomic_uint64 segment_retain_skip_count;

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

	LWLockPadded cursor_lock;
	/* spec-3.8 D3: lifecycle_lock — protects autoextend slow path
	 * (active_segment_id publication + state transitions).  Per spec
	 * §3.2:  写 record 只 cursor_lock;  撞满 → release cursor_lock →
	 * acquire lifecycle_lock → recheck active_segment_id → 必要
	 * re-acquire cursor_lock 发布 NEW active.  禁止同时长期持有两锁
	 * 做 I/O (但 lifecycle_lock 可持有期间 file create + fsync). */
	LWLockPadded lifecycle_lock;
} ClusterUndoRecordShared;


static ClusterUndoRecordShared *UndoRecordShared = NULL;

/* Per-backend state(D16 PREPARE guard support). */
static bool cluster_undo_touched_in_xact = false;

/*
 * spec-3.18 D3:  the backend's currently-held undo extent.  segment_id ==
 * CLUSTER_UNDO_EXTENT_NONE means "no extent held" (claim one on next write).
 * The cursor (cur_block / cur_free_offset / cur_slot_count) advances lock-free
 * within the extent;  only claiming a new extent touches lifecycle_lock.  Reset
 * to NONE at end of xact (cluster_undo_record_xact_reset) -- residual blocks
 * are dropped (D3.3 will add the Q2 hybrid residual cache).
 */
static ClusterUndoExtent cluster_undo_current_extent = { 0 };

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
 *	Reset (clear) happens at end of top-xact in cluster_undo_record_xact_reset.
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
 * the undo segment header.  This state is reset at xact end together with
 * cluster_undo_touched_in_xact.
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
 * cluster_undo_record_shmem_size
 *	  Bytes required by the record-level cursor + counter shmem region.
 */
Size
cluster_undo_record_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterUndoRecordShared));
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

	UndoRecordShared
		= ShmemInitStruct("ClusterUndoRecordShared", cluster_undo_record_shmem_size(), &found);

	if (!found) {
		int tranche_id = LWLockNewTrancheId();

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

		/* spec-3.8 D10: 4 NEW lifecycle counters. */
		pg_atomic_init_u64(&UndoRecordShared->autoextend_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->segment_switch_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->segment_create_fail_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->segment_hard_cap_fail_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->segment_reuse_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->tt_retention_rollover_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->segment_retain_skip_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->extent_claim_count, 0); /* spec-3.18 D3 */

		/* P0 perf hardening: per-commit undo fsync counters. */
		pg_atomic_init_u64(&UndoRecordShared->commit_fsync_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->commit_fsync_segment_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->commit_fsync_failure_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->smgr_open_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->smgr_close_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->smgr_pread_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->smgr_pwrite_count, 0);

		LWLockInitialize(&UndoRecordShared->cursor_lock.lock, tranche_id);
		LWLockInitialize(&UndoRecordShared->lifecycle_lock.lock, tranche_id);
		LWLockRegisterTranche(tranche_id, "cluster_undo_record_cursor");
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
	 * lifecycle_lock 复用 cluster_undo_record_cursor region per
	 * L206 5 步流程 — 不引入 NEW region. */
	.lwlock_count = 2,
	.owner_subsys = "cluster_undo_record",
	.reserved_flags = 0,
};

void
cluster_undo_record_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_undo_record_region);
}


/* ---- Helper: compute record total length per record_type ---- */

static inline uint16
undo_record_total_length(uint8 record_type, uint16 payload_len)
{
	return (uint16)(sizeof(UndoRecordHeader) + payload_len);
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
	char *img;

	/*
	 * spec-3.18 D1:  read-through the undo buffer pool for DATA blocks.  A NULL
	 * pin means block 0 (not poolable) or the pool is disabled — fall back to
	 * the direct smgr read.  On a hit/miss-fill the pool owns the disk read.
	 */
	img = cluster_undo_buf_pin(segment_id, owner_instance, block_no, CLUSTER_UNDO_BUF_SHARED, &pin);
	if (img == NULL)
		return cluster_undo_smgr_read_block(segment_id, owner_instance, block_no, buf);
	memcpy(buf, img, BLCKSZ);
	cluster_undo_buf_unpin(&pin);
	return true;
}


static bool
write_undo_block(uint32 segment_id, uint8 owner_instance, uint32 block_no, const char *buf,
				 bool do_fsync, XLogRecPtr wal_lsn)
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
		return cluster_undo_smgr_write_block(segment_id, owner_instance, block_no, buf, true);

	/*
	 * Write-through the pool for DATA blocks:  update the cached image and
	 * pwrite (do_fsync=false) so durability is identical to today while reads
	 * stay coherent.  NULL pin (block 0 / pool disabled) -> direct write.
	 */
	img = cluster_undo_buf_pin(segment_id, owner_instance, block_no, CLUSTER_UNDO_BUF_EXCLUSIVE,
							   &pin);
	if (img == NULL)
		return cluster_undo_smgr_write_block(segment_id, owner_instance, block_no, buf, false);

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
		 * (gate off) writes now; write-back (gate on) defers behind this LSN. */
		cluster_undo_buf_mark_dirty(&pin, wal_lsn);
	}
	PG_FINALLY();
	{
		cluster_undo_buf_unpin(&pin);
	}
	PG_END_TRY();
	return true;
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
	bool ok;

	if (!cluster_undo_buf_writeback_allowed()) {
		/* D2a: always-FPI, write-through (no checkpoint-race window). */
		lsn = cluster_undo_emit_block_write(owner_instance, segment_id, block_no, block_buf,
											old_block_lsn, rec_off, rec_len, slot_off);
		blkhdr->block_lsn = lsn;
		return write_undo_block(segment_id, owner_instance, block_no, block_buf,
								/* do_fsync = */ false, lsn);
	}

	/* D2b: DELAY_CHKPT_START spans the decision + insert + block write. */
	MyProc->delayChkptFlags |= DELAY_CHKPT_START;
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
		MyProc->delayChkptFlags &= ~DELAY_CHKPT_START;
	}
	PG_END_TRY();
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
 *	On success *ext is a held extent parked at its first (fresh) block.  Errors
 *	release the lock first (caller ereports).  Activation mirrors the pre-D3
 *	path: mark_active + tail_block_init(1) whenever a segment becomes active
 *	(tail_block=1 is the conservative retention base, safe on restart resume).
 */
static UndoExtentClaimResult
claim_undo_extent(ClusterUndoExtent *ext, uint8 owner_instance, uint32 ensured_segment_id,
				  SCN current_scn)
{
	uint32 seg;
	uint32 hw;
	uint32 n;
	bool needs_activation;

	LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);

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
		/* Segment full / corrupt bitmap -> autoextend (reuse-first inside). */
		uint32 old_seg = seg;
		bool at_hard_cap = false;
		uint32 new_seg;

		/*
		 * PGRAC (spec-3.18 D7): attribute the autoextend file-create + fsync I/O
		 * (the lifecycle_lock-held slow path) to a dedicated wait event so a
		 * backend blocked extending undo is visible in pg_stat_activity.  Only
		 * the I/O is wrapped -- the lifecycle_lock acquire is already attributed
		 * to its LWLock tranche (A1: no double-attribution of the lock wait).
		 */
		pgstat_report_wait_start(WAIT_EVENT_CLUSTER_UNDO_EXTENT_CLAIM);
		new_seg = cluster_undo_segment_extend_or_create(owner_instance, &at_hard_cap);
		pgstat_report_wait_end();

		if (new_seg == 0) {
			if (at_hard_cap)
				pg_atomic_fetch_add_u64(&UndoRecordShared->segment_hard_cap_fail_count, 1);
			else
				pg_atomic_fetch_add_u64(&UndoRecordShared->segment_create_fail_count, 1);
			LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
			return at_hard_cap ? CLAIM_HARD_CAP : CLAIM_FS_FAIL;
		}
		pg_atomic_fetch_add_u64(&UndoRecordShared->autoextend_count, 1);
		pg_atomic_fetch_add_u64(&UndoRecordShared->segment_switch_count, 1);
		if (old_seg != 0 && old_seg != new_seg)
			(void)cluster_undo_segment_mark_full(old_seg, owner_instance);
		seg = new_seg;
		needs_activation = true;
		hw = 1; /* fresh segment: data blocks from 1 */
		n = cluster_undo_extent_compute(hw, (uint32)cluster_undo_extent_blocks,
										UNDO_BLOCKS_PER_SEGMENT, ext);
		/* fresh segment has full room => n > 0 */
	}

	if (needs_activation) {
		/*
		 * review P1-C: init tail_block (the retention base) ONLY for a fresh
		 * (ALLOCATED) segment.  A restart-resumed segment is already ACTIVE on
		 * disk with a cleaner-advanced tail_block -- resetting it to 1 would
		 * make the cleaner re-scan + over-retain after every restart.
		 */
		bool is_fresh
			= (cluster_undo_segment_read_state(seg, owner_instance) == (uint8)SEGMENT_ALLOCATED);

		if (!cluster_undo_segment_mark_active(seg, owner_instance)
			|| (is_fresh && !cluster_undo_segment_tail_block_init(seg, owner_instance, 1))) {
			LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
			return CLAIM_IO_FAIL;
		}
		pg_atomic_fetch_add_u64(&UndoRecordShared->segment_claim_count, 1);
		UndoRecordShared->block_first_scn = current_scn;
	}

	/* A1: batch-mark the whole claimed range used in one block-0 RMW + fsync. */
	if (!cluster_undo_segment_mark_block_range_used(seg, owner_instance, hw, n)) {
		LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
		return CLAIM_IO_FAIL;
	}

	/* Publish: advance high-water + active segment. */
	UndoRecordShared->next_extent_block = hw + n;
	UndoRecordShared->active_segment_id = seg;
	ext->segment_id = seg;
	pg_atomic_fetch_add_u64(&UndoRecordShared->extent_claim_count, 1);

	LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
	return CLAIM_OK;
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
UBA
cluster_undo_record_alloc(uint8 record_type, const ClusterUndoRecordTarget *target,
						  uint16 tt_slot_segment_id, uint16 tt_slot_offset, const void *payload,
						  uint16 payload_len, UBA prev_uba)
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
	char block_buf[BLCKSZ];
	UndoBlockHeader *blkhdr;
	UndoRecordHeader *rechdr;
	UndoSlotDirEntry *slot;
	SCN current_scn;
	uint32 ensured_segment_id;
	int local_head_idx;
	UBA effective_prev_uba;
	bool first_in_tx;

	/* Input validation. */
	if (record_type == UNDO_RECORD_INVALID || record_type > UNDO_RECORD_ITL)
		return InvalidUba;
	if (target == NULL || payload == NULL)
		return InvalidUba;

	if (UndoRecordShared == NULL)
		return InvalidUba; /* shmem not initialized */

	record_length = undo_record_total_length(record_type, payload_len);

	/* Enforce inline cap GUC. */
	inline_cap = (uint16)cluster_undo_record_inline_max_bytes;
	if (payload_len > inline_cap)
		return InvalidUba; /* caller ereport 53R9D oversize */

	/* Hard cap (must fit in single block). */
	if (record_length > UNDO_RECORD_HARD_CAP_BYTES)
		return InvalidUba;

	/* Owner instance is current node_id + 1 (per cluster_undo_alloc.c
	 * convention: owner_instance is 1-indexed). */
	owner_instance = (uint8)(cluster_node_id + 1);

	/*
	 * Segment creation may ereport(ERROR), emit WAL, and perform filesystem I/O.
	 * Do it before taking the record cursor LWLock; otherwise an ERROR would
	 * leave the LWLock held and wedge all future undo writers in this backend.
	 */
	ensured_segment_id = cluster_undo_active_segment_for_node_or_create(cluster_node_id);
	if (ensured_segment_id == 0)
		return InvalidUba;

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

	if (!cluster_undo_local_head_ensure(tt_slot_segment_id, tt_slot_offset, &local_head_idx))
		return InvalidUba;
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
		 * active one.  The active segment is never recycled (recycle requires
		 * COMMITTED/RECYCLABLE state), so a residual whose segment_id still
		 * equals active_segment_id can never point at a reborn segment -> safe.
		 * Once the allocator rolled over (segment_id != active), the old segment
		 * may be FULL / COMMITTED / RECYCLABLE / reborn, so drop the residual.
		 *
		 * The active_segment_id read here is an INTENTIONALLY relaxed (unlocked)
		 * hint.  A stale read cannot cause data loss: the worst case is reusing a
		 * residual whose segment just rolled to FULL -- but those blocks are
		 * already claimed by us (bitmap-marked) and a FULL segment is not yet
		 * recyclable (retention horizon), so writing our own pre-claimed blocks
		 * is still correct.  Block 0 / record writes never go through the stale
		 * path uncrosschecked because the per-record write only touches our own
		 * extent's claimed blocks.
		 */
		if (ext->segment_id != CLUSTER_UNDO_EXTENT_NONE
			&& ext->segment_id != UndoRecordShared->active_segment_id)
			ext->segment_id = CLUSTER_UNDO_EXTENT_NONE; /* stale residual -> re-claim */

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
				return InvalidUba; /* block-0 header I/O fail */
			}
		}

		segment_id = ext->segment_id;
		current_block = ext->cur_block;
		free_offset = ext->cur_free_offset;
		slot_count = ext->cur_slot_count;
	}

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
		if (!read_undo_block(segment_id, owner_instance, current_block, block_buf))
			return InvalidUba; /* I/O fail (no shared lock held in D3 path) */
		blkhdr = (UndoBlockHeader *)block_buf;
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
	if (!cluster_undo_wal_protect_block(
			segment_id, owner_instance, current_block, block_buf, blkhdr->block_lsn,
			(uint16)free_offset, (uint16)record_length, (uint16)UNDO_SLOT_DIR_OFFSET(new_slot_idx)))
		return InvalidUba; /* I/O fail (no shared lock held in D3 path) */

	pg_atomic_fetch_add_u64(&UndoRecordShared->block_write_count, 1);
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

	/* Mark backend touched for D16 PREPARE guard. */
	cluster_undo_touched_in_xact = true;

	/* P0 perf hardening: record the dirtied undo segment for a single per-xact
	 * precommit fsync.  Done OUTSIDE cursor_lock (released above). */
	cluster_undo_record_touched_segment(segment_id, owner_instance);

	/* Encode UBA per spec-3.4b: (segment_id, block_no, tt_slot_offset, row_offset).
	 * For undo records, row_offset = slot-dir index within block. */
	result = uba_encode((uint32)segment_id, current_block, tt_slot_offset, new_slot_idx);
	cluster_undo_local_heads[local_head_idx].head = result;

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

	if (!uba_decode(uba, &segment_id, &block_no, &tt_slot_offset, &row_offset))
		return 0;

	/* Map segment_id back to owner_instance.  Per spec-3.4b convention:
	 *   owner_instance = (segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 */
	owner_instance = (uint8)((segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1);

	/* Own-instance only at spec-3.7. */
	if (owner_instance != (uint8)(cluster_node_id + 1)) {
		ereport(WARNING,
				(errmsg("cluster_undo_get_record: cross-instance read not supported at spec-3.7"),
				 errhint("cross-instance undo read 推 spec-3.9 CR construction / Cache Fusion")));
		return 0;
	}

	if (!read_undo_block(segment_id, owner_instance, block_no, block_buf))
		return 0;

	blkhdr = (UndoBlockHeader *)block_buf;
	if (blkhdr->magic != PGRAC_UNDO_BLOCK_MAGIC)
		return 0;

	if (row_offset >= blkhdr->slot_count)
		return 0;

	slot = UNDO_SLOT_DIR_PTR(block_buf, row_offset);
	if (slot->record_offset == 0 || slot->record_length == 0)
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
 *	No-op for a xact that wrote no undo (DDL-only / read-only).  The touched
 *	list is cleared by cluster_undo_record_xact_reset at end of xact.
 */
void
cluster_undo_xact_precommit_flush(void)
{
	int i;

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
 * cluster_undo_record_xact_reset -- end-of-xact hook;  reset per-backend
 *	touched flag.  Called from xact.c CommitTransaction / AbortTransaction.
 *
 *	Also clears the per-xact touched-undo-segment list.  On commit the list was
 *	already fsync'd by cluster_undo_xact_precommit_flush; on abort it is simply
 *	dropped (un-fsync'd undo of an aborted xact needs no durability) — this is
 *	the lazy/best-effort abort path.  Clearing here prevents leakage into the
 *	next xact on this backend.
 */
void
cluster_undo_record_xact_reset(void)
{
	cluster_undo_touched_in_xact = false;
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
	/* P0 perf hardening: drop the cached undo segment fd at xact end to bound
	 * stale-fd exposure (e.g. a recycled segment) across transactions. */
	cluster_undo_smgr_fd_cache_reset();
}


/*
 * cluster_undo_record_is_touched -- D16 PREPARE TRANSACTION guard helper.
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
 */
uint32
cluster_undo_tt_rollover_locked(int node_id, uint32 old_segment_id, bool *out_at_hard_cap)
{
	uint8 owner_instance = (uint8)(node_id + 1);
	uint32 cur;
	uint32 new_segment_id;

	if (out_at_hard_cap != NULL)
		*out_at_hard_cap = false;

	if (UndoRecordShared == NULL)
		return 0;

	LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);

	/* Double-checked: a peer may already have rolled this node over. */
	cur = cluster_tt_slot_current_segment(node_id);
	if (cur != 0 && cur != old_segment_id) {
		LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
		return cur;
	}

	new_segment_id = cluster_undo_segment_extend_or_create(owner_instance, out_at_hard_cap);
	if (new_segment_id == 0) {
		LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
		return 0;
	}

	/*
	 * A TT-only rollover segment does not receive record writes, so it would
	 * otherwise remain SEGMENT_ALLOCATED forever.  Put it into the same ACTIVE
	 * lifecycle state as record segments so a later drained rollover can advance
	 * ACTIVE -> COMMITTED -> RECYCLABLE.
	 */
	if (!cluster_undo_segment_mark_active(new_segment_id, owner_instance)) {
		LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
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
		if (!old_had_active && old_segment_id != fixed_first
			&& old_segment_id != UndoRecordShared->active_segment_id)
			(void)cluster_undo_segment_mark_committed(old_segment_id, owner_instance);
	}

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
	pg_atomic_fetch_add_u64(&UndoRecordShared->segment_retain_skip_count, 1);

	LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);
	return new_segment_id;
}

uint64
cluster_undo_tt_retention_rollover_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->tt_retention_rollover_count);
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
cluster_undo_segment_advance_recyclable(uint32 segment_id, SCN horizon)
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
	result = cluster_undo_segment_try_mark_recyclable(segment_id, owner, horizon);
	LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);

	return result;
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
