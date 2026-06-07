/*-------------------------------------------------------------------------
 *
 * cluster_undo_buf.c
 *	  pgrac spec-3.18 D1 — per-instance undo block buffer pool.
 *
 *	  AD-014 form restoration:  active undo DATA blocks live in this in-memory
 *	  shmem pool instead of being direct-written per record.  D1 ships the
 *	  pool infrastructure in a durability-NEUTRAL form (interface-lock §3):
 *	    - read-through cache + write-through writes (do_fsync=false;  fsync
 *	      stays at cluster_undo_xact_precommit_flush, NOT per-unpin);
 *	    - buffered write-back is forbidden until the D2 alpha
 *	      (cluster_undo_buf_writeback_allowed() == false + a GUC check_hook
 *	      hard-rejects turning it on);
 *	    - block 0 (segment header + durable TT slots) is NOT poolable.
 *
 *	  Concurrency (NOTES):  a miss does disk I/O OUTSIDE the pool map_lock —
 *	  this is a performance spec, so we never hold a global lock across an
 *	  8KB read.  Pattern (standard buffer-read):
 *	    map_lock EXCLUSIVE -> reserve a victim slot (pincount=1, io_in_progress,
 *	    hold its content_lock EXCLUSIVE) -> release map_lock -> read_block ->
 *	    release content_lock + clear io_in_progress -> re-acquire content_lock
 *	    in the caller's mode.  Waiters that find io_in_progress block on the
 *	    filler's content_lock.  The reserving pin (pincount=1) keeps the slot
 *	    un-evictable across the I/O window.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/storage/cluster_undo_buf.c
 *
 * NOTES
 *	  pgrac-original file.  Spec: spec-3.18-write-path-performance-overhaul.md
 *	  (FROZEN v0.6 RE-SCOPE).  Interface locked by
 *	  docs/spec-3.18-d1d2-interface-lock.md (v1.1).
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "access/xlog.h" /* XLogFlush — WAL-before-data on write-back (D2b) */
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/storage/cluster_undo_buf.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/errcodes.h"

/* GUCs (registered in cluster_guc.c). */
extern int cluster_undo_buffers;		   /* pool slot count;  0 = disabled */
extern bool cluster_undo_buffer_writeback; /* must stay off in D1 (check_hook) */

/* One pool slot.  BLCKSZ data lives in a parallel array (alignment). */
typedef struct UndoBufSlot {
	LWLock content_lock; /* SHARED reader / EXCLUSIVE writer */
	uint32 segment_id;
	uint8 owner;
	uint32 block_no;
	bool valid;				   /* slot holds a real block */
	bool io_in_progress;	   /* a backend is filling it from disk */
	bool dirty;				   /* write-back only;  D1 write-through => false */
	pg_atomic_uint32 pincount; /* > 0 => not evictable */
	uint32 clock_used;		   /* clock-sweep recency hint (0/1) */
	XLogRecPtr last_wal_lsn;   /* D2: LSN protecting a dirty block */
} UndoBufSlot;

typedef struct ClusterUndoBufPool {
	LWLock map_lock; /* slot lookup / allocate / evict */
	int nslots;
	uint32 clock_hand;
	pg_atomic_uint64 hit_count;
	pg_atomic_uint64 miss_count;
	pg_atomic_uint64 writethrough_count;
	pg_atomic_uint64 evict_count;
	/* UndoBufSlot slots[nslots] follows; char data[nslots * BLCKSZ] after. */
} ClusterUndoBufPool;

static ClusterUndoBufPool *UndoBufPool = NULL;
static UndoBufSlot *UndoBufSlots = NULL;
static char *UndoBufData = NULL;

#define SLOT_DATA(i) (UndoBufData + ((Size)(i)) * BLCKSZ)


/*
 * cluster_undo_buf_writeback_allowed -- the D1 alpha gate (interface-lock §5).
 *	Hard false during D1:  buffered write-back has no WAL protection yet.
 *	D2 flips this after the crash-restart redo proof.
 */
bool
cluster_undo_buf_writeback_allowed(void)
{
	return false; /* D1 phase — opened only by the D2 alpha. */
}


Size
cluster_undo_buf_shmem_size(void)
{
	int n = cluster_undo_buffers;
	Size sz;

	if (n <= 0)
		return 0; /* pool disabled */

	sz = MAXALIGN(sizeof(ClusterUndoBufPool));
	sz = add_size(sz, mul_size(sizeof(UndoBufSlot), n));
	sz = add_size(sz, mul_size((Size)BLCKSZ, n));
	return sz;
}


void
cluster_undo_buf_shmem_init(void)
{
	bool found;
	int n = cluster_undo_buffers;
	Size sz = cluster_undo_buf_shmem_size();

	if (n <= 0 || sz == 0) {
		UndoBufPool = NULL; /* disabled — callers fall back to direct smgr */
		return;
	}

	UndoBufPool = (ClusterUndoBufPool *)ShmemInitStruct("pgrac undo buffer pool", sz, &found);

	UndoBufSlots = (UndoBufSlot *)(((char *)UndoBufPool) + MAXALIGN(sizeof(ClusterUndoBufPool)));
	UndoBufData = ((char *)UndoBufSlots) + mul_size(sizeof(UndoBufSlot), n);

	if (found)
		return; /* EXEC_BACKEND second attach — already init'd */

	UndoBufPool->nslots = n;
	UndoBufPool->clock_hand = 0;
	LWLockInitialize(&UndoBufPool->map_lock, LWTRANCHE_CLUSTER_UNDO_BUF);
	pg_atomic_init_u64(&UndoBufPool->hit_count, 0);
	pg_atomic_init_u64(&UndoBufPool->miss_count, 0);
	pg_atomic_init_u64(&UndoBufPool->writethrough_count, 0);
	pg_atomic_init_u64(&UndoBufPool->evict_count, 0);

	for (int i = 0; i < n; i++) {
		UndoBufSlot *s = &UndoBufSlots[i];

		LWLockInitialize(&s->content_lock, LWTRANCHE_CLUSTER_UNDO_BUF);
		s->segment_id = 0;
		s->owner = 0;
		s->block_no = 0;
		s->valid = false;
		s->io_in_progress = false;
		s->dirty = false;
		pg_atomic_init_u32(&s->pincount, 0);
		s->clock_used = 0;
		s->last_wal_lsn = InvalidXLogRecPtr;
	}
}


static const ClusterShmemRegion cluster_undo_buf_region = {
	.name = "pgrac undo buffer pool",
	.size_fn = cluster_undo_buf_shmem_size,
	.init_fn = cluster_undo_buf_shmem_init,
	.lwlock_count = 0, /* tranche registered statically (lwlock.h) */
	.owner_subsys = "cluster_undo_buf",
	.reserved_flags = 0,
};

void
cluster_undo_buf_register_region(void)
{
	cluster_shmem_register_region(&cluster_undo_buf_region);
}


/* Match predicate (caller holds map_lock). */
static inline bool
slot_matches(const UndoBufSlot *s, uint32 seg, uint8 owner, uint32 blk)
{
	return s->valid && s->segment_id == seg && s->owner == owner && s->block_no == blk;
}


/*
 * Clock-sweep one unpinned victim under map_lock.  Returns slot index, or -1
 * if every slot is pinned (caller fail-closes).  Gives a second chance to
 * recently-used slots (clock_used).
 */
static int
choose_victim(void)
{
	int n = UndoBufPool->nslots;

	for (int scanned = 0; scanned < 2 * n; scanned++) {
		uint32 h = UndoBufPool->clock_hand;
		UndoBufSlot *s = &UndoBufSlots[h];

		UndoBufPool->clock_hand = (h + 1) % n;

		if (pg_atomic_read_u32(&s->pincount) != 0 || s->io_in_progress)
			continue;
		if (s->clock_used) {
			s->clock_used = 0; /* second chance */
			continue;
		}
		return (int)h;
	}
	return -1;
}


/*
 * Flush one buffered-dirty slot to disk (D2b write-back).  Caller MUST hold a
 * pin on slotno so it cannot be evicted across the I/O.  Snapshots the image +
 * its protecting LSN under the content lock and clears dirty, then OUTSIDE the
 * lock XLogFlush(lsn) (WAL-before-data) + write-through + fsync.  A no-op if the
 * slot is no longer dirty (a concurrent flush / evict won the race).
 */
static void
flush_dirty_slot(int slotno)
{
	UndoBufSlot *s = &UndoBufSlots[slotno];
	PGAlignedBlock localbuf;
	XLogRecPtr wal_lsn = InvalidXLogRecPtr;
	uint32 seg = 0;
	uint8 owner = 0;
	uint32 blk = 0;
	bool need_write = false;

	LWLockAcquire(&s->content_lock, LW_EXCLUSIVE);
	if (s->dirty) {
		memcpy(localbuf.data, SLOT_DATA(slotno), BLCKSZ);
		wal_lsn = s->last_wal_lsn;
		seg = s->segment_id;
		owner = s->owner;
		blk = s->block_no;
		s->dirty = false;
		need_write = true;
	}
	LWLockRelease(&s->content_lock);

	if (need_write) {
		XLogFlush(wal_lsn); /* WAL-before-data */
		if (!cluster_undo_smgr_write_block(seg, owner, blk, localbuf.data, /* do_fsync = */ true))
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_UNDO_RECORD_INVALID_UBA),
							errmsg("cluster undo buffer write-back flush failed "
								   "seg=%u owner=%u block=%u",
								   seg, owner, blk)));
	}
}


char *
cluster_undo_buf_pin(uint32 segment_id, uint8 owner, uint32 block_no, ClusterUndoBufMode mode,
					 ClusterUndoBufPin *pin)
{
	int slotno = -1;

	pin->slot = -1;

	/* block 0 (header + durable TT) is NOT poolable;  pool may be disabled. */
	if (block_no < CLUSTER_UNDO_BUF_FIRST_DATA_BLOCK || UndoBufPool == NULL)
		return NULL; /* caller uses the direct smgr path */

	for (;;) {
		LWLockAcquire(&UndoBufPool->map_lock, LW_EXCLUSIVE);

		/* Lookup. */
		for (int i = 0; i < UndoBufPool->nslots; i++) {
			if (slot_matches(&UndoBufSlots[i], segment_id, owner, block_no)) {
				slotno = i;
				break;
			}
		}

		if (slotno >= 0) {
			UndoBufSlot *s = &UndoBufSlots[slotno];

			if (s->io_in_progress) {
				/* Another backend is filling it — wait on its content_lock. */
				LWLockRelease(&UndoBufPool->map_lock);
				LWLockAcquire(&s->content_lock, LW_SHARED);
				LWLockRelease(&s->content_lock);
				slotno = -1;
				continue; /* re-lookup */
			}
			pg_atomic_fetch_add_u32(&s->pincount, 1);
			s->clock_used = 1;
			pg_atomic_fetch_add_u64(&UndoBufPool->hit_count, 1);
			LWLockRelease(&UndoBufPool->map_lock);
			break; /* hit — acquire content_lock below */
		}

		/* Miss — reserve a victim and fill it outside map_lock. */
		slotno = choose_victim();
		if (slotno < 0) {
			LWLockRelease(&UndoBufPool->map_lock);
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_UNDO_RECORD_INVALID_UBA),
							errmsg("cluster undo buffer pool exhausted (all %d slots pinned)",
								   UndoBufPool->nslots),
							errhint("increase cluster.undo_buffers")));
		}
		if (UndoBufSlots[slotno].dirty) {
			/*
			 * D2b write-back victim: its buffered changes must reach disk
			 * before the slot is reused (otherwise a later read would see the
			 * stale on-disk block).  Pin it, flush OUTSIDE map_lock, then
			 * re-loop and re-pick a victim (now clean).  Dormant while
			 * write-back is gated off -- a write-through victim is never dirty.
			 */
			UndoBufSlot *s = &UndoBufSlots[slotno];

			pg_atomic_fetch_add_u32(&s->pincount, 1);
			LWLockRelease(&UndoBufPool->map_lock);
			flush_dirty_slot(slotno);
			pg_atomic_fetch_sub_u32(&s->pincount, 1);
			slotno = -1;
			continue; /* re-lookup / re-pick a clean victim */
		}
		{
			UndoBufSlot *s = &UndoBufSlots[slotno];

			if (s->valid)
				pg_atomic_fetch_add_u64(&UndoBufPool->evict_count, 1);

			s->segment_id = segment_id;
			s->owner = owner;
			s->block_no = block_no;
			s->valid = true;
			s->io_in_progress = true;
			s->dirty = false;
			s->clock_used = 1;
			s->last_wal_lsn = InvalidXLogRecPtr;
			pg_atomic_write_u32(&s->pincount, 1);		   /* reserve across I/O */
			LWLockAcquire(&s->content_lock, LW_EXCLUSIVE); /* block waiters */
			LWLockRelease(&UndoBufPool->map_lock);

			/* Disk read OUTSIDE the map_lock. */
			if (!cluster_undo_smgr_read_block(segment_id, owner, block_no, SLOT_DATA(slotno))) {
				/* Fill failed — release reservation + report. */
				LWLockRelease(&s->content_lock);
				LWLockAcquire(&UndoBufPool->map_lock, LW_EXCLUSIVE);
				s->io_in_progress = false;
				s->valid = false;
				pg_atomic_write_u32(&s->pincount, 0);
				LWLockRelease(&UndoBufPool->map_lock);
				ereport(ERROR, (errcode(ERRCODE_CLUSTER_UNDO_RECORD_INVALID_UBA),
								errmsg("cluster undo buffer read failed seg=%u owner=%u block=%u",
									   segment_id, owner, block_no)));
			}
			pg_atomic_fetch_add_u64(&UndoBufPool->miss_count, 1);
			LWLockRelease(&s->content_lock); /* fill done */

			LWLockAcquire(&UndoBufPool->map_lock, LW_EXCLUSIVE);
			s->io_in_progress = false;
			LWLockRelease(&UndoBufPool->map_lock);
			break; /* pincount==1 held;  acquire mode below */
		}
	}

	/* Acquire the content lock in the caller's mode (fixed — no upgrade). */
	LWLockAcquire(&UndoBufSlots[slotno].content_lock,
				  mode == CLUSTER_UNDO_BUF_EXCLUSIVE ? LW_EXCLUSIVE : LW_SHARED);

	pin->slot = slotno;
	pin->segment_id = segment_id;
	pin->owner = owner;
	pin->block_no = block_no;
	pin->mode = mode;
	return SLOT_DATA(slotno);
}


void
cluster_undo_buf_mark_dirty(ClusterUndoBufPin *pin, XLogRecPtr wal_lsn)
{
	UndoBufSlot *s;

	Assert(pin->slot >= 0);
	Assert(pin->mode == CLUSTER_UNDO_BUF_EXCLUSIVE);
	s = &UndoBufSlots[pin->slot];

	if (!cluster_undo_buf_writeback_allowed()) {
		/*
		 * D1 write-through:  push the modified block to disk now (do_fsync =
		 * false — fsync stays at cluster_undo_xact_precommit_flush, NOT here).
		 * The block is NOT left buffered-dirty, so durability is identical to
		 * today's per-block direct write.
		 */
		if (!cluster_undo_smgr_write_block(s->segment_id, s->owner, s->block_no,
										   SLOT_DATA(pin->slot), /* do_fsync = */ false))
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_UNDO_RECORD_INVALID_UBA),
					 errmsg("cluster undo buffer write-through failed seg=%u owner=%u block=%u",
							s->segment_id, s->owner, s->block_no)));
		s->dirty = false;
		s->last_wal_lsn = wal_lsn; /* Invalid in D1 */
		pg_atomic_fetch_add_u64(&UndoBufPool->writethrough_count, 1);
		return;
	}

	/* D2 write-back path (alpha-gated):  defer the disk write, keep WAL LSN. */
	if (XLogRecPtrIsInvalid(wal_lsn) || wal_lsn < s->last_wal_lsn)
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_UNDO_RECORD_INVALID_UBA),
				 errmsg("cluster undo buffer write-back requires a monotone valid wal_lsn")));
	s->dirty = true;
	s->last_wal_lsn = wal_lsn;
}


void
cluster_undo_buf_unpin(ClusterUndoBufPin *pin)
{
	UndoBufSlot *s;

	if (pin->slot < 0)
		return; /* not pooled (direct path) */
	s = &UndoBufSlots[pin->slot];
	LWLockRelease(&s->content_lock);
	pg_atomic_fetch_sub_u32(&s->pincount, 1);
	pin->slot = -1;
}


void
cluster_undo_buf_flush_all(bool is_checkpoint)
{
	if (UndoBufPool == NULL)
		return;

	if (!cluster_undo_buf_writeback_allowed()) {
		/*
		 * D1 write-through:  nothing is buffered-dirty, so flush is a no-op.
		 * Assert the invariant in debug builds (the real guard is that
		 * mark_dirty never leaves dirty=true while write-back is gated off).
		 */
#ifdef USE_ASSERT_CHECKING
		for (int i = 0; i < UndoBufPool->nslots; i++)
			Assert(!UndoBufSlots[i].dirty);
#endif
		return;
	}

	/*
	 * D2b write-back: flush every buffered-dirty block.  Called from
	 * CheckPointGuts (is_checkpoint=true) in checkpoint phase 2, AFTER the
	 * DELAY_CHKPT_START barrier — so any in-flight undo write whose WAL is at
	 * or before this checkpoint's redo point is flushed here, which is what
	 * makes the DELAY_CHKPT_START guarantee real (spec-3.18 §2.6 v0.8).
	 *
	 * Per slot: pin it (un-evictable across the I/O), then flush_dirty_slot
	 * copies the image + LSN, clears dirty, XLogFlush(lsn) (WAL-before-data),
	 * write-through + fsync -- all but the snapshot OUTSIDE the lock.  A
	 * concurrent re-dirty after we clear sets dirty again -> next checkpoint
	 * flushes it.
	 */
	(void)is_checkpoint;
	for (int i = 0; i < UndoBufPool->nslots; i++) {
		UndoBufSlot *s = &UndoBufSlots[i];
		bool pinned = false;

		LWLockAcquire(&UndoBufPool->map_lock, LW_EXCLUSIVE);
		if (s->valid && s->dirty) {
			pg_atomic_fetch_add_u32(&s->pincount, 1);
			pinned = true;
		}
		LWLockRelease(&UndoBufPool->map_lock);
		if (!pinned)
			continue;

		flush_dirty_slot(i);
		pg_atomic_fetch_sub_u32(&s->pincount, 1);
	}
}


uint64
cluster_undo_buf_get_hit_count(void)
{
	return UndoBufPool ? pg_atomic_read_u64(&UndoBufPool->hit_count) : 0;
}

uint64
cluster_undo_buf_get_miss_count(void)
{
	return UndoBufPool ? pg_atomic_read_u64(&UndoBufPool->miss_count) : 0;
}

uint64
cluster_undo_buf_get_writethrough_count(void)
{
	return UndoBufPool ? pg_atomic_read_u64(&UndoBufPool->writethrough_count) : 0;
}

uint64
cluster_undo_buf_get_evict_count(void)
{
	return UndoBufPool ? pg_atomic_read_u64(&UndoBufPool->evict_count) : 0;
}
