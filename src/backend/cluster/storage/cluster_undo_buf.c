/*-------------------------------------------------------------------------
 *
 * cluster_undo_buf.c
 *	  pgrac spec-3.18 D1 — per-instance undo block buffer pool.
 *
 *	  AD-014 form restoration:  active undo DATA blocks live in this in-memory
 *	  shmem pool instead of being direct-written per record.  D1 shipped the
 *	  pool in a durability-NEUTRAL form;  D2b activates buffered write-back:
 *	    - read-through cache;  writes are write-through (do_fsync=false) when
 *	      write-back is off, or buffered-dirty when on;
 *	    - write-back (cluster_undo_buf_writeback_allowed()) requires the pool +
 *	      cluster.undo_buffer_writeback on + NO peers (hard single-node latch,
 *	      see that function).  Dirty blocks are WAL-protected
 *	      (XLOG_UNDO_BLOCK_WRITE) and made durable by the checkpoint write-back
 *	      flush (cluster_undo_buf_flush_all) + eviction flush;  with write-back
 *	      off, durability stays at cluster_undo_xact_precommit_flush;
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

#include "access/xlog.h"			/* XLogFlush — WAL-before-data on write-back (D2b) */
#include "cluster/cluster_conf.h"	/* cluster_conf_has_peers — single-node gate (D2b) */
#include "cluster/cluster_guc.h"	/* cluster_undo_buf_pin_fastpath (spec-3.26 D5) */
#include "cluster/cluster_inject.h" /* spec-4.8ab D1 boundary-guard injection points */
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/storage/cluster_undo_buf.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/errcodes.h"
#include "utils/wait_event.h" /* spec-3.18 D7: ClusterUndoBufFlush wait event */

/* GUCs (registered in cluster_guc.c). */
extern int cluster_undo_buffers;		   /* pool slot count;  0 = disabled */
extern bool cluster_undo_buffer_writeback; /* D2b: write-back on (single-node only) */

/*
 * spec-4.8ab D5 -- advisory boundary-check mode (two-layer model, §3.1).
 * Controls ONLY the advisory verdict accounting below;  the hard fail-closed
 * guards are unconditional.  Default ON.  Defined + registered in cluster_guc.c.
 */
extern int cluster_undo_writeback_boundary_check;

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
	pg_atomic_uint64 writeback_count; /* spec-3.18 D7: D2b write-back flushes */
	/*
	 * spec-4.8ab D7: checkpoint-writeback boundary observability.  Grown into
	 * this existing region (D0 finding-3) -- no new shmem region, no t/020
	 * region-count baseline ripple.
	 */
	pg_atomic_uint64 writeback_held_wal;	  /* D5: HOLD_WAL verdicts (WAL not durable) */
	pg_atomic_uint64 writeback_held_evidence; /* D5: HOLD_EVIDENCE verdicts (evict deferred) */
	pg_atomic_uint64 boundary_violations;	  /* D1/D5: fail-closed boundary-guard hits */
	pg_atomic_uint64 remote_evidence_holds;	  /* 4.8b D6: peered write-through evidence holds */
	/* UndoBufSlot slots[nslots] follows; char data[nslots * BLCKSZ] after. */
} ClusterUndoBufPool;

static ClusterUndoBufPool *UndoBufPool = NULL;
static UndoBufSlot *UndoBufSlots = NULL;
static char *UndoBufData = NULL;

#define SLOT_DATA(i) (UndoBufData + ((Size)(i)) * BLCKSZ)


/*
 * cluster_undo_buf_writeback_allowed -- is buffered write-back active?
 *	D2b: write-back runs when ALL of:
 *	  1. the pool exists (cluster.undo_buffers > 0).  REQUIRED -- dirty blocks
 *	     are made durable by the checkpoint write-back flush
 *	     (cluster_undo_buf_flush_all from CheckPointGuts) + eviction flush,
 *	     which is what makes the DELAY_CHKPT_START guarantee real;  with no
 *	     pool, fall back to always-FPI write-through (D2a).
 *	  2. cluster.undo_buffer_writeback is on.
 *	  3. the node has NO peers (cluster_conf_has_peers() == false).  HARD
 *	     SINGLE-NODE LATCH (spec-3.18 §3.3 v0.9):  write-back leaves committed
 *	     undo in this node's pool (data file lags), durable only via WAL +
 *	     local checkpoint flush.  A remote node reading it from shared storage
 *	     would see STALE / missing undo until a future Cache Fusion + multi-
 *	     node-recovery spec makes cross-node undo write-back-aware.  Until then
 *	     a peered topology MUST use the always-FPI write-through path (D2a),
 *	     where every commit's undo is fsync'd to shared storage.  Topology is
 *	     postmaster-static after cluster_conf_load(), so this never flips mid-
 *	     run.  A real runtime guard, not Assert (L214/L218) nor a default.
 *
 *	WAL protection (XLOG_UNDO_BLOCK_WRITE FPI/delta + redo) is unconditional,
 *	so toggling the GUC in a single-node topology is safe.
 */
bool
cluster_undo_buf_writeback_allowed(void)
{
	return UndoBufPool != NULL && cluster_undo_buffer_writeback && !cluster_conf_has_peers();
}


/*
 * cluster_undo_buf_writeback_decide -- spec-4.8ab D5 three-state boundary
 *	verdict (PURE: LSN + flags, no globals -- unit-testable).  This is the
 *	per-block decision predicate, distinct from the global write-back-mode latch
 *	(cluster_undo_buf_writeback_allowed).  See the header for the contract.
 *
 *	  1. WAL-before-data first:  if the block's protecting WAL is not durable
 *	     (block_lsn > flushed_lsn, or block_lsn invalid), it cannot be written
 *	     back yet -> HOLD_WAL.  Reuses the D1 pure predicate so the two stay in
 *	     lockstep.
 *	  2. Evidence window:  a NON-checkpoint eviction must not drop an undo image
 *	     still consumable by a reader / recovery -> HOLD_EVIDENCE.  A checkpoint
 *	     flush is "flush-and-keep" (it writes durable but does not evict), so the
 *	     evidence window does NOT block a checkpoint flush (is_checkpoint relaxes
 *	     this clause, §2.1).
 *	  3. Otherwise OK (write back, and evict when not a checkpoint).
 */
ClusterUndoWritebackVerdict
cluster_undo_buf_writeback_decide(XLogRecPtr block_lsn, XLogRecPtr flushed_lsn, bool evidence_live,
								  bool is_checkpoint)
{
	if (!cluster_undo_wal_before_data_holds(block_lsn, flushed_lsn))
		return CLUSTER_WB_HOLD_WAL;
	if (evidence_live && !is_checkpoint)
		return CLUSTER_WB_HOLD_EVIDENCE;
	return CLUSTER_WB_OK;
}


/*
 * cluster_undo_buf_account_verdict -- spec-4.8ab D5 advisory accounting (the
 *	GUC-controlled second layer, §3.1).  Computes the three-state verdict for a
 *	dirty slot at the eviction / flush decision and records it for observability.
 *	The caller has already decided to flush (a dirty victim is always flushed
 *	WAL-before-data before its slot is reused);  this only CLASSIFIES the
 *	write-back, it never gates correctness -- the hard fail-closed guards in
 *	flush_dirty_slot / flush_all run unconditionally.
 *
 *	The caller must have checked cluster_undo_writeback_boundary_check != OFF so
 *	the GetFlushRecPtr() durable-bound read is skipped on the off hot path.  A
 *	dirty slot always carries a valid monotone protecting LSN (mark_dirty
 *	enforces it);  under STRICT a dirty slot with an INVALID protecting LSN is a
 *	broken advisory invariant -> ERROR (aggressive CI/test exposure).  This is
 *	NOT the hard corruption path (that is cluster_undo_boundary_violation).
 */
static void
cluster_undo_buf_account_verdict(XLogRecPtr block_lsn, bool is_checkpoint)
{
	ClusterUndoWritebackVerdict verdict;

	verdict = cluster_undo_buf_writeback_decide(block_lsn, GetFlushRecPtr(NULL),
												/* evidence_live = */ true, is_checkpoint);

	switch (verdict) {
	case CLUSTER_WB_HOLD_WAL:
		pg_atomic_fetch_add_u64(&UndoBufPool->writeback_held_wal, 1);
		if (cluster_undo_writeback_boundary_check == CLUSTER_UNDO_WB_CHECK_STRICT
			&& XLogRecPtrIsInvalid(block_lsn))
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_UNDO_WRITEBACK_BOUNDARY_VIOLATION),
							errmsg("strict undo writeback boundary check: dirty undo slot has no "
								   "protecting WAL LSN")));
		break;
	case CLUSTER_WB_HOLD_EVIDENCE:
		pg_atomic_fetch_add_u64(&UndoBufPool->writeback_held_evidence, 1);
		break;
	case CLUSTER_WB_OK:
		break;
	}
}


/*
 * cluster_undo_wal_before_data_holds -- spec-4.8ab D1 WAL-before-data predicate.
 *
 *	True iff a dirty undo block protected by block_lsn is durable-safe to write
 *	back:  its XLOG_UNDO_BLOCK_WRITE is durable (block_lsn <= flushed_lsn).  An
 *	invalid block_lsn means the block carries no protecting WAL on disk, so
 *	writing it back is never safe -> false (fail-closed, 8.A).  Pure (no globals)
 *	so the durability comparison is unit-testable in isolation.
 */
bool
cluster_undo_wal_before_data_holds(XLogRecPtr block_lsn, XLogRecPtr flushed_lsn)
{
	if (XLogRecPtrIsInvalid(block_lsn))
		return false;
	return block_lsn <= flushed_lsn;
}


/*
 * cluster_undo_checkpoint_coverage_violation -- spec-4.8ab D1 checkpoint-
 *	coverage predicate.  True iff a dirty undo block left unflushed at checkpoint
 *	completion is a data-loss VIOLATION:  it is PRE-redo (block_lsn <
 *	checkpoint_redo), so recovery starts at redo and never replays its WAL ->
 *	lost.  A POST-redo dirty block (block_lsn >= redo) is replayed from WAL by
 *	recovery and is legal, NOT a violation.
 *
 *	DISCIPLINE (spec-4.8ab v0.3 amend):  the guard is NOT "block_lsn > redo".
 *	Writing it that way would falsely flag every legal post-redo dirty block.
 *	An invalid block_lsn has nothing durable to lose -> not a violation.
 */
bool
cluster_undo_checkpoint_coverage_violation(XLogRecPtr block_lsn, XLogRecPtr checkpoint_redo)
{
	if (XLogRecPtrIsInvalid(block_lsn))
		return false;
	return block_lsn < checkpoint_redo;
}


/*
 * cluster_undo_boundary_violation -- spec-4.8ab D1 fail-closed on a detected
 *	checkpoint-writeback boundary violation (8.A).  Never silent, never returns.
 *
 *	Inside a critical section a thrown ERROR would already promote to PANIC;  be
 *	explicit so the log records a boundary violation rather than a generic "ERROR
 *	inside critical section".  This is the HARD-corruption layer: it is NOT gated
 *	by cluster.undo_writeback_boundary_check (that GUC only controls the advisory
 *	layer -- see §3.1).  block_lsn is the offending undo block's protecting LSN;
 *	flushed_or_redo is the durable-flush bound (WAL-before-data) or the checkpoint
 *	redo point (checkpoint-coverage) it violated.
 */
void
cluster_undo_boundary_violation(const char *what, XLogRecPtr block_lsn, XLogRecPtr flushed_or_redo)
{
	int elevel = (CritSectionCount > 0) ? PANIC : ERROR;

	/* spec-4.8ab D7: record the fail-closed hit before raising (survives an
	 * ERROR-level stop for observability;  a PANIC restarts shmem anyway). */
	if (UndoBufPool != NULL)
		pg_atomic_fetch_add_u64(&UndoBufPool->boundary_violations, 1);

	ereport(elevel, (errcode(ERRCODE_CLUSTER_UNDO_WRITEBACK_BOUNDARY_VIOLATION),
					 errmsg("cluster undo checkpoint-writeback boundary violation: %s", what),
					 errdetail("undo block LSN %X/%X is past the durable/redo bound %X/%X",
							   LSN_FORMAT_ARGS(block_lsn), LSN_FORMAT_ARGS(flushed_or_redo))));
	pg_unreachable();
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
	pg_atomic_init_u64(&UndoBufPool->writeback_count, 0);	 /* spec-3.18 D7 */
	pg_atomic_init_u64(&UndoBufPool->writeback_held_wal, 0); /* spec-4.8ab D7 */
	pg_atomic_init_u64(&UndoBufPool->writeback_held_evidence, 0);
	pg_atomic_init_u64(&UndoBufPool->boundary_violations, 0);
	pg_atomic_init_u64(&UndoBufPool->remote_evidence_holds, 0);

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
 * pin on slotno so it cannot be evicted across the I/O.
 *
 *	Review finding 1 (8.A):  dirty is cleared ONLY AFTER the write succeeds, and
 *	only if no newer write landed meanwhile.  Clearing dirty before the write
 *	(the original code) would, on a write ERROR, leave the slot
 *	new-in-memory / clean / stale-on-disk -- a later clean eviction or disk
 *	re-read would then return stale undo.  So:  snapshot the image + its
 *	protecting LSN under the content lock (dirty stays set), XLogFlush
 *	(WAL-before-data) + write-through + fsync OUTSIDE the lock, then re-acquire
 *	and clear dirty only when last_wal_lsn is still the snapshotted one.  A
 *	concurrent re-dirty (newer LSN -- the owning backend racing the
 *	checkpointer) keeps dirty set so its image is flushed by a later
 *	checkpoint / eviction.
 */
static void
flush_dirty_slot(int slotno)
{
	UndoBufSlot *s = &UndoBufSlots[slotno];
	PGAlignedBlock localbuf;
	XLogRecPtr flushed_lsn = InvalidXLogRecPtr;
	XLogRecPtr durable_lsn;
	uint32 seg = 0;
	uint8 owner = 0;
	uint32 blk = 0;
	bool need_write = false;

	/*
	 * spec-4.8ab D3:  the checkpoint-writeback boundary (D1) covers undo DATA
	 * blocks only.  Block 0 (segment header + durable TT slots) is NOT poolable
	 * (cluster_undo_buf_pin rejects block_no < FIRST_DATA_BLOCK), so it never
	 * reaches this flush path -- its durability is a SEPARATE contract:  the TT
	 * slot WAL (XLOG_UNDO_TT_SLOT_* / the folded commit-record delta) is emitted
	 * before the slot's byte-targeted write, which is itself NOT fsync'd;  block
	 * 0 becomes durable only via WAL redo re-stamp (cluster_tt_durable.c).  This
	 * Assert pins that the pool only ever flushes data blocks.
	 */
	Assert(s->block_no >= CLUSTER_UNDO_BUF_FIRST_DATA_BLOCK);

	LWLockAcquire(&s->content_lock, LW_EXCLUSIVE);
	if (s->dirty) {
		memcpy(localbuf.data, SLOT_DATA(slotno), BLCKSZ);
		flushed_lsn = s->last_wal_lsn;
		seg = s->segment_id;
		owner = s->owner;
		blk = s->block_no;
		need_write = true; /* NB: dirty NOT cleared here */
	}
	LWLockRelease(&s->content_lock);
	if (!need_write)
		return;

	XLogFlush(flushed_lsn); /* WAL-before-data (XLogFlush carries its own wait event) */

	/*
	 * spec-4.8ab D1:  explicit WAL-before-data boundary guard (8.A, a real
	 * runtime check -- NOT Assert, L214).  After XLogFlush the block's protecting
	 * LSN MUST be durable;  if GetFlushRecPtr() has not reached flushed_lsn the
	 * durability ordering was broken (a missing / wrong XLogFlush), and writing
	 * the block now would put an undo image on disk whose WAL is not durable ->
	 * torn / lost on crash.  Fail-closed (53R9N) rather than write it.  This is
	 * the hard-corruption layer (unconditional, not GUC-gated -- §3.1).
	 */
	durable_lsn = GetFlushRecPtr(NULL);

	/*
	 * spec-4.8ab D1 L3a injection: deterministically force a WAL-before-data
	 * violation (pretend the durable bound is one byte below the block's
	 * protecting LSN) so the guard below fail-closes.  One-shot.  The
	 * INJECTION_POINT macro dispatches the armed SKIP (sets the pending flag);
	 * should_skip then consumes it.
	 */
	CLUSTER_INJECTION_POINT("undo-force-wal-before-data-violation");
	if (!XLogRecPtrIsInvalid(flushed_lsn)
		&& cluster_injection_should_skip("undo-force-wal-before-data-violation"))
		durable_lsn = flushed_lsn - 1;

	if (!cluster_undo_wal_before_data_holds(flushed_lsn, durable_lsn))
		cluster_undo_boundary_violation("write-back flush", flushed_lsn, durable_lsn);

	/*
	 * PGRAC (spec-3.18 D7): attribute the write-back data-file I/O (pwrite +
	 * fsync) to a dedicated wait event so a backend blocked here is visible in
	 * pg_stat_activity.wait_event.  Only the smgr write is wrapped -- the
	 * XLogFlush above is attributed to its own WAL wait event.
	 */
	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_UNDO_BUF_FLUSH);
	if (!cluster_undo_smgr_write_block(seg, owner, blk, localbuf.data, /* do_fsync = */ true)) {
		pgstat_report_wait_end();
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_UNDO_RECORD_INVALID_UBA),
						errmsg("cluster undo buffer write-back flush failed "
							   "seg=%u owner=%u block=%u",
							   seg, owner, blk)));
	}
	pgstat_report_wait_end();
	pg_atomic_fetch_add_u64(&UndoBufPool->writeback_count, 1); /* spec-3.18 D7 */

	/* Write durable -> clear dirty unless a newer write re-dirtied the slot. */
	LWLockAcquire(&s->content_lock, LW_EXCLUSIVE);
	if (s->dirty && s->last_wal_lsn == flushed_lsn)
		s->dirty = false;
	LWLockRelease(&s->content_lock);
}


/*
 * cluster_undo_buf_invalidate_segment -- spec-3.18 D3.2 (review finding 3).
 *
 *	Drop every pool slot belonging to a segment that is being reborn in place
 *	(cluster_undo_segment_reuse_in_place, generation+1).  The pool keys blocks by
 *	(segment_id, owner, block_no) WITHOUT a generation, so an old-generation
 *	cached block would otherwise keep hitting the same key after reuse and serve
 *	stale undo.  A reused segment is RECYCLABLE (retention-cleared -- no live
 *	reader/writer may dereference it) and any dirty block is WAL-protected, so we
 *	DROP without flushing:  on crash, redo replays the old XLOG_UNDO_BLOCK_WRITE
 *	then the reuse + new writes in LSN order (last-writer-wins).  Caller holds
 *	lifecycle_lock (the reuse path).
 */
void
cluster_undo_buf_invalidate_segment(uint32 segment_id, uint8 owner)
{
	if (UndoBufPool == NULL)
		return;

	LWLockAcquire(&UndoBufPool->map_lock, LW_EXCLUSIVE);
	for (int i = 0; i < UndoBufPool->nslots; i++) {
		UndoBufSlot *s = &UndoBufSlots[i];

		if (s->valid && s->segment_id == segment_id && s->owner == owner) {
			/*
			 * A recyclable segment must have no pinned blocks (no live
			 * reader/writer).  A real runtime guard, not Assert (stripped in
			 * release -- L214/L218):  a pinned slot here means a retention
			 * invariant breach (someone is using a recyclable segment), which
			 * would already be corrupting in-flight I/O -- fail-closed PANIC
			 * rather than silently invalidate a slot under an active pin.
			 */
			if (pg_atomic_read_u32(&s->pincount) != 0)
				ereport(PANIC,
						(errmsg("cluster undo buffer: pinned slot in segment being "
								"reused seg=%u owner=%u slot=%d pincount=%u",
								segment_id, (unsigned)owner, i, pg_atomic_read_u32(&s->pincount))));
			s->valid = false;
			s->dirty = false;
			s->io_in_progress = false;
			s->last_wal_lsn = InvalidXLogRecPtr;
		}
	}
	LWLockRelease(&UndoBufPool->map_lock);
}


/*
 * spec-3.26 D5 (Lever B): backend-local last-pin cache.  No shmem, no lock --
 * a per-backend hot-path hint.  A repeat pin of the same undo block (the common
 * per-DML pattern) revalidates this one cached slot under a SHARED map_lock
 * instead of paying the O(nslots) EXCLUSIVE linear scan.  A stale cache simply
 * fails revalidation and falls back to the authoritative path (never a
 * wrong-slot pin -- r1-P1-3 / r2-P1-2).
 */
typedef struct UndoBufLastPin {
	uint32 seg;
	uint8 owner;
	uint32 blk;
	int slotno;
} UndoBufLastPin;
static UndoBufLastPin undo_last_pin = { .slotno = -1 };

char *
cluster_undo_buf_pin(uint32 segment_id, uint8 owner, uint32 block_no, ClusterUndoBufMode mode,
					 ClusterUndoBufPin *pin)
{
	int slotno = -1;

	pin->slot = -1;

	/* block 0 (header + durable TT) is NOT poolable;  pool may be disabled. */
	if (block_no < CLUSTER_UNDO_BUF_FIRST_DATA_BLOCK || UndoBufPool == NULL)
		return NULL; /* caller uses the direct smgr path */

	/*
	 * spec-3.26 D5 fast path: if the cached slot still maps this (seg, owner,
	 * block) and is not mid-fill, pin it under a SHARED map_lock (which excludes
	 * the EXCLUSIVE evictor, so the slot cannot be reused under us).  Any
	 * mismatch / io_in_progress / disabled GUC -> fall through to the
	 * authoritative EXCLUSIVE scan below.  Same return semantics either way.
	 */
	if (cluster_undo_buf_pin_fastpath && undo_last_pin.slotno >= 0
		&& undo_last_pin.seg == segment_id && undo_last_pin.owner == owner
		&& undo_last_pin.blk == block_no) {
		int cs = undo_last_pin.slotno;

		LWLockAcquire(&UndoBufPool->map_lock, LW_SHARED);
		if (cs < UndoBufPool->nslots && slot_matches(&UndoBufSlots[cs], segment_id, owner, block_no)
			&& !UndoBufSlots[cs].io_in_progress) {
			pg_atomic_fetch_add_u32(&UndoBufSlots[cs].pincount, 1);
			UndoBufSlots[cs].clock_used = 1; /* same-value hint; SHARED excludes evictor */
			pg_atomic_fetch_add_u64(&UndoBufPool->hit_count, 1);
			LWLockRelease(&UndoBufPool->map_lock);
			slotno = cs;
		} else
			LWLockRelease(&UndoBufPool->map_lock);
	}

	while (slotno < 0) {
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
			XLogRecPtr victim_lsn = s->last_wal_lsn;

			pg_atomic_fetch_add_u32(&s->pincount, 1);
			LWLockRelease(&UndoBufPool->map_lock);

			/*
			 * spec-4.8ab D5: classify this dirty-victim write-back for
			 * observability (advisory layer -- skipped on the off hot path so the
			 * GetFlushRecPtr() durable-bound read costs nothing then).  A dirty
			 * victim still carries an undo image not yet on disk, so its evidence
			 * is live and a non-checkpoint eviction holds it until the flush below
			 * makes it durable;  the verdict records HOLD_WAL / HOLD_EVIDENCE.
			 * Done after the pin + map_lock release (the pin keeps the slot
			 * stable, and we never hold map_lock across GetFlushRecPtr).  The hard
			 * WAL-before-data guard is inside flush_dirty_slot, unconditional.
			 */
			if (cluster_undo_writeback_boundary_check != CLUSTER_UNDO_WB_CHECK_OFF)
				cluster_undo_buf_account_verdict(victim_lsn, /* is_checkpoint = */ false);

			PG_TRY();
			{
				flush_dirty_slot(slotno);
			}
			PG_FINALLY();
			{
				pg_atomic_fetch_sub_u32(&s->pincount, 1);
			}
			PG_END_TRY();
			slotno = -1;
			continue; /* re-lookup / re-pick a clean victim */
		}
		{
			UndoBufSlot *s = &UndoBufSlots[slotno];

			if (s->valid) {
				/*
				 * spec-4.8ab D2 evidence-preservation guard (8.A).  A valid slot
				 * is about to be repurposed for a different block;  its own undo
				 * image MUST already be durable.  The dirty-victim path above
				 * flushes (WAL-before-data) and re-loops, so a slot reaching here
				 * is clean -- if it were still dirty we would drop undo evidence
				 * (an ITL slot's companion image / a CR pre-version) that is not
				 * yet on disk, and a later reader / recovery would read a stale
				 * block -> false-visible.  Fail-closed rather than evict it.  A
				 * real runtime guard (L214), defensive over the structural
				 * flush-re-loop -- mirrors cluster_undo_buf_invalidate_segment's
				 * "must not drop a live slot" PANIC.
				 */
				if (s->dirty) {
					LWLockRelease(&UndoBufPool->map_lock);
					cluster_undo_boundary_violation("evidence eviction", s->last_wal_lsn,
													InvalidXLogRecPtr);
				}
				pg_atomic_fetch_add_u64(&UndoBufPool->evict_count, 1);
			}

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

	/* spec-3.26 D5: remember this slot so the next pin of the same block can
	 * take the SHARED fast path above.  Pure hint -- always revalidated. */
	undo_last_pin.seg = segment_id;
	undo_last_pin.owner = owner;
	undo_last_pin.blk = block_no;
	undo_last_pin.slotno = slotno;

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
cluster_undo_buf_mark_dirty(const ClusterUndoBufPin *pin, XLogRecPtr wal_lsn)
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

		/*
		 * spec-4.8ab D6 (4.8b contract-first) -- remote-undo evidence invariant.
		 * Under a PEERED topology write-back is gated OFF (writeback_allowed()
		 * returns false because cluster_conf_has_peers()), so every undo write is
		 * write-through to shared storage and the block's undo evidence is durable
		 * the instant it is written.  That is the conservative hold: the source
		 * node NEVER async-releases an undo image a remote node might still need
		 * (Q4 朝 hold).  We count these holds so the contract is observable;  the
		 * real cross-node remote-undo READ stays fail-closed
		 * (ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED in cluster_cr.c) until
		 * spec-5.57 ships it.  Distinguish the peered hold from a GUC-off /
		 * pool-disabled write-through (which is local, not a remote hold).
		 */
		if (cluster_conf_has_peers())
			pg_atomic_fetch_add_u64(&UndoBufPool->remote_evidence_holds, 1);
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

/*
 * spec-3.25 D1b: lock-free window reference.
 *
 *	The deferred-merge window must keep its block's pool slot resident (a
 *	reader re-filling an evicted slot from disk would miss the pending,
 *	not-yet-WAL'd records), WITHOUT holding the content lock across the
 *	window (that would block every reader).  pincount alone gates eviction
 *	(choose_victim skips pincount != 0), so an extra reference taken while
 *	the caller still HOLDS an EXCLUSIVE pin (pincount >= 1 -- no eviction
 *	race) is sufficient.  Released by slot index at the window flush.
 */
void
cluster_undo_buf_addref(const ClusterUndoBufPin *pin)
{
	Assert(pin != NULL && pin->slot >= 0);
	pg_atomic_fetch_add_u32(&UndoBufSlots[pin->slot].pincount, 1);
}

void
cluster_undo_buf_unref_slot(int slot)
{
	Assert(slot >= 0 && UndoBufSlots != NULL);
	pg_atomic_fetch_sub_u32(&UndoBufSlots[slot].pincount, 1);
}


void
cluster_undo_buf_flush_all(bool is_checkpoint)
{
	if (UndoBufPool == NULL)
		return;

	/*
	 * Flush every buffered-dirty block.  Called from CheckPointGuts
	 * (is_checkpoint=true) in checkpoint phase 2, AFTER the DELAY_CHKPT_START
	 * barrier — so any in-flight undo write whose WAL is at or before this
	 * checkpoint's redo point is flushed here, which is what makes the
	 * DELAY_CHKPT_START guarantee real (spec-3.18 §2.6 v0.8).
	 *
	 * Always scan for dirty slots regardless of the current write-back gate:
	 * with write-back off no slot is ever left dirty (mark_dirty write-throughs
	 * with dirty=false), so the scan is a cheap no-op;  but if the GUC was
	 * toggled on->off via SIGHUP while dirty slots existed, those slots still
	 * need flushing for durability -- the gate decides whether NEW writes
	 * buffer, not whether already-dirty blocks get persisted.
	 *
	 * Per slot: pin it (un-evictable across the I/O), then flush_dirty_slot
	 * copies the image + LSN, clears dirty, XLogFlush(lsn) (WAL-before-data),
	 * write-through + fsync -- all but the snapshot OUTSIDE the lock.  A
	 * concurrent re-dirty after we clear sets dirty again -> next checkpoint
	 * flushes it.
	 */
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

		/*
		 * spec-4.8ab D1 L3b injection: deterministically skip flushing ONE
		 * dirty block (leave it buffered-dirty) so the checkpoint-coverage
		 * re-scan below detects a PRE-redo block that escaped the flush and
		 * fail-closes.  One-shot.  The INJECTION_POINT macro dispatches the
		 * armed SKIP (sets the pending flag);  should_skip then consumes it.
		 */
		CLUSTER_INJECTION_POINT("undo-skip-checkpoint-flush-one");
		if (cluster_injection_should_skip("undo-skip-checkpoint-flush-one")) {
			pg_atomic_fetch_sub_u32(&s->pincount, 1);
			continue;
		}

		PG_TRY();
		{
			flush_dirty_slot(i);
		}
		PG_FINALLY();
		{
			pg_atomic_fetch_sub_u32(&s->pincount, 1);
		}
		PG_END_TRY();
	}

	/*
	 * spec-4.8ab D1 checkpoint-coverage boundary guard (8.A).  At checkpoint
	 * completion NO pre-redo dirty undo block may remain:  such a block escaped
	 * the flush (a DELAY_CHKPT_START regression / injected skip) and would be
	 * lost on recovery (redo starts at the redo point and never replays its
	 * WAL).  A POST-redo dirty block is legal (a backend writing undo
	 * concurrently;  redo replays it) and is NOT flagged -- the guard is
	 * cluster_undo_checkpoint_coverage_violation, NOT block_lsn > redo (v0.3
	 * amend).  Read last_wal_lsn under the content lock so the 64-bit LSN is
	 * never torn (a torn low value could false-fire).  Once per checkpoint --
	 * not a hot path.
	 */
	if (is_checkpoint) {
		XLogRecPtr redo = GetRedoRecPtr();

		for (int i = 0; i < UndoBufPool->nslots; i++) {
			UndoBufSlot *s = &UndoBufSlots[i];
			XLogRecPtr blk_lsn = InvalidXLogRecPtr;
			bool dirty = false;

			LWLockAcquire(&s->content_lock, LW_SHARED);
			if (s->valid && s->dirty) {
				dirty = true;
				blk_lsn = s->last_wal_lsn;
			}
			LWLockRelease(&s->content_lock);

			if (dirty && cluster_undo_checkpoint_coverage_violation(blk_lsn, redo))
				cluster_undo_boundary_violation("checkpoint coverage", blk_lsn, redo);
		}
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

uint64
cluster_undo_buf_get_writeback_count(void)
{
	return UndoBufPool ? pg_atomic_read_u64(&UndoBufPool->writeback_count) : 0;
}

/* ----- spec-4.8ab D7: checkpoint-writeback boundary counters ----- */
uint64
cluster_undo_buf_get_writeback_held_wal_count(void)
{
	return UndoBufPool ? pg_atomic_read_u64(&UndoBufPool->writeback_held_wal) : 0;
}

uint64
cluster_undo_buf_get_writeback_held_evidence_count(void)
{
	return UndoBufPool ? pg_atomic_read_u64(&UndoBufPool->writeback_held_evidence) : 0;
}

uint64
cluster_undo_buf_get_boundary_violation_count(void)
{
	return UndoBufPool ? pg_atomic_read_u64(&UndoBufPool->boundary_violations) : 0;
}

uint64
cluster_undo_buf_get_remote_evidence_hold_count(void)
{
	return UndoBufPool ? pg_atomic_read_u64(&UndoBufPool->remote_evidence_holds) : 0;
}
