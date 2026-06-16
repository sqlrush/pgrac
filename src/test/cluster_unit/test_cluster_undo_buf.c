/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_buf.c
 *	  pgrac spec-3.18 D1 — undo buffer pool unit tests.
 *
 *	  Links the real cluster_undo_buf.o and stubs the PG primitives it
 *	  references (ShmemInitStruct -> malloc, LWLock* -> no-op single-threaded,
 *	  cluster_undo_smgr_{read,write}_block -> capture/replay).  Verifies the
 *	  pool LOGIC (interface-lock v1.1):
 *	    U1  writeback_allowed() == false (D1 alpha gate)
 *	    U2  block 0 is NOT poolable (pin returns NULL -> caller direct path)
 *	    U3  miss reads via smgr once;  same block then hits (no 2nd read)
 *	    U4  mark_dirty write-throughs with do_fsync=false (NOT per-unpin fsync)
 *	    U5  data round-trips (modify under EXCLUSIVE pin -> write-through bytes)
 *	    U6  eviction reuses a slot when the pool is full of unpinned blocks
 *
 *	  Concurrency + the fail-closed pool-exhausted ERROR path are covered by
 *	  the integration e2e (the undo write path running on the pool), not here.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_undo_buf.c
 *
 * NOTES
 *	  pgrac-original file.
 *	  Spec: spec-3.18-write-path-performance-overhaul.md (v0.6 RE-SCOPE)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stdlib.h>
#include <string.h>

#include "cluster/cluster_conf.h" /* ClusterConf type for the single-node latch stub */
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/storage/cluster_undo_buf.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"


UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* ----- GUC globals the pool reads ----- */
int cluster_undo_buffers = 4; /* small pool for evict testing */
bool cluster_undo_buffer_writeback = false;

/*
 * spec-3.18 D7 -- flush_dirty_slot() calls pgstat_report_wait_start/_end for
 * WAIT_EVENT_CLUSTER_UNDO_BUF_FLUSH.  The inline helpers reference
 * my_wait_event_info (pgstat backend global); provide a file-static fake so the
 * standalone link resolves cleanly (mirrors test_cluster_ges.c).
 */
static uint32 ut_wait_event_info_storage = 0;
uint32 *my_wait_event_info = &ut_wait_event_info_storage;

/* ----- single-node latch: cluster_conf_has_peers() reads ClusterConfShmem;
 * NULL => no peers => latch does not block write-back (U1-U6 are single-node). */
ClusterConf *ClusterConfShmem = NULL;

/* ----- shmem stub:  one malloc'd region, "found" on re-init ----- */
static void *shmem_buf = NULL;
static Size shmem_sz = 0;

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	if (shmem_buf == NULL) {
		shmem_buf = malloc(size);
		shmem_sz = size;
		memset(shmem_buf, 0, size);
		*foundPtr = false;
	} else
		*foundPtr = true;
	return shmem_buf;
}

/* ----- LWLock stubs (single-threaded test) ----- */
void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}
bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}
void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

/* spec-4.8ab D1 -- flush_dirty_slot / flush_all reference the injection skip
 * API (CLUSTER_INJECTION_POINT macro + should_skip) for the L3a/L3b boundary
 * tests;  U1-U9 never arm a point.  armed_count = 0 keeps the macro on its fast
 * path (run is never called), and should_skip always reports "not armed". */
int cluster_injection_armed_count = 0;
void
cluster_injection_run(const char *name pg_attribute_unused())
{}
bool
cluster_injection_should_skip(const char *name pg_attribute_unused())
{
	return false;
}

/* ----- Size arithmetic (real ones overflow-check; stubs suffice here) ----- */
Size
add_size(Size s1, Size s2)
{
	return s1 + s2;
}
Size
mul_size(Size s1, Size s2)
{
	return s1 * s2;
}

/* ----- PG_TRY/PG_FINALLY machinery (D2b hardening wraps flush_dirty_slot in
 * PG_TRY in the evict + flush_all paths; the U1-U6 tests never throw, so these
 * only resolve the standalone link). ----- */
sigjmp_buf *PG_exception_stack = NULL;
struct ErrorContextCallback *error_context_stack = NULL;
void
pg_re_throw(void)
{
	abort();
}

/* ----- elog/ereport stubs (the U1-U6 paths never raise; symbols only) ----- */
bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return true;
}
bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return true;
}
void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	abort(); /* no U1-U6 test reaches an error */
}
int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}
int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/*
 * spec-4.8ab D1 -- cluster_undo_boundary_violation() reads CritSectionCount to
 * choose PANIC vs ERROR.  The U1-U9 tests never reach the fail-closed helper
 * (write-back gated off / pure predicates), so a file-static fake resolves the
 * link.  The boundary helper's fail-closed behaviour is covered by TAP injection.
 */
volatile uint32 CritSectionCount = 0;

/* ----- smgr stubs:  capture writes, replay reads with a per-block marker ----- */
static int smgr_read_calls = 0;
static int smgr_write_calls = 0;
static int smgr_fsync_write_calls = 0; /* spec-4.8ab D2: write-back flush (do_fsync) */
static bool smgr_last_write_fsync = true;
static uint32 smgr_last_write_block = 0;
static char smgr_last_write_byte0 = 0;

bool
cluster_undo_smgr_read_block(uint32 segment_id pg_attribute_unused(),
							 uint8 owner_instance pg_attribute_unused(), uint32 block_no, char *buf)
{
	smgr_read_calls++;
	memset(buf, 0, BLCKSZ);
	buf[0] = (char)block_no; /* marker so we can tell which block was read */
	return true;
}

bool
cluster_undo_smgr_write_block(uint32 segment_id pg_attribute_unused(),
							  uint8 owner_instance pg_attribute_unused(), uint32 block_no,
							  const char *buf, bool do_fsync)
{
	smgr_write_calls++;
	if (do_fsync)
		smgr_fsync_write_calls++;
	smgr_last_write_fsync = do_fsync;
	smgr_last_write_block = block_no;
	smgr_last_write_byte0 = buf[0];
	return true;
}

/* XLogFlush stub: the write-back flush path (flush_dirty_slot) references it,
 * but U1-U6 run with write-back gated off so it is never executed. */
void
XLogFlush(XLogRecPtr record pg_attribute_unused())
{}

/* GetFlushRecPtr stub: flush_dirty_slot's WAL-before-data guard (spec-4.8ab D1)
 * compares the block's protecting LSN against this durable bound.  The D2
 * dirty-eviction test marks blocks with small LSNs, so return a high bound to
 * model "WAL already durable" -- the guard passes and the flush proceeds. */
XLogRecPtr
GetFlushRecPtr(TimeLineID *insertTLI)
{
	if (insertTLI)
		*insertTLI = 0;
	return (XLogRecPtr)UINT64CONST(0x7FFFFFFFFFFFFFFF);
}

/* GetRedoRecPtr stub: spec-4.8ab D1 flush_all's checkpoint-coverage re-scan
 * reads it, but U1-U9 never call flush_all so the value is unused. */
XLogRecPtr
GetRedoRecPtr(void)
{
	return InvalidXLogRecPtr;
}

/* Re-init the pool fresh for a test (forces a new malloc'd region). */
static void
fresh_pool(void)
{
	if (shmem_buf) {
		free(shmem_buf);
		shmem_buf = NULL;
	}
	smgr_read_calls = smgr_write_calls = smgr_fsync_write_calls = 0;
	smgr_last_write_fsync = true;
	cluster_undo_buf_shmem_init();
}


/* ===== U1 — writeback gate: off by default (GUC off) ===== */
UT_TEST(test_undo_buf_writeback_gated_off)
{
	fresh_pool();
	cluster_undo_buffer_writeback = false; /* default */
	ClusterConfShmem = NULL;			   /* single-node */
	UT_ASSERT_EQ((int)cluster_undo_buf_writeback_allowed(), 0);
}


/* ===== U7 — D2b gate truth table: GUC + pool + single-node latch ===== */
UT_TEST(test_undo_buf_writeback_gate_truth_table)
{
	ClusterConf conf_two_nodes;

	fresh_pool(); /* pool exists (cluster_undo_buffers = 4) */

	/* GUC on + pool + single-node (no ClusterConfShmem) -> write-back ON. */
	cluster_undo_buffer_writeback = true;
	ClusterConfShmem = NULL;
	UT_ASSERT_EQ((int)cluster_undo_buf_writeback_allowed(), 1);

	/* HARD SINGLE-NODE LATCH: GUC on + pool but peered -> write-back OFF. */
	memset(&conf_two_nodes, 0, sizeof(conf_two_nodes));
	conf_two_nodes.node_count = 2;
	ClusterConfShmem = &conf_two_nodes;
	UT_ASSERT_EQ((int)cluster_undo_buf_writeback_allowed(), 0);

	/* node_count == 1 is still single-node -> write-back ON. */
	conf_two_nodes.node_count = 1;
	UT_ASSERT_EQ((int)cluster_undo_buf_writeback_allowed(), 1);

	/* GUC off -> OFF regardless of topology. */
	cluster_undo_buffer_writeback = false;
	ClusterConfShmem = NULL;
	UT_ASSERT_EQ((int)cluster_undo_buf_writeback_allowed(), 0);

	cluster_undo_buffer_writeback = false; /* restore default for later tests */
}


/* ===== U2 — block 0 is not poolable ===== */
UT_TEST(test_undo_buf_block0_not_poolable)
{
	ClusterUndoBufPin pin;
	char *img;

	fresh_pool();
	img = cluster_undo_buf_pin(1, 0, 0, CLUSTER_UNDO_BUF_SHARED, &pin);
	UT_ASSERT_NULL(img); /* caller falls back to direct smgr */
	UT_ASSERT_EQ(pin.slot, -1);
	UT_ASSERT_EQ(smgr_read_calls, 0); /* block 0 never touches the pool */
}


/* ===== U3 — miss reads once, then hits ===== */
UT_TEST(test_undo_buf_miss_then_hit)
{
	ClusterUndoBufPin pin;
	char *img;

	fresh_pool();

	img = cluster_undo_buf_pin(1, 0, 5, CLUSTER_UNDO_BUF_SHARED, &pin);
	UT_ASSERT_NOT_NULL(img);
	UT_ASSERT_EQ(smgr_read_calls, 1);			 /* disk read on miss */
	UT_ASSERT_EQ((int)(unsigned char)img[0], 5); /* read the right block */
	cluster_undo_buf_unpin(&pin);

	img = cluster_undo_buf_pin(1, 0, 5, CLUSTER_UNDO_BUF_SHARED, &pin);
	UT_ASSERT_NOT_NULL(img);
	UT_ASSERT_EQ(smgr_read_calls, 1); /* hit — no second read */
	UT_ASSERT_EQ((int)cluster_undo_buf_get_hit_count(), 1);
	UT_ASSERT_EQ((int)cluster_undo_buf_get_miss_count(), 1);
	cluster_undo_buf_unpin(&pin);
}


/* ===== U4/U5 — mark_dirty write-throughs with do_fsync=false + bytes ===== */
UT_TEST(test_undo_buf_write_through)
{
	ClusterUndoBufPin pin;
	char *img;

	fresh_pool();

	img = cluster_undo_buf_pin(1, 0, 7, CLUSTER_UNDO_BUF_EXCLUSIVE, &pin);
	UT_ASSERT_NOT_NULL(img);
	img[0] = (char)0x42; /* mutate under EXCLUSIVE pin */
	cluster_undo_buf_mark_dirty(&pin, InvalidXLogRecPtr);

	/* write-through happened, NOT a per-unpin fsync */
	UT_ASSERT_EQ(smgr_write_calls, 1);
	UT_ASSERT_EQ((int)smgr_last_write_fsync, 0); /* do_fsync == false */
	UT_ASSERT_EQ((int)smgr_last_write_block, 7);
	UT_ASSERT_EQ((int)(unsigned char)smgr_last_write_byte0, 0x42);
	UT_ASSERT_EQ((int)cluster_undo_buf_get_writethrough_count(), 1);

	cluster_undo_buf_unpin(&pin);
	/* unpin does NOT write again (write-through already flushed) */
	UT_ASSERT_EQ(smgr_write_calls, 1);
}


/* ===== U6 — eviction reuses a slot when the pool is full ===== */
UT_TEST(test_undo_buf_evicts_when_full)
{
	ClusterUndoBufPin pin;
	char *img;

	fresh_pool(); /* cluster_undo_buffers = 4 */

	/* Fill all 4 slots with distinct blocks, unpinning each (evictable). */
	for (uint32 b = 1; b <= 4; b++) {
		img = cluster_undo_buf_pin(1, 0, b, CLUSTER_UNDO_BUF_SHARED, &pin);
		UT_ASSERT_NOT_NULL(img);
		cluster_undo_buf_unpin(&pin);
	}
	UT_ASSERT_EQ(smgr_read_calls, 4);
	UT_ASSERT_EQ((int)cluster_undo_buf_get_evict_count(), 0);

	/* A 5th distinct block must evict one of the unpinned slots. */
	img = cluster_undo_buf_pin(1, 0, 99, CLUSTER_UNDO_BUF_SHARED, &pin);
	UT_ASSERT_NOT_NULL(img);
	UT_ASSERT_EQ((int)(unsigned char)img[0], 99);
	UT_ASSERT_EQ(smgr_read_calls, 5);
	UT_ASSERT_EQ((int)cluster_undo_buf_get_evict_count(), 1);
	cluster_undo_buf_unpin(&pin);
}


/* ===== U8 — invalidate_segment drops a segment's slots (reuse, finding 3) ===== */
UT_TEST(test_undo_buf_invalidate_segment)
{
	ClusterUndoBufPin pin;
	char *img;

	fresh_pool();

	/* Cache block 3 of segment 1 and block 3 of segment 2 (two misses). */
	img = cluster_undo_buf_pin(1, 0, 3, CLUSTER_UNDO_BUF_SHARED, &pin);
	UT_ASSERT_NOT_NULL(img);
	cluster_undo_buf_unpin(&pin);
	img = cluster_undo_buf_pin(2, 0, 3, CLUSTER_UNDO_BUF_SHARED, &pin);
	UT_ASSERT_NOT_NULL(img);
	cluster_undo_buf_unpin(&pin);
	UT_ASSERT_EQ(smgr_read_calls, 2);

	/* Both re-read as hits (no new disk reads). */
	img = cluster_undo_buf_pin(1, 0, 3, CLUSTER_UNDO_BUF_SHARED, &pin);
	cluster_undo_buf_unpin(&pin);
	img = cluster_undo_buf_pin(2, 0, 3, CLUSTER_UNDO_BUF_SHARED, &pin);
	cluster_undo_buf_unpin(&pin);
	UT_ASSERT_EQ(smgr_read_calls, 2);

	/* Invalidate segment 1 (reuse-in-place):  its slot is dropped, segment 2's survives. */
	cluster_undo_buf_invalidate_segment(1, 0);

	img = cluster_undo_buf_pin(2, 0, 3, CLUSTER_UNDO_BUF_SHARED, &pin);
	cluster_undo_buf_unpin(&pin);
	UT_ASSERT_EQ(smgr_read_calls, 2); /* segment 2 still cached -> hit */

	img = cluster_undo_buf_pin(1, 0, 3, CLUSTER_UNDO_BUF_SHARED, &pin);
	cluster_undo_buf_unpin(&pin);
	UT_ASSERT_EQ(smgr_read_calls, 3); /* segment 1 dropped -> miss (re-read from disk) */
}


/* ===== D1 (spec-4.8ab) — WAL-before-data pure predicate ===== */
UT_TEST(test_undo_boundary_wal_before_data)
{
	XLogRecPtr flushed = 5000;

	/* WAL strictly below the durable bound -> safe to write the block back. */
	UT_ASSERT_EQ((int)cluster_undo_wal_before_data_holds(4999, flushed), 1);
	/* WAL exactly at the durable bound -> safe (block's WAL is durable). */
	UT_ASSERT_EQ((int)cluster_undo_wal_before_data_holds(5000, flushed), 1);
	/* WAL beyond the durable bound -> NOT safe: writing it would put an
	 * undo block on disk whose protecting WAL is not durable (torn/lost). */
	UT_ASSERT_EQ((int)cluster_undo_wal_before_data_holds(5001, flushed), 0);
	/* Invalid block_lsn (no protecting WAL at all) -> fail-closed, NOT safe. */
	UT_ASSERT_EQ((int)cluster_undo_wal_before_data_holds(InvalidXLogRecPtr, flushed), 0);
}


/* ===== D1 (spec-4.8ab) — checkpoint-coverage pure predicate =====
 *
 *	Pins the v0.3-amend discipline: the guard is NOT "block_lsn > redo".  Only a
 *	PRE-redo dirty block left unflushed is a violation (recovery starts at redo
 *	and never replays it);  a POST-redo dirty block is replayed from WAL and is
 *	perfectly legal.
 */
UT_TEST(test_undo_boundary_checkpoint_coverage)
{
	XLogRecPtr redo = 5000;

	/* PRE-redo dirty block left unflushed -> VIOLATION (lost on recovery). */
	UT_ASSERT_EQ((int)cluster_undo_checkpoint_coverage_violation(4999, redo), 1);
	/* AT the redo boundary -> NOT a violation (recovery replays from redo). */
	UT_ASSERT_EQ((int)cluster_undo_checkpoint_coverage_violation(5000, redo), 0);
	/* POST-redo dirty block -> NOT a violation (replayed from WAL).  This is
	 * the exact case the guard must NOT flag (do not write block_lsn > redo). */
	UT_ASSERT_EQ((int)cluster_undo_checkpoint_coverage_violation(5001, redo), 0);
	/* Invalid block_lsn -> nothing durable to lose -> NOT a violation. */
	UT_ASSERT_EQ((int)cluster_undo_checkpoint_coverage_violation(InvalidXLogRecPtr, redo), 0);
}


/* ===== D2 (spec-4.8ab) — dirty undo evidence is durable before eviction =====
 *
 *	Locks in the evidence-preservation invariant: under write-back, a dirty undo
 *	block's image MUST be flushed (WAL-before-data + fsync) before its pool slot
 *	is reused for a different block.  Otherwise eviction would drop undo evidence
 *	(an ITL companion image / a CR pre-version) not yet on disk -> false-visible.
 */
UT_TEST(test_undo_buf_dirty_evict_flushes_before_reuse)
{
	ClusterUndoBufPin pin;
	char *img;

	fresh_pool();						  /* cluster_undo_buffers = 4 */
	cluster_undo_buffer_writeback = true; /* D2b write-back */
	ClusterConfShmem = NULL;			  /* single-node -> write-back active */

	/* Fill all 4 slots with DIRTY blocks (write-back buffers, no flush yet). */
	for (uint32 b = 1; b <= 4; b++) {
		img = cluster_undo_buf_pin(1, 0, b, CLUSTER_UNDO_BUF_EXCLUSIVE, &pin);
		UT_ASSERT_NOT_NULL(img);
		img[0] = (char)b;
		cluster_undo_buf_mark_dirty(&pin, (XLogRecPtr)(100 + b)); /* monotone valid */
		cluster_undo_buf_unpin(&pin);
	}
	UT_ASSERT_EQ((int)cluster_undo_buf_get_writethrough_count(),
				 0);						 /* buffered, not write-through */
	UT_ASSERT_EQ(smgr_fsync_write_calls, 0); /* nothing flushed yet */

	/* A 5th distinct block forces eviction of a DIRTY victim: it MUST be
	 * flushed + fsync'd before its slot is reused (the D2 invariant), and the
	 * D2 reuse guard must NOT fire (the slot is clean after the flush). */
	img = cluster_undo_buf_pin(1, 0, 99, CLUSTER_UNDO_BUF_EXCLUSIVE, &pin);
	UT_ASSERT_NOT_NULL(img);
	UT_ASSERT_EQ((int)(unsigned char)img[0], 99);
	/*
	 * At least one dirty victim was flushed before reuse (the clock sweep may
	 * flush several -- over-flush is safe).  The invariant: EVERY write-back was
	 * fsync'd (WAL-before-data + durable), so no dirty block's evidence was
	 * dropped on eviction, and the D2 reuse guard never fired.
	 */
	UT_ASSERT_EQ(cluster_undo_buf_get_writeback_count() >= 1 ? 1 : 0, 1);
	UT_ASSERT_EQ((int)cluster_undo_buf_get_writeback_count(), smgr_fsync_write_calls);
	UT_ASSERT_EQ(cluster_undo_buf_get_evict_count() >= 1 ? 1 : 0, 1);
	cluster_undo_buf_unpin(&pin);

	cluster_undo_buffer_writeback = false; /* restore default for later tests */
}


int
main(void)
{
	UT_RUN(test_undo_buf_writeback_gated_off);
	UT_RUN(test_undo_buf_writeback_gate_truth_table);
	UT_RUN(test_undo_buf_invalidate_segment);
	UT_RUN(test_undo_buf_block0_not_poolable);
	UT_RUN(test_undo_buf_miss_then_hit);
	UT_RUN(test_undo_buf_write_through);
	UT_RUN(test_undo_buf_evicts_when_full);
	UT_RUN(test_undo_boundary_wal_before_data);
	UT_RUN(test_undo_boundary_checkpoint_coverage);
	UT_RUN(test_undo_buf_dirty_evict_flushes_before_reuse);
	UT_DONE();
}
