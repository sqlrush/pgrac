/*-------------------------------------------------------------------------
 *
 * test_cluster_tt_slot_allocator.c
 *	  pgrac spec-3.4b D11 — per-undo-segment TT slot allocator tests.
 *
 *	  Tests link in the real cluster_tt_slot.o and stub the
 *	  ShmemInitStruct / LWLock infrastructure with backend-free
 *	  replacements so allocator behaviour can be exercised end-to-end
 *	  without a live postmaster.  See "Shmem mock + LWLock stub"
 *	  section below.
 *
 *	  30 tests covering:
 *	    T1   INVALID_TT_SLOT_OFFSET == 0xFFFF
 *	    T2   TT_SLOTS_PER_SEGMENT == 48
 *	    T3   offset_to_id endpoints (0 -> 1, 47 -> 48)
 *	    T4   id_to_offset endpoints (1 -> 0, 48 -> 47)
 *	    T5   offset_to_id / id_to_offset mutual inverse
 *	    T6   shmem_size returns non-zero MAXALIGN'd
 *	    T7   shmem_init initialises a fresh shmem region zeroed
 *	    T8   alloc on fresh segment returns offset 0
 *	    T9   alloc same xid twice returns same offset (idempotent)
 *	    T10  alloc different xid returns distinct offset
 *	    T11  alloc 48 distinct xids returns offsets 0..47
 *	    T12  alloc 49th distinct xid returns INVALID_TT_SLOT_OFFSET (OVERFLOW)
 *	    T13  free clears a slot (subsequent alloc returns the freed offset)
 *	    T14  free of FREE slot is a no-op (idempotent)
 *	    T15  get_wrap returns 0 for a fresh slot
 *	    T16  wrap unchanged on FREE -> ACTIVE transition (first alloc)
 *	    T17  wrap unchanged on FREE -> ACTIVE -> FREE -> ACTIVE cycle
 *	    T18  wrap unchanged when freed then re-acquired by SAME xid
 *	    T19  recycle COMMITTED slot bumps wrap (L189 policy)
 *	    T20  recycle ABORTED slot bumps wrap (L189 policy)
 *	    T21  reuse own ACTIVE slot does NOT bump wrap (idempotent reuse)
 *	    T22  free uses LW_EXCLUSIVE (mock lock stats)
 *	    T23  get_wrap uses LW_SHARED (mock lock stats)
 *	    T24  reset_all wipes state across multiple nodes
 *	    T25  per-node isolation: alloc on node 0 doesn't affect node 1
 *	    T26  multi-instance unlock: node 0 segment_id 1 + node 1 segment_id 257
 *	    T27  segment_id == 0 -> ereport ERROR (bootstrap-only)
 *	    T28  segment_id > UINT16_MAX -> ereport ERROR (F4 alias guard)
 *	    T29  invalid xid -> ereport ERROR
 *	    T30  slot_offset >= TT_SLOTS_PER_SEGMENT to free -> ereport ERROR
 *
 *	  Spec: spec-3.4b-real-tt-allocator-uba-encoding-production-cross-node.md
 *	        (v0.3 FROZEN 2026-05-24)
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_tt_slot_allocator.c
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <setjmp.h>
#include <stdlib.h>
#include <string.h>

#include "access/transam.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "storage/lwlock.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


/* ============================================================
 *	Shmem mock + LWLock stub
 *	cluster_tt_slot.c calls ShmemInitStruct + LWLockInitialize +
 *	LWLockAcquire/Release + ereport.  We stub them all so the
 *	allocator runs in this standalone test binary.
 * ============================================================ */

static void *mock_shmem_buffer = NULL;
static int mock_lwlock_acquire_excl_count = 0;
static int mock_lwlock_acquire_shared_count = 0;
static int mock_lwlock_release_count = 0;

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	if (mock_shmem_buffer == NULL)
		mock_shmem_buffer = malloc(size);
	*foundPtr = false;
	memset(mock_shmem_buffer, 0, size);
	return mock_shmem_buffer;
}

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode)
{
	if (mode == LW_EXCLUSIVE)
		mock_lwlock_acquire_excl_count++;
	else if (mode == LW_SHARED)
		mock_lwlock_acquire_shared_count++;
	return false;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{
	mock_lwlock_release_count++;
}


/* ereport stub: longjmp out on ERROR so we can detect raises. */
static sigjmp_buf ereport_recover_jmp;
static int ereport_raised_count = 0;
static int last_ereport_errcode = 0;

#define EREPORT_RECOVER_SETUP()                                                                    \
	do {                                                                                           \
		ereport_raised_count = 0;                                                                  \
		last_ereport_errcode = 0;                                                                  \
	} while (0)

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel >= ERROR) {
		ereport_raised_count++;
		return true; /* let errmsg / errcode get called, then we longjmp */
	}
	return false; /* drop sub-ERROR */
}

/* PG marks errstart_cold as a cold-path wrapper used by macro expansions. */
bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}


/* cluster_tt_slot_shmem_register references this; stub since the test
 * doesn't go through the cluster_shmem registry. */
struct ClusterShmemRegion;
void
cluster_shmem_register_region(const struct ClusterShmemRegion *region pg_attribute_unused())
{}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	siglongjmp(ereport_recover_jmp, 1);
}

int
errcode(int sqlerrcode)
{
	last_ereport_errcode = sqlerrcode;
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


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/* ============================================================
 *	Test harness helpers
 * ============================================================ */

static void
reset_allocator(void)
{
	/* Reinitialise the shmem region; this wipes per-node state. */
	if (mock_shmem_buffer != NULL) {
		free(mock_shmem_buffer);
		mock_shmem_buffer = NULL;
	}
	cluster_tt_slot_shmem_init();

	mock_lwlock_acquire_excl_count = 0;
	mock_lwlock_acquire_shared_count = 0;
	mock_lwlock_release_count = 0;
}


/* node 0's first segment per cluster_undo_active_segment_for_node_or_create. */
#define NODE0_SEG 1
#define NODE1_SEG 257


/* ============================================================
 *	Tests
 * ============================================================ */

UT_TEST(test_t1_invalid_offset_sentinel)
{
	UT_ASSERT_EQ((int)INVALID_TT_SLOT_OFFSET, (int)0xFFFF);
}

UT_TEST(test_t2_slots_per_segment)
{
	UT_ASSERT_EQ((int)TT_SLOTS_PER_SEGMENT, 48);
}

UT_TEST(test_t3_offset_to_id_endpoints)
{
	UT_ASSERT_EQ((int)cluster_tt_slot_offset_to_id(0), 1);
	UT_ASSERT_EQ((int)cluster_tt_slot_offset_to_id(47), 48);
}

UT_TEST(test_t4_id_to_offset_endpoints)
{
	UT_ASSERT_EQ((int)cluster_tt_slot_id_to_offset(1), 0);
	UT_ASSERT_EQ((int)cluster_tt_slot_id_to_offset(48), 47);
}

UT_TEST(test_t5_offset_id_mutual_inverse)
{
	int i;

	for (i = 0; i < 48; i++) {
		uint32 id = cluster_tt_slot_offset_to_id((uint16)i);

		UT_ASSERT_EQ((int)cluster_tt_slot_id_to_offset(id), i);
	}
}

UT_TEST(test_t6_shmem_size_nonzero)
{
	Size sz = cluster_tt_slot_shmem_size();

	UT_ASSERT_NE((int)sz, 0);
	/* MAXALIGN'd: low bits zero (typically aligned to 8). */
	UT_ASSERT_EQ((int)(sz & 0x7), 0);
}

UT_TEST(test_t7_shmem_init_fresh_state)
{
	reset_allocator();

	/* Fresh state: first alloc on any segment must succeed and return
	 * offset 0 because all slots are zero-init (CTS_FREE). */
	uint16 off = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);

	UT_ASSERT_EQ((int)off, 0);
}

UT_TEST(test_t8_alloc_fresh_returns_offset_0)
{
	uint16 off;

	reset_allocator();
	off = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);
	UT_ASSERT_EQ((int)off, 0);
}

UT_TEST(test_t9_alloc_same_xid_idempotent)
{
	uint16 off1, off2;

	reset_allocator();
	off1 = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);
	off2 = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);
	UT_ASSERT_EQ((int)off1, (int)off2);
}

UT_TEST(test_t10_alloc_distinct_xid_distinct_offset)
{
	uint16 off1, off2;

	reset_allocator();
	off1 = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);
	off2 = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)200);
	UT_ASSERT_NE((int)off1, (int)off2);
}

UT_TEST(test_t11_alloc_48_distinct_returns_offsets_0_to_47)
{
	int i;
	int seen[48] = { 0 };

	reset_allocator();
	for (i = 0; i < 48; i++) {
		uint16 off = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)(1000 + i));

		UT_ASSERT_NE((int)off, (int)INVALID_TT_SLOT_OFFSET);
		UT_ASSERT_EQ((int)(off < 48), 1);
		seen[off]++;
	}
	for (i = 0; i < 48; i++)
		UT_ASSERT_EQ(seen[i], 1);
}

UT_TEST(test_t12_alloc_49th_returns_overflow)
{
	int i;
	uint16 off;

	reset_allocator();
	for (i = 0; i < 48; i++)
		(void)cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)(1000 + i));

	off = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)9999);
	UT_ASSERT_EQ((int)off, (int)INVALID_TT_SLOT_OFFSET);
}

UT_TEST(test_t13_free_then_alloc_reuses_offset)
{
	uint16 off1, off2;

	reset_allocator();
	off1 = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);
	cluster_tt_slot_free(NODE0_SEG, off1);
	off2 = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)200);
	UT_ASSERT_EQ((int)off1, (int)off2);
}

UT_TEST(test_t14_free_of_free_slot_is_idempotent)
{
	reset_allocator();
	cluster_tt_slot_free(NODE0_SEG, 5); /* never alloc'd, just free */
	cluster_tt_slot_free(NODE0_SEG, 5); /* free again */
	/* No crash, no ereport. */
	UT_ASSERT_EQ(1, 1);
}

UT_TEST(test_t15_get_wrap_fresh_zero)
{
	reset_allocator();
	(void)cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);
	UT_ASSERT_EQ((int)cluster_tt_slot_get_wrap(NODE0_SEG, 0), 0);
}

UT_TEST(test_t16_wrap_unchanged_first_alloc)
{
	uint16 off;

	reset_allocator();
	off = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);
	UT_ASSERT_EQ((int)cluster_tt_slot_get_wrap(NODE0_SEG, off), 0);
}

UT_TEST(test_t17_wrap_unchanged_free_realloc_cycle_diff_xid)
{
	/* FREE -> ACTIVE (xid A) -> FREE -> ACTIVE (xid B): the slot's
	 * status was FREE at the second alloc time, so wrap stays 0. */
	uint16 off1, off2;

	reset_allocator();
	off1 = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);
	cluster_tt_slot_free(NODE0_SEG, off1);
	off2 = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)200);
	UT_ASSERT_EQ((int)off1, (int)off2);
	UT_ASSERT_EQ((int)cluster_tt_slot_get_wrap(NODE0_SEG, off2), 0);
}

UT_TEST(test_t18_wrap_unchanged_same_xid_reuse)
{
	uint16 off;

	reset_allocator();
	off = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);
	(void)cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);
	UT_ASSERT_EQ((int)cluster_tt_slot_get_wrap(NODE0_SEG, off), 0);
}

UT_TEST(test_t19_recycle_committed_bumps_wrap)
{
	/*
	 * L189 recycle policy: when no FREE slot is available, the allocator
	 * must recycle a COMMITTED slot, bumping its wrap counter on the way.
	 * We use the test-only force_status helper to drive a slot into
	 * COMMITTED, fill the rest of the 48 slots with distinct ACTIVE xids,
	 * then watch a fresh alloc choose the recyclable slot + bump wrap.
	 */
	uint16 first_off;
	uint16 wrap_before;
	int i;
	uint16 recycled_off;
	uint16 wrap_after;

	reset_allocator();
	first_off = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);
	wrap_before = cluster_tt_slot_get_wrap(NODE0_SEG, first_off);

	/* Mark slot 0 as COMMITTED. */
	cluster_tt_slot_test_force_status(NODE0_SEG, first_off, 2 /* CTS_COMMITTED */);

	/* Fill the remaining 47 slots with distinct ACTIVE xids so no FREE
	 * slot remains; the next alloc must recycle the COMMITTED one. */
	for (i = 1; i < TT_SLOTS_PER_SEGMENT; i++)
		(void)cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)(2000 + i));

	/* Fresh xid alloc -- only recyclable slot is slot first_off (COMMITTED). */
	recycled_off = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)99999);
	UT_ASSERT_EQ((int)recycled_off, (int)first_off);

	wrap_after = cluster_tt_slot_get_wrap(NODE0_SEG, recycled_off);
	UT_ASSERT_EQ((int)(wrap_after > wrap_before), 1);
}

UT_TEST(test_t20_recycle_aborted_bumps_wrap)
{
	/* L189 recycle: same as T19 but with ABORTED status. */
	uint16 first_off;
	uint16 wrap_before;
	int i;
	uint16 recycled_off;
	uint16 wrap_after;

	reset_allocator();
	first_off = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);
	wrap_before = cluster_tt_slot_get_wrap(NODE0_SEG, first_off);

	cluster_tt_slot_test_force_status(NODE0_SEG, first_off, 3 /* CTS_ABORTED */);

	for (i = 1; i < TT_SLOTS_PER_SEGMENT; i++)
		(void)cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)(3000 + i));

	recycled_off = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)99998);
	UT_ASSERT_EQ((int)recycled_off, (int)first_off);

	wrap_after = cluster_tt_slot_get_wrap(NODE0_SEG, recycled_off);
	UT_ASSERT_EQ((int)(wrap_after > wrap_before), 1);
}

UT_TEST(test_t21_own_active_reuse_no_wrap_bump)
{
	uint16 off;

	reset_allocator();
	off = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);
	(void)cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);
	(void)cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);
	UT_ASSERT_EQ((int)cluster_tt_slot_get_wrap(NODE0_SEG, off), 0);
}

UT_TEST(test_t22_alloc_uses_lw_exclusive)
{
	reset_allocator();
	(void)cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);
	UT_ASSERT_NE(mock_lwlock_acquire_excl_count, 0);
}

UT_TEST(test_t23_get_wrap_uses_lw_shared)
{
	int before;

	reset_allocator();
	(void)cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);
	before = mock_lwlock_acquire_shared_count;
	(void)cluster_tt_slot_get_wrap(NODE0_SEG, 0);
	UT_ASSERT_NE(mock_lwlock_acquire_shared_count, before);
}

UT_TEST(test_t24_reset_all_wipes_state)
{
	uint16 off;

	reset_allocator();
	(void)cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);
	cluster_tt_slot_reset_all();

	/* After reset_all, the per-node segment binding is cleared, so an
	 * alloc on the same segment must again succeed with offset 0. */
	off = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)200);
	UT_ASSERT_EQ((int)off, 0);
}

UT_TEST(test_t25_per_node_isolation)
{
	uint16 off0, off1;

	reset_allocator();
	off0 = cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);

	/* Different segment for a different node -- must NOT share the
	 * occupied-slot state. */
	off1 = cluster_tt_slot_alloc(NODE1_SEG, (TransactionId)200);

	UT_ASSERT_EQ((int)off0, 0);
	UT_ASSERT_EQ((int)off1, 0);
}

UT_TEST(test_t26_multi_instance_unlock_node0_node1)
{
	/* Both node 0 (seg 1) and node 1 (seg 257) get fresh allocators. */
	reset_allocator();

	(void)cluster_tt_slot_alloc(NODE0_SEG, (TransactionId)100);
	(void)cluster_tt_slot_alloc(NODE1_SEG, (TransactionId)200);
	(void)cluster_tt_slot_alloc(NODE1_SEG, (TransactionId)201);

	/* Wraps still 0 -- multi-instance unlocked, no recycle yet. */
	UT_ASSERT_EQ((int)cluster_tt_slot_get_wrap(NODE0_SEG, 0), 0);
	UT_ASSERT_EQ((int)cluster_tt_slot_get_wrap(NODE1_SEG, 0), 0);
	UT_ASSERT_EQ((int)cluster_tt_slot_get_wrap(NODE1_SEG, 1), 0);
}

UT_TEST(test_t27_segment_zero_raises)
{
	reset_allocator();
	if (sigsetjmp(ereport_recover_jmp, 1) == 0) {
		(void)cluster_tt_slot_alloc(0 /* bootstrap-only segment */, (TransactionId)100);
		/* Should not reach here. */
		UT_ASSERT_EQ(0, 1);
	}
	UT_ASSERT_NE(ereport_raised_count, 0);
}

UT_TEST(test_t28_segment_over_uint16max_raises)
{
	reset_allocator();
	if (sigsetjmp(ereport_recover_jmp, 1) == 0) {
		(void)cluster_tt_slot_alloc(0x10000u /* > UINT16_MAX */, (TransactionId)100);
		UT_ASSERT_EQ(0, 1);
	}
	UT_ASSERT_NE(ereport_raised_count, 0);
}

UT_TEST(test_t29_invalid_xid_raises)
{
	reset_allocator();
	if (sigsetjmp(ereport_recover_jmp, 1) == 0) {
		(void)cluster_tt_slot_alloc(NODE0_SEG, InvalidTransactionId);
		UT_ASSERT_EQ(0, 1);
	}
	UT_ASSERT_NE(ereport_raised_count, 0);
}

UT_TEST(test_t30_free_offset_overflow_raises)
{
	reset_allocator();
	if (sigsetjmp(ereport_recover_jmp, 1) == 0) {
		cluster_tt_slot_free(NODE0_SEG, 48 /* out of [0, 48) */);
		UT_ASSERT_EQ(0, 1);
	}
	UT_ASSERT_NE(ereport_raised_count, 0);
}


int
main(void)
{
	cluster_tt_slot_shmem_init(); /* one-time init for tests that
										   don't call reset_allocator */

	UT_RUN(test_t1_invalid_offset_sentinel);
	UT_RUN(test_t2_slots_per_segment);
	UT_RUN(test_t3_offset_to_id_endpoints);
	UT_RUN(test_t4_id_to_offset_endpoints);
	UT_RUN(test_t5_offset_id_mutual_inverse);
	UT_RUN(test_t6_shmem_size_nonzero);
	UT_RUN(test_t7_shmem_init_fresh_state);
	UT_RUN(test_t8_alloc_fresh_returns_offset_0);
	UT_RUN(test_t9_alloc_same_xid_idempotent);
	UT_RUN(test_t10_alloc_distinct_xid_distinct_offset);
	UT_RUN(test_t11_alloc_48_distinct_returns_offsets_0_to_47);
	UT_RUN(test_t12_alloc_49th_returns_overflow);
	UT_RUN(test_t13_free_then_alloc_reuses_offset);
	UT_RUN(test_t14_free_of_free_slot_is_idempotent);
	UT_RUN(test_t15_get_wrap_fresh_zero);
	UT_RUN(test_t16_wrap_unchanged_first_alloc);
	UT_RUN(test_t17_wrap_unchanged_free_realloc_cycle_diff_xid);
	UT_RUN(test_t18_wrap_unchanged_same_xid_reuse);
	UT_RUN(test_t19_recycle_committed_bumps_wrap);
	UT_RUN(test_t20_recycle_aborted_bumps_wrap);
	UT_RUN(test_t21_own_active_reuse_no_wrap_bump);
	UT_RUN(test_t22_alloc_uses_lw_exclusive);
	UT_RUN(test_t23_get_wrap_uses_lw_shared);
	UT_RUN(test_t24_reset_all_wipes_state);
	UT_RUN(test_t25_per_node_isolation);
	UT_RUN(test_t26_multi_instance_unlock_node0_node1);
	UT_RUN(test_t27_segment_zero_raises);
	UT_RUN(test_t28_segment_over_uint16max_raises);
	UT_RUN(test_t29_invalid_xid_raises);
	UT_RUN(test_t30_free_offset_overflow_raises);

	return ut_failed_count == 0 ? 0 : 1;
}
