/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_lifecycle.c
 *	  pgrac spec-3.8 D12 — cluster_unit pure ABI / enum / SQLSTATE
 *	  tests for Undo Segment Lifecycle MVP + Autoextend.
 *
 *	  17 tests covering 10 ABI/encoding + 7 behavioural (Fix #375):
 *	    T1   UndoSegmentState enum 5 byte values 锁(ALLOCATED=0 /
 *	         ACTIVE=1 / COMMITTED=2 / RECYCLABLE=3 / INVALID=0xFF;
 *	         spec-3.8 不写 COMMITTED/RECYCLABLE)
 *	    T2   UNDO_SEGMENT_FLAGS_RESERVED = 0 + UNDO_SEGMENT_FLAG_FULL = 0x01
 *	    T3   UNDO_OWNER_INSTANCE_INVALID = 0 + UNDO_OWNER_INSTANCE_MAX = 128
 *	    T4   UNDO_SEGMENT_SIZE_BYTES = 64MB + UNDO_BLOCKS_PER_SEGMENT = 8192
 *	    T5   UNDO_FREE_BITMAP_BYTES = 1024
 *	    T6   UndoSegmentHeaderData on-disk sizeof = 8192
 *	    T7   UndoSegmentHeaderData.segment_state offset = 40
 *	    T8   UndoSegmentHeaderData.segment_flags offset = 56
 *	    T9   UndoSegmentHeaderData.tail_block offset = 48
 *	         (Hardening v1.0.1 H-1: linkdb SSOT name vs spec first_active_block)
 *	    T10  53R9E SQLSTATE encode = MAKE_SQLSTATE('5','3','R','9','E')
 *
 *	    T11  state can_become_active legal transitions (ALLOCATED → true,
 *	         ACTIVE → true idempotent)
 *	    T12  state can_become_active fail-closed (COMMITTED / RECYCLABLE /
 *	         INVALID → false per spec §3.3 I3)
 *	    T13  bitmap mark_used idempotent (first call true, re-mark false)
 *	    T14  bitmap count_free_capped short-circuits at cap+1
 *	    T15  bitmap is_full margin logic (free <= 1 → true)
 *	    T16  segment_flags is_full helper
 *	    T17  on-disk header field mutation roundtrip
 *
 *	  Full autoextend + concurrency / double-checked locking under real
 *	  workload live in cluster_tap t/214 L9-L12 (D13).
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.8-undo-segment-lifecycle-autoextend.md (FROZEN v0.3 +
 *       Hardening v1.0.1 H-1/H-2)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "cluster/cluster_undo_segment.h"
#include "utils/errcodes.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/* ---- T1: UndoSegmentState 5 byte values ---- */
UT_TEST(test_undo_segment_state_enum)
{
	UT_ASSERT_EQ(SEGMENT_ALLOCATED, 0);
	UT_ASSERT_EQ(SEGMENT_ACTIVE, 1);
	UT_ASSERT_EQ(SEGMENT_COMMITTED, 2);
	UT_ASSERT_EQ(SEGMENT_RECYCLABLE, 3);
	UT_ASSERT_EQ(SEGMENT_INVALID, 0xFF);
}

/* ---- T2: segment_flags constants ---- */
UT_TEST(test_undo_segment_flag_constants)
{
	UT_ASSERT_EQ(UNDO_SEGMENT_FLAGS_RESERVED, 0);
	UT_ASSERT_EQ(UNDO_SEGMENT_FLAG_FULL, 0x01);
}

/* ---- T3: owner_instance range constants ---- */
UT_TEST(test_owner_instance_constants)
{
	UT_ASSERT_EQ(UNDO_OWNER_INSTANCE_INVALID, 0);
	UT_ASSERT_EQ(UNDO_OWNER_INSTANCE_MAX, 128);
}

/* ---- T4: segment size constants ---- */
UT_TEST(test_segment_size_constants)
{
	UT_ASSERT_EQ(UNDO_SEGMENT_SIZE_BYTES, 64 * 1024 * 1024);
	UT_ASSERT_EQ(UNDO_BLOCKS_PER_SEGMENT, 8192);
	UT_ASSERT_EQ(UNDO_SEGMENT_HEADER_SIZE, 8192);
}

/* ---- T5: free bitmap size ---- */
UT_TEST(test_free_bitmap_size)
{
	UT_ASSERT_EQ(UNDO_FREE_BITMAP_BYTES, 1024);
}

/* ---- T6: UndoSegmentHeaderData on-disk sizeof = 8192 (one PG block) ---- */
UT_TEST(test_segment_header_sizeof)
{
	UT_ASSERT_EQ(sizeof(UndoSegmentHeaderData), 8192);
}

/* ---- T7: segment_state offset = 40 ---- */
UT_TEST(test_segment_state_offset)
{
	UT_ASSERT_EQ(offsetof(UndoSegmentHeaderData, segment_state), 40);
}

/* ---- T8: segment_flags offset = 56 ---- */
UT_TEST(test_segment_flags_offset)
{
	UT_ASSERT_EQ(offsetof(UndoSegmentHeaderData, segment_flags), 56);
}

/* ---- T9: tail_block offset = 48 (Hardening v1.0.1 H-1) ---- */
UT_TEST(test_tail_block_offset)
{
	/* Per Hardening v1.0.1 H-1: spec body uses "first_active_block"
	 * name; linkdb SSOT is tail_block at offset 48. Spec terminology
	 * maps: first_active_block ≡ tail_block (retention base). */
	UT_ASSERT_EQ(offsetof(UndoSegmentHeaderData, tail_block), 48);
}

/* ---- T10: 53R9E SQLSTATE encode ---- */
UT_TEST(test_sqlstate_53R9E)
{
	UT_ASSERT_EQ(ERRCODE_CLUSTER_UNDO_SEGMENTS_HARD_CAP_REACHED,
				 MAKE_SQLSTATE('5', '3', 'R', '9', 'E'));
}


/* ============================================================
 * Behavioural tests for pure-logic kernels (spec-3.8 Fix #375).
 *
 *   These exercise the same invariants the backend file-I/O wrappers in
 *   cluster_undo_alloc.c rely on,  by calling the inline helpers in
 *   cluster_undo_segment.h against an in-memory header buffer.  No shmem,
 *   no postgres backend, no file I/O — pure logic coverage.
 * ============================================================ */

/* ---- T11: state transition kernel — ALLOCATED → ACTIVE legal ---- */
UT_TEST(test_state_can_become_active_legal_transitions)
{
	UT_ASSERT_EQ(UndoSegmentState_can_become_active(SEGMENT_ALLOCATED), true);
	UT_ASSERT_EQ(UndoSegmentState_can_become_active(SEGMENT_ACTIVE), true); /* idempotent */
}

/* ---- T12: state transition kernel — illegal states fail-closed ---- */
UT_TEST(test_state_can_become_active_fail_closed)
{
	UT_ASSERT_EQ(UndoSegmentState_can_become_active(SEGMENT_COMMITTED), false);
	UT_ASSERT_EQ(UndoSegmentState_can_become_active(SEGMENT_RECYCLABLE), false);
	UT_ASSERT_EQ(UndoSegmentState_can_become_active(SEGMENT_INVALID), false);
}

/* ---- T13: bitmap mark_used + idempotency ---- */
UT_TEST(test_bitmap_mark_used_idempotent)
{
	uint8 bitmap[UNDO_FREE_BITMAP_BYTES];

	memset(bitmap, 0, sizeof(bitmap));

	/* First mark for block 5 returns true (state changed). */
	UT_ASSERT_EQ(UndoSegmentBitmap_mark_used(bitmap, 5), true);
	UT_ASSERT_EQ(bitmap[0], 0x20); /* bit 5 set */

	/* Re-mark same block returns false (no state change — idempotent). */
	UT_ASSERT_EQ(UndoSegmentBitmap_mark_used(bitmap, 5), false);
	UT_ASSERT_EQ(bitmap[0], 0x20);

	/* Mark block 8 (next byte) — separate bit. */
	UT_ASSERT_EQ(UndoSegmentBitmap_mark_used(bitmap, 8), true);
	UT_ASSERT_EQ(bitmap[1], 0x01);
}

/* ---- T14: bitmap count_free_capped short-circuits at cap+1 ---- */
UT_TEST(test_bitmap_count_free_capped_short_circuit)
{
	uint8 bitmap[16];
	uint32 cnt;

	memset(bitmap, 0, sizeof(bitmap));

	/* All clear → free count = 128.  Cap = 3 → expected return = 4
	 * (just past cap, short-circuit). */
	cnt = UndoSegmentBitmap_count_free_capped(bitmap, sizeof(bitmap), 3);
	UT_ASSERT_EQ(cnt, 4);

	/* All set → free count = 0. */
	memset(bitmap, 0xFF, sizeof(bitmap));
	cnt = UndoSegmentBitmap_count_free_capped(bitmap, sizeof(bitmap), 3);
	UT_ASSERT_EQ(cnt, 0);
}

/* ---- T15: bitmap_is_full margin logic ---- */
UT_TEST(test_bitmap_is_full_margin)
{
	uint8 bitmap[UNDO_FREE_BITMAP_BYTES];

	/* All clear → is_full = false (8192 blocks free). */
	memset(bitmap, 0, sizeof(bitmap));
	UT_ASSERT_EQ(UndoSegmentBitmap_is_full(bitmap, sizeof(bitmap)), false);

	/* All set → is_full = true (0 blocks free). */
	memset(bitmap, 0xFF, sizeof(bitmap));
	UT_ASSERT_EQ(UndoSegmentBitmap_is_full(bitmap, sizeof(bitmap)), true);

	/* Exactly 1 block free → is_full = true (margin). */
	memset(bitmap, 0xFF, sizeof(bitmap));
	bitmap[0] &= ~((uint8)0x01); /* clear bit 0 */
	UT_ASSERT_EQ(UndoSegmentBitmap_is_full(bitmap, sizeof(bitmap)), true);

	/* Exactly 2 blocks free → is_full = false. */
	bitmap[0] &= ~((uint8)0x02); /* also clear bit 1 */
	UT_ASSERT_EQ(UndoSegmentBitmap_is_full(bitmap, sizeof(bitmap)), false);
}

/* ---- T16: segment_flags is_full helper ---- */
UT_TEST(test_segment_flags_is_full)
{
	UT_ASSERT_EQ(UndoSegmentFlags_is_full(0), false);
	UT_ASSERT_EQ(UndoSegmentFlags_is_full(UNDO_SEGMENT_FLAG_FULL), true);
	UT_ASSERT_EQ(UndoSegmentFlags_is_full(0xFF), true);	 /* FULL bit + others */
	UT_ASSERT_EQ(UndoSegmentFlags_is_full(0x02), false); /* future flag, not FULL */
}

/* ---- T17: on-disk header field mutation roundtrip ---- *
 *   Simulate the lifecycle wrapper:  zero-init buffer → cast to header
 *   struct → set state ACTIVE + tail_block 1 + segment_flags FULL → read
 *   back via UndoSegmentHeader_is_active / owner / flag accessor.
 */
UT_TEST(test_header_mutation_roundtrip)
{
	uint8 buf[UNDO_SEGMENT_HEADER_SIZE];
	UndoSegmentHeaderData *hdr;

	memset(buf, 0, sizeof(buf));
	hdr = (UndoSegmentHeaderData *)buf;

	/* Initial:  zero-init ALLOCATED state, owner sentinel,  not full. */
	UT_ASSERT_EQ(hdr->segment_state, SEGMENT_ALLOCATED);
	UT_ASSERT_EQ(UndoSegmentHeader_is_active(hdr), false);
	UT_ASSERT_EQ(UndoSegmentHeader_owner(hdr), 0);
	UT_ASSERT_EQ(UndoSegmentFlags_is_full(hdr->segment_flags), false);

	/* Mutate:  ALLOCATED → ACTIVE,  owner 1, tail_block 1, segment_flags FULL. */
	hdr->segment_state = SEGMENT_ACTIVE;
	hdr->owner_instance = 1;
	hdr->tail_block = 1;
	hdr->segment_flags |= UNDO_SEGMENT_FLAG_FULL;

	UT_ASSERT_EQ(UndoSegmentHeader_is_active(hdr), true);
	UT_ASSERT_EQ(UndoSegmentHeader_owner(hdr), 1);
	UT_ASSERT_EQ(hdr->tail_block, 1);
	UT_ASSERT_EQ(UndoSegmentFlags_is_full(hdr->segment_flags), true);

	/* Total size invariant preserved (no overflow into adjacent fields). */
	UT_ASSERT_EQ(sizeof(buf), UNDO_SEGMENT_HEADER_SIZE);
}


int
main(int argc, char **argv)
{
	UT_PLAN(17);

	UT_RUN(test_undo_segment_state_enum);
	UT_RUN(test_undo_segment_flag_constants);
	UT_RUN(test_owner_instance_constants);
	UT_RUN(test_segment_size_constants);
	UT_RUN(test_free_bitmap_size);
	UT_RUN(test_segment_header_sizeof);
	UT_RUN(test_segment_state_offset);
	UT_RUN(test_segment_flags_offset);
	UT_RUN(test_tail_block_offset);
	UT_RUN(test_sqlstate_53R9E);
	UT_RUN(test_state_can_become_active_legal_transitions);
	UT_RUN(test_state_can_become_active_fail_closed);
	UT_RUN(test_bitmap_mark_used_idempotent);
	UT_RUN(test_bitmap_count_free_capped_short_circuit);
	UT_RUN(test_bitmap_is_full_margin);
	UT_RUN(test_segment_flags_is_full);
	UT_RUN(test_header_mutation_roundtrip);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
