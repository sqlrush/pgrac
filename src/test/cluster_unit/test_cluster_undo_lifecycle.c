/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_lifecycle.c
 *	  pgrac spec-3.8 D12 — cluster_unit pure ABI / enum / SQLSTATE
 *	  tests for Undo Segment Lifecycle MVP + Autoextend.
 *
 *	  10 tests covering:
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
 *	  Behavioural / lifecycle coverage of autoextend + state transitions
 *	  + concurrency double-checked locking lives in cluster_tap t/214 (D13).
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


int
main(int argc, char **argv)
{
	UT_PLAN(10);

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

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
