/*-------------------------------------------------------------------------
 *
 * test_cluster_itl_wal.c
 *	  pgrac spec-3.4a D12 — cluster_unit static + ABI tests for the
 *	  heap WAL ITL delta block-local array layout (D7).
 *
 *	  18 tests:
 *	    T1   xl_heap_itl_delta sizeof == 24
 *	    T2   xl_heap_itl_delta.slot_idx offset == 0
 *	    T3   xl_heap_itl_delta.flags_after offset == 2
 *	    T4   xl_heap_itl_delta.xid offset == 4
 *	    T5   xl_heap_itl_delta.write_scn offset == 8
 *	    T6   xl_heap_itl_delta.commit_scn offset == 16
 *	    T7   xl_heap_itl_delta_block deltas offset == 4 (header is 4B)
 *	    T8   XLH_INSERT_ITL_DELTA == (1<<7)
 *	    T9   XLH_UPDATE_ITL_DELTA == (1<<7)
 *	    T10  XLH_DELETE_ITL_DELTA == (1<<7)
 *	    T11  No collision: XLH_INSERT_* bits 0..5 distinct from ITL_DELTA bit 7
 *	    T12  No collision: XLH_UPDATE_* bits 0..6 distinct from ITL_DELTA bit 7
 *	    T13  No collision: XLH_DELETE_* bits 0..4 distinct from ITL_DELTA bit 7
 *	    T14  ItlSlotData flags namespace stable (FREE / ACTIVE / COMMITTED /
 *	         ABORTED / NEEDS_CLEANOUT)
 *	    T15  xl_heap_itl_delta_block.ndeltas offset == 0
 *	    T16  xl_heap_itl_delta_block.reserved offset == 2
 *	    T17  COMMITTED transition requires valid commit_scn (compile-time
 *	         marker via SCN_VALID macro)
 *	    T18  block-local array supports ndeltas = 0 (empty array per block
 *	         when ITL delta optional)
 *
 *	  Standalone executable per spec-0.4 §9.2; no PG backend required.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_itl_wal.c
 *
 * Spec: spec-3.4a-itl-write-path-activation-minimal-wal.md (v1.0 FROZEN 2026-05-23)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>

#include "access/heapam_xlog.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_scn.h"

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


UT_TEST(test_t1_delta_sizeof_24)
{
	UT_ASSERT_EQ((int)sizeof(xl_heap_itl_delta), 24);
}

UT_TEST(test_t2_delta_slot_idx_offset_0)
{
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta, slot_idx), 0);
}

UT_TEST(test_t3_delta_flags_after_offset_2)
{
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta, flags_after), 2);
}

UT_TEST(test_t4_delta_xid_offset_4)
{
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta, xid), 4);
}

UT_TEST(test_t5_delta_write_scn_offset_8)
{
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta, write_scn), 8);
}

UT_TEST(test_t6_delta_commit_scn_offset_16)
{
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta, commit_scn), 16);
}

UT_TEST(test_t7_block_header_8b_aligned)
{
	/* 4B logical header (ndeltas + reserved) + 4B explicit pad for
	 * SCN 8-byte alignment of deltas[] -> 8B effective header. */
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta_block, deltas), 8);
}

UT_TEST(test_t8_insert_itl_delta_bit_7)
{
	UT_ASSERT_EQ((int)XLH_INSERT_ITL_DELTA, (int)(1u << 7));
}

UT_TEST(test_t9_update_itl_delta_bit_7)
{
	UT_ASSERT_EQ((int)XLH_UPDATE_ITL_DELTA, (int)(1u << 7));
}

UT_TEST(test_t10_delete_itl_delta_bit_7)
{
	UT_ASSERT_EQ((int)XLH_DELETE_ITL_DELTA, (int)(1u << 7));
}

UT_TEST(test_t11_no_collision_insert)
{
	UT_ASSERT_NE((int)XLH_INSERT_ITL_DELTA, (int)XLH_INSERT_ALL_VISIBLE_CLEARED);
	UT_ASSERT_NE((int)XLH_INSERT_ITL_DELTA, (int)XLH_INSERT_LAST_IN_MULTI);
	UT_ASSERT_NE((int)XLH_INSERT_ITL_DELTA, (int)XLH_INSERT_IS_SPECULATIVE);
	UT_ASSERT_NE((int)XLH_INSERT_ITL_DELTA, (int)XLH_INSERT_CONTAINS_NEW_TUPLE);
	UT_ASSERT_NE((int)XLH_INSERT_ITL_DELTA, (int)XLH_INSERT_ON_TOAST_RELATION);
	UT_ASSERT_NE((int)XLH_INSERT_ITL_DELTA, (int)XLH_INSERT_ALL_FROZEN_SET);
}

UT_TEST(test_t12_no_collision_update)
{
	UT_ASSERT_NE((int)XLH_UPDATE_ITL_DELTA, (int)XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED);
	UT_ASSERT_NE((int)XLH_UPDATE_ITL_DELTA, (int)XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED);
	UT_ASSERT_NE((int)XLH_UPDATE_ITL_DELTA, (int)XLH_UPDATE_CONTAINS_OLD_TUPLE);
	UT_ASSERT_NE((int)XLH_UPDATE_ITL_DELTA, (int)XLH_UPDATE_CONTAINS_OLD_KEY);
	UT_ASSERT_NE((int)XLH_UPDATE_ITL_DELTA, (int)XLH_UPDATE_CONTAINS_NEW_TUPLE);
	UT_ASSERT_NE((int)XLH_UPDATE_ITL_DELTA, (int)XLH_UPDATE_PREFIX_FROM_OLD);
	UT_ASSERT_NE((int)XLH_UPDATE_ITL_DELTA, (int)XLH_UPDATE_SUFFIX_FROM_OLD);
}

UT_TEST(test_t13_no_collision_delete)
{
	UT_ASSERT_NE((int)XLH_DELETE_ITL_DELTA, (int)XLH_DELETE_ALL_VISIBLE_CLEARED);
	UT_ASSERT_NE((int)XLH_DELETE_ITL_DELTA, (int)XLH_DELETE_CONTAINS_OLD_TUPLE);
	UT_ASSERT_NE((int)XLH_DELETE_ITL_DELTA, (int)XLH_DELETE_CONTAINS_OLD_KEY);
	UT_ASSERT_NE((int)XLH_DELETE_ITL_DELTA, (int)XLH_DELETE_IS_SUPER);
	UT_ASSERT_NE((int)XLH_DELETE_ITL_DELTA, (int)XLH_DELETE_IS_PARTITION_MOVE);
}

UT_TEST(test_t14_itl_flag_namespace)
{
	UT_ASSERT_EQ((int)ITL_FLAG_FREE, 0);
	UT_ASSERT_EQ((int)ITL_FLAG_ACTIVE, 1);
	UT_ASSERT_EQ((int)ITL_FLAG_COMMITTED, 2);
	UT_ASSERT_EQ((int)ITL_FLAG_ABORTED, 3);
	UT_ASSERT_EQ((int)ITL_FLAG_NEEDS_CLEANOUT, 4);
}

UT_TEST(test_t15_block_ndeltas_offset_0)
{
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta_block, ndeltas), 0);
}

UT_TEST(test_t16_block_reserved_offset_2)
{
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta_block, reserved), 2);
}

UT_TEST(test_t17_committed_requires_valid_scn)
{
	/*
	 * Compile-time guard via SCN_VALID macro existence.  Real PANIC
	 * enforcement lives in heap WAL redo (D9 — Step 8).
	 */
	UT_ASSERT_EQ((int)SCN_VALID(InvalidScn), 0);
	UT_ASSERT_NE((int)SCN_VALID((SCN)1), 0);
}

UT_TEST(test_t18_block_allows_zero_ndeltas)
{
	xl_heap_itl_delta_block hdr;

	hdr.ndeltas = 0;
	hdr.reserved = 0;
	UT_ASSERT_EQ((int)hdr.ndeltas, 0);
	UT_ASSERT_EQ((int)hdr.reserved, 0);
}

/* ---------- spec-3.4b D6 / F9 v2 40B ABI tests ---------- */

UT_TEST(test_t19_delta_v2_sizeof_40)
{
	UT_ASSERT_EQ((int)sizeof(xl_heap_itl_delta_v2), 40);
}

UT_TEST(test_t20_delta_v2_slot_idx_offset_0)
{
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta_v2, slot_idx), 0);
}

UT_TEST(test_t21_delta_v2_flags_after_offset_2)
{
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta_v2, flags_after), 2);
}

UT_TEST(test_t22_delta_v2_xid_offset_4)
{
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta_v2, xid), 4);
}

UT_TEST(test_t23_delta_v2_write_scn_offset_8)
{
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta_v2, write_scn), 8);
}

UT_TEST(test_t24_delta_v2_commit_scn_offset_16)
{
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta_v2, commit_scn), 16);
}

UT_TEST(test_t25_delta_v2_undo_segment_head_offset_24)
{
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta_v2, undo_segment_head), 24);
}

UT_TEST(test_t26_format_version_v1_v2_distinct)
{
	UT_ASSERT_EQ((int)CLUSTER_ITL_DELTA_FORMAT_V1, 0);
	UT_ASSERT_EQ((int)CLUSTER_ITL_DELTA_FORMAT_V2, 1);
}

UT_TEST(test_t27_block_format_version_repurposed_pad)
{
	/* spec-3.4b D6: header _pad field renamed to format_version.
	 * Offset must remain 4 to preserve wire compatibility. */
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta_block, format_version), 4);
}

UT_TEST(test_t28_block_deltas_offset_still_8)
{
	/* Backward-compat check: the deltas[] flexible array still starts at
	 * offset 8 regardless of format_version (v1 24B vs v2 40B). */
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta_block, deltas), 8);
}

UT_TEST(test_t29_v1_v2_slot_idx_same_offset)
{
	/* spec-3.4b D6: cluster_itl_wal_block_first_slot_idx relies on
	 * slot_idx being at offset 0 for BOTH v1 and v2.  Catch regressions
	 * that would reorder either struct. */
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta, slot_idx),
				 (int)offsetof(xl_heap_itl_delta_v2, slot_idx));
}

/* ---------- spec-5.19 MG-D v3 32B ABI tests (commit_scn dropped) ---------- */

UT_TEST(test_t30_delta_v3_sizeof_32)
{
	/* v2 40B minus the always-Invalid 8B commit_scn == 32B. */
	UT_ASSERT_EQ((int)sizeof(xl_heap_itl_delta_v3), 32);
}

UT_TEST(test_t31_delta_v3_field_offsets)
{
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta_v3, slot_idx), 0);
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta_v3, flags_after), 2);
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta_v3, xid), 4);
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta_v3, write_scn), 8);
	/* commit_scn is gone; undo_segment_head moves up from 24 (v2) to 16. */
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta_v3, undo_segment_head), 16);
}

UT_TEST(test_t32_format_version_v3_distinct)
{
	UT_ASSERT_EQ((int)CLUSTER_ITL_DELTA_FORMAT_V3, 2);
	UT_ASSERT_NE((int)CLUSTER_ITL_DELTA_FORMAT_V3, (int)CLUSTER_ITL_DELTA_FORMAT_V1);
	UT_ASSERT_NE((int)CLUSTER_ITL_DELTA_FORMAT_V3, (int)CLUSTER_ITL_DELTA_FORMAT_V2);
}

UT_TEST(test_t33_v1_v2_v3_slot_idx_same_offset)
{
	/* cluster_itl_wal_block_first_slot_idx reads slot_idx at offset 0
	 * version-agnostically -- must hold for v3 too. */
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta, slot_idx),
				 (int)offsetof(xl_heap_itl_delta_v3, slot_idx));
	UT_ASSERT_EQ((int)offsetof(xl_heap_itl_delta_v2, slot_idx),
				 (int)offsetof(xl_heap_itl_delta_v3, slot_idx));
}


int
main(void)
{
	UT_RUN(test_t1_delta_sizeof_24);
	UT_RUN(test_t2_delta_slot_idx_offset_0);
	UT_RUN(test_t3_delta_flags_after_offset_2);
	UT_RUN(test_t4_delta_xid_offset_4);
	UT_RUN(test_t5_delta_write_scn_offset_8);
	UT_RUN(test_t6_delta_commit_scn_offset_16);
	UT_RUN(test_t7_block_header_8b_aligned);
	UT_RUN(test_t8_insert_itl_delta_bit_7);
	UT_RUN(test_t9_update_itl_delta_bit_7);
	UT_RUN(test_t10_delete_itl_delta_bit_7);
	UT_RUN(test_t11_no_collision_insert);
	UT_RUN(test_t12_no_collision_update);
	UT_RUN(test_t13_no_collision_delete);
	UT_RUN(test_t14_itl_flag_namespace);
	UT_RUN(test_t15_block_ndeltas_offset_0);
	UT_RUN(test_t16_block_reserved_offset_2);
	UT_RUN(test_t17_committed_requires_valid_scn);
	UT_RUN(test_t18_block_allows_zero_ndeltas);
	UT_RUN(test_t19_delta_v2_sizeof_40);
	UT_RUN(test_t20_delta_v2_slot_idx_offset_0);
	UT_RUN(test_t21_delta_v2_flags_after_offset_2);
	UT_RUN(test_t22_delta_v2_xid_offset_4);
	UT_RUN(test_t23_delta_v2_write_scn_offset_8);
	UT_RUN(test_t24_delta_v2_commit_scn_offset_16);
	UT_RUN(test_t25_delta_v2_undo_segment_head_offset_24);
	UT_RUN(test_t26_format_version_v1_v2_distinct);
	UT_RUN(test_t27_block_format_version_repurposed_pad);
	UT_RUN(test_t28_block_deltas_offset_still_8);
	UT_RUN(test_t29_v1_v2_slot_idx_same_offset);
	UT_RUN(test_t30_delta_v3_sizeof_32);
	UT_RUN(test_t31_delta_v3_field_offsets);
	UT_RUN(test_t32_format_version_v3_distinct);
	UT_RUN(test_t33_v1_v2_v3_slot_idx_same_offset);
	UT_DONE();
}
