/*-------------------------------------------------------------------------
 *
 * test_cluster_itl_slot.c
 *	  Compile-time + link-time invariants for the ITL slot stub
 *	  introduced at stage 1.5.
 *
 *	  Stage 1.5 ships only the ClusterItlSlotData typedef + UBA stub
 *	  + flags enum + constants in cluster_itl_slot.h.  The actual
 *	  state machine (FREE -> ACTIVE -> COMMITTED -> NEEDS_CLEANOUT
 *	  -> 复用) lands at Stage 3 (AD-006 第五轮).  This binary covers
 *	  only the layout / typedef / constant invariants.
 *
 *	  Spec: spec-1.5-itl-slot.md §1.2 Deliverable 6 + §4.1 (8 项 cluster_unit 断言)
 *	  Design: docs/block-format-design.md v1.2 §4.3
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_itl_slot.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Each test_*.c is a standalone executable; see unit_test.h.
 *	  cluster_itl_slot.h is header-only at stage 1.5 -- no .o file
 *	  to link.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h> /* offsetof */

#include "cluster/cluster_itl_slot.h"

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf and we don't link libpgport in this test binary. */
#undef printf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


UT_TEST(test_itl_slot_size_is_48_bytes)
{
	/*
	 * sizeof(ClusterItlSlotData) MUST be 48.  This is the cornerstone
	 * invariant of stage 1.5; PageInitHeapPage allocates 8 × 48 = 384
	 * bytes after the PageHeader.
	 */
	UT_ASSERT_EQ((int)sizeof(ClusterItlSlotData), 48);
}


UT_TEST(test_itl_constants_match_spec_1_5)
{
	/*
	 * Lock the three constants used everywhere in the codebase.
	 * Any change requires a spec amendment + catversion bump.
	 */
	UT_ASSERT_EQ(CLUSTER_ITL_SLOT_SIZE, 48);
	UT_ASSERT_EQ(CLUSTER_ITL_INITRANS_DEFAULT, 8);
	UT_ASSERT_EQ(CLUSTER_ITL_ARRAY_SIZE, 384);
}


UT_TEST(test_itl_slot_unallocated_sentinel_is_255)
{
	/*
	 * t_itl_slot_idx default = 255 = "no slot assigned".  Stage 3
	 * populates real 0..7 indexes; stage 1.5 always writes 255.
	 */
	UT_ASSERT_EQ(CLUSTER_ITL_SLOT_UNALLOCATED, 255);
}


UT_TEST(test_itl_flag_free_zero_init)
{
	/*
	 * ITL_FLAG_FREE MUST be 0 so MemSet zero-fill in PageInit puts
	 * every slot into the FREE state without explicit assignment.
	 * (PageInitHeapPage still writes explicitly per spec-1.5 §8 Q6,
	 * but the zero-init invariant is what makes incremental Stage 3
	 * work safe.)
	 */
	UT_ASSERT_EQ((int)ITL_FLAG_FREE, 0);
}


UT_TEST(test_itl_field_offsets_locked)
{
	/*
	 * Field offsets MUST stay stable; pg_cluster_state.block_format
	 * surfaces the layout to DBAs and any drift would silently break
	 * pageinspect output + Stage 3 write algorithms.
	 */
	UT_ASSERT_EQ((int)offsetof(ClusterItlSlotData, xid), 0);
	UT_ASSERT_EQ((int)offsetof(ClusterItlSlotData, wrap), 4);
	UT_ASSERT_EQ((int)offsetof(ClusterItlSlotData, flags), 6);
	UT_ASSERT_EQ((int)offsetof(ClusterItlSlotData, lock_count), 7);
	UT_ASSERT_EQ((int)offsetof(ClusterItlSlotData, undo_segment_head), 8);
	UT_ASSERT_EQ((int)offsetof(ClusterItlSlotData, commit_scn), 24);
	UT_ASSERT_EQ((int)offsetof(ClusterItlSlotData, write_scn), 32);
	UT_ASSERT_EQ((int)offsetof(ClusterItlSlotData, first_change_lsn), 40);
}


UT_TEST(test_uba_stub_size_is_16_bytes)
{
	/*
	 * UBA is a 16-byte stub at stage 1.5; encoding (segment_id, block,
	 * slot, row) lands at spec-1.20-1.22.  The 16-byte width is
	 * locked here so the on-disk ITL slot format never changes when
	 * the encoding lands.
	 */
	UT_ASSERT_EQ((int)sizeof(UBA), 16);
}


UT_TEST(test_uba_zero_init_is_invalid)
{
	/*
	 * UBA all-zero == InvalidUba (occupies "未分配" semantics by
	 * MemSet).  This invariant is what PageInitHeapPage's loop body
	 * relies on for the placeholder write strategy.
	 */
	UBA u = { { 0, 0 } };

	UT_ASSERT(UBA_is_invalid(u));
}


UT_TEST(test_uba_nonzero_is_valid)
{
	/*
	 * Any non-zero UBA must NOT be invalid -- this catches future
	 * regressions where InvalidUba detection accidentally widens.
	 */
	UBA u1 = { { 1, 0 } };
	UBA u2 = { { 0, 1 } };

	UT_ASSERT_EQ((int)UBA_is_invalid(u1), 0);
	UT_ASSERT_EQ((int)UBA_is_invalid(u2), 0);
}


int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_itl_slot_size_is_48_bytes);
	UT_RUN(test_itl_constants_match_spec_1_5);
	UT_RUN(test_itl_slot_unallocated_sentinel_is_255);
	UT_RUN(test_itl_flag_free_zero_init);
	UT_RUN(test_itl_field_offsets_locked);
	UT_RUN(test_uba_stub_size_is_16_bytes);
	UT_RUN(test_uba_zero_init_is_invalid);
	UT_RUN(test_uba_nonzero_is_valid);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
