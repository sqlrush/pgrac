/*-------------------------------------------------------------------------
 *
 * test_cluster_block_format.c
 *	  Compile-time + link-time invariants for the stage-1.4 block format
 *	  changes (PageHeaderData +8B pd_block_scn, PG_PAGE_LAYOUT_VERSION
 *	  4 -> 5).
 *
 *	  These invariants guard the binary format at the cluster_unit layer
 *	  (no PG postmaster needed) so that any future struct field reorder
 *	  / unintended PageHeader layout change is caught before the bigger
 *	  cluster_tap suite is exercised.
 *
 *	  Spec: spec-1.4-block-format-pageheader-scn.md §1.2 Deliverable 5
 *	        + §4.1 (6 项 cluster_unit 断言)
 *	  Design: docs/block-format-design.md v1.1 §4.2
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_block_format.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Includes "postgres.h" because PG's PageHeaderData transitively
 *	  drags in PG-internal types (TransactionId, LocationIndex, etc.);
 *	  postgres.h is the canonical entry point that defines them.  PG
 *	  headers are not actually called at runtime -- this binary only
 *	  reads sizeof / offsetof at compile time.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h> /* offsetof */

#include "storage/bufpage.h"
#include "cluster/cluster_scn.h"

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf and we don't link libpgport in this test binary. */
#undef printf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


UT_TEST(test_page_header_total_size_is_32_bytes)
{
	/*
	 * SizeOfPageHeaderData = offsetof(PageHeaderData, pd_linp).  With
	 * pd_block_scn (8 bytes) inserted after pd_prune_xid, the offset
	 * grows from 24 (PG vanilla) to 32.  This is the cornerstone
	 * invariant of stage 1.4; pageinspect output, pd_lower starts, and
	 * binary compatibility checks all key off this value.
	 */
	UT_ASSERT_EQ((int)SizeOfPageHeaderData, 32);
}


UT_TEST(test_pd_block_scn_offset_is_24)
{
	/*
	 * pd_block_scn must sit immediately after pd_prune_xid (offset 20,
	 * 4 bytes wide -> next offset 24).  Verify via offsetof so a future
	 * reorder of fields (very unlikely but possible during refactor)
	 * is caught at compile time.
	 */
	UT_ASSERT_EQ((int)offsetof(PageHeaderData, pd_block_scn), 24);
}


UT_TEST(test_pd_block_scn_field_is_8_bytes)
{
	/*
	 * sizeof(pd_block_scn) == sizeof(SCN) == 8 by construction (since
	 * cluster_scn.h locks SCN = uint64).  Both sizes must agree;
	 * stating them separately catches any header-only typedef drift.
	 */
	PageHeaderData ph = { 0 };

	UT_ASSERT_EQ((int)sizeof(ph.pd_block_scn), 8);
	UT_ASSERT_EQ((int)sizeof(SCN), 8);
}


UT_TEST(test_pg_page_layout_version_is_5)
{
	/*
	 * spec-1.4 §8 Q3 = A bumps PG_PAGE_LAYOUT_VERSION 4 -> 5 inside
	 * the USE_PGRAC_CLUSTER guard.  The stored 16-bit
	 * pd_pagesize_version field is BLCKSZ | version, so PG's existing
	 * sanity check refuses vanilla PG 16 datafiles (version 4).
	 */
	UT_ASSERT_EQ((int)PG_PAGE_LAYOUT_VERSION, 5);
}


UT_TEST(test_invalid_scn_zero_init_is_natural)
{
	/*
	 * Zero-init via {0} must produce an InvalidScn-valued field.
	 * This invariant is what PageInit's MemSet relies on for the
	 * placeholder write strategy (spec-1.4 §8 Q4 = B).
	 */
	PageHeaderData ph = { 0 };

	UT_ASSERT_EQ((int)ph.pd_block_scn, (int)InvalidScn);
	UT_ASSERT_EQ((int)SCN_VALID(ph.pd_block_scn), 0);
}


UT_TEST(test_field_layout_below_pd_block_scn_unchanged)
{
	/*
	 * The PG-original fields (offset 0-23) MUST keep their byte
	 * positions to stay binary-compatible with PG-internal macros
	 * (PageGetLSN / PageGetChecksum / PageGetPageSize / etc.).
	 * Catching any drift here means any future struct reorder breaks
	 * the test at compile time.
	 */
	UT_ASSERT_EQ((int)offsetof(PageHeaderData, pd_lsn), 0);
	UT_ASSERT_EQ((int)offsetof(PageHeaderData, pd_checksum), 8);
	UT_ASSERT_EQ((int)offsetof(PageHeaderData, pd_flags), 10);
	UT_ASSERT_EQ((int)offsetof(PageHeaderData, pd_lower), 12);
	UT_ASSERT_EQ((int)offsetof(PageHeaderData, pd_upper), 14);
	UT_ASSERT_EQ((int)offsetof(PageHeaderData, pd_special), 16);
	UT_ASSERT_EQ((int)offsetof(PageHeaderData, pd_pagesize_version), 18);
	UT_ASSERT_EQ((int)offsetof(PageHeaderData, pd_prune_xid), 20);
}


/* ============================================================
 * Stage 1.5 invariants -- ITL slot array + tuple header +1B.
 * ============================================================ */

#include "access/htup_details.h"


UT_TEST(test_pd_has_itl_bit_value)
{
	/*
	 * PD_HAS_ITL = 0x0008 carved out of pd_flags reserved space
	 * (PG vanilla used 0x0001-0x0004).  PD_VALID_FLAG_BITS extended
	 * to 0x000F to acknowledge the new bit.
	 */
	UT_ASSERT_EQ(PD_HAS_ITL, 0x0008);
	UT_ASSERT_EQ(PD_VALID_FLAG_BITS, 0x000F);
}


UT_TEST(test_heap_tuple_header_size_is_24)
{
	/*
	 * SizeofHeapTupleHeader = offsetof(HeapTupleHeaderData, t_bits).
	 * pgrac inserts t_itl_slot_idx after t_hoff so this grows from
	 * 23 (PG vanilla) to 24.  Q7 A+B audit verifies no PG path
	 * hardcodes the old 23 value (see docs/spec-1.5-tuple-header-audit.md).
	 */
	UT_ASSERT_EQ((int)SizeofHeapTupleHeader, 24);
}


UT_TEST(test_t_itl_slot_idx_offset_is_23)
{
	/*
	 * t_itl_slot_idx must sit immediately after t_hoff (offset 22 +
	 * 1 byte = 23).  heap_form_tuple writes this byte explicitly to
	 * CLUSTER_ITL_SLOT_UNALLOCATED (255) per spec-1.5 §8 Q3.
	 */
	UT_ASSERT_EQ((int)offsetof(HeapTupleHeaderData, t_itl_slot_idx), 23);
}


UT_TEST(test_minimal_tuple_layout_matches_heap)
{
	/*
	 * MinimalTupleData MUST have t_itl_slot_idx at the same relative
	 * offset to t_hoff as HeapTupleHeaderData, so heap_to_minimal_tuple
	 * byte-copy preserves the placeholder value.  Both should have
	 * t_bits at offsetof t_hoff + 1.
	 */
	UT_ASSERT_EQ(
		(int)(offsetof(MinimalTupleData, t_bits) - offsetof(MinimalTupleData, t_hoff)),
		(int)(offsetof(HeapTupleHeaderData, t_bits) - offsetof(HeapTupleHeaderData, t_hoff)));
}


int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_page_header_total_size_is_32_bytes);
	UT_RUN(test_pd_block_scn_offset_is_24);
	UT_RUN(test_pd_block_scn_field_is_8_bytes);
	UT_RUN(test_pg_page_layout_version_is_5);
	UT_RUN(test_invalid_scn_zero_init_is_natural);
	UT_RUN(test_field_layout_below_pd_block_scn_unchanged);
	UT_RUN(test_pd_has_itl_bit_value);
	UT_RUN(test_heap_tuple_header_size_is_24);
	UT_RUN(test_t_itl_slot_idx_offset_is_23);
	UT_RUN(test_minimal_tuple_layout_matches_heap);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
