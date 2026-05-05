/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_segment.c
 *	  Compile-time + link-time invariants for the spec-1.21 Undo
 *	  Segment header type definition.
 *
 *	  These invariants guard the on-disk byte layout at the
 *	  cluster_unit layer (no PG postmaster needed) so any future
 *	  UndoSegmentHeaderData field reorder / unintended struct layout
 *	  change is caught before the bigger cluster_tap suite is exercised.
 *
 *	  Spec: spec-1.21-undo-segment-header.md §4.1
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_undo_segment.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Includes "postgres.h" so transitive PG-internal types resolve.
 *	  PG headers are not actually called at runtime -- this binary
 *	  only reads sizeof / offsetof at compile time and exercises the
 *	  inline helpers from cluster_undo_segment.h.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h> /* offsetof */
#include <string.h> /* memset */

#include "cluster/cluster_undo_segment.h"

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf and we don't link libpgport in this test binary. */
#undef printf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


/*
 * On-disk layout invariants -- duplicate the StaticAssertDecl block
 * from cluster_undo_segment.h at the test layer so a CI run surfaces
 * the failure in cluster_unit faster than waiting for a full backend
 * rebuild.
 */
UT_TEST(test_spec121_segheader_size_is_8192_bytes)
{
	UT_ASSERT_EQ(sizeof(UndoSegmentHeaderData), (size_t)8192);
}

UT_TEST(test_spec121_segheader_segment_id_offset_is_32)
{
	UT_ASSERT_EQ(offsetof(UndoSegmentHeaderData, segment_id), (size_t)32);
}

UT_TEST(test_spec121_segheader_segment_state_offset_is_40)
{
	UT_ASSERT_EQ(offsetof(UndoSegmentHeaderData, segment_state), (size_t)40);
}

UT_TEST(test_spec121_segheader_owner_instance_offset_is_41)
{
	UT_ASSERT_EQ(offsetof(UndoSegmentHeaderData, owner_instance), (size_t)41);
}

UT_TEST(test_spec121_segheader_tt_slots_offset_is_112)
{
	/*
	 * spec-1.21 v0.2 body table said 108, but C natural alignment
	 * forces SCN (uint64) fields to 8-byte boundaries.  After
	 * segment_flags lands at offset 56, _pad_57[7] pushes the next
	 * SCN to 64; tt_slots therefore lands at 112 (not 108).  Hardening
	 * v1.0.1 amendment to spec-1.21 captures the corrected offsets.
	 */
	UT_ASSERT_EQ(offsetof(UndoSegmentHeaderData, tt_slots), (size_t)112);
}

UT_TEST(test_spec121_segheader_tt_slots_size_is_1536_bytes)
{
	UndoSegmentHeaderData hdr;

	UT_ASSERT_EQ(sizeof(hdr.tt_slots), (size_t)1536);
}

UT_TEST(test_spec121_segheader_tt_counters_offset_is_1648)
{
	UT_ASSERT_EQ(offsetof(UndoSegmentHeaderData, tt_next_free_slot), (size_t)1648);
	UT_ASSERT_EQ(offsetof(UndoSegmentHeaderData, tt_active_count), (size_t)1652);
}

UT_TEST(test_spec121_segheader_free_bitmap_offset_is_1656)
{
	UT_ASSERT_EQ(offsetof(UndoSegmentHeaderData, free_block_bitmap), (size_t)1656);
}

UT_TEST(test_spec121_segheader_free_bitmap_size_is_1024_bytes)
{
	UndoSegmentHeaderData hdr;

	UT_ASSERT_EQ(sizeof(hdr.free_block_bitmap), (size_t)1024);
}


/*
 * Sentinel constants invariant.  Stage 2+ extensions MUST NOT change
 * these values.
 */
UT_TEST(test_spec121_state_enum_values)
{
	UT_ASSERT_EQ((unsigned)SEGMENT_ALLOCATED, 0u);
	UT_ASSERT_EQ((unsigned)SEGMENT_ACTIVE, 1u);
	UT_ASSERT_EQ((unsigned)SEGMENT_COMMITTED, 2u);
	UT_ASSERT_EQ((unsigned)SEGMENT_RECYCLABLE, 3u);
	UT_ASSERT_EQ((unsigned)SEGMENT_INVALID, 0xFFu);
}

UT_TEST(test_spec121_sentinel_constants)
{
	UT_ASSERT_EQ(UNDO_SEGMENT_SIZE_BYTES, (size_t)(64 * 1024 * 1024));
	UT_ASSERT_EQ(UNDO_SEGMENT_HEADER_SIZE, 8192);
	UT_ASSERT_EQ(UNDO_BLOCKS_PER_SEGMENT, 8192);
	UT_ASSERT_EQ(UNDO_FREE_BITMAP_BYTES, 1024);
	UT_ASSERT_EQ((unsigned)UNDO_OWNER_INSTANCE_INVALID, 0u);
	UT_ASSERT_EQ((unsigned)UNDO_OWNER_INSTANCE_MAX, 128u);
	UT_ASSERT_EQ((unsigned)UNDO_SEGMENT_FLAGS_RESERVED, 0u);
}


/*
 * Inline helpers -- four-quadrant coverage on is_available +
 * is_active across the five real status values.
 */
UT_TEST(test_spec121_helper_is_available_zero_init_passes)
{
	UndoSegmentHeaderData hdr;

	memset(&hdr, 0, sizeof(hdr));
	/* Zero-init lands at SEGMENT_ALLOCATED == 0; allocator may bind. */
	UT_ASSERT(UndoSegmentHeader_is_available(&hdr));
	UT_ASSERT(!UndoSegmentHeader_is_active(&hdr));
}

UT_TEST(test_spec121_helper_is_available_recyclable_passes)
{
	UndoSegmentHeaderData hdr;

	memset(&hdr, 0, sizeof(hdr));
	hdr.segment_state = SEGMENT_RECYCLABLE;
	UT_ASSERT(UndoSegmentHeader_is_available(&hdr));
	UT_ASSERT(!UndoSegmentHeader_is_active(&hdr));
}

UT_TEST(test_spec121_helper_is_available_active_fails)
{
	UndoSegmentHeaderData hdr;

	memset(&hdr, 0, sizeof(hdr));
	hdr.segment_state = SEGMENT_ACTIVE;
	UT_ASSERT(!UndoSegmentHeader_is_available(&hdr));
	UT_ASSERT(UndoSegmentHeader_is_active(&hdr));
}

UT_TEST(test_spec121_helper_is_active_only_active_passes)
{
	UndoSegmentHeaderData hdr;

	memset(&hdr, 0, sizeof(hdr));

	hdr.segment_state = SEGMENT_ALLOCATED;
	UT_ASSERT(!UndoSegmentHeader_is_active(&hdr));

	hdr.segment_state = SEGMENT_ACTIVE;
	UT_ASSERT(UndoSegmentHeader_is_active(&hdr));

	hdr.segment_state = SEGMENT_COMMITTED;
	UT_ASSERT(!UndoSegmentHeader_is_active(&hdr));

	hdr.segment_state = SEGMENT_RECYCLABLE;
	UT_ASSERT(!UndoSegmentHeader_is_active(&hdr));
}

UT_TEST(test_spec121_helper_owner_zero_init_returns_invalid)
{
	UndoSegmentHeaderData hdr;

	memset(&hdr, 0, sizeof(hdr));
	UT_ASSERT_EQ((unsigned)UndoSegmentHeader_owner(&hdr), (unsigned)UNDO_OWNER_INSTANCE_INVALID);
}


/*
 * Zero-init consistency: a memset(0) UndoSegmentHeaderData lands all
 * placeholder fields at their permanent sentinel values.  This is the
 * single most important invariant for safe segment header creation
 * (whether by buffer pool memset or by future PageInitUndoSegmentHeader
 * in spec-1.22).
 */
UT_TEST(test_spec121_zero_init_consistency)
{
	UndoSegmentHeaderData hdr;
	int i;

	memset(&hdr, 0, sizeof(hdr));

	/* Segment state at SEGMENT_ALLOCATED (placeholder). */
	UT_ASSERT_EQ((unsigned)hdr.segment_state, (unsigned)SEGMENT_ALLOCATED);

	/* Owner unallocated. */
	UT_ASSERT_EQ((unsigned)hdr.owner_instance, (unsigned)UNDO_OWNER_INSTANCE_INVALID);

	/* All TT slots in TT_SLOT_UNUSED state (spec-1.20 invariant). */
	for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++)
		UT_ASSERT_EQ((unsigned)hdr.tt_slots[i].status, (unsigned)TT_SLOT_UNUSED);

	/* Counters zero. */
	UT_ASSERT_EQ(hdr.tt_next_free_slot, 0u);
	UT_ASSERT_EQ(hdr.tt_active_count, 0u);

	/* Bitmap fully zero (no blocks allocated). */
	for (i = 0; i < UNDO_FREE_BITMAP_BYTES; i++)
		UT_ASSERT_EQ((unsigned)hdr.free_block_bitmap[i], 0u);
}


/*
 * TT slot dependency on spec-1.20: a zero-init UndoSegmentHeaderData's
 * tt_slots[0] must pass spec-1.20's TTSlot_is_unused() helper.
 * Catches accidental TT slot ABI change at spec-1.20 layer that would
 * silently break the TTSlot zero-init contract.
 */
UT_TEST(test_spec121_tt_slot_dependency_zero_init)
{
	UndoSegmentHeaderData hdr;

	memset(&hdr, 0, sizeof(hdr));
	UT_ASSERT(TTSlot_is_unused(&hdr.tt_slots[0]));
	UT_ASSERT(TTSlot_is_unused(&hdr.tt_slots[TT_SLOTS_PER_SEGMENT - 1]));
}


int
main(void)
{
	UT_PLAN(18);

	/* Layout invariants (9) */
	UT_RUN(test_spec121_segheader_size_is_8192_bytes);
	UT_RUN(test_spec121_segheader_segment_id_offset_is_32);
	UT_RUN(test_spec121_segheader_segment_state_offset_is_40);
	UT_RUN(test_spec121_segheader_owner_instance_offset_is_41);
	UT_RUN(test_spec121_segheader_tt_slots_offset_is_112);
	UT_RUN(test_spec121_segheader_tt_slots_size_is_1536_bytes);
	UT_RUN(test_spec121_segheader_tt_counters_offset_is_1648);
	UT_RUN(test_spec121_segheader_free_bitmap_offset_is_1656);
	UT_RUN(test_spec121_segheader_free_bitmap_size_is_1024_bytes);

	/* Sentinel + enum constants (2) */
	UT_RUN(test_spec121_state_enum_values);
	UT_RUN(test_spec121_sentinel_constants);

	/* Inline helpers, four-quadrant coverage (5) */
	UT_RUN(test_spec121_helper_is_available_zero_init_passes);
	UT_RUN(test_spec121_helper_is_available_recyclable_passes);
	UT_RUN(test_spec121_helper_is_available_active_fails);
	UT_RUN(test_spec121_helper_is_active_only_active_passes);
	UT_RUN(test_spec121_helper_owner_zero_init_returns_invalid);

	/* Zero-init consistency (1) */
	UT_RUN(test_spec121_zero_init_consistency);

	/* TT slot dependency (1) */
	UT_RUN(test_spec121_tt_slot_dependency_zero_init);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
