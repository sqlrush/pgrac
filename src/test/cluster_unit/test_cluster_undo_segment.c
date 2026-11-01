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
#include "cluster/cluster_undo_segment_init.h"

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf and we don't link libpgport in this test binary. */
#undef printf

#include "unit_test.h"


UT_DEFINE_GLOBALS();

extern void test_frontend_undo_header_v1(uint32 segment_id, uint8 owner_instance, char *page);
extern bool test_frontend_undo_header_v2(uint32 segment_id, uint8 owner_instance, char *page);

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(), int lineNumber pg_attribute_unused())
{
	abort();
}


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


/*
 * spec-1.22 D9 extension tests: PD_UNDO_SEG_HEADER bit invariants
 * (declared in storage/bufpage.h alongside spec-1.5 PD_HAS_ITL).
 * These pin the bit value + valid-flag mask, and the disjoint-by-
 * relation-type contract between heap pages (PD_HAS_ITL) and undo
 * segment headers (PD_UNDO_SEG_HEADER).
 */
UT_TEST(test_spec122_pd_undo_seg_header_bit_value)
{
	UT_ASSERT_EQ((unsigned)PD_UNDO_SEG_HEADER, 0x0010u);
}

UT_TEST(test_spec122_pd_valid_flag_bits_bumped)
{
	/* spec-1.22 added 0x0010 (0x000F->0x001F); spec-4.5 added
	 * PD_CLUSTER_FORCE_FPI = 0x0020; spec-8.4D reserves five more bits
	 * for the origin-qualified PageLSN identity; SPACE adds 0x0800. */
	UT_ASSERT_EQ((unsigned)PD_VALID_FLAG_BITS, 0x0FFFu);
}

UT_TEST(test_spec122_pd_has_itl_undo_disjoint)
{
	/* PD_HAS_ITL (heap) and PD_UNDO_SEG_HEADER (undo) are disjoint by
	 * relation type but on different bits so tooling can distinguish
	 * three page kinds: vanilla index / ITL heap / undo seg header. */
	UT_ASSERT_NE(PD_HAS_ITL, PD_UNDO_SEG_HEADER);
	UT_ASSERT_EQ(PD_HAS_ITL & PD_UNDO_SEG_HEADER, 0u);
}


UT_TEST(test_legacy_header_identity_refuses_versioned_tt_stride)
{
	UndoSegmentHeaderData header = { 0 };
	uint8 *bytes = (uint8 *)&header;
	uint16 stride = 40;

	header.pd_flags = PD_UNDO_SEG_HEADER;
	header.pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	header.segment_id = 7;
	header.segment_size_bytes = UNDO_SEGMENT_SIZE_BYTES;
	header.owner_instance = 1;
	header.tt_slots_count = TT_SLOTS_PER_SEGMENT;
	UT_ASSERT(UndoSegmentHeader_identity_matches((char *)&header, 7, 1));
	/* V2 framing lives in the old reserved prefix, before either slot stride. */
	bytes[57] = 2;
	memcpy(bytes + 58, &stride, sizeof(stride));
	UT_ASSERT(!UndoSegmentHeader_identity_matches((char *)&header, 7, 1));
	for (unsigned offset = 57; offset < 64; offset++) {
		memset(bytes + 57, 0, 7);
		bytes[offset] = 1;
		UT_ASSERT(!UndoSegmentHeader_identity_matches((char *)&header, 7, 1));
	}
}

UT_TEST(test_tt_v2_slot_and_block_zero_exact_layout)
{
	struct { char prefix; TTSlotV2 slot; } alignment;

	UT_ASSERT_EQ(sizeof(TTSlotV2), 40);
	UT_ASSERT_EQ(offsetof(TTSlotV2, original_full_xid), 32);
	UT_ASSERT_EQ((char *)&alignment.slot - (char *)&alignment, 8);
	UT_ASSERT_EQ(sizeof(UndoSegmentHeaderDataV2), BLCKSZ);
	UT_ASSERT_EQ(offsetof(UndoSegmentHeaderDataV2, tt_layout_version), 57);
	UT_ASSERT_EQ(offsetof(UndoSegmentHeaderDataV2, tt_slot_bytes), 58);
	UT_ASSERT_EQ(offsetof(UndoSegmentHeaderDataV2, tt_layout_reserved), 60);
	UT_ASSERT_EQ(offsetof(UndoSegmentHeaderDataV2, tt_slots), 112);
	UT_ASSERT_EQ(sizeof(((UndoSegmentHeaderDataV2 *)0)->tt_slots), 1920);
	UT_ASSERT_EQ(offsetof(UndoSegmentHeaderDataV2, tt_next_free_slot), 2032);
	UT_ASSERT_EQ(offsetof(UndoSegmentHeaderDataV2, tt_active_count), 2036);
	UT_ASSERT_EQ(offsetof(UndoSegmentHeaderDataV2, free_block_bitmap), 2040);
	UT_ASSERT_EQ(offsetof(UndoSegmentHeaderDataV2, wrap_count), 3064);
	UT_ASSERT_EQ(offsetof(UndoSegmentHeaderDataV2, _reserved), 3068);
	UT_ASSERT_EQ(sizeof(((UndoSegmentHeaderDataV2 *)0)->_reserved), 5124);
}

UT_TEST(test_tt_v2_fresh_frontend_backend_bytes_match_and_legacy_stays_v1)
{
	UndoSegmentHeaderDataV2 backend, frontend;
	UndoSegmentHeaderData legacy, legacy_frontend;
	const uint8 *bytes = (const uint8 *)&backend;

	memset(&backend, 0xa5, sizeof(backend));
	memset(&frontend, 0x5a, sizeof(frontend));
	UT_ASSERT(cluster_undo_segment_make_header_v2_bytes(7, 1, (char *)&backend));
	UT_ASSERT(test_frontend_undo_header_v2(7, 1, (char *)&frontend));
	UT_ASSERT(memcmp(&backend, &frontend, BLCKSZ) == 0);
	UT_ASSERT(UndoSegmentHeaderV2_identity_matches((char *)&backend, BLCKSZ, 7, 1));
	UT_ASSERT(!UndoSegmentHeader_identity_matches((char *)&backend, 7, 1));
	UT_ASSERT_EQ(backend.tt_layout_version, 2);
	UT_ASSERT_EQ(backend.tt_slot_bytes, 40);
	UT_ASSERT_EQ(backend.tt_layout_reserved, 0);
	for (Size i = 112; i < BLCKSZ; i++)
		UT_ASSERT_EQ(bytes[i], 0);
	for (int i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		UT_ASSERT_EQ(backend.tt_slots[i].status, TT_SLOT_UNUSED);
		UT_ASSERT_EQ(backend.tt_slots[i].original_full_xid, 0);
	}
	cluster_undo_segment_make_header_bytes(7, 1, (char *)&legacy);
	test_frontend_undo_header_v1(7, 1, (char *)&legacy_frontend);
	UT_ASSERT(memcmp(&legacy, &legacy_frontend, BLCKSZ) == 0);
	UT_ASSERT(UndoSegmentHeader_identity_matches((char *)&legacy, 7, 1));
	UT_ASSERT(!UndoSegmentHeaderV2_identity_matches((char *)&legacy, BLCKSZ, 7, 1));
	UT_ASSERT(memcmp(&legacy, &backend, 57) == 0);
	UT_ASSERT(memcmp((char *)&legacy + 64, (char *)&backend + 64, BLCKSZ - 64) == 0);
	UT_ASSERT_EQ(backend.pd_checksum, 0); /* template, not a sealed durable page */
}

UT_TEST(test_tt_v2_constructor_refusal_preserves_output)
{
	UndoSegmentHeaderDataV2 output, before;
	const struct { uint32 segment; uint8 owner; } invalid[] = {
		{ 0, 1 }, { 7, 0 }, { 7, 129 }, { 7, 255 }
	};

	memset(&output, 0xa5, sizeof(output));
	before = output;
	for (Size i = 0; i < lengthof(invalid); i++) {
		UT_ASSERT(!cluster_undo_segment_make_header_v2_bytes(invalid[i].segment,
			invalid[i].owner, (char *)&output));
		UT_ASSERT(memcmp(&output, &before, BLCKSZ) == 0);
		UT_ASSERT(!test_frontend_undo_header_v2(invalid[i].segment,
			invalid[i].owner, (char *)&output));
		UT_ASSERT(memcmp(&output, &before, BLCKSZ) == 0);
	}
	UT_ASSERT(!cluster_undo_segment_make_header_v2_bytes(7, 1, NULL));
	UT_ASSERT(!test_frontend_undo_header_v2(7, 1, NULL));
}

UT_TEST(test_tt_v2_identity_rejects_wrong_prefix_before_slot_stride)
{
	UndoSegmentHeaderDataV2 valid, page;
	const uint8 versions[] = { 0, 1, 3, 255 };
	const uint16 strides[] = { 0, 32, 39, 41, UINT16_MAX };
	const Size lengths[] = { 0, 1, 63, 64, BLCKSZ - 1, BLCKSZ + 1 };
	char short_buffer[1] = { 0 };

	UT_ASSERT(cluster_undo_segment_make_header_v2_bytes(7, 1, (char *)&valid));
	for (Size i = 0; i < lengthof(versions); i++) {
		page = valid;
		page.tt_layout_version = versions[i];
		UT_ASSERT(!UndoSegmentHeaderV2_identity_matches((char *)&page, BLCKSZ, 7, 1));
	}
	for (Size i = 0; i < lengthof(strides); i++) {
		page = valid;
		page.tt_slot_bytes = strides[i];
		UT_ASSERT(!UndoSegmentHeaderV2_identity_matches((char *)&page, BLCKSZ, 7, 1));
	}
	page = valid;
	page.tt_layout_reserved = 1;
	UT_ASSERT(!UndoSegmentHeaderV2_identity_matches((char *)&page, BLCKSZ, 7, 1));
	for (Size i = 0; i < lengthof(lengths); i++)
		UT_ASSERT(!UndoSegmentHeaderV2_identity_matches((char *)&valid, lengths[i], 7, 1));
	UT_ASSERT(!UndoSegmentHeaderV2_identity_matches(short_buffer, sizeof(short_buffer), 7, 1));
	UT_ASSERT(!UndoSegmentHeaderV2_identity_matches(NULL, BLCKSZ, 7, 1));
}

UT_TEST(test_tt_v2_mutable_identity_uses_new_management_offsets)
{
	UndoSegmentHeaderDataV2 page, valid;
	const uint8 states[] = { SEGMENT_ALLOCATED, SEGMENT_ACTIVE,
		SEGMENT_COMMITTED, SEGMENT_RECYCLABLE };
	const uint64 full = (UINT64_C(7) << 32) | 1234;

	UT_ASSERT(cluster_undo_segment_make_header_v2_bytes(7, 1, (char *)&valid));
	valid.tt_next_free_slot = TT_SLOTS_PER_SEGMENT;
	valid.tt_active_count = 1;
	valid.wrap_count = 9;
	valid.free_block_bitmap[1] = 0xff;
	valid.tt_slots[47].xid = 1234;
	valid.tt_slots[47].status = TT_SLOT_ACTIVE;
	valid.tt_slots[47].original_full_xid = full;
	for (Size i = 0; i < lengthof(states); i++) {
		valid.segment_state = states[i];
		UT_ASSERT(UndoSegmentHeaderV2_identity_matches((char *)&valid, BLCKSZ, 7, 1));
		UT_ASSERT_EQ(valid.tt_slots[47].original_full_xid, full);
	}
	page = valid;
	page.tt_next_free_slot++;
	UT_ASSERT(!UndoSegmentHeaderV2_identity_matches((char *)&page, BLCKSZ, 7, 1));
	page = valid;
	page.tt_active_count = TT_SLOTS_PER_SEGMENT + 1;
	UT_ASSERT(!UndoSegmentHeaderV2_identity_matches((char *)&page, BLCKSZ, 7, 1));
	page = valid;
	page.segment_state = SEGMENT_INVALID;
	UT_ASSERT(!UndoSegmentHeaderV2_identity_matches((char *)&page, BLCKSZ, 7, 1));
	page = valid;
	page.segment_size_bytes--;
	UT_ASSERT(!UndoSegmentHeaderV2_identity_matches((char *)&page, BLCKSZ, 7, 1));
	page = valid;
	page.tt_slots_count--;
	UT_ASSERT(!UndoSegmentHeaderV2_identity_matches((char *)&page, BLCKSZ, 7, 1));
	page = valid;
	page.pd_flags &= ~PD_UNDO_SEG_HEADER;
	UT_ASSERT(!UndoSegmentHeaderV2_identity_matches((char *)&page, BLCKSZ, 7, 1));
	UT_ASSERT(!UndoSegmentHeaderV2_identity_matches((char *)&valid, BLCKSZ, 8, 1));
	UT_ASSERT(!UndoSegmentHeaderV2_identity_matches((char *)&valid, BLCKSZ, 7, 2));
	UT_ASSERT(!UndoSegmentHeaderV2_identity_matches((char *)&valid, BLCKSZ, 0, 1));
	UT_ASSERT(!UndoSegmentHeaderV2_identity_matches((char *)&valid, BLCKSZ, 7, 0));
}

int
main(void)
{
	UT_PLAN(27);

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

	/* spec-1.22 D9 extension: PD_UNDO_SEG_HEADER bit invariants (3) */
	UT_RUN(test_spec122_pd_undo_seg_header_bit_value);
	UT_RUN(test_spec122_pd_valid_flag_bits_bumped);
	UT_RUN(test_spec122_pd_has_itl_undo_disjoint);
	UT_RUN(test_legacy_header_identity_refuses_versioned_tt_stride);
	UT_RUN(test_tt_v2_slot_and_block_zero_exact_layout);
	UT_RUN(test_tt_v2_fresh_frontend_backend_bytes_match_and_legacy_stays_v1);
	UT_RUN(test_tt_v2_constructor_refusal_preserves_output);
	UT_RUN(test_tt_v2_identity_rejects_wrong_prefix_before_slot_stride);
	UT_RUN(test_tt_v2_mutable_identity_uses_new_management_offsets);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
