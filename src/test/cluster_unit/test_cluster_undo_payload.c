/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_payload.c
 *	  pgrac spec-3.27 D1 / §2.5 — payload-relative undo format macro unit
 *	  tests (Q12-A: undo block gains a standard PG PageHeader at offset 0).
 *
 *	  Verifies the payload-relative addressing that replaces the legacy
 *	  full-BLCKSZ macros once a PageHeader is prepended, so that no slot or
 *	  record write escapes the physical page (P1-1 from r4 review):
 *	    P1  UNDO_PAGE_HEADER_BYTES == SizeOfPageHeaderData
 *	    P2  UNDO_PAYLOAD_BYTES == BLCKSZ - SizeOfPageHeaderData
 *	    P3  cluster_undo_page_get_payload = page + header bytes
 *	    P4  slot 0 pointer lands strictly inside [payload, page+BLCKSZ)
 *	    P5  slot N offsets are monotone and never escape the page tail
 *	    P6  payload has_space boundary matches the payload block size
 *	    P7  a max-index slot at a full payload still fits below BLCKSZ
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_undo_payload.c
 *
 * NOTES
 *	  pgrac-original file.
 *	  Spec: spec-3.27-undo-buffer-backed-model.md (FROZEN v1.0, §2.5)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "cluster/cluster_undo_format.h"

/* Un-remap the port.h printf family so this standalone test links against the
 * C library (no libpgport), matching the other simple cluster_unit tests. */
#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* A physical page + its Q12-A payload base for the boundary tests. */
static char page_buf[BLCKSZ];

static inline char *
payload_base(void)
{
	return (char *)cluster_undo_page_get_payload((Page)page_buf);
}

UT_TEST(test_p1_header_bytes_eq_pageheader)
{
	UT_ASSERT_EQ(UNDO_PAGE_HEADER_BYTES, (uint32)SizeOfPageHeaderData);
}

UT_TEST(test_p2_payload_bytes)
{
	UT_ASSERT_EQ(UNDO_PAYLOAD_BYTES, (uint32)(BLCKSZ - SizeOfPageHeaderData));
	/* Records + slots live below the UndoBlockHeader inside the payload. */
	UT_ASSERT_EQ(UNDO_PAYLOAD_BLOCK_BYTES, UNDO_PAYLOAD_BYTES - (uint32)sizeof(UndoBlockHeader));
}

UT_TEST(test_p3_get_payload_offset)
{
	char *pl = payload_base();

	/* Payload base is exactly one PageHeader past the physical page start. */
	UT_ASSERT_EQ((uint32)(pl - page_buf), UNDO_PAGE_HEADER_BYTES);
}

UT_TEST(test_p4_slot0_inside_page)
{
	char *pl = payload_base();
	char *slot0 = (char *)UNDO_PAYLOAD_SLOT_DIR_PTR(pl, 0);
	char *page_end = page_buf + BLCKSZ;

	/* slot 0 must be at/after the payload base and its 8 bytes must end at
	 * exactly the page tail — never one byte past it (the P1-1 overflow). */
	UT_ASSERT(slot0 >= pl);
	UT_ASSERT(slot0 + sizeof(UndoSlotDirEntry) <= page_end);
	UT_ASSERT_EQ((uint32)(page_end - slot0), (uint32)sizeof(UndoSlotDirEntry));
}

UT_TEST(test_p5_slotN_monotone_in_bounds)
{
	char *pl = payload_base();
	char *page_end = page_buf + BLCKSZ;
	uint32 prev_off = UNDO_PAYLOAD_BYTES; /* strictly-decreasing bound */
	int i;

	/* Walk a healthy span of slot indices;  each must sit below the previous
	 * and its full 8 bytes must remain inside the physical page. */
	for (i = 0; i < 512; i++) {
		uint32 off = UNDO_PAYLOAD_SLOT_DIR_OFFSET(i);
		char *p = (char *)UNDO_PAYLOAD_SLOT_DIR_PTR(pl, i);

		UT_ASSERT(off < prev_off);
		UT_ASSERT(p >= pl);
		UT_ASSERT(p + sizeof(UndoSlotDirEntry) <= page_end);
		prev_off = off;
	}
}

UT_TEST(test_p6_has_space_boundary)
{
	/* An empty payload can hold a record of at most (payload block bytes)
	 * minus the first slot entry;  one byte more must be rejected. */
	uint32 free0 = (uint32)sizeof(UndoBlockHeader);
	uint32 max_rec = UNDO_PAYLOAD_BYTES - free0 - (uint32)sizeof(UndoSlotDirEntry);

	UT_ASSERT(cluster_undo_payload_has_space(free0, 0, (uint16)max_rec));
	UT_ASSERT(!cluster_undo_payload_has_space(free0, 0, (uint16)(max_rec + 1)));
}

UT_TEST(test_p7_max_slot_index_fits)
{
	/* The largest slot index the payload can address (one slot per 8 bytes
	 * of the block area) must still resolve inside the page. */
	uint32 max_slots = UNDO_PAYLOAD_BLOCK_BYTES / (uint32)sizeof(UndoSlotDirEntry);
	char *pl = payload_base();
	char *page_end = page_buf + BLCKSZ;
	char *last = (char *)UNDO_PAYLOAD_SLOT_DIR_PTR(pl, max_slots - 1);

	UT_ASSERT(last >= pl + sizeof(UndoBlockHeader));
	UT_ASSERT(last + sizeof(UndoSlotDirEntry) <= page_end);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_p1_header_bytes_eq_pageheader);
	UT_RUN(test_p2_payload_bytes);
	UT_RUN(test_p3_get_payload_offset);
	UT_RUN(test_p4_slot0_inside_page);
	UT_RUN(test_p5_slotN_monotone_in_bounds);
	UT_RUN(test_p6_has_space_boundary);
	UT_RUN(test_p7_max_slot_index_fits);
	UT_DONE();
	return 0;
}
