/*-------------------------------------------------------------------------
 *
 * test_cluster_cr.c
 *	  pgrac spec-3.9 D10 — cluster_unit tests for own-instance CR block
 *	  construction.
 *
 *	  Scope (what is pure-testable standalone):
 *	    - ABI / SQLSTATE encoding (53R9F / 53R9G)
 *	    - the 4 inverse-apply helpers (cluster_cr_apply.c): real mutations on
 *	      a synthetic 8 KB heap page + their fail-closed (false) paths
 *
 *	  The chain walker driver, tuple remap, the tuple-level visibility
 *	  helpers and the 3-tier MVCC gate live in cluster_cr.c, which depends on
 *	  shmem / undo reader / injection / a live backend; their behavior is
 *	  covered by cluster_tap t/215 (L1-L8) rather than this standalone
 *	  harness (spec-3.9 §4.1 NOTE).
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.9-own-instance-cr-block-construction.md (FROZEN v0.4)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <string.h>

#include "access/htup_details.h"
#include "storage/bufpage.h"
#include "storage/itemid.h"
#include "utils/errcodes.h"

#include "cluster/cluster_cr_apply.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_undo_record.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/* ============================================================
 *	Synthetic 8 KB heap page with an ITL special area + one tuple
 * ============================================================ */

static char synthetic_page[BLCKSZ];

#define TEST_TUPLE_OFFSET FirstOffsetNumber
#define TEST_TUPLE_LEN 64
#define TEST_TUPLE_DATA_OFF (BLCKSZ - CLUSTER_ITL_ARRAY_SIZE - TEST_TUPLE_LEN)

/*
 * Build a page with PD_HAS_ITL, the 8-slot ITL array at the tail, and one
 * LP_NORMAL line pointer at TEST_TUPLE_OFFSET pointing at a TEST_TUPLE_LEN
 * tuple just before the special area.  Returns the page.
 */
static Page
build_page_with_tuple(void)
{
	PageHeader hdr;
	ItemId itemid;
	HeapTupleHeader htup;

	memset(synthetic_page, 0, BLCKSZ);
	hdr = (PageHeader)synthetic_page;
	hdr->pd_flags = PD_HAS_ITL;
	hdr->pd_special = (LocationIndex)(BLCKSZ - CLUSTER_ITL_ARRAY_SIZE);
	hdr->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	hdr->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	hdr->pd_upper = (LocationIndex)TEST_TUPLE_DATA_OFF;

	itemid = PageGetItemId((Page)synthetic_page, TEST_TUPLE_OFFSET);
	ItemIdSetNormal(itemid, TEST_TUPLE_DATA_OFF, TEST_TUPLE_LEN);

	/* Seed the tuple header so xmax/infomask have known values. */
	htup = (HeapTupleHeader)(synthetic_page + TEST_TUPLE_DATA_OFF);
	htup->t_infomask = HEAP_XMAX_INVALID;
	htup->t_infomask2 = 0;

	return (Page)synthetic_page;
}

static HeapTupleHeader
tuple_at(Page page, OffsetNumber off)
{
	return (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, off));
}


/* ============================================================
 *	ABI / SQLSTATE encoding
 * ============================================================ */

UT_TEST(test_sqlstate_53R9F)
{
	UT_ASSERT_EQ(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD, MAKE_SQLSTATE('5', '3', 'R', '9', 'F'));
}

UT_TEST(test_sqlstate_53R9G)
{
	UT_ASSERT_EQ(ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED,
				 MAKE_SQLSTATE('5', '3', 'R', '9', 'G'));
}

UT_TEST(test_data_corrupted_is_XX001)
{
	UT_ASSERT_EQ(ERRCODE_DATA_CORRUPTED, MAKE_SQLSTATE('X', 'X', '0', '0', '1'));
}

UT_TEST(test_undo_record_type_enum)
{
	UT_ASSERT_EQ(UNDO_RECORD_INSERT, 1);
	UT_ASSERT_EQ(UNDO_RECORD_UPDATE, 2);
	UT_ASSERT_EQ(UNDO_RECORD_DELETE, 3);
	UT_ASSERT_EQ(UNDO_RECORD_ITL, 4);
}

UT_TEST(test_itl_payload_sizeof_40)
{
	UT_ASSERT_EQ((int)sizeof(UndoItlPayload), 40);
}


/* ============================================================
 *	insert inverse
 * ============================================================ */

UT_TEST(test_insert_inverse_removes_tuple)
{
	Page page = build_page_with_tuple();
	UndoRecordHeader hdr;
	UndoInsertPayload p;

	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = TEST_TUPLE_OFFSET;
	memset(&p, 0, sizeof(p));
	p.inserted_tuple_len = TEST_TUPLE_LEN;

	UT_ASSERT_EQ((int)cluster_cr_apply_insert_inverse(synthetic_page, &hdr, &p), 1);
	/* line pointer is now LP_UNUSED -> not normal */
	UT_ASSERT_EQ((int)ItemIdIsNormal(PageGetItemId(page, TEST_TUPLE_OFFSET)), 0);
}

UT_TEST(test_insert_inverse_len_mismatch_false)
{
	Page page = build_page_with_tuple();
	UndoRecordHeader hdr;
	UndoInsertPayload p;

	(void)page;
	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = TEST_TUPLE_OFFSET;
	memset(&p, 0, sizeof(p));
	p.inserted_tuple_len = TEST_TUPLE_LEN + 1; /* wrong */

	UT_ASSERT_EQ((int)cluster_cr_apply_insert_inverse(synthetic_page, &hdr, &p), 0);
}

UT_TEST(test_insert_inverse_offset_out_of_range_false)
{
	UndoRecordHeader hdr;
	UndoInsertPayload p;

	build_page_with_tuple();
	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = 9999; /* past max offset */
	memset(&p, 0, sizeof(p));

	UT_ASSERT_EQ((int)cluster_cr_apply_insert_inverse(synthetic_page, &hdr, &p), 0);
}

UT_TEST(test_insert_inverse_unused_itemid_false)
{
	Page page = build_page_with_tuple();
	UndoRecordHeader hdr;
	UndoInsertPayload p;

	/* mark the slot unused first; inverse should refuse a non-normal id */
	ItemIdSetUnused(PageGetItemId(page, TEST_TUPLE_OFFSET));
	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = TEST_TUPLE_OFFSET;
	memset(&p, 0, sizeof(p));

	UT_ASSERT_EQ((int)cluster_cr_apply_insert_inverse(synthetic_page, &hdr, &p), 0);
}


/* ============================================================
 *	update / delete inverse (full-image restore)
 * ============================================================ */

UT_TEST(test_update_inverse_restores_old_image)
{
	Page page = build_page_with_tuple();
	UndoRecordHeader hdr;
	UndoUpdatePayload p;
	char old_image[TEST_TUPLE_LEN];
	HeapTupleHeader live;

	memset(old_image, 0xAB, sizeof(old_image));
	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = TEST_TUPLE_OFFSET;
	memset(&p, 0, sizeof(p));

	UT_ASSERT_EQ(
		(int)cluster_cr_apply_update_inverse(synthetic_page, &hdr, &p, old_image, TEST_TUPLE_LEN),
		1);
	live = tuple_at(page, TEST_TUPLE_OFFSET);
	UT_ASSERT_EQ(((unsigned char *)live)[0], 0xAB);
	UT_ASSERT_EQ(((unsigned char *)live)[TEST_TUPLE_LEN - 1], 0xAB);
}

UT_TEST(test_update_inverse_len_mismatch_false)
{
	UndoRecordHeader hdr;
	UndoUpdatePayload p;
	char old_image[TEST_TUPLE_LEN];

	build_page_with_tuple();
	memset(old_image, 0xAB, sizeof(old_image));
	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = TEST_TUPLE_OFFSET;
	memset(&p, 0, sizeof(p));

	UT_ASSERT_EQ((int)cluster_cr_apply_update_inverse(synthetic_page, &hdr, &p, old_image,
													  TEST_TUPLE_LEN - 1),
				 0);
}

UT_TEST(test_update_inverse_null_bytes_false)
{
	UndoRecordHeader hdr;
	UndoUpdatePayload p;

	build_page_with_tuple();
	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = TEST_TUPLE_OFFSET;
	memset(&p, 0, sizeof(p));

	UT_ASSERT_EQ(
		(int)cluster_cr_apply_update_inverse(synthetic_page, &hdr, &p, NULL, TEST_TUPLE_LEN), 0);
}

UT_TEST(test_delete_inverse_restores_full_image)
{
	Page page = build_page_with_tuple();
	UndoRecordHeader hdr;
	UndoDeletePayload p;
	char full_image[TEST_TUPLE_LEN];
	HeapTupleHeader live;

	memset(full_image, 0xCD, sizeof(full_image));
	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = TEST_TUPLE_OFFSET;
	memset(&p, 0, sizeof(p));

	UT_ASSERT_EQ(
		(int)cluster_cr_apply_delete_inverse(synthetic_page, &hdr, &p, full_image, TEST_TUPLE_LEN),
		1);
	live = tuple_at(page, TEST_TUPLE_OFFSET);
	UT_ASSERT_EQ(((unsigned char *)live)[0], 0xCD);
}

UT_TEST(test_delete_inverse_len_mismatch_false)
{
	UndoRecordHeader hdr;
	UndoDeletePayload p;
	char full_image[TEST_TUPLE_LEN + 8];

	build_page_with_tuple();
	memset(full_image, 0xCD, sizeof(full_image));
	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = TEST_TUPLE_OFFSET;
	memset(&p, 0, sizeof(p));

	UT_ASSERT_EQ((int)cluster_cr_apply_delete_inverse(synthetic_page, &hdr, &p, full_image,
													  TEST_TUPLE_LEN + 8),
				 0);
}

UT_TEST(test_delete_inverse_zero_len_false)
{
	UndoRecordHeader hdr;
	UndoDeletePayload p;
	char full_image[TEST_TUPLE_LEN];

	build_page_with_tuple();
	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = TEST_TUPLE_OFFSET;
	memset(&p, 0, sizeof(p));

	UT_ASSERT_EQ((int)cluster_cr_apply_delete_inverse(synthetic_page, &hdr, &p, full_image, 0), 0);
}


/* ============================================================
 *	ITL inverse (lock-only header + slot restore)
 * ============================================================ */

UT_TEST(test_itl_inverse_restores_header_and_slot)
{
	Page page = build_page_with_tuple();
	UndoRecordHeader hdr;
	UndoItlPayload p;
	HeapTupleHeader live;
	ClusterItlSlotData *slot;

	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = TEST_TUPLE_OFFSET;
	memset(&p, 0, sizeof(p));
	p.itl_slot_idx = 2;
	p.prev_flags = 7;
	p.prev_xmax = 12345;
	p.prev_infomask = HEAP_XMAX_INVALID;
	p.prev_infomask2 = 3;
	p.prev_commit_scn = 99;

	UT_ASSERT_EQ((int)cluster_cr_apply_itl_inverse(synthetic_page, &hdr, &p), 1);

	live = tuple_at(page, TEST_TUPLE_OFFSET);
	UT_ASSERT_EQ(live->t_infomask, HEAP_XMAX_INVALID);
	UT_ASSERT_EQ(live->t_infomask2, 3);

	slot = &ClusterPageGetItlSlots(page)[2];
	UT_ASSERT_EQ(slot->flags, 7);
	UT_ASSERT_EQ((int)slot->commit_scn, 99);
}

UT_TEST(test_itl_inverse_bad_slot_idx_false)
{
	UndoRecordHeader hdr;
	UndoItlPayload p;

	build_page_with_tuple();
	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = TEST_TUPLE_OFFSET;
	memset(&p, 0, sizeof(p));
	p.itl_slot_idx = CLUSTER_ITL_INITRANS_DEFAULT; /* out of range */

	UT_ASSERT_EQ((int)cluster_cr_apply_itl_inverse(synthetic_page, &hdr, &p), 0);
}

UT_TEST(test_itl_inverse_no_itl_page_false)
{
	UndoRecordHeader hdr;
	UndoItlPayload p;
	PageHeader phdr;

	build_page_with_tuple();
	phdr = (PageHeader)synthetic_page;
	phdr->pd_flags = 0; /* clear PD_HAS_ITL */
	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = TEST_TUPLE_OFFSET;
	memset(&p, 0, sizeof(p));
	p.itl_slot_idx = 0;

	UT_ASSERT_EQ((int)cluster_cr_apply_itl_inverse(synthetic_page, &hdr, &p), 0);
}

UT_TEST(test_itl_inverse_offset_out_of_range_false)
{
	UndoRecordHeader hdr;
	UndoItlPayload p;

	build_page_with_tuple();
	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = 9999;
	memset(&p, 0, sizeof(p));
	p.itl_slot_idx = 0;

	UT_ASSERT_EQ((int)cluster_cr_apply_itl_inverse(synthetic_page, &hdr, &p), 0);
}


/* ============================================================
 *	insert inverse idempotency-ish + multi-helper interplay
 * ============================================================ */

UT_TEST(test_insert_then_update_inverse_sequence)
{
	Page page = build_page_with_tuple();
	UndoRecordHeader hdr;
	UndoUpdatePayload up;
	char old_image[TEST_TUPLE_LEN];

	/* update inverse first restores an image, then insert inverse removes it */
	memset(old_image, 0x11, sizeof(old_image));
	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = TEST_TUPLE_OFFSET;
	memset(&up, 0, sizeof(up));

	UT_ASSERT_EQ(
		(int)cluster_cr_apply_update_inverse(synthetic_page, &hdr, &up, old_image, TEST_TUPLE_LEN),
		1);
	UT_ASSERT_EQ((int)ItemIdIsNormal(PageGetItemId(page, TEST_TUPLE_OFFSET)), 1);

	{
		UndoInsertPayload ip;

		memset(&ip, 0, sizeof(ip));
		ip.inserted_tuple_len = TEST_TUPLE_LEN;
		UT_ASSERT_EQ((int)cluster_cr_apply_insert_inverse(synthetic_page, &hdr, &ip), 1);
		UT_ASSERT_EQ((int)ItemIdIsNormal(PageGetItemId(page, TEST_TUPLE_OFFSET)), 0);
	}
}

UT_TEST(test_insert_inverse_zero_len_skips_sanity)
{
	Page page = build_page_with_tuple();
	UndoRecordHeader hdr;
	UndoInsertPayload p;

	/* inserted_tuple_len == 0 means "skip the length sanity check" */
	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = TEST_TUPLE_OFFSET;
	memset(&p, 0, sizeof(p));
	p.inserted_tuple_len = 0;

	UT_ASSERT_EQ((int)cluster_cr_apply_insert_inverse(synthetic_page, &hdr, &p), 1);
	UT_ASSERT_EQ((int)ItemIdIsNormal(PageGetItemId(page, TEST_TUPLE_OFFSET)), 0);
}


UT_TEST(test_update_inverse_offset_out_of_range_false)
{
	UndoRecordHeader hdr;
	UndoUpdatePayload p;
	char old_image[TEST_TUPLE_LEN];

	build_page_with_tuple();
	memset(old_image, 0xAB, sizeof(old_image));
	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = 9999;
	memset(&p, 0, sizeof(p));

	UT_ASSERT_EQ(
		(int)cluster_cr_apply_update_inverse(synthetic_page, &hdr, &p, old_image, TEST_TUPLE_LEN),
		0);
}

UT_TEST(test_update_inverse_unused_itemid_false)
{
	Page page = build_page_with_tuple();
	UndoRecordHeader hdr;
	UndoUpdatePayload p;
	char old_image[TEST_TUPLE_LEN];

	ItemIdSetUnused(PageGetItemId(page, TEST_TUPLE_OFFSET));
	memset(old_image, 0xAB, sizeof(old_image));
	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = TEST_TUPLE_OFFSET;
	memset(&p, 0, sizeof(p));

	UT_ASSERT_EQ(
		(int)cluster_cr_apply_update_inverse(synthetic_page, &hdr, &p, old_image, TEST_TUPLE_LEN),
		0);
}

UT_TEST(test_delete_inverse_offset_out_of_range_false)
{
	UndoRecordHeader hdr;
	UndoDeletePayload p;
	char full_image[TEST_TUPLE_LEN];

	build_page_with_tuple();
	memset(full_image, 0xCD, sizeof(full_image));
	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = 9999;
	memset(&p, 0, sizeof(p));

	UT_ASSERT_EQ(
		(int)cluster_cr_apply_delete_inverse(synthetic_page, &hdr, &p, full_image, TEST_TUPLE_LEN),
		0);
}

UT_TEST(test_delete_inverse_unused_itemid_false)
{
	Page page = build_page_with_tuple();
	UndoRecordHeader hdr;
	UndoDeletePayload p;
	char full_image[TEST_TUPLE_LEN];

	ItemIdSetUnused(PageGetItemId(page, TEST_TUPLE_OFFSET));
	memset(full_image, 0xCD, sizeof(full_image));
	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = TEST_TUPLE_OFFSET;
	memset(&p, 0, sizeof(p));

	UT_ASSERT_EQ(
		(int)cluster_cr_apply_delete_inverse(synthetic_page, &hdr, &p, full_image, TEST_TUPLE_LEN),
		0);
}

UT_TEST(test_itl_inverse_slot_zero_and_max_minus_one)
{
	Page page = build_page_with_tuple();
	UndoRecordHeader hdr;
	UndoItlPayload p;

	memset(&hdr, 0, sizeof(hdr));
	hdr.target_offset = TEST_TUPLE_OFFSET;

	/* slot 0 */
	memset(&p, 0, sizeof(p));
	p.itl_slot_idx = 0;
	p.prev_flags = 1;
	UT_ASSERT_EQ((int)cluster_cr_apply_itl_inverse(synthetic_page, &hdr, &p), 1);
	UT_ASSERT_EQ(ClusterPageGetItlSlots(page)[0].flags, 1);

	/* last valid slot */
	memset(&p, 0, sizeof(p));
	p.itl_slot_idx = CLUSTER_ITL_INITRANS_DEFAULT - 1;
	p.prev_flags = 2;
	UT_ASSERT_EQ((int)cluster_cr_apply_itl_inverse(synthetic_page, &hdr, &p), 1);
	UT_ASSERT_EQ(ClusterPageGetItlSlots(page)[CLUSTER_ITL_INITRANS_DEFAULT - 1].flags, 2);
}


int
main(int argc, char **argv)
{
	UT_PLAN(26);

	UT_RUN(test_sqlstate_53R9F);
	UT_RUN(test_sqlstate_53R9G);
	UT_RUN(test_data_corrupted_is_XX001);
	UT_RUN(test_undo_record_type_enum);
	UT_RUN(test_itl_payload_sizeof_40);

	UT_RUN(test_insert_inverse_removes_tuple);
	UT_RUN(test_insert_inverse_len_mismatch_false);
	UT_RUN(test_insert_inverse_offset_out_of_range_false);
	UT_RUN(test_insert_inverse_unused_itemid_false);

	UT_RUN(test_update_inverse_restores_old_image);
	UT_RUN(test_update_inverse_len_mismatch_false);
	UT_RUN(test_update_inverse_null_bytes_false);
	UT_RUN(test_delete_inverse_restores_full_image);
	UT_RUN(test_delete_inverse_len_mismatch_false);
	UT_RUN(test_delete_inverse_zero_len_false);

	UT_RUN(test_itl_inverse_restores_header_and_slot);
	UT_RUN(test_itl_inverse_bad_slot_idx_false);
	UT_RUN(test_itl_inverse_no_itl_page_false);
	UT_RUN(test_itl_inverse_offset_out_of_range_false);

	UT_RUN(test_update_inverse_offset_out_of_range_false);
	UT_RUN(test_update_inverse_unused_itemid_false);
	UT_RUN(test_delete_inverse_offset_out_of_range_false);
	UT_RUN(test_delete_inverse_unused_itemid_false);
	UT_RUN(test_itl_inverse_slot_zero_and_max_minus_one);

	UT_RUN(test_insert_then_update_inverse_sequence);
	UT_RUN(test_insert_inverse_zero_len_skips_sanity);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
