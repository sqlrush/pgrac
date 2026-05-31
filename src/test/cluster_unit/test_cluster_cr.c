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

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/*
 * scn_time_cmp -- byte-faithful local stub (spec-3.10): cluster_cr_apply.o's
 * candidate collect/cmp reference it.  Linking cluster_scn.o would drag in
 * shmem/atomics; the header-only scn_local() inline matches the real impl.
 */
int
scn_time_cmp(SCN a, SCN b)
{
	uint64 la = scn_local(a);
	uint64 lb = scn_local(b);

	if (la < lb)
		return -1;
	if (la > lb)
		return 1;
	return 0;
}

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
	/* const + init: zero-len call never reads the image; satisfies cppcheck
	 * constVariable + uninitvar. */
	const char full_image[TEST_TUPLE_LEN] = { 0 };

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
	const ClusterItlSlotData *slot; /* read-only assertion access (cppcheck constVariablePointer) */

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


/* ============================================================
 *	spec-3.10 D1: candidate chain collect + write_scn-DESC order
 * ============================================================ */

/* Build an ITL slot array with given write_scns; head.raw[0] = i+1 to identify. */
static void
build_slots(ClusterItlSlotData *slots, const SCN *write_scns, int n)
{
	int i;

	memset(slots, 0, sizeof(ClusterItlSlotData) * CLUSTER_ITL_INITRANS_DEFAULT);
	for (i = 0; i < n; i++) {
		slots[i].write_scn = write_scns[i];
		slots[i].undo_segment_head.raw[0] = (uint64)(i + 1);
	}
}

UT_TEST(test_collect_filters_by_write_scn)
{
	ClusterItlSlotData slots[CLUSTER_ITL_INITRANS_DEFAULT];
	ClusterCRCandidateChain out[CLUSTER_ITL_INITRANS_DEFAULT];
	/* slot0=100(<=rs) slot1=200(>rs) slot2=Invalid slot3=300(>rs) */
	SCN ws[4] = { (SCN)100, (SCN)200, InvalidScn, (SCN)300 };
	int n;

	build_slots(slots, ws, 4);
	n = cluster_cr_collect_candidate_chains(slots, (SCN)150, out, CLUSTER_ITL_INITRANS_DEFAULT);
	/* read_scn=150 -> only slot1(200) + slot3(300) qualify; 100<=150 + Invalid skipped */
	UT_ASSERT_EQ(n, 2);
}

UT_TEST(test_collect_captures_head_and_scn)
{
	ClusterItlSlotData slots[CLUSTER_ITL_INITRANS_DEFAULT];
	ClusterCRCandidateChain out[CLUSTER_ITL_INITRANS_DEFAULT];
	SCN ws[2] = { (SCN)500, (SCN)600 };
	int n;

	build_slots(slots, ws, 2);
	n = cluster_cr_collect_candidate_chains(slots, (SCN)100, out, CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(n, 2);
	/* fields captured verbatim (collection order = slot order) */
	UT_ASSERT_EQ((int)out[0].slot_idx, 0);
	UT_ASSERT_EQ((int)(out[0].write_scn == (SCN)500), 1);
	UT_ASSERT_EQ((int)(out[0].undo_segment_head.raw[0] == 1ULL), 1);
	UT_ASSERT_EQ((int)(out[1].undo_segment_head.raw[0] == 2ULL), 1);
}

UT_TEST(test_cmp_orders_write_scn_desc)
{
	/* unsorted 100/300/200 -> sorted newest-first 300/200/100 (Q10) */
	ClusterCRCandidateChain c[3];

	memset(c, 0, sizeof(c));
	c[0].slot_idx = 0;
	c[0].write_scn = (SCN)100;
	c[1].slot_idx = 1;
	c[1].write_scn = (SCN)300;
	c[2].slot_idx = 2;
	c[2].write_scn = (SCN)200;

	qsort(c, 3, sizeof(c[0]), cluster_cr_chain_cmp_by_write_scn_desc);

	UT_ASSERT_EQ((int)(c[0].write_scn == (SCN)300), 1);
	UT_ASSERT_EQ((int)(c[1].write_scn == (SCN)200), 1);
	UT_ASSERT_EQ((int)(c[2].write_scn == (SCN)100), 1);
}

UT_TEST(test_cmp_tie_break_slot_idx)
{
	/* equal write_scn -> deterministic slot_idx ascending */
	ClusterCRCandidateChain c[2];

	memset(c, 0, sizeof(c));
	c[0].slot_idx = 5;
	c[0].write_scn = (SCN)200;
	c[1].slot_idx = 2;
	c[1].write_scn = (SCN)200;

	qsort(c, 2, sizeof(c[0]), cluster_cr_chain_cmp_by_write_scn_desc);
	UT_ASSERT_EQ((int)c[0].slot_idx, 2);
	UT_ASSERT_EQ((int)c[1].slot_idx, 5);
}

UT_TEST(test_collect_snapshot_before_mutation)
{
	/* collect captures a snapshot; mutating the slots afterwards must NOT
	 * change the captured candidates (candidate-snapshot property, D1). */
	ClusterItlSlotData slots[CLUSTER_ITL_INITRANS_DEFAULT];
	ClusterCRCandidateChain out[CLUSTER_ITL_INITRANS_DEFAULT];
	SCN ws[2] = { (SCN)700, (SCN)800 };
	int n;

	build_slots(slots, ws, 2);
	n = cluster_cr_collect_candidate_chains(slots, (SCN)100, out, CLUSTER_ITL_INITRANS_DEFAULT);
	UT_ASSERT_EQ(n, 2);

	/* simulate inverse-apply rewriting the ITL slots */
	slots[0].write_scn = InvalidScn;
	slots[0].undo_segment_head.raw[0] = 0xDEAD;
	/* the live slot really changed (so the out[] stability below is meaningful) */
	UT_ASSERT_EQ((int)SCN_VALID(slots[0].write_scn), 0);
	UT_ASSERT_EQ((int)(slots[0].undo_segment_head.raw[0] == 0xDEADULL), 1);

	/* captured out[] is unchanged */
	UT_ASSERT_EQ((int)(out[0].write_scn == (SCN)700), 1);
	UT_ASSERT_EQ((int)(out[0].undo_segment_head.raw[0] == 1ULL), 1);
	UT_ASSERT_EQ((int)(out[1].write_scn == (SCN)800), 1);
}

UT_TEST(test_collect_null_args_zero)
{
	ClusterCRCandidateChain out[CLUSTER_ITL_INITRANS_DEFAULT];

	UT_ASSERT_EQ(
		cluster_cr_collect_candidate_chains(NULL, (SCN)100, out, CLUSTER_ITL_INITRANS_DEFAULT), 0);
}


/* ============================================================
 *	spec-3.10 D1: post-snapshot version prune (full-block completion)
 * ============================================================ */

/* Build a page with n LP_NORMAL tuples (offsets 1..n), tuple i has xmin=xmins[i]. */
static Page
build_page_with_tuples(const TransactionId *xmins, int n)
{
	PageHeader hdr;
	int i;
	int data_off = (int)(BLCKSZ - CLUSTER_ITL_ARRAY_SIZE);

	memset(synthetic_page, 0, BLCKSZ);
	hdr = (PageHeader)synthetic_page;
	hdr->pd_flags = PD_HAS_ITL;
	hdr->pd_special = (LocationIndex)(BLCKSZ - CLUSTER_ITL_ARRAY_SIZE);
	hdr->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	hdr->pd_lower = SizeOfPageHeaderData + (uint16)(n * sizeof(ItemIdData));

	for (i = 0; i < n; i++) {
		ItemId iid;
		HeapTupleHeader htup;

		data_off -= TEST_TUPLE_LEN;
		iid = PageGetItemId((Page)synthetic_page, FirstOffsetNumber + i);
		ItemIdSetNormal(iid, data_off, TEST_TUPLE_LEN);
		htup = (HeapTupleHeader)(synthetic_page + data_off);
		memset(htup, 0, sizeof(HeapTupleHeaderData));
		HeapTupleHeaderSetXmin(htup, xmins[i]);
		htup->t_infomask = HEAP_XMAX_INVALID;
	}
	hdr->pd_upper = (LocationIndex)data_off;
	return (Page)synthetic_page;
}

UT_TEST(test_prune_removes_candidate_xmin)
{
	/* 3 tuples (xmin 100/200/300); candidates {200,300} -> prune 2, keep 100 */
	TransactionId xmins[3] = { 100, 200, 300 };
	Page page = build_page_with_tuples(xmins, 3);
	ClusterCRCandidateChain chains[2];
	int pruned;

	memset(chains, 0, sizeof(chains));
	chains[0].xid = 200;
	chains[1].xid = 300;
	pruned = cluster_cr_prune_post_snapshot_versions((char *)page, chains, 2);
	UT_ASSERT_EQ(pruned, 2);
	UT_ASSERT_EQ((int)ItemIdIsNormal(PageGetItemId(page, 1)), 1); /* xmin=100 kept */
	UT_ASSERT_EQ((int)ItemIdIsNormal(PageGetItemId(page, 2)), 0); /* xmin=200 pruned */
	UT_ASSERT_EQ((int)ItemIdIsNormal(PageGetItemId(page, 3)), 0); /* xmin=300 pruned */
}

UT_TEST(test_prune_keeps_noncandidate_xmin)
{
	/* slot-reuse-shaped: a tuple whose creator is NOT among the candidates is
	 * KEPT.  Correct for a pre-read_scn creator; the slot-reuse edge (a
	 * post-read_scn creator whose ITL slot was recycled so it is not a
	 * candidate) is exactly this kept case (spec-3.10 §3.3). */
	TransactionId xmins[1] = { 999 };
	Page page = build_page_with_tuples(xmins, 1);
	ClusterCRCandidateChain chains[1];
	int pruned;

	memset(chains, 0, sizeof(chains));
	chains[0].xid = 200; /* 999 is not a candidate */
	pruned = cluster_cr_prune_post_snapshot_versions((char *)page, chains, 1);
	UT_ASSERT_EQ(pruned, 0);
	UT_ASSERT_EQ((int)ItemIdIsNormal(PageGetItemId(page, 1)), 1);
}

UT_TEST(test_prune_empty_candidates_noop)
{
	TransactionId xmins[2] = { 100, 200 };
	Page page = build_page_with_tuples(xmins, 2);

	UT_ASSERT_EQ(cluster_cr_prune_post_snapshot_versions((char *)page, NULL, 0), 0);
	UT_ASSERT_EQ((int)ItemIdIsNormal(PageGetItemId(page, 1)), 1);
	UT_ASSERT_EQ((int)ItemIdIsNormal(PageGetItemId(page, 2)), 1);
}

UT_TEST(test_prune_skips_invalid_xmin)
{
	/* tuple0 xmin=Invalid (skip), tuple1 xmin=200 (candidate -> prune) */
	TransactionId xmins[2] = { InvalidTransactionId, 200 };
	Page page = build_page_with_tuples(xmins, 2);
	ClusterCRCandidateChain chains[1];

	memset(chains, 0, sizeof(chains));
	chains[0].xid = 200;
	UT_ASSERT_EQ(cluster_cr_prune_post_snapshot_versions((char *)page, chains, 1), 1);
	UT_ASSERT_EQ((int)ItemIdIsNormal(PageGetItemId(page, 1)), 1); /* invalid xmin kept */
	UT_ASSERT_EQ((int)ItemIdIsNormal(PageGetItemId(page, 2)), 0); /* pruned */
}


int
main(int argc, char **argv)
{
	UT_PLAN(36);

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

	UT_RUN(test_collect_filters_by_write_scn);
	UT_RUN(test_collect_captures_head_and_scn);
	UT_RUN(test_cmp_orders_write_scn_desc);
	UT_RUN(test_cmp_tie_break_slot_idx);
	UT_RUN(test_collect_snapshot_before_mutation);
	UT_RUN(test_collect_null_args_zero);

	UT_RUN(test_prune_removes_candidate_xmin);
	UT_RUN(test_prune_keeps_noncandidate_xmin);
	UT_RUN(test_prune_empty_candidates_noop);
	UT_RUN(test_prune_skips_invalid_xmin);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
