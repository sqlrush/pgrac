/*-------------------------------------------------------------------------
 *
 * test_cluster_cr_tuple.c
 *	  pgrac spec-5.54 D7 — cluster_unit tests for the tuple-level / verdict-only
 *	  CR read fast path (cluster_cr_tuple.c).
 *
 *	  Scope (what is pure-testable standalone, mirroring test_cluster_cr.c):
 *	    - D1 static eligibility predicate (cluster_cr_tuple_eligible_page): each
 *	      pass/fail branch + the matching FALLBACK outcome (no Buffer, no undo).
 *	    - D2 TargetTupleCrState per-record application on a synthetic ITL page:
 *	      inverse INSERT/UPDATE/DELETE/ITL transitions, walk-time identity guard,
 *	      target-only prune (uses cluster_cr_apply.o + cluster_cr_tuple.o).
 *
 *	  The production walk DRIVER (cluster_cr_tuple_verdict) also runs with
 *	  scripted buffer/storage/final-verdict boundaries. The differential
 *	  fast-vs-full-block equivalence checks need a live undo reader / backend, so they
 *	  are covered by cluster_tap t/309 (real own-instance CR), NOT here.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.54-tuple-level-cr-verdict-only.md (FROZEN v1.0)
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cr_tuple.c
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <string.h>

#include "access/htup_details.h"
#include "storage/bufpage.h"
#include "storage/itemid.h"

#include "cluster/cluster_cr_tuple.h"
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
 * scn_time_cmp -- byte-faithful local stub (mirrors test_cluster_cr.c): the
 * eligibility predicate compares the recycle watermark vs read_scn through it.
 * Linking cluster_scn.o would drag in shmem/atomics; the header-only scn_local()
 * inline matches the real impl.
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

/*
 * PageAddItemExtended -- test-local faithful mirror (identical to
 * test_cluster_cr.c): cluster_cr_apply.o's cr_readd_at_offnum references it.
 * Linking the real bufpage.o would drag in elog/palloc/checksum machinery; the
 * REAL primitive is exercised end-to-end in cluster_tap.  Places a heap-tuple
 * image at a specific offnum, overwriting an LP_UNUSED slot in range or
 * appending at max+1; InvalidOffsetNumber on no-fit / rejected placement.
 */
OffsetNumber
PageAddItemExtended(Page page, Item item, Size size, OffsetNumber offsetNumber, int flags)
{
	PageHeader phdr = (PageHeader)page;
	OffsetNumber limit = OffsetNumberNext(PageGetMaxOffsetNumber(page));
	ItemId itemId;
	Size alignedSize;
	int lower;
	int upper;

	(void)flags;

	if (offsetNumber == InvalidOffsetNumber)
		offsetNumber = limit;
	if (offsetNumber > limit)
		return InvalidOffsetNumber;

	if (offsetNumber < limit) {
		itemId = PageGetItemId(page, offsetNumber);
		if (ItemIdIsUsed(itemId) || ItemIdHasStorage(itemId))
			return InvalidOffsetNumber;
		lower = phdr->pd_lower;
	} else {
		lower = (int)phdr->pd_lower + (int)sizeof(ItemIdData);
	}

	alignedSize = MAXALIGN(size);
	upper = (int)phdr->pd_upper - (int)alignedSize;
	if (lower > upper)
		return InvalidOffsetNumber;

	itemId = PageGetItemId(page, offsetNumber);
	ItemIdSetNormal(itemId, (unsigned)upper, size);
	memcpy((char *)page + upper, item, size);
	phdr->pd_lower = (LocationIndex)lower;
	phdr->pd_upper = (LocationIndex)upper;
	return offsetNumber;
}

/* ============================================================
 *	Synthetic 8 KB heap page with the FULL ITL special area
 *	(slot array + 8-byte header so ClusterPageGetItlHeader works).
 * ============================================================ */

static char synthetic_page[BLCKSZ];

#define TEST_TUPLE_OFFSET FirstOffsetNumber
#define TEST_TUPLE_LEN 64
#define TEST_TUPLE_DATA_OFF (BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE - TEST_TUPLE_LEN)

static Page
build_page(SCN recycle_wm)
{
	PageHeader hdr;
	ItemId itemid;
	HeapTupleHeader htup;

	memset(synthetic_page, 0, BLCKSZ);
	hdr = (PageHeader)synthetic_page;
	hdr->pd_flags = PD_HAS_ITL;
	hdr->pd_special = (LocationIndex)(BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE);
	hdr->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	hdr->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	hdr->pd_upper = (LocationIndex)TEST_TUPLE_DATA_OFF;

	itemid = PageGetItemId((Page)synthetic_page, TEST_TUPLE_OFFSET);
	ItemIdSetNormal(itemid, TEST_TUPLE_DATA_OFF, TEST_TUPLE_LEN);

	htup = (HeapTupleHeader)(synthetic_page + TEST_TUPLE_DATA_OFF);
	htup->t_infomask = HEAP_XMAX_INVALID;
	htup->t_infomask2 = 0;

	ClusterPageGetItlHeader((Page)synthetic_page)->itl_recycle_watermark_scn = recycle_wm;
	return (Page)synthetic_page;
}

static HeapTupleHeader
tuple_at(Page page, OffsetNumber off)
{
	return (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, off));
}

/* ===== D1 eligibility tests ===== */

UT_TEST(test_elig_ok_own_instance_nchains1)
{
	Page page = build_page(InvalidScn); /* no watermark -> complete candidate set */
	ClusterCRTupleOutcome reason = CR_TUPLE_OUTCOME__COUNT;
	bool ok
		= cluster_cr_tuple_eligible_page(page, TEST_TUPLE_OFFSET, /*read_scn*/ 100,
										 /*nchains*/ 1, /*tuple_origin*/ 0, /*self*/ 0, &reason);

	UT_ASSERT(ok);
}

UT_TEST(test_elig_ok_watermark_at_or_before_read_scn)
{
	Page page = build_page(/*watermark*/ 100);
	ClusterCRTupleOutcome reason = CR_TUPLE_OUTCOME__COUNT;
	/* watermark (100) <= read_scn (100) -> candidate set complete -> eligible */
	bool ok = cluster_cr_tuple_eligible_page(page, TEST_TUPLE_OFFSET, /*read_scn*/ 100, 1, 0, 0,
											 &reason);

	UT_ASSERT(ok);
}

UT_TEST(test_elig_fallback_remote)
{
	Page page = build_page(InvalidScn);
	ClusterCRTupleOutcome reason = CR_TUPLE_OUTCOME__COUNT;
	bool ok = cluster_cr_tuple_eligible_page(page, TEST_TUPLE_OFFSET, 100, 1,
											 /*tuple_origin*/ 2, /*self*/ 0, &reason);

	UT_ASSERT(!ok);
	UT_ASSERT_EQ(reason, CR_TUPLE_OUTCOME_FALLBACK_REMOTE);
}

UT_TEST(test_elig_fallback_recycle_wm)
{
	Page page = build_page(/*watermark*/ 200);
	ClusterCRTupleOutcome reason = CR_TUPLE_OUTCOME__COUNT;
	/* watermark (200) > read_scn (100) -> candidate set may be incomplete */
	bool ok = cluster_cr_tuple_eligible_page(page, TEST_TUPLE_OFFSET, /*read_scn*/ 100, 1, 0, 0,
											 &reason);

	UT_ASSERT(!ok);
	UT_ASSERT_EQ(reason, CR_TUPLE_OUTCOME_FALLBACK_RECYCLE_WM);
}

UT_TEST(test_elig_fallback_multichain)
{
	Page page = build_page(InvalidScn);
	ClusterCRTupleOutcome reason = CR_TUPLE_OUTCOME__COUNT;
	bool ok = cluster_cr_tuple_eligible_page(page, TEST_TUPLE_OFFSET, 100,
											 /*nchains*/ 2, 0, 0, &reason);

	UT_ASSERT(!ok);
	UT_ASSERT_EQ(reason, CR_TUPLE_OUTCOME_FALLBACK_MULTICHAIN);
}

UT_TEST(test_elig_fallback_nchains_zero)
{
	Page page = build_page(InvalidScn);
	ClusterCRTupleOutcome reason = CR_TUPLE_OUTCOME__COUNT;
	/* nchains != 1 (degenerate 0) is not in scope either -> not eligible */
	bool ok = cluster_cr_tuple_eligible_page(page, TEST_TUPLE_OFFSET, 100,
											 /*nchains*/ 0, 0, 0, &reason);

	UT_ASSERT(!ok);
}

UT_TEST(test_elig_fallback_offnum_out_of_range)
{
	Page page = build_page(InvalidScn);
	ClusterCRTupleOutcome reason = CR_TUPLE_OUTCOME__COUNT;
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
	bool ok = cluster_cr_tuple_eligible_page(page, OffsetNumberNext(maxoff), 100, 1, 0, 0, &reason);

	UT_ASSERT(!ok);
	UT_ASSERT_EQ(reason, CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN);

	ok = cluster_cr_tuple_eligible_page(page, InvalidOffsetNumber, 100, 1, 0, 0, &reason);
	UT_ASSERT(!ok);
	UT_ASSERT_EQ(reason, CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN);
}

/* ============================================================
 *	D2 TargetTupleCrState per-record application + target-only prune
 * ============================================================ */

#define CAND_XID ((TransactionId)5000)
#define OLD_XMIN ((TransactionId)4000)

/* Set the queried-offnum occupant's raw xmin/infomask (xmax) in place. */
static void
set_occupant(Page page, OffsetNumber off, TransactionId xmin, TransactionId xmax, uint16 infomask)
{
	HeapTupleHeader t = tuple_at(page, off);

	HeapTupleHeaderSetXmin(t, xmin);
	HeapTupleHeaderSetXmax(t, xmax);
	t->t_infomask = infomask;
	t->t_infomask2 = 0;
}

/*
 * Build an UPDATE undo record (header + UndoUpdatePayload + old-image bytes) into
 * `buf`; the old image is a TEST_TUPLE_LEN HeapTuple with xmin=img_xmin,
 * xmax=img_xmax, infomask=img_infomask.  Returns the total record length.
 */
static size_t
build_update_record(char *buf, OffsetNumber target_off, TransactionId img_xmin,
					TransactionId img_xmax, uint16 img_infomask)
{
	UndoRecordHeader *hdr = (UndoRecordHeader *)buf;
	UndoUpdatePayload *p = (UndoUpdatePayload *)(buf + sizeof(UndoRecordHeader));
	HeapTupleHeader img = (HeapTupleHeader)((char *)p + sizeof(UndoUpdatePayload));

	memset(buf, 0, sizeof(UndoRecordHeader) + sizeof(UndoUpdatePayload) + TEST_TUPLE_LEN);
	hdr->record_type = UNDO_RECORD_UPDATE;
	hdr->target_offset = target_off;
	p->old_tuple_length = TEST_TUPLE_LEN;
	p->old_tuple_offset = sizeof(UndoUpdatePayload); /* payload-relative */
	HeapTupleHeaderSetXmin(img, img_xmin);
	HeapTupleHeaderSetXmax(img, img_xmax);
	img->t_infomask = img_infomask;
	img->t_infomask2 = 0;
	return sizeof(UndoRecordHeader) + sizeof(UndoUpdatePayload) + TEST_TUPLE_LEN;
}

static size_t
build_insert_record(char *buf, OffsetNumber target_off)
{
	UndoRecordHeader *hdr = (UndoRecordHeader *)buf;
	UndoInsertPayload *p = (UndoInsertPayload *)(buf + sizeof(UndoRecordHeader));

	memset(buf, 0, sizeof(UndoRecordHeader) + sizeof(UndoInsertPayload));
	hdr->record_type = UNDO_RECORD_INSERT;
	hdr->target_offset = target_off;
	p->inserted_tuple_len = TEST_TUPLE_LEN;
	return sizeof(UndoRecordHeader) + sizeof(UndoInsertPayload);
}

static size_t
build_delete_record(char *buf, OffsetNumber target_off, TransactionId img_xmin,
					TransactionId img_xmax, uint16 img_infomask)
{
	UndoRecordHeader *hdr = (UndoRecordHeader *)buf;
	UndoDeletePayload *p = (UndoDeletePayload *)(buf + sizeof(UndoRecordHeader));
	HeapTupleHeader img = (HeapTupleHeader)((char *)p + sizeof(UndoDeletePayload));

	memset(buf, 0, sizeof(UndoRecordHeader) + sizeof(UndoDeletePayload) + TEST_TUPLE_LEN);
	hdr->record_type = UNDO_RECORD_DELETE;
	hdr->target_offset = target_off;
	p->full_tuple_length = TEST_TUPLE_LEN;
	p->full_tuple_offset = sizeof(UndoDeletePayload); /* payload-relative */
	HeapTupleHeaderSetXmin(img, img_xmin);
	HeapTupleHeaderSetXmax(img, img_xmax);
	img->t_infomask = img_infomask;
	img->t_infomask2 = 0;
	return sizeof(UndoRecordHeader) + sizeof(UndoDeletePayload) + TEST_TUPLE_LEN;
}

static size_t
build_itl_record(char *buf, OffsetNumber target_off, uint8 slot_idx, TransactionId prev_xmax,
				 uint16 prev_infomask)
{
	UndoRecordHeader *hdr = (UndoRecordHeader *)buf;
	UndoItlPayload *p = (UndoItlPayload *)(buf + sizeof(UndoRecordHeader));

	memset(buf, 0, sizeof(UndoRecordHeader) + sizeof(UndoItlPayload));
	hdr->record_type = UNDO_RECORD_ITL;
	hdr->target_offset = target_off;
	p->itl_slot_idx = slot_idx;
	p->prev_xmax = prev_xmax;
	p->prev_infomask = prev_infomask;
	p->prev_infomask2 = 0;
	return sizeof(UndoRecordHeader) + sizeof(UndoItlPayload);
}

UT_TEST(test_prune_target_marks_post_snapshot_unused)
{
	Page page = build_page(InvalidScn);

	set_occupant(page, TEST_TUPLE_OFFSET, /*xmin*/ CAND_XID, InvalidTransactionId,
				 HEAP_XMAX_INVALID);
	cluster_cr_tuple_prune_target((char *)page, TEST_TUPLE_OFFSET, CAND_XID);
	UT_ASSERT(!ItemIdIsNormal(PageGetItemId(page, TEST_TUPLE_OFFSET)));
}

UT_TEST(test_prune_target_keeps_pre_snapshot)
{
	Page page = build_page(InvalidScn);

	set_occupant(page, TEST_TUPLE_OFFSET, /*xmin*/ OLD_XMIN, InvalidTransactionId,
				 HEAP_XMAX_INVALID);
	cluster_cr_tuple_prune_target((char *)page, TEST_TUPLE_OFFSET, CAND_XID);
	UT_ASSERT(ItemIdIsNormal(PageGetItemId(page, TEST_TUPLE_OFFSET)));
}

UT_TEST(test_apply_insert_inverse_target_to_unused)
{
	Page page = build_page(InvalidScn);
	char rec[256];
	size_t len = build_insert_record(rec, TEST_TUPLE_OFFSET);
	ClusterCRTupleOutcome reason = CR_TUPLE_OUTCOME__COUNT;
	ClusterCRTupleApplyResult r;

	set_occupant(page, TEST_TUPLE_OFFSET, CAND_XID, InvalidTransactionId, HEAP_XMAX_INVALID);
	r = cluster_cr_tuple_apply_record((char *)page, TEST_TUPLE_OFFSET, rec, len, &reason);

	UT_ASSERT_EQ(r, CR_TUPLE_APPLY_OK);
	/* inverse-INSERT removed the row -> read_scn had no such row -> invisible. */
	UT_ASSERT(!ItemIdIsNormal(PageGetItemId(page, TEST_TUPLE_OFFSET)));
}

UT_TEST(test_apply_update_inverse_matching_identity_restores)
{
	Page page = build_page(InvalidScn);
	char rec[256];
	size_t len;
	ClusterCRTupleOutcome reason = CR_TUPLE_OUTCOME__COUNT;
	ClusterCRTupleApplyResult r;
	HeapTupleHeader occ;

	/* occupant created by CAND_XID (post-snapshot update of OLD_XMIN's row);
	 * the old image is the OLD_XMIN version (chain link: occ.xmin == img.xmax). */
	set_occupant(page, TEST_TUPLE_OFFSET, /*xmin*/ CAND_XID, InvalidTransactionId,
				 HEAP_XMAX_INVALID);
	len = build_update_record(rec, TEST_TUPLE_OFFSET, /*img_xmin*/ OLD_XMIN, /*img_xmax*/ CAND_XID,
							  HEAP_XMAX_INVALID);
	r = cluster_cr_tuple_apply_record((char *)page, TEST_TUPLE_OFFSET, rec, len, &reason);

	UT_ASSERT_EQ(r, CR_TUPLE_APPLY_OK);
	occ = tuple_at(page, TEST_TUPLE_OFFSET);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(occ), OLD_XMIN); /* old image restored */
}

UT_TEST(test_apply_update_inverse_foreign_identity_fallback)
{
	Page page = build_page(InvalidScn);
	char rec[256];
	size_t len;
	ClusterCRTupleOutcome reason = CR_TUPLE_OUTCOME__COUNT;
	ClusterCRTupleApplyResult r;

	/* occupant is a FOREIGN row (line-pointer reuse): xmin matches neither the
	 * image's xmin nor xmax -> identity guard fails -> fallback (NOT a fail-close). */
	set_occupant(page, TEST_TUPLE_OFFSET, /*xmin*/ 9999, InvalidTransactionId, HEAP_XMAX_INVALID);
	len = build_update_record(rec, TEST_TUPLE_OFFSET, /*img_xmin*/ OLD_XMIN, /*img_xmax*/ CAND_XID,
							  HEAP_XMAX_INVALID);
	r = cluster_cr_tuple_apply_record((char *)page, TEST_TUPLE_OFFSET, rec, len, &reason);

	UT_ASSERT_EQ(r, CR_TUPLE_APPLY_FALLBACK);
	UT_ASSERT_EQ(reason, CR_TUPLE_OUTCOME_FALLBACK_IDENTITY);
}

UT_TEST(test_apply_itl_inverse_restores_header_and_slot)
{
	Page page = build_page(InvalidScn);
	char rec[256];
	size_t len;
	ClusterCRTupleOutcome reason = CR_TUPLE_OUTCOME__COUNT;
	ClusterCRTupleApplyResult r;
	HeapTupleHeader occ;
	const ClusterItlSlotData *slot;

	/* lock-only ITL transition set xmax + a lock infomask; inverse restores the
	 * pre-lock header (xmax cleared) + the ITL slot's prior state (v0.4 P0). */
	set_occupant(page, TEST_TUPLE_OFFSET, OLD_XMIN, /*xmax*/ CAND_XID,
				 HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK);
	len = build_itl_record(rec, TEST_TUPLE_OFFSET, /*slot*/ 0, /*prev_xmax*/ InvalidTransactionId,
						   /*prev_infomask*/ HEAP_XMAX_INVALID);
	r = cluster_cr_tuple_apply_record((char *)page, TEST_TUPLE_OFFSET, rec, len, &reason);

	UT_ASSERT_EQ(r, CR_TUPLE_APPLY_OK);
	occ = tuple_at(page, TEST_TUPLE_OFFSET);
	UT_ASSERT_EQ(occ->t_infomask, HEAP_XMAX_INVALID); /* pre-lock header restored */
	slot = &ClusterPageGetItlSlots(page)[0];
	UT_ASSERT(!SCN_VALID(slot->commit_scn)); /* slot restored to prev (zeroed) state */
}

UT_TEST(test_apply_delete_inverse_target_restores)
{
	Page page = build_page(InvalidScn);
	char rec[256];
	size_t len;
	ClusterCRTupleOutcome reason = CR_TUPLE_OUTCOME__COUNT;
	ClusterCRTupleApplyResult r;
	HeapTupleHeader occ;

	/* occupant is the delete-marked live tuple (xmin=OLD, xmax=CAND deleter);
	 * inverse-DELETE restores the pre-delete image (xmax cleared) -> live at read_scn. */
	set_occupant(page, TEST_TUPLE_OFFSET, OLD_XMIN, /*xmax*/ CAND_XID, 0);
	len = build_delete_record(rec, TEST_TUPLE_OFFSET, /*img_xmin*/ OLD_XMIN,
							  /*img_xmax*/ InvalidTransactionId, HEAP_XMAX_INVALID);
	r = cluster_cr_tuple_apply_record((char *)page, TEST_TUPLE_OFFSET, rec, len, &reason);

	UT_ASSERT_EQ(r, CR_TUPLE_APPLY_OK);
	occ = tuple_at(page, TEST_TUPLE_OFFSET);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(occ), OLD_XMIN);
	UT_ASSERT(occ->t_infomask & HEAP_XMAX_INVALID); /* delete undone -> live */
}

UT_TEST(test_apply_same_txn_multi_update_consecutive_inverse)
{
	Page page = build_page(InvalidScn);
	char rec1[256];
	char rec2[256];
	size_t len1;
	size_t len2;
	ClusterCRTupleOutcome reason = CR_TUPLE_OUTCOME__COUNT;
	HeapTupleHeader occ;

	/*
	 * v0.4 P1: a single candidate txn (CAND_XID) updated the target twice
	 * (v0 -> v1 -> v2), all nchains==1.  The chain holds two UPDATE records;
	 * consecutive inverse must peel v2->v1 (same-txn link occ.xmin==img.xmin)
	 * then v1->v0 (chain link occ.xmin==img.xmax) -> final = the read_scn v0.
	 */
	set_occupant(page, TEST_TUPLE_OFFSET, /*v2 xmin*/ CAND_XID, InvalidTransactionId,
				 HEAP_XMAX_INVALID);
	/* record1 old image = v1 (created by CAND, later updated by CAND): xmin=CAND, xmax=CAND. */
	len1 = build_update_record(rec1, TEST_TUPLE_OFFSET, /*img_xmin*/ CAND_XID,
							   /*img_xmax*/ CAND_XID, HEAP_XMAX_INVALID);
	/* record2 old image = v0 (created by OLD, updated by CAND): xmin=OLD, xmax=CAND. */
	len2 = build_update_record(rec2, TEST_TUPLE_OFFSET, /*img_xmin*/ OLD_XMIN,
							   /*img_xmax*/ CAND_XID, HEAP_XMAX_INVALID);

	UT_ASSERT_EQ(
		cluster_cr_tuple_apply_record((char *)page, TEST_TUPLE_OFFSET, rec1, len1, &reason),
		CR_TUPLE_APPLY_OK);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple_at(page, TEST_TUPLE_OFFSET)),
				 CAND_XID); /* now v1 */
	UT_ASSERT_EQ(
		cluster_cr_tuple_apply_record((char *)page, TEST_TUPLE_OFFSET, rec2, len2, &reason),
		CR_TUPLE_APPLY_OK);
	occ = tuple_at(page, TEST_TUPLE_OFFSET);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(occ), OLD_XMIN); /* peeled to read_scn v0 */
}

UT_TEST(test_apply_nontarget_insert_is_noop)
{
	Page page = build_page(InvalidScn);
	char rec[256];
	size_t len
		= build_insert_record(rec, OffsetNumberNext(TEST_TUPLE_OFFSET)); /* different offnum */
	ClusterCRTupleOutcome reason = CR_TUPLE_OUTCOME__COUNT;
	ClusterCRTupleApplyResult r;

	set_occupant(page, TEST_TUPLE_OFFSET, OLD_XMIN, InvalidTransactionId, HEAP_XMAX_INVALID);
	r = cluster_cr_tuple_apply_record((char *)page, TEST_TUPLE_OFFSET, rec, len, &reason);

	UT_ASSERT_EQ(r, CR_TUPLE_APPLY_OK);
	/* queried offnum untouched by a non-target INSERT inverse. */
	UT_ASSERT(ItemIdIsNormal(PageGetItemId(page, TEST_TUPLE_OFFSET)));
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple_at(page, TEST_TUPLE_OFFSET)), OLD_XMIN);
}

UT_TEST(test_retained_history_requires_full_page_without_tuple_mutation)
{
	Page page = build_page(InvalidScn);
	PGAlignedBlock before;
	char record[256];
	size_t length = build_insert_record(record, TEST_TUPLE_OFFSET);
	ClusterCRTupleOutcome reason = CR_TUPLE_OUTCOME__COUNT;

	set_occupant(page, TEST_TUPLE_OFFSET, CAND_XID, InvalidTransactionId, HEAP_XMAX_INVALID);
	((UndoRecordHeader *)record)->flags |= UNDO_REC_FLAG_HAS_ITL_HISTORY;
	memcpy(before.data, page, BLCKSZ);
	UT_ASSERT_EQ(cluster_cr_tuple_apply_record(page, TEST_TUPLE_OFFSET, record, length, &reason),
				 CR_TUPLE_APPLY_FALLBACK);
	UT_ASSERT_EQ(reason, CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN);
	UT_ASSERT_EQ(memcmp(before.data, page, BLCKSZ), 0);
}

/* Run the production walk with explicit buffer/storage/verdict boundaries. */
#include "utils/memutils.h"
#include "../../backend/cluster/cluster_uba.c"

static char *cr_tuple_scratch;
static PGAlignedBlock tuple_walk_scratch;
static PGAlignedBlock tuple_walk_records[2];
static size_t tuple_walk_lengths[2];
static int tuple_walk_reads;
static int tuple_walk_verdicts;
static RelFileLocator tuple_walk_locator = { 1663, 5, 12345 };
static bool tuple_walk_expected_present;
static int tuple_walk_limit = 16;

static Page
tuple_walk_page(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	return synthetic_page;
}

static void
tuple_walk_tag(Buffer buffer, RelFileLocator *locator, ForkNumber *fork, BlockNumber *block)
{
	UT_ASSERT_EQ(buffer, 1);
	*locator = tuple_walk_locator;
	*fork = MAIN_FORKNUM;
	*block = 17;
}

static void *
tuple_walk_alloc(MemoryContext context pg_attribute_unused(), Size bytes)
{
	UT_ASSERT_EQ(bytes, BLCKSZ);
	memset(tuple_walk_scratch.data, 0, BLCKSZ);
	return tuple_walk_scratch.data;
}

static size_t
tuple_walk_get_record(UBA address, void *out, size_t capacity)
{
	unsigned index = (unsigned)tuple_walk_reads++;

	UT_ASSERT(index < lengthof(tuple_walk_records));
	if (index >= lengthof(tuple_walk_records))
		return 0;
	UT_ASSERT(cluster_undo_record_uba_equal(address, uba_encode(1, 7, 2, 1 - index)));
	UT_ASSERT(capacity >= tuple_walk_lengths[index]);
	memcpy(out, tuple_walk_records[index].data, tuple_walk_lengths[index]);
	return tuple_walk_lengths[index];
}

static ClusterCrVerdict
cluster_cr_verdict_on_image(const char *page, OffsetNumber offset, const ClusterItlSlotData *slot,
							Snapshot snapshot, bool remote, int32 origin, bool *visible)
{
	tuple_walk_verdicts++;
	UT_ASSERT_EQ(offset, TEST_TUPLE_OFFSET);
	UT_ASSERT(slot == ClusterPageGetItlSlots(synthetic_page));
	UT_ASSERT_EQ(snapshot->read_scn, 100);
	UT_ASSERT(!remote && origin == -1);
	UT_ASSERT_EQ(ItemIdIsNormal(PageGetItemId((Page)page, offset)), tuple_walk_expected_present);
	*visible = false;
	return CLUSTER_CR_DECIDED;
}

static bool
tuple_walk_materialized(int node pg_attribute_unused())
{
	UT_ASSERT(false);
	return false;
}

#define BufferGetPage tuple_walk_page
#define BufferGetTag tuple_walk_tag
#define MemoryContextAllocZero tuple_walk_alloc
#define TopMemoryContext ((MemoryContext)NULL)
#define cluster_node_id 0
#define cluster_cr_chain_walk_max_steps tuple_walk_limit
#define cluster_undo_get_record tuple_walk_get_record
#define cluster_merged_instance_is_materialized tuple_walk_materialized
#include "test_cluster_cr_tuple_walk.inc"
#undef BufferGetPage
#undef BufferGetTag
#undef MemoryContextAllocZero
#undef TopMemoryContext
#undef cluster_node_id
#undef cluster_cr_chain_walk_max_steps
#undef cluster_undo_get_record
#undef cluster_merged_instance_is_materialized

static void
tuple_walk_validate_record(const UndoRecordHeader *record)
{
	uint16 body_length;
	UndoItlHistoryTrailer history;
	UndoOperationTrailer operation;
	const char *payload = (const char *)record + sizeof(*record);

	if ((record->flags & UNDO_REC_FLAG_HAS_OPERATION) != 0) {
		if ((record->flags & UNDO_REC_FLAG_HAS_ITL_HISTORY) != 0)
			UT_ASSERT(cluster_undo_record_decode_operation_payload(
				record, payload, record->payload_length, &body_length, &history, &operation));
	} else
		UT_ASSERT(cluster_undo_record_decode_payload(record, payload, record->payload_length,
													 &body_length, &history));
}

static void
tuple_walk_record(unsigned index, uint8 flags, SCN scn, bool other_block)
{
	char *bytes = tuple_walk_records[index].data;
	UndoRecordHeader *record = (UndoRecordHeader *)bytes;
	UndoItlHistoryTrailer history = { 0 };
	UndoOperationTrailer operation = { 0 };
	size_t length;

	memset(bytes, 0, BLCKSZ);
	length = build_insert_record(bytes, TEST_TUPLE_OFFSET);
	record->flags = flags | UNDO_REC_FLAG_FIRST_IN_TX;
	record->xid = (flags & UNDO_REC_FLAG_HAS_OPERATION) ? CAND_XID + 1 : CAND_XID;
	record->origin_node_id = 0;
	record->tt_slot_segment_id = 1;
	record->tt_slot_id = 3;
	record->tt_wrap_plus1 = 8;
	record->write_scn = scn;
	record->target_locator = tuple_walk_locator;
	record->target_fork = MAIN_FORKNUM;
	record->target_block = other_block ? 18 : 17;
	if ((flags & UNDO_REC_FLAG_HAS_ITL_HISTORY) != 0) {
		history.magic = UNDO_ITL_HISTORY_MAGIC;
		history.version = UNDO_ITL_HISTORY_VERSION;
		history.count = 1;
		history.entries[0].block = record->target_block;
		history.entries[0].after_kind = ITL_FLAG_ACTIVE;
		history.entries[0].after_write_scn = scn;
		memcpy(bytes + length, &history, sizeof(history));
		length += sizeof(history);
	}
	if ((flags & UNDO_REC_FLAG_HAS_OPERATION) != 0) {
		operation.version = UNDO_OPERATION_TRAILER_VERSION;
		operation.length = sizeof(operation);
		operation.effect_state = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
		operation.canonical_top_fxid = CAND_XID;
		operation.operation_fxid = CAND_XID + 1;
		operation.operation_sequence = 1;
		operation.canonical_segment_generation = 7;
		operation.command_id = 4;
		operation.subid = TopSubTransactionId + 1;
		memcpy(bytes + length, &operation, sizeof(operation));
		length += sizeof(operation);
	}
	record->payload_length = length - sizeof(*record);
	tuple_walk_lengths[index] = length;
	tuple_walk_validate_record(record);
}

static void
tuple_walk_check(bool fallback, int reads)
{
	Page page = build_page(InvalidScn);
	ClusterItlSlotData *slot = ClusterPageGetItlSlots(page);
	HeapTupleData tuple = { 0 };
	SnapshotData snapshot = { 0 };
	ClusterCRTupleOutcome reason = CR_TUPLE_OUTCOME__COUNT;
	PGAlignedBlock before;
	bool visible = true;

	set_occupant(page, TEST_TUPLE_OFFSET, CAND_XID, InvalidTransactionId, HEAP_XMAX_INVALID);
	tuple_at(page, TEST_TUPLE_OFFSET)->t_itl_slot_idx = 0;
	slot->xid = CAND_XID;
	slot->flags = ITL_FLAG_ACTIVE;
	slot->write_scn = 150;
	slot->undo_segment_head = uba_encode(1, 7, 2, 1);
	tuple.t_data = tuple_at(page, TEST_TUPLE_OFFSET);
	tuple.t_len = TEST_TUPLE_LEN;
	ItemPointerSet(&tuple.t_self, 17, TEST_TUPLE_OFFSET);
	snapshot.read_scn = 100;
	tuple_walk_reads = tuple_walk_verdicts = 0;
	tuple_walk_expected_present = false;
	cr_tuple_scratch = NULL;
	memcpy(before.data, page, BLCKSZ);
	UT_ASSERT_EQ(cluster_cr_tuple_verdict(1, &tuple, &snapshot, &visible, &reason),
				 fallback ? CLUSTER_CR_NOT_APPLICABLE : CLUSTER_CR_DECIDED);
	UT_ASSERT_EQ(reason, fallback ? CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN : CR_TUPLE_OUTCOME_VERDICT);
	UT_ASSERT_EQ(tuple_walk_reads, reads);
	UT_ASSERT_EQ(tuple_walk_verdicts, fallback ? 0 : 1);
	UT_ASSERT_EQ(visible, fallback);
	UT_ASSERT_EQ(memcmp(before.data, page, BLCKSZ), 0);
}

UT_TEST(test_tuple_walk_history_precedes_scn_and_block_shortcuts)
{
	const uint8 flags[] = { UNDO_REC_FLAG_HAS_ITL_HISTORY,
							UNDO_REC_FLAG_HAS_ITL_HISTORY | UNDO_REC_FLAG_HAS_OPERATION,
							UNDO_REC_FLAG_HAS_OPERATION };
	unsigned i;
	int scn;
	int other;

	for (i = 0; i < lengthof(flags); i++)
		for (scn = 99; scn <= 101; scn++)
			for (other = 0; other <= 1; other++) {
				tuple_walk_record(0, flags[i], scn, other);
				tuple_walk_check(true, 1);
			}
}

UT_TEST(test_tuple_walk_history_after_legacy_predecessor)
{
	UndoRecordHeader *first;

	tuple_walk_record(0, 0, 120, true);
	first = (UndoRecordHeader *)tuple_walk_records[0].data;
	first->prev_uba = uba_encode(1, 7, 2, 0);
	first->flags &= ~UNDO_REC_FLAG_FIRST_IN_TX;
	tuple_walk_record(1, UNDO_REC_FLAG_HAS_ITL_HISTORY | UNDO_REC_FLAG_HAS_OPERATION, 99, false);
	tuple_walk_check(true, 2);
}

UT_TEST(test_tuple_walk_legacy_scn_stop_keeps_image_verdict)
{
	int scn;

	for (scn = 99; scn <= 100; scn++) {
		tuple_walk_record(0, 0, scn, false);
		tuple_walk_check(false, 1);
	}
	tuple_walk_record(0, 0, 101, true);
	tuple_walk_check(false, 1);
}

UT_TEST(test_operation_flag_without_history_requires_full_page)
{
	Page page = build_page(InvalidScn);
	PGAlignedBlock before;
	ClusterCRTupleOutcome reason = CR_TUPLE_OUTCOME__COUNT;

	set_occupant(page, TEST_TUPLE_OFFSET, CAND_XID, InvalidTransactionId, HEAP_XMAX_INVALID);
	tuple_walk_record(0, UNDO_REC_FLAG_HAS_OPERATION, 101, false);
	memcpy(before.data, page, BLCKSZ);
	UT_ASSERT_EQ(cluster_cr_tuple_apply_record(page, TEST_TUPLE_OFFSET, tuple_walk_records[0].data,
											   tuple_walk_lengths[0], &reason),
				 CR_TUPLE_APPLY_FALLBACK);
	UT_ASSERT_EQ(reason, CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN);
	UT_ASSERT_EQ(memcmp(before.data, page, BLCKSZ), 0);
}

UT_TEST(test_tuple_walk_truncated_operation_cannot_reach_image_verdict)
{
	int scn;

	for (scn = 99; scn <= 101; scn += 2) {
		tuple_walk_record(0, UNDO_REC_FLAG_HAS_ITL_HISTORY | UNDO_REC_FLAG_HAS_OPERATION, scn,
						  false);
		tuple_walk_lengths[0] = sizeof(UndoRecordHeader);
		tuple_walk_check(true, 1);
	}
}

int
main(void)
{
	UT_PLAN(22);
	UT_RUN(test_retained_history_requires_full_page_without_tuple_mutation);
	UT_RUN(test_elig_ok_own_instance_nchains1);
	UT_RUN(test_elig_ok_watermark_at_or_before_read_scn);
	UT_RUN(test_elig_fallback_remote);
	UT_RUN(test_elig_fallback_recycle_wm);
	UT_RUN(test_elig_fallback_multichain);
	UT_RUN(test_elig_fallback_nchains_zero);
	UT_RUN(test_elig_fallback_offnum_out_of_range);
	UT_RUN(test_prune_target_marks_post_snapshot_unused);
	UT_RUN(test_prune_target_keeps_pre_snapshot);
	UT_RUN(test_apply_insert_inverse_target_to_unused);
	UT_RUN(test_apply_update_inverse_matching_identity_restores);
	UT_RUN(test_apply_update_inverse_foreign_identity_fallback);
	UT_RUN(test_apply_itl_inverse_restores_header_and_slot);
	UT_RUN(test_apply_delete_inverse_target_restores);
	UT_RUN(test_apply_same_txn_multi_update_consecutive_inverse);
	UT_RUN(test_apply_nontarget_insert_is_noop);
	UT_RUN(test_tuple_walk_history_precedes_scn_and_block_shortcuts);
	UT_RUN(test_tuple_walk_history_after_legacy_predecessor);
	UT_RUN(test_tuple_walk_legacy_scn_stop_keeps_image_verdict);
	UT_RUN(test_operation_flag_without_history_requires_full_page);
	UT_RUN(test_tuple_walk_truncated_operation_cannot_reach_image_verdict);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
