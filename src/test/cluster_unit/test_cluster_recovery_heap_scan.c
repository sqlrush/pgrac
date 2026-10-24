/* Cold recovery heap candidates from original page and tuple byte layouts.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
#include "postgres.h"
#include "access/htup_details.h"
#include "cluster/cluster_recovery_heap_scan.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, file, line);
	abort();
}

#include "../../backend/cluster/cluster_recovery_heap_scan.c"

typedef union TestPage {
	uint64 alignment;
	char bytes[BLCKSZ];
} TestPage;

static void
make_page(TestPage *storage, unsigned items)
{
	Page page = storage->bytes;
	PageHeader header = (PageHeader)page;

	memset(storage, 0, sizeof(*storage));
	header->pd_lower = SizeOfPageHeaderData + items * sizeof(ItemIdData);
	header->pd_special = BLCKSZ - MAXALIGN(CLUSTER_ITL_SPECIAL_SIZE);
	header->pd_upper = header->pd_special;
	header->pd_flags = PD_HAS_ITL;
	PageSetPageSizeAndVersion(page, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
}

static HeapTupleHeader
make_tuple(TestPage *storage, unsigned index, unsigned flags)
{
	PageHeader header = (PageHeader)storage->bytes;
	ItemId item = &header->pd_linp[index - 1];
	HeapTupleHeader tuple;

	header->pd_upper -= MAXALIGN(SizeofHeapTupleHeader);
	ItemIdSetNormal(item, header->pd_upper, SizeofHeapTupleHeader);
	item->lp_flags = flags;
	tuple = (HeapTupleHeader)(storage->bytes + item->lp_off);
	tuple->t_choice.t_heap.t_xmin = 4195136 + index;
	tuple->t_choice.t_heap.t_xmax = 123;
	tuple->t_infomask = HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI;
	tuple->t_infomask2 = HEAP_HOT_UPDATED | 1;
	tuple->t_hoff = SizeofHeapTupleHeader;
	tuple->t_itl_slot_idx = CLUSTER_ITL_SLOT_UNALLOCATED;
	ItemPointerSet(&tuple->t_ctid, 9, 3);
	return tuple;
}

UT_TEST(test_all_itls_and_line_pointers_are_preserved)
{
	TestPage page;
	ClusterRecoveryHeapScan *scan = NULL;
	ClusterRecoveryHeapEntry entry;
	ClusterItlSlotData expected[CLUSTER_ITL_INITRANS_DEFAULT];
	char tuple1[SizeofHeapTupleHeader], tuple2[SizeofHeapTupleHeader];
	PageHeader header;

	make_page(&page, 5);
	header = (PageHeader)page.bytes;
	memcpy(tuple1, make_tuple(&page, 1, LP_NORMAL), sizeof(tuple1));
	memcpy(tuple2, make_tuple(&page, 2, LP_DEAD), sizeof(tuple2));
	ItemIdSetRedirect(&header->pd_linp[2], 1);
	ItemIdSetDead(&header->pd_linp[3]);
	ItemIdSetUnused(&header->pd_linp[4]);
	for (unsigned i = 0; i < lengthof(expected); i++) {
		ClusterItlSlotData *slot = &ClusterPageGetItlSlots(page.bytes)[i];

		slot->flags = i == 7 ? ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI : i;
		slot->xid = 4195200 + i;
		slot->wrap = i;
		slot->undo_segment_head.raw[0] = 100 + i;
		slot->undo_segment_head.raw[1] = 200 + i;
		slot->commit_scn = 500 + i;
		expected[i] = *slot;
	}
	UT_ASSERT_EQ(cluster_recovery_heap_scan_begin(page.bytes, BLCKSZ, &scan),
				 CLUSTER_RECOVERY_HEAP_MORE);
	memset(&page, 0xee, sizeof(page)); /* scanner must own the observed bytes */
	for (unsigned i = 0; i < lengthof(expected); i++) {
		UT_ASSERT_EQ(cluster_recovery_heap_scan_next(scan, &entry), CLUSTER_RECOVERY_HEAP_ENTRY);
		UT_ASSERT_EQ(entry.kind, CLUSTER_RECOVERY_HEAP_ITL);
		UT_ASSERT_EQ(entry.index, i);
		UT_ASSERT_EQ(memcmp(&entry.itl, &expected[i], sizeof(expected[i])), 0);
		UT_ASSERT(entry.tuple_bytes == NULL);
	}
	for (unsigned i = 1; i <= 5; i++) {
		UT_ASSERT_EQ(cluster_recovery_heap_scan_next(scan, &entry), CLUSTER_RECOVERY_HEAP_ENTRY);
		UT_ASSERT_EQ(entry.kind, CLUSTER_RECOVERY_HEAP_ITEM);
		UT_ASSERT_EQ(entry.index, i);
		if (i <= 2) {
			UT_ASSERT_EQ(entry.tuple_length, SizeofHeapTupleHeader);
			UT_ASSERT_EQ(memcmp(entry.tuple_bytes, i == 1 ? tuple1 : tuple2, entry.tuple_length),
						 0);
			UT_ASSERT_EQ(entry.item.lp_flags, i == 1 ? LP_NORMAL : LP_DEAD);
		} else {
			UT_ASSERT(entry.tuple_bytes == NULL && entry.tuple_length == 0);
			UT_ASSERT_EQ(entry.item.lp_flags, i == 3 ? LP_REDIRECT : i == 4 ? LP_DEAD : LP_UNUSED);
			if (i == 3)
				UT_ASSERT_EQ(entry.item.lp_off, 1);
		}
	}
	UT_ASSERT_EQ(cluster_recovery_heap_scan_next(scan, &entry), CLUSTER_RECOVERY_HEAP_END);
	UT_ASSERT_EQ(cluster_recovery_heap_scan_next(scan, &entry), CLUSTER_RECOVERY_HEAP_END);
	cluster_recovery_heap_scan_destroy(scan);
}

UT_TEST(test_empty_heap_still_visits_every_itl)
{
	TestPage page;
	ClusterRecoveryHeapScan *scan = NULL;
	ClusterRecoveryHeapEntry entry;
	unsigned count = 0;

	make_page(&page, 0);
	UT_ASSERT_EQ(cluster_recovery_heap_scan_begin(page.bytes, BLCKSZ, &scan),
				 CLUSTER_RECOVERY_HEAP_MORE);
	while (cluster_recovery_heap_scan_next(scan, &entry) == CLUSTER_RECOVERY_HEAP_ENTRY)
		count++;
	UT_ASSERT_EQ(count, CLUSTER_ITL_INITRANS_DEFAULT);
	cluster_recovery_heap_scan_destroy(scan);
}

UT_TEST(test_unknown_and_malformed_pages_do_not_become_empty_graphs)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		TestPage page;
		PageHeader header = (PageHeader)page.bytes;
		ClusterRecoveryHeapScan *scan = NULL;

		make_page(&page, 0);
		switch (fault) {
		case 0:
			header->pd_pagesize_version = BLCKSZ | 4;
			break;
		case 1:
			header->pd_flags = 0;
			break;
		case 2:
			header->pd_lower++;
			break;
		case 3:
			header->pd_upper = BLCKSZ;
			break;
		case 4:
			header->pd_special += 8;
			break;
		case 5:
			header->pd_flags |= PD_UNDO_SEG_HEADER;
			break;
		case 6:
			header->pd_flags |= 0x8000;
			break;
		case 7:
			ClusterPageGetItlSlots(page.bytes)[3].flags = 255;
			break;
		}
		UT_ASSERT_EQ(cluster_recovery_heap_scan_begin(page.bytes, BLCKSZ, &scan),
					 fault < 2 ? CLUSTER_RECOVERY_HEAP_UNSUPPORTED : CLUSTER_RECOVERY_HEAP_CORRUPT);
		UT_ASSERT(scan == NULL);
	}
}

UT_TEST(test_tuple_and_hot_damage_never_exports_a_partial_prefix)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		TestPage page;
		PageHeader header = (PageHeader)page.bytes;
		ClusterRecoveryHeapScan *scan = NULL;
		HeapTupleHeader tuple;

		make_page(&page, 2);
		tuple = make_tuple(&page, 1, LP_NORMAL);
		(void)make_tuple(&page, 2, LP_NORMAL);
		switch (fault) {
		case 0:
			header->pd_linp[1].lp_len = SizeofHeapTupleHeader - 1;
			break;
		case 1:
			tuple->t_hoff++;
			break;
		case 2:
			tuple->t_hoff = 0;
			break;
		case 3:
			tuple->t_infomask |= HEAP_HASNULL;
			tuple->t_infomask2 = HEAP_NATTS_MASK;
			break;
		case 4:
			tuple->t_itl_slot_idx = CLUSTER_ITL_INITRANS_DEFAULT;
			break;
		case 5:
			header->pd_linp[1].lp_off = 0;
			break;
		case 6:
			header->pd_linp[1].lp_off++;
			break;
		case 7:
			header->pd_linp[1].lp_off = header->pd_special;
			break;
		case 8:
			header->pd_linp[1] = header->pd_linp[0];
			break;
		case 9:
			ItemIdSetRedirect(&header->pd_linp[1], 3);
			break;
		case 10:
			ItemIdSetRedirect(&header->pd_linp[0], 2);
			ItemIdSetRedirect(&header->pd_linp[1], 1);
			break;
		}
		UT_ASSERT_EQ(cluster_recovery_heap_scan_begin(page.bytes, BLCKSZ, &scan),
					 CLUSTER_RECOVERY_HEAP_CORRUPT);
		UT_ASSERT(scan == NULL);
	}
}

UT_TEST(test_invalid_arguments_clear_outputs)
{
	TestPage page;
	ClusterRecoveryHeapScan *scan = (ClusterRecoveryHeapScan *)(uintptr_t)1;
	ClusterRecoveryHeapEntry entry, zero;

	make_page(&page, 0);
	UT_ASSERT_EQ(cluster_recovery_heap_scan_begin(page.bytes, BLCKSZ - 1, &scan),
				 CLUSTER_RECOVERY_HEAP_INVALID);
	UT_ASSERT(scan == NULL);
	UT_ASSERT_EQ(cluster_recovery_heap_scan_begin(NULL, BLCKSZ, &scan),
				 CLUSTER_RECOVERY_HEAP_INVALID);
	memset(&entry, 0xff, sizeof(entry));
	memset(&zero, 0, sizeof(zero));
	UT_ASSERT_EQ(cluster_recovery_heap_scan_next(NULL, &entry), CLUSTER_RECOVERY_HEAP_INVALID);
	UT_ASSERT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);
	cluster_recovery_heap_scan_destroy(NULL);
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_all_itls_and_line_pointers_are_preserved);
	UT_RUN(test_empty_heap_still_visits_every_itl);
	UT_RUN(test_unknown_and_malformed_pages_do_not_become_empty_graphs);
	UT_RUN(test_tuple_and_hot_damage_never_exports_a_partial_prefix);
	UT_RUN(test_invalid_arguments_clear_outputs);
	UT_DONE();
	return ut_failed_count != 0;
}
