/* Cold PAGE_VERSION paths use the real record decoder and CR inverse.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
#include "postgres.h"
#include "access/htup_details.h"
#include "cluster/cluster_recovery_heap_scan.h"
#include "cluster/cluster_recovery_page_history.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, file, line);
	abort();
}

#include "../../backend/cluster/cluster_uba.c"
#include "test_cluster_heap_small_scn.inc"
#include "../../backend/cluster/cluster_recovery_heap_scan.c"
#include "../../backend/cluster/cluster_recovery_undo_scan.c"
#include "../../backend/cluster/cluster_recovery_reference_graph.c"
#include "../../backend/cluster/cluster_recovery_page_history.c"

typedef union HistoryPage {
	uint64 alignment;
	char bytes[BLCKSZ];
} HistoryPage;

static HistoryPage current_page;
static HistoryPage undo_pages[24];
static ClusterRecoveryTxnKey transactions[24];
static ClusterSpaceIdentity space;

static void
prepare_history(unsigned count)
{
	PageHeader page = (PageHeader)current_page.bytes;
	ClusterItlSlotData prior = { 0 };

	memset(&current_page, 0, sizeof(current_page));
	memset(undo_pages, 0, sizeof(undo_pages));
	memset(transactions, 0, sizeof(transactions));
	memset(&space, 0, sizeof(space));
	space.key.system_identifier = 817;
	space.key.database_incarnation = 91;
	space.key.storage_uuid[0] = 4;
	space.key.locator = (RelFileLocator){ 1663, 5, 16385 };
	space.incarnation[0] = 9;
	space.sequence = 1;
	space.operation = 1;
	space.state = CLUSTER_SPACE_IDENTITY_LIVE;
	page->pd_flags = PD_HAS_ITL;
	page->pd_lower = SizeOfPageHeaderData + count * sizeof(ItemIdData);
	page->pd_special = BLCKSZ - MAXALIGN(CLUSTER_ITL_SPECIAL_SIZE);
	page->pd_upper = page->pd_special;
	PageSetPageSizeAndVersion(current_page.bytes, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	for (unsigned i = 0; i < count; i++) {
		ClusterRecoveryTxnKey *key = &transactions[i];
		UndoBlockHeader block = { 0 };
		UndoSlotDirEntry directory = { 0 };
		UndoRecordHeader record = { 0 };
		UndoInsertPayload insert = { SizeofHeapTupleHeader, 0 };
		UndoItlHistoryTrailer trailer = { 0 };
		ClusterItlSlotData after = { 0 };
		HeapTupleHeader tuple;
		char *bytes = undo_pages[i].bytes;

		key->system_identifier = space.key.system_identifier;
		key->database_incarnation = space.key.database_incarnation;
		key->segment_id = 257;
		key->segment_generation = 0;
		key->origin_node_id = 1;
		key->slot_offset = 6;
		key->slot_wrap = i + 1; /* independent of the page ITL wrap */
		key->xid = 4195136 + i;
		after.flags = ITL_FLAG_COMMITTED;
		after.xid = key->xid;
		after.wrap = i;
		after.write_scn = 100 + i * 10;
		after.commit_scn = after.write_scn + 5;
		after.undo_segment_head = uba_encode(257, i + 1, 6, 0);
		page->pd_upper -= MAXALIGN(SizeofHeapTupleHeader);
		ItemIdSetNormal(&page->pd_linp[i], page->pd_upper, SizeofHeapTupleHeader);
		tuple = (HeapTupleHeader)(current_page.bytes + page->pd_upper);
		HeapTupleHeaderSetXmin(tuple, key->xid);
		tuple->t_hoff = SizeofHeapTupleHeader;
		tuple->t_itl_slot_idx = 0;
		tuple->t_infomask = HEAP_XMAX_INVALID;
		ItemPointerSet(&tuple->t_ctid, 37, i + 1);
		record.record_type = UNDO_RECORD_INSERT;
		record.flags = UNDO_REC_FLAG_FIRST_IN_TX | UNDO_REC_FLAG_HAS_ITL_HISTORY;
		record.payload_length = sizeof(insert) + sizeof(trailer);
		record.xid = key->xid;
		record.origin_node_id = 1;
		record.tt_slot_segment_id = 257;
		record.tt_slot_id = 7;
		record.tt_wrap_plus1 = key->slot_wrap + 1;
		record.write_scn = after.write_scn + 1;
		record.target_locator = space.key.locator;
		record.target_fork = MAIN_FORKNUM;
		record.target_block = 37;
		record.target_offset = i + 1;
		trailer.magic = UNDO_ITL_HISTORY_MAGIC;
		trailer.version = UNDO_ITL_HISTORY_VERSION;
		trailer.count = 1;
		trailer.entries[0].block = 37;
		trailer.entries[0].slot_index = 0;
		trailer.entries[0].after_kind = ITL_FLAG_ACTIVE;
		trailer.entries[0].after_write_scn = after.write_scn;
		trailer.entries[0].prior = prior;
		block.magic = PGRAC_UNDO_BLOCK_MAGIC;
		block.block_version = UNDO_BLOCK_VERSION_1;
		block.slot_count = 1;
		block.free_offset = sizeof(block) + sizeof(record) + record.payload_length;
		directory.record_offset = sizeof(block);
		directory.record_length = sizeof(record) + record.payload_length;
		directory.record_type = record.record_type;
		directory.flags = record.flags;
		memcpy(bytes, &block, sizeof(block));
		memcpy(bytes + sizeof(block), &record, sizeof(record));
		memcpy(bytes + sizeof(block) + sizeof(record), &insert, sizeof(insert));
		memcpy(bytes + sizeof(block) + sizeof(record) + sizeof(insert), &trailer, sizeof(trailer));
		memcpy(bytes + UNDO_SLOT_DIR_OFFSET(0), &directory, sizeof(directory));
		prior = after;
	}
	ClusterPageGetItlSlots(current_page.bytes)[0] = prior;
}

static ClusterRecoveryPageHistory *
begin_history(size_t budget)
{
	ClusterRecoveryPageHistory *history = NULL;

	UT_ASSERT_EQ(cluster_recovery_page_history_begin(&space, MAIN_FORKNUM, 37, current_page.bytes,
													 BLCKSZ, budget, &history),
				 CLUSTER_RECOVERY_HISTORY_MORE);
	return history;
}

static void
read_undo(unsigned index, ClusterRecoveryUndoRecord *record)
{
	UT_ASSERT_EQ(cluster_recovery_undo_record_read(undo_pages[index].bytes, BLCKSZ, 257, index + 1,
												   2, uba_encode(257, index + 1, 6, 0), record),
				 CLUSTER_RECOVERY_UNDO_ENTRY);
}

static unsigned
count_tuples(const char *page)
{
	ClusterRecoveryHeapScan *scan = NULL;
	ClusterRecoveryHeapEntry entry;
	unsigned count = 0;

	UT_ASSERT_EQ(cluster_recovery_heap_scan_begin(page, BLCKSZ, &scan), CLUSTER_RECOVERY_HEAP_MORE);
	if (scan == NULL)
		return UINT32_MAX;
	while (cluster_recovery_heap_scan_next(scan, &entry) == CLUSTER_RECOVERY_HEAP_ENTRY)
		if (entry.tuple_bytes != NULL)
			count++;
	cluster_recovery_heap_scan_destroy(scan);
	return count;
}

UT_TEST(test_prior_transaction_is_a_new_dependency_not_prev_uba)
{
	ClusterRecoveryPageHistory *history;
	ClusterRecoveryHistoryEvent event;
	HistoryPage original;

	prepare_history(17);
	original = current_page;
	history = begin_history(128 * 1024);
	if (history == NULL)
		return;
	for (unsigned remaining = 17; remaining > 0; remaining--) {
		ClusterRecoveryUndoRecord record;

		UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
					 CLUSTER_RECOVERY_HISTORY_PAGE);
		UT_ASSERT_EQ(count_tuples(event.page), remaining);
		UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
					 CLUSTER_RECOVERY_HISTORY_NEED_RECORD);
		UT_ASSERT_EQ(event.locator.xid, transactions[remaining - 1].xid);
		UT_ASSERT_EQ(event.locator.tt_wrap, TT_WRAP_INVALID);
		UT_ASSERT(
			cluster_undo_record_uba_equal(event.locator.uba, uba_encode(257, remaining, 6, 0)));
		read_undo(remaining - 1, &record);
		UT_ASSERT(UBA_is_invalid(record.header.prev_uba)); /* every TX ends here, page does not */
		UT_ASSERT_EQ(
			cluster_recovery_page_history_accept(history, &transactions[remaining - 1], 0, &record),
			CLUSTER_RECOVERY_HISTORY_MORE);
	}
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_PAGE);
	UT_ASSERT_EQ(count_tuples(event.page), 0);
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_DRAINED);
	UT_ASSERT_EQ(memcmp(original.bytes, current_page.bytes, BLCKSZ), 0);
	cluster_recovery_page_history_destroy(history);
}

UT_TEST(test_transaction_predecessor_on_another_relation_is_not_inverse_applied)
{
	ClusterRecoveryPageHistory *history;
	ClusterRecoveryHistoryEvent event;
	ClusterRecoveryUndoRecord record;

	prepare_history(1);
	history = begin_history(128 * 1024);
	if (history == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_PAGE);
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_NEED_RECORD);
	read_undo(0, &record);
	record.header.flags &= ~UNDO_REC_FLAG_FIRST_IN_TX;
	record.directory.flags = record.header.flags;
	record.header.prev_uba = uba_encode(258, 9, 6, 0);
	UT_ASSERT_EQ(cluster_recovery_page_history_accept(history, &transactions[0], 0, &record),
				 CLUSTER_RECOVERY_HISTORY_MORE);
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_PAGE);
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_DRAINED);
	cluster_recovery_page_history_destroy(history);
}

UT_TEST(test_request_stays_exact_until_input_arrives)
{
	ClusterRecoveryPageHistory *history;
	ClusterRecoveryHistoryEvent first, again;

	prepare_history(1);
	history = begin_history(128 * 1024);
	if (history == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &first),
				 CLUSTER_RECOVERY_HISTORY_PAGE);
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &first),
				 CLUSTER_RECOVERY_HISTORY_NEED_RECORD);
	for (unsigned i = 0; i < 150; i++) {
		UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &again),
					 CLUSTER_RECOVERY_HISTORY_NEED_RECORD);
		UT_ASSERT_EQ(memcmp(&first.locator, &again.locator, sizeof(first.locator)), 0);
	}
	cluster_recovery_page_history_destroy(history);
}

UT_TEST(test_unknown_page_history_requires_a_base_not_an_empty_graph)
{
	ClusterRecoveryPageHistory *history;
	ClusterRecoveryHistoryEvent event;
	ClusterRecoveryUndoRecord record;

	prepare_history(1);
	history = begin_history(128 * 1024);
	if (history == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_PAGE);
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_NEED_RECORD);
	read_undo(0, &record);
	record.header.flags &= ~UNDO_REC_FLAG_HAS_ITL_HISTORY;
	record.header.payload_length = sizeof(UndoInsertPayload);
	record.directory.flags = record.header.flags;
	record.directory.record_length = sizeof(record.header) + record.header.payload_length;
	memset(&record.history, 0, sizeof(record.history));
	UT_ASSERT_EQ(cluster_recovery_page_history_accept(history, &transactions[0], 0, &record),
				 CLUSTER_RECOVERY_HISTORY_BASE_REQUIRED);
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_BASE_REQUIRED);
	UT_ASSERT(event.page == NULL);
	cluster_recovery_page_history_destroy(history);
}

UT_TEST(test_exact_namespace_and_canonical_tt_identity_are_required)
{
	for (unsigned variant = 0; variant < 6; variant++) {
		ClusterRecoveryPageHistory *history;
		ClusterRecoveryHistoryEvent event;
		ClusterRecoveryUndoRecord record;
		ClusterRecoveryTxnKey key;

		prepare_history(1);
		history = begin_history(128 * 1024);
		if (history == NULL)
			return;
		UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
					 CLUSTER_RECOVERY_HISTORY_PAGE);
		UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
					 CLUSTER_RECOVERY_HISTORY_NEED_RECORD);
		read_undo(0, &record);
		key = transactions[0];
		switch (variant) {
		case 0:
			key.database_incarnation++;
			break;
		case 1:
			key.system_identifier++;
			break;
		case 2:
			key.xid++;
			break;
		case 3:
			key.slot_wrap++;
			break;
		case 4:
			key.segment_generation++;
			break;
		case 5:
			record.header.target_locator.relNumber++;
			break;
		}
		UT_ASSERT_EQ(cluster_recovery_page_history_accept(history, &key, 0, &record),
					 CLUSTER_RECOVERY_HISTORY_IDENTITY);
		UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
					 CLUSTER_RECOVERY_HISTORY_IDENTITY);
		UT_ASSERT(event.page == NULL);
		cluster_recovery_page_history_destroy(history);
	}
}

UT_TEST(test_original_role_wrap_scn_and_tuple_checks_are_not_weakened)
{
	for (unsigned variant = 0; variant < 4; variant++) {
		ClusterRecoveryPageHistory *history;
		ClusterRecoveryHistoryEvent event;
		ClusterRecoveryUndoRecord record;
		ClusterItlSlotData *slot;
		HistoryPage original;

		prepare_history(1);
		slot = &ClusterPageGetItlSlots(current_page.bytes)[0];
		switch (variant) {
		case 0:
			slot->wrap++;
			break;
		case 1:
			slot->write_scn++;
			break;
		case 2:
			slot->flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
			break;
		case 3: {
			HeapTupleHeader tuple = (HeapTupleHeader)PageGetItem(
				current_page.bytes, PageGetItemId(current_page.bytes, 1));
			HeapTupleHeaderSetXmin(tuple, 99999);
			break;
		}
		}
		original = current_page;
		history = begin_history(128 * 1024);
		if (history == NULL)
			return;
		UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
					 CLUSTER_RECOVERY_HISTORY_PAGE);
		UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
					 CLUSTER_RECOVERY_HISTORY_NEED_RECORD);
		read_undo(0, &record);
		UT_ASSERT_EQ(cluster_recovery_page_history_accept(history, &transactions[0], 0, &record),
					 CLUSTER_RECOVERY_HISTORY_CORRUPT);
		UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
					 CLUSTER_RECOVERY_HISTORY_CORRUPT);
		UT_ASSERT(event.page == NULL);
		UT_ASSERT_EQ(memcmp(original.bytes, current_page.bytes, BLCKSZ), 0);
		cluster_recovery_page_history_destroy(history);
	}
}

UT_TEST(test_no_uba_nonfree_slot_is_not_silently_skipped)
{
	ClusterRecoveryPageHistory *history;
	ClusterRecoveryHistoryEvent event;

	prepare_history(1);
	ClusterPageGetItlSlots(current_page.bytes)[0].undo_segment_head = (UBA){ { 0, 0 } };
	history = begin_history(128 * 1024);
	if (history == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_PAGE);
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_BASE_REQUIRED);
	cluster_recovery_page_history_destroy(history);
}

UT_TEST(test_empty_page_drain_does_not_require_a_synthetic_record)
{
	ClusterRecoveryPageHistory *history;
	ClusterRecoveryHistoryEvent event;

	prepare_history(0);
	history = begin_history(128 * 1024);
	if (history == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_PAGE);
	UT_ASSERT_EQ(count_tuples(event.page), 0);
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_DRAINED);
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_DRAINED);
	cluster_recovery_page_history_destroy(history);
}

UT_TEST(test_bad_arguments_and_unsolicited_record_refuse)
{
	ClusterRecoveryPageHistory *history = (void *)1;
	ClusterRecoveryUndoRecord record;

	prepare_history(1);
	UT_ASSERT_EQ(cluster_recovery_page_history_begin(&space, MAIN_FORKNUM, 37, current_page.bytes,
													 BLCKSZ - 1, 128 * 1024, &history),
				 CLUSTER_RECOVERY_HISTORY_INVALID);
	UT_ASSERT(history == NULL);
	history = begin_history(128 * 1024);
	if (history == NULL)
		return;
	read_undo(0, &record);
	UT_ASSERT_EQ(cluster_recovery_page_history_accept(history, &transactions[0], 0, &record),
				 CLUSTER_RECOVERY_HISTORY_INVALID);
	cluster_recovery_page_history_destroy(history);
}

UT_TEST(test_retired_history_watermark_requires_a_durable_base)
{
	ClusterRecoveryPageHistory *history;
	ClusterRecoveryHistoryEvent event;

	prepare_history(0);
	ClusterPageGetItlHeader(current_page.bytes)->itl_recycle_watermark_scn = 91;
	history = begin_history(128 * 1024);
	if (history == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_PAGE);
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_BASE_REQUIRED);
	UT_ASSERT(event.page == NULL);
	cluster_recovery_page_history_destroy(history);
}

UT_TEST(test_memory_limit_never_truncates_page_history)
{
	ClusterRecoveryPageHistory *history;
	ClusterRecoveryHistoryEvent event;
	ClusterRecoveryUndoRecord record;

	prepare_history(2);
	/* One retained visit fits. A second version must refuse, not report END. */
	history = begin_history(sizeof(ClusterRecoveryPageHistory) + 2 * BLCKSZ
							+ sizeof(RecoveryHistoryVisit));
	if (history == NULL)
		return;
	for (unsigned remaining = 2; remaining > 0; remaining--) {
		UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
					 CLUSTER_RECOVERY_HISTORY_PAGE);
		UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
					 CLUSTER_RECOVERY_HISTORY_NEED_RECORD);
		read_undo(remaining - 1, &record);
		UT_ASSERT_EQ(
			cluster_recovery_page_history_accept(history, &transactions[remaining - 1], 0, &record),
			remaining == 2 ? CLUSTER_RECOVERY_HISTORY_MORE : CLUSTER_RECOVERY_HISTORY_CAPACITY);
	}
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_CAPACITY);
	UT_ASSERT(event.page == NULL);
	cluster_recovery_page_history_destroy(history);
}

UT_TEST(test_page_path_cannot_revisit_a_physical_record_as_another_transaction)
{
	ClusterRecoveryPageHistory *history;
	ClusterRecoveryHistoryEvent event;
	ClusterRecoveryUndoRecord record, older;

	prepare_history(2);
	history = begin_history(128 * 1024);
	if (history == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_PAGE);
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_NEED_RECORD);
	read_undo(1, &record);
	record.history.entries[0].prior.undo_segment_head = record.address;
	memcpy(record.payload + record.body_length, &record.history, sizeof(record.history));
	UT_ASSERT_EQ(cluster_recovery_page_history_accept(history, &transactions[1], 0, &record),
				 CLUSTER_RECOVERY_HISTORY_MORE);
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_PAGE);
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_NEED_RECORD);
	UT_ASSERT_EQ(event.locator.xid, transactions[0].xid);
	read_undo(0, &older);
	older.address = record.address;
	UT_ASSERT_EQ(cluster_recovery_page_history_accept(history, &transactions[0], 0, &older),
				 CLUSTER_RECOVERY_HISTORY_CYCLE);
	UT_ASSERT_EQ(cluster_recovery_page_history_next(history, &event),
				 CLUSTER_RECOVERY_HISTORY_CYCLE);
	UT_ASSERT(event.page == NULL);
	cluster_recovery_page_history_destroy(history);
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(test_prior_transaction_is_a_new_dependency_not_prev_uba);
	UT_RUN(test_transaction_predecessor_on_another_relation_is_not_inverse_applied);
	UT_RUN(test_request_stays_exact_until_input_arrives);
	UT_RUN(test_unknown_page_history_requires_a_base_not_an_empty_graph);
	UT_RUN(test_exact_namespace_and_canonical_tt_identity_are_required);
	UT_RUN(test_original_role_wrap_scn_and_tuple_checks_are_not_weakened);
	UT_RUN(test_no_uba_nonfree_slot_is_not_silently_skipped);
	UT_RUN(test_empty_page_drain_does_not_require_a_synthetic_record);
	UT_RUN(test_bad_arguments_and_unsolicited_record_refuse);
	UT_RUN(test_retired_history_watermark_requires_a_durable_base);
	UT_RUN(test_memory_limit_never_truncates_page_history);
	UT_RUN(test_page_path_cannot_revisit_a_physical_record_as_another_transaction);
	UT_DONE();
	return ut_failed_count != 0;
}
