/* Actual namespace/segment files through the original TT/UBA graph readers.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
#define main namespace_fixture_main
#include "test_cluster_recovery_namespace.c"
#undef main
#include "cluster/cluster_recovery_undo_store.h"
#include "../../common/cluster_undo_segment_init.c"
#include "../../backend/cluster/cluster_uba.c"
#include "test_cluster_heap_small_scn.inc"
#include "../../backend/cluster/cluster_recovery_undo_scan.c"
#include "../../backend/cluster/cluster_recovery_reference_graph.c"
#define pread namespace_test_pread
#include "../../backend/cluster/cluster_recovery_root_set.c"
#undef pread
#include "../../backend/cluster/cluster_recovery_undo_store.c"

static const uint8 store_uuid[16] = { 9, 3, 7 };

static ClusterRecoveryTxnKey
store_key(void)
{
	ClusterRecoveryTxnKey key = { 0 };

	key.system_identifier = 817;
	key.database_incarnation = 91;
	key.origin_node_id = 1;
	key.segment_id = 257;
	key.segment_generation = 0;
	key.slot_offset = 6;
	key.slot_wrap = 1;
	key.xid = 4195136;
	return key;
}

static void
store_record(char page[BLCKSZ], uint32 relation, UBA previous)
{
	UndoBlockHeader block = { 0 };
	UndoSlotDirEntry directory = { 0 };
	UndoRecordHeader header = { 0 };
	UndoInsertPayload payload = { 32, 0 };
	ClusterRecoveryTxnKey key = store_key();

	memset(page, 0, BLCKSZ);
	block.magic = PGRAC_UNDO_BLOCK_MAGIC;
	block.block_version = UNDO_BLOCK_VERSION_1;
	block.slot_count = 1;
	block.free_offset = sizeof(block) + sizeof(header) + sizeof(payload);
	header.record_type = UNDO_RECORD_INSERT;
	header.flags = UBA_is_invalid(previous) ? UNDO_REC_FLAG_FIRST_IN_TX : 0;
	header.payload_length = sizeof(payload);
	header.xid = key.xid;
	header.origin_node_id = key.origin_node_id;
	header.tt_slot_segment_id = key.segment_id;
	header.tt_slot_id = key.slot_offset + 1;
	header.tt_wrap_plus1 = key.slot_wrap + 1;
	header.prev_uba = previous;
	header.write_scn = 3;
	header.target_locator = (RelFileLocator){ 1663, 5, relation };
	header.target_fork = MAIN_FORKNUM;
	header.target_block = 0;
	header.target_offset = 1;
	directory.record_offset = sizeof(block);
	directory.record_length = sizeof(header) + sizeof(payload);
	directory.record_type = header.record_type;
	directory.flags = header.flags;
	memcpy(page, &block, sizeof(block));
	memcpy(page + sizeof(block), &header, sizeof(header));
	memcpy(page + sizeof(block) + sizeof(header), &payload, sizeof(payload));
	memcpy(page + UNDO_SLOT_DIR_OFFSET(0), &directory, sizeof(directory));
}

static void
store_file(int fd, uint32 segment, uint32 generation, UBA head, uint32 relation, UBA previous)
{
	UndoSegmentHeaderData header;
	ClusterRecoveryTxnKey key = store_key();
	char page[BLCKSZ], path[96];
	int file;

	cluster_undo_segment_make_header_bytes(segment, 2, (char *)&header);
	header.wrap_count = generation;
	if (!UBA_is_invalid(head)) {
		TTSlot *slot = &header.tt_slots[key.slot_offset];

		slot->xid = key.xid;
		slot->wrap = key.slot_wrap;
		slot->status = TT_SLOT_COMMITTED;
		slot->commit_scn = 100;
		slot->flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		slot->first_undo_block = head;
	}
	/* An unreferenced raw terminal is still an inventory item. */
	header.tt_slots[47].status = TT_SLOT_RECYCLABLE;
	header.tt_slots[47].xid = 4195144;
	header.tt_slots[47].commit_scn = InvalidScn;
	store_record(page, relation, previous);
	snprintf(path, sizeof(path), "pg_undo/instance_1/seg_%u.dat", segment);
	file = openat(fd, path, O_CREAT | O_EXCL | O_WRONLY, 0600);
	if (file < 0 || write(file, &header, BLCKSZ) != BLCKSZ || write(file, page, BLCKSZ) != BLCKSZ
		|| close(file) != 0)
		abort();
}

static int
store_files(void)
{
	int fd = new_root();
	UBA first = uba_encode(257, 1, 6, 0), last = uba_encode(258, 1, 6, 0);

	UT_ASSERT_EQ(mkdirat(fd, "pg_undo", 0700), 0);
	UT_ASSERT_EQ(mkdirat(fd, "pg_undo/instance_1", 0700), 0);
	store_file(fd, 257, 0, last, 16385, (UBA){ { 0, 0 } });
	store_file(fd, 258, 7, (UBA){ { 0, 0 } }, 16388, first);
	return fd;
}

static ClusterRecoveryRootSet *
store_roots(int fd)
{
	ClusterRecoveryRootSet *roots = NULL;
	ClusterRecoveryRootsEvent event;
	ClusterRecoveryRootsResult result;

	result = cluster_recovery_root_set_begin(fd, 817, 91, store_uuid, 1024 * 1024, &roots);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_ROOTS_MORE);
	for (unsigned i = 0; roots != NULL && i < 1000; i++) {
		result = cluster_recovery_root_set_step(roots, &event);
		if (result >= CLUSTER_RECOVERY_ROOTS_OBSERVED)
			break;
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_ROOTS_OBSERVED);
	return roots;
}

static ClusterRecoveryUndoStore *
open_store(ClusterRecoveryRootSet *roots)
{
	ClusterRecoveryUndoStore *store = NULL;

	UT_ASSERT_EQ(cluster_recovery_undo_store_begin(roots, 1024 * 1024, &store),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	return store;
}

static void
assert_store_begin_is_bounded(ClusterRecoveryRootSet *roots)
{
	ClusterRecoveryUndoStore *store = NULL;
	unsigned calls = 0;
	ClusterRecoveryUndoEntry entry;
	ClusterRecoveryGraphResult result;

	UT_ASSERT_EQ(cluster_recovery_undo_store_begin(roots, 1024 * 1024, &store),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	while (store != NULL && calls++ < 1000
		   && (result = cluster_recovery_undo_store_next(store, &entry))
				  == CLUSTER_RECOVERY_GRAPH_MORE)
		;
	UT_ASSERT(store != NULL);
	UT_ASSERT(calls >= 1);
	/* The fixture has two undo files; indexing and file-array assembly are
	 * deliberately returned as MORE before the first TT slot. */
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_GRAPH_RECORD);
	cluster_recovery_undo_store_destroy(store);
}

static ClusterRecoveryGraphResult
inventory(ClusterRecoveryUndoStore *store, ClusterRecoveryReferenceGraph *graph, unsigned *slots)
{
	for (unsigned i = 0; i < 1000; i++) {
		ClusterRecoveryUndoEntry entry;
		ClusterRecoveryGraphResult result = cluster_recovery_undo_store_next(store, &entry);

		if (result == CLUSTER_RECOVERY_GRAPH_RECORD) {
			(*slots)++;
			if (graph != NULL && !UBA_is_invalid(entry.slot.first_undo_block)) {
				ClusterRecoveryTxnKey key = store_key();

				key.segment_id = entry.segment_id;
				key.segment_generation = entry.segment_generation;
				key.slot_offset = entry.slot_offset;
				key.slot_wrap = entry.slot.wrap;
				key.xid = entry.slot.xid;
				UT_ASSERT_EQ(
					cluster_recovery_reference_graph_add(graph, &key, entry.slot.first_undo_block),
					CLUSTER_RECOVERY_GRAPH_MORE);
			}
		} else if (result != CLUSTER_RECOVERY_GRAPH_MORE)
			return result;
	}
	return CLUSTER_RECOVERY_GRAPH_MORE;
}

UT_TEST(test_real_untouched_segment_files_feed_all_slots_and_the_full_transaction_chain)
{
	int fd = store_files();
	ClusterRecoveryRootSet *roots = store_roots(fd);
	ClusterRecoveryUndoStore *store = open_store(roots);
	ClusterRecoveryReferenceGraph *graph = NULL;
	unsigned slots = 0, records = 0, zero_generation = 0, other_generation = 0;
	ClusterRecoveryGraphResult result = CLUSTER_RECOVERY_GRAPH_INVALID;
	uint64 relation_sum = 0;

	if (store == NULL)
		goto done;
	cluster_recovery_undo_store_destroy(store);
	assert_store_begin_is_bounded(roots);
	store = open_store(roots);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_begin(
					 1024 * 1024, cluster_recovery_undo_store_read, store, &graph),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	UT_ASSERT_EQ(inventory(store, graph, &slots), CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(slots, 96);
	UT_ASSERT(cluster_recovery_undo_store_header(store, 257) != NULL);
	UT_ASSERT_EQ(cluster_recovery_undo_store_header(store, 258)->wrap_count, 7);
	for (unsigned i = 0; i < 1000; i++) {
		ClusterRecoveryGraphRecord record;

		result = cluster_recovery_reference_graph_step(graph, &record);
		if (result == CLUSTER_RECOVERY_GRAPH_RECORD) {
			records++;
			relation_sum += record.record->header.target_locator.relNumber;
			zero_generation += record.record_segment_generation == 0;
			other_generation += record.record_segment_generation == 7;
			UT_ASSERT_EQ(record.transaction.segment_generation, 0);
		} else if (result != CLUSTER_RECOVERY_GRAPH_MORE)
			break;
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(records, 2);
	UT_ASSERT_EQ(relation_sum, 16385 + 16388);
	UT_ASSERT_EQ(zero_generation, 1);
	UT_ASSERT_EQ(other_generation, 1);
	UT_ASSERT_EQ(cluster_recovery_undo_store_recheck(store), CLUSTER_RECOVERY_GRAPH_MORE);
	UT_ASSERT_EQ(cluster_recovery_undo_store_recheck(store), CLUSTER_RECOVERY_GRAPH_MORE);
	UT_ASSERT_EQ(cluster_recovery_undo_store_recheck(store), CLUSTER_RECOVERY_GRAPH_DRAINED);
done:
	cluster_recovery_reference_graph_destroy(graph);
	cluster_recovery_undo_store_destroy(store);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_missing_segment_or_short_or_failed_read_is_not_an_empty_chain)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		int fd = store_files();
		ClusterRecoveryRootSet *roots = store_roots(fd);
		ClusterRecoveryUndoStore *store = open_store(roots);
		ClusterRecoveryUndoRecord record;
		ClusterRecoveryTxnKey key = store_key();
		uint32 generation = 123;
		unsigned slots = 0;

		if (store != NULL) {
			UT_ASSERT_EQ(inventory(store, NULL, &slots), CLUSTER_RECOVERY_GRAPH_DRAINED);
			inject_pread_fault = mode;
			UT_ASSERT_EQ(
				cluster_recovery_undo_store_read(
					store, &key, uba_encode(mode == 0 ? 259 : 258, 1, 6, 0), &record, &generation),
				CLUSTER_RECOVERY_GRAPH_IO);
			inject_pread_fault = 0;
			UT_ASSERT_EQ(generation, 0);
			UT_ASSERT(UBA_is_invalid(record.address));
		}
		cluster_recovery_undo_store_destroy(store);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_foreign_database_or_tt_identity_never_selects_a_local_record)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		int fd = store_files();
		ClusterRecoveryRootSet *roots = store_roots(fd);
		ClusterRecoveryUndoStore *store = open_store(roots);
		ClusterRecoveryUndoRecord record;
		ClusterRecoveryTxnKey key = store_key();
		uint32 generation;
		unsigned slots = 0;

		if (store != NULL) {
			UT_ASSERT_EQ(inventory(store, NULL, &slots), CLUSTER_RECOVERY_GRAPH_DRAINED);
			if (mode == 0)
				key.database_incarnation++;
			else if (mode == 1)
				key.segment_generation++;
			else if (mode == 2)
				key.xid++;
			else
				key.slot_wrap++;
			UT_ASSERT_EQ(cluster_recovery_undo_store_read(store, &key, uba_encode(258, 1, 6, 0),
														  &record, &generation),
						 CLUSTER_RECOVERY_GRAPH_IDENTITY);
			UT_ASSERT(UBA_is_invalid(record.address));
			UT_ASSERT_EQ(generation, 0);
		}
		cluster_recovery_undo_store_destroy(store);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_segment_header_changed_after_the_graph_invalidates_recheck)
{
	int fd = store_files();
	ClusterRecoveryRootSet *roots = store_roots(fd);
	ClusterRecoveryUndoStore *store = open_store(roots);
	unsigned slots = 0;
	uint32 generation = 99;
	int file;

	if (store != NULL) {
		UT_ASSERT_EQ(inventory(store, NULL, &slots), CLUSTER_RECOVERY_GRAPH_DRAINED);
		file = openat(fd, "pg_undo/instance_1/seg_257.dat", O_WRONLY);
		UT_ASSERT_EQ(pwrite(file, &generation, sizeof(generation),
							offsetof(UndoSegmentHeaderData, wrap_count)),
					 sizeof(generation));
		close(file);
		UT_ASSERT_EQ(cluster_recovery_undo_store_recheck(store), CLUSTER_RECOVERY_GRAPH_CHANGED);
		UT_ASSERT(cluster_recovery_undo_store_header(store, 257) == NULL);
	}
	cluster_recovery_undo_store_destroy(store);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_budget_and_unobserved_namespace_refuse_before_exporting_slots)
{
	int fd = store_files();
	ClusterRecoveryRootSet *roots = store_roots(fd);
	ClusterRecoveryUndoStore *store = (void *)1;

	UT_ASSERT_EQ(cluster_recovery_undo_store_begin(roots, 1, &store),
				 CLUSTER_RECOVERY_GRAPH_CAPACITY);
	UT_ASSERT(store == NULL);
	cluster_recovery_root_set_destroy(roots);
	roots = NULL;
	UT_ASSERT_EQ(cluster_recovery_root_set_begin(fd, 817, 91, store_uuid, 1024 * 1024, &roots),
				 CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT_EQ(cluster_recovery_undo_store_begin(roots, 1024 * 1024, &store),
				 CLUSTER_RECOVERY_GRAPH_INVALID);
	UT_ASSERT(store == NULL);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_real_untouched_segment_files_feed_all_slots_and_the_full_transaction_chain);
	UT_RUN(test_missing_segment_or_short_or_failed_read_is_not_an_empty_chain);
	UT_RUN(test_foreign_database_or_tt_identity_never_selects_a_local_record);
	UT_RUN(test_segment_header_changed_after_the_graph_invalidates_recheck);
	UT_RUN(test_budget_and_unobserved_namespace_refuse_before_exporting_slots);
	UT_DONE();
	return ut_failed_count != 0;
}
