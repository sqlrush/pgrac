/* Historical WAL heads through the original cold inventory and TX graph.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#include "cluster/cluster_recovery_undo_store.h"
#include "cluster/cluster_undo_segment_init.h"

static ClusterRecoveryGraphResult
wal_root_no_read(void *arg, const ClusterRecoveryTxnKey *key, UBA address,
				 ClusterRecoveryUndoRecord *record, uint32 *generation)
{
	return CLUSTER_RECOVERY_GRAPH_IO;
}

static ClusterRecoveryReferenceGraph *
wal_root_empty_graph(size_t budget)
{
	ClusterRecoveryReferenceGraph *graph = NULL;
	UT_ASSERT_EQ(cluster_recovery_reference_graph_begin(budget, wal_root_no_read, NULL, &graph),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	return graph;
}

static ClusterRecoveryGraphResult
wal_root_seed(ClusterRecoveryNativeInventory *inventory, ClusterRecoveryReferenceGraph *graph,
			  ClusterRecoveryNativeInventoryDiag *diag)
{
	ClusterRecoveryGraphResult result = CLUSTER_RECOVERY_GRAPH_MORE;
	for (unsigned i = 0; i < 10000 && result == CLUSTER_RECOVERY_GRAPH_MORE; i++)
		result = cluster_recovery_native_inventory_seed_graph_step(inventory, graph, diag);
	return result;
}

static void
wal_root_segment(int fd, unsigned node)
{
	UndoSegmentHeaderData header;
	UndoBlockHeader block = { 0 };
	UndoRecordHeader record = { 0 };
	UndoSlotDirEntry directory = { 0 };
	UndoInsertPayload payload = { 32, 0 };
	uint32 segment = node * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	char path[96], page[BLCKSZ] = { 0 };
	int file;

	snprintf(path, sizeof(path), "pg_undo/instance_%u", node);
	UT_ASSERT_EQ(mkdirat(fd, path, 0700), 0);
	cluster_undo_segment_make_header_bytes(segment, node + 1, (char *)&header);
	/* The disk slot is already another transaction. Its generation and
     * current xid must not replace the historical WAL binding. */
	header.tt_slots[0].xid = 4195144;
	header.tt_slots[0].wrap = 2;
	header.tt_slots[0].status = TT_SLOT_ACTIVE;
	block.magic = PGRAC_UNDO_BLOCK_MAGIC;
	block.block_version = UNDO_BLOCK_VERSION_1;
	block.slot_count = 1;
	block.free_offset = sizeof(block) + sizeof(record) + sizeof(payload);
	record.record_type = UNDO_RECORD_INSERT;
	record.flags = UNDO_REC_FLAG_FIRST_IN_TX;
	record.payload_length = sizeof(payload);
	record.xid = 4195136;
	record.origin_node_id = node;
	record.tt_slot_segment_id = segment;
	record.tt_slot_id = 1;
	record.tt_wrap_plus1 = 2;
	record.write_scn = 3;
	record.target_locator = (RelFileLocator){ 1663, 5, 16385 };
	record.target_fork = MAIN_FORKNUM;
	record.target_block = 0;
	record.target_offset = 1;
	directory.record_offset = sizeof(block);
	directory.record_length = sizeof(record) + sizeof(payload);
	directory.record_type = record.record_type;
	directory.flags = record.flags;
	memcpy(page, &block, sizeof(block));
	memcpy(page + sizeof(block), &record, sizeof(record));
	memcpy(page + sizeof(block) + sizeof(record), &payload, sizeof(payload));
	memcpy(page + UNDO_SLOT_DIR_OFFSET(0), &directory, sizeof(directory));
	snprintf(path, sizeof(path), "pg_undo/instance_%u/seg_%u.dat", node, segment);
	file = openat(fd, path, O_CREAT | O_EXCL | O_WRONLY, 0600);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(write(file, &header, BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(write(file, page, BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(file), 0);
}

UT_TEST(test_wal_heads_seed_reused_slots_through_real_files)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryUndoStore *store = NULL;
	ClusterRecoveryReferenceGraph *graph;
	ClusterRecoveryRootsEvent event;
	ClusterRecoveryUndoEntry slot;
	ClusterRecoveryGraphRecord record;
	ClusterRecoveryGraphResult result = CLUSTER_RECOVERY_GRAPH_MORE;
	ClusterRecoveryRootsResult root_result = CLUSTER_RECOVERY_ROOTS_MORE;
	unsigned records = 0;
	char path[MAXPGPATH], name[96];
	int fd;

	inventory_extra = 5;
	inventory_binding_case = 0;
	inputs = inventory_fixture(false, false);
	roots = inventory_roots(inputs, path, &fd, false);
	UT_ASSERT_EQ(mkdirat(fd, "pg_undo", 0700), 0);
	wal_root_segment(fd, 0);
	wal_root_segment(fd, 127);
	/* Capture the physical namespace only after fixture file creation. */
	cluster_recovery_root_set_destroy(roots);
	UT_ASSERT_EQ(cluster_recovery_root_set_begin(
					 fd, TEST_SYSID,
					 cluster_wal_inputs_at_v1(inputs, 0)->source.claim.database_incarnation,
					 cluster_wal_inputs_at_v1(inputs, 0)->source.claim.identity.storage_uuid,
					 1024 * 1024, &roots),
				 CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_collect_roots(inputs, roots, 1024 * 1024,
																 &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	for (unsigned i = 0; i < 1000 && root_result < CLUSTER_RECOVERY_ROOTS_OBSERVED; i++)
		root_result = cluster_recovery_root_set_step(roots, &event);
	UT_ASSERT_EQ(root_result, CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT_EQ(cluster_recovery_undo_store_begin(roots, 1024 * 1024, &store),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	UT_ASSERT_EQ(cluster_recovery_undo_store_graph_begin(
					 store, TEST_SYSID,
					 cluster_wal_inputs_at_v1(inputs, 0)->source.claim.database_incarnation,
					 1024 * 1024),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	for (unsigned i = 0; i < 1000 && result < CLUSTER_RECOVERY_GRAPH_DRAINED; i++)
		result = cluster_recovery_undo_store_next(store, &slot);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_GRAPH_DRAINED);
	graph = cluster_recovery_undo_store_graph(store);
	UT_ASSERT(graph != NULL);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_count(graph), 0);
	UT_ASSERT_EQ(wal_root_seed(inventory, graph, &diag), CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(diag.tt_roots_added, 2);
	result = CLUSTER_RECOVERY_GRAPH_MORE;
	for (unsigned i = 0; graph != NULL && i < 1000 && result < CLUSTER_RECOVERY_GRAPH_DRAINED;
		 i++) {
		result = cluster_recovery_reference_graph_step(graph, &record);
		if (result == CLUSTER_RECOVERY_GRAPH_RECORD) {
			records++;
			UT_ASSERT_EQ(record.transaction.xid, 4195136);
			UT_ASSERT_EQ(record.transaction.slot_wrap, 1);
			UT_ASSERT_EQ(record.transaction.segment_generation, 0);
			UT_ASSERT_EQ(record.record->header.xid, 4195136);
		}
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(records, 2);
	UT_ASSERT(cluster_recovery_reference_graph_observed(graph));
	UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	cluster_recovery_native_inventory_destroy(inventory);
	UT_ASSERT(!cluster_recovery_reference_graph_observed(graph));
	cluster_recovery_undo_store_destroy(store);
	for (unsigned i = 0; i < 2; i++) {
		unsigned node = i == 0 ? 0 : 127;
		snprintf(name, sizeof(name), "pg_undo/instance_%u/seg_%u.dat", node,
				 node * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1);
		UT_ASSERT_EQ(unlinkat(fd, name, 0), 0);
		snprintf(name, sizeof(name), "pg_undo/instance_%u", node);
		UT_ASSERT_EQ(unlinkat(fd, name, AT_REMOVEDIR), 0);
	}
	UT_ASSERT_EQ(unlinkat(fd, "pg_undo", AT_REMOVEDIR), 0);
	inventory_roots_done(roots, fd, path);
	cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_wal_heads_require_prior_same_source_identity)
{
	static const unsigned cases[] = { 1, 2, 3, 4, 7, 8 };
	for (unsigned i = 0; i < lengthof(cases); i++) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryGraphRecord record;
		ClusterRecoveryReferenceGraph *graph = wal_root_empty_graph(1024 * 1024);
		inventory_extra = 5;
		inventory_binding_case = cases[i];
		inputs = inventory_fixture(false, false);
		UT_ASSERT_EQ(
			cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		UT_ASSERT_EQ(wal_root_seed(inventory, graph, &diag), CLUSTER_RECOVERY_GRAPH_IDENTITY);
		UT_ASSERT_EQ(diag.tt_head_binding_missing, 1);
		UT_ASSERT(cluster_recovery_reference_graph_step(graph, &record)
				  > CLUSTER_RECOVERY_GRAPH_DRAINED);
		UT_ASSERT_EQ(wal_root_seed(inventory, graph, &diag), CLUSTER_RECOVERY_GRAPH_IDENTITY);
		cluster_recovery_native_inventory_destroy(inventory);
		cluster_recovery_reference_graph_destroy(graph);
		cold_census_done(&inputs);
	}
	inventory_extra = inventory_binding_case = 0;
}

UT_TEST(test_wal_head_seeding_preserves_effect_cursor)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeEffect effect;
	ClusterRecoveryReferenceGraph *graph = wal_root_empty_graph(1024 * 1024);
	unsigned undo = 0, messages = 0, primary = 0;
	inventory_extra = 5;
	inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT);
	UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_TERMINAL);
	UT_ASSERT_EQ(wal_root_seed(inventory, graph, &diag), CLUSTER_RECOVERY_GRAPH_DRAINED);
	while (cluster_recovery_native_inventory_next(inventory, &effect)
		   == CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT) {
		undo += effect.kind == CLUSTER_RECOVERY_NATIVE_UNDO;
		messages += effect.kind == CLUSTER_RECOVERY_NATIVE_INVALIDATION;
		primary += effect.kind == CLUSTER_RECOVERY_NATIVE_TERMINAL;
	}
	UT_ASSERT_EQ(undo, 4);
	UT_ASSERT_EQ(messages, 4);
	UT_ASSERT_EQ(primary, 1);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_count(graph), 2);
	cluster_recovery_native_inventory_destroy(inventory);
	cluster_recovery_reference_graph_destroy(graph);
	cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_wal_head_final_recheck_and_destroy_revoke_graph)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryGraphRecord record;
		ClusterRecoveryReferenceGraph *graph = wal_root_empty_graph(1024 * 1024);
		bool threw = false;
		inventory_extra = 5;
		inputs = inventory_fixture(false, false);
		UT_ASSERT_EQ(
			cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		UT_ASSERT_EQ(wal_root_seed(inventory, graph, &diag), CLUSTER_RECOVERY_GRAPH_DRAINED);
		if (mode == 3) {
			cluster_recovery_native_inventory_destroy(inventory);
			inventory = NULL;
		} else {
			if (mode == 0) {
				inventory_fault = 0;
				inventory_drift();
			} else if (mode == 1) {
				inventory_fault = 4;
				inventory_census_calls = 0;
				inventory_between_passes = inventory_drift;
			}
			PG_TRY();
			{
				UT_ASSERT_NE(cluster_recovery_native_inventory_recheck(mode == 2 ? NULL : inputs,
																	   inventory, &diag),
							 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
			}
			PG_CATCH();
			{
				threw = true;
			}
			PG_END_TRY();
			UT_ASSERT_EQ(threw, mode == 1);
			inventory_between_passes = NULL;
		}
		UT_ASSERT_EQ(cluster_recovery_reference_graph_step(graph, &record),
					 CLUSTER_RECOVERY_GRAPH_INVALID);
		cluster_recovery_native_inventory_destroy(inventory);
		cluster_recovery_reference_graph_destroy(graph);
		cold_census_done(&inputs);
	}
	inventory_extra = 0;
}

UT_TEST(test_wal_heads_reject_late_graph_and_partial_capacity)
{
	for (unsigned mode = 0; mode < 2; mode++) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryGraphRecord record;
		ClusterRecoveryReferenceGraph *graph
			= wal_root_empty_graph(mode == 0 ? 1024 * 1024 : 20000);
		inventory_extra = 5;
		inputs = inventory_fixture(false, false);
		UT_ASSERT_EQ(
			cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		if (mode == 0)
			UT_ASSERT_EQ(cluster_recovery_reference_graph_step(graph, &record),
						 CLUSTER_RECOVERY_GRAPH_DRAINED);
		UT_ASSERT_EQ(wal_root_seed(inventory, graph, &diag),
					 mode == 0 ? CLUSTER_RECOVERY_GRAPH_INVALID : CLUSTER_RECOVERY_GRAPH_CAPACITY);
		UT_ASSERT(!cluster_recovery_reference_graph_observed(graph));
		cluster_recovery_native_inventory_destroy(inventory);
		cluster_recovery_reference_graph_destroy(graph);
		cold_census_done(&inputs);
	}
	inventory_extra = 0;
}

UT_TEST(test_wal_head_bounded_search_and_lifecycle_barriers)
{
	/* The real-file missing/replaced bindings are tested above. This
     * scheduling boundary supplies a long list of original parsed records;
     * it does not claim that the constructed list is a cold input proof. */
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryReferenceGraph *graph = wal_root_empty_graph(1024 * 1024);
	NativeInventoryRecord *binding, *head;
	ClusterRecoveryNativeEffect effect;
	ClusterRecoveryGraphResult result;
	unsigned calls = 0;
	inventory_extra = 5;
	inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	binding = inventory->head->next;
	head = binding->next;
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(binding->scan, &effect),
				 CLUSTER_RECOVERY_NATIVE_EFFECT);
	for (unsigned i = 0; i < 260; i++) {
		NativeInventoryRecord *copy = calloc(1, sizeof(*copy));
		DecodedXLogRecord decoded = { 0 };
		XLogReaderState reader = { .record = &decoded };
		decoded.header = effect.record->header;
		decoded.lsn = reader.ReadRecPtr = effect.record->read_lsn;
		decoded.next_lsn = reader.EndRecPtr = effect.record->end_lsn;
		decoded.main_data = (char *)effect.record->payload;
		decoded.main_data_len = effect.record->payload_length;
		decoded.max_block_id = -1;
		reader.system_identifier = effect.record->source.claim.identity.system_identifier;
		UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&reader, &effect.record->source,
														&effect.record->cut, 65536, &copy->scan),
					 CLUSTER_RECOVERY_NATIVE_MORE);
		copy->source_index = binding->source_index;
		copy->next = head;
		binding->next = copy;
		binding = copy;
	}
	do {
		uint64 before = diag.tt_binding_records_examined;
		result = cluster_recovery_native_inventory_seed_graph_step(inventory, graph, &diag);
		UT_ASSERT(diag.tt_binding_records_examined - before <= 128);
		calls++;
	} while (result == CLUSTER_RECOVERY_GRAPH_MORE && calls < 1000);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT(calls >= 4);
	UT_ASSERT_EQ(diag.tt_roots_added, 2);
	cluster_recovery_native_inventory_destroy(inventory);
	cluster_recovery_reference_graph_destroy(graph);
	cold_census_done(&inputs);
	inventory_extra = 0;
}
