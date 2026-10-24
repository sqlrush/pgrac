/* Real undo bytes through the cold parser and transaction dependency walker.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
#include "postgres.h"
#include "cluster/cluster_recovery_reference_graph.h"
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
#include "../../backend/cluster/cluster_recovery_undo_scan.c"
#include "../../backend/cluster/cluster_recovery_reference_graph.c"

typedef struct GraphRecordInput {
	UBA address;
	uint32 generation;
	char page[BLCKSZ];
} GraphRecordInput;

typedef struct GraphInput {
	GraphRecordInput records[260];
	unsigned count;
	unsigned calls;
	unsigned pending;
	unsigned fail_call;
	unsigned change_call;
	bool change_generation;
} GraphInput;

static GraphInput input;

static ClusterRecoveryTxnKey
transaction(void)
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
add_record(uint32 segment, uint32 blockno, uint32 relation, UBA previous)
{
	GraphRecordInput *record = &input.records[input.count++];
	UndoBlockHeader block = { 0 };
	UndoSlotDirEntry directory = { 0 };
	UndoRecordHeader header = { 0 };
	UndoInsertPayload payload = { 32, 0 };
	ClusterRecoveryTxnKey key = transaction();

	record->address = uba_encode(segment, blockno, key.slot_offset, 0);
	record->generation = segment == key.segment_id ? key.segment_generation : 7;
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
	header.write_scn = 1; /* Not a reason to stop traversal, whatever H is. */
	header.target_locator = (RelFileLocator){ 1663, 5, relation };
	header.target_fork = MAIN_FORKNUM;
	header.target_block = blockno;
	header.target_offset = 1;
	directory.record_offset = sizeof(block);
	directory.record_length = sizeof(header) + sizeof(payload);
	directory.record_type = header.record_type;
	directory.flags = header.flags;
	memcpy(record->page, &block, sizeof(block));
	memcpy(record->page + sizeof(block), &header, sizeof(header));
	memcpy(record->page + sizeof(block) + sizeof(header), &payload, sizeof(payload));
	memcpy(record->page + UNDO_SLOT_DIR_OFFSET(0), &directory, sizeof(directory));
}

static ClusterRecoveryGraphResult
read_record(void *arg, const ClusterRecoveryTxnKey *key, UBA address,
			ClusterRecoveryUndoRecord *out, uint32 *generation)
{
	GraphInput *store = arg;
	uint32 segment, blockno;
	uint16 slot, row;

	store->calls++;
	if (store->pending > 0) {
		store->pending--;
		return CLUSTER_RECOVERY_GRAPH_PENDING;
	}
	if (store->calls == store->fail_call)
		return CLUSTER_RECOVERY_GRAPH_IO;
	if (!uba_decode_record(address, &segment, &blockno, &slot, &row))
		return CLUSTER_RECOVERY_GRAPH_CORRUPT;
	for (unsigned i = 0; i < store->count; i++) {
		GraphRecordInput *record = &store->records[i];
		ClusterRecoveryUndoResult result;

		if (!cluster_undo_record_uba_equal(record->address, address))
			continue;
		result = cluster_recovery_undo_record_read(record->page, BLCKSZ, segment, blockno,
												   key->origin_node_id + 1, address, out);
		if (result != CLUSTER_RECOVERY_UNDO_ENTRY)
			return CLUSTER_RECOVERY_GRAPH_CORRUPT;
		*generation = record->generation;
		if (store->calls == store->change_call) {
			if (store->change_generation)
				(*generation)++;
			else
				out->header.target_locator.relNumber++;
		}
		return CLUSTER_RECOVERY_GRAPH_RECORD;
	}
	return CLUSTER_RECOVERY_GRAPH_IO;
}

static ClusterRecoveryReferenceGraph *
begin_graph(size_t budget)
{
	ClusterRecoveryReferenceGraph *graph = NULL;

	UT_ASSERT_EQ(cluster_recovery_reference_graph_begin(budget, read_record, &input, &graph),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	return graph;
}

/* An upper test bound detects a stalled implementation; it is not a product
 * history limit. One production call may not make two storage reads. */
static ClusterRecoveryGraphResult
drain(ClusterRecoveryReferenceGraph *graph, unsigned *records, uint64 *relation_sum)
{
	for (unsigned i = 0; i < 2000; i++) {
		ClusterRecoveryGraphRecord record;
		unsigned before = input.calls;
		ClusterRecoveryGraphResult result = cluster_recovery_reference_graph_step(graph, &record);

		UT_ASSERT(input.calls - before <= 1);
		if (result == CLUSTER_RECOVERY_GRAPH_RECORD) {
			(*records)++;
			*relation_sum += record.record->header.target_locator.relNumber;
			UT_ASSERT_EQ(record.record->header.write_scn, 1);
		} else if (result != CLUSTER_RECOVERY_GRAPH_MORE
				   && result != CLUSTER_RECOVERY_GRAPH_PENDING)
			return result;
	}
	return CLUSTER_RECOVERY_GRAPH_MORE;
}

UT_TEST(test_cross_relation_and_segment_chain_is_not_page_history)
{
	ClusterRecoveryTxnKey key = transaction();
	ClusterRecoveryReferenceGraph *graph;
	unsigned records = 0;
	uint64 sum = 0;

	memset(&input, 0, sizeof(input));
	add_record(257, 1, 16385, (UBA){ { 0, 0 } });
	add_record(258, 2, 16388, input.records[0].address);
	add_record(258, 3, 20001, input.records[1].address);
	graph = begin_graph(1024 * 1024);
	if (graph == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[2].address),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	UT_ASSERT_EQ(drain(graph, &records, &sum), CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(records, 3);
	UT_ASSERT_EQ(sum, 16385 + 16388 + 20001);
	UT_ASSERT_EQ(input.calls, 6); /* every node re-read, not just counted */
	UT_ASSERT_EQ(cluster_recovery_reference_graph_count(graph), 3);
	cluster_recovery_reference_graph_destroy(graph);
}

UT_TEST(test_multiple_roots_share_a_tail_without_a_false_cycle)
{
	ClusterRecoveryTxnKey key = transaction();
	ClusterRecoveryReferenceGraph *graph;
	unsigned records = 0;
	uint64 sum = 0;

	memset(&input, 0, sizeof(input));
	add_record(257, 1, 16385, (UBA){ { 0, 0 } });
	add_record(257, 2, 16385, input.records[0].address);
	add_record(258, 3, 20001, input.records[1].address);
	graph = begin_graph(1024 * 1024);
	if (graph == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[1].address), 0);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[2].address), 0);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[1].address), 0);
	UT_ASSERT_EQ(drain(graph, &records, &sum), CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(records, 3);
	UT_ASSERT_EQ(input.calls, 6);
	cluster_recovery_reference_graph_destroy(graph);
}

UT_TEST(test_active_path_cycle_and_self_edge_are_refused)
{
	for (unsigned variant = 0; variant < 2; variant++) {
		ClusterRecoveryTxnKey key = transaction();
		ClusterRecoveryReferenceGraph *graph;
		ClusterRecoveryGraphRecord out;
		unsigned records = 0;
		uint64 sum = 0;

		memset(&input, 0, sizeof(input));
		add_record(257, 1, 16385, uba_encode(variant == 0 ? 258 : 257, 1, 6, 0));
		add_record(258, 1, 16385, input.records[0].address);
		graph = begin_graph(1024 * 1024);
		if (graph == NULL)
			return;
		UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[0].address),
					 0);
		UT_ASSERT_EQ(drain(graph, &records, &sum), CLUSTER_RECOVERY_GRAPH_CYCLE);
		UT_ASSERT_EQ(cluster_recovery_reference_graph_step(graph, &out),
					 CLUSTER_RECOVERY_GRAPH_CYCLE);
		UT_ASSERT(out.record == NULL);
		cluster_recovery_reference_graph_destroy(graph);
	}
}

UT_TEST(test_same_segment_forward_edge_is_not_a_valid_predecessor)
{
	ClusterRecoveryTxnKey key = transaction();
	ClusterRecoveryReferenceGraph *graph;
	unsigned records = 0;
	uint64 sum = 0;

	memset(&input, 0, sizeof(input));
	add_record(257, 1, 16385, uba_encode(257, 2, 6, 0));
	add_record(257, 2, 16385, (UBA){ { 0, 0 } });
	graph = begin_graph(1024 * 1024);
	if (graph == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[0].address), 0);
	UT_ASSERT_EQ(drain(graph, &records, &sum), CLUSTER_RECOVERY_GRAPH_IDENTITY);
	cluster_recovery_reference_graph_destroy(graph);
}

UT_TEST(test_pending_keeps_the_same_unread_node)
{
	ClusterRecoveryTxnKey key = transaction();
	ClusterRecoveryReferenceGraph *graph;
	ClusterRecoveryGraphRecord out;
	unsigned records = 0;
	uint64 sum = 0;

	memset(&input, 0, sizeof(input));
	add_record(257, 1, 16385, (UBA){ { 0, 0 } });
	input.pending = 3;
	graph = begin_graph(1024 * 1024);
	if (graph == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[0].address), 0);
	for (unsigned i = 0; i < 3; i++) {
		UT_ASSERT_EQ(cluster_recovery_reference_graph_step(graph, &out),
					 CLUSTER_RECOVERY_GRAPH_PENDING);
		UT_ASSERT(out.record == NULL);
	}
	UT_ASSERT_EQ(drain(graph, &records, &sum), CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(records, 1);
	UT_ASSERT_EQ(input.calls, 5);
	cluster_recovery_reference_graph_destroy(graph);
}

UT_TEST(test_missing_record_and_second_read_failure_do_not_drain)
{
	for (unsigned variant = 0; variant < 2; variant++) {
		ClusterRecoveryTxnKey key = transaction();
		ClusterRecoveryReferenceGraph *graph;
		ClusterRecoveryGraphRecord out;
		unsigned records = 0;
		uint64 sum = 0;

		memset(&input, 0, sizeof(input));
		add_record(257, 1, 16385, (UBA){ { 0, 0 } });
		input.fail_call = variant + 1;
		graph = begin_graph(1024 * 1024);
		if (graph == NULL)
			return;
		UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[0].address),
					 0);
		UT_ASSERT_EQ(drain(graph, &records, &sum), CLUSTER_RECOVERY_GRAPH_IO);
		UT_ASSERT_EQ(cluster_recovery_reference_graph_step(graph, &out), CLUSTER_RECOVERY_GRAPH_IO);
		UT_ASSERT(out.record == NULL);
		cluster_recovery_reference_graph_destroy(graph);
	}
}

UT_TEST(test_reread_checks_bytes_and_physical_generation)
{
	for (unsigned variant = 0; variant < 2; variant++) {
		ClusterRecoveryTxnKey key = transaction();
		ClusterRecoveryReferenceGraph *graph;
		unsigned records = 0;
		uint64 sum = 0;

		memset(&input, 0, sizeof(input));
		add_record(258, 1, 16385, (UBA){ { 0, 0 } });
		input.change_call = 2;
		input.change_generation = variant != 0;
		graph = begin_graph(1024 * 1024);
		if (graph == NULL)
			return;
		UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[0].address),
					 0);
		UT_ASSERT_EQ(drain(graph, &records, &sum), CLUSTER_RECOVERY_GRAPH_CHANGED);
		cluster_recovery_reference_graph_destroy(graph);
	}
}

UT_TEST(test_exact_canonical_transaction_fields_must_match)
{
	for (unsigned variant = 0; variant < 5; variant++) {
		ClusterRecoveryTxnKey key = transaction();
		ClusterRecoveryReferenceGraph *graph;
		unsigned records = 0;
		uint64 sum = 0;

		memset(&input, 0, sizeof(input));
		add_record(257, 1, 16385, (UBA){ { 0, 0 } });
		switch (variant) {
		case 0:
			key.xid++;
			break;
		case 1:
			key.slot_wrap++;
			break;
		case 2:
			key.segment_id++;
			break;
		case 3:
			key.segment_generation++;
			break;
		case 4:
			key.slot_offset++;
			break;
		}
		graph = begin_graph(1024 * 1024);
		if (graph == NULL)
			return;
		if (variant == 4)
			UT_ASSERT_EQ(
				cluster_recovery_reference_graph_add(graph, &key, input.records[0].address),
				CLUSTER_RECOVERY_GRAPH_IDENTITY);
		else {
			UT_ASSERT_EQ(
				cluster_recovery_reference_graph_add(graph, &key, input.records[0].address), 0);
			UT_ASSERT_EQ(drain(graph, &records, &sum), CLUSTER_RECOVERY_GRAPH_IDENTITY);
		}
		cluster_recovery_reference_graph_destroy(graph);
	}
}

UT_TEST(test_capacity_is_not_a_truncated_success)
{
	ClusterRecoveryTxnKey key = transaction();
	ClusterRecoveryReferenceGraph *graph;
	unsigned records = 0;
	uint64 sum = 0;

	memset(&input, 0, sizeof(input));
	for (unsigned i = 0; i < 32; i++)
		add_record(257, i + 1, 16385, i == 0 ? (UBA){ { 0, 0 } } : input.records[i - 1].address);
	graph = begin_graph(64 * 1024);
	if (graph == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[31].address), 0);
	UT_ASSERT_EQ(drain(graph, &records, &sum), CLUSTER_RECOVERY_GRAPH_CAPACITY);
	UT_ASSERT(records < 32);
	cluster_recovery_reference_graph_destroy(graph);
}

UT_TEST(test_long_chain_resumes_without_a_history_depth_cutoff)
{
	ClusterRecoveryTxnKey key = transaction();
	ClusterRecoveryReferenceGraph *graph;
	unsigned records = 0;
	uint64 sum = 0;

	memset(&input, 0, sizeof(input));
	for (unsigned i = 0; i < 257; i++)
		add_record(257, i + 1, 16385 + i,
				   i == 0 ? (UBA){ { 0, 0 } } : input.records[i - 1].address);
	graph = begin_graph(4 * 1024 * 1024);
	if (graph == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[256].address), 0);
	UT_ASSERT_EQ(drain(graph, &records, &sum), CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(records, 257);
	UT_ASSERT_EQ(input.calls, 514);
	cluster_recovery_reference_graph_destroy(graph);
}

UT_TEST(test_invalid_keys_and_late_roots_cannot_change_a_walk)
{
	ClusterRecoveryTxnKey key = transaction();
	ClusterRecoveryReferenceGraph *graph = NULL;
	ClusterRecoveryGraphRecord out;

	UT_ASSERT_EQ(cluster_recovery_reference_graph_begin(1024 * 1024, NULL, NULL, &graph),
				 CLUSTER_RECOVERY_GRAPH_INVALID);
	UT_ASSERT(graph == NULL);
	memset(&input, 0, sizeof(input));
	add_record(257, 1, 16385, (UBA){ { 0, 0 } });
	graph = begin_graph(1024 * 1024);
	if (graph == NULL)
		return;
	key.database_incarnation = 0;
	UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[0].address),
				 CLUSTER_RECOVERY_GRAPH_INVALID);
	cluster_recovery_reference_graph_destroy(graph);
	graph = begin_graph(1024 * 1024);
	if (graph == NULL)
		return;
	key = transaction();
	UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[0].address), 0);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_step(graph, &out), CLUSTER_RECOVERY_GRAPH_RECORD);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[0].address),
				 CLUSTER_RECOVERY_GRAPH_INVALID);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_step(graph, &out),
				 CLUSTER_RECOVERY_GRAPH_INVALID);
	cluster_recovery_reference_graph_destroy(graph);
}


static ClusterTxLocator
lookup_locator(UBA address)
{
	ClusterTxLocator locator = { 0 };
	locator.uba = address;
	locator.xid = transaction().xid;
	locator.tt_wrap = TT_WRAP_INVALID;
	locator.itl_kind = ITL_FLAG_COMMITTED;
	locator.itl_slot_index = 0;
	return locator;
}

UT_TEST(test_page_locator_waits_for_whole_observed_forest_and_is_bounded)
{
	ClusterRecoveryTxnKey key = transaction();
	ClusterRecoveryReferenceGraph *graph;
	ClusterRecoveryGraphLookup *lookup = NULL;
	ClusterRecoveryGraphRecord record;
	ClusterTxLocator locator;
	unsigned records = 0;
	uint64 sum = 0;

	memset(&input, 0, sizeof(input));
	for (unsigned i = 0; i < 257; i++)
		add_record(257, i + 1, 16385 + i,
				   i == 0 ? (UBA){ { 0, 0 } } : input.records[i - 1].address);
	graph = begin_graph(4 * 1024 * 1024);
	if (graph == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[256].address), 0);
	locator = lookup_locator(input.records[256].address); /* first saved node, not first return */
	UT_ASSERT_EQ(cluster_recovery_reference_lookup_begin(graph, 817, 91, &locator, BLCKSZ, &lookup),
				 CLUSTER_RECOVERY_LOOKUP_INVALID);
	UT_ASSERT(lookup == NULL);
	UT_ASSERT_EQ(drain(graph, &records, &sum), CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT(cluster_recovery_reference_graph_observed(graph));
	UT_ASSERT_EQ(cluster_recovery_reference_lookup_begin(graph, 817, 91, &locator, BLCKSZ, &lookup),
				 CLUSTER_RECOVERY_LOOKUP_MORE);
	if (lookup != NULL) {
		for (unsigned i = 0; i < 2; i++) {
			memset(&record, 0x77, sizeof(record));
			UT_ASSERT_EQ(cluster_recovery_reference_lookup_next(lookup, &record),
						 CLUSTER_RECOVERY_LOOKUP_MORE);
			UT_ASSERT(record.record == NULL);
		}
		UT_ASSERT_EQ(cluster_recovery_reference_lookup_next(lookup, &record),
					 CLUSTER_RECOVERY_LOOKUP_RECORD);
		UT_ASSERT_EQ(record.transaction.slot_wrap, 1);
		UT_ASSERT_EQ(record.transaction.segment_generation, 0);
		UT_ASSERT(record.record != NULL);
		UT_ASSERT_EQ(input.calls, 514); /* matching the saved forest causes no new disk read */
		cluster_recovery_reference_graph_invalidate(graph);
		UT_ASSERT_EQ(cluster_recovery_reference_lookup_next(lookup, &record),
					 CLUSTER_RECOVERY_LOOKUP_CHANGED);
		UT_ASSERT(record.record == NULL);
	}
	cluster_recovery_reference_lookup_destroy(lookup);
	cluster_recovery_reference_graph_destroy(graph);
}

UT_TEST(test_unknown_locator_is_not_current_slot_or_terminal_proof)
{
	ClusterRecoveryTxnKey key = transaction();
	ClusterRecoveryReferenceGraph *graph;
	unsigned records = 0;
	uint64 sum = 0;
	memset(&input, 0, sizeof(input));
	add_record(258, 1, 16385, (UBA){ { 0, 0 } });
	graph = begin_graph(1024 * 1024);
	if (graph == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[0].address), 0);
	UT_ASSERT_EQ(drain(graph, &records, &sum), CLUSTER_RECOVERY_GRAPH_DRAINED);
	for (unsigned mode = 0; mode < 6; mode++) {
		ClusterTxLocator locator = lookup_locator(input.records[0].address);
		ClusterRecoveryGraphLookup *lookup = NULL;
		ClusterRecoveryGraphRecord record;
		uint64 system = 817, database = 91;
		if (mode == 0)
			locator.xid++;
		if (mode == 1)
			locator.tt_wrap = 2;
		if (mode == 2)
			locator.uba = uba_encode(258, 2, 6, 0);
		if (mode == 3)
			system++;
		if (mode == 4)
			database++;
		UT_ASSERT_EQ(cluster_recovery_reference_lookup_begin(graph, system, database, &locator,
															 BLCKSZ, &lookup),
					 CLUSTER_RECOVERY_LOOKUP_MORE);
		if (lookup != NULL) {
			UT_ASSERT_EQ(cluster_recovery_reference_lookup_next(lookup, &record),
						 mode == 5 ? CLUSTER_RECOVERY_LOOKUP_RECORD
								   : CLUSTER_RECOVERY_LOOKUP_MISSING);
			if (mode < 5)
				UT_ASSERT(record.record == NULL);
			else {
				UT_ASSERT_EQ(record.transaction.segment_id, 257);
				UT_ASSERT_EQ(record.transaction.segment_generation, 0);
				UT_ASSERT_EQ(record.record_segment_generation, 7);
			}
		}
		cluster_recovery_reference_lookup_destroy(lookup);
	}
	cluster_recovery_reference_graph_destroy(graph);
}

UT_TEST(test_ambiguous_canonical_generation_is_not_first_match_wins)
{
	ClusterRecoveryTxnKey key = transaction();
	ClusterRecoveryReferenceGraph *graph;
	ClusterRecoveryGraphLookup *lookup = NULL;
	ClusterRecoveryGraphRecord record;
	ClusterTxLocator locator;
	unsigned records = 0;
	uint64 sum = 0;
	memset(&input, 0, sizeof(input));
	add_record(258, 1, 16385, (UBA){ { 0, 0 } });
	graph = begin_graph(1024 * 1024);
	if (graph == NULL)
		return;
	UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[0].address), 0);
	key.segment_generation++;
	UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[0].address), 0);
	UT_ASSERT_EQ(drain(graph, &records, &sum), CLUSTER_RECOVERY_GRAPH_DRAINED);
	locator = lookup_locator(input.records[0].address);
	UT_ASSERT_EQ(cluster_recovery_reference_lookup_begin(graph, 817, 91, &locator, BLCKSZ, &lookup),
				 CLUSTER_RECOVERY_LOOKUP_MORE);
	if (lookup != NULL) {
		UT_ASSERT_EQ(cluster_recovery_reference_lookup_next(lookup, &record),
					 CLUSTER_RECOVERY_LOOKUP_IDENTITY);
		UT_ASSERT(record.record == NULL);
		UT_ASSERT_EQ(cluster_recovery_reference_lookup_next(lookup, &record),
					 CLUSTER_RECOVERY_LOOKUP_IDENTITY);
	}
	cluster_recovery_reference_lookup_destroy(lookup);
	cluster_recovery_reference_graph_destroy(graph);
}

UT_TEST(test_locator_validation_and_memory_limit_precede_allocation)
{
	ClusterRecoveryReferenceGraph *graph = begin_graph(1024 * 1024);
	ClusterRecoveryGraphLookup *lookup = NULL;
	ClusterRecoveryGraphRecord record;
	ClusterTxLocator locator = lookup_locator(uba_encode(257, 1, 6, 0));
	UT_ASSERT_EQ(cluster_recovery_reference_graph_step(graph, &record),
				 CLUSTER_RECOVERY_GRAPH_DRAINED);
	for (unsigned mode = 0; mode < 6; mode++) {
		ClusterTxLocator invalid = locator;
		if (mode == 0)
			invalid.xid = InvalidTransactionId;
		if (mode == 1)
			invalid.itl_kind = ITL_FLAG_FREE;
		if (mode == 2)
			invalid.itl_slot_index = CLUSTER_ITL_INITRANS_DEFAULT;
		if (mode == 3)
			invalid.uba = (UBA){ { 0, 0 } };
		if (mode == 4)
			invalid.uba = uba_encode(257, 0, 6, 0);
		UT_ASSERT_EQ(cluster_recovery_reference_lookup_begin(graph, mode == 5 ? 0 : 817, 91,
															 &invalid, BLCKSZ, &lookup),
					 CLUSTER_RECOVERY_LOOKUP_INVALID);
		UT_ASSERT(lookup == NULL);
	}
	UT_ASSERT_EQ(cluster_recovery_reference_lookup_begin(graph, 817, 91, &locator, 1, &lookup),
				 CLUSTER_RECOVERY_LOOKUP_CAPACITY);
	UT_ASSERT(lookup == NULL);
	cluster_recovery_reference_graph_destroy(graph);
}

UT_TEST(test_observed_index_exposes_every_predecessor_without_io)
{
	ClusterRecoveryTxnKey key = transaction();
	ClusterRecoveryReferenceGraph *graph;
	ClusterRecoveryGraphRecord record, zero = { 0 };
	unsigned records = 0, seen = 0;
	uint64 sum = 0;
	memset(&input, 0, sizeof(input));
	add_record(257, 1, 16385, (UBA){ { 0, 0 } });
	add_record(258, 2, 16388, input.records[0].address);
	add_record(258, 3, 20001, input.records[1].address);
	graph = begin_graph(1024 * 1024);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[2].address), 0);
	memset(&record, 0x71, sizeof(record));
	UT_ASSERT_EQ(cluster_recovery_reference_graph_at(graph, 0, &record),
				 CLUSTER_RECOVERY_GRAPH_INVALID);
	UT_ASSERT_EQ(memcmp(&record, &zero, sizeof(record)), 0);
	UT_ASSERT_EQ(drain(graph, &records, &sum), CLUSTER_RECOVERY_GRAPH_DRAINED);
	for (unsigned i = 0; i < 3; i++) {
		ClusterRecoveryGraphResult result = cluster_recovery_reference_graph_at(graph, i, &record);
		UT_ASSERT_EQ(result, CLUSTER_RECOVERY_GRAPH_RECORD);
		if (result != CLUSTER_RECOVERY_GRAPH_RECORD || record.record == NULL)
			continue;
		UT_ASSERT_EQ(record.transaction.system_identifier, key.system_identifier);
		UT_ASSERT_EQ(record.transaction.database_incarnation, key.database_incarnation);
		UT_ASSERT_EQ(record.transaction.segment_id, key.segment_id);
		UT_ASSERT_EQ(record.transaction.segment_generation, key.segment_generation);
		UT_ASSERT_EQ(record.transaction.origin_node_id, key.origin_node_id);
		UT_ASSERT_EQ(record.transaction.xid, key.xid);
		UT_ASSERT_EQ(record.transaction.slot_offset, key.slot_offset);
		UT_ASSERT_EQ(record.transaction.slot_wrap, key.slot_wrap);
		for (unsigned j = 0; j < 3; j++) {
			if (cluster_undo_record_uba_equal(record.record->address, input.records[j].address)) {
				UT_ASSERT((seen & (1u << j)) == 0);
				seen |= 1u << j;
				UT_ASSERT_EQ(record.record_segment_generation, input.records[j].generation);
				UT_ASSERT_EQ(record.record->header.target_block, j + 1);
				UT_ASSERT_EQ(record.record->body_length, sizeof(UndoInsertPayload));
			}
		}
	}
	UT_ASSERT_EQ(seen, 7);
	memset(&record, 0x71, sizeof(record));
	UT_ASSERT_EQ(cluster_recovery_reference_graph_at(graph, 3, &record),
				 CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(memcmp(&record, &zero, sizeof(record)), 0);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_at(graph, 4, &record),
				 CLUSTER_RECOVERY_GRAPH_INVALID);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_at(graph, 0, NULL),
				 CLUSTER_RECOVERY_GRAPH_INVALID);
	UT_ASSERT_EQ(input.calls, 6);
	cluster_recovery_reference_graph_invalidate(graph);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_at(graph, 0, &record),
				 CLUSTER_RECOVERY_GRAPH_INVALID);
	UT_ASSERT_EQ(memcmp(&record, &zero, sizeof(record)), 0);
	UT_ASSERT_EQ(input.calls, 6);
	cluster_recovery_reference_graph_destroy(graph);
}

int
main(void)
{
	UT_PLAN(16);
	UT_RUN(test_cross_relation_and_segment_chain_is_not_page_history);
	UT_RUN(test_multiple_roots_share_a_tail_without_a_false_cycle);
	UT_RUN(test_active_path_cycle_and_self_edge_are_refused);
	UT_RUN(test_same_segment_forward_edge_is_not_a_valid_predecessor);
	UT_RUN(test_pending_keeps_the_same_unread_node);
	UT_RUN(test_missing_record_and_second_read_failure_do_not_drain);
	UT_RUN(test_reread_checks_bytes_and_physical_generation);
	UT_RUN(test_exact_canonical_transaction_fields_must_match);
	UT_RUN(test_capacity_is_not_a_truncated_success);
	UT_RUN(test_long_chain_resumes_without_a_history_depth_cutoff);
	UT_RUN(test_invalid_keys_and_late_roots_cannot_change_a_walk);
	UT_RUN(test_page_locator_waits_for_whole_observed_forest_and_is_bounded);
	UT_RUN(test_unknown_locator_is_not_current_slot_or_terminal_proof);
	UT_RUN(test_ambiguous_canonical_generation_is_not_first_match_wins);
	UT_RUN(test_locator_validation_and_memory_limit_precede_allocation);
	UT_RUN(test_observed_index_exposes_every_predecessor_without_io);
	UT_DONE();
	return ut_failed_count != 0;
}
