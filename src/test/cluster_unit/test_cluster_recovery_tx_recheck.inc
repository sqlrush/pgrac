/* Final physical TX reads use the original parser and immutable forest.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterRecoveryReferenceGraph *
tx_recheck_graph(void)
{
	ClusterRecoveryTxnKey key = transaction();
	ClusterRecoveryReferenceGraph *graph;
	unsigned records = 0;
	uint64 sum = 0;

	memset(&input, 0, sizeof(input));
	add_record(257, 1, 16385, (UBA){{0, 0}});
	add_record(258, 2, 20001, input.records[0].address);
	add_record(258, 3, 20002, input.records[0].address);
	graph = begin_graph(1024 * 1024);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[1].address), 0);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_add(graph, &key, input.records[2].address), 0);
	UT_ASSERT_EQ(drain(graph, &records, &sum), CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(records, 3);
	return graph;
}

UT_TEST(test_tx_final_recheck_reads_all_shared_predecessors_without_rebasing)
{
	ClusterRecoveryReferenceGraph *graph = tx_recheck_graph();
	unsigned calls = input.calls;

	input.pending = 2;
	UT_ASSERT_EQ(cluster_recovery_reference_graph_recheck_at(graph, 0), CLUSTER_RECOVERY_GRAPH_PENDING);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_recheck_at(graph, 0), CLUSTER_RECOVERY_GRAPH_PENDING);
	UT_ASSERT(cluster_recovery_reference_graph_observed(graph));
	for (uint32 i = 0; i < 3; i++)
		UT_ASSERT_EQ(cluster_recovery_reference_graph_recheck_at(graph, i), CLUSTER_RECOVERY_GRAPH_RECORD);
	UT_ASSERT_EQ(input.calls, calls + 5);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_recheck_at(graph, 3), CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(input.calls, calls + 5);
	UT_ASSERT_EQ(graph->root_count, 2);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_count(graph), 3);
	cluster_recovery_reference_graph_destroy(graph);
}

UT_TEST(test_tx_final_recheck_rejects_late_bytes_generation_and_native_read_failure)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRecoveryReferenceGraph *graph = tx_recheck_graph();
		unsigned calls;

		/* Index 2 is the shared non-root predecessor. */
		UT_ASSERT_EQ(cluster_recovery_reference_graph_recheck_at(graph, 0), CLUSTER_RECOVERY_GRAPH_RECORD);
		UT_ASSERT_EQ(cluster_recovery_reference_graph_recheck_at(graph, 1), CLUSTER_RECOVERY_GRAPH_RECORD);
		if (fault == 2) input.fail_call = input.calls + 1;
		else { input.change_call = input.calls + 1; input.change_generation = fault == 1; }
		/* The canonical predecessor's generation is part of its original
		 * transaction identity; reject it before the saved-byte comparison. */
		UT_ASSERT_EQ(cluster_recovery_reference_graph_recheck_at(graph, 2),
			fault == 2 ? CLUSTER_RECOVERY_GRAPH_IO
			: fault == 1 ? CLUSTER_RECOVERY_GRAPH_IDENTITY : CLUSTER_RECOVERY_GRAPH_CHANGED);
		calls = input.calls;
		input.change_call = input.fail_call = 0;
		UT_ASSERT(!cluster_recovery_reference_graph_observed(graph));
		UT_ASSERT(cluster_recovery_reference_graph_recheck_at(graph, 0) >= CLUSTER_RECOVERY_GRAPH_INVALID);
		UT_ASSERT_EQ(input.calls, calls);
		cluster_recovery_reference_graph_destroy(graph);
	}
}

UT_TEST(test_tx_final_recheck_requires_observed_forest_and_accepts_real_empty_end)
{
	ClusterRecoveryReferenceGraph *graph;
	unsigned records = 0;
	uint64 sum = 0;

	memset(&input, 0, sizeof(input));
	graph = begin_graph(1024 * 1024);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_recheck_at(graph, 0), CLUSTER_RECOVERY_GRAPH_INVALID);
	UT_ASSERT_EQ(input.calls, 0);
	UT_ASSERT_EQ(drain(graph, &records, &sum), CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_recheck_at(graph, 0), CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_recheck_at(graph, 1), CLUSTER_RECOVERY_GRAPH_INVALID);
	UT_ASSERT_EQ(input.calls, 0);
	cluster_recovery_reference_graph_destroy(graph);
}
