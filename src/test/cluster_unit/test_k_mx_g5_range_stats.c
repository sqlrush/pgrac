/* Real range requester/GCS/native/G2 and shared counters; DATA and backend
 * leaves remain controlled by the existing caller fixture. SCAN completion
 * is metadata completion only, never a proof or visibility qualification. */
int k_range_stats_component_main(void);
#define K_MX_NATIVE_CALLER_MAIN k_range_stats_component_main
#include "test_k_mx_g5_native_caller.c"

static ClusterCurrentMxKey
range_stats_key(void)
{
	ClusterCurrentMxKey key = {0};
	key.multixact_id = 1000;
	key.cluster_epoch = 7;
	return key;
}

static void
range_stats_expect(uint64 started, uint64 complete, uint64 unknown,
	uint64 errors, uint64 restarts, uint64 maximum)
{
	ClusterCurrentMxStatsSnapshot cut;
	UT_ASSERT(cluster_multixact_current_stats_snapshot(1, 7, &cut));
	UT_ASSERT_EQ(cut.counters[CMX_STAT_RANGE_SCAN_STARTED], started);
	UT_ASSERT_EQ(cut.counters[CMX_STAT_RANGE_SCAN_COMPLETE], complete);
	UT_ASSERT_EQ(cut.counters[CMX_STAT_RANGE_SCAN_UNKNOWN], unknown);
	UT_ASSERT_EQ(cut.counters[CMX_STAT_RANGE_SCAN_ERROR], errors);
	UT_ASSERT_EQ(cut.counters[CMX_STAT_RANGE_SCAN_RESTART], restarts);
	UT_ASSERT_EQ(cut.counters[CMX_STAT_RANGE_SCAN_RESTART_MAX], maximum);
}

UT_TEST(range_stats_uninitialized_is_safe)
{
	cluster_multixact_current_stats_record_range_restarts(UINT64_MAX);
	UT_ASSERT_EQ(cluster_multixact_current_stats_get(CMX_STAT_RANGE_SCAN_RESTART_MAX), 0);
}

UT_TEST(range_stats_complete_metadata_is_counted_once)
{
	const uint32 sizes[] = {2, 257, 65537};
	for (unsigned i = 0; i < lengthof(sizes); i++) {
		ClusterCurrentMxKey key = range_stats_key();
		ClusterMxCompositionReceiveStats stats = {0};
		ClusterMxCompositionReceive *op = NULL;
		uint32 total = 0;
		chain_seed(sizes[i], MULTIXACT_MEMBERS_PER_PAGE - 1);
		UT_ASSERT(cluster_multixact_current_describe_full(&key, &request_tag, &request_snapshot, &op, &stats));
		UT_ASSERT_NOT_NULL(cluster_mx_composition_pages(op, &total));
		UT_ASSERT_EQ(total, sizes[i]);
		UT_ASSERT_EQ(stats.accepted_frames, requests_sent);
		range_stats_expect(1, 1, 0, 0, 0, 0);
		cluster_multixact_current_describe_finish(&op);
		range_stats_expect(1, 1, 0, 0, 0, 0);
		chain_clean();
	}
}

UT_TEST(range_stats_late_unknown_is_not_an_implicit_retry)
{
	ClusterCurrentMxKey key = range_stats_key();
	ClusterMxCompositionReceiveStats stats = {0};
	ClusterMxCompositionReceive *op = NULL;
	chain_seed(257, 1);
	fault = 5;
	UT_ASSERT(!cluster_multixact_current_describe_full(&key, &request_tag, &request_snapshot, &op, &stats));
	UT_ASSERT_NULL(op);
	UT_ASSERT_EQ(requests_sent, 2);
	UT_ASSERT_EQ(stats.unknown_results, 1);
	UT_ASSERT_EQ(stats.restart_requests, 1);
	range_stats_expect(1, 0, 1, 0, 1, 1);
	cluster_multixact_current_describe_finish(&op);
	range_stats_expect(1, 0, 1, 0, 1, 1);
	chain_clean();
}

UT_TEST(range_stats_explicit_restart_sequence_is_observable)
{
	ClusterCurrentMxKey key = range_stats_key();
	ClusterMxCompositionReceiveStats stats = {0};
	ClusterMxCompositionReceive *op = NULL;
	chain_seed(257, 1);
	for (unsigned attempt = 1; attempt <= 3; attempt++) {
		requests_sent = replies_sent = 0;
		fault = 5;
		UT_ASSERT(!cluster_multixact_current_describe_full(&key, &request_tag, &request_snapshot, &op, &stats));
		UT_ASSERT_NULL(op);
		UT_ASSERT_EQ(requests_sent, 2);
		UT_ASSERT_EQ(first_history[0], 0);
		UT_ASSERT_EQ(stats.restart_requests, attempt);
		range_stats_expect(attempt, 0, attempt, 0, attempt, attempt);
	}
	requests_sent = replies_sent = fault = 0;
	UT_ASSERT(cluster_multixact_current_describe_full(&key, &request_tag, &request_snapshot, &op, &stats));
	UT_ASSERT_EQ(first_history[0], 0);
	range_stats_expect(4, 1, 3, 0, 3, 3);
	cluster_multixact_current_describe_finish(&op);
	chain_clean();
}

UT_TEST(range_stats_error_and_cancel_are_not_returned_unknown)
{
	for (unsigned mode = 6; mode <= 7; mode++) {
		ClusterCurrentMxKey key = range_stats_key();
		static ClusterMxCompositionReceiveStats stats;
		static ClusterMxCompositionReceive *op;
		volatile bool caught = false;
		chain_seed(257, 1);
		memset(&stats, 0, sizeof(stats));
		op = NULL;
		fault = mode;
		PG_TRY(); {
			(void)cluster_multixact_current_describe_full(&key, &request_tag, &request_snapshot, &op, &stats);
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_NULL(op);
		range_stats_expect(1, 0, 0, 1, 0, 0);
		InterruptPending = false;
		chain_clean();
	}
}

UT_TEST(range_stats_bounded_wrapper_keeps_its_original_outcomes)
{
	/* Executes the original scoped wrapper for OK, supported-limit and
	 * UNKNOWN, including its existing exact logical-operation assertions. */
	scoped_outcomes_in_original_snapshot();
	range_stats_expect(1, 0, 1, 0, 1, 1);
	chain_clean();
}

UT_TEST(range_stats_restart_max_is_uint64_and_monotonic)
{
	chain_seed(2, 1);
	cluster_multixact_current_stats_record_range_restarts(UINT64CONST(4294967302));
	range_stats_expect(0, 0, 0, 0, 0, UINT64CONST(4294967302));
	cluster_multixact_current_stats_record_range_restarts(3);
	range_stats_expect(0, 0, 0, 0, 0, UINT64CONST(4294967302));
	cluster_multixact_current_stats_record_range_restarts(UINT64_MAX);
	range_stats_expect(0, 0, 0, 0, 0, UINT64_MAX);
	UT_ASSERT_EQ(cluster_multixact_current_stats_get(CMX_STAT_RESTART_MAX), 0);
	chain_clean();
}

int main(void)
{
	TopMemoryContext = AllocSetContextCreate(CurrentMemoryContext, "range scan statistics", ALLOCSET_DEFAULT_SIZES);
	CurrentMemoryContext = TopMemoryContext;
	UT_PLAN(7);
	UT_RUN(range_stats_uninitialized_is_safe);
	UT_RUN(range_stats_complete_metadata_is_counted_once);
	UT_RUN(range_stats_late_unknown_is_not_an_implicit_retry);
	UT_RUN(range_stats_explicit_restart_sequence_is_observable);
	UT_RUN(range_stats_error_and_cancel_are_not_returned_unknown);
	UT_RUN(range_stats_bounded_wrapper_keeps_its_original_outcomes);
	UT_RUN(range_stats_restart_max_is_uint64_and_monotonic);
	MemoryContextDelete(TopMemoryContext);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
