/* Real selected-segment caller and cleaner counters; native lock scheduling,
 * admission and physical publication remain the original fixture boundaries. */
#define TT_ACTIVE_OWNER_FIXTURE_MAIN native_slot_owner_fixture_main
#define TT_ACTIVE_OWNER_EXTERNAL_WAIT_SCOPE
#define slot_wait_in_native undo_cleaner_tx_slot_wait_active
#include "test_cluster_tt_active_owner.c"
#undef slot_wait_in_native
#undef TT_ACTIVE_OWNER_EXTERNAL_WAIT_SCOPE

#include "test_cluster_native_slot_scope.inc"

static void
require_native_counts(uint64 entered, uint64 cancelled, uint64 rechecked)
{
	ClusterUndoCapacityStats stats;

	UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
	UT_ASSERT_EQ(stats.origin_node_id, 0);
	UT_ASSERT_EQ(stats.active_full_wait_count, entered);
	UT_ASSERT_EQ(stats.cancel_count, cancelled);
	UT_ASSERT_EQ(stats.native_wakeup_recheck_count, rechecked);
	UT_ASSERT_EQ(stats.pool_wait_count, 0);
	UT_ASSERT_EQ(stats.wakeup_recheck_count, 0);
	UT_ASSERT(!undo_cleaner_tx_slot_wait_active);
	UT_ASSERT(!cluster_undo_cleaner_tx_slot_waiting());
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(slot_wait_event, WAIT_EVENT_CLIENT_READ);
}

UT_TEST(test_native_handoff_counts_one_wait_before_reusing_the_binding)
{
	for (unsigned commit = 0; commit < 2; commit++) {
		start_full_active_test();
		slot_wait_commit = commit != 0;
		require_native_counts(0, 0, 0);
		require_published_on(1);
		require_native_counts(1, 0, 1);
		UT_ASSERT_EQ(slot_wait_calls, 1);
		require_published_on(1);
		require_native_counts(1, 0, 1);
		UT_ASSERT_EQ(slot_wait_calls, 1);
		UT_ASSERT_EQ(cold_selection_calls, 0);
	}
}

UT_TEST(test_native_errors_count_only_query_cancellation_then_allow_a_fresh_wait)
{
	static const int errors[]
		= { ERRCODE_QUERY_CANCELED, ERRCODE_LOCK_NOT_AVAILABLE, ERRCODE_T_R_DEADLOCK_DETECTED,
			ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS, ERRCODE_IO_ERROR };

	for (unsigned i = 0; i < lengthof(errors); i++) {
		uint64 cancels = errors[i] == ERRCODE_QUERY_CANCELED ? 1 : 0;

		start_full_active_test();
		slot_wait_error = errors[i];
		require_capacity_error(errors[i]);
		require_native_counts(1, cancels, 0);
		UT_ASSERT_EQ(slot_wait_calls, 1);
		UT_ASSERT_EQ(g_bind_emit_calls + cold_selection_calls, 0);
		slot_wait_error = 0;
		require_published_on(1);
		require_native_counts(2, cancels, 1);
		UT_ASSERT_EQ(slot_wait_calls, 2);
		require_published_on(1);
		require_native_counts(2, cancels, 1);
	}
}

UT_TEST(test_probe_refusals_do_not_count_a_native_wait_or_cancellation)
{
	for (unsigned refusal = 0; refusal < 5; refusal++) {
		start_full_active_test();
		slot_wait_context_safe = refusal != 0;
		slot_wait_owner_kind = refusal;
		require_capacity_error(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
		require_native_counts(0, 0, 0);
		UT_ASSERT_EQ(slot_wait_calls + g_bind_emit_calls + cold_selection_calls, 0);
	}
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_native_handoff_counts_one_wait_before_reusing_the_binding);
	UT_RUN(test_native_errors_count_only_query_cancellation_then_allow_a_fresh_wait);
	UT_RUN(test_probe_refusals_do_not_count_a_native_wait_or_cancellation);
	UT_DONE();
	return ut_failed_count != 0;
}
