/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 * Actual capacity controller: diagnostic output must not alter its decisions.
 */
#include "postgres.h"
#define CAPACITY_REFUSAL_TEST_GUC_DEFINED
bool cluster_trace_undo_capacity_refusal = false;
int capacity_refusal_parent_main(void);
#define main capacity_refusal_parent_main
#include "test_cluster_undo_cleaner_capacity.c"
#undef main

static const char *
refusal_detail(uint32 segment)
{
#ifdef CLUSTER_UNDO_CAPACITY_DIAGNOSTIC_V1
	return cluster_undo_cleaner_capacity_refusal_detail(segment, 100, true);
#else
	(void)segment;
	return NULL; /* Parent has no observation; never invent a successful one. */
#endif
}

static void
expect_detail(const char *site, const char *field)
{
	unsigned probes = test_pool_calls, sleeps = slept, locks = test_lock_depth;
	const char *detail = refusal_detail(4);

	UT_ASSERT_NOT_NULL(detail);
	if (detail == NULL)
		return;
	UT_ASSERT(strstr(detail, site) != NULL);
	UT_ASSERT(strstr(detail, field) != NULL);
	UT_ASSERT(strstr(detail, "top_xid=100 active_slot_pending=1") != NULL);
	UT_ASSERT(strlen(detail) < sizeof(undo_capacity_refusal_text));
	UT_ASSERT_EQ(test_pool_calls, probes);
	UT_ASSERT_EQ(slept, sleeps);
	UT_ASSERT_EQ(test_lock_depth, locks);
	UT_ASSERT(refusal_detail(4) == NULL);
}

UT_TEST(test_default_off_preserves_the_original_refusal)
{
	ClusterCtrcTxnKeyV1 key = { 0 };
	reset_wait_fixture();
	cluster_trace_undo_capacity_refusal = false;
	test_proof = CLUSTER_CTRC_CAPACITY_REFUSE;
	UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(4, &key));
	UT_ASSERT(refusal_detail(4) == NULL);
	UT_ASSERT_EQ(test_pool_calls, 1);
	UT_ASSERT_EQ(prepared, 1);
	UT_ASSERT_EQ(cancelled, 1);
	UT_ASSERT_EQ(slept, 0);
}

UT_TEST(test_entry_context_and_floor_have_distinct_first_sites)
{
	ClusterCtrcTxnKeyV1 key = { 0 };
	reset_wait_fixture();
	cluster_trace_undo_capacity_refusal = true;
	test_foreign_lwlock = true;
	UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(4, &key));
	expect_detail("site=entry_context", "context=0");
	UT_ASSERT_EQ(prepared, 0);
	reset_wait_fixture();
	test_floor = false;
	UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(4, &key));
	expect_detail("site=floor", "floor_ready=0");
	UT_ASSERT_EQ(test_pool_calls, 0);
}

UT_TEST(test_final_pool_refusal_retains_prior_key_and_observation)
{
	ClusterCtrcTxnKeyV1 key = { 0 };
	reset_wait_fixture();
	cluster_trace_undo_capacity_refusal = true;
	test_proof = CLUSTER_CTRC_CAPACITY_REFUSE;
	test_clear_current_continuation = true;
	key.segment_id = 256;
	key.xid = 9123;
	key.slot_wrap = 3;
	key.root_id = 71;
	UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(4, &key));
	UT_ASSERT_EQ(key.segment_id, 0);
	expect_detail("site=pool_probe", "input_key.segment=256");
	UT_ASSERT_EQ(test_pool_calls, 1);
}

UT_TEST(test_invalid_segment_route_is_the_actual_refusal)
{
	ClusterCtrcTxnKeyV1 key = { 0 };
	reset_wait_fixture();
	cluster_trace_undo_capacity_refusal = true;
	test_proof = CLUSTER_CTRC_CAPACITY_REFUSE;
	test_pool_reason = CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE;
	test_segment_owner = true;
	test_segment_fault = 1;
	UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(4, &key));
	expect_detail("site=pool_segment_route", "observation_known=1");
}

UT_TEST(test_wait_context_and_missing_owner_are_distinct)
{
	ClusterCtrcTxnKeyV1 key = { 0 };
	reset_wait_fixture();
	cluster_trace_undo_capacity_refusal = true;
	test_pin_after_probe = true;
	UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(4, &key));
	expect_detail("site=wait_context", "context=0");
	reset_wait_fixture();
	undo_cleaner_state->workers[0].status = UNDO_CLEANER_NOT_STARTED;
	UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(4, &key));
	expect_detail("site=owner_unavailable", "supplied=0");
	UT_ASSERT_EQ(slept, 0);
}

UT_TEST(test_success_retry_and_cancel_retire_stale_refusals)
{
	for (int mode = 0; mode < 3; mode++) {
		ClusterCtrcTxnKeyV1 key = { 0 };
		volatile bool caught = false;
		reset_wait_fixture();
		cluster_trace_undo_capacity_refusal = true;
		test_floor = false;
		UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(4, &key));
		test_floor = true;
		test_proof = mode == 1 ? CLUSTER_CTRC_CAPACITY_RETRY : CLUSTER_CTRC_CAPACITY_WAIT;
		test_cancel = mode == 2;
		PG_TRY();
		{
			UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(4, &key));
		}
		PG_CATCH();
		{
			caught = true;
			UT_ASSERT_EQ(geterrcode(), ERRCODE_QUERY_CANCELED);
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, mode == 2);
		UT_ASSERT(refusal_detail(4) == NULL);
		UT_ASSERT(!subscribed && test_lock_depth == 0);
	}
}

UT_TEST(test_missing_state_and_unexecuted_predicates_stay_unknown)
{
	ClusterCtrcTxnKeyV1 key = { 0 };
	UndoCleanerSharedState *saved;
	reset_wait_fixture();
	cluster_trace_undo_capacity_refusal = true;
	saved = undo_cleaner_state;
	undo_cleaner_state = NULL;
	UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(4, &key));
	undo_cleaner_state = saved;
	expect_detail("site=unavailable", "proof=-1");
	UT_ASSERT_EQ(prepared, 0);
}

UT_TEST(test_first_wins_segment_match_and_bounded_formatting)
{
	ClusterCtrcTxnKeyV1 key = { 0 };
	reset_wait_fixture();
	cluster_trace_undo_capacity_refusal = true;
	test_floor = false;
	UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(4, &key));
#ifdef CLUSTER_UNDO_CAPACITY_DIAGNOSTIC_V1
	/* Only diagnostic memory is injected; no capacity decision is supplied. */
	undo_capacity_refusal_capture(&(UndoCapacityRefusalInput){ .site = "second", .segment = 4 });
	expect_detail("site=floor", "floor_ready=0");
	UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(4, &key));
	UT_ASSERT(cluster_undo_cleaner_capacity_refusal_detail(5, 100, true) == NULL);
	UT_ASSERT(refusal_detail(4) == NULL);
	{
		char site[20000];
		const char *detail;

		memset(site, 'x', sizeof(site));
		site[sizeof(site) - 1] = '\0';
		undo_capacity_refusal_capture(&(UndoCapacityRefusalInput){ .site = site, .segment = 4 });
		detail = refusal_detail(4);
		UT_ASSERT_NOT_NULL(detail);
		if (detail != NULL) {
			UT_ASSERT_EQ(strlen(detail), sizeof(undo_capacity_refusal_text) - 1);
			UT_ASSERT(strstr(detail, " truncated=1") != NULL);
		}
		UT_ASSERT(refusal_detail(4) == NULL);
	}
	printf("# snapshot_bytes=%zu buffer_bytes=%zu\n", sizeof(UndoCapacityRefusalRecord),
		   sizeof(undo_capacity_refusal_text));
#else
	UT_ASSERT(refusal_detail(4) != NULL);
#endif
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_default_off_preserves_the_original_refusal);
	UT_RUN(test_entry_context_and_floor_have_distinct_first_sites);
	UT_RUN(test_final_pool_refusal_retains_prior_key_and_observation);
	UT_RUN(test_invalid_segment_route_is_the_actual_refusal);
	UT_RUN(test_wait_context_and_missing_owner_are_distinct);
	UT_RUN(test_success_retry_and_cancel_retire_stale_refusals);
	UT_RUN(test_missing_state_and_unexecuted_predicates_stay_unknown);
	UT_RUN(test_first_wins_segment_match_and_bounded_formatting);
	UT_DONE();
	return ut_failed_count != 0;
}
