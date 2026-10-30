/*-------------------------------------------------------------------------
 * test_cluster_vm_pair_repair.c
 *    Two-page conservative repair acquires current outside content locks.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
int pair_inherited_main(void);
#define main pair_inherited_main
#include "test_cluster_vm_visible_version.c"
#undef main

UT_TEST(test_pair_first_vm_acquisition_is_outside_heap_content)
{
	for (unsigned lpdead = 0; lpdead < 2; lpdead++) {
		setup_pair_repair(lpdead != 0, false, 0);
		run_pair_repair(lpdead != 0);
		check_pair_repair(20, 30);
		UT_ASSERT_EQ(pair_attempts, 1);
		UT_ASSERT_EQ(pair_nested_success + pair_heap_waits, 0);
		UT_ASSERT_EQ(pair_captures[0] + pair_captures[1], 2);
		UT_ASSERT_EQ(pair_resumes[0] + pair_resumes[1], 2);
		release_pair_repair();
	}
}

UT_TEST(test_pair_stale_or_busy_heap_resume_uses_unpinned_current_fallback)
{
	setup_pair_repair(true, false, 0);
	pair_resume_refusals[0] = 1;
	run_pair_repair(true);
	check_pair_repair(20, 30);
	UT_ASSERT_EQ(pair_heap_waits, 1);
	UT_ASSERT_EQ(pair_attempts, 1);
	UT_ASSERT_EQ(pair_resumes[0] + pair_resumes[1], 2);
	release_pair_repair();
}

UT_TEST(test_pair_stale_or_busy_vm_resume_retries_without_nested_acquisition)
{
	for (unsigned refusals = 1; refusals <= 4; refusals += 3) {
		setup_pair_repair(true, false, 0);
		pair_resume_refusals[1] = refusals;
		run_pair_repair(true);
		check_pair_repair(20, 30);
		UT_ASSERT_EQ(pair_attempts, refusals + 1);
		UT_ASSERT_EQ(pair_captures[0], refusals + 1);
		UT_ASSERT_EQ(pair_captures[1], refusals + 1);
		UT_ASSERT_EQ(pair_resumes[1], refusals + 1);
		UT_ASSERT_EQ(pair_nested_success + pair_heap_waits, 0);
		UT_ASSERT_EQ(pair_local_unlocks, 1);
		release_pair_repair();
	}
}

UT_TEST(test_pair_missing_original_writer_fails_before_any_mutation)
{
	for (unsigned missing = 1; missing <= 2; missing++) {
		setup_pair_repair(true, false, 0);
		pair_heap_proof = missing != 1;
		pair_vm_proof = missing != 2;
		run_pair_repair(true);
		UT_ASSERT(pair_caught && !held[1] && pinned[0] && pinned[1]);
		UT_ASSERT_EQ(held[0], missing == 1);
		UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
		UT_ASSERT_EQ(memcmp(pages, before, sizeof(pages)), 0);
		UT_ASSERT_EQ(pair_resumes[0] + pair_resumes[1] + pair_local_unlocks, 0);
		release_pair_repair();
	}
}

UT_TEST(test_pair_capture_error_releases_only_warmed_vm_scope)
{
	for (unsigned buffer = 1; buffer <= 2; buffer++) {
		setup_pair_repair(true, false, 0);
		pair_capture_error = buffer;
		run_pair_repair(true);
		UT_ASSERT(pair_caught && !held[1] && pinned[0] && pinned[1]);
		UT_ASSERT_EQ(held[0], buffer == 1);
		UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
		UT_ASSERT_EQ(memcmp(pages, before, sizeof(pages)), 0);
		UT_ASSERT(PG_exception_stack == NULL && error_context_stack == NULL);
		release_pair_repair();
	}
}

UT_TEST(test_pair_resume_error_does_not_mutate_or_leave_vm_content)
{
	for (unsigned buffer = 1; buffer <= 2; buffer++) {
		setup_pair_repair(true, false, 0);
		pair_resume_error = buffer;
		run_pair_repair(true);
		UT_ASSERT(pair_caught && !held[1] && pinned[0]);
		UT_ASSERT_EQ(held[0], buffer == 2);
		UT_ASSERT_EQ(pinned[1], buffer == 2);
		UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
		UT_ASSERT_EQ(memcmp(pages, before, sizeof(pages)), 0);
		release_pair_repair();
	}
}

UT_TEST(test_pair_cancel_after_vm_resume_refusal_stops_before_another_acquisition)
{
	setup_pair_repair(true, false, 0);
	pair_resume_refusals[1] = 1;
	pair_cancel_resume = true;
	run_pair_repair(true);
	UT_ASSERT(pair_caught && !held[0] && !held[1] && pinned[0] && !pinned[1]);
	UT_ASSERT(!InterruptPending);
	UT_ASSERT_EQ(pair_attempts, 1);
	UT_ASSERT_EQ(inserts + edge_count + dirty[0] + dirty[1], 0);
	UT_ASSERT_EQ(memcmp(pages, before, sizeof(pages)), 0);
	release_pair_repair();
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(test_pair_first_vm_acquisition_is_outside_heap_content);
	UT_RUN(test_pair_stale_or_busy_heap_resume_uses_unpinned_current_fallback);
	UT_RUN(test_pair_stale_or_busy_vm_resume_retries_without_nested_acquisition);
	UT_RUN(test_pair_missing_original_writer_fails_before_any_mutation);
	UT_RUN(test_pair_capture_error_releases_only_warmed_vm_scope);
	UT_RUN(test_pair_resume_error_does_not_mutate_or_leave_vm_content);
	UT_RUN(test_pair_cancel_after_vm_resume_refusal_stops_before_another_acquisition);
	UT_RUN(test_pair_repair_reobserves_with_original_heap_pin);
	UT_RUN(test_pair_repair_resident_misses_release_heap_before_vm_io);
	UT_RUN(test_pair_repair_captures_only_fresh_two_page_predecessors);
	UT_RUN(test_pair_repair_cancel_keeps_only_original_heap_pin);
	UT_RUN(test_pair_repair_error_or_unknown_version_precedes_mutation);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
