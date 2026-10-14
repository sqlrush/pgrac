/*-------------------------------------------------------------------------
 * test_cluster_ko_current_admission.c -- fence new requester acceptance.
 * Actual PCM round/history/terminal transitions, explicit KO/PG fixtures.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
int pcm_current_fixture_main(void);
#define main pcm_current_fixture_main
#include "test_cluster_pcm_lock.c"
#undef main

static ResourceXAssertion
current_setup(void)
{
	BufferTag tag = make_tag(6920);
	ResourceXAssertion assertion;
	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	return assertion;
}

static ResourceXBootstrapRoundAction
current_step(const ResourceXAssertion *assertion, ResourceXCallerWitness *caller, bool cached,
			 ResourceXAcquisitionRef *terminal)
{
	ResourceXDecodedFrame dispatch;
	return cluster_pcm_lock_resource_x_bootstrap_round_step_caller_exact(
		assertion, 0, 17, 31, 77, 51, 61, 1000, 900, 100, 50, 0, 0, true, true, cached,
		cached ? 9 : 0, caller, &dispatch, terminal);
}

static void
close_object(void)
{
	object_admission_allowed = false;
	object_admission_revision++;
}

UT_TEST(new_round_is_not_published_behind_fence)
{
	ResourceXAssertion assertion = current_setup();
	ResourceXCallerWitness caller = { 0 };
	ResourceXAcquisitionRef terminal;
	cluster_shared_config = true;
	close_object();
	UT_ASSERT_EQ(current_step(&assertion, &caller, false, &terminal),
				 RESOURCE_X_BOOTSTRAP_ROUND_BACKPRESSURE);
	UT_ASSERT_EQ(caller.joined_request.assertion_sequence, 0);
	UT_ASSERT_EQ(terminal.acquisition_generation, 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
}

UT_TEST(observer_cannot_admit_new_caller_to_old_round)
{
	ResourceXAssertion assertion = current_setup();
	ResourceXCallerWitness old = { 0 }, next = { 0 };
	ResourceXAcquisitionRef terminal;
	struct StopPcmEntryLayout *entry, before;
	UT_ASSERT_EQ(current_step(&assertion, &old, false, &terminal),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	entry = hash_search((HTAB *)&fake_pcm_htab_token, &assertion.resource, HASH_FIND, NULL);
	before = *entry;
	cluster_shared_config = true;
	close_object();
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_caller_observe_exact(&assertion, 17, 31, 77, &next),
				 RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(next.joined_request.assertion_sequence, 0);
	UT_ASSERT_EQ(memcmp(entry, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_caller_observe_exact(&assertion, 17, 31, 77, &old),
				 RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(step_preserves_old_round_but_refuses_new_follower)
{
	ResourceXAssertion assertion = current_setup();
	ResourceXCallerWitness old = { 0 }, next = { 0 };
	ResourceXAcquisitionRef terminal;
	UT_ASSERT_EQ(current_step(&assertion, &old, false, &terminal),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	cluster_shared_config = true;
	close_object();
	UT_ASSERT_EQ(current_step(&assertion, &next, false, &terminal),
				 RESOURCE_X_BOOTSTRAP_ROUND_BACKPRESSURE);
	UT_ASSERT_EQ(next.joined_request.assertion_sequence, 0);
	UT_ASSERT_EQ(current_step(&assertion, &old, false, &terminal), RESOURCE_X_BOOTSTRAP_ROUND_WAIT);
	UT_ASSERT_EQ(old.failed_attempt, 0);
}

UT_TEST(entry_lock_race_refuses_new_round_and_observer)
{
	for (int observer = 0; observer < 2; observer++) {
		ResourceXAssertion assertion = current_setup();
		ResourceXCallerWitness old = { 0 }, next = { 0 };
		ResourceXAcquisitionRef terminal;
		if (observer)
			UT_ASSERT_EQ(current_step(&assertion, &old, false, &terminal),
						 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
		cluster_shared_config = true;
		object_entry_lock_hook = close_object;
		if (observer)
			UT_ASSERT_EQ(
				cluster_pcm_lock_resource_x_caller_observe_exact(&assertion, 17, 31, 77, &next),
				RESOURCE_X_APPLY_BAD_STATE);
		else
			UT_ASSERT_EQ(current_step(&assertion, &next, false, &terminal),
						 RESOURCE_X_BOOTSTRAP_ROUND_BACKPRESSURE);
		UT_ASSERT_EQ(next.joined_request.assertion_sequence, 0);
		UT_ASSERT(object_admission_samples > 0);
		UT_ASSERT_EQ(fake_lwlock_depth, 0);
	}
}

UT_TEST(cached_cover_is_not_new_caller_admission)
{
	ResourceXAssertion assertion = current_setup();
	ResourceXCallerWitness next = { 0 };
	ResourceXAcquisitionRef terminal;
	ResourceXAcquisitionRef existing = setup_resource_x_test_terminal_cover(assertion.resource, 9);
	cluster_shared_config = true;
	close_object();
	UT_ASSERT_EQ(current_step(&assertion, &next, true, &terminal),
				 RESOURCE_X_BOOTSTRAP_ROUND_BACKPRESSURE);
	UT_ASSERT_EQ(terminal.acquisition_generation, 0);
	UT_ASSERT(
		cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(&existing, 31, 77, 9));
}

UT_TEST(original_rejections_and_unshared_paths_remain)
{
	for (int scope = 0; scope < 3; scope++) {
		ResourceXAssertion assertion = current_setup();
		ResourceXCallerWitness caller = { 0 };
		ResourceXAcquisitionRef terminal;
		cluster_shared_config = scope != 0;
		if (scope == 1)
			cluster_interconnect_tier = CLUSTER_IC_TIER_2;
		if (scope == 2)
			object_smgr_route = 0;
		close_object();
		UT_ASSERT_EQ(current_step(&assertion, &caller, false, &terminal),
					 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
		UT_ASSERT_EQ(object_admission_samples, 0);
		caller.failed_attempt = caller.joined_request.assertion_sequence;
		UT_ASSERT_EQ(
			cluster_pcm_lock_resource_x_caller_observe_exact(&assertion, 17, 31, 77, &caller),
			RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	}
}

UT_TEST(open_object_accepts_once_and_keeps_failed_history)
{
	ResourceXAssertion assertion = current_setup();
	ResourceXCallerWitness caller = { 0 }, next = { 0 };
	ResourceXAcquisitionRef terminal;
	ResourceXCallerWitness before;
	cluster_shared_config = true;
	UT_ASSERT_EQ(current_step(&assertion, &caller, false, &terminal),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_caller_observe_exact(&assertion, 17, 31, 77, &next),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(next.joined_request.assertion_sequence, caller.joined_request.assertion_sequence);
	caller.failed_attempt = caller.joined_request.assertion_sequence;
	before = caller;
	close_object();
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_caller_observe_exact(&assertion, 17, 31, 77, &caller),
				 RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(current_step(&assertion, &caller, false, &terminal),
				 RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED);
	UT_ASSERT_EQ(memcmp(&before, &caller, sizeof(before)), 0);
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(new_round_is_not_published_behind_fence);
	UT_RUN(observer_cannot_admit_new_caller_to_old_round);
	UT_RUN(step_preserves_old_round_but_refuses_new_follower);
	UT_RUN(entry_lock_race_refuses_new_round_and_observer);
	UT_RUN(cached_cover_is_not_new_caller_admission);
	UT_RUN(original_rejections_and_unshared_paths_remain);
	UT_RUN(open_object_accepts_once_and_keeps_failed_history);
	UT_RUN(test_resource_x_failed_caller_cannot_join_later_head);
	UT_RUN(test_resource_x_caller_retirement_is_not_current_success);
	UT_RUN(test_resource_x_node_fanin_ignores_caller_deadline_and_retry_slice);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
