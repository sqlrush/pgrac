/* Actual GCS admission/resume/publication and original PCM/codec owners.
 * Peer/session observations and physical worker admission are environmental
 * leaves. Publication does not qualify requester native completion. */
#include "test_cluster_resource_x_master_ack_fixture.inc"

static ResourceXApplyResult
publish_tick(const ResourceXBootstrapWork *work)
{
	int reads = master_durable_reads;
	unsigned acks = bootstrap_ack_count;
	ResourceXApplyResult result = cluster_gcs_block_resource_x_bootstrap_tick(work);

	UT_ASSERT_EQ(master_durable_reads, reads);
	UT_ASSERT_EQ(bootstrap_ack_count, acks);
	UT_ASSERT_EQ(master_durable_admission_depth, 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	return result;
}

static bool
publish_tick_start(int scenario, PcmState target, PublishFixture *f)
{
	bool ok = publish_start(scenario, target, f);
	ready_ring_full = true;
	ready_enqueue_calls = 0;
	return ok;
}

static void
publish_tick_unchanged_directory(PublishFixture *f)
{
	PcmAuthoritySnapshot after;
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(make_tag(850), &after));
	UT_ASSERT(memcmp(&after, &f->plan.input.directory, sizeof(after)) == 0);
	UT_ASSERT_EQ(f->state->requests[f->request.common.logical_assertion.requester_node]
		.final_authority_generation, 0);
}

UT_TEST(test_tick_queued_disk_s_and_x_publish_without_another_request)
{
	for (PcmState target = PCM_STATE_S; target <= PCM_STATE_X; target++) {
		ResourceXDecodedFrame request, a;
		ResourceXBootstrapWork work;
		ResourceXMasterSnapshot snapshot;
		PcmAuthoritySnapshot before, after;
		ResourceXIntentSlot slot, captured;
		ResourceXWireReject reject;
		uint8 bytes[RESOURCE_X_DECISION_V3_BYTES];
		uint32 cursor = 0;
		acquire_reset(); ready_ring_full = true;
		fake_pcm_clock_us = 100000;
		request = acquire_frame(1, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, target);
		UT_ASSERT_EQ(acquire_deliver(&request), RESOURCE_X_APPLY_APPLIED);
		work = bootstrap_next_work();
		UT_ASSERT(cluster_pcm_lock_authority_snapshot(make_tag(850), &before));
		UT_ASSERT_EQ(publish_tick(&work), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
			&request.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_PROOF);
		UT_ASSERT_EQ(snapshot.final_authority_generation, 0);
		UT_ASSERT_EQ(snapshot.assertion_sequence, request.common.assertion_sequence);
		/* Advance beyond the environmental FIFO refusal's due time. The
		 * real ready scanner supplies the retained canonical A. */
		fake_pcm_clock_us += 1000000;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_ready_intent_probe_exact(
			&request.common.logical_assertion.resource, &cursor, &slot), RESOURCE_X_INTENT_PROBE_FOUND);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(
			&slot, &captured, bytes, sizeof(bytes)), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
			bytes, sizeof(bytes), &a, &reject));
		UT_ASSERT(a.has_acquire_decision);
		UT_ASSERT_EQ(a.body.acquire_decision.source_kind, RESOURCE_X_ACQUIRE_SOURCE_DISK);
		UT_ASSERT(memcmp(&a.body.acquire_decision.episode, &request.body.acquire_intent,
			sizeof(request.body.acquire_intent)) == 0);
		UT_ASSERT_EQ(a.body.acquire_decision.enqueue_order, work.enqueue_order);
		UT_ASSERT(cluster_pcm_lock_authority_snapshot(make_tag(850), &after));
		UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
	}
}

UT_TEST(test_tick_canonical_disk_s_and_x_publish_conditional_a)
{
	for (PcmState target = PCM_STATE_S; target <= PCM_STATE_X; target++) {
		PublishFixture f;
		if (!publish_tick_start(0, target, &f)) continue;
		UT_ASSERT_EQ(publish_tick(&f.work), RESOURCE_X_APPLY_APPLIED);
		publish_check(&f); publish_tick_unchanged_directory(&f);
		UT_ASSERT_EQ(ready_enqueue_calls, 1);
	}
}

UT_TEST(test_tick_remote_x_source_owns_the_third_hop)
{
	for (PcmState target = PCM_STATE_S; target <= PCM_STATE_X; target++) {
		PublishFixture f;
		if (!publish_tick_start(1, target, &f)) continue;
		UT_ASSERT_EQ(publish_tick(&f.work), RESOURCE_X_APPLY_APPLIED);
		publish_check(&f); publish_tick_unchanged_directory(&f);
		UT_ASSERT_EQ(f.state->grant_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
		UT_ASSERT_EQ(ready_enqueue_calls, 1);
	}
}

UT_TEST(test_tick_shared_holders_arm_parallel_forwards_with_one_a)
{
	PublishFixture f;
	if (!publish_tick_start(2, PCM_STATE_X, &f)) return;
	UT_ASSERT_EQ(publish_tick(&f.work), RESOURCE_X_APPLY_APPLIED);
	publish_check(&f); publish_tick_unchanged_directory(&f);
	UT_ASSERT_EQ(ready_enqueue_calls, 3);
	UT_ASSERT_EQ(f.plan.decision.required_holders_bitmap, 13);
}

UT_TEST(test_tick_compatible_s_keeps_source_outside_required_set)
{
	PublishFixture f;
	if (!publish_tick_start(2, PCM_STATE_S, &f)) return;
	UT_ASSERT_EQ(publish_tick(&f.work), RESOURCE_X_APPLY_APPLIED);
	publish_check(&f); publish_tick_unchanged_directory(&f);
	UT_ASSERT_EQ(f.plan.decision.required_holders_bitmap, 0);
	UT_ASSERT_EQ(ready_enqueue_calls, 1);
}

UT_TEST(test_tick_requester_current_preserves_self_service)
{
	for (PcmState target = PCM_STATE_S; target <= PCM_STATE_X; target++) {
		PublishFixture f;
		if (!publish_tick_start(3, target, &f)) continue;
		UT_ASSERT_EQ(publish_tick(&f.work), RESOURCE_X_APPLY_APPLIED);
		publish_check(&f); publish_tick_unchanged_directory(&f);
		UT_ASSERT_EQ(f.plan.decision.source_kind, RESOURCE_X_ACQUIRE_SOURCE_LOCAL_CURRENT);
	}
}

UT_TEST(test_tick_replay_preserves_immutable_a_and_authority)
{
	for (int scenario = 0; scenario < 4; scenario++) {
		PublishFixture f;
		ClusterPcmResourceXMasterState before;
		if (!publish_tick_start(scenario, PCM_STATE_X, &f)) continue;
		UT_ASSERT_EQ(publish_tick(&f.work), RESOURCE_X_APPLY_APPLIED);
		before = *f.state;
		UT_ASSERT_EQ(publish_tick(&f.work), RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	}
}

UT_TEST(test_tick_unknown_observation_waits_on_original_owner)
{
	PublishFixture f;
	ClusterPcmResourceXMasterState before;
	if (!publish_tick_start(0, PCM_STATE_X, &f)) return;
	before = *f.state; auth_fresh = false;
	UT_ASSERT_EQ(publish_tick(&f.work), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	auth_fresh = true;
	UT_ASSERT_EQ(publish_tick(&f.work), RESOURCE_X_APPLY_APPLIED);
	publish_check(&f);
}

static void
publish_tick_unavailable_at_resume(void)
{
	auth_fresh = false;
}

UT_TEST(test_tick_rechecks_domain_between_canonical_resume_and_a)
{
	PublishFixture f;
	ClusterPcmResourceXMasterState before;
	if (!publish_tick_start(0, PCM_STATE_X, &f)) return;
	before = *f.state;
	object_entry_lock_hook = publish_tick_unavailable_at_resume;
	UT_ASSERT_EQ(publish_tick(&f.work), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	auth_fresh = true;
	UT_ASSERT_EQ(publish_tick(&f.work), RESOURCE_X_APPLY_APPLIED);
	publish_check(&f);
}

static void
publish_tick_boot_change_at_resume(void)
{
	auth_peer_boot++;
}

UT_TEST(test_tick_domain_change_after_resume_retires_only_unsent_work)
{
	PublishFixture f;
	ResourceXMasterSnapshot snapshot;
	if (!publish_tick_start(0, PCM_STATE_X, &f)) return;
	object_entry_lock_hook = publish_tick_boot_change_at_resume;
	UT_ASSERT_EQ(publish_tick(&f.work), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&f.request.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(f.state->authority_generation, f.plan.input.authority_generation);
	UT_ASSERT_EQ(f.state->bootstrap_receipts[1].highest_attempt_floor,
		f.work.request.assertion_sequence);
}

UT_TEST(test_tick_staged_continuation_survives_domain_failure)
{
	PublishFixture f;
	ClusterPcmResourceXMasterState before;
	ResourceXIntentSlot attempt;
	if (!publish_tick_start(1, PCM_STATE_X, &f)) return;
	UT_ASSERT_EQ(publish_tick(&f.work), RESOURCE_X_APPLY_APPLIED);
	fake_pcm_clock_us += 1000000;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
		&f.state->block_intents[0].slot, fake_pcm_clock_us, &attempt), RESOURCE_X_INTENT_STAGED);
	before = *f.state; auth_peer_boot++;
	UT_ASSERT_EQ(publish_tick(&f.work), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(
		&attempt, true, fake_pcm_clock_us + 1));
	UT_ASSERT_EQ(f.state->authority_generation, before.authority_generation);
}

UT_TEST(test_tick_fifo_refusal_does_not_publish_a_successor)
{
	PublishFixture f;
	ResourceXDecodedFrame second;
	ResourceXBootstrapWork forged;
	ClusterPcmResourceXMasterState before;
	if (!publish_tick_start(0, PCM_STATE_X, &f)) return;
	second = acquire_frame(2, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	UT_ASSERT_EQ(acquire_deliver(&second), RESOURCE_X_APPLY_APPLIED);
	forged = f.work; forged.request = second.common;
	forged.acquire_intent = second.body.acquire_intent; forged.enqueue_order++;
	before = *f.state;
	UT_ASSERT_EQ(publish_tick(&forged), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(publish_tick(&f.work), RESOURCE_X_APPLY_APPLIED);
	publish_check(&f);
}

UT_TEST(test_tick_exact_reference_cannot_publish_another_episode)
{
	PublishFixture f;
	ClusterPcmResourceXMasterState before;
	if (!publish_tick_start(0, PCM_STATE_X, &f)) return;
	before = *f.state;
	for (int mutation = 0; mutation < 7; mutation++) {
		ResourceXBootstrapWork wrong = f.work;
		if (mutation == 0) wrong.binding_generation++;
		if (mutation == 1) wrong.enqueue_order++;
		if (mutation == 2) wrong.acquire_intent.buffer_id++;
		if (mutation == 3) wrong.acquire_intent.backend_generation++;
		if (mutation == 4) wrong.acquire_intent.base_generation++;
		if (mutation == 5) wrong.acquire_intent.reservation_token++;
		if (mutation == 6) wrong.acquire_intent.requester_incarnation++;
		UT_ASSERT(publish_tick(&wrong) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	}
}

UT_TEST(test_tick_unknown_current_identity_never_falls_back_to_disk)
{
	PublishFixture f;
	ClusterPcmResourceXMasterState before;
	if (!publish_tick_start(1, PCM_STATE_S, &f)) return;
	memset(&f.state->holder_grants[0], 0, sizeof(f.state->holder_grants[0]));
	before = *f.state;
	UT_ASSERT_EQ(publish_tick(&f.work), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}

UT_TEST(test_tick_occupied_send_container_cannot_burn_a_generation)
{
	PublishFixture f;
	ClusterPcmResourceXMasterState before;
	if (!publish_tick_start(0, PCM_STATE_X, &f)) return;
	f.state->block_intents[31].slot.state = RESOURCE_X_INTENT_SLOT_ARMED;
	before = *f.state;
	UT_ASSERT(publish_tick(&f.work) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}

UT_TEST(test_tick_clock_failure_cannot_publish_partial_a)
{
	PublishFixture f;
	ClusterPcmResourceXMasterState before;
	if (!publish_tick_start(0, PCM_STATE_X, &f)) return;
	before = *f.state; publish_zero_clock = true;
	UT_ASSERT(publish_tick(&f.work) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	publish_zero_clock = false;
}

static void
publish_tick_throw_on_second_context(void)
{
	auth_observation_hook = acquire_throw_observation;
}

UT_TEST(test_tick_error_during_second_context_releases_admission)
{
	PublishFixture f;
	ClusterPcmResourceXMasterState before;
	volatile bool caught = false;
	if (!publish_tick_start(0, PCM_STATE_X, &f)) return;
	before = *f.state;
	object_entry_lock_hook = publish_tick_throw_on_second_context;
	fake_error_use_pg_exception_stack = true;
	PG_TRY();
	{
		(void)cluster_gcs_block_resource_x_bootstrap_tick(&f.work);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	fake_error_use_pg_exception_stack = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(master_durable_admission_depth, 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}

UT_TEST(test_tick_wrong_executor_and_null_are_not_authority)
{
	PublishFixture f;
	ClusterPcmResourceXMasterState before;
	if (!publish_tick_start(0, PCM_STATE_X, &f)) return;
	before = *f.state; MyBackendType = B_BACKEND;
	UT_ASSERT_EQ(publish_tick(&f.work), RESOURCE_X_APPLY_INVALID);
	MyBackendType = B_LMS;
	UT_ASSERT_EQ(publish_tick(NULL), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}

int
main(void)
{
	UT_PLAN(18);
	UT_RUN(test_tick_queued_disk_s_and_x_publish_without_another_request);
	UT_RUN(test_tick_canonical_disk_s_and_x_publish_conditional_a);
	UT_RUN(test_tick_remote_x_source_owns_the_third_hop);
	UT_RUN(test_tick_shared_holders_arm_parallel_forwards_with_one_a);
	UT_RUN(test_tick_compatible_s_keeps_source_outside_required_set);
	UT_RUN(test_tick_requester_current_preserves_self_service);
	UT_RUN(test_tick_replay_preserves_immutable_a_and_authority);
	UT_RUN(test_tick_unknown_observation_waits_on_original_owner);
	UT_RUN(test_tick_rechecks_domain_between_canonical_resume_and_a);
	UT_RUN(test_tick_domain_change_after_resume_retires_only_unsent_work);
	UT_RUN(test_tick_staged_continuation_survives_domain_failure);
	UT_RUN(test_tick_fifo_refusal_does_not_publish_a_successor);
	UT_RUN(test_tick_exact_reference_cannot_publish_another_episode);
	UT_RUN(test_tick_unknown_current_identity_never_falls_back_to_disk);
	UT_RUN(test_tick_occupied_send_container_cannot_burn_a_generation);
	UT_RUN(test_tick_clock_failure_cannot_publish_partial_a);
	UT_RUN(test_tick_error_during_second_context_releases_admission);
	UT_RUN(test_tick_wrong_executor_and_null_are_not_authority);
	UT_DONE();
}
