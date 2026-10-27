/*-------------------------------------------------------------------------
 * test_cluster_resource_x_acquire_ingress.c
 *    Actual GCS acquisition ingress and authenticated PCM admission FIFO.
 *
 * The PCM owner, wire codec, GCS consumer and double-sample classifier are
 * real. OS/semantic admission, master-session and peer observations use the
 * shared PCM fixture's explicit environment providers. No native installation
 * or canonical S/X service is substituted or claimed by this component test.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
int pcm_acquire_fixture_main(void);
#define main pcm_acquire_fixture_main
#include "test_cluster_pcm_lock.c"
#undef main

#include "test_cluster_resource_x_acquire_candidate.inc"

static void
acquire_reset(void)
{
	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	MyBackendType = B_LMS;
	ut_master_session = 31;
	master_durable_admission = true;
	master_durable_admission_depth = 0;
	ready_peer_ok = true;
	ready_connection_generation = 61;
	fake_rx_capability_generation = 61;
	bootstrap_ingress_generation = 61;
	bootstrap_ack_count = 0;
	auth_peer_boot = 101;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
}

static ResourceXDecodedFrame
acquire_frame(int32 node, uint8 path, PcmState observed, PcmState target)
{
	ResourceXDecodedFrame frame = make_resource_x_acquire_request(make_tag(850), node, path,
															 observed, target);
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject;
	uint8 wire[RESOURCE_X_ACQUIRE_V3_BYTES];
	uint16 len = 0;

	frame.body.acquire_intent.cluster_epoch = 17;
	frame.body.acquire_intent.requester_incarnation = node == cluster_node_id ? 31 : 101;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, &frame, wire, sizeof(wire),
											 &len, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X, wire, len, &decoded, &reject));
	return decoded;
}

static ClusterICEnvelope
acquire_envelope(int32 node)
{
	ClusterICEnvelope env = {0};
	env.msg_type = RESOURCE_X_MSG_ASSERT_X;
	env.source_node_id = node;
	env.dest_node_id = cluster_node_id;
	env.epoch = 17;
	env.payload_length = RESOURCE_X_ACQUIRE_V3_BYTES;
	return env;
}

static ResourceXApplyResult
acquire_deliver(const ResourceXDecodedFrame *frame)
{
	ClusterICEnvelope env = acquire_envelope(frame->common.logical_assertion.requester_node);
	ResourceXMasterSnapshot snapshot;
	ResourceXApplyResult result;

	memset(&snapshot, 0x5a, sizeof(snapshot));
	result = gcs_block_resource_x_bootstrapped_assert_ingress(&env, frame, 61, &snapshot);
	if (result == RESOURCE_X_APPLY_APPLIED || result == RESOURCE_X_APPLY_DUPLICATE) {
		UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_NONE);
		UT_ASSERT_EQ(snapshot.final_authority_generation, 0);
		UT_ASSERT_EQ(bootstrap_ack_count, 0);
	}
	UT_ASSERT_EQ(master_durable_admission_depth, 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	return result;
}

UT_TEST(test_acquire_payload_selects_only_exact_request_length)
{
	UT_ASSERT(gcs_block_resource_x_payload_candidate(RESOURCE_X_MSG_ASSERT_X,
														RESOURCE_X_ACQUIRE_V3_BYTES));
	UT_ASSERT(!gcs_block_resource_x_payload_candidate(RESOURCE_X_MSG_ASSERT_X, 159));
	UT_ASSERT(!gcs_block_resource_x_payload_candidate(RESOURCE_X_MSG_ASSERT_X, 161));
	UT_ASSERT(!gcs_block_resource_x_payload_candidate(RESOURCE_X_MSG_BLOCK_TO_N, 160));
	UT_ASSERT(gcs_block_resource_x_payload_candidate(RESOURCE_X_MSG_ASSERT_X,
														RESOURCE_X_CONTROL_V1_BYTES));
}

UT_TEST(test_acquire_all_ordinary_intents_queue_without_authority_or_ack)
{
	for (int path = RESOURCE_X_ACQUIRE_FIRST_FILL; path <= RESOURCE_X_ACQUIRE_RESIDENT; path++)
		for (int observed = PCM_STATE_N; observed <= PCM_STATE_X; observed++)
			for (int target = PCM_STATE_S; target <= PCM_STATE_X; target++) {
				ResourceXDecodedFrame frame;
				ResourceXBootstrapWork work;
				ResourceXMasterSnapshot master;
				PcmAuthoritySnapshot authority;
				ResourceXApplyResult result;
				if (path == RESOURCE_X_ACQUIRE_FIRST_FILL && observed != PCM_STATE_N)
					continue;
				acquire_reset();
				frame = acquire_frame(1, path, observed, target);
				result = acquire_deliver(&frame);
				UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
				if (result != RESOURCE_X_APPLY_APPLIED)
					continue;
				work = bootstrap_next_work();
				UT_ASSERT(work.has_acquire_intent);
				UT_ASSERT(memcmp(&work.acquire_intent, &frame.body.acquire_intent,
								 sizeof(work.acquire_intent)) == 0);
				UT_ASSERT_EQ(work.request.observed_mode, observed);
				UT_ASSERT_EQ(work.request.target_mode, target);
				UT_ASSERT_EQ(work.authenticated_ingress_connection_generation, 61);
				UT_ASSERT_EQ(work.master_sender_connection_generation, 61);
				UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
								 &frame.common.logical_assertion, &master), RESOURCE_X_APPLY_NOT_FOUND);
				UT_ASSERT(cluster_pcm_lock_authority_snapshot(frame.common.logical_assertion.resource,
															 &authority));
				UT_ASSERT_EQ(authority.state, PCM_STATE_N);
				UT_ASSERT_EQ(authority.x_holder_node, -1);
				UT_ASSERT(auth_slot_reads >= 4);
			}
}

UT_TEST(test_acquire_local_request_uses_self_boot_without_remote_observation)
{
	ResourceXDecodedFrame frame;
	ResourceXApplyResult result;
	acquire_reset();
	frame = acquire_frame(0, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	auth_slot_valid = auth_connection_valid = false;
	result = acquire_deliver(&frame);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(auth_slot_reads, 0);
	UT_ASSERT_EQ(auth_connection_reads, 0);
	if (result == RESOURCE_X_APPLY_APPLIED)
		UT_ASSERT_EQ(bootstrap_next_work().acquire_intent.requester_incarnation, 31);
}

UT_TEST(test_acquire_envelope_cannot_rebind_requester_or_domain)
{
	for (int variant = 0; variant < 6; variant++) {
		ResourceXDecodedFrame frame;
		ResourceXMasterSnapshot snapshot;
		ClusterICEnvelope env;
		acquire_reset();
		frame = acquire_frame(1, RESOURCE_X_ACQUIRE_RESIDENT, PCM_STATE_N, PCM_STATE_X);
		env = acquire_envelope(1);
		switch (variant) {
		case 0: env.source_node_id = 2; break;
		case 1: env.dest_node_id = 2; break;
		case 2: env.msg_type = RESOURCE_X_MSG_BLOCK_TO_N; break;
		case 3: env.epoch++; break;
		case 4: env.payload_length--; break;
		case 5: frame.body.acquire_intent.cluster_epoch++; break;
		}
		UT_ASSERT(gcs_block_resource_x_bootstrapped_assert_ingress(&env, &frame, 61, &snapshot)
				  != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active(&frame.common.logical_assertion.resource));
		UT_ASSERT_EQ(master_durable_admission_depth, 0);
	}
}

UT_TEST(test_acquire_coherent_wrong_boot_is_stale_not_admitted)
{
	ResourceXDecodedFrame frame;
	acquire_reset();
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_RESIDENT, PCM_STATE_S, PCM_STATE_X);
	auth_peer_boot++;
	UT_ASSERT_EQ(acquire_deliver(&frame), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active(&frame.common.logical_assertion.resource));
}

UT_TEST(test_acquire_unavailable_boot_observation_never_creates_work)
{
	for (int variant = 0; variant < 4; variant++) {
		ResourceXDecodedFrame frame;
		acquire_reset();
		frame = acquire_frame(1, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
		switch (variant) {
		case 0: auth_slot_valid = false; break;
		case 1: auth_connection_valid = false; break;
		case 2: auth_fresh = false; break;
		case 3: auth_epoch++; break;
		}
		UT_ASSERT_EQ(acquire_deliver(&frame), RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active(&frame.common.logical_assertion.resource));
	}
}

static void
acquire_boot_turnover(int32 node pg_attribute_unused(), unsigned read)
{
	if (read == 3)
		auth_peer_boot++;
}

UT_TEST(test_acquire_rechecks_boot_before_retaining_complete_episode)
{
	ResourceXDecodedFrame frame;
	acquire_reset();
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_X);
	auth_observation_hook = acquire_boot_turnover;
	UT_ASSERT_EQ(acquire_deliver(&frame), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active(&frame.common.logical_assertion.resource));
}

static void
acquire_torn_observation(int32 node pg_attribute_unused(), unsigned read)
{
	if (read == 2)
		auth_slot_generation++;
}

UT_TEST(test_acquire_torn_double_sample_is_local_unavailability)
{
	ResourceXDecodedFrame frame;
	acquire_reset();
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	auth_observation_hook = acquire_torn_observation;
	UT_ASSERT_EQ(acquire_deliver(&frame), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active(&frame.common.logical_assertion.resource));
}

static void
acquire_throw_observation(int32 node pg_attribute_unused(), unsigned read pg_attribute_unused())
{
	ereport(ERROR, (errmsg("acquisition observation interrupted")));
}

UT_TEST(test_acquire_error_releases_owned_semantic_admission)
{
	ResourceXDecodedFrame frame;
	volatile bool caught = false;
	acquire_reset();
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	auth_observation_hook = acquire_throw_observation;
	fake_error_use_pg_exception_stack = true;
	PG_TRY();
	{
		(void)acquire_deliver(&frame);
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
	UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active(&frame.common.logical_assertion.resource));
}

UT_TEST(test_acquire_duplicate_keeps_exact_work_without_comparing_wire_generation)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work, after;
	ResourceXApplyResult result;
	acquire_reset();
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	UT_ASSERT(frame.common.sender_connection_generation != 61);
	result = acquire_deliver(&frame);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	if (result != RESOURCE_X_APPLY_APPLIED)
		return;
	work = bootstrap_next_work();
	UT_ASSERT_EQ(acquire_deliver(&frame), RESOURCE_X_APPLY_DUPLICATE);
	after = bootstrap_next_work();
	UT_ASSERT(memcmp(&work, &after, sizeof(work)) == 0);
	UT_ASSERT_EQ(bootstrap_ack_count, 0);
}

UT_TEST(test_acquire_unknown_queued_observation_preserves_original_work)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work, after;
	ResourceXApplyResult result;
	acquire_reset();
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_RESIDENT, PCM_STATE_N, PCM_STATE_X);
	result = acquire_deliver(&frame);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	if (result != RESOURCE_X_APPLY_APPLIED)
		return;
	work = bootstrap_next_work();
	auth_fresh = false;
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&work), RESOURCE_X_APPLY_BAD_STATE);
	after = bootstrap_next_work();
	UT_ASSERT(memcmp(&work, &after, sizeof(work)) == 0);
	UT_ASSERT_EQ(bootstrap_ack_count, 0);
	UT_ASSERT_EQ(master_durable_admission_depth, 0);
}

UT_TEST(test_acquire_boot_change_retires_only_same_unconsumed_work)
{
	ResourceXDecodedFrame frame, successor;
	ResourceXBootstrapWork work, next;
	ResourceXApplyResult result;
	acquire_reset();
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_RESIDENT, PCM_STATE_N, PCM_STATE_X);
	result = acquire_deliver(&frame);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	if (result != RESOURCE_X_APPLY_APPLIED)
		return;
	work = bootstrap_next_work();
	successor = acquire_frame(2, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	UT_ASSERT_EQ(acquire_deliver(&successor), RESOURCE_X_APPLY_APPLIED);
	auth_peer_boot++;
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&work), RESOURCE_X_APPLY_STALE);
	next = bootstrap_next_work();
	UT_ASSERT_EQ(next.request.logical_assertion.requester_node, 2);
	UT_ASSERT_EQ(next.acquire_intent.requester_incarnation, 101);
	/* The same late work cannot consume R2's different E. */
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&work), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(bootstrap_next_work().enqueue_order, next.enqueue_order);
	UT_ASSERT_EQ(bootstrap_ack_count, 0);
}

UT_TEST(test_acquire_queued_domain_change_requires_exact_old_work)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work, stale;
	ResourceXApplyResult result;
	acquire_reset();
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	result = acquire_deliver(&frame);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	if (result != RESOURCE_X_APPLY_APPLIED)
		return;
	work = bootstrap_next_work();
	stale = work;
	stale.acquire_intent.reservation_token++;
	auth_peer_boot++;
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&stale), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(bootstrap_next_work().acquire_intent.reservation_token,
				 work.acquire_intent.reservation_token);
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&work), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active(&frame.common.logical_assertion.resource));
}

UT_TEST(test_acquire_domain_and_exhausted_identity_refusals_precede_queue)
{
	for (int variant = 0; variant < 9; variant++) {
		ResourceXDecodedFrame frame;
		ResourceXMasterSnapshot snapshot;
		ClusterICEnvelope env;
		uint32 ingress = 61;
		acquire_reset();
		frame = acquire_frame(1, RESOURCE_X_ACQUIRE_RESIDENT, PCM_STATE_S, PCM_STATE_X);
		env = acquire_envelope(1);
		switch (variant) {
		case 0: frame.body.acquire_intent.r4_generation++; break;
		case 1: frame.common.resource_formation++; break;
		case 2: frame.common.master_session_incarnation++; break;
		case 3: fake_gcs_master_node = 2; break;
		case 4: ingress++; break;
		case 5: ingress = UINT32_MAX; break;
		case 6: auth_peer_boot = UINT64_MAX; break;
		case 7: auth_slot_generation = UINT64_MAX; break;
		case 8: ready_connection_generation = UINT32_MAX; break;
		}
		UT_ASSERT(gcs_block_resource_x_bootstrapped_assert_ingress(&env, &frame, ingress, &snapshot)
				  != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active(&frame.common.logical_assertion.resource));
		UT_ASSERT_EQ(master_durable_admission_depth, 0);
	}
}

static int acquire_drift_variant;
static void
acquire_domain_turnover(int32 node pg_attribute_unused(), unsigned read)
{
	if (read != 3)
		return;
	switch (acquire_drift_variant) {
	case 0: ut_master_session++; break;
	case 1: ready_connection_generation++; break;
	case 2: master_durable_admission = false; break;
	}
}

UT_TEST(test_acquire_final_independent_domain_checks_close_observation_window)
{
	for (acquire_drift_variant = 0; acquire_drift_variant < 3; acquire_drift_variant++) {
		ResourceXDecodedFrame frame;
		acquire_reset();
		frame = acquire_frame(1, RESOURCE_X_ACQUIRE_RESIDENT, PCM_STATE_N, PCM_STATE_X);
		auth_observation_hook = acquire_domain_turnover;
		UT_ASSERT_EQ(acquire_deliver(&frame), RESOURCE_X_APPLY_STALE);
		UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active(&frame.common.logical_assertion.resource));
	}
}

UT_TEST(test_acquire_queued_error_returns_admission_without_losing_original_work)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work, after;
	ResourceXApplyResult result;
	volatile bool caught = false;
	acquire_reset();
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	result = acquire_deliver(&frame);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	if (result != RESOURCE_X_APPLY_APPLIED)
		return;
	work = bootstrap_next_work();
	auth_observation_hook = acquire_throw_observation;
	fake_error_use_pg_exception_stack = true;
	PG_TRY();
	{
		(void)cluster_gcs_block_resource_x_bootstrap_tick(&work);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	fake_error_use_pg_exception_stack = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(master_durable_admission_depth, 0);
	after = bootstrap_next_work();
	UT_ASSERT(memcmp(&work, &after, sizeof(work)) == 0);
}

int
main(void)
{
	UT_PLAN(16);
	UT_RUN(test_acquire_payload_selects_only_exact_request_length);
	UT_RUN(test_acquire_all_ordinary_intents_queue_without_authority_or_ack);
	UT_RUN(test_acquire_local_request_uses_self_boot_without_remote_observation);
	UT_RUN(test_acquire_envelope_cannot_rebind_requester_or_domain);
	UT_RUN(test_acquire_coherent_wrong_boot_is_stale_not_admitted);
	UT_RUN(test_acquire_unavailable_boot_observation_never_creates_work);
	UT_RUN(test_acquire_rechecks_boot_before_retaining_complete_episode);
	UT_RUN(test_acquire_torn_double_sample_is_local_unavailability);
	UT_RUN(test_acquire_error_releases_owned_semantic_admission);
	UT_RUN(test_acquire_duplicate_keeps_exact_work_without_comparing_wire_generation);
	UT_RUN(test_acquire_unknown_queued_observation_preserves_original_work);
	UT_RUN(test_acquire_boot_change_retires_only_same_unconsumed_work);
	UT_RUN(test_acquire_queued_domain_change_requires_exact_old_work);
	UT_RUN(test_acquire_domain_and_exhausted_identity_refusals_precede_queue);
	UT_RUN(test_acquire_final_independent_domain_checks_close_observation_window);
	UT_RUN(test_acquire_queued_error_returns_admission_without_losing_original_work);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
