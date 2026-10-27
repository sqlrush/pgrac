/*-------------------------------------------------------------------------
 * test_cluster_resource_x_acquire_ingress.c
 *    Actual GCS acquisition ingress and authenticated PCM admission FIFO.
 *
 * The PCM owner, wire codec, GCS consumer and double-sample classifier are
 * real. OS/semantic admission, master-session and peer observations use the
 * shared PCM fixture's explicit environment providers. Canonical admission is
 * real; source decisions, native installation and completion are not claimed.
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

static bool
acquire_canonical_start(ResourceXDecodedFrame *frame, ResourceXBootstrapWork *work)
{
	ResourceXApplyResult result = acquire_deliver(frame);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	if (result != RESOURCE_X_APPLY_APPLIED)
		return false;
	*work = bootstrap_next_work();
	result = cluster_gcs_block_resource_x_bootstrap_tick(work);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	return result == RESOURCE_X_APPLY_APPLIED;
}

static void
acquire_canonical_check(const ResourceXDecodedFrame *frame)
{
	ResourceXMasterSnapshot snapshot;
	PcmAuthoritySnapshot authority;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&frame->common.logical_assertion, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_QUEUED);
	UT_ASSERT_EQ(snapshot.base_authority_generation, 1);
	UT_ASSERT_EQ(snapshot.resource_formation, frame->common.resource_formation);
	UT_ASSERT_EQ(snapshot.master_session_incarnation, frame->common.master_session_incarnation);
	UT_ASSERT_EQ(snapshot.assertion_sequence, frame->common.assertion_sequence);
	UT_ASSERT_EQ(snapshot.final_authority_generation, 0);
	UT_ASSERT_EQ(snapshot.incompatible_holders_bitmap, 0);
	UT_ASSERT_EQ(snapshot.blocked_holders_bitmap, 0);
	UT_ASSERT_EQ(snapshot.source_node, -1);
	UT_ASSERT(snapshot.is_head);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(frame->common.logical_assertion.resource,
		&authority));
	UT_ASSERT_EQ(authority.state, PCM_STATE_N);
	UT_ASSERT_EQ(authority.x_holder_node, -1);
	UT_ASSERT_EQ(authority.pending_x_requester_node, -1);
	UT_ASSERT_EQ(bootstrap_ack_count, 0);
	UT_ASSERT_EQ(master_durable_admission_depth, 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
}

UT_TEST(test_acquire_canonical_all_modes_retain_one_unsealed_identity)
{
	for (int path = RESOURCE_X_ACQUIRE_FIRST_FILL; path <= RESOURCE_X_ACQUIRE_RESIDENT; path++)
		for (int observed = PCM_STATE_N; observed <= PCM_STATE_X; observed++)
			for (int target = PCM_STATE_S; target <= PCM_STATE_X; target++) {
				ResourceXDecodedFrame frame;
				ResourceXBootstrapWork work, after;
				if (path == RESOURCE_X_ACQUIRE_FIRST_FILL && observed != PCM_STATE_N)
					continue;
				acquire_reset();
				frame = acquire_frame(1, path, observed, target);
				if (!acquire_canonical_start(&frame, &work))
					return;
				acquire_canonical_check(&frame);
				after = bootstrap_next_work();
				UT_ASSERT(memcmp(&work, &after, sizeof(work)) == 0);
				UT_ASSERT_EQ(after.request.observed_mode, observed);
				UT_ASSERT_EQ(after.request.target_mode, target);
			}
}

UT_TEST(test_acquire_canonical_duplicate_does_not_allocate_another_order)
{
	ResourceXDecodedFrame frame, successor, out;
	ResourceXBootstrapWork work, next;
	ResourceXMasterSnapshot before, after;
	acquire_reset();
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_X);
	if (!acquire_canonical_start(&frame, &work))
		return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&frame.common.logical_assertion, &before), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(acquire_deliver(&frame), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&work), RESOURCE_X_APPLY_DUPLICATE);
	memset(&out, 0x5a, sizeof(out));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_resume_exact(&work, &out),
		RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(out.kind, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&frame.common.logical_assertion, &after), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
	successor = acquire_frame(2, RESOURCE_X_ACQUIRE_RESIDENT, PCM_STATE_S, PCM_STATE_X);
	UT_ASSERT_EQ(acquire_deliver(&successor), RESOURCE_X_APPLY_APPLIED);
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&work);
	next = bootstrap_next_work();
	UT_ASSERT_EQ(next.enqueue_order, work.enqueue_order + 1);
	UT_ASSERT_EQ(next.request.logical_assertion.requester_node, 2);
}

UT_TEST(test_acquire_canonical_head_cannot_be_overtaken_by_later_work)
{
	ResourceXDecodedFrame a, b, out;
	ResourceXBootstrapWork first, forged, next;
	ResourceXMasterSnapshot snapshot;
	acquire_reset();
	a = acquire_frame(1, RESOURCE_X_ACQUIRE_RESIDENT, PCM_STATE_N, PCM_STATE_X);
	b = acquire_frame(2, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	UT_ASSERT_EQ(acquire_deliver(&a), RESOURCE_X_APPLY_APPLIED);
	first = bootstrap_next_work();
	UT_ASSERT_EQ(acquire_deliver(&b), RESOURCE_X_APPLY_APPLIED);
	forged = first;
	forged.request = b.common;
	forged.acquire_intent = b.body.acquire_intent;
	forged.enqueue_order++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_resume_exact(&forged, &out),
		RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&b.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&first), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_resume_exact(&forged, &out),
		RESOURCE_X_APPLY_BAD_STATE);
	acquire_canonical_check(&a);
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&first);
	next = bootstrap_next_work();
	UT_ASSERT(memcmp(&forged, &next, sizeof(next)) == 0);
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&next), RESOURCE_X_APPLY_APPLIED);
	acquire_canonical_check(&b);
}

UT_TEST(test_acquire_canonical_legacy_replay_cannot_start_x_service)
{
	ResourceXDecodedFrame frame, legacy;
	ResourceXBootstrapWork work, after;
	ResourceXMasterSnapshot snapshot;
	acquire_reset();
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_RESIDENT, PCM_STATE_N, PCM_STATE_S);
	if (!acquire_canonical_start(&frame, &work))
		return;
	legacy = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X,
		frame.common.logical_assertion.resource, 1, 1);
	legacy.common.assertion_sequence = frame.common.assertion_sequence;
	legacy.common.ordered_lane = 0;
	legacy.payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&legacy, 1, &snapshot),
		RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&legacy, 1, 61, 77, 31, 61, &snapshot), RESOURCE_X_APPLY_BAD_STATE);
	acquire_canonical_check(&frame);
	after = bootstrap_next_work();
	UT_ASSERT(memcmp(&work, &after, sizeof(work)) == 0);
}

UT_TEST(test_acquire_canonical_cancel_requires_full_episode_and_binding)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work, changed, after;
	ResourceXMasterSnapshot snapshot;
	acquire_reset();
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_RESIDENT, PCM_STATE_N, PCM_STATE_X);
	if (!acquire_canonical_start(&frame, &work))
		return;
	for (int drift = 0; drift < 22; drift++) {
		changed = work;
		switch (drift) {
		case 0: changed.acquire_intent.requester_incarnation++; break;
		case 1: changed.acquire_intent.backend_generation++; break;
		case 2: changed.acquire_intent.base_generation++; break;
		case 3: changed.acquire_intent.reservation_token++; break;
		case 4: changed.acquire_intent.cluster_epoch++; break;
		case 5: changed.acquire_intent.r4_generation++; break;
		case 6: changed.acquire_intent.backend_id++; break;
		case 7: changed.acquire_intent.buffer_id++; break;
		case 8: changed.acquire_intent.path = RESOURCE_X_ACQUIRE_FIRST_FILL; break;
		case 9: changed.binding_generation++; break;
		case 10: changed.enqueue_order++; break;
		case 11: changed.r4_record_generation++; break;
		case 12: changed.authenticated_ingress_connection_generation++; break;
		case 13: changed.master_sender_connection_generation++; break;
		case 14: changed.request.assertion_sequence++; break;
		case 15: changed.request.sender_connection_generation++; break;
		case 16: changed.request.target_mode = PCM_STATE_S; break;
		case 17: changed.request.observed_mode = PCM_STATE_S; break;
		case 18: changed.has_acquire_intent = false; break;
		case 19: changed.reserved[0] = 1; break;
		case 20: changed.request.master_session_incarnation++; break;
		case 21: changed.request.resource_formation++; break;
		}
		cluster_pcm_lock_resource_x_bootstrap_forget_exact(&changed);
		after = bootstrap_next_work();
		UT_ASSERT(memcmp(&work, &after, sizeof(work)) == 0);
		acquire_canonical_check(&frame);
	}
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&work);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&frame.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active(&frame.common.logical_assertion.resource));
}

UT_TEST(test_acquire_canonical_cancel_preserves_floor_and_late_work_cannot_touch_successor)
{
	ResourceXDecodedFrame frame, successor, out;
	ResourceXBootstrapWork work, next;
	ResourceXMasterSnapshot snapshot;
	acquire_reset();
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_RESIDENT, PCM_STATE_N, PCM_STATE_X);
	if (!acquire_canonical_start(&frame, &work))
		return;
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&work);
	UT_ASSERT_EQ(acquire_deliver(&frame), RESOURCE_X_APPLY_STALE);
	successor = frame;
	successor.common.assertion_sequence++;
	successor.body.acquire_intent.reservation_token++;
	UT_ASSERT_EQ(acquire_deliver(&successor), RESOURCE_X_APPLY_APPLIED);
	next = bootstrap_next_work();
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&next), RESOURCE_X_APPLY_APPLIED);
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&work);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_resume_exact(&work, &out), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&successor.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.assertion_sequence, successor.common.assertion_sequence);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_QUEUED);
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&next), RESOURCE_X_APPLY_DUPLICATE);
}

UT_TEST(test_acquire_canonical_unknown_observation_keeps_pair_but_boot_change_retires_it)
{
	ResourceXDecodedFrame frame, successor;
	ResourceXBootstrapWork work, after;
	ResourceXMasterSnapshot snapshot;
	acquire_reset();
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_RESIDENT, PCM_STATE_N, PCM_STATE_X);
	if (!acquire_canonical_start(&frame, &work))
		return;
	successor = acquire_frame(2, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	UT_ASSERT_EQ(acquire_deliver(&successor), RESOURCE_X_APPLY_APPLIED);
	auth_fresh = false;
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&work), RESOURCE_X_APPLY_BAD_STATE);
	after = bootstrap_next_work();
	UT_ASSERT(memcmp(&work, &after, sizeof(work)) == 0);
	acquire_canonical_check(&frame);
	auth_fresh = true;
	auth_peer_boot++;
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&work), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&frame.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_NOT_FOUND);
	after = bootstrap_next_work();
	UT_ASSERT_EQ(after.request.logical_assertion.requester_node, 2);
}

UT_TEST(test_acquire_canonical_domain_change_retires_only_unsealed_exact_pair)
{
	for (int drift = 0; drift < 4; drift++) {
		ResourceXDecodedFrame frame;
		ResourceXBootstrapWork work;
		ResourceXMasterSnapshot snapshot;
		acquire_reset();
		frame = acquire_frame(1, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
		if (!acquire_canonical_start(&frame, &work))
			return;
		switch (drift) {
		case 0: ut_master_session++; break;
		case 1: ready_connection_generation++; break;
		case 2: bootstrap_ingress_generation++; break;
		case 3: fake_gcs_master_node = 2; break;
		}
		UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&work), RESOURCE_X_APPLY_STALE);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
			&frame.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_NOT_FOUND);
		UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active(&frame.common.logical_assertion.resource));
	}
}

UT_TEST(test_acquire_canonical_stop_seal_allows_existing_handoff_and_cancel)
{
	ResourceXDecodedFrame frame, newcomer;
	ResourceXBootstrapWork work;
	ResourceXMasterSnapshot snapshot;
	acquire_reset();
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	UT_ASSERT_EQ(acquire_deliver(&frame), RESOURCE_X_APPLY_APPLIED);
	work = bootstrap_next_work();
	stop_new_work_allowed = false;
	newcomer = acquire_frame(2, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	UT_ASSERT_EQ(acquire_deliver(&newcomer), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&work), RESOURCE_X_APPLY_APPLIED);
	acquire_canonical_check(&frame);
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&work);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&frame.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active(&frame.common.logical_assertion.resource));
}

UT_TEST(test_acquire_canonical_reconfiguration_retires_pair_and_retains_floor)
{
	ResourceXDecodedFrame frame, next, out;
	ResourceXBootstrapWork work;
	ResourceXMasterSnapshot snapshot;
	ResourceXReconfigToken token;
	ResourceXReconfigBatch batch;
	acquire_reset();
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_RESIDENT, PCM_STATE_N, PCM_STATE_X);
	if (!acquire_canonical_start(&frame, &work))
		return;
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch), RESOURCE_X_RECONFIG_MORE);
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch), RESOURCE_X_RECONFIG_DONE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&frame.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT(cluster_resource_x_reconfig_thaw_exact(&token));
	next = frame;
	next.common.resource_formation = 18;
	next.common.master_session_incarnation = 32;
	next.body.acquire_intent.cluster_epoch = 18;
	next.body.acquire_intent.r4_generation = 78;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&next, 1, 61, 78, 32, 61, &out), RESOURCE_X_APPLY_STALE);
	next.common.assertion_sequence++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_request_exact(
		&next, 1, 61, 78, 32, 61, &out), RESOURCE_X_APPLY_APPLIED);
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&work);
	UT_ASSERT_EQ(bootstrap_next_work().request.resource_formation, 18);
}

UT_TEST(test_acquire_canonical_node_only_death_preserves_pair_typed_death_retires_it)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work, after;
	ResourceXReconfigToken token;
	ResourceXReclaimWitness witness;
	ResourceXMasterSnapshot snapshot;
	acquire_reset();
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_RESIDENT, PCM_STATE_N, PCM_STATE_S);
	if (!acquire_canonical_start(&frame, &work))
		return;
	(void)cluster_pcm_lock_cleanup_on_node_dead(1);
	acquire_canonical_check(&frame);
	after = bootstrap_next_work();
	UT_ASSERT(memcmp(&work, &after, sizeof(work)) == 0);
	UT_ASSERT(cluster_resource_x_reconfig_freeze_exact(17, 18, UINT32_C(1) << 1, &token));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_reclaim_requester_exact(
		&frame.common.logical_assertion.resource, 1, 17, &witness),
		RESOURCE_X_RECLAIM_HEAD_SUCCESSOR_STARTED);
	UT_ASSERT_EQ(witness.source_evidence_preserved, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&frame.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active(&frame.common.logical_assertion.resource));
}

UT_TEST(test_acquire_canonical_uncertain_responsibility_cannot_be_forgotten)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work;
	ClusterPcmResourceXMasterRequest original = {0}, changed;
	char *bytes = NULL;
	int matches = 0;
	acquire_reset();
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_RESIDENT, PCM_STATE_N, PCM_STATE_X);
	if (!acquire_canonical_start(&frame, &work))
		return;
	original.base_authority_generation = 1;
	original.resource_formation = frame.common.resource_formation;
	original.master_session_incarnation = frame.common.master_session_incarnation;
	original.assertion_sequence = frame.common.assertion_sequence;
	original.enqueue_order = work.enqueue_order;
	original.sender_connection_generation = frame.common.sender_connection_generation;
	original.source_node = -1;
	original.phase = RESOURCE_X_MASTER_QUEUED;
	/* Locate the actual record by the product's extracted type and exact
	 * initialized bytes, without an extra production test hook or offset. */
	for (Size off = 0; off + sizeof(original) <= fake_pcm_header_requested_size; off++)
		if (memcmp(fake_pcm_header.data + off, &original, sizeof(original)) == 0) {
			bytes = fake_pcm_header.data + off;
			matches++;
		}
	UT_ASSERT_EQ(matches, 1);
	if (matches != 1)
		return;
	for (int debt = 0; debt < 12; debt++) {
		changed = original;
		switch (debt) {
		case 0: changed.final_authority_generation = 2; break;
		case 1: changed.delegated_authority_generation = 2; break;
		case 2: changed.head_transition_generation = 1; break;
		case 3: changed.proof_kind = RESOURCE_X_PROOF_LOCAL_IMAGE; break;
		case 4: changed.source_node = 2; break;
		case 5: changed.source_carrier_generation = 1; break;
		case 6: changed.requester_target_generation = 1; break;
		case 7: changed.incompatible_holders_bitmap = 4; break;
		case 8: changed.blocked_holders_bitmap = 4; break;
		case 9: changed.dependencies[0] = 1; break;
		case 10: changed.phase = RESOURCE_X_MASTER_WAIT_PROOF; break;
		case 11: changed.reserved = 1; break;
		}
		memcpy(bytes, &changed, sizeof(changed));
		cluster_pcm_lock_resource_x_bootstrap_forget_exact(&work);
		UT_ASSERT(memcmp(bytes, &changed, sizeof(changed)) == 0);
		memcpy(bytes, &original, sizeof(original));
		acquire_canonical_check(&frame);
	}
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&work);
	UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active(&frame.common.logical_assertion.resource));
}

UT_TEST(test_acquire_canonical_waits_for_original_pending_s_without_reserving_a_source)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork warm, work, actual;
	ResourceXMasterSnapshot snapshot;
	ClusterPcmReadGrant grant;
	PcmAuthoritySnapshot authority;
	acquire_reset();
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_RESIDENT, PCM_STATE_N, PCM_STATE_X);
	UT_ASSERT_EQ(acquire_deliver(&frame), RESOURCE_X_APPLY_APPLIED);
	warm = bootstrap_next_work();
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&warm);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(frame.common.logical_assertion.resource,
		PCM_TRANS_N_TO_S, 0));
	UT_ASSERT_EQ(cluster_pcm_lock_read_grant_begin(frame.common.logical_assertion.resource,
		3, 7, 21, 17, 99999, true, &grant), PCM_GCS_TRANSITION_APPLIED);
	frame.common.assertion_sequence++;
	UT_ASSERT_EQ(acquire_deliver(&frame), RESOURCE_X_APPLY_APPLIED);
	/* Simulate an already-copied LMS work reference reaching its entry after
	 * pending-S occupies the original service. All values derive from the
	 * real preceding work; the real scanner must later return identical E. */
	work = warm;
	work.request = frame.common;
	work.enqueue_order++;
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&work), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&frame.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(bootstrap_ack_count, 0);
	UT_ASSERT(cluster_pcm_lock_read_grant_done(&grant.key, 3, true));
	actual = bootstrap_next_work();
	UT_ASSERT(memcmp(&work, &actual, sizeof(work)) == 0);
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&actual), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&frame.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_QUEUED);
	UT_ASSERT_EQ(snapshot.final_authority_generation, 0);
	UT_ASSERT_EQ(snapshot.source_node, -1);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(frame.common.logical_assertion.resource, &authority));
	UT_ASSERT_EQ(authority.state, PCM_STATE_S);
	UT_ASSERT_EQ(authority.s_holders_bitmap, 9);
	UT_ASSERT_EQ(bootstrap_ack_count, 0);
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&actual);
}

UT_TEST(test_acquire_canonical_completed_send_payload_is_not_live_capacity)
{
	BufferTag tag = make_tag(850);
	ResourceXAssertion predecessor;
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work;
	ResourceXMasterSnapshot snapshot;

	/* This existing fixture executes the actual grant stage/send-completion
	 * and INSTALL_SETTLEMENT owners. Their payload bytes remain historical. */
	setup_local_settled_terminal_without_physical(tag);
	UT_ASSERT(resource_x_assertion_init(&tag, 0, &predecessor));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(&predecessor, &snapshot),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_SETTLED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_settled_retire_exact(
		&predecessor, snapshot.assertion_sequence, &snapshot), RESOURCE_X_APPLY_APPLIED);
	MyBackendType = B_LMS;
	ut_master_session = 31;
	master_durable_admission = true;
	ready_peer_ok = true;
	ready_connection_generation = 61;
	fake_rx_capability_generation = 61;
	bootstrap_ingress_generation = 61;
	auth_peer_boot = 101;
	frame = acquire_frame(1, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	if (!acquire_canonical_start(&frame, &work))
		return;
	UT_ASSERT_EQ(work.enqueue_order, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&frame.common.logical_assertion, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_QUEUED);
	UT_ASSERT_EQ(snapshot.base_authority_generation, 2);
	UT_ASSERT_EQ(snapshot.source_node, -1);
	UT_ASSERT_EQ(snapshot.final_authority_generation, 0);
	UT_ASSERT_EQ(bootstrap_ack_count, 0);
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&work);
	UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active(&tag));
}

static void
service_directory_transition(PcmLockTransition transition, int32 node)
{
	struct StopPcmEntryLayout *entry = (struct StopPcmEntryLayout *)fake_pcm_entries.data[0];
	LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
	cluster_pcm_transition_apply((struct GrdEntry *)entry, transition, node);
	LWLockRelease(&entry->entry_lock.lock);
}

static bool
service_snapshot_start(uint8 observed, uint8 target, ResourceXDecodedFrame *frame,
					   ResourceXBootstrapWork *work)
{
	acquire_reset();
	*frame = acquire_frame(1, RESOURCE_X_ACQUIRE_RESIDENT, observed, target);
	return acquire_canonical_start(frame, work);
}

static void
service_snapshot_refused(const ResourceXBootstrapWork *work,
						 const ResourceXAcquireServiceSnapshot *expected,
						 ResourceXApplyResult result)
{
	ResourceXAcquireServiceSnapshot out, original;

	memset(&out, 0xa5, sizeof(out));
	original = out;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(work, expected, &out), result);
	UT_ASSERT(memcmp(&out, &original, sizeof(out)) == 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT_EQ(master_durable_admission_depth, 0);
}

UT_TEST(test_service_snapshot_exact_n_head_is_input_not_authority)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work;
	ResourceXAcquireServiceSnapshot out;
	ResourceXMasterSnapshot before, after;
	char unchanged[sizeof(fake_pcm_entries)];

	if (!service_snapshot_start(PCM_STATE_N, PCM_STATE_S, &frame, &work))
		return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(&frame.common.logical_assertion,
		&before), RESOURCE_X_APPLY_APPLIED);
	memcpy(unchanged, &fake_pcm_entries, sizeof(unchanged));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(&work, NULL, &out),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&out.work, &work, sizeof(work)) == 0);
	UT_ASSERT_EQ(out.master_node, 0);
	UT_ASSERT_EQ(out.authority_generation, 1);
	UT_ASSERT_EQ(out.canonical_base_generation, 1);
	UT_ASSERT_EQ(out.directory.state, PCM_STATE_N);
	UT_ASSERT_EQ(out.current_holders_bitmap, 0);
	UT_ASSERT_EQ(out.required_holders_bitmap, 0);
	UT_ASSERT_EQ(out.pi_holders_bitmap, 0);
	UT_ASSERT_EQ(bootstrap_ack_count, 0);
	UT_ASSERT(memcmp(unchanged, &fake_pcm_entries, sizeof(unchanged)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(&frame.common.logical_assertion,
		&after), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
	UT_ASSERT_EQ(after.phase, RESOURCE_X_MASTER_QUEUED);
	UT_ASSERT_EQ(after.source_node, -1);
	UT_ASSERT_EQ(after.final_authority_generation, 0);
}

UT_TEST(test_service_snapshot_all_native_paths_preserve_original_episode)
{
	for (int path = RESOURCE_X_ACQUIRE_FIRST_FILL; path <= RESOURCE_X_ACQUIRE_RESIDENT; path++)
		for (int observed = PCM_STATE_N; observed <= PCM_STATE_X; observed++)
			for (int mode = PCM_STATE_S; mode <= PCM_STATE_X; mode++) {
				ResourceXDecodedFrame frame;
				ResourceXBootstrapWork work;
				ResourceXAcquireServiceSnapshot out;
				if (path == RESOURCE_X_ACQUIRE_FIRST_FILL && observed != PCM_STATE_N)
					continue;
				acquire_reset();
				frame = acquire_frame(1, path, observed, mode);
				if (!acquire_canonical_start(&frame, &work))
					continue;
				UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(&work, NULL, &out),
					RESOURCE_X_APPLY_APPLIED);
				UT_ASSERT(memcmp(&out.work, &work, sizeof(work)) == 0);
				UT_ASSERT_EQ(out.work.request.target_mode, mode);
				UT_ASSERT_EQ(out.work.acquire_intent.path, path);
			}
}

UT_TEST(test_service_snapshot_complete_shared_set_obeys_requested_mode)
{
	for (int mode = PCM_STATE_S; mode <= PCM_STATE_X; mode++) {
		ResourceXDecodedFrame frame;
		ResourceXBootstrapWork work;
		ResourceXAcquireServiceSnapshot out;
		if (!service_snapshot_start(PCM_STATE_S, mode, &frame, &work))
			continue;
		/* Exercise the actual directory transition owner, not a copied bitmap. */
		for (int node = 0; node < 32; node++)
			service_directory_transition( PCM_TRANS_N_TO_S, node);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(&work, NULL, &out),
			RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(out.directory.state, PCM_STATE_S);
		UT_ASSERT_EQ(out.current_holders_bitmap, UINT32_MAX);
		UT_ASSERT_EQ(out.required_holders_bitmap, mode == PCM_STATE_X ? UINT32_MAX & ~UINT32_C(2) : 0);
		UT_ASSERT_EQ(out.directory.transition_count, 32);
	}
}

UT_TEST(test_service_snapshot_x_to_s_includes_even_requester_downgrade)
{
	for (int holder = 0; holder < 4; holder++)
		for (int mode = PCM_STATE_S; mode <= PCM_STATE_X; mode++) {
			ResourceXDecodedFrame frame;
			ResourceXBootstrapWork work;
			ResourceXAcquireServiceSnapshot out;
			if (!service_snapshot_start(PCM_STATE_X, mode, &frame, &work))
				continue;
			service_directory_transition( PCM_TRANS_N_TO_X, holder);
			UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(&work, NULL, &out),
				RESOURCE_X_APPLY_APPLIED);
			UT_ASSERT_EQ(out.current_holders_bitmap, UINT32_C(1) << holder);
			UT_ASSERT_EQ(out.required_holders_bitmap,
				mode == PCM_STATE_X && holder == 1 ? 0 : UINT32_C(1) << holder);
			UT_ASSERT_EQ(out.directory.x_holder_node, holder);
		}
}

UT_TEST(test_service_snapshot_pi_and_watermarks_never_become_current)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work;
	ResourceXAcquireServiceSnapshot out;
	if (!service_snapshot_start(PCM_STATE_N, PCM_STATE_X, &frame, &work))
		return;
	pg_atomic_write_u32(&((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->pi_holders_bitmap, UINT32_C(0x80000005));
	((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->pi_watermark_lsn = 700;
	((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->pi_watermark_scn = 900;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(&work, NULL, &out),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(out.pi_holders_bitmap, UINT32_C(0x80000005));
	UT_ASSERT_EQ(out.pi_watermark_lsn, 700);
	UT_ASSERT_EQ(out.pi_watermark_scn, 900);
	UT_ASSERT_EQ(out.current_holders_bitmap, 0);
	UT_ASSERT_EQ(out.required_holders_bitmap, 0);
}

UT_TEST(test_service_snapshot_every_retained_identity_difference_is_stale)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work, wrong;
	if (!service_snapshot_start(PCM_STATE_N, PCM_STATE_X, &frame, &work))
		return;
	for (int field = 0; field < 21; field++) {
		wrong = work;
		switch (field) {
		case 0: wrong.binding_generation++; break;
		case 1: wrong.enqueue_order++; break;
		case 2: wrong.r4_record_generation++; break;
		case 3: wrong.authenticated_ingress_connection_generation++; break;
		case 4: wrong.master_sender_connection_generation++; break;
		case 5: wrong.request.assertion_sequence++; break;
		case 6: wrong.request.sender_connection_generation++; break;
		case 7: wrong.acquire_intent.requester_incarnation++; break;
		case 8: wrong.acquire_intent.backend_generation++; break;
		case 9: wrong.acquire_intent.base_generation++; break;
		case 10: wrong.acquire_intent.reservation_token++; break;
		case 11: wrong.acquire_intent.backend_id++; break;
		case 12: wrong.acquire_intent.buffer_id++; break;
		case 13: wrong.acquire_intent.path = RESOURCE_X_ACQUIRE_FIRST_FILL; break;
		case 14: wrong.acquire_intent.cluster_epoch++; break;
		case 15: wrong.acquire_intent.r4_generation++; break;
		case 16: wrong.request.target_mode = PCM_STATE_S; break;
		case 17: wrong.request.observed_mode = PCM_STATE_S; break;
		case 18: wrong.request.resource_formation++; break;
		case 19: wrong.request.master_session_incarnation++; break;
		case 20: wrong.reserved[0] = 1; break;
		}
		service_snapshot_refused(&wrong, NULL, field == 18
			? RESOURCE_X_APPLY_RECOVERY_BLOCKED : RESOURCE_X_APPLY_STALE);
	}
}

UT_TEST(test_service_snapshot_requires_canonical_head_and_preserves_fifo)
{
	ResourceXDecodedFrame first, second;
	ResourceXBootstrapWork work, later;
	ResourceXAcquireServiceSnapshot out;
	acquire_reset();
	first = acquire_frame(1, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_X);
	UT_ASSERT_EQ(acquire_deliver(&first), RESOURCE_X_APPLY_APPLIED);
	work = bootstrap_next_work();
	service_snapshot_refused(&work, NULL, RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&work), RESOURCE_X_APPLY_APPLIED);
	second = acquire_frame(2, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	UT_ASSERT_EQ(acquire_deliver(&second), RESOURCE_X_APPLY_APPLIED);
	/* Only the serviceable head is exported by the bounded scanner. Build
	 * this local second-item reference from its actual admitted request;
	 * the real owner must verify all fields and still refuse FIFO overtake. */
	later = work;
	later.request = second.common;
	later.acquire_intent = second.body.acquire_intent;
	later.enqueue_order++;
	UT_ASSERT_EQ(later.request.logical_assertion.requester_node, 2);
	service_snapshot_refused(&later, NULL, RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(&work, NULL, &out),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(out.work.enqueue_order, 1);
}

UT_TEST(test_service_snapshot_master_and_gate_changes_preserve_work)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work;
	ResourceXAcquireServiceSnapshot out;
	if (!service_snapshot_start(PCM_STATE_N, PCM_STATE_X, &frame, &work))
		return;
	fake_gcs_master_node = 2;
	service_snapshot_refused(&work, NULL, RESOURCE_X_APPLY_STALE);
	fake_gcs_master_node = -1;
	ut_master_session++;
	service_snapshot_refused(&work, NULL, RESOURCE_X_APPLY_STALE);
	ut_master_session--;
	pi_service_blocked = true;
	service_snapshot_refused(&work, NULL, RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	pi_service_blocked = false;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(&work, NULL, &out),
		RESOURCE_X_APPLY_APPLIED);
	{
		ResourceXGateSnapshot gate;
		UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate), RESOURCE_X_APPLY_APPLIED);
		service_snapshot_refused(&work, NULL, RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	}
}

UT_TEST(test_service_snapshot_optimistic_recheck_catches_same_bitmap_aba)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work;
	ResourceXAcquireServiceSnapshot old, now;
	if (!service_snapshot_start(PCM_STATE_N, PCM_STATE_X, &frame, &work))
		return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(&work, NULL, &old),
		RESOURCE_X_APPLY_APPLIED);
	service_directory_transition( PCM_TRANS_N_TO_S, 2);
	service_directory_transition( PCM_TRANS_S_TO_N_RELEASE, 2);
	service_snapshot_refused(&work, &old, RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(&work, NULL, &now),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(now.current_holders_bitmap, old.current_holders_bitmap);
	UT_ASSERT_EQ(now.directory.transition_count, old.directory.transition_count + 2);
}

UT_TEST(test_service_snapshot_recheck_binds_pi_and_queue_while_aliasing_output)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work;
	ResourceXAcquireServiceSnapshot old, unchanged;
	if (!service_snapshot_start(PCM_STATE_N, PCM_STATE_X, &frame, &work))
		return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(&work, NULL, &old),
		RESOURCE_X_APPLY_APPLIED);
	unchanged = old;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(&work, &old, &old),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&old, &unchanged, sizeof(old)) == 0);
	((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->pi_watermark_lsn++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(&work, &old, &old),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&old, &unchanged, sizeof(old)) == 0);
}

UT_TEST(test_service_snapshot_cancel_and_successor_never_reuse_old_input)
{
	ResourceXDecodedFrame frame, successor;
	ResourceXBootstrapWork work, next;
	ResourceXAcquireServiceSnapshot old, now;
	if (!service_snapshot_start(PCM_STATE_N, PCM_STATE_X, &frame, &work))
		return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(&work, NULL, &old),
		RESOURCE_X_APPLY_APPLIED);
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&work);
	service_snapshot_refused(&work, &old, RESOURCE_X_APPLY_STALE);
	successor = frame;
	successor.common.assertion_sequence++;
	successor.body.acquire_intent.reservation_token++;
	if (!acquire_canonical_start(&successor, &next))
		return;
	service_snapshot_refused(&work, &old, RESOURCE_X_APPLY_STALE);
	service_snapshot_refused(&next, &old, RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(&next, NULL, &now),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(now.work.enqueue_order, old.work.enqueue_order + 1);
	UT_ASSERT_EQ(now.work.request.assertion_sequence, old.work.request.assertion_sequence + 1);
}

UT_TEST(test_service_snapshot_bad_directory_shape_never_produces_output)
{
	for (int mutation = 0; mutation < 7; mutation++) {
		ResourceXDecodedFrame frame;
		ResourceXBootstrapWork work;
		if (!service_snapshot_start(PCM_STATE_N, PCM_STATE_X, &frame, &work))
			continue;
		if (mutation == 0) ((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->x_holder_node = 32;
		if (mutation == 1) pg_atomic_write_u32(&((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->s_holders_bitmap, 2);
		if (mutation == 2) pg_atomic_write_u32(&((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->master_state, PCM_STATE_S);
		if (mutation == 3) pg_atomic_write_u32(&((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->master_state, PCM_STATE_X);
		if (mutation == 4) pg_atomic_write_u32(&((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->master_state, 3);
		if (mutation == 5) ((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->master_holder.node_id = 1;
		if (mutation == 6) pg_atomic_write_u64(&((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->transition_count_local, UINT64_MAX);
		service_snapshot_refused(&work, NULL, RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	}
}

UT_TEST(test_service_snapshot_pending_convert_and_reserved_capacity_stay_owned)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work;
	ResourceXAcquireServiceSnapshot out;
	if (!service_snapshot_start(PCM_STATE_N, PCM_STATE_X, &frame, &work))
		return;
	((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->pending_x_requester_node = 2;
	((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->pending_x_since_lsn = 99;
	service_snapshot_refused(&work, NULL, RESOURCE_X_APPLY_BAD_STATE);
	((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->pending_x_requester_node = -1;
	((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->pending_x_since_lsn = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(&work, NULL, &out),
		RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_service_snapshot_invalid_arguments_never_allocate_or_write)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work;
	ResourceXAcquireServiceSnapshot out;
	if (!service_snapshot_start(PCM_STATE_N, PCM_STATE_X, &frame, &work))
		return;
	service_snapshot_refused(NULL, NULL, RESOURCE_X_APPLY_INVALID);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(&work, NULL, NULL),
		RESOURCE_X_APPLY_INVALID);
	work.has_acquire_intent = false;
	service_snapshot_refused(&work, NULL, RESOURCE_X_APPLY_INVALID);
	work.has_acquire_intent = true;
	work.request.logical_assertion.requester_node = 32;
	service_snapshot_refused(&work, NULL, RESOURCE_X_APPLY_INVALID);
	(void)out;
}

UT_TEST(test_service_snapshot_every_expected_byte_is_part_of_exact_recheck)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work;
	ResourceXAcquireServiceSnapshot exact, wrong;
	if (!service_snapshot_start(PCM_STATE_N, PCM_STATE_X, &frame, &work))
		return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(&work, NULL, &exact),
		RESOURCE_X_APPLY_APPLIED);
	for (Size pos = 0; pos < sizeof(exact); pos++) {
		wrong = exact;
		((uint8 *)&wrong)[pos] ^= 1;
		service_snapshot_refused(&work, &wrong, RESOURCE_X_APPLY_STALE);
	}
}

static void
service_master_turnover_at_entry_lock(void)
{
	ut_master_session++;
}

UT_TEST(test_service_snapshot_rechecks_master_after_entry_acquisition)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work;
	ResourceXAcquireServiceSnapshot out;
	if (!service_snapshot_start(PCM_STATE_N, PCM_STATE_X, &frame, &work))
		return;
	object_entry_lock_hook = service_master_turnover_at_entry_lock;
	service_snapshot_refused(&work, NULL, RESOURCE_X_APPLY_STALE);
	UT_ASSERT(object_entry_lock_hook == NULL);
	ut_master_session--;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(&work, NULL, &out),
		RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_service_snapshot_existing_stop_seal_does_not_create_or_strand_owner)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work;
	ResourceXAcquireServiceSnapshot out;
	if (!service_snapshot_start(PCM_STATE_N, PCM_STATE_S, &frame, &work))
		return;
	stop_new_work_allowed = false;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_service_snapshot_exact(&work, NULL, &out),
		RESOURCE_X_APPLY_APPLIED);
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&work);
	service_snapshot_refused(&work, &out, RESOURCE_X_APPLY_STALE);
	UT_ASSERT(!cluster_pcm_lock_resource_x_s_barrier_active(&work.request.logical_assertion.resource));
}

UT_TEST(test_service_snapshot_unsealed_proof_is_required_for_entire_canonical)
{
	ResourceXDecodedFrame frame;
	ResourceXBootstrapWork work;
	ClusterPcmResourceXMasterRequest exact = {0}, wrong;
	char *record = NULL;
	unsigned matches = 0;
	if (!service_snapshot_start(PCM_STATE_N, PCM_STATE_X, &frame, &work))
		return;
	exact.base_authority_generation = 1;
	exact.resource_formation = frame.common.resource_formation;
	exact.master_session_incarnation = frame.common.master_session_incarnation;
	exact.assertion_sequence = frame.common.assertion_sequence;
	exact.enqueue_order = work.enqueue_order;
	exact.sender_connection_generation = frame.common.sender_connection_generation;
	exact.source_node = -1;
	exact.phase = RESOURCE_X_MASTER_QUEUED;
	for (Size off = 0; off + sizeof(exact) <= fake_pcm_header_requested_size; off++)
		if (memcmp(fake_pcm_header.data + off, &exact, sizeof(exact)) == 0) {
			record = fake_pcm_header.data + off;
			matches++;
		}
	UT_ASSERT_EQ(matches, 1);
	if (matches != 1)
		return;
	for (int mutation = 0; mutation < 4; mutation++) {
		wrong = exact;
		if (mutation == 0) wrong.delegated_authority_generation = 2;
		if (mutation == 1) wrong.phase = RESOURCE_X_MASTER_WAIT_PROOF;
		if (mutation == 2) wrong.blocked_holders_bitmap = 4;
		if (mutation == 3) wrong.dependencies[0] = 7;
		memcpy(record, &wrong, sizeof(wrong));
		service_snapshot_refused(&work, NULL, RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		UT_ASSERT(memcmp(record, &wrong, sizeof(wrong)) == 0);
		memcpy(record, &exact, sizeof(exact));
	}
}

int
main(void)
{
	UT_PLAN(48);
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
	UT_RUN(test_acquire_canonical_all_modes_retain_one_unsealed_identity);
	UT_RUN(test_acquire_canonical_duplicate_does_not_allocate_another_order);
	UT_RUN(test_acquire_canonical_head_cannot_be_overtaken_by_later_work);
	UT_RUN(test_acquire_canonical_legacy_replay_cannot_start_x_service);
	UT_RUN(test_acquire_canonical_cancel_requires_full_episode_and_binding);
	UT_RUN(test_acquire_canonical_cancel_preserves_floor_and_late_work_cannot_touch_successor);
	UT_RUN(test_acquire_canonical_unknown_observation_keeps_pair_but_boot_change_retires_it);
	UT_RUN(test_acquire_canonical_domain_change_retires_only_unsealed_exact_pair);
	UT_RUN(test_acquire_canonical_stop_seal_allows_existing_handoff_and_cancel);
	UT_RUN(test_acquire_canonical_reconfiguration_retires_pair_and_retains_floor);
	UT_RUN(test_acquire_canonical_node_only_death_preserves_pair_typed_death_retires_it);
	UT_RUN(test_acquire_canonical_uncertain_responsibility_cannot_be_forgotten);
	UT_RUN(test_acquire_canonical_waits_for_original_pending_s_without_reserving_a_source);
	UT_RUN(test_acquire_canonical_completed_send_payload_is_not_live_capacity);
	UT_RUN(test_service_snapshot_exact_n_head_is_input_not_authority);
	UT_RUN(test_service_snapshot_all_native_paths_preserve_original_episode);
	UT_RUN(test_service_snapshot_complete_shared_set_obeys_requested_mode);
	UT_RUN(test_service_snapshot_x_to_s_includes_even_requester_downgrade);
	UT_RUN(test_service_snapshot_pi_and_watermarks_never_become_current);
	UT_RUN(test_service_snapshot_every_retained_identity_difference_is_stale);
	UT_RUN(test_service_snapshot_requires_canonical_head_and_preserves_fifo);
	UT_RUN(test_service_snapshot_master_and_gate_changes_preserve_work);
	UT_RUN(test_service_snapshot_optimistic_recheck_catches_same_bitmap_aba);
	UT_RUN(test_service_snapshot_recheck_binds_pi_and_queue_while_aliasing_output);
	UT_RUN(test_service_snapshot_cancel_and_successor_never_reuse_old_input);
	UT_RUN(test_service_snapshot_bad_directory_shape_never_produces_output);
	UT_RUN(test_service_snapshot_pending_convert_and_reserved_capacity_stay_owned);
	UT_RUN(test_service_snapshot_invalid_arguments_never_allocate_or_write);
	UT_RUN(test_service_snapshot_every_expected_byte_is_part_of_exact_recheck);
	UT_RUN(test_service_snapshot_rechecks_master_after_entry_acquisition);
	UT_RUN(test_service_snapshot_existing_stop_seal_does_not_create_or_strand_owner);
	UT_RUN(test_service_snapshot_unsealed_proof_is_required_for_entire_canonical);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
