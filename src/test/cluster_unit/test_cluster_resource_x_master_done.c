/* Actual master publication and completion owners. Native C is explicit
	* authenticated peer input here; its real producer is tested separately. */
#include "test_cluster_resource_x_master_ack_fixture.inc"
#ifndef DISK_MASTER_DONE_RED
#include "test_cluster_resource_x_master_done_ingress.inc"
#endif

static bool
master_done_round_trip(ResourceXDecodedFrame *frame)
{
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES];
	uint16 len = 0;
	ResourceXWireReject reject;
	bool ok = cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		frame, bytes, sizeof(bytes), &len, &reject);
	UT_ASSERT(ok); if (!ok) return false;
	ok = cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		bytes, len, frame, &reject);
	UT_ASSERT(ok); return ok;
}

static ResourceXDecodedFrame
master_done_frame(PublishFixture *f)
{
	ResourceXDecodedFrame grant = publish_decode(f->state->grant_intent.payload, false);
	ResourceXDecodedFrame done = {0};
	done.kind = RESOURCE_X_WIRE_INSTALL_SETTLEMENT;
	done.payload_bytes = RESOURCE_X_COMPLETE_V5_BYTES;
	done.has_acquire_decision = true;
	done.common = grant.common;
	done.common.action_node = done.common.logical_assertion.requester_node;
	done.common.outcome = RESOURCE_X_OUTCOME_OK;
	done.common.sender_connection_generation = 71;
	done.body.acquire_completion.decision = grant.body.acquire_decision;
	done.body.acquire_completion.native_generation = grant.body.acquire_decision.episode.base_generation + 1;
	done.body.acquire_completion.page_lsn = 201;
	done.body.acquire_completion.page_scn = scn_local(grant.body.acquire_decision.disk_watermark_scn) + 3;
	done.body.acquire_completion.page_crc32c = 99;
	done.body.acquire_completion.decision_crc32c = grant.common.semantic_crc32c;
	(void)master_done_round_trip(&done);
	return done;
}

static bool
master_done_start(PublishFixture *f, ResourceXDecodedFrame *done)
{
	if (!publish_start(0, PCM_STATE_S, f)) return false;
	publish_apply(f); *done = master_done_frame(f); return true;
}

static ResourceXIntentSlot
master_done_stage(PublishFixture *f)
{
	ResourceXIntentSlot slot = {0};
	fake_pcm_clock_us += 1000000;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
		&f->state->grant_intent.slot, ++fake_pcm_clock_us, &slot), RESOURCE_X_INTENT_STAGED);
	return slot;
}

static void
master_done_send(PublishFixture *f)
{
	ResourceXIntentSlot slot = master_done_stage(f);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(
		&slot, true, ++fake_pcm_clock_us));
}

static ResourceXApplyResult
master_done_direct(const ResourceXDecodedFrame *done, int auth, uint32 ingress,
	uint32 outbound, uint64 r4)
{
#ifdef DISK_MASTER_DONE_RED
	ResourceXMasterSnapshot out;
	(void)ingress; (void)outbound; (void)r4;
	return cluster_pcm_lock_resource_x_install_settlement_exact(done, auth, &out);
#else
	return cluster_pcm_lock_resource_x_acquire_done_exact(done, auth, ingress, outbound, r4);
#endif
}

static ResourceXApplyResult
master_done_receive(const ResourceXDecodedFrame *done)
{
#ifdef DISK_MASTER_DONE_RED
	return master_done_direct(done, done->common.action_node, 61, 61, 77);
#else
	ClusterICEnvelope env = {0};
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE;
	env.source_node_id = done->common.logical_assertion.requester_node;
	env.dest_node_id = cluster_node_id;
	env.epoch = 17; env.payload_length = RESOURCE_X_COMPLETE_V5_BYTES;
	return gcs_block_resource_x_acquire_done_ingress(&env, done, 61);
#endif
}

static void
master_done_quiet(PublishFixture *f)
{
	static const ClusterPcmResourceXMasterRequest empty_request;
	static const ClusterPcmResourceXDeferredSettlement empty_deferred;
	struct StopPcmEntryLayout *directory = (struct StopPcmEntryLayout *)fake_pcm_entries.data[0];
	int node = f->request.common.logical_assertion.requester_node;
	UT_ASSERT_EQ(f->state->requests[node].phase, RESOURCE_X_MASTER_NONE);
	UT_ASSERT(memcmp(&f->state->requests[node], &empty_request, sizeof(empty_request)) == 0);
	UT_ASSERT_EQ(f->state->bootstrap_priority[node].state, RESOURCE_X_BOOTSTRAP_PRIORITY_EMPTY);
	UT_ASSERT_EQ(f->state->bootstrap_receipts[node].highest_attempt_floor,
		f->request.common.assertion_sequence);
	UT_ASSERT(memcmp(&f->state->deferred_settlement, &empty_deferred, sizeof(empty_deferred)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&directory->master_state), PCM_STATE_S);
	UT_ASSERT_EQ(pg_atomic_read_u32(&directory->s_holders_bitmap), UINT32_C(1) << node);
	UT_ASSERT_EQ(pg_atomic_read_u32(&directory->pi_holders_bitmap), 0);
	UT_ASSERT_EQ(f->state->holder_grants[node].boot_incarnation,
		f->request.body.acquire_intent.requester_incarnation);
	UT_ASSERT_EQ(f->state->holder_grants[node].grant_generation, f->plan.proposed_authority_generation);
	UT_ASSERT_EQ(f->state->holder_grants[node].master_node, cluster_node_id);
	UT_ASSERT_EQ(f->state->authority_generation, f->plan.proposed_authority_generation);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT_EQ(master_durable_admission_depth, 0);
}

static void
master_done_refuse(PublishFixture *f, ResourceXDecodedFrame *done)
{
	ClusterPcmResourceXMasterState before = *f->state;
	UT_ASSERT(master_done_receive(done) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, f->state, sizeof(before)) == 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT_EQ(master_durable_admission_depth, 0);
}

UT_TEST(test_master_done_consumes_exact_c_after_real_grant_callback)
{
	PublishFixture f; ResourceXDecodedFrame done;
	if (!master_done_start(&f, &done)) return;
	master_done_send(&f);
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
	master_done_quiet(&f);
}
UT_TEST(test_master_done_duplicate_keeps_generation_and_current_provenance)
{
	PublishFixture f; ResourceXDecodedFrame done; ClusterPcmResourceXMasterState before;
	if (!master_done_start(&f, &done)) return;
	master_done_send(&f);
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
	before = *f.state;
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}
UT_TEST(test_master_done_before_callback_retained_and_drained_by_original_tick)
{
	PublishFixture f; ResourceXDecodedFrame done; ResourceXIntentSlot slot;
	if (!master_done_start(&f, &done)) return;
	slot = master_done_stage(&f);
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 2);
	UT_ASSERT_EQ(f.state->grant_intent.slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
	UT_ASSERT_EQ(f.state->requests[1].phase, RESOURCE_X_MASTER_WAIT_PROOF);
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&f.work), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&f.work), RESOURCE_X_APPLY_APPLIED);
	master_done_quiet(&f);
}
UT_TEST(test_master_done_failed_physical_offer_keeps_same_received_completion)
{
	PublishFixture f; ResourceXDecodedFrame done; ResourceXIntentSlot slot;
	if (!master_done_start(&f, &done)) return;
	slot = master_done_stage(&f);
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, false, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&f.work), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 2);
	master_done_send(&f);
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&f.work), RESOURCE_X_APPLY_APPLIED);
	master_done_quiet(&f);
}
UT_TEST(test_master_done_pending_duplicate_cannot_replace_i)
{
	PublishFixture f; ResourceXDecodedFrame done; ClusterPcmResourceXMasterState before;
	if (!master_done_start(&f, &done)) return;
	(void)master_done_stage(&f);
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
	before = *f.state;
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	done.body.acquire_completion.page_crc32c++;
	if (!master_done_round_trip(&done)) return;
	master_done_refuse(&f, &done);
}
UT_TEST(test_master_done_wrong_complete_a_rejected_without_mutation)
{
	for (int n = 0; n < 9; n++) {
		PublishFixture f; ResourceXDecodedFrame done;
		ResourceXDecodedAcquireDecision *a;
		if (!master_done_start(&f, &done)) continue;
		master_done_send(&f);
		a = &done.body.acquire_completion.decision;
		if (n == 0) a->resource_binding++;
		if (n == 1) a->enqueue_order++;
		if (n == 2) a->disk_watermark_scn++;
		if (n == 3) a->episode.reservation_token++;
		if (n == 4) { a->episode.base_generation++; done.body.acquire_completion.native_generation++; }
		if (n == 5) a->episode.backend_generation++;
		if (n == 6) a->requester_ingress_generation++;
		if (n == 7) a->master_ingress_generation++;
		if (n == 8) done.body.acquire_completion.decision_crc32c++;
		if (!master_done_round_trip(&done)) continue;
		master_done_refuse(&f, &done);
	}
}
UT_TEST(test_master_done_authentication_directions_and_r4_are_independent)
{
	for (int n = 0; n < 5; n++) {
		PublishFixture f; ResourceXDecodedFrame done; ClusterPcmResourceXMasterState before;
		if (!master_done_start(&f, &done)) continue;
		master_done_send(&f); before = *f.state;
		UT_ASSERT(master_done_direct(&done, n == 0 ? 2 : 1,
		 n == 1 ? 62 : 61, n == 2 ? 62 : 61, n == 3 ? 78 : (n == 4 ? 0 : 77))
		 != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	}
}
UT_TEST(test_master_done_current_peer_boot_session_and_admission_rechecked)
{
	for (int n = 0; n < 4; n++) {
		PublishFixture f; ResourceXDecodedFrame done;
		if (!master_done_start(&f, &done)) continue;
		master_done_send(&f);
		if (n == 0) auth_peer_boot++;
		if (n == 1) ut_master_session++;
		if (n == 2) master_durable_admission = false;
		if (n == 3) ready_connection_generation++;
		master_done_refuse(&f, &done);
	}
}
UT_TEST(test_master_done_deferred_resume_rechecks_domain_without_dropping_receipt)
{
	PublishFixture f; ResourceXDecodedFrame done; ClusterPcmResourceXMasterState before;
	if (!master_done_start(&f, &done)) return;
	(void)master_done_stage(&f);
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
	before = *f.state; auth_peer_boot++;
	UT_ASSERT(cluster_gcs_block_resource_x_bootstrap_tick(&f.work) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}
UT_TEST(test_master_done_without_semantic_receipt_callback_leaves_a_owned)
{
	PublishFixture f; ResourceXDecodedFrame done;
	if (!master_done_start(&f, &done)) return;
	master_done_send(&f);
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&f.work), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(f.state->requests[1].phase, RESOURCE_X_MASTER_WAIT_PROOF);
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 0);
}
UT_TEST(test_master_done_fifo_survives_exact_head_completion)
{
	PublishFixture f; ResourceXDecodedFrame done, next;
	ResourceXBootstrapWork later; ResourceXDecodedFrame ack;
	if (!master_done_start(&f, &done)) return;
	next = acquire_frame(2, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	UT_ASSERT_EQ(acquire_deliver(&next), RESOURCE_X_APPLY_APPLIED);
	master_done_send(&f);
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
	later = bootstrap_next_work();
	UT_ASSERT_EQ(later.request.logical_assertion.requester_node, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_resume_exact(&later, &ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.state->bootstrap_receipts[1].highest_attempt_floor, f.request.common.assertion_sequence);
	UT_ASSERT_EQ(f.state->holder_grants[1].boot_incarnation, 101);
}
UT_TEST(test_master_done_late_duplicate_cannot_clear_same_node_successor)
{
	PublishFixture f; ResourceXDecodedFrame done, next; ResourceXBootstrapWork later;
	ClusterPcmResourceXMasterState before;
	if (!master_done_start(&f, &done)) return;
	master_done_send(&f);
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
	next = f.request; next.common.assertion_sequence++;
	next.body.acquire_intent.reservation_token++;
	UT_ASSERT_EQ(acquire_deliver(&next), RESOURCE_X_APPLY_APPLIED);
	later = bootstrap_next_work(); before = *f.state;
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(later.request.assertion_sequence, next.common.assertion_sequence);
}
UT_TEST(test_master_done_latest_replay_requires_same_i)
{
	PublishFixture f; ResourceXDecodedFrame done;
	if (!master_done_start(&f, &done)) return;
	master_done_send(&f);
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
	done.body.acquire_completion.page_lsn++;
	if (!master_done_round_trip(&done)) return;
	master_done_refuse(&f, &done);
}
UT_TEST(test_master_done_legacy_settlement_cannot_consume_typed_s)
{
	PublishFixture f; ResourceXDecodedFrame done; ResourceXMasterSnapshot out;
	ClusterPcmResourceXMasterState before;
	if (!master_done_start(&f, &done)) return;
	master_done_send(&f); before = *f.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(&done, 1, &out), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}
UT_TEST(test_master_done_input_semantic_crc_must_match_decoded_frame)
{
	PublishFixture f; ResourceXDecodedFrame done;
	if (!master_done_start(&f, &done)) return;
	master_done_send(&f); done.common.semantic_crc32c++;
	master_done_refuse(&f, &done);
}
UT_TEST(test_master_done_transport_rebind_does_not_replace_semantic_a)
{
	PublishFixture f; ResourceXDecodedFrame done;
	if (!master_done_start(&f, &done)) return;
	master_done_send(&f); done.common.sender_connection_generation = 111;
	if (!master_done_round_trip(&done)) return;
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
	master_done_quiet(&f);
}

UT_TEST(test_master_done_empty_marker_cannot_erase_nonzero_receipt)
{
	PublishFixture f; ResourceXDecodedFrame done;
	if (!master_done_start(&f, &done)) return;
	master_done_send(&f);
	f.state->deferred_settlement.payload[RESOURCE_X_COMPLETE_V5_BYTES - 1] = 1;
	master_done_refuse(&f, &done);
}
UT_TEST(test_master_done_occupied_original_receipt_is_never_evicted)
{
	for (int n = 1; n <= 3; n++) {
		PublishFixture f; ResourceXDecodedFrame done;
		if (!master_done_start(&f, &done)) continue;
		master_done_send(&f);
		f.state->deferred_settlement.present = n;
		f.state->deferred_settlement.authenticated_sender = 2;
		master_done_refuse(&f, &done);
	}
}
UT_TEST(test_master_done_canonical_shape_and_directory_are_not_inferred_from_c)
{
	for (int n = 0; n < 3; n++) {
		PublishFixture f; ResourceXDecodedFrame done;
		if (!master_done_start(&f, &done)) continue;
		master_done_send(&f);
		if (n == 0) f.state->requests[1].page_checksum = 1;
		if (n == 1) service_directory_transition(PCM_TRANS_N_TO_S, 3);
		if (n == 2) {
			service_directory_transition(PCM_TRANS_N_TO_S, 3);
			service_directory_transition(PCM_TRANS_S_TO_N_RELEASE, 3);
		}
		master_done_refuse(&f, &done);
	}
}
UT_TEST(test_master_done_colocated_master_requester_preserves_original_boot)
{
	PublishFixture f; ResourceXDecodedFrame done;
	if (!publish_start(0, PCM_STATE_S, &f)) return;
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&f.work);
	f.request = acquire_frame(0, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	UT_ASSERT(acquire_canonical_start(&f.request, &f.work));
	f.plan = decision_plan_read(&f.work);
	publish_apply(&f); done = master_done_frame(&f);
	master_done_send(&f);
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
	master_done_quiet(&f);
	UT_ASSERT_EQ(f.state->holder_grants[0].boot_incarnation, ut_master_session);
}
UT_TEST(test_master_done_later_a_overwrites_only_nonauthority_replay_history)
{
	PublishFixture f; ResourceXDecodedFrame done, next, ack;
	ResourceXBootstrapWork work; ClusterPcmResourceXMasterState before;
	if (!master_done_start(&f, &done)) return;
	master_done_send(&f);
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
	next = acquire_frame(2, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	UT_ASSERT_EQ(acquire_deliver(&next), RESOURCE_X_APPLY_APPLIED);
	work = bootstrap_next_work();
	UT_ASSERT_EQ(work.request.logical_assertion.requester_node, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_resume_exact(&work, &ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&work, NULL), RESOURCE_X_APPLY_APPLIED);
	before = *f.state;
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}
UT_TEST(test_master_done_latest_history_compares_every_install_scalar)
{
	for (int n = 0; n < 3; n++) {
		PublishFixture f; ResourceXDecodedFrame done;
		if (!master_done_start(&f, &done)) continue;
		master_done_send(&f);
		UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
		if (n == 0) done.body.acquire_completion.page_crc32c++;
		if (n == 1) done.body.acquire_completion.write_io_generation += 2;
		if (n == 2) done.body.acquire_completion.page_scn++;
		if (!master_done_round_trip(&done)) continue;
		master_done_refuse(&f, &done);
	}
}

UT_TEST(test_master_done_received_c_prevents_never_sent_forget)
{
	PublishFixture f; ResourceXDecodedFrame done; ClusterPcmResourceXMasterState before;
	if (!master_done_start(&f, &done)) return;
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
	before = *f.state;
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&f.work);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}
UT_TEST(test_master_done_unsealed_publish_cannot_erase_unproven_receipt)
{
	PublishFixture f; ClusterPcmResourceXMasterState before;
	if (!publish_start(0, PCM_STATE_S, &f)) return;
	f.state->deferred_settlement.payload[0] = 1;
	before = *f.state;
	UT_ASSERT(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&f.work, NULL)
		!= RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}

UT_TEST(test_master_done_decoded_discriminators_cannot_alias_another_body)
{
	for (int n = 0; n < 3; n++) {
		PublishFixture f; ResourceXDecodedFrame done;
		if (!master_done_start(&f, &done)) continue;
		master_done_send(&f);
		if (n == 0) done.has_acquire_intent = true;
		if (n == 1) done.blocked_has_remote_proof = true;
		if (n == 2) done.payload_bytes--;
		master_done_refuse(&f, &done);
	}
}

UT_TEST(test_master_done_history_requires_monotonic_floor_and_authority)
{
	for (int n = 0; n < 2; n++) {
		PublishFixture f; ResourceXDecodedFrame done;
		if (!master_done_start(&f, &done)) continue;
		master_done_send(&f);
		UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
		if (n == 0) f.state->bootstrap_receipts[1].highest_attempt_floor = 0;
		if (n == 1) f.state->authority_generation--;
		{
			ClusterPcmResourceXMasterState before = *f.state;
			UT_ASSERT(master_done_receive(&done) != RESOURCE_X_APPLY_DUPLICATE);
			UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
		}
	}
}

int
main(void)
{
	UT_PLAN(26);
	UT_RUN(test_master_done_consumes_exact_c_after_real_grant_callback);
	UT_RUN(test_master_done_duplicate_keeps_generation_and_current_provenance);
	UT_RUN(test_master_done_before_callback_retained_and_drained_by_original_tick);
	UT_RUN(test_master_done_failed_physical_offer_keeps_same_received_completion);
	UT_RUN(test_master_done_pending_duplicate_cannot_replace_i);
	UT_RUN(test_master_done_wrong_complete_a_rejected_without_mutation);
	UT_RUN(test_master_done_authentication_directions_and_r4_are_independent);
	UT_RUN(test_master_done_current_peer_boot_session_and_admission_rechecked);
	UT_RUN(test_master_done_deferred_resume_rechecks_domain_without_dropping_receipt);
	UT_RUN(test_master_done_without_semantic_receipt_callback_leaves_a_owned);
	UT_RUN(test_master_done_fifo_survives_exact_head_completion);
	UT_RUN(test_master_done_late_duplicate_cannot_clear_same_node_successor);
	UT_RUN(test_master_done_latest_replay_requires_same_i);
	UT_RUN(test_master_done_legacy_settlement_cannot_consume_typed_s);
	UT_RUN(test_master_done_input_semantic_crc_must_match_decoded_frame);
	UT_RUN(test_master_done_transport_rebind_does_not_replace_semantic_a);
	UT_RUN(test_master_done_empty_marker_cannot_erase_nonzero_receipt);
	UT_RUN(test_master_done_occupied_original_receipt_is_never_evicted);
	UT_RUN(test_master_done_canonical_shape_and_directory_are_not_inferred_from_c);
	UT_RUN(test_master_done_colocated_master_requester_preserves_original_boot);
	UT_RUN(test_master_done_later_a_overwrites_only_nonauthority_replay_history);
	UT_RUN(test_master_done_latest_history_compares_every_install_scalar);
	UT_RUN(test_master_done_received_c_prevents_never_sent_forget);
	UT_RUN(test_master_done_unsealed_publish_cannot_erase_unproven_receipt);
	UT_RUN(test_master_done_decoded_discriminators_cannot_alias_another_body);
	UT_RUN(test_master_done_history_requires_monotonic_floor_and_authority);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
