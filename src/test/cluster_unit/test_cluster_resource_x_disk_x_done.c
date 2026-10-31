/* Real codec, master, ingress, LMS queue and transport callback owners.
 * Native X completion is explicit authenticated peer input. These tests do
 * not exercise physical X installation, T1--T3 or writer activation. */
#include "test_cluster_resource_x_master_consumed_fixture.inc"

static bool
disk_x_start(bool resident, uint8 outcome, PublishFixture *f, ResourceXDecodedFrame *done)
{
	ResourceXDecodedFrame grant;
	if (!publish_start_path(0, PCM_STATE_X, resident ? RESOURCE_X_ACQUIRE_RESIDENT
		: RESOURCE_X_ACQUIRE_FIRST_FILL, f)) return false;
	publish_apply(f);
	grant = publish_decode(f->state->grant_intent.payload, false);
	memset(done, 0, sizeof(*done));
	done->kind = RESOURCE_X_WIRE_INSTALL_SETTLEMENT;
	done->payload_bytes = RESOURCE_X_COMPLETE_V5_BYTES;
	done->has_acquire_decision = true;
	done->common = grant.common;
	done->common.action_node = grant.common.logical_assertion.requester_node;
	done->common.outcome = outcome;
	done->common.sender_connection_generation = 71;
	done->body.acquire_completion.decision = grant.body.acquire_decision;
	done->body.acquire_completion.native_generation = grant.body.acquire_decision.episode.base_generation
		+ (outcome == RESOURCE_X_OUTCOME_OK ? 1 : 0);
	if (outcome != RESOURCE_X_OUTCOME_QUIET_NO_IMAGE) {
		done->body.acquire_completion.page_lsn = 201;
		done->body.acquire_completion.page_scn = scn_local(grant.body.acquire_decision.disk_watermark_scn) + 3;
		done->body.acquire_completion.page_crc32c = 99;
	}
	done->body.acquire_completion.decision_crc32c = grant.common.semantic_crc32c;
	return master_done_round_trip(done);
}

static bool
disk_x_consume(bool resident, uint8 outcome, PublishFixture *f, ResourceXDecodedFrame *done)
{
	if (!disk_x_start(resident, outcome, f, done)) return false;
	master_done_send(f);
	UT_ASSERT_EQ(master_done_receive(done), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f->state->deferred_settlement.present, 3);
	return f->state->deferred_settlement.present == 3;
}

static void
disk_x_directory(PublishFixture *f, bool installed)
{
	static const ClusterPcmResourceXMasterRequest empty_request;
	static const ResourceXHolderGrantProvenance empty_holder;
	struct StopPcmEntryLayout *directory = (void *)fake_pcm_entries.data[0];
	int node = f->request.common.logical_assertion.requester_node;
	UT_ASSERT_EQ(pg_atomic_read_u32(&directory->master_state), installed ? PCM_STATE_X : PCM_STATE_N);
	UT_ASSERT_EQ(directory->x_holder_node, installed ? node : -1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&directory->s_holders_bitmap), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&directory->pi_holders_bitmap), 0);
	if (installed) {
		UT_ASSERT_EQ(f->state->holder_grants[node].boot_incarnation,
			f->request.body.acquire_intent.requester_incarnation);
		UT_ASSERT_EQ(f->state->holder_grants[node].grant_generation, f->plan.proposed_authority_generation);
		UT_ASSERT_EQ(f->state->holder_grants[node].master_node, cluster_node_id);
	} else
		UT_ASSERT(memcmp(&f->state->holder_grants[node], &empty_holder, sizeof(empty_holder)) == 0);
	UT_ASSERT(memcmp(&f->state->requests[node], &empty_request, sizeof(empty_request)) == 0);
	UT_ASSERT_EQ(f->state->bootstrap_priority[node].state, RESOURCE_X_BOOTSTRAP_PRIORITY_EMPTY);
	UT_ASSERT_EQ(f->state->bootstrap_receipts[node].highest_attempt_floor, f->request.common.assertion_sequence);
	UT_ASSERT_EQ(f->state->authority_generation, f->plan.proposed_authority_generation);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT_EQ(master_durable_admission_depth, 0);
}

UT_TEST(test_disk_x_normal_consumes_original_A_for_first_fill_and_resident_N)
{
	for (int resident = 0; resident < 2; resident++) {
		PublishFixture f; ResourceXDecodedFrame done, ack;
		if (!disk_x_consume(resident, RESOURCE_X_OUTCOME_OK, &f, &done)) continue;
		disk_x_directory(&f, true);
		ack = consumed_decode(&f);
		UT_ASSERT_EQ(ack.common.target_mode, PCM_STATE_X);
		UT_ASSERT_EQ(ack.common.outcome, RESOURCE_X_OUTCOME_DUPLICATE);
		UT_ASSERT_EQ(ack.common.action_node, cluster_node_id);
		UT_ASSERT_EQ(ack.body.acquire_completion.decision.episode.path,
			resident ? RESOURCE_X_ACQUIRE_RESIDENT : RESOURCE_X_ACQUIRE_FIRST_FILL);
		UT_ASSERT(memcmp(&ack.body.acquire_completion, &done.body.acquire_completion,
			sizeof(done.body.acquire_completion)) == 0);
		master_done_send(&f);
		UT_ASSERT_EQ(f.state->deferred_settlement.present, 0);
		disk_x_directory(&f, true);
	}
}

UT_TEST(test_disk_x_early_completion_waits_original_GRANT_callback)
{
	for (int resident = 0; resident < 2; resident++) {
		PublishFixture f; ResourceXDecodedFrame done; ResourceXIntentSlot grant;
		if (!disk_x_start(resident, RESOURCE_X_OUTCOME_OK, &f, &done)) continue;
		grant = master_done_stage(&f);
		UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(f.state->deferred_settlement.present, 2);
		UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&f.work), RESOURCE_X_APPLY_LOCAL_WAIT);
		UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&grant, true, ++fake_pcm_clock_us));
		UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&f.work), RESOURCE_X_APPLY_APPLIED);
		disk_x_directory(&f, true);
	}
}

UT_TEST(test_disk_x_quiet_with_and_without_image_never_publishes_X)
{
	for (int resident = 0; resident < 2; resident++)
		for (int image = 0; image < 2; image++)
		for (int early = 0; early < 2; early++) {
			PublishFixture f; ResourceXDecodedFrame quiet, ack;
			uint8 outcome = image ? RESOURCE_X_OUTCOME_QUIET : RESOURCE_X_OUTCOME_QUIET_NO_IMAGE;
			if (early) {
				ResourceXIntentSlot grant;
				if (!disk_x_start(resident, outcome, &f, &quiet)) continue;
				grant = master_done_stage(&f);
				UT_ASSERT_EQ(master_done_receive(&quiet), RESOURCE_X_APPLY_APPLIED);
				UT_ASSERT_EQ(f.state->deferred_settlement.present, 2);
				UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&f.work), RESOURCE_X_APPLY_LOCAL_WAIT);
				UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&grant, true, ++fake_pcm_clock_us));
				UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&f.work), RESOURCE_X_APPLY_APPLIED);
			} else if (!disk_x_consume(resident, outcome, &f, &quiet)) continue;
			disk_x_directory(&f, false);
			ack = consumed_decode(&f);
			UT_ASSERT_EQ(ack.common.outcome, image ? RESOURCE_X_OUTCOME_QUIET_RETIRED
				: RESOURCE_X_OUTCOME_QUIET_NO_IMAGE_RETIRED);
			UT_ASSERT(memcmp(&ack.body.acquire_completion, &quiet.body.acquire_completion,
				sizeof(quiet.body.acquire_completion)) == 0);
			master_done_send(&f);
			UT_ASSERT_EQ(f.state->deferred_settlement.present, 0);
			disk_x_directory(&f, false);
		}
}

UT_TEST(test_disk_x_duplicate_preserves_one_exact_receipt_and_grant)
{
	PublishFixture f; ResourceXDecodedFrame done; ClusterPcmResourceXMasterState before;
	if (!disk_x_consume(false, RESOURCE_X_OUTCOME_OK, &f, &done)) return;
	before = *f.state;
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}

UT_TEST(test_disk_x_auth_domain_and_full_completion_identity_remain_exact)
{
	for (int n = 0; n < 14; n++) {
		PublishFixture f; ResourceXDecodedFrame done;
		if (!disk_x_start(false, RESOURCE_X_OUTCOME_OK, &f, &done)) continue;
		master_done_send(&f);
		if (n == 0) done.body.acquire_completion.decision.episode.backend_generation++;
		if (n == 1) done.body.acquire_completion.decision.episode.reservation_token++;
		if (n == 2) done.body.acquire_completion.decision.episode.base_generation++;
		if (n == 3) done.body.acquire_completion.decision.resource_binding++;
		if (n == 4) done.body.acquire_completion.decision.master_ingress_generation++;
		if (n == 5) done.body.acquire_completion.decision.requester_ingress_generation++;
		if (n == 6) done.body.acquire_completion.decision_crc32c++;
		if (n == 7) done.body.acquire_completion.native_generation++;
		if (n == 8) done.body.acquire_completion.write_io_generation |= 1;
		if (n == 9) done.common.authority_generation++;
		if (n == 10) done.common.resource_formation++;
		if (n == 11) done.common.master_session_incarnation++;
		if (n == 12) done.common.target_mode = PCM_STATE_S;
		if (n == 13) done.common.logical_assertion.resource.blockNum++;
		master_done_refuse(&f, &done);
	}
	{
		PublishFixture f; ResourceXDecodedFrame done; ClusterPcmResourceXMasterState before;
		if (!disk_x_start(true, RESOURCE_X_OUTCOME_OK, &f, &done)) return;
		master_done_send(&f); before = *f.state;
		UT_ASSERT(master_done_direct(&done, 2, 61, 61, 77) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(master_done_direct(&done, 1, 62, 61, 77) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(master_done_direct(&done, 1, 61, 62, 77) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(master_done_direct(&done, 1, 61, 61, 78) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	}
}

UT_TEST(test_disk_x_actual_LMS_shape_payload_takeover_and_callback_retire_once)
{
	for (int n = 0; n < 3; n++) {
		PublishFixture f; ResourceXDecodedFrame done; ResourceXIntentSlot offered, staged;
		uint8 outcome = n == 0 ? RESOURCE_X_OUTCOME_OK : n == 1 ? RESOURCE_X_OUTCOME_QUIET
			: RESOURCE_X_OUTCOME_QUIET_NO_IMAGE;
		if (!disk_x_consume(true, outcome, &f, &done)) continue;
		consumed_ring_reset(); offered = f.state->grant_intent.slot;
		UT_ASSERT(lms_outbound_resource_x_intent_valid(&offered));
		UT_ASSERT(lms_outbound_resource_x_intent_payload_valid(&offered, f.state->grant_intent.payload));
		UT_ASSERT_EQ(consumed_actual_lms_enqueue(0, &offered, 61, 9999999), CLUSTER_LMS_ENQUEUE_ADMITTED);
		UT_ASSERT_EQ(consumed_ring_shared.rings[0].count, 1);
		memcpy(&staged, consumed_ring_shared.rings[0].ring[0].payload, sizeof(staged));
		UT_ASSERT_EQ(staged.state, RESOURCE_X_INTENT_SLOT_STAGED);
		UT_ASSERT(staged.send_episode != 0);
		UT_ASSERT_EQ(f.state->deferred_settlement.present, 3);
		UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, ++fake_pcm_clock_us));
		UT_ASSERT_EQ(f.state->deferred_settlement.present, 0);
		UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, ++fake_pcm_clock_us));
		disk_x_directory(&f, n == 0);
	}
}

UT_TEST(test_disk_x_LMS_full_and_NOT_ADMITTED_keep_original_receipt)
{
	PublishFixture f; ResourceXDecodedFrame done; ClusterPcmResourceXMasterState before;
	ResourceXIntentSlot offered;
	if (!disk_x_consume(false, RESOURCE_X_OUTCOME_OK, &f, &done)) return;
	consumed_ring_reset(); consumed_ring_shared.rings[0].count = PGRAC_LMS_OUTBOUND_CAPACITY;
	before = *f.state; offered = f.state->grant_intent.slot;
	UT_ASSERT_EQ(consumed_actual_lms_enqueue(0, &offered, 61, 9999999), CLUSTER_LMS_ENQUEUE_FULL);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(&offered,
		++fake_pcm_clock_us), RESOURCE_X_INTENT_NOT_ADMITTED);
	UT_ASSERT(memcmp(&before.deferred_settlement, &f.state->deferred_settlement,
		sizeof(before.deferred_settlement)) == 0);
	UT_ASSERT(memcmp(before.grant_intent.payload, f.state->grant_intent.payload,
		sizeof(before.grant_intent.payload)) == 0);
}

UT_TEST(test_disk_x_failed_callback_rearms_and_old_callback_cannot_retire)
{
	PublishFixture f; ResourceXDecodedFrame done; ResourceXIntentSlot old, current;
	if (!disk_x_consume(false, RESOURCE_X_OUTCOME_OK, &f, &done)) return;
	old = master_done_stage(&f);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&old, false, ++fake_pcm_clock_us));
	current = master_done_stage(&f);
	UT_ASSERT(current.send_episode > old.send_episode);
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&old, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 3);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&current, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 0);
	disk_x_directory(&f, true);
}

UT_TEST(test_disk_x_changed_independent_receipt_or_notice_blocks_retirement)
{
	for (int n = 0; n < 2; n++) {
		PublishFixture f; ResourceXDecodedFrame done, ack; ResourceXIntentSlot slot;
		ClusterPcmResourceXMasterState before;
		if (!disk_x_consume(false, RESOURCE_X_OUTCOME_OK, &f, &done)) continue;
		slot = master_done_stage(&f);
		if (n == 0) f.state->deferred_settlement.payload[260] ^= 1;
		else {
			ack = consumed_decode(&f); ack.body.acquire_completion.page_lsn++;
			UT_ASSERT(consumed_rewrite(&f, &ack));
		}
		before = *f.state;
		UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
		UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	}
}

UT_TEST(test_disk_x_consumed_notice_cannot_alias_requester_done_or_generic_release)
{
	PublishFixture f; ResourceXDecodedFrame done, ack; ResourceXIntentSlot slot;
	ClusterPcmResourceXMasterState before;
	if (!disk_x_consume(false, RESOURCE_X_OUTCOME_OK, &f, &done)) return;
	ack = consumed_decode(&f); before = *f.state;
	UT_ASSERT_EQ(master_done_receive(&ack), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	slot = master_done_stage(&f); before = *f.state;
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_intent_complete_exact(&slot));
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}

UT_TEST(test_disk_x_typed_completion_cannot_enter_legacy_settlement_owner)
{
	for (int resident = 0; resident < 2; resident++) {
		PublishFixture f; ResourceXDecodedFrame done; ResourceXMasterSnapshot out;
		ClusterPcmResourceXMasterState before;
		if (!disk_x_start(resident, RESOURCE_X_OUTCOME_OK, &f, &done)) continue;
		master_done_send(&f); before = *f.state;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(&done, 1, &out), RESOURCE_X_APPLY_INVALID);
		UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
		UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
		disk_x_directory(&f, true);
	}
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_disk_x_normal_consumes_original_A_for_first_fill_and_resident_N);
	UT_RUN(test_disk_x_early_completion_waits_original_GRANT_callback);
	UT_RUN(test_disk_x_quiet_with_and_without_image_never_publishes_X);
	UT_RUN(test_disk_x_duplicate_preserves_one_exact_receipt_and_grant);
	UT_RUN(test_disk_x_auth_domain_and_full_completion_identity_remain_exact);
	UT_RUN(test_disk_x_actual_LMS_shape_payload_takeover_and_callback_retire_once);
	UT_RUN(test_disk_x_LMS_full_and_NOT_ADMITTED_keep_original_receipt);
	UT_RUN(test_disk_x_failed_callback_rearms_and_old_callback_cannot_retire);
	UT_RUN(test_disk_x_changed_independent_receipt_or_notice_blocks_retirement);
	UT_RUN(test_disk_x_consumed_notice_cannot_alias_requester_done_or_generic_release);
	UT_RUN(test_disk_x_typed_completion_cannot_enter_legacy_settlement_owner);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
