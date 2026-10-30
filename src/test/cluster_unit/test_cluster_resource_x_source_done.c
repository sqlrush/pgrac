/* Real master ingress, canonical A and original holder ACK. Native C and
 * source capture are authenticated peer inputs, not native qualification.
 * A ready snapshot is evidence for settlement, never a consumed ACK/grant. */
#include "test_cluster_resource_x_source_ack_fixture.inc"
#include "test_cluster_resource_x_master_done_ingress.inc"

static bool
source_done_encode(ResourceXDecodedFrame *frame)
{
	uint8 bytes[RESOURCE_X_SOURCE_COMPLETE_V11_BYTES];
	uint16 len = 0;
	ResourceXWireReject reject;
	return cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		frame, bytes, sizeof(bytes), &len, &reject)
		&& len == sizeof(bytes)
		&& cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
			bytes, len, frame, &reject);
}

static bool
source_done_start(int scenario, int outcome, PublishFixture *f,
	ResourceXDecodedFrame *done, ResourceXDecodedFrame *ack)
{
	ResourceXDecodedFrame grant;
	ResourceXDecodedAcquireCompletion *c;
	if (!source_start(scenario, PCM_STATE_S, f)) return false;
	grant = publish_decode(f->state->grant_intent.payload, false);
	*ack = source_ack(f, grant.body.acquire_decision.source_node);
	memset(done, 0, sizeof(*done));
	done->kind = RESOURCE_X_WIRE_INSTALL_SETTLEMENT;
	done->payload_bytes = RESOURCE_X_SOURCE_COMPLETE_V11_BYTES;
	done->has_acquire_decision = true;
	done->common = grant.common;
	done->common.action_node = done->common.logical_assertion.requester_node;
	done->common.outcome = outcome;
	done->common.sender_connection_generation = 71;
	c = &done->body.acquire_completion;
	c->decision = grant.body.acquire_decision;
	c->native_generation = c->decision.episode.base_generation
		+ (outcome == RESOURCE_X_OUTCOME_OK ? 1 : 0);
	if (outcome != RESOURCE_X_OUTCOME_QUIET_NO_IMAGE) {
		c->page_lsn = 201;
		c->page_scn = ack->body.acquire_witness.original_proof.page_scn_lsn;
		c->page_crc32c = ack->body.acquire_witness.original_proof.page_checksum;
	}
	c->decision_crc32c = grant.common.semantic_crc32c;
	bool ok = source_done_encode(done);
	UT_ASSERT(ok); return ok;
}

static ResourceXApplyResult
source_done_receive(const ResourceXDecodedFrame *done)
{
	ClusterICEnvelope env = {0};
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE;
	env.source_node_id = done->common.logical_assertion.requester_node;
	env.dest_node_id = cluster_node_id;
	env.epoch = 17; env.payload_length = done->payload_bytes;
	return gcs_block_resource_x_acquire_done_ingress(&env, done, 61);
}

static bool
source_done_keep(PublishFixture *f, ResourceXDecodedFrame *done)
{
	ResourceXApplyResult result = source_done_receive(done);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	return result == RESOURCE_X_APPLY_APPLIED;
}

static ResourceXApplyResult
source_done_snapshot(PublishFixture *f, ResourceXDecodedFrame *out)
{
#ifdef SOURCE_MASTER_DONE_RED
	(void)out;
	return cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f->work);
#else
	return cluster_pcm_lock_resource_x_acquire_source_done_snapshot_exact(&f->work, out);
#endif
}

static void
source_done_callback(PublishFixture *f, bool sent)
{
	ResourceXIntentSlot staged;
	ResourceXIntentSlot *slot = &f->state->block_intents[0].slot;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
		slot, ++fake_pcm_clock_us, &staged), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(
		&staged, sent, ++fake_pcm_clock_us));
}

static void
source_done_unchanged(PublishFixture *f, ResourceXDecodedFrame *done)
{
	ClusterPcmResourceXMasterState before = *f->state;
	UT_ASSERT(source_done_receive(done) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, f->state, sizeof(before)) == 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT_EQ(master_durable_admission_depth, 0);
}

static void
source_done_wait(PublishFixture *f, ResourceXApplyResult expected)
{
	ResourceXDecodedFrame out, old;
	memset(&out, 0xa7, sizeof(out)); old = out;
	ClusterPcmResourceXMasterState before = *f->state;
	UT_ASSERT_EQ(source_done_snapshot(f, &out), expected);
	UT_ASSERT(memcmp(&before, f->state, sizeof(before)) == 0);
	UT_ASSERT(memcmp(&old, &out, sizeof(out)) == 0);
}

UT_TEST(test_source_done_retains_both_source_directory_modes)
{
	for (int n = 1; n <= 2; n++) {
		PublishFixture f; ResourceXDecodedFrame done, ack;
		if (!source_done_start(n, RESOURCE_X_OUTCOME_OK, &f, &done, &ack)) return;
		if (!source_done_keep(&f, &done)) return;
		UT_ASSERT_EQ(f.state->deferred_settlement.present, 4);
		UT_ASSERT_EQ(f.state->requests[1].final_authority_generation, 0);
		UT_ASSERT_EQ(f.state->grant_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
		UT_ASSERT_EQ(f.state->acquire_ack_connections[0], 0);
	}
}
UT_TEST(test_source_done_keeps_image_quiet_and_unarmed_quiet)
{
	for (int n = 0; n < 2; n++) {
		PublishFixture f; ResourceXDecodedFrame done, ack;
		if (!source_done_start(1, n ? RESOURCE_X_OUTCOME_QUIET_NO_IMAGE : RESOURCE_X_OUTCOME_QUIET,
			&f, &done, &ack)) return;
		if (!source_done_keep(&f, &done)) return;
		UT_ASSERT_EQ(f.state->deferred_settlement.present, 4);
		source_done_wait(&f, RESOURCE_X_APPLY_LOCAL_WAIT);
	}
}
UT_TEST(test_source_done_duplicate_and_outer_rebind_keep_one_exact_receipt)
{
	PublishFixture f; ResourceXDecodedFrame done, ack;
	if (!source_done_start(1, RESOURCE_X_OUTCOME_OK, &f, &done, &ack)
		|| !source_done_keep(&f, &done)) return;
	ClusterPcmResourceXMasterState before = *f.state;
	done.common.sender_connection_generation++;
	UT_ASSERT(source_done_encode(&done));
	UT_ASSERT_EQ(source_done_receive(&done), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}
UT_TEST(test_source_done_cannot_replace_pending_image_or_outcome)
{
	for (int n = 0; n < 4; n++) {
		PublishFixture f; ResourceXDecodedFrame done, ack;
		if (!source_done_start(1, RESOURCE_X_OUTCOME_OK, &f, &done, &ack)
			|| !source_done_keep(&f, &done)) return;
		if (n == 0) done.body.acquire_completion.page_crc32c++;
		if (n == 1) done.body.acquire_completion.page_scn++;
		if (n == 2) done.body.acquire_completion.page_lsn++;
		if (n == 3) { done.common.outcome = RESOURCE_X_OUTCOME_QUIET;
			done.body.acquire_completion.native_generation--; }
		UT_ASSERT(source_done_encode(&done)); source_done_unchanged(&f, &done);
	}
}
UT_TEST(test_source_done_valid_crc_cannot_replace_full_master_a)
{
	for (int n = 0; n < 7; n++) {
		PublishFixture f; ResourceXDecodedFrame done, ack;
		if (!source_done_start(2, RESOURCE_X_OUTCOME_OK, &f, &done, &ack)) return;
		ResourceXDecodedAcquireDecision *a = &done.body.acquire_completion.decision;
		if (n == 0) a->resource_binding++;
		if (n == 1) a->enqueue_order++;
		if (n == 2) a->episode.backend_generation++;
		if (n == 3) a->episode.reservation_token++;
		if (n == 4) a->holders[2].boot_incarnation++;
		if (n == 5) a->holders[3].grant_generation--;
		if (n == 6) a->source_service_generation++;
		/* Deliberately recompute both the A and completion CRCs. */
		ResourceXDecodedFrame grant = publish_decode(f.state->grant_intent.payload, false);
		grant.body.acquire_decision = *a;
		uint8 bytes[RESOURCE_X_DECISION_V4_BYTES]; uint16 len; ResourceXWireReject reject;
		UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &grant,
			bytes, sizeof(bytes), &len, &reject));
		UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
			bytes, len, &grant, &reject));
		done.body.acquire_completion.decision_crc32c = grant.common.semantic_crc32c;
		UT_ASSERT(source_done_encode(&done)); source_done_unchanged(&f, &done);
	}
}
UT_TEST(test_source_done_refuses_invalid_crc_and_ingress_domain)
{
	for (int n = 0; n < 5; n++) {
		PublishFixture f; ResourceXDecodedFrame done, ack;
		if (!source_done_start(1, RESOURCE_X_OUTCOME_OK, &f, &done, &ack)) return;
		if (n == 0) done.common.semantic_crc32c ^= 1;
		if (n == 1) ut_master_session++;
		if (n == 2) master_durable_admission = false;
		if (n == 3) auth_peer_boot++;
		if (n == 4) ready_connection_generation++;
		source_done_unchanged(&f, &done);
	}
}
UT_TEST(test_source_done_without_holder_ack_never_uses_requester_as_source_proof)
{
	PublishFixture f; ResourceXDecodedFrame done, ack;
	if (!source_done_start(2, RESOURCE_X_OUTCOME_OK, &f, &done, &ack)
		|| !source_done_keep(&f, &done)) return;
	source_done_callback(&f, true);
	source_done_wait(&f, RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT_EQ(f.state->acquire_ack_connections[0], 0);
}
UT_TEST(test_source_done_holder_ack_does_not_replace_forward_callback)
{
	PublishFixture f; ResourceXDecodedFrame done, ack;
	if (!source_done_start(1, RESOURCE_X_OUTCOME_OK, &f, &done, &ack)
		|| !source_done_keep(&f, &done)) return;
	UT_ASSERT_EQ(source_apply(&ack, 0), RESOURCE_X_APPLY_APPLIED);
	source_done_wait(&f, RESOURCE_X_APPLY_LOCAL_WAIT);
}
UT_TEST(test_source_done_failed_transport_remains_owned)
{
	PublishFixture f; ResourceXDecodedFrame done, ack;
	if (!source_done_start(1, RESOURCE_X_OUTCOME_OK, &f, &done, &ack)
		|| !source_done_keep(&f, &done)) return;
	UT_ASSERT_EQ(source_apply(&ack, 0), RESOURCE_X_APPLY_APPLIED);
	source_done_callback(&f, false);
	source_done_wait(&f, RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT_EQ(f.state->block_intents[0].slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
}
UT_TEST(test_source_done_all_arrival_orders_return_same_read_only_evidence)
{
	for (int scenario = 1; scenario <= 2; scenario++) for (int order = 0; order < 2; order++) {
		PublishFixture f; ResourceXDecodedFrame done, ack, out;
		if (!source_done_start(scenario, RESOURCE_X_OUTCOME_OK, &f, &done, &ack)) return;
		if (order) { UT_ASSERT_EQ(source_apply(&ack, 0), RESOURCE_X_APPLY_APPLIED);
			source_done_callback(&f, true); }
		if (!source_done_keep(&f, &done)) return;
		if (!order) { source_done_callback(&f, true);
			UT_ASSERT_EQ(source_apply(&ack, 0), RESOURCE_X_APPLY_APPLIED); }
		ClusterPcmResourceXMasterState before = *f.state;
		UT_ASSERT_EQ(source_done_snapshot(&f, &out), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&out.body.acquire_completion, &done.body.acquire_completion,
			sizeof(out.body.acquire_completion)) == 0);
		UT_ASSERT_EQ(out.common.sender_connection_generation, 1);
		UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
		/* Compatible S may now arm one source settlement, but cannot
		 * discharge PI or arm consumed without that source's exact ACK. */
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.work), RESOURCE_X_APPLY_LOCAL_WAIT);
		if (scenario == 2) {
			UT_ASSERT_EQ(f.state->source_settlement.state, RESOURCE_X_SOURCE_SETTLEMENT_PENDING);
			UT_ASSERT_EQ(f.state->source_settlement.intent.slot.payload_bytes, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES);
			before.source_settlement = f.state->source_settlement;
		}
		UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.work), RESOURCE_X_APPLY_LOCAL_WAIT);
		UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	}
}
UT_TEST(test_source_done_quiet_readiness_also_requires_original_ack)
{
	for (int n = 0; n < 2; n++) {
		PublishFixture f; ResourceXDecodedFrame done, ack, out;
		if (!source_done_start(2, n ? RESOURCE_X_OUTCOME_QUIET_NO_IMAGE : RESOURCE_X_OUTCOME_QUIET,
			&f, &done, &ack) || !source_done_keep(&f, &done)) return;
		source_done_callback(&f, true); source_done_wait(&f, RESOURCE_X_APPLY_LOCAL_WAIT);
		UT_ASSERT_EQ(source_apply(&ack, 0), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(source_done_snapshot(&f, &out), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(out.common.outcome, done.common.outcome);
	}
}
UT_TEST(test_source_done_wrong_image_crc_or_scn_never_becomes_ready)
{
	for (int n = 0; n < 2; n++) {
		PublishFixture f; ResourceXDecodedFrame done, ack;
		if (!source_done_start(1, RESOURCE_X_OUTCOME_OK, &f, &done, &ack)) return;
		if (n) done.body.acquire_completion.page_scn++; else done.body.acquire_completion.page_crc32c++;
		UT_ASSERT(source_done_encode(&done));
		if (!source_done_keep(&f, &done)) return;
		UT_ASSERT_EQ(source_apply(&ack, 0), RESOURCE_X_APPLY_APPLIED); source_done_callback(&f, true);
		source_done_wait(&f, RESOURCE_X_APPLY_STALE);
	}
}
UT_TEST(test_source_done_corrupt_original_proof_preserves_all_debt)
{
	PublishFixture f; ResourceXDecodedFrame done, ack;
	if (!source_done_start(1, RESOURCE_X_OUTCOME_OK, &f, &done, &ack)
		|| !source_done_keep(&f, &done)) return;
	UT_ASSERT_EQ(source_apply(&ack, 0), RESOURCE_X_APPLY_APPLIED); source_done_callback(&f, true);
	f.state->requests[1].source_fence[4] ^= 1;
	source_done_wait(&f, RESOURCE_X_APPLY_RECOVERY_BLOCKED);
}
UT_TEST(test_source_done_current_domain_drift_does_not_clear_pending)
{
	PublishFixture f; ResourceXDecodedFrame done, ack;
	if (!source_done_start(1, RESOURCE_X_OUTCOME_OK, &f, &done, &ack)
		|| !source_done_keep(&f, &done)) return;
	UT_ASSERT_EQ(source_apply(&ack, 0), RESOURCE_X_APPLY_APPLIED); source_done_callback(&f, true);
	ut_master_session++;
	source_done_wait(&f, RESOURCE_X_APPLY_STALE);
}
UT_TEST(test_source_done_late_work_does_not_read_another_episode)
{
	PublishFixture f; ResourceXDecodedFrame done, ack;
	if (!source_done_start(1, RESOURCE_X_OUTCOME_OK, &f, &done, &ack)
		|| !source_done_keep(&f, &done)) return;
	f.work.acquire_intent.backend_generation++;
	source_done_wait(&f, RESOURCE_X_APPLY_STALE);
}
UT_TEST(test_source_done_pending_cannot_be_forgotten_or_consumed_as_legacy)
{
	PublishFixture f; ResourceXDecodedFrame done, ack; ResourceXMasterSnapshot out;
	if (!source_done_start(1, RESOURCE_X_OUTCOME_OK, &f, &done, &ack)
		|| !source_done_keep(&f, &done)) return;
	ClusterPcmResourceXMasterState before = *f.state;
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&f.work);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	UT_ASSERT(cluster_pcm_lock_resource_x_install_settlement_exact(&done, 1, &out) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}

UT_TEST(test_source_done_resume_rechecks_retained_receipt_before_waiting)
{
	for (int n = 0; n < 2; n++) {
		PublishFixture f; ResourceXDecodedFrame done, ack;
		if (!source_done_start(1, RESOURCE_X_OUTCOME_OK, &f, &done, &ack)
			|| !source_done_keep(&f, &done)) return;
		if (n) f.state->deferred_settlement.authenticated_sender = 2;
		else f.state->deferred_settlement.payload[RESOURCE_X_SOURCE_COMPLETE_V11_BYTES - 1] ^= 1;
		ClusterPcmResourceXMasterState before = *f.state;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.work),
			RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	}
}

int
main(void)
{
	UT_PLAN(17);
	UT_RUN(test_source_done_retains_both_source_directory_modes);
	UT_RUN(test_source_done_keeps_image_quiet_and_unarmed_quiet);
	UT_RUN(test_source_done_duplicate_and_outer_rebind_keep_one_exact_receipt);
	UT_RUN(test_source_done_cannot_replace_pending_image_or_outcome);
	UT_RUN(test_source_done_valid_crc_cannot_replace_full_master_a);
	UT_RUN(test_source_done_refuses_invalid_crc_and_ingress_domain);
	UT_RUN(test_source_done_without_holder_ack_never_uses_requester_as_source_proof);
	UT_RUN(test_source_done_holder_ack_does_not_replace_forward_callback);
	UT_RUN(test_source_done_failed_transport_remains_owned);
	UT_RUN(test_source_done_all_arrival_orders_return_same_read_only_evidence);
	UT_RUN(test_source_done_quiet_readiness_also_requires_original_ack);
	UT_RUN(test_source_done_wrong_image_crc_or_scn_never_becomes_ready);
	UT_RUN(test_source_done_corrupt_original_proof_preserves_all_debt);
	UT_RUN(test_source_done_current_domain_drift_does_not_clear_pending);
	UT_RUN(test_source_done_late_work_does_not_read_another_episode);
	UT_RUN(test_source_done_pending_cannot_be_forgotten_or_consumed_as_legacy);
	UT_RUN(test_source_done_resume_rechecks_retained_receipt_before_waiting);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
