/* Full-E disk X through the original T1/T2/T3 and native writer fences.
 * Encoded peer A and staged I are explicit inputs. This component does not
 * read disk, qualify physical installation, or activate an ordinary writer.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_native_acquire_execution_fixture.inc"

static ResourceXAcquisitionRef xp_ref;
static ResourceXAcquireReadReceipt xp_receipt;
static ClusterPcmOwnSnapshot xp_terminal;

#define XP_REQUIRE(condition) do { \
	bool ok = (condition); UT_ASSERT(ok); if (!ok) return; \
} while (0)

static bool
xp_start_kind(EvidenceFixture *f, int native)
{
	if (!execution_start(f, 1, native) || execution_enter() != RESOURCE_X_APPLY_APPLIED)
		return false;
	xp_ref.assertion = execution_claim.observation.request.logical_assertion;
	xp_ref.formation = execution_claim.observation.request.resource_formation;
	xp_ref.acquisition_generation = execution_claim.observation.request.assertion_sequence;
	memset(&xp_receipt, 0, sizeof(xp_receipt));
	xp_receipt.authority_generation = input_grant.common.authority_generation;
	xp_receipt.decision_crc32c = input_grant.common.semantic_crc32c;
	xp_receipt.page_lsn = 12345;
	xp_receipt.page_scn = 93;
	xp_receipt.page_crc32c = 17;
	return cluster_pcm_lock_resource_x_requester_acquire_read_arm_exact(
		&execution_claim, &prepared_episode, &xp_receipt) == RESOURCE_X_APPLY_APPLIED;
}

static bool xp_start(EvidenceFixture *f) { return xp_start_kind(f, 0); }

static ResourceXApplyResult xp_t1(void)
{
	return cluster_pcm_lock_resource_x_t1_grant_delivery_exact(&xp_ref, &execution_claim);
}

/* Use the real native owner, including the independent activation binding.
 * The fixture's explicit I is not claimed as an actual disk-read result. */
static ResourceXBufferInstallProof
xp_native_commit(void)
{
	ResourceXBufferInstallProof proof;
	uint32 state = request_header_lock(&request_buf);
	UT_ASSERT_EQ(cluster_pcm_own_writer_grant_commit_exact(0, prepared_episode.base_generation,
		prepared_episode.reservation_token, &proof.ownership_generation), CLUSTER_PCM_OWN_OK);
	request_buf.pcm_state = PCM_STATE_X;
	request_buf.buffer_type = BUF_TYPE_XCUR;
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_bind_exact(0, proof.ownership_generation,
		prepared_episode.reservation_token, xp_ref.acquisition_generation), CLUSTER_PCM_OWN_OK);
	proof.writer_activation_token = prepared_episode.reservation_token;
	proof.resource_x_activation_generation = xp_ref.acquisition_generation;
	UnlockBufHdr(&request_buf, state);
	return proof;
}

static ResourceXBufferActivationProof
xp_native_activate(const ResourceXBufferInstallProof *installed)
{
	ResourceXBufferActivationProof proof;
	uint32 state = request_header_lock(&request_buf);
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_clear_exact(0, installed->ownership_generation,
		prepared_episode.reservation_token, xp_ref.acquisition_generation), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.writer_activation_token), 0);
	proof.ownership_generation = installed->ownership_generation;
	proof.writer_activation_token = 0;
	proof.resource_x_activation_generation = 0;
	UnlockBufHdr(&request_buf, state);
	return proof;
}

static void
xp_complete_io_input(void)
{
	uint32 state = request_header_lock(&request_buf);
	if (ResourceOwnerHasBufferIO(CurrentResourceOwner, 1))
		ResourceOwnerForgetBufferIO(CurrentResourceOwner, 1);
	state &= ~BM_IO_IN_PROGRESS;
	state |= BM_VALID;
	UnlockBufHdr(&request_buf, state);
	state = request_header_lock(&request_buf);
	cluster_pcm_own_snapshot_locked(&request_buf, &xp_terminal);
	UnlockBufHdr(&request_buf, state);
}

static bool
xp_finish(EvidenceFixture *f, int native)
{
	ResourceXBufferInstallProof installed;
	ResourceXBufferActivationProof active;
	if (!xp_start_kind(f, native) || xp_t1() != RESOURCE_X_APPLY_APPLIED) return false;
	installed = xp_native_commit();
	if (cluster_pcm_lock_resource_x_requester_apply_delivery_exact(
		&xp_ref, &installed, &execution_claim) != RESOURCE_X_APPLY_APPLIED) return false;
	active = xp_native_activate(&installed);
	if (cluster_pcm_lock_resource_x_requester_activate_delivery_exact(
		&xp_ref, &active, &execution_claim) != RESOURCE_X_APPLY_APPLIED) return false;
	xp_complete_io_input();
	if (cluster_pcm_lock_resource_x_requester_acquire_read_complete_exact(
		&execution_claim, &prepared_episode, &xp_receipt, &xp_terminal) != RESOURCE_X_APPLY_APPLIED)
		return false;
	{
		uint32 state = request_header_lock(&request_buf);
		UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_release_exact(0, xp_terminal.generation,
			execution_claim.executor_sequence), CLUSTER_PCM_OWN_OK);
		UnlockBufHdr(&request_buf, state);
	}
	return cluster_pcm_lock_resource_x_requester_acquire_read_handoff_exact(
		&execution_claim, &prepared_episode, &xp_receipt, &xp_terminal) == RESOURCE_X_APPLY_APPLIED;
}

static ResourceXIntentSlot
xp_offer(ResourceXDecodedFrame *frame)
{
	ResourceXIntentSlot slot, snapshot;
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES];
	uint32 cursor = RESOURCE_X_PROTOCOL_NODE_LIMIT + 3;
	ResourceXWireReject reject;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_ready_intent_probe_exact(&request_base.tag,
		&cursor, &slot), RESOURCE_X_INTENT_PROBE_FOUND);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(&slot,
		&snapshot, bytes, sizeof(bytes)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		bytes, sizeof(bytes), frame, &reject));
	return slot;
}

static ResourceXApplyResult
xp_ack(ResourceXDecodedFrame *frame)
{
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES];
	uint16 len;
	ResourceXWireReject reject;
	frame->common.outcome = RESOURCE_X_OUTCOME_DUPLICATE;
	frame->common.action_node = frame->body.acquire_completion.decision.master_node;
	frame->common.sender_connection_generation = 71;
	frame->common.semantic_crc32c = 0;
	if (!cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		frame, bytes, sizeof(bytes), &len, &reject)
		|| !cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
			bytes, len, frame, &reject)) return RESOURCE_X_APPLY_INVALID;
	auth_peer_boot = 31;
	return cluster_pcm_lock_resource_x_requester_acquire_read_consumed_exact(frame, 0, 61, 0, 61, 77);
}

UT_TEST(test_x_phase_retains_one_request_through_real_native_fences_and_T3)
{
	EvidenceFixture f;
	ResourceXBufferInstallProof installed;
	ResourceXBufferActivationProof active;
	XP_REQUIRE(xp_start(&f));
	UT_ASSERT_EQ(xp_t1(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(xp_t1(), RESOURCE_X_APPLY_DUPLICATE);
	installed = xp_native_commit();
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_delivery_exact(
		&xp_ref, &installed, &execution_claim), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_delivery_exact(
		&xp_ref, &installed, &execution_claim), RESOURCE_X_APPLY_DUPLICATE);
	active = xp_native_activate(&installed);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_delivery_exact(
		&xp_ref, &active, &execution_claim), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_delivery_exact(
		&xp_ref, &active, &execution_claim), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&xp_ref, 31, 77, active.ownership_generation));
	xp_complete_io_input();
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_read_complete_exact(
		&execution_claim, &prepared_episode, &xp_receipt, &xp_terminal), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_read_complete_exact(
		&execution_claim, &prepared_episode, &xp_receipt, &xp_terminal), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), prepared_episode.base_generation + 1);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.episode, 1);
	UT_ASSERT_EQ(request_shared.rings[1].count, 1);
}

UT_TEST(test_x_phase_T3_requires_same_T2_and_both_native_fences_quiet)
{
	EvidenceFixture f;
	ResourceXBufferInstallProof installed;
	ResourceXBufferActivationProof active;
	XP_REQUIRE(xp_start(&f));
	UT_ASSERT_EQ(xp_t1(), RESOURCE_X_APPLY_APPLIED);
	installed = xp_native_commit();
	active.ownership_generation = installed.ownership_generation;
	active.writer_activation_token = active.resource_x_activation_generation = 0;
	UT_ASSERT(cluster_pcm_lock_resource_x_requester_activate_delivery_exact(
		&xp_ref, &active, &execution_claim) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_delivery_exact(
		&xp_ref, &installed, &execution_claim), RESOURCE_X_APPLY_APPLIED);
	active.writer_activation_token = prepared_episode.reservation_token;
	UT_ASSERT(cluster_pcm_lock_resource_x_requester_activate_delivery_exact(
		&xp_ref, &active, &execution_claim) != RESOURCE_X_APPLY_APPLIED);
	active = xp_native_activate(&installed);
	active.ownership_generation++;
	UT_ASSERT(cluster_pcm_lock_resource_x_requester_activate_delivery_exact(
		&xp_ref, &active, &execution_claim) != RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_x_phase_rejects_forged_execution_and_install_generation)
{
	for (int n = 0; n < 5; n++) {
		EvidenceFixture f;
		ResourceXDeliveryClaim forged;
		ResourceXBufferInstallProof proof;
		XP_REQUIRE(xp_start(&f));
		forged = execution_claim;
		if (n == 0) forged.executor_sequence++;
		if (n == 1) forged.observation.entry_binding_generation++;
		if (n == 2) forged.observation.r4_record_generation++;
		if (n == 3) forged.target.reservation_token++;
		if (n == 4) forged.purpose = RESOURCE_X_DELIVERY_NORMAL;
		UT_ASSERT(cluster_pcm_lock_resource_x_t1_grant_delivery_exact(&xp_ref, &forged)
			!= RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(xp_t1(), RESOURCE_X_APPLY_APPLIED);
		proof = xp_native_commit();
		proof.ownership_generation++;
		UT_ASSERT(cluster_pcm_lock_resource_x_requester_apply_delivery_exact(
			&xp_ref, &proof, &execution_claim) != RESOURCE_X_APPLY_APPLIED);
		proof.ownership_generation--; proof.writer_activation_token++;
		UT_ASSERT(cluster_pcm_lock_resource_x_requester_apply_delivery_exact(
			&xp_ref, &proof, &execution_claim) != RESOURCE_X_APPLY_APPLIED);
	}
}

UT_TEST(test_x_phase_cannot_publish_C_before_T3)
{
	EvidenceFixture f;
	ResourceXBufferInstallProof installed;
	XP_REQUIRE(xp_start(&f));
	UT_ASSERT_EQ(xp_t1(), RESOURCE_X_APPLY_APPLIED);
	installed = xp_native_commit();
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_delivery_exact(
		&xp_ref, &installed, &execution_claim), RESOURCE_X_APPLY_APPLIED);
	xp_complete_io_input();
	xp_terminal.writer_activation_token = xp_terminal.resource_x_activation_generation = 0;
	UT_ASSERT(cluster_pcm_lock_resource_x_requester_acquire_read_complete_exact(
		&execution_claim, &prepared_episode, &xp_receipt, &xp_terminal) != RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_x_phase_resident_N_uses_same_native_and_completion_owners)
{
	EvidenceFixture f;
	XP_REQUIRE(xp_finish(&f, 1));
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), prepared_episode.base_generation + 1);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&xp_ref, 31, 77, xp_terminal.generation));
}

UT_TEST(test_x_phase_T1_cancel_retires_only_unpublished_original_progress)
{
	EvidenceFixture f;
	ClusterPcmOwnSnapshot native;
	uint32 state;
	XP_REQUIRE(xp_start(&f));
	UT_ASSERT_EQ(xp_t1(), RESOURCE_X_APPLY_APPLIED);
	state = request_header_lock(&request_buf);
	cluster_pcm_own_snapshot_locked(&request_buf, &native);
	UnlockBufHdr(&request_buf, state);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_read_seal_exact(
		&execution_claim, &prepared_episode, &xp_receipt, &native), RESOURCE_X_APPLY_APPLIED);
	/* The exact quiet native proof is an explicit adapter input here. */
	native.flags = 0; native.semantic_buf_state = BM_TAG_VALID | BM_IO_ERROR;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_read_quiet_exact(
		&execution_claim, &prepared_episode, &xp_receipt, &native), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(xp_t1() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		RESOURCE_X_DELIVERY_ACQUIRE_READ_QUIET);
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&xp_ref, 31, 77, prepared_episode.base_generation + 1));
}

UT_TEST(test_x_phase_T2_cannot_be_sealed_as_an_unpublished_fill)
{
	EvidenceFixture f;
	ResourceXBufferInstallProof installed;
	ClusterPcmOwnSnapshot base;
	XP_REQUIRE(xp_start(&f)); UT_ASSERT_EQ(xp_t1(), RESOURCE_X_APPLY_APPLIED);
	base = request_base; base.flags = PCM_OWN_FLAG_GRANT_PENDING;
	base.reservation_token = prepared_episode.reservation_token;
	installed = xp_native_commit();
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_delivery_exact(
		&xp_ref, &installed, &execution_claim), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_requester_acquire_read_seal_exact(
		&execution_claim, &prepared_episode, &xp_receipt, &base) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		RESOURCE_X_DELIVERY_ACQUIRE_READ_ARMED);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.writer_activation_token), prepared_episode.reservation_token);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.resource_x_activation_generation), xp_ref.acquisition_generation);
}

UT_TEST(test_x_phase_elapsed_time_neither_retries_nor_expires_native_responsibility)
{
	EvidenceFixture f;
	ResourceXBufferInstallProof installed;
	ResourceXBufferActivationProof active;
	XP_REQUIRE(xp_start(&f));
	fake_pcm_clock_us += UINT64_C(3600000000);
	UT_ASSERT_EQ(xp_t1(), RESOURCE_X_APPLY_APPLIED);
	installed = xp_native_commit();
	fake_pcm_clock_us += UINT64_C(3600000000);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_delivery_exact(
		&xp_ref, &installed, &execution_claim), RESOURCE_X_APPLY_APPLIED);
	active = xp_native_activate(&installed);
	fake_pcm_clock_us += UINT64_C(3600000000);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_delivery_exact(
		&xp_ref, &active, &execution_claim), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_shared.rings[1].count, 1);
}

UT_TEST(test_x_phase_domain_or_future_attempt_cannot_release_original_T2)
{
	for (int n = 0; n < 3; n++) {
		EvidenceFixture f;
		ResourceXBufferInstallProof installed;
		ResourceXBufferActivationProof active;
		ResourceXDeliveryClaim changed;
		ClusterPcmResourceXBootstrapRound before;
		XP_REQUIRE(xp_start(&f)); UT_ASSERT_EQ(xp_t1(), RESOURCE_X_APPLY_APPLIED);
		installed = xp_native_commit();
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_delivery_exact(
			&xp_ref, &installed, &execution_claim), RESOURCE_X_APPLY_APPLIED);
		active = xp_native_activate(&installed); changed = execution_claim;
		if (n == 0) changed.observation.request.master_session_incarnation++;
		if (n == 1) changed.observation.r4_record_generation++;
		if (n == 2) changed.observation.request.assertion_sequence++;
		before = request_entry()->resource_x_bootstrap_round;
		UT_ASSERT(cluster_pcm_lock_resource_x_requester_activate_delivery_exact(
			&xp_ref, &active, &changed) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, &request_entry()->resource_x_bootstrap_round, sizeof(before)) == 0);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_delivery_exact(
			&xp_ref, &active, &execution_claim), RESOURCE_X_APPLY_APPLIED);
	}
}

UT_TEST(test_x_phase_ACK_and_actual_callback_independently_retire_notification)
{
	EvidenceFixture f;
	ResourceXDecodedFrame frame;
	ResourceXIntentSlot slot, staged;
	XP_REQUIRE(xp_finish(&f, 0));
	slot = xp_offer(&frame);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(&slot, 201),
		RESOURCE_X_INTENT_NOT_ADMITTED);
	/* NOT_ADMITTED advances the real offer's attempt cut. Re-observe it;
	 * a stale pre-attempt descriptor cannot be staged. */
	fake_pcm_clock_us = 1000000;
	slot = xp_offer(&frame);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(&slot, 1000001, &staged),
		RESOURCE_X_INTENT_STAGED);
	UT_ASSERT_EQ(xp_ack(&frame), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		RESOURCE_X_DELIVERY_ACQUIRE_READ_CONSUMED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, false, 1000002));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		RESOURCE_X_DELIVERY_ACQUIRE_READ_SETTLED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_read_settle_exact(
		&execution_claim, &prepared_episode, &xp_receipt, &xp_terminal), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&xp_ref, 31, 77, xp_terminal.generation));
}

UT_TEST(test_x_phase_forged_ACK_cannot_consume_original_C)
{
	EvidenceFixture f;
	ResourceXDecodedFrame frame;
	ClusterPcmResourceXBootstrapRound before;
	XP_REQUIRE(xp_finish(&f, 0));
	(void)xp_offer(&frame); frame.body.acquire_completion.native_generation++;
	before = request_entry()->resource_x_bootstrap_round;
	UT_ASSERT(xp_ack(&frame) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, &request_entry()->resource_x_bootstrap_round, sizeof(before)) == 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), prepared_episode.base_generation + 1);
}

int main(void)
{
	UT_PLAN(11);
	UT_RUN(test_x_phase_retains_one_request_through_real_native_fences_and_T3);
	UT_RUN(test_x_phase_T3_requires_same_T2_and_both_native_fences_quiet);
	UT_RUN(test_x_phase_rejects_forged_execution_and_install_generation);
	UT_RUN(test_x_phase_cannot_publish_C_before_T3);
	UT_RUN(test_x_phase_resident_N_uses_same_native_and_completion_owners);
	UT_RUN(test_x_phase_T1_cancel_retires_only_unpublished_original_progress);
	UT_RUN(test_x_phase_T2_cannot_be_sealed_as_an_unpublished_fill);
	UT_RUN(test_x_phase_elapsed_time_neither_retries_nor_expires_native_responsibility);
	UT_RUN(test_x_phase_domain_or_future_attempt_cannot_release_original_T2);
	UT_RUN(test_x_phase_ACK_and_actual_callback_independently_retire_notification);
	UT_RUN(test_x_phase_forged_ACK_cannot_consume_original_C);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
