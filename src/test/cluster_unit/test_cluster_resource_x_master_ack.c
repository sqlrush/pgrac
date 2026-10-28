/* Actual master admission/publication, original ACK entry and real codec.
 * Holder native production and peer authentication remain explicit input
 * seams; this component does not qualify a holder revoke or requester C. */
#include "test_cluster_resource_x_master_ack_fixture.inc"

static bool
master_ack_round_trip(ResourceXDecodedFrame *frame)
{
	uint8 bytes[RESOURCE_X_SOURCE_WITNESS_V3_BYTES];
	uint16 len = 0;
	ResourceXWireReject reject;
	bool ok = cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCKED_TO_N, frame,
		bytes, sizeof(bytes), &len, &reject);
	UT_ASSERT(ok);
	if (!ok) return false;
	ok = cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCKED_TO_N, bytes, len, frame, &reject);
	UT_ASSERT(ok);
	return ok;
}

static ResourceXDecodedFrame
master_ack_frame(PublishFixture *f, int holder)
{
	ResourceXDecodedFrame frame = publish_decode(f->state->grant_intent.payload, false);
	ResourceXDecodedAcquireDecision a = frame.body.acquire_decision;
	ResourceXDecodedAcquireWitness w = {0};
	frame.kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
	frame.common.action_node = holder;
	frame.common.outcome = RESOURCE_X_OUTCOME_OK;
	frame.common.sender_connection_generation = 71;
	w.decision = a;
	w.original_ack = frame.common;
	w.original_ack.authority_generation = frame.common.base_authority_generation;
	w.original_ack.observed_mode = a.directory_mode;
	w.original_ack.target_mode = PCM_STATE_N;
	w.original_ack.sender_connection_generation = 61;
	frame.body.acquire_witness = w;
	(void)master_ack_round_trip(&frame);
	return frame;
}

static bool
master_ack_start(PublishFixture *f)
{
	if (!publish_start(2, PCM_STATE_X, f)) return false;
	publish_apply(f);
	return true;
}

static ResourceXApplyResult
master_ack_apply(ResourceXDecodedFrame *frame, int authenticated)
{
	ResourceXMasterSnapshot out;
	ResourceXApplyResult result = cluster_pcm_lock_resource_x_blocked_to_n_exact(frame,
		authenticated, &out);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	return result;
}

static void
master_ack_refuse_unchanged(PublishFixture *f, ResourceXDecodedFrame *frame, int auth,
	ResourceXApplyResult expected)
{
	ClusterPcmResourceXMasterState *before = malloc(sizeof(*before));
	UT_ASSERT(before != NULL); if (!before) return;
	memcpy(before, f->state, sizeof(*before));
	UT_ASSERT_EQ(master_ack_apply(frame, auth), expected);
	UT_ASSERT(memcmp(before, f->state, sizeof(*before)) == 0);
	free(before);
}

UT_TEST(test_master_ack_records_only_the_actual_non_source_holder)
{
	PublishFixture f;
	ResourceXDecodedFrame ack;
	ClusterPcmResourceXGrantIntent grant;
	ClusterPcmResourceXMasterRequest expected;
	ClusterPcmResourceXMasterRequest *r;
	if (!master_ack_start(&f)) return;
	ack = master_ack_frame(&f, 2);
	grant = f.state->grant_intent;
	expected = f.state->requests[1];
	expected.blocked_holders_bitmap = UINT32_C(4);
	/* Publication sealed the immutable directory and service set. */
	UT_ASSERT_EQ(master_ack_apply(&ack, 2), RESOURCE_X_APPLY_APPLIED);
	r = &f.state->requests[1];
	UT_ASSERT_EQ(r->blocked_holders_bitmap, UINT32_C(4));
	UT_ASSERT_EQ(r->final_authority_generation, 0);
	UT_ASSERT_EQ(r->proof_kind, 0);
	UT_ASSERT_EQ(r->phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(f.state->authority_generation, f.plan.proposed_authority_generation);
	UT_ASSERT(memcmp(r, &expected, sizeof(expected)) == 0);
	UT_ASSERT(memcmp(&f.state->grant_intent, &grant, sizeof(grant)) == 0);
}

UT_TEST(test_master_ack_non_source_permutations_do_not_complete_the_source)
{
	for (int order = 0; order < 2; order++) {
		PublishFixture f;
		if (!master_ack_start(&f)) return;
		for (int n = 0; n < 2; n++) {
			int holder = (n ^ order) ? 3 : 2;
			ResourceXDecodedFrame ack = master_ack_frame(&f, holder);
			UT_ASSERT_EQ(master_ack_apply(&ack, holder), RESOURCE_X_APPLY_APPLIED);
		}
		UT_ASSERT_EQ(f.state->requests[1].blocked_holders_bitmap, UINT32_C(12));
		UT_ASSERT_EQ(f.state->requests[1].incompatible_holders_bitmap, UINT32_C(13));
		UT_ASSERT_EQ(f.state->requests[1].phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
		UT_ASSERT_EQ(f.state->requests[1].final_authority_generation, 0);
		UT_ASSERT_EQ(f.state->requests[1].proof_kind, 0);
	}
}

UT_TEST(test_master_ack_exact_duplicate_is_a_no_op)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	if (!master_ack_start(&f)) return;
	ack = master_ack_frame(&f, 2);
	UT_ASSERT_EQ(master_ack_apply(&ack, 2), RESOURCE_X_APPLY_APPLIED);
	master_ack_refuse_unchanged(&f, &ack, 2, RESOURCE_X_APPLY_DUPLICATE);
}

UT_TEST(test_master_ack_outer_rebind_keeps_original_receipt)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	if (!master_ack_start(&f)) return;
	ack = master_ack_frame(&f, 2);
	UT_ASSERT_EQ(master_ack_apply(&ack, 2), RESOURCE_X_APPLY_APPLIED);
	ack.common.sender_connection_generation++;
	if (!master_ack_round_trip(&ack)) return;
	master_ack_refuse_unchanged(&f, &ack, 2, RESOURCE_X_APPLY_DUPLICATE);
}

UT_TEST(test_master_ack_changed_original_receipt_cannot_alias_a_duplicate)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	if (!master_ack_start(&f)) return;
	ack = master_ack_frame(&f, 2);
	UT_ASSERT_EQ(master_ack_apply(&ack, 2), RESOURCE_X_APPLY_APPLIED);
	ack.body.acquire_witness.original_ack.sender_connection_generation++;
	if (!master_ack_round_trip(&ack)) return;
	master_ack_refuse_unchanged(&f, &ack, 2, RESOURCE_X_APPLY_STALE);
}

UT_TEST(test_master_ack_authenticated_sender_cannot_ack_another_holder)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	if (!master_ack_start(&f)) return;
	ack = master_ack_frame(&f, 2);
	master_ack_refuse_unchanged(&f, &ack, 3, RESOURCE_X_APPLY_INVALID);
	master_ack_refuse_unchanged(&f, &ack, -1, RESOURCE_X_APPLY_INVALID);
}

UT_TEST(test_master_ack_valid_crc_cannot_replace_any_part_of_a)
{
	for (int field = 0; field < 12; field++) {
		PublishFixture f; ResourceXDecodedFrame ack;
		ResourceXDecodedAcquireDecision *a;
		if (!master_ack_start(&f)) return;
		ack = master_ack_frame(&f, 2); a = &ack.body.acquire_witness.decision;
		switch (field) {
		case 0: a->resource_binding++; break;
		case 1: a->enqueue_order++; break;
		case 2: a->episode.backend_generation++; break;
		case 3: a->episode.base_generation++; break;
		case 4: a->episode.reservation_token++; break;
		case 5: a->episode.cluster_epoch++; break;
		case 6: a->episode.r4_generation++; break;
		case 7: a->episode.buffer_id++; break;
		case 8: a->episode.requester_incarnation++; break;
		case 9: a->holders[2].boot_incarnation++; break;
		case 10: a->master_ingress_generation++; break;
		case 11: a->requester_ingress_generation++; break;
		}
		if (!master_ack_round_trip(&ack)) return;
		master_ack_refuse_unchanged(&f, &ack, 2, RESOURCE_X_APPLY_STALE);
	}
}

UT_TEST(test_master_ack_old_domain_preserves_canonical_and_receipts)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	if (!master_ack_start(&f)) return;
	ack = master_ack_frame(&f, 2);
	UT_ASSERT_EQ(master_ack_apply(&ack, 2), RESOURCE_X_APPLY_APPLIED);
	ut_master_session++;
	master_ack_refuse_unchanged(&f, &ack, 2, RESOURCE_X_APPLY_STALE);
}

UT_TEST(test_master_ack_received_evidence_blocks_pre_send_forget)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	ClusterPcmResourceXMasterState *before;
	if (!master_ack_start(&f)) return;
	ack = master_ack_frame(&f, 2);
	UT_ASSERT_EQ(master_ack_apply(&ack, 2), RESOURCE_X_APPLY_APPLIED);
	before = malloc(sizeof(*before)); UT_ASSERT(before != NULL); if (!before) return;
	memcpy(before, f.state, sizeof(*before));
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&f.work);
	UT_ASSERT(memcmp(before, f.state, sizeof(*before)) == 0);
	free(before);
}

UT_TEST(test_master_ack_non_source_cannot_supply_source_or_native_proof)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	if (!master_ack_start(&f)) return;
	ack = master_ack_frame(&f, 2);
	ack.common.action_node = f.plan.decision.source_node;
	ack.body.acquire_witness.original_ack.action_node = ack.common.action_node;
	master_ack_refuse_unchanged(&f, &ack, ack.common.action_node, RESOURCE_X_APPLY_INVALID);
}

UT_TEST(test_master_ack_legacy_ack_cannot_consume_conditional_a)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	if (!master_ack_start(&f)) return;
	ack = master_ack_frame(&f, 2);
	ack.common = ack.body.acquire_witness.original_ack;
	ack.has_acquire_decision = false;
	ack.payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	memset(&ack.body, 0, sizeof(ack.body));
	master_ack_refuse_unchanged(&f, &ack, 2, RESOURCE_X_APPLY_BAD_STATE);
}

UT_TEST(test_master_ack_master_replay_does_not_erase_consumed_holder)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	ClusterPcmResourceXMasterState *before;
	if (!master_ack_start(&f)) return;
	ack = master_ack_frame(&f, 2);
	UT_ASSERT_EQ(master_ack_apply(&ack, 2), RESOURCE_X_APPLY_APPLIED);
	before = malloc(sizeof(*before)); UT_ASSERT(before != NULL); if (!before) return;
	memcpy(before, f.state, sizeof(*before));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&f.work, NULL),
		RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(before, f.state, sizeof(*before)) == 0);
	free(before);
}

UT_TEST(test_master_ack_corrupt_canonical_never_consumes_valid_ack)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	if (!master_ack_start(&f)) return;
	ack = master_ack_frame(&f, 2);
	f.state->grant_intent.payload[160] ^= 1;
	master_ack_refuse_unchanged(&f, &ack, 2, RESOURCE_X_APPLY_RECOVERY_BLOCKED);
}

UT_TEST(test_master_ack_unverified_crc_and_holder_provenance_are_rejected)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	if (!master_ack_start(&f)) return;
	ack = master_ack_frame(&f, 2);
	ack.common.semantic_crc32c ^= 1;
	master_ack_refuse_unchanged(&f, &ack, 2, RESOURCE_X_APPLY_INVALID);
	ack = master_ack_frame(&f, 2);
	f.state->holder_grants[2].boot_incarnation++;
	master_ack_refuse_unchanged(&f, &ack, 2, RESOURCE_X_APPLY_STALE);
}

int
main(void)
{
	UT_PLAN(14);
	UT_RUN(test_master_ack_records_only_the_actual_non_source_holder);
	UT_RUN(test_master_ack_non_source_permutations_do_not_complete_the_source);
	UT_RUN(test_master_ack_exact_duplicate_is_a_no_op);
	UT_RUN(test_master_ack_outer_rebind_keeps_original_receipt);
	UT_RUN(test_master_ack_changed_original_receipt_cannot_alias_a_duplicate);
	UT_RUN(test_master_ack_authenticated_sender_cannot_ack_another_holder);
	UT_RUN(test_master_ack_valid_crc_cannot_replace_any_part_of_a);
	UT_RUN(test_master_ack_old_domain_preserves_canonical_and_receipts);
	UT_RUN(test_master_ack_received_evidence_blocks_pre_send_forget);
	UT_RUN(test_master_ack_non_source_cannot_supply_source_or_native_proof);
	UT_RUN(test_master_ack_legacy_ack_cannot_consume_conditional_a);
	UT_RUN(test_master_ack_master_replay_does_not_erase_consumed_holder);
	UT_RUN(test_master_ack_corrupt_canonical_never_consumes_valid_ack);
	UT_RUN(test_master_ack_unverified_crc_and_holder_provenance_are_rejected);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
