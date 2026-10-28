/* Actual original master, full-A publication and ACK consumer. Source native
 * completion and peer authentication are explicit input seams, not claimed
 * by the fixture. No requester/native/PI completion is inferred here. */
#include "test_cluster_resource_x_master_ack_fixture.inc"

static void
source_put_u64(uint8 *out, uint64 value)
{
	for (int n = 7; n >= 0; n--) { out[n] = value & 255; value >>= 8; }
}

static bool
source_round_trip(ResourceXDecodedFrame *frame)
{
	uint8 bytes[RESOURCE_X_SOURCE_WITNESS_V4_BYTES];
	uint16 len = 0; ResourceXWireReject reject;
	bool ok = cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCKED_TO_N, frame,
		bytes, sizeof(bytes), &len, &reject);
	UT_ASSERT(ok); if (!ok) return false;
	ok = cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCKED_TO_N, bytes, len, frame, &reject);
	UT_ASSERT(ok); return ok;
}

static bool
source_start(int scenario, PcmState target, PublishFixture *f)
{
	if (!publish_start(scenario, target, f)) return false;
	publish_apply(f); return true;
}

static ResourceXDecodedFrame
source_ack(PublishFixture *f, int node)
{
	ResourceXDecodedFrame frame = publish_decode(f->state->grant_intent.payload, false);
	ResourceXDecodedAcquireWitness w = {0};
	ResourceXDecodedBlockedToN *p = &w.original_proof;
	w.decision = frame.body.acquire_decision;
	frame.kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
	frame.common.action_node = node; frame.common.outcome = RESOURCE_X_OUTCOME_OK;
	frame.common.sender_connection_generation = 71;
	w.original_ack = frame.common;
	w.original_ack.authority_generation = frame.common.base_authority_generation;
	w.original_ack.observed_mode = w.decision.directory_mode;
	w.original_ack.target_mode = frame.common.target_mode == PCM_STATE_S ? PCM_STATE_S : PCM_STATE_N;
	w.original_ack.sender_connection_generation = 61;
	w.has_source_proof = node == w.decision.source_node;
	if (w.has_source_proof) {
		w.original_ack.flags = w.decision.directory_mode == PCM_STATE_S
			&& frame.common.target_mode == PCM_STATE_S ? 0 : RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED;
		p->source_fence[3] = node;
		source_put_u64(p->source_fence + 4, w.decision.holders[node].boot_incarnation);
		source_put_u64(p->source_fence + 12, 83);
		source_put_u64(p->source_fence + 20, 901);
		p->source_fence[28] = w.decision.directory_mode; p->source_fence[29] = 1;
		p->source_carrier_generation = w.decision.directory_mode == PCM_STATE_S
			&& frame.common.target_mode == PCM_STATE_S ? 901 : 902;
		p->requester_target_generation = frame.common.assertion_sequence;
		p->page_scn_lsn = 12340; p->dependency_count = 2;
		p->dependencies[0] = 19; p->dependencies[1] = 23;
		p->source_proof_crc32c = 0x12345678; p->page_checksum = 0x87456123;
		p->source_disposition = RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
		p->proof_kind = RESOURCE_X_PROOF_REMOTE_CARRIER;
		p->holder_connection_generation = 97; p->acting_formation = frame.common.resource_formation;
	}
	frame.body.acquire_witness = w;
	(void)source_round_trip(&frame); return frame;
}

static ResourceXApplyResult
source_apply(ResourceXDecodedFrame *frame, int node)
{
	ResourceXMasterSnapshot out;
	ResourceXApplyResult result = cluster_pcm_lock_resource_x_blocked_to_n_exact(frame, node, &out);
	UT_ASSERT_EQ(fake_lwlock_depth, 0); return result;
}

static void
source_unchanged(PublishFixture *f, ResourceXDecodedFrame *frame, int node, ResourceXApplyResult result)
{
	ClusterPcmResourceXMasterState *before = malloc(sizeof(*before));
	UT_ASSERT(before != NULL); if (!before) return;
	memcpy(before, f->state, sizeof(*before));
	UT_ASSERT_EQ(source_apply(frame, node), result);
	UT_ASSERT(memcmp(before, f->state, sizeof(*before)) == 0); free(before);
}

static void
source_record_matches(PublishFixture *f, ResourceXDecodedFrame *ack)
{
	ClusterPcmResourceXMasterRequest *r = &f->state->requests[1];
	const ResourceXDecodedBlockedToN *p = &ack->body.acquire_witness.original_proof;
	UT_ASSERT_EQ(r->proof_kind, p->proof_kind); UT_ASSERT_EQ(r->source_disposition, p->source_disposition);
	UT_ASSERT_EQ(r->source_carrier_generation, p->source_carrier_generation);
	UT_ASSERT_EQ(r->requester_target_generation, p->requester_target_generation);
	UT_ASSERT_EQ(r->page_scn_lsn, p->page_scn_lsn); UT_ASSERT_EQ(r->dependency_count, p->dependency_count);
	UT_ASSERT(memcmp(r->dependencies, p->dependencies, sizeof(r->dependencies)) == 0);
	UT_ASSERT(memcmp(r->source_fence, p->source_fence, sizeof(r->source_fence)) == 0);
	UT_ASSERT_EQ(r->source_proof_crc32c, p->source_proof_crc32c); UT_ASSERT_EQ(r->page_checksum, p->page_checksum);
	UT_ASSERT_EQ(r->holder_connection_generation, p->holder_connection_generation);
	UT_ASSERT_EQ(r->acting_formation, p->acting_formation); UT_ASSERT_EQ(r->proof_flags, p->proof_flags);
	UT_ASSERT_EQ(f->state->acquire_ack_connections[0], 61);
	UT_ASSERT_EQ(r->final_authority_generation, 0); UT_ASSERT_EQ(r->grant_transition_generation, 0);
}

UT_TEST(test_source_s_to_s_keeps_compatible_source_outside_blocker_mask)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	if (!source_start(2, PCM_STATE_S, &f)) return;
	ack = source_ack(&f, 0);
	UT_ASSERT_EQ(f.state->requests[1].incompatible_holders_bitmap, 0);
	UT_ASSERT_EQ(source_apply(&ack, 0), RESOURCE_X_APPLY_APPLIED);
	source_record_matches(&f, &ack);
	UT_ASSERT_EQ(f.state->requests[1].blocked_holders_bitmap, 0);
	UT_ASSERT_EQ(f.state->requests[1].source_carrier_generation, 901);
}

UT_TEST(test_source_x_to_s_keeps_downgrade_distinct_from_invalidation)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	if (!source_start(1, PCM_STATE_S, &f)) return;
	ack = source_ack(&f, 0);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_ack.target_mode, PCM_STATE_S);
	UT_ASSERT_EQ(source_apply(&ack, 0), RESOURCE_X_APPLY_APPLIED);
	source_record_matches(&f, &ack);
	UT_ASSERT_EQ(f.state->requests[1].blocked_holders_bitmap, 1);
	UT_ASSERT_EQ(f.state->requests[1].phase, RESOURCE_X_MASTER_WAIT_PROOF);
}

UT_TEST(test_source_x_to_n_records_original_proof_without_committing_grant)
{
	PublishFixture f; ResourceXDecodedFrame ack; PcmAuthoritySnapshot before, after;
	if (!source_start(1, PCM_STATE_X, &f)) return;
	ack = source_ack(&f, 0);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(make_tag(850), &before));
	UT_ASSERT_EQ(source_apply(&ack, 0), RESOURCE_X_APPLY_APPLIED);
	source_record_matches(&f, &ack);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(make_tag(850), &after));
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
}

UT_TEST(test_source_s_to_n_all_ack_permutations_keep_same_a_and_native_unfinished)
{
	const int order[6][3] = {{0,2,3},{0,3,2},{2,0,3},{2,3,0},{3,0,2},{3,2,0}};
	for (unsigned n = 0; n < lengthof(order); n++) {
		PublishFixture f; ClusterPcmResourceXGrantIntent grant; uint32 mask = 0;
		if (!source_start(2, PCM_STATE_X, &f)) return;
		grant = f.state->grant_intent;
		for (int i = 0; i < 3; i++) {
			ResourceXDecodedFrame ack = source_ack(&f, order[n][i]);
			UT_ASSERT_EQ(source_apply(&ack, order[n][i]), RESOURCE_X_APPLY_APPLIED);
			mask |= UINT32_C(1) << order[n][i];
			UT_ASSERT_EQ(f.state->requests[1].blocked_holders_bitmap, mask);
			UT_ASSERT_EQ(f.state->requests[1].phase, i == 2 ? RESOURCE_X_MASTER_WAIT_PROOF : RESOURCE_X_MASTER_WAIT_BLOCKERS);
			UT_ASSERT_EQ(f.state->requests[1].final_authority_generation, 0);
			UT_ASSERT(memcmp(&grant, &f.state->grant_intent, sizeof(grant)) == 0);
		}
	}
}

UT_TEST(test_source_all_four_profiles_replay_exactly_once)
{
	for (int scenario = 1; scenario <= 2; scenario++) for (PcmState target = PCM_STATE_S; target <= PCM_STATE_X; target++) {
		PublishFixture f; ResourceXDecodedFrame ack;
		if (!source_start(scenario, target, &f)) return;
		ack = source_ack(&f, 0);
		UT_ASSERT_EQ(source_apply(&ack, 0), RESOURCE_X_APPLY_APPLIED);
		source_unchanged(&f, &ack, 0, RESOURCE_X_APPLY_DUPLICATE);
	}
}

UT_TEST(test_source_outer_rebind_never_rebinds_original_ack)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	if (!source_start(1, PCM_STATE_S, &f)) return;
	ack = source_ack(&f, 0); UT_ASSERT_EQ(source_apply(&ack, 0), RESOURCE_X_APPLY_APPLIED);
	ack.common.sender_connection_generation++;
	if (!source_round_trip(&ack)) return;
	source_unchanged(&f, &ack, 0, RESOURCE_X_APPLY_DUPLICATE);
	ack.body.acquire_witness.original_ack.sender_connection_generation++;
	if (!source_round_trip(&ack)) return;
	source_unchanged(&f, &ack, 0, RESOURCE_X_APPLY_STALE);
}

UT_TEST(test_source_same_a_different_valid_original_proof_is_stale)
{
	for (int field = 0; field < 10; field++) {
		PublishFixture f; ResourceXDecodedFrame ack; ResourceXDecodedBlockedToN *p;
		if (!source_start(1, PCM_STATE_X, &f)) return;
		ack = source_ack(&f, 0); UT_ASSERT_EQ(source_apply(&ack, 0), RESOURCE_X_APPLY_APPLIED);
		p = &ack.body.acquire_witness.original_proof;
		switch (field) {
		case 0: p->page_scn_lsn++; break;
		case 1: p->dependencies[0]++; break;
		case 2: p->dependency_count = 1; p->dependencies[1] = 0; break;
		case 3: p->source_proof_crc32c++; break;
		case 4: p->page_checksum++; break;
		case 5: p->holder_connection_generation++; break;
		case 6: source_put_u64(p->source_fence + 12, 84); break;
		case 7: source_put_u64(p->source_fence + 20, 902); p->source_carrier_generation = 903; break;
		case 8: p->page_checksum = 0; break;
		case 9: p->page_scn_lsn = 0; break;
		}
		if (!source_round_trip(&ack)) return;
		source_unchanged(&f, &ack, 0, RESOURCE_X_APPLY_STALE);
	}
}

UT_TEST(test_source_compatible_s_keeps_generation_but_true_transitions_advance_it)
{
	for (int scenario = 1; scenario <= 2; scenario++) for (PcmState target = PCM_STATE_S; target <= PCM_STATE_X; target++) {
		PublishFixture f; ResourceXDecodedFrame ack;
		if (!source_start(scenario, target, &f)) return;
		ack = source_ack(&f, 0);
		ack.body.acquire_witness.original_proof.source_carrier_generation++;
		if (!source_round_trip(&ack)) return;
		source_unchanged(&f, &ack, 0, RESOURCE_X_APPLY_INVALID);
	}
}

UT_TEST(test_source_native_generation_exhaustion_and_invalid_fence_stay_closed)
{
	for (int field = 0; field < 7; field++) {
		PublishFixture f; ResourceXDecodedFrame ack; ResourceXDecodedBlockedToN *p;
		if (!source_start(1, PCM_STATE_X, &f)) return;
		ack = source_ack(&f, 0); p = &ack.body.acquire_witness.original_proof;
		switch (field) {
		case 0: source_put_u64(p->source_fence + 20, UINT64_MAX - 1); p->source_carrier_generation = UINT64_MAX - 1; break;
		case 1: source_put_u64(p->source_fence + 12, 0); break;
		case 2: source_put_u64(p->source_fence + 12, UINT64_MAX); break;
		case 3: p->source_fence[29] = 0; break;
		case 4: p->source_fence[30] = 1; break;
		case 5: p->source_fence[31] = 1; break;
		case 6: p->source_proof_crc32c = 0; break;
		}
		if (!source_round_trip(&ack)) return;
		source_unchanged(&f, &ack, 0, RESOURCE_X_APPLY_INVALID);
	}
}

UT_TEST(test_source_authentication_cannot_be_replaced_by_valid_crc)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	if (!source_start(1, PCM_STATE_S, &f)) return;
	ack = source_ack(&f, 0);
	source_unchanged(&f, &ack, 2, RESOURCE_X_APPLY_INVALID);
	source_unchanged(&f, &ack, -1, RESOURCE_X_APPLY_INVALID);
}

UT_TEST(test_source_invalid_local_boot_and_changed_grant_preserve_provenance)
{
	for (int field = 0; field < 2; field++) {
		PublishFixture f; ResourceXDecodedFrame ack;
		if (!source_start(1, PCM_STATE_S, &f)) return;
		ack = source_ack(&f, 0);
		if (field == 0) f.state->holder_grants[0].boot_incarnation++;
		else f.state->holder_grants[0].grant_generation++;
		/* The local master's boot must equal its session. Mutating it makes
		 * the retained grant shape invalid before the incoming-ACK check. */
		source_unchanged(&f, &ack, 0, field == 0 ? RESOURCE_X_APPLY_RECOVERY_BLOCKED : RESOURCE_X_APPLY_STALE);
	}
}

UT_TEST(test_source_full_episode_mismatch_cannot_consume_canonical)
{
	for (int field = 0; field < 4; field++) {
		PublishFixture f; ResourceXDecodedFrame ack;
		if (!source_start(1, PCM_STATE_S, &f)) return;
		ack = source_ack(&f, 0);
		switch (field) {
		case 0: ack.body.acquire_witness.decision.episode.reservation_token++; break;
		case 1: ack.body.acquire_witness.decision.episode.base_generation++; break;
		case 2: ack.body.acquire_witness.decision.source_service_generation++; break;
		case 3: ack.body.acquire_witness.decision.resource_binding++; break;
		}
		if (!source_round_trip(&ack)) return;
		source_unchanged(&f, &ack, 0, RESOURCE_X_APPLY_STALE);
	}
}

UT_TEST(test_source_session_change_preserves_old_evidence)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	if (!source_start(1, PCM_STATE_S, &f)) return;
	ack = source_ack(&f, 0); UT_ASSERT_EQ(source_apply(&ack, 0), RESOURCE_X_APPLY_APPLIED);
	ut_master_session++;
	source_unchanged(&f, &ack, 0, RESOURCE_X_APPLY_STALE);
}

UT_TEST(test_source_old_legacy_ack_cannot_shortcut_conditional_completion)
{
	PublishFixture f; ResourceXDecodedFrame ack; ResourceXDecodedBlockedToN proof;
	if (!source_start(1, PCM_STATE_X, &f)) return;
	ack = source_ack(&f, 0); proof = ack.body.acquire_witness.original_proof;
	ack.common = ack.body.acquire_witness.original_ack; ack.has_acquire_decision = false;
	ack.blocked_has_remote_proof = true; ack.body.blocked_to_n = proof;
	ack.payload_bytes = RESOURCE_X_PROOF_V1_BYTES;
	source_unchanged(&f, &ack, 0, RESOURCE_X_APPLY_BAD_STATE);
}

UT_TEST(test_source_missing_proof_and_non_source_proof_are_rejected)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	if (!source_start(2, PCM_STATE_X, &f)) return;
	ack = source_ack(&f, 0); ack.body.acquire_witness.has_source_proof = false;
	source_unchanged(&f, &ack, 0, RESOURCE_X_APPLY_INVALID);
	ack = source_ack(&f, 0); ack.common.action_node = 2;
	ack.body.acquire_witness.original_ack.action_node = 2;
	source_unchanged(&f, &ack, 2, RESOURCE_X_APPLY_INVALID);
}

UT_TEST(test_source_unverified_crc_does_not_publish_receipt)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	if (!source_start(1, PCM_STATE_X, &f)) return;
	ack = source_ack(&f, 0); ack.common.semantic_crc32c++;
	source_unchanged(&f, &ack, 0, RESOURCE_X_APPLY_INVALID);
}

UT_TEST(test_source_master_replay_and_transport_completion_do_not_clear_evidence)
{
	PublishFixture f; ResourceXDecodedFrame ack; ClusterPcmResourceXMasterState *before;
	ResourceXIntentSlot sent, staged;
	if (!source_start(1, PCM_STATE_S, &f)) return;
	ack = source_ack(&f, 0); UT_ASSERT_EQ(source_apply(&ack, 0), RESOURCE_X_APPLY_APPLIED);
	before = malloc(sizeof(*before)); UT_ASSERT(before != NULL); if (!before) return;
	memcpy(before, f.state, sizeof(*before));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&f.work, NULL), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(before, f.state, sizeof(*before)) == 0);
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&f.work);
	UT_ASSERT(memcmp(before, f.state, sizeof(*before)) == 0);
	/* An ACK can arrive before the sender's physical completion callback. */
	sent = f.state->block_intents[0].slot;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(&sent, 100001, &staged), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(&staged));
	UT_ASSERT_EQ(f.state->block_intents[0].slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	source_record_matches(&f, &ack);
	source_unchanged(&f, &ack, 0, RESOURCE_X_APPLY_DUPLICATE); free(before);
}

UT_TEST(test_source_complete_ack_set_does_not_invent_native_or_pi_retirement)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	if (!source_start(2, PCM_STATE_X, &f)) return;
	for (int n = 0; n < 4; n++) if (n != 1) {
		ack = source_ack(&f, n); UT_ASSERT_EQ(source_apply(&ack, n), RESOURCE_X_APPLY_APPLIED);
	}
	ack = source_ack(&f, 0); source_record_matches(&f, &ack);
	UT_ASSERT_EQ(f.state->requests[1].phase, RESOURCE_X_MASTER_WAIT_PROOF);
	UT_ASSERT_EQ(f.state->requests[1].local_proof_generation, 0);
	UT_ASSERT_EQ(f.state->requests[1].settlement_requester_connection_generation, 0);
}

UT_TEST(test_source_r8_keeps_sealed_proof_as_orphan_until_exact_termination)
{
	PublishFixture f; ResourceXDecodedFrame ack; ResourceXReconfigToken token;
	ResourceXReconfigBatch batch; ResourceXReconfigResult result = RESOURCE_X_RECONFIG_MORE;
	if (!source_start(1, PCM_STATE_S, &f)) return;
	ack = source_ack(&f, 0); UT_ASSERT_EQ(source_apply(&ack, 0), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	for (int scan = 0; scan < 8 && (result == RESOURCE_X_RECONFIG_MORE || result == RESOURCE_X_RECONFIG_RETRY); scan++)
		result = cluster_resource_x_reconfig_sweep(&token, 4, &batch);
	UT_ASSERT_EQ(result, RESOURCE_X_RECONFIG_ORPHAN);
	source_record_matches(&f, &ack);
}

UT_TEST(test_source_corrupt_retained_proof_or_receipt_never_accepts_replay)
{
	for (int field = 0; field < 4; field++) {
		PublishFixture f; ResourceXDecodedFrame ack;
		if (!source_start(1, PCM_STATE_S, &f)) return;
		ack = source_ack(&f, 0); UT_ASSERT_EQ(source_apply(&ack, 0), RESOURCE_X_APPLY_APPLIED);
		switch (field) {
		case 0: f.state->requests[1].source_carrier_generation++; break;
		case 1: f.state->requests[1].source_fence[30] = 1; break;
		case 2: f.state->acquire_ack_connections[0] = 0; break;
		case 3: f.state->requests[1].proof_kind = 0; break;
		}
		source_unchanged(&f, &ack, 0, RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	}
}

int
main(void)
{
	UT_PLAN(20);
	UT_RUN(test_source_s_to_s_keeps_compatible_source_outside_blocker_mask);
	UT_RUN(test_source_x_to_s_keeps_downgrade_distinct_from_invalidation);
	UT_RUN(test_source_x_to_n_records_original_proof_without_committing_grant);
	UT_RUN(test_source_s_to_n_all_ack_permutations_keep_same_a_and_native_unfinished);
	UT_RUN(test_source_all_four_profiles_replay_exactly_once);
	UT_RUN(test_source_outer_rebind_never_rebinds_original_ack);
	UT_RUN(test_source_same_a_different_valid_original_proof_is_stale);
	UT_RUN(test_source_compatible_s_keeps_generation_but_true_transitions_advance_it);
	UT_RUN(test_source_native_generation_exhaustion_and_invalid_fence_stay_closed);
	UT_RUN(test_source_authentication_cannot_be_replaced_by_valid_crc);
	UT_RUN(test_source_invalid_local_boot_and_changed_grant_preserve_provenance);
	UT_RUN(test_source_full_episode_mismatch_cannot_consume_canonical);
	UT_RUN(test_source_session_change_preserves_old_evidence);
	UT_RUN(test_source_old_legacy_ack_cannot_shortcut_conditional_completion);
	UT_RUN(test_source_missing_proof_and_non_source_proof_are_rejected);
	UT_RUN(test_source_unverified_crc_does_not_publish_receipt);
	UT_RUN(test_source_master_replay_and_transport_completion_do_not_clear_evidence);
	UT_RUN(test_source_complete_ack_set_does_not_invent_native_or_pi_retirement);
	UT_RUN(test_source_r8_keeps_sealed_proof_as_orphan_until_exact_termination);
	UT_RUN(test_source_corrupt_retained_proof_or_receipt_never_accepts_replay);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
