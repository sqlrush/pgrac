/* Full acquisition evidence through the original PCM requester owner and
 * real codec. Native BufferDesc/PGPROC completion is not supplied here. */
int requester_evidence_legacy_main(void);
#define main requester_evidence_legacy_main
#include "test_cluster_pcm_lock.c"
#undef main
#include "test_cluster_resource_x_transport_frames.inc"

typedef struct EvidenceFixture {
	ResourceXDecodedFrame request, legacy, decision, image, witness;
	ResourceXIntentSlot ticket;
} EvidenceFixture;

/* Explicit master-message input, before the real codec/join takes ownership. */
static uint64 evidence_disk_watermark_scn;

static bool
evidence_wire(ResourceXDecodedFrame *frame)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES];
	uint16 len;
	ResourceXWireReject reject;
	uint8 type = frame->kind == RESOURCE_X_WIRE_BLOCKED_TO_N
		? RESOURCE_X_MSG_BLOCKED_TO_N : frame->kind == RESOURCE_X_WIRE_BLOCK_TO_N
		? RESOURCE_X_MSG_BLOCK_TO_N : RESOURCE_X_MSG_IMAGE_OR_GRANT;
	return cluster_resource_x_wire_encode(type, frame, bytes, sizeof(bytes), &len, &reject)
		&& cluster_resource_x_wire_decode(type, bytes, len, frame, &reject);
}

static ResourceXDecodedFrame
evidence_witness(const ResourceXDecodedFrame *a, int node)
{
	ResourceXDecodedFrame frame = {0};
	frame.kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
	frame.payload_bytes = RESOURCE_X_WITNESS_V4_BYTES;
	frame.has_acquire_decision = true;
	frame.common = a->common;
	frame.common.action_node = node;
	frame.common.outcome = RESOURCE_X_OUTCOME_OK;
	frame.common.sender_connection_generation = 73;
	frame.body.acquire_witness.decision = a->body.acquire_decision;
	frame.body.acquire_witness.original_ack = frame.common;
	frame.body.acquire_witness.original_ack.authority_generation = a->common.base_authority_generation;
	frame.body.acquire_witness.original_ack.ordered_lane = 0;
	frame.body.acquire_witness.original_ack.observed_mode = a->body.acquire_decision.directory_mode;
	frame.body.acquire_witness.original_ack.target_mode = PCM_STATE_N;
	frame.body.acquire_witness.original_ack.sender_connection_generation = 83;
	UT_ASSERT(evidence_wire(&frame));
	return frame;
}

static bool
evidence_begin(EvidenceFixture *f, int profile)
{
	ResourceXDecodedAcquireDecision *a;
	PcmState observed = profile == 6 ? PCM_STATE_S : profile == 7 ? PCM_STATE_X : PCM_STATE_N;
	PcmState target = profile == 0 || profile == 2 || profile == 3 ? PCM_STATE_S : PCM_STATE_X;
	bool remote = profile >= 2 && profile <= 5;
	memset(f, 0, sizeof(*f));
	if (!episode_begin(&f->request, &f->legacy, &f->ticket, observed, target,
			observed == PCM_STATE_N ? RESOURCE_X_ACQUIRE_FIRST_FILL : RESOURCE_X_ACQUIRE_RESIDENT))
		return false;
	fake_gcs_master_node = 0;
	ut_master_session = 101; /* requester boot; its remote master session stays31 */
	fake_pcm_clock_us = 200;
	f->decision.kind = RESOURCE_X_WIRE_AUTHORITY_GRANT;
	f->decision.payload_bytes = RESOURCE_X_DECISION_V4_BYTES;
	f->decision.has_acquire_decision = true;
	f->decision.common = f->request.common;
	f->decision.common.base_authority_generation = 10000;
	f->decision.common.authority_generation = 10001;
	f->decision.common.sender_connection_generation = 61;
	a = &f->decision.body.acquire_decision;
	a->episode = f->request.body.acquire_intent;
	a->resource_binding = 40;
	a->enqueue_order = 41;
	a->master_node = 0;
	a->source_kind = remote ? RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT
		: profile >= 6 ? RESOURCE_X_ACQUIRE_SOURCE_LOCAL_CURRENT : RESOURCE_X_ACQUIRE_SOURCE_DISK;
	a->disk_watermark_scn = profile < 2 ? evidence_disk_watermark_scn : 0;
	a->source_node = remote ? 2 : profile >= 6 ? 1 : -1;
	a->source_service_generation = profile < 2 ? 0 : 55;
	a->directory_mode = profile == 3 || profile == 5 || profile == 7 ? PCM_STATE_X
		: profile < 2 ? PCM_STATE_N : PCM_STATE_S;
	a->holders_bitmap = profile < 2 ? 0 : remote ? (UINT32_C(1) << 2)
		: UINT32_C(1) << 1;
	if (profile == 4 || profile == 6)
		a->holders_bitmap |= UINT32_C(1) << 3;
	a->required_holders_bitmap = target == PCM_STATE_X ? a->holders_bitmap & ~(UINT32_C(1) << 1)
		: a->directory_mode == PCM_STATE_X ? a->holders_bitmap : 0;
	a->requester_ingress_generation = 61;
	a->master_ingress_generation = 51;
	for (int n = 0; n < RESOURCE_X_PROTOCOL_NODE_LIMIT; n++) {
		if ((a->holders_bitmap & (UINT32_C(1) << n)) == 0)
			continue;
		a->holders[n].boot_incarnation = n == 1 ? 101 : n == 0 ? 31 : (uint64)200 + n;
		a->holders[n].grant_generation = 50 + n;
	}
	UT_ASSERT(evidence_wire(&f->decision));
	if (profile == 4 || profile == 6)
		f->witness = evidence_witness(&f->decision, 3);
	if (remote) {
		uint8 bytes[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES];
		uint16 len;
		ResourceXWireReject reject;
		ResourceXDecodedFrame original = {0};
		ResourceXDecodedAcquireWitness *witness;
		ResourceXDecodedImageEnvelope *image;
		f->image = make_ship_bytes(bytes, (PcmState)a->directory_mode, target, false);
		f->image.common = f->decision.common;
		f->image.common.action_node = 2;
		f->image.common.outcome = RESOURCE_X_OUTCOME_OK;
		f->image.common.sender_connection_generation = 73;
		witness = &f->image.body.acquire_image.source;
		witness->decision = *a;
		witness->original_ack = f->image.common;
		witness->original_ack.authority_generation = f->decision.common.base_authority_generation;
		witness->original_ack.ordered_lane = 0;
		witness->original_ack.observed_mode = a->directory_mode;
		witness->original_ack.target_mode = target == PCM_STATE_S ? PCM_STATE_S : PCM_STATE_N;
		witness->original_ack.sender_connection_generation = 83;
		witness->original_ack.flags = a->directory_mode == PCM_STATE_S && target == PCM_STATE_S
			? 0 : RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED;
		test_put_u32_be(witness->original_proof.source_fence, 2);
		test_put_u64_be(witness->original_proof.source_fence + 4, a->holders[2].boot_incarnation);
		test_put_u64_be(witness->original_proof.source_fence + 12, 71);
		witness->original_proof.source_fence[28] = a->directory_mode;
		if (a->directory_mode == PCM_STATE_S && target == PCM_STATE_S)
			witness->original_proof.source_carrier_generation = 77; /* compatible S retains native generation */
		witness->original_proof.requester_target_generation = f->request.common.assertion_sequence;
		witness->original_proof.acting_formation = 17;
		image = &f->image.body.acquire_image.original_image;
		image->conversion_base_generation = f->decision.common.base_authority_generation;
		image->requester_target_generation = f->request.common.assertion_sequence;
		image->source_carrier_generation = witness->original_proof.source_carrier_generation;
		memcpy(image->source_fence, witness->original_proof.source_fence, sizeof(image->source_fence));
		test_put_u64_be(image->request_tail, 71);
		test_put_u64_be(image->request_tail + 8, f->request.common.assertion_sequence);
		f->image.body.acquire_image.original_common = f->decision.common;
		f->image.body.acquire_image.original_common.action_node = 1;
		f->image.body.acquire_image.original_common.observed_mode = a->directory_mode;
		f->image.body.acquire_image.original_common.sender_connection_generation = 71;
		f->image.body.acquire_image.original_common.flags = RESOURCE_X_COMMON_FLAG_AUTHORITY_WITH_IMAGE;
		f->image.body.acquire_image.original_common.outcome = RESOURCE_X_OUTCOME_OK;
		original.kind = RESOURCE_X_WIRE_IMAGE_ENVELOPE;
		original.common = f->image.body.acquire_image.original_common;
		original.common.target_mode = PCM_STATE_X; /* standalone V2 is X-only */
		original.body.image_envelope = *image;
		UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &original,
			bytes, sizeof(bytes), &len, &reject));
		bytes[73] = target;
		test_reseal(bytes, len);
		witness->original_proof.source_proof_crc32c = ship_get_u32(bytes + 4);
		UT_ASSERT(evidence_wire(&f->image));
	}
	return true;
}

static ResourceXApplyResult
evidence_offer_as(const ResourceXDecodedFrame *frame, int source, ResourceXRequesterJoinSnapshot *out)
{
	const ResourceXDecodedAcquireDecision *a = frame->kind == RESOURCE_X_WIRE_AUTHORITY_GRANT
		? &frame->body.acquire_decision : frame->kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE
		? &frame->body.acquire_image.source.decision : &frame->body.acquire_witness.decision;
	auth_peer_boot = source == 0 ? 31 : source == 1 ? 101 : a->holders[source].boot_incarnation;
	return cluster_pcm_lock_resource_x_requester_join_current_exact(frame, source,
		fake_rx_capability_generation, 0, 61, 77, out);
}

static ResourceXApplyResult
evidence_offer(const ResourceXDecodedFrame *frame, ResourceXRequesterJoinSnapshot *out)
{
	return evidence_offer_as(frame, frame->kind == RESOURCE_X_WIRE_AUTHORITY_GRANT
		? 0 : frame->common.action_node, out);
}

static bool
evidence_accept(const ResourceXDecodedFrame *frame, ResourceXRequesterJoinSnapshot *out)
{
	ResourceXApplyResult rc = evidence_offer(frame, out);
	UT_ASSERT_EQ(rc, RESOURCE_X_APPLY_APPLIED);
	if (rc != RESOURCE_X_APPLY_APPLIED)
		return false;
	UT_ASSERT(out->flags & RESOURCE_X_REQUESTER_JOIN_ACQUIRE);
	UT_ASSERT((out->flags & (RESOURCE_X_REQUESTER_JOIN_READY | RESOURCE_X_REQUESTER_JOIN_TERMINAL)) == 0);
	UT_ASSERT_EQ(out->t_install_us, 0);
	return true;
}

static void
evidence_profile(int profile, bool complete)
{
	EvidenceFixture f;
	ResourceXRequesterJoinSnapshot out;
	UT_ASSERT(evidence_begin(&f, profile));
	if (!evidence_accept(profile >= 2 && profile <= 5 ? &f.image : &f.decision, &out))
		return;
	UT_ASSERT_EQ((out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE) != 0, complete);
	UT_ASSERT_EQ(out.final_authority_generation, 10001);
	UT_ASSERT_EQ(out.requester_target_generation, 0); /* no legacy native ref */
}
UT_TEST(test_evidence_disk_s_is_a_source_decision_not_completed_io) { evidence_profile(0, true); }
UT_TEST(test_evidence_disk_x_does_not_activate_a_native_writer) { evidence_profile(1, true); }
UT_TEST(test_evidence_remote_s_shared_image_carries_its_conditional_a) { evidence_profile(2, true); }
UT_TEST(test_evidence_remote_x_downgrade_witness_is_part_of_the_image) { evidence_profile(3, true); }
UT_TEST(test_evidence_remote_x_current_image_is_not_a_native_c) { evidence_profile(5, true); }
UT_TEST(test_evidence_local_x_decision_still_needs_native_current_validation) { evidence_profile(7, true); }

UT_TEST(test_evidence_multi_s_image_and_other_holder_can_arrive_in_either_order)
{
	for (int order = 0; order < 2; order++) {
		EvidenceFixture f;
		ResourceXRequesterJoinSnapshot out;
		UT_ASSERT(evidence_begin(&f, 4));
		if (!evidence_accept(order ? &f.witness : &f.image, &out)) continue;
		UT_ASSERT((out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE) == 0);
		if (!evidence_accept(order ? &f.image : &f.witness, &out)) continue;
		UT_ASSERT(out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE);
	}
}

UT_TEST(test_evidence_local_current_waits_for_every_external_s_holder)
{
	EvidenceFixture f;
	ResourceXRequesterJoinSnapshot out;
	UT_ASSERT(evidence_begin(&f, 6));
	if (!evidence_accept(&f.decision, &out)) return;
	UT_ASSERT((out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE) == 0);
	if (!evidence_accept(&f.witness, &out)) return;
	UT_ASSERT(out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE);
	UT_ASSERT((out.flags & RESOURCE_X_REQUESTER_JOIN_HAS_IMAGE) == 0);
}

UT_TEST(test_evidence_replays_never_install_bump_or_replace_receipts)
{
	EvidenceFixture f;
	ResourceXRequesterJoinSnapshot before, after;
	UT_ASSERT(evidence_begin(&f, 4));
	if (!evidence_accept(&f.image, &before)) return;
	UT_ASSERT_EQ(evidence_offer(&f.image, &after), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
	if (!evidence_accept(&f.witness, &before)) return;
	UT_ASSERT_EQ(evidence_offer(&f.witness, &after), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
}

UT_TEST(test_evidence_changed_a_cannot_replace_the_original_anchor)
{
	EvidenceFixture f;
	ResourceXDecodedFrame changed;
	ResourceXRequesterJoinSnapshot out;
	UT_ASSERT(evidence_begin(&f, 4));
	if (!evidence_accept(&f.witness, &out)) return;
	changed = f.decision;
	changed.body.acquire_decision.resource_binding++;
	UT_ASSERT(evidence_wire(&changed));
	UT_ASSERT_EQ(evidence_offer(&changed, &out), RESOURCE_X_APPLY_STALE);
	if (!evidence_accept(&f.image, &out)) return;
	UT_ASSERT(out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE);
}

UT_TEST(test_evidence_every_native_and_process_field_remains_bound_to_e)
{
	for (int n = 0; n < 9; n++) {
		EvidenceFixture f;
		ResourceXDecodedAcquireIntent *e;
		ResourceXRequesterJoinSnapshot out;
		UT_ASSERT(evidence_begin(&f, 0));
		e = &f.decision.body.acquire_decision.episode;
		switch (n) {
		case 0: e->requester_incarnation++; break;
		case 1: e->backend_generation++; break;
		case 2: e->base_generation++; break;
		case 3: e->reservation_token++; break;
		case 4: e->cluster_epoch++; break;
		case 5: e->r4_generation++; break;
		case 6: e->backend_id++; break;
		case 7: e->buffer_id++; break;
		case 8: e->path = RESOURCE_X_ACQUIRE_RESIDENT; break;
		}
		UT_ASSERT(evidence_wire(&f.decision));
		UT_ASSERT_EQ(evidence_offer(&f.decision, &out), RESOURCE_X_APPLY_STALE);
	}
}

UT_TEST(test_evidence_a_master_packet_cannot_be_supplied_by_a_holder)
{
	EvidenceFixture f;
	ResourceXRequesterJoinSnapshot out;
	UT_ASSERT(evidence_begin(&f, 0));
	UT_ASSERT_EQ(evidence_offer_as(&f.decision, 2, &out), RESOURCE_X_APPLY_STALE);
	(void)evidence_accept(&f.decision, &out);
}

UT_TEST(test_evidence_image_must_arrive_from_the_designated_source)
{
	EvidenceFixture f;
	ResourceXRequesterJoinSnapshot out;
	UT_ASSERT(evidence_begin(&f, 4));
	UT_ASSERT_EQ(evidence_offer_as(&f.image, 3, &out), RESOURCE_X_APPLY_STALE);
	if (!evidence_accept(&f.witness, &out)) return;
	UT_ASSERT((out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE) == 0);
	(void)evidence_accept(&f.image, &out);
}

UT_TEST(test_evidence_observed_holder_boot_must_match_a)
{
	EvidenceFixture f;
	ResourceXRequesterJoinSnapshot out;
	UT_ASSERT(evidence_begin(&f, 4));
	auth_peer_boot = 999;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_current_exact(&f.witness, 3, 73,
		0, 61, 77, &out), RESOURCE_X_APPLY_STALE);
	(void)evidence_accept(&f.witness, &out);
}

UT_TEST(test_evidence_unknown_or_old_slot_observation_does_not_anchor_a)
{
	for (int n = 0; n < 3; n++) {
		EvidenceFixture f;
		ResourceXRequesterJoinSnapshot out;
		UT_ASSERT(evidence_begin(&f, 0));
		auth_slot_valid = n != 0;
		auth_epoch = n == 1 ? 18 : 17;
		auth_fresh = n != 2;
		UT_ASSERT_EQ(evidence_offer(&f.decision, &out), RESOURCE_X_APPLY_STALE);
	}
}

UT_TEST(test_evidence_changed_original_ack_identity_is_not_a_replay)
{
	EvidenceFixture f;
	ResourceXDecodedFrame changed;
	ResourceXRequesterJoinSnapshot out;
	UT_ASSERT(evidence_begin(&f, 4));
	if (!evidence_accept(&f.witness, &out)) return;
	changed = f.witness;
	changed.body.acquire_witness.original_ack.sender_connection_generation++;
	UT_ASSERT(evidence_wire(&changed));
	UT_ASSERT_EQ(evidence_offer(&changed, &out), RESOURCE_X_APPLY_STALE);
	if (!evidence_accept(&f.image, &out)) return;
	UT_ASSERT(out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE);
}

UT_TEST(test_evidence_fresh_outer_rebind_preserves_the_original_ack)
{
	EvidenceFixture f;
	ResourceXDecodedFrame changed;
	ResourceXRequesterJoinSnapshot before, after;
	UT_ASSERT(evidence_begin(&f, 4));
	if (!evidence_accept(&f.witness, &before)) return;
	changed = f.witness;
	changed.common.sender_connection_generation++;
	UT_ASSERT(evidence_wire(&changed));
	UT_ASSERT_EQ(evidence_offer(&changed, &after), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
}

UT_TEST(test_evidence_bad_crc_and_wrong_domain_do_not_consume_a_receipt)
{
	EvidenceFixture f;
	ResourceXDecodedFrame changed;
	ResourceXRequesterJoinSnapshot out;
	UT_ASSERT(evidence_begin(&f, 4));
	changed = f.witness;
	changed.common.semantic_crc32c ^= 1;
	UT_ASSERT_EQ(evidence_offer(&changed, &out), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_current_exact(&f.witness, 3, 73,
		0, 62, 77, &out), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_current_exact(&f.witness, 3, 73,
		0, 61, 78, &out), RESOURCE_X_APPLY_STALE);
	(void)evidence_accept(&f.witness, &out);
}

UT_TEST(test_evidence_all32_holder_positions_are_bounded_and_one_missing_blocks)
{
	EvidenceFixture f;
	ResourceXRequesterJoinSnapshot out;
	UT_ASSERT(evidence_begin(&f, 6));
	f.decision.body.acquire_decision.holders_bitmap = UINT32_MAX;
	f.decision.body.acquire_decision.required_holders_bitmap = UINT32_MAX & ~(UINT32_C(1) << 1);
	for (int n = 0; n < RESOURCE_X_PROTOCOL_NODE_LIMIT; n++) {
		f.decision.body.acquire_decision.holders[n].boot_incarnation = n == 1 ? 101 : n == 0 ? 31 : 200 + n;
		f.decision.body.acquire_decision.holders[n].grant_generation = 50 + n;
	}
	UT_ASSERT(evidence_wire(&f.decision));
	if (!evidence_accept(&f.decision, &out)) return;
	for (int n = 0; n < RESOURCE_X_PROTOCOL_NODE_LIMIT; n++) {
		ResourceXDecodedFrame ack;
		if (n == 1) continue;
		UT_ASSERT((out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE) == 0);
		ack = evidence_witness(&f.decision, n);
		if (!evidence_accept(&ack, &out)) return;
	}
	UT_ASSERT(out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE);
}

UT_TEST(test_evidence_valid_material_is_retained_protocol_debt)
{
	EvidenceFixture f;
	ResourceXRequesterJoinSnapshot out;
	PcmGrdProtocolDebtStats stats;
	UT_ASSERT(evidence_begin(&f, 4));
	if (!evidence_accept(&f.witness, &out)) return;
	cluster_pcm_grd_protocol_debt_snapshot(&stats);
	UT_ASSERT_EQ(stats.invalid_entry_count, 0);
	UT_ASSERT_EQ(stats.retained_entry_count, 1);
	UT_ASSERT_EQ(stats.active_resource_x_entry_count, 1);
}

UT_TEST(test_evidence_r8_cannot_clear_a_even_after_physical_completion)
{
	EvidenceFixture f;
	ResourceXRequesterJoinSnapshot out;
	ResourceXReconfigToken token;
	ResourceXReconfigBatch batch;
	ResourceXZeroResidualProof zero;
	UT_ASSERT(evidence_begin(&f, 4));
	if (!evidence_accept(&f.image, &out)) return;
	UT_ASSERT(cluster_pcm_lock_resource_x_requester_send_complete_exact(&f.ticket, true, 201));
	UT_ASSERT(cluster_resource_x_reconfig_freeze_exact(17, 18, 0, &token));
	UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch), RESOURCE_X_RECONFIG_ORPHAN);
	UT_ASSERT(!cluster_resource_x_reconfig_zero_proof_exact(&token, &zero));
}

UT_TEST(test_evidence_wire_ready_cannot_enter_the_legacy_native_ref)
{
	EvidenceFixture f;
	ResourceXRequesterJoinSnapshot out;
	ResourceXAcquisitionRef ref;
	ResourceXDecodedFrame grant, image;
	UT_ASSERT(evidence_begin(&f, 2));
	if (!evidence_accept(&f.image, &out)) return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_frames_exact(&f.request.common.logical_assertion,
		&grant, &image, &out), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(grant.has_acquire_decision && image.has_acquire_decision);
	ref.assertion = f.request.common.logical_assertion;
	ref.formation = 17;
	ref.acquisition_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(&ref), RESOURCE_X_APPLY_STALE);
}

UT_TEST(test_evidence_master_carrier_is_optional_and_all_arrival_orders_agree)
{
	static const int orders[6][3] = {{0,1,2}, {0,2,1}, {1,0,2}, {1,2,0}, {2,0,1}, {2,1,0}};
	for (int n = 0; n < 6; n++) {
		EvidenceFixture f;
		ResourceXRequesterJoinSnapshot out;
		bool have_image = false, have_other = false;
		UT_ASSERT(evidence_begin(&f, 4));
		for (int i = 0; i < 3; i++) {
			int item = orders[n][i];
			ResourceXApplyResult rc = evidence_offer(item == 0 ? &f.decision
				: item == 1 ? &f.image : &f.witness, &out);
			UT_ASSERT_EQ(rc, item == 0 && i != 0 ? RESOURCE_X_APPLY_DUPLICATE : RESOURCE_X_APPLY_APPLIED);
			have_image |= item == 1;
			have_other |= item == 2;
			UT_ASSERT_EQ((out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE) != 0,
				have_image && have_other);
			UT_ASSERT((out.flags & RESOURCE_X_REQUESTER_JOIN_READY) == 0);
		}
	}
}

UT_TEST(test_evidence_outer_image_rebind_does_not_rebind_its_original_source)
{
	EvidenceFixture f;
	ResourceXDecodedFrame changed;
	ResourceXRequesterJoinSnapshot before, after;
	UT_ASSERT(evidence_begin(&f, 2));
	if (!evidence_accept(&f.image, &before)) return;
	changed = f.image;
	changed.common.sender_connection_generation++;
	UT_ASSERT(evidence_wire(&changed));
	UT_ASSERT_EQ(evidence_offer(&changed, &after), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
	changed.body.acquire_image.source.original_ack.sender_connection_generation++;
	UT_ASSERT(evidence_wire(&changed));
	UT_ASSERT_EQ(evidence_offer(&changed, &after), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(evidence_offer(&f.image, &after), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
}

static void
evidence_observation_aba(int32 node pg_attribute_unused(), unsigned slot_read)
{
	if (slot_read == 2) auth_slot_generation++;
}

UT_TEST(test_evidence_same_boot_with_a_changed_observation_revision_is_not_stable)
{
	EvidenceFixture f;
	ResourceXRequesterJoinSnapshot out;
	ResourceXDecodedFrame grant, image;
	UT_ASSERT(evidence_begin(&f, 0));
	auth_observation_hook = evidence_observation_aba;
	UT_ASSERT_EQ(evidence_offer(&f.decision, &out), RESOURCE_X_APPLY_STALE);
	auth_observation_hook = NULL;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_frames_exact(
		&f.request.common.logical_assertion, &grant, &image, &out), RESOURCE_X_APPLY_NOT_FOUND);
	(void)evidence_accept(&f.decision, &out);
}

/* Execute the actual source artifact producer and original retained-image
 * reader, then switch the environment provider back to R. The native/page
 * inputs are explicit seams; this does not claim a source BufferDesc commit. */
UT_TEST(test_evidence_actual_source_keeps_h_to_r_independent_of_r_to_m)
{
	EvidenceFixture f;
	ResourceXDecodedFrame block, ship;
	ResourceXRequesterJoinSnapshot out;
	ClusterPcmOwnSnapshot prepared = {0};
	PGAlignedBlock page = {0};
	UT_ASSERT(evidence_begin(&f, 2));
	block = f.decision;
	block.kind = RESOURCE_X_WIRE_BLOCK_TO_N;
	block.common.action_node = 2;
	block.common.sender_connection_generation = 67;
	UT_ASSERT(evidence_wire(&block));
	prepared.tag = block.common.logical_assertion.resource;
	prepared.generation = 901;
	prepared.reservation_token = 77;
	prepared.flags = PCM_OWN_FLAG_REVOKING;
	prepared.pcm_state = PCM_STATE_S;
	prepared.buffer_type = BUF_TYPE_SCUR;
	prepared.semantic_buf_state = BM_VALID;
	((PageHeader)page.data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)page.data)->pd_upper = BLCKSZ;
	((PageHeader)page.data)->pd_special = BLCKSZ;
	((PageHeader)page.data)->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	cluster_node_id = 2;
	ut_master_session = 202;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(&block, 0, 7,
		&prepared, page.data, NULL, 83, 91), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_exact(&block.common.logical_assertion,
		&ship), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ship.body.acquire_image.source.decision.master_ingress_generation, 51);
	UT_ASSERT_EQ(ship.body.acquire_image.original_common.sender_connection_generation, 83);
	ship.common.sender_connection_generation = 73;
	UT_ASSERT(evidence_wire(&ship));
	cluster_node_id = 1;
	ut_master_session = 101;
	if (evidence_accept(&ship, &out))
		UT_ASSERT(out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE);
}

int
main(void)
{
	UT_PLAN(26);
	UT_RUN(test_evidence_disk_s_is_a_source_decision_not_completed_io);
	UT_RUN(test_evidence_disk_x_does_not_activate_a_native_writer);
	UT_RUN(test_evidence_remote_s_shared_image_carries_its_conditional_a);
	UT_RUN(test_evidence_remote_x_downgrade_witness_is_part_of_the_image);
	UT_RUN(test_evidence_remote_x_current_image_is_not_a_native_c);
	UT_RUN(test_evidence_local_x_decision_still_needs_native_current_validation);
	UT_RUN(test_evidence_multi_s_image_and_other_holder_can_arrive_in_either_order);
	UT_RUN(test_evidence_local_current_waits_for_every_external_s_holder);
	UT_RUN(test_evidence_replays_never_install_bump_or_replace_receipts);
	UT_RUN(test_evidence_changed_a_cannot_replace_the_original_anchor);
	UT_RUN(test_evidence_every_native_and_process_field_remains_bound_to_e);
	UT_RUN(test_evidence_a_master_packet_cannot_be_supplied_by_a_holder);
	UT_RUN(test_evidence_image_must_arrive_from_the_designated_source);
	UT_RUN(test_evidence_observed_holder_boot_must_match_a);
	UT_RUN(test_evidence_unknown_or_old_slot_observation_does_not_anchor_a);
	UT_RUN(test_evidence_changed_original_ack_identity_is_not_a_replay);
	UT_RUN(test_evidence_fresh_outer_rebind_preserves_the_original_ack);
	UT_RUN(test_evidence_bad_crc_and_wrong_domain_do_not_consume_a_receipt);
	UT_RUN(test_evidence_all32_holder_positions_are_bounded_and_one_missing_blocks);
	UT_RUN(test_evidence_valid_material_is_retained_protocol_debt);
	UT_RUN(test_evidence_r8_cannot_clear_a_even_after_physical_completion);
	UT_RUN(test_evidence_wire_ready_cannot_enter_the_legacy_native_ref);
	UT_RUN(test_evidence_master_carrier_is_optional_and_all_arrival_orders_agree);
	UT_RUN(test_evidence_outer_image_rebind_does_not_rebind_its_original_source);
	UT_RUN(test_evidence_same_boot_with_a_changed_observation_revision_is_not_stable);
	UT_RUN(test_evidence_actual_source_keeps_h_to_r_independent_of_r_to_m);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
