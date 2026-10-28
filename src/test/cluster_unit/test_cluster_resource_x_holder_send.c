/* Complete holder envelopes in the two original PCM send owners.
 *
 * The real PCM object, codec, scheduler and completion paths run. Only the
 * not-yet-connected conditional native publisher is substituted by explicit
 * typed input in a container found through a real old published descriptor.
 * This fixture does not qualify native service, authority or semantic C.
 */
int holder_send_pcm_fixture_main(void);
#define main holder_send_pcm_fixture_main
#include "test_cluster_pcm_lock.c"
#undef main
#include "test_cluster_resource_x_decision_owner_layout.inc"
#include "test_cluster_resource_x_transport_frames.inc"

typedef struct HolderSendFixture {
	ClusterPcmResourceXMasterState *state;
	ResourceXDecodedFrame status, image;
	ResourceXIntentSlot expected[2];
	uint8 bytes[2][RESOURCE_X_ACQUIRE_IMAGE_V3_BYTES];
	uint16 lengths[2];
} HolderSendFixture;

static ResourceXIntentSlot *
holder_send_slot(HolderSendFixture *f, int half)
{
	return half ? &f->state->holder_image_intent : &f->state->holder_status_intent.slot;
}

static uint8 *
holder_send_payload(HolderSendFixture *f, int half)
{
	return half ? f->state->holder_image.payload : f->state->holder_status_intent.payload;
}

static bool
holder_send_encode(HolderSendFixture *f, int half)
{
	ResourceXDecodedFrame *frame = half ? &f->image : &f->status;
	ResourceXWireReject reject;
	uint16 len = 0;
	bool ok = cluster_resource_x_wire_encode(frame->kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE
		? RESOURCE_X_MSG_IMAGE_OR_GRANT : RESOURCE_X_MSG_BLOCKED_TO_N, frame,
		f->bytes[half], sizeof(f->bytes[half]), &len, &reject);
	UT_ASSERT(ok);
	if (!ok) return false;
	f->lengths[half] = len;
	return true;
}

static bool
holder_send_start(int profile, HolderSendFixture *f)
{
	ResourceXDecodedFrame block, grant, image, status;
	ResourceXIntentSlot original;
	ResourceXIntentBodyHandle body = {0};
	uint8 old[RESOURCE_X_PROOF_V1_BYTES];
	uint8 raw[RESOURCE_X_ACQUIRE_IMAGE_V3_BYTES];
	unsigned char *memory = (unsigned char *)fake_pcm_header.data;
	Size matches = 0, location = 0;
	BufferTag tag;

	memset(f, 0, sizeof(*f));
	if (profile == 0) {
		f->status = make_witness_bytes(raw, false, PCM_STATE_S, PCM_STATE_X);
		f->image = f->status;
	} else {
		f->image = make_ship_bytes(raw, profile == 1 ? PCM_STATE_S : PCM_STATE_X,
			profile == 2 ? PCM_STATE_S : PCM_STATE_X, true);
		f->status = f->image;
		f->status.kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
		f->status.body.acquire_witness = f->image.body.acquire_image.source;
	}
	f->status.body.acquire_witness.decision.master_node = 0;
	if (profile == 0) f->image.body.acquire_witness.decision.master_node = 0;
	else f->image.body.acquire_image.source.decision.master_node = 0;
	f->status.common.sender_connection_generation = PGRAC_RESOURCE_X_RETAINED_SENDER_GENERATION;
	f->image.common.sender_connection_generation = PGRAC_RESOURCE_X_RETAINED_SENDER_GENERATION;
	if (!holder_send_encode(f, 0) || !holder_send_encode(f, 1)) return false;
	tag = f->status.common.logical_assertion.resource;

	/* Obtain a real published old pair before locating its original owner.
	 * The new native publisher is the explicit seam, not a success stub. */
	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	MyBackendType = B_LMS;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	fake_gcs_master_node = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17), RESOURCE_X_APPLY_APPLIED);
	block = make_resource_x_master_frame(RESOURCE_X_WIRE_BLOCK_TO_N, tag, 1, 0);
	block.common.observed_mode = PCM_STATE_X;
	block.common.target_mode = PCM_STATE_N;
	block.common.source_candidate = block.common.retain_pi_if_dirty = 1;
	make_resource_x_remote_join_pair(tag, 1, &grant, &image);
	status = make_resource_x_remote_blocked_frame(tag, 1, 0);
	status.body.blocked_to_n.source_proof_crc32c = image.common.semantic_crc32c;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_to_n_source_exact(&block, 1, &status, &image, NULL),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_pair_publish_exact(&block.common.logical_assertion,
		block.common.assertion_sequence, 1, block.common.master_session_incarnation), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
		&block.common.logical_assertion, &original, old, sizeof(old)), RESOURCE_X_APPLY_APPLIED);
	for (Size n = 0; n + sizeof(original) <= fake_pcm_header_requested_size; n += 8)
		if (memcmp(memory + n, &original, sizeof(original)) == 0) { matches++; location = n; }
	UT_ASSERT_EQ(matches, 1);
	if (matches != 1 || location < offsetof(ClusterPcmResourceXMasterState, holder_status_intent.slot)) return false;
	f->state = (ClusterPcmResourceXMasterState *)(memory + location
		- offsetof(ClusterPcmResourceXMasterState, holder_status_intent.slot));
	UT_ASSERT((unsigned char *)(f->state + 1) <= memory + fake_pcm_header_requested_size);
	/* These guards are required on the actual parent too: RED must not write
	 * beyond its old containers. Success below tests actual owner behavior. */
	UT_ASSERT(sizeof(f->state->holder_status.payload) >= f->lengths[0]);
	UT_ASSERT(sizeof(f->state->holder_status_intent.payload) >= f->lengths[0]);
	UT_ASSERT(sizeof(f->state->holder_image.payload) >= f->lengths[1]);
	if (sizeof(f->state->holder_status.payload) < f->lengths[0]
		|| sizeof(f->state->holder_status_intent.payload) < f->lengths[0]
		|| sizeof(f->state->holder_image.payload) < f->lengths[1]) return false;
	memset(&f->state->holder_status, 0, sizeof(f->state->holder_status));
	memset(&f->state->holder_status_intent, 0, sizeof(f->state->holder_status_intent));
	memset(&f->state->holder_image, 0, sizeof(f->state->holder_image));
	memset(&f->state->holder_image_intent, 0, sizeof(f->state->holder_image_intent));
	cluster_node_id = f->status.common.action_node;
	fake_gcs_master_node = 0;
	for (int half = 0; half < 2; half++) {
		ResourceXDecodedFrame *frame = half ? &f->image : &f->status;
		ResourceXIntentSlot *slot = holder_send_slot(f, half);
		uint32 destination = half ? frame->common.logical_assertion.requester_node : 0;
		body.assertion = frame->common.logical_assertion;
		body.owner_generation = frame->common.assertion_sequence;
		body.owner_node = cluster_node_id;
		body.owner_kind = half ? RESOURCE_X_INTENT_OWNER_HOLDER_IMAGE : RESOURCE_X_INTENT_OWNER_HOLDER_STATUS;
		memcpy(holder_send_payload(f, half), f->bytes[half], f->lengths[half]);
		if (half) {
			ClusterPcmResourceXHolderImage *r = &f->state->holder_image;
			r->body = body; r->logical_generation = body.owner_generation;
			r->authority_generation = frame->common.authority_generation;
			r->resource_formation = frame->common.resource_formation;
			r->destination_node = destination; r->payload_bytes = f->lengths[half];
			r->kind = frame->kind; r->valid = 2;
		} else {
			ClusterPcmResourceXHolderStatus *r = &f->state->holder_status;
			r->body = body; r->logical_generation = body.owner_generation;
			r->authority_generation = frame->common.authority_generation;
			r->resource_formation = frame->common.resource_formation;
			r->destination_node = destination; r->payload_bytes = f->lengths[half];
			r->kind = frame->kind; r->valid = 2;
			memcpy(r->payload, f->bytes[half], f->lengths[half]);
		}
		UT_ASSERT(cluster_pcm_lock_resource_x_intent_arm_exact(slot, &body, body.owner_generation,
			frame->common.authority_generation, 100000, destination, f->lengths[half], frame->kind));
		f->expected[half] = *slot;
	}
	fake_pcm_clock_us = 100001;
	return holder_send_slot(f, 0)->state == RESOURCE_X_INTENT_SLOT_ARMED
		&& holder_send_slot(f, 1)->state == RESOURCE_X_INTENT_SLOT_ARMED;
}

static void
holder_send_snapshot(HolderSendFixture *f, int half)
{
	uint8 output[RESOURCE_X_ACQUIRE_IMAGE_V3_BYTES];
	ResourceXIntentSlot slot;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(&f->expected[half],
		&slot, output, sizeof(output)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&slot, holder_send_slot(f, half), sizeof(slot)) == 0);
	UT_ASSERT(memcmp(output, f->bytes[half], f->lengths[half]) == 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
}

static void
holder_send_refused(HolderSendFixture *f)
{
	uint8 output[RESOURCE_X_ACQUIRE_IMAGE_V3_BYTES], before[sizeof(output)];
	ResourceXIntentSlot slot, live[2];
	for (int half = 0; half < 2; half++) live[half] = *holder_send_slot(f, half);
	memset(output, 0xa5, sizeof(output)); memcpy(before, output, sizeof(before));
	for (int half = 0; half < 2; half++) {
		UT_ASSERT_NE(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(&f->expected[half],
			&slot, output, sizeof(output)), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(output, before, sizeof(output)) == 0);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(&f->expected[half], 100001),
			RESOURCE_X_INTENT_STALE);
		UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_intent_complete_exact(&f->expected[half]));
		UT_ASSERT(memcmp(holder_send_slot(f, half), &live[half], sizeof(slot)) == 0);
	}
}

UT_TEST(test_holder_send_arm_full_profiles_stays_in_original_roles)
{
	const struct { uint16 bytes; uint8 kind, owner; } cases[] = {
		{832, RESOURCE_X_WIRE_BLOCKED_TO_N, RESOURCE_X_INTENT_OWNER_HOLDER_STATUS},
		{1048, RESOURCE_X_WIRE_BLOCKED_TO_N, RESOURCE_X_INTENT_OWNER_HOLDER_STATUS},
		{832, RESOURCE_X_WIRE_BLOCKED_TO_N, RESOURCE_X_INTENT_OWNER_HOLDER_IMAGE},
		{9800, RESOURCE_X_WIRE_IMAGE_ENVELOPE, RESOURCE_X_INTENT_OWNER_HOLDER_IMAGE}
	};
	ResourceXIntentBodyHandle body = {0};
	body.assertion.resource = make_tag(851); body.assertion.requester_node = 3;
	body.owner_node = 1; body.owner_generation = 41;
	for (unsigned n = 0; n < lengthof(cases); n++) {
		ResourceXIntentSlot slot = {0};
		body.owner_kind = cases[n].owner;
		UT_ASSERT(cluster_pcm_lock_resource_x_intent_arm_exact(&slot, &body, 41, 10001,
			100000, 3, cases[n].bytes, cases[n].kind));
		body.owner_kind = RESOURCE_X_INTENT_OWNER_HOLDER_RELEASE;
		memset(&slot, 0, sizeof(slot));
		UT_ASSERT(!cluster_pcm_lock_resource_x_intent_arm_exact(&slot, &body, 41, 10001,
			100000, 3, cases[n].bytes, cases[n].kind));
	}
}

UT_TEST(test_holder_send_full_pair_and_direct_witness_exact_bytes)
{
	for (int profile = 0; profile < 4; profile++) {
		HolderSendFixture f; ResourceXIntentSlot slot; uint8 output[9800];
		if (!holder_send_start(profile, &f)) continue;
		holder_send_snapshot(&f, 0); holder_send_snapshot(&f, 1);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
			&f.expected[0].body.assertion, &slot, output, f.lengths[0]), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(output, f.bytes[0], f.lengths[0]) == 0);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_intent_snapshot_exact(
			&f.expected[1].body.assertion, &slot, output, f.lengths[1]), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(output, f.bytes[1], f.lengths[1]) == 0);
		/* Master-local binding must not be mistaken for this holder's local binding. */
		UT_ASSERT_NE(f.status.body.acquire_witness.decision.resource_binding, f.state->binding_generation);
	}
}

UT_TEST(test_holder_send_short_output_keeps_bytes_and_responsibility)
{
	HolderSendFixture f;
	if (!holder_send_start(3, &f)) return;
	for (int half = 0; half < 2; half++) {
		uint8 output[9800], before[9800]; ResourceXIntentSlot slot;
		memset(output, 0x5a, sizeof(output)); memcpy(before, output, sizeof(before));
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(&f.expected[half],
			&slot, output, f.lengths[half] - 1), RESOURCE_X_APPLY_STALE);
		UT_ASSERT(memcmp(output, before, sizeof(output)) == 0);
		holder_send_snapshot(&f, half);
	}
}

UT_TEST(test_holder_send_ready_probe_uses_original_two_indices)
{
	for (int profile = 0; profile < 4; profile++) {
		HolderSendFixture f; ResourceXIntentSlot slot; uint32 cursor = 32, examined;
		uint8 output[9800];
		if (!holder_send_start(profile, &f)) continue;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_ready_intent_probe_exact(&f.expected[0].body.assertion.resource,
			&cursor, &slot), RESOURCE_X_INTENT_PROBE_FOUND);
		UT_ASSERT_EQ(cursor, 33); UT_ASSERT(memcmp(&slot, &f.expected[0], sizeof(slot)) == 0);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_ready_intent_probe_exact(&f.expected[0].body.assertion.resource,
			&cursor, &slot), RESOURCE_X_INTENT_PROBE_FOUND);
		UT_ASSERT_EQ(cursor, 35); UT_ASSERT(memcmp(&slot, &f.expected[1], sizeof(slot)) == 0);
		for (int half = 0; half < 2; half++) {
			ResourceXIntentProbeResult result = RESOURCE_X_INTENT_PROBE_IDLE;
			for (int n = 0; n < 32 && result != RESOURCE_X_INTENT_PROBE_FOUND; n++) {
				result = cluster_pcm_lock_resource_x_outbound_intent_probe_exact(1, &slot,
					output, sizeof(output), &examined);
				UT_ASSERT(examined <= 1);
			}
			UT_ASSERT_EQ(result, RESOURCE_X_INTENT_PROBE_FOUND);
			UT_ASSERT(memcmp(&slot, &f.expected[half], sizeof(slot)) == 0);
			UT_ASSERT(memcmp(output, f.bytes[half], f.lengths[half]) == 0);
		}
	}
}

UT_TEST(test_holder_send_not_admitted_preserves_both_original_frames)
{
	HolderSendFixture f;
	if (!holder_send_start(0, &f)) return;
	for (int half = 0; half < 2; half++) {
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(&f.expected[half], 100002),
			RESOURCE_X_INTENT_NOT_ADMITTED);
		holder_send_snapshot(&f, half);
		UT_ASSERT_EQ(holder_send_slot(&f, half)->state, RESOURCE_X_INTENT_SLOT_ARMED);
	}
}

UT_TEST(test_holder_send_one_physical_claim_per_half)
{
	HolderSendFixture f; ResourceXIntentSlot staged;
	if (!holder_send_start(3, &f)) return;
	for (int half = 0; half < 2; half++) {
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(&f.expected[half], 100001,
			&staged), RESOURCE_X_INTENT_STAGED);
		UT_ASSERT_EQ(staged.send_episode, 1);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(&f.expected[half], 100001),
			RESOURCE_X_INTENT_STALE);
	}
}

UT_TEST(test_holder_send_failed_tail_rearms_without_replacing_pair)
{
	HolderSendFixture f; ResourceXIntentSlot staged;
	if (!holder_send_start(2, &f)) return;
	for (int half = 0; half < 2; half++) {
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(&f.expected[half], 100001,
			&staged), RESOURCE_X_INTENT_STAGED);
		UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, false, 100002));
		UT_ASSERT_EQ(holder_send_slot(&f, half)->state, RESOURCE_X_INTENT_SLOT_ARMED);
		holder_send_snapshot(&f, half);
	}
}

UT_TEST(test_holder_send_success_is_independent_of_semantic_retirement)
{
	for (int first = 0; first < 2; first++) {
		HolderSendFixture f; ResourceXIntentSlot staged;
		ClusterPcmResourceXMasterState *before;
		if (!holder_send_start(first ? 0 : 3, &f)) continue;
		before = malloc(sizeof(*before)); UT_ASSERT(before != NULL); if (!before) return;
		memcpy(before, f.state, sizeof(*before));
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(&f.expected[first], 100001,
			&staged), RESOURCE_X_INTENT_STAGED);
		UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, 100002));
		UT_ASSERT_EQ(holder_send_slot(&f, first)->state, RESOURCE_X_INTENT_SLOT_EMPTY);
		holder_send_snapshot(&f, !first);
		before->holder_status_intent.slot = f.state->holder_status_intent.slot;
		before->holder_image_intent = f.state->holder_image_intent;
		UT_ASSERT(memcmp(before, f.state, sizeof(*before)) == 0);
		free(before);
	}
}

UT_TEST(test_holder_send_late_callback_cannot_close_successor_stage)
{
	HolderSendFixture f; ResourceXIntentSlot old, next;
	if (!holder_send_start(1, &f)) return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(&f.expected[1], 100001,
		&old), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&old, false, 100002));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(&f.expected[1], 600003,
		&next), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&old, true, 600004));
	UT_ASSERT(memcmp(holder_send_slot(&f, 1), &next, sizeof(next)) == 0);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&next, true, 600004));
}

UT_TEST(test_holder_send_valid_crc_cannot_change_one_half_of_a)
{
	for (int mutation = 0; mutation < 7; mutation++) {
		HolderSendFixture f; ResourceXDecodedAcquireDecision *a;
		if (!holder_send_start(3, &f)) continue;
		a = &f.status.body.acquire_witness.decision;
		switch (mutation) {
		case 0: a->resource_binding++; break;
		case 1: a->enqueue_order++; break;
		case 2: a->source_service_generation++; break;
		case 3: a->episode.base_generation++; break;
		case 4: a->episode.reservation_token++; break;
		case 5: a->episode.backend_generation++; break;
		case 6: a->requester_ingress_generation++; break;
		}
		if (!holder_send_encode(&f, 0)) continue;
		memcpy(f.state->holder_status.payload, f.bytes[0], f.lengths[0]);
		memcpy(f.state->holder_status_intent.payload, f.bytes[0], f.lengths[0]);
		holder_send_refused(&f);
	}
}

UT_TEST(test_holder_send_corrupt_or_unpublished_pair_is_never_offered)
{
	for (int mutation = 0; mutation < 5; mutation++) {
		HolderSendFixture f;
		if (!holder_send_start(3, &f)) continue;
		if (mutation == 0) f.state->holder_status.payload[4] ^= 1;
		if (mutation == 1) f.state->holder_status_intent.payload[4] ^= 1;
		if (mutation == 2) f.state->holder_image.payload[f.lengths[1] - 1] ^= 1;
		if (mutation == 3) f.state->holder_status.valid = 1;
		if (mutation == 4) f.state->holder_image.valid = 0;
		holder_send_refused(&f);
	}
}

UT_TEST(test_holder_send_record_identity_cannot_alias_other_original_owner)
{
	for (int mutation = 0; mutation < 10; mutation++) {
		HolderSendFixture f;
		if (!holder_send_start(3, &f)) continue;
		switch (mutation) {
		case 0: f.state->holder_status.body.owner_generation++; break;
		case 1: f.state->holder_image.body.owner_node++; break;
		case 2: f.state->holder_status.body.owner_index++; break;
		case 3: f.state->holder_status.body.reserved++; break;
		case 4: f.state->holder_image.destination_node = 0; break;
		case 5: f.state->holder_status.destination_node = 1; break;
		case 6: f.state->holder_image.authority_generation++; break;
		case 7: f.state->holder_status.resource_formation++; break;
		case 8: f.state->holder_image.payload_bytes--; break;
		case 9: f.state->holder_status.kind = RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2; break;
		}
		holder_send_refused(&f);
	}
}

UT_TEST(test_holder_send_callback_preserves_corrupt_staged_responsibility)
{
	HolderSendFixture f; ResourceXIntentSlot staged;
	if (!holder_send_start(3, &f)) return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(&f.expected[1], 100001,
		&staged), RESOURCE_X_INTENT_STAGED);
	f.state->holder_status.payload[4] ^= 1;
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, 100002));
	UT_ASSERT(memcmp(holder_send_slot(&f, 1), &staged, sizeof(staged)) == 0);
	f.state->holder_status.payload[4] ^= 1;
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, 100003));
}

UT_TEST(test_holder_send_legacy_pair_consumer_stays_fail_closed)
{
	HolderSendFixture f; ResourceXApplyResult result; ClusterPcmResourceXMasterState *before;
	if (!holder_send_start(3, &f)) return;
	before = malloc(sizeof(*before)); UT_ASSERT(before != NULL); if (!before) return;
	memcpy(before, f.state, sizeof(*before));
	result = cluster_pcm_lock_resource_x_holder_pair_publish_exact(&f.expected[0].body.assertion,
		f.expected[0].logical_generation, 0, f.status.common.master_session_incarnation);
	UT_ASSERT(result != RESOURCE_X_APPLY_APPLIED && result != RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(before, f.state, sizeof(*before)) == 0);
	free(before);
}

int
main(void)
{
	UT_PLAN(14);
	UT_RUN(test_holder_send_arm_full_profiles_stays_in_original_roles);
	UT_RUN(test_holder_send_full_pair_and_direct_witness_exact_bytes);
	UT_RUN(test_holder_send_short_output_keeps_bytes_and_responsibility);
	UT_RUN(test_holder_send_ready_probe_uses_original_two_indices);
	UT_RUN(test_holder_send_not_admitted_preserves_both_original_frames);
	UT_RUN(test_holder_send_one_physical_claim_per_half);
	UT_RUN(test_holder_send_failed_tail_rearms_without_replacing_pair);
	UT_RUN(test_holder_send_success_is_independent_of_semantic_retirement);
	UT_RUN(test_holder_send_late_callback_cannot_close_successor_stage);
	UT_RUN(test_holder_send_valid_crc_cannot_change_one_half_of_a);
	UT_RUN(test_holder_send_corrupt_or_unpublished_pair_is_never_offered);
	UT_RUN(test_holder_send_record_identity_cannot_alias_other_original_owner);
	UT_RUN(test_holder_send_callback_preserves_corrupt_staged_responsibility);
	UT_RUN(test_holder_send_legacy_pair_consumer_stays_fail_closed);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
