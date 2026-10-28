/*-------------------------------------------------------------------------
 * Complete acquisition frames through the original DATA transport owner.
 * The LMS object and wire codec are real. PCM/native authority and final IC
 * admission are explicit fixture seams, not holder-quiescence evidence.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_pcm_lock.h"

bool transport_legacy_encode(uint8, const ResourceXDecodedFrame *, void *, uint16,
	uint16 *, ResourceXWireReject *);
bool transport_legacy_decode(uint8, const void *, uint16, ResourceXDecodedFrame *, ResourceXWireReject *);
bool transport_legacy_rebind(uint8, void *, uint16, uint32, ResourceXWireReject *);
ClusterICSendResult transport_legacy_send(uint8, int32, const void *, uint32);
ClusterICSendResult transport_legacy_intent_send(uint8, int32, const void *, uint32,
	const ResourceXIntentSlot *);
ClusterICDispatchResult transport_legacy_dispatch(const ClusterICEnvelope *, const void *, int32);
int transport_legacy_fixture_main(void);
#define main transport_legacy_fixture_main
#define cluster_resource_x_wire_encode transport_legacy_encode
#define cluster_resource_x_wire_decode transport_legacy_decode
#define cluster_resource_x_wire_rebind_sender_generation transport_legacy_rebind
#define cluster_ic_send_envelope transport_legacy_send
#define cluster_ic_tier1_send_resource_x_intent transport_legacy_intent_send
#define cluster_ic_dispatch_envelope transport_legacy_dispatch
#include "test_cluster_lms_outbound.c"
#undef main
#undef cluster_resource_x_wire_encode
#undef cluster_resource_x_wire_decode
#undef cluster_resource_x_wire_rebind_sender_generation
#undef cluster_ic_send_envelope
#undef cluster_ic_tier1_send_resource_x_intent
#undef cluster_ic_dispatch_envelope
#include "storage/bufpage.h"
#include "test_cluster_resource_x_transport_frames.inc"

static uint8 transport_bytes[RESOURCE_X_ACQUIRE_IMAGE_V3_BYTES];
static uint32 transport_length;
static ResourceXIntentSlot transport_tail;
static bool transport_tail_owned;

static void
transport_capture(const void *bytes, uint32 length)
{
	UT_ASSERT(length <= sizeof(transport_bytes));
	if (length <= sizeof(transport_bytes)) {
		memcpy(transport_bytes, bytes, length);
		transport_length = length;
	}
}

ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 peer, const void *bytes, uint32 length)
{
	transport_capture(bytes, length);
	return transport_legacy_send(type, peer, bytes, length);
}

ClusterICSendResult
cluster_ic_tier1_send_resource_x_intent(uint8 type, int32 peer, const void *bytes,
	uint32 length, const ResourceXIntentSlot *intent)
{
	ClusterICSendResult rc;
	UT_ASSERT_EQ(intent->state, RESOURCE_X_INTENT_SLOT_STAGED);
	UT_ASSERT(intent->send_episode != 0);
	rc = cluster_ic_send_envelope(type, peer, bytes, length);
	if (rc == CLUSTER_IC_SEND_WOULD_BLOCK) {
		UT_ASSERT(!transport_tail_owned);
		transport_tail = *intent;
		transport_tail_owned = true;
	}
	return rc;
}

ClusterICDispatchResult
cluster_ic_dispatch_envelope(const ClusterICEnvelope *env, const void *bytes, int32 peer)
{
	transport_capture(bytes, env->payload_length);
	return transport_legacy_dispatch(env, bytes, peer);
}

/* No semantic/native producer is invented here: these are immutable typed
 * transport inputs. The PCM owner suite separately validates its containers. */
static ResourceXIntentSlot
transport_start(int profile)
{
	ResourceXDecodedFrame frame = {0};
	ResourceXDecodedAcquireDecision *a;
	ResourceXWireReject reject;
	ResourceXIntentSlot intent = {0};
	uint8 scratch[RESOURCE_X_ACQUIRE_IMAGE_V3_BYTES];
	uint16 len = 0;
	uint8 type;

	ut_reset_log();
	transport_length = 0;
	transport_tail_owned = false;
	memset(transport_bytes, 0, sizeof(transport_bytes));
	if (profile < 2) {
		make_decision_bytes(scratch, profile == 1,
			profile == 1 ? PCM_STATE_X : PCM_STATE_N, profile == 1 ? 2 : 0,
			profile == 1 ? PCM_STATE_X : PCM_STATE_S, profile == 1 ? 1 : -1,
			profile == 1 ? RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT : RESOURCE_X_ACQUIRE_SOURCE_DISK);
		type = profile == 1 ? RESOURCE_X_MSG_BLOCK_TO_N : RESOURCE_X_MSG_IMAGE_OR_GRANT;
		UT_ASSERT(cluster_resource_x_wire_decode(type, scratch, 736, &frame, &reject));
		a = &frame.body.acquire_decision;
		intent.body.owner_kind = profile == 1 ? RESOURCE_X_INTENT_OWNER_MASTER_BLOCK
			: RESOURCE_X_INTENT_OWNER_MASTER_GRANT;
		intent.body.owner_node = 0;
		intent.destination_node = frame.common.action_node;
		intent.body.owner_index = profile == 1 ? intent.destination_node : 0;
	} else {
		if (profile == 3 || profile == 4) {
			frame = make_ship_bytes(scratch, PCM_STATE_X, PCM_STATE_S, true);
			if (profile == 3) {
				ResourceXDecodedAcquireWitness source = frame.body.acquire_image.source;
				frame.kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
				frame.body.acquire_witness = source;
			}
		} else
			frame = make_witness_bytes(scratch, false, PCM_STATE_S, PCM_STATE_X);
		a = frame.kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE
			? &frame.body.acquire_image.source.decision : &frame.body.acquire_witness.decision;
		intent.body.owner_kind = profile < 4 ? RESOURCE_X_INTENT_OWNER_HOLDER_STATUS
			: RESOURCE_X_INTENT_OWNER_HOLDER_IMAGE;
		intent.body.owner_node = frame.common.action_node;
		intent.destination_node = profile < 4 ? 0 : frame.common.logical_assertion.requester_node;
		type = frame.kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE
			? RESOURCE_X_MSG_IMAGE_OR_GRANT : RESOURCE_X_MSG_BLOCKED_TO_N;
	}
	a->master_node = 0;
	frame.common.sender_connection_generation = 29;
	UT_ASSERT(cluster_resource_x_wire_encode(type, &frame, ut_resource_x_owner_payload,
		sizeof(ut_resource_x_owner_payload), &len, &reject));
	UT_ASSERT(len == (profile < 2 ? 736 : profile == 3 ? 1048 : profile == 4 ? 9800 : 832));
	intent.payload_bytes = len;
	intent.kind = frame.kind;
	intent.state = RESOURCE_X_INTENT_SLOT_ARMED;
	intent.body.assertion = frame.common.logical_assertion;
	intent.logical_generation = frame.common.assertion_sequence;
	intent.authority_generation = frame.common.authority_generation;
	intent.body.owner_generation = profile == 0 ? intent.authority_generation : intent.logical_generation;
	intent.first_armed_us = 43;
	ut_resource_x_owner_slot = intent;
	cluster_node_id = intent.body.owner_node;
	ut_peer_capabilities[intent.destination_node] = PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	ut_peer_cap_generation[intent.destination_node] = 77;
	ut_peer_rc[intent.destination_node] = CLUSTER_IC_SEND_DONE;
	return intent;
}

static bool
transport_enqueue(const ResourceXIntentSlot *intent)
{
	ClusterLmsEnqueueResult rc = cluster_lms_outbound_enqueue_resource_x_intent(
		0, intent, 77, UINT64_MAX);
	UT_ASSERT_EQ(rc, CLUSTER_LMS_ENQUEUE_ADMITTED);
	return rc == CLUSTER_LMS_ENQUEUE_ADMITTED;
}

static void
transport_check_bytes(const ResourceXIntentSlot *intent)
{
	ResourceXDecodedFrame frame;
	ResourceXWireReject reject;
	uint8 type = intent->kind == RESOURCE_X_WIRE_BLOCK_TO_N ? RESOURCE_X_MSG_BLOCK_TO_N
		: intent->kind == RESOURCE_X_WIRE_BLOCKED_TO_N ? RESOURCE_X_MSG_BLOCKED_TO_N
		: RESOURCE_X_MSG_IMAGE_OR_GRANT;
	UT_ASSERT_EQ(transport_length, intent->payload_bytes);
	UT_ASSERT(cluster_resource_x_wire_decode(type, transport_bytes, transport_length, &frame, &reject));
	UT_ASSERT_EQ(frame.common.sender_connection_generation, 77);
	for (unsigned i = 0; i < intent->payload_bytes; i++)
		if ((i < 4 || i >= 8) && (i < 76 || i >= 80))
			UT_ASSERT_EQ(transport_bytes[i], ut_resource_x_owner_payload[i]);
}

UT_TEST(test_full_transport_all_original_owner_profiles_send_exact_bytes)
{
	for (int p = 0; p < 6; p++) {
		ResourceXIntentSlot intent = transport_start(p);
		if (!transport_enqueue(&intent)) continue;
		UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
		UT_ASSERT_EQ(ut_resource_x_stage_count, 1);
		UT_ASSERT_EQ(ut_resource_x_complete_count, 1);
		UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
		UT_ASSERT_EQ(cluster_lms_outbound_resource_x_staged_count(), 0);
		transport_check_bytes(&intent);
	}
}

UT_TEST(test_full_transport_not_admitted_preserves_exact_semantic_owner)
{
	for (int p = 0; p < 6; p++) {
		ResourceXIntentSlot intent = transport_start(p);
		uint8 before[RESOURCE_X_ACQUIRE_IMAGE_V3_BYTES];
		memcpy(before, ut_resource_x_owner_payload, sizeof(before));
		if (!transport_enqueue(&intent)) continue;
		ut_peer_rc[intent.destination_node] = CLUSTER_IC_SEND_NOT_ADMITTED;
		UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 0);
		UT_ASSERT_EQ(ut_resource_x_owner_slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
		UT_ASSERT_EQ(ut_resource_x_rearm_count, 1);
		UT_ASSERT_EQ(ut_resource_x_complete_count, 0);
		UT_ASSERT_EQ(memcmp(before, ut_resource_x_owner_payload, sizeof(before)), 0);
		UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
		intent = ut_resource_x_owner_slot;
		ut_peer_rc[intent.destination_node] = CLUSTER_IC_SEND_DONE;
		if (!transport_enqueue(&intent)) continue;
		UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
		UT_ASSERT_EQ(ut_resource_x_complete_count, 1);
		transport_check_bytes(&intent);
	}
}

UT_TEST(test_full_transport_would_block_owns_one_tail_until_real_callback)
{
	for (int p = 0; p < 6; p++) {
		ResourceXIntentSlot intent = transport_start(p);
		if (!transport_enqueue(&intent)) continue;
		ut_peer_rc[intent.destination_node] = CLUSTER_IC_SEND_WOULD_BLOCK;
		UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
		UT_ASSERT(transport_tail_owned);
		UT_ASSERT_EQ(ut_resource_x_owner_slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
		UT_ASSERT_EQ(ut_resource_x_complete_count, 0);
		/* The ring has handed off its one copy; the real completion is
		 * still owed by the tail, not represented by another ring entry. */
		UT_ASSERT_EQ(cluster_lms_outbound_resource_x_staged_count(), 0);
		UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 0);
		UT_ASSERT_EQ(ut_sent_n, 1);
		transport_check_bytes(&intent);
		cluster_lms_outbound_resource_x_send_complete(&transport_tail, true);
		transport_tail_owned = false;
		UT_ASSERT_EQ(ut_resource_x_complete_count, 1);
		UT_ASSERT_EQ(cluster_lms_outbound_resource_x_staged_count(), 0);
	}
}

UT_TEST(test_full_transport_failed_tail_and_hard_error_keep_same_responsibility)
{
	for (int p = 0; p < 6; p++) {
		ResourceXIntentSlot intent = transport_start(p);
		if (!transport_enqueue(&intent)) continue;
		ut_peer_rc[intent.destination_node] = CLUSTER_IC_SEND_WOULD_BLOCK;
		UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
		cluster_lms_outbound_resource_x_send_complete(&transport_tail, false);
		transport_tail_owned = false;
		UT_ASSERT_EQ(ut_resource_x_owner_slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
		intent = ut_resource_x_owner_slot;
		if (!transport_enqueue(&intent)) continue;
		ut_peer_rc[intent.destination_node] = CLUSTER_IC_SEND_HARD_ERROR;
		UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 0);
		UT_ASSERT_EQ(ut_resource_x_owner_slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
		UT_ASSERT_EQ(ut_resource_x_complete_count, 0);
		UT_ASSERT_EQ(cluster_lms_outbound_resource_x_staged_count(), 0);
	}
}

UT_TEST(test_full_transport_reconnect_and_deadline_rearm_without_send)
{
	for (int p = 0; p < 6; p++)
		for (int drift = 0; drift < 3; drift++) {
			ResourceXIntentSlot intent = transport_start(p);
			ClusterLmsEnqueueResult rc = cluster_lms_outbound_enqueue_resource_x_intent(
				0, &intent, 77, drift == 2 ? 1 : UINT64_MAX);
			UT_ASSERT_EQ(rc, CLUSTER_LMS_ENQUEUE_ADMITTED);
			if (rc != CLUSTER_LMS_ENQUEUE_ADMITTED) continue;
			if (drift == 0) ut_peer_cap_generation[intent.destination_node]++;
			if (drift == 1) ut_peer_capabilities[intent.destination_node] = 0;
			UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 0);
			UT_ASSERT_EQ(ut_sent_n, 0);
			UT_ASSERT_EQ(ut_resource_x_owner_slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
			UT_ASSERT_EQ(ut_resource_x_complete_count, 0);
			UT_ASSERT_EQ(cluster_lms_outbound_resource_x_staged_count(), 0);
		}
}

UT_TEST(test_full_transport_full_ring_has_no_new_stage_or_overwrite)
{
	for (int p = 0; p < 6; p++) {
		ResourceXIntentSlot intent = transport_start(p);
		unsigned count = 0;
		while (count < 1024 && ut_enqueue_marker(0, UT_PEER_Y, 0x81)) count++;
		UT_ASSERT(count > 0 && count < 1024);
		UT_ASSERT_EQ(cluster_lms_outbound_enqueue_resource_x_intent(0, &intent, 77, UINT64_MAX),
			CLUSTER_LMS_ENQUEUE_FULL);
		UT_ASSERT_EQ(ut_resource_x_stage_count, 0);
		UT_ASSERT_EQ(ut_resource_x_owner_slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
		ut_peer_rc[UT_PEER_Y] = CLUSTER_IC_SEND_DONE;
		while (cluster_lms_outbound_depth(0)) (void)cluster_lms_outbound_drain_send(0);
		if (!transport_enqueue(&intent)) continue;
		UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
		transport_check_bytes(&intent);
	}
}

UT_TEST(test_full_transport_stale_stage_and_not_due_offer_no_ring_copy)
{
	for (int p = 0; p < 6; p++) {
		ResourceXIntentSlot intent = transport_start(p), stale = intent;
		ut_resource_x_stage_not_due = true;
		UT_ASSERT_EQ(cluster_lms_outbound_enqueue_resource_x_intent(0, &intent, 77, UINT64_MAX),
			CLUSTER_LMS_ENQUEUE_NOT_DUE);
		UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
		ut_resource_x_stage_not_due = false;
		stale.logical_generation++;
		stale.body.owner_generation = p == 0 ? stale.authority_generation : stale.logical_generation;
		UT_ASSERT_EQ(cluster_lms_outbound_enqueue_resource_x_intent(0, &stale, 77, UINT64_MAX),
			CLUSTER_LMS_ENQUEUE_INVALID);
		UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
		UT_ASSERT_EQ(ut_resource_x_stage_count, 0);
	}
}

UT_TEST(test_full_transport_malformed_frame_never_sends_or_completes)
{
	for (int p = 0; p < 6; p++) {
		ResourceXIntentSlot intent = transport_start(p);
		if (!transport_enqueue(&intent)) continue;
		ut_resource_x_owner_payload[intent.payload_bytes - 1] ^= 1;
		UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 0);
		UT_ASSERT_EQ(ut_sent_n, 0);
		UT_ASSERT_EQ(ut_resource_x_owner_slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
		UT_ASSERT_EQ(ut_resource_x_rearm_count, 1);
		UT_ASSERT_EQ(ut_resource_x_complete_count, 0);
	}
}

UT_TEST(test_full_transport_valid_frame_cannot_cross_original_owner_identity)
{
	for (int p = 0; p < 6; p++)
		for (int field = 0; field < 8; field++) {
			ResourceXIntentSlot intent = transport_start(p);
			ClusterLmsEnqueueResult rc;
			/* Mutate the supplied owner and its exact snapshot together, so
			 * this is the real frame/owner gate, not a stub STALE response. */
			switch (field) {
			case 0: intent.body.assertion.resource.blockNum++; break;
			case 1: intent.logical_generation++; break;
			case 2: intent.authority_generation++; break;
			case 3: intent.body.owner_node = 7; cluster_node_id = 7; break;
			case 4: intent.body.assertion.requester_node = 6; break;
			case 5: intent.destination_node = 6; break;
			case 6: intent.payload_bytes--; break;
			case 7: intent.body.owner_index = 9; break;
			}
			intent.body.owner_generation = p == 0 ? intent.authority_generation : intent.logical_generation;
			if (p == 1 && field == 5) intent.body.owner_index = intent.destination_node;
			if ((p == 0 || p >= 4) && field == 4) intent.destination_node = 6;
			if ((p == 0 || p >= 4) && field == 5) intent.body.assertion.requester_node = 6;
			ut_resource_x_owner_slot = intent;
			ut_peer_capabilities[intent.destination_node] = PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
			ut_peer_cap_generation[intent.destination_node] = 77;
			rc = cluster_lms_outbound_enqueue_resource_x_intent(
				0, &intent, 77, UINT64_MAX);
			if (rc == CLUSTER_LMS_ENQUEUE_ADMITTED) (void)cluster_lms_outbound_drain_send(0);
			UT_ASSERT_EQ(ut_sent_n, 0);
			UT_ASSERT_EQ(ut_resource_x_complete_count, 0);
			UT_ASSERT_EQ(ut_resource_x_owner_slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
		}
}

UT_TEST(test_full_transport_new_payload_cannot_use_unrelated_owner_or_kind)
{
	for (int p = 0; p < 6; p++)
		for (int field = 0; field < 4; field++) {
			ResourceXIntentSlot intent = transport_start(p);
			if (field == 0) intent.kind = RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2;
			if (field == 1) intent.body.owner_kind = RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT;
			if (field == 2) intent.body.owner_generation++;
			if (field == 3) intent.body.reserved = 1;
			UT_ASSERT_EQ(cluster_lms_outbound_enqueue_resource_x_intent(0, &intent, 77, UINT64_MAX),
				CLUSTER_LMS_ENQUEUE_INVALID);
			UT_ASSERT_EQ(ut_resource_x_stage_count, 0);
			UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
		}
}

UT_TEST(test_full_transport_original_pump_carries_the_largest_source_ship)
{
	for (int p = 0; p < 6; p++) {
		ResourceXIntentSlot intent = transport_start(p);
		int worker = cluster_lms_shard_for_tag(&intent.body.assertion.resource, cluster_lms_workers);
		ut_resource_x_probe_mode = RESOURCE_X_INTENT_PROBE_FOUND;
		UT_ASSERT_EQ(cluster_lms_outbound_resource_x_intent_pump(), 1);
		UT_ASSERT_EQ(cluster_lms_outbound_depth(worker), 1);
		UT_ASSERT_EQ(ut_resource_x_stage_count, 1);
		UT_ASSERT_EQ(cluster_lms_outbound_drain_send(worker), 1);
		transport_check_bytes(&intent);
	}
}

UT_TEST(test_full_transport_late_tail_callback_cannot_retire_next_stage)
{
	for (int p = 0; p < 6; p++) {
		ResourceXIntentSlot intent = transport_start(p), old, current;
		if (!transport_enqueue(&intent)) continue;
		ut_peer_rc[intent.destination_node] = CLUSTER_IC_SEND_WOULD_BLOCK;
		UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
		old = transport_tail;
		cluster_lms_outbound_resource_x_send_complete(&old, false);
		transport_tail_owned = false;
		intent = ut_resource_x_owner_slot;
		if (!transport_enqueue(&intent)) continue;
		UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
		current = ut_resource_x_owner_slot;
		UT_ASSERT(current.send_episode > old.send_episode);
		cluster_lms_outbound_resource_x_send_complete(&old, true);
		UT_ASSERT_EQ(memcmp(&current, &ut_resource_x_owner_slot, sizeof(current)), 0);
		UT_ASSERT_EQ(ut_resource_x_complete_count, 0);
		cluster_lms_outbound_resource_x_send_complete(&transport_tail, true);
		transport_tail_owned = false;
		UT_ASSERT_EQ(ut_resource_x_complete_count, 1);
	}
}

UT_TEST(test_full_transport_colocated_master_roles_use_original_loopback_owner)
{
	const int profiles[] = {0, 1, 3};
	for (unsigned p = 0; p < lengthof(profiles); p++) {
		ResourceXIntentSlot intent = transport_start(profiles[p]);
		ResourceXDecodedFrame frame;
		ResourceXDecodedAcquireDecision *a;
		ResourceXWireReject reject;
		uint16 len = 0;
		uint8 type = profiles[p] == 0 ? RESOURCE_X_MSG_IMAGE_OR_GRANT
			: profiles[p] == 1 ? RESOURCE_X_MSG_BLOCK_TO_N : RESOURCE_X_MSG_BLOCKED_TO_N;
		UT_ASSERT(cluster_resource_x_wire_decode(type, ut_resource_x_owner_payload,
			intent.payload_bytes, &frame, &reject));
		if (profiles[p] < 2) {
			a = &frame.body.acquire_decision;
			intent.body.owner_node = intent.destination_node;
		} else {
			a = &frame.body.acquire_witness.decision;
			intent.destination_node = intent.body.owner_node;
		}
		cluster_node_id = intent.body.owner_node;
		a->master_node = cluster_node_id;
		UT_ASSERT(cluster_resource_x_wire_encode(type, &frame, ut_resource_x_owner_payload,
			sizeof(ut_resource_x_owner_payload), &len, &reject));
		UT_ASSERT_EQ(len, intent.payload_bytes);
		ut_resource_x_owner_slot = intent;
		ut_peer_capabilities[intent.destination_node] = PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
		ut_peer_cap_generation[intent.destination_node] = 77;
		if (!transport_enqueue(&intent)) continue;
		ut_local_dispatch_pending = true;
		UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 0);
		UT_ASSERT_EQ(ut_sent_n, 0);
		UT_ASSERT_EQ(ut_resource_x_owner_slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
		UT_ASSERT_EQ(ut_resource_x_complete_count, 0);
		intent = ut_resource_x_owner_slot;
		ut_local_dispatch_pending = false;
		if (!transport_enqueue(&intent)) continue;
		UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
		UT_ASSERT_EQ(ut_sent_n, 0);
		UT_ASSERT_EQ(ut_local_dispatch_count, 1);
		UT_ASSERT_EQ(ut_resource_x_complete_count, 1);
		transport_check_bytes(&intent);
	}
}


/* Actual LMS/codec REQUEST transport. PCM lifetime is an explicit provider
 * here and is tested separately against the real PCM owner. */
static ResourceXDecodedFrame
transport_complete_request(void)
{
	ResourceXDecodedFrame frame = {0};
	ResourceXDecodedAcquireIntent e;
	ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_DECISION_V3_BYTES]; uint16 len;
	ut_captured_region->init_fn(); ut_reset_log();
	transport_length = 0; transport_tail_owned = false; cluster_node_id = 0;
	make_decision_bytes(bytes, false, PCM_STATE_N, 0, PCM_STATE_S, -1, RESOURCE_X_ACQUIRE_SOURCE_DISK);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, 736, &frame, &reject));
	e = frame.body.acquire_decision.episode;
	frame.kind = RESOURCE_X_WIRE_ASSERT_X; frame.payload_bytes = 160;
	frame.has_acquire_decision = false; frame.has_acquire_intent = true;
	frame.common.logical_assertion.requester_node = 0; frame.common.action_node = 0;
	frame.common.base_authority_generation = frame.common.authority_generation = 0;
	frame.common.ordered_lane = 0; frame.common.outcome = RESOURCE_X_OUTCOME_NONE;
	frame.common.flags = frame.common.source_candidate = frame.common.retain_pi_if_dirty = 0;
	frame.common.observed_mode = PCM_STATE_N; frame.common.target_mode = PCM_STATE_S;
	frame.common.sender_connection_generation = 77;
	memset(&frame.body, 0, sizeof(frame.body)); frame.body.acquire_intent = e;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, &frame, bytes, sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(len, 160);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X, bytes, len, &frame, &reject));
	ut_peer_capabilities[UT_PEER_X] = PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	ut_peer_cap_generation[UT_PEER_X] = 77;
	return frame;
}

static void
transport_complete_check(const ResourceXDecodedFrame *request)
{
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject;
	uint8 bytes[160];
	uint16 len;
	UT_ASSERT_EQ(transport_length, 160);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X, transport_bytes, transport_length, &decoded, &reject));
	UT_ASSERT(decoded.has_acquire_intent);
	UT_ASSERT(!decoded.has_acquire_decision);
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, request, bytes, sizeof(bytes), &len, &reject));
	UT_ASSERT(memcmp(bytes, transport_bytes, 160) == 0);
}

UT_TEST(test_complete_request_transport_encodes_single_full_intent_without_local_proof)
{
	ResourceXDecodedFrame request = transport_complete_request();
	ClusterLmsEnqueueResult rc = cluster_lms_outbound_enqueue_resource_x_requester(UT_PEER_X, &request, NULL);
	UT_ASSERT_EQ(rc, CLUSTER_LMS_ENQUEUE_ADMITTED);
	if (rc != CLUSTER_LMS_ENQUEUE_ADMITTED) return;
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
	UT_ASSERT_EQ(ut_requester_pending, 0);
	UT_ASSERT_EQ(ut_requester_completed, 1);
	transport_complete_check(&request);
}

UT_TEST(test_complete_request_transport_not_admitted_retains_original_fifo_and_pending)
{
	ResourceXDecodedFrame request = transport_complete_request();
	ClusterLmsEnqueueResult rc = cluster_lms_outbound_enqueue_resource_x_requester(UT_PEER_X, &request, NULL);
	UT_ASSERT_EQ(rc, CLUSTER_LMS_ENQUEUE_ADMITTED);
	if (rc != CLUSTER_LMS_ENQUEUE_ADMITTED) return;
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_NOT_ADMITTED;
	for (int i = 0; i < 2; i++) {
		UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 0);
		UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 1);
		UT_ASSERT_EQ(ut_requester_pending, 1);
		UT_ASSERT_EQ(ut_requester_completed, 0);
		transport_complete_check(&request);
	}
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
	UT_ASSERT_EQ(ut_requester_pending, 0);
	UT_ASSERT_EQ(ut_requester_completed, 1);
	transport_complete_check(&request);
}

UT_TEST(test_complete_request_transport_real_tail_owns_exact_160_bytes_until_callback)
{
	ResourceXDecodedFrame request = transport_complete_request();
	ClusterLmsEnqueueResult rc = cluster_lms_outbound_enqueue_resource_x_requester(UT_PEER_X, &request, NULL);
	UT_ASSERT_EQ(rc, CLUSTER_LMS_ENQUEUE_ADMITTED);
	if (rc != CLUSTER_LMS_ENQUEUE_ADMITTED) return;
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_WOULD_BLOCK;
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
	UT_ASSERT(transport_tail_owned);
	UT_ASSERT_EQ(transport_tail.payload_bytes, 160);
	UT_ASSERT_EQ(ut_requester_pending, 1);
	UT_ASSERT_EQ(ut_requester_completed, 0);
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 0);
	cluster_lms_outbound_resource_x_send_complete(&transport_tail, true);
	transport_tail_owned = false;
	UT_ASSERT_EQ(ut_requester_pending, 0);
	UT_ASSERT_EQ(ut_requester_completed, 1);
	transport_complete_check(&request);
}

UT_TEST(test_complete_request_transport_full_fifo_does_not_publish_episode)
{
	ResourceXDecodedFrame request = transport_complete_request();
	for (int i = 0; i < 256; i++) UT_ASSERT(ut_enqueue_marker(0, UT_PEER_X, 0x72));
	UT_ASSERT_EQ(cluster_lms_outbound_enqueue_resource_x_requester(UT_PEER_X, &request, NULL), CLUSTER_LMS_ENQUEUE_FULL);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 256);
	UT_ASSERT_EQ(ut_requester_begins, 0);
	UT_ASSERT_EQ(ut_requester_pending, 0);
}

UT_TEST(test_complete_request_transport_rejects_extra_local_proof_before_owner)
{
	ResourceXDecodedFrame request = transport_complete_request(), proof = request;
	proof.has_acquire_intent = false; proof.kind = RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION;
	UT_ASSERT_EQ(cluster_lms_outbound_enqueue_resource_x_requester(UT_PEER_X, &request, &proof), CLUSTER_LMS_ENQUEUE_INVALID);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
	UT_ASSERT_EQ(ut_requester_begins, 0);
}

int
main(void)
{
	UT_PLAN(19);
	UT_RUN(test_ring_shmem_init);
	UT_RUN(test_full_transport_all_original_owner_profiles_send_exact_bytes);
	UT_RUN(test_full_transport_not_admitted_preserves_exact_semantic_owner);
	UT_RUN(test_full_transport_would_block_owns_one_tail_until_real_callback);
	UT_RUN(test_full_transport_failed_tail_and_hard_error_keep_same_responsibility);
	UT_RUN(test_full_transport_reconnect_and_deadline_rearm_without_send);
	UT_RUN(test_full_transport_full_ring_has_no_new_stage_or_overwrite);
	UT_RUN(test_full_transport_stale_stage_and_not_due_offer_no_ring_copy);
	UT_RUN(test_full_transport_malformed_frame_never_sends_or_completes);
	UT_RUN(test_full_transport_valid_frame_cannot_cross_original_owner_identity);
	UT_RUN(test_full_transport_new_payload_cannot_use_unrelated_owner_or_kind);
	UT_RUN(test_full_transport_original_pump_carries_the_largest_source_ship);
	UT_RUN(test_full_transport_late_tail_callback_cannot_retire_next_stage);
	UT_RUN(test_full_transport_colocated_master_roles_use_original_loopback_owner);
	UT_RUN(test_complete_request_transport_encodes_single_full_intent_without_local_proof);
	UT_RUN(test_complete_request_transport_not_admitted_retains_original_fifo_and_pending);
	UT_RUN(test_complete_request_transport_real_tail_owns_exact_160_bytes_until_callback);
	UT_RUN(test_complete_request_transport_full_fifo_does_not_publish_episode);
	UT_RUN(test_complete_request_transport_rejects_extra_local_proof_before_owner);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
