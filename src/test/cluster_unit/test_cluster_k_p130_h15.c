/* Native holder publication through the original outbound ring and drain.
 * Socket admission, local dispatch and OS locks are controlled host leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_k_p130_h15_native.inc"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_membership.h"

static unsigned h15_calls[2], h15_taken[2], h15_fill_calls, h15_wakes;
static ClusterICSendResult h15_rc;
static uint8 h15_bytes[2][RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES];
static uint32 h15_length[2];
static uint32 h15_crc[2];
static ResourceXIntentSlot h15_tail;
static bool h15_tail_owned;

uint64 cluster_membership_cut_generation(void) { return 2; }
bool cluster_membership_cut_generation_current(uint64 cut) { return cut == 2; }
ClusterMembershipState cluster_membership_get_state(int32 node) { return CLUSTER_MEMBER_MEMBER; }
uint64 cluster_membership_get_last_admitted_incarnation(int32 node) { return 31; }
bool cluster_sf_peer_capability_generation_matches(int32 peer, uint32 caps, uint32 generation)
{
	return peer == 1 && generation == 83 && caps == PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
}
static bool h15_peer_sample(int32 peer, uint32 caps, uint32 *out, uint32 *generation)
{
	UT_ASSERT_EQ(peer, 1); *out = caps; *generation = 83; return true;
}
void cluster_lms_obs_note_outbound_not_admitted(int worker) { UT_ASSERT_EQ(worker, 1); }
void cluster_lms_obs_note_outbound_requeue_drop(int worker) { UT_ASSERT(false); }
void cluster_lms_obs_note_outbound_cap_guard_drop(int worker) { UT_ASSERT(false); }
void cluster_gcs_block_lmon_prepare_outbound_request(GcsBlockRequestPayload *request, int32 peer) { UT_ASSERT(false); }
void cluster_gcs_block_note_send_outcome(GcsBlockSendFamily family, ClusterICSendResult rc) { UT_ASSERT(false); }
ClusterICSendResult cluster_gcs_block_send_direct_zero_reply(int32 peer, const GcsBlockReplyHeader *header)
{ UT_ASSERT(false); return CLUSTER_IC_SEND_HARD_ERROR; }

bool cluster_ic_envelope_build(ClusterICEnvelope *env, uint8 type, uint32 source, uint32 dest,
	const void *data, uint32 length)
{
	memset(env, 0, sizeof(*env)); env->msg_type = type; env->source_node_id = source;
	env->dest_node_id = dest; env->payload_length = length; return true;
}
static ClusterICSendResult h15_send(uint8 type, int32 peer, const void *data, uint32 length)
{
	int half = type == RESOURCE_X_MSG_BLOCKED_TO_N ? 0 : 1;
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	if (type == PGRAC_IC_MSG_SCN_BROADCAST) { h15_fill_calls++; return CLUSTER_IC_SEND_DONE; }
	UT_ASSERT(peer == 0 || peer == 1);
	UT_ASSERT(length <= sizeof(h15_bytes[half]));
	if (length > sizeof(h15_bytes[half])) return CLUSTER_IC_SEND_HARD_ERROR;
	memcpy(h15_bytes[half], data, length); h15_length[half] = length; h15_calls[half]++;
	if (h15_rc != CLUSTER_IC_SEND_NOT_ADMITTED) h15_taken[half]++;
	return half == 0 && h15_rc == CLUSTER_IC_SEND_WOULD_BLOCK ? CLUSTER_IC_SEND_DONE : h15_rc;
}
ClusterICSendResult cluster_ic_send_envelope(uint8 type, int32 peer, const void *data, uint32 length)
{ return h15_send(type, peer, data, length); }
ClusterICDispatchResult cluster_ic_dispatch_envelope(const ClusterICEnvelope *env, const void *data, int32 peer)
{
	ClusterICSendResult rc = h15_send(env->msg_type, peer, data, env->payload_length);
	return rc == CLUSTER_IC_SEND_NOT_ADMITTED ? CLUSTER_IC_DISPATCH_PENDING : CLUSTER_IC_DISPATCH_DONE;
}
ClusterICSendResult cluster_ic_tier1_send_resource_x_intent(uint8 type, int32 peer,
	const void *data, uint32 length, const ResourceXIntentSlot *intent)
{
	ClusterICSendResult rc = h15_send(type, peer, data, length);
	UT_ASSERT_EQ(intent->state, RESOURCE_X_INTENT_SLOT_STAGED);
	if (rc == CLUSTER_IC_SEND_WOULD_BLOCK) {
		UT_ASSERT(!h15_tail_owned); h15_tail = *intent; h15_tail_owned = true;
	}
	return rc;
}
static void h15_cv_wake(ConditionVariable *cv)
{
	UT_ASSERT(cv == &request_shared.rings[1].space_cv); h15_wakes++;
	ConditionVariableBroadcast(cv);
}
ClusterLmsEnqueueResult h15_try_enqueue(int worker, uint8 type, uint32 peer, const void *data, uint16 length);
int h15_drain(int worker);
#define cluster_lms_outbound_try_enqueue h15_try_enqueue
#define cluster_lms_outbound_drain_send h15_drain
#define ConditionVariableBroadcast h15_cv_wake
#include "test_cluster_k_p130_h15_drain.inc"
#undef ConditionVariableBroadcast
#undef cluster_lms_outbound_drain_send
#undef cluster_lms_outbound_try_enqueue
#undef gcs_block_pcm_x_resource_x_peer_ready_exact
#define gcs_block_pcm_x_resource_x_peer_ready_exact h15_peer_ready
#define gcs_block_resource_x_stage_ready_tag h15_stage_ready
#define cluster_sf_peer_capability_word_sample h15_peer_sample
#define cluster_lms_outbound_enqueue_resource_x_intent done_actual_lms_enqueue
#include "test_cluster_k_p130_h15_ready.inc"
#undef cluster_lms_outbound_enqueue_resource_x_intent
#undef cluster_sf_peer_capability_word_sample
#undef gcs_block_resource_x_stage_ready_tag
#undef gcs_block_pcm_x_resource_x_peer_ready_exact

static void h15_pair(HolderFixture *f, const ClusterPcmResourceXHolderStatus *status,
	const ClusterPcmResourceXHolderImage *image, const ClusterPcmLocalPiSnapshotV1 *pi,
	const ClusterPcmResourceXMasterRequest *request)
{
	ClusterPcmLocalPiSnapshotV1 live;
	ClusterPcmLocalPiFloorV1 floor;
	UT_ASSERT_EQ(memcmp(status, &f->master.state->holder_status, sizeof(*status)), 0);
	UT_ASSERT_EQ(memcmp(image, &f->master.state->holder_image, sizeof(*image)), 0);
	UT_ASSERT_EQ(memcmp(request, &f->master.state->requests[1], sizeof(*request)), 0);
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(request_buf.tag, &live));
	UT_ASSERT_EQ(memcmp(pi, &live, sizeof(live)), 0);
	UT_ASSERT(cluster_pcm_local_pi_floor_v1(&pi->last.source, &floor));
	UT_ASSERT_EQ(floor.bounded, 2); UT_ASSERT_EQ(floor.floor, 100);
	UT_ASSERT_EQ(f->master.state->pi_carrier.pi_refs, 1);
	UT_ASSERT_EQ(f->master.state->pi_carrier.transport_refs, 1);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 902);
}
static void h15_check_bytes(HolderFixture *f, int half)
{
	uint16 length = half ? f->master.state->holder_image.payload_bytes : f->master.state->holder_status.payload_bytes;
	uint8 expected[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES];
	const uint8 *original = half ? f->master.state->holder_image.payload : f->master.state->holder_status_intent.payload;
	ResourceXWireReject reject;
	ResourceXDecodedFrame frame = {0};
	uint8 type = half ? RESOURCE_X_MSG_IMAGE_OR_GRANT : RESOURCE_X_MSG_BLOCKED_TO_N;
	memcpy(expected, original, length);
	UT_ASSERT(cluster_resource_x_wire_rebind_sender_generation(type, expected, length, half ? 83 : 1, &reject));
	UT_ASSERT_EQ(h15_length[half], length);
	UT_ASSERT_EQ(memcmp(expected, h15_bytes[half], length), 0);
	UT_ASSERT(cluster_resource_x_wire_decode(type, h15_bytes[half], length, &frame, &reject));
	UT_ASSERT_EQ(frame.common.sender_connection_generation, half ? 83 : 1);
	h15_crc[half] = frame.common.semantic_crc32c;
}
static void h15_run(PcmState target, bool would_block)
{
	HolderFixture f;
	ClusterPcmResourceXHolderStatus status;
	ClusterPcmResourceXHolderImage image;
	ClusterPcmLocalPiSnapshotV1 pi;
	ClusterPcmResourceXMasterRequest master_request;
	ResourceXIntentSlot forward, staged;
	uint32 marker = 42;
	WRITEBACK_START(&f, target);
	/* The fixture has already delivered this exact original FORWARD to
	 * the holder. Complete only its physical send owner before testing
	 * the newly published status/image pair. Master ACK stays absent. */
	forward = f.master.state->block_intents[0].slot; fake_pcm_clock_us += 1000000;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(&forward, fake_pcm_clock_us, &staged), RESOURCE_X_INTENT_STAGED);
	cluster_lms_outbound_resource_x_send_complete(&staged, true);
	UT_ASSERT_EQ(f.master.state->block_intents[0].slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	(void)writeback_install_wal(); writeback_dirty();
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	status = f.master.state->holder_status; image = f.master.state->holder_image;
	master_request = f.master.state->requests[1];
	UT_ASSERT_EQ(master_request.blocked_holders_bitmap, 0);
	UT_ASSERT(master_request.incompatible_holders_bitmap != 0);
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(request_buf.tag, &pi));
	memset(h15_calls, 0, sizeof(h15_calls)); memset(h15_taken, 0, sizeof(h15_taken));
	memset(h15_length, 0, sizeof(h15_length)); memset(h15_bytes, 0, sizeof(h15_bytes));
	memset(h15_crc, 0, sizeof(h15_crc));
	h15_fill_calls = h15_wakes = 0; h15_tail_owned = false;
	for (int n = 0; n < PGRAC_LMS_OUTBOUND_CAPACITY; n++)
		UT_ASSERT_EQ(h15_try_enqueue(1, PGRAC_IC_MSG_SCN_BROADCAST, 3, &marker, sizeof(marker)), CLUSTER_LMS_ENQUEUE_ADMITTED);
	UT_ASSERT_EQ(request_shared.rings[1].count, 256);
	UT_ASSERT_EQ(h15_try_enqueue(1, PGRAC_IC_MSG_SCN_BROADCAST, 3, &marker, sizeof(marker)), CLUSTER_LMS_ENQUEUE_FULL);
	for (int n = 0; n < 3; n++) {
		fake_pcm_clock_us += 1000000; h15_stage_ready(&request_buf.tag);
		for (int half = 0; half < 2; half++) { UT_ASSERT_EQ(holder_slot(&f, half)->state, RESOURCE_X_INTENT_SLOT_ARMED); UT_ASSERT_EQ(holder_slot(&f, half)->send_episode, 0); }
		h15_pair(&f, &status, &image, &pi, &master_request);
	}
	for (int n = 0; n < 4; n++) UT_ASSERT_EQ(h15_drain(1), 64);
	UT_ASSERT_EQ(request_shared.rings[1].count, 0);
	UT_ASSERT_EQ(h15_fill_calls, 256); UT_ASSERT_EQ(h15_wakes, 4);
	h15_rc = CLUSTER_IC_SEND_NOT_ADMITTED;
	for (int n = 0; n < 3; n++) {
		fake_pcm_clock_us += 1000000; h15_stage_ready(&request_buf.tag);
		UT_ASSERT_EQ(request_shared.rings[1].count, 2); UT_ASSERT_EQ(h15_drain(1), 0);
		UT_ASSERT_EQ(request_shared.rings[1].count, 0);
		for (int half = 0; half < 2; half++) { h15_check_bytes(&f, half); UT_ASSERT_EQ(holder_slot(&f, half)->state, RESOURCE_X_INTENT_SLOT_ARMED); UT_ASSERT_EQ(holder_slot(&f, half)->send_episode, n + 1); }
		h15_pair(&f, &status, &image, &pi, &master_request);
	}
	h15_rc = would_block ? CLUSTER_IC_SEND_WOULD_BLOCK : CLUSTER_IC_SEND_DONE;
	fake_pcm_clock_us += 1000000; h15_stage_ready(&request_buf.tag);
	for (int half = 0; half < 2; half++) UT_ASSERT_EQ(holder_slot(&f, half)->send_episode, 4);
	UT_ASSERT_EQ(h15_drain(1), 2); UT_ASSERT_EQ(request_shared.rings[1].count, 0);
	UT_ASSERT_EQ(holder_slot(&f, false)->state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(holder_slot(&f, true)->state, would_block ? RESOURCE_X_INTENT_SLOT_STAGED : RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(h15_taken[0], 1); UT_ASSERT_EQ(h15_taken[1], 1);
	for (int half = 0; half < 2; half++) h15_check_bytes(&f, half);
	h15_pair(&f, &status, &image, &pi, &master_request);
	for (int n = 0; n < 2; n++) { h15_stage_ready(&request_buf.tag); UT_ASSERT_EQ(h15_drain(1), 0); }
	UT_ASSERT_EQ(h15_calls[0], 4); UT_ASSERT_EQ(h15_calls[1], 4);
	if (would_block) {
		UT_ASSERT(h15_tail_owned); UT_ASSERT_EQ(h15_tail.send_episode, 4);
		cluster_lms_outbound_resource_x_send_complete(&h15_tail, true); h15_tail_owned = false;
		UT_ASSERT_EQ(holder_slot(&f, true)->state, RESOURCE_X_INTENT_SLOT_EMPTY);
		cluster_lms_outbound_resource_x_send_complete(&h15_tail, true);
	}
	h15_pair(&f, &status, &image, &pi, &master_request); writeback_idle();
	printf("# H15 target=%d WB=%d E=%llu A=%llu service=%llu gen=%llu fill=%u sends=%u/%u takes=%u/%u ring=%u draining=%u PI=%u transport=%u ack=%u wakes=%u\n",
		target, would_block, (unsigned long long)f.block.common.assertion_sequence,
		(unsigned long long)f.block.common.authority_generation, (unsigned long long)f.block.body.acquire_decision.source_service_generation,
		(unsigned long long)cluster_pcm_own_gen_get(0), h15_fill_calls, h15_calls[0], h15_calls[1], h15_taken[0], h15_taken[1],
		request_shared.rings[1].count, request_shared.rings[1].draining, f.master.state->pi_carrier.pi_refs,
		f.master.state->pi_carrier.transport_refs, f.master.state->requests[1].blocked_holders_bitmap, h15_wakes);
	printf("# H15 bytes=%u/%u physical_crc=%u/%u lower=2 floor=100 image_episode=4 tail=%d\n",
		h15_length[0], h15_length[1], h15_crc[0], h15_crc[1], h15_tail_owned);
}
UT_TEST(h15_native_S_refusal_then_DONE) { h15_run(PCM_STATE_S, false); }
UT_TEST(h15_native_S_refusal_then_WOULD_BLOCK) { h15_run(PCM_STATE_S, true); }
UT_TEST(h15_native_X_refusal_then_DONE) { h15_run(PCM_STATE_X, false); }
UT_TEST(h15_native_X_refusal_then_WOULD_BLOCK) { h15_run(PCM_STATE_X, true); }
#if defined(K_P130_EXPECT_ASSERT_OFF) && defined(USE_ASSERT_CHECKING)
#error "H15 assertion-OFF requires an assertion-disabled configuration"
#endif
int main(void)
{
#ifdef USE_ASSERT_CHECKING
	printf("# H15 USE_ASSERT_CHECKING=1\n");
#else
	printf("# H15 USE_ASSERT_CHECKING=0\n");
#endif
	UT_PLAN(4);
	UT_RUN(h15_native_S_refusal_then_DONE);
	UT_RUN(h15_native_S_refusal_then_WOULD_BLOCK);
	UT_RUN(h15_native_X_refusal_then_DONE);
	UT_RUN(h15_native_X_refusal_then_WOULD_BLOCK);
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
