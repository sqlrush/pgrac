/* Bounded acquisition and completion owners under original capacity pressure.
 * Each environment is a separate executable; no full backend is simulated.
 * Author: SqlRush <sqlrush@gmail.com> */
#if defined(K_P130_H24_RECEIPT)
#include "test_cluster_k_p130_h24_receipt.inc"

static void
h24_receipt(int profile, bool ack_first)
{
	EvidenceFixture f;
	ResourceXDecodedFrame notice, malformed;
	ResourceXIntentSlot staged;
	ClusterPcmResourceXReadIntent before;
	ClusterPcmResourceXBootstrapRound round;
	UT_ASSERT(sr_start(&f, profile));
	if (!sr_arm() || !sr_quiet(false)) { UT_ASSERT(false); return; }
	notice = sr_frame();
	before = sr_state->requester_settlement_intent;
	round = request_entry()->resource_x_bootstrap_round;
	for (int n = 0; n < 3; n++) {
		fake_pcm_clock_us += 100000000;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(
			&sr_state->requester_settlement_intent.slot, fake_pcm_clock_us), RESOURCE_X_INTENT_NOT_ADMITTED);
		UT_ASSERT_EQ(sr_state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
		UT_ASSERT(memcmp(before.payload, sr_state->requester_settlement_intent.payload,
			sizeof(before.payload)) == 0);
		malformed = notice;
		malformed.body.acquire_completion.decision.episode.backend_generation++;
		UT_ASSERT(sr_ack(malformed, true) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	}
	fake_pcm_clock_us += 100000000;
	if (!sr_stage(&staged)) { UT_ASSERT(false); return; }
	if (ack_first) {
		UT_ASSERT_EQ(sr_ack(notice, true), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(sr_state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
		UT_ASSERT(request_entry()->resource_x_bootstrap_round.has_acquire_intent);
	} else {
		UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(
			&staged, true, ++fake_pcm_clock_us));
		UT_ASSERT(request_entry()->resource_x_bootstrap_round.has_acquire_intent);
	}
	if (ack_first)
		UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(
			&staged, true, ++fake_pcm_clock_us));
	else UT_ASSERT_EQ(sr_ack(notice, true), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(sr_state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT(!request_entry()->resource_x_bootstrap_round.has_acquire_intent);
	UT_ASSERT(request_entry()->resource_x_bootstrap_round.highest_attempt_floor > 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	printf("# H24 receipt profile=%d ack_first=%d cap=1 retired=1 floor=%llu E=%llu A=%llu Icrc=%u bytes=%u native_provider=1\n",
		profile, ack_first, (unsigned long long)request_entry()->resource_x_bootstrap_round.highest_attempt_floor,
		(unsigned long long)notice.common.assertion_sequence,
		(unsigned long long)notice.common.authority_generation,
		notice.body.acquire_completion.page_crc32c, notice.payload_bytes);
}
UT_TEST(h24_receipt_S_ACK_before_callback) { h24_receipt(2, true); }
UT_TEST(h24_receipt_S_callback_before_ACK) { h24_receipt(2, false); }
UT_TEST(h24_receipt_X_ACK_before_callback) { h24_receipt(3, true); }
UT_TEST(h24_receipt_X_callback_before_ACK) { h24_receipt(3, false); }

#elif defined(K_P130_H24_TRANSPORT)
#include "test_cluster_k_p130_h24_transport.inc"
#include "test_cluster_k_p130_h24_capacity.inc"

static void
h24_transport(int first, int end)
{
	for (int profile = first; profile < end; profile++) {
		ResourceXIntentSlot intent, malformed, before;
		uint8 bytes[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES];
		unsigned count = 0;
		if (ut_captured_region == NULL) cluster_lms_outbound_shmem_register();
		ut_captured_region->init_fn();
		intent = transport_start(profile); before = ut_resource_x_owner_slot;
		memcpy(bytes, ut_resource_x_owner_payload, sizeof(bytes));
		while (count <= PGRAC_LMS_OUTBOUND_CAPACITY && ut_enqueue_marker(0, UT_PEER_Y, 0x81)) count++;
		UT_ASSERT_EQ(count, PGRAC_LMS_OUTBOUND_CAPACITY);
		UT_ASSERT_EQ(cluster_lms_outbound_depth(0), count);
		malformed = intent; malformed.kind = 0;
		UT_ASSERT_EQ(cluster_lms_outbound_enqueue_resource_x_intent(0, &malformed, 77, UINT64_MAX),
			CLUSTER_LMS_ENQUEUE_INVALID);
		for (int n = 0; n < 3; n++) {
			UT_ASSERT_EQ(cluster_lms_outbound_enqueue_resource_x_intent(0, &intent, 77, UINT64_MAX),
				CLUSTER_LMS_ENQUEUE_FULL);
			UT_ASSERT_EQ(cluster_lms_outbound_depth(0), count);
			UT_ASSERT_EQ(ut_resource_x_stage_count, 0);
			UT_ASSERT(memcmp(&before, &ut_resource_x_owner_slot, sizeof(before)) == 0);
			UT_ASSERT(memcmp(bytes, ut_resource_x_owner_payload, sizeof(bytes)) == 0);
		}
		ut_peer_rc[UT_PEER_Y] = CLUSTER_IC_SEND_DONE;
		for (unsigned n = 0; n < count && cluster_lms_outbound_depth(0); n++)
			(void)cluster_lms_outbound_drain_send(0);
		UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
		UT_ASSERT(transport_enqueue(&intent));
		UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
		transport_check_bytes(&intent);
		UT_ASSERT_EQ(ut_resource_x_complete_count, 1);
		UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
		UT_ASSERT_EQ(cluster_lms_outbound_resource_x_staged_count(), 0);
		printf("# H24 transport profile=%d cap=%u full=3 invalid=1 stage=1 complete=1 ring=0 bytes=%u semantic_provider=1\n",
			profile, count, transport_length);
	}
}
UT_TEST(h24_transport_full_original_ring_preserves_master_offers) { h24_transport(0, 2); }
UT_TEST(h24_transport_full_original_ring_drains_source_completion) { h24_transport(2, 6); }

#else
#include "test_cluster_resource_x_acquire_fixture.inc"
#include "test_cluster_resource_x_decision_owner_layout.inc"

static ClusterPcmResourceXMasterState *
h24_state(const ResourceXDecodedCommon *first)
{
	Size offset = offsetof(ClusterPcmResourceXMasterState, bootstrap_priority);
	ClusterPcmResourceXMasterState *state = NULL;
	unsigned matches = 0;
	for (Size n = offset; n + sizeof(*first) <= fake_pcm_header_requested_size; n += sizeof(uint64))
		if (memcmp(fake_pcm_header.data + n, first, sizeof(*first)) == 0) {
			state = (void *)(fake_pcm_header.data + n - offset); matches++;
		}
	UT_ASSERT_EQ(matches, 1);
	return matches == 1 ? state : NULL;
}

static void
h24_queue_census(ClusterPcmResourceXMasterState *s, unsigned queued, unsigned canonical)
{
	unsigned q = 0, c = 0, floors = 0;
	for (int n = 0; n < RESOURCE_X_PROTOCOL_NODE_LIMIT; n++) {
		q += s->bootstrap_priority[n].state != RESOURCE_X_BOOTSTRAP_PRIORITY_EMPTY;
		c += s->requests[n].phase != RESOURCE_X_MASTER_NONE;
		floors += s->bootstrap_receipts[n].highest_attempt_floor != 0;
		UT_ASSERT_EQ(s->bootstrap_receipts[n].state, RESOURCE_X_BOOTSTRAP_RECEIPT_EMPTY);
	}
	UT_ASSERT_EQ(q, queued); UT_ASSERT_EQ(c, canonical);
	UT_ASSERT_EQ(floors, RESOURCE_X_PROTOCOL_NODE_LIMIT);
	UT_ASSERT_EQ(s->authority_generation, 1);
}

static void
h24_acquire(PcmState target)
{
	ResourceXDecodedFrame frames[RESOURCE_X_PROTOCOL_NODE_LIMIT], next, bad;
	ResourceXBootstrapWork work, stale;
	ClusterPcmResourceXMasterState *state, before;
	ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES]; uint16 length = 0;
	acquire_reset();
	for (int n = 0; n < RESOURCE_X_PROTOCOL_NODE_LIMIT; n++) {
		frames[n] = acquire_frame(n, RESOURCE_X_ACQUIRE_RESIDENT, PCM_STATE_N, target);
		UT_ASSERT_EQ(acquire_deliver(&frames[n]), RESOURCE_X_APPLY_APPLIED);
	}
	state = h24_state(&frames[0].common);
	if (state == NULL) return;
	h24_queue_census(state, RESOURCE_X_PROTOCOL_NODE_LIMIT, 0);
	work = bootstrap_next_work();
	UT_ASSERT_EQ(acquire_canonical_resume(&work), RESOURCE_X_APPLY_APPLIED);
	h24_queue_census(state, RESOURCE_X_PROTOCOL_NODE_LIMIT, 1);
	before = *state;
	next = frames[0]; next.common.assertion_sequence++;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, &next, bytes, sizeof(bytes), &length, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X, bytes, length, &next, &reject));
	bad = next; bad.body.acquire_intent.reservation_token = 0;
	UT_ASSERT_EQ(acquire_deliver(&bad), RESOURCE_X_APPLY_INVALID);
	for (int age = 0; age < 3; age++) {
		fake_pcm_clock_us += 100000000;
		UT_ASSERT_EQ(acquire_deliver(&next), RESOURCE_X_APPLY_BAD_STATE);
		for (int n = 0; n < RESOURCE_X_PROTOCOL_NODE_LIMIT; n++)
			UT_ASSERT_EQ(acquire_deliver(&frames[n]), RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT(memcmp(&before, state, sizeof(before)) == 0);
	}
	stale = work; stale.binding_generation++;
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&stale);
	UT_ASSERT(memcmp(&before, state, sizeof(before)) == 0);
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&work);
	h24_queue_census(state, RESOURCE_X_PROTOCOL_NODE_LIMIT - 1, 0);
	UT_ASSERT_EQ(acquire_deliver(&next), RESOURCE_X_APPLY_APPLIED);
	for (int n = 1; n <= RESOURCE_X_PROTOCOL_NODE_LIMIT; n++) {
		work = bootstrap_next_work();
		UT_ASSERT_EQ(work.request.logical_assertion.requester_node, n % RESOURCE_X_PROTOCOL_NODE_LIMIT);
		UT_ASSERT_EQ(acquire_canonical_resume(&work), RESOURCE_X_APPLY_APPLIED);
		cluster_pcm_lock_resource_x_bootstrap_forget_exact(&work);
		h24_queue_census(state, RESOURCE_X_PROTOCOL_NODE_LIMIT - n, 0);
	}
	UT_ASSERT_EQ(acquire_deliver(&frames[0]), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(bootstrap_ack_count, 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	printf("# H24 ingress target=%d cap=%d canonical_peak=1 full=3 invalid=1 queued=0 floors=%d A=0 ACK=0 native=0 finish=0 PI=0\n",
		target, RESOURCE_X_PROTOCOL_NODE_LIMIT, RESOURCE_X_PROTOCOL_NODE_LIMIT);
}
UT_TEST(h24_ingress_full_S_queue_keeps_exact_debt_and_drains) { h24_acquire(PCM_STATE_S); }
UT_TEST(h24_ingress_full_X_queue_keeps_exact_debt_and_drains) { h24_acquire(PCM_STATE_X); }
#endif

#if defined(K_P130_EXPECT_ASSERT_OFF) && defined(USE_ASSERT_CHECKING)
#error "H24 assertion-OFF requires an assertion-disabled configuration"
#endif
int main(void);
int
main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
#ifdef USE_ASSERT_CHECKING
	printf("# H24 USE_ASSERT_CHECKING=1\n");
#else
	printf("# H24 USE_ASSERT_CHECKING=0\n");
#endif
#if defined(K_P130_H24_RECEIPT)
	UT_PLAN(4);
	UT_RUN(h24_receipt_S_ACK_before_callback);
	UT_RUN(h24_receipt_S_callback_before_ACK);
	UT_RUN(h24_receipt_X_ACK_before_callback);
	UT_RUN(h24_receipt_X_callback_before_ACK);
#elif defined(K_P130_H24_TRANSPORT)
	UT_PLAN(2);
	UT_RUN(h24_transport_full_original_ring_preserves_master_offers);
	UT_RUN(h24_transport_full_original_ring_drains_source_completion);
#else
	UT_PLAN(2);
	UT_RUN(h24_ingress_full_S_queue_keeps_exact_debt_and_drains);
	UT_RUN(h24_ingress_full_X_queue_keeps_exact_debt_and_drains);
#endif
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
