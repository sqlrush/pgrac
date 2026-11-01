/* Actual PCM receipt/FIFO/canonical owners. Runtime and authentication leaves
 * come from the original PCM fixture; no native install or socket is claimed. */
int k_p130_h26_original_main(void);
#define main k_p130_h26_original_main
#include "test_cluster_pcm_lock.c"
#undef main
#include "test_cluster_resource_x_decision_owner_layout.inc"

typedef struct H26Transport {
	ResourceXDecodedFrame request;
	ResourceXIntentSlot slot;
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXMasterState before;
	uint8 bytes[RESOURCE_X_PROOF_V1_BYTES];
	uint32 crc;
} H26Transport;

static uint32
h26_crc(const void *bytes, Size len)
{
	pg_crc32c crc;
	INIT_CRC32C(crc); COMP_CRC32C(crc, bytes, len); FIN_CRC32C(crc);
	return (uint32)crc;
}

static ClusterPcmResourceXMasterState *
h26_state(const ResourceXDecodedCommon *request)
{
	Size offset = offsetof(ClusterPcmResourceXMasterState, bootstrap_receipts);
	ClusterPcmResourceXMasterState *state = NULL;
	unsigned matches = 0;
	offset += request->logical_assertion.requester_node * sizeof(ClusterPcmResourceXBootstrapReceipt);
	for (Size n = offset; n + sizeof(*request) <= fake_pcm_header_requested_size; n += sizeof(uint64))
		if (memcmp(fake_pcm_header.data + n, request, sizeof(*request)) == 0) {
			state = (void *)(fake_pcm_header.data + n - offset); matches++;
		}
	UT_ASSERT_EQ(matches, 1);
	UT_ASSERT(state != NULL && (unsigned char *)(state + 1)
		<= (unsigned char *)fake_pcm_header.data + fake_pcm_header_requested_size);
	return matches == 1 ? state : NULL;
}

static ResourceXApplyResult
h26_deliver(const ResourceXDecodedFrame *request, ResourceXDecodedFrame *ack)
{
	int32 node = request->common.logical_assertion.requester_node;
	ResourceXApplyResult result;
	memset(ack, 0xa5, sizeof(*ack));
	result = cluster_pcm_lock_resource_x_bootstrap_request_exact(
		request, node, 61 + node, 77, 31, 71 + node, ack);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	return result;
}

static ResourceXDecodedFrame
h26_request(BufferTag tag, int node, uint64 seq, int mode)
{
	ResourceXDecodedFrame request;
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES];
	uint16 len = 0;
	if (!mode)
		return make_resource_x_bootstrap_request_values(tag, node, 17, 31, seq, 51 + node);
	request = make_resource_x_acquire_request(tag, node, RESOURCE_X_ACQUIRE_RESIDENT,
		PCM_STATE_N, mode == 1 ? PCM_STATE_S : PCM_STATE_X);
	request.common.assertion_sequence = seq;
	request.common.sender_connection_generation = 51 + node;
	request.common.semantic_crc32c = 0;
	request.body.acquire_intent.cluster_epoch = 17;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, &request,
		bytes, sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(len, RESOURCE_X_ACQUIRE_V3_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X,
		bytes, len, &decoded, &reject));
	return decoded;
}

static ResourceXDecodedFrame
h26_assertion(const ResourceXDecodedFrame *ack)
{
	ResourceXDecodedFrame assertion = *ack;
	assertion.kind = RESOURCE_X_WIRE_ASSERT_X;
	assertion.common.sender_connection_generation = 51 + ack->common.logical_assertion.requester_node;
	assertion.common.authority_generation = assertion.common.base_authority_generation;
	assertion.common.outcome = RESOURCE_X_OUTCOME_NONE;
	return assertion;
}

static void
h26_reset(void)
{
	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	MyBackendType = B_LMS;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17), RESOURCE_X_APPLY_APPLIED);
}

static void
h26_transport_start(H26Transport *t)
{
	ResourceXDecodedFrame ack, assertion;
	ResourceXMasterSnapshot snapshot;
	ResourceXDurableProof durable = {0};
	memset(t, 0, sizeof(*t));
	t->request = h26_request(make_tag(896), 3, 91, 0);
	UT_ASSERT_EQ(h26_deliver(&t->request, &ack), RESOURCE_X_APPLY_APPLIED);
	t->state = h26_state(&t->request.common);
	if (!t->state) return;
	assertion = h26_assertion(&ack);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&assertion, 3, 64, 77, 31, 74, &snapshot), RESOURCE_X_APPLY_APPLIED);
	durable.assertion = assertion.common.logical_assertion;
	durable.base_authority_generation = 1;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = 91;
	durable.requester_target_generation = 91;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x12345678);
	durable.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(&durable, &snapshot), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
		&assertion.common.logical_assertion, &t->slot, t->bytes, sizeof(t->bytes)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_stage_exact(&t->slot, 101), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
		&assertion.common.logical_assertion, &t->slot, t->bytes, sizeof(t->bytes)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(t->slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
	t->before = *t->state;
	t->crc = h26_crc(t->bytes, t->slot.payload_bytes);
}

static void
h26_transport_check(H26Transport *t)
{
	ResourceXIntentSlot slot;
	uint8 bytes[RESOURCE_X_PROOF_V1_BYTES] = {0};
	UT_ASSERT_EQ(memcmp(&t->before, t->state, sizeof(t->before)), 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
		&t->request.common.logical_assertion, &slot, bytes, sizeof(bytes)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(memcmp(&slot, &t->slot, sizeof(slot)), 0);
	UT_ASSERT_EQ(memcmp(bytes, t->bytes, slot.payload_bytes), 0);
	UT_ASSERT_EQ(h26_crc(bytes, slot.payload_bytes), t->crc);
}

static void
h26_census(ClusterPcmResourceXMasterState *s, unsigned queued, unsigned received, unsigned consumed,
	unsigned canonical)
{
	unsigned q = 0, r = 0, c = 0, a = 0;
	for (int n = 0; n < RESOURCE_X_PROTOCOL_NODE_LIMIT; n++) {
		q += s->bootstrap_priority[n].state != RESOURCE_X_BOOTSTRAP_PRIORITY_EMPTY;
		r += s->bootstrap_receipts[n].state == RESOURCE_X_BOOTSTRAP_RECEIPT_RECEIVED;
		c += s->bootstrap_receipts[n].state == RESOURCE_X_BOOTSTRAP_RECEIPT_CONSUMED_BY_ASSERT;
		a += s->requests[n].phase != RESOURCE_X_MASTER_NONE;
	}
	UT_ASSERT_EQ(q, queued); UT_ASSERT_EQ(r, received); UT_ASSERT_EQ(c, consumed); UT_ASSERT_EQ(a, canonical);
}

static void
h26_fifo(int mode)
{
	BufferTag tag = make_tag(890);
	H26Transport transport;
	ResourceXDecodedFrame old, higher, first, second, ack, old_ack, stale;
	ResourceXBootstrapWork work;
	ResourceXMasterSnapshot snapshot;
	ClusterPcmResourceXMasterState *s;
	ClusterPcmResourceXBootstrapReceipt empty = {0};
	uint64 order[3];
	h26_reset(); h26_transport_start(&transport);
	old = h26_request(tag, 0, 41, 0); higher = h26_request(tag, 0, 42, mode);
	first = h26_request(tag, 1, 51, 0); second = h26_request(tag, 2, 61, 0);
	UT_ASSERT_EQ(h26_deliver(&old, &old_ack), RESOURCE_X_APPLY_APPLIED);
	s = h26_state(&old.common); if (!s) return;
	UT_ASSERT_EQ(h26_deliver(&first, &ack), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(h26_deliver(&second, &ack), RESOURCE_X_APPLY_BAD_STATE);
	order[0] = s->bootstrap_priority[1].enqueue_order;
	order[1] = s->bootstrap_priority[2].enqueue_order;
	UT_ASSERT_EQ(h26_deliver(&higher, &ack), mode ? RESOURCE_X_APPLY_APPLIED : RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(ack.kind, 0);
	empty.highest_attempt_floor = mode ? 42 : 41;
	UT_ASSERT_EQ(memcmp(&empty, &s->bootstrap_receipts[0], sizeof(empty)), 0);
	order[2] = s->bootstrap_priority[0].enqueue_order;
	UT_ASSERT(order[0] < order[1] && order[1] < order[2]);
	h26_census(s, 3, 0, 0, 0); h26_transport_check(&transport);
	stale = h26_assertion(&old_ack);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&stale, 0, 61, 77, 31, 71, &snapshot), mode ? RESOURCE_X_APPLY_BAD_STATE : RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(h26_deliver(&old, &ack), RESOURCE_X_APPLY_STALE);
	for (int node = 1; node <= 2; node++) {
		work = bootstrap_next_work();
		UT_ASSERT_EQ(work.request.logical_assertion.requester_node, node);
		UT_ASSERT_EQ(work.enqueue_order, order[node - 1]);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_resume_exact(&work, &ack), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(ack.common.assertion_sequence, node == 1 ? 51 : 61);
		stale = h26_request(tag, node, node == 1 ? 52 : 62, 0);
		UT_ASSERT_EQ(h26_deliver(&stale, &ack), RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT_EQ(ack.kind, 0);
		h26_transport_check(&transport);
	}
	work = bootstrap_next_work();
	UT_ASSERT_EQ(work.request.logical_assertion.requester_node, 0);
	UT_ASSERT_EQ(work.request.assertion_sequence, 42); UT_ASSERT_EQ(work.enqueue_order, order[2]);
	UT_ASSERT_EQ(work.has_acquire_intent, mode != 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_resume_exact(&work, &ack), RESOURCE_X_APPLY_APPLIED);
	if (mode) {
		UT_ASSERT_EQ(ack.kind, 0);
		UT_ASSERT_EQ(s->requests[0].phase, RESOURCE_X_MASTER_QUEUED);
		UT_ASSERT_EQ(memcmp(&s->bootstrap_priority[0].acquire_intent,
			&higher.body.acquire_intent, sizeof(higher.body.acquire_intent)), 0);
		h26_census(s, 3, 0, 0, 1);
		cluster_pcm_lock_resource_x_bootstrap_forget_exact(&work);
	} else {
		UT_ASSERT_EQ(ack.common.assertion_sequence, 42);
		UT_ASSERT_EQ(ack.common.base_authority_generation, 1);
		h26_census(s, 2, 1, 0, 0);
		/* The final legacy receipt still blocks dispatch. A genuinely higher
		 * attempt retires it; the original stop gate denies new admission. */
		stale = h26_request(tag, 0, 43, 0);
		stop_new_work_allowed = false;
		UT_ASSERT_EQ(h26_deliver(&stale, &ack), RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT_EQ(ack.kind, 0);
		stop_new_work_allowed = true;
	}
	for (int node = 1; node <= 2; node++) {
		work = bootstrap_next_work(); UT_ASSERT_EQ(work.request.logical_assertion.requester_node, node);
		cluster_pcm_lock_resource_x_bootstrap_forget_exact(&work);
	}
	h26_census(s, 0, 0, 0, 0);
	UT_ASSERT_EQ(s->bootstrap_receipts[0].highest_attempt_floor, 42);
	UT_ASSERT_EQ(s->bootstrap_receipts[1].highest_attempt_floor, 51);
	UT_ASSERT_EQ(s->bootstrap_receipts[2].highest_attempt_floor, 61);
	h26_transport_check(&transport);
	UT_ASSERT(cluster_pcm_lock_resource_x_grant_intent_complete_exact(&transport.slot));
	printf("# H26 FIFO mode=%d order=%llu,%llu,%llu retired_floor=%llu final_floor=42 queue=0 received=0 canonical=0 bystander_crc=%u bystander_completed=1 native_not_exercised=1\n",
		mode, (unsigned long long)order[0], (unsigned long long)order[1], (unsigned long long)order[2],
		(unsigned long long)empty.highest_attempt_floor, transport.crc);
}

static void
h26_byte_guard(int mode)
{
	H26Transport transport;
	BufferTag tag = make_tag(891);
	ResourceXDecodedFrame old, higher, ack;
	ClusterPcmResourceXMasterState *s, before;
	unsigned rejected = 0;
	h26_reset(); h26_transport_start(&transport);
	old = h26_request(tag, 0, 41, 0); higher = h26_request(tag, 0, 42, mode);
	UT_ASSERT_EQ(h26_deliver(&old, &ack), RESOURCE_X_APPLY_APPLIED);
	s = h26_state(&old.common); if (!s) return;
	for (Size byte = 0; byte < sizeof(s->requests[0]); byte++) {
		ResourceXApplyResult result;
		((unsigned char *)&s->requests[0])[byte] = 1;
		before = *s;
		result = h26_deliver(&higher, &ack);
		UT_ASSERT_EQ(result, mode ? (s->requests[0].assertion_sequence >= 42
			? RESOURCE_X_APPLY_STALE : RESOURCE_X_APPLY_BAD_STATE) :
			byte == offsetof(ClusterPcmResourceXMasterRequest, phase)
				? RESOURCE_X_APPLY_STALE : RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(ack.kind, 0);
		UT_ASSERT_EQ(memcmp(&before, s, sizeof(before)), 0);
		rejected += result != RESOURCE_X_APPLY_APPLIED && result != RESOURCE_X_APPLY_DUPLICATE;
		h26_transport_check(&transport);
		((unsigned char *)&s->requests[0])[byte] = 0;
	}
	h26_census(s, 0, 1, 0, 0);
	UT_ASSERT_EQ(rejected, sizeof(s->requests[0]));
	UT_ASSERT_EQ(s->bootstrap_receipts[0].highest_attempt_floor, 41);
	UT_ASSERT_EQ(h26_deliver(&higher, &ack), RESOURCE_X_APPLY_APPLIED);
	if (mode) {
		ResourceXBootstrapWork work = bootstrap_next_work();
		cluster_pcm_lock_resource_x_bootstrap_forget_exact(&work);
	}
	h26_census(s, 0, mode ? 0 : 1, 0, 0); h26_transport_check(&transport);
	UT_ASSERT(cluster_pcm_lock_resource_x_grant_intent_complete_exact(&transport.slot));
	printf("# H26 byte_guard mode=%d bytes=%zu rejected=%u receipt_floor=41 restored_admitted=1 bystander_crc=%u native_not_exercised=1\n",
		mode, sizeof(s->requests[0]), rejected, transport.crc);
}

static void
h26_consumed(int mode)
{
	H26Transport transport;
	BufferTag tag = make_tag(892);
	ResourceXDecodedFrame old, higher, ack, assertion;
	ResourceXMasterSnapshot snapshot;
	ClusterPcmResourceXMasterState *s, before;
	h26_reset(); h26_transport_start(&transport);
	old = h26_request(tag, 0, 41, 0); higher = h26_request(tag, 0, 42, mode);
	UT_ASSERT_EQ(h26_deliver(&old, &ack), RESOURCE_X_APPLY_APPLIED);
	s = h26_state(&old.common); if (!s) return;
	assertion = h26_assertion(&ack);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		&assertion, 0, 61, 77, 31, 71, &snapshot), RESOURCE_X_APPLY_APPLIED);
	h26_census(s, 0, 0, 1, 1); before = *s;
	for (int repeat = 0; repeat < 3; repeat++) {
		UT_ASSERT_EQ(h26_deliver(&higher, &ack), mode ? RESOURCE_X_APPLY_BAD_STATE : RESOURCE_X_APPLY_STALE);
		UT_ASSERT_EQ(ack.kind, 0);
		UT_ASSERT_EQ(memcmp(&before, s, sizeof(before)), 0);
		h26_transport_check(&transport);
	}
	h26_census(s, 0, 0, 1, 1);
	UT_ASSERT(cluster_pcm_lock_resource_x_grant_intent_complete_exact(&transport.slot));
	printf("# H26 consumed mode=%d rejected=3 receipt=1 canonical=1 retained_floor=%llu bystander_crc=%u native_not_exercised=1\n",
		mode, (unsigned long long)s->bootstrap_receipts[0].highest_attempt_floor, transport.crc);
}

UT_TEST(h26_legacy_higher_retires_before_r1_r2_fifo) { h26_fifo(0); }
UT_TEST(h26_full_s_higher_retires_before_r1_r2_fifo) { h26_fifo(1); }
UT_TEST(h26_full_x_higher_retires_before_r1_r2_fifo) { h26_fifo(2); }
UT_TEST(h26_legacy_every_actual_canonical_byte_keeps_receipt) { h26_byte_guard(0); }
UT_TEST(h26_full_s_every_actual_canonical_byte_keeps_receipt) { h26_byte_guard(1); }
UT_TEST(h26_full_x_every_actual_canonical_byte_keeps_receipt) { h26_byte_guard(2); }
UT_TEST(h26_legacy_consumed_and_foreign_transport_survive) { h26_consumed(0); }
UT_TEST(h26_full_s_consumed_and_foreign_transport_survive) { h26_consumed(1); }
UT_TEST(h26_full_x_consumed_and_foreign_transport_survive) { h26_consumed(2); }

int main(void)
{
#ifdef USE_ASSERT_CHECKING
	printf("# H26 USE_ASSERT_CHECKING=1\n");
#else
	printf("# H26 USE_ASSERT_CHECKING=0\n");
#endif
#ifdef K_P130_EXPECT_ASSERT_OFF
#ifdef USE_ASSERT_CHECKING
#error H26 requires an actually assertion-disabled configuration
#endif
#endif
	UT_PLAN(9);
	UT_RUN(h26_legacy_higher_retires_before_r1_r2_fifo);
	UT_RUN(h26_full_s_higher_retires_before_r1_r2_fifo);
	UT_RUN(h26_full_x_higher_retires_before_r1_r2_fifo);
	UT_RUN(h26_legacy_every_actual_canonical_byte_keeps_receipt);
	UT_RUN(h26_full_s_every_actual_canonical_byte_keeps_receipt);
	UT_RUN(h26_full_x_every_actual_canonical_byte_keeps_receipt);
	UT_RUN(h26_legacy_consumed_and_foreign_transport_survive);
	UT_RUN(h26_full_s_consumed_and_foreign_transport_survive);
	UT_RUN(h26_full_x_consumed_and_foreign_transport_survive);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
