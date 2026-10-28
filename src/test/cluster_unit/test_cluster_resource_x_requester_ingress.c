/* Actual GCS receiver/admission and original PCM evidence/ACK consumers.
 * Session/capability observations are explicit environment providers; native
 * completion and unrelated service leaves must never run in these cases.
 * This suite does not qualify a complete ordinary native acquisition. */
#define gcs_block_resource_x_gate_session_snapshot_result ingress_prior_session
#define gcs_block_resource_x_gate_session_recheck_result ingress_prior_recheck
#include "test_cluster_resource_x_master_ack_fixture.inc"
#undef gcs_block_resource_x_gate_session_snapshot_result
#undef gcs_block_resource_x_gate_session_recheck_result
#include "test_cluster_resource_x_transport_frames.inc"
#include "test_cluster_resource_x_requester_evidence_fixture.inc"

static uint64 ingress_master_session = 31;
static unsigned ingress_cleanup_calls, ingress_terminal_calls, ingress_master_calls;
static unsigned ingress_requester_calls, ingress_unrelated_calls;
static ResourceXApplyResult ingress_last_master, ingress_last_requester;
static bool ingress_session_drift;

static PcmXSessionAuthResult
gcs_block_resource_x_gate_session_snapshot_result(const BufferTag *tag,
	ResourceXGateSnapshot *gate, int32 *master, uint64 *session)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	if (!cluster_pcm_lock_resource_x_gate_snapshot(gate) || gate->phase != RESOURCE_X_GATE_OPEN)
		return PCM_X_SESSION_AUTH_INVALID;
	*master = cluster_gcs_lookup_master(*tag);
	*session = ingress_master_session + ingress_session_drift;
	return PCM_X_SESSION_AUTH_OK;
}

static ResourceXApplyResult
ingress_collect(const ResourceXDecodedFrame *frame, int32 source, uint32 ingress,
	int32 master, uint32 master_ingress, uint64 r4, ResourceXRequesterJoinSnapshot *out)
{
	ingress_requester_calls++;
	ingress_last_requester = cluster_pcm_lock_resource_x_requester_join_current_exact(frame,
		source, ingress, master, master_ingress, r4, out);
	return ingress_last_requester;
}

static ResourceXApplyResult
ingress_master_ack(const ResourceXDecodedFrame *frame, int32 source, ResourceXMasterSnapshot *out)
{
	ingress_master_calls++;
	ingress_last_master = cluster_pcm_lock_resource_x_blocked_to_n_exact(frame, source, out);
	return ingress_last_master;
}

static ResourceXApplyResult
ingress_unrelated(void)
{
	ingress_unrelated_calls++;
	UT_ASSERT(false);
	return RESOURCE_X_APPLY_INVALID;
}

static ResourceXApplyResult
ingress_cleanup(void)
{
	ingress_cleanup_calls++;
	return RESOURCE_X_APPLY_STALE;
}

static ResourceXApplyResult
ingress_terminal(void)
{
	ingress_terminal_calls++;
	UT_ASSERT(false);
	return RESOURCE_X_APPLY_INVALID;
}

/* Logging and unrelated handlers are leaves, not replacements for the tested
 * receiver, TARGET admission, session recheck, join or master ACK policy. */
#define gcs_block_resource_x_diagnostic_should_log(kind, result) false
#define gcs_block_resource_x_cleanup_frame_exact(...) ingress_cleanup()
#define gcs_block_resource_x_requester_terminal_try(...) ingress_terminal()
#define gcs_block_resource_x_kind9_ingress(...) ((void)ingress_unrelated())
#define gcs_block_resource_x_type17_ingress(...) ingress_unrelated()
#define gcs_block_resource_x_source_settlement_ingress(...) ingress_unrelated()
#define gcs_block_pcm_x_resource_x_master_durable_try(...) ingress_unrelated()
#define gcs_block_resource_x_fail_closed_current() ((void)ingress_unrelated())
#define cluster_ic_tier1_my_data_channel() 0
#define cluster_pcm_lock_resource_x_requester_join_current_exact ingress_collect
#define cluster_pcm_lock_resource_x_blocked_to_n_exact ingress_master_ack
#include "test_cluster_resource_x_requester_ingress.inc"
#undef cluster_pcm_lock_resource_x_blocked_to_n_exact
#undef cluster_pcm_lock_resource_x_requester_join_current_exact
#undef cluster_ic_tier1_my_data_channel
#undef gcs_block_resource_x_fail_closed_current
#undef gcs_block_pcm_x_resource_x_master_durable_try
#undef gcs_block_resource_x_source_settlement_ingress
#undef gcs_block_resource_x_type17_ingress
#undef gcs_block_resource_x_kind9_ingress
#undef gcs_block_resource_x_requester_terminal_try
#undef gcs_block_resource_x_cleanup_frame_exact
#undef gcs_block_resource_x_diagnostic_should_log

static void
ingress_boot_observation(int32 node, unsigned read pg_attribute_unused())
{
	auth_peer_boot = node == 0 ? 31 : node == 1 ? 101 : (uint64)200 + node;
}

static bool
ingress_begin(EvidenceFixture *f, int profile)
{
	if (!evidence_begin(f, profile)) return false;
	ingress_cleanup_calls = ingress_terminal_calls = ingress_master_calls = 0;
	ingress_requester_calls = ingress_unrelated_calls = 0;
	ingress_master_session = 31;
	ingress_session_drift = false;
	ingress_last_master = ingress_last_requester = RESOURCE_X_APPLY_INVALID;
	master_durable_admission = true;
	master_durable_admission_depth = 0;
	ready_peer_ok = true;
	ready_connection_generation = fake_rx_capability_generation = 61;
	bootstrap_ingress_generation = 61;
	auth_observation_hook = ingress_boot_observation;
	return true;
}

static ClusterICEnvelope
ingress_envelope(const ResourceXDecodedFrame *frame)
{
	ClusterICEnvelope env = {0};
	env.msg_type = frame->kind == RESOURCE_X_WIRE_BLOCKED_TO_N
		? RESOURCE_X_MSG_BLOCKED_TO_N : RESOURCE_X_MSG_IMAGE_OR_GRANT;
	env.source_node_id = frame->kind == RESOURCE_X_WIRE_AUTHORITY_GRANT
		? frame->body.acquire_decision.master_node : frame->common.action_node;
	env.dest_node_id = cluster_node_id;
	env.epoch = 17;
	env.payload_length = frame->payload_bytes;
	return env;
}

static bool
ingress_deliver_as(const ResourceXDecodedFrame *frame, ClusterICEnvelope env)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES];
	uint16 len;
	ResourceXWireReject reject;
	bool consumed;
	UT_ASSERT(cluster_resource_x_wire_encode(env.msg_type, frame, bytes, sizeof(bytes), &len, &reject));
	consumed = gcs_block_try_resource_x_frame(&env, bytes);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT_EQ(master_durable_admission_depth, 0);
	UT_ASSERT_EQ(ingress_cleanup_calls + ingress_terminal_calls + ingress_unrelated_calls, 0);
	return consumed;
}

static void
ingress_deliver(const ResourceXDecodedFrame *frame)
{
	UT_ASSERT(ingress_deliver_as(frame, ingress_envelope(frame)));
}

static ResourceXApplyResult
ingress_snapshot(const EvidenceFixture *f, ResourceXRequesterJoinSnapshot *out)
{
	ResourceXDecodedFrame grant, image;
	return cluster_pcm_lock_resource_x_requester_join_frames_exact(
		&f->request.common.logical_assertion, &grant, &image, out);
}

static void
ingress_connection_changes_during_observation(int32 node, unsigned read)
{
	ingress_boot_observation(node, read);
	if (read == 2) fake_rx_capability_generation++;
}

UT_TEST(test_ingress_classifier_recognizes_all_complete_carriers)
{
	UT_ASSERT(gcs_block_resource_x_payload_candidate(RESOURCE_X_MSG_IMAGE_OR_GRANT, RESOURCE_X_DECISION_V4_BYTES));
	UT_ASSERT(gcs_block_resource_x_payload_candidate(RESOURCE_X_MSG_IMAGE_OR_GRANT, RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES));
	UT_ASSERT(gcs_block_resource_x_payload_candidate(RESOURCE_X_MSG_BLOCK_TO_N, RESOURCE_X_DECISION_V4_BYTES));
	UT_ASSERT(gcs_block_resource_x_payload_candidate(RESOURCE_X_MSG_BLOCKED_TO_N, RESOURCE_X_WITNESS_V4_BYTES));
	UT_ASSERT(gcs_block_resource_x_payload_candidate(RESOURCE_X_MSG_BLOCKED_TO_N, RESOURCE_X_SOURCE_WITNESS_V4_BYTES));
	UT_ASSERT(!gcs_block_resource_x_payload_candidate(RESOURCE_X_MSG_ASSERT_X, RESOURCE_X_DECISION_V4_BYTES));
}

UT_TEST(test_ingress_disk_grant_retains_decision_without_native_c)
{
	EvidenceFixture f; ResourceXRequesterJoinSnapshot out;
	if (!ingress_begin(&f, 0)) return;
	ingress_deliver(&f.decision);
	UT_ASSERT_EQ(ingress_last_requester, RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ingress_snapshot(&f, &out), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE);
	UT_ASSERT(!(out.flags & (RESOURCE_X_REQUESTER_JOIN_READY | RESOURCE_X_REQUESTER_JOIN_TERMINAL)));
	UT_ASSERT_EQ(out.t_install_us, 0);
}

UT_TEST(test_ingress_remote_image_is_collected_without_a_second_master_frame)
{
	for (int profile = 2; profile <= 5; profile++) {
		EvidenceFixture f; ResourceXRequesterJoinSnapshot out;
		if (!ingress_begin(&f, profile)) continue;
		ingress_deliver(&f.image);
		UT_ASSERT_EQ(ingress_last_requester, RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(ingress_snapshot(&f, &out), RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT(out.flags & RESOURCE_X_REQUESTER_JOIN_HAS_IMAGE);
		UT_ASSERT_EQ((out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE) != 0, profile != 4);
	}
}

UT_TEST(test_ingress_non_source_witness_is_a_requester_receipt_not_a_master_ack)
{
	EvidenceFixture f; ResourceXRequesterJoinSnapshot out;
	if (!ingress_begin(&f, 4)) return;
	ingress_deliver(&f.witness);
	UT_ASSERT_EQ(ingress_master_calls, 0);
	UT_ASSERT_EQ(ingress_requester_calls, 1);
	UT_ASSERT_EQ(ingress_last_requester, RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ingress_snapshot(&f, &out), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(!(out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE));
}

UT_TEST(test_ingress_each_arrival_order_keeps_all_original_obligations)
{
	const int orders[6][3] = {{0,1,2},{0,2,1},{1,0,2},{1,2,0},{2,0,1},{2,1,0}};
	for (int n = 0; n < 6; n++) {
		EvidenceFixture f; ResourceXRequesterJoinSnapshot out;
		const ResourceXDecodedFrame *frames[3];
		if (!ingress_begin(&f, 4)) continue;
		frames[0] = &f.decision; frames[1] = &f.image; frames[2] = &f.witness;
		for (int k = 0; k < 3; k++) ingress_deliver(frames[orders[n][k]]);
		UT_ASSERT_EQ(ingress_last_requester, orders[n][2] == 0
			? RESOURCE_X_APPLY_DUPLICATE : RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(ingress_snapshot(&f, &out), RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT(out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE);
		UT_ASSERT_EQ(ingress_master_calls, 0);
		UT_ASSERT_EQ(out.t_install_us, 0);
	}
}

UT_TEST(test_ingress_receiver_local_connection_is_not_a_sender_number)
{
	EvidenceFixture f; ResourceXRequesterJoinSnapshot out;
	if (!ingress_begin(&f, 4)) return;
	f.image.body.acquire_image.source.decision.master_ingress_generation = 151;
	f.image.body.acquire_image.source.decision.requester_ingress_generation = 161;
	f.witness.body.acquire_witness.decision = f.image.body.acquire_image.source.decision;
	UT_ASSERT(evidence_wire(&f.image)); UT_ASSERT(evidence_wire(&f.witness));
	ingress_deliver(&f.image); ingress_deliver(&f.witness);
	UT_ASSERT_EQ(ingress_last_requester, RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ingress_snapshot(&f, &out), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE);
}

UT_TEST(test_ingress_duplicate_does_not_publish_native_completion)
{
	EvidenceFixture f; ResourceXRequesterJoinSnapshot before, after;
	if (!ingress_begin(&f, 4)) return;
	ingress_deliver(&f.image); ingress_deliver(&f.witness);
	UT_ASSERT_EQ(ingress_snapshot(&f, &before), RESOURCE_X_APPLY_BAD_STATE);
	fake_pcm_clock_us++;
	ingress_deliver(&f.image); ingress_deliver(&f.witness);
	UT_ASSERT_EQ(ingress_last_requester, RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(ingress_snapshot(&f, &after), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
}

UT_TEST(test_ingress_changed_canonical_a_cannot_replace_receipt)
{
	EvidenceFixture f; ResourceXRequesterJoinSnapshot before, after;
	if (!ingress_begin(&f, 4)) return;
	ingress_deliver(&f.witness);
	UT_ASSERT_EQ(ingress_snapshot(&f, &before), RESOURCE_X_APPLY_BAD_STATE);
	f.witness.body.acquire_witness.decision.master_ingress_generation++;
	UT_ASSERT(evidence_wire(&f.witness)); ingress_deliver(&f.witness);
	UT_ASSERT_EQ(ingress_last_requester, RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(ingress_snapshot(&f, &after), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
}

UT_TEST(test_ingress_bad_target_epoch_or_peer_cannot_anchor_a)
{
	for (int n = 0; n < 4; n++) {
		EvidenceFixture f; ResourceXRequesterJoinSnapshot out; ClusterICEnvelope env;
		if (!ingress_begin(&f, 4)) continue;
		env = ingress_envelope(&f.witness);
		if (n == 0) env.dest_node_id = 2;
		if (n == 1) env.epoch++;
		if (n == 2) env.source_node_id = 2;
		if (n == 3) fake_rx_capability_generation = 0;
		UT_ASSERT(ingress_deliver_as(&f.witness, env));
		UT_ASSERT_EQ(ingress_snapshot(&f, &out), RESOURCE_X_APPLY_NOT_FOUND);
	}
}

UT_TEST(test_ingress_closed_target_admission_preserves_original_request)
{
	EvidenceFixture f; ResourceXRequesterJoinSnapshot out;
	if (!ingress_begin(&f, 4)) return;
	master_durable_admission = false; ingress_deliver(&f.witness);
	UT_ASSERT_EQ(ingress_snapshot(&f, &out), RESOURCE_X_APPLY_NOT_FOUND);
	master_durable_admission = true; ingress_deliver(&f.witness);
	UT_ASSERT_EQ(ingress_last_requester, RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ingress_snapshot(&f, &out), RESOURCE_X_APPLY_BAD_STATE);
}

UT_TEST(test_ingress_master_session_drift_does_not_use_legacy_cleanup)
{
	EvidenceFixture f; ResourceXRequesterJoinSnapshot out;
	if (!ingress_begin(&f, 4)) return;
	ingress_session_drift = true; ingress_deliver(&f.witness);
	UT_ASSERT_EQ(ingress_snapshot(&f, &out), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(ingress_cleanup_calls, 0);
}

UT_TEST(test_ingress_local_master_connection_drift_cannot_borrow_a)
{
	EvidenceFixture f; ResourceXRequesterJoinSnapshot out;
	if (!ingress_begin(&f, 4)) return;
	ready_connection_generation = 62; bootstrap_ingress_generation = 0;
	ingress_deliver(&f.witness);
	UT_ASSERT_EQ(ingress_snapshot(&f, &out), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(ingress_cleanup_calls, 0);
}

UT_TEST(test_ingress_source_connection_change_during_boot_sample_preserves_e)
{
	EvidenceFixture f; ResourceXRequesterJoinSnapshot out;
	if (!ingress_begin(&f, 4)) return;
	auth_slot_reads = 0;
	auth_observation_hook = ingress_connection_changes_during_observation;
	ingress_deliver(&f.witness);
	UT_ASSERT_EQ(ingress_last_requester, RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(ingress_snapshot(&f, &out), RESOURCE_X_APPLY_NOT_FOUND);
	/* Only a fresh authenticated copy may contribute; no new REQUEST. */
	auth_observation_hook = ingress_boot_observation;
	fake_rx_capability_generation = 61;
	ingress_deliver(&f.witness);
	UT_ASSERT_EQ(ingress_last_requester, RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ingress_snapshot(&f, &out), RESOURCE_X_APPLY_BAD_STATE);
}

UT_TEST(test_ingress_original_native_identity_and_r4_do_not_drift_with_the_carrier)
{
	for (int n = 0; n < 2; n++) {
		EvidenceFixture f; ResourceXRequesterJoinSnapshot out;
		if (!ingress_begin(&f, 4)) continue;
		if (n == 0) f.witness.body.acquire_witness.decision.episode.r4_generation++;
		else f.witness.body.acquire_witness.decision.episode.reservation_token++;
		UT_ASSERT(evidence_wire(&f.witness)); ingress_deliver(&f.witness);
		UT_ASSERT_EQ(ingress_last_requester, RESOURCE_X_APPLY_STALE);
		UT_ASSERT_EQ(ingress_snapshot(&f, &out), RESOURCE_X_APPLY_NOT_FOUND);
	}
}

UT_TEST(test_ingress_source_ack_without_image_is_not_requester_i)
{
	EvidenceFixture f; ResourceXDecodedFrame ack; ResourceXRequesterJoinSnapshot out;
	if (!ingress_begin(&f, 5)) return;
	ack.kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
	ack.has_acquire_decision = true; ack.common = f.image.common;
	ack.body.acquire_witness = f.image.body.acquire_image.source;
	UT_ASSERT(evidence_wire(&ack)); ingress_deliver(&ack);
	UT_ASSERT_EQ(ingress_snapshot(&f, &out), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(ingress_master_calls, 0);
	UT_ASSERT_EQ(ingress_requester_calls, 0);
}

UT_TEST(test_ingress_master_ack_keeps_its_original_independent_owner)
{
	PublishFixture f; ResourceXDecodedFrame ack;
	if (!publish_start(2, PCM_STATE_X, &f)) return;
	publish_apply(&f);
	ack = publish_decode(f.state->grant_intent.payload, false);
	ack.kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
	ack.common.action_node = 2; ack.common.outcome = RESOURCE_X_OUTCOME_OK;
	ack.body.acquire_witness.decision = f.plan.decision;
	ack.body.acquire_witness.original_ack = ack.common;
	ack.body.acquire_witness.original_ack.authority_generation = ack.common.base_authority_generation;
	ack.body.acquire_witness.original_ack.observed_mode = PCM_STATE_S;
	ack.body.acquire_witness.original_ack.target_mode = PCM_STATE_N;
	UT_ASSERT(evidence_wire(&ack));
	ingress_master_calls = ingress_requester_calls = 0;
	ingress_deliver(&ack);
	UT_ASSERT_EQ(ingress_master_calls, 1);
	UT_ASSERT_EQ(ingress_requester_calls, 0);
	UT_ASSERT_EQ(ingress_last_master, RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.state->requests[1].blocked_holders_bitmap, UINT32_C(1) << 2);
	UT_ASSERT_EQ(f.state->requests[1].final_authority_generation, 0);
}

UT_TEST(test_ingress_coincident_roles_do_not_silently_discard_either_consumer)
{
	EvidenceFixture f; ResourceXDecodedFrame ack;
	if (!ingress_begin(&f, 4)) return;
	ack = f.witness;
	ack.common.logical_assertion.requester_node = 0;
	ack.body.acquire_witness.original_ack.logical_assertion.requester_node = 0;
	ack.body.acquire_witness.decision.episode.requester_incarnation = 31;
	UT_ASSERT(evidence_wire(&ack));
	cluster_node_id = fake_gcs_master_node = 0; ut_master_session = 31;
	ingress_deliver(&ack);
	UT_ASSERT_EQ(ingress_master_calls, 1);
	UT_ASSERT_EQ(ingress_requester_calls, 1);
	/* Neither original owner has this E: routing may not manufacture it. */
	UT_ASSERT(ingress_last_master != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(ingress_last_requester != RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_ingress_malformed_complete_candidate_never_falls_back)
{
	EvidenceFixture f; ClusterICEnvelope env;
	uint8 bytes[RESOURCE_X_WITNESS_V4_BYTES]; uint16 len; ResourceXWireReject reject;
	if (!ingress_begin(&f, 4)) return;
	env = ingress_envelope(&f.witness);
	UT_ASSERT(cluster_resource_x_wire_encode(env.msg_type, &f.witness, bytes, sizeof(bytes), &len, &reject));
	bytes[4] ^= 1;
	UT_ASSERT(gcs_block_try_resource_x_frame(&env, bytes));
	UT_ASSERT(gcs_block_try_resource_x_frame(&env, NULL));
	UT_ASSERT_EQ(ingress_master_calls + ingress_requester_calls, 0);
}

UT_TEST(test_ingress_actual_compatible_source_ack_preserves_s_without_claiming_pi)
{
	PublishFixture f; ResourceXDecodedFrame block, ack, bad;
	ClusterPcmOwnSnapshot prepared = {0}, finished; PGAlignedBlock page = {0};
	ClusterPcmResourceXMasterState *before;
	if (!publish_start(2, PCM_STATE_S, &f)) return;
	publish_apply(&f);
	block = publish_decode(f.state->block_intents[0].payload, true);
	prepared.tag = block.common.logical_assertion.resource;
	prepared.generation = 901; prepared.reservation_token = 77;
	prepared.flags = PCM_OWN_FLAG_REVOKING;
	prepared.pcm_state = PCM_STATE_S; prepared.buffer_type = BUF_TYPE_SCUR;
	prepared.semantic_buf_state = BM_VALID;
	finished = prepared; finished.flags = 0;
	((PageHeader)page.data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)page.data)->pd_upper = BLCKSZ;
	((PageHeader)page.data)->pd_special = BLCKSZ;
	((PageHeader)page.data)->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(&block, 0, 7,
		&prepared, page.data, NULL, 83, 61), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&block, 0, 7,
		&prepared, &finished), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_exact(
		&block.common.logical_assertion, &ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_ack.flags, 0);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_proof.source_carrier_generation, 901);
	before = malloc(sizeof(*before)); UT_ASSERT(before != NULL); if (!before) return;
	memcpy(before, f.state, sizeof(*before));
	bad = ack; bad.body.acquire_witness.original_ack.flags = RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED;
	UT_ASSERT(evidence_wire(&bad));
	ingress_master_calls = ingress_requester_calls = 0;
	ready_ring_full = true; /* Keep the existing transport responsibility. */
	ingress_deliver(&bad);
	UT_ASSERT_EQ(ingress_last_master, RESOURCE_X_APPLY_INVALID);
	UT_ASSERT(memcmp(before, f.state, sizeof(*before)) == 0);
	ingress_deliver(&ack);
	UT_ASSERT_EQ(ingress_last_master, RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ingress_requester_calls, 0);
	UT_ASSERT_EQ(f.state->acquire_ack_connections[0], 61);
	UT_ASSERT_EQ(f.state->requests[1].source_carrier_generation, 901);
	UT_ASSERT_EQ(f.state->requests[1].final_authority_generation, 0);
	UT_ASSERT_EQ(f.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	UT_ASSERT_EQ(f.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	free(before); ready_ring_full = false;
}

int
main(void)
{
	UT_PLAN(19);
	UT_RUN(test_ingress_classifier_recognizes_all_complete_carriers);
	UT_RUN(test_ingress_disk_grant_retains_decision_without_native_c);
	UT_RUN(test_ingress_remote_image_is_collected_without_a_second_master_frame);
	UT_RUN(test_ingress_non_source_witness_is_a_requester_receipt_not_a_master_ack);
	UT_RUN(test_ingress_each_arrival_order_keeps_all_original_obligations);
	UT_RUN(test_ingress_receiver_local_connection_is_not_a_sender_number);
	UT_RUN(test_ingress_duplicate_does_not_publish_native_completion);
	UT_RUN(test_ingress_changed_canonical_a_cannot_replace_receipt);
	UT_RUN(test_ingress_bad_target_epoch_or_peer_cannot_anchor_a);
	UT_RUN(test_ingress_closed_target_admission_preserves_original_request);
	UT_RUN(test_ingress_master_session_drift_does_not_use_legacy_cleanup);
	UT_RUN(test_ingress_local_master_connection_drift_cannot_borrow_a);
	UT_RUN(test_ingress_source_connection_change_during_boot_sample_preserves_e);
	UT_RUN(test_ingress_original_native_identity_and_r4_do_not_drift_with_the_carrier);
	UT_RUN(test_ingress_source_ack_without_image_is_not_requester_i);
	UT_RUN(test_ingress_master_ack_keeps_its_original_independent_owner);
	UT_RUN(test_ingress_coincident_roles_do_not_silently_discard_either_consumer);
	UT_RUN(test_ingress_malformed_complete_candidate_never_falls_back);
	UT_RUN(test_ingress_actual_compatible_source_ack_preserves_s_without_claiming_pi);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
