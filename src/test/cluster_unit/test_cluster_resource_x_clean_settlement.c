/*-------------------------------------------------------------------------
 * test_cluster_resource_x_clean_settlement.c
 *    Actual compatible-source settlement and master receipt owners.
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_resource_x_clean_settlement.c
 * NOTES
 *    Native completion and authenticated peer state are explicit input seams.
 *    This pgrac-original component does not qualify an ordinary native caller.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_resource_x_source_done_fixture.inc"

typedef struct CleanFixture {
	PublishFixture m;
	ResourceXDecodedFrame done, ack, block;
	ClusterPcmOwnSnapshot prepared, finished;
	PGAlignedBlock page;
} CleanFixture;

static uint8
clean_type(ResourceXWireKind k)
{
	return k == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2 ? RESOURCE_X_MSG_BLOCK_TO_N
		: k == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2 ? RESOURCE_X_MSG_BLOCKED_TO_N
		: RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE;
}

static bool
clean_encode(ResourceXDecodedFrame *f)
{
	uint8 b[RESOURCE_X_SOURCE_COMPLETE_V11_BYTES]; uint16 n = 0; ResourceXWireReject r;
	return cluster_resource_x_wire_encode(clean_type(f->kind), f, b, sizeof(b), &n, &r)
		&& n == sizeof(b) && cluster_resource_x_wire_decode(clean_type(f->kind), b, n, f, &r);
}

static ResourceXApplyResult
clean_receive(ResourceXDecodedFrame *f)
{
#ifdef CLEAN_SOURCE_RED
	ResourceXSourceSettlementPlan plan;
	return cluster_pcm_lock_resource_x_source_settlement_prepare_exact(f,
		f->body.acquire_completion.decision.master_node, &plan);
#else
	ClusterICEnvelope env = {0};
	env.msg_type = RESOURCE_X_MSG_BLOCK_TO_N;
	env.source_node_id = f->body.acquire_completion.decision.master_node;
	env.dest_node_id = cluster_node_id;
	env.epoch = 17;
	env.payload_length = f->payload_bytes;
	return gcs_block_resource_x_source_settlement_ingress(&env, f, 61);
#endif
}

static ResourceXDecodedFrame
clean_decode(const uint8 *b, ResourceXWireKind k)
{
	ResourceXDecodedFrame f = {0}; ResourceXWireReject r;
	UT_ASSERT(cluster_resource_x_wire_decode(clean_type(k), b, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &f, &r));
	UT_ASSERT_EQ(f.kind, k); return f;
}

static ResourceXIntentSlot
clean_stage(ResourceXIntentSlot *s)
{
	ResourceXIntentSlot out = {0};
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(s,
		++fake_pcm_clock_us, &out), RESOURCE_X_INTENT_STAGED);
	return out;
}

static void
clean_send(ResourceXIntentSlot *s)
{
	ResourceXIntentSlot out = clean_stage(s);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&out, true, ++fake_pcm_clock_us));
}

static bool
clean_start(CleanFixture *f, int outcome, bool publish, bool sent)
{
	memset(f, 0, sizeof(*f));
	/* Seed a clean shared directory before A exists. This is a fixture input,
	 * not a product PI/writeback completion or native qualification. */
	if (!publish_start(2, PCM_STATE_S, &f->m)) return false;
	pg_atomic_write_u32(&((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->pi_holders_bitmap, 0);
	f->m.plan = decision_plan_read(&f->m.work);
	publish_apply(&f->m);
	ResourceXDecodedFrame grant = publish_decode(f->m.state->grant_intent.payload, false);
	f->done.kind = RESOURCE_X_WIRE_INSTALL_SETTLEMENT;
	f->done.has_acquire_decision = true;
	f->done.payload_bytes = RESOURCE_X_SOURCE_COMPLETE_V11_BYTES;
	f->done.common = grant.common;
	f->done.common.action_node = 1;
	f->done.common.outcome = outcome;
	f->done.common.sender_connection_generation = 71;
	f->done.body.acquire_completion.decision = grant.body.acquire_decision;
	f->done.body.acquire_completion.native_generation = grant.body.acquire_decision.episode.base_generation
		+ (outcome == RESOURCE_X_OUTCOME_OK ? 1 : 0);
	f->done.body.acquire_completion.decision_crc32c = grant.common.semantic_crc32c;
	if (outcome != RESOURCE_X_OUTCOME_QUIET_NO_IMAGE) f->done.body.acquire_completion.page_lsn = 144;
	f->block = publish_decode(f->m.state->block_intents[0].payload, true);
	f->prepared.tag = f->block.common.logical_assertion.resource;
	f->prepared.generation = 901; f->prepared.reservation_token = 77;
	f->prepared.flags = PCM_OWN_FLAG_REVOKING; f->prepared.pcm_state = PCM_STATE_S;
	f->prepared.buffer_type = BUF_TYPE_SCUR; f->prepared.semantic_buf_state = BM_VALID;
	f->finished = f->prepared; f->finished.flags = 0;
	((PageHeader)f->page.data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)f->page.data)->pd_upper = BLCKSZ; ((PageHeader)f->page.data)->pd_special = BLCKSZ;
	((PageHeader)f->page.data)->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	PageXLogRecPtrSet(((PageHeader)f->page.data)->pd_lsn, UINT64_C(144));
	((PageHeader)f->page.data)->pd_block_scn = 236;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(&f->block,
		cluster_node_id, 7, &f->prepared, f->page.data, NULL, 83, 61), RESOURCE_X_APPLY_APPLIED);
	if (publish) UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(
		&f->block, cluster_node_id, 7, &f->prepared, &f->finished), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_exact(&f->block.common.logical_assertion,
		&f->ack), RESOURCE_X_APPLY_APPLIED);
	if (outcome != RESOURCE_X_OUTCOME_QUIET_NO_IMAGE) {
		f->done.body.acquire_completion.page_scn = f->ack.body.acquire_witness.original_proof.page_scn_lsn;
		f->done.body.acquire_completion.page_crc32c = f->ack.body.acquire_witness.original_proof.page_checksum;
	}
	UT_ASSERT(source_done_encode(&f->done));
	if (!source_done_keep(&f->m, &f->done)) return false;
	UT_ASSERT_EQ(source_apply(&f->ack, 0), RESOURCE_X_APPLY_APPLIED);
	source_done_callback(&f->m, true);
	if (publish && sent) { clean_send(&f->m.state->holder_status_intent.slot); clean_send(&f->m.state->holder_image_intent); }
	return true;
}

static bool
clean_arm(CleanFixture *f, ResourceXDecodedFrame *out)
{
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f->m.work), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT_EQ(f->m.state->source_settlement.intent.slot.payload_bytes, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES);
	if (f->m.state->source_settlement.intent.slot.payload_bytes != RESOURCE_X_SOURCE_COMPLETE_V11_BYTES) return false;
	*out = clean_decode(f->m.state->source_settlement.intent.payload, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	return true;
}

UT_TEST(test_clean_existing_families_retain_complete_a_and_all_outcomes)
{
	for (int n = 0; n < 3; n++) {
		CleanFixture f; ResourceXDecodedFrame request, ack;
		int outcome = n == 0 ? RESOURCE_X_OUTCOME_OK : n == 1 ? RESOURCE_X_OUTCOME_QUIET : RESOURCE_X_OUTCOME_QUIET_NO_IMAGE;
		if (!clean_start(&f, outcome, true, true)) return;
		request = f.done; request.kind = RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2; request.common.action_node = 0;
		UT_ASSERT(clean_encode(&request)); if (!clean_encode(&request)) return;
		ack = request; ack.kind = RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2; UT_ASSERT(clean_encode(&ack));
		UT_ASSERT(memcmp(&ack.body.acquire_completion, &f.done.body.acquire_completion, sizeof(ack.body.acquire_completion)) == 0);
	}
}
UT_TEST(test_clean_all_outcomes_finish_source_master_and_notification)
{
	for (int n = 0; n < 3; n++) {
		CleanFixture f; ResourceXDecodedFrame request, ack, consumed; ResourceXIntentSlot m, h, r; ResourceXMasterSnapshot out;
		int outcome = n == 0 ? RESOURCE_X_OUTCOME_OK : n == 1 ? RESOURCE_X_OUTCOME_QUIET : RESOURCE_X_OUTCOME_QUIET_NO_IMAGE;
		if (!clean_start(&f, outcome, true, true) || !clean_arm(&f, &request)) return;
		m = clean_stage(&f.m.state->source_settlement.intent.slot);
		UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
		ack = clean_decode(f.m.state->holder_status_intent.payload, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
		h = clean_stage(&f.m.state->holder_status_intent.slot);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_ack_exact(&ack, 0, &out), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(f.m.state->source_settlement.intent.slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.m.work), RESOURCE_X_APPLY_LOCAL_WAIT);
		UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&m, true, ++fake_pcm_clock_us));
		UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&h, true, ++fake_pcm_clock_us));
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.m.work), RESOURCE_X_APPLY_APPLIED);
		consumed = clean_decode(f.m.state->grant_intent.payload, RESOURCE_X_WIRE_INSTALL_SETTLEMENT);
		UT_ASSERT_EQ(consumed.common.outcome, n == 0 ? RESOURCE_X_OUTCOME_DUPLICATE : n == 1 ? RESOURCE_X_OUTCOME_QUIET_RETIRED : RESOURCE_X_OUTCOME_QUIET_NO_IMAGE_RETIRED);
		UT_ASSERT_EQ(pg_atomic_read_u32(&((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->s_holders_bitmap), n == 0 ? 15 : 13);
		UT_ASSERT_EQ(pg_atomic_read_u32(&((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->pi_holders_bitmap), 0);
		UT_ASSERT_EQ(f.m.state->holder_status.valid, 0); UT_ASSERT_EQ(f.m.state->holder_image.valid, 0);
		UT_ASSERT_EQ(f.m.state->settlement_retry.pending, 0); UT_ASSERT_EQ(f.m.state->deferred_settlement.present, 3);
		r = clean_stage(&f.m.state->grant_intent.slot); UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_intent_complete_exact(&r));
		UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&r, true, ++fake_pcm_clock_us));
		UT_ASSERT_EQ(f.m.state->deferred_settlement.present, 0); UT_ASSERT_EQ(source_done_receive(&f.done), RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT_EQ(f.m.state->acquire_ack_connections[0], 0);
		ResourceXDecodedFrame next = acquire_frame(1, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S), next_ack;
		ResourceXBootstrapWork next_work;
		ResourceXApplyResult next_result;
		uint8 next_bytes[RESOURCE_X_ACQUIRE_V3_BYTES]; uint16 next_len; ResourceXWireReject reject;
		next.common.assertion_sequence++;
		next.body.acquire_intent.base_generation++;
		next.body.acquire_intent.reservation_token++;
		UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, &next, next_bytes, sizeof(next_bytes), &next_len, &reject));
		UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X, next_bytes, next_len, &next, &reject));
		next_result = acquire_deliver(&next);
		UT_ASSERT_EQ(next_result, RESOURCE_X_APPLY_APPLIED); if (next_result != RESOURCE_X_APPLY_APPLIED) return;
		next_work = bootstrap_next_work();
		next_result = cluster_pcm_lock_resource_x_bootstrap_resume_exact(&next_work, &next_ack);
		UT_ASSERT_EQ(next_result, RESOURCE_X_APPLY_APPLIED); if (next_result != RESOURCE_X_APPLY_APPLIED) return;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&next_work, NULL), RESOURCE_X_APPLY_APPLIED);
	}
}
UT_TEST(test_clean_master_does_not_advance_without_source_ack)
{
	CleanFixture f; ResourceXDecodedFrame request;
	if (!clean_start(&f, RESOURCE_X_OUTCOME_OK, true, true) || !clean_arm(&f, &request)) return;
	ClusterPcmResourceXMasterState before = *f.m.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.m.work), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(memcmp(&before, f.m.state, sizeof(before)) == 0);
}
UT_TEST(test_clean_source_waits_for_both_original_send_owners)
{
	CleanFixture f; ResourceXDecodedFrame request;
	if (!clean_start(&f, RESOURCE_X_OUTCOME_OK, true, false) || !clean_arm(&f, &request)) return;
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_LOCAL_WAIT); UT_ASSERT_EQ(f.m.state->settlement_retry.pending, 1);
	clean_send(&f.m.state->holder_image_intent); UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_LOCAL_WAIT);
	clean_send(&f.m.state->holder_status_intent.slot); UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
}
UT_TEST(test_clean_unpublished_native_owner_is_not_retired)
{
	CleanFixture f; ResourceXDecodedFrame request;
	if (!clean_start(&f, RESOURCE_X_OUTCOME_OK, false, false) || !clean_arm(&f, &request)) return;
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT_EQ(f.m.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	UT_ASSERT_EQ(f.m.state->holder_acquire_s_native.flags, PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(&f.block, 0, 7,
		&f.prepared, f.page.data, NULL, 83, 61), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.block, 0, 7,
		&f.prepared, &f.finished), RESOURCE_X_APPLY_APPLIED);
	clean_send(&f.m.state->holder_status_intent.slot); clean_send(&f.m.state->holder_image_intent);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
}
UT_TEST(test_clean_not_admitted_and_failed_ack_keep_the_exact_owner)
{
	CleanFixture f; ResourceXDecodedFrame request; ResourceXIntentSlot a;
	if (!clean_start(&f, RESOURCE_X_OUTCOME_OK, true, true) || !clean_arm(&f, &request)) return;
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	ClusterPcmResourceXSettlementRetry receipt = f.m.state->settlement_retry;
	a = f.m.state->holder_status_intent.slot;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(&a, ++fake_pcm_clock_us), RESOURCE_X_INTENT_NOT_ADMITTED);
	fake_pcm_clock_us += 500000; /* Original bounded transport retry cadence. */
	a = clean_stage(&f.m.state->holder_status_intent.slot);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&a, false, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(f.m.state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT(memcmp(&receipt, &f.m.state->settlement_retry, sizeof(receipt)) == 0);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_DUPLICATE);
}
UT_TEST(test_clean_wrong_image_episode_crc_or_domain_preserves_pair)
{
	for (int n = 0; n < 5; n++) {
		CleanFixture f; ResourceXDecodedFrame request;
		if (!clean_start(&f, RESOURCE_X_OUTCOME_OK, true, true) || !clean_arm(&f, &request)) return;
		if (n == 0) request.body.acquire_completion.page_crc32c++;
		if (n == 1) request.body.acquire_completion.decision.episode.reservation_token++;
		if (n == 2) request.common.semantic_crc32c++;
		if (n == 3) request.common.master_session_incarnation++;
		if (n == 4) request.body.acquire_completion.decision_crc32c++;
		if (n != 2) UT_ASSERT(clean_encode(&request));
		ClusterPcmResourceXMasterState before = *f.m.state;
		ResourceXApplyResult got = clean_receive(&request);
		UT_ASSERT(got != RESOURCE_X_APPLY_APPLIED && got != RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT(memcmp(&before, f.m.state, sizeof(before)) == 0);
	}
}
UT_TEST(test_clean_forged_source_ack_does_not_close_pending)
{
	CleanFixture f; ResourceXDecodedFrame request, ack; ResourceXMasterSnapshot out;
	if (!clean_start(&f, RESOURCE_X_OUTCOME_OK, true, true) || !clean_arm(&f, &request)) return;
	ack = request; ack.kind = RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2; ack.body.acquire_completion.page_scn++;
	UT_ASSERT(clean_encode(&ack)); ClusterPcmResourceXMasterState before = *f.m.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_ack_exact(&ack, 0, &out), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&before, f.m.state, sizeof(before)) == 0);
}
UT_TEST(test_clean_semantic_ack_keeps_staged_master_send_until_callback)
{
	CleanFixture f; ResourceXDecodedFrame request, ack; ResourceXMasterSnapshot out; ResourceXIntentSlot m;
	if (!clean_start(&f, RESOURCE_X_OUTCOME_OK, true, true) || !clean_arm(&f, &request)) return;
	m = clean_stage(&f.m.state->source_settlement.intent.slot); UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	ack = clean_decode(f.m.state->holder_status_intent.payload, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_ack_exact(&ack, 0, &out), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.m.work), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_intent_complete_exact(&m));
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&m, false, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(f.m.state->source_settlement.intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
}
UT_TEST(test_clean_source_pi_duty_cannot_use_transport_retirement)
{
	CleanFixture f; if (!clean_start(&f, RESOURCE_X_OUTCOME_OK, true, true)) return;
	pg_atomic_write_u32(&((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->pi_holders_bitmap, 1); ClusterPcmResourceXMasterState before = *f.m.state;
	UT_ASSERT(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.m.work) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, f.m.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(f.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
}
UT_TEST(test_clean_retired_source_rejects_late_forward_and_prepare)
{
	CleanFixture f; ResourceXDecodedFrame request;
	ClusterPcmResourceXMasterState before;
	if (!clean_start(&f, RESOURCE_X_OUTCOME_OK, true, true) || !clean_arm(&f, &request)) return;
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	clean_send(&f.m.state->holder_status_intent.slot);
	before = *f.m.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f.block, 0, 83, 61), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&before, f.m.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(&f.block, 0, 7,
		&f.prepared, f.page.data, NULL, 83, 61), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&before, f.m.state, sizeof(before)) == 0);
}
UT_TEST(test_clean_source_producer_and_pi_reference_keep_responsibility)
{
	for (int n = 0; n < 2; n++) {
		CleanFixture f; ResourceXDecodedFrame request;
		if (!clean_start(&f, RESOURCE_X_OUTCOME_OK, true, true) || !clean_arm(&f, &request)) return;
		if (n == 0) ((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->resource_x_source_drain.r4_record_generation = 83;
		else f.m.state->holder_pi_first.source_flags = 1;
		UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_LOCAL_WAIT);
		UT_ASSERT_EQ(f.m.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
		UT_ASSERT_EQ(f.m.state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	}
}
UT_TEST(test_clean_existing_lms_probe_and_snapshot_offer_each_owner)
{
	CleanFixture f; ResourceXDecodedFrame request, ack;
	uint32 cursor = RESOURCE_X_PROTOCOL_NODE_LIMIT + 4;
	ResourceXIntentSlot slot = {0}; ResourceXMasterSnapshot out;
	uint8 bytes[RESOURCE_X_SOURCE_COMPLETE_V11_BYTES];
	if (!clean_start(&f, RESOURCE_X_OUTCOME_OK, true, true) || !clean_arm(&f, &request)) return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_ready_intent_probe_exact(&request.common.logical_assertion.resource, &cursor, &slot), RESOURCE_X_INTENT_PROBE_FOUND);
	UT_ASSERT_EQ(slot.kind, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_intent_snapshot_exact(&request.common.logical_assertion, &slot, bytes, sizeof(bytes)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	cursor = RESOURCE_X_PROTOCOL_NODE_LIMIT;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_ready_intent_probe_exact(&request.common.logical_assertion.resource, &cursor, &slot), RESOURCE_X_INTENT_PROBE_FOUND);
	UT_ASSERT_EQ(slot.kind, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(&request.common.logical_assertion, &slot, bytes, sizeof(bytes)), RESOURCE_X_APPLY_APPLIED);
	ack = clean_decode(bytes, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_settlement_ack_exact(&ack, 0, &out), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.m.work), RESOURCE_X_APPLY_APPLIED);
	cursor = RESOURCE_X_PROTOCOL_NODE_LIMIT + 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_ready_intent_probe_exact(&request.common.logical_assertion.resource, &cursor, &slot), RESOURCE_X_INTENT_PROBE_FOUND);
	UT_ASSERT_EQ(slot.kind, RESOURCE_X_WIRE_INSTALL_SETTLEMENT);
	UT_ASSERT_EQ(slot.payload_bytes, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES);
}
UT_TEST(test_clean_remote_master_uses_original_source_and_ack_owners)
{
	CleanFixture f; ResourceXDecodedFrame grant, request;
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES]; uint16 length; ResourceXWireReject reject;
	if (!clean_start(&f, RESOURCE_X_OUTCOME_OK, true, true)) return;
	/* A is an authenticated remote peer input; native bytes remain the same
	 * explicit source seam. No remote master is simulated by a local grant. */
	f.block.body.acquire_decision.master_node = 2;
	f.block.common.master_session_incarnation = 777;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, &f.block, bytes, sizeof(bytes), &length, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N, bytes, length, &f.block, &reject));
	grant = f.block; grant.kind = RESOURCE_X_WIRE_AUTHORITY_GRANT; grant.common.action_node = 1;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &grant, bytes, sizeof(bytes), &length, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, length, &grant, &reject));
	request = f.done; request.kind = RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2;
	request.common = f.block.common; request.common.outcome = RESOURCE_X_OUTCOME_OK;
	request.body.acquire_completion.decision = f.block.body.acquire_decision;
	request.body.acquire_completion.decision_crc32c = grant.common.semantic_crc32c;
	UT_ASSERT(clean_encode(&request));
	acquire_reset(); fake_gcs_master_node = 2;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(&f.block, 2, 7,
		&f.prepared, f.page.data, NULL, 83, 61), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.block, 2, 7,
		&f.prepared, &f.finished), RESOURCE_X_APPLY_APPLIED);
	clean_send(&f.m.state->holder_status_intent.slot); clean_send(&f.m.state->holder_image_intent);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.m.state->holder_status_intent.slot.destination_node, 2);
	UT_ASSERT_EQ(f.m.state->holder_status.valid, 0);
	clean_send(&f.m.state->holder_status_intent.slot);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(f.m.state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	clean_send(&f.m.state->holder_status_intent.slot);
	ResourceXDecodedFrame old_block = f.block;
	/* A later A from another requester replaces the bounded completion
	 * history. Its lower requester-local attempt must not revive old A. */
	f.block.common.logical_assertion.requester_node = 3;
	f.block.common.assertion_sequence = 1;
	f.block.common.base_authority_generation = f.block.common.authority_generation++;
	f.block.body.acquire_decision.enqueue_order++;
	f.block.body.acquire_decision.source_service_generation++;
	f.block.body.acquire_decision.episode.requester_incarnation = f.block.body.acquire_decision.holders[3].boot_incarnation;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, &f.block, bytes, sizeof(bytes), &length, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N, bytes, length, &f.block, &reject));
	grant = f.block; grant.kind = RESOURCE_X_WIRE_AUTHORITY_GRANT; grant.common.action_node = 3;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &grant, bytes, sizeof(bytes), &length, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, length, &grant, &reject));
	request.common = f.block.common; request.common.outcome = RESOURCE_X_OUTCOME_OK;
	request.body.acquire_completion.decision = f.block.body.acquire_decision;
	request.body.acquire_completion.decision_crc32c = grant.common.semantic_crc32c;
	UT_ASSERT(clean_encode(&request));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(&f.block, 2, 7,
		&f.prepared, f.page.data, NULL, 83, 61), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.block, 2, 7,
		&f.prepared, &f.finished), RESOURCE_X_APPLY_APPLIED);
	clean_send(&f.m.state->holder_status_intent.slot); clean_send(&f.m.state->holder_image_intent);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	clean_send(&f.m.state->holder_status_intent.slot);
	ClusterPcmResourceXMasterState before = *f.m.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&old_block, 2, 83, 61), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&before, f.m.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(&old_block, 2, 7,
		&f.prepared, f.page.data, NULL, 83, 61), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&before, f.m.state, sizeof(before)) == 0);
}
UT_TEST(test_clean_master_send_cannot_borrow_another_owner_node)
{
	CleanFixture f; ResourceXDecodedFrame request; ResourceXIntentSlot slot = {0};
	uint32 cursor = RESOURCE_X_PROTOCOL_NODE_LIMIT + 4;
	if (!clean_start(&f, RESOURCE_X_OUTCOME_OK, true, true) || !clean_arm(&f, &request)) return;
	f.m.state->source_settlement.intent.slot.body.owner_node = 3;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_ready_intent_probe_exact(&request.common.logical_assertion.resource,
		&cursor, &slot), RESOURCE_X_INTENT_PROBE_CORRUPT);
}
UT_TEST(test_clean_top_level_classifies_only_complete_source_frames)
{
	UT_ASSERT(gcs_block_resource_x_payload_candidate(RESOURCE_X_MSG_BLOCK_TO_N, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES));
	UT_ASSERT(gcs_block_resource_x_payload_candidate(RESOURCE_X_MSG_BLOCKED_TO_N, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES));
	for (int n = -1; n <= 1; n += 2) {
		UT_ASSERT(!gcs_block_resource_x_payload_candidate(RESOURCE_X_MSG_BLOCK_TO_N, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES + n));
		UT_ASSERT(!gcs_block_resource_x_payload_candidate(RESOURCE_X_MSG_BLOCKED_TO_N, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES + n));
	}
}
int
main(void)
{
	UT_PLAN(16);
	UT_RUN(test_clean_existing_families_retain_complete_a_and_all_outcomes);
	UT_RUN(test_clean_all_outcomes_finish_source_master_and_notification);
	UT_RUN(test_clean_master_does_not_advance_without_source_ack);
	UT_RUN(test_clean_source_waits_for_both_original_send_owners);
	UT_RUN(test_clean_unpublished_native_owner_is_not_retired);
	UT_RUN(test_clean_not_admitted_and_failed_ack_keep_the_exact_owner);
	UT_RUN(test_clean_wrong_image_episode_crc_or_domain_preserves_pair);
	UT_RUN(test_clean_forged_source_ack_does_not_close_pending);
	UT_RUN(test_clean_semantic_ack_keeps_staged_master_send_until_callback);
	UT_RUN(test_clean_source_pi_duty_cannot_use_transport_retirement);
	UT_RUN(test_clean_retired_source_rejects_late_forward_and_prepare);
	UT_RUN(test_clean_source_producer_and_pi_reference_keep_responsibility);
	UT_RUN(test_clean_existing_lms_probe_and_snapshot_offer_each_owner);
	UT_RUN(test_clean_remote_master_uses_original_source_and_ack_owners);
	UT_RUN(test_clean_master_send_cannot_borrow_another_owner_node);
	UT_RUN(test_clean_top_level_classifies_only_complete_source_frames);
	UT_DONE(); return ut_failed_count > 0 ? 1 : 0;
}
