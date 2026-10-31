/* Actual master publication, independent holder ACKs, DONE, LMS and callback
 * owners. Native C/revoke and authenticated peer/process observations remain
 * explicit inputs. This does not qualify requester installation or use. */
#include "test_cluster_resource_x_source_done_fixture.inc"
#include "test_cluster_resource_x_consumed_ring.inc"
ClusterLmsEnqueueResult local_actual_lms_enqueue(int worker, const ResourceXIntentSlot *intent,
	uint32 connection, uint64 deadline);
#define cluster_lms_outbound_enqueue_resource_x_intent local_actual_lms_enqueue
#include "test_cluster_resource_x_consumed_lms.inc"
#undef cluster_lms_outbound_enqueue_resource_x_intent

static ClusterLmsOutboundSharedState local_ring_shared;
static LWLock local_ring_locks[CLUSTER_LMS_MAX_WORKERS];

static void
local_ring_reset(void)
{
	memset(&local_ring_shared, 0, sizeof(local_ring_shared));
	cluster_lms_outbound_shared = &local_ring_shared;
	cluster_lms_outbound_rings = local_ring_shared.rings;
	pg_atomic_init_u64(&local_ring_shared.resource_x_transport_mutation_sequence, 1);
	for (int n = 0; n < CLUSTER_LMS_MAX_WORKERS; n++) cluster_lms_outbound_locks[n] = &local_ring_locks[n];
}

/* The original fixture's metadata locator, after real canonical admission. */
static bool
local_start(PcmState mode, PcmState target, unsigned others, uint8 outcome,
	PublishFixture *f, ResourceXDecodedFrame *done)
{
	ClusterPcmResourceXMasterRequest canonical = {0};
	ResourceXDecodedFrame grant; ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES]; uint16 len;
	Size offset = offsetof(ClusterPcmResourceXMasterState, requests[0]);
	unsigned matches = 0;
	memset(f, 0, sizeof(*f));
	provenance_grant(0);
	if (mode == PCM_STATE_S) service_directory_transition(PCM_TRANS_X_TO_S_DOWNGRADE, 0);
	for (unsigned n = 0; n < others; n++)
		if (decision_plan_inject_shared_holder(n + 2, 102 + n) == NULL) return false;
	f->request = acquire_frame(0, RESOURCE_X_ACQUIRE_RESIDENT, mode, target);
	f->request.common.assertion_sequence++;
	if (!cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, &f->request, bytes, sizeof(bytes), &len, &reject)
		|| !cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X, bytes, len, &f->request, &reject)
		|| !acquire_canonical_start(&f->request, &f->work)) return false;
	f->plan = decision_plan_read(&f->work);
	/* R's original native C covers its own mode conversion, not a forwarded
	 * holder service to itself. Only incompatible other holders owe ACKs. */
	UT_ASSERT_EQ(f->plan.decision.required_holders_bitmap & UINT32_C(1), 0);
	UT_ASSERT_EQ(f->plan.service_holders_bitmap & UINT32_C(1), 0);
	canonical.base_authority_generation = f->plan.input.canonical_base_generation;
	canonical.resource_formation = f->work.request.resource_formation;
	canonical.master_session_incarnation = f->work.request.master_session_incarnation;
	canonical.assertion_sequence = f->work.request.assertion_sequence;
	canonical.enqueue_order = f->work.enqueue_order;
	canonical.sender_connection_generation = f->work.request.sender_connection_generation;
	canonical.source_node = -1; canonical.phase = RESOURCE_X_MASTER_QUEUED;
	for (Size n = offset; n + sizeof(canonical) <= fake_pcm_header_requested_size; n += 8)
		if (memcmp(fake_pcm_header.data + n, &canonical, sizeof(canonical)) == 0) {
			f->state = (void *)(fake_pcm_header.data + n - offset); matches++;
		}
	UT_ASSERT_EQ(matches, 1); if (matches != 1) return false;
	publish_apply(f); grant = publish_decode(f->state->grant_intent.payload, false);
	UT_ASSERT_EQ(grant.body.acquire_decision.source_kind, RESOURCE_X_ACQUIRE_SOURCE_LOCAL_CURRENT);
	UT_ASSERT_EQ(grant.body.acquire_decision.source_node, 0);
	memset(done, 0, sizeof(*done));
	done->kind = RESOURCE_X_WIRE_INSTALL_SETTLEMENT;
	done->payload_bytes = RESOURCE_X_SOURCE_COMPLETE_V11_BYTES;
	done->has_acquire_decision = true; done->common = grant.common;
	done->common.outcome = outcome; done->common.sender_connection_generation = 71;
	done->body.acquire_completion.decision = grant.body.acquire_decision;
	done->body.acquire_completion.native_generation = grant.body.acquire_decision.episode.base_generation
		+ (outcome == RESOURCE_X_OUTCOME_OK ? 1 : 0);
	done->body.acquire_completion.decision_crc32c = grant.common.semantic_crc32c;
	if (outcome != RESOURCE_X_OUTCOME_QUIET_NO_IMAGE) {
		done->body.acquire_completion.page_lsn = 201;
		done->body.acquire_completion.page_scn = 12340;
		done->body.acquire_completion.page_crc32c = 0x87456123;
	}
	return source_done_encode(done);
}

static ResourceXIntentSlot
local_stage(ResourceXIntentSlot *slot)
{
	ResourceXIntentSlot stage = {0};
	fake_pcm_clock_us += 1000000;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
		slot, ++fake_pcm_clock_us, &stage), RESOURCE_X_INTENT_STAGED);
	return stage;
}

static void
local_sent(ResourceXIntentSlot *slot)
{
	ResourceXIntentSlot stage = local_stage(slot);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&stage, true, ++fake_pcm_clock_us));
}

static void
local_grant_sent(PublishFixture *f)
{
	local_sent(&f->state->grant_intent.slot);
}

static void
local_acks(PublishFixture *f, bool callbacks)
{
	for (int node = 0; node < RESOURCE_X_PROTOCOL_NODE_LIMIT; node++)
		if (f->plan.decision.required_holders_bitmap & (UINT32_C(1) << node)) {
			ResourceXDecodedFrame ack = source_ack(f, node);
			UT_ASSERT_EQ(source_apply(&ack, node), RESOURCE_X_APPLY_APPLIED);
			if (callbacks) local_sent(&f->state->block_intents[node].slot);
		}
}

static bool
local_consumed(PublishFixture *f, ResourceXDecodedFrame *done)
{
	ResourceXApplyResult result = source_done_receive(done);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f->state->deferred_settlement.present, 3);
	return result == RESOURCE_X_APPLY_APPLIED && f->state->deferred_settlement.present == 3;
}

static void
local_directory(PublishFixture *f, ResourceXDecodedFrame *done, uint64 old_grant)
{
	const ResourceXDecodedAcquireDecision *a = &done->body.acquire_completion.decision;
	struct StopPcmEntryLayout *directory = (void *)fake_pcm_entries.data[0];
	static const ClusterPcmResourceXMasterRequest empty_request;
	static const ClusterPcmResourceXSourceSettlement empty_settlement;
	PcmState mode = done->common.outcome == RESOURCE_X_OUTCOME_OK ? done->common.target_mode : a->directory_mode;
	uint32 shared = mode == PCM_STATE_S ? a->holders_bitmap & ~a->required_holders_bitmap : 0;
	uint32 pi = a->pi_holders_bitmap | (done->common.outcome == RESOURCE_X_OUTCOME_OK
		&& a->directory_mode == PCM_STATE_X && mode == PCM_STATE_S ? 1 : 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&directory->master_state), mode);
	UT_ASSERT_EQ(pg_atomic_read_u32(&directory->s_holders_bitmap), shared);
	UT_ASSERT_EQ(directory->x_holder_node, mode == PCM_STATE_X ? 0 : -1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&directory->pi_holders_bitmap), pi);
	UT_ASSERT_EQ(f->state->holder_grants[0].grant_generation,
		done->common.outcome == RESOURCE_X_OUTCOME_OK ? done->common.authority_generation : old_grant);
	UT_ASSERT(memcmp(&f->state->requests[0], &empty_request, sizeof(empty_request)) == 0);
	UT_ASSERT(memcmp(&f->state->source_settlement, &empty_settlement, sizeof(empty_settlement)) == 0);
	UT_ASSERT_EQ(f->state->bootstrap_priority[0].state, RESOURCE_X_BOOTSTRAP_PRIORITY_EMPTY);
	UT_ASSERT_EQ(f->state->bootstrap_receipts[0].highest_attempt_floor, done->common.assertion_sequence);
	for (int node = 0; node < RESOURCE_X_PROTOCOL_NODE_LIMIT; node++) UT_ASSERT_EQ(f->state->acquire_ack_connections[node], 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0); UT_ASSERT_EQ(master_durable_admission_depth, 0);
}

#define LOCAL_REQUIRE(call, expected) do { ResourceXApplyResult got = (call); \
	UT_ASSERT_EQ(got, (expected)); if (got != (expected)) return; } while (0)

UT_TEST(test_local_done_success_updates_only_the_original_requester)
{
	for (PcmState mode = PCM_STATE_S; mode <= PCM_STATE_X; mode++)
		for (PcmState target = PCM_STATE_S; target <= PCM_STATE_X; target++) {
			PublishFixture f; ResourceXDecodedFrame done;
			UT_ASSERT(local_start(mode, target, mode == PCM_STATE_S ? 2 : 0, RESOURCE_X_OUTCOME_OK, &f, &done));
			local_grant_sent(&f); local_acks(&f, true);
			if (!local_consumed(&f, &done)) return;
			local_directory(&f, &done, 2);
			if (mode == PCM_STATE_S && target == PCM_STATE_S) {
				UT_ASSERT_EQ(f.state->holder_grants[2].grant_generation, f.plan.decision.holders[2].grant_generation);
				UT_ASSERT_EQ(f.state->holder_grants[3].grant_generation, f.plan.decision.holders[3].grant_generation);
			}
		}
}
UT_TEST(test_local_done_quiet_preserves_old_grant_and_records_actual_revocations)
{
	for (PcmState mode = PCM_STATE_S; mode <= PCM_STATE_X; mode++)
		for (PcmState target = PCM_STATE_S; target <= PCM_STATE_X; target++)
			for (int unarmed = 0; unarmed < 2; unarmed++) {
				PublishFixture f; ResourceXDecodedFrame done;
				UT_ASSERT(local_start(mode, target, mode == PCM_STATE_S ? 2 : 0,
					unarmed ? RESOURCE_X_OUTCOME_QUIET_NO_IMAGE : RESOURCE_X_OUTCOME_QUIET, &f, &done));
				local_grant_sent(&f); local_acks(&f, true);
				if (!local_consumed(&f, &done)) return;
				local_directory(&f, &done, 2);
			}
}
UT_TEST(test_local_done_waits_for_every_independent_holder_ACK)
{
	PublishFixture f; ResourceXDecodedFrame done, ack;
	UT_ASSERT(local_start(PCM_STATE_S, PCM_STATE_X, 2, RESOURCE_X_OUTCOME_OK, &f, &done));
	local_grant_sent(&f);
	for (int node = 2; node <= 3; node++) local_sent(&f.state->block_intents[node].slot);
	LOCAL_REQUIRE(source_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 4);
	LOCAL_REQUIRE(cluster_gcs_block_resource_x_bootstrap_tick(&f.work), RESOURCE_X_APPLY_LOCAL_WAIT);
	for (int node = 3; node >= 2; node--) {
		ack = source_ack(&f, node); LOCAL_REQUIRE(source_apply(&ack, node), RESOURCE_X_APPLY_APPLIED);
		LOCAL_REQUIRE(cluster_gcs_block_resource_x_bootstrap_tick(&f.work),
			node == 3 ? RESOURCE_X_APPLY_LOCAL_WAIT : RESOURCE_X_APPLY_APPLIED);
	}
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 3); local_directory(&f, &done, 2);
}
UT_TEST(test_local_done_waits_for_grant_and_all_original_transport_callbacks)
{
	PublishFixture f; ResourceXDecodedFrame done; ResourceXIntentSlot grant, block[2];
	UT_ASSERT(local_start(PCM_STATE_S, PCM_STATE_X, 2, RESOURCE_X_OUTCOME_OK, &f, &done));
	grant = local_stage(&f.state->grant_intent.slot); local_acks(&f, false);
	for (int node = 2; node <= 3; node++) block[node-2] = local_stage(&f.state->block_intents[node].slot);
	LOCAL_REQUIRE(source_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
	LOCAL_REQUIRE(cluster_gcs_block_resource_x_bootstrap_tick(&f.work), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&grant, true, ++fake_pcm_clock_us));
	for (int n = 0; n < 2; n++) {
		LOCAL_REQUIRE(cluster_gcs_block_resource_x_bootstrap_tick(&f.work), RESOURCE_X_APPLY_LOCAL_WAIT);
		UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&block[n], true, ++fake_pcm_clock_us));
	}
	LOCAL_REQUIRE(cluster_gcs_block_resource_x_bootstrap_tick(&f.work), RESOURCE_X_APPLY_APPLIED);
	local_directory(&f, &done, 2);
}
UT_TEST(test_local_done_rejects_wrong_E_A_domain_and_native_generation)
{
	for (int n = 0; n < 12; n++) {
		PublishFixture f; ResourceXDecodedFrame done;
		ResourceXDecodedAcquireDecision *a = &done.body.acquire_completion.decision;
		UT_ASSERT(local_start(PCM_STATE_X, PCM_STATE_X, 0, RESOURCE_X_OUTCOME_OK, &f, &done));
		local_grant_sent(&f);
		if (n == 0) a->resource_binding++;
		if (n == 1) a->enqueue_order++;
		if (n == 2) a->episode.reservation_token++;
		if (n == 3) { a->episode.base_generation++; done.body.acquire_completion.native_generation++; }
		if (n == 4) a->episode.backend_generation++;
		if (n == 5) a->requester_ingress_generation++;
		if (n == 6) a->master_ingress_generation++;
		if (n == 7) a->episode.r4_generation++;
		if (n == 8) done.body.acquire_completion.decision_crc32c++;
		if (n == 9) done.body.acquire_completion.write_io_generation |= 1;
		if (n == 10) done.body.acquire_completion.native_generation++;
		if (n == 11) done.common.master_session_incarnation++;
		if (n == 9 || n == 10) { UT_ASSERT(!source_done_encode(&done)); continue; }
		UT_ASSERT(source_done_encode(&done)); source_done_unchanged(&f, &done);
	}
}
UT_TEST(test_local_done_duplicate_keeps_one_consumed_notice_and_old_grant)
{
	PublishFixture f; ResourceXDecodedFrame done; ClusterPcmResourceXMasterState before;
	UT_ASSERT(local_start(PCM_STATE_X, PCM_STATE_S, 0, RESOURCE_X_OUTCOME_QUIET_NO_IMAGE, &f, &done));
	local_grant_sent(&f); if (!local_consumed(&f, &done)) return;
	before = *f.state; done.common.sender_connection_generation++;
	UT_ASSERT(source_done_encode(&done)); LOCAL_REQUIRE(source_done_receive(&done), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0); local_directory(&f, &done, 2);
	local_grant_sent(&f); before = *f.state;
	LOCAL_REQUIRE(source_done_receive(&done), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}
UT_TEST(test_local_done_NOT_ADMITTED_keeps_original_notice_and_receipt)
{
	PublishFixture f; ResourceXDecodedFrame done; ClusterPcmResourceXDeferredSettlement receipt;
	ClusterPcmResourceXGrantIntent notice;
	UT_ASSERT(local_start(PCM_STATE_S, PCM_STATE_X, 0, RESOURCE_X_OUTCOME_OK, &f, &done));
	local_grant_sent(&f); if (!local_consumed(&f, &done)) return;
	receipt = f.state->deferred_settlement; notice = f.state->grant_intent;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(
		&notice.slot, ++fake_pcm_clock_us), RESOURCE_X_INTENT_NOT_ADMITTED);
	UT_ASSERT(memcmp(&receipt, &f.state->deferred_settlement, sizeof(receipt)) == 0);
	UT_ASSERT(memcmp(notice.payload, f.state->grant_intent.payload, sizeof(notice.payload)) == 0);
	UT_ASSERT_EQ(f.state->grant_intent.slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
}
UT_TEST(test_local_done_real_LMS_takes_reverse_notice_until_actual_callback)
{
	PublishFixture f; ResourceXDecodedFrame done; ResourceXIntentSlot slot;
	UT_ASSERT(local_start(PCM_STATE_X, PCM_STATE_X, 0, RESOURCE_X_OUTCOME_OK, &f, &done));
	local_grant_sent(&f); if (!local_consumed(&f, &done)) return;
	local_ring_reset();
	UT_ASSERT_EQ(local_actual_lms_enqueue(0, &f.state->grant_intent.slot, 61, 9999999), CLUSTER_LMS_ENQUEUE_ADMITTED);
	UT_ASSERT_EQ(local_ring_shared.rings[0].count, 1);
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 3);
	memcpy(&slot, local_ring_shared.rings[0].ring[0].payload, sizeof(slot));
	UT_ASSERT_EQ(slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
	UT_ASSERT(memcmp(&slot, &f.state->grant_intent.slot, sizeof(slot)) == 0);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 0); local_directory(&f, &done, 2);
}
UT_TEST(test_local_done_callback_failure_rearms_without_consuming_again)
{
	PublishFixture f; ResourceXDecodedFrame done; ResourceXIntentSlot a, b;
	UT_ASSERT(local_start(PCM_STATE_S, PCM_STATE_X, 1, RESOURCE_X_OUTCOME_OK, &f, &done));
	local_grant_sent(&f); local_acks(&f, true); if (!local_consumed(&f, &done)) return;
	a = local_stage(&f.state->grant_intent.slot);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&a, false, ++fake_pcm_clock_us));
	b = local_stage(&f.state->grant_intent.slot); UT_ASSERT(b.send_episode > a.send_episode);
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&a, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 3);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&b, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 0); local_directory(&f, &done, 2);
}
UT_TEST(test_local_done_real_LMS_validates_retired_notice_without_accepting_requester_DONE)
{
	const uint8 outcomes[] = {RESOURCE_X_OUTCOME_OK, RESOURCE_X_OUTCOME_QUIET,
		RESOURCE_X_OUTCOME_QUIET_NO_IMAGE};
	for (unsigned n = 0; n < lengthof(outcomes); n++) {
		PublishFixture f; ResourceXDecodedFrame done; ResourceXIntentSlot wrong;
		ResourceXWireReject reject; uint16 len;
		uint8 bytes[RESOURCE_X_SOURCE_COMPLETE_V11_BYTES];
		UT_ASSERT(local_start(PCM_STATE_S, PCM_STATE_X, 0, outcomes[n], &f, &done));
		local_grant_sent(&f); if (!local_consumed(&f, &done)) return;
		UT_ASSERT(lms_outbound_resource_x_intent_valid(&f.state->grant_intent.slot));
		UT_ASSERT(lms_outbound_resource_x_intent_payload_valid(
			&f.state->grant_intent.slot, f.state->grant_intent.payload));
		wrong = f.state->grant_intent.slot; wrong.destination_node = 1;
		UT_ASSERT(!lms_outbound_resource_x_intent_valid(&wrong));
		UT_ASSERT(!lms_outbound_resource_x_intent_payload_valid(&wrong, f.state->grant_intent.payload));
		wrong = f.state->grant_intent.slot; wrong.authority_generation++;
		UT_ASSERT(!lms_outbound_resource_x_intent_valid(&wrong));
		UT_ASSERT(!lms_outbound_resource_x_intent_payload_valid(&wrong, f.state->grant_intent.payload));
		UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
			&done, bytes, sizeof(bytes), &len, &reject));
		UT_ASSERT_EQ(len, sizeof(bytes));
		UT_ASSERT(!lms_outbound_resource_x_intent_payload_valid(&f.state->grant_intent.slot, bytes));
		memcpy(bytes, f.state->grant_intent.payload, sizeof(bytes)); bytes[0] = 0;
		UT_ASSERT(!lms_outbound_resource_x_intent_payload_valid(&f.state->grant_intent.slot, bytes));
	}
}
UT_TEST(test_local_done_full_transition_budget_refuses_before_partial_revoke)
{
	PublishFixture f; ResourceXDecodedFrame done;
	struct StopPcmEntryLayout *directory;
	UT_ASSERT(local_start(PCM_STATE_S, PCM_STATE_X, 2, RESOURCE_X_OUTCOME_OK, &f, &done));
	local_grant_sent(&f); local_acks(&f, true); directory = (void *)fake_pcm_entries.data[0];
	pg_atomic_write_u64(&directory->transition_count_local, UINT64_MAX - 3);
	f.state->requests[0].head_transition_generation = UINT64_MAX - 3;
	source_done_unchanged(&f, &done);
	UT_ASSERT_EQ(pg_atomic_read_u32(&directory->s_holders_bitmap), 13);
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 0);
}
UT_TEST(test_local_done_corrupt_independent_ACK_cannot_be_replaced_by_DONE)
{
	PublishFixture f; ResourceXDecodedFrame done;
	UT_ASSERT(local_start(PCM_STATE_S, PCM_STATE_X, 1, RESOURCE_X_OUTCOME_OK, &f, &done));
	local_grant_sent(&f); local_acks(&f, true);
	f.state->acquire_ack_connections[2] = 0;
	source_done_unchanged(&f, &done);
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 0);
}
UT_TEST(test_local_done_late_receipt_and_callback_cannot_clear_successor)
{
	PublishFixture f; ResourceXDecodedFrame done, next; ResourceXIntentSlot old;
	ClusterPcmResourceXMasterState before; ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES]; uint16 len;
	UT_ASSERT(local_start(PCM_STATE_X, PCM_STATE_X, 0, RESOURCE_X_OUTCOME_OK, &f, &done));
	local_grant_sent(&f); if (!local_consumed(&f, &done)) return;
	old = local_stage(&f.state->grant_intent.slot);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&old, true, ++fake_pcm_clock_us));
	next = f.request; next.common.assertion_sequence++; next.body.acquire_intent.base_generation++;
	next.body.acquire_intent.reservation_token++;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, &next, bytes, sizeof(bytes), &len, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X, bytes, len, &next, &reject));
	UT_ASSERT(acquire_canonical_start(&next, &f.work)); f.plan = decision_plan_read(&f.work); publish_apply(&f);
	before = *f.state; source_done_unchanged(&f, &done);
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&old, true, ++fake_pcm_clock_us));
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}

int main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(13);
	UT_RUN(test_local_done_success_updates_only_the_original_requester);
	UT_RUN(test_local_done_quiet_preserves_old_grant_and_records_actual_revocations);
	UT_RUN(test_local_done_waits_for_every_independent_holder_ACK);
	UT_RUN(test_local_done_waits_for_grant_and_all_original_transport_callbacks);
	UT_RUN(test_local_done_rejects_wrong_E_A_domain_and_native_generation);
	UT_RUN(test_local_done_duplicate_keeps_one_consumed_notice_and_old_grant);
	UT_RUN(test_local_done_NOT_ADMITTED_keeps_original_notice_and_receipt);
	UT_RUN(test_local_done_real_LMS_takes_reverse_notice_until_actual_callback);
	UT_RUN(test_local_done_callback_failure_rearms_without_consuming_again);
	UT_RUN(test_local_done_real_LMS_validates_retired_notice_without_accepting_requester_DONE);
	UT_RUN(test_local_done_full_transition_budget_refuses_before_partial_revoke);
	UT_RUN(test_local_done_corrupt_independent_ACK_cannot_be_replaced_by_DONE);
	UT_RUN(test_local_done_late_receipt_and_callback_cannot_clear_successor);
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
