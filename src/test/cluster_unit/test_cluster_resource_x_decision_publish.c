/*-------------------------------------------------------------------------
 * test_cluster_resource_x_decision_publish.c
 *    Real master publication and original transport/cancellation owners.
 *
 * Admission, plans, codec, shared containers, staging and callbacks are real.
 * The inherited environment providers remain explicit. Multi-holder metadata
 * injections test policy/identity boundaries, not remote grant authentication.
 * Conditional holder/native consumers are not qualified by these tests.
 *-------------------------------------------------------------------------
 */
#define cluster_test_pcm_clock_gettime cluster_publish_fixture_clock_gettime
#include "test_cluster_resource_x_acquire_fixture.inc"
#undef cluster_test_pcm_clock_gettime
#include "test_cluster_resource_x_decision_owner_layout.inc"

static bool publish_zero_clock;
int cluster_test_pcm_clock_gettime(clockid_t clock_id, struct timespec *time_out);
int
cluster_test_pcm_clock_gettime(clockid_t clock_id, struct timespec *time_out)
{
	if (publish_zero_clock) {
		time_out->tv_sec = time_out->tv_nsec = 0;
		return 0;
	}
	return cluster_publish_fixture_clock_gettime(clock_id, time_out);
}

typedef struct PublishFixture {
	ResourceXDecodedFrame request;
	ResourceXBootstrapWork work;
	ResourceXAcquireDecisionPlan plan;
	ClusterPcmResourceXMasterState *state;
} PublishFixture;

static bool
publish_start(int scenario, PcmState target, PublishFixture *f)
{
	ClusterPcmResourceXMasterRequest canonical = {0};
	unsigned matches = 0;
	unsigned char *memory = (unsigned char *)fake_pcm_header.data;
	int requester = scenario == 3 ? 0 : 1;
	Size offset;

	memset(f, 0, sizeof(*f));
	publish_zero_clock = false;
	if (scenario == 0)
		acquire_reset();
	else {
		provenance_grant(0);
		if (scenario == 2) {
			service_directory_transition(PCM_TRANS_X_TO_S_DOWNGRADE, 0);
			if (decision_plan_inject_shared_holder(2, 102) == NULL
				|| decision_plan_inject_shared_holder(3, 103) == NULL) return false;
		}
	}
	f->request = acquire_frame(requester, scenario == 3 ? RESOURCE_X_ACQUIRE_RESIDENT
		: RESOURCE_X_ACQUIRE_FIRST_FILL, scenario == 3 ? PCM_STATE_X : PCM_STATE_N, target);
	if (scenario == 3) {
		ResourceXWireReject reject;
		uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES];
		uint16 len;
		/* Original local grant consumed attempt 41. Use a genuinely new E. */
		f->request.common.assertion_sequence++;
		UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, &f->request,
			bytes, sizeof(bytes), &len, &reject));
		UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X, bytes, len,
			&f->request, &reject));
	}
	if (!acquire_canonical_start(&f->request, &f->work)) return false;
	f->plan = decision_plan_read(&f->work);
	canonical.base_authority_generation = f->plan.input.canonical_base_generation;
	canonical.resource_formation = f->work.request.resource_formation;
	canonical.master_session_incarnation = f->work.request.master_session_incarnation;
	canonical.assertion_sequence = f->work.request.assertion_sequence;
	canonical.enqueue_order = f->work.enqueue_order;
	canonical.sender_connection_generation = f->work.request.sender_connection_generation;
	canonical.source_node = -1;
	canonical.phase = RESOURCE_X_MASTER_QUEUED;
	offset = offsetof(ClusterPcmResourceXMasterState, requests[requester]);
	for (Size n = offset; n + sizeof(canonical) <= fake_pcm_header_requested_size; n += 8)
		if (memcmp(memory + n, &canonical, sizeof(canonical)) == 0) {
			matches++;
			f->state = (ClusterPcmResourceXMasterState *)(memory + n - offset);
		}
	UT_ASSERT_EQ(matches, 1);
	UT_ASSERT((unsigned char *)(f->state + 1) <= memory + fake_pcm_header_requested_size);
	fake_pcm_clock_us = 100000;
	return matches == 1;
}

static void
publish_apply(PublishFixture *f)
{
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(
		&f->work, &f->plan.input), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
}

static ResourceXDecodedFrame
publish_decode(const uint8 *bytes, bool block)
{
	ResourceXDecodedFrame frame = {0};
	ResourceXWireReject reject;
	UT_ASSERT(cluster_resource_x_wire_decode(block ? RESOURCE_X_MSG_BLOCK_TO_N
		: RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, RESOURCE_X_DECISION_V4_BYTES, &frame, &reject));
	UT_ASSERT(frame.has_acquire_decision);
	return frame;
}

static void
publish_check(PublishFixture *f)
{
	ResourceXDecodedFrame grant = publish_decode(f->state->grant_intent.payload, false);
	const ResourceXDecodedAcquireDecision *a = &grant.body.acquire_decision;
	ClusterPcmResourceXMasterRequest *r = &f->state->requests[f->work.request.logical_assertion.requester_node];

	UT_ASSERT(memcmp(a, &f->plan.decision, sizeof(*a)) == 0);
	UT_ASSERT_EQ(grant.common.authority_generation, f->plan.proposed_authority_generation);
	UT_ASSERT_EQ(f->state->authority_generation, f->plan.proposed_authority_generation);
	UT_ASSERT_EQ(r->delegated_authority_generation, grant.common.authority_generation);
	UT_ASSERT_EQ(r->final_authority_generation, 0);
	UT_ASSERT_EQ(r->blocked_holders_bitmap, 0);
	UT_ASSERT_EQ(r->incompatible_holders_bitmap, a->required_holders_bitmap);
	UT_ASSERT_EQ(r->phase, a->required_holders_bitmap ? RESOURCE_X_MASTER_WAIT_BLOCKERS
		: RESOURCE_X_MASTER_WAIT_PROOF);
	UT_ASSERT_EQ(r->source_node, a->source_node);
	for (int node = 0; node < 32; node++) {
		ResourceXIntentSlot *slot = &f->state->block_intents[node].slot;
		ResourceXDecodedFrame block;
		if ((f->plan.service_holders_bitmap & (UINT32_C(1) << node)) == 0) {
			UT_ASSERT_EQ(slot->state, RESOURCE_X_INTENT_SLOT_EMPTY);
			continue;
		}
		block = publish_decode(f->state->block_intents[node].payload, true);
		UT_ASSERT_EQ(slot->state, RESOURCE_X_INTENT_SLOT_ARMED);
		UT_ASSERT_EQ(slot->authority_generation, grant.common.authority_generation);
		UT_ASSERT_EQ(slot->body.owner_kind, RESOURCE_X_INTENT_OWNER_MASTER_BLOCK);
		UT_ASSERT_EQ(slot->destination_node, node);
		UT_ASSERT(memcmp(&block.body.acquire_decision, a, sizeof(*a)) == 0);
	}
	UT_ASSERT_EQ(f->state->grant_intent.slot.state,
		a->source_kind == RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT
		? RESOURCE_X_INTENT_SLOT_EMPTY : RESOURCE_X_INTENT_SLOT_ARMED);
}

UT_TEST(test_publish_disk_s_and_x_are_conditional_not_native_grants)
{
	for (PcmState target = PCM_STATE_S; target <= PCM_STATE_X; target++) {
		PublishFixture f;
		PcmAuthoritySnapshot after;
		if (!publish_start(0, target, &f)) continue;
		publish_apply(&f); publish_check(&f);
		UT_ASSERT(cluster_pcm_lock_authority_snapshot(make_tag(850), &after));
		UT_ASSERT(memcmp(&after, &f.plan.input.directory, sizeof(after)) == 0);
	}
}

UT_TEST(test_publish_remote_x_to_s_retains_one_a_without_fourth_hop_grant)
{
	PublishFixture f;
	if (!publish_start(1, PCM_STATE_S, &f)) return;
	publish_apply(&f); publish_check(&f);
	UT_ASSERT_EQ(f.plan.service_holders_bitmap, 1);
	UT_ASSERT_EQ(f.state->grant_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
}

UT_TEST(test_publish_all_shared_blockers_are_armed_in_parallel)
{
	PublishFixture f;
	if (!publish_start(2, PCM_STATE_X, &f)) return;
	publish_apply(&f); publish_check(&f);
	UT_ASSERT_EQ(f.plan.service_holders_bitmap, 13);
	UT_ASSERT_EQ(f.state->block_intents[0].slot.first_armed_us,
		f.state->block_intents[2].slot.first_armed_us);
	UT_ASSERT_EQ(f.state->block_intents[0].slot.first_armed_us,
		f.state->block_intents[3].slot.first_armed_us);
}

UT_TEST(test_publish_compatible_s_keeps_source_outside_required_set)
{
	PublishFixture f;
	if (!publish_start(2, PCM_STATE_S, &f)) return;
	publish_apply(&f); publish_check(&f);
	UT_ASSERT_EQ(f.plan.decision.required_holders_bitmap, 0);
	UT_ASSERT_EQ(f.plan.service_holders_bitmap, 1);
}

UT_TEST(test_publish_requester_current_retains_self_downgrade_responsibility)
{
	for (PcmState target = PCM_STATE_S; target <= PCM_STATE_X; target++) {
		PublishFixture f;
		if (!publish_start(3, target, &f)) continue;
		publish_apply(&f); publish_check(&f);
		UT_ASSERT_EQ(f.plan.service_holders_bitmap, target == PCM_STATE_S ? 1 : 0);
	}
}

UT_TEST(test_publish_duplicate_never_bumps_authority_or_rearms)
{
	for (int scenario = 0; scenario <= 3; scenario++) {
		PublishFixture f;
		ClusterPcmResourceXMasterState before;
		if (!publish_start(scenario, PCM_STATE_X, &f)) continue;
		publish_apply(&f); before = *f.state;
		fake_pcm_clock_us += 1000000;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(
			&f.work, &f.plan.input), RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	}
}

UT_TEST(test_publish_stale_expected_cut_preserves_entire_master)
{
	PublishFixture f;
	ClusterPcmResourceXMasterState before;
	if (!publish_start(0, PCM_STATE_X, &f)) return;
	before = *f.state; f.plan.input.pi_watermark_lsn++;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(
		&f.work, &f.plan.input), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}

UT_TEST(test_publish_changed_native_episode_never_takes_successor_responsibility)
{
	PublishFixture f;
	ClusterPcmResourceXMasterState before;
	ResourceXBootstrapWork wrong;
	if (!publish_start(0, PCM_STATE_X, &f)) return;
	before = *f.state;
	for (int variant = 0; variant < 7; variant++) {
		wrong = f.work;
		if (variant == 0) wrong.acquire_intent.requester_incarnation++;
		if (variant == 1) wrong.acquire_intent.backend_generation++;
		if (variant == 2) wrong.acquire_intent.base_generation++;
		if (variant == 3) wrong.acquire_intent.reservation_token++;
		if (variant == 4) wrong.acquire_intent.buffer_id++;
		if (variant == 5) wrong.binding_generation++;
		if (variant == 6) wrong.enqueue_order++;
		UT_ASSERT(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&wrong, NULL)
			!= RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	}
}

UT_TEST(test_publish_rechecks_master_after_entry_acquisition)
{
	PublishFixture f;
	ClusterPcmResourceXMasterState before;
	if (!publish_start(0, PCM_STATE_X, &f)) return;
	before = *f.state;
	object_entry_lock_hook = service_master_turnover_at_entry_lock;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&f.work, NULL),
		RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}

UT_TEST(test_publish_capacity_is_checked_before_authority_is_reserved)
{
	PublishFixture f;
	ClusterPcmResourceXMasterState before;
	if (!publish_start(0, PCM_STATE_X, &f)) return;
	f.state->block_intents[31].slot.state = RESOURCE_X_INTENT_SLOT_ARMED;
	before = *f.state;
	UT_ASSERT(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&f.work, NULL)
		!= RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}

UT_TEST(test_publish_zero_clock_preserves_prepublication_owner)
{
	PublishFixture f;
	ClusterPcmResourceXMasterState before;
	if (!publish_start(0, PCM_STATE_X, &f)) return;
	before = *f.state; publish_zero_clock = true;
	UT_ASSERT(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&f.work, NULL)
		!= RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	publish_zero_clock = false;
}

UT_TEST(test_publish_unsent_exact_cancel_retires_all_owners_and_keeps_floor)
{
	for (int scenario = 0; scenario <= 3; scenario++) {
		PublishFixture f;
		PcmAuthoritySnapshot after;
		int node;
		if (!publish_start(scenario, PCM_STATE_X, &f)) continue;
		publish_apply(&f); node = f.work.request.logical_assertion.requester_node;
		cluster_pcm_lock_resource_x_bootstrap_forget_exact(&f.work);
		UT_ASSERT_EQ(f.state->requests[node].phase, RESOURCE_X_MASTER_NONE);
		UT_ASSERT_EQ(f.state->bootstrap_priority[node].state, RESOURCE_X_BOOTSTRAP_PRIORITY_EMPTY);
		UT_ASSERT_EQ(f.state->bootstrap_receipts[node].highest_attempt_floor,
			f.work.request.assertion_sequence);
		UT_ASSERT_EQ(f.state->authority_generation, f.plan.proposed_authority_generation);
		UT_ASSERT_EQ(f.state->grant_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
		for (int h = 0; h < 32; h++)
			UT_ASSERT_EQ(f.state->block_intents[h].slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
		UT_ASSERT(cluster_pcm_lock_authority_snapshot(make_tag(850), &after));
		UT_ASSERT(memcmp(&after, &f.plan.input.directory, sizeof(after)) == 0);
	}
}

UT_TEST(test_publish_staged_or_handed_off_send_prevents_quiet_cancel)
{
	for (int scenario = 0; scenario <= 2; scenario++)
		for (int result = 0; result < 3; result++) {
			PublishFixture f;
			ResourceXIntentSlot attempt;
			ResourceXIntentSlot *slot;
			ClusterPcmResourceXMasterState before;
			if (!publish_start(scenario, PCM_STATE_X, &f)) continue;
			publish_apply(&f);
			slot = scenario ? &f.state->block_intents[0].slot : &f.state->grant_intent.slot;
			UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
				slot, 100001, &attempt), RESOURCE_X_INTENT_STAGED);
			if (result != 0)
				UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(
					&attempt, result == 2, 100002));
			before = *f.state;
			cluster_pcm_lock_resource_x_bootstrap_forget_exact(&f.work);
			UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
		}
}

UT_TEST(test_publish_old_work_cannot_cancel_or_republish_successor)
{
	PublishFixture f;
	ResourceXBootstrapWork old;
	ResourceXDecodedFrame request;
	ClusterPcmResourceXMasterState before;
	if (!publish_start(0, PCM_STATE_X, &f)) return;
	publish_apply(&f); old = f.work;
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&old);
	request = f.request; request.common.assertion_sequence++;
	request.body.acquire_intent.reservation_token++;
	UT_ASSERT(acquire_canonical_start(&request, &f.work));
	f.plan = decision_plan_read(&f.work); publish_apply(&f);
	before = *f.state;
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&old);
	UT_ASSERT(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&old, NULL)
		!= RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}

UT_TEST(test_publish_transport_completion_does_not_retire_a_or_native_debt)
{
	PublishFixture f;
	ResourceXIntentSlot attempt;
	if (!publish_start(0, PCM_STATE_X, &f)) return;
	publish_apply(&f);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
		&f.state->grant_intent.slot, 100001, &attempt), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&attempt, true, 100002));
	UT_ASSERT_EQ(f.state->requests[1].final_authority_generation, 0);
	UT_ASSERT_EQ(f.state->requests[1].phase, RESOURCE_X_MASTER_WAIT_PROOF);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&f.work, NULL),
		RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(f.state->grant_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
}

UT_TEST(test_publish_preserves_fifo_and_never_replans_sealed_source)
{
	PublishFixture f;
	ResourceXDecodedFrame next, ack;
	ResourceXBootstrapWork next_work;
	ResourceXIntentSlot slot;
	ResourceXAcquisitionRef delivery;
	ResourceXIntentProbeResult probe = RESOURCE_X_INTENT_PROBE_IDLE;
	uint32 examined;
	uint8 payload[RESOURCE_X_DECISION_V4_BYTES];
	ClusterPcmResourceXMasterState before;
	if (!publish_start(1, PCM_STATE_X, &f)) return;
	publish_apply(&f);
	next = acquire_frame(2, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	UT_ASSERT_EQ(acquire_deliver(&next), RESOURCE_X_APPLY_APPLIED);
	memset(&next_work, 0, sizeof(next_work));
	for (int scan = 0; scan < 32; scan++) {
		probe = cluster_pcm_lock_resource_x_outbound_work_probe_exact(1, &slot, payload,
			sizeof(payload), &examined, &delivery);
		UT_ASSERT(probe != RESOURCE_X_INTENT_PROBE_CORRUPT);
		if (probe == RESOURCE_X_INTENT_PROBE_BOOTSTRAP) {
			memcpy(&next_work, payload, sizeof(next_work));
			break;
		}
	}
	UT_ASSERT_EQ(probe, RESOURCE_X_INTENT_PROBE_BOOTSTRAP);
	/* The original canonical head is still the first LMS work item. */
	UT_ASSERT_EQ(next_work.request.logical_assertion.requester_node, 1);
	before = *f.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_resume_exact(&f.work, &ack),
		RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&f.work, NULL),
		RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}

UT_TEST(test_publish_valid_crc_wrong_retained_a_is_not_quiet_or_duplicate)
{
	for (int mutation = 0; mutation < 4; mutation++) {
		PublishFixture f;
		ResourceXDecodedFrame a;
		ResourceXWireReject reject;
		uint16 bytes;
		ClusterPcmResourceXMasterState before;
		if (!publish_start(0, PCM_STATE_X, &f)) continue;
		publish_apply(&f); a = publish_decode(f.state->grant_intent.payload, false);
		if (mutation == 0) a.body.acquire_decision.episode.reservation_token++;
		if (mutation == 1) a.body.acquire_decision.resource_binding++;
		if (mutation == 2) a.body.acquire_decision.requester_ingress_generation++;
		if (mutation == 3) a.common.assertion_sequence++;
		UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &a,
			f.state->grant_intent.payload, sizeof(f.state->grant_intent.payload), &bytes, &reject));
		before = *f.state;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&f.work, NULL),
			RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		cluster_pcm_lock_resource_x_bootstrap_forget_exact(&f.work);
		UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	}
}

UT_TEST(test_publish_old_durable_proof_cannot_bypass_conditional_completion)
{
	PublishFixture f;
	ResourceXDurableProof proof = {0};
	ResourceXMasterSnapshot out;
	ClusterPcmResourceXMasterState before;
	if (!publish_start(0, PCM_STATE_X, &f)) return;
	publish_apply(&f); before = *f.state;
	proof.assertion = f.work.request.logical_assertion;
	proof.base_authority_generation = f.plan.input.canonical_base_generation;
	proof.resource_formation = 17; proof.master_session_incarnation = 31;
	proof.assertion_sequence = f.work.request.assertion_sequence;
	proof.requester_target_generation = 41;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(&proof, &out), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}

UT_TEST(test_publish_original_stop_seal_allows_already_admitted_owner_to_drain)
{
	PublishFixture f;
	if (!publish_start(0, PCM_STATE_S, &f)) return;
	stop_new_work_allowed = false;
	publish_apply(&f); publish_check(&f);
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&f.work);
	UT_ASSERT_EQ(f.state->requests[1].phase, RESOURCE_X_MASTER_NONE);
}

UT_TEST(test_publish_unknown_holder_is_not_permission_to_read_disk)
{
	PublishFixture f;
	ClusterPcmResourceXMasterState before;
	if (!publish_start(1, PCM_STATE_S, &f)) return;
	memset(&f.state->holder_grants[0], 0, sizeof(f.state->holder_grants[0]));
	before = *f.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&f.work, NULL),
		RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}

UT_TEST(test_publish_invalid_input_or_missing_entry_never_creates_owner)
{
	ResourceXBootstrapWork work = {0};
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(NULL, NULL),
		RESOURCE_X_APPLY_INVALID);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&work, NULL),
		RESOURCE_X_APPLY_INVALID);
}

UT_TEST(test_publish_old_blocked_and_local_proof_do_not_consume_a)
{
	PublishFixture f;
	ResourceXDecodedFrame blocked = {0}, local = {0};
	ResourceXMasterSnapshot out;
	ClusterPcmResourceXMasterState before;
	if (!publish_start(1, PCM_STATE_X, &f)) return;
	publish_apply(&f); before = *f.state;
	blocked.kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
	blocked.common = f.work.request;
	blocked.common.base_authority_generation = f.plan.input.canonical_base_generation;
	blocked.common.authority_generation = f.plan.input.canonical_base_generation;
	blocked.common.action_node = 0;
	blocked.common.outcome = RESOURCE_X_OUTCOME_OK;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(&blocked, 0, &out), RESOURCE_X_APPLY_BAD_STATE);
	local.kind = RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION;
	local.common = blocked.common;
	local.common.action_node = 1;
	local.body.local_proof.local_holder_authority_generation = 2;
	local.body.local_proof.requester_target_generation = 41;
	local.body.local_proof.requester_connection_generation = 61;
	local.body.local_proof.local_proof_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_local_proof_exact(&local, 1, &out), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}

UT_TEST(test_publish_original_transport_rechecks_forward_against_same_canonical_a)
{
	PublishFixture f;
	ResourceXDecodedFrame block;
	ResourceXWireReject reject;
	ResourceXIntentSlot slot, expected;
	uint8 bytes[736], original[736];
	uint16 len;
	if (!publish_start(1, PCM_STATE_X, &f)) return;
	publish_apply(&f);
	expected = f.state->block_intents[0].slot;
	block = publish_decode(f.state->block_intents[0].payload, true);
	block.body.acquire_decision.episode.backend_generation++;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, &block,
		f.state->block_intents[0].payload, sizeof(bytes), &len, &reject));
	memset(bytes, 0xa5, sizeof(bytes)); memcpy(original, bytes, sizeof(bytes));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(&expected,
		&slot, bytes, sizeof(bytes)), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(bytes, original, sizeof(bytes)) == 0);
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&f.work);
	UT_ASSERT_EQ(f.state->requests[1].delegated_authority_generation, f.plan.proposed_authority_generation);
}

UT_TEST(test_publish_r8_classifies_sealed_a_as_residual_not_malformed_or_empty)
{
	PublishFixture f;
	ResourceXReconfigToken token;
	ResourceXReconfigBatch batch;
	ResourceXReconfigResult result = RESOURCE_X_RECONFIG_MORE;
	if (!publish_start(0, PCM_STATE_X, &f)) return;
	publish_apply(&f);
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &token));
	for (int scan = 0; scan < 8 && (result == RESOURCE_X_RECONFIG_MORE
		|| result == RESOURCE_X_RECONFIG_RETRY); scan++)
		result = cluster_resource_x_reconfig_sweep(&token, 4, &batch);
	UT_ASSERT_EQ(result, RESOURCE_X_RECONFIG_ORPHAN);
	UT_ASSERT_EQ(f.state->requests[1].delegated_authority_generation, f.plan.proposed_authority_generation);
	UT_ASSERT_EQ(f.state->grant_intent.slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
}


static void
publish_watermark(PublishFixture *f, uint64 scn)
{
	cluster_pcm_lock_pi_watermark_scn_advance(f->work.request.logical_assertion.resource, scn,
											  CLUSTER_PCM_WM_SRC_REDECLARE, 0, 31, 17);
}
UT_TEST(test_publish_disk_carries_the_original_master_watermark)
{
	for (PcmState mode = PCM_STATE_S; mode <= PCM_STATE_X; mode++) {
		PublishFixture f;
		UT_ASSERT(publish_start(0, mode, &f));
		publish_watermark(&f, UINT64_C(0x070000000000005a));
		f.plan = decision_plan_read(&f.work);
		UT_ASSERT_EQ(f.plan.input.pi_watermark_scn, UINT64_C(0x070000000000005a));
		UT_ASSERT_EQ(f.plan.decision.disk_watermark_scn, f.plan.input.pi_watermark_scn);
		publish_apply(&f);
		publish_check(&f);
		UT_ASSERT_EQ(publish_decode(f.state->grant_intent.payload, false)
						 .body.acquire_decision.disk_watermark_scn,
					 UINT64_C(0x070000000000005a));
	}
}
UT_TEST(test_publish_watermark_change_refuses_a_stale_original_input)
{
	PublishFixture f;
	ClusterPcmResourceXMasterState before;
	UT_ASSERT(publish_start(0, PCM_STATE_S, &f));
	before = *f.state;
	publish_watermark(&f, 90);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&f.work, &f.plan.input),
				 RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}
UT_TEST(test_publish_sealed_disk_replay_keeps_its_original_watermark)
{
	PublishFixture f;
	uint8 before[RESOURCE_X_DECISION_V4_BYTES];
	UT_ASSERT(publish_start(0, PCM_STATE_S, &f));
	publish_watermark(&f, 90);
	f.plan = decision_plan_read(&f.work);
	publish_apply(&f);
	memcpy(before, f.state->grant_intent.payload, sizeof(before));
	publish_watermark(&f, 100);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&f.work, &f.plan.input),
				 RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(before, f.state->grant_intent.payload, sizeof(before)) == 0);
	UT_ASSERT_EQ(publish_decode(before, false).body.acquire_decision.disk_watermark_scn, 90);
}
UT_TEST(test_publish_current_source_does_not_reinterpret_a_disk_watermark)
{
	PublishFixture f;
	UT_ASSERT(publish_start(1, PCM_STATE_S, &f));
	publish_watermark(&f, 90);
	f.plan = decision_plan_read(&f.work);
	UT_ASSERT_EQ(f.plan.input.pi_watermark_scn, 90);
	UT_ASSERT_EQ(f.plan.decision.disk_watermark_scn, 0);
	publish_apply(&f);
	publish_check(&f);
}
UT_TEST(test_publish_WAL_position_is_not_the_cross_node_disk_version)
{
	PublishFixture f;
	UT_ASSERT(publish_start(0, PCM_STATE_S, &f));
	cluster_pcm_lock_pi_watermark_lsn_advance(f.work.request.logical_assertion.resource, 99999);
	publish_watermark(&f, 90);
	f.plan = decision_plan_read(&f.work);
	UT_ASSERT_EQ(f.plan.input.pi_watermark_lsn, 99999);
	UT_ASSERT_EQ(f.plan.decision.disk_watermark_scn, 90);
	publish_apply(&f);
	publish_check(&f);
}

int
main(void)
{
	UT_PLAN(29);
	UT_RUN(test_publish_disk_s_and_x_are_conditional_not_native_grants);
	UT_RUN(test_publish_remote_x_to_s_retains_one_a_without_fourth_hop_grant);
	UT_RUN(test_publish_all_shared_blockers_are_armed_in_parallel);
	UT_RUN(test_publish_compatible_s_keeps_source_outside_required_set);
	UT_RUN(test_publish_requester_current_retains_self_downgrade_responsibility);
	UT_RUN(test_publish_duplicate_never_bumps_authority_or_rearms);
	UT_RUN(test_publish_stale_expected_cut_preserves_entire_master);
	UT_RUN(test_publish_changed_native_episode_never_takes_successor_responsibility);
	UT_RUN(test_publish_rechecks_master_after_entry_acquisition);
	UT_RUN(test_publish_capacity_is_checked_before_authority_is_reserved);
	UT_RUN(test_publish_zero_clock_preserves_prepublication_owner);
	UT_RUN(test_publish_unsent_exact_cancel_retires_all_owners_and_keeps_floor);
	UT_RUN(test_publish_staged_or_handed_off_send_prevents_quiet_cancel);
	UT_RUN(test_publish_old_work_cannot_cancel_or_republish_successor);
	UT_RUN(test_publish_transport_completion_does_not_retire_a_or_native_debt);
	UT_RUN(test_publish_preserves_fifo_and_never_replans_sealed_source);
	UT_RUN(test_publish_valid_crc_wrong_retained_a_is_not_quiet_or_duplicate);
	UT_RUN(test_publish_old_durable_proof_cannot_bypass_conditional_completion);
	UT_RUN(test_publish_original_stop_seal_allows_already_admitted_owner_to_drain);
	UT_RUN(test_publish_unknown_holder_is_not_permission_to_read_disk);
	UT_RUN(test_publish_invalid_input_or_missing_entry_never_creates_owner);
	UT_RUN(test_publish_old_blocked_and_local_proof_do_not_consume_a);
	UT_RUN(test_publish_original_transport_rechecks_forward_against_same_canonical_a);
	UT_RUN(test_publish_r8_classifies_sealed_a_as_residual_not_malformed_or_empty);
	UT_RUN(test_publish_disk_carries_the_original_master_watermark);
	UT_RUN(test_publish_watermark_change_refuses_a_stale_original_input);
	UT_RUN(test_publish_sealed_disk_replay_keeps_its_original_watermark);
	UT_RUN(test_publish_current_source_does_not_reinterpret_a_disk_watermark);
	UT_RUN(test_publish_WAL_position_is_not_the_cross_node_disk_version);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
