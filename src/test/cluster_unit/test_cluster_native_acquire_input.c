/*-------------------------------------------------------------------------
 *
 * test_cluster_native_acquire_input.c
 *	  Exercise native reservation identity and retained acquisition inputs.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_native_acquire_input.c
 *
 * NOTES
 *	  This is a pgrac-original test using the cluster namespace. Actual
 *	  native reservation, requester send and retained A/I/all-H are exercised;
 *	  encoded master/source inputs are controlled protocol fixtures. No
 *	  executor lease, IO permission, installation or native C is supplied.
 *
 *-------------------------------------------------------------------------
 */
#include "test_cluster_native_request_fixture.inc"
#include "test_cluster_resource_x_transport_frames.inc"

static int input_native_profile = -1;

static bool
input_episode_begin(ResourceXDecodedFrame *request, ResourceXDecodedFrame *legacy,
	ResourceXIntentSlot *ticket, PcmState observed, PcmState target, uint8 path)
{
	unsigned profile = input_native_profile >= 0 ? (unsigned)input_native_profile
		: observed == PCM_STATE_S ? 3 : observed == PCM_STATE_X ? 4 : 0;
	(void)path;
	request_start(profile, false);
	if (request_send(target) != CLUSTER_LMS_ENQUEUE_ADMITTED) return false;
	*ticket = request_ticket(request);
	*legacy = request_prepared;
	return true;
}

/* Reuse the existing independently encoded A/source/ACK producer inputs;
 * only episode preparation is the actual native->LMS physical handoff. */
#define episode_begin input_episode_begin
#include "test_cluster_acquire_evidence_fixture.inc"
#undef episode_begin
#include "test_cluster_native_acquire_input.inc"

static ResourceXInstallClaimJoinObservation input_observation;
static ResourceXDecodedFrame input_grant, input_image;
static ResourceXRequesterJoinSnapshot input_join;

static bool
input_begin(EvidenceFixture *f, int profile, int native_profile)
{
	input_native_profile = native_profile;
	if (!evidence_begin(f, profile)) return false;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_join_observe_exact(
		&f->request.common.logical_assertion, 0, 17, 31, 77, 51, 61,
		&input_observation), RESOURCE_X_APPLY_APPLIED);
	memset(&input_grant, 0xA5, sizeof(input_grant));
	memset(&input_image, 0xA5, sizeof(input_image));
	memset(&input_join, 0xA5, sizeof(input_join));
	return true;
}

static void
input_complete(EvidenceFixture *f, int profile)
{
	ResourceXRequesterJoinSnapshot out;
	UT_ASSERT(evidence_accept(profile >= 2 && profile <= 5 ? &f->image : &f->decision, &out));
	if (profile == 4 || profile == 6) UT_ASSERT(evidence_accept(&f->witness, &out));
	UT_ASSERT(out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE);
}

static ResourceXApplyResult
input_get(void)
{
	return cluster_bufmgr_pcm_own_acquire_input_exact(&request_buf, &request_base,
		request_token, request_base.pcm_state == PCM_STATE_N
		&& (request_base.semantic_buf_state & BM_VALID) == 0
		? RESOURCE_X_ACQUIRE_FIRST_FILL : RESOURCE_X_ACQUIRE_RESIDENT,
		&request_admission, &input_observation, &input_grant, &input_image, &input_join);
}

static void
input_refused(ResourceXApplyResult wanted)
{
	ResourceXDecodedFrame grant = input_grant, image = input_image;
	ResourceXRequesterJoinSnapshot join = input_join;
	ClusterPcmOwnEntry own = request_own;
	ClusterPcmResourceXBootstrapRound round = request_entry()->resource_x_bootstrap_round;
	uint32 state = pg_atomic_read_u32(&request_buf.state);
	UT_ASSERT_EQ(input_get(), wanted);
	UT_ASSERT(memcmp(&grant, &input_grant, sizeof(grant)) == 0);
	UT_ASSERT(memcmp(&image, &input_image, sizeof(image)) == 0);
	UT_ASSERT(memcmp(&join, &input_join, sizeof(join)) == 0);
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	request_native_unchanged(&own, state);
}

UT_TEST(test_input_actual_native_sender_and_complete_materials_all_source_modes)
{
	for (int profile = 0; profile < 8; profile++) {
		EvidenceFixture f;
		ClusterPcmOwnEntry own;
		uint32 state;
		UT_ASSERT(input_begin(&f, profile, -1)); input_complete(&f, profile);
		own = request_own; state = pg_atomic_read_u32(&request_buf.state);
		UT_ASSERT_EQ(input_get(), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(input_grant.has_acquire_decision);
		UT_ASSERT_EQ(input_grant.common.authority_generation, 10001);
		UT_ASSERT_EQ(input_grant.body.acquire_decision.episode.base_generation, 48);
		UT_ASSERT_EQ(input_grant.body.acquire_decision.episode.reservation_token, 49);
		UT_ASSERT_EQ(input_grant.body.acquire_decision.episode.backend_id, 4);
		UT_ASSERT_EQ(input_grant.body.acquire_decision.episode.backend_generation, 71);
		UT_ASSERT(input_join.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE);
		UT_ASSERT((input_join.flags & (RESOURCE_X_REQUESTER_JOIN_READY | RESOURCE_X_REQUESTER_JOIN_TERMINAL)) == 0);
		UT_ASSERT_EQ(input_join.t_install_us, 0);
		UT_ASSERT_EQ(input_image.has_acquire_decision, profile >= 2 && profile <= 5);
		UT_ASSERT_EQ(request_shared.rings[1].count, 1);
		request_native_unchanged(&own, state);
	}
}

UT_TEST(test_input_resident_n_current_and_pi_remain_original_native_owners)
{
	for (int profile = 1; profile <= 2; profile++) {
		EvidenceFixture f;
		UT_ASSERT(input_begin(&f, 2, profile)); input_complete(&f, 2);
		UT_ASSERT_EQ(input_get(), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(input_grant.body.acquire_decision.episode.path, RESOURCE_X_ACQUIRE_RESIDENT);
		UT_ASSERT_EQ(request_buf.buffer_type, profile == 1 ? BUF_TYPE_CURRENT : BUF_TYPE_PI);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 48);
	}
}

UT_TEST(test_input_no_material_and_partial_material_stay_local_pending)
{
	EvidenceFixture f;
	ResourceXRequesterJoinSnapshot out;
	UT_ASSERT(input_begin(&f, 4, -1)); input_refused(RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT(evidence_accept(&f.decision, &out)); input_refused(RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(evidence_accept(&f.image, &out)); input_refused(RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(evidence_accept(&f.witness, &out));
	UT_ASSERT_EQ(input_get(), RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_input_all_arrival_orders_use_one_native_reservation)
{
	static const int order[6][3] = {{0,1,2},{0,2,1},{1,0,2},{1,2,0},{2,0,1},{2,1,0}};
	for (unsigned n = 0; n < 6; n++) {
		EvidenceFixture f;
		ResourceXRequesterJoinSnapshot out;
		bool image = false, other = false;
		UT_ASSERT(input_begin(&f, 4, -1));
		for (unsigned k = 0; k < 3; k++) {
			int item = order[n][k];
			ResourceXApplyResult rc = evidence_offer(item == 0 ? &f.decision : item == 1 ? &f.image : &f.witness, &out);
			UT_ASSERT(rc == RESOURCE_X_APPLY_APPLIED || rc == RESOURCE_X_APPLY_DUPLICATE);
			image |= item == 1; other |= item == 2;
			if (image && other) UT_ASSERT_EQ(input_get(), RESOURCE_X_APPLY_APPLIED);
			else input_refused(RESOURCE_X_APPLY_BAD_STATE);
			UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 48);
		}
	}
}

UT_TEST(test_input_duplicate_preparations_never_complete_install_or_send_again)
{
	EvidenceFixture f;
	ResourceXDecodedFrame grant, image;
	ResourceXRequesterJoinSnapshot join;
	UT_ASSERT(input_begin(&f, 4, -1)); input_complete(&f, 4);
	UT_ASSERT_EQ(input_get(), RESOURCE_X_APPLY_APPLIED);
	grant = input_grant; image = input_image; join = input_join;
	for (int k = 0; k < 4; k++) {
		UT_ASSERT_EQ(input_get(), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&grant, &input_grant, sizeof(grant)) == 0);
		UT_ASSERT(memcmp(&image, &input_image, sizeof(image)) == 0);
		UT_ASSERT(memcmp(&join, &input_join, sizeof(join)) == 0);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 48);
		UT_ASSERT_EQ(request_shared.rings[1].count, 1);
	}
}

UT_TEST(test_input_every_observation_domain_and_entry_binding_is_exact)
{
	for (int field = 0; field < 9; field++) {
		EvidenceFixture f;
		UT_ASSERT(input_begin(&f, 2, -1)); input_complete(&f, 2);
		if (field == 0) input_observation.entry_binding_generation++;
		if (field == 1) input_observation.request.assertion_sequence++;
		if (field == 2) input_observation.request.master_session_incarnation++;
		if (field == 3) input_observation.request.resource_formation++;
		if (field == 4) input_observation.r4_record_generation++;
		if (field == 5) input_observation.request.sender_connection_generation++;
		if (field == 6) input_observation.master_ingress_connection_generation++;
		if (field == 7) input_observation.master_node++;
		if (field == 8) input_observation.request.target_mode = PCM_STATE_X;
		input_refused(RESOURCE_X_APPLY_STALE);
	}
}

UT_TEST(test_input_native_tag_generation_token_and_process_cannot_turn_over)
{
	for (int field = 0; field < 6; field++) {
		EvidenceFixture f;
		UT_ASSERT(input_begin(&f, 2, -1)); input_complete(&f, 2);
		if (field == 0) request_buf.tag.blockNum++;
		if (field == 1) pg_atomic_write_u64(&request_own.generation, 49);
		if (field == 2) request_token++;
		if (field == 3) request_proc.cluster_grd_generation++;
		if (field == 4) request_proc.backendId++;
		if (field == 5) request_pins = 0;
		input_refused(RESOURCE_X_APPLY_STALE);
	}
}

UT_TEST(test_input_busy_native_producers_are_not_a_completed_input)
{
	for (int field = 0; field < 4; field++) {
		EvidenceFixture f;
		UT_ASSERT(input_begin(&f, 2, -1)); input_complete(&f, 2);
		if (field == 0) pg_atomic_write_u64(&request_own.delivery_attempt, 1);
		if (field == 1) pg_atomic_write_u64(&request_own.writer_activation_token, 49);
		if (field == 2) pg_atomic_write_u64(&request_own.write_io_generation, 1);
		if (field == 3) pg_atomic_write_u32(&request_own.flags, PCM_OWN_FLAG_REVOKING);
		input_refused(RESOURCE_X_APPLY_BAD_STATE);
	}
}

UT_TEST(test_input_source_side_and_closed_admission_never_supply_materials)
{
	for (int field = 0; field < 3; field++) {
		EvidenceFixture f;
		UT_ASSERT(input_begin(&f, 2, -1)); input_complete(&f, 2);
		if (field == 0) request_admission.side = CLUSTER_SEMANTIC_SOURCE_SIDE;
		if (field == 1) request_admission.entered = false;
		if (field == 2) master_durable_admission = false;
		input_refused(RESOURCE_X_APPLY_BAD_STATE);
	}
}

static void input_turn_over(void) { request_proc.cluster_grd_generation++; }
static void input_cancel_admission(void) { master_durable_admission = false; }
static void input_raise_error(void) { pg_re_throw(); }

UT_TEST(test_input_turnover_or_cancel_during_native_recheck_preserves_outputs)
{
	for (int field = 0; field < 2; field++) {
		EvidenceFixture f;
		UT_ASSERT(input_begin(&f, 2, -1)); input_complete(&f, 2);
		request_rechecks = 0; request_hook = field == 0 ? input_turn_over : input_cancel_admission;
		input_refused(field == 0 ? RESOURCE_X_APPLY_STALE : RESOURCE_X_APPLY_BAD_STATE);
	}
}

UT_TEST(test_input_provider_error_keeps_native_reservation_and_output_canaries)
{
	EvidenceFixture f;
	ResourceXDecodedFrame grant, image;
	ResourceXRequesterJoinSnapshot join;
	volatile bool caught = false;
	UT_ASSERT(input_begin(&f, 2, -1)); input_complete(&f, 2);
	grant = input_grant; image = input_image; join = input_join;
	request_rechecks = 0; request_hook = input_raise_error;
	PG_TRY(); { (void)input_get(); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & BM_LOCKED) == 0);
	UT_ASSERT(memcmp(&grant, &input_grant, sizeof(grant)) == 0);
	UT_ASSERT(memcmp(&image, &input_image, sizeof(image)) == 0);
	UT_ASSERT(memcmp(&join, &input_join, sizeof(join)) == 0);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
}

UT_TEST(test_input_round_failure_and_cancel_floors_preserve_old_materials)
{
	for (int field = 0; field < 3; field++) {
		EvidenceFixture f;
		ClusterPcmResourceXBootstrapRound *round;
		UT_ASSERT(input_begin(&f, 2, -1)); input_complete(&f, 2);
		round = &request_entry()->resource_x_bootstrap_round;
		if (field == 0) round->failed_attempt_floor = round->request.assertion_sequence;
		if (field == 1) round->cancelled_attempt_floor = round->request.assertion_sequence;
		if (field == 2) round->head_failure_reason = RESOURCE_X_HEAD_NO_PROGRESS_EXPIRED;
		input_refused(RESOURCE_X_APPLY_BAD_STATE);
	}
}

UT_TEST(test_input_closed_resource_gate_refuses_without_retiring_a)
{
	EvidenceFixture f;
	ResourceXGateSnapshot gate;
	UT_ASSERT(input_begin(&f, 2, -1)); input_complete(&f, 2);
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate), RESOURCE_X_APPLY_APPLIED);
	input_refused(RESOURCE_X_APPLY_RECOVERY_BLOCKED);
}

UT_TEST(test_input_legacy_ready_api_remains_closed_after_complete_input)
{
	EvidenceFixture f;
	ResourceXRequesterJoinSnapshot out;
	ResourceXDecodedFrame grant, image;
	UT_ASSERT(input_begin(&f, 2, -1)); input_complete(&f, 2);
	UT_ASSERT_EQ(input_get(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_frames_exact(
		&f.request.common.logical_assertion, &grant, &image, &out), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT((out.flags & RESOURCE_X_REQUESTER_JOIN_READY) == 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.accepted_base, 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 48);
}

UT_TEST(test_input_disk_and_local_sources_do_not_export_a_previous_remote_image)
{
	static const ResourceXDecodedFrame empty;
	for (int profile = 0; profile < 2; profile++) {
		EvidenceFixture f;
		UT_ASSERT(input_begin(&f, 2, -1)); input_complete(&f, 2);
		UT_ASSERT_EQ(input_get(), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(input_image.has_acquire_decision);
		UT_ASSERT(input_begin(&f, profile == 0 ? 0 : 7, -1));
		input_complete(&f, profile == 0 ? 0 : 7);
		UT_ASSERT_EQ(input_get(), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&input_image, &empty, sizeof(empty)) == 0);
	}
}

UT_TEST(test_input_post_material_native_turnover_cannot_publish_candidate_outputs)
{
	for (int field = 0; field < 2; field++) {
		EvidenceFixture f;
		UT_ASSERT(input_begin(&f, 2, -1)); input_complete(&f, 2);
		/* The imported provider hook fires at count2. Unsigned wrap targets
		 * the fourth provider call, after the locked material read. */
		request_rechecks = UINT_MAX - 1;
		request_hook = field == 0 ? input_turn_over : input_cancel_admission;
		input_refused(field == 0 ? RESOURCE_X_APPLY_STALE : RESOURCE_X_APPLY_BAD_STATE);
	}
}

UT_TEST(test_input_all32_holders_require_the_last_original_witness)
{
	EvidenceFixture f;
	ResourceXRequesterJoinSnapshot out;
	ResourceXDecodedAcquireDecision *a;
	UT_ASSERT(input_begin(&f, 6, -1));
	a = &f.decision.body.acquire_decision;
	a->holders_bitmap = UINT32_MAX;
	a->required_holders_bitmap = UINT32_MAX & ~(UINT32_C(1) << 1);
	for (int n = 0; n < RESOURCE_X_PROTOCOL_NODE_LIMIT; n++) {
		a->holders[n].boot_incarnation = n == 1 ? 101 : n == 0 ? 31 : 200 + n;
		a->holders[n].grant_generation = 50 + n;
	}
	UT_ASSERT(evidence_wire(&f.decision));
	UT_ASSERT(evidence_accept(&f.decision, &out));
	for (int n = 0; n < RESOURCE_X_PROTOCOL_NODE_LIMIT; n++) {
		ResourceXDecodedFrame ack;
		if (n == 1) continue;
		input_refused(RESOURCE_X_APPLY_BAD_STATE);
		ack = evidence_witness(&f.decision, n);
		UT_ASSERT(evidence_accept(&ack, &out));
	}
	UT_ASSERT_EQ(input_get(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 48);
}

UT_TEST(test_input_invalid_output_arguments_preserve_all_canaries)
{
	for (int field = 0; field < 5; field++) {
		EvidenceFixture f;
		ResourceXDecodedFrame grant, image;
		ResourceXRequesterJoinSnapshot join;
		UT_ASSERT(input_begin(&f, 2, -1)); input_complete(&f, 2);
		grant = input_grant; image = input_image; join = input_join;
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_acquire_input_exact(&request_buf, &request_base,
			request_token, RESOURCE_X_ACQUIRE_FIRST_FILL, &request_admission,
			field == 0 ? NULL : &input_observation, field == 1 ? NULL : &input_grant,
			field == 2 ? NULL : field == 4 ? &input_grant : &input_image,
			field == 3 ? NULL : &input_join), RESOURCE_X_APPLY_INVALID);
		UT_ASSERT(memcmp(&grant, &input_grant, sizeof(grant)) == 0);
		UT_ASSERT(memcmp(&image, &input_image, sizeof(image)) == 0);
		UT_ASSERT(memcmp(&join, &input_join, sizeof(join)) == 0);
	}
}

int
main(void)
{
	UT_PLAN(18);
	UT_RUN(test_input_actual_native_sender_and_complete_materials_all_source_modes);
	UT_RUN(test_input_resident_n_current_and_pi_remain_original_native_owners);
	UT_RUN(test_input_no_material_and_partial_material_stay_local_pending);
	UT_RUN(test_input_all_arrival_orders_use_one_native_reservation);
	UT_RUN(test_input_duplicate_preparations_never_complete_install_or_send_again);
	UT_RUN(test_input_every_observation_domain_and_entry_binding_is_exact);
	UT_RUN(test_input_native_tag_generation_token_and_process_cannot_turn_over);
	UT_RUN(test_input_busy_native_producers_are_not_a_completed_input);
	UT_RUN(test_input_source_side_and_closed_admission_never_supply_materials);
	UT_RUN(test_input_turnover_or_cancel_during_native_recheck_preserves_outputs);
	UT_RUN(test_input_provider_error_keeps_native_reservation_and_output_canaries);
	UT_RUN(test_input_round_failure_and_cancel_floors_preserve_old_materials);
	UT_RUN(test_input_closed_resource_gate_refuses_without_retiring_a);
	UT_RUN(test_input_legacy_ready_api_remains_closed_after_complete_input);
	UT_RUN(test_input_disk_and_local_sources_do_not_export_a_previous_remote_image);
	UT_RUN(test_input_post_material_native_turnover_cannot_publish_candidate_outputs);
	UT_RUN(test_input_all32_holders_require_the_last_original_witness);
	UT_RUN(test_input_invalid_output_arguments_preserve_all_canaries);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
