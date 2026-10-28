/*-------------------------------------------------------------------------
 *
 * test_cluster_native_acquire_claim.c
 *	  Exercise one original native acquisition executor and its release.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_native_acquire_claim.c
 *
 * NOTES
 *	  This pgrac-original cluster test uses actual native/PCM/codec owners.
 *	  Encoded producer inputs and process/provider leaves are controlled.
 *	  A pre-native executor is not IO permission or native completion.
 *
 *-------------------------------------------------------------------------
 */
#include "test_cluster_native_acquire_input_fixture.inc"
#include "test_cluster_native_acquire_claim.inc"

static ResourceXDeliveryClaim acquire_claim;

static bool
claim_start(EvidenceFixture *f, int profile, int native_profile)
{
	if (!input_begin(f, profile, native_profile)) return false;
	input_complete(f, profile);
	memset(&acquire_claim, 0xA5, sizeof(acquire_claim));
	request_rechecks = 0;
	return true;
}

static ResourceXApplyResult
claim_get(void)
{
	return cluster_bufmgr_pcm_own_acquire_claim_exact(&request_buf, &request_base,
		request_token, request_base.pcm_state == PCM_STATE_N
		&& (request_base.semantic_buf_state & BM_VALID) == 0
		? RESOURCE_X_ACQUIRE_FIRST_FILL : RESOURCE_X_ACQUIRE_RESIDENT,
		&request_admission, &input_observation, &input_grant, &input_image, &input_join,
		&acquire_claim);
}

static void
claim_refused(ResourceXApplyResult wanted)
{
	ResourceXDeliveryClaim claim = acquire_claim;
	ResourceXDecodedFrame grant = input_grant, image = input_image;
	ResourceXRequesterJoinSnapshot join = input_join;
	ClusterPcmResourceXBootstrapRound round = request_entry()->resource_x_bootstrap_round;
	ClusterPcmOwnEntry own = request_own;
	uint32 state = pg_atomic_read_u32(&request_buf.state);
	UT_ASSERT_EQ(claim_get(), wanted);
	UT_ASSERT(memcmp(&claim, &acquire_claim, sizeof(claim)) == 0);
	UT_ASSERT(memcmp(&grant, &input_grant, sizeof(grant)) == 0);
	UT_ASSERT(memcmp(&image, &input_image, sizeof(image)) == 0);
	UT_ASSERT(memcmp(&join, &input_join, sizeof(join)) == 0);
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	request_native_unchanged(&own, state);
}

UT_TEST(test_claim_full_episode_cannot_enter_legacy_target_binder)
{
	EvidenceFixture f;
	ClusterPcmOwnSnapshot reserved;
	uint32 state;
	UT_ASSERT(claim_start(&f, 1, 1));
	state = request_header_lock(&request_buf);
	cluster_pcm_own_snapshot_locked(&request_buf, &reserved);
	UnlockBufHdr(&request_buf, state);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_delivery_bind_target_exact(
		&input_observation, 0, &reserved, &reserved), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_target.buffer_id_plus_one, 0);
}

UT_TEST(test_claim_all_sources_and_native_modes_keep_original_reservation)
{
	for (int profile = 0; profile < 10; profile++) {
		EvidenceFixture f;
		ClusterPcmOwnEntry own;
		uint32 state;
		UT_ASSERT(claim_start(&f, profile < 8 ? profile : 2, profile < 8 ? -1 : profile - 7));
		own = request_own; state = pg_atomic_read_u32(&request_buf.state);
		UT_ASSERT_EQ(claim_get(), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(acquire_claim.executor_sequence, 1);
		UT_ASSERT_EQ(acquire_claim.purpose, RESOURCE_X_DELIVERY_NORMAL);
		UT_ASSERT_EQ(acquire_claim.target.buffer_id_plus_one, 1);
		UT_ASSERT_EQ(acquire_claim.target.generation, 48);
		UT_ASSERT_EQ(acquire_claim.target.reservation_token, 49);
		UT_ASSERT_EQ(acquire_claim.target.flags, PCM_OWN_FLAG_GRANT_PENDING);
		UT_ASSERT_EQ(input_grant.common.authority_generation, 10001);
		UT_ASSERT(input_join.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE);
		UT_ASSERT((input_join.flags & RESOURCE_X_REQUESTER_JOIN_READY) == 0);
		UT_ASSERT_EQ(input_join.t_install_us, 0);
		UT_ASSERT_EQ(request_shared.rings[1].count, 1);
		request_native_unchanged(&own, state);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_claim_release_exact(&acquire_claim),
			RESOURCE_X_APPLY_APPLIED);
		request_native_unchanged(&own, state);
	}
}

UT_TEST(test_claim_second_executor_cannot_consume_or_replace_first)
{
	EvidenceFixture f;
	ResourceXDeliveryClaim first;
	UT_ASSERT(claim_start(&f, 4, -1)); UT_ASSERT_EQ(claim_get(), RESOURCE_X_APPLY_APPLIED);
	first = acquire_claim; claim_refused(RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(input_get(), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(&first, &acquire_claim, sizeof(first)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_claim_release_exact(&first), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(input_get(), RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_claim_partial_materials_never_reserve_an_executor_in_any_arrival_order)
{
	static const int order[6][3] = {{0,1,2},{0,2,1},{1,0,2},{1,2,0},{2,0,1},{2,1,0}};
	for (unsigned n = 0; n < 6; n++) {
		EvidenceFixture f;
		ResourceXRequesterJoinSnapshot out;
		bool image = false, other = false;
		UT_ASSERT(input_begin(&f, 4, -1)); memset(&acquire_claim, 0xA5, sizeof(acquire_claim));
		claim_refused(RESOURCE_X_APPLY_NOT_FOUND);
		for (unsigned k = 0; k < 3; k++) {
			int item = order[n][k];
			ResourceXApplyResult rc = evidence_offer(item == 0 ? &f.decision : item == 1 ? &f.image : &f.witness, &out);
			UT_ASSERT(rc == RESOURCE_X_APPLY_APPLIED || rc == RESOURCE_X_APPLY_DUPLICATE);
			image |= item == 1; other |= item == 2;
			/* SHIP includes A. Do not require a second copy of its decision. */
			if (!image || !other) claim_refused(RESOURCE_X_APPLY_BAD_STATE);
			else if (k == 2) UT_ASSERT_EQ(claim_get(), RESOURCE_X_APPLY_APPLIED);
			else UT_ASSERT_EQ(input_get(), RESOURCE_X_APPLY_APPLIED);
		}
	}
}

UT_TEST(test_claim_wrong_original_observation_never_binds_a_target)
{
	for (int field = 0; field < 9; field++) {
		EvidenceFixture f;
		UT_ASSERT(claim_start(&f, 2, -1));
		if (field == 0) input_observation.entry_binding_generation++;
		if (field == 1) input_observation.request.assertion_sequence++;
		if (field == 2) input_observation.request.master_session_incarnation++;
		if (field == 3) input_observation.request.resource_formation++;
		if (field == 4) input_observation.r4_record_generation++;
		if (field == 5) input_observation.request.sender_connection_generation++;
		if (field == 6) input_observation.master_ingress_connection_generation++;
		if (field == 7) input_observation.master_node++;
		if (field == 8) input_observation.request.target_mode = PCM_STATE_X;
		claim_refused(RESOURCE_X_APPLY_STALE);
	}
}

static unsigned claim_race;
static void
claim_after_binding(void)
{
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, RESOURCE_X_DELIVERY_NORMAL);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	if (claim_race == 0) request_proc.cluster_grd_generation++;
	if (claim_race == 1) master_durable_admission = false;
	if (claim_race == 2) pg_re_throw();
	if (claim_race == 3) {
		request_entry()->resource_x_bootstrap_round.r4_record_generation++;
		master_durable_admission = false;
	}
}

UT_TEST(test_claim_post_binding_native_change_or_cancel_releases_only_own_executor)
{
	for (claim_race = 0; claim_race < 2; claim_race++) {
		EvidenceFixture f;
		ResourceXDeliveryClaim claim;
		ResourceXDecodedFrame grant;
		UT_ASSERT(claim_start(&f, 2, -1)); claim = acquire_claim; grant = input_grant;
		/* Provider2 hook reached after unsigned wrap at the fourth sample. */
		request_rechecks = UINT_MAX - 1; request_hook = claim_after_binding;
		UT_ASSERT_EQ(claim_get(), claim_race == 0 ? RESOURCE_X_APPLY_STALE : RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT(memcmp(&claim, &acquire_claim, sizeof(claim)) == 0);
		UT_ASSERT(memcmp(&grant, &input_grant, sizeof(grant)) == 0);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_target.buffer_id_plus_one, 0);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 0);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_sequence, 1);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 48);
		UT_ASSERT_EQ(request_shared.rings[1].count, 1);
	}
}

UT_TEST(test_claim_provider_error_after_binding_runs_exact_finally_release)
{
	EvidenceFixture f;
	ResourceXDeliveryClaim claim;
	ResourceXDecodedFrame grant, image;
	ResourceXRequesterJoinSnapshot join;
	volatile bool caught = false;
	UT_ASSERT(claim_start(&f, 2, -1)); claim = acquire_claim;
	grant = input_grant; image = input_image; join = input_join;
	claim_race = 2; request_rechecks = UINT_MAX - 1; request_hook = claim_after_binding;
	PG_TRY(); { (void)claim_get(); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT(memcmp(&claim, &acquire_claim, sizeof(claim)) == 0);
	UT_ASSERT(memcmp(&grant, &input_grant, sizeof(grant)) == 0);
	UT_ASSERT(memcmp(&image, &input_image, sizeof(image)) == 0);
	UT_ASSERT(memcmp(&join, &input_join, sizeof(join)) == 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_target.buffer_id_plus_one, 0);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
}

UT_TEST(test_claim_released_handle_cannot_end_a_later_executor_or_advance_native)
{
	EvidenceFixture f;
	ResourceXDeliveryClaim old;
	UT_ASSERT(claim_start(&f, 7, -1)); UT_ASSERT_EQ(claim_get(), RESOURCE_X_APPLY_APPLIED); old = acquire_claim;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_claim_release_exact(&old), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(claim_get(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(acquire_claim.executor_sequence, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_claim_release_exact(&old), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, RESOURCE_X_DELIVERY_NORMAL);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 48);
}

UT_TEST(test_claim_every_claim_identity_is_required_for_release)
{
	for (int field = 0; field < 13; field++) {
		EvidenceFixture f;
		ResourceXDeliveryClaim bad;
		ClusterPcmResourceXBootstrapRound before;
		UT_ASSERT(claim_start(&f, 2, -1)); UT_ASSERT_EQ(claim_get(), RESOURCE_X_APPLY_APPLIED);
		bad = acquire_claim; before = request_entry()->resource_x_bootstrap_round;
		if (field == 0) bad.executor_sequence++;
		if (field == 1) bad.purpose = RESOURCE_X_DELIVERY_CLEANUP;
		if (field == 2) bad.reserved[0] = 1;
		if (field == 3) bad.observation.entry_binding_generation++;
		if (field == 4) bad.observation.request.assertion_sequence++;
		if (field == 5) bad.observation.r4_record_generation++;
		if (field == 6) bad.observation.master_node++;
		if (field == 7) bad.observation.master_ingress_connection_generation++;
		if (field == 8) bad.target.generation++;
		if (field == 9) bad.target.reservation_token++;
		if (field == 10) bad.target.buffer_id_plus_one++;
		if (field == 11) bad.target.flags = 0;
		if (field == 12) bad.observation.request.master_session_incarnation++;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_claim_release_exact(&bad), RESOURCE_X_APPLY_STALE);
		UT_ASSERT(memcmp(&before, &request_entry()->resource_x_bootstrap_round, sizeof(before)) == 0);
	}
}

UT_TEST(test_claim_legacy_executor_apis_cannot_consume_full_episode_claim)
{
	EvidenceFixture f;
	ResourceXAcquisitionRef ref;
	ResourceXDeliveryClaim other;
	UT_ASSERT(claim_start(&f, 2, -1)); UT_ASSERT_EQ(claim_get(), RESOURCE_X_APPLY_APPLIED);
	ref.assertion = f.request.common.logical_assertion; ref.formation = 17;
	ref.acquisition_generation = f.request.common.assertion_sequence;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_delivery_claim_begin_exact(&ref,
		&acquire_claim.target, RESOURCE_X_DELIVERY_NORMAL, 1000, &other), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_delivery_claim_end_exact(&acquire_claim), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, RESOURCE_X_DELIVERY_NORMAL);
}

UT_TEST(test_claim_release_after_cancel_or_gate_close_preserves_original_debt)
{
	for (int field = 0; field < 3; field++) {
		EvidenceFixture f;
		ResourceXGateSnapshot gate;
		ClusterPcmResourceXBootstrapRound *round;
		UT_ASSERT(claim_start(&f, 2, -1)); UT_ASSERT_EQ(claim_get(), RESOURCE_X_APPLY_APPLIED);
		round = &request_entry()->resource_x_bootstrap_round;
		if (field == 0) round->cancelled_attempt_floor = round->request.assertion_sequence;
		if (field == 1) { round->phase = UINT8_C(5); /* Private FAILED_CLOSED phase. */ round->failed_attempt_floor = round->request.assertion_sequence; }
		if (field == 2) { UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate)); UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate), RESOURCE_X_APPLY_APPLIED); }
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_claim_release_exact(&acquire_claim), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(round->has_acquire_intent, 1);
		UT_ASSERT_EQ(round->send.frame_count, 1);
		UT_ASSERT_EQ(round->delivery_target.buffer_id_plus_one, 0);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
		UT_ASSERT_EQ(request_shared.rings[1].count, 1);
	}
}

UT_TEST(test_claim_pre_native_release_cannot_erase_an_installation_handoff)
{
	EvidenceFixture f;
	ClusterPcmResourceXBootstrapRound *round;
	UT_ASSERT(claim_start(&f, 2, -1)); UT_ASSERT_EQ(claim_get(), RESOURCE_X_APPLY_APPLIED);
	round = &request_entry()->resource_x_bootstrap_round;
	round->install_claim_source = RESOURCE_X_INSTALL_CLAIM_ORDINARY_T1;
	round->install_claim_pending_generation = 48; round->install_claim_reservation_token = 49;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_claim_release_exact(&acquire_claim), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(round->delivery_executor_purpose, RESOURCE_X_DELIVERY_NORMAL);
	UT_ASSERT_EQ(round->delivery_target.buffer_id_plus_one, 1);
}

UT_TEST(test_claim_sequence_exhaustion_and_orphan_target_preserve_inputs)
{
	for (int field = 0; field < 3; field++) {
		EvidenceFixture f;
		UT_ASSERT(claim_start(&f, 2, -1));
		if (field == 0) request_entry()->resource_x_bootstrap_round.delivery_executor_sequence = UINT64_MAX - 1;
		if (field == 1) request_entry()->resource_x_bootstrap_round.delivery_target.generation = 48;
		if (field == 2) request_entry()->resource_x_bootstrap_round.delivery_executor_purpose = RESOURCE_X_DELIVERY_NORMAL;
		claim_refused(field == 0 ? RESOURCE_X_APPLY_RECOVERY_BLOCKED : RESOURCE_X_APPLY_BAD_STATE);
	}
}

UT_TEST(test_claim_cancelled_or_failed_round_never_binds_an_executor)
{
	for (int field = 0; field < 3; field++) {
		EvidenceFixture f;
		UT_ASSERT(claim_start(&f, 2, -1));
		if (field == 0) request_entry()->resource_x_bootstrap_round.failed_attempt_floor = f.request.common.assertion_sequence;
		if (field == 1) request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor = f.request.common.assertion_sequence;
		if (field == 2) request_entry()->resource_x_bootstrap_round.head_failure_reason = RESOURCE_X_HEAD_NO_PROGRESS_EXPIRED;
		claim_refused(RESOURCE_X_APPLY_BAD_STATE);
	}
}

UT_TEST(test_claim_invalid_outputs_preserve_canaries_and_original_owner)
{
	for (int field = 0; field < 5; field++) {
		EvidenceFixture f;
		ResourceXDeliveryClaim before;
		UT_ASSERT(claim_start(&f, 2, -1)); before = acquire_claim;
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_acquire_claim_exact(&request_buf, &request_base,
			request_token, RESOURCE_X_ACQUIRE_FIRST_FILL, &request_admission, &input_observation,
			field == 0 ? NULL : &input_grant, field == 1 ? NULL : field == 4 ? &input_grant : &input_image,
			field == 2 ? NULL : &input_join, field == 3 ? NULL : &acquire_claim), RESOURCE_X_APPLY_INVALID);
		UT_ASSERT(memcmp(&before, &acquire_claim, sizeof(before)) == 0);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_sequence, 0);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
	}
}

UT_TEST(test_claim_corrupt_empty_owner_neither_acquires_nor_releases_a_capability)
{
	for (int held = 0; held < 2; held++) {
		for (int field = 0; field < 3; field++) {
			EvidenceFixture f;
			ClusterPcmResourceXLocalOwner *owner;
			UT_ASSERT(claim_start(&f, 2, -1));
			if (held) UT_ASSERT_EQ(claim_get(), RESOURCE_X_APPLY_APPLIED);
			owner = &request_entry()->resource_x_local_owner;
			if (field == 0) owner->reserved[0] = 1;
			if (field == 1) owner->held_buffer_id_plus_one = 1;
			if (field == 2) owner->highest_owner_generation = UINT64_MAX;
			if (!held) claim_refused(RESOURCE_X_APPLY_RECOVERY_BLOCKED);
			else {
				UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_claim_release_exact(&acquire_claim), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
				UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_target.buffer_id_plus_one, 1);
			}
		}
	}
}

UT_TEST(test_claim_unproved_domain_change_retains_old_executor_without_export)
{
	EvidenceFixture f;
	ResourceXDeliveryClaim claim;
	ResourceXDecodedFrame grant;
	UT_ASSERT(claim_start(&f, 2, -1)); claim = acquire_claim; grant = input_grant;
	claim_race = 3; request_rechecks = UINT_MAX - 1; request_hook = claim_after_binding;
	UT_ASSERT_EQ(claim_get(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT(memcmp(&claim, &acquire_claim, sizeof(claim)) == 0);
	UT_ASSERT(memcmp(&grant, &input_grant, sizeof(grant)) == 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, RESOURCE_X_DELIVERY_NORMAL);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_target.buffer_id_plus_one, 1);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
}

int
main(void)
{
	UT_PLAN(17);
	UT_RUN(test_claim_full_episode_cannot_enter_legacy_target_binder);
	UT_RUN(test_claim_all_sources_and_native_modes_keep_original_reservation);
	UT_RUN(test_claim_second_executor_cannot_consume_or_replace_first);
	UT_RUN(test_claim_partial_materials_never_reserve_an_executor_in_any_arrival_order);
	UT_RUN(test_claim_wrong_original_observation_never_binds_a_target);
	UT_RUN(test_claim_post_binding_native_change_or_cancel_releases_only_own_executor);
	UT_RUN(test_claim_provider_error_after_binding_runs_exact_finally_release);
	UT_RUN(test_claim_released_handle_cannot_end_a_later_executor_or_advance_native);
	UT_RUN(test_claim_every_claim_identity_is_required_for_release);
	UT_RUN(test_claim_legacy_executor_apis_cannot_consume_full_episode_claim);
	UT_RUN(test_claim_release_after_cancel_or_gate_close_preserves_original_debt);
	UT_RUN(test_claim_pre_native_release_cannot_erase_an_installation_handoff);
	UT_RUN(test_claim_sequence_exhaustion_and_orphan_target_preserve_inputs);
	UT_RUN(test_claim_cancelled_or_failed_round_never_binds_an_executor);
	UT_RUN(test_claim_invalid_outputs_preserve_canaries_and_original_owner);
	UT_RUN(test_claim_corrupt_empty_owner_neither_acquires_nor_releases_a_capability);
	UT_RUN(test_claim_unproved_domain_change_retains_old_executor_without_export);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
