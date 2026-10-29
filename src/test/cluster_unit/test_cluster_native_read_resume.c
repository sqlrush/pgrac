/* Recover the original execution after a failed local handoff postcheck.
 * Actual PCM/native/IO/DONE bodies run; admission and event leaves are
 * controlled. No test here qualifies a whole host interleaving row. */
#include "test_cluster_native_read_settle_fixture.inc"

static ResourceXDeliveryClaim resumed_execution;
static uint8 resumed_phase;

static void
resume_canaries(void)
{
	memset(&resumed_execution, 0xA5, sizeof(resumed_execution));
	resumed_phase = UINT8_C(0xA5);
}

static void
resume_no_output(void)
{
	ResourceXDeliveryClaim canary;
	memset(&canary, 0xA5, sizeof(canary));
	UT_ASSERT(memcmp(&canary, &resumed_execution, sizeof(canary)) == 0);
	UT_ASSERT_EQ(resumed_phase, UINT8_C(0xA5));
}

static ResourceXApplyResult
resume_run(void)
{
#ifdef EXECUTOR_RESUME_RED
	ResourceXDecodedFrame grant, image;
	ResourceXRequesterJoinSnapshot join;
	ResourceXApplyResult r = cluster_bufmgr_pcm_own_acquire_execute_begin_exact(
		&request_buf, &request_base, &prepared_episode, &request_admission, &prepared_claim,
		&grant, &image, &join, &resumed_execution);
	if (r == RESOURCE_X_APPLY_APPLIED)
		resumed_phase = RESOURCE_X_DELIVERY_ACQUIRE_EXECUTING;
	return r;
#else
	return cluster_bufmgr_pcm_own_acquire_resume_disk_exact(&request_buf, &request_base,
		&prepared_episode, &request_admission, &prepared_claim, &resumed_execution, &resumed_phase);
#endif
}

static bool
resume_start(EvidenceFixture *f)
{
	ready_peer_ok = true; ready_connection_generation = 61; bootstrap_ingress_generation = 0;
	auth_connection_valid = auth_slot_valid = auth_fresh = true;
	auth_peer_boot = 31; auth_slot_generation = 1; auth_epoch = 17;
	fake_rx_capability_generation = 61; auth_observation_hook = NULL;
	if (!complete_start(f)) return false;
	resume_canaries();
	return true;
}

static void
resume_original(uint8 phase)
{
	UT_ASSERT_EQ(resumed_phase, phase);
	UT_ASSERT(memcmp(&resumed_execution, &execution_claim, sizeof(execution_claim)) == 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_sequence, 1);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.episode, 1);
	UT_ASSERT_EQ(request_shared.rings[1].count, 1);
	complete_idle();
}

static bool
resume_catches(void)
{
	volatile bool caught = false;
	PG_TRY(); { (void)resume_run(); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	return caught;
}

UT_TEST(test_resume_original_prepared_handle_restores_execution_without_reentry)
{
	EvidenceFixture f; ClusterPcmOwnEntry own; ClusterPcmResourceXBootstrapRound round;
	UT_ASSERT(resume_start(&f)); own = request_own; round = request_entry()->resource_x_bootstrap_round;
	UT_ASSERT_EQ(resume_run(), RESOURCE_X_APPLY_APPLIED);
	resume_original(RESOURCE_X_DELIVERY_ACQUIRE_EXECUTING);
	UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_commits, 0);
}

UT_TEST(test_resume_twenty_observations_never_claim_a_second_executor)
{
	EvidenceFixture f;
	UT_ASSERT(resume_start(&f));
	for (int n = 0; n < 20; n++) {
		UT_ASSERT_EQ(resume_run(), RESOURCE_X_APPLY_APPLIED);
		resume_original(RESOURCE_X_DELIVERY_ACQUIRE_EXECUTING);
	}
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_io_calls, 0);
}

UT_TEST(test_resume_lost_begin_output_after_provider_ERROR_recovers_same_owner)
{
	EvidenceFixture f; ResourceXDeliveryClaim canary; volatile bool caught = false;
	UT_ASSERT(execution_start(&f, 0, -1)); canary = execution_claim;
	request_hook = stage_raise;
	PG_TRY(); { (void)execution_enter(); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught); request_hook = NULL;
	UT_ASSERT(memcmp(&canary, &execution_claim, sizeof(canary)) == 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 4);
	resume_canaries(); UT_ASSERT_EQ(resume_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(resumed_phase, 4); UT_ASSERT_EQ(resumed_execution.purpose, 4);
	UT_ASSERT_EQ(resumed_execution.executor_sequence, prepared_claim.executor_sequence);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
	UT_ASSERT_EQ(stage_leave(&prepared_claim), RESOURCE_X_APPLY_STALE);
}

UT_TEST(test_resume_restored_capability_drives_original_C_and_DONE_once)
{
	EvidenceFixture f; ResourceXDeliveryClaim saved;
	UT_ASSERT(resume_start(&f)); saved = execution_claim;
	UT_ASSERT_EQ(resume_run(), RESOURCE_X_APPLY_APPLIED); execution_claim = resumed_execution;
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(resume_run(), RESOURCE_X_APPLY_DUPLICATE);
	resume_original(RESOURCE_X_DELIVERY_ACQUIRE_READ_COMPLETE);
	UT_ASSERT(memcmp(&saved, &execution_claim, sizeof(saved)) == 0);
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_io_calls, 1);
	UT_ASSERT_EQ(complete_broadcasts, 1); UT_ASSERT_EQ(disk_reads, 1);
}

UT_TEST(test_resume_armed_before_native_C_waits_without_repeating_disk_IO)
{
	EvidenceFixture f; ClusterPcmOwnEntry own;
	UT_ASSERT(resume_start(&f)); complete_on_content = stage_header_error;
	UT_ASSERT(complete_catches()); complete_on_content = NULL; own = request_own;
	for (int n = 0; n < 3; n++) UT_ASSERT_EQ(resume_run(), RESOURCE_X_APPLY_LOCAL_WAIT);
	resume_no_output(); UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
	UT_ASSERT_EQ(disk_reads, 1); UT_ASSERT_EQ(complete_commits, 0); complete_idle();
}

UT_TEST(test_resume_armed_after_native_C_keeps_input_IO_and_exact_fence)
{
	EvidenceFixture f; ClusterPcmOwnEntry own;
	UT_ASSERT(resume_start(&f)); complete_before_io_error = true;
	UT_ASSERT(complete_catches()); own = request_own;
	UT_ASSERT_EQ(resume_run(), RESOURCE_X_APPLY_LOCAL_WAIT); resume_no_output();
	UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_io_calls, 1);
	UT_ASSERT_EQ(complete_broadcasts, 0); UT_ASSERT_EQ(disk_reads, 1); complete_idle();
}

UT_TEST(test_resume_armed_after_IO_keeps_unpublished_shared_C_responsibility)
{
	EvidenceFixture f; ClusterPcmOwnEntry own;
	UT_ASSERT(resume_start(&f)); complete_after_io_error = true;
	UT_ASSERT(complete_catches()); own = request_own;
	UT_ASSERT_EQ(resume_run(), RESOURCE_X_APPLY_LOCAL_WAIT); resume_no_output();
	UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_io_calls, 1);
	UT_ASSERT_EQ(complete_broadcasts, 1); UT_ASSERT_EQ(disk_reads, 1); complete_idle();
}

UT_TEST(test_resume_shared_C_only_releases_its_original_leftover_hold)
{
	EvidenceFixture f;
	UT_ASSERT(resume_start(&f)); complete_publish_error = true;
	UT_ASSERT(complete_catches()); complete_publish_error = false;
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
	UT_ASSERT_EQ(resume_run(), RESOURCE_X_APPLY_DUPLICATE);
	resume_original(RESOURCE_X_DELIVERY_ACQUIRE_READ_COMPLETE); complete_finished();
}

UT_TEST(test_resume_completed_S_observation_is_a_noop)
{
	EvidenceFixture f; ClusterPcmOwnEntry own; ClusterPcmResourceXBootstrapRound round;
	UT_ASSERT(resume_start(&f)); UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_APPLIED);
	own = request_own; round = request_entry()->resource_x_bootstrap_round;
	UT_ASSERT_EQ(resume_run(), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	resume_original(6); complete_finished();
}

UT_TEST(test_resume_notice_does_not_rearm_or_retire_original_DONE)
{
	EvidenceFixture f; ResourceXDecodedFrame ack; ClusterPcmResourceXReadIntent notice;
	UT_ASSERT(settle_start(&f, &ack)); resume_canaries(); notice = *done_intent;
	UT_ASSERT_EQ(resume_run(), RESOURCE_X_APPLY_DUPLICATE); resume_original(7);
	UT_ASSERT(memcmp(&notice, done_intent, sizeof(notice)) == 0); settle_once(7);
}

UT_TEST(test_resume_consumed_STAGED_notice_remains_with_physical_owner)
{
	EvidenceFixture f; ResourceXDecodedFrame ack; ClusterPcmResourceXReadIntent notice;
	UT_ASSERT(settle_start(&f, &ack)); resume_canaries(); (void)settle_stage();
	UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED); notice = *done_intent;
	UT_ASSERT_EQ(resume_run(), RESOURCE_X_APPLY_DUPLICATE); resume_original(8);
	UT_ASSERT(memcmp(&notice, done_intent, sizeof(notice)) == 0); settle_once(8);
}

UT_TEST(test_resume_settled_keeps_original_A_I_stable_S_cover)
{
	EvidenceFixture f; ResourceXDecodedFrame ack; ClusterPcmResourceXBootstrapRound round;
	UT_ASSERT(settle_start(&f, &ack)); resume_canaries();
	UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(settle_run(), RESOURCE_X_APPLY_APPLIED); round = request_entry()->resource_x_bootstrap_round;
	UT_ASSERT_EQ(resume_run(), RESOURCE_X_APPLY_DUPLICATE); resume_original(9);
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	settle_once(9);
}

UT_TEST(test_resume_never_handed_off_preparation_is_not_execution)
{
	EvidenceFixture f;
	UT_ASSERT(execution_start(&f, 0, -1)); resume_canaries();
	UT_ASSERT(resume_run() != RESOURCE_X_APPLY_APPLIED); resume_no_output();
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 3);
	UT_ASSERT_EQ(stage_leave(&prepared_claim), RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_resume_wrong_original_capability_preserves_exact_owner)
{
	for (int n = 0; n < 11; n++) {
		EvidenceFixture f; ClusterPcmResourceXBootstrapRound round;
		UT_ASSERT(resume_start(&f)); round = request_entry()->resource_x_bootstrap_round;
		if (n == 0) prepared_claim.executor_sequence++;
		if (n == 1) prepared_claim.reserved[0] = 1;
		if (n == 2) prepared_claim.observation.entry_binding_generation++;
		if (n == 3) prepared_claim.observation.request.assertion_sequence++;
		if (n == 4) prepared_claim.observation.master_node++;
		if (n == 5) prepared_claim.observation.master_ingress_connection_generation++;
		if (n == 6) prepared_claim.observation.r4_record_generation++;
		if (n == 7) prepared_claim.target.generation++;
		if (n == 8) prepared_claim.target.reservation_token++;
		if (n == 9) prepared_claim.target.buffer_id_plus_one++;
		if (n == 10) prepared_claim.target.flags = 0;
		UT_ASSERT(resume_run() != RESOURCE_X_APPLY_APPLIED); resume_no_output();
		UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	}
}

UT_TEST(test_resume_every_full_E_coordinate_stays_bound_to_original_round)
{
	for (int n = 0; n < 9; n++) {
		EvidenceFixture f;
		UT_ASSERT(resume_start(&f));
		if (n == 0) prepared_episode.requester_incarnation++;
		if (n == 1) prepared_episode.backend_generation++;
		if (n == 2) prepared_episode.base_generation++;
		if (n == 3) prepared_episode.reservation_token++;
		if (n == 4) prepared_episode.cluster_epoch++;
		if (n == 5) prepared_episode.r4_generation++;
		if (n == 6) prepared_episode.backend_id++;
		if (n == 7) prepared_episode.buffer_id++;
		if (n == 8) prepared_episode.path = RESOURCE_X_ACQUIRE_RESIDENT;
		UT_ASSERT(resume_run() != RESOURCE_X_APPLY_APPLIED); resume_no_output();
		UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1); UT_ASSERT_EQ(disk_reads, 0);
	}
}

UT_TEST(test_resume_process_pin_admission_and_original_IO_owner_are_required)
{
	for (int n = 0; n < 8; n++) {
		EvidenceFixture f;
		UT_ASSERT(resume_start(&f));
		if (n == 0) MyProc->pid++;
		if (n == 1) MyProc->cluster_grd_generation++;
		if (n == 2) request_pins = 0;
		if (n == 3) CurrentResourceOwner = NULL;
		if (n == 4) ResourceOwnerForgetBufferIO(original_io_owner, 1);
		if (n == 5) request_admission.entered = false;
		if (n == 6) request_admission.record_generation++;
		if (n == 7) MyBackendId++;
		UT_ASSERT(resume_run() != RESOURCE_X_APPLY_APPLIED); resume_no_output();
		UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1); complete_idle();
	}
}

UT_TEST(test_resume_pre_C_native_turnover_cannot_return_execution_permission)
{
	for (int n = 0; n < 9; n++) {
		EvidenceFixture f; ClusterPcmOwnEntry own;
		UT_ASSERT(resume_start(&f));
		if (n == 0) pg_atomic_write_u64(&request_own.generation, 49);
		if (n == 1) pg_atomic_write_u64(&request_own.reservation_token, request_token + 1);
		if (n == 2) pg_atomic_write_u64(&request_own.delivery_attempt, 2);
		if (n == 3) pg_atomic_write_u64(&request_own.write_io_generation, 1);
		if (n == 4) request_buf.tag.blockNum++;
		if (n == 5) request_buf.pcm_state = PCM_STATE_X;
		if (n == 6) request_buf.buffer_type = BUF_TYPE_PI;
		if (n == 7) pg_atomic_write_u32(&request_buf.state, BM_TAG_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR);
		if (n == 8) pg_atomic_write_u32(&request_own.flags, 0);
		own = request_own;
		UT_ASSERT(resume_run() != RESOURCE_X_APPLY_APPLIED); resume_no_output();
		UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0); complete_idle();
	}
}

UT_TEST(test_resume_completed_native_turnover_does_not_expose_an_old_success)
{
	for (int n = 0; n < 10; n++) {
		EvidenceFixture f; ClusterPcmOwnEntry own;
		UT_ASSERT(resume_start(&f)); UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_APPLIED);
		if (n == 0) pg_atomic_write_u64(&request_own.generation, 50);
		if (n == 1) pg_atomic_write_u64(&request_own.reservation_token, request_token + 1);
		if (n == 2) pg_atomic_write_u64(&request_own.delivery_attempt, 2);
		if (n == 3) pg_atomic_write_u64(&request_own.write_io_generation, 1);
		if (n == 4) pg_atomic_write_u64(&request_own.write_io_generation, 2);
		if (n == 5) request_buf.tag.blockNum++;
		if (n == 6) request_buf.pcm_state = PCM_STATE_X;
		if (n == 7) request_buf.buffer_type = BUF_TYPE_PI;
		if (n == 8) pg_atomic_write_u32(&request_buf.state, BM_TAG_VALID | BM_VALID | BM_IO_ERROR);
		if (n == 9) ResourceOwnerRememberBufferIO(original_io_owner, 1);
		own = request_own;
		UT_ASSERT(resume_run() != RESOURCE_X_APPLY_DUPLICATE); resume_no_output();
		UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0); complete_idle();
	}
}

UT_TEST(test_resume_retained_floor_and_entry_domain_changes_keep_shared_debt)
{
	for (int n = 0; n < 5; n++) {
		EvidenceFixture f;
		UT_ASSERT(resume_start(&f));
		if (n == 0) request_entry()->binding_generation++;
		if (n == 1) request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor =
			request_entry()->resource_x_bootstrap_round.request.assertion_sequence;
		if (n == 2) request_entry()->resource_x_bootstrap_round.failed_attempt_floor =
			request_entry()->resource_x_bootstrap_round.request.assertion_sequence;
		if (n == 3) request_entry()->resource_x_bootstrap_round.current_master_node = 2;
		if (n == 4) request_entry()->resource_x_bootstrap_round.master_ingress_connection_generation++;
		UT_ASSERT(resume_run() != RESOURCE_X_APPLY_APPLIED); resume_no_output();
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 4);
		UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
	}
}

UT_TEST(test_resume_after_header_native_change_is_rejected_on_exact_recheck)
{
	EvidenceFixture f;
	UT_ASSERT(resume_start(&f)); stage_header_hook = disk_turnover;
	UT_ASSERT(resume_run() != RESOURCE_X_APPLY_APPLIED); resume_no_output();
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1); complete_idle();
}

static void
resume_change_entry(void)
{
	request_entry()->resource_x_bootstrap_round.master_ingress_connection_generation++;
}

UT_TEST(test_resume_entry_change_at_header_boundary_is_not_permission)
{
	EvidenceFixture f;
	UT_ASSERT(resume_start(&f)); stage_header_hook = resume_change_entry;
	UT_ASSERT(resume_run() != RESOURCE_X_APPLY_APPLIED); resume_no_output();
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
}

UT_TEST(test_resume_provider_ERROR_preserves_output_and_all_original_owners)
{
	EvidenceFixture f; ClusterPcmResourceXBootstrapRound round;
	UT_ASSERT(resume_start(&f)); round = request_entry()->resource_x_bootstrap_round;
	request_rechecks = 0; request_hook = stage_raise;
	UT_ASSERT(resume_catches()); request_hook = NULL; resume_no_output();
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1); complete_idle();
}

UT_TEST(test_resume_held_content_lock_never_enters_entry_or_provider)
{
	EvidenceFixture f; int before;
	UT_ASSERT(resume_start(&f)); before = request_rechecks;
	LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE);
	UT_ASSERT_EQ(resume_run(), RESOURCE_X_APPLY_BAD_STATE); resume_no_output();
	UT_ASSERT_EQ(request_rechecks, before);
	LWLockRelease(BufferDescriptorGetContentLock(&request_buf)); complete_idle();
}

UT_TEST(test_resume_other_modes_and_source_plans_cannot_use_disk_S_restore)
{
	for (int n = 1; n < 8; n++) {
		EvidenceFixture f;
		UT_ASSERT(execution_start(&f, n, -1)); UT_ASSERT_EQ(execution_enter(), RESOURCE_X_APPLY_APPLIED);
		resume_canaries(); UT_ASSERT(resume_run() != RESOURCE_X_APPLY_APPLIED); resume_no_output();
		UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
	}
}

UT_TEST(test_resume_post_C_ERROR_does_not_repeat_C_or_erase_receipt)
{
	EvidenceFixture f; ClusterPcmResourceXBootstrapRound round;
	UT_ASSERT(resume_start(&f)); UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_APPLIED);
	round = request_entry()->resource_x_bootstrap_round; request_rechecks = 0; request_hook = stage_raise;
	UT_ASSERT(resume_catches()); request_hook = NULL; resume_no_output();
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_io_calls, 1); complete_idle();
}

UT_TEST(test_resume_old_prepared_cleanup_cannot_withdraw_restored_execution)
{
	EvidenceFixture f;
	UT_ASSERT(resume_start(&f)); UT_ASSERT_EQ(resume_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(stage_leave(&prepared_claim), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_delivery_claim_end_exact(&resumed_execution), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
}

UT_TEST(test_resume_unavailable_master_observation_never_restores_old_authority)
{
	for (int n = 0; n < 2; n++) {
		EvidenceFixture f;
		ResourceXApplyResult r;
		UT_ASSERT(resume_start(&f));
		if (n) UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_APPLIED);
		auth_connection_valid = false;
		r = resume_run();
		UT_ASSERT(r != RESOURCE_X_APPLY_APPLIED && r != RESOURCE_X_APPLY_DUPLICATE);
		resume_no_output(); complete_idle();
	}
}

static void
resume_change_connection(void)
{
	fake_rx_capability_generation++;
}

UT_TEST(test_resume_connection_turnover_before_or_during_native_observation_rejects)
{
	for (int n = 0; n < 2; n++) {
		EvidenceFixture f;
		UT_ASSERT(resume_start(&f));
		if (n == 0) fake_rx_capability_generation++;
		else stage_header_hook = resume_change_connection;
		UT_ASSERT(resume_run() != RESOURCE_X_APPLY_APPLIED); resume_no_output();
		UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1); complete_idle();
	}
}

UT_TEST(test_resume_original_executing_capability_is_also_exact)
{
	EvidenceFixture f;
	UT_ASSERT(resume_start(&f)); prepared_claim = execution_claim;
	UT_ASSERT_EQ(resume_run(), RESOURCE_X_APPLY_APPLIED); resume_original(4);
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_io_calls, 0);
}

UT_TEST(test_resume_other_purposes_cannot_be_normalized_into_execution)
{
	const uint8 purposes[] = {0, 1, 2, 5, 6, 7, 8, 9, UINT8_MAX};
	for (unsigned n = 0; n < lengthof(purposes); n++) {
		EvidenceFixture f;
		UT_ASSERT(resume_start(&f)); prepared_claim.purpose = purposes[n];
		UT_ASSERT(resume_run() != RESOURCE_X_APPLY_APPLIED); resume_no_output();
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 4);
	}
}

UT_TEST(test_resume_base_snapshot_cannot_be_replaced_after_C)
{
	for (int n = 0; n < 5; n++) {
		EvidenceFixture f;
		UT_ASSERT(resume_start(&f)); UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_APPLIED);
		if (n == 0) request_base.flags = PCM_OWN_FLAG_GRANT_PENDING;
		if (n == 1) request_base.reservation_token++;
		if (n == 2) request_base.generation++;
		if (n == 3) request_base.tag.blockNum++;
		if (n == 4) request_base.reservation_token = UINT64_MAX - 1;
		UT_ASSERT(resume_run() != RESOURCE_X_APPLY_DUPLICATE); resume_no_output(); complete_idle();
	}
}

static void
resume_complete_during_observation(void)
{
	request_hook = NULL;
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_resume_C_during_observation_never_returns_a_second_IO_permission)
{
	EvidenceFixture f;
	UT_ASSERT(resume_start(&f)); request_rechecks = 0; request_hook = resume_complete_during_observation;
	UT_ASSERT(resume_run() != RESOURCE_X_APPLY_APPLIED); resume_no_output();
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_io_calls, 1); UT_ASSERT_EQ(disk_reads, 1);
	UT_ASSERT_EQ(resume_run(), RESOURCE_X_APPLY_DUPLICATE); resume_original(6);
}

UT_TEST(test_resume_notice_never_releases_a_later_same_sequence_hold)
{
	EvidenceFixture f; ResourceXDecodedFrame ack; ClusterPcmResourceXReadIntent notice;
	UT_ASSERT(settle_start(&f, &ack)); resume_canaries(); notice = *done_intent;
	pg_atomic_write_u64(&request_own.delivery_attempt, execution_claim.executor_sequence);
	UT_ASSERT(resume_run() != RESOURCE_X_APPLY_DUPLICATE); resume_no_output();
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), execution_claim.executor_sequence);
	UT_ASSERT(memcmp(&notice, done_intent, sizeof(notice)) == 0);
}

UT_TEST(test_resume_master_boot_epoch_freshness_and_slot_generations_are_independent)
{
	for (int n = 0; n < 7; n++) {
		EvidenceFixture f;
		UT_ASSERT(resume_start(&f));
		if (n == 0) auth_peer_boot++;
		if (n == 1) auth_epoch++;
		if (n == 2) auth_slot_valid = false;
		if (n == 3) auth_fresh = false;
		if (n == 4) auth_slot_generation = 0;
		if (n == 5) auth_slot_generation = UINT64_MAX;
		if (n == 6) fake_rx_capability_generation = UINT32_MAX;
		UT_ASSERT(resume_run() != RESOURCE_X_APPLY_APPLIED); resume_no_output();
		UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
	}
}

UT_TEST(test_resume_invalid_peer_coordinates_are_rejected_before_provider_access)
{
	for (int n = 0; n < 4; n++) {
		EvidenceFixture f; unsigned connections, slots;
		UT_ASSERT(resume_start(&f)); connections = auth_connection_reads; slots = auth_slot_reads;
		if (n == 0) prepared_claim.observation.master_node = -1;
		if (n == 1) prepared_claim.observation.master_node = RESOURCE_X_PROTOCOL_NODE_LIMIT;
		if (n == 2) prepared_claim.observation.master_ingress_connection_generation = 0;
		if (n == 3) prepared_claim.observation.master_ingress_connection_generation = UINT32_MAX;
		UT_ASSERT(resume_run() != RESOURCE_X_APPLY_APPLIED); resume_no_output();
		UT_ASSERT_EQ(auth_connection_reads, connections); UT_ASSERT_EQ(auth_slot_reads, slots);
	}
}

int
main(void)
{
	UT_PLAN(35);
	UT_RUN(test_resume_original_prepared_handle_restores_execution_without_reentry);
	UT_RUN(test_resume_twenty_observations_never_claim_a_second_executor);
	UT_RUN(test_resume_lost_begin_output_after_provider_ERROR_recovers_same_owner);
	UT_RUN(test_resume_restored_capability_drives_original_C_and_DONE_once);
	UT_RUN(test_resume_armed_before_native_C_waits_without_repeating_disk_IO);
	UT_RUN(test_resume_armed_after_native_C_keeps_input_IO_and_exact_fence);
	UT_RUN(test_resume_armed_after_IO_keeps_unpublished_shared_C_responsibility);
	UT_RUN(test_resume_shared_C_only_releases_its_original_leftover_hold);
	UT_RUN(test_resume_completed_S_observation_is_a_noop);
	UT_RUN(test_resume_notice_does_not_rearm_or_retire_original_DONE);
	UT_RUN(test_resume_consumed_STAGED_notice_remains_with_physical_owner);
	UT_RUN(test_resume_settled_keeps_original_A_I_stable_S_cover);
	UT_RUN(test_resume_never_handed_off_preparation_is_not_execution);
	UT_RUN(test_resume_wrong_original_capability_preserves_exact_owner);
	UT_RUN(test_resume_every_full_E_coordinate_stays_bound_to_original_round);
	UT_RUN(test_resume_process_pin_admission_and_original_IO_owner_are_required);
	UT_RUN(test_resume_pre_C_native_turnover_cannot_return_execution_permission);
	UT_RUN(test_resume_completed_native_turnover_does_not_expose_an_old_success);
	UT_RUN(test_resume_retained_floor_and_entry_domain_changes_keep_shared_debt);
	UT_RUN(test_resume_after_header_native_change_is_rejected_on_exact_recheck);
	UT_RUN(test_resume_entry_change_at_header_boundary_is_not_permission);
	UT_RUN(test_resume_provider_ERROR_preserves_output_and_all_original_owners);
	UT_RUN(test_resume_held_content_lock_never_enters_entry_or_provider);
	UT_RUN(test_resume_other_modes_and_source_plans_cannot_use_disk_S_restore);
	UT_RUN(test_resume_post_C_ERROR_does_not_repeat_C_or_erase_receipt);
	UT_RUN(test_resume_old_prepared_cleanup_cannot_withdraw_restored_execution);
	UT_RUN(test_resume_unavailable_master_observation_never_restores_old_authority);
	UT_RUN(test_resume_connection_turnover_before_or_during_native_observation_rejects);
	UT_RUN(test_resume_original_executing_capability_is_also_exact);
	UT_RUN(test_resume_other_purposes_cannot_be_normalized_into_execution);
	UT_RUN(test_resume_base_snapshot_cannot_be_replaced_after_C);
	UT_RUN(test_resume_C_during_observation_never_returns_a_second_IO_permission);
	UT_RUN(test_resume_notice_never_releases_a_later_same_sequence_hold);
	UT_RUN(test_resume_master_boot_epoch_freshness_and_slot_generations_are_independent);
	UT_RUN(test_resume_invalid_peer_coordinates_are_rejected_before_provider_access);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
