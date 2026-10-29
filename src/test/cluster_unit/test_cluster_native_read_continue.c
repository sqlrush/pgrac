/*-------------------------------------------------------------------------
 * test_cluster_native_read_continue.c
 *   Exercise exact disk-read continuation across native completion cuts.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_native_read_continue.c
 *
 * NOTES
 *   This is a pgrac-original fixture. Product symbols retain cluster_
 *   prefixes; local test helpers use armed_ prefixes.
 *-------------------------------------------------------------------------
 */

/* Continue the same verified ARMED disk episode across real native cuts.
 * The original native/PCM/IO/codec owners run; only process/event leaves
 * and the explicit RED missing-continuation seam are controlled. */
#include "test_cluster_native_read_resume_fixture.inc"
#ifndef ARMED_CONTINUE_RED
#define LockBufHdr stage_header_lock
#define LWLockAcquire complete_content_acquire
#define GetPrivateRefCount(buffer) request_private_pin(buffer)
#define cluster_pcm_lock_resource_x_requester_acquire_read_complete_exact complete_publish
#include "test_cluster_native_read_continue.inc"
#undef cluster_pcm_lock_resource_x_requester_acquire_read_complete_exact
#undef GetPrivateRefCount
#undef LWLockAcquire
#undef LockBufHdr
#endif

static ResourceXDeliveryClaim armed_capability;

static ResourceXApplyResult
armed_advance(void)
{
#ifdef ARMED_CONTINUE_RED
	return complete_run();
#else
	return cluster_bufmgr_pcm_own_acquire_continue_disk_exact(&request_buf, &request_base,
		&prepared_episode, &request_admission, &armed_capability);
#endif
}

static bool
armed_start(EvidenceFixture *f, unsigned cut)
{
	if (!resume_start(f)) return false;
	armed_capability = prepared_claim;
	if (cut == 0) complete_on_content = stage_header_error;
	if (cut == 1) complete_before_io_error = true;
	if (cut == 2) complete_after_io_error = true;
	if (!complete_catches()) return false;
	complete_on_content = complete_on_broadcast = NULL;
	complete_before_io_error = complete_after_io_error = complete_publish_error = false;
	stage_header_hook = NULL;
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 5);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
	UT_ASSERT_EQ(disk_reads, 1);
	complete_idle();
	return true;
}

static void
armed_finished(unsigned cut)
{
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 6);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_SCUR);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR), BM_VALID);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(complete_commits, 1);
	/* The pre-IO ERROR wrapper counted its failed call, not a real IO end. */
	UT_ASSERT_EQ(complete_io_calls, cut == 1 ? 2 : 1);
	UT_ASSERT_EQ(complete_broadcasts, 1); UT_ASSERT_EQ(disk_reads, 1);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.episode, 1);
	UT_ASSERT_EQ(request_shared.rings[1].count, 1); complete_idle();
}

static void
armed_refused(void)
{
	ClusterPcmOwnEntry own = request_own;
	ClusterPcmResourceXBootstrapRound round = request_entry()->resource_x_bootstrap_round;
	uint32 state = pg_atomic_read_u32(&request_buf.state);
	unsigned commits = complete_commits, calls = complete_io_calls, wakes = complete_broadcasts;
	ResourceXApplyResult result = armed_advance();
	UT_ASSERT(result != RESOURCE_X_APPLY_APPLIED && result != RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state), state);
	UT_ASSERT_EQ(complete_commits, commits); UT_ASSERT_EQ(complete_io_calls, calls);
	UT_ASSERT_EQ(complete_broadcasts, wakes); UT_ASSERT_EQ(disk_reads, 1); complete_idle();
}

static bool
armed_catches(void)
{
	volatile bool caught = false;
	PG_TRY(); { (void)armed_advance(); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	return caught;
}

UT_TEST(test_continue_before_native_C_commits_original_verified_I_without_reread)
{
	EvidenceFixture f;
	UT_ASSERT(armed_start(&f, 0));
	UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(complete_io_calls, 0);
	UT_ASSERT_EQ(armed_advance(), RESOURCE_X_APPLY_APPLIED); armed_finished(0);
}

UT_TEST(test_continue_after_native_C_only_ends_original_input_IO)
{
	EvidenceFixture f;
	UT_ASSERT(armed_start(&f, 1));
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_broadcasts, 0);
	UT_ASSERT_EQ(armed_advance(), RESOURCE_X_APPLY_APPLIED); armed_finished(1);
}

UT_TEST(test_continue_after_IO_only_publishes_original_shared_C)
{
	EvidenceFixture f;
	UT_ASSERT(armed_start(&f, 2));
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_broadcasts, 1);
	UT_ASSERT_EQ(armed_advance(), RESOURCE_X_APPLY_APPLIED); armed_finished(2);
}

UT_TEST(test_continue_all_three_cuts_replay_without_second_C_IO_or_wakeup)
{
	for (unsigned cut = 0; cut < 3; cut++) {
		EvidenceFixture f; ClusterPcmOwnEntry own; ClusterPcmResourceXBootstrapRound round;
		UT_ASSERT(armed_start(&f, cut)); UT_ASSERT_EQ(armed_advance(), RESOURCE_X_APPLY_APPLIED);
		own = request_own; round = request_entry()->resource_x_bootstrap_round;
		for (int n = 0; n < 5; n++) UT_ASSERT_EQ(armed_advance(), RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
		UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
		armed_finished(cut);
	}
}

UT_TEST(test_continue_accepts_only_original_prepared_or_executing_capability)
{
	for (unsigned cut = 0; cut < 3; cut++) {
		EvidenceFixture f;
		UT_ASSERT(armed_start(&f, cut)); armed_capability = execution_claim;
		UT_ASSERT_EQ(armed_advance(), RESOURCE_X_APPLY_APPLIED); armed_finished(cut);
	}
}

UT_TEST(test_continue_preserves_same_original_full_A_I_receipt)
{
	for (unsigned cut = 0; cut < 3; cut++) {
		EvidenceFixture f; ResourceXAcquireReadReceipt receipt, observed; ResourceXDeliveryClaim current;
		UT_ASSERT(armed_start(&f, cut)); receipt = request_entry()->resource_x_bootstrap_round.acquire_read_receipt;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_read_owner_exact(&execution_claim,
			&prepared_episode, &current, &observed), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&receipt, &observed, sizeof(receipt)) == 0);
		UT_ASSERT_EQ(armed_advance(), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&receipt, &request_entry()->resource_x_bootstrap_round.acquire_read_receipt, sizeof(receipt)) == 0);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_read_owner_exact(&execution_claim,
			&prepared_episode, &current, &observed), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&receipt, &observed, sizeof(receipt)) == 0);
		armed_finished(cut);
	}
}

UT_TEST(test_continue_wrong_page_bytes_never_commit_or_end_IO)
{
	for (unsigned cut = 0; cut < 3; cut++) {
		EvidenceFixture f;
		UT_ASSERT(armed_start(&f, cut)); disk_shared.data[256] ^= 1; armed_refused();
	}
}

UT_TEST(test_continue_page_LSN_and_SCN_must_match_the_retained_I)
{
	for (unsigned cut = 0; cut < 3; cut++) for (int n = 0; n < 2; n++) {
		EvidenceFixture f; PageHeader page;
		UT_ASSERT(armed_start(&f, cut)); page = (PageHeader)disk_shared.data;
		if (n == 0) PageSetLSNPreserveOrigin((Page)page, PageGetLSN(page) + 1);
		else page->pd_block_scn++;
		armed_refused();
	}
}

UT_TEST(test_continue_native_generation_token_tag_and_mode_turnover_keep_debt)
{
	for (unsigned cut = 0; cut < 3; cut++) for (int n = 0; n < 5; n++) {
		EvidenceFixture f;
		UT_ASSERT(armed_start(&f, cut));
		if (n == 0) pg_atomic_write_u64(&request_own.generation, 60);
		if (n == 1) pg_atomic_write_u64(&request_own.reservation_token, request_token + 1);
		if (n == 2) request_buf.tag.blockNum++;
		if (n == 3) request_buf.pcm_state = PCM_STATE_X;
		if (n == 4) request_buf.buffer_type = BUF_TYPE_PI;
		armed_refused();
	}
}

UT_TEST(test_continue_later_delivery_hold_is_never_released_or_consumed)
{
	for (unsigned cut = 0; cut < 3; cut++) {
		EvidenceFixture f;
		UT_ASSERT(armed_start(&f, cut)); pg_atomic_write_u64(&request_own.delivery_attempt, 2);
		armed_refused(); UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 2);
	}
}

UT_TEST(test_continue_odd_or_changed_even_write_generation_is_not_same_I)
{
	for (unsigned cut = 0; cut < 3; cut++) for (int n = 1; n <= 2; n++) {
		EvidenceFixture f;
		UT_ASSERT(armed_start(&f, cut)); pg_atomic_write_u64(&request_own.write_io_generation, n);
		armed_refused();
	}
}

UT_TEST(test_continue_IO_error_and_invalid_header_shapes_do_not_gain_C)
{
	for (unsigned cut = 0; cut < 3; cut++) for (int n = 0; n < 3; n++) {
		EvidenceFixture f; uint32 state;
		UT_ASSERT(armed_start(&f, cut)); state = pg_atomic_read_u32(&request_buf.state);
		if (n == 0) state |= BM_IO_ERROR;
		if (n == 1) state &= ~BM_TAG_VALID;
		if (n == 2) state |= BM_DIRTY;
		pg_atomic_write_u32(&request_buf.state, state); armed_refused();
	}
}

UT_TEST(test_continue_original_IO_membership_must_match_native_cut)
{
	for (unsigned cut = 0; cut < 3; cut++) {
		EvidenceFixture f;
		UT_ASSERT(armed_start(&f, cut));
		if (cut == 2) ResourceOwnerRememberBufferIO(original_io_owner, 1);
		else ResourceOwnerForgetBufferIO(original_io_owner, 1);
		armed_refused();
	}
}

UT_TEST(test_continue_process_pin_ResourceOwner_and_admission_cannot_be_borrowed)
{
	for (unsigned cut = 0; cut < 3; cut++) for (int n = 0; n < 7; n++) {
		EvidenceFixture f;
		UT_ASSERT(armed_start(&f, cut));
		if (n == 0) MyProc->pid++;
		if (n == 1) MyProc->cluster_grd_generation++;
		if (n == 2) request_pins = 0;
		if (n == 3) CurrentResourceOwner = NULL;
		if (n == 4) request_admission.entered = false;
		if (n == 5) request_admission.record_generation++;
		if (n == 6) MyBackendId++;
		armed_refused();
	}
}

UT_TEST(test_continue_cancelled_old_attempt_or_changed_entry_never_consumes_successor)
{
	for (unsigned cut = 0; cut < 3; cut++) for (int n = 0; n < 4; n++) {
		EvidenceFixture f; ClusterPcmResourceXBootstrapRound *round;
		UT_ASSERT(armed_start(&f, cut)); round = &request_entry()->resource_x_bootstrap_round;
		if (n == 0) round->cancelled_attempt_floor = round->request.assertion_sequence;
		if (n == 1) round->failed_attempt_floor = round->request.assertion_sequence;
		if (n == 2) request_entry()->binding_generation++;
		if (n == 3) round->delivery_executor_sequence++;
		armed_refused();
	}
}

UT_TEST(test_continue_master_boot_epoch_connection_or_freshness_loss_retains_owner)
{
	for (unsigned cut = 0; cut < 3; cut++) for (int n = 0; n < 5; n++) {
		EvidenceFixture f;
		UT_ASSERT(armed_start(&f, cut));
		if (n == 0) auth_peer_boot++;
		if (n == 1) auth_epoch++;
		if (n == 2) fake_rx_capability_generation++;
		if (n == 3) auth_fresh = false;
		if (n == 4) auth_connection_valid = false;
		armed_refused();
	}
}

UT_TEST(test_continue_receipt_authority_decision_or_image_change_never_publishes)
{
	for (unsigned cut = 0; cut < 3; cut++) for (int n = 0; n < 4; n++) {
		EvidenceFixture f; ResourceXAcquireReadReceipt *receipt;
		UT_ASSERT(armed_start(&f, cut)); receipt = &request_entry()->resource_x_bootstrap_round.acquire_read_receipt;
		if (n == 0) receipt->authority_generation++;
		if (n == 1) receipt->decision_crc32c++;
		if (n == 2) receipt->page_crc32c++;
		if (n == 3) receipt->page_lsn++;
		armed_refused();
	}
}

UT_TEST(test_continue_capability_full_E_sequence_target_and_reserved_bytes_are_exact)
{
	for (int n = 0; n < 7; n++) {
		EvidenceFixture f;
		UT_ASSERT(armed_start(&f, 0));
		if (n == 0) armed_capability.executor_sequence++;
		if (n == 1) armed_capability.observation.request.assertion_sequence++;
		if (n == 2) armed_capability.target.generation++;
		if (n == 3) armed_capability.target.reservation_token++;
		if (n == 4) armed_capability.reserved[0] = 1;
		if (n == 5) prepared_episode.backend_generation++;
		if (n == 6) armed_capability.purpose = 5;
		armed_refused();
	}
}

UT_TEST(test_continue_content_boundary_native_turnover_blocks_original_actions)
{
	EvidenceFixture f;
	UT_ASSERT(armed_start(&f, 0)); complete_on_content = disk_turnover;
	UT_ASSERT(armed_advance() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(complete_io_calls, 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 5); complete_idle();
}

UT_TEST(test_continue_header_ERROR_keeps_same_armed_receipt_and_can_resume_later)
{
	EvidenceFixture f;
	UT_ASSERT(armed_start(&f, 0)); complete_on_content = stage_header_error;
	UT_ASSERT(armed_catches()); complete_on_content = NULL;
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 5);
	UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(complete_io_calls, 0);
	UT_ASSERT_EQ(armed_advance(), RESOURCE_X_APPLY_APPLIED); armed_finished(0);
}

UT_TEST(test_continue_repeated_before_IO_ERROR_does_not_repeat_native_commit)
{
	EvidenceFixture f;
	UT_ASSERT(armed_start(&f, 0)); complete_before_io_error = true;
	UT_ASSERT(armed_catches()); UT_ASSERT(armed_catches()); complete_before_io_error = false;
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_broadcasts, 0);
	UT_ASSERT_EQ(armed_advance(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(complete_io_calls, 3); UT_ASSERT_EQ(complete_broadcasts, 1);
	UT_ASSERT_EQ(disk_reads, 1); UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0); complete_idle();
}

UT_TEST(test_continue_after_IO_ERROR_never_repeats_IO_or_follower_wakeup)
{
	EvidenceFixture f;
	UT_ASSERT(armed_start(&f, 0)); complete_after_io_error = true;
	UT_ASSERT(armed_catches()); complete_after_io_error = false;
	UT_ASSERT_EQ(complete_broadcasts, 1);
	UT_ASSERT_EQ(armed_advance(), RESOURCE_X_APPLY_APPLIED); armed_finished(0);
}

UT_TEST(test_continue_after_shared_C_ERROR_releases_only_its_original_hold)
{
	EvidenceFixture f;
	UT_ASSERT(armed_start(&f, 0)); complete_publish_error = true;
	UT_ASSERT(armed_catches()); complete_publish_error = false;
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 6);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
	UT_ASSERT_EQ(armed_advance(), RESOURCE_X_APPLY_DUPLICATE); armed_finished(0);
}

UT_TEST(test_continue_cancel_after_IO_keeps_installed_resident_not_unpublished_abort)
{
	EvidenceFixture f;
	UT_ASSERT(armed_start(&f, 0)); complete_on_broadcast = disk_cancel;
	UT_ASSERT(armed_advance() != RESOURCE_X_APPLY_APPLIED); complete_on_broadcast = NULL;
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_broadcasts, 1);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S); UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_SCUR);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 5);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1); complete_idle();
}

UT_TEST(test_continue_held_content_lock_never_enters_entry_or_provider)
{
	EvidenceFixture f; unsigned checks;
	UT_ASSERT(armed_start(&f, 0)); checks = request_rechecks;
	LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE);
	UT_ASSERT_EQ(armed_advance(), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(request_rechecks, checks);
	LWLockRelease(BufferDescriptorGetContentLock(&request_buf)); complete_idle();
}

UT_TEST(test_continue_old_complete_and_observer_do_not_gain_armed_permission)
{
	for (unsigned cut = 0; cut < 3; cut++) {
		EvidenceFixture f;
		UT_ASSERT(armed_start(&f, cut));
		UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		resume_canaries(); UT_ASSERT_EQ(resume_run(), RESOURCE_X_APPLY_LOCAL_WAIT); resume_no_output();
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 5);
	}
}

UT_TEST(test_continue_exact_observer_after_recovery_sees_original_completed_capability)
{
	EvidenceFixture f;
	UT_ASSERT(armed_start(&f, 0)); UT_ASSERT_EQ(armed_advance(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(resume_run(), RESOURCE_X_APPLY_DUPLICATE); resume_original(6); armed_finished(0);
}

UT_TEST(test_continue_already_done_consumed_or_settled_never_rearms_notice)
{
	for (unsigned phase = 7; phase <= 9; phase++) {
		EvidenceFixture f; ResourceXDecodedFrame ack; ClusterPcmResourceXReadIntent notice;
		UT_ASSERT(settle_start(&f, &ack)); armed_capability = prepared_claim;
		if (phase >= 8) UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED);
		if (phase == 9) UT_ASSERT_EQ(settle_run(), RESOURCE_X_APPLY_APPLIED);
		notice = *done_intent;
		UT_ASSERT_EQ(armed_advance(), RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT(memcmp(&notice, done_intent, sizeof(notice)) == 0); settle_once(phase);
	}
}

UT_TEST(test_continue_later_hold_after_notice_is_never_released)
{
	EvidenceFixture f; ResourceXDecodedFrame ack; ClusterPcmResourceXReadIntent notice;
	UT_ASSERT(settle_start(&f, &ack)); armed_capability = prepared_claim; notice = *done_intent;
	pg_atomic_write_u64(&request_own.delivery_attempt, execution_claim.executor_sequence);
	UT_ASSERT(armed_advance() != RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), execution_claim.executor_sequence);
	UT_ASSERT(memcmp(&notice, done_intent, sizeof(notice)) == 0); complete_idle();
}

UT_TEST(test_continue_wrong_base_cannot_reclassify_native_C_as_a_fresh_input)
{
	for (unsigned cut = 0; cut < 3; cut++) for (int n = 0; n < 3; n++) {
		EvidenceFixture f;
		UT_ASSERT(armed_start(&f, cut));
		if (n == 0) request_base.flags = PCM_OWN_FLAG_GRANT_PENDING;
		if (n == 1) request_base.reservation_token++;
		if (n == 2) request_base.generation++;
		armed_refused();
	}
}

int
main(void)
{
	UT_PLAN(30);
	UT_RUN(test_continue_before_native_C_commits_original_verified_I_without_reread);
	UT_RUN(test_continue_after_native_C_only_ends_original_input_IO);
	UT_RUN(test_continue_after_IO_only_publishes_original_shared_C);
	UT_RUN(test_continue_all_three_cuts_replay_without_second_C_IO_or_wakeup);
	UT_RUN(test_continue_accepts_only_original_prepared_or_executing_capability);
	UT_RUN(test_continue_preserves_same_original_full_A_I_receipt);
	UT_RUN(test_continue_wrong_page_bytes_never_commit_or_end_IO);
	UT_RUN(test_continue_page_LSN_and_SCN_must_match_the_retained_I);
	UT_RUN(test_continue_native_generation_token_tag_and_mode_turnover_keep_debt);
	UT_RUN(test_continue_later_delivery_hold_is_never_released_or_consumed);
	UT_RUN(test_continue_odd_or_changed_even_write_generation_is_not_same_I);
	UT_RUN(test_continue_IO_error_and_invalid_header_shapes_do_not_gain_C);
	UT_RUN(test_continue_original_IO_membership_must_match_native_cut);
	UT_RUN(test_continue_process_pin_ResourceOwner_and_admission_cannot_be_borrowed);
	UT_RUN(test_continue_cancelled_old_attempt_or_changed_entry_never_consumes_successor);
	UT_RUN(test_continue_master_boot_epoch_connection_or_freshness_loss_retains_owner);
	UT_RUN(test_continue_receipt_authority_decision_or_image_change_never_publishes);
	UT_RUN(test_continue_capability_full_E_sequence_target_and_reserved_bytes_are_exact);
	UT_RUN(test_continue_content_boundary_native_turnover_blocks_original_actions);
	UT_RUN(test_continue_header_ERROR_keeps_same_armed_receipt_and_can_resume_later);
	UT_RUN(test_continue_repeated_before_IO_ERROR_does_not_repeat_native_commit);
	UT_RUN(test_continue_after_IO_ERROR_never_repeats_IO_or_follower_wakeup);
	UT_RUN(test_continue_after_shared_C_ERROR_releases_only_its_original_hold);
	UT_RUN(test_continue_cancel_after_IO_keeps_installed_resident_not_unpublished_abort);
	UT_RUN(test_continue_held_content_lock_never_enters_entry_or_provider);
	UT_RUN(test_continue_old_complete_and_observer_do_not_gain_armed_permission);
	UT_RUN(test_continue_exact_observer_after_recovery_sees_original_completed_capability);
	UT_RUN(test_continue_already_done_consumed_or_settled_never_rearms_notice);
	UT_RUN(test_continue_later_hold_after_notice_is_never_released);
	UT_RUN(test_continue_wrong_base_cannot_reclassify_native_C_as_a_fresh_input);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
