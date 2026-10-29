/*-------------------------------------------------------------------------
 * test_cluster_native_disk_quiet.c
 *   Exercise original uninstalled disk-read cancellation and quiet handoff.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_native_disk_quiet.c
 * NOTES
 *   Product native, PCM, IO, codec and ownership bodies are retained.
 *   Local quiet_ helpers control process/event leaves and interruption cuts.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_native_read_continue_fixture.inc"

static unsigned quiet_aborts, quiet_abort_calls, quiet_broadcasts, quiet_retires;
static bool quiet_before_abort_error, quiet_after_abort_error, quiet_publish_error, quiet_retire_refused;
static void (*quiet_on_content)(void);

static void
quiet_broadcast(ConditionVariable *cv)
{
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_N);
	UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_CURRENT);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR), BM_IO_ERROR);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 10);
	quiet_broadcasts++;
	ConditionVariableBroadcast(cv);
}
#define TerminateBufferIO quiet_actual_terminate
#define LockBufHdr stage_header_lock
#define ConditionVariableBroadcast quiet_broadcast
#include "test_cluster_native_disk_terminate.inc"
#undef ConditionVariableBroadcast
#undef LockBufHdr
#undef TerminateBufferIO

static BufferDesc *
quiet_descriptor(int id)
{
	UT_ASSERT_EQ(id, request_buf.buf_id);
	return &request_buf;
}
static char *
quiet_wrong_output(void)
{
	UT_ASSERT(false);
	return NULL;
}
static void
quiet_wrong_free(void *pointer)
{
	(void)pointer;
	UT_ASSERT(false);
}
#undef relpathperm
static void quiet_actual_abort(Buffer buffer);
#define GetBufferDescriptor(id) quiet_descriptor(id)
#define relpathperm(locator, forknum) quiet_wrong_output()
#define pfree(pointer) quiet_wrong_free(pointer)
#define LockBufHdr stage_header_lock
#define TerminateBufferIO quiet_actual_terminate
#define AbortBufferIO quiet_actual_abort
#include "test_cluster_native_disk_abort.inc"
#undef AbortBufferIO
#undef TerminateBufferIO
#undef LockBufHdr
#undef relpathperm
#undef pfree
#undef GetBufferDescriptor

#ifndef DISK_QUIET_RED
static void
quiet_abort(Buffer buffer)
{
	quiet_abort_calls++;
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, buffer));
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	if (quiet_before_abort_error) pg_re_throw();
	quiet_actual_abort(buffer);
	quiet_aborts++;
	if (quiet_after_abort_error) pg_re_throw();
}
static ClusterPcmOwnResult
quiet_retire(int id, uint64 generation, uint64 token, uint32 flags)
{
	ClusterPcmOwnResult result;

	if (quiet_retire_refused) return CLUSTER_PCM_OWN_STALE;
	result = cluster_pcm_own_reservation_abort_exact(id, generation, token, flags);
	if (result == CLUSTER_PCM_OWN_OK) quiet_retires++;
	return result;
}
static ResourceXApplyResult
quiet_publish(const ResourceXDeliveryClaim *execution, const ResourceXDecodedAcquireIntent *episode,
	const ResourceXAcquireReadReceipt *receipt, const ClusterPcmOwnSnapshot *terminal)
{
	ResourceXApplyResult result = cluster_pcm_lock_resource_x_requester_acquire_read_quiet_exact(
		execution, episode, receipt, terminal);
	if (result == RESOURCE_X_APPLY_APPLIED && quiet_publish_error) pg_re_throw();
	return result;
}
static bool
quiet_content(LWLock *lock, LWLockMode mode)
{
	bool result = LWLockAcquire(lock, mode);
	if (quiet_on_content != NULL) quiet_on_content();
	return result;
}
#define LockBufHdr stage_header_lock
#define LWLockAcquire quiet_content
#define GetPrivateRefCount(buffer) request_private_pin(buffer)
#define AbortBufferIO quiet_abort
#define cluster_pcm_own_reservation_abort_exact quiet_retire
#define cluster_pcm_lock_resource_x_requester_acquire_read_quiet_exact quiet_publish
#include "test_cluster_native_disk_quiet.inc"
#undef cluster_pcm_lock_resource_x_requester_acquire_read_quiet_exact
#undef cluster_pcm_own_reservation_abort_exact
#undef AbortBufferIO
#undef GetPrivateRefCount
#undef LWLockAcquire
#undef LockBufHdr
#endif

static ResourceXApplyResult
quiet_run(void)
{
#ifdef DISK_QUIET_RED
	return armed_advance();
#else
	return cluster_bufmgr_pcm_own_acquire_quiet_disk_exact(&request_buf, &request_base,
		&prepared_episode, &armed_capability);
#endif
}
static bool
quiet_start(EvidenceFixture *f, unsigned cut)
{
	quiet_aborts = quiet_abort_calls = quiet_broadcasts = quiet_retires = 0;
	quiet_before_abort_error = quiet_after_abort_error = quiet_publish_error = quiet_retire_refused = false;
	quiet_on_content = NULL;
	return armed_start(f, cut);
}
static bool
quiet_catches(void)
{
	volatile bool caught = false;
	PG_TRY(); { (void)quiet_run(); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	return caught;
}
static void
quiet_done(void)
{
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 11);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.reservation_token), prepared_episode.reservation_token);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_N);
	UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_CURRENT);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR), BM_IO_ERROR);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(quiet_aborts, 1); UT_ASSERT_EQ(quiet_retires, 1); UT_ASSERT_EQ(quiet_broadcasts, 1);
	UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(complete_io_calls, 0); UT_ASSERT_EQ(complete_broadcasts, 0);
	UT_ASSERT_EQ(disk_reads, 1); UT_ASSERT_EQ(request_shared.rings[1].count, 1); complete_idle();
}
static void
quiet_refused(void)
{
	ClusterPcmOwnEntry own = request_own;
	ClusterPcmResourceXBootstrapRound round = request_entry()->resource_x_bootstrap_round;
	uint32 state = pg_atomic_read_u32(&request_buf.state);
	unsigned calls = quiet_abort_calls;
	ResourceXApplyResult result = quiet_run();
	UT_ASSERT(result != RESOURCE_X_APPLY_APPLIED && result != RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state), state);
	UT_ASSERT_EQ(quiet_abort_calls, calls); complete_idle();
}

UT_TEST(test_quiet_original_uninstalled_ARMED_retires_GP_then_aborts_own_IO)
{
	EvidenceFixture f;
	UT_ASSERT(quiet_start(&f, 0)); UT_ASSERT_EQ(quiet_run(), RESOURCE_X_APPLY_APPLIED); quiet_done();
}
UT_TEST(test_quiet_retains_complete_original_E_A_I_and_pending_responsibility)
{
	EvidenceFixture f; ResourceXAcquireReadReceipt receipt; ResourceXDecodedAcquireIntent episode;
	uint64 floor;
	UT_ASSERT(quiet_start(&f, 0)); receipt = request_entry()->resource_x_bootstrap_round.acquire_read_receipt;
	episode = prepared_episode; floor = request_entry()->resource_x_bootstrap_round.highest_attempt_floor;
	UT_ASSERT_EQ(quiet_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&receipt, &request_entry()->resource_x_bootstrap_round.acquire_read_receipt, sizeof(receipt)) == 0);
	UT_ASSERT(memcmp(&episode, &request_entry()->resource_x_bootstrap_round.acquire_intent, sizeof(episode)) == 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.highest_attempt_floor, floor);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 1); quiet_done();
}
UT_TEST(test_quiet_cancelled_floor_does_not_erase_original_cleanup_owner)
{
	EvidenceFixture f;
	UT_ASSERT(quiet_start(&f, 0)); disk_cancel();
	UT_ASSERT_EQ(quiet_run(), RESOURCE_X_APPLY_APPLIED); quiet_done();
}
UT_TEST(test_quiet_closed_admission_and_gate_do_not_regrant_or_block_cleanup)
{
	EvidenceFixture f; ResourceXGateSnapshot gate;
	UT_ASSERT(quiet_start(&f, 0)); request_admission.entered = false;
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(quiet_run(), RESOURCE_X_APPLY_APPLIED); quiet_done();
}
UT_TEST(test_quiet_native_C_before_IO_is_never_reclassified_unpublished)
{
	EvidenceFixture f;
	UT_ASSERT(quiet_start(&f, 1)); quiet_refused();
	UT_ASSERT_EQ(armed_advance(), RESOURCE_X_APPLY_APPLIED); armed_finished(1);
}
UT_TEST(test_quiet_native_C_after_IO_is_never_aborted)
{
	EvidenceFixture f;
	UT_ASSERT(quiet_start(&f, 2)); quiet_refused();
	UT_ASSERT_EQ(armed_advance(), RESOURCE_X_APPLY_APPLIED); armed_finished(2);
}
UT_TEST(test_quiet_complete_notice_consumed_settled_are_not_unpublished)
{
	for (unsigned phase = 6; phase <= 9; phase++) {
		EvidenceFixture f; ResourceXDecodedFrame ack;
		if (phase == 6) { UT_ASSERT(quiet_start(&f, 0)); UT_ASSERT_EQ(armed_advance(), RESOURCE_X_APPLY_APPLIED); }
		else {
			UT_ASSERT(settle_start(&f, &ack)); armed_capability = prepared_claim;
			if (phase >= 8) UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED);
			if (phase == 9) UT_ASSERT_EQ(settle_run(), RESOURCE_X_APPLY_APPLIED);
		}
		quiet_refused();
	}
}
UT_TEST(test_quiet_duplicate_never_retires_reads_aborts_or_wakes_twice)
{
	EvidenceFixture f; ClusterPcmOwnEntry own; ClusterPcmResourceXBootstrapRound round;
	UT_ASSERT(quiet_start(&f, 0)); UT_ASSERT_EQ(quiet_run(), RESOURCE_X_APPLY_APPLIED);
	own = request_own; round = request_entry()->resource_x_bootstrap_round;
	for (int n = 0; n < 8; n++) UT_ASSERT_EQ(quiet_run(), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0); quiet_done();
}
UT_TEST(test_quiet_wrong_bytes_LSN_or_SCN_never_seals)
{
	for (int n = 0; n < 3; n++) {
		EvidenceFixture f;
		UT_ASSERT(quiet_start(&f, 0));
		if (n == 0) disk_shared.data[256] ^= 1;
		if (n == 1) PageSetLSNPreserveOrigin((Page)disk_shared.data, disk_lsn + 1);
		if (n == 2) ((PageHeader)disk_shared.data)->pd_block_scn++;
		quiet_refused();
	}
}
UT_TEST(test_quiet_wrong_native_generation_token_tag_mode_or_IO_error_never_seals)
{
	for (int n = 0; n < 6; n++) {
		EvidenceFixture f;
		UT_ASSERT(quiet_start(&f, 0));
		if (n == 0) pg_atomic_write_u64(&request_own.generation, 60);
		if (n == 1) pg_atomic_write_u64(&request_own.reservation_token, request_token + 1);
		if (n == 2) request_buf.tag.blockNum++;
		if (n == 3) request_buf.pcm_state = PCM_STATE_S;
		if (n == 4) request_buf.buffer_type = BUF_TYPE_PI;
		if (n == 5) pg_atomic_fetch_or_u32(&request_buf.state, BM_IO_ERROR);
		quiet_refused();
	}
}
UT_TEST(test_quiet_later_delivery_hold_or_write_IO_generation_never_seals)
{
	for (int n = 0; n < 3; n++) {
		EvidenceFixture f;
		UT_ASSERT(quiet_start(&f, 0));
		if (n == 0) pg_atomic_write_u64(&request_own.delivery_attempt, 2);
		if (n == 1) pg_atomic_write_u64(&request_own.write_io_generation, 1);
		if (n == 2) pg_atomic_write_u64(&request_own.write_io_generation, 2);
		quiet_refused();
	}
}
UT_TEST(test_quiet_foreign_process_pin_ResourceOwner_or_missing_IO_never_seals)
{
	for (int n = 0; n < 7; n++) {
		EvidenceFixture f;
		UT_ASSERT(quiet_start(&f, 0));
		if (n == 0) MyProc->pid++;
		if (n == 1) MyProc->cluster_grd_generation++;
		if (n == 2) request_pins = 0;
		if (n == 3) CurrentResourceOwner = NULL;
		if (n == 4) ResourceOwnerForgetBufferIO(original_io_owner, 1);
		if (n == 5) MyBackendId++;
		if (n == 6) prepared_episode.requester_incarnation++;
		quiet_refused();
	}
}
UT_TEST(test_quiet_full_E_target_sequence_and_reserved_bytes_are_exact)
{
	for (int n = 0; n < 7; n++) {
		EvidenceFixture f;
		UT_ASSERT(quiet_start(&f, 0));
		if (n == 0) armed_capability.executor_sequence++;
		if (n == 1) armed_capability.observation.request.assertion_sequence++;
		if (n == 2) armed_capability.target.generation++;
		if (n == 3) armed_capability.target.reservation_token++;
		if (n == 4) armed_capability.reserved[0] = 1;
		if (n == 5) prepared_episode.backend_generation++;
		if (n == 6) armed_capability.purpose = 5;
		quiet_refused();
	}
}
UT_TEST(test_quiet_changed_binding_A_or_receipt_is_never_consumed)
{
	for (int n = 0; n < 4; n++) {
		EvidenceFixture f;
		UT_ASSERT(quiet_start(&f, 0));
		if (n == 0) request_entry()->binding_generation++;
		if (n == 1) request_entry()->resource_x_bootstrap_round.acquire_read_receipt.authority_generation++;
		if (n == 2) request_entry()->resource_x_bootstrap_round.acquire_read_receipt.decision_crc32c++;
		if (n == 3) request_entry()->resource_x_bootstrap_round.delivery_executor_sequence++;
		quiet_refused();
	}
}
UT_TEST(test_quiet_held_content_is_rejected_before_entry_or_provider)
{
	EvidenceFixture f; unsigned checks;
	UT_ASSERT(quiet_start(&f, 0)); checks = request_rechecks;
	LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE);
	UT_ASSERT_EQ(quiet_run(), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(request_rechecks, checks);
	LWLockRelease(BufferDescriptorGetContentLock(&request_buf)); complete_idle();
}
UT_TEST(test_quiet_ERROR_after_seal_preserves_original_debt_and_can_continue)
{
	EvidenceFixture f;
	UT_ASSERT(quiet_start(&f, 0)); quiet_before_abort_error = true;
	UT_ASSERT(quiet_catches()); quiet_before_abort_error = false;
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 10);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(quiet_run(), RESOURCE_X_APPLY_APPLIED); quiet_done();
}
UT_TEST(test_quiet_ERROR_after_actual_abort_never_repeats_IO_or_wakeup)
{
	EvidenceFixture f;
	UT_ASSERT(quiet_start(&f, 0)); quiet_after_abort_error = true;
	UT_ASSERT(quiet_catches()); quiet_after_abort_error = false;
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 10);
	UT_ASSERT_EQ(quiet_run(), RESOURCE_X_APPLY_APPLIED); quiet_done();
}
UT_TEST(test_quiet_ERROR_after_shared_publication_replays_without_retirement)
{
	EvidenceFixture f;
	UT_ASSERT(quiet_start(&f, 0)); quiet_publish_error = true;
	UT_ASSERT(quiet_catches()); quiet_publish_error = false;
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 11);
	UT_ASSERT_EQ(quiet_run(), RESOURCE_X_APPLY_DUPLICATE); quiet_done();
}
UT_TEST(test_quiet_failed_GP_retirement_keeps_seal_and_never_aborts_IO)
{
	EvidenceFixture f;
	UT_ASSERT(quiet_start(&f, 0)); quiet_retire_refused = true;
	UT_ASSERT(quiet_run() != RESOURCE_X_APPLY_APPLIED); quiet_retire_refused = false;
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 10);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(quiet_abort_calls, 0);
	UT_ASSERT_EQ(quiet_run(), RESOURCE_X_APPLY_APPLIED); quiet_done();
}
UT_TEST(test_quiet_sealed_work_never_reopens_old_install_or_rollforward)
{
	EvidenceFixture f;
	UT_ASSERT(quiet_start(&f, 0)); quiet_before_abort_error = true;
	UT_ASSERT(quiet_catches()); quiet_before_abort_error = false;
	UT_ASSERT(armed_advance() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(complete_run() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT_EQ(quiet_run(), RESOURCE_X_APPLY_APPLIED); quiet_done();
}
UT_TEST(test_quiet_later_native_owner_after_seal_is_not_aborted)
{
	EvidenceFixture f;
	UT_ASSERT(quiet_start(&f, 0)); quiet_before_abort_error = true;
	UT_ASSERT(quiet_catches()); quiet_before_abort_error = false;
	pg_atomic_write_u64(&request_own.reservation_token, request_token + 1);
	quiet_refused();
}
UT_TEST(test_quiet_tombstone_cannot_clean_later_delivery_hold_or_clear_IO_ERROR)
{
	EvidenceFixture f;
	UT_ASSERT(quiet_start(&f, 0)); UT_ASSERT_EQ(quiet_run(), RESOURCE_X_APPLY_APPLIED);
	pg_atomic_write_u64(&request_own.delivery_attempt, 2); quiet_refused();
	UT_ASSERT(pg_atomic_read_u32(&request_buf.state) & BM_IO_ERROR);
}
UT_TEST(test_quiet_prepared_and_executing_original_handles_have_same_owner)
{
	EvidenceFixture f;
	UT_ASSERT(quiet_start(&f, 0)); armed_capability = execution_claim;
	UT_ASSERT_EQ(quiet_run(), RESOURCE_X_APPLY_APPLIED); quiet_done();
}
UT_TEST(test_quiet_new_base_or_wrong_input_kind_never_borrows_a_cancel_owner)
{
	for (int n = 0; n < 4; n++) {
		EvidenceFixture f;
		UT_ASSERT(quiet_start(&f, 0));
		if (n == 0) request_base.flags = PCM_OWN_FLAG_GRANT_PENDING;
		if (n == 1) request_base.generation++;
		if (n == 2) request_base.reservation_token++;
		if (n == 3) prepared_episode.path = RESOURCE_X_ACQUIRE_RESIDENT;
		quiet_refused();
	}
}

UT_TEST(test_quiet_future_cancel_or_failed_authority_floor_is_not_own_cancel)
{
	for (int n = 0; n < 2; n++) {
		EvidenceFixture f; ClusterPcmResourceXBootstrapRound *round;
		UT_ASSERT(quiet_start(&f, 0)); round = &request_entry()->resource_x_bootstrap_round;
		if (n == 0) round->cancelled_attempt_floor = round->request.assertion_sequence + 1;
		else round->failed_attempt_floor = round->request.assertion_sequence;
		quiet_refused();
	}
}
UT_TEST(test_quiet_peer_loss_prevents_normal_C_but_not_local_quiescence)
{
	for (int n = 0; n < 5; n++) {
		EvidenceFixture f;
		UT_ASSERT(quiet_start(&f, 0));
		if (n == 0) auth_peer_boot++;
		if (n == 1) auth_epoch++;
		if (n == 2) fake_rx_capability_generation++;
		if (n == 3) auth_fresh = false;
		if (n == 4) auth_connection_valid = false;
		UT_ASSERT(armed_advance() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(quiet_run(), RESOURCE_X_APPLY_APPLIED); quiet_done();
	}
}
UT_TEST(test_quiet_native_turnover_at_content_boundary_does_not_seal)
{
	EvidenceFixture f;
	UT_ASSERT(quiet_start(&f, 0)); quiet_on_content = disk_turnover;
	UT_ASSERT(quiet_run() != RESOURCE_X_APPLY_APPLIED); quiet_on_content = NULL;
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 5);
	UT_ASSERT_EQ(quiet_abort_calls, 0); UT_ASSERT_EQ(quiet_retires, 0); complete_idle();
}
UT_TEST(test_quiet_unarmed_execution_does_not_invent_a_verified_receipt)
{
	EvidenceFixture f;
	UT_ASSERT(resume_start(&f)); armed_capability = prepared_claim;
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 4);
	quiet_refused();
}

int
main(void)
{
	UT_PLAN(28);
	UT_RUN(test_quiet_original_uninstalled_ARMED_retires_GP_then_aborts_own_IO);
	UT_RUN(test_quiet_retains_complete_original_E_A_I_and_pending_responsibility);
	UT_RUN(test_quiet_cancelled_floor_does_not_erase_original_cleanup_owner);
	UT_RUN(test_quiet_closed_admission_and_gate_do_not_regrant_or_block_cleanup);
	UT_RUN(test_quiet_native_C_before_IO_is_never_reclassified_unpublished);
	UT_RUN(test_quiet_native_C_after_IO_is_never_aborted);
	UT_RUN(test_quiet_complete_notice_consumed_settled_are_not_unpublished);
	UT_RUN(test_quiet_duplicate_never_retires_reads_aborts_or_wakes_twice);
	UT_RUN(test_quiet_wrong_bytes_LSN_or_SCN_never_seals);
	UT_RUN(test_quiet_wrong_native_generation_token_tag_mode_or_IO_error_never_seals);
	UT_RUN(test_quiet_later_delivery_hold_or_write_IO_generation_never_seals);
	UT_RUN(test_quiet_foreign_process_pin_ResourceOwner_or_missing_IO_never_seals);
	UT_RUN(test_quiet_full_E_target_sequence_and_reserved_bytes_are_exact);
	UT_RUN(test_quiet_changed_binding_A_or_receipt_is_never_consumed);
	UT_RUN(test_quiet_held_content_is_rejected_before_entry_or_provider);
	UT_RUN(test_quiet_ERROR_after_seal_preserves_original_debt_and_can_continue);
	UT_RUN(test_quiet_ERROR_after_actual_abort_never_repeats_IO_or_wakeup);
	UT_RUN(test_quiet_ERROR_after_shared_publication_replays_without_retirement);
	UT_RUN(test_quiet_failed_GP_retirement_keeps_seal_and_never_aborts_IO);
	UT_RUN(test_quiet_sealed_work_never_reopens_old_install_or_rollforward);
	UT_RUN(test_quiet_later_native_owner_after_seal_is_not_aborted);
	UT_RUN(test_quiet_tombstone_cannot_clean_later_delivery_hold_or_clear_IO_ERROR);
	UT_RUN(test_quiet_prepared_and_executing_original_handles_have_same_owner);
	UT_RUN(test_quiet_new_base_or_wrong_input_kind_never_borrows_a_cancel_owner);
	UT_RUN(test_quiet_future_cancel_or_failed_authority_floor_is_not_own_cancel);
	UT_RUN(test_quiet_peer_loss_prevents_normal_C_but_not_local_quiescence);
	UT_RUN(test_quiet_native_turnover_at_content_boundary_does_not_seal);
	UT_RUN(test_quiet_unarmed_execution_does_not_invent_a_verified_receipt);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
