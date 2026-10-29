/*-------------------------------------------------------------------------
 * test_cluster_native_disk_episode.c
 *   Original disk/S acquisition, native completion and background handoff.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_native_disk_episode.c
 * NOTES
 *   Real PCM, disk, native IO, codec, callbacks and PG cleanup bodies run.
 *   Only storage/process events and exact boundary fault leaves are driven.
 *   FATAL uses the controlled exit stack, not a native process-exit test.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_native_disk_guard_fixture.inc"

static unsigned de_boundary, de_fault;
static void
de_event(unsigned boundary)
{
	if (de_boundary != boundary) return;
	de_boundary = 0;
	if (de_fault == 1) pg_re_throw();
	if (de_fault == 2) dg_fatal();
}
static ResourceXApplyResult
de_prepare(BufferDesc *buf, const ClusterPcmOwnSnapshot *base,
	const ResourceXDecodedAcquireIntent *episode, const ClusterSemanticAdmissionToken *admission,
	const ResourceXDeliveryClaim *original)
{
	ResourceXApplyResult result;
	de_event(1);
	result = cluster_bufmgr_pcm_own_acquire_prepare_disk_exact(buf, base, episode, admission, original);
	de_event(2);
	return result;
}
static ResourceXApplyResult
de_continue(BufferDesc *buf, const ClusterPcmOwnSnapshot *base,
	const ResourceXDecodedAcquireIntent *episode, const ClusterSemanticAdmissionToken *admission,
	const ResourceXDeliveryClaim *original)
{
	volatile ResourceXApplyResult result;
	/* Faults are one-shot boundary events. Leave the actual C and IO bodies
	 * intact; clear only fixture injection before the outer cleanup runs. */
	PG_TRY();
	{
		result = cluster_bufmgr_pcm_own_acquire_continue_disk_exact(buf, base, episode, admission, original);
	}
	PG_CATCH();
	{
		complete_before_io_error = complete_after_io_error = complete_publish_error = false;
		stage_header_hook = NULL;
		PG_RE_THROW();
	}
	PG_END_TRY();
	de_event(3);
	return result;
}
#ifndef DISK_EPISODE_RED
static ResourceXApplyResult
de_handoff(const ResourceXDeliveryClaim *execution, const ResourceXDecodedAcquireIntent *episode,
	const ResourceXAcquireReadReceipt *receipt, const ClusterPcmOwnSnapshot *terminal)
{
	ResourceXApplyResult result = cluster_pcm_lock_resource_x_requester_acquire_read_handoff_exact(
		execution, episode, receipt, terminal);
	de_event(4);
	return result;
}
#define LockBufHdr stage_header_lock
#define GetPrivateRefCount(buffer) request_private_pin(buffer)
#define cluster_bufmgr_pcm_own_acquire_prepare_disk_exact de_prepare
#define cluster_bufmgr_pcm_own_acquire_continue_disk_exact de_continue
#define cluster_pcm_lock_resource_x_requester_acquire_read_handoff_exact de_handoff
#define ereport(level, rest) dg_native_report(level)
#include "test_cluster_native_disk_episode.inc"
#undef ereport
#undef cluster_pcm_lock_resource_x_requester_acquire_read_handoff_exact
#undef cluster_bufmgr_pcm_own_acquire_continue_disk_exact
#undef cluster_bufmgr_pcm_own_acquire_prepare_disk_exact
#undef GetPrivateRefCount
#undef LockBufHdr
#endif

static bool
de_start(EvidenceFixture *f)
{
	de_boundary = de_fault = 0;
	return dg_start(f) && dg_locate_settlement();
}
static ResourceXApplyResult
de_run(void)
{
#ifdef DISK_EPISODE_RED
	/* Actual parent sequence: no missing API or substituted handler. */
	ResourceXApplyResult result = de_prepare(&request_buf, &request_base, &prepared_episode,
		&request_admission, &prepared_claim);
	if (result == RESOURCE_X_APPLY_APPLIED)
		result = de_continue(&request_buf, &request_base, &prepared_episode,
			&request_admission, &prepared_claim);
	if (result == RESOURCE_X_APPLY_APPLIED) {
		ResourceXDeliveryClaim execution = prepared_claim;
		execution.purpose = RESOURCE_X_DELIVERY_ACQUIRE_EXECUTING;
		result = cluster_bufmgr_pcm_own_acquire_done_disk_exact(&request_buf, &request_base,
			&prepared_episode, &request_admission, &execution);
		de_event(4);
	}
	return result;
#else
	return cluster_bufmgr_pcm_own_acquire_fill_disk_exact(&request_buf, &request_base,
		&prepared_episode, &request_admission, &prepared_claim);
#endif
}
static bool
de_error(void)
{
	volatile bool caught = false;
	PG_TRY(); { (void)de_run(); } PG_CATCH(); { caught = true; } PG_END_TRY();
	return caught;
}
static void
de_exit(void)
{
	sigjmp_buf *saved = PG_exception_stack;
	ErrorContextCallback *context = error_context_stack;
	if (sigsetjmp(dg_fatal_jump, 0) == 0) { (void)de_run(); UT_ASSERT(false); }
	PG_exception_stack = saved;
	error_context_stack = context;
}
static void
de_installed(void)
{
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 7);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_SCUR);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & CLUSTER_PCM_OWN_SEMANTIC_BUF_STATE_MASK, BM_TAG_VALID | BM_VALID);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(disk_reads, 1); UT_ASSERT_EQ(complete_commits, 1);
	UT_ASSERT_EQ(complete_broadcasts, 1); UT_ASSERT_EQ(quiet_aborts + ua_aborts, 0);
	UT_ASSERT_EQ(qn_intent->slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	dg_stack_empty();
}
static ResourceXApplyResult
de_ack(void)
{
	ResourceXDecodedFrame frame = done_frame();
	frame.common.outcome = RESOURCE_X_OUTCOME_DUPLICATE;
	frame.common.action_node = frame.body.acquire_completion.decision.master_node;
	frame.common.sender_connection_generation = 71;
	if (!qn_wire(&frame)) return RESOURCE_X_APPLY_INVALID;
	return settle_receive(&frame);
}
static void
de_retired(void)
{
	static const ResourceXIntentSlot empty;
	ClusterPcmResourceXBootstrapRound *round = &request_entry()->resource_x_bootstrap_round;
	/* Settle notification debt, retaining bounded resident provenance until
	 * the original native release/revoke owner ends that separate lifetime. */
	UT_ASSERT_EQ(round->has_acquire_intent, 1);
	UT_ASSERT_EQ(round->delivery_target.buffer_id_plus_one, 1);
	UT_ASSERT_EQ(round->delivery_executor_purpose, 9);
	UT_ASSERT_EQ(round->highest_attempt_floor, 1);
	UT_ASSERT(memcmp(&qn_intent->slot, &empty, sizeof(empty)) == 0);
	UT_ASSERT_EQ(qn_intent->payload[0], 5);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & BM_VALID, BM_VALID);
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_broadcasts, 1);
	dg_stack_empty();
}

UT_TEST(test_episode_success_installs_once_and_hands_off_original_NOTICE)
{
	EvidenceFixture f;
	UT_ASSERT(de_start(&f)); UT_ASSERT_EQ(de_run(), RESOURCE_X_APPLY_APPLIED); de_installed();
}
UT_TEST(test_episode_ERROR_before_execution_closes_original_PREPARED)
{
	EvidenceFixture f;
	UT_ASSERT(de_start(&f)); de_boundary = 1; de_fault = 1;
	UT_ASSERT(de_error()); dg_quiet(0, false);
}
UT_TEST(test_episode_ERROR_after_preparation_quiesces_verified_ARMED)
{
	EvidenceFixture f;
	UT_ASSERT(de_start(&f)); de_boundary = 2; de_fault = 1;
	UT_ASSERT(de_error()); dg_quiet(1, true);
}
UT_TEST(test_episode_ERROR_after_native_C_only_finishes_original_IO)
{
	EvidenceFixture f;
	UT_ASSERT(de_start(&f)); complete_before_io_error = true;
	UT_ASSERT(de_error()); de_installed(); UT_ASSERT_EQ(complete_io_calls, 2);
}
UT_TEST(test_episode_ERROR_after_IO_does_not_commit_or_wake_again)
{
	EvidenceFixture f;
	UT_ASSERT(de_start(&f)); complete_after_io_error = true;
	UT_ASSERT(de_error()); de_installed(); UT_ASSERT_EQ(complete_io_calls, 1);
}
UT_TEST(test_episode_ERROR_after_shared_C_releases_only_original_fence)
{
	EvidenceFixture f;
	UT_ASSERT(de_start(&f)); complete_publish_error = true;
	UT_ASSERT(de_error()); de_installed(); UT_ASSERT_EQ(complete_io_calls, 1);
}
UT_TEST(test_episode_ERROR_after_continuation_keeps_installed_resident_and_notice)
{
	EvidenceFixture f;
	UT_ASSERT(de_start(&f)); de_boundary = 3; de_fault = 1;
	UT_ASSERT(de_error()); de_installed();
}
UT_TEST(test_episode_ERROR_after_notice_does_not_repeat_publication)
{
	EvidenceFixture f;
	UT_ASSERT(de_start(&f)); de_boundary = 4; de_fault = 1;
	UT_ASSERT(de_error()); de_installed();
}
UT_TEST(test_episode_FATAL_before_execution_uses_original_LIFO_owner)
{
	EvidenceFixture f;
	UT_ASSERT(de_start(&f)); de_boundary = 1; de_fault = 2;
	de_exit(); dg_quiet(0, false);
}
UT_TEST(test_episode_FATAL_after_ARMED_keeps_verified_quiet_receipt)
{
	EvidenceFixture f;
	UT_ASSERT(de_start(&f)); de_boundary = 2; de_fault = 2;
	de_exit(); dg_quiet(1, true);
}
UT_TEST(test_episode_FATAL_after_C_hands_debt_to_background_owner)
{
	EvidenceFixture f;
	UT_ASSERT(de_start(&f)); de_boundary = 3; de_fault = 2;
	de_exit(); de_installed();
}
UT_TEST(test_episode_FATAL_after_notice_does_not_abort_installed_C)
{
	EvidenceFixture f;
	UT_ASSERT(de_start(&f)); de_boundary = 4; de_fault = 2;
	de_exit(); de_installed();
}
UT_TEST(test_episode_callback_before_ACK_retires_without_original_backend)
{
	EvidenceFixture f; ResourceXIntentSlot slot;
	PGPROC *proc; int pid; ResourceOwner owner;
	UT_ASSERT(de_start(&f)); UT_ASSERT_EQ(de_run(), RESOURCE_X_APPLY_APPLIED);
	slot = qn_stage();
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
	proc = MyProc; pid = MyProcPid; owner = CurrentResourceOwner;
	MyProc = NULL; MyProcPid = 0; CurrentResourceOwner = NULL;
	UT_ASSERT_EQ(de_ack(), RESOURCE_X_APPLY_APPLIED);
	MyProc = proc; MyProcPid = pid; CurrentResourceOwner = owner;
	de_retired();
}
UT_TEST(test_episode_ACK_before_callback_retains_exact_STAGED_then_retires)
{
	EvidenceFixture f; ResourceXIntentSlot slot;
	UT_ASSERT(de_start(&f)); UT_ASSERT_EQ(de_run(), RESOURCE_X_APPLY_APPLIED); slot = qn_stage();
	UT_ASSERT_EQ(de_ack(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(qn_intent->slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 1);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
	de_retired();
}
UT_TEST(test_episode_failed_callback_after_ACK_is_quiet_without_reoffer)
{
	EvidenceFixture f; ResourceXIntentSlot slot;
	UT_ASSERT(de_start(&f)); UT_ASSERT_EQ(de_run(), RESOURCE_X_APPLY_APPLIED); slot = qn_stage();
	UT_ASSERT_EQ(de_ack(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, false, ++fake_pcm_clock_us));
	de_retired();
}
UT_TEST(test_episode_physical_callback_alone_keeps_logical_debt)
{
	EvidenceFixture f; ResourceXIntentSlot slot;
	UT_ASSERT(de_start(&f)); UT_ASSERT_EQ(de_run(), RESOURCE_X_APPLY_APPLIED); slot = qn_stage();
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 7);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 1);
	UT_ASSERT_EQ(complete_commits, 1); dg_stack_empty();
}
UT_TEST(test_episode_stale_callback_never_retires_newer_physical_owner)
{
	EvidenceFixture f; ResourceXIntentSlot slot, stale;
	UT_ASSERT(de_start(&f)); UT_ASSERT_EQ(de_run(), RESOURCE_X_APPLY_APPLIED); slot = qn_stage();
	UT_ASSERT_EQ(de_ack(), RESOURCE_X_APPLY_APPLIED); stale = slot; stale.send_episode++;
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&stale, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(qn_intent->slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
	de_retired();
}
UT_TEST(test_episode_wrong_mode_and_original_identity_do_not_start_storage)
{
	for (int n = 0; n < 3; n++) {
		EvidenceFixture f;
		UT_ASSERT(de_start(&f));
		if (n == 0) prepared_episode.reservation_token++;
		if (n == 1) prepared_claim.executor_sequence++;
		if (n == 2) prepared_claim.observation.request.target_mode = PCM_STATE_X;
		UT_ASSERT(de_run() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_commits, 0); dg_stack_empty();
	}
}


#ifndef DISK_EPISODE_RED
static ClusterAcquireDiskGuard
de_original_guard(void)
{
	ClusterAcquireDiskGuard guard;
	guard.buf = &request_buf;
	guard.owner = original_io_owner;
	guard.base = request_base;
	guard.episode = prepared_episode;
	guard.original = prepared_claim;
	return guard;
}
static ResourceXApplyResult
de_recover(void)
{
	ClusterAcquireDiskGuard guard = de_original_guard();
	return cluster_bufmgr_acquire_disk_installed_handoff(&guard);
}
static bool
de_completed(EvidenceFixture *f)
{
	return de_start(f) && dg_run() == RESOURCE_X_APPLY_APPLIED
		&& armed_advance() == RESOURCE_X_APPLY_APPLIED;
}
static void
de_refused(void)
{
	ClusterPcmOwnEntry own = request_own;
	ClusterPcmResourceXBootstrapRound round = request_entry()->resource_x_bootstrap_round;
	uint32 state = pg_atomic_read_u32(&request_buf.state);
	unsigned io = complete_io_calls, commits = complete_commits, wakes = complete_broadcasts;
	ResourceXApplyResult result = de_recover();
	UT_ASSERT(result != RESOURCE_X_APPLY_APPLIED && result != RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state), state);
	UT_ASSERT_EQ(complete_io_calls, io); UT_ASSERT_EQ(complete_commits, commits);
	UT_ASSERT_EQ(complete_broadcasts, wakes); UT_ASSERT_EQ(quiet_aborts + ua_aborts, 0);
	dg_stack_empty();
}

UT_TEST(test_cleanup_never_crosses_unpublished_C_even_when_gate_is_live)
{
	EvidenceFixture f;
	UT_ASSERT(de_start(&f)); UT_ASSERT_EQ(dg_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(de_recover(), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(complete_io_calls, 0);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 5);
	dg_stack_empty();
}
UT_TEST(test_cleanup_installed_C_survives_original_admission_or_gate_closure)
{
	for (int n = 0; n < 3; n++) {
		EvidenceFixture f; ResourceXGateSnapshot gate;
		UT_ASSERT(de_completed(&f));
		if (n == 0) request_admission.entered = false;
		if (n == 1) {
			UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
			UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate), RESOURCE_X_APPLY_APPLIED);
		}
		if (n == 2) auth_peer_boot++;
		UT_ASSERT_EQ(de_recover(), RESOURCE_X_APPLY_APPLIED); de_installed();
		/* The cleanup proof did not reopen ordinary execution admission. */
		UT_ASSERT(cluster_bufmgr_pcm_own_acquire_done_disk_exact(&request_buf, &request_base,
			&prepared_episode, &request_admission, &execution_claim) != RESOURCE_X_APPLY_APPLIED);
	}
}
UT_TEST(test_cleanup_same_cancel_floor_can_finish_original_notification)
{
	EvidenceFixture f; ResourceXIntentSlot slot;
	UT_ASSERT(de_completed(&f));
	request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor = 1;
	UT_ASSERT_EQ(de_recover(), RESOURCE_X_APPLY_APPLIED); de_installed();
	slot = qn_stage(); UT_ASSERT_EQ(de_ack(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
	de_retired();
}
UT_TEST(test_cleanup_native_C_before_IO_can_finish_after_gate_closes)
{
	EvidenceFixture f; ResourceXGateSnapshot gate;
	UT_ASSERT(de_start(&f)); UT_ASSERT_EQ(dg_run(), RESOURCE_X_APPLY_APPLIED);
	complete_before_io_error = true;
	UT_ASSERT(armed_catches()); complete_before_io_error = false;
	UT_ASSERT_EQ(complete_commits, 1);
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(de_recover(), RESOURCE_X_APPLY_APPLIED); de_installed();
	UT_ASSERT_EQ(complete_io_calls, 2);
}
UT_TEST(test_cleanup_changed_original_E_process_or_IO_owner_preserves_installed_C)
{
	for (int n = 0; n < 8; n++) {
		EvidenceFixture f;
		UT_ASSERT(de_completed(&f));
		if (n == 0) prepared_episode.backend_generation++;
		if (n == 1) prepared_episode.reservation_token++;
		if (n == 2) prepared_claim.executor_sequence++;
		if (n == 3) prepared_claim.observation.entry_binding_generation++;
		if (n == 4) CurrentResourceOwner = NULL;
		if (n == 5) request_proc.cluster_grd_generation++;
		if (n == 6) MyProcPid++;
		if (n == 7) request_pins = 0;
		de_refused();
		CurrentResourceOwner = original_io_owner;
	}
}
UT_TEST(test_cleanup_later_native_fence_or_I_never_uses_original_handoff)
{
	for (int n = 0; n < 8; n++) {
		EvidenceFixture f;
		UT_ASSERT(de_completed(&f));
		if (n == 0) pg_atomic_write_u64(&request_own.generation, request_base.generation + 2);
		if (n == 1) pg_atomic_write_u64(&request_own.reservation_token, prepared_episode.reservation_token + 1);
		if (n == 2) pg_atomic_write_u64(&request_own.delivery_attempt, 2);
		if (n == 3) pg_atomic_write_u64(&request_own.write_io_generation, 1);
		if (n == 4) disk_shared.data[256] ^= 1;
		if (n == 5) request_buf.tag.blockNum++;
		if (n == 6) request_buf.pcm_state = PCM_STATE_X;
		if (n == 7) request_buf.buffer_type = BUF_TYPE_PI;
		de_refused();
	}
}
UT_TEST(test_cleanup_future_floor_or_changed_retained_domain_keeps_old_debt)
{
	for (int n = 0; n < 5; n++) {
		EvidenceFixture f; ClusterPcmResourceXBootstrapRound *round;
		UT_ASSERT(de_completed(&f)); round = &request_entry()->resource_x_bootstrap_round;
		if (n == 0) round->cancelled_attempt_floor = 2;
		if (n == 1) round->failed_attempt_floor = 1;
		if (n == 2) round->r4_record_generation++;
		if (n == 3) round->current_master_node = 2;
		if (n == 4) request_entry()->binding_generation++;
		de_refused();
	}
}
UT_TEST(test_cleanup_same_C_replay_across_notice_consumed_and_settled)
{
	EvidenceFixture f; ResourceXIntentSlot slot; uint8 payload[RESOURCE_X_COMPLETE_V5_BYTES];
	UT_ASSERT(de_start(&f)); UT_ASSERT_EQ(de_run(), RESOURCE_X_APPLY_APPLIED);
	memcpy(payload, qn_intent->payload, sizeof(payload));
	UT_ASSERT_EQ(de_recover(), RESOURCE_X_APPLY_DUPLICATE);
	slot = qn_stage(); UT_ASSERT_EQ(de_ack(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(de_recover(), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(de_recover(), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(payload, qn_intent->payload, sizeof(payload)) == 0);
	de_retired();
}
UT_TEST(test_cleanup_occupied_notice_returns_local_wait_without_eviction)
{
	EvidenceFixture f; ClusterPcmResourceXReadIntent before;
	UT_ASSERT(de_completed(&f));
	memset(qn_intent, 0x3B, sizeof(*qn_intent)); qn_intent->slot.state = RESOURCE_X_INTENT_SLOT_ARMED;
	before = *qn_intent;
	UT_ASSERT_EQ(de_recover(), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(memcmp(&before, qn_intent, sizeof(before)) == 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 6);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.acquire_read_handoff, 0);
	memset(qn_intent, 0, sizeof(*qn_intent));
	UT_ASSERT_EQ(de_recover(), RESOURCE_X_APPLY_APPLIED); de_installed();
}
UT_TEST(test_cleanup_handoff_flag_requires_completed_native_shape)
{
	for (int n = 0; n < 3; n++) {
		EvidenceFixture f; ClusterPcmResourceXBootstrapRound before;
		UT_ASSERT(de_start(&f));
		if (n == 0) request_entry()->resource_x_bootstrap_round.acquire_read_handoff = 1;
		if (n == 1) request_entry()->resource_x_bootstrap_round.acquire_read_handoff = 2;
		if (n == 2) request_entry()->resource_x_bootstrap_round.acquire_reserved[0] = 1;
		before = request_entry()->resource_x_bootstrap_round;
		UT_ASSERT(de_run() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, &request_entry()->resource_x_bootstrap_round, sizeof(before)) == 0);
		UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_commits, 0); dg_stack_empty();
	}
}
UT_TEST(test_cleanup_background_handoff_keeps_all_master_ACK_identity_guards)
{
	for (int n = 0; n < 7; n++) {
		EvidenceFixture f; ResourceXDecodedFrame frame; ClusterPcmResourceXReadIntent before;
		UT_ASSERT(de_start(&f)); UT_ASSERT_EQ(de_run(), RESOURCE_X_APPLY_APPLIED);
		before = *qn_intent; frame = done_frame();
		frame.common.outcome = RESOURCE_X_OUTCOME_DUPLICATE; frame.common.action_node = 0;
		frame.common.sender_connection_generation = 71;
		if (n == 0) frame.body.acquire_completion.decision.episode.backend_generation++;
		if (n == 1) frame.body.acquire_completion.decision.resource_binding++;
		if (n == 2) frame.body.acquire_completion.page_crc32c++;
		if (n == 3) frame.common.assertion_sequence++;
		if (n == 4) frame.common.master_session_incarnation++;
		if (n == 5) frame.body.acquire_completion.decision.requester_ingress_generation++;
		if (n == 6) frame.body.acquire_completion.write_io_generation += 2;
		UT_ASSERT(qn_wire(&frame));
		UT_ASSERT(settle_receive(&frame) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, qn_intent, sizeof(before)) == 0);
		de_installed();
	}
}
UT_TEST(test_cleanup_global_LMS_finds_handoff_without_original_backend)
{
	EvidenceFixture f; ResourceXIntentSlot slot;
	PGPROC *proc; int pid; ResourceOwner owner;
	UT_ASSERT(de_start(&f)); UT_ASSERT_EQ(de_run(), RESOURCE_X_APPLY_APPLIED);
	request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor = 1;
	proc = MyProc; pid = MyProcPid; owner = CurrentResourceOwner;
	MyProc = NULL; MyProcPid = 0; CurrentResourceOwner = NULL;
	UT_ASSERT(qn_global_probe(&slot)); UT_ASSERT_EQ(slot.kind, RESOURCE_X_WIRE_INSTALL_SETTLEMENT);
	MyProc = proc; MyProcPid = pid; CurrentResourceOwner = owner;
	de_installed();
}
UT_TEST(test_cleanup_unprovable_installed_C_panics_before_generic_IO_cleanup)
{
	EvidenceFixture f; ClusterAcquireDiskGuard guard;
	sigjmp_buf *saved = PG_exception_stack;
	ErrorContextCallback *context = error_context_stack;
	volatile bool caught = false;
	UT_ASSERT(de_completed(&f)); guard = de_original_guard();
	pg_atomic_write_u64(&request_own.delivery_attempt, 2);
	dg_panic_expected = true;
	if (sigsetjmp(dg_panic_jump, 0) == 0)
		cluster_bufmgr_acquire_disk_episode_cleanup(0, PointerGetDatum(&guard));
	else caught = true;
	PG_exception_stack = saved; error_context_stack = context;
	UT_ASSERT(caught); UT_ASSERT_EQ(dg_panics, 1);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 2);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 6);
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_broadcasts, 1);
	UT_ASSERT_EQ(quiet_aborts + ua_aborts, 0); dg_stack_empty();
}
#endif

int
main(void)
{
#ifdef DISK_EPISODE_RED
	UT_PLAN(18);
#else
	UT_PLAN(31);
#endif
	UT_RUN(test_episode_success_installs_once_and_hands_off_original_NOTICE);
	UT_RUN(test_episode_ERROR_before_execution_closes_original_PREPARED);
	UT_RUN(test_episode_ERROR_after_preparation_quiesces_verified_ARMED);
	UT_RUN(test_episode_ERROR_after_native_C_only_finishes_original_IO);
	UT_RUN(test_episode_ERROR_after_IO_does_not_commit_or_wake_again);
	UT_RUN(test_episode_ERROR_after_shared_C_releases_only_original_fence);
	UT_RUN(test_episode_ERROR_after_continuation_keeps_installed_resident_and_notice);
	UT_RUN(test_episode_ERROR_after_notice_does_not_repeat_publication);
	UT_RUN(test_episode_FATAL_before_execution_uses_original_LIFO_owner);
	UT_RUN(test_episode_FATAL_after_ARMED_keeps_verified_quiet_receipt);
	UT_RUN(test_episode_FATAL_after_C_hands_debt_to_background_owner);
	UT_RUN(test_episode_FATAL_after_notice_does_not_abort_installed_C);
	UT_RUN(test_episode_callback_before_ACK_retires_without_original_backend);
	UT_RUN(test_episode_ACK_before_callback_retains_exact_STAGED_then_retires);
	UT_RUN(test_episode_failed_callback_after_ACK_is_quiet_without_reoffer);
	UT_RUN(test_episode_physical_callback_alone_keeps_logical_debt);
	UT_RUN(test_episode_stale_callback_never_retires_newer_physical_owner);
	UT_RUN(test_episode_wrong_mode_and_original_identity_do_not_start_storage);
#ifndef DISK_EPISODE_RED
	UT_RUN(test_cleanup_never_crosses_unpublished_C_even_when_gate_is_live);
	UT_RUN(test_cleanup_installed_C_survives_original_admission_or_gate_closure);
	UT_RUN(test_cleanup_same_cancel_floor_can_finish_original_notification);
	UT_RUN(test_cleanup_native_C_before_IO_can_finish_after_gate_closes);
	UT_RUN(test_cleanup_changed_original_E_process_or_IO_owner_preserves_installed_C);
	UT_RUN(test_cleanup_later_native_fence_or_I_never_uses_original_handoff);
	UT_RUN(test_cleanup_future_floor_or_changed_retained_domain_keeps_old_debt);
	UT_RUN(test_cleanup_same_C_replay_across_notice_consumed_and_settled);
	UT_RUN(test_cleanup_occupied_notice_returns_local_wait_without_eviction);
	UT_RUN(test_cleanup_handoff_flag_requires_completed_native_shape);
	UT_RUN(test_cleanup_background_handoff_keeps_all_master_ACK_identity_guards);
	UT_RUN(test_cleanup_global_LMS_finds_handoff_without_original_backend);
	UT_RUN(test_cleanup_unprovable_installed_C_panics_before_generic_IO_cleanup);
#endif
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
