/*-------------------------------------------------------------------------
 * test_cluster_native_wait_cancel.c
 *   Cancel an original first-fill while its decision is still in flight.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_native_wait_cancel.c
 * NOTES
 *   Retain real native, BufferIO, PCM, codec and LMS bodies. Process,
 *   authenticated peer and interruption leaves are controlled inputs.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_native_quiet_notice_fixture.inc"

#define WAIT_SEALING UINT8_C(16)
#define WAIT_QUIET UINT8_C(17)
static unsigned wait_aborts, wait_calls, wait_wakes;
static bool wait_seal_error, wait_before_abort_error, wait_after_abort_error, wait_publish_error;

static void
wait_broadcast(ConditionVariable *cv)
{
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, WAIT_SEALING);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	wait_wakes++;
	ConditionVariableBroadcast(cv);
}
#define TerminateBufferIO wait_actual_terminate
#define ConditionVariableBroadcast wait_broadcast
#define LockBufHdr stage_header_lock
#include "test_cluster_native_disk_terminate.inc"
#undef LockBufHdr
#undef ConditionVariableBroadcast
#undef TerminateBufferIO
#define GetBufferDescriptor(id) quiet_descriptor(id)
#define relpathperm(locator, forknum) quiet_wrong_output()
#define pfree(pointer) quiet_wrong_free(pointer)
#define LockBufHdr stage_header_lock
#define TerminateBufferIO wait_actual_terminate
#define AbortBufferIO wait_actual_abort
#include "test_cluster_native_disk_abort.inc"
#undef AbortBufferIO
#undef TerminateBufferIO
#undef LockBufHdr
#undef pfree
#undef relpathperm
#undef GetBufferDescriptor

static void
wait_abort(Buffer buffer)
{
	wait_calls++;
	if (wait_before_abort_error) pg_re_throw();
	wait_actual_abort(buffer);
	wait_aborts++;
	if (wait_after_abort_error) pg_re_throw();
}

#ifndef WAIT_CANCEL_RED
static ResourceXApplyResult
wait_seal(const ResourceXInstallClaimJoinObservation *observation,
	const ResourceXDecodedAcquireIntent *episode, uint64 write_io,
	const ClusterPcmOwnSnapshot *native, ResourceXDeliveryClaim *out)
{
	ResourceXApplyResult result = cluster_pcm_lock_resource_x_requester_acquire_wait_seal_exact(
		observation, episode, write_io, native, out);
	if (wait_seal_error && result == RESOURCE_X_APPLY_APPLIED) pg_re_throw();
	return result;
}
static ResourceXApplyResult
wait_publish(const ResourceXDeliveryClaim *sealed, const ResourceXDecodedAcquireIntent *episode,
	const ResourceXAcquireReadReceipt *receipt, const ClusterPcmOwnSnapshot *native)
{
	ResourceXApplyResult result = cluster_pcm_lock_resource_x_requester_acquire_wait_quiet_exact(
		sealed, episode, receipt, native);
	if (wait_publish_error && result == RESOURCE_X_APPLY_APPLIED) pg_re_throw();
	return result;
}
#endif

#define LockBufHdr stage_header_lock
#define GetPrivateRefCount(buffer) request_private_pin(buffer)
#define AbortBufferIO wait_abort
#include "test_cluster_native_unarmed_quiet.inc"
#ifndef WAIT_CANCEL_RED
#define cluster_pcm_lock_resource_x_requester_acquire_wait_seal_exact wait_seal
#define cluster_pcm_lock_resource_x_requester_acquire_wait_quiet_exact wait_publish
#include "test_cluster_native_wait_cancel.inc"
#undef cluster_pcm_lock_resource_x_requester_acquire_wait_quiet_exact
#undef cluster_pcm_lock_resource_x_requester_acquire_wait_seal_exact
#endif
#undef AbortBufferIO
#undef GetPrivateRefCount
#undef LockBufHdr

static bool
wait_start(EvidenceFixture *f, int profile)
{
	if (!input_begin(f, profile, 0)) return false;
	prepared_episode = f->request.body.acquire_intent;
	input_grant = f->decision;
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "waiting-first-fill");
	original_io_owner = CurrentResourceOwner;
	ResourceOwnerEnlargeBufferIOs(CurrentResourceOwner);
	ResourceOwnerRememberBufferIO(CurrentResourceOwner, 1);
	BufferBlocks = disk_shared.data;
	memset(&disk_shared, 0xA7, sizeof(disk_shared));
	wait_aborts = wait_calls = wait_wakes = quiet_retires = disk_reads = complete_commits = 0;
	wait_seal_error = wait_before_abort_error = wait_after_abort_error = wait_publish_error = false;
	quiet_retire_refused = false;
	quiet_on_content = NULL;
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	return true;
}
static ResourceXApplyResult
wait_run(void)
{
#ifdef WAIT_CANCEL_RED
	ResourceXDeliveryClaim absent = {0};
	absent.observation = input_observation;
	absent.purpose = RESOURCE_X_DELIVERY_ACQUIRE_EXECUTING;
	return cluster_bufmgr_pcm_own_acquire_quiet_unarmed_disk_exact(&request_buf, &request_base,
		&prepared_episode, &absent);
#else
	return cluster_bufmgr_pcm_own_acquire_cancel_wait_exact(&request_buf, &request_base,
		&prepared_episode, &input_observation);
#endif
}
static bool
wait_catches(void)
{
	volatile bool caught = false;
	PG_TRY(); { (void)wait_run(); } PG_CATCH(); { caught = true; } PG_END_TRY();
	return caught;
}
static void
wait_done(void)
{
	const ClusterPcmResourceXBootstrapRound *r = &request_entry()->resource_x_bootstrap_round;
	UT_ASSERT_EQ(r->delivery_executor_purpose, WAIT_QUIET);
	UT_ASSERT_EQ(r->has_acquire_intent, 1);
	UT_ASSERT_EQ(r->acquire_read_receipt.authority_generation, 0);
	UT_ASSERT_EQ(r->acquire_read_receipt.decision_crc32c, 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), prepared_episode.base_generation);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR), BM_IO_ERROR);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(wait_aborts, 1); UT_ASSERT_EQ(wait_wakes, 1); UT_ASSERT_EQ(quiet_retires, 1);
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT_EQ(request_shared.rings[1].count, 1);
	for (int n = 0; n < BLCKSZ; n++) UT_ASSERT_EQ((unsigned char)disk_shared.data[n], 0xA7);
	complete_idle();
}
static void
wait_refused(void)
{
	ClusterPcmOwnEntry own = request_own;
	ClusterPcmResourceXBootstrapRound round = request_entry()->resource_x_bootstrap_round;
	uint32 state = pg_atomic_read_u32(&request_buf.state);
	ResourceXApplyResult result = wait_run();
	UT_ASSERT(result != RESOURCE_X_APPLY_APPLIED && result != RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state), state);
	UT_ASSERT_EQ(wait_calls, 0);
	complete_idle();
}
static bool
wait_material(EvidenceFixture *f, int profile)
{
	ResourceXRequesterJoinSnapshot out;
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES]; uint16 len; ResourceXWireReject reject;
	unsigned char *memory = (void *)fake_pcm_header.data;
	Size offset = offsetof(ClusterPcmResourceXMasterState, requester_join.grant_payload), found = 0;
	if (!evidence_accept(profile == 0 ? &f->decision : &f->image, &out)) return false;
	UT_ASSERT(out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE);
	/* Independently encode the retained master anchor. The legacy READY
	 * accessor deliberately does not grant access to the new full-E join. */
	input_grant = f->decision;
	input_grant.common.sender_connection_generation = PGRAC_RESOURCE_X_RETAINED_SENDER_GENERATION;
	input_grant.common.semantic_crc32c = 0;
	if (!evidence_wire(&input_grant)) return false;
	if (!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &input_grant,
		bytes, sizeof(bytes), &len, &reject)) return false;
	for (Size n = offset % sizeof(uint64); n + len <= fake_pcm_header_requested_size; n += sizeof(uint64))
		if (memcmp(memory + n, bytes, len) == 0) { qn_state = (void *)(memory + n - offset); found++; }
	if (found != 1) return false;
	qn_intent = &qn_state->requester_settlement_intent;
	return true;
}
static ResourceXApplyResult
wait_ack(void)
{
	ResourceXDecodedFrame frame;
	ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_SOURCE_COMPLETE_V11_BYTES];
	uint16 len;
	ClusterICEnvelope env = {0};
	uint16 retained_len = input_grant.body.acquire_decision.source_kind == RESOURCE_X_ACQUIRE_SOURCE_DISK
		? RESOURCE_X_COMPLETE_V5_BYTES : RESOURCE_X_SOURCE_COMPLETE_V11_BYTES;
	if (!cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		qn_intent->payload, retained_len, &frame, &reject)) return RESOURCE_X_APPLY_INVALID;
	frame.common.outcome = RESOURCE_X_OUTCOME_QUIET_NO_IMAGE_RETIRED;
	frame.common.action_node = frame.body.acquire_completion.decision.master_node;
	frame.common.sender_connection_generation = 71;
	if (!cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &frame,
		bytes, sizeof(bytes), &len, &reject)
		|| !cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
			bytes, len, &frame, &reject)) return RESOURCE_X_APPLY_INVALID;
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE;
	env.source_node_id = 0; env.dest_node_id = 1;
	env.epoch = 17; env.payload_length = len;
	auth_peer_boot = 31; /* This inbound frame is from M, not the previous H. */
	return settle_gcs(&frame, &env);
}

UT_TEST(test_wait_cancel_before_any_decision_quiets_native_without_read_or_reissue)
{
	EvidenceFixture f;
	UT_ASSERT(wait_start(&f, 0)); UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED); wait_done();
}
UT_TEST(test_wait_cancel_before_remote_current_never_installs_late_bytes)
{
	EvidenceFixture f;
	UT_ASSERT(wait_start(&f, 3)); UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(wait_material(&f, 3)); wait_done();
	UT_ASSERT(input_get() != RESOURCE_X_APPLY_APPLIED);
}
UT_TEST(test_wait_cancel_already_arrived_material_does_not_acquire_executor)
{
	for (int p = 0; p <= 3; p += 3) {
		EvidenceFixture f;
		UT_ASSERT(wait_start(&f, p)); UT_ASSERT(wait_material(&f, p));
		UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED); wait_done();
	}
}
UT_TEST(test_wait_cancel_seal_is_durable_before_native_release)
{
	EvidenceFixture f;
	UT_ASSERT(wait_start(&f, 0)); wait_seal_error = true;
	UT_ASSERT(wait_catches()); wait_seal_error = false;
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, WAIT_SEALING);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(wait_calls, 0); UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED); wait_done();
}
UT_TEST(test_wait_cancel_ERROR_after_GP_retirement_resumes_same_IO)
{
	EvidenceFixture f;
	UT_ASSERT(wait_start(&f, 0)); wait_before_abort_error = true;
	UT_ASSERT(wait_catches()); wait_before_abort_error = false;
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED); wait_done();
}
UT_TEST(test_wait_cancel_ERROR_after_IO_never_repeats_abort)
{
	EvidenceFixture f;
	UT_ASSERT(wait_start(&f, 0)); wait_after_abort_error = true;
	UT_ASSERT(wait_catches()); wait_after_abort_error = false;
	UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED); wait_done(); UT_ASSERT_EQ(wait_calls, 1);
}
UT_TEST(test_wait_cancel_ERROR_after_quiet_replays_only_original_receipt)
{
	EvidenceFixture f;
	UT_ASSERT(wait_start(&f, 0)); wait_publish_error = true;
	UT_ASSERT(wait_catches()); wait_publish_error = false;
	UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_DUPLICATE); wait_done();
}
UT_TEST(test_wait_cancel_refuses_foreign_E_or_binding_without_mutation)
{
	for (int n = 0; n < 6; n++) {
		EvidenceFixture f; UT_ASSERT(wait_start(&f, 0));
		if (n == 0) prepared_episode.backend_generation++;
		if (n == 1) prepared_episode.reservation_token++;
		if (n == 2) input_observation.entry_binding_generation++;
		if (n == 3) input_observation.request.assertion_sequence++;
		if (n == 4) request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor = 2;
		if (n == 5) request_entry()->resource_x_bootstrap_round.failed_attempt_floor = 1;
		wait_refused();
	}
}
UT_TEST(test_wait_cancel_requires_own_pin_process_IO_and_no_other_delivery)
{
	for (int n = 0; n < 6; n++) {
		EvidenceFixture f; UT_ASSERT(wait_start(&f, 0));
		if (n == 0) request_pins = 0;
		if (n == 1) MyProc->cluster_grd_generation++;
		if (n == 2) ResourceOwnerForgetBufferIO(CurrentResourceOwner, 1);
		if (n == 3) pg_atomic_write_u64(&request_own.delivery_attempt, 99);
		if (n == 4) pg_atomic_write_u64(&request_own.write_io_generation, 1);
		if (n == 5) pg_atomic_write_u32(&request_buf.state, pg_atomic_read_u32(&request_buf.state) | BM_VALID);
		wait_refused();
	}
}
UT_TEST(test_wait_cancel_admission_closed_still_seals_and_releases_original_native)
{
	EvidenceFixture f; ResourceXGateSnapshot gate;
	UT_ASSERT(wait_start(&f, 0)); UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED); wait_done();
}
UT_TEST(test_wait_cancel_passive_LMS_waits_for_material_without_corrupting_empty_join)
{
	EvidenceFixture f; ResourceXIntentSlot slot;
	UT_ASSERT(wait_start(&f, 0)); UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED);
	for (int n = 0; n < 4; n++) {
		uint32 cursor = RESOURCE_X_PROTOCOL_NODE_LIMIT + 3;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_ready_intent_probe_exact(
			&f.request.common.logical_assertion.resource, &cursor, &slot), RESOURCE_X_INTENT_PROBE_COMPLETE);
	}
	wait_done();
}
UT_TEST(test_wait_cancel_late_decision_arms_original_async_quiet)
{
	EvidenceFixture f; ResourceXIntentSlot slot;
	UT_ASSERT(wait_start(&f, 0)); UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(wait_material(&f, 0)); UT_ASSERT(qn_probe(&slot));
	UT_ASSERT_EQ(slot.body.owner_kind, RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT);
	UT_ASSERT_EQ(slot.authority_generation, f.decision.common.authority_generation); wait_done();
}
UT_TEST(test_wait_cancel_ACK_and_real_callback_both_required_in_either_order)
{
	for (int n = 0; n < 2; n++) {
		EvidenceFixture f; ResourceXIntentSlot slot;
		UT_ASSERT(wait_start(&f, 0)); UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(wait_material(&f, 0)); slot = qn_stage();
		if (n == 0) {
			UT_ASSERT_EQ(wait_ack(), RESOURCE_X_APPLY_APPLIED);
			UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 1);
			UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
		} else {
			UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
			UT_ASSERT_EQ(wait_ack(), RESOURCE_X_APPLY_APPLIED);
		}
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 0);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor, 1);
		UT_ASSERT_EQ(wait_aborts, 1); UT_ASSERT_EQ(wait_wakes, 1);
	}
}
UT_TEST(test_wait_cancel_NOT_ADMITTED_retains_body_and_receipt)
{
	EvidenceFixture f; ResourceXIntentSlot slot;
	UT_ASSERT(wait_start(&f, 0)); UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(wait_material(&f, 0)); UT_ASSERT(qn_probe(&slot));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(&slot, ++fake_pcm_clock_us), RESOURCE_X_INTENT_NOT_ADMITTED);
	UT_ASSERT_EQ(qn_intent->slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 1);
	UT_ASSERT_EQ(wait_ack(), RESOURCE_X_APPLY_APPLIED);
}
UT_TEST(test_wait_cancel_original_background_settlement_never_touches_successor)
{
	EvidenceFixture f; ResourceXIntentSlot slot; ClusterPcmOwnEntry successor;
	UT_ASSERT(wait_start(&f, 0)); UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED);
	pg_atomic_write_u64(&request_own.generation, 900);
	pg_atomic_write_u64(&request_own.reservation_token, 901);
	request_buf.pcm_state = PCM_STATE_X; request_buf.buffer_type = BUF_TYPE_XCUR;
	successor = request_own;
	UT_ASSERT(wait_material(&f, 0)); UT_ASSERT(qn_global_probe(&slot));
	UT_ASSERT_EQ(wait_ack(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&successor, &request_own, sizeof(successor)) == 0);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X);
}
UT_TEST(test_wait_cancel_partial_remote_material_waits_for_original_image)
{
	EvidenceFixture f; ResourceXRequesterJoinSnapshot join; ResourceXIntentSlot slot;
	uint32 cursor = RESOURCE_X_PROTOCOL_NODE_LIMIT + 3;
	UT_ASSERT(wait_start(&f, 3)); UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(evidence_accept(&f.decision, &join));
	UT_ASSERT((join.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_ready_intent_probe_exact(
		&f.request.common.logical_assertion.resource, &cursor, &slot), RESOURCE_X_INTENT_PROBE_COMPLETE);
	UT_ASSERT(wait_material(&f, 3)); UT_ASSERT(qn_probe(&slot));
	UT_ASSERT_EQ(slot.payload_bytes, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES);
	UT_ASSERT_EQ(wait_ack(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 0);
}
UT_TEST(test_wait_cancel_gate_loss_retains_late_debt_without_bypassing_ingress)
{
	EvidenceFixture f; ResourceXGateSnapshot gate; ResourceXIntentSlot slot;
	UT_ASSERT(wait_start(&f, 0)); UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(wait_material(&f, 0)); UT_ASSERT(qn_probe(&slot));
	/* PCM can retain the exact late cleanup input; the actual GCS entry
	 * still rejects closed admission. Neither fact is a fabricated ACK. */
	UT_ASSERT_EQ(wait_ack(), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 1);
	wait_done();
}
UT_TEST(test_wait_cancel_cannot_take_over_an_existing_normal_executor)
{
	EvidenceFixture f;
	UT_ASSERT(wait_start(&f, 0)); input_complete(&f, 0);
	UT_ASSERT_EQ(claim_get(), RESOURCE_X_APPLY_APPLIED); wait_refused();
}
UT_TEST(test_wait_cancel_failed_GP_retirement_preserves_sealed_IO_then_resumes)
{
	EvidenceFixture f;
	UT_ASSERT(wait_start(&f, 0)); quiet_retire_refused = true;
	UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, WAIT_SEALING);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1)); UT_ASSERT_EQ(wait_calls, 0);
	quiet_retire_refused = false; UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED); wait_done();
}
UT_TEST(test_wait_cancel_malformed_local_receipt_cannot_become_authority)
{
	for (int n = 0; n < 4; n++) {
		EvidenceFixture f; ResourceXIntentSlot slot;
		UT_ASSERT(wait_start(&f, 0)); UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED);
		if (n == 0) request_entry()->resource_x_bootstrap_round.acquire_read_receipt.authority_generation = 10001;
		if (n == 1) request_entry()->resource_x_bootstrap_round.acquire_read_receipt.page_lsn = 1;
		if (n == 2) request_entry()->resource_x_bootstrap_round.acquire_read_receipt.decision_crc32c = 1;
		if (n == 3) request_entry()->resource_x_bootstrap_round.acquire_read_receipt.write_io_generation = 1;
		UT_ASSERT(!qn_probe(&slot));
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 1);
	}
}
UT_TEST(test_wait_cancel_background_without_backend_keeps_full_key_on_failed_send)
{
	EvidenceFixture f; ResourceXIntentSlot slot, staged_slot; PGPROC *proc; ResourceOwner owner; int pid;
	UT_ASSERT(wait_start(&f, 0)); UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(wait_material(&f, 0)); proc = MyProc; owner = CurrentResourceOwner; pid = MyProcPid;
	MyProc = NULL; MyProcPid = 0; CurrentResourceOwner = NULL;
	UT_ASSERT(qn_global_probe(&slot));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
		&slot, ++fake_pcm_clock_us, &staged_slot), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged_slot, false, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(qn_intent->slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, WAIT_QUIET);
	MyProc = proc; CurrentResourceOwner = owner; MyProcPid = pid;
	UT_ASSERT_EQ(wait_ack(), RESOURCE_X_APPLY_APPLIED);
}
UT_TEST(test_wait_cancel_late_foreign_domain_cannot_replace_original_debt)
{
	EvidenceFixture f; ResourceXRequesterJoinSnapshot join;
	UT_ASSERT(wait_start(&f, 0)); UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED);
	f.decision.body.acquire_decision.episode.r4_generation++;
	UT_ASSERT(evidence_wire(&f.decision));
	UT_ASSERT(evidence_offer(&f.decision, &join) != RESOURCE_X_APPLY_APPLIED);
	wait_done();
}
UT_TEST(test_wait_cancel_failed_seal_is_not_misclassified_as_incomplete_material)
{
	EvidenceFixture f; ResourceXRequesterJoinSnapshot join; ResourceXIntentSlot slot;
	uint32 cursor = RESOURCE_X_PROTOCOL_NODE_LIMIT + 3;
	UT_ASSERT(wait_start(&f, 3)); UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(evidence_accept(&f.decision, &join));
	request_entry()->resource_x_bootstrap_round.head_failure_reason = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_ready_intent_probe_exact(
		&f.request.common.logical_assertion.resource, &cursor, &slot), RESOURCE_X_INTENT_PROBE_CORRUPT);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 1);
}
UT_TEST(test_wait_cancel_preserves_original_REQUEST_and_exact_callback_without_rearm)
{
	for (int sent = 0; sent < 2; sent++) {
		EvidenceFixture f;
		UT_ASSERT(wait_start(&f, 0)); UT_ASSERT_EQ(wait_run(), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(cluster_pcm_lock_resource_x_requester_send_current_exact(&f.ticket));
		UT_ASSERT(cluster_pcm_lock_resource_x_requester_send_complete_exact(&f.ticket, sent != 0, ++fake_pcm_clock_us));
		UT_ASSERT(request_send(PCM_STATE_S) != CLUSTER_LMS_ENQUEUE_ADMITTED);
		UT_ASSERT(!cluster_pcm_lock_resource_x_requester_send_complete_exact(&f.ticket, true, ++fake_pcm_clock_us));
		wait_done();
	}
}

int
main(void)
{
	UT_PLAN(24);
	UT_RUN(test_wait_cancel_before_any_decision_quiets_native_without_read_or_reissue);
	UT_RUN(test_wait_cancel_before_remote_current_never_installs_late_bytes);
	UT_RUN(test_wait_cancel_already_arrived_material_does_not_acquire_executor);
	UT_RUN(test_wait_cancel_seal_is_durable_before_native_release);
	UT_RUN(test_wait_cancel_ERROR_after_GP_retirement_resumes_same_IO);
	UT_RUN(test_wait_cancel_ERROR_after_IO_never_repeats_abort);
	UT_RUN(test_wait_cancel_ERROR_after_quiet_replays_only_original_receipt);
	UT_RUN(test_wait_cancel_refuses_foreign_E_or_binding_without_mutation);
	UT_RUN(test_wait_cancel_requires_own_pin_process_IO_and_no_other_delivery);
	UT_RUN(test_wait_cancel_admission_closed_still_seals_and_releases_original_native);
	UT_RUN(test_wait_cancel_passive_LMS_waits_for_material_without_corrupting_empty_join);
	UT_RUN(test_wait_cancel_late_decision_arms_original_async_quiet);
	UT_RUN(test_wait_cancel_ACK_and_real_callback_both_required_in_either_order);
	UT_RUN(test_wait_cancel_NOT_ADMITTED_retains_body_and_receipt);
	UT_RUN(test_wait_cancel_original_background_settlement_never_touches_successor);
	UT_RUN(test_wait_cancel_partial_remote_material_waits_for_original_image);
	UT_RUN(test_wait_cancel_gate_loss_retains_late_debt_without_bypassing_ingress);
	UT_RUN(test_wait_cancel_cannot_take_over_an_existing_normal_executor);
	UT_RUN(test_wait_cancel_failed_GP_retirement_preserves_sealed_IO_then_resumes);
	UT_RUN(test_wait_cancel_malformed_local_receipt_cannot_become_authority);
	UT_RUN(test_wait_cancel_background_without_backend_keeps_full_key_on_failed_send);
	UT_RUN(test_wait_cancel_late_foreign_domain_cannot_replace_original_debt);
	UT_RUN(test_wait_cancel_failed_seal_is_not_misclassified_as_incomplete_material);
	UT_RUN(test_wait_cancel_preserves_original_REQUEST_and_exact_callback_without_rearm);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
