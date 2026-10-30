/* Resident N/CURRENT and N/PI read request responsibility. Actual native,
 * PCM, wire and LMS bodies; authenticated peers and interrupts are leaves. */
#include "test_cluster_native_first_fill_fixture.inc"

static unsigned resident_aborts;
static bool resident_after_abort_error;
#ifndef RESIDENT_REQUEST_PARENT_RED
static ClusterPcmOwnResult
resident_abort(int32 id, uint64 generation, uint64 token, uint32 flags)
{
	ClusterPcmOwnResult result = cluster_pcm_own_reservation_abort_exact(id, generation, token, flags);
	if (result == CLUSTER_PCM_OWN_OK) resident_aborts++;
	return result;
}
static void
resident_unlock(BufferDesc *buf, uint32 state)
{
	UnlockBufHdr(buf, state);
	if (resident_after_abort_error && resident_aborts != 0) pg_re_throw();
}
#define LockBufHdr stage_header_lock
#define UnlockBufHdr resident_unlock
#define GetPrivateRefCount(buffer) request_private_pin(buffer)
#define cluster_pcm_own_reservation_abort_exact resident_abort
#define cluster_pcm_lock_resource_x_requester_acquire_wait_seal_exact wait_seal
#define cluster_pcm_lock_resource_x_requester_acquire_wait_quiet_exact wait_publish
#include "test_cluster_native_resident_wait_cancel.inc"
#undef cluster_pcm_lock_resource_x_requester_acquire_wait_quiet_exact
#undef cluster_pcm_lock_resource_x_requester_acquire_wait_seal_exact
#undef cluster_pcm_own_reservation_abort_exact
#undef GetPrivateRefCount
#undef UnlockBufHdr
#undef LockBufHdr
#else
/* Run the same native inputs against the parent's real first-fill-only
 * entries. Their semantic refusals, not a missing symbol, are the RED. */
#define cluster_gcs_block_resident_read_begin cluster_gcs_block_first_fill_begin
#define cluster_bufmgr_pcm_own_acquire_cancel_resident_wait_exact cluster_bufmgr_pcm_own_acquire_cancel_wait_exact
#endif

/* UT_ASSERT is deliberately nonfatal. Stop a case after a missing parent
 * entry instead of dereferencing material that was never produced. */
#define RESIDENT_REQUIRE_EQ(call, expected) do { \
	ResourceXApplyResult got = (call); \
	UT_ASSERT_EQ(got, (expected)); \
	if (got != (expected)) { complete_idle(); return; } \
} while (0)

static void
resident_environment(ClusterBufmgrFirstFill *scratch)
{
	ordinary_start(scratch, 2);
	ResourceOwnerForgetBufferIO(CurrentResourceOwner, 1);
	resident_aborts = 0; resident_after_abort_error = false;
}
static void
resident_start(ClusterGcsFirstFill *request, unsigned profile)
{
	ClusterBufmgrFirstFill scratch;
	resident_environment(&scratch);
	request_start(profile, false);
	/* Only this fresh fixture's unused preparation is reset; no product
 * request has been issued. The actual native reservation stays intact. */
	reset_fake_pcm_runtime(4); cluster_node_id = 1; ut_master_session = 101;
	master_durable_admission = true;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17), RESOURCE_X_APPLY_APPLIED);
	memset(request, 0, sizeof(*request)); ordinary_active = request;
	MyBackendType = B_BACKEND;
}
static bool
resident_seed(EvidenceFixture *f, int source, unsigned profile)
{
	ClusterBufmgrFirstFill scratch;
	resident_environment(&scratch);
	if (!input_begin(f, source, profile)) return false;
	prepared_episode = f->request.body.acquire_intent;
	input_grant = f->decision;
	return true;
}
static ResourceXApplyResult
resident_cancel(void)
{
	return cluster_bufmgr_pcm_own_acquire_cancel_resident_wait_exact(&request_buf, &request_base,
		&prepared_episode, &input_observation);
}
static bool
resident_catches(void)
{
	volatile bool caught = false;
	PG_TRY(); { (void)resident_cancel(); } PG_CATCH(); { caught = true; } PG_END_TRY();
	return caught;
}
static void
resident_unchanged_bytes(void)
{
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_ERROR | BM_IO_IN_PROGRESS), BM_VALID);
	UT_ASSERT_EQ(request_buf.buffer_type, request_base.buffer_type);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(wait_calls, 0); UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	for (int n = 0; n < BLCKSZ; n++) UT_ASSERT_EQ((unsigned char)disk_shared.data[n], 0xA7);
}
static void
resident_quiet(void)
{
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, WAIT_QUIET);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(resident_aborts, 1);
	resident_unchanged_bytes();
}
static void
resident_refused(void)
{
	ClusterPcmOwnEntry own = request_own;
	ClusterPcmResourceXBootstrapRound round = request_entry()->resource_x_bootstrap_round;
	uint32 state = pg_atomic_read_u32(&request_buf.state);
	ResourceXApplyResult result = resident_cancel();
	UT_ASSERT(result != RESOURCE_X_APPLY_APPLIED && result != RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state), state);
	UT_ASSERT_EQ(resident_aborts, 0); UT_ASSERT_EQ(wait_calls, 0);
}

UT_TEST(test_resident_current_and_PI_issue_once_and_passively_join)
{
	for (unsigned profile = 1; profile <= 2; profile++) {
		ClusterGcsFirstFill request; ClusterPcmOwnEntry own;
		resident_start(&request, profile); own = request_own;
		RESIDENT_REQUIRE_EQ(cluster_gcs_block_resident_read_begin(&request_buf, &request_base, request_token, &request), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(request.episode.path, RESOURCE_X_ACQUIRE_RESIDENT);
		UT_ASSERT_EQ(cluster_gcs_block_first_fill_wait(&request), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
		resident_unchanged_bytes(); cluster_gcs_block_first_fill_end(&request); complete_idle();
	}
}
UT_TEST(test_resident_empty_wait_slices_keep_one_request)
{
	ClusterGcsFirstFill request;
	resident_start(&request, 2);
	RESIDENT_REQUIRE_EQ(cluster_gcs_block_resident_read_begin(&request_buf, &request_base, request_token, &request), RESOURCE_X_APPLY_APPLIED);
	fake_cv_prepare_hook = NULL;
	for (int n = 0; n < 4; n++)
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_wait_exact(&request.observation, &request.episode), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(ordinary_sends, 1); resident_unchanged_bytes();
	cluster_gcs_block_first_fill_end(&request); complete_idle();
}
UT_TEST(test_resident_completion_during_wait_enrollment_is_not_lost)
{
	ClusterGcsFirstFill request;
	resident_start(&request, 1);
	RESIDENT_REQUIRE_EQ(cluster_gcs_block_resident_read_begin(&request_buf, &request_base, request_token, &request), RESOURCE_X_APPLY_APPLIED);
	/* The real CV preparation invokes the controlled peer delivery leaf. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_wait_exact(&request.observation, &request.episode), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ordinary_sends, 1); resident_unchanged_bytes();
	cluster_gcs_block_first_fill_end(&request); complete_idle();
}
UT_TEST(test_resident_cancel_before_or_after_join_preserves_current_and_PI)
{
	for (unsigned profile = 1; profile <= 2; profile++) for (int joined = 0; joined < 2; joined++) {
		EvidenceFixture f;
		UT_ASSERT(resident_seed(&f, 2, profile));
		if (joined) input_complete(&f, 2);
		UT_ASSERT_EQ(resident_cancel(), RESOURCE_X_APPLY_APPLIED);
		resident_quiet(); complete_idle();
	}
}
UT_TEST(test_resident_cancel_replay_does_not_abort_twice)
{
	EvidenceFixture f;
	UT_ASSERT(resident_seed(&f, 2, 2));
	UT_ASSERT_EQ(resident_cancel(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(resident_cancel(), RESOURCE_X_APPLY_DUPLICATE);
	resident_quiet(); complete_idle();
}
UT_TEST(test_resident_ERROR_after_seal_resumes_original_GP)
{
	EvidenceFixture f;
	UT_ASSERT(resident_seed(&f, 2, 2)); wait_seal_error = true;
	UT_ASSERT(resident_catches());
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(resident_aborts, 0);
	wait_seal_error = false; UT_ASSERT_EQ(resident_cancel(), RESOURCE_X_APPLY_APPLIED);
	resident_quiet(); complete_idle();
}
UT_TEST(test_resident_ERROR_after_GP_abort_resumes_without_second_abort)
{
	EvidenceFixture f;
	UT_ASSERT(resident_seed(&f, 2, 1)); resident_after_abort_error = true;
	UT_ASSERT(resident_catches()); UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	resident_after_abort_error = false; UT_ASSERT_EQ(resident_cancel(), RESOURCE_X_APPLY_APPLIED);
	resident_quiet(); complete_idle();
}
UT_TEST(test_resident_ERROR_after_quiet_is_idempotent)
{
	EvidenceFixture f;
	UT_ASSERT(resident_seed(&f, 2, 2)); wait_publish_error = true;
	UT_ASSERT(resident_catches()); wait_publish_error = false;
	UT_ASSERT_EQ(resident_cancel(), RESOURCE_X_APPLY_DUPLICATE);
	resident_quiet(); complete_idle();
}
UT_TEST(test_resident_cancel_rejects_changed_native_owner_and_IO)
{
	for (int n = 0; n < 17; n++) {
		EvidenceFixture f;
		UT_ASSERT(resident_seed(&f, 2, 2));
		if (n == 0) request_pins = 0;
		if (n == 1) request_proc.cluster_grd_generation++;
		if (n == 2) request_buf.tag.blockNum++;
		if (n == 3) pg_atomic_fetch_add_u64(&request_own.generation, 1);
		if (n == 4) pg_atomic_fetch_add_u64(&request_own.reservation_token, 1);
		if (n == 5) ResourceOwnerRememberBufferIO(CurrentResourceOwner, 1);
		if (n == 6) pg_atomic_fetch_or_u32(&request_buf.state, BM_IO_IN_PROGRESS);
		if (n == 7) pg_atomic_fetch_or_u32(&request_buf.state, BM_IO_ERROR);
		if (n == 8) pg_atomic_write_u64(&request_own.write_io_generation, 1);
		if (n == 9) request_buf.buffer_type = BUF_TYPE_CURRENT;
		if (n == 10) pg_atomic_write_u64(&request_own.writer_activation_token, 8);
		if (n == 11) request_proc.pid++;
		if (n == 12) pg_atomic_write_u64(&request_own.resource_x_activation_generation, 8);
		if (n == 13) pg_atomic_write_u64(&request_own.delivery_attempt, 8);
		if (n == 14) request_buf.pcm_state = PCM_STATE_S;
		if (n == 15) pg_atomic_write_u32(&request_own.flags, PCM_OWN_FLAG_REVOKING);
		if (n == 16) pg_atomic_write_u64(&request_own.write_io_generation, UINT64_MAX - 1);
		resident_refused(); complete_idle();
	}
}
UT_TEST(test_resident_incomplete_join_waits_but_lost_binding_is_terminal)
{
	EvidenceFixture f; ResourceXRequesterJoinSnapshot out;
	UT_ASSERT(resident_seed(&f, 3, 2));
	UT_ASSERT(evidence_accept(&f.decision, &out));
	UT_ASSERT(!(out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_wait_exact(&input_observation,
		&prepared_episode), RESOURCE_X_APPLY_NOT_FOUND);
	input_complete(&f, 3);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_wait_exact(&input_observation,
		&prepared_episode), RESOURCE_X_APPLY_APPLIED);
	input_observation.entry_binding_generation++;
	UT_ASSERT(cluster_pcm_lock_resource_x_requester_acquire_wait_exact(&input_observation,
		&prepared_episode) != RESOURCE_X_APPLY_NOT_FOUND);
	resident_unchanged_bytes(); complete_idle();
}
UT_TEST(test_resident_read_entry_does_not_issue_S_or_X_conversion)
{
	for (unsigned profile = 3; profile <= 4; profile++) {
		ClusterGcsFirstFill request;
		resident_start(&request, profile);
		UT_ASSERT_EQ(cluster_gcs_block_resident_read_begin(&request_buf, &request_base,
			request_token, &request), RESOURCE_X_APPLY_INVALID);
		UT_ASSERT_EQ(ordinary_sends, 0); UT_ASSERT(!request.admission.entered); complete_idle();
	}
}
UT_TEST(test_resident_wait_cancel_does_not_take_claimed_executor)
{
	EvidenceFixture f;
	UT_ASSERT(resident_seed(&f, 2, 2)); input_complete(&f, 2);
	UT_ASSERT_EQ(claim_get(), RESOURCE_X_APPLY_APPLIED);
	resident_refused(); complete_idle();
}
UT_TEST(test_resident_closed_gate_keeps_original_cancel_responsibility)
{
	EvidenceFixture f; ResourceXGateSnapshot gate;
	UT_ASSERT(resident_seed(&f, 2, 1)); UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(resident_cancel(), RESOURCE_X_APPLY_APPLIED);
	resident_quiet(); complete_idle();
}
UT_TEST(test_resident_unsent_retirement_preserves_native_and_attempt_floor)
{
	ClusterBufmgrFirstFill scratch; ResourceXDecodedAcquireIntent episode;
	ResourceXInstallClaimJoinObservation issued, prepared;
	ClusterPcmOwnEntry own; uint64 attempt;
	resident_environment(&scratch); request_start(2, false);
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_acquire_intent_exact(&request_buf, &request_base,
		request_token, RESOURCE_X_ACQUIRE_RESIDENT, &request_admission, NULL, &episode), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_claim_join_observe_exact(
		&request_prepared.common.logical_assertion, 0, 17, 31, 77, 51, 61, &prepared), RESOURCE_X_APPLY_APPLIED);
	own = request_own;
	attempt = prepared.request.assertion_sequence;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_first_fill_retire_unsent_exact(&prepared, &episode, &issued), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.phase, 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.highest_attempt_floor, attempt);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor, attempt);
	UT_ASSERT_EQ(request_shared.rings[1].count, 0);
	resident_unchanged_bytes(); complete_idle();
}
UT_TEST(test_resident_late_material_uses_real_LMS_and_both_retirement_events)
{
	for (int source = 0; source <= 3; source += source == 0 ? 2 : 1) for (int order = 0; order < 2; order++) {
		EvidenceFixture f; ResourceXIntentSlot slot;
		UT_ASSERT(resident_seed(&f, source, 2)); RESIDENT_REQUIRE_EQ(resident_cancel(), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(wait_material(&f, source)); slot = qn_stage();
		if (order == 0) {
			UT_ASSERT_EQ(wait_ack(), RESOURCE_X_APPLY_APPLIED);
			UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 1);
			UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
		} else {
			UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
			UT_ASSERT_EQ(wait_ack(), RESOURCE_X_APPLY_APPLIED);
		}
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 0);
		resident_unchanged_bytes(); complete_idle();
	}
}
UT_TEST(test_resident_quiet_settlement_does_not_sample_successor_native)
{
	EvidenceFixture f; ResourceXIntentSlot slot; ClusterPcmOwnEntry successor;
	UT_ASSERT(resident_seed(&f, 2, 2)); RESIDENT_REQUIRE_EQ(resident_cancel(), RESOURCE_X_APPLY_APPLIED);
	/* B has reused the descriptor. Only A's full-E receipt may be consumed. */
	pg_atomic_write_u64(&request_own.generation, 100);
	pg_atomic_write_u64(&request_own.reservation_token, 101);
	request_buf.tag.blockNum++; successor = request_own;
	UT_ASSERT(wait_material(&f, 2)); slot = qn_stage();
	UT_ASSERT_EQ(wait_ack(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
	UT_ASSERT(memcmp(&successor, &request_own, sizeof(successor)) == 0);
	UT_ASSERT_EQ(request_buf.tag.blockNum, request_base.tag.blockNum + 1); complete_idle();
}
UT_TEST(test_resident_disk_quiet_codec_never_authorizes_normal_disk_completion)
{
	EvidenceFixture f; ResourceXIntentSlot slot; ResourceXDecodedFrame frame, bad;
	ResourceXWireReject reject; uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES]; uint16 len;
	UT_ASSERT(resident_seed(&f, 0, 2)); RESIDENT_REQUIRE_EQ(resident_cancel(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(wait_material(&f, 0)); UT_ASSERT(qn_probe(&slot));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, qn_intent->payload,
		slot.payload_bytes, &frame, &reject));
	UT_ASSERT_EQ(frame.body.acquire_completion.decision.episode.path, RESOURCE_X_ACQUIRE_RESIDENT);
	for (int n = 0; n < 4; n++) {
		bad = frame;
		if (n == 0) bad.common.outcome = RESOURCE_X_OUTCOME_OK;
		if (n == 1) bad.common.target_mode = PCM_STATE_X;
		if (n == 2) bad.common.observed_mode = PCM_STATE_S;
		if (n == 3) bad.body.acquire_completion.page_crc32c = 1;
		UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &bad,
			bytes, sizeof(bytes), &len, &reject));
	}
	complete_idle();
}

int
main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(17);
	UT_RUN(test_resident_current_and_PI_issue_once_and_passively_join);
	UT_RUN(test_resident_empty_wait_slices_keep_one_request);
	UT_RUN(test_resident_completion_during_wait_enrollment_is_not_lost);
	UT_RUN(test_resident_cancel_before_or_after_join_preserves_current_and_PI);
	UT_RUN(test_resident_cancel_replay_does_not_abort_twice);
	UT_RUN(test_resident_ERROR_after_seal_resumes_original_GP);
	UT_RUN(test_resident_ERROR_after_GP_abort_resumes_without_second_abort);
	UT_RUN(test_resident_ERROR_after_quiet_is_idempotent);
	UT_RUN(test_resident_cancel_rejects_changed_native_owner_and_IO);
	UT_RUN(test_resident_incomplete_join_waits_but_lost_binding_is_terminal);
	UT_RUN(test_resident_read_entry_does_not_issue_S_or_X_conversion);
	UT_RUN(test_resident_wait_cancel_does_not_take_claimed_executor);
	UT_RUN(test_resident_closed_gate_keeps_original_cancel_responsibility);
	UT_RUN(test_resident_unsent_retirement_preserves_native_and_attempt_floor);
	UT_RUN(test_resident_late_material_uses_real_LMS_and_both_retirement_events);
	UT_RUN(test_resident_quiet_settlement_does_not_sample_successor_native);
	UT_RUN(test_resident_disk_quiet_codec_never_authorizes_normal_disk_completion);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
