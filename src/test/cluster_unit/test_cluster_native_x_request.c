/* One full X intent, passive evidence wait and original pre-executor cancel.
 * Native, PCM, codec and LMS owners are real; peers and OS events are leaves.
 * No X installation or writer activation is supplied by this fixture. */
#include "test_cluster_native_x_request_fixture.inc"

static unsigned x_native_profile;
static int x_source_profile;
static EvidenceFixture x_material;

static ResourceXApplyResult
x_begin(ClusterGcsFirstFill *request, uint8 mode)
{
#ifdef X_REQUEST_PARENT_RED
	(void) mode;
	return x_native_profile == 0
		? cluster_gcs_block_first_fill_begin(&request_buf, &request_base, request_token, request)
		: cluster_gcs_block_resident_read_begin(&request_buf, &request_base, request_token, request);
#else
	return cluster_gcs_block_acquire_begin(&request_buf, &request_base, request_token,
		x_native_profile == 0 ? RESOURCE_X_ACQUIRE_FIRST_FILL : RESOURCE_X_ACQUIRE_RESIDENT,
		mode, request);
#endif
}

static void
x_start(ClusterGcsFirstFill *request, unsigned profile)
{
	x_native_profile = profile;
	x_source_profile = profile == 3 ? 6 : profile == 4 ? 7 : 1;
	resident_start(request, profile);
	if (profile == 0) ResourceOwnerRememberBufferIO(CurrentResourceOwner, 1);
}

static void
x_deliver(void)
{
	UT_ASSERT(ordinary_evidence_begin(&x_material, x_source_profile));
	prepared_episode = x_material.request.body.acquire_intent;
	input_observation = ordinary_active->observation;
	input_grant = x_material.decision;
	input_complete(&x_material, x_source_profile);
	auth_peer_boot = 31;
}

static bool
x_seed(EvidenceFixture *f, unsigned native, int source)
{
	ClusterBufmgrFirstFill scratch;
	resident_environment(&scratch);
	x_native_profile = native;
	if (!input_begin(f, source, native)) return false;
	prepared_episode = f->request.body.acquire_intent;
	input_grant = f->decision;
	if (native == 0) ResourceOwnerRememberBufferIO(CurrentResourceOwner, 1);
	return true;
}

static ResourceXApplyResult
x_cancel(void)
{
	return x_native_profile == 0
		? cluster_bufmgr_pcm_own_acquire_cancel_wait_exact(&request_buf, &request_base,
			&prepared_episode, &input_observation)
		: resident_cancel();
}

static void
x_preserved(void)
{
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(request_buf.pcm_state, request_base.pcm_state);
	UT_ASSERT_EQ(request_buf.buffer_type, request_base.buffer_type);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state)
		& (BM_TAG_VALID | BM_VALID | BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_IN_PROGRESS | BM_IO_ERROR),
		x_native_profile == 0 ? BM_TAG_VALID | BM_IO_ERROR : request_base.semantic_buf_state);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_commits, 0);
	for (int n = 0; n < BLCKSZ; n++) UT_ASSERT_EQ((unsigned char)disk_shared.data[n], 0xA7);
}

/* Locate a separately encoded A in the retained original entry, as the
 * sibling notice fixture does. This grants no application/native evidence. */
static bool
x_anchor(EvidenceFixture *f)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES]; uint16 len; ResourceXWireReject reject;
	unsigned char *memory = (void *)fake_pcm_header.data;
	Size offset = offsetof(ClusterPcmResourceXMasterState, requester_join.grant_payload), found = 0;
	input_grant = f->decision;
	input_grant.common.sender_connection_generation = PGRAC_RESOURCE_X_RETAINED_SENDER_GENERATION;
	if (!evidence_wire(&input_grant)
		|| !cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &input_grant,
			bytes, sizeof(bytes), &len, &reject)) return false;
	for (Size n = offset % sizeof(uint64); n + len <= fake_pcm_header_requested_size; n += sizeof(uint64))
		if (memcmp(memory + n, bytes, len) == 0) { qn_state = (void *)(memory + n - offset); found++; }
	if (found != 1) return false;
	qn_intent = &qn_state->requester_settlement_intent;
	return true;
}

#define X_REQUIRE(call, expected) do { \
	ResourceXApplyResult got = (call); UT_ASSERT_EQ(got, (expected)); \
	if (got != (expected)) { complete_idle(); return; } \
} while (0)

UT_TEST(test_X_intent_issues_once_and_passively_joins_all_native_paths)
{
	for (unsigned profile = 0; profile < 5; profile++) {
		ClusterGcsFirstFill request; ClusterPcmOwnEntry before;
		x_start(&request, profile); before = request_own;
		X_REQUIRE(x_begin(&request, PCM_STATE_X), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(request.observation.request.target_mode, PCM_STATE_X);
		UT_ASSERT_EQ(request.observation.request.observed_mode, request_base.pcm_state);
		fake_cv_prepare_hook = x_deliver;
		X_REQUIRE(cluster_gcs_block_first_fill_wait(&request), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT(memcmp(&before, &request_own, sizeof(before)) == 0);
		UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_commits, 0);
		cluster_gcs_block_first_fill_end(&request); complete_idle();
	}
}
UT_TEST(test_X_wait_observation_backpressure_never_reissues)
{
	ClusterGcsFirstFill request;
	x_start(&request, 2); ordinary_unavailable_after_send = 3;
	X_REQUIRE(x_begin(&request, PCM_STATE_X), RESOURCE_X_APPLY_APPLIED);
	fake_cv_prepare_hook = x_deliver;
	X_REQUIRE(cluster_gcs_block_first_fill_wait(&request), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(ordinary_pauses, 3);
	cluster_gcs_block_first_fill_end(&request); complete_idle();
}
UT_TEST(test_X_wait_domain_drift_keeps_original_reservation)
{
	ClusterGcsFirstFill request; ClusterPcmOwnEntry before;
	x_start(&request, 1);
	X_REQUIRE(x_begin(&request, PCM_STATE_X), RESOURCE_X_APPLY_APPLIED);
	before = request_own; ordinary_connection++;
	X_REQUIRE(cluster_gcs_block_first_fill_wait(&request), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT(memcmp(&before, &request_own, sizeof(before)) == 0);
	cluster_gcs_block_first_fill_end(&request); complete_idle();
}
UT_TEST(test_invalid_acquire_modes_do_not_enter_or_send)
{
	for (int mode = 0; mode < 4; mode++) {
		ClusterGcsFirstFill request;
		x_start(&request, mode == 3 ? 3 : 0);
		X_REQUIRE(x_begin(&request, mode == 0 ? PCM_STATE_N : mode == 1 ? PCM_STATE_READ_IMAGE
			: mode == 2 ? UINT8_MAX : PCM_STATE_S), RESOURCE_X_APPLY_INVALID);
		UT_ASSERT_EQ(ordinary_sends, 0); UT_ASSERT(!request.admission.entered); complete_idle();
	}
}
UT_TEST(test_X_cancel_before_and_after_evidence_keeps_original_bytes)
{
	for (unsigned native = 0; native < 5; native++) for (int joined = 0; joined < 2; joined++) {
		EvidenceFixture f; int source = native == 3 ? 6 : native == 4 ? 7 : 1;
		UT_ASSERT(x_seed(&f, native, source));
		if (joined) input_complete(&f, source);
		X_REQUIRE(x_cancel(), RESOURCE_X_APPLY_APPLIED);
		x_preserved(); X_REQUIRE(x_cancel(), RESOURCE_X_APPLY_DUPLICATE); complete_idle();
	}
}
UT_TEST(test_X_resident_cancel_preserves_S_X_dirty_flags)
{
	for (unsigned native = 3; native < 5; native++) {
		EvidenceFixture f;
		UT_ASSERT(x_seed(&f, native, native == 3 ? 6 : 7));
		pg_atomic_fetch_or_u32(&request_buf.state, BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED);
		request_base.semantic_buf_state |= BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED;
		X_REQUIRE(x_cancel(), RESOURCE_X_APPLY_APPLIED); x_preserved(); complete_idle();
	}
}
UT_TEST(test_X_cancel_ERROR_windows_resume_same_seal_and_abort_once)
{
	for (int cut = 0; cut < 3; cut++) {
		EvidenceFixture f; volatile bool caught = false;
		UT_ASSERT(x_seed(&f, 2, 1));
		wait_seal_error = cut == 0; resident_after_abort_error = cut == 1; wait_publish_error = cut == 2;
		PG_TRY(); { (void)x_cancel(); } PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught);
		wait_seal_error = resident_after_abort_error = wait_publish_error = false;
		ResourceXApplyResult wanted = cut == 2 ? RESOURCE_X_APPLY_DUPLICATE : RESOURCE_X_APPLY_APPLIED;
		X_REQUIRE(x_cancel(), wanted); UT_ASSERT_EQ(resident_aborts, 1); x_preserved(); complete_idle();
	}
}
UT_TEST(test_X_cancel_rejects_foreign_native_process_IO_and_executor)
{
	for (int n = 0; n < 12; n++) {
		EvidenceFixture f; ClusterPcmOwnEntry before; ClusterPcmResourceXBootstrapRound round;
		UT_ASSERT(x_seed(&f, 3, 6));
		if (n == 0) request_pins = 0;
		if (n == 1) request_proc.cluster_grd_generation++;
		if (n == 2) request_buf.tag.blockNum++;
		if (n == 3) pg_atomic_fetch_add_u64(&request_own.generation, 1);
		if (n == 4) pg_atomic_fetch_add_u64(&request_own.reservation_token, 1);
		if (n == 5) ResourceOwnerRememberBufferIO(CurrentResourceOwner, 1);
		if (n == 6) pg_atomic_fetch_or_u32(&request_buf.state, BM_IO_IN_PROGRESS);
		if (n == 7) pg_atomic_write_u64(&request_own.write_io_generation, 1);
		if (n == 8) pg_atomic_write_u64(&request_own.writer_activation_token, 8);
		if (n == 9) pg_atomic_write_u64(&request_own.resource_x_activation_generation, 8);
		if (n == 10) pg_atomic_write_u64(&request_own.delivery_attempt, 8);
		if (n == 11) request_buf.buffer_type = BUF_TYPE_CURRENT;
		before = request_own; round = request_entry()->resource_x_bootstrap_round;
		ResourceXApplyResult result = x_cancel();
		UT_ASSERT(result != RESOURCE_X_APPLY_APPLIED && result != RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT(memcmp(&before, &request_own, sizeof(before)) == 0);
		UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
		complete_idle();
	}
}
UT_TEST(test_X_cancel_never_takes_a_claimed_executor)
{
	EvidenceFixture f; ClusterPcmOwnEntry before;
	UT_ASSERT(x_seed(&f, 1, 1)); input_complete(&f, 1);
	X_REQUIRE(claim_get(), RESOURCE_X_APPLY_APPLIED); before = request_own;
	UT_ASSERT(x_cancel() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, &request_own, sizeof(before)) == 0); complete_idle();
}
UT_TEST(test_X_quiet_late_disk_and_remote_material_use_original_LMS_receipt)
{
	for (int source = 1; source <= 5; source += source == 1 ? 3 : 1) for (int order = 0; order < 2; order++) {
		EvidenceFixture f; ResourceXIntentSlot slot;
		UT_ASSERT(x_seed(&f, 2, source));
		X_REQUIRE(x_cancel(), RESOURCE_X_APPLY_APPLIED);
		input_complete(&f, source); UT_ASSERT(x_anchor(&f)); slot = qn_stage();
		if (order == 0) {
			X_REQUIRE(wait_ack(), RESOURCE_X_APPLY_APPLIED);
			UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 1);
			UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
		} else {
			UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
			X_REQUIRE(wait_ack(), RESOURCE_X_APPLY_APPLIED);
		}
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 0);
		x_preserved(); complete_idle();
	}
}
UT_TEST(test_X_quiet_missing_witnesses_keep_receipt_and_do_not_send)
{
	EvidenceFixture f; ResourceXRequesterJoinSnapshot out; ResourceXIntentSlot slot;
	UT_ASSERT(x_seed(&f, 2, 4)); X_REQUIRE(x_cancel(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(evidence_accept(&f.image, &out));
	UT_ASSERT(!(out.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE));
	UT_ASSERT(!qn_probe(&slot)); UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 1);
	UT_ASSERT(evidence_accept(&f.witness, &out)); UT_ASSERT(x_anchor(&f));
	UT_ASSERT(qn_probe(&slot)); x_preserved(); complete_idle();
}
UT_TEST(test_X_quiet_late_reply_does_not_use_successor_descriptor)
{
	EvidenceFixture f; ResourceXIntentSlot slot; ClusterPcmOwnEntry successor;
	UT_ASSERT(x_seed(&f, 2, 5)); X_REQUIRE(x_cancel(), RESOURCE_X_APPLY_APPLIED);
	pg_atomic_write_u64(&request_own.generation, 100);
	pg_atomic_write_u64(&request_own.reservation_token, 101);
	request_buf.tag.blockNum++; successor = request_own;
	input_complete(&f, 5); UT_ASSERT(x_anchor(&f)); slot = qn_stage();
	X_REQUIRE(wait_ack(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
	UT_ASSERT(memcmp(&successor, &request_own, sizeof(successor)) == 0); complete_idle();
}
UT_TEST(test_X_disk_quiet_cannot_be_relabelled_as_normal_completion)
{
	EvidenceFixture f; ResourceXIntentSlot slot; ResourceXDecodedFrame frame;
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES]; uint16 len = 0; ResourceXWireReject reject;
	ResourceXAcquireReadReceipt original_receipt;
	ClusterPcmResourceXReadIntent original_intent;
	UT_ASSERT(x_seed(&f, 1, 1)); X_REQUIRE(x_cancel(), RESOURCE_X_APPLY_APPLIED);
	input_complete(&f, 1); UT_ASSERT(x_anchor(&f)); UT_ASSERT(qn_probe(&slot));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		qn_intent->payload, slot.payload_bytes, &frame, &reject));
	UT_ASSERT_EQ(frame.common.target_mode, PCM_STATE_X);
	UT_ASSERT_EQ(frame.common.outcome, RESOURCE_X_OUTCOME_QUIET_NO_IMAGE);
	original_receipt = request_entry()->resource_x_bootstrap_round.acquire_read_receipt;
	original_intent = *qn_intent;
	/* The retained native generation is cancellation evidence, not success. */
	frame.common.outcome = RESOURCE_X_OUTCOME_OK;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &frame,
		bytes, sizeof(bytes), &len, &reject));
	/* A structurally legal normal ACK cannot consume a quiet native receipt.
	 * Codec validity never substitutes for the original owner's exact I. */
	frame.body.acquire_completion.native_generation++;
	frame.common.outcome = RESOURCE_X_OUTCOME_DUPLICATE;
	frame.common.action_node = frame.body.acquire_completion.decision.master_node;
	UT_ASSERT(qn_wire(&frame));
	UT_ASSERT(settle_gcs(&frame, NULL) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&original_receipt, &request_entry()->resource_x_bootstrap_round.acquire_read_receipt,
		sizeof(original_receipt)) == 0);
	UT_ASSERT(memcmp(&original_intent, qn_intent, sizeof(original_intent)) == 0);
	x_preserved(); complete_idle();
}

int main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(13);
	UT_RUN(test_X_intent_issues_once_and_passively_joins_all_native_paths);
	UT_RUN(test_X_wait_observation_backpressure_never_reissues);
	UT_RUN(test_X_wait_domain_drift_keeps_original_reservation);
	UT_RUN(test_invalid_acquire_modes_do_not_enter_or_send);
	UT_RUN(test_X_cancel_before_and_after_evidence_keeps_original_bytes);
	UT_RUN(test_X_resident_cancel_preserves_S_X_dirty_flags);
	UT_RUN(test_X_cancel_ERROR_windows_resume_same_seal_and_abort_once);
	UT_RUN(test_X_cancel_rejects_foreign_native_process_IO_and_executor);
	UT_RUN(test_X_cancel_never_takes_a_claimed_executor);
	UT_RUN(test_X_quiet_late_disk_and_remote_material_use_original_LMS_receipt);
	UT_RUN(test_X_quiet_missing_witnesses_keep_receipt_and_do_not_send);
	UT_RUN(test_X_quiet_late_reply_does_not_use_successor_descriptor);
	UT_RUN(test_X_disk_quiet_cannot_be_relabelled_as_normal_completion);
	UT_DONE(); return ut_failed_count != 0;
}
