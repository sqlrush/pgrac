/* Original first-fill ERROR cleanup, LMS receipt and independent master.
 * Only OS transport/storage/locks and authenticated peer leaves are controlled.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_native_master_fixture.inc"

static unsigned cm_cut;
static bool cm_ack_before_callback;
static void cm_error(void)
{
	disk_on_enter = disk_on_read = disk_on_end = NULL;
	pg_re_throw();
}
static bool cm_notice_probe(ResourceXIntentSlot *slot)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES];
	ResourceXAcquisitionRef ref;
	uint32 examined;
	fake_pcm_clock_us += 1000000;
	for (int n = 0; n < 256; n++) {
		ResourceXIntentProbeResult result = cluster_pcm_lock_resource_x_outbound_work_probe_exact(
			4, slot, bytes, sizeof(bytes), &examined, &ref);
		NM_REQUIRE(result != RESOURCE_X_INTENT_PROBE_CORRUPT);
		if (result == RESOURCE_X_INTENT_PROBE_FOUND) return true;
		if (result == RESOURCE_X_INTENT_PROBE_IDLE) break;
	}
	return false;
}
static bool cm_parent(int fd, int order)
{
	ClusterBufmgrFirstFill read;
	ClusterPcmResourceXMasterState *state = NULL;
	ResourceXIntentSlot observed, staged;
	ResourceXDecodedFrame notice, consumed;
	ResourceXRequesterJoinSnapshot joined;
	ClusterICEnvelope env = {0};
	ClusterPcmResourceXBootstrapRound *round;
	ordinary_start(&read, 0); nm_fd = fd; nm_mode = 0;
	ordinary_delivery_transport = nm_deliver;
	if (cm_cut == 0) disk_on_enter = cm_error;
	else if (cm_cut == 1) disk_read_error = true;
	else if (cm_cut == 2) disk_on_read = cm_error;
	else complete_after_io_error = true;
	NM_REQUIRE(!ordinary_run(&read));
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(ordinary_leaves, 1);
	UT_ASSERT(!read.pending); UT_ASSERT(!fake_cv_prepared);
	UT_ASSERT_EQ(disk_reads, cm_cut == 0 ? 0 : 1);
	UT_ASSERT_EQ(complete_commits, cm_cut == 3 ? 1 : 0);
	UT_ASSERT_EQ(complete_broadcasts, cm_cut == 3 ? 1 : 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + (cm_cut == 3));
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR),
		cm_cut == 3 ? BM_VALID : BM_IO_ERROR);
	NM_REQUIRE(nm_requester_notice(&state));
	/* The original LMS, not the failed backend, creates/offers the shared
	 * receipt. No DONE/QUIET/RETIRED bytes are built by this test. */
	MyBackendType = B_LMS;
	NM_REQUIRE(cm_notice_probe(&observed));
	UT_ASSERT_EQ(observed.body.owner_kind, RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
		&observed, ++fake_pcm_clock_us, &staged), RESOURCE_X_INTENT_STAGED);
	NM_REQUIRE(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		state->requester_settlement_intent.payload, RESOURCE_X_COMPLETE_V5_BYTES, &notice,
		&(ResourceXWireReject){0}));
	/* Disk bytes are still unpublished at all three read cuts. Preparation
	 * arms its receipt only after stage_disk returns; read completion alone
	 * therefore cannot turn NO_IMAGE into an armed-image quiet receipt. */
	UT_ASSERT_EQ(notice.common.outcome, cm_cut == 3 ? RESOURCE_X_OUTCOME_OK
		: RESOURCE_X_OUTCOME_QUIET_NO_IMAGE);
	UT_ASSERT_EQ(notice.body.acquire_completion.native_generation,
		request_base.generation + (cm_cut == 3));
	if (!cm_ack_before_callback) NM_REQUIRE(nm_master_callback(&state->requester_settlement_intent.slot, true));
	NM_REQUIRE(nm_send(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &notice));
	NM_REQUIRE(nm_receive(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &consumed));
	UT_ASSERT_EQ(consumed.common.outcome, cm_cut == 3 ? RESOURCE_X_OUTCOME_DUPLICATE
		: RESOURCE_X_OUTCOME_QUIET_NO_IMAGE_RETIRED);
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE;
	env.source_node_id = 0; env.dest_node_id = 1; env.epoch = 17;
	env.payload_length = consumed.payload_bytes; auth_peer_boot = 31;
	NM_REQUIRE(gcs_block_resource_x_requester_join_ingress(&env, &consumed, 61, &joined)
		== RESOURCE_X_APPLY_APPLIED);
	if (cm_ack_before_callback) {
		UT_ASSERT_EQ(state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
		NM_REQUIRE(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(
			&staged, true, ++fake_pcm_clock_us));
	}
	round = &request_entry()->resource_x_bootstrap_round;
	UT_ASSERT_EQ(state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(round->send.pending, 0);
	if (cm_cut != 3) {
		UT_ASSERT_EQ(round->has_acquire_intent, 0);
		UT_ASSERT_EQ(round->delivery_executor_purpose, 0);
		UT_ASSERT_EQ(round->cancelled_attempt_floor, notice.common.assertion_sequence);
	}
	UT_ASSERT_EQ(ordinary_sends, 1);
	UT_ASSERT_EQ(complete_commits, cm_cut == 3 ? 1 : 0);
	UT_ASSERT_EQ(complete_broadcasts, cm_cut == 3 ? 1 : 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0); (void)order;
	complete_idle(); return true;
}
static void cm_run(unsigned cut, int order, bool late_callback)
{
	cm_cut = cut; cm_ack_before_callback = late_callback;
	nm_run_roles(order, nm_master, cm_parent);
}
UT_TEST(test_cancel_before_disk_after_real_master_grant) { cm_run(0, 0, false); }
UT_TEST(test_disk_ERROR_quiet_retires_at_real_master) { cm_run(1, 0, false); }
UT_TEST(test_cancel_after_DATA_before_C) { cm_run(2, 0, false); }
UT_TEST(test_ERROR_after_C_keeps_installed_and_completes_original_notice) { cm_run(3, 0, false); }
UT_TEST(test_quiet_waits_master_grant_physical_callback) { cm_run(1, 1, false); }
UT_TEST(test_master_failed_retired_notice_keeps_original_owner) { cm_run(1, 3, false); }
UT_TEST(test_quiet_ACK_before_requester_physical_callback) { cm_run(1, 0, true); }
UT_TEST(test_installed_ACK_before_requester_physical_callback) { cm_run(3, 0, true); }
int main(void)
{
	UT_PLAN(8);
	UT_RUN(test_cancel_before_disk_after_real_master_grant);
	UT_RUN(test_disk_ERROR_quiet_retires_at_real_master);
	UT_RUN(test_cancel_after_DATA_before_C);
	UT_RUN(test_ERROR_after_C_keeps_installed_and_completes_original_notice);
	UT_RUN(test_quiet_waits_master_grant_physical_callback);
	UT_RUN(test_master_failed_retired_notice_keeps_original_owner);
	UT_RUN(test_quiet_ACK_before_requester_physical_callback);
	UT_RUN(test_installed_ACK_before_requester_physical_callback);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
