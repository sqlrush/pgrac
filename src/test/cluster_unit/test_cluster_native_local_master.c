/* Requester and master share their actual PCM entry. Only authenticated
 * environment, transport scheduling and storage leaves are controlled.
 * Author: SqlRush <sqlrush@gmail.com> */
#define NATIVE_MASTER_RESIDENT_WRITE_COMPOSITION
#include "test_cluster_native_master_fixture.inc"
#undef NATIVE_MASTER_RESIDENT_WRITE_COMPOSITION

static PublishFixture lm_master;
static int lm_mode;
static bool lm_admit(const ResourceXDecodedFrame *request)
{
	ClusterICEnvelope env = {0}; ResourceXMasterSnapshot snapshot;
	ClusterPcmResourceXMasterRequest canonical = {0};
	unsigned matches = 0; Size offset;
	unsigned char *memory = (unsigned char *)fake_pcm_header.data;
	memset(&lm_master, 0, sizeof(lm_master)); lm_master.request = *request;
	env.msg_type = RESOURCE_X_MSG_ASSERT_X; env.epoch = 17;
	env.source_node_id = env.dest_node_id = 0; env.payload_length = request->payload_bytes;
	NM_REQUIRE(gcs_block_resource_x_bootstrapped_assert_ingress(&env, request, 1, &snapshot) == RESOURCE_X_APPLY_APPLIED);
	lm_master.work = bootstrap_next_work();
	NM_REQUIRE(acquire_canonical_resume(&lm_master.work) == RESOURCE_X_APPLY_APPLIED);
	lm_master.plan = decision_plan_read(&lm_master.work);
	canonical.base_authority_generation = lm_master.plan.input.canonical_base_generation;
	canonical.resource_formation = lm_master.work.request.resource_formation;
	canonical.master_session_incarnation = lm_master.work.request.master_session_incarnation;
	canonical.assertion_sequence = lm_master.work.request.assertion_sequence;
	canonical.enqueue_order = lm_master.work.enqueue_order;
	canonical.sender_connection_generation = lm_master.work.request.sender_connection_generation;
	canonical.source_node = -1; canonical.phase = RESOURCE_X_MASTER_QUEUED;
	offset = offsetof(ClusterPcmResourceXMasterState, requests);
	for (Size n = offset; n + sizeof(canonical) <= fake_pcm_header_requested_size; n += 8)
		if (memcmp(memory + n, &canonical, sizeof(canonical)) == 0) {
			lm_master.state = (void *)(memory + n - offset); matches++;
		}
	NM_REQUIRE(matches == 1); publish_apply(&lm_master); return true;
}
static bool lm_wire(uint16 type, const ResourceXDecodedFrame *frame,
	ResourceXDecodedFrame *decoded)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES];
	uint16 size = 0; ResourceXWireReject reject;
	NM_REQUIRE(cluster_resource_x_wire_encode(type, frame, bytes, sizeof(bytes), &size, &reject));
	NM_REQUIRE(cluster_resource_x_wire_decode(type, bytes, size, decoded, &reject));
	return true;
}
static ResourceXApplyResult lm_receive_exact(uint16 type, const ResourceXDecodedFrame *frame, uint32 ingress)
{
	ClusterICEnvelope env = {0}; ResourceXRequesterJoinSnapshot joined;
	ResourceXApplyResult result;
	env.msg_type = type;
	env.source_node_id = env.dest_node_id = 0; env.epoch = 17;
	env.payload_length = frame->payload_bytes; auth_peer_boot = 31;
	result = gcs_block_resource_x_requester_join_ingress(&env, frame, ingress, &joined);
	return result;
}
static ResourceXApplyResult lm_receive(uint16 type, const ResourceXDecodedFrame *frame)
{
	return lm_receive_exact(type, frame, 1);
}
static bool lm_deliver_actual(void)
{
	ResourceXDecodedFrame request, decoded;
	ResourceXIntentSlot ticket = request_ticket(&request);
	UT_ASSERT_EQ(disk_reads, 0);
	NM_REQUIRE(request.common.logical_assertion.requester_node == 0);
	NM_REQUIRE(request.body.acquire_intent.requester_incarnation == 31);
	NM_REQUIRE(lm_wire(RESOURCE_X_MSG_ASSERT_X, &request, &decoded));
	cluster_lms_outbound_resource_x_send_complete(&ticket, true);
	auth_peer_boot = 31;
	NM_REQUIRE(lm_admit(&decoded));
	nm_grant = publish_decode(lm_master.state->grant_intent.payload, false);
	UT_ASSERT_EQ(nm_grant.body.acquire_decision.required_holders_bitmap, 0);
	if (lm_mode != 1) NM_REQUIRE(nm_master_callback(&lm_master.state->grant_intent.slot, true));
	prepared_episode = request.body.acquire_intent;
	input_observation = ordinary_active->observation; input_grant = nm_grant;
	NM_REQUIRE(lm_wire(RESOURCE_X_MSG_IMAGE_OR_GRANT, &nm_grant, &decoded));
	if (lm_mode == 2) {
		ClusterPcmResourceXRequesterJoin before = lm_master.state->requester_join;
		/* A remote connection generation cannot authenticate a self frame.
		 * The actual receiver must preserve both the join and native owner. */
		NM_REQUIRE(lm_receive_exact(RESOURCE_X_MSG_IMAGE_OR_GRANT, &decoded, 61) == RESOURCE_X_APPLY_STALE);
		UT_ASSERT(memcmp(&before, &lm_master.state->requester_join, sizeof(before)) == 0);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), prepared_episode.base_generation);
		UT_ASSERT_EQ(disk_reads, 0);
	}
	NM_REQUIRE(lm_receive(RESOURCE_X_MSG_IMAGE_OR_GRANT, &decoded) == RESOURCE_X_APPLY_APPLIED);
	if (lm_mode == 2) NM_REQUIRE(lm_receive(RESOURCE_X_MSG_IMAGE_OR_GRANT, &decoded) == RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), prepared_episode.base_generation);
	return !ut_current_failed;
}
static void lm_deliver(void)
{
	if (!lm_deliver_actual()) pg_re_throw();
}
static bool lm_settle(void)
{
	ClusterPcmResourceXMasterState *state = NULL;
	ResourceXDecodedFrame done, notice, decoded; ResourceXWireReject reject;
	ClusterPcmResourceXMasterRequest empty = {0};
	ClusterICEnvelope env = {0};
	NM_REQUIRE(nm_requester_notice(&state));
	NM_REQUIRE(state == lm_master.state);
	NM_REQUIRE(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		state->requester_settlement_intent.payload, state->requester_settlement_intent.slot.payload_bytes, &done, &reject));
	NM_REQUIRE(nm_master_callback(&state->requester_settlement_intent.slot, true));
	NM_REQUIRE(lm_wire(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done, &decoded));
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE; env.epoch = 17;
	env.source_node_id = env.dest_node_id = 0; env.payload_length = decoded.payload_bytes;
	NM_REQUIRE(gcs_block_resource_x_acquire_done_ingress(&env, &decoded, 1) == RESOURCE_X_APPLY_APPLIED);
	if (lm_mode == 1) {
		UT_ASSERT(state->deferred_settlement.present != 0);
		UT_ASSERT(state->requests[0].phase != RESOURCE_X_MASTER_NONE);
		NM_REQUIRE(nm_master_callback(&state->grant_intent.slot, true));
		NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&lm_master.work) == RESOURCE_X_APPLY_APPLIED);
	}
	NM_REQUIRE(state->deferred_settlement.present == 3);
	if (lm_mode == 2) NM_REQUIRE(gcs_block_resource_x_acquire_done_ingress(&env, &decoded, 1) == RESOURCE_X_APPLY_DUPLICATE);
	NM_REQUIRE(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		state->grant_intent.payload, state->grant_intent.slot.payload_bytes, &notice, &reject));
	if (lm_mode == 3) {
		NM_REQUIRE(nm_master_callback(&state->grant_intent.slot, false));
		UT_ASSERT_EQ(state->deferred_settlement.present, 3);
		UT_ASSERT_EQ(state->grant_intent.slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
		UT_ASSERT_EQ(state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
			RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
		fake_pcm_clock_us += 1000000;
	}
	NM_REQUIRE(lm_wire(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &notice, &decoded));
	NM_REQUIRE(lm_receive(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &decoded) == RESOURCE_X_APPLY_APPLIED);
	if (lm_mode == 2) NM_REQUIRE(lm_receive(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &decoded) == RESOURCE_X_APPLY_DUPLICATE);
	NM_REQUIRE(nm_master_callback(&state->grant_intent.slot, true));
	UT_ASSERT(memcmp(&state->requests[0], &empty, sizeof(empty)) == 0);
	UT_ASSERT_EQ(state->deferred_settlement.present, 0);
	UT_ASSERT_EQ(state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.pending, 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 9);
	/* S becomes resident authorization; X keeps its terminal writer cover. */
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent,
		request_buf.pcm_state == PCM_STATE_S ? 0 : 1);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	return !ut_current_failed;
}
static bool lm_read(int mode)
{
	ClusterBufmgrFirstFill read;
	/* Fresh case only: reset process-local ledger alongside the new PCM
	 * fixture. The later S->X episode must preserve this case's S state. */
	MemSet(cluster_bufmgr_pcm_x_writer_ledger, 0, sizeof(cluster_bufmgr_pcm_x_writer_ledger));
	ordinary_start(&read, 0); cluster_node_id = 0; ut_master_session = auth_peer_boot = 31;
	ordinary_connection = ready_connection_generation = 1;
	lm_mode = mode; ordinary_delivery_transport = lm_deliver;
	NM_REQUIRE(ordinary_run(&read));
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, 1);
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_io_calls, 1);
	UT_ASSERT_EQ(complete_broadcasts, 1);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), prepared_episode.base_generation + 1);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(nm_grant.body.acquire_decision.source_kind, RESOURCE_X_ACQUIRE_SOURCE_DISK);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT(memcmp(disk_shared.data, disk_bytes.data, BLCKSZ) == 0);
	NM_REQUIRE(lm_settle()); complete_idle(); return !ut_current_failed;
}
static bool lm_writer(int mode, bool read_first)
{
	ClusterPcmXWriterLedgerEntry *entry = NULL;
	ResourceXAcquisitionRef ref;
	uint64 previous_generation;
	if (read_first) {
		NM_REQUIRE(lm_read(0));
		/* Reset exhausted transport leaves, preserving the actual resident
		 * S grant, canonical directory and native generation. */
		memset(&request_shared, 0, sizeof(request_shared));
		pg_atomic_init_u64(&request_shared.resource_x_transport_mutation_sequence, 1);
		ordinary_sends = ordinary_pauses = ordinary_leaves = 0;
		ordinary_peer_samples = ordinary_current_calls = 0;
		complete_commits = complete_io_calls = complete_broadcasts = 0; disk_reads = 0;
		wc_legacy = wc_failures = wc_waits = wc_object_checks = 0;
		wc_unavailable = wc_pauses = 0; wc_cancel_pause = false;
		wc_path = RESOURCE_X_WRITER_TARGET; wc_object_enabled = false;
		wc_r4 = 77; wc_session_id = 31; wc_master = 0;
		ordinary_active = NULL;
	} else {
		rx_writer_start(1, 1); cluster_node_id = 0; ut_master_session = 31;
	}
	ordinary_connection = ready_connection_generation = 1;
	auth_peer_boot = 31; lm_mode = mode;
	previous_generation = cluster_pcm_own_gen_get(0);
	ordinary_delivery_transport = lm_deliver;
	entry = rx_prepare(NULL); NM_REQUIRE(entry != NULL);
	UT_ASSERT(entry != NULL); UT_ASSERT_EQ(ordinary_sends, 1);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), previous_generation + 1);
	UT_ASSERT_EQ(complete_io_calls, read_first ? 0 : 1);
	UT_ASSERT_EQ(complete_broadcasts, read_first ? 0 : 1);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X); UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_XCUR);
	ref.assertion = input_observation.request.logical_assertion;
	ref.formation = input_observation.request.resource_formation;
	ref.acquisition_generation = input_observation.request.assertion_sequence;
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(&ref, 31, 77, previous_generation + 1));
	UT_ASSERT_EQ(disk_reads, read_first ? 0 : 1);
	UT_ASSERT_EQ(nm_grant.body.acquire_decision.source_kind, read_first
		? RESOURCE_X_ACQUIRE_SOURCE_LOCAL_CURRENT : RESOURCE_X_ACQUIRE_SOURCE_DISK);
	UT_ASSERT(memcmp(disk_shared.data, disk_bytes.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	LWLockAcquire(entry->content_lock, LW_EXCLUSIVE);
	NM_REQUIRE(cluster_bufmgr_pcm_x_writer_activate(entry, true));
	cluster_bufmgr_pcm_x_writer_mark_releasing(entry); LWLockRelease(entry->content_lock);
	cluster_bufmgr_pcm_x_writer_release(entry);
	UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(&request_buf) == NULL);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.writer_activation_token), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.resource_x_activation_generation), 0);
	NM_REQUIRE(lm_settle());
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), previous_generation + 1);
	UT_ASSERT_EQ(ordinary_sends, 1);
	complete_idle(); return !ut_current_failed;
}
#define LM_READ(name, mode) UT_TEST(name) { UT_ASSERT(lm_read(mode)); }
#define LM_WRITE(name, mode, prior) UT_TEST(name) { UT_ASSERT(lm_writer(mode, prior)); }
LM_READ(test_local_master_first_fill_disk, 0)
LM_READ(test_local_master_first_fill_late_grant_callback, 1)
LM_READ(test_local_master_first_fill_duplicate_frames, 2)
LM_READ(test_local_master_first_fill_failed_notice_send, 3)
LM_WRITE(test_local_master_N_X_disk, 0, false)
LM_WRITE(test_local_master_N_X_late_grant_callback, 1, false)
LM_WRITE(test_local_master_N_X_duplicate_frames, 2, false)
LM_WRITE(test_local_master_N_X_failed_notice_send, 3, false)
LM_WRITE(test_local_master_S_X_current, 0, true)
LM_WRITE(test_local_master_S_X_late_grant_callback, 1, true)
LM_WRITE(test_local_master_S_X_duplicate_frames, 2, true)
LM_WRITE(test_local_master_S_X_failed_notice_send, 3, true)
int main(void)
{
	UT_PLAN(12);
	UT_RUN(test_local_master_first_fill_disk);
	UT_RUN(test_local_master_first_fill_late_grant_callback);
	UT_RUN(test_local_master_first_fill_duplicate_frames);
	UT_RUN(test_local_master_first_fill_failed_notice_send);
	UT_RUN(test_local_master_N_X_disk);
	UT_RUN(test_local_master_N_X_late_grant_callback);
	UT_RUN(test_local_master_N_X_duplicate_frames);
	UT_RUN(test_local_master_N_X_failed_notice_send);
	UT_RUN(test_local_master_S_X_current);
	UT_RUN(test_local_master_S_X_late_grant_callback);
	UT_RUN(test_local_master_S_X_duplicate_frames);
	UT_RUN(test_local_master_S_X_failed_notice_send);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
