/* R0/M0 share their actual PCM entry, with no local current. Independent H1
 * first acquires S or dirty X through an ordinary caller, then serves R0.
 * Only authentication, sockets and OS/storage scheduling are controlled.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_native_remote_x_fixture.inc"
#include "test_cluster_native_local_master_read_fixture.inc"

static int ls_fd, ls_mode;
static ResourceXDecodedFrame ls_ack;
static unsigned ls_frames;

static bool ls_holder_ack(void)
{
	ResourceXMasterSnapshot snapshot;
	auth_peer_boot = 101;
	NM_REQUIRE(cluster_pcm_lock_resource_x_blocked_to_n_exact(&ls_ack, 1, &snapshot)
		== RESOURCE_X_APPLY_APPLIED);
	return true;
}
static bool ls_deliver_actual(void)
{
	ResourceXDecodedFrame decoded, block;
	ResourceXIntentSlot ticket = request_ticket(&nc_request), staged;
	ResourceXRequesterJoinSnapshot joined; ClusterICEnvelope env = {0};
	PGAlignedBlock before; memcpy(before.data, disk_shared.data, BLCKSZ);
	NM_REQUIRE(nc_request.common.logical_assertion.requester_node == 0);
	NM_REQUIRE(nc_request.body.acquire_intent.requester_incarnation == 31);
	NM_REQUIRE(lm_wire(RESOURCE_X_MSG_ASSERT_X, &nc_request, &decoded));
	cluster_lms_outbound_resource_x_send_complete(&ticket, true);
	auth_peer_boot = 31; NM_REQUIRE(lm_admit(&decoded));
	block = publish_decode(lm_master.state->block_intents[1].payload, true);
	UT_ASSERT_EQ(block.body.acquire_decision.source_kind, RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
	UT_ASSERT_EQ(block.body.acquire_decision.source_node, 1);
	UT_ASSERT_EQ(block.body.acquire_decision.directory_mode, nt_source_mode);
	UT_ASSERT_EQ(block.body.acquire_decision.required_holders_bitmap,
		nt_target_mode == PCM_STATE_X || nt_source_mode == PCM_STATE_X ? 2 : 0);
	NM_REQUIRE(nc_stage(&lm_master.state->block_intents[1].slot, &staged));
	NM_REQUIRE(nm_send(ls_fd, RESOURCE_X_MSG_BLOCK_TO_N, &block)); ls_frames++;
	NM_REQUIRE(nc_callback(&staged));
	NM_REQUIRE(nm_receive(ls_fd, RESOURCE_X_MSG_IMAGE_OR_GRANT, &nc_image)); ls_frames++;
	prepared_episode = nc_request.body.acquire_intent;
	input_observation = ordinary_active->observation;
	env.msg_type = RESOURCE_X_MSG_IMAGE_OR_GRANT; env.source_node_id = 1; env.dest_node_id = 0;
	env.epoch = 17; env.payload_length = nc_image.payload_bytes; auth_peer_boot = 101;
	NM_REQUIRE(gcs_block_resource_x_requester_join_ingress(&env, &nc_image, 61, &joined)
		== RESOURCE_X_APPLY_APPLIED);
	if (ls_mode == 3)
		NM_REQUIRE(gcs_block_resource_x_requester_join_ingress(&env, &nc_image, 61, &joined)
			== RESOURCE_X_APPLY_DUPLICATE);
	NM_REQUIRE(nm_receive(ls_fd, RESOURCE_X_MSG_BLOCKED_TO_N, &ls_ack)); ls_frames++;
	if (ls_mode != 2) NM_REQUIRE(ls_holder_ack());
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), prepared_episode.base_generation);
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT_EQ(complete_io_calls, 0); UT_ASSERT_EQ(complete_broadcasts, 0);
	UT_ASSERT(memcmp(before.data, disk_shared.data, BLCKSZ) == 0);
	return !ut_current_failed;
}
static void ls_deliver(void) { if (!ls_deliver_actual()) pg_re_throw(); }

static bool ls_settle(void)
{
	ClusterPcmResourceXMasterState *state = NULL;
	ClusterPcmResourceXMasterRequest empty = {0};
	ResourceXDecodedFrame done, decoded, release, retired, consumed;
	ResourceXIntentSlot staged, cleanup; ResourceXMasterSnapshot snapshot;
	ClusterICEnvelope env = {0};
	NM_REQUIRE(nc_requester_state(&state)); NM_REQUIRE(state == lm_master.state);
	NM_REQUIRE(nc_decode(state->requester_settlement_intent.payload,
		RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &done));
	NM_REQUIRE(nc_stage(&state->requester_settlement_intent.slot, &staged));
	NM_REQUIRE(nc_callback(&staged));
	NM_REQUIRE(lm_wire(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done, &decoded));
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE;
	env.source_node_id = env.dest_node_id = 0; env.epoch = 17;
	env.payload_length = decoded.payload_bytes; auth_peer_boot = 31;
	NM_REQUIRE(gcs_block_resource_x_acquire_done_ingress(&env, &decoded, 1) == RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(state->deferred_settlement.present, 4);
	if (ls_mode == 2) {
		NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_source_done_snapshot_exact(&lm_master.work, &release)
			== RESOURCE_X_APPLY_LOCAL_WAIT);
		NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&lm_master.work)
			== RESOURCE_X_APPLY_LOCAL_WAIT);
		UT_ASSERT(state->requests[0].phase != RESOURCE_X_MASTER_NONE);
		UT_ASSERT_EQ(state->source_settlement.intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
		NM_REQUIRE(ls_holder_ack());
	}
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&lm_master.work)
		== RESOURCE_X_APPLY_LOCAL_WAIT);
	NM_REQUIRE(nc_decode(state->source_settlement.intent.payload, RESOURCE_X_MSG_BLOCK_TO_N,
		RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &release));
	NM_REQUIRE(nc_stage(&state->source_settlement.intent.slot, &cleanup));
	NM_REQUIRE(nm_send(ls_fd, RESOURCE_X_MSG_BLOCK_TO_N, &release)); ls_frames++;
	NM_REQUIRE(nm_receive(ls_fd, RESOURCE_X_MSG_BLOCKED_TO_N, &retired)); ls_frames++;
	auth_peer_boot = 101;
	NM_REQUIRE(cluster_pcm_lock_resource_x_source_settlement_ack_exact(&retired, 1, &snapshot)
		== RESOURCE_X_APPLY_APPLIED);
	/* A received retirement ACK cannot discard our in-flight physical send. */
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&lm_master.work)
		== RESOURCE_X_APPLY_LOCAL_WAIT);
	NM_REQUIRE(nc_callback(&cleanup));
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&lm_master.work)
		== RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(nc_decode(state->grant_intent.payload, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &consumed));
	NM_REQUIRE(lm_wire(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &consumed, &decoded));
	NM_REQUIRE(lm_receive(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &decoded) == RESOURCE_X_APPLY_APPLIED);
	if (ls_mode == 3)
		NM_REQUIRE(lm_receive(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &decoded) == RESOURCE_X_APPLY_DUPLICATE);
	NM_REQUIRE(nm_master_callback(&state->grant_intent.slot, true));
	UT_ASSERT(memcmp(&state->requests[0], &empty, sizeof(empty)) == 0);
	UT_ASSERT_EQ(state->deferred_settlement.present, 0);
	UT_ASSERT_EQ(state->source_settlement.state, RESOURCE_X_SOURCE_SETTLEMENT_ACKED);
	UT_ASSERT_EQ(state->source_settlement.intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(state->bootstrap_receipts[0].highest_attempt_floor, nc_request.common.assertion_sequence);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.pending, 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent,
		nt_target_mode == PCM_STATE_S ? 0 : 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->s_holders_bitmap),
		nt_target_mode == PCM_STATE_S ? 3 : 0);
	UT_ASSERT_EQ(((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->x_holder_node,
		nt_target_mode == PCM_STATE_X ? 0 : -1);
	return !ut_current_failed;
}
static bool ls_parent(int fd, int mode)
{
	ClusterBufmgrFirstFill read; ClusterPcmXWriterLedgerEntry *entry = NULL;
	MemSet(cluster_bufmgr_pcm_x_writer_ledger, 0, sizeof(cluster_bufmgr_pcm_x_writer_ledger));
	if (nt_target_mode == PCM_STATE_S) ordinary_start(&read, 2);
	else rx_writer_start(1, 4);
	/* Seed only H1's directory/current ownership through original messages.
	 * Keep R0's native N descriptor and original IO owner in this process. */
	NM_REQUIRE(nm_master(fd, 0));
	cluster_node_id = 0; ut_master_session = auth_peer_boot = 31;
	MyBackendType = B_BACKEND; cluster_enabled = cluster_shared_config = true;
	ordinary_connection = ready_connection_generation = 1;
	nc_source_node = 1; nc_source_boot = 101; auth_observation_hook = nc_auth_peer;
	ls_fd = fd; ls_mode = mode; ls_frames = 0; ordinary_delivery_transport = ls_deliver;
	if (nt_target_mode == PCM_STATE_S) NM_REQUIRE(ordinary_run(&read));
	else { entry = rx_prepare(NULL); NM_REQUIRE(entry != NULL); }
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(complete_io_calls, 1); UT_ASSERT_EQ(complete_broadcasts, 1);
	if (nt_target_mode == PCM_STATE_S) UT_ASSERT_EQ(complete_commits, 1);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), prepared_episode.base_generation + 1);
	UT_ASSERT_EQ(request_buf.pcm_state, nt_target_mode);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR), BM_VALID);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT(memcmp(disk_shared.data, nc_image.body.acquire_image.original_image.page_bytes, BLCKSZ) == 0);
	if (entry != NULL) NM_REQUIRE(xt_use_writer(entry, false));
	NM_REQUIRE(ls_settle());
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(complete_io_calls, 1); UT_ASSERT_EQ(complete_broadcasts, 1);
	if (nt_target_mode == PCM_STATE_S) UT_ASSERT_EQ(complete_commits, 1);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), prepared_episode.base_generation + 1);
	UT_ASSERT_EQ(ls_frames, 5); UT_ASSERT_EQ(fake_lwlock_depth, 0);
	complete_idle(); return !ut_current_failed;
}
static void ls_run(PcmState source, PcmState target, int mode)
{
	int pair[2]; pid_t child; bool ok;
	ready_connection_generation = 61; auth_observation_hook = NULL;
	nt_source_mode = source; nt_target_mode = target;
	nt_holder_node = 1; nt_requester_node = 0; nt_keep_seed_bytes = true;
	nt_seed_holder = source == PCM_STATE_X ? xt_seed : nm_parent_as;
	nt_source_finished = source == PCM_STATE_X ? xt_source_finished : NULL;
	int result = socketpair(AF_UNIX, SOCK_STREAM, 0, pair);
	UT_ASSERT_EQ(result, 0); if (result != 0) return;
#ifdef SO_NOSIGPIPE
	int one = 1;
	setsockopt(pair[0], SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
	setsockopt(pair[1], SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
	fflush(NULL); child = fork(); UT_ASSERT(child >= 0);
	if (child == 0) {
		close(pair[0]); ok = nt_holder(pair[1], pair[1], mode); close(pair[1]);
		fflush(NULL); _exit(ok && !ut_current_failed ? 0 : 1);
	}
	close(pair[1]);
	if (child >= 0) { ok = ls_parent(pair[0], mode); UT_ASSERT(ok); }
	close(pair[0]); if (child >= 0) nm_wait_child(child);
}

#define LS_CASE(name, source, target, mode) UT_TEST(name) { ls_run(source, target, mode); }
LS_CASE(test_local_S_source_to_first_fill, PCM_STATE_S, PCM_STATE_S, 0)
LS_CASE(test_local_S_source_first_fill_DONE_before_ACK, PCM_STATE_S, PCM_STATE_S, 2)
LS_CASE(test_local_S_source_first_fill_duplicate, PCM_STATE_S, PCM_STATE_S, 3)
LS_CASE(test_local_S_source_first_fill_late_callback, PCM_STATE_S, PCM_STATE_S, 4)
LS_CASE(test_local_S_source_first_fill_busy, PCM_STATE_S, PCM_STATE_S, 5)
LS_CASE(test_local_X_source_to_first_fill, PCM_STATE_X, PCM_STATE_S, 0)
LS_CASE(test_local_X_source_first_fill_DONE_before_ACK, PCM_STATE_X, PCM_STATE_S, 2)
LS_CASE(test_local_X_source_first_fill_duplicate, PCM_STATE_X, PCM_STATE_S, 3)
LS_CASE(test_local_X_source_first_fill_late_callback, PCM_STATE_X, PCM_STATE_S, 4)
LS_CASE(test_local_X_source_first_fill_busy, PCM_STATE_X, PCM_STATE_S, 5)
LS_CASE(test_local_S_source_to_resident_X, PCM_STATE_S, PCM_STATE_X, 0)
LS_CASE(test_local_S_source_resident_X_DONE_before_ACK, PCM_STATE_S, PCM_STATE_X, 2)
LS_CASE(test_local_S_source_resident_X_duplicate, PCM_STATE_S, PCM_STATE_X, 3)
LS_CASE(test_local_S_source_resident_X_late_callback, PCM_STATE_S, PCM_STATE_X, 4)
LS_CASE(test_local_S_source_resident_X_busy, PCM_STATE_S, PCM_STATE_X, 5)
LS_CASE(test_local_X_source_to_resident_X, PCM_STATE_X, PCM_STATE_X, 0)
LS_CASE(test_local_X_source_resident_X_DONE_before_ACK, PCM_STATE_X, PCM_STATE_X, 2)
LS_CASE(test_local_X_source_resident_X_duplicate, PCM_STATE_X, PCM_STATE_X, 3)
LS_CASE(test_local_X_source_resident_X_late_callback, PCM_STATE_X, PCM_STATE_X, 4)
LS_CASE(test_local_X_source_resident_X_busy, PCM_STATE_X, PCM_STATE_X, 5)
#undef LS_CASE
int main(void)
{
	UT_PLAN(20);
	UT_RUN(test_local_S_source_to_first_fill);
	UT_RUN(test_local_S_source_first_fill_DONE_before_ACK);
	UT_RUN(test_local_S_source_first_fill_duplicate);
	UT_RUN(test_local_S_source_first_fill_late_callback);
	UT_RUN(test_local_S_source_first_fill_busy);
	UT_RUN(test_local_X_source_to_first_fill);
	UT_RUN(test_local_X_source_first_fill_DONE_before_ACK);
	UT_RUN(test_local_X_source_first_fill_duplicate);
	UT_RUN(test_local_X_source_first_fill_late_callback);
	UT_RUN(test_local_X_source_first_fill_busy);
	UT_RUN(test_local_S_source_to_resident_X);
	UT_RUN(test_local_S_source_resident_X_DONE_before_ACK);
	UT_RUN(test_local_S_source_resident_X_duplicate);
	UT_RUN(test_local_S_source_resident_X_late_callback);
	UT_RUN(test_local_S_source_resident_X_busy);
	UT_RUN(test_local_X_source_to_resident_X);
	UT_RUN(test_local_X_source_resident_X_DONE_before_ACK);
	UT_RUN(test_local_X_source_resident_X_duplicate);
	UT_RUN(test_local_X_source_resident_X_late_callback);
	UT_RUN(test_local_X_source_resident_X_busy);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
