/* R0/M0 share one real current/PCM entry; independent H1/H2/H3 acquire S
 * through ordinary reads before the original local writer upgrades to X.
 * Only peer identity, socket scheduling and OS/storage leaves are controlled.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_native_multi_holder_fixture.inc"
static void lh_seed_start(ClusterBufmgrFirstFill *read, int profile)
{
	ordinary_start(read, profile);
	/* A distinct disk leaf for M's first real read, before any grant/IO.
	 * Peers retain their own different old disk bytes in other processes. */
	disk_bytes.data[BLCKSZ - 16] = 0x7a;
	((PageHeader)disk_bytes.data)->pd_checksum = pg_checksum_page(disk_bytes.data, request_buf.tag.blockNum);
	disk_store();
}
#define ordinary_start lh_seed_start
#include "test_cluster_native_local_master_read_fixture.inc"
#undef ordinary_start

static int lh_fds[3], lh_count, lh_mode;
static ResourceXDecodedFrame lh_acks[3];
static unsigned lh_frames;

static void lh_auth(int32 node, unsigned sample)
{
	(void)sample; UT_ASSERT(node >= 0 && node <= lh_count);
	auth_peer_boot = node == 0 ? 31 : 101;
}

static bool lh_accept(const ResourceXDecodedFrame *frame, int node,
	ResourceXRequesterJoinSnapshot *joined)
{
	ClusterICEnvelope env = {0};
	env.msg_type = frame->kind == RESOURCE_X_WIRE_BLOCKED_TO_N
		? RESOURCE_X_MSG_BLOCKED_TO_N : RESOURCE_X_MSG_IMAGE_OR_GRANT;
	env.source_node_id = node; env.dest_node_id = 0; env.epoch = 17;
	env.payload_length = frame->payload_bytes; auth_peer_boot = 101;
	NM_REQUIRE(gcs_block_resource_x_requester_join_ingress(&env, frame, 61, joined)
		== RESOURCE_X_APPLY_APPLIED);
	return true;
}
static bool lh_incomplete(void)
{
	ResourceXDecodedFrame grant, image; ResourceXRequesterJoinSnapshot joined;
	ResourceXDeliveryClaim claim = {0}; ClusterPcmOwnEntry before = request_own;
	char bytes[BLCKSZ]; memcpy(bytes, disk_shared.data, BLCKSZ);
	NM_REQUIRE(cluster_pcm_lock_resource_x_requester_acquire_claim_exact(
		&input_observation, &prepared_episode, &grant, &image, &joined, &claim)
		== RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(&before, &request_own, sizeof(before)) == 0);
	UT_ASSERT(memcmp(bytes, disk_shared.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(complete_io_calls, 0);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	return !ut_current_failed;
}
static bool lh_deliver_actual(void)
{
	ResourceXDecodedFrame request, decoded, block, witness;
	ResourceXRequesterJoinSnapshot joined; ResourceXMasterSnapshot snapshot;
	ResourceXIntentSlot ticket = request_ticket(&request), staged;
	uint32 required = lh_count == 1 ? 2 : lh_count == 2 ? 6 : 14;
	NM_REQUIRE(request.common.logical_assertion.requester_node == 0);
	NM_REQUIRE(request.common.observed_mode == PCM_STATE_S);
	NM_REQUIRE(request.common.target_mode == PCM_STATE_X);
	NM_REQUIRE(lm_wire(RESOURCE_X_MSG_ASSERT_X, &request, &decoded));
	cluster_lms_outbound_resource_x_send_complete(&ticket, true);
	auth_peer_boot = 31; NM_REQUIRE(lm_admit(&decoded));
	nm_grant = publish_decode(lm_master.state->grant_intent.payload, false);
	UT_ASSERT_EQ(nm_grant.body.acquire_decision.source_kind, RESOURCE_X_ACQUIRE_SOURCE_LOCAL_CURRENT);
	UT_ASSERT_EQ(nm_grant.body.acquire_decision.source_node, 0);
	UT_ASSERT_EQ(nm_grant.body.acquire_decision.required_holders_bitmap, required);
	UT_ASSERT_EQ(nm_grant.body.acquire_decision.holders_bitmap, required | 1);
	prepared_episode = request.body.acquire_intent;
	input_observation = ordinary_active->observation; input_grant = nm_grant;
	NM_REQUIRE(nm_master_callback(&lm_master.state->grant_intent.slot, true));
	if (lh_mode != 1) {
		NM_REQUIRE(lm_wire(RESOURCE_X_MSG_IMAGE_OR_GRANT, &nm_grant, &decoded));
		NM_REQUIRE(lm_receive(RESOURCE_X_MSG_IMAGE_OR_GRANT, &decoded) == RESOURCE_X_APPLY_APPLIED);
		NM_REQUIRE(lh_incomplete());
	}
	for (int n = 0; n < lh_count; n++) {
		block = publish_decode(lm_master.state->block_intents[n + 1].payload, true);
		NM_REQUIRE(nc_stage(&lm_master.state->block_intents[n + 1].slot, &staged));
		NM_REQUIRE(nm_send(lh_fds[n], RESOURCE_X_MSG_BLOCK_TO_N, &block)); lh_frames++;
		NM_REQUIRE(nc_callback(&staged));
	}
	for (int n = 0; n < lh_count; n++) {
		NM_REQUIRE(nm_receive(lh_fds[n], RESOURCE_X_MSG_BLOCKED_TO_N, &witness)); lh_frames++;
		NM_REQUIRE(lh_accept(&witness, n + 1, &joined));
		if (n + 1 < lh_count) {
			UT_ASSERT(!(joined.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE));
			NM_REQUIRE(lh_incomplete());
		} else UT_ASSERT(joined.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE);
		NM_REQUIRE(nm_receive(lh_fds[n], RESOURCE_X_MSG_BLOCKED_TO_N, &lh_acks[n])); lh_frames++;
		if (lh_mode != 3 || n + 1 < lh_count) {
			auth_peer_boot = 101;
			NM_REQUIRE(cluster_pcm_lock_resource_x_blocked_to_n_exact(&lh_acks[n], n + 1, &snapshot)
				== RESOURCE_X_APPLY_APPLIED);
		}
		UT_ASSERT_EQ(complete_commits, 0);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), prepared_episode.base_generation);
	}
	if (lh_mode == 1) {
		NM_REQUIRE(lm_wire(RESOURCE_X_MSG_IMAGE_OR_GRANT, &nm_grant, &decoded));
		/* Witnesses already carry the complete master A; a later standalone
		 * copy cannot require another completion or become a second grant. */
		NM_REQUIRE(lm_receive(RESOURCE_X_MSG_IMAGE_OR_GRANT, &decoded) == RESOURCE_X_APPLY_DUPLICATE);
	}
	return !ut_current_failed;
}
static void lh_deliver(void) { if (!lh_deliver_actual()) pg_re_throw(); }

static bool lh_settle(void)
{
	ClusterPcmResourceXMasterState *state = NULL; ClusterPcmResourceXMasterRequest empty = {0};
	ResourceXDecodedFrame done, decoded, release, retired, consumed;
	ResourceXIntentSlot staged; ResourceXMasterSnapshot snapshot; ClusterICEnvelope env = {0};
	NM_REQUIRE(nm_requester_notice(&state)); NM_REQUIRE(state == lm_master.state);
	NM_REQUIRE(nc_decode(state->requester_settlement_intent.payload, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &done));
	NM_REQUIRE(nm_master_callback(&state->requester_settlement_intent.slot, true));
	NM_REQUIRE(lm_wire(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done, &decoded));
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE; env.source_node_id = env.dest_node_id = 0;
	env.epoch = 17; env.payload_length = decoded.payload_bytes; auth_peer_boot = 31;
	NM_REQUIRE(gcs_block_resource_x_acquire_done_ingress(&env, &decoded, 1) == RESOURCE_X_APPLY_APPLIED);
	if (lh_mode == 3) {
		NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&lm_master.work) == RESOURCE_X_APPLY_LOCAL_WAIT);
		UT_ASSERT_EQ(state->acquire_ack_connections[lh_count], 0);
		UT_ASSERT(state->requests[0].phase != RESOURCE_X_MASTER_NONE);
		UT_ASSERT_EQ(state->source_settlement.intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
		auth_peer_boot = 101;
		NM_REQUIRE(cluster_pcm_lock_resource_x_blocked_to_n_exact(&lh_acks[lh_count - 1], lh_count, &snapshot)
			== RESOURCE_X_APPLY_APPLIED);
	}
	for (int n = 0; n < lh_count; n++) {
		NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&lm_master.work) == RESOURCE_X_APPLY_LOCAL_WAIT);
		NM_REQUIRE(nc_decode(state->source_settlement.intent.payload, RESOURCE_X_MSG_BLOCK_TO_N,
			RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &release));
		UT_ASSERT_EQ(release.common.action_node, n + 1);
		NM_REQUIRE(nc_stage(&state->source_settlement.intent.slot, &staged));
		NM_REQUIRE(nm_send(lh_fds[n], RESOURCE_X_MSG_BLOCK_TO_N, &release)); lh_frames++;
		NM_REQUIRE(nm_receive(lh_fds[n], RESOURCE_X_MSG_BLOCKED_TO_N, &retired)); lh_frames++;
		auth_peer_boot = 101;
		NM_REQUIRE(cluster_pcm_lock_resource_x_source_settlement_ack_exact(&retired, n + 1, &snapshot)
			== RESOURCE_X_APPLY_APPLIED);
		NM_REQUIRE(nc_callback(&staged));
	}
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&lm_master.work) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(nc_decode(state->grant_intent.payload, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &consumed));
	NM_REQUIRE(lm_wire(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &consumed, &decoded));
	NM_REQUIRE(lm_receive(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &decoded) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(nm_master_callback(&state->grant_intent.slot, true));
	UT_ASSERT(memcmp(&state->requests[0], &empty, sizeof(empty)) == 0);
	UT_ASSERT_EQ(state->deferred_settlement.present, 0);
	UT_ASSERT_EQ(state->acquire_settled_holders_bitmap, 0);
	UT_ASSERT_EQ(state->source_settlement.intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(state->pi_carrier.pi_refs, 0); UT_ASSERT_EQ(state->pi_carrier.transport_refs, 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.pending, 0);
	UT_ASSERT_EQ(((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->x_holder_node, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->s_holders_bitmap), 0);
	return !ut_current_failed;
}
static bool lh_parent(void)
{
	ClusterPcmXWriterLedgerEntry *entry; char previous[BLCKSZ]; uint64 generation;
	/* The local holder's grant and attempt floor must come from its own
	 * completed REQUEST, rather than the unrelated incumbent seed helper. */
	NM_REQUIRE(lm_read(0)); ready_connection_generation = 61;
	auth_observation_hook = lh_auth;
	for (int n = 0; n < lh_count; n++) {
		NM_REQUIRE(nc_master_service(lh_fds[n], 0, true));
	}
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	generation = cluster_pcm_own_gen_get(0); memcpy(previous, disk_shared.data, BLCKSZ);
	/* Only reuse the exhausted transport leaf. Directory/native state from
	 * the real seed reads remains intact, including all remote grants. */
	memset(&request_shared, 0, sizeof(request_shared));
	pg_atomic_init_u64(&request_shared.resource_x_transport_mutation_sequence, 1);
	ordinary_sends = ordinary_pauses = ordinary_leaves = ordinary_peer_samples = ordinary_current_calls = 0;
	disk_reads = complete_commits = complete_io_calls = complete_broadcasts = 0;
	ordinary_active = NULL; ordinary_connection = ready_connection_generation = 1;
	cluster_node_id = 0; ut_master_session = 31; auth_peer_boot = 31;
	wc_object_enabled = false; wc_object_valid = true; wc_r4 = 77; wc_session_id = 31;
	wc_master = 0; wc_path = RESOURCE_X_WRITER_TARGET;
	wc_legacy = wc_failures = wc_waits = wc_object_checks = wc_unavailable = wc_pauses = 0;
	wc_cancel_pause = rx_barrier = rx_cancel_wait = false; MyBackendType = B_BACKEND;
	auth_observation_hook = lh_auth; ordinary_delivery_transport = lh_deliver; lh_frames = 0;
	entry = rx_prepare(NULL); NM_REQUIRE(entry != NULL);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, 0);
	/* Resident S->X commits through the original handoff, not the input-IO
	 * commit helper. Its single native publication is generation+1 below. */
	UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(complete_io_calls, 0); UT_ASSERT_EQ(complete_broadcasts, 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), generation + 1); UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X);
	UT_ASSERT(memcmp(previous, disk_shared.data, BLCKSZ) == 0);
	LWLockAcquire(entry->content_lock, LW_EXCLUSIVE);
	NM_REQUIRE(cluster_bufmgr_pcm_x_writer_activate(entry, true));
	cluster_bufmgr_pcm_x_writer_mark_releasing(entry); LWLockRelease(entry->content_lock);
	cluster_bufmgr_pcm_x_writer_release(entry);
	UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(&request_buf) == NULL);
	NM_REQUIRE(lh_settle());
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.writer_activation_token), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.resource_x_activation_generation), 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), generation + 1);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(lh_frames, 5 * lh_count);
	UT_ASSERT(memcmp(previous, disk_shared.data, BLCKSZ) == 0);
	printf("# local master holders=%d mode=%d remote_frames=%u REQUEST=%u generation=%llu->%llu\n",
		lh_count, lh_mode, lh_frames, ordinary_sends,
		(unsigned long long)generation, (unsigned long long)cluster_pcm_own_gen_get(0));
	holder_no_debt(); complete_idle(); return !ut_current_failed;
}
static void lh_run(int count, int mode)
{
	int pairs[3][2]; pid_t children[3] = {-1, -1, -1}; bool ok;
	lh_count = count; lh_mode = mode;
	ready_connection_generation = 61; auth_observation_hook = NULL;
	for (int n = 0; n < count; n++) {
		int rc = socketpair(AF_UNIX, SOCK_STREAM, 0, pairs[n]); UT_ASSERT_EQ(rc, 0);
		if (rc != 0) { for (int k = 0; k < n; k++) { close(pairs[k][0]); close(pairs[k][1]); } return; }
#ifdef SO_NOSIGPIPE
		int one = 1;
		for (int k = 0; k < 2; k++) setsockopt(pairs[n][k], SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
	}
	for (int n = 0; n < count; n++) {
		fflush(NULL); children[n] = fork(); UT_ASSERT(children[n] >= 0);
		if (children[n] == 0) {
			for (int j = 0; j < count; j++) for (int k = 0; k < 2; k++)
				if (j != n || k != 1) close(pairs[j][k]);
			ok = mh_holder(pairs[n][1], pairs[n][1], mode, n + 1);
			close(pairs[n][1]); fflush(NULL); _exit(ok && !ut_current_failed ? 0 : 1);
		}
		if (children[n] < 0) {
			for (int j = 0; j < count; j++) for (int k = 0; k < 2; k++) close(pairs[j][k]);
			for (int j = 0; j < n; j++) nm_wait_child(children[j]); return;
		}
	}
	for (int n = 0; n < count; n++) { close(pairs[n][1]); lh_fds[n] = pairs[n][0]; }
	ok = lh_parent(); UT_ASSERT(ok);
	for (int n = 0; n < count; n++) close(pairs[n][0]);
	for (int n = 0; n < count; n++) nm_wait_child(children[n]);
}
#define LH_CASE(name, count, mode) UT_TEST(name) { lh_run(count, mode); }
LH_CASE(test_local_one_holder_grant_first, 1, 0)
LH_CASE(test_local_one_holder_witness_first, 1, 1)
LH_CASE(test_local_one_holder_late_send_callback, 1, 2)
LH_CASE(test_local_one_holder_DONE_before_master_ACK, 1, 3)
LH_CASE(test_local_one_holder_busy_resume, 1, 4)
LH_CASE(test_local_two_holders_grant_first, 2, 0)
LH_CASE(test_local_two_holders_witnesses_first, 2, 1)
LH_CASE(test_local_two_holders_late_send_callback, 2, 2)
LH_CASE(test_local_two_holders_DONE_before_master_ACK, 2, 3)
LH_CASE(test_local_two_holders_busy_resume, 2, 4)
LH_CASE(test_local_three_holders_grant_first, 3, 0)
LH_CASE(test_local_three_holders_witnesses_first, 3, 1)
LH_CASE(test_local_three_holders_late_send_callback, 3, 2)
LH_CASE(test_local_three_holders_DONE_before_master_ACK, 3, 3)
LH_CASE(test_local_three_holders_busy_resume, 3, 4)
int main(void)
{
	UT_PLAN(15);
	UT_RUN(test_local_one_holder_grant_first);
	UT_RUN(test_local_one_holder_witness_first);
	UT_RUN(test_local_one_holder_late_send_callback);
	UT_RUN(test_local_one_holder_DONE_before_master_ACK);
	UT_RUN(test_local_one_holder_busy_resume);
	UT_RUN(test_local_two_holders_grant_first);
	UT_RUN(test_local_two_holders_witnesses_first);
	UT_RUN(test_local_two_holders_late_send_callback);
	UT_RUN(test_local_two_holders_DONE_before_master_ACK);
	UT_RUN(test_local_two_holders_busy_resume);
	UT_RUN(test_local_three_holders_grant_first);
	UT_RUN(test_local_three_holders_witnesses_first);
	UT_RUN(test_local_three_holders_late_send_callback);
	UT_RUN(test_local_three_holders_DONE_before_master_ACK);
	UT_RUN(test_local_three_holders_busy_resume);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
