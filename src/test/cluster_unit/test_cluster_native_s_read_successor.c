/* S is installed by an ordinary read, revoked for a real peer writer, then
 * reacquired on the same native descriptor. M, R and H keep separate state.
 * Only sockets, authentication, storage and OS scheduling are test leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_native_remote_x_fixture.inc"
#define NATIVE_RESIDENT_LOCK_REUSE_FIXTURE
#include "test_cluster_native_resident_lock_fixture.inc"
#undef NATIVE_RESIDENT_LOCK_REUSE_FIXTURE

static ResourceXDecodedFrame ss_old_consumed;
static ResourceXIntentSlot ss_old_callback;
static void ss_capture(const ResourceXIntentSlot *slot, const ResourceXDecodedFrame *receipt)
{
	ss_old_callback = *slot; ss_old_consumed = *receipt;
}
static bool ss_seed_kept(int fd, int mode)
{
	(void)fd; (void)mode;
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	return true;
}
static bool ss_holder_kept(int fd, int mode, int node)
{
	(void)fd; (void)mode;
	UT_ASSERT_EQ(cluster_node_id, node);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X);
	return true;
}
static void ss_roles(bool second)
{
	nt_holder_node = second ? 1 : 2; nt_requester_node = second ? 2 : 1;
	nt_source_mode = second ? PCM_STATE_X : PCM_STATE_S;
	nt_target_mode = second ? PCM_STATE_S : PCM_STATE_X;
	nt_seed_master = second ? ss_seed_kept : nm_master;
	nt_seed_holder = second ? ss_holder_kept : nm_parent_as;
	nt_source_finished = second ? xt_source_finished : NULL;
	nt_keep_seed_bytes = true;
}
static bool ss_accept(const ResourceXDecodedFrame *frame, ResourceXApplyResult expected)
{
	ClusterICEnvelope env = {0}; ResourceXRequesterJoinSnapshot joined;
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE;
	env.source_node_id = 0; env.dest_node_id = cluster_node_id;
	env.epoch = 17; env.payload_length = frame->payload_bytes; auth_peer_boot = 31;
	NM_REQUIRE(gcs_block_resource_x_requester_join_ingress(&env, frame, 61, &joined) == expected);
	return true;
}
static bool ss_reader(int master, int writer, int mode)
{
	PGIOAlignedBlock old_bytes;
	ClusterPcmOwnSnapshot base;
	ClusterPcmResourceXMasterState *state = NULL;
	ClusterPcmResourceXBootstrapRound before;
	ResourceXDecodedFrame done, consumed; ResourceXIntentSlot staged;
	uint64 floor = request_entry()->resource_x_bootstrap_round.highest_attempt_floor;
	NM_REQUIRE(cluster_bufmgr_pcm_own_snapshot(&request_buf, &base) == CLUSTER_PCM_OWN_OK);
	/* This N is the result of the preceding real FORWARD, native invalidate,
	 * holder ACK and source cleanup. Never call the lower S RELEASE tail. */
	UT_ASSERT_EQ(base.pcm_state, PCM_STATE_N);
	memcpy(old_bytes.data, disk_shared.data, BLCKSZ);
	memset(&request_shared, 0, sizeof(request_shared));
	pg_atomic_init_u64(&request_shared.resource_x_transport_mutation_sequence, 1);
	ordinary_sends = ordinary_pauses = ordinary_leaves = 0;
	ordinary_current_calls = ordinary_peer_samples = 0;
	disk_reads = complete_commits = complete_io_calls = complete_broadcasts = 0;
	rl_waits = rl_share_locks = 0; rl_barrier = rl_cancel_wait = false;
	rl_on_wait = rl_on_share = rl_after_deliver = NULL;
	ordinary_active = NULL; nc_fd = master; nc_image_fd = writer; nc_mode = mode;
	nc_source_node = 1; nc_source_boot = 101;
	auth_observation_hook = nc_auth_peer; ordinary_delivery_transport = xt_deliver;
	MyBackendType = B_BACKEND;
	cluster_enabled = cluster_shared_config = cluster_gcs_block_local_cache = true;
	NM_REQUIRE(rl_run(NULL));
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), base.generation + 1);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT(memcmp(disk_shared.data, nc_image.body.acquire_image.original_image.page_bytes, BLCKSZ) == 0);
	UT_ASSERT(memcmp(old_bytes.data, disk_shared.data, BLCKSZ) != 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.highest_attempt_floor, floor + 1);
	rl_actual_unlock(&request_buf);
	NM_REQUIRE(nc_requester_state(&state));
	NM_REQUIRE(nc_decode(state->requester_settlement_intent.payload,
		RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &done));
	NM_REQUIRE(nc_stage(&state->requester_settlement_intent.slot, &staged));
	before = request_entry()->resource_x_bootstrap_round;
	NM_REQUIRE(ss_accept(&ss_old_consumed, RESOURCE_X_APPLY_STALE));
	NM_REQUIRE(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(
		&ss_old_callback, true, ++fake_pcm_clock_us));
	UT_ASSERT(memcmp(&before, &request_entry()->resource_x_bootstrap_round, sizeof(before)) == 0);
	if (mode != 4) NM_REQUIRE(nc_callback(&staged));
	NM_REQUIRE(nm_send(master, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done));
	NM_REQUIRE(nm_receive(master, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &consumed));
	NM_REQUIRE(ss_accept(&consumed, RESOURCE_X_APPLY_APPLIED));
	if (mode == 4) {
		UT_ASSERT_EQ(state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
		NM_REQUIRE(nc_callback(&staged));
	}
	UT_ASSERT_EQ(state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.resident_s_authorized, 1);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.pending, 0);
	/* The next local reader consumes the resulting real S without REQUEST. */
	NM_REQUIRE(rl_run(NULL)); rl_actual_unlock(&request_buf);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), base.generation + 1);
	complete_idle(); return true;
}
static bool ss_master(int reader, int writer, int mode)
{
	ss_roles(false); NM_REQUIRE(nt_master(writer, reader, mode));
	/* Retain M's directory, grants, attempt floors and canonical cleanup. */
	ss_roles(true); NM_REQUIRE(nt_master(reader, writer, mode));
	return true;
}
static bool ss_reader_role(int master, int writer, int mode, int source, uint64 boot)
{
	ss_roles(false); nm_settled_observer = ss_capture;
	NM_REQUIRE(nt_holder(master, writer, mode)); nm_settled_observer = NULL;
	ss_roles(true); NM_REQUIRE(ss_reader(master, writer, mode));
	(void)source; (void)boot; return true;
}
static bool ss_writer_role(int master, int reader, int mode)
{
	ClusterPcmXWriterLedgerEntry *entry;
	ss_roles(false); NM_REQUIRE(xt_requester(master, reader, mode, 2, 101));
	/* A real local X writer changes the page, then the original holder flush
	 * and X-to-S service supplies these bytes to the old reader. */
	entry = rx_prepare(NULL); NM_REQUIRE(entry != NULL);
	NM_REQUIRE(xt_use_writer(entry, true)); UT_ASSERT_EQ(ordinary_sends, 1);
	holder_writes = holder_write_notes = holder_output_wakes = holder_wal_flushes = 0;
	holder_written_wal_flushes = 0; holder_redirty = false;
	local_pi_writer_ready = local_pi_covered = false;
	BufferDescriptorGetContentLock(&request_buf)->tranche = LWTRANCHE_BUFFER_CONTENT;
	ss_roles(true); NM_REQUIRE(nt_holder(master, reader, mode));
	return true;
}
static void ss_run(int mode)
{
	nt_master_role = ss_master; nt_holder_role = ss_writer_role;
	nt_requester = ss_reader_role;
	nt_run(mode);
}
UT_TEST(test_revoked_S_reacquires_S_through_real_peer_writer) { ss_run(0); }
UT_TEST(test_S_read_successor_DONE_precedes_holder_ACK) { ss_run(2); }
UT_TEST(test_S_read_successor_retains_inflight_image_and_notice) { ss_run(4); }
UT_TEST(test_S_read_successor_resumes_original_content_wait) { ss_run(5); }
int main(void)
{
	UT_PLAN(4);
	UT_RUN(test_revoked_S_reacquires_S_through_real_peer_writer);
	UT_RUN(test_S_read_successor_DONE_precedes_holder_ACK);
	UT_RUN(test_S_read_successor_retains_inflight_image_and_notice);
	UT_RUN(test_S_read_successor_resumes_original_content_wait);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
