/* An independent current writer serves a later ordinary reader/writer.
 * Real requester/master/holder bodies exchange their original frames.
 * Authentication, OS locks and host file handles are controlled leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
#define NATIVE_HOLDER_X_REQUESTER_COMPOSITION
#define NATIVE_HOLDER_WRITEBACK
#include "test_cluster_native_three_role_fixture.inc"
#undef NATIVE_HOLDER_WRITEBACK
#undef NATIVE_HOLDER_X_REQUESTER_COMPOSITION

static bool xt_notice(int master, ClusterPcmResourceXMasterState *state, bool remote)
{
	ResourceXDecodedFrame done, consumed;
	ResourceXIntentSlot staged;
	ResourceXRequesterJoinSnapshot joined;
	ClusterICEnvelope env = {0};
	NM_REQUIRE(nc_decode(state->requester_settlement_intent.payload,
		RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		remote ? RESOURCE_X_SOURCE_COMPLETE_V11_BYTES : RESOURCE_X_COMPLETE_V5_BYTES, &done));
	NM_REQUIRE(nc_stage(&state->requester_settlement_intent.slot, &staged));
	NM_REQUIRE(nc_callback(&staged));
	NM_REQUIRE(nm_send(master, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done));
	NM_REQUIRE(nm_receive(master, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &consumed));
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE;
	env.source_node_id = 0; env.dest_node_id = cluster_node_id;
	env.epoch = 17; env.payload_length = consumed.payload_bytes; auth_peer_boot = 31;
	NM_REQUIRE(gcs_block_resource_x_requester_join_ingress(&env, &consumed, 61, &joined)
		== RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.pending, 0);
	return true;
}
static bool xt_use_writer(ClusterPcmXWriterLedgerEntry *entry, bool modify)
{
	NM_REQUIRE(entry != NULL);
	LWLockAcquire(entry->content_lock, LW_EXCLUSIVE);
	NM_REQUIRE(cluster_bufmgr_pcm_x_writer_activate(entry, true));
	if (modify) {
		/* Hint-class change: no invented WAL or DATA-durability receipt. The
		 * checksum stays from the old disk image until the real flush copy. */
		disk_shared.data[BLCKSZ - 16] ^= 0x7a;
		pg_atomic_fetch_or_u32(&request_buf.state, BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_PERMANENT);
	}
	cluster_bufmgr_pcm_x_writer_mark_releasing(entry);
	LWLockRelease(entry->content_lock);
	cluster_bufmgr_pcm_x_writer_release(entry);
	UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(&request_buf) == NULL);
	return true;
}
static bool xt_seed(int master, int mode, int requester)
{
	ClusterPcmResourceXMasterState *state = NULL;
	ClusterPcmXWriterLedgerEntry *entry;
	(void)mode;
	rx_writer_start(1, 1); cluster_node_id = requester;
	nm_fd = master; nm_mode = 0; auth_observation_hook = NULL;
	input_grant.common.master_session_incarnation = 31; fake_gcs_master_node = 0;
	ordinary_delivery_transport = nm_deliver;
	entry = rx_prepare(NULL); NM_REQUIRE(entry != NULL);
	UT_ASSERT_EQ(wc_legacy, 0); UT_ASSERT_EQ(wc_failures, 0);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, 1);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X);
	UT_ASSERT_EQ(complete_io_calls, 1); UT_ASSERT_EQ(complete_broadcasts, 1);
	NM_REQUIRE(nm_requester_notice(&state)); NM_REQUIRE(xt_notice(master, state, false));
	NM_REQUIRE(xt_use_writer(entry, true));
	UT_ASSERT(memcmp(disk_shared.data, disk_bytes.data, BLCKSZ) != 0);
	UT_ASSERT_EQ(((PageHeader)disk_shared.data)->pd_checksum, ((PageHeader)disk_bytes.data)->pd_checksum);
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & BM_IO_IN_PROGRESS) == 0);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	holder_writes = holder_write_notes = holder_output_wakes = holder_wal_flushes = 0;
	holder_written_wal_flushes = 0; holder_redirty = false;
	local_pi_writer_ready = local_pi_covered = false;
	BufferDescriptorGetContentLock(&request_buf)->tranche = LWTRANCHE_BUFFER_CONTENT;
	return true;
}
static void xt_source_finished(void)
{
	PGAlignedBlock actual;
	UT_ASSERT_EQ(holder_writes, 1); UT_ASSERT_EQ(holder_write_notes, 1);
	UT_ASSERT_EQ(holder_wal_flushes, 1); UT_ASSERT_EQ(holder_written_wal_flushes, 1);
	UT_ASSERT_EQ(holder_output_wakes, 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), 2);
	UT_ASSERT_EQ(pread(fileno(disk_file), actual.data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT(memcmp(actual.data, holder_written.data, BLCKSZ) == 0);
	UT_ASSERT(PageIsVerifiedExtended(actual.data, request_buf.tag.blockNum, 0));
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & (BM_DIRTY | BM_IO_IN_PROGRESS)) == 0);
	UT_ASSERT(!ResourceOwnerHasBufferIO(CurrentResourceOwner, 1));
}
static void xt_deliver(void)
{
	nc_deliver();
	input_observation = ordinary_active->observation;
}
static bool xt_requester(int master, int holder, int mode, int source, uint64 boot)
{
	ClusterPcmXWriterLedgerEntry *entry;
	ClusterPcmResourceXMasterState *state = NULL;
	rx_writer_start(1, 4); nc_fd = master; nc_image_fd = holder; nc_mode = mode;
	nc_source_node = source; nc_source_boot = boot;
	input_grant.common.master_session_incarnation = 31; fake_gcs_master_node = 0;
	auth_observation_hook = nc_auth_peer; ordinary_delivery_transport = xt_deliver;
	entry = rx_prepare(NULL); NM_REQUIRE(entry != NULL);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(wc_legacy, 0); UT_ASSERT_EQ(wc_failures, 0);
	UT_ASSERT_EQ(complete_io_calls, 1); UT_ASSERT_EQ(complete_broadcasts, 1);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X);
	UT_ASSERT(memcmp(disk_shared.data, nc_image.body.acquire_image.original_image.page_bytes, BLCKSZ) == 0);
	NM_REQUIRE(xt_use_writer(entry, false));
	NM_REQUIRE(nc_requester_state(&state)); NM_REQUIRE(xt_notice(master, state, true));
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(complete_broadcasts, 1);
	complete_idle(); return true;
}
static void xt_run(int mode, PcmState target)
{
	nt_source_mode = PCM_STATE_X; nt_target_mode = target;
	nt_seed_holder = xt_seed; nt_source_finished = xt_source_finished;
	nt_keep_seed_bytes = true;
	nt_requester = target == PCM_STATE_X ? xt_requester : nc_parent_from;
	nt_run(mode);
}
UT_TEST(test_remote_X_dirty_to_ordinary_S) { xt_run(0, PCM_STATE_S); }
UT_TEST(test_remote_X_dirty_to_ordinary_X) { xt_run(0, PCM_STATE_X); }
UT_TEST(test_remote_X_DONE_before_holder_ACK) { xt_run(2, PCM_STATE_X); }
UT_TEST(test_remote_X_cleanup_waits_image_callback) { xt_run(4, PCM_STATE_X); }
UT_TEST(test_remote_X_content_busy_keeps_original_service) { xt_run(5, PCM_STATE_X); }
int main(void)
{
	UT_PLAN(5);
	UT_RUN(test_remote_X_dirty_to_ordinary_S);
	UT_RUN(test_remote_X_dirty_to_ordinary_X);
	UT_RUN(test_remote_X_DONE_before_holder_ACK);
	UT_RUN(test_remote_X_cleanup_waits_image_callback);
	UT_RUN(test_remote_X_content_busy_keeps_original_service);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
