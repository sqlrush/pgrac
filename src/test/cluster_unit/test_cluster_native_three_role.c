/* Independent requester, master and current holder exchange original frames.
 * Authentication, storage and OS-lock leaves remain controlled by the shared
 * native fixtures; no role receives another role's PCM state.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_native_current_fixture.inc"

/* Alternate compositions seed an incumbent through the same real caller,
 * and keep its actual modified bytes instead of the clean fixture image. */
static PcmState nt_source_mode = PCM_STATE_S, nt_target_mode = PCM_STATE_S;
static bool nt_keep_seed_bytes;
static bool (*nt_seed_holder)(int, int, int) = nm_parent_as;
static bool (*nt_requester)(int, int, int, int, uint64) = nc_parent_from;
static void (*nt_source_finished)(void);
/* Consecutive compositions retain each process's actual state across roles. */
static bool (*nt_seed_master)(int, int) = nm_master;
static int nt_holder_node = 2, nt_requester_node = 1;

static bool nt_master(int requester, int holder, int mode)
{
	PublishFixture f; ResourceXDecodedFrame request, block, ack, done, release, retired, consumed;
	ResourceXIntentSlot forwarded, cleanup; ResourceXMasterSnapshot snapshot;
	/* Seed the remote incumbent through a complete real ordinary disk episode.
	 * Legacy/tag-only remote grants deliberately lack boot provenance. */
	NM_REQUIRE(nt_seed_master(holder, 0));
	NM_REQUIRE(nm_receive(requester, RESOURCE_X_MSG_ASSERT_X, &request));
	NM_REQUIRE(nm_master_admit(&f, &request));
	block = publish_decode(f.state->block_intents[nt_holder_node].payload, true);
	UT_ASSERT_EQ(block.body.acquire_decision.source_node, nt_holder_node);
	UT_ASSERT_EQ(block.body.acquire_decision.directory_mode, nt_source_mode);
	NM_REQUIRE(nc_stage(&f.state->block_intents[nt_holder_node].slot, &forwarded));
	NM_REQUIRE(nm_send(holder, RESOURCE_X_MSG_BLOCK_TO_N, &block));
	NM_REQUIRE(nc_callback(&forwarded));
	/* R's C and H's proof are independent messages, on independent peers. */
	if (mode == 2) {
		NM_REQUIRE(nm_receive(requester, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done));
		NM_REQUIRE(nm_master_done(&done) == RESOURCE_X_APPLY_APPLIED);
		NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_source_done_snapshot_exact(&f.work, &release) == RESOURCE_X_APPLY_LOCAL_WAIT);
	}
	NM_REQUIRE(nm_receive(holder, RESOURCE_X_MSG_BLOCKED_TO_N, &ack));
	auth_peer_boot = 101;
	NM_REQUIRE(cluster_pcm_lock_resource_x_blocked_to_n_exact(&ack, nt_holder_node, &snapshot) == RESOURCE_X_APPLY_APPLIED);
	if (mode != 2) {
		NM_REQUIRE(nm_receive(requester, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done));
		NM_REQUIRE(nm_master_done(&done) == RESOURCE_X_APPLY_APPLIED);
	}
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 4);
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.work) == RESOURCE_X_APPLY_LOCAL_WAIT);
	NM_REQUIRE(nc_decode(f.state->source_settlement.intent.payload, RESOURCE_X_MSG_BLOCK_TO_N,
		RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &release));
	NM_REQUIRE(nc_stage(&f.state->source_settlement.intent.slot, &cleanup));
	NM_REQUIRE(nm_send(holder, RESOURCE_X_MSG_BLOCK_TO_N, &release));
	NM_REQUIRE(nm_receive(holder, RESOURCE_X_MSG_BLOCKED_TO_N, &retired));
	NM_REQUIRE(cluster_pcm_lock_resource_x_source_settlement_ack_exact(&retired, nt_holder_node, &snapshot) == RESOURCE_X_APPLY_APPLIED);
	/* ACK arrival cannot consume this master's still-owned physical send. */
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.work) == RESOURCE_X_APPLY_LOCAL_WAIT);
	NM_REQUIRE(nc_callback(&cleanup));
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.work) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(nc_decode(f.state->grant_intent.payload, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &consumed));
	NM_REQUIRE(nm_send(requester, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &consumed));
	NM_REQUIRE(nm_master_callback(&f.state->grant_intent.slot, true));
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 0);
	UT_ASSERT_EQ(f.state->requests[nt_requester_node].phase, RESOURCE_X_MASTER_NONE);
	UT_ASSERT_EQ(f.state->source_settlement.state, RESOURCE_X_SOURCE_SETTLEMENT_ACKED);
	UT_ASSERT_EQ(f.state->source_settlement.intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(f.state->bootstrap_receipts[nt_requester_node].highest_attempt_floor, request.common.assertion_sequence);
	UT_ASSERT_EQ(pg_atomic_read_u32(&((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->s_holders_bitmap),
		nt_target_mode == PCM_STATE_X ? 0 : done.common.outcome == RESOURCE_X_OUTCOME_OK
			? (1u << nt_holder_node) | (1u << nt_requester_node) : (1u << nt_holder_node));
	if (nt_target_mode == PCM_STATE_X)
		UT_ASSERT_EQ(((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->x_holder_node,
			done.common.outcome == RESOURCE_X_OUTCOME_OK ? nt_requester_node : -1);
	UT_ASSERT_EQ(fake_lwlock_depth, 0); return true;
}
static bool nt_holder_state(const ResourceXDecodedFrame *image, ClusterPcmResourceXMasterState **out)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES]; uint16 size;
	ResourceXWireReject reject; unsigned matches = 0;
	unsigned char *memory = (unsigned char *)fake_pcm_header.data;
	Size offset = offsetof(ClusterPcmResourceXMasterState, holder_image.payload);
	NM_REQUIRE(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, image,
		bytes, sizeof(bytes), &size, &reject));
	/* Locate the actual published pair, not an imported master state. */
	for (Size n = offset; n + size <= fake_pcm_header_requested_size; n += 8)
		if (memcmp(memory + n, bytes, size) == 0
			&& n - offset + sizeof(**out) <= fake_pcm_header_requested_size) {
			*out = (void *)(memory + n - offset); matches++;
		}
	NM_REQUIRE(matches == 1); return true;
}
static bool nt_cleanup_work(ResourceXAcquisitionRef *ref)
{
	ResourceXIntentSlot slot; uint8 bytes[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES]; uint32 examined;
	for (int n = 0; n < 256; n++) {
		ResourceXIntentProbeResult result = cluster_pcm_lock_resource_x_outbound_work_probe_exact(
			4, &slot, bytes, sizeof(bytes), &examined, ref);
		NM_REQUIRE(result != RESOURCE_X_INTENT_PROBE_CORRUPT);
		if (result == RESOURCE_X_INTENT_PROBE_SOURCE_SETTLEMENT) return true;
		if (result == RESOURCE_X_INTENT_PROBE_IDLE) break;
	}
	return false;
}
static bool nt_holder(int master, int requester, int mode)
{
	HolderFixture f = {0}; ResourceXDecodedFrame block, image, ack, release, retired;
	ResourceXIntentSlot image_send, proof_send, retired_send;
	ClusterPcmResourceXMasterState *state = NULL; ClusterICEnvelope env = {0};
	ResourceXSourceDrainWork next; ResourceXAcquisitionRef cleanup_ref;
	uint32 header; uint64 base;
	NM_REQUIRE(nt_seed_holder(master, 0, nt_holder_node));
	/* Preserve the actual installed mode, generation and local PCM owner. */
	UT_ASSERT_EQ(cluster_node_id, nt_holder_node); UT_ASSERT_EQ(request_buf.pcm_state, nt_source_mode);
	base = cluster_pcm_own_gen_get(0);
	holder_pins = holder_fuses = 0; holder_content_busy = false;
	holder_after_prepare = NULL; holder_peer_current = true; holder_write_error = false;
	holder_io_enters = holder_io_ends = 0;
	holder_interrupt_repairs = holder_interrupt_releases = 0;
	holder_requester_connection = 61; holder_requester_node = nt_requester_node; MyBackendType = B_LMS;
	NM_REQUIRE(nm_receive(master, RESOURCE_X_MSG_BLOCK_TO_N, &block)); f.block = block;
	UT_ASSERT(BufferTagsEqual(&request_buf.tag, &block.common.logical_assertion.resource));
	NM_REQUIRE(cluster_pcm_lock_resource_x_gate_snapshot(&f.gate));
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&block, 0, 77, 61) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(holder_probe(&f.work));
	header = request_header_lock(&request_buf);
	cluster_pcm_own_snapshot_locked(&request_buf, &request_base); UnlockBufHdr(&request_buf, header);
	if (!nt_keep_seed_bytes) {
		disk_bytes.data[BLCKSZ - 16] = 0x7a;
		((PageHeader)disk_bytes.data)->pd_checksum = pg_checksum_page(disk_bytes.data, request_buf.tag.blockNum);
		disk_store(); memcpy(disk_shared.data, disk_bytes.data, BLCKSZ);
	}
	if (mode == 5) {
		holder_content_busy = true;
		NM_REQUIRE(holder_capture(&f) == RESOURCE_X_APPLY_BAD_STATE);
		NM_REQUIRE(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_BAD_STATE) == RESOURCE_X_APPLY_APPLIED);
		NM_REQUIRE(!holder_probe(&next)); holder_content_busy = false;
		cluster_pcm_lock_resource_x_source_drain_content_released();
		NM_REQUIRE(holder_probe(&next));
		UT_ASSERT(memcmp(&next.request, &f.work.request, sizeof(next.request)) == 0); f.work = next;
	}
	NM_REQUIRE(holder_capture(&f) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(holder_finish(&f) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_APPLIED) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_holder_image_exact(&block.common.logical_assertion, &image) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_holder_status_exact(&block.common.logical_assertion, &ack) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(nt_holder_state(&image, &state));
	UT_ASSERT_EQ(request_buf.pcm_state, nt_target_mode == PCM_STATE_X ? PCM_STATE_N : PCM_STATE_S);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), base + (nt_source_mode == PCM_STATE_X || nt_target_mode == PCM_STATE_X));
	if (nt_source_finished != NULL) nt_source_finished();
	NM_REQUIRE(nc_stage(&state->holder_image_intent, &image_send));
	NM_REQUIRE(nm_send(requester, RESOURCE_X_MSG_IMAGE_OR_GRANT, &image));
	if (mode != 4) NM_REQUIRE(nc_callback(&image_send));
	NM_REQUIRE(nc_stage(&state->holder_status_intent.slot, &proof_send));
	NM_REQUIRE(nm_send(master, RESOURCE_X_MSG_BLOCKED_TO_N, &ack));
	NM_REQUIRE(nc_callback(&proof_send));
	NM_REQUIRE(nm_receive(master, RESOURCE_X_MSG_BLOCK_TO_N, &release));
	env.msg_type = RESOURCE_X_MSG_BLOCK_TO_N; env.source_node_id = 0; env.dest_node_id = nt_holder_node;
	env.epoch = 17; env.payload_length = release.payload_bytes;
	if (mode == 4) {
		NM_REQUIRE(gcs_block_resource_x_source_settlement_ingress(&env, &release, 61) == RESOURCE_X_APPLY_LOCAL_WAIT);
		UT_ASSERT_EQ(state->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_STAGED);
		UT_ASSERT_EQ(state->settlement_retry.pending, 1);
		NM_REQUIRE(nc_callback(&image_send));
		/* Resume the retained original cleanup from the real local work probe. */
		NM_REQUIRE(nt_cleanup_work(&cleanup_ref));
		NM_REQUIRE(cluster_gcs_block_resource_x_source_settlement_tick(&cleanup_ref) == RESOURCE_X_APPLY_APPLIED);
	} else NM_REQUIRE(gcs_block_resource_x_source_settlement_ingress(&env, &release, 61) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(nc_decode(state->holder_status_intent.payload, RESOURCE_X_MSG_BLOCKED_TO_N,
		RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &retired));
	NM_REQUIRE(nc_stage(&state->holder_status_intent.slot, &retired_send));
	NM_REQUIRE(nm_send(master, RESOURCE_X_MSG_BLOCKED_TO_N, &retired));
	NM_REQUIRE(nc_callback(&retired_send));
	UT_ASSERT_EQ(state->holder_image.valid, 0); UT_ASSERT_EQ(state->holder_status.valid, 0);
	UT_ASSERT_EQ(state->settlement_retry.pending, 0); UT_ASSERT_EQ(state->pi_carrier.transport_refs, 0);
	UT_ASSERT_EQ(state->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); NM_REQUIRE(!holder_probe(&next));
	holder_no_debt(); return true;
}
static bool (*nt_master_role)(int, int, int) = nt_master;
static bool (*nt_holder_role)(int, int, int) = nt_holder;
static void nt_run(int mode)
{
	int rm[2], mh[2], hr[2]; pid_t master, holder; bool ok;
	/* All descriptors exist before fork; each process closes every foreign end. */
	int a = socketpair(AF_UNIX, SOCK_STREAM, 0, rm);
	UT_ASSERT_EQ(a, 0); if (a != 0) return;
	int b = socketpair(AF_UNIX, SOCK_STREAM, 0, mh);
	UT_ASSERT_EQ(b, 0); if (b != 0) { close(rm[0]); close(rm[1]); return; }
	int c = socketpair(AF_UNIX, SOCK_STREAM, 0, hr);
	UT_ASSERT_EQ(c, 0); if (c != 0) { close(rm[0]); close(rm[1]); close(mh[0]); close(mh[1]); return; }
#ifdef SO_NOSIGPIPE
	int one = 1; int fds[] = {rm[0], rm[1], mh[0], mh[1], hr[0], hr[1]};
	for (unsigned n = 0; n < lengthof(fds); n++) setsockopt(fds[n], SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
	fflush(NULL); master = fork(); UT_ASSERT(master >= 0);
	if (master == 0) {
		close(rm[0]); close(mh[1]); close(hr[0]); close(hr[1]);
		ok = nt_master_role(rm[1], mh[0], mode); close(rm[1]); close(mh[0]);
		fflush(NULL); _exit(ok && !ut_current_failed ? 0 : 1);
	}
	if (master < 0) {
		close(rm[0]); close(rm[1]); close(mh[0]); close(mh[1]); close(hr[0]); close(hr[1]); return;
	}
	fflush(NULL); holder = fork(); UT_ASSERT(holder >= 0);
	if (holder == 0) {
		close(rm[0]); close(rm[1]); close(mh[0]); close(hr[0]);
		ok = nt_holder_role(mh[1], hr[1], mode); close(mh[1]); close(hr[1]);
		fflush(NULL); _exit(ok && !ut_current_failed ? 0 : 1);
	}
	close(rm[1]); close(mh[0]); close(mh[1]); close(hr[1]);
	if (holder >= 0) { ok = nt_requester(rm[0], hr[0], mode, 2, 101); UT_ASSERT(ok); }
	close(rm[0]); close(hr[0]); nm_wait_child(master); if (holder >= 0) nm_wait_child(holder);
}
UT_TEST(test_three_role_S_source) { nt_run(0); }
UT_TEST(test_three_role_DONE_before_holder_ACK) { nt_run(2); }
UT_TEST(test_three_role_duplicate_delivery) { nt_run(3); }
UT_TEST(test_three_role_image_callback_after_cleanup) { nt_run(4); }
UT_TEST(test_three_role_holder_content_release) { nt_run(5); }
int main(void)
{
	UT_PLAN(5);
	UT_RUN(test_three_role_S_source);
	UT_RUN(test_three_role_DONE_before_holder_ACK);
	UT_RUN(test_three_role_duplicate_delivery);
	UT_RUN(test_three_role_image_callback_after_cleanup);
	UT_RUN(test_three_role_holder_content_release);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
