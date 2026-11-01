/* Independent requester and colocated master/current-holder composition.
 * Real protocol/native bodies exchange encoded frames; peers, socket and
 * OS lock/storage scheduling remain explicit fixture boundaries.
 * Author: SqlRush <sqlrush@gmail.com> */
#define NATIVE_MASTER_HOLDER_COMPOSITION
#include "test_cluster_native_master_fixture.inc"
#undef NATIVE_MASTER_HOLDER_COMPOSITION

static ResourceXDecodedFrame nc_image, nc_request;
static int nc_fd, nc_mode;
static bool nc_decode(const uint8 *bytes, uint16 type, uint16 size, ResourceXDecodedFrame *out)
{
	ResourceXWireReject reject;
	NM_REQUIRE(cluster_resource_x_wire_decode(type, bytes, size, out, &reject));
	return true;
}
static bool nc_stage(ResourceXIntentSlot *slot, ResourceXIntentSlot *out)
{
	NM_REQUIRE(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
		slot, ++fake_pcm_clock_us, out) == RESOURCE_X_INTENT_STAGED);
	return true;
}
static bool nc_callback(const ResourceXIntentSlot *slot)
{
	NM_REQUIRE(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(slot, true, ++fake_pcm_clock_us));
	return true;
}
static bool nc_holder_start(HolderFixture *f, const ResourceXDecodedFrame *request, PcmState mode)
{
	uint32 state;
	holder_native_start(mode);
	/* Co-located M/H uses this cohort's actual shared peer identity, rather
	 * than the unrelated 83-generation leaf in isolated holder cases. */
	holder_requester_connection = 61;
	request_buf.tag = request->common.logical_assertion.resource;
	provenance_grant_for(0, request_buf.tag);
	if (mode == PCM_STATE_S) service_directory_transition(PCM_TRANS_X_TO_S_DOWNGRADE, 0);
	/* Seed a clean incumbent before this A exists. This is not a fabricated
	 * write-completion or a retirement of an existing episode's PI debt. */
	pg_atomic_write_u32(&((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->pi_holders_bitmap, 0);
	NM_REQUIRE(nm_master_admit(&f->master, request));
	f->block = publish_decode(f->master.state->block_intents[0].payload, true);
	UT_ASSERT_EQ(f->block.body.acquire_decision.source_node, 0);
	UT_ASSERT_EQ(f->block.body.acquire_decision.directory_mode, mode);
	NM_REQUIRE(nm_master_callback(&f->master.state->block_intents[0].slot, true));
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&f->gate));
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f->block, 0, 77, 61) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(holder_probe(&f->work));
	state = request_header_lock(&request_buf);
	cluster_pcm_own_snapshot_locked(&request_buf, &request_base); UnlockBufHdr(&request_buf, state);
	/* Make the real source differ from the requester's old disk bytes. */
	disk_bytes.data[BLCKSZ - 16] = 0x7a;
	((PageHeader)disk_bytes.data)->pd_checksum = pg_checksum_page(disk_bytes.data, request_buf.tag.blockNum);
	disk_store(); memcpy(disk_shared.data, disk_bytes.data, BLCKSZ);
	/* Current memory may retain its prior on-disk checksum. */
	if (mode == PCM_STATE_X) ((PageHeader)disk_shared.data)->pd_checksum ^= UINT16_MAX;
	return true;
}
static bool nc_master(int fd, int mode)
{
	HolderFixture f = {0}; PcmState source = mode == 0 ? PCM_STATE_S : PCM_STATE_X;
	ResourceXDecodedFrame request, image, ack, done, release, retired, consumed;
	ResourceXIntentSlot image_send, master_release, holder_retired;
	ResourceXMasterSnapshot snapshot; ClusterICEnvelope env = {0};
	ResourceXSourceDrainWork next;
	NM_REQUIRE(nm_receive(fd, RESOURCE_X_MSG_ASSERT_X, &request));
	NM_REQUIRE(nc_holder_start(&f, &request, source));
	if (mode == 5) {
		holder_content_busy = true;
		NM_REQUIRE(holder_capture(&f) == RESOURCE_X_APPLY_BAD_STATE);
		NM_REQUIRE(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_BAD_STATE) == RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(!holder_probe(&next)); holder_content_busy = false;
		cluster_pcm_lock_resource_x_source_drain_content_released();
		NM_REQUIRE(holder_probe(&next));
		UT_ASSERT(memcmp(&next.request, &f.work.request, sizeof(next.request)) == 0);
		f.work = next;
	}
	NM_REQUIRE(holder_capture(&f) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(holder_finish(&f) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_APPLIED) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_holder_image_exact(&f.block.common.logical_assertion, &image) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_holder_status_exact(&f.block.common.logical_assertion, &ack) == RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), source == PCM_STATE_S ? 901 : 902);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT(memcmp(image.body.acquire_image.original_image.page_bytes, disk_shared.data, BLCKSZ) == 0);
	NM_REQUIRE(nc_stage(&f.master.state->holder_image_intent, &image_send));
	NM_REQUIRE(nm_send(fd, RESOURCE_X_MSG_IMAGE_OR_GRANT, &image));
	if (mode != 4) NM_REQUIRE(nc_callback(&image_send));
	NM_REQUIRE(nm_master_callback(&f.master.state->holder_status_intent.slot, true));
	if (mode != 2) NM_REQUIRE(cluster_pcm_lock_resource_x_blocked_to_n_exact(&ack, 0, &snapshot) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(nm_receive(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done));
	UT_ASSERT_EQ(done.payload_bytes, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES);
	UT_ASSERT(memcmp(&done.body.acquire_completion.decision, &image.body.acquire_image.source.decision,
		sizeof(done.body.acquire_completion.decision)) == 0);
	NM_REQUIRE(nm_master_done(&done) == RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.master.state->deferred_settlement.present, 4);
	if (mode == 2) {
		NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_source_done_snapshot_exact(&f.master.work, &release) == RESOURCE_X_APPLY_LOCAL_WAIT);
		NM_REQUIRE(cluster_pcm_lock_resource_x_blocked_to_n_exact(&ack, 0, &snapshot) == RESOURCE_X_APPLY_APPLIED);
	}
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.master.work) == RESOURCE_X_APPLY_LOCAL_WAIT);
	NM_REQUIRE(nc_decode(f.master.state->source_settlement.intent.payload, RESOURCE_X_MSG_BLOCK_TO_N,
		RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &release));
	if (mode == 4) {
		/* C/DONE cannot retire H's still-owned image transport callback. */
		NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_read_source_settlement_exact(&release, 0, 61) == RESOURCE_X_APPLY_LOCAL_WAIT);
		UT_ASSERT_EQ(f.master.state->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_STAGED);
		NM_REQUIRE(nc_callback(&image_send));
	}
	NM_REQUIRE(nc_stage(&f.master.state->source_settlement.intent.slot, &master_release));
	env.msg_type = RESOURCE_X_MSG_BLOCK_TO_N; env.source_node_id = 0; env.dest_node_id = 0;
	env.epoch = 17; env.payload_length = release.payload_bytes;
	NM_REQUIRE(gcs_block_resource_x_source_settlement_ingress(&env, &release, 61) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(nc_decode(f.master.state->holder_status_intent.payload, RESOURCE_X_MSG_BLOCKED_TO_N,
		RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &retired));
	NM_REQUIRE(nc_stage(&f.master.state->holder_status_intent.slot, &holder_retired));
	NM_REQUIRE(cluster_pcm_lock_resource_x_source_settlement_ack_exact(&retired, 0, &snapshot) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.master.work) == RESOURCE_X_APPLY_LOCAL_WAIT);
	NM_REQUIRE(nc_callback(&master_release)); NM_REQUIRE(nc_callback(&holder_retired));
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.master.work) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(nc_decode(f.master.state->grant_intent.payload, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &consumed));
	NM_REQUIRE(nm_send(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &consumed));
	NM_REQUIRE(nm_master_callback(&f.master.state->grant_intent.slot, true));
	UT_ASSERT_EQ(f.master.state->deferred_settlement.present, 0);
	UT_ASSERT_EQ(f.master.state->requests[1].phase, RESOURCE_X_MASTER_NONE);
	UT_ASSERT_EQ(f.master.state->holder_image.valid, 0); UT_ASSERT_EQ(f.master.state->holder_status.valid, 0);
	UT_ASSERT_EQ(f.master.state->settlement_retry.pending, 0);
	UT_ASSERT_EQ(f.master.state->pi_carrier.transport_refs, 0);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	holder_no_debt(); return true;
}
static void nc_deliver(void)
{
	ResourceXIntentSlot ticket = request_ticket(&nc_request);
	ResourceXRequesterJoinSnapshot joined; ClusterICEnvelope env = {0};
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_commits, 0);
	if (!nm_send(nc_fd, RESOURCE_X_MSG_ASSERT_X, &nc_request)) { UT_ASSERT(false); pg_re_throw(); }
	cluster_lms_outbound_resource_x_send_complete(&ticket, true);
	if (!nm_receive(nc_fd, RESOURCE_X_MSG_IMAGE_OR_GRANT, &nc_image)) { UT_ASSERT(false); pg_re_throw(); }
	UT_ASSERT_EQ(nc_image.kind, RESOURCE_X_WIRE_IMAGE_ENVELOPE);
	prepared_episode = nc_request.body.acquire_intent;
	env.msg_type = RESOURCE_X_MSG_IMAGE_OR_GRANT; env.source_node_id = 0; env.dest_node_id = 1;
	env.epoch = 17; env.payload_length = nc_image.payload_bytes; auth_peer_boot = 31;
	ResourceXApplyResult result = gcs_block_resource_x_requester_join_ingress(&env, &nc_image, 61, &joined);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED); if (result != RESOURCE_X_APPLY_APPLIED) pg_re_throw();
	if (nc_mode == 3) UT_ASSERT_EQ(gcs_block_resource_x_requester_join_ingress(&env, &nc_image, 61, &joined), RESOURCE_X_APPLY_DUPLICATE);
}
static bool nc_requester_state(ClusterPcmResourceXMasterState **state)
{
	/* Locate the actual original notice by its complete E/owner; do not
	 * synthesize a grant from the received image or construct a DONE. */
	unsigned char *memory = (unsigned char *)fake_pcm_header.data;
	Size offset = offsetof(ClusterPcmResourceXMasterState, requester_settlement_intent);
	unsigned matches = 0;
	for (Size n = offset; n + sizeof(ClusterPcmResourceXReadIntent) <= fake_pcm_header_requested_size; n += 8) {
		ClusterPcmResourceXReadIntent *notice = (void *)(memory + n);
		if (notice->slot.state == RESOURCE_X_INTENT_SLOT_ARMED
			&& notice->slot.kind == RESOURCE_X_WIRE_INSTALL_SETTLEMENT
			&& notice->slot.body.owner_kind == RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT
			&& notice->slot.payload_bytes == RESOURCE_X_SOURCE_COMPLETE_V11_BYTES
			&& memcmp(&notice->slot.body.assertion, &nc_request.common.logical_assertion,
				sizeof(nc_request.common.logical_assertion)) == 0) {
			*state = (void *)(memory + n - offset); matches++;
		}
	}
	NM_REQUIRE(matches == 1); return true;
}
static bool nc_parent(int fd, int mode)
{
	ClusterBufmgrFirstFill read; ClusterPcmResourceXMasterState *state = NULL;
	ResourceXDecodedFrame done, consumed; ResourceXIntentSlot staged;
	ClusterICEnvelope env = {0}; ResourceXRequesterJoinSnapshot joined;
	ordinary_start(&read, 2); nc_fd = fd; nc_mode = mode;
	ordinary_delivery_transport = nc_deliver;
	NM_REQUIRE(ordinary_run(&read));
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_broadcasts, 1);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S); UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_SCUR);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT(memcmp(disk_shared.data, nc_image.body.acquire_image.original_image.page_bytes, BLCKSZ) == 0);
	UT_ASSERT(memcmp(disk_shared.data, disk_bytes.data, BLCKSZ) != 0);
	NM_REQUIRE(nc_requester_state(&state));
	NM_REQUIRE(nc_decode(state->requester_settlement_intent.payload, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &done));
	NM_REQUIRE(nc_stage(&state->requester_settlement_intent.slot, &staged)); NM_REQUIRE(nc_callback(&staged));
	NM_REQUIRE(nm_send(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done));
	NM_REQUIRE(nm_receive(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &consumed));
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE; env.source_node_id = 0; env.dest_node_id = 1;
	env.epoch = 17; env.payload_length = consumed.payload_bytes; auth_peer_boot = 31;
	NM_REQUIRE(gcs_block_resource_x_requester_join_ingress(&env, &consumed, 61, &joined) == RESOURCE_X_APPLY_APPLIED);
	if (mode == 3) NM_REQUIRE(gcs_block_resource_x_requester_join_ingress(&env, &consumed, 61, &joined) == RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, RESOURCE_X_DELIVERY_ACQUIRE_READ_SETTLED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.pending, 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(ordinary_sends, 1);
	complete_idle(); return true;
}
UT_TEST(test_native_current_S_to_S_full_episode) { nm_run_roles(0, nc_master, nc_parent); }
UT_TEST(test_native_current_X_to_S_full_episode) { nm_run_roles(1, nc_master, nc_parent); }
UT_TEST(test_native_current_DONE_waits_independent_holder_ACK) { nm_run_roles(2, nc_master, nc_parent); }
UT_TEST(test_native_current_duplicate_image_and_consumed_complete_once) { nm_run_roles(3, nc_master, nc_parent); }
UT_TEST(test_native_current_DONE_keeps_inflight_holder_send) { nm_run_roles(4, nc_master, nc_parent); }
UT_TEST(test_native_current_holder_busy_resumes_same_request) { nm_run_roles(5, nc_master, nc_parent); }
int main(void)
{
	UT_PLAN(6);
	UT_RUN(test_native_current_S_to_S_full_episode);
	UT_RUN(test_native_current_X_to_S_full_episode);
	UT_RUN(test_native_current_DONE_waits_independent_holder_ACK);
	UT_RUN(test_native_current_duplicate_image_and_consumed_complete_once);
	UT_RUN(test_native_current_DONE_keeps_inflight_holder_send);
	UT_RUN(test_native_current_holder_busy_resumes_same_request);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
