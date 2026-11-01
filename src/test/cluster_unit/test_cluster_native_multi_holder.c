/* Two real S holders, one ordinary X requester and exact per-holder cleanup.
 * M0 also owns the selected source; H2 and R1 are independent processes.
 * Author: SqlRush <sqlrush@gmail.com> */
#define NATIVE_HOLDER_X_REQUESTER_COMPOSITION
#include "test_cluster_native_current_fixture.inc"
#undef NATIVE_HOLDER_X_REQUESTER_COMPOSITION

static bool mh_source_prepare(HolderFixture *f, const ResourceXDecodedFrame *request)
{
	uint32 header;
	holder_requester_node = request->common.logical_assertion.requester_node;
	NM_REQUIRE(nm_master_admit(&f->master, request));
	UT_ASSERT_EQ(f->master.plan.decision.required_holders_bitmap, 5);
	UT_ASSERT_EQ(f->master.plan.decision.source_node, 0);
	f->block = publish_decode(f->master.state->block_intents[0].payload, true);
	NM_REQUIRE(nm_master_callback(&f->master.state->block_intents[0].slot, true));
	NM_REQUIRE(cluster_pcm_lock_resource_x_gate_snapshot(&f->gate));
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f->block, 0, 77, 61) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(holder_probe(&f->work));
	header = request_header_lock(&request_buf);
	cluster_pcm_own_snapshot_locked(&request_buf, &request_base); UnlockBufHdr(&request_buf, header);
	NM_REQUIRE(holder_capture(f) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(holder_finish(f) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f->work, RESOURCE_X_APPLY_APPLIED) == RESOURCE_X_APPLY_APPLIED);
	return true;
}
static bool mh_local_cleanup(HolderFixture *f)
{
	ResourceXDecodedFrame release, ack; ResourceXIntentSlot offer, reply;
	ResourceXMasterSnapshot snapshot; ClusterICEnvelope env = {0};
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f->master.work) == RESOURCE_X_APPLY_LOCAL_WAIT);
	NM_REQUIRE(nc_decode(f->master.state->source_settlement.intent.payload, RESOURCE_X_MSG_BLOCK_TO_N,
		RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &release));
	UT_ASSERT_EQ(release.common.action_node, 0);
	NM_REQUIRE(nc_stage(&f->master.state->source_settlement.intent.slot, &offer));
	env.msg_type = RESOURCE_X_MSG_BLOCK_TO_N; env.source_node_id = env.dest_node_id = 0;
	env.epoch = 17; env.payload_length = release.payload_bytes;
	NM_REQUIRE(gcs_block_resource_x_source_settlement_ingress(&env, &release, 61) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(nc_decode(f->master.state->holder_status_intent.payload, RESOURCE_X_MSG_BLOCKED_TO_N,
		RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &ack));
	NM_REQUIRE(nc_stage(&f->master.state->holder_status_intent.slot, &reply));
	NM_REQUIRE(cluster_pcm_lock_resource_x_source_settlement_ack_exact(&ack, 0, &snapshot) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(nc_callback(&offer)); NM_REQUIRE(nc_callback(&reply));
	UT_ASSERT_EQ(f->master.state->pi_carrier.transport_refs, 0);
	/* This seed is clean and has no WAL debt. Native PI is separate from
	 * the zero logical-WAL references; this is no DATA retirement proof. */
	UT_ASSERT_EQ(f->master.state->pi_carrier.pi_refs, 0);
	return true;
}
static bool mh_master(int requester, int holder, int mode)
{
	HolderFixture f = {0}; ResourceXDecodedFrame request, block, image, ack0, ack2, done, release, retired, consumed;
	ResourceXIntentSlot image_send, forward, cleanup; ResourceXMasterSnapshot snapshot;
	/* Actual prior S acquisition installs H2 and records its boot provenance. */
	NM_REQUIRE(nc_master(holder, 0));
	UT_ASSERT_EQ(pg_atomic_read_u32(&((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->s_holders_bitmap), 5);
	NM_REQUIRE(nm_receive(requester, RESOURCE_X_MSG_ASSERT_X, &request));
	UT_ASSERT_EQ(request.common.target_mode, PCM_STATE_X);
	NM_REQUIRE(mh_source_prepare(&f, &request));
	block = publish_decode(f.master.state->block_intents[2].payload, true);
	NM_REQUIRE(nc_stage(&f.master.state->block_intents[2].slot, &forward));
	NM_REQUIRE(nm_send(holder, RESOURCE_X_MSG_BLOCK_TO_N, &block)); NM_REQUIRE(nc_callback(&forward));
	NM_REQUIRE(cluster_pcm_lock_resource_x_holder_image_exact(&f.block.common.logical_assertion, &image) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_holder_status_exact(&f.block.common.logical_assertion, &ack0) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(nc_stage(&f.master.state->holder_image_intent, &image_send));
	NM_REQUIRE(nm_send(requester, RESOURCE_X_MSG_IMAGE_OR_GRANT, &image)); NM_REQUIRE(nc_callback(&image_send));
	NM_REQUIRE(nm_master_callback(&f.master.state->holder_status_intent.slot, true));
	NM_REQUIRE(cluster_pcm_lock_resource_x_blocked_to_n_exact(&ack0, 0, &snapshot) == RESOURCE_X_APPLY_APPLIED);
	if (mode == 3) {
		NM_REQUIRE(nm_receive(requester, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done));
		NM_REQUIRE(nm_master_done(&done) == RESOURCE_X_APPLY_APPLIED);
		NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_source_done_snapshot_exact(&f.master.work, &release) == RESOURCE_X_APPLY_LOCAL_WAIT);
	}
	NM_REQUIRE(nm_receive(holder, RESOURCE_X_MSG_BLOCKED_TO_N, &ack2)); auth_peer_boot = 101;
	NM_REQUIRE(cluster_pcm_lock_resource_x_blocked_to_n_exact(&ack2, 2, &snapshot) == RESOURCE_X_APPLY_APPLIED);
	if (mode != 3) {
		NM_REQUIRE(nm_receive(requester, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done));
		NM_REQUIRE(nm_master_done(&done) == RESOURCE_X_APPLY_APPLIED);
	}
	NM_REQUIRE(mh_local_cleanup(&f));
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.master.work) == RESOURCE_X_APPLY_LOCAL_WAIT);
	NM_REQUIRE(nc_decode(f.master.state->source_settlement.intent.payload, RESOURCE_X_MSG_BLOCK_TO_N,
		RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &release));
	UT_ASSERT_EQ(release.common.action_node, 2); UT_ASSERT(f.master.state->requests[1].phase != RESOURCE_X_MASTER_NONE);
	NM_REQUIRE(nc_stage(&f.master.state->source_settlement.intent.slot, &cleanup));
	NM_REQUIRE(nm_send(holder, RESOURCE_X_MSG_BLOCK_TO_N, &release));
	NM_REQUIRE(nm_receive(holder, RESOURCE_X_MSG_BLOCKED_TO_N, &retired));
	NM_REQUIRE(cluster_pcm_lock_resource_x_source_settlement_ack_exact(&retired, 2, &snapshot) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.master.work) == RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(f.master.state->requests[1].phase != RESOURCE_X_MASTER_NONE);
	NM_REQUIRE(nc_callback(&cleanup));
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.master.work) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(nc_decode(f.master.state->grant_intent.payload, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &consumed));
	NM_REQUIRE(nm_send(requester, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &consumed));
	NM_REQUIRE(nm_master_callback(&f.master.state->grant_intent.slot, true));
	UT_ASSERT_EQ(f.master.state->requests[1].phase, RESOURCE_X_MASTER_NONE);
	UT_ASSERT_EQ(f.master.state->deferred_settlement.present, 0);
	UT_ASSERT_EQ(f.master.state->acquire_settled_holders_bitmap, 0);
	UT_ASSERT_EQ(f.master.state->source_settlement.intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(f.master.state->pi_carrier.pi_refs, 0); UT_ASSERT_EQ(f.master.state->pi_carrier.transport_refs, 0);
	UT_ASSERT_EQ(((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->x_holder_node, 1);
	holder_no_debt(); return true;
}
static bool mh_holder_state(const ResourceXAssertion *assertion, ClusterPcmResourceXMasterState **out)
{
	ResourceXIntentSlot slot; uint8 bytes[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES]; unsigned matches = 0;
	unsigned char *memory = (unsigned char *)fake_pcm_header.data;
	Size offset = offsetof(ClusterPcmResourceXMasterState, holder_status_intent.slot);
	NM_REQUIRE(cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(assertion, &slot, bytes, sizeof(bytes)) == RESOURCE_X_APPLY_APPLIED);
	for (Size n = offset; n + sizeof(slot) <= fake_pcm_header_requested_size; n += 8)
		if (memcmp(memory + n, &slot, sizeof(slot)) == 0 && n - offset + sizeof(**out) <= fake_pcm_header_requested_size) {
			*out = (void *)(memory + n - offset); matches++;
		}
	NM_REQUIRE(matches == 1); return true;
}
static bool mh_cleanup_work(ResourceXAcquisitionRef *ref)
{
	ResourceXIntentSlot slot; uint8 bytes[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES]; uint32 examined;
	for (int n = 0; n < 256; n++) {
		ResourceXIntentProbeResult result = cluster_pcm_lock_resource_x_outbound_work_probe_exact(4, &slot, bytes, sizeof(bytes), &examined, ref);
		NM_REQUIRE(result != RESOURCE_X_INTENT_PROBE_CORRUPT);
		if (result == RESOURCE_X_INTENT_PROBE_SOURCE_SETTLEMENT) return true;
		if (result == RESOURCE_X_INTENT_PROBE_IDLE) break;
	}
	return false;
}
static bool mh_holder(int master, int requester, int mode)
{
	HolderFixture f = {0}; ClusterPcmResourceXMasterState *state = NULL;
	ResourceXDecodedFrame block, witness, ack, release, retired;
	ResourceXIntentSlot witness_send, proof_send, reply; ResourceXSourceDrainWork next;
	ResourceXAcquisitionRef ref; ResourceXAcquireSSourceState phase; ClusterPcmOwnSnapshot native;
	int32 id; ClusterICEnvelope env = {0}; uint32 header; uint64 base;
	NM_REQUIRE(nc_parent_role(master, master, 0, 0, 31, 2));
	base = cluster_pcm_own_gen_get(0); UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	holder_pins = holder_fuses = 0; holder_content_busy = false; holder_after_prepare = NULL;
	holder_peer_current = true; holder_write_error = false; holder_requester_connection = 61; holder_requester_node = 1;
	holder_io_enters = holder_io_ends = holder_interrupt_repairs = holder_interrupt_releases = 0;
	MyBackendType = B_LMS;
	NM_REQUIRE(nm_receive(master, RESOURCE_X_MSG_BLOCK_TO_N, &block)); f.block = block;
	NM_REQUIRE(cluster_pcm_lock_resource_x_gate_snapshot(&f.gate));
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&block, 0, 77, 61) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(holder_probe(&f.work));
	header = request_header_lock(&request_buf); cluster_pcm_own_snapshot_locked(&request_buf, &request_base); UnlockBufHdr(&request_buf, header);
	if (mode == 4) {
		holder_content_busy = true; NM_REQUIRE(holder_capture(&f) == RESOURCE_X_APPLY_BAD_STATE);
		NM_REQUIRE(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_BAD_STATE) == RESOURCE_X_APPLY_APPLIED);
		NM_REQUIRE(!holder_probe(&next)); holder_content_busy = false;
		cluster_pcm_lock_resource_x_source_drain_content_released(); NM_REQUIRE(holder_probe(&next));
		UT_ASSERT(memcmp(&next.request, &f.work.request, sizeof(next.request)) == 0); f.work = next;
	}
	NM_REQUIRE(holder_capture(&f) == RESOURCE_X_APPLY_APPLIED); NM_REQUIRE(holder_finish(&f) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_APPLIED) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_s_source_snapshot_exact(&block, 0, &id, &native, &witness, &phase) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_holder_status_exact(&block.common.logical_assertion, &ack) == RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(witness.kind, RESOURCE_X_WIRE_BLOCKED_TO_N); UT_ASSERT(!witness.body.acquire_witness.has_source_proof);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), base + 1); UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_N);
	NM_REQUIRE(mh_holder_state(&block.common.logical_assertion, &state));
	NM_REQUIRE(nc_stage(&state->holder_image_intent, &witness_send));
	NM_REQUIRE(nm_send(requester, RESOURCE_X_MSG_BLOCKED_TO_N, &witness));
	if (mode != 2) NM_REQUIRE(nc_callback(&witness_send));
	NM_REQUIRE(nc_stage(&state->holder_status_intent.slot, &proof_send));
	NM_REQUIRE(nm_send(master, RESOURCE_X_MSG_BLOCKED_TO_N, &ack)); NM_REQUIRE(nc_callback(&proof_send));
	NM_REQUIRE(nm_receive(master, RESOURCE_X_MSG_BLOCK_TO_N, &release));
	env.msg_type = RESOURCE_X_MSG_BLOCK_TO_N; env.source_node_id = 0; env.dest_node_id = 2;
	env.epoch = 17; env.payload_length = release.payload_bytes;
	if (mode == 2) {
		NM_REQUIRE(gcs_block_resource_x_source_settlement_ingress(&env, &release, 61) == RESOURCE_X_APPLY_LOCAL_WAIT);
		UT_ASSERT_EQ(state->settlement_retry.pending, 1);
		NM_REQUIRE(nc_callback(&witness_send)); NM_REQUIRE(mh_cleanup_work(&ref));
		NM_REQUIRE(cluster_gcs_block_resource_x_source_settlement_tick(&ref) == RESOURCE_X_APPLY_APPLIED);
	} else NM_REQUIRE(gcs_block_resource_x_source_settlement_ingress(&env, &release, 61) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(nc_decode(state->holder_status_intent.payload, RESOURCE_X_MSG_BLOCKED_TO_N, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &retired));
	NM_REQUIRE(nc_stage(&state->holder_status_intent.slot, &reply));
	NM_REQUIRE(nm_send(master, RESOURCE_X_MSG_BLOCKED_TO_N, &retired)); NM_REQUIRE(nc_callback(&reply));
	UT_ASSERT_EQ(state->holder_image.valid, 0); UT_ASSERT_EQ(state->holder_status.valid, 0);
	UT_ASSERT_EQ(state->settlement_retry.pending, 0); UT_ASSERT_EQ(state->pi_carrier.pi_refs, 0);
	UT_ASSERT_EQ(state->pi_carrier.transport_refs, 0); UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	NM_REQUIRE(!holder_probe(&next)); holder_no_debt(); return true;
}
static int mh_master_fd, mh_holder_fd, mh_mode;
static void mh_auth(int32 node, unsigned sample)
{
	(void)sample; UT_ASSERT(node == 0 || node == 2); auth_peer_boot = node == 0 ? 31 : 101;
}
static bool mh_accept(const ResourceXDecodedFrame *frame, int source, ResourceXRequesterJoinSnapshot *out)
{
	ClusterICEnvelope env = {0};
	env.msg_type = source == 0 ? RESOURCE_X_MSG_IMAGE_OR_GRANT : RESOURCE_X_MSG_BLOCKED_TO_N;
	env.source_node_id = source; env.dest_node_id = 1; env.epoch = 17; env.payload_length = frame->payload_bytes;
	ResourceXApplyResult result = gcs_block_resource_x_requester_join_ingress(&env, frame, 61, out);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED); NM_REQUIRE(result == RESOURCE_X_APPLY_APPLIED); return true;
}
static void mh_deliver(void)
{
	ResourceXDecodedFrame witness; ResourceXRequesterJoinSnapshot joined; bool image_first = mh_mode != 1;
	ResourceXIntentSlot ticket = request_ticket(&nc_request);
	if (!nm_send(mh_master_fd, RESOURCE_X_MSG_ASSERT_X, &nc_request)) { UT_ASSERT(false); pg_re_throw(); }
	cluster_lms_outbound_resource_x_send_complete(&ticket, true); prepared_episode = nc_request.body.acquire_intent;
	/* Native event leaves observe this actual call's X intent. */
	input_observation = ordinary_active->observation;
	if (image_first) {
		if (!nm_receive(mh_master_fd, RESOURCE_X_MSG_IMAGE_OR_GRANT, &nc_image) || !mh_accept(&nc_image, 0, &joined)) pg_re_throw();
	} else if (!nm_receive(mh_holder_fd, RESOURCE_X_MSG_BLOCKED_TO_N, &witness) || !mh_accept(&witness, 2, &joined)) pg_re_throw();
	UT_ASSERT((joined.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE) == 0);
	UT_ASSERT_EQ(complete_io_calls, 0); UT_ASSERT_EQ(complete_broadcasts, 0); UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_N);
	if (image_first) {
		if (!nm_receive(mh_holder_fd, RESOURCE_X_MSG_BLOCKED_TO_N, &witness) || !mh_accept(&witness, 2, &joined)) pg_re_throw();
	} else if (!nm_receive(mh_master_fd, RESOURCE_X_MSG_IMAGE_OR_GRANT, &nc_image) || !mh_accept(&nc_image, 0, &joined)) pg_re_throw();
	UT_ASSERT(joined.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE);
	if (mh_mode == 4) {
		ClusterICEnvelope duplicate = {0};
		duplicate.msg_type = RESOURCE_X_MSG_BLOCKED_TO_N; duplicate.source_node_id = 2; duplicate.dest_node_id = 1;
		duplicate.epoch = 17; duplicate.payload_length = witness.payload_bytes;
		UT_ASSERT_EQ(gcs_block_resource_x_requester_join_ingress(&duplicate, &witness, 61, &joined), RESOURCE_X_APPLY_DUPLICATE);
	}
}
static bool mh_parent(int master, int holder, int mode)
{
	ClusterPcmXWriterLedgerEntry *entry; ClusterPcmResourceXMasterState *state = NULL;
	ResourceXIntentSlot notice; ResourceXDecodedFrame done, consumed;
	ResourceXRequesterJoinSnapshot joined; ClusterICEnvelope env = {0};
	rx_writer_start(1, 4); mh_master_fd = master; mh_holder_fd = holder; mh_mode = mode;
	/* The inherited read-ingress session leaf uses this scalar. Bind it to
	 * M0's independently fixed boot, never to a received grant or image. */
	input_grant.common.master_session_incarnation = 31; fake_gcs_master_node = 0;
	auth_observation_hook = mh_auth; ordinary_delivery_transport = mh_deliver;
	entry = rx_prepare(NULL); NM_REQUIRE(entry != NULL);
	UT_ASSERT_EQ(wc_legacy, 0); UT_ASSERT_EQ(wc_failures, 0); UT_ASSERT_EQ(ordinary_sends, 1);
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_io_calls, 1); UT_ASSERT_EQ(complete_broadcasts, 1); UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	UT_ASSERT(memcmp(disk_shared.data, nc_image.body.acquire_image.original_image.page_bytes, BLCKSZ) == 0);
	LWLockAcquire(entry->content_lock, LW_EXCLUSIVE);
	NM_REQUIRE(cluster_bufmgr_pcm_x_writer_activate(entry, true));
	cluster_bufmgr_pcm_x_writer_mark_releasing(entry); LWLockRelease(entry->content_lock);
	cluster_bufmgr_pcm_x_writer_release(entry); UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(&request_buf) == NULL);
	NM_REQUIRE(nc_requester_state(&state));
	NM_REQUIRE(nc_decode(state->requester_settlement_intent.payload, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &done));
	NM_REQUIRE(nc_stage(&state->requester_settlement_intent.slot, &notice)); NM_REQUIRE(nc_callback(&notice));
	NM_REQUIRE(nm_send(master, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done));
	NM_REQUIRE(nm_receive(master, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &consumed));
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE; env.source_node_id = 0; env.dest_node_id = 1;
	env.epoch = 17; env.payload_length = consumed.payload_bytes;
	NM_REQUIRE(gcs_block_resource_x_requester_join_ingress(&env, &consumed, 61, &joined) == RESOURCE_X_APPLY_APPLIED);
	if (mode == 4) NM_REQUIRE(gcs_block_resource_x_requester_join_ingress(&env, &consumed, 61, &joined) == RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.pending, 0);
	UT_ASSERT_EQ(complete_io_calls, 1); UT_ASSERT_EQ(complete_broadcasts, 1);
	UT_ASSERT_EQ(ordinary_sends, 1); complete_idle(); return true;
}
static void mh_run(int mode)
{
	int rm[2], mh[2], hr[2]; pid_t master, holder; bool ok;
	int a = socketpair(AF_UNIX, SOCK_STREAM, 0, rm); UT_ASSERT_EQ(a, 0); if (a != 0) return;
	int b = socketpair(AF_UNIX, SOCK_STREAM, 0, mh); UT_ASSERT_EQ(b, 0);
	if (b != 0) { close(rm[0]); close(rm[1]); return; }
	int c = socketpair(AF_UNIX, SOCK_STREAM, 0, hr); UT_ASSERT_EQ(c, 0);
	if (c != 0) { close(rm[0]); close(rm[1]); close(mh[0]); close(mh[1]); return; }
#ifdef SO_NOSIGPIPE
	int one = 1; int fds[] = {rm[0], rm[1], mh[0], mh[1], hr[0], hr[1]};
	for (unsigned n = 0; n < lengthof(fds); n++) setsockopt(fds[n], SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
	fflush(NULL); master = fork(); UT_ASSERT(master >= 0);
	if (master == 0) {
		close(rm[0]); close(mh[1]); close(hr[0]); close(hr[1]);
		ok = mh_master(rm[1], mh[0], mode); close(rm[1]); close(mh[0]); fflush(NULL); _exit(ok && !ut_current_failed ? 0 : 1);
	}
	if (master < 0) { close(rm[0]); close(rm[1]); close(mh[0]); close(mh[1]); close(hr[0]); close(hr[1]); return; }
	fflush(NULL); holder = fork(); UT_ASSERT(holder >= 0);
	if (holder == 0) {
		close(rm[0]); close(rm[1]); close(mh[0]); close(hr[0]);
		ok = mh_holder(mh[1], hr[1], mode); close(mh[1]); close(hr[1]); fflush(NULL); _exit(ok && !ut_current_failed ? 0 : 1);
	}
	close(rm[1]); close(mh[0]); close(mh[1]); close(hr[1]);
	if (holder >= 0) { ok = mh_parent(rm[0], hr[0], mode); UT_ASSERT(ok); }
	close(rm[0]); close(hr[0]); nm_wait_child(master); if (holder >= 0) nm_wait_child(holder);
}
UT_TEST(test_multi_holder_image_first_then_X_writer) { mh_run(0); }
UT_TEST(test_multi_holder_witness_first_then_X_writer) { mh_run(1); }
UT_TEST(test_multi_holder_cleanup_waits_witness_callback) { mh_run(2); }
UT_TEST(test_multi_holder_DONE_waits_independent_ACK) { mh_run(3); }
UT_TEST(test_multi_holder_busy_resumes_original_service) { mh_run(4); }
int main(void)
{
	UT_PLAN(5);
	UT_RUN(test_multi_holder_image_first_then_X_writer);
	UT_RUN(test_multi_holder_witness_first_then_X_writer);
	UT_RUN(test_multi_holder_cleanup_waits_witness_callback);
	UT_RUN(test_multi_holder_DONE_waits_independent_ACK);
	UT_RUN(test_multi_holder_busy_resumes_original_service);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
