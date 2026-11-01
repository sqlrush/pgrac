/* Independent requester/master first-fill with the real outbound consumer. */
#include "test_cluster_k_p130_h01_master.inc"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_membership.h"

#if defined(K_P130_EXPECT_ASSERT_OFF) && defined(USE_ASSERT_CHECKING)
#error "H01 assertion-OFF test requires an assertion-disabled configuration"
#endif

static ClusterBufmgrFirstFill *h01_read;
static ResourceXDecodedFrame h01_request;
static unsigned h01_sends, h01_pre_a_probes, h01_open_probes, h01_read_probes;
static bool h01_has_a;

/* Other outbound families/local dispatch are outside this disk-source case.
 * They must fail the test if selected; none supplies an acquire decision. */
void
cluster_gcs_block_lmon_prepare_outbound_request(GcsBlockRequestPayload *request, int32 node)
{
	UT_ASSERT(false);
}
void
cluster_gcs_block_note_send_outcome(GcsBlockSendFamily family, ClusterICSendResult result)
{
	UT_ASSERT(false);
}
ClusterICSendResult
cluster_gcs_block_send_direct_zero_reply(int32 node, const GcsBlockReplyHeader *header)
{
	UT_ASSERT(false); return CLUSTER_IC_SEND_HARD_ERROR;
}
bool
cluster_ic_envelope_build(ClusterICEnvelope *env, uint8 type, uint32 source,
	uint32 destination, const void *payload, uint32 length)
{
	UT_ASSERT(false); return false;
}
ClusterICDispatchResult
cluster_ic_dispatch_envelope(const ClusterICEnvelope *env, const void *payload, int32 peer)
{
	UT_ASSERT(false); return CLUSTER_IC_DISPATCH_REJECTED;
}
ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 destination, const void *payload, uint32 length)
{
	UT_ASSERT(false); return CLUSTER_IC_SEND_HARD_ERROR;
}
void cluster_lms_obs_note_outbound_cap_guard_drop(int worker) { UT_ASSERT(false); }
void cluster_lms_obs_note_outbound_not_admitted(int worker) { UT_ASSERT(false); }
void cluster_lms_obs_note_outbound_requeue_drop(int worker) { UT_ASSERT(false); }
bool cluster_membership_cut_generation_current(uint64 expected) { UT_ASSERT(false); return false; }
ClusterMembershipState cluster_membership_get_state(int32 node)
{
	UT_ASSERT(false); return CLUSTER_MEMBER_ABSENT;
}

/* This is the external IC transport boundary. The real drain owns the FIFO
 * and completion callback; only the remote socket bytes are controlled. */
static ClusterICSendResult
h01_send(uint8 type, int32 destination, const void *bytes, uint32 length,
	const ResourceXIntentSlot *intent)
{
	ResourceXWireReject reject;
	uint16 header[2] = {type, (uint16)length};
	UT_ASSERT_EQ(type, RESOURCE_X_MSG_ASSERT_X);
	UT_ASSERT_EQ(destination, 0);
	UT_ASSERT_EQ(length, RESOURCE_X_ACQUIRE_V3_BYTES);
	UT_ASSERT(cluster_pcm_lock_resource_x_requester_send_current_exact(intent));
	if (length != RESOURCE_X_ACQUIRE_V3_BYTES
		|| !cluster_resource_x_wire_decode(type, bytes, length, &h01_request, &reject))
		return CLUSTER_IC_SEND_HARD_ERROR;
	h01_sends++;
	return nm_transfer(nm_fd, header, sizeof(header), true)
		&& nm_transfer(nm_fd, (void *)bytes, length, true)
		? CLUSTER_IC_SEND_DONE : CLUSTER_IC_SEND_HARD_ERROR;
}

static bool
h01_peer(int32 node, uint32 capability, uint32 generation)
{
	UT_ASSERT_EQ(node, 0);
	UT_ASSERT_EQ(capability, PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1);
	/* The inherited provider deliberately separates the requester's send
	 * session (51) from the master's authenticated ingress session (61). */
	return node == 0 && generation == 51;
}

#define cluster_ic_tier1_send_resource_x_intent h01_send
#define cluster_sf_peer_capability_generation_matches h01_peer
#include "test_cluster_k_p130_h01_drain.inc"
#undef cluster_sf_peer_capability_generation_matches
#undef cluster_ic_tier1_send_resource_x_intent

static void
h01_before_a(void)
{
	ResourceXDeliveryClaim absent = {0};
	ResourceXDecodedFrame request;
	ResourceXDecodedAcquireIntent episode;
	ResourceXApplyResult result;
	PGIOAlignedBlock page, before;
	XLogRecPtr lsn = 123;
	uint64 scn = 456;
	(void)request_ticket(&request);
	episode = request.body.acquire_intent;
	memset(&page, 0x5B, sizeof(page)); before = page;
	UT_ASSERT(!h01_has_a);
	UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
	result = cluster_bufmgr_pcm_own_acquire_read_disk_exact(&request_buf,
		&h01_read->base, &episode, &h01_read->request.admission,
		&absent, &page, &lsn, &scn);
	UT_ASSERT(result != RESOURCE_X_APPLY_APPLIED && result != RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&page, &before, sizeof(page)) == 0);
	UT_ASSERT_EQ(lsn, 123); UT_ASSERT_EQ(scn, 456);
	UT_ASSERT_EQ(disk_opens, 0); UT_ASSERT_EQ(disk_reads, 0);
	h01_pre_a_probes++;
}

static void
h01_open(void)
{
	UT_ASSERT(h01_has_a);
	UT_ASSERT_EQ(nm_grant.body.acquire_decision.source_kind, RESOURCE_X_ACQUIRE_SOURCE_DISK);
	UT_ASSERT_EQ(nm_grant.body.acquire_decision.required_holders_bitmap, 0);
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation);
	h01_open_probes++;
}

static void
h01_storage_read(void)
{
	UT_ASSERT(h01_has_a);
	UT_ASSERT_EQ(disk_reads, 1); UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
	for (int i = 0; i < BLCKSZ; i++) UT_ASSERT_EQ((unsigned char)disk_shared.data[i], 0xA7);
	h01_read_probes++;
}

static void
h01_deliver(void)
{
	ClusterICEnvelope env = {0};
	ResourceXRequesterJoinSnapshot joined;
	BackendType previous = MyBackendType;
	h01_before_a();
	UT_ASSERT_EQ(request_shared.rings[1].count, 1);
	MyBackendType = B_LMS;
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(1), 1);
	MyBackendType = previous;
	if (h01_sends != 1) pg_re_throw();
	UT_ASSERT_EQ(h01_sends, 1);
	UT_ASSERT_EQ(request_shared.rings[1].count, 0);
	UT_ASSERT_EQ(request_shared.rings[1].draining, 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.pending, 0);
	if (!nm_receive(nm_fd, RESOURCE_X_MSG_IMAGE_OR_GRANT, &nm_grant)) {
		UT_ASSERT(false); pg_re_throw();
	}
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_commits, 0);
	prepared_episode = h01_request.body.acquire_intent;
	input_observation = ordinary_active->observation;
	input_grant = nm_grant;
	env.msg_type = RESOURCE_X_MSG_IMAGE_OR_GRANT; env.source_node_id = 0;
	env.dest_node_id = cluster_node_id; env.epoch = 17; env.payload_length = nm_grant.payload_bytes;
	auth_peer_boot = 31;
	if (gcs_block_resource_x_requester_join_ingress(&env, &nm_grant, 61, &joined)
		!= RESOURCE_X_APPLY_APPLIED) { UT_ASSERT(false); pg_re_throw(); }
	h01_has_a = true;
	if (nm_mode == 2)
		UT_ASSERT_EQ(gcs_block_resource_x_requester_join_ingress(&env, &nm_grant, 61, &joined),
			RESOURCE_X_APPLY_DUPLICATE);
}

static bool
h01_master(int fd, int mode)
{
	PublishFixture f;
	ResourceXDecodedFrame request, grant, done, notice;
	ClusterPcmResourceXMasterRequest empty = {0};
	struct StopPcmEntryLayout *entry;
	int requester;
	NM_REQUIRE(nm_receive(fd, RESOURCE_X_MSG_ASSERT_X, &request));
	requester = request.common.logical_assertion.requester_node;
	NM_REQUIRE(nm_master_begin(&f, &request));
	grant = publish_decode(f.state->grant_intent.payload, false);
	UT_ASSERT(memcmp(&grant.body.acquire_decision.episode, &request.body.acquire_intent,
		sizeof(request.body.acquire_intent)) == 0);
	UT_ASSERT_EQ(grant.body.acquire_decision.source_kind, RESOURCE_X_ACQUIRE_SOURCE_DISK);
	UT_ASSERT_EQ(grant.body.acquire_decision.source_node, -1);
	UT_ASSERT_EQ(grant.body.acquire_decision.source_service_generation, 0);
	UT_ASSERT_EQ(grant.body.acquire_decision.holders_bitmap, 0);
	UT_ASSERT_EQ(grant.body.acquire_decision.pi_holders_bitmap, 0);
	UT_ASSERT_EQ(grant.body.acquire_decision.required_holders_bitmap, 0);
	if (mode != 1) NM_REQUIRE(nm_master_callback(&f.state->grant_intent.slot, true));
	NM_REQUIRE(nm_send(fd, RESOURCE_X_MSG_IMAGE_OR_GRANT, &grant));
	NM_REQUIRE(nm_receive(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done));
	UT_ASSERT(memcmp(&done.body.acquire_completion.decision, &grant.body.acquire_decision,
		sizeof(grant.body.acquire_decision)) == 0);
	UT_ASSERT_EQ(done.common.outcome, mode == 4 || mode == 5
		? RESOURCE_X_OUTCOME_QUIET_NO_IMAGE : RESOURCE_X_OUTCOME_OK);
	NM_REQUIRE(nm_master_done(&done) == RESOURCE_X_APPLY_APPLIED);
	if (mode == 1) {
		UT_ASSERT(f.state->deferred_settlement.present != 0);
		UT_ASSERT(f.state->requests[requester].phase != RESOURCE_X_MASTER_NONE);
		NM_REQUIRE(nm_master_callback(&f.state->grant_intent.slot, true));
		NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.work) == RESOURCE_X_APPLY_APPLIED);
	}
	NM_REQUIRE(f.state->deferred_settlement.present == 3);
	if (mode == 2) NM_REQUIRE(nm_master_done(&done) == RESOURCE_X_APPLY_DUPLICATE);
	NM_REQUIRE(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		f.state->grant_intent.payload, RESOURCE_X_COMPLETE_V5_BYTES, &notice, &(ResourceXWireReject){0}));
	if (mode == 3) {
		NM_REQUIRE(nm_master_callback(&f.state->grant_intent.slot, false));
		UT_ASSERT_EQ(f.state->deferred_settlement.present, 3);
		UT_ASSERT_EQ(f.state->grant_intent.slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
		fake_pcm_clock_us += 1000000;
	}
	NM_REQUIRE(nm_send(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &notice));
	NM_REQUIRE(nm_master_callback(&f.state->grant_intent.slot, true));
	entry = hash_search((HTAB *)&fake_pcm_htab_token,
		&request.common.logical_assertion.resource, HASH_FIND, NULL);
	NM_REQUIRE(entry != NULL);
	UT_ASSERT(memcmp(&f.state->requests[requester], &empty, sizeof(empty)) == 0);
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 0);
	UT_ASSERT_EQ(f.state->grant_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(f.state->bootstrap_priority[requester].state, RESOURCE_X_BOOTSTRAP_PRIORITY_EMPTY);
	UT_ASSERT_EQ(f.state->bootstrap_receipts[requester].highest_attempt_floor, request.common.assertion_sequence);
	UT_ASSERT_EQ(pg_atomic_read_u32(&entry->pi_holders_bitmap), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&entry->s_holders_bitmap), mode == 4 || mode == 5 ? 0 : 1U << requester);
	printf("# H01 M case=%d E=" UINT64_FORMAT " A=" UINT64_FORMAT
		" canonical=0 notice=0 pending=0 PI=0 S=%u\n", mode, request.common.assertion_sequence,
		grant.common.authority_generation, pg_atomic_read_u32(&entry->s_holders_bitmap));
	return true;
}

static bool
h01_requester(int fd, int mode)
{
	ClusterBufmgrFirstFill read;
	ClusterPcmResourceXMasterState *state = NULL;
	ResourceXIntentSlot offered, staged;
	ResourceXDecodedFrame done, notice;
	ResourceXWireReject reject;
	ClusterICEnvelope env = {0};
	ResourceXRequesterJoinSnapshot joined;
	bool bad = mode == 4 || mode == 5;
	bool completed;
	ordinary_start(&read, 0); h01_read = &read;
	h01_sends = h01_pre_a_probes = h01_open_probes = h01_read_probes = 0;
	h01_has_a = false; nm_fd = fd; nm_mode = mode;
	ordinary_delivery_transport = h01_deliver;
	disk_on_open = h01_open; disk_on_read = h01_storage_read;
	if (mode == 4) ((PageHeader)disk_bytes.data)->pd_checksum ^= UINT16_MAX;
	if (mode == 5) {
		((PageHeader)disk_bytes.data)->pd_lower = BLCKSZ;
		((PageHeader)disk_bytes.data)->pd_upper = SizeOfPageHeaderData;
		((PageHeader)disk_bytes.data)->pd_checksum = pg_checksum_page(disk_bytes.data, request_base.tag.blockNum);
	}
	if (bad) disk_store();
	completed = ordinary_run(&read);
	UT_ASSERT_EQ(completed, !bad);
	UT_ASSERT_EQ(h01_pre_a_probes, 1); UT_ASSERT_EQ(h01_open_probes, 1);
	UT_ASSERT_EQ(h01_read_probes, 1); UT_ASSERT_EQ(h01_sends, 1);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, 1);
	UT_ASSERT_EQ(disk_enters, 1); UT_ASSERT_EQ(disk_ends, 1);
	UT_ASSERT_EQ(complete_commits, bad ? 0 : 1);
	UT_ASSERT_EQ(complete_io_calls, bad ? 0 : 1);
	UT_ASSERT_EQ(wait_calls, bad ? 1 : 0);
	UT_ASSERT_EQ(wait_aborts, bad ? 1 : 0);
	UT_ASSERT_EQ(wait_wakes, bad ? 1 : 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + (bad ? 0 : 1));
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT(!read.pending); UT_ASSERT(!fake_cv_prepared);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR),
		bad ? BM_IO_ERROR : BM_VALID);
	if (!bad) {
		UT_ASSERT_EQ(complete_broadcasts, 1);
		UT_ASSERT(memcmp(disk_shared.data, disk_bytes.data, BLCKSZ) == 0);
	} else {
		for (int i = 0; i < BLCKSZ; i++) UT_ASSERT_EQ((unsigned char)disk_shared.data[i], 0xA7);
		UT_ASSERT_EQ(disk_traces, 1);
	}
	NM_REQUIRE(nm_requester_notice(&state));
	qn_state = state; qn_intent = &state->requester_settlement_intent;
	NM_REQUIRE(qn_probe(&offered));
	NM_REQUIRE(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		state->requester_settlement_intent.payload, RESOURCE_X_COMPLETE_V5_BYTES, &done, &reject));
	UT_ASSERT_EQ(done.body.acquire_completion.native_generation,
		request_base.generation + (bad ? 0 : 1));
	UT_ASSERT_EQ(done.body.acquire_completion.page_crc32c,
		bad ? 0 : cluster_gcs_block_compute_checksum(disk_bytes.data));
	UT_ASSERT_EQ(done.body.acquire_completion.page_lsn, bad ? 0 : PageGetLSN(disk_bytes.data));
	UT_ASSERT_EQ(done.body.acquire_completion.page_scn, bad ? 0 : ((PageHeader)disk_bytes.data)->pd_block_scn);
	printf("# H01 identity case=%d boot=" UINT64_FORMAT " backend=%d/" UINT64_FORMAT
		" buffer=%d base=" UINT64_FORMAT " token=" UINT64_FORMAT
		" epoch=" UINT64_FORMAT " r4=" UINT64_FORMAT " binding=" UINT64_FORMAT
		" enqueue=" UINT64_FORMAT " Cgen=" UINT64_FORMAT " write_io=" UINT64_FORMAT
		" page_lsn=" UINT64_FORMAT " page_scn=" UINT64_FORMAT " aborts=%u\n", mode,
		h01_request.body.acquire_intent.requester_incarnation,
		h01_request.body.acquire_intent.backend_id, h01_request.body.acquire_intent.backend_generation,
		h01_request.body.acquire_intent.buffer_id, h01_request.body.acquire_intent.base_generation,
		h01_request.body.acquire_intent.reservation_token, h01_request.body.acquire_intent.cluster_epoch,
		h01_request.body.acquire_intent.r4_generation, nm_grant.body.acquire_decision.resource_binding,
		nm_grant.body.acquire_decision.enqueue_order, done.body.acquire_completion.native_generation,
		done.body.acquire_completion.write_io_generation, (uint64)done.body.acquire_completion.page_lsn,
		done.body.acquire_completion.page_scn, wait_aborts);
	NM_REQUIRE(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
		&offered, ++fake_pcm_clock_us, &staged) == RESOURCE_X_INTENT_STAGED);
	if (mode != 6)
		NM_REQUIRE(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, ++fake_pcm_clock_us));
	NM_REQUIRE(nm_send(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done));
	NM_REQUIRE(nm_receive(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &notice));
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE; env.source_node_id = 0;
	env.dest_node_id = cluster_node_id; env.epoch = 17; env.payload_length = notice.payload_bytes;
	auth_peer_boot = 31;
	NM_REQUIRE(gcs_block_resource_x_requester_join_ingress(&env, &notice, 61, &joined) == RESOURCE_X_APPLY_APPLIED);
	if (mode == 6) {
		UT_ASSERT_EQ(state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
		NM_REQUIRE(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, ++fake_pcm_clock_us));
	}
	if (mode == 2)
		UT_ASSERT_EQ(gcs_block_resource_x_requester_join_ingress(&env, &notice, 61, &joined), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		bad ? 0 : RESOURCE_X_DELIVERY_ACQUIRE_READ_SETTLED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.pending, 0);
	UT_ASSERT_EQ(request_shared.rings[1].count, 0); UT_ASSERT_EQ(request_shared.rings[1].draining, 0);
	UT_ASSERT_EQ(complete_commits, bad ? 0 : 1); UT_ASSERT_EQ(ordinary_sends, 1);
	printf("# H01 R case=%d E=" UINT64_FORMAT " A=" UINT64_FORMAT
		" Icrc=%u C=%u reads=%u io=%u ring=0 notice=0 resident_receipt=%u\n", mode,
		h01_request.common.assertion_sequence, nm_grant.common.authority_generation,
		bad ? 0 : cluster_gcs_block_compute_checksum(disk_shared.data), complete_commits,
		disk_reads, complete_io_calls, bad ? 0 : 1);
	complete_idle(); return true;
}

UT_TEST(test_k_h01_disk_A_before_IO_and_terminal_census) { nm_run_roles(0, h01_master, h01_requester); }
UT_TEST(test_k_h01_DONE_before_grant_callback) { nm_run_roles(1, h01_master, h01_requester); }
UT_TEST(test_k_h01_duplicate_frames_do_not_repeat_C) { nm_run_roles(2, h01_master, h01_requester); }
UT_TEST(test_k_h01_failed_master_notice_retains_original) { nm_run_roles(3, h01_master, h01_requester); }
UT_TEST(test_k_h01_bad_checksum_never_completes_native) { nm_run_roles(4, h01_master, h01_requester); }
UT_TEST(test_k_h01_bad_shape_with_valid_checksum_never_completes_native) { nm_run_roles(5, h01_master, h01_requester); }
UT_TEST(test_k_h01_ACK_before_real_callback_keeps_notice) { nm_run_roles(6, h01_master, h01_requester); }

int main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
#ifdef USE_ASSERT_CHECKING
	printf("# H01 USE_ASSERT_CHECKING=1\n");
#else
	printf("# H01 USE_ASSERT_CHECKING=0\n");
#endif
	UT_PLAN(7);
	UT_RUN(test_k_h01_disk_A_before_IO_and_terminal_census);
	UT_RUN(test_k_h01_DONE_before_grant_callback);
	UT_RUN(test_k_h01_duplicate_frames_do_not_repeat_C);
	UT_RUN(test_k_h01_failed_master_notice_retains_original);
	UT_RUN(test_k_h01_bad_checksum_never_completes_native);
	UT_RUN(test_k_h01_bad_shape_with_valid_checksum_never_completes_native);
	UT_RUN(test_k_h01_ACK_before_real_callback_keeps_notice);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
