/* H02 current-source callers, original native C and independent send debt.
 * Protocol bodies are extracted; session/storage/OS scheduling are leaves. */
int holder_clean_fixture_main(void);
#define NATIVE_MASTER_HOLDER_COMPOSITION
#define NATIVE_HOLDER_WRITEBACK
#include "test_cluster_native_master_fixture.inc"
#undef NATIVE_MASTER_HOLDER_COMPOSITION
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_membership.h"

#if defined(K_P130_EXPECT_ASSERT_OFF) && defined(USE_ASSERT_CHECKING)
#error "H02 assertion-OFF requires an assertion-disabled configuration"
#endif

static bool h02_wal, h02_captured;
static unsigned h02_capture_calls, h02_finish_calls, h02_request_sends;
static unsigned h02_source_reads, h02_source_writes, h02_images, h02_forwards;
static uint64 h02_source_base;
static PGIOAlignedBlock h02_source_bytes;
static ClusterPageWalBindingV1 h02_binding;
static ClusterPcmLocalPiSnapshotV1 h02_pi;
static ClusterPcmResourceXMasterState *h02_holder_state;
static ResourceXDecodedFrame h02_request, h02_image;

static ResourceXApplyResult h02_capture(HolderFixture *f);
static ResourceXApplyResult h02_finish(HolderFixture *f);
static void h02_census(void);
static void h02_pair_census(ClusterPcmResourceXMasterState *state);
static bool h02_run(ClusterBufmgrFirstFill *read);
static bool h02_send_frame(int fd, uint16 type, const ResourceXDecodedFrame *frame);
static bool nt_holder_state(const ResourceXDecodedFrame *image, ClusterPcmResourceXMasterState **out);

/* Retain the original role controllers and all protocol calls/results. The
 * generated current controller replaces its clean-only pair-zero check with
 * h02_pair_census: PAGE/WAL debt must retain the published immutable pair. */
#define ordinary_run h02_run
#define holder_capture h02_capture
#define holder_finish h02_finish
#define holder_no_debt h02_census
#define nm_send h02_send_frame
#include "test_cluster_k_p130_h02_current.inc"
#include "test_cluster_k_p130_h02_three.inc"
#undef nm_send
#undef holder_no_debt
#undef holder_finish
#undef holder_capture
#undef ordinary_run
#include "test_cluster_k_p130_h02_wal.inc"
#include "test_cluster_k_p130_h02_leaves.inc"

static ClusterICSendResult
h02_request_send(uint8 type, int32 node, const void *bytes, uint32 length,
	const ResourceXIntentSlot *intent)
{
	uint16 header[2] = {type, (uint16)length};
	ResourceXWireReject reject;
	UT_ASSERT_EQ(node, 0); UT_ASSERT_EQ(type, RESOURCE_X_MSG_ASSERT_X);
	UT_ASSERT_EQ(length, RESOURCE_X_ACQUIRE_V3_BYTES);
	UT_ASSERT(cluster_pcm_lock_resource_x_requester_send_current_exact(intent));
	if (length != RESOURCE_X_ACQUIRE_V3_BYTES
		|| !cluster_resource_x_wire_decode(type, bytes, length, &h02_request, &reject))
		return CLUSTER_IC_SEND_HARD_ERROR;
	h02_request_sends++;
	return nm_transfer(nc_fd, header, sizeof(header), true)
		&& nm_transfer(nc_fd, (void *)bytes, length, true)
		? CLUSTER_IC_SEND_DONE : CLUSTER_IC_SEND_HARD_ERROR;
}
static bool h02_peer(int32 node, uint32 capability, uint32 generation)
{
	UT_ASSERT_EQ(node, 0);
	UT_ASSERT_EQ(capability, PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1);
	/* Original fixture distinguishes sender session 51 from ingress 61. */
	return node == 0 && generation == 51;
}
#define cluster_ic_tier1_send_resource_x_intent h02_request_send
#define cluster_sf_peer_capability_generation_matches h02_peer
#include "test_cluster_k_p130_h01_drain.inc"
#undef cluster_sf_peer_capability_generation_matches
#undef cluster_ic_tier1_send_resource_x_intent

static void
h02_deliver(void)
{
	ResourceXRequesterJoinSnapshot joined;
	ClusterICEnvelope env = {0};
	BackendType previous = MyBackendType;
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT_EQ(request_shared.rings[1].count, 1);
	MyBackendType = B_LMS;
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(1), 1);
	MyBackendType = previous;
	if (h02_request_sends != 1) pg_re_throw();
	UT_ASSERT_EQ(request_shared.rings[1].count, 0);
	UT_ASSERT_EQ(request_shared.rings[1].draining, 0);
	nc_request = h02_request;
	if (!nm_receive(nc_image_fd, RESOURCE_X_MSG_IMAGE_OR_GRANT, &nc_image)) {
		UT_ASSERT(false); pg_re_throw();
	}
	UT_ASSERT_EQ(nc_image.kind, RESOURCE_X_WIRE_IMAGE_ENVELOPE);
	UT_ASSERT(nc_image.body.acquire_image.source.decision.source_kind != RESOURCE_X_ACQUIRE_SOURCE_DISK);
	UT_ASSERT_EQ(nc_image.body.acquire_image.source.decision.source_node, nc_source_node);
	UT_ASSERT(memcmp(nc_image.body.acquire_image.original_image.page_bytes, disk_bytes.data, BLCKSZ) != 0);
	prepared_episode = h02_request.body.acquire_intent;
	env.msg_type = RESOURCE_X_MSG_IMAGE_OR_GRANT; env.source_node_id = nc_source_node;
	env.dest_node_id = 1; env.epoch = 17; env.payload_length = nc_image.payload_bytes;
	auth_peer_boot = nc_source_boot;
	if (gcs_block_resource_x_requester_join_ingress(&env, &nc_image, 61, &joined)
		!= RESOURCE_X_APPLY_APPLIED) { UT_ASSERT(false); pg_re_throw(); }
	if (nc_mode == 3)
		UT_ASSERT_EQ(gcs_block_resource_x_requester_join_ingress(&env, &nc_image, 61, &joined), RESOURCE_X_APPLY_DUPLICATE);
}

static bool
h02_run(ClusterBufmgrFirstFill *read)
{
	bool result;
	ResourceXDeliveryClaim installed = {0};
	ResourceXAcquireReadReceipt receipt = {0};
	if (h02_wal) {
		/* Authenticated local WAL-thread provider input, independent of the
		 * received image. R shares H's storage/database, but owns node 1. */
		memset(&local_pi_writer, 0, sizeof(local_pi_writer));
		local_pi_writer.claim.identity.system_identifier = 123;
		local_pi_writer.claim.identity.origin_node_id = cluster_node_id;
		local_pi_writer.claim.identity.origin_thread_id = 1;
		local_pi_writer.claim.identity.origin_owner_incarnation = cluster_qvotec_get_self_incarnation();
		local_pi_writer.claim.identity.thread_claim_created_at = 8;
		local_pi_writer.claim.identity.root_lineage_seq = 9;
		local_pi_writer.claim.identity.storage_uuid[0] = 10;
		local_pi_writer.claim.identity.authority_uuid[0] = 11;
		local_pi_writer.claim.database_incarnation = 12;
		local_pi_writer.claim.max_config_generation = 13;
		local_pi_writer.claim.claim_sha256[0] = 14;
		local_pi_writer.timeline = 1; local_pi_writer_ready = true;
		pg_atomic_fetch_or_u32(&request_buf.state, BM_PERMANENT);
	}
	ordinary_delivery_transport = h02_deliver;
	result = ordinary_run(read);
	UT_ASSERT(result);
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(disk_opens, 0);
	UT_ASSERT_EQ(h02_request_sends, 1); UT_ASSERT_EQ(ordinary_sends, 1);
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_io_calls, 1);
	UT_ASSERT_EQ(complete_broadcasts, 1);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT(memcmp(disk_shared.data, nc_image.body.acquire_image.original_image.page_bytes, BLCKSZ) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_read_close_observe_exact(
		&read->claim, &read->request.episode, &installed, &receipt), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(receipt.authority_generation, nc_image.common.authority_generation);
	UT_ASSERT_EQ(receipt.page_crc32c, cluster_gcs_block_compute_checksum(disk_shared.data));
	UT_ASSERT_EQ(receipt.page_lsn, PageGetLSN(disk_shared.data));
	UT_ASSERT_EQ(receipt.page_scn, ((PageHeader)disk_shared.data)->pd_block_scn);
	UT_ASSERT_EQ(installed.purpose, RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
	printf("# H02 R source=%d E=" UINT64_FORMAT " A=" UINT64_FORMAT
		" base=" UINT64_FORMAT " token=" UINT64_FORMAT " Cgen=" UINT64_FORMAT
		" Icrc=%u Ilsn=" UINT64_FORMAT " Iscn=" UINT64_FORMAT
		" REQUEST=%u C=%u IO=%u reads=%u ring=0\n", nc_source_node,
		h02_request.common.assertion_sequence, nc_image.common.authority_generation,
		request_base.generation, request_base.reservation_token, cluster_pcm_own_gen_get(0),
		receipt.page_crc32c, (uint64)receipt.page_lsn, receipt.page_scn, h02_request_sends,
		complete_commits, complete_io_calls, disk_reads);
	return result;
}

static bool
h02_send_frame(int fd, uint16 type, const ResourceXDecodedFrame *frame)
{
	if (type == RESOURCE_X_MSG_BLOCK_TO_N && frame->kind == RESOURCE_X_WIRE_BLOCK_TO_N
		&& frame->payload_bytes == RESOURCE_X_DECISION_V4_BYTES) {
		UT_ASSERT(frame->body.acquire_decision.source_kind != RESOURCE_X_ACQUIRE_SOURCE_DISK);
		h02_forwards++;
	}
	if (type == RESOURCE_X_MSG_IMAGE_OR_GRANT && frame->kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE) {
		h02_images++;
		UT_ASSERT(memcmp(frame->body.acquire_image.original_image.page_bytes, h02_source_bytes.data, BLCKSZ) == 0);
	}
	if (cluster_node_id == 0 && type == RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE
		&& frame->payload_bytes == RESOURCE_X_SOURCE_COMPLETE_V11_BYTES
		&& frame->common.logical_assertion.requester_node == 1) {
		UT_ASSERT_EQ(h02_forwards, h02_images ? 0 : 1);
		UT_ASSERT(frame->body.acquire_completion.decision.source_kind != RESOURCE_X_ACQUIRE_SOURCE_DISK);
		printf("# H02 M E=" UINT64_FORMAT " A=" UINT64_FORMAT
			" FORWARD=%u source=%d consumed_Cgen=" UINT64_FORMAT "\n",
			frame->common.assertion_sequence, frame->common.authority_generation, h02_forwards,
			frame->body.acquire_completion.decision.source_node,
			frame->body.acquire_completion.native_generation);
	}
	return nm_send(fd, type, frame);
}

static ResourceXApplyResult
h02_capture(HolderFixture *f)
{
	ResourceXApplyResult result;
	if (!h02_captured) {
		h02_captured = true;
		/* Prior disk bootstrap is a different episode, not this service IO. */
		disk_reads = disk_opens = 0;
		holder_writes = holder_write_notes = holder_output_wakes = holder_wal_flushes = 0;
		holder_written_wal_flushes = 0; holder_redirty = false;
		local_pi_writer_ready = local_pi_covered = false;
		BufferDescriptorGetContentLock(&request_buf)->tranche = LWTRANCHE_BUFFER_CONTENT;
		h02_source_base = cluster_pcm_own_gen_get(0);
		if (h02_wal) {
			h02_binding = writeback_install_wal();
			pg_atomic_fetch_or_u32(&request_buf.state, BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED);
		}
		memcpy(h02_source_bytes.data, disk_shared.data, BLCKSZ);
	}
	h02_capture_calls++;
	result = holder_capture(f);
	h02_source_reads = disk_reads; h02_source_writes = holder_writes;
	if (h02_wal && result == RESOURCE_X_APPLY_APPLIED) {
		UT_ASSERT_EQ(holder_writes, 1); UT_ASSERT_EQ(holder_write_notes, 1);
		/* This producer supplies NATIVE_FLUSHED. An extra XLogFlush is not
		 * required; actual output and source gates must retain its binding. */
		UT_ASSERT((h02_binding.flags & CLUSTER_PAGE_WAL_NATIVE_FLUSHED) != 0);
		UT_ASSERT_EQ(holder_written_wal_flushes, holder_wal_flushes);
		UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(request_buf.tag, &h02_pi));
		UT_ASSERT(cluster_page_wal_same_mutation_v1(&h02_pi.last, &h02_binding));
	}
	return result;
}

static ResourceXApplyResult
h02_finish(HolderFixture *f)
{
	ResourceXApplyResult result;
	h02_finish_calls++;
	result = holder_finish(f);
	if (result == RESOURCE_X_APPLY_APPLIED) {
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_exact(&f->block.common.logical_assertion, &h02_image), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(nt_holder_state(&h02_image, &h02_holder_state));
		UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
		UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_SCUR);
		UT_ASSERT(memcmp(h02_source_bytes.data, disk_shared.data, BLCKSZ) == 0);
	}
	return result;
}

static void
h02_pair_census(ClusterPcmResourceXMasterState *state)
{
	uint32 expected = h02_wal ? RESOURCE_X_HOLDER_PAIR_PUBLISHED : 0;
	UT_ASSERT(state != NULL);
	if (state == NULL) return;
	UT_ASSERT_EQ(state->holder_image.valid, expected);
	UT_ASSERT_EQ(state->holder_status.valid, expected);
	UT_ASSERT_EQ(state->pi_carrier.pi_refs, h02_wal ? 1 : 0);
	UT_ASSERT_EQ(state->pi_carrier.transport_refs, 0);
	if (h02_wal) {
		ResourceXDecodedFrame retained;
		UT_ASSERT(state->pi_carrier.last.source_flags != 0);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_exact(
			&h02_image.common.logical_assertion, &retained), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&retained, &h02_image, sizeof(retained)) == 0);
	}
}

static void
h02_census(void)
{
	ClusterPcmLocalPiSnapshotV1 after = {0};
	ClusterPcmLocalPiFloorV1 floor = {0};
	UT_ASSERT_EQ(h02_images, 1); UT_ASSERT_EQ(h02_finish_calls, 1);
	UT_ASSERT(h02_holder_state != NULL);
	if (h02_holder_state == NULL) return;
	h02_pair_census(h02_holder_state);
	UT_ASSERT_EQ(h02_holder_state->settlement_retry.pending, 0);
	UT_ASSERT_EQ(h02_holder_state->pi_carrier.transport_refs, 0);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_SCUR);
	UT_ASSERT(!ResourceOwnerHasBufferIO(CurrentResourceOwner, 1));
	if (h02_wal) {
		UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(request_buf.tag, &after));
		UT_ASSERT(memcmp(&h02_pi, &after, sizeof(after)) == 0);
		UT_ASSERT(cluster_pcm_local_pi_floor_v1(&h02_binding.source, &floor));
		UT_ASSERT_EQ(floor.bounded, 2); UT_ASSERT_EQ(floor.floor, h02_binding.record_start);
		UT_ASSERT_EQ(fsync(fileno(disk_file)), 0);
		UT_ASSERT(!cluster_pcm_local_pi_retire_v1(&after, NULL, NULL, NULL, 0, NULL));
		UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(request_buf.tag, &after));
		UT_ASSERT(memcmp(&h02_pi, &after, sizeof(after)) == 0);
	}
	printf("# H02 H node=%d base=" UINT64_FORMAT " final=" UINT64_FORMAT
		" crc=%u capture=%u finish=%u SHIP=%u reads=%u writes=%u"
		" pair=%u pending=%u transport=%u PI=%u PI_WAL=" UINT64_FORMAT " floor=" UINT64_FORMAT "\n",
		cluster_node_id, h02_source_base, cluster_pcm_own_gen_get(0),
		cluster_gcs_block_compute_checksum(disk_shared.data), h02_capture_calls, h02_finish_calls,
		h02_images, h02_source_reads, h02_source_writes, h02_holder_state->holder_image.valid,
		h02_holder_state->settlement_retry.pending, h02_holder_state->pi_carrier.transport_refs,
		h02_holder_state->pi_carrier.pi_refs, floor.bounded, floor.floor);
	/* H02's frozen row says no smgrread. Keep H and R counts explicit;
	 * the existing S-source storage refresh must not be hidden in R's zero. */
	UT_ASSERT_EQ(h02_source_reads, 0);
	holder_no_debt();
}

static void h02_case(bool third_party, int mode, bool wal)
{
	h02_wal = wal; h02_captured = false; h02_holder_state = NULL;
	h02_capture_calls = h02_finish_calls = h02_request_sends = 0;
	h02_source_reads = h02_source_writes = h02_images = h02_forwards = 0;
	memset(&h02_pi, 0, sizeof(h02_pi));
	if (third_party) nt_run(mode);
	else nm_run_roles(mode, nc_master, nc_parent);
}

UT_TEST(test_k_h02_independent_S_current) { h02_case(true, 0, false); }
UT_TEST(test_k_h02_independent_S_DONE_before_ACK) { h02_case(true, 2, false); }
UT_TEST(test_k_h02_independent_S_duplicate) { h02_case(true, 3, false); }
UT_TEST(test_k_h02_independent_S_inflight_image_cleanup) { h02_case(true, 4, false); }
UT_TEST(test_k_h02_colocated_X_current) { h02_case(false, 1, false); }
UT_TEST(test_k_h02_colocated_X_DONE_before_ACK) { h02_case(false, 2, false); }
UT_TEST(test_k_h02_colocated_X_duplicate) { h02_case(false, 3, false); }
UT_TEST(test_k_h02_colocated_X_inflight_image_cleanup) { h02_case(false, 4, false); }
UT_TEST(test_k_h02_colocated_X_PAGE_WAL_PI) { h02_case(false, 1, true); }
UT_TEST(test_k_h02_colocated_X_PAGE_WAL_PI_inflight_image) { h02_case(false, 4, true); }

int main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
#ifdef USE_ASSERT_CHECKING
	printf("# H02 USE_ASSERT_CHECKING=1\n");
#else
	printf("# H02 USE_ASSERT_CHECKING=0\n");
#endif
	UT_PLAN(10);
	UT_RUN(test_k_h02_independent_S_current);
	UT_RUN(test_k_h02_independent_S_DONE_before_ACK);
	UT_RUN(test_k_h02_independent_S_duplicate);
	UT_RUN(test_k_h02_independent_S_inflight_image_cleanup);
	UT_RUN(test_k_h02_colocated_X_current);
	UT_RUN(test_k_h02_colocated_X_DONE_before_ACK);
	UT_RUN(test_k_h02_colocated_X_duplicate);
	UT_RUN(test_k_h02_colocated_X_inflight_image_cleanup);
	UT_RUN(test_k_h02_colocated_X_PAGE_WAL_PI);
	UT_RUN(test_k_h02_colocated_X_PAGE_WAL_PI_inflight_image);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
