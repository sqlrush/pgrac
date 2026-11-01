/* Ordinary resident N/CURRENT reads from independently produced current bytes.
 * Original controllers exchange wire frames; OS/session leaves are controlled. */
int holder_clean_fixture_main(void);
#define NATIVE_MASTER_HOLDER_COMPOSITION
#define NATIVE_RESIDENT_REQUEST_COMPOSITION
#define NATIVE_RESIDENT_FILL_COMPOSITION
#include "test_cluster_native_master_fixture.inc"
#undef NATIVE_RESIDENT_FILL_COMPOSITION
#undef NATIVE_RESIDENT_REQUEST_COMPOSITION
#undef NATIVE_MASTER_HOLDER_COMPOSITION
#include "test_cluster_k_p130_h03_request.inc"
#include "test_cluster_k_p130_h03_fill.inc"
#include "test_cluster_k_p130_h03_lock.inc"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_membership.h"

#if defined(K_P130_EXPECT_ASSERT_OFF) && defined(USE_ASSERT_CHECKING)
#error "H03 assertion-OFF requires an assertion-disabled configuration"
#endif

static unsigned h03_requests, h03_probes, h03_captures, h03_finishes, h03_images, h03_forwards;
static uint64 h03_old_scn, h03_source_base;
static PGIOAlignedBlock h03_old, h03_source;
static ResourceXDecodedFrame h03_request, h03_image;
static ClusterPcmResourceXMasterState *h03_holder_state;

static ResourceXApplyResult h03_capture(HolderFixture *f);
static ResourceXApplyResult h03_finish(HolderFixture *f);
static void h03_census(void);
static bool h03_send(int fd, uint16 type, const ResourceXDecodedFrame *frame);
static bool h03_parent_from(int fd, int image_fd, int mode, int source_node, uint64 source_boot);
static bool nt_holder_state(const ResourceXDecodedFrame *image, ClusterPcmResourceXMasterState **out);

#define holder_capture h03_capture
#define holder_finish h03_finish
#define holder_no_debt h03_census
#define nm_send h03_send
#include "test_cluster_k_p130_h03_current.inc"
#define nc_parent_from h03_parent_from
#include "test_cluster_k_p130_h02_three.inc"
#undef nc_parent_from
#undef nm_send
#undef holder_no_debt
#undef holder_finish
#undef holder_capture
#include "test_cluster_k_p130_h02_leaves.inc"

static ClusterICSendResult
h03_request_send(uint8 type, int32 node, const void *bytes, uint32 length,
	const ResourceXIntentSlot *intent)
{
	uint16 header[2] = {type, (uint16)length}; ResourceXWireReject reject;
	UT_ASSERT_EQ(node, 0); UT_ASSERT_EQ(type, RESOURCE_X_MSG_ASSERT_X);
	UT_ASSERT_EQ(length, RESOURCE_X_ACQUIRE_V3_BYTES);
	UT_ASSERT(cluster_pcm_lock_resource_x_requester_send_current_exact(intent));
	if (length != RESOURCE_X_ACQUIRE_V3_BYTES
		|| !cluster_resource_x_wire_decode(type, bytes, length, &h03_request, &reject))
		return CLUSTER_IC_SEND_HARD_ERROR;
	h03_requests++;
	return nm_transfer(nc_fd, header, sizeof(header), true)
		&& nm_transfer(nc_fd, (void *)bytes, length, true)
		? CLUSTER_IC_SEND_DONE : CLUSTER_IC_SEND_HARD_ERROR;
}
static bool h03_peer(int32 node, uint32 capability, uint32 generation)
{
	UT_ASSERT_EQ(node, 0);
	UT_ASSERT_EQ(capability, PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1);
	return node == 0 && generation == 51;
}
#define cluster_ic_tier1_send_resource_x_intent h03_request_send
#define cluster_sf_peer_capability_generation_matches h03_peer
#include "test_cluster_k_p130_h01_drain.inc"
#undef cluster_sf_peer_capability_generation_matches
#undef cluster_ic_tier1_send_resource_x_intent

static bool
h03_grant_only(void)
{
	ResourceXDecodedFrame grant = {0}, absent_image = {0}, refused_grant = {0};
	ResourceXDeliveryClaim claim = {0}; ResourceXRequesterJoinSnapshot joined = {0};
	ClusterPcmOwnEntry own = request_own; uint32 state = pg_atomic_read_u32(&request_buf.state);
	ClusterICEnvelope env = {0}; ResourceXApplyResult result;
	/* A negative input projection of the real M's decision carried by SHIP.
	 * It is not another positive-path wire hop or an invented source image. */
	grant.kind = RESOURCE_X_WIRE_AUTHORITY_GRANT;
	grant.payload_bytes = RESOURCE_X_DECISION_V4_BYTES; grant.has_acquire_decision = true;
	grant.common = nc_image.common; grant.common.action_node = 1;
	grant.common.sender_connection_generation = 61; grant.common.outcome = RESOURCE_X_OUTCOME_NONE;
	grant.body.acquire_decision = nc_image.body.acquire_image.source.decision;
	NM_REQUIRE(evidence_wire(&grant));
	env.msg_type = RESOURCE_X_MSG_IMAGE_OR_GRANT; env.source_node_id = 0;
	env.dest_node_id = 1; env.epoch = 17; env.payload_length = grant.payload_bytes;
	auth_peer_boot = 31;
	result = gcs_block_resource_x_requester_join_ingress(&env, &grant, 61, &joined);
	printf("# H03 grant-only ingress=%d flags=%u\n", result, joined.flags);
	NM_REQUIRE(result == RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT((joined.flags & RESOURCE_X_REQUESTER_JOIN_HAS_GRANT) != 0);
	UT_ASSERT((joined.flags & (RESOURCE_X_REQUESTER_JOIN_READY | RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE)) == 0);
	result = cluster_bufmgr_pcm_own_acquire_claim_exact(&request_buf, &request_base, request_token,
		RESOURCE_X_ACQUIRE_RESIDENT, &ordinary_active->admission, &ordinary_active->observation,
		&refused_grant, &absent_image, &joined, &claim);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_BAD_STATE);
	request_native_unchanged(&own, state);
	UT_ASSERT(memcmp(disk_shared.data, h03_old.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(complete_io_calls, 0);
	UT_ASSERT(!ResourceOwnerHasBufferIO(CurrentResourceOwner, 1));
	h03_probes++;
	printf("# H03 grant-only E=" UINT64_FORMAT " A=" UINT64_FORMAT
		" old_scn=" UINT64_FORMAT " old_crc=%u claim=%d C=0 IO=0 reads=0\n",
		h03_request.common.assertion_sequence, grant.common.authority_generation, h03_old_scn,
		cluster_gcs_block_compute_checksum(h03_old.data), result);
	return true;
}

static void
h03_deliver(void)
{
	ResourceXRequesterJoinSnapshot joined; ClusterICEnvelope env = {0};
	BackendType previous = MyBackendType;
	UT_ASSERT_EQ(request_shared.rings[1].count, 1);
	UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT_EQ(complete_commits, 0);
	MyBackendType = B_LMS;
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(1), 1);
	MyBackendType = previous;
	if (h03_requests != 1) { UT_ASSERT(false); pg_re_throw(); }
	nc_request = h03_request;
	UT_ASSERT_EQ(h03_request.body.acquire_intent.path, RESOURCE_X_ACQUIRE_RESIDENT);
	UT_ASSERT_EQ(h03_request.common.observed_mode, PCM_STATE_N);
	UT_ASSERT_EQ(request_shared.rings[1].count, 0); UT_ASSERT_EQ(request_shared.rings[1].draining, 0);
	if (!nm_receive(nc_image_fd, RESOURCE_X_MSG_IMAGE_OR_GRANT, &nc_image)) {
		UT_ASSERT(false); pg_re_throw();
	}
	UT_ASSERT_EQ(nc_image.kind, RESOURCE_X_WIRE_IMAGE_ENVELOPE);
	UT_ASSERT_EQ(nc_image.body.acquire_image.source.decision.source_kind, RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
	UT_ASSERT_EQ(nc_image.body.acquire_image.source.decision.source_node, nc_source_node);
	UT_ASSERT(memcmp(nc_image.body.acquire_image.original_image.page_bytes, h03_old.data, BLCKSZ) != 0);
	prepared_episode = h03_request.body.acquire_intent;
	if (!h03_grant_only()) pg_re_throw();
	env.msg_type = RESOURCE_X_MSG_IMAGE_OR_GRANT; env.source_node_id = nc_source_node;
	env.dest_node_id = 1; env.epoch = 17; env.payload_length = nc_image.payload_bytes;
	auth_peer_boot = nc_source_boot;
	if (gcs_block_resource_x_requester_join_ingress(&env, &nc_image, 61, &joined)
		!= RESOURCE_X_APPLY_APPLIED) { UT_ASSERT(false); pg_re_throw(); }
	if (nc_mode == 3)
		UT_ASSERT_EQ(gcs_block_resource_x_requester_join_ingress(&env, &nc_image, 61, &joined), RESOURCE_X_APPLY_DUPLICATE);
}

static void
h03_share(void)
{
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_io_calls, 1);
	UT_ASSERT_EQ(complete_broadcasts, 1); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S); UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_SCUR);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT(memcmp(disk_shared.data, nc_image.body.acquire_image.original_image.page_bytes, BLCKSZ) == 0);
	UT_ASSERT(memcmp(disk_shared.data, h03_old.data, BLCKSZ) != 0);
	UT_ASSERT_EQ(((PageHeader)disk_shared.data)->pd_block_scn, 93);
	UT_ASSERT_EQ(PageGetLSN(disk_shared.data), 12345);
	UT_ASSERT(!ResourceOwnerHasBufferIO(CurrentResourceOwner, 1));
}

static bool
h03_notice(int fd, int mode)
{
	ClusterPcmResourceXMasterState *state = NULL;
	ResourceXDecodedFrame done, consumed; ResourceXIntentSlot staged;
	ClusterICEnvelope env = {0}; ResourceXRequesterJoinSnapshot joined;
#include "test_cluster_k_p130_h03_notice.inc"
}

static bool
h03_parent_from(int fd, int image_fd, int mode, int source_node, uint64 source_boot)
{
	uint64 generation;
	rl_start(1, 2);
	/* request_start resets this session leaf after ordinary_start. The real
	 * ingress must observe the same remote M as the ordinary request driver. */
	fake_gcs_master_node = 0;
	/* Both candidates have valid shape/checksum. Even a higher old SCN is
	 * not current authority or proof of the source's complete lineage. */
	memcpy(disk_shared.data, disk_bytes.data, BLCKSZ);
	((PageHeader)disk_shared.data)->pd_block_scn = h03_old_scn;
	PageSetLSNPreserveOrigin(disk_shared.data, 123);
	disk_shared.data[BLCKSZ - 16] = 0x35;
	((PageHeader)disk_shared.data)->pd_checksum = pg_checksum_page(disk_shared.data, request_buf.tag.blockNum);
	UT_ASSERT(PageIsVerifiedExtended(disk_shared.data, request_buf.tag.blockNum, 0));
	memcpy(h03_old.data, disk_shared.data, BLCKSZ);
	nc_fd = fd; nc_image_fd = image_fd; nc_mode = mode;
	nc_source_node = source_node; nc_source_boot = source_boot;
	auth_observation_hook = nc_auth_peer;
	ordinary_delivery_transport = h03_deliver; rl_on_share = h03_share;
	NM_REQUIRE(rl_run(NULL));
	UT_ASSERT_EQ(rl_share_locks, 1); UT_ASSERT_EQ(rl_waits, 0);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(h03_requests, 1); UT_ASSERT_EQ(h03_probes, 1);
	UT_ASSERT(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(&request_buf), LW_SHARED));
	generation = cluster_pcm_own_gen_get(0);
	rl_actual_unlock(&request_buf);
	NM_REQUIRE(h03_notice(fd, mode));
	/* Consume the resulting durable S through the same ordinary caller. */
	NM_REQUIRE(rl_run(NULL));
	UT_ASSERT_EQ(rl_share_locks, 2); UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), generation);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(h03_requests, 1); UT_ASSERT_EQ(complete_commits, 1);
	rl_actual_unlock(&request_buf);
	printf("# H03 R source=%d boot=" UINT64_FORMAT " backend=%d/" UINT64_FORMAT
		" E=" UINT64_FORMAT " A=" UINT64_FORMAT " base=" UINT64_FORMAT " token=" UINT64_FORMAT
		" epoch=" UINT64_FORMAT " r4=" UINT64_FORMAT " binding=" UINT64_FORMAT
		" old_scn=" UINT64_FORMAT " Icrc=%u Cgen=" UINT64_FORMAT
		" REQUEST=%u C=%u IO=%u shares=%u reads=%u notice=0 ring=0\n", source_node,
		h03_request.body.acquire_intent.requester_incarnation, h03_request.body.acquire_intent.backend_id,
		h03_request.body.acquire_intent.backend_generation, h03_request.common.assertion_sequence,
		nc_image.common.authority_generation, h03_request.body.acquire_intent.base_generation,
		h03_request.body.acquire_intent.reservation_token, h03_request.body.acquire_intent.cluster_epoch,
		h03_request.body.acquire_intent.r4_generation, nc_image.body.acquire_image.source.decision.resource_binding,
		h03_old_scn, cluster_gcs_block_compute_checksum(disk_shared.data), generation,
		h03_requests, complete_commits, complete_io_calls, rl_share_locks, disk_reads);
	UT_ASSERT_EQ(rl_legacy_releases, 0); complete_idle(); return true;
}
static bool h03_parent(int fd, int mode) { return h03_parent_from(fd, fd, mode, 0, 31); }

static bool
h03_send(int fd, uint16 type, const ResourceXDecodedFrame *frame)
{
	if (type == RESOURCE_X_MSG_BLOCK_TO_N && frame->kind == RESOURCE_X_WIRE_BLOCK_TO_N
		&& frame->payload_bytes == RESOURCE_X_DECISION_V4_BYTES) {
		UT_ASSERT_EQ(frame->body.acquire_decision.source_kind, RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
		h03_forwards++;
	}
	if (type == RESOURCE_X_MSG_IMAGE_OR_GRANT && frame->kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE) {
		h03_images++;
		UT_ASSERT(memcmp(frame->body.acquire_image.original_image.page_bytes, h03_source.data, BLCKSZ) == 0);
	}
	if (cluster_node_id == 0 && type == RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE
		&& frame->payload_bytes == RESOURCE_X_SOURCE_COMPLETE_V11_BYTES
		&& frame->common.logical_assertion.requester_node == 1) {
		UT_ASSERT_EQ(h03_forwards, h03_images ? 0 : 1);
		UT_ASSERT_EQ(frame->body.acquire_completion.decision.source_kind, RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
		printf("# H03 M E=" UINT64_FORMAT " A=" UINT64_FORMAT
			" FORWARD=%u source=%d consumed_Cgen=" UINT64_FORMAT "\n",
			frame->common.assertion_sequence, frame->common.authority_generation, h03_forwards,
			frame->body.acquire_completion.decision.source_node,
			frame->body.acquire_completion.native_generation);
	}
	return nm_send(fd, type, frame);
}
static ResourceXApplyResult
h03_capture(HolderFixture *f)
{
	ResourceXApplyResult result;
	h03_source_base = cluster_pcm_own_gen_get(0); h03_captures++;
	result = holder_capture(f);
	if (result == RESOURCE_X_APPLY_APPLIED) memcpy(h03_source.data, disk_shared.data, BLCKSZ);
	return result;
}
static ResourceXApplyResult
h03_finish(HolderFixture *f)
{
	ResourceXApplyResult result;
	h03_finishes++; result = holder_finish(f);
	if (result == RESOURCE_X_APPLY_APPLIED) {
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_image_exact(&f->block.common.logical_assertion,
			&h03_image), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(nt_holder_state(&h03_image, &h03_holder_state));
	}
	return result;
}
static void
h03_census(void)
{
	UT_ASSERT(h03_holder_state != NULL);
	if (h03_holder_state == NULL) return;
	UT_ASSERT_EQ(h03_captures, 1); UT_ASSERT_EQ(h03_finishes, 1); UT_ASSERT_EQ(h03_images, 1);
	UT_ASSERT_EQ(h03_holder_state->holder_image.valid, 0);
	UT_ASSERT_EQ(h03_holder_state->holder_status.valid, 0);
	UT_ASSERT_EQ(h03_holder_state->settlement_retry.pending, 0);
	UT_ASSERT_EQ(h03_holder_state->pi_carrier.transport_refs, 0);
	UT_ASSERT_EQ(h03_holder_state->pi_carrier.pi_refs, 0);
	printf("# H03 H node=%d base=" UINT64_FORMAT " final=" UINT64_FORMAT
		" crc=%u capture=%u finish=%u SHIP=%u pair=0 pending=0 transport=0 PI=0\n",
		cluster_node_id, h03_source_base, cluster_pcm_own_gen_get(0),
		cluster_gcs_block_compute_checksum(h03_source.data), h03_captures, h03_finishes, h03_images);
	holder_no_debt();
}

static void h03_case(bool independent, int mode, uint64 old_scn)
{
	h03_requests = h03_probes = h03_captures = h03_finishes = h03_images = h03_forwards = 0;
	h03_holder_state = NULL; h03_old_scn = old_scn;
	if (independent) nt_run(mode);
	else nm_run_roles(mode, nc_master, h03_parent);
}
UT_TEST(test_k_h03_independent_S_replaces_low_old_current) { h03_case(true, 0, 7); }
UT_TEST(test_k_h03_independent_S_replaces_high_old_watermark) { h03_case(true, 0, 999); }
UT_TEST(test_k_h03_independent_S_duplicate_image_completes_once) { h03_case(true, 3, 7); }
UT_TEST(test_k_h03_independent_S_cleanup_waits_image_callback) { h03_case(true, 4, 999); }
UT_TEST(test_k_h03_colocated_X_replaces_low_old_current) { h03_case(false, 1, 7); }
UT_TEST(test_k_h03_colocated_X_replaces_high_old_watermark) { h03_case(false, 1, 999); }
UT_TEST(test_k_h03_colocated_X_duplicate_image_completes_once) { h03_case(false, 3, 7); }
UT_TEST(test_k_h03_colocated_X_cleanup_waits_image_callback) { h03_case(false, 4, 999); }
int main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
#ifdef USE_ASSERT_CHECKING
	printf("# H03 USE_ASSERT_CHECKING=1\n");
#else
	printf("# H03 USE_ASSERT_CHECKING=0\n");
#endif
	UT_PLAN(8);
	UT_RUN(test_k_h03_independent_S_replaces_low_old_current);
	UT_RUN(test_k_h03_independent_S_replaces_high_old_watermark);
	UT_RUN(test_k_h03_independent_S_duplicate_image_completes_once);
	UT_RUN(test_k_h03_independent_S_cleanup_waits_image_callback);
	UT_RUN(test_k_h03_colocated_X_replaces_low_old_current);
	UT_RUN(test_k_h03_colocated_X_replaces_high_old_watermark);
	UT_RUN(test_k_h03_colocated_X_duplicate_image_completes_once);
	UT_RUN(test_k_h03_colocated_X_cleanup_waits_image_callback);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
