/*-------------------------------------------------------------------------
 * test_cluster_k_p130_h08.c
 *    Drain two cached readers before one native self-source conversion.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_k_p130_h08.c
 * NOTES
 *    Original R/M, native handoff and use owners run. Actor lock contexts,
 *    authenticated sessions, pins and storage remain controlled host leaves.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "storage/lwlock.h"
#include "cluster/cluster_pcm_own.h"
static void h08_error(int code, Datum arg);
static void h08_wait_event(void);
static bool h08_conditional(LWLock *lock, LWLockMode mode);
static ClusterPcmOwnResult h08_handoff(int id, uint64 generation, uint64 token);
#define cluster_test_pcm_clock_gettime cluster_publish_fixture_clock_gettime
#include "test_cluster_k_p130_h08_writer.inc"
#undef cluster_test_pcm_clock_gettime
#define ereport(level, rest) ordinary_report(level)
#define gcs_block_resource_x_payload_candidate native_master_payload_candidate
#define RESOURCE_X_ACQUIRE_REUSE_PCM_FIXTURE
#include "test_cluster_resource_x_master_ack_fixture.inc"
#undef RESOURCE_X_ACQUIRE_REUSE_PCM_FIXTURE
#undef gcs_block_resource_x_payload_candidate
#include "test_cluster_k_p130_h08_master.inc"
#include "test_cluster_k_p130_h08_reader.inc"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_membership.h"
#if defined(K_P130_EXPECT_ASSERT_OFF) && defined(USE_ASSERT_CHECKING)
#error "H08 assertion-OFF requires an assertion-disabled configuration"
#endif

static LWLock *h08_readers[2];
static uint64 h08_generation, h08_token, h08_first_E;
static unsigned h08_handoffs, h08_requests, h08_releases, h08_attempts;
static int h08_fd, h08_order, h08_variant;
static uint32 h08_semantic;
static PGIOAlignedBlock h08_bytes;
static ResourceXDecodedFrame h08_request;
static void rx_local_bytes(void) { memcpy(disk_shared.data, disk_bytes.data, BLCKSZ); }
static void h08_error(int code, Datum arg)
{
	printf("# H08 before-cleanup oldE=" UINT64_FORMAT " phase=%u purpose=%u has_intent=%u handoff=%u flags=%u gen=" UINT64_FORMAT " token=" UINT64_FORMAT " pauses=%u REQUEST=%u C=%u\n",
		request_entry()->resource_x_bootstrap_round.request.assertion_sequence,
		request_entry()->resource_x_bootstrap_round.phase,
		request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		request_entry()->resource_x_bootstrap_round.has_acquire_intent,
		request_entry()->resource_x_bootstrap_round.acquire_read_handoff,
		cluster_pcm_own_flags_get(0), cluster_pcm_own_gen_get(0),
		cluster_pcm_own_reservation_token_get(0), ordinary_pauses, h08_requests, complete_commits - 1);
	cluster_bufmgr_first_fill_error(code, arg);
}


/* Parking an actor does not unlock it. Only the original reader unlock
 * removes a hold observed by the writer's controlled conditional leaf. */
static void h08_park_reader(int reader)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 1);
	UT_ASSERT(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(&request_buf), LW_SHARED));
	h08_readers[reader] = fake_lwlock_stack[0];
	fake_lwlock_stack[0] = NULL; fake_lwlock_depth = 0; fake_lwlock_held = NULL;
}
static void h08_release_reader(int reader)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0); UT_ASSERT(h08_readers[reader] != NULL);
	fake_lwlock_stack[0] = h08_readers[reader]; fake_lwlock_mode_stack[0] = LW_SHARED;
	fake_lwlock_depth = 1; fake_lwlock_held = h08_readers[reader]; fake_lwlock_mode = LW_SHARED;
	rl_actual_unlock(&request_buf);
	UT_ASSERT_EQ(fake_lwlock_depth, 0); h08_readers[reader] = NULL; h08_releases++;
}
static bool h08_conditional(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(&request_buf)); UT_ASSERT_EQ(mode, LW_EXCLUSIVE);
	UT_ASSERT_EQ(fake_lwlock_depth, 0); h08_attempts++;
	if (h08_readers[0] != NULL || h08_readers[1] != NULL) return false;
	return LWLockConditionalAcquire(lock, mode);
}
static void h08_wait_event(void)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	if (wc_waits < 1 || wc_waits > 2) { UT_ASSERT(false); pg_re_throw(); }
	UT_ASSERT_EQ(h08_requests, 0); UT_ASSERT_EQ(h08_handoffs, 0);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(complete_commits, 1);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), h08_generation);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), h08_token);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(request_pins, 1);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&request_buf.state)), 3);
	h08_release_reader((wc_waits - 1) ^ h08_order);
	printf("# H08 reader=%u released=%u remaining=%u token=" UINT64_FORMAT " flags=0 REQUEST=0 C=0 refcount=3\n",
		(unsigned)((wc_waits - 1) ^ h08_order), h08_releases, 2 - h08_releases, h08_token);
}
static ClusterPcmOwnResult h08_handoff(int id, uint64 generation, uint64 token)
{
	ClusterPcmOwnResult result;
	UT_ASSERT_EQ(id, 0); UT_ASSERT_EQ(generation, h08_generation); UT_ASSERT_EQ(token, h08_token + 1);
	UT_ASSERT_EQ(h08_releases, 2); UT_ASSERT_EQ(h08_requests, 0);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(id), PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(id), token);
	UT_ASSERT(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE));
	result = cluster_pcm_own_revoke_to_grant_handoff_exact(id, generation, token);
	UT_ASSERT_EQ(result, CLUSTER_PCM_OWN_OK); h08_handoffs++;
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(id), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(id), token);
	return result;
}
static ClusterICSendResult h08_send(uint8 type, int32 node, const void *bytes, uint32 length, const ResourceXIntentSlot *intent)
{
	uint16 header[2] = {type, (uint16)length}; ResourceXWireReject reject;
	UT_ASSERT_EQ(node, 0); UT_ASSERT_EQ(type, RESOURCE_X_MSG_ASSERT_X);
	UT_ASSERT_EQ(length, RESOURCE_X_ACQUIRE_V3_BYTES);
	UT_ASSERT(cluster_pcm_lock_resource_x_requester_send_current_exact(intent));
	if (!cluster_resource_x_wire_decode(type, bytes, length, &h08_request, &reject)) return CLUSTER_IC_SEND_HARD_ERROR;
	h08_requests++;
	return nm_transfer(h08_fd, header, sizeof(header), true)
		&& nm_transfer(h08_fd, (void *)bytes, length, true) ? CLUSTER_IC_SEND_DONE : CLUSTER_IC_SEND_HARD_ERROR;
}
static bool h08_peer(int32 node, uint32 capability, uint32 generation)
{
	UT_ASSERT_EQ(node, 0); UT_ASSERT_EQ(capability, PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1);
	return node == 0 && generation == 51;
}
#define cluster_ic_tier1_send_resource_x_intent h08_send
#define cluster_sf_peer_capability_generation_matches h08_peer
#include "test_cluster_k_p130_h01_drain.inc"
#undef cluster_sf_peer_capability_generation_matches
#undef cluster_ic_tier1_send_resource_x_intent
#include "test_cluster_k_p130_h02_leaves.inc"

static void h08_deliver(void)
{
	ClusterICEnvelope env = {0}; ResourceXRequesterJoinSnapshot joined;
	BackendType previous = MyBackendType;
	UT_ASSERT_EQ(h08_releases, 2); UT_ASSERT_EQ(h08_handoffs, 1);
	MyBackendType = B_LMS; UT_ASSERT_EQ(cluster_lms_outbound_drain_send(1), 1); MyBackendType = previous;
	UT_ASSERT_EQ(h08_requests, 1);
	UT_ASSERT_EQ(h08_request.common.observed_mode, PCM_STATE_S); UT_ASSERT_EQ(h08_request.common.target_mode, PCM_STATE_X);
	UT_ASSERT_EQ(h08_request.body.acquire_intent.path, RESOURCE_X_ACQUIRE_RESIDENT);
	UT_ASSERT(h08_request.common.assertion_sequence > h08_first_E);
	UT_ASSERT_EQ(h08_request.body.acquire_intent.base_generation, h08_generation);
	UT_ASSERT_EQ(h08_request.body.acquire_intent.reservation_token, h08_token + 1);
	if (!nm_receive(h08_fd, RESOURCE_X_MSG_IMAGE_OR_GRANT, &nm_grant)) { UT_ASSERT(false); pg_re_throw(); }
	UT_ASSERT_EQ(nm_grant.body.acquire_decision.source_kind, RESOURCE_X_ACQUIRE_SOURCE_LOCAL_CURRENT);
	UT_ASSERT_EQ(nm_grant.body.acquire_decision.source_node, 1);
	UT_ASSERT_EQ(nm_grant.body.acquire_decision.holders_bitmap, 2);
	UT_ASSERT_EQ(nm_grant.body.acquire_decision.required_holders_bitmap, 0);
	UT_ASSERT(memcmp(h08_bytes.data, disk_shared.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_io_calls, 1); UT_ASSERT_EQ(disk_reads, 1);
	prepared_episode = h08_request.body.acquire_intent;
	input_observation = ordinary_active->observation; input_grant = nm_grant;
	env.msg_type = RESOURCE_X_MSG_IMAGE_OR_GRANT; env.source_node_id = 0; env.dest_node_id = 1;
	env.epoch = 17; env.payload_length = nm_grant.payload_bytes; auth_peer_boot = 31;
	UT_ASSERT_EQ(gcs_block_resource_x_requester_join_ingress(&env, &nm_grant, 61, &joined), RESOURCE_X_APPLY_APPLIED);
	if (h08_variant == 2) UT_ASSERT_EQ(gcs_block_resource_x_requester_join_ingress(&env, &nm_grant, 61, &joined), RESOURCE_X_APPLY_DUPLICATE);
}
static bool h08_master(int fd, int variant)
{
	PublishFixture f; ResourceXDecodedFrame request, grant, done, notice; ResourceXWireReject reject;
	NM_REQUIRE(nm_master(fd, 0));
	NM_REQUIRE(nm_receive(fd, RESOURCE_X_MSG_ASSERT_X, &request)); NM_REQUIRE(nm_master_admit(&f, &request));
	grant = publish_decode(f.state->grant_intent.payload, false);
	UT_ASSERT_EQ(grant.body.acquire_decision.source_kind, RESOURCE_X_ACQUIRE_SOURCE_LOCAL_CURRENT);
	UT_ASSERT_EQ(grant.body.acquire_decision.required_holders_bitmap, 0); UT_ASSERT_EQ(grant.body.acquire_decision.holders_bitmap, 2);
	if (variant != 1) NM_REQUIRE(nm_master_callback(&f.state->grant_intent.slot, true));
	NM_REQUIRE(nm_send(fd, RESOURCE_X_MSG_IMAGE_OR_GRANT, &grant));
	NM_REQUIRE(nm_receive(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done));
	NM_REQUIRE(nm_master_done(&done) == RESOURCE_X_APPLY_APPLIED);
	if (variant == 1) {
		UT_ASSERT(f.state->deferred_settlement.present != 0);
		NM_REQUIRE(nm_master_callback(&f.state->grant_intent.slot, true));
		NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_done_resume_exact(&f.work) == RESOURCE_X_APPLY_APPLIED);
	}
	if (variant == 2) NM_REQUIRE(nm_master_done(&done) == RESOURCE_X_APPLY_DUPLICATE);
	NM_REQUIRE(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		f.state->grant_intent.payload, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &notice, &reject));
	NM_REQUIRE(nm_send(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &notice)); NM_REQUIRE(nm_master_callback(&f.state->grant_intent.slot, true));
	UT_ASSERT_EQ(f.state->requests[1].phase, RESOURCE_X_MASTER_NONE); UT_ASSERT_EQ(f.state->deferred_settlement.present, 0);
	printf("# H08 M E=" UINT64_FORMAT " A=" UINT64_FORMAT " source=1 required=0 head=0 pending=0 callback=0\n",
		request.common.assertion_sequence, grant.common.authority_generation);
	return true;
}
static bool h08_parent(int fd, int variant)
{
	ClusterPcmXWriterLedgerEntry *entry; ClusterPcmResourceXMasterState *state = NULL;
	ResourceXIntentSlot staged; ResourceXDecodedFrame done, notice;
	ResourceXWireReject reject; ClusterICEnvelope env = {0}; ResourceXRequesterJoinSnapshot joined;
	NM_REQUIRE(nm_parent_as(fd, 0, 1));
	h08_first_E = nm_grant.common.assertion_sequence;
	h08_generation = cluster_pcm_own_gen_get(0); h08_token = cluster_pcm_own_reservation_token_get(0);
	memcpy(h08_bytes.data, disk_shared.data, BLCKSZ); h08_fd = fd; h08_variant = variant;
	h08_handoffs = h08_requests = h08_releases = h08_attempts = 0; h08_readers[0] = h08_readers[1] = NULL;
	wc_legacy = wc_failures = wc_waits = wc_object_checks = 0;
	wc_object_enabled = false; wc_object_valid = true; wc_r4 = 77; wc_path = RESOURCE_X_WRITER_TARGET;
	wc_session_id = 31; wc_master = 0; wc_unavailable = wc_pauses = 0; wc_cancel_pause = false;
	rx_barrier = rx_cancel_wait = false; rx_native_boundary = NULL;
	MemSet(cluster_bufmgr_pcm_x_writer_ledger, 0, sizeof(cluster_bufmgr_pcm_x_writer_ledger));
	rl_waits = rl_share_locks = rl_service_wakes = 0; rl_barrier = rl_cancel_wait = false; rl_on_wait = rl_on_share = rl_after_deliver = NULL;
	cluster_gcs_block_local_cache = cluster_enabled = cluster_shared_config = true;
	h08_semantic = BM_TAG_VALID | BM_VALID;
	if (variant == 1) { h08_semantic |= BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED; pg_atomic_fetch_or_u32(&request_buf.state, h08_semantic); }
	/* Keep the explicit pin provider at three passive pins, including the
	 * writer's original private pin, throughout the conversion. */
	pg_atomic_fetch_add_u32(&request_buf.state, 2 * BUF_REFCOUNT_ONE);
	for (int reader = 0; reader < 2; reader++) { NM_REQUIRE(rl_run(NULL)); h08_park_reader(reader); }
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(rl_share_locks, 2); ordinary_delivery_transport = h08_deliver;
	entry = rx_prepare(NULL);
	printf("# H08 prepare entry=%d waits=%u attempts=%u handoff=%u issued=%u sends=%u native=%u type=%u flags=%u gen=" UINT64_FORMAT " token=" UINT64_FORMAT " failures=%u PANIC=%u\n",
		entry != NULL, wc_waits, h08_attempts, h08_handoffs, h08_requests, ordinary_sends,
		request_buf.pcm_state, request_buf.buffer_type, cluster_pcm_own_flags_get(0),
		cluster_pcm_own_gen_get(0), cluster_pcm_own_reservation_token_get(0), wc_failures, ordinary_panics);
	UT_ASSERT_EQ(h08_handoffs, 1); UT_ASSERT_EQ(h08_releases, 2);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), h08_token + 1);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); UT_ASSERT_EQ(ordinary_panics, 0);
	UT_ASSERT(memcmp(h08_bytes.data, disk_shared.data, BLCKSZ) == 0);
	NM_REQUIRE(entry != NULL);
	UT_ASSERT_EQ(wc_legacy, 0); UT_ASSERT_EQ(wc_failures, 0); UT_ASSERT_EQ(wc_waits, 2);
	UT_ASSERT_EQ(h08_handoffs, 1); UT_ASSERT_EQ(h08_requests, 1); UT_ASSERT_EQ(h08_attempts, 3);
	UT_ASSERT_EQ(h08_releases, 2); UT_ASSERT_EQ(rl_service_wakes, 2);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), h08_generation + 1); UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), h08_token + 1);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X); UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_XCUR);
	UT_ASSERT_EQ(complete_commits, 2); UT_ASSERT_EQ(complete_io_calls, 1); UT_ASSERT_EQ(disk_reads, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & CLUSTER_PCM_OWN_SEMANTIC_BUF_STATE_MASK, h08_semantic);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&request_buf.state)), 3); UT_ASSERT(memcmp(h08_bytes.data, disk_shared.data, BLCKSZ) == 0);
	LWLockAcquire(entry->content_lock, LW_EXCLUSIVE); NM_REQUIRE(cluster_bufmgr_pcm_x_writer_activate(entry, true));
	cluster_bufmgr_pcm_x_writer_mark_releasing(entry); LWLockRelease(entry->content_lock); cluster_bufmgr_pcm_x_writer_release(entry);
	UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(&request_buf) == NULL);
	NM_REQUIRE(nm_requester_notice(&state));
	NM_REQUIRE(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		state->requester_settlement_intent.payload, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &done, &reject));
	NM_REQUIRE(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
		&state->requester_settlement_intent.slot, ++fake_pcm_clock_us, &staged) == RESOURCE_X_INTENT_STAGED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, ++fake_pcm_clock_us));
	NM_REQUIRE(nm_send(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done)); NM_REQUIRE(nm_receive(fd, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &notice));
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE; env.source_node_id = 0; env.dest_node_id = 1;
	env.epoch = 17; env.payload_length = notice.payload_bytes; auth_peer_boot = 31;
	NM_REQUIRE(gcs_block_resource_x_requester_join_ingress(&env, &notice, 61, &joined) == RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, RESOURCE_X_DELIVERY_ACQUIRE_READ_SETTLED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.pending, 0);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.writer_activation_token), 0); UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.resource_x_activation_generation), 0);
	UT_ASSERT_EQ(ordinary_panics, 0); UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	printf("# H08 R order=%d variant=%d oldE=" UINT64_FORMAT " E=" UINT64_FORMAT " A=" UINT64_FORMAT
		" base=" UINT64_FORMAT " gen=" UINT64_FORMAT " token=" UINT64_FORMAT " crc=%u"
		" readers=2 releases=2 waits=2 handoff=1 REQUEST=1 C=1 IO=0 reads=0 refcount=3 notice=0 ledger=0\n",
		h08_order, variant, h08_first_E, h08_request.common.assertion_sequence, nm_grant.common.authority_generation,
		h08_generation, cluster_pcm_own_gen_get(0), cluster_pcm_own_reservation_token_get(0), cluster_gcs_block_compute_checksum(disk_shared.data));
	complete_idle(); return true;
}
static void h08_case(int order, int variant) { h08_order = order; nm_run_roles(variant, h08_master, h08_parent); }
UT_TEST(test_k_h08_clean_readers_01_one_self_conversion) { h08_case(0, 0); }
UT_TEST(test_k_h08_clean_readers_10_one_self_conversion) { h08_case(1, 0); }
UT_TEST(test_k_h08_dirty_readers_01_done_before_callback) { h08_case(0, 1); }
UT_TEST(test_k_h08_dirty_readers_10_done_before_callback) { h08_case(1, 1); }
UT_TEST(test_k_h08_duplicate_grant_readers_01_one_C) { h08_case(0, 2); }
UT_TEST(test_k_h08_duplicate_grant_readers_10_one_C) { h08_case(1, 2); }
int main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
#ifdef USE_ASSERT_CHECKING
	printf("# H08 USE_ASSERT_CHECKING=1\n");
#else
	printf("# H08 USE_ASSERT_CHECKING=0\n");
#endif
	UT_PLAN(6);
	UT_RUN(test_k_h08_clean_readers_01_one_self_conversion);
	UT_RUN(test_k_h08_clean_readers_10_one_self_conversion);
	UT_RUN(test_k_h08_dirty_readers_01_done_before_callback);
	UT_RUN(test_k_h08_dirty_readers_10_done_before_callback);
	UT_RUN(test_k_h08_duplicate_grant_readers_01_one_C);
	UT_RUN(test_k_h08_duplicate_grant_readers_10_one_C);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
