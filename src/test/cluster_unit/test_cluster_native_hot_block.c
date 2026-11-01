/*-------------------------------------------------------------------------
 * test_cluster_native_hot_block.c
 *    Drain eight independently owned cached readers before one writer conversion.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_native_hot_block.c
 * NOTES
 *    Original R/M, native handoff and use owners run. Actor lock contexts,
 *    authenticated sessions, pins and storage remain controlled host leaves.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "storage/lwlock.h"
#include "cluster/cluster_pcm_own.h"
static void hb_error(int code, Datum arg);
static void hb_wait_event(void);
static bool hb_conditional(LWLock *lock, LWLockMode mode);
static ClusterPcmOwnResult hb_handoff(int id, uint64 generation, uint64 token);
#define cluster_test_pcm_clock_gettime cluster_publish_fixture_clock_gettime
#include "test_cluster_native_hot_block_writer.inc"
#undef cluster_test_pcm_clock_gettime
#define ereport(level, rest) ordinary_report(level)
#define gcs_block_resource_x_payload_candidate native_master_payload_candidate
#define RESOURCE_X_ACQUIRE_REUSE_PCM_FIXTURE
#include "test_cluster_resource_x_master_ack_fixture.inc"
#undef RESOURCE_X_ACQUIRE_REUSE_PCM_FIXTURE
#undef gcs_block_resource_x_payload_candidate
#include "test_cluster_native_hot_block_master.inc"
#include "test_cluster_native_hot_block_reader.inc"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_membership.h"
#if defined(NATIVE_HOT_EXPECT_ASSERT_OFF) && defined(USE_ASSERT_CHECKING)
#error "hot-block assertion-OFF requires an assertion-disabled configuration"
#endif

#define pfree(pointer) free(pointer)
#include "test_cluster_native_hot_block_owner.inc"
#undef pfree

static unsigned hb_output_writes, hb_output_wakes;
static uint64 hb_output_generation;
static void hb_io_wait(BufferDesc *buf)
{
 (void)buf; UT_ASSERT(false); pg_re_throw();
}
static void hb_io_wake(ConditionVariable *cv)
{
 UT_ASSERT(cv == BufferDescriptorGetIOCV(&request_buf));
 UT_ASSERT(!ResourceOwnerHasBufferIO(CurrentResourceOwner, 1));
 UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & BM_IO_IN_PROGRESS, 0);
 hb_output_wakes++; ConditionVariableBroadcast(cv);
}
#define LockBufHdr stage_header_lock
#define StartBufferIO hb_io_begin
#define TerminateBufferIO hb_io_end
#define WaitIO hb_io_wait
#define ConditionVariableBroadcast hb_io_wake
#define cluster_lever_h_note_pi_implicit_discard() UT_ASSERT(false)
#define ereport(level, rest) ordinary_report(level)
#include "test_cluster_native_hot_block_io.inc"
#undef ereport
#undef cluster_lever_h_note_pi_implicit_discard
#undef ConditionVariableBroadcast
#undef WaitIO
#undef TerminateBufferIO
#undef StartBufferIO
#undef LockBufHdr

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


enum { HB_READERS = 8 };
static LWLock *hb_readers[HB_READERS];
static PGPROC hb_procs[HB_READERS];
static ResourceOwner hb_owners[HB_READERS];
static PGPROC *hb_writer_proc;
static ResourceOwner hb_writer_owner;
static int hb_writer_pid;
static BackendId hb_writer_backend;
static unsigned hb_errors;
static bool hb_writeback;
static void hb_output_begin(void);
static void hb_output_end(void);
static uint64 hb_generation, hb_token, hb_first_E;
static unsigned hb_handoffs, hb_requests, hb_releases, hb_attempts;
static int hb_fd, hb_order, hb_variant;
static uint32 hb_semantic;
static PGIOAlignedBlock hb_bytes;
static ResourceXDecodedFrame hb_request;
static void rx_local_bytes(void) { memcpy(disk_shared.data, disk_bytes.data, BLCKSZ); }
static void hb_error(int code, Datum arg)
{
	printf("# hot-block before-cleanup oldE=" UINT64_FORMAT " phase=%u purpose=" UINT64_FORMAT " has_intent=%u handoff=%u flags=%u gen=" UINT64_FORMAT " token=" UINT64_FORMAT " pauses=%u REQUEST=%u C=%u\n",
		request_entry()->resource_x_bootstrap_round.request.assertion_sequence,
		request_entry()->resource_x_bootstrap_round.phase,
		request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		request_entry()->resource_x_bootstrap_round.has_acquire_intent,
		request_entry()->resource_x_bootstrap_round.acquire_read_handoff,
		cluster_pcm_own_flags_get(0), cluster_pcm_own_gen_get(0),
		cluster_pcm_own_reservation_token_get(0), ordinary_pauses, hb_requests, complete_commits - 1);
	cluster_bufmgr_first_fill_error(code, arg);
}


/* Parking an actor does not unlock it. Only the original reader unlock
 * removes a hold observed by the writer's controlled conditional leaf. */
static void hb_writer_context(void)
{
 MyProc = hb_writer_proc; MyProcPid = hb_writer_pid;
 MyBackendId = hb_writer_backend; CurrentResourceOwner = hb_writer_owner;
 request_pins = 1;
}
static void hb_reader_context(int reader)
{
 MyProc = &hb_procs[reader]; MyProcPid = MyProc->pid;
 MyBackendId = MyProc->backendId; CurrentResourceOwner = hb_owners[reader];
 request_pins = 1;
}
static unsigned hb_held(void)
{
 unsigned held = 0;
 for (int i = 0; i < HB_READERS; i++) held += hb_readers[i] != NULL;
 return held;
}
/* Park just the scheduler's lock context. The actor still owns this lock. */
static void hb_park_reader(int reader)
{
 UT_ASSERT_EQ(fake_lwlock_depth, 1);
 UT_ASSERT(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(&request_buf), LW_SHARED));
 hb_readers[reader] = fake_lwlock_stack[0];
 fake_lwlock_stack[0] = NULL; fake_lwlock_depth = 0; fake_lwlock_held = NULL;
 hb_writer_context();
}
static void hb_release_reader(int reader)
{
 ClusterPcmOwnEntry before = request_own;
 volatile bool caught = false;
 UT_ASSERT_EQ(fake_lwlock_depth, 0); UT_ASSERT(hb_readers[reader] != NULL);
 hb_reader_context(reader);
 fake_lwlock_stack[0] = hb_readers[reader]; fake_lwlock_mode_stack[0] = LW_SHARED;
 fake_lwlock_depth = 1; fake_lwlock_held = hb_readers[reader]; fake_lwlock_mode = LW_SHARED;
 /* The scheduler supplies an ERROR in one completed cached-reader scope.
  * Its original unlock must not adopt the output IO or writer's episode. */
 PG_TRY(); {
  PG_TRY(); {
   if (hb_variant == 4 && reader == (hb_order ? 4 : 3)) {
	UT_ASSERT(ResourceOwnerHasBufferIO(hb_writer_owner, 1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), hb_output_generation + 1);
	pg_re_throw();
   }
  } PG_FINALLY(); {
   rl_actual_unlock(&request_buf);
  } PG_END_TRY();
 } PG_CATCH(); { caught = true; } PG_END_TRY();
 UT_ASSERT_EQ(caught, hb_variant == 4 && reader == (hb_order ? 4 : 3));
 if (caught) hb_errors++;
 UT_ASSERT_EQ(fake_lwlock_depth, 0);
 UT_ASSERT(memcmp(&before, &request_own, sizeof(before)) == 0);
 UT_ASSERT(!ResourceOwnerHasBufferIO(CurrentResourceOwner, 1));
 /* Private/shared pins are controlled provider leaves, as in H08. */
 UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_fetch_sub_u32(&request_buf.state,
  BUF_REFCOUNT_ONE)), HB_READERS + 1 - hb_releases);
 request_pins = 0; hb_readers[reader] = NULL; hb_releases++;
 hb_writer_context();
 ResourceOwnerDelete(hb_owners[reader]); hb_owners[reader] = NULL;
}
static bool hb_conditional(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(&request_buf)); UT_ASSERT_EQ(mode, LW_EXCLUSIVE);
	UT_ASSERT_EQ(fake_lwlock_depth, 0); hb_attempts++;
	if (hb_writeback) {
		UT_ASSERT_EQ(hb_output_wakes, 1);
		UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), hb_output_generation + 2);
		UT_ASSERT(!ResourceOwnerHasBufferIO(hb_writer_owner, 1));
	}
	if (hb_held() != 0) return false;
	return LWLockConditionalAcquire(lock, mode);
}
static void hb_output_begin(void)
{
 hb_output_writes = hb_output_wakes = 0;
 hb_output_generation = pg_atomic_read_u64(&request_own.write_io_generation);
 UT_ASSERT_EQ(hb_output_generation & 1, 0);
 UT_ASSERT(hb_io_begin(&request_buf, false));
 UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), hb_output_generation + 1);
 UT_ASSERT(ResourceOwnerHasBufferIO(hb_writer_owner, 1));
}
static void hb_output_end(void)
{
 PGAlignedBlock copy, actual;
 uint16 shared_checksum = ((PageHeader)disk_shared.data)->pd_checksum;
 UT_ASSERT_EQ(hb_held(), 4);
 UT_ASSERT(ResourceOwnerHasBufferIO(hb_writer_owner, 1));
 UT_ASSERT_EQ(hb_requests, 0); UT_ASSERT_EQ(hb_handoffs, 0);
 /* Storage completion is an OS leaf. Original Start/TerminateBufferIO own
  * the live IO bits, writer generation, ResourceOwner and notification. */
 LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_SHARED);
 memcpy(copy.data, disk_shared.data, BLCKSZ);
 PageSetChecksumInplace(copy.data, request_buf.tag.blockNum);
 UT_ASSERT_EQ(pwrite(fileno(disk_file), copy.data, BLCKSZ, 0), BLCKSZ);
 hb_output_writes++;
 pg_atomic_fetch_and_u32(&request_buf.state, ~BM_JUST_DIRTIED);
 hb_io_end(&request_buf, true, 0);
 LWLockRelease(BufferDescriptorGetContentLock(&request_buf));
 UT_ASSERT_EQ(pread(fileno(disk_file), actual.data, BLCKSZ, 0), BLCKSZ);
 UT_ASSERT(memcmp(copy.data, actual.data, BLCKSZ) == 0);
 UT_ASSERT(PageIsVerifiedExtended(actual.data, request_buf.tag.blockNum, 0));
 UT_ASSERT_EQ(((PageHeader)disk_shared.data)->pd_checksum, shared_checksum);
 UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), hb_output_generation + 2);
 UT_ASSERT_EQ(hb_output_writes, 1); UT_ASSERT_EQ(hb_output_wakes, 1);
 UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_IO_IN_PROGRESS | BM_DIRTY), 0);
}
static void hb_wait_event(void)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	if (wc_waits < 1 || wc_waits > HB_READERS) { UT_ASSERT(false); pg_re_throw(); }
	UT_ASSERT_EQ(hb_requests, 0); UT_ASSERT_EQ(hb_handoffs, 0);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(complete_commits, 1);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), hb_generation);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), hb_token);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(request_pins, 1);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&request_buf.state)), HB_READERS + 1 - hb_releases);
	/* The first five scheduled waits belong to the live output IO. The
	 * original caller must not even enter content-lock conversion yet. */
	if (hb_writeback && wc_waits <= 5) UT_ASSERT_EQ(hb_attempts, 0);
	if (hb_writeback && wc_waits == 5) hb_output_end();
 hb_release_reader(hb_order ? HB_READERS - wc_waits : wc_waits - 1);
	printf("# hot-block reader=%u released=%u remaining=%u token=" UINT64_FORMAT " flags=0 REQUEST=0 C=0\n",
		(unsigned)(hb_order ? HB_READERS - wc_waits : wc_waits - 1), hb_releases, HB_READERS - hb_releases, hb_token);
}
static ClusterPcmOwnResult hb_handoff(int id, uint64 generation, uint64 token)
{
	ClusterPcmOwnResult result;
	UT_ASSERT_EQ(id, 0); UT_ASSERT_EQ(generation, hb_generation); UT_ASSERT_EQ(token, hb_token + 1);
	UT_ASSERT_EQ(hb_releases, HB_READERS); UT_ASSERT_EQ(hb_requests, 0);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(id), PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(id), token);
	UT_ASSERT(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE));
	result = cluster_pcm_own_revoke_to_grant_handoff_exact(id, generation, token);
	UT_ASSERT_EQ(result, CLUSTER_PCM_OWN_OK); hb_handoffs++;
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(id), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(id), token);
	return result;
}
static ClusterICSendResult hb_send(uint8 type, int32 node, const void *bytes, uint32 length, const ResourceXIntentSlot *intent)
{
	uint16 header[2] = {type, (uint16)length}; ResourceXWireReject reject;
	UT_ASSERT_EQ(node, 0); UT_ASSERT_EQ(type, RESOURCE_X_MSG_ASSERT_X);
	UT_ASSERT_EQ(length, RESOURCE_X_ACQUIRE_V3_BYTES);
	UT_ASSERT(cluster_pcm_lock_resource_x_requester_send_current_exact(intent));
	if (!cluster_resource_x_wire_decode(type, bytes, length, &hb_request, &reject)) return CLUSTER_IC_SEND_HARD_ERROR;
	hb_requests++;
	return nm_transfer(hb_fd, header, sizeof(header), true)
		&& nm_transfer(hb_fd, (void *)bytes, length, true) ? CLUSTER_IC_SEND_DONE : CLUSTER_IC_SEND_HARD_ERROR;
}
static bool hb_peer(int32 node, uint32 capability, uint32 generation)
{
	UT_ASSERT_EQ(node, 0); UT_ASSERT_EQ(capability, PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1);
	return node == 0 && generation == 51;
}
#define cluster_ic_tier1_send_resource_x_intent hb_send
#define cluster_sf_peer_capability_generation_matches hb_peer
#include "test_cluster_native_hot_block_drain.inc"
#undef cluster_sf_peer_capability_generation_matches
#undef cluster_ic_tier1_send_resource_x_intent


static void hb_deliver(void)
{
	ClusterICEnvelope env = {0}; ResourceXRequesterJoinSnapshot joined;
	BackendType previous = MyBackendType;
	UT_ASSERT_EQ(hb_releases, HB_READERS); UT_ASSERT_EQ(hb_handoffs, 1);
	MyBackendType = B_LMS; UT_ASSERT_EQ(cluster_lms_outbound_drain_send(1), 1); MyBackendType = previous;
	UT_ASSERT_EQ(hb_requests, 1);
	UT_ASSERT_EQ(hb_request.common.observed_mode, PCM_STATE_S); UT_ASSERT_EQ(hb_request.common.target_mode, PCM_STATE_X);
	UT_ASSERT_EQ(hb_request.body.acquire_intent.path, RESOURCE_X_ACQUIRE_RESIDENT);
	UT_ASSERT(hb_request.common.assertion_sequence > hb_first_E);
	UT_ASSERT_EQ(hb_request.body.acquire_intent.base_generation, hb_generation);
	UT_ASSERT_EQ(hb_request.body.acquire_intent.reservation_token, hb_token + 1);
	if (!nm_receive(hb_fd, RESOURCE_X_MSG_IMAGE_OR_GRANT, &nm_grant)) { UT_ASSERT(false); pg_re_throw(); }
	UT_ASSERT_EQ(nm_grant.body.acquire_decision.source_kind, RESOURCE_X_ACQUIRE_SOURCE_LOCAL_CURRENT);
	UT_ASSERT_EQ(nm_grant.body.acquire_decision.source_node, 1);
	UT_ASSERT_EQ(nm_grant.body.acquire_decision.holders_bitmap, 2);
	UT_ASSERT_EQ(nm_grant.body.acquire_decision.required_holders_bitmap, 0);
	UT_ASSERT(memcmp(hb_bytes.data, disk_shared.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_io_calls, 1); UT_ASSERT_EQ(disk_reads, 1);
	prepared_episode = hb_request.body.acquire_intent;
	input_observation = ordinary_active->observation; input_grant = nm_grant;
	env.msg_type = RESOURCE_X_MSG_IMAGE_OR_GRANT; env.source_node_id = 0; env.dest_node_id = 1;
	env.epoch = 17; env.payload_length = nm_grant.payload_bytes; auth_peer_boot = 31;
	UT_ASSERT_EQ(gcs_block_resource_x_requester_join_ingress(&env, &nm_grant, 61, &joined), RESOURCE_X_APPLY_APPLIED);
	if (hb_variant == 2) UT_ASSERT_EQ(gcs_block_resource_x_requester_join_ingress(&env, &nm_grant, 61, &joined), RESOURCE_X_APPLY_DUPLICATE);
}
static bool hb_master(int fd, int variant)
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
	printf("# hot-block M E=" UINT64_FORMAT " A=" UINT64_FORMAT " source=1 required=0 head=0 pending=0 callback=0\n",
		request.common.assertion_sequence, grant.common.authority_generation);
	return true;
}
static bool hb_parent(int fd, int variant)
{
	ClusterPcmXWriterLedgerEntry *entry; ClusterPcmResourceXMasterState *state = NULL;
	ResourceXIntentSlot staged; ResourceXDecodedFrame done, notice;
	ResourceXWireReject reject; ClusterICEnvelope env = {0}; ResourceXRequesterJoinSnapshot joined;
	NM_REQUIRE(nm_parent_as(fd, 0, 1));
	hb_first_E = nm_grant.common.assertion_sequence;
	hb_generation = cluster_pcm_own_gen_get(0); hb_token = cluster_pcm_own_reservation_token_get(0);
	memcpy(hb_bytes.data, disk_shared.data, BLCKSZ); hb_fd = fd; hb_variant = variant;
	hb_handoffs = hb_requests = hb_releases = hb_attempts = hb_errors = 0;
 hb_output_writes = hb_output_wakes = 0;
 memset(hb_readers, 0, sizeof(hb_readers));
 hb_writer_proc = MyProc; hb_writer_pid = MyProcPid; hb_writer_backend = MyBackendId;
 hb_writer_owner = CurrentResourceOwner; hb_writeback = variant >= 3;
	wc_legacy = wc_failures = wc_waits = wc_object_checks = 0;
	wc_object_enabled = false; wc_object_valid = true; wc_r4 = 77; wc_path = RESOURCE_X_WRITER_TARGET;
	wc_session_id = 31; wc_master = 0; wc_unavailable = wc_pauses = 0; wc_cancel_pause = false;
	rx_barrier = rx_cancel_wait = false; rx_native_boundary = NULL;
	MemSet(cluster_bufmgr_pcm_x_writer_ledger, 0, sizeof(cluster_bufmgr_pcm_x_writer_ledger));
	rl_waits = rl_share_locks = rl_service_wakes = 0; rl_barrier = rl_cancel_wait = false; rl_on_wait = rl_on_share = rl_after_deliver = NULL;
	cluster_gcs_block_local_cache = cluster_enabled = cluster_shared_config = true;
	hb_semantic = BM_TAG_VALID | BM_VALID;
	if (variant == 1) { hb_semantic |= BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED; pg_atomic_fetch_or_u32(&request_buf.state, hb_semantic); }
	/* Every cached reader enters the real caller with its own native owner.
  * The scheduler parks that context while the common descriptor remains. */
 if (hb_writeback) {
  pg_atomic_fetch_or_u32(&request_buf.state, BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED);
  disk_shared.data[BLCKSZ - 16] ^= 0x3c;
  memcpy(hb_bytes.data, disk_shared.data, BLCKSZ);
 }
 pg_atomic_fetch_add_u32(&request_buf.state, HB_READERS * BUF_REFCOUNT_ONE);
 for (int reader = 0; reader < HB_READERS; reader++) {
  hb_procs[reader] = *hb_writer_proc;
  hb_procs[reader].pid += 100 + reader;
  hb_procs[reader].backendId += 1 + reader;
  hb_procs[reader].pgprocno += 1 + reader;
  hb_procs[reader].cluster_grd_generation += 1 + reader;
  hb_owners[reader] = ResourceOwnerCreate(hb_writer_owner, "hot cached reader");
  hb_reader_context(reader); NM_REQUIRE(rl_run(NULL)); hb_park_reader(reader);
 }
 if (hb_writeback) hb_output_begin();
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(rl_share_locks, HB_READERS); ordinary_delivery_transport = hb_deliver;
	entry = rx_prepare(NULL);
	printf("# hot-block prepare entry=%d waits=%u attempts=%u handoff=%u issued=%u sends=%u native=%u type=%u flags=%u gen=" UINT64_FORMAT " token=" UINT64_FORMAT " failures=%u PANIC=%u\n",
		entry != NULL, wc_waits, hb_attempts, hb_handoffs, hb_requests, ordinary_sends,
		request_buf.pcm_state, request_buf.buffer_type, cluster_pcm_own_flags_get(0),
		cluster_pcm_own_gen_get(0), cluster_pcm_own_reservation_token_get(0), wc_failures, ordinary_panics);
	UT_ASSERT_EQ(hb_handoffs, 1); UT_ASSERT_EQ(hb_releases, HB_READERS);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), hb_token + 1);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); UT_ASSERT_EQ(ordinary_panics, 0);
	UT_ASSERT(memcmp(hb_bytes.data, disk_shared.data, BLCKSZ) == 0);
	NM_REQUIRE(entry != NULL);
	UT_ASSERT_EQ(wc_legacy, 0); UT_ASSERT_EQ(wc_failures, 0); UT_ASSERT_EQ(wc_waits, HB_READERS);
	UT_ASSERT_EQ(hb_handoffs, 1); UT_ASSERT_EQ(hb_requests, 1);
	UT_ASSERT_EQ(hb_attempts, HB_READERS + 1 - (hb_writeback ? 5 : 0));
	UT_ASSERT_EQ(hb_releases, HB_READERS); UT_ASSERT_EQ(rl_service_wakes, HB_READERS);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), hb_generation + 1); UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), hb_token + 1);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X); UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_XCUR);
	/* This callback counts the first read; X completion uses the real native owner below. */
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_io_calls, 1); UT_ASSERT_EQ(disk_reads, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & CLUSTER_PCM_OWN_SEMANTIC_BUF_STATE_MASK, hb_semantic);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&request_buf.state)), HB_READERS + 1 - hb_releases); UT_ASSERT(memcmp(hb_bytes.data, disk_shared.data, BLCKSZ) == 0);
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
	UT_ASSERT_EQ(hb_held(), 0); UT_ASSERT_EQ(hb_errors, variant == 4 ? 1 : 0);
 if (hb_writeback) {
  UT_ASSERT_EQ(hb_output_writes, 1); UT_ASSERT_EQ(hb_output_wakes, 1);
  UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), hb_output_generation + 2);
 }
 for (int n = 0; n < HB_READERS; n++) UT_ASSERT(hb_owners[n] == NULL);
 UT_ASSERT(hb_writer_owner->firstchild == NULL);
 UT_ASSERT_EQ(fake_lwlock_depth, 0);
 printf("# hot-block R order=%d variant=%d oldE=" UINT64_FORMAT " E=" UINT64_FORMAT " A=" UINT64_FORMAT
		" base=" UINT64_FORMAT " gen=" UINT64_FORMAT " token=" UINT64_FORMAT " crc=%u"
		" readers=%u releases=%u waits=%u handoff=%u REQUEST=%u nativeX=%u IO=%u extra_reads=%u refcount=%u notice=%u ledger=%u output_writes=%u reader_errors=%u\n",
		hb_order, variant, hb_first_E, hb_request.common.assertion_sequence, nm_grant.common.authority_generation,
		hb_generation, cluster_pcm_own_gen_get(0), cluster_pcm_own_reservation_token_get(0), cluster_gcs_block_compute_checksum(disk_shared.data),
		(unsigned)HB_READERS, hb_releases, wc_waits, hb_handoffs, hb_requests,
		(unsigned)(request_buf.pcm_state == PCM_STATE_X),
		(unsigned)((pg_atomic_read_u32(&request_buf.state) & BM_IO_IN_PROGRESS) != 0),
		disk_reads - 1, BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&request_buf.state)),
		state->requester_settlement_intent.slot.state,
		(unsigned)(cluster_bufmgr_pcm_x_writer_find(&request_buf) != NULL), hb_output_writes, hb_errors);
	complete_idle(); return true;
}
static void hb_case(int order, int variant) { hb_order = order; nm_run_roles(variant, hb_master, hb_parent); }
UT_TEST(test_hot_eight_readers_forward_one_original_writer) { hb_case(0, 0); }
UT_TEST(test_hot_eight_readers_reverse_one_original_writer) { hb_case(1, 0); }
UT_TEST(test_hot_eight_readers_forward_ACK_before_callback) { hb_case(0, 1); }
UT_TEST(test_hot_eight_readers_reverse_ACK_before_callback) { hb_case(1, 1); }
UT_TEST(test_hot_eight_readers_forward_duplicate_grant) { hb_case(0, 2); }
UT_TEST(test_hot_eight_readers_reverse_duplicate_grant) { hb_case(1, 2); }
UT_TEST(test_hot_eight_readers_forward_output_overlap) { hb_case(0, 3); }
UT_TEST(test_hot_eight_readers_reverse_output_overlap) { hb_case(1, 3); }
UT_TEST(test_hot_eight_readers_forward_reader_ERROR_during_output) { hb_case(0, 4); }
UT_TEST(test_hot_eight_readers_reverse_reader_ERROR_during_output) { hb_case(1, 4); }
int main(void)
{
 setvbuf(stdout, NULL, _IONBF, 0);
#ifdef USE_ASSERT_CHECKING
 printf("# hot-block USE_ASSERT_CHECKING=1\n");
#else
 printf("# hot-block USE_ASSERT_CHECKING=0\n");
#endif
 UT_PLAN(10);
 UT_RUN(test_hot_eight_readers_forward_one_original_writer);
 UT_RUN(test_hot_eight_readers_reverse_one_original_writer);
 UT_RUN(test_hot_eight_readers_forward_ACK_before_callback);
 UT_RUN(test_hot_eight_readers_reverse_ACK_before_callback);
 UT_RUN(test_hot_eight_readers_forward_duplicate_grant);
 UT_RUN(test_hot_eight_readers_reverse_duplicate_grant);
 UT_RUN(test_hot_eight_readers_forward_output_overlap);
 UT_RUN(test_hot_eight_readers_reverse_output_overlap);
 UT_RUN(test_hot_eight_readers_forward_reader_ERROR_during_output);
 UT_RUN(test_hot_eight_readers_reverse_reader_ERROR_during_output);
 UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
