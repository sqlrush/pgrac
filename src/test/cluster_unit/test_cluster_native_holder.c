/* Actual holder source capture, native downgrade, immutable PCM pair and send.
 * Peer/session, OS locks and storage are controlled leaves. This composition
 * does not qualify multi-process contention or the complete H matrix.
 * Author: SqlRush <sqlrush@gmail.com> */
#define cluster_test_pcm_clock_gettime cluster_publish_fixture_clock_gettime
#include "test_cluster_native_first_fill_fixture.inc"
#undef cluster_test_pcm_clock_gettime
#define ereport(level, rest) ordinary_report(level)
#define gcs_block_resource_x_payload_candidate holder_payload_candidate
#define RESOURCE_X_ACQUIRE_REUSE_PCM_FIXTURE
#include "test_cluster_resource_x_master_ack_fixture.inc"
#undef RESOURCE_X_ACQUIRE_REUSE_PCM_FIXTURE
#undef gcs_block_resource_x_payload_candidate

#include "cluster/cluster_itl.h"
#include "test_cluster_native_holder_itl.inc"
static LWLock holder_mapping_lock;
static bool holder_content_busy;
static unsigned holder_pins, holder_fuses;
static bool holder_peer_current = true;
static void (*holder_after_prepare)(void);
static void holder_pin(BufferDesc *buf, uint32 state)
{
	UT_ASSERT(buf == &request_buf); UT_ASSERT_EQ(fake_lwlock_depth, 1);
	holder_pins++; UnlockBufHdr(buf, state + BUF_REFCOUNT_ONE);
}
static void holder_unpin(BufferDesc *buf)
{
	UT_ASSERT(buf == &request_buf); UT_ASSERT(holder_pins > 0);
	holder_pins--; pg_atomic_fetch_sub_u32(&buf->state, BUF_REFCOUNT_ONE);
}
static bool holder_conditional(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(&request_buf));
	if (holder_content_busy) return false;
	LWLockAcquire(lock, mode); return true;
}
/* Storage write is outside this clean-source composition. Never forge it. */
static bool holder_write_error;
static void holder_unexpected_write(void) { UT_ASSERT(holder_write_error); pg_re_throw(); }
static unsigned holder_io_enters, holder_io_ends;
bool cluster_ko_directory_io_begin_v3(RelFileLocator locator, ClusterKoDirectoryIoV3 *io)
{
	UT_ASSERT(RelFileLocatorEquals(locator, BufTagGetRelFileLocator(&request_buf.tag)));
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	io->slot = 1; io->serial = ++holder_io_enters; return true;
}
bool cluster_ko_directory_io_end_v3(const ClusterKoDirectoryIoV3 *io)
{
	UT_ASSERT_EQ(io->slot, 1); UT_ASSERT(io->serial <= holder_io_enters);
	UT_ASSERT_EQ(fake_lwlock_depth, 0); holder_io_ends++; if (holder_after_prepare != NULL) holder_after_prepare(); return true;
}
bool RecoveryInProgress(void) { return false; }
XLogRecPtr GetXLogInsertRecPtr(void) { holder_unexpected_write(); return InvalidXLogRecPtr; }
void XLogFlush(XLogRecPtr lsn) { (void)lsn; holder_unexpected_write(); }
#define smgropen disk_open
#define smgrread disk_read
#define AbortBufferIO wait_actual_abort
#define BufTableHashCode(tag) ((void)(tag), 0)
#define BufMappingPartitionLock(hash) ((void)(hash), &holder_mapping_lock)
#define BufTableLookup(lookup, hash) ((void)(hash), BufferTagsEqual(lookup, &request_buf.tag) ? 0 : -1)
#define BufHdrGetBlock(buf) ((void)(buf), disk_shared.data)
#define LockBufHdr stage_header_lock
#define cluster_bufmgr_pin_for_gcs_locked holder_pin
#define cluster_bufmgr_unpin_for_gcs holder_unpin
#define LWLockConditionalAcquire holder_conditional
#define FlushBufferWithRecovery(buf, rel, object, context, attempt, started, wal, owner) \
	((void)(buf), (void)(rel), (void)(object), (void)(context), (void)(attempt), \
	 (void)(started), (void)(wal), (void)(owner), holder_unexpected_write())
#define FlushBuffer(...) holder_unexpected_write()
#define PinBuffer_Locked(buf) holder_pin(buf, pg_atomic_read_u32(&(buf)->state) & ~BM_LOCKED)
#define UnpinBuffer(buf) holder_unpin(buf)
#define ReservePrivateRefCountEntry() ((void)0)
#define ResourceOwnerEnlargeBuffers(owner) ((void)(owner))
#define cluster_bufmgr_pcm_own_observe_s_source_hard_failure(reason, cause, buf, native, required, local, storage, state, type, result, abort_result) \
	((void)(reason), (void)(cause), (void)(buf), (void)(native), (void)(required), \
	 (void)(local), (void)(storage), (void)(state), (void)(type), (void)(result), (void)(abort_result))
#define cluster_bufmgr_resource_x_fail_closed_current() (holder_fuses++)
#include "test_cluster_native_holder_bufmgr.inc"
#undef cluster_bufmgr_resource_x_fail_closed_current
#undef cluster_bufmgr_pcm_own_observe_s_source_hard_failure
#undef ResourceOwnerEnlargeBuffers
#undef ReservePrivateRefCountEntry
#undef UnpinBuffer
#undef PinBuffer_Locked
#undef FlushBuffer
#undef FlushBufferWithRecovery
#undef LWLockConditionalAcquire
#undef cluster_bufmgr_unpin_for_gcs
#undef cluster_bufmgr_pin_for_gcs_locked
#undef LockBufHdr
#undef BufHdrGetBlock
#undef BufTableLookup
#undef BufMappingPartitionLock
#undef BufTableHashCode
#undef AbortBufferIO
#undef smgrread
#undef smgropen
#define gcs_block_resource_x_fail_closed_current() (holder_fuses++)
#define gcs_block_resource_x_gate_session_recheck_result ordinary_current
#define gcs_block_pcm_x_resource_x_peer_ready_exact holder_master_peer
#define gcs_block_resource_x_target_peer_result_exact holder_peer_result

static bool holder_master_peer(int32 node, uint32 *connection)
{
	UT_ASSERT_EQ(node, 0); *connection = 61; return true;
}
static ResourceXApplyResult holder_peer_result(const ClusterSemanticAdmissionToken *admission,
	int32 node, uint32 connection)
{
	return holder_peer_current && admission->entered && ((node == 0 && connection == 61) || (node == 1 && connection == 83))
		? RESOURCE_X_APPLY_APPLIED : RESOURCE_X_APPLY_STALE;
}
static bool holder_peer_sample(int32 node, uint64 required, uint64 *caps, uint32 *connection)
{
	UT_ASSERT(node >= 0 && node < RESOURCE_X_PROTOCOL_NODE_LIMIT);
	*caps = required; *connection = 83; return true;
}
#define cluster_sf_peer_capability_word_sample holder_peer_sample
#include "test_cluster_native_holder_gcs.inc"
#undef cluster_sf_peer_capability_word_sample


typedef struct HolderFixture {
	PublishFixture master;
	ResourceXDecodedFrame block;
	ResourceXSourceDrainWork work;
	ResourceXGateSnapshot gate;
	ClusterPcmOwnSnapshot prepared;
} HolderFixture;
static bool holder_probe(ResourceXSourceDrainWork *work)
{
	ResourceXIntentSlot slot; ResourceXAcquisitionRef delivery;
	uint8 bytes[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES]; uint32 examined;
	for (int n = 0; n < 256; n++) {
		ResourceXIntentProbeResult result = cluster_pcm_lock_resource_x_outbound_work_probe_exact(
			4, &slot, bytes, sizeof(bytes), &examined, &delivery);
		UT_ASSERT(result != RESOURCE_X_INTENT_PROBE_CORRUPT);
		if (result == RESOURCE_X_INTENT_PROBE_SOURCE_DRAIN) { memcpy(work, bytes, sizeof(*work)); return true; }
		if (result == RESOURCE_X_INTENT_PROBE_IDLE) break;
	}
	return false;
}
static bool holder_start_mode(HolderFixture *f, PcmState mode)
{
	ClusterBufmgrFirstFill scratch;
	uint32 state;
	ordinary_start(&scratch, 2);
	ResourceOwnerForgetBufferIO(CurrentResourceOwner, 1);
	memset(&request_own, 0, sizeof(request_own));
	pg_atomic_init_u64(&request_own.generation, 901);
	pg_atomic_init_u64(&request_own.reservation_token, 77);
	request_buf.tag = make_tag(850);
	request_buf.pcm_state = mode; request_buf.buffer_type = mode == PCM_STATE_X ? BUF_TYPE_XCUR : BUF_TYPE_SCUR;
	pg_atomic_write_u32(&request_buf.state, BM_TAG_VALID | BM_VALID | BUF_REFCOUNT_ONE);
	((PageHeader)disk_bytes.data)->pd_checksum = pg_checksum_page(disk_bytes.data, 850);
	disk_store(); memcpy(disk_shared.data, disk_bytes.data, BLCKSZ);
	BufferBlocks = disk_shared.data;
	request_pins = 1; holder_pins = holder_fuses = 0; holder_content_busy = false;
	holder_after_prepare = NULL; holder_peer_current = true; holder_write_error = false;
	holder_io_enters = holder_io_ends = 0;
	memset(f, 0, sizeof(*f));
	if (!publish_start(mode == PCM_STATE_X ? 1 : 2, PCM_STATE_S, &f->master)) return false;
	publish_apply(&f->master);
	f->block = publish_decode(f->master.state->block_intents[0].payload, true);
	UT_ASSERT_EQ(f->block.body.acquire_decision.directory_mode, mode);
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&f->gate));
	if (cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f->block, 0, 77, 61)
		!= RESOURCE_X_APPLY_APPLIED || !holder_probe(&f->work)) return false;
	state = request_header_lock(&request_buf);
	cluster_pcm_own_snapshot_locked(&request_buf, &request_base);
	UnlockBufHdr(&request_buf, state);
	return true;
}
static bool holder_start(HolderFixture *f) { return holder_start_mode(f, PCM_STATE_X); }
static ResourceXApplyResult holder_capture(HolderFixture *f)
{
	return gcs_block_resource_x_acquire_s_source_capture_owned(&f->work, &f->block,
		&request_admission, &f->gate);
}
static ResourceXApplyResult holder_finish(HolderFixture *f)
{
	ResourceXDecodedFrame image; ResourceXAcquireSSourceState phase; int32 id;
	ResourceXApplyResult result = cluster_pcm_lock_resource_x_acquire_s_source_snapshot_exact(
		&f->block, 0, &id, &f->prepared, &image, &phase);
	if (result != RESOURCE_X_APPLY_APPLIED) return result;
	return gcs_block_resource_x_acquire_s_read_finish_owned(&f->block, 0, 77, &request_buf, &f->prepared);
}
UT_TEST(test_holder_capture_native_downgrade_and_original_pair)
{
	HolderFixture f; ResourceXApplyResult result;
	bool started = holder_start(&f); UT_ASSERT(started); if (!started) return;
	result = holder_capture(&f);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED); if (result != RESOURCE_X_APPLY_APPLIED) return;
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(f.master.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	UT_ASSERT_EQ(f.master.state->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	result = holder_finish(&f);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED); if (result != RESOURCE_X_APPLY_APPLIED) return;
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S); UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_SCUR);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 902); UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(f.master.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	UT_ASSERT_EQ(f.master.state->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(holder_pins, 0); UT_ASSERT_EQ(holder_fuses, 0); complete_idle();
}
#define HOLDER_EQ(expr, expected) do { \
	ResourceXApplyResult value = (expr); UT_ASSERT_EQ(value, (expected)); \
	if (value != (expected)) return; \
} while (0)
#define HOLDER_START(f) do { bool ok = holder_start(f); UT_ASSERT(ok); if (!ok) return; } while (0)
static ResourceXIntentSlot *holder_slot(HolderFixture *f, bool image)
{
	return image ? &f->master.state->holder_image_intent : &f->master.state->holder_status_intent.slot;
}
static void holder_no_debt(void)
{
	UT_ASSERT_EQ(holder_pins, 0); UT_ASSERT_EQ(holder_fuses, 0);
	UT_ASSERT_EQ(holder_io_enters, holder_io_ends); complete_idle();
}
static void holder_wrong_peer(void) { holder_peer_current = false; }
UT_TEST(test_holder_real_busy_before_capture_keeps_one_work_until_release)
{
	for (int io = 0; io < 2; io++) {
		HolderFixture f; ResourceXSourceDrainWork next; bool found; HOLDER_START(&f);
		if (io) pg_atomic_fetch_or_u32(&request_buf.state, BM_IO_IN_PROGRESS);
		else holder_content_busy = true;
		HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
		UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_ACQUIRE_S_SOURCE_FORWARD);
		HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(!holder_probe(&next));
		holder_content_busy = false; pg_atomic_fetch_and_u32(&request_buf.state, ~BM_IO_IN_PROGRESS);
		cluster_pcm_lock_resource_x_source_drain_content_released();
		found = holder_probe(&next); UT_ASSERT(found); if (!found) return;
		UT_ASSERT(next.attempt_generation > f.work.attempt_generation);
		UT_ASSERT(memcmp(&next.request, &f.work.request, sizeof(next.request)) == 0);
		f.work = next;
		HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
		HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
		HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(!holder_probe(&next)); holder_no_debt();
	}
}
UT_TEST(test_holder_real_busy_after_capture_preserves_exact_native_fence_and_image)
{
	for (int io = 0; io < 2; io++) {
		HolderFixture f; ClusterPcmResourceXHolderImage before; uint64 token;
		HOLDER_START(&f); HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
		before = f.master.state->holder_image;
		token = cluster_pcm_own_reservation_token_get(0);
		if (io) pg_atomic_fetch_or_u32(&request_buf.state, BM_IO_IN_PROGRESS);
		else holder_content_busy = true;
		HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 901);
		UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), token);
		UT_ASSERT(memcmp(&before, &f.master.state->holder_image, sizeof(before)) == 0);
		UT_ASSERT_EQ(holder_slot(&f, true)->state, RESOURCE_X_INTENT_SLOT_EMPTY);
		holder_content_busy = false; pg_atomic_fetch_and_u32(&request_buf.state, ~BM_IO_IN_PROGRESS);
		HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED); holder_no_debt();
	}
}
UT_TEST(test_holder_interrupted_after_native_commit_adopts_same_downgrade_once)
{
	HolderFixture f; ClusterPcmOwnSnapshot prepared, finished; ResourceXDecodedFrame image;
	ClusterPcmResourceXHolderImage before;
	ResourceXAcquireSSourceState phase; int32 id; HOLDER_START(&f);
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(cluster_pcm_lock_resource_x_acquire_s_source_snapshot_exact(&f.block, 0, &id, &prepared, &image, &phase), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_finish_x_source(&request_buf, &prepared,
		(const char *)image.body.acquire_image.original_image.page_bytes, &finished), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	before = f.master.state->holder_image;
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 902);
	UT_ASSERT(memcmp(&before, &f.master.state->holder_image, sizeof(before)) == 0); holder_no_debt();
}
UT_TEST(test_holder_changed_memory_image_cannot_publish_native_S)
{
	HolderFixture f; HOLDER_START(&f); HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	disk_shared.data[1024] ^= 1;
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X); UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 901);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(holder_slot(&f, true)->state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT(holder_fuses > 0); UT_ASSERT_EQ(holder_pins, 0); complete_idle();
}
UT_TEST(test_holder_changed_native_token_cannot_publish_or_clear_successor)
{
	HolderFixture f; uint64 next; HOLDER_START(&f); HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	next = pg_atomic_fetch_add_u64(&request_own.reservation_token, 1) + 1;
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), next);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(holder_slot(&f, true)->state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT(holder_fuses > 0); UT_ASSERT_EQ(holder_pins, 0); complete_idle();
}
UT_TEST(test_holder_peer_loss_after_native_prepare_aborts_only_unhanded_fence)
{
	HolderFixture f; HOLDER_START(&f); holder_after_prepare = holder_wrong_peer;
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 901);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_ACQUIRE_S_SOURCE_FORWARD);
	UT_ASSERT_EQ(holder_slot(&f, true)->state, RESOURCE_X_INTENT_SLOT_EMPTY);
	holder_no_debt();
}
UT_TEST(test_holder_storage_write_ERROR_before_shared_handoff_releases_native_fence_and_pins)
{
	HolderFixture f; volatile bool caught = false; HOLDER_START(&f);
	/* Physical write failure is a storage leaf here. Native reservation,
	 * content/pin unwind and shared service responsibility remain real. */
	holder_write_error = true;
	pg_atomic_fetch_or_u32(&request_buf.state, BM_DIRTY);
	PG_TRY(); { (void)holder_capture(&f); } PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X); UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 901);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_ACQUIRE_S_SOURCE_FORWARD);
	holder_no_debt();
}
UT_TEST(test_holder_compatible_S_captures_verified_storage_and_preserves_native_generation)
{
	HolderFixture f; bool ok = holder_start_mode(&f, PCM_STATE_S); UT_ASSERT(ok); if (!ok) return;
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(disk_reads, 1); UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_REVOKING);
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S); UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 901);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(holder_slot(&f, true)->state, RESOURCE_X_INTENT_SLOT_ARMED); holder_no_debt();
}
UT_TEST(test_holder_S_storage_read_ERROR_unwinds_native_but_retains_original_service)
{
	HolderFixture f; volatile bool caught = false;
	bool ok = holder_start_mode(&f, PCM_STATE_S); UT_ASSERT(ok); if (!ok) return;
	disk_read_error = true;
	PG_TRY(); { (void)holder_capture(&f); } PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_ACQUIRE_S_SOURCE_FORWARD);
	UT_ASSERT_EQ(holder_slot(&f, true)->state, RESOURCE_X_INTENT_SLOT_EMPTY); holder_no_debt();
}
UT_TEST(test_holder_native_itl_active_slot_defers_same_current_before_revoke)
{
	HolderFixture f; PageHeader page; HOLDER_START(&f);
	page = (PageHeader)disk_shared.data;
	page->pd_flags |= PD_HAS_ITL; page->pd_special = BLCKSZ - CLUSTER_ITL_ARRAY_SIZE;
	ClusterPageGetItlSlots(disk_shared.data)[0].flags = ITL_FLAG_ACTIVE;
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 901);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_ACQUIRE_S_SOURCE_FORWARD);
	ClusterPageGetItlSlots(disk_shared.data)[0].flags = ITL_FLAG_FREE;
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED); holder_no_debt();
}
UT_TEST(test_holder_real_full_ring_keeps_immutable_pair_and_both_send_owners)
{
	HolderFixture f; ClusterPcmResourceXHolderImage image; ClusterPcmResourceXHolderStatus status;
	HOLDER_START(&f); HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	image = f.master.state->holder_image; status = f.master.state->holder_status;
	request_shared.rings[0].count = PGRAC_LMS_OUTBOUND_CAPACITY;
	for (int half = 0; half < 2; half++) {
		ResourceXIntentSlot before = *holder_slot(&f, half);
		UT_ASSERT_EQ(done_actual_lms_enqueue(0, &before, half ? 83 : 61, 9999999), CLUSTER_LMS_ENQUEUE_FULL);
		UT_ASSERT(memcmp(&before, holder_slot(&f, half), sizeof(before)) == 0);
	}
	UT_ASSERT(memcmp(&image, &f.master.state->holder_image, sizeof(image)) == 0);
	UT_ASSERT(memcmp(&status, &f.master.state->holder_status, sizeof(status)) == 0); holder_no_debt();
}
UT_TEST(test_holder_real_transport_failure_rearms_same_payload_and_success_keeps_semantic_debt)
{
	HolderFixture f; HOLDER_START(&f); HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	for (int half = 0; half < 2; half++) {
		uint8 before[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES], after[sizeof(before)];
		ResourceXIntentSlot slot = *holder_slot(&f, half), observed, staged_slot;
		HOLDER_EQ(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(&slot, &observed, before, sizeof(before)), RESOURCE_X_APPLY_APPLIED);
		for (int sent = 0; sent < 2; sent++) {
			uint32 index;
			slot = *holder_slot(&f, half); fake_pcm_clock_us += 1000000;
			index = request_shared.rings[0].head;
			UT_ASSERT_EQ(done_actual_lms_enqueue(0, &slot, half ? 83 : 61, UINT64_C(900000000)), CLUSTER_LMS_ENQUEUE_ADMITTED);
			memcpy(&staged_slot, request_shared.rings[0].ring[index].payload, sizeof(staged_slot));
			HOLDER_EQ(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(&staged_slot, &observed, after, sizeof(after)), RESOURCE_X_APPLY_APPLIED);
			UT_ASSERT(memcmp(before, after, slot.payload_bytes) == 0);
			UT_ASSERT(lms_outbound_resource_x_intent_payload_valid(&observed, after));
			cluster_lms_outbound_resource_x_send_complete(&staged_slot, sent);
			UT_ASSERT_EQ(holder_slot(&f, half)->state, sent ? RESOURCE_X_INTENT_SLOT_EMPTY : RESOURCE_X_INTENT_SLOT_ARMED);
		}
	}
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	UT_ASSERT_EQ(f.master.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 902); holder_no_debt();
}
UT_TEST(test_holder_shared_work_retires_only_after_real_native_publish)
{
	HolderFixture f; ResourceXSourceDrainWork next; bool found; HOLDER_START(&f);
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_REVOKING);
	cluster_pcm_lock_resource_x_source_drain_content_released();
	found = holder_probe(&next); UT_ASSERT(found); if (!found) return;
	f.work = next; HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!holder_probe(&next)); holder_no_debt();
}
UT_TEST(test_holder_stale_master_before_capture_never_reserves_native)
{
	HolderFixture f; HOLDER_START(&f); ordinary_master_session++;
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); UT_ASSERT_EQ(holder_io_enters, 0);
	UT_ASSERT_EQ(f.master.state->holder_status.valid, RESOURCE_X_ACQUIRE_S_SOURCE_FORWARD); holder_no_debt();
}
UT_TEST(test_holder_captured_bytes_and_real_native_ACK_reach_original_master)
{
	HolderFixture f; ResourceXDecodedFrame image, ack; ResourceXMasterSnapshot out;
	HOLDER_START(&f); HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(cluster_pcm_lock_resource_x_holder_image_exact(&f.block.common.logical_assertion, &image), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(image.body.acquire_image.original_image.page_bytes, disk_shared.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(image.body.acquire_image.original_image.page_checksum,
		cluster_gcs_block_compute_checksum(disk_shared.data));
	UT_ASSERT_EQ(image.body.acquire_image.original_image.source_carrier_generation, 902);
	HOLDER_EQ(cluster_pcm_lock_resource_x_holder_status_exact(&f.block.common.logical_assertion, &ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_ack.observed_mode, PCM_STATE_X);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_ack.target_mode, PCM_STATE_S);
	HOLDER_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(&ack, 0, &out), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.master.state->requests[1].final_authority_generation, 0);
	UT_ASSERT_EQ(holder_slot(&f, true)->state, RESOURCE_X_INTENT_SLOT_ARMED); holder_no_debt();
}
int main(void)
{
	UT_PLAN(16);
	UT_RUN(test_holder_capture_native_downgrade_and_original_pair);
	UT_RUN(test_holder_real_busy_before_capture_keeps_one_work_until_release);
	UT_RUN(test_holder_real_busy_after_capture_preserves_exact_native_fence_and_image);
	UT_RUN(test_holder_interrupted_after_native_commit_adopts_same_downgrade_once);
	UT_RUN(test_holder_changed_memory_image_cannot_publish_native_S);
	UT_RUN(test_holder_changed_native_token_cannot_publish_or_clear_successor);
	UT_RUN(test_holder_peer_loss_after_native_prepare_aborts_only_unhanded_fence);
	UT_RUN(test_holder_storage_write_ERROR_before_shared_handoff_releases_native_fence_and_pins);
	UT_RUN(test_holder_compatible_S_captures_verified_storage_and_preserves_native_generation);
	UT_RUN(test_holder_S_storage_read_ERROR_unwinds_native_but_retains_original_service);
	UT_RUN(test_holder_native_itl_active_slot_defers_same_current_before_revoke);
	UT_RUN(test_holder_real_full_ring_keeps_immutable_pair_and_both_send_owners);
	UT_RUN(test_holder_real_transport_failure_rearms_same_payload_and_success_keeps_semantic_debt);
	UT_RUN(test_holder_shared_work_retires_only_after_real_native_publish);
	UT_RUN(test_holder_stale_master_before_capture_never_reserves_native);
	UT_RUN(test_holder_captured_bytes_and_real_native_ACK_reach_original_master);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
