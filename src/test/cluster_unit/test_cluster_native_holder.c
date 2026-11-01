/* Actual holder source capture, native downgrade, immutable PCM pair and send.
 * Peer/session, OS locks and storage are controlled leaves. This composition
 * does not qualify multi-process contention or the complete H matrix.
 * Author: SqlRush <sqlrush@gmail.com> */
#define cluster_test_pcm_clock_gettime cluster_publish_fixture_clock_gettime
#ifdef NATIVE_HOLDER_X_REQUESTER_COMPOSITION
#include "test_cluster_native_resident_x_fixture.inc"
#else
#include "test_cluster_native_first_fill_fixture.inc"
#endif
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
static unsigned holder_interrupt_repairs, holder_interrupt_releases;
static bool holder_peer_current = true;
static uint32 holder_requester_connection = 83;
static int32 holder_requester_node = 1;
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
/* The PCM fixture's OS lock leaf has no interrupt hold. Preserve the real
 * owner's ERROR repair/release balance without leaking a synthetic hold. */
static void holder_repair_interrupt_hold(void)
{
	UT_ASSERT(LWLockHeldByMe(BufferDescriptorGetContentLock(&request_buf)));
	holder_interrupt_repairs++;
}
static void holder_lock_release(LWLock *lock)
{
	if (holder_interrupt_repairs > holder_interrupt_releases) {
		UT_ASSERT(lock == BufferDescriptorGetContentLock(&request_buf));
		holder_interrupt_releases++;
	}
	LWLockRelease(lock);
#ifdef NATIVE_HOLDER_WRITEBACK
#include "test_cluster_native_holder_content_release.inc"
#endif
}
/* Storage write is outside this clean-source composition. Never forge it. */
static bool holder_write_error;
static void holder_unexpected_write(void) { UT_ASSERT(holder_write_error); pg_re_throw(); }
#ifdef NATIVE_HOLDER_WRITEBACK
static void holder_flush(BufferDesc *, SMgrRelation, IOObject, IOContext);
static void holder_flush_attempt(BufferDesc *, SMgrRelation, IOObject, IOContext, volatile bool *);
static void holder_flush_recovery(BufferDesc *, SMgrRelation, IOObject, IOContext,
	const struct ClusterSpaceRecoveryBatchV1 *, volatile bool *,
	const struct ClusterPageWalBindingV1 *, volatile bool *);
static void holder_output_abort(Buffer);
#endif
/* These clean MAIN cases never authorize a descriptor DROP or WAL/DATA
 * success. Keep an accidental dispatch an error, not a fabricated receipt. */
static void holder_unexpected_drop(void) { UT_ASSERT(false); pg_re_throw(); }
static bool cluster_pcm_x_finish_retain_flush_active;
#ifdef ENABLE_INJECTION
static bool cluster_pcm_x_finish_retain_flush_fault_active;
#endif
static bool cluster_pcm_x_finish_retain_flush_io_active;
static bool cluster_pcm_x_finish_retain_flush_error_context_pushed;
static ErrorContextCallback *cluster_pcm_x_finish_retain_flush_error_context_previous;
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
#ifdef NATIVE_HOLDER_WRITEBACK
static unsigned holder_wal_flushes;
XLogRecPtr GetXLogInsertRecPtr(void) { return UINT64_MAX; }
void XLogFlush(XLogRecPtr lsn) { UT_ASSERT(lsn <= GetXLogInsertRecPtr()); holder_wal_flushes++; }
#else
XLogRecPtr GetXLogInsertRecPtr(void) { holder_unexpected_write(); return InvalidXLogRecPtr; }
void XLogFlush(XLogRecPtr lsn) { (void)lsn; holder_unexpected_write(); }
#endif
#define smgropen disk_open
#define smgrread disk_read
#ifdef NATIVE_HOLDER_WRITEBACK
#define AbortBufferIO holder_output_abort
#else
#define AbortBufferIO wait_actual_abort
#endif
#define BufTableHashCode(tag) ((void)(tag), 0)
#define BufMappingPartitionLock(hash) ((void)(hash), &holder_mapping_lock)
#define BufTableLookup(lookup, hash) ((void)(hash), BufferTagsEqual(lookup, &request_buf.tag) ? 0 : -1)
#define BufHdrGetBlock(buf) ((void)(buf), disk_shared.data)
#define LockBufHdr stage_header_lock
#define cluster_bufmgr_pin_for_gcs_locked holder_pin
#define cluster_bufmgr_unpin_for_gcs holder_unpin
#define LWLockConditionalAcquire holder_conditional
#define LWLockRelease holder_lock_release
#undef HOLD_INTERRUPTS
#define HOLD_INTERRUPTS() holder_repair_interrupt_hold()
#ifdef NATIVE_HOLDER_WRITEBACK
#define FlushBufferWithRecovery holder_flush_recovery
#define FlushBuffer holder_flush
#define FlushBufferWithAttempt holder_flush_attempt
#else
#define FlushBufferWithRecovery(buf, rel, object, context, attempt, started, wal, owner) \
	((void)(buf), (void)(rel), (void)(object), (void)(context), (void)(attempt), \
	 (void)(started), (void)(wal), (void)(owner), holder_unexpected_write())
#define FlushBuffer(...) holder_unexpected_write()
#define FlushBufferWithAttempt(buf, rel, object, context, wrote) \
	((void)(buf), (void)(rel), (void)(object), (void)(context), (void)(wrote), holder_unexpected_write())
#endif
#define InvalidateBufferCommitTailLocked(buf, tag, hash, lock, state, pcm, io) \
	((void)(buf), (void)(tag), (void)(hash), (void)(lock), (void)(state), \
	 (void)(pcm), (void)(io), holder_unexpected_drop())
#define CurrentMemoryContext ((MemoryContext)0)
#define MemoryContextSwitchTo(context) ((void)(context))
#define FlushErrorState() ((void)0)
#define geterrcode() ERRCODE_INTERNAL_ERROR
#define cluster_injection_is_armed(name) ((void)(name), false)
#define cluster_pcm_x_retain_flush_error_target_matches(spc, db, rel, fork, block) \
	((void)(spc), (void)(db), (void)(rel), (void)(fork), (void)(block), false)
#define PinBuffer_Locked(buf) holder_pin(buf, pg_atomic_read_u32(&(buf)->state) & ~BM_LOCKED)
#define UnpinBuffer(buf) holder_unpin(buf)
#define ReservePrivateRefCountEntry() ((void)0)
#define ResourceOwnerEnlargeBuffers(owner) ((void)(owner))
#define cluster_bufmgr_pcm_own_observe_s_source_hard_failure(reason, cause, buf, native, required, local, storage, state, type, result, abort_result) \
	((void)(reason), (void)(cause), (void)(buf), (void)(native), (void)(required), \
	 (void)(local), (void)(storage), (void)(state), (void)(type), (void)(result), (void)(abort_result))
#define cluster_bufmgr_resource_x_fail_closed_current() (holder_fuses++)
#ifdef NATIVE_HOLDER_X_REQUESTER_COMPOSITION
/* Same original bodies, distinct test lock leaves for the two roles. */
static ClusterPcmOwnResult holder_begin_s_revoke(BufferDesc *, const ClusterPcmOwnSnapshot *, ClusterPcmOwnSnapshot *);
static ClusterPcmOwnResult holder_abort_s_revoke(BufferDesc *, const ClusterPcmOwnSnapshot *);
#define cluster_bufmgr_pcm_current_image_locked holder_current_image_locked
#define cluster_bufmgr_pcm_own_begin_s_revoke holder_begin_s_revoke
#define cluster_bufmgr_pcm_own_abort_s_revoke holder_abort_s_revoke
#endif
#include "test_cluster_native_holder_bufmgr.inc"
#ifdef NATIVE_HOLDER_X_REQUESTER_COMPOSITION
#undef cluster_bufmgr_pcm_current_image_locked
#undef cluster_bufmgr_pcm_own_begin_s_revoke
#undef cluster_bufmgr_pcm_own_abort_s_revoke
#endif
#undef cluster_bufmgr_resource_x_fail_closed_current
#undef cluster_bufmgr_pcm_own_observe_s_source_hard_failure
#undef ResourceOwnerEnlargeBuffers
#undef ReservePrivateRefCountEntry
#undef UnpinBuffer
#undef PinBuffer_Locked
#undef FlushBuffer
#undef geterrcode
#undef cluster_pcm_x_retain_flush_error_target_matches
#undef cluster_injection_is_armed
#undef FlushErrorState
#undef MemoryContextSwitchTo
#undef CurrentMemoryContext
#undef InvalidateBufferCommitTailLocked
#undef FlushBufferWithAttempt
#undef FlushBufferWithRecovery
#undef LWLockConditionalAcquire
#undef LWLockRelease
#undef HOLD_INTERRUPTS
#define HOLD_INTERRUPTS() (InterruptHoldoffCount++)
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
	return holder_peer_current && admission->entered && ((node == 0 && connection == 61) || (node == holder_requester_node && connection == holder_requester_connection))
		? RESOURCE_X_APPLY_APPLIED : RESOURCE_X_APPLY_STALE;
}
static bool holder_peer_sample(int32 node, uint64 required, uint64 *caps, uint32 *connection)
{
	UT_ASSERT(node >= 0 && node < RESOURCE_X_PROTOCOL_NODE_LIMIT);
	*caps = required; *connection = holder_requester_connection; return true;
}
#define cluster_sf_peer_capability_word_sample holder_peer_sample
static ErrorData holder_error = { .message = "controlled storage ERROR" };
#define CurrentMemoryContext ((MemoryContext)0)
#define MemoryContextSwitchTo(context) ((void)(context))
#define CopyErrorData() (&holder_error)
#define FreeErrorData(error) ((void)(error))
#define FlushErrorState() ((void)0)
#include "test_cluster_native_holder_gcs.inc"
#undef FlushErrorState
#undef FreeErrorData
#undef CopyErrorData
#undef MemoryContextSwitchTo
#undef CurrentMemoryContext
#undef cluster_sf_peer_capability_word_sample

#ifdef NATIVE_HOLDER_WRITEBACK
#include "test_cluster_native_holder_writeback_fixture.inc"
#endif

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
static void holder_native_start(PcmState mode)
{
	ClusterBufmgrFirstFill scratch;
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
	holder_interrupt_repairs = holder_interrupt_releases = 0;
	holder_requester_connection = 83; holder_requester_node = 1;
}
static bool holder_start_role(HolderFixture *f, PcmState mode, PcmState target, int32 holder)
{
	uint32 state;
	holder_native_start(mode);
	memset(f, 0, sizeof(*f));
	if (!publish_start(mode == PCM_STATE_X ? 1 : 2, target, &f->master)) return false;
	publish_apply(&f->master);
	f->block = publish_decode(f->master.state->block_intents[holder].payload, true);
	UT_ASSERT_EQ(f->block.body.acquire_decision.directory_mode, mode);
	if (holder != 0) {
		/* A separate instance owns its own PCM entry, not the master's
		 * canonical queue. Only authenticated role/boot is an input. */
		acquire_reset(); cluster_node_id = holder; fake_gcs_master_node = 0;
		ut_master_session = f->block.body.acquire_decision.holders[holder].boot_incarnation;
	}
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&f->gate));
	if (cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&f->block, 0, 77, 61)
		!= RESOURCE_X_APPLY_APPLIED || !holder_probe(&f->work)) return false;
	state = request_header_lock(&request_buf);
	cluster_pcm_own_snapshot_locked(&request_buf, &request_base);
	UnlockBufHdr(&request_buf, state);
	return true;
}
static bool holder_start_target(HolderFixture *f, PcmState mode, PcmState target) { return holder_start_role(f, mode, target, 0); }
static bool holder_start_mode(HolderFixture *f, PcmState mode) { return holder_start_target(f, mode, PCM_STATE_S); }
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
	return gcs_block_resource_x_acquire_s_source_finish_owned(&f->block, 0, 77, &request_buf, &f->prepared);
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
	/* Temporary scopes only. Immutable transport/native/PI owners are
	 * intentionally retained and inspected separately by each case. */
	UT_ASSERT_EQ(holder_pins, 0); UT_ASSERT_EQ(holder_fuses, 0);
	UT_ASSERT_EQ(holder_io_enters, holder_io_ends); complete_idle();
	UT_ASSERT_EQ(holder_interrupt_repairs, holder_interrupt_releases);
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
UT_TEST(test_holder_X_to_N_real_native_PI_handoff)
{
	HolderFixture f; ResourceXDecodedFrame image, ack; ResourceXMasterSnapshot out;
	bool ok = holder_start_target(&f, PCM_STATE_X, PCM_STATE_X); UT_ASSERT(ok); if (!ok) return;
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_N); UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_PI);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 902); UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_ERROR | BM_IO_IN_PROGRESS), BM_VALID);
	HOLDER_EQ(cluster_pcm_lock_resource_x_holder_image_exact(&f.block.common.logical_assertion, &image), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(image.body.acquire_image.original_image.page_bytes, disk_shared.data, BLCKSZ) == 0);
	HOLDER_EQ(cluster_pcm_lock_resource_x_holder_status_exact(&f.block.common.logical_assertion, &ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ack.body.acquire_witness.original_ack.target_mode, PCM_STATE_N);
	HOLDER_EQ(cluster_pcm_lock_resource_x_blocked_to_n_exact(&ack, 0, &out), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.master.state->requests[1].final_authority_generation, 0);
	holder_no_debt();
}
UT_TEST(test_holder_S_to_N_real_native_PI_handoff)
{
	HolderFixture f;
	bool ok = holder_start_target(&f, PCM_STATE_S, PCM_STATE_X); UT_ASSERT(ok); if (!ok) return;
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_N); UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_PI);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 902); UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(f.master.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
	holder_no_debt();
}
/* Stop between actual owners to model interruption; no native post-state or
 * PCM publication is manufactured by these helpers. */
static ResourceXApplyResult holder_retained_native(HolderFixture *f, ClusterPcmOwnSnapshot *finished)
{
	ResourceXDecodedFrame image; ResourceXAcquireSSourceState phase; int32 id;
	ResourceXApplyResult result = cluster_pcm_lock_resource_x_acquire_s_source_snapshot_exact(
		&f->block, 0, &id, &f->prepared, &image, &phase);
	ClusterPcmOwnFinishRefusal refusal;
	ClusterPcmOwnResult own;
	if (result != RESOURCE_X_APPLY_APPLIED) return result;
	own = f->block.body.acquire_decision.source_node == cluster_node_id
		? cluster_bufmgr_pcm_own_finish_revoke_retain(&request_buf, &f->prepared,
			PageGetLSN((Page)image.body.acquire_image.original_image.page_bytes), finished, &refusal, NULL)
		: cluster_bufmgr_pcm_own_finish_s_holder_revoke(&request_buf, &f->prepared, finished, &refusal);
	UT_ASSERT_EQ(own, CLUSTER_PCM_OWN_OK);
	return own == CLUSTER_PCM_OWN_OK ? RESOURCE_X_APPLY_APPLIED : RESOURCE_X_APPLY_RECOVERY_BLOCKED;
}
static ResourceXApplyResult holder_publish_retained(HolderFixture *f)
{
	ClusterPcmOwnSnapshot finished;
	ResourceXApplyResult result = holder_retained_native(f, &finished);
	if (result != RESOURCE_X_APPLY_APPLIED) return result;
	return cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f->block, 0, 0, &f->prepared, &finished);
}
static void holder_clean_PI_carrier(HolderFixture *f)
{
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_N); UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_PI);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 902);
	/* Clean pages have no first-dirty WAL record. The original carrier
	 * still retains native and transport responsibility, never fake WAL. */
	UT_ASSERT_EQ(f->master.state->pi_carrier.pi_refs, 0);
	UT_ASSERT_EQ(f->master.state->pi_carrier.transport_refs, 1);
	UT_ASSERT_EQ(f->master.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
}
UT_TEST(test_holder_N_busy_preserves_same_service_until_content_or_IO_event)
{
	for (int mode = 0; mode < 2; mode++) for (int io = 0; io < 2; io++) {
		HolderFixture f; ResourceXSourceDrainWork next; ClusterPcmResourceXHolderImage before;
		bool ok = holder_start_target(&f, mode ? PCM_STATE_X : PCM_STATE_S, PCM_STATE_X);
		UT_ASSERT(ok); if (!ok) return;
		HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED); before = f.master.state->holder_image;
		if (io) pg_atomic_fetch_or_u32(&request_buf.state, BM_IO_IN_PROGRESS); else holder_content_busy = true;
		HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 901);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_REVOKING);
		UT_ASSERT(memcmp(&before, &f.master.state->holder_image, sizeof(before)) == 0);
		HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(!holder_probe(&next));
		holder_content_busy = false; pg_atomic_fetch_and_u32(&request_buf.state, ~BM_IO_IN_PROGRESS);
		cluster_pcm_lock_resource_x_source_drain_content_released();
		ok = holder_probe(&next); UT_ASSERT(ok); if (!ok) return;
		UT_ASSERT(memcmp(&next.request, &f.work.request, sizeof(next.request)) == 0);
		f.work = next; HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
		HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(!holder_probe(&next)); holder_clean_PI_carrier(&f); holder_no_debt();
	}
}
UT_TEST(test_holder_N_interrupted_after_native_commit_adopts_once)
{
	for (int mode = 0; mode < 2; mode++) {
		HolderFixture f; ClusterPcmOwnSnapshot finished;
		bool ok = holder_start_target(&f, mode ? PCM_STATE_X : PCM_STATE_S, PCM_STATE_X);
		UT_ASSERT(ok); if (!ok) return;
		HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
		HOLDER_EQ(holder_retained_native(&f, &finished), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(finished.pcm_state, PCM_STATE_N); UT_ASSERT_EQ(finished.flags, PCM_OWN_FLAG_REVOKING);
		UT_ASSERT_EQ(f.master.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
		UT_ASSERT_EQ(holder_slot(&f, true)->state, RESOURCE_X_INTENT_SLOT_EMPTY);
		HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
		HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); holder_clean_PI_carrier(&f); holder_no_debt();
	}
}
UT_TEST(test_holder_N_published_pair_keeps_native_fence_when_handoff_busy)
{
	for (int io = 0; io < 2; io++) {
		HolderFixture f; ClusterPcmResourceXPiCarrier before;
		bool ok = holder_start_target(&f, PCM_STATE_X, PCM_STATE_X); UT_ASSERT(ok); if (!ok) return;
		HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
		HOLDER_EQ(holder_publish_retained(&f), RESOURCE_X_APPLY_APPLIED); before = f.master.state->pi_carrier;
		if (io) pg_atomic_fetch_or_u32(&request_buf.state, BM_IO_IN_PROGRESS); else holder_content_busy = true;
		HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_REVOKING);
		UT_ASSERT(memcmp(&before, &f.master.state->pi_carrier, sizeof(before)) == 0);
		holder_content_busy = false; pg_atomic_fetch_and_u32(&request_buf.state, ~BM_IO_IN_PROGRESS);
		HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); holder_clean_PI_carrier(&f); holder_no_debt();
	}
}
UT_TEST(test_holder_N_interrupted_after_native_fence_release_rejoins_same_receipt)
{
	HolderFixture f; ClusterPcmOwnSnapshot pending, completed; ClusterPcmResourceXPiCarrier before; int32 id;
	bool ok = holder_start_target(&f, PCM_STATE_X, PCM_STATE_X); UT_ASSERT(ok); if (!ok) return;
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(holder_publish_retained(&f), RESOURCE_X_APPLY_APPLIED); before = f.master.state->pi_carrier;
	HOLDER_EQ(cluster_pcm_lock_resource_x_acquire_s_source_handoff_snapshot_exact(&f.block, 0, &id, &pending), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_release_pi_carrier_exact(id, &pending, &completed), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(completed.flags, 0);
	UT_ASSERT(memcmp(&before, &f.master.state->pi_carrier, sizeof(before)) == 0);
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_DUPLICATE);
	before.completed = completed;
	UT_ASSERT(memcmp(&before, &f.master.state->pi_carrier, sizeof(before)) == 0);
	holder_clean_PI_carrier(&f); holder_no_debt();
}
UT_TEST(test_holder_N_handoff_never_releases_successor_token_or_generation)
{
	for (int token = 0; token < 2; token++) {
		HolderFixture f; ClusterPcmResourceXPiCarrier before; uint64 generation, reservation;
		bool ok = holder_start_target(&f, PCM_STATE_X, PCM_STATE_X); UT_ASSERT(ok); if (!ok) return;
		HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
		HOLDER_EQ(holder_publish_retained(&f), RESOURCE_X_APPLY_APPLIED); before = f.master.state->pi_carrier;
		if (token) pg_atomic_fetch_add_u64(&request_own.reservation_token, 1);
		else pg_atomic_fetch_add_u64(&request_own.generation, 1);
		generation = cluster_pcm_own_gen_get(0); reservation = cluster_pcm_own_reservation_token_get(0);
		HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), generation);
		UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), reservation);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_REVOKING);
		UT_ASSERT(memcmp(&before, &f.master.state->pi_carrier, sizeof(before)) == 0);
		UT_ASSERT(holder_fuses > 0); UT_ASSERT_EQ(holder_pins, 0); complete_idle();
	}
}
UT_TEST(test_holder_N_work_completion_cannot_retire_unpublished_native_responsibility)
{
	HolderFixture f; ResourceXSourceDrainWork next;
	bool ok = holder_start_target(&f, PCM_STATE_X, PCM_STATE_X); UT_ASSERT(ok); if (!ok) return;
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_REVOKING);
	cluster_pcm_lock_resource_x_source_drain_content_released();
	ok = holder_probe(&next); UT_ASSERT(ok); if (!ok) return; f.work = next;
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!holder_probe(&next)); holder_clean_PI_carrier(&f); holder_no_debt();
}
UT_TEST(test_holder_N_transport_backpressure_and_callbacks_do_not_retire_PI)
{
	HolderFixture f; ClusterPcmResourceXPiCarrier before;
	bool ok = holder_start_target(&f, PCM_STATE_X, PCM_STATE_X); UT_ASSERT(ok); if (!ok) return;
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED); HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	before = f.master.state->pi_carrier;
	request_shared.rings[0].count = PGRAC_LMS_OUTBOUND_CAPACITY;
	UT_ASSERT_EQ(done_actual_lms_enqueue(0, holder_slot(&f, true), 83, 9999999), CLUSTER_LMS_ENQUEUE_FULL);
	UT_ASSERT_EQ(holder_slot(&f, true)->state, RESOURCE_X_INTENT_SLOT_ARMED);
	request_shared.rings[0].count = 0;
	for (int half = 0; half < 2; half++) {
		for (int sent = 0; sent < 2; sent++) {
			ResourceXIntentSlot slot = *holder_slot(&f, half), staged;
			uint32 index = request_shared.rings[0].head;
			fake_pcm_clock_us += 1000000;
			UT_ASSERT_EQ(done_actual_lms_enqueue(0, &slot, half ? 83 : 61, UINT64_C(900000000)), CLUSTER_LMS_ENQUEUE_ADMITTED);
			memcpy(&staged, request_shared.rings[0].ring[index].payload, sizeof(staged));
			cluster_lms_outbound_resource_x_send_complete(&staged, sent);
			UT_ASSERT_EQ(holder_slot(&f, half)->state, sent ? RESOURCE_X_INTENT_SLOT_EMPTY : RESOURCE_X_INTENT_SLOT_ARMED);
		}
	}
	UT_ASSERT(memcmp(&before, &f.master.state->pi_carrier, sizeof(before)) == 0);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); holder_clean_PI_carrier(&f); holder_no_debt();
}
UT_TEST(test_holder_N_finish_storage_ERROR_preserves_pair_fence_and_dirty_page)
{
	HolderFixture f; ClusterPcmResourceXHolderImage before;
	uint32 interrupt_holds = InterruptHoldoffCount;
	bool ok = holder_start_target(&f, PCM_STATE_X, PCM_STATE_X); UT_ASSERT(ok); if (!ok) return;
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED); before = f.master.state->holder_image;
	pg_atomic_fetch_or_u32(&request_buf.state, BM_DIRTY | BM_JUST_DIRTIED);
	holder_write_error = true;
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 901); UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_DIRTY | BM_JUST_DIRTIED), BM_VALID | BM_DIRTY | BM_JUST_DIRTIED);
	UT_ASSERT(memcmp(&before, &f.master.state->holder_image, sizeof(before)) == 0);
	UT_ASSERT(holder_fuses > 0); UT_ASSERT_EQ(holder_pins, 0);
	UT_ASSERT_EQ(holder_interrupt_repairs, 1); UT_ASSERT_EQ(holder_interrupt_releases, 1);
	UT_ASSERT_EQ(InterruptHoldoffCount, interrupt_holds); complete_idle();
}
UT_TEST(test_other_S_holder_real_native_N_emits_only_two_control_witnesses)
{
	HolderFixture f; ResourceXDecodedFrame witness; ResourceXAcquireSSourceState phase; int32 id;
	ClusterPcmOwnSnapshot native;
	bool ok = holder_start_role(&f, PCM_STATE_S, PCM_STATE_X, 2); UT_ASSERT(ok); if (!ok) return;
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_N); UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_PI);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 902);
	HOLDER_EQ(cluster_pcm_lock_resource_x_acquire_s_source_snapshot_exact(&f.block, 0, &id, &native, &witness, &phase), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(witness.kind, RESOURCE_X_WIRE_BLOCKED_TO_N); UT_ASSERT(!witness.body.acquire_witness.has_source_proof);
	UT_ASSERT_EQ(witness.body.acquire_witness.original_ack.action_node, 2);
	UT_ASSERT_EQ(witness.body.acquire_witness.original_ack.target_mode, PCM_STATE_N);
	UT_ASSERT_EQ(holder_slot(&f, false)->payload_bytes, RESOURCE_X_WITNESS_V4_BYTES);
	UT_ASSERT_EQ(holder_slot(&f, true)->payload_bytes, RESOURCE_X_WITNESS_V4_BYTES);
	UT_ASSERT_EQ(f.master.state->pi_carrier.pi_refs, 0); holder_no_debt();
	/* Native release is now proven by the retained control receipt; both
	 * independent send/settlement responsibilities still remain. */
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
}
UT_TEST(test_other_S_holder_busy_keeps_original_native_fence_until_event)
{
	HolderFixture f; ResourceXSourceDrainWork next;
	bool ok = holder_start_role(&f, PCM_STATE_S, PCM_STATE_X, 2); UT_ASSERT(ok); if (!ok) return;
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED); holder_content_busy = true;
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 901);
	HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!holder_probe(&next)); holder_content_busy = false;
	cluster_pcm_lock_resource_x_source_drain_content_released();
	ok = holder_probe(&next); UT_ASSERT(ok); if (!ok) return; f.work = next;
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_N); UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 902); holder_no_debt();
}
/* Full-A settlement uses a real conditional decision and CRC. The original
 * requester completion is an authenticated input here; native H and both
 * actual LMS callbacks are executed below. */
static bool
holder_control_settlement(HolderFixture *f, ResourceXDecodedFrame *request, int outcome)
{
	ResourceXDecodedFrame grant = f->block;
	ResourceXDecodedAcquireDecision decision = grant.body.acquire_decision;
	uint8 bytes[RESOURCE_X_SOURCE_COMPLETE_V11_BYTES]; uint16 length; ResourceXWireReject reject;
	grant.kind = RESOURCE_X_WIRE_AUTHORITY_GRANT;
	grant.common.action_node = grant.common.logical_assertion.requester_node;
	if (!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &grant, bytes, sizeof(bytes), &length, &reject)
		|| !cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, length, &grant, &reject)) return false;
	memset(request, 0, sizeof(*request));
	request->kind = RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2; request->has_acquire_decision = true;
	request->payload_bytes = RESOURCE_X_SOURCE_COMPLETE_V11_BYTES;
	request->common = f->block.common; request->common.outcome = outcome;
	request->body.acquire_completion.decision = decision;
	request->body.acquire_completion.decision_crc32c = grant.common.semantic_crc32c;
	request->body.acquire_completion.native_generation = decision.episode.base_generation
		+ (outcome == RESOURCE_X_OUTCOME_OK ? 1 : 0);
	if (outcome != RESOURCE_X_OUTCOME_QUIET_NO_IMAGE) {
		request->body.acquire_completion.page_lsn = 144;
		request->body.acquire_completion.page_scn = 236;
		request->body.acquire_completion.page_crc32c = 1234;
	}
	return cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, request, bytes, sizeof(bytes), &length, &reject)
		&& length == sizeof(bytes)
		&& cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N, bytes, length, request, &reject);
}
static void
holder_send_control(HolderFixture *f, bool image, bool sent)
{
	ResourceXIntentSlot slot = *holder_slot(f, image), staged;
	uint32 index = request_shared.rings[0].head;
	const uint8 *payload = image ? f->master.state->holder_image.payload : f->master.state->holder_status_intent.payload;
	UT_ASSERT(lms_outbound_resource_x_intent_payload_valid(&slot, payload));
	fake_pcm_clock_us += 1000000;
	UT_ASSERT_EQ(done_actual_lms_enqueue(0, &slot, image ? 83 : 61, UINT64_C(900000000)), CLUSTER_LMS_ENQUEUE_ADMITTED);
	memcpy(&staged, request_shared.rings[0].ring[index].payload, sizeof(staged));
	cluster_lms_outbound_resource_x_send_complete(&staged, sent);
}
UT_TEST(test_other_S_holder_retains_exact_quiet_native_receipt)
{
	HolderFixture f; ClusterPcmResourceXPiCarrier before; ClusterPcmOwnSnapshot quiet; int32 id;
	bool ok = holder_start_role(&f, PCM_STATE_S, PCM_STATE_X, 2); UT_ASSERT(ok); if (!ok) return;
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(f.master.state->pi_carrier.transport_refs, 1);
	UT_ASSERT_EQ(f.master.state->pi_carrier.pi_refs, 0);
	before = f.master.state->pi_carrier;
	HOLDER_EQ(cluster_pcm_lock_resource_x_acquire_s_source_handoff_snapshot_exact(&f.block, 0, &id, &quiet), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(id, 0); UT_ASSERT_EQ(quiet.flags, 0); UT_ASSERT_EQ(quiet.generation, 902);
	HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, &f.master.state->pi_carrier, sizeof(before)) == 0); holder_no_debt();
}
UT_TEST(test_other_S_holder_settlement_waits_real_work_and_both_callbacks)
{
	const int outcomes[] = {RESOURCE_X_OUTCOME_OK, RESOURCE_X_OUTCOME_QUIET, RESOURCE_X_OUTCOME_QUIET_NO_IMAGE};
	for (int n = 0; n < 3; n++) {
		HolderFixture f; ResourceXDecodedFrame request; ClusterPcmResourceXPiCarrier before;
		bool ok = holder_start_role(&f, PCM_STATE_S, PCM_STATE_X, 2); UT_ASSERT(ok); if (!ok) return;
		HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED); HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
		ok = holder_control_settlement(&f, &request, outcomes[n]); UT_ASSERT(ok); if (!ok) return;
		before = f.master.state->pi_carrier;
		HOLDER_EQ(cluster_pcm_lock_resource_x_acquire_read_source_settlement_exact(&request, 0, 61), RESOURCE_X_APPLY_LOCAL_WAIT);
		HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
		holder_send_control(&f, false, true); holder_send_control(&f, true, false);
		HOLDER_EQ(cluster_pcm_lock_resource_x_acquire_read_source_settlement_exact(&request, 0, 61), RESOURCE_X_APPLY_LOCAL_WAIT);
		UT_ASSERT(memcmp(&before, &f.master.state->pi_carrier, sizeof(before)) == 0);
		holder_send_control(&f, true, true);
		HOLDER_EQ(cluster_pcm_lock_resource_x_acquire_read_source_settlement_exact(&request, 0, 61), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(f.master.state->holder_image.valid, 0); UT_ASSERT_EQ(f.master.state->pi_carrier.transport_refs, 0);
		UT_ASSERT_EQ(f.master.state->settlement_retry.pending, 0);
		UT_ASSERT_EQ(f.master.state->holder_status_intent.slot.kind, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
		holder_send_control(&f, false, false);
		UT_ASSERT_EQ(f.master.state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
		holder_send_control(&f, false, true); UT_ASSERT_EQ(f.master.state->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
		holder_no_debt();
	}
}
UT_TEST(test_other_S_holder_old_settlement_does_not_touch_reused_native_descriptor)
{
	HolderFixture f; ResourceXDecodedFrame request; BufferDesc before; uint64 gen, token;
	bool ok = holder_start_role(&f, PCM_STATE_S, PCM_STATE_X, 2); UT_ASSERT(ok); if (!ok) return;
	HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED); HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
	ok = holder_control_settlement(&f, &request, RESOURCE_X_OUTCOME_QUIET_NO_IMAGE); UT_ASSERT(ok); if (!ok) return;
	HOLDER_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
	holder_send_control(&f, false, true); holder_send_control(&f, true, true);
	HOLDER_EQ(cluster_pcm_lock_resource_x_acquire_read_source_settlement_exact(&request, 0, 61), RESOURCE_X_APPLY_APPLIED);
	holder_send_control(&f, false, true);
	request_buf.pcm_state = PCM_STATE_S; request_buf.buffer_type = BUF_TYPE_SCUR;
	gen = pg_atomic_fetch_add_u64(&request_own.generation, 7) + 7;
	token = pg_atomic_fetch_add_u64(&request_own.reservation_token, 3) + 3;
	before = request_buf;
	HOLDER_EQ(cluster_pcm_lock_resource_x_acquire_read_source_settlement_exact(&request, 0, 61), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, &request_buf, sizeof(before)) == 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), gen); UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), token);
	holder_send_control(&f, false, true); holder_no_debt();
}
UT_TEST(test_other_S_holder_interruption_rejoins_only_original_native_receipt)
{
	for (int phase = 0; phase < 3; phase++) {
		HolderFixture f; ClusterPcmOwnSnapshot pending, completed;
		bool ok = holder_start_role(&f, PCM_STATE_S, PCM_STATE_X, 3); UT_ASSERT(ok); if (!ok) return;
		HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
		if (phase == 0) {
			HOLDER_EQ(holder_retained_native(&f, &completed), RESOURCE_X_APPLY_APPLIED);
		} else {
			HOLDER_EQ(holder_publish_retained(&f), RESOURCE_X_APPLY_APPLIED);
			pending = f.master.state->pi_carrier.completed;
			if (phase == 1) {
				holder_content_busy = true; HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_BAD_STATE);
				UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_REVOKING);
				holder_content_busy = false;
			} else UT_ASSERT_EQ(cluster_bufmgr_pcm_own_release_pi_carrier_exact(0, &pending, &completed), CLUSTER_PCM_OWN_OK);
		}
		HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_APPLIED);
		HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 902);
		UT_ASSERT_EQ(f.master.state->pi_carrier.completed.flags, 0); holder_no_debt();
	}
}
UT_TEST(test_other_S_holder_corrupt_receipt_cannot_release_native_fence)
{
	for (int fault = 0; fault < 6; fault++) {
		HolderFixture f;
		bool ok = holder_start_role(&f, PCM_STATE_S, PCM_STATE_X, 2); UT_ASSERT(ok); if (!ok) return;
		HOLDER_EQ(holder_capture(&f), RESOURCE_X_APPLY_APPLIED);
		HOLDER_EQ(holder_publish_retained(&f), RESOURCE_X_APPLY_APPLIED);
		if (fault == 0) f.master.state->pi_carrier.completed.generation++;
		if (fault == 1) f.master.state->pi_carrier.completed.reservation_token++;
		if (fault == 2) f.master.state->pi_carrier.completed.tag.blockNum++;
		if (fault == 3) f.master.state->pi_carrier.image_crc++;
		if (fault == 4) f.master.state->pi_carrier.pi_refs = 1;
		if (fault == 5) f.master.state->pi_carrier.reserved[0] = 1;
		HOLDER_EQ(holder_finish(&f), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_REVOKING);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 902); UT_ASSERT_EQ(f.master.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
		UT_ASSERT_EQ(holder_pins, 0); complete_idle();
	}
}
#if defined(NATIVE_HOLDER_WRITEBACK) || defined(NATIVE_MASTER_HOLDER_COMPOSITION)
int holder_clean_fixture_main(void)
#else
int main(void)
#endif
{
	UT_PLAN(33);
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
	UT_RUN(test_holder_X_to_N_real_native_PI_handoff);
	UT_RUN(test_holder_S_to_N_real_native_PI_handoff);
	UT_RUN(test_holder_N_busy_preserves_same_service_until_content_or_IO_event);
	UT_RUN(test_holder_N_interrupted_after_native_commit_adopts_once);
	UT_RUN(test_holder_N_published_pair_keeps_native_fence_when_handoff_busy);
	UT_RUN(test_holder_N_interrupted_after_native_fence_release_rejoins_same_receipt);
	UT_RUN(test_holder_N_handoff_never_releases_successor_token_or_generation);
	UT_RUN(test_holder_N_work_completion_cannot_retire_unpublished_native_responsibility);
	UT_RUN(test_holder_N_transport_backpressure_and_callbacks_do_not_retire_PI);
	UT_RUN(test_holder_N_finish_storage_ERROR_preserves_pair_fence_and_dirty_page);
	UT_RUN(test_other_S_holder_real_native_N_emits_only_two_control_witnesses);
	UT_RUN(test_other_S_holder_busy_keeps_original_native_fence_until_event);
	UT_RUN(test_other_S_holder_retains_exact_quiet_native_receipt);
	UT_RUN(test_other_S_holder_settlement_waits_real_work_and_both_callbacks);
	UT_RUN(test_other_S_holder_old_settlement_does_not_touch_reused_native_descriptor);
	UT_RUN(test_other_S_holder_interruption_rejoins_only_original_native_receipt);
	UT_RUN(test_other_S_holder_corrupt_receipt_cannot_release_native_fence);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
