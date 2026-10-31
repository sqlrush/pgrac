/* Actual resident N/PI to X: original request, image/disk, T1-T3 and C.
 * Authenticated peer, process scheduling and storage are controlled leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "storage/lwlock.h"
static void xd_content_release(LWLock *lock);
#define NATIVE_X_DISK_COMPOSITION
#include "test_cluster_native_resident_fill_fixture.inc"
#undef NATIVE_X_DISK_COMPOSITION
static void xd_content_release(LWLock *lock) { LWLockRelease(lock); }

static ResourceXApplyResult cluster_bufmgr_pcm_acquire_resident_x(
	BufferDesc *buf, const BufferTag *expected, uint64 r4, bool *barrier);

#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_xnode_profile.h"
#include "miscadmin.h"

static unsigned wc_legacy, wc_failures, wc_waits, wc_object_checks;
static bool wc_object_enabled, wc_object_valid = true;
static uint64 wc_r4 = 77, wc_session_id = 31;
static int32 wc_master;
static unsigned wc_unavailable, wc_pauses;
static bool wc_cancel_pause;
static ResourceXWriterPath wc_path = RESOURCE_X_WRITER_TARGET;
ResourceXWriterPath cluster_resource_x_writer_path_snapshot(uint64 *r4)
{
	*r4 = wc_r4; return wc_path;
}
static PcmXSessionAuthResult
wc_session(const BufferTag *tag, ResourceXGateSnapshot *gate, int32 *master, uint64 *session)
{
	/* Existing pre-use recheck also samples this local domain under content-X. */
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & BM_LOCKED, 0);
	UT_ASSERT(BufferTagsEqual(tag, &request_base.tag));
	if (wc_unavailable != 0) { wc_unavailable--; return PCM_X_SESSION_AUTH_FRESH_NOT_READY; }
	if (!cluster_pcm_lock_resource_x_gate_snapshot(gate) || gate->phase != RESOURCE_X_GATE_OPEN)
		return PCM_X_SESSION_AUTH_INVALID;
	*master = wc_master; *session = wc_session_id;
	return PCM_X_SESSION_AUTH_OK;
}
static ResourceXApplyResult
gcs_block_resource_x_target_acquire_internal_trace_impl(
	BufferDesc *buf, const BufferTag *tag, uint64 r4, uint64 native, uint64 token,
	bool join, uint64 *deadline, ResourceXAuxiliaryAcquireContext *aux,
	bool *creation, ResourceXAcquisitionRef *ref)
{
	(void)buf; (void)tag; (void)r4; (void)native; (void)token; (void)join;
	(void)deadline; (void)aux; (void)creation; (void)ref;
	wc_legacy++;
	/* Preserve the original driver's input rejection at this leaf. */
	if (deadline != NULL && *deadline == UINT64_MAX)
		return RESOURCE_X_APPLY_INVALID;
	if (aux != NULL) {
		UT_ASSERT(aux->active && !aux->reobserve);
		UT_ASSERT(BufferTagsEqual(tag, &aux->resource));
		UT_ASSERT(deadline == &aux->absolute_deadline_us);
	}
	return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
}
#define gcs_block_resource_x_gate_session_snapshot_result wc_session
static void wc_pause(long usecs)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0); UT_ASSERT_EQ(usecs, 1000);
	if (++wc_pauses > 2 || wc_cancel_pause) pg_re_throw();
}
#define pg_usleep wc_pause
#ifdef X_WRITER_PARENT_SOURCE
#include X_WRITER_PARENT_SOURCE
#else
#include "test_cluster_native_x_writer_gcs.inc"
#endif
#undef pg_usleep
#undef gcs_block_resource_x_gate_session_snapshot_result

static void wc_fail(ResourceXApplyResult result, BufferDesc *buf, const char *operation)
{
	(void)result; (void)buf; (void)operation;
	wc_failures++; pg_re_throw();
}
static void wc_close(const ResourceXGateSnapshot *gate)
{
	(void)cluster_pcm_lock_resource_x_gate_fail_closed_exact(gate);
}
static bool wc_wait(LWLock *lock, int32 id, uint32 n, bool *barrier)
{
	(void)lock; (void)id; (void)n; (void)barrier;
	if (++wc_waits > 2) pg_re_throw();
	return true;
}
static bool wc_object_begin(RelFileLocator locator, uint64 *revision)
{
	(void)locator; UT_ASSERT_EQ(fake_lwlock_depth, 0);
	*revision = 7; return true;
}
static bool wc_object_current(uint64 revision)
{
	UT_ASSERT_EQ(revision, 7); wc_object_checks++;
	return wc_object_valid;
}
#define cluster_bufmgr_should_pcm_track(buf) cluster_pcm_x_buffer_tag_tracked(&(buf)->tag, cluster_shared_catalog)
#define LockBufHdr stage_header_lock
#define ereport(level, rest) wc_fail(RESOURCE_X_APPLY_RECOVERY_BLOCKED, NULL, "ledger error report")
#define cluster_bufmgr_resource_x_writer_report_failure wc_fail
#define cluster_bufmgr_resource_x_fail_closed_exact wc_close
#define cluster_bufmgr_resource_x_wait_retry wc_wait
#define cluster_content_trace_pair_event(...) ((void)0)
#define cluster_enabled wc_object_enabled
#define cluster_shared_config wc_object_enabled
#define cluster_interconnect_tier CLUSTER_IC_TIER_1
#define cluster_smgr_which_for(...) 1
#define cluster_ko_peer_delivery_begin_v3 wc_object_begin
#define cluster_ko_peer_delivery_current_v3 wc_object_current
#include "test_cluster_native_x_writer_ledger.inc"
#undef cluster_ko_peer_delivery_current_v3
#undef cluster_ko_peer_delivery_begin_v3
#undef cluster_smgr_which_for
#undef cluster_interconnect_tier
#undef cluster_shared_config
#undef cluster_enabled
#undef cluster_content_trace_pair_event
#undef cluster_bufmgr_resource_x_wait_retry
#undef cluster_bufmgr_resource_x_fail_closed_exact
#undef cluster_bufmgr_resource_x_writer_report_failure
#undef cluster_bufmgr_should_pcm_track
#undef LockBufHdr
#undef ereport



static bool rx_barrier;
static bool rx_cancel_wait;
static void rx_deliver(void)
{
	ordinary_deliver();
	cluster_enabled = cluster_shared_config = true;
}
static ResourceXApplyResult
rx_begin(BufferDesc *buf, const ClusterPcmOwnSnapshot *base, uint64 token,
	uint8 path, uint8 mode, ClusterGcsFirstFill *request)
{
	ResourceXApplyResult result;
	request_base = *base; request_token = token; ordinary_active = request;
	result = cluster_gcs_block_acquire_begin(buf, base, token, path, mode, request);
	if (fake_cv_prepare_hook == ordinary_deliver) fake_cv_prepare_hook = rx_deliver;
	return result;
}
static bool rx_wait(LWLock *lock, int32 id, uint32 waits, bool *barrier)
{
	(void)lock; (void)id; (void)waits;
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	if (++wc_waits > 8 || rx_cancel_wait) pg_re_throw();
	if (rx_barrier && barrier != NULL) { *barrier = true; return false; }
	return true;
}
#define cluster_bufmgr_resource_x_wait_retry rx_wait
#define cluster_gcs_block_acquire_begin rx_begin
#define LockBufHdr stage_header_lock
#define GetPrivateRefCount(buffer) request_private_pin(buffer)
#define ereport(level, rest) ordinary_report(level)
#include "test_cluster_native_resident_x_driver.inc"
#ifdef RESIDENT_X_PARENT_DRIVER
#include RESIDENT_X_PARENT_DRIVER
#endif
#undef ereport
#undef GetPrivateRefCount
#undef LockBufHdr
#undef cluster_gcs_block_acquire_begin
#undef cluster_bufmgr_resource_x_wait_retry

static void
rx_writer_start(unsigned native, int source)
{
	ClusterGcsFirstFill scratch;
	resident_start(&scratch, native);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, request_base.generation,
		request_token, PCM_OWN_FLAG_GRANT_PENDING), CLUSTER_PCM_OWN_OK);
	ordinary_profile = source; ordinary_active = NULL;
	cluster_enabled = cluster_shared_config = cluster_gcs_block_local_cache = true;
	cluster_page_wal_shmem_init(); rf_enlarge_error = false;
	wc_legacy = wc_failures = wc_waits = wc_object_checks = 0;
	wc_object_enabled = false; wc_object_valid = true;
	wc_r4 = 77; wc_path = RESOURCE_X_WRITER_TARGET;
	wc_session_id = 31; wc_master = 0;
	wc_unavailable = wc_pauses = 0; wc_cancel_pause = false;
	rx_barrier = rx_cancel_wait = false;
	cluster_xnode_profile_enabled = cluster_update_trace_enabled = false;
	MemSet(cluster_bufmgr_pcm_x_writer_ledger, 0, sizeof(cluster_bufmgr_pcm_x_writer_ledger));
}
static ClusterPcmXWriterLedgerEntry *rx_prepare(bool *barrier)
{
	uint64 deadline = 0;
	ClusterPcmXWriterLedgerEntry *volatile entry = NULL;
	PG_TRY(); {
		entry = cluster_bufmgr_pcm_x_writer_prepare_target(&request_buf, &request_buf.tag,
			PCM_LOCK_MODE_X, 77, &deadline, barrier, NULL, NULL);
	} PG_CATCH(); { } PG_END_TRY();
	return entry;
}

#define RX_REQUIRE(call) do { bool ok = (call); UT_ASSERT(ok); \
	if (!ok) { complete_idle(); return; } } while (0)

static bool
rx_start(ClusterGcsFirstFill *request, unsigned native, int source)
{
	resident_start(request, native);
	if (native == 0) ResourceOwnerRememberBufferIO(CurrentResourceOwner, 1);
	ordinary_profile = source;
	rf_enlarge_error = false;
	cluster_enabled = cluster_shared_config = true;
	cluster_page_wal_shmem_init();
	if (cluster_gcs_block_acquire_begin(&request_buf, &request_base, request_token,
			native == 0 ? RESOURCE_X_ACQUIRE_FIRST_FILL : RESOURCE_X_ACQUIRE_RESIDENT, PCM_STATE_X, request) != RESOURCE_X_APPLY_APPLIED
		|| cluster_gcs_block_first_fill_wait(request) != RESOURCE_X_APPLY_APPLIED
		|| claim_get() != RESOURCE_X_APPLY_APPLIED)
		return false;
	cluster_enabled = cluster_shared_config = true;
	return cluster_bufmgr_pcm_own_acquire_stage_begin_exact(&request_buf, &request_base,
		request_token, native == 0 ? RESOURCE_X_ACQUIRE_FIRST_FILL : RESOURCE_X_ACQUIRE_RESIDENT,
		&request_admission, &acquire_claim, &prepared_claim)
		== RESOURCE_X_APPLY_APPLIED;
}

static void
rx_installed(int source, unsigned io_attempts)
{
	ResourceXAcquisitionRef ref;
	ref.assertion = prepared_claim.observation.request.logical_assertion;
	ref.formation = prepared_claim.observation.request.resource_formation;
	ref.acquisition_generation = prepared_claim.observation.request.assertion_sequence;
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X);
	UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_XCUR);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.writer_activation_token), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.resource_x_activation_generation), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & CLUSTER_PCM_OWN_SEMANTIC_BUF_STATE_MASK,
		BM_TAG_VALID | BM_VALID);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(&ref,
		31, 77, request_base.generation + 1));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT(memcmp(disk_shared.data, disk_bytes.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(disk_reads, source == 1 ? 1 : 0);
	UT_ASSERT_EQ(complete_io_calls, io_attempts);
	UT_ASSERT_EQ(complete_broadcasts, 1);
	UT_ASSERT_EQ(ordinary_sends, 1);
}

UT_TEST(test_resident_X_disk_and_remote_S_X_complete_original_native_owners)
{
	for (unsigned native = 1; native <= 2; native++)
		for (int source = 1; source <= 5; source += source == 1 ? 3 : 1) {
			ClusterGcsFirstFill request;
			RX_REQUIRE(rx_start(&request, native, source));
			RESIDENT_REQUIRE_EQ(rf_run(), RESOURCE_X_APPLY_APPLIED);
			rx_installed(source, 1);
			cluster_gcs_block_first_fill_end(&request); complete_idle();
		}
}
UT_TEST(test_resident_X_completed_replay_never_copies_or_commits_twice)
{
	for (int source = 1; source <= 5; source += source == 1 ? 3 : 1) {
		ClusterGcsFirstFill request;
		RX_REQUIRE(rx_start(&request, 2, source));
		RESIDENT_REQUIRE_EQ(rf_run(), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(rf_run(), RESOURCE_X_APPLY_DUPLICATE);
		rx_installed(source, 1);
		cluster_gcs_block_first_fill_end(&request); complete_idle();
	}
}
UT_TEST(test_resident_X_ERROR_before_IO_preserves_old_PI_and_quiets_same_E)
{
	ClusterGcsFirstFill request;
	RX_REQUIRE(rx_start(&request, 2, 4));
	rf_enlarge_error = true;
	UT_ASSERT(rf_catches());
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_ERROR | BM_IO_IN_PROGRESS), BM_VALID);
	UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_PI);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(disk_reads, 0);
	for (int i = 0; i < BLCKSZ; i++) UT_ASSERT_EQ((unsigned char)disk_shared.data[i], 0xA7);
	cluster_gcs_block_first_fill_end(&request); complete_idle();
}
UT_TEST(test_resident_X_storage_error_retires_only_original_input_IO)
{
	ClusterGcsFirstFill request;
	RX_REQUIRE(rx_start(&request, 2, 1)); disk_read_error = true;
	UT_ASSERT(rf_catches());
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_ERROR | BM_IO_IN_PROGRESS), BM_IO_ERROR);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	cluster_gcs_block_first_fill_end(&request); complete_idle();
}
UT_TEST(test_resident_X_ERROR_after_native_commit_finishes_same_C)
{
	for (int source = 1; source <= 5; source += source == 1 ? 3 : 1) {
		ClusterGcsFirstFill request;
		RX_REQUIRE(rx_start(&request, 2, source)); complete_after_io_error = true;
		UT_ASSERT(rf_catches()); rx_installed(source, 1);
		cluster_gcs_block_first_fill_end(&request); complete_idle();
	}
}
UT_TEST(test_resident_X_identity_refusal_keeps_old_bytes_and_no_C)
{
	for (int n = 0; n < 6; n++) {
		ClusterGcsFirstFill request; ClusterPcmOwnEntry own; uint32 state;
		RX_REQUIRE(rx_start(&request, 2, 4));
		if (n == 0) request_pins = 0;
		if (n == 1) request_buf.tag.blockNum++;
		if (n == 2) pg_atomic_write_u64(&request_own.reservation_token, request_token + 1);
		if (n == 3) pg_atomic_write_u64(&request_own.write_io_generation, 1);
		if (n == 4) request_proc.cluster_grd_generation++;
		if (n == 5) pg_atomic_fetch_or_u32(&request_buf.state, BM_IO_ERROR);
		own = request_own; state = pg_atomic_read_u32(&request_buf.state);
		UT_ASSERT(rf_run() != RESOURCE_X_APPLY_APPLIED);
		request_native_unchanged(&own, state);
		for (int i = 0; i < BLCKSZ; i++) UT_ASSERT_EQ((unsigned char)disk_shared.data[i], 0xA7);
		UT_ASSERT_EQ(complete_commits, 0);
		cluster_gcs_block_first_fill_end(&request); complete_idle();
	}
}


UT_TEST(test_ordinary_N_PI_X_prepare_activate_and_release_uses_one_full_E)
{
	for (unsigned native = 1; native <= 2; native++)
		for (int source = 1; source <= 5; source += source == 1 ? 3 : 1) {
			ClusterPcmXWriterLedgerEntry *entry;
			rx_writer_start(native, source);
			entry = rx_prepare(NULL);
			RX_REQUIRE(entry != NULL);
			UT_ASSERT_EQ(wc_legacy, 0); UT_ASSERT_EQ(wc_failures, 0);
			UT_ASSERT_EQ(ordinary_sends, 1);
			UT_ASSERT_EQ(disk_reads, source == 1 ? 1 : 0);
			UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X);
			LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE);
			UT_ASSERT(cluster_bufmgr_pcm_x_writer_activate(entry, true));
			cluster_bufmgr_pcm_x_writer_mark_releasing(entry);
			LWLockRelease(BufferDescriptorGetContentLock(&request_buf));
			cluster_bufmgr_pcm_x_writer_release(entry);
			UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(&request_buf) == NULL);
			UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
			complete_idle();
		}
}
UT_TEST(test_ordinary_N_PI_X_cancel_before_decision_quiets_same_request)
{
	rx_writer_start(2, 4); ordinary_cancel_at = 1;
	UT_ASSERT(rx_prepare(NULL) == NULL);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(wc_legacy, 0);
	if (ordinary_sends != 1) { complete_idle(); return; }

	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_PI);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_ERROR | BM_IO_IN_PROGRESS), BM_VALID);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, WAIT_QUIET);
	UT_ASSERT_EQ(disk_reads, 0);
	cluster_bufmgr_pcm_x_writer_exception_cleanup_all(); complete_idle();
}


UT_TEST(test_X_remote_first_fill_uses_same_image_and_native_X_continuation)
{
	for (int source = 4; source <= 5; source++) {
		ClusterGcsFirstFill request;
		RX_REQUIRE(rx_start(&request, 0, source));
		RESIDENT_REQUIRE_EQ(cluster_bufmgr_pcm_own_acquire_fill_image_exact(&request_buf,
			&request_base, &prepared_episode, &request_admission, &prepared_claim), RESOURCE_X_APPLY_APPLIED);
		rx_installed(source, 1);
		cluster_gcs_block_first_fill_end(&request); complete_idle();
	}
}
UT_TEST(test_ordinary_X_ERROR_after_copy_or_IO_keeps_exact_terminal_responsibility)
{
	for (int cut = 0; cut < 2; cut++) {
		rx_writer_start(2, 4);
		if (cut == 0) stage_after_content = rf_error_after_copy;
		else complete_after_io_error = true;
		UT_ASSERT(rx_prepare(NULL) == NULL);
		UT_ASSERT_EQ(ordinary_panics, 0);
		UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(wc_legacy, 0);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
		UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
		UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + cut);
		UT_ASSERT_EQ(request_buf.pcm_state, cut ? PCM_STATE_X : PCM_STATE_N);
		UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_ERROR | BM_IO_IN_PROGRESS),
			cut ? BM_VALID : BM_IO_ERROR);
		cluster_bufmgr_pcm_x_writer_exception_cleanup_all(); complete_idle();
	}
}
static void rx_cancel_capacity(void) { pg_re_throw(); }
UT_TEST(test_ordinary_X_cancel_before_enqueue_retires_only_unissued_reservation)
{
	rx_writer_start(2, 4);
	request_ring_locks[1].tranche = 0;
	request_shared.rings[1].count = PGRAC_LMS_OUTBOUND_CAPACITY;
	request_on_sleep = rx_cancel_capacity;
	UT_ASSERT(rx_prepare(NULL) == NULL);
	UT_ASSERT_EQ(ordinary_panics, 0); UT_ASSERT_EQ(ordinary_sends, 1);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_ERROR | BM_IO_IN_PROGRESS), BM_VALID);
	UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_PI);
	UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 0);
	cluster_bufmgr_pcm_x_writer_exception_cleanup_all(); complete_idle();
}
UT_TEST(test_ordinary_X_cancel_waiting_for_another_native_IO_never_aborts_it)
{
	ClusterPcmOwnEntry before; uint32 state;
	rx_writer_start(2, 1);
	pg_atomic_fetch_or_u32(&request_buf.state, BM_IO_IN_PROGRESS);
	pg_atomic_write_u64(&request_own.write_io_generation, 1);
	before = request_own; state = pg_atomic_read_u32(&request_buf.state);
	rx_cancel_wait = true;
	UT_ASSERT(rx_prepare(NULL) == NULL);
	UT_ASSERT_EQ(ordinary_sends, 0); UT_ASSERT_EQ(wc_legacy, 0);
	UT_ASSERT_EQ(ordinary_panics, 0); UT_ASSERT_EQ(wc_waits, 1);
	request_native_unchanged(&before, state);
	cluster_bufmgr_pcm_x_writer_exception_cleanup_all(); complete_idle();
}
UT_TEST(test_ordinary_X_barrier_refusal_waiting_for_existing_owner_leaves_no_ledger)
{
	bool barrier = false;
	rx_writer_start(2, 1);
	pg_atomic_write_u64(&request_own.write_io_generation, 1); rx_barrier = true;
	UT_ASSERT(rx_prepare(&barrier) == NULL); UT_ASSERT(barrier);
	UT_ASSERT_EQ(ordinary_sends, 0); UT_ASSERT_EQ(wc_legacy, 0);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(&request_buf) == NULL);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), 1);
	complete_idle();
}
UT_TEST(test_resident_X_C_callback_and_authenticated_ACK_retire_independently)
{
	for (int source = 1; source <= 5; source += source == 1 ? 3 : 1)
		for (int order = 0; order < 2; order++) {
			ClusterGcsFirstFill request; ResourceXIntentSlot slot;
			RX_REQUIRE(rx_start(&request, 2, source));
			RESIDENT_REQUIRE_EQ(rf_run(), RESOURCE_X_APPLY_APPLIED);
			slot = qn_stage();
			if (order == 0) {
				UT_ASSERT_EQ(rf_ack(false), RESOURCE_X_APPLY_APPLIED);
				UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
			} else {
				UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
				UT_ASSERT_EQ(rf_ack(false), RESOURCE_X_APPLY_APPLIED);
			}
			UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
				RESOURCE_X_DELIVERY_ACQUIRE_READ_SETTLED);
			UT_ASSERT(!qn_probe(&slot));
			UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X);
			UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, source == 1 ? 1 : 0);
			cluster_gcs_block_first_fill_end(&request); complete_idle();
		}
}


UT_TEST(test_old_completed_X_cleanup_never_revalidates_successor_local_writes)
{
	for (int settled = 0; settled < 2; settled++) {
		ClusterGcsFirstFill request; ClusterPcmXWriterLedgerEntry *entry;
		ClusterPcmOwnEntry own; uint32 state; PGIOAlignedBlock before;
		volatile bool caught = false;
		RX_REQUIRE(rx_start(&request, 2, 4));
		RESIDENT_REQUIRE_EQ(rf_run(), RESOURCE_X_APPLY_APPLIED);
		if (settled) {
			ResourceXIntentSlot slot = qn_stage();
			UT_ASSERT_EQ(rf_ack(false), RESOURCE_X_APPLY_APPLIED);
			UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
		}
		wc_r4 = 77; wc_path = RESOURCE_X_WRITER_TARGET; wc_master = 0; wc_session_id = 31;
		wc_unavailable = 0; wc_object_enabled = false;
		MemSet(cluster_bufmgr_pcm_x_writer_ledger, 0, sizeof(cluster_bufmgr_pcm_x_writer_ledger));
		entry = rx_prepare(NULL); RX_REQUIRE(entry != NULL);
		LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE);
		RX_REQUIRE(cluster_bufmgr_pcm_x_writer_activate(entry, true));
		disk_shared.data[BLCKSZ-1] ^= 1;
		pg_atomic_fetch_or_u32(&request_buf.state, BM_DIRTY | BM_JUST_DIRTIED);
		cluster_bufmgr_pcm_x_writer_mark_releasing(entry);
		LWLockRelease(BufferDescriptorGetContentLock(&request_buf));
		cluster_bufmgr_pcm_x_writer_release(entry);
		own = request_own; state = pg_atomic_read_u32(&request_buf.state);
		memcpy(before.data, disk_shared.data, BLCKSZ);
		PG_TRY(); {
			cluster_bufmgr_acquire_resident_scope_cleanup(&request_buf, &request_base,
				&prepared_episode, &prepared_claim);
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(!caught);
		request_native_unchanged(&own, state);
		UT_ASSERT(memcmp(before.data, disk_shared.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(ordinary_sends, 1);
		cluster_gcs_block_first_fill_end(&request); complete_idle();
	}
}


UT_TEST(test_X_handoff_observer_requires_original_E_and_existing_shared_handoff)
{
	ClusterGcsFirstFill request; ResourceXDeliveryClaim wrong; ResourceXDecodedAcquireIntent episode;
	ClusterPcmOwnEntry own; uint32 state;
	RX_REQUIRE(rx_start(&request, 2, 4));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_read_handed_off_exact(
		&prepared_claim, &prepared_episode), RESOURCE_X_APPLY_BAD_STATE);
	RESIDENT_REQUIRE_EQ(rf_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_read_handed_off_exact(
		&prepared_claim, &prepared_episode), RESOURCE_X_APPLY_APPLIED);
	own = request_own; state = pg_atomic_read_u32(&request_buf.state);
	for (int n = 0; n < 4; n++) {
		wrong = prepared_claim; episode = prepared_episode;
		if (n == 0) wrong.executor_sequence++;
		if (n == 1) wrong.observation.request.master_session_incarnation++;
		if (n == 2) episode.backend_generation++;
		if (n == 3) wrong.target.reservation_token++;
		UT_ASSERT(cluster_pcm_lock_resource_x_requester_acquire_read_handed_off_exact(
			&wrong, &episode) != RESOURCE_X_APPLY_APPLIED);
		request_native_unchanged(&own, state);
	}
	cluster_gcs_block_first_fill_end(&request); complete_idle();
}
UT_TEST(test_resident_X_uncovered_PI_wait_retains_bytes_carrier_and_same_prepared_E)
{
	ClusterGcsFirstFill request; ClusterPageWalBindingV1 wal;
	ClusterPcmOwnEntry own; uint32 state;
	wal = local_pi_setup(make_tag(850));
	RX_REQUIRE(rx_start(&request, 2, 4));
	wal.identity.locator = BufTagGetRelFileLocator(&request_base.tag);
	wal.identity.forknum = request_base.tag.forkNum; wal.identity.blockno = request_base.tag.blockNum;
	wal.source.claim.identity.origin_node_id = cluster_node_id;
	wal.source.claim.identity.origin_thread_id = cluster_node_id + 1;
	wal.source.claim.identity.origin_owner_incarnation = cluster_qvotec_get_self_incarnation();
	local_pi_writer = wal.source; local_pi_writer_ready = true;
	RX_REQUIRE(cluster_pcm_local_pi_record_first_v1(request_base.tag, &wal, &wal));
	own = request_own; state = pg_atomic_read_u32(&request_buf.state);
	UT_ASSERT_EQ(rf_run(), RESOURCE_X_APPLY_LOCAL_WAIT);
	request_native_unchanged(&own, state);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		RESOURCE_X_DELIVERY_ACQUIRE_PREPARED);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, 0);
	for (int i = 0; i < BLCKSZ; i++) UT_ASSERT_EQ((unsigned char)disk_shared.data[i], 0xA7);
	cluster_bufmgr_acquire_resident_scope_cleanup(&request_buf, &request_base,
		&prepared_episode, &prepared_claim);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_PI);
	cluster_gcs_block_first_fill_end(&request); complete_idle();
}
UT_TEST(test_ordinary_X_ERROR_inside_admitted_enqueue_quiets_original_E)
{
	rx_writer_start(2, 4); ordinary_send_error = true;
	UT_ASSERT(rx_prepare(NULL) == NULL);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(ordinary_panics, 0);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_PI);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, WAIT_QUIET);
	UT_ASSERT_EQ(disk_reads, 0);
	cluster_bufmgr_pcm_x_writer_exception_cleanup_all(); complete_idle();
}

int main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(18);
	UT_RUN(test_resident_X_disk_and_remote_S_X_complete_original_native_owners);
	UT_RUN(test_resident_X_completed_replay_never_copies_or_commits_twice);
	UT_RUN(test_resident_X_ERROR_before_IO_preserves_old_PI_and_quiets_same_E);
	UT_RUN(test_resident_X_storage_error_retires_only_original_input_IO);
	UT_RUN(test_resident_X_ERROR_after_native_commit_finishes_same_C);
	UT_RUN(test_resident_X_identity_refusal_keeps_old_bytes_and_no_C);
	UT_RUN(test_ordinary_N_PI_X_prepare_activate_and_release_uses_one_full_E);
	UT_RUN(test_ordinary_N_PI_X_cancel_before_decision_quiets_same_request);
	UT_RUN(test_X_remote_first_fill_uses_same_image_and_native_X_continuation);
	UT_RUN(test_ordinary_X_ERROR_after_copy_or_IO_keeps_exact_terminal_responsibility);
	UT_RUN(test_ordinary_X_cancel_before_enqueue_retires_only_unissued_reservation);
	UT_RUN(test_ordinary_X_cancel_waiting_for_another_native_IO_never_aborts_it);
	UT_RUN(test_ordinary_X_barrier_refusal_waiting_for_existing_owner_leaves_no_ledger);
	UT_RUN(test_resident_X_C_callback_and_authenticated_ACK_retire_independently);
	UT_RUN(test_old_completed_X_cleanup_never_revalidates_successor_local_writes);
	UT_RUN(test_X_handoff_observer_requires_original_E_and_existing_shared_handoff);
	UT_RUN(test_resident_X_uncovered_PI_wait_retains_bytes_carrier_and_same_prepared_E);
	UT_RUN(test_ordinary_X_ERROR_inside_admitted_enqueue_quiets_original_E);
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
