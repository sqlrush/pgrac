/* Ordinary resident SHARE: retain actual native reservation, requester,
 * installation and cleanup bodies. Peers, process and OS events are leaves. */
#include "test_cluster_native_resident_fill_fixture.inc"
#include "cluster/cluster_pcm_x_bufmgr.h"

#undef cluster_bufmgr_pcm_begin_grant_reservation_wait
static unsigned rl_waits, rl_share_locks;
static bool rl_barrier, rl_cancel_wait;
static void (*rl_on_wait)(void), (*rl_on_share)(void);
static void (*rl_after_deliver)(void);
static ClusterPageWalBindingV1 rl_pi;
static ClusterPcmLocalPiSnapshotV1 rl_pi_before;
static ClusterConf rl_conf = {.node_count = 4};
ClusterConf *ClusterConfShmem = &rl_conf;
bool cluster_recmerge_window_active = false;
static bool
rl_wait(LWLock *lock, int32 id, uint32 waits, bool *barrier)
{
	(void)waits;
	UT_ASSERT(lock == BufferDescriptorGetContentLock(&request_buf));
	UT_ASSERT_EQ(id, 0); UT_ASSERT_EQ(fake_lwlock_depth, 0);
	if (++rl_waits > 8) { UT_ASSERT(false); pg_re_throw(); }
	if (rl_cancel_wait) pg_re_throw();
	if (rl_on_wait != NULL) rl_on_wait();
	if (barrier != NULL && rl_barrier) { *barrier = true; return false; }
	return true;
}
static bool
rl_content(LWLock *lock, LWLockMode mode)
{
	if (mode == LW_SHARED) {
		rl_share_locks++;
		if (rl_on_share != NULL) rl_on_share();
	}
	return LWLockAcquire(lock, mode);
}
static bool rl_skip(const char *name)
{
	return rl_barrier && strcmp(name, "cluster-pcm-share-barrier-refuse-after-acquire") == 0;
}
static void rl_fail(void) { ordinary_report(ERROR); }
static void rl_cancel(void) { pg_re_throw(); }
static void
rl_deliver(void)
{
	ordinary_deliver();
	/* Building an independent peer frame resets fixture globals, not the
	 * real requester process configuration. Restore the subject's leaves. */
	cluster_enabled = cluster_shared_config = true;
	if (rl_after_deliver != NULL) rl_after_deliver();
}
static ResourceXApplyResult
rl_begin(BufferDesc *buf, const ClusterPcmOwnSnapshot *base, uint64 token, ClusterGcsFirstFill *request)
{
	ResourceXApplyResult result;
	request_base = *base; request_token = token; ordinary_active = request;
	result = cluster_gcs_block_resident_read_begin(buf, base, token, request);
	if (fake_cv_prepare_hook == ordinary_deliver) fake_cv_prepare_hook = rl_deliver;
	return result;
}
#define cluster_pcm_own_report_bump_failure(...) rl_fail()
#define cluster_bufmgr_resource_x_wait_retry rl_wait
#define LockBufHdr stage_header_lock
#define LWLockAcquireOrWait LWLockAcquire
#define cluster_pcm_own_reclaim_read_image_exact(...) (CLUSTER_PCM_OWN_INVALID)
#include "test_cluster_native_resident_lock_reserve.inc"
#undef cluster_pcm_own_reclaim_read_image_exact
#undef LWLockAcquireOrWait

#define GetPrivateRefCount(buffer) request_private_pin(buffer)
#define ereport(level, rest) ordinary_report(level)
#define LWLockAcquire rl_content
#define CLUSTER_INJECTION_POINT(name) ((void)0)
#define cluster_injection_should_skip rl_skip
#define cluster_gcs_block_resident_read_begin rl_begin
#ifdef RESIDENT_LOCK_PARENT_RED
#include RESIDENT_LOCK_PARENT_SOURCE
#else
#include "test_cluster_native_resident_lock.inc"
#endif
#undef cluster_gcs_block_resident_read_begin
#undef cluster_injection_should_skip
#undef CLUSTER_INJECTION_POINT
#undef LWLockAcquire
#undef ereport
#undef GetPrivateRefCount
#undef LockBufHdr
#undef cluster_bufmgr_resource_x_wait_retry
#undef cluster_pcm_own_report_bump_failure

/* Execute the actual outer unlock and cache-OFF tail, not bare LWLockRelease.
 * This reader has no writer ledger. Any legacy release/mirror is forbidden. */
typedef struct ClusterPcmXWriterLedgerEntry ClusterPcmXWriterLedgerEntry;
static unsigned rl_legacy_releases, rl_service_wakes;
static void
rl_no_writer(ClusterPcmXWriterLedgerEntry *writer)
{
	UT_ASSERT(writer == NULL);
}
static void
rl_legacy_release(void)
{
	rl_legacy_releases++;
	rl_fail();
}
#define cluster_bufmgr_pcm_x_writer_find(buf) ((ClusterPcmXWriterLedgerEntry *)NULL)
#define cluster_bufmgr_pcm_x_writer_mark_releasing rl_no_writer
#define cluster_bufmgr_pcm_x_writer_release rl_no_writer
#define cluster_bufmgr_first_fill_service_wakeup(buf) ((void)rl_service_wakes++)
#define cluster_bufmgr_should_pcm_track(buf) cluster_pcm_x_buffer_tag_tracked(&(buf)->tag, cluster_shared_catalog)
#define cluster_pcm_own_release_read_image_exact(...) (rl_fail(), CLUSTER_PCM_OWN_INVALID)
#define cluster_pcm_own_report_bump_failure(...) rl_fail()
#define cluster_pcm_lock_unlock_content_buffer(...) rl_legacy_release()
#define cluster_pcm_own_transition(...) rl_fail()
#include "test_cluster_native_resident_unlock.inc"
#undef cluster_pcm_own_transition
#undef cluster_pcm_lock_unlock_content_buffer
#undef cluster_pcm_own_report_bump_failure
#undef cluster_pcm_own_release_read_image_exact
#undef cluster_bufmgr_should_pcm_track
#undef cluster_bufmgr_first_fill_service_wakeup
#undef cluster_bufmgr_pcm_x_writer_release
#undef cluster_bufmgr_pcm_x_writer_mark_releasing
#undef cluster_bufmgr_pcm_x_writer_find

static void
rl_start(unsigned native, int source)
{
	ClusterGcsFirstFill scratch;
	resident_start(&scratch, native);
	/* This fixture's initial seed has never been issued. Start the subject
	 * at a stable resident page, before its real native reservation. */
	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, request_base.generation,
		request_token, PCM_OWN_FLAG_GRANT_PENDING), CLUSTER_PCM_OWN_OK);
	request_base.reservation_token = request_token;
	ordinary_profile = source; ordinary_active = NULL;
	rl_waits = rl_share_locks = 0; rl_barrier = rl_cancel_wait = false;
	rl_on_wait = rl_on_share = NULL;
	rl_after_deliver = NULL;
	cluster_enabled = cluster_shared_config = cluster_gcs_block_local_cache = true;
	cluster_page_wal_shmem_init(); rf_enlarge_error = false;
}
static bool
rl_run(bool *barrier)
{
	volatile bool result = false;
	PG_TRY(); { result = cluster_bufmgr_pcm_lock_shared(&request_buf, barrier); }
	PG_CATCH(); { result = false; } PG_END_TRY();
	UT_ASSERT_EQ(ordinary_panics, 0);
	return result;
}
static void
rl_release(void)
{
	if (LWLockHeldByMe(BufferDescriptorGetContentLock(&request_buf)))
		LWLockRelease(BufferDescriptorGetContentLock(&request_buf));
	complete_idle();
}
static void
rl_cached(uint8 mode)
{
	request_buf.pcm_state = mode;
	request_buf.buffer_type = mode == PCM_STATE_S ? BUF_TYPE_SCUR : BUF_TYPE_XCUR;
	pg_atomic_write_u32(&request_own.flags, 0);
}
static void rl_install_cover(void) { rl_on_wait = NULL; rl_cached(PCM_STATE_S); }
static void
rl_finish_write(void)
{
	rl_on_wait = NULL;
	pg_atomic_fetch_and_u32(&request_buf.state, ~BM_IO_IN_PROGRESS);
	pg_atomic_write_u64(&request_own.write_io_generation, 2);
}
static void
rl_preserve_pi(void)
{
	rl_pi.identity.locator = BufTagGetRelFileLocator(&request_base.tag);
	rl_pi.identity.forknum = request_base.tag.forkNum;
	rl_pi.identity.blockno = request_base.tag.blockNum;
	rl_pi.source.claim.identity.origin_node_id = cluster_node_id;
	rl_pi.source.claim.identity.origin_thread_id = cluster_node_id + 1;
	rl_pi.source.claim.identity.origin_owner_incarnation = cluster_qvotec_get_self_incarnation();
	local_pi_writer = rl_pi.source; local_pi_writer_ready = true;
	UT_ASSERT(cluster_pcm_local_pi_record_first_v1(request_base.tag, &rl_pi, &rl_pi));
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(request_base.tag, &rl_pi_before));
}
static void
rl_lose_cover(void)
{
	rl_on_share = NULL;
	request_buf.pcm_state = PCM_STATE_N; request_buf.buffer_type = BUF_TYPE_CURRENT;
	pg_atomic_fetch_add_u64(&request_own.generation, 1);
	request_base.generation++;
}

UT_TEST(test_ordinary_resident_sources_install_before_content_share)
{
	for (unsigned native = 1; native <= 2; native++)
		for (int source = 0; source <= 3; source += source == 0 ? 2 : 1) {
			rl_start(native, source);
			UT_ASSERT(rl_run(NULL));
			UT_ASSERT(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(&request_buf), LW_SHARED));
			rf_installed(source, 1); UT_ASSERT_EQ(rl_share_locks, 1);
			UT_ASSERT_EQ(ordinary_leaves, 1); rl_release();
		}
}
UT_TEST(test_cached_S_and_X_read_without_request_or_input_IO)
{
	for (uint8 mode = PCM_STATE_S; mode <= PCM_STATE_X; mode++) {
		rl_start(mode == PCM_STATE_S ? 3 : 4, 2); rl_cached(mode);
		UT_ASSERT(rl_run(NULL)); UT_ASSERT_EQ(ordinary_sends, 0);
		UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(disk_reads, 0);
		UT_ASSERT_EQ(rl_share_locks, 1); rl_release();
	}
}
UT_TEST(test_cached_cover_lost_before_content_requalifies_before_one_request)
{
	rl_start(3, 2); rl_cached(PCM_STATE_S); rl_on_share = rl_lose_cover;
	UT_ASSERT(rl_run(NULL)); UT_ASSERT_EQ(rl_share_locks, 2);
	rf_installed(2, 1); rl_release();
}
UT_TEST(test_local_reservation_follower_waits_for_cover_without_request)
{
	rl_start(1, 2);
	pg_atomic_write_u32(&request_own.flags, PCM_OWN_FLAG_GRANT_PENDING);
	rl_on_wait = rl_install_cover;
	UT_ASSERT(rl_run(NULL)); UT_ASSERT_EQ(rl_waits, 1);
	UT_ASSERT_EQ(ordinary_sends, 0); UT_ASSERT_EQ(complete_commits, 0); rl_release();
}
UT_TEST(test_cancel_before_send_retires_only_own_reservation_and_keeps_old_page)
{
	rl_start(2, 2); request_ring_locks[1].tranche = 0;
	request_shared.rings[1].count = PGRAC_LMS_OUTBOUND_CAPACITY;
	request_on_sleep = rl_cancel; /* interrupt at the original capacity wait */
	UT_ASSERT(!rl_run(NULL));
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); resident_unchanged_bytes();
	UT_ASSERT_EQ(ordinary_leaves, 1); rl_release();
}
UT_TEST(test_cancel_after_send_keeps_old_page_and_quiets_original_E)
{
	rl_start(2, 2); ordinary_cancel_at = 1;
	UT_ASSERT(!rl_run(NULL)); UT_ASSERT_EQ(ordinary_sends, 1);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); resident_unchanged_bytes();
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, WAIT_QUIET);
	UT_ASSERT_EQ(ordinary_leaves, 1); rl_release();
}
UT_TEST(test_original_disk_error_is_not_returned_as_content_authority)
{
	rl_start(2, 0); disk_read_error = true;
	UT_ASSERT(!rl_run(NULL)); UT_ASSERT_EQ(ordinary_sends, 1);
	UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(rl_share_locks, 0);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_ERROR | BM_IO_IN_PROGRESS), BM_IO_ERROR);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1)); rl_release();
}
UT_TEST(test_barrier_refusal_after_C_keeps_completed_node_cover_without_content)
{
	bool refused = false;
	rl_start(1, 2); rl_barrier = true;
	UT_ASSERT(!rl_run(&refused)); UT_ASSERT(refused);
	UT_ASSERT_EQ(rl_share_locks, 0); rf_installed(2, 1); rl_release();
}
UT_TEST(test_error_after_C_finishes_original_IO_without_new_request)
{
	rl_start(2, 2); complete_after_io_error = true;
	UT_ASSERT(!rl_run(NULL)); rf_installed(2, 1);
	UT_ASSERT_EQ(ordinary_leaves, 1); rl_release();
}
UT_TEST(test_resident_read_waits_for_existing_write_before_reserving_or_requesting)
{
	rl_start(2, 2);
	pg_atomic_fetch_or_u32(&request_buf.state, BM_IO_IN_PROGRESS);
	pg_atomic_write_u64(&request_own.write_io_generation, 1);
	rl_on_wait = rl_finish_write;
	UT_ASSERT(rl_run(NULL)); UT_ASSERT_EQ(rl_waits, 1);
	rf_installed(2, 1); rl_release();
}
UT_TEST(test_cancel_waiting_for_existing_write_does_not_touch_its_native_IO)
{
	ClusterPcmOwnEntry own; uint32 state;
	rl_start(2, 2);
	pg_atomic_fetch_or_u32(&request_buf.state, BM_IO_IN_PROGRESS);
	pg_atomic_write_u64(&request_own.write_io_generation, 1);
	own = request_own; state = pg_atomic_read_u32(&request_buf.state);
	rl_cancel_wait = true;
	UT_ASSERT(!rl_run(NULL)); request_native_unchanged(&own, state);
	UT_ASSERT_EQ(ordinary_sends, 0); UT_ASSERT_EQ(ordinary_panics, 0);
	rl_release();
}
UT_TEST(test_PI_wait_cancel_keeps_original_PI_and_quiets_prepared_E)
{
	ClusterPcmLocalPiSnapshotV1 after;
	rl_pi = local_pi_setup(make_tag(850));
	rl_start(2, 2); rl_after_deliver = rl_preserve_pi; rl_cancel_wait = true;
	UT_ASSERT(!rl_run(NULL)); UT_ASSERT_EQ(rl_waits, 1);
	UT_ASSERT_EQ(ordinary_sends, 1); resident_unchanged_bytes();
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		RESOURCE_X_DELIVERY_ACQUIRE_UNARMED_QUIET);
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(request_base.tag, &after));
	UT_ASSERT(memcmp(&rl_pi_before, &after, sizeof(after)) == 0);
	UT_ASSERT_EQ(ordinary_leaves, 1); rl_release();
}
UT_TEST(test_optional_cache_off_still_uses_resident_S_and_X_read_authority)
{
	for (uint8 mode = PCM_STATE_S; mode <= PCM_STATE_X; mode++) {
		rl_start(mode == PCM_STATE_S ? 3 : 4, 2); rl_cached(mode);
		cluster_gcs_block_local_cache = false;
		UT_ASSERT(rl_run(NULL)); UT_ASSERT_EQ(ordinary_sends, 0);
		UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(disk_reads, 0);
		rl_release();
	}
}
UT_TEST(test_missing_native_owner_is_rejected_before_reservation)
{
	for (int n = 0; n < 3; n++) {
		ClusterPcmOwnEntry own; uint32 state;
		rl_start(2, 2);
		if (n == 0) request_pins = 0;
		if (n == 1) CurrentResourceOwner = NULL;
		if (n == 2) request_proc.backendId++;
		own = request_own; state = pg_atomic_read_u32(&request_buf.state);
		UT_ASSERT(!rl_run(NULL)); request_native_unchanged(&own, state);
		UT_ASSERT_EQ(ordinary_sends, 0); UT_ASSERT_EQ(ordinary_leaves, 0);
		CurrentResourceOwner = original_io_owner; request_pins = 1; rl_release();
	}
}
UT_TEST(test_real_cache_OFF_unlock_preserves_readers_resident_S_or_X_cover)
{
	for (unsigned native = 2; native <= 4; native++) {
		ClusterPcmOwnEntry own; uint32 state;
		volatile bool unlocked = false;
		rl_start(native, 2);
		if (native >= 3) rl_cached(native == 3 ? PCM_STATE_S : PCM_STATE_X);
		cluster_gcs_block_local_cache = false;
		UT_ASSERT(rl_run(NULL));
		UT_ASSERT(cluster_pcm_is_active());
		own = request_own; state = pg_atomic_read_u32(&request_buf.state);
		rl_legacy_releases = rl_service_wakes = 0;
		PG_TRY(); { rl_actual_unlock(&request_buf); unlocked = true; }
		PG_CATCH(); { unlocked = false; } PG_END_TRY();
		UT_ASSERT(unlocked); request_native_unchanged(&own, state);
		UT_ASSERT_EQ(rl_legacy_releases, 0); UT_ASSERT_EQ(rl_service_wakes, 1);
		UT_ASSERT(!LWLockHeldByMe(BufferDescriptorGetContentLock(&request_buf)));
		rl_release();
	}
}

int
main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(15);
	UT_RUN(test_ordinary_resident_sources_install_before_content_share);
	UT_RUN(test_cached_S_and_X_read_without_request_or_input_IO);
	UT_RUN(test_cached_cover_lost_before_content_requalifies_before_one_request);
	UT_RUN(test_local_reservation_follower_waits_for_cover_without_request);
	UT_RUN(test_cancel_before_send_retires_only_own_reservation_and_keeps_old_page);
	UT_RUN(test_cancel_after_send_keeps_old_page_and_quiets_original_E);
	UT_RUN(test_original_disk_error_is_not_returned_as_content_authority);
	UT_RUN(test_barrier_refusal_after_C_keeps_completed_node_cover_without_content);
	UT_RUN(test_error_after_C_finishes_original_IO_without_new_request);
	UT_RUN(test_resident_read_waits_for_existing_write_before_reserving_or_requesting);
	UT_RUN(test_cancel_waiting_for_existing_write_does_not_touch_its_native_IO);
	UT_RUN(test_PI_wait_cancel_keeps_original_PI_and_quiets_prepared_E);
	UT_RUN(test_optional_cache_off_still_uses_resident_S_and_X_read_authority);
	UT_RUN(test_missing_native_owner_is_rejected_before_reservation);
	UT_RUN(test_real_cache_OFF_unlock_preserves_readers_resident_S_or_X_cover);
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
