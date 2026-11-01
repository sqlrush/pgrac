/*-------------------------------------------------------------------------
 * test_cluster_k_p130_h07.c
 *    Ordinary cached SHARE versus native revoke owners.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_k_p130_h07.c
 *
 * NOTES
 *    OS locks and event scheduling are controlled leaves. Carrier settlement
 *    and the subsequent remote acquisition are outside this composition.
 *-------------------------------------------------------------------------
 */
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

#if defined(K_P130_EXPECT_ASSERT_OFF) && defined(USE_ASSERT_CHECKING)
#error "H07 assertion-OFF requires an assertion-disabled configuration"
#endif

static uint8 h07_mode;
static bool h07_retain;
static unsigned h07_begins, h07_finishes, h07_aborts, h07_busy, h07_cancels;
static ClusterPcmOwnSnapshot h07_before, h07_revoking, h07_retained;
static PGIOAlignedBlock h07_bytes;
static uint32 h07_state;

static void
h07_no_delivery(void)
{
	/* Unexpected protocol admission is a failure, never a fabricated reply. */
	UT_ASSERT(false); pg_re_throw();
}

static void
h07_begin(void)
{
	ClusterPcmOwnResult result = h07_mode == PCM_STATE_S
		? cluster_bufmgr_pcm_own_begin_s_revoke(&request_buf, &h07_before, &h07_revoking)
		: cluster_bufmgr_pcm_own_begin_x_revoke(&request_buf, &h07_before, &h07_revoking);
	UT_ASSERT_EQ(result, CLUSTER_PCM_OWN_OK); h07_begins++;
	UT_ASSERT_EQ(h07_revoking.generation, h07_before.generation);
	UT_ASSERT_EQ(h07_revoking.reservation_token, h07_before.reservation_token + 1);
	UT_ASSERT_EQ(h07_revoking.flags, PCM_OWN_FLAG_REVOKING);
}

static void
h07_abort(void)
{
	ClusterPcmOwnResult result = h07_mode == PCM_STATE_S
		? cluster_bufmgr_pcm_own_abort_s_revoke(&request_buf, &h07_revoking)
		: cluster_bufmgr_pcm_own_abort_x_revoke(&request_buf, &h07_revoking);
	UT_ASSERT_EQ(result, CLUSTER_PCM_OWN_OK); h07_aborts++;
}

static void
h07_finish(bool busy)
{
	ClusterPcmOwnSnapshot before, after, completed;
	ClusterPcmOwnFinishRefusal refusal;
	ClusterPcmOwnResult result;
	LWLock *reader_lock = NULL;
	uint32 state = pg_atomic_read_u32(&request_buf.state);
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&request_buf, &before), CLUSTER_PCM_OWN_OK);
	/* This is the controlled OS event: another holder worker cannot take X
	 * while the ordinary caller actually retains its tracked SHARE lock. */
	holder_content_busy = LWLockHeldByMeInMode(BufferDescriptorGetContentLock(&request_buf), LW_SHARED);
	UT_ASSERT_EQ(holder_content_busy, busy);
	if (busy) {
		/* Switch the controlled lock-owner context to the holder worker.
		 * The reader's SHARE remains busy; it is not the worker's own lock.
		 * This avoids making the original pin leaf accept nested actors. */
		UT_ASSERT_EQ(fake_lwlock_depth, 1);
		reader_lock = fake_lwlock_stack[0];
		fake_lwlock_depth = 0; fake_lwlock_stack[0] = NULL; fake_lwlock_held = NULL;
	}
	result = h07_mode == PCM_STATE_S
		? cluster_bufmgr_pcm_own_finish_s_holder_revoke(&request_buf, &h07_revoking, &completed, &refusal)
		: cluster_bufmgr_pcm_own_finish_revoke_retain(&request_buf, &h07_revoking,
			PageGetLSN(disk_shared.data), &completed, &refusal, NULL);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	if (busy) {
		fake_lwlock_stack[0] = reader_lock; fake_lwlock_mode_stack[0] = LW_SHARED;
		fake_lwlock_depth = 1; fake_lwlock_held = reader_lock; fake_lwlock_mode = LW_SHARED;
	}
	UT_ASSERT_EQ(result, busy ? CLUSTER_PCM_OWN_BUSY : CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(holder_pins, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state), state);
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&request_buf, &after), CLUSTER_PCM_OWN_OK);
	if (busy) {
		UT_ASSERT_EQ(refusal.reason, CLUSTER_PCM_OWN_FINISH_REFUSAL_CONTENT_LOCK);
		UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&before, &after)); h07_busy++;
	} else {
		h07_retained = completed; h07_finishes++;
		UT_ASSERT_EQ(completed.generation, h07_before.generation + 1);
		UT_ASSERT_EQ(completed.reservation_token, h07_revoking.reservation_token);
		UT_ASSERT_EQ(completed.pcm_state, PCM_STATE_N);
		UT_ASSERT_EQ(completed.buffer_type, BUF_TYPE_PI);
		UT_ASSERT_EQ(completed.flags, PCM_OWN_FLAG_REVOKING);
		UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&completed, &after));
	}
	UT_ASSERT(memcmp(h07_bytes.data, disk_shared.data, BLCKSZ) == 0);
}

static void
h07_cancel(void)
{
	h07_cancels++;
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_N);
	/* Explicit caller cancellation, not a local-wait timeout. Retain the
	 * native fence for its original carrier/settlement owner. */
	pg_re_throw();
}

static void
h07_wait(void)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT_EQ(ordinary_sends, 0); UT_ASSERT_EQ(complete_commits, 0);
	if (h07_retain) { h07_finish(false); rl_on_wait = h07_cancel; }
	else { h07_abort(); rl_on_wait = NULL; }
}

static void
h07_before_share(void)
{
	rl_on_share = NULL;
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	h07_begin(); rl_on_wait = h07_wait;
}

static void
h07_start(uint8 mode)
{
	rl_start(mode == PCM_STATE_S ? 3 : 4, 2); rl_cached(mode);
	h07_mode = mode; h07_retain = false;
	h07_begins = h07_finishes = h07_aborts = h07_busy = h07_cancels = 0;
	holder_pins = holder_fuses = 0; holder_content_busy = false;
	holder_after_prepare = NULL; holder_write_error = false;
	holder_interrupt_repairs = holder_interrupt_releases = 0;
	ordinary_delivery_transport = h07_no_delivery;
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&request_buf, &h07_before), CLUSTER_PCM_OWN_OK);
	h07_state = pg_atomic_read_u32(&request_buf.state);
	memcpy(h07_bytes.data, disk_shared.data, BLCKSZ);
}

static void
h07_census(const char *scenario)
{
	ClusterPcmOwnSnapshot live;
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&request_buf, &live), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(ordinary_sends, 0); UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT_EQ(complete_io_calls, 0); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(complete_broadcasts, 0); UT_ASSERT_EQ(ordinary_panics, 0);
	UT_ASSERT_EQ(request_shared.rings[1].count, 0); UT_ASSERT_EQ(request_shared.rings[1].draining, 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	if (h07_finishes != 0)
		UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&live, &h07_retained));
	UT_ASSERT_EQ(live.writer_activation_token, 0); UT_ASSERT_EQ(live.resource_x_activation_generation, 0);
	UT_ASSERT_EQ(holder_pins, 0); UT_ASSERT_EQ(holder_fuses, 0);
	UT_ASSERT_EQ(rl_legacy_releases, 0);
	UT_ASSERT(!ResourceOwnerHasBufferIO(CurrentResourceOwner, 1));
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state), h07_state);
	UT_ASSERT(memcmp(h07_bytes.data, disk_shared.data, BLCKSZ) == 0);
	printf("# H07 %s mode=%u base=" UINT64_FORMAT " gen=" UINT64_FORMAT
		" token=" UINT64_FORMAT " flags=%u native=%u type=%u crc=%u"
		" begin=%u abort=%u finish=%u busy=%u cancel=%u shares=%u waits=%u"
		" REQUEST=0 E/A/I/C=absent IO=0 reads=0 ring=0 delivery=0 pins=0\n",
		scenario, h07_mode, h07_before.generation, live.generation, live.reservation_token,
		live.flags, live.pcm_state, live.buffer_type, cluster_gcs_block_compute_checksum(disk_shared.data),
		h07_begins, h07_aborts, h07_finishes, h07_busy, h07_cancels, rl_share_locks, rl_waits);
	complete_idle();
}

static void
h07_cached(uint8 mode)
{
	h07_start(mode); UT_ASSERT(rl_run(NULL));
	UT_ASSERT_EQ(rl_share_locks, 1); UT_ASSERT_EQ(rl_waits, 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), h07_before.generation);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), h07_before.reservation_token);
	rl_actual_unlock(&request_buf);
	h07_census("cached");
}

static void
h07_pre_content(uint8 mode, bool retain)
{
	h07_start(mode); h07_retain = retain; rl_on_share = h07_before_share;
	UT_ASSERT_EQ(rl_run(NULL), !retain);
	UT_ASSERT_EQ(h07_begins, 1); UT_ASSERT_EQ(h07_finishes, retain ? 1 : 0);
	UT_ASSERT_EQ(h07_aborts, retain ? 0 : 1); UT_ASSERT_EQ(h07_cancels, retain ? 1 : 0);
	UT_ASSERT_EQ(rl_share_locks, retain ? 1 : 2); UT_ASSERT_EQ(rl_waits, retain ? 2 : 1);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), h07_before.generation + (retain ? 1 : 0));
	if (!retain) {
		UT_ASSERT(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(&request_buf), LW_SHARED));
		UT_ASSERT_EQ(request_buf.pcm_state, mode); UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
		rl_actual_unlock(&request_buf);
	} else UT_ASSERT(!LWLockHeldByMe(BufferDescriptorGetContentLock(&request_buf)));
	h07_census(retain ? "pre-retained" : "pre-aborted");
}

static void
h07_post_content(uint8 mode)
{
	h07_start(mode); UT_ASSERT(rl_run(NULL));
	UT_ASSERT(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(&request_buf), LW_SHARED));
	h07_begin(); h07_finish(true);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), h07_before.generation);
	UT_ASSERT_EQ(request_buf.pcm_state, mode);
	rl_actual_unlock(&request_buf); h07_finish(false);
	UT_ASSERT_EQ(h07_begins, 1); UT_ASSERT_EQ(h07_finishes, 1); UT_ASSERT_EQ(h07_busy, 1);
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_begin_s_revoke(&request_buf, &h07_before, &h07_revoking),
		mode == PCM_STATE_S ? CLUSTER_PCM_OWN_STALE : CLUSTER_PCM_OWN_INVALID);
	rl_on_wait = h07_cancel; UT_ASSERT(!rl_run(NULL));
	UT_ASSERT_EQ(rl_share_locks, 1); UT_ASSERT_EQ(rl_waits, 1); UT_ASSERT_EQ(h07_cancels, 1);
	UT_ASSERT(!LWLockHeldByMe(BufferDescriptorGetContentLock(&request_buf)));
	h07_census("post-retained");
}

UT_TEST(test_k_h07_cached_S_zero_request) { h07_cached(PCM_STATE_S); }
UT_TEST(test_k_h07_cached_X_zero_request) { h07_cached(PCM_STATE_X); }
UT_TEST(test_k_h07_pre_content_S_abort_revalidates) { h07_pre_content(PCM_STATE_S, false); }
UT_TEST(test_k_h07_pre_content_X_abort_revalidates) { h07_pre_content(PCM_STATE_X, false); }
UT_TEST(test_k_h07_pre_content_S_retained_fence_refuses_read) { h07_pre_content(PCM_STATE_S, true); }
UT_TEST(test_k_h07_pre_content_X_retained_fence_refuses_read) { h07_pre_content(PCM_STATE_X, true); }
UT_TEST(test_k_h07_post_content_S_revoke_waits_reader) { h07_post_content(PCM_STATE_S); }
UT_TEST(test_k_h07_post_content_X_revoke_waits_reader) { h07_post_content(PCM_STATE_X); }
int main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
#ifdef USE_ASSERT_CHECKING
	printf("# H07 USE_ASSERT_CHECKING=1\n");
#else
	printf("# H07 USE_ASSERT_CHECKING=0\n");
#endif
	UT_PLAN(8);
	UT_RUN(test_k_h07_cached_S_zero_request);
	UT_RUN(test_k_h07_cached_X_zero_request);
	UT_RUN(test_k_h07_pre_content_S_abort_revalidates);
	UT_RUN(test_k_h07_pre_content_X_abort_revalidates);
	UT_RUN(test_k_h07_pre_content_S_retained_fence_refuses_read);
	UT_RUN(test_k_h07_pre_content_X_retained_fence_refuses_read);
	UT_RUN(test_k_h07_post_content_S_revoke_waits_reader);
	UT_RUN(test_k_h07_post_content_X_revoke_waits_reader);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
