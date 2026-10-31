/* Actual first-fill disk X, writer fences, T1/T2/T3, original IO and cleanup.
 * Host storage, authenticated peers and process-exit scheduling are leaves.
 * Controlled FATAL callbacks are not a native process-exit qualification.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "storage/lwlock.h"
static void xd_content_release(LWLock *lock);
#define NATIVE_X_DISK_COMPOSITION
#include "test_cluster_native_disk_episode_fixture.inc"
#undef NATIVE_X_DISK_COMPOSITION

static unsigned xd_boundary, xd_fault;

static void
xd_content_release(LWLock *lock)
{
	bool hit;
	LWLockRelease(lock);
	if (xd_boundary == 0 || cluster_pcm_own_gen_get(0) != prepared_episode.base_generation + 1)
		return;
	hit = xd_boundary == 1
		? pg_atomic_read_u64(&request_own.writer_activation_token) == prepared_episode.reservation_token
		: xd_boundary == 2 && pg_atomic_read_u64(&request_own.writer_activation_token) == 0
			&& request_entry()->resource_x_bootstrap_round.phase == 1;
	if (!hit) return;
	xd_boundary = 0;
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	if (xd_fault >= 3) {
		/* Corrupt only an observed native identity at this exact cut. The
		 * original continuation and cleanup must retain IO and refuse it. */
		if (xd_fault == 3)
			pg_atomic_fetch_add_u64(&request_own.writer_activation_token, 1);
		else if (xd_fault == 4)
			pg_atomic_fetch_add_u64(&request_own.resource_x_activation_generation, 1);
		else
			pg_atomic_fetch_add_u64(&request_own.write_io_generation, 2);
		return;
	}
	if (xd_fault == 1) pg_re_throw();
	else dg_fatal();
}

#define XD_REQUIRE(condition) do { \
	bool ok = (condition); UT_ASSERT(ok); if (!ok) return; \
} while (0)

static bool
xd_start(EvidenceFixture *f)
{
	bool ok;
	if (!de_start(f)) return false;
	evidence_disk_watermark_scn = 90;
	ok = execution_start(f, 1, 0);
	evidence_disk_watermark_scn = 0;
	if (!ok) return false;
	input_grant = f->decision;
	input_grant.common.sender_connection_generation = PGRAC_RESOURCE_X_RETAINED_SENDER_GENERATION;
	input_grant.common.semantic_crc32c = 0;
	if (!evidence_wire(&input_grant) || !dg_locate_settlement()) return false;
	armed_capability = prepared_claim;
	xd_boundary = xd_fault = 0;
	return true;
}

static ResourceXApplyResult xd_run(void) { return de_run(); }

static void
xd_installed(void)
{
	ResourceXAcquisitionRef ref;
	ref.assertion = prepared_claim.observation.request.logical_assertion;
	ref.formation = prepared_claim.observation.request.resource_formation;
	ref.acquisition_generation = prepared_claim.observation.request.assertion_sequence;
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.writer_activation_token), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.resource_x_activation_generation), 0);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X);
	UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_XCUR);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & CLUSTER_PCM_OWN_SEMANTIC_BUF_STATE_MASK,
		BM_TAG_VALID | BM_VALID);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(&ref,
		31, 77, request_base.generation + 1));
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT(memcmp(disk_shared.data, disk_bytes.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(disk_reads, 1);
	UT_ASSERT_EQ(complete_broadcasts, 1);
	UT_ASSERT_EQ(quiet_aborts + ua_aborts, 0);
	UT_ASSERT_EQ(request_shared.rings[1].count, 1);
	dg_stack_empty();
}

UT_TEST(test_x_disk_reads_verified_bytes_once_and_finishes_original_native_owners)
{
	EvidenceFixture f;
	XD_REQUIRE(xd_start(&f));
	UT_ASSERT_EQ(xd_run(), RESOURCE_X_APPLY_APPLIED);
	xd_installed();
	UT_ASSERT_EQ(complete_io_calls, 1);
}

UT_TEST(test_x_disk_completed_replay_never_reads_or_bumps_native_again)
{
	EvidenceFixture f;
	XD_REQUIRE(xd_start(&f));
	XD_REQUIRE(xd_run() == RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_acquire_continue_disk_exact(&request_buf,
		&request_base, &prepared_episode, &request_admission, &prepared_claim), RESOURCE_X_APPLY_DUPLICATE);
	xd_installed();
	UT_ASSERT_EQ(complete_io_calls, 1);
}

UT_TEST(test_x_disk_native_commit_ERROR_or_exit_resumes_same_fenced_T2_T3)
{
	for (int fault = 1; fault <= 2; fault++) {
		EvidenceFixture f;
		XD_REQUIRE(xd_start(&f));
		xd_boundary = 1; xd_fault = fault;
		if (fault == 1) UT_ASSERT(de_error()); else de_exit();
		UT_ASSERT_EQ(xd_boundary, 0);
		xd_installed();
		UT_ASSERT_EQ(complete_io_calls, 1);
	}
}

UT_TEST(test_x_disk_open_native_fences_ERROR_or_exit_still_requires_original_T3)
{
	for (int fault = 1; fault <= 2; fault++) {
		EvidenceFixture f;
		XD_REQUIRE(xd_start(&f));
		xd_boundary = 2; xd_fault = fault;
		if (fault == 1) UT_ASSERT(de_error()); else de_exit();
		UT_ASSERT_EQ(xd_boundary, 0);
		xd_installed();
		UT_ASSERT_EQ(complete_io_calls, 1);
	}
}

UT_TEST(test_x_disk_original_IO_ERROR_before_or_after_termination_finishes_once)
{
	for (int after = 0; after <= 1; after++) {
		EvidenceFixture f;
		XD_REQUIRE(xd_start(&f));
		complete_before_io_error = !after; complete_after_io_error = after;
		UT_ASSERT(de_error());
		xd_installed();
		UT_ASSERT_EQ(complete_io_calls, after ? 1 : 2);
	}
}

UT_TEST(test_x_disk_after_shared_C_ERROR_preserves_cover_and_hands_off_notice)
{
	EvidenceFixture f;
	XD_REQUIRE(xd_start(&f));
	complete_publish_error = true;
	UT_ASSERT(de_error());
	xd_installed();
	UT_ASSERT_EQ(complete_io_calls, 1);
}

UT_TEST(test_x_disk_pre_native_ERROR_or_exit_quiets_without_installing_X)
{
	for (int fault = 1; fault <= 2; fault++) {
		EvidenceFixture f;
		XD_REQUIRE(xd_start(&f));
		de_boundary = 2; de_fault = fault;
		if (fault == 1) UT_ASSERT(de_error()); else de_exit();
		UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_N);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation);
		UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR), BM_IO_ERROR);
		UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
		UT_ASSERT_EQ(complete_broadcasts, 0);
		UT_ASSERT_EQ(disk_reads, 1);
		dg_stack_empty();
	}
}

UT_TEST(test_x_disk_bad_checksum_cannot_publish_X_or_weaken_IO_error)
{
	EvidenceFixture f;
	XD_REQUIRE(xd_start(&f));
	disk_bytes.data[400] ^= 1; disk_store();
	UT_ASSERT(xd_run() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(disk_bad_pages, 1);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_N);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR), BM_IO_ERROR);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	dg_stack_empty();
}

UT_TEST(test_x_disk_installed_identity_mismatch_retains_IO_before_fail_closed_cleanup)
{
	for (int fault = 3; fault <= 5; fault++) {
		EvidenceFixture f;
		sigjmp_buf *saved = PG_exception_stack;
		ErrorContextCallback *context = error_context_stack;
		volatile bool caught = false;
		XD_REQUIRE(xd_start(&f));
		xd_boundary = 1; xd_fault = fault;
		dg_panic_expected = true;
		if (sigsetjmp(dg_panic_jump, 0) == 0)
			(void)xd_run();
		else caught = true;
		PG_exception_stack = saved; error_context_stack = context;
		UT_ASSERT(caught); UT_ASSERT_EQ(dg_panics, 1);
		UT_ASSERT_EQ(xd_boundary, 0);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
		UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X);
		UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.writer_activation_token),
			prepared_episode.reservation_token + (fault == 3));
		UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.resource_x_activation_generation),
			prepared_claim.observation.request.assertion_sequence + (fault == 4));
		UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.write_io_generation), fault == 5 ? 2 : 0);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
			RESOURCE_X_DELIVERY_ACQUIRE_READ_ARMED);
		UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR),
			BM_IO_IN_PROGRESS);
		UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
		UT_ASSERT_EQ(quiet_aborts + ua_aborts + complete_io_calls + complete_broadcasts, 0);
		UT_ASSERT_EQ(disk_reads, 1);
		UT_ASSERT_EQ(request_shared.rings[1].count, 1);
		UT_ASSERT_EQ(fake_lwlock_depth, 0);
		/* A real PANIC ends the process. Retire only the controlled test
		 * callback stack before constructing the next independent fixture. */
		disk_exit_count = dg_stack_base;
		complete_idle();
	}
}

int main(void)
{
	UT_PLAN(9);
	UT_RUN(test_x_disk_reads_verified_bytes_once_and_finishes_original_native_owners);
	UT_RUN(test_x_disk_completed_replay_never_reads_or_bumps_native_again);
	UT_RUN(test_x_disk_native_commit_ERROR_or_exit_resumes_same_fenced_T2_T3);
	UT_RUN(test_x_disk_open_native_fences_ERROR_or_exit_still_requires_original_T3);
	UT_RUN(test_x_disk_original_IO_ERROR_before_or_after_termination_finishes_once);
	UT_RUN(test_x_disk_after_shared_C_ERROR_preserves_cover_and_hands_off_notice);
	UT_RUN(test_x_disk_pre_native_ERROR_or_exit_quiets_without_installing_X);
	UT_RUN(test_x_disk_bad_checksum_cannot_publish_X_or_weaken_IO_error);
	UT_RUN(test_x_disk_installed_identity_mismatch_retains_IO_before_fail_closed_cleanup);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
