/*-------------------------------------------------------------------------
 *
 * test_cluster_native_acquire_stage.c
 *    Exercise original native preparation and exact unpublished withdrawal.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_native_acquire_stage.c
 *
 * NOTES
 *    This pgrac-original test uses actual native/PCM/codec owners. Encoded
 *    master/holder inputs and process/provider leaves are controlled. The
 *    preparation hold grants no physical operation or native completion.
 *
 *-------------------------------------------------------------------------
 */
#include "test_cluster_native_acquire_claim_fixture.inc"
static void (*stage_header_hook)(void);
static uint32
stage_header_lock(BufferDesc *buf)
{
	if (stage_header_hook != NULL) {
		void (*hook)(void) = stage_header_hook;
		stage_header_hook = NULL;
		hook();
	}
	return request_header_lock(buf);
}
#define LockBufHdr stage_header_lock
#define GetPrivateRefCount(buffer) request_private_pin(buffer)
#include "test_cluster_native_acquire_stage.inc"
#undef GetPrivateRefCount
#undef LockBufHdr

static ResourceXDeliveryClaim prepared_claim;
static ResourceXDecodedAcquireIntent prepared_episode;

static bool
stage_start(EvidenceFixture *f, int profile, int native_profile)
{
	if (!claim_start(f, profile, native_profile) || claim_get() != RESOURCE_X_APPLY_APPLIED)
		return false;
	stage_header_hook = NULL;
	prepared_episode = f->decision.body.acquire_decision.episode;
	memset(&prepared_claim, 0xA5, sizeof(prepared_claim));
	request_rechecks = 0;
	return true;
}

static ResourceXApplyResult
stage_enter(void)
{
	return cluster_bufmgr_pcm_own_acquire_stage_begin_exact(
		&request_buf, &request_base, request_token, prepared_episode.path, &request_admission,
		&acquire_claim, &prepared_claim);
}

static ResourceXApplyResult
stage_leave(const ResourceXDeliveryClaim *claim)
{
	return cluster_bufmgr_pcm_own_acquire_stage_release_exact(&request_buf, &request_base,
															  &prepared_episode, claim);
}

UT_TEST(test_stage_all_native_modes_fence_old_release_without_completing)
{
	for (int profile = 0; profile < 10; profile++) {
		EvidenceFixture f;
		ClusterPcmOwnEntry own;
		uint32 state;
		UT_ASSERT(stage_start(&f, profile < 8 ? profile : 2, profile < 8 ? -1 : profile - 7));
		own = request_own;
		state = pg_atomic_read_u32(&request_buf.state);
		UT_ASSERT_EQ(stage_enter(), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(prepared_claim.executor_sequence, acquire_claim.executor_sequence);
		UT_ASSERT_EQ(prepared_claim.purpose, RESOURCE_X_DELIVERY_ACQUIRE_PREPARED);
		UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
		UT_ASSERT_EQ(
			cluster_pcm_lock_resource_x_requester_acquire_claim_release_exact(&acquire_claim),
			RESOURCE_X_APPLY_STALE);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_delivery_claim_end_exact(&prepared_claim),
					 RESOURCE_X_APPLY_STALE);
		pg_atomic_write_u64(&own.delivery_attempt, 1);
		request_native_unchanged(&own, state);
		UT_ASSERT((input_join.flags & RESOURCE_X_REQUESTER_JOIN_READY) == 0);
		UT_ASSERT_EQ(request_shared.rings[1].count, 1);
		UT_ASSERT_EQ(stage_leave(&prepared_claim), RESOURCE_X_APPLY_APPLIED);
		pg_atomic_write_u64(&own.delivery_attempt, 0);
		request_native_unchanged(&own, state);
		UT_ASSERT_EQ(
			cluster_pcm_lock_resource_x_requester_acquire_claim_release_exact(&acquire_claim),
			RESOURCE_X_APPLY_APPLIED);
	}
}

UT_TEST(test_stage_duplicate_old_and_forged_capabilities_do_not_touch_successor)
{
	EvidenceFixture f;
	ResourceXDeliveryClaim stale, canary;
	UT_ASSERT(stage_start(&f, 4, -1));
	UT_ASSERT_EQ(stage_enter(), RESOURCE_X_APPLY_APPLIED);
	stale = prepared_claim;
	canary = prepared_claim;
	UT_ASSERT_EQ(stage_enter(), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT(memcmp(&canary, &prepared_claim, sizeof(canary)) == 0);
	UT_ASSERT_EQ(stage_leave(&stale), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_claim_release_exact(&acquire_claim),
				 RESOURCE_X_APPLY_APPLIED);
	request_rechecks = 0;
	UT_ASSERT_EQ(claim_get(), RESOURCE_X_APPLY_APPLIED);
	request_rechecks = 0;
	UT_ASSERT_EQ(stage_enter(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(prepared_claim.executor_sequence, 2);
	UT_ASSERT_EQ(stage_leave(&stale), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 2);
	UT_ASSERT_EQ(stage_leave(&prepared_claim), RESOURCE_X_APPLY_APPLIED);
}

static void
stage_cancel_after_pcm(void)
{
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
				 RESOURCE_X_DELIVERY_ACQUIRE_PREPARED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_claim_release_exact(&acquire_claim),
				 RESOURCE_X_APPLY_STALE);
	request_proc.cluster_grd_generation++;
}

UT_TEST(test_stage_post_pcm_native_turnover_restores_only_quiet_preparation)
{
	EvidenceFixture f;
	ResourceXDeliveryClaim canary;
	UT_ASSERT(stage_start(&f, 1, 1));
	canary = prepared_claim;
	request_rechecks = UINT_MAX - 1;
	request_hook = stage_cancel_after_pcm;
	UT_ASSERT_EQ(stage_enter(), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&canary, &prepared_claim, sizeof(canary)) == 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
				 RESOURCE_X_DELIVERY_NORMAL);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_claim_release_exact(&acquire_claim),
				 RESOURCE_X_APPLY_APPLIED);
}

static void
stage_error_after_pcm(void)
{
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
				 RESOURCE_X_DELIVERY_ACQUIRE_PREPARED);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & BM_LOCKED) == 0);
	pg_re_throw();
}

UT_TEST(test_stage_error_finally_restores_only_unpublished_exact_owner)
{
	EvidenceFixture f;
	volatile bool caught = false;
	UT_ASSERT(stage_start(&f, 2, 2));
	request_rechecks = UINT_MAX - 1;
	request_hook = stage_error_after_pcm;
	PG_TRY();
	{
		(void)stage_enter();
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
				 RESOURCE_X_DELIVERY_NORMAL);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_claim_release_exact(&acquire_claim),
				 RESOURCE_X_APPLY_APPLIED);
}

static void
stage_unknown_after_pcm(void)
{
	request_base.tag.blockNum++;
	request_buf.tag.blockNum++;
}

UT_TEST(test_stage_unknown_native_after_pcm_keeps_original_stage)
{
	EvidenceFixture f;
	UT_ASSERT(stage_start(&f, 1, 1));
	request_rechecks = UINT_MAX - 1;
	request_hook = stage_unknown_after_pcm;
	UT_ASSERT_EQ(stage_enter(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
				 RESOURCE_X_DELIVERY_ACQUIRE_PREPARED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_claim_release_exact(&acquire_claim),
				 RESOURCE_X_APPLY_STALE);
}

UT_TEST(test_stage_missing_or_changed_materials_never_acquire_native_hold)
{
	EvidenceFixture f;
	UT_ASSERT(stage_start(&f, 4, -1));
	request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor = 1;
	UT_ASSERT_EQ(stage_enter(), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
				 RESOURCE_X_DELIVERY_NORMAL);
}

UT_TEST(test_stage_withdrawal_keeps_unknown_native_write_and_activation_debt)
{
	for (int n = 0; n < 7; n++) {
		EvidenceFixture f;
		ClusterPcmOwnEntry own;
		uint32 state;
		UT_ASSERT(stage_start(&f, 1, 1));
		UT_ASSERT_EQ(stage_enter(), RESOURCE_X_APPLY_APPLIED);
		if (n == 0)
			pg_atomic_write_u64(&request_own.generation, 49);
		if (n == 1)
			pg_atomic_write_u64(&request_own.reservation_token, 50);
		if (n == 2)
			pg_atomic_write_u32(&request_own.flags, PCM_OWN_FLAG_REVOKING);
		if (n == 3)
			pg_atomic_write_u64(&request_own.writer_activation_token, 9);
		if (n == 4)
			pg_atomic_write_u64(&request_own.resource_x_activation_generation, 9);
		if (n == 5)
			pg_atomic_write_u64(&request_own.write_io_generation, 1);
		if (n == 6)
			request_buf.tag.blockNum++;
		own = request_own;
		state = pg_atomic_read_u32(&request_buf.state);
		UT_ASSERT(stage_leave(&prepared_claim) != RESOURCE_X_APPLY_APPLIED);
		request_native_unchanged(&own, state);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
					 RESOURCE_X_DELIVERY_ACQUIRE_PREPARED);
	}
}

UT_TEST(test_stage_unpublished_withdrawal_after_cancel_and_gate_close_is_exact)
{
	EvidenceFixture f;
	UT_ASSERT(stage_start(&f, 1, 1));
	UT_ASSERT_EQ(stage_enter(), RESOURCE_X_APPLY_APPLIED);
	request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor = 1;
	{
		ResourceXGateSnapshot gate;
		UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate),
					 RESOURCE_X_APPLY_APPLIED);
	}
	UT_ASSERT_EQ(stage_leave(&prepared_claim), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_claim_release_exact(&acquire_claim),
				 RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_stage_stale_pcm_before_native_withdraw_preserves_both_owners)
{
	EvidenceFixture f;
	UT_ASSERT(stage_start(&f, 1, 1));
	UT_ASSERT_EQ(stage_enter(), RESOURCE_X_APPLY_APPLIED);
	request_entry()->binding_generation++;
	UT_ASSERT_EQ(stage_leave(&prepared_claim), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_own.flags), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
				 RESOURCE_X_DELIVERY_ACQUIRE_PREPARED);
}

UT_TEST(test_stage_invalid_out_and_foreign_backend_preserve_original_owner)
{
	EvidenceFixture f;
	ClusterPcmOwnEntry own;
	UT_ASSERT(stage_start(&f, 1, 1));
	own = request_own;
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_acquire_stage_begin_exact(
					 &request_buf, &request_base, request_token, prepared_episode.path,
					 &request_admission, &acquire_claim, NULL),
				 RESOURCE_X_APPLY_INVALID);
	request_proc.cluster_grd_generation++;
	UT_ASSERT_EQ(stage_enter(), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
}

UT_TEST(test_stage_wrong_shared_capability_cannot_withdraw_native_hold)
{
	EvidenceFixture f;
	ResourceXDeliveryClaim wrong;
	ClusterPcmOwnEntry own;
	uint32 state;
	UT_ASSERT(stage_start(&f, 1, 1));
	UT_ASSERT_EQ(stage_enter(), RESOURCE_X_APPLY_APPLIED);
	wrong = prepared_claim;
	wrong.observation.r4_record_generation++;
	own = request_own;
	state = pg_atomic_read_u32(&request_buf.state);
	UT_ASSERT_EQ(stage_leave(&wrong), RESOURCE_X_APPLY_STALE);
	request_native_unchanged(&own, state);
}

static void
stage_binding_turnover(void)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	request_entry()->binding_generation++;
}

UT_TEST(test_stage_pcm_turnover_after_check_retains_stage_after_quiet_withdrawal)
{
	EvidenceFixture f;
	UT_ASSERT(stage_start(&f, 1, 1));
	UT_ASSERT_EQ(stage_enter(), RESOURCE_X_APPLY_APPLIED);
	stage_header_hook = stage_binding_turnover;
	UT_ASSERT_EQ(stage_leave(&prepared_claim), RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_own.flags), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
				 RESOURCE_X_DELIVERY_ACQUIRE_PREPARED);
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(test_stage_all_native_modes_fence_old_release_without_completing);
	UT_RUN(test_stage_duplicate_old_and_forged_capabilities_do_not_touch_successor);
	UT_RUN(test_stage_post_pcm_native_turnover_restores_only_quiet_preparation);
	UT_RUN(test_stage_error_finally_restores_only_unpublished_exact_owner);
	UT_RUN(test_stage_unknown_native_after_pcm_keeps_original_stage);
	UT_RUN(test_stage_missing_or_changed_materials_never_acquire_native_hold);
	UT_RUN(test_stage_withdrawal_keeps_unknown_native_write_and_activation_debt);
	UT_RUN(test_stage_unpublished_withdrawal_after_cancel_and_gate_close_is_exact);
	UT_RUN(test_stage_stale_pcm_before_native_withdraw_preserves_both_owners);
	UT_RUN(test_stage_invalid_out_and_foreign_backend_preserve_original_owner);
	UT_RUN(test_stage_wrong_shared_capability_cannot_withdraw_native_hold);
	UT_RUN(test_stage_pcm_turnover_after_check_retains_stage_after_quiet_withdrawal);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
