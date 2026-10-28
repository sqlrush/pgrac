/*-------------------------------------------------------------------------
 * test_cluster_native_disk_complete.c
 *    Exercise exact first-fill disk S completion and interrupted ownership.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_native_disk_complete.c
 * NOTES
 *    Keep actual native/PCM/codec/staging/checksum/ResourceOwner and original
 *    TerminateBufferIO bodies. Only explicit event/counter leaves are wrapped.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_native_disk_stage_fixture.inc"
static ConditionVariableMinimallyPadded complete_iocvs[1];
ConditionVariableMinimallyPadded *BufferIOCVArray = complete_iocvs;
static unsigned complete_commits, complete_io_calls, complete_broadcasts;
static bool complete_before_io_error, complete_after_io_error, complete_publish_error;
static void (*complete_on_content)(void), (*complete_on_broadcast)(void);
static void
complete_broadcast(ConditionVariable *cv)
{
	complete_broadcasts++;
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_SCUR);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & BM_IO_IN_PROGRESS, 0);
	UT_ASSERT(pg_atomic_read_u32(&request_buf.state) & BM_VALID);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		RESOURCE_X_DELIVERY_ACQUIRE_READ_ARMED);
	ConditionVariableBroadcast(cv);
	if (complete_on_broadcast != NULL)
		complete_on_broadcast();
}
#define LockBufHdr stage_header_lock
#define ConditionVariableBroadcast complete_broadcast
#define TerminateBufferIO complete_actual_terminate
#include "test_cluster_native_disk_terminate.inc"
#undef TerminateBufferIO
#undef ConditionVariableBroadcast
#undef LockBufHdr
static void
TerminateBufferIO(BufferDesc *buf, bool clear_dirty, uint32 set_bits)
{
	complete_io_calls++;
	UT_ASSERT_EQ(buf->pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&buf->state) & BM_VALID, 0);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
	if (complete_before_io_error)
		pg_re_throw();
	complete_actual_terminate(buf, clear_dirty, set_bits);
	if (complete_after_io_error)
		pg_re_throw();
}
#ifndef DISK_COMPLETE_RED
static ClusterPcmOwnResult
complete_read_commit(int id, uint64 generation, uint64 token, uint64 attempt,
	uint64 write, uint64 *out)
{
	complete_commits++;
	return cluster_pcm_own_delivery_read_commit_exact(id, generation, token, attempt, write, out);
}
static ResourceXApplyResult
complete_publish(const ResourceXDeliveryClaim *execution,
	const ResourceXDecodedAcquireIntent *episode, const ResourceXAcquireReadReceipt *receipt,
	const ClusterPcmOwnSnapshot *terminal)
{
	ResourceXApplyResult r = cluster_pcm_lock_resource_x_requester_acquire_read_complete_exact(
		execution, episode, receipt, terminal);
	if (r == RESOURCE_X_APPLY_APPLIED && complete_publish_error)
		stage_header_hook = stage_raise;
	return r;
}
#endif
static bool
complete_content_acquire(LWLock *lock, LWLockMode mode)
{
	bool ok = LWLockAcquire(lock, mode);
	UT_ASSERT(lock == BufferDescriptorGetContentLock(&request_buf));
	UT_ASSERT_EQ(mode, LW_EXCLUSIVE);
	if (complete_on_content != NULL)
		complete_on_content();
	return ok;
}
#define LockBufHdr stage_header_lock
#define LWLockAcquire complete_content_acquire
#define cluster_pcm_own_delivery_read_commit_exact complete_read_commit
#define cluster_pcm_lock_resource_x_requester_acquire_read_complete_exact complete_publish
#include "test_cluster_native_disk_complete.inc"
#undef cluster_pcm_lock_resource_x_requester_acquire_read_complete_exact
#undef cluster_pcm_own_delivery_read_commit_exact
#undef LWLockAcquire
#undef LockBufHdr
static bool
complete_start(EvidenceFixture *f)
{
	if (!stage_disk_start(f, 0))
		return false;
	complete_commits = complete_io_calls = complete_broadcasts = 0;
	complete_before_io_error = complete_after_io_error = complete_publish_error = false;
	complete_on_content = complete_on_broadcast = NULL;
	return true;
}
static ResourceXApplyResult
complete_run(void)
{
	return cluster_bufmgr_pcm_own_acquire_complete_disk_exact(&request_buf, &request_base,
		&prepared_episode, &request_admission, &execution_claim);
}
static void
complete_idle(void)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & BM_LOCKED, 0);
}
static void
complete_finished(void)
{
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_SCUR);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & CLUSTER_PCM_OWN_SEMANTIC_BUF_STATE_MASK,
		BM_TAG_VALID | BM_VALID);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(complete_commits, 1);
	UT_ASSERT_EQ(complete_io_calls, 1);
	UT_ASSERT_EQ(complete_broadcasts, 1);
	UT_ASSERT_EQ(disk_reads, 1);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		RESOURCE_X_DELIVERY_ACQUIRE_READ_COMPLETE);
	complete_idle();
}
static void
complete_armed(void)
{
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		RESOURCE_X_DELIVERY_ACQUIRE_READ_ARMED);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
	complete_idle();
}
static void
complete_tamper(void)
{
	disk_shared.data[256] ^= 1;
	stage_after_content = NULL;
}
static bool
complete_catches(void)
{
	volatile bool caught = false;
	PG_TRY();
	{
		(void)complete_run();
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	return caught;
}
UT_TEST(test_complete_actual_C_then_original_IO_then_receipt_before_hold_release)
{
	EvidenceFixture f;
	UT_ASSERT(complete_start(&f));
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_APPLIED);
	complete_finished();
	UT_ASSERT(memcmp(disk_shared.data, disk_bytes.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.episode, 1);
}
UT_TEST(test_complete_exact_replay_has_no_read_bump_C_or_IO_broadcast)
{
	EvidenceFixture f;
	ClusterPcmResourceXBootstrapRound round;
	UT_ASSERT(complete_start(&f));
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_APPLIED);
	round = request_entry()->resource_x_bootstrap_round;
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_DUPLICATE);
	complete_finished();
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
}
UT_TEST(test_complete_receipt_binds_real_A_verified_bytes_and_native_IO)
{
	EvidenceFixture f;
	const ResourceXAcquireReadReceipt *r;
	UT_ASSERT(complete_start(&f));
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_APPLIED);
	r = &request_entry()->resource_x_bootstrap_round.acquire_read_receipt;
	UT_ASSERT_EQ(sizeof(*r), 40);
	UT_ASSERT_EQ(r->authority_generation, input_grant.common.authority_generation);
	UT_ASSERT_EQ(r->decision_crc32c, input_grant.common.semantic_crc32c);
	UT_ASSERT_EQ(r->page_crc32c, cluster_gcs_block_compute_checksum(disk_bytes.data));
	UT_ASSERT_EQ(r->page_lsn, 12345);
	UT_ASSERT_EQ(r->page_scn, 93);
	UT_ASSERT_EQ(r->write_io_generation, 0);
}
UT_TEST(test_complete_other_sources_and_known_X_never_enter_S_completion)
{
	for (int profile = 1; profile < 8; profile++) {
		EvidenceFixture f;
		UT_ASSERT(complete_start(&f));
		UT_ASSERT(stage_disk_start(&f, profile));
		UT_ASSERT(complete_run() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(disk_reads, 0);
		UT_ASSERT_EQ(complete_commits, 0);
		UT_ASSERT_EQ(complete_io_calls, 0);
	}
}
UT_TEST(test_complete_stale_execution_cannot_read_or_publish)
{
	EvidenceFixture f;
	UT_ASSERT(complete_start(&f));
	execution_claim.executor_sequence++;
	UT_ASSERT(complete_run() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(complete_io_calls, 0);
}
UT_TEST(test_complete_checksum_failure_preserves_original_unpublished_IO)
{
	EvidenceFixture f;
	UT_ASSERT(complete_start(&f));
	disk_bytes.data[256] ^= 1;
	rewind(disk_file);
	UT_ASSERT_EQ(fwrite(disk_bytes.data, 1, BLCKSZ, disk_file), BLCKSZ);
	fflush(disk_file);
	UT_ASSERT(complete_run() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(disk_bad_pages, 1);
	UT_ASSERT_EQ(complete_commits, 0);
	disk_preserved();
}
UT_TEST(test_complete_changed_shared_bytes_are_not_verified_I)
{
	EvidenceFixture f;
	UT_ASSERT(complete_start(&f));
	stage_after_content = complete_tamper;
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	complete_armed();
	UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT_EQ(complete_io_calls, 0);
}
UT_TEST(test_complete_content_boundary_generation_turnover_cannot_commit)
{
	EvidenceFixture f;
	UT_ASSERT(complete_start(&f));
	complete_on_content = disk_turnover;
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	complete_armed();
	UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT_EQ(complete_io_calls, 0);
}
UT_TEST(test_complete_content_boundary_even_write_generation_must_match)
{
	EvidenceFixture f;
	UT_ASSERT(complete_start(&f));
	complete_on_content = stage_write_turnover;
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	complete_armed();
	UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT_EQ(complete_io_calls, 0);
}
UT_TEST(test_complete_header_ERROR_retains_armed_owner_and_input_IO)
{
	EvidenceFixture f;
	UT_ASSERT(complete_start(&f));
	complete_on_content = stage_header_error;
	UT_ASSERT(complete_catches());
	complete_armed();
	complete_on_content = NULL;
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(disk_reads, 1);
	UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
}
UT_TEST(test_complete_ERROR_after_native_C_cannot_reenter_or_abort_as_unpublished)
{
	EvidenceFixture f;
	UT_ASSERT(complete_start(&f));
	complete_before_io_error = true;
	UT_ASSERT(complete_catches());
	complete_armed();
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(disk_reads, 1);
	UT_ASSERT_EQ(complete_commits, 1);
	UT_ASSERT_EQ(complete_io_calls, 1);
	UT_ASSERT_EQ(complete_broadcasts, 0);
}
UT_TEST(test_complete_ERROR_after_IO_cannot_repeat_C_or_IO)
{
	EvidenceFixture f;
	UT_ASSERT(complete_start(&f));
	complete_after_io_error = true;
	UT_ASSERT(complete_catches());
	complete_armed();
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT(pg_atomic_read_u32(&request_buf.state) & BM_VALID);
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(disk_reads, 1);
	UT_ASSERT_EQ(complete_commits, 1);
	UT_ASSERT_EQ(complete_io_calls, 1);
	UT_ASSERT_EQ(complete_broadcasts, 1);
}
UT_TEST(test_complete_cancel_after_IO_preserves_installed_resident_and_receipt)
{
	EvidenceFixture f;
	UT_ASSERT(complete_start(&f));
	complete_on_broadcast = disk_cancel;
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	complete_armed();
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT(pg_atomic_read_u32(&request_buf.state) & BM_VALID);
	UT_ASSERT_EQ(complete_io_calls, 1);
}
UT_TEST(test_complete_post_shared_C_ERROR_replay_only_releases_original_hold)
{
	EvidenceFixture f;
	UT_ASSERT(complete_start(&f));
	complete_publish_error = true;
	UT_ASSERT(complete_catches());
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		RESOURCE_X_DELIVERY_ACQUIRE_READ_COMPLETE);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
	complete_idle();
	complete_publish_error = false;
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_DUPLICATE);
	complete_finished();
}
UT_TEST(test_complete_replay_does_not_release_successor_native_hold)
{
	EvidenceFixture f;
	UT_ASSERT(complete_start(&f));
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_APPLIED);
	pg_atomic_write_u64(&request_own.generation, request_base.generation + 2);
	UT_ASSERT(complete_run() != RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 2);
	UT_ASSERT_EQ(complete_io_calls, 1);
}
UT_TEST(test_complete_held_content_rejected_without_entry_provider_or_IO)
{
	EvidenceFixture f;
	UT_ASSERT(complete_start(&f));
	LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE);
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_BAD_STATE);
	LWLockRelease(BufferDescriptorGetContentLock(&request_buf));
	UT_ASSERT_EQ(disk_enters, 0);
	UT_ASSERT_EQ(complete_commits, 0);
}
UT_TEST(test_complete_old_prepared_revert_cannot_consume_armed_or_completed_owner)
{
	EvidenceFixture f;
	UT_ASSERT(complete_start(&f));
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_requester_acquire_stage_revert_exact(
		&prepared_claim, &prepared_episode, &request_base) != RESOURCE_X_APPLY_APPLIED);
	complete_finished();
}
UT_TEST(test_complete_cancel_before_physical_C_keeps_original_GP_and_IO)
{
	EvidenceFixture f;
	UT_ASSERT(complete_start(&f));
	disk_on_end = disk_cancel;
	UT_ASSERT(complete_run() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT_EQ(complete_io_calls, 0);
	disk_preserved();
}
int
main(void)
{
	UT_PLAN(18);
	UT_RUN(test_complete_actual_C_then_original_IO_then_receipt_before_hold_release);
	UT_RUN(test_complete_exact_replay_has_no_read_bump_C_or_IO_broadcast);
	UT_RUN(test_complete_receipt_binds_real_A_verified_bytes_and_native_IO);
	UT_RUN(test_complete_other_sources_and_known_X_never_enter_S_completion);
	UT_RUN(test_complete_stale_execution_cannot_read_or_publish);
	UT_RUN(test_complete_checksum_failure_preserves_original_unpublished_IO);
	UT_RUN(test_complete_changed_shared_bytes_are_not_verified_I);
	UT_RUN(test_complete_content_boundary_generation_turnover_cannot_commit);
	UT_RUN(test_complete_content_boundary_even_write_generation_must_match);
	UT_RUN(test_complete_header_ERROR_retains_armed_owner_and_input_IO);
	UT_RUN(test_complete_ERROR_after_native_C_cannot_reenter_or_abort_as_unpublished);
	UT_RUN(test_complete_ERROR_after_IO_cannot_repeat_C_or_IO);
	UT_RUN(test_complete_cancel_after_IO_preserves_installed_resident_and_receipt);
	UT_RUN(test_complete_post_shared_C_ERROR_replay_only_releases_original_hold);
	UT_RUN(test_complete_replay_does_not_release_successor_native_hold);
	UT_RUN(test_complete_held_content_rejected_without_entry_provider_or_IO);
	UT_RUN(test_complete_old_prepared_revert_cannot_consume_armed_or_completed_owner);
	UT_RUN(test_complete_cancel_before_physical_C_keeps_original_GP_and_IO);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
