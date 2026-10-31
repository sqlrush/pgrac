/*-------------------------------------------------------------------------
 *
 * test_cluster_native_disk_stage.c
 *    Exercise unpublished shared staging under the original disk/IO owner.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_native_disk_stage.c
 *
 * NOTES
 *    Original disk/native/PCM/codec/ResourceOwner/checksum bodies are real.
 *    Storage, process and content-lock boundaries are explicit test leaves.
 *    Staging deliberately does not publish native C or complete input IO.
 *
 *-------------------------------------------------------------------------
 */
#include "test_cluster_native_disk_read_fixture.inc"
#include "port/pg_crc32c.h"

char *BufferBlocks;
int NLocBuffer;
Block *LocalBufferBlockPointers;
static ResourceXDiskStageObservation staged;
static void (*stage_on_content)(void), (*stage_after_content)(void);

/* Keep the existing lock implementation; inject only at its boundaries. */
static bool
stage_content_acquire(LWLock *lock, LWLockMode mode)
{
	bool acquired = LWLockAcquire(lock, mode);
	UT_ASSERT(lock == BufferDescriptorGetContentLock(&request_buf));
	UT_ASSERT_EQ(mode, LW_EXCLUSIVE);
	if (stage_on_content != NULL)
		stage_on_content();
	return acquired;
}
static void
stage_content_release(LWLock *lock)
{
	LWLockRelease(lock);
	if (stage_after_content != NULL)
		stage_after_content();
}
#define LockBufHdr stage_header_lock
#define LWLockAcquire stage_content_acquire
#define LWLockRelease stage_content_release
#include "test_cluster_native_disk_stage.inc"
#undef LWLockRelease
#undef LWLockAcquire
#undef LockBufHdr

static bool
stage_disk_start(EvidenceFixture *f, int profile)
{
	if (!disk_start_watermark(f, profile, 90))
		return false;
	BufferBlocks = disk_shared.data;
	memset(&disk_shared, 0x7B, sizeof(disk_shared));
	memset(&staged, 0xA5, sizeof(staged));
	stage_on_content = stage_after_content = NULL;
	return true;
}
static ResourceXApplyResult
stage_disk_run(void)
{
	return cluster_bufmgr_pcm_own_acquire_stage_disk_exact(&request_buf, &request_base,
		&prepared_episode, &request_admission, &execution_claim, &staged);
}
static void
stage_no_observation(void)
{
	ResourceXDiskStageObservation canary;
	memset(&canary, 0xA5, sizeof(canary));
	UT_ASSERT(memcmp(&canary, &staged, sizeof(canary)) == 0);
	disk_preserved();
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & BM_LOCKED) == 0);
}
static void
stage_no_copy(void)
{
	PGIOAlignedBlock canary;
	memset(&canary, 0x7B, sizeof(canary));
	UT_ASSERT(memcmp(&canary, &disk_shared, sizeof(canary)) == 0);
	stage_no_observation();
}
static void
stage_write_turnover(void)
{
	pg_atomic_write_u64(&request_own.write_io_generation, 2);
}
static void
stage_input_error(void)
{
	pg_atomic_write_u32(&request_buf.state,
		pg_atomic_read_u32(&request_buf.state) | BM_IO_ERROR);
}
static void
stage_raise(void)
{
	pg_re_throw();
}
static void
stage_header_error(void)
{
	stage_header_hook = stage_raise;
}

UT_TEST(test_disk_stage_actual_bytes_remain_unpublished_under_original_fence)
{
	EvidenceFixture f;
	ClusterPcmOwnEntry own;
	ClusterPcmResourceXBootstrapRound round;
	uint32 state;
	UT_ASSERT(stage_disk_start(&f, 0));
	own = request_own;
	round = request_entry()->resource_x_bootstrap_round;
	state = pg_atomic_read_u32(&request_buf.state);
	UT_ASSERT_EQ(stage_disk_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(disk_shared.data, disk_bytes.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(staged.page_lsn, 12345);
	UT_ASSERT_EQ(staged.page_scn, 93);
	UT_ASSERT_EQ(staged.page_crc32c, cluster_gcs_block_compute_checksum(disk_shared.data));
	UT_ASSERT_EQ(staged.decision_crc32c, input_grant.common.semantic_crc32c);
	UT_ASSERT_EQ(staged.authority_generation, input_grant.common.authority_generation);
	UT_ASSERT_EQ(staged.executor_sequence, execution_claim.executor_sequence);
	UT_ASSERT_EQ(staged.pending.generation, request_base.generation);
	UT_ASSERT_EQ(staged.pending.flags, PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(staged.pending.semantic_buf_state, BM_TAG_VALID | BM_IO_IN_PROGRESS);
	UT_ASSERT_EQ(staged.write_io_generation, 0);
	request_native_unchanged(&own, state);
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & BM_VALID) == 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
}
UT_TEST(test_disk_stage_accepts_no_caller_supplied_page_or_scalar)
{
	EvidenceFixture f;
	UT_ASSERT(stage_disk_start(&f, 0));
	memset(disk_output.data, 0xE2, BLCKSZ);
	UT_ASSERT_EQ(stage_disk_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(disk_shared.data, disk_output.data, BLCKSZ) != 0);
	UT_ASSERT(memcmp(disk_shared.data, disk_bytes.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(disk_reads, 1);
}
UT_TEST(test_disk_stage_disk_X_remains_unpublished_and_other_sources_cannot_stage)
{
	for (int profile = 1; profile < 8; profile++) {
		EvidenceFixture f;
		UT_ASSERT(stage_disk_start(&f, profile));
		if (profile == 1) {
			/* The same physical staging owner now accepts DISK/X. Staged
			 * bytes are still not native X or a usable completion. */
			UT_ASSERT_EQ(stage_disk_run(), RESOURCE_X_APPLY_APPLIED);
			UT_ASSERT_EQ(disk_reads, 1);
			UT_ASSERT(memcmp(disk_shared.data, disk_bytes.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_N);
			UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_CURRENT);
			UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation);
			UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
			UT_ASSERT(pg_atomic_read_u32(&request_buf.state) & BM_IO_IN_PROGRESS);
			UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & BM_VALID, 0);
		} else {
			UT_ASSERT(stage_disk_run() != RESOURCE_X_APPLY_APPLIED);
			UT_ASSERT_EQ(disk_reads, 0);
			stage_no_copy();
		}
	}
}
UT_TEST(test_disk_stage_stale_executor_never_enters_storage)
{
	EvidenceFixture f;
	UT_ASSERT(stage_disk_start(&f, 0));
	execution_claim.executor_sequence++;
	UT_ASSERT(stage_disk_run() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(disk_enters, 0);
	stage_no_copy();
}
UT_TEST(test_disk_stage_storage_cancel_retains_original_execution_and_IO)
{
	EvidenceFixture f;
	UT_ASSERT(stage_disk_start(&f, 0));
	disk_on_end = disk_cancel;
	UT_ASSERT(stage_disk_run() != RESOURCE_X_APPLY_APPLIED);
	stage_no_copy();
}
UT_TEST(test_disk_stage_real_checksum_failure_never_touches_shared_page)
{
	EvidenceFixture f;
	UT_ASSERT(stage_disk_start(&f, 0));
	disk_bytes.data[300] ^= 1;
	disk_store();
	UT_ASSERT(stage_disk_run() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(disk_traces, 1);
	stage_no_copy();
}
UT_TEST(test_disk_stage_real_shape_failure_preserves_fence_and_IO)
{
	EvidenceFixture f;
	UT_ASSERT(stage_disk_start(&f, 0));
	((PageHeader)disk_bytes.data)->pd_upper = 1;
	((PageHeader)disk_bytes.data)->pd_checksum
		= pg_checksum_page(disk_bytes.data, request_base.tag.blockNum);
	disk_store();
	UT_ASSERT(stage_disk_run() != RESOURCE_X_APPLY_APPLIED);
	stage_no_copy();
}
UT_TEST(test_disk_stage_stale_disk_version_cannot_become_shared_current)
{
	EvidenceFixture f;
	UT_ASSERT(stage_disk_start(&f, 0));
	((PageHeader)disk_bytes.data)->pd_block_scn = 89;
	((PageHeader)disk_bytes.data)->pd_checksum
		= pg_checksum_page(disk_bytes.data, request_base.tag.blockNum);
	disk_store();
	UT_ASSERT(stage_disk_run() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(disk_reads, 2);
	stage_no_copy();
}
UT_TEST(test_disk_stage_content_boundary_native_turnover_prevents_copy)
{
	EvidenceFixture f;
	UT_ASSERT(stage_disk_start(&f, 0));
	stage_on_content = disk_turnover;
	UT_ASSERT(stage_disk_run() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(disk_reads, 1);
	stage_no_copy();
}
UT_TEST(test_disk_stage_even_write_IO_turnover_is_not_the_original_read)
{
	EvidenceFixture f;
	UT_ASSERT(stage_disk_start(&f, 0));
	stage_on_content = stage_write_turnover;
	UT_ASSERT(stage_disk_run() != RESOURCE_X_APPLY_APPLIED);
	stage_no_copy();
}
UT_TEST(test_disk_stage_IO_error_at_content_boundary_is_not_overwritten)
{
	EvidenceFixture f;
	UT_ASSERT(stage_disk_start(&f, 0));
	stage_on_content = stage_input_error;
	UT_ASSERT(stage_disk_run() != RESOURCE_X_APPLY_APPLIED);
	stage_no_copy();
	UT_ASSERT(pg_atomic_read_u32(&request_buf.state) & BM_IO_ERROR);
}
UT_TEST(test_disk_stage_held_content_rejected_before_provider_or_storage)
{
	EvidenceFixture f;
	unsigned rechecks;
	UT_ASSERT(stage_disk_start(&f, 0));
	LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE);
	rechecks = request_rechecks;
	UT_ASSERT(stage_disk_run() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_rechecks, rechecks);
	LWLockRelease(BufferDescriptorGetContentLock(&request_buf));
	UT_ASSERT_EQ(disk_enters, 0);
	stage_no_copy();
}
UT_TEST(test_disk_stage_header_ERROR_releases_only_its_content_lock)
{
	EvidenceFixture f;
	volatile bool caught = false;
	UT_ASSERT(stage_disk_start(&f, 0));
	stage_on_content = stage_header_error;
	PG_TRY();
	{
		(void)stage_disk_run();
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	stage_no_copy();
}
UT_TEST(test_disk_stage_changed_ResourceOwner_cannot_copy_or_export)
{
	EvidenceFixture f;
	UT_ASSERT(stage_disk_start(&f, 0));
	stage_on_content = disk_change_owner;
	UT_ASSERT(stage_disk_run() != RESOURCE_X_APPLY_APPLIED);
	stage_no_copy();
}
UT_TEST(test_disk_stage_cancel_after_copy_exports_no_completion_observation)
{
	EvidenceFixture f;
	UT_ASSERT(stage_disk_start(&f, 0));
	stage_after_content = disk_cancel;
	UT_ASSERT(stage_disk_run() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(disk_shared.data, disk_bytes.data, BLCKSZ) == 0);
	stage_no_observation();
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & BM_VALID) == 0);
}
UT_TEST(test_disk_stage_write_IO_turnover_after_copy_exports_no_observation)
{
	EvidenceFixture f;
	UT_ASSERT(stage_disk_start(&f, 0));
	stage_after_content = stage_write_turnover;
	UT_ASSERT(stage_disk_run() != RESOURCE_X_APPLY_APPLIED);
	stage_no_observation();
}
UT_TEST(test_disk_stage_shared_or_input_alias_output_is_refused_before_IO)
{
	EvidenceFixture f;
	UT_ASSERT(stage_disk_start(&f, 0));
	UT_ASSERT(cluster_bufmgr_pcm_own_acquire_stage_disk_exact(&request_buf, &request_base,
		&prepared_episode, &request_admission, &execution_claim,
		(ResourceXDiskStageObservation *)disk_shared.data) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_bufmgr_pcm_own_acquire_stage_disk_exact(&request_buf, &request_base,
		&prepared_episode, &request_admission, &execution_claim,
		(ResourceXDiskStageObservation *)&request_base) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(disk_reads, 0);
	stage_no_copy();
}
UT_TEST(test_disk_stage_same_executor_never_bumps_generation_or_sends_again)
{
	EvidenceFixture f;
	ResourceXDiskStageObservation first;
	UT_ASSERT(stage_disk_start(&f, 0));
	UT_ASSERT_EQ(stage_disk_run(), RESOURCE_X_APPLY_APPLIED);
	first = staged;
	UT_ASSERT_EQ(stage_disk_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&first, &staged, sizeof(first)) == 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation);
	UT_ASSERT_EQ(request_shared.rings[1].count, 1);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.episode, 1);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		RESOURCE_X_DELIVERY_ACQUIRE_EXECUTING);
}

int
main(void)
{
	UT_PLAN(18);
	UT_RUN(test_disk_stage_actual_bytes_remain_unpublished_under_original_fence);
	UT_RUN(test_disk_stage_accepts_no_caller_supplied_page_or_scalar);
	UT_RUN(test_disk_stage_disk_X_remains_unpublished_and_other_sources_cannot_stage);
	UT_RUN(test_disk_stage_stale_executor_never_enters_storage);
	UT_RUN(test_disk_stage_storage_cancel_retains_original_execution_and_IO);
	UT_RUN(test_disk_stage_real_checksum_failure_never_touches_shared_page);
	UT_RUN(test_disk_stage_real_shape_failure_preserves_fence_and_IO);
	UT_RUN(test_disk_stage_stale_disk_version_cannot_become_shared_current);
	UT_RUN(test_disk_stage_content_boundary_native_turnover_prevents_copy);
	UT_RUN(test_disk_stage_even_write_IO_turnover_is_not_the_original_read);
	UT_RUN(test_disk_stage_IO_error_at_content_boundary_is_not_overwritten);
	UT_RUN(test_disk_stage_held_content_rejected_before_provider_or_storage);
	UT_RUN(test_disk_stage_header_ERROR_releases_only_its_content_lock);
	UT_RUN(test_disk_stage_changed_ResourceOwner_cannot_copy_or_export);
	UT_RUN(test_disk_stage_cancel_after_copy_exports_no_completion_observation);
	UT_RUN(test_disk_stage_write_IO_turnover_after_copy_exports_no_observation);
	UT_RUN(test_disk_stage_shared_or_input_alias_output_is_refused_before_IO);
	UT_RUN(test_disk_stage_same_executor_never_bumps_generation_or_sends_again);
	if (disk_file != NULL)
		fclose(disk_file);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
