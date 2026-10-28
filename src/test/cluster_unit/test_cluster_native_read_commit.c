/*-------------------------------------------------------------------------
 *
 * test_cluster_native_read_commit.c
 *    Exercise the exact read-executor transition of the native owner tuple.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_native_read_commit.c
 *
 * NOTES
 *    Links the production ownership object. Only unused allocation/registry
 *    and assertion dependencies are leaves; no native transition is mocked.
 *    This is not shared-page installation or native IO completion.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_pcm_own.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

int NBuffers = 2;
static ClusterPcmOwnEntry native[3];
static uint64 committed;

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *found pg_attribute_unused())
{
	abort();
}
Size
mul_size(Size a, Size b)
{
	if (b != 0 && a > SIZE_MAX / b)
		abort();
	return a * b;
}
void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{
	abort();
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "unexpected assertion: %s at %s:%d\n", condition, file, line);
	abort();
}

static void
read_reset(void)
{
	memset(native, 0xA5, sizeof(native));
	ClusterPcmOwnArray = native;
	pg_atomic_init_u64(&native[0].generation, 17);
	pg_atomic_init_u64(&native[0].reservation_token, 31);
	pg_atomic_init_u32(&native[0].flags, PCM_OWN_FLAG_GRANT_PENDING);
	pg_atomic_init_u64(&native[0].writer_activation_token, 0);
	pg_atomic_init_u64(&native[0].resource_x_activation_generation, 0);
	pg_atomic_init_u64(&native[0].delivery_attempt, 43);
	pg_atomic_init_u64(&native[0].write_io_generation, 56);
	native[0].service_waiters = 5;
	native[0].service_event_generation = 78;
	committed = UINT64_MAX;
}

static ClusterPcmOwnResult
read_commit(void)
{
	return cluster_pcm_own_delivery_read_commit_exact(0, 17, 31, 43, 56, &committed);
}

static bool
read_refused_unchanged(ClusterPcmOwnResult expected)
{
	ClusterPcmOwnEntry before[3];
	memcpy(before, native, sizeof(before));
	return read_commit() == expected && committed == 0
		&& memcmp(before, native, sizeof(before)) == 0;
}

UT_TEST(test_read_commit_consumes_GP_once_and_preserves_exact_hold)
{
	ClusterPcmOwnEntry expected[3];
	read_reset();
	memcpy(expected, native, sizeof(expected));
	pg_atomic_write_u64(&expected[0].generation, 18);
	pg_atomic_write_u32(&expected[0].flags, 0);
	UT_ASSERT_EQ(read_commit(), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed, 18);
	UT_ASSERT(memcmp(expected, native, sizeof(expected)) == 0);
}
UT_TEST(test_read_generic_commit_still_refuses_adopted_delivery)
{
	ClusterPcmOwnEntry before[3];
	read_reset();
	memcpy(before, native, sizeof(before));
	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 17, 31, &committed),
				 CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(committed, 0);
	UT_ASSERT(memcmp(before, native, sizeof(before)) == 0);
}
UT_TEST(test_read_duplicate_old_base_never_bumps_generation_twice)
{
	read_reset();
	UT_ASSERT_EQ(read_commit(), CLUSTER_PCM_OWN_OK);
	UT_ASSERT(read_refused_unchanged(CLUSTER_PCM_OWN_STALE));
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 18);
}
UT_TEST(test_read_changed_base_generation_cannot_commit_successor)
{
	read_reset();
	pg_atomic_write_u64(&native[0].generation, 18);
	UT_ASSERT(read_refused_unchanged(CLUSTER_PCM_OWN_STALE));
}
UT_TEST(test_read_changed_token_cannot_commit_successor)
{
	read_reset();
	pg_atomic_write_u64(&native[0].reservation_token, 32);
	UT_ASSERT(read_refused_unchanged(CLUSTER_PCM_OWN_STALE));
}
UT_TEST(test_read_changed_executor_cannot_consume_its_hold)
{
	read_reset();
	pg_atomic_write_u64(&native[0].delivery_attempt, 44);
	UT_ASSERT(read_refused_unchanged(CLUSTER_PCM_OWN_STALE));
}
UT_TEST(test_read_absent_delivery_hold_is_not_an_execution_capability)
{
	read_reset();
	pg_atomic_write_u64(&native[0].delivery_attempt, 0);
	UT_ASSERT(read_refused_unchanged(CLUSTER_PCM_OWN_STALE));
}
UT_TEST(test_read_never_consumes_empty_revoke_or_malformed_flags)
{
	const uint32 flags[] = { 0, PCM_OWN_FLAG_REVOKING,
		PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING, UINT32_MAX };
	const ClusterPcmOwnResult results[] = { CLUSTER_PCM_OWN_STALE, CLUSTER_PCM_OWN_BUSY,
		CLUSTER_PCM_OWN_CORRUPT, CLUSTER_PCM_OWN_CORRUPT };
	for (unsigned i = 0; i < lengthof(flags); i++) {
		read_reset();
		pg_atomic_write_u32(&native[0].flags, flags[i]);
		UT_ASSERT(read_refused_unchanged(results[i]));
	}
}
UT_TEST(test_read_live_GP_with_zero_token_remains_corrupt)
{
	read_reset();
	pg_atomic_write_u64(&native[0].reservation_token, 0);
	UT_ASSERT(read_refused_unchanged(CLUSTER_PCM_OWN_CORRUPT));
}
UT_TEST(test_read_never_clears_a_writer_or_ResourceX_activation)
{
	for (unsigned i = 0; i < 2; i++) {
		read_reset();
		pg_atomic_write_u64(i == 0 ? &native[0].writer_activation_token
								: &native[0].resource_x_activation_generation, 31);
		UT_ASSERT(read_refused_unchanged(CLUSTER_PCM_OWN_CORRUPT));
	}
}
UT_TEST(test_read_current_output_IO_must_be_quiet)
{
	read_reset();
	pg_atomic_write_u64(&native[0].write_io_generation, 57);
	UT_ASSERT(read_refused_unchanged(CLUSTER_PCM_OWN_BUSY));
}
UT_TEST(test_read_completed_output_IO_still_invalidates_old_proof)
{
	read_reset();
	pg_atomic_write_u64(&native[0].write_io_generation, 58);
	UT_ASSERT(read_refused_unchanged(CLUSTER_PCM_OWN_STALE));
}
UT_TEST(test_read_invalid_parameters_never_mutate_a_native_tuple)
{
	const uint64 tokens[] = { 0, 31, 31, 31 };
	const uint64 attempts[] = { 43, 0, UINT64_MAX, 43 };
	const uint64 io[] = { 56, 56, 56, 57 };
	for (unsigned i = 0; i < lengthof(tokens); i++) {
		ClusterPcmOwnEntry before[3];
		read_reset();
		memcpy(before, native, sizeof(before));
		UT_ASSERT_EQ(cluster_pcm_own_delivery_read_commit_exact(
			0, 17, tokens[i], attempts[i], io[i], &committed), CLUSTER_PCM_OWN_INVALID);
		UT_ASSERT_EQ(committed, 0);
		UT_ASSERT(memcmp(before, native, sizeof(before)) == 0);
	}
}
UT_TEST(test_read_null_output_cannot_consume_the_original_reservation)
{
	ClusterPcmOwnEntry before[3];
	read_reset();
	memcpy(before, native, sizeof(before));
	UT_ASSERT_EQ(cluster_pcm_own_delivery_read_commit_exact(0, 17, 31, 43, 56, NULL),
				 CLUSTER_PCM_OWN_INVALID);
	UT_ASSERT(memcmp(before, native, sizeof(before)) == 0);
}
UT_TEST(test_read_array_bounds_and_uninitialized_owner_are_refused)
{
	const int ids[] = { -1, 2, 3 };
	ClusterPcmOwnEntry before[3];
	read_reset();
	memcpy(before, native, sizeof(before));
	for (unsigned i = 0; i < lengthof(ids); i++) {
		UT_ASSERT_EQ(cluster_pcm_own_delivery_read_commit_exact(ids[i], 17, 31, 43, 56,
			&committed), CLUSTER_PCM_OWN_NOT_READY);
		UT_ASSERT_EQ(committed, 0);
	}
	ClusterPcmOwnArray = NULL;
	UT_ASSERT_EQ(read_commit(), CLUSTER_PCM_OWN_NOT_READY);
	UT_ASSERT_EQ(committed, 0);
	UT_ASSERT(memcmp(before, native, sizeof(before)) == 0);
	ClusterPcmOwnArray = native;
}
UT_TEST(test_read_exhaustion_is_not_wraparound_or_release)
{
	ClusterPcmOwnEntry before[3];
	read_reset();
	memcpy(before, native, sizeof(before));
	UT_ASSERT_EQ(cluster_pcm_own_delivery_read_commit_exact(0, UINT64_MAX, 31, 43, 56,
		&committed), CLUSTER_PCM_OWN_EXHAUSTED);
	UT_ASSERT_EQ(committed, 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_read_commit_exact(0, 17, UINT64_MAX, 43, 56,
		&committed), CLUSTER_PCM_OWN_EXHAUSTED);
	UT_ASSERT_EQ(committed, 0);
	UT_ASSERT(memcmp(before, native, sizeof(before)) == 0);
}
UT_TEST(test_read_last_generation_is_committed_once_then_exhausted)
{
	read_reset();
	pg_atomic_write_u64(&native[0].generation, UINT64_MAX - 1);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_read_commit_exact(0, UINT64_MAX - 1, 31, 43,
		56, &committed), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed, UINT64_MAX);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 43);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_read_commit_exact(0, UINT64_MAX, 31, 43,
		56, &committed), CLUSTER_PCM_OWN_EXHAUSTED);
	UT_ASSERT_EQ(committed, 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), UINT64_MAX);
}
UT_TEST(test_read_original_hold_blocks_successor_before_explicit_handoff)
{
	uint64 token;
	read_reset();
	UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_release_exact(0, 17, 43), CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(read_commit(), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 18, PCM_OWN_FLAG_GRANT_PENDING,
		&token), CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_release_exact(0, 17, 43), CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 43);
}
UT_TEST(test_read_old_executor_cannot_commit_after_exact_successor_adoption)
{
	uint64 token;
	read_reset();
	UT_ASSERT_EQ(read_commit(), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_release_exact(0, 18, 43), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 18, PCM_OWN_FLAG_GRANT_PENDING,
		&token), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(token, 32);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_adopt_grant_exact(0, 18, 32, 44), CLUSTER_PCM_OWN_OK);
	UT_ASSERT(read_refused_unchanged(CLUSTER_PCM_OWN_STALE));
	UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_release_exact(0, 18, 43), CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 44);
}
UT_TEST(test_read_output_IO_and_service_generations_are_not_completion_receipts)
{
	read_reset();
	native[0].service_waiters = UINT32_MAX;
	native[0].service_event_generation = UINT64_MAX;
	UT_ASSERT_EQ(read_commit(), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(pg_atomic_read_u64(&native[0].write_io_generation), 56);
	UT_ASSERT_EQ(native[0].service_waiters, UINT32_MAX);
	UT_ASSERT_EQ(native[0].service_event_generation, UINT64_MAX);
	UT_ASSERT_EQ(cluster_pcm_own_writer_activation_token_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_generation_get(0), 0);
}

int
main(void)
{
	UT_PLAN(20);
	UT_RUN(test_read_commit_consumes_GP_once_and_preserves_exact_hold);
	UT_RUN(test_read_generic_commit_still_refuses_adopted_delivery);
	UT_RUN(test_read_duplicate_old_base_never_bumps_generation_twice);
	UT_RUN(test_read_changed_base_generation_cannot_commit_successor);
	UT_RUN(test_read_changed_token_cannot_commit_successor);
	UT_RUN(test_read_changed_executor_cannot_consume_its_hold);
	UT_RUN(test_read_absent_delivery_hold_is_not_an_execution_capability);
	UT_RUN(test_read_never_consumes_empty_revoke_or_malformed_flags);
	UT_RUN(test_read_live_GP_with_zero_token_remains_corrupt);
	UT_RUN(test_read_never_clears_a_writer_or_ResourceX_activation);
	UT_RUN(test_read_current_output_IO_must_be_quiet);
	UT_RUN(test_read_completed_output_IO_still_invalidates_old_proof);
	UT_RUN(test_read_invalid_parameters_never_mutate_a_native_tuple);
	UT_RUN(test_read_null_output_cannot_consume_the_original_reservation);
	UT_RUN(test_read_array_bounds_and_uninitialized_owner_are_refused);
	UT_RUN(test_read_exhaustion_is_not_wraparound_or_release);
	UT_RUN(test_read_last_generation_is_committed_once_then_exhausted);
	UT_RUN(test_read_original_hold_blocks_successor_before_explicit_handoff);
	UT_RUN(test_read_old_executor_cannot_commit_after_exact_successor_adoption);
	UT_RUN(test_read_output_IO_and_service_generations_are_not_completion_receipts);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
