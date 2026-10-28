/*-------------------------------------------------------------------------
 * test_cluster_ctrc_local_empty.c
 *    Original native directory, with conditional lock scheduling controlled.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
int ctrc_local_empty_original_main(void);
#define main ctrc_local_empty_original_main
#include "test_cluster_ctrc_cleaner.c"
#undef main

static LWLock *empty_locks[CLUSTER_CTRC_CLEANER_WORKERS + 2];
static unsigned empty_held, empty_attempts, empty_releases, empty_collision, empty_error;
static bool empty_caller_lock;
static unsigned empty_config_reads;

static void
empty_config_read(void)
{
	UT_ASSERT_EQ(empty_held, 0);
	empty_config_reads++;
}

static LWLock *
empty_lock_at(unsigned index)
{
	if (index == 0) return &CtrcShared->origin_lock;
	if (index <= CLUSTER_CTRC_CLEANER_WORKERS)
		return &CtrcShared->participant_locks[index - 1].lock;
	UT_ASSERT_EQ(index, CLUSTER_CTRC_CLEANER_WORKERS + 1);
	return &CtrcShared->receipt_lock;
}

bool
LWLockConditionalAcquire(LWLock *lock, LWLockMode mode)
{
	unsigned call = ++empty_attempts;
	UT_ASSERT_EQ(mode, LW_SHARED);
	UT_ASSERT(!empty_caller_lock && held_count == 0);
	UT_ASSERT(empty_held < lengthof(empty_locks));
	UT_ASSERT(lock == empty_lock_at(empty_held));
	if (call == empty_error) pg_re_throw();
	if (call == empty_collision) return false;
	empty_locks[empty_held++] = lock;
	return true;
}

void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *context)
{
	if (empty_caller_lock) callback((LWLock *)&empty_caller_lock, LW_EXCLUSIVE, context);
	for (unsigned i = 0; i < held_count; i++)
		callback((LWLock *)held_locks[i], LW_EXCLUSIVE, context);
	for (unsigned i = 0; i < empty_held; i++) callback(empty_locks[i], LW_SHARED, context);
}

static bool
empty_release(LWLock *lock, LWLockMode mode pg_attribute_unused(), bool acquire)
{
	/* The new path must never fall back to blocking LWLockAcquire. */
	UT_ASSERT(!acquire);
	UT_ASSERT(empty_held > 0 && empty_locks[empty_held - 1] == lock);
	empty_locks[--empty_held] = NULL;
	empty_releases++;
	return true;
}

static void
empty_reset(void)
{
	UT_ASSERT_EQ(empty_held, 0);
	reset_fixture();
	empty_attempts = empty_releases = empty_collision = empty_error = 0;
	empty_caller_lock = false;
	empty_config_reads = 0;
	external_lock_hook = empty_release;
}

UT_TEST(test_local_empty_checks_real_fresh_layout_without_mutation)
{
	uint8 *before;
	empty_reset(); config_read_hook = empty_config_read;
	before = malloc(CtrcShared->total_bytes);
	memcpy(before, CtrcShared, CtrcShared->total_bytes);
	UT_ASSERT_EQ(cluster_ctrc_recovery_local_empty_v1(), CLUSTER_CTRC_LOCAL_EMPTY);
	UT_ASSERT_EQ(empty_attempts, lengthof(empty_locks));
	UT_ASSERT_EQ(empty_config_reads, 1);
	UT_ASSERT_EQ(empty_releases, empty_attempts); UT_ASSERT_EQ(empty_held, 0);
	UT_ASSERT_EQ(sleepable_acquisitions, 0); UT_ASSERT_EQ(allocation_attempts, 0);
	UT_ASSERT(memcmp(before, CtrcShared, CtrcShared->total_bytes) == 0);
	free(before);
}

UT_TEST(test_local_empty_refuses_missing_or_changed_original_layout)
{
	ClusterCtrcSharedHeader *saved;
	empty_reset(); saved = CtrcShared; CtrcShared = NULL;
	UT_ASSERT_EQ(cluster_ctrc_recovery_local_empty_v1(), CLUSTER_CTRC_LOCAL_REFUSED);
	CtrcShared = saved;
	CtrcShared->participant_offset++;
	UT_ASSERT_EQ(cluster_ctrc_recovery_local_empty_v1(), CLUSTER_CTRC_LOCAL_REFUSED);
	CtrcShared->participant_offset--;
	CtrcShared->global_grant_generation = 0;
	UT_ASSERT_EQ(cluster_ctrc_recovery_local_empty_v1(), CLUSTER_CTRC_LOCAL_REFUSED);
	CtrcShared->global_grant_generation = 1;
	UT_ASSERT_EQ(empty_attempts, 0); UT_ASSERT_EQ(empty_held, 0);
}

UT_TEST(test_local_empty_keeps_unknown_bytes_in_every_original_array)
{
	uint64 offsets[9];
	empty_reset();
	offsets[0] = CtrcShared->origin_offset;
	offsets[1] = CtrcShared->participant_offset;
	offsets[2] = CtrcShared->receipt_offset;
	offsets[3] = CtrcShared->receipt_probe_offset;
	offsets[4] = CtrcShared->receipt_link_offset;
	offsets[5] = CtrcShared->participant_receipt_head_offset;
	offsets[6] = CtrcShared->participant_ack_offset;
	offsets[7] = CtrcShared->origin_ack_offset;
	offsets[8] = CtrcShared->total_bytes;
	for (unsigned i = 0; i < 8; i++) {
		for (unsigned end = 0; end < 2; end++) {
			uint8 *byte = (uint8 *)CtrcShared + (end ? offsets[i + 1] - 1 : offsets[i]);
			*byte = 0x81;
			UT_ASSERT_EQ(cluster_ctrc_recovery_local_empty_v1(), CLUSTER_CTRC_LOCAL_WAIT);
			UT_ASSERT_EQ(*byte, 0x81); UT_ASSERT_EQ(empty_held, 0);
			*byte = 0;
		}
	}
	UT_ASSERT_EQ(cluster_ctrc_recovery_local_empty_v1(), CLUSTER_CTRC_LOCAL_EMPTY);
}

UT_TEST(test_local_empty_keeps_actual_origin_and_participant_obligations)
{
	ClusterCtrcOriginEntry *origin;
	empty_reset(); external_lock_hook = NULL;
	origin = seed_origin(0, 2);
	UT_ASSERT(origin != NULL);
	external_lock_hook = empty_release;
	UT_ASSERT_EQ(cluster_ctrc_recovery_local_empty_v1(), CLUSTER_CTRC_LOCAL_WAIT);
	UT_ASSERT_EQ(empty_held, 0);
}

UT_TEST(test_local_empty_releases_every_partial_collision_in_reverse)
{
	for (unsigned at = 1; at <= lengthof(empty_locks); at++) {
		empty_reset(); empty_collision = at;
		UT_ASSERT_EQ(cluster_ctrc_recovery_local_empty_v1(), CLUSTER_CTRC_LOCAL_WAIT);
		UT_ASSERT_EQ(empty_attempts, at); UT_ASSERT_EQ(empty_releases, at - 1);
		UT_ASSERT_EQ(empty_held, 0); UT_ASSERT_EQ(sleepable_acquisitions, 0);
	}
}

UT_TEST(test_local_empty_rejects_caller_locks_before_acquisition)
{
	empty_reset(); empty_caller_lock = true;
	UT_ASSERT_EQ(cluster_ctrc_recovery_local_empty_v1(), CLUSTER_CTRC_LOCAL_REFUSED);
	UT_ASSERT_EQ(empty_attempts, 0); UT_ASSERT_EQ(empty_releases, 0);
	empty_caller_lock = false;
}

UT_TEST(test_local_empty_error_releases_only_acquired_local_locks)
{
	for (unsigned at = 1; at <= lengthof(empty_locks); at++) {
		volatile bool caught = false;
		empty_reset(); empty_error = at;
		PG_TRY(); { (void)cluster_ctrc_recovery_local_empty_v1(); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught); UT_ASSERT_EQ(empty_attempts, at);
		UT_ASSERT_EQ(empty_releases, at - 1); UT_ASSERT_EQ(empty_held, 0);
	}
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_local_empty_checks_real_fresh_layout_without_mutation);
	UT_RUN(test_local_empty_refuses_missing_or_changed_original_layout);
	UT_RUN(test_local_empty_keeps_unknown_bytes_in_every_original_array);
	UT_RUN(test_local_empty_keeps_actual_origin_and_participant_obligations);
	UT_RUN(test_local_empty_releases_every_partial_collision_in_reverse);
	UT_RUN(test_local_empty_rejects_caller_locks_before_acquisition);
	UT_RUN(test_local_empty_error_releases_only_acquired_local_locks);
	external_lock_hook = NULL;
	UT_ASSERT_EQ(empty_held, 0); reset_fixture(); free(CtrcShared); CtrcShared = NULL;
	UT_DONE(); return ut_failed_count != 0;
}
