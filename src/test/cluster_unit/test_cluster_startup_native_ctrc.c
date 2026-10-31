/*-------------------------------------------------------------------------
 * test_cluster_startup_native_ctrc.c
 *     Original native CTRC directory in the original Startup test binary.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * NOTES
 *     Complete native fixture and conditional-lock scheduler are retained.
 *     Only ERROR context and TAP globals are shared. Scheduling is controlled.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_startup_native_ctrc_namespace.h"
int main(void);
#define TEST_CLUSTER_STARTUP_CTRC
#include "test_cluster_ctrc_cleaner.c"
#include "test_cluster_startup_native_ctrc_owner.inc"
#include "test_cluster_startup_native_ctrc.h"

static unsigned startup_native_probes, startup_native_sleepable;
static uint8 *startup_native_snapshot;
static Size startup_native_snapshot_bytes;

void
test_startup_native_ctrc_unlocked(void)
{
	UT_ASSERT_EQ(empty_held, 0);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(sleepable_acquisitions, startup_native_sleepable);
	UT_ASSERT_EQ(allocation_attempts, 0);
}

void
test_startup_native_ctrc_begin(unsigned mode, unsigned at)
{
	UT_ASSERT(startup_native_snapshot == NULL && CtrcShared == NULL);
	UT_ASSERT(mode <= 3 && at <= lengthof(empty_locks));
	empty_reset();
	if (mode == 1) {
		external_lock_hook = NULL;
		UT_ASSERT(seed_origin(0, 2) != NULL);
		external_lock_hook = empty_release;
	}
	if (mode == 2) CtrcShared->participant_offset++;
	if (mode == 3) empty_error = at;
	else if (mode == 0) empty_collision = at;
	config_read_hook = empty_config_read;
	startup_native_probes = 0;
	startup_native_sleepable = sleepable_acquisitions;
	startup_native_snapshot_bytes = CtrcShared->total_bytes;
	startup_native_snapshot = malloc(startup_native_snapshot_bytes);
	UT_ASSERT(startup_native_snapshot != NULL);
	memcpy(startup_native_snapshot, CtrcShared, startup_native_snapshot_bytes);
	test_startup_native_ctrc_unlocked();
}

ClusterCtrcLocalEmptyV1
test_startup_native_ctrc_probe(void)
{
	startup_native_probes++;
	test_startup_native_ctrc_unlocked();
	return cluster_ctrc_recovery_local_empty_v1();
}

void
test_startup_native_ctrc_check(unsigned probes, unsigned attempts, unsigned releases)
{
	test_startup_native_ctrc_unlocked();
	UT_ASSERT_EQ(startup_native_probes, probes);
	UT_ASSERT_EQ(empty_attempts, attempts);
	UT_ASSERT_EQ(empty_releases, releases);
	UT_ASSERT(startup_native_snapshot != NULL);
	UT_ASSERT(memcmp(startup_native_snapshot, CtrcShared, startup_native_snapshot_bytes) == 0);
}

void
test_startup_native_ctrc_end(void)
{
	test_startup_native_ctrc_unlocked();
	UT_ASSERT(memcmp(startup_native_snapshot, CtrcShared, startup_native_snapshot_bytes) == 0);
	free(startup_native_snapshot);
	startup_native_snapshot = NULL;
	free(CtrcShared);
	CtrcShared = NULL;
	external_lock_hook = NULL;
}

/* The original fixture already includes the complete production TU. Reuse
 * those exact functions with its private symbols, never an online admission. */
ClusterCtrcItlCleanoutApplyResult
test_startup_native_ctrc_project(const ClusterColdTtSlotInputV1 *subjects, Size count,
	const char *before, char *after)
{
	return cluster_ctrc_recovery_itl_page_image_v1(subjects, count, before, after);
}

bool
test_startup_native_ctrc_heap(const char *image)
{
	return cluster_ctrc_recovery_heap_itl_references_v1(image);
}

bool
test_startup_native_ctrc_hot(const char *image, BlockNumber block)
{
	return cluster_ctrc_recovery_heap_hot_references_v1(image, block);
}

/* Packed tuple grammar from the same complete original production TU. */
bool
test_startup_native_ctrc_tuple(const ClusterItlSlotData *slots, const char *bytes, Size length)
{
	return cluster_ctrc_recovery_heap_tuple_itl_references_v1(slots, bytes, length);
}
