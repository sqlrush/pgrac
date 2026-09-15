/* Original node-removal owner; native disk completion is explicitly supplied
 * at the original QVOTEC completion edge. No live cluster or marker I/O. */
#include "postgres.h"
#include "miscadmin.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_node_remove.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_reconfig.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "utils/timestamp.h"
#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();
bool IsUnderPostmaster = true;
BackendType MyBackendType = B_LMON;
int cluster_quorum_poll_interval_ms = 2000;
static ClusterNodeRemoveState region;
static unsigned lock_depth, wake_calls;
static bool foreign_lock;
void
ExceptionalCondition(const char *cond, const char *file, int line)
{
	fprintf(stderr, "assertion %s at %s:%d\n", cond, file, line);
	abort();
}
void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	(void)name;
	UT_ASSERT_EQ(size, MAXALIGN(sizeof(region)));
	*found = false;
	return &region;
}
void
LWLockInitialize(LWLock *lock, int tranche)
{
	(void)lock;
	(void)tranche;
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT(lock == &region.lock);
	UT_ASSERT_EQ(mode, LW_SHARED);
	lock_depth++;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == &region.lock);
	UT_ASSERT_EQ(lock_depth, 1);
	lock_depth--;
}
bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	(void)lock;
	(void)mode;
	return false;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	(void)lock;
	return lock_depth != 0;
}
void
ForEachLWLockHeldByMe(void (*cb)(LWLock *, LWLockMode, void *), void *arg)
{
	if (foreign_lock || lock_depth != 0)
		cb(&region.lock, LW_SHARED, arg);
}
void
SetLatch(Latch *latch)
{
	(void)latch;
	wake_calls++;
}
void
cluster_lmon_marker_complete_wakeup(void)
{
	wake_calls++;
}
TimestampTz
GetCurrentTimestamp(void)
{
	return 1000000;
}
void
cluster_reconfig_note_marker_timeout(ClusterMarkerAsyncKind kind, int32 node, uint64 elapsed)
{
	(void)kind;
	(void)node;
	(void)elapsed;
}
void
cluster_reconfig_note_marker_slow_ack(ClusterMarkerAsyncKind kind, int32 node, uint64 elapsed)
{
	(void)kind;
	(void)node;
	(void)elapsed;
}
#include "../../backend/cluster/cluster_node_remove.c"
static void
setup(void)
{
	IsUnderPostmaster = true;
	MyBackendType = B_LMON;
	lock_depth = wake_calls = 0;
	foreign_lock = false;
	cluster_node_remove_shmem_init();
	nr_qvotec_last_processed_marker_seq = nr_qvotec_inflight_marker_seq = 0;
	memset(&nr_marker_async, 0, sizeof(nr_marker_async));
	nr_release_marker_stage();
}
static ClusterNormalStopPollResult
poll(const char **reason)
{
	const char *domain = NULL;
	uint64 key = 0;
	unsigned wakes = wake_calls;
	ClusterNodeRemoveState before = region;
	ClusterMarkerAsync stage_before = nr_marker_async;
	ClusterNormalStopPollResult result
		= cluster_node_remove_normal_stop_poll(&domain, &key, reason);
	UT_ASSERT_STR_EQ(domain, "NODE_REMOVE");
	UT_ASSERT(*reason != NULL);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(wake_calls, wakes);
	UT_ASSERT(memcmp(&region, &before, sizeof(region)) == 0);
	UT_ASSERT(memcmp(&nr_marker_async, &stage_before, sizeof(stage_before)) == 0);
	return result;
}
UT_TEST(original_init_and_clean_abort_history_are_not_new_debt)
{
	const char *reason;
	setup();
	UT_ASSERT_EQ(poll(&reason), CLUSTER_NORMAL_STOP_READY);
	pg_atomic_write_u32(&region.phase, CLUSTER_REMOVE_ABORTED);
	region.coordinator_node_id = 1;
	region.removal_event_id = 991;
	pg_atomic_write_u64(&region.removal_aborted_count, 5);
	UT_ASSERT_EQ(poll(&reason), CLUSTER_NORMAL_STOP_READY);
}
UT_TEST(all_in_progress_phases_and_terminal_conflicts_are_classified)
{
	const char *reason;
	for (unsigned phase = 1; phase <= CLUSTER_REMOVE_ABORTED_ESCALATE; phase++) {
		setup();
		pg_atomic_write_u32(&region.phase, phase);
		if (phase != CLUSTER_REMOVE_ABORTED) {
			region.target_node_id = 2;
			region.coordinator_node_id = 1;
		}
		UT_ASSERT_EQ(poll(&reason), phase == CLUSTER_REMOVE_ABORTED
										? CLUSTER_NORMAL_STOP_READY
										: (phase == CLUSTER_REMOVE_CLEANUP_BLOCKED
												   || phase == CLUSTER_REMOVE_COMMITTED
												   || phase == CLUSTER_REMOVE_ABORTED_ESCALATE
											   ? CLUSTER_NORMAL_STOP_INVALID
											   : CLUSTER_NORMAL_STOP_PENDING));
	}
}
UT_TEST(real_marker_stays_owned_after_disk_ack_until_original_consumer)
{
	const char *reason;
	ClusterRemovalMarker disk_input;
	setup();
	UT_ASSERT(!nr_write_marker(CLUSTER_REMOVAL_MARKER_REMOVING, 2, 7, 31, 100));
	UT_ASSERT_EQ(poll(&reason), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(cluster_node_remove_qvotec_poll_pending(&disk_input));
	UT_ASSERT_EQ(disk_input.removal_event_id, 100);
	cluster_node_remove_qvotec_complete(true); /* native durable boundary */
	UT_ASSERT_EQ(poll(&reason), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(nr_write_marker(CLUSTER_REMOVAL_MARKER_REMOVING, 2, 7, 31, 100));
	UT_ASSERT_EQ(poll(&reason), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT(!nr_write_marker(CLUSTER_REMOVAL_MARKER_REMOVING, 2, 7, 31, 101));
	UT_ASSERT_EQ(poll(&reason), CLUSTER_NORMAL_STOP_PENDING);
}
UT_TEST(mailbox_and_private_residue_cannot_hide_behind_idle)
{
	const char *reason;
	setup();
	pg_atomic_write_u64(&region.marker_request_seq, 1);
	UT_ASSERT_EQ(poll(&reason), CLUSTER_NORMAL_STOP_PENDING);
	setup();
	nr_marker_async.has_staged_event = true;
	UT_ASSERT_EQ(poll(&reason), CLUSTER_NORMAL_STOP_PENDING);
	setup();
	nr_marker_submitted = true;
	UT_ASSERT_EQ(poll(&reason), CLUSTER_NORMAL_STOP_PENDING);
	setup();
	nr_marker_phase = CLUSTER_REMOVAL_MARKER_REMOVING;
	UT_ASSERT_EQ(poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
}
UT_TEST(invalid_overrides_pending_without_erasing_any_owner)
{
	const char *reason;
	setup();
	pg_atomic_write_u64(&region.marker_request_seq, 1);
	nr_marker_async.state = (ClusterMarkerAsyncState)99;
	UT_ASSERT_EQ(poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
	setup();
	pg_atomic_write_u64(&region.marker_completion_seq, 1);
	UT_ASSERT_EQ(poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
	setup();
	region.fence_armed = true;
	UT_ASSERT_EQ(poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
	setup();
	pg_atomic_write_u32(&region.phase, 99);
	UT_ASSERT_EQ(poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
}
UT_TEST(missing_state_wrong_owner_and_nested_lock_are_not_empty)
{
	const char *reason;
	setup();
	nr_state = NULL;
	UT_ASSERT_EQ(poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
	setup();
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT_EQ(poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
	setup();
	IsUnderPostmaster = false;
	UT_ASSERT_EQ(poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
	setup();
	foreign_lock = true;
	UT_ASSERT_EQ(poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
}
int
main(void)
{
	UT_PLAN(6);
	UT_RUN(original_init_and_clean_abort_history_are_not_new_debt);
	UT_RUN(all_in_progress_phases_and_terminal_conflicts_are_classified);
	UT_RUN(real_marker_stays_owned_after_disk_ack_until_original_consumer);
	UT_RUN(mailbox_and_private_residue_cannot_hide_behind_idle);
	UT_RUN(invalid_overrides_pending_without_erasing_any_owner);
	UT_RUN(missing_state_wrong_owner_and_nested_lock_are_not_empty);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
