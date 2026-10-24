/* Author: SqlRush <sqlrush@gmail.com>
 * Native worker-handle exit observation and the original shared F4 windows.
 * BGW status and process lifetime are doubles; no OS exit/crash is claimed. */
int recovery_stamp_base_main(void);
#define main recovery_stamp_base_main
#include "test_cluster_recovery_stamp.c"
#undef main

/* The exact-parent RED has no exit API or reaper notification. These two
 * refusal stubs let its real reaper run; they are not product definitions. */
#ifndef CLUSTER_RECOVERY_STAMP_EXIT_V1
#define CLUSTER_RECOVERY_STAMP_ADOPT_PENDING 5
static bool
cluster_recovery_stamp_worker_begin_v1(uint16 thread, uint64 attempt)
{
	return false;
}
static bool
cluster_recovery_stamp_worker_stopped_v1(uint16 thread, uint64 attempt)
{
	return false;
}
#endif

static BgwHandleStatus exit_status;
static unsigned exit_frees, exit_fatals;
static bool expect_adopt_at_free;
static BackgroundWorkerHandle *const exit_handle = (BackgroundWorkerHandle *)(uintptr_t)0x1234;

BgwHandleStatus
GetBackgroundWorkerPid(BackgroundWorkerHandle *handle, pid_t *pid)
{
	UT_ASSERT(handle == exit_handle);
	*pid = exit_status == BGWH_STARTED ? 11 : InvalidPid;
	return exit_status;
}
static void
exit_free(void *handle)
{
	UT_ASSERT(handle == exit_handle);
	if (expect_adopt_at_free)
		UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_ADOPT_PENDING);
	exit_frees++;
}
#undef ereport
#define ereport(level, rest) do { exit_fatals++; return; } while (0)
#define pfree exit_free
#include "test_cluster_recovery_stamp_reap.inc"
#undef pfree

static void
exit_fresh(void)
{
	fresh();
	memset(thread_recovery_owned, 0, sizeof(thread_recovery_owned));
	exit_frees = exit_fatals = 0;
	expect_adopt_at_free = false;
	exit_status = BGWH_STOPPED;
	MyBackendType = B_BG_WORKER;
	pg_atomic_write_u32(&region.thread_replay[2].state, CLUSTER_THREADREC_REPLAY_REPLAYING);
	pg_atomic_write_u64(&region.thread_replay[2].episode_epoch, 77);
	UT_ASSERT(cluster_recovery_stamp_worker_begin_v1(2, 77));
	thread_recovery_owned[2].handle = exit_handle;
	thread_recovery_owned[2].attempt_stamp = 77;
}

static void
exit_observe(void)
{
	MyBackendType = B_LMON;
	thread_recovery_reap_one(2, NULL);
}

UT_TEST(stopped_preserves_each_original_phase_and_payload)
{
	for (unsigned phase = 0; phase < 4; phase++) {
		ClusterRecoveryStampHandleV1 h;
		ClusterRecoveryStampSlotV1 before;
		ClusterRecoveryStampCaptureV1 capture;
		exit_fresh();
		if (!begin(&h))
			continue;
		if (phase == 1) {
			CritSectionCount = 1;
			UT_ASSERT(cluster_recovery_stamp_inserting_v1(&h));
			CritSectionCount = 0;
		} else if (phase >= 2) {
			insert(&h);
			if (phase == 3)
				UT_ASSERT(cluster_recovery_stamp_durable_v1(&h));
		}
		before = region.thread_replay[2].stamp;
		UT_ASSERT(cluster_recovery_stamp_capture_v1(&capture));
		expect_adopt_at_free = true;
		exit_observe();
		UT_ASSERT_EQ(exit_fatals, 0);
		UT_ASSERT_EQ(exit_frees, 1);
		UT_ASSERT_EQ(region.thread_replay[2].stamp.adopt_from, before.phase);
		UT_ASSERT_EQ(pg_atomic_read_u32(&region.thread_replay[2].state), CLUSTER_THREADREC_REPLAY_IDLE);
		UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_ADOPT_PENDING);
		UT_ASSERT(memcmp(&before.handle, &region.thread_replay[2].stamp.handle, sizeof(h)) == 0);
		UT_ASSERT(memcmp(before.handle.payload, region.thread_replay[2].stamp.handle.payload, sizeof(before.handle.payload)) == 0);
		UT_ASSERT_EQ(before.record_start, region.thread_replay[2].stamp.record_start);
		UT_ASSERT_EQ(before.record_end, region.thread_replay[2].stamp.record_end);
		UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&capture), CLUSTER_RECOVERY_STAMP_PENDING);
		MyBackendType = B_BG_WORKER;
		UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
		UT_ASSERT(!cluster_recovery_stamp_durable_v1(&h));
		UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
		UT_ASSERT(!cluster_recovery_stamp_fail_v1(&h));
	}
}

UT_TEST(stopped_preserves_failed_obligation_for_adoption)
{
	ClusterRecoveryStampHandleV1 h;
	ClusterRecoveryStampCaptureV1 capture;
	exit_fresh();
	if (!begin(&h)) return;
	insert(&h);
	UT_ASSERT(cluster_recovery_stamp_fail_v1(&h));
	UT_ASSERT(cluster_recovery_stamp_capture_v1(&capture));
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&capture), CLUSTER_RECOVERY_STAMP_INVALID);
	exit_observe();
	UT_ASSERT(region.thread_replay[2].stamp.failed);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_ADOPT_PENDING);
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&capture), CLUSTER_RECOVERY_STAMP_PENDING);
	MyBackendType = B_LMON;
	UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
}

UT_TEST(stale_attempt_and_wrong_role_cannot_mark_exit)
{
	ClusterRecoveryStampHandleV1 h;
	exit_fresh();
	UT_ASSERT(cluster_recovery_stamp_worker_begin_v1(2, 77));
	UT_ASSERT(!cluster_recovery_stamp_worker_begin_v1(2, 78));
	MyProcPid++;
	UT_ASSERT(!cluster_recovery_stamp_worker_begin_v1(2, 77));
	MyProcPid--;
	if (!begin(&h)) return;
	UT_ASSERT(!cluster_recovery_stamp_worker_stopped_v1(2, 77));
	MyBackendType = B_LMON;
	UT_ASSERT(!cluster_recovery_stamp_worker_stopped_v1(2, 78));
	UT_ASSERT(!cluster_recovery_stamp_worker_stopped_v1(2, 0));
	UT_ASSERT(!cluster_recovery_stamp_worker_stopped_v1(0, 77));
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_PRE_INSERT);
	exit_observe();
	UT_ASSERT(cluster_recovery_stamp_worker_stopped_v1(2, 77)); /* same observation is idempotent */
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_ADOPT_PENDING);
}

UT_TEST(original_handle_and_attempt_control_exit_observation)
{
	ClusterRecoveryStampHandleV1 h;
	bool retained = false;
	exit_fresh();
	if (!begin(&h)) return;
	MyBackendType = B_LMON;
	exit_status = BGWH_STARTED;
	thread_recovery_reap_one(2, &retained);
	UT_ASSERT(retained);
	UT_ASSERT_EQ(exit_frees, 0);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_PRE_INSERT);
	exit_status = BGWH_STOPPED;
	thread_recovery_owned[2].attempt_stamp = 78;
	thread_recovery_reap_one(2, NULL);
	UT_ASSERT_EQ(exit_fatals, 1);
	UT_ASSERT_EQ(exit_frees, 0);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_PRE_INSERT);
	thread_recovery_owned[2].attempt_stamp = 77;
	exit_observe();
	UT_ASSERT_EQ(exit_frees, 1);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_ADOPT_PENDING);
}

UT_TEST(replacement_cannot_reuse_unfinished_window)
{
	ClusterRecoveryStampHandleV1 h, next;
	exit_fresh();
	if (!begin(&h)) return;
	exit_observe();
	MyBackendType = B_BG_WORKER;
	MyProcPid = owner.executor_pid = 12;
	pg_atomic_write_u32(&region.thread_replay[2].state, CLUSTER_THREADREC_REPLAY_REPLAYING);
	pg_atomic_write_u64(&region.thread_replay[2].episode_epoch, 78);
	UT_ASSERT(!cluster_recovery_stamp_worker_begin_v1(2, 78));
	UT_ASSERT(!cluster_recovery_stamp_prepare_v1(&owner, 1, payload, sizeof(payload), &next));
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_ADOPT_PENDING);
	/* The refused replacement still has a real native handle to reap. Its
	 * exit cannot retire the older obligation or be mistaken for its owner. */
	thread_recovery_owned[2].handle = exit_handle;
	thread_recovery_owned[2].attempt_stamp = 78;
	exit_observe();
	UT_ASSERT_EQ(exit_fatals, 0);
	UT_ASSERT_EQ(exit_frees, 2);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.executor_attempt_stamp, 77);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_ADOPT_PENDING);
}

UT_TEST(empty_exit_and_pid_reuse_do_not_adopt_startup_window)
{
	ClusterRecoveryStampHandleV1 h;
	exit_fresh();
	exit_observe();
	UT_ASSERT_EQ(exit_fatals, 0);
	UT_ASSERT_EQ(exit_frees, 1);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);

	exit_fresh();
	MyBackendType = B_STARTUP; /* identical PID is not the registered BGW launch */
	if (!begin(&h)) return;
	exit_observe();
	UT_ASSERT_EQ(exit_fatals, 0);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_PRE_INSERT);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(stopped_preserves_each_original_phase_and_payload);
	UT_RUN(stopped_preserves_failed_obligation_for_adoption);
	UT_RUN(stale_attempt_and_wrong_role_cannot_mark_exit);
	UT_RUN(original_handle_and_attempt_control_exit_observation);
	UT_RUN(replacement_cannot_reuse_unfinished_window);
	UT_RUN(empty_exit_and_pid_reuse_do_not_adopt_startup_window);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
