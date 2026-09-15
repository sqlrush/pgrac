/* A148: original recovery region and replay publication/retirement.
 * Native worker scheduling is a fixture; no live recovery or I/O. */
#include "postgres.h"
#include "miscadmin.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_recovery_worker.h"
#include "cluster/cluster_thread_recovery.h"
#include "storage/shmem.h"
#include "../../backend/cluster/cluster_recovery_plan.c"
#include "../../backend/cluster/cluster_thread_recovery_orchestrator.c"
#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool IsUnderPostmaster = true;
BackendType MyBackendType = B_LMON;
static ClusterRecoveryPlanShmem region;
static bool found_region;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "assertion %s at %s:%d\n", condition, file, line);
	abort();
}
void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	UT_ASSERT(strcmp(name, "pgrac recovery plan") == 0);
	UT_ASSERT_EQ(size, MAXALIGN(sizeof(region)));
	*found = found_region;
	found_region = true;
	return &region;
}
static void
fresh_region(void)
{
	found_region = false;
	cluster_recovery_plan_shmem_init();
	IsUnderPostmaster = true;
	MyBackendType = B_LMON;
}

UT_TEST(test_original_region_and_exact_observer_role)
{
	UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	fresh_region();
	UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	IsUnderPostmaster = false;
	UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	IsUnderPostmaster = true;
	MyBackendType = B_BACKEND;
	UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
}

UT_TEST(test_validation_workers_complete_but_history_is_retained)
{
	ClusterRecoveryWorkerPool *pool;
	ClusterRecoveryPlan historical = { .generated = true, .failed = true };
	const char *domain, *reason;
	uint64 key;
	fresh_region();
	pool = cluster_recovery_worker_pool_ptr();
	/* Native scheduler/CAS boundary: these workers validate, not redo.
	 * Failed validation/spawn is terminal history, not recovery authority. */
	for (unsigned i = 0; i < CLUSTER_RECOVERY_WORKER_MAX_SLOTS; i++) {
		pg_atomic_write_u32(&pool->slot_state[i], CLUSTER_RECOVERY_WORKER_REQUESTED);
		UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(&domain, &key, &reason),
					 CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT(strcmp(domain, "RECOVERY_WORKER") == 0);
		UT_ASSERT_EQ(key, i);
		pg_atomic_write_u32(&pool->slot_state[i], CLUSTER_RECOVERY_WORKER_RUNNING);
		UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(NULL, NULL, NULL),
					 CLUSTER_NORMAL_STOP_PENDING);
		for (uint32 terminal = CLUSTER_RECOVERY_WORKER_DONE;
			 terminal <= CLUSTER_RECOVERY_WORKER_SPAWN_FAILED; terminal++) {
			pg_atomic_write_u32(&pool->slot_state[i], terminal);
			UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(NULL, NULL, NULL),
						 CLUSTER_NORMAL_STOP_READY);
		}
	}
	publish_plan(&historical); /* the plan is expressly observational */
	UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT(region.plan.failed);
}

UT_TEST(test_every_replay_slot_needs_original_exact_retirement)
{
	const char *domain, *reason;
	uint64 key;
	fresh_region();
	for (uint16 tid = 1; tid <= CLUSTER_RECOVERY_PLAN_THREADS; tid++) {
		ClusterThreadReplaySlot *slot = cluster_thread_recovery_replay_slot(tid);
		ClusterThreadReplaySlot before;
		UT_ASSERT(cluster_thread_recovery_replay_mark_replaying(tid, 700 + tid));
		memcpy(&before, slot, sizeof(before));
		UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(&domain, &key, &reason),
					 CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT(strcmp(domain, "THREAD_REPLAY") == 0);
		UT_ASSERT_EQ(key, tid);
		UT_ASSERT(memcmp(&before, slot, sizeof(before)) == 0);
		UT_ASSERT_EQ(
			cluster_thread_recovery_replay_transition_if_match(
				tid, 701 + tid, CLUSTER_THREADREC_REPLAY_REPLAYING, CLUSTER_THREADREC_REPLAY_DONE),
			CLUSTER_THREADREC_MATCH_STAMP_MISMATCH);
		UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(NULL, NULL, NULL),
					 CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_EQ(
			cluster_thread_recovery_replay_transition_if_match(
				tid, 700 + tid, CLUSTER_THREADREC_REPLAY_REPLAYING, CLUSTER_THREADREC_REPLAY_DONE),
			CLUSTER_THREADREC_MATCH_CHANGED);
		UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(NULL, NULL, NULL),
					 CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(pg_atomic_read_u64(&slot->episode_epoch), 700 + tid);
	}
	UT_ASSERT(cluster_thread_recovery_replay_mark_replaying(128, 900));
	UT_ASSERT_EQ(cluster_thread_recovery_replay_transition_if_match(
					 128, 900, CLUSTER_THREADREC_REPLAY_REPLAYING, CLUSTER_THREADREC_REPLAY_IDLE),
				 CLUSTER_THREADREC_MATCH_CHANGED);
	UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(pg_atomic_read_u64(&region.thread_replay[128].episode_epoch), 900);
}

UT_TEST(test_last_corruption_overrides_pending_and_blocked_never_passes)
{
	const char *domain, *reason;
	uint64 key;
	uint16 last = CLUSTER_RECOVERY_PLAN_THREADS;
	fresh_region();
	pg_atomic_write_u32(&region.pool.slot_state[0], CLUSTER_RECOVERY_WORKER_RUNNING);
	pg_atomic_write_u32(&region.pool.slot_state[CLUSTER_RECOVERY_WORKER_MAX_SLOTS - 1], 99);
	UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(&domain, &key, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(key, CLUSTER_RECOVERY_WORKER_MAX_SLOTS - 1);
	fresh_region();
	UT_ASSERT(cluster_thread_recovery_replay_mark_replaying(1, 10));
	UT_ASSERT(cluster_thread_recovery_replay_mark_replaying(last, 12));
	UT_ASSERT_EQ(
		cluster_thread_recovery_replay_transition_if_match(
			last, 12, CLUSTER_THREADREC_REPLAY_REPLAYING, CLUSTER_THREADREC_REPLAY_BLOCKED),
		CLUSTER_THREADREC_MATCH_CHANGED);
	UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(&domain, &key, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(key, last);
	UT_ASSERT(strcmp(reason, "THREAD_REPLAY_BLOCKED") == 0);
	/* Explicit corruption fixture, never an authorized producer. */
	pg_atomic_write_u32(&region.thread_replay[last].state, CLUSTER_THREADREC_REPLAY_REPLAYING);
	pg_atomic_write_u64(&region.thread_replay[last].episode_epoch, 0);
	UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(NULL, NULL, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT(strcmp(reason, "THREAD_REPLAY_STATE_INVALID") == 0);
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_original_region_and_exact_observer_role);
	UT_RUN(test_validation_workers_complete_but_history_is_retained);
	UT_RUN(test_every_replay_slot_needs_original_exact_retirement);
	UT_RUN(test_last_corruption_overrides_pending_and_blocked_never_passes);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
