/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_lms_controls.c
 *	Stage 8 R4 D4-B LMS worker-incarnation publication.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_ic_rdma.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_lms.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

extern uint64 cluster_lms_test_publish_r4_worker_incarnation(ClusterLmsSharedState *state,
															 int worker_id);
extern bool cluster_lms_test_r4_drain_request(ClusterLmsSharedState *state, uint64 generation,
											  uint64 *worker_incarnation);
extern bool cluster_lms_test_r4_drain_ack(ClusterLmsSharedState *state, uint64 worker_incarnation,
										  uint64 generation);
extern bool cluster_lms_test_r4_drain_ack_matches(ClusterLmsSharedState *state,
												  uint64 worker_incarnation, uint64 generation);
extern bool cluster_lms_test_r4_drain_ack_tick(ClusterLmsSharedState *state,
											   uint64 worker_incarnation);

int cluster_node_id = 1;

static int ut_lock_acquire_count;
static int ut_lock_release_count;
static LWLock *ut_lock;
static LWLockMode ut_lock_mode;
static bool ut_local_drained;
static bool ut_tier1_pending[CLUSTER_MAX_NODES];
static bool ut_rdma_pending[CLUSTER_MAX_NODES];
static int ut_local_drained_calls;
static int ut_tier1_pending_calls;
static int ut_rdma_pending_calls;

bool
cluster_cr_server_r4_worker0_drained(void)
{
	ut_local_drained_calls++;
	return ut_local_drained;
}

bool
cluster_ic_tier1_pending_outbound(int32 peer_id)
{
	UT_ASSERT(peer_id >= 0 && peer_id < CLUSTER_MAX_NODES);
	ut_tier1_pending_calls++;
	return ut_tier1_pending[peer_id];
}

bool
cluster_ic_rdma_pending_outbound(int32 peer_id)
{
	UT_ASSERT(peer_id >= 0 && peer_id < CLUSTER_MAX_NODES);
	ut_rdma_pending_calls++;
	return ut_rdma_pending[peer_id];
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	ut_lock_acquire_count++;
	ut_lock = lock;
	ut_lock_mode = mode;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	ut_lock_release_count++;
	UT_ASSERT(lock == ut_lock);
}

static void
reset_fixture(ClusterLmsSharedState *state)
{
	memset(state, 0, sizeof(*state));
	ut_lock_acquire_count = 0;
	ut_lock_release_count = 0;
	ut_lock = NULL;
	ut_lock_mode = LW_SHARED;
	ut_local_drained = true;
	memset(ut_tier1_pending, 0, sizeof(ut_tier1_pending));
	memset(ut_rdma_pending, 0, sizeof(ut_rdma_pending));
	ut_local_drained_calls = 0;
	ut_tier1_pending_calls = 0;
	ut_rdma_pending_calls = 0;
}

UT_TEST(test_worker0_clears_stale_ack_before_fresh_incarnation)
{
	ClusterLmsSharedState state;
	uint64 published;

	reset_fixture(&state);
	state.r4_controls.data_worker_incarnation[0] = UINT64_C(7);
	state.r4_controls.drain_request_generation = UINT64_C(17);
	state.r4_controls.drain_ack_generation = UINT64_C(99);

	published = cluster_lms_test_publish_r4_worker_incarnation(&state, 0);

	UT_ASSERT_EQ(published, UINT64_C(8));
	UT_ASSERT_EQ(state.r4_controls.data_worker_incarnation[0], UINT64_C(8));
	UT_ASSERT_EQ(state.r4_controls.drain_request_generation, UINT64_C(17));
	UT_ASSERT_EQ(state.r4_controls.drain_ack_generation, UINT64_C(0));
	UT_ASSERT_EQ(ut_lock_acquire_count, 1);
	UT_ASSERT_EQ(ut_lock_release_count, 1);
	UT_ASSERT(ut_lock == &state.lwlock);
	UT_ASSERT_EQ(ut_lock_mode, LW_EXCLUSIVE);
}

UT_TEST(test_nonzero_worker_publishes_only_own_incarnation)
{
	ClusterLmsSharedState state;
	uint64 published;

	reset_fixture(&state);
	state.r4_controls.data_worker_incarnation[0] = UINT64_C(7);
	state.r4_controls.data_worker_incarnation[3] = UINT64_C(41);
	state.r4_controls.drain_request_generation = UINT64_C(17);
	state.r4_controls.drain_ack_generation = UINT64_C(99);

	published = cluster_lms_test_publish_r4_worker_incarnation(&state, 3);

	UT_ASSERT_EQ(published, UINT64_C(42));
	UT_ASSERT_EQ(state.r4_controls.data_worker_incarnation[0], UINT64_C(7));
	UT_ASSERT_EQ(state.r4_controls.data_worker_incarnation[3], UINT64_C(42));
	UT_ASSERT_EQ(state.r4_controls.drain_request_generation, UINT64_C(17));
	UT_ASSERT_EQ(state.r4_controls.drain_ack_generation, UINT64_C(99));
	UT_ASSERT_EQ(ut_lock_acquire_count, 1);
	UT_ASSERT_EQ(ut_lock_release_count, 1);
}

UT_TEST(test_incarnation_exhaustion_refuses_without_wrap)
{
	ClusterLmsSharedState state;
	uint64 published;

	reset_fixture(&state);
	state.r4_controls.data_worker_incarnation[0] = UINT64_MAX;
	state.r4_controls.drain_ack_generation = UINT64_C(99);

	published = cluster_lms_test_publish_r4_worker_incarnation(&state, 0);

	UT_ASSERT_EQ(published, UINT64_C(0));
	UT_ASSERT_EQ(state.r4_controls.data_worker_incarnation[0], UINT64_MAX);
	UT_ASSERT_EQ(state.r4_controls.drain_ack_generation, UINT64_C(0));
	UT_ASSERT_EQ(ut_lock_acquire_count, 1);
	UT_ASSERT_EQ(ut_lock_release_count, 1);
}

UT_TEST(test_invalid_worker_refuses_without_lock_or_mutation)
{
	ClusterLmsSharedState state;
	ClusterLmsSharedState before;

	reset_fixture(&state);
	state.r4_controls.data_worker_incarnation[0] = UINT64_C(7);
	state.r4_controls.drain_ack_generation = UINT64_C(99);
	before = state;

	UT_ASSERT_EQ(cluster_lms_test_publish_r4_worker_incarnation(&state, -1), UINT64_C(0));
	UT_ASSERT_EQ(cluster_lms_test_publish_r4_worker_incarnation(&state, CLUSTER_LMS_MAX_WORKERS),
				 UINT64_C(0));
	UT_ASSERT(memcmp(&state, &before, sizeof(state)) == 0);
	UT_ASSERT_EQ(ut_lock_acquire_count, 0);
	UT_ASSERT_EQ(ut_lock_release_count, 0);
}

UT_TEST(test_drain_request_and_ack_are_current_incarnation_bound)
{
	ClusterLmsSharedState state;
	uint64 worker_incarnation = 0;

	reset_fixture(&state);
	state.r4_controls.data_worker_incarnation[0] = UINT64_C(8);
	state.r4_controls.drain_ack_generation = UINT64_C(99);

	UT_ASSERT(cluster_lms_test_r4_drain_request(&state, UINT64_C(17), &worker_incarnation));
	UT_ASSERT_EQ(worker_incarnation, UINT64_C(8));
	UT_ASSERT_EQ(state.r4_controls.drain_request_generation, UINT64_C(17));
	UT_ASSERT_EQ(state.r4_controls.drain_ack_generation, UINT64_C(0));

	UT_ASSERT(!cluster_lms_test_r4_drain_ack(&state, UINT64_C(7), UINT64_C(17)));
	UT_ASSERT_EQ(state.r4_controls.drain_ack_generation, UINT64_C(0));
	UT_ASSERT(!cluster_lms_test_r4_drain_ack(&state, UINT64_C(8), UINT64_C(18)));
	UT_ASSERT_EQ(state.r4_controls.drain_ack_generation, UINT64_C(0));
	UT_ASSERT(cluster_lms_test_r4_drain_ack(&state, UINT64_C(8), UINT64_C(17)));
	UT_ASSERT_EQ(state.r4_controls.drain_ack_generation, UINT64_C(17));
	UT_ASSERT(cluster_lms_test_r4_drain_ack_matches(&state, UINT64_C(8), UINT64_C(17)));

	state.r4_controls.data_worker_incarnation[0] = UINT64_C(9);
	UT_ASSERT(!cluster_lms_test_r4_drain_ack_matches(&state, UINT64_C(8), UINT64_C(17)));
	UT_ASSERT_EQ(ut_lock_acquire_count, 6);
	UT_ASSERT_EQ(ut_lock_release_count, 6);
	UT_ASSERT_EQ(ut_lock_mode, LW_SHARED);
}

UT_TEST(test_same_generation_drain_request_preserves_matching_ack)
{
	ClusterLmsSharedState state;
	uint64 worker_incarnation = 0;

	reset_fixture(&state);
	state.r4_controls.data_worker_incarnation[0] = UINT64_C(8);
	state.r4_controls.drain_request_generation = UINT64_C(17);
	state.r4_controls.drain_ack_generation = UINT64_C(17);

	UT_ASSERT(cluster_lms_test_r4_drain_request(&state, UINT64_C(17), &worker_incarnation));
	UT_ASSERT_EQ(worker_incarnation, UINT64_C(8));
	UT_ASSERT_EQ(state.r4_controls.drain_request_generation, UINT64_C(17));
	UT_ASSERT_EQ(state.r4_controls.drain_ack_generation, UINT64_C(17));

	UT_ASSERT(cluster_lms_test_r4_drain_request(&state, UINT64_C(18), &worker_incarnation));
	UT_ASSERT_EQ(state.r4_controls.drain_request_generation, UINT64_C(18));
	UT_ASSERT_EQ(state.r4_controls.drain_ack_generation, UINT64_C(0));
}

UT_TEST(test_drain_request_rejects_zero_and_missing_worker_without_mutation)
{
	ClusterLmsSharedState state;
	ClusterLmsSharedState before;
	uint64 worker_incarnation = UINT64_C(55);

	reset_fixture(&state);
	state.r4_controls.drain_request_generation = UINT64_C(11);
	state.r4_controls.drain_ack_generation = UINT64_C(12);
	before = state;

	UT_ASSERT(!cluster_lms_test_r4_drain_request(&state, UINT64_C(0), &worker_incarnation));
	UT_ASSERT(!cluster_lms_test_r4_drain_request(&state, UINT64_C(13), NULL));
	UT_ASSERT(!cluster_lms_test_r4_drain_request(NULL, UINT64_C(13), &worker_incarnation));
	UT_ASSERT_EQ(worker_incarnation, UINT64_C(55));
	UT_ASSERT(memcmp(&state, &before, sizeof(state)) == 0);
	UT_ASSERT_EQ(ut_lock_acquire_count, 0);
	UT_ASSERT_EQ(ut_lock_release_count, 0);

	UT_ASSERT(!cluster_lms_test_r4_drain_request(&state, UINT64_C(13), &worker_incarnation));
	UT_ASSERT_EQ(worker_incarnation, UINT64_C(55));
	UT_ASSERT(memcmp(&state, &before, sizeof(state)) == 0);
	UT_ASSERT_EQ(ut_lock_acquire_count, 1);
	UT_ASSERT_EQ(ut_lock_release_count, 1);
}

UT_TEST(test_drain_ack_tick_requires_local_and_both_transport_zeros)
{
	ClusterLmsSharedState state;

	reset_fixture(&state);
	state.r4_controls.data_worker_incarnation[0] = UINT64_C(8);
	state.r4_controls.drain_request_generation = UINT64_C(17);

	ut_local_drained = false;
	UT_ASSERT(!cluster_lms_test_r4_drain_ack_tick(&state, UINT64_C(8)));
	UT_ASSERT_EQ(ut_local_drained_calls, 1);
	UT_ASSERT_EQ(ut_tier1_pending_calls, 0);
	UT_ASSERT_EQ(ut_rdma_pending_calls, 0);
	UT_ASSERT_EQ(state.r4_controls.drain_ack_generation, UINT64_C(0));

	ut_local_drained = true;
	ut_tier1_pending[2] = true;
	UT_ASSERT(!cluster_lms_test_r4_drain_ack_tick(&state, UINT64_C(8)));
	UT_ASSERT_EQ(state.r4_controls.drain_ack_generation, UINT64_C(0));
	ut_tier1_pending[2] = false;

	ut_rdma_pending[3] = true;
	UT_ASSERT(!cluster_lms_test_r4_drain_ack_tick(&state, UINT64_C(8)));
	UT_ASSERT_EQ(state.r4_controls.drain_ack_generation, UINT64_C(0));
	ut_rdma_pending[3] = false;

	ut_tier1_pending_calls = 0;
	ut_rdma_pending_calls = 0;
	UT_ASSERT(cluster_lms_test_r4_drain_ack_tick(&state, UINT64_C(8)));
	UT_ASSERT_EQ(state.r4_controls.drain_ack_generation, UINT64_C(17));
	UT_ASSERT_EQ(ut_tier1_pending_calls, CLUSTER_MAX_NODES - 1);
	UT_ASSERT_EQ(ut_rdma_pending_calls, CLUSTER_MAX_NODES - 1);
}

UT_TEST(test_drain_ack_tick_rejects_stale_worker_and_zero_request_before_local_scan)
{
	ClusterLmsSharedState state;

	reset_fixture(&state);
	state.r4_controls.data_worker_incarnation[0] = UINT64_C(8);
	state.r4_controls.drain_request_generation = UINT64_C(17);

	UT_ASSERT(!cluster_lms_test_r4_drain_ack_tick(&state, UINT64_C(7)));
	UT_ASSERT_EQ(ut_local_drained_calls, 0);
	UT_ASSERT_EQ(ut_tier1_pending_calls, 0);
	UT_ASSERT_EQ(ut_rdma_pending_calls, 0);

	state.r4_controls.drain_request_generation = UINT64_C(0);
	UT_ASSERT(!cluster_lms_test_r4_drain_ack_tick(&state, UINT64_C(8)));
	UT_ASSERT_EQ(ut_local_drained_calls, 0);
	UT_ASSERT_EQ(state.r4_controls.drain_ack_generation, UINT64_C(0));
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_worker0_clears_stale_ack_before_fresh_incarnation);
	UT_RUN(test_nonzero_worker_publishes_only_own_incarnation);
	UT_RUN(test_incarnation_exhaustion_refuses_without_wrap);
	UT_RUN(test_invalid_worker_refuses_without_lock_or_mutation);
	UT_RUN(test_drain_request_and_ack_are_current_incarnation_bound);
	UT_RUN(test_same_generation_drain_request_preserves_matching_ack);
	UT_RUN(test_drain_request_rejects_zero_and_missing_worker_without_mutation);
	UT_RUN(test_drain_ack_tick_requires_local_and_both_transport_zeros);
	UT_RUN(test_drain_ack_tick_rejects_stale_worker_and_zero_request_before_local_scan);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
