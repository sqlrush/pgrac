/*-------------------------------------------------------------------------
 *
 * test_cluster_marker_async.c
 *	  Unit tests for the process-local qvotec marker async FSM.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_marker_async.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_marker_async.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static int ut_set_latch_count = 0;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

void
SetLatch(Latch *latch pg_attribute_unused())
{
	ut_set_latch_count++;
}

static void
ut_reset(ClusterMarkerAsync *a, pg_atomic_uint64 *request_seq, pg_atomic_uint64 *completion_seq,
		 pg_atomic_uint32 *result_slot)
{
	cluster_marker_async_init(a);
	pg_atomic_init_u64(request_seq, 0);
	pg_atomic_init_u64(completion_seq, 0);
	pg_atomic_init_u32(result_slot, 0);
	ut_set_latch_count = 0;
}

UT_TEST(test_init_defaults_idle)
{
	ClusterMarkerAsync a;

	cluster_marker_async_init(&a);

	UT_ASSERT_EQ(a.state, CLUSTER_MARKER_ASYNC_IDLE);
	UT_ASSERT_EQ(a.inflight_seq, 0);
	UT_ASSERT_EQ(a.target_node, -1);
	UT_ASSERT(!a.has_staged_event);
}

UT_TEST(test_submit_publishes_one_request)
{
	ClusterMarkerAsync a;
	pg_atomic_uint64 request_seq;
	pg_atomic_uint64 completion_seq;
	pg_atomic_uint32 result_slot;
	Latch latch;
	bool ok;

	ut_reset(&a, &request_seq, &completion_seq, &result_slot);

	ok = cluster_marker_async_submit(&a, &request_seq, &completion_seq, &latch, 1000, 5000,
									 CLUSTER_MARKER_KIND_FENCE_FAILSTOP, 7);

	UT_ASSERT(ok);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_seq), 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&completion_seq), 0);
	UT_ASSERT_EQ(ut_set_latch_count, 1);
	UT_ASSERT_EQ(a.state, CLUSTER_MARKER_ASYNC_SUBMITTED);
	UT_ASSERT_EQ(a.inflight_seq, 1);
	UT_ASSERT_EQ(a.deadline_us, 6000);
	UT_ASSERT_EQ(a.kind, CLUSTER_MARKER_KIND_FENCE_FAILSTOP);
	UT_ASSERT_EQ(a.target_node, 7);
}

UT_TEST(test_poll_pending_before_completion)
{
	ClusterMarkerAsync a;
	pg_atomic_uint64 request_seq;
	pg_atomic_uint64 completion_seq;
	pg_atomic_uint32 result_slot;
	ClusterMarkerPollResult pr;
	uint32 result = 99;
	uint64 elapsed = 99;

	ut_reset(&a, &request_seq, &completion_seq, &result_slot);
	UT_ASSERT(cluster_marker_async_submit(&a, &request_seq, &completion_seq, NULL, 1000, 5000,
										  CLUSTER_MARKER_KIND_JOIN_PREPARE, 2));

	pr = cluster_marker_async_poll(&a, &completion_seq, &result_slot, 1500, &result, &elapsed);

	UT_ASSERT_EQ(pr, CLUSTER_MARKER_POLL_PENDING);
	UT_ASSERT_EQ(a.state, CLUSTER_MARKER_ASYNC_SUBMITTED);
	UT_ASSERT_EQ(result, 0);
	UT_ASSERT_EQ(elapsed, 0);
}

UT_TEST(test_poll_ack_returns_result_and_elapsed)
{
	ClusterMarkerAsync a;
	pg_atomic_uint64 request_seq;
	pg_atomic_uint64 completion_seq;
	pg_atomic_uint32 result_slot;
	ClusterMarkerPollResult pr;
	uint32 result = 0;
	uint64 elapsed = 0;

	ut_reset(&a, &request_seq, &completion_seq, &result_slot);
	UT_ASSERT(cluster_marker_async_submit(&a, &request_seq, &completion_seq, NULL, 1000, 5000,
										  CLUSTER_MARKER_KIND_JOIN_COMMITTED, 3));
	pg_atomic_write_u32(&result_slot, 42);
	pg_atomic_write_u64(&completion_seq, 1);

	pr = cluster_marker_async_poll(&a, &completion_seq, &result_slot, 2500, &result, &elapsed);

	UT_ASSERT_EQ(pr, CLUSTER_MARKER_POLL_ACKED);
	UT_ASSERT_EQ(result, 42);
	UT_ASSERT_EQ(elapsed, 1500);
	UT_ASSERT_EQ(a.state, CLUSTER_MARKER_ASYNC_IDLE);
}

UT_TEST(test_poll_timeout_releases_submit_state)
{
	ClusterMarkerAsync a;
	pg_atomic_uint64 request_seq;
	pg_atomic_uint64 completion_seq;
	pg_atomic_uint32 result_slot;
	ClusterMarkerPollResult pr;
	uint64 elapsed = 0;

	ut_reset(&a, &request_seq, &completion_seq, &result_slot);
	UT_ASSERT(cluster_marker_async_submit(&a, &request_seq, &completion_seq, NULL, 1000, 1000,
										  CLUSTER_MARKER_KIND_CLEAN_LEAVE_COMMITTED, 4));

	pr = cluster_marker_async_poll(&a, &completion_seq, &result_slot, 2000, NULL, &elapsed);

	UT_ASSERT_EQ(pr, CLUSTER_MARKER_POLL_TIMEOUT);
	UT_ASSERT_EQ(elapsed, 1000);
	UT_ASSERT_EQ(a.state, CLUSTER_MARKER_ASYNC_IDLE);
}

UT_TEST(test_mailbox_busy_does_not_publish)
{
	ClusterMarkerAsync a;
	pg_atomic_uint64 request_seq;
	pg_atomic_uint64 completion_seq;
	pg_atomic_uint32 result_slot;
	Latch latch;
	bool ok;

	ut_reset(&a, &request_seq, &completion_seq, &result_slot);
	pg_atomic_write_u64(&request_seq, 2);
	pg_atomic_write_u64(&completion_seq, 1);

	ok = cluster_marker_async_submit(&a, &request_seq, &completion_seq, &latch, 1000, 5000,
									 CLUSTER_MARKER_KIND_NODE_REMOVE_SHRUNK, 5);

	UT_ASSERT(!ok);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_seq), 2);
	UT_ASSERT_EQ(ut_set_latch_count, 0);
	UT_ASSERT_EQ(a.state, CLUSTER_MARKER_ASYNC_IDLE);
}

UT_TEST(test_reentrant_submit_does_not_bump_again)
{
	ClusterMarkerAsync a;
	pg_atomic_uint64 request_seq;
	pg_atomic_uint64 completion_seq;
	pg_atomic_uint32 result_slot;
	bool ok;

	ut_reset(&a, &request_seq, &completion_seq, &result_slot);
	UT_ASSERT(cluster_marker_async_submit(&a, &request_seq, &completion_seq, NULL, 1000, 5000,
										  CLUSTER_MARKER_KIND_FENCE_NODE_REMOVED, 6));

	ok = cluster_marker_async_submit(&a, &request_seq, &completion_seq, NULL, 1200, 5000,
									 CLUSTER_MARKER_KIND_FENCE_FAILSTOP, 99);

	UT_ASSERT(ok);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_seq), 1);
	UT_ASSERT_EQ(a.inflight_seq, 1);
	UT_ASSERT_EQ(a.kind, CLUSTER_MARKER_KIND_FENCE_NODE_REMOVED);
	UT_ASSERT_EQ(a.target_node, 6);
}

UT_TEST(test_release_stage_clears_staged_record)
{
	ClusterMarkerAsync a;
	pg_atomic_uint64 request_seq;
	pg_atomic_uint64 completion_seq;
	pg_atomic_uint32 result_slot;

	ut_reset(&a, &request_seq, &completion_seq, &result_slot);
	a.has_staged_event = true;
	a.staged_expect_epoch = 123;
	UT_ASSERT(cluster_marker_async_submit(&a, &request_seq, &completion_seq, NULL, 1000, 5000,
										  CLUSTER_MARKER_KIND_NODE_REMOVE_REMOVED, 8));

	cluster_marker_async_release_stage(&a);

	UT_ASSERT_EQ(a.state, CLUSTER_MARKER_ASYNC_IDLE);
	UT_ASSERT_EQ(a.inflight_seq, 0);
	UT_ASSERT_EQ(a.target_node, -1);
	UT_ASSERT(!a.has_staged_event);
	UT_ASSERT_EQ(a.staged_expect_epoch, 0);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_init_defaults_idle);
	UT_RUN(test_submit_publishes_one_request);
	UT_RUN(test_poll_pending_before_completion);
	UT_RUN(test_poll_ack_returns_result_and_elapsed);
	UT_RUN(test_poll_timeout_releases_submit_state);
	UT_RUN(test_mailbox_busy_does_not_publish);
	UT_RUN(test_reentrant_submit_does_not_bump_again);
	UT_RUN(test_release_stage_clears_staged_record);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
