/*-------------------------------------------------------------------------
 *
 * test_cluster_share_barrier.c
 *	  Behavioral tests for barrier-refusal cleanup choreography.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_share_barrier.c
 *
 * NOTES
 *	  This test executes the same dependency-light refusal sequencer used by
 *	  bufmgr.c.  Callbacks model the real holder, writer, master and local
 *	  ownership operations while retaining literal state and call-order
 *	  evidence.  No source text is inspected.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#undef printf

#include "storage/bufmgr_barrier.h"
#include "unit_test.h"


UT_DEFINE_GLOBALS();


typedef enum FixtureAction {
	ACTION_ABORT_HOLDER = 1,
	ACTION_ABORT_WRITER,
	ACTION_RELEASE_MASTER,
	ACTION_CONVERGE_LOCAL,
	ACTION_ABORT_PENDING,
	ACTION_PROVE_EMPTY
} FixtureAction;

typedef struct BarrierFixture {
	bool content_lock;
	bool holder;
	bool writer;
	bool pending;
	bool master_grant;
	bool cached_share_cover;
	bool outer_lock;
	bool first_buffer_lock;
	int pins;
	ClusterBufferBarrierCleanupResult holder_result;
	ClusterBufferBarrierCleanupResult writer_result;
	ClusterBufferBarrierCleanupResult master_result;
	ClusterBufferBarrierCleanupResult converge_result;
	ClusterBufferBarrierCleanupResult pending_result;
	FixtureAction actions[8];
	int action_count;
} BarrierFixture;

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

static void
record_action(BarrierFixture *fixture, FixtureAction action)
{
	fixture->actions[fixture->action_count++] = action;
}

static ClusterBufferBarrierCleanupResult
fixture_abort_holder(void *context)
{
	BarrierFixture *fixture = context;

	record_action(fixture, ACTION_ABORT_HOLDER);
	if (fixture->holder_result == CLUSTER_BUFFER_BARRIER_CLEAN)
		fixture->holder = false;
	return fixture->holder_result;
}

static ClusterBufferBarrierCleanupResult
fixture_abort_writer(void *context)
{
	BarrierFixture *fixture = context;

	record_action(fixture, ACTION_ABORT_WRITER);
	if (fixture->writer_result == CLUSTER_BUFFER_BARRIER_CLEAN)
		fixture->writer = false;
	return fixture->writer_result;
}

static ClusterBufferBarrierCleanupResult
fixture_release_master(void *context)
{
	BarrierFixture *fixture = context;

	record_action(fixture, ACTION_RELEASE_MASTER);
	if (fixture->master_result == CLUSTER_BUFFER_BARRIER_CLEAN)
		fixture->master_grant = false;
	return fixture->master_result;
}

static ClusterBufferBarrierCleanupResult
fixture_converge_local(void *context)
{
	BarrierFixture *fixture = context;

	record_action(fixture, ACTION_CONVERGE_LOCAL);
	if (fixture->converge_result == CLUSTER_BUFFER_BARRIER_CLEAN)
		fixture->pending = false;
	return fixture->converge_result;
}

static ClusterBufferBarrierCleanupResult
fixture_abort_pending(void *context)
{
	BarrierFixture *fixture = context;

	record_action(fixture, ACTION_ABORT_PENDING);
	if (fixture->pending_result == CLUSTER_BUFFER_BARRIER_CLEAN)
		fixture->pending = false;
	return fixture->pending_result;
}

static bool
fixture_prove_empty(void *context)
{
	BarrierFixture *fixture = context;

	record_action(fixture, ACTION_PROVE_EMPTY);
	return !fixture->content_lock && !fixture->holder && !fixture->writer && !fixture->pending
		   && !fixture->master_grant;
}

static const ClusterBufferBarrierUnwindOps fixture_ops = { .abort_holder = fixture_abort_holder,
														   .abort_writer = fixture_abort_writer,
														   .release_master = fixture_release_master,
														   .converge_local = fixture_converge_local,
														   .abort_pending = fixture_abort_pending,
														   .prove_empty = fixture_prove_empty };

static BarrierFixture
clean_fixture(void)
{
	BarrierFixture fixture;

	memset(&fixture, 0, sizeof(fixture));
	fixture.outer_lock = true;
	fixture.first_buffer_lock = true;
	fixture.cached_share_cover = true;
	fixture.pins = 2;
	return fixture;
}

UT_TEST(test_u1_early_barrier_proves_empty)
{
	BarrierFixture fixture = clean_fixture();

	UT_ASSERT_EQ(cluster_buffer_barrier_unwind_execute(&fixture_ops, &fixture, false, false),
				 CLUSTER_BUFFER_BARRIER_UNWIND_OK);
	UT_ASSERT_EQ(fixture.action_count, 3);
	UT_ASSERT_EQ(fixture.actions[2], ACTION_PROVE_EMPTY);
}

UT_TEST(test_u2_cached_cover_is_not_released)
{
	BarrierFixture fixture = clean_fixture();

	UT_ASSERT_EQ(cluster_buffer_barrier_unwind_execute(&fixture_ops, &fixture, false, false),
				 CLUSTER_BUFFER_BARRIER_UNWIND_OK);
	UT_ASSERT(!fixture.master_grant);
	UT_ASSERT(fixture.cached_share_cover);
	UT_ASSERT_EQ(fixture.actions[0], ACTION_ABORT_HOLDER);
}

UT_TEST(test_u3_read_image_clears_exact_pending)
{
	BarrierFixture fixture = clean_fixture();

	fixture.pending = true;
	UT_ASSERT_EQ(cluster_buffer_barrier_unwind_execute(&fixture_ops, &fixture, false, true),
				 CLUSTER_BUFFER_BARRIER_UNWIND_OK);
	UT_ASSERT(!fixture.pending);
	UT_ASSERT_EQ(fixture.actions[2], ACTION_ABORT_PENDING);
}

UT_TEST(test_u4_durable_grant_releases_master_before_local)
{
	BarrierFixture fixture = clean_fixture();

	fixture.pending = true;
	fixture.master_grant = true;
	UT_ASSERT_EQ(cluster_buffer_barrier_unwind_execute(&fixture_ops, &fixture, true, true),
				 CLUSTER_BUFFER_BARRIER_UNWIND_OK);
	UT_ASSERT_EQ(fixture.actions[2], ACTION_RELEASE_MASTER);
	UT_ASSERT_EQ(fixture.actions[3], ACTION_CONVERGE_LOCAL);
	UT_ASSERT_EQ(fixture.actions[4], ACTION_PROVE_EMPTY);
}

UT_TEST(test_u5_rearm_barrier_does_not_abort_twice)
{
	BarrierFixture fixture = clean_fixture();

	UT_ASSERT_EQ(cluster_buffer_barrier_unwind_execute(&fixture_ops, &fixture, false, false),
				 CLUSTER_BUFFER_BARRIER_UNWIND_OK);
	UT_ASSERT_EQ(fixture.action_count, 3);
}

UT_TEST(test_u6_holder_is_removed_before_clean_return)
{
	BarrierFixture fixture = clean_fixture();
	BarrierFixture deferred = clean_fixture();

	fixture.holder = true;
	UT_ASSERT_EQ(cluster_buffer_barrier_unwind_execute(&fixture_ops, &fixture, false, false),
				 CLUSTER_BUFFER_BARRIER_UNWIND_OK);
	UT_ASSERT(!fixture.holder);
	UT_ASSERT_EQ(fixture.actions[2], ACTION_PROVE_EMPTY);

	deferred.holder = true;
	deferred.holder_result = CLUSTER_BUFFER_BARRIER_DEFERRED;
	UT_ASSERT_EQ(cluster_buffer_barrier_unwind_execute(&fixture_ops, &deferred, false, false),
				 CLUSTER_BUFFER_BARRIER_UNWIND_HOLDER_NOT_EMPTY);
	UT_ASSERT(deferred.holder);
}

UT_TEST(test_u7_master_release_failure_preserves_pending)
{
	BarrierFixture fixture = clean_fixture();

	fixture.pending = true;
	fixture.master_grant = true;
	fixture.master_result = CLUSTER_BUFFER_BARRIER_FAILED;
	UT_ASSERT_EQ(cluster_buffer_barrier_unwind_execute(&fixture_ops, &fixture, true, true),
				 CLUSTER_BUFFER_BARRIER_UNWIND_MASTER_RELEASE_FAILED);
	UT_ASSERT(fixture.pending);
	UT_ASSERT(fixture.master_grant);
}

UT_TEST(test_u8_local_convergence_failure_is_not_clean)
{
	BarrierFixture fixture = clean_fixture();

	fixture.pending = true;
	fixture.master_grant = true;
	fixture.converge_result = CLUSTER_BUFFER_BARRIER_FAILED;
	UT_ASSERT_EQ(cluster_buffer_barrier_unwind_execute(&fixture_ops, &fixture, true, true),
				 CLUSTER_BUFFER_BARRIER_UNWIND_LOCAL_CONVERGENCE_FAILED);
	UT_ASSERT(fixture.pending);
	UT_ASSERT(!fixture.master_grant);
}

UT_TEST(test_u9_clean_success_is_the_only_false_postcondition)
{
	BarrierFixture fixture = clean_fixture();

	UT_ASSERT_EQ(cluster_buffer_barrier_unwind_execute(&fixture_ops, &fixture, false, false),
				 CLUSTER_BUFFER_BARRIER_UNWIND_OK);
	UT_ASSERT(!fixture.content_lock);
}

UT_TEST(test_u10_exclusive_writer_cleanup_is_exact)
{
	BarrierFixture fixture = clean_fixture();
	BarrierFixture deferred = clean_fixture();

	fixture.writer = true;
	UT_ASSERT_EQ(cluster_buffer_barrier_unwind_execute(&fixture_ops, &fixture, false, false),
				 CLUSTER_BUFFER_BARRIER_UNWIND_OK);
	UT_ASSERT(!fixture.writer);
	UT_ASSERT(fixture.outer_lock);

	deferred.writer = true;
	deferred.writer_result = CLUSTER_BUFFER_BARRIER_DEFERRED;
	UT_ASSERT_EQ(cluster_buffer_barrier_unwind_execute(&fixture_ops, &deferred, false, false),
				 CLUSTER_BUFFER_BARRIER_UNWIND_WRITER_NOT_EMPTY);
	UT_ASSERT(deferred.writer);
}

UT_TEST(test_u11_two_buffer_outer_lock_stays_owned)
{
	BarrierFixture fixture = clean_fixture();

	UT_ASSERT_EQ(cluster_buffer_barrier_unwind_execute(&fixture_ops, &fixture, false, false),
				 CLUSTER_BUFFER_BARRIER_UNWIND_OK);
	UT_ASSERT(fixture.first_buffer_lock);
}

UT_TEST(test_u12_pins_and_outer_ownership_are_unchanged)
{
	BarrierFixture fixture = clean_fixture();

	UT_ASSERT_EQ(cluster_buffer_barrier_unwind_execute(&fixture_ops, &fixture, false, false),
				 CLUSTER_BUFFER_BARRIER_UNWIND_OK);
	UT_ASSERT_EQ(fixture.pins, 2);
	UT_ASSERT(fixture.outer_lock);
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(test_u1_early_barrier_proves_empty);
	UT_RUN(test_u2_cached_cover_is_not_released);
	UT_RUN(test_u3_read_image_clears_exact_pending);
	UT_RUN(test_u4_durable_grant_releases_master_before_local);
	UT_RUN(test_u5_rearm_barrier_does_not_abort_twice);
	UT_RUN(test_u6_holder_is_removed_before_clean_return);
	UT_RUN(test_u7_master_release_failure_preserves_pending);
	UT_RUN(test_u8_local_convergence_failure_is_not_clean);
	UT_RUN(test_u9_clean_success_is_the_only_false_postcondition);
	UT_RUN(test_u10_exclusive_writer_cleanup_is_exact);
	UT_RUN(test_u11_two_buffer_outer_lock_stays_owned);
	UT_RUN(test_u12_pins_and_outer_ownership_are_unchanged);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
