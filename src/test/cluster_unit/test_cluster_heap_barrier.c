/*-------------------------------------------------------------------------
 *
 * test_cluster_heap_barrier.c
 *	  Behavioral tests for typed index-fetch dispatch and cleanup.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_heap_barrier.c
 *
 * NOTES
 *	  This standalone test executes the dependency-light typed dispatcher
 *	  consumed by tableam.c.  Literal fixtures cover all three outcomes,
 *	  legacy fallback, error propagation and exact cleanup counts without
 *	  reading production source files.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>

#undef printf

#include "access/tableam.h"
#include "access/tableam_barrier.h"
#include "unit_test.h"


UT_DEFINE_GLOBALS();


typedef struct FetchFixture {
	TableIndexFetchTupleResult typed_result;
	bool legacy_found;
	bool mutate_tid;
	bool set_all_dead;
	bool slot_empty;
	bool call_again_seen;
	int tid;
	int typed_calls;
	int legacy_calls;
	int cleanup_calls;
	bool throw_error;
} FetchFixture;

static sigjmp_buf error_jump;

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

static TableIndexFetchTupleResult
fixture_typed_fetch(void *context, bool *call_again, bool *all_dead)
{
	FetchFixture *fixture = context;

	fixture->typed_calls++;
	fixture->call_again_seen = *call_again;
	if (fixture->throw_error)
		siglongjmp(error_jump, 1);
	if (fixture->mutate_tid)
		fixture->tid++;
	if (fixture->set_all_dead && all_dead != NULL)
		*all_dead = true;
	if (fixture->typed_result == TABLE_INDEX_FETCH_FOUND)
		fixture->slot_empty = false;
	return fixture->typed_result;
}

static bool
fixture_legacy_fetch(void *context, bool *call_again, bool *all_dead)
{
	FetchFixture *fixture = context;

	fixture->legacy_calls++;
	fixture->call_again_seen = *call_again;
	if (fixture->set_all_dead && all_dead != NULL)
		*all_dead = true;
	if (fixture->legacy_found)
		fixture->slot_empty = false;
	return fixture->legacy_found;
}

static void
fixture_cleanup(void *context)
{
	FetchFixture *fixture = context;

	fixture->cleanup_calls++;
}

static const TableIndexFetchBarrierOps fixture_ops = { .typed_fetch = fixture_typed_fetch,
													   .legacy_fetch = fixture_legacy_fetch,
													   .cleanup = fixture_cleanup };

static FetchFixture
fetch_fixture(void)
{
	FetchFixture fixture;

	MemSet(&fixture, 0, sizeof(fixture));
	fixture.slot_empty = true;
	fixture.tid = 42;
	return fixture;
}

UT_TEST(test_t1_legacy_false_maps_to_not_found)
{
	FetchFixture fixture = fetch_fixture();
	bool all_dead = true;

	UT_ASSERT_EQ(table_index_fetch_barrier_execute(&fixture_ops, &fixture, false, &all_dead),
				 TABLE_INDEX_FETCH_NOT_FOUND);
	UT_ASSERT_EQ(fixture.legacy_calls, 1);
	UT_ASSERT_EQ(fixture.typed_calls, 0);
	UT_ASSERT_EQ(fixture.cleanup_calls, 1);
}

UT_TEST(test_t2_legacy_true_maps_to_found)
{
	FetchFixture fixture = fetch_fixture();

	fixture.legacy_found = true;
	UT_ASSERT_EQ(table_index_fetch_barrier_execute(&fixture_ops, &fixture, false, NULL),
				 TABLE_INDEX_FETCH_FOUND);
	UT_ASSERT(!fixture.slot_empty);
	UT_ASSERT_EQ(fixture.cleanup_calls, 1);
}

UT_TEST(test_t3_barrier_is_disjoint_and_mutates_no_visibility_state)
{
	FetchFixture fixture = fetch_fixture();
	bool all_dead = true;

	fixture.typed_result = TABLE_INDEX_FETCH_BARRIER_CLOSED;
	UT_ASSERT_EQ(table_index_fetch_barrier_execute(&fixture_ops, &fixture, true, &all_dead),
				 TABLE_INDEX_FETCH_BARRIER_CLOSED);
	UT_ASSERT_EQ(fixture.tid, 42);
	UT_ASSERT(!all_dead);
	UT_ASSERT(fixture.slot_empty);
	UT_ASSERT_EQ(fixture.legacy_calls, 0);
}

UT_TEST(test_t4_typed_not_found_preserves_completed_scan_result)
{
	FetchFixture fixture = fetch_fixture();

	fixture.typed_result = TABLE_INDEX_FETCH_NOT_FOUND;
	UT_ASSERT_EQ(table_index_fetch_barrier_execute(&fixture_ops, &fixture, true, NULL),
				 TABLE_INDEX_FETCH_NOT_FOUND);
	UT_ASSERT_EQ(fixture.typed_calls, 1);
	UT_ASSERT(fixture.slot_empty);
}

UT_TEST(test_t5_typed_found_preserves_callback_outputs)
{
	FetchFixture fixture = fetch_fixture();
	bool all_dead = false;

	fixture.typed_result = TABLE_INDEX_FETCH_FOUND;
	fixture.mutate_tid = true;
	fixture.set_all_dead = true;
	UT_ASSERT_EQ(table_index_fetch_barrier_execute(&fixture_ops, &fixture, true, &all_dead),
				 TABLE_INDEX_FETCH_FOUND);
	UT_ASSERT_EQ(fixture.tid, 43);
	UT_ASSERT(all_dead);
	UT_ASSERT(!fixture.slot_empty);
	UT_ASSERT(!fixture.call_again_seen);
}

UT_TEST(test_t6_callback_error_propagates_without_false_result)
{
	FetchFixture fixture = fetch_fixture();

	fixture.throw_error = true;
	if (sigsetjmp(error_jump, 1) == 0) {
		(void)table_index_fetch_barrier_execute(&fixture_ops, &fixture, true, NULL);
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(fixture.cleanup_calls, 0);
}

UT_TEST(test_t7_cleanup_runs_once_for_each_normal_result)
{
	FetchFixture fixture = fetch_fixture();
	TableIndexFetchTupleResult result;

	for (result = TABLE_INDEX_FETCH_NOT_FOUND; result <= TABLE_INDEX_FETCH_BARRIER_CLOSED;
		 result++) {
		fixture.typed_result = result;
		UT_ASSERT_EQ(table_index_fetch_barrier_execute(&fixture_ops, &fixture, true, NULL), result);
	}
	UT_ASSERT_EQ(fixture.cleanup_calls, 3);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_t1_legacy_false_maps_to_not_found);
	UT_RUN(test_t2_legacy_true_maps_to_found);
	UT_RUN(test_t3_barrier_is_disjoint_and_mutates_no_visibility_state);
	UT_RUN(test_t4_typed_not_found_preserves_completed_scan_result);
	UT_RUN(test_t5_typed_found_preserves_callback_outputs);
	UT_RUN(test_t6_callback_error_propagates_without_false_result);
	UT_RUN(test_t7_cleanup_runs_once_for_each_normal_result);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
