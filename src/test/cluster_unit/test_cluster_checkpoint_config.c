/*-------------------------------------------------------------------------
 *
 * test_cluster_checkpoint_config.c
 *    Execute the original checkpoint/restartpoint configuration wrappers.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_checkpoint_config.c
 * NOTES
 *    Inner checkpoint I/O is a boundary; native TAP runs the real checkpoint.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xlog.h"
#include "cluster/cluster_shared_config.h"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static bool managed, throw_error, restart_result;
static unsigned enters, leaves, checkpoints, restartpoints, depth;
static int expected_flags;

bool
cluster_shared_config_delivery_work_enter(void)
{
	enters++;
	if (managed)
		depth++;
	return managed;
}

void
cluster_shared_config_delivery_work_leave(bool entered)
{
	leaves++;
	UT_ASSERT(entered == managed);
	if (entered) {
		UT_ASSERT_EQ(depth, 1);
		depth--;
	}
}

void
pg_re_throw(void)
{
	if (!PG_exception_stack)
		abort();
	siglongjmp(*PG_exception_stack, 1);
}

static void
CreateCheckPointInternal(int flags)
{
	UT_ASSERT_EQ(flags, expected_flags);
	UT_ASSERT_EQ(depth, managed ? 1 : 0);
	checkpoints++;
	if (throw_error)
		pg_re_throw();
}

static bool
CreateRestartPointInternal(int flags)
{
	UT_ASSERT_EQ(flags, expected_flags);
	UT_ASSERT_EQ(depth, managed ? 1 : 0);
	restartpoints++;
	if (throw_error)
		pg_re_throw();
	return restart_result;
}

#include "test_cluster_checkpoint_config.inc"

static void
reset(void)
{
	managed = true;
	throw_error = false;
	restart_result = true;
	enters = leaves = checkpoints = restartpoints = depth = 0;
	expected_flags = CHECKPOINT_FORCE;
}

UT_TEST(checkpoint_holds_through_every_native_entry)
{
	int flags[] = { 0, CHECKPOINT_FORCE, CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_IMMEDIATE,
					CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_IMMEDIATE };
	for (size_t i = 0; i < lengthof(flags); ++i) {
		reset();
		expected_flags = flags[i];
		CreateCheckPoint(flags[i]);
		UT_ASSERT_EQ(enters, 1);
		UT_ASSERT_EQ(leaves, 1);
		UT_ASSERT_EQ(depth, 0);
		UT_ASSERT_EQ(checkpoints, 1);
	}
}

UT_TEST(restartpoint_preserves_both_return_values)
{
	for (unsigned value = 0; value < 2; ++value) {
		reset();
		restart_result = value != 0;
		UT_ASSERT(CreateRestartPoint(expected_flags) == restart_result);
		UT_ASSERT_EQ(restartpoints, 1);
		UT_ASSERT_EQ(enters, 1);
		UT_ASSERT_EQ(leaves, 1);
		UT_ASSERT_EQ(depth, 0);
	}
}

UT_TEST(checkpoint_error_rethrows_after_local_cleanup)
{
	for (unsigned restart = 0; restart < 2; ++restart) {
		sigjmp_buf error_boundary;
		reset();
		throw_error = true;
		PG_exception_stack = &error_boundary;
		if (sigsetjmp(error_boundary, 0) == 0) {
			if (restart)
				(void)CreateRestartPoint(expected_flags);
			else
				CreateCheckPoint(expected_flags);
			UT_ASSERT(false);
		}
		UT_ASSERT(PG_exception_stack == &error_boundary);
		UT_ASSERT_EQ(enters, 1);
		UT_ASSERT_EQ(leaves, 1);
		UT_ASSERT_EQ(depth, 0);
		UT_ASSERT_EQ(checkpoints + restartpoints, 1);
		PG_exception_stack = NULL;
	}
}

UT_TEST(unmanaged_native_checkpoint_keeps_original_behavior)
{
	reset();
	managed = false;
	CreateCheckPoint(expected_flags);
	UT_ASSERT(CreateRestartPoint(expected_flags));
	UT_ASSERT_EQ(checkpoints, 1);
	UT_ASSERT_EQ(restartpoints, 1);
	UT_ASSERT_EQ(enters, 2);
	UT_ASSERT_EQ(leaves, 2);
	UT_ASSERT_EQ(depth, 0);
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(checkpoint_holds_through_every_native_entry);
	UT_RUN(restartpoint_preserves_both_return_values);
	UT_RUN(checkpoint_error_rethrows_after_local_cleanup);
	UT_RUN(unmanaged_native_checkpoint_keeps_original_behavior);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
