/*-------------------------------------------------------------------------
 *
 * test_cluster_storage_sql_route.c
 *    Execute production command and executor admission hooks.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_storage_sql_route.c
 *
 * NOTES
 *    PGRAC-original unit tests; product symbols retain the cluster_ prefix.
 *    Spec: spec-s9p2-06-online-membership.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <setjmp.h>
#include "access/xact.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "utils/timeout.h"
#include "cluster/cluster_storage_quorum.h"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

static bool xact_started, eligible;
static unsigned checks, starts, executions;
static jmp_buf refused;
bool IsUnderPostmaster = true;
int client_connection_check_interval;
struct Port *MyProcPort;
ExecutorStart_hook_type ExecutorStart_hook;

void cluster_storage_quorum_check_sql(void);
void
cluster_storage_quorum_check_sql(void)
{
	checks++;
	if (!eligible)
		longjmp(refused, 1);
}
void
StartTransactionCommand(void)
{
	starts++;
}
static void
enable_statement_timeout(void)
{}
bool
get_timeout_active(TimeoutId id)
{
	return true;
}
void
enable_timeout_after(TimeoutId id, int delay)
{
	abort();
}
static void
pgstat_report_query_id(uint64 queryid, bool force)
{}
void
standard_ExecutorStart(QueryDesc *desc, int flags)
{
	executions++;
}

#include "test_cluster_storage_sql_route.inc"

UT_TEST(new_and_existing_commands_check_before_transaction_work)
{
	eligible = false;
	checks = starts = 0;
	xact_started = false;
	if (setjmp(refused) == 0)
		start_xact_command();
	UT_ASSERT_EQ(checks, 1);
	UT_ASSERT_EQ(starts, 0);
	xact_started = true;
	if (setjmp(refused) == 0)
		start_xact_command();
	UT_ASSERT_EQ(checks, 2);
	UT_ASSERT_EQ(starts, 0);
	eligible = true;
	start_xact_command();
	UT_ASSERT_EQ(checks, 3);
}
UT_TEST(executor_checks_parallel_and_hooked_entries_before_work)
{
	PlannedStmt plan = { 0 };
	QueryDesc query = { 0 };

	query.plannedstmt = &plan;
	checks = executions = 0;
	eligible = false;
	if (setjmp(refused) == 0)
		ExecutorStart(&query, 0);
	UT_ASSERT_EQ(checks, 1);
	UT_ASSERT_EQ(executions, 0);
	ExecutorStart_hook = standard_ExecutorStart;
	if (setjmp(refused) == 0)
		ExecutorStart(&query, 0);
	UT_ASSERT_EQ(checks, 2);
	UT_ASSERT_EQ(executions, 0);
	eligible = true;
	ExecutorStart(&query, 0);
	UT_ASSERT_EQ(executions, 1);
}
int
main(void)
{
	UT_PLAN(2);
	UT_RUN(new_and_existing_commands_check_before_transaction_work);
	UT_RUN(executor_checks_parallel_and_hooked_entries_before_work);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
