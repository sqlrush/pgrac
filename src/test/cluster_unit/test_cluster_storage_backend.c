/*-------------------------------------------------------------------------
 *
 * test_cluster_storage_backend.c
 *    Check SQL admission and periodic interrupt safety points.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_storage_backend.c
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
#include "cluster/cluster_storage_quorum.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_shared_config = true;
bool IsUnderPostmaster = true;
int cluster_node_id = 1, cluster_quorum_poll_interval_ms = 2000;
volatile sig_atomic_t InterruptPending;
static Latch latch;
Latch *MyLatch = &latch;
static bool eligible, active_transaction, timer_active, expecting_fatal;
static unsigned registrations, enables, wakes, checks, fatals;
static int error_code;
static jmp_buf fatal_return;
static timeout_handler_proc timeout_callback;

bool
cluster_storage_quorum_allows_node(int node)
{
	UT_ASSERT_EQ(node, 1);
	checks++;
	return eligible;
}
bool
IsTransactionState(void)
{
	return active_transaction;
}
TimestampTz
GetCurrentTimestamp(void)
{
	return 1000000;
}
TimeoutId
RegisterTimeout(TimeoutId id, timeout_handler_proc handler)
{
	UT_ASSERT_EQ(id, USER_TIMEOUT);
	registrations++;
	timeout_callback = handler;
	return USER_TIMEOUT + 2;
}
bool
get_timeout_active(TimeoutId id)
{
	UT_ASSERT_EQ(id, USER_TIMEOUT + 2);
	return timer_active;
}
void
enable_timeout_every(TimeoutId id, TimestampTz finish, int delay)
{
	UT_ASSERT_EQ(id, USER_TIMEOUT + 2);
	UT_ASSERT(delay > 0 && delay <= 1000);
	UT_ASSERT_EQ(finish, GetCurrentTimestamp() + delay * 1000);
	enables++;
	timer_active = true;
}
void
SetLatch(Latch *target)
{
	UT_ASSERT(target == MyLatch);
	wakes++;
}
bool
errstart(int level, const char *domain)
{
	UT_ASSERT_EQ(level, FATAL);
	fatals++;
	return true;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
int
errcode(int code)
{
	error_code = code;
	return 0;
}
int
errmsg(const char *format, ...)
{
	return 0;
}
int
errhint(const char *format, ...)
{
	return 0;
}
void
errfinish(const char *file, int line, const char *function)
{
	if (!expecting_fatal)
		abort();
	longjmp(fatal_return, 1);
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}

#include "../../backend/cluster/cluster_storage_quorum_backend.c"

static void
reset(void)
{
	storage_backend_timeout = MAX_TIMEOUTS;
	storage_backend_check_pending = false;
	cluster_shared_config = IsUnderPostmaster = eligible = true;
	active_transaction = timer_active = expecting_fatal = false;
	InterruptPending = false;
	registrations = enables = wakes = checks = fatals = 0;
}
static void
must_refuse(void (*operation)(void))
{
	expecting_fatal = true;
	if (setjmp(fatal_return) == 0) {
		operation();
		UT_ASSERT(false);
	}
	expecting_fatal = false;
	UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_QUORUM_LOST_BACKEND);
}
UT_TEST(unavailable_command_is_refused_before_timer_admission)
{
	reset();
	eligible = false;
	must_refuse(cluster_storage_quorum_check_sql);
	UT_ASSERT_EQ(checks, 1);
	UT_ASSERT_EQ(registrations, 0);
}
UT_TEST(repeated_sql_does_not_extend_the_periodic_timer)
{
	reset();
	cluster_storage_quorum_check_sql();
	cluster_storage_quorum_check_sql();
	UT_ASSERT_EQ(registrations, 1);
	UT_ASSERT_EQ(enables, 1);
	UT_ASSERT_EQ(checks, 2);
}
UT_TEST(timeout_is_flag_only_and_active_read_fails_at_safe_point)
{
	reset();
	cluster_storage_quorum_check_sql();
	eligible = false;
	active_transaction = true;
	timeout_callback();
	UT_ASSERT(InterruptPending);
	UT_ASSERT_EQ(wakes, 1);
	UT_ASSERT_EQ(fatals, 0);
	UT_ASSERT_EQ(checks, 1);
	must_refuse(cluster_storage_quorum_check_interrupts);
	UT_ASSERT(!storage_backend_check_pending);
}
UT_TEST(idle_absorbs_reminder_but_next_command_is_refused)
{
	reset();
	cluster_storage_quorum_check_sql();
	eligible = false;
	timeout_callback();
	cluster_storage_quorum_check_interrupts();
	UT_ASSERT_EQ(fatals, 0);
	UT_ASSERT_EQ(checks, 1);
	must_refuse(cluster_storage_quorum_check_sql);
}
UT_TEST(pg_error_timer_cleanup_is_rearmed_without_another_slot)
{
	reset();
	cluster_storage_quorum_check_sql();
	timer_active = false; /* PG ERROR's disable_all_timeouts. */
	cluster_storage_quorum_check_sql();
	UT_ASSERT_EQ(registrations, 1);
	UT_ASSERT_EQ(enables, 2);
}
UT_TEST(native_bypasses_but_single_user_cannot_bypass_shared_eligibility)
{
	reset();
	eligible = false;
	cluster_shared_config = false;
	cluster_storage_quorum_check_sql();
	cluster_shared_config = true;
	IsUnderPostmaster = false;
	must_refuse(cluster_storage_quorum_check_sql);
	UT_ASSERT_EQ(checks, 1);
	UT_ASSERT_EQ(registrations, 0);
}
int
main(void)
{
	UT_PLAN(6);
	UT_RUN(unavailable_command_is_refused_before_timer_admission);
	UT_RUN(repeated_sql_does_not_extend_the_periodic_timer);
	UT_RUN(timeout_is_flag_only_and_active_read_fails_at_safe_point);
	UT_RUN(idle_absorbs_reminder_but_next_command_is_refused);
	UT_RUN(pg_error_timer_cleanup_is_rearmed_without_another_slot);
	UT_RUN(native_bypasses_but_single_user_cannot_bypass_shared_eligibility);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
