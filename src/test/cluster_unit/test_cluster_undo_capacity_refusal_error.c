/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 * Original preflight/allocator/controller/error sink, with bounded log capture.
 */
#include "postgres.h"
#include <stdarg.h>
#define CAPACITY_REFUSAL_TEST_GUC_DEFINED
bool cluster_trace_undo_capacity_refusal = false;
#define TT_DURABLE_EXTERNAL_ERROR_DETAIL
static char captured_refusal[8192];
static unsigned captured_details;
int
errdetail(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(captured_refusal, sizeof(captured_refusal), fmt, args);
	va_end(args);
	if (strstr(captured_refusal, "undo_capacity_first_refusal_v1") != NULL)
		captured_details++;
	return 0;
}
#define UNDO_CAPACITY_BIND_FIXTURE_MAIN capacity_refusal_bind_parent_main
#include "test_cluster_undo_capacity_bind.c"

static void
refusal_bind_reset(bool enabled, bool active)
{
	bind_reset(false, active);
	cluster_trace_undo_capacity_refusal = enabled;
	captured_refusal[0] = '\0';
	captured_details = 0;
}

UT_TEST(test_original_54000_is_unchanged_with_diagnostic_off)
{
	volatile bool caught = false;
	refusal_bind_reset(false, false);
	bind_no_owner = true;
	PG_TRY();
	{
		cluster_executor_preflight_write(&empty_statement, &empty_estate, 0);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_PROGRAM_LIMIT_EXCEEDED);
	UT_ASSERT_EQ(captured_details, 0);
	UT_ASSERT(strstr(captured_refusal, "cleaner_tick_age_ms=") != NULL);
	UT_ASSERT(strstr(captured_refusal, "undo_capacity_first_refusal_v1") == NULL);
	bind_assert_counts(0, 1, 0, 0, 1);
}

UT_TEST(test_real_pool_and_selected_slot_errors_carry_exact_one_first_refusal)
{
	for (int active = 0; active < 2; active++) {
		volatile bool caught = false;
		refusal_bind_reset(true, active != 0);
		bind_no_owner = true;
		PG_TRY();
		{
			cluster_executor_preflight_write(&empty_statement, &empty_estate, 0);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_PROGRAM_LIMIT_EXCEEDED);
		UT_ASSERT_EQ(captured_details, 1);
		UT_ASSERT(strstr(captured_refusal, "cleaner_tick_age_ms=") != NULL);
		UT_ASSERT(strstr(captured_refusal, "site=pool_probe") != NULL);
		UT_ASSERT(strstr(captured_refusal, active ? "top_xid=100 active_slot_pending=1"
												  : "top_xid=100 active_slot_pending=0") != NULL);
		UT_ASSERT_EQ(g_bind_emit_calls + g_ctrc_reserve_calls, 0);
		UT_ASSERT_EQ(modifier_enters, modifier_leaves);
		bind_assert_counts(0, 1, 0, 0, 1);
	}
}

UT_TEST(test_original_cancel_does_not_gain_a_capacity_error_or_log)
{
	volatile bool caught = false;
	refusal_bind_reset(true, false);
	bind_wait_error = ERRCODE_QUERY_CANCELED;
	PG_TRY();
	{
		cluster_executor_preflight_write(&empty_statement, &empty_estate, 0);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_QUERY_CANCELED);
	UT_ASSERT_EQ(captured_details, 0);
	bind_assert_counts(1, 1, 0, 1, 0);
}

UT_TEST(test_successful_capacity_reentry_and_bind_do_not_log)
{
	ClusterCanonicalTxnBinding binding;
	refusal_bind_reset(true, false);
	UT_ASSERT(empty_preflight(&binding));
	UT_ASSERT_EQ(captured_details, 0);
	bind_assert_counts(3, 3, 3, 0, 0);
	bind_abort_and_reuse(&binding);
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_original_54000_is_unchanged_with_diagnostic_off);
	UT_RUN(test_real_pool_and_selected_slot_errors_carry_exact_one_first_refusal);
	UT_RUN(test_original_cancel_does_not_gain_a_capacity_error_or_log);
	UT_RUN(test_successful_capacity_reentry_and_bind_do_not_log);
	UT_DONE();
	return ut_failed_count != 0;
}
