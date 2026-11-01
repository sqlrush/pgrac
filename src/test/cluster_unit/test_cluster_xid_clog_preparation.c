/* Allocator preparation contract. Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "access/transam.h"

int xid_allocation_original_main(void);
#define main xid_allocation_original_main
#include "test_cluster_xid_allocation.c"
#undef main

/* These are the CLOG-facing boundaries to be connected to the original
 * control producer. They model no file identity, custody or successful I/O. */
void CLOGPrepareForAllocation(FullTransactionId first, FullTransactionId last);
bool CLOGAllocationRangeReady(FullTransactionId first, FullTransactionId last);

static unsigned prepare_calls, range_checks;
static uint64 prepared_first, prepared_last, preparation_advance;
static uint64 coverage_first, coverage_last, checked_first, checked_last;
static bool preparation_refused, preparation_effect, preparation_error;

/* Controlled complete-range projection only. These ordinals are not file
 * creation identities or permission to initialize a CLOG page. */
#define TEST_CLOG_XIDS_PER_SEGMENT UINT64CONST(1048576)

void
CLOGPrepareForAllocation(FullTransactionId first, FullTransactionId last)
{
	prepare_calls++;
	UT_ASSERT(!held);
	if (prepare_calls == 1)
		UT_ASSERT_EQ(waits, 0);
	UT_ASSERT_EQ(extensions, 0);
	UT_ASSERT_EQ(entries[0].xid, InvalidTransactionId);
	UT_ASSERT_EQ(visible_xids[0], InvalidTransactionId);
	UT_ASSERT(FullTransactionIdIsNormal(first));
	UT_ASSERT(!FullTransactionIdPrecedes(last, first));
	if (preparation_error)
	{
		preparation_effect = true;
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("controlled post-registration failure")));
	}
	if (preparation_refused)
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						errmsg("controlled registration refusal")));
	prepared_first = U64FromFullTransactionId(first);
	prepared_last = U64FromFullTransactionId(last);
	coverage_first = prepared_first / TEST_CLOG_XIDS_PER_SEGMENT;
	coverage_last = prepared_last / TEST_CLOG_XIDS_PER_SEGMENT + 2;
	if (preparation_advance != 0)
	{
		variables.nextXid = FullTransactionIdFromU64(preparation_advance);
		preparation_advance = 0;
	}
}

bool
CLOGAllocationRangeReady(FullTransactionId first, FullTransactionId last)
{
	range_checks++;
	UT_ASSERT(held && held_lock == XidGenLock && held_mode == LW_EXCLUSIVE);
	checked_first = U64FromFullTransactionId(first);
	checked_last = U64FromFullTransactionId(last);
	return prepared_first != 0 &&
		checked_first / TEST_CLOG_XIDS_PER_SEGMENT >= coverage_first &&
		checked_last / TEST_CLOG_XIDS_PER_SEGMENT + 2 <= coverage_last;
}

static void
reset_preparation(void)
{
	reset_allocator();
	prepare_calls = range_checks = 0;
	prepared_first = prepared_last = preparation_advance = 0;
	coverage_first = coverage_last = checked_first = checked_last = 0;
	preparation_refused = preparation_effect = preparation_error = false;
}

UT_TEST(cold_preparation_precedes_original_lease_and_storage)
{
	reset_preparation();
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), 2052);
	UT_ASSERT_EQ(prepare_calls, 1);
	UT_ASSERT(range_checks >= 2);
	UT_ASSERT_EQ(prepared_first, 2052);
	UT_ASSERT_EQ(prepared_last, 2052);
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(lock_errors, 0);
}

UT_TEST(preparation_reentry_rederives_the_original_counter)
{
	reset_preparation();
	preparation_advance = 2084;
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), 2084);
	UT_ASSERT(prepare_calls >= 1);
	UT_ASSERT(range_checks >= 2);
	UT_ASSERT_EQ(entries[0].xid, 2084);
	UT_ASSERT_EQ(lock_errors, 0);
}

UT_TEST(lease_retry_rechecks_the_entire_new_herding_gap)
{
	reset_preparation();
	jump = true;
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), 4100);
	UT_ASSERT(prepare_calls >= 1);
	UT_ASSERT(range_checks >= 3);
	UT_ASSERT_EQ(prepared_first, 2052);
	UT_ASSERT_EQ(checked_first, 2052);
	UT_ASSERT_EQ(checked_last, 4100);
	UT_ASSERT_EQ(clog_full_first, 2052);
	UT_ASSERT_EQ(clog_full_last, 4100);
	UT_ASSERT_EQ(lock_errors, 0);
}

UT_TEST(complete_warm_range_has_no_control_preparation)
{
	reset_preparation();
	prepared_first = 2052;
	prepared_last = 8196;
	coverage_first = 0;
	coverage_last = 2;
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), 2052);
	UT_ASSERT_EQ(prepare_calls, 0);
	UT_ASSERT(range_checks >= 2);
	UT_ASSERT_EQ(lock_errors, 0);
}

UT_TEST(preparation_covers_the_complete_multi_segment_gap)
{
	uint64 candidate = 3 * TEST_CLOG_XIDS_PER_SEGMENT + 4;

	reset_preparation();
	reservation = true;
	floor_full = candidate;
	limit_full = candidate + 1024;
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), candidate);
	UT_ASSERT_EQ(prepare_calls, 1);
	UT_ASSERT_EQ(prepared_first, 2052);
	UT_ASSERT_EQ(prepared_last, candidate);
	UT_ASSERT_EQ(coverage_first, 0);
	UT_ASSERT_EQ(coverage_last, 5);
	UT_ASSERT_EQ(clog_full_first, 2052);
	UT_ASSERT_EQ(clog_full_last, candidate);
	UT_ASSERT_EQ(waits, 0);
	UT_ASSERT_EQ(lock_errors, 0);
}

UT_TEST(current_segment_without_successors_is_not_a_warm_range)
{
	reset_preparation();
	prepared_first = prepared_last = 2052;
	coverage_first = coverage_last = 0;
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), 2052);
	UT_ASSERT_EQ(prepare_calls, 1);
	UT_ASSERT_EQ(coverage_last, 2);
	UT_ASSERT(range_checks >= 2);
	UT_ASSERT_EQ(lock_errors, 0);
}

UT_TEST(wrap_prediction_keeps_both_original_full_identities)
{
	FullTransactionId first = FullTransactionIdFromEpochAndXid(7, UINT32_MAX);
	FullTransactionId candidate = FullTransactionIdFromEpochAndXid(8, 4);

	reset_preparation();
	variables.nextXid = first;
	reservation = true;
	floor_full = U64FromFullTransactionId(candidate);
	limit_full = floor_full + 1024;
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), floor_full);
	UT_ASSERT_EQ(prepare_calls, 1);
	UT_ASSERT_EQ(prepared_first, U64FromFullTransactionId(first));
	UT_ASSERT_EQ(prepared_last, U64FromFullTransactionId(candidate));
	UT_ASSERT_EQ(checked_first, prepared_first);
	UT_ASSERT_EQ(checked_last, prepared_last);
	UT_ASSERT_EQ(lock_errors, 0);
}

static void
expect_preparation_error(int expected_code)
{
	int caught;

	allocation_error_expected = true;
	caught = sigsetjmp(allocation_error_env, 1);
	if (caught == 0)
		(void) GetNewTransactionId(false);
	allocation_error_expected = false;
	UT_ASSERT_EQ(caught, 1);
	UT_ASSERT_EQ(allocation_errors, 1);
	UT_ASSERT_EQ(allocation_error_code, expected_code);
	UT_ASSERT_EQ(prepare_calls, 1);
	UT_ASSERT(!held);
	UT_ASSERT_EQ(waits, 0);
	UT_ASSERT_EQ(extensions, 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), 2052);
	UT_ASSERT_EQ(entries[0].xid, InvalidTransactionId);
	UT_ASSERT_EQ(visible_xids[0], InvalidTransactionId);
}

UT_TEST(registration_refusal_cannot_acquire_a_lease_or_issue)
{
	reset_preparation();
	preparation_refused = true;
	expect_preparation_error(ERRCODE_INSUFFICIENT_RESOURCES);
	UT_ASSERT(!preparation_effect);
}

UT_TEST(post_registration_error_retains_duty_without_retry)
{
	reset_preparation();
	preparation_error = true;
	expect_preparation_error(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
	UT_ASSERT(preparation_effect);
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(cold_preparation_precedes_original_lease_and_storage);
	UT_RUN(preparation_reentry_rederives_the_original_counter);
	UT_RUN(lease_retry_rechecks_the_entire_new_herding_gap);
	UT_RUN(complete_warm_range_has_no_control_preparation);
	UT_RUN(preparation_covers_the_complete_multi_segment_gap);
	UT_RUN(current_segment_without_successors_is_not_a_warm_range);
	UT_RUN(wrap_prediction_keeps_both_original_full_identities);
	UT_RUN(registration_refusal_cannot_acquire_a_lease_or_issue);
	UT_RUN(post_registration_error_retains_duty_without_retry);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
