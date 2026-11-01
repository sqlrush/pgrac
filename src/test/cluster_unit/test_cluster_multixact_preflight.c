/*-------------------------------------------------------------------------
 * test_cluster_multixact_preflight.c
 *    Current MultiXact construction before native durable effects.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * The real constructor executes. Only backend ERROR and the existing unit
 * allocator/WAL/SLRU tail are replaced. This does not certify persistence.
 *-------------------------------------------------------------------------
 */
#define USE_CLUSTER_UNIT 1
#include "postgres.h"
#include <setjmp.h>
#include "../../backend/access/transam/multixact.c"
#undef printf
#undef fprintf
#include "unit_test.h"
#if defined(PREFLIGHT_EXPECT_CASSERT) != defined(USE_ASSERT_CHECKING)
#error preflight test compiled with the wrong PostgreSQL Assert configuration
#endif
UT_DEFINE_GLOBALS();

volatile uint32 CritSectionCount;
static jmp_buf error_boundary;
static bool catch_error, fail_reservation;
static int error_code, reserve_calls, materialize_calls;
static int reserved_blocks, reserved_datas;
static MultiXactMember input[256];

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	return elevel >= ERROR;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

int
errcode(int code)
{
	error_code = code;
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *function pg_attribute_unused())
{
	if (catch_error)
		longjmp(error_boundary, 1);
	abort();
}

void
XLogEnsureRecordSpace(int max_block_id, int ndatas)
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(materialize_calls, 0);
	reserve_calls++;
	reserved_blocks = max_block_id;
	reserved_datas = ndatas;
	if (fail_reservation)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("WAL workspace unavailable")));
}

MultiXactId
cluster_multixact_test_materialize_local_current(int nmembers, MultiXactMember *members)
{
	UT_ASSERT_EQ(reserve_calls, 1);
	UT_ASSERT_EQ(reserved_blocks, 0);
	UT_ASSERT_EQ(reserved_datas, 2);
	UT_ASSERT_EQ(members[0].xid, 100);
	UT_ASSERT_EQ(members[nmembers - 1].xid, 99 + nmembers);
	materialize_calls++;
	return 901;
}

static void
reset_fixture(void)
{
	catch_error = fail_reservation = false;
	error_code = reserve_calls = materialize_calls = 0;
	reserved_blocks = reserved_datas = -1;
	CritSectionCount = 0;
	memset(input, 0, sizeof(input));
	for (unsigned i = 0; i < lengthof(input); i++) {
		input[i].xid = 100 + i;
		input[i].status = MultiXactStatusForShare;
	}
}

static bool
construction_errors(int count, MultiXactMember *members)
{
	catch_error = true;
	if (setjmp(error_boundary) == 0) {
		(void)MultiXactIdCreateLocalCurrentMembers(count, members);
		catch_error = false;
		return false;
	}
	catch_error = false;
	return true;
}

UT_TEST(native_wal_overflow_is_refused_before_reading_members)
{
	reset_fixture();
	UT_ASSERT(construction_errors(XLogRecordMaxSize / sizeof(MultiXactMember), input));
	UT_ASSERT_EQ(error_code, ERRCODE_PROGRAM_LIMIT_EXCEEDED);
	UT_ASSERT_EQ(reserve_calls, 0);
	UT_ASSERT_EQ(materialize_calls, 0);
}

UT_TEST(wal_workspace_failure_cannot_reach_materialization)
{
	reset_fixture();
	fail_reservation = true;
	UT_ASSERT(construction_errors(2, input));
	UT_ASSERT_EQ(error_code, ERRCODE_OUT_OF_MEMORY);
	UT_ASSERT_EQ(reserve_calls, 1);
	UT_ASSERT_EQ(materialize_calls, 0);
}

UT_TEST(two_and_256_members_reserve_before_materialization)
{
	const int counts[] = {2, 256};
	for (unsigned i = 0; i < lengthof(counts); i++) {
		reset_fixture();
		UT_ASSERT_EQ(MultiXactIdCreateLocalCurrentMembers(counts[i], input), 901);
		UT_ASSERT_EQ(reserve_calls, 1);
		UT_ASSERT_EQ(materialize_calls, 1);
	}
}

UT_TEST(old_domain_still_refuses_257_before_any_effect)
{
	reset_fixture();
	UT_ASSERT(construction_errors(257, input));
	UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
	UT_ASSERT_EQ(reserve_calls, 0);
	UT_ASSERT_EQ(materialize_calls, 0);
}

UT_TEST(native_size_accepts_2_256_257_without_allocating)
{
	const struct { uint32 count; Size members; uint32 record; } cases[] = {
		{2, 16, 67}, {256, 2048, 2102}, {257, 2056, 2110}
	};
	reset_fixture();
	for (unsigned i = 0; i < lengthof(cases); i++) {
		Size member_bytes = 99;
		uint32 record_bytes = 99;
		UT_ASSERT(MultiXactIdCurrentMembersSize(cases[i].count, &member_bytes, &record_bytes));
		UT_ASSERT_EQ(member_bytes, cases[i].members);
		UT_ASSERT_EQ(record_bytes, cases[i].record);
	}
	UT_ASSERT_EQ(reserve_calls, 0);
	UT_ASSERT_EQ(materialize_calls, 0);
}

UT_TEST(native_size_includes_short_long_and_top_xid_headers)
{
	Size member_bytes = 0;
	uint32 record_bytes = 0;
	/* Native payload is 12 + 8*N bytes: 252 is short, 260 is long. */
	UT_ASSERT(MultiXactIdCurrentMembersSize(30, &member_bytes, &record_bytes));
	UT_ASSERT_EQ(member_bytes, 240);
	UT_ASSERT_EQ(record_bytes, 291);
	UT_ASSERT(MultiXactIdCurrentMembersSize(31, &member_bytes, &record_bytes));
	UT_ASSERT_EQ(member_bytes, 248);
	UT_ASSERT_EQ(record_bytes, 302);
}

UT_TEST(native_size_checks_full_record_before_truncation_or_allocation)
{
	Size member_bytes = 0;
	uint32 record_bytes = 0;
	/* 32 fixed + 5 top-XID + 5 long-data + 12 create + 8*N. */
	UT_ASSERT(MultiXactIdCurrentMembersSize(133693433, &member_bytes, &record_bytes));
	UT_ASSERT_EQ(member_bytes, 1069547464);
	UT_ASSERT_EQ(record_bytes, 1069547518);
	UT_ASSERT(!MultiXactIdCurrentMembersSize(133693434, &member_bytes, &record_bytes));
	UT_ASSERT_EQ(member_bytes, 0);
	UT_ASSERT_EQ(record_bytes, 0);
	UT_ASSERT(!MultiXactIdCurrentMembersSize(UINT32_MAX, &member_bytes, &record_bytes));
	UT_ASSERT_EQ(member_bytes, 0);
	UT_ASSERT_EQ(record_bytes, 0);
	UT_ASSERT(!MultiXactIdCurrentMembersSize(INT32_MAX, &member_bytes, &record_bytes));
	UT_ASSERT_EQ(member_bytes, 0);
	UT_ASSERT_EQ(record_bytes, 0);
}

UT_TEST(native_size_clears_outputs_on_invalid_input)
{
	Size member_bytes = 99;
	uint32 record_bytes = 99;
	UT_ASSERT(!MultiXactIdCurrentMembersSize(0, &member_bytes, &record_bytes));
	UT_ASSERT_EQ(member_bytes, 0);
	UT_ASSERT_EQ(record_bytes, 0);
	record_bytes = 99;
	UT_ASSERT(!MultiXactIdCurrentMembersSize(2, NULL, &record_bytes));
	UT_ASSERT_EQ(record_bytes, 0);
	member_bytes = 99;
	UT_ASSERT(!MultiXactIdCurrentMembersSize(2, &member_bytes, NULL));
	UT_ASSERT_EQ(member_bytes, 0);
}

UT_TEST(invalid_members_and_multiple_updaters_never_reserve)
{
	reset_fixture();
	input[1].xid = InvalidTransactionId;
	UT_ASSERT(construction_errors(2, input));
	UT_ASSERT_EQ(error_code, ERRCODE_DATA_CORRUPTED);
	UT_ASSERT_EQ(reserve_calls, 0);
	UT_ASSERT_EQ(materialize_calls, 0);
	reset_fixture();
	input[0].status = input[1].status = MultiXactStatusUpdate;
	UT_ASSERT(construction_errors(2, input));
	UT_ASSERT_EQ(error_code, ERRCODE_DATA_CORRUPTED);
	UT_ASSERT_EQ(reserve_calls, 0);
	UT_ASSERT_EQ(materialize_calls, 0);
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(native_wal_overflow_is_refused_before_reading_members);
	UT_RUN(wal_workspace_failure_cannot_reach_materialization);
	UT_RUN(two_and_256_members_reserve_before_materialization);
	UT_RUN(old_domain_still_refuses_257_before_any_effect);
	UT_RUN(native_size_accepts_2_256_257_without_allocating);
	UT_RUN(native_size_includes_short_long_and_top_xid_headers);
	UT_RUN(native_size_checks_full_record_before_truncation_or_allocation);
	UT_RUN(native_size_clears_outputs_on_invalid_input);
	UT_RUN(invalid_members_and_multiple_updaters_never_reserve);
	UT_DONE();
	return ut_failed_count != 0;
}
