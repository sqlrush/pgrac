/*-------------------------------------------------------------------------
 *
 * test_cluster_clog_startup.c
 *    Execute native CLOG startup/trim without discarding retained status.
 *
 * SLRU and locks are in-memory boundaries, not physical durability evidence.
 *
 * Portions Copyright (c) 2026, PGRAC contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_clog_startup.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <setjmp.h>
#include "cluster/cluster_guc.h"
#include "../../backend/access/transam/clog.c"

#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_shared_config;
bool InRecovery;
BackendType MyBackendType;
static VariableCacheData variables;
VariableCache ShmemVariableCache = &variables;
static LWLockPadded locks[128];
LWLockPadded *MainLWLockArray = locks;
static SlruSharedData shared;
static char page[BLCKSZ];
static char *buffers[] = { page };
static bool dirty[1];
static unsigned read_calls;
static int locked;
static bool expect_error, read_error;
static jmp_buf error_boundary;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "unexpected assertion %s (%s:%d)\n", condition, file, line);
	abort();
}
bool
errstart(int level, const char *domain pg_attribute_unused())
{
	if (level >= ERROR && expect_error)
		longjmp(error_boundary, 1);
	abort();
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *func pg_attribute_unused())
{
	abort();
}
int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errcode(int code pg_attribute_unused())
{
	return 0;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == XactSLRULock && mode == LW_EXCLUSIVE);
	UT_ASSERT_EQ(locked, 0);
	locked++;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == XactSLRULock);
	UT_ASSERT_EQ(locked, 1);
	locked--;
}
int
SimpleLruReadPage(SlruCtl ctl, int pageno, bool write_ok, TransactionId xid)
{
	UT_ASSERT(ctl == XactCtl && locked == 1 && !write_ok);
	UT_ASSERT_EQ(xid, XidFromFullTransactionId(variables.nextXid));
	UT_ASSERT_EQ(pageno, TransactionIdToPage(xid));
	read_calls++;
	if (read_error)
		(void)errstart(ERROR, NULL);
	return 0;
}

static void
reset_fixture(TransactionId next, bool clustered)
{
	memset(&shared, 0, sizeof(shared));
	memset(&variables, 0, sizeof(variables));
	memset(page, 0, sizeof(page));
	XactCtl->shared = &shared;
	shared.page_buffer = buffers;
	shared.page_dirty = dirty;
	dirty[0] = false;
	variables.nextXid = FullTransactionIdFromU64(next);
	cluster_shared_config = clustered;
	InRecovery = false;
	MyBackendType = B_STARTUP;
	read_calls = 0;
	locked = 0;
	expect_error = read_error = false;
}

static bool
trim_refuses(void)
{
	expect_error = true;
	if (setjmp(error_boundary) == 0) {
		TrimCLOG();
		expect_error = false;
		return false;
	}
	expect_error = false;
	return true;
}

UT_TEST(shared_trim_preserves_each_partial_byte_without_dirtying)
{
	for (unsigned entry = 0; entry < CLOG_XACTS_PER_BYTE; entry++) {
		char before[BLCKSZ];
		TransactionId next = 3 * CLOG_XACTS_PER_PAGE + 100 + entry;
		reset_fixture(next, true);
		memset(page, 0x55, TransactionIdToByte(next));
		page[TransactionIdToByte(next)] = (1 << (entry * CLOG_BITS_PER_XACT)) - 1;
		memcpy(before, page, sizeof(before));
		StartupCLOG();
		TrimCLOG();
		UT_ASSERT_EQ(shared.latest_page_number, 3);
		UT_ASSERT_EQ(read_calls, 1);
		UT_ASSERT_EQ(locked, 0);
		UT_ASSERT(!dirty[0]);
		UT_ASSERT(memcmp(before, page, sizeof(before)) == 0);
	}
}

UT_TEST(shared_trim_refuses_status_at_or_beyond_next_xid)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		char before[BLCKSZ];
		TransactionId next = 101;
		reset_fixture(next, true);
		page[0] = 0x55;
		if (fault == 0)
			page[TransactionIdToByte(next)] = 1 << CLOG_BITS_PER_XACT;
		else
			page[fault == 1 ? TransactionIdToByte(next) + 1 : BLCKSZ - 1] = 1;
		memcpy(before, page, sizeof(before));
		UT_ASSERT(trim_refuses());
		UT_ASSERT_EQ(locked, 0);
		UT_ASSERT_EQ(read_calls, 1);
		UT_ASSERT(!dirty[0]);
		UT_ASSERT(memcmp(before, page, sizeof(before)) == 0);
	}
}

UT_TEST(ordinary_trim_keeps_native_suffix_reset)
{
	reset_fixture(101, false);
	memset(page, 0xff, sizeof(page));
	TrimCLOG();
	UT_ASSERT_EQ((unsigned char)page[0], 0xff);
	UT_ASSERT_EQ(page[TransactionIdToByte(101)], 3);
	UT_ASSERT_EQ(page[TransactionIdToByte(101) + 1], 0);
	UT_ASSERT_EQ(page[BLCKSZ - 1], 0);
	UT_ASSERT(dirty[0]);
	UT_ASSERT_EQ(locked, 0);
}

UT_TEST(shared_boundary_is_no_io_and_no_mutation)
{
	char before[BLCKSZ];
	reset_fixture(2 * CLOG_XACTS_PER_PAGE, true);
	memset(page, 0x55, sizeof(page));
	memcpy(before, page, sizeof(before));
	StartupCLOG();
	TrimCLOG();
	UT_ASSERT_EQ(shared.latest_page_number, 2);
	UT_ASSERT_EQ(read_calls, 0);
	UT_ASSERT_EQ(locked, 0);
	UT_ASSERT(!dirty[0]);
	UT_ASSERT(memcmp(before, page, sizeof(before)) == 0);
}

UT_TEST(shared_wrong_cut_refuses_before_read_or_mutation)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		reset_fixture(101, true);
		if (fault == 0)
			InRecovery = true;
		else if (fault == 1)
			MyBackendType = B_BACKEND;
		else
			variables.nextXid = FullTransactionIdFromU64(InvalidTransactionId);
		UT_ASSERT(trim_refuses());
		UT_ASSERT_EQ(read_calls, 0);
		UT_ASSERT_EQ(locked, 0);
		UT_ASSERT(!dirty[0]);
	}
}

UT_TEST(shared_read_failure_is_not_unused_status)
{
	char before[BLCKSZ];
	reset_fixture(101, true);
	page[0] = 0x55;
	memcpy(before, page, sizeof(before));
	read_error = true;
	UT_ASSERT(trim_refuses());
	UT_ASSERT_EQ(read_calls, 1);
	UT_ASSERT(!dirty[0]);
	UT_ASSERT(memcmp(before, page, sizeof(before)) == 0);
	/* Actual process error cleanup is outside this memory boundary. */
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(shared_trim_preserves_each_partial_byte_without_dirtying);
	UT_RUN(shared_trim_refuses_status_at_or_beyond_next_xid);
	UT_RUN(ordinary_trim_keeps_native_suffix_reset);
	UT_RUN(shared_boundary_is_no_io_and_no_mutation);
	UT_RUN(shared_wrong_cut_refuses_before_read_or_mutation);
	UT_RUN(shared_read_failure_is_not_unused_status);
	UT_DONE();
	return ut_failed_count != 0;
}
