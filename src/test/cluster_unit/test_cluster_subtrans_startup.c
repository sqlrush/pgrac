/*-------------------------------------------------------------------------
 *
 * test_cluster_subtrans_startup.c
 *    Execute native SUBTRANS startup against a retained page image.
 *
 * The production body is included unchanged. SLRU and lock calls are boundary
 * fixtures, not a claim of physical SLRU durability or shared startup admission.
 *
 * Portions Copyright (c) 2026, PGRAC contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_subtrans_startup.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <setjmp.h>

#include "access/xlogutils.h"
#include "cluster/cluster_guc.h"
#include "miscadmin.h"

#include "../../backend/access/transam/subtrans.c"

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
static TransactionId page[SUBTRANS_XACTS_PER_PAGE];
static char *buffers[] = { (char *)page };
static bool dirty[1];
static unsigned zero_calls, read_calls;
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
errmsg(const char *format pg_attribute_unused(), ...)
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
	UT_ASSERT(lock == SubtransSLRULock);
	UT_ASSERT_EQ(mode, LW_EXCLUSIVE);
	UT_ASSERT_EQ(locked, 0);
	locked++;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == SubtransSLRULock);
	UT_ASSERT_EQ(locked, 1);
	locked--;
}

int
SimpleLruZeroPage(SlruCtl ctl, int pageno)
{
	UT_ASSERT(ctl == SubTransCtl && locked == 1);
	zero_calls++;
	memset(page, 0, sizeof(page));
	shared.latest_page_number = pageno;
	dirty[0] = true;
	return 0;
}

int
SimpleLruReadPage(SlruCtl ctl, int pageno, bool write_ok, TransactionId xid)
{
	UT_ASSERT(ctl == SubTransCtl && locked == 1);
	UT_ASSERT_EQ(pageno, TransactionIdToPage(XidFromFullTransactionId(variables.nextXid)));
	UT_ASSERT_EQ(xid, XidFromFullTransactionId(variables.nextXid));
	UT_ASSERT(!write_ok);
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
	SubTransCtl->shared = &shared;
	shared.page_buffer = buffers;
	shared.page_dirty = dirty;
	dirty[0] = false;
	variables.nextXid = FullTransactionIdFromU64(next);
	cluster_shared_config = clustered;
	InRecovery = false;
	MyBackendType = B_STARTUP;
	zero_calls = read_calls = 0;
	locked = 0;
	expect_error = read_error = false;
}

UT_TEST(test_shared_startup_preserves_retained_parentage)
{
	TransactionId before[SUBTRANS_XACTS_PER_PAGE];
	TransactionId next = 12 * SUBTRANS_XACTS_PER_PAGE + 100;
	reset_fixture(next, true);
	page[10] = next - 95;
	page[50] = next - 99;
	memcpy(before, page, sizeof(before));
	StartupSUBTRANS(next);
	UT_ASSERT(memcmp(before, page, sizeof(before)) == 0);
	UT_ASSERT_EQ(zero_calls, 0);
	UT_ASSERT_EQ(read_calls, 1);
	UT_ASSERT(!dirty[0]);
	UT_ASSERT_EQ(locked, 0);
	UT_ASSERT_EQ(shared.latest_page_number, 12);
}

UT_TEST(test_shared_page_boundary_does_not_allocate_or_clear)
{
	TransactionId before[SUBTRANS_XACTS_PER_PAGE];
	TransactionId next = 13 * SUBTRANS_XACTS_PER_PAGE;
	reset_fixture(next, true);
	page[10] = next - 95;
	memcpy(before, page, sizeof(before));
	StartupSUBTRANS(next);
	UT_ASSERT(memcmp(before, page, sizeof(before)) == 0);
	UT_ASSERT_EQ(zero_calls, 0);
	UT_ASSERT_EQ(read_calls, 0);
	UT_ASSERT(!dirty[0]);
	UT_ASSERT_EQ(locked, 0);
	UT_ASSERT_EQ(shared.latest_page_number, 13);
}

UT_TEST(test_ordinary_startup_keeps_native_page_reset)
{
	TransactionId next = SUBTRANS_XACTS_PER_PAGE + 100;
	reset_fixture(next, false);
	page[10] = 5;
	StartupSUBTRANS(100);
	UT_ASSERT_EQ(zero_calls, 2);
	UT_ASSERT_EQ(read_calls, 0);
	UT_ASSERT_EQ(page[10], 0);
	UT_ASSERT(dirty[0]);
	UT_ASSERT_EQ(locked, 0);
}

static bool
startup_refuses(TransactionId oldest)
{
	expect_error = true;
	if (setjmp(error_boundary) == 0) {
		StartupSUBTRANS(oldest);
		expect_error = false;
		return false;
	}
	expect_error = false;
	return true;
}

UT_TEST(test_shared_startup_refuses_wrong_cut_before_read_or_clear)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		TransactionId before[SUBTRANS_XACTS_PER_PAGE];
		TransactionId next = 100;
		reset_fixture(next, true);
		page[10] = 5;
		memcpy(before, page, sizeof(before));
		if (fault == 0)
			InRecovery = true;
		else if (fault == 1)
			MyBackendType = B_BACKEND;
		else if (fault == 2)
			variables.nextXid = FullTransactionIdFromU64(InvalidTransactionId);
		else
			next--;
		UT_ASSERT(startup_refuses(next));
		UT_ASSERT_EQ(zero_calls, 0);
		UT_ASSERT_EQ(read_calls, 0);
		UT_ASSERT_EQ(locked, 0);
		UT_ASSERT(memcmp(before, page, sizeof(before)) == 0);
	}
}

UT_TEST(test_shared_startup_never_erases_contradictory_suffix)
{
	unsigned entries[] = { 100, 150, SUBTRANS_XACTS_PER_PAGE - 1 };
	for (unsigned i = 0; i < lengthof(entries); ++i) {
		TransactionId before[SUBTRANS_XACTS_PER_PAGE];
		reset_fixture(100, true);
		page[10] = 5;
		page[entries[i]] = 75;
		memcpy(before, page, sizeof(before));
		UT_ASSERT(startup_refuses(100));
		UT_ASSERT_EQ(zero_calls, 0);
		UT_ASSERT_EQ(read_calls, 1);
		UT_ASSERT_EQ(locked, 0);
		UT_ASSERT(!dirty[0]);
		UT_ASSERT(memcmp(before, page, sizeof(before)) == 0);
	}
}

UT_TEST(test_shared_startup_read_error_is_not_fabricated_parentage)
{
	TransactionId before[SUBTRANS_XACTS_PER_PAGE];
	reset_fixture(100, true);
	page[10] = 5;
	memcpy(before, page, sizeof(before));
	read_error = true;
	UT_ASSERT(startup_refuses(100));
	UT_ASSERT_EQ(zero_calls, 0);
	UT_ASSERT_EQ(read_calls, 1);
	UT_ASSERT(!dirty[0]);
	UT_ASSERT(memcmp(before, page, sizeof(before)) == 0);
	/* Native SLRU error propagation is the boundary; process error cleanup is
	 * not implemented by this standalone memory fixture. */
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_shared_startup_preserves_retained_parentage);
	UT_RUN(test_shared_page_boundary_does_not_allocate_or_clear);
	UT_RUN(test_ordinary_startup_keeps_native_page_reset);
	UT_RUN(test_shared_startup_refuses_wrong_cut_before_read_or_clear);
	UT_RUN(test_shared_startup_never_erases_contradictory_suffix);
	UT_RUN(test_shared_startup_read_error_is_not_fabricated_parentage);
	UT_DONE();
	return ut_failed_count != 0;
}
