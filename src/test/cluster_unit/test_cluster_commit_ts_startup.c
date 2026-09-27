/*-------------------------------------------------------------------------
 *
 * test_cluster_commit_ts_startup.c
 *    Native commit-ts activation must not erase retained shared input.
 *
 * Actual production bodies; memory SLRU/lock/filesystem-presence boundaries.
 * No physical fsync, native recovery or configuration admission is simulated.
 *
 * Portions Copyright (c) 2026, PGRAC contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_commit_ts_startup.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <setjmp.h>
#include "cluster/cluster_guc.h"
#include "../../backend/access/transam/commit_ts.c"
#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_shared_config;
bool InRecovery;
BackendType MyBackendType;
ProcessingMode Mode = NormalProcessing;
static VariableCacheData variables;
VariableCache ShmemVariableCache = &variables;
static LWLockPadded locks[128];
LWLockPadded *MainLWLockArray = locks;
static CommitTimestampShared ts_shared;
static SlruSharedData slru_shared;
static char page[BLCKSZ];
static char *buffers[] = { page };
static bool dirty[1];
static bool physical_exists, expect_error;
static unsigned zeroes, writes, deletes;
static int held[128];
static jmp_buf error_boundary;

void
ExceptionalCondition(const char *c, const char *f, int l)
{
	fprintf(stderr, "unexpected assertion %s (%s:%d)\n", c, f, l);
	abort();
}
bool
errstart(int level, const char *domain pg_attribute_unused())
{
	if (level < ERROR)
		return false;
	if (expect_error)
		longjmp(error_boundary, 1);
	abort();
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{
	abort();
}
int
errmsg(const char *s pg_attribute_unused(), ...)
{
	return 0;
}
int
errcode(int code pg_attribute_unused())
{
	return 0;
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode pg_attribute_unused())
{
	int i = ((LWLockPadded *)lock) - locks;
	UT_ASSERT(i >= 0 && i < 128 && held[i] == 0);
	held[i]++;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	int i = ((LWLockPadded *)lock) - locks;
	UT_ASSERT(i >= 0 && i < 128 && held[i] == 1);
	held[i]--;
}
FullTransactionId
ReadNextFullTransactionId(void)
{
	return variables.nextXid;
}
bool
SimpleLruDoesPhysicalPageExist(SlruCtl ctl, int pageno)
{
	UT_ASSERT(ctl == CommitTsCtl);
	UT_ASSERT_EQ(pageno, TransactionIdToCTsPage(XidFromFullTransactionId(variables.nextXid)));
	return physical_exists;
}
int
SimpleLruZeroPage(SlruCtl ctl, int pageno pg_attribute_unused())
{
	UT_ASSERT(ctl == CommitTsCtl);
	zeroes++;
	memset(page, 0, sizeof(page));
	dirty[0] = true;
	return 0;
}
void
SimpleLruWritePage(SlruCtl ctl, int slot)
{
	UT_ASSERT(ctl == CommitTsCtl && slot == 0);
	writes++;
	dirty[0] = false;
}
bool
SlruScanDirectory(SlruCtl ctl, SlruScanCallback cb, void *data pg_attribute_unused())
{
	UT_ASSERT(ctl == CommitTsCtl && cb == SlruScanDirCbDeleteAll);
	deletes++;
	memset(page, 0, sizeof(page));
	return false;
}
bool
SlruScanDirCbDeleteAll(SlruCtl ctl pg_attribute_unused(), char *name pg_attribute_unused(),
					   int seg pg_attribute_unused(), void *data pg_attribute_unused())
{
	abort();
}
static void
reset_fixture(bool clustered, TransactionId next)
{
	memset(&variables, 0, sizeof(variables));
	memset(&ts_shared, 0, sizeof(ts_shared));
	memset(&slru_shared, 0, sizeof(slru_shared));
	memset(held, 0, sizeof(held));
	memset(page, 0x55, sizeof(page));
	commitTsShared = &ts_shared;
	CommitTsCtl->shared = &slru_shared;
	slru_shared.page_buffer = buffers;
	slru_shared.page_dirty = dirty;
	dirty[0] = false;
	variables.nextXid = FullTransactionIdFromU64(next);
	variables.oldestCommitTsXid = 20;
	variables.newestCommitTsXid = 80;
	cluster_shared_config = clustered;
	InRecovery = false;
	MyBackendType = B_STARTUP;
	track_commit_timestamp = true;
	physical_exists = true;
	expect_error = false;
	zeroes = writes = deletes = 0;
}
static bool
activate_refuses(void)
{
	expect_error = true;
	if (setjmp(error_boundary) == 0) {
		StartupCommitTs();
		expect_error = false;
		return false;
	}
	expect_error = false;
	return true;
}
UT_TEST(shared_deactivate_preserves_history_but_disables_answers)
{
	char before[BLCKSZ];
	reset_fixture(true, 101);
	ts_shared.commitTsActive = true;
	ts_shared.xidLastCommit = 80;
	track_commit_timestamp = false;
	memcpy(before, page, BLCKSZ);
	CompleteCommitTsInitialization();
	UT_ASSERT(!ts_shared.commitTsActive);
	UT_ASSERT_EQ(ts_shared.xidLastCommit, InvalidTransactionId);
	UT_ASSERT_EQ(variables.oldestCommitTsXid, InvalidTransactionId);
	UT_ASSERT_EQ(variables.newestCommitTsXid, InvalidTransactionId);
	UT_ASSERT_EQ(memcmp(before, page, BLCKSZ), 0);
	UT_ASSERT_EQ(deletes + zeroes + writes, 0);
}
UT_TEST(shared_activate_reuses_current_page_and_bounds)
{
	char before[BLCKSZ];
	reset_fixture(true, 101);
	memcpy(before, page, BLCKSZ);
	StartupCommitTs();
	CompleteCommitTsInitialization();
	UT_ASSERT(ts_shared.commitTsActive);
	UT_ASSERT_EQ(variables.oldestCommitTsXid, 20);
	UT_ASSERT_EQ(variables.newestCommitTsXid, 80);
	UT_ASSERT_EQ(memcmp(before, page, BLCKSZ), 0);
	UT_ASSERT_EQ(deletes + zeroes + writes, 0);
}
UT_TEST(shared_missing_partial_page_is_not_reconstructed)
{
	reset_fixture(true, 101);
	physical_exists = false;
	UT_ASSERT(activate_refuses());
	UT_ASSERT(!ts_shared.commitTsActive);
	UT_ASSERT_EQ(deletes + zeroes + writes, 0);
}
UT_TEST(shared_unused_page_boundary_needs_no_startup_creation)
{
	reset_fixture(true, COMMIT_TS_XACTS_PER_PAGE);
	physical_exists = false;
	StartupCommitTs();
	UT_ASSERT(ts_shared.commitTsActive);
	UT_ASSERT_EQ(deletes + zeroes + writes, 0);
}
UT_TEST(shared_wrong_owner_cannot_activate)
{
	reset_fixture(true, 101);
	MyBackendType = B_BACKEND;
	UT_ASSERT(activate_refuses());
	UT_ASSERT(!ts_shared.commitTsActive);
	UT_ASSERT_EQ(deletes + zeroes + writes, 0);
}
UT_TEST(ordinary_missing_page_and_disable_keep_native_behavior)
{
	reset_fixture(false, 101);
	physical_exists = false;
	StartupCommitTs();
	UT_ASSERT(ts_shared.commitTsActive);
	UT_ASSERT_EQ(zeroes, 1);
	UT_ASSERT_EQ(writes, 1);
	track_commit_timestamp = false;
	CompleteCommitTsInitialization();
	UT_ASSERT(!ts_shared.commitTsActive);
	UT_ASSERT_EQ(deletes, 1);
}
int
main(void)
{
	UT_PLAN(6);
	UT_RUN(shared_deactivate_preserves_history_but_disables_answers);
	UT_RUN(shared_activate_reuses_current_page_and_bounds);
	UT_RUN(shared_missing_partial_page_is_not_reconstructed);
	UT_RUN(shared_unused_page_boundary_needs_no_startup_creation);
	UT_RUN(shared_wrong_owner_cannot_activate);
	UT_RUN(ordinary_missing_page_and_disable_keep_native_behavior);
	UT_DONE();
	return ut_failed_count != 0;
}
