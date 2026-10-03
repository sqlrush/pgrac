/*-------------------------------------------------------------------------
 *
 * test_cluster_xid_allocation.c
 *    Exercise the actual native allocator across stripe reservation waits.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_xid_allocation.c
 *
 * NOTES
 *    PGRAC-original tests. Spec: spec-s9p2-06-online-membership.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_enabled, cluster_shared_catalog, cluster_xid_striping;
int cluster_node_id = 4, cluster_xid_herding_slack = 4194304;
bool IsUnderPostmaster;
ProcessingMode Mode = NormalProcessing;
static VariableCacheData variables;
VariableCache ShmemVariableCache = &variables;
static PGPROC proc;
PGPROC *MyProc = &proc;
static PROC_HDR procs;
PROC_HDR *ProcGlobal = &procs;
static TransactionId visible_xids[1];
static XidCacheStatus subxids[1];
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static bool held, reservation, jump, advance_while_unlocked;
static unsigned waits, extensions, early_extensions, lock_errors;
static uint64 floor_full, limit_full;
int cluster_injection_armed_count;

bool
IsInParallelMode(void)
{
	return false;
}
bool
RecoveryInProgress(void)
{
	return false;
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (lock != XidGenLock || held)
		lock_errors++;
	held = true;
	return false;
}
void
LWLockRelease(LWLock *lock)
{
	if (lock != XidGenLock || !held)
		lock_errors++;
	held = false;
}
FullTransactionId
ReadNextFullTransactionId(void)
{
	return variables.nextXid;
}
void
cluster_xid_stripe_lazy_latch(void)
{}
FullTransactionId
cluster_xid_stripe_my_slot_floor(void)
{
	return FullTransactionIdFromU64(1024);
}
FullTransactionId
cluster_xid_stripe_herding_floor(void)
{
	return FullTransactionIdFromU64(floor_full);
}
bool
cluster_xid_stripe_window_exceeded(FullTransactionId candidate)
{
	return false;
}
bool
cluster_xid_wrap_barrier_passed(void)
{
	return true;
}
bool
cluster_xid_stripe_lease_ready(FullTransactionId candidate)
{
	return !cluster_shared_catalog
		   || (reservation && U64FromFullTransactionId(candidate) >= floor_full
			   && U64FromFullTransactionId(candidate) < limit_full);
}
void
cluster_xid_stripe_wait_lease(FullTransactionId candidate)
{
	waits++;
	if (held)
		lock_errors++;
	reservation = true;
	floor_full = jump ? 4100 : 2052;
	limit_full = 8196;
	if (advance_while_unlocked)
		variables.nextXid = FullTransactionIdFromU64(2084);
}
static void
extended(void)
{
	extensions++;
	if (cluster_shared_catalog && !reservation)
		early_extensions++;
	if (!held)
		lock_errors++;
}
void
ExtendCLOG(TransactionId xid)
{
	extended();
}
void
ExtendCommitTs(TransactionId xid)
{
	extended();
}
void
ExtendSUBTRANS(TransactionId xid)
{
	extended();
}
void
SendPostmasterSignal(PMSignalReason reason)
{}
char *
get_database_name(Oid databaseid)
{
	abort();
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}
#undef errstart
#undef errstart_cold
bool
errstart(int level, const char *domain)
{
	abort();
}
bool
errstart_cold(int level, const char *domain)
{
	abort();
}
void
errfinish(const char *file, int line, const char *function)
{
	abort();
}

#include "test_cluster_xid_allocation.inc"

static void
reset_allocator(void)
{
	memset(&variables, 0, sizeof(variables));
	memset(&proc, 0, sizeof(proc));
	memset(&procs, 0, sizeof(procs));
	memset(subxids, 0, sizeof(subxids));
	memset(visible_xids, 0, sizeof(visible_xids));
	variables.nextXid = FullTransactionIdFromU64(2052);
	variables.xidVacLimit = UINT32_MAX / 4;
	procs.xids = visible_xids;
	procs.subxidStates = subxids;
	held = reservation = jump = advance_while_unlocked = false;
	cluster_enabled = cluster_shared_catalog = cluster_xid_striping = true;
	waits = extensions = early_extensions = lock_errors = 0;
	floor_full = limit_full = 0;
}

UT_TEST(no_storage_or_procarray_mutation_before_reservation)
{
	reset_allocator();
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), 2052);
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(early_extensions, 0);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
	UT_ASSERT_EQ(proc.xid, 2052);
	UT_ASSERT_EQ(visible_xids[0], 2052);
}
UT_TEST(wait_rederives_native_counter_and_new_boot_floor)
{
	reset_allocator();
	advance_while_unlocked = true;
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(true)), 2084);
	UT_ASSERT_EQ(subxids[0].count, 1);
	UT_ASSERT_EQ(proc.subxids.xids[0], 2084);
	UT_ASSERT_EQ(lock_errors, 0);
	reset_allocator();
	jump = true;
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), 4100);
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(early_extensions, 0);
	UT_ASSERT_EQ(lock_errors, 0);
}
UT_TEST(native_allocation_preserves_ordinary_counter)
{
	reset_allocator();
	cluster_enabled = cluster_shared_catalog = cluster_xid_striping = false;
	variables.nextXid = FullTransactionIdFromU64(2053);
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), 2053);
	UT_ASSERT_EQ(waits, 0);
	UT_ASSERT_EQ(extensions, 3);
	UT_ASSERT_EQ(lock_errors, 0);
}
int
main(void)
{
	UT_PLAN(3);
	UT_RUN(no_storage_or_procarray_mutation_before_reservation);
	UT_RUN(wait_rederives_native_counter_and_new_boot_floor);
	UT_RUN(native_allocation_preserves_ordinary_counter);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
