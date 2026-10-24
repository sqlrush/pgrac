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
#include "access/twophase.h"
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
#include "storage/procarray.h"
#include "test_cluster_pgproc_active_types.inc"
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
static bool sampling;
static LWLock *held_lock;
static unsigned sample_lock_phase;
static char array_storage[offsetof(ProcArrayStruct, pgprocnos) + sizeof(int)]
	pg_attribute_aligned(MAXIMUM_ALIGNOF);
static ProcArrayStruct *procArray = (ProcArrayStruct *) array_storage;
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
	if (held || (!sampling && (lock != XidGenLock || mode != LW_EXCLUSIVE))
		|| (sampling && (mode != LW_SHARED
			|| (sample_lock_phase == 0 ? lock != XidGenLock
				: sample_lock_phase != 2 || lock != ProcArrayLock))))
		lock_errors++;
	held = true;
	held_lock = lock;
	if (sampling)
		sample_lock_phase++;
	return false;
}
void
LWLockRelease(LWLock *lock)
{
	FullTransactionId published = FullTransactionIdFromU64(
		pg_atomic_read_u64(&proc.cluster_top_full_xid_atomic));

	if (!held || lock != held_lock)
		lock_errors++;
	/* Observe the publication itself, not an epoch guessed from the already
	 * advanced nextXid. Tests below compare the exact allocator return too. */
	if (TransactionIdIsNormal(proc.xid) &&
		(!FullTransactionIdIsNormal(published) || XidFromFullTransactionId(published) != proc.xid))
		lock_errors++;
	if (sampling)
		sample_lock_phase++;
	held = false;
	held_lock = NULL;
}
bool
cluster_xid_stripe_sample_future(FullTransactionId reference, FullTransactionId *future,
								 ClusterXidStripeSampleFence *fence)
{
	UT_ASSERT(sampling && held && held_lock == XidGenLock);
	*future = reference;
	MemSet(fence, 0, sizeof(*fence));
	return true;
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
	memset(array_storage, 0, sizeof(array_storage));
	procArray->numProcs = procArray->maxProcs = 1;
	procArray->pgprocnos[0] = 0;
	variables.nextXid = FullTransactionIdFromU64(2052);
	variables.xidVacLimit = UINT32_MAX / 4;
	procs.xids = visible_xids;
	procs.allProcs = &proc;
	procs.subxidStates = subxids;
	held = reservation = jump = advance_while_unlocked = false;
	cluster_enabled = cluster_shared_catalog = cluster_xid_striping = true;
	waits = extensions = early_extensions = lock_errors = 0;
	floor_full = limit_full = 0;
	sampling = false;
	held_lock = NULL;
	sample_lock_phase = 0;
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
UT_TEST(original_top_fullxid_is_published_and_subxid_does_not_replace_it)
{
	FullTransactionId top, child;

	reset_allocator();
	cluster_enabled = cluster_shared_catalog = cluster_xid_striping = false;
	variables.nextXid = FullTransactionIdFromEpochAndXid(7, 2053);
	top = GetNewTransactionId(false);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_top_full_xid_atomic),
				 U64FromFullTransactionId(top));
	child = GetNewTransactionId(true);
	UT_ASSERT(FullTransactionIdFollows(child, top));
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_top_full_xid_atomic),
				 U64FromFullTransactionId(top));
	UT_ASSERT_EQ(proc.xid, XidFromFullTransactionId(top));
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(raw_wrap_preserves_the_allocated_top_epoch)
{
	FullTransactionId top, child;

	reset_allocator();
	cluster_enabled = cluster_shared_catalog = cluster_xid_striping = false;
	variables.nextXid = FullTransactionIdFromEpochAndXid(7, UINT32_MAX);
	top = GetNewTransactionId(false);
	child = GetNewTransactionId(true);
	UT_ASSERT_EQ(U64FromFullTransactionId(top), (UINT64CONST(7) << 32) + UINT32_MAX);
	UT_ASSERT_EQ(EpochFromFullTransactionId(child), 8);
	UT_ASSERT_EQ(XidFromFullTransactionId(child), FirstNormalTransactionId);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_top_full_xid_atomic),
				 U64FromFullTransactionId(top));
	UT_ASSERT_EQ(proc.xid, UINT32_MAX);
	{
		FullTransactionId n, future, minimum;
		ClusterXidStripeSampleFence fence;

		sampling = true;
		UT_ASSERT(ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &fence));
		UT_ASSERT_EQ(EpochFromFullTransactionId(n), 8);
		UT_ASSERT(FullTransactionIdEquals(minimum, top));
		UT_ASSERT_EQ(sample_lock_phase, 4);
	}
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(real_sample_keeps_original_top_when_child_cache_overflows)
{
	FullTransactionId top, n, future, minimum;
	ClusterXidStripeSampleFence fence;

	reset_allocator();
	cluster_enabled = cluster_shared_catalog = cluster_xid_striping = false;
	variables.nextXid = FullTransactionIdFromEpochAndXid(7, 2053);
	top = GetNewTransactionId(false);
	for (unsigned i = 0; i < PGPROC_MAX_CACHED_SUBXIDS + 2; i++)
		(void) GetNewTransactionId(true);
	UT_ASSERT(proc.subxidStatus.overflowed);
	UT_ASSERT_EQ(proc.subxidStatus.count, PGPROC_MAX_CACHED_SUBXIDS);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_top_full_xid_atomic),
				 U64FromFullTransactionId(top));
	sampling = true;
	UT_ASSERT(ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &fence));
	UT_ASSERT(FullTransactionIdEquals(minimum, top));
	UT_ASSERT(FullTransactionIdFollows(n, top));
	UT_ASSERT_EQ(sample_lock_phase, 4);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(real_sample_refuses_raw_publication_that_does_not_match_allocation)
{
	FullTransactionId top, n, future, minimum;
	ClusterXidStripeSampleFence fence;

	reset_allocator();
	cluster_enabled = cluster_shared_catalog = cluster_xid_striping = false;
	top = GetNewTransactionId(false);
	/* Mutate only the visible compact raw word, never the actual allocation. */
	visible_xids[0] = XidFromFullTransactionId(top) + 1;
	sampling = true;
	UT_ASSERT(!ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &fence));
	UT_ASSERT(!FullTransactionIdIsValid(n));
	UT_ASSERT(!FullTransactionIdIsValid(future));
	UT_ASSERT(!FullTransactionIdIsValid(minimum));
	UT_ASSERT_EQ(sample_lock_phase, 4);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(no_storage_or_procarray_mutation_before_reservation);
	UT_RUN(wait_rederives_native_counter_and_new_boot_floor);
	UT_RUN(native_allocation_preserves_ordinary_counter);
	UT_RUN(original_top_fullxid_is_published_and_subxid_does_not_replace_it);
	UT_RUN(raw_wrap_preserves_the_allocated_top_epoch);
	UT_RUN(real_sample_keeps_original_top_when_child_cache_overflows);
	UT_RUN(real_sample_refuses_raw_publication_that_does_not_match_allocation);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
