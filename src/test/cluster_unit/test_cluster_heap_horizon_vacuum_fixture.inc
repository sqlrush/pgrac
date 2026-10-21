/* VACUUM I/O leaves; cutoff/age policy and FullXid widening are production.
 * The durable promise sample is a leaf double, not a durability test.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "commands/vacuum.h"
#include "postmaster/autovacuum.h"
#include "storage/lwlock.h"
#include "storage/procarray.h"
#include "utils/snapmgr.h"

int vacuum_freeze_min_age = 100;
int vacuum_freeze_table_age = 500;
int vacuum_multixact_freeze_min_age = 10;
int vacuum_multixact_freeze_table_age = 10000;
int autovacuum_freeze_max_age = 200000000;
int old_snapshot_threshold = -1;

static VariableCacheData age_cache;
VariableCache ShmemVariableCache = &age_cache;
static LWLockPadded age_locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = age_locks;
static bool age_xid_lock, age_promise_ready, age_promise_current, age_revoke_on_release;
static unsigned age_sample_reads, age_fence_reads, age_lock_acquires, age_lock_releases;
static FullTransactionId age_future;

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == XidGenLock && mode == LW_SHARED && !age_xid_lock);
	age_xid_lock = true;
	age_lock_acquires++;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == XidGenLock && age_xid_lock);
	age_xid_lock = false;
	age_lock_releases++;
	if (age_revoke_on_release)
		age_promise_current = false;
}

bool
cluster_xid_stripe_sample_future(FullTransactionId reference, FullTransactionId *future,
								 ClusterXidStripeSampleFence *fence)
{
	UT_ASSERT(age_xid_lock);
	UT_ASSERT_EQ(U64FromFullTransactionId(reference), U64FromFullTransactionId(stub_next_full));
	age_sample_reads++;
	*future = InvalidFullTransactionId;
	memset(fence, 0, sizeof(*fence));
	if (!age_promise_ready)
		return false;
	*future = age_future;
	fence->incarnation = 81;
	return true;
}

bool
cluster_xid_stripe_sample_fence_current(const ClusterXidStripeSampleFence *fence)
{
	UT_ASSERT(!age_xid_lock);
	UT_ASSERT_EQ(fence->incarnation, 81);
	age_fence_reads++;
	return age_promise_current;
}

TransactionId
GetOldestNonRemovableTransactionId(Relation rel)
{
	return 80;
}

MultiXactId
ReadNextMultiXactId(void)
{
	return 100;
}

int
MultiXactMemberFreezeThreshold(void)
{
	return 100000;
}

#include "test_cluster_heap_horizon_compare.inc"

bool
TransactionIdLimitedForOldSnapshots(TransactionId xmin, Relation rel, TransactionId *limit_xid,
									TimestampTz *limit_ts)
{
	abort();
}

void
SetOldSnapshotThresholdTimestamp(TimestampTz ts, TransactionId limit_xid)
{
	abort();
}

int
errcode(int code)
{
	return 0;
}
int
errmsg(const char *fmt, ...)
{
	return 0;
}
int
errdetail(const char *fmt, ...)
{
	return 0;
}
int
errhint(const char *fmt, ...)
{
	return 0;
}

#include "test_cluster_heap_horizon_vacuum.inc"

static bool
vacuum_age_scan(TransactionId frozen, int table_age, char persistence, MultiXactId minmulti,
				struct VacuumCutoffs *cutoffs)
{
	FormData_pg_class form = { 0 };
	RelationData rel = { 0 };
	VacuumParams params = { 0 };

	form.relpersistence = persistence;
	form.relfrozenxid = frozen;
	form.relminmxid = minmulti;
	rel.rd_rel = &form;
	params.freeze_min_age = -1;
	params.freeze_table_age = table_age;
	params.multixact_freeze_min_age = -1;
	params.multixact_freeze_table_age = -1;
	return vacuum_get_cutoffs(&rel, &params, cutoffs);
}

static void
setup_age(void)
{
	setup_horizon();
	activation_floor = 100;
	stub_next_full = FullTransactionIdFromU64(200);
	age_cache.nextXid = stub_next_full;
	age_future = FullTransactionIdFromU64(1000);
	age_xid_lock = age_revoke_on_release = false;
	age_promise_ready = age_promise_current = true;
	age_sample_reads = age_fence_reads = age_lock_acquires = age_lock_releases = 0;
	vacuum_freeze_table_age = 500;
	vacuum_multixact_freeze_table_age = 10000;
	autovacuum_freeze_max_age = 200000000;
}
