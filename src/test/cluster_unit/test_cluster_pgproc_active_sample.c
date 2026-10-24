/*-------------------------------------------------------------------------
 * test_cluster_pgproc_active_sample.c
 *    Execute the native FullXid active sampler and next-XID reader.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_pgproc_active_sample.c
 *
 * Product bodies and private array type are extracted verbatim. Only shared
 * memory, recovery state and LWLock scheduling are boundary inputs.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "miscadmin.h"
#include "access/xlog.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "test_cluster_pgproc_active_types.inc"

#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

static VariableCacheData variables;
VariableCache ShmemVariableCache = &variables;
static PROC_HDR procs;
PROC_HDR *ProcGlobal = &procs;
static char array_storage[offsetof(ProcArrayStruct, pgprocnos) + 8 * sizeof(int)]
	pg_attribute_aligned(MAXIMUM_ALIGNOF);
#define array (*(ProcArrayStruct *) array_storage)
static ProcArrayStruct *procArray = &array;
static TransactionId visible_xids[8];
static PGPROC entries[8];
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static unsigned lock_phase, lock_errors;
static bool held, recovering, allocate_after_n;
static FullTransactionId future_override;
static bool future_unavailable;
static ClusterXidStripeSampleFence sample_fence;
static uint64 lease_incarnation;
static bool change_lease_after_n;
static bool lifecycle_mode;
static XidCacheStatus subxid_states[8];
static uint8 status_flags[8];
PGPROC *MyProc;
BackendId MyBackendId;
bool IsPostmasterEnvironment;
AuxProcType MyAuxProcType;
static GlobalTransaction MyLockedGxact;
static TransactionId completed_xid;

static void
MaintainLatestCompletedXid(TransactionId latestXid)
{
	completed_xid = latestXid;
}

bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	return lifecycle_mode && held && mode == LW_EXCLUSIVE &&
		(lock == ProcArrayLock || lock == TwoPhaseStateLock);
}

bool
cluster_xid_stripe_sample_future(FullTransactionId reference, FullTransactionId *future,
								 ClusterXidStripeSampleFence *fence)
{
	if (!held || lock_phase != 1)
		lock_errors++;
	*future = future_unavailable						  ? InvalidFullTransactionId
			  : FullTransactionIdIsValid(future_override) ? future_override
														  : reference;
	MemSet(fence, 0, sizeof(*fence));
	if (!future_unavailable)
		fence->incarnation = lease_incarnation;
	return !future_unavailable;
}

bool
RecoveryInProgress(void)
{
	return recovering;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (lifecycle_mode)
	{
		if (held || mode != LW_EXCLUSIVE || (lock != ProcArrayLock && lock != TwoPhaseStateLock))
			lock_errors++;
		held = true;
		return true;
	}
	if (held || mode != LW_SHARED
		|| (lock_phase == 0 ? lock != XidGenLock : lock_phase != 2 || lock != ProcArrayLock))
		lock_errors++;
	held = true;
	lock_phase++;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	if (lifecycle_mode)
	{
		if (!held || (lock != ProcArrayLock && lock != TwoPhaseStateLock))
			lock_errors++;
		held = false;
		return;
	}
	if (!held || (lock_phase == 1 ? lock != XidGenLock : lock_phase != 3 || lock != ProcArrayLock))
		lock_errors++;
	held = false;
	lock_phase++;
	if (lock == XidGenLock && change_lease_after_n)
		lease_incarnation++;
	if (lock == XidGenLock && allocate_after_n) {
		/* A later allocation is visible at the ProcArray cut, but N stays fixed. */
		visible_xids[0] = 101;
		pg_atomic_write_u64(&entries[0].cluster_top_full_xid_atomic,
							(UINT64CONST(7) << 32) + 101);
		variables.nextXid = FullTransactionIdFromEpochAndXid(7, 102);
	}
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

#include "test_cluster_pgproc_active_sample.inc"

static void
publish_top(int index, FullTransactionId full)
{
	visible_xids[index] = XidFromFullTransactionId(full);
	pg_atomic_write_u64(&entries[array.pgprocnos[index]].cluster_top_full_xid_atomic,
						U64FromFullTransactionId(full));
}

static void
reset_fixture(void)
{
	MemSet(&variables, 0, sizeof(variables));
	MemSet(&procs, 0, sizeof(procs));
	MemSet(array_storage, 0, sizeof(array_storage));
	MemSet(visible_xids, 0, sizeof(visible_xids));
	MemSet(entries, 0, sizeof(entries));
	MemSet(subxid_states, 0, sizeof(subxid_states));
	MemSet(status_flags, 0, sizeof(status_flags));
	ShmemVariableCache = &variables;
	ProcGlobal = &procs;
	procArray = &array;
	procs.xids = visible_xids;
	procs.allProcs = entries;
	procs.subxidStates = subxid_states;
	procs.statusFlags = status_flags;
	MyProc = &entries[7];
	MyBackendId = 19;
	MyAuxProcType = NotAnAuxProcess;
	MyLockedGxact = NULL;
	IsPostmasterEnvironment = lifecycle_mode = false;
	completed_xid = InvalidTransactionId;
	array.maxProcs = lengthof(visible_xids);
	for (int i = 0; i < lengthof(visible_xids); i++)
		array.pgprocnos[i] = i;
	variables.nextXid = FullTransactionIdFromEpochAndXid(7, 100);
	lock_phase = lock_errors = 0;
	held = recovering = allocate_after_n = future_unavailable = false;
	future_override = InvalidFullTransactionId;
	MemSet(&sample_fence, 0, sizeof(sample_fence));
	lease_incarnation = 31;
	change_lease_after_n = false;
}

UT_TEST(test_empty_array_retains_one_captured_n)
{
	FullTransactionId n, future, minimum;

	reset_fixture();
	UT_ASSERT(ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &sample_fence));
	UT_ASSERT_EQ(U64FromFullTransactionId(n), (UINT64CONST(7) << 32) + 100);
	UT_ASSERT_EQ(U64FromFullTransactionId(minimum), U64FromFullTransactionId(n));
	UT_ASSERT_EQ(lock_phase, 4);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_prepared_vacuum_and_other_database_top_xids_are_included)
{
	for (int oldest = 0; oldest < 3; oldest++) {
		FullTransactionId n, future, minimum;

		reset_fixture();
		array.numProcs = 4;
		/* Native compact xids include pid=0 prepared dummies, VACUUM and
		 * backends in other databases; none may be filtered out. */
		entries[0].pid = 0;
		entries[1].statusFlags = PROC_IN_VACUUM;
		entries[2].databaseId = 999;
		for (int i = 0; i < 3; i++)
			publish_top(i, FullTransactionIdFromEpochAndXid(7, i == oldest ? 50 : 90));
		UT_ASSERT(ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &sample_fence));
		UT_ASSERT_EQ(U64FromFullTransactionId(minimum), (UINT64CONST(7) << 32) + 50);
		UT_ASSERT_EQ(lock_phase, 4);
		UT_ASSERT_EQ(lock_errors, 0);
		UT_ASSERT(!held);
	}
}

UT_TEST(test_previous_epoch_and_post_n_allocation_use_same_anchor)
{
	FullTransactionId n, future, minimum;

	reset_fixture();
	array.numProcs = 2;
	publish_top(0, FullTransactionIdFromEpochAndXid(6, UINT32_MAX - 20));
	publish_top(1, FullTransactionIdFromEpochAndXid(7, 70));
	UT_ASSERT(ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &sample_fence));
	UT_ASSERT_EQ(U64FromFullTransactionId(minimum), (UINT64CONST(6) << 32) + UINT32_MAX - 20);
	reset_fixture();
	array.numProcs = 1;
	allocate_after_n = true;
	UT_ASSERT(ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &sample_fence));
	UT_ASSERT_EQ(U64FromFullTransactionId(n), (UINT64CONST(7) << 32) + 100);
	UT_ASSERT_EQ(U64FromFullTransactionId(minimum), U64FromFullTransactionId(n));
	UT_ASSERT_EQ(XidFromFullTransactionId(variables.nextXid), 102);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_unprovable_xid_clears_whole_sample_and_releases_locks)
{
	for (int fault = 0; fault < 5; fault++) {
		FullTransactionId n = FullTransactionIdFromU64(1000);
		FullTransactionId future = n, minimum = n;

		reset_fixture();
		array.numProcs = 2;
		visible_xids[0] = 90; /* Never expose a partial earlier minimum. */
		visible_xids[1] = fault == 0 ? BootstrapTransactionId : FrozenTransactionId;
		if (fault == 2)
			visible_xids[1] = 100 + UINT32_C(0x80000000);
		if (fault == 3) {
			variables.nextXid = FullTransactionIdFromU64(100);
			visible_xids[1] = UINT32_MAX;
		}
		if (fault == 4) {
			variables.nextXid = FullTransactionIdFromU64(UINT64_MAX - 10);
			visible_xids[0] = UINT32_MAX - 20;
			visible_xids[1] = 100;
		}
		UT_ASSERT(!ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &sample_fence));
		UT_ASSERT(!FullTransactionIdIsValid(n));
		UT_ASSERT(!FullTransactionIdIsValid(future));
		UT_ASSERT(!FullTransactionIdIsValid(minimum));
		UT_ASSERT_EQ(lock_phase, 4);
		UT_ASSERT_EQ(lock_errors, 0);
		UT_ASSERT(!held);
	}
}

UT_TEST(test_invalid_reference_or_array_never_returns_a_sample)
{
	for (int fault = 0; fault < 8; fault++) {
		FullTransactionId n = FullTransactionIdFromU64(1000);
		FullTransactionId future = n, minimum = n;

		reset_fixture();
		if (fault < 3)
			variables.nextXid = FullTransactionIdFromEpochAndXid(7, fault);
		if (fault == 3)
			array.numProcs = -1;
		if (fault == 4)
			array.numProcs = array.maxProcs + 1;
		if (fault == 5)
			procArray = NULL;
		if (fault == 6)
			ProcGlobal = NULL;
		if (fault == 7)
			procs.xids = NULL;
		UT_ASSERT(!ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &sample_fence));
		UT_ASSERT(!FullTransactionIdIsValid(n));
		UT_ASSERT(!FullTransactionIdIsValid(future));
		UT_ASSERT(!FullTransactionIdIsValid(minimum));
		UT_ASSERT_EQ(lock_errors, 0);
		UT_ASSERT(!held);
	}
}

UT_TEST(test_recovery_and_missing_outputs_do_not_sample)
{
	FullTransactionId n = FullTransactionIdFromU64(1000);
	FullTransactionId future = n, minimum = n;

	reset_fixture();
	recovering = true;
	UT_ASSERT(!ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &sample_fence));
	UT_ASSERT(!FullTransactionIdIsValid(n));
	UT_ASSERT(!FullTransactionIdIsValid(future));
	UT_ASSERT(!FullTransactionIdIsValid(minimum));
	UT_ASSERT_EQ(lock_phase, 0);
	recovering = false;
	UT_ASSERT(!ProcArrayGetActiveFullXidSample(NULL, &future, &minimum, &sample_fence));
	UT_ASSERT(!ProcArrayGetActiveFullXidSample(&n, &future, NULL, &sample_fence));
	UT_ASSERT(!ProcArrayGetActiveFullXidSample(&n, NULL, &minimum, &sample_fence));
	UT_ASSERT(!ProcArrayGetActiveFullXidSample(&n, &n, &minimum, &sample_fence));
	UT_ASSERT(!ProcArrayGetActiveFullXidSample(&n, &future, &future, &sample_fence));
	UT_ASSERT(!ProcArrayGetActiveFullXidSample(&n, &future, &n, &sample_fence));
	UT_ASSERT_EQ(lock_phase, 0);
}

UT_TEST(test_durable_future_advances_idle_minimum_but_retains_old_top)
{
	FullTransactionId n, future, minimum;

	reset_fixture();
	future_override = FullTransactionIdFromEpochAndXid(7, 500);
	UT_ASSERT(ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &sample_fence));
	UT_ASSERT_EQ(XidFromFullTransactionId(n), 100);
	UT_ASSERT_EQ(XidFromFullTransactionId(minimum), 500);
	UT_ASSERT_EQ(U64FromFullTransactionId(future), U64FromFullTransactionId(future_override));
	UT_ASSERT_EQ(lock_errors, 0);
	reset_fixture();
	future_override = FullTransactionIdFromEpochAndXid(7, 500);
	array.numProcs = 1;
	publish_top(0, FullTransactionIdFromEpochAndXid(7, 90));
	UT_ASSERT(ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &sample_fence));
	UT_ASSERT_EQ(XidFromFullTransactionId(minimum), 90);
	UT_ASSERT_EQ(lock_errors, 0);
}

UT_TEST(test_unavailable_future_refuses_before_pgproc_scan)
{
	FullTransactionId n = FullTransactionIdFromU64(1000), future = n, minimum = n;

	reset_fixture();
	future_unavailable = true;
	UT_ASSERT(!ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &sample_fence));
	UT_ASSERT(!FullTransactionIdIsValid(n));
	UT_ASSERT(!FullTransactionIdIsValid(future));
	UT_ASSERT(!FullTransactionIdIsValid(minimum));
	UT_ASSERT_EQ(lock_phase, 2);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_lease_fence_is_from_same_n_cut_and_failure_clears_it)
{
	FullTransactionId n, future, minimum;
	ClusterXidStripeSampleFence empty = { 0 };

	reset_fixture();
	change_lease_after_n = true;
	UT_ASSERT(ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &sample_fence));
	UT_ASSERT_EQ(sample_fence.incarnation, 31);
	UT_ASSERT_EQ(lease_incarnation, 32);
	UT_ASSERT_EQ(lock_errors, 0);

	reset_fixture();
	sample_fence.incarnation = 99;
	array.numProcs = 1;
	visible_xids[0] = FrozenTransactionId;
	UT_ASSERT(!ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &sample_fence));
	UT_ASSERT_EQ(memcmp(&sample_fence, &empty, sizeof(empty)), 0);
	UT_ASSERT(!FullTransactionIdIsValid(n));
	UT_ASSERT(!FullTransactionIdIsValid(future));
	UT_ASSERT(!FullTransactionIdIsValid(minimum));
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);

	reset_fixture();
	UT_ASSERT(!ProcArrayGetActiveFullXidSample(&n, &future, &minimum, NULL));
	UT_ASSERT_EQ(lock_phase, 0);
}

UT_TEST(test_raw_only_top_xid_never_qualifies_original_fullxid)
{
	FullTransactionId n = FullTransactionIdFromU64(1000);
	FullTransactionId future = n, minimum = n;

	reset_fixture();
	array.numProcs = 1;
	entries[0].pid = 123;
	visible_xids[0] = 90;
	/* A matching raw xid around N cannot identify its original epoch. */
	UT_ASSERT(!ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &sample_fence));
	UT_ASSERT(!FullTransactionIdIsValid(n));
	UT_ASSERT(!FullTransactionIdIsValid(future));
	UT_ASSERT(!FullTransactionIdIsValid(minimum));
	UT_ASSERT_EQ(lock_phase, 4);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_original_epoch_and_compact_mapping_are_not_guessed_from_n)
{
	FullTransactionId n, future, minimum;

	reset_fixture();
	array.numProcs = 2;
	array.pgprocnos[0] = 6;
	array.pgprocnos[1] = 3;
	publish_top(0, FullTransactionIdFromEpochAndXid(5, 90));
	publish_top(1, FullTransactionIdFromEpochAndXid(7, 50));
	UT_ASSERT(ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &sample_fence));
	UT_ASSERT_EQ(U64FromFullTransactionId(minimum), (UINT64CONST(5) << 32) + 90);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_unknown_mismatched_and_sentinel_original_identity_refuse)
{
	for (int fault = 0; fault < 4; fault++)
	{
		FullTransactionId n, future, minimum;

		reset_fixture();
		array.numProcs = 2;
		publish_top(0, FullTransactionIdFromEpochAndXid(7, 40));
		publish_top(1, FullTransactionIdFromEpochAndXid(7, 90));
		if (fault == 0)
			pg_atomic_write_u64(&entries[1].cluster_top_full_xid_atomic, 0);
		if (fault == 1)
			pg_atomic_write_u64(&entries[1].cluster_top_full_xid_atomic,
								(UINT64CONST(7) << 32) + 91);
		if (fault == 2)
		{
			visible_xids[1] = UINT32_MAX;
			pg_atomic_write_u64(&entries[1].cluster_top_full_xid_atomic, UINT64_MAX);
		}
		if (fault == 3)
			ProcGlobal->allProcs = NULL;
		UT_ASSERT(!ProcArrayGetActiveFullXidSample(&n, &future, &minimum, &sample_fence));
		UT_ASSERT(!FullTransactionIdIsValid(n));
		UT_ASSERT(!FullTransactionIdIsValid(future));
		UT_ASSERT(!FullTransactionIdIsValid(minimum));
		UT_ASSERT_EQ(lock_errors, 0);
		UT_ASSERT(!held);
	}
}

UT_TEST(test_live_prepare_handoff_precedes_original_proc_clear)
{
	GlobalTransactionData gxact = { 0 };
	FullTransactionId full = FullTransactionIdFromEpochAndXid(5, 90);

	reset_fixture();
	lifecycle_mode = true;
	MyProc->lxid = 8;
	MyProc->xid = 90;
	pg_atomic_write_u64(&MyProc->cluster_top_full_xid_atomic, U64FromFullTransactionId(full));
	gxact.pgprocno = 0;
	LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);
	MarkAsPreparingGuts(&gxact, 90, "original", 123, 456, 789);
	LWLockRelease(TwoPhaseStateLock);
	UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_top_full_xid_atomic),
				 U64FromFullTransactionId(full));
	UT_ASSERT_EQ(entries[0].xid, 90);
	UT_ASSERT(MyLockedGxact == &gxact);
	MyProc->pgxactoff = 1;
	visible_xids[1] = 90;
	ProcArrayClearTransaction(MyProc);
	UT_ASSERT_EQ(pg_atomic_read_u64(&MyProc->cluster_top_full_xid_atomic), 0);
	UT_ASSERT_EQ(visible_xids[1], InvalidTransactionId);
	UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_top_full_xid_atomic),
				 U64FromFullTransactionId(full));
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_recovered_unknown_or_conflicting_prepare_does_not_reuse_fullxid)
{
	for (int fault = 0; fault < 4; fault++)
	{
		GlobalTransactionData gxact = { 0 };

		reset_fixture();
		lifecycle_mode = true;
		MyProc->xid = 90;
		pg_atomic_write_u64(&MyProc->cluster_top_full_xid_atomic,
							(UINT64CONST(5) << 32) + 90);
		/* The old dummy carrier must not survive its original memset/init. */
		pg_atomic_write_u64(&entries[0].cluster_top_full_xid_atomic,
							(UINT64CONST(4) << 32) + 90);
		if (fault == 0)
			recovering = true;
		if (fault == 1)
			pg_atomic_write_u64(&MyProc->cluster_top_full_xid_atomic, 0);
		if (fault == 2)
			pg_atomic_write_u64(&MyProc->cluster_top_full_xid_atomic,
								(UINT64CONST(5) << 32) + 91);
		if (fault == 3)
			MyProc->xid = 91;
		gxact.pgprocno = 0;
		LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);
		MarkAsPreparingGuts(&gxact, 90, "unknown", 123, 456, 789);
		LWLockRelease(TwoPhaseStateLock);
		UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_top_full_xid_atomic), 0);
		UT_ASSERT_EQ(entries[0].xid, 90);
		UT_ASSERT_EQ(lock_errors, 0);
		UT_ASSERT(!held);
	}
}

UT_TEST(test_original_end_transaction_retires_fullxid_with_raw_xid)
{
	reset_fixture();
	lifecycle_mode = true;
	MyProc->pgxactoff = 1;
	MyProc->xid = visible_xids[1] = 90;
	pg_atomic_write_u64(&MyProc->cluster_top_full_xid_atomic, (UINT64CONST(5) << 32) + 90);
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	ProcArrayEndTransactionInternal(MyProc, 91);
	LWLockRelease(ProcArrayLock);
	UT_ASSERT_EQ(pg_atomic_read_u64(&MyProc->cluster_top_full_xid_atomic), 0);
	UT_ASSERT_EQ(MyProc->xid, InvalidTransactionId);
	UT_ASSERT_EQ(visible_xids[1], InvalidTransactionId);
	UT_ASSERT_EQ(completed_xid, 91);
	UT_ASSERT_EQ(variables.xactCompletionCount, 1);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

int
main(void)
{
	UT_PLAN(15);
	UT_RUN(test_empty_array_retains_one_captured_n);
	UT_RUN(test_prepared_vacuum_and_other_database_top_xids_are_included);
	UT_RUN(test_previous_epoch_and_post_n_allocation_use_same_anchor);
	UT_RUN(test_unprovable_xid_clears_whole_sample_and_releases_locks);
	UT_RUN(test_invalid_reference_or_array_never_returns_a_sample);
	UT_RUN(test_recovery_and_missing_outputs_do_not_sample);
	UT_RUN(test_durable_future_advances_idle_minimum_but_retains_old_top);
	UT_RUN(test_unavailable_future_refuses_before_pgproc_scan);
	UT_RUN(test_lease_fence_is_from_same_n_cut_and_failure_clears_it);
	UT_RUN(test_raw_only_top_xid_never_qualifies_original_fullxid);
	UT_RUN(test_original_epoch_and_compact_mapping_are_not_guessed_from_n);
	UT_RUN(test_unknown_mismatched_and_sentinel_original_identity_refuse);
	UT_RUN(test_live_prepare_handoff_precedes_original_proc_clear);
	UT_RUN(test_recovered_unknown_or_conflicting_prepare_does_not_reuse_fullxid);
	UT_RUN(test_original_end_transaction_retires_fullxid_with_raw_xid);
	UT_DONE();
	return ut_failed_count != 0;
}
