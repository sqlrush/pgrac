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
#include "access/xlog.h"
#include "cluster/cluster_xid_stripe.h"
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
static ProcArrayStruct array;
static ProcArrayStruct *procArray = &array;
static TransactionId visible_xids[8];
static PGPROC entries[8];
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static unsigned lock_phase, lock_errors;
static bool held, recovering, allocate_after_n;

bool
RecoveryInProgress(void)
{
	return recovering;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
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
	if (!held || (lock_phase == 1 ? lock != XidGenLock : lock_phase != 3 || lock != ProcArrayLock))
		lock_errors++;
	held = false;
	lock_phase++;
	if (lock == XidGenLock && allocate_after_n) {
		/* A later allocation is visible at the ProcArray cut, but N stays fixed. */
		visible_xids[0] = 101;
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
reset_fixture(void)
{
	MemSet(&variables, 0, sizeof(variables));
	MemSet(&procs, 0, sizeof(procs));
	MemSet(&array, 0, sizeof(array));
	MemSet(visible_xids, 0, sizeof(visible_xids));
	MemSet(entries, 0, sizeof(entries));
	ShmemVariableCache = &variables;
	ProcGlobal = &procs;
	procArray = &array;
	procs.xids = visible_xids;
	procs.allProcs = entries;
	array.maxProcs = lengthof(visible_xids);
	variables.nextXid = FullTransactionIdFromEpochAndXid(7, 100);
	lock_phase = lock_errors = 0;
	held = recovering = allocate_after_n = false;
}

UT_TEST(test_empty_array_retains_one_captured_n)
{
	FullTransactionId n, minimum;

	reset_fixture();
	UT_ASSERT(ProcArrayGetActiveFullXidSample(&n, &minimum));
	UT_ASSERT_EQ(U64FromFullTransactionId(n), (UINT64CONST(7) << 32) + 100);
	UT_ASSERT_EQ(U64FromFullTransactionId(minimum), U64FromFullTransactionId(n));
	UT_ASSERT_EQ(lock_phase, 4);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_prepared_vacuum_and_other_database_top_xids_are_included)
{
	for (int oldest = 0; oldest < 3; oldest++) {
		FullTransactionId n, minimum;

		reset_fixture();
		array.numProcs = 4;
		/* Native compact xids include pid=0 prepared dummies, VACUUM and
		 * backends in other databases; none may be filtered out. */
		entries[0].pid = 0;
		entries[1].statusFlags = PROC_IN_VACUUM;
		entries[2].databaseId = 999;
		for (int i = 0; i < 3; i++)
			visible_xids[i] = i == oldest ? 50 : 90;
		UT_ASSERT(ProcArrayGetActiveFullXidSample(&n, &minimum));
		UT_ASSERT_EQ(U64FromFullTransactionId(minimum), (UINT64CONST(7) << 32) + 50);
		UT_ASSERT_EQ(lock_phase, 4);
		UT_ASSERT_EQ(lock_errors, 0);
		UT_ASSERT(!held);
	}
}

UT_TEST(test_previous_epoch_and_post_n_allocation_use_same_anchor)
{
	FullTransactionId n, minimum;

	reset_fixture();
	array.numProcs = 2;
	visible_xids[0] = UINT32_MAX - 20;
	visible_xids[1] = 70;
	UT_ASSERT(ProcArrayGetActiveFullXidSample(&n, &minimum));
	UT_ASSERT_EQ(U64FromFullTransactionId(minimum), (UINT64CONST(6) << 32) + UINT32_MAX - 20);
	reset_fixture();
	array.numProcs = 1;
	allocate_after_n = true;
	UT_ASSERT(ProcArrayGetActiveFullXidSample(&n, &minimum));
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
		FullTransactionId minimum = n;

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
		UT_ASSERT(!ProcArrayGetActiveFullXidSample(&n, &minimum));
		UT_ASSERT(!FullTransactionIdIsValid(n));
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
		FullTransactionId minimum = n;

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
		UT_ASSERT(!ProcArrayGetActiveFullXidSample(&n, &minimum));
		UT_ASSERT(!FullTransactionIdIsValid(n));
		UT_ASSERT(!FullTransactionIdIsValid(minimum));
		UT_ASSERT_EQ(lock_errors, 0);
		UT_ASSERT(!held);
	}
}

UT_TEST(test_recovery_and_missing_outputs_do_not_sample)
{
	FullTransactionId n = FullTransactionIdFromU64(1000);
	FullTransactionId minimum = n;

	reset_fixture();
	recovering = true;
	UT_ASSERT(!ProcArrayGetActiveFullXidSample(&n, &minimum));
	UT_ASSERT(!FullTransactionIdIsValid(n));
	UT_ASSERT(!FullTransactionIdIsValid(minimum));
	UT_ASSERT_EQ(lock_phase, 0);
	recovering = false;
	UT_ASSERT(!ProcArrayGetActiveFullXidSample(NULL, &minimum));
	UT_ASSERT(!ProcArrayGetActiveFullXidSample(&n, NULL));
	UT_ASSERT_EQ(lock_phase, 0);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_empty_array_retains_one_captured_n);
	UT_RUN(test_prepared_vacuum_and_other_database_top_xids_are_included);
	UT_RUN(test_previous_epoch_and_post_n_allocation_use_same_anchor);
	UT_RUN(test_unprovable_xid_clears_whole_sample_and_releases_locks);
	UT_RUN(test_invalid_reference_or_array_never_returns_a_sample);
	UT_RUN(test_recovery_and_missing_outputs_do_not_sample);
	UT_DONE();
	return ut_failed_count != 0;
}
