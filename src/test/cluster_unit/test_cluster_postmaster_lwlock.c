/*-------------------------------------------------------------------------
 *
 * test_cluster_postmaster_lwlock.c
 *    Exercise the production LWLock waiting entry points and role boundary.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_postmaster_lwlock.c
 *
 * NOTES
 *    Native locks are real. Semaphore sleeps are forbidden in this fixture;
 *    the role assertion must fire before entering any waiting machinery.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include LWLOCK_SOURCE_PATH
#include <setjmp.h>
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool IsUnderPostmaster;
PGPROC *MyProc;
volatile uint32 InterruptHoldoffCount;
volatile sig_atomic_t InterruptPending;
static sigjmp_buf assertion_jump;
static bool capture_assertion;

const char *const IndividualLWLockNames[NUM_INDIVIDUAL_LWLOCKS] = { "fixture" };
PROC_HDR *ProcGlobal;
static uint32 wait_event_storage;
uint32 *my_wait_event_info = &wait_event_storage;

void
PGSemaphoreLock(PGSemaphore sema)
{
	abort();
}
void
PGSemaphoreUnlock(PGSemaphore sema)
{
	abort();
}
void
perform_spin_delay(SpinDelayStatus *status)
{
	abort();
}
void
finish_spin_delay(SpinDelayStatus *status)
{}

void
ExceptionalCondition(const char *condition, const char *filename, int line)
{
	if (capture_assertion)
		siglongjmp(assertion_jump, 1);
	abort();
}

void
ProcessInterrupts(void)
{}

#if defined(USE_PGRAC_CLUSTER) && defined(USE_ASSERT_CHECKING)
UT_TEST(test_postmaster_all_waiting_entries_refuse_before_queueing)
{
	for (int entry = 0; entry < 3; entry++) {
		LWLock lock;
		uint64 value = 0;
		uint64 observed = 0;
		bool caught = false;

		LWLockInitialize(&lock, LWTRANCHE_FIRST_USER_DEFINED);
		IsUnderPostmaster = false;
		MyProc = NULL;
		PostmasterLWLockWaitForbidden = true;
		capture_assertion = true;
		if (sigsetjmp(assertion_jump, 1) == 0) {
			if (entry == 0)
				(void)LWLockAcquire(&lock, LW_SHARED);
			else if (entry == 1)
				(void)LWLockAcquireOrWait(&lock, LW_SHARED);
			else
				(void)LWLockWaitForVar(&lock, &value, 0, &observed);
		} else
			caught = true;
		capture_assertion = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(InterruptHoldoffCount, 0);
		UT_ASSERT_EQ(num_held_lwlocks, 0);
		UT_ASSERT(LWLockConditionalAcquire(&lock, LW_EXCLUSIVE));
		LWLockRelease(&lock);
	}
	PostmasterLWLockWaitForbidden = false;
}
#endif

UT_TEST(test_initialization_and_child_do_not_inherit_postmaster_prohibition)
{
	PGPROC proc;
	LWLock lock;

	memset(&proc, 0, sizeof(proc));
	for (int child = 0; child < 2; child++) {
		IsUnderPostmaster = child;
		MyProc = child ? &proc : NULL;
#if defined(USE_PGRAC_CLUSTER) && defined(USE_ASSERT_CHECKING)
		PostmasterLWLockWaitForbidden = child;
#endif
		LWLockInitialize(&lock, LWTRANCHE_FIRST_USER_DEFINED);
		UT_ASSERT(LWLockAcquire(&lock, LW_SHARED));
		LWLockRelease(&lock);
		UT_ASSERT(LWLockAcquireOrWait(&lock, LW_EXCLUSIVE));
		LWLockRelease(&lock);
		UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	}
}

int
main(void)
{
#if defined(USE_PGRAC_CLUSTER) && defined(USE_ASSERT_CHECKING)
	UT_PLAN(2);
	UT_RUN(test_postmaster_all_waiting_entries_refuse_before_queueing);
#else
	UT_PLAN(1);
#endif
	UT_RUN(test_initialization_and_child_do_not_inherit_postmaster_prohibition);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
