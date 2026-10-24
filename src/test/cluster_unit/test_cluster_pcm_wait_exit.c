/*-------------------------------------------------------------------------
 *
 * test_cluster_pcm_wait_exit.c
 *    Exercise the real PCM waits across return, ERROR and proc_exit cleanup.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_pcm_wait_exit.c
 *
 * NOTES
 *    This pgrac-original test exports only the existing fixture boundaries.
 *    CV wakeups, PG exit callbacks and shared memory are boundary fixtures.
 *    FATAL runs the LIFO exit callbacks and bypasses the PG exception stack.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"

int pcm_fixture_main(void);
void pcm_fixture_before_exit(pg_on_exit_callback function, Datum arg);
void pcm_fixture_cancel_exit(pg_on_exit_callback function, Datum arg);
void pcm_fixture_sleep(ConditionVariable *cv, uint32 wait_event_info);
bool pcm_fixture_timed_sleep(ConditionVariable *cv, long timeout, uint32 wait_event_info);

#define main pcm_fixture_main
#define before_shmem_exit pcm_fixture_before_exit
#define cancel_before_shmem_exit pcm_fixture_cancel_exit
#define ConditionVariableSleep pcm_fixture_sleep
#define ConditionVariableTimedSleep pcm_fixture_timed_sleep
#include "test_cluster_pcm_lock.c"
#undef main
#undef before_shmem_exit
#undef cancel_before_shmem_exit
#undef ConditionVariableSleep
#undef ConditionVariableTimedSleep

static struct {
	pg_on_exit_callback function;
	Datum arg;
} exit_callbacks[20];
static unsigned exit_callback_count;
static unsigned persistent_callback_count;
static enum { WAIT_RETURN, WAIT_ERROR, WAIT_FATAL } wait_exit;
static sigjmp_buf fatal_return;
static bool exit_boundary_seen;
static struct StopPcmEntryLayout *wait_entry;
static struct StopPcmEntryLayout expected_entry;
static PcmEntryRef other_owner_pin;
static int cancel_count_at_exit;

void
before_shmem_exit(pg_on_exit_callback function, Datum arg)
{
	UT_ASSERT(exit_callback_count < lengthof(exit_callbacks));
	exit_callbacks[exit_callback_count].function = function;
	exit_callbacks[exit_callback_count++].arg = arg;
}

void
cancel_before_shmem_exit(pg_on_exit_callback function, Datum arg)
{
	UT_ASSERT(exit_callback_count > 0);
	if (exit_callback_count == 0)
		return;
	UT_ASSERT(exit_callbacks[exit_callback_count - 1].function == function);
	UT_ASSERT(exit_callbacks[exit_callback_count - 1].arg == arg);
	exit_callback_count--;
}

static void
wait_exit_boundary(ConditionVariable *cv)
{
	PcmEntryAcquireResult acquired;
	long i;

	if (wait_exit == WAIT_RETURN)
		return;
	exit_boundary_seen = true;
	wait_entry = NULL;
	for (i = 0; i < fake_pcm_entry_count; i++) {
		struct StopPcmEntryLayout *entry = (void *)fake_pcm_entries.data[i];
		if (&entry->wait_cv == cv)
			wait_entry = entry;
	}
	UT_ASSERT(wait_entry != NULL);
	if (wait_entry == NULL)
		abort();
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT(fake_cv_prepared);
	UT_ASSERT_EQ(pg_atomic_read_u32(&wait_entry->pin_count), 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&wait_entry->wait_refcount), 1);
	/* Another owner's exact pin must survive the exiting waiter's cleanup. */
	UT_ASSERT(pcm_entry_ref_acquire(&wait_entry->tag, false, &other_owner_pin, &acquired));
	expected_entry = *wait_entry;
	pg_atomic_write_u32(&expected_entry.pin_count, 1);
	pg_atomic_write_u32(&expected_entry.wait_refcount, 0);
	cancel_count_at_exit = fake_cv_cancel_count;
	if (wait_exit == WAIT_ERROR)
		pg_re_throw();
	/* proc_exit does not unwind PG_TRY/PG_FINALLY. */
	while (exit_callback_count > 0) {
		unsigned slot = --exit_callback_count;
		exit_callbacks[slot].function(1, exit_callbacks[slot].arg);
	}
	siglongjmp(fatal_return, 1);
}

void
ConditionVariableSleep(ConditionVariable *cv, uint32 wait_event_info)
{
	wait_exit_boundary(cv);
	pcm_fixture_sleep(cv, wait_event_info);
}

bool
ConditionVariableTimedSleep(ConditionVariable *cv, long timeout, uint32 wait_event_info)
{
	wait_exit_boundary(cv);
	return pcm_fixture_timed_sleep(cv, timeout, wait_event_info);
}

static void
exercise_wait_exit(void (*body)(void), int mode)
{
	sigjmp_buf error_return;
	sigjmp_buf *saved_exception_stack = PG_exception_stack;
	ErrorContextCallback *saved_error_context = error_context_stack;
	volatile bool caught = false;

	wait_exit = mode;
	exit_boundary_seen = false;
	wait_entry = NULL;
	memset(&other_owner_pin, 0, sizeof(other_owner_pin));
	PG_exception_stack = &error_return;
	if (sigsetjmp(error_return, 0) == 0) {
		if (sigsetjmp(fatal_return, 0) == 0)
			body();
		else
			caught = true;
	} else
		caught = true;
	PG_exception_stack = saved_exception_stack;
	error_context_stack = saved_error_context;
	if (mode == WAIT_FATAL) {
		UT_ASSERT_EQ(exit_callback_count, 0);
		/* Resume the harness as a new process, retaining its permanent hook. */
		exit_callback_count = persistent_callback_count;
	}
	UT_ASSERT_EQ(exit_callback_count, persistent_callback_count);
	UT_ASSERT_EQ(caught, mode != WAIT_RETURN);
	UT_ASSERT_EQ(exit_boundary_seen, mode != WAIT_RETURN);
	UT_ASSERT(!fake_cv_prepared);
	if (mode != WAIT_RETURN && wait_entry != NULL) {
		UT_ASSERT_EQ(pg_atomic_read_u32(&wait_entry->pin_count), 1);
		UT_ASSERT_EQ(pg_atomic_read_u32(&wait_entry->wait_refcount), 0);
		UT_ASSERT_EQ(fake_cv_cancel_count, cancel_count_at_exit + 1);
		/* No holder, intent, round, transport or other owner's state changed. */
		UT_ASSERT_EQ(memcmp(wait_entry, &expected_entry, sizeof(expected_entry)), 0);
		pcm_entry_ref_release(&other_owner_pin);
		UT_ASSERT_EQ(pg_atomic_read_u32(&wait_entry->pin_count), 0);
	}
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
}

#define WAIT_EXIT_TESTS(name, body) \
	UT_TEST(name##_return) { exercise_wait_exit(body, WAIT_RETURN); } \
	UT_TEST(name##_error) { exercise_wait_exit(body, WAIT_ERROR); } \
	UT_TEST(name##_fatal) { exercise_wait_exit(body, WAIT_FATAL); }

WAIT_EXIT_TESTS(compatible, test_pcm_H3_incompatible_x_waits_and_wakes)
WAIT_EXIT_TESTS(bootstrap, test_resource_x_round_wait_rechecks_progress_in_finite_slices)
WAIT_EXIT_TESTS(predecessor, test_resource_x_predecessor_cv_observes_settlement_without_a_head)
WAIT_EXIT_TESTS(install, test_resource_x_target_install_wait_uses_receipt_and_fixed_deadline)

int
main(void)
{
	IsUnderPostmaster = true;
	MyBackendId = 1;
	cluster_pcm_lock_register_backend_exit_hook();
	persistent_callback_count = exit_callback_count;
	UT_PLAN(12);
	UT_RUN(compatible_return);
	UT_RUN(compatible_error);
	UT_RUN(compatible_fatal);
	UT_RUN(bootstrap_return);
	UT_RUN(bootstrap_error);
	UT_RUN(bootstrap_fatal);
	UT_RUN(predecessor_return);
	UT_RUN(predecessor_error);
	UT_RUN(predecessor_fatal);
	UT_RUN(install_return);
	UT_RUN(install_error);
	UT_RUN(install_fatal);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
