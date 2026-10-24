/*-------------------------------------------------------------------------
 *
 * test_cluster_pcm_stop_cv.c
 *    PCM shutdown through the native condition-variable wait loop.
 *
 * Reuse the real PCM object and its runtime fixture, replacing only the
 * fixture's returning CV stubs with condition_variable.c. Latch delivery,
 * time and the separate producer are controlled; no real process is signaled.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_pcm_stop_cv.c
 *
 *-------------------------------------------------------------------------
 */

#define PCM_NATIVE_CV_TEST
int pcm_fixture_main(void);
#define main pcm_fixture_main
#include "test_cluster_pcm_lock.c"
#undef main

#include "portability/instr_time.h"
#include "storage/proclist.h"

static int64 native_now_ns;

/* Only the clock boundary is replaced; retain native timeout accounting,
 * wait-list rechecks and CHECK_FOR_INTERRUPTS inside the actual CV loop. */
#undef INSTR_TIME_SET_CURRENT
#define INSTR_TIME_SET_CURRENT(t) ((t).ticks = native_now_ns)
#include "../../backend/storage/lmgr/condition_variable.c"

typedef enum NativeWaitAction {
	NATIVE_SHUTDOWN_LATCH,
	NATIVE_CONTENTION,
	NATIVE_PRODUCER,
	NATIVE_SPURIOUS
} NativeWaitAction;

static PROC_HDR native_proc_header;
static PGPROC native_procs[2];
Latch *MyLatch;
static BufferTag native_tag;
static NativeWaitAction native_action;
static unsigned native_wait_calls;
static unsigned native_timeout_waits;
static bool native_external_release;
static long native_first_timeout;

/* Broadcast as another backend: the prepared CV registration is local to
 * the waiting process, while its PGPROC link remains in the shared list. */
static void
native_release_writer_barrier(void)
{
	PGPROC *saved_proc = MyProc;
	ConditionVariable *saved_target = cv_sleep_target;
	int saved_node = cluster_node_id;

	MyProc = &native_procs[1];
	cv_sleep_target = NULL;
	cluster_node_id = 3;
	UT_ASSERT(cluster_pcm_lock_clear_queue_pending_x_exact(native_tag, 3, 9012));
	UT_ASSERT_NULL(cv_sleep_target);
	cluster_node_id = saved_node;
	cv_sleep_target = saved_target;
	MyProc = saved_proc;
}

int
WaitLatch(Latch *latch, int wake_events, long timeout, uint32 wait_event_info)
{
	native_wait_calls++;
	UT_ASSERT(latch == MyLatch);
	UT_ASSERT_EQ(wait_event_info, WAIT_EVENT_PCM_COMPATIBLE_STATE_WAIT);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT_NOT_NULL(cv_sleep_target);
	UT_ASSERT(proclist_contains(&cv_sleep_target->wakeup, MyProc->pgprocno, cvWaitLink));
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(native_tag), UINT32_C(1));
	UT_ASSERT_EQ(cluster_pcm_lock_query_pending_x_requester(native_tag), 3);
	if (native_wait_calls == 1)
		native_first_timeout = timeout;
	if ((wake_events & WL_TIMEOUT) != 0) {
		native_timeout_waits++;
		UT_ASSERT(timeout > 0 && timeout <= 100);
	}
	if (native_action == NATIVE_SHUTDOWN_LATCH && native_wait_calls == 1) {
		/* SIGTERM sets the latch, not the CV's producer wakeup link. */
		ProcDiePending = true;
		InterruptPending = true;
		native_now_ns += NS_PER_MS;
		return WL_LATCH_SET;
	}
	if (native_action == NATIVE_SPURIOUS && native_wait_calls == 1) {
		native_now_ns += NS_PER_MS;
		return WL_LATCH_SET;
	}
	if ((native_action == NATIVE_SHUTDOWN_LATCH
		 || (native_action == NATIVE_CONTENTION && native_wait_calls <= 4))
		&& (wake_events & WL_TIMEOUT) != 0) {
		native_now_ns += (timeout + 1) * NS_PER_MS;
		return WL_TIMEOUT;
	}
	/* Bound the broken parent's indefinite wait with an explicit producer
	 * wake. This is test containment, never a simulated cancellation. */
	if (native_action == NATIVE_SHUTDOWN_LATCH)
		native_external_release = true;
	native_release_writer_barrier();
	return WL_LATCH_SET;
}

void
ResetLatch(Latch *latch)
{
	UT_ASSERT(latch == MyLatch);
}

static void
native_wait_setup(NativeWaitAction action)
{
	UT_ASSERT_NULL(cv_sleep_target);
	memset(&native_proc_header, 0, sizeof(native_proc_header));
	memset(native_procs, 0, sizeof(native_procs));
	native_proc_header.allProcs = native_procs;
	native_proc_header.allProcCount = lengthof(native_procs);
	ProcGlobal = &native_proc_header;
	native_procs[0].pgprocno = 0;
	native_procs[1].pgprocno = 1;
	MyProc = &native_procs[0];
	MyLatch = &MyProc->procLatch;
	reset_fake_pcm_runtime(4);
	native_tag = make_tag(9100);
	native_action = action;
	native_now_ns = NS_PER_S;
	native_wait_calls = 0;
	native_timeout_waits = 0;
	native_first_timeout = 0;
	native_external_release = false;
	ProcDiePending = false;
	InterruptPending = false;
	InterruptHoldoffCount = 1; /* An outer content lock, outside the PCM entry lock. */
	CritSectionCount = 0;
	MyBackendType = B_BACKEND;
	cluster_shared_config = true;
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(native_tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(native_tag, 3, 9012),
				 PCM_PENDING_X_RESERVE_OK);
	cluster_node_id = 1;
}

static bool
native_wait_acquire(void)
{
	volatile bool caught = false;
	struct StopPcmEntryLayout *entry;

	fake_error_use_pg_exception_stack = true;
	PG_TRY();
	{
		cluster_pcm_lock_acquire(native_tag, PCM_LOCK_MODE_S);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	fake_error_use_pg_exception_stack = false;
	UT_ASSERT_NULL(cv_sleep_target);
	UT_ASSERT_EQ(MyProc->cvWaitLink.next, 0);
	UT_ASSERT_EQ(MyProc->cvWaitLink.prev, 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	entry = hash_search((HTAB *)&fake_pcm_htab_token, &native_tag, HASH_FIND, NULL);
	UT_ASSERT_NOT_NULL(entry);
	if (entry != NULL) {
		UT_ASSERT_EQ(pg_atomic_read_u32(&entry->pin_count), 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&entry->wait_refcount), 0);
	}
	if (caught)
		UT_ASSERT_EQ(fake_last_sqlerrcode, ERRCODE_ADMIN_SHUTDOWN);
	return caught;
}

UT_TEST(test_shutdown_before_native_cv_unwinds_exact_entry)
{
	native_wait_setup(NATIVE_SHUTDOWN_LATCH);
	ProcDiePending = true;
	UT_ASSERT(native_wait_acquire());
	UT_ASSERT_EQ(native_wait_calls, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_query_pending_x_requester(native_tag), 3);
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(native_tag), UINT32_C(1));
	UT_ASSERT(ProcDiePending && InterruptHoldoffCount == 1 && CritSectionCount == 0);
}

UT_TEST(test_shutdown_latch_unwinds_without_holder_wakeup)
{
	native_wait_setup(NATIVE_SHUTDOWN_LATCH);
	UT_ASSERT(native_wait_acquire());
	UT_ASSERT_EQ(native_wait_calls, 2);
	UT_ASSERT(!native_external_release);
	UT_ASSERT_EQ(native_timeout_waits, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_query_pending_x_requester(native_tag), 3);
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(native_tag), UINT32_C(1));
	UT_ASSERT(ProcDiePending && InterruptPending);
	UT_ASSERT_EQ(InterruptHoldoffCount, 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
}

UT_TEST(test_contention_slices_do_not_fail_or_publish_s)
{
	native_wait_setup(NATIVE_CONTENTION);
	UT_ASSERT(!native_wait_acquire());
	UT_ASSERT_EQ(native_wait_calls, 5);
	UT_ASSERT_EQ(native_timeout_waits, 5);
	UT_ASSERT_EQ(cluster_pcm_lock_query_pending_x_requester(native_tag), -1);
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(native_tag), UINT32_C(3));
	UT_ASSERT(!ProcDiePending);
}

UT_TEST(test_native_producer_wake_is_immediate)
{
	native_wait_setup(NATIVE_PRODUCER);
	UT_ASSERT(!native_wait_acquire());
	UT_ASSERT_EQ(native_wait_calls, 1);
	UT_ASSERT_EQ(native_now_ns, NS_PER_S);
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(native_tag), UINT32_C(3));
}

UT_TEST(test_spurious_latch_does_not_grant_ownership)
{
	native_wait_setup(NATIVE_SPURIOUS);
	UT_ASSERT(!native_wait_acquire());
	UT_ASSERT_EQ(native_wait_calls, 2);
	UT_ASSERT_EQ(native_now_ns, NS_PER_S + NS_PER_MS);
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(native_tag), UINT32_C(3));
}

static void
native_wait_excluded(BackendType backend, uint32 holdoff, uint32 critical)
{
	native_wait_setup(NATIVE_PRODUCER);
	MyBackendType = backend;
	InterruptHoldoffCount = holdoff;
	CritSectionCount = critical;
	UT_ASSERT(!native_wait_acquire());
	UT_ASSERT_EQ(native_wait_calls, 1);
	UT_ASSERT_EQ(native_timeout_waits, 0);
	UT_ASSERT_EQ(native_first_timeout, -1);
	UT_ASSERT_EQ(InterruptHoldoffCount, holdoff);
	UT_ASSERT_EQ(CritSectionCount, critical);
}

UT_TEST(test_background_retains_native_indefinite_wait)
{
	native_wait_excluded(B_LMS, 1, 0);
}

UT_TEST(test_zero_holdoff_retains_native_indefinite_wait)
{
	native_wait_excluded(B_BACKEND, 0, 0);
}

UT_TEST(test_critical_section_retains_native_indefinite_wait)
{
	native_wait_excluded(B_BACKEND, 1, 1);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_shutdown_before_native_cv_unwinds_exact_entry);
	UT_RUN(test_shutdown_latch_unwinds_without_holder_wakeup);
	UT_RUN(test_contention_slices_do_not_fail_or_publish_s);
	UT_RUN(test_native_producer_wake_is_immediate);
	UT_RUN(test_spurious_latch_does_not_grant_ownership);
	UT_RUN(test_background_retains_native_indefinite_wait);
	UT_RUN(test_zero_holdoff_retains_native_indefinite_wait);
	UT_RUN(test_critical_section_retains_native_indefinite_wait);
	UT_DONE();
	return ut_failed_count != 0;
}
