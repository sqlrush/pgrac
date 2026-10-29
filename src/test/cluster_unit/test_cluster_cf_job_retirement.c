/*-------------------------------------------------------------------------
 * test_cluster_cf_job_retirement.c -- original CF owner completion receipts.
 * Real CF/owner/registry and ACK receiver; transport, clock and latch are
 * explicit fixture boundaries. No native/network result is claimed.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#define PGRAC_CONTROL_CF_EMBEDDED
#define PGRAC_CF_JOB_LATCH_FIXTURE
#define WaitLatch cf_job_fixture_wait
#include "test_cluster_cf_enqueue.c"
#undef WaitLatch

static PROC_HDR job_proc_global;
static PGPROC job_procs[43];
PROC_HDR *ProcGlobal = &job_proc_global;
static uint64 control_driver;
static unsigned job_wakes;
static bool job_throw_on_wait;

int WaitLatch(Latch *latch, int events, long timeout, uint32 event);
int
WaitLatch(Latch *latch, int events, long timeout, uint32 event)
{
	if (job_throw_on_wait) {
		job_throw_on_wait = false;
		pg_re_throw(); /* Actual owner wait unwinds after receipt binding. */
	}
	return cf_job_fixture_wait(latch, events, timeout, event);
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	abort();
	return false;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
SetLatch(Latch *latch)
{
	UT_ASSERT(latch == MyLatch);
	job_wakes++;
}

#include "test_cluster_cf_job_ack.inc"

static void
job_begin(void)
{
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	UT_ASSERT(cluster_control_request_empty());
	cluster_shared_config = true;
	MyBackendType = B_CHECKPOINTER;
	MyProcPid = MyProc->pid = 4002;
	cluster_cf_enqueue_timeout_ms = 1000;
	owner_clock_step = 100000;
	owner_ack_on_wait = false;
	g_seven_result = g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	cf_poll_result = CLUSTER_GES_ACQUIRE_GRANTED;
	g_next_request_id++;
}

static void
job_end(void)
{
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	UT_ASSERT(cluster_control_request_empty());
	cluster_shared_config = false;
	MyBackendType = B_INVALID;
	owner_ack_on_wait = false;
	cluster_cf_enqueue_timeout_ms = 30000;
}

static void
job_ack(ClusterCfJobReceiptV1 *receipt)
{
	ClusterControlRequestCut cut;
	ClusterControlRetireMessage ack;
	unsigned wakes = job_wakes;

	control_cut(&cut);
	UT_ASSERT(cluster_control_request_claim(&receipt->original.registry.handle,
											retire_driver, &cut, &ack));
	ack.verb = CLUSTER_CONTROL_RETIRED;
	control_receive_ack(&ack, cut.master);
	UT_ASSERT_EQ(job_wakes, wakes + 1);
}

UT_TEST(job_healthy_first_release_keeps_bounded_confirmation)
{
	ClusterCfJobReceiptV1 receipt = { .job_generation = 1 };
	unsigned waits = owner_wait_calls;

	job_begin();
	UT_ASSERT_EQ(sizeof(receipt), 184);
	UT_ASSERT(cluster_cf_lock(ShareLock));
	owner_ack_on_wait = true;
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &receipt), CF_JOB_CONFIRMED);
	UT_ASSERT(owner_wait_calls > waits);
	UT_ASSERT_EQ(receipt.state, CF_JOB_RECEIPT_CONFIRMED);
	UT_ASSERT_EQ(cluster_cf_job_retirement_poll_v1(&receipt), CF_JOB_CONFIRMED);
	job_end();
}

UT_TEST(job_pending_reentry_and_poll_never_repeat_synchronous_wait)
{
	ClusterCfJobReceiptV1 receipt = { .job_generation = 2 };
	unsigned waits;

	job_begin();
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &receipt), CF_JOB_PENDING);
	waits = owner_wait_calls;
	UT_ASSERT(!cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &receipt), CF_JOB_PENDING);
	UT_ASSERT_EQ(cluster_cf_job_retirement_poll_v1(&receipt), CF_JOB_PENDING);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	job_ack(&receipt);
	UT_ASSERT_EQ(cluster_cf_job_retirement_poll_v1(&receipt), CF_JOB_CONFIRMED);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	job_end();
}

UT_TEST(job_background_completion_survives_cookie_overwrite)
{
	ClusterCfJobReceiptV1 receipt = { .job_generation = 3 };
	uint64 cookie;

	job_begin();
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &receipt), CF_JOB_PENDING);
	cookie = receipt.original.cookie;
	job_ack(&receipt);
	cluster_cf_retirement_poll();
	UT_ASSERT_EQ(receipt.state, CF_JOB_RECEIPT_CONFIRMED);
	UT_ASSERT(cluster_cf_lock(ShareLock));
	owner_ack_on_wait = true;
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT(!cluster_cf_release_completed(ShareLock, cookie));
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_cf_job_retirement_poll_v1(&receipt), CF_JOB_CONFIRMED);
	UT_ASSERT(cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	job_end();
}

UT_TEST(job_cannot_adopt_a_foreign_or_copied_receipt)
{
	static const char caller, foreign;
	ClusterCfJobReceiptV1 receipt = { .job_generation = 4 }, copy;

	job_begin();
	MyBackendType = B_LMON; /* Tagged cooperative CF is an original LMON/LMS API. */
	UT_ASSERT(cluster_cf_lock_poll_owned(ShareLock, &caller));
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, &foreign, &receipt), CF_JOB_INVALID);
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &receipt), CF_JOB_INVALID);
	UT_ASSERT(cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, &caller, &receipt), CF_JOB_PENDING);
	copy = receipt;
	UT_ASSERT_EQ(cluster_cf_job_retirement_poll_v1(&copy), CF_JOB_INVALID);
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, &caller, &copy), CF_JOB_INVALID);
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, &foreign, &receipt), CF_JOB_INVALID);
	job_ack(&receipt);
	UT_ASSERT_EQ(cluster_cf_job_retirement_poll_v1(&receipt), CF_JOB_CONFIRMED);
	job_end();
}

UT_TEST(job_reused_generation_and_corrupt_identity_keep_original_debt)
{
	ClusterCfJobReceiptV1 receipt = { .job_generation = 5 }, saved;

	job_begin();
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &receipt), CF_JOB_PENDING);
	saved = receipt;
	for (unsigned i = 0; i < 10; ++i) {
		receipt = saved;
		switch (i) {
		case 0: receipt.job_generation++; break;
		case 1: receipt.original.cookie++; break;
		case 2: receipt.original.registry.handle.generation++; break;
		case 3: receipt.original.registry.handle.slot++; break;
		case 4: receipt.original.registry.owner.incarnation++; break;
		case 5: receipt.original.registry.owner.pid++; break;
		case 6: receipt.original.registry.message.key.holder.request_id++; break;
		case 7: receipt.original.registry.message.key.resid.type++; break;
		case 8: receipt.original.registry.mode = ExclusiveLock; break;
		case 9: receipt.state = CF_JOB_RECEIPT_CONFIRMED; break;
		}
		UT_ASSERT_EQ(cluster_cf_job_retirement_poll_v1(&receipt), CF_JOB_INVALID);
		UT_ASSERT(cluster_cf_held(ShareLock));
		UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	}
	receipt = saved;
	job_ack(&receipt);
	UT_ASSERT_EQ(cluster_cf_job_retirement_poll_v1(&receipt), CF_JOB_CONFIRMED);
	job_end();
}

UT_TEST(job_wrong_ack_and_changed_cut_cannot_confirm)
{
	ClusterCfJobReceiptV1 receipt = { .job_generation = 6 };
	ClusterControlRequestCut cut;
	ClusterControlRetireMessage ack;
	unsigned wakes;

	job_begin();
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &receipt), CF_JOB_PENDING);
	control_cut(&cut);
	UT_ASSERT(cluster_control_request_claim(&receipt.original.registry.handle,
											retire_driver, &cut, &ack));
	ack.verb = CLUSTER_CONTROL_RETIRED;
	ack.exchange_id++;
	wakes = job_wakes;
	control_receive_ack(&ack, cut.master);
	UT_ASSERT_EQ(job_wakes, wakes);
	UT_ASSERT_EQ(cluster_cf_job_retirement_poll_v1(&receipt), CF_JOB_PENDING);
	owner_epoch++;
	UT_ASSERT_EQ(cluster_cf_job_retirement_poll_v1(&receipt), CF_JOB_PENDING);
	UT_ASSERT(cluster_cf_held(ShareLock));
	UT_ASSERT(!cluster_cf_held_is_usable(ShareLock));
	owner_epoch--;
	job_ack(&receipt);
	UT_ASSERT_EQ(cluster_cf_job_retirement_poll_v1(&receipt), CF_JOB_CONFIRMED);
	job_end();
}

UT_TEST(job_unbound_invalid_and_native_shapes_never_claim_completion)
{
	ClusterCfJobReceiptV1 receipt = { .job_generation = 7 };

	job_begin();
	UT_ASSERT_EQ(cluster_cf_job_retirement_poll_v1(&receipt), CF_JOB_INVALID);
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &receipt), CF_JOB_INVALID);
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(NoLock, NULL, &receipt), CF_JOB_INVALID);
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, NULL), CF_JOB_INVALID);
	UT_ASSERT(cluster_cf_lock(ShareLock));
	receipt.reserved = 1;
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &receipt), CF_JOB_INVALID);
	receipt.reserved = 0;
	receipt.job_generation = 0;
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &receipt), CF_JOB_INVALID);
	UT_ASSERT(cluster_cf_held_is_usable(ShareLock));
	owner_ack_on_wait = true;
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_NATIVE;
	UT_ASSERT(cluster_cf_lock(ShareLock));
	receipt.job_generation = 7;
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &receipt), CF_JOB_INVALID);
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_NOT_HELD);
	job_end();
}

UT_TEST(job_failed_acquire_retains_original_request_until_ack)
{
	ClusterCfJobReceiptV1 receipt = { .job_generation = 8 };

	job_begin();
	g_s5_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	UT_ASSERT(!cluster_cf_lock(ShareLock));
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &receipt), CF_JOB_PENDING);
	UT_ASSERT(!cluster_cf_held_is_usable(ShareLock));
	job_ack(&receipt);
	UT_ASSERT_EQ(cluster_cf_job_retirement_poll_v1(&receipt), CF_JOB_CONFIRMED);
	job_end();
}

UT_TEST(job_startup_s_and_x_use_the_same_receipt_seam)
{
	for (unsigned i = 0; i < 2; ++i) {
		LOCKMODE mode = i ? ExclusiveLock : ShareLock;
		ClusterCfJobReceiptV1 receipt = { .job_generation = 9 + i };

		job_begin();
		MyBackendType = B_STARTUP;
		UT_ASSERT(cluster_cf_lock(mode));
		UT_ASSERT_EQ(cluster_cf_unlock_job_v1(mode, NULL, &receipt), CF_JOB_PENDING);
		job_ack(&receipt);
		UT_ASSERT_EQ(cluster_cf_job_retirement_poll_v1(&receipt), CF_JOB_CONFIRMED);
		job_end();
	}
}

UT_TEST(job_each_of_128_ack_events_drives_completion_without_timer_polling)
{
	unsigned waits, wakes;

	job_begin();
	for (unsigned i = 0; i < 128; ++i) {
		ClusterCfJobReceiptV1 receipt = { .job_generation = 100 + i };

		UT_ASSERT(cluster_cf_lock(ShareLock));
		/* Only the existing first-release bounded wait is allowed. */
		UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &receipt), CF_JOB_PENDING);
		waits = owner_wait_calls;
		wakes = job_wakes;
		job_ack(&receipt);
		UT_ASSERT_EQ(job_wakes, wakes + 1);
		UT_ASSERT_EQ(cluster_cf_job_retirement_poll_v1(&receipt), CF_JOB_CONFIRMED);
		UT_ASSERT_EQ(owner_wait_calls, waits);
		UT_ASSERT_EQ(job_wakes, wakes + 2); /* ACK + original CF completion. */
	}
	job_end();
}

UT_TEST(job_original_owner_census_may_consume_ack_before_cf_poll)
{
	ClusterCfJobReceiptV1 receipt = { .job_generation = 300 };
	uint64 enumerated;

	job_begin();
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &receipt), CF_JOB_PENDING);
	job_ack(&receipt);
	UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT_EQ(enumerated, 1);
	UT_ASSERT(cluster_control_request_empty());
	UT_ASSERT_EQ(cluster_cf_job_retirement_poll_v1(&receipt), CF_JOB_CONFIRMED);
	job_end();
}

UT_TEST(job_error_during_original_wait_keeps_borrowed_receipt)
{
	ClusterCfJobReceiptV1 receipt = { .job_generation = 301 };
	volatile bool caught = false;
	unsigned waits;

	job_begin();
	UT_ASSERT(cluster_cf_lock(ShareLock));
	job_throw_on_wait = true;
	PG_TRY();
	{
		(void)cluster_cf_unlock_job_v1(ShareLock, NULL, &receipt);
	}
	PG_CATCH();
	{
		caught = true;
		FlushErrorState();
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(receipt.state, CF_JOB_RECEIPT_RETIRING);
	UT_ASSERT(!cluster_cf_held_is_usable(ShareLock));
	waits = owner_wait_calls;
	job_ack(&receipt);
	UT_ASSERT_EQ(cluster_cf_job_retirement_poll_v1(&receipt), CF_JOB_CONFIRMED);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	job_end();
}

UT_TEST(job_binding_an_original_failed_release_does_not_wait_again)
{
	ClusterCfJobReceiptV1 receipt = { .job_generation = 302 };
	unsigned waits;

	job_begin();
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_UNCONFIRMED);
	waits = owner_wait_calls;
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &receipt), CF_JOB_PENDING);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	job_ack(&receipt);
	UT_ASSERT_EQ(cluster_cf_job_retirement_poll_v1(&receipt), CF_JOB_CONFIRMED);
	job_end();
}

UT_TEST(job_fresh_cut_ack_retires_original_holder_without_rebinding)
{
	ClusterCfJobReceiptV1 receipt = { .job_generation = 303 };
	ClusterCfRetirementSnapshot original;

	job_begin();
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &receipt), CF_JOB_PENDING);
	original = receipt.original;
	owner_epoch++;
	owner_generation++;
	owner_routing_generation++;
	job_ack(&receipt);
	UT_ASSERT_EQ(cluster_cf_job_retirement_poll_v1(&receipt), CF_JOB_CONFIRMED);
	UT_ASSERT(memcmp(&receipt.original, &original, sizeof(original)) == 0);
	job_end();
}

int
main(void)
{
	MyProc = &job_procs[42]; /* Same procno as the original S3 boundary fixture. */
	MyProc->pgprocno = 42;
	MyLatch = &MyProc->procLatch;
	job_proc_global.allProcs = job_procs;
	job_proc_global.allProcCount = lengthof(job_procs);
	pg_atomic_init_u32(&MyProc->cluster_grd_registered_count, 0);
	pg_atomic_init_u64(&MyProc->cluster_grd_redeclare_acked, 0);
	pg_atomic_init_u64(&MyProc->cluster_grd_redeclare_acked_epoch, 0);
	cluster_shared_config = true;
	cluster_control_request_shmem_init();
	control_driver = retire_driver = cluster_control_request_driver_start();
	UT_PLAN(14);
	UT_RUN(job_healthy_first_release_keeps_bounded_confirmation);
	UT_RUN(job_pending_reentry_and_poll_never_repeat_synchronous_wait);
	UT_RUN(job_background_completion_survives_cookie_overwrite);
	UT_RUN(job_cannot_adopt_a_foreign_or_copied_receipt);
	UT_RUN(job_reused_generation_and_corrupt_identity_keep_original_debt);
	UT_RUN(job_wrong_ack_and_changed_cut_cannot_confirm);
	UT_RUN(job_unbound_invalid_and_native_shapes_never_claim_completion);
	UT_RUN(job_failed_acquire_retains_original_request_until_ack);
	UT_RUN(job_startup_s_and_x_use_the_same_receipt_seam);
	UT_RUN(job_each_of_128_ack_events_drives_completion_without_timer_polling);
	UT_RUN(job_original_owner_census_may_consume_ack_before_cf_poll);
	UT_RUN(job_error_during_original_wait_keeps_borrowed_receipt);
	UT_RUN(job_binding_an_original_failed_release_does_not_wait_again);
	UT_RUN(job_fresh_cut_ack_retires_original_holder_without_rebinding);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
