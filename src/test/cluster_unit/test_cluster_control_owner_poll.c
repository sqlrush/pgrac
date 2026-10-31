/*-------------------------------------------------------------------------
 * test_cluster_control_owner_poll.c -- exact guarded retirement without wait.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Use the original WALR wrappers, stable owners, registry and ACK receiver.
 * Native locks, clock, transport and S1-S5 remain explicit fixture leaves.
 * This tests the owner seam, not the future joint-job service loop.
 *-------------------------------------------------------------------------
 */
#define PGRAC_CF_JOB_LATCH_FIXTURE
int control_walr_fixture_main(void);
#define main control_walr_fixture_main
#include "test_cluster_control_walr.c"
#undef main

static PROC_HDR poll_proc_global;
static PGPROC poll_procs[43];
PROC_HDR *ProcGlobal = &poll_proc_global;
static uint64 control_driver;
static unsigned poll_wakes;

void
SetLatch(Latch *latch)
{
	UT_ASSERT(latch == MyLatch);
	poll_wakes++;
}

#include "test_cluster_cf_job_ack.inc"

static void
poll_begin(void)
{
	UT_ASSERT(cluster_control_request_empty());
	UT_ASSERT_EQ(native_refs[ShareLock], 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	cluster_shared_config = true;
	MyBackendType = B_CHECKPOINTER;
	MyProcPid = MyProc->pid = 4002;
	owner_clock_step = 100000;
	owner_ack_on_wait = false;
}

static void
poll_end(void)
{
	UT_ASSERT(cluster_control_request_empty());
	UT_ASSERT_EQ(native_refs[ShareLock], 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	MyBackendType = B_INVALID;
}

static ClusterControlRequestView
poll_view(const ClusterLockAcquireRequest *request)
{
	ClusterControlRequestView view;
	uint32 cursor = 0;

	while (cluster_control_request_next(&cursor, &view))
		if (view.message.key.holder.request_id == request->holder.request_id
			&& memcmp(&view.message.key.resid, &request->resid, sizeof(request->resid)) == 0)
			return view;
	UT_ASSERT(false);
	memset(&view, 0, sizeof(view));
	return view;
}

static ClusterControlRetireMessage
poll_claim(const ClusterLockAcquireRequest *request)
{
	ClusterControlRequestView view = poll_view(request);
	ClusterControlRequestCut cut;
	ClusterControlRetireMessage ack;

	control_cut(&cut);
	UT_ASSERT(cluster_control_request_claim(&view.handle, retire_driver, &cut, &ack));
	ack.verb = CLUSTER_CONTROL_RETIRED;
	return ack;
}

static void
poll_ack(const ClusterLockAcquireRequest *request)
{
	ClusterControlRetireMessage ack = poll_claim(request);
	unsigned wakes = poll_wakes;

	control_receive_ack(&ack, 0);
	UT_ASSERT_EQ(poll_wakes, wakes + 1);
}

/* The owner seam owns no PG-native ref. Its caller may use the original
 * native-release helper only after the seam returned exact confirmation. */
static void
poll_finish_native(const ClusterLockAcquireRequest *request)
{
	UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(request), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	walr_native_lock_release_or_fatal(request);
}

UT_TEST(owner_poll_does_not_repeat_the_original_bounded_confirmation)
{
	ClusterLockAcquireRequest request = walr_fixture_request(), original;
	unsigned waits = owner_wait_calls;

	poll_begin();
	UT_ASSERT_EQ(walr_request_acquire_actual(&request), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	original = request;
	UT_ASSERT_EQ(walr_request_release_actual(&request), CLUSTER_LOCK_ACQUIRE_PENDING);
	UT_ASSERT(owner_wait_calls > waits); /* Healthy first-release policy is unchanged. */
	waits = owner_wait_calls;
	for (unsigned i = 0; i < 4; ++i)
		UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(&request), CLUSTER_LOCK_ACQUIRE_PENDING);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(native_refs[ShareLock], 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	UT_ASSERT(!cluster_lock_owner_request_usable(&request));
	UT_ASSERT(memcmp(&request, &original, sizeof(request)) == 0);
	poll_ack(&request);
	poll_finish_native(&request);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	/* A missing/consumed original owner never becomes a successful release. */
	UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(&request), CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	poll_end();
}

UT_TEST(owner_poll_uses_the_original_request_guard_without_mutation)
{
	ClusterLockAcquireRequest request = walr_fixture_request(), copy;
	unsigned waits = owner_wait_calls;

	poll_begin();
	UT_ASSERT_EQ(walr_request_acquire_actual(&request), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(NULL), CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	for (unsigned i = 0; i < 7; ++i) {
		copy = request;
		switch (i) {
		case 0: copy.control_owner_id = 0; break;
		case 1: copy.control_owner_id++; break;
		case 2: copy.resid.field1++; break;
		case 3: copy.resid.type++; break;
		case 4: copy.lockmode = ExclusiveLock; break;
		case 5: copy.holder.node_id++; break;
		case 6: copy.holder.procno++; break;
		}
		UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(&copy), CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
		UT_ASSERT(cluster_lock_owner_request_usable(&request));
		UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	}
	cluster_shared_config = false;
	UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(&request), CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	cluster_shared_config = true;
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(&request), CLUSTER_LOCK_ACQUIRE_PENDING);
	poll_ack(&request);
	poll_finish_native(&request);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	poll_end();
}

UT_TEST(owner_poll_wrong_ack_retains_the_original_native_hold)
{
	ClusterLockAcquireRequest request = walr_fixture_request();
	ClusterControlRetireMessage ack, wrong;
	unsigned waits = owner_wait_calls, wakes;

	poll_begin();
	UT_ASSERT_EQ(walr_request_acquire_actual(&request), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(&request), CLUSTER_LOCK_ACQUIRE_PENDING);
	ack = poll_claim(&request);
	wakes = poll_wakes;
	for (unsigned i = 0; i < 6; ++i) {
		wrong = ack;
		switch (i) {
		case 0: wrong.key.holder.request_id++; break;
		case 1: wrong.key.resid.field1++; break;
		case 2: wrong.cleanup_epoch++; break;
		case 3: wrong.exchange_id++; break;
		case 4: wrong.previous_request++; break;
		case 5: wrong.previous_mode++; break;
		}
		control_receive_ack(&wrong, 0);
		UT_ASSERT_EQ(poll_wakes, wakes);
		UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(&request), CLUSTER_LOCK_ACQUIRE_PENDING);
		UT_ASSERT_EQ(native_refs[ShareLock], 1);
		UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	}
	control_receive_ack(&ack, 1); /* Wrong master is not proof. */
	UT_ASSERT_EQ(poll_wakes, wakes);
	UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(&request), CLUSTER_LOCK_ACQUIRE_PENDING);
	control_receive_ack(&ack, 0);
	UT_ASSERT_EQ(poll_wakes, wakes + 1);
	poll_finish_native(&request);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	poll_end();
}

UT_TEST(owner_poll_fresh_cut_census_keeps_the_original_guard)
{
	ClusterLockAcquireRequest request = walr_fixture_request(), original;
	uint64 enumerated;
	unsigned waits = owner_wait_calls;

	poll_begin();
	UT_ASSERT_EQ(walr_request_acquire_actual(&request), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	original = request;
	UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(&request), CLUSTER_LOCK_ACQUIRE_PENDING);
	owner_epoch++;
	owner_generation++;
	owner_routing_generation++;
	poll_ack(&request); /* Original key, original creator; ACK at the fresh cut. */
	UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT_EQ(enumerated, 1);
	UT_ASSERT_EQ(native_refs[ShareLock], 1);
	UT_ASSERT(memcmp(&request, &original, sizeof(request)) == 0);
	poll_finish_native(&request);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	poll_end();
}

UT_TEST(owner_poll_128_original_ack_events_need_no_timer_wait)
{
	ClusterLockAcquireRequest requests[CLUSTER_WAL_RETENTION_MAX_THREADS];
	unsigned waits = owner_wait_calls, wakes = poll_wakes, acquired;

	poll_begin();
	for (unsigned i = 0; i < lengthof(requests); ++i) {
		requests[i] = walr_fixture_request();
		requests[i].resid.field1 = i + 1;
		UT_ASSERT_EQ(walr_request_acquire_actual(&requests[i]), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	}
	acquired = g_seven_count;
	for (unsigned i = 0; i < lengthof(requests); ++i)
		UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(&requests[i]), CLUSTER_LOCK_ACQUIRE_PENDING);
	UT_ASSERT_EQ(native_refs[ShareLock], lengthof(requests));
	for (unsigned i = 0; i < lengthof(requests); ++i) {
		ResetLatch(MyLatch); /* Reset before observation/ACK, never after it. */
		poll_ack(&requests[i]);
		UT_ASSERT_EQ(poll_wakes, wakes + i + 1);
		UT_ASSERT_EQ(native_refs[ShareLock], lengthof(requests) - i);
		poll_finish_native(&requests[i]);
		UT_ASSERT_EQ(native_refs[ShareLock], lengthof(requests) - i - 1);
		UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count),
					 lengthof(requests) - i - 1);
		UT_ASSERT_EQ(owner_wait_calls, waits);
		UT_ASSERT_EQ(g_seven_count, acquired); /* No replacement acquisition. */
	}
	poll_end();
}

int
main(void)
{
	MyProc = &poll_procs[42];
	MyProc->pgprocno = 42;
	MyLatch = &MyProc->procLatch;
	poll_proc_global.allProcs = poll_procs;
	poll_proc_global.allProcCount = lengthof(poll_procs);
	pg_atomic_init_u32(&MyProc->cluster_grd_registered_count, 0);
	pg_atomic_init_u64(&MyProc->cluster_grd_redeclare_acked, 0);
	pg_atomic_init_u64(&MyProc->cluster_grd_redeclare_acked_epoch, 0);
	cluster_shared_config = true;
	cluster_control_request_shmem_init();
	control_driver = retire_driver = cluster_control_request_driver_start();
	UT_PLAN(5);
	UT_RUN(owner_poll_does_not_repeat_the_original_bounded_confirmation);
	UT_RUN(owner_poll_uses_the_original_request_guard_without_mutation);
	UT_RUN(owner_poll_wrong_ack_retains_the_original_native_hold);
	UT_RUN(owner_poll_fresh_cut_census_keeps_the_original_guard);
	UT_RUN(owner_poll_128_original_ack_events_need_no_timer_wait);
	UT_DONE();
	return ut_failed_count != 0;
}
