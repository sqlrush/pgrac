/*-------------------------------------------------------------------------
 * test_cluster_control_owner_custody.c -- original failed-acquire custody.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Stable owner, registry, ACK receiver and WALR native-finish bodies are
 * production C. Native locks, transport, clock and S1-S5 are fixture leaves.
 * This is the owner seam, not a job-bound pin or native service-loop test.
 *-------------------------------------------------------------------------
 */
#define PGRAC_CF_JOB_LATCH_FIXTURE
int control_walr_fixture_main(void);
#define main control_walr_fixture_main
#include "test_cluster_control_walr.c"
#undef main

/* The same assertions run against the old API for the behavioral RED. */
#ifndef PGRAC_TEST_OWNER_CUSTODY_ACQUIRE
#define PGRAC_TEST_OWNER_CUSTODY_ACQUIRE cluster_lock_owner_request_acquire_retained
#endif

static PROC_HDR custody_proc_global;
static PGPROC custody_procs[43];
PROC_HDR *ProcGlobal = &custody_proc_global;
static unsigned custody_wakes;
static uint64 control_driver;

void
SetLatch(Latch *latch)
{
	UT_ASSERT(latch == MyLatch);
	custody_wakes++;
}

#include "test_cluster_cf_job_ack.inc"

static void
custody_begin(void)
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
custody_end(void)
{
	UT_ASSERT(cluster_control_request_empty());
	UT_ASSERT_EQ(native_refs[ShareLock], 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	UT_ASSERT(!throw_before_publication && !throw_during_acquire && !throw_during_install
		&& !fail_after_publication && !drift_during_install);
	MyBackendType = B_INVALID;
}

static ClusterControlRequestView
custody_view(const ClusterLockAcquireRequest *request)
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
custody_claim(const ClusterLockAcquireRequest *request)
{
	ClusterControlRequestView view = custody_view(request);
	ClusterControlRequestCut cut;
	ClusterControlRetireMessage ack;

	control_cut(&cut);
	UT_ASSERT(cluster_control_request_claim(&view.handle, retire_driver, &cut, &ack));
	ack.verb = CLUSTER_CONTROL_RETIRED;
	return ack;
}

static void
custody_ack(const ClusterLockAcquireRequest *request)
{
	ClusterControlRetireMessage ack = custody_claim(request);
	unsigned wakes = custody_wakes;

	control_receive_ack(&ack, 0);
	UT_ASSERT_EQ(custody_wakes, wakes + 1);
}

static ClusterLockAcquireResult
custody_acquire(ClusterLockAcquireRequest *request, volatile bool *native_held)
{
	return PGRAC_TEST_OWNER_CUSTODY_ACQUIRE(request, walr_request_finish_native,
										 (void *)native_held);
}

static void
custody_confirmed_finish(const ClusterLockAcquireRequest *request, bool native_held)
{
	UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(request), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	/* Also clean up a failing legacy comparison after its already-proven
	 * exact ACK. This does not change the failed assertion into success. */
	if (native_held)
		walr_native_lock_release_or_fatal(request);
	UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(request), CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
}

UT_TEST(custody_failed_member_survives_ack_and_original_census)
{
	ClusterLockAcquireRequest request = walr_fixture_request(), original;
	volatile bool native_held = false;
	uint64 enumerated;
	unsigned waits = owner_wait_calls;

	custody_begin();
	g_seven_result = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
	fail_after_publication = true;
	UT_ASSERT_EQ(custody_acquire(&request, &native_held), CLUSTER_LOCK_ACQUIRE_NOT_AVAIL);
	fail_after_publication = false;
	original = request;
	UT_ASSERT(!native_held && !cluster_lock_owner_request_usable(&request));
	UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(&request), CLUSTER_LOCK_ACQUIRE_PENDING);
	custody_ack(&request);
	UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT_EQ(enumerated, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	UT_ASSERT(memcmp(&request, &original, sizeof(request)) == 0);
	custody_confirmed_finish(&request, false);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	custody_end();
}

UT_TEST(custody_partial_set_keeps_prior_native_and_failed_member_distinct)
{
	ClusterLockAcquireRequest first = walr_fixture_request(), failed;
	volatile bool first_native = false, failed_native = false;
	uint64 enumerated;
	unsigned waits = owner_wait_calls;

	custody_begin();
	UT_ASSERT_EQ(custody_acquire(&first, &first_native), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	failed = walr_fixture_request();
	failed.resid.field1 = 3;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
	fail_after_publication = true;
	UT_ASSERT_EQ(custody_acquire(&failed, &failed_native), CLUSTER_LOCK_ACQUIRE_NOT_AVAIL);
	fail_after_publication = false;
	UT_ASSERT(first_native && !failed_native);
	UT_ASSERT_EQ(native_refs[ShareLock], 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 2);
	UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(&first), CLUSTER_LOCK_ACQUIRE_PENDING);
	custody_ack(&failed);
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT_EQ(enumerated, 2);
	custody_confirmed_finish(&failed, false);
	UT_ASSERT_EQ(native_refs[ShareLock], 1);
	UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(&first), CLUSTER_LOCK_ACQUIRE_PENDING);
	custody_ack(&first);
	custody_confirmed_finish(&first, true);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	custody_end();
}

UT_TEST(custody_post_native_cut_failure_waits_for_exact_ack)
{
	ClusterLockAcquireRequest request = walr_fixture_request(), original;
	volatile bool native_held = false;
	uint64 enumerated;
	unsigned waits = owner_wait_calls;

	custody_begin();
	drift_during_install = true;
	UT_ASSERT_EQ(custody_acquire(&request, &native_held), CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	drift_during_install = false;
	original = request;
	UT_ASSERT(native_held && !cluster_lock_owner_request_usable(&request));
	UT_ASSERT_EQ(native_refs[ShareLock], 1);
	UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(&request), CLUSTER_LOCK_ACQUIRE_PENDING);
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT_EQ(native_refs[ShareLock], 1);
	custody_ack(&request); /* Original key/creator at the current cut. */
	UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT_EQ(enumerated, 1);
	UT_ASSERT(memcmp(&request, &original, sizeof(request)) == 0);
	UT_ASSERT_EQ(native_refs[ShareLock], 1);
	custody_confirmed_finish(&request, true);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	custody_end();
}

UT_TEST(custody_errors_publish_the_original_guard_before_rethrow)
{
	static ClusterLockAcquireRequest request;

	custody_begin();
	for (unsigned i = 0; i < 2; ++i) {
		ClusterLockAcquireRequest original;
		volatile bool native_held = false, caught = false;
		uint64 enumerated;
		unsigned waits = owner_wait_calls;

		/* The original guard survives longjmp; do not use a modified
		 * non-volatile automatic object as the ERROR custody proof. */
		request = walr_fixture_request();
		throw_during_acquire = i == 0;
		throw_during_install = i == 1;
		PG_TRY();
		{
			(void)custody_acquire(&request, &native_held);
		}
		PG_CATCH();
		{
			caught = true;
			FlushErrorState();
		}
		PG_END_TRY();
		throw_during_acquire = throw_during_install = false;
		original = request;
		UT_ASSERT(caught && request.control_owner_id != 0);
		UT_ASSERT_EQ(native_held, i == 1);
		UT_ASSERT(!cluster_lock_owner_request_usable(&request));
		UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(&request), CLUSTER_LOCK_ACQUIRE_PENDING);
		custody_ack(&request);
		UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
		UT_ASSERT_EQ(enumerated, 1);
		UT_ASSERT(memcmp(&request, &original, sizeof(request)) == 0);
		custody_confirmed_finish(&request, native_held);
		UT_ASSERT_EQ(owner_wait_calls, waits);
	}
	custody_end();
}

UT_TEST(custody_prepublication_refusal_and_error_have_no_remote_debt)
{
	static ClusterLockAcquireRequest request;

	custody_begin();
	for (unsigned i = 0; i < 2; ++i) {
		volatile bool native_held = false, caught = false;
		uint64 enumerated;

		request = walr_fixture_request();
		g_seven_result = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
		throw_before_publication = i == 1;
		PG_TRY();
		{
			UT_ASSERT_EQ(custody_acquire(&request, &native_held), CLUSTER_LOCK_ACQUIRE_NOT_AVAIL);
		}
		PG_CATCH();
		{
			caught = true;
			FlushErrorState();
		}
		PG_END_TRY();
		throw_before_publication = false;
		UT_ASSERT_EQ(caught, i == 1);
		UT_ASSERT(!native_held && cluster_control_request_empty());
		UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
		UT_ASSERT_EQ(enumerated, 1);
		custody_confirmed_finish(&request, false);
	}
	custody_end();
}

UT_TEST(custody_wrong_ack_and_guard_cannot_discharge_or_rebuild)
{
	ClusterLockAcquireRequest request = walr_fixture_request(), wrong;
	ClusterControlRetireMessage ack, bad;
	volatile bool native_held = false;
	unsigned waits = owner_wait_calls, wakes;

	custody_begin();
	g_seven_result = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
	fail_after_publication = true;
	UT_ASSERT_EQ(custody_acquire(&request, &native_held), CLUSTER_LOCK_ACQUIRE_NOT_AVAIL);
	fail_after_publication = false;
	ack = custody_claim(&request);
	wakes = custody_wakes;
	for (unsigned i = 0; i < 4; ++i) {
		bad = ack;
		switch (i) {
		case 0: bad.key.holder.request_id++; break;
		case 1: bad.key.resid.field1++; break;
		case 2: bad.cleanup_epoch++; break;
		case 3: bad.exchange_id++; break;
		}
		control_receive_ack(&bad, 0);
		UT_ASSERT_EQ(custody_wakes, wakes);
		UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(&request), CLUSTER_LOCK_ACQUIRE_PENDING);
		wrong = request;
		wrong.control_owner_id++;
		UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(&wrong), CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
		UT_ASSERT(!cluster_lock_owner_request_usable(&request));
		UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	}
	control_receive_ack(&ack, 0);
	UT_ASSERT_EQ(custody_wakes, wakes + 1);
	custody_confirmed_finish(&request, false);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	custody_end();
}

UT_TEST(custody_rejects_the_original_invalid_request_shapes)
{
	ClusterLockAcquireRequest request = walr_fixture_request(), bad;
	volatile bool native_held = false;
	PGPROC *saved;
	unsigned acquired = g_seven_count;

	custody_begin();
	UT_ASSERT_EQ(custody_acquire(NULL, &native_held), CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	for (unsigned i = 0; i < 5; ++i) {
		bad = request;
		saved = MyProc;
		switch (i) {
		case 0: cluster_shared_config = false; break;
		case 1: MyProc = NULL; break;
		case 2: bad.resid.type = 0; break;
		case 3: bad.op = CLUSTER_LOCK_OP_CONVERT; break;
		case 4: bad.control_owner_id = 1; break;
		}
		UT_ASSERT_EQ(custody_acquire(&bad, &native_held), CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
		MyProc = saved;
		cluster_shared_config = true;
	}
	UT_ASSERT_EQ(g_seven_count, acquired);
	custody_end();
}

UT_TEST(custody_legacy_api_keeps_its_original_autonomous_reaping)
{
	ClusterLockAcquireRequest request;
	volatile bool native_held = false;
	uint64 enumerated;

	custody_begin();
	for (unsigned i = 0; i < 2; ++i) {
		request = walr_fixture_request();
		g_seven_result = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
		fail_after_publication = i == 1;
		UT_ASSERT_EQ(cluster_lock_owner_request_acquire(&request, walr_request_finish_native,
			(void *)&native_held), CLUSTER_LOCK_ACQUIRE_NOT_AVAIL);
		fail_after_publication = false;
		if (i == 1)
			custody_ack(&request);
		UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
		UT_ASSERT_EQ(enumerated, 0);
		UT_ASSERT_EQ(cluster_lock_owner_request_release_poll(&request), CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	}
	custody_end();
}

int
main(void)
{
	MyProc = &custody_procs[42];
	MyProc->pgprocno = 42;
	MyLatch = &MyProc->procLatch;
	custody_proc_global.allProcs = custody_procs;
	custody_proc_global.allProcCount = lengthof(custody_procs);
	pg_atomic_init_u32(&MyProc->cluster_grd_registered_count, 0);
	pg_atomic_init_u64(&MyProc->cluster_grd_redeclare_acked, 0);
	pg_atomic_init_u64(&MyProc->cluster_grd_redeclare_acked_epoch, 0);
	cluster_shared_config = true;
	cluster_control_request_shmem_init();
	control_driver = retire_driver = cluster_control_request_driver_start();
	printf("# owner=%zu original_guard=%zu new_resident_fields=0\n",
		sizeof(ClusterLockOwner), sizeof(ClusterLockAcquireRequest));
	UT_PLAN(8);
	UT_RUN(custody_failed_member_survives_ack_and_original_census);
	UT_RUN(custody_partial_set_keeps_prior_native_and_failed_member_distinct);
	UT_RUN(custody_post_native_cut_failure_waits_for_exact_ack);
	UT_RUN(custody_errors_publish_the_original_guard_before_rethrow);
	UT_RUN(custody_prepublication_refusal_and_error_have_no_remote_debt);
	UT_RUN(custody_wrong_ack_and_guard_cannot_discharge_or_rebuild);
	UT_RUN(custody_rejects_the_original_invalid_request_shapes);
	UT_RUN(custody_legacy_api_keeps_its_original_autonomous_reaping);
	UT_DONE();
	return ut_failed_count != 0;
}
