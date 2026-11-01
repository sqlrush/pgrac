/* Actual LmonMain wait/duty/dispatch boundaries. PGPROC, original module
 * results and sockets are explicit host fixtures, not a closure receipt.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
volatile uint32 CritSectionCount;
static PGPROC test_producer_proc;
static unsigned test_producer_module, test_producer_samples, test_producer_active_samples;
static uint32 test_producer_seen;
static bool test_producer_event_debt;

/* The online CLOSE owner is a distinct entry from its shutdown wrapper.
 * Propagate its explicit host result; never replace it with empty success. */
ClusterNormalStopPollResult
cluster_clean_leave_service_poll(int *peer, const char **reason)
{
	*peer = 1;
	*reason = "FIXTURE_ONLINE_CLOSE_OWNER";
	return test_stop_poll(9);
}

void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *arg)
{
	for (unsigned i = 0; i < test_stop_lock_depth; i++)
		callback(test_stop_locks[i], LW_EXCLUSIVE, arg);
}

static bool
test_producer_case(void)
{
	return test_stop_on && test_stop_case >= 60 && test_stop_case <= 63;
}

static void
test_producer_prepare(void)
{
	memset(&test_producer_proc, 0, sizeof(test_producer_proc));
	MyProcPid = test_producer_proc.pid = 4321;
	test_producer_proc.pgprocno = 7;
	test_producer_proc.backendId = InvalidBackendId;
	test_producer_proc.cluster_grd_generation = 17;
	MyProc = &test_producer_proc;
	CritSectionCount = 0;
	test_producer_samples = test_producer_active_samples = 0;
	test_producer_seen = 0;
	test_producer_event_debt = false;
	if (test_stop_case == 60)
		test_stop_observation[test_producer_module] = CLUSTER_NORMAL_STOP_PENDING;
	if (test_stop_case == 61) {
		test_stop_observation[0] = CLUSTER_NORMAL_STOP_PENDING;
		test_stop_observation[test_producer_module] = CLUSTER_NORMAL_STOP_INVALID;
	}
	if (test_collection_on)
		test_collection_prepare();
}

static void
test_producer_poll_boundary(unsigned module)
{
	UT_ASSERT(module < 25);
	UT_ASSERT((test_producer_seen & (UINT32_C(1) << module)) == 0);
	test_producer_seen |= UINT32_C(1) << module;
	if (test_stop_case == 62 && module == 24)
		test_producer_proc.cluster_grd_generation++;
}

static void
test_producer_work_boundary(bool event)
{
	ClusterServiceProducerObservation out;
	unsigned polls = test_stop_polls;
	if (test_collection_on) {
		test_collection_work(event);
		return;
	}
	UT_ASSERT_EQ(cluster_service_producer_observe(3, &out), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(out.actor.active_depth, 1);
	UT_ASSERT_EQ(out.responsibility.modules, 0);
	UT_ASSERT_EQ(test_stop_polls, polls);
	test_producer_active_samples++;
	if (test_stop_case == 63 && event) {
		UT_ASSERT_EQ(test_lmon_wait_calls, 1);
		test_producer_event_debt = true;
		test_stop_observation[test_producer_module] = CLUSTER_NORMAL_STOP_PENDING;
	}
	/* This host boundary represents the original owner completing work in
	 * its next duty. The read-only observer never changes the module result. */
	if (!event && test_lmon_wait_calls == 2 && (test_stop_case == 60 || test_stop_case == 63))
		test_stop_observation[test_producer_module] = CLUSTER_NORMAL_STOP_READY;
}

static int
test_producer_wait_boundary(WaitEvent *events)
{
	ClusterServiceProducerObservation out = { 0 };
	ClusterCleanLeaveSharedState before = test_stop_region;
	ClusterNormalStopPollResult result, expected;
	unsigned polls = test_stop_polls, sample;
	if (test_collection_on)
		return test_collection_wait(events);
	sample = ++test_producer_samples;

	UT_ASSERT(!cluster_normal_stop_requested());
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
	test_producer_seen = 0;
	result = cluster_service_producer_observe(3, &out);
	if (test_stop_case == 60)
		expected = sample < 3 ? CLUSTER_NORMAL_STOP_PENDING : CLUSTER_NORMAL_STOP_READY;
	else if (test_stop_case == 61)
		expected = CLUSTER_NORMAL_STOP_INVALID;
	else if (test_stop_case == 62)
		expected = CLUSTER_NORMAL_STOP_PENDING;
	else
		expected = sample == 2 ? CLUSTER_NORMAL_STOP_PENDING : CLUSTER_NORMAL_STOP_READY;
	UT_ASSERT_EQ(result, expected);
	UT_ASSERT_EQ(test_stop_polls - polls, 25);
	UT_ASSERT_EQ(test_producer_seen, (UINT32_C(1) << 25) - 1);
	UT_ASSERT_EQ(out.responsibility.modules, 25);
	UT_ASSERT_EQ(out.identity.pid, 4321);
	UT_ASSERT_EQ(out.identity.pgprocno, 7);
	UT_ASSERT_EQ(out.identity.backend_id, InvalidBackendId);
	UT_ASSERT_EQ(out.identity.epoch, 3);
	UT_ASSERT_EQ(out.identity.proc_generation, 17);
	UT_ASSERT_EQ(out.actor.active_depth, 0);
	UT_ASSERT_EQ(memcmp(&before, &test_stop_region, sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
	if (test_stop_case == 60 && sample < 3)
		UT_ASSERT_EQ(test_stop_observation[test_producer_module], CLUSTER_NORMAL_STOP_PENDING);
	if (test_stop_case == 62) {
		UT_ASSERT_EQ(test_producer_proc.cluster_grd_generation, 18);
		UT_ASSERT(out.reason != NULL && strcmp(out.reason, "NATIVE_PRODUCER_IDENTITY_CHANGED") == 0);
	}
	if ((test_stop_case == 60 || test_stop_case == 63) && sample < 3) {
		if (test_stop_case == 63 && sample == 1) {
			UT_ASSERT(events != NULL);
			events[0].events = WL_SOCKET_READABLE;
			events[0].fd = 42;
			events[0].user_data = (void *)(intptr_t)-1;
			return 1;
		}
		return 0;
	}
	ShutdownRequestPending = true;
	return 0;
}

static void
test_producer_run(bool transport, int scenario)
{
	PGPROC *saved_proc = MyProc;
	int saved_pid = MyProcPid;
	test_run_normal_stop_lmon(transport, scenario);
	UT_ASSERT_EQ(test_stop_exit_code, 0);
	UT_ASSERT(test_producer_active_samples > 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_idle_mask), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_seal), 0);
	MyProc = saved_proc;
	MyProcPid = saved_pid;
}

UT_TEST(test_online_lmon_collection_observes_each_original_owner_in_both_loops)
{
	for (unsigned mode = 0; mode < 2; mode++)
		for (test_producer_module = 0; test_producer_module < 25; test_producer_module++) {
			test_producer_run(mode != 0, 60);
			UT_ASSERT_EQ(test_producer_samples, 3);
			UT_ASSERT_EQ(test_stop_observation[test_producer_module], CLUSTER_NORMAL_STOP_READY);
		}
}

UT_TEST(test_online_lmon_collection_keeps_invalid_over_pending_in_both_loops)
{
	for (unsigned mode = 0; mode < 2; mode++)
		for (test_producer_module = 1; test_producer_module < 25; test_producer_module++) {
			if (test_producer_module == 20)
				continue; /* The original control-request API returns only empty/not-empty. */
			test_producer_run(mode != 0, 61);
			UT_ASSERT_EQ(test_producer_samples, 1);
			UT_ASSERT_EQ(test_stop_observation[test_producer_module], CLUSTER_NORMAL_STOP_INVALID);
		}
}

UT_TEST(test_online_lmon_collection_rechecks_actual_caller_after_module_fanout)
{
	for (unsigned mode = 0; mode < 2; mode++) {
		test_producer_run(mode != 0, 62);
		UT_ASSERT_EQ(test_producer_samples, 1);
	}
}

UT_TEST(test_online_lmon_dispatch_work_reopens_debt_before_the_next_collection)
{
	for (test_producer_module = 0; test_producer_module < 25; test_producer_module++) {
		test_producer_run(true, 63);
		UT_ASSERT_EQ(test_producer_samples, 3);
		UT_ASSERT(test_producer_event_debt);
		UT_ASSERT_EQ(test_stop_events, 1);
	}
}
