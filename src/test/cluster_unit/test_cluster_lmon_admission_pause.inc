/* Actual original LmonMain wait/duty/dispatch boundaries; no producer cut.
 * Shared atomics, PGPROC and module completion remain explicit host inputs.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
static ClusterSemanticActivationShmem test_collection_gate;
static unsigned test_collection_side, test_collection_bit, test_collection_samples;
static unsigned test_collection_kind, test_collection_fault, test_collection_active;
static unsigned test_collection_barriers;
static bool test_collection_returned, test_collection_event_debt;

static ClusterSemanticAdmissionObservationResult
test_collection_read(ClusterServiceProducerCollection *out)
{
	return cluster_service_producer_collect(3, 11, out);
}

static void
test_collection_prepare(void)
{
	memset(&test_collection_gate, 0, sizeof(test_collection_gate));
	SemanticActivationShmem = &test_collection_gate;
	pg_atomic_init_u64(&test_collection_gate.admission_seq, 2);
	pg_atomic_init_u64(&test_collection_gate.active_bits, 0);
	pg_atomic_init_u64(&test_collection_gate.record_generation, 11);
	pg_atomic_init_u64(&test_collection_gate.formation_epoch, 3);
	pg_atomic_init_u32(&test_collection_gate.transition_closed, 0);
	for (unsigned side = 0; side < 2; side++)
		for (unsigned bit = 0; bit < 64; bit++)
			pg_atomic_init_u32(&test_collection_gate.inflight[side][bit], 0);
	for (unsigned i = 0; i < 25; i++)
		test_stop_observation[i] = CLUSTER_NORMAL_STOP_READY;
	if (test_collection_kind == 0)
		pg_atomic_write_u32(&test_collection_gate.inflight[test_collection_side][test_collection_bit], 1);
	if (test_collection_kind == 2) {
		test_stop_observation[0] = CLUSTER_NORMAL_STOP_PENDING;
		test_stop_observation[24] = CLUSTER_NORMAL_STOP_INVALID;
		if (test_collection_fault == 0)
			SemanticActivationShmem = NULL;
		else
			pg_atomic_write_u64(&test_collection_gate.admission_seq, 3);
	}
	test_collection_samples = test_collection_active = 0;
	test_collection_returned = test_collection_event_debt = false;
	test_collection_barrier_hook = NULL;
}

static void
test_collection_work(bool event)
{
	ClusterServiceProducerCollection out;
	unsigned polls = test_stop_polls;
	if (test_collection_kind == 1 && event) {
		UT_ASSERT_EQ(test_lmon_wait_calls, 1);
		pg_atomic_write_u32(&test_collection_gate.inflight[test_collection_side][test_collection_bit], 1);
		test_collection_event_debt = true;
	}
	UT_ASSERT_EQ(test_collection_read(&out), test_collection_kind == 2 && test_collection_fault == 0
		? CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED
		: CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT_EQ(out.producer.actor.active_depth, 1);
	UT_ASSERT_EQ(out.producer.responsibility.modules, 0);
	UT_ASSERT_EQ(test_stop_polls, polls);
	test_collection_active++;
	/* Explicit original-owner completion in its third duty, never by read. */
	if (!event && test_lmon_wait_calls == 2 && test_collection_kind < 2) {
		pg_atomic_write_u32(&test_collection_gate.inflight[test_collection_side][test_collection_bit], 0);
		test_collection_returned = true;
	}
}

static void
test_collection_change_at_shared_cut(void)
{
	if (++test_collection_barriers != 3)
		return;
	test_collection_barrier_hook = NULL;
	if (test_collection_fault == 0)
		test_producer_proc.cluster_grd_generation++;
	else if (test_collection_fault == 4)
		UT_ASSERT(cluster_normal_stop_service_enter());
	else {
		pg_atomic_write_u64(&test_collection_gate.admission_seq, 3);
		if (test_collection_fault == 1)
			pg_atomic_write_u64(&test_collection_gate.record_generation, 12);
		if (test_collection_fault == 2)
			pg_atomic_write_u64(&test_collection_gate.formation_epoch, 4);
		if (test_collection_fault != 3)
			pg_atomic_write_u64(&test_collection_gate.admission_seq, 4);
	}
}

static int
test_collection_wait(WaitEvent *events)
{
	ClusterServiceProducerCollection out;
	ClusterCleanLeaveSharedState before = test_stop_region;
	ClusterSemanticAdmissionObservationResult result, expected;
	unsigned polls = test_stop_polls, sample = ++test_collection_samples;

	UT_ASSERT(sample <= 3);
	UT_ASSERT(!cluster_normal_stop_requested());
	test_producer_seen = 0;
	if (test_collection_kind == 3) {
		test_collection_barriers = 0;
		test_collection_barrier_hook = test_collection_change_at_shared_cut;
	}
	result = test_collection_read(&out);
	test_collection_barrier_hook = NULL;
	if (test_collection_kind < 2)
		expected = (test_collection_kind == 0 && sample < 3)
			|| (test_collection_kind == 1 && sample == 2)
			? CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING
			: CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_CURRENT;
	else if (test_collection_kind == 2)
		expected = CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED;
	else
		expected = test_collection_fault == 1 || test_collection_fault == 2
			? CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_STALE
			: CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING;
	UT_ASSERT_EQ(result, expected);
	UT_ASSERT_EQ(test_stop_polls - polls, 25);
	UT_ASSERT_EQ(test_producer_seen, (UINT32_C(1) << 25) - 1);
	UT_ASSERT_EQ(out.producer.responsibility.modules, 25);
	UT_ASSERT_EQ(out.producer.identity.pid, 4321);
	UT_ASSERT_EQ(out.producer.identity.pgprocno, 7);
	UT_ASSERT_EQ(out.producer.identity.backend_id, InvalidBackendId);
	UT_ASSERT_EQ(out.producer.identity.proc_generation, 17);
	UT_ASSERT_EQ(out.producer.identity.epoch, 3);
	if (test_collection_kind == 3 && test_collection_fault == 4) {
		UT_ASSERT_EQ(out.producer.actor.active_depth, 1);
		UT_ASSERT(cluster_normal_stop_service_leave(true));
	} else
		UT_ASSERT_EQ(out.producer.actor.active_depth, 0);
	UT_ASSERT_EQ(memcmp(&before, &test_stop_region, sizeof(before)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_idle_mask), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_seal), 0);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
	if (test_collection_kind < 2) {
		bool debt = (test_collection_kind == 0 && sample < 3)
			|| (test_collection_kind == 1 && sample == 2);
		UT_ASSERT(out.admission.known);
		UT_ASSERT_EQ(out.admission.seq, 2);
		UT_ASSERT_EQ(out.admission.record_generation, 11);
		UT_ASSERT_EQ(out.admission.formation_epoch, 3);
		UT_ASSERT_EQ(out.admission.inflight[test_collection_side][test_collection_bit], debt ? 1 : 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&test_collection_gate.inflight[test_collection_side][test_collection_bit]), debt ? 1 : 0);
		UT_ASSERT_EQ(test_collection_returned, sample == 3);
	} else if (test_collection_kind == 2) {
		UT_ASSERT(!out.admission.known);
		UT_ASSERT_EQ(test_stop_observation[0], CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_EQ(test_stop_observation[24], CLUSTER_NORMAL_STOP_INVALID);
	} else {
		UT_ASSERT_EQ(test_collection_barriers, 3);
		if (test_collection_fault != 4)
			UT_ASSERT(!out.admission.known);
	}
	if (test_collection_kind < 2 && sample < 3) {
		if (test_collection_kind == 1 && sample == 1) {
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
test_collection_run(bool transport)
{
	PGPROC *saved_proc = MyProc;
	int saved_pid = MyProcPid;
	ClusterSemanticActivationShmem *saved_gate = SemanticActivationShmem;
	unsigned saved_module = test_producer_module;
	/* Scenario 60 prepares its original module before our shared inputs. */
	test_producer_module = 0;
	test_collection_on = true;
	test_run_normal_stop_lmon(transport, 60);
	UT_ASSERT_EQ(test_stop_exit_code, 0);
	UT_ASSERT(test_collection_active > 0);
	UT_ASSERT_EQ(test_collection_samples, test_collection_kind < 2 ? 3 : 1);
	test_collection_on = false;
	test_collection_barrier_hook = NULL;
	MyProc = saved_proc;
	MyProcPid = saved_pid;
	SemanticActivationShmem = saved_gate;
	test_producer_module = saved_module;
}

UT_TEST(test_lmon_collects_shared_debt_before_original_owner_returns)
{
	test_collection_kind = 0;
	for (unsigned mode = 0; mode < 2; mode++)
		for (test_collection_side = 0; test_collection_side < 2; test_collection_side++)
			for (test_collection_bit = 0; test_collection_bit < 64; test_collection_bit++)
				test_collection_run(mode != 0);
}

UT_TEST(test_lmon_collects_dispatch_created_shared_debt_before_next_duty)
{
	test_collection_kind = 1;
	test_collection_side = 1;
	test_collection_bit = 63;
	test_collection_run(true);
	UT_ASSERT(test_collection_event_debt);
	UT_ASSERT_EQ(test_stop_events, 1);
}

UT_TEST(test_lmon_collection_preserves_original_invalid_over_unknown_shared_scope)
{
	test_collection_kind = 2;
	for (unsigned mode = 0; mode < 2; mode++)
		for (test_collection_fault = 0; test_collection_fault < 2; test_collection_fault++)
			test_collection_run(mode != 0);
}

UT_TEST(test_lmon_collection_detects_shared_scan_generation_and_late_actor_work)
{
	test_collection_kind = 3;
	for (unsigned mode = 0; mode < 2; mode++)
		for (test_collection_fault = 0; test_collection_fault < 5; test_collection_fault++)
			test_collection_run(mode != 0);
}
