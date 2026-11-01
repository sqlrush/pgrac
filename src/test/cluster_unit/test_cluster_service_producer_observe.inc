/* Own native actor and original module observations with PGPROC identity.
 * Module results, PGPROC and epoch are controlled host boundaries here;
 * no retained closure, cross-process snapshot or service proof is produced.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
static PGPROC producer_proc, producer_replacement;
PGPROC *MyProc;
static unsigned producer_fault;

static void
producer_begin(const NativeActorFixtureRole *role)
{
	native_actor_begin(role->backend, role->aux);
	memset(&producer_proc, 0, sizeof(producer_proc));
	producer_proc.pid = MyProcPid;
	producer_proc.pgprocno = 11;
	producer_proc.backendId = InvalidBackendId;
	producer_proc.cluster_grd_generation = 7;
	MyProc = &producer_proc;
	producer_observe_during_poll = NULL;
	native_observe_fixture_epoch = 9;
	CritSectionCount = 0;
}

static void
producer_change_during_poll(void)
{
	switch (producer_fault) {
	case 0: producer_proc.cluster_grd_generation++; break;
	case 1: producer_proc.pid++; break;
	case 2: producer_proc.pgprocno++; break;
	case 3: producer_proc.backendId = 0; break;
	case 4: MyProc = NULL; break;
	case 5:
		producer_replacement = producer_proc;
		MyProc = &producer_replacement;
		break;
	case 6: native_observe_fixture_epoch++; break;
	case 7: MyAuxProcType = LmonProcess; MyBackendType = B_LMON; break;
	case 8: cluster_shared_config = false; break;
	case 9: UT_ASSERT(cluster_normal_stop_service_enter()); break;
	case 10: lock_holds = 1; break;
	case 11: CritSectionCount = 1; break;
	}
	producer_observe_during_poll = NULL;
}

UT_TEST(test_producer_observation_binds_real_lms_roles_and_signed_aux_identity)
{
	for (unsigned r = 1; r <= 8; r++) {
		ClusterServiceProducerObservation out;
		ClusterCleanLeaveSharedState before;
		unsigned locks;
		producer_begin(&native_actor_roles[r]);
		before = test_region;
		locks = lock_acquisitions;
		UT_ASSERT_EQ(cluster_service_producer_observe(9, &out), CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(out.identity.pid, MyProcPid);
		UT_ASSERT_EQ(out.identity.pgprocno, 11);
		UT_ASSERT_EQ(out.identity.backend_id, InvalidBackendId);
		UT_ASSERT_EQ(out.identity.proc_generation, 7);
		UT_ASSERT_EQ(out.identity.epoch, 9);
		UT_ASSERT_EQ(out.identity.actor_bit, native_actor_roles[r].bit);
		UT_ASSERT_EQ(out.actor.active_depth, 0);
		UT_ASSERT_EQ(out.responsibility.modules, 5);
		UT_ASSERT_EQ(lms_polls, 5);
		UT_ASSERT(strcmp(out.reason, "NATIVE_PRODUCER_POINT_ONLY") == 0);
		UT_ASSERT_EQ(lock_acquisitions, locks);
		UT_ASSERT_EQ(memcmp(&before, &test_region, sizeof(before)), 0);
	}
}

UT_TEST(test_producer_active_outer_work_precedes_module_observation)
{
	for (unsigned r = 0; r < lengthof(native_actor_roles); r++) {
		ClusterServiceProducerObservation out;
		producer_begin(&native_actor_roles[r]);
		UT_ASSERT(cluster_normal_stop_service_enter());
		UT_ASSERT(cluster_normal_stop_service_enter());
		UT_ASSERT_EQ(cluster_service_producer_observe(9, &out), CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_EQ(out.actor.active_depth, 2);
		UT_ASSERT_EQ(out.responsibility.modules, 0);
		UT_ASSERT_EQ(lms_polls, 0);
		UT_ASSERT(cluster_normal_stop_service_leave(true));
		UT_ASSERT_EQ(cluster_service_producer_observe(9, &out), CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_EQ(out.actor.active_depth, 1);
		UT_ASSERT(cluster_normal_stop_service_leave(true));
		UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
	}
}

UT_TEST(test_producer_module_debt_is_neither_retired_nor_hidden_by_idle_actor)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterServiceProducerObservation out;
		producer_begin(&native_actor_roles[1]);
		lms_cr_observation = CLUSTER_NORMAL_STOP_PENDING;
		if (fault != 0)
			lms_ic_observation = fault == 1 ? CLUSTER_NORMAL_STOP_PENDING
				: fault == 2 ? CLUSTER_NORMAL_STOP_INVALID : (ClusterNormalStopPollResult)77;
		for (unsigned repeat = 0; repeat < 3; repeat++) {
			UT_ASSERT_EQ(cluster_service_producer_observe(9, &out),
				fault >= 2 ? CLUSTER_NORMAL_STOP_INVALID : CLUSTER_NORMAL_STOP_PENDING);
			UT_ASSERT_EQ(out.responsibility.modules, 5);
			UT_ASSERT(strcmp(out.responsibility.domain, fault >= 2 ? "IC_FIXTURE" : "CR") == 0);
			UT_ASSERT_EQ(lms_cr_observation, CLUSTER_NORMAL_STOP_PENDING);
		}
		lms_cr_observation = lms_ic_observation = CLUSTER_NORMAL_STOP_READY;
		UT_ASSERT_EQ(cluster_service_producer_observe(9, &out), CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(lms_polls, 20);
	}
}

UT_TEST(test_producer_identity_and_unlocked_context_are_required_before_poll)
{
	for (unsigned fault = 0; fault < 15; fault++) {
		ClusterServiceProducerObservation out;
		uint64 epoch = 9;
		producer_begin(&native_actor_roles[1]);
		switch (fault) {
		case 0: MyProc = NULL; break;
		case 1: producer_proc.pid = 0; break;
		case 2: producer_proc.pid++; break;
		case 3: producer_proc.pgprocno = -1; break;
		case 4: producer_proc.cluster_grd_generation = 0; break;
		case 5: producer_proc.cluster_grd_generation = UINT64_MAX; break;
		case 6: producer_proc.backendId = 0; break;
		case 7: producer_proc.backendId = -2; break;
		case 8: epoch = 0; break;
		case 9: epoch = UINT64_MAX; break;
		case 10: lock_holds = 1; break;
		case 11: CritSectionCount = 1; break;
		case 12: cluster_shared_config = false; break;
		case 13: MyBackendType = B_BACKEND; break;
		case 14: MyAuxProcType = NotAnAuxProcess; break;
		}
		UT_ASSERT_EQ(cluster_service_producer_observe(epoch, &out), CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT_EQ(lms_polls, 0);
		UT_ASSERT_EQ(out.responsibility.modules, 0);
		lock_holds = CritSectionCount = 0;
	}
	producer_begin(&native_actor_roles[1]);
	UT_ASSERT_EQ(cluster_service_producer_observe(9, NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(lms_polls, 0);
}

UT_TEST(test_producer_identity_epoch_or_role_change_cannot_return_ready)
{
	for (producer_fault = 0; producer_fault <= 8; producer_fault++) {
		ClusterServiceProducerObservation out;
		producer_begin(&native_actor_roles[1]);
		producer_observe_during_poll = producer_change_during_poll;
		UT_ASSERT_EQ(cluster_service_producer_observe(9, &out), CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_EQ(out.identity.proc_generation, 7);
		UT_ASSERT_EQ(out.identity.epoch, 9);
		UT_ASSERT_EQ(out.responsibility.modules, 5);
		UT_ASSERT_EQ(lms_polls, 5);
		UT_ASSERT(strcmp(out.reason, "NATIVE_PRODUCER_IDENTITY_CHANGED") == 0);
	}
	producer_begin(&native_actor_roles[1]);
	UT_ASSERT_EQ(cluster_service_producer_observe(8, &(ClusterServiceProducerObservation){0}),
		CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(lms_polls, 0);
}

UT_TEST(test_producer_last_actor_work_and_invalid_module_keep_their_owners)
{
	ClusterServiceProducerObservation out;
	producer_begin(&native_actor_roles[1]);
	producer_fault = 9;
	producer_observe_during_poll = producer_change_during_poll;
	UT_ASSERT_EQ(cluster_service_producer_observe(9, &out), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(out.actor.active_depth, 1);
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 1);
	UT_ASSERT(cluster_normal_stop_service_leave(true));
	UT_ASSERT_EQ(cluster_service_producer_observe(9, &out), CLUSTER_NORMAL_STOP_READY);
	producer_begin(&native_actor_roles[1]);
	lms_ic_observation = CLUSTER_NORMAL_STOP_INVALID;
	producer_fault = 0;
	producer_observe_during_poll = producer_change_during_poll;
	UT_ASSERT_EQ(cluster_service_producer_observe(9, &out), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT(strcmp(out.responsibility.domain, "IC_FIXTURE") == 0);
	for (producer_fault = 10; producer_fault <= 11; producer_fault++) {
		producer_begin(&native_actor_roles[1]);
		producer_observe_during_poll = producer_change_during_poll;
		UT_ASSERT_EQ(cluster_service_producer_observe(9, &out), CLUSTER_NORMAL_STOP_INVALID);
		lock_holds = CritSectionCount = 0;
	}
	MyProc = NULL;
}
