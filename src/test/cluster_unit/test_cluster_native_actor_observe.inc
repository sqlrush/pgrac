/* Actual original enter/leave and private actor counters, not a new seal.
 * Original module returns/locks remain the existing host fixture boundaries.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
typedef struct NativeActorFixtureRole {
	BackendType backend;
	AuxProcType aux;
	uint32 bit;
} NativeActorFixtureRole;
static const NativeActorFixtureRole native_actor_roles[] = {
	{ B_LMON, LmonProcess, 1 }, { B_LMS, LmsProcess, 2 },
	{ B_LMS_WORKER, LmsWorker1Process, 4 }, { B_LMS_WORKER, LmsWorker2Process, 8 },
	{ B_LMS_WORKER, LmsWorker3Process, 16 }, { B_LMS_WORKER, LmsWorker4Process, 32 },
	{ B_LMS_WORKER, LmsWorker5Process, 64 }, { B_LMS_WORKER, LmsWorker6Process, 128 },
	{ B_LMS_WORKER, LmsWorker7Process, 256 }, { B_SINVAL_BCAST, SinvalBcastProcess, 512 },
	{ B_LMD, LmdProcess, 1024 }
};

static void
native_actor_begin(BackendType backend, AuxProcType aux)
{
	reset_identity();
	IsUnderPostmaster = true;
	cluster_enabled = cluster_shared_config = true;
	cluster_lmd_enabled = true;
	MyBackendType = backend;
	MyAuxProcType = aux;
	lms_polls = lms_closes = 0;
	lms_cr_observation = lms_ic_observation = CLUSTER_NORMAL_STOP_READY;
	UT_ASSERT(!cluster_normal_stop_requested());
}

UT_TEST(test_native_actor_observation_tracks_all_original_nested_owners)
{
	for (unsigned r = 0; r < lengthof(native_actor_roles); r++) {
		ClusterServiceActorObservation out;
		ClusterCleanLeaveSharedState before;
		unsigned locks;
		const NativeActorFixtureRole *role = &native_actor_roles[r];
		native_actor_begin(role->backend, role->aux);
		before = test_region;
		locks = lock_acquisitions;
		UT_ASSERT_EQ(cluster_service_actor_observe(&out), CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(out.actor_bit, role->bit);
		UT_ASSERT_EQ(out.bound_actor_bit, 0);
		UT_ASSERT_EQ(out.active_depth, 0);
		UT_ASSERT(cluster_normal_stop_service_enter());
		UT_ASSERT(cluster_normal_stop_service_enter());
		for (unsigned repeat = 0; repeat < 3; repeat++) {
			UT_ASSERT_EQ(cluster_service_actor_observe(&out), CLUSTER_NORMAL_STOP_PENDING);
			UT_ASSERT_EQ(out.actor_bit, role->bit);
			UT_ASSERT_EQ(out.bound_actor_bit, role->bit);
			UT_ASSERT_EQ(out.active_depth, 2);
			UT_ASSERT_EQ(cl_normal_stop_service_depth, 2);
			UT_ASSERT_EQ(cl_normal_stop_service_bit, role->bit);
		}
		UT_ASSERT(cluster_normal_stop_service_leave(true));
		UT_ASSERT_EQ(cluster_service_actor_observe(&out), CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_EQ(out.active_depth, 1);
		UT_ASSERT(cluster_normal_stop_service_leave(true));
		UT_ASSERT_EQ(cluster_service_actor_observe(&out), CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(out.bound_actor_bit, 0);
		UT_ASSERT_EQ(out.active_depth, 0);
		UT_ASSERT_EQ(lock_acquisitions, locks);
		UT_ASSERT_EQ(memcmp(&before, &test_region, sizeof(before)), 0);
	}
}

UT_TEST(test_native_actor_observation_rejects_empty_copy_and_wrong_identity)
{
	for (unsigned r = 0; r < lengthof(native_actor_roles); r++)
		for (unsigned fault = 0; fault < 2; fault++) {
			ClusterServiceActorObservation out;
			const NativeActorFixtureRole *role = &native_actor_roles[r];
			native_actor_begin(role->backend, role->aux);
			if (fault == 0)
				MyBackendType = B_BACKEND;
			else
				MyAuxProcType = CheckpointerProcess;
			UT_ASSERT_EQ(cluster_service_actor_observe(&out), CLUSTER_NORMAL_STOP_INVALID);
			UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
			UT_ASSERT_EQ(cl_normal_stop_service_bit, 0);
		}
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterServiceActorObservation out;
		native_actor_begin(B_LMD, LmdProcess);
		switch (fault) {
		case 0: IsUnderPostmaster = false; break;
		case 1: cluster_shared_config = false; break;
		case 2: cluster_enabled = false; break;
		case 3: cluster_lmd_enabled = false; break;
		}
		UT_ASSERT_EQ(cluster_service_actor_observe(&out), CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
	}
	native_actor_begin(B_LMS, LmsProcess);
	UT_ASSERT_EQ(cluster_service_actor_observe(NULL), CLUSTER_NORMAL_STOP_INVALID);
}

UT_TEST(test_native_actor_observation_never_repairs_mismatched_or_full_depth)
{
	ClusterServiceActorObservation out;
	native_actor_begin(B_LMS, LmsProcess);
	UT_ASSERT(cluster_normal_stop_service_enter());
	MyBackendType = B_LMON;
	MyAuxProcType = LmonProcess;
	UT_ASSERT_EQ(cluster_service_actor_observe(&out), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(out.actor_bit, 1);
	UT_ASSERT_EQ(out.bound_actor_bit, 2);
	UT_ASSERT_EQ(out.active_depth, 1);
	UT_ASSERT_EQ(cl_normal_stop_service_bit, 2);
	MyBackendType = B_LMS;
	MyAuxProcType = LmsProcess;
	cl_normal_stop_service_depth = UINT32_MAX;
	UT_ASSERT_EQ(cluster_service_actor_observe(&out), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(out.active_depth, UINT32_MAX);
	UT_ASSERT_EQ(cl_normal_stop_service_depth, UINT32_MAX);
	cl_normal_stop_service_depth = 1;
	UT_ASSERT(cluster_normal_stop_service_leave(true));
	cl_normal_stop_service_bit = 2;
	UT_ASSERT_EQ(cluster_service_actor_observe(&out), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(cl_normal_stop_service_bit, 2);
	cl_normal_stop_service_bit = 0;
	UT_ASSERT(cluster_normal_stop_service_enter());
	cl_normal_stop_service_bit = 0;
	UT_ASSERT_EQ(cluster_service_actor_observe(&out), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 1);
	cl_normal_stop_service_bit = 2;
	UT_ASSERT(cluster_normal_stop_service_leave(true));
}

UT_TEST(test_native_actor_return_does_not_complete_original_module_debt)
{
	ClusterServiceActorObservation actor;
	ClusterServiceObservation modules;
	ClusterCleanLeaveSharedState before;
	native_actor_begin(B_LMS, LmsProcess);
	before = test_region;
	UT_ASSERT(cluster_normal_stop_service_enter());
	/* The old convenience wrapper still keeps its original online shortcut. */
	UT_ASSERT_EQ(cluster_lms_normal_stop_idle(), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(lms_polls, 0);
	UT_ASSERT_EQ(cluster_service_actor_observe(&actor), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(!cluster_normal_stop_service_leave(false));
	lms_cr_observation = CLUSTER_NORMAL_STOP_PENDING;
	for (unsigned repeat = 0; repeat < 3; repeat++) {
		UT_ASSERT_EQ(cluster_service_actor_observe(&actor), CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(cluster_service_observe(&modules), CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT(strcmp(modules.domain, "CR") == 0);
		UT_ASSERT_EQ(lms_cr_observation, CLUSTER_NORMAL_STOP_PENDING);
	}
	UT_ASSERT_EQ(lms_polls, 15);
	UT_ASSERT_EQ(memcmp(&before, &test_region, sizeof(before)), 0);
	lms_cr_observation = CLUSTER_NORMAL_STOP_READY;
	UT_ASSERT_EQ(cluster_service_observe(&modules), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_native_actor_observation_needs_no_stop_storage_or_request)
{
	ClusterServiceActorObservation out;
	ClusterLeaveState *saved_leave;
	ClusterNormalStopState *saved_stop;
	ClusterCleanLeaveSharedState before;
	native_actor_begin(B_LMS_WORKER, LmsWorker7Process);
	before = test_region;
	saved_leave = cl_state;
	saved_stop = cl_normal_stop;
	cl_state = NULL;
	cl_normal_stop = NULL;
	UT_ASSERT(cluster_normal_stop_service_enter());
	UT_ASSERT_EQ(cluster_service_actor_observe(&out), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(out.actor_bit, 256);
	UT_ASSERT_EQ(out.bound_actor_bit, 256);
	UT_ASSERT(cluster_normal_stop_service_leave(true));
	UT_ASSERT_EQ(cluster_service_actor_observe(&out), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(memcmp(&before, &test_region, sizeof(before)), 0);
	cl_state = saved_leave;
	cl_normal_stop = saved_stop;
}
