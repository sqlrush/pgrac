/* Real native fanout and original stop gates. Native owner returns/locks
	* are the unchanged original fixture boundaries, not a producer cut.
	* Author: SqlRush <sqlrush@gmail.com> */
void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *arg)
{
	if (lock_holds != 0)
		callback(&test_region.leave.lock, LW_SHARED, arg);
}

static void
online_native_begin(void)
{
	reset_identity();
	MyAuxProcType = CheckpointerProcess;
	MyBackendType = B_CHECKPOINTER;
	cluster_shared_config = true;
	native_observe_fixture_epoch = 9;
	native_observe_epoch_after_poll = 0;
	CritSectionCount = 0;
	UT_ASSERT(!cluster_normal_stop_requested());
}

UT_TEST(test_online_native_observation_keeps_original_stop_gate)
{
	unsigned existing_calls;
	ClusterNormalStopModuleObservation out;
	ClusterCleanLeaveSharedState before;
	online_native_begin();
	before = test_region;
	existing_calls = module_pi_retire_calls + checkpoint_native_calls + checkpoint_retire_calls;
	UT_ASSERT_EQ(cluster_normal_stop_modules_poll(false, &out), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(module_calls[9], 0);
	UT_ASSERT_EQ(cluster_native_producers_observe(identity_root, 9, &out), CLUSTER_NORMAL_STOP_READY);
	for (unsigned i = 0; i < lengthof(module_calls); i++)
		UT_ASSERT_EQ(module_calls[i], 1);
	UT_ASSERT_EQ(module_post_calls, 0);
	UT_ASSERT(strcmp(out.reason, "NATIVE_SHARED_POINT_READY_NOT_A_PRODUCER_CUT") == 0);
	UT_ASSERT_EQ(memcmp(&before, &test_region, sizeof(before)), 0);
	UT_ASSERT_EQ(module_pi_retire_calls + checkpoint_native_calls + checkpoint_retire_calls, existing_calls);
}

UT_TEST(test_online_native_each_pending_and_exact_original_return)
{
	for (unsigned i = 0; i < lengthof(module_results); i++) {
		ClusterNormalStopModuleObservation out;
		ClusterCleanLeaveSharedState before;
		online_native_begin();
		before = test_region;
		if (i == 3)
			ctrc_observation = CLUSTER_NORMAL_STOP_PENDING;
		else
			module_results[i] = CLUSTER_NORMAL_STOP_PENDING;
		for (unsigned repeat = 0; repeat < 3; repeat++) {
			UT_ASSERT_EQ(cluster_native_producers_observe(identity_root, 9, &out), CLUSTER_NORMAL_STOP_PENDING);
			UT_ASSERT(out.module != NULL && out.reason != NULL && out.object[0] != '\0');
			UT_ASSERT_EQ(memcmp(&before, &test_region, sizeof(before)), 0);
			UT_ASSERT_EQ(module_calls[13], repeat + 1);
		}
		ctrc_observation = module_results[i] = CLUSTER_NORMAL_STOP_READY;
		UT_ASSERT_EQ(cluster_native_producers_observe(identity_root, 9, &out), CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(memcmp(&before, &test_region, sizeof(before)), 0);
	}
}

UT_TEST(test_online_native_invalid_and_unknown_never_poison_stop)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterNormalStopModuleObservation out;
		ClusterCleanLeaveSharedState before;
		online_native_begin();
		before = test_region;
		module_results[0] = CLUSTER_NORMAL_STOP_PENDING;
		module_results[12] = fault ? (ClusterNormalStopPollResult)93 : CLUSTER_NORMAL_STOP_INVALID;
		UT_ASSERT_EQ(cluster_native_producers_observe(identity_root, 9, &out), CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT(strcmp(out.module, "HW") == 0);
		UT_ASSERT_EQ(module_calls[13], 1);
		UT_ASSERT_EQ(memcmp(&before, &test_region, sizeof(before)), 0);
		module_results[0] = module_results[12] = CLUSTER_NORMAL_STOP_READY;
		UT_ASSERT_EQ(cluster_native_producers_observe(identity_root, 9, &out), CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
	}
}

UT_TEST(test_online_native_owner_input_and_unlocked_guards)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		ClusterNormalStopModuleObservation out;
		ClusterCleanLeaveSharedState before;
		const uint8 *root;
		uint64 epoch = 9;
		online_native_begin();
		before = test_region;
		root = identity_root;
		switch (fault) {
		case 0: IsUnderPostmaster = false; break;
		case 1: MyAuxProcType = LmonProcess; break;
		case 2: MyBackendType = B_BACKEND; break;
		case 3: root = NULL; break;
		case 4: epoch = 0; break;
		case 5: epoch = UINT64_MAX; break;
		case 6: CritSectionCount = 1; break;
		case 7: lock_holds = 1; break;
		case 8: cluster_shared_config = false; break;
		case 9: cluster_enabled = false; break;
		case 10: epoch = 8; break;
		}
		UT_ASSERT_EQ(cluster_native_producers_observe(root, epoch, &out), fault == 10
			? CLUSTER_NORMAL_STOP_PENDING : CLUSTER_NORMAL_STOP_INVALID);
		for (unsigned i = 0; i < lengthof(module_calls); i++)
			UT_ASSERT_EQ(module_calls[i], 0);
		UT_ASSERT_EQ(memcmp(&before, &test_region, sizeof(before)), 0);
		lock_holds = 0;
		CritSectionCount = 0;
	}
	online_native_begin();
	UT_ASSERT_EQ(cluster_native_producers_observe(identity_root, 9, NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(module_calls[0], 0);
}

UT_TEST(test_online_native_epoch_and_input_changes_remain_provisional)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterNormalStopModuleObservation out;
		ClusterCleanLeaveSharedState before;
		online_native_begin();
		before = test_region;
		if (fault != 1)
			native_observe_epoch_after_poll = 10;
		if (fault != 0)
			module_change_root = true;
		if (fault == 2)
			module_results[0] = CLUSTER_NORMAL_STOP_INVALID;
		UT_ASSERT_EQ(cluster_native_producers_observe(identity_root, 9, &out), fault == 2
			? CLUSTER_NORMAL_STOP_INVALID : CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT(strcmp(out.module, fault == 2 ? "ACTIVE_WRITE" : "NATIVE_INPUT") == 0);
		UT_ASSERT_EQ(memcmp(&before, &test_region, sizeof(before)), 0);
	}
	native_observe_fixture_epoch = 9;
	native_observe_epoch_after_poll = 0;
}

UT_TEST(test_online_native_observation_does_not_need_leave_state)
{
	ClusterNormalStopModuleObservation out;
	ClusterCleanLeaveSharedState before;
	ClusterLeaveState *saved_leave;
	ClusterNormalStopState *saved_stop;
	online_native_begin();
	before = test_region;
	saved_leave = cl_state;
	saved_stop = cl_normal_stop;
	cl_state = NULL;
	cl_normal_stop = NULL;
	UT_ASSERT_EQ(cluster_native_producers_observe(identity_root, 9, &out), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(memcmp(&before, &test_region, sizeof(before)), 0);
	cl_state = saved_leave;
	cl_normal_stop = saved_stop;
}
