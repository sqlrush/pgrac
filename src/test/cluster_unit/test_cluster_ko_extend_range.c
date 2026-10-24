/* Author: SqlRush <sqlrush@gmail.com> */
/* Real KO finite-owner transitions, including a second physical range.
 * Storage, current-grant and transport boundaries are the existing fixtures. */
int ko_extend_range_fixture_main(void);
#define main ko_extend_range_fixture_main
#include "test_cluster_ko_stop.c"
#undef main

UT_TEST(test_borrowed_ranges_keep_one_owner_until_outer_retirement)
{
	ClusterKoSharedMessageV3 request = peer_fence_fixture(1501);
	ClusterKoPeerFenceHandleV3 handle;
	ClusterKoPeerFenceStateV3 state;
	RelFileLocator locator = request.base.key.locator;
	uint64 serial;
	uint32 slot;

	UT_ASSERT(cluster_ko_extend_begin_v3(locator, MAIN_FORKNUM, &serial, NULL));
	slot = ko_extend_slot;
	UT_ASSERT(cluster_ko_peer_fence_begin_v3(&request, &handle, &state));
	for (int range = 0; range < 2; range++) {
		/* This local transition must also be legal beneath caller content
		 * ownership. It must not reacquire the shared object lock. */
		SpinLockAcquire(&storage.shared_lock);
		UT_ASSERT(cluster_ko_extend_range_begin_v3(locator, MAIN_FORKNUM, serial));
		SpinLockRelease(&storage.shared_lock);
		UT_ASSERT(cluster_ko_extend_write_begin_v3(locator, MAIN_FORKNUM));
		UT_ASSERT(cluster_ko_extend_write_begin_v3(locator, MAIN_FORKNUM));
		UT_ASSERT(cluster_ko_extend_write_done_v3(locator, MAIN_FORKNUM));
		UT_ASSERT_EQ(ko_extend_slot, slot);
		UT_ASSERT_EQ(storage.extend_work[slot].serial, serial);
		UT_ASSERT(!fixture_completed_fence_ready(&request, &handle));
	}
	UT_ASSERT(cluster_ko_extend_end_v3(serial));
	UT_ASSERT(!storage.extend_work[slot].used);
	UT_ASSERT(fixture_completed_fence_ready(&request, &handle));
}

UT_TEST(test_second_unfinished_range_cannot_inherit_first_materialization)
{
	ClusterKoSharedMessageV3 request = peer_fence_fixture(1502);
	RelFileLocator locator = request.base.key.locator;
	const char *reason;
	uint64 serial;
	uint32 slot;

	UT_ASSERT(cluster_ko_extend_begin_v3(locator, MAIN_FORKNUM, &serial, NULL));
	slot = ko_extend_slot;
	UT_ASSERT(cluster_ko_extend_range_begin_v3(locator, MAIN_FORKNUM, serial));
	UT_ASSERT(cluster_ko_extend_write_begin_v3(locator, MAIN_FORKNUM));
	UT_ASSERT(!cluster_ko_extend_range_begin_v3(locator, MAIN_FORKNUM, serial));
	UT_ASSERT(cluster_ko_extend_write_done_v3(locator, MAIN_FORKNUM));
	UT_ASSERT(cluster_ko_extend_range_begin_v3(locator, MAIN_FORKNUM, serial));
	UT_ASSERT(!cluster_ko_extend_write_done_v3(locator, MAIN_FORKNUM));
	UT_ASSERT(cluster_ko_extend_write_begin_v3(locator, MAIN_FORKNUM));
	UT_ASSERT(!cluster_ko_extend_range_begin_v3(locator, MAIN_FORKNUM, serial));
	UT_ASSERT(!cluster_ko_extend_end_v3(serial));
	UT_ASSERT(storage.extend_work[slot].used && storage.extend_work[slot].failed);
	UT_ASSERT_EQ(storage.extend_work[slot].serial, serial);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_INVALID);
}

UT_TEST(test_range_borrow_refuses_wrong_lifetime_without_resetting_obligation)
{
	for (int fault = 0; fault < 13; fault++) {
		ClusterKoSharedMessageV3 request = peer_fence_fixture(1510 + fault);
		RelFileLocator locator = request.base.key.locator;
		ForkNumber fork = MAIN_FORKNUM;
		uint64 serial;

		cluster_enabled = cluster_shared_config = true;
		cluster_interconnect_tier = CLUSTER_IC_TIER_1;
		fixture_recovering = false;
		UT_ASSERT(cluster_ko_extend_begin_v3(locator, MAIN_FORKNUM, &serial, NULL));
		UT_ASSERT(cluster_ko_extend_write_begin_v3(locator, MAIN_FORKNUM));
		UT_ASSERT(cluster_ko_extend_write_done_v3(locator, MAIN_FORKNUM));
		if (fault == 0)
			serial = 0;
		else if (fault == 1)
			serial++;
		else if (fault == 2)
			locator.relNumber++;
		else if (fault == 3)
			fork = FSM_FORKNUM;
		else if (fault == 4)
			CurrentResourceOwner = (ResourceOwner)2;
		else if (fault == 5)
			MyProcPid++;
		else if (fault == 6)
			cluster_ko_extend_abort_v3(serial);
		else if (fault == 7)
			writer.claim.identity.origin_owner_incarnation++;
		else if (fault == 8)
			current_epoch++;
		else if (fault == 9)
			fixture_recovering = true;
		else if (fault == 10)
			cluster_enabled = false;
		else if (fault == 11)
			cluster_shared_config = false;
		else
			cluster_interconnect_tier = CLUSTER_IC_TIER_2;
		UT_ASSERT(!cluster_ko_extend_range_begin_v3(locator, fork, serial));
		UT_ASSERT(ko_extend_write_armed && ko_extend_materialized);
	}
	cluster_enabled = cluster_shared_config = true;
	cluster_interconnect_tier = CLUSTER_IC_TIER_1;
	fixture_recovering = false;
}

UT_TEST(test_second_range_abort_keeps_original_after_locks_recovery)
{
	ClusterKoSharedMessageV3 request = peer_fence_fixture(1540);
	RelFileLocator locator = request.base.key.locator;
	uint64 serial;
	uint32 slot;

	UT_ASSERT(cluster_ko_extend_begin_v3(locator, MAIN_FORKNUM, &serial, NULL));
	slot = ko_extend_slot;
	UT_ASSERT(cluster_ko_extend_write_begin_v3(locator, MAIN_FORKNUM));
	UT_ASSERT(cluster_ko_extend_write_done_v3(locator, MAIN_FORKNUM));
	UT_ASSERT(cluster_ko_extend_range_begin_v3(locator, MAIN_FORKNUM, serial));
	UT_ASSERT(cluster_ko_extend_write_begin_v3(locator, MAIN_FORKNUM));
	cluster_ko_extend_abort_v3(serial);
	ko_shared_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, NULL);
	UT_ASSERT(storage.extend_work[slot].used && !storage.extend_work[slot].failed);
	ko_shared_resource_release(RESOURCE_RELEASE_AFTER_LOCKS, false, false, NULL);
	UT_ASSERT(storage.extend_work[slot].used && storage.extend_work[slot].failed);
	UT_ASSERT_EQ(storage.extend_work[slot].serial, serial);
	UT_ASSERT(!cluster_ko_extend_active_v3());
	UT_ASSERT(!cluster_ko_extend_range_begin_v3(locator, MAIN_FORKNUM, serial));
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_borrowed_ranges_keep_one_owner_until_outer_retirement);
	UT_RUN(test_second_unfinished_range_cannot_inherit_first_materialization);
	UT_RUN(test_range_borrow_refuses_wrong_lifetime_without_resetting_obligation);
	UT_RUN(test_second_range_abort_keeps_original_after_locks_recovery);
	UT_RUN(test_extension_attempt_blocks_ready_and_only_reads_its_original_space);
	UT_RUN(test_exact_extension_current_admission_expires_with_original_attempt);
	UT_RUN(test_extension_error_cannot_retire_an_unmaterialized_reservation);
	UT_RUN(test_extension_scope_refuses_source_owner_and_registry_drift);
	UT_RUN(test_extension_capacity_and_serial_never_recycle_running_work);
	UT_RUN(test_extension_error_expires_locally_then_retires_only_after_original_locks);
	UT_RUN(test_extension_backend_exit_retains_exact_unfinished_work_for_recovery);
	UT_DONE();
	return ut_failed_count != 0;
}
