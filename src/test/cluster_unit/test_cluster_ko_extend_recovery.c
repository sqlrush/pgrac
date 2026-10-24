/* Author: SqlRush <sqlrush@gmail.com> */
/* Real KO finite-owner cleanup. Storage/current boundaries remain explicit. */
int ko_extend_recovery_fixture_main(void);
#define main ko_extend_recovery_fixture_main
#include "test_cluster_ko_stop.c"
#undef main

static uint32 recovery_slot, recovery_pages, recovery_flushes, recovery_syncs;
static int recovery_fault;

static bool
finish_page(RelFileLocator locator, BlockNumber block)
{
	UT_ASSERT_EQ(block, 10 + recovery_pages);
	UT_ASSERT(cluster_ko_extend_recovery_target_v3(locator, MAIN_FORKNUM, block));
	UT_ASSERT(!cluster_ko_extend_recovery_target_v3(locator, MAIN_FORKNUM, block + 1));
	UT_ASSERT(!cluster_ko_extend_recovery_target_v3(locator, FSM_FORKNUM, block));
	UT_ASSERT_EQ(storage.extend_recovery[recovery_slot].executor_pid, MyProcPid);
	UT_ASSERT(!cluster_ko_extend_recovery_poll_v3()); /* No duplicate execution. */
	recovery_pages++;
	if (recovery_fault == 3)
		storage.extend_work[recovery_slot].serial++;
	if (recovery_fault == 4)
		pg_re_throw();
	return recovery_fault != 1;
}

static void
flush_reservation(XLogRecPtr lsn)
{
	UT_ASSERT_EQ(lsn, 0x100);
	recovery_flushes++;
	if (recovery_fault == 2)
		current_epoch++;
}

static void
sync_reservation(SMgrRelation smgr, ForkNumber fork)
{
	UT_ASSERT(smgr == &relation && fork == MAIN_FORKNUM);
	UT_ASSERT_EQ(recovery_pages, 3);
	recovery_syncs++;
	if (recovery_fault == 5)
		pg_re_throw();
}

static uint32
prepare_reserved_failure(uint64 batch)
{
	ClusterKoSharedMessageV3 request = peer_fence_fixture(batch);
	ClusterSpaceReservationChange change = {0};
	uint64 serial;
	RelFileLocator locator = request.base.key.locator;

	recovery_pages = recovery_flushes = recovery_syncs = 0;
	recovery_fault = 0;
	space_identity.key.locator = locator;
	UT_ASSERT(cluster_ko_extend_begin_v3(locator, MAIN_FORKNUM, &serial, NULL));
	UT_ASSERT(cluster_ko_extend_write_begin_v3(locator, MAIN_FORKNUM));
	UT_ASSERT(cluster_ko_extend_reserve_begin_v3(&space_identity));
	change.action = CLUSTER_SPACE_RESERVATION_ADVANCE;
	change.before.identity = change.result.identity = space_identity;
	change.before.next_block = change.first_block = 10;
	change.granted = 3;
	change.result.next_block = 13;
	change.before_token = 1;
	change.result_token = 2;
	UT_ASSERT(cluster_ko_extend_reserve_record_v3(&change, 0x100));
	recovery_slot = ko_extend_slot;
	/* Still live until the original AFTER_LOCKS handoff. */
	UT_ASSERT(!storage.extend_work[recovery_slot].failed);
	cluster_ko_extend_abort_v3(serial);
	ko_shared_resource_release(RESOURCE_RELEASE_AFTER_LOCKS, false, true, NULL);
	UT_ASSERT(storage.extend_work[recovery_slot].used && storage.extend_work[recovery_slot].failed);
	UT_ASSERT(!cluster_ko_extend_active_v3());
	MyBackendType = B_CHECKPOINTER;
	MyAuxProcType = CheckpointerProcess;
	relation.smgr_which = 1;
	ko_extension_page_boundary = finish_page;
	ko_extension_flush_boundary = flush_reservation;
	ko_extension_sync_boundary = sync_reservation;
	return recovery_slot;
}

UT_TEST(test_reserved_failure_recovers_each_page_then_syncs_and_retires)
{
	uint32 slot = prepare_reserved_failure(1610);
	const char *reason;
	uint64 serial = storage.extend_work[slot].serial;
	for (int page = 0; page < 3; page++) {
		UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_STR_EQ(reason, "KO_EXTENSION_RECOVERY_PENDING");
		UT_ASSERT(cluster_ko_extend_recovery_poll_v3());
		UT_ASSERT(storage.extend_work[slot].used && storage.extend_work[slot].failed);
		UT_ASSERT_EQ(storage.extend_work[slot].serial, serial);
		UT_ASSERT_EQ(storage.extend_recovery[slot].cursor, page + 1);
		UT_ASSERT_EQ(storage.extend_recovery[slot].executor_pid, 0);
		UT_ASSERT_EQ(recovery_syncs, 0);
	}
	UT_ASSERT(cluster_ko_extend_recovery_poll_v3());
	UT_ASSERT_EQ(recovery_syncs, 1);
	UT_ASSERT_EQ(recovery_flushes, 4);
	UT_ASSERT(!storage.extend_work[slot].used);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT(!cluster_ko_extend_recovery_poll_v3());
	UT_ASSERT(!cluster_ko_extend_recovery_target_v3(space_identity.key.locator, MAIN_FORKNUM, 12));
}

UT_TEST(test_cancel_before_reservation_has_no_physical_work_to_recover)
{
	ClusterKoSharedMessageV3 request = peer_fence_fixture(1611);
	uint64 serial;
	uint32 slot;
	space_identity.key.locator = request.base.key.locator;
	UT_ASSERT(cluster_ko_extend_begin_v3(request.base.key.locator, MAIN_FORKNUM, &serial, NULL));
	slot = ko_extend_slot;
	UT_ASSERT(cluster_ko_extend_write_begin_v3(request.base.key.locator, MAIN_FORKNUM));
	UT_ASSERT(cluster_ko_extend_reserve_begin_v3(&space_identity));
	cluster_ko_extend_abort_v3(serial);
	ko_shared_resource_release(RESOURCE_RELEASE_AFTER_LOCKS, false, true, NULL);
	UT_ASSERT(!storage.extend_work[slot].used);
}

UT_TEST(test_recovery_refusal_and_source_change_keep_failed_work)
{
	for (int fault = 1; fault <= 3; fault++) {
		uint32 slot = prepare_reserved_failure(1620 + fault);
		uint64 serial = storage.extend_work[slot].serial;
		const char *reason;
		recovery_fault = fault;
		UT_ASSERT(!cluster_ko_extend_recovery_poll_v3());
		UT_ASSERT(storage.extend_work[slot].used && storage.extend_work[slot].failed);
		UT_ASSERT_EQ(storage.extend_work[slot].serial, serial + (fault == 3));
		UT_ASSERT_EQ(storage.extend_recovery[slot].cursor, 0);
		UT_ASSERT_EQ(recovery_syncs, 0);
		UT_ASSERT_EQ(recovery_pages, fault == 2 ? 0 : 1);
		if (fault != 3) {
			UT_ASSERT(storage.extend_recovery[slot].invalid);
			UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_INVALID);
			UT_ASSERT(!cluster_ko_extend_recovery_poll_v3());
		}
	}
}

UT_TEST(test_recovery_has_one_executor_and_keeps_stop_absolute_gate)
{
	uint32 slot = prepare_reserved_failure(1630);
	const char *reason;
	storage.extend_recovery[slot].executor_pid = MyProcPid + 1;
	UT_ASSERT(!cluster_ko_extend_recovery_poll_v3());
	UT_ASSERT_EQ(recovery_pages, 0);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
	storage.extend_recovery[slot].executor_pid = 0;
	ko_stop_requested = true;
	ko_continue_allowed = false;
	UT_ASSERT(!cluster_ko_extend_recovery_poll_v3());
	UT_ASSERT_EQ(recovery_pages, 0);
	UT_ASSERT(!storage.extend_recovery[slot].invalid);
	ko_continue_allowed = true;
	UT_ASSERT(cluster_ko_extend_recovery_poll_v3());
	UT_ASSERT_EQ(recovery_pages, 1);
	ko_stop_requested = false;
}

UT_TEST(test_recovery_io_error_retains_exact_owner_and_clears_execution_claim)
{
	for (int fault = 4; fault <= 5; fault++) {
		uint32 slot = prepare_reserved_failure(1640 + fault);
		bool caught = false;
		if (fault == 5)
			for (int page = 0; page < 3; page++)
				UT_ASSERT(cluster_ko_extend_recovery_poll_v3());
		recovery_fault = fault;
		PG_TRY();
		{
			(void)cluster_ko_extend_recovery_poll_v3();
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT(storage.extend_work[slot].used && storage.extend_work[slot].failed);
		UT_ASSERT(storage.extend_recovery[slot].invalid);
		UT_ASSERT_EQ(storage.extend_recovery[slot].executor_pid, 0);
		UT_ASSERT(!ko_extend_executor.active);
		UT_ASSERT(!cluster_ko_extend_recovery_poll_v3());
	}
}

UT_TEST(test_exit_keeps_unarmed_work_until_original_resource_cleanup)
{
	ClusterKoSharedMessageV3 request = peer_fence_fixture(1601);
	RelFileLocator locator = request.base.key.locator;
	uint64 serial;
	uint32 slot;
	const char *reason;

	UT_ASSERT(cluster_ko_extend_begin_v3(locator, MAIN_FORKNUM, &serial, NULL));
	slot = ko_extend_slot;
	ko_shared_backend_exit(1, (Datum)0);
	UT_ASSERT(storage.extend_work[slot].used);
	UT_ASSERT(!storage.extend_work[slot].failed);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
	ko_shared_resource_release(RESOURCE_RELEASE_AFTER_LOCKS, false, true, NULL);
	UT_ASSERT(!storage.extend_work[slot].used);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_exit_retires_completed_range_only_after_original_locks)
{
	ClusterKoSharedMessageV3 request = peer_fence_fixture(1602);
	RelFileLocator locator = request.base.key.locator;
	uint64 serial;
	uint32 slot;

	UT_ASSERT(cluster_ko_extend_begin_v3(locator, MAIN_FORKNUM, &serial, NULL));
	slot = ko_extend_slot;
	UT_ASSERT(cluster_ko_extend_write_begin_v3(locator, MAIN_FORKNUM));
	UT_ASSERT(cluster_ko_extend_write_done_v3(locator, MAIN_FORKNUM));
	ko_shared_backend_exit(1, (Datum)0);
	UT_ASSERT(!storage.extend_work[slot].failed);
	ko_shared_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(storage.extend_work[slot].used);
	ko_shared_resource_release(RESOURCE_RELEASE_AFTER_LOCKS, false, true, NULL);
	UT_ASSERT(!storage.extend_work[slot].used);
}

UT_TEST(test_exit_does_not_drop_unproved_unfinished_physical_work)
{
	ClusterKoSharedMessageV3 request = peer_fence_fixture(1603);
	RelFileLocator locator = request.base.key.locator;
	uint64 serial;
	uint32 slot;
	const char *reason;

	UT_ASSERT(cluster_ko_extend_begin_v3(locator, MAIN_FORKNUM, &serial, NULL));
	slot = ko_extend_slot;
	UT_ASSERT(cluster_ko_extend_write_begin_v3(locator, MAIN_FORKNUM));
	ko_shared_backend_exit(1, (Datum)0);
	ko_shared_resource_release(RESOURCE_RELEASE_AFTER_LOCKS, false, true, NULL);
	UT_ASSERT(storage.extend_work[slot].used && storage.extend_work[slot].failed);
	UT_ASSERT_EQ(storage.extend_work[slot].serial, serial);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_INVALID);
}

UT_TEST(test_create_exit_preserves_unarmed_original_retirement)
{
	ClusterKoSharedMessageV3 request = peer_fence_fixture(1604);
	uint64 serial;
	uint32 slot;

	UT_ASSERT(cluster_ko_create_begin_v3(request.base.key.locator, &serial));
	slot = ko_create_slot;
	ko_shared_backend_exit(1, (Datum)0);
	UT_ASSERT(storage.extend_work[slot].used && !storage.extend_work[slot].failed);
	ko_shared_resource_release(RESOURCE_RELEASE_AFTER_LOCKS, false, true, NULL);
	UT_ASSERT(!storage.extend_work[slot].used);
}

UT_TEST(test_ko_wait_graph_preserves_failed_reservation_and_busy)
{
	ClusterWaitGraphKoSnapshot *snapshot = calloc(1, sizeof(*snapshot));
	uint32 slot = prepare_reserved_failure(1691);
	bool found = false;
	UT_ASSERT(snapshot != NULL);
	cluster_ko_wait_graph_snapshot(snapshot);
	UT_ASSERT_EQ(snapshot->status, CLWG_OBSERVED);
	for (uint32 i = 0; i < snapshot->nrows; i++) {
		const ClusterWaitGraphKoRow *row = &snapshot->rows[i];
		if (row->kind != 0 || row->slot != slot) continue;
		found = true;
		UT_ASSERT(row->failed && row->observed && row->reserved);
		UT_ASSERT_EQ(row->first, 10);
		UT_ASSERT_EQ(row->count, 3);
		UT_ASSERT_EQ(row->lsn, 0x100);
		UT_ASSERT_EQ(row->serial, storage.extend_work[slot].serial);
	}
	UT_ASSERT(found);
	UT_ASSERT_EQ(recovery_pages + recovery_syncs, 0);
	SpinLockAcquire(&storage.shared_lock);
	cluster_ko_wait_graph_snapshot(snapshot);
	SpinLockRelease(&storage.shared_lock);
	UT_ASSERT_EQ(snapshot->status, CLWG_BUSY);
	UT_ASSERT_EQ(snapshot->nrows, 0);
	UT_ASSERT(storage.extend_work[slot].used && storage.extend_work[slot].failed);
	ko_state = NULL;
	cluster_ko_wait_graph_snapshot(snapshot);
	UT_ASSERT_EQ(snapshot->status, CLWG_UNAVAILABLE);
	ko_state = &storage;
	free(snapshot);
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_ko_wait_graph_preserves_failed_reservation_and_busy);
	UT_RUN(test_exit_keeps_unarmed_work_until_original_resource_cleanup);
	UT_RUN(test_exit_retires_completed_range_only_after_original_locks);
	UT_RUN(test_exit_does_not_drop_unproved_unfinished_physical_work);
	UT_RUN(test_create_exit_preserves_unarmed_original_retirement);
	UT_RUN(test_reserved_failure_recovers_each_page_then_syncs_and_retires);
	UT_RUN(test_cancel_before_reservation_has_no_physical_work_to_recover);
	UT_RUN(test_recovery_refusal_and_source_change_keep_failed_work);
	UT_RUN(test_recovery_has_one_executor_and_keeps_stop_absolute_gate);
	UT_RUN(test_recovery_io_error_retains_exact_owner_and_clears_execution_claim);
	UT_DONE();
	return ut_failed_count != 0;
}
