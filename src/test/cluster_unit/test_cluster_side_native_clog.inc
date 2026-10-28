/*-------------------------------------------------------------------------
 * test_cluster_side_native_clog.inc
 *     Original sealed record executor with real own-origin CLOG.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_side_native_clog.inc
 * NOTES
 *     Original native bodies execute; authority, locks and faults are fixtures.
 *     No generation lifetime or joint completion certification.
 *-------------------------------------------------------------------------
 */

static void
side_native_clog_begin(RfSideOnlineProductionOwnerV1 *owner,
	ClusterThreadRecoveryAuthorityV1 *authority)
{
	clog_applied_owner(owner, authority);
	test_side_native_clog_start();
	side_native_clog_test = true;
}

static void
side_native_clog_end(RfSideOnlinePlanV1 **plan)
{
	side_native_clog_test = false;
	test_side_native_clog_stop();
	clog_applied_done(plan);
}

UT_TEST(test_side_native_clog_original_record_success)
{
	ClusterWalSourceRef source, other;
	FakeXactRecord record, copy;
	RfSideOnlinePlanV1 *plan = clog_applied_plan(&source, &record, 0);
	RfSideOnlinePlanV1 *unused = clog_applied_plan(&other, &copy, 0);
	RfSideOnlineOperationV1 op;
	RfSideOnlineProductionOwnerV1 owner;
	ClusterRecoverySerialGuard guard = {.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED};
	ClusterThreadRecoveryAuthorityV1 authority = {.duty = &canonical_duty, .serial_guard = &guard};
	SideNativeClogState before, after;
	side_native_clog_begin(&owner, &authority);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &op));
	/* Native reads must use the original SIDE scope, not a bridge authority. */
	UT_ASSERT(!test_side_native_clog_verify(2, &op.projection, op.owned_payload,
		op.owned_payload_length));
	test_side_native_clog_state(&before);
	UT_ASSERT(!before.disk_zero && before.writes == 0);
	UT_ASSERT_EQ(rf_side_online_production_apply_record_v1(plan, 0, &op.identity, &owner), 0);
	test_side_native_clog_state(&after);
	UT_ASSERT(after.disk_zero && after.writes > 0 && after.syncs > 0 && after.dirsyncs > 0);
	UT_ASSERT_EQ(cold_clog_record_projections, 1);
	UT_ASSERT(rf_side_online_plan_clog_applied_v1(plan, &source, &record.reader));
	UT_ASSERT(!rf_side_online_plan_clog_applied_v1(unused, &other, &copy.reader));
	UT_ASSERT(!test_side_native_clog_verify(2, &op.projection, op.owned_payload,
		op.owned_payload_length));
	UT_ASSERT_NE(rf_side_online_production_apply_record_v1(plan, 0, &op.identity, &owner), 0);
	test_side_native_clog_state(&before);
	UT_ASSERT_EQ(before.writes, after.writes);
	UT_ASSERT_EQ(before.syncs, after.syncs);
	rf_side_online_plan_destroy_v1(&unused);
	side_native_clog_end(&plan);
}

UT_TEST(test_side_native_clog_native_failure_keeps_attempt)
{
	for (unsigned fault = 1; fault <= 2; ++fault) {
		ClusterWalSourceRef source;
		FakeXactRecord record;
		RfSideOnlinePlanV1 *plan = clog_applied_plan(&source, &record, 0);
		RfSideOnlineOperationV1 op;
		RfSideOnlineProductionOwnerV1 owner;
		ClusterRecoverySerialGuard guard = {.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED};
		ClusterThreadRecoveryAuthorityV1 authority = {.duty = &canonical_duty, .serial_guard = &guard};
		SideNativeClogState failed, after;
		volatile bool caught = false;
		RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		side_native_clog_begin(&owner, &authority);
		test_side_native_clog_fault(fault);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &op));
		PG_TRY(); {
			detail = rf_side_online_production_apply_record_v1(plan, 0, &op.identity, &owner);
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 2);
		UT_ASSERT_NE(detail, RF_PAGE_PROOF_DETAIL_OK);
		test_side_native_clog_state(&failed);
		UT_ASSERT(failed.writes > 0 && failed.syncs > 0);
		UT_ASSERT_EQ(cold_clog_record_projections, 0);
		UT_ASSERT(!rf_side_online_plan_clog_applied_v1(plan, &source, &record.reader));
		test_side_native_clog_fault(0);
		UT_ASSERT_NE(rf_side_online_production_apply_record_v1(plan, 0, &op.identity, &owner), 0);
		test_side_native_clog_state(&after);
		UT_ASSERT_EQ(after.writes, failed.writes);
		UT_ASSERT_EQ(after.syncs, failed.syncs);
		UT_ASSERT_EQ(after.dirsyncs, failed.dirsyncs);
		UT_ASSERT(!rf_side_online_plan_clog_applied_v1(plan, &source, &record.reader));
		side_native_clog_end(&plan);
	}
}

UT_TEST(test_side_native_clog_postwrite_drift_blocks_projection)
{
	ClusterWalSourceRef source;
	FakeXactRecord record;
	RfSideOnlinePlanV1 *plan = clog_applied_plan(&source, &record, 0);
	RfSideOnlineOperationV1 op;
	RfSideOnlineProductionOwnerV1 owner;
	ClusterRecoverySerialGuard guard = {.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED};
	ClusterThreadRecoveryAuthorityV1 authority = {.duty = &canonical_duty, .serial_guard = &guard};
	SideNativeClogState failed, after;
	side_native_clog_begin(&owner, &authority);
	test_side_native_clog_fault(3);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &op));
	UT_ASSERT_NE(rf_side_online_production_apply_record_v1(plan, 0, &op.identity, &owner), 0);
	test_side_native_clog_state(&failed);
	UT_ASSERT(failed.writes > 0 && failed.syncs > 0 && failed.dirsyncs > 0);
	UT_ASSERT(!failed.disk_zero);
	UT_ASSERT_EQ(cold_clog_record_projections, 0);
	UT_ASSERT(!rf_side_online_plan_clog_applied_v1(plan, &source, &record.reader));
	test_side_native_clog_fault(0);
	UT_ASSERT_NE(rf_side_online_production_apply_record_v1(plan, 0, &op.identity, &owner), 0);
	test_side_native_clog_state(&after);
	UT_ASSERT_EQ(after.writes, failed.writes);
	side_native_clog_end(&plan);
}

UT_TEST(test_side_native_clog_final_owner_blocks_applied)
{
	ClusterWalSourceRef source;
	FakeXactRecord record;
	RfSideOnlinePlanV1 *plan = clog_applied_plan(&source, &record, 0);
	RfSideOnlineOperationV1 op;
	RfSideOnlineProductionOwnerV1 owner;
	ClusterRecoverySerialGuard guard = {.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED};
	ClusterThreadRecoveryAuthorityV1 authority = {.duty = &canonical_duty, .serial_guard = &guard};
	SideNativeClogState failed, after;
	side_native_clog_begin(&owner, &authority);
	clog_applied_final_failure = true;
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &op));
	UT_ASSERT_NE(rf_side_online_production_apply_record_v1(plan, 0, &op.identity, &owner), 0);
	test_side_native_clog_state(&failed);
	UT_ASSERT(failed.disk_zero && failed.writes > 0 && failed.syncs > 0 && failed.dirsyncs > 0);
	UT_ASSERT_EQ(cold_clog_record_projections, 1);
	UT_ASSERT(!rf_side_online_plan_clog_applied_v1(plan, &source, &record.reader));
	clog_applied_final_failure = false;
	UT_ASSERT_NE(rf_side_online_production_apply_record_v1(plan, 0, &op.identity, &owner), 0);
	test_side_native_clog_state(&after);
	UT_ASSERT_EQ(after.writes, failed.writes);
	UT_ASSERT_EQ(after.syncs, failed.syncs);
	side_native_clog_end(&plan);
}
