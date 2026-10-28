/*-------------------------------------------------------------------------
 * test_cluster_side_clog_applied.inc
 *    Original cold native CLOG execution observations stay with their plan.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_side_clog_applied.inc
 * NOTES
 *    Real SIDE plan/production executor; native storage and authority leaves
 *    remain controlled. This is not physical lifetime qualification.
 *-------------------------------------------------------------------------
 */
extern bool rf_side_online_plan_clog_applied_v1(const RfSideOnlinePlanV1 *plan,
	const ClusterWalSourceRef *source, const XLogReaderState *reader);

static bool clog_applied_final_failure, clog_applied_final_error;

static bool
clog_applied_fresh(void *arg)
{
	if (cold_clog_record_projections != 0 && canonical_scope == NULL) {
		if (clog_applied_final_error)
			siglongjmp(*PG_exception_stack, 1);
		if (clog_applied_final_failure)
			return false;
	}
	return canonical_authority_fresh(arg);
}

static void
clog_applied_owner(RfSideOnlineProductionOwnerV1 *owner,
	ClusterThreadRecoveryAuthorityV1 *authority)
{
	cold_clog_test = cold_clog_record_test = control_scope_test = true;
	control_scope_denied = false;
	control_scope_fault = 0;
	cold_clog_record_online = cold_clog_denied = false;
	cold_clog_apply_fail = cold_clog_verify_fail = cold_clog_throw = false;
	cold_clog_stale_after_verify = false;
	clog_applied_final_failure = clog_applied_final_error = false;
	cold_clog_applies = cold_clog_verifies = cold_clog_record_projections = 0;
	UT_ASSERT(rf_side_online_production_owner_init_v1(
		owner, authority, clog_applied_fresh, 19, true));
	UT_ASSERT(rf_side_online_production_bind_undo_v1(owner, authority));
}

static void
clog_applied_done(RfSideOnlinePlanV1 **plan)
{
	UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0);
	cold_clog_test = cold_clog_record_test = control_scope_test = false;
	cold_clog_record_online = cold_clog_denied = false;
	cold_clog_apply_fail = cold_clog_verify_fail = cold_clog_throw = false;
	cold_clog_stale_after_verify = false;
	clog_applied_final_failure = clog_applied_final_error = false;
	rf_side_online_plan_destroy_v1(plan);
}

static RfSideOnlinePlanV1 *
clog_applied_plan(ClusterWalSourceRef *source, FakeXactRecord *record, unsigned kind)
{
	RfSideOnlinePlanV1 *plan = terminal_witness_plan(source, 0);
	clog_witness_frame(record, kind);
	UT_ASSERT_EQ(clog_witness_feed(plan, record, source, 100, 512), 0);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), 0);
	return plan;
}

UT_TEST(test_clog_applied_requires_original_cold_record_success)
{
	for (unsigned kind = 0; kind < 3; ++kind) {
		ClusterWalSourceRef source;
		FakeXactRecord record;
		RfSideOnlinePlanV1 *plan = clog_applied_plan(&source, &record, kind);
		RfSideOnlineOperationV1 operation;
		RfSideOnlineProductionOwnerV1 owner;
		ClusterRecoverySerialGuard guard = {.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED};
		ClusterThreadRecoveryAuthorityV1 authority = {.duty = &canonical_duty, .serial_guard = &guard};
		clog_applied_owner(&owner, &authority);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
		UT_ASSERT(rf_side_online_plan_clog_matches_v1(plan, &source, &record.reader));
		UT_ASSERT(!rf_side_online_plan_clog_applied_v1(plan, &source, &record.reader));
		UT_ASSERT_EQ(rf_side_online_production_preflight_v1(plan, &owner), 0);
		UT_ASSERT(!rf_side_online_plan_clog_applied_v1(plan, &source, &record.reader));
		UT_ASSERT_EQ(cold_clog_applies + cold_clog_verifies, 0);
		UT_ASSERT_EQ(rf_side_online_production_apply_record_v1(plan, 0, &operation.identity, &owner), 0);
		UT_ASSERT_EQ(cold_clog_applies, 1);
		UT_ASSERT_EQ(cold_clog_verifies, 1);
		UT_ASSERT_EQ(cold_clog_record_projections, 1);
		UT_ASSERT(!owner.protected_set_active && !owner.protected_set_complete);
		UT_ASSERT(rf_side_online_plan_clog_applied_v1(plan, &source, &record.reader));
		/* A getter does no replay; another attempt on this tracked witness
		 * cannot zero an already replayed page for a second time. */
		UT_ASSERT_NE(rf_side_online_production_apply_record_v1(plan, 0, &operation.identity, &owner), 0);
		UT_ASSERT_EQ(cold_clog_applies, 1);
		clog_applied_done(&plan);
	}
}

UT_TEST(test_clog_applied_failed_attempt_and_error_keep_no_reusable_success)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		ClusterWalSourceRef source;
		FakeXactRecord record;
		RfSideOnlinePlanV1 *plan = clog_applied_plan(&source, &record, 0);
		RfSideOnlineOperationV1 operation;
		RfSideOnlineProductionOwnerV1 owner;
		ClusterRecoverySerialGuard guard = {.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED};
		ClusterThreadRecoveryAuthorityV1 authority = {.duty = &canonical_duty, .serial_guard = &guard};
		volatile bool caught = false;
		RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		clog_applied_owner(&owner, &authority);
		cold_clog_apply_fail = fault == 0;
		cold_clog_verify_fail = fault == 1;
		cold_clog_stale_after_verify = fault == 2;
		cold_clog_throw = fault == 3;
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
		PG_TRY(); {
			detail = rf_side_online_production_apply_record_v1(plan, 0, &operation.identity, &owner);
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 3);
		UT_ASSERT_NE(detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(cold_clog_applies, 1);
		UT_ASSERT(!rf_side_online_plan_clog_applied_v1(plan, &source, &record.reader));
		cold_clog_apply_fail = cold_clog_verify_fail = cold_clog_stale_after_verify
			= cold_clog_throw = cold_clog_denied = false;
		UT_ASSERT_NE(rf_side_online_production_apply_record_v1(plan, 0, &operation.identity, &owner), 0);
		UT_ASSERT_EQ(cold_clog_applies, 1);
		UT_ASSERT(!rf_side_online_plan_clog_applied_v1(plan, &source, &record.reader));
		clog_applied_done(&plan);
	}
}

UT_TEST(test_clog_applied_cannot_borrow_other_plan_or_whole_source_apply)
{
	ClusterWalSourceRef source, other;
	FakeXactRecord record, second;
	RfSideOnlinePlanV1 *plan = clog_applied_plan(&source, &record, 0);
	RfSideOnlinePlanV1 *foreign = clog_applied_plan(&other, &second, 0);
	RfSideOnlineOperationV1 operation;
	RfSideOnlineProductionOwnerV1 owner;
	ClusterRecoverySerialGuard guard = {.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED};
	ClusterThreadRecoveryAuthorityV1 authority = {.duty = &canonical_duty, .serial_guard = &guard};
	clog_applied_owner(&owner, &authority);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
	UT_ASSERT_EQ(rf_side_online_production_apply_record_v1(plan, 0, &operation.identity, &owner), 0);
	UT_ASSERT(rf_side_online_plan_clog_applied_v1(plan, &source, &record.reader));
	UT_ASSERT(!rf_side_online_plan_clog_applied_v1(foreign, &other, &second.reader));
	clog_applied_owner(&owner, &authority);
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(foreign, &owner), 0);
	UT_ASSERT(!rf_side_online_plan_clog_applied_v1(foreign, &other, &second.reader));
	other.claim.identity.origin_owner_incarnation++;
	UT_ASSERT(!rf_side_online_plan_clog_applied_v1(plan, &other, &record.reader));
	record.data[8] ^= 1;
	UT_ASSERT(!rf_side_online_plan_clog_applied_v1(plan, &source, &record.reader));
	clog_applied_done(&foreign);
	clog_applied_done(&plan);
}

UT_TEST(test_clog_applied_requires_exact_identity_and_final_owner_after_cleanup)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		ClusterWalSourceRef source;
		FakeXactRecord record;
		RfSideOnlinePlanV1 *plan = clog_applied_plan(&source, &record, 0);
		RfSideOnlineOperationV1 operation;
		RfSideOnlineProductionOwnerV1 owner;
		ClusterRecoverySerialGuard guard = {.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED};
		ClusterThreadRecoveryAuthorityV1 authority = {.duty = &canonical_duty, .serial_guard = &guard};
		volatile bool caught = false;
		RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		clog_applied_owner(&owner, &authority);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
		if (fault == 0) operation.identity.record.record_crc++;
		clog_applied_final_failure = fault == 1;
		clog_applied_final_error = fault == 2;
		PG_TRY(); {
			detail = rf_side_online_production_apply_record_v1(plan, 0, &operation.identity, &owner);
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 2);
		UT_ASSERT_NE(detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(cold_clog_applies, fault == 0 ? 0 : 1);
		UT_ASSERT(!rf_side_online_plan_clog_applied_v1(plan, &source, &record.reader));
		clog_applied_done(&plan);
	}
}
