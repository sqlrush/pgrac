/*-------------------------------------------------------------------------
 * test_cluster_side_clog_native_consumed.inc
 *    Original native callback consumption remains separate from cold apply.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_side_clog_native_consumed.inc
 * NOTES
 *    Original sealed plan executes; native callback/currentness are controlled.
 *    Consumption is not durability, lifetime or participant completion.
 *-------------------------------------------------------------------------
 */
extern bool side_plan_clog_native_replay(const RfSideOnlinePlanV1 *plan,
	const ClusterWalSourceRef *source, XLogReaderState *reader,
	void (*redo)(XLogReaderState *, void *), bool (*current)(void *), void *arg);
extern bool rf_side_online_plan_clog_native_consumed_v1(const RfSideOnlinePlanV1 *plan,
	const ClusterWalSourceRef *source, const XLogReaderState *reader);

static unsigned native_consumed_calls, native_consumed_checks, native_consumed_fault;

static bool
native_consumed_current(void *arg)
{
	UT_ASSERT(arg == &native_consumed_calls);
	++native_consumed_checks;
	if ((native_consumed_fault == 1 && native_consumed_calls == 0)
		|| (native_consumed_fault == 4 && native_consumed_calls != 0))
		return false;
	if ((native_consumed_fault == 2 && native_consumed_calls == 0)
		|| (native_consumed_fault == 5 && native_consumed_calls != 0))
		siglongjmp(*PG_exception_stack, 1);
	return true;
}

static void
native_consumed_redo(XLogReaderState *reader, void *arg)
{
	UT_ASSERT(arg == &native_consumed_calls && reader != NULL);
	++native_consumed_calls;
	if (native_consumed_fault == 3)
		siglongjmp(*PG_exception_stack, 1);
	if (native_consumed_fault == 6)
		((uint8 *)XLogRecGetData(reader))[0] ^= 1;
}

static void
native_consumed_reset(unsigned fault)
{
	native_consumed_calls = native_consumed_checks = 0;
	native_consumed_fault = fault;
}

UT_TEST(test_native_clog_consumed_runs_original_callback_once_without_cold_applied)
{
	for (unsigned kind = 0; kind < 3; ++kind) {
		ClusterWalSourceRef source;
		FakeXactRecord record;
		RfSideOnlinePlanV1 *plan = clog_applied_plan(&source, &record, kind);
		native_consumed_reset(0);
		UT_ASSERT(!rf_side_online_plan_clog_native_consumed_v1(plan, &source, &record.reader));
		UT_ASSERT(side_plan_clog_native_replay(plan, &source, &record.reader,
			native_consumed_redo, native_consumed_current, &native_consumed_calls));
		UT_ASSERT_EQ(native_consumed_calls, 1);
		UT_ASSERT_EQ(native_consumed_checks, 2);
		UT_ASSERT(rf_side_online_plan_clog_native_consumed_v1(plan, &source, &record.reader));
		UT_ASSERT(!rf_side_online_plan_clog_applied_v1(plan, &source, &record.reader));
		UT_ASSERT(!side_plan_clog_native_replay(plan, &source, &record.reader,
			native_consumed_redo, native_consumed_current, &native_consumed_calls));
		UT_ASSERT_EQ(native_consumed_calls, 1);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_native_clog_consumed_refuses_wrong_bytes_source_and_missing_callback)
{
	ClusterWalSourceRef source, other;
	FakeXactRecord record;
	RfSideOnlinePlanV1 *plan = clog_applied_plan(&source, &record, 0);
	native_consumed_reset(0);
	other = source;
	other.claim.identity.origin_owner_incarnation++;
	UT_ASSERT(!side_plan_clog_native_replay(plan, &other, &record.reader,
		native_consumed_redo, native_consumed_current, &native_consumed_calls));
	record.data[0] ^= 1;
	UT_ASSERT(!side_plan_clog_native_replay(plan, &source, &record.reader,
		native_consumed_redo, native_consumed_current, &native_consumed_calls));
	record.data[0] ^= 1;
	UT_ASSERT(!side_plan_clog_native_replay(plan, &source, &record.reader,
		NULL, native_consumed_current, &native_consumed_calls));
	UT_ASSERT(!side_plan_clog_native_replay(plan, &source, &record.reader,
		native_consumed_redo, NULL, &native_consumed_calls));
	UT_ASSERT_EQ(native_consumed_calls + native_consumed_checks, 0);
	UT_ASSERT(side_plan_clog_native_replay(plan, &source, &record.reader,
		native_consumed_redo, native_consumed_current, &native_consumed_calls));
	UT_ASSERT_EQ(native_consumed_calls, 1);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_native_clog_consumed_false_error_and_postcallback_drift_do_not_retry)
{
	for (unsigned fault = 1; fault <= 6; ++fault) {
		ClusterWalSourceRef source;
		FakeXactRecord record;
		RfSideOnlinePlanV1 *plan = clog_applied_plan(&source, &record, 0);
		volatile bool caught = false;
		bool ok = false;
		native_consumed_reset(fault);
		PG_TRY(); {
			ok = side_plan_clog_native_replay(plan, &source, &record.reader,
				native_consumed_redo, native_consumed_current, &native_consumed_calls);
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(!ok);
		UT_ASSERT_EQ(caught, fault == 2 || fault == 3 || fault == 5);
		UT_ASSERT_EQ(native_consumed_calls, fault <= 2 ? 0 : 1);
		if (fault == 6)
			record.data[0] ^= 1;
		native_consumed_fault = 0;
		UT_ASSERT(!rf_side_online_plan_clog_native_consumed_v1(plan, &source, &record.reader));
		UT_ASSERT(!rf_side_online_plan_clog_applied_v1(plan, &source, &record.reader));
		UT_ASSERT(!side_plan_clog_native_replay(plan, &source, &record.reader,
			native_consumed_redo, native_consumed_current, &native_consumed_calls));
		UT_ASSERT_EQ(native_consumed_calls, fault <= 2 ? 0 : 1);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_native_clog_consumed_cannot_borrow_another_plan_or_cold_success)
{
	ClusterWalSourceRef source, other;
	FakeXactRecord record, copy;
	RfSideOnlinePlanV1 *plan = clog_applied_plan(&source, &record, 0);
	RfSideOnlinePlanV1 *clone = clog_applied_plan(&other, &copy, 0);
	RfSideOnlineOperationV1 operation;
	RfSideOnlineProductionOwnerV1 owner;
	ClusterRecoverySerialGuard guard = {.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED};
	ClusterThreadRecoveryAuthorityV1 authority = {.duty = &canonical_duty, .serial_guard = &guard};
	native_consumed_reset(0);
	UT_ASSERT(side_plan_clog_native_replay(plan, &source, &record.reader,
		native_consumed_redo, native_consumed_current, &native_consumed_calls));
	UT_ASSERT(!rf_side_online_plan_clog_native_consumed_v1(clone, &other, &copy.reader));
	clog_applied_owner(&owner, &authority);
	UT_ASSERT(rf_side_online_plan_operation_v1(clone, 0, &operation));
	UT_ASSERT_EQ(rf_side_online_production_apply_record_v1(clone, 0, &operation.identity, &owner), 0);
	UT_ASSERT(rf_side_online_plan_clog_applied_v1(clone, &other, &copy.reader));
	UT_ASSERT(!rf_side_online_plan_clog_native_consumed_v1(clone, &other, &copy.reader));
	UT_ASSERT(!side_plan_clog_native_replay(clone, &other, &copy.reader,
		native_consumed_redo, native_consumed_current, &native_consumed_calls));
	UT_ASSERT_EQ(native_consumed_calls, 1);
	clog_applied_done(&clone);
	rf_side_online_plan_destroy_v1(&plan);
}
