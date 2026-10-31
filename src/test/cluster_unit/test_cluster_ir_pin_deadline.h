/* Actual Startup merge, IR, ROOT and pin/CF consumers. Clock and transport
 * completion are explicit leaves. Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(startup_ir_preflight_carries_the_original_deadline)
{
	set_cf_fixture(0);
	set_cf_plan.merge_deadline_ns = set_cf_clock_ns + 137 * NS_PER_MS;
	UT_ASSERT(set_cf_call());
	UT_ASSERT_EQ(ir_cf_root_calls, 2);
	UT_ASSERT_EQ(ir_cf_root_budgets[0], 137);
	UT_ASSERT_EQ(ir_cf_root_budgets[1], 137);
	set_cf_cleanup();
}

UT_TEST(startup_ir_budget_is_recomputed_after_root_preflight)
{
	set_cf_fixture(0);
	set_cf_plan.merge_deadline_ns = set_cf_clock_ns + 137 * NS_PER_MS;
	ir_cf_after_root_ns = 37 * NS_PER_MS;
	UT_ASSERT(set_cf_call());
	UT_ASSERT_EQ(ir_cf_acquires, 2);
	UT_ASSERT_EQ(ir_cf_acquire_budgets[0], 100);
	UT_ASSERT_EQ(ir_cf_acquire_budgets[1], 63);
	set_cf_cleanup();
}

UT_TEST(startup_pin_wait_is_bounded_by_the_original_deadline)
{
	ClusterWalPinResult result = CLUSTER_WAL_PIN_INVALID;
	pin_cf_fixture(true, false);
	set_cf_clock_ns = NS_PER_S;
	cold_deadline_active = true;
	result = cluster_wal_retention_pin_preflight_deadline_v1(&pin_cf_existing,
															 set_cf_clock_ns + 137 * NS_PER_MS);
	UT_ASSERT_EQ(cold_deadline_waits[0], 100);
	UT_ASSERT_EQ(result, CLUSTER_WAL_PIN_STALE);
	UT_ASSERT_EQ(entry_waits, 2);
	UT_ASSERT_EQ(cold_deadline_waits[1], 37);
	UT_ASSERT_EQ(retention_attempts, 1);
	cold_deadline_active = false;
	pin_cf_cleanup();
}

UT_TEST(startup_ir_exhaustion_during_preflight_starts_no_ir)
{
	set_cf_fixture(0);
	set_cf_plan.merge_deadline_ns = set_cf_clock_ns + 137 * NS_PER_MS;
	ir_cf_after_root_ns = 137 * NS_PER_MS;
	UT_ASSERT(!set_cf_call());
	UT_ASSERT_EQ(ir_cf_root_calls, 1);
	UT_ASSERT_EQ(ir_cf_acquires, 0);
	UT_ASSERT_EQ(ir_cf_held_count(), 0);
	UT_ASSERT_EQ(set_cf_pin_binds, 0);
	UT_ASSERT_EQ(entry_waits, 0);
	set_cf_cleanup();
}

UT_TEST(startup_ir_expiry_rolls_back_with_the_independent_cleanup_budget)
{
	for (unsigned confirmed = 0; confirmed < 2; ++confirmed) {
		ClusterRecoverySerialAcquireResult result;
		uint16 failed;

		ir_cf_fixture(CLUSTER_RECOVERY_SERIAL_COLD_FORMED, 0);
		set_cf_clock_ns = NS_PER_S;
		ir_cf_after_root_ns = 70 * NS_PER_MS;
		ir_cf_confirm_release = confirmed;
		result = cluster_recovery_serial_acquire_set_deadline_v1(
			ir_cf_requests, 2, 1000, &ir_cf_set, &failed, set_cf_clock_ns + 137 * NS_PER_MS);
		UT_ASSERT_EQ(result, confirmed ? CLUSTER_RECOVERY_SERIAL_RETRY
									   : CLUSTER_RECOVERY_SERIAL_INTERNAL_FAILURE);
		UT_ASSERT_EQ(failed, 1);
		UT_ASSERT_EQ(ir_cf_acquires, 1);
		UT_ASSERT_EQ(ir_cf_releases, 1);
		UT_ASSERT_EQ(ir_cf_release_budgets[0], 1000);
		UT_ASSERT_EQ(ir_cf_set.count, confirmed ? 0 : 1);
		if (!confirmed)
			UT_ASSERT(ir_cf_set.guards[0].release_uncertain);
		UT_ASSERT_EQ(entry_waits, 0);
		ir_cf_cleanup();
	}
}

UT_TEST(startup_ir_preflight_cannot_renew_the_set_aggregate_budget)
{
	ClusterRecoverySerialAcquireResult result;
	uint16 failed;

	ir_cf_fixture(CLUSTER_RECOVERY_SERIAL_COLD_FORMED, 0);
	set_cf_clock_ns = NS_PER_S;
	ir_cf_after_root_ns = 700 * NS_PER_MS;
	result = cluster_recovery_serial_acquire_set_deadline_v1(
		ir_cf_requests, 2, 1000, &ir_cf_set, &failed, set_cf_clock_ns + 10000 * NS_PER_MS);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_SERIAL_RETRY);
	UT_ASSERT_EQ(failed, 1);
	UT_ASSERT_EQ(ir_cf_root_budgets[0], 1000);
	UT_ASSERT_EQ(ir_cf_root_budgets[1], 300);
	UT_ASSERT_EQ(ir_cf_acquire_budgets[0], 300);
	UT_ASSERT_EQ(ir_cf_acquires, 1);
	UT_ASSERT_EQ(ir_cf_set.count, 0);
	ir_cf_cleanup();
}

UT_TEST(startup_ir_expired_submillisecond_or_unknown_clock_publishes_nothing)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		uint16 failed;
		uint64 deadline;

		ir_cf_fixture(CLUSTER_RECOVERY_SERIAL_COLD_FORMED, 0);
		set_cf_clock_ns = NS_PER_S;
		deadline = set_cf_clock_ns + (fault == 1 ? NS_PER_MS - 1 : 0);
		cf_budget_clock_fail = fault == 2;
		UT_ASSERT_EQ(cluster_recovery_serial_acquire_set_deadline_v1(ir_cf_requests, 2, 1000,
																	 &ir_cf_set, &failed, deadline),
					 CLUSTER_RECOVERY_SERIAL_RETRY);
		UT_ASSERT_EQ(failed, 0);
		UT_ASSERT_EQ(ir_cf_root_calls, 0);
		UT_ASSERT_EQ(ir_cf_acquires, 0);
		UT_ASSERT_EQ(ir_cf_set.count, 0);
		cf_budget_clock_fail = false;
		ir_cf_cleanup();
	}
}

static bool
ir_pin_acquire_call(uint64 deadline, const uint16 *threads, uint16 count,
					ClusterWalPinResult *result)
{
	volatile bool completed = false;
	PG_TRY();
	{
		*result = pin_cf_preflight
					  ? cluster_wal_retention_pin_preflight_deadline_v1(&pin_cf_existing, deadline)
					  : cluster_wal_retention_pin_acquire_deadline_v1(&pin_cf_request, 1, threads,
																	  count, &pin_cf_out, deadline);
		completed = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return completed;
}

UT_TEST(startup_pin_walr_and_root_share_the_same_absolute_budget)
{
	const uint16 threads[] = { 1, 2, 3 };
	ClusterWalPinResult result = CLUSTER_WAL_PIN_INVALID;

	pin_cf_fixture(false, false);
	set_cf_clock_ns = NS_PER_S;
	pin_cf_after_walr_ns = 37 * NS_PER_MS;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT(ir_pin_acquire_call(set_cf_clock_ns + 137 * NS_PER_MS, threads, 3, &result));
	UT_ASSERT_EQ(result, CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(pin_cf_walr_acquires, 3);
	UT_ASSERT_EQ(pin_cf_walr_budgets[0], 137);
	UT_ASSERT_EQ(pin_cf_walr_budgets[1], 100);
	UT_ASSERT_EQ(pin_cf_walr_budgets[2], 63);
	UT_ASSERT_EQ(g_last_timeout_ms, 26);
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT(pin_cf_out != NULL && !pin_cf_out->poisoned);
	pin_cf_cleanup();
	UT_ASSERT_EQ(pin_cf_walr_releases, 3);
	UT_ASSERT_EQ(pin_cf_release_budget, 0);
}

UT_TEST(startup_pin_walr_restores_cleanup_on_success_refusal_and_error)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		ClusterWalPinResult result = CLUSTER_WAL_PIN_INVALID;
		bool completed;

		pin_cf_fixture(false, false);
		set_cf_clock_ns = NS_PER_S;
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		pin_cf_walr_refuse = fault == 1;
		pin_cf_walr_error = fault == 2;
		completed = ir_pin_acquire_call(set_cf_clock_ns + 137 * NS_PER_MS, NULL, 0, &result);
		UT_ASSERT_EQ(completed, fault != 2);
		UT_ASSERT_EQ(pin_cf_walr_budgets[0], 137);
		if (fault == 1) {
			UT_ASSERT_EQ(result, CLUSTER_WAL_PIN_UNAVAILABLE);
			UT_ASSERT(active_pin == NULL && pin_cf_out == NULL);
		} else {
			UT_ASSERT(active_pin != NULL);
			UT_ASSERT_EQ(active_pin->threads[0].walr.request.timeout_ms, 0);
		}
		pin_cf_walr_error = false;
		pin_cf_cleanup();
	}
}

UT_TEST(startup_pin_expiry_preserves_partial_walr_cleanup_debt)
{
	const uint16 threads[] = { 1, 2 };
	for (unsigned confirmed = 0; confirmed < 2; ++confirmed) {
		ClusterWalPinResult result = CLUSTER_WAL_PIN_INVALID;

		pin_cf_fixture(false, false);
		set_cf_clock_ns = NS_PER_S;
		pin_cf_after_walr_ns = 137 * NS_PER_MS;
		pin_cf_release_confirmed = confirmed;
		UT_ASSERT(ir_pin_acquire_call(set_cf_clock_ns + 137 * NS_PER_MS, threads, 2, &result));
		UT_ASSERT_EQ(result,
					 confirmed ? CLUSTER_WAL_PIN_UNAVAILABLE : CLUSTER_WAL_PIN_RELEASE_UNCERTAIN);
		UT_ASSERT_EQ(pin_cf_walr_acquires, 1);
		UT_ASSERT_EQ(pin_cf_walr_releases, 1);
		UT_ASSERT_EQ(pin_cf_release_budget, 0);
		UT_ASSERT_EQ(retention_attempts, 0);
		UT_ASSERT_EQ(entry_waits, 0);
		if (!confirmed)
			UT_ASSERT(pin_cf_out != NULL && pin_cf_out->poisoned
					  && pin_cf_out->threads[0].walr.release_uncertain);
		else
			UT_ASSERT(pin_cf_out == NULL && active_pin == NULL);
		pin_cf_release_confirmed = true;
		pin_cf_cleanup();
	}
}

UT_TEST(startup_pin_zero_mode_never_sleeps_or_retries)
{
	for (unsigned preflight = 0; preflight < 2; ++preflight) {
		for (unsigned success = 0; success < 2; ++success) {
			ClusterWalPinResult result = CLUSTER_WAL_PIN_INVALID;

			pin_cf_fixture(preflight, false);
			g_seven_result
				= success ? CLUSTER_LOCK_ACQUIRE_OK_GRANTED : CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
			UT_ASSERT(ir_pin_acquire_call(0, NULL, 0, &result));
			UT_ASSERT_EQ(result, success ? CLUSTER_WAL_PIN_OK : CLUSTER_WAL_PIN_STALE);
			UT_ASSERT_EQ(retention_attempts, 1);
			UT_ASSERT_EQ(g_last_timeout_ms, 1000);
			UT_ASSERT_EQ(entry_waits, 0);
			if (!preflight)
				UT_ASSERT_EQ(pin_cf_walr_budgets[0], 0);
			pin_cf_cleanup();
		}
	}
}

UT_TEST(startup_pin_ack_is_live_before_deadline_and_refused_at_deadline)
{
	for (unsigned late = 0; late < 2; ++late) {
		ClusterWalPinResult result = CLUSTER_WAL_PIN_INVALID;

		pin_cf_fixture(true, false);
		set_cf_clock_ns = NS_PER_S;
		cold_deadline_active = true;
		entry_ack_at = late ? 2 : 1;
		UT_ASSERT(ir_pin_acquire_call(set_cf_clock_ns + 137 * NS_PER_MS, NULL, 0, &result));
		UT_ASSERT_EQ(result, late ? CLUSTER_WAL_PIN_STALE : CLUSTER_WAL_PIN_OK);
		UT_ASSERT_EQ(entry_waits, late ? 2 : 1);
		UT_ASSERT_EQ(retention_attempts, late ? 1 : 2);
		UT_ASSERT_EQ(cold_deadline_waits[0], 100);
		if (!late)
			UT_ASSERT_EQ(g_last_timeout_ms, 37);
		cold_deadline_active = false;
		pin_cf_cleanup();
	}
}

UT_TEST(startup_pin_expired_submillisecond_and_unknown_clock_create_no_request)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		ClusterWalPinResult result = CLUSTER_WAL_PIN_INVALID;
		uint64 deadline;

		pin_cf_fixture(false, false);
		set_cf_clock_ns = NS_PER_S;
		deadline = set_cf_clock_ns + (fault == 1 ? NS_PER_MS - 1 : 0);
		cf_budget_clock_fail = fault == 2;
		UT_ASSERT(ir_pin_acquire_call(deadline, NULL, 0, &result));
		UT_ASSERT_EQ(result, CLUSTER_WAL_PIN_UNAVAILABLE);
		UT_ASSERT_EQ(pin_cf_walr_acquires, 0);
		UT_ASSERT_EQ(retention_attempts, 0);
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT(active_pin == NULL && pin_cf_out == NULL);
		cf_budget_clock_fail = false;
		pin_cf_cleanup();
	}
}

UT_TEST(startup_bounded_pin_rejects_initializer_and_foreign_role)
{
	ClusterWalPinResult result = CLUSTER_WAL_PIN_OK;

	pin_cf_fixture(false, true);
	UT_ASSERT(ir_pin_acquire_call(0, NULL, 0, &result));
	UT_ASSERT_EQ(result, CLUSTER_WAL_PIN_INVALID);
	UT_ASSERT_EQ(pin_cf_walr_acquires, 0);
	UT_ASSERT_EQ(retention_attempts, 0);
	pin_cf_cleanup();
	pin_cf_fixture(true, false);
	MyBackendType = B_BACKEND;
	UT_ASSERT(ir_pin_acquire_call(0, NULL, 0, &result));
	UT_ASSERT_EQ(result, CLUSTER_WAL_PIN_INVALID);
	UT_ASSERT_EQ(retention_attempts, 0);
	MyBackendType = B_STARTUP;
	pin_cf_cleanup();
}
