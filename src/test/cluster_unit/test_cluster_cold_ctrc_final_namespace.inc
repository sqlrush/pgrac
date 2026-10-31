/* Final namespace observation in the original Startup owner. Namespace I/O
 * is a controlled leaf here; actual file enumeration has its own fixture.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static bool final_namespace_probe;
static unsigned final_namespace_calls, final_namespace_fault, final_namespace_at;

bool cluster_recovery_census_namespace_current_v1(ClusterRecoveryCensus *cursor);
bool
cluster_recovery_census_namespace_current_v1(ClusterRecoveryCensus *cursor)
{
	UT_ASSERT(cursor == (ClusterRecoveryCensus *)&observation_cursor_object);
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
	if (final_namespace_probe && ++final_namespace_calls == final_namespace_at) {
		if (final_namespace_fault == 1) return false;
		if (final_namespace_fault == 2) owner_boot++;
		if (final_namespace_fault == 3) pg_re_throw();
	}
	return true;
}

static void
final_namespace_ready(bool zero)
{
	untouched_ready(zero);
	final_namespace_probe = true;
	final_namespace_calls = final_namespace_fault = 0; final_namespace_at = 1;
}
static void
final_namespace_done(void)
{
	final_namespace_probe = false;
	untouched_done();
}
UT_TEST(test_startup_final_namespace_covers_completed_and_zero_page_sets)
{
	for (unsigned zero = 0; zero < 2; zero++) {
		final_namespace_ready(zero); retirement_run_startup();
		UT_ASSERT(retirement_service_reached); UT_ASSERT_EQ(final_namespace_calls, 1);
		UT_ASSERT(observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		final_namespace_done();
	}
}
UT_TEST(test_startup_final_namespace_refusal_keeps_same_fd_and_original_scope)
{
	for (unsigned zero = 0; zero < 2; zero++) {
		final_namespace_ready(zero); final_namespace_fault = 1; retirement_run_startup();
		UT_ASSERT_EQ(final_namespace_calls, 1);
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT(observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		if (!zero) UT_ASSERT(pending_storage_live);
		final_namespace_done();
	}
}
UT_TEST(test_startup_final_namespace_late_owner_and_error_keep_debt)
{
	for (unsigned fault = 2; fault <= 3; fault++) {
		final_namespace_ready(false); final_namespace_fault = fault; retirement_run_startup();
		UT_ASSERT_EQ(final_namespace_calls, 1);
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT(pending_storage_live && observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		final_namespace_done();
	}
}
UT_TEST(test_startup_final_namespace_repeat_and_wait_cannot_cache_empty_set)
{
	unsigned steps, opens;
	final_namespace_ready(false); retirement_run_startup();
	UT_ASSERT_EQ(final_namespace_calls, 1); UT_ASSERT(retirement_service_reached);
	steps = prefix_steps; opens = pending_storage_created;
	final_namespace_fault = 1; final_namespace_at = 2; retirement_service_reached = false;
	retirement_run_startup();
	UT_ASSERT_EQ(final_namespace_calls, 2);
	UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
	UT_ASSERT_EQ(prefix_steps, steps); UT_ASSERT_EQ(pending_storage_created, opens);
	UT_ASSERT(pending_storage_live); final_namespace_done();
	final_namespace_ready(false); final_local_empty_mode = 1; retirement_run_startup();
	UT_ASSERT(retirement_service_reached); UT_ASSERT_EQ(final_namespace_calls, 3);
	UT_ASSERT_EQ(prefix_steps, 1); UT_ASSERT_EQ(pending_storage_created, 1);
	final_namespace_done();
}
UT_TEST(test_startup_final_namespace_rechecks_after_independent_service_wait)
{
	post_service_ready(0, 0); final_namespace_probe = true;
	final_namespace_calls = 0; final_namespace_fault = 1; final_namespace_at = 2;
	retirement_run_startup();
	UT_ASSERT(post_service_polled); UT_ASSERT_EQ(final_namespace_calls, 2);
	UT_ASSERT_EQ(claim_arbitrations, 0); UT_ASSERT_EQ(claim_releases, 0);
	UT_ASSERT(info.cluster_tx_start != NULL && info.cluster_tx_start->revoked);
	UT_ASSERT(pending_storage_mask == 3 && observation_scope_live && observation_cursor_live);
	UT_ASSERT_EQ(pending_storage_closes, 0);
	final_namespace_probe = false; post_service_done();
}
