/* The original owner round must retain its first complete sample through
 * directory/H waits and their final callbacks. PAGE/FD/report transport is
 * controlled; the original Start, wait loops and cleanup execute.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned owner_continuation_directory_calls, owner_continuation_directory_mode;

static ClusterCtrcLocalEmptyV1
owner_continuation_directory(void)
{
	owner_continuation_directory_calls++;
	UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	UT_ASSERT(pending_storage_mask == 3 && pending_storage_closes == 0);
	if (owner_continuation_directory_mode == 1 && owner_continuation_directory_calls == 1) {
		ctrc_driver_waiting = true;
		return CLUSTER_CTRC_LOCAL_WAIT;
	}
	if (owner_continuation_directory_mode == 2) owner_round_change_first();
	if (owner_continuation_directory_mode == 3) pg_re_throw();
	return CLUSTER_CTRC_LOCAL_EMPTY;
}

static void
owner_continuation_ready(bool unchanged, bool history)
{
	if (history) history_ready(unchanged ? 1 : 0);
	else owner_round_ready(unchanged);
	owner_continuation_directory_calls = owner_continuation_directory_mode = 0;
}

static void
owner_continuation_cleanup(void)
{
	startup_native_directory = NULL;
	history_sample_step = NULL;
	history_expect_wait = false;
	owner_round_cleanup();
}

static void
owner_continuation_resolve_history_and_change(void)
{
	history_resolve_wait();
	owner_round_change_first();
}

UT_TEST(test_startup_owner_continuation_stable_directory_and_h_wait_keep_same_operations)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		for (unsigned history = 0; history < 2; history++) {
			owner_continuation_ready(unchanged != 0, history != 0);
			if (history) {
				history_views[1].valid = false; history_expect_wait = true;
				ctrc_driver_wait_step = history_resolve_wait;
			} else {
				owner_continuation_directory_mode = 1;
				startup_native_directory = owner_continuation_directory;
			}
			retirement_run_startup();
			{
				bool reached = retirement_service_reached && !info.cluster_tx_start->revoked;
				owner_continuation_cleanup();
				UT_ASSERT(reached);
			}
			UT_ASSERT_EQ(ctrc_driver_waits, 1);
			UT_ASSERT_EQ(prefix_steps, 2); UT_ASSERT_EQ(pending_storage_created, 2);
		}
	}
}

UT_TEST(test_startup_owner_continuation_directory_wait_cannot_replace_original_sample)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		owner_continuation_ready(unchanged != 0, false);
		owner_continuation_directory_mode = 1;
		startup_native_directory = owner_continuation_directory;
		ctrc_driver_wait_step = owner_round_change_first;
		retirement_run_startup();
		{
			bool refused = !retirement_service_reached && info.cluster_tx_start->revoked;
			owner_continuation_cleanup();
			UT_ASSERT(refused);
		}
		UT_ASSERT_EQ(ctrc_driver_waits, 1);
	}
}

UT_TEST(test_startup_owner_continuation_h_wait_cannot_replace_original_sample)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		owner_continuation_ready(unchanged != 0, true);
		history_views[1].valid = false; history_expect_wait = true;
		ctrc_driver_wait_step = owner_continuation_resolve_history_and_change;
		retirement_run_startup();
		{
			bool refused = !retirement_service_reached && info.cluster_tx_start->revoked;
			owner_continuation_cleanup();
			UT_ASSERT(refused);
		}
		UT_ASSERT_EQ(ctrc_driver_waits, 1);
	}
}

UT_TEST(test_startup_owner_continuation_last_directory_callback_cannot_change_sample)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		owner_continuation_ready(unchanged != 0, false);
		owner_continuation_directory_mode = 2;
		startup_native_directory = owner_continuation_directory;
		retirement_run_startup();
		{
			bool refused = !retirement_service_reached && info.cluster_tx_start->revoked;
			owner_continuation_cleanup();
			UT_ASSERT(refused);
		}
		UT_ASSERT_EQ(owner_continuation_directory_calls, 1);
	}
}

UT_TEST(test_startup_owner_continuation_last_h_sample_cannot_change_sample)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		owner_continuation_ready(unchanged != 0, true);
		history_sample_step = owner_round_change_first;
		retirement_run_startup();
		{
			bool refused = !retirement_service_reached && info.cluster_tx_start->revoked;
			owner_continuation_cleanup();
			UT_ASSERT(refused);
		}
		UT_ASSERT_EQ(history_samples, 1);
	}
}

UT_TEST(test_startup_owner_continuation_directory_and_h_errors_keep_original_custody)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		for (unsigned history = 0; history < 2; history++) {
			owner_continuation_ready(unchanged != 0, history != 0);
			if (history) history_sample_step = history_sample_error;
			else {
				owner_continuation_directory_mode = 3;
				startup_native_directory = owner_continuation_directory;
			}
			retirement_run_startup();
			{
				bool refused = !retirement_service_reached && info.cluster_tx_start->revoked;
				owner_continuation_cleanup();
				UT_ASSERT(refused);
			}
		}
	}
}
