/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_native_directory.inc
 *     Original native local directory through the original Startup drain.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * NOTES
 *     The native layout, entry producer and scanner are real. Scheduling,
 *     PAGE/TT and authority leaves remain controlled; no discharge grant.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_startup_native_ctrc.h"

static ClusterCtrcLocalEmptyV1
startup_native_directory_probe(void)
{
	ClusterCtrcLocalEmptyV1 result;
	UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
	UT_ASSERT_EQ(prefix_destroyed, 2 * prefix_pages);
	result = test_startup_native_ctrc_probe();
	test_startup_native_ctrc_unlocked();
	if (result == CLUSTER_CTRC_LOCAL_WAIT) ctrc_driver_waiting = true;
	return result;
}

static void
startup_native_directory_ready(unsigned path, unsigned mode, unsigned at)
{
	final_local_empty_ready(path);
	test_startup_native_ctrc_begin(mode, at);
	startup_native_directory = startup_native_directory_probe;
}

static void
startup_native_directory_cancel(void)
{
	test_startup_native_ctrc_unlocked();
	if (ctrc_driver_waits != 0) ShutdownRequestPending = true;
}

static void
startup_native_directory_cleanup(void)
{
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	UT_ASSERT_EQ(pending_storage_closes, 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	test_startup_native_ctrc_unlocked();
	retirement_local_cleanup();
	test_startup_native_ctrc_end();
	startup_native_directory = NULL;
}

UT_TEST(test_startup_native_directory_empty_preserves_fpi_unchanged_and_zero_page)
{
	for (unsigned path = 0; path < 3; path++) {
		startup_native_directory_ready(path, 0, 0);
		retirement_run_startup();
		UT_ASSERT(retirement_service_reached);
		test_startup_native_ctrc_check(1, 10, 10);
		UT_ASSERT_EQ(ctrc_driver_waits, 0);
		UT_ASSERT_EQ(pending_storage_created, path == 2 ? 0 : 2);
		startup_native_directory_cleanup();
	}
}

UT_TEST(test_startup_native_directory_partial_lock_wait_repeats_same_fd_readback)
{
	for (unsigned at = 1; at <= 10; at++) {
		startup_native_directory_ready(0, 0, at);
		retirement_run_startup();
		UT_ASSERT(retirement_service_reached);
		test_startup_native_ctrc_check(2, at + 10, at + 9);
		UT_ASSERT_EQ(ctrc_driver_waits, 1);
		UT_ASSERT_EQ(startup_first_calls, 4);
		UT_ASSERT_EQ(completed_readback_calls, 4);
		UT_ASSERT_EQ(prefix_steps, 2); UT_ASSERT_EQ(pending_storage_created, 2);
		startup_native_directory_cleanup();
	}
}

UT_TEST(test_startup_native_directory_real_origin_cannot_be_discharged_by_wait)
{
	startup_native_directory_ready(0, 1, 0);
	ctrc_interrupt_step = startup_native_directory_cancel;
	retirement_run_startup();
	UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
	test_startup_native_ctrc_check(1, 10, 10);
	UT_ASSERT_EQ(ctrc_driver_waits, 1);
	UT_ASSERT_EQ(pending_storage_mask, 3);
	startup_native_directory_cleanup();
}

UT_TEST(test_startup_native_directory_invalid_layout_and_partial_error_keep_debt)
{
	for (unsigned at = 0; at <= 10; at++) {
		startup_native_directory_ready(0, at == 0 ? 2 : 3, at);
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		test_startup_native_ctrc_check(1, at, at == 0 ? 0 : at - 1);
		UT_ASSERT_EQ(ctrc_driver_waits, 0);
		UT_ASSERT_EQ(pending_storage_mask, 3);
		startup_native_directory_cleanup();
	}
}
