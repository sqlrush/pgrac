/*-------------------------------------------------------------------------
 *
 * test_cluster_tt_scan_wait_cases.inc
 *    Bounded publisher-wait negatives through the real TT/SMGR consumers.
 *
 *    Included after the helpers in test_cluster_tt_scan_storage.c.  Its
 *    clock/sleep boundary drives deterministic interleavings; TT, storage
 *    reads, inventory publication and identity checks remain real.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_tt_scan_wait_cases.inc
 *
 * NOTES
 *    No real sleeping, processes or publication durability are exercised.
 *
 *-------------------------------------------------------------------------
 */

static int scan_wait_case_sleep_observations;
static int scan_wait_case_scope_change;
static int scan_wait_case_retry_publications;

static void
scan_wait_case_observe_sleep(long microsec)
{
	UT_ASSERT_EQ(microsec, 1000);
	UT_ASSERT(scan_sleep_calls <= 5);
	UT_ASSERT_EQ(scan_wait_depth, 0);
	UT_ASSERT(SpinLockFree(&scan_inventory.lock));
	scan_wait_case_sleep_observations++;
}

static bool
scan_wait_case_begin(BackendType role, ScanConsumer consumer)
{
	if (!scan_begin_case(role))
		return false;
	scan_seed_three(0);
	scan_expect_target(scan_query(consumer, 0, SCAN_XID, SCAN_WRAP), consumer, 0);
	scan_reset_counts();
	scan_wait_case_sleep_observations = 0;
	scan_wait_case_retry_publications = 0;
	scan_after_sleep = scan_wait_case_observe_sleep;
	UT_ASSERT(cluster_undo_inventory_publish_begin(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1));
	return true;
}

static void
scan_wait_case_expect_unavailable(ScanAnswer answer, ScanConsumer consumer)
{
	/* Even a matching first-pass slot cannot escape with unproven finality. */
	scan_expect(answer, consumer,
				consumer == SCAN_RESOLVE ? CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE
										 : CLUSTER_TT_DURABLE_LOCATE_SCAN_UNAVAILABLE,
				0, 0, TT_SLOT_INVALID, InvalidScn);
}

static void
scan_wait_case_finish(void)
{
	scan_after_sleep = NULL;
	scan_after_read = NULL;
	/* Also cleanly retire the original publisher on the no-wait RED base.
	 * Scope-change callbacks retire it before detaching/reinitializing. */
	if (scan_inventory.publishers != 0)
		cluster_undo_inventory_publish_end(scan_wait_case_retry_publications ? 3 : 2, false);
	scan_finish_case();
}

UT_TEST(test_scan_wait_persistent_publisher_stops_after_five_slices)
{
	for (int role = 0; role < lengthof(scan_roles); role++) {
		for (int consumer = SCAN_RESOLVE; consumer <= SCAN_LOCATE; consumer++) {
			ClusterUndoInventoryStats stats;

			if (!scan_wait_case_begin(scan_roles[role], consumer))
				return;
			scan_wait_case_expect_unavailable(scan_query(consumer, 0, SCAN_XID, SCAN_WRAP),
											  consumer);
			UT_ASSERT_EQ(scan_sleep_calls, 5);
			UT_ASSERT_EQ(scan_wait_case_sleep_observations, 5);
			UT_ASSERT_EQ(scan_sleep_us, 5000);
			UT_ASSERT_EQ(scan_clock_ns, UINT64CONST(1005000000));
			UT_ASSERT_EQ(scan_wait_starts, 1);
			UT_ASSERT_EQ(scan_pread_calls, 3);
			UT_ASSERT_EQ(scan_calls, 1);
			UT_ASSERT_EQ(scan_inventory.publishers, 1);
			UT_ASSERT(cluster_undo_inventory_read_stats(&stats));
			UT_ASSERT_EQ(stats.cut_refusal_count, 1);
			scan_wait_case_finish();
		}
	}
}

static void
scan_wait_case_jump_past_deadline(long microsec)
{
	scan_wait_case_observe_sleep(microsec);
	/* The first 1ms sleep was delayed: elapsed time, not just slice count,
	 * must stop this wait before a second sleep or an unproven rescan. */
	scan_clock_ns = UINT64CONST(1006000000);
}

UT_TEST(test_scan_wait_clock_jump_stops_after_one_slice)
{
	for (int role = 0; role < lengthof(scan_roles); role++) {
		for (int consumer = SCAN_RESOLVE; consumer <= SCAN_LOCATE; consumer++) {
			if (!scan_wait_case_begin(scan_roles[role], consumer))
				return;
			scan_after_sleep = scan_wait_case_jump_past_deadline;
			scan_wait_case_expect_unavailable(scan_query(consumer, 0, SCAN_XID, SCAN_WRAP),
											  consumer);
			UT_ASSERT_EQ(scan_sleep_calls, 1);
			UT_ASSERT_EQ(scan_wait_case_sleep_observations, 1);
			UT_ASSERT_EQ(scan_sleep_us, 1000);
			UT_ASSERT_EQ(scan_clock_ns, UINT64CONST(1006000000));
			UT_ASSERT_EQ(scan_wait_starts, 1);
			UT_ASSERT_EQ(scan_pread_calls, 3);
			UT_ASSERT_EQ(scan_inventory.publishers, 1);
			scan_wait_case_finish();
		}
	}
}

static void
scan_wait_case_change_scope_after_sleep(long microsec)
{
	scan_wait_case_observe_sleep(microsec);
	/* Publication actually completes.  Refusal must now be caused by the
	 * changed storage lifetime, not by a publisher left running forever. */
	scan_seed(2, SCAN_XID + 99, TT_SLOT_COMMITTED, SCAN_WRAP, scn_encode(1, 123), false);
	cluster_undo_inventory_publish_end(2, true);
	scan_after_sleep = scan_wait_case_observe_sleep;
	switch (scan_wait_case_scope_change) {
	case 0:
		cluster_undo_inventory_disable(1);
		break;
	case 1:
		cluster_undo_inventory_attach(NULL, false);
		break;
	case 2:
		cluster_undo_inventory_attach(&scan_inventory, true);
		break;
	}
}

static void
scan_wait_case_scope_refusal(int change)
{
	for (int role = 0; role < lengthof(scan_roles); role++) {
		for (int consumer = SCAN_RESOLVE; consumer <= SCAN_LOCATE; consumer++) {
			if (!scan_wait_case_begin(scan_roles[role], consumer))
				return;
			scan_wait_case_scope_change = change;
			scan_after_sleep = scan_wait_case_change_scope_after_sleep;
			scan_wait_case_expect_unavailable(scan_query(consumer, 0, SCAN_XID, SCAN_WRAP),
											  consumer);
			UT_ASSERT_EQ(scan_sleep_calls, 1);
			UT_ASSERT_EQ(scan_wait_case_sleep_observations, 1);
			UT_ASSERT_EQ(scan_sleep_us, 1000);
			UT_ASSERT_EQ(scan_wait_starts, 1);
			UT_ASSERT_EQ(scan_pread_calls, 3);
			UT_ASSERT_EQ(scan_inventory.publishers, 0);
			scan_wait_case_finish();
		}
	}
}

UT_TEST(test_scan_wait_disable_during_sleep_refuses)
{
	scan_wait_case_scope_refusal(0);
}

UT_TEST(test_scan_wait_attachment_change_during_sleep_refuses)
{
	scan_wait_case_scope_refusal(1);
	scan_wait_case_scope_refusal(2);
}

static void
scan_wait_case_publish_again_during_rescan(uint32 segment)
{
	if (segment != 256)
		return;
	scan_after_read = NULL;
	UT_ASSERT_EQ(scan_sleep_calls, 1);
	UT_ASSERT(cluster_undo_inventory_publish_begin(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1));
	scan_wait_case_retry_publications++;
}

static void
scan_wait_case_complete_then_arm_second_publication(long microsec)
{
	scan_wait_case_observe_sleep(microsec);
	scan_seed(2, SCAN_XID + 99, TT_SLOT_COMMITTED, SCAN_WRAP, scn_encode(1, 123), false);
	cluster_undo_inventory_publish_end(2, true);
	/* The next publication starts behind the rescan cursor and remains
	 * active.  A third pass or another waiting allowance would be a bug. */
	scan_after_read = scan_wait_case_publish_again_during_rescan;
	scan_after_sleep = scan_wait_case_observe_sleep;
}

UT_TEST(test_scan_wait_rescan_publication_never_waits_again)
{
	for (int role = 0; role < lengthof(scan_roles); role++) {
		for (int consumer = SCAN_RESOLVE; consumer <= SCAN_LOCATE; consumer++) {
			ClusterUndoInventoryStats stats;

			if (!scan_wait_case_begin(scan_roles[role], consumer))
				return;
			scan_after_sleep = scan_wait_case_complete_then_arm_second_publication;
			scan_wait_case_expect_unavailable(scan_query(consumer, 0, SCAN_XID, SCAN_WRAP),
											  consumer);
			UT_ASSERT_EQ(scan_sleep_calls, 1);
			UT_ASSERT_EQ(scan_wait_case_sleep_observations, 1);
			UT_ASSERT_EQ(scan_sleep_us, 1000);
			UT_ASSERT_EQ(scan_clock_ns, UINT64CONST(1001000000));
			UT_ASSERT_EQ(scan_wait_case_retry_publications, 1);
			UT_ASSERT_EQ(scan_wait_starts, 2);
			UT_ASSERT_EQ(scan_pread_calls, 7);
			UT_ASSERT_EQ(scan_calls, 1);
			UT_ASSERT_EQ(scan_inventory.publishers, 1);
			UT_ASSERT(cluster_undo_inventory_read_stats(&stats));
			UT_ASSERT_EQ(stats.cut_refusal_count, 1);
			scan_wait_case_finish();
		}
	}
}
