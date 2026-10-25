/*-------------------------------------------------------------------------
 * Actual Checkpointer windows through the original complete cold I/O scan.
 * Original ROOT/retained visitors, decoding and storage leaves are fixtures.
 * This composition grants no native durability or recovery authority.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_cold_io_checkpoint_publisher.inc
 * NOTES
 *   PGRAC-original; reuse the existing namespaced test bridge unchanged.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_cold_checkpoint_publisher.h"

static ClusterWalInputV1 *
io_publisher_fixture(uint64 next, uint64 issued, unsigned fault, XLogRecPtr end,
					 TestColdCheckpointPublisherResult *published)
{
	ClusterWalInputV1 *input;

	scope_reset();
	input = generation(2, 12, false, REQUIRED, 0x1000, 0x1000, 0x1300);
	input->source.claim.database_incarnation = 3;
	input->native_checkpoint.nextXid = FullTransactionIdFromU64(next);
	scan_reset();
	scoped_checkpoint_wal(input);
	for (int i = 0; i < visit_count; i++)
		decode_xid_input[i].complete = true;
	memset(input->native_window, 0xa5, sizeof(input->native_window));
	*published = test_cold_checkpoint_publish(&input->source, &input->native_checkpoint,
											  input->checkpoint_start, end, issued, fault,
											  input->native_window);
	return input;
}

static ClusterColdDetailV1
io_publisher_scan(ClusterWalInputV1 *input, unsigned route, ClusterColdScanResultV1 *result)
{
	ClusterColdPlanV1 *plan = scan_plan();
	ClusterControlRootReadToken token = { 0 };
	ClusterColdDetailV1 detail;

	if (route) {
		selected_checkpoint.source = input->source;
		selected_checkpoint.checkpoint_start = input->checkpoint_start;
		selected_checkpoint.checkpoint = input->native_checkpoint;
		memcpy(selected_checkpoint.native_window, input->native_window, 160);
		detail = cluster_cold_scan_root_v1(plan, 0, &input->checkpoint, &token, false, true, NULL,
										   result);
	} else
		detail = cluster_cold_scan_input_v1(plan, 0, SCOPE, 0, false, true, NULL, result);
	cluster_cold_plan_destroy_v1(&plan);
	return detail;
}

UT_TEST(test_original_checkpointer_window_survives_both_complete_io_routes)
{
	const uint64 next[] = { UINT64_C(0x800000064), UINT64_C(0x1fffffff0) };
	const uint64 late[] = { UINT64_C(0x800000089), UINT64_C(0x200000003) };

	for (unsigned route = 0; route < 2; route++)
		for (unsigned sample = 0; sample < lengthof(next); sample++) {
			TestColdCheckpointPublisherResult published;
			ClusterColdScanResultV1 result;
			ClusterRelmapCheckpointWindow window;
			ClusterWalInputV1 *input
				= io_publisher_fixture(next[sample], late[sample], 0, 0x1100, &published);
			ClusterWalInputV1 saved = *input;
			uint64 issued = 0;

			UT_ASSERT(published.captured && published.finished);
			UT_ASSERT_EQ(published.locks_remaining, 0);
			UT_ASSERT_EQ(published.interrupt_errors, 0);
			UT_ASSERT_EQ(published.assertion_failed, 0);
			UT_ASSERT_EQ(published.before_issued, next[sample] - 1);
			UT_ASSERT_EQ(published.after_issued, late[sample]);
			UT_ASSERT_EQ(published.allocation_next, late[sample] + 0x1000);
			UT_ASSERT_EQ(io_publisher_scan(input, route, &result), CLUSTER_COLD_OK);
			UT_ASSERT_EQ(result.records, 2);
			UT_ASSERT_EQ(memcmp(result.native_window, input->native_window, 160), 0);
			UT_ASSERT_EQ(memcmp(input, &saved, sizeof(saved)), 0);
			UT_ASSERT(cluster_relmap_checkpoint_window_decode_v2(
				result.native_window, &input->native_checkpoint, input->checkpoint_start, 0x1100,
				&input->source.claim.identity, 3, &window, &issued));
			UT_ASSERT_EQ(issued, late[sample]);
			UT_ASSERT_EQ(window.last_issued, next[sample] - 1);
			UT_ASSERT_EQ(window.allocation_next, next[sample]);
			/* Original ordered WAL fold is distinct from this exported bound. */
			UT_ASSERT_EQ(result.history_xid.next_xid, route ? 0 : next[sample]);
			UT_ASSERT_EQ(result.history_xid.begun, route == 0);
			UT_ASSERT(!result.history_xid.failed);
		}
}

UT_TEST(test_original_checkpointer_io_refuses_mismatch_and_incomplete_visits)
{
	const uint8 zero[160] = { 0 };

	for (unsigned route = 0; route < 2; route++)
		for (unsigned fault = 0; fault < 8; fault++) {
			TestColdCheckpointPublisherResult published;
			ClusterColdScanResultV1 result;
			ClusterWalInputV1 *input = io_publisher_fixture(
				UINT64_C(0x800000064), UINT64_C(0x800000089), 0, 0x1100 + (fault == 0), &published);

			UT_ASSERT(published.captured && published.finished);
			UT_ASSERT_EQ(published.assertion_failed, 0);
			if (fault == 1)
				visit_checkpoints[0].nextOid++;
			if (fault == 2)
				input->source.claim.identity.origin_owner_incarnation++;
			if (fault == 3)
				input->source.claim.database_incarnation++;
			if (fault == 4)
				visit_result = retained_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			if (fault == 5)
				observed_records_delta = 1;
			if (fault == 6)
				observed_end--;
			if (fault == 7)
				decode_refusal_at[1] = CLUSTER_COLD_OPCODE_UNSUPPORTED;
			UT_ASSERT_EQ(io_publisher_scan(input, route, &result),
						 fault == 7 ? CLUSTER_COLD_OPCODE_UNSUPPORTED : CLUSTER_COLD_SOURCE_GAP);
			UT_ASSERT_EQ(memcmp(result.native_window, zero, 160), 0);
			UT_ASSERT_EQ(result.history_xid.next_xid, 0);
		}
}

UT_TEST(test_original_checkpointer_unknown_io_keeps_only_original_legacy_fold)
{
	const uint8 zero[160] = { 0 };

	for (unsigned route = 0; route < 2; route++)
		for (unsigned fault = 0; fault < 8; fault++) {
			TestColdCheckpointPublisherResult published;
			ClusterColdScanResultV1 result;
			ClusterRelmapCheckpointWindow window;
			ClusterWalInputV1 *input = io_publisher_fixture(
				UINT64_C(0x800000064), UINT64_C(0x800000089), fault, 0x1100, &published);
			uint64 issued = 0;

			UT_ASSERT_EQ(published.finished, fault == 0);
			UT_ASSERT_EQ(published.assertion_failed, 0);
			UT_ASSERT_EQ(published.locks_remaining, 0);
			UT_ASSERT_EQ(published.interrupt_errors, 0);
			UT_ASSERT_EQ(io_publisher_scan(input, route, &result), CLUSTER_COLD_OK);
			UT_ASSERT_EQ(result.history_xid.next_xid, route ? 0 : UINT64_C(0x800000064));
			UT_ASSERT_EQ(memcmp(result.native_window, input->native_window, 160), 0);
			UT_ASSERT_EQ(cluster_relmap_checkpoint_window_decode_v2(
							 result.native_window, &input->native_checkpoint,
							 input->checkpoint_start, 0x1100, &input->source.claim.identity, 3,
							 &window, &issued),
						 fault == 0);
			if (fault) {
				UT_ASSERT_EQ(memcmp(result.native_window, zero, 160), 0);
				UT_ASSERT_EQ(issued, 0);
			} else
				UT_ASSERT_EQ(issued, UINT64_C(0x800000089));
		}
}
