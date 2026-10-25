/*-------------------------------------------------------------------------
 * Actual Checkpointer input through original ROOT/claim/retained WAL readers.
 * ROOT publication and cold/native authority remain fixture boundaries.
 * This composition grants only the existing provisional BUILDING input.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_relmap_checkpoint_publisher.inc
 * NOTES
 *   PGRAC-original; reuse the existing namespaced test bridge unchanged.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_cold_checkpoint_publisher.h"

static ClusterWalInputsV1 *
retained_publisher_fixture(unsigned fault, bool wrong_end,
						   TestColdCheckpointPublisherResult *published, uint8 window[160])
{
	ClusterWalInputsV1 *inputs = origin_build_setup(29, false);
	ClusterWalInputV1 selected = *cluster_wal_inputs_at_v1(inputs, 0);
	ControlRootImage root;
	ClusterRecoveryAnchorRefV2 ref = { 0 };
	ClusterRecoveryAnchorV2 anchor;
	uint8 bytes[66048], image[512];
	char path[MAXPGPATH], hex[65];

	/* Release only preliminary setup pins before fixture publication. The
	 * final original read scope below owns its pins until normal cleanup. */
	cluster_wal_inputs_release_v1(&inputs);
	UT_ASSERT(!inputs_pin_held);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, bytes, sizeof(bytes));
	UT_ASSERT_EQ(
		cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	ref.identity = selected.source.claim.identity;
	ref.database_incarnation = root.header.v2.database_incarnation;
	ref.max_config_generation = root.header.v2.config_generation;
	ref.anchor_generation = root.refs[0].anchor_generation;
	memcpy(ref.anchor_sha256, root.refs[0].anchor_sha256, 32);
	memcpy(ref.claim_sha256, root.refs[0].claim_sha256, 32);
	for (unsigned i = 0; i < 32; i++)
		snprintf(hex + i * 2, 3, "%02x", ref.anchor_sha256[i]);
	snprintf(path, sizeof(path),
			 "%s/global/anchor_images/thread_%u/generation_" UINT64_FORMAT "/anchor_" UINT64_FORMAT
			 "-%s.bin",
			 cluster_shared_data_dir, ref.identity.origin_thread_id,
			 ref.identity.origin_owner_incarnation, ref.anchor_generation, hex);
	read_all_or_abort(path, image, sizeof(image));
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(image, sizeof(image), &ref, &anchor), 0);
	*published = test_cold_checkpoint_publish(
		&selected.source, &selected.native_checkpoint, selected.checkpoint_start,
		prefix_test_checkpoint_end + wrong_end, custody_era_offset + 105, fault, window);
	memcpy(anchor.native_window, window, sizeof(anchor.native_window));
	/* Existing fixture writers, not the product publisher/CAS or WAL flush. */
	v2_anchor_object(bytes, &anchor, &anchor.identity, path);
	v2_write_roots(bytes);
	UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	prefix_base_expected = *cluster_wal_inputs_at_v1(inputs, 0);
	UT_ASSERT_EQ(memcmp(&prefix_base_expected.source, &selected.source, sizeof(selected.source)),
				 0);
	UT_ASSERT_EQ(memcmp(&prefix_base_expected.native_checkpoint, &selected.native_checkpoint,
						sizeof(selected.native_checkpoint)),
				 0);
	UT_ASSERT_EQ(prefix_base_expected.checkpoint_start, selected.checkpoint_start);
	UT_ASSERT_EQ(prefix_base_expected.native_redo, selected.native_redo);
	UT_ASSERT_EQ(memcmp(prefix_base_expected.native_window, window, 160), 0);
	execution_pending_guard_fault = 0;
	execution_checkpoint_after_hook = NULL;
	return inputs;
}

static bool
retained_publisher_build(ClusterWalInputsV1 **inputs, unsigned route,
						 ClusterRelmapRetentionDirectory *directory,
						 ClusterRelmapRecoverySourceObservationV1 *source)
{
	if (!route)
		return cluster_relmap_recovery_retention_build_start_v1(*inputs, 0, directory);
	return cluster_relmap_recovery_retention_build_start_cold_source_v1(0, directory, source);
}

static void
retained_publisher_done(ClusterWalInputsV1 **inputs, unsigned route)
{
	UT_ASSERT(test_actual_cf == NoLock);
	UT_ASSERT(route ? (!inputs_pin_held && inputs_pin_refuse)
					: (inputs_pin_held && inputs_pin_current));
	UT_ASSERT_EQ(prefix_base_reads + custody_terminal_calls + custody_inventory_calls, 0);
	UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
	if (route)
		execution_build_done();
	else
		origin_build_done(inputs);
}

UT_TEST(test_actual_checkpointer_reselected_window_builds_both_original_inputs)
{
	for (unsigned route = 0; route < 2; route++)
		for (unsigned era = 0; era < 2; era++) {
			TestColdCheckpointPublisherResult published;
			ClusterRelmapRetentionDirectory directory = { 0 };
			ClusterRelmapRecoverySourceObservationV1 source = { 0 };
			ClusterRelmapCheckpointWindow decoded;
			uint8 window[160];
			uint64 post_issued = 0;
			ClusterWalInputsV1 *inputs;

			custody_era_offset = (uint64)era << 32;
			inputs = retained_publisher_fixture(0, false, &published, window);
			UT_ASSERT(published.captured && published.finished);
			UT_ASSERT_EQ(published.locks_remaining + published.interrupt_errors
							 + published.assertion_failed,
						 0);
			UT_ASSERT_EQ(published.before_issued, custody_era_offset + 99);
			UT_ASSERT_EQ(published.after_issued, custody_era_offset + 105);
			UT_ASSERT_EQ(published.allocation_next, custody_era_offset + 4201);
			UT_ASSERT(cluster_relmap_checkpoint_window_decode_v2(
				window, &prefix_base_expected.native_checkpoint,
				prefix_base_expected.checkpoint_start, prefix_test_checkpoint_end,
				&prefix_base_expected.source.claim.identity,
				prefix_base_expected.source.claim.database_incarnation, &decoded, &post_issued));
			UT_ASSERT_EQ(post_issued, published.after_issued);
			if (route)
				execution_inputs_from_read_scope(&inputs);
			UT_ASSERT(retained_publisher_build(&inputs, route, &directory, &source));
			UT_ASSERT_EQ(directory.state, CLUSTER_RELMAP_RETENTION_BUILDING);
			UT_ASSERT_EQ(directory.generation, custody_scope);
			UT_ASSERT_EQ(directory.native_floor, custody_era_offset + 79);
			UT_ASSERT_EQ(directory.last_issued, published.before_issued);
			UT_ASSERT_EQ(directory.truncate_serial, 0);
			UT_ASSERT_EQ(
				cluster_relmap_retention_allocation_check(&directory, directory.last_issued + 1),
				CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
			if (route) {
				UT_ASSERT_EQ(
					memcmp(&source.source, &prefix_base_expected.source, sizeof(source.source)), 0);
				UT_ASSERT_EQ(memcmp(&source.checkpoint, &execution_root, sizeof(execution_root)),
							 0);
				UT_ASSERT_EQ(source.native_redo, prefix_base_expected.native_redo);
				UT_ASSERT_EQ(source.scope_serial, directory.generation);
				UT_ASSERT_EQ(execution_visits, 1);
				UT_ASSERT(execution_records >= 3);
			}
			retained_publisher_done(&inputs, route);
			if (ut_current_failed)
				return;
		}
}

UT_TEST(test_actual_checkpointer_unknown_window_keeps_original_factory_unavailable)
{
	for (unsigned route = 0; route < 2; route++)
		for (unsigned fault = 1; fault <= 7; fault++) {
			TestColdCheckpointPublisherResult published;
			ClusterRelmapRetentionDirectory directory = { 0 }, zero_directory = { 0 };
			ClusterRelmapRecoverySourceObservationV1 source = { 0 }, zero_source = { 0 };
			uint8 window[160], zero[160] = { 0 };
			ClusterWalInputsV1 *inputs
				= retained_publisher_fixture(fault, false, &published, window);

			UT_ASSERT(!published.finished);
			UT_ASSERT_EQ(published.captured, fault > 2);
			UT_ASSERT_EQ(published.locks_remaining + published.interrupt_errors
							 + published.assertion_failed,
						 0);
			UT_ASSERT_EQ(memcmp(window, zero, sizeof(window)), 0);
			if (route)
				execution_inputs_from_read_scope(&inputs);
			UT_ASSERT(!retained_publisher_build(&inputs, route, &directory, &source));
			UT_ASSERT_EQ(memcmp(&directory, &zero_directory, sizeof(directory)), 0);
			UT_ASSERT_EQ(memcmp(&source, &zero_source, sizeof(source)), 0);
			retained_publisher_done(&inputs, route);
			if (ut_current_failed)
				return;
		}
}

UT_TEST(test_actual_checkpointer_factory_refuses_end_source_and_late_owner_drift)
{
	for (unsigned route = 0; route < 2; route++)
		for (unsigned fault = 0; fault < 4; fault++) {
			TestColdCheckpointPublisherResult published;
			ClusterRelmapRetentionDirectory directory = { 0 }, zero_directory = { 0 };
			ClusterRelmapRecoverySourceObservationV1 source = { 0 }, zero_source = { 0 };
			uint8 window[160];
			ClusterWalInputsV1 *inputs
				= retained_publisher_fixture(0, fault == 0, &published, window);

			UT_ASSERT(published.captured && published.finished);
			if (route) {
				execution_inputs_from_read_scope(&inputs);
				if (fault > 0)
					execution_fault = fault == 1 ? 4 : fault == 2 ? 6 : 2;
			} else {
				ClusterWalInputV1 *input = (void *)cluster_wal_inputs_at_v1(inputs, 0);
				if (fault == 1)
					input->native_checkpoint.nextXid = FullTransactionIdFromU64(101);
				if (fault == 2)
					input->source.claim.database_incarnation++;
				if (fault == 3)
					origin_build_scope_change_after = 2;
			}
			UT_ASSERT(!retained_publisher_build(&inputs, route, &directory, &source));
			UT_ASSERT_EQ(memcmp(&directory, &zero_directory, sizeof(directory)), 0);
			UT_ASSERT_EQ(memcmp(&source, &zero_source, sizeof(source)), 0);
			if (route && fault == 3)
				UT_ASSERT(execution_records >= 3);
			retained_publisher_done(&inputs, route);
			if (ut_current_failed)
				return;
		}
}
