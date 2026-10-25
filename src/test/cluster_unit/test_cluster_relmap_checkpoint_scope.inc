/*-------------------------------------------------------------------------
 * Actual Checkpointer window under the original cold scope and SIDE plan.
 * Lower authority facilities and ROOT publication remain controlled.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_relmap_checkpoint_scope.inc
 * NOTES
 *   PGRAC-original; only provisional BUILDING inputs are exercised here.
 *-------------------------------------------------------------------------
 */

static void
checkpoint_scope_setup(bool wrong_end, TestColdCheckpointPublisherResult *published,
					   uint8 window[160])
{
	ClusterWalInputsV1 *inputs;

	/* Select the existing typed SCN/TT terminal before writing fixture WAL.
	 * Do not change pending owner identity to match the new window. */
	cold_native_real_scope = true;
	inputs = retained_publisher_fixture(0, wrong_end, published, window);
	execution_inputs_from_read_scope(&inputs);
	UT_ASSERT(inputs == NULL && !inputs_pin_held && inputs_pin_refuse);
	UT_ASSERT(ClusterConfShmem == NULL);
	ClusterConfShmem = &cold_native_conf;
	cold_original_scope_setup();
}

static void
checkpoint_scope_done(void)
{
	UT_ASSERT(!inputs_pin_held && inputs_pin_refuse && test_actual_cf == NoLock);
	UT_ASSERT(cold_original_plan != NULL && cold_original_serial.held);
	UT_ASSERT_EQ(prefix_base_reads + custody_terminal_calls + custody_inventory_calls, 0);
	UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
	cold_original_scope_done();
	ClusterConfShmem = NULL;
	cold_native_real_scope = false;
	execution_build_done();
}

UT_TEST(test_actual_checkpointer_original_scope_builds_same_window_and_source)
{
	for (unsigned era = 0; era < 2; era++) {
		TestColdCheckpointPublisherResult published;
		ClusterRelmapRetentionDirectory directory = { 0 };
		ClusterRelmapRecoverySourceObservationV1 source = { 0 };
		ClusterWalRecoveryCheckpointV1 checkpoint;
		ClusterRelmapCheckpointWindow decoded;
		uint8 window[160];
		uint64 post_issued = 0, serial;

		custody_era_offset = (uint64)era << 32;
		checkpoint_scope_setup(false, &published, window);
		serial = cold_original_id();
		UT_ASSERT(published.captured && published.finished && serial != 0);
		UT_ASSERT_EQ(cluster_control_root_recovery_checkpoint_v1(&execution_root, &execution_token,
																 &checkpoint),
					 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(memcmp(checkpoint.native_window, window, sizeof(window)), 0);
		UT_ASSERT(cluster_relmap_checkpoint_window_decode_v2(
			window, &checkpoint.checkpoint, checkpoint.checkpoint_start, prefix_test_checkpoint_end,
			&checkpoint.source.claim.identity, checkpoint.source.claim.database_incarnation,
			&decoded, &post_issued));
		UT_ASSERT_EQ(post_issued, published.after_issued);
		UT_ASSERT(
			cluster_relmap_recovery_retention_build_start_cold_source_v1(0, &directory, &source));
		UT_ASSERT_EQ(directory.state, CLUSTER_RELMAP_RETENTION_BUILDING);
		UT_ASSERT_EQ(directory.generation, serial);
		UT_ASSERT_EQ(source.scope_serial, serial);
		UT_ASSERT_EQ(directory.native_floor, custody_era_offset + 79);
		UT_ASSERT_EQ(directory.last_issued, published.before_issued);
		UT_ASSERT_EQ(directory.truncate_serial, 0);
		UT_ASSERT_EQ(memcmp(&source.source, &checkpoint.source, sizeof(source.source)), 0);
		UT_ASSERT_EQ(memcmp(&source.checkpoint, &execution_root, sizeof(execution_root)), 0);
		UT_ASSERT_EQ(source.native_redo, checkpoint.checkpoint.redo);
		UT_ASSERT(test_relmap_scope_single_source(&source.source, &source.checkpoint,
												  source.native_redo, source.scope_serial));
		UT_ASSERT_EQ(
			cluster_relmap_retention_allocation_check(&directory, directory.last_issued + 1),
			CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(execution_visits, 1);
		UT_ASSERT_EQ(execution_records, 0); /* Controlled visitor was bypassed. */
		checkpoint_scope_done();
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_actual_checkpointer_original_scope_refusal_keeps_outputs_private)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		TestColdCheckpointPublisherResult published;
		ClusterRelmapRetentionDirectory directory = { 0 }, zero_directory = { 0 };
		ClusterRelmapRecoverySourceObservationV1 source = { 0 }, zero_source = { 0 };
		uint8 window[160];
		volatile bool threw = false;

		checkpoint_scope_setup(fault == 0, &published, window);
		UT_ASSERT(published.captured && published.finished && cold_original_id() != 0);
		if (fault == 1)
			cold_original_serial.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		if (fault == 2)
			cold_original_serial.held = false;
		if (fault == 3)
			cold_original_serial.release_uncertain = true;
		if (fault == 4)
			cold_authority_fault = 2;
		if (fault == 5)
			cold_authority_fault = 6;
		test_capture_error_level = true;
		PG_TRY();
		{
			UT_ASSERT(!cluster_relmap_recovery_retention_build_start_cold_source_v1(0, &directory,
																					&source));
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT_EQ(threw, fault == 5);
		UT_ASSERT_EQ(memcmp(&directory, &zero_directory, sizeof(directory)), 0);
		UT_ASSERT_EQ(memcmp(&source, &zero_source, sizeof(source)), 0);
		UT_ASSERT(cold_original_plan != NULL && !inputs_pin_held && inputs_pin_refuse);
		cold_original_serial.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		cold_original_serial.held = true;
		cold_original_serial.release_uncertain = false;
		cold_authority_fault = 0;
		UT_ASSERT(cold_original_id() != 0);
		checkpoint_scope_done();
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_actual_checkpointer_scope_reentry_requires_fresh_building_outputs)
{
	TestColdCheckpointPublisherResult published;
	ClusterRelmapRetentionDirectory old = { 0 }, saved, fresh = { 0 };
	ClusterRelmapRecoverySourceObservationV1 source = { 0 }, saved_source, next = { 0 };
	uint8 window[160];
	uint64 before, after;

	checkpoint_scope_setup(false, &published, window);
	before = cold_original_id();
	UT_ASSERT(published.captured && published.finished && before != 0);
	UT_ASSERT(cluster_relmap_recovery_retention_build_start_cold_source_v1(0, &old, &source));
	saved = old;
	saved_source = source;
	cluster_undo_recovery_scope_leave_v1(&cold_original_scope);
	UT_ASSERT(cluster_undo_recovery_scope_enter_v1(&cold_original_scope, &cold_original_authority,
												   cold_original_plan));
	after = cold_original_id();
	UT_ASSERT(after > before);
	UT_ASSERT(!test_relmap_scope_single_source(&source.source, &source.checkpoint,
											   source.native_redo, source.scope_serial));
	UT_ASSERT(!cluster_relmap_recovery_retention_build_start_cold_source_v1(0, &old, &next));
	UT_ASSERT(!cluster_relmap_recovery_retention_build_start_cold_source_v1(0, &fresh, &source));
	UT_ASSERT_EQ(memcmp(&old, &saved, sizeof(old)), 0);
	UT_ASSERT_EQ(memcmp(&source, &saved_source, sizeof(source)), 0);
	UT_ASSERT_EQ(fresh.state, CLUSTER_RELMAP_RETENTION_UNKNOWN);
	UT_ASSERT_EQ(next.scope_serial, 0);
	UT_ASSERT(cluster_relmap_recovery_retention_build_start_cold_source_v1(0, &fresh, &next));
	UT_ASSERT_EQ(fresh.state, CLUSTER_RELMAP_RETENTION_BUILDING);
	UT_ASSERT_EQ(fresh.generation, after);
	UT_ASSERT_EQ(next.scope_serial, after);
	UT_ASSERT_EQ(memcmp(&next.source, &source.source, sizeof(next.source)), 0);
	UT_ASSERT_EQ(memcmp(&next.checkpoint, &source.checkpoint, sizeof(next.checkpoint)), 0);
	UT_ASSERT_EQ(next.native_redo, source.native_redo);
	UT_ASSERT_EQ(fresh.native_floor, saved.native_floor);
	UT_ASSERT_EQ(fresh.last_issued, saved.last_issued);
	checkpoint_scope_done();
}
