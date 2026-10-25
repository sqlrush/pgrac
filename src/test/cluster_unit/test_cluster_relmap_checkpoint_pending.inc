/*-------------------------------------------------------------------------
 * Actual Checkpointer window and the original pending/native read path.
 * Writer observations and lower authority facilities remain controlled.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_relmap_checkpoint_pending.inc
 * NOTES
 *   PGRAC-original; no pair mutation or completion qualification here.
 *-------------------------------------------------------------------------
 */
static bool checkpoint_pending_saved_shared;

static void
checkpoint_pending_setup(bool shared, TestColdCheckpointWriterV1 writer, unsigned fault,
						 XidStatus status, TestColdCheckpointPublisherResult *published)
{
	ClusterWalInputsV1 *inputs;
	ClusterWalInputV1 selected;
	ControlRootImage root;
	ClusterRecoveryAnchorRefV2 ref = { 0 };
	ClusterRecoveryAnchorV2 anchor;
	uint8 bytes[66048], image[512], window[160];
	char path[MAXPGPATH], hex[65];

	/* Both independent fixture identities are fixed before WAL creation.
	 * Never copy a decoded window into the original pending owner. */
	checkpoint_pending_saved_shared = custody_shared;
	custody_shared = shared;
	cold_native_real_scope = cold_native_real_clog = true;
	inputs = origin_build_setup(29, false);
	selected = *cluster_wal_inputs_at_v1(inputs, 0);
	UT_ASSERT_EQ(custody_image.pending_owner.membership_epoch, 97);
	UT_ASSERT_EQ(custody_image.pending_owner.boot_incarnation, 411);
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
	*published = test_cold_checkpoint_publish_identity(
		&selected.source, &selected.native_checkpoint, selected.checkpoint_start,
		prefix_test_checkpoint_end, custody_era_offset + 105, fault, &writer, window);
	memcpy(anchor.native_window, window, sizeof(anchor.native_window));
	v2_anchor_object(bytes, &anchor, &anchor.identity, path);
	v2_write_roots(bytes);
	UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	prefix_base_expected = *cluster_wal_inputs_at_v1(inputs, 0);
	UT_ASSERT_EQ(memcmp(&prefix_base_expected.source, &selected.source, sizeof(selected.source)),
				 0);
	UT_ASSERT_EQ(memcmp(&prefix_base_expected.native_checkpoint, &selected.native_checkpoint,
						sizeof(selected.native_checkpoint)),
				 0);
	UT_ASSERT_EQ(memcmp(prefix_base_expected.native_window, window, sizeof(window)), 0);
	execution_inputs_from_read_scope(&inputs);
	execution_pending_guard_fault = 0;
	execution_checkpoint_after_hook = NULL;
	UT_ASSERT(ClusterConfShmem == NULL);
	ClusterConfShmem = &cold_native_conf;
	test_relmap_native_clog_setup(test_root, 0, 99, status, 0x24);
	cold_original_scope_setup();
}

static void
checkpoint_pending_done(void)
{
	TestRelmapNativeClogStats stats = test_relmap_native_clog_done();
	UT_ASSERT_EQ(stats.open_fds + stats.assertion_failed, 0);
	UT_ASSERT(cold_original_serial.held && cold_original_plan != NULL);
	UT_ASSERT(!inputs_pin_held && inputs_pin_refuse && test_actual_cf == NoLock);
	UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
	cold_original_scope_done();
	ClusterConfShmem = NULL;
	cold_native_real_scope = cold_native_real_clog = false;
	custody_shared = checkpoint_pending_saved_shared;
	execution_build_done();
}

UT_TEST(test_actual_checkpointer_pending_uses_original_scope_and_held_native_read)
{
	for (unsigned sample = 0; sample < 4; sample++) {
		TestColdCheckpointPublisherResult published;
		ClusterRelmapRecoveryHandle *handle = NULL;
		ClusterRelmapRecoverySourceObservationV1 source;
		ClusterRelmapRecoveryTerminalV1 terminal;
		ClusterRelmapV2View view;
		ClusterRelmapV2Image before;

		custody_era_offset = sample & 2 ? UINT64CONST(1) << 32 : 0;
		checkpoint_pending_setup(sample & 1, (TestColdCheckpointWriterV1){ 97, 411 }, 0,
								 TRANSACTION_STATUS_COMMITTED, &published);
		before = custody_image;
		view = custody_view(false);
		UT_ASSERT(published.captured && published.finished);
		UT_ASSERT(cluster_relmap_recovery_prepare_cold_v1(&custody_image.identity, &view, &handle));
		UT_ASSERT(handle != NULL);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
		UT_ASSERT(cluster_relmap_recovery_handle_source_v1(handle, &source));
		UT_ASSERT_EQ(source.scope_serial, cold_original_id());
		UT_ASSERT_EQ(memcmp(&source.checkpoint, &execution_root, sizeof(execution_root)), 0);
		UT_ASSERT(
			cluster_relmap_recovery_handle_matches_v1(handle, &custody_image.identity, &view));
		UT_ASSERT(cluster_relmap_recovery_terminal_read(handle, &custody_image.identity, &view,
														&terminal));
		UT_ASSERT_EQ(terminal.top_full_xid, custody_era_offset + 99);
		UT_ASSERT_EQ(terminal.bounds.membership_epoch, 97);
		UT_ASSERT_EQ(terminal.bounds.boot_incarnation, 411);
		UT_ASSERT_EQ(terminal.bounds.native_floor, custody_era_offset + 79);
		UT_ASSERT_EQ(terminal.bounds.last_issued, published.before_issued);
		UT_ASSERT_EQ(terminal.status, TRANSACTION_STATUS_COMMITTED);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
		UT_ASSERT_EQ(prefix_base_reads, 3);
		UT_ASSERT_EQ(memcmp(&custody_image, &before, sizeof(before)), 0);
		cluster_relmap_recovery_handle_release_v1(&handle);
		UT_ASSERT(handle == NULL && cold_original_plan != NULL && cold_original_serial.held);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 0);
		checkpoint_pending_done();
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_actual_checkpointer_pending_refuses_independent_writer_mismatch)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		TestColdCheckpointPublisherResult published;
		ClusterRelmapRecoveryHandle *handle = NULL;
		ClusterRelmapRetentionDirectory directory = { 0 };
		ClusterRelmapRecoverySourceObservationV1 source = { 0 };
		ClusterRelmapV2View view;
		ClusterRelmapV2Image before;

		checkpoint_pending_setup(
			false, (TestColdCheckpointWriterV1){ fault == 0 ? 98 : 97, fault == 1 ? 412 : 411 },
			fault == 2 ? 5 : 0, TRANSACTION_STATUS_COMMITTED, &published);
		before = custody_image;
		view = custody_view(false);
		UT_ASSERT_EQ(published.finished, fault != 2);
		UT_ASSERT_EQ(
			cluster_relmap_recovery_retention_build_start_cold_source_v1(0, &directory, &source),
			fault != 2);
		UT_ASSERT(
			!cluster_relmap_recovery_prepare_cold_v1(&custody_image.identity, &view, &handle));
		UT_ASSERT(handle == NULL && cold_original_id() != 0);
		UT_ASSERT_EQ(prefix_base_reads, 0);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().calls, 0);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 0);
		UT_ASSERT_EQ(memcmp(&custody_image, &before, sizeof(before)), 0);
		checkpoint_pending_done();
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_actual_checkpointer_pending_native_refusal_keeps_original_protection)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		TestColdCheckpointPublisherResult published;
		ClusterRelmapRecoveryHandle *handle = NULL;
		ClusterRelmapV2View view;
		ClusterRelmapV2Image before;
		volatile bool threw = false;

		checkpoint_pending_setup(
			false, (TestColdCheckpointWriterV1){ 97, 411 }, 0,
			fault == 0 ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED, &published);
		before = custody_image;
		view = custody_view(false);
		UT_ASSERT(published.captured && published.finished);
		if (fault != 0)
			test_relmap_native_clog_fault(fault == 1 ? 1 : 5, 1);
		PG_TRY();
		{
			UT_ASSERT(
				!cluster_relmap_recovery_prepare_cold_v1(&custody_image.identity, &view, &handle));
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(threw, fault == 2);
		UT_ASSERT(handle == NULL && cold_original_id() != 0);
		UT_ASSERT_EQ(prefix_base_reads, 1);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 0);
		UT_ASSERT_EQ(memcmp(&custody_image, &before, sizeof(before)), 0);
		checkpoint_pending_done();
		if (ut_current_failed)
			return;
	}
}
