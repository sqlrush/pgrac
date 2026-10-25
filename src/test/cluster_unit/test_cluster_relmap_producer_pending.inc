/* Original owner capture before fixture RELMAP WAL, then original cold read.
 * FullXid assignment and lower authority providers remain controlled.
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterRelmapOwnerSampleV2 producer_pending_sample;
static TestRelmapNativeOwnerResult producer_pending_capture;

static bool
producer_pending_capture_owner(const ClusterWalSourceRef *source, ClusterRelmapV2Owner *out)
{
	producer_pending_capture = test_relmap_native_owner_capture(
		source, custody_era_offset + 99, 97, 0, &producer_pending_sample);
	if (!producer_pending_capture.captured)
		return false;
	*out = producer_pending_sample.owner;
	return true;
}

static bool
producer_pending_setup(bool shared, TestColdCheckpointPublisherResult *published)
{
	ClusterWalInputsV1 *inputs;
	ClusterWalInputV1 selected;
	ControlRootImage root;
	ClusterRecoveryAnchorRefV2 ref = { 0 };
	ClusterRecoveryAnchorV2 anchor;
	uint8 bytes[66048], image[512], window[160];
	char path[MAXPGPATH], hex[65];
	/* Independent fixture observations precede pending/WAL creation. The
	 * original writer requires boot99 and admitted99 separately. */
	const TestColdCheckpointWriterV1 writer = { 97, 99 };

	checkpoint_pending_saved_shared = custody_shared;
	custody_shared = shared;
	cold_native_real_scope = cold_native_real_clog = true;
	custody_original_owner_capture = producer_pending_capture_owner;
	inputs = origin_build_setup(29, false);
	custody_original_owner_capture = NULL;
	if (inputs == NULL) {
		UT_ASSERT(!inputs_pin_held);
		cold_native_real_scope = cold_native_real_clog = false;
		custody_shared = checkpoint_pending_saved_shared;
		cluster_node_id = custody_saved_node;
		custody_era_offset = 0;
		test_relmap_native_owner_done();
		return false;
	}
	selected = *cluster_wal_inputs_at_v1(inputs, 0);
	UT_ASSERT_EQ(producer_pending_sample.owner.wal_end, 0);
	UT_ASSERT_EQ(custody_image.pending_owner.top_full_xid, custody_era_offset + 99);
	UT_ASSERT_EQ(custody_image.pending_owner.membership_epoch, 97);
	UT_ASSERT_EQ(custody_image.pending_owner.admitted_incarnation, 99);
	UT_ASSERT_EQ(custody_image.pending_owner.boot_incarnation, 99);
	UT_ASSERT(custody_image.pending_owner.wal_end > selected.native_checkpoint.redo);
	UT_ASSERT_EQ(memcmp(&producer_pending_sample.writer.ref, &selected.source,
						sizeof(selected.source)), 0);
	cluster_wal_inputs_release_v1(&inputs);
	UT_ASSERT(!inputs_pin_held);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, bytes, sizeof(bytes));
	UT_ASSERT_EQ(cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root),
				 0);
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
		prefix_test_checkpoint_end, custody_era_offset + 105, 0, &writer, window);
	memcpy(anchor.native_window, window, sizeof(anchor.native_window));
	v2_anchor_object(bytes, &anchor, &anchor.identity, path);
	v2_write_roots(bytes);
	UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	prefix_base_expected = *cluster_wal_inputs_at_v1(inputs, 0);
	UT_ASSERT_EQ(memcmp(&prefix_base_expected.source, &selected.source, sizeof(selected.source)), 0);
	UT_ASSERT_EQ(memcmp(prefix_base_expected.native_window, window, sizeof(window)), 0);
	execution_inputs_from_read_scope(&inputs);
	execution_pending_guard_fault = 0;
	execution_checkpoint_after_hook = NULL;
	UT_ASSERT(ClusterConfShmem == NULL);
	ClusterConfShmem = &cold_native_conf;
	test_relmap_native_clog_setup(test_root, 0, 99, TRANSACTION_STATUS_COMMITTED, 0x24);
	cold_original_scope_setup();
	return true;
}

static void
producer_pending_done(void)
{
	UT_ASSERT(custody_original_owner_capture == NULL);
	test_relmap_native_owner_done();
	checkpoint_pending_done();
}

UT_TEST(test_original_owner_sample_reaches_pending_and_native_read)
{
	for (unsigned sample = 0; sample < 4; sample++) {
		TestColdCheckpointPublisherResult published;
		TestRelmapNativeOwnerResult checked;
		ClusterRelmapRecoveryHandle *handle = NULL;
		ClusterRelmapRecoveryTerminalV1 terminal;
		ClusterRelmapV2View view;
		ClusterRelmapV2Image before;
		bool ready;

		custody_era_offset = sample & 2 ? UINT64CONST(1) << 32 : 0;
		ready = producer_pending_setup(sample & 1, &published);
		UT_ASSERT(ready);
		if (!ready)
			return;
		before = custody_image;
		UT_ASSERT(published.captured && published.finished);
		UT_ASSERT_EQ(producer_pending_capture.assignments, 1);
		UT_ASSERT_EQ(producer_pending_capture.syncs + producer_pending_capture.renames
					 + producer_pending_capture.assertion_failed, 0);
		checked = test_relmap_native_owner_check(&producer_pending_sample, 0);
		UT_ASSERT(checked.matched);
		UT_ASSERT_EQ(checked.assignments, 1);
		UT_ASSERT_EQ(checked.syncs + checked.renames + checked.assertion_failed, 0);
		view = custody_view(false);
		UT_ASSERT(cluster_relmap_recovery_prepare_cold_v1(&custody_image.identity, &view, &handle));
		UT_ASSERT(handle != NULL);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
		UT_ASSERT(cluster_relmap_recovery_terminal_read(handle, &custody_image.identity, &view,
														&terminal));
		UT_ASSERT_EQ(terminal.top_full_xid, custody_era_offset + 99);
		UT_ASSERT_EQ(terminal.bounds.membership_epoch, 97);
		UT_ASSERT_EQ(terminal.bounds.boot_incarnation, 99);
		UT_ASSERT_EQ(terminal.bounds.admitted_incarnation, 99);
		UT_ASSERT_EQ(terminal.bounds.native_floor, custody_era_offset + 79);
		UT_ASSERT_EQ(terminal.bounds.last_issued, published.before_issued);
		UT_ASSERT_EQ(terminal.status, TRANSACTION_STATUS_COMMITTED);
		UT_ASSERT_EQ(prefix_base_reads, 2);
		UT_ASSERT_EQ(memcmp(&custody_image, &before, sizeof(before)), 0);
		cluster_relmap_recovery_handle_release_v1(&handle);
		UT_ASSERT(handle == NULL && cold_original_serial.held && cold_original_plan != NULL);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 0);
		producer_pending_done();
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_original_owner_capture_refusal_preserves_pending_and_scope)
{
	for (unsigned fault = 1; fault <= 10; fault++) {
		TestColdCheckpointPublisherResult published;
		ClusterRelmapOwnerSampleV2 out, zero = { 0 };
		TestRelmapNativeOwnerResult result;
		ClusterRelmapV2Image before;
		bool ready = producer_pending_setup(false, &published);
		UT_ASSERT(ready);
		if (!ready)
			return;
		before = custody_image;
		memset(&out, 0xa5, sizeof(out));
		result = test_relmap_native_owner_capture(&producer_pending_sample.writer.ref, 99, 97,
												fault, &out);
		UT_ASSERT(!result.captured);
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(zero)), 0);
		UT_ASSERT_EQ(result.assignments, fault <= 4 ? 0 : 1);
		UT_ASSERT_EQ(result.syncs + result.renames + result.assertion_failed, 0);
		UT_ASSERT_EQ(prefix_base_reads + test_relmap_native_clog_stats().calls, 0);
		UT_ASSERT_EQ(memcmp(&custody_image, &before, sizeof(before)), 0);
		UT_ASSERT(cold_original_serial.held && cold_original_plan != NULL);
		producer_pending_done();
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_original_owner_recheck_never_adopts_or_reassigns)
{
	for (unsigned fault = 1; fault <= 7; fault++) {
		TestColdCheckpointPublisherResult published;
		TestRelmapNativeOwnerResult result;
		ClusterRelmapOwnerSampleV2 before;
		ClusterRelmapV2Image image;
		bool ready = producer_pending_setup(false, &published);
		UT_ASSERT(ready);
		if (!ready)
			return;
		before = producer_pending_sample;
		image = custody_image;
		result = test_relmap_native_owner_check(&producer_pending_sample, fault);
		UT_ASSERT(!result.matched);
		UT_ASSERT_EQ(result.assignments, 1);
		UT_ASSERT_EQ(result.syncs + result.renames + result.assertion_failed, 0);
		UT_ASSERT_EQ(memcmp(&producer_pending_sample, &before, sizeof(before)), 0);
		UT_ASSERT_EQ(memcmp(&custody_image, &image, sizeof(image)), 0);
		UT_ASSERT_EQ(prefix_base_reads + test_relmap_native_clog_stats().calls, 0);
		UT_ASSERT(cold_original_serial.held && cold_original_plan != NULL);
		producer_pending_done();
		if (ut_current_failed)
			return;
	}
}
