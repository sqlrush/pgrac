/* Complete namespace batches through real terminal custody and original Start.
 * SIDE declarations remain the existing controlled leaf; no all-origin grant.
 * Author: SqlRush <sqlrush@gmail.com> */
static ColdRelmapInput terminal_batch[2];

static bool
terminal_batch_output_empty(const ClusterColdRelmapTerminalCheckV1 *out)
{
	const uint8 *bytes = (const uint8 *)out;

	for (Size i = 0; i < sizeof(*out); ++i)
		if (bytes[i] != 0) return false;
	return true;
}

static void
terminal_batch_setup(bool aborted, bool reverse)
{
	int fd;

	custody_second_namespace = true;
	terminal_setup(aborted);
	custody_second_namespace = false;
	terminal_batch[0] = terminal_batch[1] = terminal_input;
	terminal_batch[0].input.frame.read_rec_ptr = prefix_base_expected.native_redo;
	terminal_batch[1].input.staged = custody_second_image;
	terminal_batch[1].input.frame.read_rec_ptr = custody_second_record.record_start;
	terminal_batch[1].input.frame.end_rec_ptr = custody_second_record.exclusive_end;
	/* The same recovered transaction changed both namespaces. Each has its
	 * own actual staged pair and original conservative pin identity. */
	fd = open(test_root, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(fd >= 0);
	for (unsigned i = 0; i < 2; ++i)
		relmap_census_file(fd, cold_native_paths[i], &terminal_batch[1].input.staged);
	UT_ASSERT_EQ(close(fd), 0);
	if (reverse) {
		ColdRelmapInput saved = terminal_batch[0];
		terminal_batch[0] = terminal_batch[1];
		terminal_batch[1] = saved;
	}
	terminal_plan.relmap_inputs = terminal_batch;
	terminal_plan.relmap_count = 2;
	terminal_pins.count = terminal_pins.expected = 2;
	for (unsigned i = 0; i < 2; ++i) {
		terminal_pins.attempted[i] = terminal_pins.staged[i] = true;
		terminal_pins.images[i] = terminal_batch[i].input.staged;
	}
}

UT_TEST(test_terminal_batch_both_namespaces_commit_abort_and_order)
{
	for (unsigned sample = 0; sample < 4; ++sample) {
		ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
		unsigned reads, syncs;
		bool aborted = (sample & 1) != 0;

		terminal_batch_setup(aborted, (sample & 2) != 0);
		UT_ASSERT(cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 2);
		if (terminal_bind_original_start()) {
			reads = prefix_base_reads; syncs = test_relmap_native_clog_stats().file_syncs;
			UT_ASSERT(cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
			UT_ASSERT_EQ(out->count, 2);
			for (unsigned i = 0; i < 2; ++i) {
				const ClusterColdRelmapInputV1 *input = &terminal_batch[i].input;
				UT_ASSERT_EQ(out->entries[i].participant, input->participant);
				UT_ASSERT_EQ(out->entries[i].status, aborted ?
					TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
				UT_ASSERT_EQ(out->entries[i].top_full_xid, input->staged.pending_owner.top_full_xid);
				UT_ASSERT_EQ(out->entries[i].generation, input->staged.pending_generation);
				UT_ASSERT_EQ(memcmp(&out->entries[i].identity, &input->staged.identity,
					sizeof(input->staged.identity)), 0);
				UT_ASSERT_EQ(memcmp(&out->entries[i].staged, &input->staged, sizeof(input->staged)), 0);
			}
			UT_ASSERT(cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
			UT_ASSERT_EQ(out->count, 2);
			UT_ASSERT_EQ(prefix_base_reads, reads);
			UT_ASSERT_EQ(test_relmap_native_clog_stats().file_syncs, syncs);
			UT_ASSERT(test_relmap_start_pending() && !terminal_pins.transferred);
		} else UT_ASSERT(false);
		free(out); terminal_done();
	}
}

UT_TEST(test_terminal_batch_second_fd_loss_withdraws_whole_output)
{
	ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
	unsigned reads, syncs;

	terminal_batch_setup(false, false);
	UT_ASSERT(cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
	if (terminal_bind_original_start()) {
		UT_ASSERT(cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
		UT_ASSERT_EQ(out->count, 2);
		/* Only the second held descriptor is replaced with identical bytes in
		 * a distinct inode. The first descriptor still names the old file. */
		test_relmap_native_clog_reopen_fault(terminal_last_native, 7);
		reads = prefix_base_reads; syncs = test_relmap_native_clog_stats().file_syncs;
		UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
		UT_ASSERT(terminal_batch_output_empty(out));
		UT_ASSERT(!test_relmap_start_current(terminal_start));
		UT_ASSERT(!terminal_pins.transferred && terminal_pins.count == 2);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 2);
		UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
		UT_ASSERT_EQ(prefix_base_reads, reads);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().file_syncs, syncs);
	} else UT_ASSERT(false);
	free(out); terminal_done();
}

UT_TEST(test_terminal_batch_second_saved_image_drift_keeps_all_debt)
{
	ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));

	terminal_batch_setup(true, true);
	UT_ASSERT(cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
	if (terminal_bind_original_start()) {
		UT_ASSERT(cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
		UT_ASSERT_EQ(out->count, 2);
		terminal_batch[1].input.staged.pending_generation++;
		UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
		UT_ASSERT(terminal_batch_output_empty(out));
		UT_ASSERT(!test_relmap_start_current(terminal_start));
		UT_ASSERT(!terminal_pins.transferred && terminal_pins.count == 2);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 2);
		UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
	} else UT_ASSERT(false);
	free(out); terminal_done();
}

UT_TEST(test_terminal_batch_partial_capture_error_never_enters_start)
{
	volatile bool threw = false;
	unsigned reads;

	terminal_batch_setup(false, false);
	terminal_capture_throw_after_detach = true;
	PG_TRY(); {
		UT_ASSERT(!cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
	} PG_CATCH(); { threw = true; } PG_END_TRY();
	terminal_capture_throw_after_detach = false;
	UT_ASSERT(threw && terminal_plan.relmap_failed);
	UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
	reads = prefix_base_reads;
	UT_ASSERT(!cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
	UT_ASSERT_EQ(prefix_base_reads, reads);
	UT_ASSERT(!terminal_bind_original_start());
	UT_ASSERT(terminal_start == NULL);
	UT_ASSERT(!terminal_pins.transferred && terminal_pins.count == 2);
	UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
	terminal_done();
}
