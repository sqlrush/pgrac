/* Original Start requires staging bytes and terminal custody independently.
 * Declaration and authority leaves remain controlled, not joint qualification.
 * Author: SqlRush <sqlrush@gmail.com> */
bool
test_relmap_start_physical_recheck(const struct ClusterRecoveryStartV1 *start)
{
	return cluster_cold_relmap_replay_recheck_v1(start);
}

static ColdParticipant staging_terminal_participant;
static uint32 staging_terminal_canonical;

static void
staging_terminal_setup(bool aborted, bool reverse)
{
	terminal_batch_setup(aborted, reverse);
	memset(&staging_terminal_participant, 0, sizeof(staging_terminal_participant));
	staging_terminal_participant.cut = terminal_typed.participants[0];
	staging_terminal_canonical = 0;
	terminal_plan.participants = &staging_terminal_participant;
	terminal_plan.canonical = &staging_terminal_canonical;
	for (unsigned i = 0; i < 2; ++i)
		terminal_batch[i].input.frame.source_xid
			= (TransactionId)terminal_batch[i].input.staged.pending_owner.top_full_xid;
}

static void
staging_terminal_change_file(bool changed_image)
{
	ClusterRelmapV2Image changed = terminal_batch[1].input.staged;
	int fd = open(test_root, O_RDONLY | O_DIRECTORY);
	const char *path = changed.identity.shared ? cold_native_paths[1] : cold_native_paths[3];

	UT_ASSERT(fd >= 0);
	if (changed_image) {
		changed.pending.mappings[0].mapfilenumber++;
		relmap_test_crc(&changed.pending);
		relmap_census_file(fd, path, &changed);
	} else {
		int copy = openat(fd, path, O_WRONLY);
		uint8 bad = 0;
		UT_ASSERT(copy >= 0);
		UT_ASSERT_EQ(pwrite(copy, &bad, 1, 0), 1);
		UT_ASSERT_EQ(close(copy), 0);
	}
	UT_ASSERT_EQ(close(fd), 0);
}

UT_TEST(test_start_staging_and_terminal_both_required_for_complete_batch)
{
	for (unsigned sample = 0; sample < 4; ++sample) {
		ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
		staging_terminal_setup((sample & 1) != 0, (sample & 2) != 0);
		UT_ASSERT(cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
		if (terminal_bind_original_start()) {
			unsigned reads = prefix_base_reads;
			UT_ASSERT(test_relmap_start_staging(terminal_start));
			UT_ASSERT(cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
			UT_ASSERT_EQ(out->count, 2);
			UT_ASSERT(test_relmap_start_staging(terminal_start));
			UT_ASSERT(cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
			UT_ASSERT_EQ(out->count, 2);
			UT_ASSERT_EQ(prefix_base_reads, reads);
			UT_ASSERT(test_relmap_start_pending() && !terminal_pins.transferred);
		} else UT_ASSERT(false);
		free(out); terminal_done();
	}
}

UT_TEST(test_start_staging_refusal_does_not_borrow_valid_terminal)
{
	for (unsigned fault = 0; fault < 2; ++fault) {
		ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
		staging_terminal_setup(false, false);
		UT_ASSERT(cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
		if (terminal_bind_original_start()) {
			UT_ASSERT(cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
			UT_ASSERT_EQ(out->count, 2);
			staging_terminal_change_file(fault != 0);
			UT_ASSERT(!test_relmap_start_staging(terminal_start));
			UT_ASSERT(!test_relmap_start_current(terminal_start));
			UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
			UT_ASSERT(terminal_batch_output_empty(out));
			UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 2);
			UT_ASSERT(!terminal_pins.transferred && terminal_pins.count == 2);
		} else UT_ASSERT(false);
		free(out); terminal_done();
	}
}

UT_TEST(test_start_terminal_refusal_does_not_borrow_valid_staging)
{
	ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
	staging_terminal_setup(true, true);
	UT_ASSERT(cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
	if (terminal_bind_original_start()) {
		UT_ASSERT(test_relmap_start_staging(terminal_start));
		UT_ASSERT(cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
		test_relmap_native_clog_reopen_fault(terminal_last_native, 7);
		UT_ASSERT(test_relmap_start_staging(terminal_start));
		UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
		UT_ASSERT(terminal_batch_output_empty(out));
		UT_ASSERT(!test_relmap_start_current(terminal_start));
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 2);
		UT_ASSERT(!terminal_pins.transferred && terminal_pins.count == 2);
	} else UT_ASSERT(false);
	free(out); terminal_done();
}

UT_TEST(test_start_staging_repeat_rereads_the_second_copy)
{
	ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
	staging_terminal_setup(false, true);
	UT_ASSERT(cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
	if (terminal_bind_original_start()) {
		UT_ASSERT(test_relmap_start_staging(terminal_start));
		UT_ASSERT(cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
		staging_terminal_change_file(true);
		UT_ASSERT(!test_relmap_start_staging(terminal_start));
		UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
		UT_ASSERT(terminal_batch_output_empty(out));
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 2);
		UT_ASSERT(!terminal_pins.transferred && terminal_pins.count == 2);
	} else UT_ASSERT(false);
	free(out); terminal_done();
}
