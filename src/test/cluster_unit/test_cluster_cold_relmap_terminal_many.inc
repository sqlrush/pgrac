/*-------------------------------------------------------------------------
 * test_cluster_cold_relmap_terminal_many.inc
 *     Complete native terminal batches span more than one receipt page.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_relmap_terminal_many.inc
 * NOTES
 *     Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
static ColdRelmapInput *terminal_many_inputs;
static ClusterRelmapRecoverySegment *terminal_many_overflow;
static uint32 terminal_many_count, terminal_many_creation_count;

static void
terminal_many_setup(uint32 count, bool aborted)
{
	int fd;
	UT_ASSERT(count > 64 && count <= 129);
	terminal_namespace_count = count;
	terminal_namespace_images = calloc(count, sizeof(*terminal_namespace_images));
	terminal_namespace_records = calloc(count, sizeof(*terminal_namespace_records));
	terminal_many_inputs = calloc(count, sizeof(*terminal_many_inputs));
	terminal_many_count = count;
#ifdef TEST_TERMINAL_REAL_START
	/* The original Start bridge intentionally retains its creation refusal.
	 * Its batch starts pending-only; the other target tests the mixed plan. */
	terminal_setup(aborted);
	terminal_many_creation_count = 0;
#else
	pending_terminal_setup(aborted);
	terminal_many_creation_count = 1;
#endif
	terminal_namespace_count = 0;
	fd = open(test_root, O_RDONLY | O_DIRECTORY); UT_ASSERT(fd >= 0);
	for (uint32 i = 0; i < count; ++i) {
		ClusterRelmapRecoverySegment *segment = NULL;
		char path[MAXPGPATH];
		terminal_many_inputs[i] = terminal_input;
		terminal_many_inputs[i].input.staged = terminal_namespace_images[i];
		terminal_many_inputs[i].input.frame.read_rec_ptr = terminal_namespace_records[i].record_start;
		terminal_many_inputs[i].input.frame.end_rec_ptr = terminal_namespace_records[i].exclusive_end;
		if (i != 0) {
			snprintf(path, sizeof(path), "base/%u", terminal_namespace_images[i].identity.dbid);
			UT_ASSERT_EQ(mkdirat(fd, path, 0700), 0);
			for (unsigned copy = 0; copy < 2; ++copy) {
				snprintf(path, sizeof(path), "base/%u/pgrac_relmap_authority%s",
					terminal_namespace_images[i].identity.dbid, copy == 0 ? "" : ".bak");
				relmap_census_file(fd, path, &terminal_namespace_images[i]);
			}
		}
		if (i < 64) {
			terminal_pins.images[i] = terminal_namespace_images[i];
			terminal_pins.attempted[i] = terminal_pins.staged[i] = true;
		} else {
			ClusterRelmapRecoverySegment **at = &terminal_pins.segments;
			uint32 index = (i - 64) % 64;
			for (uint32 n = 0; n < (i - 64) / 64; ++n) at = &(*at)->next;
			if (*at == NULL) *at = calloc(1, sizeof(**at));
			segment = *at;
			segment->images[index] = terminal_namespace_images[i];
			segment->attempted[index] = segment->staged[index] = true;
		}
	}
	UT_ASSERT_EQ(close(fd), 0);
	terminal_many_overflow = terminal_pins.segments;
	terminal_pins.expected = terminal_pins.count = count;
	terminal_plan.relmap_count = terminal_plan.relmap_input_capacity = count;
	terminal_plan.relmap_inputs = terminal_many_inputs;
}

static bool
terminal_many_leave(void)
{
#ifdef TEST_TERMINAL_REAL_START
	return terminal_bind_original_start();
#else
	terminal_leave();
	return true;
#endif
}

static void
terminal_many_done(void)
{
	int fd = open(test_root, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(fd >= 0);
	for (uint32 i = 1; i < terminal_many_count; ++i) {
		char path[MAXPGPATH];
		for (unsigned copy = 0; copy < 2; ++copy) {
			snprintf(path, sizeof(path), "base/%u/pgrac_relmap_authority%s",
				terminal_namespace_images[i].identity.dbid, copy == 0 ? "" : ".bak");
			UT_ASSERT_EQ(unlinkat(fd, path, 0), 0);
		}
		snprintf(path, sizeof(path), "base/%u", terminal_namespace_images[i].identity.dbid);
		UT_ASSERT_EQ(unlinkat(fd, path, AT_REMOVEDIR), 0);
	}
	UT_ASSERT_EQ(close(fd), 0);
	terminal_done();
	while (terminal_many_overflow != NULL) {
		ClusterRelmapRecoverySegment *next = terminal_many_overflow->next;
		free(terminal_many_overflow); terminal_many_overflow = next;
	}
	terminal_pins.segments = NULL;
	free(terminal_many_inputs); free(terminal_namespace_images); free(terminal_namespace_records);
	terminal_many_inputs = NULL; terminal_namespace_images = NULL; terminal_namespace_records = NULL;
	terminal_many_count = 0;
}

UT_TEST(test_terminal_many_complete_pages_commit_abort)
{
	for (unsigned aborted = 0; aborted < 2; ++aborted) {
		ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
		uint8 digest[32];
		uint32 seen = 0;
		unsigned reads, syncs;
		terminal_many_setup(aborted ? 129 : 65, aborted != 0);
		UT_ASSERT(cluster_cold_relmap_pending_terminal_capture_v1(&terminal_typed, terminal_fence));
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, terminal_many_count);
		if (!terminal_many_leave()) {
			UT_ASSERT(false); free(out); terminal_many_done(); return;
		}
		reads = prefix_base_reads;
		syncs = test_relmap_native_clog_stats().file_syncs;
		for (uint32 offset = 0; offset < terminal_many_count; offset += 64) {
			uint32 expected = Min(64, terminal_many_count - offset);
			bool accepted = cluster_cold_relmap_pending_terminal_recheck_segment_v1(terminal_start, offset, out);
			UT_ASSERT(accepted);
			if (!accepted) break;
			UT_ASSERT_EQ(out->count, expected);
			if (out->count != expected) break;
			UT_ASSERT_EQ(out->offset, offset); UT_ASSERT_EQ(out->total_count, terminal_many_count);
			if (offset == 0) memcpy(digest, out->digest, sizeof(digest));
			UT_ASSERT_EQ(memcmp(digest, out->digest, sizeof(digest)), 0);
			UT_ASSERT(!terminal_segment_empty(digest, sizeof(digest)));
			for (uint32 i = 0; i < out->count; ++i) {
				UT_ASSERT_EQ(out->entries[i].identity.dbid, terminal_namespace_images[offset + i].identity.dbid);
				UT_ASSERT_EQ(out->entries[i].status, aborted ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
				UT_ASSERT_EQ(memcmp(&out->entries[i].staged, &terminal_namespace_images[offset + i], sizeof(out->entries[i].staged)), 0);
				seen++;
			}
		}
		UT_ASSERT_EQ(seen, terminal_many_count);
		UT_ASSERT_EQ(prefix_base_reads, reads);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().file_syncs, syncs);
		UT_ASSERT(!terminal_pins.transferred && terminal_pins.count == terminal_many_count);
		UT_ASSERT(terminal_plan.relmap_create_count == terminal_many_creation_count);
		UT_ASSERT(!terminal_many_creation_count || !pending_terminal_creation.entries[0].attempted);
		free(out); terminal_many_done();
	}
}

UT_TEST(test_terminal_many_tail_failure_revokes_prefix)
{
	ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
	terminal_many_setup(65, false);
	UT_ASSERT(cluster_cold_relmap_pending_terminal_capture_v1(&terminal_typed, terminal_fence));
	if (!terminal_many_leave()) {
		UT_ASSERT(false); free(out); terminal_many_done(); return;
	}
	UT_ASSERT(cluster_cold_relmap_pending_terminal_recheck_segment_v1(terminal_start, 0, out));
	test_relmap_native_clog_reopen_fault(terminal_last_native, 7);
	UT_ASSERT(!cluster_cold_relmap_pending_terminal_recheck_segment_v1(terminal_start, 0, out));
	UT_ASSERT(terminal_segment_empty(out, sizeof(*out)) && terminal_plan.relmap_failed);
	UT_ASSERT(!cluster_cold_relmap_pending_terminal_recheck_segment_v1(terminal_start, 64, out));
	UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, terminal_many_count);
	UT_ASSERT(!terminal_pins.transferred && terminal_pins.count == terminal_many_count);
	free(out); terminal_many_done();
}

UT_TEST(test_terminal_many_prefix_failure_refuses_tail)
{
	ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
	terminal_many_setup(65, true);
	UT_ASSERT(cluster_cold_relmap_pending_terminal_capture_v1(&terminal_typed, terminal_fence));
	if (!terminal_many_leave()) {
		UT_ASSERT(false); free(out); terminal_many_done(); return;
	}
	UT_ASSERT(cluster_cold_relmap_pending_terminal_recheck_segment_v1(terminal_start, 64, out));
	terminal_many_inputs[0].input.staged.pending_generation++;
	UT_ASSERT(!cluster_cold_relmap_pending_terminal_recheck_segment_v1(terminal_start, 64, out));
	UT_ASSERT(terminal_segment_empty(out, sizeof(*out)) && terminal_plan.relmap_failed);
	UT_ASSERT(!cluster_cold_relmap_pending_terminal_recheck_segment_v1(terminal_start, 0, out));
	UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, terminal_many_count);
	UT_ASSERT(!terminal_pins.transferred && terminal_plan.relmap_create_count == terminal_many_creation_count);
	free(out); terminal_many_done();
}

UT_TEST(test_terminal_many_partial_capture_error_keeps_all_debt)
{
	volatile bool threw = false;
	unsigned reads;
	terminal_many_setup(65, false);
	terminal_capture_throw_after_detach = true;
	PG_TRY(); { UT_ASSERT(!cluster_cold_relmap_pending_terminal_capture_v1(&terminal_typed, terminal_fence)); }
	PG_CATCH(); { threw = true; } PG_END_TRY();
	terminal_capture_throw_after_detach = false;
	UT_ASSERT(threw && terminal_plan.relmap_failed && terminal_plan.relmap_terminal_attempted);
	UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
	reads = prefix_base_reads;
	UT_ASSERT(!cluster_cold_relmap_pending_terminal_capture_v1(&terminal_typed, terminal_fence));
	UT_ASSERT_EQ(prefix_base_reads, reads);
	UT_ASSERT(!terminal_pins.transferred && terminal_pins.count == terminal_many_count);
	terminal_many_done();
}
