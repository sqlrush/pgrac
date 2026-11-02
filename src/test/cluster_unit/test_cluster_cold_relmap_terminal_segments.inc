/*-------------------------------------------------------------------------
 * test_cluster_cold_relmap_terminal_segments.inc
 *     Paged pending terminal observations preserve complete-batch failures.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_relmap_terminal_segments.inc
 * NOTES
 *     Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
static bool terminal_segment_empty(const void *value, Size bytes)
{
	const uint8 *p = value;
	for (Size i = 0; i < bytes; ++i) if (p[i] != 0) return false;
	return true;
}

UT_TEST(test_pending_segment_preserves_batch_and_terminal_polarity)
{
	for (unsigned aborted = 0; aborted < 2; ++aborted) {
		ClusterColdRelmapTerminalCheckV1 *whole = calloc(1, sizeof(*whole));
		ClusterColdRelmapTerminalCheckV1 *page = calloc(1, sizeof(*page));
		pending_terminal_setup(aborted != 0);
		UT_ASSERT(cluster_cold_relmap_pending_terminal_capture_v1(&terminal_typed, terminal_fence));
		terminal_leave();
		UT_ASSERT(cluster_cold_relmap_pending_terminal_recheck_v1(terminal_start, whole));
		UT_ASSERT(cluster_cold_relmap_pending_terminal_recheck_segment_v1(terminal_start, 0, page));
		UT_ASSERT_EQ(page->count, 1); UT_ASSERT_EQ(page->total_count, 1); UT_ASSERT_EQ(page->offset, 0);
		UT_ASSERT(!terminal_segment_empty(page->digest, sizeof(page->digest)));
		UT_ASSERT_EQ(memcmp(whole, page, sizeof(*page)), 0);
		UT_ASSERT_EQ(page->entries[0].status, aborted ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
		UT_ASSERT_EQ(terminal_plan.relmap_create_count, 1);
		UT_ASSERT(!pending_terminal_creation.entries[0].attempted && !terminal_pins.transferred);
		free(whole); free(page); terminal_done();
	}
}

UT_TEST(test_pending_segment_invalid_offset_revokes_without_prefix)
{
	const uint32 offsets[] = {1, UINT32_MAX};
	for (unsigned i = 0; i < lengthof(offsets); ++i) {
		ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
		pending_terminal_setup(false);
		UT_ASSERT(cluster_cold_relmap_pending_terminal_capture_v1(&terminal_typed, terminal_fence));
		terminal_leave();
		UT_ASSERT(cluster_cold_relmap_pending_terminal_recheck_segment_v1(terminal_start, 0, out));
		UT_ASSERT(!cluster_cold_relmap_pending_terminal_recheck_segment_v1(terminal_start, offsets[i], out));
		UT_ASSERT(terminal_segment_empty(out, sizeof(*out)) && terminal_revoked && terminal_plan.relmap_failed);
		UT_ASSERT(!cluster_cold_relmap_pending_terminal_recheck_segment_v1(terminal_start, 0, out));
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
		UT_ASSERT(!terminal_pins.transferred && terminal_plan.relmap_create_count == 1);
		free(out); terminal_done();
	}
}

UT_TEST(test_pending_segment_alias_in_overflow_storage_precedes_providers)
{
	ClusterRelmapRecoverySegment *segment = calloc(1, sizeof(*segment));
	ClusterRelmapRecoverySegment *saved = calloc(1, sizeof(*saved));
	unsigned calls;
	pending_terminal_setup(false);
	terminal_pins.expected = 65; terminal_pins.segments = segment;
	segment->images[0] = custody_image; *saved = *segment;
	terminal_leave(); calls = terminal_current_calls;
	UT_ASSERT(!cluster_cold_relmap_pending_terminal_recheck_segment_v1(terminal_start, 0,
		(ClusterColdRelmapTerminalCheckV1 *)&segment->images[0]));
	UT_ASSERT_EQ(memcmp(saved, segment, sizeof(*saved)), 0);
	UT_ASSERT_EQ(terminal_current_calls, calls);
	UT_ASSERT(!terminal_revoked && !terminal_pins.transferred);
	terminal_pins.segments = NULL; terminal_pins.expected = 1;
	free(saved); free(segment); terminal_done();
}

UT_TEST(test_segment_old_gate_and_late_mutation_keep_creation_and_fd_debt)
{
	for (unsigned mode = 0; mode < 2; ++mode) {
		ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
		pending_terminal_setup(false);
		UT_ASSERT(cluster_cold_relmap_pending_terminal_capture_v1(&terminal_typed, terminal_fence));
		terminal_leave();
		if (mode == 0)
			UT_ASSERT(!cluster_cold_relmap_terminal_recheck_segment_v1(terminal_start, 0, out));
		else {
			UT_ASSERT(cluster_cold_relmap_pending_terminal_recheck_segment_v1(terminal_start, 0, out));
			test_relmap_native_clog_mutate(true);
			UT_ASSERT(!cluster_cold_relmap_pending_terminal_recheck_segment_v1(terminal_start, 0, out));
			test_relmap_native_clog_mutate(false);
		}
		UT_ASSERT(terminal_segment_empty(out, sizeof(*out)) && terminal_revoked);
		UT_ASSERT(!terminal_pins.transferred && terminal_plan.relmap_create_count == 1);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
		free(out); terminal_done();
	}
}
