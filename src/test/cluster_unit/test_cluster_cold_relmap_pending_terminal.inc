/*-------------------------------------------------------------------------
 * test_cluster_cold_relmap_pending_terminal.inc
 *     Pending-only terminal participant retains independent creation debt.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_relmap_pending_terminal.inc
 * NOTES
 *     Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */

/* Creation is deliberately unconsumed. These tests isolate the pending reader;
 * they do not claim creation authorization or qualification from this fixture. */
static ColdRelmapCreateChunk pending_terminal_creation;
static void
pending_terminal_setup(bool aborted)
{
	terminal_setup(aborted);
	memset(&pending_terminal_creation, 0, sizeof(pending_terminal_creation));
	pending_terminal_creation.entries[0].captured = true;
	pending_terminal_creation.entries[0].input.creation.identity.database_oid = 2000;
	terminal_plan.relmap_create_count = 1;
	terminal_plan.relmap_creates = &pending_terminal_creation;
}

UT_TEST(test_pending_terminal_mixed_commit_abort_and_repeat)
{
	for (unsigned aborted = 0; aborted < 2; ++aborted) {
		ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
		unsigned reads, syncs;
		pending_terminal_setup(aborted != 0);
		UT_ASSERT(cluster_cold_relmap_pending_terminal_capture_v1(&terminal_typed, terminal_fence));
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
		terminal_leave(); reads = prefix_base_reads;
		syncs = test_relmap_native_clog_stats().file_syncs;
		for (unsigned repeat = 0; repeat < 2; ++repeat) {
			UT_ASSERT(cluster_cold_relmap_pending_terminal_recheck_v1(terminal_start, out));
			UT_ASSERT_EQ(out->count, 1);
			UT_ASSERT_EQ(out->entries[0].status, aborted ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
			UT_ASSERT_EQ(memcmp(&out->entries[0].staged, &custody_image, sizeof(custody_image)), 0);
		}
		UT_ASSERT_EQ(prefix_base_reads, reads);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().file_syncs, syncs);
		UT_ASSERT_EQ(terminal_plan.relmap_create_count, 1);
		UT_ASSERT(!pending_terminal_creation.entries[0].attempted && !pending_terminal_creation.entries[0].receipt);
		UT_ASSERT(!terminal_pins.transferred && !terminal_revoked);
		UT_ASSERT(!cluster_cold_relmap_pending_terminal_capture_v1(&terminal_typed, terminal_fence));
		free(out); terminal_done();
	}
}

UT_TEST(test_pending_terminal_does_not_open_legacy_mixed_gates)
{
	ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
	pending_terminal_setup(false);
	UT_ASSERT(!cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
	UT_ASSERT_EQ(test_relmap_native_clog_stats().calls, 0);
	UT_ASSERT(!cluster_cold_relmap_pending_terminal_capture_v1(&terminal_typed, terminal_fence));
	terminal_done();
	pending_terminal_setup(false);
	UT_ASSERT(cluster_cold_relmap_pending_terminal_capture_v1(&terminal_typed, terminal_fence));
	terminal_leave();
	UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
	UT_ASSERT(out->count == 0 && terminal_revoked && !terminal_pins.transferred);
	UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
	free(out); terminal_done();
}

UT_TEST(test_pending_terminal_empty_does_not_manufacture_qualification)
{
	ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
	pending_terminal_setup(false);
	terminal_plan.relmap_count = terminal_plan.relmap_input_capacity = 0; terminal_plan.relmap_inputs = NULL;
	UT_ASSERT_EQ(cluster_cold_typed_relmap_pending_count_v1(&terminal_typed), 0);
	UT_ASSERT(!cluster_cold_relmap_pending_terminal_capture_v1(&terminal_typed, terminal_fence));
	UT_ASSERT(terminal_plan.relmap_terminal_attempted && terminal_plan.relmap_failed);
	UT_ASSERT_NULL(terminal_plan.relmap_terminal);
	UT_ASSERT_EQ(test_relmap_native_clog_stats().calls, 0);
	terminal_leave();
	UT_ASSERT(!cluster_cold_relmap_pending_terminal_recheck_v1(terminal_start, out));
	UT_ASSERT(out->count == 0 && terminal_revoked && terminal_plan.relmap_create_count == 1);
	free(out); terminal_done();
}

UT_TEST(test_pending_terminal_source_writer_and_pin_drift_stay_failed)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
		pending_terminal_setup(false);
		UT_ASSERT(cluster_cold_relmap_pending_terminal_capture_v1(&terminal_typed, terminal_fence));
		terminal_leave();
		UT_ASSERT(cluster_cold_relmap_pending_terminal_recheck_v1(terminal_start, out));
		if (fault == 0) terminal_typed.sources[0].timeline++;
		if (fault == 1) terminal_owner.install_generation++;
		if (fault == 2) terminal_pins.images[0].pending_generation++;
		memset(out, 0xa5, sizeof(*out));
		UT_ASSERT(!cluster_cold_relmap_pending_terminal_recheck_v1(terminal_start, out));
		UT_ASSERT(out->count == 0 && terminal_revoked && !terminal_pins.transferred);
		UT_ASSERT(!cluster_cold_relmap_pending_terminal_recheck_v1(terminal_start, out));
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
		free(out); terminal_done();
	}
}

UT_TEST(test_pending_terminal_mutation_and_active_writer_keep_fd_debt)
{
	for (unsigned active = 0; active < 2; ++active) {
		ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
		pending_terminal_setup(false);
		UT_ASSERT(cluster_cold_relmap_pending_terminal_capture_v1(&terminal_typed, terminal_fence));
		terminal_leave(); test_relmap_native_clog_mutate(true);
		if (!active) test_relmap_native_clog_mutate(false);
		UT_ASSERT(!cluster_cold_relmap_pending_terminal_recheck_v1(terminal_start, out));
		UT_ASSERT(out->count == 0 && terminal_revoked && !terminal_pins.transferred);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
		if (active) test_relmap_native_clog_mutate(false);
		free(out); terminal_done();
	}
}

UT_TEST(test_pending_terminal_capture_and_start_errors_keep_debt)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
		volatile bool caught = false;
		pending_terminal_setup(false);
		if (fault < 2) {
			terminal_episode_error = fault == 0;
			terminal_capture_throw_after_detach = fault == 1;
			PG_TRY(); { (void)cluster_cold_relmap_pending_terminal_capture_v1(&terminal_typed, terminal_fence); }
			PG_CATCH(); { caught = true; } PG_END_TRY();
			terminal_episode_error = terminal_capture_throw_after_detach = false;
			UT_ASSERT(caught && terminal_plan.relmap_failed);
			UT_ASSERT(!cluster_cold_relmap_pending_terminal_capture_v1(&terminal_typed, terminal_fence));
		} else {
			UT_ASSERT(cluster_cold_relmap_pending_terminal_capture_v1(&terminal_typed, terminal_fence));
			terminal_leave(); terminal_throw = true;
			PG_TRY(); { (void)cluster_cold_relmap_pending_terminal_recheck_v1(terminal_start, out); }
			PG_CATCH(); { caught = true; } PG_END_TRY();
			terminal_throw = false;
			UT_ASSERT(caught && terminal_revoked && out->count == 0);
			UT_ASSERT(!cluster_cold_relmap_pending_terminal_recheck_v1(terminal_start, out));
		}
		UT_ASSERT(!terminal_pins.transferred && terminal_plan.relmap_create_count == 1);
		free(out); terminal_done();
	}
}

UT_TEST(test_pending_terminal_aliases_refuse_before_owner_or_output)
{
	void *aliases[5]; unsigned calls;
	pending_terminal_setup(false);
	UT_ASSERT(cluster_cold_relmap_pending_terminal_capture_v1(&terminal_typed, terminal_fence));
	terminal_leave(); calls = terminal_current_calls;
	aliases[0] = &terminal_typed; aliases[1] = &terminal_plan;
	aliases[2] = &terminal_input; aliases[3] = &terminal_pins; aliases[4] = &pending_terminal_creation;
	for (unsigned i = 0; i < lengthof(aliases); ++i) {
		unsigned char before[16]; memcpy(before, aliases[i], sizeof(before));
		UT_ASSERT(!cluster_cold_relmap_pending_terminal_recheck_v1(terminal_start, aliases[i]));
		UT_ASSERT_EQ(memcmp(before, aliases[i], sizeof(before)), 0);
	}
	UT_ASSERT_EQ(terminal_current_calls, calls);
	UT_ASSERT(!terminal_revoked);
	terminal_done();
}
