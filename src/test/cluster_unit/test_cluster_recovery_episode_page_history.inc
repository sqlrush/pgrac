/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_page_history.inc
 *    Complete native FULL page counter and lifecycle checks.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_episode_page_history.inc
 * NOTES
 *    Original ROOT/WAL/counter/SIDE and held native files execute. Startup,
 *    locks, native replay and foreign cold-applied leaves remain controlled.
 *-------------------------------------------------------------------------
 */
static unsigned episode_page_history_mode;

static WalTestRecord
episode_page_history_prefix(const ClusterWalSourceRef *source, WalTestRecord previous)
{
	if (source->claim.identity.origin_node_id == 0)
		return episode_birth_match_prefix(source, previous);
	for (unsigned i = 0; i < episode_native_pages_foreign; ++i) {
		xl_clog_zeropage_full birth = {0};
		bool last = i + 1 == episode_native_pages_foreign;
		int page = 17 + i;
		birth.pageno = page;
		birth.first_full_xid = ((last && episode_page_history_mode == 1 ? UINT64_C(2)
			: last && episode_page_history_mode == 2 ? UINT64_C(4) : UINT64_C(3)) << 32)
			| (uint64)page * CLUSTER_NATIVE_CLOG_PER_PAGE;
		if (last && episode_page_history_mode == 5) {
			previous = inventory_write_record(source, previous.exclusive_end, previous.record_start,
				RM_CLOG_ID, CLOG_ZEROPAGE, &page, sizeof(page));
			previous = episode_xid_frame(source, previous, InvalidTransactionId, InvalidTransactionId);
		}
		previous = inventory_write_record(source, previous.exclusive_end, previous.record_start,
			RM_CLOG_ID, CLOG_ZEROPAGE_FULL, &birth, sizeof(birth));
		previous = episode_xid_frame(source, previous, InvalidTransactionId, InvalidTransactionId);
		if (last && episode_page_history_mode == 3) {
			previous = inventory_write_record(source, previous.exclusive_end, previous.record_start,
				RM_CLOG_ID, CLOG_ZEROPAGE, &page, sizeof(page));
			previous = episode_xid_frame(source, previous, InvalidTransactionId, InvalidTransactionId);
		}
		if (last && episode_page_history_mode == 4) {
			xl_clog_truncate truncate = {16, 524291, 606};
			previous = inventory_write_record(source, previous.exclusive_end, previous.record_start,
				RM_CLOG_ID, CLOG_TRUNCATE, &truncate, sizeof(truncate));
			previous = episode_xid_frame(source, previous, InvalidTransactionId, InvalidTransactionId);
		}
	}
	if (episode_page_history_mode == 6 || episode_page_history_mode == 7) {
		episode_birth_halfspace_tail_next = (UINT64_C(3) << 32) + UINT64_C(557056)
			+ (episode_page_history_mode == 6 ? UINT64_C(0x7fffffff) : UINT64_C(0x80000000));
		previous = episode_birth_halfspace_tail(source, previous);
	}
	return previous;
}

static void
episode_page_history_begin(unsigned count, bool duplicate, size_t budget,
	ClusterWalInputsV1 **inputs, ClusterRecoveryRelmapHistory **history,
	ClusterRelmapV2Image *pending)
{
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryNativeInventoryDiag diag;
	char path[MAXPGPATH];
	const char *dirs[] = {"native_side", "native_side/origin_0",
		"native_side/origin_0/pg_xact", "native_side/origin_127",
		"native_side/origin_127/pg_xact", "episode-page-custody"};

	episode_native_clog = true;
	episode_base_reset(); episode_birth_match_mode = 0;
	episode_native_pages_foreign = count; episode_native_pages_duplicate = duplicate;
	episode_pending_prefix_hook = episode_page_history_prefix;
	episode_clog_side_terminal_begin(EPISODE_TERMINAL_COMMIT, inputs, history, pending);
	episode_pending_prefix_hook = NULL;
	for (uint32 i = 0; i < tx_inputs_typed.participant_count; ++i)
		rf_side_online_plan_destroy_v1(&tx_inputs_typed.side_plans[i]);
	episode_clog_side_fault = 0;
	episode_clog_side_plans(*inputs);
	/* The native provider's exact fixed custody size was selected before
	 * the history build; rebuild only to select this case's original budget. */
	cluster_recovery_relmap_history_destroy_v1(history);
	UT_ASSERT_EQ(episode_history_build(*inputs, budget, history, &diag), 0);
	UT_ASSERT(*history != NULL);
	for (unsigned i = 0; i < lengthof(dirs); ++i) {
		snprintf(path, sizeof(path), "%s/%s", cluster_shared_data_dir, dirs[i]);
		UT_ASSERT_EQ(mkdir(path, 0700), 0);
	}
	UT_ASSERT(getcwd(episode_native_pages_cwd, sizeof(episode_native_pages_cwd)) != NULL);
	snprintf(episode_native_pages_work, sizeof(episode_native_pages_work), "%s/episode-page-custody",
		cluster_shared_data_dir);
	UT_ASSERT_EQ(chdir(episode_native_pages_work), 0);
	snprintf(path, sizeof(path), "%s/native_side/origin_0/pg_xact", cluster_shared_data_dir);
	UT_ASSERT_EQ(symlink(path, "pg_xact"), 0);
	episode_native_pages_file(0, 128, 208, TRANSACTION_STATUS_COMMITTED);
	for (unsigned i = 0; i < count; ++i)
		episode_native_pages_file(127, 17 + (duplicate ? 0 : i), 0, 0);
	test_episode_native_clog_start(cluster_shared_data_dir);
	episode_native_clog_calls = 0;
	UT_ASSERT_EQ(episode_pending_drain(*history, pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
}

static void
episode_page_history_refusal(unsigned mode)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out, zero = {0};
	ClusterRecoveryRelmapNativePageV1 page;
	ClusterRecoveryNativeInventoryDiag diag;
	int fds[3] = {-1, -1, -1};
	unsigned reads, census;

	episode_page_history_mode = mode;
	episode_page_history_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	consumed_setup(inputs, history, false, 1);
	/* Existing observations keep their original nonqualifying contract. */
	UT_ASSERT(cluster_recovery_relmap_history_pending_native_pages_v1(history, &pending, &out, &diag));
	UT_ASSERT_EQ(out.page_count, 3);
	for (uint32 i = 0; i < 3; ++i) {
		UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
		fds[i] = episode_native_pages_find_fd(page.native.inode);
		UT_ASSERT(fds[i] >= 0);
	}
	reads = episode_native_clog_calls; census = inventory_census_calls;
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_recovery_relmap_history_pending_consumed_pages_v1(history, &pending, &out, &diag));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT(history->failed && relmap_failed > 0);
	UT_ASSERT_EQ(episode_native_clog_calls, reads + 6);
	UT_ASSERT_EQ(inventory_census_calls, census + 1);
	for (unsigned i = 0; i < 3; ++i) UT_ASSERT(fcntl(fds[i], F_GETFD) >= 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
	UT_ASSERT(history->inventory->relmap_xid == NULL && history->inventory->relmap_terminal == NULL);
	reads = episode_native_clog_calls; census = inventory_census_calls;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_consumed_pages_v1(history, &pending, &out, &diag));
	UT_ASSERT_EQ(episode_native_clog_calls, reads); UT_ASSERT_EQ(inventory_census_calls, census);
	consumed_mode = 0;
	episode_native_pages_end(&history, &inputs);
	for (unsigned i = 0; i < 3; ++i) UT_ASSERT_EQ(fcntl(fds[i], F_GETFD), -1);
}

UT_TEST(test_episode_page_history_checks_later_nonquery_full_eras)
{
	episode_page_history_refusal(1);
	episode_page_history_refusal(2);
}

UT_TEST(test_episode_page_history_refuses_postbirth_zero_and_truncate)
{
	episode_page_history_refusal(3);
	episode_page_history_refusal(4);
}

UT_TEST(test_episode_page_history_uses_each_sources_whole_page_halfspace)
{
	for (unsigned mode = 6; mode <= 7; ++mode) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapPendingNativePagesV1 out, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;
		unsigned census, reads;
		bool ok;
		episode_page_history_mode = mode;
		episode_page_history_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
		consumed_setup(inputs, history, false, 1);
		census = inventory_census_calls; reads = episode_native_clog_calls;
		ok = cluster_recovery_relmap_history_pending_consumed_pages_v1(history, &pending, &out, &diag);
		UT_ASSERT_EQ(ok, mode == 6);
		UT_ASSERT_EQ(inventory_census_calls, census + 1); UT_ASSERT_EQ(episode_native_clog_calls, reads + 6);
		if (ok && mode == 6) {
			unsigned foreign = 0;
			UT_ASSERT_EQ(out.page_count, 3);
			UT_ASSERT_EQ(out.inputs.query.birth.inputs.tail_next, (UINT64_C(3) << 32) | UINT64_C(4196000));
			for (uint32 i = 0; i < out.inputs.clog_count; ++i) {
				const ClusterRecoveryRelmapClogEventV1 *event = &out.inputs.clog[i];
				const ClusterRecoveryRelmapXidGenerationV1 *gen = &out.inputs.query.birth.inputs.generations[event->source_index];
				if (gen->observation.source.claim.identity.origin_node_id != 127) continue;
				foreign++;
				UT_ASSERT(event->counter_observed && gen->counter_observed);
				UT_ASSERT_EQ(gen->tail_next, (UINT64_C(3) << 32) + UINT64_C(557056) + UINT64_C(0x7fffffff));
			}
			UT_ASSERT_EQ(foreign, 2);
			UT_ASSERT(!history->failed && relmap_failed == 0);
		} else if (!ok && mode == 7) {
			UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
			UT_ASSERT(history->failed && relmap_failed > 0);
		}
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
		consumed_mode = 0; episode_native_pages_end(&history, &inputs);
	}
}

UT_TEST(test_episode_page_history_preserves_prebirth_legacy_input)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out;
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned census;
	episode_page_history_mode = 5;
	episode_page_history_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	consumed_setup(inputs, history, false, 1); census = inventory_census_calls;
	UT_ASSERT(cluster_recovery_relmap_history_pending_consumed_pages_v1(history, &pending, &out, &diag));
	UT_ASSERT_EQ(out.page_count, 3); UT_ASSERT_EQ(out.inputs.clog_count, 4);
	UT_ASSERT_EQ(inventory_census_calls, census + 1); UT_ASSERT_EQ(episode_native_clog_calls, 6);
	UT_ASSERT_EQ(consumed_cold_calls, 3);
	UT_ASSERT(!history->failed && relmap_failed == 0 && inputs_pin_held && ctrc_set_held == 2);
	consumed_mode = 0; episode_native_pages_end(&history, &inputs);
}
