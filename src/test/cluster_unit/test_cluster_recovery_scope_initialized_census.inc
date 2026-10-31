/*-------------------------------------------------------------------------
 * test_cluster_recovery_scope_initialized_census.inc
 *     Native FULL pages and dual-copy observations in one original episode.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_scope_initialized_census.inc
 * NOTES
 *     Native WAL, CLOG/SLRU, held files and pair readers execute. Startup,
 *     locks, XACT outcome and foreign consumed facts remain controlled.
 *     These observations do not qualify era, lifetime or completion.
 *-------------------------------------------------------------------------
 */
static char scope_initialized_cwd[MAXPGPATH];
static char scope_initialized_work[MAXPGPATH];
/* NULL keeps every original setup path. The new composition supplies actual
 * native callback consumption before this same scope starts observation. */
static void (*scope_initialized_replay_hook)(ClusterWalInputsV1 *,
	ClusterRecoveryRelmapHistory *, const ClusterRelmapV2Image *, bool);

/* Populate both actual identities before the original scope captures ROOT.
 * No selected source or replay request is renamed after acquisition. */
static void
scope_initialized_records(uint8 *bytes, ClusterRecoveryAnchorV2 *anchors)
{
	ControlRootImage root;
	char path[MAXPGPATH];

	UT_ASSERT_EQ(cluster_control_root_v3_decode(bytes, 66048, v2_storage, TEST_SYSID, &root), 0);
	for (unsigned node = 0; node < 2; ++node) {
		ClusterRecoveryAnchorV2 *anchor = &anchors[node];
		ClusterRelmapCheckpointWindow window = {0};
		ControlFileData native = {0};
		ClusterWalSourceRef source = {0};
		WalTestRecord tail;
		char map[TEST_RELMAP_LENGTH];

		anchor->checkpoint_copy.nextXid = FullTransactionIdFromU64((UINT64_C(3) << 32) | 4196000);
		native.state = anchor->state;
		native.checkPoint = anchor->checkpoint;
		native.checkPointCopy = anchor->checkpoint_copy;
		snprintf(test_checkpoint_prefix_path, sizeof(test_checkpoint_prefix_path),
			"%s/thread_%u/generation_" UINT64_FORMAT "/durable_prefix/current",
			cluster_wal_threads_dir, anchor->identity.origin_thread_id,
			anchor->identity.origin_owner_incarnation);
		v2_checkpoint_wal_record(&anchor->identity, &native, 8);
		window.system_identifier = TEST_SYSID;
		window.database_incarnation = anchor->database_incarnation;
		window.origin = node;
		window.epoch = 9;
		window.admitted_incarnation = anchor->identity.origin_owner_incarnation;
		window.boot_incarnation = 17;
		window.native_floor = (UINT64_C(3) << 32) | 4195000;
		window.last_issued = (UINT64_C(3) << 32) | 4195200;
		window.allocation_next = (UINT64_C(3) << 32) | 4195300;
		UT_ASSERT(cluster_relmap_checkpoint_window_encode_v2(&window, &anchor->checkpoint_copy,
			anchor->checkpoint, test_checkpoint_end, (UINT64_C(3) << 32) | 4195250,
			anchor->native_window));
		v2_anchor_object(bytes, anchor, &anchor->identity, path);
		UT_ASSERT_EQ(cluster_control_root_v3_decode(bytes, 66048, v2_storage, TEST_SYSID, &root), 0);
		root.records[node].checkpoint_record_crc32c = test_checkpoint_crc;
		source.claim.identity = anchor->identity;
		source.timeline = anchor->checkpoint_copy.ThisTimeLineID;
		tail = (WalTestRecord){1, test_checkpoint_end, anchor->checkpoint, test_checkpoint_crc};
		tail = episode_native_pages_prefix(&source, tail);
		test_relmap_payload(map, node == 0, 2);
		tail = inventory_write_record(&source, tail.exclusive_end, tail.record_start,
			RM_RELMAP_ID, XLOG_RELMAP_UPDATE, map, sizeof(map));
		tail = episode_terminal_write(&source, tail, false, node == 0 ? 4195136 : 4196001);
		root.records[node].tail_last_record_lsn = tail.record_start;
		root.records[node].tail_last_record_crc32c = tail.record_crc;
		root.records[node].validated_tail_lsn_exclusive = tail.exclusive_end;
		UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
		memcpy(bytes, root.bytes, 66048);
	}
}

static void
scope_initialized_begin(ClusterWalInputsV1 **inputs, ClusterRecoveryRelmapHistory **history,
	ClusterRelmapV2Image *pending, bool foreign)
{
	const char *dirs[] = {"native_side", "native_side/origin_0", "native_side/origin_0/pg_xact",
		"native_side/origin_1", "native_side/origin_1/pg_xact"};
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryRelmapFoldSummary summary;
	char path[MAXPGPATH];

	episode_base_reset();
	episode_native_clog = true;
	episode_birth_match_mode = 0;
	episode_native_pages_foreign = 1;
	episode_native_pages_duplicate = false;
	scope_native_records_hook = scope_initialized_records;
	scope_native_setup(inputs);
	scope_native_records_hook = NULL;
	for (unsigned i = 0; i < lengthof(dirs); ++i) {
		snprintf(path, sizeof(path), "%s/%s", cluster_shared_data_dir, dirs[i]);
		UT_ASSERT_EQ(mkdir(path, 0700), 0);
	}
	UT_ASSERT(getcwd(scope_initialized_cwd, sizeof(scope_initialized_cwd)) != NULL);
	strlcpy(scope_initialized_work, "/tmp/pgrac-scope-initialized-XXXXXX", sizeof(scope_initialized_work));
	UT_ASSERT(mkdtemp(scope_initialized_work) != NULL);
	UT_ASSERT_EQ(chdir(scope_initialized_work), 0);
	snprintf(path, sizeof(path), "%s/native_side/origin_0/pg_xact", cluster_shared_data_dir);
	UT_ASSERT_EQ(symlink(path, "pg_xact"), 0);
	episode_native_pages_file(0, 128, 0, 0);
	episode_native_pages_file(1, 17, 0, 0);
	test_episode_native_clog_start(cluster_shared_data_dir);
	episode_clog_side_fault = 0;
	episode_clog_side_plans(*inputs);
	UT_ASSERT_EQ(episode_history_build(*inputs, 8 * 1024 * 1024, history, &diag), 0);
	if (*history == NULL) return;
	*pending = relmap_fold_pending((*history)->inventory, 0);
	UT_ASSERT_EQ(episode_pending_drain(*history, pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	if (scope_initialized_replay_hook != NULL)
		scope_initialized_replay_hook(*inputs, *history, pending, foreign);
	else {
		consumed_mode = 1;
		consumed_foreign_origin = 1;
		consumed_cold_calls = 0;
		UT_ASSERT_EQ(consumed_native_replay(*inputs, *history, pending, true, false), 0);
		UT_ASSERT_EQ(consumed_native_calls, 1);
		test_episode_native_clog_write_all();
		if (foreign) {
			unsigned matched = 0;
			xl_clog_zeropage_full birth = {17, 0, (UINT64_C(3) << 32) | 17 * CLUSTER_NATIVE_CLOG_PER_PAGE};
			for (uint32 i = 0; i < cluster_wal_inputs_relmap_count_v1(*inputs); ++i) {
				ClusterRecoveryRelmapHistoryEntryV1 entry;
				UT_ASSERT(cluster_recovery_relmap_history_source_v1(*history, i, &entry));
				UT_ASSERT(entry.source.scope == *inputs);
				if (entry.source.selected.source.claim.identity.origin_node_id != 1) continue;
				UT_ASSERT(test_episode_native_clog_full_apply(&entry.source, &birth));
				matched++;
			}
			UT_ASSERT_EQ(matched, 1);
		}
	}
	episode_native_clog_calls = 0;
}

static bool
scope_initialized_read(ClusterRecoveryRelmapHistory *history, const ClusterRelmapV2Image *pending,
	int fds[2])
{
	ClusterRecoveryRelmapPendingNativePagesV1 pages;
	ClusterRecoveryNativeInventoryDiag diag;
	bool ok = cluster_recovery_relmap_history_pending_initialized_pages_v1(history, pending, &pages, &diag);

	if (!ok) return false;
	UT_ASSERT_EQ(pages.page_count, 2);
	for (uint32 i = 0; i < pages.page_count && i < 2; ++i) {
		ClusterRecoveryRelmapNativePageV1 page;
		UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
		UT_ASSERT_EQ(page.native.origin, page.history.source.selected.source.claim.identity.origin_node_id);
		UT_ASSERT(page.native.origin < 2);
		UT_ASSERT(page.native.resident_birth_observed);
		UT_ASSERT_EQ(page.native.resident_first_full_xid, page.birth.first_full_xid);
		fds[i] = episode_native_pages_find_fd(page.native.inode);
		UT_ASSERT(fds[i] >= 0);
	}
	return true;
}

static void
scope_initialized_end(ClusterWalInputsV1 **inputs, ClusterRecoveryRelmapHistory **history,
	const int fds[2])
{
	char path[MAXPGPATH];
	const char *dirs[] = {"native_side/origin_0/pg_xact", "native_side/origin_0",
		"native_side/origin_1/pg_xact", "native_side/origin_1", "native_side"};

	cluster_recovery_relmap_history_destroy_v1(history);
	for (unsigned i = 0; i < 2; ++i) if (fds[i] >= 0) UT_ASSERT_EQ(fcntl(fds[i], F_GETFD), -1);
	UT_ASSERT(ctrc_set_held == 2 && inputs_pin_held && fcntl(scope_native_fd, F_GETFD) >= 0);
	for (uint32 p = 0; p < tx_inputs_typed.participant_count; ++p)
		rf_side_online_plan_destroy_v1(&tx_inputs_typed.side_plans[p]);
	scope_native_finish(inputs); /* The original scope retires its census before IR/WALR. */
	test_episode_native_clog_discard_foreign(1);
	test_episode_native_clog_stop();
	episode_native_pages_path(path, 0, 128); UT_ASSERT_EQ(unlink(path), 0);
	episode_native_pages_path(path, 1, 17); UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(unlink("pg_xact"), 0);
	UT_ASSERT_EQ(chdir(scope_initialized_cwd), 0);
	UT_ASSERT_EQ(rmdir(scope_initialized_work), 0);
	for (unsigned i = 0; i < lengthof(dirs); ++i) {
		snprintf(path, sizeof(path), "%s/%s", cluster_shared_data_dir, dirs[i]);
		UT_ASSERT_EQ(rmdir(path), 0);
	}
	episode_native_clog = false;
	consumed_mode = 0;
	consumed_foreign_origin = 127;
}

UT_TEST(test_scope_initialized_census_shares_complete_original_episode)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	const ClusterRelmapRetentionCensus *census = NULL;
	int fds[2] = {-1, -1};
	unsigned calls;

	scope_initialized_begin(&inputs, &history, &pending, true);
	calls = ctrc_set_calls;
	UT_ASSERT(scope_initialized_read(history, &pending, fds));
	UT_ASSERT(scope_native_prepare(inputs, &census));
	UT_ASSERT(census != NULL);
	UT_ASSERT_EQ(cluster_relmap_retention_census_next((ClusterRelmapRetentionCensus *)census),
		CLUSTER_RELMAP_CENSUS_DRAINED);
	UT_ASSERT_EQ(ctrc_set_calls, calls);
	UT_ASSERT_EQ(scope_native_legacy_calls | ctrc_set_release_calls | relmap_failed, 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
	UT_ASSERT(scope_initialized_read(history, &pending, fds));
	scope_initialized_end(&inputs, &history, fds);
	UT_ASSERT_EQ(scope_native_destroyed, 1);
}

UT_TEST(test_scope_initialized_census_unknown_birth_preserves_custody)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	const ClusterRelmapRetentionCensus *census = NULL;
	int fds[2] = {-1, -1};

	scope_initialized_begin(&inputs, &history, &pending, false);
	UT_ASSERT(scope_native_prepare(inputs, &census));
	UT_ASSERT(!scope_initialized_read(history, &pending, fds));
	UT_ASSERT(history->failed && relmap_failed > 0 && ctrc_set_held == 2 && inputs_pin_held);
	UT_ASSERT_NE(cluster_relmap_retention_census_next((ClusterRelmapRetentionCensus *)census),
		CLUSTER_RELMAP_CENSUS_DRAINED);
	UT_ASSERT_EQ(scope_native_destroyed | ctrc_set_release_calls, 0);
	scope_initialized_end(&inputs, &history, fds);
}

UT_TEST(test_scope_initialized_census_pair_refusal_keeps_native_custody)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		const ClusterRelmapRetentionCensus *census = NULL;
		int fds[2] = {-1, -1};

		scope_initialized_begin(&inputs, &history, &pending, true);
		UT_ASSERT(scope_initialized_read(history, &pending, fds));
		if (fault == 0) UT_ASSERT_EQ(unlinkat(scope_native_fd, "base/5/pgrac_relmap_authority.bak", 0), 0);
		else scope_native_write_pair(5, fault == 2, fault == 1);
		UT_ASSERT(!scope_native_prepare(inputs, &census));
		UT_ASSERT(census == NULL && relmap_failed > 0);
		UT_ASSERT(!scope_initialized_read(history, &pending, fds));
		for (unsigned i = 0; i < 2; ++i) UT_ASSERT(fcntl(fds[i], F_GETFD) >= 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
		scope_initialized_end(&inputs, &history, fds);
	}
}

UT_TEST(test_scope_initialized_census_late_original_owner_loss_invalidates_both)
{
	for (unsigned error = 0; error < 2; ++error) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		const ClusterRelmapRetentionCensus *census = NULL;
		int fds[2] = {-1, -1};
		volatile bool caught = false;

		scope_initialized_begin(&inputs, &history, &pending, true);
		UT_ASSERT(scope_initialized_read(history, &pending, fds));
		UT_ASSERT(scope_native_prepare(inputs, &census));
		if (error) ctrc_replay_current_override = relmap_episode_terminal_error_current;
		else relmap_sibling->release_uncertain = true;
		test_capture_error_level = true;
		PG_TRY(); {
			UT_ASSERT(!scope_initialized_read(history, &pending, fds));
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		test_capture_error_level = false;
		ctrc_replay_current_override = ctrc_set_original_current;
		if (!error) relmap_sibling->release_uncertain = false;
		UT_ASSERT_EQ(caught, error != 0);
		UT_ASSERT(history->failed && relmap_failed > 0);
		UT_ASSERT_NE(cluster_relmap_retention_census_next((ClusterRelmapRetentionCensus *)census),
			CLUSTER_RELMAP_CENSUS_DRAINED);
		for (unsigned i = 0; i < 2; ++i) UT_ASSERT(fcntl(fds[i], F_GETFD) >= 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
		scope_initialized_end(&inputs, &history, fds);
	}
}

UT_TEST(test_scope_initialized_census_observations_do_not_grant_completion)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	const ClusterRelmapRetentionCensus *census = NULL, *again = NULL;
	ClusterRelmapCensusCommitted committed, zero = {0};
	int fds[2] = {-1, -1};

	scope_initialized_begin(&inputs, &history, &pending, true);
	UT_ASSERT(scope_initialized_read(history, &pending, fds));
	UT_ASSERT(scope_native_prepare(inputs, &census));
	memset(&committed, 0xa5, sizeof(committed));
	UT_ASSERT(!cluster_relmap_retention_census_committed_cold((ClusterRelmapRetentionCensus *)census,
		InvalidOid, &committed));
	UT_ASSERT_EQ(memcmp(&committed, &zero, sizeof(zero)), 0);
	UT_ASSERT(!scope_native_prepare(inputs, &again) && again == NULL);
	UT_ASSERT_EQ(ctrc_set_release_calls | scope_native_legacy_calls, 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && relmap_failed == 0);
	scope_initialized_end(&inputs, &history, fds);
}
