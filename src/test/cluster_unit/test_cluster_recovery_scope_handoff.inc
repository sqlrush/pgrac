/*-------------------------------------------------------------------------
 * test_cluster_recovery_scope_handoff.inc
 *     Keep native replay receipts through the original post-EOR input handoff.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_scope_handoff.inc
 * NOTES
 *     Native replay, immutable SIDE, held pages and both map copies execute.
 *     The original complete ROOT scan refuses an undeclared relmap effect.
 *     The older receipt-handoff cases retain their controlled plan boundary.
 *     ROOT completion, installed Start and external authority remain fixtures.
 *     This is no lifetime, completion or retention-release qualification.
 *-------------------------------------------------------------------------
 */
#include "pgstat.h"

void
pgstat_report_checksum_failure(void)
{
	abort(); /* This CLOG-only plan must not report a PAGE checksum failure. */
}

static ClusterWalInputV1 scope_handoff_selected[2];
static const RfSideOnlinePlanV1 *scope_handoff_plans[2];
static unsigned scope_handoff_fault;
static bool scope_handoff_published;
static ClusterColdDetailV1 scope_handoff_plan_result;
static bool scope_handoff_original_plan;
static uint32 scope_handoff_wait_event;
uint32 *my_wait_event_info = &scope_handoff_wait_event;

static bool
scope_handoff_current(void *arg)
{
	return arg == &tx_inputs_typed && scope_handoff_replaying
		&& test_scope_startup_sources_current(&tx_inputs_typed,
			(ClusterRecoveryFencePlan *)&tx_inputs_typed);
}

static void
scope_handoff_native(XLogReaderState *reader, void *arg)
{
	UT_ASSERT(arg == &tx_inputs_typed && RecoveryInProgress());
	UT_ASSERT(!inputs_pin_held && ctrc_set_held == 0);
	UT_ASSERT(cluster_cold_typed_clog_native_record_current_v1(0, reader));
	++scope_replay_calls;
	test_episode_native_clog_replay(reader, tx_inputs_typed.sources[0].timeline,
		scope_handoff_fault <= 2 ? scope_handoff_fault : 0, &scope_replay_stats);
	if (scope_handoff_fault == 3)
		++tx_inputs_typed.sources[0].claim.max_config_generation;
}

/* Use the original pass-2 reader after dropping pass-1's provisional read
 * pin. No read-only census visitor is repurposed as mutation authority. */
static bool
scope_handoff_replay(uint32 participant)
{
	ClusterColdReaderV1 *reader;
	const ClusterWalSourceRef *source = &tx_inputs_typed.sources[participant];
	XLogRecPtr end = tx_inputs_typed.participants[participant].tail_end;
	volatile bool ok = false;
	char *error = NULL;

	reader = cluster_cold_reader_open_v1(source, TEST_SYSID,
		tx_inputs_typed.participants[participant].native_redo);
	if (reader == NULL) return false;
	PG_TRY(); {
		XLogReaderState *record;
		while ((record = cluster_cold_reader_next_v1(reader, &error)) != NULL) {
			if (record->EndRecPtr > end) break;
			if (XLogRecGetRmid(record) == RM_CLOG_ID) {
				if (participant == 0) {
					bool consumed;
					test_episode_native_clog_record_guard(cluster_cold_typed_clog_native_record_current_v1);
					PG_TRY(); {
						consumed = cluster_cold_typed_clog_native_v1(&tx_inputs_typed,
							(ClusterRecoveryFencePlan *)&tx_inputs_typed, record,
							scope_handoff_native, &tx_inputs_typed);
					} PG_FINALLY(); {
						test_episode_native_clog_record_guard(NULL);
					} PG_END_TRY();
					scope_replay_consumed = rf_side_online_plan_clog_native_consumed_v1(
						tx_inputs_typed.side_plans[0], source, record);
					if (!consumed) break;
				} else {
					xl_clog_zeropage_full birth;
					if ((XLogRecGetInfo(record) & ~XLR_INFO_MASK) != CLOG_ZEROPAGE_FULL
						|| XLogRecGetDataLen(record) != sizeof(birth)
						|| !rf_side_online_plan_clog_matches_v1(tx_inputs_typed.side_plans[participant],
							source, record)) break;
					memcpy(&birth, XLogRecGetData(record), sizeof(birth));
					if (!test_episode_native_clog_cold_full_apply(source, &birth,
						scope_handoff_current, &tx_inputs_typed)) break;
					++scope_replay_foreign;
				}
			}
			if (participant == 0 && XLogRecGetRmid(record) == RM_XACT_ID
				&& XLogRecGetXid(record) == 4195136
				&& (XLogRecGetInfo(record) & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT)
				test_episode_native_clog_set_committed(4195136); /* controlled outcome */
			if (record->EndRecPtr == end) {
				ok = scope_handoff_current(&tx_inputs_typed);
				break;
			}
		}
	} PG_FINALLY(); {
		cluster_cold_reader_close_v1(&reader);
	} PG_END_TRY();
	return ok;
}

static bool
scope_handoff_before_done(ClusterWalInputsV1 **original, ControlRootImage *root)
{
	volatile bool ok = false;
	char path[MAXPGPATH];

	UT_ASSERT(RecoveryInProgress() && inputs_pin_held);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(*original), 2);
	for (unsigned i = 0; i < 2; ++i)
		scope_handoff_selected[i] = *cluster_wal_inputs_at_v1(*original, i);
	scope_initialized_storage();
	for (unsigned i = 0; i < 2; ++i) {
		snprintf(path, sizeof(path), "%s/native_side/origin_%u/pg_commit_ts", cluster_shared_data_dir, i);
		UT_ASSERT_EQ(mkdir(path, 0700), 0);
	}
	episode_clog_side_fault = 0;
	episode_clog_side_plans(*original);
	for (unsigned i = 0; i < 2; ++i)
		scope_handoff_plans[i] = tx_inputs_typed.side_plans[i];
	cluster_wal_inputs_release_v1(original);
	UT_ASSERT(!inputs_pin_held);
	/* Installed original plan/fence and ROOT publication are fixture leaves.
	 * No post-EOR owner exists while the real native owner is executing. */
	tx_inputs_typed.top_tx_census = &ctrc_inputs_top;
	ctrc_set_held = ctrc_set_calls = ctrc_set_release_calls = 0;
	scope_handoff_replaying = true;
	test_restart_ref = tx_inputs_typed.sources[0];
	test_restart_ref_valid = true;
	if (scope_handoff_original_plan) {
		scope_handoff_plan_result = test_scope_startup_prepare_plan(&tx_inputs_typed,
			scope_handoff_selected, 8 * 1024 * 1024);
		if (scope_handoff_plan_result != CLUSTER_COLD_OK) return false;
	} else {
		/* Retain D442's explicitly controlled receipt-handoff component.
		 * It is not the real-plan relmap participant tested separately. */
		tx_inputs_typed.plan = (ClusterColdPlanV1 *)&tx_inputs_typed;
		UT_ASSERT(test_scope_startup_attach_cold(&tx_inputs_typed, scope_handoff_selected));
	}
	test_capture_error_level = true;
	PG_TRY(); {
		ok = scope_handoff_replay(0) && scope_handoff_replay(1);
	} PG_CATCH(); { scope_replay_error = true; } PG_END_TRY();
	test_capture_error_level = false;
	if (!ok) return false; /* Keep the original attempted plan/owner for audit. */
	test_episode_native_clog_write_all();
	UT_ASSERT(scope_handoff_current(&tx_inputs_typed));
	test_scope_startup_detach(&tx_inputs_typed);
	scope_handoff_replaying = false;
	test_restart_ref_valid = false;
	/* Match production phase order, not a relmap completion setter. This
	 * controlled publication is BEFORE the complete input scope is created. */
	for (unsigned i = 0; i < 2; ++i) tx_ir_complete(&root->records[i]);
	UT_ASSERT_EQ(cluster_control_root_v3_encode(root), 0);
	v2_write_roots(root->bytes);
	scope_handoff_published = true;
	return true;
}

static bool
scope_handoff_begin(ClusterWalInputsV1 **inputs, ClusterRecoveryRelmapHistory **history,
	ClusterRelmapV2Image *pending)
{
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryRelmapFoldSummary summary;

	episode_base_reset();
	episode_native_clog = true;
	episode_birth_match_mode = 0;
	episode_native_pages_foreign = 1;
	episode_native_pages_duplicate = false;
	scope_replay_calls = scope_replay_foreign = 0;
	scope_replay_consumed = scope_replay_error = scope_handoff_published = false;
	memset(&scope_replay_stats, 0, sizeof(scope_replay_stats));
	scope_native_records_hook = scope_initialized_records;
	scope_native_capture_hook = scope_startup_required;
	scope_native_handoff_hook = scope_handoff_before_done;
	scope_native_setup(inputs);
	scope_native_records_hook = NULL;
	scope_native_capture_hook = NULL;
	scope_native_handoff_hook = NULL;
	if (!scope_handoff_published) return false;
	UT_ASSERT(*inputs != NULL && !RecoveryInProgress());
	for (unsigned i = 0; i < 2; ++i) {
		UT_ASSERT(tx_inputs_typed.side_plans[i] == scope_handoff_plans[i]);
		UT_ASSERT_EQ(memcmp(&tx_inputs_typed.sources[i], &scope_handoff_selected[i].source,
			sizeof(tx_inputs_typed.sources[i])), 0);
	}
	consumed_mode = 1; consumed_foreign_origin = 1; consumed_cold_calls = 0;
	UT_ASSERT_EQ(episode_history_build(*inputs, 8 * 1024 * 1024, history, &diag), 0);
	if (*history == NULL) return false;
	*pending = relmap_fold_pending((*history)->inventory, 0);
	UT_ASSERT_EQ(episode_pending_drain(*history, pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	return true;
}

static void
scope_handoff_end(ClusterWalInputsV1 **inputs, ClusterRecoveryRelmapHistory **history,
	const int fds[2])
{
	char path[MAXPGPATH];
	for (unsigned i = 0; i < 2; ++i) {
		snprintf(path, sizeof(path), "%s/native_side/origin_%u/pg_commit_ts", cluster_shared_data_dir, i);
		UT_ASSERT_EQ(rmdir(path), 0);
	}
	if (*inputs != NULL)
		scope_initialized_end(inputs, history, fds);
	else {
		UT_ASSERT(scope_handoff_replaying && !scope_handoff_published);
		UT_ASSERT(!inputs_pin_held && ctrc_set_held == 0);
		test_scope_startup_detach(&tx_inputs_typed);
		scope_handoff_replaying = false;
		test_restart_ref_valid = false;
		for (unsigned i = 0; i < 2; ++i)
			rf_side_online_plan_destroy_v1(&tx_inputs_typed.side_plans[i]);
		scope_initialized_storage_end();
	}
	if (scope_handoff_original_plan)
		cluster_cold_plan_destroy_v1(&tx_inputs_typed.plan);
}

UT_TEST(test_scope_handoff_reuses_native_receipt_without_replaying_after_eor)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	const ClusterRelmapRetentionCensus *census = NULL;
	ClusterRelmapCensusCommitted committed, zero = {0};
	int fds[2] = {-1, -1};
	bool ready;

	scope_handoff_fault = 0;
	ready = scope_handoff_begin(&inputs, &history, &pending);
	if (!ready)
		fprintf(stderr, "handoff refusal=%u detail=%s error=%u calls=%u foreign=%u consumed=%u redos=%u sync=%u/%u\n",
			tx_inputs_typed.refusal, tx_inputs_typed.refusal_detail, scope_replay_error,
			scope_replay_calls, scope_replay_foreign, scope_replay_consumed,
			scope_replay_stats.redos, scope_replay_stats.file_syncs, scope_replay_stats.directory_syncs);
	UT_ASSERT(ready);
	if (ready) {
		unsigned calls = scope_replay_calls;
		UT_ASSERT_EQ(calls, 1);
		UT_ASSERT(scope_replay_consumed && scope_replay_stats.returned);
		UT_ASSERT_EQ(scope_replay_foreign, 1);
		UT_ASSERT(scope_replay_stats.file_syncs > 0 && scope_replay_stats.directory_syncs > 0);
		UT_ASSERT(!RecoveryInProgress());
		UT_ASSERT(scope_initialized_read(history, &pending, fds));
		for (unsigned i = 0; i < 2; ++i) {
			ClusterRecoveryRelmapNativePageV1 page;
			uint8 expected[BLCKSZ] = {0};
			UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
			if (page.native.origin == 0) expected[208] = TRANSACTION_STATUS_COMMITTED;
			UT_ASSERT_EQ(memcmp(page.native.image, expected, sizeof(expected)), 0);
		}
		UT_ASSERT(scope_native_prepare(inputs, &census));
		UT_ASSERT(census != NULL);
		if (census != NULL)
			UT_ASSERT_EQ(cluster_relmap_retention_census_next((ClusterRelmapRetentionCensus *)census),
				CLUSTER_RELMAP_CENSUS_DRAINED);
		UT_ASSERT(scope_initialized_read(history, &pending, fds));
		memset(&committed, 0xa5, sizeof(committed));
		UT_ASSERT(!cluster_relmap_retention_census_committed_cold((ClusterRelmapRetentionCensus *)census,
			InvalidOid, &committed));
		UT_ASSERT_EQ(memcmp(&committed, &zero, sizeof(zero)), 0);
		UT_ASSERT_EQ(scope_replay_calls, calls);
		UT_ASSERT_EQ(scope_handoff_wait_event, 0);
		UT_ASSERT_EQ(relmap_failed | ctrc_set_release_calls | scope_native_legacy_calls, 0);
	}
	scope_handoff_end(&inputs, &history, fds);
}

UT_TEST(test_scope_handoff_original_plan_refuses_missing_relmap_declaration)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	int fds[2] = {-1, -1};
	bool ready;

	scope_handoff_fault = 0;
	scope_handoff_original_plan = true;
	ready = scope_handoff_begin(&inputs, &history, &pending);
	scope_handoff_end(&inputs, &history, fds);
	scope_handoff_original_plan = false;
	UT_ASSERT_EQ(scope_handoff_plan_result, CLUSTER_COLD_STRUCTURAL_UNSUPPORTED);
	UT_ASSERT(!ready && !scope_handoff_published && scope_replay_calls == 0);
}

UT_TEST(test_scope_handoff_native_failure_cannot_publish_or_retry)
{
	for (unsigned fault = 1; fault <= 3; ++fault) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		int fds[2] = {-1, -1};
		unsigned calls;
		volatile bool retried = false;

		scope_handoff_fault = fault;
		UT_ASSERT(!scope_handoff_begin(&inputs, &history, &pending));
		UT_ASSERT(inputs == NULL && history == NULL && !scope_handoff_published);
		UT_ASSERT(RecoveryInProgress() && scope_handoff_replaying);
		UT_ASSERT(!inputs_pin_held && ctrc_set_calls == 0 && ctrc_set_held == 0);
		UT_ASSERT_EQ(scope_replay_calls, 1);
		UT_ASSERT(!scope_replay_consumed);
		UT_ASSERT_EQ(scope_replay_error, fault <= 2);
		UT_ASSERT_EQ(scope_replay_stats.returned, fault == 3);
		UT_ASSERT_EQ(scope_replay_stats.redos, 1);
		UT_ASSERT(!cluster_cold_typed_clog_native_record_current_v1(0, NULL));
		for (unsigned i = 0; i < 2; ++i) {
			ClusterControlRootSnapshot root;
			ClusterControlRootReadToken token;
			UT_ASSERT(tx_inputs_typed.side_plans[i] == scope_handoff_plans[i]);
			UT_ASSERT_EQ(cluster_control_root_read_canonical(
				scope_handoff_selected[i].source.claim.identity.origin_thread_id,
				&scope_handoff_selected[i].source.claim.identity, CLUSTER_CONTROL_ROOT_READ_STRONG,
				&root, &token), 0);
			UT_ASSERT_EQ(root.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED);
		}
		calls = scope_replay_calls;
		scope_handoff_fault = 0;
		test_capture_error_level = true;
		PG_TRY(); { retried = scope_handoff_replay(0); }
		PG_CATCH(); { UT_ASSERT(false); } PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT(!retried && scope_replay_calls == calls);
		scope_handoff_end(&inputs, &history, fds);
	}
	inputs_native_recovery = false; /* Fixture teardown, not EOR publication. */
}

UT_TEST(test_scope_handoff_late_census_failures_keep_receipts_and_held_pages)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		const ClusterRelmapRetentionCensus *census = NULL;
		int fds[2] = {-1, -1};
		volatile bool ok = false, caught = false;

		scope_handoff_fault = 0;
		UT_ASSERT(scope_handoff_begin(&inputs, &history, &pending));
		UT_ASSERT(scope_initialized_read(history, &pending, fds));
		if (fault == 0)
			UT_ASSERT_EQ(unlinkat(scope_native_fd, "base/5/pgrac_relmap_authority.bak", 0), 0);
		else if (fault == 1) relmap_sibling->release_uncertain = true;
		else ctrc_replay_current_override = relmap_episode_terminal_error_current;
		test_capture_error_level = true;
		PG_TRY(); { ok = scope_native_prepare(inputs, &census); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		test_capture_error_level = false;
		ctrc_replay_current_override = ctrc_set_original_current;
		UT_ASSERT(!ok && census == NULL);
		UT_ASSERT_EQ(caught, fault == 2);
		scope_replay_refused(inputs, history, &pending, fds);
		UT_ASSERT_EQ(scope_replay_calls, 1);
		UT_ASSERT(scope_replay_consumed && scope_replay_stats.returned);
		for (unsigned i = 0; i < 2; ++i)
			UT_ASSERT(tx_inputs_typed.side_plans[i] == scope_handoff_plans[i]);
		if (fault == 1) relmap_sibling->release_uncertain = false;
		scope_handoff_end(&inputs, &history, fds);
	}
}
