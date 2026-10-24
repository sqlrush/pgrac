/* Original native/CP/provider/exact-retire chain over two real file sets.
 * PCM/WB, GES and WAL are explicit stateful fixture boundaries; no fixed
 * READY result can finish LOCAL before its 2048-slot scan actually ends.
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 */
#define DROP_WORK_NO_MAIN
#include "test_cluster_drop_work.c"
#undef DROP_WORK_NO_MAIN

static RelFileNumber z_objects[2];
static unsigned z_selected[2], z_order[128], z_order_count;
static bool z_first_pending;
static bool z_fail_first;

static void
z_sync_before_own_unlinks(int fd)
{
	const char *suffix[] = {"", "_fsm", "_vm", "_init", "_space"};
	struct stat actual, candidate;
	char path[MAXPGPATH];
	bool found = false;
	UT_ASSERT_EQ(fstat(fd, &actual), 0);
	for (unsigned object = 0; object < 2; object++) {
		snprintf(path, sizeof(path), "%s/%u", work_parent, z_objects[object]);
		UT_ASSERT_EQ(stat(path, &candidate), 0);
		if (actual.st_dev != candidate.st_dev || actual.st_ino != candidate.st_ino)
			continue;
		found = true;
		for (unsigned fork = 1; fork <= MAX_FORKNUM; fork++) {
			snprintf(path, sizeof(path), "%s/%u%s", work_parent, z_objects[object], suffix[fork]);
			UT_ASSERT_EQ(access(path, F_OK), 0);
		}
	}
	UT_ASSERT(found);
}

static void
z_local(RelFileLocator locator)
{
	unsigned object = locator.relNumber == z_objects[0] ? 0 : 1;
	UT_ASSERT_EQ(locator.relNumber, z_objects[object]);
	local_scan_rel_number = locator.relNumber;
	z_selected[object]++;
	if (z_order_count < lengthof(z_order))
		z_order[z_order_count++] = object;
	local_relation_scan_advance = 128;
	local_relation_scan_result = local_relation_scan_input + 128 < 2048
		? CLUSTER_PCM_PI_RELATION_MORE : CLUSTER_PCM_PI_RELATION_EMPTY;
	if (object == 0 && z_first_pending) {
		local_relation_scan_result = CLUSTER_PCM_PI_RELATION_PENDING;
		local_relation_scan_advance = 0;
	}
	work_fault = object == 0 && z_fail_first ? WORK_MAIN_SYNC : WORK_OK;
}

static bool
z_setup(bool second)
{
	RelationData relation = {0};
	ClusterKoCompletionV2 *claimed = NULL;
	ClusterSpaceStructureChange change;
	Oid database;
	static unsigned sequence;
	char name[64];
	ko_stop_requested = false;
	ko_continue_allowed = true;
#ifdef Z_REAL_WB_PROOF
	cluster_pi_writeback_shmem_register_v1();
#endif

	snprintf(name, sizeof(name), "z_%u_%u", second, ++sequence);
	if (!work_setup_v3(name))
		return false;
	z_objects[0] = space_identity.key.locator.relNumber;
	memset(z_selected, 0, sizeof(z_selected));
	z_order_count = 0;
	z_first_pending = false;
	z_fail_first = false;
	if (second) {
		/* Same original producer and post-lock path, without resetting the
		 * first retained native/file responsibility. */
		MyBackendType = B_BACKEND;
		native_commit_active = false;
		multiple_barriers = true;
		allocated_batch = last_shared_request.batch_id;
		space_identity.key.locator.relNumber++;
		local_scan_rel_number = space_identity.key.locator.relNumber;
		barrier_requests = 0; /* Per-producer fixture expectation, not shared work. */
		relation.rd_id = 4243;
		relation.rd_locator = space_identity.key.locator;
		database = MyDatabaseId;
		MyDatabaseId = relation.rd_locator.dbOid;
		cluster_ko_flush_and_wait_ack_relation(&relation, RELPERSISTENCE_PERMANENT,
			CLUSTER_KO_SHARED_V3_OPERATION_DROP);
		MyDatabaseId = database;
		postcommit_owner = ko_completions;
		UT_ASSERT(postcommit_owner != NULL);
		if (postcommit_owner == NULL)
			return false;
		UT_ASSERT(cluster_ko_shared_claim_structure(&space_identity,
			CLUSTER_SPACE_WAL_TOMBSTONE, &claimed));
		UT_ASSERT(claimed == postcommit_owner);
		v3_structure_record(true, space_identity.operation, &postcommit_binding, postcommit_wal);
		UT_ASSERT(cluster_ko_shared_observe_space_v2(postcommit_owner,
			&postcommit_binding, postcommit_wal, sizeof(postcommit_wal)));
		postcommit_deletes = postcommit_phases = 0;
		postcommit_native_count = 1;
		postcommit_storage_consumer = cluster_smgr_unlink_committed_drop;
		native_commit_active = true;
		run_native_postcommit();
		postcommit_storage_consumer = NULL;
		MyBackendType = B_CHECKPOINTER;
		UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PROGRESS);
		UT_ASSERT(cluster_space_structure_wal_decode(postcommit_wal,
			sizeof(postcommit_wal), &change));
		prepare_files(&change);
	}
	z_objects[1] = space_identity.key.locator.relNumber;
	local_relation_scan_hook = z_local;
	work_main_sync_observe = second ? z_sync_before_own_unlinks : NULL;
	ko_drop_poll_boundary = cluster_smgr_drop_work_poll;
	ko_drop_scan = 0;
	return true;
}

static void
z_release_ax(void)
{
	for (unsigned slot = 0; slot < CLUSTER_KO_SHARED_CAPACITY; slot++) {
		ClusterKoSharedContext context = storage.contexts[slot];
		LOCKTAG tag;
		if (!context.used || context.structure_owned)
			continue;
		SET_LOCKTAG_RELATION(tag, context.request_v3.ax_resid.field1,
			context.request_v3.ax_resid.field2);
		UT_ASSERT(cluster_ko_ax_release_handoff_v3(&tag, AccessExclusiveLock,
			&context.request_v3.ax_holder));
	}
	release_poll_result = CLUSTER_GES_REDECLARE_CONFIRMED;
	for (unsigned tick = 0; tick < CLUSTER_KO_SHARED_CAPACITY; tick++)
		shared_send_tick();
}

UT_TEST(test_z_partial_local_progress_is_visible_without_provider_effects)
{
	if (!z_setup(false)) return;
	UT_ASSERT(cluster_ko_shared_native_poll_v2());
	UT_ASSERT_EQ(z_selected[0], 1);
	UT_ASSERT_EQ(ko_drop_work[work_slot]->local_scan_cursor, 128);
	assert_no_drop_io();
}

UT_TEST(test_z_changed_cut_and_real_pending_do_not_claim_progress)
{
	if (!z_setup(false)) return;
	z_first_pending = true;
	UT_ASSERT(!cluster_ko_shared_native_poll_v2());
	assert_no_drop_io();
	z_first_pending = false;
	local_relation_scan_drift = 2;
	UT_ASSERT(!cluster_ko_shared_native_poll_v2());
	UT_ASSERT_EQ(ko_drop_work[work_slot]->local_scan_cursor, 0);
	local_relation_scan_drift = 0;
	UT_ASSERT(cluster_ko_shared_native_poll_v2());
	UT_ASSERT_EQ(local_relation_scan_input, 0);
	UT_ASSERT_EQ(ko_drop_work[work_slot]->local_scan_cursor, 128);
}

UT_TEST(test_z_two_objects_reach_real_file_retirement_before_stop_idle)
{
	const char *reason;
	unsigned polls = 0, pending = 0;
	char paths[2][MAXPGPATH];
	struct stat st;
	if (!z_setup(true)) return;
	for (unsigned i = 0; i < 2; i++)
		snprintf(paths[i], sizeof(paths[i]), "%s/%u", work_parent, z_objects[i]);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
	while (z_selected[0] < 16 || z_selected[1] < 16) {
		if (++polls > 80) break;
		if (!cluster_ko_shared_native_poll_v2()) pending++;
		if (polls == 8)
			UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
	}
	UT_ASSERT_EQ(z_selected[0], 16);
	UT_ASSERT_EQ(z_selected[1], 16);
	UT_ASSERT_EQ(polls, 32);
	UT_ASSERT_EQ(pending, 0);
	for (unsigned i = 0; i < 32 && i < z_order_count; i++)
		UT_ASSERT_EQ(z_order[i], i % 2);
	UT_ASSERT_EQ(truncates, 2);
	UT_ASSERT_EQ(main_syncs, 2);
	UT_ASSERT_EQ(dir_syncs, 2);
	UT_ASSERT_EQ(external_fds, 0);
	for (unsigned i = 0; i < 2; i++) {
		UT_ASSERT_EQ(stat(paths[i], &st), 0);
		UT_ASSERT_EQ(st.st_size, 0);
	}
	/* File completion alone cannot grant idle while independent AX remains. */
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
	z_release_ax();
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_z_pending_object_cannot_starve_other_object_or_claim_idle)
{
	const char *reason;
	if (!z_setup(true)) return;
	z_first_pending = true;
	for (unsigned i = 0; i < 32; i++)
		(void)cluster_ko_shared_native_poll_v2();
	UT_ASSERT_EQ(z_selected[0], 16);
	UT_ASSERT_EQ(z_selected[1], 16);
	UT_ASSERT_EQ(main_syncs, 1);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
	z_first_pending = false;
	for (unsigned i = 0; i < 16; i++)
		UT_ASSERT(cluster_ko_shared_native_poll_v2());
	UT_ASSERT_EQ(main_syncs, 2);
	z_release_ax();
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_z_real_io_failure_retains_exact_context_and_error)
{
	ClusterKoDropTraceV1 trace;
	const char *reason;
	if (!z_setup(true)) return;
	z_fail_first = true;
	for (unsigned i = 0; i < 32; i++)
		(void)cluster_ko_shared_native_poll_v2();
	UT_ASSERT_EQ(dir_syncs, 1);
	UT_ASSERT_EQ(external_fds, 0);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(reason, "KO_SHARED_DROP_RECOVERY_REQUIRED");
	UT_ASSERT(cluster_ko_drop_pending_v1(&trace));
	if (!cluster_ko_drop_pending_v1(&trace)) return;
	UT_ASSERT_EQ(trace.locator.relNumber, z_objects[0]);
	UT_ASSERT(trace.failed && trace.observed && trace.io_attempted);
	UT_ASSERT_EQ(trace.phase, CLUSTER_KO_DROP_TRACE_PROVIDER);
	UT_ASSERT_EQ(trace.step, SHAREDFS_DROP_TRACE_MAIN_SYNC);
	UT_ASSERT_EQ(trace.io_error, EIO);
	UT_ASSERT(storage.contexts[work_slot].structure_drop_failed);
}

UT_TEST(test_z_changed_context_never_relabels_old_prefix_or_retires_new_identity)
{
	ClusterKoDropTraceV1 trace;
	if (!z_setup(false)) return;
	UT_ASSERT(cluster_ko_shared_native_poll_v2());
	storage.contexts[work_slot].serial++;
	UT_ASSERT(!cluster_ko_shared_native_poll_v2());
	UT_ASSERT(cluster_ko_drop_pending_v1(&trace));
	UT_ASSERT(!trace.observed);
	UT_ASSERT_EQ(trace.serial, storage.contexts[work_slot].serial);
	UT_ASSERT(storage.contexts[work_slot].used);
	assert_no_drop_io();
}

UT_TEST(test_z_pending_observation_binds_original_context_and_local_prefix)
{
	ClusterKoDropTraceV1 trace;
	if (!z_setup(false)) return;
	UT_ASSERT(cluster_ko_shared_native_poll_v2());
	UT_ASSERT(cluster_ko_drop_pending_v1(&trace));
	if (!cluster_ko_drop_pending_v1(&trace)) return;
	UT_ASSERT(trace.observed);
	UT_ASSERT_EQ(trace.locator.relNumber, z_objects[0]);
	UT_ASSERT_EQ(trace.serial, storage.contexts[work_slot].serial);
	UT_ASSERT_EQ(trace.batch, storage.contexts[work_slot].request.batch_id);
	UT_ASSERT_EQ(trace.phase, CLUSTER_KO_DROP_TRACE_LOCAL);
	UT_ASSERT_EQ(trace.cursor, 128);
	UT_ASSERT_EQ(trace.prefix_cursor, 128);
	UT_ASSERT_EQ(trace.local_result, CLUSTER_PCM_PI_RELATION_MORE);
	UT_ASSERT_EQ(trace.selections, 1);
	UT_ASSERT(!trace.io_attempted);
	UT_ASSERT_EQ(trace.io_error, 0);
}

#ifndef Z_DROP_NO_MAIN
int
main(void)
{
	setvbuf(stdout, NULL, _IOLBF, 0);
	UT_PLAN(45);
	UT_RUN(test_z_pending_object_cannot_starve_other_object_or_claim_idle);
	UT_RUN(test_z_real_io_failure_retains_exact_context_and_error);
	UT_RUN(test_z_changed_context_never_relabels_old_prefix_or_retires_new_identity);
	UT_RUN(test_z_pending_observation_binds_original_context_and_local_prefix);
	UT_RUN(test_z_partial_local_progress_is_visible_without_provider_effects);
	UT_RUN(test_z_changed_cut_and_real_pending_do_not_claim_progress);
	UT_RUN(test_z_two_objects_reach_real_file_retirement_before_stop_idle);
	UT_RUN(test_v3_origin_closed_uses_resource_formation_and_membership_epoch);
	UT_RUN(test_v3_origin_resource_formation_change_restarts_local_prefix);
	UT_RUN(test_v3_native_finish_releases_only_file_context_without_legacy_offer);
	UT_RUN(test_v3_native_finish_rechecks_local_cut_before_releasing_context);
	UT_RUN(test_v3_native_failed_io_keeps_original_recovery_obligation);
	UT_RUN(test_v3_origin_local_pending_invalid_or_partial_never_opens_files);
	UT_RUN(test_v3_origin_scan_retains_prefix_only_for_same_cut);
	UT_RUN(test_v3_origin_unaccepted_physical_ack_blocks_deletion);
	UT_RUN(test_v3_origin_cannot_carry_old_prefix_into_new_sample);
	UT_RUN(test_v3_origin_census_drift_error_and_bad_progress_expire_hint);
	UT_RUN(test_v3_origin_rechecks_local_ack_with_retained_directory_fd);
	UT_RUN(test_v3_origin_finite_local_owner_still_blocks_empty_census);
	UT_RUN(test_failed_sync_abandons_original_fds_and_requires_recovery);
	UT_RUN(test_replaced_cut_cleanup_closes_each_original_fd_once);
	UT_RUN(test_known_failure_is_not_hidden_by_unavailable_formation);
	UT_RUN(test_full_replaced_work_table_returns_descriptor_credits);
	UT_RUN(test_abandoned_work_scan_does_not_starve_healthy_work);
	UT_RUN(test_unknown_cut_and_wrong_owner_cannot_dispose_retryable_fds);
	UT_RUN(test_work_success_keeps_main_and_structure_obligation);
	UT_RUN(test_work_absent_auxiliary_forks_are_not_unlink_completions);
	UT_RUN(test_work_truncate_retry);
	UT_RUN(test_work_main_fsync_failure_requires_recovery);
	UT_RUN(test_work_aux_unlink_retry);
	UT_RUN(test_work_partial_aux_retry);
	UT_RUN(test_work_directory_sync_failure_requires_recovery);
	UT_RUN(test_work_permanent_failure_is_bounded_and_keeps_responsibility);
	UT_RUN(test_work_finish_retry_never_reopens_or_repeats_io);
	UT_RUN(test_work_retry_rejects_replacement_aux_without_unlinking_it);
	UT_RUN(test_work_short_space_and_fsync_off_never_mutate_main);
	UT_RUN(test_work_scan_completes_next_item_after_first_io_failure);
	UT_RUN(test_work_cut_checked_before_main_sync);
	UT_RUN(test_work_cut_checked_before_aux_unlink);
	UT_RUN(test_work_cut_checked_between_aux_unlinks);
	UT_RUN(test_work_ambiguous_unlink_uses_original_unlinked_inode);
	UT_RUN(test_work_enoent_with_linked_original_never_finishes);
	UT_RUN(test_work_nfs_refused_before_first_mutation);
	UT_RUN(test_work_close_error_never_finishes_or_retries_unknown_descriptor);
	UT_RUN(test_work_executor_process_exit_does_not_redo_partial_io);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
#endif
