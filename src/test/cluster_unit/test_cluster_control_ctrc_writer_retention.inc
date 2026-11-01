/*-------------------------------------------------------------------------
 * test_cluster_control_ctrc_writer_retention.inc
 *     Original all-replay custody also retains the new recovery writer.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * NOTES
 *     Native WAL inputs/selection execute. Startup and GES are fixture leaves.
 *-------------------------------------------------------------------------
 */
static void
ctrc_writer_retention_fixture(ClusterWalInputsV1 **inputs, unsigned writer_index)
{
	ctrc_set_fixture(3, true, inputs);
	cluster_wal_inputs_release_v1(inputs); ctrc_ir_original_scope = NULL;
	ctrc_inputs_owner.writer.ref = tx_inputs_typed.sources[writer_index];
	ctrc_inputs_owner.writer.startup_first_lsn = 0x1000000;
	UT_ASSERT_EQ(ctrc_inputs_begin(inputs), 0);
	ctrc_ir_original_scope = *inputs;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(*inputs), 0);
}

UT_TEST(test_ctrc_writer_retention_binds_both_original_sources_without_new_cut)
{
	for (unsigned writer_index = 0; writer_index < 2; writer_index++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterWalWriterToken writer;
		unsigned cf, grants, releases;
		uint64 pin;
		ctrc_writer_retention_fixture(&inputs, writer_index);
		writer = ctrc_inputs_owner.writer;
		cf = test_cf_lock_calls; grants = test_tx_ir_grants;
		releases = test_tx_ir_releases; pin = inputs_pin_generation;
		for (unsigned index = 0; index < 2; index++) {
			ClusterRecoveryCtrcReplaySetSourceV1 source, original;
			source = original = cluster_wal_inputs_ctrc_replays_source_v1(inputs, index);
			UT_ASSERT(cluster_wal_inputs_ctrc_writer_retained_v1(&source, &writer));
			UT_ASSERT(memcmp(&source, &original, sizeof(source)) == 0);
		}
		UT_ASSERT_EQ(test_cf_lock_calls, cf); UT_ASSERT_EQ(test_tx_ir_grants, grants);
		UT_ASSERT_EQ(test_tx_ir_releases, releases); UT_ASSERT_EQ(inputs_pin_generation, pin);
		UT_ASSERT(inputs_recovery_held && ctrc_set_held == 2);
		ctrc_set_finish(&inputs);
	}
}

UT_TEST(test_ctrc_writer_retention_rejects_different_complete_writer_or_source)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryCtrcReplaySetSourceV1 source, changed;
	ClusterWalWriterToken writer, different;
	ctrc_writer_retention_fixture(&inputs, 1);
	source = cluster_wal_inputs_ctrc_replays_source_v1(inputs, 0);
	writer = ctrc_inputs_owner.writer;
	UT_ASSERT(!cluster_wal_inputs_ctrc_writer_retained_v1(NULL, &writer));
	UT_ASSERT(!cluster_wal_inputs_ctrc_writer_retained_v1(&source, NULL));
	for (unsigned field = 0; field < 7; field++) {
		different = writer;
		switch (field) {
		case 0: different.epoch++; break;
		case 1: different.ref.timeline++; break;
		case 2: different.startup_first_lsn++; break;
		case 3: different.ref.claim.claim_sha256[31]++; break;
		case 4: different.ref.claim.identity.origin_owner_incarnation++; break;
		case 5: different.ref.claim.max_config_generation++; break;
		case 6: different.ref.claim.database_incarnation++; break;
		}
		UT_ASSERT(!cluster_wal_inputs_ctrc_writer_retained_v1(&source, &different));
	}
	changed = source; changed.input_owner.generation++;
	UT_ASSERT(!cluster_wal_inputs_ctrc_writer_retained_v1(&changed, &writer));
	changed = source; changed.root.file_txn_seq++;
	UT_ASSERT(!cluster_wal_inputs_ctrc_writer_retained_v1(&changed, &writer));
	UT_ASSERT(cluster_wal_inputs_ctrc_writer_retained_v1(&source, &writer));
	ctrc_set_finish(&inputs);
}

UT_TEST(test_ctrc_writer_retention_requires_actual_writer_thread_not_first_pin)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryCtrcReplaySetSourceV1 source;
	ClusterWalWriterToken writer;
	ctrc_writer_retention_fixture(&inputs, 0);
	source = cluster_wal_inputs_ctrc_replays_source_v1(inputs, 1);
	writer = ctrc_inputs_owner.writer;
	UT_ASSERT_EQ(writer.ref.claim.identity.origin_thread_id, 128);
	UT_ASSERT_EQ(inputs_pin_threads[0], 1); UT_ASSERT_EQ(inputs_pin_threads[1], 128);
	inputs_pin_threads[1] = 2;
	UT_ASSERT(cluster_wal_inputs_ctrc_replays_source_current_v1(&source));
	UT_ASSERT(!cluster_wal_inputs_ctrc_writer_retained_v1(&source, &writer));
	UT_ASSERT(inputs_recovery_held && ctrc_set_held == 2 && test_tx_ir_releases == 0);
	inputs_pin_threads[1] = 128;
	ctrc_set_finish(&inputs);
}

UT_TEST(test_ctrc_writer_retention_loss_preserves_original_cleanup_debt)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryCtrcReplaySetSourceV1 source;
		ClusterWalWriterToken writer;
		ctrc_writer_retention_fixture(&inputs, 1);
		source = cluster_wal_inputs_ctrc_replays_source_v1(inputs, 0);
		writer = ctrc_inputs_owner.writer;
		if (fault == 0) inputs_pin_current = false;
		if (fault == 1) test_input_current = false;
		if (fault == 2) ctrc_inputs_work.generation++;
		if (fault == 3) CritSectionCount++;
		if (fault == 4) ShutdownRequestPending = true;
		UT_ASSERT(!cluster_wal_inputs_ctrc_writer_retained_v1(&source, &writer));
		UT_ASSERT(inputs_recovery_held && ctrc_set_held == 2 && test_tx_ir_releases == 0);
		inputs_pin_current = test_input_current = true;
		if (fault == 2) ctrc_inputs_work.generation--;
		if (fault == 3) CritSectionCount--;
		ShutdownRequestPending = false;
		ctrc_set_finish(&inputs);
	}
}

UT_TEST(test_ctrc_writer_retention_old_incarnation_cannot_borrow_reopened_scope)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryCtrcReplaySetSourceV1 source;
	ClusterWalWriterToken writer;
	ctrc_writer_retention_fixture(&inputs, 1);
	source = cluster_wal_inputs_ctrc_replays_source_v1(inputs, 0);
	writer = ctrc_inputs_owner.writer;
	ctrc_set_finish(&inputs);
	UT_ASSERT(!cluster_wal_inputs_ctrc_writer_retained_v1(&source, &writer));
	ctrc_writer_retention_fixture(&inputs, 1);
	UT_ASSERT(!cluster_wal_inputs_ctrc_writer_retained_v1(&source, &writer));
	UT_ASSERT(inputs_recovery_held && ctrc_set_held == 2);
	ctrc_set_finish(&inputs);
}

static ClusterRecoveryCtrcReplaySetSourceV1 *ctrc_writer_changed_source;
static ClusterWalWriterToken *ctrc_writer_changed_writer;
static bool
ctrc_writer_retention_change(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 index, ClusterWalInputsV1 *scope,
	const ClusterRecoverySerialRequest *request)
{
	bool current = ctrc_set_original_current(start, work, index, scope, request);
	if (ctrc_writer_changed_source != NULL) {
		ctrc_writer_changed_source->work_generation++; ctrc_writer_changed_source = NULL;
	}
	if (ctrc_writer_changed_writer != NULL) {
		ctrc_writer_changed_writer->epoch++; ctrc_writer_changed_writer = NULL;
	}
	return current;
}

UT_TEST(test_ctrc_writer_retention_rechecks_complete_inputs_after_original_callbacks)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryCtrcReplaySetSourceV1 source;
		ClusterWalWriterToken writer;
		ctrc_writer_retention_fixture(&inputs, 1);
		source = cluster_wal_inputs_ctrc_replays_source_v1(inputs, 0);
		writer = ctrc_inputs_owner.writer;
		if (fault == 0) ctrc_writer_changed_source = &source;
		else ctrc_writer_changed_writer = &writer;
		ctrc_replay_current_override = ctrc_writer_retention_change;
		UT_ASSERT(!cluster_wal_inputs_ctrc_writer_retained_v1(&source, &writer));
		UT_ASSERT(ctrc_writer_changed_source == NULL && ctrc_writer_changed_writer == NULL);
		UT_ASSERT(inputs_recovery_held && ctrc_set_held == 2 && test_tx_ir_releases == 0);
		ctrc_set_finish(&inputs);
	}
}
