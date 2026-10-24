/* Actual ROOT/claims/anchors/retained WAL; only original Startup/work ownership
 * is a fixture leaf (its producer is tested in cold_tx_start/cold_tt_redo).
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
static union {
	uint64 align;
	unsigned char bytes[offsetof(ClusterColdCtrcWorkV1, entries)
		+ 2 * sizeof(ClusterColdTtSlotInputV1)];
} ctrc_inputs_work_storage;
#define ctrc_inputs_work (*(ClusterColdCtrcWorkV1 *)ctrc_inputs_work_storage.bytes)
static ClusterColdTopTxCensusV1 ctrc_inputs_top;
static ClusterRecoveryCheckpointOwnerV1 ctrc_inputs_owner;

bool
ClusterColdRecoveryCtrcWorkCurrent(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work)
{
	return ClusterColdRecoveryTxCurrent(start) && work != NULL
		&& work == tx_inputs_typed.ctrc_work && work->plan == tx_inputs_typed.plan
		&& work->top_tx_census == tx_inputs_typed.top_tx_census
		&& work->generation != 0 && work->generation == tx_inputs_typed.ctrc_work_generation;
}

bool
ClusterColdRecoveryTxCheckpointOwnerV1(const struct ClusterRecoveryStartV1 *start,
	ClusterRecoveryCheckpointOwnerV1 *out)
{
	memset(out, 0, sizeof(*out));
	if (!ClusterColdRecoveryTxCurrent(start))
		return false;
	*out = ctrc_inputs_owner;
	out->start_generation = tx_inputs_generation;
	out->start_serial = tx_inputs_serial;
	return true;
}

static void
ctrc_inputs_activate_work(void)
{
	memset(&ctrc_inputs_work_storage, 0, sizeof(ctrc_inputs_work_storage));
	memset(&ctrc_inputs_top, 0, sizeof(ctrc_inputs_top));
	memset(&ctrc_inputs_owner, 0, sizeof(ctrc_inputs_owner));
	tx_inputs_typed.plan = (ClusterColdPlanV1 *)&tx_inputs_typed;
	tx_inputs_typed.top_tx_census = &ctrc_inputs_top;
	ctrc_inputs_top.plan = tx_inputs_typed.plan;
	ctrc_inputs_work.plan = tx_inputs_typed.plan;
	ctrc_inputs_work.top_tx_census = &ctrc_inputs_top;
	ctrc_inputs_work.generation = ++tx_inputs_typed.ctrc_work_generation;
	ctrc_inputs_work.counts.subject_count = 2;
	ctrc_inputs_work.counts.unresolved_count = 2;
	ctrc_inputs_work.entries[0] = tx_inputs_subject.input;
	ctrc_inputs_work.entries[0].slot.status = TT_SLOT_ABORTED;
	ctrc_inputs_work.entries[1] = tx_inputs_subject.input;
	ctrc_inputs_work.entries[1].key.slot_offset++;
	ctrc_inputs_work.entries[1].slot.status = TT_SLOT_COMMITTED;
	tx_inputs_typed.ctrc_work = &ctrc_inputs_work;
	/* No ACTIVE subject may be invented to obtain this read scope. */
	tx_inputs_subject.input.slot.status = TT_SLOT_ABORTED;
	ctrc_inputs_owner.writer.epoch = 17;
	ctrc_inputs_owner.operation_uuid[0] = 1;
	ctrc_inputs_owner.install_generation = 7;
	ctrc_inputs_owner.owner_boot = 19;
	ctrc_inputs_owner.owner_pid = getpid();
}

static void
ctrc_inputs_fixture(void)
{
	tx_inputs_fixture();
	ctrc_inputs_activate_work();
}

static ClusterControlRootResult
ctrc_inputs_begin(ClusterWalInputsV1 **out)
{
	return cluster_wal_inputs_begin_ctrc_census_v1(tx_inputs_start, &ctrc_inputs_work, out);
}

UT_TEST(test_ctrc_inputs_terminal_work_without_active_reads_all_retained_sources)
{
	ClusterWalInputsV1 *inputs = NULL;
	ColdCensusTest work = { 0 };
	uint64 records = 99;
	RfPageProofDetailV1 detail;
	ClusterWalTailObservation tail;
	InputsVisitTest visit = { 0 };

	ctrc_inputs_fixture();
	UT_ASSERT_EQ(ctrc_inputs_top.count, 0);
	UT_ASSERT_EQ(tx_inputs_begin(&inputs), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	inputs_claim_observe = true;
	inputs_claim_calls = inputs_claim_under_cf = inputs_claim_unpinned = 0;
	UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
	UT_ASSERT_NOT_NULL(inputs);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(inputs), 2);
	UT_ASSERT_EQ(inputs_claim_calls, 2);
	UT_ASSERT_EQ(inputs_claim_under_cf + inputs_claim_unpinned, 0);
	for (uint32 i = 0; i < 2; i++) {
		UT_ASSERT_EQ(cluster_wal_inputs_visit_retained_v1(inputs, i, inputs_visit_record,
			&visit, &tail), 0);
		UT_ASSERT_EQ(tail.records, 1);
	}
	UT_ASSERT_EQ(visit.calls, 2);
	UT_ASSERT_EQ(cluster_wal_inputs_cold_census_v1(inputs, cold_census_record,
		cold_census_source, &work, &records, &detail), 0);
	UT_ASSERT_EQ(records, 2);
	UT_ASSERT_EQ(work.begins, 2);
	UT_ASSERT_EQ(work.ends, 2);
	UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), 0);
	cluster_wal_inputs_release_v1(&inputs);
	UT_ASSERT(!inputs_pin_held && test_actual_cf == NoLock);
	UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
	inputs_claim_observe = false;
	test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_inputs_requires_exact_original_work_and_post_eor_start)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		ClusterWalInputsV1 *inputs = (ClusterWalInputsV1 *)(uintptr_t)1;
		const ClusterColdCtrcWorkV1 *work;
		const struct ClusterRecoveryStartV1 *start;
		ClusterColdCtrcWorkV1 copy;

		ctrc_inputs_fixture();
		work = &ctrc_inputs_work;
		start = tx_inputs_start;
		copy = *work;
		if (fault == 0) work = &copy;
		if (fault == 1) work = NULL;
		if (fault == 2) work = (const ClusterColdCtrcWorkV1 *)(uintptr_t)1;
		if (fault == 3) start = (const struct ClusterRecoveryStartV1 *)(uintptr_t)1;
		if (fault == 4) tx_inputs_typed.ctrc_work = NULL;
		if (fault == 5) tx_inputs_typed.census_root_captured = false;
		if (fault == 6) ctrc_inputs_work.generation = 0;
		if (fault == 7) ctrc_inputs_work.top_tx_census = NULL;
		if (fault == 8) MyBackendType = B_BG_WORKER;
		if (fault == 9) inputs_native_recovery = true;
		UT_ASSERT_EQ(cluster_wal_inputs_begin_ctrc_census_v1(start, work, &inputs),
			CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		UT_ASSERT_NULL(inputs);
		UT_ASSERT(!inputs_pin_held && test_actual_cf == NoLock);
	}
	test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_inputs_each_original_owner_and_work_version_is_rechecked)
{
	for (unsigned fault = 0; fault < 16; fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		ResourceOwner owner;

		ctrc_inputs_fixture();
		UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
		owner = CurrentResourceOwner;
		switch (fault) {
		case 0: tx_inputs_generation++; break;
		case 1: tx_inputs_serial++; break;
		case 2: ctrc_inputs_owner.writer.epoch++; break;
		case 3: ctrc_inputs_owner.operation_uuid[0]++; break;
		case 4: ctrc_inputs_owner.install_generation++; break;
		case 5: ctrc_inputs_owner.owner_boot++; break;
		case 6: ctrc_inputs_owner.owner_pid++; break;
		case 7: ctrc_inputs_activate_work(); break; /* same address, new work */
		case 8: tx_inputs_typed.ctrc_work = NULL; break;
		case 9: tx_inputs_typed.census_root.image_sha256[0] ^= 1; break;
		case 10: tx_inputs_owner_current = false; break;
		case 11: inputs_native_recovery = true; break;
		case 12: CurrentResourceOwner = NULL; break;
		case 13: tx_inputs_typed.top_tx_census = NULL; break;
		case 14: inputs_pin_current = false; break;
		case 15: MyBackendType = B_BG_WORKER; break;
		}
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(inputs), 0);
		UT_ASSERT_NULL(cluster_wal_inputs_at_v1(inputs, 0));
		UT_ASSERT_NE(cluster_wal_inputs_revalidate_v1(inputs), 0);
		CurrentResourceOwner = owner;
		cluster_wal_inputs_release_v1(&inputs);
		UT_ASSERT(!inputs_pin_held && test_actual_cf == NoLock);
	}
	test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_inputs_preserves_original_source_selection_and_post_io_check)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterWalInputsV1 *inputs = NULL;

		ctrc_inputs_fixture();
		inputs_claim_observe = true;
		inputs_claim_calls = 0;
		inputs_claim_fault = fault == 0 ? 4 : 0;
		if (fault == 1) tx_inputs_typed.sources[0].claim.claim_sha256[0] ^= 1;
		if (fault == 2) tx_inputs_typed.participants[0].tail_end++;
		if (fault == 3) tx_inputs_typed.participants[0].native_redo++;
		if (fault == 4) inputs_pin_race = true;
		if (fault == 5) inputs_claim_fault = 1;
		if (fault == 6) {
			tx_inputs_typed.participant_count++;
			tx_inputs_typed.replay_count++;
		}
		UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT_NULL(inputs);
		UT_ASSERT(!inputs_pin_held && test_actual_cf == NoLock);
		inputs_claim_observe = false;
		inputs_claim_fault = 0;
	}
	test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_inputs_wait_resume_keeps_original_work_and_whole_root)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterWalInputsV1 *inputs = NULL;

		ctrc_inputs_fixture();
		UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
		UT_ASSERT_EQ(cluster_wal_inputs_suspend_v1(inputs), 0);
		UT_ASSERT(!inputs_pin_held && cluster_wal_inputs_at_v1(inputs, 0) == NULL);
		if (fault == 1) ctrc_inputs_activate_work();
		if (fault == 2) tx_inputs_owner_current = false;
		if (fault == 3) v2_checkpoint_root_race();
		if (fault == 4) ctrc_inputs_owner.install_generation++;
		UT_ASSERT_EQ(cluster_wal_inputs_resume_v1(inputs), fault == 0 ? 0
			: CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT_EQ(inputs_pin_held, fault == 0);
		cluster_wal_inputs_release_v1(&inputs);
		UT_ASSERT(!inputs_pin_held && test_actual_cf == NoLock);
	}
	test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_inputs_open_wait_has_no_partial_census_or_mutation_permission)
{
	ClusterWalInputsV1 *inputs = NULL;
	ControlRootImage root;
	ControlFileData common;
	ClusterControlRootFileToken token;
	ColdCensusTest work = { 0 };
	uint64 records = 99;
	RfPageProofDetailV1 detail;
	XLogRecPtr end = 99;
	char path[MAXPGPATH];

	ctrc_inputs_fixture();
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID,
		&root, &common, &token), 0);
	test_actual_cf = test_cf_mode = NoLock;
	root.records[127].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	v2_write_roots(root.bytes);
	UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_cold_census_v1(inputs, cold_census_record,
		cold_census_source, &work, &records, &detail), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(records, 0);
	UT_ASSERT_EQ(work.begins + work.records + work.ends, 0);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(inputs), 2);
	for (uint32 i = 0; i < 2; i++) {
		UT_ASSERT_EQ(cluster_wal_inputs_prepare_live_v1(inputs, i, &end),
			CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		UT_ASSERT_EQ(end, 0);
	}
	UT_ASSERT(!cluster_wal_inputs_tx_abort_current_v1(inputs, &tx_inputs_candidate.record));
	UT_ASSERT(!cluster_wal_inputs_tx_abort_path_v1(inputs, &tx_inputs_candidate.record,
		path, sizeof(path)));
	UT_ASSERT_EQ(tx_inputs_path_calls, 0);
	UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
	UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), 0);
	cluster_wal_inputs_release_v1(&inputs);
	UT_ASSERT(!inputs_pin_held && test_actual_cf == NoLock);
	test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_inputs_reselects_original_history_after_actual_install)
{
	inputs_reselects_original_history_after_actual_install(true);
}
