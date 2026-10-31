/*-------------------------------------------------------------------------
 * test_cluster_cold_tx_relmap_handoff.inc
 *     Original Start keeps saved relmap replay in the same joint obligation.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_tx_relmap_handoff.inc
 * NOTES
 *     Saved plan fields are inputs; native authority remains controlled.
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
static unsigned relmap_start_destroyed;

static void
relmap_start_fixture(void)
{
	ColdRelmapInput *entry;
	reset();
	typed.system_identifier = 91;
	typed.replay_count = 1;
	typed.participants[0].thread_id = 1;
	typed.participants[0].timeline = 1;
	typed.participants[0].physical_lower = 0x100;
	typed.participants[0].native_redo = 0x400;
	typed.participants[0].tail_end = 0x1000;
	typed.sources[0] = observed_writer.ref;
	relmap_start_plan.participant_count = typed.participant_count;
	relmap_start_plan.relmap_count = 1;
	relmap_start_plan.relmap_inputs = relmap_start_entries;
	entry = &relmap_start_entries[0];
	entry->captured = entry->attempted = entry->applied = true;
	entry->input.cut = typed.participants[0];
	entry->input.source = typed.sources[0];
	entry->input.frame.read_rec_ptr = 0x800;
	entry->input.frame.end_rec_ptr = 0x900;
	entry->input.staged.identity.system_identifier = typed.system_identifier;
	entry->input.staged.pending_generation = 8;
	entry->input.staged.pending_owner.wal_end = 0x900;
	retain();
	relmap_start_destroyed = input_destroys;
}

UT_TEST(test_start_automatically_keeps_replayed_relmap_in_joint_obligation)
{
	relmap_start_fixture();
	UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT(ClusterColdRecoveryRelmapCurrent(info.cluster_tx_start));
	UT_ASSERT(window_region.checkpoint_owner.relmap_required);
	UT_ASSERT(!cluster_recovery_checkpoint_handoff_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(input_destroys, relmap_start_destroyed);
	UT_ASSERT(claim_held);
}

UT_TEST(test_start_refuses_incomplete_or_failed_relmap_receipts)
{
	for (unsigned fault = 0; fault < 7; ++fault) {
		relmap_start_fixture();
		switch (fault) {
		case 0: relmap_start_entries[0].captured = false; break;
		case 1: relmap_start_entries[0].attempted = false; break;
		case 2: relmap_start_entries[0].applied = false; break;
		case 3: relmap_start_plan.relmap_failed = true; break;
		case 4: relmap_start_plan.phase = COLD_PHASE_FAILED; break;
		case 5: relmap_start_plan.relmap_count = CLUSTER_COLD_RELMAP_MAX_INPUTS + 1; break;
		case 6: relmap_start_plan.relmap_inputs = NULL; break;
		}
		UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
		UT_ASSERT_NULL(info.cluster_tx_start);
		UT_ASSERT_NULL(cluster_recovery_cold_start);
		UT_ASSERT_EQ(input_destroys, relmap_start_destroyed);
		UT_ASSERT(claim_held);
	}
}

UT_TEST(test_start_cannot_silently_drop_relmap_source_or_cut)
{
	for (unsigned fault = 0; fault < 5; ++fault) {
		relmap_start_fixture();
		switch (fault) {
		case 0: relmap_start_entries[0].input.source.timeline++; break;
		case 1: relmap_start_entries[0].input.cut.tail_end++; break;
		case 2: relmap_start_entries[0].input.participant = 1; break;
		case 3: typed.system_identifier++; break;
		case 4: typed.refusal = CLUSTER_COLD_SOURCE_GAP; break;
		}
		UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
		UT_ASSERT_NULL(info.cluster_tx_start);
		UT_ASSERT_EQ(input_destroys, relmap_start_destroyed);
		UT_ASSERT(claim_held);
	}
}

UT_TEST(test_start_relmap_receipt_is_complete_by_value_and_bound_to_original_owner)
{
	ClusterColdRelmapReplayReceiptV1 value, copy, empty = {0};
	ResourceOwner original_owner;
	relmap_start_fixture();
	relmap_start_entries[0].input.staged.pending.mappings[0].mapoid = 42;
	UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	value = ClusterColdRecoveryRelmapReplayInput(info.cluster_tx_start, 0);
	UT_ASSERT_EQ(value.count, 1);
	UT_ASSERT_EQ(value.index, 0);
	UT_ASSERT_EQ(memcmp(&value.input, &relmap_start_entries[0].input, sizeof(value.input)), 0);
	copy = value;
	copy.input.staged.pending.mappings[0].mapoid++;
	value = ClusterColdRecoveryRelmapReplayInput(info.cluster_tx_start, 0);
	UT_ASSERT_EQ(value.input.staged.pending.mappings[0].mapoid, 42);
	UT_ASSERT_NE(copy.input.staged.pending.mappings[0].mapoid,
		value.input.staged.pending.mappings[0].mapoid);
	value = ClusterColdRecoveryRelmapReplayInput(info.cluster_tx_start, 1);
	UT_ASSERT_EQ(memcmp(&value, &empty, sizeof(value)), 0);
	UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	value = ClusterColdRecoveryRelmapReplayInput((const struct ClusterRecoveryStartV1 *)(uintptr_t)1, 0);
	UT_ASSERT_EQ(memcmp(&value, &empty, sizeof(value)), 0);
	original_owner = CurrentResourceOwner;
	CurrentResourceOwner = (ResourceOwner)(uintptr_t)42;
	value = ClusterColdRecoveryRelmapReplayInput(info.cluster_tx_start, 0);
	CurrentResourceOwner = original_owner;
	UT_ASSERT_EQ(memcmp(&value, &empty, sizeof(value)), 0);
	UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT_EQ(input_destroys, relmap_start_destroyed);
	UT_ASSERT(claim_held);
}

static unsigned relmap_start_fault;
static void
relmap_start_after_registration(void)
{
	if (checkpoint_registered_start == NULL) return;
	relmap_owner_check_step = NULL;
	if (relmap_start_fault == 0) owner_boot++;
	else if (relmap_start_fault == 1) pg_re_throw();
	else typed.sources[0].timeline++;
}

UT_TEST(test_start_relmap_receipt_drift_or_owner_error_revokes_without_destruction)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		ClusterColdRelmapReplayReceiptV1 value = {0}, empty = {0};
		volatile bool caught = false;
		relmap_start_fixture();
		UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
		if (fault == 0) relmap_start_entries[0].applied = false;
		else if (fault == 1) typed.sources[0].timeline++;
		else { relmap_start_fault = 1; relmap_owner_check_step = relmap_start_after_registration; }
		PG_TRY(); { value = ClusterColdRecoveryRelmapReplayInput(info.cluster_tx_start, 0); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 2);
		UT_ASSERT_EQ(memcmp(&value, &empty, sizeof(value)), 0);
		UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
		UT_ASSERT(!window_region.checkpoint_owner.valid);
		UT_ASSERT_EQ(input_destroys, relmap_start_destroyed);
		UT_ASSERT(claim_held);
	}
}

UT_TEST(test_start_relmap_registration_refusal_or_error_keeps_original_inputs)
{
	for (unsigned fault = 0; fault < 2; ++fault) {
		volatile bool caught = false, begun = false;
		relmap_start_fixture();
		relmap_start_fault = fault;
		relmap_owner_check_step = relmap_start_after_registration;
		PG_TRY(); { begun = ClusterBeginColdRecoveryTx(&info); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 1);
		UT_ASSERT(!begun);
		UT_ASSERT_NULL(info.cluster_tx_start);
		UT_ASSERT_NULL(cluster_recovery_cold_start);
		UT_ASSERT(!window_region.checkpoint_owner.valid);
		UT_ASSERT_EQ(cluster_recovery_cold_inputs, &typed);
		UT_ASSERT_EQ(input_destroys, relmap_start_destroyed);
		UT_ASSERT(claim_held);
		owner_boot = observed_writer.ref.claim.identity.origin_owner_incarnation;
		UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
	}
}

UT_TEST(test_start_relmap_registration_rechecks_saved_batch_after_providers)
{
	relmap_start_fixture();
	relmap_start_fault = 2;
	relmap_owner_check_step = relmap_start_after_registration;
	UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT_NULL(info.cluster_tx_start);
	UT_ASSERT_NULL(cluster_recovery_cold_start);
	UT_ASSERT(!window_region.checkpoint_owner.valid);
	UT_ASSERT_EQ(input_destroys, relmap_start_destroyed);
	UT_ASSERT(claim_held);
}
