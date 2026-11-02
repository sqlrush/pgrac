/*-------------------------------------------------------------------------
 * test_cluster_cold_relmap_pending.inc
 *     Pending-only conjunction of original mixed relmap obligations.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_relmap_pending.inc
 * NOTES
 *     Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */

/* Real declarations/feed/seal, with a distinct creation namespace. Creation
 * stays unconsumed: successful pending checks must not complete that family. */
static void
cold_pending_mixed(bool pending)
{
	ClusterColdRelmapCreateInputV1 create = {0};
	ClusterColdRecordV1 record = {0};
	uint32 p = pending ? 1 : 0;
	cold_map_setup();
	if (!pending) {
		cluster_cold_plan_destroy_v1(&replay.plan);
		replay.participant_count = replay.replay_count = 1;
		UT_ASSERT_EQ(cluster_cold_plan_create_v1(replay.participants, 1, 4194304,
			&replay.plan), CLUSTER_COLD_OK);
	}
	create.participant = p;
	create.cut = replay.participants[p];
	create.source = replay.sources[p];
	create.frame.read_rec_ptr = create.cut.physical_lower;
	create.frame.end_rec_ptr = create.cut.tail_end;
	create.frame.record_crc = 88;
	create.frame.participant = p;
	create.frame.rmid = RM_RELMAP_ID;
	create.frame.info = XLOG_RELMAP_CREATE_V1;
	create.creation.identity.kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
	create.creation.identity.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	create.creation.identity.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	create.creation.identity.system_identifier = identity.system_identifier;
	create.creation.identity.database_incarnation = identity.database_incarnation;
	memcpy(create.creation.identity.storage_uuid, create.source.claim.identity.storage_uuid, 16);
	create.creation.identity.redo_branch_uuid[0] = 9;
	create.creation.identity.redo_branch_generation = 1;
	create.creation.identity.redo_timeline = 1;
	create.creation.identity.database_oid = 2000;
	create.creation.identity.tablespace_oid = DEFAULTTABLESPACE_OID;
	create.creation.identity.origin = create.source.claim.identity.origin_node_id;
	create.creation.identity.creation_generation = create.cut.owner_incarnation;
	create.creation.identity.registration_generation = 1;
	create.creation.batch_uuid[0] = 7;
	create.creation.committed = declared.staged.pending;
	record.read_rec_ptr = create.frame.read_rec_ptr;
	record.end_rec_ptr = create.frame.end_rec_ptr;
	record.record_crc = 88;
	record.scn = 30;
	record.rmid = RM_RELMAP_ID;
	record.info = XLOG_RELMAP_CREATE_V1;
	record.record_flags = CLUSTER_COLD_RECORD_STRUCTURAL;
	record.relmap_create = &create.creation;
	record.relmap_dbid = 2000;
	record.relmap_tsid = DEFAULTTABLESPACE_OID;
	UT_ASSERT_EQ(cluster_cold_plan_relmap_create_declare_v1(replay.plan, &create, 1), CLUSTER_COLD_OK);
	if (pending) {
		UT_ASSERT_EQ(cluster_cold_plan_relmap_declare_v1(replay.plan, &declared, 1), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(cluster_cold_plan_feed_v1(replay.plan, 0, &records[0][0]), CLUSTER_COLD_OK);
		replay.side_plans[0] = cold_map_side(&declared, true);
	}
	UT_ASSERT_EQ(cluster_cold_plan_feed_v1(replay.plan, p, &record), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(replay.plan, cold_map_observe, NULL, NULL), CLUSTER_COLD_OK);
}

static void
cold_pending_apply(void)
{
	ClusterColdStepV1 step;
	UT_ASSERT(cluster_cold_plan_step_v1(replay.plan, 0, &step));
	UT_ASSERT_EQ(step.step_kind, CLUSTER_COLD_STEP_RELMAP);
	UT_ASSERT(cluster_cold_typed_relmap_record_v1(&replay,
		(ClusterRecoveryFencePlan *)&replay, &step, &map_reader));
}

UT_TEST(test_pending_conjunct_does_not_complete_unconsumed_creation)
{
	unsigned after;
	const ColdRelmapCreateInput *create;
	cold_pending_mixed(true);
	UT_ASSERT(!cluster_cold_typed_relmap_pending_ready_v1(&replay, (ClusterRecoveryFencePlan *)&replay));
	UT_ASSERT_EQ(cluster_cold_typed_relmap_pending_count_v1(&replay), UINT32_MAX);
	UT_ASSERT(!replay.plan->relmap_failed);
	cold_pending_apply();
	after = writes;
	UT_ASSERT(cluster_cold_typed_relmap_pending_ready_v1(&replay, (ClusterRecoveryFencePlan *)&replay));
	UT_ASSERT_EQ(cluster_cold_typed_relmap_pending_count_v1(&replay), 1);
	UT_ASSERT(!cluster_cold_typed_relmap_ready_v1(&replay, (ClusterRecoveryFencePlan *)&replay));
	UT_ASSERT_EQ(cluster_cold_typed_relmap_replay_count_v1(&replay), UINT32_MAX);
	UT_ASSERT_EQ(cluster_cold_typed_relmap_replay_input_v1(&replay, 0).count, 0);
	create = cold_relmap_create_at(replay.plan, 0);
	UT_ASSERT(create && create->captured && !create->attempted && !create->applied && !create->receipt);
	UT_ASSERT_EQ(writes, after);
	UT_ASSERT(writer_retention.pins[0].occupied && writer_retention.pins[0].may_be_staged);
	cold_map_end();
}

UT_TEST(test_creation_only_has_empty_pending_component_and_old_gates_refuse)
{
	cold_pending_mixed(false);
	UT_ASSERT(cluster_cold_typed_relmap_pending_ready_v1(&replay, (ClusterRecoveryFencePlan *)&replay));
	UT_ASSERT_EQ(cluster_cold_typed_relmap_pending_count_v1(&replay), 0);
	UT_ASSERT(!cluster_cold_typed_relmap_ready_v1(&replay, (ClusterRecoveryFencePlan *)&replay));
	UT_ASSERT_EQ(cluster_cold_typed_relmap_replay_count_v1(&replay), UINT32_MAX);
	UT_ASSERT(!cold_relmap_create_at(replay.plan, 0)->attempted);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(source_checks, 0);
	UT_ASSERT(!cluster_cold_typed_relmap_pending_ready_v1(NULL, (ClusterRecoveryFencePlan *)&replay));
	UT_ASSERT(!cluster_cold_typed_relmap_pending_ready_v1(&replay, NULL));
	UT_ASSERT_EQ(cluster_cold_typed_relmap_pending_count_v1(NULL), UINT32_MAX);
	replay.plan->phase = COLD_PHASE_FEEDING;
	UT_ASSERT(!cluster_cold_typed_relmap_pending_ready_v1(&replay, (ClusterRecoveryFencePlan *)&replay));
	UT_ASSERT_EQ(cluster_cold_typed_relmap_pending_count_v1(&replay), UINT32_MAX);
	cold_map_end();
}

UT_TEST(test_pending_count_requires_complete_original_receipts_and_metadata)
{
	for (unsigned fault = 0; fault < 7; ++fault) {
		unsigned after;
		cold_pending_mixed(true);
		cold_pending_apply();
		after = source_checks;
		if (fault == 0) replay.plan->relmap_inputs[0].captured = false;
		if (fault == 1) replay.plan->relmap_inputs[0].attempted = false;
		if (fault == 2) replay.plan->relmap_inputs[0].applied = false;
		if (fault == 3) ++replay.participants[0].owner_incarnation;
		if (fault == 4) ++replay.sources[0].claim.max_config_generation;
		if (fault == 5) ++replay.system_identifier;
		if (fault == 6) replay.plan->relmap_count = CLUSTER_COLD_RELMAP_MAX_INPUTS + 1;
		UT_ASSERT_EQ(cluster_cold_typed_relmap_pending_count_v1(&replay), UINT32_MAX);
		UT_ASSERT_EQ(source_checks, after);
		if (fault == 6) replay.plan->relmap_count = 1;
		UT_ASSERT(writer_retention.pins[0].occupied && writer_retention.pins[0].may_be_staged);
		cold_map_end();
	}
}

UT_TEST(test_pending_readiness_rejects_late_source_side_pin_or_pair_change)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		unsigned after;
		unsigned char saved[CLUSTER_RELMAP_V2_FILE_SIZE], changed[CLUSTER_RELMAP_V2_FILE_SIZE];
		cold_pending_mixed(true);
		cold_pending_apply();
		after = writes;
		if (fault == 0) original_source_current = false;
		if (fault == 1) { detached_side = replay.side_plans[0]; replay.side_plans[0] = NULL; }
		if (fault == 2) writer_retention.pins[0].may_be_staged = false;
		if (fault == 3) {
			read_copy(1, saved);
			memcpy(changed, saved, sizeof(changed));
			changed[CLUSTER_RELMAP_V2_PENDING_OFFSET + 500] ^= 1;
			write_copy(1, changed, sizeof(changed));
		}
		UT_ASSERT(!cluster_cold_typed_relmap_pending_ready_v1(&replay, (ClusterRecoveryFencePlan *)&replay));
		original_source_current = true;
		if (fault == 1) { replay.side_plans[0] = detached_side; detached_side = NULL; }
		writer_retention.pins[0].may_be_staged = true;
		if (fault == 3) write_copy(1, saved, sizeof(saved));
		UT_ASSERT(!cluster_cold_typed_relmap_pending_ready_v1(&replay, (ClusterRecoveryFencePlan *)&replay));
		UT_ASSERT_EQ(cluster_cold_typed_relmap_pending_count_v1(&replay), UINT32_MAX);
		UT_ASSERT_EQ(writes, after);
		UT_ASSERT(writer_retention.pins[0].occupied && writer_retention.pins[0].may_be_staged);
		cold_map_end();
	}
}

UT_TEST(test_pending_readiness_owner_errors_are_sticky_without_rewrite)
{
	for (unsigned fault = 1; fault <= 2; ++fault) {
		volatile bool caught = false;
		unsigned after;
		cold_pending_mixed(true);
		cold_pending_apply();
		after = writes;
		source_error_at = source_checks + fault;
		PG_TRY(); {
			(void)cluster_cold_typed_relmap_pending_ready_v1(&replay, (ClusterRecoveryFencePlan *)&replay);
		}
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT(caught);
		source_error_at = 0;
		UT_ASSERT(!cluster_cold_typed_relmap_pending_ready_v1(&replay, (ClusterRecoveryFencePlan *)&replay));
		UT_ASSERT_EQ(cluster_cold_typed_relmap_pending_count_v1(&replay), UINT32_MAX);
		UT_ASSERT_EQ(writes, after);
		UT_ASSERT(writer_retention.pins[0].occupied && writer_retention.pins[0].may_be_staged);
		UT_ASSERT_EQ(retention_acquires, retention_releases);
		UT_ASSERT_EQ(InterruptHoldoffCount, 0);
		cold_map_end();
	}
}

UT_TEST(test_pending_only_plan_keeps_original_completion_behavior)
{
	cold_map_setup();
	UT_ASSERT_EQ(cold_map_seal(true, true), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cold_map_run(), CLUSTER_COLD_REPLAY_OK);
	UT_ASSERT_EQ(cluster_cold_typed_relmap_pending_count_v1(&replay), 1);
	UT_ASSERT_EQ(cluster_cold_typed_relmap_replay_count_v1(&replay), 1);
	UT_ASSERT(cluster_cold_typed_relmap_pending_ready_v1(&replay, (ClusterRecoveryFencePlan *)&replay));
	UT_ASSERT(cluster_cold_typed_relmap_ready_v1(&replay, (ClusterRecoveryFencePlan *)&replay));
	cold_map_end();
}
