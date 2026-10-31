/*-------------------------------------------------------------------------
 * test_cluster_cold_startup_relmap.inc
 *     Declared relmap SIDE custody in the original Startup orchestrator.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_startup_relmap.inc
 * NOTES
 *     ROOT, scanning and SIDE storage are the existing boundary fixtures.
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
static int startup_relmap_pin, startup_relmap_lock;

static void
startup_relmap_fixture(ClusterColdRelmapInputV1 *input)
{
	fixture();
	native_side_complete_claims = true;
	memset(input, 0, sizeof(*input));
	input->participant = 0;
	input->cut = (ClusterColdParticipantV1){1, 0, 1, 11, 0x100, 0x800, 0x2000};
	input->source.claim.identity = roots[1].identity;
	input->source.claim.database_incarnation = 5;
	input->source.claim.max_config_generation = 1;
	memset(input->source.claim.claim_sha256, 1, 32);
	input->source.timeline = 1;
	input->frame = (ClusterColdRecordFrameV1){0x800, 0x2000, 0, 0, 0x123, 0,
		0, 0, RM_RELMAP_ID, XLOG_RELMAP_UPDATE, false};
	input->staged.identity = (ClusterRelmapV2Namespace){9, 5, InvalidOid, true};
	input->staged.committed_generation = 7;
	input->staged.pending_generation = 8;
	input->staged.pending_owner = (ClusterRelmapV2Owner){0, UINT64CONST(0x100000123),
		0, 11, 0x2000, 41};
	input->staged.committed.magic = input->staged.pending.magic = RELMAPPER_FILEMAGIC;
	INIT_CRC32C(input->staged.pending.crc);
	COMP_CRC32C(input->staged.pending.crc, &input->staged.pending, offsetof(RelMapFile, crc));
	FIN_CRC32C(input->staged.pending.crc);
	input->staged.committed = input->staged.pending;
	input->retention = (struct ClusterRelmapRetentionDirectory *)&startup_relmap_pin;
	input->retention_lock = (struct LWLock *)&startup_relmap_lock;
	scan_relmap_input = input;
}

static ClusterColdTypedV1 *
startup_relmap_prepare(const ClusterColdRelmapInputV1 *input)
{
	return cluster_cold_typed_prepare_relmap_v1((ClusterRecoveryFencePlan *)&fixture_context,
		1, 0x800, input, 1);
}

UT_TEST(test_startup_declared_relmap_binds_database_before_capture)
{
	ClusterColdRelmapInputV1 input;
	ClusterColdTypedV1 *typed;
	startup_relmap_fixture(&input);
	scan_relmap_require_early_binding = true;
	typed = startup_relmap_prepare(&input);
	UT_ASSERT_NOT_NULL(typed);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT(typed->side_plans[0] != NULL && side_fixture[0].sealed);
	UT_ASSERT(typed->plan->relmap_count == 1 && typed->plan->relmap_inputs[0].captured);
	UT_ASSERT(!typed->plan->relmap_inputs[0].applied);
	UT_ASSERT_EQ(typed->scanned_records, 2 * NROOTS);
	cluster_cold_typed_destroy_v1(&typed);
	UT_ASSERT_EQ(contexts_alive, 0);
}

UT_TEST(test_startup_declared_relmap_cannot_drop_scan_or_seal_failure)
{
	const unsigned faults[] = {6, 7, 16};
	for (unsigned f = 0; f < lengthof(faults); ++f) {
		ClusterColdRelmapInputV1 input;
		ClusterColdTypedV1 *typed;
		startup_relmap_fixture(&input);
		side_fault = faults[f];
		typed = startup_relmap_prepare(&input);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SIDE_OWNER_MISSING);
		UT_ASSERT(typed->plan->relmap_failed && typed->plan->phase == COLD_PHASE_FAILED);
		UT_ASSERT(typed->side_plans[0] == NULL);
		cluster_cold_typed_destroy_v1(&typed);
		UT_ASSERT_EQ(contexts_alive, 0);
	}
}

UT_TEST(test_startup_relmap_early_binding_refuses_before_scan)
{
	ClusterColdRelmapInputV1 input;
	ClusterColdTypedV1 *typed;
	startup_relmap_fixture(&input);
	scan_relmap_require_early_binding = true;
	side_fault = 17;
	typed = startup_relmap_prepare(&input);
	UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
	UT_ASSERT_EQ(typed->scanned_records, 0);
	UT_ASSERT_EQ(typed->plan->input_frame_count, 0);
	UT_ASSERT(typed->side_plans[0] == NULL);
	cluster_cold_typed_destroy_v1(&typed);
	UT_ASSERT_EQ(contexts_alive, 0);
}
