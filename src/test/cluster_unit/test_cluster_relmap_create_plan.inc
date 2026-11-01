/*-------------------------------------------------------------------------
 * test_cluster_relmap_create_plan.inc
 *    Independent clean creation declarations in the original cold planner.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_create_plan.inc
 * NOTES
 *    Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */

static void
create_plan_input(EffectRecord *r, ClusterColdDecodedV1 *decoded,
	ClusterColdRelmapCreateInputV1 *in)
{
	creation_record(r);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&r->reader, 99, uuid, true, true,
		decoded), CLUSTER_COLD_OK);
	memset(in, 0, sizeof(*in));
	in->cut = (ClusterColdParticipantV1){1, 0, 3, 31,
		decoded->record.read_rec_ptr, decoded->record.read_rec_ptr, decoded->record.end_rec_ptr};
	in->source.timeline = in->cut.timeline;
	in->source.claim.database_incarnation = 77;
	in->source.claim.max_config_generation = 1;
	in->source.claim.identity.system_identifier = 99;
	in->source.claim.identity.origin_node_id = 2;
	in->source.claim.identity.origin_thread_id = 1;
	in->source.claim.identity.origin_owner_incarnation = 31;
	memcpy(in->source.claim.identity.storage_uuid, uuid, 16);
	memset(in->source.claim.claim_sha256, 7, 32);
	in->frame = (ClusterColdRecordFrameV1){decoded->record.read_rec_ptr,
		decoded->record.end_rec_ptr, decoded->record.record_crc, 0,
		decoded->record.source_xid, decoded->record.source_top_xid, 0, 0,
		RM_RELMAP_ID, XLOG_RELMAP_CREATE_V1, false};
	in->creation = decoded->relmap_create;
}

static bool
create_no_page_observation(void *arg pg_attribute_unused(),
	const RfPageIdentityV1 *page pg_attribute_unused(), ClusterColdDataV1 *out pg_attribute_unused())
{
	abort(); /* Creating a relmap is not a fabricated PAGE observation. */
}

UT_TEST(create_declaration_captures_whole_values_and_original_step)
{
	EffectRecord r;
	ClusterColdDecodedV1 d = {0};
	ClusterColdRelmapCreateInputV1 in, saved;
	ClusterColdPlanV1 *plan = NULL;
	ClusterColdStepV1 step;
	const ClusterColdRelmapCreateInputV1 *captured;
	create_plan_input(&r, &d, &in); saved = in;
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(&in.cut, 1, 2*1024*1024, &plan), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_relmap_create_declare_v1(plan, &in, 1), CLUSTER_COLD_OK);
	UT_ASSERT(cluster_cold_plan_relmap_required_v1(plan, 0));
	UT_ASSERT_EQ(cluster_cold_plan_feed_v1(plan, 0, &d.record), CLUSTER_COLD_OK);
	captured = cluster_cold_plan_relmap_create_captured_v1(plan, 0, &d.record);
	UT_ASSERT_NOT_NULL(captured);
	if (captured) UT_ASSERT_EQ(memcmp(&captured->creation, &saved.creation, sizeof(saved.creation)), 0);
	memset(&in, 0xee, sizeof(in));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, create_no_page_observation, NULL, NULL), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 1);
	UT_ASSERT(cluster_cold_plan_step_v1(plan, 0, &step));
	UT_ASSERT_EQ(step.step_kind, CLUSTER_COLD_STEP_RELMAP);
	UT_ASSERT_EQ(step.info, XLOG_RELMAP_CREATE_V1);
	UT_ASSERT_EQ(step.end_rec_ptr, 0x2000);
	captured = cluster_cold_plan_relmap_create_step_v1(plan, &step);
	UT_ASSERT_NOT_NULL(captured);
	if (captured) UT_ASSERT_EQ(memcmp(&captured->creation, &saved.creation, sizeof(saved.creation)), 0);
	UT_ASSERT_NULL(cluster_cold_plan_relmap_create_captured_v1(plan, 0, &d.record));
	++step.record_crc;
	UT_ASSERT_NULL(cluster_cold_plan_relmap_create_step_v1(plan, &step));
	/* Input retention cannot route or execute a refused structural effect. */
	UT_ASSERT(!cluster_cold_replay_effects_v1(&d, NULL, false));
	cluster_cold_plan_destroy_v1(&plan);
}

UT_TEST(create_steps_keep_original_cross_source_merge_order)
{
	EffectRecord r[2];
	ClusterColdDecodedV1 d[2] = {{0}};
	ClusterColdRelmapCreateInputV1 in[2];
	ClusterColdParticipantV1 cuts[2];
	ClusterColdPlanV1 *plan = NULL;
	ClusterColdStepV1 step;
	for (unsigned i = 0; i < 2; ++i) {
		create_plan_input(&r[i], &d[i], &in[i]);
		in[i].participant = in[i].frame.participant = i;
		in[i].cut.thread_id = in[i].source.claim.identity.origin_thread_id = 2-i;
		in[i].creation.identity.database_oid += i;
		d[i].relmap_create = in[i].creation;
		d[i].record.relmap_dbid += i;
		d[i].record.scn = i == 0 ? 50 : 10;
		cuts[i] = in[i].cut;
	}
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(cuts, 2, 2*1024*1024, &plan), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_relmap_create_declare_v1(plan, in, 2), CLUSTER_COLD_OK);
	for (unsigned i = 0; i < 2; ++i)
		UT_ASSERT_EQ(cluster_cold_plan_feed_v1(plan, i, &d[i].record), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, create_no_page_observation, NULL, NULL), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 2);
	for (unsigned i = 0; i < 2; ++i) {
		const ClusterColdRelmapCreateInputV1 *value;
		UT_ASSERT(cluster_cold_plan_step_v1(plan, i, &step));
		UT_ASSERT_EQ(step.participant, 1-i);
		value = cluster_cold_plan_relmap_create_step_v1(plan, &step);
		UT_ASSERT_NOT_NULL(value);
		if (value) UT_ASSERT_EQ(value->creation.identity.database_oid, 2001-i);
	}
	cluster_cold_plan_destroy_v1(&plan);
}

UT_TEST(create_declaration_validates_whole_batch_before_copy)
{
	for (unsigned fault = 0; fault < 15; ++fault) {
		EffectRecord r;
		ClusterColdDecodedV1 d = {0};
		ClusterColdRelmapCreateInputV1 in[2];
		ClusterColdPlanV1 *plan = NULL;
		create_plan_input(&r, &d, &in[0]); in[1] = in[0];
		in[1].creation.identity.database_oid++;
		in[1].frame.read_rec_ptr = 0x2000; in[1].frame.end_rec_ptr = 0x3000;
		in[0].cut.tail_end = in[1].cut.tail_end = 0x3000;
		UT_ASSERT_EQ(cluster_cold_plan_create_v1(&in[0].cut, 1, 2*1024*1024, &plan), CLUSTER_COLD_OK);
		switch (fault) {
		case 0: in[1].participant = 1; break;
		case 1: ++in[1].cut.owner_incarnation; break;
		case 2: ++in[1].source.timeline; break;
		case 3: ++in[1].source.claim.identity.origin_thread_id; break;
		case 4: ++in[1].source.claim.identity.origin_owner_incarnation; break;
		case 5: ++in[1].source.claim.database_incarnation; break;
		case 6: ++in[1].source.claim.identity.system_identifier; break;
		case 7: in[1].source.claim.identity.storage_uuid[0] ^= 1; break;
		case 8: memset(in[1].source.claim.claim_sha256, 0, 32); break;
		case 9: in[1].frame.history = true; break;
		case 10: in[1].frame.info = XLOG_RELMAP_UPDATE; break;
		case 11: in[1].frame.page_components = 1; break;
		case 12: memset(in[1].creation.batch_uuid, 0, 16); break;
		case 13: ++in[1].creation.committed.mappings[MAX_MAPPINGS-1].mapfilenumber; break;
		case 14: in[1].creation.identity.database_oid = 2000; break;
		}
		UT_ASSERT(cluster_cold_plan_relmap_create_declare_v1(plan, in, 2) != CLUSTER_COLD_OK);
		UT_ASSERT(!cluster_cold_plan_relmap_required_v1(plan, 0));
		UT_ASSERT_EQ(cluster_cold_plan_relmap_create_declare_v1(plan, in, 1), CLUSTER_COLD_OK);
		cluster_cold_plan_destroy_v1(&plan);
	}
}

UT_TEST(create_capture_rejects_changed_frame_and_complete_image)
{
	for (unsigned fault = 0; fault < 10; ++fault) {
		EffectRecord r;
		ClusterColdDecodedV1 d = {0};
		ClusterColdRelmapCreateInputV1 in;
		ClusterColdPlanV1 *plan = NULL;
		create_plan_input(&r, &d, &in);
		UT_ASSERT_EQ(cluster_cold_plan_create_v1(&in.cut, 1, 2*1024*1024, &plan), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(cluster_cold_plan_relmap_create_declare_v1(plan, &in, 1), CLUSTER_COLD_OK);
		switch (fault) {
		case 0: ++d.record.record_crc; break;
		case 1: ++d.record.source_xid; break;
		case 2: ++d.record.source_top_xid; break;
		case 3: --d.record.end_rec_ptr; break;
		case 4: ++d.record.relmap_dbid; break;
		case 5: ++d.relmap_create.identity.registration_generation; break;
		case 6: ++d.relmap_create.committed.mappings[MAX_MAPPINGS-1].mapfilenumber; break;
		case 7: d.record.relmap_image = &d.relmap_create.committed; break;
		case 8: d.record.relmap_create = NULL; break;
		case 9: d.relmap_create.batch_uuid[1] ^= 1; break;
		}
		UT_ASSERT_EQ(cluster_cold_plan_feed_v1(plan, 0, &d.record), CLUSTER_COLD_STRUCTURAL_UNSUPPORTED);
		UT_ASSERT_NULL(cluster_cold_plan_relmap_create_captured_v1(plan, 0, &d.record));
		UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 0);
		cluster_cold_plan_destroy_v1(&plan);
	}
}

UT_TEST(create_plan_refuses_missing_or_repeated_obligations)
{
	for (unsigned missing = 0; missing < 2; ++missing) {
		EffectRecord r;
		ClusterColdDecodedV1 d = {0};
		ClusterColdRelmapCreateInputV1 in[2];
		ClusterColdPlanV1 *plan = NULL;
		create_plan_input(&r, &d, &in[0]); in[1] = in[0];
		if (missing) {
			in[0].cut.tail_end = in[1].cut.tail_end = 0x3000;
			in[1].creation.identity.database_oid++;
			in[1].frame.read_rec_ptr = 0x2000; in[1].frame.end_rec_ptr = 0x3000;
		}
		UT_ASSERT_EQ(cluster_cold_plan_create_v1(&in[0].cut, 1, 2*1024*1024, &plan), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(cluster_cold_plan_relmap_create_declare_v1(plan, in, missing ? 2 : 1), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(cluster_cold_plan_feed_v1(plan, 0, &d.record), CLUSTER_COLD_OK);
		if (missing) {
			d.record.prev_rec_ptr = 0x1000;
			d.record.read_rec_ptr = 0x2000; d.record.end_rec_ptr = 0x3000;
			d.record.rmid = RM_XLOG_ID; d.record.info = XLOG_NOOP;
			d.record.record_flags = 0; d.record.relmap_create = NULL;
			UT_ASSERT_EQ(cluster_cold_plan_feed_v1(plan, 0, &d.record), CLUSTER_COLD_OK);
			UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, create_no_page_observation, NULL, NULL), CLUSTER_COLD_STRUCTURAL_UNSUPPORTED);
		} else UT_ASSERT(cluster_cold_plan_feed_v1(plan, 0, &d.record) != CLUSTER_COLD_OK);
		UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 0);
		cluster_cold_plan_destroy_v1(&plan);
	}
}

UT_TEST(create_declarations_cross_segments_without_a_64_item_limit)
{
	EffectRecord r;
	ClusterColdDecodedV1 d = {0};
	ClusterColdRelmapCreateInputV1 in[65];
	ClusterColdPlanV1 *plan = NULL;
	ClusterColdStepV1 step;
	create_plan_input(&r, &d, &in[0]);
	in[0].cut.tail_end = 0x1000+65*0x1000;
	for (unsigned i = 0; i < 65; ++i) {
		if (i) in[i] = in[0];
		in[i].creation.identity.database_oid = 2000+i;
		in[i].frame.read_rec_ptr = 0x1000+i*0x1000;
		in[i].frame.end_rec_ptr = in[i].frame.read_rec_ptr+0x1000;
	}
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(&in[0].cut, 1, 8*1024*1024, &plan), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_relmap_create_declare_v1(plan, in, 65), CLUSTER_COLD_OK);
	for (unsigned i = 0; i < 65; ++i) {
		d.relmap_create = in[i].creation;
		d.record.relmap_dbid = 2000+i;
		d.record.prev_rec_ptr = i ? in[i-1].frame.read_rec_ptr : 0;
		d.record.read_rec_ptr = in[i].frame.read_rec_ptr; d.record.end_rec_ptr = in[i].frame.end_rec_ptr;
		UT_ASSERT_EQ(cluster_cold_plan_feed_v1(plan, 0, &d.record), CLUSTER_COLD_OK);
	}
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, create_no_page_observation, NULL, NULL), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 65);
	UT_ASSERT(cluster_cold_plan_step_v1(plan, 64, &step));
	{
		const ClusterColdRelmapCreateInputV1 *value = cluster_cold_plan_relmap_create_step_v1(plan, &step);
		UT_ASSERT_NOT_NULL(value);
		if (value) UT_ASSERT_EQ(value->creation.identity.database_oid, 2064);
	}
	cluster_cold_plan_destroy_v1(&plan);
}

UT_TEST(create_budget_and_sealed_state_refuse_without_publication)
{
	EffectRecord r;
	ClusterColdDecodedV1 d = {0};
	ClusterColdRelmapCreateInputV1 in;
	ClusterColdPlanV1 *plan = NULL;
	ClusterColdStepV1 step = {0};
	create_plan_input(&r, &d, &in);
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(&in.cut, 1, 4096, &plan), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_relmap_create_declare_v1(plan, &in, 1), CLUSTER_COLD_CAPACITY);
	UT_ASSERT(!cluster_cold_plan_relmap_required_v1(plan, 0));
	UT_ASSERT_NULL(cluster_cold_plan_relmap_create_step_v1(plan, &step));
	cluster_cold_plan_destroy_v1(&plan);
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(&in.cut, 1, 2*1024*1024, &plan), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_relmap_create_declare_v1(plan, &in, 1), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_relmap_create_declare_v1(plan, &in, 1), CLUSTER_COLD_STATE);
	UT_ASSERT_EQ(cluster_cold_plan_feed_v1(plan, 0, &d.record), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, create_no_page_observation, NULL, NULL), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_relmap_create_declare_v1(plan, &in, 1), CLUSTER_COLD_STATE);
	cluster_cold_plan_relmap_invalidate_v1(plan);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 0);
	UT_ASSERT_NULL(cluster_cold_plan_relmap_create_step_v1(plan, &step));
	cluster_cold_plan_destroy_v1(&plan);
}
