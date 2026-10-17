/* C0 uses the real SIDE planner and codec. Original IO/authority consumers
 * remain the existing controlled fixture; none grants C0 execution. */
int side_xact_fixture_main(void);
#define main side_xact_fixture_main
#include "test_cluster_side_xact.c"
#undef main
#include "cluster/cluster_side_route.h"

typedef struct C0Fixture {
	ClusterRecoveryTTAbortV1 c0;
	ClusterWalSourceRef sources[2];
	RfContributorStreamCutV1 cuts[2];
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;
	FakeXactRecord fake;
} C0Fixture;

static void
c0_encode(C0Fixture *f)
{
	UT_ASSERT(cluster_recovery_tt_abort_encode_v1(&f->c0, f->fake.data, 152));
}

static void
c0_fixture(C0Fixture *f)
{
	uint8 uuid[16];
	memset(f, 0, sizeof(*f));
	memset(uuid, 0x44, sizeof(uuid));
	f->fake.u.decoded.header.xl_rmid = RM_CLUSTER_UNDO_ID;
	f->fake.u.decoded.header.xl_info = XLOG_UNDO_RECOVERY_TT_ABORT;
	f->fake.u.decoded.main_data = (char *)f->fake.data;
	f->fake.u.decoded.main_data_len = 152;
	f->fake.u.decoded.max_block_id = -1;
	f->fake.reader.record = &f->fake.u.decoded;
	f->identity = make_identity(&f->fake, uuid);
	f->identity.participant_index = 1;
	for (uint32 i = 0; i < 2; i++) {
		ClusterWalThreadClaimRefV2 *ref = &f->sources[i].claim;
		ref->identity.system_identifier = f->identity.record.system_identifier;
		memcpy(ref->identity.storage_uuid, uuid, 16);
		memset(ref->identity.authority_uuid, 0x55, 16);
		ref->identity.origin_thread_id = i + 2;
		ref->identity.origin_node_id = i + 1;
		ref->identity.origin_owner_incarnation = 22 + 11 * i;
		ref->identity.root_lineage_seq = 1;
		ref->identity.thread_claim_created_at = 42;
		ref->database_incarnation = 7;
		ref->max_config_generation = 9;
		memset(ref->claim_sha256, 0x20 + i, 32);
		f->sources[i].timeline = 7;
		f->cuts[i].failed_thread = i + 2;
		f->cuts[i].origin_owner_incarnation = ref->identity.origin_owner_incarnation;
		f->cuts[i].timeline_id = 7;
		f->cuts[i].scan_begin_inclusive = 100;
		f->cuts[i].scan_end_exclusive = i == 0 ? 100 : 200;
		f->cuts[i].flags
			= RF_CONTRIBUTOR_CUT_COMPLETE | (i == 0 ? RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY : 0);
	}
	f->c0.version = 1;
	f->c0.length = 152;
	f->c0.flags = CLUSTER_RECOVERY_TT_ABORT_TOP_KNOWN;
	f->c0.target_thread = 2;
	f->c0.writer_thread = 3;
	f->c0.target_incarnation = 22;
	f->c0.writer_incarnation = 33;
	f->c0.recovery_serial = 19;
	f->c0.target_epoch = 4;
	f->c0.target_full_xid = f->c0.top_full_xid = UINT64_C(0x700400102);
	memcpy(f->c0.target_claim_sha256, f->sources[0].claim.claim_sha256, 32);
	f->c0.exact = (xl_undo_tt_slot_abort_exact){ .segment_id = 257,
												 .segment_generation = 0,
												 .xid = 0x400102,
												 .slot_offset = 5,
												 .wrap = 9,
												 .instance = 2,
												 .format_version = 1 };
	f->c0.before.xid = f->c0.exact.xid;
	f->c0.before.wrap = 9;
	f->c0.before.status = TT_SLOT_ACTIVE;
	f->c0.before.first_undo_block.raw[0] = UINT64_C(0x300000102);
	f->c0.before.first_undo_block.raw[1] = UINT64_C(0x1000005);
	c0_encode(f);
	f->record.source_record = &f->fake.reader;
	/* The same registered route is independently tested below. This fixed
	 * input exposes decoder failures independently from a missing row. */
	f->record.route.rmid = RM_CLUSTER_UNDO_ID;
	f->record.route.normalized_info = XLOG_UNDO_RECOVERY_TT_ABORT;
	f->record.route.record_owner = RF_ROUTE_OWNER_SIDE_TYPED;
	f->record.route.block_policy = RF_ROUTE_BLOCKS_FORBIDDEN;
	f->record.route.codec_id = RF_ROUTE_CODEC_SIDE_CLUSTER_UNDO;
	f->record.preflight_complete = true;
}

static RfSideOnlinePlanV1 *
c0_plan_redo(C0Fixture *f, RfSidePlanPurposeV1 purpose, Size budget, const XLogRecPtr *redo)
{
	RfSideOnlinePlanRequestV1 request = { 0 };
	RfSideOnlinePlanV1 *plan = NULL;
	request.system_identifier = f->identity.record.system_identifier;
	memcpy(request.storage_uuid, f->identity.record.storage_uuid, 16);
	request.physical_cuts = f->cuts;
	request.participant_count = 2;
	request.purpose = purpose;
	request.memory_budget = budget;
	request.redo_starts = redo;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

static RfSideOnlinePlanV1 *
c0_plan(C0Fixture *f, RfSidePlanPurposeV1 purpose, Size budget)
{
	return c0_plan_redo(f, purpose, budget, NULL);
}

static bool
c0_bind(RfSideOnlinePlanV1 *plan, C0Fixture *f)
{
	bool bound = rf_side_online_plan_bind_sources_v1(plan, f->sources, 2);
	UT_ASSERT(bound);
	return bound;
}

UT_TEST(c0_keeps_writer_and_target_full_values)
{
	C0Fixture f;
	RfSideOnlinePlanV1 *plan;
	RfSideOnlineOperationV1 op = { 0 };
	ClusterWalSourceRef writer = { 0 }, target = { 0 };
	c0_fixture(&f);
	plan = c0_plan(&f, RF_SIDE_PLAN_RECOVERY, 0);
	if (c0_bind(plan, &f)) {
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &f.record, &f.identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		if (rf_side_online_plan_operation_v1(plan, 0, &op)) {
			UT_ASSERT_EQ(op.kind, RF_SIDE_ONLINE_OPERATION_RECOVERY_TT_ABORT);
			UT_ASSERT_EQ(op.identity.record.origin_thread, 3);
			UT_ASSERT_EQ(op.recovery_target_participant, 0);
			UT_ASSERT_EQ(op.recovery_abort.target_thread, 2);
			UT_ASSERT_EQ(op.owned_payload_length, 152);
			UT_ASSERT(memcmp(op.owned_payload, f.fake.data, 152) == 0);
		} else
			UT_ASSERT(false);
		UT_ASSERT(rf_side_online_plan_recovery_abort_sources_v1(plan, 0, &writer, &target));
		UT_ASSERT(memcmp(&writer, &f.sources[1], sizeof(writer)) == 0);
		UT_ASSERT(memcmp(&target, &f.sources[0], sizeof(target)) == 0);
		memset(&f.sources, 0, sizeof(f.sources));
		memset(f.fake.data, 0xff, 152);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &op));
		if (op.owned_payload != NULL)
			UT_ASSERT_EQ(op.owned_payload[0], 1);
		else
			UT_ASSERT(false);
		UT_ASSERT(rf_side_online_plan_recovery_abort_sources_v1(plan, 0, &writer, &target));
		UT_ASSERT_EQ(writer.claim.identity.origin_thread_id, 3);
		UT_ASSERT_EQ(target.claim.identity.origin_owner_incarnation, 22);
	}
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(c0_missing_or_changed_claim_never_becomes_ordinary_undo)
{
	for (int v = 0; v < 7; v++) {
		C0Fixture f;
		RfSideOnlinePlanV1 *plan;
		c0_fixture(&f);
		switch (v) {
		case 1:
			f.c0.target_claim_sha256[31] ^= 1;
			break;
		case 2:
			f.c0.target_incarnation++;
			break;
		case 3:
			f.c0.writer_incarnation++;
			break;
		case 4:
			f.c0.writer_thread = 1;
			break;
		case 5:
			f.sources[0].claim.identity.authority_uuid[15] ^= 1;
			break;
		case 6:
			f.c0.target_thread = 4;
			f.c0.exact.instance = 4;
			f.c0.exact.segment_id = 769;
			memset(&f.c0.before.first_undo_block, 0, sizeof(f.c0.before.first_undo_block));
			break;
		}
		c0_encode(&f);
		plan = c0_plan(&f, RF_SIDE_PLAN_RECOVERY, 0);
		if (v == 0 || c0_bind(plan, &f))
			UT_ASSERT_NE(rf_side_online_plan_feed_record_v1(plan, &f.record, &f.identity),
						 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_NE(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(c0_malformed_payload_and_header_refuse_before_capture)
{
	for (int v = 0; v < 8; v++) {
		C0Fixture f;
		RfSideOnlinePlanV1 *plan;
		c0_fixture(&f);
		switch (v) {
		case 0:
			f.fake.data[0] = 2;
			break;
		case 1:
			f.fake.data[4] = 3;
			break;
		case 2:
			f.fake.u.decoded.main_data_len--;
			break;
		case 3:
			f.fake.u.decoded.main_data_len++;
			break;
		case 4:
			f.fake.u.decoded.header.xl_xid = 4;
			break;
		case 5:
			f.fake.u.decoded.toplevel_xid = 5;
			break;
		case 6:
			f.fake.u.decoded.record_origin = 1;
			break;
		case 7:
			f.fake.u.decoded.max_block_id = 0;
			f.fake.u.decoded.blocks[0].in_use = true;
			break;
		}
		plan = c0_plan(&f, RF_SIDE_PLAN_RECOVERY, 0);
		if (c0_bind(plan, &f))
			UT_ASSERT_NE(rf_side_online_plan_feed_record_v1(plan, &f.record, &f.identity),
						 RF_PAGE_PROOF_DETAIL_OK);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(c0_full_source_bind_is_bounded_and_sticky)
{
	for (int v = 0; v < 11; v++) {
		C0Fixture f;
		RfSideOnlinePlanV1 *plan;
		c0_fixture(&f);
		plan = c0_plan(&f, RF_SIDE_PLAN_RECOVERY, 0);
		switch (v) {
		case 0:
			f.sources[0].claim.identity.reserved42 = 1;
			break;
		case 1:
			f.sources[0].claim.identity.system_identifier++;
			break;
		case 2:
			f.sources[0].claim.identity.storage_uuid[15] ^= 1;
			break;
		case 3:
			f.sources[0].claim.database_incarnation++;
			break;
		case 4:
			f.sources[0].claim.max_config_generation = 0;
			break;
		case 5:
			f.sources[0].claim.identity.origin_owner_incarnation++;
			break;
		case 6:
			f.sources[0].timeline++;
			break;
		case 7:
			memset(f.sources[0].claim.claim_sha256, 0, 32);
			break;
		case 8:
			f.sources[0].claim.identity.origin_node_id++;
			break;
		case 9:
			f.sources[0].claim.identity.root_lineage_seq = 0;
			break;
		case 10:
			f.sources[0].claim.identity.thread_claim_created_at = 0;
			break;
		}
		UT_ASSERT(!rf_side_online_plan_bind_sources_v1(plan, f.sources, 2));
		c0_fixture(&f);
		UT_ASSERT(!rf_side_online_plan_bind_sources_v1(plan, f.sources, 2));
		UT_ASSERT_NE(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		rf_side_online_plan_destroy_v1(&plan);
	}
	/* The original plan permits this tiny budget, but cannot copy two full claims. */
	{
		C0Fixture f;
		RfSideOnlinePlanV1 *plan;
		c0_fixture(&f);
		plan = c0_plan(&f, RF_SIDE_PLAN_RECOVERY, 512);
		UT_ASSERT(!rf_side_online_plan_bind_sources_v1(plan, f.sources, 2));
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(c0_bind_cannot_change_after_feed_or_seal)
{
	C0Fixture f;
	RfSideOnlinePlanV1 *plan;
	c0_fixture(&f);
	plan = c0_plan(&f, RF_SIDE_PLAN_RECOVERY, 0);
	if (c0_bind(plan, &f)) {
		UT_ASSERT(!rf_side_online_plan_bind_sources_v1(plan, f.sources, 2));
		UT_ASSERT_NE(rf_side_online_plan_feed_record_v1(plan, &f.record, &f.identity),
					 RF_PAGE_PROOF_DETAIL_OK);
	}
	rf_side_online_plan_destroy_v1(&plan);
	plan = c0_plan(&f, RF_SIDE_PLAN_RECOVERY, 0);
	if (c0_bind(plan, &f)) {
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &f.record, &f.identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(!rf_side_online_plan_bind_sources_v1(plan, f.sources, 2));
		UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 0);
		{
			RfSideOnlineOperationV1 op = { 0 };
			ClusterWalSourceRef writer = { 0 }, target = { 0 };
			UT_ASSERT(!rf_side_online_plan_operation_v1(plan, 0, &op));
			UT_ASSERT(!rf_side_online_plan_recovery_abort_sources_v1(plan, 0, &writer, &target));
		}
	}
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(c0_retained_plan_is_not_execution_or_pi_proof)
{
	C0Fixture f;
	RfSideOnlinePlanV1 *plan;
	RfSideOnlineApplyOpsV1 ops = { 0 };
	ApplyCapture capture = { 0 };
	RfSideContributionOwnersV1 owners;
	c0_fixture(&f);
	ops.arg = &capture;
	ops.begin_protected_set = capture_begin;
	ops.end_protected_set = capture_end;
	ops.preflight_undo = ops.preflight_projection = accept_preflight;
	ops.apply_undo = capture_apply_undo;
	ops.apply_projection = capture_apply_projection;
	plan = c0_plan(&f, RF_SIDE_PLAN_RECOVERY, 0);
	if (c0_bind(plan, &f)) {
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &f.record, &f.identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_NE(rf_side_online_plan_preflight_v1(plan, &ops), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_NE(rf_side_online_plan_apply_v1(plan, &ops), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(!rf_side_online_plan_contribution_owners_v1(plan, 0, &owners));
		UT_ASSERT_EQ(capture.begin_count, 0);
		UT_ASSERT_EQ(capture.undo_count + capture.projection_count, 0);
	}
	rf_side_online_plan_destroy_v1(&plan);
	plan = c0_plan(&f, RF_SIDE_PLAN_PI_CONTRIBUTIONS, 0);
	if (c0_bind(plan, &f))
		UT_ASSERT_NE(rf_side_online_plan_feed_record_v1(plan, &f.record, &f.identity),
					 RF_PAGE_PROOF_DETAIL_OK);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(c0_registered_route_stays_blocked_for_ordinary_undo)
{
	RfOpcodeRouteV1 route = { 0 };
	ClusterSideRouteRow row = { 0 };
	ClusterUndoDecoded decoded;
	C0Fixture f;
	c0_fixture(&f);
	UT_ASSERT_EQ(rf_opcode_route_lookup_v1(RM_CLUSTER_UNDO_ID, XLOG_UNDO_RECOVERY_TT_ABORT, false,
										   true, &route),
				 RF_OPCODE_ROUTE_OK);
	UT_ASSERT_EQ(route.record_owner, RF_ROUTE_OWNER_SIDE_TYPED);
	UT_ASSERT_EQ(route.block_policy, RF_ROUTE_BLOCKS_FORBIDDEN);
	UT_ASSERT(cluster_side_route_lookup(RM_CLUSTER_UNDO_ID, XLOG_UNDO_RECOVERY_TT_ABORT, &row));
	UT_ASSERT_EQ(cluster_side_route_verdict(&row), CLUSTER_SIDE_ROUTE_VERDICT_BLOCKED);
	UT_ASSERT(!cluster_undo_decode(&f.fake.reader, &decoded));
}

UT_TEST(c0_failed_record_cannot_be_skipped_or_repaired_in_place)
{
	for (int bound = 0; bound < 2; bound++) {
		C0Fixture f;
		RfSideOnlinePlanV1 *plan;
		c0_fixture(&f);
		plan = c0_plan(&f, RF_SIDE_PLAN_RECOVERY, 0);
		if (bound) {
			UT_ASSERT(c0_bind(plan, &f));
			f.fake.data[0] = 2;
		}
		UT_ASSERT_NE(rf_side_online_plan_feed_record_v1(plan, &f.record, &f.identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		c0_encode(&f);
		UT_ASSERT(!rf_side_online_plan_bind_sources_v1(plan, f.sources, 2));
		UT_ASSERT_NE(rf_side_online_plan_feed_record_v1(plan, &f.record, &f.identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_NE(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(c0_writer_checkpoint_does_not_complete_target_history)
{
	C0Fixture f;
	XLogRecPtr redo[2] = { 100, 200 };
	RfSideOnlinePlanV1 *plan;
	RfSideOnlineOperationV1 op = { 0 };
	RfSideOnlineApplyOpsV1 ops = { 0 };
	ApplyCapture capture = { 0 };
	c0_fixture(&f);
	ops.arg = &capture;
	ops.begin_protected_set = capture_begin;
	ops.end_protected_set = capture_end;
	ops.preflight_undo = ops.preflight_projection = accept_preflight;
	ops.apply_undo = capture_apply_undo;
	ops.apply_projection = capture_apply_projection;
	plan = c0_plan_redo(&f, RF_SIDE_PLAN_RECOVERY, 0, redo);
	if (c0_bind(plan, &f)) {
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &f.record, &f.identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &op));
		UT_ASSERT(op.history_only);
		/* Both whole-plan and exact writer selection need a target consumer. */
		for (int selected = 0; selected < 2; selected++) {
			ops.source_thread = selected ? 3 : 0;
			UT_ASSERT_NE(rf_side_online_plan_preflight_v1(plan, &ops), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_NE(rf_side_online_plan_apply_v1(plan, &ops), RF_PAGE_PROOF_DETAIL_OK);
		}
		UT_ASSERT_EQ(capture.begin_count, 0);
		UT_ASSERT_EQ(capture.undo_count + capture.projection_count, 0);
		/* Selecting the other empty source does not discharge this C0. */
		ops.source_thread = 2;
		UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &ops), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(capture.begin_count, 0);
	}
	rf_side_online_plan_destroy_v1(&plan);
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(c0_keeps_writer_and_target_full_values);
	UT_RUN(c0_missing_or_changed_claim_never_becomes_ordinary_undo);
	UT_RUN(c0_malformed_payload_and_header_refuse_before_capture);
	UT_RUN(c0_full_source_bind_is_bounded_and_sticky);
	UT_RUN(c0_bind_cannot_change_after_feed_or_seal);
	UT_RUN(c0_retained_plan_is_not_execution_or_pi_proof);
	UT_RUN(c0_registered_route_stays_blocked_for_ordinary_undo);
	UT_RUN(c0_failed_record_cannot_be_skipped_or_repaired_in_place);
	UT_RUN(c0_writer_checkpoint_does_not_complete_target_history);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
