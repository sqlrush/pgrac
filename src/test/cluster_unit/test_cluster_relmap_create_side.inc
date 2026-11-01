/*-------------------------------------------------------------------------
 * test_cluster_relmap_create_side.inc
 *    Complete declared creation bytes in the original sealed SIDE plan.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_create_side.inc
 * NOTES
 *    Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
#include "common/cluster_relmap_create.h"

static void
create_side_frame(FakeXactRecord *fake, const ClusterWalSourceRef *source,
	ClusterColdRelmapCreateInputV1 *in, RfPageOnlineRecordIdentityV1 *identity,
	XLogRecPtr begin, XLogRecPtr end, Oid dbid)
{
	ClusterColdRelmapInputV1 pending;
	uint8 bytes[CLUSTER_RELMAP_CREATE_WAL_SIZE_V1];
	ClusterRecoveryFileIdentityV2 *id;
	relmap_side_frame(fake, source, &pending, identity, begin, end);
	memset(in, 0, sizeof(*in));
	in->participant = in->frame.participant = 6; /* Not the SIDE local index. */
	in->cut = pending.cut; in->source = *source; in->frame = pending.frame;
	in->frame.participant = in->participant;
	in->frame.info = XLOG_RELMAP_CREATE_V1;
	id = &in->creation.identity;
	id->kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
	id->format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	id->format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	id->system_identifier = source->claim.identity.system_identifier;
	id->database_incarnation = source->claim.database_incarnation;
	memcpy(id->storage_uuid, source->claim.identity.storage_uuid, 16);
	id->redo_branch_uuid[0] = 9; id->redo_branch_generation = 4;
	id->redo_timeline = in->cut.timeline;
	id->database_oid = dbid; id->tablespace_oid = DEFAULTTABLESPACE_OID;
	id->origin = source->claim.identity.origin_node_id;
	id->creation_generation = source->claim.identity.origin_owner_incarnation;
	id->registration_generation = 41;
	in->creation.batch_uuid[0] = 7; in->creation.batch_uuid[15] = 19;
	in->creation.committed = pending.staged.committed;
	/* The complete native image includes inactive mapping bytes. */
	in->creation.committed.mappings[MAX_MAPPINGS-1] = (RelMapping){999, 777};
	INIT_CRC32C(in->creation.committed.crc);
	COMP_CRC32C(in->creation.committed.crc, &in->creation.committed, offsetof(RelMapFile, crc));
	FIN_CRC32C(in->creation.committed.crc);
	UT_ASSERT(cluster_relmap_create_encode_v1(id, in->creation.batch_uuid,
		&in->creation.committed, bytes, sizeof(bytes)));
	make_projection_record(fake, RM_RELMAP_ID, XLOG_RELMAP_CREATE_V1, bytes, sizeof(bytes));
	fake->u.decoded.max_block_id = -1;
	fake->u.decoded.header.xl_xid = 120;
	fake->u.decoded.header.xl_tot_len = SizeOfXLogRecord + 3 + sizeof(bytes);
	fake->reader.cluster_expected_thread_id = in->cut.thread_id;
	fake->reader.seg.ws_tli = in->cut.timeline;
	*identity = make_identity(fake, (uint8 *)source->claim.identity.storage_uuid);
	set_identity_range(fake, identity, begin, end);
	in->frame.record_crc = identity->record.record_crc;
}

UT_TEST(test_creation_side_owns_original_captured_values_and_bytes)
{
	ClusterWalSourceRef source;
	RfSideOnlinePlanV1 *side = adopted_relmap_terminal_witness_plan(&source, 0);
	ClusterColdPlanV1 *cold = NULL;
	ClusterColdRelmapCreateInputV1 in, saved;
	ClusterColdRecordV1 record = {0};
	FakeXactRecord fake, native;
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 detached;
	const ClusterColdRelmapCreateInputV1 *captured;
	RfSideOnlineProductionOwnerV1 owner;
	ClusterRecoverySerialGuard guard = {.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED};
	ClusterThreadRecoveryAuthorityV1 authority = {.duty = &canonical_duty, .serial_guard = &guard};
	create_side_frame(&fake, &source, &in, &identity, 100, 512, 2000);
	in.participant = in.frame.participant = 0; saved = in;
	record.read_rec_ptr = 100; record.end_rec_ptr = 512;
	record.record_crc = in.frame.record_crc; record.source_xid = 120;
	record.rmid = RM_RELMAP_ID; record.info = XLOG_RELMAP_CREATE_V1;
	record.record_flags = CLUSTER_COLD_RECORD_STRUCTURAL | CLUSTER_COLD_RECORD_SIDE_UNOWNED;
	record.relmap_dbid = 2000; record.relmap_tsid = DEFAULTTABLESPACE_OID;
	record.relmap_create = &in.creation;
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(&in.cut, 1, 1048576, &cold), 0);
	UT_ASSERT_EQ(cluster_cold_plan_relmap_create_declare_v1(cold, &in, 1), 0);
	UT_ASSERT_EQ(cluster_cold_plan_feed_v1(cold, 0, &record), 0);
	captured = cluster_cold_plan_relmap_create_captured_v1(cold, 0, &record);
	UT_ASSERT_NOT_NULL(captured);
	detached = make_projection_record_plan(&fake);
	UT_ASSERT_EQ(rf_side_online_plan_feed_relmap_create_v1(side, &detached, &identity, captured), 0);
	clog_witness_copy(&native, &fake);
	memset(fake.data, 0xa5, sizeof(fake.data));
	memset(&in, 0xa5, sizeof(in));
	cluster_cold_plan_destroy_v1(&cold);
	UT_ASSERT_EQ(rf_side_online_plan_relmap_create_count_v1(side, &source), UINT32_MAX);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(side), 0);
	UT_ASSERT_EQ(rf_side_online_plan_relmap_create_count_v1(side, &source), 1);
	UT_ASSERT(rf_side_online_plan_relmap_create_input_matches_v1(side, &saved));
	UT_ASSERT(rf_side_online_plan_relmap_create_matches_v1(side, &source, &native.reader));
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(side), 0);
	clog_applied_owner(&owner, &authority);
	UT_ASSERT_NE(rf_side_online_production_apply_v1(side, &owner), 0);
	UT_ASSERT(!owner.protected_set_complete && !owner.protected_set_active);
	clog_applied_done(&side);
}

UT_TEST(test_creation_side_refuses_wrong_owner_source_frame_and_envelope)
{
	for (unsigned fault = 0; fault < 23; ++fault) {
		ClusterWalSourceRef source;
		RfSideOnlinePlanV1 *side = adopted_relmap_terminal_witness_plan(&source, 0);
		ClusterColdRelmapCreateInputV1 in;
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 detached;
		create_side_frame(&fake, &source, &in, &identity, 100, 512, 2000);
		detached = make_projection_record_plan(&fake);
		switch (fault) {
		case 0: in.source.claim.claim_sha256[0] ^= 1; break;
		case 1: in.cut.owner_incarnation++; break;
		case 2: in.cut.native_redo++; break;
		case 3: in.cut.tail_end++; break;
		case 4: in.frame.record_crc++; break;
		case 5: in.frame.source_xid++; break;
		case 6: in.frame.history = true; break;
		case 7: in.creation.batch_uuid[0]++; break;
		case 8: in.creation.identity.origin++; break;
		case 9: in.creation.identity.creation_generation++; break;
		case 10: in.creation.identity.database_incarnation++; break;
		case 11: fake.u.decoded.max_block_id = 0; break;
		case 12: fake.u.decoded.has_page_version_edge = true; break;
		case 13: fake.reader.cluster_record_crc_failed = true; break;
		case 14: fake.u.decoded.record_origin = 1; break;
		case 15: detached.component_count = 1; break;
		case 16: detached.route.normalized_info = XLOG_RELMAP_UPDATE; break;
		case 17: fake.u.decoded.main_data_len--; break;
		case 18: fake.reader.cluster_expected_thread_id++; break;
		case 19: in.frame.participant++; break;
		case 20: in.frame.space_effects = 1; break;
		case 21: in.frame.source_top_xid = 121; break;
		case 22: in.creation.committed.mappings[MAX_MAPPINGS-1].mapoid++; break;
		}
		UT_ASSERT_NE(rf_side_online_plan_feed_relmap_create_v1(side, &detached, &identity, &in), 0);
		UT_ASSERT_NE(rf_side_online_plan_seal_v1(side), 0);
		UT_ASSERT_EQ(rf_side_online_plan_relmap_create_count_v1(side, &source), UINT32_MAX);
		rf_side_online_plan_destroy_v1(&side);
	}
}

UT_TEST(test_creation_side_matches_complete_native_bytes_and_declared_values)
{
	ClusterWalSourceRef source;
	RfSideOnlinePlanV1 *side = adopted_relmap_terminal_witness_plan(&source, 0);
	ClusterColdRelmapCreateInputV1 in;
	FakeXactRecord fake, other;
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 detached;
	create_side_frame(&fake, &source, &in, &identity, 100, 512, 2000);
	detached = make_projection_record_plan(&fake);
	UT_ASSERT_EQ(rf_side_online_plan_feed_relmap_create_v1(side, &detached, &identity, &in), 0);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(side), 0);
	for (Size byte = 0; byte < CLUSTER_RELMAP_CREATE_WAL_SIZE_V1; ++byte) {
		clog_witness_copy(&other, &fake); other.data[byte] ^= 1;
		UT_ASSERT(!rf_side_online_plan_relmap_create_matches_v1(side, &source, &other.reader));
	}
	clog_witness_copy(&other, &fake); other.u.decoded.header.xl_prev++;
	UT_ASSERT(!rf_side_online_plan_relmap_create_matches_v1(side, &source, &other.reader));
	for (unsigned fault = 0; fault < 8; ++fault) {
		ClusterColdRelmapCreateInputV1 changed = in;
		switch (fault) {
		case 0: changed.participant++; break;
		case 1: changed.cut.physical_lower++; break;
		case 2: changed.source.claim.identity.root_lineage_seq++; break;
		case 3: changed.frame.end_rec_ptr++; break;
		case 4: changed.creation.batch_uuid[15]++; break;
		case 5: changed.creation.identity.registration_generation++; break;
		case 6: changed.creation.committed.mappings[MAX_MAPPINGS-1].mapoid++; break;
		case 7: changed.frame.source_top_xid++; break;
		}
		UT_ASSERT(!rf_side_online_plan_relmap_create_input_matches_v1(side, &changed));
	}
	UT_ASSERT(rf_side_online_plan_relmap_create_matches_v1(side, &source, &fake.reader));
	UT_ASSERT(rf_side_online_plan_relmap_create_input_matches_v1(side, &in));
	source.claim.claim_sha256[1] ^= 1;
	UT_ASSERT_EQ(rf_side_online_plan_relmap_create_count_v1(side, &source), UINT32_MAX);
	rf_side_online_plan_destroy_v1(&side);
}

UT_TEST(test_creation_side_budget_and_duplicate_refusal_are_sticky)
{
	Size used = 0;
	for (unsigned attempt = 0; attempt < 3; ++attempt) {
		ClusterWalSourceRef source;
		RfSideOnlinePlanV1 *side = adopted_relmap_terminal_witness_plan(&source,
			attempt == 0 ? 0 : used - (attempt == 1));
		ClusterColdRelmapCreateInputV1 in;
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 detached;
		create_side_frame(&fake, &source, &in, &identity, 100, 512, 2000);
		detached = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_relmap_create_v1(side, &detached, &identity, &in),
			attempt == 1 ? RF_PAGE_PROOF_DETAIL_CAPACITY : 0);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(side) == 0, attempt != 1);
		if (attempt == 0) used = RF_SIDE_ONLINE_PLAN_MAX_BYTES - rf_side_online_plan_scratch_available_v1(side);
		rf_side_online_plan_destroy_v1(&side);
	}
	{
		ClusterWalSourceRef source;
		RfSideOnlinePlanV1 *side = adopted_relmap_terminal_witness_plan(&source, 0);
		ClusterColdRelmapCreateInputV1 in;
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 detached;
		create_side_frame(&fake, &source, &in, &identity, 100, 300, 2000);
		detached = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_relmap_create_v1(side, &detached, &identity, &in), 0);
		UT_ASSERT_NE(rf_side_online_plan_feed_relmap_create_v1(side, &detached, &identity, &in), 0);
		UT_ASSERT_NE(rf_side_online_plan_seal_v1(side), 0);
		rf_side_online_plan_destroy_v1(&side);
	}
}

UT_TEST(test_creation_side_cannot_use_ordinary_or_online_route)
{
	for (unsigned ordinary = 0; ordinary < 2; ++ordinary) {
		ClusterWalSourceRef source;
		RfSideOnlinePlanV1 *side = adopted_relmap_terminal_witness_plan_shape(&source, 0, ordinary ? 0 : 3);
		ClusterColdRelmapCreateInputV1 in;
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 detached;
		create_side_frame(&fake, &source, &in, &identity, 100, 512, 2000);
		detached = make_projection_record_plan(&fake);
		UT_ASSERT_NE(ordinary ? rf_side_online_plan_feed_record_v1(side, &detached, &identity)
			: rf_side_online_plan_feed_relmap_create_v1(side, &detached, &identity, &in), 0);
		UT_ASSERT_NE(rf_side_online_plan_seal_v1(side), 0);
		rf_side_online_plan_destroy_v1(&side);
	}
}

UT_TEST(test_creation_pending_clog_coexist_without_whole_source_completion)
{
	ClusterWalSourceRef source;
	RfSideOnlinePlanV1 *side = adopted_relmap_terminal_witness_plan(&source, 0);
	ClusterColdRelmapCreateInputV1 in;
	ClusterColdRelmapInputV1 pending;
	FakeXactRecord fake, update, clog;
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 detached;
	RfSideOnlineProductionOwnerV1 owner;
	ClusterRecoverySerialGuard guard = {.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED};
	ClusterThreadRecoveryAuthorityV1 authority = {.duty = &canonical_duty, .serial_guard = &guard};
	create_side_frame(&fake, &source, &in, &identity, 100, 200, 2000);
	detached = make_projection_record_plan(&fake);
	UT_ASSERT_EQ(rf_side_online_plan_feed_relmap_create_v1(side, &detached, &identity, &in), 0);
	relmap_side_frame(&update, &source, &pending, &identity, 200, 300);
	detached = make_projection_record_plan(&update);
	UT_ASSERT_EQ(rf_side_online_plan_feed_relmap_v1(side, &detached, &identity, &pending), 0);
	clog_witness_frame(&clog, 0);
	UT_ASSERT_EQ(clog_witness_feed(side, &clog, &source, 300, 512), 0);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(side), 0);
	UT_ASSERT_EQ(rf_side_online_plan_relmap_create_count_v1(side, &source), 1);
	UT_ASSERT_EQ(rf_side_online_plan_relmap_count_v1(side, &source), 1);
	UT_ASSERT_EQ(rf_side_online_plan_native_clog_count_v1(side, &source), 1);
	clog_applied_owner(&owner, &authority);
	UT_ASSERT_NE(rf_side_online_production_apply_v1(side, &owner), 0);
	UT_ASSERT_EQ(cold_clog_applies, 0);
	UT_ASSERT(!owner.protected_set_complete);
	clog_applied_done(&side);
}

UT_TEST(test_creation_side_streams_more_than_sixty_four_records)
{
	ClusterWalSourceRef source;
	RfSideOnlinePlanV1 *side = adopted_relmap_terminal_witness_plan(&source, 0);
	ClusterColdRelmapCreateInputV1 first;
	FakeXactRecord first_record;
	for (unsigned i = 0; i < 70; ++i) {
		ClusterColdRelmapCreateInputV1 in;
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 detached;
		create_side_frame(&fake, &source, &in, &identity, 100+i*5, i==69 ? 512 : 105+i*5, 2000+i);
		detached = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_relmap_create_v1(side, &detached, &identity, &in), 0);
		if (i==0) { first=in; clog_witness_copy(&first_record, &fake); }
	}
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(side), 0);
	UT_ASSERT_EQ(rf_side_online_plan_relmap_create_count_v1(side, &source), 70);
	UT_ASSERT(rf_side_online_plan_relmap_create_input_matches_v1(side, &first));
	UT_ASSERT(rf_side_online_plan_relmap_create_matches_v1(side, &source, &first_record.reader));
	rf_side_online_plan_destroy_v1(&side);
}
