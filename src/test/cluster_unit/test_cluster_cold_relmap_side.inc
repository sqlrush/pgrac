/*-------------------------------------------------------------------------
 * test_cluster_cold_relmap_side.inc
 *    Complete externally owned relmap carrier and original SIDE coexistence.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_cold_relmap_side.inc
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_cold_relmap.h"
#include "utils/relmapper.h"
#include "catalog/pg_tablespace_d.h"

extern RfPageProofDetailV1 rf_side_online_plan_feed_relmap_v1(RfSideOnlinePlanV1 *,
	const RfDetachedRecordPlanV1 *, const RfPageOnlineRecordIdentityV1 *,
	const ClusterColdRelmapInputV1 *);
extern bool rf_side_online_plan_relmap_matches_v1(const RfSideOnlinePlanV1 *,
	const ClusterWalSourceRef *, const XLogReaderState *);
extern uint32 rf_side_online_plan_relmap_count_v1(const RfSideOnlinePlanV1 *,
	const ClusterWalSourceRef *);
extern const ClusterColdRelmapInputV1 *cluster_cold_plan_relmap_captured_v1(
	const ClusterColdPlanV1 *, uint32, const ClusterColdRecordV1 *);

static int relmap_side_pin_token, relmap_side_lock_token;

static void
relmap_side_frame(FakeXactRecord *fake, const ClusterWalSourceRef *source,
	ClusterColdRelmapInputV1 *input, RfPageOnlineRecordIdentityV1 *identity,
	XLogRecPtr begin, XLogRecPtr end)
{
	RelMapFile map = {0};
	xl_relmap_update header = {InvalidOid, GLOBALTABLESPACE_OID, sizeof(RelMapFile)};
	uint8 payload[MinSizeOfRelmapUpdate + sizeof(RelMapFile)];
	map.magic = RELMAPPER_FILEMAGIC;
	map.num_mappings = 1;
	map.mappings[0].mapoid = 1259;
	map.mappings[0].mapfilenumber = 40000;
	INIT_CRC32C(map.crc);
	COMP_CRC32C(map.crc, &map, offsetof(RelMapFile, crc));
	FIN_CRC32C(map.crc);
	memcpy(payload, &header, MinSizeOfRelmapUpdate);
	memcpy(payload + MinSizeOfRelmapUpdate, &map, sizeof(map));
	make_projection_record(fake, RM_RELMAP_ID, XLOG_RELMAP_UPDATE, payload, sizeof(payload));
	fake->u.decoded.max_block_id = -1;
	fake->u.decoded.header.xl_xid = 120;
	fake->u.decoded.header.xl_tot_len = SizeOfXLogRecord + 3 + sizeof(payload);
	fake->reader.cluster_expected_thread_id = 3;
	fake->reader.seg.ws_tli = 7;
	*identity = make_identity(fake, (uint8 *)source->claim.identity.storage_uuid);
	set_identity_range(fake, identity, begin, end);
	memset(input, 0, sizeof(*input));
	/* Cold participant index and this SIDE plan's local index are distinct. */
	input->participant = 0;
	input->cut = (ClusterColdParticipantV1){3, 0, 7, 19, 100, 100, 512};
	input->source = *source;
	input->frame = (ClusterColdRecordFrameV1){begin, end, identity->record.record_crc,
		0, 120, 0, 0, 0, RM_RELMAP_ID, XLOG_RELMAP_UPDATE, false};
	input->staged.identity = (ClusterRelmapV2Namespace){source->claim.identity.system_identifier,
		source->claim.database_incarnation, InvalidOid, true};
	input->staged.committed_generation = 7;
	input->staged.pending_generation = 8;
	input->staged.pending_owner = (ClusterRelmapV2Owner){2,
		(UINT64_C(8) << 32) | 120, 7, 19, end, 41};
	input->staged.committed = input->staged.pending = map;
	input->retention = (struct ClusterRelmapRetentionDirectory *)&relmap_side_pin_token;
	input->retention_lock = (struct LWLock *)&relmap_side_lock_token;
}

static ClusterColdRecordV1
relmap_side_cold_record(const ClusterColdRelmapInputV1 *in)
{
	ClusterColdRecordV1 record = {0};
	record.read_rec_ptr = in->frame.read_rec_ptr;
	record.end_rec_ptr = in->frame.end_rec_ptr;
	record.record_crc = in->frame.record_crc;
	record.source_xid = in->frame.source_xid;
	record.rmid = RM_RELMAP_ID;
	record.info = XLOG_RELMAP_UPDATE;
	record.record_flags = CLUSTER_COLD_RECORD_STRUCTURAL | CLUSTER_COLD_RECORD_SIDE_UNOWNED;
	record.relmap_image = &in->staged.pending;
	record.relmap_tsid = GLOBALTABLESPACE_OID;
	return record;
}

UT_TEST(test_declared_relmap_capture_keeps_complete_side_bytes)
{
	ClusterWalSourceRef source;
	RfSideOnlinePlanV1 *side = adopted_relmap_terminal_witness_plan(&source, 0);
	ClusterColdPlanV1 *cold = NULL;
	ClusterColdRelmapInputV1 input;
	FakeXactRecord fake, saved;
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 detached;
	ClusterColdRecordV1 record;
	const ClusterColdRelmapInputV1 *captured;
	relmap_side_frame(&fake, &source, &input, &identity, 100, 512);
	record = relmap_side_cold_record(&input);
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(&input.cut, 1, 1048576, &cold), 0);
	UT_ASSERT_EQ(cluster_cold_plan_relmap_declare_v1(cold, &input, 1), 0);
	UT_ASSERT(cluster_cold_plan_relmap_captured_v1(cold, 0, &record) == NULL);
	UT_ASSERT_EQ(cluster_cold_plan_feed_v1(cold, 0, &record), 0);
	captured = cluster_cold_plan_relmap_captured_v1(cold, 0, &record);
	UT_ASSERT(captured != NULL);
	detached = make_projection_record_plan(&fake);
	UT_ASSERT_EQ(rf_side_online_plan_feed_relmap_v1(side, &detached, &identity, captured), 0);
	clog_witness_copy(&saved, &fake);
	memset(fake.data, 0xa5, sizeof(fake.data));
	UT_ASSERT_EQ(rf_side_online_plan_relmap_count_v1(side, &source), UINT32_MAX);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(side), 0);
	UT_ASSERT_EQ(rf_side_online_plan_relmap_count_v1(side, &source), 1);
	UT_ASSERT(rf_side_online_plan_relmap_matches_v1(side, &source, &saved.reader));
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(side), 0);
	UT_ASSERT(cluster_cold_plan_relmap_captured_v1(cold, 1, &record) == NULL);
	record.record_crc++;
	UT_ASSERT(cluster_cold_plan_relmap_captured_v1(cold, 0, &record) == NULL);
	cluster_cold_plan_destroy_v1(&cold);
	rf_side_online_plan_destroy_v1(&side);
}

UT_TEST(test_relmap_carrier_refuses_missing_owner_and_keeps_original_refusal)
{
	ClusterWalSourceRef source;
	RfSideOnlinePlanV1 *side = adopted_relmap_terminal_witness_plan(&source, 0);
	ClusterColdPlanV1 *cold = NULL;
	ClusterColdRelmapInputV1 input;
	FakeXactRecord fake;
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 detached;
	ClusterColdRecordV1 record;
	relmap_side_frame(&fake, &source, &input, &identity, 100, 512);
	detached = make_projection_record_plan(&fake);
	record = relmap_side_cold_record(&input);
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(&input.cut, 1, 1048576, &cold), 0);
	UT_ASSERT_EQ(cluster_cold_plan_feed_v1(cold, 0, &record), CLUSTER_COLD_STRUCTURAL_UNSUPPORTED);
	UT_ASSERT(cluster_cold_plan_relmap_captured_v1(cold, 0, &record) == NULL);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(side, &detached, &identity),
		RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED);
	UT_ASSERT_NE(rf_side_online_plan_seal_v1(side), 0);
	cluster_cold_plan_destroy_v1(&cold);
	rf_side_online_plan_destroy_v1(&side);
}

UT_TEST(test_relmap_carrier_rejects_source_frame_cut_payload_and_purpose_drift)
{
	for (unsigned fault = 0; fault < 15; ++fault) {
		ClusterWalSourceRef source;
		RfSideOnlinePlanV1 *side = adopted_relmap_terminal_witness_plan_shape(&source, 0, fault == 14 ? 3 : 0);
		ClusterColdRelmapInputV1 input;
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 detached;
		relmap_side_frame(&fake, &source, &input, &identity, 100, 512);
		detached = make_projection_record_plan(&fake);
		switch (fault) {
		case 0: input.source.claim.claim_sha256[0] ^= 1; break;
		case 1: input.cut.owner_incarnation++; break;
		case 2: input.cut.native_redo++; break;
		case 3: input.cut.tail_end++; break;
		case 4: input.frame.record_crc++; break;
		case 5: input.frame.source_xid++; break;
		case 6: input.frame.history = true; break;
		case 7: input.staged.pending.mappings[0].mapfilenumber++; break;
		case 8: fake.u.decoded.max_block_id = 0; break;
		case 9: fake.u.decoded.has_page_version_edge = true; break;
		case 10: fake.reader.cluster_record_crc_failed = true; break;
		case 11: fake.u.decoded.record_origin = 1; break;
		case 12: detached.component_count = 1; break;
		case 13: input.staged.identity.database_incarnation++; break;
		}
		UT_ASSERT_NE(rf_side_online_plan_feed_relmap_v1(side, &detached, &identity, &input), 0);
		UT_ASSERT_NE(rf_side_online_plan_seal_v1(side), 0);
		UT_ASSERT_EQ(rf_side_online_plan_relmap_count_v1(side, &source), UINT32_MAX);
		rf_side_online_plan_destroy_v1(&side);
	}
}

UT_TEST(test_relmap_carrier_charges_original_budget_and_duplicate_is_sticky)
{
	Size used = 0;
	for (unsigned attempt = 0; attempt < 3; ++attempt) {
		ClusterWalSourceRef source;
		RfSideOnlinePlanV1 *side = adopted_relmap_terminal_witness_plan(&source,
			attempt == 0 ? 0 : used - (attempt == 1));
		ClusterColdRelmapInputV1 input;
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 detached;
		relmap_side_frame(&fake, &source, &input, &identity, 100, 512);
		detached = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_relmap_v1(side, &detached, &identity, &input),
			attempt == 1 ? RF_PAGE_PROOF_DETAIL_CAPACITY : 0);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(side) == 0, attempt != 1);
		if (attempt == 0)
			used = RF_SIDE_ONLINE_PLAN_MAX_BYTES - rf_side_online_plan_scratch_available_v1(side);
		rf_side_online_plan_destroy_v1(&side);
	}
	{
		ClusterWalSourceRef source;
		RfSideOnlinePlanV1 *side = adopted_relmap_terminal_witness_plan(&source, 0);
		ClusterColdRelmapInputV1 input;
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 detached;
		relmap_side_frame(&fake, &source, &input, &identity, 100, 300);
		detached = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_relmap_v1(side, &detached, &identity, &input), 0);
		UT_ASSERT_NE(rf_side_online_plan_feed_relmap_v1(side, &detached, &identity, &input), 0);
		UT_ASSERT_NE(rf_side_online_plan_seal_v1(side), 0);
		rf_side_online_plan_destroy_v1(&side);
	}
}

UT_TEST(test_relmap_carrier_checks_entire_header_main_and_source_after_seal)
{
	ClusterWalSourceRef source;
	RfSideOnlinePlanV1 *side = adopted_relmap_terminal_witness_plan(&source, 0);
	ClusterColdRelmapInputV1 input;
	FakeXactRecord fake;
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 detached;
	relmap_side_frame(&fake, &source, &input, &identity, 100, 512);
	detached = make_projection_record_plan(&fake);
	UT_ASSERT_EQ(rf_side_online_plan_feed_relmap_v1(side, &detached, &identity, &input), 0);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(side), 0);
	for (unsigned fault = 0; fault < 8; ++fault) {
		FakeXactRecord query;
		ClusterWalSourceRef other = source;
		clog_witness_copy(&query, &fake);
		switch (fault) {
		case 0: query.u.decoded.header.xl_prev++; break;
		case 1: query.data[MinSizeOfRelmapUpdate + 500] ^= 1; break;
		case 2: query.reader.EndRecPtr++; break;
		case 3: other.claim.claim_sha256[0] ^= 1; break;
		case 4: query.u.decoded.toplevel_xid++; break;
		case 5: query.u.decoded.record_origin = 1; break;
		case 6: query.reader.cluster_record_crc_failed = true; break;
		case 7: query.u.decoded.max_block_id = 0; break;
		}
		UT_ASSERT(!rf_side_online_plan_relmap_matches_v1(side, &other, &query.reader));
	}
	UT_ASSERT(rf_side_online_plan_relmap_matches_v1(side, &source, &fake.reader));
	rf_side_online_plan_destroy_v1(&side);
}

UT_TEST(test_relmap_and_clog_coexist_but_side_cannot_publish_whole_source)
{
	ClusterWalSourceRef source;
	RfSideOnlinePlanV1 *side = adopted_relmap_terminal_witness_plan(&source, 0);
	ClusterColdRelmapInputV1 input;
	FakeXactRecord fake, clog;
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 detached;
	RfSideOnlineOperationV1 operation;
	RfSideOnlineProductionOwnerV1 owner;
	ClusterRecoverySerialGuard guard = {.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED};
	ClusterThreadRecoveryAuthorityV1 authority = {.duty = &canonical_duty, .serial_guard = &guard};
	relmap_side_frame(&fake, &source, &input, &identity, 100, 300);
	input.participant = input.frame.participant = 6; /* SIDE index remains zero. */
	detached = make_projection_record_plan(&fake);
	UT_ASSERT_EQ(rf_side_online_plan_feed_relmap_v1(side, &detached, &identity, &input), 0);
	clog_witness_frame(&clog, 0);
	UT_ASSERT_EQ(clog_witness_feed(side, &clog, &source, 300, 512), 0);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(side), 0);
	UT_ASSERT_EQ(rf_side_online_plan_relmap_count_v1(side, &source), 1);
	UT_ASSERT_EQ(rf_side_online_plan_native_clog_count_v1(side, &source), 1);
	UT_ASSERT(rf_side_online_plan_clog_matches_v1(side, &source, &clog.reader));
	UT_ASSERT(rf_side_online_plan_relmap_matches_v1(side, &source, &fake.reader));
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(side), 1);
	clog_applied_owner(&owner, &authority);
	UT_ASSERT_NE(rf_side_online_production_apply_v1(side, &owner), 0);
	UT_ASSERT_EQ(cold_clog_applies, 0);
	UT_ASSERT(!owner.protected_set_complete);
	UT_ASSERT(rf_side_online_plan_operation_v1(side, 0, &operation));
	UT_ASSERT_EQ(rf_side_online_production_apply_record_v1(side, 0, &operation.identity, &owner), 0);
	UT_ASSERT_EQ(cold_clog_applies, 1);
	UT_ASSERT(rf_side_online_plan_clog_applied_v1(side, &source, &clog.reader));
	UT_ASSERT(!owner.protected_set_complete);
	clog_applied_done(&side);
}
