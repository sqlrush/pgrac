/*-------------------------------------------------------------------------
 * test_cluster_relmap_create_conjunct.inc
 *    Original pending and creation readiness on the same saved cold plan.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_create_conjunct.inc
 * NOTES
 *    No Startup dispatcher, final owner or completion publisher is replaced.
 *-------------------------------------------------------------------------
 */

#include "utils/relmapper.h"

/* A second, unconsumed namespace is a real declaration/feed in the original
 * plan. Its empty retained directory supplies no pin or execution grant. */
static bool creation_with_unconsumed_pending(XLogReaderState *reader)
{
	ClusterColdRelmapInputV1 pending = {0};
	ClusterColdRecordV1 record = {0};
	static ClusterRelmapRetentionDirectory unconsumed;
	static LWLock unconsumed_lock;
	cluster_cold_plan_destroy_v1(&redo_typed.plan);
	owner_declaration.cut.tail_end = reader->EndRecPtr + 64;
	redo_typed.participants[0] = owner_declaration.cut;
	pending.cut = owner_declaration.cut;
	pending.source = owner_declaration.source;
	pending.frame = (ClusterColdRecordFrameV1){reader->EndRecPtr,
		reader->EndRecPtr + 64, 0x12345678, 0, 0x123, 0, 0, 0,
		RM_RELMAP_ID, XLOG_RELMAP_UPDATE, false};
	pending.staged.identity = (ClusterRelmapV2Namespace){
		redo_typed.system_identifier, pending.source.claim.database_incarnation, 1001, false};
	pending.staged.committed_generation = 9;
	pending.staged.pending_generation = 10;
	pending.staged.committed = owner_declaration.creation.committed;
	pending.staged.pending = pending.staged.committed;
	pending.staged.pending.mappings[0].mapfilenumber++;
	INIT_CRC32C(pending.staged.pending.crc);
	COMP_CRC32C(pending.staged.pending.crc, &pending.staged.pending, offsetof(RelMapFile, crc));
	FIN_CRC32C(pending.staged.pending.crc);
	pending.staged.pending_owner.node = pending.source.claim.identity.origin_node_id;
	pending.staged.pending_owner.top_full_xid = UINT64CONST(0x200000123);
	pending.staged.pending_owner.admitted_incarnation = pending.cut.owner_incarnation;
	pending.staged.pending_owner.boot_incarnation = 1;
	pending.staged.pending_owner.wal_end = pending.frame.end_rec_ptr;
	memset(&unconsumed, 0, sizeof(unconsumed));
	pending.retention = &unconsumed;
	pending.retention_lock = &unconsumed_lock;
	if (cluster_cold_plan_create_v1(&owner_declaration.cut, 1, 4*1024*1024, &redo_typed.plan) != 0
		|| cluster_cold_plan_relmap_create_declare_v1(redo_typed.plan, &owner_declaration, 1) != 0
		|| cluster_cold_plan_relmap_declare_v1(redo_typed.plan, &pending, 1) != 0) return false;
	record.read_rec_ptr = reader->ReadRecPtr;
	record.end_rec_ptr = reader->EndRecPtr;
	record.record_crc = owner_declaration.frame.record_crc;
	record.scn = 10;
	record.rmid = RM_RELMAP_ID;
	record.info = CLUSTER_RELMAP_CREATE_WAL_INFO_V1;
	record.record_flags = CLUSTER_COLD_RECORD_STRUCTURAL | CLUSTER_COLD_RECORD_SIDE_UNOWNED;
	record.relmap_dbid = owner_declaration.creation.identity.database_oid;
	record.relmap_tsid = DEFAULTTABLESPACE_OID;
	record.relmap_create = &owner_declaration.creation;
	if (cluster_cold_plan_feed_v1(redo_typed.plan, 0, &record) != 0) return false;
	memset(&record, 0, sizeof(record));
	record.read_rec_ptr = pending.frame.read_rec_ptr;
	record.end_rec_ptr = pending.frame.end_rec_ptr;
	record.prev_rec_ptr = reader->ReadRecPtr;
	record.record_crc = pending.frame.record_crc;
	record.source_xid = pending.frame.source_xid;
	record.scn = 20;
	record.rmid = RM_RELMAP_ID;
	record.info = XLOG_RELMAP_UPDATE;
	record.record_flags = CLUSTER_COLD_RECORD_STRUCTURAL;
	record.relmap_dbid = 1001;
	record.relmap_tsid = DEFAULTTABLESPACE_OID;
	record.relmap_image = &pending.staged.pending;
	return cluster_cold_plan_feed_v1(redo_typed.plan, 0, &record) == 0
		&& cluster_cold_plan_seal_v1(redo_typed.plan, owner_no_pages, NULL, NULL) == 0
		&& cluster_cold_plan_step_v1(redo_typed.plan, 0, &owner_step);
}

/* Feed both original structural records so the SIDE seal covers the same
 * full cut as the cold plan; the pending namespace remains unexecuted. */
static RfSideOnlinePlanV1 *creation_pending_side(XLogReaderState *creation_reader)
{
	const ClusterColdRelmapInputV1 *pending = &redo_typed.plan->relmap_inputs[0].input;
	RfSideOnlinePlanV1 *side = actual_creation_side(&owner_declaration, creation_reader, false);
	RfDetachedRecordPlanV1 detached = {0};
	RfPageOnlineRecordIdentityV1 identity = {0};
	XLogReaderState reader = {0};
	DecodedXLogRecord decoded = {0};
	xl_relmap_update header = {0};
	char payload[MinSizeOfRelmapUpdate + sizeof(RelMapFile)];
	if (!side) return NULL;
	header.dbid = pending->staged.identity.dbid;
	header.tsid = DEFAULTTABLESPACE_OID;
	header.nbytes = sizeof(RelMapFile);
	memcpy(payload, &header, MinSizeOfRelmapUpdate);
	memcpy(payload + MinSizeOfRelmapUpdate, &pending->staged.pending, sizeof(RelMapFile));
	reader.system_identifier = pending->source.claim.identity.system_identifier;
	reader.cluster_expected_thread_id = pending->cut.thread_id;
	reader.seg.ws_tli = pending->cut.timeline;
	reader.ReadRecPtr = decoded.lsn = pending->frame.read_rec_ptr;
	reader.EndRecPtr = decoded.next_lsn = pending->frame.end_rec_ptr;
	reader.record = &decoded;
	decoded.header.xl_rmid = RM_RELMAP_ID;
	decoded.header.xl_info = XLOG_RELMAP_UPDATE;
	decoded.header.xl_prev = creation_reader->ReadRecPtr;
	decoded.header.xl_crc = pending->frame.record_crc;
	decoded.header.xl_xid = pending->frame.source_xid;
	decoded.main_data = payload;
	decoded.main_data_len = sizeof(payload);
	decoded.max_block_id = -1;
	identity.record.system_identifier = reader.system_identifier;
	memcpy(identity.record.storage_uuid, pending->source.claim.identity.storage_uuid, 16);
	identity.record.origin_thread = pending->cut.thread_id;
	identity.record.timeline_id = pending->cut.timeline;
	identity.record.read_rec_ptr = reader.ReadRecPtr;
	identity.record.end_rec_ptr = reader.EndRecPtr;
	identity.record.record_crc = pending->frame.record_crc;
	identity.record.rmid = RM_RELMAP_ID;
	identity.record.info = XLOG_RELMAP_UPDATE;
	detached.source_record = &reader;
	detached.preflight_complete = true;
	detached.route.record_owner = RF_ROUTE_OWNER_SIDE_TYPED;
	detached.route.rmid = RM_RELMAP_ID;
	detached.route.normalized_info = XLOG_RELMAP_UPDATE;
	detached.route.codec_id = RF_ROUTE_CODEC_SIDE_STANDARD;
	if (rf_side_online_plan_feed_relmap_v1(side, &detached, &identity, pending) != 0
		|| rf_side_online_plan_seal_v1(side) != 0) rf_side_online_plan_destroy_v1(&side);
	return side;
}

UT_TEST(creation_pending_empty_cannot_replace_confirmation_or_old_completion)
{
	ClusterControlNativeRelmapCreateV2 *creator = NULL;
	XLogReaderState reader; DecodedXLogRecord decoded;
	RfSideOnlinePlanV1 *side; unsigned syncs, renames;
	UT_ASSERT(owner_setup(&creator, &reader, &decoded));
	side = actual_creation_side(&owner_declaration, &reader, true); UT_ASSERT(side);
	redo_typed.side_plans[0] = side;
	UT_ASSERT_EQ(cluster_cold_typed_relmap_pending_count_v1(&redo_typed), 0);
	UT_ASSERT(cluster_cold_typed_relmap_pending_ready_v1(&redo_typed, redo_fence));
	UT_ASSERT(!cluster_cold_typed_relmap_create_ready_v2(&redo_typed, redo_fence) && !target_available());
	if (!owner_apply(&reader, 65536)) {
		UT_ASSERT(false); owner_done(&creator); rf_side_online_plan_destroy_v1(&side); return;
	}
	syncs = copy_syncs; renames = root_renames;
	UT_ASSERT(cluster_cold_typed_relmap_pending_ready_v1(&redo_typed, redo_fence)
		&& cluster_cold_typed_relmap_create_ready_v2(&redo_typed, redo_fence));
	UT_ASSERT_EQ(cluster_cold_typed_relmap_pending_count_v1(&redo_typed), 0);
	UT_ASSERT(!cluster_cold_typed_relmap_ready_v1(&redo_typed, redo_fence));
	UT_ASSERT_EQ(cluster_cold_typed_relmap_replay_count_v1(&redo_typed), UINT32_MAX);
	UT_ASSERT_EQ(cluster_cold_typed_relmap_replay_input_v1(&redo_typed, 0).count, 0);
	UT_ASSERT(copy_syncs == syncs && root_renames == renames && !redo_failed);
	owner_done(&creator); rf_side_online_plan_destroy_v1(&side);
}

UT_TEST(creation_confirmed_receipt_cannot_complete_unconsumed_pending)
{
	ClusterControlNativeRelmapCreateV2 *creator = NULL;
	XLogReaderState reader; DecodedXLogRecord decoded;
	RfSideOnlinePlanV1 *side; unsigned syncs, renames;
	UT_ASSERT(owner_setup(&creator, &reader, &decoded));
	if (!creation_with_unconsumed_pending(&reader)) {
		UT_ASSERT(false); owner_done(&creator); return;
	}
	side = creation_pending_side(&reader);
	if (!side) { UT_ASSERT(false); owner_done(&creator); return; }
	redo_typed.side_plans[0] = side;
	UT_ASSERT_EQ(redo_typed.plan->relmap_count, 1);
	UT_ASSERT_EQ(redo_typed.plan->relmap_create_count, 1);
	UT_ASSERT(!cluster_cold_typed_relmap_pending_ready_v1(&redo_typed, redo_fence));
	UT_ASSERT_EQ(cluster_cold_typed_relmap_pending_count_v1(&redo_typed), UINT32_MAX);
	UT_ASSERT(owner_apply(&reader, 65536));
	syncs = copy_syncs; renames = root_renames;
	UT_ASSERT(cluster_cold_typed_relmap_create_ready_v2(&redo_typed, redo_fence));
	UT_ASSERT(!cluster_cold_typed_relmap_pending_ready_v1(&redo_typed, redo_fence));
	UT_ASSERT_EQ(cluster_cold_typed_relmap_pending_count_v1(&redo_typed), UINT32_MAX);
	UT_ASSERT(redo_typed.plan->relmap_inputs[0].captured
		&& !redo_typed.plan->relmap_inputs[0].attempted && !redo_typed.plan->relmap_inputs[0].applied
		&& cold_relmap_create_at(redo_typed.plan, 0)->receipt);
	UT_ASSERT(copy_syncs == syncs && root_renames == renames && !redo_failed);
	owner_done(&creator); rf_side_online_plan_destroy_v1(&side);
}

UT_TEST(creation_late_file_refusal_closes_pending_component_without_retry)
{
	ClusterControlNativeRelmapCreateV2 *creator = NULL;
	XLogReaderState reader; DecodedXLogRecord decoded;
	RfSideOnlinePlanV1 *side; unsigned syncs, renames;
	UT_ASSERT(owner_setup(&creator, &reader, &decoded));
	side = actual_creation_side(&owner_declaration, &reader, true); UT_ASSERT(side);
	redo_typed.side_plans[0] = side;
	if (!owner_apply(&reader, 65536)) {
		UT_ASSERT(false); owner_done(&creator); rf_side_online_plan_destroy_v1(&side); return;
	}
	syncs = copy_syncs; renames = root_renames;
	UT_ASSERT(cluster_cold_typed_relmap_pending_ready_v1(&redo_typed, redo_fence));
	physical_target_fault(creator, 3);
	UT_ASSERT(!cluster_cold_typed_relmap_create_ready_v2(&redo_typed, redo_fence));
	UT_ASSERT(!cluster_cold_typed_relmap_pending_ready_v1(&redo_typed, redo_fence));
	UT_ASSERT_EQ(cluster_cold_typed_relmap_pending_count_v1(&redo_typed), UINT32_MAX);
	UT_ASSERT(!owner_apply(&reader, 65536) && copy_syncs == syncs && root_renames == renames
		&& cold_relmap_create_at(redo_typed.plan, 0)->receipt && redo_failed);
	owner_done(&creator); rf_side_online_plan_destroy_v1(&side);
}

UT_TEST(creation_final_owner_error_keeps_both_components_closed)
{
	ClusterControlNativeRelmapCreateV2 *creator = NULL;
	XLogReaderState reader; DecodedXLogRecord decoded;
	RfSideOnlinePlanV1 *side; unsigned syncs, renames;
	volatile bool caught = false;
	UT_ASSERT(owner_setup(&creator, &reader, &decoded));
	side = actual_creation_side(&owner_declaration, &reader, true); UT_ASSERT(side);
	redo_typed.side_plans[0] = side;
	if (!owner_apply(&reader, 65536)) {
		UT_ASSERT(false); owner_done(&creator); rf_side_online_plan_destroy_v1(&side); return;
	}
	syncs = copy_syncs; renames = root_renames;
	UT_ASSERT(cluster_cold_typed_relmap_pending_ready_v1(&redo_typed, redo_fence));
	owner_source_error = true;
	PG_TRY(); { (void)cluster_cold_typed_relmap_create_ready_v2(&redo_typed, redo_fence); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	owner_source_error = false;
	UT_ASSERT(caught && redo_failed && redo_typed.plan->relmap_failed);
	UT_ASSERT(!cluster_cold_typed_relmap_pending_ready_v1(&redo_typed, redo_fence)
		&& !cluster_cold_typed_relmap_create_ready_v2(&redo_typed, redo_fence));
	UT_ASSERT_EQ(cluster_cold_typed_relmap_pending_count_v1(&redo_typed), UINT32_MAX);
	UT_ASSERT(!owner_apply(&reader, 65536) && copy_syncs == syncs && root_renames == renames
		&& cold_relmap_create_at(redo_typed.plan, 0)->receipt && cf_held && copy_x
		&& !held_family_lock && !InterruptHoldoffCount);
	owner_done(&creator); rf_side_online_plan_destroy_v1(&side);
}
