/*-------------------------------------------------------------------------
 *
 * test_cluster_control_relmap_terminal.inc
 *	  Original episode terminal carrier correspondence tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_control_relmap_terminal.inc
 *
 * NOTES
 *	  Lower Startup/GES authority leaves are controlled; no CLOG lifetime
 *	  or COMPLETE qualification is supplied.
 *
 *-------------------------------------------------------------------------
 */
#include "access/xact.h"
#include "cluster/cluster_side_online_plan.h"

typedef struct RelmapEpisodeTerminal {
	XLogReaderState reader;
	uint8 payload[sizeof(xl_xact_commit) + sizeof(xl_xact_xinfo)
		+ sizeof(xl_xact_scn) + sizeof(xl_xact_tt_commit)];
	DecodedXLogRecord decoded;
} RelmapEpisodeTerminal;

static void
relmap_episode_terminal_frame(RelmapEpisodeTerminal *frame, uint32 p)
{
	const ClusterWalSourceRef *source = &tx_inputs_typed.sources[p];
	const ClusterColdParticipantV1 *part = &tx_inputs_typed.participants[p];
	xl_xact_commit commit = {0}; xl_xact_xinfo xinfo = {0};
	xl_xact_scn scn = {0}; xl_xact_tt_commit tt = {0}; uint8 *cursor;
	memset(frame, 0, sizeof(*frame));
	commit.xact_time = 123456; xinfo.xinfo = XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_TT_COMMIT;
	scn.scn = 12345; tt.instance = source->claim.identity.origin_node_id + 1;
	tt.segment_id = (tt.instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	tt.segment_generation = 11; tt.slot_offset = 4;
	tt.wrap = 7; tt.xid = 802; tt.format_version = CLUSTER_XACT_TT_COMMIT_VERSION;
	tt.commit_scn = scn.scn; cursor = frame->payload;
	memcpy(cursor, &commit, sizeof(commit)); cursor += sizeof(commit);
	memcpy(cursor, &xinfo, sizeof(xinfo)); cursor += sizeof(xinfo);
	memcpy(cursor, &scn, sizeof(scn)); cursor += sizeof(scn);
	memcpy(cursor, &tt, sizeof(tt)); cursor += sizeof(tt);
	frame->decoded.header.xl_rmid = RM_XACT_ID;
	frame->decoded.header.xl_info = XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO;
	frame->decoded.header.xl_xid = 802;
	frame->decoded.header.xl_crc = 0xabc123;
	frame->decoded.header.xl_tot_len = SizeOfXLogRecord + 2 + (cursor - frame->payload);
	frame->decoded.main_data = (char *)frame->payload;
	frame->decoded.main_data_len = cursor - frame->payload;
	frame->decoded.max_block_id = -1;
	frame->decoded.lsn = part->physical_lower;
	frame->decoded.next_lsn = part->tail_end;
	frame->reader.record = &frame->decoded;
	frame->reader.system_identifier = source->claim.identity.system_identifier;
	frame->reader.cluster_expected_thread_id = source->claim.identity.origin_thread_id;
	frame->reader.seg.ws_tli = source->timeline;
	frame->reader.ReadRecPtr = frame->decoded.lsn;
	frame->reader.EndRecPtr = frame->decoded.next_lsn;
}

static RfSideOnlinePlanV1 *
relmap_episode_terminal_plan(uint32 p, bool complete_bind, bool seal)
{
	const ClusterWalSourceRef *source = &tx_inputs_typed.sources[p];
	const ClusterColdParticipantV1 *part = &tx_inputs_typed.participants[p];
	RfSideOnlinePlanV1 *plan = NULL; RfSideOnlinePlanRequestV1 request = {0};
	RfContributorStreamCutV1 cut = {0}; RfPageOnlineRecordIdentityV1 identity = {0};
	RfDetachedRecordPlanV1 record = {0}; RelmapEpisodeTerminal frame;
	cut.failed_thread = part->thread_id; cut.timeline_id = part->timeline;
	cut.origin_owner_incarnation = part->owner_incarnation;
	cut.scan_begin_inclusive = part->physical_lower; cut.scan_end_exclusive = part->tail_end;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	request.system_identifier = source->claim.identity.system_identifier;
	memcpy(request.storage_uuid, source->claim.identity.storage_uuid, 16);
	request.physical_cuts = &cut; request.participant_count = 1;
	request.redo_starts = &part->native_redo; request.purpose = RF_SIDE_PLAN_RECOVERY;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), 0);
	UT_ASSERT(rf_side_online_plan_bind_sources_v1(plan,
		complete_bind ? tx_inputs_typed.sources : source,
		complete_bind ? tx_inputs_typed.participant_count : 1));
	relmap_episode_terminal_frame(&frame, p);
	identity.record.system_identifier = request.system_identifier;
	memcpy(identity.record.storage_uuid, request.storage_uuid, 16);
	identity.record.origin_thread = cut.failed_thread; identity.record.timeline_id = cut.timeline_id;
	identity.record.read_rec_ptr = frame.reader.ReadRecPtr;
	identity.record.end_rec_ptr = frame.reader.EndRecPtr;
	identity.record.record_crc = frame.decoded.header.xl_crc;
	identity.record.rmid = RM_XACT_ID; identity.record.info = frame.decoded.header.xl_info;
	record.source_record = &frame.reader; record.preflight_complete = true;
	record.route.rmid = RM_XACT_ID; record.route.normalized_info = XLOG_XACT_COMMIT;
	record.route.legal_info_flags = XLOG_XACT_HAS_INFO;
	record.route.record_owner = RF_ROUTE_OWNER_SIDE_TYPED;
	record.route.block_policy = RF_ROUTE_BLOCKS_FORBIDDEN;
	record.route.codec_id = RF_ROUTE_CODEC_SIDE_STANDARD;
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity), 0);
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, source->claim.database_incarnation));
	if (seal) UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), 0);
	return plan;
}

static void
relmap_episode_terminal_finish(ClusterWalInputsV1 **inputs)
{
	for (uint32 p = 0; p < tx_inputs_typed.participant_count; p++)
		rf_side_online_plan_destroy_v1(&tx_inputs_typed.side_plans[p]);
	ctrc_set_finish(inputs); relmap_declared = false;
}

UT_TEST(test_relmap_episode_terminal_uses_original_generation_not_input_order)
{
	ClusterWalInputsV1 *inputs = NULL; unsigned calls;
	ClusterWalSourceRef source_swap; ClusterColdParticipantV1 part_swap;
	relmap_participant_fixture(&inputs, false);
	/* Original typed participants need not use the input census order. */
	cluster_wal_inputs_release_v1(&inputs);
	source_swap = tx_inputs_typed.sources[0]; tx_inputs_typed.sources[0] = tx_inputs_typed.sources[1];
	tx_inputs_typed.sources[1] = source_swap;
	part_swap = tx_inputs_typed.participants[0]; tx_inputs_typed.participants[0] = tx_inputs_typed.participants[1];
	tx_inputs_typed.participants[1] = part_swap;
	UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0); ctrc_ir_original_scope = inputs;
	for (uint32 p = 0; p < 2; p++) tx_inputs_typed.side_plans[p] = relmap_episode_terminal_plan(p, true, true);
	relmap_declared = true; UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	calls = ctrc_set_calls;
	for (uint32 i = 0; i < 2; i++) {
		ClusterRecoveryRelmapSourceV1 source = cluster_wal_inputs_relmap_source_v1(inputs, i);
		RelmapEpisodeTerminal frame;
		uint32 p = source.selected.source.claim.identity.origin_thread_id == tx_inputs_typed.sources[0].claim.identity.origin_thread_id ? 0 : 1;
		UT_ASSERT(p != i); relmap_episode_terminal_frame(&frame, p);
		UT_ASSERT_EQ(cluster_wal_inputs_relmap_terminal_count_v1(&source), 1);
		UT_ASSERT(cluster_wal_inputs_relmap_terminal_matches_v1(&source, &frame.reader));
	}
	UT_ASSERT_EQ(ctrc_set_calls, calls); UT_ASSERT_EQ(ctrc_set_held, 2);
	UT_ASSERT_EQ(relmap_failed, 0); UT_ASSERT(inputs_recovery_held);
	relmap_episode_terminal_finish(&inputs);
}

UT_TEST(test_relmap_episode_terminal_refuses_partial_unsealed_or_wrong_original_plan)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterWalInputsV1 *inputs = NULL; ClusterRecoveryRelmapSourceV1 source;
		relmap_participant_fixture(&inputs, false);
		tx_inputs_typed.side_plans[0] = relmap_episode_terminal_plan(fault == 3 ? 1 : 0, fault != 2, fault != 1);
		if (fault == 0) rf_side_online_plan_destroy_v1(&tx_inputs_typed.side_plans[0]);
		if (fault == 4) tx_inputs_typed.side_capture_detail[0] = 1;
		relmap_declared = true; UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
		source = cluster_wal_inputs_relmap_source_v1(inputs, 0);
		UT_ASSERT_EQ(cluster_wal_inputs_relmap_terminal_count_v1(&source), UINT32_MAX);
		UT_ASSERT_EQ(relmap_failed, 1); UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT(inputs_recovery_held);
		relmap_episode_terminal_finish(&inputs);
	}
}

UT_TEST(test_relmap_episode_terminal_wrong_native_bytes_revoke_without_cleanup)
{
	ClusterWalInputsV1 *inputs = NULL; ClusterRecoveryRelmapSourceV1 source;
	RelmapEpisodeTerminal frame;
	relmap_participant_fixture(&inputs, false);
	tx_inputs_typed.side_plans[0] = relmap_episode_terminal_plan(0, true, true);
	relmap_declared = true; UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	source = cluster_wal_inputs_relmap_source_v1(inputs, 0); relmap_episode_terminal_frame(&frame, 0);
	UT_ASSERT(cluster_wal_inputs_relmap_terminal_matches_v1(&source, &frame.reader));
	frame.payload[0] ^= 1;
	UT_ASSERT(!cluster_wal_inputs_relmap_terminal_matches_v1(&source, &frame.reader));
	UT_ASSERT_EQ(relmap_failed, 1); UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT(inputs_recovery_held);
	UT_ASSERT_EQ(rf_side_online_plan_native_terminal_count_v1(tx_inputs_typed.side_plans[0], &source.selected.source), 1);
	UT_ASSERT_EQ(cluster_wal_inputs_relmap_terminal_count_v1(&source), UINT32_MAX);
	relmap_episode_terminal_finish(&inputs);
}

UT_TEST(test_relmap_episode_terminal_preconditions_keep_original_episode)
{
	ClusterWalInputsV1 *inputs = NULL; ClusterRecoveryRelmapSourceV1 source, forged;
	RelmapEpisodeTerminal frame;
	relmap_participant_fixture(&inputs, false);
	tx_inputs_typed.side_plans[0] = relmap_episode_terminal_plan(0, true, true);
	relmap_declared = true; UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	source = cluster_wal_inputs_relmap_source_v1(inputs, 0); relmap_episode_terminal_frame(&frame, 0);
	forged = source; forged.work_generation++;
	UT_ASSERT_EQ(cluster_wal_inputs_relmap_terminal_count_v1(&forged), UINT32_MAX);
	for (unsigned fault = 0; fault < 2; fault++) {
		if (fault == 0) CritSectionCount = 1; else test_actual_cf = ShareLock;
		UT_ASSERT(!cluster_wal_inputs_relmap_terminal_matches_v1(&source, &frame.reader));
		UT_ASSERT_EQ(cluster_wal_inputs_relmap_terminal_count_v1(&source), UINT32_MAX);
		CritSectionCount = 0; test_actual_cf = NoLock;
	}
	UT_ASSERT_EQ(relmap_failed, 0);
	UT_ASSERT(cluster_wal_inputs_relmap_terminal_matches_v1(&source, &frame.reader));
	relmap_sibling->release_uncertain = true;
	UT_ASSERT_EQ(cluster_wal_inputs_relmap_terminal_count_v1(&source), UINT32_MAX);
	relmap_sibling->release_uncertain = false;
	UT_ASSERT(!cluster_wal_inputs_relmap_source_current_v1(&source));
	UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT(inputs_recovery_held);
	relmap_episode_terminal_finish(&inputs);
}

static bool
relmap_episode_terminal_error_current(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 index, ClusterWalInputsV1 *scope,
	const ClusterRecoverySerialRequest *request)
{
	(void)start; (void)work; (void)index; (void)scope; (void)request;
	ereport(ERROR, (errmsg("injected original terminal owner observation ERROR")));
	return false;
}

UT_TEST(test_relmap_episode_terminal_first_owner_error_is_sticky)
{
	for (unsigned match = 0; match < 2; match++) {
		ClusterWalInputsV1 *inputs = NULL; ClusterRecoveryRelmapSourceV1 source;
		RelmapEpisodeTerminal frame; volatile bool caught = false;
		relmap_participant_fixture(&inputs, false);
		tx_inputs_typed.side_plans[0] = relmap_episode_terminal_plan(0, true, true);
		relmap_declared = true; UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
		source = cluster_wal_inputs_relmap_source_v1(inputs, 0); relmap_episode_terminal_frame(&frame, 0);
		test_capture_error_level = true;
		ctrc_replay_current_override = relmap_episode_terminal_error_current;
		PG_TRY(); {
			if (match) (void)cluster_wal_inputs_relmap_terminal_matches_v1(&source, &frame.reader);
			else (void)cluster_wal_inputs_relmap_terminal_count_v1(&source);
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		ctrc_replay_current_override = ctrc_set_original_current;
		test_capture_error_level = false;
		UT_ASSERT(caught); UT_ASSERT_EQ(relmap_failed, 1);
		UT_ASSERT(!cluster_wal_inputs_relmap_source_current_v1(&source));
		UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT(inputs_recovery_held);
		UT_ASSERT_EQ(rf_side_online_plan_native_terminal_count_v1(tx_inputs_typed.side_plans[0], &source.selected.source), 1);
		relmap_episode_terminal_finish(&inputs);
	}
}
