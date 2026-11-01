/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_relmap_files.inc
 *     Actual Start/recheck/SIDE/pin/files composition, without completion.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_ctrc_relmap_files.inc
 * NOTES
 *     Source/TT census and native lock scheduling remain controlled leaves.
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#include "test_cluster_cold_startup_relmap_storage.h"
#include "cluster/cluster_side_online_plan.h"
#include "utils/relmapper.h"
#include "catalog/pg_tablespace_d.h"

static RfSideOnlinePlanV1 *relmap_files_side;
static ColdParticipant relmap_files_participants[2];
static uint32 relmap_files_canonical[2];
static unsigned relmap_files_read_at, relmap_files_fault;

/* The retained replay receipt is an input fixture; original SIDE capture,
 * retention transitions, two-file writer and all consumer bodies are real. */
static void
relmap_files_begin(void)
{
	ColdRelmapInput *entry;
	ClusterColdRelmapInputV1 *input;
	RfSideOnlinePlanRequestV1 request = {0};
	RfContributorStreamCutV1 cut = {0};
	RfPageOnlineRecordIdentityV1 identity = {0};
	RfDetachedRecordPlanV1 record = {0};
	XLogReaderState reader = {0};
	DecodedXLogRecord decoded = {0};
	xl_relmap_update header = {InvalidOid, GLOBALTABLESPACE_OID, sizeof(RelMapFile)};
	char payload[MinSizeOfRelmapUpdate + sizeof(RelMapFile)];
	XLogRecPtr redo = 0x800;

	observation_ready();
	typed.system_identifier = 91;
	/* Preserve the original C0 target/source and both native counter inputs.
	 * Supply its otherwise unused physical cut, plus the retained history
	 * identity, rather than substituting the recoverer's writer for it. */
	typed.sources[0].timeline = observed_writer.ref.timeline;
	typed.sources[1] = observed_writer.ref;
	typed.sources[1].claim.identity.origin_node_id = 2;
	typed.sources[1].claim.identity.origin_thread_id = 3;
	typed.sources[1].claim.identity.origin_owner_incarnation = typed.participants[1].owner_incarnation;
	typed.sources[1].claim.claim_sha256[0] = 42;
	typed.participants[0].timeline = typed.sources[0].timeline;
	typed.participants[0].physical_lower = typed.participants[0].native_redo = redo;
	typed.participants[0].tail_end = 0x900;
	memset(relmap_files_participants, 0, sizeof(relmap_files_participants));
	for (uint32 i = 0; i < typed.participant_count; ++i) {
		relmap_files_participants[i].cut = typed.participants[i];
		relmap_files_canonical[i] = i;
	}
	relmap_start_plan.participants = relmap_files_participants;
	relmap_start_plan.canonical = relmap_files_canonical;
	relmap_start_plan.participant_count = typed.participant_count;
	relmap_start_plan.memory_budget = 2 * 1024 * 1024;
	relmap_start_plan.relmap_count = 1;
	relmap_start_plan.relmap_inputs = relmap_start_entries;
	entry = &relmap_start_entries[0]; input = &entry->input;
	input->cut = typed.participants[0]; input->source = typed.sources[0];
	input->frame = (ClusterColdRecordFrameV1){redo, 0x900, 0x1234, 0,
		811, InvalidTransactionId, 0, 0, RM_RELMAP_ID, XLOG_RELMAP_UPDATE, false};
	input->staged.identity.system_identifier = typed.system_identifier;
	input->staged.identity.database_incarnation = input->source.claim.database_incarnation;
	input->staged.identity.shared = true;
	input->staged.committed_generation = 7; input->staged.pending_generation = 8;
	input->staged.pending_owner.node = input->source.claim.identity.origin_node_id;
	input->staged.pending_owner.top_full_xid = (UINT64CONST(7) << 32) | 811;
	input->staged.pending_owner.membership_epoch = observed_writer.epoch;
	input->staged.pending_owner.admitted_incarnation = input->cut.owner_incarnation;
	input->staged.pending_owner.boot_incarnation = owner_boot;
	input->staged.pending_owner.wal_end = input->frame.end_rec_ptr;
	test_cold_relmap_storage_begin(input);
	UT_ASSERT(test_cold_relmap_storage_stage(input, &entry->retention_snapshot));
	entry->has_retention_snapshot = true;
	entry->captured = entry->attempted = entry->applied = true;

	cut.failed_thread = input->cut.thread_id; cut.timeline_id = input->cut.timeline;
	cut.origin_owner_incarnation = input->cut.owner_incarnation;
	cut.scan_begin_inclusive = input->cut.physical_lower;
	cut.scan_end_exclusive = input->cut.tail_end; cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	request.system_identifier = typed.system_identifier;
	memcpy(request.storage_uuid, input->source.claim.identity.storage_uuid, 16);
	request.physical_cuts = &cut; request.participant_count = 1;
	request.redo_starts = &redo; request.purpose = RF_SIDE_PLAN_RECOVERY;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &relmap_files_side), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_bind_database_v1(relmap_files_side,
		input->source.claim.database_incarnation));
	UT_ASSERT(rf_side_online_plan_bind_sources_v1(relmap_files_side, typed.sources, typed.participant_count));
	memcpy(payload, &header, MinSizeOfRelmapUpdate);
	memcpy(payload + MinSizeOfRelmapUpdate, &input->staged.pending, sizeof(RelMapFile));
	reader.system_identifier = typed.system_identifier;
	reader.cluster_expected_thread_id = cut.failed_thread; reader.seg.ws_tli = cut.timeline_id;
	reader.ReadRecPtr = decoded.lsn = redo; reader.EndRecPtr = decoded.next_lsn = cut.scan_end_exclusive;
	reader.record = &decoded;
	decoded.header.xl_tot_len = SizeOfXLogRecord + 3 + sizeof(payload);
	decoded.header.xl_xid = 811; decoded.header.xl_crc = input->frame.record_crc;
	decoded.header.xl_rmid = RM_RELMAP_ID; decoded.header.xl_info = XLOG_RELMAP_UPDATE;
	decoded.max_block_id = -1; decoded.main_data = payload; decoded.main_data_len = sizeof(payload);
	identity.record.system_identifier = typed.system_identifier;
	memcpy(identity.record.storage_uuid, request.storage_uuid, 16);
	identity.record.origin_thread = cut.failed_thread; identity.record.timeline_id = cut.timeline_id;
	identity.record.read_rec_ptr = redo; identity.record.end_rec_ptr = cut.scan_end_exclusive;
	identity.record.record_crc = input->frame.record_crc;
	identity.record.rmid = RM_RELMAP_ID; identity.record.info = XLOG_RELMAP_UPDATE;
	record.source_record = &reader; record.preflight_complete = true;
	UT_ASSERT_EQ(rf_opcode_route_lookup_v1(RM_RELMAP_ID, XLOG_RELMAP_UPDATE,
		false, false, &record.route), RF_OPCODE_ROUTE_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_relmap_v1(relmap_files_side, &record, &identity, input),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(relmap_files_side), RF_PAGE_PROOF_DETAIL_OK);
	typed.side_plans[0] = relmap_files_side;
	UT_ASSERT(ClusterColdRecoveryRelmapDeclare(info.cluster_tx_start));
	relmap_files_active = true;
	relmap_files_read_at = test_cold_relmap_storage_stats().reads + 1;
}

static void
relmap_files_end(void)
{
	test_cold_relmap_storage_read_hook(NULL);
	/* Original Start/observation retires before borrowed SIDE and files. */
	reset();
	rf_side_online_plan_destroy_v1(&relmap_files_side);
	test_cold_relmap_storage_end();
}

static void
relmap_files_failed(TestColdRelmapStorageStats before, Size used)
{
	TestColdRelmapStorageStats after = test_cold_relmap_storage_stats();
	UT_ASSERT(after.pin_retained && after.may_be_staged);
	UT_ASSERT_EQ(after.writes, before.writes);
	UT_ASSERT_EQ(after.io_under_retention_lock, 0);
	UT_ASSERT_EQ(relmap_start_plan.memory_used, used);
	UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT(!window_region.checkpoint_owner.valid);
	UT_ASSERT(claim_held && info.cluster_cold_inputs == &typed);
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 0));
	UT_ASSERT_EQ(test_cold_relmap_storage_stats().reads, after.reads);
	UT_ASSERT_EQ(test_cold_relmap_storage_stats().writes, after.writes);
}

UT_TEST(test_ctrc_relmap_files_reads_real_pair_and_repeat_keeps_joint_pending)
{
	TestColdRelmapStorageStats before, after;
	bool observed;
	relmap_files_begin(); before = test_cold_relmap_storage_stats();
	observed = ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024);
	if (!observed) printf("# original Start refusal: %s\n",
		ClusterColdRecoveryCtrcObservationRefusal(info.cluster_tx_start));
	UT_ASSERT(observed);
	after = test_cold_relmap_storage_stats();
	UT_ASSERT(after.reads > before.reads && after.writes == before.writes);
	UT_ASSERT_EQ(relmap_observe_checks, 2);
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 0));
	UT_ASSERT(test_cold_relmap_storage_stats().reads > after.reads);
	UT_ASSERT_EQ(test_cold_relmap_storage_stats().writes, before.writes);
	UT_ASSERT_EQ(test_cold_relmap_storage_stats().io_under_retention_lock, 0);
	UT_ASSERT(cluster_recovery_ctrc_result_collect_v1(info.cluster_tx_start));
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_result.state, CLUSTER_RECOVERY_CTRC_RESULT_PENDING);
	UT_ASSERT(!cluster_recovery_checkpoint_handoff_v1(info.cluster_tx_start));
	UT_ASSERT(test_cold_relmap_storage_stats().pin_retained);
	relmap_files_end();
}

UT_TEST(test_ctrc_relmap_files_each_incomplete_copy_refuses_before_census)
{
	for (unsigned copy = 0; copy < 2; ++copy) {
		TestColdRelmapStorageStats before;
		relmap_files_begin(); test_cold_relmap_storage_damage_copy(copy);
		before = test_cold_relmap_storage_stats();
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		UT_ASSERT_EQ(observation_scope_begins, 0);
		relmap_files_failed(before, 0);
		relmap_files_end();
	}
}

UT_TEST(test_ctrc_relmap_files_preflights_original_side_pin_and_budget)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		TestColdRelmapStorageStats before;
		Size used;
		relmap_files_begin(); before = test_cold_relmap_storage_stats();
		if (fault == 0) typed.side_plans[0] = NULL;
		if (fault == 1) test_cold_relmap_storage_pin_drift(0);
		if (fault == 2) relmap_start_plan.memory_used = relmap_start_plan.memory_budget;
		used = relmap_start_plan.memory_used;
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		UT_ASSERT_EQ(test_cold_relmap_storage_stats().reads, before.reads);
		UT_ASSERT_EQ(observation_scope_begins, 0);
		relmap_files_failed(before, used);
		relmap_files_end();
	}
}

static void
relmap_files_late_damage(void)
{
	if (relmap_observe_checks == 2) test_cold_relmap_storage_damage_copy(1);
}

UT_TEST(test_ctrc_relmap_files_final_physical_check_can_revoke_completed_census)
{
	TestColdRelmapStorageStats before;
	relmap_files_begin(); before = test_cold_relmap_storage_stats();
	relmap_observe_step = relmap_files_late_damage;
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT(observation_steps >= 3);
	UT_ASSERT_EQ(observation_census_destroys, 1);
	UT_ASSERT_EQ(observation_scope_releases, 1);
	relmap_files_failed(before, 0);
	relmap_files_end();
}

static void
relmap_files_read_fault(unsigned reads)
{
	if (reads != relmap_files_read_at) return;
	if (relmap_files_fault == 0) test_cold_relmap_storage_pin_drift(0);
	else if (relmap_files_fault == 1) owner_boot++;
	else if (relmap_files_fault == 2) pg_re_throw();
	else {
		UT_ASSERT(!info.cluster_tx_start->ctrc_observation_complete);
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 0));
	}
}

UT_TEST(test_ctrc_relmap_files_read_time_pin_or_owner_drift_is_sticky)
{
	for (unsigned fault = 0; fault < 2; ++fault) {
		TestColdRelmapStorageStats before;
		relmap_files_begin(); before = test_cold_relmap_storage_stats();
		relmap_files_fault = fault;
		test_cold_relmap_storage_read_hook(relmap_files_read_fault);
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		test_cold_relmap_storage_read_hook(NULL);
		UT_ASSERT(test_cold_relmap_storage_stats().reads > before.reads);
		relmap_files_failed(before, 0);
		relmap_files_end();
	}
}

UT_TEST(test_ctrc_relmap_files_native_read_error_keeps_original_pin_and_plan)
{
	TestColdRelmapStorageStats before;
	volatile bool caught = false;
	relmap_files_begin(); before = test_cold_relmap_storage_stats();
	relmap_files_fault = 2;
	test_cold_relmap_storage_read_hook(relmap_files_read_fault);
	PG_TRY(); { (void)ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	test_cold_relmap_storage_read_hook(NULL);
	UT_ASSERT(caught);
	relmap_files_failed(before, 0);
	relmap_files_end();
}

UT_TEST(test_ctrc_relmap_files_real_io_withdraws_old_observation_and_refuses_reentry)
{
	relmap_files_begin();
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	relmap_files_fault = 3;
	relmap_files_read_at = test_cold_relmap_storage_stats().reads + 1;
	test_cold_relmap_storage_read_hook(relmap_files_read_fault);
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 0));
	test_cold_relmap_storage_read_hook(NULL);
	UT_ASSERT(info.cluster_tx_start->ctrc_observation_complete);
	UT_ASSERT_EQ(observation_scope_begins, 1);
	test_cold_relmap_storage_damage_copy(0);
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 0));
	UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT(test_cold_relmap_storage_stats().pin_retained);
	relmap_files_end();
}

UT_TEST(test_ctrc_relmap_files_cannot_waive_original_terminal_rejection)
{
	TestColdRelmapStorageStats before;
	relmap_files_begin(); before = test_cold_relmap_storage_stats();
	observation_physical[0].slot.flags |= TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT(test_cold_relmap_storage_stats().reads > before.reads);
	UT_ASSERT_EQ(observation_census_destroys, 1);
	UT_ASSERT_EQ(observation_scope_releases, 1);
	UT_ASSERT(!info.cluster_tx_start->ctrc_observation_complete);
	UT_ASSERT(test_cold_relmap_storage_stats().pin_retained);
	UT_ASSERT_EQ(test_cold_relmap_storage_stats().writes, before.writes);
	relmap_files_end();
}

static int
test_ctrc_relmap_files_main(void)
{
	UT_PLAN(8);
	UT_RUN(test_ctrc_relmap_files_reads_real_pair_and_repeat_keeps_joint_pending);
	UT_RUN(test_ctrc_relmap_files_each_incomplete_copy_refuses_before_census);
	UT_RUN(test_ctrc_relmap_files_preflights_original_side_pin_and_budget);
	UT_RUN(test_ctrc_relmap_files_final_physical_check_can_revoke_completed_census);
	UT_RUN(test_ctrc_relmap_files_read_time_pin_or_owner_drift_is_sticky);
	UT_RUN(test_ctrc_relmap_files_native_read_error_keeps_original_pin_and_plan);
	UT_RUN(test_ctrc_relmap_files_real_io_withdraws_old_observation_and_refuses_reentry);
	UT_RUN(test_ctrc_relmap_files_cannot_waive_original_terminal_rejection);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
