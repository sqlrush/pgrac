/*-------------------------------------------------------------------------
 * test_cluster_cold_relmap.c
 *     Original cold planner/sequencer and real dual-copy relmap staging.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_relmap.c
 * NOTES
 *     Source authority and PAGE application are controlled boundaries.
 *     Planning, ordering, retention pins and dual-copy file I/O are real.
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
int storage_fixture_main(void);
#define main storage_fixture_main
#include "test_cluster_relmap_v2_storage.c"
#undef main
#include "access/xlogreader.h"
#include "cluster/cluster_cold_relmap.h"
#include "cluster/cluster_cold_recovery_plan_internal.h"
#include "utils/relmapper.h"

volatile uint32 InterruptHoldoffCount;

static ClusterColdTypedV1 replay;
static ClusterColdRelmapInputV1 declared;
static ClusterColdRecordV1 records[2][2];
static ClusterColdComponentV1 pages[2];
static unsigned next_record[2], current_record[2], page_observations;
static bool original_source_current;
static unsigned source_checks, source_error_at;
static XLogReaderState map_reader;
static DecodedXLogRecord map_decoded;
static char map_payload[MinSizeOfRelmapUpdate + sizeof(RelMapFile)];

bool
cluster_cold_typed_sources_current_v1(ClusterColdTypedV1 *typed,
	ClusterRecoveryFencePlan *fence)
{
	if (++source_checks == source_error_at)
		siglongjmp(*PG_exception_stack, 1);
	return typed == &replay && fence == (ClusterRecoveryFencePlan *)&replay
		&& original_source_current && typed->refusal == CLUSTER_COLD_OK;
}

static RfPageVersionV1
cold_map_version(uint64 token)
{
	RfPageVersionV1 v = { 0 };
	memset(v.segment_incarnation, 7, sizeof(v.segment_incarnation));
	v.mutation_token = token;
	return v;
}

static bool
cold_map_observe(void *arg, const RfPageIdentityV1 *page, ClusterColdDataV1 *out)
{
	(void)arg;
	(void)page;
	memset(out, 0, sizeof(*out));
	out->kind = CLUSTER_COLD_DATA_PRESENT;
	out->flags = CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED;
	out->version = cold_map_version(10);
	return true;
}

static void
cold_map_setup(void)
{
	xl_relmap_update *wal = (xl_relmap_update *)map_payload;
	setup(&identity);
	/* The original namespace owner retains X for this entire replay. */
	relmap_x_held = true;
	memset(&replay, 0, sizeof(replay));
	memset(&declared, 0, sizeof(declared));
	memset(records, 0, sizeof(records));
	memset(pages, 0, sizeof(pages));
	memset(next_record, 0, sizeof(next_record));
	page_observations = 0;
	original_source_current = true;
	source_checks = source_error_at = 0;
	replay.system_identifier = identity.system_identifier;
	replay.participant_count = replay.replay_count = 2;
	replay.own_participant = 0;
	replay.participants[0] = (ClusterColdParticipantV1){4, 0, 1, 101, 800, 800, 909};
	replay.participants[1] = (ClusterColdParticipantV1){5, 0, 1, 102, 100, 100, 250};
	for (unsigned i = 0; i < 2; ++i) {
		ClusterWalSourceRef *source = &replay.sources[i];
		source->timeline = 1;
		source->claim.database_incarnation = identity.database_incarnation;
		source->claim.max_config_generation = 1;
		source->claim.identity.system_identifier = identity.system_identifier;
		source->claim.identity.origin_thread_id = 4 + i;
		source->claim.identity.origin_node_id = 3 + i;
		source->claim.identity.origin_owner_incarnation = 101 + i;
		memset(source->claim.identity.storage_uuid, 3, 16);
		memset(source->claim.claim_sha256, 4 + i, 32);
	}
	declared.participant = 0;
	declared.cut = replay.participants[0];
	declared.source = replay.sources[0];
	declared.frame = (ClusterColdRecordFrameV1){800, 909, 77, 0, 0x123, 0, 0, 0,
		RM_RELMAP_ID, XLOG_RELMAP_UPDATE, false};
	declared.staged.identity = identity;
	declared.staged.committed_generation = 7;
	declared.staged.committed = map_image(40000);
	declared.staged.pending_generation = 8;
	declared.staged.pending_owner = owner;
	declared.staged.pending = map_image(40001);
	declared.retention = &writer_retention;
	declared.retention_lock = &retention_lock;
	memset(&map_reader, 0, sizeof(map_reader));
	memset(&map_decoded, 0, sizeof(map_decoded));
	memset(map_payload, 0, sizeof(map_payload));
	wal->dbid = InvalidOid;
	wal->tsid = GLOBALTABLESPACE_OID;
	wal->nbytes = sizeof(RelMapFile);
	memcpy(wal->data, &declared.staged.pending, sizeof(RelMapFile));
	map_reader.ReadRecPtr = 800;
	map_reader.EndRecPtr = 909;
	map_reader.record = &map_decoded;
	map_decoded.header.xl_rmid = RM_RELMAP_ID;
	map_decoded.header.xl_info = XLOG_RELMAP_UPDATE;
	map_decoded.header.xl_xid = 0x123;
	map_decoded.header.xl_crc = 77;
	map_decoded.max_block_id = -1;
	map_decoded.main_data = map_payload;
	map_decoded.main_data_len = sizeof(map_payload);
	records[0][0].read_rec_ptr = 800;
	records[0][0].end_rec_ptr = 909;
	records[0][0].record_crc = 77;
	records[0][0].source_xid = 0x123;
	records[0][0].scn = 20;
	records[0][0].rmid = RM_RELMAP_ID;
	records[0][0].info = XLOG_RELMAP_UPDATE;
	records[0][0].record_flags = CLUSTER_COLD_RECORD_STRUCTURAL;
	records[0][0].relmap_image = &declared.staged.pending;
	records[0][0].relmap_dbid = InvalidOid;
	records[0][0].relmap_tsid = GLOBALTABLESPACE_OID;
	for (unsigned i = 0; i < 2; ++i) {
		ClusterColdRecordV1 *r = &records[1][i];
		ClusterColdComponentV1 *p = &pages[i];
		r->read_rec_ptr = 100 + i * 100;
		r->end_rec_ptr = 150 + i * 100;
		r->prev_rec_ptr = i == 0 ? 0 : 100;
		r->scn = 10 + i * 20;
		r->rmid = RM_HEAP_ID;
		r->component_count = 1;
		r->components = p;
		p->page.system_identifier = identity.system_identifier;
		memset(p->page.storage_uuid, 3, 16);
		p->page.locator = (RelFileLocator){1663, 5, 500};
		p->page_class = RF_PAGE_CLASS_ORDINARY;
		p->before_kind = RF_PAGE_STATE_PRESENT;
		p->result_kind = RF_PAGE_STATE_PRESENT;
		p->before = cold_map_version(10 + i);
		p->result = cold_map_version(11 + i);
	}
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(replay.participants, 2, 4194304, &replay.plan),
		CLUSTER_COLD_OK);
}

static ClusterColdDetailV1
cold_map_seal(bool declaration, bool include_map)
{
	ClusterColdDiagV1 diag;
	ClusterColdDetailV1 detail;
	if (declaration) {
		detail = cluster_cold_plan_relmap_declare_v1(replay.plan, &declared, 1);
		if (detail != CLUSTER_COLD_OK) return detail;
	}
	if (include_map) {
		detail = cluster_cold_plan_feed_v1(replay.plan, 0, &records[0][0]);
		if (detail != CLUSTER_COLD_OK) return detail;
	}
	for (unsigned i = 0; i < 2; ++i) {
		detail = cluster_cold_plan_feed_v1(replay.plan, 1, &records[1][i]);
		if (detail != CLUSTER_COLD_OK) return detail;
	}
	return cluster_cold_plan_seal_v1(replay.plan, cold_map_observe, NULL, &diag);
}

static bool
cold_map_next(void *arg, uint32 p, ClusterColdReplayRecordV1 *out)
{
	ClusterColdRecordV1 *r;
	(void)arg;
	if (next_record[p] == (p == 0 ? 1 : 2)) return false;
	current_record[p] = next_record[p]++;
	r = &records[p][current_record[p]];
	*out = (ClusterColdReplayRecordV1){r->read_rec_ptr, r->end_rec_ptr,
		r->record_crc, r->rmid, r->info, {0, 0}};
	return true;
}

static bool
cold_map_unscheduled(void *arg, uint32 p)
{
	(void)arg; (void)p;
	return false;
}

static bool
cold_map_scheduled(void *arg, uint32 p, const ClusterColdStepV1 *step,
	ClusterColdPageActionV1 action)
{
	ClusterRelmapV2View view;
	(void)arg;
	if (p == 0) {
		if (action != CLUSTER_COLD_RELMAP_STEP || step->step_kind != CLUSTER_COLD_STEP_RELMAP)
			return false;
		return cluster_cold_typed_relmap_record_v1(&replay,
			(ClusterRecoveryFencePlan *)&replay, step, &map_reader);
	}
	if (action != CLUSTER_COLD_PAGE_APPLY)
		return false;
	if (cluster_relmap_authority_v2_read(&identity, &view) != CLUSTER_RELMAP_V2_PAIR_VALID
		|| view.has_pending != (page_observations == 1)) return false;
	if (view.has_pending && memcmp(&view.pending, &declared.staged.pending, sizeof(view.pending)))
		return false;
	++page_observations;
	return true;
}

static ClusterColdReplayDetailV1
cold_map_run(void)
{
	const ClusterColdReplayOpsV1 ops = {cold_map_next, cold_map_unscheduled, cold_map_scheduled};
	ClusterColdReplayResultV1 result;
	return cluster_cold_replay_run_v1(replay.plan, replay.participants, 2, 0, &ops, NULL, &result);
}

static void
cold_map_end(void)
{
	cluster_cold_plan_destroy_v1(&replay.plan);
	cleanup();
}

UT_TEST(test_relmap_runs_between_other_source_pages_and_duplicate_does_not_write)
{
	ClusterColdStepV1 step;
	unsigned staged_writes;
	cold_map_setup();
	UT_ASSERT_EQ(cold_map_seal(true, true), CLUSTER_COLD_OK);
	UT_ASSERT(!cluster_cold_typed_relmap_ready_v1(&replay, (ClusterRecoveryFencePlan *)&replay));
	UT_ASSERT_EQ(cold_map_run(), CLUSTER_COLD_REPLAY_OK);
	UT_ASSERT_EQ(page_observations, 2);
	UT_ASSERT(cluster_cold_typed_relmap_ready_v1(&replay, (ClusterRecoveryFencePlan *)&replay));
	UT_ASSERT(cluster_cold_plan_step_v1(replay.plan, 1, &step));
	staged_writes = writes;
	UT_ASSERT(cluster_cold_typed_relmap_record_v1(&replay,
		(ClusterRecoveryFencePlan *)&replay, &step, &map_reader));
	UT_ASSERT_EQ(writes, staged_writes);
	UT_ASSERT(writer_retention.pins[0].may_be_staged);
	cold_map_end();
}

UT_TEST(test_relmap_without_declared_participant_keeps_structural_refusal)
{
	cold_map_setup();
	UT_ASSERT_EQ(cold_map_seal(false, true), CLUSTER_COLD_STRUCTURAL_UNSUPPORTED);
	UT_ASSERT_EQ(writes, 0);
	cold_map_end();
}

UT_TEST(test_relmap_missing_capture_never_seals_or_writes)
{
	cold_map_setup();
	/* Cover the complete source cut with another record: the missing
	 * declaration must refuse even when cursor/tail completeness passes. */
	records[0][0].rmid = RM_XLOG_ID;
	records[0][0].record_flags = CLUSTER_COLD_RECORD_FOREIGN_CONTROL;
	records[0][0].relmap_image = NULL;
	UT_ASSERT_EQ(cold_map_seal(true, true), CLUSTER_COLD_STRUCTURAL_UNSUPPORTED);
	UT_ASSERT_EQ(writes, 0);
	cold_map_end();
}

UT_TEST(test_relmap_wrong_generation_or_source_declaration_refuses_before_io)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		cold_map_setup();
		if (fault == 0) ++declared.cut.owner_incarnation;
		if (fault == 1) ++declared.staged.pending_generation;
		if (fault == 2) ++declared.source.claim.identity.origin_owner_incarnation;
		UT_ASSERT(cold_map_seal(true, true) != CLUSTER_COLD_OK);
		UT_ASSERT_EQ(writes, 0);
		cold_map_end();
	}
}

UT_TEST(test_relmap_partial_pair_write_retains_pin_and_refuses_retry)
{
	ClusterColdStepV1 step;
	unsigned failed_writes;
	cold_map_setup();
	UT_ASSERT_EQ(cold_map_seal(true, true), CLUSTER_COLD_OK);
	fail_rename = 2;
	UT_ASSERT_EQ(cold_map_run(), CLUSTER_COLD_REPLAY_CALLBACK);
	UT_ASSERT(writer_retention.pins[0].occupied && writer_retention.pins[0].may_be_staged);
	failed_writes = writes;
	fail_rename = 0;
	UT_ASSERT(cluster_cold_plan_step_v1(replay.plan, 1, &step));
	UT_ASSERT(!cluster_cold_typed_relmap_record_v1(&replay,
		(ClusterRecoveryFencePlan *)&replay, &step, &map_reader));
	UT_ASSERT(!cluster_cold_typed_relmap_ready_v1(&replay, (ClusterRecoveryFencePlan *)&replay));
	UT_ASSERT_EQ(writes, failed_writes);
	cold_map_end();
}

UT_TEST(test_relmap_source_drift_or_missing_original_pin_cannot_stage)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		cold_map_setup();
		UT_ASSERT_EQ(cold_map_seal(true, true), CLUSTER_COLD_OK);
		if (fault == 0) ++replay.sources[0].claim.max_config_generation;
		if (fault == 1) writer_retention.pins[0].occupied = false;
		if (fault == 2) original_source_current = false;
		if (fault == 3) relmap_x_held = false;
		UT_ASSERT_EQ(cold_map_run(), CLUSTER_COLD_REPLAY_CALLBACK);
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT(!cluster_cold_typed_relmap_ready_v1(&replay, (ClusterRecoveryFencePlan *)&replay));
		cold_map_end();
	}
}

UT_TEST(test_relmap_complete_native_image_is_compared_before_stage)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		cold_map_setup();
		UT_ASSERT_EQ(cold_map_seal(true, true), CLUSTER_COLD_OK);
		if (fault == 0) map_payload[MinSizeOfRelmapUpdate + 500] ^= 1;
		if (fault == 1) ++map_decoded.header.xl_crc;
		if (fault == 2) ++map_decoded.header.xl_xid;
		if (fault == 3) ++map_reader.EndRecPtr;
		UT_ASSERT_EQ(cold_map_run(), CLUSTER_COLD_REPLAY_CALLBACK);
		UT_ASSERT_EQ(writes, 0);
		cold_map_end();
	}
}

UT_TEST(test_relmap_late_pair_drift_blocks_completion_without_rewrite)
{
	unsigned staged_writes;
	unsigned char saved[CLUSTER_RELMAP_V2_FILE_SIZE], changed[CLUSTER_RELMAP_V2_FILE_SIZE];
	cold_map_setup();
	UT_ASSERT_EQ(cold_map_seal(true, true), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cold_map_run(), CLUSTER_COLD_REPLAY_OK);
	staged_writes = writes;
	read_copy(1, saved);
	memcpy(changed, saved, sizeof(changed));
	changed[CLUSTER_RELMAP_V2_PENDING_OFFSET + 500] ^= 1;
	write_copy(1, changed, sizeof(changed));
	UT_ASSERT(!cluster_cold_typed_relmap_ready_v1(&replay, (ClusterRecoveryFencePlan *)&replay));
	write_copy(1, saved, sizeof(saved));
	UT_ASSERT(!cluster_cold_typed_relmap_ready_v1(&replay, (ClusterRecoveryFencePlan *)&replay));
	UT_ASSERT_EQ(writes, staged_writes);
	cold_map_end();
}

UT_TEST(test_relmap_first_and_late_owner_errors_are_sticky)
{
	for (unsigned fault = 1; fault <= 3; fault += 2) {
		volatile bool caught = false;
		unsigned failed_writes;
		ClusterColdStepV1 step;
		cold_map_setup();
		UT_ASSERT_EQ(cold_map_seal(true, true), CLUSTER_COLD_OK);
		source_error_at = fault;
		PG_TRY(); { (void)cold_map_run(); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT(caught && writer_retention.pins[0].occupied);
		UT_ASSERT_EQ(InterruptHoldoffCount, 0);
		UT_ASSERT_EQ(retention_acquires, retention_releases);
		failed_writes = writes;
		source_error_at = 0;
		UT_ASSERT(cluster_cold_plan_step_v1(replay.plan, 1, &step));
		UT_ASSERT(!cluster_cold_typed_relmap_record_v1(&replay,
			(ClusterRecoveryFencePlan *)&replay, &step, &map_reader));
		UT_ASSERT(!cluster_cold_typed_relmap_ready_v1(&replay, (ClusterRecoveryFencePlan *)&replay));
		UT_ASSERT_EQ(writes, failed_writes);
		if (fault == 3) UT_ASSERT(writer_retention.pins[0].may_be_staged && writes == 2);
		cold_map_end();
	}
}

UT_TEST(test_relmap_declaration_is_before_feed_once_and_budgeted)
{
	ClusterColdPlanV1 *small = NULL;
	Size bytes;
	cold_map_setup();
	bytes = replay.plan->memory_used;
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(replay.participants, 2, bytes, &small), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_relmap_declare_v1(small, &declared, 1), CLUSTER_COLD_CAPACITY);
	cluster_cold_plan_destroy_v1(&small);
	UT_ASSERT_EQ(cluster_cold_plan_relmap_declare_v1(replay.plan, &declared, 1), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_relmap_declare_v1(replay.plan, &declared, 1), CLUSTER_COLD_STATE);
	UT_ASSERT_EQ(cluster_cold_plan_feed_v1(replay.plan, 1, &records[1][0]), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_relmap_declare_v1(replay.plan, &declared, 1), CLUSTER_COLD_STATE);
	cold_map_end();
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_relmap_runs_between_other_source_pages_and_duplicate_does_not_write);
	UT_RUN(test_relmap_without_declared_participant_keeps_structural_refusal);
	UT_RUN(test_relmap_missing_capture_never_seals_or_writes);
	UT_RUN(test_relmap_wrong_generation_or_source_declaration_refuses_before_io);
	UT_RUN(test_relmap_partial_pair_write_retains_pin_and_refuses_retry);
	UT_RUN(test_relmap_source_drift_or_missing_original_pin_cannot_stage);
	UT_RUN(test_relmap_complete_native_image_is_compared_before_stage);
	UT_RUN(test_relmap_late_pair_drift_blocks_completion_without_rewrite);
	UT_RUN(test_relmap_first_and_late_owner_errors_are_sticky);
	UT_RUN(test_relmap_declaration_is_before_feed_once_and_budgeted);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
