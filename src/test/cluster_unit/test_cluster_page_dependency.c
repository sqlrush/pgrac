/*-------------------------------------------------------------------------
 * test_cluster_page_dependency.c
 *    Closed-input dependency planning with real WAL image/delta codecs.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xloginsert.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_block_apply.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_page_online_plan.h"
#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_thread_recovery_authority.h"
#include "cluster/cluster_thread_recovery_fabric.h"
#include "cluster/cluster_wal_tail.h"
#include "storage/bufpage.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_enabled = true, cluster_shared_config = true;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;
int cluster_node_id;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# unexpected Assert %s %s:%d\n", condition, file, line);
	abort();
}

/* The tested FPI/GENERIC routes use real product codecs and xlogreader.
 * Entering the unrelated heap route is a fixture failure. */
ClusterBlkApplyResult
cluster_block_apply_heap(XLogReaderState *record, uint8 block_id, char *page)
{
	abort();
}

typedef struct RecordFixture {
	XLogReaderState reader;
	PGAlignedBlock image;
	char delta[2 * sizeof(OffsetNumber) + 1];
	char error[1024];
	union {
		DecodedXLogRecord record;
		char bytes[sizeof(DecodedXLogRecord) + sizeof(DecodedBkpBlock)];
	} decoded;
} RecordFixture;

static void
record_init(RecordFixture *fixture, uint64 before, uint64 result, bool image,
			OffsetNumber offset, uint8 value)
{
	DecodedXLogRecord *record;
	DecodedBkpBlock *block;
	RfPageVersionEdgeEntryV1 *edge;
	OffsetNumber length = 1;

	memset(fixture, 0, sizeof(*fixture));
	record = &fixture->decoded.record;
	fixture->reader.record = record;
	fixture->reader.ReadRecPtr = record->lsn = 0x100;
	fixture->reader.EndRecPtr = record->next_lsn = 0x200;
	fixture->reader.system_identifier = 99;
	fixture->reader.errormsg_buf = fixture->error;
	record->header.xl_rmid = image ? RM_XLOG_ID : RM_GENERIC_ID;
	record->header.xl_info = image ? XLOG_FPI : 0;
	record->header.xl_scn = 77;
	record->header.xl_crc = (pg_crc32c)result;
	record->max_block_id = 0;
	record->has_page_version_edge = true;
	record->page_version_edge.entry_count = 1;
	record->page_version_edge.result_token = result;
	block = &record->blocks[0];
	block->in_use = true;
	block->rlocator = (RelFileLocator){1, 2, 10};
	block->forknum = MAIN_FORKNUM;
	block->blkno = 4;
	block->has_image = block->apply_image = image;
	if (image) {
		PageInit(fixture->image.data, BLCKSZ, 0);
		((PageHeader)fixture->image.data)->pd_upper = BLCKSZ - 8;
		((PageHeader)fixture->image.data)->pd_block_scn = result;
		memset(fixture->image.data + BLCKSZ - 8, value, 8);
		block->bkp_image = fixture->image.data;
		block->bimg_len = BLCKSZ;
		block->bimg_info = BKPIMAGE_APPLY;
	} else {
		memcpy(fixture->delta, &offset, sizeof(offset));
		memcpy(fixture->delta + sizeof(offset), &length, sizeof(length));
		fixture->delta[sizeof(offset) + sizeof(length)] = value;
		block->data = fixture->delta;
		block->data_len = sizeof(fixture->delta);
		block->has_data = true;
	}
	edge = &record->page_version_edge.entries[0];
	edge->page_class = RF_PAGE_CLASS_ORDINARY;
	edge->before_kind = edge->result_kind = RF_PAGE_STATE_PRESENT;
	memset(edge->before.segment_incarnation, 7, 16);
	memset(edge->result_incarnation, 7, 16);
	edge->before.mutation_token = before;
	if (image)
		edge->edge_flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
}

/* Only the authority/root I/O and the empty SIDE lane are fixtures below.
 * The production scanner, fabric barrier, detached PAGE decoder, dependency
 * queue and native image/delta codecs execute together. */
static char empty_side_plan, source_pin;
static unsigned source_visits;
static int source_failure;
/* Independent original checkpoints; no cross-thread LSN ordering. */
static unsigned source_completed;

ClusterThreadRecoveryAuthorityResultV1
cluster_thread_recovery_authority_revalidate_nowait_v1(
	const ClusterThreadRecoveryAuthorityV1 *authority)
{
	return authority != NULL && authority->duty != NULL
		? CLUSTER_THREAD_AUTHORITY_OK : CLUSTER_THREAD_AUTHORITY_INVALID;
}

bool
cluster_thread_recovery_authority_covers_window_v1(
	const ClusterThreadRecoveryAuthorityV1 *authority, uint16 thread, XLogRecPtr begin,
	XLogRecPtr end)
{
	return authority != NULL && authority->duty->origin_thread_id == thread
		&& begin == 0x100 && end == 0x200;
}

XLogReaderState *
cluster_thread_wal_reader_make(uint16 thread, void **private_out)
{
	abort();
}

void
cluster_thread_wal_reader_free(XLogReaderState *reader, void *private_state)
{
	abort();
}

ClusterControlRootResult
cluster_control_root_recovery_source_v1(const ClusterControlRootSnapshot *root,
										const ClusterControlRootReadToken *token,
										ClusterWalSourceRef *out, XLogRecPtr *native_redo)
{
	memset(out, 0, sizeof(*out));
	out->claim.identity = root->identity;
	out->claim.database_incarnation = 42;
	out->claim.max_config_generation = 5;
	out->claim.claim_sha256[0] = root->identity.origin_thread_id;
	out->timeline = root->checkpoint_tli;
	*native_redo = source_completed & (1U << (root->identity.origin_thread_id - 1)) ? 0x200 : 0x100;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_control_root_recovery_visit(const ClusterControlRootSnapshot *root,
	const ClusterControlRootReadToken *token, ClusterWalRecordVisitor visitor, void *arg,
	ClusterWalTailObservation *out)
{
	RecordFixture fixture;
	uint16 thread = root->identity.origin_thread_id;

	source_visits++;
	UT_ASSERT_EQ(root->checkpoint_lower_lsn, 0x100);
	UT_ASSERT_EQ(root->validated_tail_lsn_exclusive, 0x200);
	UT_ASSERT_EQ(token->origin_thread_id, thread);
	if (source_failure == 1 && thread == 2)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (thread == 1)
		record_init(&fixture, 60, 7, false, BLCKSZ - 1, 0xc3);
	else if (thread == 2)
		record_init(&fixture, source_failure == 2 ? 901 : 900, 60, false, BLCKSZ - 2, 0xb2);
	else
		record_init(&fixture, 10, 900, true, 0, 0xa1);
	if (source_failure >= 3 && source_failure <= 5) {
		RfPageVersionEdgeEntryV1 *edge;
		uint8 encoded[XLR_PAGE_VERSION_EDGE_HEADER_SIZE + XLR_PAGE_VERSION_EDGE_ENTRY_SIZE];
		Size length;

		record_init(&fixture, 10, 900, true, 0, 0xa1);
		edge = &fixture.decoded.record.page_version_edge.entries[0];
		memset(edge, 0, sizeof(*edge));
		edge->page_class = source_failure == 3	 ? RF_PAGE_CLASS_ROUTED_HEADER
						   : source_failure == 4 ? RF_PAGE_CLASS_ROUTED_SIDE
												 : RF_PAGE_CLASS_ROUTED_SPACE;
		edge->before_kind = edge->result_kind = RF_PAGE_STATE_ROUTED;
		UT_ASSERT(XLogEncodePageVersionEdgeV1(encoded, sizeof(encoded), 900, edge, 1, &length));
		if (source_failure == 5)
			fixture.decoded.record.blocks[0].forknum = SPACE_FORKNUM;
	}
	fixture.reader.seg.ws_tli = root->checkpoint_tli;
	if (!visitor(&fixture.reader, arg))
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	memset(&fixture, 0xdd, sizeof(fixture));
	memset(out, 0, sizeof(*out));
	out->records = 1;
	out->complete_end = 0x200;
	out->database_incarnation = 42;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

RfPageProofDetailV1
rf_side_online_plan_create_v1(const RfSideOnlinePlanRequestV1 *request,
	RfSideOnlinePlanV1 **out)
{
	UT_ASSERT_EQ(request->participant_count, 3);
	*out = (RfSideOnlinePlanV1 *)&empty_side_plan;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_side_online_plan_feed_record_v1(RfSideOnlinePlanV1 *plan,
	const RfDetachedRecordPlanV1 *record, const RfPageOnlineRecordIdentityV1 *identity)
{
	UT_ASSERT(plan == (RfSideOnlinePlanV1 *)&empty_side_plan);
	UT_ASSERT_EQ(record->route.record_owner, RF_ROUTE_OWNER_PAGE_CODEC);
	UT_ASSERT_EQ(identity->record.origin_thread, source_visits);
	return RF_PAGE_PROOF_DETAIL_OK;
}

bool
rf_side_online_plan_bind_database_v1(RfSideOnlinePlanV1 *plan, uint64 database_incarnation)
{
	return plan == (RfSideOnlinePlanV1 *)&empty_side_plan && database_incarnation == 42;
}

RfPageProofDetailV1
rf_side_online_plan_seal_v1(RfSideOnlinePlanV1 *plan)
{
	UT_ASSERT(plan == (RfSideOnlinePlanV1 *)&empty_side_plan);
	return RF_PAGE_PROOF_DETAIL_OK;
}

void
rf_side_online_plan_destroy_v1(RfSideOnlinePlanV1 **plan)
{
	*plan = NULL;
}

/* This fixture exercises PAGE planning, including refusal of every routed
 * SIDE component. The separate streaming census is never entered; invoking
 * it here must fail the test, not invent a successful SIDE observation. */
RfPageProofDetailV1
rf_side_record_census_v1(const RfDetachedRecordPlanV1 *record pg_attribute_unused(),
						 const RfPageOnlineRecordIdentityV1 *identity pg_attribute_unused(),
						 const RfContributorStreamCutV1 *cut pg_attribute_unused(),
						 uint64 incarnation pg_attribute_unused(),
						 RfSideCensusSpaceVisitorV1 visit pg_attribute_unused(),
						 void *arg pg_attribute_unused(), RfSideContributionOwnersV1 *out pg_attribute_unused())
{
	abort();
}

static RfPageProofDetailV1
scan_three_roots(ClusterThreadRecoveryFabricPlanV1 **out, uint64 *records)
{
	ClusterThreadRecoveryAuthorityV1 authorities[3] = { 0 };
	ClusterRecoveryDutyKey duties[3] = { 0 };
	ClusterControlRootSnapshot roots[3] = { 0 };
	ClusterControlRootReadToken tokens[3] = { 0 };
	unsigned i;

	source_visits = 0;
	for (i = 0; i < 3; i++) {
		duties[i].system_identifier = 99;
		memset(duties[i].storage_uuid, 3, 16);
		duties[i].origin_thread_id = i + 1;
		duties[i].origin_node_id = i;
		duties[i].authority_uuid[0] = 1;
		duties[i].thread_claim_created_at = 10;
		duties[i].origin_owner_incarnation = 11;
		duties[i].root_lineage_seq = 12;
		roots[i].identity = duties[i];
		roots[i].checkpoint_tli = roots[i].tail_tli = i + 1;
		roots[i].checkpoint_lower_lsn = 0x100;
		roots[i].validated_tail_lsn_exclusive = 0x200;
		tokens[i].origin_thread_id = i + 1;
		authorities[i].duty = &duties[i];
		authorities[i].root_snapshot = &roots[i];
		authorities[i].root_token = &tokens[i];
		authorities[i].retention_pin = (ClusterWalRetentionPin *)&source_pin;
	}
	return cluster_thread_recovery_fabric_scan_roots_v1(authorities, 3, source_failure == 5, out,
														records);
}

UT_TEST(test_original_root_scanner_resolves_reverse_three_origin_native_page_chain)
{
	ClusterThreadRecoveryFabricPlanV1 *plan = NULL;
	RfPageOnlineTargetViewV1 view;
	uint64 records = 0;

	source_failure = 0;
	cluster_node_id = 4;
	cluster_recmerge_window_active = cluster_recmerge_apply_foreign = true;
	cluster_recmerge_window_scn = 12345;
	cluster_recmerge_window_own_lsn = 0x77;
	UT_ASSERT_EQ(scan_three_roots(&plan, &records), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(records, 3);
	UT_ASSERT_EQ(source_visits, 3);
	for (int i = 0; i < 3; i++) {
		ClusterWalSourceRef source = { 0 };
		UT_ASSERT(rf_page_online_plan_source_v1(cluster_thread_recovery_fabric_page_plan_v1(plan),
												i, &source));
		UT_ASSERT_EQ(source.claim.identity.origin_thread_id, i + 1);
		UT_ASSERT_EQ(source.claim.claim_sha256[0], i + 1);
		UT_ASSERT_EQ(source.claim.database_incarnation, 42);
		UT_ASSERT_EQ(source.claim.max_config_generation, 5);
		UT_ASSERT_EQ(source.timeline, i + 1);
	}
	UT_ASSERT(rf_page_online_plan_target_v1(cluster_thread_recovery_fabric_page_plan_v1(plan), 0, &view));
	if (rf_page_online_plan_target_v1(cluster_thread_recovery_fabric_page_plan_v1(plan), 0, &view)) {
		UT_ASSERT_EQ(view.expected_before.mutation_token, 10);
		UT_ASSERT_EQ(view.expected_result.mutation_token, 7);
		UT_ASSERT_EQ(view.contributors->edge_count, 3);
		UT_ASSERT_EQ((uint8)view.canonical_page[BLCKSZ - 3], 0xa1);
		UT_ASSERT_EQ((uint8)view.canonical_page[BLCKSZ - 2], 0xb2);
		UT_ASSERT_EQ((uint8)view.canonical_page[BLCKSZ - 1], 0xc3);
		{
			int origin = -1;
			UT_ASSERT_EQ(PageGetLSN((Page)view.canonical_page), 0x200);
			UT_ASSERT(PageGetLSNOrigin((Page)view.canonical_page, &origin));
			UT_ASSERT_EQ(origin, 0); /* Final edge is from original thread 1. */
		}
	}
	cluster_thread_recovery_fabric_plan_destroy_v1(&plan);
	cluster_node_id = 0;
	cluster_recmerge_window_active = cluster_recmerge_apply_foreign = false;
	cluster_recmerge_window_scn = cluster_recmerge_window_own_lsn = 0;
}

UT_TEST(test_original_root_scanner_refuses_missing_source_or_version_dependency)
{
	for (source_failure = 1; source_failure <= 2; source_failure++) {
		ClusterThreadRecoveryFabricPlanV1 *plan = NULL;
		uint64 records = 99;

		UT_ASSERT_EQ(scan_three_roots(&plan, &records), source_failure == 1
			? RF_PAGE_PROOF_DETAIL_SOURCE_GAP : RF_PAGE_PROOF_DETAIL_EDGE_GAP);
		UT_ASSERT(plan == NULL && records == 0);
		UT_ASSERT_EQ(source_visits, source_failure == 1 ? 2 : 3);
	}
	source_failure = 0;
}

static RfPageOnlinePlanV1 *
make_plan_redo(uint32 participants, XLogRecPtr end, const XLogRecPtr *redo)
{
	RfContributorStreamCutV1 cuts[3] = {{0}};
	RfPageOnlinePlanRequestV1 request = {0};
	RfPageOnlinePlanV1 *plan = NULL;
	uint32 i;
	for (i = 0; i < participants; i++) {
		cuts[i].failed_thread = i + 1;
		cuts[i].timeline_id = 1;
		cuts[i].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cuts[i].scan_begin_inclusive = 0x100;
		cuts[i].scan_end_exclusive = end;
	}
	request.system_identifier = 99;
	memset(request.storage_uuid, 3, 16);
	request.physical_cuts = cuts;
	request.participant_count = participants;
	request.retention_binding_cookie = 41;
	request.redo_starts = redo;
	UT_ASSERT_EQ(rf_page_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

static RfPageOnlinePlanV1 *
make_plan_to(uint32 participants, XLogRecPtr end)
{
	return make_plan_redo(participants, end, NULL);
}

static RfPageOnlinePlanV1 *
make_plan(uint32 participants)
{
	return make_plan_to(participants, 0x200);
}

static RfPageProofDetailV1
enqueue_origin(RfPageOnlinePlanV1 *plan, RecordFixture *fixture, uint16 participant, uint16 origin)
{
	RfDetachedRecordPlanV1 detached;
	RfPageOnlineRecordIdentityV1 id = {0};
	RfPageProofDetailV1 detail;
	DecodedXLogRecord *record = fixture->reader.record;

	detail = rf_page_detached_preflight_v1(&fixture->reader, false, NULL, &detached);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	id.participant_index = participant;
	id.record.system_identifier = 99;
	memset(id.record.storage_uuid, 3, 16);
	id.record.origin_thread = origin;
	id.record.timeline_id = 1;
	id.record.read_rec_ptr = record->lsn;
	id.record.end_rec_ptr = record->next_lsn;
	id.record.record_crc = record->header.xl_crc;
	id.record.rmid = record->header.xl_rmid;
	id.record.info = record->header.xl_info;
	return rf_page_online_plan_queue_record_v1(plan, &detached, &id);
}

static RfPageProofDetailV1
enqueue(RfPageOnlinePlanV1 *plan, RecordFixture *fixture, uint16 participant)
{
	return enqueue_origin(plan, fixture, participant, participant + 1);
}

UT_TEST(test_same_thread_generations_keep_separate_prefixes_and_full_claims)
{
	RfContributorStreamCutV1 cuts[2] = { { 0 } };
	ClusterWalSourceRef sources[2], changed[2];
	RfPageOnlinePlanRequestV1 request = { 0 };
	RfPageOnlinePlanV1 *plan = NULL;
	RfPageContributionPrefixV1 checkpoint[2], retained[2], sentinel[2];
	RfPageOnlineTargetViewV1 target;
	RecordFixture record;
	memset(sources, 0, sizeof(sources));
	for (uint32 i = 0; i < 2; i++) {
		cuts[i].failed_thread = cuts[i].timeline_id = 1;
		cuts[i].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cuts[i].scan_begin_inclusive = 0x100;
		cuts[i].scan_end_exclusive = 0x200;
		cuts[i].origin_owner_incarnation = 41 + i;
		sources[i].claim.identity.system_identifier = 99;
		memset(sources[i].claim.identity.storage_uuid, 3, 16);
		memset(sources[i].claim.identity.authority_uuid, 4, 16);
		sources[i].claim.identity.origin_thread_id = 1;
		sources[i].claim.identity.origin_owner_incarnation = 41 + i;
		sources[i].claim.identity.root_lineage_seq = 10 + i;
		sources[i].claim.identity.thread_claim_created_at = 20 + i;
		sources[i].claim.database_incarnation = 42;
		sources[i].claim.max_config_generation = 5;
		sources[i].claim.claim_sha256[0] = 41 + i;
		sources[i].timeline = 1;
	}
	request.system_identifier = 99;
	memset(request.storage_uuid, 3, 16);
	request.physical_cuts = cuts;
	request.participant_count = 2;
	request.retention_binding_cookie = 41;
	UT_ASSERT_EQ(rf_page_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	if (plan == NULL)
		return;
	memcpy(changed, sources, sizeof(changed));
	changed[1] = sources[0];
	UT_ASSERT(!rf_page_online_plan_bind_sources_v1(plan, changed, 2));
	UT_ASSERT(rf_page_online_plan_bind_sources_v1(plan, sources, 2));
	/* Same thread, TLI and LSN, but different retained generation. */
	record_init(&record, 90, 7, false, BLCKSZ - 1, 0xb2);
	UT_ASSERT_EQ(enqueue_origin(plan, &record, 1, 1), RF_PAGE_PROOF_DETAIL_OK);
	record_init(&record, 10, 90, true, 0, 0xa1);
	UT_ASSERT_EQ(enqueue_origin(plan, &record, 0, 1), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_page_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_page_online_plan_target_v1(plan, 0, &target));
	if (rf_page_online_plan_target_v1(plan, 0, &target)) {
		RfPageStableGraphRequestV1 graph = *target.graph;
		RfPageStableSelectionV1 selected;
		RfPagePinnedSourceV1 base = *target.source;
		uint32 chain[2];

		UT_ASSERT_EQ(target.contributors->edge_count, 2);
		UT_ASSERT_EQ((uint8)target.canonical_page[BLCKSZ - 1], 0xb2);
		/* Exercise the lower graph validator as well as queue/prefix code.
		 * These booleans are a pure graph fixture, never a durable proof. */
		base.source_version = target.expected_before;
		graph.source = &base;
		graph.root_current = graph.duty_current = graph.fence_current = graph.retention_current
			= true;
		graph.retention_binding_cookie = graph.current_retention_binding_cookie = 41;
		UT_ASSERT_EQ(rf_page_stable_base_select_v1(&graph, chain, 2, &selected),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(selected.chain_length, 2);
	}
	UT_ASSERT(rf_page_online_plan_page_prefix_v1(plan, NULL, 0, checkpoint, 2));
	UT_ASSERT_EQ(checkpoint[0].origin_owner_incarnation, 41);
	UT_ASSERT_EQ(checkpoint[1].origin_owner_incarnation, 42);
	checkpoint[1].first_uncovered_lsn = 0x200;
	UT_ASSERT(rf_page_online_plan_dependency_prefix_v1(plan, checkpoint, 2, retained));
	UT_ASSERT_EQ(retained[1].first_uncovered_lsn, 0x100);
	checkpoint[0].first_uncovered_lsn = 0x200;
	UT_ASSERT(rf_page_online_plan_dependency_prefix_v1(plan, checkpoint, 2, retained));
	UT_ASSERT_EQ(retained[0].first_uncovered_lsn, 0x200);
	UT_ASSERT_EQ(retained[1].first_uncovered_lsn, 0x200);
	memset(sentinel, 0xa5, sizeof(sentinel));
	memcpy(retained, sentinel, sizeof(retained));
	checkpoint[1].origin_owner_incarnation = 41;
	UT_ASSERT(!rf_page_online_plan_dependency_prefix_v1(plan, checkpoint, 2, retained));
	UT_ASSERT_EQ(memcmp(retained, sentinel, sizeof(retained)), 0);
	rf_page_online_plan_destroy_v1(&plan);
	/* Neither omitted nor repeated generation can distinguish these cuts. */
	for (uint32 i = 0; i < 3; i++) {
		cuts[1].origin_owner_incarnation = i == 0 ? 0 : i == 1 ? 41 : UINT64_MAX;
		UT_ASSERT_NE(rf_page_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(plan == NULL);
	}
}

UT_TEST(test_reverse_real_fpi_delta_chain_owns_reader_bytes)
{
	RfPageOnlinePlanV1 *plan = make_plan(3);
	RecordFixture fixture;
	RfPageOnlineTargetViewV1 view;
	PGAlignedBlock expected;

	record_init(&fixture, 60, 7, false, BLCKSZ - 1, 0xc3);
	UT_ASSERT_EQ(enqueue(plan, &fixture, 2), RF_PAGE_PROOF_DETAIL_OK);
	record_init(&fixture, 900, 60, false, BLCKSZ - 2, 0xb2);
	UT_ASSERT_EQ(enqueue(plan, &fixture, 1), RF_PAGE_PROOF_DETAIL_OK);
	record_init(&fixture, 10, 900, true, 0, 0xa1);
	expected = fixture.image;
	UT_ASSERT_EQ(enqueue(plan, &fixture, 0), RF_PAGE_PROOF_DETAIL_OK);
	memset(&fixture, 0xdd, sizeof(fixture));
	UT_ASSERT_EQ(rf_page_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_page_online_plan_target_v1(plan, 0, &view));
	if (rf_page_online_plan_target_v1(plan, 0, &view)) {
		expected.data[BLCKSZ - 2] = (char)0xb2;
		expected.data[BLCKSZ - 1] = (char)0xc3;
		PageSetLSNPreserveOrigin(expected.data, 0x200);
		UT_ASSERT(PageSetLSNOrigin(expected.data, 2));
		((PageHeader)expected.data)->pd_block_scn = 7;
		UT_ASSERT(memcmp(expected.data, view.canonical_page, BLCKSZ) == 0);
		UT_ASSERT_EQ(view.contributors->edge_count, 3);
		UT_ASSERT_EQ(view.expected_before.mutation_token, 10);
		UT_ASSERT_EQ(view.expected_result.mutation_token, 7);
	}
	rf_page_online_plan_destroy_v1(&plan);
}

UT_TEST(test_cycle_and_branch_do_not_expose_canonical_pages)
{
	for (int cycle = 0; cycle < 2; cycle++) {
		RfPageOnlinePlanV1 *plan = make_plan(2);
		RecordFixture fixture;
		record_init(&fixture, 10, 11, true, 0, 1);
		UT_ASSERT_EQ(enqueue(plan, &fixture, 0), RF_PAGE_PROOF_DETAIL_OK);
		record_init(&fixture, cycle ? 11 : 10, cycle ? 10 : 12, true, 0, 2);
		UT_ASSERT_EQ(enqueue(plan, &fixture, 1), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_page_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_EDGE_GAP);
		UT_ASSERT_EQ(rf_page_online_plan_target_count_v1(plan), 0);
		rf_page_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_corrupt_owned_fpi_is_refused_by_real_decoder)
{
	RfPageOnlinePlanV1 *plan = make_plan(1);
	RecordFixture fixture;
	record_init(&fixture, 10, 11, true, 0, 1);
	fixture.decoded.record.blocks[0].bimg_len = 1;
	fixture.decoded.record.blocks[0].bimg_info |= BKPIMAGE_COMPRESS_PGLZ;
	UT_ASSERT_EQ(enqueue(plan, &fixture, 0), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_page_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_IMAGE_DECODE_FAILED);
	UT_ASSERT_EQ(rf_page_online_plan_target_count_v1(plan), 0);
	rf_page_online_plan_destroy_v1(&plan);
}

UT_TEST(test_shared_page_lsn_keeps_opaque_version_in_merge_window)
{
	PGAlignedBlock page;
	for (int shared = 0; shared < 2; shared++) {
		for (int foreign = 0; foreign < 2; foreign++) {
			PageInit(page.data, BLCKSZ, 0);
			((PageHeader)page.data)->pd_block_scn = 7;
			cluster_shared_config = shared != 0;
			cluster_recmerge_window_active = true;
			cluster_recmerge_apply_foreign = foreign != 0;
			cluster_recmerge_window_scn = 900;
			cluster_recmerge_window_own_lsn = 0x100;
			PageSetLSN(page.data, 0x200);
			UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, shared ? 7 : 900);
			UT_ASSERT_EQ(PageGetLSN(page.data), foreign ? 0x100 : 0x200);
		}
	}
	cluster_shared_config = true;
	cluster_recmerge_window_active = cluster_recmerge_apply_foreign = false;
	cluster_recmerge_window_scn = cluster_recmerge_window_own_lsn = 0;
}

UT_TEST(test_native_delta_decode_ignores_absent_image_fields)
{
	RfPageOnlinePlanV1 *plan = make_plan(2);
	RecordFixture fixture;
	PGAlignedBlock wire;
	DecodedXLogRecord *decoded;
	XLogRecordBlockHeader block_header = {0};
	XLogRecord *record = (XLogRecord *)wire.data;
	RfPageOnlineTargetViewV1 target;
	Size edge_bytes, size;
	char *next, *error = NULL;
	RfPageProofDetailV1 detail;

	record_init(&fixture, 10, 900, true, 0, 0xa1);
	UT_ASSERT_EQ(enqueue(plan, &fixture, 0), RF_PAGE_PROOF_DETAIL_OK);
	record_init(&fixture, 900, 7, false, BLCKSZ - 1, 0xb2);
	memset(&wire, 0, sizeof(wire));
	*record = fixture.decoded.record.header;
	next = wire.data + SizeOfXLogRecord;
	UT_ASSERT(XLogEncodePageVersionEdgeV1((uint8 *)next, BLCKSZ - SizeOfXLogRecord,
		7, fixture.decoded.record.page_version_edge.entries, 1, &edge_bytes));
	next += edge_bytes;
	block_header.id = 0;
	block_header.fork_flags = MAIN_FORKNUM | BKPBLOCK_HAS_DATA;
	block_header.data_length = sizeof(fixture.delta);
	memcpy(next, &block_header, SizeOfXLogRecordBlockHeader);
	next += SizeOfXLogRecordBlockHeader;
	memcpy(next, &fixture.decoded.record.blocks[0].rlocator, sizeof(RelFileLocator));
	next += sizeof(RelFileLocator);
	memcpy(next, &fixture.decoded.record.blocks[0].blkno, sizeof(BlockNumber));
	next += sizeof(BlockNumber);
	memcpy(next, fixture.delta, sizeof(fixture.delta));
	next += sizeof(fixture.delta);
	record->xl_tot_len = next - wire.data;
	size = DecodeXLogRecordRequiredSpace(record->xl_tot_len);
	decoded = malloc(size);
	UT_ASSERT(decoded != NULL);
	if (decoded == NULL)
		abort();
	memset(decoded, 0xa5, size);
	decoded->blocks[0].bkp_image = NULL;
	UT_ASSERT(DecodeXLogRecord(&fixture.reader, decoded, record, 0x100, &error));
	UT_ASSERT(!decoded->blocks[0].has_image);
	UT_ASSERT_EQ(decoded->blocks[0].bimg_len, 0xa5a5);
	decoded->next_lsn = 0x200;
	fixture.reader.record = decoded;
	detail = enqueue(plan, &fixture, 1);
	UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
	memset(decoded, 0xdd, size);
	free(decoded);
	if (detail == RF_PAGE_PROOF_DETAIL_OK) {
		UT_ASSERT_EQ(rf_page_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_page_online_plan_target_v1(plan, 0, &target));
		if (rf_page_online_plan_target_v1(plan, 0, &target))
			UT_ASSERT_EQ((uint8)target.canonical_page[BLCKSZ - 1], 0xb2);
	}
	rf_page_online_plan_destroy_v1(&plan);
}

UT_TEST(test_checkpoint_prefix_keeps_complete_three_origin_ancestry)
{
	RfPageOnlinePlanV1 *plan = make_plan(3);
	RecordFixture fixture;
	RfPageContributionPrefixV1 checkpoints[3] = { { 0 } }, retained[3], sentinel[3];

	memset(sentinel, 0xa5, sizeof(sentinel));
	memcpy(retained, sentinel, sizeof(retained));
	UT_ASSERT(!rf_page_online_plan_dependency_prefix_v1(plan, checkpoints, 3, retained));
	UT_ASSERT_EQ(memcmp(retained, sentinel, sizeof(retained)), 0);
	record_init(&fixture, 60, 7, false, BLCKSZ - 1, 0xc3);
	UT_ASSERT_EQ(enqueue(plan, &fixture, 2), RF_PAGE_PROOF_DETAIL_OK);
	record_init(&fixture, 900, 60, false, BLCKSZ - 2, 0xb2);
	UT_ASSERT_EQ(enqueue(plan, &fixture, 1), RF_PAGE_PROOF_DETAIL_OK);
	record_init(&fixture, 10, 900, true, 0, 0xa1);
	UT_ASSERT_EQ(enqueue(plan, &fixture, 0), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_page_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	for (unsigned completed = 0; completed < 8; completed++) {
		for (int i = 0; i < 3; i++) {
			checkpoints[i].origin_thread = i + 1;
			checkpoints[i].timeline = 1;
			checkpoints[i].first_uncovered_lsn = (completed & (1U << i)) ? 0x200 : 0x100;
		}
		UT_ASSERT(rf_page_online_plan_dependency_prefix_v1(plan, checkpoints, 3, retained));
		for (int i = 0; i < 3; i++) {
			UT_ASSERT_EQ(retained[i].origin_thread, i + 1);
			UT_ASSERT_EQ(retained[i].timeline, 1);
			UT_ASSERT_EQ(retained[i].first_uncovered_lsn, completed == 7 ? 0x200 : 0x100);
		}
	}
	for (int bad = 0; bad < 7; bad++) {
		RfPageContributionPrefixV1 changed[3];
		memcpy(changed, checkpoints, sizeof(changed));
		memcpy(retained, sentinel, sizeof(retained));
		switch (bad) {
		case 0:
			changed[1].origin_thread = 1;
			break;
		case 1:
			changed[1].timeline = 2;
			break;
		case 2:
			changed[1].reserved_zero = 1;
			break;
		case 3:
			changed[1].first_uncovered_lsn = 0xff;
			break;
		case 4:
			changed[1].first_uncovered_lsn = 0x201;
			break;
		case 5:
			changed[1].first_uncovered_lsn = 0x108;
			break;
		case 6:
			changed[1].first_uncovered_lsn = InvalidXLogRecPtr;
			break;
		}
		UT_ASSERT(!rf_page_online_plan_dependency_prefix_v1(plan, changed, 3, retained));
		UT_ASSERT_EQ(memcmp(retained, sentinel, sizeof(retained)), 0);
	}
	checkpoints[0].first_uncovered_lsn = 0x100;
	UT_ASSERT(rf_page_online_plan_dependency_prefix_v1(plan, checkpoints, 3, checkpoints));
	for (int i = 0; i < 3; i++)
		UT_ASSERT_EQ(checkpoints[i].first_uncovered_lsn, 0x100);
	rf_page_online_plan_destroy_v1(&plan);
}

UT_TEST(test_retained_history_does_not_reopen_completed_other_page)
{
	RfPageOnlinePlanV1 *plan = make_plan_to(2, 0x300);
	RecordFixture fixture;
	RfPageContributionPrefixV1 checkpoints[2] = { { 1, 0, 1, 0x300 }, { 2, 0, 1, 0x200 } };
	RfPageContributionPrefixV1 retained[2] = { { 0 } };

	/* Page 4: A -> B, B is still an obligation. Page 5: B -> A, both
	 * completed. Retaining A's earlier page 4 record is not a new duty on
	 * page 5 and must not recursively pull B's old page 5 record back in. */
	record_init(&fixture, 10, 900, true, 0, 0xa1);
	UT_ASSERT_EQ(enqueue(plan, &fixture, 0), RF_PAGE_PROOF_DETAIL_OK);
	record_init(&fixture, 19, 3, false, BLCKSZ - 1, 0xa2);
	fixture.reader.ReadRecPtr = fixture.decoded.record.lsn = 0x200;
	fixture.reader.EndRecPtr = fixture.decoded.record.next_lsn = 0x300;
	fixture.decoded.record.blocks[0].blkno = 5;
	UT_ASSERT_EQ(enqueue(plan, &fixture, 0), RF_PAGE_PROOF_DETAIL_OK);
	record_init(&fixture, 20, 19, true, 0, 0xb1);
	fixture.decoded.record.blocks[0].blkno = 5;
	UT_ASSERT_EQ(enqueue(plan, &fixture, 1), RF_PAGE_PROOF_DETAIL_OK);
	record_init(&fixture, 900, 7, false, BLCKSZ - 1, 0xb2);
	fixture.reader.ReadRecPtr = fixture.decoded.record.lsn = 0x200;
	fixture.reader.EndRecPtr = fixture.decoded.record.next_lsn = 0x300;
	UT_ASSERT_EQ(enqueue(plan, &fixture, 1), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_page_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_page_online_plan_dependency_prefix_v1(plan, checkpoints, 2, retained));
	UT_ASSERT_EQ(retained[0].first_uncovered_lsn, 0x100);
	UT_ASSERT_EQ(retained[1].first_uncovered_lsn, 0x200);
	checkpoints[1].first_uncovered_lsn = 0x300;
	UT_ASSERT(rf_page_online_plan_dependency_prefix_v1(plan, checkpoints, 2, retained));
	UT_ASSERT_EQ(retained[0].first_uncovered_lsn, 0x300);
	UT_ASSERT_EQ(retained[1].first_uncovered_lsn, 0x300);
	rf_page_online_plan_destroy_v1(&plan);
}

UT_TEST(test_retained_checkpoint_history_keeps_ancestors_without_new_page_duty)
{
	for (unsigned completed = 0; completed < 8; completed++) {
		XLogRecPtr redo[3];
		RfPageOnlinePlanV1 *plan;
		RfPageOnlineTargetViewV1 view;
		RecordFixture fixture;
		for (int i = 0; i < 3; i++)
			redo[i] = completed & (1U << i) ? 0x200 : 0x100;
		plan = make_plan_redo(3, 0x200, redo);
		/* The plan owns the selected boundaries, not these caller bytes. */
		memset(redo, 0, sizeof(redo));
		record_init(&fixture, 60, 7, false, BLCKSZ - 1, 0xc3);
		UT_ASSERT_EQ(enqueue(plan, &fixture, 2), RF_PAGE_PROOF_DETAIL_OK);
		record_init(&fixture, 900, 60, false, BLCKSZ - 2, 0xb2);
		UT_ASSERT_EQ(enqueue(plan, &fixture, 1), RF_PAGE_PROOF_DETAIL_OK);
		record_init(&fixture, 10, 900, true, 0, 0xa1);
		UT_ASSERT_EQ(enqueue(plan, &fixture, 0), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_page_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		if (completed == 7) {
			UT_ASSERT_EQ(rf_page_online_plan_target_count_v1(plan), 0);
			rf_page_online_plan_destroy_v1(&plan);
			continue;
		}
		UT_ASSERT(rf_page_online_plan_target_v1(plan, 0, &view));
		UT_ASSERT_EQ(view.history_only, completed == 7);
		UT_ASSERT_EQ(view.contributors->edge_count, 3);
		UT_ASSERT_EQ((uint8)view.canonical_page[BLCKSZ - 3], 0xa1);
		UT_ASSERT_EQ((uint8)view.canonical_page[BLCKSZ - 2], 0xb2);
		UT_ASSERT_EQ((uint8)view.canonical_page[BLCKSZ - 1], 0xc3);
		rf_page_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_unneeded_history_delta_does_not_require_a_new_data_base)
{
	/* Completed generations at the same address are not ancestors of a
	 * recreated page. Neither an old delta nor an old FPI may reopen them. */
	for (unsigned scenario = 0; scenario < 3; scenario++) {
		XLogRecPtr redo = 0x200;
		RfPageOnlinePlanV1 *plan = make_plan_redo(1, 0x300, &redo);
		RecordFixture fixture;
		RfPageOnlineTargetViewV1 view;
		BlockNumber blockno = scenario == 0 ? 5 : 4;
		uint8 incarnation = scenario == 0 ? 7 : 8;

		record_init(&fixture, 60, 7, scenario == 2, BLCKSZ - 1, 0xc3);
		UT_ASSERT_EQ(enqueue(plan, &fixture, 0), RF_PAGE_PROOF_DETAIL_OK);
		record_init(&fixture, 10, 900, true, 0, 0xa1);
		fixture.decoded.record.blocks[0].blkno = blockno;
		memset(fixture.decoded.record.page_version_edge.entries[0].before.segment_incarnation,
			   incarnation, 16);
		memset(fixture.decoded.record.page_version_edge.entries[0].result_incarnation, incarnation,
			   16);
		fixture.reader.ReadRecPtr = fixture.decoded.record.lsn = 0x200;
		fixture.reader.EndRecPtr = fixture.decoded.record.next_lsn = 0x300;
		UT_ASSERT_EQ(enqueue(plan, &fixture, 0), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_page_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_page_online_plan_target_count_v1(plan), 1);
		UT_ASSERT(rf_page_online_plan_target_v1(plan, 0, &view));
		if (rf_page_online_plan_target_v1(plan, 0, &view)) {
			UT_ASSERT_EQ(view.page_identity.blockno, blockno);
			UT_ASSERT(!view.history_only);
			UT_ASSERT_EQ(view.contributors->edge_count, 1);
			UT_ASSERT_EQ(view.expected_result.segment_incarnation[0], incarnation);
			UT_ASSERT_EQ((uint8)view.canonical_page[BLCKSZ - 1], 0xa1);
		}
		rf_page_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_native_checkpoint_cannot_split_a_retained_record)
{
	XLogRecPtr redo = 0x180;
	RfPageOnlinePlanV1 *plan = make_plan_redo(1, 0x200, &redo);
	RecordFixture fixture;
	record_init(&fixture, 10, 900, true, 0, 0xa1);
	UT_ASSERT_EQ(enqueue(plan, &fixture, 0), RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	UT_ASSERT_EQ(rf_page_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	rf_page_online_plan_destroy_v1(&plan);
}

UT_TEST(test_actual_root_scan_binds_native_redo_to_complete_ancestry_input)
{
	for (source_completed = 0; source_completed < 8; source_completed++) {
		ClusterThreadRecoveryFabricPlanV1 *fabric = NULL;
		const RfPageOnlinePlanV1 *plan;
		uint64 records;
		RfPageOnlineTargetViewV1 view;
		UT_ASSERT_EQ(scan_three_roots(&fabric, &records), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(records, 3);
		plan = cluster_thread_recovery_fabric_page_plan_v1(fabric);
		UT_ASSERT_EQ(rf_page_online_plan_target_count_v1(plan), source_completed == 7 ? 0 : 1);
		if (source_completed != 7 && rf_page_online_plan_target_v1(plan, 0, &view)) {
			UT_ASSERT_EQ(view.contributors->edge_count, 3);
			UT_ASSERT(!view.history_only);
			UT_ASSERT_EQ((uint8)view.canonical_page[BLCKSZ - 3], 0xa1);
			UT_ASSERT_EQ((uint8)view.canonical_page[BLCKSZ - 2], 0xb2);
			UT_ASSERT_EQ((uint8)view.canonical_page[BLCKSZ - 1], 0xc3);
		}
		cluster_thread_recovery_fabric_plan_destroy_v1(&fabric);
	}
	source_completed = 0;
}

UT_TEST(test_actual_fabric_refuses_routed_components_without_side_consumer)
{
	for (source_failure = 3; source_failure <= 5; source_failure++) {
		ClusterThreadRecoveryFabricPlanV1 *plan = NULL;
		uint64 records = UINT64_MAX;

		UT_ASSERT_EQ(scan_three_roots(&plan, &records), RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT(plan == NULL);
		UT_ASSERT_EQ(records, 0);
		UT_ASSERT_EQ(source_visits, 1);
		cluster_thread_recovery_fabric_plan_destroy_v1(&plan);
	}
	source_failure = 0;
}

int
main(void)
{
	UT_PLAN(15);
	UT_RUN(test_same_thread_generations_keep_separate_prefixes_and_full_claims);
	UT_RUN(test_reverse_real_fpi_delta_chain_owns_reader_bytes);
	UT_RUN(test_cycle_and_branch_do_not_expose_canonical_pages);
	UT_RUN(test_corrupt_owned_fpi_is_refused_by_real_decoder);
	UT_RUN(test_shared_page_lsn_keeps_opaque_version_in_merge_window);
	UT_RUN(test_native_delta_decode_ignores_absent_image_fields);
	UT_RUN(test_original_root_scanner_resolves_reverse_three_origin_native_page_chain);
	UT_RUN(test_original_root_scanner_refuses_missing_source_or_version_dependency);
	UT_RUN(test_checkpoint_prefix_keeps_complete_three_origin_ancestry);
	UT_RUN(test_retained_history_does_not_reopen_completed_other_page);
	UT_RUN(test_actual_fabric_refuses_routed_components_without_side_consumer);
	UT_RUN(test_retained_checkpoint_history_keeps_ancestors_without_new_page_duty);
	UT_RUN(test_native_checkpoint_cannot_split_a_retained_record);
	UT_RUN(test_unneeded_history_delta_does_not_require_a_new_data_base);
	UT_RUN(test_actual_root_scan_binds_native_redo_to_complete_ancestry_input);
	UT_DONE();
	return ut_failed_count != 0;
}
