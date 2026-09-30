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
#include "storage/bufpage.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_enabled = true, cluster_shared_config = true;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;
int cluster_node_id;

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

static RfPageOnlinePlanV1 *
make_plan(uint32 participants)
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
		cuts[i].scan_end_exclusive = 0x200;
	}
	request.system_identifier = 99;
	memset(request.storage_uuid, 3, 16);
	request.physical_cuts = cuts;
	request.participant_count = participants;
	request.retention_binding_cookie = 41;
	UT_ASSERT_EQ(rf_page_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

static RfPageProofDetailV1
enqueue(RfPageOnlinePlanV1 *plan, RecordFixture *fixture, uint16 participant)
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
	id.record.origin_thread = participant + 1;
	id.record.timeline_id = 1;
	id.record.read_rec_ptr = record->lsn;
	id.record.end_rec_ptr = record->next_lsn;
	id.record.record_crc = record->header.xl_crc;
	id.record.rmid = record->header.xl_rmid;
	id.record.info = record->header.xl_info;
	return rf_page_online_plan_queue_record_v1(plan, &detached, &id);
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
		PageSetLSN(expected.data, 0x200);
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

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_reverse_real_fpi_delta_chain_owns_reader_bytes);
	UT_RUN(test_cycle_and_branch_do_not_expose_canonical_pages);
	UT_RUN(test_corrupt_owned_fpi_is_refused_by_real_decoder);
	UT_RUN(test_shared_page_lsn_keeps_opaque_version_in_merge_window);
	UT_RUN(test_native_delta_decode_ignores_absent_image_fields);
	UT_DONE();
	return ut_failed_count != 0;
}
