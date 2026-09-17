/*-------------------------------------------------------------------------
 *
 * test_cluster_page_detached.c
 *    STOP-06 whole-record detached codec preflight and apply contract.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#if defined(__has_include)
#if __has_include("cluster/cluster_page_detached.h")
#include "access/rmgr.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "cluster/cluster_block_apply.h"
#include "cluster/cluster_page_detached.h"
#include "cluster/cluster_rf_route.h"
#include "storage/bufpage.h"
#define TEST_HAVE_CLUSTER_PAGE_DETACHED 1
#endif
#endif

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

#ifndef TEST_HAVE_CLUSTER_PAGE_DETACHED

UT_TEST(test_detached_codec_capability_red)
{
	printf("# JIT_SEMANTIC_RED:D6-4-DETACHED-CODEC-PREFLIGHT\n");
	UT_ASSERT(false);
}

int
main(void)
{
	UT_PLAN(1);
	UT_RUN(test_detached_codec_capability_red);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#else

bool cluster_recmerge_window_active = false;
uint64 cluster_recmerge_window_scn = 0;
uint64 cluster_recmerge_window_own_lsn = 0;
bool cluster_recmerge_apply_foreign = false;
int cluster_node_id = 0;

bool
rf_page_version_present_v1(const RfPageVersionV1 *version)
{
	uint8 value = 0;
	int i;

	if (version == NULL || version->mutation_token == 0)
		return false;
	for (i = 0; i < 16; i++)
		value |= version->segment_incarnation[i];
	return value != 0;
}

static int restore_calls;
static bool restore_ok = true;

bool
RestoreBlockImage(XLogReaderState *record, uint8 block_id, char *page)
{
	PageHeader header = (PageHeader)page;

	restore_calls++;
	if (!restore_ok)
		return false;
	memset(page, 0, BLCKSZ);
	header->pd_lower = SizeOfPageHeaderData;
	header->pd_upper = BLCKSZ;
	header->pd_special = BLCKSZ;
	return true;
}

char *
XLogRecGetBlockData(XLogReaderState *record, uint8 block_id, Size *len)
{
	if (len != NULL)
		*len = 0;
	return NULL;
}

ClusterBlkApplyResult
cluster_block_apply_heap(XLogReaderState *record, uint8 block_id, char *page)
{
	return CLUSTER_BLKAPPLY_UNSUPPORTED;
}

typedef struct FakeRecord {
	XLogReaderState state;
	union {
		DecodedXLogRecord decoded;
		char padding[sizeof(DecodedXLogRecord)
					 + XLR_PAGE_VERSION_EDGE_MAX_ENTRIES * sizeof(DecodedBkpBlock)];
	} storage;
} FakeRecord;

typedef struct OwnerFixture {
	int calls;
	RfPageProofDetailV1 result;
} OwnerFixture;

static void
set_incarnation(uint8 incarnation[16], uint8 value)
{
	memset(incarnation, value, 16);
}

static XLogReaderState *
make_page_record(FakeRecord *record, uint8 rmid, uint8 info, uint8 page_class, ForkNumber forknum,
				 bool apply_image)
{
	static char image[BLCKSZ];
	DecodedXLogRecord *decoded;
	DecodedBkpBlock *block;
	RfPageVersionEdgeEntryV1 *edge;

	memset(record, 0, sizeof(*record));
	decoded = &record->storage.decoded;
	decoded->header.xl_rmid = rmid;
	decoded->header.xl_info = info;
	decoded->max_block_id = 0;
	decoded->has_page_version_edge = true;
	decoded->page_version_edge.entry_count = 1;
	decoded->page_version_edge.result_token = 22;
	block = &decoded->blocks[0];
	block->in_use = true;
	block->forknum = forknum;
	block->component_ordinal = 0;
	block->has_image = apply_image;
	block->apply_image = apply_image;
	block->bimg_len = apply_image ? BLCKSZ : 0;
	block->bkp_image = apply_image ? image : NULL;
	edge = &decoded->page_version_edge.entries[0];
	edge->block_id = 0;
	edge->page_class = page_class;
	edge->before_kind = RF_PAGE_STATE_PRESENT;
	edge->result_kind = RF_PAGE_STATE_PRESENT;
	edge->component_ordinal = 0;
	set_incarnation(edge->before.segment_incarnation, 7);
	edge->before.mutation_token = 21;
	set_incarnation(edge->result_incarnation, 7);
	if (apply_image)
		edge->edge_flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
	record->state.record = decoded;
	record->state.EndRecPtr = 0x1234;
	return &record->state;
}

static RfPageProofDetailV1
owner_preflight(void *arg, const RfOpcodeRouteV1 *route, const RfPageVersionEdgeEntryV1 *edge,
				const DecodedBkpBlock *block)
{
	OwnerFixture *fixture = (OwnerFixture *)arg;

	fixture->calls++;
	return fixture->result;
}

static RfDetachedOwnerOpsV1
make_owner_ops(OwnerFixture *fixture)
{
	RfDetachedOwnerOpsV1 ops;

	memset(&ops, 0, sizeof(ops));
	ops.arg = fixture;
	ops.preflight_side_record = owner_preflight;
	ops.preflight_side_component = owner_preflight;
	ops.preflight_rebuildable_component = owner_preflight;
	return ops;
}

UT_TEST(test_every_page_route_has_exact_codec_vtable)
{
	size_t i;

	for (i = 0; i < rf_opcode_route_manifest_count_v1(); i++) {
		RfOpcodeRouteV1 route;
		const RfDetachedPageCodecV1 *codec;
		bool active;

		UT_ASSERT(rf_opcode_route_manifest_entry_v1(i, &route, &active));
		if (route.record_owner != RF_ROUTE_OWNER_PAGE_CODEC)
			continue;
		codec = rf_page_detached_codec_lookup_v1(route.codec_id);
		UT_ASSERT(codec != NULL);
		UT_ASSERT_EQ(codec->codec_id, route.codec_id);
		UT_ASSERT_EQ(codec->rmid, route.rmid);
		UT_ASSERT_EQ(codec->abi_version, 1);
		UT_ASSERT(codec->preflight != NULL);
		UT_ASSERT(codec->apply != NULL);
	}
}

UT_TEST(test_ordinary_record_preflight_builds_immutable_plan)
{
	FakeRecord record;
	RfDetachedRecordPlanV1 plan;
	XLogReaderState *state
		= make_page_record(&record, RM_GENERIC_ID, 0, RF_PAGE_CLASS_ORDINARY, MAIN_FORKNUM, true);

	memset(&plan, 0xa5, sizeof(plan));
	UT_ASSERT_EQ(rf_page_detached_preflight_v1(state, false, NULL, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(plan.preflight_complete);
	UT_ASSERT_EQ(plan.component_count, 1);
	UT_ASSERT_EQ(plan.components[0].before_kind, RF_PAGE_STATE_PRESENT);
	UT_ASSERT_EQ(plan.components[0].result_kind, RF_PAGE_STATE_PRESENT);
	UT_ASSERT_EQ(plan.components[0].owner, RF_DETACHED_COMPONENT_PAGE_CODEC);
	UT_ASSERT_EQ(plan.components[0].block_id, 0);
	UT_ASSERT_EQ(plan.result_token, 22);
}

UT_TEST(test_missing_edge_fails_without_plan_exposure)
{
	FakeRecord record;
	RfDetachedRecordPlanV1 plan;
	RfDetachedRecordPlanV1 before;
	XLogReaderState *state
		= make_page_record(&record, RM_GENERIC_ID, 0, RF_PAGE_CLASS_ORDINARY, MAIN_FORKNUM, true);

	state->record->has_page_version_edge = false;
	memset(&plan, 0xa5, sizeof(plan));
	before = plan;
	UT_ASSERT_EQ(rf_page_detached_preflight_v1(state, false, NULL, &plan),
				 RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE);
	UT_ASSERT(memcmp(&plan, &before, sizeof(plan)) == 0);
}

UT_TEST(test_ordinary_wrong_fork_is_class_failure)
{
	FakeRecord record;
	RfDetachedRecordPlanV1 plan;
	XLogReaderState *state
		= make_page_record(&record, RM_GENERIC_ID, 0, RF_PAGE_CLASS_ORDINARY, FSM_FORKNUM, true);

	UT_ASSERT_EQ(rf_page_detached_preflight_v1(state, false, NULL, &plan),
				 RF_PAGE_PROOF_DETAIL_CLASS_UNKNOWN);
}

UT_TEST(test_rebuildable_component_requires_owner_preflight)
{
	FakeRecord record;
	RfDetachedRecordPlanV1 plan;
	OwnerFixture fixture = { 0, RF_PAGE_PROOF_DETAIL_OK };
	RfDetachedOwnerOpsV1 ops = make_owner_ops(&fixture);
	XLogReaderState *state = make_page_record(&record, RM_GENERIC_ID, 0,
											  RF_PAGE_CLASS_REBUILDABLE_FSM, FSM_FORKNUM, false);
	RfPageVersionEdgeEntryV1 *edge = &state->record->page_version_edge.entries[0];

	memset(&edge->before, 0, sizeof(edge->before));
	memset(edge->result_incarnation, 0, sizeof(edge->result_incarnation));
	edge->before_kind = RF_PAGE_STATE_REBUILDABLE;
	edge->result_kind = RF_PAGE_STATE_REBUILDABLE;
	UT_ASSERT_EQ(rf_page_detached_preflight_v1(state, false, &ops, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(fixture.calls, 1);
	UT_ASSERT_EQ(plan.components[0].owner, RF_DETACHED_COMPONENT_REBUILDABLE);
}

UT_TEST(test_space_component_requires_activation_and_typed_owner)
{
	FakeRecord record;
	RfDetachedRecordPlanV1 plan;
	OwnerFixture fixture = { 0, RF_PAGE_PROOF_DETAIL_OK };
	RfDetachedOwnerOpsV1 ops = make_owner_ops(&fixture);
	XLogReaderState *state = make_page_record(&record, RM_GENERIC_ID, 0, RF_PAGE_CLASS_ROUTED_SPACE,
											  (ForkNumber)4, false);
	RfPageVersionEdgeEntryV1 *edge = &state->record->page_version_edge.entries[0];

	memset(&edge->before, 0, sizeof(edge->before));
	memset(edge->result_incarnation, 0, sizeof(edge->result_incarnation));
	edge->before_kind = RF_PAGE_STATE_ROUTED;
	edge->result_kind = RF_PAGE_STATE_ROUTED;
	UT_ASSERT_EQ(rf_page_detached_preflight_v1(state, false, &ops, &plan),
				 RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED);
	UT_ASSERT_EQ(fixture.calls, 0);
	UT_ASSERT_EQ(rf_page_detached_preflight_v1(state, true, &ops, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(fixture.calls, 1);
	UT_ASSERT_EQ(plan.components[0].owner, RF_DETACHED_COMPONENT_SIDE_TYPED);
}

UT_TEST(test_side_record_is_preflighted_by_typed_owner)
{
	FakeRecord record;
	RfDetachedRecordPlanV1 plan;
	OwnerFixture fixture = { 0, RF_PAGE_PROOF_DETAIL_OK };
	RfDetachedOwnerOpsV1 ops = make_owner_ops(&fixture);
	XLogReaderState *state
		= make_page_record(&record, RM_SMGR_ID, 0x10, RF_PAGE_CLASS_ORDINARY, MAIN_FORKNUM, false);

	state->record->max_block_id = -1;
	state->record->has_page_version_edge = false;
	memset(&state->record->page_version_edge, 0, sizeof(state->record->page_version_edge));
	UT_ASSERT_EQ(rf_page_detached_preflight_v1(state, false, &ops, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(fixture.calls, 1);
	UT_ASSERT_EQ(plan.component_count, 0);
	UT_ASSERT(plan.preflight_complete);
}

UT_TEST(test_owner_failure_blocks_whole_record_without_plan)
{
	FakeRecord record;
	RfDetachedRecordPlanV1 plan;
	RfDetachedRecordPlanV1 before;
	OwnerFixture fixture = { 0, RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE };
	RfDetachedOwnerOpsV1 ops = make_owner_ops(&fixture);
	XLogReaderState *state
		= make_page_record(&record, RM_SMGR_ID, 0x10, RF_PAGE_CLASS_ORDINARY, MAIN_FORKNUM, false);

	state->record->max_block_id = -1;
	state->record->has_page_version_edge = false;
	memset(&plan, 0xa5, sizeof(plan));
	before = plan;
	UT_ASSERT_EQ(rf_page_detached_preflight_v1(state, false, &ops, &plan),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT(memcmp(&plan, &before, sizeof(plan)) == 0);
}

UT_TEST(test_fpi_apply_stamps_successor_token)
{
	FakeRecord record;
	RfDetachedRecordPlanV1 plan;
	PGAlignedBlock old_page;
	PGAlignedBlock output;
	XLogReaderState *state
		= make_page_record(&record, RM_GENERIC_ID, 0, RF_PAGE_CLASS_ORDINARY, MAIN_FORKNUM, true);

	memset(old_page.data, 0, BLCKSZ);
	memset(output.data, 0xa5, BLCKSZ);
	restore_calls = 0;
	restore_ok = true;
	UT_ASSERT_EQ(rf_page_detached_preflight_v1(state, false, NULL, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_page_detached_apply_v1(&plan, 0, old_page.data, output.data),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(restore_calls, 1);
	UT_ASSERT_EQ(((PageHeader)output.data)->pd_block_scn, 22);
}

UT_TEST(test_unsupported_delta_leaves_output_untouched)
{
	FakeRecord record;
	RfDetachedRecordPlanV1 plan;
	PGAlignedBlock old_page;
	PGAlignedBlock output;
	PGAlignedBlock before;
	XLogReaderState *state
		= make_page_record(&record, RM_BTREE_ID, 0, RF_PAGE_CLASS_ORDINARY, MAIN_FORKNUM, false);

	memset(old_page.data, 0, BLCKSZ);
	((PageHeader)old_page.data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)old_page.data)->pd_upper = BLCKSZ;
	((PageHeader)old_page.data)->pd_special = BLCKSZ;
	memset(output.data, 0xa5, BLCKSZ);
	before = output;
	UT_ASSERT_EQ(rf_page_detached_preflight_v1(state, false, NULL, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_page_detached_apply_v1(&plan, 0, old_page.data, output.data),
				 RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED);
	UT_ASSERT(memcmp(output.data, before.data, BLCKSZ) == 0);
}

UT_TEST(test_component_ordinal_mismatch_blocks_before_owner)
{
	FakeRecord record;
	RfDetachedRecordPlanV1 plan;
	OwnerFixture fixture = { 0, RF_PAGE_PROOF_DETAIL_OK };
	RfDetachedOwnerOpsV1 ops = make_owner_ops(&fixture);
	XLogReaderState *state = make_page_record(&record, RM_GENERIC_ID, 0,
											  RF_PAGE_CLASS_REBUILDABLE_FSM, FSM_FORKNUM, false);
	RfPageVersionEdgeEntryV1 *edge = &state->record->page_version_edge.entries[0];

	memset(&edge->before, 0, sizeof(edge->before));
	memset(edge->result_incarnation, 0, sizeof(edge->result_incarnation));
	edge->before_kind = RF_PAGE_STATE_REBUILDABLE;
	edge->result_kind = RF_PAGE_STATE_REBUILDABLE;
	edge->component_ordinal = 9;
	UT_ASSERT_EQ(rf_page_detached_preflight_v1(state, false, &ops, &plan),
				 RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE);
	UT_ASSERT_EQ(fixture.calls, 0);
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_every_page_route_has_exact_codec_vtable);
	UT_RUN(test_ordinary_record_preflight_builds_immutable_plan);
	UT_RUN(test_missing_edge_fails_without_plan_exposure);
	UT_RUN(test_ordinary_wrong_fork_is_class_failure);
	UT_RUN(test_rebuildable_component_requires_owner_preflight);
	UT_RUN(test_space_component_requires_activation_and_typed_owner);
	UT_RUN(test_side_record_is_preflighted_by_typed_owner);
	UT_RUN(test_owner_failure_blocks_whole_record_without_plan);
	UT_RUN(test_fpi_apply_stamps_successor_token);
	UT_RUN(test_unsupported_delta_leaves_output_untouched);
	UT_RUN(test_component_ordinal_mismatch_blocks_before_owner);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#endif
