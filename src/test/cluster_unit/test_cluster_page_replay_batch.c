/*-------------------------------------------------------------------------
 *
 * test_cluster_page_replay_batch.c
 *    STOP-06 detached-plan to canonical-install production bridge.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#if defined(__has_include)
#if __has_include("cluster/cluster_page_replay_batch.h")
#include "cluster/cluster_page_replay_batch.h"
#include "storage/bufpage.h"
#define TEST_HAVE_CLUSTER_PAGE_REPLAY_BATCH 1
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

#ifndef TEST_HAVE_CLUSTER_PAGE_REPLAY_BATCH

UT_TEST(test_detached_batch_caller_capability_red)
{
	printf("# JIT_SEMANTIC_RED:D6-7-DETACHED-BATCH-CALLER\n");
	UT_ASSERT(false);
}

int
main(void)
{
	UT_PLAN(1);
	UT_RUN(test_detached_batch_caller_capability_red);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#else

typedef struct FakeRecord {
	XLogReaderState state;
	union {
		DecodedXLogRecord decoded;
		char padding[sizeof(DecodedXLogRecord)
					 + XLR_PAGE_VERSION_EDGE_MAX_ENTRIES * sizeof(DecodedBkpBlock)];
	} storage;
} FakeRecord;

typedef struct BatchFixture {
	PGAlignedBlock disk[RF_PAGE_STABLE_MAX_COMPONENTS];
	bool exists[RF_PAGE_STABLE_MAX_COMPONENTS];
	int read_calls;
	int write_calls;
	int sync_calls;
	int promote_calls;
	int publish_calls;
	int release_calls;
	bool last_extend;
} BatchFixture;

typedef struct BatchCase {
	BatchFixture fixture;
	FakeRecord records[4];
	RfDetachedRecordPlanV1 plans[4];
	RfPageReplayRecordV1 replay_records[4];
	RfContributorStreamCutV1 participants[2];
	uint8 record_component_seen[4][RF_PAGE_STABLE_MAX_COMPONENTS];
	RfPageReplayStepV1 steps[RF_PAGE_STABLE_MAX_COMPONENTS][2];
	RfPageReplayTargetV1 targets[RF_PAGE_STABLE_MAX_COMPONENTS];
	PGAlignedBlock base[RF_PAGE_STABLE_MAX_COMPONENTS];
	PGAlignedBlock canonical[RF_PAGE_STABLE_MAX_COMPONENTS];
	PGAlignedBlock prepared[RF_PAGE_STABLE_MAX_COMPONENTS];
	PGAlignedBlock io[RF_PAGE_STABLE_MAX_COMPONENTS];
	RfPageInstallStorageOpsV1 storage;
	RfPageInstallAuthorityOpsV1 authority;
	RfPageReplayBatchRequestV1 request;
} BatchCase;

static int apply_calls;
static int apply_fail_call;

static void
set_incarnation(uint8 incarnation[16], uint8 value)
{
	memset(incarnation, value, 16);
}

static void
init_page(char page[BLCKSZ], uint64 token, uint8 payload)
{
	PageHeader header;

	memset(page, payload, BLCKSZ);
	header = (PageHeader)page;
	memset(header, 0, SizeOfPageHeaderData);
	header->pd_lower = SizeOfPageHeaderData;
	header->pd_upper = BLCKSZ;
	header->pd_special = BLCKSZ;
	header->pd_pagesize_version = (BLCKSZ & 0xFF00) | PG_PAGE_LAYOUT_VERSION;
	header->pd_block_scn = token;
}

bool
rf_page_identity_valid_v1(const RfPageIdentityV1 *identity)
{
	return identity != NULL && identity->system_identifier != 0
		   && identity->locator.spcOid != InvalidOid && identity->locator.dbOid != InvalidOid
		   && identity->locator.relNumber != InvalidRelFileNumber
		   && identity->blockno != InvalidBlockNumber && identity->reserved_zero == 0;
}

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

bool
rf_page_version_equal_v1(const RfPageVersionV1 *left, const RfPageVersionV1 *right)
{
	return left != NULL && right != NULL && left->mutation_token == right->mutation_token
		   && memcmp(left->segment_incarnation, right->segment_incarnation, 16) == 0;
}

RfPageProofDetailV1
rf_page_detached_apply_v1(const RfDetachedRecordPlanV1 *plan, uint32 component_index,
						  const char old_page[BLCKSZ], char new_page[BLCKSZ])
{
	const RfDetachedComponentPlanV1 *component;

	apply_calls++;
	if (apply_fail_call != 0 && apply_calls == apply_fail_call)
		return RF_PAGE_PROOF_DETAIL_IMAGE_DECODE_FAILED;
	if (plan == NULL || !plan->preflight_complete || component_index >= plan->component_count)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	component = &plan->components[component_index];
	if (old_page != new_page)
		memcpy(new_page, old_page, BLCKSZ);
	if (PageIsNew((Page)new_page))
		init_page(new_page, component->result.mutation_token, 0x61);
	((PageHeader)new_page)->pd_block_scn = component->result.mutation_token;
	new_page[BLCKSZ - 1] = (char)component->result.mutation_token;
	return RF_PAGE_PROOF_DETAIL_OK;
}

static bool
storage_read(void *arg, uint32 index, const RfPageIdentityV1 *identity, char page[BLCKSZ],
			 bool *exists)
{
	BatchFixture *fixture = (BatchFixture *)arg;

	fixture->read_calls++;
	*exists = fixture->exists[index];
	if (*exists)
		memcpy(page, fixture->disk[index].data, BLCKSZ);
	else
		memset(page, 0, BLCKSZ);
	return true;
}

static bool
storage_write(void *arg, uint32 index, const RfPageIdentityV1 *identity, const char page[BLCKSZ],
			  bool extend)
{
	BatchFixture *fixture = (BatchFixture *)arg;

	fixture->write_calls++;
	fixture->last_extend = extend;
	memcpy(fixture->disk[index].data, page, BLCKSZ);
	fixture->exists[index] = true;
	return true;
}

static bool
storage_sync(void *arg, uint32 index, const RfPageIdentityV1 *identity)
{
	BatchFixture *fixture = (BatchFixture *)arg;

	fixture->sync_calls++;
	return true;
}

static uint16
storage_checksum(void *arg, const char page[BLCKSZ], BlockNumber blockno)
{
	return (uint16)(blockno + 1);
}

static bool
authority_identity(void *arg, const RfPageIdentityV1 *identity, const uint8 incarnation[16])
{
	return true;
}

static bool
authority_promote(void *arg)
{
	BatchFixture *fixture = (BatchFixture *)arg;

	fixture->promote_calls++;
	return true;
}

static bool
authority_publish(void *arg)
{
	BatchFixture *fixture = (BatchFixture *)arg;

	fixture->publish_calls++;
	return true;
}

static bool
authority_release(void *arg)
{
	BatchFixture *fixture = (BatchFixture *)arg;

	fixture->release_calls++;
	return true;
}

static void
init_plan(FakeRecord *record, RfDetachedRecordPlanV1 *plan, const RfPageIdentityV1 *identity,
		  uint8 before_kind, uint64 before_token, uint64 result_token, uint16 edge_flags,
		  uint32 record_ordinal)
{
	DecodedXLogRecord *decoded;
	DecodedBkpBlock *block;
	RfDetachedComponentPlanV1 *component;

	memset(record, 0, sizeof(*record));
	decoded = &record->storage.decoded;
	decoded->header.xl_rmid = RM_GENERIC_ID;
	decoded->header.xl_info = 0;
	decoded->header.xl_crc = 0xCAFE0000 + record_ordinal;
	decoded->max_block_id = 0;
	block = &decoded->blocks[0];
	block->in_use = true;
	block->rlocator = identity->locator;
	block->forknum = (ForkNumber)identity->forknum;
	block->blkno = identity->blockno;
	record->state.record = decoded;
	record->state.ReadRecPtr = 0x1100 + (XLogRecPtr)record_ordinal * 0x100;
	record->state.EndRecPtr = record->state.ReadRecPtr + 0x80;
	memset(plan, 0, sizeof(*plan));
	plan->source_record = &record->state;
	plan->result_token = result_token;
	plan->component_count = 1;
	plan->preflight_complete = true;
	component = &plan->components[0];
	component->block_id = 0;
	component->page_class = RF_PAGE_CLASS_ORDINARY;
	component->owner = RF_DETACHED_COMPONENT_PAGE_CODEC;
	component->codec_id = RF_ROUTE_CODEC_GENERIC;
	component->before_kind = before_kind;
	component->result_kind = RF_PAGE_STATE_PRESENT;
	component->edge_flags = edge_flags;
	if (before_kind != RF_PAGE_STATE_ABSENT)
		set_incarnation(component->before.segment_incarnation, 7);
	component->before.mutation_token = before_token;
	set_incarnation(component->result.segment_incarnation, 7);
	component->result.mutation_token = result_token;
}

static void
bind_record(RfPageReplayRecordV1 *record, const RfDetachedRecordPlanV1 *plan,
			uint16 participant_index, uint64 system_identifier, const uint8 storage_uuid[16],
			uint16 origin_thread, TimeLineID timeline_id)
{
	memset(record, 0, sizeof(*record));
	record->record_plan = plan;
	record->participant_index = participant_index;
	record->identity.system_identifier = system_identifier;
	memcpy(record->identity.storage_uuid, storage_uuid, 16);
	record->identity.origin_thread = origin_thread;
	record->identity.timeline_id = timeline_id;
	record->identity.read_rec_ptr = plan->source_record->ReadRecPtr;
	record->identity.end_rec_ptr = plan->source_record->EndRecPtr;
	record->identity.record_crc = plan->source_record->record->header.xl_crc;
	record->identity.rmid = plan->source_record->record->header.xl_rmid;
	record->identity.info = plan->source_record->record->header.xl_info;
}

static void
init_case(BatchCase *test_case, uint32 count)
{
	uint32 i;

	memset(test_case, 0, sizeof(*test_case));
	apply_calls = 0;
	apply_fail_call = 0;
	test_case->participants[0].failed_thread = 1;
	test_case->participants[0].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	test_case->participants[0].timeline_id = 1;
	test_case->participants[0].scan_begin_inclusive = 0x1000;
	test_case->participants[0].scan_end_exclusive = 0x2000;
	test_case->participants[0].contributor_count = count;
	test_case->participants[0].component_count = count;
	for (i = 0; i < count; i++) {
		RfPageReplayTargetV1 *target = &test_case->targets[i];

		target->page_identity.system_identifier = 99;
		memset(target->page_identity.storage_uuid, 3, 16);
		target->page_identity.locator.spcOid = 1;
		target->page_identity.locator.dbOid = 2;
		target->page_identity.locator.relNumber = 100 + i;
		target->page_identity.forknum = MAIN_FORKNUM;
		target->page_identity.blockno = i;
		target->before_kind = RF_PAGE_STATE_PRESENT;
		set_incarnation(target->expected_before.segment_incarnation, 7);
		target->expected_before.mutation_token = 10;
		set_incarnation(target->expected_result.segment_incarnation, 7);
		target->expected_result.mutation_token = 11;
		init_page(test_case->base[i].data, 10, (uint8)(0x20 + i));
		init_page(test_case->fixture.disk[i].data, 10, (uint8)(0x30 + i));
		test_case->fixture.exists[i] = true;
		target->base_page = test_case->base[i].data;
		init_plan(&test_case->records[i], &test_case->plans[i], &target->page_identity,
				  RF_PAGE_STATE_PRESENT, 10, 11, 0, i);
		bind_record(&test_case->replay_records[i], &test_case->plans[i], 0,
					target->page_identity.system_identifier, target->page_identity.storage_uuid, 1,
					1);
		test_case->steps[i][0].record_index = i;
		test_case->steps[i][0].component_index = 0;
		target->steps = test_case->steps[i];
		target->step_count = 1;
	}
	test_case->request.system_identifier = 99;
	memset(test_case->request.storage_uuid, 3, 16);
	test_case->request.participants = test_case->participants;
	test_case->request.participant_count = 1;
	test_case->request.records = test_case->replay_records;
	test_case->request.record_count = count;
	test_case->storage.arg = &test_case->fixture;
	test_case->storage.read = storage_read;
	test_case->storage.write = storage_write;
	test_case->storage.sync = storage_sync;
	test_case->storage.checksum = storage_checksum;
	test_case->authority.arg = &test_case->fixture;
	test_case->authority.validate_identity = authority_identity;
	test_case->authority.promote = authority_promote;
	test_case->authority.publish = authority_publish;
	test_case->authority.release = authority_release;
	test_case->request.targets = test_case->targets;
	test_case->request.target_count = count;
	test_case->request.record_component_seen = test_case->record_component_seen[0];
	test_case->request.record_component_seen_capacity = sizeof(test_case->record_component_seen);
	test_case->request.canonical_pages = test_case->canonical[0].data;
	test_case->request.canonical_capacity = sizeof(test_case->canonical);
	test_case->request.install_prepared_pages = test_case->prepared[0].data;
	test_case->request.install_prepared_capacity = sizeof(test_case->prepared);
	test_case->request.install_io_pages = test_case->io[0].data;
	test_case->request.install_io_capacity = sizeof(test_case->io);
	test_case->request.storage = &test_case->storage;
	test_case->request.authority = &test_case->authority;
	test_case->request.global_preflight_ok = true;
}

UT_TEST(test_two_targets_apply_then_install_as_one_proven_batch)
{
	BatchCase test_case;
	RfPageReplayBatchProofV1 proof;

	init_case(&test_case, 2);
	UT_ASSERT_EQ(rf_page_replay_batch_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(apply_calls, 2);
	UT_ASSERT_EQ(test_case.fixture.write_calls, 2);
	UT_ASSERT_EQ(test_case.fixture.sync_calls, 2);
	UT_ASSERT_EQ(test_case.fixture.read_calls, 4);
	UT_ASSERT_EQ(test_case.fixture.promote_calls, 1);
	UT_ASSERT_EQ(proof.step_count, 2);
	UT_ASSERT(proof.detached_apply_complete && proof.install.postread_complete);
}

UT_TEST(test_incomplete_sibling_plan_blocks_before_any_apply_or_target_read)
{
	BatchCase test_case;
	RfPageReplayBatchProofV1 proof;

	init_case(&test_case, 2);
	test_case.plans[1].preflight_complete = false;
	UT_ASSERT_EQ(rf_page_replay_batch_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE);
	UT_ASSERT_EQ(apply_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.read_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.promote_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.write_calls, 0);
}

UT_TEST(test_chain_gap_blocks_whole_batch_before_apply)
{
	BatchCase test_case;
	RfPageReplayBatchProofV1 proof;

	init_case(&test_case, 1);
	init_plan(&test_case.records[2], &test_case.plans[2], &test_case.targets[0].page_identity,
			  RF_PAGE_STATE_PRESENT, 12, 13, 0, 1);
	bind_record(&test_case.replay_records[1], &test_case.plans[2], 0, 99,
				test_case.request.storage_uuid, 1, 1);
	test_case.request.record_count = 2;
	test_case.participants[0].contributor_count = 2;
	test_case.participants[0].component_count = 2;
	test_case.steps[0][1].record_index = 1;
	test_case.targets[0].step_count = 2;
	test_case.targets[0].expected_result.mutation_token = 13;
	UT_ASSERT_EQ(rf_page_replay_batch_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE);
	UT_ASSERT_EQ(apply_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.read_calls, 0);
}

UT_TEST(test_detached_apply_failure_never_reaches_target_install)
{
	BatchCase test_case;
	RfPageReplayBatchProofV1 proof;

	init_case(&test_case, 2);
	apply_fail_call = 2;
	UT_ASSERT_EQ(rf_page_replay_batch_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_IMAGE_DECODE_FAILED);
	UT_ASSERT_EQ(apply_calls, 2);
	UT_ASSERT_EQ(test_case.fixture.read_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.promote_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.write_calls, 0);
}

UT_TEST(test_record_block_identity_mismatch_is_zero_apply)
{
	BatchCase test_case;
	RfPageReplayBatchProofV1 proof;

	init_case(&test_case, 1);
	test_case.records[0].storage.decoded.blocks[0].rlocator.relNumber++;
	UT_ASSERT_EQ(rf_page_replay_batch_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE);
	UT_ASSERT_EQ(apply_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.read_calls, 0);
}

UT_TEST(test_exact_record_identity_must_match_immutable_decoded_source)
{
	BatchCase test_case;
	RfPageReplayBatchProofV1 proof;

	init_case(&test_case, 1);
	test_case.replay_records[0].identity.record_crc++;
	UT_ASSERT_EQ(rf_page_replay_batch_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	UT_ASSERT_EQ(apply_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.read_calls, 0);

	init_case(&test_case, 1);
	test_case.replay_records[0].identity.info++;
	UT_ASSERT_EQ(rf_page_replay_batch_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	UT_ASSERT_EQ(apply_calls, 0);
}

UT_TEST(test_unreferenced_page_sibling_blocks_record_wide_install)
{
	BatchCase test_case;
	RfPageReplayBatchProofV1 proof;
	RfDetachedComponentPlanV1 *sibling;
	DecodedBkpBlock *block;

	init_case(&test_case, 1);
	test_case.plans[0].component_count = 2;
	test_case.participants[0].contributor_count = 2;
	test_case.participants[0].component_count = 2;
	test_case.records[0].storage.decoded.max_block_id = 1;
	sibling = &test_case.plans[0].components[1];
	*sibling = test_case.plans[0].components[0];
	sibling->block_id = 1;
	sibling->component_ordinal = 1;
	block = &test_case.records[0].storage.decoded.blocks[1];
	*block = test_case.records[0].storage.decoded.blocks[0];
	block->blkno = 1;
	block->component_ordinal = 1;
	UT_ASSERT_EQ(rf_page_replay_batch_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE);
	UT_ASSERT_EQ(apply_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.read_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.write_calls, 0);
}

UT_TEST(test_all_page_siblings_of_one_record_install_together)
{
	BatchCase test_case;
	RfPageReplayBatchProofV1 proof;
	RfDetachedComponentPlanV1 *sibling;
	DecodedBkpBlock *block;

	init_case(&test_case, 2);
	test_case.plans[0].component_count = 2;
	test_case.records[0].storage.decoded.max_block_id = 1;
	sibling = &test_case.plans[0].components[1];
	*sibling = test_case.plans[0].components[0];
	sibling->block_id = 1;
	sibling->component_ordinal = 1;
	block = &test_case.records[0].storage.decoded.blocks[1];
	*block = test_case.records[0].storage.decoded.blocks[0];
	block->rlocator = test_case.targets[1].page_identity.locator;
	block->blkno = test_case.targets[1].page_identity.blockno;
	block->component_ordinal = 1;
	test_case.steps[1][0].record_index = 0;
	test_case.steps[1][0].component_index = 1;
	test_case.request.record_count = 1;
	UT_ASSERT_EQ(rf_page_replay_batch_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(apply_calls, 2);
	UT_ASSERT_EQ(test_case.fixture.write_calls, 2);
	UT_ASSERT_EQ(proof.step_count, 2);
}

UT_TEST(test_result_source_with_zero_steps_uses_canonical_skip)
{
	BatchCase test_case;
	RfPageReplayBatchProofV1 proof;

	init_case(&test_case, 1);
	test_case.targets[0].expected_before = test_case.targets[0].expected_result;
	test_case.targets[0].steps = NULL;
	test_case.targets[0].step_count = 0;
	test_case.participants[0].flags = RF_CONTRIBUTOR_CUT_KNOWN_MASK;
	test_case.participants[0].scan_end_exclusive = test_case.participants[0].scan_begin_inclusive;
	test_case.participants[0].contributor_count = 0;
	test_case.participants[0].component_count = 0;
	test_case.request.records = NULL;
	test_case.request.record_count = 0;
	test_case.request.record_component_seen = NULL;
	test_case.request.record_component_seen_capacity = 0;
	init_page(test_case.base[0].data, 11, 0x44);
	memcpy(test_case.fixture.disk[0].data, test_case.base[0].data, BLCKSZ);
	UT_ASSERT_EQ(rf_page_replay_batch_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(apply_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.write_calls, 0);
	UT_ASSERT_EQ(proof.install.result_skip_count, 1);
}

UT_TEST(test_zero_record_table_requires_explicit_empty_participant_cut)
{
	BatchCase test_case;
	RfPageReplayBatchProofV1 proof;

	init_case(&test_case, 1);
	test_case.targets[0].expected_before = test_case.targets[0].expected_result;
	test_case.targets[0].steps = NULL;
	test_case.targets[0].step_count = 0;
	test_case.request.records = NULL;
	test_case.request.record_count = 0;
	test_case.request.record_component_seen = NULL;
	test_case.request.record_component_seen_capacity = 0;
	init_page(test_case.base[0].data, 11, 0x44);
	memcpy(test_case.fixture.disk[0].data, test_case.base[0].data, BLCKSZ);
	UT_ASSERT_EQ(rf_page_replay_batch_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING);
	UT_ASSERT_EQ(apply_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.read_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.write_calls, 0);
}

UT_TEST(test_targets_must_be_strict_canonical_identity_order)
{
	BatchCase test_case;
	RfPageReplayBatchProofV1 proof;

	init_case(&test_case, 2);
	test_case.targets[1].page_identity.locator.relNumber = 99;
	UT_ASSERT_EQ(rf_page_replay_batch_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_ORDER_VIOLATION);
	UT_ASSERT_EQ(apply_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.read_calls, 0);
}

UT_TEST(test_oversized_step_count_is_rejected_before_indexing)
{
	BatchCase test_case;
	RfPageReplayBatchProofV1 proof;

	init_case(&test_case, 1);
	test_case.targets[0].step_count = RF_PAGE_STABLE_MAX_EDGES + 1;
	UT_ASSERT_EQ(rf_page_replay_batch_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_CAPACITY);
	UT_ASSERT_EQ(apply_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.read_calls, 0);
}

UT_TEST(test_exact_two_step_chain_reaches_terminal_version)
{
	BatchCase test_case;
	RfPageReplayBatchProofV1 proof;

	init_case(&test_case, 1);
	init_plan(&test_case.records[2], &test_case.plans[2], &test_case.targets[0].page_identity,
			  RF_PAGE_STATE_PRESENT, 11, 12, 0, 1);
	bind_record(&test_case.replay_records[1], &test_case.plans[2], 0, 99,
				test_case.request.storage_uuid, 1, 1);
	test_case.request.record_count = 2;
	test_case.participants[0].contributor_count = 2;
	test_case.participants[0].component_count = 2;
	test_case.steps[0][1].record_index = 1;
	test_case.targets[0].step_count = 2;
	test_case.targets[0].expected_result.mutation_token = 12;
	UT_ASSERT_EQ(rf_page_replay_batch_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(apply_calls, 2);
	UT_ASSERT_EQ(((PageHeader)test_case.fixture.disk[0].data)->pd_block_scn, 12);
	UT_ASSERT_EQ(proof.step_count, 2);
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(test_two_targets_apply_then_install_as_one_proven_batch);
	UT_RUN(test_incomplete_sibling_plan_blocks_before_any_apply_or_target_read);
	UT_RUN(test_chain_gap_blocks_whole_batch_before_apply);
	UT_RUN(test_detached_apply_failure_never_reaches_target_install);
	UT_RUN(test_record_block_identity_mismatch_is_zero_apply);
	UT_RUN(test_exact_record_identity_must_match_immutable_decoded_source);
	UT_RUN(test_unreferenced_page_sibling_blocks_record_wide_install);
	UT_RUN(test_all_page_siblings_of_one_record_install_together);
	UT_RUN(test_result_source_with_zero_steps_uses_canonical_skip);
	UT_RUN(test_zero_record_table_requires_explicit_empty_participant_cut);
	UT_RUN(test_targets_must_be_strict_canonical_identity_order);
	UT_RUN(test_oversized_step_count_is_rejected_before_indexing);
	UT_RUN(test_exact_two_step_chain_reaches_terminal_version);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#endif
