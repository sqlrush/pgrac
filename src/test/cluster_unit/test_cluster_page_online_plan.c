/*-------------------------------------------------------------------------
 *
 * test_cluster_page_online_plan.c
 *    STOP-06 pre-IR immutable online PAGE plan contract.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "access/xlogreader.h"
#include "cluster/cluster_page_online_plan.h"
#include "storage/bufpage.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

static int fail_component = -1;

bool
rf_page_identity_valid_v1(const RfPageIdentityV1 *identity)
{
	return identity != NULL && identity->system_identifier != 0
		   && identity->locator.spcOid != InvalidOid && identity->locator.dbOid != InvalidOid
		   && identity->locator.relNumber != InvalidRelFileNumber
		   && identity->blockno != InvalidBlockNumber && identity->reserved_zero == 0;
}

bool
rf_page_identity_equal_v1(const RfPageIdentityV1 *left, const RfPageIdentityV1 *right)
{
	return left != NULL && right != NULL && left->system_identifier == right->system_identifier
		   && memcmp(left->storage_uuid, right->storage_uuid, 16) == 0
		   && RelFileLocatorEquals(left->locator, right->locator) && left->forknum == right->forknum
		   && left->blockno == right->blockno;
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
	PageHeader header;

	if ((int)component_index == fail_component)
		return RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED;
	component = &plan->components[component_index];
	memset(new_page, (int)component->result.mutation_token, BLCKSZ);
	header = (PageHeader)new_page;
	memset(header, 0, SizeOfPageHeaderData);
	header->pd_lower = SizeOfPageHeaderData;
	header->pd_upper = BLCKSZ;
	header->pd_special = BLCKSZ;
	header->pd_pagesize_version = (BLCKSZ & 0xFF00) | PG_PAGE_LAYOUT_VERSION;
	header->pd_block_scn = component->result.mutation_token;
	return RF_PAGE_PROOF_DETAIL_OK;
}

typedef struct FakeRecord {
	XLogReaderState reader;
	RfDetachedRecordPlanV1 plan;
	union {
		DecodedXLogRecord decoded;
		char padding[sizeof(DecodedXLogRecord)
					 + RF_PAGE_STABLE_MAX_COMPONENTS * sizeof(DecodedBkpBlock)];
	} storage;
} FakeRecord;

static void
set_version(RfPageVersionV1 *version, uint64 token)
{
	memset(version, 7, sizeof(version->segment_incarnation));
	version->mutation_token = token;
}

static void
init_record(FakeRecord *record, XLogRecPtr begin, XLogRecPtr end, uint32 count)
{
	memset(record, 0, sizeof(*record));
	record->reader.ReadRecPtr = begin;
	record->reader.EndRecPtr = end;
	record->reader.system_identifier = 99;
	record->reader.record = &record->storage.decoded;
	record->storage.decoded.lsn = begin;
	record->storage.decoded.next_lsn = end;
	record->storage.decoded.header.xl_crc = (pg_crc32c)(begin ^ end);
	record->storage.decoded.header.xl_rmid = RM_XLOG_ID;
	record->storage.decoded.header.xl_info = (uint8)(begin & 0xf0);
	record->storage.decoded.max_block_id = (int)count - 1;
	record->plan.source_record = &record->reader;
	record->plan.component_count = count;
	record->plan.preflight_complete = true;
}

static void
set_component(FakeRecord *record, uint32 index, Oid rel, BlockNumber block, uint64 before,
			  uint64 result, bool anchor)
{
	DecodedBkpBlock *decoded = &record->storage.decoded.blocks[index];
	RfDetachedComponentPlanV1 *component = &record->plan.components[index];

	decoded->in_use = true;
	decoded->rlocator.spcOid = 1;
	decoded->rlocator.dbOid = 2;
	decoded->rlocator.relNumber = rel;
	decoded->forknum = MAIN_FORKNUM;
	decoded->blkno = block;
	component->block_id = (uint8)index;
	component->page_class = RF_PAGE_CLASS_ORDINARY;
	component->owner = RF_DETACHED_COMPONENT_PAGE_CODEC;
	component->component_ordinal = (uint16)index;
	component->before_kind = RF_PAGE_STATE_PRESENT;
	component->result_kind = RF_PAGE_STATE_PRESENT;
	set_version(&component->before, before);
	set_version(&component->result, result);
	if (anchor)
		component->edge_flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
}

static RfPageOnlinePlanV1 *
create_plan(XLogRecPtr begin, XLogRecPtr end)
{
	RfContributorStreamCutV1 cut;
	RfPageOnlinePlanRequestV1 request;
	RfPageOnlinePlanV1 *plan = NULL;

	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 1;
	cut.timeline_id = 1;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	cut.scan_begin_inclusive = begin;
	cut.scan_end_exclusive = end;
	memset(&request, 0, sizeof(request));
	request.system_identifier = 99;
	memset(request.storage_uuid, 3, 16);
	request.physical_cuts = &cut;
	request.participant_count = 1;
	request.retention_binding_cookie = 41;
	UT_ASSERT_EQ(rf_page_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

static RfPageOnlineRecordIdentityV1
record_identity(const FakeRecord *record)
{
	RfPageOnlineRecordIdentityV1 identity;

	memset(&identity, 0, sizeof(identity));
	identity.record.system_identifier = 99;
	memset(identity.record.storage_uuid, 3, sizeof(identity.record.storage_uuid));
	identity.record.origin_thread = 1;
	identity.record.timeline_id = 1;
	identity.record.read_rec_ptr = record->reader.ReadRecPtr;
	identity.record.end_rec_ptr = record->reader.EndRecPtr;
	identity.record.record_crc = (uint32)record->storage.decoded.header.xl_crc;
	identity.record.rmid = record->storage.decoded.header.xl_rmid;
	identity.record.info = record->storage.decoded.header.xl_info;
	return identity;
}

UT_TEST(test_two_record_chain_builds_canonical_target)
{
	RfPageOnlinePlanV1 *plan = create_plan(0x100, 0x300);
	FakeRecord first;
	FakeRecord second;
	RfPageOnlineRecordIdentityV1 id;
	RfPageOnlineTargetViewV1 target;

	init_record(&first, 0x100, 0x200, 1);
	set_component(&first, 0, 10, 4, 10, 11, true);
	id = record_identity(&first);
	UT_ASSERT_EQ(rf_page_online_plan_feed_record_v1(plan, &first.plan, &id),
				 RF_PAGE_PROOF_DETAIL_OK);
	init_record(&second, 0x200, 0x300, 1);
	set_component(&second, 0, 10, 4, 11, 12, false);
	id = record_identity(&second);
	UT_ASSERT_EQ(rf_page_online_plan_feed_record_v1(plan, &second.plan, &id),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_page_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_page_online_plan_target_count_v1(plan), 1);
	UT_ASSERT(rf_page_online_plan_target_v1(plan, 0, &target));
	UT_ASSERT_EQ(target.expected_before.mutation_token, 10);
	UT_ASSERT_EQ(target.expected_result.mutation_token, 12);
	UT_ASSERT_EQ(((PageHeader)target.canonical_page)->pd_block_scn, 12);
	UT_ASSERT_EQ(target.source->source_version.mutation_token, 11);
	UT_ASSERT_EQ(target.contributors->edge_count, 2);
	UT_ASSERT_EQ(target.contributors->cuts[0].contributor_count, 2);
	UT_ASSERT_EQ(target.contributors->cuts[0].component_count, 2);
	rf_page_online_plan_destroy_v1(&plan);
}

UT_TEST(test_record_failure_is_atomic_and_retryable)
{
	RfPageOnlinePlanV1 *plan = create_plan(0x100, 0x200);
	FakeRecord record;
	RfPageOnlineRecordIdentityV1 id;
	RfPageOnlineTargetViewV1 first;
	RfPageOnlineTargetViewV1 second;

	init_record(&record, 0x100, 0x200, 2);
	set_component(&record, 0, 20, 1, 10, 11, true);
	set_component(&record, 1, 10, 1, 20, 21, true);
	id = record_identity(&record);
	fail_component = 1;
	UT_ASSERT_EQ(rf_page_online_plan_feed_record_v1(plan, &record.plan, &id),
				 RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED);
	fail_component = -1;
	UT_ASSERT_EQ(rf_page_online_plan_feed_record_v1(plan, &record.plan, &id),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_page_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_page_online_plan_target_count_v1(plan), 2);
	UT_ASSERT(rf_page_online_plan_target_v1(plan, 0, &first));
	UT_ASSERT(rf_page_online_plan_target_v1(plan, 1, &second));
	UT_ASSERT_EQ(first.page_identity.locator.relNumber, 10);
	UT_ASSERT_EQ(second.page_identity.locator.relNumber, 20);
	rf_page_online_plan_destroy_v1(&plan);
}

UT_TEST(test_first_delta_requires_full_anchor)
{
	RfPageOnlinePlanV1 *plan = create_plan(0x100, 0x200);
	FakeRecord record;
	RfPageOnlineRecordIdentityV1 id;

	init_record(&record, 0x100, 0x200, 1);
	set_component(&record, 0, 10, 1, 10, 11, false);
	id = record_identity(&record);
	UT_ASSERT_EQ(rf_page_online_plan_feed_record_v1(plan, &record.plan, &id),
				 RF_PAGE_PROOF_DETAIL_ANCHOR_MISSING);
	set_component(&record, 0, 10, 1, 10, 11, true);
	UT_ASSERT_EQ(rf_page_online_plan_feed_record_v1(plan, &record.plan, &id),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_page_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	rf_page_online_plan_destroy_v1(&plan);
}

UT_TEST(test_edge_gap_does_not_advance_stream)
{
	RfPageOnlinePlanV1 *plan = create_plan(0x100, 0x300);
	FakeRecord first;
	FakeRecord second;
	RfPageOnlineRecordIdentityV1 id;

	init_record(&first, 0x100, 0x200, 1);
	set_component(&first, 0, 10, 1, 10, 11, true);
	id = record_identity(&first);
	UT_ASSERT_EQ(rf_page_online_plan_feed_record_v1(plan, &first.plan, &id),
				 RF_PAGE_PROOF_DETAIL_OK);
	init_record(&second, 0x200, 0x300, 1);
	set_component(&second, 0, 10, 1, 99, 12, false);
	id = record_identity(&second);
	UT_ASSERT_EQ(rf_page_online_plan_feed_record_v1(plan, &second.plan, &id),
				 RF_PAGE_PROOF_DETAIL_EDGE_GAP);
	set_component(&second, 0, 10, 1, 11, 12, false);
	UT_ASSERT_EQ(rf_page_online_plan_feed_record_v1(plan, &second.plan, &id),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_page_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	rf_page_online_plan_destroy_v1(&plan);
}

UT_TEST(test_seal_requires_complete_physical_cut)
{
	RfPageOnlinePlanV1 *plan = create_plan(0x100, 0x300);
	FakeRecord record;
	RfPageOnlineRecordIdentityV1 id;

	init_record(&record, 0x100, 0x200, 1);
	set_component(&record, 0, 10, 1, 10, 11, true);
	id = record_identity(&record);
	UT_ASSERT_EQ(rf_page_online_plan_feed_record_v1(plan, &record.plan, &id),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_page_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	rf_page_online_plan_destroy_v1(&plan);
}

UT_TEST(test_record_identity_binds_full_decoded_tuple)
{
	RfPageOnlinePlanV1 *plan = create_plan(0x100, 0x200);
	FakeRecord record;
	RfPageOnlineRecordIdentityV1 valid;
	RfPageOnlineRecordIdentityV1 changed;
	pg_crc32c saved_crc;

	init_record(&record, 0x100, 0x200, 1);
	set_component(&record, 0, 10, 1, 10, 11, true);
	valid = record_identity(&record);

#define ASSERT_IDENTITY_REJECTED(statement_)                                                       \
	do {                                                                                           \
		changed = valid;                                                                           \
		statement_;                                                                                \
		UT_ASSERT_EQ(rf_page_online_plan_feed_record_v1(plan, &record.plan, &changed),             \
					 RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);                                      \
	} while (0)
	ASSERT_IDENTITY_REJECTED(changed.record.system_identifier++);
	ASSERT_IDENTITY_REJECTED(changed.record.storage_uuid[0]++);
	ASSERT_IDENTITY_REJECTED(changed.record.origin_thread++);
	ASSERT_IDENTITY_REJECTED(changed.record.timeline_id++);
	ASSERT_IDENTITY_REJECTED(changed.record.record_crc++);
	ASSERT_IDENTITY_REJECTED(changed.record.rmid++);
	ASSERT_IDENTITY_REJECTED(changed.record.info++);
#undef ASSERT_IDENTITY_REJECTED

	saved_crc = record.storage.decoded.header.xl_crc;
	record.storage.decoded.header.xl_crc++;
	UT_ASSERT_EQ(rf_page_online_plan_feed_record_v1(plan, &record.plan, &valid),
				 RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
	record.storage.decoded.header.xl_crc = saved_crc;
	record.storage.decoded.next_lsn++;
	UT_ASSERT_EQ(rf_page_online_plan_feed_record_v1(plan, &record.plan, &valid),
				 RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
	record.storage.decoded.next_lsn--;

	UT_ASSERT_EQ(rf_page_online_plan_feed_record_v1(plan, &record.plan, &valid),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_page_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	rf_page_online_plan_destroy_v1(&plan);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_two_record_chain_builds_canonical_target);
	UT_RUN(test_record_failure_is_atomic_and_retryable);
	UT_RUN(test_first_delta_requires_full_anchor);
	UT_RUN(test_edge_gap_does_not_advance_stream);
	UT_RUN(test_seal_requires_complete_physical_cut);
	UT_RUN(test_record_identity_binds_full_decoded_tuple);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
