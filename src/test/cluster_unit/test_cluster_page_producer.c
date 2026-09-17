/*-------------------------------------------------------------------------
 *
 * test_cluster_page_producer.c
 *    STOP-06 mutation-token and PageVersion producer-batch contract.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xloginsert.h"

#if defined(__has_include)
#if __has_include("cluster/cluster_page_producer.h")
#include "cluster/cluster_page_producer.h"
#define TEST_HAVE_CLUSTER_PAGE_PRODUCER 1
#endif
#endif

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# Assert failed: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

#ifndef TEST_HAVE_CLUSTER_PAGE_PRODUCER

UT_TEST(test_page_producer_capability_red)
{
	printf("# JIT_SEMANTIC_RED:D6-PAGE-PRODUCER-BATCH\n");
	UT_ASSERT(false);
}

int
main(void)
{
	UT_PLAN(1);
	UT_RUN(test_page_producer_capability_red);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#else

static SCN next_token = 101;
static SCN observed_token;
static uint32 advance_calls;
static uint32 observe_calls;
static uint32 register_calls;
static uint64 registered_token;
static uint8 registered_count;
static RfPageVersionEdgeEntryV1 registered_entries[RF_PAGE_PRODUCER_MAX_COMPONENTS];

SCN
cluster_scn_advance(void)
{
	advance_calls++;
	return next_token;
}

void
cluster_scn_observe(SCN token)
{
	observe_calls++;
	observed_token = token;
}

void
XLogRegisterPageVersionEdge(uint64 result_token, const RfPageVersionEdgeEntryV1 *entries,
							uint8 entry_count)
{
	register_calls++;
	registered_token = result_token;
	registered_count = entry_count;
	memcpy(registered_entries, entries, sizeof(entries[0]) * entry_count);
}

static void
reset_allocator(SCN token)
{
	next_token = token;
	observed_token = InvalidScn;
	advance_calls = 0;
	observe_calls = 0;
	register_calls = 0;
	registered_token = 0;
	registered_count = 0;
	memset(registered_entries, 0, sizeof(registered_entries));
}

static void
make_page(PGAlignedBlock *block, uint64 token)
{
	PageHeader header;

	memset(block, 0, sizeof(*block));
	header = (PageHeader)block->data;
	if (token != 0) {
		header->pd_lower = SizeOfPageHeaderData;
		header->pd_upper = BLCKSZ;
		header->pd_special = BLCKSZ;
		header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	}
	header->pd_block_scn = (SCN)token;
}

static void
set_incarnation(uint8 incarnation[16], uint8 seed)
{
	int i;

	for (i = 0; i < 16; i++)
		incarnation[i] = seed + i;
}

static RfPageProducerComponentV1
ordinary_component(uint8 block_id, uint8 before_kind, Page page, uint8 seed)
{
	RfPageProducerComponentV1 component;

	memset(&component, 0, sizeof(component));
	component.block_id = block_id;
	component.page_class = RF_PAGE_CLASS_ORDINARY;
	component.before_kind = before_kind;
	component.component_ordinal = block_id;
	component.page = page;
	set_incarnation(component.segment_incarnation, seed);
	return component;
}

UT_TEST(test_token_next_delegates_once)
{
	reset_allocator((SCN)901);
	UT_ASSERT_EQ(rf_page_mutation_token_next(), UINT64_C(901));
	UT_ASSERT_EQ(advance_calls, 1);
}

UT_TEST(test_token_observe_delegates_exact_value)
{
	reset_allocator((SCN)1);
	rf_page_mutation_token_observe(UINT64_C(777));
	UT_ASSERT_EQ(observe_calls, 1);
	UT_ASSERT_EQ(observed_token, (SCN)777);
}

UT_TEST(test_prepare_captures_before_without_mutation)
{
	PGAlignedBlock page;
	RfPageProducerComponentV1 component;
	RfPageProducerBatchV1 batch;

	make_page(&page, 40);
	component = ordinary_component(0, RF_PAGE_STATE_PRESENT, page.data, 3);
	reset_allocator((SCN)41);
	UT_ASSERT(rf_page_producer_prepare_v1(&component, 1, &batch));
	UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, (SCN)40);
	UT_ASSERT_EQ(batch.entries[0].before.mutation_token, UINT64_C(40));
	UT_ASSERT_EQ(batch.result_token, UINT64_C(41));
	UT_ASSERT(!batch.stamped);
}

UT_TEST(test_stamp_installs_result_token)
{
	PGAlignedBlock page;
	RfPageProducerComponentV1 component;
	RfPageProducerBatchV1 batch;

	make_page(&page, 40);
	component = ordinary_component(0, RF_PAGE_STATE_PRESENT, page.data, 3);
	reset_allocator((SCN)41);
	UT_ASSERT(rf_page_producer_prepare_v1(&component, 1, &batch));
	UT_ASSERT(rf_page_producer_stamp_v1(&batch));
	UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, (SCN)41);
	UT_ASSERT(batch.stamped);
}

UT_TEST(test_multiblock_batch_uses_one_token)
{
	PGAlignedBlock pages[3];
	RfPageProducerComponentV1 components[3];
	RfPageProducerBatchV1 batch;
	int i;

	for (i = 0; i < 3; i++) {
		make_page(&pages[i], 10 + i);
		components[i] = ordinary_component(i, RF_PAGE_STATE_PRESENT, pages[i].data, 8);
	}
	reset_allocator((SCN)88);
	UT_ASSERT(rf_page_producer_prepare_v1(components, 3, &batch));
	UT_ASSERT_EQ(advance_calls, 1);
	UT_ASSERT(rf_page_producer_stamp_v1(&batch));
	for (i = 0; i < 3; i++)
		UT_ASSERT_EQ(((PageHeader)pages[i].data)->pd_block_scn, (SCN)88);
}

UT_TEST(test_routed_component_has_zero_version_and_no_page)
{
	RfPageProducerComponentV1 component;
	RfPageProducerBatchV1 batch;

	memset(&component, 0, sizeof(component));
	component.page_class = RF_PAGE_CLASS_ROUTED_SIDE;
	component.before_kind = RF_PAGE_STATE_ROUTED;
	reset_allocator((SCN)55);
	UT_ASSERT(rf_page_producer_prepare_v1(&component, 1, &batch));
	UT_ASSERT_EQ(batch.entries[0].result_kind, RF_PAGE_STATE_ROUTED);
	UT_ASSERT_EQ(batch.entries[0].before.mutation_token, 0);
	UT_ASSERT(rf_page_producer_stamp_v1(&batch));
}

UT_TEST(test_invalid_count_never_allocates_token)
{
	RfPageProducerBatchV1 batch;

	memset(&batch, 0x5a, sizeof(batch));
	reset_allocator((SCN)60);
	UT_ASSERT(!rf_page_producer_prepare_v1(NULL, 0, &batch));
	UT_ASSERT_EQ(advance_calls, 0);
	UT_ASSERT_EQ(((unsigned char *)&batch)[0], 0x5a);
}

UT_TEST(test_unsorted_block_ids_fail_before_token)
{
	PGAlignedBlock pages[2];
	RfPageProducerComponentV1 components[2];
	RfPageProducerBatchV1 batch;

	make_page(&pages[0], 1);
	make_page(&pages[1], 2);
	components[0] = ordinary_component(1, RF_PAGE_STATE_PRESENT, pages[0].data, 2);
	components[0].component_ordinal = 0;
	components[1] = ordinary_component(0, RF_PAGE_STATE_PRESENT, pages[1].data, 2);
	components[1].component_ordinal = 1;
	reset_allocator((SCN)3);
	UT_ASSERT(!rf_page_producer_prepare_v1(components, 2, &batch));
	UT_ASSERT_EQ(advance_calls, 0);
}

UT_TEST(test_duplicate_page_fails_before_token)
{
	PGAlignedBlock page;
	RfPageProducerComponentV1 components[2];
	RfPageProducerBatchV1 batch;

	make_page(&page, 1);
	components[0] = ordinary_component(0, RF_PAGE_STATE_PRESENT, page.data, 2);
	components[1] = ordinary_component(1, RF_PAGE_STATE_PRESENT, page.data, 2);
	reset_allocator((SCN)3);
	UT_ASSERT(!rf_page_producer_prepare_v1(components, 2, &batch));
	UT_ASSERT_EQ(advance_calls, 0);
}

UT_TEST(test_zero_incarnation_fails_before_token)
{
	PGAlignedBlock page;
	RfPageProducerComponentV1 component;
	RfPageProducerBatchV1 batch;

	make_page(&page, 1);
	component = ordinary_component(0, RF_PAGE_STATE_PRESENT, page.data, 2);
	memset(component.segment_incarnation, 0, 16);
	reset_allocator((SCN)3);
	UT_ASSERT(!rf_page_producer_prepare_v1(&component, 1, &batch));
	UT_ASSERT_EQ(advance_calls, 0);
}

UT_TEST(test_zero_allocator_result_leaves_page_and_output_untouched)
{
	PGAlignedBlock page;
	RfPageProducerComponentV1 component;
	RfPageProducerBatchV1 batch;

	make_page(&page, 9);
	component = ordinary_component(0, RF_PAGE_STATE_PRESENT, page.data, 2);
	memset(&batch, 0x6b, sizeof(batch));
	reset_allocator(InvalidScn);
	UT_ASSERT(!rf_page_producer_prepare_v1(&component, 1, &batch));
	UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, (SCN)9);
	UT_ASSERT_EQ(((unsigned char *)&batch)[0], 0x6b);
}

UT_TEST(test_stamp_drift_is_zero_mutation_for_whole_batch)
{
	PGAlignedBlock pages[2];
	RfPageProducerComponentV1 components[2];
	RfPageProducerBatchV1 batch;

	make_page(&pages[0], 10);
	make_page(&pages[1], 20);
	components[0] = ordinary_component(0, RF_PAGE_STATE_PRESENT, pages[0].data, 2);
	components[1] = ordinary_component(1, RF_PAGE_STATE_PRESENT, pages[1].data, 2);
	reset_allocator((SCN)30);
	UT_ASSERT(rf_page_producer_prepare_v1(components, 2, &batch));
	((PageHeader)pages[1].data)->pd_block_scn = (SCN)21;
	UT_ASSERT(!rf_page_producer_stamp_v1(&batch));
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, (SCN)10);
	UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, (SCN)21);
}

UT_TEST(test_absent_and_unformatted_before_shapes)
{
	PGAlignedBlock pages[2];
	RfPageProducerComponentV1 components[2];
	RfPageProducerBatchV1 batch;
	int i;

	make_page(&pages[0], 0);
	make_page(&pages[1], 0);
	components[0] = ordinary_component(0, RF_PAGE_STATE_ABSENT, pages[0].data, 4);
	components[1] = ordinary_component(1, RF_PAGE_STATE_UNFORMATTED, pages[1].data, 4);
	reset_allocator((SCN)44);
	UT_ASSERT(rf_page_producer_prepare_v1(components, 2, &batch));
	for (i = 0; i < 16; i++)
		UT_ASSERT_EQ(batch.entries[0].before.segment_incarnation[i], 0);
	UT_ASSERT(
		memcmp(batch.entries[1].before.segment_incarnation, components[1].segment_incarnation, 16)
		== 0);
	UT_ASSERT(rf_page_producer_stamp_v1(&batch));
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, (SCN)44);
	UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, (SCN)44);
}

UT_TEST(test_stamp_retry_is_idempotent)
{
	PGAlignedBlock page;
	RfPageProducerComponentV1 component;
	RfPageProducerBatchV1 batch;

	make_page(&page, 7);
	component = ordinary_component(0, RF_PAGE_STATE_PRESENT, page.data, 5);
	reset_allocator((SCN)8);
	UT_ASSERT(rf_page_producer_prepare_v1(&component, 1, &batch));
	UT_ASSERT(rf_page_producer_stamp_v1(&batch));
	UT_ASSERT(rf_page_producer_stamp_v1(&batch));
	UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, (SCN)8);
}

UT_TEST(test_wal_registration_requires_stamped_batch)
{
	PGAlignedBlock page;
	RfPageProducerComponentV1 component;
	RfPageProducerBatchV1 batch;

	make_page(&page, 7);
	component = ordinary_component(0, RF_PAGE_STATE_PRESENT, page.data, 5);
	reset_allocator((SCN)8);
	UT_ASSERT(rf_page_producer_prepare_v1(&component, 1, &batch));
	UT_ASSERT(!rf_page_producer_register_wal_v1(&batch));
	UT_ASSERT_EQ(register_calls, 0);
}

UT_TEST(test_wal_registration_forwards_exact_batch)
{
	PGAlignedBlock page;
	RfPageProducerComponentV1 component;
	RfPageProducerBatchV1 batch;

	make_page(&page, 7);
	component = ordinary_component(0, RF_PAGE_STATE_PRESENT, page.data, 5);
	reset_allocator((SCN)8);
	UT_ASSERT(rf_page_producer_prepare_v1(&component, 1, &batch));
	UT_ASSERT(rf_page_producer_stamp_v1(&batch));
	UT_ASSERT(rf_page_producer_register_wal_v1(&batch));
	UT_ASSERT_EQ(register_calls, 1);
	UT_ASSERT_EQ(registered_token, UINT64_C(8));
	UT_ASSERT_EQ(registered_count, 1);
	UT_ASSERT(memcmp(&registered_entries[0], &batch.entries[0], sizeof(batch.entries[0])) == 0);
}

UT_TEST(test_wal_registration_revalidates_stamped_pages)
{
	PGAlignedBlock page;
	RfPageProducerComponentV1 component;
	RfPageProducerBatchV1 batch;

	make_page(&page, 7);
	component = ordinary_component(0, RF_PAGE_STATE_PRESENT, page.data, 5);
	reset_allocator((SCN)8);
	UT_ASSERT(rf_page_producer_prepare_v1(&component, 1, &batch));
	UT_ASSERT(rf_page_producer_stamp_v1(&batch));
	((PageHeader)page.data)->pd_block_scn = (SCN)9;
	UT_ASSERT(!rf_page_producer_register_wal_v1(&batch));
	UT_ASSERT_EQ(register_calls, 0);
}

int
main(void)
{
	UT_PLAN(17);
	UT_RUN(test_token_next_delegates_once);
	UT_RUN(test_token_observe_delegates_exact_value);
	UT_RUN(test_prepare_captures_before_without_mutation);
	UT_RUN(test_stamp_installs_result_token);
	UT_RUN(test_multiblock_batch_uses_one_token);
	UT_RUN(test_routed_component_has_zero_version_and_no_page);
	UT_RUN(test_invalid_count_never_allocates_token);
	UT_RUN(test_unsorted_block_ids_fail_before_token);
	UT_RUN(test_duplicate_page_fails_before_token);
	UT_RUN(test_zero_incarnation_fails_before_token);
	UT_RUN(test_zero_allocator_result_leaves_page_and_output_untouched);
	UT_RUN(test_stamp_drift_is_zero_mutation_for_whole_batch);
	UT_RUN(test_absent_and_unformatted_before_shapes);
	UT_RUN(test_stamp_retry_is_idempotent);
	UT_RUN(test_wal_registration_requires_stamped_batch);
	UT_RUN(test_wal_registration_forwards_exact_batch);
	UT_RUN(test_wal_registration_revalidates_stamped_pages);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#endif
