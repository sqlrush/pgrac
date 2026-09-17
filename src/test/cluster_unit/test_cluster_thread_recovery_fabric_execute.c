/*-------------------------------------------------------------------------
 * test_cluster_thread_recovery_fabric_execute.c
 *    Root scan must complete before apply and always release the fabric.
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "cluster/cluster_thread_recovery_fabric.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

static char fabric_object;
static char authority_object;
static RfPageProofDetailV1 scan_detail;
static RfPageProofDetailV1 apply_detail;
static int step;
static int scan_step;
static int apply_step;
static int destroy_step;

RfPageProofDetailV1
cluster_thread_recovery_fabric_scan_root_v1(
	uint16 dead_thread, XLogRecPtr scan_begin, XLogRecPtr scan_end,
	const struct ClusterThreadRecoveryAuthorityV1 *authority, bool space_active,
	ClusterThreadRecoveryFabricPlanV1 **out_plan, uint64 *out_record_count)
{
	UT_ASSERT(dead_thread == 2 && scan_begin == 0x100 && scan_end == 0x200
			  && authority == (const struct ClusterThreadRecoveryAuthorityV1 *)&authority_object
			  && !space_active && out_plan != NULL && out_record_count != NULL);
	scan_step = ++step;
	if (scan_detail != RF_PAGE_PROOF_DETAIL_OK)
		return scan_detail;
	*out_plan = (ClusterThreadRecoveryFabricPlanV1 *)&fabric_object;
	*out_record_count = 4;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
cluster_thread_recovery_fabric_apply_v1(const ClusterThreadRecoveryFabricPlanV1 *plan,
										const struct ClusterThreadRecoveryAuthorityV1 *authority,
										ClusterThreadRecoveryFabricApplyResultV1 *result)
{
	UT_ASSERT(plan == (const ClusterThreadRecoveryFabricPlanV1 *)&fabric_object
			  && authority == (const struct ClusterThreadRecoveryAuthorityV1 *)&authority_object
			  && result != NULL);
	apply_step = ++step;
	if (apply_detail != RF_PAGE_PROOF_DETAIL_OK)
		return apply_detail;
	memset(result, 0, sizeof(*result));
	result->page_target_count = 3;
	result->page_write_count = 2;
	result->page_result_skip_count = 1;
	result->page_durability_complete = true;
	result->page_postread_complete = true;
	result->side_apply_complete = true;
	return RF_PAGE_PROOF_DETAIL_OK;
}

void
cluster_thread_recovery_fabric_plan_destroy_v1(ClusterThreadRecoveryFabricPlanV1 **plan)
{
	UT_ASSERT(plan != NULL && *plan == (ClusterThreadRecoveryFabricPlanV1 *)&fabric_object);
	destroy_step = ++step;
	*plan = NULL;
}

static void
init_case(void)
{
	scan_detail = RF_PAGE_PROOF_DETAIL_OK;
	apply_detail = RF_PAGE_PROOF_DETAIL_OK;
	step = scan_step = apply_step = destroy_step = 0;
}

UT_TEST(test_scan_then_apply_then_destroy_returns_only_complete_result)
{
	ClusterThreadRecoveryFabricApplyResultV1 result;
	uint64 records = 0;

	init_case();
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_execute_root_v1(
					 2, 0x100, 0x200,
					 (const struct ClusterThreadRecoveryAuthorityV1 *)&authority_object, false,
					 &result, &records),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(scan_step < apply_step && apply_step < destroy_step);
	UT_ASSERT_EQ(records, 4);
	UT_ASSERT_EQ(result.page_target_count, 3);
	UT_ASSERT_EQ(result.page_write_count, 2);
	UT_ASSERT_EQ(result.page_result_skip_count, 1);
	UT_ASSERT(result.page_durability_complete && result.page_postread_complete
			  && result.side_apply_complete);
}

UT_TEST(test_scan_failure_never_calls_apply_and_leaves_zero_outputs)
{
	ClusterThreadRecoveryFabricApplyResultV1 result;
	uint64 records = 9;

	init_case();
	scan_detail = RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	memset(&result, 0x7f, sizeof(result));
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_execute_root_v1(
					 2, 0x100, 0x200,
					 (const struct ClusterThreadRecoveryAuthorityV1 *)&authority_object, false,
					 &result, &records),
				 RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	UT_ASSERT(scan_step != 0 && apply_step == 0 && destroy_step == 0);
	UT_ASSERT_EQ(records, 0);
	UT_ASSERT_EQ(result.page_target_count, 0);
}

UT_TEST(test_apply_failure_destroys_plan_and_leaves_zero_outputs)
{
	ClusterThreadRecoveryFabricApplyResultV1 result;
	uint64 records = 9;

	init_case();
	apply_detail = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	memset(&result, 0x7f, sizeof(result));
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_execute_root_v1(
					 2, 0x100, 0x200,
					 (const struct ClusterThreadRecoveryAuthorityV1 *)&authority_object, false,
					 &result, &records),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT(scan_step < apply_step && apply_step < destroy_step);
	UT_ASSERT_EQ(records, 0);
	UT_ASSERT_EQ(result.page_target_count, 0);
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_scan_then_apply_then_destroy_returns_only_complete_result);
	UT_RUN(test_scan_failure_never_calls_apply_and_leaves_zero_outputs);
	UT_RUN(test_apply_failure_destroys_plan_and_leaves_zero_outputs);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
