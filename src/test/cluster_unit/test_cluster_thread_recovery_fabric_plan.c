/*-------------------------------------------------------------------------
 * test_cluster_thread_recovery_fabric_plan.c
 *    RF ROOT immutable PAGE/SIDE planning barrier.
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

static char page_plan_object;
static char side_plan_object;
static int step;
static int detached_step;
static int page_feed_step;
static int side_feed_step;
static int page_seal_step;
static int side_seal_step;
static bool fail_page_feed;
static bool fail_side_feed;
static RfPageOnlineRecordIdentityV1 observed_identity;

RfPageProofDetailV1
rf_page_online_plan_create_v1(const RfPageOnlinePlanRequestV1 *request,
							  RfPageOnlinePlanV1 **out_plan)
{
	UT_ASSERT(request != NULL && request->system_identifier == 99 && request->participant_count == 1
			  && request->retention_binding_cookie == 41);
	*out_plan = (RfPageOnlinePlanV1 *)&page_plan_object;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_side_online_plan_create_v1(const RfSideOnlinePlanRequestV1 *request,
							  RfSideOnlinePlanV1 **out_plan)
{
	UT_ASSERT(request != NULL && request->system_identifier == 99
			  && request->participant_count == 1);
	*out_plan = (RfSideOnlinePlanV1 *)&side_plan_object;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_page_detached_preflight_v1(XLogReaderState *record, bool space_active,
							  const RfDetachedOwnerOpsV1 *owner_ops, RfDetachedRecordPlanV1 *plan)
{
	RfOpcodeRouteV1 route;

	UT_ASSERT(!space_active && owner_ops != NULL && owner_ops->preflight_side_record != NULL
			  && owner_ops->preflight_side_component != NULL
			  && owner_ops->preflight_rebuildable_component != NULL);
	memset(&route, 0, sizeof(route));
	route.record_owner = RF_ROUTE_OWNER_LOGICAL_NOOP;
	UT_ASSERT_EQ(owner_ops->preflight_side_record(owner_ops->arg, &route, NULL, NULL),
				 RF_PAGE_PROOF_DETAIL_OK);
	route.record_owner = RF_ROUTE_OWNER_SIDE_TYPED;
	UT_ASSERT_EQ(owner_ops->preflight_side_record(owner_ops->arg, &route, NULL, NULL),
				 RF_PAGE_PROOF_DETAIL_OK);
	route.record_owner = RF_ROUTE_OWNER_INVALID;
	UT_ASSERT_EQ(owner_ops->preflight_side_record(owner_ops->arg, &route, NULL, NULL),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	memset(plan, 0, sizeof(*plan));
	plan->source_record = record;
	plan->route.record_owner = RF_ROUTE_OWNER_LOGICAL_NOOP;
	plan->preflight_complete = true;
	detached_step = ++step;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_page_online_plan_feed_record_v1(RfPageOnlinePlanV1 *plan,
								   const RfDetachedRecordPlanV1 *record_plan,
								   const RfPageOnlineRecordIdentityV1 *identity)
{
	UT_ASSERT(plan == (RfPageOnlinePlanV1 *)&page_plan_object && record_plan != NULL
			  && record_plan->preflight_complete);
	observed_identity = *identity;
	page_feed_step = ++step;
	return fail_page_feed ? RF_PAGE_PROOF_DETAIL_EDGE_GAP : RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_side_online_plan_feed_record_v1(RfSideOnlinePlanV1 *plan,
								   const RfDetachedRecordPlanV1 *record_plan,
								   const RfPageOnlineRecordIdentityV1 *identity)
{
	UT_ASSERT(plan == (RfSideOnlinePlanV1 *)&side_plan_object && record_plan != NULL
			  && identity != NULL);
	side_feed_step = ++step;
	return fail_side_feed ? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_page_online_plan_seal_v1(RfPageOnlinePlanV1 *plan)
{
	UT_ASSERT(plan == (RfPageOnlinePlanV1 *)&page_plan_object);
	page_seal_step = ++step;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_side_online_plan_seal_v1(RfSideOnlinePlanV1 *plan)
{
	UT_ASSERT(plan == (RfSideOnlinePlanV1 *)&side_plan_object);
	side_seal_step = ++step;
	return RF_PAGE_PROOF_DETAIL_OK;
}

void
rf_page_online_plan_destroy_v1(RfPageOnlinePlanV1 **plan)
{
	*plan = NULL;
}

void
rf_side_online_plan_destroy_v1(RfSideOnlinePlanV1 **plan)
{
	*plan = NULL;
}

static void
reset_steps(void)
{
	step = detached_step = page_feed_step = side_feed_step = 0;
	page_seal_step = side_seal_step = 0;
	fail_page_feed = fail_side_feed = false;
	memset(&observed_identity, 0, sizeof(observed_identity));
}

static ClusterThreadRecoveryFabricPlanV1 *
create_plan(void)
{
	ClusterThreadRecoveryFabricPlanRequestV1 request;
	ClusterThreadRecoveryFabricPlanV1 *plan = NULL;
	static RfContributorStreamCutV1 cut;

	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 2;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 0x100;
	cut.scan_end_exclusive = 0x200;
	memset(&request, 0, sizeof(request));
	request.system_identifier = 99;
	memset(request.storage_uuid, 3, 16);
	request.physical_cuts = &cut;
	request.participant_count = 1;
	request.retention_binding_cookie = 41;
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_plan_create_v1(&request, &plan),
				 RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

static void
init_reader(XLogReaderState *reader, DecodedXLogRecord *decoded)
{
	memset(reader, 0, sizeof(*reader));
	memset(decoded, 0, sizeof(*decoded));
	reader->record = decoded;
	reader->system_identifier = 99;
	reader->ReadRecPtr = 0x100;
	reader->EndRecPtr = 0x200;
	decoded->lsn = reader->ReadRecPtr;
	decoded->next_lsn = reader->EndRecPtr;
	decoded->header.xl_crc = (pg_crc32c)0x12345678;
	decoded->header.xl_rmid = RM_XLOG_ID;
	decoded->header.xl_info = 0x50;
}

UT_TEST(test_record_is_preflighted_then_fed_to_page_and_side)
{
	ClusterThreadRecoveryFabricPlanV1 *plan = create_plan();
	XLogReaderState reader;
	DecodedXLogRecord decoded;

	reset_steps();
	init_reader(&reader, &decoded);
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_plan_feed_record_v1(plan, &reader, 0),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(detached_step < page_feed_step && page_feed_step < side_feed_step);
	UT_ASSERT_EQ(observed_identity.record.system_identifier, 99);
	UT_ASSERT_EQ(observed_identity.record.origin_thread, 2);
	UT_ASSERT_EQ(observed_identity.record.timeline_id, 7);
	UT_ASSERT_EQ(observed_identity.record.read_rec_ptr, 0x100);
	UT_ASSERT_EQ(observed_identity.record.end_rec_ptr, 0x200);
	UT_ASSERT_EQ(observed_identity.record.record_crc, 0x12345678);
	UT_ASSERT_EQ(observed_identity.record.rmid, RM_XLOG_ID);
	UT_ASSERT_EQ(observed_identity.record.info, 0x50);
	cluster_thread_recovery_fabric_plan_destroy_v1(&plan);
	UT_ASSERT(plan == NULL);
}

UT_TEST(test_page_failure_prevents_side_and_poisons_plan)
{
	ClusterThreadRecoveryFabricPlanV1 *plan = create_plan();
	XLogReaderState reader;
	DecodedXLogRecord decoded;

	reset_steps();
	init_reader(&reader, &decoded);
	fail_page_feed = true;
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_plan_feed_record_v1(plan, &reader, 0),
				 RF_PAGE_PROOF_DETAIL_EDGE_GAP);
	UT_ASSERT_EQ(side_feed_step, 0);
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_plan_seal_v1(plan),
				 RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE);
	cluster_thread_recovery_fabric_plan_destroy_v1(&plan);
}

UT_TEST(test_side_failure_poisons_whole_plan)
{
	ClusterThreadRecoveryFabricPlanV1 *plan = create_plan();
	XLogReaderState reader;
	DecodedXLogRecord decoded;

	reset_steps();
	init_reader(&reader, &decoded);
	fail_side_feed = true;
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_plan_feed_record_v1(plan, &reader, 0),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT(page_feed_step != 0 && side_feed_step != 0);
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_plan_seal_v1(plan),
				 RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE);
	cluster_thread_recovery_fabric_plan_destroy_v1(&plan);
}

UT_TEST(test_seal_closes_page_before_side_and_exposes_both)
{
	ClusterThreadRecoveryFabricPlanV1 *plan = create_plan();
	XLogReaderState reader;
	DecodedXLogRecord decoded;

	reset_steps();
	init_reader(&reader, &decoded);
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_plan_feed_record_v1(plan, &reader, 0),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(page_seal_step < side_seal_step);
	UT_ASSERT(cluster_thread_recovery_fabric_page_plan_v1(plan)
			  == (RfPageOnlinePlanV1 *)&page_plan_object);
	UT_ASSERT(cluster_thread_recovery_fabric_side_plan_v1(plan)
			  == (RfSideOnlinePlanV1 *)&side_plan_object);
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_participant_count_v1(plan), 1);
	UT_ASSERT(cluster_thread_recovery_fabric_identity_matches_v1(
		plan, 99, (const uint8[16]){ 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3 }));
	{
		RfContributorStreamCutV1 cut;

		UT_ASSERT(cluster_thread_recovery_fabric_cut_v1(plan, 0, &cut));
		UT_ASSERT_EQ(cut.failed_thread, 2);
		UT_ASSERT_EQ(cut.timeline_id, 7);
		UT_ASSERT_EQ(cut.scan_begin_inclusive, 0x100);
		UT_ASSERT_EQ(cut.scan_end_exclusive, 0x200);
	}
	cluster_thread_recovery_fabric_plan_destroy_v1(&plan);
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_record_is_preflighted_then_fed_to_page_and_side);
	UT_RUN(test_page_failure_prevents_side_and_poisons_plan);
	UT_RUN(test_side_failure_poisons_whole_plan);
	UT_RUN(test_seal_closes_page_before_side_and_exposes_both);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
