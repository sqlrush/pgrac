/*-------------------------------------------------------------------------
 * test_cluster_cold_native_side.h
 *     Opaque original sealed SIDE plan for Startup/native storage tests.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_native_side.h
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_COLD_NATIVE_SIDE_H
#define TEST_CLUSTER_COLD_NATIVE_SIDE_H
#include "cluster/cluster_side_online_plan.h"

typedef struct TestColdNativeSide TestColdNativeSide;
extern TestColdNativeSide *test_cold_native_side_create(const ClusterWalSourceRef *sources,
	uint32 count, const RfContributorStreamCutV1 *cut, XLogRecPtr redo,
	XLogReaderState *reader);
extern void test_cold_native_side_destroy(TestColdNativeSide **side);
extern bool test_cold_native_side_matches(const TestColdNativeSide *side,
	const ClusterWalSourceRef *source, const XLogReaderState *reader);
extern bool test_cold_native_side_replay(const TestColdNativeSide *side,
	const ClusterWalSourceRef *source, XLogReaderState *reader,
	void (*redo)(XLogReaderState *, void *), bool (*current)(void *), void *arg);
extern bool test_cold_native_side_consumed(const TestColdNativeSide *side,
	const ClusterWalSourceRef *source, const XLogReaderState *reader);
extern bool test_cold_native_side_applied(const TestColdNativeSide *side,
	const ClusterWalSourceRef *source, const XLogReaderState *reader);

/* Namespaced original entry points for opt-in Startup relmap replay only. */
extern RfPageProofDetailV1 cold_native_side_rf_side_online_plan_create_v1(
	const RfSideOnlinePlanRequestV1 *request, RfSideOnlinePlanV1 **out);
extern bool cold_native_side_rf_side_online_plan_bind_sources_v1(RfSideOnlinePlanV1 *plan,
	const ClusterWalSourceRef *sources, uint32 count);
extern bool cold_native_side_rf_side_online_plan_bind_database_v1(RfSideOnlinePlanV1 *plan,
	uint64 database);
extern RfPageProofDetailV1 cold_native_side_rf_side_online_plan_feed_record_v1(
	RfSideOnlinePlanV1 *plan, const RfDetachedRecordPlanV1 *record,
	const RfPageOnlineRecordIdentityV1 *identity);
extern RfPageProofDetailV1 cold_native_side_rf_side_online_plan_feed_relmap_v1(
	RfSideOnlinePlanV1 *plan, const RfDetachedRecordPlanV1 *record,
	const RfPageOnlineRecordIdentityV1 *identity, const struct ClusterColdRelmapInputV1 *input);
extern RfPageProofDetailV1 cold_native_side_rf_side_online_plan_seal_v1(RfSideOnlinePlanV1 *plan);
extern void cold_native_side_rf_side_online_plan_destroy_v1(RfSideOnlinePlanV1 **plan);
extern bool cold_native_side_rf_side_online_plan_relmap_input_matches_v1(
	const RfSideOnlinePlanV1 *plan, const struct ClusterColdRelmapInputV1 *input);
#endif
