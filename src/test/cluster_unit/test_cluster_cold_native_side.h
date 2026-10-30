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
#endif
