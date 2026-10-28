/*-------------------------------------------------------------------------
 * test_cluster_side_native_clog.h
 *     Test-only SIDE owner to native CLOG boundary.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_side_native_clog.h
 * NOTES
 *     Original native bodies execute; authority, locks and faults are fixtures.
 *     No generation lifetime or joint completion certification.
 *-------------------------------------------------------------------------
 */

#ifndef TEST_CLUSTER_SIDE_NATIVE_CLOG_H
#define TEST_CLUSTER_SIDE_NATIVE_CLOG_H
#include "cluster/cluster_rf_route.h"

typedef struct SideNativeClogState {
	unsigned writes, syncs, dirsyncs;
	bool disk_zero;
} SideNativeClogState;

extern void test_side_native_clog_start(void);
extern void test_side_native_clog_stop(void);
extern void test_side_native_clog_fault(unsigned fault);
extern void test_side_native_clog_state(SideNativeClogState *out);
extern bool test_side_native_clog_apply(int origin,
	const ClusterSideProjectionOperationV1 *operation, const uint8 *payload, uint32 length);
extern bool test_side_native_clog_verify(int origin,
	const ClusterSideProjectionOperationV1 *operation, const uint8 *payload, uint32 length);
#endif
