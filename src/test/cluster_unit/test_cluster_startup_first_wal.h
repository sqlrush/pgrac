/* Original native first-record owner in the original Startup fixture.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_CLUSTER_STARTUP_FIRST_WAL_H
#define TEST_CLUSTER_STARTUP_FIRST_WAL_H
#include "cluster/cluster_page_wal.h"
extern void test_startup_first_native_begin(const ClusterWalSourceRef *source, unsigned mode);
extern bool test_startup_first_native_scan(const ClusterWalSourceRef *source,
	ClusterPageWalDirtyFloorV1 *out);
extern bool test_startup_first_native_clear(unsigned fault);
extern void test_startup_first_native_recapture(void);
extern unsigned test_startup_first_native_references(void);
extern void test_startup_first_native_end(void);
#endif
