/*-------------------------------------------------------------------------
 * test_cluster_cold_startup_relmap_storage.h
 *     Original relmap files and retention pins beside the Startup fixture.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_startup_relmap_storage.h
 * NOTES
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_COLD_STARTUP_RELMAP_STORAGE_H
#define TEST_CLUSTER_COLD_STARTUP_RELMAP_STORAGE_H
#include "cluster/cluster_cold_relmap.h"
#include "storage/lwlock.h"

typedef struct TestColdRelmapStorageStats {
	unsigned writes;
	unsigned io_under_retention_lock;
	bool pin_retained;
	bool may_be_staged;
	unsigned reads;
} TestColdRelmapStorageStats;

extern void test_cold_relmap_storage_begin(ClusterColdRelmapInputV1 *input);
extern void test_cold_relmap_storage_end(void);
extern void test_cold_relmap_storage_fault(unsigned write, void (*hook)(bool));
extern void test_cold_relmap_storage_read_hook(void (*hook)(unsigned));
extern ssize_t test_cold_relmap_storage_read(int fd, void *bytes, size_t len);
extern void test_cold_relmap_storage_pin_error(void);
extern bool test_cold_relmap_storage_acquire(LWLock *lock, LWLockMode mode);
extern TestColdRelmapStorageStats test_cold_relmap_storage_stats(void);
extern bool test_cold_relmap_storage_matches(const ClusterColdRelmapInputV1 *input, bool pending);
#endif
