/* Actual sharedfs namespace provider for the retained cold composition.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_CLUSTER_RELMAP_NATIVE_STORAGE_H
#define TEST_CLUSTER_RELMAP_NATIVE_STORAGE_H
#include "postgres.h"
#include "utils/elog.h"
typedef struct TestRelmapNativeStorageStats {
	unsigned calls, reads;
	int live_fds, assertion_failed;
	bool sentinel_unchanged, readonly_cloexec;
} TestRelmapNativeStorageStats;
extern void test_relmap_native_storage_setup(const char *root, const uint8 uuid[16]);
extern void test_relmap_native_storage_fault(unsigned fault);
extern int test_relmap_native_storage_open(const uint8 uuid[16], sigjmp_buf *outer_stack);
extern TestRelmapNativeStorageStats test_relmap_native_storage_stats(void);
extern TestRelmapNativeStorageStats test_relmap_native_storage_done(void);
#endif
