/* Test-only bridge to the original owner/native-writer fixture.
 * Transaction, membership, fence and lock providers remain controlled.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_CLUSTER_RELMAP_NATIVE_OWNER_H
#define TEST_CLUSTER_RELMAP_NATIVE_OWNER_H
#include "cluster/cluster_relmap_owner.h"

typedef struct TestRelmapNativeOwnerResult {
	bool captured, matched;
	unsigned assignments, syncs, renames, assertion_failed;
} TestRelmapNativeOwnerResult;

extern TestRelmapNativeOwnerResult test_relmap_native_owner_capture(
	const ClusterWalSourceRef *source, uint64 top, uint64 epoch, unsigned fault,
	ClusterRelmapOwnerSampleV2 *out);
extern TestRelmapNativeOwnerResult
test_relmap_native_owner_check(const ClusterRelmapOwnerSampleV2 *sample, unsigned fault);
extern void test_relmap_native_owner_done(void);
#endif
