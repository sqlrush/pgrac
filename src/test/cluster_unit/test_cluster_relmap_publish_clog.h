/* Test-only original native observer/CLOG bridge.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_CLUSTER_RELMAP_PUBLISH_CLOG_H
#define TEST_CLUSTER_RELMAP_PUBLISH_CLOG_H
#include "cluster/cluster_relmap_owner.h"
#include "cluster/cluster_relmap_recovery.h"

typedef struct TestRelmapPublishClogStats {
	unsigned calls, owner_checks, wal_flushes, sync_requests, file_syncs, dir_syncs;
	int open_fds, holdoff, assertion_failed;
	bool locks_held;
	XidStatus disk_status;
} TestRelmapPublishClogStats;

extern void test_relmap_publish_clog_setup(const char *root,
	const ClusterRelmapOwnerSampleV2 *sample, XidStatus status, unsigned fault);
extern bool test_relmap_publish_clog_sync(const ClusterRelmapOwnerSampleV2 *sample,
	ClusterRelmapRecoveryNativeTerminal *out,
	bool (*owner_current)(const ClusterRelmapOwnerSampleV2 *), sigjmp_buf *outer_stack);
extern TestRelmapPublishClogStats test_relmap_publish_clog_stats(void);
extern TestRelmapPublishClogStats test_relmap_publish_clog_done(void);
#endif
