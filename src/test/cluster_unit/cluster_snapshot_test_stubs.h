/* Author: SqlRush <sqlrush@gmail.com>
 * Memo/lock fixtures do not run snapshot admission. Track only the actual
 * caller and balanced error cleanup; this grants no retention proof.
 * The native lifecycle and admission consumer run in test_cluster_snapshot_admission.
 */
#ifndef CLUSTER_SNAPSHOT_TEST_STUBS_H
#define CLUSTER_SNAPSHOT_TEST_STUBS_H

#include "utils/snapmgr.h"

static ClusterSnapshotReadScopeV1 *ut_snapshot_scope;

void
cluster_snapshot_read_enter_v1(ClusterSnapshotReadScopeV1 *scope, Snapshot snapshot)
{
	memset(scope, 0, sizeof(*scope));
	scope->previous = ut_snapshot_scope;
	scope->snapshot = snapshot;
	ut_snapshot_scope = scope;
}

void
cluster_snapshot_read_exit_v1(ClusterSnapshotReadScopeV1 *scope)
{
	UT_ASSERT(ut_snapshot_scope == scope);
	ut_snapshot_scope = scope->previous;
}
#endif
