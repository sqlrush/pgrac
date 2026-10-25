/*-------------------------------------------------------------------------
 * test_cluster_relmap_native_lock.c
 *    Link the complete original singleton with explicit lower GES leaves.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "test_cluster_relmap_native_lock.h"

/* Isolate this module's definitions without changing its implementation. */
#define cluster_relmap_resid_encode test_relmap_native_resid_encode
#define cluster_relmap_authority_x_lock test_relmap_native_lock_acquire
#define cluster_relmap_authority_x_held test_relmap_native_lock_held
#define cluster_relmap_authority_x_unlock test_relmap_native_lock_release
#define cluster_relmap_lock_abort_release test_relmap_native_abort_release
#define cluster_lock_acquire_seven_step test_relmap_native_acquire_steps
#define cluster_lock_acquire_s5_promote test_relmap_native_promote
#define cluster_lock_acquire_s6_release test_relmap_native_release_step
#define cluster_ges_request_timeout_ms test_relmap_native_timeout_ms
#define GetCurrentTimestamp test_relmap_native_timestamp

#include "../../backend/cluster/cluster_relmap_lock.c"

int test_relmap_native_timeout_ms = 1234;
static TestRelmapNativeLockConfig native_config;
static TestRelmapNativeLockStats native_stats;
static ClusterLockAcquireRequest native_granted;

void
test_relmap_native_lock_reset(const TestRelmapNativeLockConfig *config)
{
	/* Reset only for an isolated test process, including after modeled PANIC. */
	memset(&relmap_x_hold, 0, sizeof(relmap_x_hold));
	memset(&native_granted, 0, sizeof(native_granted));
	memset(&native_stats, 0, sizeof(native_stats));
	native_stats.exact_request = true;
	native_config = *config;
}

void
test_relmap_native_lock_stats(TestRelmapNativeLockStats *out)
{
	*out = native_stats;
}

TimestampTz
test_relmap_native_timestamp(void)
{
	return INT64CONST(987654000);
}

ClusterLockAcquireResult
test_relmap_native_acquire_steps(const ClusterLockAcquireRequest *request)
{
	ClusterLockAcquireRequest *mutable_request = (ClusterLockAcquireRequest *) request;

	native_stats.acquires++;
	native_stats.exact_request = native_stats.exact_request
		&& request->resid.type == CLUSTER_RELMAP_RESID_TYPE
		&& request->resid.field1 == 0 && request->resid.field2 == 0
		&& request->resid.field3 == 0 && request->resid.field4 == 0
		&& request->resid.lockmethodid == DEFAULT_LOCKMETHOD
		&& request->lockmode == ExclusiveLock && request->op == CLUSTER_LOCK_OP_REQUEST
		&& request->current_mode == NoLock && request->lockmethod_id == DEFAULT_LOCKMETHOD
		&& !request->dontwait && !request->sessionLock
		&& request->caller_local_start_ts_ms == 987654
		&& request->timeout_ms == test_relmap_native_timeout_ms
		&& request->wait_event == WAIT_EVENT_CLUSTER_RELMAP_WRITE;
	/* Model the existing provider filling its original exact holder identity. */
	mutable_request->holder.node_id = 3;
	mutable_request->holder.procno = 79;
	mutable_request->holder.cluster_epoch = 411;
	mutable_request->holder.request_id = 900 + native_stats.acquires;
	mutable_request->request_id = mutable_request->holder.request_id;
	mutable_request->master_gen_snapshot = 13;
	native_granted = *request;
	native_stats.request_id = request->request_id;
	return (ClusterLockAcquireResult) native_config.acquire_result;
}

ClusterLockAcquireResult
test_relmap_native_promote(const ClusterLockAcquireRequest *request)
{
	native_stats.promotions++;
	native_stats.exact_request = native_stats.exact_request
		&& memcmp(request, &native_granted, sizeof(*request)) == 0;
	return (ClusterLockAcquireResult) native_config.promote_result;
}

ClusterLockAcquireResult
test_relmap_native_release_step(const ClusterLockAcquireRequest *request)
{
	native_stats.releases++;
	native_stats.exact_request = native_stats.exact_request
		&& memcmp(request, &native_granted, sizeof(*request)) == 0;
	native_stats.held_at_release = test_relmap_native_lock_held();
	if (native_config.before_release)
		native_config.before_release();
	if (native_config.release_error)
		ereport(ERROR, (errmsg("controlled original GES release ERROR")));
	return (ClusterLockAcquireResult) native_config.release_result;
}
