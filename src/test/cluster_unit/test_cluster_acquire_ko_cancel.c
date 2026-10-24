/*-------------------------------------------------------------------------
 *
 * test_cluster_acquire_ko_cancel.c
 *    Real S3/S5 consumers; KO ownership and native/GES are explicit fixtures.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_acquire_ko_cancel.c
 *
 * NOTES
 *    PGRAC-original standalone tests. Product APIs retain their cluster_
 *    namespace; dependency fixtures do not establish SQL or fabric results.
 *
 *-------------------------------------------------------------------------
 */
int acquire_fixture_main(int argc, char **argv);
#define main acquire_fixture_main
#include "test_cluster_lock_acquire.c"
#undef main

UT_TEST(test_pending_ko_blocks_s3_before_reservation)
{
	ClusterLockAcquireRequest req = { 0 };
	int before = stub_reserve_calls;
	req.resid.type = LOCKTAG_RELATION;
	req.locktag.locktag_type = LOCKTAG_RELATION;
	req.op = CLUSTER_LOCK_OP_REQUEST;
	req.lockmode = AccessShareLock;
	req.dontwait = true;
	ko_ax_pending = true;
	UT_ASSERT_EQ(cluster_lock_acquire_s3_partition_reservation(&req),
				 CLUSTER_LOCK_ACQUIRE_NOT_AVAIL);
	UT_ASSERT_EQ(stub_reserve_calls, before);
	ko_ax_pending = false;
}
UT_TEST(test_pending_ko_blocks_s5_and_convert_without_consumption)
{
	for (int convert = 0; convert < 2; convert++) {
		ClusterLockAcquireRequest req = { 0 };
		BackendType saved = MyBackendType;
		req.resid.type = LOCKTAG_RELATION;
		req.locktag.locktag_type = LOCKTAG_RELATION;
		req.lockmode = AccessExclusiveLock;
		req.op = convert ? CLUSTER_LOCK_OP_CONVERT : CLUSTER_LOCK_OP_REQUEST;
		req.hw_grant.key.request_id = 19;
		ko_ax_pending = true;
		MyBackendType = B_LMON; /* cooperative caller must yield, never sleep */
		UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&req), CLUSTER_LOCK_ACQUIRE_PENDING);
		UT_ASSERT(!req.hw_grant.consumed);
		UT_ASSERT_STR_EQ(req.registration_failure_reason, "KO_AX_RELEASE_PENDING");
		MyBackendType = saved;
		ko_ax_pending = false;
	}
}
UT_TEST(test_cancellation_after_local_promotion_preserves_original_grant)
{
	ClusterLockAcquireRequest req = { 0 };
	BackendType saved = MyBackendType;
	req.resid.type = LOCKTAG_RELATION;
	req.locktag.locktag_type = LOCKTAG_RELATION;
	req.lockmode = AccessExclusiveLock;
	req.op = CLUSTER_LOCK_OP_REQUEST;
	req.hw_grant.key.request_id = 19;
	req.hw_grant.master = cluster_node_id;
	owner_relation_grant_current = true;
	owner_promote_result = CLUSTER_GRD_ENTRY_OK;
	ko_ax_reads = 0;
	ko_ax_pending_after = 2;
	MyBackendType = B_LMON;
	UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&req), CLUSTER_LOCK_ACQUIRE_PENDING);
	UT_ASSERT(req.hw_grant.local_promoted && !req.hw_grant.consumed);
	UT_ASSERT_STR_EQ(req.registration_failure_reason, "KO_AX_RELEASE_PENDING");
	UT_ASSERT_EQ(ko_ax_reads, 2);
	MyBackendType = saved;
	ko_ax_pending_after = 0;
	owner_relation_grant_current = false;
	owner_promote_result = CLUSTER_GRD_ENTRY_NOT_FOUND;
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_pending_ko_blocks_s3_before_reservation);
	UT_RUN(test_pending_ko_blocks_s5_and_convert_without_consumption);
	UT_RUN(test_cancellation_after_local_promotion_preserves_original_grant);
	UT_DONE();
	return ut_failed_count != 0;
}
