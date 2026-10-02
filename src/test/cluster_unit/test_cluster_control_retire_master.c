/*-------------------------------------------------------------------------
 * test_cluster_control_retire_master.c -- real GES/GRD retirement callback.
 * Reuses the real-table, separate-role handoff fixture. Dedup storage is a
 * boundary fixture here; its real exact deletion has a separate C test.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#define PGRAC_HW_HANDOFF_EMBEDDED
#include "test_cluster_hw_handoff.c"
#include "cluster/cluster_control_retire.h"

static bool receipt_table_ready = true;

bool
cluster_ges_dedup_retire_control_request(uint32 node, uint32 procno, uint64 epoch, uint64 request)
{
	UT_ASSERT_EQ(node, 1);
	UT_ASSERT_EQ(procno, 21);
	UT_ASSERT_EQ(epoch, 1);
	UT_ASSERT_EQ(request, 201);
	return receipt_table_ready;
}

static void
retire_master_setup(ClusterLockAcquireRequest *request, ClusterControlRetireMessage *message,
					ClusterControlRequestCut *cut)
{
	cf_case = true;
	cf_mode = ShareLock;
	fault = HW_NORMAL;
	cooperative_case = false;
	receipt_table_ready = true;
	setup_case(request, false);
	cluster_node_id = 3;
	memset(message, 0, sizeof(*message));
	message->key.resid = request->resid;
	message->key.holder = request->holder;
	message->cleanup_epoch = 1;
	message->exchange_id = 501;
	message->verb = CLUSTER_CONTROL_RETIRE;
	UT_ASSERT(cluster_control_retire_cut(&request->resid, cut));
}

UT_TEST(retire_closes_queued_acquisition_not_just_holder)
{
	ClusterLockAcquireRequest request;
	ClusterControlRetireMessage message;
	ClusterControlRequestCut cut;
	ClusterGrdHolderId blocker = grd_lifecycle_holder(2, 23, 203);
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	int nconflicts = 0;

	retire_master_setup(&request, &message, &cut);
	blocker.cluster_epoch = 1;
	UT_ASSERT_EQ(
		cluster_grd_entry_rebind_or_insert_holder(&request.resid, &blocker, 2, ExclusiveLock),
		CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(cluster_grd_entry_enqueue_or_grant(&request.resid, &request.holder, 1, 201, 9,
													GES_REQ_OPCODE_REQUEST, ShareLock, conflicts,
													&nconflicts),
				 CLUSTER_GRD_ENQUEUED_WAITER);
	UT_ASSERT_EQ(cluster_ges_control_retire_at_master(&message, &cut), CLUSTER_CONTROL_RETIRED);
	UT_ASSERT_EQ(cluster_grd_cancel_waiter_by_id(&request.resid, &request.holder),
				 CLUSTER_GRD_ENTRY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_ges_release_and_drain_local(&request.resid, &blocker),
				 GES_REJECT_REASON_NONE);
	UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &request.holder, NULL));
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);
	UT_ASSERT_EQ(cluster_ges_control_retire_at_master(&message, &cut), CLUSTER_CONTROL_RETIRED);
}

UT_TEST(retire_wrong_cut_and_missing_tables_are_not_certificates)
{
	ClusterLockAcquireRequest request;
	ClusterControlRetireMessage message;
	ClusterControlRequestCut cut, wrong;

	retire_master_setup(&request, &message, &cut);
	wrong = cut;
	wrong.generation++;
	UT_ASSERT_EQ(cluster_ges_control_retire_at_master(&message, &wrong),
				 CLUSTER_CONTROL_RETIRE_RETRY);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 1);
	MyBackendType = B_BACKEND;
	UT_ASSERT_EQ(cluster_ges_control_retire_at_master(&message, &cut),
				 CLUSTER_CONTROL_RETIRE_RETRY);
	MyBackendType = B_LMON;
	receipt_table_ready = false;
	UT_ASSERT_EQ(cluster_ges_control_retire_at_master(&message, &cut),
				 CLUSTER_CONTROL_RETIRE_RETRY);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);
	receipt_table_ready = true;
	UT_ASSERT_EQ(cluster_ges_control_retire_at_master(&message, &cut), CLUSTER_CONTROL_RETIRED);
	grd_lifecycle_reset(0);
	UT_ASSERT_EQ(cluster_ges_control_retire_at_master(&message, &cut),
				 CLUSTER_CONTROL_RETIRE_RETRY);
}

int
main(void)
{
	MyBackendType = B_LMON;
	UT_PLAN(2);
	UT_RUN(retire_closes_queued_acquisition_not_just_holder);
	UT_RUN(retire_wrong_cut_and_missing_tables_are_not_certificates);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
